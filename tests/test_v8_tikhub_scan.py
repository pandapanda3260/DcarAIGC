from __future__ import annotations

import copy
import hashlib
import json
import sqlite3
import tempfile
import unittest
from datetime import datetime, timedelta
from pathlib import Path
from unittest.mock import patch

from tests.roster_fixture import accept_roster
from v8 import provider_budget, providers, tikhub_scan
from v8.capture import CaptureError, ProviderResult
from v8.durable_runs import claim_run, get_run, recover_run
from v8.operations import upsert_content
from v8.storage import connect, initialize_database, transaction

NOW = "2026-08-29T04:00:00Z"
START = "2026-08-21T16:00:00Z"
END = "2026-08-28T16:00:00Z"
PUBLISHED = "2026-08-27T12:00:00Z"
DY_UID = "12345678901"
XHS_UID = "0123456789abcdef01234567"
REFERENCE = "MS4wLjAB" + "a" * 64


def epoch(at):
    return int(datetime.fromisoformat(at.replace("Z", "+00:00")).timestamp())


def later(at=NOW, seconds=301):
    return (datetime.fromisoformat(at.replace("Z", "+00:00")) + timedelta(seconds=seconds)).isoformat()


def dy_item(number=1, **overrides):
    return {
        "aweme_id": str(7600000000000000000 + number), "desc": f"fixture {number}",
        "author": {"uid": DY_UID, "nickname": "fixture"}, "create_time": epoch(PUBLISHED),
        "video": {"play_addr": {"url_list": ["https://fixture.invalid/video.mp4"]}},
        "statistics": {"digg_count": 10, "comment_count": 2}, **overrides,
    }


def dy_page(items, *, more=False, cursor=None, total=None):
    data = {"aweme_list": items, "has_more": more}
    if cursor is not None:
        data["max_cursor"] = cursor
    if total is not None:
        data["total"] = total
    return {"code": 200, "data": data}


def xhs_item(number=1, *, kind=None, uid=XHS_UID, **overrides):
    card = {
        "title": f"fixture {number}", "desc": "test note", "time": epoch(PUBLISHED),
        "user": {"user_id": uid}, "interact_info": {"liked_count": "5"}, **overrides,
    }
    if kind is not None:
        card["type"] = kind
    return {"note_id": f"{number:024x}", "note_card": card}


def result(body):
    return ProviderResult({}, copy.deepcopy(body), 200, True)


class TikHubScanTest(unittest.TestCase):
    def setUp(self):
        self.temporary = tempfile.TemporaryDirectory()
        self.addCleanup(self.temporary.cleanup)
        self.root = Path(self.temporary.name).resolve()
        self.db = self.root / "scan.sqlite3"
        self.raw_root = self.root / "raw"
        with connect(self.db) as connection:
            initialize_database(connection)
            for platform, uid in (("douyin", DY_UID), ("xiaohongshu", XHS_UID)):
                account = connection.execute(
                    "INSERT INTO accounts(phone,created_at,updated_at) VALUES ('',?,?)", (NOW, NOW),
                ).lastrowid
                connection.execute(
                    "INSERT INTO account_platform_identities(account_id,platform,uid,created_at,updated_at) "
                    "VALUES (?,?,?,?,?)", (account, platform, uid, NOW, NOW),
                )
            self.snapshot = accept_roster(connection, accepted_at=NOW)
            connection.execute(
                "INSERT INTO account_provider_references(account_identity_id,provider,reference_kind,"
                "reference_value,created_at,updated_at) VALUES (1,'TikHub','sec_user_id',?,?,?)",
                (REFERENCE, NOW, NOW),
            )
        for target in ("v8.capture.now_utc", "v8.providers.now_utc", "v8.tikhub_scan.now_utc"):
            clock = patch(target, return_value=NOW)
            clock.start()
            self.addCleanup(clock.stop)
        no_network = patch("urllib.request.urlopen", side_effect=AssertionError("Network forbidden in scan tests"))
        no_network.start()
        self.addCleanup(no_network.stop)
        key = patch.object(providers, "_load_key", return_value="fixture-only")
        key.start()
        self.addCleanup(key.stop)

    def scan(self, **kwargs):
        arguments = {
            "window_start": START, "window_end": END, "purpose": "reconcile",
            "roster_snapshot_id": self.snapshot["id"],
            "roster_snapshot_hash": self.snapshot["members_sha256"],
            "db_path": self.db, "raw_root": self.raw_root, "now": NOW,
        }
        arguments.update(kwargs)
        return tikhub_scan.run_account_scan(arguments.pop("identity_id", 1), **arguments)

    def resume(self, run_id, **kwargs):
        return tikhub_scan.resume_account_scan(
            run_id, db_path=self.db, raw_root=self.raw_root, now=kwargs.pop("now", later()), **kwargs,
        )

    def scalar(self, sql, params=()):
        with connect(self.db) as connection:
            return connection.execute(sql, params).fetchone()[0]

    def state(self, run_id):
        return get_run(run_id, db_path=self.db)["details"]["checkpoint"]

    def fetch_attempt_count(self, *, stage=None):
        if stage is None:
            return self.scalar("SELECT COUNT(*) FROM fetch_attempts")
        return self.scalar(
            "SELECT COUNT(*) FROM fetch_attempts fa "
            "JOIN fetch_slots fs ON fs.id=fa.slot_id WHERE fs.stage=?",
            (stage,),
        )

    def only_run(self):
        return self.scalar("SELECT id FROM scheduler_runs WHERE job_id IN ('tikhub_reconcile','history_recovery')")

    def recover(self, run_id):
        run = get_run(run_id, db_path=self.db)
        self.assertTrue(recover_run(
            run_id, expected_attempt_id=run["details"]["owner"]["attempt_id"], db_path=self.db, now=later(),
        ))

    def activation(self):
        with connect(self.db) as connection:
            return dict(connection.execute(
                "SELECT id activation_id,profile_id,activation_sha256 "
                "FROM acquisition_profile_activations ORDER BY id DESC LIMIT 1"
            ).fetchone())

    def manifest(self, head):
        body = Path(head["path"]).read_bytes()
        self.assertEqual(head["byte_size"], len(body))
        self.assertEqual(head["sha256"], hashlib.sha256(body).hexdigest())
        return json.loads(body)

    def test_scan_identity_and_paid_scope_freeze_complete_activation_epoch(self):
        active = self.activation()
        done = self.scan(
            **active,
            call_override=lambda _operation, _request: result(dy_page([dy_item()])),
        )
        frozen = get_run(done["scheduler_run_id"], db_path=self.db)["details"]["identity"]
        self.assertEqual(
            {key: frozen[key] for key in active},
            active,
        )
        with connect(self.db) as connection:
            usage = json.loads(connection.execute(
                "SELECT details_json FROM provider_usage ORDER BY id DESC LIMIT 1"
            ).fetchone()[0])
        self.assertEqual(usage["scope"]["activation_id"], active["activation_id"])
        with connect(self.db) as connection:
            materialization_id = int(connection.execute(
                "SELECT id FROM scheduler_runs WHERE job_id=?",
                (tikhub_scan.MATERIALIZATION_JOB,),
            ).fetchone()[0])
        materialization = get_run(materialization_id, db_path=self.db)
        self.assertEqual(
            {key: materialization["details"]["identity"][key] for key in active},
            active,
        )

    def test_disabled_day_scope_is_terminal_operator_paused_without_network(self):
        with connect(self.db) as connection, transaction(connection):
            connection.execute("UPDATE accounts SET enabled=0 WHERE id=1")
        blocked = self.scan(
            call_override=lambda *_args: self.fail("Disabled member must not call TikHub")
        )
        self.assertEqual(
            (blocked["status"], blocked["reason"], blocked["terminal_class"]),
            ("failed", "operator_paused", "not_applicable"),
        )
        self.assertTrue(blocked["accounted"])
        self.assertFalse(blocked["required"])
        self.assertFalse(blocked["publication_blocker"])
        self.assertEqual(self.fetch_attempt_count(), 0)
        self.assertEqual(self.scalar("SELECT COUNT(*) FROM provider_usage"), 0)

    def test_explicit_activation_digest_mismatch_is_rejected_before_claim(self):
        active = self.activation()
        active["activation_sha256"] = "0" * 64
        run_count = self.scalar("SELECT COUNT(*) FROM scheduler_runs")
        with self.assertRaisesRegex(tikhub_scan.RosterError, "activation digest"):
            self.scan(
                **active,
                call_override=lambda *_args: self.fail("Invalid epoch must not call TikHub"),
            )
        self.assertEqual(self.scalar("SELECT COUNT(*) FROM scheduler_runs"), run_count)
        self.assertEqual(self.fetch_attempt_count(), 0)

    def test_four_way_conservation_and_half_open_window(self):
        existing = upsert_content({
            "platform": "douyin", "platform_content_id": dy_item(1)["aweme_id"],
            "canonical_url": f"https://www.douyin.com/video/{dy_item(1)['aweme_id']}",
            "title": "keep title", "published_at": PUBLISHED,
        }, db_path=self.db)
        page = dy_page([
            dy_item(8, create_time=epoch(START) - 1),  # Pinned old row cannot stop scanning.
            dy_item(1), dy_item(2, create_time=epoch(START)), dy_item(2),
            dy_item(3, author={"uid": "987654321"}), {}, "not-an-object",
            dy_item(4, create_time=epoch(END)), dy_item(5, create_time=None),
        ])
        done = self.scan(call_override=lambda operation, request: result(page))
        self.assertTrue(done["complete"])
        self.assertEqual(done["counts"], {"existing": 2, "inserted": 1, "quarantined": 4, "unparseable": 2})
        self.assertEqual(done["raw_items"], sum(done["counts"].values()))
        self.assertEqual(self.scalar("SELECT COUNT(*) FROM content_items"), 2)
        self.assertEqual(self.scalar("SELECT title FROM content_items WHERE id=?", (existing["id"],)), "keep title")
        self.assertEqual(self.scalar("SELECT account_id FROM content_items WHERE id=?", (existing["id"],)), 1)
        manifest = self.manifest(done["last_manifest"])
        self.assertEqual(len(manifest["items"]), 9)
        self.assertEqual([item["index"] for item in manifest["items"]], list(range(9)))
        self.assertEqual(self.scalar("SELECT COUNT(*) FROM content_metric_observations"), 2)
        self.assertEqual(self.scalar(
            "SELECT COUNT(*) FROM fetch_slots WHERE stage='detail' AND status='succeeded'",
        ), 2)
        self.assertEqual(self.scalar(
            "SELECT COALESCE(SUM(fa.billed),0) FROM fetch_attempts fa "
            "JOIN fetch_slots fs ON fs.id=fa.slot_id WHERE fs.stage!='discovery'",
        ), 0)
        with connect(self.db) as connection:
            artifacts = list(connection.execute(
                "SELECT status,local_path FROM evidence_artifacts ORDER BY id",
            ))
        self.assertEqual(len(artifacts), 2)
        self.assertTrue(all(row["status"] == "available" for row in artifacts))
        self.assertTrue(all(
            Path(row["local_path"]).is_relative_to(self.raw_root / "derived-media")
            for row in artifacts
        ))
        self.assertEqual(self.scalar("SELECT DISTINCT window_key FROM content_metric_observations"), "2026-08-29")

    def test_fixed_http_routes_and_xhs_unknown_type(self):
        page = {"notes": [xhs_item(1), xhs_item(2, kind="normal"), xhs_item(3, kind="video"),
                           xhs_item(4, uid=""), {"bad": "item"}], "has_more": False}
        payload = {"code": 200, "data": {"code": 0, "success": True, "data": page}}
        with patch.object(providers, "_request_json", return_value=(200, payload)) as http:
            done = self.scan(identity_id=2)
        self.assertTrue(done["complete"])
        self.assertEqual(done["counts"], {"existing": 0, "inserted": 3, "quarantined": 1, "unparseable": 1})
        self.assertTrue(http.call_args.args[0].endswith("/api/v1/xiaohongshu/app_v2/get_user_posted_notes"))
        self.assertEqual(http.call_args.kwargs["params"], {"user_id": XHS_UID, "cursor": ""})
        with connect(self.db) as connection:
            self.assertEqual([row[0] for row in connection.execute("SELECT content_type FROM content_items ORDER BY id")],
                             ["unknown", "image", "video"])
        self.assertAlmostEqual(self.scalar("SELECT SUM(amount) FROM provider_usage"), 0.01)

    def test_v2_provider_exhaustion_retains_provider_cursor_but_closes_execution_cursor(self):
        page = dy_page(
            [dy_item(1, create_time=epoch(START) - 1, is_top=False)],
            more=False,
            cursor=77,
        )
        done = self.scan(task_id="v2-provider-exhausted", call_override=lambda *args: result(page))
        self.assertTrue(done["complete"])
        self.assertEqual(
            (done["terminal_class"], done["accounted"], done["required"], done["publication_blocker"]),
            ("success", True, True, False),
        )
        self.assertEqual(done["completion_reason"], "provider_exhausted")
        self.assertEqual(done["reason"], "provider_exhausted")
        self.assertIsNone(done["next_cursor"])
        self.assertEqual(done["provider_next_cursor"], 77)
        run = get_run(done["scheduler_run_id"], db_path=self.db)
        self.assertEqual(run["details"]["identity"]["contract_version"], "tikhub-account-scan-v2")
        checkpoint = run["details"]["checkpoint"]
        self.assertEqual(checkpoint["completion_reason"], "provider_exhausted")
        self.assertEqual(checkpoint["provider_next_cursor"], 77)
        self.assertIsNone(checkpoint["cursor"])
        manifest = self.manifest(done["last_manifest"])
        self.assertEqual(manifest["contract_version"], "tikhub-account-scan-v2")
        self.assertFalse(manifest["provider_has_more"])
        self.assertEqual(manifest["provider_next_cursor"], 77)
        self.assertIsNone(manifest["execution_next_cursor"])
        self.assertIsNone(manifest["next_cursor"])
        self.assertEqual(manifest["completion_reason"], "provider_exhausted")

    def test_v2_two_consecutive_old_non_pinned_pages_complete_with_replayable_proof(self):
        calls = []

        def fixture(operation, request):
            cursor = request["cursor"]
            calls.append(cursor)
            return result(dy_page(
                [
                    dy_item(cursor + 1, create_time=epoch(START) - cursor - 1, is_top=False),
                    dy_item(cursor + 101, create_time=epoch(PUBLISHED), is_top=True),
                ],
                more=True,
                cursor=cursor + 1,
            ))

        done = self.scan(task_id="v2-two-old-pages", call_override=fixture)
        self.assertTrue(done["complete"])
        self.assertEqual(calls, [0, 1])
        self.assertEqual(done["completion_reason"], "range_start_reached")
        self.assertEqual(done["reason"], "range_start_reached")
        self.assertEqual(done["qualifying_old_page_count"], 2)
        self.assertEqual(done["provider_next_cursor"], 2)
        self.assertIsNone(done["next_cursor"])
        head = self.manifest(done["last_manifest"])
        previous = self.manifest(head["previous"])
        self.assertEqual(
            [(previous["generation"], previous["page_number"]),
             (head["generation"], head["page_number"])],
            [(0, 0), (0, 1)],
        )
        self.assertEqual(
            [previous["range_start_proof"]["qualifying_old_page_count"],
             head["range_start_proof"]["qualifying_old_page_count"]],
            [1, 2],
        )
        self.assertTrue(previous["range_start_proof"]["qualifies"])
        self.assertTrue(head["range_start_proof"]["qualifies"])
        self.assertEqual(head["range_start_proof"]["pinned_count"], 1)
        self.assertEqual(head["range_start_proof"]["non_pinned_count"], 1)
        self.assertEqual(head["completion_reason"], "range_start_reached")
        self.assertTrue(head["provider_has_more"])
        self.assertEqual(head["provider_next_cursor"], 2)
        self.assertIsNone(head["execution_next_cursor"])
        item = head["items"][0]
        self.assertFalse(item["is_pinned"])
        self.assertEqual(item["published_at"], item["event_tuple"][0])
        self.assertEqual(item["platform_content_id"], item["event_tuple"][1])

    def test_v2_xhs_nested_sticky_marker_is_normalized_for_two_page_boundary(self):
        calls = []

        def fixture(operation, request):
            cursor = request["cursor"]
            calls.append(cursor)
            number = len(calls)
            return result({
                "notes": [xhs_item(number, time=epoch(START) - number, sticky="0")],
                "has_more": True,
                "cursor": f"page-{number + 1}",
            })

        done = self.scan(identity_id=2, task_id="v2-xhs-two-old-pages", call_override=fixture)
        self.assertTrue(done["complete"])
        self.assertEqual(calls, ["", "page-2"])
        self.assertEqual(done["completion_reason"], "range_start_reached")
        manifest = self.manifest(done["last_manifest"])
        self.assertFalse(manifest["items"][0]["is_pinned"])
        self.assertEqual(manifest["provider_next_cursor"], "page-3")
        self.assertIsNone(manifest["execution_next_cursor"])

    def test_v2_nonqualifying_pages_never_claim_range_completion(self):
        old = epoch(START) - 1
        cases = {
            "pinned_only": [
                [dy_item(1, create_time=old, is_top=True)],
                [dy_item(2, create_time=old - 1, is_top=True)],
            ],
            "unknown_pinned": [
                [dy_item(3, create_time=old)],
                [dy_item(4, create_time=old - 1)],
            ],
            "missing_published_at": [
                [dy_item(5, create_time=None, is_top=False)],
                [dy_item(6, create_time=None, is_top=False)],
            ],
            "missing_content_id": [
                [dy_item(7, aweme_id="", create_time=old, is_top=False)],
                [dy_item(8, aweme_id="", create_time=old - 1, is_top=False)],
            ],
            "equal_to_start": [
                [dy_item(9, create_time=epoch(START), is_top=False)],
                [dy_item(10, create_time=epoch(START), is_top=False)],
            ],
            "single_old_page": [
                [dy_item(11, create_time=old, is_top=False)],
                [dy_item(12, create_time=epoch(PUBLISHED), is_top=False)],
            ],
        }
        for name, pages in cases.items():
            with self.subTest(name=name):
                calls = []

                def fixture(operation, request, pages=pages):
                    cursor = request["cursor"]
                    calls.append(cursor)
                    return result(dy_page(pages[cursor], more=True, cursor=cursor + 1))

                blocked = self.scan(
                    task_id=f"v2-nonqualifying-{name}", max_pages=2, call_override=fixture,
                )
                self.assertEqual(calls, [0, 1])
                self.assertEqual(
                    (blocked["status"], blocked["reason"], blocked["complete"]),
                    ("partial", "page_limit_yield", False),
                )
                self.assertIsNone(blocked["completion_reason"])
                self.assertEqual(blocked["qualifying_old_page_count"], 0)
                manifest = self.manifest(blocked["last_manifest"])
                self.assertIsNone(manifest["completion_reason"])
                self.assertIsNotNone(manifest["execution_next_cursor"])
                if name == "pinned_only":
                    self.assertEqual(manifest["range_start_proof"]["non_pinned_count"], 0)
                elif name == "unknown_pinned":
                    self.assertEqual(manifest["range_start_proof"]["unknown_pinned_count"], 1)
                elif name.startswith("missing_"):
                    first = self.manifest(manifest["previous"])
                    self.assertEqual(first["range_start_proof"]["missing_event_count"], 1)
                elif name == "equal_to_start":
                    first = self.manifest(manifest["previous"])
                    self.assertFalse(first["range_start_proof"]["all_non_pinned_before_window_start"])
                else:
                    self.assertFalse(manifest["range_start_proof"]["qualifies"])

    def test_v2_page_cap_yields_and_old_page_streak_survives_resume(self):
        calls = []

        def fixture(operation, request):
            cursor = request["cursor"]
            calls.append(cursor)
            return result(dy_page(
                [dy_item(cursor + 20, create_time=epoch(START) - cursor - 1, is_top=False)],
                more=True,
                cursor=cursor + 1,
            ))

        first = self.scan(
            task_id="v2-streak-resume", max_pages=1, call_override=fixture,
        )
        self.assertEqual(
            (first["status"], first["reason"], first["complete"]),
            ("partial", "page_limit_yield", False),
        )
        self.assertEqual(first["qualifying_old_page_count"], 1)
        self.assertEqual(first["next_cursor"], 1)
        done = self.resume(
            first["scheduler_run_id"], max_pages=1, call_override=fixture,
        )
        self.assertTrue(done["complete"])
        self.assertEqual(calls, [0, 1])
        self.assertEqual(done["completion_reason"], "range_start_reached")
        self.assertEqual(done["qualifying_old_page_count"], 2)
        self.assertEqual(done["provider_next_cursor"], 2)
        self.assertIsNone(done["next_cursor"])

    def test_v2_cursor_expiry_does_not_reset_paid_generation(self):
        calls = []

        def first_generation(operation, request):
            cursor = request["cursor"]
            calls.append(cursor)
            if cursor == 1:
                raise CaptureError(
                    "fixture cursor expired", retryable=False, error_code="http_400",
                    http_status=400, billed=False, raw_response={"message": "max_cursor expired"},
                )
            return result(dy_page(
                [dy_item(30, create_time=epoch(START) - 1, is_top=False)],
                more=True,
                cursor=1,
            ))

        first = self.scan(
            task_id="v2-generation-reset", max_pages=1, call_override=first_generation,
        )
        self.assertEqual(first["qualifying_old_page_count"], 1)
        reset = self.resume(
            first["scheduler_run_id"], max_pages=1, call_override=first_generation,
        )
        self.assertEqual(reset["reason"], "cursor_expired")
        self.assertEqual(reset["terminal_class"], "provider_transient")
        self.assertEqual(self.state(reset["scheduler_run_id"])["generation"], 0)
        self.assertEqual(reset["qualifying_old_page_count"], 1)
        self.assertEqual(reset["provider_next_cursor"], 1)

        generation_one_calls = 0

        def generation_one(operation, request):
            nonlocal generation_one_calls
            cursor = request["cursor"]
            calls.append(cursor)
            generation_one_calls += 1
            return result(dy_page(
                [dy_item(30 + generation_one_calls, create_time=epoch(START) - generation_one_calls,
                         is_top=False)],
                more=True,
                cursor=cursor + 1,
            ))

        done = self.resume(
            first["scheduler_run_id"], now=later(NOW, 602), call_override=generation_one,
        )
        self.assertFalse(done["complete"])
        self.assertEqual(done["reason"], "cursor_expired")
        self.assertEqual(calls, [0, 1])
        self.assertEqual(generation_one_calls, 0)
        self.assertEqual(done["qualifying_old_page_count"], 1)

    def test_v1_partial_resume_retains_v1_contract_and_exhaustion_semantics(self):
        scope = tikhub_scan._freeze(
            1,
            window_start=START,
            window_end=END,
            purpose="reconcile",
            roster_snapshot_id=self.snapshot["id"],
            roster_snapshot_hash=self.snapshot["members_sha256"],
            db_path=self.db,
            task_id="legacy-v1-resume",
            task_max_amount=None,
        )
        scope["contract_version"] = tikhub_scan.LEGACY_CONTRACT_VERSION
        calls = []
        materialize_patch = patch.object(
            providers,
            "materialize_account_discovery_page",
            side_effect=AssertionError("v1 scans must not create v2 materialization children"),
        )
        materialize = materialize_patch.start()
        self.addCleanup(materialize_patch.stop)

        def fixture(operation, request):
            cursor = request["cursor"]
            calls.append(cursor)
            return result(dy_page(
                [dy_item(cursor + 40, create_time=epoch(START) - cursor - 1, is_top=False)],
                more=cursor < 2,
                cursor=cursor + 1,
            ))

        first = tikhub_scan._run(
            scope, db_path=self.db, max_pages=1, raw_root=self.raw_root,
            now=NOW, call_override=fixture,
        )
        self.assertFalse(first["complete"])
        second = self.resume(
            first["scheduler_run_id"], max_pages=1, call_override=fixture,
        )
        self.assertFalse(second["complete"])
        self.assertIsNone(second["completion_reason"])
        done = self.resume(
            first["scheduler_run_id"], max_pages=1, now=later(NOW, 602), call_override=fixture,
        )
        self.assertTrue(done["complete"])
        self.assertEqual(done["reason"], "source_exhausted")
        self.assertEqual(calls, [0, 1, 2])
        head = done["last_manifest"]
        versions = []
        while head is not None:
            manifest = self.manifest(head)
            versions.append(manifest["contract_version"])
            self.assertNotIn("range_start_proof", manifest)
            head = manifest["previous"]
        self.assertEqual(versions, [tikhub_scan.LEGACY_CONTRACT_VERSION] * 3)
        with connect(self.db) as connection:
            adapters = [row[0] for row in connection.execute(
                "SELECT adapter_version FROM fetch_slots "
                "WHERE stage='discovery' ORDER BY id",
            )]
        self.assertEqual(adapters, [tikhub_scan.LEGACY_CONTRACT_VERSION] * 3)
        materialize.assert_not_called()

    def test_twenty_pages_yield_and_resume_not_exhaustion(self):
        calls = []

        def fixture(operation, request):
            cursor = request["cursor"]
            calls.append(cursor)
            return result(dy_page([dy_item(cursor + 1)], more=cursor < 20, cursor=cursor + 1))

        first = self.scan(call_override=fixture)
        self.assertEqual((first["status"], first["reason"], first["complete"]), ("partial", "page_limit_yield", False))
        self.assertEqual((first["pages"], first["next_cursor"]), (20, 20))
        self.assertEqual(len(calls), 20)
        early = self.resume(first["scheduler_run_id"], now=NOW, call_override=fixture)
        self.assertEqual(early["attempt_id"], first["attempt_id"])
        self.assertEqual(len(calls), 20)
        done = self.resume(first["scheduler_run_id"], call_override=fixture)
        self.assertTrue(done["complete"])
        self.assertEqual((done["pages"], done["pages_this_run"]), (21, 1))
        self.assertEqual(calls, list(range(21)))
        self.assertEqual(self.fetch_attempt_count(stage="discovery"), 21)
        self.assertEqual(self.fetch_attempt_count(stage="detail"), 21)
        previous = self.manifest(done["last_manifest"])["previous"]
        self.assertEqual(self.manifest(previous)["page_number"], 19)

    def test_budget_block_before_dispatch_is_terminal_and_zero_network_on_reentry(self):
        def fixture(operation, request):
            return result(dy_page([dy_item()]))
        with patch.dict(provider_budget.BUDGET_BUCKET_MICROUSD, {"discovery": 0}):
            blocked = self.scan(call_override=fixture)
        self.assertEqual(
            (blocked["status"], blocked["reason"]),
            ("failed", "discovery_budget_exhausted"),
        )
        self.assertEqual(
            (blocked["terminal_class"], blocked["accounted"], blocked["required"]),
            ("budget_deferred", True, True),
        )
        self.assertEqual(blocked["blocker"], "discovery_budget_exhausted")
        self.assertFalse(blocked["complete"])
        self.assertEqual((blocked["pages"], blocked["next_cursor"]), (0, 0))
        self.assertEqual(self.scalar("SELECT COUNT(*) FROM fetch_attempts"), 0)
        self.assertEqual(self.scalar("SELECT COUNT(*) FROM provider_usage"), 0)
        again = self.resume(
            blocked["scheduler_run_id"], call_override=lambda *args: self.fail("Terminal budget receipt must not retry")
        )
        self.assertEqual(
            (again["status"], again["reason"]),
            ("failed", "discovery_budget_exhausted"),
        )

    def test_scan_cannot_send_after_frozen_beijing_business_day(self):
        calls = []
        next_day = "2026-08-29T16:00:00Z"
        with patch("v8.capture.now_utc", return_value=next_day):
            blocked = self.scan(
                now=next_day,
                call_override=lambda *args: calls.append(args),
            )
        self.assertEqual((blocked["reason"], blocked["blocker"]),
                         ("business_day_expired", "business_day_expired"))
        self.assertEqual((blocked["status"], blocked["terminal_class"]), ("failed", "deadline"))
        self.assertEqual(calls, [])
        self.assertEqual(self.scalar("SELECT COUNT(*) FROM fetch_attempts"), 0)
        self.assertEqual(self.scalar("SELECT SUM(request_attempts) FROM provider_usage"), 0)
        again = self.resume(
            blocked["scheduler_run_id"],
            call_override=lambda *args: self.fail("Deadline terminal must not retry"),
        )
        self.assertEqual((again["status"], again["terminal_class"]), ("failed", "deadline"))


    def test_crash_rolls_back_business_and_cursor_then_replays_raw_free(self):
        calls = []
        real_checkpoint = tikhub_scan.checkpoint

        def fixture(operation, request):
            calls.append(operation)
            return result(dy_page([dy_item()]))

        def crash(connection, claim, changes, **kwargs):
            if "last_manifest" in changes:
                raise RuntimeError("crash after business writes before cursor commit")
            return real_checkpoint(connection, claim, changes, **kwargs)

        with patch.object(tikhub_scan, "checkpoint", side_effect=crash), self.assertRaises(RuntimeError):
            self.scan(call_override=fixture)
        run_id = self.only_run()
        self.assertEqual(self.scalar("SELECT COUNT(*) FROM content_items"), 0)
        self.assertEqual(self.scalar("SELECT COUNT(*) FROM content_metric_observations"), 0)
        self.assertEqual(self.scalar("SELECT COUNT(*) FROM provider_raw_responses"), 1)
        self.assertEqual((self.state(run_id)["page_number"], self.state(run_id)["cursor"]), (0, 0))
        self.assertIsNotNone(self.state(run_id)["pending_raw"])
        self.recover(run_id)
        done = self.resume(run_id, call_override=fixture)
        self.assertTrue(done["complete"])
        self.assertEqual(calls, ["douyin_user_posts"])
        self.assertEqual(self.fetch_attempt_count(stage="discovery"), 1)
        self.assertEqual(self.fetch_attempt_count(stage="detail"), 1)
        self.assertEqual(self.scalar("SELECT COUNT(*) FROM content_metric_observations"), 1)
        self.assertEqual(done["counts"]["inserted"], 1)

    def test_v2_materialization_starts_after_parent_page_commit_and_closes_pending(self):
        real_materialize = providers.materialize_account_discovery_page
        observed = {}

        def inspect_commit(**kwargs):
            run_id = self.only_run()
            state = self.state(run_id)
            observed.update(
                page_number=state["page_number"],
                complete=state["complete"],
                pending=copy.deepcopy(state["pending_materialization"]),
            )
            with connect(self.db) as connection:
                observed["raw_source"] = connection.execute(
                    "SELECT source FROM provider_raw_responses WHERE id=?",
                    (kwargs["source_raw_response_id"],),
                ).fetchone()[0]
            return real_materialize(**kwargs)

        with patch.object(
            providers, "materialize_account_discovery_page", side_effect=inspect_commit,
        ) as materialize:
            done = self.scan(call_override=lambda *args: result(dy_page([dy_item()])))

        self.assertTrue(done["complete"])
        self.assertEqual(observed["page_number"], 1)
        self.assertFalse(observed["complete"])
        self.assertIsNotNone(observed["pending"])
        self.assertEqual(observed["raw_source"], "live")
        self.assertIsNone(self.state(done["scheduler_run_id"])["pending_materialization"])
        materialize.assert_called_once()
        with connect(self.db) as connection:
            child = connection.execute(
                "SELECT status,details_json FROM scheduler_runs WHERE job_id=?",
                (tikhub_scan.MATERIALIZATION_JOB,),
            ).fetchone()
        details = json.loads(child["details_json"])
        identity = details["identity"]
        self.assertEqual(child["status"], "succeeded")
        self.assertTrue(details["checkpoint"]["complete"])
        self.assertEqual(identity["derived_adapter_version"], "tikhub-discovery-derived-v8.1")
        self.assertEqual(
            identity["derived_operations"],
            {"detail": "douyin_video_detail", "metrics": "douyin_video_statistics"},
        )
        self.assertTrue(identity["preserve_existing_content_fields"])
        self.assertEqual(self.scalar("SELECT COUNT(*) FROM provider_usage"), 1)
        self.assertEqual(self.scalar(
            "SELECT COALESCE(SUM(fa.billed),0) FROM fetch_attempts fa "
            "JOIN fetch_slots fs ON fs.id=fa.slot_id WHERE fs.stage!='discovery'",
        ), 0)

    def test_v2_derived_detail_atomically_preserves_concurrent_richer_content(self):
        real_materialize = providers.materialize_account_discovery_page

        def concurrent_live_detail(**kwargs):
            with connect(self.db) as connection, transaction(connection):
                connection.execute(
                    "UPDATE content_items SET title=?,body=?,updated_at=?",
                    ("RICH live title", "RICH live body", later(NOW, 1)),
                )
            return real_materialize(**kwargs)

        with patch.object(
            providers,
            "materialize_account_discovery_page",
            side_effect=concurrent_live_detail,
        ):
            done = self.scan(
                call_override=lambda *args: result(dy_page([dy_item()])),
            )
        self.assertTrue(done["complete"])
        with connect(self.db) as connection:
            content = connection.execute(
                "SELECT title,body FROM content_items",
            ).fetchone()
        self.assertEqual(
            (content["title"], content["body"]),
            ("RICH live title", "RICH live body"),
        )

    def test_v2_materialization_lock_failure_resumes_locally_without_provider_call(self):
        provider_calls = []

        def provider_call(operation, request):
            cursor = request["cursor"]
            provider_calls.append(cursor)
            return result(dy_page(
                [dy_item(cursor + 1)], more=cursor == 0, cursor=cursor + 1,
            ))

        with patch.object(
            providers,
            "materialize_account_discovery_page",
            side_effect=sqlite3.OperationalError("database is locked"),
        ):
            blocked = self.scan(call_override=provider_call)
        self.assertEqual((blocked["status"], blocked["reason"]), ("partial", "materialization_pending"))
        self.assertFalse(blocked["complete"])
        pending = self.state(blocked["scheduler_run_id"])["pending_materialization"]
        self.assertIsNotNone(pending)
        self.assertEqual(provider_calls, [0])
        with connect(self.db) as connection:
            child = connection.execute(
                "SELECT status FROM scheduler_runs WHERE job_id=?",
                (tikhub_scan.MATERIALIZATION_JOB,),
            ).fetchone()
        self.assertEqual(child["status"], "partial")

        real_materialize = providers.materialize_account_discovery_page
        with patch.object(
            providers, "materialize_account_discovery_page", wraps=real_materialize,
        ) as local_retry:
            local_only = self.resume(
                blocked["scheduler_run_id"],
                call_override=lambda *args: self.fail("Pending local child must not call provider"),
            )
        self.assertEqual(
            (local_only["status"], local_only["reason"], local_only["complete"]),
            ("partial", "materialization_replay_yield", False),
        )
        self.assertEqual(provider_calls, [0])
        self.assertIsNone(self.state(local_only["scheduler_run_id"])["pending_materialization"])
        local_retry.assert_called_once()
        self.assertEqual(self.fetch_attempt_count(stage="discovery"), 1)

        done = self.resume(
            blocked["scheduler_run_id"],
            now=local_only["next_resume_at"],
            call_override=provider_call,
        )
        self.assertTrue(done["complete"])
        self.assertEqual(provider_calls, [0, 1])
        self.assertEqual(self.fetch_attempt_count(stage="discovery"), 2)

    def test_v2_succeeded_child_is_reused_after_parent_clear_crash(self):
        provider_calls = []
        real_finish = tikhub_scan._finish_materialization_parent

        def provider_call(operation, request):
            provider_calls.append(operation)
            return result(dy_page([dy_item()]))

        with patch.object(
            tikhub_scan,
            "_finish_materialization_parent",
            side_effect=RuntimeError("crash after child success before parent clear"),
        ), self.assertRaises(RuntimeError):
            self.scan(call_override=provider_call)
        run_id = self.only_run()
        self.assertIsNotNone(self.state(run_id)["pending_materialization"])
        with connect(self.db) as connection:
            child = connection.execute(
                "SELECT status FROM scheduler_runs WHERE job_id=?",
                (tikhub_scan.MATERIALIZATION_JOB,),
            ).fetchone()
        self.assertEqual(child["status"], "succeeded")
        self.recover(run_id)
        with patch.object(
            providers,
            "materialize_account_discovery_page",
            side_effect=AssertionError("Succeeded local child must not replay"),
        ) as materialize, patch.object(
            tikhub_scan, "_finish_materialization_parent", wraps=real_finish,
        ):
            done = self.resume(
                run_id,
                call_override=lambda *args: self.fail("Succeeded local child must not call provider"),
            )
        self.assertTrue(done["complete"])
        self.assertIsNone(self.state(run_id)["pending_materialization"])
        self.assertEqual(provider_calls, ["douyin_user_posts"])
        materialize.assert_not_called()

    def test_v2_completed_running_child_finishes_after_local_finish_failure(self):
        real_finish = tikhub_scan.finish_run
        finish_failed = False

        def fail_child_finish_once(claim, **kwargs):
            nonlocal finish_failed
            run = get_run(claim.scheduler_run_id, db_path=self.db)
            if (
                run["job_id"] == tikhub_scan.MATERIALIZATION_JOB
                and kwargs["status"] == "succeeded"
                and not finish_failed
            ):
                finish_failed = True
                raise sqlite3.OperationalError("database is locked")
            return real_finish(claim, **kwargs)

        with patch.object(tikhub_scan, "finish_run", side_effect=fail_child_finish_once):
            blocked = self.scan(
                call_override=lambda *args: result(dy_page([dy_item()])),
            )
        self.assertEqual((blocked["status"], blocked["reason"]), ("partial", "materialization_pending"))
        with connect(self.db) as connection:
            child = connection.execute(
                "SELECT status,details_json FROM scheduler_runs WHERE job_id=?",
                (tikhub_scan.MATERIALIZATION_JOB,),
            ).fetchone()
        self.assertEqual(child["status"], "running")
        self.assertTrue(json.loads(child["details_json"])["checkpoint"]["complete"])

        with patch.object(
            providers,
            "materialize_account_discovery_page",
            side_effect=AssertionError("Completed local child must not replay"),
        ) as materialize:
            done = self.resume(
                blocked["scheduler_run_id"],
                call_override=lambda *args: self.fail("Completed local child must not call provider"),
            )
        self.assertTrue(done["complete"])
        materialize.assert_not_called()
        with connect(self.db) as connection:
            child = connection.execute(
                "SELECT status FROM scheduler_runs WHERE job_id=?",
                (tikhub_scan.MATERIALIZATION_JOB,),
            ).fetchone()
        self.assertEqual(child["status"], "succeeded")

    def test_corrupt_raw_blocks_replay_without_repaying(self):
        def fixture(operation, request):
            return result(dy_page([dy_item()]))
        with patch.object(tikhub_scan, "_apply", side_effect=RuntimeError("crash")), self.assertRaises(RuntimeError):
            self.scan(call_override=fixture)
        run_id = self.only_run()
        with connect(self.db) as connection:
            raw = connection.execute("SELECT local_path FROM provider_raw_responses").fetchone()[0]
        Path(raw).write_bytes(b"corrupted fixture")
        self.recover(run_id)
        done = self.resume(run_id, call_override=lambda *args: self.fail("Must not refetch corrupt successful raw"))
        self.assertEqual((done["status"], done["reason"], done["pages"]), ("failed", "raw_integrity_error", 0))
        self.assertEqual((done["terminal_class"], done["publication_blocker"]), ("integrity", True))
        self.assertEqual(self.scalar("SELECT COUNT(*) FROM content_items"), 0)
        self.assertEqual(self.fetch_attempt_count(stage="discovery"), 1)

    def test_stale_attempt_can_retain_raw_but_cannot_apply_or_finish(self):
        new_claims = []

        def fixture(operation, request):
            run_id = self.only_run()
            self.recover(run_id)
            run = get_run(run_id, db_path=self.db)
            new_claims.append(claim_run(run["job_id"], run["details"]["identity"], db_path=self.db, now=later()))
            return result(dy_page([dy_item()]))

        old = self.scan(call_override=fixture)
        self.assertEqual(old["reason"], "attempt_owner_lost")
        self.assertEqual(old["status"], "running")
        self.assertFalse(old["complete"])
        self.assertEqual(old["attempt_id"], new_claims[0].attempt_id)
        self.assertEqual(self.scalar("SELECT COUNT(*) FROM content_items"), 0)
        self.assertEqual(self.scalar("SELECT COUNT(*) FROM provider_raw_responses"), 1)
        self.assertEqual(self.state(old["scheduler_run_id"])["page_number"], 0)
        self.assertEqual(self.scalar("SELECT COUNT(*) FROM scheduler_run_attempts WHERE status='interrupted'"), 1)

    def test_activation_replacement_is_terminal_even_after_reacceptance(self):
        calls = []

        def fixture(operation, request):
            calls.append(operation)
            with connect(self.db) as connection, transaction(connection):
                accept_roster(connection, [2], accepted_at=later(NOW, 1))
            return result(dy_page([dy_item()]))

        blocked = self.scan(call_override=fixture)
        self.assertEqual(blocked["reason"], "profile_superseded")
        self.assertEqual((blocked["status"], blocked["terminal_class"]), ("failed", "deadline"))
        self.assertEqual(self.scalar("SELECT COUNT(*) FROM content_items"), 0)
        self.assertEqual(self.scalar("SELECT COUNT(*) FROM provider_raw_responses"), 1)
        with connect(self.db) as connection, transaction(connection):
            accept_roster(connection, [1, 2], accepted_at=later(NOW, 2))
        done = self.resume(
            blocked["scheduler_run_id"],
            call_override=lambda *args: self.fail("Terminal scope change must not replay"),
        )
        self.assertEqual((done["status"], done["terminal_class"]), ("failed", "deadline"))
        self.assertEqual(len(calls), 1)

    def test_invalid_missing_and_repeated_cursor_never_advance(self):
        for cursor, expected in ((None, "invalid_cursor"), (0, "repeated_cursor"), (True, "invalid_cursor")):
            with self.subTest(cursor=cursor):
                payload = dy_page([dy_item()], more=True, cursor=cursor)
                blocked = self.scan(task_id=f"bad-cursor-{cursor}", call_override=lambda *args: result(payload))
                self.assertFalse(blocked["complete"])
                self.assertEqual((blocked["reason"], blocked["pages"]), (expected, 0))
                before = self.scalar("SELECT COUNT(*) FROM fetch_attempts")
                replay = self.resume(blocked["scheduler_run_id"], call_override=lambda *args: self.fail("Paid replay"))
                self.assertEqual(replay["reason"], expected)
                self.assertEqual(self.scalar("SELECT COUNT(*) FROM fetch_attempts"), before)
        self.assertEqual(self.scalar("SELECT COUNT(*) FROM content_items"), 0)

    def test_declared_total_drift_preserves_prior_checkpoint(self):
        def fixture(operation, request):
            cursor = request["cursor"]
            return result(dy_page([dy_item(cursor + 1)], more=cursor == 0, cursor=cursor + 1, total=2 + cursor))

        blocked = self.scan(call_override=fixture)
        self.assertEqual((blocked["reason"], blocked["pages"], blocked["next_cursor"]), ("total_drift", 1, 1))
        self.assertEqual(self.scalar("SELECT COUNT(*) FROM content_items"), 1)
        self.assertIsNotNone(self.state(blocked["scheduler_run_id"])["pending_raw"])

    def test_success_shape_error_is_saved_once_for_free_replay(self):
        bad = {"code": 200, "data": {"aweme_list": [dy_item()]}}
        with patch.object(providers, "_request_json", return_value=(200, bad)) as http:
            blocked = self.scan()
            again = self.resume(blocked["scheduler_run_id"])
            exhausted = self.resume(
                blocked["scheduler_run_id"], now=again["next_resume_at"]
            )
            terminal_reentry = self.resume(
                blocked["scheduler_run_id"],
                now=later(again["next_resume_at"]),
                call_override=lambda *args: self.fail("Terminal invalid raw must not repay"),
            )
        self.assertEqual((blocked["status"], again["status"]), ("partial", "partial"))
        self.assertEqual((exhausted["status"], exhausted["terminal_class"]),
                         ("failed", "provider_transient"))
        self.assertEqual(terminal_reentry["status"], "failed")
        self.assertEqual(http.call_count, 1)
        self.assertEqual(self.scalar("SELECT COUNT(*) FROM content_items"), 0)


    def test_profile_reference_proves_uid_and_uses_fixed_douyin_http_routes(self):
        with connect(self.db) as connection, transaction(connection):
            connection.execute("DELETE FROM account_provider_references")
        profile = {"code": 200, "data": {"status_code": 0, "user": {"uid": DY_UID, "sec_uid": REFERENCE}}}

        def http_fixture(url, **kwargs):
            if url.endswith("/api/v1/douyin/web/fetch_user_profile_by_uid"):
                self.assertEqual(kwargs["params"], {"uid": DY_UID})
                return 200, profile
            self.assertTrue(url.endswith("/api/v1/douyin/app/v3/fetch_user_post_videos"))
            self.assertEqual(kwargs["params"], {"sec_user_id": REFERENCE, "max_cursor": 0, "count": 20, "sort_type": 0})
            return 200, dy_page([dy_item()])

        with patch.object(providers, "_request_json", side_effect=http_fixture) as http:
            done = self.scan()
            again = self.scan(window_start="2026-08-20T16:00:00Z")
        self.assertTrue(done["complete"] and again["complete"])
        self.assertEqual(http.call_count, 3)  # One lifetime reference, two separate frozen lists.
        self.assertEqual(self.scalar("SELECT reference_value FROM account_provider_references"), REFERENCE)
        self.assertAlmostEqual(self.scalar("SELECT SUM(amount) FROM provider_usage"), 0.003)

    def test_profile_wrong_uid_stays_raw_and_never_fetches_posts(self):
        with connect(self.db) as connection, transaction(connection):
            connection.execute("DELETE FROM account_provider_references")
        profile = {"code": 200, "data": {"user": {"uid": "999999", "sec_uid": REFERENCE}}}
        with patch.object(providers, "_request_json", return_value=(200, profile)) as http:
            blocked = self.scan()
            again = self.resume(blocked["scheduler_run_id"])
        self.assertEqual((blocked["reason"], again["reason"]), ("identity_conflict", "identity_conflict"))
        self.assertEqual((blocked["status"], blocked["terminal_class"]), ("failed", "integrity"))
        self.assertEqual(http.call_count, 1)
        self.assertEqual(self.scalar("SELECT COUNT(*) FROM account_provider_references"), 0)
        self.assertEqual(self.scalar("SELECT COUNT(*) FROM content_items"), 0)

    def test_history_paid_scan_is_budget_deferred_without_compensation(self):
        def fixture(operation, request):
            return result(dy_page([dy_item()]))

        one = self.scan(purpose="history", call_override=fixture)
        two = self.scan(purpose="history", window_start="2026-08-20T16:00:00Z", call_override=fixture)
        self.assertNotEqual(one["scan_id"], two["scan_id"])
        self.assertEqual(
            [(one["reason"], one["terminal_class"]),
             (two["reason"], two["terminal_class"])],
            [
                ("repair_budget_exhausted", "budget_deferred"),
                ("repair_budget_exhausted", "budget_deferred"),
            ],
        )
        with connect(self.db) as connection:
            usage = list(connection.execute("SELECT task_id,details_json FROM provider_usage"))
            budgets = list(connection.execute("SELECT max_amount FROM provider_budget_batches"))
            self.assertEqual(usage, [])
            self.assertEqual([row[0] for row in budgets], [100, 100])
        self.assertEqual(self.scalar("SELECT COUNT(*) FROM content_items"), 0)
        self.assertEqual(self.fetch_attempt_count(stage="discovery"), 0)

    def test_explicit_campaign_budget_is_cumulative_across_windows_and_days(self):
        def fixture(operation, request):
            return result(dy_page([dy_item()]))

        one = self.scan(purpose="history", task_id="campaign-fixture", task_max_amount=0.001, call_override=fixture)
        self.assertEqual(
            (one["reason"], one["terminal_class"]),
            ("repair_budget_exhausted", "budget_deferred"),
        )
        next_day = "2026-08-30T04:00:00Z"
        with patch("v8.capture.now_utc", return_value=next_day), patch("v8.providers.now_utc", return_value=next_day):
            blocked = self.scan(purpose="history", window_start="2026-08-20T16:00:00Z", now=next_day,
                                task_id="campaign-fixture", task_max_amount=0.001,
                                call_override=lambda *args: self.fail("Campaign ceiling must block before HTTP"))
        self.assertEqual(blocked["reason"], "repair_budget_exhausted")
        self.assertEqual(blocked["blocker"], "repair_budget_exhausted")
        self.assertEqual((blocked["status"], blocked["terminal_class"]), ("failed", "budget_deferred"))
        self.assertEqual(self.fetch_attempt_count(stage="discovery"), 0)
        frozen = get_run(blocked["scheduler_run_id"], db_path=self.db)["details"]["identity"]
        self.assertEqual((frozen["task_id"], frozen["task_max_microusd"]), ("campaign-fixture", 1000))

    def test_billing_unknown_transient_error_is_not_retried_in_invocation(self):
        calls = []

        def fixture(operation, request):
            calls.append(operation)
            raise CaptureError("fixture upstream unavailable", retryable=True, error_code="http_503", http_status=503)

        blocked = self.scan(call_override=fixture)
        self.assertEqual(blocked["reason"], "http_503")
        self.assertEqual(len(calls), 1)
        self.assertEqual(self.scalar("SELECT COUNT(*) FROM fetch_attempts"), 1)
        self.assertFalse(blocked["complete"])

    def test_explicitly_unbilled_transient_error_does_not_authorize_retry(self):
        calls = []

        def fixture(operation, request):
            calls.append(operation)
            raise CaptureError(
                "fixture upstream unavailable",
                retryable=True,
                error_code="http_503",
                http_status=503,
                billed=False,
            )

        blocked = self.scan(call_override=fixture)
        self.assertEqual(blocked["reason"], "http_503")
        self.assertEqual(len(calls), 1)
        self.assertEqual(self.scalar("SELECT COUNT(*) FROM fetch_attempts"), 1)
        self.assertFalse(blocked["complete"])
        self.assertEqual(self.state(blocked["scheduler_run_id"])["provider_transient"]["attempts"], 1)
        held = self.resume(
            blocked["scheduler_run_id"],
            now=blocked["next_resume_at"],
            call_override=fixture,
        )
        self.assertEqual(held["reason"], "paid_identity_hold")
        self.assertEqual(
            (held["status"], held["terminal_class"]),
            ("failed", "readiness_operator"),
        )
        self.assertEqual(len(calls), 1)
        self.assertEqual(self.scalar("SELECT COUNT(*) FROM fetch_attempts"), 1)
        self.assertIsNone(held.get("next_resume_at"))
        held_usage = self.scalar("SELECT COUNT(*) FROM provider_usage")
        held_dispatch_events = self.scalar(
            "SELECT COUNT(*) FROM paid_provider_dispatch_events"
        )
        again = self.resume(
            blocked["scheduler_run_id"],
            now=later(blocked["next_resume_at"]),
            call_override=lambda *args: self.fail("Held paid identity must not retry"),
        )
        self.assertEqual(
            (again["reason"], again["terminal_class"]),
            ("paid_identity_hold", "readiness_operator"),
        )
        self.assertEqual(len(calls), 1)
        self.assertEqual(self.scalar("SELECT COUNT(*) FROM provider_usage"), held_usage)
        self.assertEqual(
            self.scalar("SELECT COUNT(*) FROM paid_provider_dispatch_events"),
            held_dispatch_events,
        )

    def test_retry_after_defers_without_sleep_or_second_request(self):
        calls = []

        def fixture(operation, request):
            calls.append(operation)
            raise CaptureError("fixture limited", retryable=True, error_code="http_429", http_status=429,
                               retry_after_seconds=900, billed=False)

        blocked = self.scan(call_override=fixture)
        self.assertEqual(len(calls), 1)
        self.assertEqual(datetime.fromisoformat(blocked["next_resume_at"].replace("Z", "+00:00")),
                         datetime.fromisoformat(later(NOW, 900)))
        second = self.resume(
            blocked["scheduler_run_id"],
            now=blocked["next_resume_at"],
            call_override=fixture,
        )
        self.assertEqual(
            (second["status"], second["reason"], second["terminal_class"]),
            ("failed", "operation_blocked", "readiness_operator"),
        )
        self.assertEqual(len(calls), 1)
        self.assertIsNone(second.get("next_resume_at"))
        held_usage = self.scalar("SELECT COUNT(*) FROM provider_usage")
        held_dispatch_events = self.scalar(
            "SELECT COUNT(*) FROM paid_provider_dispatch_events"
        )
        held = self.resume(
            blocked["scheduler_run_id"],
            now=later(blocked["next_resume_at"]),
            call_override=fixture,
        )
        self.assertEqual(len(calls), 1)
        self.assertEqual(
            (held["status"], held["reason"], held["terminal_class"]),
            ("failed", "operation_blocked", "readiness_operator"),
        )
        self.assertEqual(self.scalar("SELECT COUNT(*) FROM provider_usage"), held_usage)
        self.assertEqual(
            self.scalar("SELECT COUNT(*) FROM paid_provider_dispatch_events"),
            held_dispatch_events,
        )

    def test_balance_circuit_prevents_subsequent_paid_attempts(self):
        calls = []

        def fixture(operation, request):
            calls.append(operation)
            raise CaptureError("fixture balance block", retryable=True, error_code="provider_balance_blocked",
                               http_status=402, billed=False, raw_response={"code": 402})

        blocked = self.scan(call_override=fixture)
        self.assertEqual(blocked["reason"], "provider_balance_blocked")
        again = self.resume(blocked["scheduler_run_id"], call_override=fixture)
        self.assertEqual((again["reason"], again["blocker"]),
                         ("provider_balance_blocked", "provider_balance_blocked"))
        self.assertEqual((again["status"], again["terminal_class"]), ("failed", "readiness_operator"))
        self.assertEqual(len(calls), 1)
        self.assertEqual(self.scalar("SELECT COUNT(*) FROM fetch_attempts"), 1)

    def test_expired_cursor_stops_without_rebuying_prior_pages(self):
        calls = []

        def fixture(operation, request):
            cursor = request["cursor"]
            calls.append(cursor)
            if len(calls) == 2:
                raise CaptureError("fixture cursor expired", retryable=False, error_code="http_400",
                                   http_status=400, billed=False, raw_response={"message": "max_cursor expired"})
            return result(dy_page([dy_item(cursor + 1)], more=cursor == 0, cursor=cursor + 1))

        blocked = self.scan(call_override=fixture)
        self.assertEqual(blocked["reason"], "cursor_expired")
        self.assertEqual(
            (blocked["status"], blocked["terminal_class"]),
            ("failed", "provider_transient"),
        )
        self.assertEqual(
            (
                self.state(blocked["scheduler_run_id"])["generation"],
                blocked["next_cursor"],
            ),
            (0, 1),
        )
        done = self.resume(blocked["scheduler_run_id"], call_override=fixture)
        self.assertEqual(calls, [0, 1])
        self.assertFalse(done["complete"])
        self.assertEqual(done["reason"], "cursor_expired")
        self.assertEqual(self.scalar("SELECT COUNT(*) FROM content_items"), 1)
        self.assertEqual(self.scalar(
            "SELECT COUNT(DISTINCT window_key) FROM fetch_slots WHERE stage='discovery'",
        ), 2)

    def test_existing_conflicting_identity_is_kept_without_new_facts(self):
        source = dy_item()
        record = upsert_content({
            "platform": "douyin", "platform_content_id": source["aweme_id"],
            "canonical_url": f"https://www.douyin.com/video/{source['aweme_id']}",
            "title": "existing protected", "account_uid": "other-uid", "published_at": PUBLISHED,
        }, db_path=self.db)
        done = self.scan(call_override=lambda *args: result(dy_page([source])))
        self.assertEqual(done["counts"]["existing"], 1)
        self.assertEqual(self.scalar("SELECT id FROM content_items"), record["id"])
        self.assertEqual(self.scalar("SELECT raw_account_uid FROM content_items"), "other-uid")
        self.assertEqual(self.scalar("SELECT COUNT(*) FROM content_metric_observations"), 0)
        self.assertEqual(self.manifest(done["last_manifest"])["items"][0]["reason"], "existing_identity_conflict")

    def test_frozen_window_and_scope_validation_is_before_requests(self):
        for values in ({"window_start": END}, {"window_start": "2026-08-21T00:00:00"},
                       {"purpose": "metrics"}, {"task_max_amount": 0}, {"task_id": ""}):
            with self.subTest(values=values), self.assertRaises(tikhub_scan.TikHubScanError):
                self.scan(**values, call_override=lambda *args: self.fail("Invalid scope must not dispatch"))
        self.assertEqual(self.scalar("SELECT COUNT(*) FROM fetch_attempts"), 0)


    def test_replay_preserves_capture_time_and_records_actual_application_time(self):
        with patch.object(tikhub_scan, "_apply", side_effect=RuntimeError("crash")), self.assertRaises(RuntimeError):
            self.scan(call_override=lambda *args: result(dy_page([dy_item()])))
        run_id = self.only_run()
        self.recover(run_id)
        applied_at = "2026-08-30T05:00:00Z"
        with patch.object(tikhub_scan, "now_utc", return_value=applied_at):
            done = self.resume(run_id, now=applied_at, call_override=lambda *args: self.fail("Replay must be free"))
        self.assertTrue(done["complete"])
        with connect(self.db) as connection:
            observed = connection.execute("SELECT captured_at,recorded_at FROM content_metric_observations").fetchone()
        self.assertEqual((observed["captured_at"], observed["recorded_at"]), (NOW, applied_at))

    def test_committed_manifest_damage_blocks_next_request(self):
        first = self.scan(max_pages=1, call_override=lambda *args: result(dy_page([dy_item()], more=True, cursor=1)))
        Path(first["last_manifest"]["path"]).write_bytes(b"broken receipt")
        blocked = self.resume(first["scheduler_run_id"], call_override=lambda *args: self.fail("Do not dispatch after receipt damage"))
        self.assertEqual(blocked["reason"], "manifest_integrity_error")
        self.assertEqual(blocked["pages"], 1)
        self.assertEqual(self.fetch_attempt_count(stage="discovery"), 1)

    def test_per_item_failure_rolls_back_its_partial_writes(self):
        def invalid_after_write(*args, **kwargs):
            upsert_content(*args, **kwargs)
            raise tikhub_scan.OperationError("fixture item cannot be accepted")

        with patch.object(tikhub_scan, "upsert_content", side_effect=invalid_after_write):
            done = self.scan(call_override=lambda *args: result(dy_page([dy_item()])))
        self.assertTrue(done["complete"])
        self.assertEqual(done["counts"]["unparseable"], 1)
        self.assertEqual(self.scalar("SELECT COUNT(*) FROM content_items"), 0)
        self.assertEqual(self.scalar("SELECT COUNT(*) FROM content_identities"), 0)

    def test_two_existing_records_are_never_merged_by_scan(self):
        for number in (1, 2):
            item = dy_item(number)
            upsert_content({"platform": "douyin", "platform_content_id": item["aweme_id"],
                            "canonical_url": f"https://www.douyin.com/video/{item['aweme_id']}",
                            "account_uid": DY_UID, "published_at": PUBLISHED}, db_path=self.db)
        with connect(self.db) as connection, transaction(connection):
            connection.execute("UPDATE content_items SET normalized_url_hash=(SELECT normalized_url_hash FROM content_items WHERE id=1) WHERE id=2")
        done = self.scan(call_override=lambda *args: result(dy_page([dy_item(1)])))
        self.assertTrue(done["complete"])
        self.assertEqual(self.scalar("SELECT COUNT(*) FROM content_items"), 2)
        self.assertEqual(done["counts"]["existing"], 1)
        self.assertEqual(self.scalar("SELECT COUNT(*) FROM content_metric_observations"), 0)


if __name__ == "__main__":
    unittest.main()
