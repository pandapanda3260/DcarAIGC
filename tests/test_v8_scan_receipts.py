"""Real scan receipts, frozen rosters and read-only discovery coverage.

Only supplier responses and clocks are mocked. SQLite, roster acceptance,
durable attempts, raw files, pagination and row dispositions stay real.
"""
from __future__ import annotations

import copy
import hashlib
import json
import tempfile
import unittest
from datetime import timedelta
from pathlib import Path
from unittest.mock import patch

from tests.roster_fixture import accept_roster
from v8 import (
    durable_runs,
    matrix_scan,
    pipeline,
    providers,
    runtime_receipts,
    scan_receipts,
    scan_terminals,
    tikhub_scan,
)
from v8.capture import CaptureError, ProviderResult
from v8.newrank_matrix import GATEWAY, MatrixConfig, NewrankMatrixClient
from v8.operations import upsert_content
from v8.source_routing import parse_time
from v8.storage import connect, initialize_database, transaction


DAY = "2026-08-28"
START = "2026-08-27T16:00:00Z"
END = "2026-08-28T16:00:00Z"
ROSTER_AT = "2026-08-28T16:00:00Z"
ROUND_AT = "2026-08-28T19:00:00Z"  # 2026-08-29 03:00 Beijing.
CUTOFF = "2026-08-29T04:00:00Z"
AFTER_CUTOFF = "2026-08-29T04:00:01Z"
PUBLISHED = "2026-08-28T04:00:00Z"
SCAN_START = "2026-08-21T16:00:00Z"
DY_UID = "12345678901"
XHS_UID = "0123456789abcdef01234567"
REFERENCE = "MS4wLjAB" + "a" * 64
CONFIG = MatrixConfig(
    api_url=GATEWAY, n_token="fixture-token", key_id="fixture-key", secret_key="fixture-sign",
)


def dy_item(number=1, **overrides):
    return {
        "aweme_id": str(7600000000000000000 + number), "desc": f"fixture {number}",
        "author": {"uid": DY_UID, "nickname": "fixture"},
        "create_time": int(parse_time(PUBLISHED).timestamp()),
        "video": {"play_addr": {"url_list": ["https://fixture.invalid/video.mp4"]}},
        "statistics": {"digg_count": 10, "comment_count": 2}, **overrides,
    }


def dy_page(items, *, more=False, cursor=None):
    data = {"aweme_list": items, "has_more": more}
    if cursor is not None:
        data["max_cursor"] = cursor
    return {"code": 200, "data": data}


def xhs_page(items):
    return {"code": 200, "data": {"code": 0, "success": True,
                                  "data": {"notes": items, "has_more": False}}}


def xhs_item(number=1):
    return {"note_id": f"{number:024x}", "note_card": {
        "title": "fixture note", "type": "normal", "time": int(parse_time(PUBLISHED).timestamp()),
        "user": {"user_id": XHS_UID}, "interact_info": {"liked_count": "5"},
    }}


def matrix_work(work_id="7379190309625810185", **overrides):
    return {
        "platType": 2, "awemeId": work_id, "uid": DY_UID, "nickname": "fixture",
        "createTime": "2026-08-28 12:00:00", "title": "fixture matrix work",
        "playCount": 123, "diggCount": 4, "commentCount": 0,
        "shareCount": 1, "favoriteCount": 2, "scrollId": [1234567890, work_id], **overrides,
    }


class ScanReceiptsTest(unittest.TestCase):
    def setUp(self):
        temporary = tempfile.TemporaryDirectory(prefix="dcar-scan-receipts-")
        self.addCleanup(temporary.cleanup)
        self.root = Path(temporary.name).resolve()
        self.db = self.root / "receipts.sqlite3"
        self.raw_root = self.root / "raw"
        self.reports_root = self.root / "reports"
        self.matrix_calls = []
        self.tikhub_calls = []
        self.monotonic = 0.0
        self.limiter = matrix_scan.MatrixRateLimiter(clock=lambda: self.monotonic, sleeper=self._advance)
        for target in ("v8.capture.now_utc", "v8.providers.now_utc", "v8.tikhub_scan.now_utc",
                       "v8.matrix_scan.now_utc", "v8.durable_runs.now_utc", "v8.pipeline.now_utc"):
            clock = patch(target, return_value=ROUND_AT)
            clock.start()
            self.addCleanup(clock.stop)
        for target in ("v8.capture.RAW_ROOT", "v8.tikhub_scan.RAW_ROOT"):
            raw_root = patch(target, self.raw_root)
            raw_root.start()
            self.addCleanup(raw_root.stop)
        no_network = patch("urllib.request.urlopen", side_effect=AssertionError("Network forbidden in receipt tests"))
        self.network = no_network.start()
        self.addCleanup(no_network.stop)
        key = patch.object(providers, "_load_key", return_value="fixture-only")
        key.start()
        self.addCleanup(key.stop)
        with connect(self.db) as connection:
            initialize_database(connection)
            for platform, uid, enabled in (
                ("douyin", DY_UID, 1), ("xiaohongshu", XHS_UID, 1),
                ("douyin", "12345678902", 0), ("douyin", "12345678903", 1),
            ):
                account_id = connection.execute(
                    "INSERT INTO accounts(phone,enabled,created_at,updated_at) VALUES ('',?,?,?)",
                    (enabled, ROSTER_AT, ROSTER_AT),
                ).lastrowid
                connection.execute(
                    "INSERT INTO account_platform_identities(account_id,platform,uid,created_at,updated_at) "
                    "VALUES (?,?,?,?,?)", (account_id, platform, uid, ROSTER_AT, ROSTER_AT),
                )
            connection.execute(
                "INSERT INTO account_provider_references(account_identity_id,provider,reference_kind,"
                "reference_value,created_at,updated_at) VALUES (1,'TikHub','sec_user_id',?,?,?)",
                (REFERENCE, ROSTER_AT, ROSTER_AT),
            )
        self.snapshot = None

    def tearDown(self):
        self.network.assert_not_called()

    def _advance(self, delay):
        self.monotonic += delay

    def _accept(self, ids=(1, 2, 3), *, at=ROSTER_AT):
        with connect(self.db) as connection, transaction(connection):
            self.snapshot = accept_roster(connection, ids, accepted_at=at)
        return self.snapshot

    def _activate(self, ids=(1, 2, 3)):
        snapshot = self._accept(ids)
        details = {"contract_version": pipeline.PIPELINE_VERSION, "mode": "active",
                   "cutover_at": ROSTER_AT, "roster_snapshot_id": snapshot["id"],
                   "roster_snapshot_hash": snapshot["members_sha256"]}
        with connect(self.db) as connection, transaction(connection):
            connection.execute(
                "INSERT INTO scheduler_runs(job_id,scheduled_for,status,started_at,completed_at,details_json) "
                "VALUES (?,?,'succeeded',?,?,?)",
                (pipeline.ACTIVATION_JOB, ROSTER_AT, ROSTER_AT, ROSTER_AT, json.dumps(details)),
            )
        return snapshot

    def _system_activate(self, ids=(1, 2)):
        from v8.profile_activations import TIKHUB_PROFILE, append_activation
        from v8.paid_drain import issue_activation_permit_in_transaction

        with connect(self.db) as connection, transaction(connection):
            rows = [dict(row) for row in connection.execute(
                "SELECT id,platform,uid FROM account_platform_identities "
                f"WHERE id IN ({','.join('?' for _ in ids)}) ORDER BY id",
                tuple(ids),
            )]
            keys = sorted(f"uid:{row['platform']}:{row['uid']}" for row in rows)
            source = json.dumps(keys, sort_keys=True, separators=(",", ":")).encode()
            source_path = self.root / "system-roster.json"
            source_path.write_bytes(source)
            source_path.chmod(0o600)
            members_sha256 = hashlib.sha256(
                json.dumps(keys, ensure_ascii=False, sort_keys=True,
                           separators=(",", ":"), allow_nan=False).encode()
            ).hexdigest()
            cursor = connection.execute(
                """INSERT INTO account_roster_snapshots(
                       source_family,source_type,scope_key,scope_json,
                       source_instance_id,source_captured_at,accepted_at,
                       declared_count,member_count,members_sha256,source_sha256,
                       source_path,contract_version,metadata_json)
                   VALUES ('system','system_managed','isolated-system','{}',?,?,?,?,
                           ?,?,?,?,'system-managed-roster-v1','{}')""",
                (
                    "system-fixture", ROSTER_AT, ROSTER_AT, len(rows), len(rows),
                    members_sha256, hashlib.sha256(source).hexdigest(),
                    str(source_path),
                ),
            )
            snapshot_id = int(cursor.lastrowid)
            connection.executemany(
                """INSERT INTO account_roster_members(
                       snapshot_id,account_identity_id,platform,member_key,uid,
                       matrix_account_id,profile_ref,monitoring_status,
                       authorization_status,metadata_json)
                   VALUES (?,?,?,?,?,NULL,NULL,'unknown','unknown','{}')""",
                [
                    (
                        snapshot_id, row["id"], row["platform"],
                        f"uid:{row['platform']}:{row['uid']}", row["uid"],
                    )
                    for row in rows
                ],
            )
            activation = append_activation(
                connection,
                profile_id=TIKHUB_PROFILE,
                roster_snapshot_id=snapshot_id,
                roster_members_sha256=members_sha256,
                effective_at=ROSTER_AT,
                build_receipt_sha256=hashlib.sha256(b"system-build").hexdigest(),
                actor="test-fixture",
                reason="activate system fixture",
                created_at=ROSTER_AT,
            )
            issue_activation_permit_in_transaction(
                connection,
                activation_id=int(activation["activation_id"]),
                drain_id=f"system-fixture:{activation['activation_id']}",
                source_activation_id=int(activation["activation_id"]),
                business_day="2026-08-29",
                planned_effective_at=activation["effective_at"],
                build_receipt_sha256=hashlib.sha256(b"system-build").hexdigest(),
                runtime_root_receipt_sha256=hashlib.sha256(b"system-runtime").hexdigest(),
                now=ROSTER_AT,
            )
            self.snapshot = dict(connection.execute(
                "SELECT * FROM account_roster_snapshots WHERE id=?", (snapshot_id,),
            ).fetchone())
        return self.snapshot

    def _round(self, *, pages=None, finish_at=None):
        pages = pages if pages is not None else {
            "douyin_user_posts": [dy_page([])], "xiaohongshu_user_posts": [xhs_page([])],
        }
        remaining = {operation: iter(values) for operation, values in pages.items()}

        def supplier(operation, request):
            self.tikhub_calls.append((operation, copy.deepcopy(request)))
            self.assertIn(operation, remaining, "Only frozen account-list operations may dispatch")
            try:
                body = next(remaining[operation])
            except StopIteration:
                self.fail(f"Unexpected extra supplier request: {operation}")
            return ProviderResult({}, copy.deepcopy(body), 200, True)

        finish = tikhub_scan.finish_run

        def terminal_receipt(claim, **kwargs):
            if isinstance(finish_at, dict):
                scope = json.loads(self._row(claim.scheduler_run_id)["details_json"])["identity"]
                kwargs["now"] = finish_at.get(scope["identity_id"], kwargs["now"])
            elif finish_at is not None:
                kwargs["now"] = finish_at
            return finish(claim, **kwargs)

        with patch.object(tikhub_scan, "finish_run", side_effect=terminal_receipt):
            result = pipeline.dispatch(
                "tikhub_reconcile", db_path=self.db, reports_root=self.reports_root,
                at=ROUND_AT, call_override=supplier,
            )
            child_ids = [item["scheduler_run_id"] for item in result.get("scans", [])]
            resume_at = parse_time(ROUND_AT)
            for _attempt in range(50):
                parent = durable_runs.get_run(result["round_run_id"], db_path=self.db)
                if parent["details"].get("complete"):
                    break
                resume_at += timedelta(seconds=301)
                pipeline.resume_due_work(
                    at=pipeline._iso(resume_at), db_path=self.db,
                    reports_root=self.reports_root, call_override=supplier,
                )
            else:
                self.fail("TikHub receipt fixture did not finish within fifty fair page turns")
        parent = durable_runs.get_run(result["round_run_id"], db_path=self.db)
        result.update(
            status=parent["status"], complete=bool(parent["details"].get("complete")),
            scans=[tikhub_scan._result(run_id, db_path=self.db) for run_id in child_ids],
        )
        self.assertTrue(result.get("complete"), result)
        self.assertEqual(result["status"], "succeeded")
        return result

    def _matrix(self, platform="douyin", *, pages=None, now=ROUND_AT, max_pages=20,
                start=START, end=END, overall_start=None, overall_end=None):
        self.assertIsNotNone(self.snapshot)
        responses = iter([[]] if pages is None else pages)

        def transport(request, timeout):
            body = json.loads(request.data)
            self.matrix_calls.append({"platform": platform, "path": body["pathName"],
                                      "query": json.loads(body["reqJson"])})
            try:
                rows = next(responses)
            except StopIteration:
                self.fail("Unexpected extra Matrix page request")
            return 200, json.dumps({"code": 0, "data": json.dumps(rows)}).encode()

        client = NewrankMatrixClient(CONFIG, transport=transport, clock=lambda: parse_time(now))
        return matrix_scan.run_matrix_scan(
            "works", platform, purpose="daily", start_at=start, end_at=end,
            overall_start_at=overall_start, overall_end_at=overall_end,
            roster_snapshot_id=self.snapshot["id"], roster_snapshot_hash=self.snapshot["members_sha256"],
            db_path=self.db, raw_root=self.raw_root, client=client, rate_limiter=self.limiter,
            now=now, max_pages=max_pages,
        )

    def _runtime_slice(self, index, platform, *, correct_overall=True, pages=None,
                       max_pages=20):
        end = parse_time(END)
        return self._matrix(
            platform, start=pipeline._iso(end - timedelta(days=index + 1)),
            end=pipeline._iso(end - timedelta(days=index)),
            overall_start=pipeline._iso(end - timedelta(days=30)) if correct_overall else None,
            overall_end=END if correct_overall else None, pages=pages,
            max_pages=max_pages,
        )

    def _remaining_runtime_slices(self, *, first_index=1):
        for index in range(first_index, 30):
            for platform in ("douyin", "xiaohongshu"):
                self._runtime_slice(index, platform)

    def _runtime(self, *, at=CUTOFF):
        with connect(self.db) as connection:
            before = connection.total_changes
            result = scan_receipts.runtime_coverage(connection, at=at)
            self.assertEqual(connection.total_changes, before)
            return result

    def _fake_succeeded(self, job, scope):
        claim = durable_runs.claim_run(job, scope, db_path=self.db, now=ROUND_AT)
        self.assertIsNotNone(claim)
        with connect(self.db) as connection, transaction(connection):
            durable_runs.checkpoint(connection, claim, {"complete": True}, now=ROUND_AT)
        durable_runs.finish_run(claim, status="succeeded", db_path=self.db, now=ROUND_AT)
        return claim.scheduler_run_id

    def _closed_day(self, *, finish_at=None, with_rows=False):
        self._activate()
        pages = {
            "douyin_user_posts": [dy_page([dy_item(1)], more=True, cursor=101), dy_page([dy_item(2)])],
            "xiaohongshu_user_posts": [xhs_page([xhs_item()])],
        } if with_rows else None
        round_result = self._round(pages=pages, finish_at=finish_at)
        matrix_dy = self._runtime_slice(
            0, "douyin", pages=[[matrix_work()], []] if with_rows else None,
        )
        matrix_xhs = self._runtime_slice(0, "xiaohongshu")
        self._remaining_runtime_slices()
        self.assertTrue(matrix_dy["complete"], matrix_dy)
        self.assertTrue(matrix_xhs["complete"], matrix_xhs)
        return round_result, matrix_dy, matrix_xhs

    def _coverage(self, *, start=DAY, end=DAY, cutoff=CUTOFF):
        with connect(self.db) as connection:
            before = connection.total_changes
            result = scan_receipts.coverage(connection, period_start=start, period_end=end, cutoff_at=cutoff)
            self.assertEqual(connection.total_changes, before, "Coverage verification must be read-only")
            return result

    def _row(self, run_id):
        with connect(self.db) as connection:
            return dict(connection.execute("SELECT * FROM scheduler_runs WHERE id=?", (run_id,)).fetchone())

    def _verify(self, row, *, cutoff=CUTOFF):
        with connect(self.db) as connection:
            before = connection.total_changes
            result = scan_receipts.verify_scan(connection, row, cutoff_at=cutoff)
            self.assertEqual(connection.total_changes, before)
            return result

    def _changed_head(self, run_id, change, *, checkpoint_change=None):
        """Rehash a forged manifest: hash checks alone must not prove pagination."""
        row = self._row(run_id)
        details = json.loads(row["details_json"])
        head = details["checkpoint"]["last_manifest"]
        value = json.loads(Path(head["path"]).read_bytes())
        change(value)
        details["checkpoint"]["last_manifest"] = self._forged_manifest(value)
        if checkpoint_change:
            checkpoint_change(details["checkpoint"])
        row["details_json"] = json.dumps(details)
        return row

    def _forged_manifest(self, value):
        body = json.dumps(value, sort_keys=True, separators=(",", ":")).encode()
        digest = hashlib.sha256(body).hexdigest()
        path = self.root / f"forged-{digest}.manifest.json"
        path.write_bytes(body)
        return {"path": str(path), "sha256": digest, "byte_size": len(body)}

    def _v2_range_start_scan(self, *, task_id="receipt-v2-range-start"):
        self._activate()
        old = int(parse_time(SCAN_START).timestamp()) - 1
        pages = iter([
            dy_page([
                dy_item(71, create_time=old, is_top=False),
                dy_item(72, create_time=int(parse_time(PUBLISHED).timestamp()), is_top=True),
            ], more=True, cursor=101),
            dy_page([
                dy_item(73, create_time=old - 1, is_top=False),
                dy_item(74, create_time=int(parse_time(PUBLISHED).timestamp()), is_top=True),
            ], more=True, cursor=202),
        ])
        calls = []

        def supplier(operation, request):
            calls.append((operation, request["cursor"]))
            return ProviderResult({}, next(pages), 200, True)

        result = tikhub_scan.run_account_scan(
            1, window_start=SCAN_START, window_end=END, purpose="reconcile",
            roster_snapshot_id=self.snapshot["id"],
            roster_snapshot_hash=self.snapshot["members_sha256"],
            db_path=self.db, raw_root=self.raw_root, now=ROUND_AT,
            task_id=task_id, call_override=supplier,
        )
        self.assertTrue(result["complete"], result)
        self.assertEqual(calls, [("douyin_user_posts", 0), ("douyin_user_posts", 101)])
        self.assertEqual(result["completion_reason"], "range_start_reached")
        return result

    def _assert_rejected(self, row):
        with self.assertRaises((ValueError, RuntimeError)):
            self._verify(row)

    def test_real_round_and_both_sources_close_with_every_raw_page_traceable(self):
        round_result, matrix_dy, matrix_xhs = self._closed_day(with_rows=True)
        result = self._coverage()
        self.assertTrue(result["complete"])
        self.assertTrue(result["scan_traceable"])
        self.assertTrue(result["roster_evidence_valid"])
        self.assertEqual(result["scan_errors"], {})
        self.assertEqual(len(result["scan_references"]), 62)
        self.assertEqual(result["discovery_coverage"]["percentage"], 100.0)
        self.assertEqual(result["discovery_coverage"]["eligible_identity_occurrence_count"], 2)
        day = result["days"][0]
        self.assertEqual(day["eligible_identity_ids"], [1, 2])
        self.assertEqual(day["covered_identity_ids"], [1, 2])
        self.assertEqual(day["round_run_id"], round_result["round_run_id"])
        self.assertEqual(len(day["matrix_run_ids"]), 60)
        self.assertTrue(
            {matrix_dy["scheduler_run_id"], matrix_xhs["scheduler_run_id"]}
            <= set(day["matrix_run_ids"])
        )
        self.assertCountEqual(day["tikhub_run_ids"], [item["scheduler_run_id"] for item in round_result["scans"]])
        proofs = {item["run_id"]: item for item in result["scan_references"]}
        self.assertEqual(proofs[matrix_dy["scheduler_run_id"]]["counts"],
                         {"known": 0, "new": 1, "quarantined": 0, "unparseable": 0})
        tik_dy = next(item for item in round_result["scans"] if item["pages"] == 2)
        self.assertEqual(proofs[tik_dy["scheduler_run_id"]]["counts"],
                         {"existing": 0, "inserted": 2, "quarantined": 0, "unparseable": 0})
        references = [ref for proof in proofs.values() for ref in proof["references"]]
        self.assertEqual(len(references), 64)
        self.assertEqual(len({ref["raw_id"] for ref in references}), 64)
        for ref in references:
            body = Path(ref["raw_path"]).read_bytes()
            self.assertTrue(Path(ref["raw_path"]).is_relative_to(self.root))
            self.assertEqual(len(body), ref["raw_byte_size"])
            self.assertEqual(hashlib.sha256(body).hexdigest(), ref["raw_sha256"])
            manifest = Path(ref["manifest"]["path"]).read_bytes()
            self.assertEqual(hashlib.sha256(manifest).hexdigest(), ref["manifest"]["sha256"])
        self.assertEqual([request["cursor"] for operation, request in self.tikhub_calls if operation == "douyin_user_posts"], [0, 101])
        self.assertEqual(self.matrix_calls[1]["query"]["scrollId"], matrix_work()["scrollId"])

    def test_four_way_dispositions_are_conserved_from_real_scans_not_content_totals(self):
        self._activate()
        upsert_content({"platform": "douyin", "platform_content_id": "777000", "title": "keep",
                        "canonical_url": "https://www.douyin.com/video/777000", "account_uid": DY_UID,
                        "published_at": PUBLISHED}, db_path=self.db)
        matrix_rows = [matrix_work("777000"), matrix_work(), matrix_work("999000", uid="998877665544"),
                       {"platType": 2, "uid": DY_UID, "scrollId": [3, "invalid"]},
                       matrix_work(scrollId=[4, matrix_work()["awemeId"]])]
        matrix_result = self._runtime_slice(0, "douyin", pages=[matrix_rows, []])
        tik_rows = [dy_item(1), dy_item(1), dy_item(2, author={"uid": "999888777"}), {}, "invalid"]
        round_result = self._round(pages={"douyin_user_posts": [dy_page(tik_rows)],
                                         "xiaohongshu_user_posts": [xhs_page([])]})
        self._runtime_slice(0, "xiaohongshu")
        self._remaining_runtime_slices()
        result = self._coverage()
        self.assertTrue(result["complete"])
        proofs = {item["run_id"]: item for item in result["scan_references"]}
        self.assertEqual(proofs[matrix_result["scheduler_run_id"]]["counts"],
                         {"known": 2, "new": 1, "quarantined": 1, "unparseable": 1})
        tik_id = round_result["scans"][0]["scheduler_run_id"]
        self.assertEqual(proofs[tik_id]["counts"],
                         {"existing": 1, "inserted": 1, "quarantined": 1, "unparseable": 2})
        self.assertEqual(sum(proofs[tik_id]["counts"].values()), len(tik_rows))
        with connect(self.db) as connection:
            self.assertEqual(connection.execute("SELECT COUNT(*) FROM content_items").fetchone()[0], 3)

    def test_frozen_roster_and_eligible_ids_survive_later_roster_and_enable_changes(self):
        round_result, _, _ = self._closed_day()
        frozen = copy.deepcopy(self.snapshot)
        with connect(self.db) as connection, transaction(connection):
            connection.execute("UPDATE accounts SET enabled=0 WHERE id=1")
            connection.execute("UPDATE accounts SET enabled=1 WHERE id=3")
        latest = self._accept((2, 3, 4), at="2026-08-29T00:00:00Z")
        self.assertNotEqual(frozen["id"], latest["id"])
        result = self._coverage()
        self.assertTrue(result["complete"])
        day = result["days"][0]
        self.assertEqual((day["roster_snapshot_id"], day["roster_snapshot_hash"]),
                         (frozen["id"], frozen["members_sha256"]))
        self.assertEqual(day["eligible_identity_ids"], [1, 2])
        self.assertEqual(day["covered_identity_ids"], [1, 2])
        frozen_round = json.loads(self._row(round_result["round_run_id"])["details_json"])["identity"]
        self.assertEqual(frozen_round["beijing_day"], "2026-08-29")
        self.assertEqual(frozen_round["eligible_identity_ids"], [1, 2])
        self.assertEqual(result["discovery_coverage"]["eligible_identity_occurrence_count"], 2)

    def test_completion_after_cutoff_never_counts_even_when_raw_predates_cutoff(self):
        round_result, _, _ = self._closed_day(finish_at=AFTER_CUTOFF)
        result = self._coverage()
        self.assertFalse(result["complete"])
        self.assertFalse(result["scan_traceable"])
        self.assertTrue(result["days"][0]["known"])
        self.assertEqual(result["days"][0]["covered_identity_ids"], [])
        self.assertEqual(result["discovery_coverage"]["percentage"], 0.0)
        self.assertEqual(result["pipeline_observation"]["pipeline_gap_dates"], [DAY])
        for item in round_result["scans"]:
            self.assertEqual(result["scan_errors"][str(item["scheduler_run_id"])], "scan_not_complete_at_cutoff")
        late = self._coverage(cutoff=AFTER_CUTOFF)
        self.assertTrue(late["complete"])
        self.assertEqual(late["discovery_coverage"]["percentage"], 100.0)

    def test_completion_exactly_at_cutoff_is_accepted(self):
        self._closed_day(finish_at=CUTOFF)
        self.assertTrue(self._coverage()["complete"])

    def test_matrix_partial_page_budget_is_a_gap_not_terminal_exhaustion(self):
        self._activate()
        self._round()
        partial = self._runtime_slice(
            0, "douyin", pages=[[matrix_work()]], max_pages=1,
        )
        self.assertEqual(partial["status"], "partial")
        self.assertFalse(partial["complete"])
        self._runtime_slice(0, "xiaohongshu")
        result = self._coverage()
        self.assertFalse(result["complete"])
        self.assertFalse(result["scan_traceable"])
        self.assertEqual(result["days"][0]["covered_identity_ids"], [1, 2])
        self.assertEqual(result["days"][0]["reason"], "scan_pagination_or_provider_gap")
        self.assertEqual(result["scan_errors"][str(partial["scheduler_run_id"])], "scan_not_complete_at_cutoff")

    def test_missing_matrix_platform_cannot_be_hidden_by_full_tikhub_coverage(self):
        self._activate()
        self._round()
        self._runtime_slice(0, "douyin")
        result = self._coverage()
        self.assertFalse(result["complete"])
        self.assertEqual(result["discovery_coverage"]["percentage"], 100.0)
        self.assertFalse(result["discovery_coverage"]["complete"])
        self.assertEqual(result["pipeline_observation"]["pipeline_gap_dates"], [DAY])

    def test_legacy_daily_capture_success_does_not_supply_a_new_discovery_round(self):
        self._accept()
        with connect(self.db) as connection, transaction(connection):
            connection.execute(
                "INSERT INTO scheduler_runs(job_id,scheduled_for,status,started_at,completed_at,details_json) "
                "VALUES ('daily_capture',?,'succeeded',?,?,?)",
                (ROUND_AT, ROUND_AT, ROUND_AT, json.dumps({"complete": True, "coverage_percent": 100,
                    "covered_identity_ids": [1, 2], "eligible_identity_ids": [1, 2]})),
            )
        result = self._coverage()
        self.assertFalse(result["complete"])
        self.assertEqual(result["discovery_coverage"]["status"], "unknown")
        self.assertIsNone(result["discovery_coverage"]["percentage"])
        self.assertEqual(result["days"][0]["reason"], "historical_roster_scope_unknown")
        self.assertEqual(result["pipeline_observation"]["legacy_unobserved_dates"], [DAY])
        self.assertEqual(result["scan_references"], [])

    def test_no_historical_scope_is_unknown_not_a_zero_or_current_roster_backfill(self):
        self._closed_day()
        result = self._coverage(start="2026-08-27")
        self.assertFalse(result["complete"])
        self.assertFalse(result["roster_evidence_valid"])
        self.assertEqual([day["known"] for day in result["days"]], [False, True])
        self.assertEqual(result["days"][0]["eligible_identity_ids"], [])
        self.assertEqual(result["days"][0]["reason"], "historical_roster_scope_unknown")
        self.assertEqual(result["discovery_coverage"]["status"], "unknown")
        self.assertIsNone(result["discovery_coverage"]["percentage"])
        self.assertEqual(result["discovery_coverage"]["observed_occurrence_count"], 1)
        self.assertEqual(result["discovery_coverage"]["expected_occurrence_count"], 2)
        self.assertEqual(result["pipeline_observation"]["legacy_unobserved_dates"], ["2026-08-27"])
        self.assertEqual(result["pipeline_observation"]["zero_content_dates"], [])

    def test_same_day_complete_empty_accepted_roster_is_explicitly_not_applicable(self):
        self._activate(())
        result = self._round(pages={})
        self.assertEqual(result["scans"], [])
        self.assertEqual(self.tikhub_calls, [])
        self._runtime_slice(0, "douyin")
        self._runtime_slice(0, "xiaohongshu")
        self._remaining_runtime_slices()
        coverage = self._coverage()
        self.assertTrue(coverage["complete"])
        self.assertTrue(coverage["days"][0]["known"])
        detail = coverage["discovery_coverage"]
        self.assertEqual(detail["status"], "not_applicable")
        self.assertEqual(detail["eligible_identity_occurrence_count"], 0)
        self.assertEqual(detail["covered_identity_occurrence_count"], 0)
        self.assertIsNone(detail["percentage"])
        self.assertEqual(detail["reason"], "已验证完整空名册，无适用采集账号")

    def test_empty_current_roster_without_a_frozen_round_is_unknown(self):
        self._accept(())
        result = self._coverage()
        self.assertFalse(result["complete"])
        self.assertEqual(result["discovery_coverage"]["status"], "unknown")
        self.assertIsNone(result["discovery_coverage"]["percentage"])
        self.assertEqual(result["days"][0]["reason"], "historical_roster_scope_unknown")

    def test_duplicate_exact_profile_day_anchor_is_ambiguous_not_last_write_wins(self):
        self._activate((1, 2))
        round_result = self._round()
        identity = json.loads(
            self._row(round_result["round_run_id"])["details_json"]
        )["identity"]
        self._fake_succeeded(
            "pipeline_round:tikhub_reconcile",
            {**identity, "duplicate_anchor_fixture": True},
        )
        result = self._coverage()
        self.assertFalse(result["days"][0]["known"])
        self.assertEqual(
            result["days"][0]["reason"], "profile_day_anchor_ambiguous"
        )

    def test_nonempty_roster_with_empty_pages_is_full_scanned_coverage_not_missing(self):
        self._closed_day()
        result = self._coverage()
        self.assertTrue(result["complete"])
        self.assertEqual(result["discovery_coverage"]["eligible_identity_occurrence_count"], 2)
        self.assertEqual(result["discovery_coverage"]["status"], "available")
        self.assertEqual(result["discovery_coverage"]["percentage"], 100.0)
        self.assertTrue(all(sum(item["counts"].values()) == 0 for item in result["scan_references"]))
        self.assertEqual(result["pipeline_observation"]["legacy_unobserved_dates"], [])

    def test_terminal_blocked_member_is_accounted_but_readiness_blocks_publication(self):
        self._activate((1, 2))
        with connect(self.db) as connection:
            active = pipeline.activation(connection, at=ROUND_AT)
        self.assertIsNotNone(active)
        round_identity = {
            "pipeline_version": pipeline.PIPELINE_VERSION,
            "beijing_day": "2026-08-29",
            "round_id": "tikhub_reconcile:03:00",
            "registration_id": "tikhub_reconcile",
            "job_id": "tikhub_reconcile",
            "scheduled_at": ROUND_AT,
            "roster_snapshot_id": self.snapshot["id"],
            "roster_snapshot_hash": self.snapshot["members_sha256"],
            "activation_id": active["activation_id"],
            "activation_sha256": active["activation_sha256"],
            "profile_id": active["profile_id"],
            "eligible_identity_ids": [1, 2],
        }
        round_claim = durable_runs.claim_run(
            "pipeline_round:tikhub_reconcile",
            round_identity,
            db_path=self.db,
            now=ROUND_AT,
        )
        self.assertIsNotNone(round_claim)
        with connect(self.db) as connection, transaction(connection):
            durable_runs.checkpoint(
                connection, round_claim, {"complete": True}, now=ROUND_AT
            )
        durable_runs.finish_run(
            round_claim, status="succeeded", db_path=self.db, now=ROUND_AT
        )

        calls = []

        def unavailable(_operation, _request):
            calls.append("http_404")
            raise CaptureError(
                "fixture account route unavailable",
                retryable=False,
                error_code="http_404",
                http_status=404,
                billed=False,
                raw_response={"code": 404},
            )

        blocked = tikhub_scan.run_account_scan(
            1,
            window_start=SCAN_START,
            window_end=END,
            purpose="reconcile",
            roster_snapshot_id=self.snapshot["id"],
            roster_snapshot_hash=self.snapshot["members_sha256"],
            db_path=self.db,
            raw_root=self.raw_root,
            now=ROUND_AT,
            task_id="receipt-terminal-readiness",
            call_override=unavailable,
        )
        succeeded = tikhub_scan.run_account_scan(
            2,
            window_start=SCAN_START,
            window_end=END,
            purpose="reconcile",
            roster_snapshot_id=self.snapshot["id"],
            roster_snapshot_hash=self.snapshot["members_sha256"],
            db_path=self.db,
            raw_root=self.raw_root,
            now=ROUND_AT,
            task_id="receipt-terminal-success",
            call_override=lambda _operation, _request: ProviderResult(
                {}, xhs_page([]), 200, True
            ),
        )
        self.assertEqual(
            (blocked["status"], blocked["terminal_class"]),
            ("failed", "readiness_operator"),
        )
        self.assertTrue(succeeded["complete"])
        # A later, softer terminal for the same frozen obligation must not
        # overwrite the earlier readiness veto merely because its run id is
        # newer.
        blocked_scope = json.loads(
            self._row(blocked["scheduler_run_id"])["details_json"]
        )["identity"]
        later_scope = {**blocked_scope, "task_id": "receipt-terminal-deadline"}
        later = durable_runs.claim_run(
            "tikhub_reconcile", later_scope, db_path=self.db, now=ROUND_AT
        )
        self.assertIsNotNone(later)
        durable_runs.finish_run(
            later,
            status="failed",
            db_path=self.db,
            now=ROUND_AT,
            summary=scan_terminals.terminal_summary(
                reason="business_day_expired", terminal_class="deadline"
            ),
        )
        self._runtime_slice(0, "douyin")
        self._runtime_slice(0, "xiaohongshu")
        self._remaining_runtime_slices()

        result = self._coverage()
        day = result["days"][0]
        self.assertEqual(day["succeeded_identity_ids"], [2])
        self.assertEqual(day["blocked_identity_ids"], [1])
        self.assertEqual(day["not_applicable_identity_ids"], [])
        self.assertEqual(day["accounted_identity_ids"], [1, 2])
        self.assertEqual(day["required_identity_ids"], [1, 2])
        self.assertEqual((day["accounted_percentage"], day["success_percentage"]), (100.0, 50.0))
        self.assertFalse(result["complete"])
        self.assertFalse(result["partial_publishable"])
        self.assertFalse(result["scan_traceable"])
        self.assertEqual(
            day["terminal_blockers"]["1"]["terminal_class"],
            "readiness_operator",
        )
        self.assertEqual(
            day["terminal_blockers"]["1"]["terminal_classes"],
            ["deadline", "readiness_operator"],
        )
        self.assertTrue(day["terminal_blockers"]["1"]["publication_blocker"])
        detail = result["discovery_coverage"]
        self.assertEqual(
            (
                detail["succeeded_identity_occurrence_count"],
                detail["blocked_identity_occurrence_count"],
                detail["not_applicable_identity_occurrence_count"],
                detail["accounted_identity_occurrence_count"],
                detail["required_identity_occurrence_count"],
            ),
            (1, 1, 0, 2, 2),
        )
        with connect(self.db) as connection:
            proof = scan_receipts.verify_terminal_scan(
                connection, self._row(blocked["scheduler_run_id"]), cutoff_at=CUTOFF
            )
        self.assertEqual(proof["terminal_class"], "readiness_operator")
        forged = self._row(blocked["scheduler_run_id"])
        forged_details = json.loads(forged["details_json"])
        forged_details["summary"]["accounted"] = False
        forged["details_json"] = json.dumps(forged_details)
        with connect(self.db) as connection, self.assertRaisesRegex(
            ValueError, "scan_terminal_summary_invalid"
        ):
            scan_receipts.verify_terminal_scan(
                connection, forged, cutoff_at=CUTOFF
            )
        unpaired = self._row(blocked["scheduler_run_id"])
        unpaired["details_json"] += " "
        with connect(self.db) as connection, self.assertRaisesRegex(
            ValueError, "scan_terminal_attempt_mismatch"
        ):
            scan_receipts.verify_terminal_scan(
                connection, unpaired, cutoff_at=CUTOFF
            )
        again = tikhub_scan.resume_account_scan(
            blocked["scheduler_run_id"],
            db_path=self.db,
            raw_root=self.raw_root,
            now=CUTOFF,
            call_override=lambda *_args: self.fail("Terminal receipt must be zero-network"),
        )
        self.assertEqual(again["status"], "failed")
        self.assertEqual(calls, ["http_404"])

    def test_partial_publishable_coverage_keeps_verified_scans_traceable(self):
        self._closed_day()
        actual_decision = scan_receipts.coverage_decision

        def partial_decision(**kwargs):
            decision = actual_decision(**kwargs)
            return {**decision, "complete": False, "partial_publishable": True}

        with patch.object(
            scan_receipts, "coverage_decision", side_effect=partial_decision
        ):
            result = self._coverage()

        self.assertFalse(result["complete"])
        self.assertTrue(result["partial_publishable"])
        self.assertTrue(result["scan_traceable"])
        self.assertEqual(result["scan_errors"], {})

    def test_roster_source_tampering_fails_closed_without_substituting_latest_accounts(self):
        self._closed_day()
        with connect(self.db) as connection:
            path = Path(connection.execute("SELECT source_path FROM account_roster_snapshots WHERE id=?",
                                           (self.snapshot["id"],)).fetchone()[0])
        path.write_bytes(path.read_bytes() + b" ")
        result = self._coverage()
        self.assertFalse(result["complete"])
        self.assertFalse(result["roster_evidence_valid"])
        self.assertEqual(result["days"][0]["reason"], "roster_source_missing_or_changed")
        self.assertIsNone(result["discovery_coverage"]["percentage"])

    def test_raw_body_tampering_rejects_previously_successful_scan(self):
        _, matrix_dy, _ = self._closed_day()
        proof = self._verify(self._row(matrix_dy["scheduler_run_id"]))
        path = Path(proof["references"][0]["raw_path"])
        path.write_bytes(path.read_bytes() + b" ")
        result = self._coverage()
        self.assertFalse(result["complete"])
        self.assertEqual(result["scan_errors"][str(matrix_dy["scheduler_run_id"])], "scan_raw_hash_mismatch")

    def test_malformed_raw_database_sha_is_a_scan_integrity_error(self):
        _, matrix_dy, _ = self._closed_day()
        proof = self._verify(self._row(matrix_dy["scheduler_run_id"]))
        raw_id = int(proof["references"][0]["raw_id"])
        forged_row = self._changed_head(
            matrix_dy["scheduler_run_id"],
            lambda value: value.update(raw_sha256="invalid"),
        )
        with connect(self.db) as connection, transaction(connection):
            connection.execute(
                "UPDATE provider_raw_responses SET sha256='invalid' WHERE id=?",
                (raw_id,),
            )

        with self.assertRaisesRegex(ValueError, "scan_raw_hash_mismatch"):
            self._verify(forged_row)

    def test_missing_raw_file_is_not_success_and_does_not_trigger_refetch(self):
        round_result, _, _ = self._closed_day()
        run_id = round_result["scans"][0]["scheduler_run_id"]
        proof = self._verify(self._row(run_id))
        Path(proof["references"][0]["raw_path"]).unlink()
        before = (len(self.tikhub_calls), len(self.matrix_calls))
        result = self._coverage()
        self.assertFalse(result["complete"])
        self.assertEqual(result["scan_errors"][str(run_id)], "scan_raw_missing")
        self.assertEqual(before, (len(self.tikhub_calls), len(self.matrix_calls)))

    def test_manifest_bytes_tampering_rejects_previously_successful_scan(self):
        _, matrix_dy, _ = self._closed_day()
        row = self._row(matrix_dy["scheduler_run_id"])
        head = json.loads(row["details_json"])["checkpoint"]["last_manifest"]
        path = Path(head["path"])
        path.write_bytes(path.read_bytes() + b" ")
        self._assert_rejected(row)
        result = self._coverage()
        self.assertFalse(result["complete"])
        self.assertIn(str(matrix_dy["scheduler_run_id"]), result["scan_errors"])

    def test_rehashed_disposition_count_mismatch_is_rejected(self):
        self._activate()
        result = self._matrix(pages=[[matrix_work()], []])
        row = self._changed_head(result["scheduler_run_id"], lambda value: value.update(row_count=1))
        self._assert_rejected(row)

    def test_checkpoint_count_mismatch_is_rejected(self):
        self._activate()
        result = self._matrix()
        row = self._row(result["scheduler_run_id"])
        details = json.loads(row["details_json"])
        details["checkpoint"]["counts"]["known"] = 1
        row["details_json"] = json.dumps(details)
        self._assert_rejected(row)

    def test_success_with_pending_raw_or_page_is_rejected(self):
        round_result, matrix_dy, _ = self._closed_day()
        for run_id, pending_key in (
            (matrix_dy["scheduler_run_id"], "pending_page"),
            (round_result["scans"][0]["scheduler_run_id"], "pending_raw"),
            (round_result["scans"][0]["scheduler_run_id"], "pending_materialization"),
        ):
            with self.subTest(pending_key=pending_key):
                row = self._row(run_id)
                details = json.loads(row["details_json"])
                details["checkpoint"][pending_key] = {"raw_id": 1}
                row["details_json"] = json.dumps(details)
                self._assert_rejected(row)

    def test_rehashed_tikhub_scope_mismatch_is_rejected(self):
        round_result, _, _ = self._closed_day()
        row = self._changed_head(round_result["scans"][0]["scheduler_run_id"],
                                 lambda value: value["scope"].update(identity_id=2))
        self._assert_rejected(row)

    def test_rehashed_matrix_nonterminal_manifest_cannot_claim_complete(self):
        self._activate()
        result = self._matrix()
        row = self._changed_head(result["scheduler_run_id"],
                                 lambda value: value.update(complete=False, next_cursor=[1, "still-more"]))
        self._assert_rejected(row)

    def test_rehashed_matrix_chain_cannot_drop_first_page_and_reset_counts(self):
        self._activate()
        result = self._matrix(pages=[[matrix_work()], []])

        def reset(cp):
            cp.update(counts={"known": 0, "new": 0, "quarantined": 0, "unparseable": 0},
                      raw_row_count=0, page_index=1)

        row = self._changed_head(result["scheduler_run_id"], lambda value: value.update(previous=None),
                                 checkpoint_change=reset)
        self._assert_rejected(row)

    def test_rehashed_manifest_cannot_invent_dispositions_absent_from_raw(self):
        self._activate()
        result = self._matrix()
        counts = {"known": 0, "new": 0, "quarantined": 0, "unparseable": 1}

        def forge(value):
            value.update(row_count=1, counts=counts,
                         rows=[{"index": 0, "disposition": "unparseable", "reason": "invalid"}])

        row = self._changed_head(result["scheduler_run_id"], forge,
                                 checkpoint_change=lambda cp: cp.update(counts=counts, raw_row_count=1))
        self._assert_rejected(row)

    def test_rehashed_tikhub_chain_cannot_drop_first_page_and_reset_counts(self):
        self._activate()
        result = self._round(pages={
            "douyin_user_posts": [dy_page([dy_item(1)], more=True, cursor=101), dy_page([])],
            "xiaohongshu_user_posts": [xhs_page([])],
        })
        counts = {"existing": 0, "inserted": 0, "quarantined": 0, "unparseable": 0}
        row = self._changed_head(result["scans"][0]["scheduler_run_id"],
                                 lambda value: value.update(previous=None),
                                 checkpoint_change=lambda cp: cp.update(counts=counts, raw_items=0, page_number=1))
        self._assert_rejected(row)

    def test_v2_range_start_head_with_provider_cursor_is_verified_from_retained_raw(self):
        result = self._v2_range_start_scan()
        row = self._row(result["scheduler_run_id"])
        proof = self._verify(row)
        self.assertEqual(len(proof["references"]), 2)
        details = json.loads(row["details_json"])
        checkpoint = details["checkpoint"]
        self.assertEqual(checkpoint["completion_reason"], "range_start_reached")
        self.assertEqual(checkpoint["qualifying_old_page_count"], 2)
        self.assertEqual(checkpoint["provider_next_cursor"], 202)
        self.assertIsNone(checkpoint["cursor"])
        head = json.loads(Path(checkpoint["last_manifest"]["path"]).read_bytes())
        self.assertTrue(head["provider_has_more"])
        self.assertEqual(head["provider_next_cursor"], 202)
        self.assertIsNone(head["execution_next_cursor"])
        self.assertEqual(head["range_start_proof"]["non_pinned_count"], 1)
        self.assertEqual(head["range_start_proof"]["pinned_count"], 1)

    def test_v2_forged_item_boundary_and_checkpoint_claims_are_rejected(self):
        result = self._v2_range_start_scan()
        run_id = result["scheduler_run_id"]
        for name, change, checkpoint_change in (
            ("platform_content_id", lambda value: value["items"][0].update(platform_content_id="forged"), None),
            ("published_at", lambda value: value["items"][0].update(published_at=PUBLISHED), None),
            ("is_pinned", lambda value: value["items"][0].update(is_pinned=True), None),
            ("is_pinned_numeric", lambda value: value["items"][0].update(is_pinned=0), None),
            ("event_tuple", lambda value: value["items"][0].update(event_tuple=[PUBLISHED, "forged"]), None),
            ("range_proof", lambda value: value["range_start_proof"].update(non_pinned_count=2), None),
            ("provider_cursor", lambda value: value.update(provider_next_cursor=999), None),
            ("provider_cursor_float", lambda value: value.update(provider_next_cursor=202.0), None),
            ("checkpoint_reason", lambda _value: None,
             lambda checkpoint: checkpoint.update(completion_reason="provider_exhausted")),
            ("checkpoint_streak", lambda _value: None,
             lambda checkpoint: checkpoint.update(qualifying_old_page_count=1)),
            ("checkpoint_cursor_float", lambda _value: None,
             lambda checkpoint: checkpoint.update(provider_next_cursor=202.0)),
        ):
            with self.subTest(name=name):
                self._assert_rejected(self._changed_head(
                    run_id, change, checkpoint_change=checkpoint_change,
                ))

    def test_v2_mixed_manifest_contract_chain_is_rejected(self):
        result = self._v2_range_start_scan()
        row = self._row(result["scheduler_run_id"])
        details = json.loads(row["details_json"])
        head = json.loads(Path(details["checkpoint"]["last_manifest"]["path"]).read_bytes())
        previous = json.loads(Path(head["previous"]["path"]).read_bytes())
        previous["contract_version"] = tikhub_scan.LEGACY_CONTRACT_VERSION
        head["previous"] = self._forged_manifest(previous)
        details["checkpoint"]["last_manifest"] = self._forged_manifest(head)
        row["details_json"] = json.dumps(details)
        self._assert_rejected(row)

    def test_v2_raw_attempt_slot_provider_and_adapter_are_bound(self):
        result = self._v2_range_start_scan()
        row = self._row(result["scheduler_run_id"])
        details = json.loads(row["details_json"])
        head = json.loads(Path(details["checkpoint"]["last_manifest"]["path"]).read_bytes())
        previous = json.loads(Path(head["previous"]["path"]).read_bytes())
        head_raw_id = head["raw"]["raw_response_id"]
        previous_raw_id = previous["raw"]["raw_response_id"]
        head_slot_id = head["raw"]["slot_id"]
        with connect(self.db) as connection:
            head_attempt_id = connection.execute(
                "SELECT fetch_attempt_id FROM provider_raw_responses WHERE id=?", (head_raw_id,),
            ).fetchone()[0]
            other_attempt_id = connection.execute(
                "SELECT fetch_attempt_id FROM provider_raw_responses WHERE id=?", (previous_raw_id,),
            ).fetchone()[0]
            original_provider, original_adapter = connection.execute(
                "SELECT provider,adapter_version FROM fetch_slots WHERE id=?", (head_slot_id,),
            ).fetchone()
        for name, update, restore in (
            (
                "raw_attempt_from_other_slot",
                ("UPDATE provider_raw_responses SET fetch_attempt_id=? WHERE id=?",
                 (other_attempt_id, head_raw_id)),
                ("UPDATE provider_raw_responses SET fetch_attempt_id=? WHERE id=?",
                 (head_attempt_id, head_raw_id)),
            ),
            (
                "slot_provider",
                ("UPDATE fetch_slots SET provider='Other' WHERE id=?", (head_slot_id,)),
                ("UPDATE fetch_slots SET provider=? WHERE id=?", (original_provider, head_slot_id)),
            ),
            (
                "slot_adapter",
                ("UPDATE fetch_slots SET adapter_version=? WHERE id=?",
                 (tikhub_scan.LEGACY_CONTRACT_VERSION, head_slot_id)),
                ("UPDATE fetch_slots SET adapter_version=? WHERE id=?",
                 (original_adapter, head_slot_id)),
            ),
        ):
            with self.subTest(name=name):
                with connect(self.db) as connection, transaction(connection):
                    connection.execute(*update)
                self._assert_rejected(row)
                with connect(self.db) as connection, transaction(connection):
                    connection.execute(*restore)
                self._verify(row)

    def test_v2_range_start_proof_survives_terminal_cursor_expiry_without_repurchase(self):
        self._activate()
        old = int(parse_time(SCAN_START).timestamp()) - 1
        calls = []

        def first_generation(operation, request):
            calls.append(request["cursor"])
            if request["cursor"] == 101:
                raise CaptureError(
                    "fixture cursor expired", retryable=False, error_code="http_400",
                    http_status=400, billed=False, raw_response={"message": "max_cursor expired"},
                )
            return ProviderResult({}, dy_page([
                dy_item(81, create_time=old, is_top=False),
            ], more=True, cursor=101), 200, True)

        first = tikhub_scan.run_account_scan(
            1, window_start=SCAN_START, window_end=END, purpose="reconcile",
            roster_snapshot_id=self.snapshot["id"],
            roster_snapshot_hash=self.snapshot["members_sha256"],
            db_path=self.db, raw_root=self.raw_root, now=ROUND_AT,
            task_id="receipt-v2-generation-reset", max_pages=1,
            call_override=first_generation,
        )
        self.assertEqual(first["qualifying_old_page_count"], 1)
        reset_at = pipeline._iso(parse_time(ROUND_AT) + timedelta(seconds=301))
        reset = tikhub_scan.resume_account_scan(
            first["scheduler_run_id"], db_path=self.db, raw_root=self.raw_root,
            now=reset_at, max_pages=1, call_override=first_generation,
        )
        self.assertEqual(reset["reason"], "cursor_expired")
        self.assertEqual(reset["terminal_class"], "provider_transient")
        self.assertEqual(reset["qualifying_old_page_count"], 1)
        self.assertEqual(reset["provider_next_cursor"], 101)
        self.assertEqual(reset["next_cursor"], 101)
        with connect(self.db) as connection:
            before = {
                "raw": connection.execute(
                    "SELECT COUNT(*) FROM provider_raw_responses"
                ).fetchone()[0],
                "slots": connection.execute(
                    "SELECT COUNT(*) FROM fetch_slots WHERE stage='discovery'"
                ).fetchone()[0],
                "usage": connection.execute(
                    "SELECT COUNT(*) FROM provider_usage"
                ).fetchone()[0],
            }

        terminal_reentry = tikhub_scan.resume_account_scan(
            first["scheduler_run_id"], db_path=self.db, raw_root=self.raw_root,
            now=pipeline._iso(parse_time(reset_at) + timedelta(seconds=301)),
            max_pages=1,
            call_override=lambda *_args: self.fail("Terminal cursor expiry must not repurchase"),
        )
        self.assertEqual(terminal_reentry["reason"], "cursor_expired")
        self.assertEqual(terminal_reentry["terminal_class"], "provider_transient")
        self.assertEqual(terminal_reentry["qualifying_old_page_count"], 1)
        self.assertEqual(calls, [0, 101])
        with connect(self.db) as connection:
            self.assertEqual(connection.execute(
                "SELECT COUNT(*) FROM provider_raw_responses"
            ).fetchone()[0], before["raw"])
            self.assertEqual(connection.execute(
                "SELECT COUNT(*) FROM fetch_slots WHERE stage='discovery'"
            ).fetchone()[0], before["slots"])
            self.assertEqual(connection.execute(
                "SELECT COUNT(*) FROM provider_usage"
            ).fetchone()[0], before["usage"])

    def test_v1_provider_exhausted_receipt_remains_verifiable(self):
        self._activate()
        scope = tikhub_scan._freeze(
            1, window_start=SCAN_START, window_end=END, purpose="reconcile",
            roster_snapshot_id=self.snapshot["id"],
            roster_snapshot_hash=self.snapshot["members_sha256"],
            db_path=self.db, task_id="receipt-v1-provider-exhausted",
            task_max_amount=None,
        )
        scope["contract_version"] = tikhub_scan.LEGACY_CONTRACT_VERSION
        done = tikhub_scan._run(
            scope, db_path=self.db, max_pages=1, raw_root=self.raw_root,
            now=ROUND_AT, call_override=lambda _operation, _request: ProviderResult(
                {}, dy_page([dy_item(91)], more=False), 200, True,
            ),
        )
        self.assertTrue(done["complete"])
        self.assertEqual(done["reason"], "source_exhausted")
        proof = self._verify(self._row(done["scheduler_run_id"]))
        self.assertEqual(len(proof["references"]), 1)
        manifest = json.loads(Path(proof["references"][0]["manifest"]["path"]).read_bytes())
        self.assertEqual(manifest["contract_version"], tikhub_scan.LEGACY_CONTRACT_VERSION)
        self.assertNotIn("completion_reason", manifest)
        self.assertNotIn("range_start_proof", manifest)
        mixed_checkpoint = self._row(done["scheduler_run_id"])
        mixed_details = json.loads(mixed_checkpoint["details_json"])
        mixed_details["checkpoint"]["completion_reason"] = "provider_exhausted"
        mixed_checkpoint["details_json"] = json.dumps(mixed_details)
        self._assert_rejected(mixed_checkpoint)
        mixed_item = self._changed_head(
            done["scheduler_run_id"],
            lambda value: value["items"][0].update(is_pinned=False),
        )
        self._assert_rejected(mixed_item)

    def test_synthetic_succeeded_durable_scan_without_raw_is_rejected(self):
        self._activate()
        scope = tikhub_scan._freeze(
            1, window_start="2026-08-21T16:00:00Z", window_end=END, purpose="reconcile",
            roster_snapshot_id=self.snapshot["id"], roster_snapshot_hash=self.snapshot["members_sha256"],
            db_path=self.db, task_id=None, task_max_amount=None,
        )
        run_id = self._fake_succeeded("tikhub_reconcile", scope)
        row = self._row(run_id)
        self.assertEqual(row["status"], "succeeded")
        self.assertTrue(json.loads(row["details_json"])["complete"])
        self.assertTrue(json.loads(row["details_json"])["checkpoint"]["complete"])
        with self.assertRaisesRegex(ValueError, "scan_manifest_missing"):
            self._verify(row)
        with connect(self.db) as connection:
            self.assertEqual(connection.execute("SELECT COUNT(*) FROM provider_raw_responses").fetchone()[0], 0)
        self.assertEqual(self.tikhub_calls, [])
        self.assertEqual(self.matrix_calls, [])

    def test_valid_tikhub_cursor_expiry_preserves_cursor_and_never_restarts_generation(self):
        self._activate()
        cursors = []

        def supplier(operation, request):
            self.assertEqual(operation, "douyin_user_posts")
            cursor = request["cursor"]
            cursors.append(cursor)
            if len(cursors) == 2:
                raise CaptureError("fixture cursor expired", retryable=False, error_code="http_400",
                                   http_status=400, billed=False,
                                   raw_response={"message": "max_cursor expired"})
            return ProviderResult({}, dy_page([dy_item(cursor + 1)], more=cursor == 0, cursor=cursor + 1), 200, True)

        blocked = tikhub_scan.run_account_scan(
            1, window_start="2026-08-21T16:00:00Z", window_end=END, purpose="reconcile",
            roster_snapshot_id=self.snapshot["id"], roster_snapshot_hash=self.snapshot["members_sha256"],
            db_path=self.db, raw_root=self.raw_root, now=ROUND_AT, call_override=supplier,
        )
        self.assertFalse(blocked["complete"])
        self.assertEqual(blocked["reason"], "cursor_expired")
        self.assertEqual(blocked["next_cursor"], 1)
        self.assertEqual(blocked["provider_next_cursor"], 1)
        self.assertEqual(blocked["terminal_class"], "provider_transient")
        with connect(self.db) as connection:
            before = {
                "raw": connection.execute(
                    "SELECT COUNT(*) FROM provider_raw_responses"
                ).fetchone()[0],
                "slots": connection.execute(
                    "SELECT COUNT(*) FROM fetch_slots WHERE stage='discovery'"
                ).fetchone()[0],
                "attempts": connection.execute(
                    "SELECT COUNT(*) FROM scheduler_run_attempts a "
                    "JOIN scheduler_runs r ON r.id=a.scheduler_run_id "
                    "WHERE r.job_id='tikhub_reconcile'"
                ).fetchone()[0],
                "usage": connection.execute(
                    "SELECT COUNT(*) FROM provider_usage"
                ).fetchone()[0],
            }
        resumed_at = "2026-08-28T19:05:01Z"
        with patch("v8.capture.now_utc", return_value=resumed_at), patch("v8.providers.now_utc", return_value=resumed_at), \
                patch("v8.tikhub_scan.now_utc", return_value=resumed_at):
            done = tikhub_scan.resume_account_scan(
                blocked["scheduler_run_id"], db_path=self.db, raw_root=self.raw_root,
                now=resumed_at, call_override=supplier,
            )
        self.assertEqual(done["scheduler_run_id"], blocked["scheduler_run_id"])
        self.assertFalse(done["complete"])
        self.assertEqual(done["reason"], "cursor_expired")
        self.assertEqual(done["terminal_class"], "provider_transient")
        self.assertEqual(done["next_cursor"], 1)
        self.assertEqual(cursors, [0, 1])
        self.assertEqual(done["counts"], {"existing": 0, "inserted": 1, "quarantined": 0, "unparseable": 0})
        with connect(self.db) as connection:
            self.assertEqual(connection.execute("SELECT COUNT(*) FROM content_items").fetchone()[0], 1)
            self.assertEqual(connection.execute(
                "SELECT COUNT(*) FROM provider_raw_responses"
            ).fetchone()[0], before["raw"])
            self.assertEqual(connection.execute(
                "SELECT COUNT(*) FROM fetch_slots WHERE stage='discovery'",
            ).fetchone()[0], before["slots"])
            self.assertEqual(connection.execute(
                "SELECT COUNT(*) FROM scheduler_run_attempts a "
                "JOIN scheduler_runs r ON r.id=a.scheduler_run_id "
                "WHERE r.job_id='tikhub_reconcile'",
            ).fetchone()[0], before["attempts"])
            self.assertEqual(connection.execute(
                "SELECT COUNT(*) FROM provider_usage"
            ).fetchone()[0], before["usage"])

    def test_fake_completed_matrix_flags_do_not_make_report_coverage_complete(self):
        self._activate()
        self._round()
        fake_ids = []
        for platform in ("douyin", "xiaohongshu"):
            scope = matrix_scan.scan_spec(
                "works", platform, purpose="daily", start_at=START, end_at=END,
                overall_start_at=pipeline._iso(parse_time(END) - timedelta(days=30)),
                overall_end_at=END,
                db_path=self.db, roster_snapshot_id=self.snapshot["id"],
                roster_snapshot_hash=self.snapshot["members_sha256"],
            )
            fake_ids.append(self._fake_succeeded("matrix_works_scan", scope))
        result = self._coverage()
        self.assertFalse(result["complete"])
        self.assertFalse(result["scan_traceable"])
        self.assertEqual(result["days"][0]["matrix_run_ids"], [])
        self.assertEqual(result["days"][0]["covered_identity_ids"], [1, 2])
        self.assertEqual(result["scan_errors"], {str(run_id): "scan_manifest_missing" for run_id in fake_ids})

    def test_runtime_requires_all_sixty_exact_matrix_slices_with_real_raw_receipts(self):
        self._activate()
        self._round()
        runs = []
        for index in range(30):
            for platform in ("douyin", "xiaohongshu"):
                if (index, platform) != (29, "xiaohongshu"):
                    runs.append(self._runtime_slice(index, platform))
        result = self._runtime()
        self.assertEqual(result["matrix_expected_windows"], 60)
        self.assertEqual(result["matrix_complete_windows"], 59)
        self.assertEqual(result["tikhub_expected_members"], 2)
        self.assertEqual(result["tikhub_complete_members"], 2)
        self.assertEqual(result["status"], "incomplete")
        self.assertFalse(result["complete"])
        last = self._runtime_slice(29, "xiaohongshu")
        runs.append(last)
        closed = self._runtime()
        self.assertTrue(closed["complete"])
        self.assertEqual(closed["status"], "complete")
        self.assertEqual(closed["contract_version"], "profile-day-coverage-v1")
        self.assertEqual(closed["profile_id"], "matrix_hybrid_v1")
        self.assertEqual(closed["source_family"], "matrix")
        self.assertEqual(closed["matrix_complete_windows"], 60)
        self.assertEqual(len(closed["required_scan_run_ids"]), 62)
        self.assertEqual(closed["scan_errors"], {})
        self.assertEqual(len(self.matrix_calls), 60)
        self.assertEqual(len({run["scheduler_run_id"] for run in runs}), 60)
        with connect(self.db) as connection:
            self.assertEqual(connection.execute("SELECT COUNT(*) FROM provider_raw_responses WHERE provider='newrank_matrix'").fetchone()[0], 60)
        receipt_refresh = runtime_receipts.refresh_runtime_receipts(
            db_path=self.db,
            cutoff_at=CUTOFF,
            evidence_root=self.root / "runtime-evidence",
        )
        self.assertEqual(receipt_refresh["status"], "succeeded")
        summary = pipeline.pipeline_summary(db_path=self.db, at=CUTOFF)
        self.assertTrue(summary["discovery_complete"])
        self.assertEqual(summary["discovery_coverage"]["matrix_complete_windows"], 60)
        proof = self._verify(self._row(last["scheduler_run_id"]))
        path = Path(proof["references"][0]["raw_path"])
        path.write_bytes(path.read_bytes() + b" ")
        broken = self._runtime()
        self.assertFalse(broken["complete"])
        self.assertEqual(broken["matrix_complete_windows"], 59)
        self.assertEqual(broken["scan_errors"][str(last["scheduler_run_id"])], "scan_raw_hash_mismatch")
        # The hot summary remains bound to its already sealed day receipt.
        # Publisher/deep-audit consumers verify retained evidence separately.
        self.assertTrue(pipeline.pipeline_summary(db_path=self.db, at=CUTOFF)["discovery_complete"])

    def test_mode_b_profile_day_requires_tikhub_only_and_forbids_matrix(self):
        self._system_activate()
        round_result = self._round()
        result = self._runtime()
        self.assertTrue(result["complete"])
        self.assertEqual(result["profile_id"], "tikhub_managed_v1")
        self.assertEqual(result["source_family"], "system")
        self.assertEqual(
            (result["matrix_expected_windows"], result["matrix_complete_windows"]),
            (0, 0),
        )
        self.assertEqual(result["matrix_run_ids"], [])
        self.assertEqual(
            result["required_scan_run_ids"],
            sorted(item["scheduler_run_id"] for item in round_result["scans"]),
        )
        self.assertEqual(self.matrix_calls, [])

    def test_wrong_epoch_child_scan_is_ignored_without_cross_profile_leakage(self):
        self._activate((1, 2))
        round_result = self._round()
        source_id = round_result["scans"][0]["scheduler_run_id"]
        wrong_scope = json.loads(self._row(source_id)["details_json"])["identity"]
        wrong_scope = {
            **wrong_scope,
            "activation_id": wrong_scope["activation_id"] + 10_000,
            "task_id": "wrong-epoch-fixture",
        }
        wrong_id = self._fake_succeeded("tikhub_reconcile", wrong_scope)
        for index in range(30):
            for platform in ("douyin", "xiaohongshu"):
                self._runtime_slice(index, platform)
        result = self._runtime()
        self.assertTrue(result["complete"])
        self.assertEqual(result["tikhub_complete_members"], 2)
        self.assertNotIn(wrong_id, result["tikhub_run_ids"])
        self.assertNotIn(str(wrong_id), result["scan_errors"])

    def test_runtime_one_missing_identity_and_six_day_scan_cannot_replace_seven_days(self):
        self._activate()
        round_result = self._round(finish_at={2: AFTER_CUTOFF})
        for index in range(30):
            for platform in ("douyin", "xiaohongshu"):
                self._runtime_slice(index, platform)
        initial = self._runtime()
        self.assertEqual(initial["matrix_complete_windows"], 60)
        self.assertEqual(initial["tikhub_expected_members"], 2)
        self.assertEqual(initial["tikhub_complete_members"], 1)
        self.assertFalse(initial["complete"])
        short = tikhub_scan.run_account_scan(
            2, window_start="2026-08-22T16:00:00Z", window_end=END, purpose="reconcile",
            roster_snapshot_id=self.snapshot["id"], roster_snapshot_hash=self.snapshot["members_sha256"],
            db_path=self.db, raw_root=self.raw_root, now=ROUND_AT,
            call_override=lambda operation, request: ProviderResult({}, xhs_page([]), 200, True),
        )
        self.assertTrue(short["complete"])
        self.assertEqual(len(self._verify(self._row(short["scheduler_run_id"]))["references"]), 1)
        after_short = self._runtime()
        self.assertFalse(after_short["complete"])
        self.assertEqual(after_short["tikhub_complete_members"], 1)
        self.assertEqual(self._coverage()["days"][0]["covered_identity_ids"], [1])
        late_id = round_result["scans"][1]["scheduler_run_id"]
        self.assertEqual(after_short["scan_errors"][str(late_id)], "scan_not_complete_at_cutoff")
        late = self._runtime(at=AFTER_CUTOFF)
        self.assertTrue(late["complete"])
        self.assertEqual(late["tikhub_complete_members"], 2)

    def test_runtime_sixty_synthetic_success_flags_with_no_manifests_are_zero_verified(self):
        self._activate()
        self._round()
        end = parse_time(END)
        fake_ids = []
        for index in range(30):
            for platform in ("douyin", "xiaohongshu"):
                scope = matrix_scan.scan_spec(
                    "works", platform, purpose="daily", db_path=self.db,
                    start_at=pipeline._iso(end - timedelta(days=index + 1)),
                    end_at=pipeline._iso(end - timedelta(days=index)),
                    overall_start_at=pipeline._iso(end - timedelta(days=30)), overall_end_at=END,
                    roster_snapshot_id=self.snapshot["id"], roster_snapshot_hash=self.snapshot["members_sha256"],
                )
                fake_ids.append(self._fake_succeeded("matrix_works_scan", scope))
        result = self._runtime()
        self.assertEqual(result["matrix_expected_windows"], 60)
        self.assertEqual(result["matrix_complete_windows"], 0)
        self.assertEqual(result["tikhub_complete_members"], 2)
        self.assertFalse(result["complete"])
        self.assertEqual(result["scan_errors"], {str(run_id): "scan_manifest_missing" for run_id in fake_ids})
        self.assertEqual(self.matrix_calls, [])
        summary = pipeline.pipeline_summary(db_path=self.db, at=CUTOFF)
        self.assertFalse(summary["discovery_complete"])
        self.assertEqual(summary["discovery_coverage"]["matrix_complete_windows"], 0)

    def test_runtime_wrong_matrix_overall_scope_is_not_a_thirtieth_day_slice(self):
        self._activate()
        self._round()
        for index in range(30):
            for platform in ("douyin", "xiaohongshu"):
                self._runtime_slice(index, platform, correct_overall=(index, platform) != (29, "xiaohongshu"))
        before = self._runtime()
        self.assertEqual(before["matrix_complete_windows"], 59)
        self.assertFalse(before["complete"])
        self._runtime_slice(29, "xiaohongshu")
        after = self._runtime()
        self.assertEqual(after["matrix_complete_windows"], 60)
        self.assertTrue(after["complete"])

    def test_runtime_without_a_frozen_round_is_unknown_not_current_enabled_count(self):
        self._accept()
        result = self._runtime()
        self.assertFalse(result["complete"])
        self.assertEqual(result["status"], "unknown")
        self.assertIsNone(result["tikhub_expected_members"])
        self.assertEqual(result["tikhub_complete_members"], 0)
        self.assertEqual(result["reason"], "historical_roster_scope_unknown")

    def test_invalid_period_is_rejected(self):
        with self.assertRaisesRegex(ValueError, "invalid discovery period"):
            self._coverage(start="2026-08-29", end="2026-08-28")


if __name__ == "__main__":
    unittest.main()
