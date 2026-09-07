from __future__ import annotations

import hashlib
import json
import sqlite3
import tempfile
import unittest
from contextlib import contextmanager
from datetime import datetime, timezone
from pathlib import Path
from unittest.mock import patch

from tests.roster_fixture import accept_roster
from tests.schema_fixture import initialize_historical_schema
from v8 import matrix_scan, storage
from v8.account_roster import (
    PLATFORMS,
    accept_candidate,
    prepare_candidate,
    snapshot_by_id,
)
from v8.durable_runs import claim_run, get_run, recover_run
from v8.capture import CaptureError
from v8.matrix_scan import MatrixRateLimiter, MatrixScanError, read_manifests, run_matrix_scan
from v8.newrank_matrix import GATEWAY, MatrixConfig, MatrixConfigurationError, NewrankMatrixClient
from v8.operations import upsert_account, upsert_content
from v8.paid_drain import (
    PaidDrainBlocked,
    release_paid_drain,
    seal_paid_drain,
    start_paid_drain,
)
from v8.profile_activations import TIKHUB_PROFILE, append_activation
from v8.storage import connect, initialize_database, transaction

NOW = "2026-08-29T04:00:00Z"
LATER = "2026-08-29T04:31:00Z"
START = "2026-08-26T16:00:00Z"
END = "2026-08-27T16:00:00Z"
UID = "1234567890123456789"
LONG_ID = "7379190309625810185"
CONFIG = MatrixConfig(api_url=GATEWAY, n_token="fixture-token", key_id="fixture-key", secret_key="fixture-sign")


def drain_binding():
    return {
        "source_activation_id": 94,
        "target_activation_id": "planned:matrix-hybrid-v1",
        "business_day": "2026-08-29",
        "planned_effective_at": "2026-08-29T16:00:00Z",
        "build_receipt_sha256": "a" * 64,
        "runtime_root_receipt_sha256": "b" * 64,
    }


def work(work_id=LONG_ID, **changes):
    value = {
        "platType": 2, "awemeId": work_id, "uid": UID, "nickname": "fixture",
        "createTime": "2026-08-27 10:00:00", "title": "fixture content",
        "playCount": 123, "diggCount": 4, "commentCount": 0,
        "shareCount": 1, "favoriteCount": 2, "scrollId": [1234567890, work_id],
    }
    value.update(changes)
    return value


def envelope(rows, **changes):
    value = {"code": 0, "data": json.dumps(rows)}
    value.update(changes)
    return 200, json.dumps(value).encode()


class MatrixScanTest(unittest.TestCase):
    def setUp(self):
        self.temp = tempfile.TemporaryDirectory()
        self.addCleanup(self.temp.cleanup)
        self.root = Path(self.temp.name).resolve()
        self.db = self.root / "matrix.sqlite3"
        self.raw = self.root / "scan_raw"
        with connect(self.db) as connection:
            initialize_database(connection)
        self.account = upsert_account({
            "phone": "", "operator_name": "fixture", "platforms": [
                {"platform": "douyin", "uid": UID, "nickname": "fixture"}
            ],
        }, db_path=self.db)
        with connect(self.db) as connection:
            self.identity = dict(connection.execute("SELECT * FROM account_platform_identities").fetchone())
        self.calls = []
        self.clock = 0.0
        self.sleeps = []
        self.limiter = MatrixRateLimiter(clock=lambda: self.clock, sleeper=self.sleep)

    def sleep(self, delay):
        self.sleeps.append(delay)
        self.clock += delay

    def roster(self, identities=None, at="2026-08-03T00:00:00Z"):
        with connect(self.db) as connection, transaction(connection):
            return accept_roster(connection, identities, accepted_at=at)

    def activate_tikhub_profile(self, *, at=NOW):
        member = {
            "platform": "douyin",
            "uid": UID,
            "profile_ref": None,
            "sec_user_id": "MS4w.fixture",
            "nickname": "fixture",
        }
        source = json.dumps({"members": [member]}, sort_keys=True).encode()
        with connect(self.db) as connection, transaction(connection):
            candidate = prepare_candidate(
                connection,
                {
                    "source_type": "system_managed",
                    "source_captured_at": at,
                    "scope": {
                        "organization": "isolated-test",
                        "coverage": "full",
                        "account_scope": "all_managed_accounts",
                        "platforms": sorted(PLATFORMS),
                    },
                    "source_evidence": {
                        "kind": "system_roster_seal",
                        "evidence_kind": "system_roster_manifest",
                        "source_format": "system-roster-json-v1",
                        "seal_id": "matrix-gate-mode-b",
                        "sealed_at": at,
                        "source_sha256": hashlib.sha256(source).hexdigest(),
                        "source_name": "system-roster.json",
                        "scope_evidence": "Complete isolated system roster.",
                    },
                    "declared_count": 1,
                    "pagination": {
                        "pages": [1],
                        "expected_pages": 1,
                        "terminal": True,
                        "declared_totals": [1],
                    },
                    "members": [member],
                },
                source_bytes=source,
                raw_root=self.root / "roster-raw",
                observed_at=at,
            )
            accepted = accept_candidate(
                connection, int(candidate["candidate_id"]), accepted_at=at
            )
            snapshot = snapshot_by_id(connection, int(accepted["snapshot_id"]))
            return append_activation(
                connection,
                profile_id=TIKHUB_PROFILE,
                roster_snapshot_id=int(snapshot["id"]),
                roster_members_sha256=str(snapshot["members_sha256"]),
                effective_at=at,
                build_receipt_sha256="c" * 64,
                actor="test-fixture",
                reason="switch to TikHub managed profile",
                created_at=at,
            )

    def client(self, responses, hook=None):
        iterator = iter(responses)

        def transport(request, timeout):
            body = json.loads(request.data)
            self.calls.append({"query": json.loads(body["reqJson"]), "path": body["pathName"]})
            if hook:
                hook()
            return next(iterator)

        return NewrankMatrixClient(
            CONFIG, transport=transport,
            clock=lambda: datetime(2026, 8, 28, 17, 0, 0, tzinfo=timezone.utc),
        )

    def scan(self, client=None, **kwargs):
        kind = kwargs.pop("kind", "works")
        purpose = kwargs.pop("purpose", "daily")
        options = {"start_at": START, "end_at": END} if kind == "works" else {"rank_date": "2026-08-28"}
        options.update(kwargs)
        return run_matrix_scan(
            kind, "douyin", purpose=purpose, db_path=self.db, raw_root=self.raw,
            client=client, rate_limiter=self.limiter, now=options.pop("now", NOW), **options,
        )

    def rows(self, table):
        with connect(self.db) as connection:
            return [dict(row) for row in connection.execute(f"SELECT * FROM {table} ORDER BY id")]

    def dispatch_chains(self):
        chains = {}
        for row in self.rows("paid_provider_dispatch_events"):
            chains.setdefault(row["dispatch_id"], []).append(row)
        return list(chains.values())

    def test_roster_gate_is_before_network_or_run_creation(self):
        with self.assertRaisesRegex(MatrixScanError, "roster_not_ready"):
            self.scan(self.client([envelope([])]))
        self.assertEqual(self.calls, [])
        self.assertEqual(self.rows("scheduler_runs"), [])
        self.assertEqual(self.rows("provider_raw_responses"), [])

    def test_drain_gate_before_claim_creates_no_matrix_run_or_network(self):
        self.roster()
        start_paid_drain(
            "switch-before-claim",
            binding=drain_binding(),
            db_path=self.db,
            now=NOW,
        )

        with self.assertRaises(PaidDrainBlocked):
            self.scan(self.client([envelope([])]))

        self.assertEqual(self.calls, [])
        self.assertEqual(
            [row for row in self.rows("scheduler_runs") if row["job_id"].startswith("matrix_")],
            [],
        )
        self.assertEqual(self.rows("provider_usage"), [])
        self.assertEqual(self.rows("fetch_attempts"), [])

    def test_tikhub_profile_rejects_matrix_before_claim_or_network(self):
        self.roster()
        self.activate_tikhub_profile()

        with self.assertRaisesRegex(MatrixScanError, "profile_not_scheduled"):
            self.scan(self.client([envelope([])]))

        self.assertEqual(self.calls, [])
        self.assertEqual(
            [
                row
                for row in self.rows("scheduler_runs")
                if row["job_id"].startswith("matrix_")
            ],
            [],
        )
        self.assertEqual(self.rows("paid_provider_dispatch_events"), [])

    def test_profile_switch_while_waiting_for_slot_stops_before_network(self):
        self.roster()

        class SwitchAtLimiter:
            @contextmanager
            def slot(inner_self):
                del inner_self
                self.activate_tikhub_profile()
                yield

        result = run_matrix_scan(
            "works",
            "douyin",
            purpose="daily",
            db_path=self.db,
            raw_root=self.raw,
            start_at=START,
            end_at=END,
            client=self.client([envelope([])]),
            rate_limiter=SwitchAtLimiter(),
            now=NOW,
        )

        self.assertEqual(
            (result["status"], result["reason"]),
            ("partial", "profile_not_scheduled"),
        )
        self.assertEqual(result["network_requests"], 0)
        self.assertEqual(self.calls, [])
        chain = self.dispatch_chains()[0]
        self.assertEqual(
            [event["event_type"] for event in chain],
            ["reserved", "not_sent"],
        )

    def test_drain_start_after_claim_wins_final_gate_with_zero_network(self):
        self.roster()

        class StartAtLimiter:
            @contextmanager
            def slot(inner_self):
                del inner_self
                start_paid_drain(
                    "switch-final-gate",
                    binding=drain_binding(),
                    db_path=self.db,
                    now=NOW,
                )
                yield

        result = run_matrix_scan(
            "works",
            "douyin",
            purpose="daily",
            db_path=self.db,
            raw_root=self.raw,
            start_at=START,
            end_at=END,
            client=self.client([envelope([])]),
            rate_limiter=StartAtLimiter(),
            now=NOW,
        )

        self.assertEqual((result["status"], result["reason"]), ("partial", "profile_switch_drain"))
        self.assertEqual(result["network_requests"], 0)
        self.assertEqual(self.calls, [])
        self.assertEqual(self.rows("provider_raw_responses"), [])
        chain = self.dispatch_chains()[0]
        self.assertEqual(
            [event["event_type"] for event in chain],
            ["reserved", "not_sent"],
        )
        self.assertFalse(
            any(event["event_type"] == "send_marked" for event in chain)
        )
        matrix_run = next(
            row for row in self.rows("scheduler_runs")
            if row["job_id"] == "matrix_works_scan"
        )
        matrix_attempt = next(
            row for row in self.rows("scheduler_run_attempts")
            if row["scheduler_run_id"] == matrix_run["id"]
        )
        self.assertEqual(matrix_attempt["status"], "partial")
        seal_paid_drain("switch-final-gate", db_path=self.db, now=LATER)

    def test_request_linearized_before_start_is_the_only_inflight_request(self):
        self.roster()
        started = False

        def start_after_send():
            nonlocal started
            if started:
                return
            started = True
            start_paid_drain(
                "switch-inflight",
                binding=drain_binding(),
                db_path=self.db,
                now=NOW,
            )

        client = self.client([envelope([work()]), envelope([])], hook=start_after_send)
        first = self.scan(client)

        self.assertEqual((first["status"], first["reason"]), ("partial", "profile_switch_drain"))
        self.assertEqual(first["network_requests"], 1)
        self.assertEqual(len(self.calls), 1)
        seal_paid_drain("switch-inflight", db_path=self.db, now=LATER)
        release_paid_drain("switch-inflight", db_path=self.db, now="2026-08-29T04:30:30Z")

        resumed = self.scan(client, now=LATER)
        self.assertTrue(resumed["complete"])
        self.assertEqual(resumed["network_requests"], 2)
        self.assertEqual(len(self.calls), 2)

    def test_successful_pages_have_fully_bound_request_ledger_chains(self):
        snapshot = self.roster()
        with patch("v8.matrix_scan.now_utc", return_value=NOW):
            result = self.scan(
                self.client([envelope([work()]), envelope([])])
            )

        self.assertTrue(result["complete"])
        chains = self.dispatch_chains()
        self.assertEqual(len(chains), 2)
        raw_ids = {row["id"] for row in self.rows("provider_raw_responses")}
        run = next(
            row
            for row in self.rows("scheduler_runs")
            if row["id"] == result["scheduler_run_id"]
        )
        attempt = next(
            row
            for row in self.rows("scheduler_run_attempts")
            if row["scheduler_run_id"] == run["id"]
        )
        activation_id = json.loads(run["details_json"])["identity"][
            "activation_id"
        ]
        for page_index, chain in enumerate(chains):
            self.assertEqual(
                [event["event_type"] for event in chain],
                ["reserved", "send_marked", "succeeded"],
            )
            self.assertTrue(
                all(event["provider"] == "newrank_matrix" for event in chain)
            )
            self.assertTrue(
                all(event["operation"] == "matrix_works_list" for event in chain)
            )
            self.assertTrue(
                all(event["activation_id"] == activation_id for event in chain)
            )
            self.assertTrue(
                all(event["business_day"] == "2026-08-29" for event in chain)
            )
            self.assertTrue(
                all(event["scheduler_run_id"] == run["id"] for event in chain)
            )
            self.assertTrue(
                all(
                    event["scheduler_attempt_id"] == attempt["id"]
                    for event in chain
                )
            )
            self.assertEqual(
                len({event["permit_event_id"] for event in chain}), 1
            )
            scope = json.loads(chain[0]["scope_json"])
            self.assertEqual(scope["scan_id"], result["scan_id"])
            self.assertEqual(scope["scan"]["roster_snapshot_id"], snapshot["id"])
            cursor = json.loads(chain[0]["cursor_identity_json"])
            self.assertEqual(cursor["page_index"], page_index)
            self.assertEqual(cursor["request_number"], page_index + 1)
            self.assertEqual(
                cursor["request_cursor"],
                None if page_index == 0 else [1234567890, LONG_ID],
            )
            self.assertIn(chain[-1]["raw_response_id"], raw_ids)

    def test_profile_change_after_send_still_terminalizes_the_request(self):
        self.roster()
        switched = False

        def switch_during_request():
            nonlocal switched
            if not switched:
                switched = True
                self.activate_tikhub_profile()

        result = self.scan(
            self.client([envelope([work()])], hook=switch_during_request)
        )

        self.assertEqual(
            (result["status"], result["reason"]),
            ("partial", "profile_not_scheduled"),
        )
        self.assertEqual(result["network_requests"], 1)
        self.assertEqual(len(self.calls), 1)
        chain = self.dispatch_chains()[0]
        self.assertEqual(
            [event["event_type"] for event in chain],
            ["reserved", "send_marked", "succeeded"],
        )
        self.assertIsNotNone(chain[-1]["raw_response_id"])

    def test_schema18_scan_runs_without_request_ledger(self):
        legacy_db = self.root / "matrix-schema18.sqlite3"
        with sqlite3.connect(legacy_db) as connection:
            connection.row_factory = sqlite3.Row
            storage.configure_connection_safety(connection)
            initialize_historical_schema(connection, target_version=17)
            storage.migrate_database(connection, from_version=17, to_version=18)
        upsert_account(
            {
                "phone": "",
                "operator_name": "schema18",
                "platforms": [
                    {"platform": "douyin", "uid": UID, "nickname": "fixture"}
                ],
            },
            db_path=legacy_db,
        )
        with connect(legacy_db) as connection, transaction(connection):
            accept_roster(connection, accepted_at=NOW)

        result = run_matrix_scan(
            "works",
            "douyin",
            purpose="daily",
            db_path=legacy_db,
            raw_root=self.root / "schema18-raw",
            start_at=START,
            end_at=END,
            client=self.client([envelope([])]),
            rate_limiter=self.limiter,
            now=NOW,
        )

        self.assertTrue(result["complete"])
        with connect(legacy_db) as connection:
            self.assertIsNone(
                connection.execute(
                    "SELECT 1 FROM sqlite_master "
                    "WHERE type='table' AND name='paid_provider_dispatch_events'"
                ).fetchone()
            )

    def test_raw_first_conservation_identity_and_terminal_empty(self):
        snapshot = self.roster()
        old = upsert_content({
            "platform": "douyin", "platform_content_id": "777000", "account_uid": UID,
            "title": "original title", "published_at": "2026-08-20T02:00:00Z",
            "canonical_url": "https://www.douyin.com/video/777000", "content_type": "video",
        }, db_path=self.db)
        rows = [
            work("777000"), work(), work("999000", uid="998877665544"),
            {"platType": 2, "uid": UID, "scrollId": [3, "invalid"]},
            work(scrollId=[4, LONG_ID]),
        ]
        result = self.scan(self.client([envelope(rows), envelope([])]))
        self.assertEqual(result["status"], "succeeded")
        self.assertTrue(result["complete"])
        self.assertEqual(result["counts"], {"known": 2, "new": 1, "quarantined": 1, "unparseable": 1})
        self.assertEqual(result["raw_row_count"], 5)
        content = self.rows("content_items")
        self.assertEqual(len(content), 2)
        self.assertEqual(content[0]["id"], old["id"])
        self.assertEqual(content[0]["title"], "original title")
        self.assertEqual(content[1]["content_type"], "unknown")
        self.assertEqual(content[1]["account_id"], self.account["id"])
        self.assertEqual(len(self.rows("fetch_slots")), 0)
        self.assertEqual(len(self.rows("provider_usage")), 0)
        for raw in self.rows("provider_raw_responses"):
            self.assertIsNone(raw["account_id"])
            self.assertIsNone(raw["content_id"])
            self.assertIsNone(raw["fetch_attempt_id"])
            self.assertEqual(raw["provider"], "newrank_matrix")
            body = Path(raw["local_path"]).read_bytes()
            self.assertEqual(hashlib.sha256(body).hexdigest(), raw["sha256"])
            for secret in ("fixture-token", "fixture-sign", "fixture-key", "N-Token"):
                self.assertNotIn(secret.encode(), body)
        manifests = read_manifests(result["scheduler_run_id"], db_path=self.db)
        self.assertEqual(len(manifests), 2)
        self.assertEqual(manifests[0]["rows"][3]["index"], 3)
        self.assertEqual(manifests[0]["rows"][3]["reason"], "missing_or_invalid_work_id")
        self.assertTrue(manifests[1]["complete"])
        self.assertEqual(sum(manifests[0]["counts"].values()), len(rows))
        self.assertEqual(self.calls[1]["query"]["scrollId"], [4, LONG_ID])
        self.assertEqual(self.calls[0]["query"]["endDate"], "2026-08-27 23:59:59")
        observations = self.rows("content_metric_observations")
        self.assertEqual(len(observations), 2)
        self.assertTrue(all(row["window_key"] == "2026-08-29" for row in observations))
        self.assertTrue(all(row["source"] == "newrank_matrix" for row in observations))
        state = get_run(result["scheduler_run_id"], db_path=self.db)["details"]
        self.assertEqual(state["identity"]["roster_snapshot_hash"], snapshot["members_sha256"])

    def test_automatic_scan_stops_before_first_request_after_business_day(self):
        self.roster()
        current_day = "2026-08-29T04:00:00Z"
        next_day = "2026-08-29T16:00:00Z"
        client = self.client([envelope([work()]), envelope([])])
        with patch(
            "v8.matrix_scan.now_utc",
            side_effect=[current_day, current_day, current_day, next_day],
        ):
            result = self.scan(
                client,
                purpose="daily-works",
                overall_end_at="2026-08-28T16:00:00Z",
            )
        self.assertEqual((result["status"], result["reason"]),
                         ("partial", "business_day_expired"))
        self.assertEqual(len(self.calls), 1)
        self.assertEqual(result["network_requests"], 1)
        self.assertEqual(len(self.rows("provider_raw_responses")), 1)

    def test_automatic_business_day_uses_overall_end_and_rank_date_plus_one(self):
        works = {
            "kind": "works",
            "purpose": "daily-works",
            "overall_end_at": "2026-08-28T16:00:00Z",
        }
        accounts = {
            "kind": "accounts",
            "purpose": "daily-account-metrics",
            "rank_date": "2026-08-28",
        }
        self.assertEqual(matrix_scan._automatic_business_day(works).isoformat(), "2026-08-29")
        self.assertEqual(matrix_scan._automatic_business_day(accounts).isoformat(), "2026-08-29")

    def test_same_complete_scan_returns_existing_success_without_network(self):
        self.roster()
        result = self.scan(self.client([envelope([])]))
        repeated = self.scan(self.client([]))
        self.assertEqual(repeated["scheduler_run_id"], result["scheduler_run_id"])
        self.assertEqual(repeated["status"], "succeeded")
        self.assertEqual(repeated["reason"], "already_complete")
        self.assertTrue(repeated["complete"])
        self.assertEqual(repeated["network_requests"], 1)
        self.assertEqual(len(self.calls), 1)

    def test_missing_publication_keeps_null_and_generated_link(self):
        self.roster()
        result = self.scan(self.client([envelope([work(createTime=None)]), envelope([])]))
        content = self.rows("content_items")[0]
        self.assertIsNone(content["published_at"])
        self.assertEqual(content["canonical_url"], "https://www.douyin.com/video/" + LONG_ID)
        self.assertEqual(read_manifests(result["scheduler_run_id"], db_path=self.db)[0]["rows"][0]["reason"], "publication_time_pending")

    def test_existing_unlinked_without_uid_retains_report_identity(self):
        self.roster()
        old = upsert_content({
            "platform": "douyin", "platform_content_id": LONG_ID,
            "canonical_url": "https://www.douyin.com/video/" + LONG_ID,
            "title": "legacy", "published_at": "2026-08-01T01:00:00Z",
        }, db_path=self.db)
        before = self.rows("content_items")
        result = self.scan(self.client([envelope([work(uid=None)]), envelope([])]))
        self.assertEqual(result["counts"]["known"], 1)
        self.assertEqual(self.rows("content_items"), before)
        self.assertEqual(before[0]["id"], old["id"])
        self.assertEqual(self.rows("content_metric_observations"), [])

    def test_outside_overall_scope_is_quarantined_but_adjacent_day_is_retained(self):
        self.roster()
        items = [
            work("1001001", createTime="2026-08-26 22:00:00"),
            work("1001002", createTime="2026-08-02 22:00:00"),
        ]
        result = self.scan(self.client([envelope(items), envelope([])]), overall_start_at="2026-08-25T16:00:00Z")
        self.assertEqual(result["counts"]["new"], 1)
        self.assertEqual(result["counts"]["quarantined"], 1)
        self.assertEqual(self.rows("content_items")[0]["platform_content_id"], "1001001")

    def test_retired_identity_only_accepts_its_historical_managed_window(self):
        self.roster()
        self.roster([], at="2026-08-26T16:00:00Z")
        items = [
            work("1001001", createTime="2026-08-25 22:00:00"),
            work("1001002", createTime="2026-08-27 22:00:00"),
        ]
        result = self.scan(self.client([envelope(items), envelope([])]), overall_start_at="2026-08-03T00:00:00+08:00")
        self.assertEqual(result["counts"]["new"], 1)
        self.assertEqual(result["counts"]["quarantined"], 1)
        manifest = read_manifests(result["scheduler_run_id"], db_path=self.db)[0]
        self.assertEqual(manifest["rows"][1]["reason"], "outside_management_window")

    def test_account_pages_only_persist_observations_never_roster(self):
        self.roster()
        accounts_before = self.rows("accounts")
        roster_before = self.rows("account_roster_snapshots")
        records = [
            {"platType": 2, "uid": UID, "rankDate": "2026-08-28", "cTotalFans": 0,
             "workPlayCountAdd": -3, "scrollId": [1, UID]},
            {"platType": 2, "uid": "unknown", "rankDate": "2026-08-28", "scrollId": [2, "unknown"]},
            {"platType": 2, "uid": UID, "rankDate": "2026-08-27", "scrollId": [3, UID]},
        ]
        result = self.scan(self.client([envelope(records), envelope([])]), kind="accounts")
        self.assertTrue(result["complete"])
        self.assertEqual(self.rows("accounts"), accounts_before)
        self.assertEqual(self.rows("account_roster_snapshots"), roster_before)
        self.assertEqual(self.rows("content_items"), [])
        facts = self.rows("account_metric_observations")
        self.assertEqual(len(facts), 1)
        payload = json.loads(facts[0]["payload_json"])
        self.assertEqual(payload["statistics_date"], "2026-08-28")
        self.assertEqual(payload["fields"]["follower_count"]["value"], 0)
        self.assertEqual(payload["fields"]["work_view_daily_increment"]["value"], -3)
        self.assertEqual(self.calls[0]["query"]["rankData"], "2026-08-28")


    def test_twenty_pages_yield_then_resume_to_terminal_without_repurchasing(self):
        self.roster()
        pages = [envelope([work(str(1000000 + index))]) for index in range(20)]
        client = self.client(pages + [envelope([])])
        first = self.scan(client)
        self.assertEqual(first["reason"], "page_yield")
        self.assertEqual(first["status"], "partial")
        self.assertFalse(first["complete"])
        self.assertEqual(first["network_requests"], 20)
        self.assertEqual(first["raw_row_count"], 20)
        waiting = self.scan(client)
        self.assertEqual(waiting["reason"], "not_due")
        self.assertEqual(len(self.calls), 20)
        resumed = self.scan(client, now=LATER)
        self.assertTrue(resumed["complete"])
        self.assertEqual(resumed["scheduler_run_id"], first["scheduler_run_id"])
        self.assertEqual(resumed["network_requests"], 21)
        self.assertEqual(len(self.rows("content_items")), 20)
        attempts = self.rows("scheduler_run_attempts")
        self.assertEqual([row["status"] for row in attempts], ["partial", "succeeded"])
        state = get_run(first["scheduler_run_id"], db_path=self.db)["details"]["checkpoint"]
        self.assertEqual(state["page_index"], 21)
        self.assertEqual(len(read_manifests(first["scheduler_run_id"], db_path=self.db)), 21)
        self.assertLess(len(json.dumps(state)), 2000)

    def test_missing_cursor_is_visible_partial_and_never_empty_success(self):
        self.roster()
        result = self.scan(self.client([envelope([work(scrollId=None)])]))
        self.assertFalse(result["complete"])
        self.assertEqual(result["reason"], "missing_cursor")
        self.assertEqual(result["counts"]["new"], 1)
        repeated = self.scan(self.client([]), now=LATER)
        self.assertEqual(repeated["reason"], "missing_cursor")
        self.assertEqual(len(self.calls), 1)
        self.assertFalse(read_manifests(result["scheduler_run_id"], db_path=self.db)[0]["complete"])

    def test_repeated_cursor_stops_with_conserved_page_dispositions(self):
        self.roster()
        result = self.scan(self.client([
            envelope([work()]), envelope([work()]),
        ]))
        self.assertEqual(result["reason"], "repeated_cursor")
        self.assertFalse(result["complete"])
        self.assertEqual(result["counts"]["new"], 1)
        self.assertEqual(result["counts"]["known"], 1)
        self.assertEqual(result["raw_row_count"], 2)
        self.assertEqual(len(self.rows("content_items")), 1)

    def test_declared_total_drift_never_marks_terminal_empty_complete(self):
        self.roster()
        result = self.scan(self.client([
            envelope([work()], totalNum=1), envelope([], totalNum=2),
        ]))
        self.assertFalse(result["complete"])
        self.assertEqual(result["reason"], "declared_total_drift")
        manifest = read_manifests(result["scheduler_run_id"], db_path=self.db)[-1]
        self.assertEqual(manifest["row_count"], 0)
        self.assertFalse(manifest["complete"])

    def test_registered_raw_replays_free_after_atomic_business_rollback(self):
        self.roster()
        real_upsert = matrix_scan.upsert_content
        calls = 0

        def crash_on_second(*args, **kwargs):
            nonlocal calls
            calls += 1
            if calls == 2:
                raise RuntimeError("simulated process crash")
            return real_upsert(*args, **kwargs)

        with patch.object(matrix_scan, "upsert_content", side_effect=crash_on_second):
            with self.assertRaisesRegex(RuntimeError, "simulated process crash"):
                self.scan(self.client([envelope([work("1001001"), work("1001002")])]))
        self.assertEqual(len(self.calls), 1)
        self.assertEqual(self.rows("content_items"), [])
        self.assertEqual(self.rows("content_metric_observations"), [])
        run = self.rows("scheduler_runs")[0]
        details = json.loads(run["details_json"])
        self.assertEqual(details["checkpoint"]["page_index"], 0)
        self.assertIsNotNone(details["checkpoint"]["pending_page"])
        raw = self.rows("provider_raw_responses")[0]
        original_bytes = Path(raw["local_path"]).read_bytes()
        attempt = self.rows("scheduler_run_attempts")[0]
        recover_run(run["id"], expected_attempt_id=attempt["id"], db_path=self.db, now=LATER)
        resumed = self.scan(self.client([envelope([])]), now=LATER)
        self.assertTrue(resumed["complete"])
        self.assertEqual(resumed["network_requests"], 2)
        self.assertEqual(len(self.calls), 2)
        self.assertEqual(len(self.rows("content_items")), 2)
        self.assertEqual(len(self.rows("content_metric_observations")), 2)
        self.assertEqual(Path(raw["local_path"]).read_bytes(), original_bytes)
        self.assertEqual(len(self.rows("provider_raw_responses")), 2)

    def test_old_http_response_loses_fence_and_cannot_register_or_materialize(self):
        self.roster()
        replacement = []

        def takeover():
            run = self.rows("scheduler_runs")[0]
            attempt = self.rows("scheduler_run_attempts")[0]
            recover_run(run["id"], expected_attempt_id=attempt["id"], db_path=self.db, now=LATER)
            identity = json.loads(run["details_json"])["identity"]
            replacement.append(claim_run(run["job_id"], identity, db_path=self.db, now=LATER))

        result = self.scan(self.client([envelope([work()])], hook=takeover))
        self.assertEqual(result["reason"], "owner_lost")
        self.assertEqual(result["status"], "interrupted")
        self.assertEqual(self.rows("content_items"), [])
        self.assertEqual(self.rows("provider_raw_responses"), [])
        self.assertEqual(len(list(self.raw.rglob("*.response.json"))), 1)
        state = get_run(replacement[0].scheduler_run_id, db_path=self.db)["details"]["checkpoint"]
        self.assertEqual(state["page_index"], 0)
        self.assertEqual(state["network_requests"], 0)
        chain = self.dispatch_chains()[0]
        self.assertEqual(
            [event["event_type"] for event in chain],
            ["reserved", "send_marked", "billing_unknown"],
        )

    def test_corrupted_pending_raw_blocks_without_network_or_business(self):
        self.roster()
        with patch.object(matrix_scan, "apply_pending_page", side_effect=RuntimeError("crash")):
            with self.assertRaises(RuntimeError):
                self.scan(self.client([envelope([work()])]))
        run = self.rows("scheduler_runs")[0]
        attempt = self.rows("scheduler_run_attempts")[0]
        raw = self.rows("provider_raw_responses")[0]
        Path(raw["local_path"]).write_bytes(b'{"corrupted":true}')
        recover_run(run["id"], expected_attempt_id=attempt["id"], db_path=self.db, now=LATER)
        resumed = self.scan(self.client([]), now=LATER)
        self.assertFalse(resumed["complete"])
        self.assertEqual(resumed["reason"], "raw_hash_mismatch")
        self.assertEqual(len(self.calls), 1)
        self.assertEqual(self.rows("content_items"), [])
        state = get_run(run["id"], db_path=self.db)["details"]["checkpoint"]
        self.assertIsNotNone(state["pending_page"])
        self.assertEqual(state["page_index"], 0)

    def test_config_error_is_partial_without_a_permanently_running_attempt(self):
        self.roster()
        with patch("v8.newrank_matrix.load_config", side_effect=MatrixConfigurationError("unavailable")):
            result = self.scan()
        self.assertFalse(result["complete"])
        self.assertEqual(result["reason"], "matrix_configuration_invalid")
        self.assertEqual(result["network_requests"], 0)
        self.assertEqual(self.rows("scheduler_run_attempts")[0]["status"], "partial")
        self.assertEqual(self.rows("provider_raw_responses"), [])
        chain = self.dispatch_chains()[0]
        self.assertEqual(
            [event["event_type"] for event in chain],
            ["reserved", "send_marked", "failed"],
        )
        self.assertIsNone(chain[-1]["raw_response_id"])

    def test_known_unbilled_provider_error_is_failed_with_raw_evidence(self):
        self.roster()

        class KnownFailureClient:
            def fetch_works_page(inner_self, *args, **kwargs):
                del inner_self, args, kwargs
                self.calls.append({"known_failure": True})
                raise CaptureError(
                    "known provider rejection",
                    retryable=False,
                    error_code="matrix_known_rejection",
                    http_status=400,
                    billed=False,
                    raw_response={"code": 400, "message": "rejected"},
                )

        result = self.scan(KnownFailureClient())

        self.assertEqual(result["reason"], "matrix_known_rejection")
        self.assertEqual(result["network_requests"], 1)
        chain = self.dispatch_chains()[0]
        self.assertEqual(
            [event["event_type"] for event in chain],
            ["reserved", "send_marked", "failed"],
        )
        raw = self.rows("provider_raw_responses")[0]
        self.assertEqual(chain[-1]["raw_response_id"], raw["id"])

    def test_http_retry_is_bounded_two_and_unknown_cost_is_not_zero(self):
        self.roster()
        failed = (503, b'{"code":5000,"data":null}')
        result = self.scan(self.client([failed, failed]))
        self.assertEqual(result["status"], "partial")
        self.assertEqual(result["reason"], "matrix_http_503")
        self.assertEqual(result["network_requests"], 2)
        self.assertIsNone(result["provider_cost"])
        self.assertEqual(result["provider_cost_status"], "unknown")
        self.assertEqual(len(self.rows("provider_raw_responses")), 2)
        self.assertEqual(self.rows("provider_usage"), [])
        chains = self.dispatch_chains()
        self.assertEqual(len(chains), 2)
        self.assertTrue(
            all(
                [event["event_type"] for event in chain]
                == ["reserved", "send_marked", "billing_unknown"]
                for chain in chains
            )
        )
        self.assertTrue(
            all(chain[-1]["raw_response_id"] is not None for chain in chains)
        )

    def test_auth_error_does_not_retry_and_is_preserved_as_global_raw(self):
        self.roster()
        result = self.scan(self.client([(401, b'{"code":401,"data":null}')]))
        self.assertEqual(result["reason"], "matrix_http_401")
        self.assertEqual(len(self.calls), 1)
        raw = self.rows("provider_raw_responses")[0]
        self.assertEqual(raw["source"], "matrix_scan_error")
        self.assertIsNone(raw["account_id"])
        self.assertIsNone(raw["content_id"])

    def test_rate_limiter_starts_at_most_two_per_second(self):
        starts = []
        for _ in range(7):
            with self.limiter.slot():
                starts.append(self.clock)
        self.assertEqual(starts, [0, 0, 1, 1, 2, 2, 3])
        self.assertEqual(self.sleeps, [1, 1, 1])

    def test_wrong_account_statistics_day_keeps_raw_and_no_observation(self):
        self.roster()
        records = [{
            "platType": 2, "uid": UID, "rankDate": "2026-08-27",
            "cTotalFans": 99, "scrollId": [1, UID],
        }]
        result = self.scan(self.client([envelope(records), envelope([])]), kind="accounts")
        self.assertTrue(result["complete"])
        self.assertEqual(result["counts"]["unparseable"], 1)
        self.assertEqual(self.rows("account_metric_observations"), [])
        manifest = read_manifests(result["scheduler_run_id"], db_path=self.db)[0]
        self.assertEqual(manifest["rows"][0]["reason"], "statistics_date_mismatch")


    def test_invalid_work_identifier_is_counted_without_poisoning_valid_rows(self):
        self.roster()
        result = self.scan(self.client([
            envelope([work("12"), work(LONG_ID)]), envelope([]),
        ]))
        self.assertTrue(result["complete"])
        self.assertEqual(result["counts"]["new"], 1)
        self.assertEqual(result["counts"]["unparseable"], 1)
        self.assertEqual(len(self.rows("content_items")), 1)
        manifest = read_manifests(result["scheduler_run_id"], db_path=self.db)[0]
        self.assertEqual(manifest["rows"][0]["reason"], "invalid_content_identity")

    def test_xhs_zero_views_remain_not_applicable_while_raw_keeps_value(self):
        account = upsert_account({
            "phone": "", "platforms": [{
                "platform": "xiaohongshu", "uid": "65abcd0123456789abcdef012",
                "nickname": "xhs",
            }],
        }, db_path=self.db)
        self.roster()
        note_id = "a" * 24
        row = work(note_id, platType=6, uid="65abcd0123456789abcdef012", playCount=0)
        result = run_matrix_scan(
            "works", "xiaohongshu", purpose="daily", db_path=self.db,
            start_at=START, end_at=END, raw_root=self.raw,
            client=self.client([envelope([row]), envelope([])]), rate_limiter=self.limiter, now=NOW,
        )
        self.assertTrue(result["complete"])
        self.assertEqual(result["counts"]["new"], 1, read_manifests(result["scheduler_run_id"], db_path=self.db))
        content = self.rows("content_items")[0]
        self.assertEqual(content["account_id"], account["id"])
        fact = self.rows("content_metric_observations")[0]
        self.assertIsNone(fact["view_count"])
        self.assertEqual(json.loads(fact["metadata_json"])["fields"]["view_count"]["status"], "not_applicable")
        raw = self.rows("provider_raw_responses")[0]
        payload = json.loads(Path(raw["local_path"]).read_bytes())
        self.assertEqual(json.loads(payload["response"]["data"])[0]["playCount"], 0)

    def test_raw_root_symlink_is_rejected_without_business_or_registered_raw(self):
        self.roster()
        directory = self.root / "actual"
        directory.mkdir()
        link = self.root / "alias"
        link.symlink_to(directory, target_is_directory=True)
        self.raw = link
        result = self.scan(self.client([envelope([work()])]))
        self.assertEqual(result["reason"], "artifact_symlink")
        self.assertFalse(result["complete"])
        self.assertEqual(self.calls, [])
        self.assertEqual(self.rows("content_items"), [])
        self.assertEqual(self.rows("provider_raw_responses"), [])


if __name__ == "__main__":
    unittest.main()
