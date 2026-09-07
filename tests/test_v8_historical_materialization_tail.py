from __future__ import annotations

import json
import sqlite3
import unittest
from pathlib import Path
from unittest.mock import patch

from tests import test_v8_tikhub_scan as scan_fixture
from tests.roster_fixture import accept_roster
from v8 import durable_runs, metric_observations, paid_drain, profile_control, providers, tikhub_scan
from v8.storage import connect, transaction


class HistoricalMaterializationTailTest(unittest.TestCase):
    def setUp(self):
        self.fixture = scan_fixture.TikHubScanTest(methodName="runTest")
        self.addCleanup(self.fixture.doCleanups)
        self.fixture.setUp()
        self.db = self.fixture.db
        with patch.object(providers, "materialize_account_discovery_page", side_effect=sqlite3.OperationalError("fixture local failure")):
            pending = self.fixture.scan(call_override=lambda *_: scan_fixture.result(scan_fixture.dy_page(
                [scan_fixture.dy_item(number) for number in (1, 2, 3)], more=True, cursor=7,
            )))
        self.assertEqual(pending["reason"], "materialization_pending")
        self.run_id = pending["scheduler_run_id"]
        self.original = durable_runs.get_run(self.run_id, db_path=self.db)
        with connect(self.db) as connection, transaction(connection):
            accept_roster(connection, [1, 2], accepted_at=scan_fixture.later(scan_fixture.NOW, 1))
        self.hold_at = scan_fixture.later(scan_fixture.NOW, 20)
        self.at = scan_fixture.later(scan_fixture.NOW, 302)
        self.capture_at = None
        self.drain_id = "historical-local-tail"
        profile_control.begin_current_activation_hold(
            db_path=self.db, drain_id=self.drain_id, build_receipt_sha256="3" * 64,
            runtime_root_receipt_sha256="4" * 64, actor="fixture", reason="hold after historical raw",
            not_before_business_day="2026-08-31", now=self.hold_at,
        )
        self.ledger = self._ledger()
        with connect(self.db) as connection:
            self.old_attempts = [tuple(row) for row in connection.execute("SELECT * FROM scheduler_run_attempts ORDER BY id")]
            self.fence = [tuple(row) for row in connection.execute("SELECT * FROM pipeline_paid_drain_events ORDER BY id")]
        self.enterContext(patch("v8.capture.now_utc", side_effect=lambda: self.capture_at or self.at))
        self.enterContext(patch.object(providers, "now_utc", side_effect=lambda: self.at))
        self.enterContext(patch.object(tikhub_scan, "now_utc", side_effect=lambda: self.at))
        self.enterContext(patch.object(metric_observations, "now_utc", side_effect=lambda: self.at))
        self.enterContext(patch.object(tikhub_scan, "_monotonic_now", return_value=0.0))

    def _ledger(self):
        with connect(self.db) as connection:
            return {table: [tuple(row) for row in connection.execute(f"SELECT * FROM {table} ORDER BY id")]
                    for table in ("provider_usage", "provider_budget_batches", "paid_provider_dispatch_events")}

    def _replay(self, *, limit=50):
        with patch.object(tikhub_scan, "_raw", side_effect=AssertionError("local replay cannot purchase")):
            return tikhub_scan.resume_local_materialization(
                self.run_id, db_path=self.db, raw_root=self.fixture.raw_root,
                now=self.at, max_items=limit, deadline=60.0,
            )

    def _verify(self):
        with connect(self.db) as connection:
            connection.execute("PRAGMA query_only=ON")
            connection.execute("BEGIN")
            before = connection.total_changes
            result = paid_drain.verify_profile_drain_sealable(connection, self.drain_id, now=self.at)
            self.assertEqual(connection.total_changes, before)
            return result["diagnostic_tail"]

    def test_pre_start_pending_epoch_failure_and_two_local_slices_close_exact_tail(self):
        failed = self.fixture.resume(self.run_id, now=scan_fixture.later(scan_fixture.NOW, 301),
                                     call_override=lambda *_: self.fail("superseded scan must not send"))
        self.assertEqual((failed["status"], failed["reason"]), ("failed", "profile_superseded"))
        with connect(self.db) as connection:
            failed_attempt = tuple(connection.execute("SELECT * FROM scheduler_run_attempts WHERE id=?", (failed["attempt_id"],)).fetchone())
        self.capture_at = scan_fixture.later(self.at, 2)
        first = self._replay(limit=1)
        self.assertFalse(first["materialization_finalized"])
        with self.assertRaises(paid_drain.PaidDrainError):
            self._verify()
        self.at = scan_fixture.later(scan_fixture.NOW, 306)
        self.capture_at = scan_fixture.later(self.at, 2)
        self.assertTrue(self._replay()["materialization_finalized"])
        with connect(self.db) as connection:
            latest_finish = connection.execute("SELECT MAX(response_finished_at) FROM fetch_attempts").fetchone()[0]
        source = durable_runs.get_run(self.run_id, db_path=self.db)
        self.assertGreater(latest_finish, source["completed_at"])
        self.at = scan_fixture.later(scan_fixture.NOW, 310)
        proof = self._verify()
        self.assertEqual(len(proof["verified_ids"]["paid_attempt_ids"]), 3)
        self.assertEqual(len(proof["verified_ids"]["materialization_attempt_ids"]), 2)
        self.assertEqual(proof["verified_ids"]["usage_ids"], [])
        self.assertEqual(proof["verified_ids"]["dispatch_event_ids"], [])
        self.assertGreater(len(proof["verified_ids"]["raw_response_ids"]), 0)
        self.assertEqual(proof, self._verify())
        self.assertEqual(self._ledger(), self.ledger)
        with connect(self.db) as connection:
            self.assertEqual(tuple(connection.execute("SELECT * FROM scheduler_run_attempts WHERE id=?", (failed["attempt_id"],)).fetchone()), failed_attempt)
            for row in self.old_attempts:
                self.assertEqual(tuple(connection.execute("SELECT * FROM scheduler_run_attempts WHERE id=?", (row[0],)).fetchone()), row)
            self.assertEqual([tuple(row) for row in connection.execute("SELECT * FROM pipeline_paid_drain_events ORDER BY id")], self.fence)

    def test_real_local_replay_then_extra_zero_usage_is_not_whitelisted(self):
        self.assertTrue(self._replay()["materialization_finalized"])
        self._verify()
        with connect(self.db) as connection, transaction(connection):
            connection.execute(
                "INSERT INTO provider_usage(provider,operation,request_attempts,currency,amount,recorded_at,details_json) "
                "VALUES ('TikHub','douyin_video_detail',0,'USD',0,?,?)",
                (self.at, json.dumps({"state": "local_only", "source": "historical_materialization"})),
            )
        with self.assertRaisesRegex(paid_drain.PaidDrainError, "usage_ids"):
            self._verify()

    def test_extra_attempt_with_local_label_but_no_child_owner_is_rejected(self):
        self.assertTrue(self._replay()["materialization_finalized"])
        self.at = scan_fixture.later(scan_fixture.NOW, 305)
        claim = durable_runs.claim_run(
            "tikhub_reconcile", self.original["details"]["identity"], db_path=self.db,
            now=self.at, invocation_source="operator_retry",
        )
        self.assertIsNotNone(claim)
        durable_runs.finish_run(claim, status="partial", db_path=self.db, now=self.at,
                                next_resume_at=self.at, summary={"reason": "materialization_replay_yield", "local_only": True})
        with self.assertRaises(paid_drain.PaidDrainError):
            self._verify()

    def test_changed_frozen_manifest_is_rejected_even_after_successful_replay(self):
        self.assertTrue(self._replay()["materialization_finalized"])
        self._verify()
        head = self.original["details"]["checkpoint"]["last_manifest"]
        Path(head["path"]).write_bytes(b'{"tampered":true}')
        with self.assertRaises(paid_drain.PaidDrainError):
            self._verify()

    def test_local_wall_time_beyond_verification_time_is_rejected(self):
        self.capture_at = scan_fixture.later(self.at, 2)
        self.assertTrue(self._replay()["materialization_finalized"])
        with self.assertRaisesRegex(paid_drain.PaidDrainError, "materialization window"):
            self._verify()
        self.at = scan_fixture.later(self.at, 3)
        self._verify()

    def test_source_operation_cannot_change_with_unchanged_raw_bytes(self):
        self.assertTrue(self._replay()["materialization_finalized"])
        raw_id = self.original["details"]["checkpoint"]["pending_materialization"]["identity"]["raw_response_id"]
        with connect(self.db) as connection, transaction(connection):
            connection.execute("UPDATE provider_raw_responses SET operation='xiaohongshu_user_posts' WHERE id=?", (raw_id,))
        with self.assertRaisesRegex(paid_drain.PaidDrainError, "pre-START account raw"):
            self._verify()


if __name__ == "__main__":
    unittest.main()
