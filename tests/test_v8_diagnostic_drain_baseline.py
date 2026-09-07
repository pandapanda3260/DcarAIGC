from __future__ import annotations

import json
import sqlite3
import unittest

from tests import test_v8_paid_drain as fixtures
from tests import test_v8_transport_cohort as raw_fixtures
from tests.schema_fixture import initialize_historical_schema
from v8 import durable_runs, paid_dispatch, paid_drain, provider_budget, storage
from v8.storage import connect, transaction

AT = fixtures.STARTED_AT


class DiagnosticDrainBaselineTest(unittest.TestCase):
    _raw = raw_fixtures.TransportCohortTest._raw

    def setUp(self):
        self.fixture = fixtures.PaidDrainTest(methodName="runTest")
        self.addCleanup(self.fixture.doCleanups)
        self.fixture.setUp()
        self.db = self.fixture.db
        self.root = self.fixture.root
        self.raw_root = self.root / "baseline-raw"

    def _start(self, drain_id="baseline"):
        return paid_drain.start_paid_drain(drain_id, binding=fixtures.binding(), db_path=self.db, now=AT)

    def test_v2_and_actual_v3_reserved_and_sent_are_frozen_exactly(self):
        reserved_id, sent_id, _matrix_attempt = self.fixture._insert_active_dispatches()
        with connect(self.db) as connection, transaction(connection):
            reserved, sent = paid_drain._current_tikhub_dispatches(connection, high_watermark=sent_id)
            self.assertEqual([row["usage_id"] for row in reserved], [reserved_id])
            self.assertEqual([row["usage_id"] for row in sent], [sent_id])
            self.assertEqual(provider_budget.POLICY_VERSION, "tikhub-global-budget-v3")
            for usage_id in (reserved_id, sent_id):
                row = connection.execute("SELECT details_json FROM provider_usage WHERE id=?", (usage_id,)).fetchone()
                details = json.loads(row["details_json"])
                details["policy_version"] = provider_budget.POLICY_VERSION
                connection.execute("UPDATE provider_usage SET details_json=? WHERE id=?", (json.dumps(details), usage_id))
        frozen = self._start().payload["frozen_dispatch"]
        self.assertEqual(frozen["tikhub_reserved"], reserved)
        self.assertEqual(frozen["tikhub_send_marked"], sent)
        self.assertEqual(frozen["tikhub_reserved_sha256"], paid_drain._digest(reserved))
        self.assertEqual(frozen["tikhub_send_marked_sha256"], paid_drain._digest(sent))
        with self.assertRaisesRegex(paid_drain.PaidDrainError, "unresolved frozen dispatches"):
            paid_drain.seal_paid_drain("baseline", db_path=self.db, now=fixtures.SEALED_AT)

    def test_all_paid_owner_classes_freeze_but_reports_and_local_children_do_not(self):
        direct_jobs = (
            "daily_capture", "content_pipeline", "comments_refresh", "metrics_backfill",
            "history_recovery", "history_scan_catalog", "pipeline_reconcile", "paid_capture_direct",
            "tikhub_reconcile", "matrix_works_scan", "matrix_account_metrics", "transport_diagnostic_operator",
        )
        round_jobs = ("matrix_works_scan", "matrix_account_metrics", "tikhub_reconcile",
                      "tikhub_works_scan", "tikhub_account_metrics", "metrics_backfill")
        claims = []
        for job in direct_jobs:
            claims.append(durable_runs.claim_run(job, {"fixture": job}, db_path=self.db, now=AT))
        for job in round_jobs:
            claims.append(durable_runs.claim_run("pipeline_round:" + job, {"job_id": job}, db_path=self.db, now=AT))
        for job in ("daily_report", "tikhub_scan_materialize", "transport_receipt:cohort"):
            durable_runs.claim_run(job, {"fixture": job}, db_path=self.db, now=AT)
        durable_runs.claim_run("pipeline_round:daily_report", {"job_id": "daily_report"}, db_path=self.db, now=AT)
        frozen = self._start().payload["frozen_dispatch"]
        self.assertTrue(all(claim is not None for claim in claims))
        self.assertEqual(
            [(row["run_id"], row["attempt_id"]) for row in frozen["paid_running_attempts"]],
            [(claim.scheduler_run_id, claim.attempt_id) for claim in claims],
        )
        self.assertEqual(frozen["paid_running_attempts_sha256"], paid_drain._digest(frozen["paid_running_attempts"]))
        self.assertEqual({row["job_id"] for row in frozen["matrix_running_dispatches"]}, {"matrix_works_scan", "matrix_account_metrics"})
        with self.assertRaisesRegex(paid_drain.PaidDrainError, "unresolved frozen dispatches"):
            paid_drain.seal_paid_drain("baseline", db_path=self.db, now=fixtures.SEALED_AT)

    def test_start_records_actual_raw_and_dispatch_event_id_high_watermarks(self):
        raw = self._raw(account_id=1, padding=100, captured_at=AT)
        claim = durable_runs.claim_run("paid_capture_direct", {"fixture": "dispatch-hwm"}, db_path=self.db, now=AT)
        with connect(self.db) as connection, transaction(connection):
            state = paid_drain.dispatch_state(connection, at=AT)
            reserved = paid_dispatch.reserve_dispatch_in_transaction(
                connection, provider="TikHub", operation="douyin_user_posts",
                activation_id=state.activation_id, business_day="2026-09-01",
                scheduler_run_id=claim.scheduler_run_id, scheduler_attempt_id=claim.attempt_id,
                scope={"purpose": "reconcile"}, cursor_identity={"cursor": 0}, created_at=AT,
            )
            paid_dispatch.mark_dispatch_sent_in_transaction(connection, reserved.dispatch_id, fetch_attempt_id=None, created_at=AT)
            terminal = paid_dispatch.finish_dispatch_in_transaction(
                connection, reserved.dispatch_id, outcome="succeeded", raw_response_id=raw["raw_id"], created_at=AT,
            )
            attempt_hwm = connection.execute("SELECT MAX(id) FROM scheduler_run_attempts").fetchone()[0]
        first = self._start()
        frozen = first.payload["frozen_dispatch"]
        self.assertEqual(frozen["scheduler_attempt_high_watermark"], attempt_hwm)
        self.assertEqual(attempt_hwm, claim.attempt_id)
        self.assertGreater(first.attempt_id, attempt_hwm)  # START is not itself pre-START work.
        self.assertEqual(frozen["raw_response_high_watermark"], raw["raw_id"])
        self.assertEqual(frozen["dispatch_event_high_watermark"], terminal.event_id)
        self.assertGreater(terminal.event_id, reserved.event_id)
        with connect(self.db) as connection:
            self.assertEqual(frozen["raw_response_high_watermark"], connection.execute("SELECT MAX(id) FROM provider_raw_responses").fetchone()[0])
            self.assertEqual(frozen["dispatch_event_high_watermark"], connection.execute("SELECT MAX(id) FROM paid_provider_dispatch_events").fetchone()[0])
        self._raw(account_id=1, padding=200, captured_at=AT)
        self.assertEqual(self._start().event_hash, first.event_hash)
        self.assertEqual(self._start().payload["frozen_dispatch"], frozen)

    def test_real_schema18_without_dispatch_table_gets_zero_dispatch_baseline(self):
        historical = self.root / "schema18.sqlite3"
        with sqlite3.connect(historical) as connection:
            connection.row_factory = sqlite3.Row
            storage.configure_connection_safety(connection)
            initialize_historical_schema(connection, target_version=17)
            storage.migrate_database(connection, from_version=17, to_version=18)
            self.assertEqual(connection.execute("PRAGMA user_version").fetchone()[0], 18)
            self.assertIsNone(connection.execute("SELECT name FROM sqlite_master WHERE name='paid_provider_dispatch_events'").fetchone())
            with transaction(connection):
                claim = durable_runs.claim_run_in_transaction(connection, "tikhub_reconcile", {"fixture": "schema18"}, now=AT)
            frozen = paid_drain._freeze_start(connection)
            self.assertEqual(frozen["dispatch_event_high_watermark"], 0)
            self.assertEqual(frozen["raw_response_high_watermark"], 0)
            self.assertEqual(frozen["scheduler_attempt_high_watermark"], claim.attempt_id)
            self.assertEqual(frozen["scheduler_attempt_high_watermark"], connection.execute("SELECT MAX(id) FROM scheduler_run_attempts").fetchone()[0])

    def test_post_start_diagnostic_tags_and_new_paid_owners_still_block_sealing(self):
        self._start()
        # A diagnostic label confers no tail exemption; this remains ordinary
        # usage evidence until a future strict, completed-member verifier exists.
        with connect(self.db) as connection, transaction(connection):
            connection.execute(
                "INSERT INTO provider_usage(provider,operation,request_attempts,billed_requests,"
                "currency,amount,recorded_at,details_json) VALUES ('TikHub','douyin_user_posts',1,1,'USD',0.001,?,?)",
                (AT, json.dumps({"state": "completed", "policy_version": provider_budget.POLICY_VERSION,
                                 "diagnostic_member": {"receipt_id": 1234}})),
            )
        for job, identity in (
            ("transport_diagnostic_operator", {"fixture": "operator"}),
            ("pipeline_round:tikhub_works_scan", {"job_id": "tikhub_works_scan"}),
            ("pipeline_round:tikhub_account_metrics", {"job_id": "tikhub_account_metrics"}),
        ):
            durable_runs.claim_run(job, identity, db_path=self.db, now=AT)
        with self.assertRaisesRegex(paid_drain.PaidDrainError, "post-START dispatch tail") as caught:
            paid_drain.seal_paid_drain("baseline", db_path=self.db, now=fixtures.SEALED_AT)
        self.assertIn("usage=[", str(caught.exception))
        self.assertNotIn("runs=[]", str(caught.exception))
        with connect(self.db) as connection:
            self.assertEqual(paid_drain.dispatch_state(connection, at=AT).state, "draining")


if __name__ == "__main__":
    unittest.main()
