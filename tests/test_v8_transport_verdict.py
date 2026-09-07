from __future__ import annotations

import json
import sqlite3
import unittest

from tests import test_v8_transport_cohort as cohort_fixture
from tests import test_v8_transport_runner as runner_fixture
from v8 import paid_drain
from v8.runtime_database import RuntimeDatabaseError
from v8.storage import connect, transaction
from v8.transport_accounting import settle_primary_member_unknown
from v8.transport_receipts import read_transport_receipt
from v8.transport_verdict import record_primary_route_verdict

AT = runner_fixture.AT
EXPIRED = "2026-09-08T05:30:00Z"


class TransportVerdictTest(unittest.TestCase):
    def setUp(self):
        self.fixture = runner_fixture.TransportRunnerTest(methodName="runTest")
        self.addCleanup(self.fixture.doCleanups)
        self.fixture.setUp()
        self.db = self.fixture.db
        self.campaign = self.fixture.fixture.fixture.campaign
        self.mirror_root = self.fixture.fixture.fixture.mirror_root

    def _record(self, *, at=AT):
        with connect(self.db) as connection, transaction(connection):
            return record_primary_route_verdict(
                connection, self.campaign["receipt_id"], at=at, mirror_root=self.mirror_root,
            )

    def _verdict_count(self):
        with connect(self.db) as connection:
            return connection.execute(
                "SELECT COUNT(*) FROM scheduler_runs WHERE job_id='transport_receipt:route_verdict'",
            ).fetchone()[0]

    def _ledger(self):
        with connect(self.db) as connection:
            result = {table: [tuple(row) for row in connection.execute(f"SELECT * FROM {table} ORDER BY id")]
                      for table in ("provider_usage", "fetch_slots", "fetch_attempts", "provider_raw_responses",
                                    "paid_provider_dispatch_events", "pipeline_paid_drain_events", "provider_budget_batches")}
            result["faults"] = [tuple(row) for row in connection.execute(
                "SELECT * FROM scheduler_runs WHERE job_id LIKE 'provider_fault_%' ORDER BY id",
            )]
            return result

    def _assert_not_authority(self, receipt):
        payload = receipt["payload"]
        self.assertEqual(receipt["kind"], "route_verdict")
        self.assertEqual(receipt["identity_key"], f"primary-route:{self.campaign['receipt_id']}")
        self.assertFalse(payload["operation_qualified"])
        self.assertFalse(payload["ordinary_paid_authorized"])
        self.assertEqual(payload["sample_limit"], 20)
        self.assertEqual(payload["required_operation_sample_size"], 200)
        with connect(self.db) as connection, transaction(connection):
            self.assertEqual(read_transport_receipt(connection, receipt["receipt_id"]), receipt)
            self.assertEqual(paid_drain.dispatch_state(connection, at=AT).state, "draining")
            with self.assertRaises(paid_drain.PaidDrainBlocked):
                paid_drain.require_paid_dispatch_open(
                    connection, provider="TikHub", operation="douyin_user_posts", at=AT,
                )

    def test_twenty_real_members_pass_idempotently_without_purchase_or_unlock(self):
        self.fixture._run()
        before = self._ledger()
        with connect(self.db) as connection:
            with self.assertRaises(RuntimeError):
                record_primary_route_verdict(connection, self.campaign["receipt_id"], at=AT, mirror_root=self.mirror_root)
        # This is evidence-only: it neither needs nor changes a scheduler state.
        self.fixture.scheduler.resume()
        receipt = self._record()
        payload = receipt["payload"]
        self.assertTrue(payload["route_passed"])
        self.assertEqual(payload["status"], "passed")
        self.assertEqual(payload["selected_route"], self.campaign["payload"]["request_transport"]["manifest"])
        self.assertEqual(payload["selected_route"]["request_host"], "api.tikhub.dev")
        self.assertEqual(payload["next_action"], "freeze_operation_samples")
        self.assertEqual((payload["effective_starts"], payload["response_complete_count"], payload["usable_page_count"]), (20, 20, 20))
        self.assertEqual((payload["transport_uncertain_count"], payload["accounted_microusd"]), (0, 20_000))
        self._assert_not_authority(receipt)
        self.assertEqual(before, self._ledger())
        with connect(self.db) as connection:
            counts = tuple(connection.execute(
                "SELECT (SELECT COUNT(*) FROM scheduler_runs),(SELECT COUNT(*) FROM scheduler_run_attempts)",
            ).fetchone())
        self.assertEqual(receipt, self._record(at="2026-09-06T05:31:00Z"))
        with connect(self.db) as connection:
            self.assertEqual(counts, tuple(connection.execute(
                "SELECT (SELECT COUNT(*) FROM scheduler_runs),(SELECT COUNT(*) FROM scheduler_run_attempts)",
            ).fetchone()))
            target = self.fixture.fixture.fixture.root / "without-writer.sqlite3"
            with sqlite3.connect(target) as unowned:
                connection.backup(unowned)
            with connect(target) as unowned, transaction(unowned):
                with self.assertRaises(RuntimeDatabaseError):
                    record_primary_route_verdict(unowned, self.campaign["receipt_id"], at=AT, mirror_root=self.mirror_root)
        self.assertEqual(self._verdict_count(), 1)
        self.assertEqual(self.fixture.calls, list(range(1, 21)))

    def test_unknown_refuses_verdict_until_conservative_accounting_then_fails_route(self):
        self.fixture.fail_rank = 1
        terminal = self.fixture._run()["receipt"]
        with self.assertRaises(RuntimeError):
            self._record()
        self.assertEqual(self._verdict_count(), 0)
        with connect(self.db) as connection, transaction(connection):
            settle_primary_member_unknown(
                connection, terminal["payload"]["members"][0]["receipt_id"],
                scheduler=self.fixture.scheduler, at=AT, mirror_root=self.mirror_root,
            )
        before = self._ledger()
        receipt = self._record()
        self.assertEqual(receipt["payload"]["status"], "failed")
        self.assertFalse(receipt["payload"]["route_passed"])
        self.assertIsNone(receipt["payload"]["selected_route"])
        self.assertEqual(receipt["payload"]["next_action"], "run_disjoint_control_arms")
        self.assertEqual(receipt["payload"]["effective_starts"], 20)
        self.assertEqual(receipt["payload"]["transport_uncertain_count"], 1)
        self.assertEqual(receipt["payload"]["accounted_microusd"], 20_000)
        self._assert_not_authority(receipt)
        self.assertEqual(before, self._ledger())
        self.assertEqual(self.fixture.calls, list(range(1, 21)))

    def test_balance_prefix_is_incomplete_without_shrinking_twenty_member_denominator(self):
        self.fixture.status = 402
        self.fixture.body_override = {"code": 402, "message": "insufficient balance"}
        terminal = self.fixture._run()["receipt"]
        self.assertEqual(len(terminal["payload"]["members"]), 20)
        before = self._ledger()
        receipt = self._record()
        self.assertEqual(receipt["payload"]["status"], "incomplete")
        self.assertFalse(receipt["payload"]["route_passed"])
        self.assertIsNone(receipt["payload"]["selected_route"])
        self.assertEqual(receipt["payload"]["next_action"], "remain_blocked")
        self.assertEqual(receipt["payload"]["effective_starts"], 1)
        self.assertEqual(receipt["payload"]["response_complete_count"], 1)
        self.assertEqual(receipt["payload"]["usable_page_count"], 0)
        self.assertEqual(receipt["payload"]["accounted_microusd"], 0)
        self._assert_not_authority(receipt)
        self.assertEqual(before, self._ledger())
        self.assertEqual(self.fixture.calls, [1])

    def test_terminal_tampering_is_rejected_even_when_a_verdict_already_exists(self):
        terminal = self.fixture._run()["receipt"]
        original = self._record()
        with connect(self.db) as connection:
            connection.execute("BEGIN IMMEDIATE")
            details = json.loads(connection.execute(
                "SELECT details_json FROM scheduler_runs WHERE id=?", (terminal["receipt_id"],),
            ).fetchone()[0])
            details["payload"]["effective_starts"] = 200
            try:
                connection.execute("UPDATE scheduler_runs SET details_json=? WHERE id=?", (json.dumps(details), terminal["receipt_id"]))
            except sqlite3.IntegrityError:
                pass  # A database-level immutable guard is also a valid rejection.
            else:
                with self.assertRaises(RuntimeError):
                    record_primary_route_verdict(connection, self.campaign["receipt_id"], at=AT, mirror_root=self.mirror_root)
            finally:
                connection.rollback()
        self.assertEqual(original, self._record())
        self.assertEqual(self._verdict_count(), 1)

    def test_expired_members_remain_historical_evidence_but_current_hold_drift_rejects(self):
        self.fixture._run()
        receipt = self._record(at=EXPIRED)
        self.assertTrue(receipt["payload"]["route_passed"])
        self._assert_not_authority(receipt)
        self.fixture.fixture.fixture._register_prerequisite(
            "config", cohort_fixture.NEW_CONFIG, at="2026-09-08T05:31:00Z",
        )
        with self.assertRaises(RuntimeError):
            self._record(at="2026-09-08T05:32:00Z")
        self.assertEqual(self._verdict_count(), 1)
        self.assertEqual(self.fixture.calls, list(range(1, 21)))
