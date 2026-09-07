from __future__ import annotations

import json
import unittest
from pathlib import Path
from unittest.mock import patch

from tests import test_v8_transport_runner as runner_fixture
from v8 import capture, provider_budget, transport_accounting
from v8.storage import connect, transaction
from v8.transport_accounting import read_primary_member_accounting, settle_primary_member_unknown
from v8.transport_evidence import DiagnosticEvidenceError
from v8.transport_receipts import TransportReceiptError, read_transport_receipt

AT = runner_fixture.AT


class TransportAccountingTest(unittest.TestCase):
    def setUp(self):
        self.fixture = runner_fixture.TransportRunnerTest(methodName="runTest")
        self.addCleanup(self.fixture.doCleanups)
        self.fixture.setUp()
        self.fixture.fail_rank = 1
        self.db = self.fixture.db
        self.result = self.fixture._run()["receipt"]
        self.member_id = self.result["payload"]["members"][0]["receipt_id"]

    def _settle(self, *, at=AT):
        with connect(self.db) as connection, transaction(connection):
            return settle_primary_member_unknown(
                connection, self.member_id, scheduler=self.fixture.scheduler,
                at=at, mirror_root=self.fixture.fixture.fixture.mirror_root,
            )

    def _read(self, *, at=AT):
        with connect(self.db) as connection:
            connection.execute("PRAGMA query_only=ON")
            result = read_primary_member_accounting(connection, self.member_id, at=at)
            self.assertEqual(connection.total_changes, 0)
            return result

    def _ledger(self):
        with connect(self.db) as connection:
            return {
                "usage": list(connection.execute("SELECT id,request_attempts,billed_requests,amount,budget_batch_id FROM provider_usage ORDER BY id")),
                "attempts": list(connection.execute("SELECT * FROM fetch_attempts ORDER BY id")),
                "slots": list(connection.execute("SELECT * FROM fetch_slots ORDER BY id")),
                "batches": list(connection.execute("SELECT * FROM provider_budget_batches ORDER BY id")),
                "faults": list(connection.execute("SELECT * FROM scheduler_runs WHERE job_id LIKE 'provider_fault_%' ORDER BY id")),
            }

    def test_conservative_accounting_retains_every_charge_and_retry_guard_once(self):
        before = self._ledger()
        self.assertFalse(self._read()["accounting_terminal"])
        result = self._settle()
        self.assertTrue(result["accounting_terminal"])
        self.assertFalse(result["provider_bill_verified"])
        self.assertFalse(result["billing_settled"])
        self.assertFalse(result["qualified"])
        self.assertEqual(result["amount_microusd"], 1000)
        self.assertEqual(result["accounting_state"], "charged_unverified")
        self.assertEqual(before, self._ledger())
        self.assertEqual(result, self._read(at="2026-09-10T00:00:00Z"))
        with connect(self.db) as connection:
            count = connection.execute("SELECT COUNT(*) FROM scheduler_runs").fetchone()[0]
            summary = provider_budget.budget_summary(connection, at=AT)
            self.assertEqual(summary["total_microusd"], 20_000)
            self.assertEqual(summary["billing_unknown"]["unresolved_count"], 0)
            self.assertEqual(summary["charged_unverified"]["count"], 1)
            self.assertEqual(summary["charged_unverified"]["amount_microusd"], 1000)
            self.assertEqual(summary["charged_unverified"]["budget_day_microusd"], 1000)
            usage = connection.execute("SELECT details_json FROM provider_usage ORDER BY id LIMIT 1").fetchone()
            details = json.loads(usage[0])
            self.assertNotIn("billing_reconciliation", details)
            self.assertNotIn("settlement_receipt_id", details)  # Cannot authorize compensation.
            slot = connection.execute("SELECT * FROM fetch_slots WHERE id=?", (details["slot_id"],)).fetchone()
            self.assertEqual(slot["last_error_code"], capture.BILLING_UNKNOWN_SLOT_ERROR)
        self.assertEqual(result, self._settle())
        with connect(self.db) as connection:
            self.assertEqual(connection.execute("SELECT COUNT(*) FROM scheduler_runs").fetchone()[0], count)
        self.assertEqual(len(self.fixture.calls), 20)

    def test_mutable_charged_state_without_receipt_is_not_accepted(self):
        with connect(self.db) as connection, transaction(connection):
            connection.execute("UPDATE provider_usage SET details_json=json_set(details_json,'$.state','charged_unverified') WHERE id=(SELECT MIN(id) FROM provider_usage)")
        with self.assertRaises(DiagnosticEvidenceError):
            self._read()
        with self.assertRaises(DiagnosticEvidenceError):
            self._settle()

    def test_tampered_settlement_mirror_is_not_accepted(self):
        result = self._settle()
        with connect(self.db) as connection:
            receipt = read_transport_receipt(connection, result["accounting_receipt_id"])
        Path(receipt["mirror"]["path"]).write_bytes(b"{}")
        with self.assertRaises(TransportReceiptError):
            self._read()

    def test_releasing_original_unknown_guard_is_not_accepted(self):
        self._settle()
        with connect(self.db) as connection, transaction(connection):
            connection.execute("UPDATE fetch_slots SET last_error_code=NULL WHERE last_error_code=?", (capture.BILLING_UNKNOWN_SLOT_ERROR,))
        with self.assertRaises(DiagnosticEvidenceError):
            self._read()

    def test_failed_receipt_write_rolls_back_without_billing_changes(self):
        before = self._ledger()
        with patch.object(transport_accounting, "append_transport_receipt", side_effect=OSError("fixture mirror unavailable")):
            with self.assertRaises(OSError):
                self._settle()
        self.assertEqual(before, self._ledger())
        self.assertFalse(self._read()["accounting_terminal"])

    def test_accounting_cannot_run_with_active_scheduler(self):
        self.fixture.scheduler.resume()
        with self.assertRaises(DiagnosticEvidenceError):
            self._settle()
        self.assertFalse(self._read()["accounting_terminal"])

    def test_accounting_cannot_override_known_provider_billing(self):
        self.member_id = self.result["payload"]["members"][1]["receipt_id"]
        with self.assertRaises(DiagnosticEvidenceError):
            self._settle()
        self.assertTrue(self._read()["accounting_terminal"])
