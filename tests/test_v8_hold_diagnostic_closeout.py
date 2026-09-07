from __future__ import annotations

import copy
import json
import unittest
from unittest.mock import patch

from tests import test_v8_transport_accounting as accounting_fixture
from tests import test_v8_transport_operator_command as command_fixture
from v8 import paid_drain, profile_control, transport_accounting
from v8.storage import connect, transaction
from v8.transport_receipts import read_transport_receipt

AT = accounting_fixture.AT


class HoldDiagnosticCloseoutTest(unittest.TestCase):
    def setUp(self):
        self.base = accounting_fixture.TransportAccountingTest(methodName="runTest")
        self.addCleanup(self.base.doCleanups)
        self.base.setUp()
        self.db = self.base.db
        self.runner = self.base.fixture
        self.hold = self.runner.fixture.fixture.campaign["payload"]["hold_binding"]
        self.mirror = self.runner.fixture.fixture.mirror_root

    def closeout(self, connection):
        return transport_accounting.settle_closed_hold_unknowns(
            connection, drain_id=self.hold["drain_id"], scheduler=self.runner.scheduler, at=AT, mirror_root=self.mirror,
        )

    def test_verified_unknown_closes_exact_tail_without_releasing_billing_or_identity_guards(self):
        before = self.base._ledger()
        with connect(self.db) as connection, transaction(connection):
            with self.assertRaises(paid_drain.PaidDrainError):
                paid_drain.verify_profile_drain_sealable(connection, self.hold["drain_id"], now=AT)
            receipts = self.closeout(connection)
            verified = paid_drain.verify_profile_drain_sealable(connection, self.hold["drain_id"], now=AT)
            self.assertEqual(len(receipts), 1)
            self.assertFalse(receipts[0]["provider_bill_verified"])
            self.assertFalse(verified["diagnostic_tail"]["qualified"])
            self.assertEqual(self.closeout(connection), [])
        self.assertEqual(before, self.base._ledger())
        self.assertEqual(len(self.runner.calls), 20)
        state = self.base._read()
        self.assertEqual(state["state"], "billing_unknown")
        self.assertEqual(state["accounting_state"], "charged_unverified")
        self.assertFalse(state["billing_settled"])

    def test_active_owner_is_rejected_before_any_usage_changes(self):
        operator = self.runner.fixture.fixture.operator
        with connect(self.db) as connection, transaction(connection):
            connection.execute("UPDATE scheduler_runs SET status='running' WHERE id=?", (operator.scheduler_run_id,))
        with connect(self.db) as connection, transaction(connection):
            before = [tuple(row) for row in connection.execute("SELECT * FROM provider_usage ORDER BY id")]
            with self.assertRaises(RuntimeError):
                self.closeout(connection)
            self.assertEqual(before, [tuple(row) for row in connection.execute("SELECT * FROM provider_usage ORDER BY id")])
        self.assertFalse(self.base._read()["accounting_terminal"])

    def test_control_arm_uses_its_own_terminal_key(self):
        with connect(self.db) as connection:
            member = read_transport_receipt(connection, self.base.member_id)
            campaign_id = member["payload"]["campaign_receipt_id"]
            original_terminal = connection.execute(
                "SELECT id FROM scheduler_runs WHERE job_id='transport_receipt:campaign_terminal' AND scheduled_for=?",
                (f"primary-execution:{campaign_id}",),
            ).fetchone()

            class ArmConnection:
                def execute(_self, query, parameters=()):
                    if "campaign_terminal" in query and "scheduled_for=?" in query:
                        self.assertEqual(parameters, (f"control_legacy-execution:{campaign_id}",))
                        return connection.execute("SELECT id FROM scheduler_runs WHERE id=?", (original_terminal["id"],))
                    return connection.execute(query, parameters)

            control_member = copy.deepcopy(member)
            control_member["payload"]["arm"] = "control_legacy"
            terminal = transport_accounting._closed_campaign(ArmConnection(), control_member, AT)
            self.assertEqual(terminal["receipt_id"], original_terminal["id"])

    def test_build_advance_closes_terminal_unknown_before_existing_strict_check(self):
        values = {"build": "a" * 64, "runtime": "b" * 64, "config": "c" * 64}
        for kind, artifact in values.items():
            profile_control.record_current_activation_hold_prerequisite(
                db_path=self.db, drain_id=self.hold["drain_id"], kind=kind, artifact_sha256=artifact,
                receipt_contract_version=profile_control.CURRENT_HOLD_PREREQUISITE_CONTRACTS[kind],
                expires_at="2026-09-10T00:00:00Z", evidence={"valid": True, "readback": True, "artifact_sha256": artifact},
                actor="fixture", generation=2, now=AT, mirror_root=self.mirror,
            )
        result = profile_control.advance_current_activation_hold_build(
            db_path=self.db, drain_id=self.hold["drain_id"], from_build_receipt_sha256=self.hold["build_receipt_sha256"],
            to_build_receipt_sha256=values["build"], runtime_root_receipt_sha256=values["runtime"],
            config_receipt_sha256=values["config"], actor="fixture", reason="forward build", now=AT,
            mirror_root=self.mirror, scheduler=self.runner.scheduler,
        )
        self.assertIn("build_advance", result)
        self.assertTrue(self.base._read()["accounting_terminal"])
        self.assertEqual(self.runner.scheduler.state, 2)


class DiagnosticRunnerCloseoutTest(unittest.TestCase):
    def test_later_verdict_failure_does_not_roll_back_local_conservative_closeout(self):
        base = command_fixture.TransportOperatorCommandTest(methodName="runTest")
        self.addCleanup(base.doCleanups)
        base.setUp()
        base.fixture.fail_rank = 1
        base._enqueue()
        with patch("v8.transport_verdict.record_primary_route_verdict", side_effect=RuntimeError("fixture later verdict failure")):
            result = base._process()
        self.assertEqual(result["processed"][0]["status"], "failed")
        with connect(base.fixture.db) as connection:
            states = [json.loads(row[0]).get("state") for row in connection.execute("SELECT details_json FROM provider_usage")]
            self.assertEqual(states.count("charged_unverified"), 1)
            self.assertEqual(states.count("billing_unknown"), 0)
            self.assertEqual(connection.execute("SELECT COUNT(*) FROM paid_provider_dispatch_events WHERE event_type='billing_unknown'").fetchone()[0], 1)
        self.assertEqual(len(base.fixture.calls), 20)


if __name__ == "__main__":
    unittest.main()
