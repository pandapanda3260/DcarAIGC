from __future__ import annotations

import unittest
from unittest.mock import patch

from tests import test_v8_transport_runner as runner_fixture
from v8 import durable_runs, profile_control
from v8.storage import connect


class TransportOperatorCommandTest(unittest.TestCase):
    def setUp(self):
        self.fixture = runner_fixture.TransportRunnerTest(methodName="runTest")
        self.addCleanup(self.fixture.doCleanups)
        # This fixture normally preclaims its operator. Here the production
        # command must create that operator itself, inside the queue transaction.
        with patch("v8.durable_runs.claim_run", return_value=durable_runs.DurableClaim(0, 0, 0, "fixture", "fixture")):
            self.fixture.setUp()
        self.base = self.fixture.fixture.fixture
        self.enterContext(patch.object(profile_control, "now_utc", return_value=runner_fixture.AT))
        self.enterContext(patch("v8.providers._freeze_tikhub_transport", return_value=self.base.transport))
        self.enterContext(patch("v8.capture.RAW_ROOT", self.fixture.raw_root))

    def _enqueue(self, command_id="primary-test"):
        return profile_control.enqueue_current_activation_hold_command(
            db_path=self.fixture.db, command_id=command_id, command="transport_primary",
            parameters={"drain_id": self.base.campaign["payload"]["hold_binding"]["drain_id"]},
        )

    def _process(self, *, scheduler=True):
        return profile_control.process_current_activation_hold_commands(
            db_path=self.fixture.db, mirror_root=self.base.mirror_root,
            scheduler=self.fixture.scheduler if scheduler else None,
        )

    def _status(self, run_id):
        with connect(self.fixture.db) as connection:
            return profile_control.current_activation_hold_command_status(connection, run_id=run_id)

    def test_existing_queue_executes_twenty_then_duplicate_is_zero_purchase(self):
        queued = self._enqueue()
        batch = self._process()
        self.assertEqual(batch["processed"][0]["status"], "succeeded", batch)
        result = self._status(queued["run_id"])["result"]
        self.assertTrue(result["route_passed"])
        self.assertFalse(result["ordinary_paid_authorized"])
        self.assertEqual(result["accounted_microusd"], 20_000)
        self.assertEqual(self.fixture.calls, list(range(1, 21)))
        self.assertEqual(self._enqueue()["run_id"], queued["run_id"])
        self.assertEqual(self._process()["count"], 0)
        duplicate = self._enqueue("another-command")
        self.assertEqual(self._process()["processed"][0]["status"], "failed")
        self.assertEqual(self._status(duplicate["run_id"])["error"]["code"], "diagnostic_operator_exists")
        self.assertEqual(self.fixture.calls, list(range(1, 21)))

    def test_unknown_is_conservatively_accounted_and_route_stays_failed(self):
        self.fixture.fail_rank = 1
        queued = self._enqueue()
        batch = self._process()
        self.assertEqual(batch["processed"][0]["status"], "succeeded", batch)
        result = self._status(queued["run_id"])["result"]
        self.assertEqual(result["status"], "failed")
        self.assertEqual(result["original_billing_unknown_count"], 1)
        self.assertEqual(result["accounted_microusd"], 20_000)
        self.assertFalse(result["ordinary_paid_authorized"])

    def test_missing_or_running_scheduler_never_creates_operator_or_sends(self):
        self._enqueue()
        self.assertEqual(self._process(scheduler=False)["processed"][0]["error"]["code"], "diagnostic_scheduler_active")
        self.fixture.scheduler.resume()
        self._enqueue("running-scheduler")
        self.assertEqual(self._process()["processed"][0]["error"]["code"], "diagnostic_scheduler_active")
        with connect(self.fixture.db) as connection:
            self.assertEqual(connection.execute(
                "SELECT COUNT(*) FROM scheduler_runs WHERE job_id='transport_diagnostic_operator'",
            ).fetchone()[0], 0)
        self.assertEqual(self.fixture.calls, [])

    def test_request_cannot_override_targets_route_or_cost(self):
        for extra in ("scheduler", "raw_root", "sample_limit", "request_transport", "max_cost_microusd"):
            with self.subTest(extra=extra), self.assertRaises(profile_control.ProfileControlError):
                profile_control.enqueue_current_activation_hold_command(
                    db_path=self.fixture.db, command_id="invalid-command", command="transport_primary",
                    parameters={"drain_id": "current", extra: "forbidden"},
                )
        self.assertEqual(self.fixture.calls, [])
