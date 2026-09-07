from __future__ import annotations

import json
import unittest
from contextlib import contextmanager
from dataclasses import asdict
from unittest.mock import patch

from tests import test_v8_transport_operator_command as command_fixture
from tests import test_v8_transport_execution as execution_fixture
from tests import test_v8_transport_runner as runner_fixture
from v8 import capture, durable_runs, metric_observations, paid_drain, pipeline, profile_control
from v8 import providers, tikhub_scan, transport_execution, transport_natural_due, transport_runner
from v8.source_routing import parse_time
from v8.storage import connect, transaction
from v8.transport_due_candidates import list_primary_due_candidates
from v8.transport_members import OPERATOR_JOB, primary_operator_identity
from v8.transport_owner_evidence import read_primary_campaign_owners
from v8.transport_preparation import prepare_primary_due_inventory
from v8.transport_verdict import record_primary_route_verdict

NOW = "2026-09-06T16:03:00Z"  # 00:03 Beijing: no current-day discovery cron is due.


class TransportOnDemandTest(unittest.TestCase):
    def setUp(self):
        self.command = command_fixture.TransportOperatorCommandTest(methodName="runTest")
        self.addCleanup(self.command.doCleanups)
        self.command.setUp()
        self.runner = self.command.fixture
        self.base = self.command.base
        self.db = self.runner.db
        for module in (capture, metric_observations, profile_control, providers, tikhub_scan,
                       transport_execution, transport_runner):
            self.enterContext(patch.object(module, "now_utc", return_value=NOW))
        self.enterContext(patch.object(runner_fixture, "AT", NOW))

    def _rows(self, connection):
        return {table: [tuple(row) for row in connection.execute(f"SELECT * FROM {table} ORDER BY id")]
                for table in ("provider_usage", "paid_provider_dispatch_events", "fetch_attempts",
                              "provider_raw_responses", "pipeline_paid_drain_events", "scheduler_runs",
                              "scheduler_run_attempts")}

    def test_midnight_command_runs_twenty_and_terminal_owner_tail_verdict_replay(self):
        self.assertIsNone(pipeline._scheduled_round_at("tikhub_works_scan", parse_time(NOW)))
        queued = self.command._enqueue()
        processed = self.command._process()
        self.assertEqual(processed["processed"][0]["status"], "succeeded", processed)
        result = self.command._status(queued["run_id"])["result"]
        self.assertTrue(result["route_passed"])
        self.assertFalse(result["operation_qualified"])
        self.assertFalse(result["ordinary_paid_authorized"])
        self.assertEqual(self.runner.calls, list(range(1, 21)))
        with connect(self.db) as connection, transaction(connection):
            before = self._rows(connection)
            parents = connection.execute(
                "SELECT details_json FROM scheduler_runs WHERE job_id LIKE 'pipeline_round:%'"
            ).fetchall()
            self.assertEqual(len(parents), 1)
            identity = json.loads(parents[0][0])["identity"]
            self.assertEqual(identity["registration_id"], transport_natural_due.OPERATOR_REGISTRATION)
            self.assertEqual((identity["source"], identity["due_kind"]), ("operator", "on_demand"))
            self.assertEqual(identity["scheduled_at"], NOW)
            self.assertEqual(identity["operator_due"]["command_run_id"], queued["run_id"])
            owners = read_primary_campaign_owners(connection, result["campaign_receipt_id"], at=NOW)
            self.assertEqual(len(owners["paid_run_ids"]), 22)
            tail = paid_drain.verify_profile_drain_sealable(
                connection, self.base.campaign["payload"]["hold_binding"]["drain_id"], now=NOW,
            )
            self.assertEqual(len(tail["diagnostic_tail"]["verified_ids"]["usage_ids"]), 20)
            repeated = record_primary_route_verdict(
                connection, result["campaign_receipt_id"], at=NOW, mirror_root=self.base.mirror_root,
            )
            self.assertEqual(repeated["receipt_id"], result["verdict_receipt_id"])
            self.assertEqual(self._rows(connection), before)
            self.assertEqual(paid_drain.dispatch_state(connection, at=NOW).state, "draining")
        self.assertEqual(self.command._enqueue()["run_id"], queued["run_id"])
        self.assertEqual(self.command._process()["count"], 0)

    def test_real_new_window_does_not_reuse_the_prior_cron_page_identity(self):
        # Derive yesterday's real cron scopes in a rolled-back fixture
        # transaction; no old source, permit or ledger row is rewritten.
        with connect(self.db) as connection:
            connection.execute("BEGIN IMMEDIATE")
            operator = durable_runs.claim_run_in_transaction(
                connection, OPERATOR_JOB, primary_operator_identity(self.base.campaign),
                now=execution_fixture.AT, invocation_source="operator_retry",
            )
            prepare_primary_due_inventory(
                connection, campaign_receipt_id=self.base.campaign["receipt_id"], operator_claim=operator,
                scheduler=self.runner.scheduler, at=execution_fixture.AT,
            )
            old = {item["request_identity"].scope_identity for item in list_primary_due_candidates(
                connection, at=execution_fixture.AT,
            )}
            connection.rollback()
        self.assertEqual(len(old), 60)
        self.command._enqueue()
        processed = self.command._process()
        self.assertEqual(processed["processed"][0]["status"], "succeeded", processed)
        with connect(self.db) as connection:
            permits = [json.loads(row[0])["payload"] for row in connection.execute(
                "SELECT details_json FROM scheduler_runs WHERE job_id='transport_receipt:member_permit'"
            )]
        self.assertEqual(len(permits), 20)
        self.assertFalse(old & {permit["paid_scope_identity"] for permit in permits})
        self.assertTrue(all(permit["sequence"] == 0 for permit in permits))
        self.assertTrue(all(permit["natural_due"]["request_document"]["request_window"]["end"] == NOW
                            for permit in permits))

    def test_fabricated_on_demand_parent_without_a_real_command_is_rejected(self):
        parent = {"registration_id": transport_natural_due.OPERATOR_REGISTRATION,
                  "job_id": "tikhub_works_scan", "source": "operator", "due_kind": "on_demand",
                  "scheduled_at": NOW, "operator_due": {"command_run_id": 999999,
                  "command_attempt_id": 999999, "campaign_receipt_id": self.base.campaign["receipt_id"],
                  "operator_claim": asdict(self.base.operator)}}
        with connect(self.db) as connection, self.assertRaises(transport_natural_due.NaturalDueError):
            transport_natural_due._round_schedule(parent, now=parse_time(NOW), active=parent, connection=connection)
        self.assertEqual(self.runner.calls, [])

    def test_command_owner_drift_after_semaphore_prevents_network_start(self):
        queued = self.command._enqueue()
        semaphore = capture.TIKHUB_NETWORK_SLOTS

        @contextmanager
        def changed_owner():
            with connect(self.db) as connection, transaction(connection):
                row = connection.execute("SELECT details_json FROM scheduler_runs WHERE id=?", (queued["run_id"],)).fetchone()
                details = json.loads(row[0])
                details["primary_operator"]["claim"]["owner_token"] = "not-the-operator"
                connection.execute("UPDATE scheduler_runs SET details_json=? WHERE id=?",
                                   (json.dumps(details), queued["run_id"]))
            with semaphore:
                yield

        with patch.object(capture, "TIKHUB_NETWORK_SLOTS", changed_owner()):
            processed = self.command._process()
        self.assertEqual(processed["processed"][0]["status"], "failed", processed)
        self.assertEqual(self.runner.calls, [])
        with connect(self.db) as connection:
            usage = connection.execute("SELECT request_attempts,amount,details_json FROM provider_usage").fetchall()
            self.assertEqual(len(usage), 1)
            self.assertEqual((usage[0][0], usage[0][1]), (0, 0))
            self.assertEqual(json.loads(usage[0][2])["state"], "not_sent")
            self.assertEqual(connection.execute("SELECT COUNT(*) FROM fetch_attempts").fetchone()[0], 0)


if __name__ == "__main__":
    unittest.main()
