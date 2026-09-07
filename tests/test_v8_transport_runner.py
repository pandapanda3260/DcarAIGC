from __future__ import annotations

import json
import unittest
from types import SimpleNamespace
from unittest.mock import patch

from apscheduler.schedulers.background import BackgroundScheduler  # type: ignore[import-untyped]

from tests import test_v8_transport_execution as execution_fixture
from tests import test_v8_transport_members as member_fixture
from v8 import metric_observations, providers, tikhub_scan, transport_execution, transport_runner
from v8.runtime_database import acquire_writer_lock
from v8.storage import connect, transaction
from v8.transport_due_candidates import list_primary_due_candidates
from v8.transport_runner import run_primary_campaign

AT = execution_fixture.AT


class TransportRunnerTest(unittest.TestCase):
    def setUp(self):
        fixture = member_fixture.TransportMembersTest(methodName="runTest")
        self.addCleanup(fixture.doCleanups)
        fixture.setUp()
        self.db = fixture.db
        self.raw_root = fixture.root / "runner-raw"
        self.enterContext(acquire_writer_lock(fixture.writer_access))
        self.scheduler = BackgroundScheduler(timezone="UTC")
        self.scheduler.start(paused=True)
        self.addCleanup(self.scheduler.shutdown, wait=False)
        self.fixture = SimpleNamespace(fixture=fixture, candidates=[])
        self.calls = []
        self.bodies = {}
        self.fail_rank = None
        self.more = False
        self.status = 200
        self.body_override = None
        self.omit_length = False
        for module in (providers, tikhub_scan, transport_execution, transport_runner, metric_observations):
            self.enterContext(patch.object(module, "now_utc", return_value=AT))
        self.enterContext(patch("v8.capture.now_utc", return_value=AT))
        self.enterContext(patch.object(providers, "_load_key", return_value="fixture-no-real-key"))
        self.enterContext(patch.object(providers, "request_json_transport", side_effect=self._http))
        with connect(self.db) as connection, transaction(connection):
            for identity_id in range(1, 21):
                connection.execute(
                    "INSERT INTO account_provider_references(account_identity_id,provider,reference_kind,"
                    "reference_value,created_at,updated_at) VALUES (?,'TikHub','sec_user_id',?,?,?)",
                    (identity_id, "MS4wLjAB" + "r" * 40 + str(identity_id), AT, AT),
                )

    def _http(self, request, **kwargs):
        if not self.fixture.candidates:
            with connect(self.db) as connection:
                self.fixture.candidates = list_primary_due_candidates(connection, at=AT)
                # First request cannot precede the complete fixed member batch.
                # Control arms add their own fixed batch after the primary
                # campaign, so the database may legitimately contain more than
                # one 20-member batch.
                self.assertGreaterEqual(connection.execute(
                    "SELECT COUNT(*) FROM scheduler_runs WHERE job_id='transport_receipt:member_permit'",
                ).fetchone()[0], 20)
        return execution_fixture.TransportExecutionTest._http(self, request, **kwargs)

    def _run(self):
        fixture = self.fixture.fixture
        return run_primary_campaign(
            fixture.campaign["receipt_id"], operator_claim=fixture.operator,
            scheduler=self.scheduler, db_path=self.db, raw_root=self.raw_root,
            mirror_root=fixture.mirror_root,
        )

    def _assert_no_running_inventory(self):
        with connect(self.db) as connection:
            self.assertEqual(connection.execute(
                "SELECT COUNT(*) FROM scheduler_run_attempts WHERE status='running'",
            ).fetchone()[0], 0)

    def test_natural_inventory_full_batch_terminal_receipt_and_repeat_are_bounded(self):
        result = self._run()
        payload = result["receipt"]["payload"]
        self.assertFalse(result["already_completed"])
        self.assertEqual(self.calls, list(range(1, 21)))
        self.assertTrue(payload["sample_complete"])
        self.assertFalse(payload["qualified"])
        self.assertEqual(payload["charged_microusd"], 20_000)
        self.assertEqual(payload["effective_starts"], 20)
        self.assertEqual(payload["unresolved_billing_count"], 0)
        self._assert_no_running_inventory()
        with connect(self.db) as connection:
            before = connection.execute("SELECT COUNT(*) FROM scheduler_run_attempts").fetchone()[0]
        repeated = self._run()
        self.assertTrue(repeated["already_completed"])
        self.assertEqual(repeated["receipt"], result["receipt"])
        self.assertEqual(len(self.calls), 20)
        with connect(self.db) as connection:
            self.assertEqual(connection.execute("SELECT COUNT(*) FROM scheduler_run_attempts").fetchone()[0], before)

    def test_truncated_first_rank_is_preserved_and_never_qualified(self):
        self.fail_rank = 1
        result = self._run()["receipt"]["payload"]
        self.assertEqual(self.calls, list(range(1, 21)))
        self.assertTrue(result["sample_complete"])
        self.assertFalse(result["qualified"])
        self.assertEqual(result["unresolved_billing_count"], 1)
        self.assertEqual(result["results"][0]["dispatch_terminal"], "billing_unknown")
        self.assertEqual(result["results"][0]["rank"], 1)
        self.assertEqual(result["charged_microusd"], 20_000)
        self._assert_no_running_inventory()

    def test_balance_error_finishes_inventory_without_replacing_unused_ranks(self):
        self.status = 402
        self.body_override = {"code": 402, "message": "insufficient balance"}
        result = self._run()["receipt"]["payload"]
        self.assertEqual(self.calls, [1])
        self.assertFalse(result["sample_complete"])
        self.assertFalse(result["qualified"])
        self.assertEqual(len(result["members"]), 20)
        self.assertEqual(len(result["results"]), 2)
        self.assertEqual(result["effective_starts"], 1)
        self.assertEqual(result["charged_microusd"], 0)
        self._assert_no_running_inventory()

    def test_insufficient_natural_members_roll_back_preparation_without_purchase(self):
        with connect(self.db) as connection, transaction(connection):
            connection.execute("DELETE FROM account_provider_references WHERE account_identity_id>6")
        with self.assertRaises(RuntimeError):
            self._run()
        self.assertEqual(self.calls, [])
        self._assert_no_running_inventory()
        with connect(self.db) as connection:
            self.assertEqual(connection.execute("SELECT COUNT(*) FROM provider_usage").fetchone()[0], 0)
            self.assertEqual(connection.execute(
                "SELECT COUNT(*) FROM scheduler_runs WHERE job_id='tikhub_reconcile' OR job_id LIKE 'pipeline_round:%'",
            ).fetchone()[0], 0)
            details = json.loads(connection.execute(
                "SELECT details_json FROM scheduler_runs WHERE id=?", (self.fixture.fixture.operator.scheduler_run_id,),
            ).fetchone()[0])
            self.assertNotIn("primary_due_inventory", details["checkpoint"])
