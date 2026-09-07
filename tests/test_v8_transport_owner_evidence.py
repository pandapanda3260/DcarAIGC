from __future__ import annotations

import json
import sqlite3
import time
import unittest
from contextlib import ExitStack
from unittest.mock import patch

from tests import test_v8_transport_runner as runner_fixture
from v8 import capture, durable_runs, metric_observations, providers, tikhub_scan
from v8.storage import connect, transaction
from v8.transport_owner_evidence import DiagnosticOwnerEvidenceError, read_primary_campaign_owners

AT = runner_fixture.AT
LATER = "2026-09-10T00:00:00Z"
RECOVERY_AT = "2026-09-06T05:36:00Z"


class TransportOwnerEvidenceTest(unittest.TestCase):
    def setUp(self):
        self.fixture = runner_fixture.TransportRunnerTest(methodName="runTest")
        self.addCleanup(self.fixture.doCleanups)
        self.fixture.setUp()
        self.db = self.fixture.db
        self.campaign_id = self.fixture.fixture.fixture.campaign["receipt_id"]

    def _read(self):
        with connect(self.db) as connection:
            connection.execute("PRAGMA query_only=ON")
            before = connection.total_changes
            result = read_primary_campaign_owners(connection, self.campaign_id, at=LATER)
            self.assertEqual(connection.total_changes, before)
            return result

    def _details(self, run_id):
        with connect(self.db) as connection:
            return json.loads(connection.execute(
                "SELECT details_json FROM scheduler_runs WHERE id=?", (run_id,),
            ).fetchone()[0])

    def test_full_twenty_closes_exact_inventory_and_local_owners_read_only(self):
        terminal = self.fixture._run()["receipt"]
        result = self._read()
        self.assertEqual(self.fixture.calls, list(range(1, 21)))
        self.assertEqual(result["terminal_receipt_id"], terminal["receipt_id"])
        self.assertEqual(result["terminal_receipt_sha256"], terminal["self_sha256"])
        self.assertEqual(len(result["materialization_run_ids"]), 20)
        operator = self.fixture.fixture.fixture.operator
        inventory = self._details(operator.scheduler_run_id)["checkpoint"]["primary_due_inventory"]
        claims = [inventory["operator"]["claim"], *inventory["parent_claims"], *inventory["child_claims"]]
        self.assertEqual(result["paid_run_ids"], sorted(claim["scheduler_run_id"] for claim in claims))
        self.assertEqual(result["paid_attempt_ids"], sorted(claim["attempt_id"] for claim in claims))
        self.assertGreater(len(inventory["child_claims"]), 20)
        self.assertEqual(result, self._read())  # Expired permits need no live writer/operator context.
        with connect(self.db) as connection:
            for member in result["members"]:
                self.assertEqual(member["state"], "succeeded")
                self.assertEqual(len(member["eligible_content_ids"]), 1)
                self.assertEqual(member["raw_receipt"]["raw_response_id"], member["raw_response_id"])
                self.assertEqual(member["materialization_identity"]["parent_scheduler_run_id"], member["source_run_id"])
                self.assertEqual(connection.execute(
                    "SELECT account_id FROM content_items WHERE id=?", (member["eligible_content_ids"][0],),
                ).fetchone()[0], member["materialization_identity"]["account_id"])

    def test_one_page_has_more_is_closed_partial_not_incomplete_materialization(self):
        self.fixture.more = True
        self.fixture._run()
        result = self._read()
        self.assertEqual(len(result["materialization_run_ids"]), 20)
        for member in result["members"]:
            state = self._details(member["source_run_id"])["checkpoint"]
            self.assertFalse(state["complete"])
            self.assertEqual((state["page_number"], state["cursor"]), (1, 123))
            self.assertIsNone(state["pending_materialization"])

    def test_balance_failure_prefix_can_close_all_unused_owners(self):
        self.fixture.status = 402
        self.fixture.body_override = {"code": 402, "message": "insufficient balance"}
        self.fixture._run()
        result = self._read()
        self.assertEqual(self.fixture.calls, [1])
        self.assertEqual(result["materialization_run_ids"], [])
        self.assertEqual(result["members"][0]["state"], "failed")
        self.assertIsNotNone(result["members"][0]["raw_response_id"])
        self.assertTrue(all(member["state"] == "not_started" for member in result["members"][1:]))
        for member in result["members"]:
            state = self._details(member["source_run_id"])["checkpoint"]
            self.assertEqual((state["page_number"], state["cursor"]), (0, 0))

    def test_truncation_keeps_original_cursor_without_inventing_local_materialization(self):
        self.fixture.fail_rank = 1
        self.fixture._run()
        result = self._read()
        first = result["members"][0]
        self.assertEqual(first["state"], "billing_unknown")
        self.assertIsNone(first["raw_response_id"])
        self.assertIsNone(first["materialization_run_id"])
        self.assertEqual(len(result["materialization_run_ids"]), 19)
        state = self._details(first["source_run_id"])["checkpoint"]
        self.assertEqual((state["generation"], state["page_number"], state["cursor"]), (0, 0, 0))

    def test_local_materialization_failure_cannot_become_owner_closure(self):
        with patch.object(providers, "materialize_account_discovery_page", side_effect=sqlite3.OperationalError("fixture local failure")):
            self.fixture._run()
        self.fixture._assert_no_running_inventory()
        with self.assertRaisesRegex(DiagnosticOwnerEvidenceError, "pending materialization"):
            self._read()
        self.assertEqual(len(self.fixture.calls), 20)

    def _recover_first_local_failure(self):
        materialize = providers.materialize_account_discovery_page
        first = True

        def fail_once(**kwargs):
            nonlocal first
            if first:
                first = False
                raise sqlite3.OperationalError("fixture one local failure")
            return materialize(**kwargs)

        with patch.object(providers, "materialize_account_discovery_page", side_effect=fail_once):
            terminal = self.fixture._run()["receipt"]
        original = terminal["payload"]["results"][0]
        self.assertFalse(original["materialized"])
        with self.assertRaisesRegex(DiagnosticOwnerEvidenceError, "pending materialization"):
            self._read()
        with connect(self.db) as connection:
            before = {table: [tuple(row) for row in connection.execute(f"SELECT * FROM {table} ORDER BY id")]
                      for table in ("provider_usage", "paid_provider_dispatch_events")}
            source = self._details(original["source_run_id"])
            original_attempt = connection.execute(
                "SELECT details_json FROM scheduler_run_attempts WHERE id=?", (source["owner"]["attempt_id"],),
            ).fetchone()[0]
        with ExitStack() as stack:
            for module in (capture, providers, tikhub_scan, metric_observations):
                stack.enter_context(patch.object(module, "now_utc", return_value=RECOVERY_AT))
            recovery = tikhub_scan.resume_local_materialization(
                original["source_run_id"], db_path=self.db, raw_root=self.fixture.raw_root,
                now=RECOVERY_AT, deadline=time.monotonic() + 50,
            )
        self.assertTrue(recovery["materialization_finalized"])
        self.assertEqual(recovery["pages_this_run"], 0)
        self.assertEqual(self.fixture.calls, list(range(1, 21)))
        with connect(self.db) as connection:
            for table, rows in before.items():
                self.assertEqual(rows, [tuple(row) for row in connection.execute(f"SELECT * FROM {table} ORDER BY id")])
            self.assertEqual(original_attempt, connection.execute(
                "SELECT details_json FROM scheduler_run_attempts WHERE id=?", (source["owner"]["attempt_id"],),
            ).fetchone()[0])
        return terminal, recovery

    def test_local_only_recovery_closes_history_without_rewriting_campaign_outcome(self):
        terminal, recovery = self._recover_first_local_failure()
        result = self._read()
        first = result["members"][0]
        self.assertFalse(first["campaign_materialized"])
        self.assertEqual(len(first["source_attempt_ids"]), 2)
        self.assertEqual(first["source_attempt_id"], first["source_attempt_ids"][0])
        self.assertEqual(first["source_attempt_ids"][-1], recovery["attempt_id"])
        self.assertEqual(len(first["materialization_attempt_ids"]), 2)
        self.assertEqual(first["materialization_started_at"], AT)
        self.assertEqual(first["source_completed_at"], RECOVERY_AT)
        self.assertEqual(len(result["paid_attempt_ids"]), 65)
        self.assertEqual(len(result["materialization_attempt_ids"]), 21)
        self.assertEqual(result["terminal_receipt_sha256"], terminal["self_sha256"])

    def test_recovered_partial_page_rejects_paid_suffix_and_extra_empty_source_attempt(self):
        self.fixture.more = True
        _, recovery = self._recover_first_local_failure()
        result = self._read()
        first = result["members"][0]
        self.assertFalse(recovery["complete"])
        with connect(self.db) as connection:
            connection.execute("BEGIN")
            connection.execute(
                "INSERT INTO provider_usage(provider,operation,request_attempts,billed_requests,currency,amount,recorded_at,details_json) "
                "VALUES ('TikHub','douyin_user_posts',0,0,'USD',0,?,?)",
                (RECOVERY_AT, json.dumps({"state": "not_sent", "scope": {"scheduler_attempt_id": recovery["attempt_id"]}})),
            )
            with self.assertRaisesRegex(DiagnosticOwnerEvidenceError, "paid reservation or dispatch"):
                read_primary_campaign_owners(connection, self.campaign_id, at=LATER)
            connection.rollback()
        identity = self._details(first["source_run_id"])["identity"]
        late = durable_runs.claim_run("tikhub_reconcile", identity, db_path=self.db, now=LATER, invocation_source="operator_retry")
        self.assertIsNotNone(late)
        durable_runs.finish_run(late, status="partial", db_path=self.db, now=LATER, next_resume_at=LATER)
        with self.assertRaisesRegex(DiagnosticOwnerEvidenceError, "unexplained source or materializer attempts"):
            self._read()

    def test_forged_original_owner_and_late_attempt_even_unselected_are_rejected(self):
        self.fixture._run()
        good = self._read()
        # scheduler_runs is mutable; its terminal attempt is not. A false owner
        # must not be accepted just because its current row looks terminal.
        for run_id in (good["members"][0]["source_run_id"], good["materialization_run_ids"][0]):
            with self.subTest(run_id=run_id):
                details = self._details(run_id)
                changed = json.loads(json.dumps(details))
                changed["owner"]["token"] = "forged-original-owner"
                with connect(self.db) as connection, transaction(connection):
                    connection.execute("UPDATE scheduler_runs SET details_json=? WHERE id=?", (json.dumps(changed), run_id))
                with self.assertRaises(DiagnosticOwnerEvidenceError):
                    self._read()
                with connect(self.db) as connection, transaction(connection):
                    original = connection.execute(
                        "SELECT details_json FROM scheduler_run_attempts WHERE id=?", (details["owner"]["attempt_id"],),
                    ).fetchone()[0]
                    connection.execute("UPDATE scheduler_runs SET details_json=? WHERE id=?", (original, run_id))
        selected = {member["source_run_id"] for member in good["members"]}
        operator = self.fixture.fixture.fixture.operator
        inventory = self._details(operator.scheduler_run_id)["checkpoint"]["primary_due_inventory"]
        unused = next(claim for claim in inventory["child_claims"] if claim["scheduler_run_id"] not in selected)
        identity = self._details(unused["scheduler_run_id"])["identity"]
        late = durable_runs.claim_run("tikhub_reconcile", identity, db_path=self.db, now=LATER)
        self.assertIsNotNone(late)
        with self.assertRaisesRegex(DiagnosticOwnerEvidenceError, "running or later attempt"):
            self._read()
        durable_runs.finish_run(late, status="partial", db_path=self.db, now=LATER, next_resume_at=LATER)
        with self.assertRaisesRegex(DiagnosticOwnerEvidenceError, "running or later attempt"):
            self._read()
