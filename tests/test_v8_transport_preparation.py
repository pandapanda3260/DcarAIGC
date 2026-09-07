from __future__ import annotations

import json
import unittest
from dataclasses import replace
from unittest.mock import patch

from apscheduler.schedulers.background import BackgroundScheduler  # type: ignore[import-untyped]

from tests import test_v8_transport_members as fixtures
from v8 import durable_runs, pipeline, providers
from v8.runtime_database import RuntimeDatabaseError, acquire_writer_lock
from v8.storage import connect, transaction
from v8.transport_due_candidates import list_primary_due_candidates
from v8.transport_members import DiagnosticMemberError
from v8.transport_preparation import prepare_primary_due_inventory

AT = fixtures.AT


class TransportPreparationTest(unittest.TestCase):
    def setUp(self):
        self.fixture = fixtures.TransportMembersTest(methodName="runTest")
        self.addCleanup(self.fixture.doCleanups)
        self.fixture.setUp()
        self.db = self.fixture.db
        self.scheduler = BackgroundScheduler(timezone="UTC")
        self.scheduler.start(paused=True)
        self.addCleanup(self.scheduler.shutdown, wait=False)
        self.enterContext(patch.object(pipeline, "_dispatch", side_effect=AssertionError("no dispatch")))
        self.enterContext(patch.object(providers, "_load_key", side_effect=AssertionError("no provider")))

    def _cache(self, count=20):
        with connect(self.db) as connection, transaction(connection):
            connection.executemany(
                "INSERT OR IGNORE INTO account_provider_references(account_identity_id,provider,"
                "reference_kind,reference_value,created_at,updated_at) VALUES (?,'TikHub','sec_user_id',?,?,?)",
                [(identity_id, "MS4wLjAB" + "r" * 40 + str(identity_id), AT, AT)
                 for identity_id in range(1, count + 1)],
            )

    def _prepare(self, connection, **changes):
        return prepare_primary_due_inventory(connection, **{
            "campaign_receipt_id": self.fixture.campaign["receipt_id"],
            "operator_claim": self.fixture.operator, "scheduler": self.scheduler, "at": AT, **changes,
        })

    def _counts(self, connection):
        return [connection.execute(f"SELECT COUNT(*) FROM {table}").fetchone()[0] for table in (
            "scheduler_runs", "scheduler_run_attempts", "provider_usage", "fetch_attempts",
            "paid_provider_dispatch_events", "pipeline_paid_drain_events", "provider_raw_responses",
        )]

    def test_complete_latest_due_roster_is_atomic_idempotent_and_has_no_purchase(self):
        self._cache()
        with acquire_writer_lock(self.fixture.writer_access), connect(self.db) as connection, transaction(connection):
            before = self._counts(connection)
            result = self._prepare(connection)
            self.assertEqual(result["eligible_identity_ids"], list(range(1, 21)))
            self.assertEqual(len(result["parent_claims"]), 3)
            self.assertEqual(len(result["child_claims"]), 60)
            candidates = list_primary_due_candidates(connection, at=AT)
            self.assertEqual(result["candidate_run_ids"], [item["scope"].scheduler_run_id for item in candidates])
            self.assertEqual(len(candidates), 60)
            self.assertEqual(result["skipped"], [])
            for value in result["parent_claims"]:
                details = durable_runs.assert_owner(connection, durable_runs.DurableClaim(**value))
                self.assertEqual(details["identity"]["eligible_identity_ids"], list(range(1, 21)))
                self.assertEqual(len(details["checkpoint"]["child_run_ids"]), 20)
            parents = [json.loads(row[0])["identity"] for row in connection.execute(
                "SELECT details_json FROM scheduler_runs WHERE job_id LIKE 'pipeline_round:%' ORDER BY id")]
            self.assertEqual([row["round_id"] for row in parents], [
                "tikhub_works_scan:02:10", "tikhub_reconcile:03:00", "tikhub_works_refresh:12:00",
            ])
            self.assertEqual(self._counts(connection)[2:], before[2:])
            changes = connection.total_changes
            self.assertEqual(self._prepare(connection), result)
            self.assertEqual(connection.total_changes, changes)
            members = self.fixture._issue(connection)
            self.assertEqual(len(members), 20)
            self.assertEqual([row["payload"]["natural_due"] for row in members], [item["proof"] for item in candidates[:20]])

    def test_transaction_writer_scheduler_and_operator_are_real_required_fences(self):
        with connect(self.db) as connection:
            with self.assertRaises(DiagnosticMemberError):
                self._prepare(connection)
            with transaction(connection):
                before = self._counts(connection)
                with self.assertRaises(RuntimeDatabaseError):
                    self._prepare(connection)
                self.assertEqual(self._counts(connection), before)
        with acquire_writer_lock(self.fixture.writer_access), connect(self.db) as connection, transaction(connection):
            before = self._counts(connection)
            with self.assertRaises(DiagnosticMemberError):
                self._prepare(connection, scheduler=True)
            self.scheduler.resume()
            with self.assertRaises(DiagnosticMemberError):
                self._prepare(connection)
            self.scheduler.pause()
            with self.assertRaises(durable_runs.LostOwnership):
                self._prepare(connection, operator_claim=replace(self.fixture.operator, owner_token="not-owner"))
            self.assertEqual(self._counts(connection), before)

    def test_terminal_pending_local_future_and_missing_reference_are_not_claimed(self):
        self.fixture._natural_children(range(1, 21))
        children = list(self.fixture.children.values())
        with connect(self.db) as connection, transaction(connection):
            parent_row = connection.execute("SELECT * FROM scheduler_runs WHERE job_id='pipeline_round:tikhub_works_scan'").fetchone()
            details = json.loads(parent_row["details_json"])
            owner = details["owner"]
            parent = durable_runs.DurableClaim(parent_row["id"], owner["attempt_id"], owner["attempt_number"], owner["token"], details["scan_id"])
            durable_runs.checkpoint(connection, parent, {"child_run_ids": [claim.scheduler_run_id for claim in children]}, now=AT)
            durable_runs.checkpoint(connection, children[0], {"complete": True}, now=AT)
            durable_runs.checkpoint(connection, children[1], {"pending_raw": {"fixture": "local-only"}}, now=AT)
            connection.execute("DELETE FROM account_provider_references WHERE account_identity_id=20")
        durable_runs.finish_run(parent, status="partial", db_path=self.db, now=AT, next_resume_at=AT)
        for index, child in enumerate(children):
            durable_runs.finish_run(child, status="succeeded" if index == 0 else "partial", db_path=self.db, now=AT,
                                    next_resume_at="2099-01-01T00:00:00Z" if index == 2 else AT)
        with acquire_writer_lock(self.fixture.writer_access), connect(self.db) as connection, transaction(connection):
            result = self._prepare(connection)
            omitted = {claim.scheduler_run_id for claim in children[:3]} | {children[-1].scheduler_run_id}
            self.assertFalse(omitted & {row["scheduler_run_id"] for row in result["child_claims"]})
            self.assertEqual(len(result["child_claims"]), 54)
            self.assertEqual({item["reason"] for item in result["skipped"]}, {
                "terminal", "pending_local", "resume_not_due", "missing_cached_reference",
            })
            for child in children[:3]:
                attempts = connection.execute("SELECT COUNT(*) FROM scheduler_run_attempts WHERE scheduler_run_id=?", (child.scheduler_run_id,)).fetchone()[0]
                self.assertEqual(attempts, 1)
            prepared_parent = result["parent_claims"][0]
            self.assertEqual(prepared_parent["scheduler_run_id"], parent.scheduler_run_id)
            self.assertEqual(prepared_parent["attempt_number"], 2)

    def test_foreign_live_owner_rolls_back_earlier_claims_and_never_overwrites_owner(self):
        self.fixture._natural_children(range(1, 21), registration="tikhub_works_refresh")
        with acquire_writer_lock(self.fixture.writer_access), connect(self.db) as connection, transaction(connection):
            before = self._counts(connection)
            original = [tuple(row) for row in connection.execute("SELECT id,details_json FROM scheduler_runs ORDER BY id")]
            with self.assertRaises(DiagnosticMemberError) as caught:
                self._prepare(connection)
            self.assertEqual(caught.exception.code, "diagnostic_foreign_owner")
            self.assertEqual(self._counts(connection), before)
            self.assertEqual([tuple(row) for row in connection.execute("SELECT id,details_json FROM scheduler_runs ORDER BY id")], original)
            self.assertNotIn("primary_due_inventory", durable_runs.assert_owner(connection, self.fixture.operator)["checkpoint"])

    def test_existing_parent_preserves_frozen_roster_across_enabled_drift(self):
        with connect(self.db) as connection, transaction(connection):
            connection.execute("UPDATE accounts SET enabled=0 WHERE id=20")
        self.fixture._natural_children(range(1, 20))
        self._cache()
        children = list(self.fixture.children.values())
        with connect(self.db) as connection, transaction(connection):
            row = connection.execute("SELECT * FROM scheduler_runs WHERE job_id='pipeline_round:tikhub_works_scan'").fetchone()
            details = json.loads(row["details_json"])
            frozen_identity = details["identity"]
            owner = details["owner"]
            parent = durable_runs.DurableClaim(row["id"], owner["attempt_id"], owner["attempt_number"], owner["token"], details["scan_id"])
            durable_runs.checkpoint(connection, parent, {"child_run_ids": [claim.scheduler_run_id for claim in children]}, now=AT)
            connection.execute("UPDATE accounts SET enabled=1 WHERE id=20")
            connection.execute("UPDATE accounts SET enabled=0 WHERE id=1")
        durable_runs.finish_run(parent, status="partial", db_path=self.db, now=AT, next_resume_at=AT)
        for child in children:
            durable_runs.finish_run(child, status="partial", db_path=self.db, now=AT, next_resume_at=AT)
        with acquire_writer_lock(self.fixture.writer_access), connect(self.db) as connection, transaction(connection):
            result = self._prepare(connection)
            prepared_parent = durable_runs.DurableClaim(**result["parent_claims"][0])
            retained = durable_runs.assert_owner(connection, prepared_parent)
            self.assertEqual(retained["identity"], frozen_identity)
            self.assertEqual(retained["checkpoint"]["child_run_ids"], [child.scheduler_run_id for child in children])
            prepared_runs = {claim["scheduler_run_id"] for claim in result["child_claims"]}
            self.assertNotIn(children[0].scheduler_run_id, prepared_runs)
            prepared_old_ids = []
            for value in result["child_claims"]:
                identity = durable_runs.assert_owner(connection, durable_runs.DurableClaim(**value))["identity"]
                if identity["window_start"] == "2026-08-06T16:00:00Z":
                    prepared_old_ids.append(identity["identity_id"])
            self.assertEqual(prepared_old_ids, list(range(2, 20)))
            self.assertEqual(result["eligible_identity_ids"], list(range(2, 21)))


if __name__ == "__main__":
    unittest.main()
