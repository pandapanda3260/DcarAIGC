"""Real schema20 worker/checkpoint/lease tests; provider I/O is forbidden.

Route readiness, the one-request result and terminal raw verification are fixtures. Claim, checkpoint,
retry scheduling, charge-day continuation and both heartbeat ledgers are real.
"""
from __future__ import annotations

import json
import socket
import tempfile
import threading
import time
import unittest
from contextlib import nullcontext
from datetime import datetime, timedelta
from pathlib import Path
from unittest.mock import patch

from v8 import account_states, capture_runtime as runtime, durable_runs, provider_budget, schema_v20, storage, work_readiness
from v8.runtime_database import (DatabaseAccessMode, FileIdentity, InstalledWriterContract,
    ResolvedDatabaseAccess, acquire_writer_lock)

AT = "2026-09-07T03:00:00Z"
REAL_READINESS = runtime._readiness


def after(at: str, seconds: int) -> str:
    return (datetime.fromisoformat(at.replace("Z", "+00:00")) + timedelta(seconds=seconds)).isoformat().replace("+00:00", "Z")


class CaptureWorkerBackoffTest(unittest.TestCase):
    def setUp(self):
        self.temp = tempfile.TemporaryDirectory()
        self.addCleanup(self.temp.cleanup)
        self.root = Path(self.temp.name).resolve()
        self.db = self.root / "worker.sqlite3"
        with storage.connect(self.db) as connection:
            storage.initialize_database(connection)
            schema_v20.migrate(connection)
        lock = self.root / "writer.lock"
        lock.touch(mode=0o600)
        installed = InstalledWriterContract(self.root, self.root / "fixture.plist", self.root,
            self.root / "fixture.py", self.db, lock, {})
        access = ResolvedDatabaseAccess(DatabaseAccessMode.WRITER, self.db,
            FileIdentity.from_stat(self.db.stat()), self.root, lock, installed)
        self.enterContext(acquire_writer_lock(access))
        self.network = self.enterContext(patch.object(socket.socket, "connect", side_effect=AssertionError("network forbidden")))
        self.http = self.enterContext(patch.object(runtime.providers, "_request_json", side_effect=AssertionError("HTTP forbidden")))
        self.clock = AT
        self.enterContext(patch.object(runtime, "now_utc", lambda: self.clock))
        self.enterContext(patch.object(runtime, "activation_at", return_value={"profile_id": "integrated_route_v1"}))
        self.enterContext(patch.object(runtime, "_readiness", return_value=("runnable", "")))
        self.enterContext(patch.object(runtime, "_verify_raws", return_value=None))
        self.enterContext(patch.object(runtime.planning, "execution_route_context", lambda *_: nullcontext()))
        self.envelope = {"assignment_id": 1, "account_id": 1, "identity_id": 1, "capture_stage": "detail", "logical_due": "lifetime",
            "operation": "douyin_video_detail",
            "category": "detail", "activation_id": 3, "roster_snapshot_id": 2,
            "roster_members_sha256": "a" * 64, "stage": "detail", "data_business_day": "2026-09-07",
            "window_start": "2026-09-06T16:00:00Z", "window_end": "2026-09-07T16:00:00Z"}
        with storage.connect(self.db) as connection, storage.transaction(connection):
            connection.execute("""INSERT INTO accounts(id,phone,phone_normalized,operator_name,account_type,
                content_direction,enabled,created_at,updated_at) VALUES(1,'',NULL,'','unknown','unknown',1,?,?)""", (AT, AT))
            connection.execute("""INSERT INTO account_platform_identities(id,account_id,platform,uid,nickname,source,created_at,updated_at)
                VALUES(1,1,'douyin','10000001','fixture','manual',?,?)""", (AT, AT))
            assignment_id = runtime.planning.assign_route(connection, scope_type="account",
                scope_key="1", account_id=1, provider="tikhub", operation="douyin_video_detail", expected_generation=0,
                route="integrated", mode="active", effective_at=AT, recorded_at=AT)
            self.envelope["assignment_id"] = assignment_id
            self.work_id = int(connection.execute("""INSERT INTO capture_work_items(
                work_identity,assignment_id,account_id,provider,operation,due_at,data_business_day,state,envelope_json,created_at,updated_at)
                VALUES(?,?,1,'tikhub','douyin_video_detail',?,'2026-09-07','runnable',?,?,?)""",
                ("b" * 64, assignment_id, runtime.planning.timestamp(AT), json.dumps(self.envelope), AT, AT)).lastrowid)

    def work(self):
        with storage.connect(self.db) as connection:
            return dict(connection.execute("SELECT * FROM capture_work_items WHERE id=?", (self.work_id,)).fetchone())

    def checkpoint(self):
        with storage.connect(self.db) as connection:
            row = connection.execute("SELECT details_json FROM scheduler_runs ORDER BY id DESC LIMIT 1").fetchone()
            return json.loads(row[0])["checkpoint"]

    def run_result(self, *, continuation=False, complete=False, elapsed=0):
        # Model the planner's readiness promotion, leaving the worker's real due
        # check and the durable scheduler's next_resume_at untouched.
        with storage.connect(self.db) as connection, storage.transaction(connection):
            connection.execute("UPDATE capture_work_items SET state='runnable' WHERE id=? AND state IN ('provider_blocked','budget_deferred')", (self.work_id,))
        calls = []

        def execute(envelope, **_kwargs):
            calls.append(True)
            self.clock = after(self.clock, elapsed)
            return {"complete": complete, "continuation": continuation, "envelope": dict(envelope),
                "evidence": {"raw_response_ids": []}, "reason": "" if complete or continuation else "provider_circuit_open",
                "provider_cost": 0.0}

        with patch.object(runtime, "_execute_one", side_effect=execute):
            result = runtime._run_single(self.db, self.clock)
        return result, calls

    def assert_no_provider(self):
        self.network.assert_not_called()
        self.http.assert_not_called()
        with storage.connect(self.db) as connection:
            for table in ("provider_request_start_events", "provider_usage", "fetch_slots"):
                self.assertEqual(connection.execute(f"SELECT count(*) FROM {table}").fetchone()[0], 0)
            self.assertEqual(connection.execute("PRAGMA user_version").fetchone()[0], 20)
            self.assertEqual(connection.execute("PRAGMA foreign_key_check").fetchall(), [])

    def test_successful_pages_do_not_count_as_failures_and_success_resets(self):
        for _ in range(5):
            result, calls = self.run_result(continuation=True)
            self.assertEqual((result["status"], len(calls)), ("runnable", 1))
            self.assertEqual(self.checkpoint()["capture_consecutive_failures"], 0)
            self.clock = self.work()["due_at"]
        result, _ = self.run_result(elapsed=35)
        self.assertEqual(result["status"], "provider_blocked")
        self.assertEqual(self.work()["attempt_count"], 6)
        self.assertEqual(self.work()["due_at"], runtime.planning.timestamp(after(self.clock, 300)))
        self.assertEqual(self.checkpoint()["capture_consecutive_failures"], 1)
        self.clock = self.work()["due_at"]
        self.run_result()
        self.assertEqual(self.checkpoint()["capture_consecutive_failures"], 2)
        self.assertEqual(self.work()["due_at"], runtime.planning.timestamp(after(self.clock, 600)))
        self.clock = self.work()["due_at"]
        self.run_result(continuation=True)
        self.assertEqual(self.checkpoint()["capture_consecutive_failures"], 0)
        self.clock = self.work()["due_at"]
        self.run_result()
        self.assertEqual(self.checkpoint()["capture_consecutive_failures"], 1)
        self.assertEqual(self.work()["due_at"], runtime.planning.timestamp(after(self.clock, 300)))
        self.clock = self.work()["due_at"]
        self.run_result(complete=True)
        self.assertEqual(self.checkpoint()["capture_consecutive_failures"], 0)
        with storage.connect(self.db) as connection:
            self.assertEqual(connection.execute("SELECT count(*) FROM operational_alerts").fetchone()[0], 0)
        self.assert_no_provider()

    def test_consecutive_failures_are_backed_off_alerted_and_not_run_early(self):
        for failure in range(1, 9):
            self.run_result()
            due = self.work()["due_at"]
            expected = min(300 * (2 ** (failure - 1)), 21600)
            self.assertEqual(due, runtime.planning.timestamp(after(self.clock, expected)))
            self.assertEqual(self.checkpoint()["capture_consecutive_failures"], failure)
            early, calls = self.run_result()
            self.assertEqual((early["status"], calls), ("idle", []))
            self.clock = due
        with storage.connect(self.db) as connection:
            alerts = connection.execute("SELECT * FROM operational_alerts").fetchall()
            self.assertEqual(len(alerts), 1)
            self.assertEqual(json.loads(alerts[0]["evidence_json"])["consecutive_failures"], 6)
        self.assert_no_provider()

    def test_charge_day_child_keeps_the_existing_failure_streak(self):
        self.run_result()
        self.clock = after(AT, 86400)
        self.run_result()
        self.assertEqual(self.checkpoint()["capture_consecutive_failures"], 2)
        self.assertEqual(self.work()["due_at"], runtime.planning.timestamp(after(self.clock, 600)))
        with storage.connect(self.db) as connection:
            self.assertEqual(connection.execute("SELECT count(*) FROM scheduler_runs WHERE root_run_id IS NOT NULL").fetchone()[0], 1)
        self.assert_no_provider()

    def test_heartbeat_waits_for_standalone_writer_and_renews_both_fences(self):
        with storage.connect(self.db) as connection, storage.transaction(connection):
            claim = runtime._claim_work(connection, self.work(), at=AT)
            connection.execute("UPDATE capture_work_items SET state='running',owner_token=?,heartbeat_at=?,lease_expires_at=? WHERE id=?",
                (claim.owner_token, runtime.planning.timestamp(AT), runtime.planning.timestamp(after(AT, 180)), self.work_id))
        inside, release = threading.Event(), threading.Event()
        errors = []

        def writer():
            try:
                with storage.connect(self.db) as connection, account_states._atomic(connection):
                    inside.set()
                    if not release.wait(2):
                        raise AssertionError("writer release timed out")
            except BaseException as error:
                errors.append(error)

        with patch.object(durable_runs, "HEARTBEAT_SECONDS", .01), runtime._maintain_work_lease(self.db, self.work_id, claim) as check:
            thread = threading.Thread(target=writer)
            thread.start()
            try:
                self.assertTrue(inside.wait(2))
                self.clock = after(AT, 120)
                time.sleep(.04)
            finally:
                release.set()
                thread.join(2)
            self.assertFalse(thread.is_alive())
            self.assertEqual(errors, [])
            deadline = time.monotonic() + 2
            while time.monotonic() < deadline:
                check()
                with storage.connect(self.db) as connection:
                    attempt = connection.execute("SELECT heartbeat_at,lease_expires_at FROM scheduler_run_attempts WHERE id=?", (claim.attempt_id,)).fetchone()
                if attempt["heartbeat_at"] == self.work()["heartbeat_at"] == runtime.planning.timestamp(self.clock):
                    break
                time.sleep(.005)
            else:
                self.fail("both leases did not renew after standalone writer released")
            self.assertEqual(attempt["lease_expires_at"], self.work()["lease_expires_at"])
            self.assertEqual(attempt["lease_expires_at"], runtime.planning.timestamp(after(self.clock, 180)))
        self.assert_no_provider()

    def open_planning_gate(self):
        # A planning marker only; no paid authorization is manufactured or used.
        with storage.connect(self.db) as connection, storage.transaction(connection):
            connection.execute("""INSERT INTO capture_paid_send_gate_events(provider,operation,state,reason,
                evidence_json,recorded_at,event_sha256) VALUES('tikhub','douyin_video_detail','open',
                'isolated-planning-fixture','{}',?,?)""", (AT, "f" * 64))

    def assert_no_attempt(self):
        self.assertEqual(self.work()["attempt_count"], 0)
        with storage.connect(self.db) as connection:
            self.assertEqual(connection.execute("SELECT count(*) FROM scheduler_run_attempts").fetchone()[0], 0)
        self.assert_no_provider()

    def test_preclaim_closed_gate_waits_but_never_claims_or_reopens_gate(self):
        with patch.object(runtime, "_readiness", side_effect=REAL_READINESS) as ready, \
                patch.object(runtime, "_execute_one", side_effect=AssertionError("must not execute")), \
                patch.object(runtime, "_cohort_plan", return_value={"id": 1, "cohort": []}):
            result = runtime._run_single(self.db, AT)
            self.assertEqual(result["reason"], "provider_transport_blocked")
            self.assertEqual(self.work()["due_at"], runtime.planning.timestamp(after(AT, 300)))
            ready.reset_mock()
            self.assertEqual(runtime.plan_tick(self.db, AT)["reconsidered"], 0)
            ready.assert_not_called()
            self.clock = after(AT, 300)
            runtime.plan_tick(self.db, self.clock)
            self.assertEqual(self.work()["state"], "provider_blocked")
            self.assertEqual(self.work()["due_at"], runtime.planning.timestamp(after(AT, 600)))
        with storage.connect(self.db) as connection:
            self.assertEqual(connection.execute("SELECT count(*) FROM capture_paid_send_gate_events").fetchone()[0], 0)
        self.assert_no_attempt()

    def test_preclaim_budget_failure_is_only_a_delayed_readiness_check(self):
        self.open_planning_gate()
        with patch.object(runtime, "_readiness", side_effect=REAL_READINESS), \
                patch.object(work_readiness, "check_reservation", side_effect=provider_budget.BudgetBlocked("isolated exhausted budget")), \
                patch.object(runtime, "_execute_one", side_effect=AssertionError("must not execute")):
            result = runtime._run_single(self.db, AT)
        self.assertEqual(result["status"], "budget_deferred")
        self.assertEqual(self.work()["due_at"], runtime.planning.timestamp(after(AT, 300)))
        self.assert_no_attempt()

    def test_preclaim_authorization_hold_never_becomes_an_execution_retry(self):
        self.open_planning_gate()
        with storage.connect(self.db) as connection, storage.transaction(connection):
            provider_budget.record_fault_state(connection, scope_kind="authorization_hard", authorization_id=1,
                fault_class="account_authorization", reason="authorization_scope_missing", usage_id=None, at=AT)
        with patch.object(runtime, "_readiness", side_effect=REAL_READINESS), \
                patch.object(runtime, "_execute_one", side_effect=AssertionError("must not execute")), \
                patch.object(runtime, "_cohort_plan", return_value={"id": 1, "cohort": []}):
            result = runtime._run_single(self.db, AT)
            self.assertEqual(result["reason"], "authorization_hard")
            self.clock = after(AT, 300)
            runtime.plan_tick(self.db, self.clock)
            self.assertEqual(self.work()["state"], "provider_blocked")
            self.assertEqual(runtime._run_single(self.db, self.clock)["status"], "idle")
        self.assert_no_attempt()

    def test_planner_recheck_limit_does_not_starve_older_unchecked_work(self):
        with storage.connect(self.db) as connection, storage.transaction(connection):
            connection.execute("UPDATE capture_work_items SET state='provider_blocked',reason='provider_transport_blocked'")
            for index in range(500):
                envelope = {**self.envelope, "fixture_index": index}
                connection.execute("""INSERT INTO capture_work_items(work_identity,assignment_id,account_id,provider,operation,
                    due_at,data_business_day,state,reason,envelope_json,created_at,updated_at)
                    VALUES(?,?,1,'tikhub','douyin_video_detail',?,'2026-09-07','provider_blocked',
                    'provider_transport_blocked',?,?,?)""",
                    (runtime.planning.digest({"fixture": index}), self.envelope["assignment_id"],
                     runtime.planning.timestamp(AT), json.dumps(envelope), AT, AT))
        seen = []
        def assess(connection, envelope, **kwargs):
            seen.append(envelope.get("fixture_index", -1))
            return REAL_READINESS(connection, envelope, **kwargs)
        with patch.object(runtime, "_cohort_plan", return_value={"id": 1, "cohort": []}), \
                patch.object(runtime, "_readiness", side_effect=assess):
            first = runtime.plan_tick(self.db, AT)
            self.assertEqual(first["reconsidered"], 500)
            self.assertNotIn(499, seen)
            seen.clear()
            runtime.plan_tick(self.db, after(AT, 300))
            self.assertEqual(seen[0], 499)
        self.assert_no_attempt()
