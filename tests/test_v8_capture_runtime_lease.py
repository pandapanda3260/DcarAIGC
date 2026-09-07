from __future__ import annotations

import json
import sqlite3
import tempfile
import time
import unittest
from contextlib import ExitStack, contextmanager, nullcontext
from datetime import datetime, timedelta
from pathlib import Path
from unittest.mock import patch

from v8 import capture_batches as batches, capture_runtime as runtime, durable_runs
from v8.storage import transaction

NOW = "2026-09-07T03:00:00Z"


def after(seconds):
    return (datetime.fromisoformat(NOW.replace("Z", "+00:00")) + timedelta(seconds=seconds)).isoformat().replace("+00:00", "Z")


class CaptureRuntimeLeaseTest(unittest.TestCase):
    def setUp(self):
        self.temp = tempfile.TemporaryDirectory()
        self.addCleanup(self.temp.cleanup)
        self.db = Path(self.temp.name) / "capture.sqlite3"
        with self.connection() as connection:
            connection.executescript("""
                PRAGMA journal_mode=WAL;
                PRAGMA user_version=20;
                CREATE TABLE scheduler_runs(id INTEGER PRIMARY KEY,job_id TEXT,scheduled_for TEXT,status TEXT,
                    started_at TEXT,completed_at TEXT,details_json TEXT,root_run_id INTEGER,
                    continuation_sequence INTEGER,charge_business_day TEXT);
                CREATE TABLE scheduler_run_attempts(id INTEGER PRIMARY KEY,scheduler_run_id INTEGER,
                    attempt_number INTEGER,invocation_source TEXT,status TEXT,started_at TEXT,completed_at TEXT,
                    details_json TEXT,owner_token TEXT,heartbeat_at TEXT,lease_expires_at TEXT);
                CREATE TABLE capture_work_items(id INTEGER PRIMARY KEY,work_identity TEXT,assignment_id INTEGER,
                    source_plan_id INTEGER,account_id INTEGER,content_id INTEGER,provider TEXT,operation TEXT,
                    due_at TEXT,data_business_day TEXT,state TEXT,reason TEXT,envelope_json TEXT,owner_token TEXT,
                    heartbeat_at TEXT,lease_expires_at TEXT,attempt_count INTEGER,created_at TEXT,updated_at TEXT,
                    completed_at TEXT);
                CREATE TABLE data_quality_receipts(scope_key TEXT,cutoff_at TEXT,payload_json TEXT,
                    recorded_at TEXT,receipt_sha256 TEXT);
            """)
        self.clock = NOW
        self.envelope = {"assignment_id": 1, "category": "detail", "activation_id": 3,
                         "roster_snapshot_id": 2, "roster_members_sha256": "a" * 64,
                         "stage": "detail", "data_business_day": "2026-09-07",
                         "window_start": "2026-09-06T16:00:00Z", "window_end": "2026-09-07T16:00:00Z"}
        with self.connection() as connection:
            connection.execute("""INSERT INTO capture_work_items(id,work_identity,operation,due_at,data_business_day,
                state,reason,envelope_json,attempt_count,created_at,updated_at)
                VALUES(1,'frozen-work','douyin_video_detail',?,'2026-09-07','runnable','',?,0,?,?)""",
                (runtime.planning.timestamp(NOW), json.dumps(self.envelope), NOW, NOW))
        self.stack = ExitStack()
        self.addCleanup(self.stack.close)
        self.stack.enter_context(patch.object(runtime, "connect", self.connection))
        self.stack.enter_context(patch.object(runtime, "now_utc", lambda: self.clock))

    @contextmanager
    def connection(self, _path=None):
        connection = sqlite3.connect(self.db, timeout=2)
        connection.row_factory = sqlite3.Row
        try:
            with connection:
                yield connection
        finally:
            connection.close()

    def claim(self):
        with self.connection() as connection, transaction(connection):
            work = dict(connection.execute("SELECT * FROM capture_work_items WHERE id=1").fetchone())
            claim = runtime._claim_work(connection, work, at=NOW)
            connection.execute("UPDATE capture_work_items SET state='running',owner_token=?,heartbeat_at=?,lease_expires_at=? WHERE id=1",
                (claim.owner_token, runtime.planning.timestamp(NOW), runtime.planning.timestamp(after(180))))
        return claim

    def rows(self):
        with self.connection() as connection:
            return (dict(connection.execute("SELECT * FROM capture_work_items WHERE id=1").fetchone()),
                    dict(connection.execute("SELECT * FROM scheduler_run_attempts ORDER BY id DESC LIMIT 1").fetchone()))

    def renew(self, claim, at):
        with self.connection() as connection, transaction(connection):
            runtime._renew_work_lease(connection, 1, claim, at=at)

    def wait_heartbeat(self, expected):
        deadline = time.monotonic() + 2
        while time.monotonic() < deadline:
            work, attempt = self.rows()
            if work["heartbeat_at"] == attempt["heartbeat_at"] == runtime.planning.timestamp(expected):
                return
            time.sleep(.005)
        self.fail("worker did not renew both leases")

    def test_renewal_updates_both_owners_atomically(self):
        claim = self.claim()
        self.renew(claim, after(30))
        work, attempt = self.rows()
        self.assertEqual(work["lease_expires_at"], runtime.planning.timestamp(after(210)))
        self.assertEqual(work["lease_expires_at"], attempt["lease_expires_at"])

    def test_expired_scheduler_lease_is_never_revived(self):
        claim = self.claim()
        before = self.rows()
        with self.assertRaises(durable_runs.LostOwnership):
            self.renew(claim, after(181))
        self.assertEqual(self.rows(), before)

    def test_expired_work_lease_rolls_back_scheduler_renewal(self):
        claim = self.claim()
        with self.connection() as connection:
            connection.execute("UPDATE capture_work_items SET lease_expires_at=?", (runtime.planning.timestamp(after(10)),))
        before = self.rows()
        with self.assertRaises(durable_runs.LostOwnership):
            self.renew(claim, after(30))
        self.assertEqual(self.rows(), before)

    def test_replaced_work_owner_rolls_back_scheduler_renewal(self):
        claim = self.claim()
        with self.connection() as connection:
            connection.execute("UPDATE capture_work_items SET owner_token='replacement'")
        before = self.rows()
        with self.assertRaises(durable_runs.LostOwnership):
            self.renew(claim, after(30))
        self.assertEqual(self.rows(), before)

    def test_replaced_scheduler_owner_cannot_renew_work(self):
        claim = self.claim()
        with self.connection() as connection:
            connection.execute("UPDATE scheduler_run_attempts SET owner_token='replacement'")
        before = self.rows()
        with self.assertRaises(durable_runs.LostOwnership):
            self.renew(claim, after(30))
        self.assertEqual(self.rows(), before)

    def test_finished_attempt_cannot_be_revived(self):
        claim = self.claim()
        with self.connection() as connection:
            connection.execute("UPDATE scheduler_runs SET status='succeeded'")
            connection.execute("UPDATE scheduler_run_attempts SET status='succeeded'")
        with self.assertRaises(durable_runs.LostOwnership):
            self.renew(claim, after(30))

    def test_background_heartbeat_covers_work_longer_than_one_lease(self):
        claim = self.claim()
        with patch.object(durable_runs, "HEARTBEAT_SECONDS", .01), runtime._maintain_work_lease(self.db, 1, claim) as check:
            for seconds in (120, 240, 360):
                self.clock = after(seconds)
                self.wait_heartbeat(self.clock)
                check()
        work, attempt = self.rows()
        self.assertEqual(work["lease_expires_at"], runtime.planning.timestamp(after(540)))
        self.assertEqual(attempt["lease_expires_at"], work["lease_expires_at"])
        self.assertEqual(work["data_business_day"], "2026-09-07")
        self.assertEqual(json.loads(work["envelope_json"]), self.envelope)

    def test_background_owner_loss_is_reported_to_executor(self):
        claim = self.claim()
        with patch.object(durable_runs, "HEARTBEAT_SECONDS", .01), runtime._maintain_work_lease(self.db, 1, claim) as check:
            with self.connection() as connection:
                connection.execute("UPDATE capture_work_items SET owner_token='replacement'")
            deadline = time.monotonic() + 2
            while time.monotonic() < deadline:
                try:
                    check()
                except durable_runs.LostOwnership:
                    break
                time.sleep(.01)
            else:
                self.fail("lost work owner was not fenced")

    def execute_context(self, execute, readiness=None):
        stack = ExitStack()
        stack.enter_context(patch.object(runtime, "activation_at", return_value={"profile_id": "integrated_route_v1"}))
        stack.enter_context(patch.object(runtime, "_readiness", side_effect=readiness or (lambda *_a, **_k: ("runnable", ""))))
        stack.enter_context(patch.object(runtime.planning, "execution_route_context", lambda *_: nullcontext()))
        stack.enter_context(patch.object(runtime, "_execute_one", side_effect=execute))
        stack.enter_context(patch.object(runtime, "_verify_raws", return_value=None))
        return stack

    def complete(self, envelope):
        return {"complete": True, "continuation": False, "envelope": envelope,
                "evidence": {"raw_response_ids": []}, "reason": "", "provider_cost": 0}

    def test_claim_and_completion_use_actual_clock_after_slow_readiness(self):
        def readiness(*_args, **_kwargs):
            self.clock = after(240)
            return "runnable", ""

        def execute(envelope, **kwargs):
            self.assertEqual(kwargs["at"], after(240))
            self.assertEqual(envelope, self.envelope)
            self.clock = after(265)
            return self.complete(envelope)

        with self.execute_context(execute, readiness):
            result = runtime._run_single(self.db, NOW)
        work, attempt = self.rows()
        self.assertEqual(result["status"], "terminal")
        self.assertEqual(attempt["started_at"], after(240))
        self.assertEqual(attempt["completed_at"], after(265))
        self.assertEqual(work["completed_at"], runtime.planning.timestamp(after(265)))
        with self.connection() as connection:
            row = connection.execute("SELECT recorded_at FROM data_quality_receipts").fetchone()
        self.assertEqual(row[0], runtime.planning.timestamp(after(265)))

    def test_lost_owner_during_execution_cannot_publish_result(self):
        def execute(envelope, **_kwargs):
            with self.connection() as connection:
                connection.execute("UPDATE capture_work_items SET owner_token='replacement'")
            return self.complete(envelope)

        with self.execute_context(execute), self.assertRaises(durable_runs.LostOwnership):
            runtime._run_single(self.db, NOW)
        with self.connection() as connection:
            self.assertEqual(connection.execute("SELECT count(*) FROM data_quality_receipts").fetchone()[0], 0)
        self.assertEqual(self.rows()[0]["owner_token"], "replacement")

    def test_expiry_before_execution_does_not_enter_provider(self):
        calls = []
        with self.execute_context(lambda *_a, **_k: calls.append(True)), patch.object(runtime, "now_utc", side_effect=[NOW, after(181)]):
            with self.assertRaises(durable_runs.LostOwnership):
                runtime._run_single(self.db, NOW)
        self.assertEqual(calls, [])

    def second_member(self, claim=None):
        with self.connection() as connection:
            connection.execute("UPDATE capture_work_items SET operation=?,content_id=1", (batches.OPERATION,))
            work = dict(connection.execute("SELECT * FROM capture_work_items WHERE id=1").fetchone())
            work.update(id=2, work_identity="second-frozen-work", content_id=2)
            connection.execute(f"INSERT INTO capture_work_items({','.join(work)}) VALUES({','.join('?' for _ in work)})", tuple(work.values()))
        return claim

    def test_batch_heartbeat_renews_every_member(self):
        claim = self.second_member(self.claim())
        with patch.object(durable_runs, "HEARTBEAT_SECONDS", .01), runtime._maintain_work_lease(self.db, (1, 2), claim) as check:
            self.clock = after(120)
            self.wait_heartbeat(self.clock)
            check()
            with self.connection() as connection:
                leases = [row[0] for row in connection.execute("SELECT lease_expires_at FROM capture_work_items")]
            self.assertEqual(leases, [runtime.planning.timestamp(after(300))] * 2)

    def test_one_batch_member_loss_rolls_back_every_renewal(self):
        claim = self.second_member(self.claim())
        with self.connection() as connection:
            connection.execute("UPDATE capture_work_items SET owner_token='replacement' WHERE id=2")
        before = self.rows()
        with self.connection() as connection, self.assertRaises(durable_runs.LostOwnership), transaction(connection):
            runtime._renew_work_lease(connection, (1, 2), claim, at=after(30))
        self.assertEqual(self.rows(), before)

    def batch_context(self, execute, readiness=None):
        from v8 import profile_activations
        stack = self.execute_context(lambda *_a, **_k: None, readiness)
        stack.enter_context(patch.object(batches, "connect", self.connection))
        stack.enter_context(patch.object(batches, "now_utc", lambda: self.clock))
        stack.enter_context(patch.object(profile_activations, "activation_at", return_value={"profile_id": "integrated_route_v1"}))

        def freeze(connection, **_kwargs):
            members = [{"work": dict(row), "envelope": json.loads(row["envelope_json"])}
                       for row in connection.execute("SELECT * FROM capture_work_items ORDER BY id")]
            return {"batch_id": 11, "members": members}

        stack.enter_context(patch.object(batches, "freeze_batch", side_effect=freeze))
        stack.enter_context(patch.object(batches, "execute_batch", side_effect=execute))
        return stack

    def batch_result(self):
        return {"complete": True, "provider_cost": .001, "provider_calls": 1,
                "members": [{"content_id": identifier, "disposition": "valid"} for identifier in (1, 2)]}

    def test_statistics_batch_claims_after_readiness_and_finishes_at_real_time(self):
        self.second_member()

        def readiness(*_args, **_kwargs):
            self.clock = after(240)
            return "runnable", ""

        def execute(_frozen, **kwargs):
            self.assertEqual(kwargs["at"], after(240))
            self.assertIsNotNone(kwargs["lease_claim"])
            self.clock = after(265)
            return self.batch_result()

        with self.batch_context(execute, readiness):
            result = batches.run_one(self.db, NOW)
        self.assertEqual(result["member_count"], 2)
        work, attempt = self.rows()
        self.assertEqual(attempt["started_at"], after(240))
        self.assertEqual(attempt["completed_at"], after(265))
        with self.connection() as connection:
            self.assertEqual([row[0] for row in connection.execute("SELECT completed_at FROM capture_work_items")],
                             [runtime.planning.timestamp(after(265))] * 2)
        self.assertEqual(work["data_business_day"], "2026-09-07")

    def test_statistics_batch_refuses_to_finish_if_second_owner_changed(self):
        self.second_member()

        def execute(_frozen, **_kwargs):
            with self.connection() as connection:
                connection.execute("UPDATE capture_work_items SET owner_token='replacement' WHERE id=2")
            return self.batch_result()

        with self.batch_context(execute), self.assertRaises(durable_runs.LostOwnership):
            batches.run_one(self.db, NOW)
        with self.connection() as connection:
            self.assertEqual(connection.execute("SELECT count(*) FROM data_quality_receipts").fetchone()[0], 0)
            self.assertEqual(connection.execute("SELECT count(*) FROM capture_work_items WHERE state='terminal'").fetchone()[0], 0)

    def test_statistics_raw_replay_checks_all_owners_before_materialization(self):
        claim = self.second_member(self.claim())
        with self.connection() as connection:
            connection.executescript("""CREATE TABLE provider_raw_responses(id INTEGER,fetch_attempt_id INTEGER);
                CREATE TABLE fetch_attempts(id INTEGER,request_batch_id INTEGER);
                INSERT INTO provider_raw_responses VALUES(1,10);
                INSERT INTO fetch_attempts VALUES(10,11);""")
            connection.execute("UPDATE capture_work_items SET owner_token='replacement' WHERE id=2")
        frozen = {"batch_id": 11, "members": [{"work": {"id": identifier}} for identifier in (1, 2)]}
        with patch.object(batches, "connect", self.connection), patch.object(batches, "now_utc", return_value=NOW), patch.object(batches, "materialize_batch") as materialize:
            with self.assertRaises(durable_runs.LostOwnership):
                batches.execute_batch(frozen, db_path=self.db, at=NOW, lease_claim=claim)
        materialize.assert_not_called()

    def test_batch_leaves_each_worker_to_obtain_its_own_clock(self):
        with patch.object(runtime, "run_one", return_value={"provider_cost": 0}) as run:
            runtime.run_ready(self.db)
        self.assertEqual(run.call_count, 4)
        self.assertTrue(all(call.args == (self.db, None) for call in run.call_args_list))


if __name__ == "__main__":
    unittest.main()
