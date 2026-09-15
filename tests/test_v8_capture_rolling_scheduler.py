"""Bounded fair dispatch against isolated queues; provider traffic is forbidden."""
from __future__ import annotations

from collections import Counter
from contextlib import nullcontext
from contextvars import ContextVar
from itertools import count
import json
import socket
from threading import Event, Lock
import unittest
from unittest.mock import patch

from tests import test_v8_capture_runtime_lease as fixtures
from v8 import capture_batches, capture_runtime as runtime, pipeline

NOW = fixtures.NOW
LANES = ("ordinary", "douyin", "kuaishou", "xiaohongshu", "wechat_channels")


class CaptureRollingSchedulerTest(unittest.TestCase):
    connection = fixtures.CaptureRuntimeLeaseTest.connection
    complete = fixtures.CaptureRuntimeLeaseTest.complete
    execute_context = fixtures.CaptureRuntimeLeaseTest.execute_context

    def setUp(self):
        fixtures.CaptureRuntimeLeaseTest.setUp(self)
        self.stack.enter_context(patch.object(socket.socket, "connect",
            side_effect=AssertionError("provider network forbidden")))
        with self.connection() as connection:
            connection.execute("DELETE FROM capture_work_items")

    def add_work(self, identifier, lane="ordinary", *, due=NOW, state="runnable", operation=None, extra=None):
        envelope = {**self.envelope, "fixture_id": identifier, "platform": lane}
        if lane != "ordinary":
            envelope.update(stage="profile_prepare", intake_request_id=identifier,
                preparation_plan_id=1, preparation_key=f"prepare-{identifier}",
                preparation_subject=f"subject-{identifier}")
        envelope.update(extra or {})
        with self.connection() as connection:
            connection.execute("""INSERT INTO capture_work_items(id,work_identity,operation,due_at,data_business_day,
                state,reason,envelope_json,attempt_count,created_at,updated_at)
                VALUES(?,?,?,?,'2026-09-07',?,'',?,0,?,?)""",
                (identifier, f"work-{identifier}", operation or f"{lane}_profile", runtime.planning.timestamp(due),
                 state, json.dumps(envelope), NOW, NOW))

    def test_each_platform_and_ordinary_work_get_turns_with_real_claims(self):
        # Deliberately group IDs by platform: global FIFO would spend all
        # sixteen requests on preparation before reaching ordinary work.
        for offset, lane in enumerate((*LANES[1:], "ordinary")):
            for number in range(4):
                self.add_work(offset * 4 + number + 1, lane)
        executed = []

        def execute(envelope, **_kwargs):
            executed.append((envelope["platform"], envelope["fixture_id"]))
            return self.complete(envelope)

        with self.execute_context(execute):
            result = runtime.run_ready(self.db, max_items=1, rolling=True)
        self.assertEqual([lane for lane, _ in executed], list(LANES) * 3 + ["ordinary"])
        self.assertEqual(len({identifier for _, identifier in executed}), 16)
        # FIFO is preserved within each platform, and claims/finalization are
        # the production SQLite and durable-run code, not a mocked dispatcher.
        for lane in LANES:
            identifiers = [identifier for selected, identifier in executed if selected == lane]
            self.assertEqual(identifiers, sorted(identifiers))
        with self.connection() as connection:
            states = dict(connection.execute("SELECT state,count(*) FROM capture_work_items GROUP BY state"))
            self.assertEqual(connection.execute("SELECT count(*) FROM scheduler_run_attempts").fetchone()[0], 16)
        self.assertEqual(states, {"terminal": 16, "runnable": 4})
        self.assertEqual(len(result["results"]), 16)

    def test_each_refill_rechecks_current_readiness_before_claim_and_send(self):
        for identifier in range(1, 4):
            self.add_work(identifier, "douyin")
        checked = []
        executed = []

        def readiness(_connection, envelope, *, at):
            checked.append((envelope["fixture_id"], at))
            return {1: ("runnable", ""), 2: ("budget_deferred", "budget_exhausted"),
                    3: ("paid_identity_hold", "billing_unknown")}[envelope["fixture_id"]]

        def execute(envelope, **_kwargs):
            executed.append(envelope["fixture_id"])
            self.clock = fixtures.after(60)
            return self.complete(envelope)

        with self.execute_context(execute, readiness):
            result = runtime.run_ready(self.db, "2000-01-01T00:00:00Z", max_items=1, rolling=True)
        self.assertEqual(executed, [1])
        self.assertEqual(checked, [(1, NOW), (2, fixtures.after(60)), (3, fixtures.after(60))])
        self.assertEqual([row["status"] for row in result["results"]],
                         ["terminal", "budget_deferred", "paid_identity_hold", "idle"])
        with self.connection() as connection:
            self.assertEqual(connection.execute("SELECT count(*) FROM scheduler_run_attempts").fetchone()[0], 1)
            self.assertEqual([tuple(row) for row in connection.execute(
                "SELECT state,attempt_count FROM capture_work_items ORDER BY id")],
                [("terminal", 1), ("budget_deferred", 0), ("paid_identity_hold", 0)])

    def test_concurrent_claims_execute_each_work_once(self):
        for identifier in range(1, 21):
            self.add_work(identifier)
        executed = []
        lock = Lock()

        def execute(envelope, **_kwargs):
            with lock:
                executed.append(envelope["fixture_id"])
            return self.complete(envelope)

        with self.execute_context(execute):
            result = runtime.run_ready(self.db, rolling=True)
        self.assertEqual(len(executed), 16)
        self.assertEqual(len(set(executed)), 16)
        self.assertTrue(all(row["status"] == "terminal" for row in result["results"]))
        with self.connection() as connection:
            self.assertEqual(connection.execute("SELECT count(*) FROM scheduler_run_attempts").fetchone()[0], 16)

    def test_completion_refills_before_slow_peer_finishes_and_stays_bounded(self):
        all_started, refilled = Event(), Event()
        lock = Lock()
        context = ContextVar("rolling_fixture_authority")
        token = context.set("current-authority")
        self.addCleanup(context.reset, token)
        calls, active, peak = [], 0, 0

        def dispatch(db, at=None):
            nonlocal active, peak
            with lock:
                index = len(calls)
                calls.append((db, at, runtime._WORK_SELECTION_LANE.get(), context.get()))
                active += 1
                peak = max(peak, active)
                if index == 3:
                    all_started.set()
            try:
                if index == 0:
                    self.assertTrue(refilled.wait(3), "slow peer held the entire worker batch")
                elif index < 4:
                    self.assertTrue(all_started.wait(3))
                else:
                    refilled.set()
                return {"status": "terminal", "provider_cost": .25}
            finally:
                with lock:
                    active -= 1

        with patch.object(runtime, "run_one", side_effect=dispatch):
            result = runtime.run_ready(self.db, "2000-01-01T00:00:00Z", rolling=True)
        self.assertEqual(len(calls), 16)
        self.assertEqual(peak, 4)
        self.assertEqual(active, 0)
        self.assertTrue(all(db == self.db and at is None and authority == "current-authority"
                            for db, at, _, authority in calls))
        self.assertEqual(Counter(lane for _, _, lane, _ in calls),
                         {"ordinary": 4, "douyin": 3, "kuaishou": 3, "xiaohongshu": 3, "wechat_channels": 3})
        self.assertEqual(result["provider_cost"], 4)
        self.assertIsNone(runtime._WORK_SELECTION_LANE.get())

    def test_empty_lane_falls_back_without_selecting_held_or_future_work(self):
        self.add_work(1, "douyin", state="paid_identity_hold")
        self.add_work(2, "douyin", due=fixtures.after(60))
        self.add_work(3, "kuaishou")
        with self.execute_context(lambda envelope, **_kwargs: self.complete(envelope)):
            result = runtime._run_ready_one(self.db, "wechat_channels")
        self.assertEqual(result["work_id"], 3)

    def test_selector_keeps_explicit_id_and_operation_filters(self):
        self.add_work(1, "douyin", operation=capture_batches.OPERATION)
        self.add_work(2, "kuaishou")
        self.add_work(3, "douyin")
        token = runtime._WORK_SELECTION_LANE.set("douyin")
        try:
            with self.connection() as connection:
                row = runtime._select_runnable_work(connection, NOW,
                    filters=" AND id=? AND operation<>'douyin_video_statistics'", parameters=(2,))
                self.assertEqual(row["id"], 2)
                row = runtime._select_runnable_work(connection, NOW,
                    filters=" AND operation<>'douyin_video_statistics'")
                self.assertEqual(row["id"], 3)
        finally:
            runtime._WORK_SELECTION_LANE.reset(token)

    def test_statistics_and_compensation_keep_their_dispatch_boundaries(self):
        self.add_work(1, operation=capture_batches.OPERATION)
        self.add_work(2, "kuaishou")
        with patch.object(capture_batches, "run_one", return_value={"status": "statistics"}) as batch, \
             patch.object(runtime, "_run_single", return_value={"status": "single"}) as single:
            self.assertEqual(runtime._run_ready_one(self.db, "kuaishou")["status"], "single")
            batch.assert_not_called()
            self.assertEqual(runtime._run_ready_one(self.db, "ordinary")["status"], "statistics")
            batch.assert_called_once_with(self.db, NOW)
            single.assert_called_once_with(self.db, NOW)
        with self.connection() as connection:
            envelope = {**self.envelope, "compensation": {"fixture": True}}
            connection.execute("UPDATE capture_work_items SET envelope_json=? WHERE id=1", (json.dumps(envelope),))
        with patch("v8.capture_compensation.run_authorized_work", return_value={"status": "compensation"}) as authorized:
            self.assertEqual(runtime._run_ready_one(self.db, "ordinary")["status"], "compensation")
            authorized.assert_called_once_with(self.db, 1, NOW)

    def test_idle_or_shadow_stops_refilling(self):
        for state in ("idle", "shadow"):
            with self.subTest(state=state), patch.object(runtime, "run_one", return_value={"status": state}) as dispatch:
                runtime.run_ready(self.db, rolling=True)
                self.assertEqual(dispatch.call_count, 4)

    def test_statistics_idle_does_not_prevent_waiting_platform_from_filling_a_slot(self):
        self.add_work(1, operation=capture_batches.OPERATION)
        for identifier, lane in enumerate(LANES[1:], 2):
            self.add_work(identifier, lane)
        wechat_started = Event()
        executed = []
        lock = Lock()

        def execute(envelope, **_kwargs):
            if envelope["platform"] == "wechat_channels":
                wechat_started.set()
            else:
                self.assertTrue(wechat_started.wait(3), "statistics idle prevented the fifth lane from starting")
            with lock:
                executed.append(envelope["platform"])
            return self.complete(envelope)

        # Batch idle is operation-local. Its runnable row may remain while
        # other platforms still have genuine work awaiting their turn.
        with self.execute_context(execute), \
             patch.object(capture_batches, "run_one", return_value={"status": "idle"}) as batch:
            result = runtime.run_ready(self.db, rolling=True)
        batch.assert_called()
        self.assertEqual(set(executed), set(LANES[1:]))
        self.assertEqual(len(executed), 4)
        self.assertEqual(len(result["results"]), 16)
        with self.connection() as connection:
            self.assertEqual(connection.execute("SELECT count(*) FROM capture_work_items WHERE state='terminal'").fetchone()[0], 4)

    def test_worker_failure_refills_manual_lane_before_slow_peers_finish(self):
        with self.connection() as connection:
            connection.execute("PRAGMA user_version=23")
        all_started, manual_started = Event(), Event()
        lock = Lock()
        calls, active, peak = [], 0, 0
        context = ContextVar("rolling_failure_authority")
        token = context.set("current-authority")
        self.addCleanup(context.reset, token)

        def dispatch(_db, at=None):
            nonlocal active, peak
            with lock:
                index = len(calls)
                lane = runtime._WORK_SELECTION_LANE.get()
                calls.append((lane, context.get(), at))
                active += 1
                peak = max(peak, active)
                if index == 3:
                    all_started.set()
            try:
                if index == 0:
                    self.assertTrue(all_started.wait(3))
                    raise runtime.durable_runs.LostOwnership("fixture owner expired")
                if index < 4:
                    self.assertTrue(manual_started.wait(3), "failed peer prevented manual slot refill")
                if lane == "manual_media":
                    manual_started.set()
                return {"status": "terminal", "provider_cost": .25}
            finally:
                with lock:
                    active -= 1

        # Round 2 starts at lane 5, putting the first manual lane in refill
        # slot 4 rather than among the four initial requests.
        with patch.object(runtime, "_V23_ROLLING_ROUNDS", count(2)), \
             patch.object(runtime, "run_one", side_effect=dispatch):
            result = runtime.run_ready(self.db, "2000-01-01T00:00:00Z", rolling=True)
        self.assertEqual((len(calls), peak, active), (16, 4, 0))
        self.assertEqual(calls[4][0], "manual_media")
        self.assertTrue(all(authority == "current-authority" and at is None for _, authority, at in calls))
        self.assertEqual(result["status"], "rolling_partial")
        self.assertEqual(result["failed_count"], 1)
        self.assertEqual(result["results"][0], {"status": "worker_failed", "error_type": "LostOwnership",
            "reason": "fixture owner expired", "dispatch_index": 0, "lane": "douyin"})
        self.assertEqual(result["provider_cost"], 3.75)
        self.assertFalse(result["provider_cost_complete"])
        self.assertIsNone(runtime._WORK_SELECTION_LANE.get())

    def test_lost_owner_preserves_real_work_attempt_and_paid_ledger(self):
        self.add_work(1)
        tables = ("capture_work_items", "scheduler_runs", "scheduler_run_attempts",
                  "provider_usage", "paid_provider_dispatch_events", "data_quality_receipts")
        with self.connection() as connection:
            connection.executescript("""
                CREATE TABLE provider_usage(id INTEGER PRIMARY KEY, amount REAL);
                CREATE TABLE paid_provider_dispatch_events(id INTEGER PRIMARY KEY,
                    scheduler_attempt_id INTEGER, event_type TEXT, provider_usage_id INTEGER);
            """)
        before = {}

        def execute(envelope, **_kwargs):
            with self.connection() as connection:
                connection.execute("UPDATE capture_work_items SET owner_token='replacement'")
                connection.execute("INSERT INTO provider_usage VALUES(1,.25)")
                connection.execute("""INSERT INTO paid_provider_dispatch_events
                    SELECT 1,id,'send_marked',1 FROM scheduler_run_attempts""")
                before.update({table: [tuple(row) for row in connection.execute("SELECT * FROM " + table)]
                               for table in tables})
            return self.complete(envelope)

        with self.execute_context(execute):
            result = runtime.run_ready(self.db, max_items=1, rolling=True)
        self.assertEqual(result["status"], "rolling_partial")
        self.assertEqual(result["failed_count"], 1)
        self.assertEqual(result["results"][0]["error_type"], "LostOwnership")
        self.assertFalse(result["provider_cost_complete"])
        with self.connection() as connection:
            after = {table: [tuple(row) for row in connection.execute("SELECT * FROM " + table)]
                     for table in tables}
        self.assertEqual(after, before)

    def test_exceptions_restore_hint_and_remain_bounded(self):
        with patch.object(runtime, "run_one", side_effect=RuntimeError("fixture failure")) as dispatch:
            result = runtime.run_ready(self.db, max_items=1, rolling=True)
            self.assertEqual(dispatch.call_count, 16)
            self.assertEqual(result["status"], "rolling_partial")
            self.assertEqual(result["failed_count"], 16)
            self.assertTrue(all(row["reason"] == "fixture failure" for row in result["results"]))
            with self.assertRaisesRegex(RuntimeError, "fixture failure"):
                runtime._run_ready_one(self.db, "douyin")
        self.assertIsNone(runtime._WORK_SELECTION_LANE.get())

    def test_process_control_exception_is_not_collected_as_worker_failure(self):
        for exception in (KeyboardInterrupt, SystemExit):
            with self.subTest(exception=exception), \
                 patch.object(runtime, "run_one", side_effect=exception("fixture stop")) as dispatch:
                with self.assertRaisesRegex(exception, "fixture stop"):
                    runtime.run_ready(self.db, max_items=1, rolling=True)
                self.assertEqual(dispatch.call_count, 1)
            self.assertIsNone(runtime._WORK_SELECTION_LANE.get())

    def test_default_batch_retains_supplied_clock_and_size(self):
        with patch.object(runtime, "run_one", return_value={"provider_cost": 0}) as dispatch:
            result = runtime.run_ready(self.db, NOW, max_items=2)
        self.assertEqual(dispatch.call_count, 2)
        self.assertTrue(all(call.args == (self.db, NOW) for call in dispatch.call_args_list))
        self.assertEqual(result["status"], "batch_complete")
        self.assertEqual(result["max_requests"], 2)

    def test_periodic_entry_preserves_rolling_for_schema22_and_23(self):
        for version in (20, 21, 22, 23):
            with self.subTest(version=version):
                with self.connection() as connection:
                    connection.execute(f"PRAGMA user_version={version}")
                with patch.object(pipeline, "connect", self.connection), \
                     patch("v8.runtime_database.require_current_process_writer_lock"), \
                     patch("v8.capture_authorizations.runtime_authority", return_value=nullcontext()), \
                     patch("v8.account_roster_capture.activate_prepared_roster_capture_in_transaction"), \
                     patch("v8.capture_commands.process_commands"), \
                     patch.object(runtime, "run_ready", return_value={"status": "fixture"}) as dispatch:
                    pipeline._capture_v25_job(kind="execute", db_path=self.db, at=NOW)
                dispatch.assert_called_once_with(self.db, NOW, max_items=4, rolling=version in {22, 23})


if __name__ == "__main__":
    unittest.main()
