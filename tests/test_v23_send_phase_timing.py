"""Bounded send fairness and real SQLite timing, with no provider calls."""
from concurrent.futures import ThreadPoolExecutor
from contextvars import copy_context
import json
import os
from pathlib import Path
import sqlite3
import subprocess
import sys
import tempfile
import threading
import time
import unittest
from unittest.mock import patch

from v8 import runtime_paths, runtime_phase_timing as timing, storage


class SendPhaseTimingTest(unittest.TestCase):
    def setUp(self):
        self.temp = tempfile.TemporaryDirectory()
        self.addCleanup(self.temp.cleanup)
        self.root = Path(self.temp.name)
        self.root.chmod(0o700)
        self.metrics = self.root / "metrics.jsonl"
        self.lock = storage._FairWriteLock()
        self.enterContext(patch.object(storage, "_SQLITE_WRITE_TRANSACTION_LOCK", self.lock))
        self.enterContext(patch.dict(os.environ, {
            "DCAR_SQLITE_METRICS_FILE": str(self.metrics), "DCAR_SQLITE_TIMING_FILE": ""}))

    def queued(self, *, normal=0, send=0, heartbeat=0):
        until = time.monotonic() + 3
        while time.monotonic() < until:
            with self.lock._condition:
                if tuple(map(len, (self.lock._normal_waiters, self.lock._send_waiters,
                                   self.lock._heartbeat_waiters))) == (normal, send, heartbeat):
                    return
            time.sleep(.001)
        self.fail("Expected write queue did not form")

    def test_bounded_send_priority_survives_heartbeat_interleaving(self):
        order, workers = [], []
        def record(value, priority):
            with self.lock.hold(priority=priority):
                order.append(value)
        with self.lock:
            for index in range(2):
                worker = threading.Thread(target=record, args=("n" + str(index), "normal"))
                workers.append(worker); worker.start(); self.queued(normal=index + 1)
            for index in range(6):
                worker = threading.Thread(target=record, args=("s" + str(index), "send"))
                workers.append(worker); worker.start(); self.queued(normal=2, send=index + 1)
            for index in range(12):
                worker = threading.Thread(target=record, args=("h" + str(index), "heartbeat"))
                workers.append(worker); worker.start(); self.queued(normal=2, send=6, heartbeat=index + 1)
        for worker in workers:
            worker.join(3); self.assertFalse(worker.is_alive())
        self.assertEqual(order, [*["h" + str(i) for i in range(8)], "s0",
            *["h" + str(i) for i in range(8, 12)], "s1", "s2", "s3", "n0", "s4", "s5", "n1"])

    def test_four_sqlite_workers_send_ahead_of_queued_admissions_and_record_real_wait(self):
        database = self.root / "work.sqlite3"
        with sqlite3.connect(database) as connection:
            connection.execute("CREATE TABLE work(id PRIMARY KEY,state,reserved_ns,sent_ns)")
        first_admitted, second_admitted, send_queued = threading.Event(), threading.Event(), threading.Event()
        order = []
        real_emit = storage._emit_transaction_metric
        def emit(record):
            with self.lock._condition:
                self.assertNotEqual(self.lock._owner, threading.get_ident())
            real_emit(record)
        def work(identity):
            with sqlite3.connect(database) as connection:
                with storage.transaction_metrics_context(job_id="tikhub_paid_claim", work_id=identity), storage.transaction(connection):
                    with timing.phase("fixture.admission", rows=1):
                        connection.execute("INSERT INTO work VALUES (?,'reserved',?,NULL)",
                            (identity, time.monotonic_ns()))
                        order.append("a" + str(identity))
                        if identity == 0:
                            first_admitted.set()
                            self.queued(normal=3)
                        if identity == 1:
                            second_admitted.set()
                            # The first B must be waiting before this owner lets
                            # another normal admission take the lock.
                            self.queued(normal=2, send=1)
                            send_queued.set()
                        time.sleep(.01)
                if identity == 0:
                    self.assertTrue(second_admitted.wait(3))
                with storage.transaction_metrics_context(job_id="tikhub_paid_send", work_id=identity), storage.transaction(connection, priority="send"):
                    with timing.phase("fixture.send", rows=1):
                        connection.execute("UPDATE work SET state='sent',sent_ns=? WHERE id=?",
                            (time.monotonic_ns(), identity))
                        order.append("b" + str(identity))
        with patch.object(storage, "_emit_transaction_metric", side_effect=emit), ThreadPoolExecutor(max_workers=4) as pool:
            futures = [pool.submit(work, 0)]
            self.assertTrue(first_admitted.wait(3))
            # Queue A1 before A2/A3 so its controlled hold is deterministic.
            for identity in range(1, 4):
                futures.append(pool.submit(work, identity))
                if identity < 3:
                    self.queued(normal=identity)
            for future in futures:
                future.result(timeout=10)
        self.assertTrue(send_queued.is_set())
        self.assertLess(order.index("b0"), order.index("a2"))
        records = [json.loads(line) for line in self.metrics.read_text().splitlines()]
        finished = [row for row in records if row["phase"] == "finish"]
        self.assertEqual(len(finished), 8)
        self.assertTrue(all(row["stage_timing"]["dropped"] == 0 for row in finished))
        self.assertTrue(all(row["lock_hold_ms"] >= row["hold_ms"] for row in finished))
        first_send = next(row for row in finished if row["job_id"] == "tikhub_paid_send" and row["work_id"] == 0)
        self.assertGreaterEqual(first_send["queue_wait_ms"], 9)
        self.assertTrue(all(row["priority"] == "send" for row in finished if row["job_id"] == "tikhub_paid_send"))
        with sqlite3.connect(database) as connection:
            rows = connection.execute("SELECT state,(sent_ns-reserved_ns)/1000000.0 FROM work").fetchall()
        self.assertEqual([row[0] for row in rows], ["sent"] * 4)
        self.assertTrue(all(0 < row[1] < 180_000 for row in rows))
        # Real seconds, not a simulated performance claim about the formal DB.
        self.runtime_summary = {"fixture": "four-thread SQLite queue, no authority/provider",
            "max_lock_hold_ms": max(row["lock_hold_ms"] for row in finished),
            "max_queue_wait_ms": max(row["queue_wait_ms"] for row in finished),
            "max_reservation_age_ms": max(row[1] for row in rows)}

    def test_nested_flush_is_after_outer_release_and_failure_does_not_retry_commit(self):
        connection = sqlite3.connect(":memory:")
        self.addCleanup(connection.close)
        connection.execute("CREATE TABLE work(value)")
        emitted = []
        def emit(row):
            self.assertNotEqual(self.lock._owner, threading.get_ident())
            self.assertFalse(connection.in_transaction)
            emitted.append(row)
            if row["phase"] == "finish":
                raise OSError("fixture diagnostic failure")
        with patch.object(storage, "_emit_transaction_metric", side_effect=emit):
            with storage.write_lock():
                with storage.transaction(connection):
                    connection.execute("INSERT INTO work VALUES (1)")
                self.assertEqual(emitted, [])
        self.assertEqual(connection.execute("SELECT value FROM work").fetchall(), [(1,)])
        self.assertEqual([row["phase"] for row in emitted], ["begin", "finish"])

    def test_phase_tree_exclusive_time_thread_binding_and_control_exceptions(self):
        with timing.recording() as record:
            with timing.phase("parent"):
                with timing.phase("child", rows=2):
                    time.sleep(.002)
                with ThreadPoolExecutor(max_workers=1) as pool:
                    pool.submit(copy_context().run, timing.measured, "foreign", lambda: None).result()
            with self.assertRaises(KeyboardInterrupt):
                with timing.phase("interrupt"):
                    raise KeyboardInterrupt()
        events = timing.snapshot(record)["events"]
        self.assertEqual([event["name"] for event in events], ["parent", "child", "interrupt"])
        self.assertEqual(events[1]["parent"], events[0]["id"])
        self.assertEqual(events[0]["exclusive_ns"] + events[1]["wall_ns"], events[0]["wall_ns"])
        self.assertEqual(events[-1]["outcome"], "error")

    def test_local_subprocess_stages_keep_output_and_nonzero_error(self):
        with timing.recording() as record:
            result = runtime_paths._measured_git_run([sys.executable, "-c", "print('ok')"], env=dict(os.environ))
            with self.assertRaises(subprocess.CalledProcessError) as caught:
                runtime_paths._measured_git_run([sys.executable, "-c", "import sys; print('bad'); sys.exit(7)"], env=dict(os.environ))
        self.assertEqual(result.stdout, b"ok\n")
        self.assertEqual((caught.exception.returncode, caught.exception.output), (7, b"bad\n"))
        self.assertEqual([event["name"] for event in record["events"]],
            ["subprocess.spawn_setup", "subprocess.communicate_wait"] * 2)
