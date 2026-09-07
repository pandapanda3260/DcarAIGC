from __future__ import annotations

import json
import os
import sqlite3
import tempfile
import threading
import time
import unittest
from pathlib import Path
from unittest.mock import patch

from v8 import storage


class FairWriteLockTest(unittest.TestCase):
    def setUp(self) -> None:
        self.lock = storage._FairWriteLock()
        self.threads: list[threading.Thread] = []
        self.errors: list[BaseException] = []

    def tearDown(self) -> None:
        for worker in self.threads:
            worker.join(timeout=2)
            self.assertFalse(worker.is_alive(), "write-lock worker did not finish")
        if self.errors:
            raise self.errors[0]

    def spawn(self, function) -> threading.Thread:
        def checked() -> None:
            try:
                function()
            except BaseException as error:
                self.errors.append(error)

        worker = threading.Thread(target=checked, daemon=True)
        self.threads.append(worker)
        worker.start()
        return worker

    def queued(self, normal: int = 0, heartbeat: int = 0) -> None:
        deadline = time.monotonic() + 2
        while time.monotonic() < deadline:
            with self.lock._condition:
                if (len(self.lock._normal_waiters), len(self.lock._heartbeat_waiters)) == (
                    normal, heartbeat
                ):
                    return
            time.sleep(0.001)
        self.fail(f"expected {normal} ordinary and {heartbeat} heartbeat waiters")

    def record(self, output: list, value: object, *, priority="normal") -> None:
        with self.lock.hold(priority=priority):
            output.append(value)

    def test_ordinary_writers_acquire_in_fifo_order(self) -> None:
        order: list[int] = []
        with self.lock:
            for index in range(6):
                self.spawn(lambda value=index: self.record(order, value))
                self.queued(normal=index + 1)
        for worker in self.threads:
            worker.join(timeout=2)
        self.assertEqual(order, list(range(6)))

    def test_release_and_immediate_reacquire_cannot_barge(self) -> None:
        order: list[str] = []
        with self.lock:
            self.spawn(lambda: self.record(order, "waiting"))
            self.queued(normal=1)
        with self.lock:
            order.append("previous-owner")
        self.assertEqual(order, ["waiting", "previous-owner"])

    def test_heartbeat_bypasses_all_queued_normal_writers(self) -> None:
        order: list[object] = []
        with self.lock:
            for index in range(4):
                self.spawn(lambda value=index: self.record(order, value))
                self.queued(normal=index + 1)
            self.spawn(lambda: self.record(order, "heartbeat", priority="heartbeat"))
            self.queued(normal=4, heartbeat=1)
        for worker in self.threads:
            worker.join(timeout=2)
        self.assertEqual(order, ["heartbeat", 0, 1, 2, 3])

    def test_heartbeats_are_fifo_within_their_priority(self) -> None:
        order: list[object] = []
        with self.lock:
            self.spawn(lambda: self.record(order, "ordinary"))
            for index in range(4):
                self.spawn(lambda value=index: self.record(order, value, priority="heartbeat"))
                self.queued(normal=1, heartbeat=index + 1)
        for worker in self.threads:
            worker.join(timeout=2)
        self.assertEqual(order, [0, 1, 2, 3, "ordinary"])

    def test_continuous_normal_writers_cannot_starve_heartbeat(self) -> None:
        stop = threading.Event()
        heartbeat_done = threading.Event()
        order: list[object] = []

        def write_repeatedly(index: int) -> None:
            while not stop.is_set():
                with self.lock:
                    order.append(index)
                    time.sleep(0.001)

        def heartbeat() -> None:
            with self.lock.hold(priority="heartbeat"):
                order.append("heartbeat")
                heartbeat_done.set()

        try:
            with self.lock:
                for index in range(8):
                    self.spawn(lambda value=index: write_repeatedly(value))
                    self.queued(normal=index + 1)
                self.spawn(heartbeat)
                self.queued(normal=8, heartbeat=1)
            self.assertTrue(heartbeat_done.wait(timeout=1))
            self.assertEqual(order[0], "heartbeat")
        finally:
            stop.set()

    def test_continuous_heartbeat_queue_gives_normal_writer_a_bounded_turn(self) -> None:
        order: list[object] = []
        with self.lock:
            self.spawn(lambda: self.record(order, "ordinary"))
            for index in range(12):
                self.spawn(lambda value=index: self.record(order, value, priority="heartbeat"))
                self.queued(normal=1, heartbeat=index + 1)
        for worker in self.threads:
            worker.join(timeout=2)
        self.assertEqual(order, [*range(8), "ordinary", *range(8, 12)])

    def test_uncontended_heartbeats_do_not_accumulate_priority_debt(self) -> None:
        for _ in range(10):
            with self.lock.hold(priority="heartbeat"):
                pass
        order: list[str] = []
        with self.lock:
            self.spawn(lambda: self.record(order, "ordinary"))
            self.spawn(lambda: self.record(order, "heartbeat", priority="heartbeat"))
            self.queued(normal=1, heartbeat=1)
        for worker in self.threads:
            worker.join(timeout=2)
        self.assertEqual(order, ["heartbeat", "ordinary"])

    def test_priority_never_preempts_a_transaction_already_in_progress(self) -> None:
        acquired = threading.Event()

        def heartbeat() -> None:
            with self.lock.hold(priority="heartbeat"):
                acquired.set()

        with self.lock:
            self.spawn(heartbeat)
            self.queued(heartbeat=1)
            self.assertFalse(acquired.is_set())
        self.assertTrue(acquired.wait(timeout=2))

    def test_reentrant_acquire_keeps_outer_ownership_and_queue_order(self) -> None:
        order: list[str] = []
        with self.lock:
            self.spawn(lambda: self.record(order, "waiter"))
            self.queued(normal=1)
            with self.lock.hold(priority="heartbeat"):
                with self.lock:
                    self.assertEqual(self.lock._depth, 3)
            self.assertEqual(self.lock._depth, 1)
            self.assertEqual(order, [])
        for worker in self.threads:
            worker.join(timeout=2)
        self.assertEqual(order, ["waiter"])

    def test_exception_releases_owner_and_nested_exception_preserves_outer_lock(self) -> None:
        with self.assertRaisesRegex(ValueError, "outer"):
            with self.lock:
                with self.assertRaisesRegex(ValueError, "inner"):
                    with self.lock:
                        raise ValueError("inner")
                self.assertEqual(self.lock._depth, 1)
                raise ValueError("outer")
        order: list[str] = []
        self.spawn(lambda: self.record(order, "released")).join(timeout=2)
        self.assertEqual(order, ["released"])

    def test_interrupted_waiter_is_removed_and_does_not_block_successors(self) -> None:
        with self.lock:
            def interrupted() -> None:
                with patch.object(self.lock._condition, "wait", side_effect=InterruptedError):
                    with self.assertRaises(InterruptedError):
                        self.lock.acquire(priority="heartbeat")

            self.spawn(interrupted).join(timeout=2)
            self.queued()
            self.spawn(lambda: self.record([], "successor"))
            self.queued(normal=1)

    def test_non_owner_cannot_release_another_thread_lock(self) -> None:
        with self.lock:
            def incorrect_release() -> None:
                with self.assertRaisesRegex(RuntimeError, "unowned"):
                    self.lock.release()

            self.spawn(incorrect_release).join(timeout=2)
            self.assertEqual(self.lock._owner, threading.get_ident())
            self.assertEqual(self.lock._depth, 1)

    def test_invalid_priority_fails_before_entering_queue(self) -> None:
        with self.assertRaises(ValueError):
            self.lock.acquire(priority="invalid")
        self.queued()

    def test_nonblocking_acquire_supports_free_and_reentrant_lock(self) -> None:
        self.assertTrue(self.lock.acquire(False))
        self.assertTrue(self.lock.acquire(blocking=False, priority="heartbeat"))
        self.assertEqual(self.lock._depth, 2)
        self.lock.release()
        self.lock.release()
        self.assertIsNone(self.lock._owner)

    def test_nonblocking_failure_leaves_no_waiter_in_either_queue(self) -> None:
        with self.lock:
            def try_locked() -> None:
                self.assertFalse(self.lock.acquire(blocking=False))
                self.assertFalse(self.lock.acquire(False, priority="heartbeat"))

            self.spawn(try_locked).join(timeout=2)
            self.queued()

    def test_nonblocking_acquire_cannot_barge_an_existing_waiter(self) -> None:
        release_waiter = threading.Event()

        def existing_waiter() -> None:
            with self.lock:
                self.assertTrue(release_waiter.wait(timeout=2))

        try:
            with self.lock:
                self.spawn(existing_waiter)
                self.queued(normal=1)
            self.assertFalse(self.lock.acquire(blocking=False))
        finally:
            release_waiter.set()

    def test_finite_timeout_withdraws_ticket_without_blocking_successor(self) -> None:
        order: list[str] = []
        with self.lock:
            def timeout_waiter() -> None:
                self.assertFalse(self.lock.acquire(timeout=0.01, priority="heartbeat"))

            timed = self.spawn(timeout_waiter)
            self.queued(heartbeat=1)
            self.spawn(lambda: self.record(order, "ordinary"))
            timed.join(timeout=2)
            self.queued(normal=1)
        for worker in self.threads:
            worker.join(timeout=2)
        self.assertEqual(order, ["ordinary"])

    def test_timed_acquire_succeeds_when_owner_releases_before_deadline(self) -> None:
        result: list[bool] = []

        def timed_waiter() -> None:
            acquired = self.lock.acquire(timeout=1)
            result.append(acquired)
            if acquired:
                self.lock.release()

        with self.lock:
            self.spawn(timed_waiter)
            self.queued(normal=1)
        for worker in self.threads:
            worker.join(timeout=2)
        self.assertEqual(result, [True])

    def test_zero_timeout_is_an_immediate_attempt_and_preserves_reentrancy(self) -> None:
        self.assertTrue(self.lock.acquire(timeout=0))
        try:
            self.assertTrue(self.lock.acquire(True, 0, priority="heartbeat"))
            self.lock.release()

            def locked_attempt() -> None:
                self.assertFalse(self.lock.acquire(timeout=0))

            self.spawn(locked_attempt).join(timeout=2)
            self.queued()
        finally:
            self.lock.release()

    def test_invalid_timeouts_match_rlock_errors_before_entering_queue(self) -> None:
        for arguments, error_type in (
            ({"blocking": False, "timeout": 0}, ValueError),
            ({"timeout": -2}, ValueError),
            ({"timeout": float("nan")}, ValueError),
            ({"timeout": float("inf")}, OverflowError),
        ):
            with self.subTest(arguments=arguments):
                with self.assertRaises(error_type):
                    self.lock.acquire(**arguments)
                self.queued()

    def test_shared_outer_write_lock_can_nest_existing_transactions(self) -> None:
        first = sqlite3.connect(":memory:")
        second = sqlite3.connect(":memory:")
        self.addCleanup(first.close)
        self.addCleanup(second.close)
        first.execute("CREATE TABLE records(value)")
        second.execute("CREATE TABLE records(value)")
        with patch.object(storage, "_SQLITE_WRITE_TRANSACTION_LOCK", self.lock):
            # The account route wrapper will take this same lock without BEGIN.
            with storage._SQLITE_WRITE_TRANSACTION_LOCK:
                with storage.transaction(first):
                    first.execute("INSERT INTO records VALUES (1)")
                    with storage.transaction(second, priority="heartbeat"):
                        second.execute("INSERT INTO records VALUES (2)")
            self.assertIsNone(self.lock._owner)
        self.assertEqual(first.execute("SELECT value FROM records").fetchone()[0], 1)
        self.assertEqual(second.execute("SELECT value FROM records").fetchone()[0], 2)

    def test_transaction_failure_rolls_back_and_releases_for_next_writer(self) -> None:
        connection = sqlite3.connect(":memory:")
        self.addCleanup(connection.close)
        connection.execute("CREATE TABLE records(value)")
        with patch.object(storage, "_SQLITE_WRITE_TRANSACTION_LOCK", self.lock):
            with self.assertRaisesRegex(ValueError, "rollback"):
                with storage.transaction(connection, priority="heartbeat"):
                    connection.execute("INSERT INTO records VALUES (1)")
                    raise ValueError("rollback")
            with storage.transaction(connection):
                connection.execute("INSERT INTO records VALUES (2)")
        self.assertEqual(connection.execute("SELECT value FROM records").fetchall(), [(2,)])
        self.assertIsNone(self.lock._owner)

    def test_transaction_metrics_include_priority_and_actual_queue_wait(self) -> None:
        with tempfile.TemporaryDirectory() as temporary:
            root = Path(temporary)
            root.chmod(0o700)
            metrics = root / "transactions.jsonl"
            with patch.dict(os.environ, {"DCAR_SQLITE_METRICS_FILE": str(metrics)}), patch.object(
                storage, "_SQLITE_WRITE_TRANSACTION_LOCK", self.lock
            ):
                def write_heartbeat() -> None:
                    connection = sqlite3.connect(":memory:")
                    try:
                        with storage.transaction(connection, priority="heartbeat"):
                            connection.execute("CREATE TABLE records(value)")
                    finally:
                        connection.close()

                with self.lock:
                    self.spawn(write_heartbeat)
                    self.queued(heartbeat=1)
                    time.sleep(0.01)
                for worker in self.threads:
                    worker.join(timeout=2)
            records = [json.loads(line) for line in metrics.read_text().splitlines()]
            self.assertEqual([row["phase"] for row in records], ["begin", "finish"])
            self.assertTrue(all(row["priority"] == "heartbeat" for row in records))
            self.assertGreaterEqual(records[0]["queue_wait_ms"], 10)
            self.assertGreaterEqual(records[1]["hold_ms"], 0)

    def test_real_capture_renewal_beats_four_queued_provider_writes(self) -> None:
        from tests.test_v8_capture_runtime_lease import CaptureRuntimeLeaseTest, after
        from v8 import capture_runtime as runtime

        fixture = CaptureRuntimeLeaseTest()
        fixture.setUp()
        self.addCleanup(fixture.doCleanups)
        elapsed = 50
        lease_at_first_provider: list[str] = []

        def provider_write() -> None:
            nonlocal elapsed
            with fixture.connection() as connection, storage.transaction(connection):
                lease_at_first_provider.append(connection.execute(
                    "SELECT lease_expires_at FROM capture_work_items WHERE id=1"
                ).fetchone()[0])
                elapsed += 50
                fixture.clock = after(elapsed)
                # Each simulated 50-second admission gives the heartbeat thread
                # time to queue, while the other admissions remain queued.
                time.sleep(0.01)

        with patch.object(storage, "_SQLITE_WRITE_TRANSACTION_LOCK", self.lock), patch.object(
            runtime.durable_runs, "HEARTBEAT_SECONDS", 0.005
        ):
            claim = fixture.claim()
            with runtime._maintain_work_lease(fixture.db, 1, claim) as check:
                with self.lock:
                    fixture.clock = after(elapsed)
                    for _ in range(4):
                        self.spawn(provider_write)
                    deadline = time.monotonic() + 2
                    while True:
                        with self.lock._condition:
                            if len(self.lock._normal_waiters) + len(self.lock._heartbeat_waiters) == 5:
                                break
                        if time.monotonic() >= deadline:
                            self.fail("four provider writes and one heartbeat did not queue")
                        time.sleep(0.001)
                for worker in self.threads:
                    worker.join(timeout=2)
                check()
                work, attempt = fixture.rows()
                self.assertGreater(work["lease_expires_at"], fixture.clock)
                self.assertEqual(work["lease_expires_at"], attempt["lease_expires_at"])
                self.assertGreater(lease_at_first_provider[0], after(180))


if __name__ == "__main__":
    unittest.main()
