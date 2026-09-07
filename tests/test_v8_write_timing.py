"""Optional runtime timings never participate in SQLite business transactions."""
from __future__ import annotations

import json
import os
import sqlite3
import stat
import tempfile
import threading
import time
import unittest
from pathlib import Path
from unittest.mock import patch

from v8 import storage


class WriteTimingTest(unittest.TestCase):
    def setUp(self):
        self.temp = tempfile.TemporaryDirectory()
        self.addCleanup(self.temp.cleanup)
        self.root = Path(self.temp.name)
        self.root.chmod(0o700)
        self.path = self.root / "timing.jsonl"
        self.enterContext(patch.dict(os.environ, {"DCAR_SQLITE_TIMING_FILE": str(self.path), "DCAR_SQLITE_METRICS_FILE": ""}))
        self.connection = sqlite3.connect(":memory:")
        self.addCleanup(self.connection.close)
        self.connection.execute("CREATE TABLE probe(value TEXT)")

    def rows(self):
        return [json.loads(line) for line in self.path.read_text().splitlines()]

    def test_one_finish_after_release_and_commit_without_private_context(self):
        statements, writes = [], []
        self.connection.set_trace_callback(statements.append)
        real_write = os.write
        def write(fd, record):
            with storage._SQLITE_WRITE_TRANSACTION_LOCK._condition:
                self.assertNotEqual(storage._SQLITE_WRITE_TRANSACTION_LOCK._owner, threading.get_ident())
            self.assertFalse(self.connection.in_transaction)
            writes.append(record)
            return real_write(fd, record)
        with storage.transaction_metrics_context(job_id="content_pipeline", token="private-token",
                sql="private-sql", phone="private-phone"), patch.object(storage.os, "write", side_effect=write):
            with storage.transaction(self.connection, priority="heartbeat"):
                self.connection.execute("INSERT INTO probe VALUES ('saved')")
                self.assertFalse(self.path.exists())
        self.assertEqual(len(writes), 1)
        row = self.rows()[0]
        self.assertEqual((row["phase"], row["priority"], row["outcome"]), ("finish", "heartbeat", "completed"))
        self.assertEqual(row["job_category"], "content_pipeline")
        self.assertNotIn("private-", self.path.read_text())
        self.assertGreaterEqual(row["wait_ms"], 0)
        self.assertGreaterEqual(row["hold_ms"], 0)
        self.assertEqual(stat.S_IMODE(self.path.stat().st_mode), 0o600)
        self.assertEqual(statements, ["BEGIN IMMEDIATE", "INSERT INTO probe VALUES ('saved')", "COMMIT"])

    def test_standalone_nested_transaction_emits_only_after_outer_release(self):
        with storage.write_lock():
            with storage.write_lock():
                with storage.transaction(self.connection):
                    self.connection.execute("INSERT INTO probe VALUES ('nested')")
            self.assertFalse(self.path.exists())
        self.assertEqual(len(self.rows()), 1)
        self.assertEqual(self.connection.execute("SELECT value FROM probe").fetchall(), [("nested",)])

    def test_write_failure_cannot_rollback_commit_or_replace_original_error(self):
        with patch.object(storage.os, "write", side_effect=OSError("disk unavailable")):
            with storage.transaction(self.connection):
                self.connection.execute("INSERT INTO probe VALUES ('saved')")
            with self.assertRaisesRegex(ValueError, "original failure"):
                with storage.transaction(self.connection):
                    self.connection.execute("INSERT INTO probe VALUES ('rolled back')")
                    raise ValueError("original failure")
        self.assertEqual(self.connection.execute("SELECT value FROM probe").fetchall(), [("saved",)])
        self.assertFalse(self.connection.in_transaction)
        with storage._SQLITE_WRITE_TRANSACTION_LOCK._condition:
            self.assertIsNone(storage._SQLITE_WRITE_TRANSACTION_LOCK._owner)

    def test_invalid_path_and_symlink_are_ignored_without_business_error(self):
        for target in ("relative.jsonl", str(self.root / "absent" / "file.jsonl"), str(self.root / "database.sqlite3")):
            with self.subTest(target=target), patch.dict(os.environ, {"DCAR_SQLITE_TIMING_FILE": target}):
                with storage.transaction(self.connection):
                    self.connection.execute("INSERT INTO probe VALUES ('saved')")
        other = self.root / "other.jsonl"
        other.write_text("untouched")
        self.path.symlink_to(other)
        with storage.transaction(self.connection):
            self.connection.execute("INSERT INTO probe VALUES ('saved')")
        self.assertEqual(other.read_text(), "untouched")
        self.assertEqual(self.connection.execute("SELECT count(*) FROM probe").fetchone()[0], 4)

    def test_permission_and_descriptor_failures_are_best_effort(self):
        self.root.chmod(0o755)
        with storage.transaction(self.connection):
            self.connection.execute("INSERT INTO probe VALUES ('saved')")
        self.assertFalse(self.path.exists())
        self.root.chmod(0o700)
        with patch.object(storage.os, "open", side_effect=PermissionError("read only")):
            with storage.transaction(self.connection):
                self.connection.execute("INSERT INTO probe VALUES ('saved')")
        self.assertEqual(self.connection.execute("SELECT count(*) FROM probe").fetchone()[0], 2)

    def test_slow_timing_write_does_not_hold_up_the_next_writer(self):
        entered, release, acquired = threading.Event(), threading.Event(), threading.Event()
        errors = []
        original = storage._emit_write_timing
        def emit(**kwargs):
            entered.set()
            if not release.wait(2):
                raise AssertionError("timing write not released")
            original(**kwargs)
        def first():
            try:
                with storage.write_lock():
                    pass
            except BaseException as error:
                errors.append(error)
        def next_writer():
            try:
                with storage._SQLITE_WRITE_TRANSACTION_LOCK:
                    acquired.set()
            except BaseException as error:
                errors.append(error)
        with patch.object(storage, "_emit_write_timing", side_effect=emit):
            thread = threading.Thread(target=first)
            other = threading.Thread(target=next_writer)
            thread.start()
            try:
                self.assertTrue(entered.wait(2))
                other.start()
                self.assertTrue(acquired.wait(2), "timing I/O blocked the next writer")
            finally:
                release.set()
                thread.join(2)
                if other.ident is not None:
                    other.join(2)
        self.assertEqual(errors, [])
        self.assertFalse(thread.is_alive())
        self.assertFalse(other.is_alive())

    def test_actual_wait_and_standalone_hold_are_recorded(self):
        errors = []
        def waiter():
            try:
                with storage.write_lock():
                    time.sleep(.01)
            except BaseException as error:
                errors.append(error)
        # Direct lock is only the test blocker; the observed production wrapper
        # queues behind it and must include that delay in its timing record.
        with storage._SQLITE_WRITE_TRANSACTION_LOCK:
            thread = threading.Thread(target=waiter)
            thread.start()
            deadline = time.monotonic() + 2
            while time.monotonic() < deadline:
                with storage._SQLITE_WRITE_TRANSACTION_LOCK._condition:
                    if storage._SQLITE_WRITE_TRANSACTION_LOCK._normal_waiters:
                        break
                time.sleep(.001)
            else:
                self.fail("writer did not queue")
            time.sleep(.01)
        thread.join(2)
        self.assertEqual(errors, [])
        self.assertFalse(thread.is_alive())
        self.assertGreaterEqual(self.rows()[0]["wait_ms"], 10)
        self.assertGreaterEqual(self.rows()[0]["hold_ms"], 10)
