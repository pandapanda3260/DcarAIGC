"""Standalone ``BEGIN IMMEDIATE`` writers must hold the process write lock.

``storage.transaction`` serializes in-process writes with a re-entrant lock.
A writer that opens its own ``BEGIN IMMEDIATE`` outside that helper used to
skip the lock, so a ``transaction()`` holder could block on the SQLite file
lock for ``busy_timeout`` while stalling every other in-process writer.
"""

from __future__ import annotations

import sqlite3
import tempfile
import threading
import unittest
from contextlib import contextmanager
from pathlib import Path

from v8 import account_states, operations, raw_archive, storage
from v8.storage import connect, initialize_database


class WriteLockTest(unittest.TestCase):
    def setUp(self):
        self.temp = tempfile.TemporaryDirectory()
        self.db = Path(self.temp.name).resolve() / "lock.sqlite3"
        with connect(self.db) as connection:
            initialize_database(connection)

    def tearDown(self):
        self.temp.cleanup()

    def _assert_lock_held_inside(self, enter_standalone_writer):
        """Run the writer on a thread; from here the lock must look taken."""

        inside = threading.Event()
        release = threading.Event()
        failures: list[BaseException] = []

        def worker():
            try:
                with enter_standalone_writer():
                    inside.set()
                    self.assertTrue(release.wait(5), "test did not release the writer")
            except BaseException as error:  # pragma: no cover - surfaced below
                failures.append(error)
                inside.set()

        thread = threading.Thread(target=worker)
        thread.start()
        try:
            self.assertTrue(inside.wait(5), "writer never entered its transaction")
            self.assertFalse(failures, failures)
            # Another thread cannot take the process write lock while the
            # standalone writer is inside BEGIN..COMMIT.
            self.assertFalse(storage._SQLITE_WRITE_TRANSACTION_LOCK.acquire(blocking=False))
        finally:
            release.set()
            thread.join(5)
        self.assertFalse(thread.is_alive())
        self.assertFalse(failures, failures)
        self.assertTrue(storage._SQLITE_WRITE_TRANSACTION_LOCK.acquire(blocking=False))
        storage._SQLITE_WRITE_TRANSACTION_LOCK.release()

    def test_content_write_transaction_holds_process_lock(self):
        @contextmanager
        def writer():
            with operations._content_write_transaction(self.db) as connection:
                self.assertTrue(connection.in_transaction)
                yield

        self._assert_lock_held_inside(writer)

    def test_atomic_helper_holds_process_lock_when_standalone(self):
        @contextmanager
        def writer():
            with connect(self.db) as connection:
                with account_states._atomic(connection):
                    self.assertTrue(connection.in_transaction)
                    yield

        self._assert_lock_held_inside(writer)

    def test_atomic_helper_nested_inside_transaction_keeps_savepoint_semantics(self):
        with connect(self.db) as connection, storage.transaction(connection):
            with self.assertRaises(RuntimeError):
                with account_states._atomic(connection):
                    connection.execute("CREATE TABLE savepoint_probe(id INTEGER)")
                    raise RuntimeError("roll back to savepoint")
            # The outer transaction survives the inner rollback.
            self.assertTrue(connection.in_transaction)
            with self.assertRaises(sqlite3.OperationalError):
                connection.execute("SELECT * FROM savepoint_probe")

    def test_raw_archive_transaction_holds_process_lock(self):
        @contextmanager
        def writer():
            with connect(self.db) as connection:
                with raw_archive._transaction(connection):
                    self.assertTrue(connection.in_transaction)
                    yield

        self._assert_lock_held_inside(writer)


if __name__ == "__main__":
    unittest.main()
