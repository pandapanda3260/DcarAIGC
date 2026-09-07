from __future__ import annotations

import os
import sqlite3
import tempfile
import unittest
from pathlib import Path
from unittest.mock import patch

from tests.test_v8_runtime_database import InstalledRuntimeFixture
from v8 import runtime_database as runtime


class DiagnosticWriterLeaseTest(unittest.TestCase):
    def setUp(self):
        temporary = tempfile.TemporaryDirectory()
        self.addCleanup(temporary.cleanup)
        self.root = Path(temporary.name).resolve()
        self.fixture = InstalledRuntimeFixture(self.root)
        self.fixture.database.write_bytes(b"")
        self.connection = sqlite3.connect(self.fixture.database)
        self.connection.execute("CREATE TABLE fixture(value INTEGER)")
        self.connection.commit()
        self.addCleanup(self.connection.close)
        self.installed = runtime.load_installed_writer_contract(home=self.fixture.home)

    def _access(self, mode="writer"):
        return runtime.resolve_installed_database_access(
            mode, database=self.fixture.database, project_root=self.fixture.project,
            environ=self.fixture.environment, installed=self.installed,
        )

    def test_only_current_writer_context_proves_ownership_and_cleanup(self):
        with self.assertRaises(runtime.RuntimeDatabaseError):
            runtime.require_current_process_writer_lock(self.connection)
        with runtime.acquire_writer_lock(self._access()) as lease:
            before = self.connection.total_changes
            value = runtime.require_current_process_writer_lock(self.connection)
            self.assertEqual(self.connection.total_changes, before)
            self.assertEqual(value["pid"], os.getpid())
            self.assertEqual(value["lock_inode"], lease.identity.inode)
            self.assertEqual(value["database_inode"], self.fixture.database.stat().st_ino)
            with patch.object(runtime.os, "getpid", return_value=os.getpid() + 1):
                with self.assertRaises(runtime.RuntimeDatabaseError):
                    runtime.require_current_process_writer_lock(self.connection)
        with self.assertRaises(runtime.RuntimeDatabaseError):
            runtime.require_current_process_writer_lock(self.connection)

    def test_formal_mutation_and_other_database_are_not_writer_authority(self):
        with runtime.acquire_writer_lock(self._access("formal_mutation")):
            with self.assertRaises(runtime.RuntimeDatabaseError):
                runtime.require_current_process_writer_lock(self.connection)
        other = sqlite3.connect(self.root / "other.sqlite3")
        self.addCleanup(other.close)
        with runtime.acquire_writer_lock(self._access()):
            with self.assertRaises(runtime.RuntimeDatabaseError):
                runtime.require_current_process_writer_lock(other)

    def test_replaced_lock_path_invalidates_live_descriptor(self):
        with runtime.acquire_writer_lock(self._access()):
            runtime.require_current_process_writer_lock(self.connection)
            self.fixture.writer_lock.rename(self.root / "old-writer.lock")
            self.fixture.writer_lock.write_bytes(b"")
            self.fixture.writer_lock.chmod(0o600)
            with self.assertRaisesRegex(runtime.RuntimeDatabaseError, "identity changed"):
                runtime.require_current_process_writer_lock(self.connection)
        self.assertEqual(runtime._PROCESS_WRITER_LEASES, {})


if __name__ == "__main__":
    unittest.main()
