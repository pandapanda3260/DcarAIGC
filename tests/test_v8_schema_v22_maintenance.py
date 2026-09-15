"""Real maintenance leases on temporary installed Writer fixtures only."""
from __future__ import annotations

import hashlib
from pathlib import Path
import sqlite3
import unittest
from unittest.mock import patch

from tests import test_account_classification_install as fixtures
from v8 import runtime_database, schema_v21, schema_v22


class Schema22MaintenanceTest(unittest.TestCase):
    def setUp(self):
        self.fixture = fixtures.AccountClassificationInstallTest("test_install_preserves_inode_and_binds_external_receipt_to_database")
        self.fixture.setUp()
        self.addCleanup(self.fixture.doCleanups)
        self.fixture.do_install()
        self.db = self.fixture.connection
        self.backup = self.fixture.root / "schema21-backup.sqlite3"
        with sqlite3.connect(self.backup) as backup:
            self.db.backup(backup)
        self.backup.chmod(0o600)
        self.sha = hashlib.sha256(self.backup.read_bytes()).hexdigest()

    def test_actual_formal_lease_and_sealed_backup_preserve_inode_and_lineage(self):
        before = self.fixture.db.stat()
        with runtime_database.hold_formal_mutation(self.fixture.db, project_root=self.fixture.project) as access:
            schema_v22.migrate(self.db, maintenance=schema_v22.MaintenanceContext(access, self.backup, self.sha))
        self.assertEqual(self.fixture.db.stat().st_ino, before.st_ino)
        self.assertEqual(self.fixture.db.stat().st_mode, before.st_mode)
        self.assertEqual(self.db.execute("PRAGMA user_version").fetchone()[0], 22)
        with self.fixture.installer.connect(self.backup, read_only=True) as source:
            self.assertTrue(schema_v22.validate_lineage(source, self.db)["retained_tables_verified"])

    def test_expired_lease_and_wrong_backup_hash_do_not_write(self):
        with runtime_database.hold_formal_mutation(self.fixture.db, project_root=self.fixture.project) as access:
            context = schema_v22.MaintenanceContext(access, self.backup, "0"*64)
            with self.assertRaisesRegex(ValueError, "backup checksum"):
                schema_v22.migrate(self.db, maintenance=context)
            valid = schema_v22.MaintenanceContext(access, self.backup, self.sha)
        with self.assertRaisesRegex(ValueError, "lease"):
            schema_v22.migrate(self.db, maintenance=valid)
        self.assertEqual(self.db.execute("PRAGMA user_version").fetchone()[0], 21)

    def test_failure_in_maintenance_transaction_rolls_back_schema_and_rows(self):
        before = schema_v22._table_digests(self.db)
        objects = schema_v22._objects(self.db)
        with runtime_database.hold_formal_mutation(self.fixture.db, project_root=self.fixture.project) as access:
            with patch.object(schema_v22, "_fetch_sql", side_effect=RuntimeError("injected maintenance failure")), self.assertRaises(RuntimeError):
                schema_v22.migrate(self.db, maintenance=schema_v22.MaintenanceContext(access, self.backup, self.sha))
        self.assertEqual(schema_v22._objects(self.db), objects)
        self.assertEqual(schema_v22._table_digests(self.db), before)
        self.assertEqual(schema_v21.migration_proof(self.db)["schema_version"], 21)


if __name__ == "__main__":
    unittest.main()
