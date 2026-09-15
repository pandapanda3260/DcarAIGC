"""Exercise the schema22 builder boundary with real migrated SQLite fixtures."""
from __future__ import annotations

import unittest
from unittest.mock import patch

from tests import test_account_classification_snapshot_deployment as fixtures
from v8 import account_cleanup_snapshot, schema_v22, storage

builder = fixtures.builder


class IntakeSnapshotBuilderTest(unittest.TestCase):
    def setUp(self):
        self.fixture = fixtures.ClassificationSnapshotDeploymentTest('test_actual21_identity_structure_and_explicit_pair')
        self.fixture.setUp()
        self.addCleanup(self.fixture.doCleanups)
        with storage.connect(self.fixture.database) as connection:
            schema_v22.migrate(connection)

    def build(self, **options):
        return builder.build_snapshot(project_root=self.fixture.root, database=self.fixture.database,
            legacy_database=None, output=self.fixture.root / 'bundle', expected_user_version=22, **options)

    def test_real22_identity_requires_explicit_version_and_exact_migration(self):
        identity = builder._runtime_identity(self.fixture.database, expected_user_version=22)
        self.assertEqual(identity['database_schema_migration'], 'unified-account-intake-v1')
        with self.assertRaises(builder.SnapshotBuildError):
            builder._runtime_identity(self.fixture.database, expected_user_version=21)
        with storage.connect(self.fixture.database) as connection:
            connection.execute("UPDATE schema_migrations SET name='wrong-intake-migration' WHERE version=22")
            connection.commit()
        with self.assertRaisesRegex(builder.SnapshotBuildError, 'database_schema_migration'):
            builder._runtime_identity(self.fixture.database, expected_user_version=22)

    def test_unproved22_cannot_emit_a_bundle(self):
        with self.assertRaisesRegex(builder.SnapshotBuildError, 'inherited cleanup'):
            self.build()
        self.assertFalse((self.fixture.root / 'bundle').exists())

    def test_cleanup_validation_runs_on_frozen22_and_both_proofs_are_required(self):
        proof = {'schema_version': 22, 'schema_migration': 'unified-account-intake-v1',
                 'account_classification_migration': {}, 'account_intake_migration': {}, 'status': 'candidate'}
        for missing in ('account_classification_migration', 'account_intake_migration'):
            partial = {key: value for key, value in proof.items() if key != missing}
            def validate(connection, **kwargs):
                self.assertEqual(connection.execute('PRAGMA user_version').fetchone()[0], 22)
                self.assertNotEqual(connection.execute('PRAGMA database_list').fetchone()[2], str(self.fixture.database))
                return partial
            with self.subTest(missing=missing), patch.object(account_cleanup_snapshot, 'is_cleanup', return_value=True), \
                    patch.object(account_cleanup_snapshot, 'validate', side_effect=validate), \
                    self.assertRaisesRegex(builder.SnapshotBuildError, 'migration proof is missing'):
                self.build()
            self.assertFalse((self.fixture.root / 'bundle').exists())

    def test_candidate_proof_does_not_satisfy_acceptance_gate(self):
        proof = {'schema_version': 22, 'schema_migration': 'unified-account-intake-v1',
                 'account_classification_migration': {}, 'account_intake_migration': {}, 'status': 'candidate'}
        with patch.object(account_cleanup_snapshot, 'is_cleanup', return_value=True), \
                patch.object(account_cleanup_snapshot, 'validate', return_value=proof), \
                self.assertRaisesRegex(builder.SnapshotBuildError, 'has not been accepted'):
            self.build(require_accepted_deployment=True)
        self.assertFalse((self.fixture.root / 'bundle').exists())


if __name__ == '__main__':
    unittest.main()
