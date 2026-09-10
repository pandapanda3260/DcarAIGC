"""Schema21 receiver/publisher contracts using only temporary files and SQLite."""
from __future__ import annotations

import json
import shutil
import sqlite3
import tempfile
import unittest
from dataclasses import replace
from pathlib import Path
from unittest.mock import patch

from tests import test_server_snapshot_deployment as fixtures
from tests.test_macos_snapshot_publisher import FakeRunner, publisher
from v8 import account_cleanup_snapshot, schema_v20, schema_v21, storage

ROOT = Path(__file__).resolve().parents[1]
installer, builder = fixtures.installer, fixtures.builder


class ClassificationSnapshotDeploymentTest(unittest.TestCase):
    def setUp(self):
        temporary = tempfile.TemporaryDirectory()
        self.addCleanup(temporary.cleanup)
        self.root = Path(temporary.name).resolve()
        self.database = self.root / "candidate.sqlite3"
        fixtures._create_main_database(self.database, self.root)
        with storage.connect(self.database) as connection:
            schema_v20.migrate(connection, legacy_project_root=self.root)
        self.parent = self.root / "parent20.sqlite3"
        with storage.connect(self.database) as source, sqlite3.connect(self.parent) as destination:
            source.backup(destination)
        with storage.connect(self.database) as connection:
            schema_v21.migrate(connection)
        self.identity = builder._runtime_identity(self.database, expected_user_version=21)
        self.config = publisher.PublishConfig(
            ssh_alias="fixture", remote_project_root="/var/www/dcar-aigc/current",
            remote_state_root="/var/lib/dcar-aigc", remote_python="/var/www/dcar-aigc/current/.venv/bin/python",
            snapshot_root=self.root / "snapshots", minimum_remote_free_bytes=1024,
            expected_user_version=21, maximum_content_lag_days=1,
        )

    def probe(self):
        value = FakeRunner(self.identity).probe()
        value["health"]["database_state"]["user_version"] = 21
        value["installer_schema_support"] = {"versions": [17, 18, 19, 20, 21],
            "transitions": [[17, 18], [18, 19], [19, 20], [20, 21]]}
        value["schema_transition"] = {"schema": publisher.CLASSIFICATION_TRANSITION_SCHEMA,
            "from_schema": 20, "to_schema": 21, "status": "succeeded", "completed_at": "2026-09-08T03:00:00Z"}
        return value

    def test_actual21_identity_structure_and_explicit_pair(self):
        self.assertEqual(self.identity["database_schema_version"], 21)
        self.assertEqual(self.identity["database_schema_migration"], "account-classification-v1")
        self.assertEqual(self.identity["report_version"], "dcar-content-operations-report-v8.9")
        installer._strict_schema(self.database, 21)
        self.assertEqual(installer._transition_contract(20, 21), (
            "dcar-schema20-to21-server-transition-v1", "dcar-schema20-to21-release-seal-v1"))
        self.assertTrue({(17, 18), (18, 19), (19, 20)} <= installer.SUPPORTED_SCHEMA_TRANSITIONS)
        with self.assertRaises(installer.SnapshotInstallError):
            installer._transition_contract(19, 21)
        with self.assertRaises(builder.SnapshotBuildError):
            builder._runtime_identity(self.database, expected_user_version=20)

    def test_relabelled20_is_not_a_schema21_database(self):
        with sqlite3.connect(self.parent) as connection:
            connection.execute("PRAGMA user_version=21")
            connection.execute("INSERT INTO schema_migrations VALUES(21,'account-classification-v1','2026-09-08T03:00:00Z')")
        with self.assertRaisesRegex(installer.SnapshotInstallError, "exact sealed schema structure"):
            installer._strict_schema(self.parent, 21)

    def test_frozen21_release_requires_actual_migration_and_publisher_contract(self):
        installer._verify_release_contract(ROOT, 21)
        release = self.root / "release"
        names = ["src/dcar_eval/v8/storage.py", "src/dcar_eval/v8/contracts.py", "src/dcar_eval/v8/schema_v21.py",
            "deploy/server/install_snapshot.py", "deploy/macos/publish_snapshot.py", "scripts/build_server_snapshot.py"]
        for name in names:
            destination = release / name
            destination.parent.mkdir(parents=True, exist_ok=True)
            shutil.copy2(ROOT / name, destination)
        path = release / "deploy/macos/publish_snapshot.py"
        path.write_text(path.read_text().replace('CLASSIFICATION_SCHEMA_MIGRATION = "account-classification-v1"',
            'CLASSIFICATION_SCHEMA_MIGRATION = "integrated-video-capture-v25"'))
        with self.assertRaisesRegex(installer.SnapshotInstallError, "incompatible"):
            installer._verify_release_contract(release, 21)
        shutil.copy2(ROOT / "deploy/macos/publish_snapshot.py", path)
        path = release / "src/dcar_eval/v8/schema_v21.py"
        path.write_text(path.read_text().replace("def validate_lineage(", "def unsupported_lineage("))
        with self.assertRaisesRegex(installer.SnapshotInstallError, "complete schema21 migration contract"):
            installer._verify_release_contract(release, 21)

    def test_schema21_without_cleanup_proof_cannot_publish_or_build_snapshot(self):
        with storage.connect(self.database) as connection:
            with self.assertRaisesRegex(publisher.SnapshotPublishError, "inherited cleanup"):
                publisher._schema20_deployment(connection, project_root=self.root)
        with self.assertRaisesRegex(builder.SnapshotBuildError, "inherited cleanup"):
            builder.build_snapshot(project_root=self.root, database=self.database, legacy_database=None,
                                   output=self.root / "bundle", expected_user_version=21)
        self.assertFalse((self.root / "bundle").exists())

    def test_portable21_proof_cannot_reuse_unextended_schema20_receipt(self):
        proof = {"contract_version": account_cleanup_snapshot.CONTRACT, "deployment_id": "fixture"}
        manifest = {"deployment_readiness": proof}
        with patch.object(account_cleanup_snapshot, "validate", return_value=proof), \
             patch.object(installer, "_verify_schema20_activation", side_effect=AssertionError("unproved snapshot reached activation")):
            with self.assertRaisesRegex(installer.SnapshotInstallError, "portable deployment proof is invalid"):
                installer._verify_schema20_deployment(self.database, manifest, bundle=self.root)
        with self.assertRaisesRegex(installer.SnapshotInstallError, "portable deployment proof is invalid"):
            installer._verify_schema20_deployment(self.database,
                {"deployment_readiness": {"contract_version": "v25-bounded-deployment-readiness-v2-local-retention"}}, bundle=self.root)

    def test_remote21_requires_completed20_to21_pair_and_matching_health(self):
        publisher._validate_remote_probe(self.probe(), config=self.config)
        variants = []
        no_pair = self.probe(); no_pair["schema_transition"] = None; variants.append(no_pair)
        old_pair = self.probe(); old_pair["schema_transition"].update(schema=publisher.INTEGRATED_TRANSITION_SCHEMA,
            from_schema=19, to_schema=20); variants.append(old_pair)
        old_receiver = self.probe(); old_receiver["installer_schema_support"]["versions"].remove(21); variants.append(old_receiver)
        unpaired = self.probe(); unpaired["installer_schema_support"]["transitions"].remove([20, 21]); variants.append(unpaired)
        rollback = self.probe(); rollback["schema_transition"]["status"] = "rolled_back"; variants.append(rollback)
        old_health = self.probe(); old_health["health"]["database_state"]["user_version"] = 20; variants.append(old_health)
        for value in variants:
            with self.subTest(value=value["schema_transition"]), self.assertRaises(publisher.SnapshotPublishError):
                publisher._validate_remote_probe(value, config=self.config)

    def test_restored20_can_publish_only_against_schema20_health(self):
        identity = builder._runtime_identity(self.parent, expected_user_version=20)
        value = FakeRunner(identity).probe()
        value["health"]["database_state"]["user_version"] = 20
        value["installer_schema_support"] = self.probe()["installer_schema_support"]
        value["schema_transition"] = self.probe()["schema_transition"]
        value["schema_transition"]["status"] = "rolled_back"
        publisher._validate_remote_probe(value, config=replace(self.config, expected_user_version=20))
        value["schema_transition"]["status"] = "succeeded"
        with self.assertRaises(publisher.SnapshotPublishError):
            publisher._validate_remote_probe(value, config=replace(self.config, expected_user_version=20))

    def test_normal_remote_install_explicitly_passes_schema21(self):
        commands = []
        def runner(arguments, **kwargs):
            import subprocess
            commands.append(arguments)
            return subprocess.CompletedProcess(arguments, 0, stdout=json.dumps({"status": "verified", "snapshot_id": "20260908T030000Z-" + "a" * 12}), stderr="")
        # The subprocess boundary is replaced: no network or remote installation.
        publisher._remote_installer_operation(self.config, ["ssh", "fixture"], "verify", runner=runner)
        self.assertIn("--expected-schema 21", " ".join(commands[0]))


if __name__ == "__main__":
    unittest.main()
