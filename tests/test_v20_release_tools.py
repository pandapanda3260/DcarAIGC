from __future__ import annotations

import json
import os
import sqlite3
import shutil
import subprocess
import tempfile
import unittest
from pathlib import Path
from typing import Any
from unittest.mock import patch

from tests import test_dual_profile_offline_tools as offline_fixture
from tests import test_seal_r0_receipts as seal_fixture
from tests import test_server_snapshot_deployment as snapshot_fixture
from v8 import capture_code_successor
from v8.storage import connect, initialize_database
from v8.schema_v20 import migrate
from v8.profile_activations import TIKHUB_PROFILE, activation_at, append_activation
from v8.system_roster import seal_system_members
from tests.v9_report_fixture import activate_v9_report_fixture
from tests.capture_v25_fixture import local_storage_receipt

import v20_release_contract as release

migrator = offline_fixture.migrator
installer = offline_fixture.installer
restorer = offline_fixture.restorer
safety = offline_fixture.shared_safety
REAL_CODE_IDENTITY = safety.code_identity


class IntegratedOfflineToolsTest(unittest.TestCase):
    _scenario = offline_fixture.DualProfileOfflineToolsTest._scenario
    _install = offline_fixture.DualProfileOfflineToolsTest._install
    _restore = offline_fixture.DualProfileOfflineToolsTest._restore
    _assert_version = offline_fixture.DualProfileOfflineToolsTest._assert_version

    def setUp(self) -> None:
        self.enterContext(patch.object(
            capture_code_successor, "_installed_build_path",
            side_effect=lambda: Path(os.environ["DCAR_LOADED_BUILD_RECEIPT"])
            if os.environ.get("DCAR_LOADED_BUILD_RECEIPT") else None,
        ))

    def _build_schema18(self, path: Path) -> None:
        # Reuse only the safe temporary-directory/lock layout of the old tests;
        # the source below is a real schema19 database, not a PRAGMA relabel.
        with connect(path) as connection:
            initialize_database(connection)
            connection.execute(
                "INSERT INTO content_items(id,link_id,platform,canonical_url,title,imported_at,created_at,updated_at) "
                "VALUES(10,'V20001','douyin','https://example.test/v20','retained-title',?,?,?)",
                (offline_fixture.STAMP,) * 3,
            )
            connection.commit()
            connection.execute("PRAGMA wal_checkpoint(TRUNCATE)")
            connection.execute("PRAGMA journal_mode=DELETE")
        path.chmod(0o600)

    def _backup(self, layout: dict[str, Path]) -> dict[str, Any]:
        return migrator.prepare_verified_backup(
            source_database=layout["formal"], backup=layout["verified_backup"],
            expected_source_sha256=offline_fixture._sha256(layout["formal"]), from_version=19,
            freeze_lock=layout["freeze"], migration_lock=layout["migration_lock"],
            receipt=layout["backup_receipt"], isolated=True, holder_checker=lambda _: [],
        )

    def _build(self, layout: dict[str, Path]) -> dict[str, Any]:
        return migrator.build_migration_candidate(
            source_database=layout["formal"], candidate=layout["candidate"],
            expected_source_sha256=offline_fixture._sha256(layout["formal"]), from_version=19, to_version=20,
            freeze_lock=layout["freeze"], migration_lock=layout["migration_lock"],
            backup_receipt=layout["backup_receipt"], receipt=layout["migration_receipt"],
            isolated=True, holder_checker=lambda _: [], legacy_project_root=layout["project"],
        )

    def test_true19_backup_migrate_install20_restore19_preserves_source(self) -> None:
        with self._scenario() as layout:
            before = offline_fixture._sha256(layout["formal"])
            backup = self._backup(layout)
            candidate = self._build(layout)
            self.assertEqual(backup["schema_version"], "dcar-v20-offline-backup-v1")
            self.assertEqual(candidate["schema_version"], "dcar-v20-offline-migration-v1")
            self.assertEqual(candidate["lineage"]["contract_version"], "schema20-source-lineage-v1")
            self.assertEqual(offline_fixture._sha256(layout["formal"]), before)
            installed = self._install(layout)
            self.assertEqual(installed["schema_version"], "dcar-writer-database-v20-install-v1")
            self._assert_version(layout["formal"], 20)
            restored = self._restore(layout)
            self.assertEqual(restored["schema_version"], "dcar-writer-database-v19-restore-v1")
            self._assert_version(layout["formal"], 19)
            # SQLite backup preserves all rows but may change file-header bytes;
            # restore is exactly the independently verified backup, not a cp.
            self.assertEqual(offline_fixture._sha256(layout["formal"]), backup["backup_sha256"])
            with sqlite3.connect(layout["formal"]) as connection:
                self.assertEqual(connection.execute("SELECT title FROM content_items WHERE id=10").fetchone()[0], "retained-title")

    def test_candidate_historical_tamper_is_rejected_before_install(self) -> None:
        with self._scenario() as layout:
            self._backup(layout)
            self._build(layout)
            with sqlite3.connect(layout["candidate"]) as connection:
                connection.execute("UPDATE content_items SET title='tampered' WHERE id=10")
            with safety.using_contract(safety.INTEGRATED_V19_V20):
                with self.assertRaises(installer.CandidateInstallError):
                    installer._validate_source_candidate_lineage(layout["formal"], layout["candidate"])
            self._assert_version(layout["formal"], 19)

    def test_preinstall_seal_validator_and_installed_candidate_snapshot_have_no_hash_cycle(self) -> None:
        sealer = seal_fixture.receipts
        with self._scenario() as layout, patch.object(safety, "code_identity", REAL_CODE_IDENTITY):
            layout = {key: value.resolve() for key, value in layout.items()}
            project = layout["project"]
            # Real Git identity and real tool receipts; only the external writer
            # lease is isolated by the existing offline fixture.
            subprocess.run(["git", "init", "-b", "main", str(project)], check=True, capture_output=True)
            (project / ".gitignore").write_text("app/data/\ndata/\nruntime/\nreports/\n")
            subprocess.run(["git", "-C", str(project), "add", ".gitignore"], check=True, capture_output=True)
            subprocess.run(["git", "-C", str(project), "-c", "user.name=Test", "-c", "user.email=test@example.invalid",
                            "commit", "--allow-empty", "-m", "release fixture"], check=True, capture_output=True)
            cache = project / "data/cache"
            cache.mkdir(parents=True)
            for name in (".comment_hash_salt", ".platform_user_salt"):
                (cache / name).write_bytes(b"t" * 32)
                (cache / name).chmod(0o600)
            activate_v9_report_fixture(layout["formal"], [])
            with connect(layout["formal"]) as connection:
                roster = seal_system_members(connection, [{"platform": "douyin", "uid": "1234567890", "profile_ref": "https://www.douyin.com/user/MS4w.fixture"}], raw_root=cache / "roster", actor="fixture", reason="source",
                                             sealed_at=offline_fixture.STAMP)
                snapshot_id = roster["snapshot_id"]
                snapshot = connection.execute("SELECT members_sha256 FROM account_roster_snapshots WHERE id=?", (snapshot_id,)).fetchone()
                append_activation(connection, profile_id=TIKHUB_PROFILE, roster_snapshot_id=snapshot_id,
                                  roster_members_sha256=snapshot[0], effective_at=offline_fixture.STAMP,
                                  build_receipt_sha256="a" * 64, actor="fixture", created_at=offline_fixture.STAMP)
                connection.commit()
                connection.execute("PRAGMA wal_checkpoint(TRUNCATE)")
                connection.execute("PRAGMA journal_mode=DELETE")
            self._backup(layout)
            self._build(layout)
            before = offline_fixture._sha256(layout["candidate"])
            preinstall = release.validate_migration_candidate(source=layout["formal"], candidate=layout["candidate"],
                receipt=layout["migration_receipt"], project_root=project)
            self.assertEqual(preinstall["status"], "candidate")
            self.assertFalse(preinstall["deployment_eligible"])
            self.assertEqual(offline_fixture._sha256(layout["candidate"]), before)
            self._install(layout)

            evidence_root = layout["migration_receipt"].parent
            git_record = sealer._git_record(project, allow_working_tree=True)
            source_archive = evidence_root / "source.tar"
            sealer._write_source_archive(project, source_archive, git_record)
            logs = {}
            for name in sealer.REQUIRED_TEST_RESULTS:
                path = evidence_root / f"{name}.log"
                path.write_text(f"fixture {name}\nDCAR_TEST_RESULT name={name} exit=0\n")
                path.chmod(0o600)
                logs[name] = path
            checks = evidence_root / "checks.json"
            checks.write_text(json.dumps(sealer._envelope(sealer.TEST_RESULTS_CONTRACT,
                sealer._test_results_payload(project, paths=logs, git_record=git_record))))
            checks.chmod(0o600)
            def ref(path: Path) -> dict[str, str]:
                return {"path": str(path), "sha256": offline_fixture._sha256(path), "result": "passed"}
            with connect(layout["formal"]) as connection:
                active = activation_at(connection, "2026-09-06T00:00:00Z")
                payload = {"contract_version": release.CONTRACT_VERSION, "schema_version": 20,
                    "schema_migration": release.SCHEMA_MIGRATIONS[20], "coverage_complete": False,
                    "storage_policy": local_storage_receipt(evidence_root / "local-raw", daily_stored_p95=0),
                    "bindings": {**{key: active[key] for key in ("activation_id", "profile_id", "roster_snapshot_id",
                        "roster_members_sha256", "activation_sha256")}, "build_sha256": "a" * 64,
                        "runtime_sha256": "b" * 64, "config_sha256": "c" * 64},
                    "evidence": {"source_archive": ref(source_archive), "migration": ref(layout["migration_receipt"]),
                        "rollback": ref(layout["backup_receipt"]), "full_checks": ref(checks),
                        "install": ref(layout["install_receipt"])}}
                recorded = "2026-09-06T00:00:00Z"
                checksum = release.deployment_digest(deployment_id="candidate-fixture", status="candidate", payload=payload, recorded_at=recorded)
                connection.execute("INSERT INTO deployment_readiness_receipts(deployment_id,status,payload_json,recorded_at,receipt_sha256) "
                                   "VALUES('candidate-fixture','candidate',?,?,?)", (json.dumps(payload), recorded, checksum))
                connection.commit()
                checked = release.validate_deployment_receipt(connection, project_root=project)
                self.assertFalse(checked["deployment_eligible"])
                self.assertIn("local_storage_capacity_forecast", checked["unmet_evidence"])
                with self.assertRaisesRegex(release.ReleaseContractError, "not production acceptance"):
                    release.validate_deployment_receipt(connection, require_accepted=True, project_root=project)
            manifest = snapshot_fixture.builder.build_snapshot(project_root=project, database=layout["formal"],
                output=evidence_root / "snapshot20", expected_user_version=20, deployment_id="candidate-fixture")
            self.assertEqual(manifest["runtime_identity"]["database_schema_version"], 20)
            self.assertEqual(manifest["deployment_readiness"]["status"], "candidate")
            self.assertFalse(manifest["deployment_readiness"]["coverage_complete"])
            self.assertEqual(manifest["deployment_readiness"]["contract_version"], release.CONTRACT_VERSION)
            self.assertEqual(manifest["deployment_readiness"]["storage_policy"]["root"],
                             str(evidence_root / "local-raw"))
            self.assertEqual(manifest["deployment_readiness"]["storage_policy"]["policy"],
                             release.LOCAL_STORAGE_POLICY)

    def test_hardlinked_raw_is_copied_bound_and_tamper_prevents_install(self) -> None:
        with self._scenario() as layout:
            raw = layout["project"] / "raw.json"
            raw.write_bytes(b'{"data":{"value":1}}')
            os.link(raw, layout["project"] / "raw-second-link.json")
            with sqlite3.connect(layout["formal"]) as connection:
                connection.execute("INSERT INTO provider_raw_responses(content_id,provider,operation,local_path,sha256,byte_size,"
                                   "captured_at,source) VALUES(10,'tikhub','douyin_video_detail','raw.json',?,?,?,'live_applied')",
                                   (offline_fixture._sha256(raw), raw.stat().st_size, offline_fixture.STAMP))
            self._backup(layout)
            migration = self._build(layout)
            raw_manifest = migration["legacy_raw"]
            self.assertEqual(raw_manifest["copy_count"], 1)
            copy_path = Path(raw_manifest["copies"][0]["path"])
            self.assertEqual(copy_path.parent, layout["candidate"].with_name(layout["candidate"].name + ".legacy-blobs").resolve())
            self.assertEqual(copy_path.stat().st_nlink, 1)
            self.assertEqual(raw.stat().st_nlink, 2)
            copy_path.write_bytes(b"tampered")
            with self.assertRaises(installer.CandidateInstallError):
                self._install(layout)
            self._assert_version(layout["formal"], 19)


class ExplicitReleaseSchemaTest(unittest.TestCase):
    def test_real_local_disk_contract_accepted_without_independent_archive(self) -> None:
        with tempfile.TemporaryDirectory() as temporary:
            capacity = local_storage_receipt(Path(temporary).resolve() / "raw")
            checked = release.validate_storage_policy(capacity, require_forecast=True)
            self.assertEqual(checked["policy"], release.LOCAL_STORAGE_POLICY)
            self.assertEqual(checked["device"], Path(temporary).stat().st_dev)
            self.assertNotIn("mount", checked)
            self.assertNotIn("free_bytes", checked)
            self.assertTrue(checked["physical_io_verified"])
            with patch("v8.local_raw_retention.os.open", side_effect=AssertionError("read-only validation wrote a probe")):
                self.assertEqual(release.validate_storage_policy(capacity), checked)

    def test_local_storage_identity_capacity_and_forecast_fail_closed(self) -> None:
        with tempfile.TemporaryDirectory() as temporary:
            capacity = local_storage_receipt(Path(temporary).resolve() / "raw")
            for key in ("device", "inode", "required_free_bytes"):
                with self.subTest(key=key), self.assertRaises(release.ReleaseContractError):
                    release.validate_storage_policy({**capacity, key: capacity[key] + 1})
            with self.assertRaisesRegex(release.ReleaseContractError, "not ready"):
                release.validate_storage_policy({**capacity, "daily_stored_p95": 10**18})
            physical_only = local_storage_receipt(Path(temporary).resolve() / "raw", daily_stored_p95=0)
            self.assertFalse(release.validate_storage_policy(physical_only)["forecast_known"])
            with self.assertRaisesRegex(release.ReleaseContractError, "nonzero"):
                release.validate_storage_policy(physical_only, require_forecast=True)
            with self.assertRaisesRegex(release.ReleaseContractError, "explicit local"):
                release.validate_storage_policy({**capacity, "schema": "raw-archive-volume-v1"})
            with self.assertRaisesRegex(release.ReleaseContractError, "qualified I/O"):
                release.validate_storage_policy({**capacity, "physical_io_verified": False})

    def test_previously_signed_archive_contract_is_not_reinterpreted(self) -> None:
        with tempfile.TemporaryDirectory() as temporary, connect(Path(temporary) / "old.sqlite3") as connection:
            initialize_database(connection, target_version=20)
            payload = {"contract_version": "v25-bounded-deployment-readiness-v1",
                       "archive": {"schema": "raw-archive-volume-v1"}}
            recorded = "2026-09-06T00:00:00Z"
            checksum = release.deployment_digest(deployment_id="old-archive", status="accepted", payload=payload, recorded_at=recorded)
            connection.execute("INSERT INTO deployment_readiness_receipts(deployment_id,status,payload_json,recorded_at,receipt_sha256) "
                               "VALUES('old-archive','accepted',?,?,?)", (json.dumps(payload), recorded, checksum))
            with self.assertRaisesRegex(release.ReleaseContractError, "payload contract differs"):
                release.validate_deployment_receipt(connection)

    def test_storage_pressure_blocks_admission_but_not_owned_cleanup(self) -> None:
        with tempfile.TemporaryDirectory() as temporary:
            root = Path(temporary).resolve() / "raw"
            capacity = local_storage_receipt(root)
            disk = shutil.disk_usage(root)
            low_space = disk._replace(free=1, used=disk.total - 1)
            with patch("v8.local_raw_retention.shutil.disk_usage", return_value=low_space):
                with self.assertRaisesRegex(release.ReleaseContractError, "not ready"):
                    release.validate_storage_policy(capacity)
                self.assertEqual(release.validate_storage_policy(capacity, maintenance_only=True)["inode"],
                                 capacity["inode"])
                other = root.with_name("other")
                other.mkdir(mode=0o700)
                with self.assertRaisesRegex(release.ReleaseContractError, "identity or capacity"):
                    release.validate_storage_policy({**capacity, "root": str(other)}, maintenance_only=True)

    def test_sealer_explicit_pairs_do_not_change_legacy_default(self) -> None:
        sealer = seal_fixture.receipts
        self.assertEqual(sealer._schema_contract(19)["transition"], "19-to-19")
        self.assertEqual(sealer._schema_contract(19, 20)["target_migration"], release.SCHEMA_MIGRATIONS[20])
        self.assertEqual(sealer._schema_contract(20, 20)["operation"], "code_update")
        for source, target in ((18, 20), (20, 19), (20, 21)):
            with self.assertRaises(sealer.R0ReceiptError):
                sealer._schema_contract(source, target)

    def test_true20_snapshot_identity_supported_but_unreceipted_snapshot_refused(self) -> None:
        fixture = snapshot_fixture.ServerSnapshotDeploymentTest(methodName="runTest")
        fixture.setUp()
        self.addCleanup(fixture.tearDown)
        with connect(fixture.database) as connection:
            migrate(connection, legacy_project_root=fixture.project)
        identity = snapshot_fixture.builder._runtime_identity(fixture.database, expected_user_version=20)
        self.assertEqual(identity["database_schema_migration"], release.SCHEMA_MIGRATIONS[20])
        with self.assertRaisesRegex(snapshot_fixture.builder.SnapshotBuildError, "deployment receipt is missing"):
            snapshot_fixture.builder.build_snapshot(project_root=fixture.project, database=fixture.database,
                                                     output=fixture.bundle, expected_user_version=20)
        self.assertFalse(fixture.bundle.exists())

    def test_candidate_deployment_receipt_cannot_self_assert_acceptance(self) -> None:
        fixture = snapshot_fixture.ServerSnapshotDeploymentTest(methodName="runTest")
        fixture.setUp()
        self.addCleanup(fixture.tearDown)
        with connect(fixture.database) as connection:
            migrate(connection)
            payload = {"contract_version": release.CONTRACT_VERSION, "schema_version": 20,
                       "schema_migration": release.SCHEMA_MIGRATIONS[20], "deployment_eligible": True,
                       "coverage_complete": True}
            recorded = "2026-09-06T00:00:00Z"
            checksum = release.deployment_digest(deployment_id="unproven", status="candidate", payload=payload, recorded_at=recorded)
            connection.execute("INSERT INTO deployment_readiness_receipts(deployment_id,status,payload_json,recorded_at,receipt_sha256) "
                               "VALUES('unproven','candidate',?,?,?)", (json.dumps(payload), recorded, checksum))
            connection.commit()
            with self.assertRaises(release.ReleaseContractError):
                release.validate_deployment_receipt(connection, require_accepted=True, project_root=fixture.project)


if __name__ == "__main__":
    unittest.main()
