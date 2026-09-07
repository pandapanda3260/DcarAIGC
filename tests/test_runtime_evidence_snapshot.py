"""Formal receipt mirrors stay byte-identical across a real read-only replica."""
from __future__ import annotations

import hashlib
import json
import os
import shutil
import sqlite3
import unittest
from pathlib import Path
from unittest.mock import patch

from tests import test_server_snapshot_deployment as deployment
from v8 import artifact_paths, raw_evidence, runtime_receipts, transport_receipts
from v8.storage import connect, transaction


class RuntimeEvidenceSnapshotTest(unittest.TestCase):
    def setUp(self) -> None:
        self.fixture = deployment.ServerSnapshotDeploymentTest()
        self.fixture.setUp()
        self.addCleanup(self.fixture.tearDown)
        self.project = self.fixture.project
        self.state = self.fixture.root / "home/Library/Application Support/DcarAIGC"
        self.evidence = self.state / "evidence"
        self.evidence.mkdir(mode=0o700, parents=True)
        self.aliases: dict = {}
        self.patcher = patch.object(deployment.builder, "FORMAL_STATE_ROOT", self.state)
        self.patcher.start()
        self.addCleanup(self.patcher.stop)
        self.addCleanup(artifact_paths._context.cache_clear)

    def ordinary(self) -> dict:
        return runtime_receipts._write_evidence(
            self.fixture.database, "profile-day-coverage-v3",
            {"contract_version": "profile-day-coverage-evidence-v3", "coverage": {"complete": False}},
            evidence_root=self.evidence,
        )

    def mirror(self, reference: dict) -> str:
        return deployment.builder._declared_project_reference(
            reference["path"], reference["sha256"], reference["byte_size"],
            project_root=self.project, aliases=self.aliases,
        )

    def add_reference(self, reference: dict) -> None:
        with connect(self.fixture.database) as connection, transaction(connection):
            connection.execute("INSERT INTO scheduler_runs(job_id,scheduled_for,status,started_at,details_json) "
                               "VALUES('external-evidence-fixture','fixture','succeeded','2026-09-06T05:00:00Z',?)",
                               (json.dumps({"evidence": reference}),))

    def test_content_addressed_mirror_preserves_signed_database_and_installs(self) -> None:
        reference = self.ordinary()
        self.add_reference(reference)
        original = Path(reference["path"]).read_bytes()
        before = self.fixture.database.read_bytes()
        manifest = self.fixture.build_bundle()
        self.assertEqual(self.fixture.database.read_bytes(), before)
        alias = artifact_paths.runtime_evidence_aliases(manifest)[reference["path"]]
        self.assertEqual((self.project / alias["project_path"]).read_bytes(), original)
        self.assertEqual(Path(reference["path"]).read_bytes(), original)
        with sqlite3.connect(self.fixture.bundle / "databases/dcar_insight.sqlite3") as connection:
            value = json.loads(connection.execute("SELECT details_json FROM scheduler_runs "
                                                  "WHERE job_id='external-evidence-fixture'").fetchone()[0])
            self.assertEqual(value["evidence"], reference)
        config = self.fixture.server_config()
        self.fixture.stage_artifacts(manifest, config)
        self.assertEqual(deployment.installer.verify_bundle(self.fixture.bundle, config=config)["snapshot_id"],
                         manifest["snapshot_id"])

    def test_transport_receipt_reads_from_replica_without_writer_path(self) -> None:
        with connect(self.fixture.database) as connection, transaction(connection):
            original = transport_receipts.append_transport_receipt(
                connection, kind="campaign_terminal", identity_key="snapshot-readonly:1",
                payload={"status": "partial"}, at="2026-09-06T05:00:00Z",
                mirror_root=self.state / "data/current-hold-control",
            )
        manifest = self.fixture.build_bundle()
        config = self.fixture.server_config()
        self.fixture.stage_artifacts(manifest, config)
        deployment.installer.verify_bundle(self.fixture.bundle, config=config)
        replica = self.fixture.root / "replica"
        for entry in manifest["files"]:
            target = replica / entry["project_path"]
            target.parent.mkdir(parents=True, exist_ok=True)
            shutil.copyfile(self.project / entry["project_path"], target)
            target.chmod(0o640)
        runtime = self.fixture.root / "installed-runtime"
        manifest_path = runtime / "snapshot-history" / manifest["snapshot_id"] / "manifest.json"
        manifest_path.parent.mkdir(parents=True)
        shutil.copyfile(self.fixture.bundle / "manifest.json", manifest_path)
        manifest_path.chmod(0o640)
        receipt = {"snapshot_id": manifest["snapshot_id"], "writer_project_root": str(self.project),
                   "runtime_identity": manifest["runtime_identity"], "artifact_policy": manifest["artifact_policy"],
                   "manifest_path": str(manifest_path), "manifest_sha256": deployment._sha256(manifest_path),
                   "database_sha256": {row["name"]: row["sha256"] for row in manifest["databases"]}}
        receipt_path = runtime / "active-snapshot.json"
        receipt_path.write_text(json.dumps(receipt))
        receipt_path.chmod(0o640)
        source = Path(original["mirror"]["path"])
        source.rename(source.with_suffix(".unavailable"))
        db = self.fixture.bundle / "databases/dcar_insight.sqlite3"
        with patch.dict(os.environ, {"DCAR_READ_ONLY": "1", "DCAR_ACTIVE_SNAPSHOT": str(receipt_path)}), \
                patch.object(artifact_paths, "PROJECT_ROOT", replica), \
                sqlite3.connect(db.as_uri() + "?mode=ro&immutable=1", uri=True) as connection:
            connection.row_factory = sqlite3.Row
            self.assertEqual(transport_receipts.read_transport_receipt(connection, original["receipt_id"]), original)
            self.assertEqual(connection.total_changes, 0)
            relocated = artifact_paths.resolve(source)
            # Same parsed JSON, different bytes: the installed hash must still fail.
            relocated.write_bytes(b" " + relocated.read_bytes())
            with self.assertRaises(transport_receipts.TransportReceiptError):
                transport_receipts.read_transport_receipt(connection, original["receipt_id"])
        self.assertFalse(Path(str(db) + "-wal").exists())
        self.assertFalse(Path(str(db) + "-shm").exists())

    def test_nested_external_evidence_references_are_mirrored_recursively(self) -> None:
        inner = self.ordinary()
        outer = runtime_receipts._write_evidence(
            self.fixture.database, "scan-verification-v2",
            {"contract_version": "scan-verification-evidence-v2", "proof": inner},
            evidence_root=self.evidence,
        )
        self.add_reference(outer)
        manifest = self.fixture.build_bundle()
        self.assertEqual(set(artifact_paths.runtime_evidence_aliases(manifest)), {inner["path"], outer["path"]})
        config = self.fixture.server_config()
        self.fixture.stage_artifacts(manifest, config)
        deployment.installer.verify_bundle(self.fixture.bundle, config=config)

    def test_external_alias_rejects_unsafe_source_and_mutated_identity(self) -> None:
        reference = self.ordinary()
        source = Path(reference["path"])
        for bad in ({**reference, "path": str(self.fixture.root / source.name)},
                    {**reference, "sha256": "a" * 64}, {**reference, "byte_size": reference["byte_size"] + 1}):
            with self.subTest(bad=bad), self.assertRaises(deployment.builder.SnapshotBuildError):
                self.mirror(bad)
        source.chmod(0o644)
        with self.assertRaises(deployment.builder.SnapshotBuildError):
            self.mirror(reference)
        source.chmod(0o600)
        other = source.with_suffix(".original")
        source.rename(other)
        source.symlink_to(other)
        with self.assertRaises(deployment.builder.SnapshotBuildError):
            self.mirror(reference)
        source.unlink()
        os.link(other, source)
        with self.assertRaises(deployment.builder.SnapshotBuildError):
            self.mirror(reference)

    def test_installer_rejects_missing_alias_and_unbound_alias_target(self) -> None:
        reference = self.ordinary()
        self.add_reference(reference)
        manifest = self.fixture.build_bundle()
        config = self.fixture.server_config()
        self.fixture.stage_artifacts(manifest, config)
        missing = dict(manifest)
        missing.pop("runtime_evidence_aliases")
        deployment._write_bundle_manifest(self.fixture.bundle, missing)
        with self.assertRaisesRegex(deployment.installer.SnapshotInstallError, "external runtime evidence"):
            deployment.installer.verify_bundle(self.fixture.bundle, config=config)
        manifest["runtime_evidence_aliases"]["files"][0]["project_path"] = "data/cache/another.json"
        deployment._write_bundle_manifest(self.fixture.bundle, manifest)
        with self.assertRaisesRegex(deployment.installer.SnapshotInstallError, "alias contract"):
            deployment.installer.verify_bundle(self.fixture.bundle, config=config)

    def test_tampered_receipt_self_hash_and_foreign_json_are_refused(self) -> None:
        reference = self.ordinary()
        source = Path(reference["path"])
        body = json.loads(source.read_bytes())
        body["coverage"] = {"complete": True}
        encoded = raw_evidence.canonical_json_bytes(body)
        digest = hashlib.sha256(encoded).hexdigest()
        path = source.with_name("profile-day-coverage-v3." + digest + ".json")
        path.write_bytes(encoded)
        path.chmod(0o600)
        with self.assertRaises(deployment.builder.SnapshotBuildError):
            self.mirror({"path": str(path), "sha256": digest, "byte_size": len(encoded)})


if __name__ == "__main__":
    unittest.main()
