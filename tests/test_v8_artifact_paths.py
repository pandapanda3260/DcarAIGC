"""Real SQLite/media relocation without originals; no production or network.

The installer separately verifies the release/model and complete bundle. Here a
genuine sealed-media fixture exercises unchanged signed bytes on another root.
"""
from __future__ import annotations

import hashlib
import json
import os
import shutil
import sqlite3
import unittest
from pathlib import Path
from unittest.mock import patch
from fastapi.testclient import TestClient

from tests import test_v8_media_completion as fixtures
from v8 import api, artifact_paths, media, media_api, media_completion, media_lifecycle
from v8.snapshot_contract import ARTIFACT_POLICY, descriptor, validate_descriptor
from v8.storage import connect


class ArtifactRelocationTest(unittest.TestCase):
    def setUp(self):
        self.fixture = fixtures.MediaCompletionTest()
        self.fixture.setUp()
        self.addCleanup(self.fixture.doCleanups)
        self.root = self.fixture.fixture.root
        self.writer = self.root / "writer"
        self.replica = self.root / "replica"
        self.runtime = self.root / "runtime"
        self.fixture.fixture.root = self.writer / "data/cache/fixture"
        self.fixture.fixture.root.mkdir(parents=True, mode=0o700)
        self.fixture.fixture.media_root = self.writer / "data/cache/v8/media"
        self.fixture.fixture.media_root.parent.mkdir(parents=True, mode=0o700)
        self.fixture.fixture._patch(media, "MEDIA_ROOT", self.fixture.fixture.media_root)
        self.bundle = self.fixture.ready()
        sealed = self.fixture.seal(self.bundle)
        self.assertTrue(sealed["ready"], sealed)
        self.source_db = self.fixture.db
        with connect(self.source_db) as connection:
            self.bundle = media_lifecycle.load_bundle(connection, self.bundle["bundle_id"])
        self.originals = {self.bundle["originals_root"] / member["relative_path"] for member in self.bundle["manifest"]["members"]}
        self.manifest = {
            "schema": "dcar-read-replica-snapshot-v2", "snapshot_id": "20260829T040000Z-123456789abc",
            "created_at": "2026-08-29T04:00:00Z", "writer_project_root": str(self.writer),
            "runtime_identity": {"database_schema_version": 18},
            "snapshot_contract": descriptor(), "artifact_policy": ARTIFACT_POLICY,
            "files": [], "optional_reuse_files": [],
            "managed_originals": {"contract_version": "managed-originals-v1", "bundles": [{
                "bundle_id": self.bundle["bundle_id"], "members": [
                    {**member, "project_path": (self.bundle["originals_root"] / member["relative_path"]).relative_to(self.writer).as_posix()}
                    for member in self.bundle["manifest"]["members"]]}]},
        }
        for source in sorted(self.writer.rglob("*")):
            if not source.is_file() or source in self.originals or source.suffix == ".lock":
                continue
            relative = source.relative_to(self.writer)
            target = self.replica / relative
            target.parent.mkdir(parents=True, exist_ok=True, mode=0o750)
            shutil.copyfile(source, target)
            target.chmod(0o640)
            self.manifest["files"].append({"project_path": relative.as_posix(),
                "sha256": hashlib.sha256(source.read_bytes()).hexdigest(), "byte_size": source.stat().st_size})
        for directory in (self.replica, *[path for path in self.replica.rglob("*") if path.is_dir()]):
            directory.chmod(0o750)
        self.replica_db = self.root / "replica.sqlite3"
        with sqlite3.connect(self.source_db) as source, sqlite3.connect(self.replica_db) as target:
            source.backup(target)
            target.execute("PRAGMA journal_mode=DELETE")
        with connect(self.replica_db, read_only=True) as connection:
            self.manifest["runtime_identity"] = api._database_state(connection)["runtime_identity"]
        database_sha = hashlib.sha256(self.replica_db.read_bytes()).hexdigest()
        self.manifest["databases"] = [{"name": "dcar_insight.sqlite3", "sha256": database_sha}]
        self.receipt_path = self.runtime / "active-snapshot.json"
        self.manifest_path = self.runtime / "snapshot-history" / self.manifest["snapshot_id"] / "manifest.json"
        self.receipt = {"snapshot_id": self.manifest["snapshot_id"],
            "writer_project_root": str(self.writer), "runtime_identity": self.manifest["runtime_identity"],
            "artifact_policy": ARTIFACT_POLICY, "manifest_path": str(self.manifest_path),
            "database_sha256": {"dcar_insight.sqlite3": database_sha}}
        self.write_receipt()
        self.patchers = [patch.dict(os.environ, {"DCAR_READ_ONLY": "1", "DCAR_ACTIVE_SNAPSHOT": str(self.receipt_path)}),
            patch.object(artifact_paths, "PROJECT_ROOT", self.replica),
            patch.object(media, "PROJECT_ROOT", self.replica),
            patch.object(media_lifecycle, "PROJECT_ROOT", self.replica)]
        for item in self.patchers:
            item.start()
            self.addCleanup(item.stop)
        self.addCleanup(artifact_paths._context.cache_clear)

    def write_receipt(self):
        self.manifest_path.parent.mkdir(parents=True, exist_ok=True)
        self.manifest_path.write_text(json.dumps(self.manifest), encoding="utf-8")
        self.manifest_path.chmod(0o640)
        self.receipt["manifest_sha256"] = hashlib.sha256(self.manifest_path.read_bytes()).hexdigest()
        self.receipt_path.write_text(json.dumps(self.receipt), encoding="utf-8")
        self.receipt_path.chmod(0o640)

    def test_whole_retained_evidence_reads_on_other_root_without_original_bytes(self):
        original_hashes = {path: hashlib.sha256(path.read_bytes()).hexdigest() for path in self.originals}
        db_before = self.replica_db.read_bytes()
        with connect(self.replica_db, read_only=True) as connection:
            bundle = media_lifecycle.load_bundle(connection, self.bundle["bundle_id"])
            self.assertTrue(media_completion._verify(connection, bundle)["ready"])
            view = media_api.managed_evidence(connection, 1, read_only=True)
            self.assertEqual(view["media_lifecycle"]["reason"], "replica_original_omitted")
            self.assertFalse(view["media_lifecycle"]["can_restore"])
            self.assertEqual([item["index"] for item in view["media"]], [0, 1, 2])
            self.assertEqual(len(view["previews"]), 3)
            status = media_api.lifecycle_status(connection, read_only=True)
            self.assertEqual(status["snapshot_captured_at"], self.manifest["created_at"])
            self.assertIsInstance(status["snapshot_lag_seconds"], int)
        self.assertEqual(self.replica_db.read_bytes(), db_before)
        for path, digest in original_hashes.items():
            self.assertEqual(hashlib.sha256(path.read_bytes()).hexdigest(), digest)
            self.assertFalse((self.replica / path.relative_to(self.writer)).exists())
        self.assertFalse(Path(str(self.replica_db) + "-wal").exists())
        self.assertFalse(Path(str(self.replica_db) + "-shm").exists())

    def test_unlisted_path_and_hash_changed_manifest_never_fall_back_to_writer(self):
        with self.assertRaisesRegex(artifact_paths.ArtifactPathError, "unlisted"):
            artifact_paths.resolve(str(self.writer / "data/cache/unlisted.json"))
        self.manifest_path.write_text("{}", encoding="utf-8")
        with self.assertRaisesRegex(artifact_paths.ArtifactPathError, "changed"):
            artifact_paths.resolve(self.manifest["files"][0]["project_path"])

    def test_transferred_original_and_path_traversal_are_rejected(self):
        member = self.manifest["managed_originals"]["bundles"][0]["members"][0]
        self.manifest["files"].append({key: member[key] for key in ("project_path", "sha256", "byte_size")})
        self.write_receipt()
        with self.assertRaisesRegex(artifact_paths.ArtifactPathError, "transferred"):
            artifact_paths.installed_snapshot()
        self.manifest["files"].pop()
        self.manifest["files"][0]["project_path"] = "data/cache/../private.json"
        self.write_receipt()
        with self.assertRaisesRegex(artifact_paths.ArtifactPathError, "path_invalid"):
            artifact_paths.installed_snapshot()

    def test_only_receipt_bound_replica_evidence_accepts_group_read_permission(self):
        with connect(self.replica_db, read_only=True) as connection:
            media_lifecycle.load_bundle(connection, self.bundle["bundle_id"])
        path = artifact_paths.resolve(self.bundle["manifest"]["source"]["local_path"])
        self.assertEqual(media_lifecycle._file(path)["sha256"], self.bundle["manifest"]["source"]["sha256"])
        with patch.dict(os.environ, {"DCAR_READ_ONLY": "0"}):
            with self.assertRaisesRegex(media_lifecycle.LifecycleError, "not_private"):
                media_lifecycle._file(path)
        path.chmod(0o660)
        with self.assertRaisesRegex(media_lifecycle.LifecycleError, "not_private"):
            media_lifecycle._file(path)

    def test_missing_preview_or_changed_retained_evidence_is_not_a_success(self):
        with connect(self.replica_db, read_only=True) as connection:
            bundle = media_lifecycle.load_bundle(connection, self.bundle["bundle_id"])
            _, members = media_api._preview_members(connection, bundle)
            path = members[0]["path"]
            path.write_bytes(b"corrupt preview")
            with self.assertRaises(media_lifecycle.LifecycleError):
                media_api.managed_evidence(connection, 1, read_only=True)
            with self.assertRaises(media_completion.CompletionBlocked):
                media_completion._verify(connection, bundle)

    def test_receipt_path_or_contract_cannot_be_substituted(self):
        self.receipt["manifest_path"] = str(self.root / "outside.json")
        self.write_receipt()
        with self.assertRaisesRegex(artifact_paths.ArtifactPathError, "manifest_path"):
            artifact_paths.installed_snapshot()
        with self.assertRaisesRegex(ValueError, "consumer_contract"):
            validate_descriptor({**descriptor(), "media_retention_sha256": "0" * 64})

    def test_manifest_has_a_separate_bound_from_the_small_active_receipt(self):
        self.manifest["padding"] = "x" * (
            artifact_paths.MAX_SNAPSHOT_RECEIPT_BYTES + 1
        )
        self.write_receipt()
        artifact_paths._context.cache_clear()
        manifest_size = self.manifest_path.stat().st_size

        self.assertGreater(
            manifest_size, artifact_paths.MAX_SNAPSHOT_RECEIPT_BYTES
        )
        self.assertEqual(
            artifact_paths.installed_snapshot()["receipt"]["snapshot_id"],
            self.manifest["snapshot_id"],
        )
        artifact_paths._context.cache_clear()
        with patch.object(
            artifact_paths, "MAX_SNAPSHOT_MANIFEST_BYTES", manifest_size - 1
        ):
            with self.assertRaisesRegex(
                artifact_paths.ArtifactPathError, "oversized"
            ):
                artifact_paths.installed_snapshot()

    def test_builder_root_context_is_local_and_does_not_modify_readonly_environment(self):
        before = os.environ["DCAR_READ_ONLY"]
        with artifact_paths.using_artifact_root(self.writer):
            self.assertEqual(artifact_paths.resolve("data/cache/fixture/raw-0.json"), self.writer / "data/cache/fixture/raw-0.json")
            self.assertIsNone(artifact_paths.installed_snapshot())
        self.assertEqual(os.environ["DCAR_READ_ONLY"], before)
        self.assertEqual(artifact_paths.resolve("data/cache/fixture/raw-0.json"), self.replica / "data/cache/fixture/raw-0.json")

    def test_real_readonly_api_startup_binds_manifest_database_and_disables_lifecycle(self):
        config = api.ApiConfig(db_path=self.replica_db, reports_root=self.replica / "reports", read_only=True,
                               scheduler_enabled=False, startup_catchup_enabled=False,
                               legacy_db_path=self.root / "legacy.sqlite3", operator_freeze_lock=self.root / "freeze.lock")
        before = self.replica_db.read_bytes()
        with TestClient(api.create_app(config)) as client:
            response = client.get("/api/v8/health")
            self.assertEqual(response.status_code, 200, response.text)
            self.assertEqual(response.json()["snapshot_contract"], descriptor())
            self.assertFalse(response.json()["lifecycle_jobs_enabled"])
            self.assertEqual(response.json()["database_state"]["sha256"], self.receipt["database_sha256"]["dcar_insight.sqlite3"])
        self.assertEqual(before, self.replica_db.read_bytes())
        self.receipt["database_sha256"]["dcar_insight.sqlite3"] = "0" * 64
        self.write_receipt()
        with self.assertRaisesRegex(artifact_paths.ArtifactPathError, "database_binding"):
            with TestClient(api.create_app(config)):
                self.fail("mismatched snapshot receipt started")


if __name__ == "__main__":
    unittest.main()
