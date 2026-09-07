"""Thin snapshots use real temporary lifecycle proofs, never production media."""
from __future__ import annotations

import hashlib
import json
import shutil
import sqlite3
import unittest
from pathlib import Path
from typing import Any

from tests import test_server_snapshot_deployment as deployment
from tests import test_v8_media_completion as completion_fixtures
from v8 import media, media_lifecycle as lifecycle, media_retention as retention
from v8.snapshot_contract import descriptor
from v8.storage import SCHEMA_VERSION, connect, transaction


class ThinServerSnapshotTest(unittest.TestCase):
    def setUp(self) -> None:
        self.completion = completion_fixtures.MediaCompletionTest()
        self.completion.setUp()
        self.addCleanup(self.completion.doCleanups)
        self.fixture = self.completion.fixture
        self.project = self.fixture.root
        self.fixture.root = self.project / "data/cache/source-fixture"
        self.fixture.root.mkdir(parents=True)
        self.fixture.media_root = self.project / "data/cache/v8/media"
        self.fixture.media_root.parent.mkdir(mode=0o700)
        self.fixture._patch(media, "MEDIA_ROOT", self.fixture.media_root)
        self.db = self.fixture.db
        for name in (".comment_hash_salt", ".platform_user_salt"):
            path = self.project / "data/cache" / name
            path.write_bytes(b"s" * 32)
            path.chmod(0o600)

    def prepare(self, content_id: int = 1, *, seal: bool = True,
                candidates: bool = False) -> dict[str, Any]:
        if candidates:
            register = lifecycle.register_download

            def with_candidates(connection: sqlite3.Connection, intent: dict[str, Any],
                                artifact_id: int, slot_id: int) -> dict[str, Any]:
                # A fallback URL alone is not a downloaded member. Explicitly
                # create three fixture candidates before real registration so
                # its file-set/hash/ordering checks see all six owned files.
                row = connection.execute("SELECT * FROM evidence_artifacts WHERE id=?", (artifact_id,)).fetchone()
                body = json.loads(media._resolved(row["local_path"]).read_bytes())
                additional = []
                for index, frame in enumerate(body["frames"]):
                    path = Path(intent["originals_root"]) / f"candidate-{index}.png"
                    path.write_bytes(media._resolved(frame["path"]).read_bytes())
                    path.chmod(0o600)
                    additional.append({"path": str(path), "kind": "image_candidate",
                                       "group_index": index, "candidate_index": 1,
                                       "sha256": deployment._sha256(path), "byte_size": path.stat().st_size})
                metadata = json.loads(row["metadata_json"])
                metadata["media_lifecycle_download_members"] = additional
                connection.execute("UPDATE evidence_artifacts SET metadata_json=? WHERE id=?",
                                   (json.dumps(metadata), artifact_id))
                return register(connection, intent, artifact_id, slot_id)

            self.fixture._patch(lifecycle, "register_download", side_effect=with_candidates)
        if candidates:
            self.assertFalse(seal, "candidate fixture intentionally models an unfinished hot bundle")
            self.fixture._source(content_id)
            self.fixture._activate()
            self.fixture._download(content_id, process=False)
            self.fixture._release()
            bundle = self.fixture._bundle(content_id)
        else:
            bundle = self.completion.ready(content_id)
        if seal:
            result = self.completion.seal(bundle)
            self.assertTrue(result["ready"], result)
        # Keep the historical evaluation/proof intact while adding the current
        # consumer release; frozen references deliberately omit mutable status.
        with connect(self.db) as connection, transaction(connection):
            connection.execute("UPDATE evaluation_releases SET status='retired' WHERE status='active'")
            connection.execute(
                "INSERT INTO taxonomy_versions(id,version,status,definition,created_at) "
                "VALUES('snapshot-current','selling-points-v5.2','published','fixture',?)",
                (self.fixture.now,),
            )
            connection.execute(
                "INSERT INTO evaluation_releases(id,rule_version,taxonomy_version,matcher_rule_sha256,"
                "status,created_at,updated_at) VALUES(?,'evaluation-v9','selling-points-v5.2',?,'active',?,?)",
                (deployment.TARGET_RELEASE_ID, "a" * 64, self.fixture.now, self.fixture.now),
            )
        return self.fixture._bundle(content_id)

    def build(self, name: str = "bundle") -> dict[str, Any]:
        return deployment.builder.build_snapshot(
            project_root=self.project, database=self.db,
            output=self.project / name, expected_user_version=SCHEMA_VERSION,
        )

    def assert_originals_not_transferred(self, snapshot: dict[str, Any], bundle: dict[str, Any]) -> None:
        self.assertEqual(snapshot["snapshot_contract"], descriptor())
        self.assertEqual(snapshot["writer_project_root"], str(self.project))
        self.assertEqual(len(snapshot["runtime_identity"]), 10)
        managed = snapshot["managed_originals"]
        self.assertEqual(managed["contract_version"], "managed-originals-v1")
        self.assertEqual(len(managed["bundles"]), 1)
        received = managed["bundles"][0]
        self.assertEqual(received["bundle_id"], bundle["bundle_id"])
        self.assertEqual(received["control_artifact_id"], bundle["control_artifact_id"])
        self.assertEqual(received["original_artifact_id"], bundle["manifest"]["original_artifact"]["artifact_id"])
        self.assertEqual(received["storage_state"], bundle["state"]["storage_state"])
        self.assertEqual([row["index"] for row in received["members"]],
                         [row["index"] for row in bundle["manifest"]["members"]])
        self.assertEqual([row["member_id"] for row in received["members"]],
                         [row["member_id"] for row in bundle["manifest"]["members"]])
        paths = {row["project_path"] for row in snapshot["files"] + snapshot["optional_reuse_files"]}
        for original, item in zip(bundle["manifest"]["members"], received["members"], strict=True):
            self.assertEqual(item["sha256"], original["sha256"])
            self.assertEqual(item["byte_size"], original["byte_size"])
            self.assertEqual(item["project_path"], "data/cache/" + item["path"])
            self.assertEqual(item["root"], "cache")
            self.assertNotIn(item["project_path"], paths)
        for proof in received["proofs"]:
            self.assertIn(proof["project_path"], {row["project_path"] for row in snapshot["files"]})
            self.assertEqual(deployment._sha256(self.project / proof["project_path"]), proof["sha256"])
        self.assertNotIn(str(self.fixture.archive), json.dumps(managed))
        self.assertFalse(snapshot["artifact_policy"]["delete_unlisted"])

    def test_archived_then_expired_originals_build_without_rewriting_proofs(self) -> None:
        bundle = self.prepare()
        archived = retention.archive_bundle(bundle["bundle_id"], db_path=self.db, at=self.fixture.now)
        self.assertEqual(archived["status"], "archived", archived)
        bundle = self.fixture._bundle()
        original_paths = [bundle["originals_root"] / row["relative_path"] for row in bundle["manifest"]["members"]]
        self.assertFalse(any(path.exists() for path in original_paths))
        immutable = {path: path.read_bytes() for path in bundle["evidence_root"].rglob("*") if path.is_file()}
        before_db = self.db.read_bytes()
        before_calls = len(self.fixture.calls)
        snapshot = self.build("archived-snapshot")
        self.assert_originals_not_transferred(snapshot, bundle)
        self.assertEqual(self.db.read_bytes(), before_db)
        self.assertEqual({path: path.read_bytes() for path in immutable}, immutable)
        self.assertEqual(len(self.fixture.calls), before_calls)
        roles = {row["role"] for row in snapshot["managed_originals"]["bundles"][0]["proofs"]}
        self.assertTrue({"completion_receipt", "archive_receipt", "hot_release_receipt", "image_preview"} <= roles)
        self.fixture.now = bundle["state"]["delete_due_at"]
        purged = retention.purge_bundle(bundle["bundle_id"], db_path=self.db, at=self.fixture.now)
        self.assertEqual(purged["status"], "expired", purged)
        bundle = self.fixture._bundle()
        snapshot = self.build("expired-snapshot")
        self.assert_originals_not_transferred(snapshot, bundle)
        self.assertIn("deletion_receipt", {row["role"] for row in snapshot["managed_originals"]["bundles"][0]["proofs"]})

    def test_hot_images_keep_download_manifest_and_all_fixed_member_indices(self) -> None:
        bundle = self.prepare(seal=False, candidates=True)
        snapshot = self.build()
        self.assert_originals_not_transferred(snapshot, bundle)
        managed = snapshot["managed_originals"]["bundles"][0]
        self.assertEqual(len(managed["members"]), 6)
        self.assertEqual(sum(row["kind"] == "image_candidate" for row in managed["members"]), 3)
        path = Path(bundle["manifest"]["original_artifact"]["local_path"]).relative_to(self.project).as_posix()
        self.assertIn(path, {row["project_path"] for row in snapshot["files"]})
        self.assertNotIn(path, {row["project_path"] for row in snapshot["optional_reuse_files"]})

    def test_managed_video_never_becomes_optional_reuse(self) -> None:
        bundle = self.prepare(2)
        snapshot = self.build()
        self.assert_originals_not_transferred(snapshot, bundle)
        managed = snapshot["managed_originals"]["bundles"][0]
        self.assertEqual([row["kind"] for row in managed["members"]], ["video"])
        roles = {row["role"] for row in managed["proofs"]}
        self.assertTrue({"asr", "ocr", "video_frame", "completion_receipt"} <= roles)

    def test_missing_preview_fails_and_does_not_publish_bundle(self) -> None:
        bundle = self.prepare()
        with connect(self.db) as connection:
            proof = media._resolved(lifecycle.load_bundle(connection, bundle["bundle_id"])["state"]["completion_receipt"]["path"])
        body = json.loads(proof.read_bytes())
        preview = next(row for row in body["evidence_files"] if row["role"] == "image_preview")
        media._resolved(preview["path"]).unlink()
        with self.assertRaises(deployment.builder.SnapshotBuildError):
            self.build()
        self.assertFalse((self.project / "bundle").exists())

    def test_hash_tampered_lifecycle_manifest_fails_before_recursion(self) -> None:
        bundle = self.prepare()
        with connect(self.db) as connection:
            row = connection.execute("SELECT local_path FROM evidence_artifacts WHERE id=?",
                                     (bundle["control_artifact_id"],)).fetchone()
        media._resolved(row[0]).write_bytes(b'{"tampered":true}\n')
        with self.assertRaisesRegex(deployment.builder.SnapshotBuildError, "managed-original validation"):
            self.build()
        self.assertFalse((self.project / "bundle").exists())

    def test_unknown_missing_required_evidence_is_not_optional_source_missing(self) -> None:
        self.prepare()
        with connect(self.db) as connection:
            connection.execute(
                "INSERT INTO evidence_artifacts(content_id,artifact_type,local_path,status,sha256,byte_size,created_at) "
                "VALUES(1,'media_source','data/cache/unknown-source.json','available',?,2,?)",
                (hashlib.sha256(b"{}").hexdigest(), self.fixture.now),
            )
        with self.assertRaisesRegex(deployment.builder.SnapshotBuildError, "missing or unsafe"):
            self.build()
        self.assertFalse((self.project / "bundle").exists())

    def test_missing_provider_raw_and_hash_tampered_scan_receipt_are_required(self) -> None:
        self.prepare()
        with connect(self.db) as connection:
            raw = connection.execute("SELECT local_path FROM provider_raw_responses LIMIT 1").fetchone()
        raw_path = media._resolved(raw[0])
        raw_bytes = raw_path.read_bytes()
        raw_path.unlink()
        with self.assertRaises(deployment.builder.SnapshotBuildError):
            self.build()
        raw_path.write_bytes(raw_bytes)
        raw_path.chmod(0o600)
        receipt = self.project / "data/cache/scan-manifest.json"
        receipt.write_bytes(b'{"complete":true}\n')
        pointer = {"path": str(receipt), "sha256": deployment._sha256(receipt),
                   "byte_size": receipt.stat().st_size}
        with connect(self.db) as connection:
            connection.execute(
                "INSERT INTO scheduler_runs(job_id,scheduled_for,status,started_at,completed_at,details_json) "
                "VALUES('pipeline_cutover',?,'succeeded',?,?,?)",
                (self.fixture.now, self.fixture.now, self.fixture.now,
                 json.dumps({"checkpoint": {"receipt": pointer}})),
            )
        receipt.write_bytes(b'{"complete":false}\n')
        with self.assertRaisesRegex(deployment.builder.SnapshotBuildError, "SHA-256 drifted"):
            self.build()
        self.assertFalse((self.project / "bundle").exists())

    def test_legacy_comment_directory_expands_and_passes_installer(self) -> None:
        self.prepare()
        comments = self.project / "data/cache/tikhub/2026-08-02/comments/legacy"
        comments.mkdir(parents=True)
        pages = {
            "page_001.json": b'{"comments":[1]}\n',
            "nested/page_002.json": b'{"comments":[2]}\n',
        }
        digest = hashlib.sha256()
        byte_size = 0
        for relative, payload in sorted(pages.items()):
            path = comments / relative
            path.parent.mkdir(parents=True, exist_ok=True)
            path.write_bytes(payload)
            path.chmod(0o600)
            child_sha256 = deployment._sha256(path)
            byte_size += len(payload)
            digest.update(relative.encode("utf-8"))
            digest.update(b"\0")
            digest.update(child_sha256.encode("ascii"))
            digest.update(b"\0")
        with connect(self.db) as connection:
            cursor = connection.execute(
                "INSERT INTO evidence_artifacts(content_id,artifact_type,local_path,status,sha256,byte_size,created_at) "
                "VALUES(1,'comments',?,'available',?,?,?)",
                (
                    comments.relative_to(self.project).as_posix(),
                    digest.hexdigest(),
                    byte_size,
                    self.fixture.now,
                ),
            )
            artifact_id = cursor.lastrowid
        snapshot = self.build()
        paths = {row["project_path"] for row in snapshot["files"]}
        self.assertNotIn(comments.relative_to(self.project).as_posix(), paths)
        for relative in pages:
            self.assertIn((comments / relative).relative_to(self.project).as_posix(), paths)
        for item in snapshot["files"]:
            target = self.project / "artifacts" / item["root"] / item["path"]
            target.parent.mkdir(parents=True, exist_ok=True)
            shutil.copy2(self.project / item["project_path"], target)
        config = deployment.installer.InstallConfig(
            database_root=self.project / "server/db",
            cache_root=self.project / "server/cache",
            reports_root=self.project / "server/reports",
            runtime_root=self.project / "server/runtime",
        )
        deployment.installer.verify_bundle(
            self.project / "bundle", config, verify_artifacts=True
        )
        with connect(self.db) as connection:
            connection.execute(
                "UPDATE evidence_artifacts SET sha256=? WHERE id=?",
                ("f" * 64, artifact_id),
            )
        with self.assertRaisesRegex(
            deployment.builder.SnapshotBuildError,
            "comment directory SHA-256 drifted",
        ):
            self.build("tampered-comments-bundle")

    def test_foreign_artifact_cannot_claim_registered_original(self) -> None:
        bundle = self.prepare()
        original = bundle["manifest"]["members"][0]
        with connect(self.db) as connection:
            connection.execute(
                "INSERT INTO evidence_artifacts(content_id,artifact_type,local_path,status,sha256,byte_size,created_at) "
                "VALUES(2,'media',?,'available',?,?,?)",
                (str(bundle["originals_root"] / original["relative_path"]),
                 original["sha256"], original["byte_size"], self.fixture.now),
            )
        with self.assertRaisesRegex(deployment.builder.SnapshotBuildError, "ownership is shared"):
            self.build()

    def test_unregistered_managed_original_is_never_legacy_optional(self) -> None:
        bundle = self.prepare()
        unknown = bundle["originals_root"] / "unregistered.mp4"
        unknown.write_bytes(b"not-a-registered-original")
        with connect(self.db) as connection:
            connection.execute(
                "INSERT INTO evidence_artifacts(content_id,artifact_type,local_path,status,sha256,byte_size,created_at) "
                "VALUES(1,'media',?,'available',?,?,?)",
                (str(unknown), deployment._sha256(unknown), unknown.stat().st_size, self.fixture.now),
            )
        with self.assertRaisesRegex(deployment.builder.SnapshotBuildError, "unregistered managed original"):
            self.build()
        self.assertFalse((self.project / "bundle").exists())

    def test_builder_does_not_change_backup_bytes_during_managed_validation(self) -> None:
        self.prepare()
        snapshot = self.build()
        database = self.project / "bundle/databases/dcar_insight.sqlite3"
        self.assertEqual(deployment._sha256(database), snapshot["databases"][0]["sha256"])
        with sqlite3.connect(f"{database.as_uri()}?mode=ro", uri=True) as connection:
            self.assertEqual(connection.execute("PRAGMA user_version").fetchone()[0], SCHEMA_VERSION)
            self.assertEqual(connection.execute("PRAGMA foreign_key_check").fetchall(), [])

    def test_archived_builder_output_passes_installer_without_original_staging(self) -> None:
        bundle = self.prepare()
        result = retention.archive_bundle(bundle["bundle_id"], db_path=self.db, at=self.fixture.now)
        self.assertEqual(result["status"], "archived", result)
        snapshot = self.build()
        for item in snapshot["files"]:
            target = self.project / "artifacts" / item["root"] / item["path"]
            target.parent.mkdir(parents=True, exist_ok=True)
            shutil.copy2(self.project / item["project_path"], target)
        server = self.project / "server"
        config = deployment.installer.InstallConfig(
            database_root=server / "db", cache_root=server / "cache",
            reports_root=server / "reports", runtime_root=server / "runtime",
        )
        verified = deployment.installer.verify_bundle(self.project / "bundle", config, verify_artifacts=True)
        self.assertEqual(verified["managed_originals"], snapshot["managed_originals"])
        for member in snapshot["managed_originals"]["bundles"][0]["members"]:
            self.assertFalse((self.project / "artifacts" / member["root"] / member["path"]).exists())

    def test_managed_snapshot_verifies_with_all_required_files_already_active(self) -> None:
        bundle = self.prepare()
        retention.archive_bundle(bundle["bundle_id"], db_path=self.db, at=self.fixture.now)
        snapshot = self.build()
        server = self.project / "server"
        config = deployment.installer.InstallConfig(
            database_root=server / "db",
            cache_root=server / "cache",
            reports_root=server / "reports",
            runtime_root=server / "runtime",
        )
        for item in snapshot["files"]:
            root = config.cache_root if item["root"] == "cache" else config.reports_root
            target = root / item["path"]
            target.parent.mkdir(parents=True, exist_ok=True)
            shutil.copy2(self.project / item["project_path"], target)

        verified = deployment.installer.verify_bundle(
            self.project / "bundle", config, verify_artifacts=True
        )

        self.assertEqual(verified["managed_originals"], snapshot["managed_originals"])
        self.assertFalse((self.project / "artifacts").exists())


if __name__ == "__main__":
    unittest.main()
