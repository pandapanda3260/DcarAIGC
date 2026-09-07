from __future__ import annotations

import hashlib
import json
import os
import re
import shlex
import sqlite3
import subprocess
import sys
import tempfile
import unittest
from pathlib import Path


ROOT = Path(__file__).resolve().parents[1]
RELEASE_ROOT = "/var/www/dcar-aigc/current"


class WebThumbnailDeploymentContractTestCase(unittest.TestCase):
    def setUp(self) -> None:
        self.unit = (ROOT / "deploy/server/systemd/dcar-web.service").read_text()
        self.environment = {}
        for line in self.unit.splitlines():
            if line.startswith("Environment="):
                for assignment in shlex.split(line.partition("=")[2]):
                    name, _, value = assignment.partition("=")
                    self.environment[name] = value

    def test_thumbnail_reader_has_complete_runtime_and_read_only_cache_mount(self) -> None:
        server = (ROOT / "app/web/app/lib/contentThumbnailServer.ts").read_text()
        required = set(re.findall(r"process\.env\.(DCAR_THUMBNAIL_\w+)", server))
        self.assertEqual(len(required), 4)
        self.assertFalse(required - self.environment.keys())
        self.assertEqual(self.environment["DCAR_THUMBNAIL_PYTHON"], f"{RELEASE_ROOT}/.venv/bin/python")
        self.assertEqual(self.environment["DCAR_THUMBNAIL_PROJECT_ROOT"], RELEASE_ROOT)
        self.assertEqual(self.environment["DCAR_THUMBNAIL_DB"], "/var/lib/dcar-aigc/db/dcar_insight.sqlite3")
        helper = self.environment["DCAR_THUMBNAIL_HELPER"]
        self.assertTrue(helper.startswith(RELEASE_ROOT + "/"))
        self.assertTrue((ROOT / helper.removeprefix(RELEASE_ROOT + "/")).is_file())
        self.assertIn(f"BindReadOnlyPaths=/var/lib/dcar-aigc/cache:{RELEASE_ROOT}/data/cache", self.unit)
        self.assertEqual(self.environment["DCAR_READ_ONLY"], "1")
        self.assertIn("ReadOnlyPaths=/var/lib/dcar-aigc/db /var/lib/dcar-aigc/cache", self.unit)
        for setting in ("ProtectSystem=strict", "ProtectHome=true", "NoNewPrivileges=true"):
            self.assertIn(setting, self.unit)
        self.assertNotIn("ReadWritePaths=", self.unit)
        self.assertNotIn("DCAR_UPDATE_COORDINATOR_TOKEN_FILE", self.environment)

    def test_configured_helper_projects_relative_snapshot_raw_without_changing_database(self) -> None:
        helper = ROOT / self.environment["DCAR_THUMBNAIL_HELPER"].removeprefix(RELEASE_ROOT + "/")
        with tempfile.TemporaryDirectory(prefix="dcar-thumbnail-deploy-") as temporary:
            project = Path(temporary)
            relative_raw = "data/cache/v8/raw_responses/detail.json"
            raw = project / relative_raw
            raw.parent.mkdir(parents=True)
            cover = "https://example.invalid/cover.jpg"
            body = json.dumps({"aweme_id": "111", "video": {"cover": {"url_list": [cover]}}}).encode()
            raw.write_bytes(body)
            database = project / "snapshot.sqlite3"
            with sqlite3.connect(database) as connection:
                connection.executescript("""
                    PRAGMA user_version=20;
                    PRAGMA journal_mode=DELETE;
                    CREATE TABLE accounts(id INTEGER PRIMARY KEY,enabled INTEGER);
                    CREATE TABLE content_items(id INTEGER PRIMARY KEY,platform TEXT,platform_content_id TEXT,account_id INTEGER);
                    CREATE TABLE content_identity_merge_events(loser_content_id INTEGER);
                    CREATE TABLE evidence_artifacts(id INTEGER PRIMARY KEY,content_id INTEGER,artifact_type TEXT,local_path TEXT,status TEXT,sha256 TEXT,byte_size INTEGER,metadata_json TEXT);
                    CREATE TABLE provider_raw_responses(id INTEGER PRIMARY KEY,content_id INTEGER,account_id INTEGER,operation TEXT,local_path TEXT,sha256 TEXT,byte_size INTEGER,captured_at TEXT);
                    INSERT INTO accounts VALUES(1,1);
                    INSERT INTO content_items VALUES(1,'douyin','111',1);
                """)
                connection.execute("INSERT INTO provider_raw_responses VALUES(1,1,1,?,?,?,?,?)",
                                   ("douyin_video_detail", relative_raw, hashlib.sha256(body).hexdigest(), len(body), "2026-09-07"))
            before = database.read_bytes()
            database.chmod(0o444)
            result = subprocess.run([sys.executable, "-B", "-I", str(helper), "--db", str(database),
                                     "--project-root", str(project), "--ids", "1"],
                                    text=True, capture_output=True, check=True, timeout=8)
            self.assertEqual(json.loads(result.stdout), {"items": {"1": {"local_url": None, "remote_url": cover}}})
            self.assertEqual(database.read_bytes(), before)
            self.assertFalse(Path(str(database) + "-wal").exists())
            self.assertFalse(Path(str(database) + "-shm").exists())


class ReplicaThumbnailRelocationTestCase(unittest.TestCase):
    def setUp(self) -> None:
        from v8.snapshot_contract import ARTIFACT_POLICY, descriptor

        self.temporary = tempfile.TemporaryDirectory(prefix="dcar-thumbnail-replica-")
        self.addCleanup(self.temporary.cleanup)
        self.root = Path(self.temporary.name).resolve()
        self.project = self.root / "replica"
        self.project.mkdir()
        (self.project / "src").symlink_to(ROOT / "src", target_is_directory=True)
        self.writer = self.root / "writer"
        self.database = self.root / "snapshot.sqlite3"
        self.cover = "https://example.invalid/authorized.jpg"
        body = json.dumps({"aweme_id": "111", "video": {"cover": {"url_list": [self.cover]}}}).encode()
        self.files = []
        rows = []
        for content_id, name, listed, outside, damaged in (
            (1, "allowed.json", True, False, False),
            (2, "unlisted.json", False, False, False),
            (3, "outside.json", False, True, False),
            (4, "damaged.json", True, False, True),
        ):
            relative = "data/cache/v8/raw_responses/" + name
            path = self.root / name if outside else self.project / relative
            path.parent.mkdir(parents=True, exist_ok=True)
            stored = body + b" " if damaged else body
            path.write_bytes(stored)
            if listed:
                self.files.append({"project_path": relative, "sha256": hashlib.sha256(body).hexdigest(), "byte_size": len(body)})
            original = path if outside else self.writer / relative
            rows.append((content_id, content_id, 1, "douyin_video_detail", str(original), hashlib.sha256(stored).hexdigest(), len(stored), "2026-09-07"))
        with sqlite3.connect(self.database) as connection:
            connection.executescript("""
                PRAGMA user_version=20;
                PRAGMA journal_mode=DELETE;
                CREATE TABLE accounts(id INTEGER PRIMARY KEY,enabled INTEGER);
                CREATE TABLE content_items(id INTEGER PRIMARY KEY,platform TEXT,platform_content_id TEXT,account_id INTEGER);
                CREATE TABLE content_identity_merge_events(loser_content_id INTEGER);
                CREATE TABLE evidence_artifacts(id INTEGER PRIMARY KEY,content_id INTEGER,artifact_type TEXT,local_path TEXT,status TEXT,sha256 TEXT,byte_size INTEGER,metadata_json TEXT);
                CREATE TABLE provider_raw_responses(id INTEGER PRIMARY KEY,content_id INTEGER,account_id INTEGER,operation TEXT,local_path TEXT,sha256 TEXT,byte_size INTEGER,captured_at TEXT);
                INSERT INTO accounts VALUES(1,1);
            """)
            connection.executemany("INSERT INTO content_items VALUES(?,'douyin','111',1)", [(i,) for i in range(1, 5)])
            connection.executemany("INSERT INTO provider_raw_responses VALUES(?,?,?,?,?,?,?,?)", rows)
        self.database_before = self.database.read_bytes()
        digest = hashlib.sha256(self.database_before).hexdigest()
        snapshot_id = "20260907T000000Z-123456789abc"
        self.receipt = self.root / "runtime/active-snapshot.json"
        self.manifest = self.root / "runtime/snapshot-history" / snapshot_id / "manifest.json"
        self.manifest.parent.mkdir(parents=True)
        manifest = {"schema": "dcar-read-replica-snapshot-v2", "snapshot_id": snapshot_id,
                    "writer_project_root": str(self.writer), "runtime_identity": {"database_schema_version": 20},
                    "snapshot_contract": descriptor(), "artifact_policy": ARTIFACT_POLICY, "files": self.files,
                    "databases": [{"name": "dcar_insight.sqlite3", "sha256": digest}],
                    "managed_originals": {"contract_version": "managed-originals-v1", "bundles": []}}
        self.manifest.write_text(json.dumps(manifest))
        receipt = {"snapshot_id": snapshot_id, "writer_project_root": str(self.writer),
                   "runtime_identity": manifest["runtime_identity"], "artifact_policy": ARTIFACT_POLICY,
                   "manifest_path": str(self.manifest), "manifest_sha256": hashlib.sha256(self.manifest.read_bytes()).hexdigest(),
                   "database_sha256": {"dcar_insight.sqlite3": digest}}
        self.receipt.write_text(json.dumps(receipt))
        self.manifest.chmod(0o640)
        self.receipt.chmod(0o640)

    def run_reader(self):
        return subprocess.run([sys.executable, "-B", "-I", str(ROOT / "app/web/server/content_thumbnails.py"),
                               "--db", str(self.database), "--project-root", str(self.project), "--ids", "1,2,3,4"],
                              env={**os.environ, "DCAR_READ_ONLY": "1", "DCAR_ACTIVE_SNAPSHOT": str(self.receipt)},
                              text=True, capture_output=True, timeout=8)

    def test_writer_paths_require_exact_membership_and_snapshot_file_hash(self) -> None:
        result = self.run_reader()
        self.assertEqual(result.returncode, 0, result.stderr)
        items = json.loads(result.stdout)["items"]
        self.assertEqual(items["1"]["remote_url"], self.cover)
        for content_id in ("2", "3", "4"):
            self.assertIsNone(items[content_id]["remote_url"])
        self.assertEqual(self.database.read_bytes(), self.database_before)
        self.assertFalse(Path(str(self.database) + "-wal").exists())
        self.assertFalse(Path(str(self.database) + "-shm").exists())

    def test_changed_manifest_cannot_authorize_a_file(self) -> None:
        self.manifest.write_bytes(self.manifest.read_bytes() + b" ")
        result = self.run_reader()
        self.assertEqual(result.returncode, 1)
        self.assertEqual(json.loads(result.stdout), {"error": "thumbnail_projection_unavailable"})

    def test_legacy_image_bundle_matches_read_only_api_omission_and_keeps_remote_cover(self) -> None:
        image_name = "data/cache/v8/media/sample/image.jpg"
        bundle_name = "data/cache/v8/media/sample/manifest.json"
        image = self.project / image_name
        image.parent.mkdir(parents=True)
        image.write_bytes(b"\xff\xd8\xff" + b"a" * 50)
        bundle = self.project / bundle_name
        bundle.write_text(json.dumps({"image_paths": [str(self.writer / image_name)]}))
        with sqlite3.connect(self.database) as connection:
            connection.execute("INSERT INTO evidence_artifacts VALUES(101,1,'media_manifest',?,'available',?,?, '{}')",
                               (str(self.writer / bundle_name), hashlib.sha256(bundle.read_bytes()).hexdigest(), bundle.stat().st_size))
        manifest = json.loads(self.manifest.read_text())
        for relative, path in ((image_name, image), (bundle_name, bundle)):
            manifest["files"].append({"project_path": relative, "sha256": hashlib.sha256(path.read_bytes()).hexdigest(), "byte_size": path.stat().st_size})
        self.database_before = self.database.read_bytes()
        digest = hashlib.sha256(self.database_before).hexdigest()
        manifest["databases"][0]["sha256"] = digest
        self.manifest.write_text(json.dumps(manifest))
        receipt = json.loads(self.receipt.read_text())
        receipt["database_sha256"]["dcar_insight.sqlite3"] = digest
        receipt["manifest_sha256"] = hashlib.sha256(self.manifest.read_bytes()).hexdigest()
        self.receipt.write_text(json.dumps(receipt))
        result = self.run_reader()
        self.assertEqual(result.returncode, 0, result.stderr)
        item = json.loads(result.stdout)["items"]["1"]
        self.assertIsNone(item["local_url"])
        self.assertEqual(item["remote_url"], self.cover)
        image.write_bytes(b"\xff\xd8\xff" + b"b" * 50)
        result = self.run_reader()
        self.assertEqual(result.returncode, 0, result.stderr)
        self.assertIsNone(json.loads(result.stdout)["items"]["1"]["local_url"])
        self.assertEqual(self.database.read_bytes(), self.database_before)


if __name__ == "__main__":
    unittest.main()
