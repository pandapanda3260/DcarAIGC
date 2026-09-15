"""Synthetic CAS and schema-21 regressions for the isolated thumbnail reader."""
import sys
sys.dont_write_bytecode = True

import hashlib
import importlib.util
import json
import os
from pathlib import Path
import sqlite3
import subprocess
import tempfile
import unittest
from unittest.mock import patch

import zstandard


HELPER_PATH = Path(__file__).parents[1] / "server/content_thumbnails.py"
SPEC = importlib.util.spec_from_file_location("content_thumbnail_blobs_helper", HELPER_PATH)
helper = importlib.util.module_from_spec(SPEC)
SPEC.loader.exec_module(helper)


class ThumbnailBlobTests(unittest.TestCase):
    def setUp(self):
        self.temp = tempfile.TemporaryDirectory()
        self.addCleanup(self.temp.cleanup)
        self.root = Path(self.temp.name)
        self.db = self.root / "test.sqlite3"
        self.environment = patch.dict(os.environ, {"DCAR_READ_ONLY": "0"})
        self.environment.start()
        self.addCleanup(self.environment.stop)
        with sqlite3.connect(self.db) as connection:
            connection.executescript("""
                PRAGMA user_version=21;
                CREATE TABLE accounts(id INTEGER PRIMARY KEY,enabled INTEGER);
                CREATE TABLE content_items(id INTEGER PRIMARY KEY,platform TEXT,platform_content_id TEXT,account_id INTEGER);
                CREATE TABLE content_identity_merge_events(id INTEGER PRIMARY KEY,loser_content_id INTEGER);
                CREATE TABLE evidence_artifacts(id INTEGER PRIMARY KEY,content_id INTEGER,artifact_type TEXT,local_path TEXT,status TEXT,sha256 TEXT,byte_size INTEGER,metadata_json TEXT);
                CREATE TABLE provider_raw_responses(id INTEGER PRIMARY KEY,content_id INTEGER,account_id INTEGER,operation TEXT,local_path TEXT,sha256 TEXT,byte_size INTEGER,captured_at TEXT,raw_blob_id INTEGER);
                CREATE TABLE provider_raw_blobs(id INTEGER PRIMARY KEY,entity_sha256 TEXT,entity_size INTEGER,codec TEXT,codec_version TEXT,stored_sha256 TEXT,stored_size INTEGER,hot_path TEXT,hot_owned INTEGER,hot_state TEXT,raw_stored_at TEXT,recorded_at TEXT);
                INSERT INTO accounts VALUES(1,1),(2,0);
                INSERT INTO content_items VALUES(1,'douyin','111',1),(2,'douyin','222',1),(3,'xiaohongshu','333',1),(4,'douyin','444',2),(5,'douyin','555',1);
                INSERT INTO content_identity_merge_events VALUES(1,5);
            """)

    @staticmethod
    def cover(identity="111"):
        return {"aweme_id": identity, "video": {"cover": {"url_list": [f"https://example.com/{identity}.jpg"]}}}

    def blob(self, blob_id, body, *, codec="zstd"):
        entity = json.dumps(body, separators=(",", ":")).encode()
        stored = zstandard.ZstdCompressor().compress(entity) if codec == "zstd" else entity
        entity_sha = hashlib.sha256(entity).hexdigest()
        stored_sha = hashlib.sha256(stored).hexdigest()
        version = "zstd-3-v1" if codec == "zstd" else "identity-v1"
        path = self.root / "raw_responses" / "blobs-v1" / entity_sha[:2] / f"{entity_sha}.{version}.zst"
        path.parent.mkdir(parents=True, exist_ok=True)
        path.write_bytes(stored)
        with sqlite3.connect(self.db) as connection:
            connection.execute("INSERT INTO provider_raw_blobs VALUES(?,?,?,?,?,?,?,?,?,?,?,?)", (
                blob_id, entity_sha, len(entity), codec, version, stored_sha, len(stored), str(path),
                1, "present", "2026-09-11T00:00:00Z", "2026-09-11T00:00:00Z"))
        return {"id": blob_id, "path": path, "stored_sha": stored_sha, "stored_size": len(stored),
                "entity_sha": entity_sha, "entity_size": len(entity)}

    def response(self, row_id, content_id, blob, *, receipt="stored", account=1,
                 operation="douyin_video_detail"):
        sha, size = blob[f"{receipt}_sha"], blob[f"{receipt}_size"]
        with sqlite3.connect(self.db) as connection:
            connection.execute("INSERT INTO provider_raw_responses VALUES(?,?,?,?,?,?,?,?,?)", (
                row_id, content_id, account, operation, str(blob["path"]), sha, size,
                "2026-09-11T00:00:00Z", blob["id"]))
        return sha

    def change(self, sql, values=()):
        with sqlite3.connect(self.db) as connection:
            connection.execute(sql, values)

    def result(self, ids=(1,)):
        return helper.project(self.db, list(ids), project_root=self.root)["items"]

    def assert_unavailable(self, reason="source_unavailable"):
        self.assertEqual(self.result()["1"], {"local_url": None, "remote_url": None,
                                             "remote_urls": [], "reason": reason})

    def test_schema21_keeps_canonical_and_enabled_content_only(self):
        blob = self.blob(1, {"data": [self.cover("111"), self.cover("444"), self.cover("555")]})
        for content_id in (1, 4, 5):
            self.response(content_id, content_id, blob)
        result = self.result((1, 4, 5, 999))
        self.assertEqual(set(result), {"1"})
        self.assertEqual(result["1"]["remote_url"], "https://example.com/111.jpg")

    def test_cas_zstd_and_identity_need_no_json_sidecar(self):
        for codec, cid, identity in (("zstd", 1, "111"), ("identity", 2, "222")):
            blob = self.blob(cid, self.cover(identity), codec=codec)
            self.response(cid, cid, blob)
        self.assertFalse(list(self.root.rglob("*.metadata.json")))
        result = self.result((1, 2))
        self.assertEqual(result["1"]["remote_url"], "https://example.com/111.jpg")
        self.assertEqual(result["2"]["remote_url"], "https://example.com/222.jpg")

    def test_discovery_only_cas_page_covers_multiple_contents_without_media_reads(self):
        page = self.blob(10, {"data": {"aweme_list": [self.cover("111"), self.cover("222")]}})
        self.response(10, None, page, operation="douyin_user_posts")
        with patch.object(helper, "_read", wraps=helper._read) as read:
            result = self.result((1, 2))
        self.assertEqual(result["1"], {"local_url": None, "remote_url": "https://example.com/111.jpg",
                                      "remote_urls": ["https://example.com/111.jpg"], "reason": None})
        self.assertEqual(result["2"], {"local_url": None, "remote_url": "https://example.com/222.jpg",
                                      "remote_urls": ["https://example.com/222.jpg"], "reason": None})
        self.assertEqual([Path(call.args[0]) for call in read.call_args_list], [page["path"]])
        self.change("UPDATE provider_raw_responses SET account_id=2 WHERE id=10")
        self.assert_unavailable("not_found")

    def test_compressed_cas_response_must_bind_stored_receipt(self):
        blob = self.blob(1, self.cover())
        self.response(1, 1, blob, receipt="entity")
        self.assert_unavailable()

    def test_stored_and_entity_receipt_mismatches_fail_closed(self):
        blob = self.blob(1, self.cover())
        self.response(1, 1, blob)
        for field, invalid, valid in (
            ("stored_sha256", "0" * 64, blob["stored_sha"]),
            ("stored_size", blob["stored_size"] + 1, blob["stored_size"]),
            ("entity_sha256", "0" * 64, blob["entity_sha"]),
            ("entity_size", blob["entity_size"] + 1, blob["entity_size"]),
        ):
            with self.subTest(field=field):
                self.change(f"UPDATE provider_raw_blobs SET {field}=? WHERE id=1", (invalid,))
                self.assert_unavailable()
                self.change(f"UPDATE provider_raw_blobs SET {field}=? WHERE id=1", (valid,))
        self.change("UPDATE provider_raw_responses SET sha256=? WHERE id=1", ("0" * 64,))
        self.assert_unavailable()

    def test_corrupt_zstd_with_matching_stored_receipt_is_not_decoded(self):
        blob = self.blob(1, self.cover())
        self.response(1, 1, blob)
        corrupt = b"invalid-zstd-frame"
        blob["path"].write_bytes(corrupt)
        sha = hashlib.sha256(corrupt).hexdigest()
        self.change("UPDATE provider_raw_blobs SET stored_sha256=?,stored_size=? WHERE id=1", (sha, len(corrupt)))
        self.change("UPDATE provider_raw_responses SET sha256=?,byte_size=? WHERE id=1", (sha, len(corrupt)))
        self.assert_unavailable()

    def test_stored_entity_and_total_budgets_remain_bounded(self):
        blob = self.blob(1, self.cover())
        self.response(1, 1, blob)
        for constant, limit in (("MAX_STORED", blob["stored_size"] - 1),
                                ("MAX_ENTITY", blob["entity_size"] - 1),
                                ("MAX_TOTAL", blob["stored_size"] + blob["entity_size"] - 1)):
            with self.subTest(constant=constant), patch.object(helper, constant, limit):
                self.assert_unavailable()
        with patch.object(helper, "MAX_TOTAL", 0), patch.object(helper, "_read", side_effect=AssertionError("budget exhausted")):
            self.assert_unavailable()

    def test_evicted_deleted_and_missing_blobs_return_no_candidate(self):
        blob = self.blob(1, self.cover())
        self.response(1, 1, blob)
        for state in ("evicted", "deleted"):
            with self.subTest(state=state):
                self.change("UPDATE provider_raw_blobs SET hot_state=? WHERE id=1", (state,))
                with patch.object(helper, "_read", side_effect=AssertionError("inactive hot blob must not be read")):
                    self.assert_unavailable()
        self.change("UPDATE provider_raw_blobs SET hot_state='present' WHERE id=1")
        blob["path"].unlink()
        self.assert_unavailable()

    def test_missing_blob_receipt_never_falls_back_to_legacy_json(self):
        blob = self.blob(1, self.cover())
        self.response(1, 1, blob)
        legacy = self.root / "otherwise-valid.json"
        body = json.dumps(self.cover()).encode()
        legacy.write_bytes(body)
        self.change("UPDATE provider_raw_responses SET local_path=?,sha256=?,byte_size=? WHERE id=1",
                    (str(legacy), hashlib.sha256(body).hexdigest(), len(body)))
        self.change("DELETE FROM provider_raw_blobs WHERE id=1")
        with patch.object(helper, "_read", side_effect=AssertionError("missing CAS identity must not become legacy")):
            self.assert_unavailable()

    def test_linked_blob_uses_its_hot_path_not_legacy_response_path(self):
        blob = self.blob(1, self.cover())
        self.response(1, 1, blob)
        self.change("UPDATE provider_raw_responses SET local_path='obsolete/response.json' WHERE id=1")
        self.assertEqual(self.result()["1"]["remote_url"], "https://example.com/111.jpg")

    def test_derived_parent_uses_cas_and_checks_parent_identity(self):
        parent = self.blob(10, {"data": [self.cover("111"), self.cover("222")]})
        sha = self.response(10, None, parent, operation="douyin_user_posts")
        for row_id, content_id in ((11, 1), (12, 2)):
            child = self.blob(row_id, {"source_raw_response_id": 10, "source_sha256": sha,
                                      "source_captured_at": "2026-09-11T00:00:00Z"})
            self.response(row_id, content_id, child)
        with patch.object(helper, "_read", wraps=helper._read) as read:
            result = self.result((1, 2))
        self.assertEqual(result["1"]["remote_url"], "https://example.com/111.jpg")
        self.assertEqual(result["2"]["remote_url"], "https://example.com/222.jpg")
        self.assertEqual(sum(Path(call.args[0]) == parent["path"] for call in read.call_args_list), 1)
        self.change("UPDATE provider_raw_responses SET account_id=2 WHERE id=10")
        self.assert_unavailable()

    def test_shared_blob_is_read_once_for_multiple_response_rows(self):
        blob = self.blob(1, {"data": [self.cover("111"), self.cover("222")]})
        self.response(1, 1, blob, receipt="stored")
        self.response(2, 2, blob)
        self.change("UPDATE provider_raw_responses SET local_path='other/response.json' WHERE id=2")
        with patch.object(helper, "_read", wraps=helper._read) as read:
            result = self.result((1, 2))
        self.assertEqual(result["1"]["remote_url"], "https://example.com/111.jpg")
        self.assertEqual(result["2"]["remote_url"], "https://example.com/222.jpg")
        self.assertEqual(sum(Path(call.args[0]) == blob["path"] for call in read.call_args_list), 1)

    def test_cas_projection_does_not_change_database_or_files(self):
        self.response(1, 1, self.blob(1, self.cover()))
        before = {p.relative_to(self.root): p.read_bytes() for p in self.root.rglob("*") if p.is_file()}
        original_connect = sqlite3.connect
        connections = []
        statements = []

        def connect(database_uri, **kwargs):
            connections.append((database_uri, kwargs))
            connection = original_connect(database_uri, **kwargs)
            connection.set_trace_callback(statements.append)
            return connection

        with patch.object(helper.sqlite3, "connect", side_effect=connect):
            self.assertEqual(self.result()["1"]["remote_url"], "https://example.com/111.jpg")
        after = {p.relative_to(self.root): p.read_bytes() for p in self.root.rglob("*") if p.is_file()}
        self.assertEqual(before, after)
        self.assertTrue(all(uri.endswith("?mode=ro") and options.get("uri") for uri, options in connections))
        self.assertTrue(any("QUERY_ONLY=ON" in statement.upper().replace(" ", "") for statement in statements))

    def test_cli_isolated_python_reads_cas_without_application_package_or_writes(self):
        self.response(1, 1, self.blob(1, self.cover()))
        poison = self.root / "src" / "dcar_eval" / "v8"
        poison.mkdir(parents=True)
        (poison / "__init__.py").write_text("raise AssertionError('application package must not be imported')\n")
        before = {p.relative_to(self.root): p.read_bytes() for p in self.root.rglob("*") if p.is_file()}
        process = subprocess.run([sys.executable, "-B", "-I", str(HELPER_PATH.resolve()),
                                  "--db", str(self.db), "--ids", "1", "--project-root", str(self.root)],
                                 cwd=self.root, capture_output=True, text=True, timeout=10)
        self.assertEqual(process.returncode, 0, process.stderr + process.stdout)
        self.assertEqual(json.loads(process.stdout)["items"]["1"]["remote_url"], "https://example.com/111.jpg")
        after = {p.relative_to(self.root): p.read_bytes() for p in self.root.rglob("*") if p.is_file()}
        self.assertEqual(before, after)


class ThumbnailBlobSchema22Tests(ThumbnailBlobTests):
    """The same CAS safety contract applies after account-intake migration."""
    def setUp(self):
        super().setUp()
        self.change("PRAGMA user_version=22")


if __name__ == "__main__":
    unittest.main()
