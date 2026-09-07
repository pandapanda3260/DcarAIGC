"""Bounded, synthetic-only checks for the workbench's read-only projection."""
import sys
sys.dont_write_bytecode = True

import hashlib
import importlib.util
import json
from pathlib import Path
import sqlite3
import tempfile
import unittest
from unittest.mock import patch

SPEC = importlib.util.spec_from_file_location("content_thumbnails", Path(__file__).parents[1] / "server/content_thumbnails.py")
helper = importlib.util.module_from_spec(SPEC)
SPEC.loader.exec_module(helper)


class ThumbnailTests(unittest.TestCase):
    def setUp(self):
        self.temp = tempfile.TemporaryDirectory()
        self.addCleanup(self.temp.cleanup)
        self.root = Path(self.temp.name)
        self.db = self.root / "test.sqlite3"
        with sqlite3.connect(self.db) as c:
            c.executescript("""
                PRAGMA user_version=19;
                CREATE TABLE accounts(id INTEGER PRIMARY KEY,enabled INTEGER);
                CREATE TABLE content_items(id INTEGER PRIMARY KEY,platform TEXT,platform_content_id TEXT,account_id INTEGER);
                CREATE TABLE evidence_artifacts(id INTEGER PRIMARY KEY,content_id INTEGER,artifact_type TEXT,local_path TEXT,status TEXT,sha256 TEXT,byte_size INTEGER,metadata_json TEXT);
                CREATE TABLE provider_raw_responses(id INTEGER PRIMARY KEY,content_id INTEGER,account_id INTEGER,operation TEXT,local_path TEXT,sha256 TEXT,byte_size INTEGER,captured_at TEXT);
                INSERT INTO accounts VALUES(1,1),(2,0);
                INSERT INTO content_items VALUES(1,'douyin','111',1),(2,'douyin','222',1),(3,'xiaohongshu','333',1),(4,'douyin','444',2);
            """)

    def file(self, name, body):
        path = self.root / name
        path.parent.mkdir(parents=True, exist_ok=True)
        path.write_bytes(body)
        return path, hashlib.sha256(body).hexdigest(), len(body)

    def raw(self, row_id, content_id, body, account=1, operation="douyin_video_detail", compressed=False):
        encoded = json.dumps(body).encode()
        if compressed:
            import zstandard
            stored = zstandard.ZstdCompressor().compress(encoded)
            filename = f"raw-{row_id}.json.zst"
        else:
            stored = encoded
            filename = f"raw-{row_id}.json"
        path, sha, size = self.file(filename, stored)
        if compressed:
            metadata = {"schema": "provider-raw-sidecar-v1", "codec": "zstd", "raw_filename": filename,
                        "stored_sha256": sha, "stored_size": size, "entity_sha256": hashlib.sha256(encoded).hexdigest(), "entity_size": len(encoded)}
            path.with_name(filename + ".metadata.json").write_text(json.dumps(metadata))
        with sqlite3.connect(self.db) as c:
            c.execute("INSERT INTO provider_raw_responses VALUES(?,?,?,?,?,?,?,?)",
                      (row_id, content_id, account, operation, str(path), sha, size, "2026-09-06"))
        return sha

    def artifact(self, row_id, cid, kind, path, metadata=None, status="available"):
        body = path.read_bytes()
        with sqlite3.connect(self.db) as c:
            c.execute("INSERT INTO evidence_artifacts VALUES(?,?,?,?,?,?,?,?)", (row_id, cid, kind, str(path), status,
                      hashlib.sha256(body).hexdigest(), len(body), json.dumps(metadata or {})))

    def result(self, ids=(1,)):
        return helper.project(self.db, list(ids))["items"]

    def test_local_image_existing_missing_and_no_frame_endpoint(self):
        image, _, _ = self.file("images/1.jpg", b"\xff\xd8\xff" + b"a" * 50)
        manifest, _, _ = self.file("images/manifest.json", json.dumps({"image_paths": [str(image)]}).encode())
        self.artifact(1, 1, "media_manifest", manifest)
        self.assertEqual(self.result()["1"]["local_url"], "/api/v8/contents/1/evidence/files/1/0")
        image.unlink()
        self.assertIsNone(self.result()["1"]["local_url"])
        frame, _, _ = self.file("frames/frame-000.jpg", b"\xff\xd8\xff" + b"b" * 50)
        frames, _, _ = self.file("frames/manifest.json", json.dumps({"frames": [{"path": str(frame)}]}).encode())
        self.artifact(2, 1, "frames_manifest", frames)
        self.assertIsNone(self.result()["1"]["local_url"])

    def test_legacy_image_cannot_bypass_new_or_managed_source(self):
        image, _, _ = self.file("image.jpg", b"\xff\xd8\xff" + b"a" * 50)
        self.artifact(1, 1, "media", image, {"source_sha256": "old"})
        source, _, _ = self.file("source.json", b"{}")
        self.artifact(2, 1, "media_source", source, {"source_sha256": "new"})
        self.assertIsNone(self.result()["1"]["local_url"])
        with sqlite3.connect(self.db) as c:
            c.execute("DELETE FROM evidence_artifacts WHERE id=2")
        self.artifact(3, 1, "media_lifecycle_manifest", source, status="failed")
        self.assertIsNone(self.result()["1"]["local_url"])

    def test_cover_is_content_specific_not_avatar_or_collection(self):
        self.raw(1, 1, {"data": {"aweme_detail": {"aweme_id": "111", "author": {"cover": "https://example.com/avatar.jpg"},
            "mix_info": {"cover_url": {"url_list": ["https://example.com/mix.jpg"]}},
            "video": {"dynamic_cover": {"url_list": ["https://example.com/animated.webp"]}}}}})
        self.assertIsNone(self.result()["1"]["remote_url"])
        self.raw(2, 1, {"data": {"aweme_detail": {"aweme_id": "111", "video": {"cover": {"url_list": ["http://example.com/wrong.jpg", "https://example.com/right.jpg"]}}}}})
        self.assertEqual(self.result()["1"]["remote_url"], "https://example.com/right.jpg")

    def test_derived_parent_exact_identity_and_decode_once(self):
        parent = {"data": {"aweme_list": [
            {"aweme_id": "222", "video": {"cover": {"url_list": ["https://example.com/222.jpg"]}}},
            {"aweme_id": "111", "video": {"cover": {"url_list": ["https://example.com/111.jpg"]}}}]}}
        sha = self.raw(10, None, parent, operation="douyin_user_posts", compressed=True)
        for row_id, cid in [(11, 1), (12, 2)]:
            self.raw(row_id, cid, {"source_raw_response_id": 10, "source_sha256": sha, "source_captured_at": "2026-09-06", "data": {}})
        with patch.object(helper, "_read", wraps=helper._read) as read:
            result = self.result((1, 2))
        self.assertEqual(result["1"]["remote_url"], "https://example.com/111.jpg")
        self.assertEqual(result["2"]["remote_url"], "https://example.com/222.jpg")
        raw_reads = [c for c in read.call_args_list if str(c.args[0]).endswith("raw-10.json.zst")]
        self.assertEqual(len(raw_reads), 1)
        with sqlite3.connect(self.db) as c:
            c.execute("UPDATE provider_raw_responses SET account_id=2 WHERE id=10")
        self.assertIsNone(self.result()["1"]["remote_url"])

    def test_xhs_thumbnail_and_paused_account_omission(self):
        self.raw(1, 3, {"notes": [{"note_id": "333", "video_info_v2": {"image": {"thumbnail": "https://example.com/xhs.jpg"}}}]}, operation="xiaohongshu_note_detail")
        result = self.result((3, 4, 999))
        self.assertEqual(result["3"]["remote_url"], "https://example.com/xhs.jpg")
        self.assertNotIn("4", result)
        self.assertNotIn("999", result)

    def test_bad_compression_missing_and_oversized_receipt_fail_closed(self):
        self.raw(1, 1, {"aweme_id": "111"}, compressed=True)
        path = self.root / "raw-1.json.zst"
        path.write_bytes(b"not-zstd")
        sha = hashlib.sha256(path.read_bytes()).hexdigest()
        with sqlite3.connect(self.db) as c:
            c.execute("UPDATE provider_raw_responses SET sha256=?,byte_size=? WHERE id=1", (sha, 8))
        side = path.with_name(path.name + ".metadata.json")
        metadata = json.loads(side.read_text())
        metadata.update(stored_sha256=sha, stored_size=8)
        side.write_text(json.dumps(metadata))
        self.assertIsNone(self.result()["1"]["remote_url"])
        metadata["entity_size"] = helper.MAX_ENTITY + 1
        side.write_text(json.dumps(metadata))
        self.assertIsNone(self.result()["1"]["remote_url"])
        path.unlink()
        self.assertIsNone(self.result()["1"]["remote_url"])

    def test_relative_paths_use_explicit_root_from_unrelated_cwd(self):
        self.raw(1, 1, {"aweme_id": "111", "video": {"cover": {"url_list": ["https://example.com/1.jpg"]}}}, compressed=True)
        image, _, _ = self.file("images/1.jpg", b"\xff\xd8\xff" + b"a" * 50)
        manifest, _, _ = self.file("images/manifest.json", json.dumps({"image_paths": ["images/1.jpg"]}).encode())
        self.artifact(1, 1, "media_manifest", manifest)
        with sqlite3.connect(self.db) as c:
            c.execute("UPDATE provider_raw_responses SET local_path='raw-1.json.zst'")
            c.execute("UPDATE evidence_artifacts SET local_path='images/manifest.json'")
        with tempfile.TemporaryDirectory() as other, patch.object(helper.Path, "cwd", return_value=Path(other)):
            result = helper.project(self.db, [1], project_root=self.root)["items"]["1"]
        self.assertEqual(result["remote_url"], "https://example.com/1.jpg")
        self.assertEqual(result["local_url"], "/api/v8/contents/1/evidence/files/1/0")

    def test_ids_and_schema_validation(self):
        for value in ["", "0", "-1", "1,0", "1;DROP TABLE", "1, 2", ",".join(["1"] * 101)]:
            with self.subTest(value=value), self.assertRaises(helper.ReadError):
                helper.parse_ids(value)
        self.assertEqual(helper.parse_ids("1,2,1"), [1, 2])
        with sqlite3.connect(self.db) as c:
            c.execute("PRAGMA user_version=18")
        with self.assertRaises(helper.ReadError):
            self.result()

    def test_exhausted_budget_stops_subsequent_file_reads(self):
        reader = helper.Reader(self.root)
        reader.used = helper.MAX_TOTAL
        row = {"local_path": "unused.json", "sha256": "0" * 64, "byte_size": 2}
        with patch.object(helper, "_read", side_effect=AssertionError("must not read")):
            self.assertIsNone(reader.json(row, raw=True))

    def test_cover_prefers_existing_browser_format_without_rewriting_signed_url(self):
        signed_jpeg = "https://example.com/cover.jpeg?signature=existing%2Fsignature&expires=123"
        self.raw(1, 1, {"aweme_id": "111", "video": {"cover": {"url_list": [
            "https://example.com/cover.heic?signature=heic",
            "https://example.com/opaque-image?signature=unknown",
            signed_jpeg,
        ]}}})
        self.assertEqual(self.result()["1"]["remote_url"], signed_jpeg)
        for suffix in ("jpg", "jpeg", "png", "webp", "gif", "avif"):
            candidate = f"https://example.com/cover.{suffix}?signature=kept"
            self.assertEqual(helper._https(["https://example.com/opaque", candidate]), candidate)

    def test_heic_only_cover_uses_existing_origin_or_no_image(self):
        content = {"platform": "douyin", "platform_content_id": "111"}
        body = {"aweme_id": "111", "video": {
            "cover": {"url_list": ["https://example.com/cover.HEIC?signature=kept"]},
            "origin_cover": {"url_list": ["https://example.com/origin.heif", "https://example.com/origin.webp"]},
        }}
        self.assertEqual(helper.cover_url(body, content), "https://example.com/origin.webp")
        body["video"]["cover"]["url_list"] = ["https://example.com/opaque?signature=kept"]
        self.assertEqual(helper.cover_url(body, content), "https://example.com/origin.webp")
        body["video"]["cover"]["url_list"] = ["https://example.com/cover.heic"]
        body["video"]["origin_cover"]["url_list"] = ["https://example.com/origin.heif"]
        self.assertIsNone(helper.cover_url(body, content))
        self.assertEqual(helper._https(["https://example.com/cover.heic", "https://example.com/opaque?signature=kept"]),
                         "https://example.com/opaque?signature=kept")

    def test_projection_opens_readonly_without_artifacts_or_database_changes(self):
        self.raw(1, 1, {"aweme_id": "111", "video": {"cover": {"url_list": ["https://example.com/1.jpg"]}}})
        before = {str(p.relative_to(self.root)): p.read_bytes() for p in self.root.rglob("*") if p.is_file()}
        original_connect = sqlite3.connect
        calls = []
        def connect(database_uri, **kwargs):
            calls.append((database_uri, kwargs))
            return original_connect(database_uri, **kwargs)
        with patch.object(helper.sqlite3, "connect", side_effect=connect):
            self.result()
        after = {str(p.relative_to(self.root)): p.read_bytes() for p in self.root.rglob("*") if p.is_file()}
        self.assertEqual(before, after)
        self.assertTrue(calls[0][0].endswith("?mode=ro"))
        self.assertTrue(calls[0][1]["uri"])


if __name__ == "__main__":
    unittest.main()
