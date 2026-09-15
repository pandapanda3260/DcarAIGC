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

    def raw(self, row_id, content_id, body, account=1, operation="douyin_video_detail", compressed=False,
            captured_at="2026-09-06"):
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
                      (row_id, content_id, account, operation, str(path), sha, size, captured_at))
        return sha

    def artifact(self, row_id, cid, kind, path, metadata=None, status="available"):
        body = path.read_bytes()
        with sqlite3.connect(self.db) as c:
            c.execute("INSERT INTO evidence_artifacts VALUES(?,?,?,?,?,?,?,?)", (row_id, cid, kind, str(path), status,
                      hashlib.sha256(body).hexdigest(), len(body), json.dumps(metadata or {})))

    def result(self, ids=(1,)):
        return helper.project(self.db, list(ids))["items"]

    def test_online_projection_never_reads_local_images_manifests_or_frames(self):
        image, _, _ = self.file("images/1.jpg", b"\xff\xd8\xff" + b"a" * 50)
        manifest, _, _ = self.file("images/manifest.json", json.dumps({"image_paths": [str(image)]}).encode())
        self.artifact(1, 1, "media_manifest", manifest)
        frame, _, _ = self.file("frames/frame-000.jpg", b"\xff\xd8\xff" + b"b" * 50)
        frames, _, _ = self.file("frames/manifest.json", json.dumps({"frames": [{"path": str(frame)}]}).encode())
        self.artifact(2, 1, "frames_manifest", frames)
        with patch.object(helper, "_read", side_effect=AssertionError("must not read media")):
            self.assertEqual(self.result()["1"], {"local_url": None, "remote_url": None,
                                                 "remote_urls": [], "reason": "not_found"})

    def test_discovery_only_pages_supply_exact_online_covers_without_detail(self):
        self.raw(10, None, {"data": {"aweme_list": [
            {"aweme_id": "111", "images": [{"url_list": ["https://example.com/111.jpg"]}]},
            {"aweme_id": "222", "video": {"cover": {"url_list": ["https://example.com/222.jpg"]}}},
        ]}}, operation="douyin_user_posts", compressed=True)
        # The newest page omits both works. The earlier saved page still applies.
        self.raw(11, None, {"data": {"aweme_list": [{"aweme_id": "999",
            "video": {"cover": {"url_list": ["https://example.com/wrong.jpg"]}}}]}}, operation="douyin_user_posts")
        with patch.object(helper, "_read", wraps=helper._read) as read:
            result = self.result((1, 2))
        self.assertEqual(result["1"], {"local_url": None, "remote_url": "https://example.com/111.jpg",
                                      "remote_urls": ["https://example.com/111.jpg"], "reason": None})
        self.assertEqual(result["2"]["remote_url"], "https://example.com/222.jpg")
        self.assertEqual(sum(str(call.args[0]).endswith("raw-10.json.zst") for call in read.call_args_list), 1)

    def test_newest_saved_cover_wins_across_detail_and_discovery(self):
        def cover(url):
            return {"aweme_id": "111", "video": {"cover": {"url_list": [url]}}}
        self.raw(20, 1, cover("https://example.com/old.jpg"), captured_at="2026-09-05T00:00:00Z")
        self.raw(10, None, {"data": [cover("https://example.com/new.jpg")]},
                 operation="douyin_user_posts", captured_at="2026-09-06T00:00:00Z")
        self.assertEqual(self.result()["1"]["remote_url"], "https://example.com/new.jpg")
        # A newer failed/empty result does not erase a valid saved URL.
        self.raw(30, 1, {"data": {}}, captured_at="2026-09-07T00:00:00Z")
        self.assertEqual(self.result()["1"]["remote_url"], "https://example.com/new.jpg")
        self.raw(31, 1, cover("https://example.com/latest.jpg"), captured_at="2026-09-08T00:00:00Z")
        self.assertEqual(self.result()["1"]["remote_url"], "https://example.com/latest.jpg")

    def test_wrong_account_content_and_operation_are_not_cover_sources(self):
        body = {"aweme_id": "111", "video": {"cover": {"url_list": ["https://example.com/wrong.jpg"]}}}
        self.raw(1, None, body, account=2, operation="douyin_user_posts")
        self.raw(2, 1, body, account=2)
        self.raw(3, 2, body)
        self.raw(4, None, body, operation="xiaohongshu_user_posts")
        self.assertIsNone(self.result()["1"]["remote_url"])

    def test_content_bound_detail_survives_more_than_one_thousand_newer_account_pages(self):
        # Real content detail rows have a content owner and a NULL account_id.
        self.raw(1, 1, {"aweme_id": "111", "video": {"cover": {"url_list": ["https://example.com/detail.jpg"]}}},
                 account=None, captured_at="2026-09-05T00:00:00Z")
        self.raw(2, None, {"data": []}, operation="douyin_user_posts", captured_at="2026-09-06T00:00:00Z")
        with sqlite3.connect(self.db) as c:
            row = c.execute("SELECT * FROM provider_raw_responses WHERE id=2").fetchone()
            c.executemany("INSERT INTO provider_raw_responses VALUES(?,?,?,?,?,?,?,?)",
                          [(i, *row[1:]) for i in range(3, 1005)])
        result = self.result()["1"]
        self.assertEqual(result, {"local_url": None, "remote_url": "https://example.com/detail.jpg",
                                  "remote_urls": ["https://example.com/detail.jpg"], "reason": None})

    def test_kuaishou_saved_page_uses_exact_photo_and_url_not_cdn_label(self):
        with sqlite3.connect(self.db) as c:
            c.execute("INSERT INTO content_items VALUES(6,'kuaishou','666',1)")
        signed = "http://zj66.a.kwimgs.com/cover.jpg?signature=kept%2Fexact&expires=123"
        self.raw(10, None, {"data": {"feeds": [
            {"photo_id": 999, "cover_thumbnail_urls": [{"url": "https://example.com/other.jpg"}]},
            {"photo_id": 666, "cover_thumbnail_urls": [
                {"cdn": "zj66.a.kwimgs.com", "url": "http://user@unverified.example.com/ignored.jpg"},
                {"cdn": "untrusted.example.com", "url": signed}]}]}}, operation="kuaishou_user_posts")
        self.assertEqual(self.result((6,))["6"], {"local_url": None, "remote_url": "https:" + signed[5:],
                                                "remote_urls": ["https:" + signed[5:]], "reason": None})
        with sqlite3.connect(self.db) as c:
            c.execute("UPDATE provider_raw_responses SET account_id=2 WHERE id=10")
        self.assertIsNone(self.result((6,))["6"]["remote_url"])

    def test_kuaishou_saved_webp_covers_try_https_without_host_inventory_or_signature_changes(self):
        for cid, host in enumerate(("ty2.a.kwimgs.com", "hw2.a.kwimgs.com", "tx2.a.yximgs.com",
                                    "ws2.a.kwimgs.com", "tx2.a.kwimgs.com", "cc2.xyydnode.com",
                                    "dl-bv2.currylb.cn", "future-cdn.example.net"), start=6):
            with self.subTest(host=host):
                photo_id = str(cid * 111)
                with sqlite3.connect(self.db) as c:
                    c.execute("INSERT INTO content_items VALUES(?,'kuaishou',?,1)", (cid, photo_id))
                signed = f"http://{host}/upic/2026/09/12/{photo_id}_cover.webp?signature=kept%2Fexact%2Bvalue&expires=123"
                self.raw(cid + 10, None, {"data": {"feeds": [
                    {"photo_id": photo_id, "cover_thumbnail_urls": [{"url": signed}]}]}},
                    operation="kuaishou_user_posts")
                self.assertEqual(self.result((cid,))[str(cid)],
                                 {"local_url": None, "remote_url": "https:" + signed[5:],
                                  "remote_urls": ["https:" + signed[5:]], "reason": None})

    def test_wechat_saved_page_uses_exact_object_media_cover_without_avatar(self):
        with sqlite3.connect(self.db) as c:
            c.execute("INSERT INTO content_items VALUES(7,'wechat_channels','777',1)")
        body = {"data": {"object": [
            {"id": 999, "objectDesc": {"media": [{"fullCoverUrl": "https://example.com/wrong.jpg"}]}},
            {"id": 777, "contact": {"coverUrl": "https://example.com/avatar.jpg"},
             "objectDesc": {"media": [{"fullCoverUrl": "https://example.com/full.jpg?signature=kept",
                                         "coverUrl": "https://example.com/small.jpg"}]}}]}}
        self.raw(10, None, body, operation="wechat_channels_user_posts")
        self.assertEqual(self.result((7,))["7"], {"local_url": None, "remote_url": "https://example.com/full.jpg?signature=kept",
                                                "remote_urls": ["https://example.com/full.jpg?signature=kept",
                                                                "https://example.com/small.jpg"], "reason": None})
        item = body["data"]["object"][1]
        item["objectId"] = 888
        self.assertIsNone(helper.cover_url(body, {"platform": "wechat_channels", "platform_content_id": "777"}))

    def test_kuaishou_app_only_cover_does_not_use_avatar_or_rewrite_its_format(self):
        body = {"photo_id": 666, "cover_thumbnail_urls": [{"url": "http://hw.a.yximgs.com/cover.kvif?signature=exact"}],
                "override_cover_thumbnail_urls": [{"url": "https://hw.a.yximgs.com/cover.kpg?signature=exact"}],
                "headurls": [{"url": "https://hw.a.yximgs.com/avatar.jpg"}],
                "soundTrack": {"imageUrls": [{"url": "https://hw.a.yximgs.com/music.webp"}]}}
        self.assertIsNone(helper.cover_url(body, {"platform": "kuaishou", "platform_content_id": "666"}))

    def test_saved_candidates_are_deduplicated_bounded_and_keep_browser_format_preference(self):
        body = {"photo_id": "666", "cover_thumbnail_urls": [
            {"url": "http://cdn.example.net/opaque?token=exact"},
            {"url": "http://cdn.example.net/a.webp?token=exact%2Bvalue"},
            {"url": "https://cdn.example.net/a.webp?token=exact%2Bvalue"},
            {"url": "http://backup.example.net/b.webp"}],
            "override_cover_thumbnail_urls": [{"url": "http://third.example.net/c.jpg"},
                                               {"url": "http://fourth.example.net/d.png"}]}
        with sqlite3.connect(self.db) as c:
            c.execute("INSERT INTO content_items VALUES(6,'kuaishou','666',1)")
        self.raw(10, None, {"data": {"feeds": [body]}}, operation="kuaishou_user_posts")
        expected = ["https://cdn.example.net/a.webp?token=exact%2Bvalue",
                    "https://backup.example.net/b.webp", "https://third.example.net/c.jpg"]
        self.assertEqual(self.result((6,))["6"], {"local_url": None, "remote_url": expected[0],
                                                "remote_urls": expected, "reason": None})
        body["override_cover_thumbnail_urls"] = []
        urls, reason = helper.cover_candidates(body, {"platform": "kuaishou", "platform_content_id": "666"})
        self.assertEqual(urls, expected[:2] + ["https://cdn.example.net/opaque?token=exact"])
        self.assertIsNone(reason)

    def test_cover_urls_reject_malformed_values_without_losing_valid_backups(self):
        bad = ["http://user:secret@cdn.example/cover.webp", "http://@cdn.example/cover.webp",
               "http://cdn.example:80/cover.webp", "http://cdn.example:/cover.webp",
               "https://cdn.example:444/cover.webp", "https://cdn.example:99999/cover.webp",
               "https://cdn.example:/cover.webp", "//cdn.example/cover.webp", "javascript:alert(1)",
               "http:cdn.example/cover.webp", "http:///cover.webp", "https://bad_host.example/a.webp",
               "http://cdn.example/a\\b.webp", "http://cdn.example/a\nb.webp",
               " http://cdn.example/cover.webp", "http://cdn.example/a\x00b.webp",
               "http://cdn.example/a\u00a0b.webp", "http://cdn.example/a\u2028b.webp",
               "https://cdn.example/" + "x" * 4096, "https://cdn.example/" + "图" * 1400,
               "http://cdn.example/" + "x" * (4096 - len("http://cdn.example/"))]
        content = {"platform": "kuaishou", "platform_content_id": "666"}
        for value in bad:
            with self.subTest(value=repr(value[:100])):
                body = {"photo_id": "666", "cover_thumbnail_urls": [{"url": value}]}
                self.assertEqual(helper.cover_candidates(body, content), ([], "source_unavailable"))
                body["cover_thumbnail_urls"].append({"url": "http://future.example.org/good.webp?sig=%2Fkept"})
                self.assertEqual(helper.cover_candidates(body, content),
                                 (["https://future.example.org/good.webp?sig=%2Fkept"], None))
        signed = "HTTPS://cdn.example:443/cover.webp?sig=exact%2Fvalue"
        self.assertEqual(helper._https(signed), signed)
        self.assertIsNone(helper._https("http://cdn.example/cover.webp"))
        boundary = "https://cdn.example/" + "x" * (4096 - len("https://cdn.example/"))
        self.assertEqual(helper._https(boundary), boundary)

    def test_latest_valid_source_keeps_its_candidates_when_new_detail_is_app_only(self):
        with sqlite3.connect(self.db) as c:
            c.execute("INSERT INTO content_items VALUES(6,'kuaishou','666',1)")
        old = {"photo_id": "666", "cover_thumbnail_urls": [{"url": "http://cdn.example/old.webp"},
                                                              {"url": "http://backup.example/old.webp"}]}
        self.raw(10, None, {"data": {"feeds": [old]}}, operation="kuaishou_user_posts", captured_at="2026-09-05")
        self.raw(11, 6, {"photo_id": "666", "cover_thumbnail_urls": [{"url": "http://cdn.example/new.kpg"}]},
                 operation="kuaishou_video_detail", captured_at="2026-09-06")
        result = self.result((6,))["6"]
        self.assertEqual(result["remote_urls"], ["https://cdn.example/old.webp", "https://backup.example/old.webp"])
        self.assertIsNone(result["reason"])
        self.raw(12, 6, {"photo_id": "666", "cover_thumbnail_urls": [{"url": "http://cdn.example/new.webp"}]},
                 operation="kuaishou_video_detail", captured_at="2026-09-07")
        self.assertEqual(self.result((6,))["6"]["remote_urls"], ["https://cdn.example/new.webp"])

    def test_missing_unreadable_and_unsupported_sources_have_distinct_reasons(self):
        with sqlite3.connect(self.db) as c:
            c.execute("INSERT INTO content_items VALUES(6,'kuaishou','666',1)")
        self.assertEqual(self.result((6,))["6"]["reason"], "not_found")
        self.raw(10, 6, {"photo_id": "666"}, operation="kuaishou_video_detail")
        (self.root / "raw-10.json").write_text("corrupt")
        self.assertEqual(self.result((6,))["6"], {"local_url": None, "remote_url": None,
                                                "remote_urls": [], "reason": "source_unavailable"})
        self.raw(11, 6, {"photo_id": "666", "headurls": [{"url": "https://cdn.example/avatar.jpg"}]},
                 operation="kuaishou_video_detail", captured_at="2026-09-05")
        self.assertEqual(self.result((6,))["6"]["reason"], "not_found")
        self.raw(12, 6, {"photo_id": "666", "cover_thumbnail_urls": [{"url": "http://cdn.example/a.kvif"}],
                         "ff_cover_thumbnail_urls": [{"url": "http://backup.example/a.kpg"}]},
                 operation="kuaishou_video_detail", captured_at="2026-09-04")
        self.assertEqual(self.result((6,))["6"]["reason"], "unsupported_format")

    def test_unrelated_page_work_cannot_hide_an_unreadable_detail(self):
        self.raw(10, 1, {"aweme_id": "111"})
        (self.root / "raw-10.json").write_text("corrupt")
        self.raw(11, None, {"data": [{"aweme_id": "222", "video": {
            "cover": {"url_list": ["https://cdn.example/other.jpg"]}}}]}, operation="douyin_user_posts")
        self.assertEqual(self.result()["1"]["reason"], "source_unavailable")
        self.raw(12, None, {"data": [{"aweme_id": "111"}]}, operation="douyin_user_posts")
        self.assertEqual(self.result()["1"]["reason"], "not_found")

    def test_derived_source_receipt_mismatch_never_supplies_candidates(self):
        sha = self.raw(10, None, {"data": [{"aweme_id": "111", "video": {
            "cover": {"url_list": ["https://cdn.example/a.jpg", "https://backup.example/a.jpg"]}}}]},
            operation="douyin_user_posts")
        self.raw(11, 1, {"source_raw_response_id": 10, "source_sha256": "0" * 64,
                         "source_captured_at": "2026-09-06"})
        # Isolate the derived route; the independent legitimate page is not in this read window.
        with patch.object(helper, "MAX_RAW_ROWS", 0):
            self.assertEqual(self.result()["1"]["reason"], "source_unavailable")
            self.assertEqual(self.result()["1"]["remote_urls"], [])
            self.raw(12, 1, {"source_raw_response_id": 10, "source_sha256": sha,
                             "source_captured_at": "2026-09-06"})
            self.assertEqual(self.result()["1"]["remote_urls"],
                             ["https://cdn.example/a.jpg", "https://backup.example/a.jpg"])

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
        self.assertIsNone(result["local_url"])

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
