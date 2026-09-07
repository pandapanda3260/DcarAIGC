"""Default Douyin image queues use verified local raw, never a paid refetch."""

from __future__ import annotations

import hashlib
import io
import json
import tempfile
import unittest
from pathlib import Path
from typing import Any
from unittest.mock import patch

from v8 import media, providers
from v8.storage import connect, initialize_database, now_utc


class ImageResponse(io.BytesIO):
    def __init__(self, body: bytes, url: str) -> None:
        super().__init__(body)
        self.headers = {"Content-Length": str(len(body))}
        self.url = url

    def geturl(self) -> str:
        return self.url


class V8MediaPipelineGroupsTest(unittest.TestCase):
    def setUp(self) -> None:
        self.temp = tempfile.TemporaryDirectory()
        self.addCleanup(self.temp.cleanup)
        self.root = Path(self.temp.name).resolve()
        self.db = self.root / "media-groups.sqlite3"
        self.media_root = self.root / "media"
        self.calls: list[str] = []
        self.ocr_counts: list[int] = []
        self.urls = [
            f"https://p3-sign.douyinpic.com/frozen-candidate-{index}.bin"
            for index in range(6)
        ]
        self.uid = "100001"
        self.aweme_id = "9000000000000000001"
        self.at = now_utc()
        with connect(self.db) as connection:
            initialize_database(connection)
            connection.executemany(
                "INSERT INTO accounts(id,phone,created_at,updated_at) VALUES (?, '', ?, ?)",
                [(1, self.at, self.at), (2, self.at, self.at)],
            )
            connection.execute(
                """INSERT INTO account_platform_identities(
                    account_id,platform,uid,created_at,updated_at
                ) VALUES (1,'douyin',?,?,?)""",
                (self.uid, self.at, self.at),
            )
            connection.execute(
                """INSERT INTO content_items(
                    id,link_id,platform,platform_content_id,canonical_url,account_id,
                    raw_account_uid,title,content_type,published_at,imported_at,created_at,updated_at
                ) VALUES (1,'M1EDIA','douyin',?,'https://www.douyin.com/note/fixture',1,
                          ?,'fixture','image',?,?,?,?)""",
                (self.aweme_id, self.uid, self.at, self.at, self.at, self.at),
            )
            connection.commit()
        self._patch(media, "MEDIA_ROOT", self.media_root)
        self._patch(media.urllib.request, "urlopen", side_effect=self._open_image)
        self._patch(providers, "_request_json", side_effect=AssertionError("paid network forbidden"))
        self._patch(media, "snapshot_download", side_effect=AssertionError("model download forbidden"))
        self._patch(media, "compile_ocr_binary", return_value=self.root / "fixture-ocr")
        self._patch(media, "_run_ocr", side_effect=self._ocr)

    def _patch(self, target: Any, name: str, *args: Any, **kwargs: Any) -> Any:
        patcher = patch.object(target, name, *args, **kwargs)
        result = patcher.start()
        self.addCleanup(patcher.stop)
        return result

    def _open_image(self, request: Any, **_kwargs: Any) -> ImageResponse:
        url = request.full_url
        self.assertIn(url, self.urls)
        self.calls.append(url)
        # The first candidate is unusable; the second belongs to that same image.
        body = b"unsupported" * 100 if url == self.urls[0] else b"\xff\xd8\xff" + b"J" * 700
        return ImageResponse(body, url)

    def _ocr(self, manifest: Path, output: Path, **_kwargs: Any) -> Path:
        body = json.loads(manifest.read_bytes())
        count = len(body["frames"])
        self.ocr_counts.append(count)
        media._atomic_json(output, {
            "status": "success", "processor_version": media.processor_versions()["ocr"],
            "combined_text": "fixture car evidence", "source_count": count,
            "ocr_observation_count": count,
            "observations": [{"text": "fixture car evidence"} for _ in range(count)],
        })
        return output

    def _raw(
        self, body: dict[str, Any], *, operation: str,
        account_id: int | None, content_id: int | None, source: str = "live_applied",
    ) -> int:
        encoded = json.dumps(body, sort_keys=True).encode()
        with connect(self.db) as connection:
            sequence = connection.execute("SELECT COALESCE(MAX(id),0)+1 FROM provider_raw_responses").fetchone()[0]
            path = self.root / f"raw-{sequence}.json"
            path.write_bytes(encoded)
            cursor = connection.execute(
                """INSERT INTO provider_raw_responses(
                    account_id,content_id,provider,operation,local_path,sha256,
                    byte_size,http_status,captured_at,source
                ) VALUES (?,?,'TikHub',?,?,?,?,200,?,?)""",
                (account_id, content_id, operation, str(path), hashlib.sha256(encoded).hexdigest(),
                 len(encoded), self.at, source),
            )
            self.assertIsNotNone(cursor.lastrowid)
            raw_id = int(cursor.lastrowid or 0)
            connection.commit()
        return raw_id

    def _source(self, kind: str = "detail") -> dict[str, Any]:
        aweme = {
            "aweme_id": self.aweme_id, "author": {"uid": self.uid}, "desc": "fixture",
            "images": [
                {"download_url_list": [self.urls[index]], "url_list": [self.urls[index + 1]]}
                for index in range(0, len(self.urls), 2)
            ],
        }
        payload: dict[str, Any] = {"code": 200, "data": {
            "status_code": 0,
            **({"aweme_detail": aweme} if kind == "detail" else {"aweme_list": [aweme]}),
        }}
        raw_id = self._raw(
            payload,
            operation="douyin_video_detail" if kind == "detail" else "douyin_user_posts",
            content_id=1 if kind == "detail" else None,
            account_id=None if kind == "detail" else 1,
        )
        source_raw_id = raw_id
        if kind == "derived":
            with connect(self.db) as connection:
                original = connection.execute("SELECT * FROM provider_raw_responses WHERE id=?", (raw_id,)).fetchone()
            source_raw_id = self._raw({
                "stage": "detail", "data": {
                    "content_type": "image", "account_uid": self.uid,
                    "media_urls": self.urls, "title": "fixture", "body": "fixture",
                    "_evidence_captured_at": self.at,
                },
                "derived_from_operation": "douyin_user_posts",
                "source_raw_response_id": raw_id, "source_sha256": original["sha256"],
                "source_captured_at": self.at,
            }, operation="douyin_video_detail", account_id=None, content_id=1, source="derived_applied")
        artifact = media.store_media_source_manifest(
            1, media_kind="image", urls=self.urls, raw_response_id=source_raw_id,
            db_path=self.db, media_root=self.media_root,
        )
        self.assertIsNotNone(artifact)
        assert artifact is not None
        return {"raw_id": raw_id, "source_raw_id": source_raw_id,
                "payload": payload, "aweme": aweme, "artifact": artifact}

    def _rewrite(self, raw_id: int, payload: dict[str, Any]) -> None:
        encoded = json.dumps(payload, sort_keys=True).encode()
        with connect(self.db) as connection:
            path = Path(connection.execute("SELECT local_path FROM provider_raw_responses WHERE id=?", (raw_id,)).fetchone()[0])
            path.write_bytes(encoded)
            connection.execute("UPDATE provider_raw_responses SET sha256=?,byte_size=? WHERE id=?",
                               (hashlib.sha256(encoded).hexdigest(), len(encoded), raw_id))
            connection.commit()

    def _assert_queue_success(self, kind: str) -> None:
        fixture = self._source(kind)
        source_path = media._resolved(fixture["artifact"].local_path)
        source_bytes = source_path.read_bytes()
        with connect(self.db) as connection:
            raw_count = connection.execute("SELECT COUNT(*) FROM provider_raw_responses").fetchone()[0]
        downloaded = media.run_media_download_queue(db_path=self.db, max_workers=1)
        self.assertEqual(downloaded["candidates"], 1, downloaded)
        self.assertEqual(downloaded["downloaded"], 1, downloaded)
        processed = media.run_media_processing_queue(db_path=self.db)
        self.assertEqual(processed["evidence_ready"], 1, processed)
        self.assertEqual(self.calls, [self.urls[0], self.urls[1], self.urls[2], self.urls[4]])
        self.assertEqual(self.ocr_counts, [3])
        self.assertEqual(media.run_media_download_queue(db_path=self.db)["candidates"], 0)
        self.assertEqual(media.run_media_processing_queue(db_path=self.db)["candidates"], 0)
        self.assertEqual(source_path.read_bytes(), source_bytes)
        with connect(self.db) as connection:
            self.assertEqual(connection.execute("SELECT COUNT(*) FROM provider_usage").fetchone()[0], 0)
            self.assertEqual(connection.execute("SELECT COUNT(*) FROM provider_raw_responses").fetchone()[0], raw_count)
            rows = connection.execute("SELECT * FROM evidence_artifacts WHERE artifact_type='media_manifest'").fetchall()
        self.assertEqual(len(rows), 1)
        manifest = json.loads(media._resolved(rows[0]["local_path"]).read_bytes())
        self.assertEqual(manifest["source_count"], 3)
        self.assertEqual(manifest["source_url_count"], 6)
        self.assertEqual(len(manifest["groups"]), 3)
        self.assertTrue(all(media._resolved(path).is_file() for path in manifest["image_paths"]))

    def test_default_queue_downloads_all_groups_from_detail_raw(self) -> None:
        self._assert_queue_success("detail")

    def test_default_queue_downloads_all_groups_from_account_discovery_raw(self) -> None:
        self._assert_queue_success("discovery")

    def test_default_queue_resolves_local_derived_detail_to_discovery_raw(self) -> None:
        self._assert_queue_success("derived")

    def _assert_refused(self) -> None:
        result = media.run_media_download_queue(db_path=self.db, max_workers=1)
        self.assertEqual(result["candidates"], 1, result)
        self.assertEqual(result["failed"], 1, result)
        self.assertEqual(result["downloaded"], 0, result)
        self.assertEqual(self.calls, [])
        with connect(self.db) as connection:
            self.assertEqual(connection.execute("SELECT COUNT(*) FROM evidence_artifacts WHERE artifact_type='media_manifest'").fetchone()[0], 0)
            self.assertEqual(connection.execute("SELECT COUNT(*) FROM provider_usage").fetchone()[0], 0)

    def test_corrupt_raw_bytes_or_size_reject_before_download(self) -> None:
        for mutation in ("bytes", "size"):
            with self.subTest(mutation=mutation):
                fixture = self._source()
                with connect(self.db) as connection:
                    row = connection.execute("SELECT * FROM provider_raw_responses WHERE id=?", (fixture["raw_id"],)).fetchone()
                    if mutation == "bytes":
                        Path(row["local_path"]).write_bytes(b"corrupt")
                    else:
                        connection.execute("UPDATE provider_raw_responses SET byte_size=byte_size+1 WHERE id=?", (fixture["raw_id"],))
                    connection.commit()
                self._assert_refused()

    def test_raw_identity_business_errors_and_incomplete_groups_reject(self) -> None:
        for mutation in ("uid", "work", "missing_group", "empty_group", "invalid_candidate", "duplicate_work", "provider_code", "upstream_code"):
            with self.subTest(mutation=mutation):
                fixture = self._source("discovery")
                aweme = fixture["aweme"]
                payload = fixture["payload"]
                if mutation == "uid":
                    aweme["author"]["uid"] = "different-author"
                elif mutation == "work":
                    aweme["aweme_id"] = "different-work"
                elif mutation == "missing_group":
                    aweme["images"].pop()
                elif mutation == "empty_group":
                    aweme["images"].append({"download_url_list": [], "url_list": []})
                elif mutation == "invalid_candidate":
                    aweme["images"][0]["url_list"].append(123)
                elif mutation == "duplicate_work":
                    payload["data"]["aweme_list"].append(aweme)
                elif mutation == "provider_code":
                    payload["code"] = 500
                else:
                    payload["data"]["status_code"] = 5
                self._rewrite(fixture["raw_id"], payload)
                self._assert_refused()

    def test_wrong_raw_account_content_provider_or_http_reject(self) -> None:
        for column, value in (("account_id", 2), ("content_id", None), ("provider", "Other"), ("http_status", 403)):
            with self.subTest(column=column):
                fixture = self._source()
                with connect(self.db) as connection:
                    connection.execute(f"UPDATE provider_raw_responses SET {column}=? WHERE id=?", (value, fixture["raw_id"]))
                    connection.commit()
                self._assert_refused()

    def test_derived_raw_cannot_substitute_discovery_hash(self) -> None:
        fixture = self._source("derived")
        with connect(self.db) as connection:
            row = connection.execute("SELECT local_path FROM provider_raw_responses WHERE id=?", (fixture["source_raw_id"],)).fetchone()
        body = json.loads(Path(row[0]).read_bytes())
        body["source_sha256"] = "0" * 64
        self._rewrite(fixture["source_raw_id"], body)
        self._assert_refused()

    def test_cached_download_still_revalidates_raw_before_ocr(self) -> None:
        fixture = self._source()
        self.assertEqual(media.run_media_download_queue(db_path=self.db)["downloaded"], 1)
        with connect(self.db) as connection:
            row = connection.execute("SELECT local_path FROM provider_raw_responses WHERE id=?", (fixture["raw_id"],)).fetchone()
        Path(row[0]).write_bytes(b"changed after download")
        calls = list(self.calls)
        result = media.run_media_processing_queue(db_path=self.db)
        self.assertEqual(result["failed"], 1, result)
        self.assertEqual(self.calls, calls)
        self.assertEqual(self.ocr_counts, [])


if __name__ == "__main__":
    unittest.main()
