from __future__ import annotations

import json
import subprocess
import tempfile
import unittest
from pathlib import Path
from unittest.mock import patch

from v8 import media
from v8.storage import connect, initialize_database, now_utc


class V8MediaTest(unittest.TestCase):
    def setUp(self) -> None:
        self.temp = tempfile.TemporaryDirectory()
        self.root = Path(self.temp.name)
        self.db = self.root / "media.sqlite3"
        captured_at = now_utc()
        with connect(self.db) as connection:
            initialize_database(connection)
            connection.execute(
                """
                INSERT INTO content_items(
                    link_id, platform, platform_content_id, canonical_url, title,
                    content_type, imported_at, created_at, updated_at
                ) VALUES ('A2BC3D', 'xiaohongshu', 'abc123',
                          'https://www.xiaohongshu.com/explore/abc123', '', 'video', ?, ?, ?)
                """,
                (captured_at, captured_at, captured_at),
            )
            connection.execute(
                """
                INSERT INTO review_queue(
                    content_id, reason_code, status, created_at, updated_at
                ) VALUES (1, 'stale_local_evidence', 'pending', ?, ?)
                """,
                (captured_at, captured_at),
            )
            connection.commit()
        self.video = self.root / "video.mp4"
        subprocess.run(
            [
                "ffmpeg", "-y", "-nostdin", "-loglevel", "error",
                "-f", "lavfi", "-i", "color=c=black:s=320x240:d=1",
                "-f", "lavfi", "-i", "anullsrc=r=16000:cl=mono",
                "-shortest", "-c:v", "libx264", "-c:a", "aac", str(self.video),
            ],
            check=True,
            timeout=30,
        )

    def tearDown(self) -> None:
        self.temp.cleanup()

    def test_processor_version_pins_library_model_and_revision(self) -> None:
        config = media.load_media_config()
        version = media.processor_versions()["asr"]
        self.assertEqual(config["asr"]["library_version"], "0.4.3")
        self.assertEqual(config["asr"]["model_id"], "mlx-community/whisper-large-v3-turbo")
        self.assertEqual(
            config["asr"]["model_revision"],
            "a4aaeec0636e6fef84abdcbe3544cb2bf7e9f6fb",
        )
        self.assertIn("mlx-whisper==0.4.3", version)
        self.assertIn(config["asr"]["model_revision"], version)
        self.assertEqual(media.ocr_binary_path(), media.PROJECT_ROOT / "runtime/bin/vision_ocr")

    def test_pinned_whisper_model_path_is_resolved_once_per_process(self) -> None:
        expected = self.root / "whisper-model"
        media.pinned_whisper_model_path.cache_clear()
        try:
            with (
                patch.object(media, "package_version", return_value="0.4.3"),
                patch.object(media, "snapshot_download", return_value=str(expected)) as download,
            ):
                first = media.pinned_whisper_model_path(local_files_only=True)
                second = media.pinned_whisper_model_path(local_files_only=True)
        finally:
            media.pinned_whisper_model_path.cache_clear()

        self.assertEqual(first, expected)
        self.assertEqual(second, expected)
        download.assert_called_once()

    def test_video_pipeline_records_asr_and_ocr_once_per_processor(self) -> None:
        calls = {"frames": 0, "asr": 0, "ocr": 0}

        def fake_frames(_source: Path, target_dir: Path) -> Path:
            calls["frames"] += 1
            target_dir.mkdir(parents=True, exist_ok=True)
            frame = target_dir / "frame-000.jpg"
            frame.write_bytes(b"frame" * 500)
            manifest = target_dir / "frames.json"
            media._atomic_json(
                manifest,
                {"frames": [{"path": str(frame), "sha256": media.file_sha256(frame)}]},
            )
            return manifest

        def fake_asr(_source: Path, target: Path) -> Path:
            calls["asr"] += 1
            media._atomic_json(target, {"status": "success", "text": "汽车内容"})
            return target

        def fake_ocr(_manifest: Path, target: Path) -> Path:
            calls["ocr"] += 1
            media._atomic_json(target, {"status": "success", "combined_text": "懂车帝"})
            return target

        with (
            patch.object(media, "MEDIA_ROOT", self.root / "outputs"),
            patch.object(media, "_extract_frames", side_effect=fake_frames),
            patch.object(media, "_run_asr", side_effect=fake_asr),
            patch.object(media, "_run_ocr", side_effect=fake_ocr),
        ):
            first = media.process_video_evidence(1, self.video, db_path=self.db)
            second = media.process_video_evidence(1, self.video, db_path=self.db)

        self.assertEqual(calls, {"frames": 1, "asr": 1, "ocr": 1})
        self.assertEqual(first["asr"].sha256, second["asr"].sha256)
        self.assertEqual(first["ocr"].sha256, second["ocr"].sha256)
        with connect(self.db) as connection:
            slots = connection.execute(
                "SELECT processor_type, status, attempt_count FROM media_processing_slots ORDER BY processor_type"
            ).fetchall()
        self.assertEqual(len(slots), 3)
        self.assertTrue(all(row["status"] == "succeeded" for row in slots))
        self.assertTrue(all(row["attempt_count"] == 1 for row in slots))

    def test_video_pipeline_continues_ocr_when_audio_cannot_be_decoded(self) -> None:
        def fake_frames(_source: Path, target_dir: Path) -> Path:
            target_dir.mkdir(parents=True, exist_ok=True)
            frame = target_dir / "frame-000.jpg"
            frame.write_bytes(b"frame" * 500)
            manifest = target_dir / "frames.json"
            media._atomic_json(
                manifest,
                {"frames": [{"path": str(frame), "sha256": media.file_sha256(frame)}]},
            )
            return manifest

        def fake_ocr(_manifest: Path, target: Path) -> Path:
            media._atomic_json(
                target,
                {
                    "status": "success",
                    "combined_text": "可用的画面文字证据",
                    "source_count": 1,
                },
            )
            return target

        with (
            patch.object(media, "MEDIA_ROOT", self.root / "corrupt-audio"),
            patch.object(media, "_extract_frames", side_effect=fake_frames),
            patch.object(media, "_run_ocr", side_effect=fake_ocr),
            patch.object(
                media,
                "pinned_whisper_model_path",
                return_value=self.root / "whisper-model",
            ),
            patch(
                "mlx_whisper.transcribe",
                side_effect=RuntimeError("Failed to load audio: corrupt AAC stream"),
            ),
        ):
            artifacts = media.process_video_evidence(1, self.video, db_path=self.db)

        asr_payload = json.loads(
            Path(artifacts["asr"].local_path).read_text(encoding="utf-8")
        )
        ocr_payload = json.loads(
            Path(artifacts["ocr"].local_path).read_text(encoding="utf-8")
        )
        self.assertEqual(asr_payload["status"], "unavailable")
        self.assertEqual(asr_payload["reason"], "audio_decode_failed")
        self.assertEqual(asr_payload["text"], "")
        self.assertEqual(ocr_payload["status"], "success")
        with connect(self.db) as connection:
            slots = {
                row["processor_type"]: row["status"]
                for row in connection.execute(
                    "SELECT processor_type,status FROM media_processing_slots"
                )
            }
        self.assertEqual(
            slots,
            {"frames": "succeeded", "asr": "succeeded", "ocr": "succeeded"},
        )

    def test_asr_does_not_hide_non_decode_runtime_failures(self) -> None:
        with (
            patch.object(
                media,
                "pinned_whisper_model_path",
                return_value=self.root / "whisper-model",
            ),
            patch(
                "mlx_whisper.transcribe",
                side_effect=RuntimeError("Metal backend failed"),
            ),
        ):
            with self.assertRaisesRegex(RuntimeError, "Metal backend failed"):
                media._run_asr(self.video, self.root / "unexpected-asr.json")

    def test_provider_media_download_has_its_own_idempotent_slot(self) -> None:
        calls = 0

        def fake_download(_urls, target: Path) -> Path:
            nonlocal calls
            calls += 1
            target.parent.mkdir(parents=True, exist_ok=True)
            target.write_bytes(self.video.read_bytes())
            return target

        with (
            patch.object(media, "MEDIA_ROOT", self.root / "downloads"),
            patch.object(media, "_download_video", side_effect=fake_download),
        ):
            first = media.download_video_sources(
                1, ["https://cdn.example/video.mp4"], db_path=self.db
            )
            second = media.download_video_sources(
                1, ["https://cdn.example/video.mp4"], db_path=self.db
            )
        self.assertEqual(calls, 1)
        self.assertEqual(first.sha256, second.sha256)
        with connect(self.db) as connection:
            slot = connection.execute(
                "SELECT status, attempt_count FROM media_processing_slots WHERE processor_type='download'"
            ).fetchone()
        self.assertEqual((slot["status"], slot["attempt_count"]), ("succeeded", 1))

    def test_source_identity_includes_kind_and_normalized_url_set(self) -> None:
        first_urls, first_sha = media._media_source_identity(
            "video",
            [
                " HTTPS://CDN.Example:443/video.mp4#preview ",
                "https://cdn.example/video.mp4",
                "https://cdn.example/fallback.mp4",
            ],
        )
        second_urls, second_sha = media._media_source_identity(
            "video",
            [
                "https://cdn.example/fallback.mp4",
                "https://cdn.example/video.mp4",
            ],
        )
        _, image_sha = media._media_source_identity("image", second_urls)

        self.assertEqual(
            first_urls,
            ["https://cdn.example/video.mp4", "https://cdn.example/fallback.mp4"],
        )
        self.assertEqual(second_urls, list(reversed(first_urls)))
        self.assertEqual(first_sha, second_sha)
        self.assertNotEqual(first_sha, image_sha)

    def test_source_manifests_are_append_only_and_legacy_source_json_remains_readable(self) -> None:
        with patch.object(media, "MEDIA_ROOT", self.root / "manifest-history"):
            first = media.store_media_source_manifest(
                1,
                media_kind="video",
                urls=["https://cdn.example/first.mp4"],
                raw_response_id=101,
                db_path=self.db,
            )
            self.assertIsNotNone(first)
            assert first is not None
            first_path = media._resolved(first.local_path)
            first_bytes = first_path.read_bytes()
            second = media.store_media_source_manifest(
                1,
                media_kind="video",
                urls=["https://cdn.example/second.mp4"],
                raw_response_id=102,
                db_path=self.db,
            )
            self.assertIsNotNone(second)
            assert second is not None
            second_path = media._resolved(second.local_path)

            self.assertRegex(first_path.name, r"^source-101-[0-9a-f]{12}\.json$")
            self.assertRegex(second_path.name, r"^source-102-[0-9a-f]{12}\.json$")
            self.assertNotEqual(first_path, second_path)
            self.assertEqual(first_path.read_bytes(), first_bytes)
            self.assertTrue(first_path.is_file())
            latest = media.get_media_source_state(1, db_path=self.db)
            self.assertIsNotNone(latest)
            assert latest is not None
            self.assertEqual(latest["raw_response_id"], 102)
            self.assertEqual(latest["urls"], ["https://cdn.example/second.mp4"])

            legacy_path = self.root / "manifest-history" / "A2BC3D" / "source.json"
            media._atomic_json(
                legacy_path,
                {
                    "media_kind": "video",
                    "urls": ["https://cdn.example/legacy.mp4"],
                    "raw_response_id": 99,
                },
            )
            with connect(self.db) as connection:
                media.register_artifact(
                    connection,
                    content_id=1,
                    artifact_type="media_source",
                    path=legacy_path,
                    processor_version="provider-media-source-v8.0",
                )
                connection.commit()
            legacy = media.get_media_source_state(1, db_path=self.db)

        self.assertIsNotNone(legacy)
        assert legacy is not None
        self.assertEqual(legacy["raw_response_id"], 99)
        self.assertEqual(legacy["urls"], ["https://cdn.example/legacy.mp4"])

    def test_refreshed_video_source_has_isolated_path_and_preserves_old_bytes(self) -> None:
        def fake_download(urls, target: Path) -> Path:
            target.parent.mkdir(parents=True, exist_ok=True)
            target.write_bytes((list(urls)[0] * 100).encode("utf-8"))
            return target

        with (
            patch.object(media, "MEDIA_ROOT", self.root / "versioned-video"),
            patch.object(media, "_download_video", side_effect=fake_download),
        ):
            first = media.download_video_sources(
                1, ["https://cdn.example/first.mp4"], db_path=self.db
            )
            first_path = media._resolved(first.local_path)
            first_bytes = first_path.read_bytes()
            second = media.download_video_sources(
                1, ["https://cdn.example/second.mp4"], db_path=self.db
            )
            second_path = media._resolved(second.local_path)

        self.assertNotEqual(first.local_path, second.local_path)
        self.assertIn("/downloads/", first.local_path)
        self.assertEqual(first_path.read_bytes(), first_bytes)
        self.assertNotEqual(first_path.read_bytes(), second_path.read_bytes())

    def test_refreshed_image_source_has_isolated_path_and_preserves_old_bytes(self) -> None:
        def fake_download(urls, target_dir: Path) -> Path:
            target_dir.mkdir(parents=True, exist_ok=True)
            image = target_dir / "image-000.bin"
            image.write_bytes((list(urls)[0] * 100).encode("utf-8"))
            manifest = target_dir / "manifest.json"
            media._atomic_json(
                manifest,
                {"frames": [{"path": str(image), "sha256": media.file_sha256(image)}]},
            )
            return manifest

        with (
            patch.object(media, "MEDIA_ROOT", self.root / "versioned-image"),
            patch.object(media, "_download_images", side_effect=fake_download),
        ):
            first = media.download_image_sources(
                1, ["https://cdn.example/first.jpg"], db_path=self.db
            )
            first_manifest = media._resolved(first.local_path)
            first_image = first_manifest.parent / "image-000.bin"
            first_bytes = first_image.read_bytes()
            second = media.download_image_sources(
                1, ["https://cdn.example/second.jpg"], db_path=self.db
            )
            second_manifest = media._resolved(second.local_path)

        self.assertNotEqual(first.local_path, second.local_path)
        self.assertIn("/downloads/", first.local_path)
        self.assertEqual(first_image.read_bytes(), first_bytes)
        self.assertNotEqual(
            first_image.read_bytes(), (second_manifest.parent / "image-000.bin").read_bytes()
        )

    def test_v80_success_is_reused_but_v80_terminal_blocks_same_source(self) -> None:
        urls, source_sha = media._media_source_identity(
            "video", ["https://cdn.example/legacy-success.mp4"]
        )
        legacy_sha = media._legacy_media_source_sha256(urls)
        legacy_media = self.root / "legacy-success.mp4"
        legacy_media.write_bytes(self.video.read_bytes())
        captured_at = now_utc()
        with connect(self.db) as connection:
            artifact = media.register_artifact(
                connection,
                content_id=1,
                artifact_type="media",
                path=legacy_media,
                processor_version="provider-media-download-v8.0",
            )
            connection.execute(
                """
                INSERT INTO media_processing_slots(
                    content_id, source_sha256, processor_type, processor_version,
                    status, output_artifact_id, attempt_count, created_at, updated_at
                ) VALUES (1, ?, 'download', 'provider-media-download-v8.0',
                          'succeeded', ?, 1, ?, ?)
                """,
                (legacy_sha, artifact.id, captured_at, captured_at),
            )
            connection.commit()

        with patch.object(media, "_download_video") as download:
            reused = media.download_video_sources(1, urls, db_path=self.db)
        self.assertEqual(reused.id, artifact.id)
        download.assert_not_called()
        self.assertEqual(media.VIDEO_DOWNLOAD_VERSION, "provider-media-download-v8.1")

        terminal_urls, _ = media._media_source_identity(
            "video", ["https://cdn.example/legacy-terminal.mp4"]
        )
        terminal_legacy_sha = media._legacy_media_source_sha256(terminal_urls)
        with connect(self.db) as connection:
            connection.execute(
                """
                INSERT INTO media_processing_slots(
                    content_id, source_sha256, processor_type, processor_version,
                    status, attempt_count, created_at, updated_at
                ) VALUES (1, ?, 'download', 'provider-media-download-v8.0',
                          'terminal_failed', 3, ?, ?)
                """,
                (terminal_legacy_sha, captured_at, captured_at),
            )
            connection.commit()
        with self.assertRaisesRegex(media.MediaProcessingError, "terminal"):
            media.download_video_sources(1, terminal_urls, db_path=self.db)

        self.assertNotEqual(source_sha, terminal_legacy_sha)

    def test_get_media_source_state_reports_latest_source_and_matching_download_slot(self) -> None:
        with patch.object(media, "MEDIA_ROOT", self.root / "source-state"):
            media.store_media_source_manifest(
                1,
                media_kind="video",
                urls=["https://cdn.example/state.mp4"],
                raw_response_id=501,
                db_path=self.db,
            )
            with patch.object(
                media,
                "_download_video",
                side_effect=media.MediaProcessingError("expired"),
            ):
                result = media.run_media_download_queue(
                    limit=1, max_workers=1, db_path=self.db
                )
            state = media.get_media_source_state(1, db_path=self.db)

        self.assertEqual(result["retryable_failed"], 1)
        self.assertIsNotNone(state)
        assert state is not None
        self.assertEqual(state["raw_response_id"], 501)
        self.assertEqual(state["media_kind"], "video")
        self.assertEqual(state["urls"], ["https://cdn.example/state.mp4"])
        self.assertRegex(state["source_sha256"], r"^[0-9a-f]{64}$")
        self.assertEqual(state["download_slot"]["status"], "retryable_failed")
        self.assertEqual(state["download_slot"]["attempt_count"], 1)
        self.assertIsNone(state["download_slot"]["output_artifact_id"])

    def test_complete_migrated_evidence_is_ready_without_paid_source_refresh(self) -> None:
        media_path = self.root / "legacy.mp4"
        asr_path = self.root / "legacy-asr.json"
        ocr_path = self.root / "legacy-ocr.json"
        media_path.write_bytes(b"legacy-media" * 200)
        media._atomic_json(asr_path, {"status": "success", "text": "完整语音证据"})
        media._atomic_json(ocr_path, {"status": "success", "combined_text": "完整画面证据"})
        with connect(self.db) as connection:
            for artifact_type, path in (("media", media_path), ("transcript", asr_path), ("ocr", ocr_path)):
                media.register_artifact(
                    connection,
                    content_id=1,
                    artifact_type=artifact_type,
                    path=path,
                    processor_version="legacy-test",
                )
            connection.commit()
        result = media.process_content_media(1, db_path=self.db)
        self.assertEqual(result["status"], "evidence_ready")
        self.assertEqual(result["source"], "existing_local_evidence")
        self.assertEqual(set(result["artifacts"]), {"media", "asr", "ocr"})
        with connect(self.db) as connection:
            self.assertEqual(
                connection.execute("SELECT COUNT(*) FROM media_processing_slots").fetchone()[0],
                0,
            )

    def test_automatic_download_and_processing_queues_produce_complete_video_evidence(self) -> None:
        calls = {"download": 0, "frames": 0, "asr": 0, "ocr": 0}

        def fake_download(_urls, target: Path) -> Path:
            calls["download"] += 1
            target.parent.mkdir(parents=True, exist_ok=True)
            target.write_bytes(self.video.read_bytes())
            return target

        def fake_frames(_source: Path, target_dir: Path) -> Path:
            calls["frames"] += 1
            target_dir.mkdir(parents=True, exist_ok=True)
            frame = target_dir / "frame-000.jpg"
            frame.write_bytes(b"frame" * 500)
            manifest = target_dir / "frames.json"
            media._atomic_json(
                manifest,
                {"frames": [{"path": str(frame), "sha256": media.file_sha256(frame)}]},
            )
            return manifest

        def fake_asr(_source: Path, target: Path) -> Path:
            calls["asr"] += 1
            media._atomic_json(target, {"status": "success", "text": "完整汽车语音证据"})
            return target

        def fake_ocr(_manifest: Path, target: Path) -> Path:
            calls["ocr"] += 1
            media._atomic_json(
                target,
                {"status": "success", "combined_text": "完整汽车画面证据", "source_count": 1},
            )
            return target

        with (
            patch.object(media, "MEDIA_ROOT", self.root / "automatic"),
            patch.object(media, "_download_video", side_effect=fake_download),
            patch.object(media, "_extract_frames", side_effect=fake_frames),
            patch.object(media, "_run_asr", side_effect=fake_asr),
            patch.object(media, "_run_ocr", side_effect=fake_ocr),
            patch.object(media, "compile_ocr_binary", return_value=self.root / "vision_ocr"),
        ):
            source = media.store_media_source_manifest(
                1,
                media_kind="video",
                urls=["https://cdn.example/video.mp4"],
                raw_response_id=1,
                db_path=self.db,
            )
            downloaded = media.run_media_download_queue(db_path=self.db)
            processed = media.run_media_processing_queue(db_path=self.db)
            repeated_download = media.run_media_download_queue(db_path=self.db)
            repeated_processing = media.run_media_processing_queue(db_path=self.db)

        self.assertIsNotNone(source)
        self.assertEqual(downloaded["downloaded"], 1)
        self.assertEqual(processed["evidence_ready"], 1)
        self.assertEqual(repeated_download["candidates"], 0)
        self.assertEqual(repeated_processing["candidates"], 0)
        self.assertEqual(calls, {"download": 1, "frames": 1, "asr": 1, "ocr": 1})
        with connect(self.db) as connection:
            artifact_types = {
                row["artifact_type"]
                for row in connection.execute(
                    "SELECT artifact_type FROM evidence_artifacts WHERE content_id=1"
                )
            }
        self.assertTrue({"media_source", "media", "frames_manifest", "asr", "ocr"} <= artifact_types)

    def test_automatic_image_queue_downloads_and_produces_ocr_without_asr(self) -> None:
        with connect(self.db) as connection:
            connection.execute("UPDATE content_items SET content_type='image' WHERE id=1")
            connection.commit()

        def fake_download(_urls, target_dir: Path) -> Path:
            target_dir.mkdir(parents=True, exist_ok=True)
            image = target_dir / "image-000.jpg"
            image.write_bytes(b"\xff\xd8\xff" + b"image" * 200)
            manifest = target_dir / "manifest.json"
            media._atomic_json(
                manifest,
                {
                    "status": "complete",
                    "frames": [{"path": str(image), "sha256": media.file_sha256(image)}],
                },
            )
            return manifest

        def fake_ocr(_manifest: Path, target: Path) -> Path:
            media._atomic_json(
                target,
                {"status": "success", "combined_text": "图文汽车证据", "source_count": 1},
            )
            return target

        with (
            patch.object(media, "MEDIA_ROOT", self.root / "automatic-images"),
            patch.object(media, "_download_images", side_effect=fake_download),
            patch.object(media, "_run_ocr", side_effect=fake_ocr),
            patch.object(media, "compile_ocr_binary", return_value=self.root / "vision_ocr"),
        ):
            media.store_media_source_manifest(
                1,
                media_kind="image",
                urls=["https://cdn.example/image.jpg"],
                raw_response_id=2,
                db_path=self.db,
            )
            downloaded = media.run_media_download_queue(db_path=self.db)
            processed = media.run_media_processing_queue(db_path=self.db)

        self.assertEqual(downloaded["downloaded"], 1)
        self.assertEqual(processed["evidence_ready"], 1)
        with connect(self.db) as connection:
            artifact_types = {
                row["artifact_type"]
                for row in connection.execute(
                    "SELECT artifact_type FROM evidence_artifacts WHERE content_id=1"
                )
            }
        self.assertTrue({"media_source", "media_manifest", "ocr"} <= artifact_types)
        self.assertNotIn("asr", artifact_types)

    def test_processing_queue_does_not_reprocess_migrated_media_without_v8_source(self) -> None:
        captured_at = now_utc()
        legacy = self.root / "legacy.mp4"
        legacy.write_bytes(self.video.read_bytes())
        with connect(self.db) as connection:
            connection.execute(
                """
                INSERT INTO evidence_artifacts(
                    content_id, artifact_type, local_path, sha256, processor_version,
                    status, captured_at, created_at
                ) VALUES (1, 'media', ?, ?, 'legacy-migration', 'available', ?, ?)
                """,
                (str(legacy), media.file_sha256(legacy), captured_at, captured_at),
            )
            connection.commit()
        with patch.object(media, "compile_ocr_binary") as compile_ocr:
            result = media.run_media_processing_queue(db_path=self.db)
        self.assertEqual(result["candidates"], 0)
        compile_ocr.assert_not_called()

    def test_media_queue_prioritizes_newest_published_content(self) -> None:
        captured_at = now_utc()
        with connect(self.db) as connection:
            connection.execute(
                "UPDATE content_items SET published_at='2026-07-20T00:00:00Z' WHERE id=1"
            )
            for index, published_at in enumerate(
                ("2026-08-01T00:00:00Z", "2026-08-02T00:00:00Z"), start=2
            ):
                connection.execute(
                    """
                    INSERT INTO content_items(
                        link_id, platform, platform_content_id, canonical_url, title,
                        content_type, published_at, imported_at, created_at, updated_at
                    ) VALUES (?, 'douyin', ?, ?, '', 'video', ?, ?, ?, ?)
                    """,
                    (
                        f"A2BC3{index}", str(100000000 + index),
                        f"https://www.douyin.com/video/{100000000 + index}",
                        published_at, captured_at, captured_at, captured_at,
                    ),
                )
            for content_id in (1, 2, 3):
                source = self.root / f"source-{content_id}.json"
                media._atomic_json(
                    source,
                    {"media_kind": "video", "urls": [f"https://cdn.example/{content_id}.mp4"]},
                )
                media.register_artifact(
                    connection,
                    content_id=content_id,
                    artifact_type="media_source",
                    path=source,
                    processor_version="queue-order-test",
                )
            connection.commit()

        self.assertEqual(
            media._queue_content_ids(stage="download", limit=3, db_path=self.db),
            [3, 2, 1],
        )
        self.assertEqual(
            media._queue_content_ids(
                stage="download", limit=3, db_path=self.db,
                published_start="2026-07-31T16:00:00Z",
                published_end="2026-08-02T18:00:00Z",
            ),
            [3, 2],
        )

    def test_download_queue_stops_after_three_failures_without_starving_older_content(self) -> None:
        captured_at = now_utc()
        with connect(self.db) as connection:
            connection.execute(
                "UPDATE content_items SET published_at='2026-08-02T00:00:00Z' WHERE id=1"
            )
            connection.execute(
                """
                INSERT INTO content_items(
                    link_id, platform, platform_content_id, canonical_url, title,
                    content_type, published_at, imported_at, created_at, updated_at
                ) VALUES ('B2CD3E', 'douyin', '100000002',
                          'https://www.douyin.com/video/100000002', '', 'video',
                          '2026-08-01T00:00:00Z', ?, ?, ?)
                """,
                (captured_at, captured_at, captured_at),
            )
            connection.commit()

        with patch.object(media, "MEDIA_ROOT", self.root / "bounded-downloads"):
            for content_id in (1, 2):
                media.store_media_source_manifest(
                    content_id,
                    media_kind="video",
                    urls=[f"https://cdn.example/{content_id}.mp4"],
                    raw_response_id=content_id,
                    db_path=self.db,
                )

            def fake_download(urls, target: Path) -> Path:
                if "/1.mp4" in list(urls)[0]:
                    raise media.MediaProcessingError("expired source")
                target.parent.mkdir(parents=True, exist_ok=True)
                target.write_bytes(self.video.read_bytes())
                return target

            with patch.object(media, "_download_video", side_effect=fake_download):
                attempts = [
                    media.run_media_download_queue(
                        limit=1, max_workers=1, db_path=self.db
                    )
                    for _ in range(4)
                ]

        self.assertEqual([item["downloaded"] for item in attempts], [0, 0, 0, 1])
        self.assertEqual([item["failed"] for item in attempts], [1, 1, 1, 0])
        self.assertEqual(
            [item["retryable_failed"] for item in attempts], [1, 1, 0, 0]
        )
        self.assertEqual(
            [item["terminal_failed"] for item in attempts], [0, 0, 1, 0]
        )
        self.assertEqual(attempts[2]["results"][0]["status"], "terminal_failed")
        self.assertTrue(all("stale_recovery" in item for item in attempts))
        with connect(self.db) as connection:
            failed = connection.execute(
                """
                SELECT status, attempt_count FROM media_processing_slots
                WHERE content_id=1 AND processor_type='download'
                """
            ).fetchone()
            successful = connection.execute(
                """
                SELECT status, attempt_count FROM media_processing_slots
                WHERE content_id=2 AND processor_type='download'
                """
            ).fetchone()
        self.assertEqual((failed["status"], failed["attempt_count"]), ("terminal_failed", 3))
        self.assertEqual((successful["status"], successful["attempt_count"]), ("succeeded", 1))

    def test_stale_running_slots_are_recovered_with_cas_and_bounded_statuses(self) -> None:
        captured_at = "2000-01-01T00:00:00Z"
        with connect(self.db) as connection:
            connection.executemany(
                """
                INSERT INTO media_processing_slots(
                    content_id, source_sha256, processor_type, processor_version,
                    status, attempt_count, created_at, updated_at
                ) VALUES (1, ?, ?, ?, 'running', ?, ?, ?)
                """,
                [
                    ("a" * 64, "download", media.VIDEO_DOWNLOAD_VERSION, 3, captured_at, captured_at),
                    ("b" * 64, "frames", "frames-test-v1", 2, captured_at, captured_at),
                    ("c" * 64, "asr", "asr-test-v1", 3, captured_at, captured_at),
                    ("d" * 64, "ocr", "ocr-test-v1", 1, now_utc(), now_utc()),
                ],
            )
            connection.commit()

        recovered = media.recover_stale_media_processing_slots(db_path=self.db)

        self.assertEqual(recovered["stale_candidates"], 3)
        self.assertEqual(recovered["recovered"], 3)
        self.assertEqual(recovered["retryable_failed"], 1)
        self.assertEqual(recovered["terminal_failed"], 2)
        self.assertEqual(recovered["cas_conflicts"], 0)
        with connect(self.db) as connection:
            statuses = {
                row["processor_type"]: row["status"]
                for row in connection.execute(
                    "SELECT processor_type,status FROM media_processing_slots"
                )
            }
        self.assertEqual(statuses["download"], "terminal_failed")
        self.assertEqual(statuses["frames"], "retryable_failed")
        self.assertEqual(statuses["asr"], "terminal_failed")
        self.assertEqual(statuses["ocr"], "running")

    def test_existing_exhausted_retryable_slot_is_normalized_to_terminal(self) -> None:
        captured_at = now_utc()
        versions = media.processor_versions()
        with connect(self.db) as connection:
            connection.execute(
                """
                INSERT INTO media_processing_slots(
                    content_id, source_sha256, processor_type, processor_version,
                    status, attempt_count, error_message, created_at, updated_at
                ) VALUES (1, ?, 'frames', ?, 'retryable_failed', 6,
                          'historical retries', ?, ?)
                """,
                ("e" * 64, versions["frames"], captured_at, captured_at),
            )
            connection.commit()

        with patch.object(media, "compile_ocr_binary"):
            queue = media.run_media_processing_queue(limit=1, db_path=self.db)
        recovery = queue["stale_recovery"]

        self.assertEqual(recovery["exhausted_normalized"], 1)
        with connect(self.db) as connection:
            slot = connection.execute(
                "SELECT status,attempt_count FROM media_processing_slots"
            ).fetchone()
        self.assertEqual((slot["status"], slot["attempt_count"]), ("terminal_failed", 6))

    def test_processing_queue_stops_current_processor_version_after_three_failures(self) -> None:
        with patch.object(media, "MEDIA_ROOT", self.root / "bounded-processing"):
            media.store_media_source_manifest(
                1,
                media_kind="video",
                urls=["https://cdn.example/process.mp4"],
                raw_response_id=701,
                db_path=self.db,
            )

            def fake_download(_urls, target: Path) -> Path:
                target.parent.mkdir(parents=True, exist_ok=True)
                target.write_bytes(self.video.read_bytes())
                return target

            with patch.object(media, "_download_video", side_effect=fake_download):
                downloaded = media.run_media_download_queue(
                    limit=1, max_workers=1, db_path=self.db
                )
            with (
                patch.object(
                    media,
                    "_extract_frames",
                    side_effect=media.MediaProcessingError("no frames"),
                ),
                patch.object(media, "compile_ocr_binary", return_value=self.root / "ocr"),
            ):
                attempts = [
                    media.run_media_processing_queue(limit=1, db_path=self.db)
                    for _ in range(4)
                ]

        self.assertEqual(downloaded["downloaded"], 1)
        self.assertEqual([item["candidates"] for item in attempts], [1, 1, 1, 0])
        self.assertEqual(
            [item["retryable_failed"] for item in attempts], [1, 1, 0, 0]
        )
        self.assertEqual(
            [item["terminal_failed"] for item in attempts], [0, 0, 1, 0]
        )
        self.assertEqual([item["failed"] for item in attempts], [1, 1, 1, 0])
        self.assertEqual(attempts[2]["results"][0]["status"], "terminal_failed")
        self.assertTrue(all("stale_recovery" in item for item in attempts))
        with connect(self.db) as connection:
            slot = connection.execute(
                """
                SELECT status,attempt_count FROM media_processing_slots
                WHERE processor_type='frames'
                """
            ).fetchone()
        self.assertEqual((slot["status"], slot["attempt_count"]), ("terminal_failed", 3))

    def test_download_queue_allows_refreshed_source_after_terminal_failure(self) -> None:
        with patch.object(media, "MEDIA_ROOT", self.root / "refreshed-downloads"):
            media.store_media_source_manifest(
                1,
                media_kind="video",
                urls=["https://cdn.example/expired.mp4"],
                raw_response_id=1,
                db_path=self.db,
            )
            with patch.object(
                media,
                "_download_video",
                side_effect=media.MediaProcessingError("expired source"),
            ):
                for _ in range(3):
                    media.run_media_download_queue(
                        limit=1, max_workers=1, db_path=self.db
                    )
            self.assertEqual(
                media._queue_content_ids(stage="download", limit=1, db_path=self.db),
                [],
            )
            media.store_media_source_manifest(
                1,
                media_kind="video",
                urls=["https://cdn.example/refreshed.mp4"],
                raw_response_id=2,
                db_path=self.db,
            )
            self.assertEqual(
                media._queue_content_ids(stage="download", limit=1, db_path=self.db),
                [1],
            )

    def test_legacy_ocr_observations_are_aggregated_before_resolution(self) -> None:
        asr = self.root / "asr.json"
        ocr = self.root / "ocr.json"
        asr.write_text(json.dumps({"status": "success", "text": ""}), encoding="utf-8")
        ocr.write_text(
            json.dumps(
                {
                    "status": "success",
                    "combined_text": "",
                    "observations": [
                        {"text": "汽车常见异响"},
                        {"text": "刹车片 平衡杆"},
                    ],
                },
                ensure_ascii=False,
            ),
            encoding="utf-8",
        )
        with patch.object(media, "MEDIA_ROOT", self.root / "normalized"):
            artifacts = media.ingest_existing_video_evidence(
                1,
                media_path=self.video,
                asr_path=asr,
                ocr_path=ocr,
                db_path=self.db,
            )
        normalized = json.loads(
            (self.root / "normalized" / "legacy-1" / "ocr.json").read_text(encoding="utf-8")
        )
        self.assertEqual(normalized["combined_text"], "汽车常见异响\n刹车片 平衡杆")
        self.assertEqual(set(artifacts), {"media", "asr", "ocr"})
        with connect(self.db) as connection:
            status = connection.execute("SELECT status FROM review_queue").fetchone()[0]
        self.assertEqual(status, "resolved")


if __name__ == "__main__":
    unittest.main()
