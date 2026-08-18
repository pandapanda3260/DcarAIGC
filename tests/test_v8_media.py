from __future__ import annotations

import json
import hashlib
import io
import os
import subprocess
import tempfile
import unittest
import urllib.error
from pathlib import Path
from unittest.mock import patch

from v8 import media
from v8.storage import connect, initialize_database, now_utc, transaction


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

    def _store_current_image_source(
        self, urls: list[str], *, platform: str | None = None
    ) -> None:
        with connect(self.db) as connection:
            if platform is None:
                connection.execute(
                    "UPDATE content_items SET content_type='image' WHERE id=1"
                )
            else:
                connection.execute(
                    "UPDATE content_items SET platform=?,content_type='image' "
                    "WHERE id=1",
                    (platform,),
                )
            connection.commit()
            raw_response_id = (
                connection.execute(
                    "SELECT COALESCE(MAX(id),0)+1000 FROM evidence_artifacts"
                ).fetchone()[0]
            )
        media.store_media_source_manifest(
            1,
            media_kind="image",
            urls=urls,
            raw_response_id=int(raw_response_id),
            db_path=self.db,
            media_root=self.root / "test-image-sources",
        )

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

    def test_media_validation_requires_a_video_stream(self) -> None:
        audio = self.root / "audio-only.m4a"
        subprocess.run(
            [
                "ffmpeg", "-y", "-nostdin", "-loglevel", "error",
                "-f", "lavfi", "-i", "sine=frequency=1000:duration=1",
                "-c:a", "aac", str(audio),
            ],
            check=True,
            timeout=30,
        )

        self.assertGreater(media._probe_duration(audio), 0)
        self.assertFalse(media._has_video_stream(audio))
        self.assertFalse(media._valid_media(audio))
        self.assertTrue(media._has_video_stream(self.video))
        self.assertTrue(media._valid_media(self.video))
        self.assertFalse(
            media._valid_media(self.video, maximum_duration_seconds=0.5)
        )
        self.assertTrue(
            media._valid_media(self.video, maximum_duration_seconds=2.0)
        )

    def test_frame_outputs_are_staged_until_manifest_is_durable(self) -> None:
        target_dir = self.root / "atomic-frames"
        manifest = target_dir / "frames.json"
        observed = {"manifest_before_final": False}
        original_atomic_json = media._atomic_json

        def observe_manifest(path: Path, payload) -> None:
            if path == manifest:
                observed["manifest_before_final"] = True
                self.assertFalse(any(target_dir.glob("frame-*.jpg")))
                staged = sorted(target_dir.glob(".frame-*.tmp.jpg"))
                self.assertTrue(staged)
                self.assertTrue(all(media._valid_image(path) for path in staged))
            original_atomic_json(path, payload)

        with patch.object(media, "_atomic_json", side_effect=observe_manifest):
            result = media._extract_frames(self.video, target_dir)

        self.assertEqual(result, manifest)
        self.assertTrue(observed["manifest_before_final"])
        body = json.loads(manifest.read_text())
        self.assertTrue(body["frames"])
        self.assertTrue(all(Path(row["path"]).is_file() for row in body["frames"]))
        self.assertFalse(any(target_dir.glob(".*.tmp.jpg")))

    def test_image_validation_reads_only_the_header(self) -> None:
        image = self.root / "large-image.png"
        image.write_bytes(b"\x89PNG\r\n\x1a\n" + b"x" * 1024)

        with patch.object(
            Path,
            "read_bytes",
            side_effect=AssertionError("full-file read is forbidden"),
        ):
            self.assertTrue(media._valid_image(image))

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
            frame.write_bytes(b"\xff\xd8\xff" + b"frame" * 500)
            manifest = target_dir / "frames.json"
            media._atomic_json(
                manifest,
                {
                    "status": "success",
                    "duration_seconds": 1.0,
                    "frames": [
                        {
                            "path": str(frame),
                            "sha256": media.file_sha256(frame),
                        }
                    ],
                    "contact_sheet": None,
                },
            )
            return manifest

        def fake_asr(_source: Path, target: Path) -> Path:
            calls["asr"] += 1
            config = media.load_media_config()["asr"]
            media._atomic_json(
                target,
                {
                    "status": "success",
                    "processor_version": media.processor_versions()["asr"],
                    "model_id": config["model_id"],
                    "model_revision": config["model_revision"],
                    "language": config["language"],
                    "text": "汽车内容",
                    "segments": [],
                    "elapsed_seconds": 0.1,
                },
            )
            return target

        def fake_ocr(_manifest: Path, target: Path, **_kwargs) -> Path:
            calls["ocr"] += 1
            media._atomic_json(
                target,
                {
                    "status": "success",
                    "processor_version": media.processor_versions()["ocr"],
                    "source_count": 1,
                    "ocr_observation_count": 1,
                    "combined_text": "懂车帝",
                    "observations": [{"text": "懂车帝"}],
                },
            )
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

    def test_cached_video_outputs_reject_coordinated_body_and_row_tampering(
        self,
    ) -> None:
        output_root = self.root / "strict-cached-video"
        versions = media.processor_versions()
        asr_config = media.load_media_config()["asr"]

        def fake_asr(_source: Path, target: Path, **_kwargs) -> Path:
            media._atomic_json(
                target,
                {
                    "status": "success",
                    "processor_version": versions["asr"],
                    "model_id": asr_config["model_id"],
                    "model_revision": asr_config["model_revision"],
                    "language": asr_config["language"],
                    "text": "缓存语音证据",
                    "segments": [
                        {
                            "start": 0.0,
                            "end": 0.5,
                            "text": "缓存语音证据",
                            "avg_logprob": -0.1,
                            "no_speech_prob": 0.0,
                        }
                    ],
                    "elapsed_seconds": 0.1,
                },
            )
            return target

        def fake_ocr(manifest: Path, target: Path, **_kwargs) -> Path:
            frame_count = len(json.loads(manifest.read_text())["frames"])
            observations = [
                {"text": f"缓存画面证据{index}"}
                for index in range(frame_count)
            ]
            media._atomic_json(
                target,
                {
                    "status": "success",
                    "processor_version": versions["ocr"],
                    "source_count": frame_count,
                    "ocr_observation_count": frame_count,
                    "combined_text": "\n".join(
                        item["text"] for item in observations
                    ),
                    "observations": observations,
                },
            )
            return target

        with (
            patch.object(media, "_run_asr", side_effect=fake_asr),
            patch.object(media, "_run_ocr", side_effect=fake_ocr),
        ):
            artifacts = media.process_video_evidence(
                1,
                self.video,
                db_path=self.db,
                media_root=output_root,
            )
        artifact_ids = {
            key: value.id for key, value in artifacts.items() if key != "media"
        }
        artifact_paths = {
            key: Path(value.local_path)
            for key, value in artifacts.items()
            if key != "media"
        }
        original_bodies = {
            key: json.loads(path.read_text())
            for key, path in artifact_paths.items()
        }
        with connect(self.db) as connection:
            original_counts = (
                connection.execute(
                    "SELECT COUNT(*) FROM media_processing_slots"
                ).fetchone()[0],
                connection.execute(
                    "SELECT COUNT(*) FROM evidence_artifacts"
                ).fetchone()[0],
            )

        mutations = {
            "frames": lambda body: body["frames"][0].__setitem__(
                "sha256", "0" * 64
            ),
            "asr": lambda body: body["segments"][0].__setitem__("start", True),
            "ocr": lambda body: body.__setitem__(
                "combined_text", "协调伪造正文"
            ),
        }
        for key, mutate in mutations.items():
            with self.subTest(artifact=key):
                for restore_key, path in artifact_paths.items():
                    media._atomic_json(path, original_bodies[restore_key])
                    with connect(self.db) as connection:
                        connection.execute(
                            "UPDATE evidence_artifacts SET byte_size=?,sha256=? "
                            "WHERE id=?",
                            (
                                path.stat().st_size,
                                media.file_sha256(path),
                                artifact_ids[restore_key],
                            ),
                        )
                        connection.commit()
                forged = json.loads(
                    json.dumps(original_bodies[key], ensure_ascii=False)
                )
                mutate(forged)
                media._atomic_json(artifact_paths[key], forged)
                with connect(self.db) as connection:
                    connection.execute(
                        "UPDATE evidence_artifacts SET byte_size=?,sha256=? "
                        "WHERE id=?",
                        (
                            artifact_paths[key].stat().st_size,
                            media.file_sha256(artifact_paths[key]),
                            artifact_ids[key],
                        ),
                    )
                    connection.commit()
                with (
                    patch.object(
                        media,
                        "_extract_frames",
                        return_value=artifact_paths["frames"],
                    ) as extract_frames,
                    patch.object(
                        media,
                        "_run_asr",
                        return_value=artifact_paths["asr"],
                    ) as run_asr,
                    patch.object(
                        media,
                        "_run_ocr",
                        return_value=artifact_paths["ocr"],
                    ) as run_ocr,
                    self.assertRaises(media.MediaProcessingError),
                ):
                    media.process_video_evidence(
                        1,
                        self.video,
                        db_path=self.db,
                        media_root=output_root,
                    )
                extract_frames.assert_not_called()
                run_asr.assert_not_called()
                run_ocr.assert_not_called()
                with connect(self.db) as connection:
                    self.assertEqual(
                        (
                            connection.execute(
                                "SELECT COUNT(*) FROM media_processing_slots"
                            ).fetchone()[0],
                            connection.execute(
                                "SELECT COUNT(*) FROM evidence_artifacts"
                            ).fetchone()[0],
                        ),
                        original_counts,
                    )

    def test_register_artifact_update_preserves_id_and_sqlite_sequence(self) -> None:
        artifact_path = self.root / "sequence-stable-media.mp4"
        artifact_path.write_bytes(b"first" * 300)
        with connect(self.db) as connection, transaction(connection):
            first = media.register_artifact(
                connection,
                content_id=1,
                artifact_type="media",
                path=artifact_path,
                processor_version="sequence-test-v1",
                captured_at="2026-01-01T00:00:00Z",
                metadata={"version": 1},
            )
            before = connection.execute(
                "SELECT seq FROM sqlite_sequence "
                "WHERE name='evidence_artifacts'"
            ).fetchone()[0]
        artifact_path.write_bytes(b"second" * 400)
        with connect(self.db) as connection, transaction(connection):
            connection.execute(
                "UPDATE evidence_artifacts SET status='failed' WHERE id=?",
                (first.id,),
            )
            second = media.register_artifact(
                connection,
                content_id=1,
                artifact_type="media",
                path=artifact_path,
                processor_version="sequence-test-v2",
                captured_at="2026-01-02T00:00:00Z",
                metadata={"version": 2},
            )
            after = connection.execute(
                "SELECT seq FROM sqlite_sequence "
                "WHERE name='evidence_artifacts'"
            ).fetchone()[0]
            maximum = connection.execute(
                "SELECT MAX(id) FROM evidence_artifacts"
            ).fetchone()[0]
            row = connection.execute(
                "SELECT * FROM evidence_artifacts WHERE id=?",
                (first.id,),
            ).fetchone()

        self.assertEqual(second.id, first.id)
        self.assertEqual((int(before), int(after), int(maximum)), (1, 1, 1))
        self.assertEqual(row["status"], "available")
        self.assertEqual(row["byte_size"], artifact_path.stat().st_size)
        self.assertEqual(row["sha256"], media.file_sha256(artifact_path))
        self.assertEqual(row["captured_at"], "2026-01-02T00:00:00Z")
        self.assertEqual(row["processor_version"], "sequence-test-v2")
        self.assertEqual(json.loads(row["metadata_json"]), {"version": 2})

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

        def fake_ocr(_manifest: Path, target: Path, **_kwargs) -> Path:
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

    def test_explicit_roots_and_local_processors_flow_without_global_patch(self) -> None:
        isolated_root = self.root / "isolated-media"
        source_root = self.root / "source-manifests"
        model_path = self.root / "local-whisper"
        binary_path = self.root / "vision-ocr"
        model_path.mkdir()
        binary_path.write_bytes(b"fixture")
        calls: dict[str, object] = {}

        def fake_download(_urls, target: Path, **kwargs) -> Path:
            calls["download"] = kwargs
            target.parent.mkdir(parents=True, exist_ok=True)
            target.write_bytes(self.video.read_bytes())
            return target

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

        def fake_asr(_source: Path, target: Path, *, model_path: Path) -> Path:
            calls["model_path"] = model_path
            media._atomic_json(target, {"status": "success", "text": "本地语音"})
            return target

        def fake_ocr(
            _manifest: Path, target: Path, *, binary_path: Path
        ) -> Path:
            calls["binary_path"] = binary_path
            media._atomic_json(
                target,
                {"status": "success", "combined_text": "本地画面", "source_count": 1},
            )
            return target

        sentinel_urlopen = object()
        media.store_media_source_manifest(
            1,
            media_kind="video",
            urls=["https://cdn.example/explicit.mp4"],
            raw_response_id=701,
            db_path=self.db,
            media_root=source_root,
        )
        original_root = media.MEDIA_ROOT
        with (
            patch.object(media, "_download_video", side_effect=fake_download),
            patch.object(media, "_extract_frames", side_effect=fake_frames),
            patch.object(media, "_run_asr", side_effect=fake_asr),
            patch.object(media, "_run_ocr", side_effect=fake_ocr),
        ):
            result = media.process_content_media(
                1,
                db_path=self.db,
                media_root=isolated_root,
                whisper_model_path=model_path,
                ocr_binary=binary_path,
                urlopen_fn=sentinel_urlopen,  # type: ignore[arg-type]
                maximum_download_bytes=123456,
                require_exact_response_url=True,
            )

        self.assertEqual(result["status"], "evidence_ready")
        self.assertEqual(media.MEDIA_ROOT, original_root)
        self.assertEqual(calls["model_path"], model_path)
        self.assertEqual(calls["binary_path"], binary_path)
        self.assertEqual(
            calls["download"],
            {
                "urlopen_fn": sentinel_urlopen,
                "maximum_bytes": 123456,
                "require_exact_response_url": True,
                "reuse_existing": True,
                "maximum_duration_seconds": None,
            },
        )
        with connect(self.db) as connection:
            generated = [
                Path(row["local_path"]).resolve()
                for row in connection.execute(
                    """
                    SELECT local_path FROM evidence_artifacts
                    WHERE content_id=1 AND artifact_type<>'media_source'
                    """
                )
            ]
        self.assertTrue(generated)
        self.assertTrue(
            all(path == isolated_root.resolve() or path.is_relative_to(isolated_root.resolve()) for path in generated)
        )

    def test_controlled_download_rejects_redirect_and_oversized_response(self) -> None:
        class Response(io.BytesIO):
            def __init__(self, body: bytes, *, final_url: str, length: int) -> None:
                super().__init__(body)
                self.headers = {"Content-Length": str(length)}
                self._final_url = final_url

            def geturl(self) -> str:
                return self._final_url

            def __enter__(self):
                return self

            def __exit__(self, *_args) -> None:
                self.close()

        source_url = "https://cdn.example/video.mp4"
        target = self.root / "controlled.mp4"
        with self.assertRaisesRegex(media.MediaProcessingError, "download failed"):
            media._download_video(
                [source_url],
                target,
                urlopen_fn=lambda *_args, **_kwargs: Response(
                    b"x" * 4096,
                    final_url="https://redirect.example/video.mp4",
                    length=4096,
                ),
                maximum_bytes=8192,
                require_exact_response_url=True,
            )
        self.assertFalse(target.exists())

        with self.assertRaisesRegex(media.MediaProcessingError, "download failed"):
            media._download_video(
                [source_url],
                target,
                urlopen_fn=lambda *_args, **_kwargs: Response(
                    b"x" * 4096,
                    final_url=source_url,
                    length=4096,
                ),
                maximum_bytes=1024,
                require_exact_response_url=True,
            )
        self.assertFalse(target.exists())

    def test_response_writes_never_follow_image_or_video_candidate_aliases(
        self,
    ) -> None:
        class AliasingResponse(io.BytesIO):
            def __init__(
                self, body: bytes, *, url: str, alias: Path, victim: Path
            ) -> None:
                super().__init__(body)
                self.headers = {"Content-Length": str(len(body))}
                self._url = url
                self._alias = alias
                self._victim = victim

            def geturl(self) -> str:
                return self._url

            def __enter__(self):
                self._alias.symlink_to(self._victim)
                return self

            def __exit__(self, *_args) -> None:
                self.close()

        video_url = "https://cdn.example/alias-video.mp4"
        video_target = self.root / "alias-video" / "video.mp4"
        video_candidate = video_target.with_name(
            f".{video_target.name}.candidate-0"
        )
        video_victim = self.root / "external-video-victim"
        video_sentinel = b"external-video-victim-must-not-change"
        video_victim.write_bytes(video_sentinel)

        def open_video(*_args, **_kwargs):
            return AliasingResponse(
                b"V" * 4096,
                url=video_url,
                alias=video_candidate,
                victim=video_victim,
            )

        with self.assertRaisesRegex(
            media.MediaProcessingError, "occupied or aliased"
        ):
            media._download_video(
                [video_url],
                video_target,
                urlopen_fn=open_video,
                maximum_bytes=10_000,
                require_exact_response_url=True,
            )
        self.assertEqual(video_victim.read_bytes(), video_sentinel)
        self.assertTrue(video_candidate.is_symlink())
        self.assertFalse(video_target.exists())

        image_url = "https://p3-sign.douyinpic.com/alias-image.jpeg"
        image_groups = media.douyin_image_source_groups(
            [image_url], [[image_url]]
        )
        image_target_dir = self.root / "alias-images"
        image_temporary = image_target_dir / ".image-000.bin.attempt-0.tmp"
        image_victim = self.root / "external-image-victim"
        image_sentinel = b"external-image-victim-must-not-change"
        image_victim.write_bytes(image_sentinel)

        def open_image(*_args, **_kwargs):
            return AliasingResponse(
                b"\xff\xd8\xff" + b"I" * 700,
                url=image_url,
                alias=image_temporary,
                victim=image_victim,
            )

        with self.assertRaisesRegex(
            media.MediaProcessingError, "occupied or aliased"
        ):
            media._download_images(
                [image_url],
                image_target_dir,
                platform="douyin",
                frozen_image_groups=image_groups,
                urlopen_fn=open_image,
                maximum_bytes=10_000,
                require_exact_response_url=True,
                reuse_existing=False,
            )
        self.assertEqual(image_victim.read_bytes(), image_sentinel)
        self.assertTrue(image_temporary.is_symlink())
        self.assertFalse((image_target_dir / "image-000.bin").exists())
        self.assertFalse((image_target_dir / "manifest.json").exists())

        preexisting_video_target = self.root / "preexisting-video" / "video.mp4"
        preexisting_video_target.parent.mkdir()
        preexisting_video_candidate = preexisting_video_target.with_name(
            f".{preexisting_video_target.name}.candidate-0"
        )
        preexisting_video_bytes = b"preexisting-private-video-candidate"
        preexisting_video_candidate.write_bytes(preexisting_video_bytes)
        video_network_calls = 0

        def blocked_video_network(*_args, **_kwargs):
            nonlocal video_network_calls
            video_network_calls += 1
            raise AssertionError("network must not run")

        with self.assertRaisesRegex(
            media.MediaProcessingError, "occupied or aliased"
        ):
            media._download_video(
                [video_url],
                preexisting_video_target,
                urlopen_fn=blocked_video_network,
            )
        self.assertEqual(video_network_calls, 0)
        self.assertEqual(
            preexisting_video_candidate.read_bytes(), preexisting_video_bytes
        )

        preexisting_image_dir = self.root / "preexisting-images"
        preexisting_image_dir.mkdir()
        preexisting_image_candidate = (
            preexisting_image_dir / ".image-000.bin.attempt-0.tmp"
        )
        preexisting_image_bytes = b"preexisting-private-image-candidate"
        preexisting_image_candidate.write_bytes(preexisting_image_bytes)
        image_network_calls = 0

        def blocked_image_network(*_args, **_kwargs):
            nonlocal image_network_calls
            image_network_calls += 1
            raise AssertionError("network must not run")

        with self.assertRaisesRegex(
            media.MediaProcessingError, "candidate path is occupied"
        ):
            media._download_images(
                [image_url],
                preexisting_image_dir,
                platform="douyin",
                frozen_image_groups=image_groups,
                urlopen_fn=blocked_image_network,
                reuse_existing=False,
            )
        self.assertEqual(image_network_calls, 0)
        self.assertEqual(
            preexisting_image_candidate.read_bytes(), preexisting_image_bytes
        )

        class PlainResponse(io.BytesIO):
            def __init__(self, body: bytes, *, url: str) -> None:
                super().__init__(body)
                self.headers = {"Content-Length": str(len(body))}
                self._url = url

            def geturl(self) -> str:
                return self._url

            def __enter__(self):
                return self

            def __exit__(self, *_args) -> None:
                self.close()

        replacement_video_target = self.root / "replace-video" / "video.mp4"
        replacement_video_candidate = replacement_video_target.with_name(
            f".{replacement_video_target.name}.candidate-0"
        )
        replacement_video_sentinel = b"video-write-return-replacement"

        def replace_after_video_write(path: Path) -> None:
            replacement = path.with_name(f".{path.name}.external")
            replacement.write_bytes(replacement_video_sentinel)
            os.replace(replacement, path)

        with patch.object(
            media,
            "_after_bounded_response_write",
            side_effect=replace_after_video_write,
        ):
            with self.assertRaises(media.MediaProcessingError):
                media._download_video(
                    [video_url],
                    replacement_video_target,
                    urlopen_fn=lambda *_args, **_kwargs: PlainResponse(
                        b"W" * 4096, url=video_url
                    ),
                    maximum_bytes=10_000,
                    require_exact_response_url=True,
                )
        self.assertEqual(
            replacement_video_candidate.read_bytes(),
            replacement_video_sentinel,
        )
        self.assertFalse(replacement_video_target.exists())

        replacement_image_dir = self.root / "replace-images"
        replacement_image_temporary = (
            replacement_image_dir / ".image-000.bin.attempt-0.tmp"
        )
        replacement_image_sentinel = b"image-write-return-replacement"

        def replace_after_image_write(path: Path) -> None:
            replacement = path.with_name(f".{path.name}.external")
            replacement.write_bytes(replacement_image_sentinel)
            os.replace(replacement, path)

        with patch.object(
            media,
            "_after_bounded_response_write",
            side_effect=replace_after_image_write,
        ):
            with self.assertRaises(media.MediaProcessingError):
                media._download_images(
                    [image_url],
                    replacement_image_dir,
                    platform="douyin",
                    frozen_image_groups=image_groups,
                    urlopen_fn=lambda *_args, **_kwargs: PlainResponse(
                        b"\xff\xd8\xff" + b"R" * 700, url=image_url
                    ),
                    maximum_bytes=10_000,
                    require_exact_response_url=True,
                    reuse_existing=False,
                )
        self.assertEqual(
            replacement_image_temporary.read_bytes(),
            replacement_image_sentinel,
        )
        self.assertFalse((replacement_image_dir / "image-000.bin").exists())
        self.assertFalse((replacement_image_dir / "manifest.json").exists())

    def test_atomic_staging_never_exposes_partial_finals_and_resumes_source(
        self,
    ) -> None:
        class Response(io.BytesIO):
            def __init__(self, body: bytes, *, url: str) -> None:
                super().__init__(body)
                self.headers = {"Content-Length": str(len(body))}
                self._url = url

            def geturl(self) -> str:
                return self._url

            def __enter__(self):
                return self

            def __exit__(self, *_args) -> None:
                self.close()

        video_url = "https://cdn.example/atomic-positive-video.mp4"
        positive_video = self.root / "atomic-positive" / "source.mp4"

        def validate_spooled_video(path: Path, **_kwargs) -> bool:
            return str(path).startswith("/dev/fd/")

        with patch.object(
            media, "_valid_media", side_effect=validate_spooled_video
        ):
            media._download_video(
                [video_url],
                positive_video,
                urlopen_fn=lambda *_args, **_kwargs: Response(
                    b"V" * 4096, url=video_url
                ),
                maximum_bytes=10_000,
                require_exact_response_url=True,
            )
        self.assertEqual(positive_video.read_bytes(), b"V" * 4096)
        self.assertFalse(
            positive_video.with_name(
                f".{positive_video.name}.candidate-0"
            ).exists()
        )

        interrupted_video = self.root / "atomic-video" / "source.mp4"
        interrupted_video_staging = interrupted_video.with_name(
            f".{interrupted_video.name}.candidate-0"
        )

        def interrupt_video_copy(path: Path, _byte_size: int) -> None:
            if path == interrupted_video_staging:
                raise media.MediaProcessingError("simulated video copy crash")

        with (
            patch.object(media, "_valid_media", side_effect=validate_spooled_video),
            patch.object(
                media,
                "_after_private_staging_chunk",
                side_effect=interrupt_video_copy,
            ),
            self.assertRaises(media.MediaProcessingError),
        ):
            media._download_video(
                [video_url],
                interrupted_video,
                urlopen_fn=lambda *_args, **_kwargs: Response(
                    b"W" * (2 * 1024 * 1024), url=video_url
                ),
                maximum_bytes=3 * 1024 * 1024,
                require_exact_response_url=True,
            )
        self.assertFalse(interrupted_video.exists())
        self.assertEqual(interrupted_video_staging.stat().st_size, 1024 * 1024)
        with patch.object(media.urllib.request, "urlopen") as urlopen:
            with self.assertRaisesRegex(
                media.MediaProcessingError, "occupied or aliased"
            ):
                media._download_video([video_url], interrupted_video)
            urlopen.assert_not_called()

        image_url = "https://p3-sign.douyinpic.com/atomic-image.jpeg"
        groups = media.douyin_image_source_groups(
            [image_url], [[image_url]]
        )
        interrupted_images = self.root / "atomic-images"
        interrupted_image = interrupted_images / "image-000.bin"
        interrupted_image_staging = (
            interrupted_images / ".image-000.bin.tmp"
        )

        def interrupt_image_copy(path: Path, _byte_size: int) -> None:
            if path == interrupted_image_staging:
                raise media.MediaProcessingError("simulated image copy crash")

        with (
            patch.object(
                media,
                "_after_private_staging_chunk",
                side_effect=interrupt_image_copy,
            ),
            self.assertRaisesRegex(
                media.MediaProcessingError, "simulated image copy crash"
            ),
        ):
            media._download_images(
                [image_url],
                interrupted_images,
                platform="douyin",
                frozen_image_groups=groups,
                urlopen_fn=lambda *_args, **_kwargs: Response(
                    b"\xff\xd8\xff" + b"I" * (2 * 1024 * 1024),
                    url=image_url,
                ),
                maximum_bytes=3 * 1024 * 1024,
                require_exact_response_url=True,
                reuse_existing=False,
            )
        self.assertFalse(interrupted_image.exists())
        self.assertEqual(interrupted_image_staging.stat().st_size, 1024 * 1024)
        self.assertFalse((interrupted_images / "manifest.json").exists())
        with patch.object(media.urllib.request, "urlopen") as urlopen:
            with self.assertRaisesRegex(
                media.MediaProcessingError, "candidate path is occupied"
            ):
                media._download_images(
                    [image_url],
                    interrupted_images,
                    platform="douyin",
                    frozen_image_groups=groups,
                    reuse_existing=False,
                )
            urlopen.assert_not_called()

        interrupted_manifest_root = self.root / "atomic-manifest"
        interrupted_manifest = interrupted_manifest_root / "manifest.json"
        interrupted_manifest_staging = (
            interrupted_manifest_root / ".manifest.json.tmp"
        )

        def interrupt_manifest_copy(path: Path, _byte_size: int) -> None:
            if path == interrupted_manifest_staging:
                raise media.MediaProcessingError("simulated manifest copy crash")

        with (
            patch.object(
                media,
                "_after_private_staging_chunk",
                side_effect=interrupt_manifest_copy,
            ),
            self.assertRaisesRegex(
                media.MediaProcessingError, "simulated manifest copy crash"
            ),
        ):
            media._download_images(
                [image_url],
                interrupted_manifest_root,
                platform="douyin",
                frozen_image_groups=groups,
                urlopen_fn=lambda *_args, **_kwargs: Response(
                    b"\xff\xd8\xff" + b"M" * 700,
                    url=image_url,
                ),
                maximum_bytes=10_000,
                require_exact_response_url=True,
                reuse_existing=False,
            )
        self.assertTrue(
            (interrupted_manifest_root / "image-000.bin").is_file()
        )
        self.assertFalse(interrupted_manifest.exists())
        self.assertTrue(interrupted_manifest_staging.is_file())
        with patch.object(media.urllib.request, "urlopen") as urlopen:
            with self.assertRaisesRegex(
                media.MediaProcessingError, "candidate path is occupied"
            ):
                media._download_images(
                    [image_url],
                    interrupted_manifest_root,
                    platform="douyin",
                    frozen_image_groups=groups,
                    reuse_existing=False,
                )
            urlopen.assert_not_called()

        source_root = self.root / "atomic-source"
        source_url = "https://cdn.example/atomic-source.mp4"
        _values, source_sha256 = media._media_source_identity(
            "video", [source_url]
        )
        source_target = (
            source_root
            / "A2BC3D"
            / "sources"
            / f"source-990-{source_sha256[:12]}.json"
        )
        source_staging = source_target.with_name(f".{source_target.name}.tmp")

        def interrupt_source_copy(path: Path, _byte_size: int) -> None:
            if path == source_staging:
                raise media.MediaProcessingError("simulated source copy crash")

        with (
            patch.object(
                media,
                "_after_private_staging_chunk",
                side_effect=interrupt_source_copy,
            ),
            self.assertRaisesRegex(
                media.MediaProcessingError, "simulated source copy crash"
            ),
        ):
            media.store_media_source_manifest(
                1,
                media_kind="video",
                urls=[source_url],
                raw_response_id=990,
                db_path=self.db,
                media_root=source_root,
            )
        self.assertFalse(source_target.exists())
        self.assertTrue(source_staging.is_file())
        with connect(self.db) as connection:
            self.assertEqual(
                connection.execute(
                    "SELECT COUNT(*) FROM evidence_artifacts "
                    "WHERE artifact_type='media_source'"
                ).fetchone()[0],
                0,
            )
        artifact = media.store_media_source_manifest(
            1,
            media_kind="video",
            urls=[source_url],
            raw_response_id=990,
            db_path=self.db,
            media_root=source_root,
        )
        self.assertIsNotNone(artifact)
        self.assertTrue(source_target.is_file())
        self.assertFalse(source_staging.exists())

    def test_image_manifest_publish_is_no_clobber_and_never_follows_alias(
        self,
    ) -> None:
        image_url = "https://p3-sign.douyinpic.com/manifest-publish.jpeg"
        groups = media.douyin_image_source_groups(
            [image_url], [[image_url]]
        )
        image_body = b"\xff\xd8\xff" + b"P" * 700

        class Response(io.BytesIO):
            headers = {"Content-Length": str(len(image_body))}

            def geturl(self) -> str:
                return image_url

            def __enter__(self):
                return self

            def __exit__(self, *_args) -> None:
                self.close()

        collision_root = self.root / "manifest-final-collision"
        collision_manifest = collision_root / "manifest.json"
        collision_staging = collision_root / ".manifest.json.tmp"
        final_sentinel = b"concurrent-manifest-final"

        def occupy_final(path: Path) -> None:
            self.assertEqual(path, collision_manifest)
            path.write_bytes(final_sentinel)

        with (
            patch.object(
                media, "_before_image_manifest_publish", side_effect=occupy_final
            ),
            self.assertRaisesRegex(
                media.MediaProcessingError, "target already exists"
            ),
        ):
            media._download_images(
                [image_url],
                collision_root,
                platform="douyin",
                frozen_image_groups=groups,
                urlopen_fn=lambda *_args, **_kwargs: Response(image_body),
                maximum_bytes=10_000,
                require_exact_response_url=True,
                reuse_existing=False,
            )
        self.assertEqual(collision_manifest.read_bytes(), final_sentinel)
        self.assertTrue(collision_staging.is_file())

        alias_root = self.root / "manifest-staging-alias"
        alias_manifest = alias_root / "manifest.json"
        alias_staging = alias_root / ".manifest.json.tmp"
        alias_victim = self.root / "manifest-staging-victim"
        alias_sentinel = b"manifest-staging-victim-must-not-change"
        alias_victim.write_bytes(alias_sentinel)

        def occupy_staging(path: Path) -> None:
            self.assertEqual(path, alias_manifest)
            alias_staging.symlink_to(alias_victim)

        with (
            patch.object(
                media,
                "_before_image_manifest_publish",
                side_effect=occupy_staging,
            ),
            self.assertRaisesRegex(
                media.MediaProcessingError, "staging path"
            ),
        ):
            media._download_images(
                [image_url],
                alias_root,
                platform="douyin",
                frozen_image_groups=groups,
                urlopen_fn=lambda *_args, **_kwargs: Response(image_body),
                maximum_bytes=10_000,
                require_exact_response_url=True,
                reuse_existing=False,
            )
        self.assertEqual(alias_victim.read_bytes(), alias_sentinel)
        self.assertTrue(alias_staging.is_symlink())
        self.assertFalse(alias_manifest.exists())
        with connect(self.db) as connection:
            self.assertEqual(
                connection.execute(
                    "SELECT COUNT(*) FROM evidence_artifacts "
                    "WHERE artifact_type='media_manifest'"
                ).fetchone()[0],
                0,
            )

    def test_staging_publish_rejects_swapped_link_ancestor_before_outside_write(
        self,
    ) -> None:
        def swap_link_to_outside(media_root: Path, outside: Path) -> None:
            link = media_root / "A2BC3D"
            moved = media_root / "A2BC3D-owned-after-swap"
            os.replace(link, moved)
            link.symlink_to(outside, target_is_directory=True)

        source_root = self.root / "ancestor-source-root"
        outside_source = self.root / "ancestor-source-outside"
        (outside_source / "sources").mkdir(parents=True)
        source_url = "https://cdn.example/ancestor-source.mp4"
        _values, source_sha256 = media._media_source_identity(
            "video", [source_url]
        )
        source_target = (
            source_root
            / "A2BC3D"
            / "sources"
            / f"source-995-{source_sha256[:12]}.json"
        )

        def swap_source_ancestor(_target: Path) -> None:
            swap_link_to_outside(source_root, outside_source)

        with patch.object(
            media,
            "_before_media_source_final_create",
            side_effect=swap_source_ancestor,
        ):
            with self.assertRaisesRegex(
                media.MediaProcessingError,
                "symlink component|parent binding changed",
            ):
                media.store_media_source_manifest(
                    1,
                    media_kind="video",
                    urls=[source_url],
                    raw_response_id=995,
                    db_path=self.db,
                    media_root=source_root,
                )
        self.assertFalse(source_target.exists())
        self.assertFalse(
            any(
                path.is_file() or path.is_symlink()
                for path in outside_source.rglob("*")
            )
        )
        with connect(self.db) as connection:
            self.assertEqual(
                connection.execute(
                    "SELECT COUNT(*) FROM evidence_artifacts "
                    "WHERE artifact_type='media_source'"
                ).fetchone()[0],
                0,
            )

        class Response(io.BytesIO):
            def __init__(self, body: bytes, *, url: str) -> None:
                super().__init__(body)
                self.headers = {"Content-Length": str(len(body))}
                self._url = url

            def geturl(self) -> str:
                return self._url

            def __enter__(self):
                return self

            def __exit__(self, *_args) -> None:
                self.close()

        video_source_root = self.root / "ancestor-video-source"
        video_url = "https://cdn.example/ancestor-video.mp4"
        media.store_media_source_manifest(
            1,
            media_kind="video",
            urls=[video_url],
            raw_response_id=996,
            db_path=self.db,
            media_root=video_source_root,
        )
        _values, video_sha256 = media._media_source_identity(
            "video", [video_url]
        )
        video_root = self.root / "ancestor-video-root"
        outside_video = self.root / "ancestor-video-outside"
        (outside_video / "downloads" / video_sha256).mkdir(parents=True)
        video_hook_called = False

        def swap_video_ancestor(_candidate: Path) -> None:
            nonlocal video_hook_called
            if not video_hook_called:
                video_hook_called = True
                swap_link_to_outside(video_root, outside_video)

        with (
            patch.object(
                media,
                "_after_bounded_response_write",
                side_effect=swap_video_ancestor,
            ),
            patch.object(
                media,
                "_valid_media",
                side_effect=lambda path, **kwargs: bool(
                    kwargs.get("inherited_descriptor") is not None
                    and str(path).startswith("/dev/fd/")
                ),
            ),
            self.assertRaises(media.MediaProcessingError),
        ):
            media.download_video_sources(
                1,
                [video_url],
                db_path=self.db,
                media_root=video_root,
                urlopen_fn=lambda *_args, **_kwargs: Response(
                    b"V" * 4096, url=video_url
                ),
                maximum_bytes=10_000,
                require_exact_response_url=True,
                reuse_existing=False,
            )
        self.assertTrue(video_hook_called)
        self.assertFalse(
            any(
                path.is_file() or path.is_symlink()
                for path in outside_video.rglob("*")
            )
        )

        image_url = "https://p3-sign.douyinpic.com/ancestor-image.jpeg"
        groups = media.douyin_image_source_groups(
            [image_url], [[image_url]]
        )
        self._store_current_image_source([image_url], platform="douyin")
        _values, image_source_sha256 = media._media_source_identity(
            "image", [image_url]
        )
        image_binding = media.image_download_binding_sha256(
            image_source_sha256, media.image_groups_sha256(groups)
        )
        image_root = self.root / "ancestor-image-root"
        outside_image = self.root / "ancestor-image-outside"
        (outside_image / "downloads" / image_binding / "images").mkdir(
            parents=True
        )
        image_hook_called = False

        def swap_image_ancestor(_candidate: Path) -> None:
            nonlocal image_hook_called
            if not image_hook_called:
                image_hook_called = True
                swap_link_to_outside(image_root, outside_image)

        with (
            patch.object(
                media,
                "_after_bounded_response_write",
                side_effect=swap_image_ancestor,
            ),
            self.assertRaises(media.MediaProcessingError),
        ):
            media.download_image_sources(
                1,
                [image_url],
                db_path=self.db,
                media_root=image_root,
                frozen_image_groups=groups,
                urlopen_fn=lambda *_args, **_kwargs: Response(
                    b"\xff\xd8\xff" + b"I" * 700,
                    url=image_url,
                ),
                maximum_bytes=10_000,
                require_exact_response_url=True,
                reuse_existing=False,
            )
        self.assertTrue(image_hook_called)
        self.assertFalse(
            any(
                path.is_file() or path.is_symlink()
                for path in outside_image.rglob("*")
            )
        )
        with connect(self.db) as connection:
            self.assertEqual(
                connection.execute(
                    "SELECT COUNT(*) FROM evidence_artifacts "
                    "WHERE artifact_type IN ('media','media_manifest')"
                ).fetchone()[0],
                0,
            )
            statuses = {
                row["status"]
                for row in connection.execute(
                    "SELECT status FROM media_processing_slots "
                    "WHERE processor_type='download'"
                ).fetchall()
            }
            self.assertNotIn("succeeded", statuses)

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
        def fake_download(urls, target_dir: Path, **_kwargs) -> Path:
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
            self._store_current_image_source(
                ["https://cdn.example/first.jpg"]
            )
            first = media.download_image_sources(
                1, ["https://cdn.example/first.jpg"], db_path=self.db
            )
            first_manifest = media._resolved(first.local_path)
            first_image = first_manifest.parent / "image-000.bin"
            first_bytes = first_image.read_bytes()
            self._store_current_image_source(
                ["https://cdn.example/second.jpg"]
            )
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

    def test_partial_image_download_is_retryable_and_never_signed_succeeded(self) -> None:
        urls = [
            "https://cdn.example/image-1.jpg",
            "https://cdn.example/image-2.jpg",
        ]
        image_bytes = b"\xff\xd8\xff" + b"image" * 200

        class Response:
            def __init__(self, url: str) -> None:
                self.url = url
                self.headers = {"Content-Length": str(len(image_bytes))}
                self.body = io.BytesIO(image_bytes)

            def read(self, size: int = -1) -> bytes:
                return self.body.read(size)

            def geturl(self) -> str:
                return self.url

            def __enter__(self):
                return self

            def __exit__(self, *_args):
                return None

        first_calls = 0

        def partial_open(request, **_kwargs):
            nonlocal first_calls
            first_calls += 1
            if first_calls == 2:
                raise OSError("second image failed")
            return Response(request.full_url)

        output_root = self.root / "partial-images"
        self._store_current_image_source(urls)
        with self.assertRaisesRegex(media.MediaProcessingError, "incomplete"):
            media.download_image_sources(
                1,
                urls,
                db_path=self.db,
                media_root=output_root,
                urlopen_fn=partial_open,
                maximum_bytes=10_000,
                require_exact_response_url=True,
                reuse_existing=False,
            )
        with connect(self.db) as connection:
            failed = connection.execute(
                "SELECT status,attempt_count,output_artifact_id "
                "FROM media_processing_slots WHERE processor_type='download'"
            ).fetchone()
        self.assertEqual(tuple(failed), ("retryable_failed", 1, None))
        self.assertEqual(list(output_root.rglob("image-*.bin")), [])
        self.assertEqual(list(output_root.rglob("manifest.json")), [])

        artifact = media.download_image_sources(
            1,
            urls,
            db_path=self.db,
            media_root=output_root,
            urlopen_fn=lambda request, **_kwargs: Response(request.full_url),
            maximum_bytes=10_000,
            require_exact_response_url=True,
            reuse_existing=False,
        )
        with connect(self.db) as connection:
            succeeded = connection.execute(
                "SELECT status,attempt_count,output_artifact_id "
                "FROM media_processing_slots WHERE processor_type='download'"
            ).fetchone()
        self.assertEqual(
            tuple(succeeded), ("succeeded", 2, artifact.id)
        )

    def test_xhs_image_groups_only_exact_preview_detail_signature_pair(self) -> None:
        preview = (
            "https://sns-i11.rednotecdn.com/notes_pre_post/example?"
            "imageView2/2/w/576/format/webp/q/87%7CimageMogr2/strip&"
            "redImage/frame/0&ap=12&sc=USR_PRV&sign=abc&t=123&src=A&origin=0"
        )
        detail = (
            "https://sns-i11.rednotecdn.com/notes_pre_post/example?"
            "imageView2/2/w/1440/format/webp&ap=12&sc=USR_DTL&"
            "sign=abc&t=123&src=A&origin=0"
        )
        different_signature = detail.replace("sign=abc", "sign=other")
        different_host = detail.replace("sns-i11", "sns-i27")

        groups = media.image_source_groups(
            [preview, detail, different_signature, different_host],
            platform="xiaohongshu",
        )

        self.assertEqual(len(groups), 3)
        self.assertEqual(groups[0]["identity"]["kind"], "xhs-preview-detail-v1")
        self.assertEqual(
            [candidate["profile"] for candidate in groups[0]["candidates"]],
            ["detail-1440-webp-v1", "preview-576-webp-v1"],
        )
        self.assertEqual(
            [candidate["source_index"] for candidate in groups[0]["candidates"]],
            [1, 0],
        )
        self.assertTrue(
            all(group["identity"]["kind"] == "exact-url-v1" for group in groups[1:])
        )
        douyin_groups = media.image_source_groups(
            [preview, detail], platform="douyin"
        )
        self.assertEqual(len(douyin_groups), 2)
        self.assertTrue(
            all(
                group["identity"]["kind"] == "exact-url-v1"
                for group in douyin_groups
            )
        )

    def test_douyin_frozen_groups_bind_every_source_url_in_original_order(self) -> None:
        urls = [
            f"https://p3-sign.douyinpic.com/image-{index}"
            for index in range(9)
        ]
        groups = media.douyin_image_source_groups(
            urls, [urls[:4], urls[4:]]
        )

        self.assertEqual(len(groups), 2)
        self.assertEqual(
            [len(group["candidates"]) for group in groups], [4, 5]
        )
        self.assertEqual(
            [
                candidate["source_index"]
                for group in groups
                for candidate in group["candidates"]
            ],
            list(range(9)),
        )
        self.assertTrue(
            all(
                group["identity"]
                == {
                    "kind": "douyin-discovery-image-v1",
                    "platform": "douyin",
                    "image_index": group_index,
                }
                for group_index, group in enumerate(groups)
            )
        )
        self.assertEqual(
            media.validate_frozen_image_groups(
                urls, groups, platform="douyin"
            ),
            groups,
        )
        with self.assertRaisesRegex(
            media.MediaProcessingError, "content platform"
        ):
            media.validate_frozen_image_groups(
                urls, groups, platform="xiaohongshu"
            )

    def test_frozen_image_group_validation_blocks_drift_before_network(self) -> None:
        urls = [
            f"https://p3-sign.douyinpic.com/image-{index}"
            for index in range(8)
        ]
        baseline = media.douyin_image_source_groups(
            urls, [urls[:4], urls[4:]]
        )
        mutations = {}

        missing = json.loads(json.dumps(baseline))
        missing[0]["candidates"].pop()
        mutations["missing source index"] = missing

        duplicate = json.loads(json.dumps(baseline))
        duplicate[1]["candidates"][0]["source_index"] = 3
        duplicate[1]["candidates"][0]["url"] = urls[3]
        duplicate[1]["candidates"][0]["url_sha256"] = media._image_url_sha256(
            urls[3]
        )
        mutations["duplicate source index"] = duplicate

        bad_sha = json.loads(json.dumps(baseline))
        bad_sha[0]["candidates"][0]["url_sha256"] = "0" * 64
        mutations["URL SHA"] = bad_sha

        nonconsecutive = json.loads(json.dumps(baseline))
        nonconsecutive[1]["group_index"] = 3
        mutations["group index"] = nonconsecutive

        reordered = json.loads(json.dumps(baseline))
        reordered.reverse()
        mutations["group index"] = reordered

        candidate_reordered = json.loads(json.dumps(baseline))
        candidate_reordered[0]["candidates"].reverse()
        mutations["candidate order"] = candidate_reordered

        forged_identity = json.loads(json.dumps(baseline))
        forged_identity[0]["identity"]["image_index"] = 99
        mutations["identity"] = forged_identity

        false_identity = json.loads(json.dumps(baseline))
        false_identity[0]["identity"]["image_index"] = False
        mutations["identity fields"] = false_identity

        true_identity = json.loads(json.dumps(baseline))
        true_identity[1]["identity"]["image_index"] = True
        mutations["group identity fields"] = true_identity

        forged_profile = json.loads(json.dumps(baseline))
        forged_profile[0]["candidates"][0]["profile"] = "forged"
        mutations["profile"] = forged_profile

        for message, groups in mutations.items():
            with self.subTest(message=message):
                with patch.object(media.urllib.request, "urlopen") as urlopen:
                    with self.assertRaisesRegex(media.MediaProcessingError, message):
                        media._download_images(
                            urls,
                            self.root / f"invalid-{len(urlopen.mock_calls)}",
                            platform="douyin",
                            frozen_image_groups=groups,
                        )
                    urlopen.assert_not_called()

    def test_image_group_and_download_contract_rejects_non_exact_string_urls(
        self,
    ) -> None:
        class DerivedString(str):
            pass

        valid_url = "https://p3-sign.douyinpic.com/exact-string.jpeg"
        invalid_values = [7, Path(valid_url), DerivedString(valid_url)]
        for value in invalid_values:
            with self.subTest(value_type=type(value).__name__):
                with self.assertRaisesRegex(
                    media.MediaProcessingError, "exact strings"
                ):
                    media.image_source_groups([value], platform="douyin")
                with self.assertRaisesRegex(
                    media.MediaProcessingError, "exact strings"
                ):
                    media.douyin_image_source_groups([value], [[value]])
                with patch.object(media.urllib.request, "urlopen") as urlopen:
                    with self.assertRaisesRegex(
                        media.MediaProcessingError, "exact strings"
                    ):
                        media._download_images(
                            [value],
                            self.root / "invalid-exact-string",
                            platform="douyin",
                        )
                    with self.assertRaisesRegex(
                        media.MediaProcessingError, "exact strings"
                    ):
                        media.download_image_sources(
                            1,
                            [value],
                            db_path=self.db,
                            media_root=self.root / "invalid-exact-string-public",
                        )
                    urlopen.assert_not_called()

        groups = media.douyin_image_source_groups([valid_url], [[valid_url]])
        groups[0]["candidates"][0]["url"] = DerivedString(valid_url)
        with self.assertRaisesRegex(media.MediaProcessingError, "candidate URL"):
            media.validate_frozen_image_groups(
                [valid_url], groups, platform="douyin"
            )

    def test_douyin_candidate_fallback_outputs_one_file_per_logical_image(self) -> None:
        urls = [
            "https://p3-sign.douyinpic.com/water/image.webp",
            "https://p3-sign.douyinpic.com/water/image.jpeg",
            "https://p3-sign.douyinpic.com/aweme-images-v2/image.heic",
            "https://p3-sign.douyinpic.com/aweme-images-v2/image.jpeg",
            "https://p11-sign.douyinpic.com/tos-cn-i/image.vvic",
        ]
        groups = media.douyin_image_source_groups(urls, [urls])
        invalid_heic = b"\x00\x00\x00\x18ftypheic" + b"H" * 700
        valid_jpeg = b"\xff\xd8\xff" + b"J" * 700

        class Response:
            def __init__(self, url: str, body: bytes) -> None:
                self.url = url
                self.body = io.BytesIO(body)
                self.headers = {"Content-Length": str(len(body))}

            def read(self, size: int = -1) -> bytes:
                return self.body.read(size)

            def geturl(self) -> str:
                return self.url

            def __enter__(self):
                return self

            def __exit__(self, *_args):
                return None

        calls: list[str] = []

        def open_candidate(request, **_kwargs):
            calls.append(request.full_url)
            if len(calls) == 1:
                raise TimeoutError("first candidate timed out")
            if len(calls) == 2:
                return Response(request.full_url, invalid_heic)
            return Response(request.full_url, valid_jpeg)

        manifest_path = media._download_images(
            urls,
            self.root / "douyin-fallback",
            platform="douyin",
            frozen_image_groups=groups,
            urlopen_fn=open_candidate,
            maximum_bytes=10_000,
            require_exact_response_url=True,
            reuse_existing=False,
        )
        manifest = json.loads(manifest_path.read_text())

        self.assertEqual(calls, urls[:3])
        self.assertEqual(manifest["source_url_count"], 5)
        self.assertEqual(manifest["source_count"], 1)
        self.assertEqual(len(manifest["image_paths"]), 1)
        self.assertEqual(
            [attempt["outcome"] for attempt in manifest["groups"][0]["attempts"]],
            ["request_failed", "unsupported_image", "selected"],
        )

    def test_image_request_failures_project_partial_and_empty_response_evidence(
        self,
    ) -> None:
        urls = [
            "https://p3-sign.douyinpic.com/pre-open.jpeg",
            "https://p3-sign.douyinpic.com/partial.jpeg",
            "https://p3-sign.douyinpic.com/redirect.jpeg",
            "https://p3-sign.douyinpic.com/selected.jpeg",
        ]
        groups = media.douyin_image_source_groups(urls, [urls])
        partial_body = b"\xff\xd8\xff" + b"P" * 97
        selected_body = b"\xff\xd8\xff" + b"S" * 700

        class PartialResponse:
            headers = {}

            def __init__(self) -> None:
                self.read_count = 0

            def read(self, _size: int = -1) -> bytes:
                self.read_count += 1
                if self.read_count == 1:
                    return partial_body
                raise OSError("partial read failed")

            def geturl(self) -> str:
                return urls[1]

            def __enter__(self):
                return self

            def __exit__(self, *_args):
                return None

        class RedirectResponse:
            headers = {}

            def read(self, _size: int = -1) -> bytes:
                self.fail("redirect response body must not be read")

            def geturl(self) -> str:
                return "https://p3-sign.douyinpic.com/changed.jpeg"

            def __enter__(self):
                return self

            def __exit__(self, *_args):
                return None

        class SelectedResponse:
            headers = {"Content-Length": str(len(selected_body))}

            def __init__(self) -> None:
                self.body = io.BytesIO(selected_body)

            def read(self, size: int = -1) -> bytes:
                return self.body.read(size)

            def geturl(self) -> str:
                return urls[3]

            def __enter__(self):
                return self

            def __exit__(self, *_args):
                return None

        def open_candidate(request, **_kwargs):
            if request.full_url == urls[0]:
                raise TimeoutError("failed before response open")
            if request.full_url == urls[1]:
                return PartialResponse()
            if request.full_url == urls[2]:
                return RedirectResponse()
            return SelectedResponse()

        manifest_path = media._download_images(
            urls,
            self.root / "response-failure-evidence",
            platform="douyin",
            frozen_image_groups=groups,
            urlopen_fn=open_candidate,
            maximum_bytes=10_000,
            require_exact_response_url=True,
            reuse_existing=False,
        )
        manifest = json.loads(manifest_path.read_text())
        attempts = manifest["groups"][0]["attempts"]
        self.assertEqual(
            [attempt["outcome"] for attempt in attempts],
            ["request_failed", "request_failed", "request_failed", "selected"],
        )
        self.assertEqual(
            (
                attempts[0]["response_sha256"],
                attempts[0]["byte_size"],
                attempts[0]["error"],
            ),
            (None, 0, "TimeoutError"),
        )
        self.assertEqual(
            (attempts[1]["response_sha256"], attempts[1]["byte_size"], attempts[1]["error"]),
            (hashlib.sha256(partial_body).hexdigest(), len(partial_body), "OSError"),
        )
        self.assertEqual(
            (attempts[2]["response_sha256"], attempts[2]["byte_size"], attempts[2]["error"]),
            (hashlib.sha256(b"").hexdigest(), 0, "MediaProcessingError"),
        )
        forged = json.loads(json.dumps(manifest))
        forged["groups"][0]["attempts"][2]["response_sha256"] = "0" * 64
        with self.assertRaisesRegex(
            media.MediaProcessingError, "request failure evidence"
        ):
            media._validate_current_grouped_image_manifest(
                manifest_path,
                forged,
                source_urls=urls,
                platform="douyin",
                frozen_image_groups=groups,
            )

    def test_douyin_public_image_entries_require_frozen_groups_before_writes(
        self,
    ) -> None:
        url = "https://p3-sign.douyinpic.com/requires-groups.jpeg"
        with connect(self.db) as connection:
            connection.execute(
                "UPDATE content_items SET platform='douyin',content_type='image' "
                "WHERE id=1"
            )
            connection.commit()
            before = (
                connection.execute(
                    "SELECT COUNT(*) FROM media_processing_slots"
                ).fetchone()[0],
                connection.execute(
                    "SELECT COUNT(*) FROM evidence_artifacts"
                ).fetchone()[0],
            )
        target = self.root / "missing-douyin-groups"
        with patch.object(media.urllib.request, "urlopen") as urlopen:
            for operation in (
                lambda: media._download_images(
                    [url], target, platform="douyin"
                ),
                lambda: media.download_image_sources(
                    1,
                    [url],
                    db_path=self.db,
                    media_root=target,
                ),
                lambda: media.process_image_evidence(
                    1,
                    target / "manifest.json",
                    db_path=self.db,
                    media_root=target,
                ),
            ):
                with self.subTest(operation=operation):
                    with self.assertRaisesRegex(
                        media.MediaProcessingError, "frozen discovery groups"
                    ):
                        operation()
            urlopen.assert_not_called()
        self.assertFalse(target.exists())
        with connect(self.db) as connection:
            after = (
                connection.execute(
                    "SELECT COUNT(*) FROM media_processing_slots"
                ).fetchone()[0],
                connection.execute(
                    "SELECT COUNT(*) FROM evidence_artifacts"
                ).fetchone()[0],
            )
        self.assertEqual(after, before)

    def test_image_download_binding_separates_same_urls_with_different_groups(
        self,
    ) -> None:
        urls = [
            f"https://p3-sign.douyinpic.com/binding-{index}.jpeg"
            for index in range(4)
        ]
        first_groups = media.douyin_image_source_groups(
            urls, [urls[:2], urls[2:]]
        )
        second_groups = media.douyin_image_source_groups(
            urls, [urls[:1], urls[1:]]
        )
        flat_sha = media._media_source_identity("image", urls)[1]
        first_groups_sha = media.image_groups_sha256(first_groups)
        second_groups_sha = media.image_groups_sha256(second_groups)
        first_binding = media.image_download_binding_sha256(
            flat_sha, first_groups_sha
        )
        second_binding = media.image_download_binding_sha256(
            flat_sha, second_groups_sha
        )
        self.assertNotEqual(first_groups_sha, second_groups_sha)
        self.assertNotEqual(first_binding, second_binding)

        image_bytes = b"\xff\xd8\xff" + b"I" * 700

        class Response:
            def __init__(self, url: str) -> None:
                self.url = url
                self.body = io.BytesIO(image_bytes)
                self.headers = {"Content-Length": str(len(image_bytes))}

            def read(self, size: int = -1) -> bytes:
                return self.body.read(size)

            def geturl(self) -> str:
                return self.url

            def __enter__(self):
                return self

            def __exit__(self, *_args):
                return None

        calls: list[str] = []

        def open_image(request, **_kwargs):
            calls.append(request.full_url)
            return Response(request.full_url)

        with connect(self.db) as connection:
            connection.execute(
                "UPDATE content_items SET platform='douyin',content_type='image' "
                "WHERE id=1"
            )
            connection.commit()
        self._store_current_image_source(urls, platform="douyin")
        output_root = self.root / "binding-images"
        first = media.download_image_sources(
            1,
            urls,
            db_path=self.db,
            media_root=output_root,
            frozen_image_groups=first_groups,
            urlopen_fn=open_image,
            maximum_bytes=10_000,
            reuse_existing=False,
        )
        second = media.download_image_sources(
            1,
            urls,
            db_path=self.db,
            media_root=output_root,
            frozen_image_groups=second_groups,
            urlopen_fn=open_image,
            maximum_bytes=10_000,
            reuse_existing=False,
        )

        self.assertNotEqual(first.id, second.id)
        self.assertNotEqual(first.local_path, second.local_path)
        self.assertIn(first_binding, first.local_path)
        self.assertIn(second_binding, second.local_path)
        self.assertEqual(calls, [urls[0], urls[2], urls[0], urls[1]])
        with connect(self.db) as connection:
            slots = connection.execute(
                "SELECT source_sha256,status FROM media_processing_slots "
                "WHERE content_id=1 AND processor_type='download' ORDER BY id"
            ).fetchall()
            artifacts = connection.execute(
                "SELECT local_path,metadata_json FROM evidence_artifacts "
                "WHERE id IN (?,?) ORDER BY id",
                (first.id, second.id),
            ).fetchall()
        self.assertEqual(
            [(row["source_sha256"], row["status"]) for row in slots],
            [(first_binding, "succeeded"), (second_binding, "succeeded")],
        )
        for row, groups_sha, binding in zip(
            artifacts,
            (first_groups_sha, second_groups_sha),
            (first_binding, second_binding),
            strict=True,
        ):
            metadata = json.loads(row["metadata_json"])
            self.assertEqual(
                metadata,
                {
                    "source_count": 2,
                    "source_url_count": 4,
                    "source_sha256": flat_sha,
                    "image_groups_sha256": groups_sha,
                    "download_binding_sha256": binding,
                },
            )
            manifest = json.loads(Path(row["local_path"]).read_text())
            self.assertEqual(manifest["source_sha256"], flat_sha)
            self.assertEqual(manifest["image_groups_sha256"], groups_sha)
            self.assertEqual(manifest["download_binding_sha256"], binding)

    def test_cached_grouped_image_download_revalidates_exact_closure(self) -> None:
        urls = [
            f"https://p3-sign.douyinpic.com/cached-{index}.jpeg"
            for index in range(2)
        ]
        groups = media.douyin_image_source_groups(urls, [[urls[0]], [urls[1]]])
        image_bytes = b"\xff\xd8\xff" + b"cached-image" * 80

        class Response:
            def __init__(self, url: str) -> None:
                self.url = url
                self.body = io.BytesIO(image_bytes)
                self.headers = {"Content-Length": str(len(image_bytes))}

            def read(self, size: int = -1) -> bytes:
                return self.body.read(size)

            def geturl(self) -> str:
                return self.url

            def __enter__(self):
                return self

            def __exit__(self, *_args):
                return None

        with connect(self.db) as connection:
            connection.execute(
                "UPDATE content_items SET platform='douyin',content_type='image' "
                "WHERE id=1"
            )
            connection.commit()
        self._store_current_image_source(urls, platform="douyin")
        output_root = self.root / "cached-grouped"
        artifact = media.download_image_sources(
            1,
            urls,
            db_path=self.db,
            media_root=output_root,
            frozen_image_groups=groups,
            urlopen_fn=lambda request, **_kwargs: Response(request.full_url),
            reuse_existing=False,
        )
        manifest_path = Path(artifact.local_path)
        manifest_bytes = manifest_path.read_bytes()
        image_path = manifest_path.parent / "image-000.bin"
        image_original = image_path.read_bytes()
        with connect(self.db) as connection:
            original_row = dict(
                connection.execute(
                    "SELECT * FROM evidence_artifacts WHERE id=?", (artifact.id,)
                ).fetchone()
            )
            counts = (
                connection.execute(
                    "SELECT COUNT(*) FROM media_processing_slots"
                ).fetchone()[0],
                connection.execute(
                    "SELECT COUNT(*) FROM evidence_artifacts"
                ).fetchone()[0],
            )

        def restore() -> None:
            manifest_path.write_bytes(manifest_bytes)
            image_path.write_bytes(image_original)
            with connect(self.db) as connection:
                connection.execute(
                    """
                    UPDATE evidence_artifacts SET local_path=?,status=?,byte_size=?,
                        sha256=?,processor_version=?,metadata_json=? WHERE id=?
                    """,
                    (
                        original_row["local_path"],
                        original_row["status"],
                        original_row["byte_size"],
                        original_row["sha256"],
                        original_row["processor_version"],
                        original_row["metadata_json"],
                        artifact.id,
                    ),
                )
                connection.commit()

        mutations = []

        def metadata_drift() -> None:
            metadata = json.loads(original_row["metadata_json"])
            metadata["source_count"] = True
            with connect(self.db) as connection:
                connection.execute(
                    "UPDATE evidence_artifacts SET metadata_json=? WHERE id=?",
                    (json.dumps(metadata, sort_keys=True), artifact.id),
                )
                connection.commit()

        mutations.append(metadata_drift)

        def path_drift() -> None:
            outside = self.root / "equivalent-manifest.json"
            outside.write_bytes(manifest_bytes)
            with connect(self.db) as connection:
                connection.execute(
                    "UPDATE evidence_artifacts SET local_path=? WHERE id=?",
                    (str(outside), artifact.id),
                )
                connection.commit()

        mutations.append(path_drift)

        def sha_drift() -> None:
            with connect(self.db) as connection:
                connection.execute(
                    "UPDATE evidence_artifacts SET sha256=? WHERE id=?",
                    ("0" * 64, artifact.id),
                )
                connection.commit()

        mutations.append(sha_drift)

        def group_drift() -> None:
            body = json.loads(manifest_bytes)
            body["image_groups_sha256"] = "0" * 64
            media._atomic_json(manifest_path, body)
            with connect(self.db) as connection:
                connection.execute(
                    "UPDATE evidence_artifacts SET byte_size=?,sha256=? WHERE id=?",
                    (
                        manifest_path.stat().st_size,
                        media.file_sha256(manifest_path),
                        artifact.id,
                    ),
                )
                connection.commit()

        mutations.append(group_drift)
        mutations.append(lambda: image_path.write_bytes(b"\xff\xd8\xff" + b"x" * 900))

        for mutation in mutations:
            with self.subTest(mutation=mutation):
                restore()
                mutation()
                with patch.object(media.urllib.request, "urlopen") as urlopen:
                    with self.assertRaises(media.MediaProcessingError):
                        media.download_image_sources(
                            1,
                            urls,
                            db_path=self.db,
                            media_root=output_root,
                            frozen_image_groups=groups,
                        )
                    urlopen.assert_not_called()
                with connect(self.db) as connection:
                    self.assertEqual(
                        (
                            connection.execute(
                                "SELECT COUNT(*) FROM media_processing_slots"
                            ).fetchone()[0],
                            connection.execute(
                                "SELECT COUNT(*) FROM evidence_artifacts"
                            ).fetchone()[0],
                        ),
                        counts,
                    )
        restore()

    def test_image_download_rejects_duplicate_urls_and_unsafe_cleanup_entries(
        self,
    ) -> None:
        urls = [
            "https://p3-sign.douyinpic.com/cleanup-0.jpeg",
            "https://p3-sign.douyinpic.com/cleanup-1.jpeg",
        ]
        groups = media.douyin_image_source_groups(urls, [urls])
        with patch.object(media.urllib.request, "urlopen") as urlopen:
            with self.assertRaisesRegex(
                media.MediaProcessingError, "duplicate or noncanonical"
            ):
                media._download_images(
                    [urls[0], urls[0]],
                    self.root / "duplicate-cleanup",
                    platform="douyin",
                )
            urlopen.assert_not_called()

        external = self.root / "external.bin"
        external.write_bytes(b"external")
        cases: list[tuple[str, callable]] = []

        def unknown(target: Path) -> Path:
            path = target / "unknown.bin"
            path.write_bytes(b"unknown")
            return path

        def symlink(target: Path) -> Path:
            path = target / "image-000.bin"
            path.symlink_to(external)
            return path

        def hardlink(target: Path) -> Path:
            path = target / "image-000.bin"
            os.link(external, path)
            return path

        cases.extend(
            [("unknown entry", unknown), ("private file", symlink), ("private file", hardlink)]
        )
        for index, (message, arrange) in enumerate(cases):
            with self.subTest(message=message, index=index):
                target = self.root / f"unsafe-cleanup-{index}"
                target.mkdir()
                unsafe = arrange(target)
                with patch.object(media.urllib.request, "urlopen") as urlopen:
                    with self.assertRaisesRegex(
                        media.MediaProcessingError, message
                    ):
                        media._download_images(
                            urls,
                            target,
                            platform="douyin",
                            frozen_image_groups=groups,
                            reuse_existing=False,
                        )
                    urlopen.assert_not_called()
                self.assertTrue(os.path.lexists(unsafe))

        recoverable = self.root / "recoverable-manifest-temp"
        recoverable.mkdir()
        manifest_temp = recoverable / ".manifest.json.tmp"
        manifest_temp.write_bytes(b"owned-temp")
        with patch.object(media.urllib.request, "urlopen") as urlopen:
            with self.assertRaisesRegex(
                media.MediaProcessingError, "candidate path is occupied"
            ):
                media._download_images(
                    urls,
                    recoverable,
                    platform="douyin",
                    frozen_image_groups=groups,
                    reuse_existing=False,
                )
            urlopen.assert_not_called()
        self.assertEqual(manifest_temp.read_bytes(), b"owned-temp")

    def test_download_targets_reject_unsafe_link_ids_and_parent_symlinks_before_io(
        self,
    ) -> None:
        url = "https://p3-sign.douyinpic.com/safe-target.jpeg"
        groups = media.douyin_image_source_groups([url], [[url]])
        media_root = self.root / "safe-download-root"
        outside = self.root / "outside-download-root"
        outside.mkdir()
        outside_sentinel = outside / "image-000.bin"
        outside_sentinel.write_bytes(b"outside-sentinel")
        self._store_current_image_source([url], platform="douyin")
        with connect(self.db) as connection:
            connection.execute(
                "UPDATE content_items SET platform='douyin',content_type='image',"
                "link_id='../abc' WHERE id=1"
            )
            connection.commit()
        before = outside_sentinel.read_bytes()
        with patch.object(media.urllib.request, "urlopen") as urlopen:
            with self.assertRaisesRegex(media.MediaProcessingError, "safe six-character"):
                media.download_image_sources(
                    1,
                    [url],
                    db_path=self.db,
                    media_root=media_root,
                    frozen_image_groups=groups,
                    reuse_existing=False,
                )
            with connect(self.db) as connection:
                connection.execute(
                    "UPDATE content_items SET content_type='video' WHERE id=1"
                )
                connection.execute(
                    "UPDATE evidence_artifacts SET status='missing' "
                    "WHERE content_id=1 AND artifact_type='media_source'"
                )
                connection.commit()
            with self.assertRaisesRegex(media.MediaProcessingError, "safe six-character"):
                media.download_video_sources(
                    1,
                    ["https://cdn.example/unsafe-link.mp4"],
                    db_path=self.db,
                    media_root=media_root,
                    reuse_existing=False,
                )
            urlopen.assert_not_called()
        self.assertEqual(outside_sentinel.read_bytes(), before)

        with connect(self.db) as connection:
            connection.execute(
                "UPDATE content_items SET link_id='ABC123',content_type='image' "
                "WHERE id=1"
            )
            connection.execute(
                "UPDATE evidence_artifacts SET status='available' "
                "WHERE content_id=1 AND artifact_type='media_source'"
            )
            connection.commit()
        link_target = media_root / "ABC123"
        media_root.mkdir()
        link_target.symlink_to(outside, target_is_directory=True)
        with patch.object(media.urllib.request, "urlopen") as urlopen:
            with self.assertRaisesRegex(
                media.MediaProcessingError, "symlink component"
            ):
                media.download_image_sources(
                    1,
                    [url],
                    db_path=self.db,
                    media_root=media_root,
                    frozen_image_groups=groups,
                    reuse_existing=False,
                )
            urlopen.assert_not_called()
        self.assertEqual(outside_sentinel.read_bytes(), before)
        with connect(self.db) as connection:
            self.assertEqual(
                connection.execute(
                    "SELECT COUNT(*) FROM media_processing_slots "
                    "WHERE processor_type='download'"
                ).fetchone()[0],
                0,
            )

    def test_xhs_image_variant_falls_back_with_grouped_manifest_provenance(self) -> None:
        preview = (
            "https://sns-i11.rednotecdn.com/notes_pre_post/example?"
            "imageView2/2/w/576/format/webp/q/87%7CimageMogr2/strip&"
            "redImage/frame/0&ap=12&sc=USR_PRV&sign=abc&t=123&src=A&origin=0"
        )
        detail = (
            "https://sns-i11.rednotecdn.com/notes_pre_post/example?"
            "imageView2/2/w/1440/format/webp&ap=12&sc=USR_DTL&"
            "sign=abc&t=123&src=A&origin=0"
        )
        image_bytes = b"RIFF" + b"\x00" * 4 + b"WEBP" + b"image" * 200

        class Response:
            headers = {"Content-Length": str(len(image_bytes))}

            def __init__(self, url: str) -> None:
                self.url = url
                self.body = io.BytesIO(image_bytes)

            def read(self, size: int = -1) -> bytes:
                return self.body.read(size)

            def geturl(self) -> str:
                return self.url

            def __enter__(self):
                return self

            def __exit__(self, *_args):
                return None

        calls: list[str] = []

        def variant_open(request, **_kwargs):
            calls.append(request.full_url)
            if request.full_url == detail:
                raise urllib.error.HTTPError(detail, 498, "expired", {}, None)
            return Response(request.full_url)

        output_root = self.root / "grouped-images"
        with connect(self.db) as connection:
            connection.execute(
                "UPDATE content_items SET content_type='image' WHERE id=1"
            )
            connection.commit()
        media.store_media_source_manifest(
            1,
            media_kind="image",
            urls=[preview, detail],
            raw_response_id=700,
            db_path=self.db,
            media_root=self.root / "grouped-image-sources",
        )
        artifact = media.download_image_sources(
            1,
            [preview, detail],
            db_path=self.db,
            media_root=output_root,
            urlopen_fn=variant_open,
            maximum_bytes=10_000,
            require_exact_response_url=True,
            reuse_existing=False,
        )
        manifest_path = media._resolved(artifact.local_path)
        target = manifest_path.parent
        manifest = json.loads(manifest_path.read_text())

        self.assertEqual(calls, [detail, preview])
        self.assertEqual(manifest["schema_version"], media.IMAGE_MANIFEST_VERSION)
        self.assertEqual(manifest["source_url_count"], 2)
        self.assertEqual(manifest["source_count"], 1)
        self.assertEqual(len(manifest["frames"]), 1)
        group = manifest["groups"][0]
        self.assertEqual(
            [attempt["outcome"] for attempt in group["attempts"]],
            ["request_failed", "selected"],
        )
        self.assertEqual(
            group["selected_url_sha256"], media._image_url_sha256(preview)
        )
        self.assertEqual(
            group["selected_response_sha256"],
            media.file_sha256(target / "image-000.bin"),
        )
        self.assertEqual(
            sorted(path.name for path in target.iterdir()),
            ["image-000.bin", "manifest.json"],
        )
        with connect(self.db) as connection:
            row = connection.execute(
                "SELECT processor_version,metadata_json FROM evidence_artifacts "
                "WHERE id=?",
                (artifact.id,),
            ).fetchone()
        self.assertEqual(row["processor_version"], media.IMAGE_DOWNLOAD_VERSION)
        self.assertEqual(
            json.loads(row["metadata_json"]),
            {
                "source_count": 1,
                "source_url_count": 2,
                "source_sha256": media._media_source_identity(
                    "image", [preview, detail]
                )[1],
                "image_groups_sha256": media.image_groups_sha256(
                    media.image_source_groups(
                        [preview, detail], platform="xiaohongshu"
                    )
                ),
                "download_binding_sha256": media.image_download_binding_sha256(
                    media._media_source_identity("image", [preview, detail])[1],
                    media.image_groups_sha256(
                        media.image_source_groups(
                            [preview, detail], platform="xiaohongshu"
                        )
                    ),
                ),
            },
        )
        selected_image = target / "image-000.bin"
        selected_bytes = selected_image.read_bytes()
        selected_image.write_bytes(
            b"RIFF" + b"\x00" * 4 + b"WEBP" + b"drift" * 200
        )
        with self.assertRaisesRegex(
            media.MediaProcessingError, "selected image file evidence drifted"
        ):
            media.process_image_evidence(
                1, manifest_path, db_path=self.db, media_root=output_root
            )
        with connect(self.db) as connection:
            self.assertEqual(
                connection.execute(
                    "SELECT COUNT(*) FROM media_processing_slots "
                    "WHERE content_id=1 AND processor_type='ocr'"
                ).fetchone()[0],
                0,
            )
        selected_image.write_bytes(selected_bytes)

        def mutate_platform_before_ocr_claim(
            content_id: int, processor_type: str
        ) -> None:
            if content_id == 1 and processor_type == "ocr":
                with connect(self.db) as connection:
                    connection.execute(
                        "UPDATE content_items SET platform='douyin' WHERE id=1"
                    )
                    connection.commit()

        with connect(self.db) as connection:
            before_ocr_artifacts = connection.execute(
                "SELECT COUNT(*) FROM evidence_artifacts WHERE content_id=1"
            ).fetchone()[0]
        with (
            patch.object(
                media,
                "_before_processing_slot_claim",
                side_effect=mutate_platform_before_ocr_claim,
            ),
            patch.object(media, "_run_ocr") as blocked_ocr,
        ):
            with self.assertRaisesRegex(
                media.MediaProcessingError, "frozen discovery groups"
            ):
                media.process_image_evidence(
                    1, manifest_path, db_path=self.db, media_root=output_root
                )
            blocked_ocr.assert_not_called()
        with connect(self.db) as connection:
            self.assertEqual(
                connection.execute(
                    "SELECT COUNT(*) FROM media_processing_slots "
                    "WHERE content_id=1 AND processor_type='ocr'"
                ).fetchone()[0],
                0,
            )
            self.assertEqual(
                connection.execute(
                    "SELECT COUNT(*) FROM evidence_artifacts WHERE content_id=1"
                ).fetchone()[0],
                before_ocr_artifacts,
            )
            connection.execute(
                "UPDATE content_items SET platform='xiaohongshu' WHERE id=1"
            )
            connection.commit()

        def fake_ocr(_manifest: Path, output: Path, **_kwargs) -> Path:
            media._atomic_json(
                output,
                {
                    "status": "success",
                    "processor_version": media.processor_versions()["ocr"],
                    "combined_text": "图文证据",
                    "source_count": 1,
                    "ocr_observation_count": 1,
                    "observations": [{"text": "图文证据"}],
                },
            )
            return output

        with patch.object(media, "_run_ocr", side_effect=fake_ocr) as ocr:
            first = media.process_image_evidence(
                1, manifest_path, db_path=self.db, media_root=output_root
            )
            second = media.process_image_evidence(
                1, manifest_path, db_path=self.db, media_root=output_root
            )
        self.assertEqual(first["media"].id, artifact.id)
        self.assertEqual(second["media"].id, artifact.id)
        self.assertEqual(first["ocr"].id, second["ocr"].id)
        ocr.assert_called_once()
        with connect(self.db) as connection:
            preserved = connection.execute(
                "SELECT processor_version,metadata_json FROM evidence_artifacts "
                "WHERE id=?",
                (artifact.id,),
            ).fetchone()
            manifest_count = connection.execute(
                "SELECT COUNT(*) FROM evidence_artifacts "
                "WHERE content_id=1 AND artifact_type='media_manifest'"
            ).fetchone()[0]
        self.assertEqual(preserved["processor_version"], media.IMAGE_DOWNLOAD_VERSION)
        self.assertEqual(preserved["metadata_json"], row["metadata_json"])
        self.assertEqual(manifest_count, 1)

        ocr_path = media._resolved(first["ocr"].local_path)
        valid_ocr_bytes = ocr_path.read_bytes()

        def coordinate_ocr_body(payload: dict) -> None:
            media._atomic_json(ocr_path, payload)
            with connect(self.db) as connection:
                connection.execute(
                    "UPDATE evidence_artifacts SET byte_size=?,sha256=? WHERE id=?",
                    (ocr_path.stat().st_size, media.file_sha256(ocr_path), first["ocr"].id),
                )
                connection.commit()

        with connect(self.db) as connection:
            before_counts = (
                connection.execute(
                    "SELECT COUNT(*) FROM evidence_artifacts"
                ).fetchone()[0],
                connection.execute(
                    "SELECT COUNT(*) FROM media_processing_slots"
                ).fetchone()[0],
            )
        valid_ocr_body = json.loads(valid_ocr_bytes)
        for forged_body in (
            {**valid_ocr_body, "combined_text": "协调伪造正文"},
            {},
        ):
            with self.subTest(forged_ocr_body=forged_body):
                coordinate_ocr_body(forged_body)
                with patch.object(media, "_run_ocr") as rerun:
                    with self.assertRaisesRegex(
                        media.MediaProcessingError, "OCR output body contract"
                    ):
                        media.process_image_evidence(
                            1,
                            manifest_path,
                            db_path=self.db,
                            media_root=output_root,
                        )
                    rerun.assert_not_called()
                with connect(self.db) as connection:
                    self.assertEqual(
                        (
                            connection.execute(
                                "SELECT COUNT(*) FROM evidence_artifacts"
                            ).fetchone()[0],
                            connection.execute(
                                "SELECT COUNT(*) FROM media_processing_slots"
                            ).fetchone()[0],
                        ),
                        before_counts,
                    )
                ocr_path.write_bytes(valid_ocr_bytes)
                with connect(self.db) as connection:
                    connection.execute(
                        "UPDATE evidence_artifacts SET byte_size=?,sha256=? WHERE id=?",
                        (
                            ocr_path.stat().st_size,
                            media.file_sha256(ocr_path),
                            first["ocr"].id,
                        ),
                    )
                    connection.commit()

    def test_media_source_kind_and_direct_claim_commit_context_are_fail_closed(
        self,
    ) -> None:
        source_root = self.root / "cross-kind-sources"
        with self.assertRaisesRegex(
            media.MediaProcessingError, "does not match content type"
        ):
            media.store_media_source_manifest(
                1,
                media_kind="image",
                urls=["https://cdn.example/wrong-kind.jpeg"],
                raw_response_id=801,
                db_path=self.db,
                media_root=source_root,
            )
        self.assertFalse(any(source_root.rglob("source-*.json")))

        video_url = "https://cdn.example/current-video.mp4"
        media.store_media_source_manifest(
            1,
            media_kind="video",
            urls=[video_url],
            raw_response_id=802,
            db_path=self.db,
            media_root=source_root,
        )

        def mutate_video_source(content_id: int, processor_type: str) -> None:
            if content_id == 1 and processor_type == "download":
                media.store_media_source_manifest(
                    1,
                    media_kind="video",
                    urls=["https://cdn.example/new-video.mp4"],
                    raw_response_id=805,
                    db_path=self.db,
                    media_root=source_root,
                )

        with (
            patch.object(
                media,
                "_before_processing_slot_claim",
                side_effect=mutate_video_source,
            ),
            patch.object(media, "_download_video") as download_video,
        ):
            with self.assertRaisesRegex(
                media.MediaProcessingError, "content/source identity changed"
            ):
                media.download_video_sources(
                    1,
                    [video_url],
                    db_path=self.db,
                    media_root=self.root / "claim-video-output",
                    reuse_existing=False,
                )
            download_video.assert_not_called()
        with connect(self.db) as connection:
            connection.execute(
                "UPDATE content_items SET platform='douyin',content_type='image' "
                "WHERE id=1"
            )
            connection.commit()
        self.assertIsNone(media.get_media_source_state(1, db_path=self.db))
        self.assertEqual(
            media._queue_content_ids(stage="download", limit=10, db_path=self.db),
            [],
        )
        with (
            patch.object(media, "download_video_sources") as download_video,
            patch.object(media.urllib.request, "urlopen") as urlopen,
        ):
            with self.assertRaisesRegex(
                media.MediaProcessingError, "does not match content type"
            ):
                media.process_content_media(1, db_path=self.db)
            download_video.assert_not_called()
            urlopen.assert_not_called()
        with connect(self.db) as connection:
            self.assertEqual(
                connection.execute(
                    "SELECT COUNT(*) FROM media_processing_slots"
                ).fetchone()[0],
                0,
            )

        image_url = "https://sns-i11.rednotecdn.com/claim-image.jpeg"
        with connect(self.db) as connection:
            connection.execute(
                "UPDATE evidence_artifacts SET status='missing' "
                "WHERE artifact_type='media_source'"
            )
            connection.execute(
                "UPDATE content_items SET platform='xiaohongshu',content_type='image' "
                "WHERE id=1"
            )
            connection.commit()
        media.store_media_source_manifest(
            1,
            media_kind="image",
            urls=[image_url],
            raw_response_id=803,
            db_path=self.db,
            media_root=source_root,
        )

        def mutate_image_platform(content_id: int, processor_type: str) -> None:
            if content_id == 1 and processor_type == "download":
                with connect(self.db) as connection:
                    connection.execute(
                        "UPDATE content_items SET platform='douyin' WHERE id=1"
                    )
                    connection.commit()

        with (
            patch.object(
                media,
                "_before_processing_slot_claim",
                side_effect=mutate_image_platform,
            ),
            patch.object(media.urllib.request, "urlopen") as urlopen,
        ):
            with self.assertRaisesRegex(
                media.MediaProcessingError, "content/source identity changed"
            ):
                media.download_image_sources(
                    1,
                    [image_url],
                    db_path=self.db,
                    media_root=self.root / "claim-image-output",
                    reuse_existing=False,
                )
            urlopen.assert_not_called()

        with connect(self.db) as connection:
            connection.execute(
                "UPDATE content_items SET platform='xiaohongshu' WHERE id=1"
            )
            connection.commit()

        def fake_image_download(_urls, target_dir: Path, **_kwargs) -> Path:
            target_dir.mkdir(parents=True, exist_ok=True)
            manifest = target_dir / "manifest.json"
            media._atomic_json(manifest, {"staged": True})
            return manifest

        def mutate_image_source(content_id: int, processor_type: str) -> None:
            if content_id == 1 and processor_type == "download":
                media.store_media_source_manifest(
                    1,
                    media_kind="image",
                    urls=["https://sns-i11.rednotecdn.com/new-current.jpeg"],
                    raw_response_id=804,
                    db_path=self.db,
                    media_root=source_root,
                )

        with (
            patch.object(media, "_download_images", side_effect=fake_image_download),
            patch.object(
                media,
                "_before_processing_slot_commit",
                side_effect=mutate_image_source,
            ),
        ):
            with self.assertRaisesRegex(
                media.MediaProcessingError, "content/source identity changed"
            ):
                media.download_image_sources(
                    1,
                    [image_url],
                    db_path=self.db,
                    media_root=self.root / "commit-image-output",
                    reuse_existing=False,
                )
        with connect(self.db) as connection:
            slots = connection.execute(
                "SELECT status FROM media_processing_slots"
            ).fetchall()
            self.assertTrue(slots)
            self.assertNotIn("succeeded", {row["status"] for row in slots})
            self.assertEqual(
                connection.execute(
                    "SELECT COUNT(*) FROM evidence_artifacts "
                    "WHERE artifact_type='media_manifest'"
                ).fetchone()[0],
                0,
            )

    def test_media_source_registration_revalidates_and_never_deletes_replacement(
        self,
    ) -> None:
        source_root = self.root / "source-registration-toctou"
        url = "https://cdn.example/staged-video.mp4"
        _values, source_sha256 = media._media_source_identity("video", [url])
        target = (
            source_root
            / "A2BC3D"
            / "sources"
            / f"source-901-{source_sha256[:12]}.json"
        )

        def mutate_content(_content_id: int) -> None:
            with connect(self.db) as connection:
                connection.execute(
                    "UPDATE content_items SET content_type='image' WHERE id=1"
                )
                connection.commit()

        with patch.object(
            media,
            "_before_media_source_manifest_commit",
            side_effect=mutate_content,
        ):
            with self.assertRaisesRegex(
                media.MediaProcessingError, "identity changed"
            ):
                media.store_media_source_manifest(
                    1,
                    media_kind="video",
                    urls=[url],
                    raw_response_id=901,
                    db_path=self.db,
                    media_root=source_root,
                )
        self.assertTrue(target.is_file())
        self.assertEqual(json.loads(target.read_text())["raw_response_id"], 901)
        with connect(self.db) as connection:
            self.assertEqual(
                connection.execute(
                    "SELECT COUNT(*) FROM evidence_artifacts "
                    "WHERE artifact_type='media_source'"
                ).fetchone()[0],
                0,
            )
            connection.execute(
                "UPDATE content_items SET content_type='video' WHERE id=1"
            )
            connection.commit()

        replacement_url = "https://cdn.example/replaced-video.mp4"
        _values, replacement_sha256 = media._media_source_identity(
            "video", [replacement_url]
        )
        replacement_target = (
            source_root
            / "A2BC3D"
            / "sources"
            / f"source-902-{replacement_sha256[:12]}.json"
        )
        sentinel = b"external-replacement-sentinel"

        def replace_then_mutate(_content_id: int) -> None:
            replacement_target.write_bytes(sentinel)
            mutate_content(1)

        with patch.object(
            media,
            "_before_media_source_manifest_commit",
            side_effect=replace_then_mutate,
        ):
            with self.assertRaisesRegex(
                media.MediaProcessingError, "changed after validation"
            ):
                media.store_media_source_manifest(
                    1,
                    media_kind="video",
                    urls=[replacement_url],
                    raw_response_id=902,
                    db_path=self.db,
                    media_root=source_root,
                )
        self.assertEqual(replacement_target.read_bytes(), sentinel)
        with connect(self.db) as connection:
            self.assertEqual(
                connection.execute(
                    "SELECT COUNT(*) FROM evidence_artifacts "
                    "WHERE artifact_type='media_source'"
                ).fetchone()[0],
                0,
            )
            connection.execute(
                "UPDATE content_items SET content_type='video' WHERE id=1"
            )
            connection.commit()

        final_window_url = "https://cdn.example/final-window-video.mp4"
        _values, final_window_sha256 = media._media_source_identity(
            "video", [final_window_url]
        )
        final_window_target = (
            source_root
            / "A2BC3D"
            / "sources"
            / f"source-903-{final_window_sha256[:12]}.json"
        )
        final_window_sentinel = b"assert-to-quarantine-window-sentinel"

        def replace_after_cleanup_assert(target: Path) -> None:
            replacement = target.with_name(f".{target.name}.replacement")
            replacement.write_bytes(final_window_sentinel)
            os.replace(replacement, target)

        with (
            patch.object(
                media,
                "_before_media_source_manifest_commit",
                side_effect=mutate_content,
            ),
            patch.object(
                media,
                "_before_staged_media_source_quarantine",
                side_effect=replace_after_cleanup_assert,
            ),
        ):
            with self.assertRaisesRegex(
                media.MediaProcessingError, "changed after validation"
            ):
                media.store_media_source_manifest(
                    1,
                    media_kind="video",
                    urls=[final_window_url],
                    raw_response_id=903,
                    db_path=self.db,
                    media_root=source_root,
                )
        self.assertEqual(final_window_target.read_bytes(), final_window_sentinel)
        with connect(self.db) as connection:
            self.assertEqual(
                connection.execute(
                    "SELECT COUNT(*) FROM evidence_artifacts "
                    "WHERE artifact_type='media_source'"
                ).fetchone()[0],
                0,
            )
            connection.execute(
                "UPDATE content_items SET content_type='video' WHERE id=1"
            )
            connection.commit()

        replacement_only_url = "https://cdn.example/replacement-only-video.mp4"
        _values, replacement_only_sha256 = media._media_source_identity(
            "video", [replacement_only_url]
        )
        replacement_only_target = (
            source_root
            / "A2BC3D"
            / "sources"
            / f"source-904-{replacement_only_sha256[:12]}.json"
        )
        replacement_only_sentinel = b"replacement-only-sentinel"

        def replace_without_content_drift(_content_id: int) -> None:
            replacement = replacement_only_target.with_name(
                f".{replacement_only_target.name}.external"
            )
            replacement.write_bytes(replacement_only_sentinel)
            os.replace(replacement, replacement_only_target)

        with patch.object(
            media,
            "_before_media_source_manifest_commit",
            side_effect=replace_without_content_drift,
        ):
            with self.assertRaisesRegex(
                media.MediaProcessingError, "changed after validation"
            ):
                media.store_media_source_manifest(
                    1,
                    media_kind="video",
                    urls=[replacement_only_url],
                    raw_response_id=904,
                    db_path=self.db,
                    media_root=source_root,
                )
        self.assertEqual(
            replacement_only_target.read_bytes(), replacement_only_sentinel
        )
        with connect(self.db) as connection:
            self.assertEqual(
                connection.execute(
                    "SELECT COUNT(*) FROM evidence_artifacts "
                    "WHERE artifact_type='media_source'"
                ).fetchone()[0],
                0,
            )

        publish_collision_url = (
            "https://cdn.example/source-publish-collision.mp4"
        )
        _values, publish_collision_sha256 = media._media_source_identity(
            "video", [publish_collision_url]
        )
        publish_collision_target = (
            source_root
            / "A2BC3D"
            / "sources"
            / f"source-909-{publish_collision_sha256[:12]}.json"
        )
        publish_collision_staging = publish_collision_target.with_name(
            f".{publish_collision_target.name}.tmp"
        )
        publish_collision_sentinel = b"concurrent-final-must-not-be-clobbered"

        def occupy_source_final(target: Path) -> None:
            self.assertEqual(target, publish_collision_target)
            target.write_bytes(publish_collision_sentinel)

        with patch.object(
            media,
            "_before_media_source_final_create",
            side_effect=occupy_source_final,
        ):
            with self.assertRaisesRegex(
                media.MediaProcessingError, "target already exists"
            ):
                media.store_media_source_manifest(
                    1,
                    media_kind="video",
                    urls=[publish_collision_url],
                    raw_response_id=909,
                    db_path=self.db,
                    media_root=source_root,
                )
        self.assertEqual(
            publish_collision_target.read_bytes(), publish_collision_sentinel
        )
        self.assertTrue(publish_collision_staging.is_file())
        with connect(self.db) as connection:
            self.assertEqual(
                connection.execute(
                    "SELECT COUNT(*) FROM evidence_artifacts "
                    "WHERE artifact_type='media_source'"
                ).fetchone()[0],
                0,
            )

        parent_alias_url = "https://cdn.example/cleanup-parent-alias.mp4"
        _values, parent_alias_sha256 = media._media_source_identity(
            "video", [parent_alias_url]
        )
        parent_alias_target = (
            source_root
            / "A2BC3D"
            / "sources"
            / f"source-908-{parent_alias_sha256[:12]}.json"
        )
        external_parent = self.root / "external-cleanup-parent"
        external_parent.mkdir()
        external_sentinel = external_parent / "sentinel.bin"
        external_bytes = b"cleanup-parent-external-tree-must-not-change"
        external_sentinel.write_bytes(external_bytes)

        def replace_cleanup_parent(_target: Path) -> None:
            sources = parent_alias_target.parent
            owned_sources = sources.with_name("sources-owned-recovery")
            os.replace(sources, owned_sources)
            sources.symlink_to(external_parent, target_is_directory=True)

        with (
            patch.object(
                media,
                "_before_media_source_manifest_commit",
                side_effect=mutate_content,
            ),
            patch.object(
                media,
                "_before_staged_media_source_quarantine",
                side_effect=replace_cleanup_parent,
            ),
        ):
            with self.assertRaises(media.MediaProcessingError):
                media.store_media_source_manifest(
                    1,
                    media_kind="video",
                    urls=[parent_alias_url],
                    raw_response_id=908,
                    db_path=self.db,
                    media_root=source_root,
                )
        self.assertEqual(external_sentinel.read_bytes(), external_bytes)
        self.assertEqual(
            sorted(path.name for path in external_parent.iterdir()),
            ["sentinel.bin"],
        )
        self.assertTrue(
            (
                source_root
                / "A2BC3D"
                / "sources-owned-recovery"
                / parent_alias_target.name
            ).is_file()
        )
        with connect(self.db) as connection:
            self.assertEqual(
                connection.execute(
                    "SELECT COUNT(*) FROM evidence_artifacts "
                    "WHERE artifact_type='media_source'"
                ).fetchone()[0],
                0,
            )

    def test_media_source_store_rejects_non_exact_inputs_before_io(self) -> None:
        invalid_calls = [
            {"content_id": False},
            {"content_id": 0},
            {"content_id": -1},
            {"content_id": 1.0},
            {"content_id": "1"},
            {"media_kind": 7},
            {"media_kind": "audio"},
            {"raw_response_id": True},
            {"raw_response_id": 0},
            {"raw_response_id": -1},
            {"raw_response_id": 1.0},
            {"raw_response_id": "1"},
            {"urls": ("https://cdn.example/exact.mp4",)},
            {"urls": "https://cdn.example/exact.mp4"},
            {"urls": [Path("https://cdn.example/exact.mp4")]},
        ]
        defaults = {
            "content_id": 1,
            "media_kind": "video",
            "urls": ["https://cdn.example/exact.mp4"],
            "raw_response_id": 905,
        }
        source_root = self.root / "invalid-source-inputs"
        for override in invalid_calls:
            arguments = {**defaults, **override}
            with self.subTest(override=override), patch.object(
                media, "connect"
            ) as database_connect:
                with self.assertRaises(media.MediaProcessingError):
                    media.store_media_source_manifest(
                        **arguments,
                        db_path=self.db,
                        media_root=source_root,
                    )
                database_connect.assert_not_called()
        self.assertFalse(source_root.exists())

    def test_media_source_store_never_follows_legacy_fixed_temp_symlink(
        self,
    ) -> None:
        source_root = self.root / "source-fixed-temp-symlink"
        url = "https://cdn.example/fixed-temp-video.mp4"
        _values, source_sha256 = media._media_source_identity("video", [url])
        target = (
            source_root
            / "A2BC3D"
            / "sources"
            / f"source-906-{source_sha256[:12]}.json"
        )
        target.parent.mkdir(parents=True)
        victim = self.root / "external-fixed-temp-victim"
        sentinel = b"external-fixed-temp-victim-sentinel"
        victim.write_bytes(sentinel)
        legacy_temporary = target.with_name(f".{target.name}.tmp")
        legacy_temporary.symlink_to(victim)
        with self.assertRaisesRegex(
            media.MediaProcessingError, "staging cannot be opened safely"
        ):
            media.store_media_source_manifest(
                1,
                media_kind="video",
                urls=[url],
                raw_response_id=906,
                db_path=self.db,
                media_root=source_root,
            )
        self.assertEqual(victim.read_bytes(), sentinel)
        self.assertTrue(legacy_temporary.is_symlink())
        self.assertFalse(target.exists())
        with connect(self.db) as connection:
            self.assertEqual(
                connection.execute(
                    "SELECT COUNT(*) FROM evidence_artifacts "
                    "WHERE artifact_type='media_source'"
                ).fetchone()[0],
                0,
            )

    def test_media_source_store_creates_parent_chain_without_following_alias(
        self,
    ) -> None:
        source_root = self.root / "source-parent-alias"
        external = self.root / "external-parent-alias-victim"
        external.mkdir()
        sentinel = external / "sentinel.bin"
        sentinel_bytes = b"external-parent-tree-must-remain-unchanged"
        sentinel.write_bytes(sentinel_bytes)

        def install_parent_alias(_target: Path) -> None:
            source_root.mkdir()
            (source_root / "A2BC3D").symlink_to(
                external, target_is_directory=True
            )

        with patch.object(
            media,
            "_before_media_source_directory_chain",
            side_effect=install_parent_alias,
        ):
            with self.assertRaisesRegex(
                media.MediaProcessingError,
                "symlink component|directory chain contains an alias",
            ):
                media.store_media_source_manifest(
                    1,
                    media_kind="video",
                    urls=["https://cdn.example/parent-alias.mp4"],
                    raw_response_id=907,
                    db_path=self.db,
                    media_root=source_root,
                )
        self.assertEqual(sentinel.read_bytes(), sentinel_bytes)
        self.assertEqual(sorted(path.name for path in external.iterdir()), ["sentinel.bin"])
        self.assertFalse((external / "sources").exists())
        with connect(self.db) as connection:
            self.assertEqual(
                connection.execute(
                    "SELECT COUNT(*) FROM evidence_artifacts "
                    "WHERE artifact_type='media_source'"
                ).fetchone()[0],
                0,
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

    def test_process_image_evidence_rejects_unregistered_or_drifted_manifest(self) -> None:
        url = "https://sns-i11.rednotecdn.com/current-image.jpg"
        image_bytes = b"\xff\xd8\xff" + b"image" * 200

        class Response:
            headers = {"Content-Length": str(len(image_bytes))}

            def __init__(self) -> None:
                self.body = io.BytesIO(image_bytes)

            def read(self, size: int = -1) -> bytes:
                return self.body.read(size)

            def geturl(self) -> str:
                return url

            def __enter__(self):
                return self

            def __exit__(self, *_args):
                return None

        output_root = self.root / "strict-image-evidence"
        self._store_current_image_source([url])
        artifact = media.download_image_sources(
            1,
            [url],
            db_path=self.db,
            media_root=output_root,
            urlopen_fn=lambda *_args, **_kwargs: Response(),
            reuse_existing=False,
        )
        manifest = media._resolved(artifact.local_path)
        manifest_bytes = manifest.read_bytes()
        with connect(self.db) as connection:
            row = connection.execute(
                "SELECT * FROM evidence_artifacts WHERE id=?", (artifact.id,)
            ).fetchone()
        original_metadata = row["metadata_json"]

        with connect(self.db) as connection:
            connection.execute(
                "UPDATE evidence_artifacts SET metadata_json=?, processor_version=? "
                "WHERE id=?",
                (
                    original_metadata,
                    media.LEGACY_IMAGE_DOWNLOAD_VERSION,
                    artifact.id,
                ),
            )
            connection.commit()
        with self.assertRaisesRegex(media.MediaProcessingError, "metadata"):
            media.process_image_evidence(
                1, manifest, db_path=self.db, media_root=output_root
            )

        with connect(self.db) as connection:
            connection.execute(
                "UPDATE evidence_artifacts SET processor_version=? WHERE id=?",
                (media.IMAGE_DOWNLOAD_VERSION, artifact.id),
            )
            connection.commit()
        manifest.write_bytes(manifest_bytes + b" ")
        with self.assertRaisesRegex(media.MediaProcessingError, "metadata"):
            media.process_image_evidence(
                1, manifest, db_path=self.db, media_root=output_root
            )
        manifest.write_bytes(manifest_bytes)

        unregistered = self.root / "unregistered-manifest.json"
        unregistered.write_bytes(manifest_bytes)
        with self.assertRaisesRegex(
            media.MediaProcessingError, "outside media root"
        ):
            media.process_image_evidence(
                1, unregistered, db_path=self.db, media_root=output_root
            )
        with connect(self.db) as connection:
            self.assertEqual(
                connection.execute(
                    "SELECT COUNT(*) FROM media_processing_slots "
                    "WHERE content_id=1 AND processor_type='ocr'"
                ).fetchone()[0],
                0,
            )

    def test_process_image_evidence_rejects_erased_current_metadata_before_ocr(self) -> None:
        url = "https://sns-i11.rednotecdn.com/current-erased-image.jpg"
        image_bytes = b"\xff\xd8\xff" + b"legacy-erasure" * 100

        class Response:
            headers = {"Content-Length": str(len(image_bytes))}

            def __init__(self) -> None:
                self.body = io.BytesIO(image_bytes)

            def read(self, size: int = -1) -> bytes:
                return self.body.read(size)

            def geturl(self) -> str:
                return url

            def __enter__(self):
                return self

            def __exit__(self, *_args):
                return None

        output_root = self.root / "erased-image-evidence"
        source_root = self.root / "erased-image-sources"
        with connect(self.db) as connection:
            connection.execute(
                "UPDATE content_items SET content_type='image' WHERE id=1"
            )
            connection.commit()
        source = media.store_media_source_manifest(
            1,
            media_kind="image",
            urls=[url],
            raw_response_id=701,
            db_path=self.db,
            media_root=source_root,
        )
        self.assertIsNotNone(source)
        artifact = media.download_image_sources(
            1,
            [url],
            db_path=self.db,
            media_root=output_root,
            urlopen_fn=lambda *_args, **_kwargs: Response(),
            reuse_existing=False,
        )
        manifest = media._resolved(artifact.local_path)
        with connect(self.db) as connection:
            connection.execute(
                "UPDATE evidence_artifacts SET metadata_json='{}' WHERE id=?",
                (artifact.id,),
            )
            connection.commit()
            preserved_before = dict(
                connection.execute(
                    "SELECT * FROM evidence_artifacts WHERE id=?", (artifact.id,)
                ).fetchone()
            )

        def fake_ocr(_manifest: Path, output: Path, **_kwargs) -> Path:
            media._atomic_json(
                output,
                {
                    "status": "success",
                    "combined_text": "兼容旧metadata",
                    "source_count": 1,
                },
            )
            return output

        with patch.object(media, "_run_ocr", side_effect=fake_ocr) as ocr:
            with self.assertRaisesRegex(
                media.MediaProcessingError, "artifact metadata"
            ):
                media.process_image_evidence(
                    1, manifest, db_path=self.db, media_root=output_root
                )
        ocr.assert_not_called()
        with connect(self.db) as connection:
            preserved_after = dict(
                connection.execute(
                    "SELECT * FROM evidence_artifacts WHERE id=?", (artifact.id,)
                ).fetchone()
            )
            manifest_count = connection.execute(
                "SELECT COUNT(*) FROM evidence_artifacts "
                "WHERE content_id=1 AND artifact_type='media_manifest'"
            ).fetchone()[0]
        self.assertEqual(preserved_after, preserved_before)
        self.assertEqual(preserved_after["metadata_json"], "{}")
        self.assertEqual(manifest_count, 1)
        with connect(self.db) as connection:
            self.assertEqual(
                connection.execute(
                    "SELECT COUNT(*) FROM media_processing_slots "
                    "WHERE content_id=1 AND processor_type='ocr'"
                ).fetchone()[0],
                0,
            )

    def test_image_ocr_precommit_rejects_latest_source_inserted_during_ocr(self) -> None:
        original_url = "https://sns-i11.rednotecdn.com/toctou-original.jpg"
        replacement_url = "https://sns-i11.rednotecdn.com/toctou-replacement.jpg"
        image_bytes = b"\xff\xd8\xff" + b"toctou-image" * 100

        class Response:
            headers = {"Content-Length": str(len(image_bytes))}

            def __init__(self) -> None:
                self.body = io.BytesIO(image_bytes)

            def read(self, size: int = -1) -> bytes:
                return self.body.read(size)

            def geturl(self) -> str:
                return original_url

            def __enter__(self):
                return self

            def __exit__(self, *_args):
                return None

        output_root = self.root / "toctou-image-output"
        source_root = self.root / "toctou-image-source"
        with connect(self.db) as connection:
            connection.execute(
                "UPDATE content_items SET content_type='image' WHERE id=1"
            )
            connection.commit()
        media.store_media_source_manifest(
            1,
            media_kind="image",
            urls=[original_url],
            raw_response_id=801,
            db_path=self.db,
            media_root=source_root,
        )
        artifact = media.download_image_sources(
            1,
            [original_url],
            db_path=self.db,
            media_root=output_root,
            urlopen_fn=lambda *_args, **_kwargs: Response(),
            reuse_existing=False,
        )

        def replace_source_during_ocr(
            _manifest: Path, target: Path, **_kwargs
        ) -> Path:
            media.store_media_source_manifest(
                1,
                media_kind="image",
                urls=[replacement_url],
                raw_response_id=802,
                db_path=self.db,
                media_root=source_root,
            )
            media._atomic_json(
                target,
                {"status": "success", "combined_text": "stale", "source_count": 1},
            )
            return target

        with patch.object(
            media, "_run_ocr", side_effect=replace_source_during_ocr
        ):
            with self.assertRaisesRegex(media.MediaProcessingError, "current source"):
                media.process_image_evidence(
                    1,
                    media._resolved(artifact.local_path),
                    db_path=self.db,
                    media_root=output_root,
                )
        with connect(self.db) as connection:
            self.assertEqual(
                connection.execute(
                    "SELECT COUNT(*) FROM evidence_artifacts "
                    "WHERE content_id=1 AND artifact_type='ocr' AND status='available'"
                ).fetchone()[0],
                0,
            )
            self.assertEqual(
                connection.execute(
                    "SELECT COUNT(*) FROM media_processing_slots "
                    "WHERE content_id=1 AND processor_type='ocr' AND status='succeeded'"
                ).fetchone()[0],
                0,
            )

    def test_processing_slot_success_commit_rejects_claim_identity_tamper(self) -> None:
        output = self.root / "tampered-slot-output.json"
        source_sha256 = "a" * 64

        def tamper_claimed_slot() -> Path:
            media._atomic_json(output, {"status": "success"})
            with connect(self.db) as connection:
                connection.execute(
                    "UPDATE media_processing_slots SET source_sha256=?,"
                    "processor_version='tampered' "
                    "WHERE content_id=1 AND processor_type='ocr' AND status='running'",
                    ("f" * 64,),
                )
                connection.commit()
            return output

        with self.assertRaisesRegex(media.MediaProcessingError, "identity changed"):
            media._run_processing_slot(
                db_path=self.db,
                content_id=1,
                source_sha256=source_sha256,
                processor_type="ocr",
                processor_version="cas-test-v1",
                artifact_type="ocr",
                produce=tamper_claimed_slot,
            )
        with connect(self.db) as connection:
            self.assertEqual(
                connection.execute(
                    "SELECT COUNT(*) FROM evidence_artifacts "
                    "WHERE content_id=1 AND artifact_type='ocr'"
                ).fetchone()[0],
                0,
            )
            self.assertEqual(
                connection.execute(
                    "SELECT COUNT(*) FROM media_processing_slots "
                    "WHERE content_id=1 AND processor_type='ocr' AND status='succeeded'"
                ).fetchone()[0],
                0,
            )

    def test_erased_v82_metadata_requires_exact_slot_source_and_groups(self) -> None:
        url = "https://sns-i11.rednotecdn.com/current-erased-negative.jpg"
        image_bytes = b"\xff\xd8\xff" + b"negative-erasure" * 100

        class Response:
            headers = {"Content-Length": str(len(image_bytes))}

            def __init__(self) -> None:
                self.body = io.BytesIO(image_bytes)

            def read(self, size: int = -1) -> bytes:
                return self.body.read(size)

            def geturl(self) -> str:
                return url

            def __enter__(self):
                return self

            def __exit__(self, *_args):
                return None

        output_root = self.root / "erased-image-negative"
        with connect(self.db) as connection:
            connection.execute(
                "UPDATE content_items SET content_type='image' WHERE id=1"
            )
            connection.commit()
        source = media.store_media_source_manifest(
            1,
            media_kind="image",
            urls=[url],
            raw_response_id=702,
            db_path=self.db,
            media_root=self.root / "erased-image-negative-sources",
        )
        self.assertIsNotNone(source)
        artifact = media.download_image_sources(
            1,
            [url],
            db_path=self.db,
            media_root=output_root,
            urlopen_fn=lambda *_args, **_kwargs: Response(),
            reuse_existing=False,
        )
        manifest = media._resolved(artifact.local_path)
        original_manifest = manifest.read_bytes()
        with connect(self.db) as connection:
            slot = connection.execute(
                "SELECT id,source_sha256 FROM media_processing_slots "
                "WHERE content_id=1 AND processor_type='download' "
                "AND processor_version=?",
                (media.IMAGE_DOWNLOAD_VERSION,),
            ).fetchone()
            connection.execute(
                "UPDATE media_processing_slots SET source_sha256=? WHERE id=?",
                ("f" * 64, slot["id"]),
            )
            connection.commit()
        with self.assertRaisesRegex(media.MediaProcessingError, "slot|path|binding"):
            media.process_image_evidence(
                1, manifest, db_path=self.db, media_root=output_root
            )

        with connect(self.db) as connection:
            connection.execute(
                "UPDATE media_processing_slots SET source_sha256=? WHERE id=?",
                (slot["source_sha256"], slot["id"]),
            )
            connection.execute(
                "UPDATE evidence_artifacts SET processor_version=? WHERE id=?",
                (media.LEGACY_IMAGE_DOWNLOAD_VERSION, source.id),
            )
            connection.commit()
        with self.assertRaisesRegex(media.MediaProcessingError, "current source"):
            media.process_image_evidence(
                1, manifest, db_path=self.db, media_root=output_root
            )

        drifted_body = json.loads(original_manifest)
        drifted_body["groups"][0]["identity"] = {
            "kind": "exact-url-v1",
            "url_sha256": "0" * 64,
        }
        media._atomic_json(manifest, drifted_body)
        with connect(self.db) as connection:
            connection.execute(
                "UPDATE evidence_artifacts SET processor_version=? WHERE id=?",
                (media.MEDIA_SOURCE_VERSION, source.id),
            )
            connection.execute(
                "UPDATE evidence_artifacts SET byte_size=?,sha256=? WHERE id=?",
                (manifest.stat().st_size, media.file_sha256(manifest), artifact.id),
            )
            connection.commit()
        with self.assertRaisesRegex(media.MediaProcessingError, "identity"):
            media.process_image_evidence(
                1, manifest, db_path=self.db, media_root=output_root
            )
        manifest.write_bytes(original_manifest)
        with connect(self.db) as connection:
            connection.execute(
                "UPDATE evidence_artifacts SET byte_size=?,sha256=? WHERE id=?",
                (len(original_manifest), media.file_sha256(manifest), artifact.id),
            )
            connection.commit()
            self.assertEqual(
                connection.execute(
                    "SELECT COUNT(*) FROM media_processing_slots "
                    "WHERE content_id=1 AND processor_type='ocr'"
                ).fetchone()[0],
                0,
            )

    def test_image_manifest_rejects_historical_order_for_latest_source(self) -> None:
        urls = [
            "https://sns-i11.rednotecdn.com/order-first.jpg",
            "https://sns-i11.rednotecdn.com/order-second.jpg",
        ]
        source_root = self.root / "ordered-image-sources"
        output_root = self.root / "ordered-image-output"
        with connect(self.db) as connection:
            connection.execute(
                "UPDATE content_items SET content_type='image' WHERE id=1"
            )
            connection.commit()
        media.store_media_source_manifest(
            1,
            media_kind="image",
            urls=urls,
            raw_response_id=710,
            db_path=self.db,
            media_root=source_root,
        )

        class Response:
            def __init__(self, url: str) -> None:
                body = b"\xff\xd8\xff" + url.encode("utf-8") * 40
                self.headers = {"Content-Length": str(len(body))}
                self.body = io.BytesIO(body)
                self.url = url

            def read(self, size: int = -1) -> bytes:
                return self.body.read(size)

            def geturl(self) -> str:
                return self.url

            def __enter__(self):
                return self

            def __exit__(self, *_args):
                return None

        artifact = media.download_image_sources(
            1,
            urls,
            db_path=self.db,
            media_root=output_root,
            urlopen_fn=lambda request, **_kwargs: Response(request.full_url),
            reuse_existing=False,
        )
        media.store_media_source_manifest(
            1,
            media_kind="image",
            urls=list(reversed(urls)),
            raw_response_id=711,
            db_path=self.db,
            media_root=source_root,
        )
        regrouped = media.download_image_sources(
            1,
            list(reversed(urls)),
            db_path=self.db,
            media_root=output_root,
            urlopen_fn=lambda request, **_kwargs: Response(request.full_url),
            reuse_existing=False,
        )
        self.assertNotEqual(regrouped.id, artifact.id)
        self.assertNotEqual(regrouped.local_path, artifact.local_path)

        def fake_ocr(_manifest: Path, output: Path, **_kwargs) -> Path:
            media._atomic_json(
                output,
                {
                    "status": "success",
                    "combined_text": "顺序兼容",
                    "source_count": 2,
                },
            )
            return output

        with patch.object(media, "_run_ocr", side_effect=fake_ocr) as ocr:
            with self.assertRaisesRegex(media.MediaProcessingError, "current source"):
                media.process_image_evidence(
                    1,
                    media._resolved(artifact.local_path),
                    db_path=self.db,
                    media_root=output_root,
                )
            processed = media.process_image_evidence(
                1,
                media._resolved(regrouped.local_path),
                db_path=self.db,
                media_root=output_root,
            )
        self.assertEqual(processed["media"].id, regrouped.id)
        ocr.assert_called_once()
        media.store_media_source_manifest(
            1,
            media_kind="image",
            urls=[urls[0], "https://sns-i11.rednotecdn.com/different.jpg"],
            raw_response_id=712,
            db_path=self.db,
            media_root=source_root,
        )
        with self.assertRaisesRegex(media.MediaProcessingError, "current source"):
            media.process_image_evidence(
                1,
                media._resolved(regrouped.local_path),
                db_path=self.db,
                media_root=output_root,
            )
        with connect(self.db) as connection:
            self.assertEqual(
                connection.execute(
                    "SELECT COUNT(*) FROM evidence_artifacts "
                    "WHERE content_id=1 AND artifact_type='media_manifest'"
                ).fetchone()[0],
                2,
            )

    def test_v80_image_success_is_not_reused_for_grouped_manifest(self) -> None:
        urls, _source_sha = media._media_source_identity(
            "image", ["https://sns-i11.rednotecdn.com/legacy-image.jpg"]
        )
        legacy_sha = media._legacy_media_source_sha256(urls)
        legacy_manifest = self.root / "legacy-image-manifest.json"
        media._atomic_json(
            legacy_manifest,
            {"status": "complete", "image_paths": [], "frames": [], "errors": []},
        )
        replacement_manifest = self.root / "grouped-image-manifest.json"
        media._atomic_json(
            replacement_manifest,
            {
                "schema_version": media.IMAGE_MANIFEST_VERSION,
                "status": "complete",
                "source_url_count": 1,
                "source_count": 1,
                "image_paths": [],
                "frames": [],
                "groups": [],
            },
        )
        captured_at = now_utc()
        with connect(self.db) as connection:
            connection.execute(
                "UPDATE content_items SET platform='xiaohongshu', "
                "content_type='image' WHERE id=1"
            )
            legacy_artifact = media.register_artifact(
                connection,
                content_id=1,
                artifact_type="media_manifest",
                path=legacy_manifest,
                processor_version=media.LEGACY_IMAGE_DOWNLOAD_VERSION,
            )
            connection.execute(
                """
                INSERT INTO media_processing_slots(
                    content_id, source_sha256, processor_type, processor_version,
                    status, output_artifact_id, attempt_count, created_at, updated_at
                ) VALUES (1, ?, 'download', ?, 'succeeded', ?, 1, ?, ?)
                """,
                (
                    legacy_sha,
                    media.LEGACY_IMAGE_DOWNLOAD_VERSION,
                    legacy_artifact.id,
                    captured_at,
                    captured_at,
                ),
            )
            connection.commit()

        with patch.object(
            media, "_download_images", return_value=replacement_manifest
        ) as download:
            self._store_current_image_source(urls, platform="xiaohongshu")
            artifact = media.download_image_sources(
                1, urls, db_path=self.db, media_root=self.root / "new-image-root"
            )

        download.assert_called_once()
        self.assertNotEqual(artifact.id, legacy_artifact.id)
        self.assertEqual(artifact.processor_version, media.IMAGE_DOWNLOAD_VERSION)

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

    def test_generic_douyin_state_and_queues_fail_closed_but_explicit_groups_reuse(
        self,
    ) -> None:
        urls = [
            f"https://p3-sign.douyinpic.com/queue-grouped-{index}.jpeg"
            for index in range(4)
        ]
        groups = media.douyin_image_source_groups(
            urls, [urls[:2], urls[2:]]
        )
        image_bytes = b"\xff\xd8\xff" + b"grouped-queue" * 100

        class Response:
            def __init__(self, url: str) -> None:
                self.url = url
                self.body = io.BytesIO(image_bytes)
                self.headers = {"Content-Length": str(len(image_bytes))}

            def read(self, size: int = -1) -> bytes:
                return self.body.read(size)

            def geturl(self) -> str:
                return self.url

            def __enter__(self):
                return self

            def __exit__(self, *_args):
                return None

        def fake_ocr(_manifest: Path, target: Path, **_kwargs) -> Path:
            media._atomic_json(
                target,
                {
                    "status": "success",
                    "combined_text": "抖音分组复用",
                    "source_count": 2,
                },
            )
            return target

        media_root = self.root / "grouped-queue-media"
        source_root = self.root / "grouped-queue-source"
        with connect(self.db) as connection:
            connection.execute(
                "UPDATE content_items SET platform='douyin',content_type='image' "
                "WHERE id=1"
            )
            connection.commit()
        media.store_media_source_manifest(
            1,
            media_kind="image",
            urls=urls,
            raw_response_id=920,
            db_path=self.db,
            media_root=source_root,
        )
        artifact = media.download_image_sources(
            1,
            urls,
            db_path=self.db,
            media_root=media_root,
            frozen_image_groups=groups,
            urlopen_fn=lambda request, **_kwargs: Response(request.full_url),
            maximum_bytes=10_000,
            reuse_existing=False,
        )
        with connect(self.db) as connection:
            download_counts_before = (
                connection.execute(
                    "SELECT COUNT(*) FROM media_processing_slots "
                    "WHERE content_id=1 AND processor_type='download'"
                ).fetchone()[0],
                connection.execute(
                    "SELECT COUNT(*) FROM evidence_artifacts "
                    "WHERE content_id=1 AND artifact_type='media_manifest'"
                ).fetchone()[0],
            )

        with (
            patch.object(media, "MEDIA_ROOT", media_root),
            patch.object(
                media.urllib.request,
                "urlopen",
                side_effect=AssertionError("grouped queue must not redownload"),
            ) as urlopen,
            patch.object(media, "_run_ocr", side_effect=fake_ocr) as ocr,
            patch.object(
                media,
                "compile_ocr_binary",
                return_value=self.root / "vision_ocr",
            ),
        ):
            state = media.get_media_source_state(1, db_path=self.db)
            download_queue = media.run_media_download_queue(
                limit=1, max_workers=1, db_path=self.db
            )
            process_queue = media.run_media_processing_queue(
                limit=1, db_path=self.db
            )
            repeated_process = media.run_media_processing_queue(
                limit=1, db_path=self.db
            )
            with self.assertRaisesRegex(
                media.MediaProcessingError, "frozen discovery groups"
            ):
                media.process_content_media(1, db_path=self.db)
            explicit = media.process_content_media(
                1,
                db_path=self.db,
                media_root=media_root,
                frozen_image_groups=groups,
            )

        self.assertIsNotNone(state)
        assert state is not None
        self.assertIsNone(state["download_slot"])
        self.assertEqual(download_queue["candidates"], 0)
        self.assertEqual(process_queue["candidates"], 0)
        self.assertEqual(repeated_process["candidates"], 0)
        self.assertEqual(explicit["status"], "evidence_ready")
        urlopen.assert_not_called()
        ocr.assert_called_once()
        with connect(self.db) as connection:
            self.assertEqual(
                (
                    connection.execute(
                        "SELECT COUNT(*) FROM media_processing_slots "
                        "WHERE content_id=1 AND processor_type='download'"
                    ).fetchone()[0],
                    connection.execute(
                        "SELECT COUNT(*) FROM evidence_artifacts "
                        "WHERE content_id=1 AND artifact_type='media_manifest'"
                    ).fetchone()[0],
                ),
                download_counts_before,
            )

        forged_groups = media.douyin_image_source_groups(urls, [urls])
        forged_groups_sha256 = media.image_groups_sha256(forged_groups)
        flat_sha256 = media._media_source_identity("image", urls)[1]
        forged_binding_sha256 = media.image_download_binding_sha256(
            flat_sha256, forged_groups_sha256
        )
        with connect(self.db) as connection:
            metadata = json.loads(
                connection.execute(
                    "SELECT metadata_json FROM evidence_artifacts WHERE id=?",
                    (artifact.id,),
                ).fetchone()[0]
            )
            metadata["source_count"] = 1
            metadata["image_groups_sha256"] = forged_groups_sha256
            metadata["download_binding_sha256"] = forged_binding_sha256
            connection.execute(
                "UPDATE evidence_artifacts SET metadata_json=? WHERE id=?",
                (json.dumps(metadata, sort_keys=True), artifact.id),
            )
            connection.execute(
                "UPDATE media_processing_slots SET source_sha256=? "
                "WHERE output_artifact_id=? AND processor_type='download'",
                (forged_binding_sha256, artifact.id),
            )
            connection.commit()
        with (
            patch.object(media, "MEDIA_ROOT", media_root),
            patch.object(
                media.urllib.request,
                "urlopen",
                side_effect=AssertionError("forged grouping must not redownload"),
            ) as forged_urlopen,
        ):
            forged_state = media.get_media_source_state(1, db_path=self.db)
            self.assertIsNone(forged_state["download_slot"])
            self.assertEqual(
                media._queue_content_ids(
                    stage="download", limit=1, db_path=self.db
                ),
                [],
            )
            self.assertEqual(
                media._queue_content_ids(
                    stage="process", limit=1, db_path=self.db
                ),
                [],
            )
            with self.assertRaises(media.MediaProcessingError):
                media.process_image_evidence(
                    1,
                    media._resolved(artifact.local_path),
                    db_path=self.db,
                    media_root=media_root,
                    frozen_image_groups=groups,
                )
        forged_urlopen.assert_not_called()

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

        def fake_ocr(_manifest: Path, target: Path, **_kwargs) -> Path:
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

        def fake_download(urls, target_dir: Path, **kwargs) -> Path:
            target_dir.mkdir(parents=True, exist_ok=True)
            image = target_dir / "image-000.bin"
            image.write_bytes(b"\xff\xd8\xff" + b"image" * 200)
            manifest = target_dir / "manifest.json"
            values = list(urls)
            groups = media.image_source_groups(
                values, platform=str(kwargs["platform"])
            )
            source_sha256 = media._media_source_identity("image", values)[1]
            groups_sha256 = media.image_groups_sha256(groups)
            binding_sha256 = media.image_download_binding_sha256(
                source_sha256, groups_sha256
            )
            candidate = groups[0]["candidates"][0]
            image_sha256 = media.file_sha256(image)
            media._atomic_json(
                manifest,
                {
                    "schema_version": media.IMAGE_MANIFEST_VERSION,
                    "status": "complete",
                    "source_url_count": 1,
                    "source_count": 1,
                    "source_sha256": source_sha256,
                    "image_groups_sha256": groups_sha256,
                    "download_binding_sha256": binding_sha256,
                    "image_paths": [media._relative(image)],
                    "frames": [
                        {"path": media._relative(image), "sha256": image_sha256}
                    ],
                    "groups": [
                        {
                            "group_index": 0,
                            "identity": groups[0]["identity"],
                            "source_url_sha256s": [candidate["url_sha256"]],
                            "selected_url_sha256": candidate["url_sha256"],
                            "selected_response_sha256": image_sha256,
                            "selected_byte_size": image.stat().st_size,
                            "image_path": media._relative(image),
                            "attempts": [
                                {
                                    "attempt_index": 0,
                                    "source_index": candidate["source_index"],
                                    "profile": candidate["profile"],
                                    "url_sha256": candidate["url_sha256"],
                                    "outcome": "selected",
                                    "response_sha256": image_sha256,
                                    "byte_size": image.stat().st_size,
                                    "error": None,
                                }
                            ],
                        }
                    ],
                },
            )
            return manifest

        def fake_ocr(_manifest: Path, target: Path, **_kwargs) -> Path:
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

    def test_media_queue_orders_newest_first_without_excluding_old_content(self) -> None:
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

    def test_download_queue_probes_one_extra_without_executing_it(self) -> None:
        def downloaded(content_id: int, **_kwargs: object) -> dict[str, object]:
            return {"content_id": content_id, "status": "downloaded"}

        with (
            patch.object(
                media, "recover_stale_media_processing_slots", return_value={}
            ),
            patch.object(
                media, "_queue_content_ids", return_value=[1, 2, 3]
            ) as select_ids,
            patch.object(
                media, "process_content_media", side_effect=downloaded
            ) as process,
        ):
            result = media.run_media_download_queue(
                limit=2, max_workers=1, db_path=self.db
            )

        select_ids.assert_called_once_with(
            stage="download",
            limit=3,
            db_path=self.db,
        )
        self.assertEqual([item.args[0] for item in process.call_args_list], [1, 2])
        self.assertEqual(result["candidates"], 2)
        self.assertTrue(result["truncated"])
        self.assertTrue(result["has_more"])

    def test_download_queue_exact_boundary_is_not_truncated(self) -> None:
        def downloaded(content_id: int, **_kwargs: object) -> dict[str, object]:
            return {"content_id": content_id, "status": "downloaded"}

        with (
            patch.object(
                media, "recover_stale_media_processing_slots", return_value={}
            ),
            patch.object(media, "_queue_content_ids", return_value=[1, 2]),
            patch.object(media, "process_content_media", side_effect=downloaded),
        ):
            result = media.run_media_download_queue(
                limit=2, max_workers=1, db_path=self.db
            )

        self.assertEqual(result["candidates"], 2)
        self.assertFalse(result["truncated"])
        self.assertFalse(result["has_more"])

    def test_processing_queue_probes_one_extra_without_executing_it(self) -> None:
        def evidence_ready(content_id: int, **_kwargs: object) -> dict[str, object]:
            return {"content_id": content_id, "status": "evidence_ready"}

        with (
            patch.object(
                media, "recover_stale_media_processing_slots", return_value={}
            ),
            patch.object(
                media, "_queue_content_ids", return_value=[1, 2, 3]
            ) as select_ids,
            patch.object(media, "ocr_binary_path", return_value=self.video),
            patch.object(
                media, "process_content_media", side_effect=evidence_ready
            ) as process,
        ):
            result = media.run_media_processing_queue(limit=2, db_path=self.db)

        select_ids.assert_called_once_with(
            stage="process",
            limit=3,
            db_path=self.db,
        )
        self.assertEqual([item.args[0] for item in process.call_args_list], [1, 2])
        self.assertEqual(result["candidates"], 2)
        self.assertTrue(result["truncated"])
        self.assertTrue(result["has_more"])

    def test_media_queues_use_shared_default_batch_limit(self) -> None:
        with (
            patch.object(
                media, "recover_stale_media_processing_slots", return_value={}
            ),
            patch.object(media, "_queue_content_ids", return_value=[]) as select_ids,
        ):
            result = media.run_media_download_queue(db_path=self.db)

        select_ids.assert_called_once_with(
            stage="download",
            limit=media.MEDIA_QUEUE_BATCH_LIMIT + 1,
            db_path=self.db,
        )
        self.assertEqual(media.MEDIA_QUEUE_BATCH_LIMIT, 500)
        self.assertFalse(result["has_more"])

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

    def test_stale_running_slots_without_current_upstream_are_not_recovered(self) -> None:
        captured_at = "2000-01-01T00:00:00Z"
        versions = media.processor_versions()
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
                    ("b" * 64, "frames", versions["frames"], 2, captured_at, captured_at),
                    ("c" * 64, "asr", versions["asr"], 3, captured_at, captured_at),
                    ("d" * 64, "ocr", "ocr-test-v1", 1, now_utc(), now_utc()),
                ],
            )
            connection.commit()

        recovered = media.recover_stale_media_processing_slots(
            db_path=self.db,
            processor_version_by_type={
                "frames": versions["frames"],
                "asr": versions["asr"],
                "ocr": versions["ocr"],
            },
        )

        self.assertEqual(recovered["stale_candidates"], 3)
        self.assertEqual(recovered["recovered"], 0)
        self.assertEqual(recovered["retryable_failed"], 0)
        self.assertEqual(recovered["terminal_failed"], 0)
        self.assertEqual(recovered["cas_conflicts"], 0)
        with connect(self.db) as connection:
            statuses = {
                row["processor_type"]: row["status"]
                for row in connection.execute(
                    "SELECT processor_type,status FROM media_processing_slots"
                )
            }
        self.assertEqual(statuses["download"], "running")
        self.assertEqual(statuses["frames"], "running")
        self.assertEqual(statuses["asr"], "running")
        self.assertEqual(statuses["ocr"], "running")

    def test_unbound_exhausted_retryable_slot_is_not_normalized(self) -> None:
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

        self.assertEqual(recovery["exhausted_normalized"], 0)
        with connect(self.db) as connection:
            slot = connection.execute(
                "SELECT status,attempt_count FROM media_processing_slots"
            ).fetchone()
        self.assertEqual((slot["status"], slot["attempt_count"]), ("retryable_failed", 6))

    def test_generic_recovery_requires_and_accepts_current_upstream_chain(self) -> None:
        media_root = self.root / "recovery-current"
        captured_at = "2000-01-01T00:00:00Z"
        versions = media.processor_versions()
        with patch.object(media, "MEDIA_ROOT", media_root):
            media.store_media_source_manifest(
                1,
                media_kind="video",
                urls=["https://cdn.example/recovery-current.mp4"],
                raw_response_id=740,
                db_path=self.db,
            )

            def fake_download(_urls, target: Path) -> Path:
                target.parent.mkdir(parents=True, exist_ok=True)
                target.write_bytes(self.video.read_bytes())
                return target

            with patch.object(media, "_download_video", side_effect=fake_download):
                download = media.process_content_media(
                    1, download_only=True, db_path=self.db
                )
            media_artifact_id = int(download["artifact_id"])
            with connect(self.db) as connection:
                media_artifact = connection.execute(
                    "SELECT * FROM evidence_artifacts WHERE id=?",
                    (media_artifact_id,),
                ).fetchone()
                frames_path = media_root / "A2BC3D" / "frames" / "frames.json"
                media._atomic_json(frames_path, {"frames": []})
                frames = media.register_artifact(
                    connection,
                    content_id=1,
                    artifact_type="frames_manifest",
                    path=frames_path,
                    processor_version=versions["frames"],
                )
                connection.execute(
                    """
                    INSERT INTO media_processing_slots(
                        content_id,source_sha256,processor_type,processor_version,
                        status,output_artifact_id,attempt_count,created_at,updated_at
                    ) VALUES (1,?,'frames',?,'succeeded',?,1,?,?)
                    """,
                    (
                        media_artifact["sha256"],
                        versions["frames"],
                        frames.id,
                        captured_at,
                        captured_at,
                    ),
                )
                connection.executemany(
                    """
                    INSERT INTO media_processing_slots(
                        content_id,source_sha256,processor_type,processor_version,
                        status,attempt_count,created_at,updated_at
                    ) VALUES (1,?,?,?,'running',?,?,?)
                    """,
                    [
                        (
                            media_artifact["sha256"],
                            "asr",
                            versions["asr"],
                            2,
                            captured_at,
                            captured_at,
                        ),
                        (
                            frames.sha256,
                            "ocr",
                            versions["ocr"],
                            3,
                            captured_at,
                            captured_at,
                        ),
                        (
                            "f" * 64,
                            "asr",
                            versions["asr"],
                            1,
                            captured_at,
                            captured_at,
                        ),
                    ],
                )
                connection.commit()
            recovered = media.recover_stale_media_processing_slots(
                db_path=self.db,
                processor_types=("asr", "ocr"),
                processor_version_by_type={
                    "asr": versions["asr"],
                    "ocr": versions["ocr"],
                },
            )
        self.assertEqual(recovered["recovered"], 2)
        self.assertEqual(recovered["retryable_failed"], 1)
        self.assertEqual(recovered["terminal_failed"], 1)
        with connect(self.db) as connection:
            statuses = {
                row["source_sha256"]: row["status"]
                for row in connection.execute(
                    "SELECT source_sha256,status FROM media_processing_slots "
                    "WHERE processor_type IN ('asr','ocr')"
                )
            }
        self.assertEqual(statuses["f" * 64], "running")

    def test_queue_recovery_includes_old_content_with_current_work_slots(self) -> None:
        media_root = self.root / "recovery-window"
        captured_at = "2000-01-01T00:00:00Z"
        versions = media.processor_versions()
        with patch.object(media, "MEDIA_ROOT", media_root):
            media.store_media_source_manifest(
                1,
                media_kind="video",
                urls=["https://cdn.example/recovery-window.mp4"],
                raw_response_id=741,
                db_path=self.db,
            )

            def fake_download(_urls, target: Path) -> Path:
                target.parent.mkdir(parents=True, exist_ok=True)
                target.write_bytes(self.video.read_bytes())
                return target

            with patch.object(media, "_download_video", side_effect=fake_download):
                download = media.process_content_media(
                    1, download_only=True, db_path=self.db
                )
            with connect(self.db) as connection:
                artifact = connection.execute(
                    "SELECT sha256 FROM evidence_artifacts WHERE id=?",
                    (download["artifact_id"],),
                ).fetchone()
                connection.execute(
                    "UPDATE content_items SET published_at='2020-01-01T00:00:00Z' "
                    "WHERE id=1"
                )
                connection.execute(
                    """
                    INSERT INTO media_processing_slots(
                        content_id,source_sha256,processor_type,processor_version,
                        status,attempt_count,created_at,updated_at
                    ) VALUES (1,?,'asr',?,'running',1,?,?)
                    """,
                    (artifact["sha256"], versions["asr"], captured_at, captured_at),
                )
                connection.commit()
            selected = media._queue_recovery_scope_content_ids(
                stage="process",
                limit=1,
                db_path=self.db,
            )
        self.assertEqual(selected, [1])
        with connect(self.db) as connection:
            status = connection.execute(
                "SELECT status FROM media_processing_slots WHERE processor_type='asr'"
            ).fetchone()[0]
        self.assertEqual(status, "running")

    def test_generic_queue_never_recovers_unproven_douyin_grouped_slots(self) -> None:
        captured_at = "2000-01-01T00:00:00Z"
        with connect(self.db) as connection:
            connection.execute(
                "UPDATE content_items SET platform='douyin',content_type='image' "
                "WHERE id=1"
            )
            connection.execute(
                """
                INSERT INTO media_processing_slots(
                    content_id,source_sha256,processor_type,processor_version,
                    status,attempt_count,created_at,updated_at
                ) VALUES (1,?,'download',?,'running',1,?,?)
                """,
                ("f" * 64, media.IMAGE_DOWNLOAD_VERSION, captured_at, captured_at),
            )
            connection.commit()
            before = dict(
                connection.execute(
                    "SELECT * FROM media_processing_slots WHERE content_id=1"
                ).fetchone()
            )

        zero = media.run_media_download_queue(limit=0, db_path=self.db)
        positive = media.run_media_download_queue(
            limit=1, max_workers=1, db_path=self.db
        )
        with connect(self.db) as connection:
            after = dict(
                connection.execute(
                    "SELECT * FROM media_processing_slots WHERE content_id=1"
                ).fetchone()
            )
        self.assertEqual(zero["stale_recovery"], media._empty_stale_recovery_counts())
        self.assertEqual(positive["stale_recovery"]["recovered"], 0)
        self.assertEqual(before, after)

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


if __name__ == "__main__":
    unittest.main()
