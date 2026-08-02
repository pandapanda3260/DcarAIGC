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
