from __future__ import annotations

import tempfile
import unittest
from pathlib import Path

import process_xhs_media_v5 as media


class ProcessXhsMediaV5Test(unittest.TestCase):
    def test_combined_ocr_deduplicates_in_source_order(self):
        value = media.combined_ocr_text([
            {"observations": [{"text": "懂车帝 查价格"}, {"text": "同级对比"}]},
            {"observations": [{"text": "懂车帝 查价格"}, {"text": " 真实车主口碑 "}]},
        ])
        self.assertEqual(value.splitlines(), ["懂车帝 查价格", "同级对比", "真实车主口碑"])

    def test_compact_segments_removes_provider_bulk_fields(self):
        value = media.compact_segments([{
            "start": 1.234, "end": 2.345, "text": " 测试 ",
            "avg_logprob": -0.12345, "no_speech_prob": 0.01234, "tokens": [1, 2],
        }])
        self.assertNotIn("tokens", value[0])
        self.assertEqual(value[0]["text"], "测试")

    def test_transcript_need_requires_video_and_insufficient_ocr(self):
        self.assertFalse(media.transcript_needed("definitely-missing-note"))

    def test_text_file_is_never_accepted_as_video(self):
        with tempfile.TemporaryDirectory() as directory:
            path = Path(directory) / "fake.mp4"
            path.write_text("1\n00:00:01 --> 00:00:02\nsubtitle\n" * 100, encoding="utf-8")
            self.assertFalse(media.valid_video(path))

    def test_cover_ocr_is_invalidated_when_full_video_becomes_available(self):
        cached = {"status": "success", "source_kind": "all_original_images"}
        self.assertTrue(media.reusable_ocr(cached, has_video=False))
        self.assertFalse(media.reusable_ocr(cached, has_video=True))
        self.assertTrue(
            media.reusable_ocr(
                {"status": "success", "source_kind": "video_frames"},
                has_video=True,
            )
        )


if __name__ == "__main__":
    unittest.main()
