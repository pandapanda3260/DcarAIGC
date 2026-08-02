from __future__ import annotations

import tempfile
import unittest
from pathlib import Path

from collect_rnote_pilot import CacheStore, CollectorError, FatalProviderError
from collect_tikhub_xhs_full import IMAGE_ENDPOINT, VIDEO_ENDPOINT, collect_content, unwrap


class TikHubXhsFullTest(unittest.TestCase):
    def test_nested_success_payload_is_unwrapped(self):
        self.assertEqual(
            unwrap({"code": 200, "data": {"code": 0, "success": True, "data": {"comments": []}}}),
            {"comments": []},
        )

    def test_balance_failure_stops_the_batch(self):
        with self.assertRaises(FatalProviderError):
            unwrap({"code": 402, "message_zh": "余额不足"})

    def test_business_error_is_not_silently_accepted(self):
        with self.assertRaises(CollectorError):
            unwrap({"code": 200, "data": {"code": 1001, "success": False, "msg": "not found"}})

    def test_video_note_falls_through_image_detail_to_video_detail(self):
        note_id = "6a047212000000000702764f"

        class Client:
            def __init__(self):
                self.calls = []

            def get(self, endpoint, params):
                self.calls.append(endpoint)
                note = {"id": note_id, "type": "video", "title": "汽车视频", "images_list": []}
                if endpoint == VIDEO_ENDPOINT:
                    note["video_info_v2"] = {
                        "media": {"stream": {"h264": [{"master_url": "https://cdn.example/video.mp4"}]}}
                    }
                return [note]

        with tempfile.TemporaryDirectory() as temporary:
            client = Client()
            content = collect_content(
                {
                    "note_id": note_id,
                    "sample_attempt_id": "A0001",
                    "url": f"https://www.xiaohongshu.com/explore/{note_id}",
                },
                store=CacheStore(Path(temporary)),
                client=client,
            )

        self.assertEqual(client.calls, [IMAGE_ENDPOINT, VIDEO_ENDPOINT])
        self.assertEqual(content["endpoint_type"], "video")
        self.assertEqual(content["video_urls"][0]["url"], "https://cdn.example/video.mp4")


if __name__ == "__main__":
    unittest.main()
