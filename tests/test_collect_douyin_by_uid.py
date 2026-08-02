import json
from pathlib import Path
import tempfile
import unittest

from collect_douyin_by_uid import collect_uid, normalize_post


UID = "1619994549436234"


def profile(uid: str = UID) -> dict:
    return {
        "status_code": 0,
        "user": {
            "uid": uid,
            "sec_uid": "SEC_UID",
            "nickname": "测试账号",
            "unique_id": "douyin-test",
            "aweme_count": 1,
        },
    }


def post(uid: str = UID) -> dict:
    return {
        "aweme_id": "1234567890",
        "desc": "测试作品",
        "create_time": 1_700_000_000,
        "author": {"uid": uid},
        "statistics": {"digg_count": 3, "comment_count": 2},
        "video": {
            "duration": 5000,
            "cover": {"url_list": ["https://cdn.example/cover.jpg"]},
            "play_addr": {"url_list": ["https://cdn.example/video.mp4"]},
        },
    }


class NeverNetworkClient:
    def profile(self, uid: str) -> dict:
        raise AssertionError("cache hit should not request profile")

    def recent_posts(self, sec_uid: str, count: int) -> dict:
        raise AssertionError("cache hit should not request posts")


class DouyinCollectorTests(unittest.TestCase):
    def test_normalize_video_uses_canonical_share_url(self) -> None:
        row = normalize_post(UID, profile(), post())
        self.assertEqual(row["content_type"], "video")
        self.assertEqual(row["share_url"], "https://www.douyin.com/video/1234567890")
        self.assertEqual(row["video_url"], "https://cdn.example/video.mp4")
        self.assertEqual(row["returned_author_uid"], UID)

    def test_normalize_image_text_prefers_image_as_cover(self) -> None:
        item = post()
        item["video"] = {}
        item["images"] = [{"url_list": ["https://cdn.example/image.jpg"]}]
        row = normalize_post(UID, profile(), item)
        self.assertEqual(row["content_type"], "image_text")
        self.assertEqual(row["cover_url"], "https://cdn.example/image.jpg")
        self.assertEqual(row["image_urls"], ["https://cdn.example/image.jpg"])

    def test_cached_collection_does_not_use_network(self) -> None:
        with tempfile.TemporaryDirectory() as temp_dir:
            account_dir = Path(temp_dir) / "accounts" / UID
            account_dir.mkdir(parents=True)
            (account_dir / "profile_raw.json").write_text(
                json.dumps(profile()), encoding="utf-8"
            )
            (account_dir / "posts_page_001_raw.json").write_text(
                json.dumps({"status_code": 0, "aweme_list": [post()], "has_more": 0}),
                encoding="utf-8",
            )
            account, rows = collect_uid(
                NeverNetworkClient(), UID, Path(temp_dir), count=20, refresh=False
            )
            self.assertTrue(account["cache_used"])
            self.assertEqual(account["collected_recent_count"], 1)
            self.assertEqual(len(rows), 1)

    def test_cached_profile_uid_mismatch_is_rejected(self) -> None:
        with tempfile.TemporaryDirectory() as temp_dir:
            account_dir = Path(temp_dir) / "accounts" / UID
            account_dir.mkdir(parents=True)
            (account_dir / "profile_raw.json").write_text(
                json.dumps(profile("999")), encoding="utf-8"
            )
            (account_dir / "posts_page_001_raw.json").write_text(
                json.dumps({"status_code": 0, "aweme_list": []}), encoding="utf-8"
            )
            with self.assertRaisesRegex(RuntimeError, "账号解析校验失败"):
                collect_uid(
                    NeverNetworkClient(), UID, Path(temp_dir), count=20, refresh=False
                )


if __name__ == "__main__":
    unittest.main()
