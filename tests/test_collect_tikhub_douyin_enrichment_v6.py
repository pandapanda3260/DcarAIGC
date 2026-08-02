#!/usr/bin/env python3

import unittest

import collect_tikhub_douyin_enrichment_v6 as v6


class TikHubDouyinEnrichmentV6Test(unittest.TestCase):
    def test_sanitizer_retains_no_raw_identity(self) -> None:
        payload = {
            "data": {
                "cursor": 20,
                "has_more": True,
                "total": 30,
                "comments": [{
                    "text": "我的秦L高速油耗5.2",
                    "level": 1,
                    "digg_count": 3,
                    "user": {"uid": "123", "sec_uid": "MS4w", "nickname": "张三"},
                }],
            }
        }
        page = v6.sanitize_comment_page(
            aweme_id="999", author_uid="456", cursor_requested=0, payload=payload
        )
        encoded = str(page)
        self.assertNotIn("张三", encoded)
        self.assertNotIn("MS4w", encoded)
        self.assertNotIn("'123'", encoded)
        self.assertTrue(page["comments"][0]["user_key"].startswith("U"))

    def test_author_spam_and_empty_are_not_valid(self) -> None:
        pages = [{"comments": [
            {"user_key": "U1", "is_author": True, "text": "作者回复"},
            {"user_key": "U2", "is_author": False, "text": "加微信进群"},
            {"user_key": "U3", "is_author": False, "text": "😂😂"},
            {"user_key": "U4", "is_author": False, "text": "落地多少钱"},
            {"user_key": "U4", "is_author": False, "text": "哪个配置"},
        ]}]
        users = v6.valid_unique_comments(pages)
        self.assertEqual(users, {"U4": "落地多少钱；哪个配置"})

    def test_chunking_is_two_per_statistics_call(self) -> None:
        self.assertEqual(list(v6.chunks(["1", "2", "3"], 2)), [["1", "2"], ["3"]])


if __name__ == "__main__":
    unittest.main()
