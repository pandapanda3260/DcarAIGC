#!/usr/bin/env python3

import unittest

from fetch_onebound_comments import (
    aggregate_valid_users,
    comment_items,
    extract_comment_records,
    response_cursor,
)


class OneBoundCommentParserTest(unittest.TestCase):
    def test_comment_envelope_aliases(self):
        node = {"comment_id": "c1", "user_id": "u1", "content": "hello"}
        fixtures = [
            {"items": {"item": [node]}},
            {"items": {"list": [node]}},
            {"data": {"comments": [node]}},
            {"data": {"items": {"list": [node]}}},
            {"comments": [node]},
            {"items": [node]},
        ]
        for payload in fixtures:
            with self.subTest(payload=payload):
                self.assertEqual(comment_items(payload), [node])

    def test_subcomment_aliases_are_flattened(self):
        parent = {
            "comment_id": "c1",
            "user_id": "u1",
            "content": "main",
            "sub_comments": [
                {"commentId": "c2", "userId": "u2", "commentContent": "reply"}
            ],
        }
        records, node_count = extract_comment_records([parent], "test-secret")
        self.assertEqual(node_count, 2)
        self.assertEqual([record["text"] for record in records], ["main", "reply"])
        self.assertEqual(records[1]["parent_comment_hash"], records[0]["comment_hash"])

    def test_unique_users_not_unique_texts_are_counted(self):
        nodes = [
            {"comment_id": "c1", "user_id": "u1", "content": "同一句"},
            {"comment_id": "c2", "user_id": "u2", "content": "同一句"},
            {"comment_id": "c3", "user_id": "u1", "content": "第二句"},
        ]
        records, _ = extract_comment_records(nodes, "test-secret")
        users = aggregate_valid_users(records)
        self.assertEqual(len(users), 2)
        self.assertEqual(sorted(len(user["texts"]) for user in users), [1, 2])

    def test_author_spam_empty_and_missing_identity_are_excluded(self):
        nodes = [
            {"comment_id": "c1", "user_id": "u1", "content": "作者回复", "is_author": True},
            {"comment_id": "c2", "user_id": "u2", "content": "加我微信进群"},
            {"comment_id": "c3", "user_id": "u3", "content": "😂😂"},
            {"comment_id": "c4", "content": "这车不错"},
            {"comment_id": "c5", "user_id": "u5", "content": "这车不错"},
        ]
        records, _ = extract_comment_records(nodes, "test-secret")
        users = aggregate_valid_users(records)
        self.assertEqual(len(users), 1)
        self.assertEqual(users[0]["texts"], ["这车不错"])
        self.assertEqual(
            [record["exclusion_reason"] for record in records[:4]],
            [
                "author_reply",
                "spam_or_engagement_bait",
                "no_semantic_text",
                "missing_user_identity",
            ],
        )

    def test_voice_transcript_is_text_but_numeric_count_is_not(self):
        nodes = [
            {"comment_id": "c1", "user_id": "u1", "voice_count": "我的声音是低音炮"},
            {"comment_id": "c2", "user_id": "u2", "voice_count": "12"},
        ]
        records, _ = extract_comment_records(nodes, "test-secret")
        self.assertEqual(records[0]["text"], "我的声音是低音炮")
        self.assertTrue(records[0]["is_valid"])
        self.assertEqual(records[1]["text"], "")
        self.assertFalse(records[1]["is_valid"])

    def test_pagination_aliases(self):
        self.assertEqual(
            response_cursor({"data": {"nextCursor": "abc", "hasMore": "true"}}),
            ("abc", True),
        )
        self.assertEqual(
            response_cursor({"items": {"cursor": "def", "has_more": 0}}),
            ("def", False),
        )


if __name__ == "__main__":
    unittest.main()
