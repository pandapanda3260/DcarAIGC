import json
import tempfile
import unittest
from pathlib import Path
from types import SimpleNamespace
from unittest.mock import patch

import collect_rnote_full
import collect_rnote_pilot
from collect_rnote_pilot import (
    CacheStore,
    CollectorError,
    FatalProviderError,
    RnoteClient,
    RnoteCursor,
    RequestBudget,
    apply_duplicate_filter,
    collection_status,
    find_note,
    load_key,
    normalize_content,
    normalize_comment_tree,
    parse_cursor,
    valid_user_hashes,
)


class RnoteCollectorTest(unittest.TestCase):
    def test_nested_rnote_envelope_is_unwrapped(self):
        data, billed, debug_id = RnoteClient._unwrap(
            {
                "success": True,
                "billed": True,
                "data": {
                    "success": True,
                    "code": 0,
                    "debug_id": "inner-debug",
                    "data": {"comments": []},
                },
            }
        )
        self.assertEqual(data, {"comments": []})
        self.assertTrue(billed)
        self.assertEqual(debug_id, "inner-debug")

    def test_cursor_json_is_split_into_three_query_fields(self):
        cursor = parse_cursor(
            '{"cursor":"abcdef","index":2,"pageArea":"FOLDED"}',
            RnoteCursor(),
        )
        self.assertEqual(cursor, RnoteCursor("abcdef", 2, "FOLDED"))
        self.assertEqual(
            parse_cursor("next-id", RnoteCursor("old", 4, "UNFOLDED")),
            RnoteCursor("next-id", 5, "UNFOLDED"),
        )

    def test_invalid_cursor_json_is_not_silently_reused(self):
        with self.assertRaises(CollectorError):
            parse_cursor("{not-json", RnoteCursor())

    def test_find_note_requires_requested_id(self):
        note, container = find_note(
            [{"note_list": [{"id": "a" * 24, "title": "ok"}]}],
            "a" * 24,
        )
        self.assertEqual(note["title"], "ok")
        self.assertIn("note_list", container)
        with self.assertRaises(CollectorError):
            find_note([{"note_list": [{"id": "b" * 24}]}], "a" * 24)

    def test_comment_cache_hashes_ids_filters_author_and_keeps_semantic_text(self):
        with tempfile.TemporaryDirectory() as directory:
            store = CacheStore(Path(directory))
            author_hash = store.digest("xiaohongshu:" + "a" * 24 + ":user", "author")
            rows, _ = normalize_comment_tree(
                [
                    {
                        "id": "c1",
                        "content": "这台车落地多少钱？",
                        "user": {"userid": "buyer"},
                        "sub_comments": [
                            {
                                "id": "c2",
                                "content": "我也想知道",
                                "user": {"userid": "reader"},
                            }
                        ],
                    },
                    {
                        "id": "c3",
                        "content": "作者回复",
                        "user": {"userid": "author"},
                    },
                    {
                        "id": "c4",
                        "content": "@朋友",
                        "user": {"userid": "tagger"},
                    },
                ],
                note_id="a" * 24,
                store=store,
                author_hash=author_hash,
                start_order=0,
            )
            apply_duplicate_filter(rows)
            self.assertEqual(len(valid_user_hashes(rows)), 2)
            serialized = json.dumps(rows, ensure_ascii=False)
            self.assertNotIn('"buyer"', serialized)
            self.assertNotIn('"author"', serialized)
            self.assertEqual(rows[2]["exclusion_reason"], "author_comment")
            self.assertEqual(rows[3]["exclusion_reason"], "no_semantic_text")

    def test_user_hash_is_scoped_to_content_and_default_is_two_total_attempts(self):
        with tempfile.TemporaryDirectory() as directory:
            store = CacheStore(Path(directory))
            first, _ = normalize_comment_tree(
                [{"id": "1", "content": "有效文字", "user": {"userid": "same"}}],
                note_id="a" * 24,
                store=store,
                author_hash=None,
                start_order=0,
            )
            second, _ = normalize_comment_tree(
                [{"id": "2", "content": "有效文字", "user": {"userid": "same"}}],
                note_id="b" * 24,
                store=store,
                author_hash=None,
                start_order=0,
            )
            self.assertNotEqual(first[0]["user_hash"], second[0]["user_hash"])
        client = RnoteClient("sk-test", RequestBudget(None), delay=0)
        self.assertEqual(client.retries, 1)

    def test_retired_client_fails_before_budget_or_network(self):
        budget = RequestBudget(None)
        client = RnoteClient("sk-test", budget, delay=0, retries=1)
        with (
            patch("collect_rnote_pilot.urllib.request.urlopen") as urlopen,
            self.assertRaisesRegex(CollectorError, "Rnote retired; use TikHub"),
        ):
            client.get("https://rnote.dev/api/v2/crawler/note/comments", {"note_id": "x"})
        urlopen.assert_not_called()
        self.assertEqual(budget.used, 0)
        self.assertEqual(client.request_log, [])

    def test_retired_collectors_fail_before_credentials_or_input(self):
        with (
            patch("collect_rnote_pilot.load_key") as pilot_key,
            patch("collect_rnote_pilot.build_base_rows") as pilot_input,
            self.assertRaisesRegex(CollectorError, "Rnote retired; use TikHub"),
        ):
            collect_rnote_pilot.run_pilot(SimpleNamespace())
        pilot_key.assert_not_called()
        pilot_input.assert_not_called()

        with (
            patch("collect_rnote_full.load_key") as full_key,
            patch("collect_rnote_full.read_input") as full_input,
            self.assertRaisesRegex(CollectorError, "Rnote retired; use TikHub"),
        ):
            collect_rnote_full.collect_all(SimpleNamespace())
        full_key.assert_not_called()
        full_input.assert_not_called()

    def test_retired_collector_clis_fail_before_credentials(self):
        with (
            patch(
                "collect_rnote_pilot.parse_args",
                return_value=SimpleNamespace(),
            ),
            patch("collect_rnote_pilot.load_key") as pilot_key,
            self.assertRaisesRegex(CollectorError, "Rnote retired; use TikHub"),
        ):
            collect_rnote_pilot.main()
        pilot_key.assert_not_called()

        with (
            patch(
                "collect_rnote_full.parse_args",
                return_value=SimpleNamespace(preflight=False),
            ),
            patch("collect_rnote_full.load_key") as full_key,
            self.assertRaisesRegex(CollectorError, "Rnote retired; use TikHub"),
        ):
            collect_rnote_full.main()
        full_key.assert_not_called()

    def test_content_author_and_comment_user_use_the_same_content_scope(self):
        with tempfile.TemporaryDirectory() as directory:
            store = CacheStore(Path(directory))
            note_id = "a" * 24
            content = normalize_content(
                [{"note_list": [{"id": note_id, "user": {"userid": "author"}}]}],
                row={"note_id": note_id, "sample_attempt_id": "A1", "url": "https://www.xiaohongshu.com/explore/" + note_id},
                endpoint_type="image",
                store=store,
            )
            comments, _ = normalize_comment_tree(
                [{"id": "comment", "content": "作者回复", "user": {"userid": "author"}}],
                note_id=note_id,
                store=store,
                author_hash=content["author_hash"],
                start_order=0,
            )
            self.assertEqual(comments[0]["exclusion_reason"], "author_comment")

    def test_balance_and_authentication_errors_are_fatal(self):
        for payload in (
            {"success": False, "error": "insufficient balance"},
            {"success": False, "detail": "API key invalid"},
        ):
            with self.assertRaises(FatalProviderError):
                RnoteClient._unwrap(payload)

    def test_repeated_long_copy_is_excluded_but_short_common_text_is_kept(self):
        records = []
        for index in range(3):
            records.extend(
                [
                    {
                        "user_hash": f"long-{index}",
                        "text": "这是完全相同的一段引流复制文字123",
                        "base_exclusion_reason": None,
                    },
                    {
                        "user_hash": f"short-{index}",
                        "text": "好看",
                        "base_exclusion_reason": None,
                    },
                ]
            )
        apply_duplicate_filter(records)
        self.assertEqual(
            [row["exclusion_reason"] for row in records if row["text"] == "好看"],
            [None, None, None],
        )
        self.assertEqual(
            {
                row["exclusion_reason"]
                for row in records
                if row["text"].startswith("这是")
            },
            {"duplicate_copy_text"},
        )

    def test_collection_status_does_not_treat_partial_below_gate_as_zero(self):
        content = {"title": "x"}
        base = {"content": {"status": "complete"}}
        partial = {
            **base,
            "comments": {
                "status": "partial",
                "valid_unique_commenters": 10,
                "stop_reason": "request_error",
            },
        }
        self.assertEqual(
            collection_status(content, partial)["comment_sample_status"],
            "technical_missing",
        )
        empty = {
            **base,
            "comments": {
                "status": "confirmed_empty",
                "valid_unique_commenters": 0,
                "stop_reason": "confirmed_empty",
            },
        }
        self.assertEqual(
            collection_status(content, empty)["comment_sample_status"],
            "confirmed_zero",
        )

    def test_key_parser_accepts_raw_or_env_form_without_rewriting_file(self):
        with tempfile.TemporaryDirectory() as directory:
            path = Path(directory) / "key.txt"
            path.write_text("RNOTE_API_KEY=sk-example-value\n", encoding="utf-8")
            self.assertEqual(load_key(path), "sk-example-value")
            path.write_text("not-a-key\n", encoding="utf-8")
            with self.assertRaises(CollectorError):
                load_key(path)


if __name__ == "__main__":
    unittest.main()
