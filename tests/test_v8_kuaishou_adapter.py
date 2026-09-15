"""Kuaishou response contracts use redacted identities and preserve missing values."""

from __future__ import annotations

import copy
import unittest

from v8.capture import CaptureError
from v8 import kuaishou_adapter as adapter


def profile_payload():
    # Shape from the saved complete profile response; identities/content redacted.
    return {
        "code": 200, "router": adapter.PROFILE_PATH, "params": {"user_id": "001234"},
        "data": {"result": 1, "userProfile": {
            "profile": {"user_id": "001234", "user_name": "示例", "user_text": "介绍"},
            "ownerCount": {"fan": 0, "photo_public": "12", "total_photo_like": 30},
        }},
    }


def feed_item():
    # Minimal structural projection of the complete 2026-09-12 posts response.
    return {
        "photo_id": 5234567890123456789, "user_id": "001234", "user_name": "示例",
        "caption": "作品", "timestamp": 1789038695454, "type": 1,
        "view_count": 12, "like_count": 0, "comment_count": 1,
        "share_count": 2, "collect_count": 3,
        "share_info": "userId=3xuser&photoId=3xwork",
        "main_mv_urls": [{"url": "http://v1.kwaicdn.com/video.mp4?signature=original"}],
    }


def discovery_payload():
    return {
        "code": 200, "router": adapter.DISCOVERY_PATH,
        "params": {"user_id": "001234", "pcursor": "", "sort": "latest"},
        "data": {"result": 1, "feeds": [feed_item()], "pcursor": "1.786699524802E12"},
    }


def detail_payload():
    # The complete detail response uses photos, not feeds or a guessed photo.
    return {
        "code": 200, "router": adapter.DETAIL_PATH,
        "params": {"photo_id": "5234567890123456789"},
        "data": {"result": 1, "photos": [feed_item()]},
    }


class KuaishouRequestTests(unittest.TestCase):
    def test_request_ids_preserve_zeros(self):
        self.assertEqual(adapter.request_spec("profile", "001234")["params"], {"user_id": "001234"})
        self.assertEqual(adapter.request_spec("detail", "001234")["params"], {"photo_id": "001234"})
        self.assertEqual(adapter.request_spec("metrics", "3xexample")["path"], adapter.DETAIL_PATH)

    def test_discovery_passes_opaque_cursor_and_latest_sort(self):
        self.assertEqual(adapter.request_spec("discovery", "001234", "opaque=2/")["params"], {
            "user_id": "001234", "pcursor": "opaque=2/", "sort": "latest",
        })

    def test_reject_unsafe_account_identifiers(self):
        for value in (None, True, 1.2, "1.2e4", "3xeid", "昵称", "１２３"):
            with self.subTest(value=value), self.assertRaises(ValueError):
                adapter.request_spec("discovery", value)

    def test_comments_are_not_claimed_supported(self):
        with self.assertRaises(ValueError):
            adapter.request_spec("comments", "1234")

    def test_reject_numeric_or_terminal_cursor(self):
        for cursor in (100, True, "no_more"):
            with self.subTest(cursor=cursor), self.assertRaises(ValueError):
                adapter.request_spec("discovery", "1234", cursor)


class KuaishouProfileTests(unittest.TestCase):
    def test_profile_exact_uid_and_explicit_zero(self):
        result = adapter.parse_profile(profile_payload(), "001234")
        self.assertEqual(result["account_uid"], "001234")
        self.assertEqual(result["follower_count"], 0)
        self.assertEqual(result["platform_work_count"], 12)
        self.assertEqual(result["field_status"]["follower_count"]["status"], "provided")

    def test_no_mutation_or_guessing_missing_fields(self):
        payload = profile_payload()
        del payload["data"]["userProfile"]["ownerCount"]
        original = copy.deepcopy(payload)
        result = adapter.parse_profile(payload, "001234")
        self.assertIsNone(result["follower_count"])
        self.assertEqual(result["field_status"]["follower_count"]["status"], "missing")
        self.assertNotIn("account_id", result)
        self.assertEqual(payload, original)

    def test_approximate_and_invalid_counts_not_rounded(self):
        for raw in ("1.2万", 1.2, True, -1, "未知"):
            with self.subTest(raw=raw):
                payload = profile_payload()
                payload["data"]["userProfile"]["ownerCount"]["fan"] = raw
                result = adapter.parse_profile(payload, "001234")
                self.assertIsNone(result["follower_count"])
                self.assertEqual(result["field_status"]["follower_count"]["status"], "invalid")

    def test_identity_conflicts_fail_even_if_names_match(self):
        for at in ("returned", "request"):
            with self.subTest(at=at):
                payload = profile_payload()
                if at == "returned":
                    payload["data"]["userProfile"]["profile"]["user_id"] = "1234"
                else:
                    payload["params"]["user_id"] = "1234"
                with self.assertRaises(CaptureError) as caught:
                    adapter.parse_profile(payload, "001234")
                self.assertEqual(caught.exception.error_code, "identity_conflict")

    def test_requires_full_success_envelope_and_exact_profile_path(self):
        for transform in (
            lambda p: p.update(code="200"),
            lambda p: p.update(data=None),
            lambda p: p["data"].update(result=True),
            lambda p: p["data"].update(result=0),
            lambda p: p["data"].pop("result"),
            lambda p: p.update(router=adapter.DISCOVERY_PATH),
            lambda p: p["data"]["userProfile"].pop("profile"),
        ):
            payload = profile_payload()
            transform(payload)
            with self.assertRaises(CaptureError):
                adapter.parse_profile(payload, "001234")


class KuaishouDiscoveryTests(unittest.TestCase):
    def test_exact_identity_large_id_and_opaque_cursor(self):
        result = adapter.parse_discovery(discovery_payload(), "001234")
        item = result["items"][0]
        self.assertEqual(item["platform_content_id"], "5234567890123456789")
        self.assertEqual(item["account_uid"], "001234")
        self.assertEqual(item["canonical_url"], "https://www.kuaishou.com/short-video/3xwork")
        self.assertEqual(result["next_cursor"], "1.786699524802E12")
        self.assertTrue(result["has_more"])
        self.assertEqual(item["metrics"]["like_count"], 0)
        self.assertTrue(item["published_at"].endswith("Z"))

    def test_media_url_signature_preserved_and_avatar_excluded(self):
        payload = discovery_payload()
        payload["data"]["feeds"][0]["headurls"] = [{"url": "https://example.com/avatar.jpg"}]
        item = adapter.parse_discovery(payload, "001234")["items"][0]
        self.assertEqual(item["media_urls"], ["http://v1.kwaicdn.com/video.mp4?signature=original"])

    def test_missing_metrics_stay_missing(self):
        payload = discovery_payload()
        del payload["data"]["feeds"][0]["view_count"]
        metrics = adapter.parse_discovery(payload, "001234")["items"][0]["metrics"]
        self.assertIsNone(metrics["view_count"])
        self.assertEqual(metrics["_field_status"]["view_count"]["status"], "missing")

    def test_empty_page_with_cursor_is_not_exhaustion(self):
        payload = discovery_payload()
        payload["data"]["feeds"] = []
        result = adapter.parse_discovery(payload, "001234")
        self.assertTrue(result["has_more"])

    def test_only_explicit_terminal_cursor_closes_pagination(self):
        for feeds in ([], [feed_item()]):
            payload = discovery_payload()
            payload["data"].update(feeds=feeds, pcursor="no_more")
            result = adapter.parse_discovery(payload, "001234")
            self.assertFalse(result["has_more"])
            self.assertIsNone(result["next_cursor"])
            self.assertEqual(result["pagination_evidence"]["value"], "no_more")

    def test_empty_page_without_exact_request_uid_cannot_claim_account_coverage(self):
        payload = discovery_payload()
        payload["data"]["feeds"] = []
        del payload["params"]["user_id"]
        with self.assertRaises(CaptureError):
            adapter.parse_discovery(payload, "001234")

    def test_missing_invalid_cursor_or_feed_cannot_claim_coverage(self):
        for transform in (
            lambda p: p["data"].pop("pcursor"),
            lambda p: p["data"].update(pcursor=""),
            lambda p: p["data"].update(pcursor=None),
            lambda p: p["data"].update(pcursor=1786999524802),
            lambda p: p["data"].update(feeds=None),
            lambda p: p["data"].update(feeds=[{}]),
            lambda p: p["params"].update(pcursor="1.786699524802E12"),
            lambda p: p["params"].update(sort="hot"),
        ):
            payload = discovery_payload()
            transform(payload)
            with self.assertRaises(CaptureError):
                adapter.parse_discovery(payload, "001234")

    def test_cross_account_row_rejects_the_whole_page(self):
        payload = discovery_payload()
        wrong = feed_item()
        wrong["photo_id"], wrong["user_id"] = "5555", "9999"
        payload["data"]["feeds"].append(wrong)
        with self.assertRaises(CaptureError) as caught:
            adapter.parse_discovery(payload, "001234")
        self.assertEqual(caught.exception.error_code, "identity_conflict")

    def test_duplicate_identity_cannot_silently_replace_different_data(self):
        payload = discovery_payload()
        payload["data"]["feeds"].append(feed_item())
        self.assertEqual(len(adapter.parse_discovery(payload, "001234")["items"]), 1)
        payload["data"]["feeds"][1]["caption"] = "不同资料"
        with self.assertRaises(CaptureError):
            adapter.parse_discovery(payload, "001234")

    def test_unverified_content_type_stays_unknown(self):
        payload = discovery_payload()
        payload["data"]["feeds"][0]["type"] = 99
        item = adapter.parse_discovery(payload, "001234")["items"][0]
        self.assertEqual(item["content_type"], "unknown")
        self.assertEqual(item["media_urls"], [])


class KuaishouStageTests(unittest.TestCase):
    def test_detail_and_metrics_share_response_with_exact_identity(self):
        payload = detail_payload()
        detail = adapter.parse_stage("detail", "5234567890123456789", payload, "001234")
        metrics = adapter.parse_stage("metrics", "5234567890123456789", payload, "001234")
        self.assertEqual(detail["metrics"]["view_count"], metrics["view_count"])
        self.assertEqual(metrics["like_count"], 0)
        self.assertEqual(detail["account_uid"], "001234")
        self.assertEqual(detail["content_type"], "video")

    def test_short_id_requires_the_same_photo_explicit_share_mapping(self):
        payload = detail_payload()
        payload["params"]["photo_id"] = "3xwork"
        self.assertEqual(adapter.parse_stage("detail", "3xwork", payload)["platform_content_id"], "5234567890123456789")
        payload["data"]["photos"][0]["share_info"] = "photoId=3xother"
        with self.assertRaises(CaptureError):
            adapter.parse_stage("detail", "3xwork", payload)

    def test_wrong_photo_or_author_never_updates_metrics(self):
        for field, wrong in (("photo_id", "5555"), ("user_id", "9999")):
            with self.subTest(field=field):
                payload = detail_payload()
                payload["data"]["photos"][0][field] = wrong
                with self.assertRaises(CaptureError) as caught:
                    adapter.parse_stage("metrics", "5234567890123456789", payload, "001234")
                self.assertEqual(caught.exception.error_code, "identity_conflict")

    def test_detail_rejects_ambiguous_or_unknown_wrappers(self):
        for data in (
            {"result": 1, "photo": feed_item()},
            {"result": 1, "feeds": [feed_item()]},
            {"result": 1, "photos": []},
            {"result": 1, "photos": [feed_item(), feed_item()]},
        ):
            payload = detail_payload()
            payload["data"] = data
            with self.assertRaises(CaptureError):
                adapter.parse_stage("detail", "5234567890123456789", payload)

    def test_missing_metrics_are_not_successful_zero(self):
        payload = detail_payload()
        payload["data"]["photos"][0].pop("view_count")
        result = adapter.parse_stage("metrics", "5234567890123456789", payload)
        self.assertIsNone(result["view_count"])
        self.assertEqual(result["_field_status"]["view_count"]["status"], "missing")


if __name__ == "__main__":
    unittest.main()
