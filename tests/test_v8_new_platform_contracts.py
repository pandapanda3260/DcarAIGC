"""New platform recovery, media URL and operation-price integration boundaries."""

from __future__ import annotations

import unittest

from tests.test_v8_kuaishou_adapter import profile_payload, discovery_payload, detail_payload
from tests.test_v8_wechat_channels_adapter import UID, OBJECT, response, video
from v8 import media, provider_budget, providers
from v8.operation_contracts import OperationContractError, require_response


KS_UID, KS_OBJECT = "001234", "5234567890123456789"


def specimens(platform):
    if platform == "kuaishou":
        return KS_UID, KS_OBJECT, profile_payload(), discovery_payload(), detail_payload()
    return UID, OBJECT, response({"contact": {"username": UID, "nickname": "测试"}, "fansCount": 0}), response({
        "baseResponse": {"ret": 0}, "contact": {"username": UID},
        "object": [video()], "continueFlag": 1, "upContinueFlag": 0, "lastBuffer": "opaque+//==",
    }), response({"baseResponse": {"ret": 0}, "objects": [video()]})


def request_identity(platform, suffix, subject, params):
    return {"provider": "tikhub", "operation": f"{platform}_{suffix}",
            "subject": subject, "request_parameters": params, "cursor": ""}


class NewPlatformOperationContractTests(unittest.TestCase):
    def require(self, platform, suffix, subject, params, payload, uid):
        return require_response(f"{platform}_{suffix}",
                                request_identity(platform, suffix, subject, params),
                                payload, account_uid=uid)

    def test_all_new_operations_accept_exact_subject_and_explicit_zero(self):
        for platform in ("kuaishou", "wechat_channels"):
            uid, obj, profile, page, detail = specimens(platform)
            account_param = "user_id" if platform == "kuaishou" else "username"
            content_param = "photo_id" if platform == "kuaishou" else "object_id"
            for suffix, subject, params, payload in (
                ("user_profile", uid, {account_param: uid}, profile),
                ("user_posts", uid, {account_param: uid}, page),
                ("video_detail", obj, {content_param: obj}, detail),
                ("video_statistics", obj, {content_param: obj}, detail),
            ):
                with self.subTest(platform=platform, suffix=suffix):
                    self.require(platform, suffix, subject, params, payload, uid)

    def test_frozen_request_parameter_cannot_disagree_with_valid_response(self):
        # A valid response for the subject cannot rescue a contradictory frozen
        # paid-request parameter; every operation must bind both independently.
        for platform in ("kuaishou", "wechat_channels"):
            uid, obj, profile, page, detail = specimens(platform)
            account_param = "user_id" if platform == "kuaishou" else "username"
            content_param = "photo_id" if platform == "kuaishou" else "object_id"
            for suffix, subject, parameter, payload in (
                ("user_profile", uid, account_param, profile),
                ("user_posts", uid, account_param, page),
                ("video_detail", obj, content_param, detail),
                ("video_statistics", obj, content_param, detail),
            ):
                with self.subTest(platform=platform, suffix=suffix), self.assertRaises(OperationContractError):
                    self.require(platform, suffix, subject, {parameter: "different"}, payload, uid)

    def test_wrong_content_author_is_rejected_for_detail_and_metrics(self):
        for platform in ("kuaishou", "wechat_channels"):
            uid, obj, _, _, detail = specimens(platform)
            parameter = "photo_id" if platform == "kuaishou" else "object_id"
            if platform == "kuaishou":
                detail["data"]["photos"][0]["user_id"] = "9999"
            else:
                detail["data"]["objects"][0].update(username="v2_different@finder", contact={})
            for suffix in ("video_detail", "video_statistics"):
                with self.subTest(platform=platform, suffix=suffix), self.assertRaises(OperationContractError):
                    self.require(platform, suffix, obj, {parameter: obj}, detail, uid)

    def test_cross_account_discovery_cannot_prove_recovery(self):
        for platform in ("kuaishou", "wechat_channels"):
            uid, _, _, page, _ = specimens(platform)
            parameter = "user_id" if platform == "kuaishou" else "username"
            if platform == "kuaishou":
                page["data"]["feeds"][0]["user_id"] = "9999"
            else:
                page["data"]["object"][0].update(username="v2_different@finder", contact={})
            with self.subTest(platform=platform), self.assertRaises(OperationContractError):
                self.require(platform, "user_posts", uid, {parameter: uid}, page, uid)

    def test_discovery_without_publication_time_cannot_prove_scan_recovery(self):
        for platform in ("kuaishou", "wechat_channels"):
            uid, _, _, page, _ = specimens(platform)
            parameter = "user_id" if platform == "kuaishou" else "username"
            if platform == "kuaishou":
                page["data"]["feeds"][0].pop("timestamp")
            else:
                page["data"]["object"][0].pop("createtime")
            with self.subTest(platform=platform), self.assertRaises(OperationContractError):
                self.require(platform, "user_posts", uid, {parameter: uid}, page, uid)

    def test_metrics_explicit_zero_is_evidence_but_all_missing_is_not(self):
        for platform in ("kuaishou", "wechat_channels"):
            uid, obj, _, _, detail = specimens(platform)
            parameter = "photo_id" if platform == "kuaishou" else "object_id"
            raw = detail["data"]["photos" if platform == "kuaishou" else "objects"][0]
            fields = ("view_count", "like_count", "comment_count", "share_count", "collect_count") if platform == "kuaishou" else ("likeCount", "commentCount", "forwardCount", "favCount", "readCount")
            for field in fields:
                raw.pop(field, None)
            raw["like_count" if platform == "kuaishou" else "likeCount"] = 0
            with self.subTest(platform=platform, state="zero"):
                self.require(platform, "video_statistics", obj, {parameter: obj}, detail, uid)
            raw.pop("like_count" if platform == "kuaishou" else "likeCount")
            with self.subTest(platform=platform, state="missing"), self.assertRaises(OperationContractError):
                self.require(platform, "video_statistics", obj, {parameter: obj}, detail, uid)

    def test_missing_bound_author_never_validates_recovery(self):
        for platform in ("kuaishou", "wechat_channels"):
            uid, obj, _, page, detail = specimens(platform)
            for suffix, subject, payload in (("user_posts", uid, page), ("video_detail", obj, detail), ("video_statistics", obj, detail)):
                with self.subTest(platform=platform, suffix=suffix), self.assertRaises(OperationContractError):
                    self.require(platform, suffix, subject, {}, payload, None)


class NewPlatformMediaAndPriceTests(unittest.TestCase):
    def test_observed_http_kuaishou_cdn_preserves_signed_query(self):
        value = "http://v23-3.kwaicdn.com/media.mp4?pkey=AA%2B//%3D&tag=a-b&clientCacheKey=xyz"
        self.assertTrue(media.is_supported_media_url(value))
        self.assertEqual(media._normalize_media_url(value), value)
        urls, _ = media._media_source_identity("video", [value])
        self.assertEqual(urls, [value])

    def test_http_lookalike_or_other_hosts_are_rejected(self):
        for value in (
            "http://v1.kwaicdn.com.evil.invalid/media.mp4",
            "http://evil-kwaicdn.com/media.mp4",
            "http://kwaicdn.comevil.invalid/media.mp4",
            "http://v1.kwaicdn.com@evil.invalid/media.mp4",
            "http://127.0.0.1/media.mp4",
        ):
            with self.subTest(value=value):
                self.assertFalse(media.is_supported_media_url(value))
                self.assertIsNone(media._normalize_media_url(value))

    def test_userinfo_is_not_preserved_in_a_media_source(self):
        self.assertIsNone(media._normalize_media_url("http://user:secret@v1.kwaicdn.com/media.mp4"))

    def test_kuaishou_reservation_and_dispatch_prices_match_verified_endpoint_prices(self):
        expected = {"kuaishou_user_profile": 10_000, "kuaishou_user_posts": 10_000,
                    "kuaishou_video_detail": 1_000, "kuaishou_video_statistics": 1_000}
        for operation, price in expected.items():
            with self.subTest(operation=operation):
                self.assertEqual(provider_budget.PRICES_MICROUSD[operation], price)
        self.assertEqual(providers._platform_price("kuaishou"), 0.01)
        for stage in ("detail", "metrics"):
            config = providers.STAGE_CONFIG[("kuaishou", stage)]
            self.assertEqual(config[3], 0.001)
            self.assertEqual(provider_budget.micro_usd(config[3]), expected[config[2]])


if __name__ == "__main__":
    unittest.main()
