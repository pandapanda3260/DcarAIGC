"""Offline supplier contract examples; no live capture is implied by these tests."""

from copy import deepcopy
import unittest

from v8.capture import CaptureError
from v8.wechat_channels_adapter import (
    parse_channel_info, parse_discovery, parse_profile, parse_reference,
    parse_stage, request_spec,
)


UID = "v2_0011aabb@finder"
OTHER_UID = "v2_0011aabc@finder"
OBJECT = "0014941130915890399732"
CHANNEL = "sphUnitTest"


def response(data, **params):
    return {"code": 200, "params": params, "data": data}


def video(**overrides):
    result = {
        "id": OBJECT, "username": UID, "contact": {"username": UID, "nickname": "测试账号"},
        "objectDesc": {"description": "作品", "media": [{
            "mediaType": 4,
            "url": "https://video.example.test/encrypted.mp4", "urlToken": "?token=abc+//==",
            "decodeKey": "18446744073709551615",
        }]},
        "createtime": 1700000000, "likeCount": "12", "commentCount": 0,
    }
    result.update(overrides)
    return result


class WeChatChannelsAdapterTests(unittest.TestCase):
    def test_all_requests_use_json_body_and_actual_v2_endpoints(self):
        subjects = {
            "resolve": CHANNEL, "channel_info": UID, "profile": UID,
            "account_metrics": UID, "discovery": UID, "detail": OBJECT,
            "metrics": OBJECT, "comments": OBJECT,
        }
        for stage, subject in subjects.items():
            with self.subTest(stage=stage):
                result = request_spec(stage, subject)
                self.assertEqual(result["method"], "POST")
                self.assertTrue(result["path"].startswith("/api/v1/wechat_channels/v2/"))
                self.assertNotIn("params", result)
                self.assertIs(result["body"]["raw"], True)

    def test_pagination_round_trips_base64_and_reply_context(self):
        cursor = {"last_buffer": "ab+//==", "comment_id": "0018446744073709551615"}
        original = deepcopy(cursor)
        result = request_spec("comments", OBJECT, cursor)
        self.assertEqual(result["body"]["last_buffer"], cursor["last_buffer"])
        self.assertEqual(result["body"]["comment_id"], cursor["comment_id"])
        self.assertEqual(cursor, original)
        self.assertEqual(result["body"]["object_id"], OBJECT)

    def test_uid_and_display_id_are_not_interchangeable(self):
        for stage, subject in (("discovery", CHANNEL), ("resolve", UID), ("metrics", 1.8446744073709552e19)):
            with self.subTest(stage=stage), self.assertRaises(CaptureError):
                request_spec(stage, subject)

    def test_unknown_or_cross_stage_cursor_context_is_rejected(self):
        for stage, cursor in (("discovery", {"comment_id": "9"}), ("detail", "YWJj"), ("comments", {"username": UID}), ("comments", 0)):
            with self.subTest(stage=stage), self.assertRaises(CaptureError):
                request_spec(stage, UID if stage == "discovery" else OBJECT, cursor)

    def test_profile_raw_and_simplified_return_same_follower_semantics(self):
        raw = response({"baseResponse": {"ret": 0}, "contact": {"username": UID, "nickname": "测试"}, "fansCount": "9007199254740993"})
        slim = response({"username": UID, "nickname": "测试", "fans_count": "9007199254740993"})
        for payload in (raw, slim):
            result = parse_profile(payload, UID)
            self.assertEqual(result["follower_count"], 9007199254740993)
            self.assertIsNone(result["metrics"]["platform_work_count"])

    def test_profile_unproven_fan_zero_is_not_an_authoritative_zero(self):
        for supplied, expected in (({}, None), ({"fansCount": 0}, None), ({"fansCount": False}, None), ({"fansCount": "1.2万"}, None)):
            with self.subTest(supplied=supplied):
                payload = response({"contact": {"username": UID, "nickname": "测试"}, **supplied})
                self.assertEqual(parse_profile(payload, UID)["follower_count"], expected)

    def test_outer_200_never_masks_inner_errors(self):
        cases = [
            {"baseResponse": {"ret": -1}}, {"baseResponse": {}},
            {"error": "invalid parameters"}, {"errcode": 400}, {"success": False},
            {"code": 500}, {"success": 0}, {"ret": 100001},
        ]
        for error in cases:
            with self.subTest(error=error), self.assertRaises(CaptureError):
                parse_profile(response({"username": UID, "nickname": "测试", **error}), UID)
        for code in ("200", True, 500):
            with self.subTest(code=code), self.assertRaises(CaptureError):
                parse_profile({"code": code, "data": {"username": UID, "nickname": "测试"}}, UID)

    def test_conflicting_author_fields_never_bind_even_with_matching_name(self):
        with self.assertRaises(CaptureError) as raised:
            parse_profile(response({"username": UID, "contact": {"username": OTHER_UID, "nickname": "测试"}}), UID)
        self.assertEqual(raised.exception.error_code, "identity_conflict")

    def test_reference_is_only_candidate_until_reverse_check(self):
        payload = response({"query": CHANNEL, "data": [{"items": [{"jumpInfo": {"userName": UID}}]}]})
        candidate = parse_reference(payload, CHANNEL)
        self.assertEqual(candidate["uid"], UID)
        self.assertFalse(candidate["identity_verified"])
        self.assertTrue(candidate["requires_channel_confirmation"])
        info = response({"baseResponse": {"ret": 0}, "sections": [{"items": [{"title": "视频号ID", "content": CHANNEL}]}]}, username=UID)
        self.assertTrue(parse_channel_info(info, UID, CHANNEL)["identity_verified"])

    def test_resolver_rejects_wrong_query_or_multiple_candidates(self):
        for data in (
            {"query": "sphDifferent", "username": UID},
            {"query": CHANNEL, "data": [{"items": [{"jumpInfo": {"userName": UID}}, {"jumpInfo": {"userName": OTHER_UID}}]}]},
        ):
            with self.subTest(data=data), self.assertRaises(CaptureError):
                parse_reference(response(data), CHANNEL)

    def test_reverse_check_requires_matching_both_public_and_finder_ids(self):
        data = {"finder_username": UID, "channel_id": CHANNEL}
        for expected_uid, expected_channel in ((OTHER_UID, CHANNEL), (UID, "sphOther")):
            with self.assertRaises(CaptureError):
                parse_channel_info(response(data), expected_uid, expected_channel)
        with self.assertRaises(CaptureError):
            parse_channel_info(response({"channel_id": CHANNEL}), UID, CHANNEL)

    def test_live_channel_info_english_id_label_keeps_exact_reverse_match(self):
        payload = response({"baseResponse": {"ret": 0}, "sections": [{"title": "基础信息", "items": [
            {"title": "IP归属地", "content": "北京"},
            {"title": "Channels ID", "content": CHANNEL},
        ]}]}, username=UID)
        self.assertEqual(parse_channel_info(payload, UID, CHANNEL)["channel_id"], CHANNEL)
        with self.assertRaises(CaptureError):
            parse_channel_info(payload, UID, "sphDifferent")
        payload["data"]["sections"][0]["items"].append({"title": "视频号ID", "content": "sphDifferent"})
        with self.assertRaises(CaptureError):
            parse_channel_info(payload, UID, CHANNEL)

    def test_discovery_preserves_opaque_buffer_and_large_identifier(self):
        payload = response({"baseResponse": {"ret": 0}, "contact": {"username": UID}, "object": [video()], "continueFlag": 1, "lastBuffer": "ab+//=="})
        result = parse_discovery(payload, UID)
        self.assertEqual(result["next_cursor"], "ab+//==")
        self.assertTrue(result["has_more"])
        item = result["items"][0]
        self.assertEqual(item["platform_content_id"], OBJECT)
        self.assertEqual(item["metrics"]["like_count"], 12)
        self.assertEqual(item["metrics"]["comment_count"], 0)
        self.assertIsNone(item["metrics"]["view_count"])

    def test_explicit_empty_page_is_distinct_from_missing_pagination(self):
        data = {"username": UID, "videos": [], "up_continue": False, "last_buffer": ""}
        self.assertEqual(parse_discovery(response(data), UID)["items"], [])
        del data["up_continue"]
        with self.assertRaises(CaptureError):
            parse_discovery(response(data), UID)

    def test_more_pages_without_cursor_cannot_look_exhausted(self):
        with self.assertRaises(CaptureError):
            parse_discovery(response({"username": UID, "videos": [], "up_continue": 1}), UID)

    def test_live_fifteen_item_first_pages_are_not_mistaken_for_exhaustion(self):
        for feeds_count in (617, 1123):
            with self.subTest(feeds_count=feeds_count):
                items = [video(id=str(14941130915890399732 + n)) for n in range(15)]
                payload = response({"baseResponse": {"ret": 0}, "contact": {"username": UID},
                                    "object": items, "continueFlag": 1, "upContinueFlag": 0,
                                    "feedsCount": feeds_count, "lastBuffer": "A" * 488, "upLastbuffer": ""})
                result = parse_discovery(payload, UID)
                self.assertEqual(len(result["items"]), 15)
                self.assertTrue(result["has_more"])
                self.assertEqual(request_spec("discovery", UID, result["next_cursor"])["body"]["last_buffer"], "A" * 488)

    def test_raw_completion_uses_backward_flag_regardless_of_upward_flag(self):
        data = {"contact": {"username": UID}, "object": [video()],
                "continueFlag": 0, "upContinueFlag": 1, "lastBuffer": "YWJj"}
        self.assertFalse(parse_discovery(response(data), UID)["has_more"])
        del data["continueFlag"]
        with self.assertRaises(CaptureError):
            parse_discovery(response(data), UID)

    def test_slim_up_flag_with_nonempty_cursor_does_not_prove_exhaustion(self):
        with self.assertRaises(CaptureError):
            parse_discovery(response({"username": UID, "videos": [video()], "up_continue": 0, "last_buffer": "YWJj"}), UID)

    def test_live_profile_placeholder_aggregate_zeros_are_retained_only_in_raw(self):
        for feeds in (617, 1123):
            data = {"contact": {"username": UID, "nickname": "测试"}, "fansCount": 0,
                    "feedsCount": feeds, "feedsLikeCount": 0, "feedsFavCount": 0, "feedsForwardCount": 0}
            payload = response(data)
            original = deepcopy(payload)
            result = parse_profile(payload, UID)
            self.assertEqual(result["metrics"]["platform_work_count"], feeds)
            for key in ("follower_count", "like_count", "collect_count", "share_count"):
                self.assertIsNone(result["metrics"][key])
                self.assertEqual(result["metrics"]["_field_status"][key]["reason"], "upstream_zero_not_authoritative")
            self.assertEqual(payload, original)

    def test_discovery_rejects_wrong_or_missing_author(self):
        for row in (video(username=OTHER_UID, contact={}), video(username=None, contact={})):
            with self.assertRaises(CaptureError):
                parse_discovery(response({"username": UID, "videos": [row], "up_continue": 0}), UID)

    def test_detail_and_metrics_verify_requested_object_and_author(self):
        payload = response({"baseResponse": {"ret": 0}, "objects": [video()]})
        self.assertEqual(parse_stage("detail", OBJECT, payload, UID)["account_uid"], UID)
        self.assertEqual(parse_stage("metrics", OBJECT, payload, UID)["like_count"], 12)
        for content_id, author in (("1", UID), (OBJECT, OTHER_UID)):
            with self.assertRaises(CaptureError):
                parse_stage("metrics", content_id, payload, author)

    def test_media_source_and_same_response_key_are_preserved_as_encrypted(self):
        result = parse_stage("detail", OBJECT, response({"objects": [video()]}), UID)
        self.assertEqual(result["media_urls"], [])
        self.assertEqual(result["media_processing_status"], "requires_decryption")
        entry = result["media_evidence"][0]
        self.assertEqual(entry["decode_key"], "18446744073709551615")
        self.assertEqual(entry["full_url"], "https://video.example.test/encrypted.mp4?token=abc+//==")

    def test_invalid_and_conflicting_metrics_are_not_rounded(self):
        item = video(readCount=1.2, likeCount=-1, commentCount=True, favCount="20", fav_count="21")
        metrics = parse_stage("metrics", OBJECT, response({"objects": [item]}))
        for field in ("view_count", "like_count", "comment_count", "collect_count"):
            self.assertIsNone(metrics[field])
            self.assertEqual(metrics["_field_status"][field]["status"], "invalid")

    def test_live_detail_read_zero_does_not_claim_zero_exposure(self):
        for counts in ((2, 1, 2, 1), (40, 0, 59, 4)):
            likes, comments, collects, shares = counts
            payload = response({"objects": [video(readCount=0, likeCount=likes, commentCount=comments, favCount=collects, forwardCount=shares)]})
            metrics = parse_stage("metrics", OBJECT, payload, UID)
            self.assertIsNone(metrics["view_count"])
            self.assertEqual(metrics["_field_status"]["view_count"]["reason"], "upstream_zero_not_authoritative")
            self.assertEqual([metrics[k] for k in ("like_count", "comment_count", "collect_count", "share_count")], list(counts))
        self.assertEqual(parse_stage("metrics", OBJECT, response({"objects": [video(readCount=123)]}))["view_count"], 123)

    def test_error_only_continuation_never_looks_like_empty_final_page(self):
        payload = response({"message": "请求参数可能有误，请检查参数后重试。", "debug_info": "opaque"}, username=UID, last_buffer="A" * 488)
        with self.assertRaises(CaptureError) as raised:
            parse_discovery(payload, UID)
        self.assertEqual(raised.exception.error_code, "invalid_response")

    def test_headerless_continuation_requires_exact_echo_and_each_author(self):
        payload = response({"baseResponse": {"ret": 0}, "object": [video()], "continueFlag": 1, "lastBuffer": "bmV4dA=="}, username=UID, last_buffer="cHJpb3I=")
        payload["router"] = "/api/v1/wechat_channels/v2/fetch_user_videos"
        self.assertTrue(parse_discovery(payload, UID)["has_more"])
        payload["data"]["object"] = [video(username=OTHER_UID, contact={})]
        with self.assertRaises(CaptureError):
            parse_discovery(payload, UID)
        payload["data"].update(object=[], continueFlag=0, lastBuffer="")
        self.assertFalse(parse_discovery(payload, UID)["has_more"])
        payload["params"]["username"] = OTHER_UID
        with self.assertRaises(CaptureError):
            parse_discovery(payload, UID)

    def test_raw_comments_preserve_context_and_do_not_use_page_count_as_total(self):
        payload = response({"baseResponse": {"ret": 0}, "commentInfo": [{"commentId": "18446744073709551615", "username": "comment-user", "content": " 正文 ", "likeCount": 0}], "downContinueFlag": 1, "lastBuffer": "YWJj+//==", "count": 1}, object_id=OBJECT, comment_id="0009")
        result = parse_stage("comments", OBJECT, payload)
        self.assertIsNone(result["declared_total"])
        self.assertEqual(result["next_cursor_params"], {"last_buffer": "YWJj+//==", "comment_id": "0009"})
        self.assertEqual(result["comments"][0]["parent_comment_id"], "0009")
        self.assertEqual(result["comments"][0]["raw_user_id"], "comment-user")

    def test_comment_object_mismatch_is_rejected(self):
        with self.assertRaises(CaptureError):
            parse_stage("comments", OBJECT, response({"object_id": "3", "comments": [], "down_continue": 0}))

    def test_comment_request_binding_and_reply_context_cannot_disagree(self):
        with self.assertRaises(CaptureError):
            parse_stage("comments", OBJECT, response({"object_id": OBJECT, "comments": [], "down_continue": 0}, object_id="3"))
        with self.assertRaises(CaptureError):
            parse_stage("comments", OBJECT, response({"object_id": OBJECT, "comment_id": "8", "comments": [], "down_continue": 0}, object_id=OBJECT, comment_id="9"))


if __name__ == "__main__":
    unittest.main()
