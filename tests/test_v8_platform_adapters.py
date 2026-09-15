from __future__ import annotations

import copy
import unittest
from unittest.mock import patch

from v8 import platform_adapters as a
from v8 import kuaishou_adapter, wechat_channels_adapter


def response(operation, params, data):
    return {"code": 200, "router": a.ROUTES[operation][1], "params": params, "data": data}


class PlatformAdaptersTest(unittest.TestCase):
    def test_ids_stay_text_and_aliases_are_separate(self):
        self.assertEqual(a.normalize_account_input({"platform": "kuaishou", "uid": "000123"})["uid"], "000123")
        value = a.normalize_account_input({"platform": "kuaishou", "uid": "3xabcde123"})
        self.assertIsNone(value["uid"])
        self.assertEqual(value["references"], {"eid": "3xabcde123"})
        value = a.normalize_account_input({"platform": "wechat_channels", "uid": "sphabc123"})
        self.assertIsNone(value["uid"])
        with self.assertRaises(a.PlatformAdapterError):
            a.normalize_account_input({"platform": "douyin", "uid": 123456})

    def test_url_conflict_and_unofficial_host(self):
        for value in ({"platform": "douyin", "uid": "123456", "profile_url": "https://www.douyin.com/user/234567"},
                      {"platform": "kuaishou", "profile_url": "https://evil.test/profile/123456"},
                      {"platform": "douyin", "profile_url": "https://www.douyin.com@evil.test/user/123456"}):
            with self.assertRaises(a.PlatformAdapterError):
                a.normalize_account_input(value)
        self.assertEqual(a.normalize_account_input({"profile_url": "https://www.douyin.com/user/123456"})["platform"], "douyin")
        with self.assertRaises(a.PlatformAdapterError):
            a.normalize_account_input({"profile_url": "https://www.douyin.com/user/MS4wLjAB" + "a" * 40,
                "references": {"sec_user_id": "MS4wLjAB" + "b" * 40}})

    def test_official_shortlink_preprocessing_and_explicit_unsupported_share(self):
        value = a.normalize_submission_input({"profile_url": "https://v.douyin.com/abc/"},
            expand_url=lambda url: "https://www.douyin.com/user/123456")
        self.assertEqual(value["uid"], "123456")
        with self.assertRaisesRegex(a.PlatformAdapterError, "短链"):
            a.normalize_account_input({"profile_url": "https://v.kuaishou.com/abc/"})
        with self.assertRaisesRegex(a.PlatformAdapterError, "作品分享"):
            a.normalize_account_input({"profile_url": "https://weixin.qq.com/sph/abc"})
        with self.assertRaises(a.PlatformAdapterError):
            a.normalize_submission_input({"profile_url": "https://v.douyin.com/abc/"},
                expand_url=lambda url: "https://www.kuaishou.com/profile/123456")

    def test_xhs_recurring_provider_profile_uses_profile_route(self):
        from v8 import providers
        uid = "0123456789abcdef01234567"
        payload = response("xiaohongshu_user_profile", {"user_id": uid}, {"code": 0, "success": True,
            "data": {"userid": uid, "nickname": "示例", "red_id": "12345", "fans": 7, "result": {"code": 0, "success": True}}})
        self.assertEqual(providers.PROFILE_OPERATIONS["xiaohongshu"], "xiaohongshu_user_profile")
        self.assertEqual(providers._content_request_params("xiaohongshu", "profile", uid, ""), {"user_id": uid})
        result = providers._extra_parse("xiaohongshu", "profile", uid, payload)
        self.assertEqual(result.data["metrics"]["follower_count"], 7)

    def test_douyin_identity_alias_conflict(self):
        value = {"platform": "douyin", "uid": "123456"}
        payload = response("douyin_uid_profile", {"uid": "123456"}, {"status_code": 0, "data": {
            "id_str": "123456", "uid": "123456", "nickname": "示例", "sec_uid": "MS4wLjAB" + "a" * 40}})
        self.assertEqual(a.normalize_profile("douyin", value, payload)["uid"], "123456")
        payload["data"]["data"]["uid"] = "234567"
        with self.assertRaises(a.PlatformAdapterError):
            a.normalize_profile("douyin", value, payload)

    def test_kuaishou_profile_no_nickname_mapping(self):
        payload = response("kuaishou_user_profile", {"user_id": "123456"}, {"result": 1, "userProfile": {
            "profile": {"user_id": 123456, "user_name": "示例"}, "ownerCount": {"fan": 0}}})
        profile = a.normalize_profile("kuaishou", {"uid": "123456"}, payload)
        self.assertEqual(profile["metrics"]["follower_count"], 0)
        self.assertEqual(profile["display_account_id"], "")
        payload["params"]["user_id"] = "3xabcde123"
        with self.assertRaisesRegex(a.PlatformAdapterError, "eid_mapping_evidence_missing"):
            a.normalize_profile("kuaishou", {"references": {"eid": "3xabcde123"}}, payload)

    def test_wechat_requires_reverse_channel_evidence(self):
        finder = "v2_0123456789abcdef@finder"
        value = {"platform": "wechat_channels", "display_account_id": "sphabc123"}
        first = a.next_profile_request(value)
        self.assertEqual(first["operation"], "wechat_channels_resolve")
        resolved = {"operation": first["operation"], "raw_response_id": 1, "payload": response(first["operation"], first["params"], {"channel_id": "sphabc123", "username": finder})}
        info_req = a.next_profile_request(value, [resolved])
        info = {"operation": info_req["operation"], "raw_response_id": 2, "payload": response(info_req["operation"], info_req["params"],
            {"baseResponse": {"ret": 0}, "sections": [{"items": [{"title": "视频号ID", "content": "sphabc123"}]}]})}
        profile_req = a.next_profile_request(value, [resolved, info])
        final = {"operation": profile_req["operation"], "raw_response_id": 3, "payload": response(profile_req["operation"], profile_req["params"],
            {"baseResponse": {"ret": 0}, "contact": {"username": finder, "nickname": "示例"}, "fansCount": 0})}
        profile = a.normalize_profile("wechat_channels", value, final["payload"], prior_responses=[resolved, info, final])
        self.assertEqual(profile["uid"], finder)
        self.assertIsNone(profile["metrics"]["follower_count"])
        self.assertEqual(profile["reference_raw_response_ids"]["channel_id"], 2)
        self.assertIsNone(a.next_profile_request(value, [resolved, info, final]))
        info["payload"]["data"]["sections"][0]["items"][0]["content"] = "sphother"
        with self.assertRaises(a.PlatformAdapterError):
            a.next_profile_request(value, [resolved, info])

    def test_wechat_http_200_inner_failure(self):
        value = {"platform": "wechat_channels", "display_account_id": "sphabc123"}
        entry = {"operation": "wechat_channels_resolve", "raw_response_id": 1,
                 "payload": {"code": 200, "data": {"ret": 100001, "msg": "failure"}}}
        with self.assertRaisesRegex(a.PlatformAdapterError, "provider_business_failure"):
            a.next_profile_request(value, [entry])

    def test_kuaishou_eid_requires_numeric_confirmation(self):
        value = {"platform": "kuaishou", "references": {"eid": "3xabcde123"}}
        first = response("kuaishou_user_profile", {"user_id": "3xabcde123"}, {"result": 1,
            "userProfile": {"profile": {"user_id": 123456, "user_name": "Same name"}}})
        responses = [{"operation": "kuaishou_user_profile", "payload": first, "raw_response_id": 1}]
        self.assertEqual(a.next_profile_request(value, responses)["params"]["user_id"], "123456")
        second = copy.deepcopy(first)
        second["params"]["user_id"] = "123456"
        responses.append({"operation": "kuaishou_user_profile", "payload": second, "raw_response_id": 2})
        self.assertIsNone(a.next_profile_request(value, responses))
        profile = a.normalize_profile("kuaishou", value, second, prior_responses=responses)
        self.assertEqual(profile["reference_raw_response_ids"]["eid"], 1)
        second["data"]["userProfile"]["profile"]["user_id"] = 987654
        with self.assertRaises(a.PlatformAdapterError):
            a.next_profile_request(value, responses)

    def test_wechat_checks_later_candidate_without_using_name(self):
        uid1, uid2 = "v2_aaaaaaaa@finder", "v2_bbbbbbbb@finder"
        value = {"platform": "wechat_channels", "display_account_id": "sphwanted"}
        convert = response("wechat_channels_resolve", {"channel_id": "sphwanted", "raw": True},
            {"data": [{"items": [{"jumpInfo": {"userName": uid1}}, {"jumpInfo": {"userName": uid2}}]}]})
        responses = [{"operation": "wechat_channels_resolve", "raw_response_id": 1, "payload": convert}]
        self.assertEqual(a.next_profile_request(value, responses)["params"]["username"], uid1)
        info = response("wechat_channels_channel_info", {"username": uid1, "raw": True},
            {"baseResponse": {"ret": 0}, "sections": [{"items": [{"title": "Channels ID", "content": "sphother"}]}]})
        responses.append({"operation": "wechat_channels_channel_info", "raw_response_id": 2, "payload": info})
        self.assertEqual(a.next_profile_request(value, responses)["params"]["username"], uid2)

    def test_xhs_display_search_requires_red_id_and_uid_both_match(self):
        uid = "0123456789abcdef01234567"
        value = {"platform": "xiaohongshu", "display_account_id": "0012345"}
        spec = a.next_profile_request(value)
        self.assertEqual(spec["operation"], "xiaohongshu_user_search")
        search = response(spec["operation"], spec["params"], {"success": True, "code": 0,
            "data": {"users": [{"id": uid, "red_id": "0012345", "name": "Same name"}]}})
        responses = [{"operation": "xiaohongshu_user_search", "raw_response_id": 1, "payload": search}]
        spec = a.next_profile_request(value, responses)
        self.assertEqual(spec["params"], {"user_id": uid})
        profile = response(spec["operation"], spec["params"], {"success": True, "code": 0,
            "data": {"userid": uid, "red_id": "0012345", "nickname": "Another name", "fans": 7, "result": {"success": True, "code": 0}}})
        responses.append({"operation": "xiaohongshu_user_profile", "raw_response_id": 2, "payload": profile})
        self.assertIsNone(a.next_profile_request(value, responses))
        profile["data"]["data"]["red_id"] = "wrong"
        with self.assertRaises(a.PlatformAdapterError):
            a.next_profile_request(value, responses)

    def test_xhs_shortlink_does_not_override_supplied_display_identity(self):
        uid = "0123456789abcdef01234567"
        value = {"platform": "xiaohongshu", "profile_url": "https://xhslink.com/abc", "display_account_id": "oldhandle"}
        payload = response("xiaohongshu_user_profile", {"share_text": value["profile_url"]}, {"code": 0, "success": True,
            "data": {"userid": uid, "nickname": "示例", "red_id": "newhandle", "fans": 7, "result": {"code": 0, "success": True}}})
        with self.assertRaisesRegex(a.PlatformAdapterError, "identity_conflict"):
            a.normalize_profile("xiaohongshu", value, payload)
        # An explicit canonical UID can establish a legitimate handle change;
        # the supplied previous value remains in the preparation evidence.
        payload["params"] = {"user_id": uid}
        profile = a.normalize_profile("xiaohongshu", {**value, "uid": uid}, payload)
        self.assertEqual(profile["metadata"]["display_account_id_change"]["previous"], "oldhandle")

    def test_kuaishou_pages_strict_author_cursor_and_long_content_id(self):
        content_id = "14941130915890399732"
        payload = response("kuaishou_user_posts", {"user_id": "123456"}, {"result": 1, "pcursor": "nextopaque", "feeds": [
            {"photo_id": int(content_id), "timestamp": 1789200000000, "caption": "测试", "user_id": 123456}]})
        parsed = kuaishou_adapter.parse_discovery(payload, "123456")
        self.assertEqual(parsed["items"][0]["platform_content_id"], content_id)
        self.assertTrue(parsed["has_more"])
        self.assertIsNone(parsed["items"][0]["metrics"]["view_count"])
        wrong = copy.deepcopy(payload)
        wrong["data"]["feeds"][0]["user_id"] = 654321
        with self.assertRaises(Exception):
            kuaishou_adapter.parse_discovery(wrong, "123456")
        del payload["data"]["pcursor"]
        with self.assertRaises(Exception):
            kuaishou_adapter.parse_discovery(payload, "123456")

    def test_wechat_raw_content_has_exact_author(self):
        uid = "v2_0123456789abcdef@finder"
        data = {"baseResponse": {"ret": 0}, "contact": {"username": uid}, "object": [{"id": 14941130915890399732,
            "username": uid, "createTime": 1789200000, "objectDesc": {"description": "测试"}, "likeCount": 3}],
            "upContinueFlag": 0, "continueFlag": 0, "lastBuffer": ""}
        page = wechat_channels_adapter.parse_discovery({"code": 200, "data": data}, uid)
        self.assertFalse(page["has_more"])
        self.assertEqual(page["items"][0]["platform_content_id"], "14941130915890399732")
        self.assertEqual(page["items"][0]["metrics"]["like_count"], 3)


if __name__ == "__main__":
    unittest.main()
