"""Anonymous mixed resolver results retain the full Channels identity chain."""
from __future__ import annotations

import copy
import unittest

from v8 import platform_adapters as adapters

CHANNEL = "sphfixture123"
FINDER = "v2_" + "a" * 76 + "@finder"
VALUE = {"platform": "wechat_channels", "display_account_id": CHANNEL}


def payload(operation, params, data):
    return {"code": 200, "router": adapters.ROUTES[operation][1], "params": params, "data": data}


def official_item():
    return {"accTypeName": "公众号", "jumpInfo": {"userName": "gh_0123456789ab"}}


def finder_item(uid=FINDER):
    return {"accTypeName": "视频号", "jumpInfo": {"userName": uid}, "noticeParam": {"finderUsername": uid}}


def resolver(items):
    # Same group/subBoxes structure as the failure; all identifiers are synthetic.
    return payload("wechat_channels_resolve", {"channel_id": CHANNEL, "raw": True},
                   {"ret": 0, "data": [{"subBoxes": [{"items": [item]} for item in items]}]})


def entry(operation, raw_id, value):
    return {"operation": operation, "raw_response_id": raw_id, "payload": value}


class WechatMixedSearchTest(unittest.TestCase):
    def test_mixed_search_reuses_saved_resolver_and_requires_info_then_profile(self):
        raw = resolver([official_item(), finder_item()])
        self.assertEqual(adapters._finder_candidates(raw, CHANNEL), [FINDER])
        responses = [entry("wechat_channels_resolve", 1, raw)]
        info_request = adapters.next_profile_request(VALUE, responses)
        self.assertEqual(info_request["operation"], "wechat_channels_channel_info")
        self.assertEqual(info_request["params"]["username"], FINDER)
        info = payload(info_request["operation"], info_request["params"],
                       {"baseResponse": {"ret": 0}, "sections": [{"items": [
                           {"title": "Channels ID", "content": CHANNEL}]}]})
        responses.append(entry(info_request["operation"], 2, info))
        profile_request = adapters.next_profile_request(VALUE, responses)
        self.assertEqual(profile_request["operation"], "wechat_channels_user_profile")
        profile = payload(profile_request["operation"], profile_request["params"],
                          {"baseResponse": {"ret": 0}, "contact": {"username": FINDER, "nickname": "测试账号"}})
        responses.append(entry(profile_request["operation"], 3, profile))
        self.assertIsNone(adapters.next_profile_request(VALUE, responses))
        normalized = adapters.normalize_profile("wechat_channels", VALUE, profile, prior_responses=responses)
        self.assertEqual(normalized["source_raw_response_ids"], [1, 2, 3])
        self.assertEqual(normalized["reference_raw_response_ids"]["channel_id"], 2)

    def test_official_account_only_is_unresolved(self):
        with self.assertRaisesRegex(adapters.PlatformAdapterError, "identity_unresolved"):
            adapters._finder_candidates(resolver([official_item()]), CHANNEL)

    def test_unknown_invalid_or_contradictory_results_are_not_silently_skipped(self):
        bad_items = [
            {"accTypeName": "视频号", "jumpInfo": {"userName": "gh_0123456789ab"}},
            {"jumpInfo": {"userName": "gh_0123456789ab"}},
            {"accTypeName": "公众号", "jumpInfo": {"userName": "wxid_not_a_finder"}},
            {"accTypeName": "公众号", "jumpInfo": {"userName": "gh_invalid"}},
            {"accTypeName": "公众号", "jumpInfo": {"userName": "v2_not_hex@finder"}},
            {**official_item(), "noticeParam": {"finderUsername": FINDER}},
        ]
        for item in bad_items:
            with self.subTest(item=item), self.assertRaisesRegex(adapters.PlatformAdapterError, "invalid_finder_candidate"):
                adapters._finder_candidates(resolver([official_item(), item, finder_item()]), CHANNEL)
        raw = resolver([finder_item()])
        raw["data"]["username"] = "gh_0123456789ab"
        with self.assertRaisesRegex(adapters.PlatformAdapterError, "invalid_finder_candidate"):
            adapters._finder_candidates(raw, CHANNEL)

    def test_limit_and_request_binding_are_preserved(self):
        items = [official_item(), *[finder_item(f"v2_{index:08x}@finder") for index in range(11)]]
        with self.assertRaisesRegex(adapters.PlatformAdapterError, "finder_candidate_limit"):
            adapters._finder_candidates(resolver(items), CHANNEL)
        raw = resolver([official_item(), finder_item()])
        raw["params"]["channel_id"] = "sphdifferent"
        with self.assertRaisesRegex(adapters.PlatformAdapterError, "request_identity_mismatch"):
            adapters._finder_candidates(raw, CHANNEL)

    def test_mixed_search_does_not_replace_reverse_identity_evidence(self):
        responses = [entry("wechat_channels_resolve", 1, resolver([official_item(), finder_item()]))]
        request = adapters.next_profile_request(VALUE, responses)
        info = payload(request["operation"], request["params"],
                       {"baseResponse": {"ret": 0}, "sections": [{"items": [
                           {"title": "Channels ID", "content": "sphdifferent"}]}]})
        with self.assertRaisesRegex(adapters.PlatformAdapterError, "channel_id_not_matched"):
            adapters.next_profile_request(VALUE, [*responses, entry(request["operation"], 2, info)])
        empty = copy.deepcopy(info)
        empty["data"]["sections"] = []
        with self.assertRaisesRegex(adapters.PlatformAdapterError, "channel_id_evidence_missing"):
            adapters.next_profile_request(VALUE, [*responses, entry(request["operation"], 2, empty)])


if __name__ == "__main__":
    unittest.main()
