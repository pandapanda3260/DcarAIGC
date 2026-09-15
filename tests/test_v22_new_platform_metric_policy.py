"""Saved-contract parsers feed qualified fields, without provider I/O."""
from __future__ import annotations

import json
import unittest
from unittest.mock import patch

from tests import test_v8_source_routing as fixture
from tests.test_v8_new_platform_contracts import specimens
from v8 import kuaishou_adapter, wechat_channels_adapter
from v8.metric_observations import persist_metric_observation
from v8.metric_source_policy import field_eligibility
from v8.provider_updates import missing_metric_fields
from v8.source_routing import METRIC_FIELDS, OPERATION_FIELD_POLICY_VERSION, select_content_metrics
from v8.storage import initialize_database, transaction


class NewPlatformMetricPolicyTest(unittest.TestCase):
    def setUp(self):
        self.fx = fixture.SourceRoutingTest(methodName="runTest")
        with patch.object(fixture, "initialize_database", side_effect=lambda c: initialize_database(c, target_version=22)):
            self.fx.setUp()
        self.addCleanup(self.fx.tearDown)
        self.c = self.fx.connection
        self.c.execute("UPDATE content_items SET platform='kuaishou' WHERE id=1")
        self.c.execute("UPDATE content_items SET platform='wechat_channels' WHERE id=2")
        self.c.commit()
        self.enterContext(patch("socket.socket.connect", side_effect=AssertionError("network forbidden")))

    def metrics(self, platform, stage="metrics", overrides=None):
        uid, obj, _, page, detail = specimens(platform)
        adapter = kuaishou_adapter if platform == "kuaishou" else wechat_channels_adapter
        if stage == "discovery":
            row = page["data"]["feeds" if platform == "kuaishou" else "object"][0]
            row.update(overrides or {})
            return adapter.parse_discovery(page, uid)["items"][0]["metrics"]
        row = detail["data"]["photos" if platform == "kuaishou" else "objects"][0]
        row.update(overrides or {})
        parsed = adapter.parse_stage(stage, obj, detail, expected_uid=uid)
        return parsed if stage == "metrics" else parsed["metrics"]

    def observe(self, platform, operation, metrics, hour=1):
        cid = 1 if platform == "kuaishou" else 2
        at = f"2026-08-29T{hour:02d}:00:00Z"
        stage = "discovery" if operation.endswith(("user_posts", "discovery_metrics")) else "detail" if operation.endswith("video_detail") else "metrics"
        with transaction(self.c):
            raw = self.fx.raw("TikHub", at, cid, operation, stage=stage)
            return persist_metric_observation(self.c, content_id=cid, captured_at=at, recorded_at=at,
                window_key=at[:10], provider="TikHub", source="tikhub", raw_response_id=raw,
                status="available", observation_origin="provider_capture",
                metadata_json=json.dumps({"operation":operation, "fields":metrics["_field_status"]}),
                **{field:metrics.get(field) for field in METRIC_FIELDS})

    def selected(self, platform):
        cid = 1 if platform == "kuaishou" else 2
        return select_content_metrics(self.c, [cid], cutoff_at=fixture.NOW,
            policy_version=OPERATION_FIELD_POLICY_VERSION)[cid]["fields"]

    def test_statistics_and_detail_valid_zero_and_all_returned_fields_are_selected(self):
        for platform in ("kuaishou", "wechat_channels"):
            overrides = {"like_count":0} if platform == "kuaishou" else {"likeCount":0,"favCount":3,"forwardCount":2,"readCount":0}
            for hour, stage in enumerate(("metrics", "detail"), start=1):
                operation = platform + ("_video_statistics" if stage == "metrics" else "_video_detail")
                observation = self.observe(platform, operation, self.metrics(platform, stage, overrides), hour)
                fields = self.selected(platform)
                for name in METRIC_FIELDS:
                    if platform == "wechat_channels" and name == "view_count":
                        self.assertIsNone(fields[name]["value"])
                        self.assertNotEqual(fields[name]["status"], "not_applicable")
                        continue
                    self.assertEqual(fields[name]["status"], "provided", (platform,name,fields[name]))
                    self.assertEqual(fields[name]["freshness"], "fresh")
                    self.assertEqual(fields[name]["observation_id"], observation.observation_id)
                self.assertEqual(fields["like_count"]["value"], 0)
            self.assertEqual(missing_metric_fields(self.c, 1 if platform == "kuaishou" else 2, at=fixture.NOW),
                [])

    def test_discovery_complete_counts_qualify_but_short_display_counts_do_not(self):
        for platform in ("kuaishou", "wechat_channels"):
            overrides = {"like_count":"1.2万", "share_count":None,"collect_count":None} if platform == "kuaishou" else {"likeCount":"1.2万","forwardCount":None,"favCount":None}
            metrics = self.metrics(platform, "discovery", overrides)
            self.observe(platform, platform + "_user_posts", metrics)
            fields = self.selected(platform)
            self.assertIsNone(fields["like_count"]["value"])
            self.assertIsNone(fields["share_count"]["value"])
            self.assertIsNone(fields["collect_count"]["value"])
            self.assertEqual(fields["comment_count"]["status"], "provided")

    def test_same_source_missing_invalid_do_not_overwrite_valid_values_or_refresh_time(self):
        for platform in ("kuaishou", "wechat_channels"):
            operation = platform + "_video_statistics"
            initial = self.observe(platform, operation, self.metrics(platform))
            overrides = {"view_count":None,"like_count":"1.2万"} if platform == "kuaishou" else {"readCount":0,"likeCount":"1.2万"}
            self.observe(platform, operation, self.metrics(platform, overrides=overrides), hour=2)
            fields = self.selected(platform)
            self.assertEqual(fields["like_count"]["observation_id"], initial.observation_id)
            self.assertEqual(fields["like_count"]["captured_at"], "2026-08-29T01:00:00Z")
            self.assertEqual(fields["like_count"]["freshness"], "stale")
            if platform == "kuaishou":
                self.assertEqual(fields["view_count"]["value"], 12)
                self.assertEqual(fields["view_count"]["observation_id"], initial.observation_id)
                self.assertEqual(fields["view_count"]["captured_at"], "2026-08-29T01:00:00Z")

    def test_newest_valid_complete_discovery_wins_without_maximum_and_unlisted_stays_unqualified(self):
        for platform in ("kuaishou", "wechat_channels"):
            self.observe(platform, platform + "_video_statistics", self.metrics(platform))
            overrides = {"like_count":0} if platform == "kuaishou" else {"likeCount":0}
            observation = self.observe(platform, platform + "_user_posts", self.metrics(platform,"discovery",overrides), hour=2)
            self.assertEqual(self.selected(platform)["like_count"]["observation_id"], observation.observation_id)
            self.assertEqual(self.selected(platform)["like_count"]["value"], 0)
            self.assertEqual(field_eligibility(platform,"tikhub",platform+"_comments","like_count")[0], "historical_only")
            self.assertEqual(field_eligibility(platform,"tikhub",platform+"_discovery_metrics","like_count")[0], "historical_only")
        self.assertEqual(field_eligibility("wechat_channels","tikhub","wechat_channels_video_statistics","view_count")[0],"audit_only")


if __name__ == "__main__":
    unittest.main()
