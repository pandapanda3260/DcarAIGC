"""Current business reads agree; historical report contracts remain immutable."""
from __future__ import annotations

import copy
import csv
import json
import tempfile
import unittest
from pathlib import Path
from unittest.mock import patch

from tests import test_v8_source_routing as fixture
from tests import test_report_metric_validity as report_fixture
from tests import test_v8_audience_rate as audience_fixture
from v8 import audience_rate, insights, report_inputs, report_metric_validity as validity, reports
from v8.metric_source_policy import (CURRENT_METRIC_POLICY, OPERATION_FIELD_POLICY_VERSION,
    auto_collectable_fields, current_policy_binding, load_operation_field_policy)
from v8.provider_updates import missing_metric_fields
from v8.source_routing import METRIC_FIELDS, select_content_metrics, select_current_content_metrics
from v8.storage import connect, initialize_database, transaction


CUTOFF = report_fixture.CUTOFF
field = report_fixture.field


class CurrentMetricPolicyTest(unittest.TestCase):
    def setUp(self):
        self.fx = fixture.SourceRoutingTest(methodName="runTest")
        with patch.object(fixture, "initialize_database", side_effect=lambda c: initialize_database(c, target_version=23)):
            self.fx.setUp()
        self.addCleanup(self.fx.tearDown)
        self.c = self.fx.connection
        self.enterContext(patch("socket.socket.connect", side_effect=AssertionError("network forbidden")))

    def observe(self, operation, hour, *, cid=1, **values):
        at = f"2026-08-29T{hour:02d}:00:00Z"
        return self.fx.observe("TikHub", operation=operation, content_id=cid,
            captured_at=at, recorded_at=at, values=values)

    def test_current_statistics_and_gap_selector_agree_without_relabeling_legacy(self):
        self.observe("douyin_video_statistics", 1, view_count=1200, like_count=20)
        self.observe("douyin_video_detail", 2, view_count=0, like_count=0,
                     comment_count=1, share_count=2, collect_count=3)
        current = select_current_content_metrics(self.c, [1], cutoff_at=fixture.NOW)[1]
        explicit = select_content_metrics(self.c, [1], cutoff_at=fixture.NOW,
            policy_version=CURRENT_METRIC_POLICY)[1]
        self.assertEqual(current, explicit)
        self.assertEqual(current["view_count"], 1200)
        self.assertEqual(current["like_count"], 20)
        self.assertEqual(current["fields"]["view_count"]["effective_operation"], "douyin_video_statistics")
        self.assertEqual(missing_metric_fields(self.c, 1, at=fixture.NOW), [])
        self.assertEqual(current["fields"]["view_count"]["capability"], "supported")
        legacy = select_content_metrics(self.c, [1], cutoff_at=fixture.NOW,
            policy_version="source-routing-matrix-first-v2")[1]
        self.assertNotEqual(legacy["policy_version"], current["policy_version"])

    def test_current_decrease_zero_missing_and_late_fact_keep_exact_evidence(self):
        self.observe("douyin_video_statistics", 1, view_count=1200)
        self.observe("douyin_video_statistics", 2, view_count=0)
        exact = select_current_content_metrics(self.c, [1], cutoff_at=fixture.NOW)[1]["fields"]["view_count"]
        self.assertEqual(exact["value"], 0)
        self.observe("douyin_video_statistics", 3, view_count=None)
        latest = select_current_content_metrics(self.c, [1], cutoff_at=fixture.NOW)[1]["fields"]["view_count"]
        self.assertEqual((latest["value"], latest["captured_at"]), (0, exact["captured_at"]))
        self.assertEqual(latest["freshness"], "stale")
        self.assertEqual(select_current_content_metrics(self.c, [1], cutoff_at="2026-08-29T02:30:00Z")[1]["fields"]["view_count"], exact)

    def test_video_only_unsupported_views_never_remain_a_collectable_gap(self):
        with transaction(self.c):
            self.c.execute("UPDATE content_items SET platform='wechat_channels' WHERE id=1")
        original_raw = self.fx.raw
        with patch.object(self.fx, "raw", side_effect=lambda *args: original_raw(*args, stage="metrics")):
            self.observe("wechat_channels_video_statistics", 1, view_count=99,
                         like_count=0, comment_count=1, share_count=2, collect_count=3)
        current = select_current_content_metrics(self.c, [1], cutoff_at=fixture.NOW)[1]
        view = current["fields"]["view_count"]
        self.assertEqual((view["status"], view["capability"], view["value"]), ("unavailable", "unavailable", None))
        self.assertFalse(view["auto_collectable"])
        self.assertEqual(missing_metric_fields(self.c, 1, at=fixture.NOW), [])
        self.assertNotIn("view_count", auto_collectable_fields("wechat_channels"))
        legacy = select_content_metrics(self.c, [1], cutoff_at=fixture.NOW,
            policy_version=OPERATION_FIELD_POLICY_VERSION)[1]
        self.assertNotEqual(legacy["fields"]["view_count"]["status"], "unavailable")

    def test_partial_read_matches_full_projection_after_cached_current_write(self):
        self.observe("douyin_video_statistics", 1, view_count=1000, like_count=3)
        all_fields = select_current_content_metrics(self.c, [1], cutoff_at=fixture.NOW)[1]
        subset = select_current_content_metrics(self.c, [1], cutoff_at=fixture.NOW,
            metric_fields=("view_count", "like_count"))[1]
        self.assertEqual(set(subset["fields"]), {"view_count", "like_count"})
        for key in subset["fields"]:
            self.assertEqual(subset["fields"][key], all_fields["fields"][key])

    def test_alias_freeze_reverses_later_merge_and_excludes_later_identity(self):
        from v8.metric_field_facts import append_identity_merge
        with transaction(self.c):
            self.c.execute("UPDATE content_items SET platform='kuaishou' WHERE id IN (1,3)")
            for cid,value,created in ((1,'99887766',fixture.CAPTURE),(3,'3xeid',fixture.CAPTURE),(1,'3xlater','2026-08-29T06:00:00Z')):
                self.c.execute("INSERT INTO content_identities(content_id,identity_kind,identity_value,platform_identity_key,created_at) VALUES (?,'platform_content_id',?,?,?)",
                    (cid,value,'kuaishou:'+value,created))
            identities = [dict(row) for row in self.c.execute("SELECT * FROM content_identities")]
            append_identity_merge(self.c,winner_id=1,loser_id=3,recorded_at='2026-08-29T05:00:00Z',identity_snapshot={'identities':identities})
            self.c.execute("UPDATE content_identities SET content_id=1 WHERE content_id=3")
        before = [{'id':1,'platform':'kuaishou'},{'id':3,'platform':'kuaishou'}]
        frozen = report_inputs.freeze_content_identity_aliases(self.c,before,knowledge_at=fixture.NOW)
        self.assertEqual(before[0]['platform_content_id_aliases'],['99887766'])
        self.assertEqual(before[1]['platform_content_id_aliases'],['3xeid'])
        after = [{'id':1,'platform':'kuaishou'}]
        report_inputs.freeze_content_identity_aliases(self.c,after,knowledge_at='2026-08-29T05:30:00Z')
        self.assertEqual(after[0]['platform_content_id_aliases'],['3xeid','99887766'])
        self.assertNotIn('3xlater',[row['identity_value'] for row in frozen['rows']])


class CurrentReportPolicyTest(unittest.TestCase):
    def test_policy_binding_is_immutable_and_old_scope_keeps_its_policy(self):
        legacy = validity.scope_binding({"task_type": "daily"}, cutoff_at=CUTOFF, schema_version=22)
        before = copy.deepcopy(legacy)
        for task_type in ("daily", "weekly", "custom"):
            binding = validity.scope_binding({"task_type": task_type}, cutoff_at=CUTOFF, schema_version=23)
            self.assertEqual(validity.policy_for_scope({"cutoff_at": CUTOFF, "metric_validity": binding}), CURRENT_METRIC_POLICY)
            self.assertEqual(binding["source_policy_sha256"], current_policy_binding()["policy_sha256"])
            binding["field_capabilities"]["wechat_channels"]["view_count"]["status"] = "supported"
            with self.assertRaises(ValueError):
                validity.policy_for_scope({"cutoff_at": CUTOFF, "metric_validity": binding})
        self.assertEqual(legacy, before)
        self.assertEqual(validity.policy_for_scope({"cutoff_at": CUTOFF, "metric_validity": legacy}), OPERATION_FIELD_POLICY_VERSION)
        self.assertNotIn("field_capabilities", load_operation_field_policy())

    def test_supported_coverage_preserves_unavailable_business_exposure(self):
        selected = {name: field(0, policy_version=CURRENT_METRIC_POLICY) for name in METRIC_FIELDS}
        source = {1: {"fields": selected}, 999: {"fields": selected}}
        quality = validity.quality_detail([1, 1], source, platforms={1: "wechat_channels"},
            cutoff_at=CUTOFF, minimum_percentage=90, policy_version=CURRENT_METRIC_POLICY)
        self.assertEqual((quality["eligible_count"], quality["fresh_count"], quality["percentage"]), (1, 1, 100.0))
        exposure = quality["business_availability"]["view_count"]
        self.assertEqual((exposure["supported_count"], exposure["unavailable_count"], exposure["complete"]), (0, 1, False))
        displayed = validity.display_sources(None, platform="wechat_channels", cutoff_at=CUTOFF,
            policy_version=CURRENT_METRIC_POLICY)
        self.assertEqual(displayed["view_count"]["report_status"], "unavailable")

    def test_four_channel_csv_and_exposure_do_not_silently_drop_new_platforms(self):
        rows = [{"content_id": 1, "platform": "wechat_channels", "content_direction": "new_car",
                 "evidence_level": "V2", "selling_point_included": True, "primary_tier": "core",
                 "content_automotive_score": 90, "acquisition_potential_score": 88, "view_count": None}]
        channels = insights.build_channel_conclusions(rows, channels=insights.OVERVIEW_CHANNELS)
        metric = channels["wechat_channels"]["summary"]["metrics"]
        self.assertEqual(metric["selling_point_count_share"]["numerator"], 1)
        self.assertIsNone(metric["selling_point_exposure_share"]["percentage"])
        self.assertEqual(metric["selling_point_exposure_share"]["status"], "not_calculable")
        self.assertIsNone(metric["acquisition_potential"]["value"])
        self.assertEqual(metric["content_verticality"]["value"], 90)
        with tempfile.TemporaryDirectory() as directory:
            output = Path(directory) / "channels.csv"
            reports._write_channel_csv(output, channels)
            with output.open(encoding="utf-8-sig") as handle:
                exported = list(csv.DictReader(handle))
        self.assertEqual({row["platform"] for row in exported}, {platform for platform, _ in insights.OVERVIEW_CHANNELS})


class PlatformCalibrationTest(unittest.TestCase):
    def setUp(self):
        self.fx = audience_fixture.CalibrationGateTest(methodName="runTest")
        self.fx.setUp()
        self.addCleanup(self.fx.doCleanups)

    def test_two_platform_approval_cannot_approve_new_platform(self):
        self.fx._write(self.fx._record())
        self.assertEqual(audience_rate.active_classifier_state(None, record_path=self.fx.path), "approved")
        for platform in ("kuaishou", "wechat_channels"):
            self.assertEqual(audience_rate.active_classifier_state(None, record_path=self.fx.path, platform=platform), "rejected")

    def test_platform_approval_does_not_require_or_borrow_another_platform(self):
        record = self.fx._record()
        record["platforms"] = {"kuaishou": self.fx._platform(95, 4, 15, 386)}
        self.fx._write(record)
        self.assertEqual(audience_rate.active_classifier_state(None, record_path=self.fx.path, platform="kuaishou"), "approved")
        self.assertEqual(audience_rate.active_classifier_state(None, record_path=self.fx.path, platform="wechat_channels"), "rejected")


class CurrentReportIntegrationTest(unittest.TestCase):
    def setUp(self):
        self.fx = report_fixture.DailyMetricReportIntegrationTest(methodName="runTest")
        self.fx.setUp()
        self.addCleanup(self.fx.doCleanups)
        self.fx.observations()
        with connect(self.fx.fx.db) as connection:
            initialize_database(connection, target_version=23)

    def test_actual_report_freezes_v4_and_renders_all_four_channels(self):
        with connect(self.fx.fx.db) as connection:
            content_id = int(connection.execute("SELECT id FROM content_items ORDER BY id LIMIT 1").fetchone()[0])
            connection.execute("INSERT INTO content_identities(content_id,identity_kind,identity_value,platform_identity_key,created_at) VALUES (?,'platform_content_id','778899','douyin:778899','2026-09-11T23:00:00Z')",(content_id,))
            connection.commit()
        task = self.fx.fx.task(period="2026-09-11", at=CUTOFF, automatic=True)
        report = self.fx.fx.run_report(task, at=CUTOFF)
        self.assertEqual(set(report["channels"]), {platform for platform, _ in insights.OVERVIEW_CHANNELS})
        self.assertEqual(report["summary_metrics"]["view_count"]["value"], 1200)
        self.assertEqual(report["data_quality_details"]["metrics_freshness"]["percentage"], 100.0)
        self.assertEqual(report["content_details"][0]["metric_sources"]["view_count"]["report_status"], "fresh")
        with connect(self.fx.fx.db) as connection:
            scope = report_inputs.load_event(connection, task["id"], report_inputs.SCOPE_EVENT)["payload"]
        self.assertEqual(scope["metric_validity"]["source_policy_version"], CURRENT_METRIC_POLICY)
        self.assertEqual(scope["source_policy_sha256"], current_policy_binding()["policy_sha256"])
        self.assertEqual(scope["channels"], [list(item) for item in insights.OVERVIEW_CHANNELS])
        self.assertIn("视频号渠道", reports._markdown(report))
        self.assertIn("受支持指标", reports._markdown(report))
        self.assertIn('778899',report['content_details'][0]['platform_content_id_aliases'])
        with tempfile.TemporaryDirectory() as directory:
            path = Path(directory)/'content.csv'
            reports._write_csv(path,report['content_details'],new_classification=True)
            with path.open(encoding='utf-8-sig') as stream:
                exported = list(csv.DictReader(stream))
            self.assertEqual(json.loads(exported[0]['platform_content_id_aliases']),report['content_details'][0]['platform_content_id_aliases'])
        with connect(self.fx.fx.db) as connection:
            connection.execute("INSERT INTO content_identities(content_id,identity_kind,identity_value,platform_identity_key,created_at) VALUES (?,'platform_content_id','990011','douyin:990011','2026-09-12T23:00:00Z')",(content_id,))
            connection.commit()
        replay = self.fx.fx.run_report(task,at=CUTOFF)
        self.assertEqual(replay['content_details'],report['content_details'])
        self.assertNotIn('990011',replay['content_details'][0]['platform_content_id_aliases'])


if __name__ == "__main__":
    unittest.main()
