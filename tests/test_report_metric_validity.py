from __future__ import annotations

import copy
import csv
import tempfile
from pathlib import Path
import unittest
from unittest.mock import patch

from v8 import report_metric_validity as validity
from v8 import reports, report_inputs
from v8.source_routing import METRIC_FIELDS, POLICY_VERSION

CUTOFF = "2026-09-12T00:00:00Z"


def field(value=1000, **changes):
    result = dict(value=value, status="provided", freshness="fresh", is_latest_valid=True,
                  policy_version=validity.SOURCE_POLICY_VERSION, effective_provider="tikhub",
                  captured_at="2026-09-11T22:00:00Z", recorded_at="2026-09-11T22:01:00Z",
                  provider_data_at=None, expires_at="2026-09-12T22:00:00Z")
    result.update(changes)
    return result


def snapshot(**changes):
    fields = {name: field() for name in METRIC_FIELDS}
    fields.update(changes)
    return {"fields": fields}


class ReportMetricValidityTest(unittest.TestCase):
    def quality(self, snapshots, platforms=None):
        return reports._metric_freshness_detail(
            None, list(platforms or snapshots), cutoff_at=CUTOFF, minimum_percentage=90,
            latest_observations=snapshots, policy_version=validity.SOURCE_POLICY_VERSION,
            platforms=platforms or {key: "douyin" for key in snapshots},
        )

    def test_single_missing_metric_does_not_erase_others_or_shrink_denominator(self):
        source = {1: snapshot(share_count={"status": "missing", "value": None})}
        before = copy.deepcopy(source)
        result = self.quality(source, {1: "douyin", 2: "douyin"})
        self.assertEqual(result["eligible_count"], 2)
        self.assertEqual(result["fresh_count"], 0)
        self.assertEqual(result["fields"]["view_count"]["fresh_count"], 1)
        self.assertEqual(result["fields"]["share_count"]["missing_count"], 2)
        self.assertEqual(result["partial_content_count"], 1)
        self.assertIn("全部适用指标有效且未过期", result["reason"])
        self.assertEqual(source, before)

    def test_ninety_percent_boundary_unchanged(self):
        source = {key: snapshot() for key in range(10)}
        source[0] = snapshot(like_count={"status": "missing"})
        self.assertEqual(self.quality(source)["status"], "available")
        source[1] = snapshot(like_count={"status": "missing"})
        result = self.quality(source)
        self.assertEqual(result["percentage"], 80.0)
        self.assertEqual(result["status"], "below_threshold")

    def test_twenty_five_hour_value_is_not_fresh_even_within_thirty_six_hours(self):
        expired = field(captured_at="2026-09-10T23:00:00Z", recorded_at="2026-09-10T23:01:00Z",
                        expires_at="2026-09-11T23:00:00Z")
        self.assertEqual(validity.field_status(expired, cutoff_at=CUTOFF), "stale")
        self.assertEqual(expired["value"], 1000)

    def test_provider_data_timestamp_future_and_expiry_boundary(self):
        for changes in ({"provider_data_at": "2026-09-12T00:01:00Z"}, {"expires_at": CUTOFF}):
            with self.subTest(changes=changes):
                self.assertEqual(validity.field_status(field(**changes), cutoff_at=CUTOFF), "stale")

    def test_late_captured_or_late_recorded_value_never_passes(self):
        for key in ("captured_at", "recorded_at"):
            with self.subTest(key=key):
                self.assertEqual(validity.field_status(field(**{key: "2026-09-12T00:01:00Z"}),
                                                       cutoff_at=CUTOFF), "stale")

    def test_longer_ttl_does_not_bypass_report_thirty_six_hour_window(self):
        self.assertEqual(validity.field_status(field(captured_at="2026-09-10T11:59:59Z"),
                                               cutoff_at=CUTOFF), "stale")

    def test_real_zero_is_valid_but_missing_and_invalid_are_not_zero(self):
        self.assertEqual(validity.field_status(field(0), cutoff_at=CUTOFF), "fresh")
        for status in ("missing", "invalid"):
            self.assertEqual(validity.field_status({"status": status, "value": None}, cutoff_at=CUTOFF), status)
        self.assertEqual(validity.field_status(field(True), cutoff_at=CUTOFF), "invalid")

    def test_unsupported_xhs_exposure_only_is_excluded(self):
        result = self.quality({1: snapshot(view_count={"status": "not_applicable"})}, {1: "xiaohongshu"})
        self.assertEqual(result["fresh_count"], 1)
        self.assertEqual(result["fields"]["view_count"]["eligible_count"], 0)
        result = self.quality({1: snapshot(share_count={"status": "not_applicable"})})
        self.assertEqual(result["fields"]["share_count"]["eligible_count"], 1)
        self.assertEqual(result["fresh_count"], 0)

    def test_empty_scope_has_no_percentage(self):
        result = self.quality({})
        self.assertEqual(result["status"], "not_applicable")
        self.assertIsNone(result["percentage"])

    def test_daily_only_at_effective_boundary_old_scope_keeps_old_policy(self):
        self.assertIsNone(validity.scope_binding({"task_type": "daily"},
                          cutoff_at="2026-09-11T23:59:59Z", schema_version=21))
        for task_type, schema in (("weekly", 21), ("custom", 21), ("daily", 19)):
            self.assertIsNone(validity.scope_binding({"task_type": task_type}, cutoff_at=CUTOFF,
                                                    schema_version=schema))
        binding = validity.scope_binding({"task_type": "daily"}, cutoff_at=CUTOFF, schema_version=21)
        self.assertEqual(validity.policy_for_scope({"cutoff_at": CUTOFF}), "source-routing-matrix-first-v2")
        self.assertEqual(validity.scope_binding({"task_type": "daily"}, cutoff_at=CUTOFF,
                                               schema_version=22), binding)
        self.assertEqual(validity.policy_for_scope({"cutoff_at": CUTOFF, "metric_validity": binding}),
                         validity.SOURCE_POLICY_VERSION)
        binding["window_hours"] = 100
        with self.assertRaises(ValueError):
            validity.policy_for_scope({"cutoff_at": CUTOFF, "metric_validity": binding})

    def test_existing_frozen_report_is_returned_without_running_new_selector(self):
        report = {"metadata": {"collection_cutoff_at": CUTOFF},
                  "input_references": {"release_id": "release", "source_policy": {"policy_version": POLICY_VERSION}},
                  "data_quality": {"metrics_freshness": 0}}
        event = {"payload": report, "event_id": 1, "sha256": report_inputs.digest(report)}
        with patch.object(report_inputs, "frozen_report", return_value=event), \
             patch.object(reports, "_assemble_report_data", side_effect=AssertionError("old report reassembled")):
            result = reports._build_report_data(None, {"id": "old", "task_type": "daily", "creation_source": "automatic",
                                                       "period_end": "2026-09-11"}, release={"id": "release"},
                                                revision=2, generated_at="2026-09-13T00:00:00Z", files=[])
        self.assertEqual(result["data_quality"], report["data_quality"])
        self.assertEqual(result["input_references"], report["input_references"])

    def test_display_marks_old_and_missing_without_changing_numeric_values(self):
        source = snapshot(view_count=field(1200, freshness="stale", is_latest_valid=False), share_count={})
        result = validity.display_sources(source, platform="douyin", cutoff_at=CUTOFF)
        self.assertEqual(result["view_count"]["value"], 1200)
        self.assertEqual(result["view_count"]["captured_at"], source["fields"]["view_count"]["captured_at"])
        self.assertEqual(result["view_count"]["report_status"], "stale")
        self.assertEqual(result["share_count"]["report_status_label"], "未取到")

    def test_channel_exposure_with_stale_value_is_marked_without_changing_denominator(self):
        from v8.insights import build_channel_conclusions
        rows = [{"platform": "douyin", "view_count": 1000, "evidence_level": "V2",
                 "content_direction": "new_car", "selling_point_included": True, "primary_tier": "core",
                 "metric_sources": {"view_count": {"report_status": "stale"}}}]
        channels = build_channel_conclusions(rows)
        before = copy.deepcopy(channels)
        validity.mark_historical_exposure(channels, rows)
        metric = channels["douyin"]["summary"]["metrics"]["selling_point_exposure_share"]
        self.assertEqual(metric["status"], "stale")
        self.assertEqual(metric["denominator"], 1000)
        self.assertIn("旧数据", metric["reason"])
        self.assertEqual(channels["douyin"]["summary"]["metrics"]["selling_point_count_share"],
                         before["douyin"]["summary"]["metrics"]["selling_point_count_share"])

    def test_daily_csv_exposes_status_and_capture_time_for_each_metric(self):
        source = validity.display_sources(snapshot(view_count=field(1200, freshness="stale"), share_count={}),
                                         platform="douyin", cutoff_at=CUTOFF)
        with tempfile.TemporaryDirectory() as directory:
            path = Path(directory) / "details.csv"
            reports._write_csv(path, [{"view_count": 1200, "share_count": None, "metric_sources": source}])
            with path.open(encoding="utf-8-sig", newline="") as handle:
                rows = list(csv.DictReader(handle))
        self.assertEqual(rows[0]["view_count"], "1200")
        self.assertEqual(rows[0]["share_count"], "")
        self.assertEqual(rows[0]["播放量采集时间"], "2026-09-12 06:00:00 +0800")
        self.assertIn("旧数据", rows[0]["播放量状态"])
        self.assertEqual(rows[0]["分享数状态"], "未取到")


class DailyMetricReportIntegrationTest(unittest.TestCase):
    def setUp(self):
        from tests.test_v8_report_inputs import FrozenReportIntegrationTest
        from v8.storage import connect, initialize_database, transaction
        self.fx = FrozenReportIntegrationTest(methodName="runTest")
        self.fx.setUp()
        self.addCleanup(self.fx.doCleanups)
        with connect(self.fx.db) as connection:
            initialize_database(connection, target_version=21)
            with transaction(connection):
                connection.execute("UPDATE content_items SET published_at='2026-09-11T10:00:00Z' WHERE id=1")

    def observations(self, *, missing_share=False, extra_value=None, later_missing=False):
        from tests.test_v8_source_routing import SourceRoutingTest
        from v8.storage import connect
        source = SourceRoutingTest(methodName="runTest")
        with connect(self.fx.db) as connection:
            source.connection = connection
            for at, value in (("2026-09-11T20:00:00Z", 1000), ("2026-09-11T21:00:00Z", 1200)):
                source.observe("TikHub", operation="douyin_video_statistics", captured_at=at,
                               recorded_at=at, values={"view_count": value})
            source.observe("TikHub", operation="douyin_video_detail", captured_at="2026-09-11T22:00:00Z",
                           recorded_at="2026-09-11T22:00:00Z",
                           values={"view_count": 0, "share_count": None if missing_share else 5})
            if extra_value is not None:
                source.observe("TikHub", operation="douyin_video_statistics", captured_at="2026-09-11T23:00:00Z",
                               recorded_at="2026-09-11T23:00:00Z", values={"view_count": extra_value})
            if later_missing:
                source.observe("TikHub", operation="douyin_video_statistics", captured_at="2026-09-11T23:30:00Z",
                               recorded_at="2026-09-11T23:30:00Z", values={"view_count": None})

    def test_full_report_freezes_new_policy_and_replay_keeps_latest_effective_value(self):
        from v8.storage import connect
        from v8.source_routing import load_policy
        self.observations()
        task = self.fx.task(period="2026-09-11", at=CUTOFF, automatic=True)
        first = self.fx.run_report(task, at=CUTOFF)
        self.assertEqual(first["summary_metrics"]["view_count"]["value"], 1200)
        self.assertEqual(first["data_quality_details"]["metrics_freshness"]["percentage"], 100.0)
        self.assertEqual(first["content_details"][0]["metric_sources"]["view_count"]["report_status"], "fresh")
        with connect(self.fx.db) as connection:
            scope = report_inputs.load_event(connection, task["id"], report_inputs.SCOPE_EVENT)["payload"]
        self.assertEqual(scope["source_policy"], load_policy(policy_version=validity.SOURCE_POLICY_VERSION))
        self.assertEqual(scope["source_policy_sha256"], report_inputs.digest(scope["source_policy"]))
        reports.retry_task(task["id"], db_path=self.fx.db)
        with patch.object(reports, "_latest_metric_observations_at", side_effect=AssertionError("reselected frozen facts")):
            second = self.fx.run_report(task, at="2026-09-13T00:00:00Z")
        for name in ("input_references", "data_quality_details", "summary_metrics", "content_details", "frozen_inputs"):
            self.assertEqual(first[name], second[name], name)

    def test_missing_share_keeps_play_count_and_explains_partial_quality(self):
        self.observations(missing_share=True)
        task = self.fx.task(period="2026-09-11", at=CUTOFF, automatic=True)
        report = self.fx.run_report(task, at=CUTOFF)
        self.assertEqual(report["summary_metrics"]["view_count"]["value"], 1200)
        quality = report["data_quality_details"]["metrics_freshness"]
        self.assertEqual(quality["percentage"], 0.0)
        self.assertEqual(quality["fields"]["view_count"]["percentage"], 100.0)
        self.assertEqual(quality["fields"]["share_count"]["missing_count"], 1)
        detail = report["content_details"][0]
        self.assertIsNone(detail["share_count"])
        self.assertEqual(detail["metric_sources"]["share_count"]["report_status_label"], "未取到")
        markdown = reports._markdown(report)
        self.assertIn("全部指标有效且未过期的内容占比", markdown)
        self.assertIn("播放量：有效 1/1", markdown)
        self.assertIn("分享数：有效 0/1", markdown)

    def test_scope_only_retry_rejects_policy_drift(self):
        from v8.storage import connect, transaction
        from v8.source_routing import load_policy
        self.observations()
        task = self.fx.task(period="2026-09-11", at=CUTOFF, automatic=True)
        with connect(self.fx.db) as connection, transaction(connection):
            reports._snapshot_task_contents(connection, task)
            release = dict(reports.selected_active_release(connection))
            changed = load_policy(policy_version=validity.SOURCE_POLICY_VERSION)
            changed["content_recent_freshness_seconds"] = 999999
            with patch.object(reports, "load_policy", return_value=changed):
                with self.assertRaisesRegex(reports.ReportTaskError, "frozen report metric source policy changed"):
                    reports._build_report_data(connection, task, release=release, revision=1,
                                               generated_at=CUTOFF, files=[])

    def test_report_reads_actual_latest_value_and_later_failure_keeps_it_marked_old(self):
        self.observations(extra_value=98765, later_missing=True)
        task = self.fx.task(period="2026-09-11", at=CUTOFF, automatic=True)
        report = self.fx.run_report(task, at=CUTOFF)
        self.assertEqual(report["content_details"][0]["view_count"], 98765)
        self.assertEqual(report["summary_metrics"]["view_count"]["status"], "stale")
        source = report["content_details"][0]["metric_sources"]["view_count"]
        self.assertEqual(source["report_status"], "stale")
        self.assertEqual(source["captured_at"], "2026-09-11T23:00:00Z")


if __name__ == "__main__":
    unittest.main()
