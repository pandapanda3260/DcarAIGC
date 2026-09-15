"""The opt-in report policy uses real schema21 facts, never provider calls."""
from __future__ import annotations

import hashlib
import json
import unittest
from unittest.mock import patch

from tests import test_v8_source_routing as fixture
from v8.metric_field_facts import business_projection, project_content, record_correction, record_policy_transition, select_field_facts
from v8.source_routing import OPERATION_FIELD_POLICY_VERSION as V3, POLICY_VERSION, load_policy, select_content_metrics
from v8.storage import initialize_database, transaction


class OperationFieldPolicyTest(unittest.TestCase):
    def setUp(self):
        self.fx = fixture.SourceRoutingTest(methodName="runTest")
        with patch.object(fixture, "initialize_database", side_effect=lambda c: initialize_database(c, target_version=21)):
            self.fx.setUp()
        self.addCleanup(self.fx.tearDown)
        self.c = self.fx.connection
        self.assertEqual(self.c.execute("PRAGMA user_version").fetchone()[0], 21)

    def observe(self, operation, hour, *, values=None, content_id=1, recorded_at=None, metadata=None, provider="TikHub"):
        at = f"2026-08-29T{hour:02d}:00:00Z"
        return self.fx.observe(provider, operation=operation, captured_at=at, recorded_at=recorded_at or at,
                               values=values, content_id=content_id, metadata=metadata)

    def selected(self, *, cutoff="2026-08-29T04:00:00Z", policy=V3, content_id=1):
        return select_content_metrics(self.c, [content_id], cutoff_at=cutoff, policy_version=policy)[content_id]

    def field(self, name="view_count", **kwargs):
        return self.selected(**kwargs)["fields"][name]

    def test_v3_recovers_statistics_while_default_v2_and_facts_are_unchanged(self):
        statistics = self.observe("douyin_video_statistics", 1, values={"view_count": 1000})
        self.observe("douyin_video_detail", 2, values={"view_count": 0})
        default_before = self.fx.selected()
        facts_before = [tuple(row) for row in self.c.execute("SELECT * FROM content_metric_field_facts ORDER BY id")]
        changes_before = self.c.total_changes
        field = self.field()
        self.assertEqual((field["value"], field["observation_id"], field["freshness"]), (1000, statistics.observation_id, "fresh"))
        self.assertTrue(field["is_latest_valid"])
        self.assertEqual(field["source_eligibility"], "active")
        self.assertEqual(field["expires_at"], "2026-08-30T01:00:00Z")
        self.assertEqual(self.fx.selected(), default_before)
        self.assertEqual(default_before["fields"]["view_count"]["freshness"], "stale")
        self.assertNotIn("source_eligibility", default_before["fields"]["view_count"])
        self.assertEqual([tuple(row) for row in self.c.execute("SELECT * FROM content_metric_field_facts ORDER BY id")], facts_before)
        self.assertEqual(self.c.total_changes, changes_before)

    def test_new_values_increases_decreases_and_true_zero_are_used_without_maximum(self):
        for hour, value in enumerate((1000, 1200, 1180, 0), start=0):
            result = self.observe("douyin_video_statistics", hour, values={"view_count": value})
            field = self.field(cutoff=f"2026-08-29T{hour:02d}:30:00Z")
            self.assertEqual((field["value"], field["status"], field["freshness"]), (value, "provided", "fresh"))
            self.assertEqual(field["observation_id"], result.observation_id)
            self.assertTrue(field["is_latest_valid"])

    def test_later_statistics_missing_and_invalid_invalidate_only_own_old_value(self):
        for cid, value, status in ((1, None, "missing"), (3, -1, "invalid")):
            with self.subTest(status=status):
                old = self.observe("douyin_video_statistics", 1, content_id=cid, values={"view_count": 1000})
                self.observe("douyin_video_statistics", 2, content_id=cid, values={"view_count": value})
                field = self.field(content_id=cid)
                self.assertEqual((field["value"], field["observation_id"], field["freshness"], field["latest_provider_status"]), (1000, old.observation_id, "stale", status))
                self.assertFalse(field["is_latest_valid"])
                self.assertEqual(field["captured_at"], "2026-08-29T01:00:00Z")

    def test_unqualified_positive_detail_view_and_like_do_not_hide_statistics_failure(self):
        self.observe("douyin_video_statistics", 1, values={"view_count": 1000})
        self.observe("douyin_video_statistics", 2, values={"view_count": None, "like_count": None})
        self.observe("douyin_video_detail", 3, values={"view_count": 9999, "like_count": 999})
        self.assertEqual(self.field()["value"], 1000)
        self.assertFalse(self.field()["is_latest_valid"])
        self.assertEqual(self.field("like_count")["value"], 20)
        self.assertFalse(self.field("like_count")["is_latest_valid"])

    def test_statistics_share_number_missing_and_invalid_never_affect_detail(self):
        for cid, value in ((1, 77), (2, None), (3, -1)):
            # Three independent times on one Douyin content also prove repeated
            # contract-ineligible events cannot renew or invalidate the detail.
            if cid == 1:
                detail = self.observe("douyin_video_detail", 0, values={"share_count": 5})
            self.observe("douyin_video_statistics", cid, values={"share_count": value})
            field = self.field("share_count")
            self.assertEqual((field["value"], field["observation_id"], field["captured_at"], field["freshness"]), (5, detail.observation_id, "2026-08-29T00:00:00Z", "fresh"))
            self.assertTrue(field["is_latest_valid"])

    def test_statistics_share_alone_remains_audit_only_even_when_empty(self):
        self.observe("douyin_video_statistics", 1, values={"share_count": None})
        field = self.field("share_count")
        self.assertIsNone(field["value"])
        self.assertEqual(field["status"], "audit_only")
        self.assertFalse(field["is_latest_valid"])

    def test_newest_qualified_source_wins_and_decreases_are_not_replaced_by_maximum(self):
        self.observe("douyin_video_detail", 0, values={"comment_count": 5, "collect_count": 2})
        self.assertEqual(self.field("comment_count", cutoff="2026-08-29T00:30:00Z")["value"], 5)
        discovery = self.observe("douyin_user_posts", 1, values={"comment_count": 30, "collect_count": None, "view_count": 0})
        field = self.field("comment_count")
        self.assertEqual((field["value"], field["observation_id"]), (30, discovery.observation_id))
        self.assertEqual(self.field("collect_count")["freshness"], "fresh")
        self.observe("douyin_video_detail", 2, values={"comment_count": None, "collect_count": None})
        fallback = self.field("comment_count")
        self.assertEqual((fallback["value"], fallback["observation_id"], fallback["freshness"]), (30, discovery.observation_id, "fresh"))
        self.assertEqual(self.field("collect_count")["freshness"], "stale")
        self.assertFalse(self.field("collect_count")["is_latest_valid"])
        decreased = self.observe("douyin_video_detail", 3, values={"comment_count": 20})
        self.assertEqual((self.field("comment_count")["value"], self.field("comment_count")["observation_id"]), (20, decreased.observation_id))

    def test_missing_single_field_does_not_discard_other_fields_or_become_zero(self):
        self.observe("douyin_video_statistics", 1, values={"view_count": 1000})
        self.observe("douyin_video_detail", 2, values={"comment_count": None})
        fields = self.selected()["fields"]
        self.assertEqual((fields["view_count"]["value"], fields["view_count"]["freshness"]), (1000, "fresh"))
        self.assertIsNone(fields["comment_count"]["value"])
        self.assertEqual(fields["comment_count"]["status"], "missing")
        self.assertFalse(fields["comment_count"]["is_latest_valid"])

    def test_retired_matrix_preserves_historical_value_without_fresh_qualification(self):
        self.observe("matrix_works_list", 1, provider="newrank_matrix", values={"view_count": 8888})
        field = self.field()
        self.assertEqual(field["value"], 8888)
        self.assertEqual((field["freshness"], field["source_eligibility"]), ("stale", "historical_only"))
        self.assertFalse(field["is_latest_valid"])
        self.observe("douyin_video_statistics", 2, values={"view_count": 12})
        self.assertEqual(self.field()["value"], 12)
        self.assertEqual(self.field()["effective_provider"], "tikhub")

    def test_unknown_policy_cannot_implicitly_qualify_sources(self):
        self.observe("douyin_video_statistics", 1)
        self.assertEqual(self.field(policy="unconfigured-policy-v999")["freshness"], "stale")

    def test_expired_fact_is_not_renewed_by_later_other_operation(self):
        self.observe("douyin_video_statistics", 1, values={"view_count": 1000})
        self.observe("douyin_video_detail", 3, values={"view_count": 0})
        field = self.field(cutoff="2026-08-30T03:00:00Z")
        self.assertEqual((field["value"], field["freshness"], field["captured_at"]), (1000, "stale", "2026-08-29T01:00:00Z"))
        self.assertEqual(field["expires_at"], "2026-08-30T01:00:00Z")
        self.assertTrue(field["is_latest_valid"])

    def test_provider_data_time_controls_expiry_not_later_capture_time(self):
        self.observe("douyin_video_statistics", 1, metadata={"provider_data_at": "2026-08-28T00:00:00Z"})
        field = self.field()
        self.assertEqual(field["freshness"], "stale")
        self.assertEqual(field["provider_data_at"], "2026-08-28T00:00:00Z")
        self.assertEqual(field["expires_at"], "2026-08-29T00:00:00Z")

    def test_late_recording_and_future_capture_do_not_change_old_cutoff(self):
        self.observe("douyin_video_statistics", 1, values={"view_count": 1000})
        before = self.field()
        self.observe("douyin_video_statistics", 2, recorded_at="2026-08-29T05:00:00Z", values={"view_count": None})
        self.observe("douyin_video_statistics", 6, values={"view_count": 2000})
        self.assertEqual(self.field(), before)
        self.assertFalse(self.field(cutoff="2026-08-29T05:30:00Z")["is_latest_valid"])
        self.assertEqual(self.field(cutoff="2026-08-29T06:30:00Z")["value"], 2000)

    def test_targeted_correction_remains_effective_at_knowledge_boundary(self):
        self.observe("douyin_video_statistics", 1)
        before = self.field(cutoff="2026-08-29T01:30:00Z")
        with transaction(self.c):
            record_correction(self.c, before["selected_fact_id"], action="invalidate", rule_id="offline-test",
                              recorded_at="2026-08-29T02:00:00Z")
        self.assertEqual(self.field(cutoff="2026-08-29T01:30:00Z"), before)
        self.assertIsNone(self.field()["value"])
        self.assertEqual(self.field()["status"], "invalid")
        self.assertFalse(self.field()["is_latest_valid"])

    def test_cached_v2_projection_cannot_be_used_by_explicit_v3(self):
        self.observe("douyin_video_statistics", 1)
        self.observe("douyin_video_detail", 2, values={"view_count": 0})
        with transaction(self.c):
            stored = project_content(self.c, 1, cutoff_at="2026-08-29T02:30:00Z", policy_version=V3)
        self.assertEqual(stored["business_projection"]["fields"]["view_count"]["freshness"], "fresh")
        self.assertEqual(self.field(cutoff="2026-08-29T02:30:00Z")["freshness"], "fresh")
        self.assertEqual(self.fx.selected(cutoff="2026-08-29T02:30:00Z")["fields"]["view_count"]["freshness"], "stale")
        self.observe("douyin_video_statistics", 3, values={"view_count": None})
        self.assertFalse(self.field()["is_latest_valid"])

    def test_transition_may_close_active_source_but_cannot_promote_audit_share(self):
        self.observe("douyin_video_statistics", 1)
        with transaction(self.c):
            for field, eligibility in (("view_count", "historical_only"), ("share_count", "active")):
                record_policy_transition(self.c, policy_version=V3, provider="tikhub", operation="douyin_video_statistics",
                    field=field, eligibility=eligibility, priority=0, effective_at="2026-08-29T02:00:00Z",
                    recorded_at="2026-08-29T02:00:00Z", contract_receipt_sha256="a"*64)
        self.assertEqual(self.field()["freshness"], "stale")
        self.assertFalse(self.field()["is_latest_valid"])
        self.assertIsNone(self.field("share_count")["value"])
        self.assertFalse(self.field("share_count")["is_latest_valid"])

    def test_xhs_view_is_not_applicable_and_counts_are_qualified(self):
        self.observe("xiaohongshu_note_statistics", 1, content_id=2, values={"view_count": 999, "share_count": 0})
        fields = self.selected(content_id=2)["fields"]
        self.assertEqual(fields["view_count"]["status"], "not_applicable")
        self.assertIsNone(fields["view_count"]["value"])
        for name in ("comment_count", "like_count", "share_count", "collect_count"):
            self.assertTrue(fields[name]["is_latest_valid"])
            self.assertEqual(fields[name]["freshness"], "fresh")
        self.assertEqual(fields["share_count"]["value"], 0)

    def test_qualified_missing_is_not_hidden_by_later_unqualified_audit_field(self):
        self.observe("douyin_video_statistics", 1, values={"view_count": None})
        self.observe("douyin_video_detail", 2, values={"view_count": 999})
        field = self.field()
        self.assertEqual(field["status"], "missing")
        self.assertEqual(field["effective_operation"], "douyin_video_statistics")
        self.assertIsNone(field["value"])
        self.assertFalse(field["is_latest_valid"])

    def test_expiry_boundary_and_transition_cannot_extend_original_ttl(self):
        self.observe("douyin_video_statistics", 1)
        with transaction(self.c):
            record_policy_transition(self.c, policy_version=V3, provider="tikhub", operation="douyin_video_statistics",
                field="view_count", eligibility="active", priority=0, ttl_seconds=999999,
                effective_at="2026-08-29T02:00:00Z", recorded_at="2026-08-29T02:00:00Z", contract_receipt_sha256="a"*64)
        field = self.field(cutoff="2026-08-30T01:00:00Z")
        self.assertEqual(field["expires_at"], "2026-08-30T01:00:00Z")
        self.assertEqual(field["freshness"], "stale")

    def test_independent_availability_event_invalidates_only_v3_cached_projection(self):
        self.observe("douyin_video_detail", 1, values={"share_count": 5})
        with transaction(self.c):
            project_content(self.c, 1, cutoff_at="2026-08-29T01:30:00Z", policy_version=V3)
        original = self.field("share_count", cutoff="2026-08-29T01:30:00Z")
        self.assertEqual(original["freshness"], "fresh")
        fact_count = self.c.execute("SELECT COUNT(*) FROM content_metric_field_facts").fetchone()[0]
        # The real availability producer appends this independent event shape;
        # it has no new metric observation and cannot trigger fact invalidation.
        with transaction(self.c):
            raw_id = self.fx.raw("TikHub", captured_at="2026-08-29T02:00:00Z", operation="douyin_video_detail")
            value = dict(content_id=1, observation_id=None, provider="tikhub", operation="douyin_video_detail",
                         availability="unavailable", captured_at="2026-08-29T02:00:00Z", recorded_at="2026-08-29T03:00:00Z",
                         raw_response_id=raw_id, reason="confirmed_unavailable")
            value["evidence_sha256"] = hashlib.sha256(json.dumps(value, sort_keys=True, separators=(",", ":")).encode()).hexdigest()
            self.c.execute("INSERT INTO content_availability_observations(" + ",".join(value) + ") VALUES(" + ",".join("?" for _ in value) + ")", tuple(value.values()))
        self.assertEqual(self.c.execute("SELECT COUNT(*) FROM content_metric_field_facts").fetchone()[0], fact_count)
        self.assertEqual(self.field("share_count", cutoff="2026-08-29T02:30:00Z"), original)
        exact = business_projection(self.c, select_field_facts(self.c, 1, cutoff_at="2026-08-29T04:00:00Z", policy_version=V3))
        cached = self.selected()
        self.assertEqual(cached, exact)
        self.assertEqual(cached["fields"]["share_count"]["freshness"], "stale")

    def test_equal_capture_time_uses_recording_time_before_insert_id(self):
        newer = self.observe("douyin_video_detail", 1, recorded_at="2026-08-29T03:00:00Z", values={"comment_count": 10})
        self.observe("douyin_user_posts", 1, recorded_at="2026-08-29T02:00:00Z", values={"comment_count": 20})
        field = self.field("comment_count")
        self.assertEqual((field["value"], field["observation_id"]), (10, newer.observation_id))

    def test_v3_source_streams_do_not_depend_on_future_v2_configuration(self):
        self.observe("douyin_video_statistics", 1, values={"view_count": 1000})
        self.observe("douyin_video_detail", 2, values={"view_count": 0})
        expected = self.selected()
        with patch("v8.metric_field_facts.load_policy", side_effect=AssertionError("v3 must use its frozen streams")):
            self.assertEqual(self.selected(), expected)
        self.assertIn({"provider": "newrank_matrix", "operation": "matrix_works_list"},
                      load_policy(policy_version=V3)["historical_streams"])

    def test_policy_is_complete_versioned_and_mutation_safe(self):
        policy = load_policy(policy_version=V3)
        self.assertEqual(policy["policy_version"], V3)
        policy["operation_rules"][0]["active_fields"].append("share_count")
        self.assertNotIn("share_count", load_policy(policy_version=V3)["operation_rules"][0]["active_fields"])
        self.assertEqual(load_policy()["policy_version"], POLICY_VERSION)


if __name__ == "__main__":
    unittest.main()
