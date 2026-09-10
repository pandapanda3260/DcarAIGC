"""Schema20 read optimization keeps historical facts and API evidence exact."""
from __future__ import annotations

import unittest
from unittest.mock import patch

from tests import test_v8_source_routing as fixture
from v8 import metric_field_facts as facts, schema_v20
from v8.source_routing import METRIC_FIELDS, select_content_metrics
from v8.storage import transaction


class MetricProjectionPerformanceTest(unittest.TestCase):
    raw = fixture.SourceRoutingTest.raw
    observe = fixture.SourceRoutingTest.observe
    tearDown = fixture.SourceRoutingTest.tearDown

    def setUp(self):
        fixture.SourceRoutingTest.setUp(self)
        schema_v20.migrate(self.connection)

    def assert_matches_unfiltered(self, *, cutoff=fixture.NOW, window=None):
        for fields in (METRIC_FIELDS, ("view_count", "comment_count")):
            with self.subTest(fields=fields, cutoff=cutoff, window=window):
                arguments = dict(cutoff_at=cutoff, knowledge_at=cutoff,
                                 window_key=window, metric_fields=fields)
                with patch.object(facts, "read_projection", return_value=None):
                    actual = facts.select_metric_projections(self.connection, [1, 2, 3], **arguments)
                    with patch.object(facts, "_fact_streams", return_value=facts._streams()):
                        reference = facts.select_metric_projections(self.connection, [1, 2, 3], **arguments)
                self.assertEqual(actual, reference)

    def test_sparse_sources_avoid_empty_stream_queries_and_duplicate_projection(self):
        self.observe("TikHub")
        statements = []
        self.connection.set_trace_callback(statements.append)
        try:
            with (patch.object(facts, "read_projection", return_value=None),
                  patch.object(facts, "business_projection", wraps=facts.business_projection) as build):
                result = select_content_metrics(self.connection, [1], cutoff_at=fixture.NOW,
                                                metric_fields=("view_count", "comment_count"))
                self.assertEqual(build.call_count, 1)
        finally:
            self.connection.set_trace_callback(None)
        self.assertEqual(result[1]["view_count"], 100)
        self.assertLess(len(statements), 60)
        stream_queries = [sql for sql in statements if "SELECT f.*,x.id correction_id" in sql]
        self.assertTrue(stream_queries)
        self.assertTrue(all("f.provider='tikhub'" in sql for sql in stream_queries))
        self.assert_matches_unfiltered()

    def test_empty_content_and_unknown_content_do_not_create_metrics(self):
        self.assertEqual(select_content_metrics(self.connection, [1, 999], cutoff_at=fixture.NOW), {})
        self.assert_matches_unfiltered()

    def test_future_knowledge_and_other_window_do_not_leak_into_reads(self):
        self.observe("TikHub", recorded_at="2026-08-30T04:00:00Z", window="future")
        self.observe(content_id=2, window="other")
        self.assert_matches_unfiltered()
        self.assert_matches_unfiltered(window="future")
        self.assert_matches_unfiltered(cutoff="2026-08-31T04:00:00Z")

    def test_missing_invalid_and_unrequested_events_preserve_old_values_and_ttl(self):
        self.observe("TikHub", captured_at="2026-08-29T01:00:00Z")
        self.observe("TikHub", values={"view_count": None, "comment_count": -1})
        self.observe("TikHub", operation="douyin_video_detail", content_id=2,
                     values={"view_count": 0, "comment_count": None})
        self.assert_matches_unfiltered()
        self.assert_matches_unfiltered(cutoff="2026-09-01T04:00:00Z")

    def test_correction_keeps_original_capture_and_knowledge_cutoff(self):
        self.observe("TikHub")
        fact_id = self.connection.execute(
            "SELECT id FROM content_metric_field_facts WHERE content_id=1 AND field='view_count'"
        ).fetchone()[0]
        with transaction(self.connection):
            facts.record_correction(self.connection, fact_id, action="replace", value=77,
                                    rule_id="fixture-rule", recorded_at="2026-08-29T05:00:00Z")
        self.assert_matches_unfiltered()
        self.assert_matches_unfiltered(cutoff="2026-08-29T06:00:00Z")
        selected = select_content_metrics(self.connection, [1], cutoff_at="2026-08-29T06:00:00Z")
        self.assertEqual(selected[1]["view_count"], 77)
        self.assertEqual(selected[1]["captured_at"], fixture.CAPTURE)

    def test_saved_and_expired_projection_keep_partial_field_results(self):
        self.observe("TikHub")
        with transaction(self.connection):
            saved = facts.project_content(self.connection, 1, cutoff_at=fixture.NOW)
        self.assertIsNotNone(saved.get("projection_id"))
        fields = ("view_count", "comment_count")
        self.assertEqual(
            select_content_metrics(self.connection, [1], cutoff_at=fixture.NOW, metric_fields=fields)[1],
            facts.business_projection(self.connection, saved, metric_fields=fields),
        )
        self.assert_matches_unfiltered()
        self.assert_matches_unfiltered(cutoff="2026-09-01T04:00:00Z")

    def test_alias_scope_includes_sources_on_every_merged_content(self):
        self.observe()
        self.observe("TikHub", content_id=3, captured_at="2026-08-29T03:30:00Z")
        with transaction(self.connection):
            self.connection.execute(
                "INSERT INTO content_identity_merge_events("
                "loser_content_id,winner_content_id,recorded_at,identity_snapshot_json,reason,event_sha256) "
                "VALUES(3,1,?,'{}','fixture-merge',?)", (fixture.NOW, "a" * 64),
            )
        streams = facts._fact_streams(self.connection, [1, 3])
        self.assertIn(("newrank_matrix", "matrix_works_list"), streams)
        self.assertIn(("tikhub", "douyin_video_statistics"), streams)
        self.assert_matches_unfiltered()

    def test_invalidation_keeps_earlier_history_available(self):
        self.observe("TikHub", captured_at="2026-08-29T01:00:00Z", values={"view_count": 50})
        self.observe("TikHub")
        fact_id = self.connection.execute(
            "SELECT id FROM content_metric_field_facts WHERE content_id=1 AND field='view_count' "
            "ORDER BY id DESC LIMIT 1"
        ).fetchone()[0]
        with transaction(self.connection):
            facts.record_correction(self.connection, fact_id, action="invalidate",
                                    rule_id="fixture-invalidate", recorded_at="2026-08-29T05:00:00Z")
        self.assert_matches_unfiltered(cutoff="2026-08-29T06:00:00Z")
        result = select_content_metrics(self.connection, [1], cutoff_at="2026-08-29T06:00:00Z")[1]
        self.assertEqual(result["view_count"], 50)
        self.assertEqual(result["fields"]["view_count"]["freshness"], "stale")

    def test_snapshot_fallback_stays_current_only(self):
        self.connection.execute(
            "INSERT INTO content_metric_snapshots("
            "content_id,captured_at,window_key,view_count,comment_count,status,source) "
            "VALUES (3,?,'legacy',123,7,'available','douyin')", (fixture.CAPTURE,)
        )
        self.connection.commit()
        args = dict(cutoff_at=fixture.NOW, knowledge_at=fixture.NOW,
                    window_key=None, metric_fields=("view_count", "comment_count"))
        current = facts.select_metric_projections(self.connection, [3], current_read=True, **args)
        self.assertEqual(current[3]["view_count"], 123)
        self.assertEqual(current[3]["status"], "stale")
        self.assertEqual(facts.select_metric_projections(self.connection, [3], **args), {})
        self.assert_matches_unfiltered()


if __name__ == "__main__":
    unittest.main()
