"""Metric gaps select qualified routes without repeating complete requests."""
from __future__ import annotations

import socket
import unittest
from unittest.mock import patch

from tests import test_v8_provider_updates as update_fixture
from tests import test_v8_source_routing as source_fixture
from v8 import pipeline, provider_updates
from v8.source_routing import METRIC_FIELDS
from v8.storage import initialize_database


AT = "2026-08-29T04:00:00Z"


class MetricSupplementRouteExecutionTest(unittest.TestCase):
    """Use the existing isolated legacy fixture to exercise the real executor."""

    def setUp(self):
        self.enterContext(patch.object(socket.socket, "connect", side_effect=AssertionError("network forbidden")))
        self.fx = update_fixture.ProviderUpdatesTest(methodName="runTest")
        self.fx.setUp()
        self.addCleanup(self.fx.tearDown)

    def test_only_missing_share_requests_detail_once_and_reentry_is_free(self):
        self.fx.observe(view_count=100, like_count=20, comment_count=10, collect_count=3)
        first = self.fx.run_metrics()
        second = self.fx.run_metrics()
        self.assertEqual(self.fx.calls, ["detail_counts"])
        self.assertEqual(first["missing_fields"], [])
        self.assertEqual(first["provider_cost"], .001)
        self.assertEqual(second["provider_cost"], 0)

    def test_shared_detail_gaps_do_not_purchase_a_statistics_request(self):
        self.fx.observe(view_count=100, like_count=20)
        result = self.fx.run_metrics()
        self.assertEqual(self.fx.calls, ["detail_counts"])
        self.assertEqual(result["missing_fields"], [])

    def test_both_statistics_fields_missing_need_only_one_request(self):
        self.fx.observe(comment_count=10, share_count=2, collect_count=3)
        result = self.fx.run_metrics()
        self.assertEqual(self.fx.calls, ["statistics"])
        self.assertEqual(result["missing_fields"], [])


class MetricSupplementQualifiedSourceTest(unittest.TestCase):
    """Use real schema21 facts and planning; no selector or gap stubs."""

    def setUp(self):
        self.enterContext(patch.object(socket.socket, "connect", side_effect=AssertionError("network forbidden")))
        self.fx = source_fixture.SourceRoutingTest(methodName="runTest")
        with patch.object(source_fixture, "initialize_database",
                          side_effect=lambda c: initialize_database(c, target_version=21)):
            self.fx.setUp()
        self.addCleanup(self.fx.tearDown)
        self.c = self.fx.connection
        self.content = dict(self.c.execute("SELECT * FROM content_items WHERE id=1").fetchone())

    def observe(self, operation, *, captured_at="2026-08-29T03:00:00Z", **values):
        return self.fx.observe("TikHub", operation=operation, captured_at=captured_at,
                               values={name: None for name in METRIC_FIELDS} | values)

    def missing(self):
        return provider_updates.missing_metric_fields(self.c, 1, at=AT)

    def targets(self):
        return pipeline._candidate_work_targets(self.c, "metrics_backfill", self.content, at=AT)

    def test_only_missing_share_plans_the_detail_operation(self):
        self.observe("douyin_video_statistics", view_count=100, like_count=20, share_count=900)
        self.observe("douyin_user_posts", comment_count=10, collect_count=3)
        self.assertEqual(self.missing(), ["share_count"])
        targets = self.targets()
        self.assertEqual([(item["group"], item["operation"]) for item in targets],
                         [("detail_counts", "douyin_video_detail")])

    def test_post_view_and_like_do_not_satisfy_statistics_gaps(self):
        self.observe("douyin_user_posts", view_count=100, like_count=20,
                     comment_count=10, share_count=2, collect_count=3)
        self.assertEqual(self.missing(), ["view_count", "like_count"])
        self.assertEqual([(item["group"], item["operation"]) for item in self.targets()],
                         [("statistics", "douyin_video_statistics")])

    def test_each_statistics_field_alone_plans_statistics(self):
        self.observe("douyin_user_posts", comment_count=10, share_count=2, collect_count=3)
        for absent, supplied in (("view_count", {"like_count": 20}),
                                 ("like_count", {"view_count": 100})):
            with self.subTest(field=absent):
                self.observe("douyin_video_statistics", **supplied)
                self.assertEqual(self.missing(), [absent])
                self.assertEqual([item["operation"] for item in self.targets()],
                                 ["douyin_video_statistics"])

    def test_newer_post_placeholders_do_not_repurchase_fresh_statistics(self):
        self.observe("douyin_video_statistics", view_count=100, like_count=20)
        self.observe("douyin_user_posts", captured_at="2026-08-29T03:30:00Z",
                     view_count=0, like_count=0, comment_count=10, share_count=2, collect_count=3)
        self.assertEqual(self.missing(), [])
        self.assertEqual(self.targets(), [])

