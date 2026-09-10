"""Only newly received facts get diagnostics; fixtures never contact a provider."""
from __future__ import annotations

import json
import unittest

from tests import test_v8_source_routing as fixture
from v8 import metric_field_facts as facts, schema_v20
from v8.storage import transaction


class NewMetricChecksTest(unittest.TestCase):
    raw = fixture.SourceRoutingTest.raw
    observe = fixture.SourceRoutingTest.observe
    selected = fixture.SourceRoutingTest.selected
    tearDown = fixture.SourceRoutingTest.tearDown

    def setUp(self):
        fixture.SourceRoutingTest.setUp(self)
        schema_v20.migrate(self.connection)

    def alerts(self):
        return [dict(row) for row in self.connection.execute(
            "SELECT * FROM operational_alerts WHERE owner='capture-data' ORDER BY id")]

    def pair(self, old, new, field="view_count", operation="douyin_video_statistics"):
        first = self.observe("TikHub", operation=operation, captured_at="2026-08-29T01:00:00Z",
                             values={field: old})
        second = self.observe("TikHub", operation=operation, captured_at="2026-08-29T02:00:00Z",
                              values={field: new})
        return first, second

    def test_decline_is_logged_but_real_zero_is_not_replaced_by_maximum(self):
        first, second = self.pair(1000, 0)
        self.assertEqual(self.selected()["view_count"], 0)
        self.assertEqual(self.selected()["fields"]["view_count"]["freshness"], "fresh")
        self.assertEqual(len(self.alerts()), 1)
        alert = json.loads(self.alerts()[0]["evidence_json"])
        self.assertEqual((alert["previous_value"], alert["value"], alert["kind"]), (1000, 0, "decline"))
        with transaction(self.connection):
            facts.ingest_observation(self.connection, second.observation_id, record_anomalies=True)
        self.assertEqual(len(self.alerts()), 1)
        self.assertEqual(self.connection.execute("SELECT view_count FROM content_metric_observations WHERE id=?",
                                               (first.observation_id,)).fetchone()[0], 1000)

    def test_strict_threshold_and_small_changes_do_not_alert(self):
        self.pair(1000, 800)
        self.assertEqual(self.alerts(), [])
        self.assertEqual(self.selected()["view_count"], 800)

    def test_large_increase_is_logged_without_extra_request(self):
        self.pair(100, 1201)
        self.assertEqual(len(self.alerts()), 1)
        evidence = json.loads(self.alerts()[0]["evidence_json"])
        self.assertEqual(evidence["kind"], "increase")
        self.assertEqual(evidence["provider_calls"], 0)

    def test_different_operation_is_not_compared(self):
        self.observe("TikHub", operation="douyin_video_detail", captured_at="2026-08-29T01:00:00Z",
                     values={"like_count": 1000})
        self.observe("TikHub", operation="douyin_user_posts", values={"like_count": 0})
        self.assertEqual(self.alerts(), [])

    def test_out_of_order_new_arrival_does_not_trigger_historical_audit(self):
        self.observe("TikHub", values={"view_count": 1000})
        self.observe("TikHub", captured_at="2026-08-29T01:00:00Z", values={"view_count": 0})
        self.assertEqual(self.alerts(), [])
        self.assertEqual(self.selected()["view_count"], 1000)

    def test_invalid_and_missing_keep_last_value_stale_not_new_zero(self):
        first = self.observe("TikHub", captured_at="2026-08-29T01:00:00Z", values={"view_count": 1000})
        for index, value in enumerate((None, -1, True, 1.5)):
            with self.subTest(value=value):
                self.observe("TikHub", captured_at=f"2026-08-29T02:0{index}:00Z", values={"view_count": value})
                field = self.selected()["fields"]["view_count"]
                self.assertEqual(field["value"], 1000)
                self.assertEqual(field["observation_id"], first.observation_id)
                self.assertEqual(field["freshness"], "stale")
        self.assertEqual(self.alerts(), [])

    def test_placeholder_zero_and_audit_only_share_are_not_trusted(self):
        self.pair(1000, 0, operation="douyin_video_detail")
        field = self.selected()["fields"]["view_count"]
        self.assertEqual(field["value"], 1000)
        self.assertEqual(field["freshness"], "stale")
        self.assertEqual(self.alerts(), [])
        self.observe("TikHub", operation="douyin_video_statistics", values={"share_count": 9999})
        self.assertNotEqual(self.selected()["share_count"], 9999)

    def test_plain_fact_reingestion_and_migration_do_not_backfill_alerts(self):
        self.pair(1000, 0)
        count = len(self.alerts())
        with transaction(self.connection):
            for row in self.connection.execute("SELECT id FROM content_metric_observations"):
                facts.ingest_observation(self.connection, row[0])
        self.assertEqual(len(self.alerts()), count)


if __name__ == "__main__":
    unittest.main()
