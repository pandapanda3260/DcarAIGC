"""Explicit historical policy reads never inherit the current platform rules."""
from __future__ import annotations

import json
from pathlib import Path
import unittest
from unittest.mock import patch

from tests import test_v8_source_routing as fixtures
from v8 import metric_field_facts, report_inputs, source_routing
from v8.source_routing import LEGACY_POLICY_VERSION as V2, POLICY_VERSION as CURRENT
from v8.storage import initialize_database, transaction


class MetricPolicyCompatibilityTest(unittest.TestCase):
    def fixture(self, version):
        fx = fixtures.SourceRoutingTest(methodName="runTest")
        with patch.object(fixtures, "initialize_database", side_effect=lambda c: initialize_database(c,target_version=version)):
            fx.setUp()
        self.addCleanup(fx.tearDown)
        return fx

    def test_explicit_v2_loads_its_original_file_and_default_remains_v3(self):
        path = Path(source_routing.__file__).resolve().parents[3]/"config/source_routing_matrix_first_v2.json"
        original = path.read_bytes()
        legacy = source_routing.load_policy(policy_version=V2)
        self.assertEqual(legacy, json.loads(original))
        self.assertNotIn("kuaishou", legacy["metric_supplement_groups"])
        legacy["metric_supplement_groups"]["kuaishou"] = []
        self.assertEqual(source_routing.load_policy(policy_version=V2), json.loads(original))
        self.assertEqual(path.read_bytes(), original)
        self.assertEqual(source_routing.load_policy()["policy_version"], CURRENT)
        self.assertIn("kuaishou", source_routing.load_policy()["metric_supplement_groups"])
        with self.assertRaises(ValueError):
            source_routing.load_policy(policy_version="source-routing-matrix-first-v999")

    def test_schema19_explicit_legacy_selector_labels_and_uses_v2(self):
        fx = self.fixture(19)
        fx.observe("TikHub", operation="douyin_video_statistics", values={"view_count":123})
        selected = source_routing.select_content_metrics(fx.connection,[1],cutoff_at=fixtures.NOW,policy_version=V2)[1]
        self.assertEqual(selected["policy_version"],V2)
        self.assertEqual(selected["view_count"],123)
        self.assertEqual(json.loads(selected["metadata_json"])["policy_version"],V2)
        self.assertEqual(source_routing.select_content_metrics(fx.connection,[1],cutoff_at=fixtures.NOW)[1]["policy_version"],CURRENT)

    def test_stored_v2_projection_and_frozen_report_remain_byte_identical(self):
        for version in (21,22):
            with self.subTest(schema=version):
                fx = self.fixture(version)
                c = fx.connection
                fx.observe("TikHub",operation="douyin_video_statistics",values={"view_count":123})
                with transaction(c):
                    projection = metric_field_facts.project_content(c,1,cutoff_at=fixtures.NOW,policy_version=V2)
                    c.execute("INSERT INTO report_tasks(id,task_type,name,period_start,period_end,creation_source,task_status,created_at,updated_at) VALUES ('legacy','daily','legacy fixture','2026-08-29','2026-08-29','manual','queued',?,?)",(fixtures.NOW,fixtures.NOW))
                    payload = {"source_policy":source_routing.load_policy(policy_version=V2),"metric_projection":projection}
                    frozen = report_inputs._store(c,"legacy",report_inputs.INPUT_EVENT,payload)
                saved = c.execute("SELECT payload_json FROM content_metric_projection_versions WHERE policy_version=?",(V2,)).fetchall()
                event = c.execute("SELECT payload_json FROM task_events WHERE task_id='legacy'").fetchall()
                writes = c.total_changes
                selected = source_routing.select_content_metrics(c,[1],cutoff_at=fixtures.NOW,policy_version=V2)[1]
                self.assertEqual(selected["view_count"],123)
                self.assertEqual(selected["fields"]["view_count"]["freshness"],"fresh")
                self.assertEqual(selected["policy_version"],V2)
                self.assertEqual(report_inputs.frozen_report(c,"legacy"),frozen)
                self.assertEqual(c.execute("SELECT payload_json FROM content_metric_projection_versions WHERE policy_version=?",(V2,)).fetchall(),saved)
                self.assertEqual(c.execute("SELECT payload_json FROM task_events WHERE task_id='legacy'").fetchall(),event)
                self.assertEqual(c.total_changes,writes)

    def test_old_v2_streams_do_not_gain_new_platform_eligibility(self):
        fx = self.fixture(22)
        fx.connection.execute("UPDATE content_items SET platform='kuaishou' WHERE id=1")
        fx.connection.commit()
        with transaction(fx.connection):
            raw = fx.raw("TikHub",fixtures.CAPTURE,1,"kuaishou_video_statistics",stage="metrics")
        fx.observe("TikHub",operation="kuaishou_video_statistics",raw_id=raw,values={"view_count":12})
        legacy = metric_field_facts.select_field_facts(fx.connection,1,cutoff_at=fixtures.NOW,policy_version=V2)
        current = metric_field_facts.select_field_facts(fx.connection,1,cutoff_at=fixtures.NOW,policy_version=CURRENT)
        self.assertIsNone(legacy["fields"]["view_count"]["selected_fact_id"])
        self.assertEqual(current["fields"]["view_count"]["value"],12)
        self.assertEqual(current["fields"]["view_count"]["freshness"],"fresh")


if __name__ == "__main__":
    unittest.main()
