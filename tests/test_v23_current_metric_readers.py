"""Actual CSV/SPU readers use the API's current field facts; schema19 stays v2."""
from __future__ import annotations

import csv
import io
import json
import unittest
from datetime import datetime, timedelta
from unittest.mock import patch

from tests import test_v8_matrix_metric_readers as fixture
from v8.api import ContentSearchRequest, _content_search
from v8.metric_observations import persist_metric_observation
from v8.metric_source_policy import CURRENT_METRIC_POLICY
from v8.operations import export_contents_csv, upsert_content
from v8.source_routing import (
    LEGACY_POLICY_VERSION, select_content_metrics, select_current_content_metrics,
)
from v8.spu_audience import build_stats
from v8.storage import initialize_database, live_wal_read_only_connections, transaction


class CurrentMetricReadersTest(unittest.TestCase):
    def setUp(self):
        self.enterContext(patch("socket.socket.connect", side_effect=AssertionError("network forbidden")))

    def database(self, version=23):
        fx = fixture.MatrixMetricReadersTest(methodName="runTest")
        with patch.object(fixture, "initialize_database", side_effect=lambda c: initialize_database(c, target_version=version)):
            fx.setUp()
        self.addCleanup(fx.tearDown)
        return fx

    def rows(self, fx):
        exported = list(csv.DictReader(io.StringIO(export_contents_csv(db_path=fx.db).decode("utf-8-sig"))))
        page = _content_search(ContentSearchRequest(), db_path=fx.db)["items"]
        with live_wal_read_only_connections():
            stats = build_stats(db_path=fx.db, read_only=True)
        return ({row["platform_content_id"]: row for row in exported},
                {row["id"]: row for row in page}, stats)

    def test_current_csv_and_both_spu_rollups_match_api_operation_fact(self):
        for version in (20, 21, 22, 23):
            with self.subTest(schema=version):
                fx = self.database(version)
                trusted = fx.capture("tikhub", {"view_count": 1200, "comment_count": 7})
                matrix = fx.capture("newrank_matrix", {"view_count": 9000, "comment_count": 99})
                old = select_content_metrics(fx.connection, [fx.cid])[fx.cid]
                self.assertEqual(old["view_count"], 9000)  # This fixture distinguishes the old path.
                selected = select_current_content_metrics(fx.connection, [fx.cid])[fx.cid]
                subset = select_current_content_metrics(fx.connection, [fx.cid], metric_fields=("view_count",))[fx.cid]
                field = selected["fields"]["view_count"]
                self.assertEqual(selected["policy_version"], CURRENT_METRIC_POLICY)
                self.assertEqual((field["value"], field["status"], field["effective_provider"], field["raw_response_id"]),
                                 (1200, "provided", "tikhub", trusted["raw_id"]))
                self.assertEqual(subset["fields"]["view_count"], field)
                self.assertEqual(selected["fields"]["comment_count"]["raw_response_id"], matrix["raw_id"])
                csv_rows, page, stats = self.rows(fx)
                self.assertEqual(page[fx.cid]["metric_fields"]["view_count"], field)
                self.assertEqual(page[fx.cid]["comment_count"], 99)
                with self.subTest(reader="csv"):
                    self.assertEqual((csv_rows["1234567890123456789"]["view_count"], csv_rows["1234567890123456789"]["comment_count"]),
                                     ("1200", "99"))
                with self.subTest(reader="spu"):
                    self.assertTrue(stats["ready"])
                    self.assertEqual(stats["totals"]["valid_exposure_views"], page[fx.cid]["view_count"])
                    self.assertEqual(stats["channel_totals"]["douyin"]["valid_views"], 1200)

    def test_valid_decrease_to_zero_and_later_missing_keep_original_source(self):
        fx = self.database()
        base = datetime.fromisoformat(fx.now.replace("Z", "+00:00"))
        def at(minutes):
            return (base - timedelta(minutes=minutes)).isoformat().replace("+00:00", "Z")
        fx.capture("tikhub", {"view_count": 1200}, captured=at(4))
        zero = fx.capture("tikhub", {"view_count": 0}, captured=at(3))
        fx.capture("tikhub", {"view_count": None}, captured=at(2))
        selected = select_current_content_metrics(fx.connection, [fx.cid])[fx.cid]
        field = selected["fields"]["view_count"]
        self.assertEqual((field["value"], field["raw_response_id"], field["captured_at"]), (0, zero["raw_id"], at(3)))
        csv_rows, page, stats = self.rows(fx)
        self.assertEqual(page[fx.cid]["metric_fields"]["view_count"], field)
        self.assertEqual(csv_rows["1234567890123456789"]["view_count"], "0")
        self.assertEqual(stats["totals"]["valid_exposure_views"], 0)
        self.assertEqual(stats["channel_totals"]["douyin"]["valid_views"], 0)

    def test_unavailable_wechat_views_are_blank_and_do_not_inflate_spu_total(self):
        fx = self.database()
        cid = upsert_content({"platform": "wechat_channels", "platform_content_id": "12345678901234567890",
                              "canonical_url": "https://channels.weixin.qq.com/video/12345678901234567890",
                              "published_at": fx.now}, db_path=fx.db)["id"]
        with transaction(fx.connection):
            slot = fx.connection.execute("""INSERT INTO fetch_slots(content_id,stage,window_key,provider,
                adapter_version,status,attempt_count,created_at,updated_at)
                VALUES (?,'metrics','wechat-fixture','TikHub','fixture','succeeded',1,?,?)""", (cid, fx.now, fx.now)).lastrowid
            attempt = fx.connection.execute("""INSERT INTO fetch_attempts(slot_id,attempt_number,
                request_started_at,response_finished_at,http_status,billed) VALUES (?,1,?,?,200,0)""",
                (slot, fx.now, fx.now)).lastrowid
            raw = fx.connection.execute("""INSERT INTO provider_raw_responses(fetch_attempt_id,content_id,
                provider,operation,local_path,sha256,byte_size,http_status,captured_at)
                VALUES (?,?,'TikHub','wechat_channels_video_statistics','wechat-fixture.json',?,10,200,?)""",
                (attempt, cid, "a" * 64, fx.now)).lastrowid
            persist_metric_observation(fx.connection, content_id=cid, captured_at=fx.now, recorded_at=fx.now,
                window_key=fx.window, view_count=9000, comment_count=7, like_count=None, share_count=None,
                collect_count=None, status="available", provider="tikhub",
                platform="wechat_channels", raw_response_id=raw,
                metadata_json=json.dumps({"fields": {"view_count": {"status": "provided"}, "comment_count": {"status": "provided"}}}))
        selected = select_current_content_metrics(fx.connection, [cid])[cid]
        field = selected["fields"]["view_count"]
        self.assertEqual((field["status"], field["capability"], field["value"]), ("unavailable", "unavailable", None))
        csv_rows, page, stats = self.rows(fx)
        self.assertEqual(page[cid]["metric_fields"]["view_count"], field)
        with self.subTest(reader="csv"):
            self.assertEqual(csv_rows["12345678901234567890"]["view_count"], "")
            self.assertEqual(csv_rows["12345678901234567890"]["comment_count"], "7")
        with self.subTest(reader="spu"):
            self.assertEqual(stats["totals"]["valid_exposure_views"], 0)

    def test_schema19_csv_and_spu_keep_explicit_v2_api_contract(self):
        fx = self.database(19)
        fx.capture("tikhub", {"view_count": 1200, "comment_count": 7})
        matrix = fx.capture("newrank_matrix", {"view_count": 9000, "comment_count": 99})
        selected = select_content_metrics(fx.connection, [fx.cid], policy_version=LEGACY_POLICY_VERSION)[fx.cid]
        field = selected["fields"]["view_count"]
        self.assertEqual((selected["policy_version"], field["raw_response_id"]), (LEGACY_POLICY_VERSION, matrix["raw_id"]))
        with self.assertRaisesRegex(ValueError, "requires schema20"):
            select_current_content_metrics(fx.connection, [fx.cid])
        csv_rows, page, stats = self.rows(fx)
        self.assertEqual(page[fx.cid]["metric_fields"]["view_count"], field)
        self.assertEqual(csv_rows["1234567890123456789"]["view_count"], "9000")
        self.assertEqual(stats["totals"]["valid_exposure_views"], 9000)
        self.assertEqual(stats["channel_totals"]["douyin"]["valid_views"], 9000)


if __name__ == "__main__":
    unittest.main()
