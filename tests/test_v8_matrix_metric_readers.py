from __future__ import annotations

import csv
import io
import json
import tempfile
import unittest
from datetime import datetime, timedelta, timezone
from pathlib import Path
from types import SimpleNamespace
from unittest.mock import patch

from v8.api import ContentSearchRequest, _content_search
from v8.metric_observations import persist_metric_observation
from v8.operations import export_contents_csv, upsert_content
from v8.providers import (
    ProviderConfigurationError, _parse_douyin_stage_payload, _store_stage_result,
    _xhs_metrics, _zero_comment_metric_result,
)
from v8.reports import _latest_metric_observations_at, _metric_freshness_detail
from v8.source_routing import select_content_metrics
from v8.spu_audience import build_stats
from v8.storage import connect, initialize_database, now_utc, transaction
from tests.v9_report_fixture import activate_v9_report_fixture


FIELDS = ("view_count", "comment_count", "like_count", "share_count", "collect_count")


class MatrixMetricReadersTest(unittest.TestCase):
    def setUp(self) -> None:
        self.temp = tempfile.TemporaryDirectory()
        self.db = Path(self.temp.name) / "metrics.sqlite3"
        self.connection = connect(self.db)
        initialize_database(self.connection)
        self.now = now_utc()
        self.window = self.now[:10]
        self.cid = upsert_content({"platform": "douyin", "platform_content_id": "1234567890123456789",
                                   "canonical_url": "https://www.douyin.com/video/1234567890123456789", "published_at": self.now}, db_path=self.db)["id"]
        activate_v9_report_fixture(self.db, [])

    def tearDown(self) -> None:
        self.connection.close()
        self.temp.cleanup()

    def capture(self, provider, values, *, captured=None, recorded=None, cid=None) -> dict:
        captured = captured or self.now
        cid = cid or self.cid
        with transaction(self.connection):
            attempt_id = None
            if provider == "tikhub":
                slot = self.connection.execute(
                    """INSERT INTO fetch_slots(
                        content_id,stage,window_key,provider,adapter_version,
                        status,attempt_count,created_at,updated_at
                    ) VALUES (?,'metrics',?,'TikHub','fixture-v1',
                              'succeeded',1,?,?)""",
                    (
                        cid,
                        f"reader-fixture:{self.connection.total_changes}",
                        captured,
                        captured,
                    ),
                )
                attempt = self.connection.execute(
                    """INSERT INTO fetch_attempts(
                        slot_id,attempt_number,request_started_at,
                        response_finished_at,http_status,billed
                    ) VALUES (?,1,?,?,200,0)""",
                    (int(slot.lastrowid), captured, captured),
                )
                attempt_id = int(attempt.lastrowid)
            cursor = self.connection.execute(
                """INSERT INTO provider_raw_responses(
                    fetch_attempt_id,content_id,provider,operation,local_path,
                    sha256,byte_size,http_status,captured_at
                ) VALUES (?,?,?,?,?,?,10,200,?)""",
                (attempt_id, cid, "TikHub" if provider == "tikhub" else provider,
                 "douyin_video_statistics" if provider == "tikhub" else "matrix_works_list",
                 f"raw-{self.connection.total_changes}.json", "a" * 64, captured),
            )
            raw_id = int(cursor.lastrowid)
            status = {field: {"status": "provided" if values.get(field) is not None else "missing"} for field in FIELDS}
            result = persist_metric_observation(
                self.connection, content_id=cid, captured_at=captured, recorded_at=recorded or captured,
                window_key=self.window, **{field: values.get(field) for field in FIELDS},
                status="available", provider=provider, platform="douyin", raw_response_id=raw_id,
                metadata_json=json.dumps({"fields": status}),
            )
        return {"raw_id": raw_id, "observation_id": result.observation_id}

    def test_content_csv_report_and_zero_comments_use_the_same_field_provenance(self) -> None:
        tikhub = self.capture("tikhub", dict(zip(FIELDS, (100, 7, 10, 2, 3))))
        matrix = self.capture("newrank_matrix", dict(zip(FIELDS, (None, 0, 5, 1, 1))))
        projection = select_content_metrics(self.connection, [self.cid], cutoff_at=self.now)[self.cid]
        self.assertEqual((projection["view_count"], projection["like_count"], projection["comment_count"]), (100, 5, 0))
        self.assertIsNone(projection["raw_response_id"])
        self.assertEqual(projection["fields"]["view_count"]["raw_response_id"], tikhub["raw_id"])
        self.assertEqual(projection["fields"]["comment_count"]["raw_response_id"], matrix["raw_id"])
        page = _content_search(ContentSearchRequest(), db_path=self.db)["items"][0]
        self.assertEqual((page["view_count"], page["like_count"], page["comment_count"]), (100, 5, 0))
        exported = next(csv.DictReader(io.StringIO(export_contents_csv(db_path=self.db).decode("utf-8-sig"))))
        self.assertEqual((exported["view_count"], exported["comment_count"]), ("100", "0"))
        report = _latest_metric_observations_at(self.connection, [self.cid], cutoff_at=self.now)[self.cid]
        self.assertEqual(report["fields"], projection["fields"])
        zero = _zero_comment_metric_result(self.cid, metric_window_key=self.window, db_path=self.db)
        self.assertIsNotNone(zero)
        self.assertEqual(zero.raw_response["source_raw_response_id"], matrix["raw_id"])
        self.assertEqual(zero.raw_response["source_captured_at"], self.now)

    def test_future_recorded_fact_does_not_enter_past_report_or_freshness(self) -> None:
        future = (datetime.fromisoformat(self.now.replace("Z", "+00:00")) + timedelta(days=1)).isoformat()
        self.capture("newrank_matrix", dict(zip(FIELDS, (100, 0, 5, 1, 1))), recorded=future)
        report = _latest_metric_observations_at(self.connection, [self.cid], cutoff_at=self.now).get(self.cid, {})
        self.assertIsNone(report.get("view_count"))
        fresh = _metric_freshness_detail(self.connection, [self.cid], cutoff_at=self.now, minimum_percentage=90)
        self.assertEqual(fresh["fresh_count"], 0)

    def test_spu_both_rollup_passes_use_shared_selection(self) -> None:
        self.capture("tikhub", dict(zip(FIELDS, (999, 7, 10, 2, 3))))
        self.capture("newrank_matrix", dict(zip(FIELDS, (100, 0, 5, 1, 1))))
        stats = build_stats(db_path=self.db)
        self.assertTrue(stats["ready"])
        self.assertEqual(stats["totals"]["valid_exposure_views"], 100)
        self.assertEqual(stats["channel_totals"]["douyin"]["valid_views"], 100)
        self.assertEqual(stats["channel_totals"]["xiaohongshu"]["exposure_status"], "not_applicable")

    def test_old_comment_zero_cannot_be_refreshed_by_new_play_count(self) -> None:
        old = (datetime.now(timezone.utc) - timedelta(days=4)).isoformat()
        self.capture("tikhub", dict(zip(FIELDS, (100, 0, 5, 1, 1))), captured=old)
        self.capture("newrank_matrix", {"view_count": 200})
        self.assertIsNone(_zero_comment_metric_result(self.cid, metric_window_key=self.window, db_path=self.db))
        selected = select_content_metrics(self.connection, [self.cid])[self.cid]
        self.assertEqual(selected["fields"]["comment_count"]["captured_at"], old)
        self.assertEqual(selected["fields"]["comment_count"]["freshness"], "stale")

    def test_statistics_zero_is_valid_but_unrequested_fields_are_not_missing(self) -> None:
        payload = {"code": 200, "data": {"status_code": 0, "statistics_list": [{"aweme_id": "12345", "play_count": 0}]}}
        parsed = _parse_douyin_stage_payload("metrics", "12345", payload)
        self.assertEqual(parsed.data["view_count"], 0)
        self.assertEqual(parsed.data["_field_status"]["view_count"]["status"], "provided")
        self.assertEqual(parsed.data["_field_status"]["comment_count"]["status"], "not_requested")
        xhs = _xhs_metrics({"view_count": 999, "read_count": 333, "liked_count": 0})
        self.assertIsNone(xhs["view_count"])
        self.assertEqual(xhs["_field_status"]["view_count"]["status"], "not_applicable")
        self.assertEqual(xhs["like_count"], 0)

    def test_unknown_media_cannot_mutate_detail_or_make_an_image_manifest(self) -> None:
        content = dict(self.connection.execute("SELECT * FROM content_items WHERE id=?", (self.cid,)).fetchone())
        before = dict(content)
        outcome = SimpleNamespace(data={"content_type": "unknown", "title": "bad type"}, raw_response_id=999)
        with patch("v8.providers.store_media_source_manifest") as write_manifest:
            with self.assertRaisesRegex(ProviderConfigurationError, "unknown"):
                _store_stage_result(content, "detail", "lifetime", outcome, db_path=self.db)
        write_manifest.assert_not_called()
        self.assertEqual(dict(self.connection.execute("SELECT * FROM content_items WHERE id=?", (self.cid,)).fetchone()), before)


if __name__ == "__main__":
    unittest.main()
