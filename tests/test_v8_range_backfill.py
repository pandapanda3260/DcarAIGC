from __future__ import annotations

import tempfile
import unittest
from datetime import datetime
from pathlib import Path
from zoneinfo import ZoneInfo

from v8.capture import CaptureError, ProviderResult
from v8.operations import upsert_account, upsert_content
from v8.range_backfill import (
    pending_content_ids,
    repair_discovery_placeholder_metrics,
    run_content_backfill,
    run_discovery_backfill,
    run_repaired_metrics_backfill,
    task_id_for,
)
from v8.storage import connect, initialize_database


SHANGHAI = ZoneInfo("Asia/Shanghai")


class V8RangeBackfillTest(unittest.TestCase):
    def setUp(self) -> None:
        self.temp = tempfile.TemporaryDirectory()
        self.root = Path(self.temp.name)
        self.db = self.root / "range.sqlite3"
        self.state = self.root / "state"
        with connect(self.db) as connection:
            initialize_database(connection)
            connection.commit()
        self.start = datetime(2026, 7, 20, 0, 0, tzinfo=SHANGHAI)
        self.end = datetime(2026, 8, 3, 14, 30, tzinfo=SHANGHAI)
        self.task_id = task_id_for(self.start, self.end)

    def tearDown(self) -> None:
        self.temp.cleanup()

    def test_discovery_filters_range_pages_and_replays_without_new_cost(self) -> None:
        account = upsert_account(
            {
                "phone": "13800138000",
                "platforms": [{
                    "platform": "xiaohongshu",
                    "uid": "67f6657f000000000e02c21c",
                    "nickname": "小红书汽车号",
                }],
            },
            db_path=self.db,
        )
        calls: list[str] = []

        def page(note_id: str, published_at: str) -> dict:
            return {
                "id": note_id,
                "title": f"笔记-{note_id[0]}",
                "desc": "汽车内容",
                "time": published_at,
                "type": "normal",
                "user": {"userid": "67f6657f000000000e02c21c"},
            }

        def discovery_call(operation, identity):
            self.assertEqual(operation, "discover_content")
            cursor = identity.get("cursor")
            calls.append(str(cursor))
            notes = (
                [
                    page("a" * 24, "2026-08-02T01:00:00Z"),
                    page("b" * 24, "2026-07-30T01:00:00Z"),
                ]
                if cursor is None
                else [page("c" * 24, "2026-07-19T01:00:00Z")]
            )
            next_cursor = "page-2" if cursor is None else ""
            has_more = cursor is None
            data = {"notes": notes, "cursor": next_cursor, "has_more": has_more}
            normalized = {
                "items": [{
                    "platform": "xiaohongshu",
                    "platform_content_id": item["id"],
                    "canonical_url": f"https://www.xiaohongshu.com/explore/{item['id']}",
                    "title": item["title"], "body": item["desc"],
                    "published_at": item["time"], "content_type": "image",
                } for item in notes],
                "next_cursor": next_cursor,
                "has_more": has_more,
            }
            raw = {
                "code": 200,
                "data": {"success": True, "code": 0, "data": data},
            }
            return ProviderResult(normalized, raw, 200, True)

        first = run_discovery_backfill(
            start=self.start, end=self.end, task_id=self.task_id,
            max_amount=0.10, db_path=self.db, platforms=["xiaohongshu"],
            call_override=discovery_call, state_root=self.state,
        )
        self.assertEqual(first["status"], "succeeded")
        self.assertEqual(first["pages_processed"], 2)
        self.assertEqual(first["inserted"], 2)
        self.assertEqual(first["usage"]["amount"], 0.02)
        self.assertEqual(calls, ["None", "page-2"])
        with connect(self.db) as connection:
            ids = {
                row["platform_content_id"]
                for row in connection.execute("SELECT platform_content_id FROM content_items")
            }
        self.assertEqual(ids, {"a" * 24, "b" * 24})

        def no_provider_call(operation, identity):
            raise AssertionError(f"unexpected provider call: {operation}")

        second = run_discovery_backfill(
            start=self.start, end=self.end, task_id=self.task_id,
            max_amount=0.10, db_path=self.db, platforms=["xiaohongshu"],
            call_override=no_provider_call, state_root=self.state,
        )
        self.assertEqual(second["status"], "succeeded")
        self.assertEqual(second["inserted"], 0)
        self.assertEqual(second["usage"]["amount"], 0.02)
        self.assertTrue(all(
            page_result["replayed"]
            for result in second["results"] for page_result in result["pages"]
        ))
        self.assertEqual(int(account["id"]), second["results"][0]["account_id"])

    def test_discovery_reports_partial_when_an_account_page_fails(self) -> None:
        upsert_account(
            {
                "phone": "13800138008",
                "platforms": [{
                    "platform": "xiaohongshu",
                    "uid": "67f6657f000000000e02c218",
                    "nickname": "失败样本号",
                }],
            },
            db_path=self.db,
        )

        def failed_call(operation, identity):
            raise CaptureError(
                "provider asked to retry",
                retryable=True,
                error_code="provider_retry_requested",
                billed=False,
            )

        result = run_discovery_backfill(
            start=self.start,
            end=self.end,
            task_id=self.task_id,
            max_amount=0.10,
            db_path=self.db,
            platforms=["xiaohongshu"],
            call_override=failed_call,
            state_root=self.state,
        )
        self.assertEqual(result["status"], "partial")
        self.assertEqual(result["failed_pages"], 1)
        self.assertEqual(result["results"][0]["pages"][0]["status"], "failed")

    def test_content_backfill_is_newest_first_idempotent_and_task_bounded(self) -> None:
        account = upsert_account(
            {
                "phone": "13800138001",
                "platforms": [{
                    "platform": "douyin", "uid": "99887766", "nickname": "汽车号",
                }],
            },
            db_path=self.db,
        )
        for content_id, published_at in (
            ("111111111", "2026-07-25T01:00:00Z"),
            ("222222222", "2026-08-02T01:00:00Z"),
        ):
            upsert_content(
                {
                    "platform": "douyin",
                    "platform_content_id": content_id,
                    "canonical_url": f"https://www.douyin.com/video/{content_id}",
                    "title": "待补抓", "body": "汽车内容",
                    "published_at": published_at, "content_type": "video",
                    "account_uid": "99887766", "account_name": "汽车号",
                },
                db_path=self.db,
            )
        self.assertEqual(
            pending_content_ids(
                start=self.start, end=self.end, as_of=self.end,
                db_path=self.db, limit=1,
            ),
            [2],
        )
        calls: list[tuple[str, str]] = []

        def content_call(stage, content):
            calls.append((stage, str(content["platform_content_id"])))
            if stage == "detail":
                data = {
                    "title": "详情", "body": "汽车详情",
                    "published_at": content["published_at"],
                    "account_uid": "99887766", "account_name": "汽车号",
                    "content_type": "video", "media_urls": [],
                }
            elif stage == "metrics":
                data = {
                    "view_count": 100, "comment_count": 2, "like_count": 10,
                    "share_count": 1, "collect_count": None,
                }
            else:
                data = {"comment_count": 0, "comments": []}
            return ProviderResult(data, {"stage": stage, "data": data}, 200, True)

        first = run_content_backfill(
            start=self.start, end=self.end, task_id=self.task_id,
            max_amount=0.10, db_path=self.db, call_override=content_call,
            state_root=self.state,
        )
        self.assertEqual(first["status"], "succeeded")
        self.assertEqual(first["processed"], 2)
        self.assertEqual(first["usage"]["amount"], 0.006)
        self.assertEqual(
            [content_id for stage, content_id in calls if stage == "detail"],
            ["222222222", "111111111"],
        )
        with connect(self.db) as connection:
            task_ids = {
                row["task_id"] for row in connection.execute("SELECT task_id FROM provider_usage")
            }
        self.assertEqual(task_ids, {self.task_id})
        self.assertEqual(int(account["id"]), 1)

        second = run_content_backfill(
            start=self.start, end=self.end, task_id=self.task_id,
            max_amount=0.10, db_path=self.db, call_override=content_call,
            state_root=self.state,
        )
        self.assertEqual(second["candidates"], 0)
        self.assertEqual(second["usage"]["amount"], 0.006)

    def test_placeholder_metric_repair_is_dry_run_safe_and_reuses_slot(self) -> None:
        upsert_account(
            {
                "phone": "13800138009",
                "platforms": [{
                    "platform": "douyin", "uid": "99887769",
                    "nickname": "曝光修复号",
                }],
            },
            db_path=self.db,
        )
        content = upsert_content(
            {
                "platform": "douyin", "platform_content_id": "333333333",
                "canonical_url": "https://www.douyin.com/video/333333333",
                "title": "曝光占位", "body": "汽车内容",
                "published_at": "2026-08-02T01:00:00Z", "content_type": "video",
                "account_uid": "99887769", "account_name": "曝光修复号",
            },
            db_path=self.db,
        )
        with connect(self.db) as connection:
            slot = connection.execute(
                """
                INSERT INTO fetch_slots(
                    content_id,stage,window_key,provider,adapter_version,status,
                    attempt_count,created_at,updated_at
                ) VALUES (?, 'metrics', '2026-08-03', 'TikHub',
                          'tikhub-discovery-derived-v8.1', 'succeeded', 1,
                          '2026-08-03T00:00:00Z','2026-08-03T00:00:00Z')
                """,
                (content["id"],),
            )
            connection.execute(
                """
                INSERT INTO content_metric_snapshots(
                    content_id,captured_at,window_key,view_count,comment_count,
                    like_count,status,source,metadata_json
                ) VALUES (?, '2026-08-03T00:00:00Z','2026-08-03',0,3,20,
                          'available','douyin','{}')
                """,
                (content["id"],),
            )
            connection.commit()
            slot_id = int(slot.lastrowid)

        dry_run = repair_discovery_placeholder_metrics(
            start=self.start, end=self.end, as_of=self.end,
            db_path=self.db, platforms=["douyin"],
        )
        self.assertEqual(dry_run["status"], "dry_run")
        self.assertEqual(dry_run["candidates"], 1)
        with connect(self.db) as connection:
            self.assertEqual(
                connection.execute(
                    "SELECT status FROM fetch_slots WHERE id=?", (slot_id,)
                ).fetchone()[0],
                "succeeded",
            )

        applied = repair_discovery_placeholder_metrics(
            start=self.start, end=self.end, as_of=self.end,
            db_path=self.db, platforms=["douyin"], apply_changes=True,
        )
        repeated = repair_discovery_placeholder_metrics(
            start=self.start, end=self.end, as_of=self.end,
            db_path=self.db, platforms=["douyin"], apply_changes=True,
        )
        self.assertEqual(applied["applied"], 1)
        self.assertEqual(repeated["candidates"], 0)
        with connect(self.db) as connection:
            repaired_slot = connection.execute(
                "SELECT * FROM fetch_slots WHERE id=?", (slot_id,)
            ).fetchone()
            repaired_snapshot = connection.execute(
                "SELECT * FROM content_metric_snapshots WHERE content_id=?",
                (content["id"],),
            ).fetchone()
        self.assertEqual(repaired_slot["status"], "retryable_failed")
        self.assertEqual(repaired_slot["attempt_count"], 1)
        self.assertIsNone(repaired_snapshot["view_count"])
        self.assertEqual(repaired_snapshot["comment_count"], 3)
        self.assertEqual(repaired_snapshot["like_count"], 20)
        self.assertEqual(repaired_snapshot["status"], "missing")

        calls: list[str] = []

        def statistics_call(stage, content_row):
            calls.append(stage)
            return ProviderResult(
                {
                    "view_count": 4321, "comment_count": 3,
                    "like_count": 20, "share_count": 1,
                    "collect_count": 2,
                },
                {"stage": stage, "view_count": 4321},
                200,
                True,
            )

        task_id = "placeholder-metrics-repair-test"
        fetched = run_repaired_metrics_backfill(
            start=self.start, end=self.end, as_of=self.end,
            task_id=task_id, max_amount=0.01, db_path=self.db,
            platforms=["douyin"], call_override=statistics_call,
            state_root=self.state,
        )
        second_fetch = run_repaired_metrics_backfill(
            start=self.start, end=self.end, as_of=self.end,
            task_id=task_id, max_amount=0.01, db_path=self.db,
            platforms=["douyin"], call_override=statistics_call,
            state_root=self.state,
        )
        self.assertEqual(calls, ["metrics"])
        self.assertEqual(fetched["usage"]["amount"], 0.001)
        self.assertEqual(second_fetch["candidates"], 0)
        with connect(self.db) as connection:
            final_slot = connection.execute(
                "SELECT * FROM fetch_slots WHERE id=?", (slot_id,)
            ).fetchone()
            final_snapshot = connection.execute(
                "SELECT * FROM content_metric_snapshots WHERE content_id=?",
                (content["id"],),
            ).fetchone()
        self.assertEqual(final_slot["status"], "succeeded")
        self.assertEqual(final_slot["attempt_count"], 2)
        self.assertEqual(final_slot["adapter_version"], "tikhub-statistics-v8.0")
        self.assertEqual(final_snapshot["view_count"], 4321)
        self.assertEqual(final_snapshot["status"], "available")


if __name__ == "__main__":
    unittest.main()
