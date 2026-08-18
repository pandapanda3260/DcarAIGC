from __future__ import annotations

import contextlib
import inspect
import io
import json
import os
import tempfile
import unittest
from datetime import datetime
from pathlib import Path
from unittest.mock import patch
from zoneinfo import ZoneInfo

import v8.capture as capture_module
import v8.range_backfill as range_module
from v8.capture import CaptureError, ProviderResult
from v8.operations import upsert_account, upsert_content
from v8.range_backfill import (
    RangeBackfillError,
    _iso,
    _parse_datetime,
    main,
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
        self.raw_root = self.root / "raw"
        raw_root_patch = patch.object(capture_module, "RAW_ROOT", self.raw_root)
        raw_root_patch.start()
        self.addCleanup(raw_root_patch.stop)
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
            call_override=discovery_call, state_root=self.state, as_of=self.end,
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
            call_override=no_provider_call, state_root=self.state, as_of=self.end,
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
            as_of=self.end,
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
            state_root=self.state, as_of=self.end,
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
            state_root=self.state, as_of=self.end,
        )
        self.assertEqual(second["candidates"], 0)
        self.assertEqual(second["usage"]["amount"], 0.006)

    def test_comment_pending_state_uses_capture_run_not_legacy_slot(self) -> None:
        upsert_account(
            {
                "phone": "13800138011",
                "platforms": [
                    {"platform": "douyin", "uid": "99887771", "nickname": "汽车号"}
                ],
            },
            db_path=self.db,
        )
        content = upsert_content(
            {
                "platform": "douyin",
                "platform_content_id": "444444444",
                "canonical_url": "https://www.douyin.com/video/444444444",
                "title": "旧周槽",
                "body": "汽车内容",
                "published_at": "2026-08-02T01:00:00Z",
                "content_type": "video",
                "account_uid": "99887771",
                "account_name": "汽车号",
            },
            db_path=self.db,
        )
        with connect(self.db) as connection:
            connection.execute(
                """
                INSERT INTO fetch_slots(
                    content_id,stage,window_key,provider,adapter_version,status,
                    created_at,updated_at
                ) VALUES (?,'comments','2026-W32','legacy-cache','legacy-cache',
                          'succeeded','2026-08-03T00:00:00Z','2026-08-03T00:00:00Z')
                """,
                (content["id"],),
            )
            connection.commit()
        pending = pending_content_ids(
            start=self.start,
            end=self.end,
            as_of=self.end,
            db_path=self.db,
            stages=["comments"],
        )
        self.assertEqual(pending, [int(content["id"])])

        with connect(self.db) as connection:
            connection.execute(
                """
                INSERT INTO comment_capture_runs(
                    content_id,window_key,provider,adapter_version,status,
                    completion_kind,created_at,updated_at,completed_at
                ) VALUES (?,'2026-W32','TikHub',
                          'tikhub-comments-v8.0+paged-comments-v2','succeeded',
                          'provider_exhausted','2026-08-03T01:00:00Z',
                          '2026-08-03T01:00:00Z','2026-08-03T01:00:00Z')
                """,
                (content["id"],),
            )
            connection.commit()
        self.assertEqual(
            pending_content_ids(
                start=self.start,
                end=self.end,
                as_of=self.end,
                db_path=self.db,
                stages=["comments"],
            ),
            [],
        )

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

        scoped_before_tag = repair_discovery_placeholder_metrics(
            start=self.start,
            end=self.end,
            as_of=self.end,
            db_path=self.db,
            platforms=["douyin"],
            history_only=True,
        )
        self.assertEqual(scoped_before_tag["candidates"], 0)
        with connect(self.db) as connection:
            connection.execute(
                "UPDATE content_items SET source_group='history-backfill' WHERE id=?",
                (content["id"],),
            )
            connection.commit()

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
            repaired_observations = connection.execute(
                """
                SELECT observation_origin,captured_at,view_count,status
                FROM content_metric_observations WHERE content_id=? ORDER BY id
                """,
                (content["id"],),
            ).fetchall()
        self.assertEqual(repaired_slot["status"], "retryable_failed")
        self.assertEqual(repaired_slot["attempt_count"], 1)
        self.assertIsNone(repaired_snapshot["view_count"])
        self.assertEqual(repaired_snapshot["comment_count"], 3)
        self.assertEqual(repaired_snapshot["like_count"], 20)
        self.assertEqual(repaired_snapshot["status"], "missing")
        self.assertEqual(
            [tuple(row) for row in repaired_observations],
            [("system_correction", "2026-08-03T00:00:00Z", None, "missing")],
        )

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
            state_root=self.state, history_only=True,
        )
        second_fetch = run_repaired_metrics_backfill(
            start=self.start, end=self.end, as_of=self.end,
            task_id=task_id, max_amount=0.01, db_path=self.db,
            platforms=["douyin"], call_override=statistics_call,
            state_root=self.state, history_only=True,
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
            final_observations = connection.execute(
                """
                SELECT observation_origin,view_count,status
                FROM content_metric_observations WHERE content_id=? ORDER BY id
                """,
                (content["id"],),
            ).fetchall()
        self.assertEqual(final_slot["status"], "succeeded")
        self.assertEqual(final_slot["attempt_count"], 2)
        self.assertEqual(final_slot["adapter_version"], "tikhub-statistics-v8.0")
        self.assertEqual(final_snapshot["view_count"], 4321)
        self.assertEqual(final_snapshot["status"], "available")
        self.assertEqual(
            [tuple(row) for row in final_observations],
            [
                ("system_correction", None, "missing"),
                ("provider_capture", 4321, "available"),
            ],
        )

    def test_iso_preserves_fractional_second_end_boundary(self) -> None:
        fractional = datetime.fromisoformat("2026-08-17T23:59:59.999999+08:00")
        exact = datetime.fromisoformat("2026-08-17T23:59:59+08:00")
        self.assertEqual(_iso(fractional), "2026-08-17T15:59:59.999999Z")
        self.assertEqual(_iso(exact), "2026-08-17T15:59:59Z")

    def test_parse_datetime_rejects_timezone_less_values(self) -> None:
        with self.assertRaisesRegex(RangeBackfillError, "必须包含时区"):
            _parse_datetime("2026-08-17T23:59:59.999999")

    def test_mutating_range_functions_require_explicit_as_of(self) -> None:
        for function in (
            range_module.run_discovery_backfill,
            range_module.run_content_backfill,
            range_module.run_local_evidence_backfill,
        ):
            signature = inspect.signature(function)
            self.assertIs(
                signature.parameters["as_of"].default,
                inspect.Parameter.empty,
                function.__name__,
            )
            with self.assertRaises(TypeError, msg=function.__name__):
                signature.bind(
                    start=self.start,
                    end=self.end,
                    task_id="explicit-as-of-contract",
                    max_amount=0.10,
                )

    def test_cli_requires_explicit_start_and_as_of_before_dispatch(self) -> None:
        with patch.object(range_module, "summarize_range_status") as summarize:
            with contextlib.redirect_stderr(io.StringIO()):
                with self.assertRaises(SystemExit) as missing_start:
                    main(
                        [
                            "status",
                            "--end",
                            self.end.isoformat(),
                            "--as-of",
                            self.end.isoformat(),
                            "--db",
                            str(self.db),
                        ]
                    )
                with self.assertRaises(SystemExit) as missing_as_of:
                    main(
                        [
                            "status",
                            "--start",
                            self.start.isoformat(),
                            "--end",
                            self.end.isoformat(),
                            "--db",
                            str(self.db),
                        ]
                    )
        self.assertEqual(missing_start.exception.code, 2)
        self.assertEqual(missing_as_of.exception.code, 2)
        summarize.assert_not_called()

    def test_campaign_contract_is_anchored_before_db_or_provider_work(self) -> None:
        upsert_account(
            {
                "phone": "13800138012",
                "platforms": [
                    {
                        "platform": "xiaohongshu",
                        "uid": "67f6657f000000000e02c22c",
                        "nickname": "合同测试号",
                    }
                ],
            },
            db_path=self.db,
        )
        task_id = "range-contract-test"
        as_of = datetime.fromisoformat("2026-08-18T12:34:56.123456+08:00")
        provider_calls = 0

        def discovery_call(operation, identity):
            nonlocal provider_calls
            provider_calls += 1
            return ProviderResult(
                {"items": [], "next_cursor": None, "has_more": False},
                {"items": []},
                200,
                True,
            )

        result = run_discovery_backfill(
            start=self.start,
            end=self.end,
            as_of=as_of,
            task_id=task_id,
            max_amount=0.10,
            max_pages_per_account=8,
            db_path=self.db,
            platforms=["xiaohongshu"],
            call_override=discovery_call,
            state_root=self.state,
        )
        self.assertEqual(result["status"], "succeeded")
        self.assertEqual(provider_calls, 1)
        state_path = self.state / f"{task_id}.json"
        state = json.loads(state_path.read_text(encoding="utf-8"))
        self.assertEqual(
            state["contract"],
            {
                "task_id": task_id,
                "start": _iso(self.start),
                "end": _iso(self.end),
                "as_of": _iso(as_of),
                "max_amount": 0.10,
                "max_pages": 8,
                "platforms": ["xiaohongshu"],
            },
        )
        self.assertEqual(state["phase_contracts"]["discover"]["phase"], "discover")

        run_content_backfill(
            start=self.start,
            end=self.end,
            as_of=as_of,
            task_id=task_id,
            max_amount=0.10,
            contract_max_pages=8,
            db_path=self.db,
            platforms=["xiaohongshu"],
            stages=["detail"],
            call_override=lambda stage, content: (_ for _ in ()).throw(
                AssertionError("empty discovery must not create content work")
            ),
            state_root=self.state,
        )
        state = json.loads(state_path.read_text(encoding="utf-8"))
        self.assertEqual(
            set(state["phase_contracts"]), {"discover", "content"}
        )

        with connect(self.db) as connection:
            before = tuple(
                connection.execute(
                    "SELECT COUNT(*),COALESCE(SUM(amount),0) FROM provider_usage"
                ).fetchone()
            )
        mismatches = (
            {"as_of": datetime.fromisoformat("2026-08-18T12:34:57+08:00")},
            {"max_amount": 0.20},
            {"max_pages_per_account": 9},
            {"platforms": ["douyin"]},
        )
        for mismatch in mismatches:
            arguments = {
                "start": self.start,
                "end": self.end,
                "as_of": as_of,
                "task_id": task_id,
                "max_amount": 0.10,
                "max_pages_per_account": 8,
                "db_path": self.db,
                "platforms": ["xiaohongshu"],
                "call_override": discovery_call,
                "state_root": self.state,
            }
            arguments.update(mismatch)
            with (
                patch.object(
                    range_module,
                    "_enabled_identities",
                    side_effect=AssertionError("DB work happened before contract rejection"),
                ),
                self.assertRaisesRegex(RangeBackfillError, "完整合同"),
            ):
                run_discovery_backfill(**arguments)
        self.assertEqual(provider_calls, 1)
        with connect(self.db) as connection:
            after = tuple(
                connection.execute(
                    "SELECT COUNT(*),COALESCE(SUM(amount),0) FROM provider_usage"
                ).fetchone()
            )
        self.assertEqual(after, before)

    def test_formal_mutation_requires_freeze_before_state_db_or_provider(self) -> None:
        missing_freeze = self.root / "missing-freeze.lock"
        provider_called = False

        def unexpected_call(operation, identity):
            nonlocal provider_called
            provider_called = True
            raise AssertionError("provider must not run without freeze")

        state_root = self.root / "formal-state"
        with (
            patch.object(range_module, "DEFAULT_DB", self.db),
            patch.dict(
                os.environ,
                {"DCAR_OPERATOR_FREEZE_LOCK": str(missing_freeze)},
                clear=False,
            ),
            patch.object(
                range_module,
                "_enabled_identities",
                side_effect=AssertionError("DB work must not run without freeze"),
            ),
            self.assertRaisesRegex(RangeBackfillError, "operator freeze lock"),
        ):
            run_discovery_backfill(
                start=self.start,
                end=self.end,
                as_of=self.end,
                task_id="formal-freeze-test",
                max_amount=0.10,
                db_path=self.db,
                platforms=["xiaohongshu"],
                call_override=unexpected_call,
                state_root=state_root,
            )
        self.assertFalse(provider_called)
        self.assertFalse(state_root.exists())
        with connect(self.db) as connection:
            self.assertEqual(
                connection.execute("SELECT COUNT(*) FROM provider_usage").fetchone()[0],
                0,
            )

    def test_formal_mutation_alias_cannot_bypass_freeze(self) -> None:
        alias = self.root / "apfs-firmlink-spelling.sqlite3"
        alias.hardlink_to(self.db)
        missing_freeze = self.root / "missing-alias-freeze.lock"
        with (
            patch.object(range_module, "DEFAULT_DB", self.db),
            patch.dict(
                os.environ,
                {"DCAR_OPERATOR_FREEZE_LOCK": str(missing_freeze)},
                clear=False,
            ),
            self.assertRaisesRegex(RangeBackfillError, "operator freeze lock"),
        ):
            range_module._require_formal_mutation_freeze(db_path=alias)

    def test_detail_only_backfill_does_not_create_metric_snapshots(self) -> None:
        upsert_account(
            {
                "phone": "13800138013",
                "platforms": [
                    {"platform": "douyin", "uid": "99887773", "nickname": "详情号"}
                ],
            },
            db_path=self.db,
        )
        upsert_content(
            {
                "platform": "douyin",
                "platform_content_id": "555555555",
                "canonical_url": "https://www.douyin.com/video/555555555",
                "title": "待补详情",
                "body": "汽车内容",
                "published_at": "2026-08-02T01:00:00Z",
                "content_type": "video",
                "account_uid": "99887773",
                "account_name": "详情号",
            },
            db_path=self.db,
        )

        def detail_call(stage, content):
            self.assertEqual(stage, "detail")
            return ProviderResult(
                {
                    "title": "实时详情",
                    "body": "实时详情正文",
                    "published_at": content["published_at"],
                    "account_uid": "99887773",
                    "account_name": "详情号",
                    "content_type": "video",
                    "media_urls": [],
                },
                {"stage": stage},
                200,
                True,
            )

        result = run_content_backfill(
            start=self.start,
            end=self.end,
            as_of=datetime.fromisoformat("2026-08-18T15:00:00+08:00"),
            task_id="detail-only-test",
            max_amount=0.10,
            db_path=self.db,
            platforms=["douyin"],
            stages=["detail"],
            call_override=detail_call,
            state_root=self.state,
        )
        self.assertEqual(result["status"], "succeeded")
        with connect(self.db) as connection:
            self.assertEqual(
                connection.execute(
                    "SELECT COUNT(*) FROM content_metric_snapshots"
                ).fetchone()[0],
                0,
            )


if __name__ == "__main__":
    unittest.main()
