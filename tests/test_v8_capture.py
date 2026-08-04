from __future__ import annotations

import hashlib
import json
import tempfile
import unittest
from datetime import datetime, timezone
from pathlib import Path
from unittest.mock import patch

from v8.capture import (
    BudgetBlocked,
    CaptureError,
    ProviderResult,
    RawResponseIntegrityError,
    SlotUnavailable,
    activate_pilot_budget,
    evaluate_pilot_gate,
    execute_account_fetch,
    execute_content_fetch,
    load_succeeded_raw_response,
    recover_stale_fetch_slots,
)
from v8.storage import connect, initialize_database, now_utc


class V8CaptureTest(unittest.TestCase):
    def setUp(self) -> None:
        self.temp = tempfile.TemporaryDirectory()
        self.root = Path(self.temp.name)
        self.db = self.root / "capture.sqlite3"
        self.raw = self.root / "raw"
        captured_at = now_utc()
        with connect(self.db) as connection:
            initialize_database(connection)
            connection.execute(
                """
                INSERT INTO content_items(
                    link_id, platform, platform_content_id, canonical_url, title,
                    content_type, imported_at, created_at, updated_at
                ) VALUES ('A2BC3D', 'xiaohongshu', 'abc123',
                          'https://www.xiaohongshu.com/explore/abc123', '', 'video', ?, ?, ?)
                """,
                (captured_at, captured_at, captured_at),
            )
            connection.execute(
                """
                INSERT INTO provider_budget_batches(
                    id, purpose, provider, operation, currency, verified_unit_price,
                    max_billable_requests, max_amount, pilot_size, daily_quota,
                    price_verified_at, status, created_at, updated_at
                ) VALUES ('pilot', 'test', 'Rnote', 'xiaohongshu_video_detail',
                          'USD', 0.008, 2, 0.016, 2, 2, ?, 'draft', ?, ?)
                """,
                (captured_at, captured_at, captured_at),
            )
            connection.execute(
                """
                INSERT INTO accounts(
                    phone, phone_normalized, operator_name, account_type,
                    content_direction, enabled, created_at, updated_at
                ) VALUES ('13800138000', '13800138000', '测试运营', 'unknown',
                          'unknown', 1, ?, ?)
                """,
                (captured_at, captured_at),
            )
            connection.commit()

    def tearDown(self) -> None:
        self.temp.cleanup()

    def test_startup_recovery_releases_only_stale_running_fetch_slots(self) -> None:
        with connect(self.db) as connection:
            connection.executemany(
                """
                INSERT INTO fetch_slots(
                    content_id,stage,window_key,provider,adapter_version,status,
                    attempt_count,started_at,created_at,updated_at
                ) VALUES (1,'metrics',?,'TikHub','statistics-v1','running',1,?,?,?)
                """,
                [
                    (
                        "stale", "2026-08-04T00:00:00Z",
                        "2026-08-04T00:00:00Z", "2026-08-04T00:00:00Z",
                    ),
                    (
                        "fresh", "2026-08-04T00:19:00Z",
                        "2026-08-04T00:19:00Z", "2026-08-04T00:19:00Z",
                    ),
                ],
            )
            connection.commit()
        result = recover_stale_fetch_slots(
            db_path=self.db,
            stale_after_seconds=600,
            current_time=datetime(2026, 8, 4, 0, 20, tzinfo=timezone.utc),
        )
        self.assertEqual(result, {"stale_candidates": 1, "recovered": 1})
        with connect(self.db) as connection:
            rows = {
                row["window_key"]: dict(row)
                for row in connection.execute(
                    "SELECT * FROM fetch_slots ORDER BY window_key"
                )
            }
        self.assertEqual(rows["stale"]["status"], "retryable_failed")
        self.assertEqual(rows["stale"]["last_error_code"], "interrupted")
        self.assertEqual(rows["fresh"]["status"], "running")

    def test_success_writes_sha256_raw_response_and_locks_slot(self) -> None:
        activate_pilot_budget("pilot", expected_unit_price=0.008, db_path=self.db)
        outcome = execute_content_fetch(
            content_id=1,
            stage="media_source_refresh",
            window_key="lifetime",
            provider="Rnote",
            adapter_version="rnote-video-v8.0",
            operation="xiaohongshu_video_detail",
            budget_id="pilot",
            db_path=self.db,
            raw_root=self.raw,
            call=lambda: ProviderResult(
                data={"video_urls": ["https://cdn.example/video.mp4"]},
                raw_response={"success": True, "token": "must-not-leak"},
                http_status=200,
                billed=True,
            ),
        )
        self.assertTrue(outcome.billed)
        self.assertEqual(outcome.amount, 0.008)
        with connect(self.db) as connection:
            slot = connection.execute("SELECT * FROM fetch_slots").fetchone()
            raw = connection.execute("SELECT * FROM provider_raw_responses").fetchone()
            budget = connection.execute(
                "SELECT * FROM provider_budget_batches WHERE id='pilot'"
            ).fetchone()
        self.assertEqual(slot["status"], "succeeded")
        path = Path(raw["local_path"])
        self.assertTrue(path.is_absolute())
        body = path.read_bytes()
        self.assertEqual(hashlib.sha256(body).hexdigest(), raw["sha256"])
        self.assertNotIn(b"must-not-leak", body)
        self.assertEqual(json.loads(body)["token"], "[REDACTED]")
        self.assertEqual(budget["consumed_requests"], 1)
        with self.assertRaises(SlotUnavailable):
            execute_content_fetch(
                content_id=1,
                stage="media_source_refresh",
                window_key="lifetime",
                provider="Rnote",
                adapter_version="rnote-video-v8.1",
                operation="xiaohongshu_video_detail",
                budget_id="pilot",
                db_path=self.db,
                raw_root=self.raw,
                call=lambda: ProviderResult({}, {}, 200, True),
            )
        with connect(self.db) as connection:
            self.assertEqual(connection.execute("SELECT COUNT(*) FROM fetch_slots").fetchone()[0], 1)
            usage_before_replay = connection.execute(
                "SELECT COUNT(*), SUM(amount) FROM provider_usage"
            ).fetchone()
        replayed = load_succeeded_raw_response(
            content_id=1,
            stage="media_source_refresh",
            window_key="lifetime",
            operation="xiaohongshu_video_detail",
            db_path=self.db,
        )
        self.assertEqual(replayed.raw_response_id, outcome.raw_response_id)
        self.assertEqual(replayed.value["token"], "[REDACTED]")
        with connect(self.db) as connection:
            usage_after_replay = connection.execute(
                "SELECT COUNT(*), SUM(amount) FROM provider_usage"
            ).fetchone()
        self.assertEqual(tuple(usage_after_replay), tuple(usage_before_replay))

        replayed.local_path.write_bytes(b'{"tampered":true}\n')
        with self.assertRaises(RawResponseIntegrityError):
            load_succeeded_raw_response(
                content_id=1,
                stage="media_source_refresh",
                window_key="lifetime",
                db_path=self.db,
            )

    def test_failed_attempt_is_retryable_and_not_billed(self) -> None:
        activate_pilot_budget("pilot", expected_unit_price=0.008, db_path=self.db)
        with self.assertRaises(CaptureError):
            execute_content_fetch(
                content_id=1,
                stage="media_source_refresh",
                window_key="lifetime",
                provider="Rnote",
                adapter_version="rnote-video-v8.0",
                operation="xiaohongshu_video_detail",
                budget_id="pilot",
                db_path=self.db,
                raw_root=self.raw,
                call=lambda: (_ for _ in ()).throw(
                    CaptureError(
                        "upstream unavailable",
                        retryable=True,
                        error_code="http_503",
                        http_status=503,
                        billed=False,
                        raw_response={"error": "busy"},
                    )
                ),
            )
        outcome = execute_content_fetch(
            content_id=1,
            stage="media_source_refresh",
            window_key="lifetime",
            provider="Rnote",
            adapter_version="rnote-video-v8.0",
            operation="xiaohongshu_video_detail",
            budget_id="pilot",
            db_path=self.db,
            raw_root=self.raw,
            call=lambda: ProviderResult({"ok": True}, {"ok": True}, 200, True),
        )
        self.assertTrue(outcome.billed)
        with connect(self.db) as connection:
            attempts = connection.execute(
                "SELECT billed, error_code FROM fetch_attempts ORDER BY attempt_number"
            ).fetchall()
            budget = connection.execute(
                "SELECT * FROM provider_budget_batches WHERE id='pilot'"
            ).fetchone()
        self.assertEqual([(row["billed"], row["error_code"]) for row in attempts], [(0, "http_503"), (1, None)])
        self.assertEqual(budget["consumed_requests"], 1)
        self.assertEqual(budget["status"], "suspended")

    def test_daily_quota_uses_shanghai_calendar_day(self) -> None:
        with connect(self.db) as connection:
            connection.execute(
                """
                UPDATE provider_budget_batches
                SET status='approved', max_billable_requests=10, max_amount=0.08,
                    daily_quota=1
                WHERE id='pilot'
                """
            )
            connection.commit()

        with patch("v8.capture.now_utc", return_value="2026-08-02T15:59:59Z"):
            execute_content_fetch(
                content_id=1,
                stage="media_source_refresh",
                window_key="beijing-2026-08-02",
                provider="Rnote",
                adapter_version="rnote-video-v8.0",
                operation="xiaohongshu_video_detail",
                budget_id="pilot",
                db_path=self.db,
                raw_root=self.raw,
                call=lambda: ProviderResult(
                    {"page": 1}, {"page": 1}, 200, True
                ),
            )
        with patch("v8.capture.now_utc", return_value="2026-08-02T16:00:00Z"):
            execute_content_fetch(
                content_id=1,
                stage="media_source_refresh",
                window_key="beijing-2026-08-03",
                provider="Rnote",
                adapter_version="rnote-video-v8.0",
                operation="xiaohongshu_video_detail",
                budget_id="pilot",
                db_path=self.db,
                raw_root=self.raw,
                call=lambda: ProviderResult(
                    {"page": 2}, {"page": 2}, 200, True
                ),
            )
        with (
            patch("v8.capture.now_utc", return_value="2026-08-03T15:00:00Z"),
            self.assertRaises(BudgetBlocked),
        ):
            execute_content_fetch(
                content_id=1,
                stage="media_source_refresh",
                window_key="beijing-2026-08-03-second",
                provider="Rnote",
                adapter_version="rnote-video-v8.0",
                operation="xiaohongshu_video_detail",
                budget_id="pilot",
                db_path=self.db,
                raw_root=self.raw,
                call=lambda: ProviderResult({}, {"unexpected": True}, 200, True),
            )
        with connect(self.db) as connection:
            usage = connection.execute(
                "SELECT recorded_at FROM provider_usage ORDER BY id"
            ).fetchall()
        self.assertEqual(
            [row["recorded_at"] for row in usage],
            ["2026-08-02T15:59:59Z", "2026-08-02T16:00:00Z"],
        )

    def test_task_amount_ceiling_is_fail_closed_before_provider_call(self) -> None:
        with connect(self.db) as connection:
            connection.execute(
                """
                UPDATE provider_budget_batches
                SET status='approved', max_billable_requests=10, max_amount=0.08,
                    daily_quota=10
                WHERE id='pilot'
                """
            )
            connection.commit()
        execute_content_fetch(
            content_id=1,
            stage="media_source_refresh",
            window_key="range-page-1",
            provider="Rnote",
            adapter_version="rnote-video-v8.0",
            operation="xiaohongshu_video_detail",
            budget_id="pilot",
            task_id="backfill-2026-07-20",
            task_max_amount=0.012,
            db_path=self.db,
            raw_root=self.raw,
            call=lambda: ProviderResult({"page": 1}, {"page": 1}, 200, True),
        )
        provider_called = False

        def unexpected_call() -> ProviderResult:
            nonlocal provider_called
            provider_called = True
            return ProviderResult({}, {}, 200, True)

        with self.assertRaises(BudgetBlocked):
            execute_content_fetch(
                content_id=1,
                stage="media_source_refresh",
                window_key="range-page-2",
                provider="Rnote",
                adapter_version="rnote-video-v8.0",
                operation="xiaohongshu_video_detail",
                budget_id="pilot",
                task_id="backfill-2026-07-20",
                task_max_amount=0.012,
                db_path=self.db,
                raw_root=self.raw,
                call=unexpected_call,
            )
        self.assertFalse(provider_called)
        with connect(self.db) as connection:
            usage = connection.execute("SELECT * FROM provider_usage").fetchall()
            blocked_slot = connection.execute(
                "SELECT status,last_error_code FROM fetch_slots WHERE window_key='range-page-2'"
            ).fetchone()
        self.assertEqual(len(usage), 1)
        self.assertEqual(usage[0]["task_id"], "backfill-2026-07-20")
        self.assertEqual(usage[0]["amount"], 0.008)
        self.assertEqual(
            (blocked_slot["status"], blocked_slot["last_error_code"]),
            ("retryable_failed", "budget_blocked"),
        )

    def test_account_fetch_records_task_and_replays_raw_without_cost(self) -> None:
        with connect(self.db) as connection:
            connection.execute(
                """
                UPDATE provider_budget_batches
                SET status='approved', max_billable_requests=10, max_amount=0.08,
                    daily_quota=10
                WHERE id='pilot'
                """
            )
            connection.commit()
        outcome = execute_account_fetch(
            account_id=1,
            stage="discovery",
            window_key="backfill:first-page",
            provider="Rnote",
            adapter_version="rnote-user-posts-v8.0",
            operation="xiaohongshu_video_detail",
            budget_id="pilot",
            task_id="backfill-account-test",
            task_max_amount=0.008,
            db_path=self.db,
            raw_root=self.raw,
            call=lambda: ProviderResult(
                {"items": [{"id": "note-1"}]},
                {"data": {"items": [{"id": "note-1"}]}},
                200,
                True,
            ),
        )
        replayed = load_succeeded_raw_response(
            account_id=1,
            stage="discovery",
            window_key="backfill:first-page",
            operation="xiaohongshu_video_detail",
            db_path=self.db,
        )
        self.assertEqual(replayed.raw_response_id, outcome.raw_response_id)
        self.assertEqual(replayed.value["data"]["items"][0]["id"], "note-1")
        with connect(self.db) as connection:
            usage = connection.execute("SELECT * FROM provider_usage").fetchone()
        self.assertEqual(usage["task_id"], "backfill-account-test")
        self.assertEqual(usage["amount"], 0.008)

    def test_budget_is_fail_closed_and_quality_gate_is_quantified(self) -> None:
        with self.assertRaises(BudgetBlocked):
            execute_content_fetch(
                content_id=1,
                stage="media_source_refresh",
                window_key="lifetime",
                provider="Rnote",
                adapter_version="rnote-video-v8.0",
                operation="xiaohongshu_video_detail",
                budget_id="pilot",
                db_path=self.db,
                raw_root=self.raw,
                call=lambda: ProviderResult({}, {}, 200, True),
            )
        with connect(self.db) as connection:
            self.assertEqual(connection.execute("SELECT COUNT(*) FROM provider_usage").fetchone()[0], 0)
            self.assertEqual(connection.execute("SELECT status FROM fetch_slots").fetchone()[0], "retryable_failed")

        with connect(self.db) as connection:
            connection.execute(
                "UPDATE provider_budget_batches SET status='suspended' WHERE id='pilot'"
            )
            connection.commit()
        rejected = evaluate_pilot_gate(
            "pilot", attempted=20, media_recovered=13, evidence_ready=12, db_path=self.db
        )
        self.assertFalse(rejected["approved"])
        with connect(self.db) as connection:
            self.assertEqual(
                connection.execute("SELECT status FROM provider_budget_batches").fetchone()[0],
                "suspended",
            )


if __name__ == "__main__":
    unittest.main()
