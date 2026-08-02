from __future__ import annotations

import hashlib
import json
import tempfile
import unittest
from pathlib import Path

from v8.capture import (
    BudgetBlocked,
    CaptureError,
    ProviderResult,
    SlotUnavailable,
    activate_pilot_budget,
    evaluate_pilot_gate,
    execute_content_fetch,
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
            connection.commit()

    def tearDown(self) -> None:
        self.temp.cleanup()

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
