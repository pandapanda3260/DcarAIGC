from __future__ import annotations

import hashlib
import json
import stat
import tempfile
import threading
import unittest
from concurrent.futures import ThreadPoolExecutor
from datetime import datetime, timezone
from pathlib import Path
from unittest.mock import patch

import v8.capture as capture_module
from v8.capture import (
    BudgetBlocked,
    CaptureError,
    DailyAttemptQuotaExhausted,
    ProviderResult,
    RawResponseIntegrityError,
    SlotUnavailable,
    TaskBudgetExhausted,
    activate_pilot_budget,
    evaluate_pilot_gate,
    execute_account_fetch,
    execute_content_fetch,
    load_succeeded_raw_response,
    mark_succeeded_fetch_slot_retryable_failure,
    recover_stale_fetch_slots,
)
from v8.storage import connect, initialize_database, now_utc


def _tree_metadata_inventory(root: Path) -> tuple[str, int]:
    digest = hashlib.sha256()
    if not root.exists():
        digest.update(b"missing\n")
        return digest.hexdigest(), 0
    pending = [root]
    entries = 0
    while pending:
        path = pending.pop()
        metadata = path.lstat()
        relative = "." if path == root else path.relative_to(root).as_posix()
        if stat.S_ISDIR(metadata.st_mode):
            kind = "directory"
            pending.extend(
                reversed(sorted(path.iterdir(), key=lambda child: child.name))
            )
        elif stat.S_ISREG(metadata.st_mode):
            kind = "file"
        elif stat.S_ISLNK(metadata.st_mode):
            kind = "symlink"
        else:
            kind = "other"
        digest.update(
            json.dumps(
                {
                    "relative_path": relative,
                    "kind": kind,
                    "mode": metadata.st_mode,
                    "device": metadata.st_dev,
                    "inode": metadata.st_ino,
                    "nlink": metadata.st_nlink,
                    "byte_size": metadata.st_size,
                    "mtime_ns": metadata.st_mtime_ns,
                    "ctime_ns": metadata.st_ctime_ns,
                },
                sort_keys=True,
                separators=(",", ":"),
            ).encode("utf-8")
        )
        digest.update(b"\n")
        entries += 1
    return digest.hexdigest(), entries


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

    def _insert_task_budget(
        self,
        *,
        task_id: str,
        operation: str,
        max_amount: float,
        provider: str = "Rnote",
        unit_price: float = 0.008,
    ) -> str:
        digest = hashlib.sha256(task_id.encode("utf-8")).hexdigest()[:16]
        budget_id = f"task-{digest}-{provider.lower()}-{operation}-v1"
        captured_at = now_utc()
        with connect(self.db) as connection:
            connection.execute(
                """
                INSERT INTO provider_budget_batches(
                    id,purpose,provider,operation,currency,verified_unit_price,
                    max_billable_requests,max_amount,pilot_size,daily_quota,
                    price_verified_at,status,created_at,updated_at
                ) VALUES (?,?,?,?,'USD',?,100,?,0,100,?,'approved',?,?)
                """,
                (
                    budget_id,
                    f"test_task_{digest}_{operation}",
                    provider,
                    operation,
                    unit_price,
                    max_amount,
                    captured_at,
                    captured_at,
                    captured_at,
                ),
            )
            connection.commit()
        return budget_id

    def tearDown(self) -> None:
        self.temp.cleanup()

    def test_runtime_default_raw_root_isolated_without_canonical_drift(self) -> None:
        canonical_before = _tree_metadata_inventory(capture_module.RAW_ROOT)
        isolated_raw_root = self.root / "runtime-default-raw"
        with patch.object(capture_module, "RAW_ROOT", isolated_raw_root):
            outcome = execute_content_fetch(
                content_id=1,
                stage="detail",
                window_key="lifetime",
                provider="TestProvider",
                adapter_version="test-provider-v1",
                operation="runtime_default_raw_root",
                db_path=self.db,
                call=lambda: ProviderResult(
                    data={"ok": True},
                    raw_response={"fixture": "isolated"},
                    http_status=200,
                    billed=False,
                ),
            )
        canonical_after = _tree_metadata_inventory(capture_module.RAW_ROOT)
        self.assertEqual(canonical_after, canonical_before)
        with connect(self.db) as connection:
            raw = connection.execute(
                "SELECT local_path FROM provider_raw_responses WHERE id=?",
                (outcome.raw_response_id,),
            ).fetchone()
        stored_path = Path(str(raw["local_path"]))
        self.assertEqual(stored_path.parents[3], isolated_raw_root)
        self.assertTrue(stored_path.is_file())

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

    def test_task_amount_ceiling_spans_operations_before_provider_call(self) -> None:
        task_id = "backfill-2026-07-20"
        max_amount = 0.012
        first_operation = "xiaohongshu_video_detail"
        second_operation = "xiaohongshu_note_statistics"
        first_budget = self._insert_task_budget(
            task_id=task_id,
            operation=first_operation,
            max_amount=max_amount,
        )
        second_budget = self._insert_task_budget(
            task_id=task_id,
            operation=second_operation,
            max_amount=max_amount,
        )
        execute_content_fetch(
            content_id=1,
            stage="media_source_refresh",
            window_key="range-page-1",
            provider="Rnote",
            adapter_version="rnote-video-v8.0",
            operation=first_operation,
            budget_id=first_budget,
            task_id=task_id,
            task_max_amount=max_amount,
            db_path=self.db,
            raw_root=self.raw,
            call=lambda: ProviderResult({"page": 1}, {"page": 1}, 200, True),
        )
        provider_called = False

        def unexpected_call() -> ProviderResult:
            nonlocal provider_called
            provider_called = True
            return ProviderResult({}, {}, 200, True)

        with self.assertRaises(TaskBudgetExhausted):
            execute_content_fetch(
                content_id=1,
                stage="media_source_refresh",
                window_key="range-page-2",
                provider="Rnote",
                adapter_version="rnote-video-v8.0",
                operation=second_operation,
                budget_id=second_budget,
                task_id=task_id,
                task_max_amount=max_amount,
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
            blocked_attempt = connection.execute(
                """
                SELECT error_code FROM fetch_attempts fa
                JOIN fetch_slots fs ON fs.id=fa.slot_id
                WHERE fs.window_key='range-page-2'
                """
            ).fetchone()
        self.assertEqual(len(usage), 1)
        self.assertEqual(usage[0]["task_id"], task_id)
        self.assertEqual(usage[0]["amount"], 0.008)
        self.assertEqual(
            (blocked_slot["status"], blocked_slot["last_error_code"]),
            ("retryable_failed", "task_budget_exhausted"),
        )
        self.assertEqual(blocked_attempt["error_code"], "task_budget_exhausted")

    def test_daily_quota_has_structured_error_code_in_slot_and_attempt(self) -> None:
        task_id = "daily-quota-structured"
        operation = "xiaohongshu_video_detail"
        max_amount = 0.016
        budget_id = self._insert_task_budget(
            task_id=task_id,
            operation=operation,
            max_amount=max_amount,
        )
        with connect(self.db) as connection:
            connection.execute(
                "UPDATE provider_budget_batches SET daily_quota=1 WHERE id=?",
                (budget_id,),
            )
            connection.commit()
        execute_content_fetch(
            content_id=1,
            stage="detail",
            window_key="quota-first",
            provider="Rnote",
            adapter_version="rnote-video-v8.0",
            operation=operation,
            budget_id=budget_id,
            task_id=task_id,
            task_max_amount=max_amount,
            db_path=self.db,
            raw_root=self.raw,
            call=lambda: ProviderResult({"ok": True}, {"ok": True}, 200, True),
        )
        with self.assertRaises(DailyAttemptQuotaExhausted):
            execute_content_fetch(
                content_id=1,
                stage="detail",
                window_key="quota-second",
                provider="Rnote",
                adapter_version="rnote-video-v8.0",
                operation=operation,
                budget_id=budget_id,
                task_id=task_id,
                task_max_amount=max_amount,
                db_path=self.db,
                raw_root=self.raw,
                call=lambda: ProviderResult({}, {}, 200, True),
            )
        with connect(self.db) as connection:
            row = connection.execute(
                """
                SELECT fs.last_error_code,fa.error_code
                FROM fetch_slots fs JOIN fetch_attempts fa ON fa.slot_id=fs.id
                WHERE fs.window_key='quota-second'
                """
            ).fetchone()
        self.assertEqual(tuple(row), ("budget_daily_quota_exhausted",) * 2)

    def test_unbilled_call_releases_task_capacity_for_another_operation(self) -> None:
        task_id = "backfill-unbilled-release"
        max_amount = 0.008
        first_operation = "xiaohongshu_video_detail"
        second_operation = "xiaohongshu_note_statistics"
        first_budget = self._insert_task_budget(
            task_id=task_id,
            operation=first_operation,
            max_amount=max_amount,
        )
        second_budget = self._insert_task_budget(
            task_id=task_id,
            operation=second_operation,
            max_amount=max_amount,
        )
        first = execute_content_fetch(
            content_id=1,
            stage="detail",
            window_key="unbilled-first",
            provider="Rnote",
            adapter_version="rnote-video-v8.0",
            operation=first_operation,
            budget_id=first_budget,
            task_id=task_id,
            task_max_amount=max_amount,
            db_path=self.db,
            raw_root=self.raw,
            call=lambda: ProviderResult({"ok": True}, {"ok": True}, 200, False),
        )
        second = execute_content_fetch(
            content_id=1,
            stage="metrics",
            window_key="unbilled-second",
            provider="Rnote",
            adapter_version="rnote-statistics-v8.0",
            operation=second_operation,
            budget_id=second_budget,
            task_id=task_id,
            task_max_amount=max_amount,
            db_path=self.db,
            raw_root=self.raw,
            call=lambda: ProviderResult({"ok": True}, {"ok": True}, 200, True),
        )
        self.assertFalse(first.billed)
        self.assertEqual(first.amount, 0.0)
        self.assertTrue(second.billed)
        self.assertEqual(second.amount, 0.008)
        with connect(self.db) as connection:
            usage = connection.execute(
                "SELECT amount,billed_requests FROM provider_usage "
                "WHERE task_id=? ORDER BY id",
                (task_id,),
            ).fetchall()
            budgets = {
                row["id"]: (row["consumed_requests"], row["consumed_amount"])
                for row in connection.execute(
                    "SELECT id,consumed_requests,consumed_amount "
                    "FROM provider_budget_batches WHERE id IN (?,?)",
                    (first_budget, second_budget),
                )
            }
        self.assertEqual([tuple(row) for row in usage], [(0.0, 0), (0.008, 1)])
        self.assertEqual(budgets[first_budget], (0, 0.0))
        self.assertEqual(budgets[second_budget], (1, 0.008))

    def test_concurrent_reservations_cannot_exceed_task_ceiling(self) -> None:
        task_id = "backfill-concurrent-ceiling"
        max_amount = 0.008
        first_operation = "xiaohongshu_video_detail"
        second_operation = "xiaohongshu_note_statistics"
        first_budget = self._insert_task_budget(
            task_id=task_id,
            operation=first_operation,
            max_amount=max_amount,
        )
        second_budget = self._insert_task_budget(
            task_id=task_id,
            operation=second_operation,
            max_amount=max_amount,
        )
        first_callback_entered = threading.Event()
        release_first_callback = threading.Event()
        second_callback_called = False

        def first_call() -> ProviderResult:
            first_callback_entered.set()
            if not release_first_callback.wait(timeout=5):
                raise AssertionError("test did not release the first provider callback")
            return ProviderResult({"ok": True}, {"ok": True}, 200, True)

        def second_call() -> ProviderResult:
            nonlocal second_callback_called
            second_callback_called = True
            return ProviderResult({"ok": True}, {"ok": True}, 200, True)

        with ThreadPoolExecutor(max_workers=1) as pool:
            first_future = pool.submit(
                execute_content_fetch,
                content_id=1,
                stage="detail",
                window_key="concurrent-first",
                provider="Rnote",
                adapter_version="rnote-video-v8.0",
                operation=first_operation,
                budget_id=first_budget,
                task_id=task_id,
                task_max_amount=max_amount,
                db_path=self.db,
                raw_root=self.raw,
                call=first_call,
            )
            self.assertTrue(first_callback_entered.wait(timeout=5))
            try:
                with self.assertRaisesRegex(BudgetBlocked, "task amount ceiling"):
                    execute_content_fetch(
                        content_id=1,
                        stage="metrics",
                        window_key="concurrent-second",
                        provider="Rnote",
                        adapter_version="rnote-statistics-v8.0",
                        operation=second_operation,
                        budget_id=second_budget,
                        task_id=task_id,
                        task_max_amount=max_amount,
                        db_path=self.db,
                        raw_root=self.raw,
                        call=second_call,
                    )
            finally:
                release_first_callback.set()
            first_outcome = first_future.result(timeout=5)

        self.assertTrue(first_outcome.billed)
        self.assertFalse(second_callback_called)
        with connect(self.db) as connection:
            usage = connection.execute(
                "SELECT operation,amount FROM provider_usage WHERE task_id=?",
                (task_id,),
            ).fetchall()
        self.assertEqual(
            [tuple(row) for row in usage], [(first_operation, max_amount)]
        )

    def test_task_budget_contract_rejects_omissions_wrong_task_and_larger_cap(self) -> None:
        task_id = "backfill-contract"
        operation = "xiaohongshu_video_detail"
        max_amount = 0.008
        budget_id = self._insert_task_budget(
            task_id=task_id,
            operation=operation,
            max_amount=max_amount,
        )
        provider_called = False

        def unexpected_call() -> ProviderResult:
            nonlocal provider_called
            provider_called = True
            return ProviderResult({}, {}, 200, True)

        common = {
            "content_id": 1,
            "stage": "detail",
            "provider": "Rnote",
            "adapter_version": "rnote-video-v8.0",
            "operation": operation,
            "db_path": self.db,
            "raw_root": self.raw,
            "call": unexpected_call,
        }
        with self.assertRaisesRegex(ValueError, "provided together"):
            execute_content_fetch(
                **common,
                window_key="missing-cap",
                budget_id=budget_id,
                task_id=task_id,
            )
        with self.assertRaisesRegex(BudgetBlocked, "does not match"):
            execute_content_fetch(
                **common,
                window_key="wrong-task",
                budget_id=budget_id,
                task_id="another-task",
                task_max_amount=max_amount,
            )
        with self.assertRaisesRegex(BudgetBlocked, "requires task_id"):
            execute_content_fetch(
                **common,
                window_key="missing-task",
                budget_id=budget_id,
            )
        with self.assertRaisesRegex(BudgetBlocked, "runtime ceiling"):
            execute_content_fetch(
                **common,
                window_key="larger-cap",
                budget_id=budget_id,
                task_id=task_id,
                task_max_amount=0.016,
            )
        self.assertFalse(provider_called)
        with connect(self.db) as connection:
            self.assertEqual(
                connection.execute("SELECT COUNT(*) FROM provider_usage").fetchone()[0],
                0,
            )
            slots = connection.execute(
                "SELECT window_key,status,last_error_code FROM fetch_slots"
            ).fetchall()
        self.assertEqual(
            [tuple(row) for row in slots],
            [("larger-cap", "retryable_failed", "budget_blocked")],
        )

    def test_account_fetch_records_task_and_replays_raw_without_cost(self) -> None:
        task_id = "backfill-account-test"
        max_amount = 0.008
        operation = "xiaohongshu_video_detail"
        budget_id = self._insert_task_budget(
            task_id=task_id,
            operation=operation,
            max_amount=max_amount,
        )
        outcome = execute_account_fetch(
            account_id=1,
            stage="discovery",
            window_key="backfill:first-page",
            provider="Rnote",
            adapter_version="rnote-user-posts-v8.0",
            operation=operation,
            budget_id=budget_id,
            task_id=task_id,
            task_max_amount=max_amount,
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
        self.assertEqual(usage["task_id"], task_id)
        self.assertEqual(usage["amount"], 0.008)

    def test_successful_account_fetch_can_be_reopened_after_materialization_failure(
        self,
    ) -> None:
        outcome = execute_account_fetch(
            account_id=1,
            stage="discovery",
            window_key="openapi:page:0",
            provider="DouyinOpenAPI",
            adapter_version="douyin-openapi-video-list-v1",
            operation="douyin_openapi_video_list",
            db_path=self.db,
            raw_root=self.raw,
            call=lambda: ProviderResult(
                {"items": [{"platform_content_id": "123456789"}]},
                {"data": {"list": [{"item_id": "123456789"}]}},
                200,
                False,
            ),
        )
        with connect(self.db) as connection:
            before_attempt = dict(
                connection.execute(
                    "SELECT * FROM fetch_attempts WHERE id=?", (outcome.attempt_id,)
                ).fetchone()
            )
            before_raw = dict(
                connection.execute(
                    "SELECT * FROM provider_raw_responses WHERE id=?",
                    (outcome.raw_response_id,),
                ).fetchone()
            )

        changed = mark_succeeded_fetch_slot_retryable_failure(
            db_path=self.db,
            slot_id=outcome.slot_id,
            error_code="derived_materialization_failed",
            error_message="content write interrupted",
        )

        self.assertEqual(changed["status"], "retryable_failed")
        with connect(self.db) as connection:
            slot = connection.execute(
                "SELECT * FROM fetch_slots WHERE id=?", (outcome.slot_id,)
            ).fetchone()
            after_attempt = dict(
                connection.execute(
                    "SELECT * FROM fetch_attempts WHERE id=?", (outcome.attempt_id,)
                ).fetchone()
            )
            after_raw = dict(
                connection.execute(
                    "SELECT * FROM provider_raw_responses WHERE id=?",
                    (outcome.raw_response_id,),
                ).fetchone()
            )
        self.assertEqual(slot["last_error_code"], "derived_materialization_failed")
        self.assertEqual(slot["attempt_count"], 1)
        self.assertEqual(after_attempt, before_attempt)
        self.assertEqual(after_raw, before_raw)

        with self.assertRaisesRegex(RuntimeError, "cannot become retryable"):
            mark_succeeded_fetch_slot_retryable_failure(
                db_path=self.db,
                slot_id=outcome.slot_id,
                error_code="derived_materialization_failed",
                error_message="duplicate transition",
            )
        with self.assertRaisesRegex(ValueError, "derived_materialization_failed"):
            mark_succeeded_fetch_slot_retryable_failure(
                db_path=self.db,
                slot_id=outcome.slot_id,
                error_code="some_other_error",
                error_message="wrong code",
            )

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
