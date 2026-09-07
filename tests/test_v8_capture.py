from __future__ import annotations

import hashlib
import json
import sqlite3
import tempfile
import threading
import unittest
from concurrent.futures import ThreadPoolExecutor
from datetime import datetime, timezone
from pathlib import Path
from typing import Callable, TypedDict
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
    execute_derived_content_fetch,
    load_succeeded_raw_response,
    mark_succeeded_fetch_slot_retryable_failure,
    mark_fetch_slot_terminal_failure,
    recover_stale_fetch_slots,
)
from v8.metric_observations import persist_metric_observation
from v8.provider_budget import record_fault_state, task_budget_id
from v8.raw_evidence import RawEvidenceError
from v8.storage import connect, initialize_database, now_utc, transaction


class ContentFetchFixtureArgs(TypedDict):
    content_id: int
    stage: str
    provider: str
    adapter_version: str
    operation: str
    db_path: Path
    raw_root: Path
    call: Callable[[], ProviderResult]


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

    def _transport_receipt(
        self,
        entity: bytes,
        *,
        http_status: int = 200,
        content_encoding: str = "identity",
    ) -> dict[str, object]:
        return {
            "contract_version": "provider-json-transport-v1",
            "transport_route_id": "fixture-route-v1",
            "route_generation": "route-config-sha256:fixture",
            "http_stack": "fixture-stream-v1",
            "request_host": "fixture.invalid",
            "status": "succeeded",
            "error_code": None,
            "http_status": http_status,
            "content_encoding": content_encoding,
            "content_length": len(entity),
            "clean_eof": True,
            "length_match": True,
            "gzip_crc_ok": True if content_encoding == "gzip" else None,
            "json_parse_ok": True,
            "entity_bytes": len(entity),
            "entity_sha256": hashlib.sha256(entity).hexdigest(),
            "zero_body": not entity,
        }

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
        budget_id = task_budget_id(task_id, provider, operation)
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
        canonical_raw_root = capture_module.RAW_ROOT.resolve()
        isolated_raw_root = self.root / "runtime-default-raw"
        with (
            patch.object(capture_module, "RAW_ROOT", isolated_raw_root),
            patch.object(
                capture_module,
                "write_zstd_raw_evidence",
                wraps=capture_module.write_zstd_raw_evidence,
            ) as raw_write,
        ):
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
        raw_write.assert_called_once()
        written_path = Path(raw_write.call_args.args[0]).resolve()
        self.assertEqual(written_path.parents[3], isolated_raw_root.resolve())
        self.assertNotEqual(written_path.parents[3], canonical_raw_root)
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
        self.assertEqual(
            capture_module.read_raw_json(
                path,
                expected_stored_sha256=raw["sha256"],
                expected_stored_size=raw["byte_size"],
            )["token"],
            "[REDACTED]",
        )
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

    def test_raw_paths_are_unique_across_slots_with_same_attempt_and_payload(self) -> None:
        raw_response = {"fixture": "same-payload"}
        outcomes = [
            execute_content_fetch(
                content_id=1,
                stage=stage,
                window_key=window_key,
                provider="TestProvider",
                adapter_version="test-provider-v1",
                operation="shared_operation",
                db_path=self.db,
                raw_root=self.raw,
                call=lambda: ProviderResult(
                    data={"ok": True},
                    raw_response=raw_response,
                    http_status=200,
                    billed=False,
                ),
            )
            for stage, window_key in (
                ("detail", "window-a"),
                ("metrics", "window-b"),
            )
        ]

        with connect(self.db) as connection:
            rows = connection.execute(
                """
                SELECT pr.local_path,pr.sha256,fa.attempt_number,fs.id slot_id
                FROM provider_raw_responses pr
                JOIN fetch_attempts fa ON fa.id=pr.fetch_attempt_id
                JOIN fetch_slots fs ON fs.id=fa.slot_id
                ORDER BY pr.id
                """
            ).fetchall()

        self.assertEqual([row["attempt_number"] for row in rows], [1, 1])
        self.assertEqual(len({row["local_path"] for row in rows}), 2)
        self.assertEqual([row["slot_id"] for row in rows], [item.slot_id for item in outcomes])
        for row in rows:
            filename = Path(row["local_path"]).name
            self.assertTrue(filename.startswith("scope-"))
            self.assertTrue(filename.endswith("-sequence-0001.json.zst"))
            self.assertNotIn(str(row["sha256"]), filename)

    def test_complete_null_error_entity_is_stored_as_raw_evidence(self) -> None:
        entity = b"null"
        receipt = self._transport_receipt(entity, http_status=500)
        with self.assertRaises(CaptureError):
            execute_content_fetch(
                content_id=1,
                stage="metrics",
                window_key="null-error",
                provider="FixtureTransport",
                adapter_version="fixture-transport-v1",
                operation="fixture_metrics",
                db_path=self.db,
                raw_root=self.raw,
                call=lambda: (_ for _ in ()).throw(
                    CaptureError(
                        "HTTP 500 with JSON null",
                        retryable=True,
                        error_code="http_500",
                        http_status=500,
                        billed=False,
                        raw_response=None,
                        entity_bytes=entity,
                        transport_receipt=receipt,
                    )
                ),
            )
        with connect(self.db) as connection:
            row = connection.execute(
                "SELECT local_path,sha256,byte_size FROM provider_raw_responses"
            ).fetchone()
        self.assertIsNotNone(row)
        self.assertIsNone(
            capture_module.read_raw_json(
                Path(str(row["local_path"])),
                expected_stored_sha256=str(row["sha256"]),
                expected_stored_size=int(row["byte_size"]),
            )
        )
        self.assertFalse((self.raw / "quarantine").exists())

    def test_database_storage_failure_is_storage_hard_and_quarantines_entity(self) -> None:
        entity = b'{"ok":true}'
        receipt = self._transport_receipt(entity)
        with (
            patch(
                "v8.capture._store_raw_response",
                side_effect=sqlite3.OperationalError("fixture database write failed"),
            ) as store,
            self.assertRaises(CaptureError) as raised,
        ):
            execute_content_fetch(
                content_id=1,
                stage="metrics",
                window_key="storage-failure",
                provider="FixtureTransport",
                adapter_version="fixture-transport-v1",
                operation="fixture_metrics",
                db_path=self.db,
                raw_root=self.raw,
                call=lambda: ProviderResult(
                    {"ok": True},
                    {"ok": True},
                    200,
                    False,
                    entity_bytes=entity,
                    transport_receipt=receipt,
                ),
            )
        self.assertEqual(raised.exception.error_code, "storage_hard")
        self.assertFalse(raised.exception.retryable)
        store.assert_called_once()
        quarantined = list((self.raw / "quarantine").rglob("*.entity"))
        self.assertEqual(len(quarantined), 1)
        self.assertEqual(quarantined[0].read_bytes(), entity)
        receipt_paths = list((self.raw / "quarantine").rglob("*.receipt.json"))
        self.assertEqual(len(receipt_paths), 1)
        persisted_receipt = json.loads(receipt_paths[0].read_text())
        self.assertEqual(persisted_receipt["quarantine_path"], str(quarantined[0]))
        self.assertEqual(persisted_receipt["http_status"], 200)
        with connect(self.db) as connection:
            slot = connection.execute(
                "SELECT status,last_error_code FROM fetch_slots WHERE window_key=?",
                ("storage-failure",),
            ).fetchone()
            attempt = connection.execute(
                "SELECT http_status,error_code FROM fetch_attempts WHERE slot_id=(SELECT id FROM fetch_slots WHERE window_key=?)",
                ("storage-failure",),
            ).fetchone()
        self.assertEqual(tuple(slot), ("terminal_failed", "storage_hard"))
        self.assertEqual(tuple(attempt), (200, "storage_hard"))

    def test_incomplete_or_mismatched_transport_receipt_cannot_succeed(self) -> None:
        entity = b'{"ok":true}'
        mutations = {
            "contract_version": "tampered",
            "http_status": 503,
            "clean_eof": False,
            "length_match": False,
            "gzip_crc_ok": False,
            "json_parse_ok": False,
            "entity_sha256": "0" * 64,
            "entity_bytes": len(entity) + 1,
            "route_generation": "",
        }
        for index, (field, value) in enumerate(mutations.items()):
            if index:
                # Each malformed response must reach its own validation path;
                # a previous sample's durable storage fault blocks later sends.
                self.tearDown()
                self.setUp()
            receipt = self._transport_receipt(entity)
            receipt[field] = value
            with (
                self.subTest(field=field),
                self.assertRaises(CaptureError) as raised,
            ):
                execute_content_fetch(
                    content_id=1,
                    stage="metrics",
                    window_key=f"tampered-receipt-{index}",
                    provider="FixtureTransport",
                    adapter_version="fixture-transport-v1",
                    operation="fixture_metrics",
                    db_path=self.db,
                    raw_root=self.raw,
                    call=lambda receipt=receipt: ProviderResult(
                        {"ok": True},
                        {"ok": True},
                        200,
                        False,
                        entity_bytes=entity,
                        transport_receipt=receipt,
                    ),
                )
            self.assertEqual(raised.exception.error_code, "storage_hard")
            with connect(self.db) as connection:
                raw_count = connection.execute(
                    "SELECT COUNT(*) FROM provider_raw_responses WHERE operation='fixture_metrics'"
                ).fetchone()[0]
            self.assertEqual(raw_count, 0)

    def test_raw_writer_rejects_traversal_and_ancestor_symlink_escape(self) -> None:
        outside = self.root / "outside"
        outside.mkdir()
        entity = b'{"ok":true}'
        receipt = self._transport_receipt(entity)

        with self.assertRaises(CaptureError) as traversal:
            execute_content_fetch(
                content_id=1,
                stage="metrics",
                window_key="traversal-operation",
                provider="FixtureTransport",
                adapter_version="fixture-transport-v1",
                operation="../../../outside",
                db_path=self.db,
                raw_root=self.raw,
                call=lambda: ProviderResult(
                    {"ok": True},
                    {"ok": True},
                    200,
                    False,
                    entity_bytes=entity,
                    transport_receipt=receipt,
                ),
            )
        self.assertEqual(traversal.exception.error_code, "storage_hard")
        self.assertEqual(list(outside.iterdir()), [])

    def test_raw_writer_rejects_ancestor_symlink_escape(self) -> None:
        outside = self.root / "outside"
        outside.mkdir()
        entity = b'{"ok":true}'
        receipt = self._transport_receipt(entity)
        self.raw.mkdir(mode=0o700, exist_ok=True)
        (self.raw / "fixturetransport").symlink_to(outside, target_is_directory=True)
        with self.assertRaises(CaptureError) as symlink:
            execute_content_fetch(
                content_id=1,
                stage="detail",
                window_key="symlink-provider",
                provider="FixtureTransport",
                adapter_version="fixture-transport-v1",
                operation="fixture_detail",
                db_path=self.db,
                raw_root=self.raw,
                call=lambda: ProviderResult(
                    {"ok": True},
                    {"ok": True},
                    200,
                    False,
                    entity_bytes=entity,
                    transport_receipt=receipt,
                ),
            )
        self.assertEqual(symlink.exception.error_code, "storage_hard")
        self.assertEqual(list(outside.iterdir()), [])

    def test_nonpaid_transport_attempts_keep_distinct_quarantine_evidence(self) -> None:
        partials = (b'{"first":', b'{"second":')
        for partial in partials:
            with self.assertRaises(CaptureError):
                execute_content_fetch(
                    content_id=1,
                    stage="metrics",
                    window_key="two-partials",
                    provider="FixtureTransport",
                    adapter_version="fixture-transport-v1",
                    operation="fixture_metrics",
                    db_path=self.db,
                    raw_root=self.raw,
                    call=lambda partial=partial: (_ for _ in ()).throw(
                        CaptureError(
                            "fixture incomplete response",
                            retryable=True,
                            error_code="transport_error",
                            billed=False,
                            transport_partial=partial,
                            transport_receipt={
                                "status": "failed",
                                "error_code": "transport_incomplete_read",
                                "http_status": 200,
                                "partial_bytes": len(partial),
                                "partial_sha256": hashlib.sha256(partial).hexdigest(),
                                "zero_body": False,
                            },
                        )
                    ),
                )

        quarantined = list((self.raw / "quarantine").rglob("*.partial"))
        self.assertEqual({path.read_bytes() for path in quarantined}, set(partials))
        self.assertTrue(any("sequence-0001" in path.name for path in quarantined))
        self.assertTrue(any("sequence-0002" in path.name for path in quarantined))
        self.assertEqual(
            len(list((self.raw / "quarantine").rglob("*.receipt.json"))),
            2,
        )
        with connect(self.db) as connection:
            slot = connection.execute(
                "SELECT status,attempt_count FROM fetch_slots WHERE window_key='two-partials'"
            ).fetchone()
            attempts = connection.execute(
                "SELECT COUNT(*) FROM fetch_attempts WHERE slot_id=(SELECT id FROM fetch_slots WHERE window_key='two-partials')"
            ).fetchone()[0]
        self.assertEqual(tuple(slot), ("retryable_failed", 2))
        self.assertEqual(attempts, 2)

    def test_quarantine_persistence_failure_still_terminalizes_attempt(self) -> None:
        with (
            patch(
                "v8.capture._quarantine_transport_evidence",
                side_effect=OSError("fixture quarantine unavailable"),
            ),
            self.assertRaises(CaptureError) as raised,
        ):
            execute_content_fetch(
                content_id=1,
                stage="metrics",
                window_key="quarantine-failure",
                provider="FixtureTransport",
                adapter_version="fixture-transport-v1",
                operation="fixture_metrics",
                db_path=self.db,
                raw_root=self.raw,
                call=lambda: (_ for _ in ()).throw(
                    CaptureError(
                        "fixture transport failed",
                        retryable=True,
                        error_code="transport_error",
                        http_status=200,
                        billed=False,
                        transport_partial=b"partial",
                        transport_receipt={"status": "failed"},
                    )
                ),
            )
        self.assertEqual(raised.exception.error_code, "storage_hard")
        self.assertFalse(raised.exception.retryable)
        with connect(self.db) as connection:
            slot = connection.execute(
                "SELECT status,last_error_code FROM fetch_slots WHERE window_key='quarantine-failure'"
            ).fetchone()
            attempt = connection.execute(
                "SELECT http_status,error_code FROM fetch_attempts WHERE slot_id=(SELECT id FROM fetch_slots WHERE window_key='quarantine-failure')"
            ).fetchone()
        self.assertEqual(tuple(slot), ("terminal_failed", "storage_hard"))
        self.assertEqual(tuple(attempt), (200, "storage_hard"))

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

    def test_task_ceiling_counts_legacy_v1_usage_after_v2_budget_rotation(self) -> None:
        task_id = "legacy-budget-rotation"
        operation = "xiaohongshu_video_detail"
        max_amount = 0.008
        task_digest = hashlib.sha256(task_id.encode("utf-8")).hexdigest()[:16]
        legacy_budget_id = f"task-{task_digest}-rnote-{operation}-v1"
        current_budget_id = self._insert_task_budget(
            task_id=task_id,
            operation=operation,
            max_amount=max_amount,
        )
        self.assertNotEqual(current_budget_id, legacy_budget_id)
        with connect(self.db) as connection:
            connection.execute(
                """
                INSERT INTO provider_usage(
                    task_id,budget_batch_id,provider,operation,request_attempts,
                    billed_requests,currency,amount,recorded_at,details_json
                ) VALUES (?,?,?,?,1,1,'USD',?,?, '{}')
                """,
                (
                    task_id,
                    legacy_budget_id,
                    "Rnote",
                    operation,
                    max_amount,
                    "2026-08-01T00:00:00Z",
                ),
            )
            connection.commit()

        provider_called = False

        def unexpected_call() -> ProviderResult:
            nonlocal provider_called
            provider_called = True
            return ProviderResult({}, {}, 200, True)

        with self.assertRaises(TaskBudgetExhausted):
            execute_content_fetch(
                content_id=1,
                stage="detail",
                window_key="v2-after-v1",
                provider="Rnote",
                adapter_version="rnote-video-v8.0",
                operation=operation,
                budget_id=current_budget_id,
                task_id=task_id,
                task_max_amount=max_amount,
                db_path=self.db,
                raw_root=self.raw,
                call=unexpected_call,
            )
        self.assertFalse(provider_called)
        with connect(self.db) as connection:
            usage = connection.execute(
                "SELECT budget_batch_id,amount FROM provider_usage WHERE task_id=?",
                (task_id,),
            ).fetchall()
        self.assertEqual(
            [tuple(row) for row in usage], [(legacy_budget_id, max_amount)]
        )

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

        common: ContentFetchFixtureArgs = {
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

    def test_storage_fault_blocks_non_tikhub_network_before_callback(self) -> None:
        calls = 0

        def provider_call() -> ProviderResult:
            nonlocal calls
            calls += 1
            return ProviderResult({}, {}, 200, False)

        with connect(self.db) as connection, transaction(connection):
            record_fault_state(
                connection,
                scope_kind="storage_hard",
                fault_class="local_evidence_store",
                reason="raw_mount_unavailable",
                usage_id=None,
                at=now_utc(),
                provider="all",
                state_evidence={"storage_receipt": "unavailable"},
            )
        with self.assertRaises(BudgetBlocked) as blocked:
            execute_account_fetch(
                account_id=1,
                stage="discovery",
                window_key="openapi:page:0",
                provider="DouyinOpenAPI",
                adapter_version="douyin-openapi-video-list-v1",
                operation="douyin_openapi_video_list",
                db_path=self.db,
                raw_root=self.raw,
                call=provider_call,
            )
        self.assertEqual(blocked.exception.error_code, "storage_hard")
        self.assertEqual(calls, 0)
        with connect(self.db) as connection:
            attempt = connection.execute(
                "SELECT error_code FROM fetch_attempts ORDER BY id DESC LIMIT 1"
            ).fetchone()
        self.assertEqual(attempt["error_code"], "storage_hard")

    def test_non_tikhub_storage_failure_opens_global_storage_gate(self) -> None:
        calls = 0

        def provider_call() -> ProviderResult:
            nonlocal calls
            calls += 1
            return ProviderResult({"items": []}, {"data": []}, 200, False)

        with (
            patch.object(
                capture_module,
                "_store_raw_response",
                side_effect=RawEvidenceError("fixture disk write failure"),
            ),
            self.assertRaises(CaptureError) as failed,
        ):
            execute_account_fetch(
                account_id=1,
                stage="discovery",
                window_key="openapi:first",
                provider="DouyinOpenAPI",
                adapter_version="douyin-openapi-video-list-v1",
                operation="douyin_openapi_video_list",
                db_path=self.db,
                raw_root=self.raw,
                call=provider_call,
            )
        self.assertEqual(failed.exception.error_code, "storage_hard")
        self.assertEqual(calls, 1)
        with connect(self.db) as connection:
            storage_fault = connection.execute(
                """SELECT details_json FROM scheduler_runs
                   WHERE json_extract(details_json,'$.scope_kind')='storage_hard'
                   ORDER BY id DESC LIMIT 1"""
            ).fetchone()
        self.assertIsNotNone(storage_fault)
        self.assertTrue(json.loads(storage_fault["details_json"])["open"])

        with self.assertRaises(BudgetBlocked) as blocked:
            execute_account_fetch(
                account_id=1,
                stage="discovery",
                window_key="openapi:second",
                provider="DouyinOpenAPI",
                adapter_version="douyin-openapi-video-list-v1",
                operation="douyin_openapi_video_list",
                db_path=self.db,
                raw_root=self.raw,
                call=provider_call,
            )
        self.assertEqual(blocked.exception.error_code, "storage_hard")
        self.assertEqual(calls, 1)

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



    def source_for_derivation(self):
        result = execute_content_fetch(
            content_id=1, stage="detail", window_key="type-probe", provider="TestProvider",
            adapter_version="fixture-v1", operation="source_detail",
            call=lambda: ProviderResult({}, {"content_id": "abc123", "comment_count": 0}, 200, False),
            db_path=self.db, raw_root=self.raw,
        )
        with connect(self.db) as connection:
            return dict(connection.execute(
                "SELECT * FROM provider_raw_responses WHERE id=?", (result.raw_response_id,)
            ).fetchone())

    def derive(self, source, *, window="week", allow_terminal_retry=False):
        return execute_derived_content_fetch(
            content_id=1, stage="comments", window_key=window, provider="TikHub",
            adapter_version="local-derived-v1", operation="comments_derived",
            result=ProviderResult(
                {"comment_count": 0, "comments": []},
                {"source_raw_response_id": source["id"], "source_sha256": source["sha256"]},
                200, False,
            ),
            source_raw_response_id=source["id"], db_path=self.db, raw_root=self.raw,
            allow_terminal_retry=allow_terminal_retry,
        )

    def test_derived_fetch_checks_real_raw_bytes_before_claiming(self):
        source = self.source_for_derivation()
        path = Path(source["local_path"])
        path.write_text('{"changed":true}', encoding="utf-8")
        with self.assertRaises(RawResponseIntegrityError):
            self.derive(source)
        with connect(self.db) as connection:
            self.assertEqual(connection.execute("SELECT COUNT(*) FROM fetch_slots WHERE stage='comments'").fetchone()[0], 0)
            self.assertEqual(connection.execute("SELECT COUNT(*) FROM provider_usage").fetchone()[0], 0)

    def test_derived_fetch_checks_byte_size_and_target_before_claiming(self):
        source = self.source_for_derivation()
        with connect(self.db) as connection, transaction(connection):
            connection.execute("UPDATE provider_raw_responses SET byte_size=byte_size+1 WHERE id=?", (source["id"],))
        with self.assertRaisesRegex(RawResponseIntegrityError, "byte size"):
            self.derive(source)
        with connect(self.db) as connection, transaction(connection):
            connection.execute("UPDATE provider_raw_responses SET byte_size=byte_size-1,account_id=1 WHERE id=?", (source["id"],))
        with self.assertRaisesRegex(RawResponseIntegrityError, "another account"):
            self.derive(source)

    def test_derived_fetch_terminal_retry_is_explicit_and_never_bills(self):
        source = self.source_for_derivation()
        first = self.derive(source)
        mark_fetch_slot_terminal_failure(
            db_path=self.db, slot_id=first.slot_id,
            error_code="derived_fixture_failure", error_message="isolated fixture",
        )
        with self.assertRaises(SlotUnavailable):
            self.derive(source)
        retried = self.derive(source, allow_terminal_retry=True)
        self.assertEqual(first.slot_id, retried.slot_id)
        self.assertFalse(retried.billed)
        self.assertEqual(retried.amount, 0)
        with connect(self.db) as connection:
            self.assertEqual(connection.execute("SELECT attempt_count FROM fetch_slots WHERE id=?", (first.slot_id,)).fetchone()[0], 2)
            self.assertEqual(connection.execute("SELECT COUNT(*) FROM provider_usage").fetchone()[0], 0)

    def test_matrix_global_raw_can_derive_zero_comments_only_with_target_observation(self):
        captured = now_utc()
        path = self.root / "matrix-global.json"
        body = capture_module.canonical_json_bytes(
            {"code": 0, "data": [{"awemeId": "abc123", "commentCount": 0}]}
        )
        path.write_bytes(body)
        path.chmod(0o600)
        with connect(self.db) as connection, transaction(connection):
            cursor = connection.execute(
                """INSERT INTO provider_raw_responses(provider,operation,local_path,sha256,
                   byte_size,http_status,captured_at,source)
                   VALUES('newrank_matrix','matrix_works_list',?,?,?,200,?,'matrix_page_applied')""",
                (str(path), hashlib.sha256(body).hexdigest(), len(body), captured),
            )
            source = dict(connection.execute("SELECT * FROM provider_raw_responses WHERE id=?", (cursor.lastrowid,)).fetchone())
        with self.assertRaises(RawResponseIntegrityError):
            self.derive(source)
        with connect(self.db) as connection, transaction(connection):
            persist_metric_observation(
                connection, content_id=1, captured_at=captured, window_key="matrix:today",
                view_count=None, comment_count=0, like_count=None, share_count=None,
                collect_count=None, status="available", provider="newrank_matrix",
                raw_response_id=source["id"], metadata_json="{}", recorded_at=captured,
            )
        result = self.derive(source)
        self.assertEqual(result.data["comments"], [])
        self.assertEqual(result.data["comment_count"], 0)
        with connect(self.db) as connection:
            self.assertEqual(connection.execute("SELECT COUNT(*) FROM provider_usage").fetchone()[0], 0)


if __name__ == "__main__":
    unittest.main()
