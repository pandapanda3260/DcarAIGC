from __future__ import annotations

import json
import os
import tempfile
import threading
import unittest
from concurrent.futures import ThreadPoolExecutor
from pathlib import Path
from typing import Any
from unittest.mock import Mock, patch

from tests.roster_fixture import accept_roster
from tests.test_v8_provider_transport import FakeOpener, FakeResponse, clock
from tikhub_config import resolve_tikhub_transport_manifest
from v8 import capture, providers
from v8.capture import CaptureError, ProviderResult, execute_content_fetch
from v8.paid_identity import build_paid_request_identity
from v8.provider_transport import (
    RequestTransportBindingError,
    current_request_transport,
    request_json,
)
from v8.storage import connect, initialize_database, now_utc, transaction


OPERATION = "douyin_video_detail"
CONTENT_KEY = "transport-bound-video"
TASK_ID = "request-transport-binding"
TASK_MAX_AMOUNT = 0.01
DEV_BASE = "https://api.tikhub.dev"
IO_BASE = "https://api.tikhub.io"


class BlockingNetworkSlot:
    def __init__(self) -> None:
        self.entered = threading.Event()
        self.release = threading.Event()

    def __enter__(self) -> None:
        self.entered.set()
        if not self.release.wait(timeout=5):
            raise TimeoutError("fixture network slot was not released")

    def __exit__(self, *_args: object) -> None:
        return None


class RequestTransportBindingTest(unittest.TestCase):
    def setUp(self) -> None:
        temporary = tempfile.TemporaryDirectory(prefix="dcar-request-transport-")
        self.addCleanup(temporary.cleanup)
        self.root = Path(temporary.name).resolve()
        self.db = self.root / "transport.sqlite3"
        self.raw = self.root / "raw"
        self.config = self.root / "tikhub.env"
        self._write_config(DEV_BASE)
        environment = patch.dict(
            os.environ,
            {"TIKHUB_API_KEY_FILE": str(self.config)},
        )
        environment.start()
        self.addCleanup(environment.stop)

        captured_at = now_utc()
        with connect(self.db) as connection:
            initialize_database(connection)
            connection.execute(
                """
                INSERT INTO accounts(
                    id,phone,phone_normalized,operator_name,account_type,
                    content_direction,enabled,created_at,updated_at
                ) VALUES (1,'',NULL,'','unknown','unknown',1,?,?)
                """,
                (captured_at, captured_at),
            )
            connection.execute(
                """
                INSERT INTO account_platform_identities(
                    id,account_id,platform,uid,nickname,source,created_at,updated_at
                ) VALUES (1,1,'douyin','managed-account','fixture','manual',?,?)
                """,
                (captured_at, captured_at),
            )
            connection.execute(
                """
                INSERT INTO content_items(
                    id,link_id,platform,platform_content_id,canonical_url,
                    account_id,raw_account_uid,content_type,imported_at,created_at,
                    updated_at
                ) VALUES (1,'TRNSPT','douyin',?,
                          'https://www.douyin.com/video/transport-bound-video',
                          1,'managed-account','video',?,?,?)
                """,
                (CONTENT_KEY, captured_at, captured_at, captured_at),
            )
            connection.commit()
            with transaction(connection):
                accept_roster(connection, accepted_at=captured_at)

        self.budget_id = providers.ensure_task_budget(
            provider="TikHub",
            operation=OPERATION,
            price=providers.TIKHUB_PRICE,
            task_id=TASK_ID,
            task_max_amount=TASK_MAX_AMOUNT,
            db_path=self.db,
        )
        self.identity = build_paid_request_identity(
            provider="TikHub",
            operation=OPERATION,
            platform="douyin",
            subject=CONTENT_KEY,
            request_parameters={"aweme_id": CONTENT_KEY},
            cursor=None,
            due_bucket="lifetime",
        )

    def _write_config(self, api_base: str) -> None:
        self.config.write_text(
            f"TIKHUB_API_BASE={api_base}\nTIKHUB_API_KEY=fixture-secret\n",
            encoding="utf-8",
        )
        self.config.chmod(0o600)

    def _binding(self) -> dict[str, Any]:
        return {
            "manifest": resolve_tikhub_transport_manifest(self.config),
            "config_path": str(self.config),
            "honor_environment": True,
        }

    def _execute(
        self,
        *,
        request_transport: dict[str, Any],
        call: Any,
    ) -> Any:
        return execute_content_fetch(
            content_id=1,
            stage="detail",
            window_key="lifetime",
            provider="TikHub",
            adapter_version="transport-binding-fixture-v1",
            operation=OPERATION,
            call=call,
            db_path=self.db,
            raw_root=self.raw,
            budget_id=self.budget_id,
            task_id=TASK_ID,
            task_max_amount=TASK_MAX_AMOUNT,
            paid_request_identity=self.identity,
            request_transport=request_transport,
        )

    def _count(self, table: str) -> int:
        with connect(self.db) as connection:
            return int(connection.execute(f"SELECT COUNT(*) FROM {table}").fetchone()[0])

    def _send_markers(self) -> list[Path]:
        return list((self.db.parent / "paid_send_claims").rglob("*.claim.json"))

    def test_route_drift_before_claim_has_no_usage_attempt_marker_or_provider_call(
        self,
    ) -> None:
        frozen = self._binding()
        self._write_config(IO_BASE)
        provider_call = Mock(side_effect=AssertionError("provider must not run"))

        with self.assertRaises(RequestTransportBindingError):
            self._execute(request_transport=frozen, call=provider_call)

        provider_call.assert_not_called()
        self.assertEqual(self._count("provider_usage"), 0)
        self.assertEqual(self._count("fetch_attempts"), 0)
        self.assertEqual(self._count("fetch_slots"), 0)
        self.assertEqual(self._send_markers(), [])

    def test_route_drift_while_waiting_for_network_slot_releases_reservation_unsent(
        self,
    ) -> None:
        frozen = self._binding()
        gate = BlockingNetworkSlot()
        provider_call = Mock(side_effect=AssertionError("provider must not run"))

        with patch.object(capture, "TIKHUB_NETWORK_SLOTS", gate):
            with ThreadPoolExecutor(max_workers=1) as pool:
                future = pool.submit(
                    self._execute,
                    request_transport=frozen,
                    call=provider_call,
                )
                self.assertTrue(gate.entered.wait(timeout=5))
                with connect(self.db) as connection:
                    reserved = connection.execute(
                        "SELECT request_attempts,details_json FROM provider_usage"
                    ).fetchone()
                self.assertIsNotNone(reserved)
                assert reserved is not None
                self.assertEqual(reserved["request_attempts"], 0)
                self.assertEqual(json.loads(reserved["details_json"])["state"], "reserved")
                self._write_config(IO_BASE)
                gate.release.set()
                with self.assertRaises(RequestTransportBindingError):
                    future.result(timeout=5)

        provider_call.assert_not_called()
        with connect(self.db) as connection:
            usage = connection.execute(
                "SELECT request_attempts,billed_requests,amount,details_json "
                "FROM provider_usage"
            ).fetchone()
            slot = connection.execute(
                "SELECT status,attempt_count FROM fetch_slots"
            ).fetchone()
        self.assertIsNotNone(usage)
        self.assertIsNotNone(slot)
        assert usage is not None and slot is not None
        self.assertEqual(
            (usage["request_attempts"], usage["billed_requests"], usage["amount"]),
            (0, 0, 0.0),
        )
        self.assertEqual(json.loads(usage["details_json"])["state"], "not_sent")
        self.assertEqual(tuple(slot), ("pending", 0))
        self.assertEqual(self._count("fetch_attempts"), 0)
        self.assertEqual(self._send_markers(), [])

    def test_route_change_after_send_uses_the_frozen_url_and_transport_receipt(
        self,
    ) -> None:
        frozen = self._binding()
        manifest = frozen["manifest"]
        payload = {
            "stage": "detail",
            "data": {"content_type": "video", "media_urls": []},
        }
        body = json.dumps(payload, separators=(",", ":")).encode("utf-8")
        observed: list[dict[str, Any]] = []

        def transport(request: Any, **kwargs: Any) -> Any:
            observed.append({"url": request.full_url, **kwargs})
            return request_json(
                request,
                opener=FakeOpener(
                    FakeResponse(
                        body,
                        headers={"Content-Length": str(len(body))},
                        response_url=request.full_url,
                    )
                ),
                clock=clock(),
                **kwargs,
            )

        real_mark_paid_sent = capture._mark_paid_sent

        def mark_then_change_route(*args: Any, **kwargs: Any) -> Any:
            claim = real_mark_paid_sent(*args, **kwargs)
            self._write_config(IO_BASE)
            return claim

        with (
            patch.object(
                capture,
                "_mark_paid_sent",
                side_effect=mark_then_change_route,
            ),
            patch.object(
                providers,
                "request_json_transport",
                side_effect=transport,
            ),
        ):
            outcome = self._execute(
                request_transport=frozen,
                call=lambda: providers._douyin_call(
                    "detail", CONTENT_KEY, "fixture-secret"
                ),
            )

        self.assertTrue(outcome.billed)
        self.assertEqual(len(observed), 1)
        self.assertTrue(observed[0]["url"].startswith(f"{DEV_BASE}/api/"))
        self.assertFalse(observed[0]["url"].startswith(IO_BASE))
        self.assertEqual(observed[0]["route_id"], manifest["transport_route_id"])
        self.assertEqual(observed[0]["route_generation"], manifest["route_generation"])
        self.assertEqual(observed[0]["http_stack"], manifest["http_stack"])
        with connect(self.db) as connection:
            details = json.loads(
                connection.execute(
                    "SELECT details_json FROM provider_usage"
                ).fetchone()[0]
            )
        self.assertEqual(details["transport"]["request_host"], "api.tikhub.dev")
        self.assertEqual(
            details["transport"]["request_transport_config_sha256"],
            manifest["config_sha256"],
        )
        self.assertIsNone(current_request_transport())

    def test_same_paid_identity_cannot_rebuy_on_another_route_and_context_resets(
        self,
    ) -> None:
        dev_binding = self._binding()
        provider_calls: list[str] = []

        def retryable_provider() -> ProviderResult:
            active = current_request_transport()
            self.assertIsNotNone(active)
            assert active is not None
            provider_calls.append(active["manifest"]["api_base"])
            raise CaptureError(
                "fixture retry response",
                retryable=True,
                error_code="provider_retry_requested",
                http_status=400,
                billed=False,
                raw_response={"detail": {"message": "Please retry"}},
            )

        with self.assertRaises(CaptureError) as first:
            self._execute(
                request_transport=dev_binding,
                call=retryable_provider,
            )
        self.assertEqual(first.exception.error_code, "provider_retry_requested")
        self.assertIsNone(current_request_transport())

        self._write_config(IO_BASE)
        io_binding = self._binding()
        second_provider = Mock(side_effect=AssertionError("identity hold must win"))
        with self.assertRaises(CaptureError) as second:
            self._execute(
                request_transport=io_binding,
                call=second_provider,
            )

        self.assertEqual(second.exception.error_code, "paid_identity_hold")
        second_provider.assert_not_called()
        self.assertEqual(provider_calls, [DEV_BASE])
        self.assertIsNone(current_request_transport())
        with connect(self.db) as connection:
            usages = list(
                connection.execute(
                    "SELECT request_attempts,billed_requests,amount,details_json "
                    "FROM provider_usage ORDER BY id"
                )
            )
        self.assertEqual(len(usages), 2)
        self.assertEqual(
            [json.loads(row["details_json"])["state"] for row in usages],
            ["failed", "not_sent"],
        )
        self.assertEqual([row["request_attempts"] for row in usages], [1, 0])
        self.assertEqual(self._count("fetch_attempts"), 1)
        self.assertEqual(len(self._send_markers()), 1)


if __name__ == "__main__":
    unittest.main()
