from __future__ import annotations

import os
import sqlite3
import tempfile
import threading
import unittest
from concurrent.futures import ThreadPoolExecutor
from pathlib import Path
from unittest.mock import patch

from fastapi.testclient import TestClient

# Importing the API must never enable a paid LLM path in this test process.
os.environ.setdefault("DCAR_LLM_DISABLED", "1")

import v8.api as api_module
from v8.storage import connect, initialize_database


COMMANDS_PATH = "/api/v8/internal/current-activation-hold/commands"
BUILD = "a" * 64
RUNTIME = "b" * 64


class CurrentHoldCommandApiFailureTest(unittest.TestCase):
    """HTTP failure and writer-side read contracts for durable commands."""

    def setUp(self) -> None:
        temporary = tempfile.TemporaryDirectory(
            prefix="dcar-current-hold-command-api-"
        )
        self.addCleanup(temporary.cleanup)
        self.root = Path(temporary.name)
        self.db = self.root / "commands.sqlite3"
        with connect(self.db) as connection:
            initialize_database(connection)

        config = api_module.ApiConfig(
            db_path=self.db,
            reports_root=self.root / "reports",
            legacy_db_path=self.root / "legacy.sqlite3",
            operator_freeze_lock=self.root / "operator-freeze.lock",
            writer_lock=self.root / "writer.lock",
            scheduler_enabled=False,
            startup_catchup_enabled=False,
            read_only=False,
            project_root=self.root,
        )
        self.app = api_module.create_app(config)
        self.app.state.writer_lock_held = True
        self.client = TestClient(self.app)
        self.addCleanup(self.client.close)

    @staticmethod
    def _body() -> dict[str, object]:
        return {
            "command_id": "hold-command-persist-failure",
            "command": "hold_begin",
            "parameters": {
                "drain_id": "activation-2-hold-persist-failure",
                "build_receipt_sha256": BUILD,
                "runtime_root_receipt_sha256": RUNTIME,
                "actor": "fixture-release-owner",
                "reason": "exercise durable persistence failure",
                "not_before_business_day": "2026-09-08",
            },
        }

    def test_post_sqlite_persist_failure_is_stable_503(self) -> None:
        self.app.state.current_hold_control_executor = object()
        failure = sqlite3.OperationalError("database is locked")
        with (
            patch.object(
                api_module,
                "enqueue_current_activation_hold_command",
                side_effect=failure,
            ) as enqueue,
            patch.object(api_module, "_submit_current_hold_control") as wake_executor,
        ):
            response = self.client.post(COMMANDS_PATH, json=self._body())

        self.assertEqual(response.status_code, 503, response.text)
        self.assertEqual(
            response.json(),
            {
                "detail": {
                    "code": "durable_command_persist_failed",
                    "message": "database is locked",
                }
            },
        )
        enqueue.assert_called_once()
        wake_executor.assert_not_called()

    def test_writer_get_uses_live_wal_read_without_claiming(self) -> None:
        run_id = 73
        command_status = {
            "run_id": run_id,
            "status": "queued",
            "command_id": "hold-command-live-wal",
            "command": "hold_begin",
            "attempts": [],
        }
        with (
            patch.object(
                api_module,
                "read_current_activation_hold_command",
                return_value=command_status,
            ) as read_command,
            patch.object(
                api_module,
                "process_current_activation_hold_commands",
                side_effect=AssertionError("GET claimed a durable command"),
            ) as claim_commands,
        ):
            response = self.client.get(f"{COMMANDS_PATH}/{run_id}")

        self.assertEqual(response.status_code, 200, response.text)
        self.assertEqual(response.json(), command_status)
        read_command.assert_called_once_with(
            db_path=self.db,
            run_id=run_id,
            read_only=True,
            live_wal=True,
        )
        claim_commands.assert_not_called()

    def test_single_background_future_drains_every_bounded_batch(self) -> None:
        executor = ThreadPoolExecutor(max_workers=1)
        self.addCleanup(executor.shutdown, wait=True, cancel_futures=False)
        self.app.state.current_hold_control_executor = executor
        gate = threading.Event()
        counts = iter((10, 10, 3))

        def process_batch(**kwargs: object) -> dict[str, object]:
            gate.wait(timeout=5)
            return {"count": next(counts), "processed": [], "kwargs": kwargs}

        with patch.object(
            api_module,
            "process_current_activation_hold_commands",
            side_effect=process_batch,
        ) as process:
            first = api_module._submit_current_hold_control(request_app=self.app)
            second = api_module._submit_current_hold_control(request_app=self.app)
            gate.set()
            result = first.result(timeout=5)

        self.assertIs(second, first)
        self.assertEqual(result["count"], 23)
        self.assertEqual(result["batches"], 3)
        self.assertEqual(process.call_count, 3)
        for call in process.call_args_list:
            self.assertEqual(call.kwargs["limit"], 10)


if __name__ == "__main__":
    unittest.main()
