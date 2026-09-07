from __future__ import annotations

import os
import subprocess
import tempfile
import threading
import unittest
from contextlib import ExitStack
from dataclasses import replace
from datetime import date
from pathlib import Path
from unittest.mock import patch

from apscheduler.schedulers.base import STATE_PAUSED  # type: ignore[import-untyped]
from apscheduler.schedulers.background import BackgroundScheduler  # type: ignore[import-untyped]
from fastapi.testclient import TestClient

from tests import test_v8_api as api_fixture
from v8 import api


class TransportOperatorApiTest(unittest.TestCase):
    def setUp(self):
        temporary = tempfile.TemporaryDirectory(prefix="dcar-paused-operator-api-")
        self.addCleanup(temporary.cleanup)
        self.root = Path(temporary.name)
        self.config = replace(api_fixture._test_config(self.root), writer_lock=self.root / "writer.lock")

    def test_paused_environment_disables_catchup_and_rejects_nonwriter_scheduler_modes(self):
        environment = {
            "DCAR_SCHEDULER_ENABLED": "1", "DCAR_SCHEDULER_START_PAUSED": "1",
            "DCAR_STARTUP_CATCHUP_ENABLED": "1", "DCAR_DAILY_CAPTURE_RECONCILE_FROM": "2026-08-21",
            "DCAR_V8_DB": str(self.config.db_path), "DCAR_PROJECT_ROOT": str(self.root),
            "DCAR_WRITER_LOCK": str(self.config.writer_lock),
        }
        with patch.dict(os.environ, environment, clear=True):
            config = api.ApiConfig.from_env()
        self.assertTrue(config.scheduler_start_paused)
        self.assertFalse(config.effective_startup_catchup_enabled)
        with patch.dict(os.environ, {**environment, "DCAR_SCHEDULER_START_PAUSED": "0"}, clear=True):
            default = api.ApiConfig.from_env()
        self.assertFalse(default.scheduler_start_paused)
        self.assertTrue(default.effective_startup_catchup_enabled)
        for invalid in (replace(config, read_only=True), replace(config, scheduler_enabled=False)):
            with self.subTest(config=invalid), self.assertRaisesRegex(RuntimeError, "DCAR_SCHEDULER_START_PAUSED"):
                invalid.validate_daily_capture_reconcile_contract()
        self.assertFalse(self.config.db_path.exists())
        self.assertFalse(self.config.writer_lock.exists())

    def test_actual_scheduler_starts_paused_without_startup_catchup(self):
        config = replace(self.config, scheduler_enabled=True, scheduler_start_paused=True,
                         startup_catchup_enabled=True, daily_capture_reconcile_from=date(2026, 8, 21))
        api_fixture._seed_read_model_database(config.db_path)
        scheduler = BackgroundScheduler(timezone="Asia/Shanghai")
        with ExitStack() as stack:
            for recovery in api_fixture.ApiStartupSafetyTest._patch_runtime_recovery(self):
                stack.enter_context(recovery)
            stack.enter_context(patch.object(api, "BackgroundScheduler", return_value=scheduler))
            install = stack.enter_context(patch.object(api, "install_jobs"))
            catchup = stack.enter_context(patch.object(api, "_run_startup_catchup"))
            stack.enter_context(patch.object(api, "assert_report_runtime_ready"))
            stack.enter_context(patch.object(api, "process_current_activation_hold_commands", return_value={"count": 0, "processed": []}))
            started = stack.enter_context(patch.object(scheduler, "start", wraps=scheduler.start))
            app = api.create_app(config)
            with TestClient(app):
                self.assertIs(app.state.scheduler, scheduler)
                self.assertEqual(scheduler.state, STATE_PAUSED)
                self.assertFalse(app.state.startup_catchup_enabled)
                self.assertEqual(app.state.catchup_status, "disabled")
            started.assert_called_once_with(paused=True)
            install.assert_called_once()
            catchup.assert_not_called()

    def test_explicit_primary_command_and_same_scheduler_reach_existing_executor(self):
        app = api.create_app(self.config)
        app.state.writer_lock_held = True
        app.state.current_hold_control_executor = object()
        app.state.current_hold_control_lock = threading.Lock()
        app.state.current_hold_control_wake_generation = 0
        scheduler = BackgroundScheduler(timezone="UTC")
        scheduler.start(paused=True)
        self.addCleanup(scheduler.shutdown, wait=False)
        app.state.scheduler = scheduler
        with TestClient(app) as client:
            # Lifespan installs its own state; this test exercises POST and the
            # bounded control adapter, not a paid run or installed writer proof.
            app.state.writer_lock_held = True
            app.state.current_hold_control_executor = object()
            app.state.scheduler = scheduler
            with patch.object(api, "enqueue_current_activation_hold_command", return_value={"run_id": 77, "status": "queued"}) as enqueue, \
                    patch.object(api, "_submit_current_hold_control") as wake:
                response = client.post("/api/v8/internal/current-activation-hold/commands", json={
                    "command_id": "primary-explicit-1", "command": "transport_primary", "parameters": {"drain_id": "fixture-hold"},
                })
            self.assertEqual(response.status_code, 202, response.text)
            enqueue.assert_called_once_with(db_path=self.config.db_path, command_id="primary-explicit-1",
                                            command="transport_primary", parameters={"drain_id": "fixture-hold"})
            wake.assert_called_once_with(request_app=app)
            with patch.object(api, "process_current_activation_hold_commands", return_value={"count": 0, "processed": []}) as process:
                api._drain_current_hold_control(request_app=app, config=self.config)
            self.assertIs(process.call_args.kwargs["scheduler"], scheduler)
            del app.state.scheduler
            with patch.object(api, "process_current_activation_hold_commands", return_value={"count": 0, "processed": []}) as process:
                api._drain_current_hold_control(request_app=app, config=self.config)
            self.assertIsNone(process.call_args.kwargs["scheduler"])

    def test_writer_wrapper_export_tail_preserves_default_and_explicit_pause(self):
        wrapper = Path(__file__).resolve().parents[1] / "deploy/macos/run_writer_worker.sh"
        parsed = subprocess.run(["/bin/bash", "-n", str(wrapper)], capture_output=True, text=True, check=False)
        self.assertEqual(parsed.returncode, 0, parsed.stderr)
        # Execute only the literal export/log tail: never preflight, open a DB,
        # invoke caffeinate, launch uvicorn or contact an installed service.
        tail = "export DCAR_READ_ONLY=0" + wrapper.read_text().split("export DCAR_READ_ONLY=0", 1)[1].split("exec /usr/bin/caffeinate", 1)[0]
        for pause, catchup, log in (("0", "1", "scheduler=1 catchup=report_only"), ("1", "0", "scheduler=paused catchup=disabled")):
            with self.subTest(pause=pause):
                code = 'scheduler_start_paused="$DCAR_SCHEDULER_START_PAUSED"\nreconcile_from=2026-08-21\n' + tail
                code += 'printf "observed:%s:%s\\n" "$DCAR_SCHEDULER_START_PAUSED" "$DCAR_STARTUP_CATCHUP_ENABLED"\n'
                result = subprocess.run(["/bin/bash", "-c", code], env={"DCAR_SCHEDULER_START_PAUSED": pause},
                                        capture_output=True, text=True, check=False)
                self.assertEqual(result.returncode, 0, result.stderr)
                self.assertIn(log, result.stdout)
                self.assertIn(f"observed:{pause}:{catchup}", result.stdout)
