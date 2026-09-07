from __future__ import annotations

import json
import os
import tempfile
import unittest
from dataclasses import replace
from pathlib import Path
from unittest.mock import patch

from fastapi.testclient import TestClient

# Never let importing the API test fixture enable a paid LLM background path.
os.environ.setdefault("DCAR_LLM_DISABLED", "1")

import v8.api as api_module
import v8.profile_control as profile_control
from v8.storage import connect, initialize_database


COMMANDS_PATH = "/api/v8/internal/current-activation-hold/commands"
COMMAND_JOB = "current_activation_hold_command"
COMMAND_CONTRACT = "current-activation-hold-command-v1"
SUBMITTED_AT = "2026-09-06T04:00:00Z"
BUILD = "a" * 64
RUNTIME = "b" * 64


class CurrentActivationHoldCommandTest(unittest.TestCase):
    """Failure-first HTTP and executor contracts for current-hold commands."""

    def setUp(self) -> None:
        self.temp = tempfile.TemporaryDirectory()
        self.addCleanup(self.temp.cleanup)
        self.root = Path(self.temp.name)
        self.db = self.root / "commands.sqlite3"
        with connect(self.db) as connection:
            initialize_database(connection)

        self.config = api_module.ApiConfig(
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
        self.app = api_module.create_app(self.config)
        self.app.state.writer_lock_held = False
        self.client = TestClient(self.app)
        self.addCleanup(self.client.close)

    def _parameters(self, *, drain_id: str = "activation-2-hold-1") -> dict:
        return {
            "drain_id": drain_id,
            "build_receipt_sha256": BUILD,
            "runtime_root_receipt_sha256": RUNTIME,
            "actor": "fixture-release-owner",
            "reason": "qualify the already-current activation",
            "not_before_business_day": "2026-09-08",
        }

    def _body(
        self,
        *,
        command_id: str = "hold-command-001",
        drain_id: str = "activation-2-hold-1",
        use_binding_alias: bool = False,
    ) -> dict:
        parameters_key = "binding" if use_binding_alias else "parameters"
        return {
            "command_id": command_id,
            "command": "hold_begin",
            parameters_key: self._parameters(drain_id=drain_id),
        }

    def _command_rows(self) -> list[dict]:
        with connect(self.db) as connection:
            rows = connection.execute(
                "SELECT * FROM scheduler_runs WHERE job_id=? ORDER BY id",
                (COMMAND_JOB,),
            ).fetchall()
        return [dict(row) for row in rows]

    def _attempt_count(self, run_id: int) -> int:
        with connect(self.db) as connection:
            return int(
                connection.execute(
                    "SELECT COUNT(*) FROM scheduler_run_attempts "
                    "WHERE scheduler_run_id=?",
                    (run_id,),
                ).fetchone()[0]
            )

    def _drain_event_count(self) -> int:
        with connect(self.db) as connection:
            return int(
                connection.execute(
                    "SELECT COUNT(*) FROM pipeline_paid_drain_events"
                ).fetchone()[0]
            )

    def test_post_only_persists_queued_command_and_returns_run_id(self) -> None:
        before_drain_events = self._drain_event_count()
        self.app.state.writer_lock_held = True
        self.app.state.current_hold_control_executor = object()
        with patch.object(
            api_module, "_submit_current_hold_control", return_value=None
        ):
            response = self.client.post(
                COMMANDS_PATH,
                json=self._body(use_binding_alias=True),
            )

        self.assertEqual(response.status_code, 202, response.text)
        run_id = response.json()["run_id"]
        self.assertEqual(response.json(), {"run_id": run_id, "status": "pending"})
        self.assertIsInstance(run_id, int)

        rows = self._command_rows()
        self.assertEqual(len(rows), 1)
        self.assertEqual(rows[0]["id"], run_id)
        self.assertEqual(rows[0]["status"], "interrupted")
        details = json.loads(rows[0]["details_json"])
        self.assertEqual(details["contract_version"], COMMAND_CONTRACT)
        self.assertEqual(details["state"], "queued")
        self.assertEqual(details["binding"]["command_id"], "hold-command-001")
        self.assertEqual(details["binding"]["command"], "hold_begin")
        self.assertEqual(details["binding"]["parameters"], self._parameters())
        self.assertEqual(self._attempt_count(run_id), 0)
        self.assertEqual(self._drain_event_count(), before_drain_events)

    def test_post_requires_writer_lock_and_read_only_mode_is_forbidden(self) -> None:
        without_lock = self.client.post(COMMANDS_PATH, json=self._body())
        self.assertEqual(without_lock.status_code, 503, without_lock.text)

        read_only_app = api_module.create_app(replace(self.config, read_only=True))
        read_only_app.state.writer_lock_held = True
        read_only_client = TestClient(read_only_app)
        self.addCleanup(read_only_client.close)
        read_only = read_only_client.post(COMMANDS_PATH, json=self._body())
        self.assertEqual(read_only.status_code, 403, read_only.text)
        self.assertEqual(self._command_rows(), [])

    def test_get_is_pure_read_and_never_claims_queued_command(self) -> None:
        queued = profile_control.enqueue_current_activation_hold_command(
            db_path=self.db,
            command_id="hold-command-read-only",
            command="hold_begin",
            parameters=self._parameters(),
            submitted_at=SUBMITTED_AT,
        )
        run_id = int(queued["run_id"])
        before_row = self._command_rows()[0]
        before_attempts = self._attempt_count(run_id)
        self.app.state.writer_lock_held = False

        with patch.object(
            api_module,
            "process_current_activation_hold_commands",
            side_effect=AssertionError("GET claimed a durable command"),
        ):
            response = self.client.get(f"{COMMANDS_PATH}/{run_id}")

        self.assertEqual(response.status_code, 200, response.text)
        body = response.json()
        self.assertEqual(body["run_id"], run_id)
        self.assertEqual(body["status"], "queued")
        self.assertEqual(body["command_id"], "hold-command-read-only")
        self.assertEqual(body["command"], "hold_begin")
        self.assertEqual(body["attempts"], [])
        self.assertEqual(self._command_rows()[0], before_row)
        self.assertEqual(self._attempt_count(run_id), before_attempts)

    def test_executor_is_bounded_and_restart_does_not_reexecute_terminal_runs(
        self,
    ) -> None:
        first = profile_control.enqueue_current_activation_hold_command(
            db_path=self.db,
            command_id="hold-command-bounded-1",
            command="hold_begin",
            parameters=self._parameters(drain_id="activation-2-hold-bounded-1"),
            submitted_at=SUBMITTED_AT,
        )
        repeated = profile_control.enqueue_current_activation_hold_command(
            db_path=self.db,
            command_id="hold-command-bounded-1",
            command="hold_begin",
            parameters=self._parameters(drain_id="activation-2-hold-bounded-1"),
            submitted_at=SUBMITTED_AT,
        )
        second = profile_control.enqueue_current_activation_hold_command(
            db_path=self.db,
            command_id="hold-command-bounded-2",
            command="hold_begin",
            parameters=self._parameters(drain_id="activation-2-hold-bounded-2"),
            submitted_at=SUBMITTED_AT,
        )
        self.assertEqual(repeated["run_id"], first["run_id"])

        fake_result = {
            "activation": {"activation_id": 2},
            "start": {"event_id": 10, "event_hash": "c" * 64},
        }
        with patch.object(
            profile_control,
            "begin_current_activation_hold",
            return_value=fake_result,
        ) as begin:
            first_tick = profile_control.process_current_activation_hold_commands(
                db_path=self.db, limit=1
            )
            second_tick_after_restart = (
                profile_control.process_current_activation_hold_commands(
                    db_path=self.db, limit=1
                )
            )
            empty_tick_after_restart = (
                profile_control.process_current_activation_hold_commands(
                    db_path=self.db, limit=1
                )
            )

        self.assertEqual(first_tick["count"], 1)
        self.assertEqual(second_tick_after_restart["count"], 1)
        self.assertEqual(empty_tick_after_restart, {"processed": [], "count": 0})
        self.assertEqual(
            [item["run_id"] for item in first_tick["processed"]],
            [first["run_id"]],
        )
        self.assertEqual(
            [item["run_id"] for item in second_tick_after_restart["processed"]],
            [second["run_id"]],
        )
        self.assertEqual(begin.call_count, 2)
        self.assertEqual(self._attempt_count(int(first["run_id"])), 1)
        self.assertEqual(self._attempt_count(int(second["run_id"])), 1)
        self.assertEqual(
            [row["status"] for row in self._command_rows()],
            ["succeeded", "succeeded"],
        )

    def test_post_existing_terminal_commands_returns_their_real_status(self) -> None:
        self.app.state.writer_lock_held = True
        self.app.state.current_hold_control_executor = object()
        failed = profile_control.ProfileControlError(
            "fixture_terminal_failure", "fixture terminal failure"
        )
        with patch.object(api_module, "_submit_current_hold_control") as wake_executor:
            succeeded_post = self.client.post(
                COMMANDS_PATH,
                json=self._body(
                    command_id="hold-command-terminal-succeeded",
                    drain_id="activation-2-hold-terminal-succeeded",
                ),
            )
            failed_post = self.client.post(
                COMMANDS_PATH,
                json=self._body(
                    command_id="hold-command-terminal-failed",
                    drain_id="activation-2-hold-terminal-failed",
                ),
            )
            self.assertEqual(wake_executor.call_count, 2)

            with patch.object(
                profile_control,
                "begin_current_activation_hold",
                side_effect=[
                    {
                        "activation": {"activation_id": 2},
                        "start": {"event_id": 10, "event_hash": "c" * 64},
                    },
                    failed,
                ],
            ):
                processed = profile_control.process_current_activation_hold_commands(
                    db_path=self.db, limit=2
                )
            self.assertEqual(
                [item["status"] for item in processed["processed"]],
                ["succeeded", "failed"],
            )

            wake_executor.reset_mock()
            succeeded_repeat = self.client.post(
                COMMANDS_PATH,
                json=self._body(
                    command_id="hold-command-terminal-succeeded",
                    drain_id="activation-2-hold-terminal-succeeded",
                ),
            )
            failed_repeat = self.client.post(
                COMMANDS_PATH,
                json=self._body(
                    command_id="hold-command-terminal-failed",
                    drain_id="activation-2-hold-terminal-failed",
                ),
            )

        self.assertEqual(succeeded_post.status_code, 202, succeeded_post.text)
        self.assertEqual(failed_post.status_code, 202, failed_post.text)
        self.assertEqual(succeeded_repeat.status_code, 202, succeeded_repeat.text)
        self.assertEqual(failed_repeat.status_code, 202, failed_repeat.text)
        self.assertEqual(succeeded_repeat.json()["status"], "succeeded")
        self.assertEqual(failed_repeat.json()["status"], "failed")
        wake_executor.assert_not_called()

    def test_same_command_id_with_changed_binding_is_rejected(self) -> None:
        self.app.state.writer_lock_held = True
        self.app.state.current_hold_control_executor = object()
        with patch.object(
            api_module, "_submit_current_hold_control", return_value=None
        ):
            accepted = self.client.post(COMMANDS_PATH, json=self._body())
            changed = self._body()
            changed["parameters"]["reason"] = "different operator intent"
            rejected = self.client.post(COMMANDS_PATH, json=changed)

        self.assertEqual(accepted.status_code, 202, accepted.text)
        self.assertEqual(rejected.status_code, 409, rejected.text)
        rows = self._command_rows()
        self.assertEqual(len(rows), 1)
        details = json.loads(rows[0]["details_json"])
        self.assertEqual(
            details["binding"]["parameters"]["reason"],
            "qualify the already-current activation",
        )
        self.assertEqual(self._attempt_count(int(rows[0]["id"])), 0)


if __name__ == "__main__":
    unittest.main()
