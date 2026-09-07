from __future__ import annotations

import hashlib
import json
import tempfile
import unittest
from contextlib import contextmanager
from pathlib import Path
from typing import Any, Callable
from unittest.mock import MagicMock, patch

from v8 import capture, profile_control, scheduler
from v8.paid_drain import (
    PaidDrainBlocked,
    dispatch_state,
    issue_activation_permit_in_transaction,
    require_paid_dispatch_open,
)
from v8.paid_dispatch import (
    mark_dispatch_sent_in_transaction,
    reserve_dispatch_in_transaction,
)
from v8.profile_activations import (
    MATRIX_PROFILE,
    TIKHUB_PROFILE,
    activation_at,
    append_activation,
)
from v8.profile_control import (
    ProfileControlError,
    begin_cross_profile_switch,
    complete_cross_profile_switch,
)
from v8.provider_budget import PRICES_MICROUSD, record_circuit
from v8.storage import connect, initialize_database, transaction


CURRENT_HOLD_CONTRACT = "current_activation_hold_v1"
INITIAL_AT = "2026-09-01T00:00:00.000000Z"
LEGACY_SWITCH_AT = "2026-09-05T03:03:46Z"
LEGACY_RELEASE_AT = "2026-09-05T03:03:55Z"
HOLD_BEGIN_AT = "2026-09-06T04:00:00Z"
HOLD_SEAL_AT = "2026-09-06T06:00:00Z"
REOPEN_AT = "2026-09-07T12:00:00Z"
NOT_BEFORE_BUSINESS_DAY = "2026-09-08"
HOLD_ID = "current-activation-2-recovery-1"
REOPENED_HOLD_ID = "current-activation-2-recovery-2"
ACTOR = "fixture-release-owner"

LEGACY_BUILD = "6" * 64
LEGACY_RUNTIME = "7" * 64
HOLD_BUILD = "a" * 64
HOLD_RUNTIME = "b" * 64
ADVANCED_BUILD = "c" * 64
ADVANCED_RUNTIME = "d" * 64
CONFIG_RECEIPT = "e" * 64
QUALIFICATION_RECEIPT = "f" * 64
CAPACITY_RECEIPT = "0" * 64
PRICE_RECEIPT = "1" * 64
BUDGET_RECEIPT = "2" * 64


class CurrentActivationHoldTest(unittest.TestCase):
    """Failure-first contract tests for the schema-19 current hold control."""

    def setUp(self) -> None:
        self.temp = tempfile.TemporaryDirectory()
        self.addCleanup(self.temp.cleanup)
        self.root = Path(self.temp.name)
        self.db = self.root / "current-hold.sqlite3"

        with connect(self.db) as connection:
            initialize_database(connection)
            matrix = self._snapshot(connection, "matrix", "matrix-roster-1")
            system = self._snapshot(connection, "system", "system-roster-2")
            connection.commit()
            with transaction(connection):
                initial = append_activation(
                    connection,
                    profile_id=MATRIX_PROFILE,
                    roster_snapshot_id=int(matrix["id"]),
                    roster_members_sha256=str(matrix["members_sha256"]),
                    effective_at=INITIAL_AT,
                    build_receipt_sha256=LEGACY_BUILD,
                    actor="fixture",
                    reason="initial Matrix activation",
                    created_at=INITIAL_AT,
                )
                issue_activation_permit_in_transaction(
                    connection,
                    activation_id=int(initial["activation_id"]),
                    drain_id="fixture-profile:1",
                    source_activation_id=int(initial["activation_id"]),
                    business_day="2026-09-01",
                    planned_effective_at=INITIAL_AT,
                    build_receipt_sha256=LEGACY_BUILD,
                    runtime_root_receipt_sha256=LEGACY_RUNTIME,
                    now=INITIAL_AT,
                )

        # Reproduce the already-effective Mode-B activation and its historical
        # immediate RELEASE.  That RELEASE is deliberately not a current-hold
        # full-day permit.
        switched = begin_cross_profile_switch(
            db_path=self.db,
            drain_id="legacy-immediate-mode-b",
            target_profile_id=TIKHUB_PROFILE,
            roster_snapshot_id=int(system["id"]),
            build_receipt_sha256=LEGACY_BUILD,
            runtime_root_receipt_sha256=LEGACY_RUNTIME,
            actor="fixture",
            reason="historical immediate Mode-B switch",
            now=LEGACY_SWITCH_AT,
            effective_now=True,
        )
        completed = complete_cross_profile_switch(
            db_path=self.db,
            drain_id="legacy-immediate-mode-b",
            now=LEGACY_RELEASE_AT,
            mirror_root=self.root / "legacy-switch-mirrors",
        )
        self.activation_id = int(switched["activation"]["activation_id"])
        self.legacy_release_id = int(completed["release"]["event_id"])
        self.assertEqual(self.activation_id, 2)

    def _snapshot(self, connection: Any, family: str, key: str) -> dict[str, object]:
        source = json.dumps({"family": family, "key": key}).encode()
        source_hash = hashlib.sha256(source).hexdigest()
        members_hash = hashlib.sha256(key.encode()).hexdigest()
        path = self.root / f"{key}.json"
        path.write_bytes(source)
        source_type = "system_managed" if family == "system" else "manual_export"
        cursor = connection.execute(
            """INSERT INTO account_roster_snapshots(
                   source_family,source_type,scope_key,scope_json,source_instance_id,
                   source_captured_at,accepted_at,declared_count,member_count,
                   members_sha256,source_sha256,source_path,contract_version,metadata_json)
               VALUES (?,?,?,'{}',?,'2026-09-01T00:00:00Z',
                       '2026-09-01T00:00:00Z',0,0,?,?,?,'account-roster-v2','{}')""",
            (family, source_type, key, key, members_hash, source_hash, str(path)),
        )
        return {"id": int(cursor.lastrowid or 0), "members_sha256": members_hash}

    def _command(self, name: str) -> Callable[..., dict[str, Any]]:
        command = getattr(profile_control, name, None)
        self.assertTrue(
            callable(command),
            f"required production interface v8.profile_control.{name} is missing",
        )
        return command

    def _event(
        self, result: dict[str, Any], key: str, event_type: str
    ) -> dict[str, Any]:
        self.assertIsInstance(result, dict)
        self.assertIn(key, result)
        event = result[key]
        self.assertIsInstance(event, dict)
        self.assertIsInstance(event.get("event_id"), int)
        self.assertRegex(str(event.get("event_hash", "")), r"^[0-9a-f]{64}$")
        self.assertEqual(event.get("event_type"), event_type)
        self.assertIsInstance(event.get("payload"), dict)
        return event

    def _state(self, at: str):
        with connect(self.db) as connection:
            return dispatch_state(connection, at=at)

    def _activation_count(self) -> int:
        with connect(self.db) as connection:
            return int(
                connection.execute(
                    "SELECT COUNT(*) FROM acquisition_profile_activations"
                ).fetchone()[0]
            )

    def _hold_event_rows(self, *drain_ids: str) -> list[dict[str, Any]]:
        placeholders = ",".join("?" for _ in drain_ids)
        with connect(self.db) as connection:
            rows = connection.execute(
                "SELECT * FROM pipeline_paid_drain_events "
                f"WHERE drain_id IN ({placeholders}) ORDER BY id",
                drain_ids,
            ).fetchall()
        return [dict(row) for row in rows]

    def _insert_matrix_run(
        self,
        *,
        scheduled_for: str,
        network_requests: int,
        status: str = "succeeded",
        details_network_requests: int | None = None,
    ) -> tuple[int, int]:
        started_at = "2026-09-06T04:10:00Z"
        completed_at = None if status == "running" else "2026-09-06T04:10:01Z"
        run_details = json.dumps(
            {"checkpoint": {"network_requests": network_requests}}, sort_keys=True
        )
        attempt_details = json.dumps(
            {
                "checkpoint": {
                    "network_requests": (
                        network_requests
                        if details_network_requests is None
                        else details_network_requests
                    )
                }
            },
            sort_keys=True,
        )
        with connect(self.db) as connection, transaction(connection):
            run = connection.execute(
                "INSERT INTO scheduler_runs(job_id,scheduled_for,status,started_at,"
                "completed_at,details_json) VALUES "
                "('matrix_works_scan',?,?,?,?,?)",
                (scheduled_for, status, started_at, completed_at, run_details),
            )
            run_id = int(run.lastrowid or 0)
            attempt = connection.execute(
                "INSERT INTO scheduler_run_attempts(scheduler_run_id,attempt_number,"
                "invocation_source,status,started_at,completed_at,details_json) "
                "VALUES (?,1,'scheduled',?,?,?,?)",
                (
                    run_id,
                    status,
                    started_at,
                    completed_at,
                    attempt_details,
                ),
            )
            return run_id, int(attempt.lastrowid or 0)

    def _begin(self) -> dict[str, Any]:
        return self._command("begin_current_activation_hold")(
            db_path=self.db,
            drain_id=HOLD_ID,
            build_receipt_sha256=HOLD_BUILD,
            runtime_root_receipt_sha256=HOLD_RUNTIME,
            actor=ACTOR,
            reason="qualify the already-current activation",
            not_before_business_day=NOT_BEFORE_BUSINESS_DAY,
            now=HOLD_BEGIN_AT,
        )

    def _seal(self) -> dict[str, Any]:
        self._register_prerequisites()
        result = self._command("seal_current_activation_hold")(
            db_path=self.db,
            drain_id=HOLD_ID,
            final_build_receipt_sha256=HOLD_BUILD,
            runtime_root_receipt_sha256=HOLD_RUNTIME,
            config_receipt_sha256=CONFIG_RECEIPT,
            qualification_receipt_sha256=QUALIFICATION_RECEIPT,
            capacity_receipt_sha256=CAPACITY_RECEIPT,
            price_receipt_sha256=PRICE_RECEIPT,
            budget_receipt_sha256=BUDGET_RECEIPT,
            actor=ACTOR,
            now=HOLD_SEAL_AT,
        )
        sealed = self._event(result, "sealed", "sealed")
        payload = sealed["payload"]
        self.assertEqual(payload["control_contract_version"], CURRENT_HOLD_CONTRACT)
        self.assertEqual(payload["final_build_receipt_sha256"], HOLD_BUILD)
        self.assertEqual(payload["runtime_root_receipt_sha256"], HOLD_RUNTIME)
        self.assertEqual(payload["config_receipt_sha256"], CONFIG_RECEIPT)
        self.assertEqual(payload["qualification_receipt_sha256"], QUALIFICATION_RECEIPT)
        self.assertEqual(payload["capacity_receipt_sha256"], CAPACITY_RECEIPT)
        self.assertEqual(payload["price_receipt_sha256"], PRICE_RECEIPT)
        self.assertEqual(payload["budget_receipt_sha256"], BUDGET_RECEIPT)
        self.assertEqual(self._state(HOLD_SEAL_AT).state, "sealed")
        return sealed

    def _register_prerequisites(
        self, *, expires_at: str = "2026-09-10T00:00:00Z"
    ) -> None:
        artifacts = {
            "build": HOLD_BUILD,
            "runtime": HOLD_RUNTIME,
            "config": CONFIG_RECEIPT,
            "qualification": QUALIFICATION_RECEIPT,
            "capacity": CAPACITY_RECEIPT,
            "price": PRICE_RECEIPT,
            "budget": BUDGET_RECEIPT,
        }
        for kind, artifact in artifacts.items():
            evidence: dict[str, Any] = {
                "valid": True,
                "readback": True,
                "artifact_sha256": artifact,
            }
            if kind == "qualification":
                evidence["required_operations"] = [
                    {"operation": operation, "qualified": True}
                    for operation in PRICES_MICROUSD
                ]
            elif kind == "capacity":
                evidence.update(
                    archive_independent=True,
                    archive_recovery_tested=True,
                    runway_days=90,
                )
            elif kind == "price":
                evidence["prices_microusd"] = dict(PRICES_MICROUSD)
            elif kind == "budget":
                evidence.update(
                    caps_microusd={
                        "discovery": 30_000_000,
                        "metrics": 15_000_000,
                        "automatic_repair": 0,
                        "automatic_total": 50_000_000,
                    },
                    forecast_microusd=44_246_200,
                )
            profile_control.record_current_activation_hold_prerequisite(
                db_path=self.db,
                drain_id=HOLD_ID,
                kind=kind,
                artifact_sha256=artifact,
                receipt_contract_version=(
                    profile_control.CURRENT_HOLD_PREREQUISITE_CONTRACTS[kind]
                ),
                expires_at=expires_at,
                evidence=evidence,
                actor=ACTOR,
                now="2026-09-06T05:00:00Z",
            )

    def test_missing_current_hold_fails_closed_before_any_paid_dispatch(self) -> None:
        state = self._state(HOLD_BEGIN_AT)
        self.assertEqual(state.state, "closed")
        self.assertEqual(state.reason, "current_activation_hold_missing")
        self.assertIsNone(state.permit_event_id)

        with connect(self.db) as connection, transaction(connection):
            with self.assertRaises(PaidDrainBlocked) as blocked:
                require_paid_dispatch_open(
                    connection,
                    provider="TikHub",
                    operation="douyin_user_posts",
                    at=HOLD_BEGIN_AT,
                )
        self.assertEqual(
            blocked.exception.error_code, "current_activation_hold_missing"
        )

    def test_capture_entry_missing_current_hold_has_no_provider_side_effects(
        self,
    ) -> None:
        network_send = MagicMock(name="provider_network_send")

        with (
            patch.object(capture, "now_utc", return_value=HOLD_BEGIN_AT),
            patch("v8.provider_budget.now_utc", return_value=HOLD_BEGIN_AT),
            self.assertRaises(PaidDrainBlocked) as blocked,
        ):
            capture.execute_content_fetch(
                content_id=1,
                stage="detail",
                window_key="current-activation-hold-missing",
                provider="TikHub",
                adapter_version="test-v1",
                operation="douyin_video_detail",
                call=network_send,
                db_path=self.db,
                raw_root=self.root / "raw",
                budget_id="fixture-budget",
            )

        self.assertEqual(
            blocked.exception.error_code, "current_activation_hold_missing"
        )
        network_send.assert_not_called()
        with connect(self.db) as connection:
            counts = {
                table: int(
                    connection.execute(f"SELECT COUNT(*) FROM {table}").fetchone()[0]
                )
                for table in (
                    "provider_usage",
                    "fetch_attempts",
                    "paid_provider_dispatch_events",
                    "fetch_slots",
                )
            }
        self.assertEqual(
            counts,
            {
                "provider_usage": 0,
                "fetch_attempts": 0,
                "paid_provider_dispatch_events": 0,
                "fetch_slots": 0,
            },
        )

    def test_legacy_release_for_current_activation_does_not_satisfy_hold(self) -> None:
        with connect(self.db) as connection:
            legacy = connection.execute(
                "SELECT * FROM pipeline_paid_drain_events WHERE id=?",
                (self.legacy_release_id,),
            ).fetchone()
        self.assertIsNotNone(legacy)
        assert legacy is not None
        self.assertEqual(legacy["target_activation_id"], self.activation_id)
        self.assertEqual(legacy["event_type"], "release")
        payload = json.loads(str(legacy["payload_json"]))
        self.assertNotEqual(
            payload.get("control_contract_version"), CURRENT_HOLD_CONTRACT
        )

        state = self._state(HOLD_BEGIN_AT)
        self.assertEqual(state.state, "closed")
        self.assertEqual(state.reason, "current_activation_hold_missing")
        self.assertIsNone(state.permit_event_id)

    def test_hold_begin_reuses_current_activation_and_enters_draining(self) -> None:
        before_count = self._activation_count()
        result = self._begin()
        start = self._event(result, "start", "start")

        self.assertEqual(result["activation"]["activation_id"], self.activation_id)
        self.assertEqual(start["target_activation_id"], self.activation_id)
        self.assertEqual(self._activation_count(), before_count)
        self.assertEqual(before_count, 2)
        payload = start["payload"]
        self.assertEqual(payload["control_contract_version"], CURRENT_HOLD_CONTRACT)
        binding = payload["binding"]
        self.assertEqual(binding["source_activation_id"], self.activation_id)
        self.assertEqual(binding["target_activation_id"], self.activation_id)
        self.assertEqual(binding["build_receipt_sha256"], HOLD_BUILD)
        self.assertEqual(binding["runtime_root_receipt_sha256"], HOLD_RUNTIME)
        self.assertEqual(binding["not_before_business_day"], NOT_BEFORE_BUSINESS_DAY)

        with connect(self.db) as connection:
            active = activation_at(connection, HOLD_BEGIN_AT)
        self.assertIsNotNone(active)
        assert active is not None
        self.assertEqual(active["activation_id"], self.activation_id)
        state = self._state(HOLD_BEGIN_AT)
        self.assertEqual((state.state, state.drain_id), ("draining", HOLD_ID))

    def test_hold_begin_ignores_a_prewritten_future_activation_permit(self) -> None:
        with connect(self.db) as connection:
            future_roster = self._snapshot(
                connection, "system", "system-roster-future"
            )
            connection.commit()
        scheduled = profile_control.schedule_same_profile_roster_activation(
            db_path=self.db,
            roster_snapshot_id=int(future_roster["id"]),
            build_receipt_sha256=LEGACY_BUILD,
            runtime_root_receipt_sha256=LEGACY_RUNTIME,
            actor=ACTOR,
            reason="prewrite the next-midnight activation permit",
            now=HOLD_BEGIN_AT,
        )
        future_activation_id = int(scheduled["activation"]["activation_id"])
        future_release_id = int(scheduled["release"]["event_id"])
        self.assertNotEqual(future_activation_id, self.activation_id)
        self.assertGreater(future_release_id, self.legacy_release_id)

        started = self._event(self._begin(), "start", "start")
        control = started["payload"]["control"]

        self.assertEqual(started["target_activation_id"], self.activation_id)
        self.assertEqual(control["activation_id"], self.activation_id)
        self.assertEqual(
            control["previous_release_event_id"], self.legacy_release_id
        )
        self.assertNotEqual(
            control["previous_release_event_id"], future_release_id
        )
        with connect(self.db) as connection:
            current = activation_at(connection, HOLD_BEGIN_AT)
        self.assertIsNotNone(current)
        assert current is not None
        self.assertEqual(current["activation_id"], self.activation_id)

    def test_hold_seal_rejects_missing_registered_prerequisites(self) -> None:
        self._begin()

        with self.assertRaises(ProfileControlError) as caught:
            self._command("seal_current_activation_hold")(
                db_path=self.db,
                drain_id=HOLD_ID,
                final_build_receipt_sha256=HOLD_BUILD,
                runtime_root_receipt_sha256=HOLD_RUNTIME,
                config_receipt_sha256=CONFIG_RECEIPT,
                qualification_receipt_sha256=QUALIFICATION_RECEIPT,
                capacity_receipt_sha256=CAPACITY_RECEIPT,
                price_receipt_sha256=PRICE_RECEIPT,
                budget_receipt_sha256=BUDGET_RECEIPT,
                actor=ACTOR,
                now=HOLD_SEAL_AT,
            )

        self.assertEqual(caught.exception.code, "current_hold_prerequisite_missing")
        self.assertEqual(self._state(HOLD_SEAL_AT).state, "draining")
        self.assertEqual(
            [row["event_type"] for row in self._hold_event_rows(HOLD_ID)],
            ["start"],
        )

    def test_hold_build_advance_writes_idempotent_durable_receipt(self) -> None:
        self._begin()
        for kind, artifact in {
            "build": ADVANCED_BUILD,
            "runtime": ADVANCED_RUNTIME,
            "config": CONFIG_RECEIPT,
        }.items():
            profile_control.record_current_activation_hold_prerequisite(
                db_path=self.db,
                drain_id=HOLD_ID,
                kind=kind,
                artifact_sha256=artifact,
                receipt_contract_version=(
                    profile_control.CURRENT_HOLD_PREREQUISITE_CONTRACTS[kind]
                ),
                expires_at="2026-09-10T00:00:00Z",
                evidence={
                    "valid": True,
                    "readback": True,
                    "artifact_sha256": artifact,
                },
                actor=ACTOR,
                generation=2,
                now="2026-09-06T04:30:00Z",
            )
        advance = self._command("advance_current_activation_hold_build")
        arguments = {
            "db_path": self.db,
            "drain_id": HOLD_ID,
            "from_build_receipt_sha256": HOLD_BUILD,
            "to_build_receipt_sha256": ADVANCED_BUILD,
            "runtime_root_receipt_sha256": ADVANCED_RUNTIME,
            "config_receipt_sha256": CONFIG_RECEIPT,
            "actor": ACTOR,
            "reason": "diagnostic transport implementation changed",
            "now": "2026-09-06T05:00:00Z",
        }
        first = self._event(advance(**arguments), "build_advance", "build_advance")
        payload = first["payload"]
        self.assertEqual(payload["control_contract_version"], CURRENT_HOLD_CONTRACT)
        self.assertEqual(payload["from_build_receipt_sha256"], HOLD_BUILD)
        self.assertEqual(payload["to_build_receipt_sha256"], ADVANCED_BUILD)
        self.assertEqual(payload["runtime_root_receipt_sha256"], ADVANCED_RUNTIME)
        self.assertEqual(payload["config_receipt_sha256"], CONFIG_RECEIPT)
        self.assertEqual(payload["actor"], ACTOR)

        # A second path-level call opens a new connection, so equality proves
        # command idempotence is backed by a durable receipt, not process state.
        repeated = self._event(advance(**arguments), "build_advance", "build_advance")
        self.assertEqual(repeated["event_id"], first["event_id"])
        self.assertEqual(repeated["event_hash"], first["event_hash"])
        self.assertEqual(self._state(arguments["now"]).state, "draining")
        self.assertEqual(self._activation_count(), 2)

    def test_hold_build_advance_rejects_unregistered_next_generation(self) -> None:
        self._begin()

        with self.assertRaises(ProfileControlError) as caught:
            self._command("advance_current_activation_hold_build")(
                db_path=self.db,
                drain_id=HOLD_ID,
                from_build_receipt_sha256=HOLD_BUILD,
                to_build_receipt_sha256=ADVANCED_BUILD,
                runtime_root_receipt_sha256=ADVANCED_RUNTIME,
                config_receipt_sha256=CONFIG_RECEIPT,
                actor=ACTOR,
                reason="unregistered candidate build",
                now="2026-09-06T05:00:00Z",
            )

        self.assertEqual(caught.exception.code, "current_hold_prerequisite_missing")
        self.assertEqual(self._state("2026-09-06T05:00:00Z").state, "draining")

    def test_matrix_zero_network_terminal_does_not_block_seal(self) -> None:
        self._begin()
        self._insert_matrix_run(
            scheduled_for="matrix-local-close", network_requests=0
        )
        self._seal()
        self.assertEqual(self._state(HOLD_SEAL_AT).state, "sealed")

    def test_matrix_network_delta_blocks_seal(self) -> None:
        self._begin()
        self._insert_matrix_run(
            scheduled_for="matrix-network-delta", network_requests=1
        )
        with self.assertRaises(ProfileControlError) as caught:
            self._seal()
        self.assertEqual(caught.exception.code, "current_hold_matrix_delta")
        self.assertEqual(self._state(HOLD_SEAL_AT).state, "draining")

    def test_matrix_raw_delta_uses_the_real_provider_identifier(self) -> None:
        self._begin()
        with connect(self.db) as connection, transaction(connection):
            connection.execute(
                "INSERT INTO provider_raw_responses(provider,operation,local_path,"
                "sha256,byte_size,http_status,captured_at,source) VALUES "
                "('newrank_matrix','matrix_works_list',?,?,2,200,?,"
                "'matrix_page_pending')",
                (
                    str(self.root / "matrix-after-start.json"),
                    hashlib.sha256(b"{}").hexdigest(),
                    "2026-09-06T04:10:01Z",
                ),
            )
        with self.assertRaises(ProfileControlError) as caught:
            self._seal()
        self.assertEqual(caught.exception.code, "current_hold_matrix_delta")
        self.assertEqual(self._state(HOLD_SEAL_AT).state, "draining")

    def test_matrix_terminal_run_attempt_mismatch_fails_closed(self) -> None:
        self._insert_matrix_run(
            scheduled_for="matrix-evidence-mismatch",
            network_requests=0,
            details_network_requests=1,
        )
        with self.assertRaises(ProfileControlError) as caught:
            self._begin()
        self.assertEqual(caught.exception.code, "current_hold_matrix_evidence_invalid")
        self.assertEqual(self._hold_event_rows(HOLD_ID), [])

    def test_hold_begin_rejects_running_matrix_attempt(self) -> None:
        self._insert_matrix_run(
            scheduled_for="matrix-still-running",
            network_requests=0,
            status="running",
        )
        with self.assertRaises(ProfileControlError) as caught:
            self._begin()
        self.assertEqual(caught.exception.code, "current_hold_matrix_inflight")
        self.assertEqual(self._hold_event_rows(HOLD_ID), [])

    def test_hold_begin_rejects_terminal_matrix_send_without_settlement(self) -> None:
        self._begin()
        self._seal()
        self._command("release_current_activation_hold")(
            db_path=self.db,
            drain_id=HOLD_ID,
            actor=ACTOR,
            now="2026-09-07T16:04:59Z",
        )
        run_id, attempt_id = self._insert_matrix_run(
            scheduled_for="matrix-crash-before-checkpoint",
            network_requests=0,
            status="running",
        )
        with connect(self.db) as connection, transaction(connection):
            reserved = reserve_dispatch_in_transaction(
                connection,
                provider="newrank_matrix",
                operation="matrix_works_list",
                activation_id=self.activation_id,
                business_day="2026-09-08",
                scheduler_run_id=run_id,
                scheduler_attempt_id=attempt_id,
                scope={"fixture": "matrix-crash-before-checkpoint"},
                cursor_identity={"page_index": 0, "request_number": 1},
                created_at="2026-09-07T16:10:00Z",
            )
            assert reserved is not None
            mark_dispatch_sent_in_transaction(
                connection,
                reserved.dispatch_id,
                fetch_attempt_id=None,
                created_at="2026-09-07T16:10:01Z",
            )
            connection.execute(
                "UPDATE scheduler_run_attempts SET status='interrupted',completed_at=? "
                "WHERE id=? AND status='running'",
                ("2026-09-07T16:10:02Z", attempt_id),
            )
            connection.execute(
                "UPDATE scheduler_runs SET status='interrupted',completed_at=? "
                "WHERE id=? AND status='running'",
                ("2026-09-07T16:10:02Z", run_id),
            )

        with self.assertRaises(ProfileControlError) as caught:
            self._command("begin_current_activation_hold")(
                db_path=self.db,
                drain_id="current-activation-2-recovery-after-matrix-crash",
                build_receipt_sha256=ADVANCED_BUILD,
                runtime_root_receipt_sha256=ADVANCED_RUNTIME,
                actor=ACTOR,
                reason="close after Matrix crash",
                not_before_business_day="2026-09-09",
                now="2026-09-07T17:00:00Z",
            )

        self.assertEqual(caught.exception.code, "current_hold_matrix_inflight")
        self.assertEqual(
            [row["event_type"] for row in self._hold_event_rows(HOLD_ID)],
            ["start", "sealed", "release"],
        )

    def test_release_rejects_before_not_before_and_outside_window(self) -> None:
        self._begin()
        self._seal()

        for timestamp in (
            "2026-09-06T16:00:00Z",  # Beijing 09-07 00:00, before not-before.
            "2026-09-07T15:59:59Z",  # Beijing 09-07 23:59:59, T-1 second.
            "2026-09-07T16:05:00Z",  # Beijing 09-08 00:05:00, first outside.
        ):
            with self.subTest(timestamp=timestamp):
                with self.assertRaises(ProfileControlError) as caught:
                    self._command("release_current_activation_hold")(
                        db_path=self.db,
                        drain_id=HOLD_ID,
                        actor=ACTOR,
                        now=timestamp,
                    )
                self.assertRegex(
                    caught.exception.code, r"^current_activation_hold_release_"
                )
                self.assertEqual(self._state(timestamp).state, "sealed")
                self.assertEqual(
                    [row["event_type"] for row in self._hold_event_rows(HOLD_ID)],
                    ["start", "sealed"],
                )

    def test_release_accepts_last_second_of_midnight_window(self) -> None:
        self._begin()
        self._seal()
        released_at = "2026-09-07T16:04:59Z"  # Beijing 09-08 00:04:59.
        result = self._command("release_current_activation_hold")(
            db_path=self.db,
            drain_id=HOLD_ID,
            actor=ACTOR,
            now=released_at,
        )
        release = self._event(result, "release", "release")
        self.assertEqual(
            release["payload"]["control_contract_version"], CURRENT_HOLD_CONTRACT
        )
        self.assertEqual(release["payload"]["control_purpose"], "full_day_release")
        state = self._state(released_at)
        self.assertEqual(state.state, "open")
        self.assertEqual(state.activation_id, self.activation_id)
        self.assertEqual(state.permit_event_id, release["event_id"])
        self.assertEqual(self._activation_count(), 2)

    def test_release_reads_production_clock_after_begin_immediate(self) -> None:
        self._begin()
        self._seal()
        transaction_entered = False
        actual_transaction = profile_control.transaction

        @contextmanager
        def observed_transaction(connection: Any):
            nonlocal transaction_entered
            with actual_transaction(connection) as active_transaction:
                transaction_entered = True
                yield active_transaction

        def production_clock() -> str:
            return (
                "2026-09-07T16:05:00Z"
                if transaction_entered
                else "2026-09-07T16:04:59Z"
            )

        with (
            patch.object(profile_control, "transaction", observed_transaction),
            patch.object(profile_control, "now_utc", side_effect=production_clock),
            self.assertRaises(ProfileControlError) as caught,
        ):
            self._command("release_current_activation_hold")(
                db_path=self.db,
                drain_id=HOLD_ID,
                actor=ACTOR,
            )

        self.assertTrue(transaction_entered)
        self.assertEqual(
            caught.exception.code, "current_activation_hold_release_window"
        )
        self.assertEqual(
            [row["event_type"] for row in self._hold_event_rows(HOLD_ID)],
            ["start", "sealed"],
        )
        self.assertEqual(self._state("2026-09-07T16:05:00Z").state, "sealed")

    def test_release_revalidates_prerequisite_expiry(self) -> None:
        self._begin()
        self._register_prerequisites(expires_at="2026-09-07T15:59:59Z")
        self._command("seal_current_activation_hold")(
            db_path=self.db,
            drain_id=HOLD_ID,
            final_build_receipt_sha256=HOLD_BUILD,
            runtime_root_receipt_sha256=HOLD_RUNTIME,
            config_receipt_sha256=CONFIG_RECEIPT,
            qualification_receipt_sha256=QUALIFICATION_RECEIPT,
            capacity_receipt_sha256=CAPACITY_RECEIPT,
            price_receipt_sha256=PRICE_RECEIPT,
            budget_receipt_sha256=BUDGET_RECEIPT,
            actor=ACTOR,
            now=HOLD_SEAL_AT,
        )
        with self.assertRaises(ProfileControlError) as expired:
            self._command("release_current_activation_hold")(
                db_path=self.db,
                drain_id=HOLD_ID,
                actor=ACTOR,
                now="2026-09-07T16:04:59Z",
            )
        self.assertEqual(expired.exception.code, "current_hold_prerequisite_drift")
        self.assertEqual(self._state("2026-09-07T16:04:59Z").state, "sealed")

    def test_release_rejects_open_provider_circuit(self) -> None:
        self._begin()
        self._seal()
        with connect(self.db) as connection, transaction(connection):
            record_circuit(
                connection,
                reason="provider_balance_blocked",
                usage_id=None,
                at="2026-09-07T16:00:00Z",
            )
        with self.assertRaises(ProfileControlError) as blocked:
            self._command("release_current_activation_hold")(
                db_path=self.db,
                drain_id=HOLD_ID,
                actor=ACTOR,
                now="2026-09-07T16:04:59Z",
            )
        self.assertEqual(blocked.exception.code, "provider_blocked")
        self.assertEqual(self._state("2026-09-07T16:04:59Z").state, "sealed")

    def test_admitted_release_survives_prerequisite_expiry_but_new_fault_blocks(self) -> None:
        self._begin()
        self._seal()
        self._command("release_current_activation_hold")(
            db_path=self.db,
            drain_id=HOLD_ID,
            actor=ACTOR,
            now="2026-09-07T16:04:59Z",
        )

        with connect(self.db) as connection, transaction(connection):
            state = require_paid_dispatch_open(
                connection,
                provider="TikHub",
                operation="douyin_user_posts",
                at="2026-09-10T00:00:01Z",
            )
            self.assertTrue(state.paid_dispatch_open)
            record_circuit(
                connection, reason="provider_balance_blocked", usage_id=None,
                at="2026-09-10T00:00:02Z",
            )
            with self.assertRaises(PaidDrainBlocked) as caught:
                require_paid_dispatch_open(
                    connection,
                    provider="TikHub",
                    operation="douyin_user_posts",
                    at="2026-09-10T00:00:03Z",
                )

        self.assertEqual(caught.exception.error_code, "provider_blocked")

    def test_existing_release_with_different_actor_is_not_idempotent(self) -> None:
        self._begin()
        self._seal()
        released_at = "2026-09-07T16:04:59Z"
        first = self._command("release_current_activation_hold")(
            db_path=self.db,
            drain_id=HOLD_ID,
            actor=ACTOR,
            now=released_at,
        )

        with self.assertRaises(ProfileControlError) as caught:
            self._command("release_current_activation_hold")(
                db_path=self.db,
                drain_id=HOLD_ID,
                actor="different-release-owner",
                now=released_at,
            )

        self.assertEqual(caught.exception.code, "current_hold_idempotency_conflict")
        rows = self._hold_event_rows(HOLD_ID)
        self.assertEqual(
            [row["event_type"] for row in rows],
            ["start", "sealed", "release"],
        )
        self.assertEqual(rows[-1]["id"], first["release"]["event_id"])
        self.assertEqual(self._state(released_at).state, "open")

    def test_restarted_release_does_not_replay_first_claim_time(self) -> None:
        self._begin()
        self._seal()
        queued = profile_control.enqueue_current_activation_hold_command(
            db_path=self.db,
            command_id="hold-release-crash-before-write",
            command="hold_release",
            parameters={"drain_id": HOLD_ID, "actor": ACTOR},
            submitted_at="2026-09-07T16:04:58Z",
        )

        with (
            patch.object(
                profile_control,
                "now_utc",
                return_value="2026-09-07T16:04:59Z",
            ),
            patch.object(
                profile_control,
                "release_current_activation_hold",
                side_effect=SystemExit("simulated writer crash before HOLD_RELEASE"),
            ),
            self.assertRaises(SystemExit),
        ):
            profile_control.process_current_activation_hold_commands(
                db_path=self.db,
                limit=1,
            )

        command = profile_control.read_current_activation_hold_command(
            db_path=self.db,
            run_id=int(queued["run_id"]),
            read_only=False,
        )
        self.assertEqual(command["status"], "running")
        self.assertEqual(
            [row["event_type"] for row in self._hold_event_rows(HOLD_ID)],
            ["start", "sealed"],
        )

        with patch.object(
            scheduler,
            "now_utc",
            return_value="2026-09-07T16:05:00Z",
        ):
            self.assertEqual(
                scheduler.recover_interrupted_scheduler_runs(db_path=self.db),
                1,
            )
        with patch.object(
            profile_control,
            "now_utc",
            return_value="2026-09-07T16:05:00Z",
        ):
            retried = profile_control.process_current_activation_hold_commands(
                db_path=self.db,
                limit=1,
            )

        self.assertEqual(retried["count"], 1)
        self.assertEqual(retried["processed"][0]["status"], "failed")
        self.assertEqual(
            retried["processed"][0]["error"]["code"],
            "current_activation_hold_release_window",
        )
        command = profile_control.read_current_activation_hold_command(
            db_path=self.db,
            run_id=int(queued["run_id"]),
            read_only=False,
        )
        self.assertEqual(command["status"], "failed")
        self.assertEqual(len(command["attempts"]), 2)
        self.assertEqual(
            [row["event_type"] for row in self._hold_event_rows(HOLD_ID)],
            ["start", "sealed"],
        )
        self.assertEqual(self._state("2026-09-07T16:05:00Z").state, "sealed")

    def test_hold_reopen_atomically_ends_in_new_draining_generation(self) -> None:
        self._begin()
        sealed = self._seal()
        result = self._command("reopen_current_activation_hold")(
            db_path=self.db,
            drain_id=HOLD_ID,
            new_drain_id=REOPENED_HOLD_ID,
            build_receipt_sha256=ADVANCED_BUILD,
            runtime_root_receipt_sha256=ADVANCED_RUNTIME,
            actor=ACTOR,
            reason="sealed qualification expired",
            not_before_business_day="2026-09-09",
            now=REOPEN_AT,
        )
        release = self._event(result, "release", "release")
        start = self._event(result, "start", "start")

        self.assertEqual(release["drain_id"], HOLD_ID)
        self.assertEqual(release["previous_event_id"], sealed["event_id"])
        self.assertEqual(release["previous_event_hash"], sealed["event_hash"])
        self.assertEqual(release["payload"]["control_purpose"], "reopen_only")
        self.assertEqual(start["drain_id"], REOPENED_HOLD_ID)
        self.assertEqual(start["target_activation_id"], self.activation_id)
        self.assertEqual(start["previous_event_id"], release["event_id"])
        self.assertEqual(start["previous_event_hash"], release["event_hash"])
        self.assertEqual(
            start["payload"]["binding"]["not_before_business_day"], "2026-09-09"
        )

        rows = self._hold_event_rows(HOLD_ID, REOPENED_HOLD_ID)
        self.assertEqual(
            [(row["drain_id"], row["event_type"]) for row in rows],
            [
                (HOLD_ID, "start"),
                (HOLD_ID, "sealed"),
                (HOLD_ID, "release"),
                (REOPENED_HOLD_ID, "start"),
            ],
        )
        state = self._state(REOPEN_AT)
        self.assertEqual(
            (state.state, state.drain_id, state.last_event_id),
            ("draining", REOPENED_HOLD_ID, start["event_id"]),
        )
        self.assertEqual(self._activation_count(), 2)

    def test_hold_reopen_cannot_reset_a_matrix_network_delta(self) -> None:
        self._begin()
        self._seal()
        self._insert_matrix_run(
            scheduled_for="matrix-after-seal",
            status="succeeded",
            network_requests=1,
        )

        with self.assertRaises(ProfileControlError) as caught:
            self._command("reopen_current_activation_hold")(
                db_path=self.db,
                drain_id=HOLD_ID,
                new_drain_id=REOPENED_HOLD_ID,
                build_receipt_sha256=ADVANCED_BUILD,
                runtime_root_receipt_sha256=ADVANCED_RUNTIME,
                actor=ACTOR,
                reason="qualification expired after Matrix drift",
                not_before_business_day="2026-09-09",
                now=REOPEN_AT,
            )

        self.assertEqual(caught.exception.code, "current_hold_matrix_delta")
        self.assertEqual(self._state(REOPEN_AT).state, "sealed")
        self.assertEqual(
            [row["event_type"] for row in self._hold_event_rows(HOLD_ID)],
            ["start", "sealed"],
        )


if __name__ == "__main__":
    unittest.main()
