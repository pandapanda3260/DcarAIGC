from __future__ import annotations

import hashlib
import json
import tempfile
import unittest
from pathlib import Path
from types import SimpleNamespace
from typing import Any
from unittest.mock import MagicMock, patch

from v8 import api as api_module
from v8 import profile_control, runtime_receipts
from v8.paid_drain import DrainState, issue_activation_permit_in_transaction
from v8.profile_activations import MATRIX_PROFILE, TIKHUB_PROFILE, append_activation
from v8.profile_control import begin_cross_profile_switch, complete_cross_profile_switch
from v8.provider_budget import PRICES_MICROUSD, record_circuit
from v8.runtime_database import DatabaseAccessMode
from v8.storage import connect, initialize_database, transaction


INITIAL_AT = "2026-09-01T00:00:00.000000Z"
IMMEDIATE_SWITCH_AT = "2026-09-05T03:03:46Z"
LEGACY_RELEASE_AT = "2026-09-05T03:03:55Z"
READINESS_AT = "2026-09-06T04:00:00Z"
HOLD_SEAL_AT = "2026-09-06T06:00:00Z"
TRANSITION_READINESS_AT = "2026-09-06T03:00:00Z"

LEGACY_BUILD = "6" * 64
LEGACY_RUNTIME = "7" * 64
HOLD_BUILD = "a" * 64
HOLD_RUNTIME = "b" * 64
CONFIG_RECEIPT = "c" * 64
QUALIFICATION_RECEIPT = "d" * 64
CAPACITY_RECEIPT = "e" * 64
PRICE_RECEIPT = "f" * 64
BUDGET_RECEIPT = "0" * 64
HOLD_ID = "current-activation-readiness-fixture"


class CurrentActivationReadinessTest(unittest.TestCase):
    """Boundary tests for current, rather than merely historical, readiness."""

    def setUp(self) -> None:
        temporary = tempfile.TemporaryDirectory(
            prefix="dcar-current-activation-readiness-"
        )
        self.addCleanup(temporary.cleanup)
        self.root = Path(temporary.name).resolve()
        self.db = self.root / "readiness.sqlite3"

        with connect(self.db) as connection:
            initialize_database(connection)
            matrix = self._snapshot(connection, "matrix", "matrix-roster")
            system = self._snapshot(connection, "system", "system-roster")
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
                    drain_id="initial-activation-permit",
                    source_activation_id=int(initial["activation_id"]),
                    business_day="2026-09-01",
                    planned_effective_at=INITIAL_AT,
                    build_receipt_sha256=LEGACY_BUILD,
                    runtime_root_receipt_sha256=LEGACY_RUNTIME,
                    now=INITIAL_AT,
                )

        switched = begin_cross_profile_switch(
            db_path=self.db,
            drain_id="historical-immediate-mode-b",
            target_profile_id=TIKHUB_PROFILE,
            roster_snapshot_id=int(system["id"]),
            build_receipt_sha256=LEGACY_BUILD,
            runtime_root_receipt_sha256=LEGACY_RUNTIME,
            actor="fixture",
            reason="historical immediate Mode-B switch",
            now=IMMEDIATE_SWITCH_AT,
            effective_now=True,
        )
        complete_cross_profile_switch(
            db_path=self.db,
            drain_id="historical-immediate-mode-b",
            now=LEGACY_RELEASE_AT,
            mirror_root=self.root / "legacy-release-mirrors",
        )
        self.activation_id = int(switched["activation"]["activation_id"])
        self.assertEqual(self.activation_id, 2)

    def _snapshot(
        self, connection: Any, family: str, scope_key: str
    ) -> dict[str, object]:
        source = json.dumps({"family": family, "scope_key": scope_key}).encode()
        source_sha256 = hashlib.sha256(source).hexdigest()
        members_sha256 = hashlib.sha256(scope_key.encode()).hexdigest()
        source_path = self.root / f"{scope_key}.json"
        source_path.write_bytes(source)
        cursor = connection.execute(
            """INSERT INTO account_roster_snapshots(
                   source_family,source_type,scope_key,scope_json,source_instance_id,
                   source_captured_at,accepted_at,declared_count,member_count,
                   members_sha256,source_sha256,source_path,contract_version,metadata_json)
               VALUES (?,?,?,'{}',?,'2026-09-01T00:00:00Z',
                       '2026-09-01T00:00:00Z',0,0,?,?,?,'account-roster-v2','{}')""",
            (
                family,
                "system_managed" if family == "system" else "manual_export",
                scope_key,
                scope_key,
                members_sha256,
                source_sha256,
                str(source_path),
            ),
        )
        return {
            "id": int(cursor.lastrowid or 0),
            "members_sha256": members_sha256,
        }

    def _readiness(self, at: str = READINESS_AT) -> dict[str, Any]:
        with connect(self.db) as connection:
            return runtime_receipts.current_activation_readiness(connection, at=at)

    def _insert_day_receipt_row(
        self, *, business_day: str, scope: dict[str, Any]
    ) -> None:
        recorded_at = f"{business_day}T18:00:00Z"
        with connect(self.db) as connection, transaction(connection):
            active = connection.execute(
                "SELECT profile_id,roster_snapshot_id,roster_members_sha256 "
                "FROM acquisition_profile_activations WHERE id=?",
                (self.activation_id,),
            ).fetchone()
            assert active is not None
            run = connection.execute(
                "INSERT INTO scheduler_runs(job_id,scheduled_for,status,started_at,"
                "completed_at,details_json) VALUES "
                "('profile_day_coverage',?,'succeeded',?,?,'{}')",
                (f"fixture:{business_day}", recorded_at, recorded_at),
            )
            run_id = int(run.lastrowid or 0)
            attempt = connection.execute(
                "INSERT INTO scheduler_run_attempts(scheduler_run_id,attempt_number,"
                "invocation_source,status,started_at,completed_at,details_json) "
                "VALUES (?,1,'scheduled','succeeded',?,?,'{}')",
                (run_id, recorded_at, recorded_at),
            )
            connection.execute(
                "INSERT INTO profile_day_coverage_receipts("
                "source_bridge_run_id,source_bridge_attempt_id,activation_id,profile_id,"
                "roster_snapshot_id,roster_members_sha256,business_day,sequence,sealed_at,"
                "status,complete,partial_publishable,scope_json,summary_json,evidence_json,"
                "contract_version,receipt_sha256,recorded_at) "
                "VALUES (?,?,?,?,?,?,?,1,?,'complete',1,0,?,'{}','{}',?,?,?)",
                (
                    run_id,
                    int(attempt.lastrowid or 0),
                    self.activation_id,
                    str(active["profile_id"]),
                    int(active["roster_snapshot_id"]),
                    str(active["roster_members_sha256"]),
                    business_day,
                    recorded_at,
                    json.dumps(scope, sort_keys=True),
                    runtime_receipts.PROFILE_DAY_SCOPE_CONTRACT,
                    hashlib.sha256(business_day.encode()).hexdigest(),
                    recorded_at,
                ),
            )

    def _begin_hold(self) -> None:
        profile_control.begin_current_activation_hold(
            db_path=self.db,
            drain_id=HOLD_ID,
            build_receipt_sha256=HOLD_BUILD,
            runtime_root_receipt_sha256=HOLD_RUNTIME,
            actor="fixture-release-owner",
            reason="readiness contract fixture",
            not_before_business_day="2026-09-08",
            now=READINESS_AT,
        )

    def _seal_hold(self) -> None:
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
                expires_at="2026-09-10T00:00:00Z",
                evidence=evidence,
                actor="fixture-release-owner",
                now="2026-09-06T05:00:00Z",
            )
        profile_control.seal_current_activation_hold(
            db_path=self.db,
            drain_id=HOLD_ID,
            final_build_receipt_sha256=HOLD_BUILD,
            runtime_root_receipt_sha256=HOLD_RUNTIME,
            config_receipt_sha256=CONFIG_RECEIPT,
            qualification_receipt_sha256=QUALIFICATION_RECEIPT,
            capacity_receipt_sha256=CAPACITY_RECEIPT,
            price_receipt_sha256=PRICE_RECEIPT,
            budget_receipt_sha256=BUDGET_RECEIPT,
            actor="fixture-release-owner",
            now=HOLD_SEAL_AT,
        )

    def _api_request(
        self,
        *,
        heartbeat_at: str | None,
        scheduler_requested: bool = True,
        runtime_access_mode: DatabaseAccessMode | None = None,
        writer_lock_held: bool = True,
    ) -> SimpleNamespace:
        config = api_module.ApiConfig(
            db_path=self.db,
            reports_root=self.root / "reports",
            legacy_db_path=self.root / "legacy.sqlite3",
            operator_freeze_lock=self.root / "operator-freeze.lock",
            writer_lock=self.root / "writer.lock",
            scheduler_enabled=False,
            startup_catchup_enabled=False,
            runtime_access_mode=runtime_access_mode,
            project_root=self.root,
        )
        state = SimpleNamespace(
            config=config,
            scheduler_requested=scheduler_requested,
            writer_lock_held=writer_lock_held,
            writer_heartbeat_at=heartbeat_at,
        )
        return SimpleNamespace(app=SimpleNamespace(state=state))

    @staticmethod
    def _json_response(value: Any) -> tuple[int, dict[str, Any]]:
        status_code = int(getattr(value, "status_code", 200))
        body = getattr(value, "body", None)
        if body is None:
            return status_code, value
        return status_code, json.loads(body)

    def test_old_activation_receipt_cannot_make_current_activation_ready(self) -> None:
        old_receipt = {
            "run_id": 41,
            "scope": {
                "activation_id": 1,
                "profile_id": MATRIX_PROFILE,
                "business_day": "2026-09-04",
            },
        }
        closed = DrainState(
            "closed",
            reason="current_activation_hold_missing",
            activation_id=self.activation_id,
        )
        with (
            patch.object(api_module, "now_utc", return_value=READINESS_AT),
            patch(
                "v8.runtime_receipts.latest_runtime_coverage",
                return_value={"receipt": old_receipt},
            ),
            patch("v8.paid_drain.dispatch_state", return_value=closed),
        ):
            status, payload = self._json_response(
                api_module.v8_readyz(
                    self._api_request(heartbeat_at="2026-09-06T03:59:30Z")
                )
            )

        self.assertEqual(status, 503)
        self.assertEqual(payload["reason"], "current_activation_hold_missing")
        self.assertFalse(payload["data_readiness"])
        self.assertIsNone(payload["profile_day_receipt"])
        self.assertEqual(payload["closed_business_day_receipt"], old_receipt)

    def test_immediate_activation_without_current_hold_has_stable_reason(self) -> None:
        readiness = self._readiness()

        self.assertEqual(readiness["activation"]["activation_id"], self.activation_id)
        self.assertFalse(readiness["control_readiness"])
        self.assertFalse(readiness["data_readiness"])
        self.assertEqual(readiness["paid_dispatch_state"], "closed")
        self.assertEqual(readiness["reason"], "current_activation_hold_missing")

    def test_hold_start_and_sealed_are_control_ready_but_not_data_ready(self) -> None:
        self._begin_hold()
        started = self._readiness()
        self.assertTrue(started["control_readiness"])
        self.assertFalse(started["data_readiness"])
        self.assertEqual(started["paid_dispatch_state"], "draining")
        self.assertEqual(started["reason"], "current_activation_coverage_incomplete")

        self._seal_hold()
        sealed = self._readiness(HOLD_SEAL_AT)
        self.assertTrue(sealed["control_readiness"])
        self.assertFalse(sealed["data_readiness"])
        self.assertEqual(sealed["paid_dispatch_state"], "sealed")
        self.assertEqual(sealed["reason"], "current_activation_coverage_incomplete")

    def test_transition_day_rejects_even_a_complete_pseudo_receipt(self) -> None:
        pseudo_complete_row = {
            "id": 91,
            "activation_id": self.activation_id,
            "business_day": "2026-09-05",
            "complete": 1,
        }
        pseudo_complete_receipt = {
            "scope": {
                "activation_id": self.activation_id,
                "profile_id": TIKHUB_PROFILE,
                "business_day": "2026-09-05",
            },
            "summary": {"coverage": {"complete": True}},
        }
        open_state = DrainState(
            "open",
            activation_id=self.activation_id,
            permit_event_id=77,
        )
        with (
            patch("v8.paid_drain.dispatch_state", return_value=open_state),
            patch.object(
                runtime_receipts, "_current_hold_control_valid", return_value=True
            ),
            patch.object(
                runtime_receipts,
                "_latest_complete_current_day_row",
                return_value=pseudo_complete_row,
            ) as receipt_lookup,
            patch.object(
                runtime_receipts,
                "_validate_native_day_row",
                return_value=pseudo_complete_receipt,
            ) as receipt_validator,
        ):
            readiness = self._readiness(TRANSITION_READINESS_AT)

        self.assertTrue(readiness["control_readiness"])
        self.assertFalse(readiness["data_readiness"])
        self.assertEqual(readiness["reason"], "activation_transition_day")
        self.assertIsNone(readiness["receipt"])
        receipt_lookup.assert_called_once()
        receipt_validator.assert_not_called()

    def test_readiness_keeps_release_day_proof_after_later_complete_day(self) -> None:
        self._begin_hold()
        self._seal_hold()
        released_at = "2026-09-07T16:04:59Z"
        released = profile_control.release_current_activation_hold(
            db_path=self.db,
            drain_id=HOLD_ID,
            actor="fixture-release-owner",
            now=released_at,
        )["release"]
        release_scope = {
            "activation_id": self.activation_id,
            "profile_id": TIKHUB_PROFILE,
            "roster_snapshot_id": 2,
            "roster_snapshot_hash": None,
            "business_day": "2026-09-08",
            "drain_id": HOLD_ID,
            "release_event_id": released["event_id"],
            "release_event_hash": released["event_hash"],
            "released_at": released_at,
            "control_contract_version": "current_activation_hold_v1",
        }
        with connect(self.db) as connection:
            release_scope["roster_snapshot_hash"] = connection.execute(
                "SELECT roster_members_sha256 FROM acquisition_profile_activations "
                "WHERE id=?",
                (self.activation_id,),
            ).fetchone()[0]
        self._insert_day_receipt_row(
            business_day="2026-09-08", scope=release_scope
        )
        self._insert_day_receipt_row(
            business_day="2026-09-09",
            scope={
                "activation_id": self.activation_id,
                "profile_id": TIKHUB_PROFILE,
                "roster_snapshot_id": 2,
                "roster_snapshot_hash": release_scope["roster_snapshot_hash"],
                "business_day": "2026-09-09",
            },
        )

        def validate(_connection: Any, row: Any) -> dict[str, Any]:
            day = str(row["business_day"])
            scope = release_scope if day == "2026-09-08" else {
                **release_scope,
                "business_day": day,
                "drain_id": None,
                "release_event_id": None,
                "release_event_hash": None,
                "released_at": None,
                "control_contract_version": None,
            }
            return {
                "run_id": int(row["source_bridge_run_id"]),
                "attempt_id": int(row["source_bridge_attempt_id"]),
                "scope": scope,
                "summary": {
                    "sequence": 1,
                    "sealed_at": row["sealed_at"],
                    "coverage": {"complete": True},
                },
                "self_sha256": row["receipt_sha256"],
            }

        with patch.object(
            runtime_receipts, "_validate_native_day_row", side_effect=validate
        ):
            readiness = self._readiness("2026-09-09T19:00:00Z")
            with connect(self.db) as connection, transaction(connection):
                record_circuit(
                    connection,
                    reason="provider_balance_blocked",
                    usage_id=None,
                    at="2026-09-09T19:00:01Z",
                )
            provider_blocked = self._readiness("2026-09-09T19:00:02Z")

        self.assertTrue(readiness["control_readiness"])
        self.assertTrue(readiness["data_readiness"])
        self.assertIsNone(readiness["reason"])
        self.assertEqual(
            readiness["receipt"]["scope"]["business_day"], "2026-09-08"
        )
        self.assertFalse(provider_blocked["data_readiness"])
        self.assertEqual(provider_blocked["reason"], "provider_blocked")

    def test_revoked_newest_revision_never_falls_back_to_older_revision(self) -> None:
        connection = MagicMock()
        connection.execute.return_value.fetchone.return_value = (1,)
        active = {
            "activation_id": self.activation_id,
            "profile_id": TIKHUB_PROFILE,
            "activation_sha256": "1" * 64,
            "source_family": "system",
            "roster_snapshot_id": 2,
            "roster_snapshot_hash": "2" * 64,
        }
        newest = {"id": 102, "sequence": 2}
        older = {"id": 101, "sequence": 1}
        with (
            patch.object(runtime_receipts, "_has_native_receipts", return_value=True),
            patch.object(
                runtime_receipts, "_active_schema19_profile", return_value=active
            ),
            patch.object(
                runtime_receipts, "_native_day_row", side_effect=[newest, older]
            ) as receipt_lookup,
            patch.object(runtime_receipts, "_validate_native_day_row") as validator,
        ):
            selected = runtime_receipts.read_profile_day_coverage_receipt(
                connection,
                at=READINESS_AT,
                business_day="2026-09-05",
            )

        self.assertIsNone(selected)
        receipt_lookup.assert_called_once()
        validator.assert_not_called()

    def test_revoked_newest_scan_revision_never_falls_back_to_older_revision(
        self,
    ) -> None:
        connection = MagicMock()
        newest = {"id": 102, "scan_run_id": 71, "sequence": 2}
        older = {"id": 101, "scan_run_id": 71, "sequence": 1}
        receipt_cursor = MagicMock()
        receipt_cursor.fetchone.side_effect = [newest, older]
        revocation_cursor = MagicMock()
        revocation_cursor.fetchone.return_value = (1,)
        connection.execute.side_effect = [receipt_cursor, revocation_cursor]

        with patch.object(runtime_receipts, "_has_native_receipts", return_value=True):
            selected = runtime_receipts._native_scan_row(
                connection,
                scan_run_id=71,
            )

        self.assertIsNone(selected)
        self.assertEqual(receipt_cursor.fetchone.call_count, 1)
        self.assertEqual(connection.execute.call_count, 2)
        receipt_sql = connection.execute.call_args_list[0].args[0]
        self.assertIn("ORDER BY r.id DESC LIMIT 1", receipt_sql)
        self.assertNotIn("runtime_receipt_revocations", receipt_sql)

    def test_paused_scheduler_writer_still_requires_receipt_and_heartbeat(
        self,
    ) -> None:
        current_receipt = {
            "scope": {
                "activation_id": self.activation_id,
                "profile_id": TIKHUB_PROFILE,
                "business_day": "2026-09-05",
            }
        }
        open_state = DrainState(
            "open",
            activation_id=self.activation_id,
            permit_event_id=77,
        )
        missing_current = {
            "control_readiness": False,
            "data_readiness": False,
            "reason": "current_activation_coverage_incomplete",
            "receipt": None,
        }
        # Deliberately omit every other writer signal so WRITER access mode is
        # the only fact that can activate the readiness requirements.
        with (
            patch.object(api_module, "now_utc", return_value=READINESS_AT),
            patch(
                "v8.runtime_receipts.current_activation_readiness",
                return_value=missing_current,
            ),
            patch(
                "v8.runtime_receipts.latest_runtime_coverage",
                return_value={"receipt": None},
            ),
            patch("v8.paid_drain.dispatch_state", return_value=open_state),
        ):
            request = self._api_request(
                heartbeat_at=None,
                scheduler_requested=False,
                runtime_access_mode=DatabaseAccessMode.WRITER,
                writer_lock_held=False,
            )
            status, payload = self._json_response(api_module.v8_readyz(request))

        self.assertEqual(status, 503)
        self.assertEqual(payload["reason"], "writer_lock_missing")
        self.assertFalse(payload["conditions"]["control_readiness"])
        self.assertFalse(payload["conditions"]["writer_heartbeat"])
        self.assertFalse(payload["conditions"]["profile_day_receipt"])

        cases = (
            (
                missing_current,
                "2026-09-06T03:59:30Z",
                "current_activation_coverage_incomplete",
                True,
                False,
            ),
            (
                {
                    "control_readiness": True,
                    "data_readiness": True,
                    "reason": None,
                    "receipt": current_receipt,
                },
                None,
                "writer_heartbeat_stale",
                False,
                True,
            ),
        )
        for current, heartbeat_at, reason, heartbeat_ok, receipt_ok in cases:
            with (
                self.subTest(reason=reason),
                patch.object(api_module, "now_utc", return_value=READINESS_AT),
                patch(
                    "v8.runtime_receipts.current_activation_readiness",
                    return_value=current,
                ),
                patch(
                    "v8.runtime_receipts.latest_runtime_coverage",
                    return_value={"receipt": None},
                ),
                patch("v8.paid_drain.dispatch_state", return_value=open_state),
            ):
                request = self._api_request(
                    heartbeat_at=heartbeat_at,
                    scheduler_requested=False,
                    runtime_access_mode=DatabaseAccessMode.WRITER,
                )
                self.assertFalse(request.app.state.config.scheduler_enabled)
                self.assertFalse(request.app.state.scheduler_requested)
                status, payload = self._json_response(api_module.v8_readyz(request))

            self.assertEqual(status, 503)
            self.assertEqual(payload["reason"], reason)
            self.assertEqual(payload["conditions"]["writer_heartbeat"], heartbeat_ok)
            self.assertEqual(payload["conditions"]["profile_day_receipt"], receipt_ok)

    def test_api_rejects_missing_and_expired_writer_heartbeat(self) -> None:
        current_receipt = {
            "scope": {
                "activation_id": self.activation_id,
                "profile_id": TIKHUB_PROFILE,
                "business_day": "2026-09-05",
            }
        }
        current_ready = {
            "control_readiness": True,
            "data_readiness": True,
            "reason": None,
            "receipt": current_receipt,
        }
        open_state = DrainState(
            "open",
            activation_id=self.activation_id,
            permit_event_id=77,
        )
        for heartbeat_at in (None, "2026-09-06T03:56:59Z"):
            with (
                self.subTest(heartbeat_at=heartbeat_at),
                patch.object(api_module, "now_utc", return_value=READINESS_AT),
                patch(
                    "v8.runtime_receipts.current_activation_readiness",
                    return_value=current_ready,
                ),
                patch(
                    "v8.runtime_receipts.latest_runtime_coverage",
                    return_value={"receipt": None},
                ),
                patch("v8.paid_drain.dispatch_state", return_value=open_state),
            ):
                status, payload = self._json_response(
                    api_module.v8_readyz(
                        self._api_request(heartbeat_at=heartbeat_at)
                    )
                )

                self.assertEqual(status, 503)
                self.assertEqual(payload["reason"], "writer_heartbeat_stale")
                self.assertFalse(payload["conditions"]["writer_heartbeat"])
                self.assertTrue(payload["conditions"]["control_readiness"])
                self.assertTrue(payload["conditions"]["profile_day_receipt"])


if __name__ == "__main__":
    unittest.main()
