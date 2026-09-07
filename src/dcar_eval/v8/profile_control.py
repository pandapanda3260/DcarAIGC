"""Activation-bound acquisition profile scheduling and cold cutover control."""

from __future__ import annotations

import argparse
import hashlib
import json
import os
import re
import sqlite3
import stat
from contextlib import nullcontext
from datetime import datetime, time, timedelta, timezone
from pathlib import Path
from typing import Any, Callable, Mapping, Sequence
from zoneinfo import ZoneInfo

from apscheduler.schedulers.base import BaseScheduler  # type: ignore[import-untyped]

from .account_roster import snapshot_by_id
from .capture import recover_stale_fetch_slots
from .paid_drain import (
    CURRENT_ACTIVATION_HOLD_CONTRACT,
    PaidDrainError,
    dispatch_state,
    issue_activation_permit_in_transaction,
    release_profile_drain_in_transaction,
    seal_profile_drain_in_transaction,
    start_profile_drain_in_transaction,
    verify_profile_drain_sealable,
    write_audit_mirror,
)
from .profile_activations import (
    ELIGIBILITY_CONTRACT,
    INTEGRATED_PROFILE,
    TIKHUB_PROFILE,
    PROFILE_FAMILIES,
    activation_at,
    activation_by_id,
    activation_eligibility,
    append_activation,
    cancel_activation,
    replace_scheduled_activation,
)
from .runtime_database import (
    DatabaseAccessMode,
    ResolvedDatabaseAccess,
    RuntimeDatabaseError,
    acquire_writer_lock,
    resolve_installed_database_access,
    resolve_isolated_candidate,
    require_current_process_writer_lock,
)
from .scheduler import recover_interrupted_scheduler_runs
from .source_routing import parse_time
from .storage import (
    DEFAULT_DB,
    PROJECT_ROOT,
    connect,
    is_formal_database_path,
    live_wal_read_only_connections,
    now_utc,
    transaction,
)

BEIJING = ZoneInfo("Asia/Shanghai")
BEGIN_EARLIEST = time(20, 0)
CONTROL_CONTRACT = "acquisition-profile-control-v1"
IMMEDIATE_CONTROL_CONTRACT = "acquisition-profile-control-v2"
CURRENT_HOLD_CONTROL_JOB = "current_activation_hold_control"
CURRENT_HOLD_CONTROL_RECEIPT = "current-activation-hold-control-v1"
CURRENT_HOLD_COMMAND_JOB = "current_activation_hold_command"
CURRENT_HOLD_COMMAND_CONTRACT = "current-activation-hold-command-v1"
CURRENT_HOLD_COMMAND_SCOPE_CONTRACT = "current-hold-command-scope-v1"
CURRENT_HOLD_PREREQUISITE_CONTRACT = "current-hold-prerequisite-v1"
CURRENT_HOLD_PREREQUISITE_CONTRACTS = {
    "build": "sealed-build-receipt-v1",
    "runtime": "runtime-root-binding-v1",
    "config": "current-hold-config-receipt-v1",
    "qualification": "provider-operation-qualification-v1",
    "capacity": "storage_capacity_receipt_v1",
    "price": "provider-price-receipt-v1",
    "budget": "provider-budget-receipt-v1",
}
FULL_DAY_RELEASE_WINDOW_SECONDS = 5 * 60
_MATRIX_PROVIDER = "newrank_matrix"
_MATRIX_JOBS = ("matrix_works_scan", "matrix_account_metrics")
_SHA256 = re.compile(r"[0-9a-f]{64}")
_COMMAND_ID = re.compile(r"[A-Za-z0-9][A-Za-z0-9_.:-]{0,127}")
_CURRENT_HOLD_COMMANDS = frozenset(
    {"hold_begin", "hold_build_advance", "hold_seal", "hold_release", "hold_reopen", "transport_primary", "transport_control", "forward_only_release", "capture_release"}
)


class ProfileControlError(RuntimeError):
    def __init__(self, code: str, message: str) -> None:
        super().__init__(message)
        self.code = code


def _utc(value: str | None) -> str:
    try:
        parsed = parse_time(value or now_utc()).astimezone(timezone.utc)
    except (TypeError, ValueError) as exc:
        raise ProfileControlError("profile_control_time_invalid", "A timezone-aware time is required") from exc
    return parsed.isoformat(timespec="microseconds").replace("+00:00", "Z")


def _next_midnight(value: str) -> str:
    local = parse_time(value).astimezone(BEIJING)
    midnight = datetime.combine(local.date() + timedelta(days=1), time.min, BEIJING)
    return midnight.astimezone(timezone.utc).isoformat(timespec="microseconds").replace(
        "+00:00", "Z"
    )


def _sha(value: str, *, label: str) -> str:
    if len(value) != 64 or any(character not in "0123456789abcdef" for character in value):
        raise ProfileControlError("profile_control_receipt_invalid", f"{label} must be lowercase SHA-256")
    return value


def _active(connection: sqlite3.Connection, at: str) -> dict[str, Any]:
    try:
        value = activation_at(connection, at)
    except Exception as exc:
        raise ProfileControlError(
            "profile_control_activation_invalid", "The active activation chain is invalid"
        ) from exc
    if value is None:
        raise ProfileControlError(
            "profile_control_activation_missing", "No acquisition profile is active"
        )
    return value


def _target_snapshot(
    connection: sqlite3.Connection, *, profile_id: str, roster_snapshot_id: int
) -> dict[str, Any]:
    if profile_id not in PROFILE_FAMILIES:
        raise ProfileControlError("profile_control_profile_invalid", "Unknown acquisition profile")
    if profile_id == INTEGRATED_PROFILE and int(connection.execute("PRAGMA user_version").fetchone()[0]) != 20:
        raise ProfileControlError("profile_control_schema_invalid", "Integrated acquisition requires schema 20")
    try:
        snapshot = snapshot_by_id(connection, roster_snapshot_id)
    except Exception as exc:
        raise ProfileControlError(
            "profile_control_roster_missing", "Accepted target roster snapshot does not exist"
        ) from exc
    if snapshot["source_family"] != PROFILE_FAMILIES[profile_id]:
        raise ProfileControlError(
            "profile_control_roster_mismatch", "Target profile and accepted roster family differ"
        )
    return snapshot


def _require_schema19(connection: sqlite3.Connection) -> None:
    if int(connection.execute("PRAGMA user_version").fetchone()[0]) not in {19, 20}:
        raise ProfileControlError(
            "profile_control_schema_invalid", "Profile control requires schema 19 or 20"
        )


def _canonical(value: Any) -> str:
    try:
        return json.dumps(
            value,
            ensure_ascii=False,
            sort_keys=True,
            separators=(",", ":"),
            allow_nan=False,
        )
    except (TypeError, ValueError) as exc:
        raise ProfileControlError(
            "current_hold_payload_invalid", "Current-hold payload is not canonical JSON"
        ) from exc


def _digest(value: Any) -> str:
    return hashlib.sha256(_canonical(value).encode("utf-8")).hexdigest()


def _business_day(value: str, *, label: str) -> str:
    try:
        parsed = datetime.strptime(value, "%Y-%m-%d").date()
    except (TypeError, ValueError) as exc:
        raise ProfileControlError(
            "current_hold_business_day_invalid", f"{label} must be YYYY-MM-DD"
        ) from exc
    if parsed.isoformat() != value:
        raise ProfileControlError(
            "current_hold_business_day_invalid", f"{label} must be YYYY-MM-DD"
        )
    return value


def _day_midnight_utc(value: str) -> str:
    local = datetime.combine(
        datetime.strptime(value, "%Y-%m-%d").date(), time.min, BEIJING
    )
    return local.astimezone(timezone.utc).isoformat(timespec="seconds").replace(
        "+00:00", "Z"
    )


def _matrix_high_watermarks(connection: sqlite3.Connection) -> dict[str, int]:
    network_starts = 0
    for row in connection.execute(
        "SELECT id,status,details_json FROM scheduler_runs WHERE job_id IN (?,?)",
        _MATRIX_JOBS,
    ):
        attempt = connection.execute(
            "SELECT status,details_json FROM scheduler_run_attempts "
            "WHERE scheduler_run_id=? ORDER BY attempt_number DESC,id DESC LIMIT 1",
            (int(row["id"]),),
        ).fetchone()
        if attempt is None:
            raise ProfileControlError(
                "current_hold_matrix_evidence_invalid",
                "Matrix scheduler run has no immutable attempt evidence",
            )
        if row["status"] != "running" and (
            attempt["status"] != row["status"]
            or str(attempt["details_json"]) != str(row["details_json"])
        ):
            raise ProfileControlError(
                "current_hold_matrix_evidence_invalid",
                "Matrix scheduler run and terminal attempt evidence differ",
            )
        try:
            details = json.loads(str(row["details_json"] or "{}"))
        except (TypeError, ValueError) as exc:
            raise ProfileControlError(
                "current_hold_matrix_evidence_invalid",
                "Matrix scheduler evidence is invalid",
            ) from exc
        checkpoint = details.get("checkpoint") if isinstance(details, dict) else None
        count = checkpoint.get("network_requests", 0) if isinstance(checkpoint, dict) else 0
        if type(count) is not int or count < 0:
            raise ProfileControlError(
                "current_hold_matrix_evidence_invalid",
                "Matrix network request evidence is invalid",
            )
        network_starts += count

    def scalar(query: str) -> int:
        return int(connection.execute(query).fetchone()[0])

    return {
        "network_starts": network_starts,
        "dispatch_event_count": scalar(
            "SELECT COUNT(*) FROM paid_provider_dispatch_events "
            f"WHERE lower(provider)='{_MATRIX_PROVIDER}'"
        ),
        "dispatch_event_high_watermark": scalar(
            "SELECT COALESCE(MAX(id),0) FROM paid_provider_dispatch_events "
            f"WHERE lower(provider)='{_MATRIX_PROVIDER}'"
        ),
        "dispatch_send_marked_count": scalar(
            "SELECT COUNT(*) FROM paid_provider_dispatch_events "
            f"WHERE lower(provider)='{_MATRIX_PROVIDER}' "
            "AND event_type='send_marked'"
        ),
        "raw_count": scalar(
            "SELECT COUNT(*) FROM provider_raw_responses "
            f"WHERE lower(provider)='{_MATRIX_PROVIDER}'"
        ),
        "raw_high_watermark": scalar(
            "SELECT COALESCE(MAX(id),0) FROM provider_raw_responses "
            f"WHERE lower(provider)='{_MATRIX_PROVIDER}'"
        ),
        "content_observation_count": scalar(
            "SELECT COUNT(*) FROM content_metric_observations "
            f"WHERE lower(source)='{_MATRIX_PROVIDER}'"
        ),
        "content_observation_high_watermark": scalar(
            "SELECT COALESCE(MAX(id),0) FROM content_metric_observations "
            f"WHERE lower(source)='{_MATRIX_PROVIDER}'"
        ),
        "account_observation_count": scalar(
            "SELECT COUNT(*) FROM account_metric_observations "
            f"WHERE lower(source)='{_MATRIX_PROVIDER}'"
        ),
        "account_observation_high_watermark": scalar(
            "SELECT COALESCE(MAX(id),0) FROM account_metric_observations "
            f"WHERE lower(source)='{_MATRIX_PROVIDER}'"
        ),
    }


def _matrix_running_run_ids(connection: sqlite3.Connection) -> list[int]:
    rows = connection.execute(
        "SELECT DISTINCT r.id FROM scheduler_runs r "
        "LEFT JOIN scheduler_run_attempts a ON a.scheduler_run_id=r.id "
        "WHERE r.job_id IN (?,?) AND (r.status='running' OR a.status='running') "
        "ORDER BY r.id",
        _MATRIX_JOBS,
    ).fetchall()
    return [int(row["id"]) for row in rows]


def _matrix_unresolved_dispatch_ids(connection: sqlite3.Connection) -> list[str]:
    rows = connection.execute(
        "SELECT e.dispatch_id FROM paid_provider_dispatch_events e "
        f"WHERE lower(e.provider)='{_MATRIX_PROVIDER}' "
        "AND e.id=(SELECT MAX(latest.id) FROM paid_provider_dispatch_events latest "
        "WHERE latest.dispatch_id=e.dispatch_id) "
        "AND e.event_type IN ('reserved','send_marked') ORDER BY e.id"
    ).fetchall()
    return [str(row["dispatch_id"]) for row in rows]


def _current_activation_predecessor_release(
    connection: sqlite3.Connection,
    *,
    activation_id: int,
    at: str,
) -> sqlite3.Row:
    row = connection.execute(
        "SELECT * FROM pipeline_paid_drain_events "
        "WHERE target_activation_id=? AND event_type='release' "
        "AND julianday(created_at)<=julianday(?) ORDER BY id DESC LIMIT 1",
        (activation_id, at),
    ).fetchone()
    if row is None:
        raise ProfileControlError(
            "current_hold_predecessor_invalid",
            "HOLD_BEGIN requires a terminal RELEASE for the current activation",
        )
    return row


def _current_hold_command_submission_scope(
    connection: sqlite3.Connection, *, at: str
) -> dict[str, Any]:
    try:
        active = _active(connection, at)
        predecessor = _current_activation_predecessor_release(
            connection,
            activation_id=int(active["activation_id"]),
            at=at,
        )
    except ProfileControlError as exc:
        return {
            "contract_version": CURRENT_HOLD_COMMAND_SCOPE_CONTRACT,
            "valid": False,
            "error_code": exc.code,
        }
    return {
        "contract_version": CURRENT_HOLD_COMMAND_SCOPE_CONTRACT,
        "valid": True,
        "activation_id": int(active["activation_id"]),
        "profile_id": str(active["profile_id"]),
        "roster_snapshot_id": int(active["roster_snapshot_id"]),
        "roster_snapshot_hash": str(active["roster_members_sha256"]),
        "predecessor_release_event_id": int(predecessor["id"]),
        "predecessor_release_event_hash": str(predecessor["event_hash"]),
    }


def _require_current_hold_command_submission_scope(
    *,
    expected: Mapping[str, Any] | None,
    active: Mapping[str, Any],
    predecessor: Mapping[str, Any],
) -> None:
    if expected is None:
        return
    current = {
        "contract_version": CURRENT_HOLD_COMMAND_SCOPE_CONTRACT,
        "valid": True,
        "activation_id": int(active["activation_id"]),
        "profile_id": str(active["profile_id"]),
        "roster_snapshot_id": int(active["roster_snapshot_id"]),
        "roster_snapshot_hash": str(active["roster_members_sha256"]),
        "predecessor_release_event_id": int(predecessor["id"]),
        "predecessor_release_event_hash": str(predecessor["event_hash"]),
    }
    if dict(expected) != current:
        raise ProfileControlError(
            "current_hold_submission_scope_drift",
            "HOLD_BEGIN activation, roster, or predecessor changed after submission",
        )


def _require_matrix_fence(
    connection: sqlite3.Connection, expected: Mapping[str, Any]
) -> dict[str, int]:
    current = _matrix_high_watermarks(connection)
    if dict(expected) != current:
        raise ProfileControlError(
            "current_hold_matrix_delta",
            "Matrix produced network/raw/observation evidence after HOLD_BEGIN",
        )
    return current


def _hold_event(
    connection: sqlite3.Connection, *, drain_id: str, event_type: str
) -> dict[str, Any] | None:
    row = connection.execute(
        "SELECT * FROM pipeline_paid_drain_events WHERE drain_id=? AND event_type=?",
        (drain_id, event_type),
    ).fetchone()
    if row is None:
        return None
    try:
        payload = json.loads(str(row["payload_json"]))
    except (TypeError, ValueError) as exc:
        raise ProfileControlError(
            "current_hold_chain_invalid", "Current-hold event payload is invalid"
        ) from exc
    if not isinstance(payload, dict):
        raise ProfileControlError(
            "current_hold_chain_invalid", "Current-hold event payload is invalid"
        )
    return {**dict(row), "event_id": int(row["id"]), "payload": payload}


def _require_current_hold_start(
    connection: sqlite3.Connection, *, drain_id: str, timestamp: str
) -> tuple[dict[str, Any], dict[str, Any], dict[str, Any]]:
    start = _hold_event(connection, drain_id=drain_id, event_type="start")
    if start is None:
        raise ProfileControlError(
            "current_hold_missing", "Current activation HOLD_BEGIN is missing"
        )
    control = start["payload"].get("control")
    active = _active(connection, timestamp)
    if (
        not isinstance(control, dict)
        or control.get("contract_version") != CURRENT_ACTIVATION_HOLD_CONTRACT
        or control.get("control_purpose") != "hold_begin"
        or start["payload"].get("switch_kind") != "same_profile"
        or start["payload"].get("nonblocking") is not False
        or int(start["target_activation_id"]) != int(active["activation_id"])
        or control.get("activation_id") != int(active["activation_id"])
        or control.get("roster_snapshot_id") != int(active["roster_snapshot_id"])
        or control.get("roster_snapshot_hash") != active["roster_members_sha256"]
        or control.get("drain_id") != drain_id
    ):
        raise ProfileControlError(
            "current_hold_chain_invalid", "Current-hold START binding is invalid"
        )
    return start, control, active


def _read_hold_control_receipt(
    connection: sqlite3.Connection, row: sqlite3.Row
) -> dict[str, Any]:
    attempts = connection.execute(
        "SELECT * FROM scheduler_run_attempts WHERE scheduler_run_id=? ORDER BY id",
        (row["id"],),
    ).fetchall()
    try:
        details = json.loads(str(row["details_json"]))
    except (TypeError, ValueError) as exc:
        raise ProfileControlError(
            "current_hold_control_invalid", "Current-hold control receipt is invalid"
        ) from exc
    frozen = dict(details) if isinstance(details, dict) else {}
    receipt_hash = frozen.pop("self_sha256", None)
    if (
        row["job_id"] != CURRENT_HOLD_CONTROL_JOB
        or row["status"] != "succeeded"
        or row["started_at"] != row["completed_at"]
        or len(attempts) != 1
        or attempts[0]["attempt_number"] != 1
        or attempts[0]["invocation_source"] != "operator_retry"
        or attempts[0]["status"] != "succeeded"
        or attempts[0]["started_at"] != row["started_at"]
        or attempts[0]["completed_at"] != row["completed_at"]
        or attempts[0]["details_json"] != row["details_json"]
        or frozen.get("contract_version") != CURRENT_HOLD_CONTROL_RECEIPT
        or frozen.get("run_id") != int(row["id"])
        or frozen.get("attempt_id") != int(attempts[0]["id"])
        or receipt_hash != _digest(frozen)
    ):
        raise ProfileControlError(
            "current_hold_control_invalid", "Current-hold control receipt is invalid"
        )
    return details


def _record_hold_control_receipt(
    connection: sqlite3.Connection,
    *,
    scheduled_for: str,
    payload: Mapping[str, Any],
    timestamp: str,
) -> dict[str, Any]:
    existing = connection.execute(
        "SELECT * FROM scheduler_runs WHERE job_id=? AND scheduled_for=?",
        (CURRENT_HOLD_CONTROL_JOB, scheduled_for),
    ).fetchone()
    if existing is not None:
        receipt = _read_hold_control_receipt(connection, existing)
        if receipt.get("payload") != dict(payload):
            raise ProfileControlError(
                "current_hold_idempotency_conflict",
                "Existing current-hold control receipt has a different binding",
            )
        return receipt
    cursor = connection.execute(
        "INSERT INTO scheduler_runs(job_id,scheduled_for,status,started_at,details_json) "
        "VALUES (?,?,'running',?,'{}')",
        (CURRENT_HOLD_CONTROL_JOB, scheduled_for, timestamp),
    )
    run_id = int(cursor.lastrowid or 0)
    attempt_cursor = connection.execute(
        "INSERT INTO scheduler_run_attempts(scheduler_run_id,attempt_number,"
        "invocation_source,status,started_at,details_json) "
        "VALUES (?,1,'operator_retry','running',?,'{}')",
        (run_id, timestamp),
    )
    attempt_id = int(attempt_cursor.lastrowid or 0)
    details: dict[str, Any] = {
        "contract_version": CURRENT_HOLD_CONTROL_RECEIPT,
        "run_id": run_id,
        "attempt_id": attempt_id,
        "scheduled_for": scheduled_for,
        "recorded_at": timestamp,
        "payload": json.loads(_canonical(dict(payload))),
    }
    details["self_sha256"] = _digest(details)
    encoded = _canonical(details)
    attempt = connection.execute(
        "UPDATE scheduler_run_attempts SET status='succeeded',completed_at=?,details_json=? "
        "WHERE id=? AND status='running'",
        (timestamp, encoded, attempt_id),
    )
    run = connection.execute(
        "UPDATE scheduler_runs SET status='succeeded',completed_at=?,details_json=? "
        "WHERE id=? AND status='running'",
        (timestamp, encoded, run_id),
    )
    if attempt.rowcount != 1 or run.rowcount != 1:
        raise ProfileControlError(
            "current_hold_control_failed", "Current-hold control receipt did not finalize"
        )
    return details


def _hold_generation(
    connection: sqlite3.Connection,
    *,
    drain_id: str,
    start_control: Mapping[str, Any],
) -> dict[str, Any]:
    current: dict[str, Any] = {
        "build_receipt_sha256": str(start_control["initial_build_receipt_sha256"]),
        "runtime_root_receipt_sha256": str(
            start_control["initial_runtime_root_receipt_sha256"]
        ),
        "config_receipt_sha256": None,
        "generation": 1,
    }
    rows = connection.execute(
        "SELECT * FROM scheduler_runs WHERE job_id=? AND status='succeeded' ORDER BY id",
        (CURRENT_HOLD_CONTROL_JOB,),
    ).fetchall()
    for row in rows:
        receipt = _read_hold_control_receipt(connection, row)
        payload = receipt.get("payload")
        if (
            not isinstance(payload, dict)
            or payload.get("action") != "hold_build_advance"
            or payload.get("drain_id") != drain_id
        ):
            continue
        if (
            payload.get("from_build_receipt_sha256")
            != current["build_receipt_sha256"]
            or payload.get("generation") != current["generation"] + 1
        ):
            raise ProfileControlError(
                "current_hold_build_chain_invalid",
                "Current-hold build-advance chain is disconnected",
            )
        current = {
            "build_receipt_sha256": payload["to_build_receipt_sha256"],
            "runtime_root_receipt_sha256": payload[
                "runtime_root_receipt_sha256"
            ],
            "config_receipt_sha256": payload["config_receipt_sha256"],
            "generation": payload["generation"],
        }
    return current


def _validate_prerequisite_evidence(
    *, kind: str, artifact_sha256: str, evidence: Mapping[str, Any]
) -> dict[str, Any]:
    frozen = json.loads(_canonical(dict(evidence)))
    if frozen.get("valid") is not True or frozen.get("readback") is not True:
        raise ProfileControlError(
            "current_hold_prerequisite_invalid",
            f"Current-hold {kind} prerequisite is not valid and read back",
        )
    if frozen.get("artifact_sha256") != artifact_sha256:
        raise ProfileControlError(
            "current_hold_prerequisite_invalid",
            f"Current-hold {kind} prerequisite does not bind its artifact",
        )
    if kind == "qualification":
        from .provider_budget import PRICES_MICROUSD

        operations = frozen.get("required_operations")
        if not isinstance(operations, list):
            raise ProfileControlError(
                "current_hold_prerequisite_invalid",
                "Operation qualification has no canonical operation set",
            )
        qualified: dict[str, bool] = {}
        for item in operations:
            if not isinstance(item, dict) or not isinstance(item.get("operation"), str):
                raise ProfileControlError(
                    "current_hold_prerequisite_invalid",
                    "Operation qualification member is invalid",
                )
            operation = str(item["operation"])
            if operation in qualified:
                raise ProfileControlError(
                    "current_hold_prerequisite_invalid",
                    "Operation qualification contains a duplicate member",
                )
            qualified[operation] = item.get("qualified") is True
        if qualified != {operation: True for operation in PRICES_MICROUSD}:
            raise ProfileControlError(
                "current_hold_prerequisite_invalid",
                "Operation qualification does not cover every priced operation",
            )
    elif kind == "capacity":
        runway = frozen.get("runway_days")
        if (
            frozen.get("archive_independent") is not True
            or frozen.get("archive_recovery_tested") is not True
            or not isinstance(runway, (int, float))
            or isinstance(runway, bool)
            or float(runway) < 90.0
        ):
            raise ProfileControlError(
                "current_hold_prerequisite_invalid",
                "Storage capacity prerequisite does not prove an independent 90-day runway",
            )
    elif kind == "price":
        from .provider_budget import PRICES_MICROUSD

        prices = frozen.get("prices_microusd")
        if prices != PRICES_MICROUSD:
            raise ProfileControlError(
                "current_hold_prerequisite_invalid",
                "Price prerequisite differs from the complete priced operation table",
            )
    elif kind == "budget":
        caps = frozen.get("caps_microusd")
        if caps != {
            "discovery": 30_000_000,
            "metrics": 15_000_000,
            "automatic_repair": 0,
            "automatic_total": 50_000_000,
        }:
            raise ProfileControlError(
                "current_hold_prerequisite_invalid",
                "Budget prerequisite differs from the fixed 30/15/0/50 limits",
            )
        forecast = frozen.get("forecast_microusd")
        if (
            not isinstance(forecast, int)
            or isinstance(forecast, bool)
            or forecast < 0
            or forecast > 50_000_000
        ):
            raise ProfileControlError(
                "current_hold_prerequisite_invalid",
                "Budget prerequisite forecast exceeds the automatic total limit",
            )
    return frozen


def record_current_activation_hold_prerequisite(
    *,
    db_path: Path,
    drain_id: str,
    kind: str,
    artifact_sha256: str,
    receipt_contract_version: str,
    expires_at: str,
    evidence: Mapping[str, Any],
    actor: str,
    generation: int | None = None,
    now: str | None = None,
    mirror_root: Path | None = None,
) -> dict[str, Any]:
    """Register a verified prerequisite produced by its owning subsystem.

    This is intentionally not exposed by the HTTP control API.  HOLD_SEAL only
    consumes these immutable, activation-scoped receipts and therefore cannot
    be authorized by operator-supplied SHA-looking strings alone.
    """

    if kind not in CURRENT_HOLD_PREREQUISITE_CONTRACTS:
        raise ProfileControlError(
            "current_hold_prerequisite_invalid",
            "Current-hold prerequisite kind is unsupported",
        )
    expected_contract = CURRENT_HOLD_PREREQUISITE_CONTRACTS[kind]
    if receipt_contract_version != expected_contract:
        raise ProfileControlError(
            "current_hold_prerequisite_invalid",
            f"Current-hold {kind} prerequisite contract differs",
        )
    if is_formal_database_path(db_path) and mirror_root is None:
        raise ProfileControlError(
            "current_hold_mirror_required",
            "Formal prerequisite registration requires a private mirror root",
        )
    expiry = _utc(expires_at)
    artifact = _sha(artifact_sha256, label=f"{kind} prerequisite")
    verified_evidence = _validate_prerequisite_evidence(
        kind=kind, artifact_sha256=artifact, evidence=evidence
    )
    owner = str(actor).strip()
    if not owner:
        raise ProfileControlError(
            "current_hold_actor_invalid", "Prerequisite actor is required"
        )
    with connect(db_path) as connection, transaction(connection):
        # Read the production clock only after BEGIN IMMEDIATE linearizes this
        # control mutation.  A caller waiting for the writer lock must not act
        # on an activation or expiry window captured before it acquired the DB.
        timestamp = _utc(now)
        if parse_time(expiry) <= parse_time(timestamp):
            raise ProfileControlError(
                "current_hold_prerequisite_expired",
                f"Current-hold {kind} prerequisite is already expired",
            )
        start, start_control, active = _require_current_hold_start(
            connection, drain_id=drain_id, timestamp=timestamp
        )
        if _hold_event(connection, drain_id=drain_id, event_type="sealed") is not None:
            raise ProfileControlError(
                "current_hold_already_sealed",
                "Prerequisites cannot change after HOLD_SEAL",
            )
        current_generation = _hold_generation(
            connection, drain_id=drain_id, start_control=start_control
        )
        target_generation = (
            int(current_generation["generation"])
            if generation is None
            else generation
        )
        if (
            type(target_generation) is not int
            or target_generation < 1
            or target_generation > int(current_generation["generation"]) + 1
            or (
                target_generation == int(current_generation["generation"]) + 1
                and kind not in {"build", "runtime", "config"}
            )
        ):
            raise ProfileControlError(
                "current_hold_prerequisite_generation_invalid",
                "Prerequisite generation is not current or the next build generation",
            )
        payload = {
            "action": "hold_prerequisite",
            "contract_version": CURRENT_HOLD_PREREQUISITE_CONTRACT,
            "control_contract_version": CURRENT_ACTIVATION_HOLD_CONTRACT,
            "drain_id": drain_id,
            "start_event_id": int(start["event_id"]),
            "start_event_hash": str(start["event_hash"]),
            "activation_id": int(active["activation_id"]),
            "roster_snapshot_id": int(active["roster_snapshot_id"]),
            "roster_snapshot_hash": str(active["roster_members_sha256"]),
            "generation": target_generation,
            "kind": kind,
            "receipt_contract_version": expected_contract,
            "artifact_sha256": artifact,
            "issued_at": timestamp,
            "expires_at": expiry,
            "evidence": verified_evidence,
            "actor": owner,
        }
        receipt = _record_hold_control_receipt(
            connection,
            scheduled_for=(
                f"current-hold:{drain_id}:prerequisite:"
                f"{target_generation}:{kind}:{artifact}"
            ),
            payload=payload,
            timestamp=timestamp,
        )
        if mirror_root is not None:
            _write_control_receipt_mirror(receipt, mirror_root)
        return {"prerequisite": _control_event_public(receipt)}


def _current_hold_prerequisites(
    connection: sqlite3.Connection,
    *,
    drain_id: str,
    generation: int,
    active: Mapping[str, Any],
    expected: Mapping[str, str],
    at: str,
) -> dict[str, dict[str, Any]]:
    rows = connection.execute(
        "SELECT * FROM scheduler_runs WHERE job_id=? AND status='succeeded' "
        "ORDER BY id DESC",
        (CURRENT_HOLD_CONTROL_JOB,),
    ).fetchall()
    latest: dict[str, dict[str, Any]] = {}
    for row in rows:
        receipt = _read_hold_control_receipt(connection, row)
        payload = receipt.get("payload")
        if (
            not isinstance(payload, dict)
            or payload.get("action") != "hold_prerequisite"
            or payload.get("drain_id") != drain_id
            or payload.get("generation") != generation
        ):
            continue
        kind = payload.get("kind")
        if isinstance(kind, str) and kind in expected and kind not in latest:
            latest[kind] = {"receipt": receipt, "payload": payload}
    if set(latest) != set(expected):
        missing = sorted(set(expected) - set(latest))
        raise ProfileControlError(
            "current_hold_prerequisite_missing",
            f"HOLD_SEAL prerequisites are missing: {','.join(missing)}",
        )
    verified: dict[str, dict[str, Any]] = {}
    for kind, artifact in expected.items():
        entry = latest[kind]
        receipt = entry["receipt"]
        payload = entry["payload"]
        expiry = payload.get("expires_at")
        if (
            payload.get("contract_version")
            != CURRENT_HOLD_PREREQUISITE_CONTRACT
            or payload.get("receipt_contract_version")
            != CURRENT_HOLD_PREREQUISITE_CONTRACTS[kind]
            or payload.get("artifact_sha256") != artifact
            or payload.get("activation_id") != int(active["activation_id"])
            or payload.get("roster_snapshot_id")
            != int(active["roster_snapshot_id"])
            or payload.get("roster_snapshot_hash")
            != active["roster_members_sha256"]
            or not isinstance(expiry, str)
            or parse_time(expiry) <= parse_time(at)
        ):
            raise ProfileControlError(
                "current_hold_prerequisite_drift",
                f"Current-hold {kind} prerequisite is stale or changed",
            )
        _validate_prerequisite_evidence(
            kind=kind,
            artifact_sha256=artifact,
            evidence=payload.get("evidence", {}),
        )
        verified[kind] = {
            "receipt_sha256": str(receipt["self_sha256"]),
            "artifact_sha256": artifact,
            "receipt_contract_version": payload["receipt_contract_version"],
            "issued_at": payload["issued_at"],
            "expires_at": expiry,
        }
    return verified


def validate_current_hold_release_prerequisites(
    connection: sqlite3.Connection,
    *,
    active: Mapping[str, Any],
    release_control: Mapping[str, Any],
    at: str,
) -> dict[str, dict[str, Any]]:
    """Verify admission evidence at release time and current provider health.

    Prerequisite expiry limits when a release may be admitted. Once released,
    those immutable receipts cannot be renewed; their expiry is not a lease
    on every subsequent request. The durable release must still match exactly.
    """

    released = _hold_event(
        connection,
        drain_id=str(release_control.get("drain_id") or ""),
        event_type="release",
    )
    if (
        released is None
        or released["payload"].get("control") != dict(release_control)
        or parse_time(str(released["created_at"])) > parse_time(at)
    ):
        raise ProfileControlError(
            "required_operation_unqualified", "Release evidence is absent or changed"
        )

    final = release_control.get("final_contract")
    if not isinstance(final, Mapping):
        raise ProfileControlError(
            "required_operation_unqualified",
            "Current-hold release has no frozen final contract",
        )
    expected = {
        "build": str(final.get("final_build_receipt_sha256") or ""),
        "runtime": str(final.get("runtime_root_receipt_sha256") or ""),
        "config": str(final.get("config_receipt_sha256") or ""),
        "qualification": str(final.get("qualification_receipt_sha256") or ""),
        "capacity": str(final.get("capacity_receipt_sha256") or ""),
        "price": str(final.get("price_receipt_sha256") or ""),
        "budget": str(final.get("budget_receipt_sha256") or ""),
    }
    generation = final.get("generation")
    if (
        type(generation) is not int
        or generation < 1
        or any(_SHA256.fullmatch(value) is None for value in expected.values())
    ):
        raise ProfileControlError(
            "required_operation_unqualified",
            "Current-hold release prerequisite binding is invalid",
        )
    try:
        verified = _current_hold_prerequisites(
            connection,
            drain_id=str(release_control.get("drain_id") or ""),
            generation=generation,
            active=active,
            expected=expected,
            at=str(released["created_at"]),
        )
    except ProfileControlError as exc:
        raise ProfileControlError(
            "required_operation_unqualified",
            "Current-hold release prerequisite is missing, stale, or invalid",
        ) from exc
    if final.get("prerequisites") != verified:
        raise ProfileControlError(
            "required_operation_unqualified",
            "Current-hold release prerequisite set drifted",
        )
    from .provider_budget import circuit_state

    provider_circuit = circuit_state(connection)
    if provider_circuit and provider_circuit.get("open"):
        raise ProfileControlError(
            "provider_blocked", "TikHub provider circuit is open"
        )
    return verified


def _control_event_public(receipt: Mapping[str, Any]) -> dict[str, Any]:
    payload = receipt.get("payload")
    if not isinstance(payload, dict):
        raise ProfileControlError(
            "current_hold_control_invalid", "Current-hold control payload is invalid"
        )
    event_type = str(payload.get("action") or "")
    if event_type == "hold_build_advance":
        event_type = "build_advance"
    return {
        "event_id": int(receipt["run_id"]),
        "event_hash": str(receipt["self_sha256"]),
        "event_type": event_type,
        "created_at": str(receipt["recorded_at"]),
        "payload": json.loads(_canonical(payload)),
    }


def begin_current_activation_hold_in_transaction(
    connection: sqlite3.Connection,
    *,
    drain_id: str,
    build_receipt_sha256: str,
    runtime_root_receipt_sha256: str,
    actor: str,
    reason: str,
    not_before_business_day: str,
    now: str,
    expected_submission_scope: Mapping[str, Any] | None = None,
) -> dict[str, Any]:
    """Fence ordinary paid traffic for the already-current activation."""

    if not connection.in_transaction:
        raise ProfileControlError(
            "profile_control_transaction_required", "HOLD_BEGIN requires a caller transaction"
        )
    _require_schema19(connection)
    timestamp = _utc(now)
    build = _sha(build_receipt_sha256, label="build receipt")
    runtime = _sha(runtime_root_receipt_sha256, label="runtime-root receipt")
    not_before = _business_day(
        not_before_business_day, label="not-before business day"
    )
    active = _active(connection, timestamp)
    existing = _hold_event(connection, drain_id=drain_id, event_type="start")
    if existing is not None:
        existing_control = existing["payload"].get("control")
        if (
            not isinstance(existing_control, dict)
            or existing_control.get("contract_version")
            != CURRENT_ACTIVATION_HOLD_CONTRACT
            or existing_control.get("control_purpose") != "hold_begin"
            or existing_control.get("activation_id") != int(active["activation_id"])
            or existing_control.get("initial_build_receipt_sha256") != build
            or existing_control.get("initial_runtime_root_receipt_sha256") != runtime
            or existing_control.get("not_before_business_day") != not_before
            or existing_control.get("actor") != str(actor).strip()
            or existing_control.get("reason") != str(reason).strip()
            or int(existing["target_activation_id"]) != int(active["activation_id"])
        ):
            raise ProfileControlError(
                "current_hold_idempotency_conflict",
                "Existing HOLD_BEGIN has a different binding",
            )
        return {"activation": active, "start": _event_public(existing)}
    matrix_running = _matrix_running_run_ids(connection)
    matrix_unresolved = _matrix_unresolved_dispatch_ids(connection)
    if matrix_running or matrix_unresolved:
        raise ProfileControlError(
            "current_hold_matrix_inflight",
            "HOLD_BEGIN requires zero Matrix running attempts and unresolved "
            f"dispatches: runs={matrix_running}, dispatches={matrix_unresolved}",
        )
    predecessor = _current_activation_predecessor_release(
        connection,
        activation_id=int(active["activation_id"]),
        at=timestamp,
    )
    _require_current_hold_command_submission_scope(
        expected=expected_submission_scope,
        active=active,
        predecessor=dict(predecessor),
    )
    previous_release_event_id = int(predecessor["id"])
    previous_release_event_hash = str(predecessor["event_hash"])
    control = {
        "contract_version": CURRENT_ACTIVATION_HOLD_CONTRACT,
        "control_purpose": "hold_begin",
        "drain_id": drain_id,
        "activation_id": int(active["activation_id"]),
        "profile_id": str(active["profile_id"]),
        "roster_snapshot_id": int(active["roster_snapshot_id"]),
        "roster_snapshot_hash": str(active["roster_members_sha256"]),
        "previous_release_event_id": previous_release_event_id,
        "previous_release_event_hash": previous_release_event_hash,
        "initial_build_receipt_sha256": build,
        "initial_runtime_root_receipt_sha256": runtime,
        "matrix_high_watermarks": _matrix_high_watermarks(connection),
        "not_before_business_day": not_before,
        "actor": str(actor).strip(),
        "reason": str(reason).strip(),
    }
    if not control["actor"] or not control["reason"]:
        raise ProfileControlError(
            "current_hold_actor_invalid", "HOLD_BEGIN actor and reason are required"
        )
    binding = {
        "source_activation_id": int(active["activation_id"]),
        "target_activation_id": int(active["activation_id"]),
        "business_day": parse_time(timestamp).astimezone(BEIJING).date().isoformat(),
        "planned_effective_at": _day_midnight_utc(not_before),
        "build_receipt_sha256": build,
        "runtime_root_receipt_sha256": runtime,
        "not_before_business_day": not_before,
    }
    try:
        start = start_profile_drain_in_transaction(
            connection,
            drain_id,
            binding=binding,
            switch_kind="same_profile",
            now=timestamp,
            nonblocking=False,
            control=control,
        )
    except PaidDrainError as exc:
        raise ProfileControlError("current_hold_begin_failed", str(exc)) from exc
    return {"activation": active, "start": start.as_dict()}


def begin_current_activation_hold(
    *,
    db_path: Path,
    drain_id: str,
    build_receipt_sha256: str,
    runtime_root_receipt_sha256: str,
    actor: str,
    reason: str,
    not_before_business_day: str,
    now: str | None = None,
    mirror_root: Path | None = None,
    expected_submission_scope: Mapping[str, Any] | None = None,
) -> dict[str, Any]:
    if is_formal_database_path(db_path) and mirror_root is None:
        raise ProfileControlError(
            "current_hold_mirror_required", "Formal HOLD_BEGIN requires a private mirror root"
        )
    with connect(db_path) as connection, transaction(connection):
        result = begin_current_activation_hold_in_transaction(
            connection,
            drain_id=drain_id,
            build_receipt_sha256=build_receipt_sha256,
            runtime_root_receipt_sha256=runtime_root_receipt_sha256,
            actor=actor,
            reason=reason,
            not_before_business_day=not_before_business_day,
            now=_utc(now),
            expected_submission_scope=expected_submission_scope,
        )
        if mirror_root is not None:
            event = _hold_event(connection, drain_id=drain_id, event_type="start")
            assert event is not None
            _write_native_event_mirror(event, mirror_root)
        return result


def advance_current_activation_hold_build(
    *,
    db_path: Path,
    drain_id: str,
    from_build_receipt_sha256: str,
    to_build_receipt_sha256: str,
    runtime_root_receipt_sha256: str,
    config_receipt_sha256: str,
    actor: str,
    reason: str,
    now: str | None = None,
    mirror_root: Path | None = None,
    scheduler: BaseScheduler | None = None,
) -> dict[str, Any]:
    if is_formal_database_path(db_path) and mirror_root is None:
        raise ProfileControlError(
            "current_hold_mirror_required",
            "Formal HOLD_BUILD_ADVANCE requires a private mirror root",
        )
    if is_formal_database_path(db_path) and scheduler is None:
        raise ProfileControlError("current_hold_writer_required", "Formal build advancement requires the paused Writer scheduler")
    with connect(db_path) as connection, transaction(connection):
        timestamp = _utc(now)
        start, control, active = _require_current_hold_start(
            connection, drain_id=drain_id, timestamp=timestamp
        )
        if _hold_event(connection, drain_id=drain_id, event_type="sealed") is not None:
            raise ProfileControlError(
                "current_hold_already_sealed", "HOLD_BUILD_ADVANCE is forbidden after HOLD_SEAL"
            )
        source = _sha(from_build_receipt_sha256, label="from build receipt")
        target = _sha(to_build_receipt_sha256, label="to build receipt")
        runtime = _sha(runtime_root_receipt_sha256, label="runtime-root receipt")
        config = _sha(config_receipt_sha256, label="config receipt")
        scheduled_for = f"current-hold:{drain_id}:build:{target}"
        existing = connection.execute(
            "SELECT * FROM scheduler_runs WHERE job_id=? AND scheduled_for=?",
            (CURRENT_HOLD_CONTROL_JOB, scheduled_for),
        ).fetchone()
        if existing is not None:
            receipt = _read_hold_control_receipt(connection, existing)
            payload = receipt.get("payload")
            if (
                not isinstance(payload, dict)
                or payload.get("from_build_receipt_sha256") != source
                or payload.get("to_build_receipt_sha256") != target
                or payload.get("runtime_root_receipt_sha256") != runtime
                or payload.get("config_receipt_sha256") != config
                or payload.get("actor") != str(actor).strip()
                or payload.get("reason") != str(reason).strip()
            ):
                raise ProfileControlError(
                    "current_hold_idempotency_conflict",
                    "Existing HOLD_BUILD_ADVANCE has a different binding",
                )
            if mirror_root is not None:
                _write_control_receipt_mirror(receipt, mirror_root)
            return {"build_advance": _control_event_public(receipt)}
        try:
            if scheduler is not None:
                from .transport_accounting import settle_closed_hold_unknowns

                if mirror_root is None:
                    raise ProfileControlError("current_hold_mirror_required", "Diagnostic closeout requires a private mirror root")
                settle_closed_hold_unknowns(connection, drain_id=drain_id, scheduler=scheduler,
                                            at=timestamp, mirror_root=mirror_root)
            verification = verify_profile_drain_sealable(
                connection, drain_id, now=timestamp
            )
        except PaidDrainError as exc:
            raise ProfileControlError("current_hold_build_inflight", str(exc)) from exc
        current = _hold_generation(
            connection, drain_id=drain_id, start_control=control
        )
        if source != current["build_receipt_sha256"]:
            raise ProfileControlError(
                "current_hold_build_generation_conflict",
                "HOLD_BUILD_ADVANCE source is not the current generation",
            )
        if source == target:
            raise ProfileControlError(
                "current_hold_build_unchanged", "HOLD_BUILD_ADVANCE must change the build"
            )
        next_generation = int(current["generation"]) + 1
        prerequisites = _current_hold_prerequisites(
            connection,
            drain_id=drain_id,
            generation=next_generation,
            active=active,
            expected={
                "build": target,
                "runtime": runtime,
                "config": config,
            },
            at=timestamp,
        )
        payload = {
            "action": "hold_build_advance",
            "control_contract_version": CURRENT_ACTIVATION_HOLD_CONTRACT,
            "drain_id": drain_id,
            "start_event_id": int(start["event_id"]),
            "start_event_hash": str(start["event_hash"]),
            "activation_id": int(active["activation_id"]),
            "generation": next_generation,
            "from_build_receipt_sha256": source,
            "to_build_receipt_sha256": target,
            "runtime_root_receipt_sha256": runtime,
            "config_receipt_sha256": config,
            "prerequisites": prerequisites,
            "verification": verification,
            "actor": str(actor).strip(),
            "reason": str(reason).strip(),
        }
        if not payload["actor"] or not payload["reason"]:
            raise ProfileControlError(
                "current_hold_actor_invalid",
                "HOLD_BUILD_ADVANCE actor and reason are required",
            )
        receipt = _record_hold_control_receipt(
            connection,
            scheduled_for=scheduled_for,
            payload=payload,
            timestamp=timestamp,
        )
        if mirror_root is not None:
            _write_control_receipt_mirror(receipt, mirror_root)
        return {"build_advance": _control_event_public(receipt)}


def seal_current_activation_hold(
    *,
    db_path: Path,
    drain_id: str,
    final_build_receipt_sha256: str,
    runtime_root_receipt_sha256: str,
    config_receipt_sha256: str,
    qualification_receipt_sha256: str,
    capacity_receipt_sha256: str,
    price_receipt_sha256: str,
    budget_receipt_sha256: str,
    actor: str,
    now: str | None = None,
    mirror_root: Path | None = None,
) -> dict[str, Any]:
    if is_formal_database_path(db_path) and mirror_root is None:
        raise ProfileControlError(
            "current_hold_mirror_required", "Formal HOLD_SEAL requires a private mirror root"
        )
    with connect(db_path) as connection, transaction(connection):
        timestamp = _utc(now)
        start, start_control, active = _require_current_hold_start(
            connection, drain_id=drain_id, timestamp=timestamp
        )
        generation = _hold_generation(
            connection, drain_id=drain_id, start_control=start_control
        )
        matrix_high_watermarks = start_control.get("matrix_high_watermarks")
        if not isinstance(matrix_high_watermarks, dict):
            raise ProfileControlError(
                "current_hold_matrix_evidence_invalid",
                "HOLD_BEGIN Matrix high-watermark evidence is missing",
            )
        _require_matrix_fence(connection, matrix_high_watermarks)
        final = {
            "final_build_receipt_sha256": _sha(
                final_build_receipt_sha256, label="final build receipt"
            ),
            "runtime_root_receipt_sha256": _sha(
                runtime_root_receipt_sha256, label="runtime-root receipt"
            ),
            "config_receipt_sha256": _sha(
                config_receipt_sha256, label="config receipt"
            ),
            "qualification_receipt_sha256": _sha(
                qualification_receipt_sha256, label="qualification receipt"
            ),
            "capacity_receipt_sha256": _sha(
                capacity_receipt_sha256, label="capacity receipt"
            ),
            "price_receipt_sha256": _sha(
                price_receipt_sha256, label="price receipt"
            ),
            "budget_receipt_sha256": _sha(
                budget_receipt_sha256, label="budget receipt"
            ),
        }
        if final["final_build_receipt_sha256"] != generation["build_receipt_sha256"]:
            raise ProfileControlError(
                "current_hold_final_build_mismatch", "HOLD_SEAL final build is not current"
            )
        if final["runtime_root_receipt_sha256"] != generation["runtime_root_receipt_sha256"]:
            raise ProfileControlError(
                "current_hold_runtime_mismatch", "HOLD_SEAL runtime receipt is not current"
            )
        if (
            generation["config_receipt_sha256"] is not None
            and final["config_receipt_sha256"] != generation["config_receipt_sha256"]
        ):
            raise ProfileControlError(
                "current_hold_config_mismatch", "HOLD_SEAL config receipt is not current"
            )
        expected_prerequisites = {
            "build": final["final_build_receipt_sha256"],
            "runtime": final["runtime_root_receipt_sha256"],
            "config": final["config_receipt_sha256"],
            "qualification": final["qualification_receipt_sha256"],
            "capacity": final["capacity_receipt_sha256"],
            "price": final["price_receipt_sha256"],
            "budget": final["budget_receipt_sha256"],
        }
        prerequisite_fields = {
            "build": "final_build_receipt_sha256",
            "runtime": "runtime_root_receipt_sha256",
            "config": "config_receipt_sha256",
            "qualification": "qualification_receipt_sha256",
            "capacity": "capacity_receipt_sha256",
            "price": "price_receipt_sha256",
            "budget": "budget_receipt_sha256",
        }
        existing = _hold_event(connection, drain_id=drain_id, event_type="sealed")
        if existing is not None:
            existing_control = existing["payload"].get("control")
            if (
                not isinstance(existing_control, dict)
                or existing_control.get("contract_version")
                != CURRENT_ACTIVATION_HOLD_CONTRACT
                or existing_control.get("control_purpose") != "hold_seal"
                or existing_control.get("drain_id") != drain_id
                or existing_control.get("activation_id")
                != int(active["activation_id"])
                or existing_control.get("generation") != int(generation["generation"])
                or existing_control.get("actor") != str(actor).strip()
                or any(
                    existing_control.get(prerequisite_fields[kind])
                    != artifact
                    for kind, artifact in expected_prerequisites.items()
                )
            ):
                raise ProfileControlError(
                    "current_hold_idempotency_conflict",
                    "Existing HOLD_SEAL has a different binding",
                )
            if mirror_root is not None:
                _write_native_event_mirror(existing, mirror_root)
            return {"sealed": _event_public(existing)}
        prerequisites = _current_hold_prerequisites(
            connection,
            drain_id=drain_id,
            generation=int(generation["generation"]),
            active=active,
            expected=expected_prerequisites,
            at=timestamp,
        )
        control = {
            "contract_version": CURRENT_ACTIVATION_HOLD_CONTRACT,
            "control_purpose": "hold_seal",
            "drain_id": drain_id,
            "activation_id": int(active["activation_id"]),
            "roster_snapshot_id": int(active["roster_snapshot_id"]),
            "roster_snapshot_hash": str(active["roster_members_sha256"]),
            "start_event_id": int(start["event_id"]),
            "start_event_hash": str(start["event_hash"]),
            "generation": int(generation["generation"]),
            **final,
            "prerequisites": prerequisites,
            "actor": str(actor).strip(),
        }
        if not control["actor"]:
            raise ProfileControlError(
                "current_hold_actor_invalid", "HOLD_SEAL actor is required"
            )
        try:
            sealed = seal_profile_drain_in_transaction(
                connection,
                drain_id,
                now=timestamp,
                force_strict=True,
                control=control,
            )
        except PaidDrainError as exc:
            raise ProfileControlError("current_hold_seal_failed", str(exc)) from exc
        if mirror_root is not None:
            event = _hold_event(connection, drain_id=drain_id, event_type="sealed")
            assert event is not None
            _write_native_event_mirror(event, mirror_root)
        return {"sealed": sealed.as_dict()}


def release_current_activation_hold(
    *,
    db_path: Path,
    drain_id: str,
    actor: str,
    now: str | None = None,
    mirror_root: Path | None = None,
) -> dict[str, Any]:
    if is_formal_database_path(db_path) and mirror_root is None:
        raise ProfileControlError(
            "current_hold_mirror_required", "Formal HOLD_RELEASE requires a private mirror root"
        )
    with connect(db_path) as connection, transaction(connection):
        timestamp = _utc(now)
        start, start_control, active = _require_current_hold_start(
            connection, drain_id=drain_id, timestamp=timestamp
        )
        existing = _hold_event(connection, drain_id=drain_id, event_type="release")
        if existing is not None:
            control = existing["payload"].get("control")
            sealed = _hold_event(connection, drain_id=drain_id, event_type="sealed")
            existing_release_day = (
                parse_time(str(existing["created_at"]))
                .astimezone(BEIJING)
                .date()
                .isoformat()
            )
            if (
                not isinstance(control, dict)
                or control.get("contract_version") != CURRENT_ACTIVATION_HOLD_CONTRACT
                or control.get("control_purpose") != "full_day_release"
                or control.get("drain_id") != drain_id
                or control.get("activation_id") != int(active["activation_id"])
                or control.get("roster_snapshot_id")
                != int(active["roster_snapshot_id"])
                or control.get("roster_snapshot_hash")
                != active["roster_members_sha256"]
                or control.get("release_business_day") != existing_release_day
                or control.get("actor") != str(actor).strip()
                or sealed is None
                or control.get("sealed_event_id") != int(sealed["event_id"])
                or control.get("sealed_event_hash") != str(sealed["event_hash"])
                or control.get("final_contract")
                != sealed["payload"].get("control")
            ):
                raise ProfileControlError(
                    "current_hold_idempotency_conflict",
                    "Existing HOLD_RELEASE has a different or invalid binding",
                )
            if mirror_root is not None:
                _write_native_event_mirror(existing, mirror_root)
            return {"release": _event_public(existing)}
        sealed = _hold_event(connection, drain_id=drain_id, event_type="sealed")
        if sealed is None:
            raise ProfileControlError(
                "current_hold_not_sealed", "HOLD_RELEASE requires HOLD_SEAL"
            )
        seal_control = sealed["payload"].get("control")
        if (
            not isinstance(seal_control, dict)
            or seal_control.get("contract_version") != CURRENT_ACTIVATION_HOLD_CONTRACT
            or seal_control.get("control_purpose") != "hold_seal"
        ):
            raise ProfileControlError(
                "current_hold_seal_invalid", "HOLD_SEAL binding is invalid"
            )
        matrix_high_watermarks = start_control.get("matrix_high_watermarks")
        if not isinstance(matrix_high_watermarks, dict):
            raise ProfileControlError(
                "current_hold_matrix_evidence_invalid",
                "HOLD_BEGIN Matrix high-watermark evidence is missing",
            )
        _require_matrix_fence(connection, matrix_high_watermarks)
        expected_prerequisites = {
            "build": str(seal_control.get("final_build_receipt_sha256") or ""),
            "runtime": str(seal_control.get("runtime_root_receipt_sha256") or ""),
            "config": str(seal_control.get("config_receipt_sha256") or ""),
            "qualification": str(
                seal_control.get("qualification_receipt_sha256") or ""
            ),
            "capacity": str(seal_control.get("capacity_receipt_sha256") or ""),
            "price": str(seal_control.get("price_receipt_sha256") or ""),
            "budget": str(seal_control.get("budget_receipt_sha256") or ""),
        }
        if any(_SHA256.fullmatch(value) is None for value in expected_prerequisites.values()):
            raise ProfileControlError(
                "current_hold_seal_invalid", "HOLD_SEAL prerequisite hashes are invalid"
            )
        prerequisites = _current_hold_prerequisites(
            connection,
            drain_id=drain_id,
            generation=int(seal_control.get("generation") or 0),
            active=active,
            expected=expected_prerequisites,
            at=timestamp,
        )
        if seal_control.get("prerequisites") != prerequisites:
            raise ProfileControlError(
                "current_hold_prerequisite_drift",
                "HOLD_RELEASE prerequisite binding differs from HOLD_SEAL",
            )
        from .provider_budget import circuit_state

        provider_circuit = circuit_state(connection)
        if provider_circuit and provider_circuit.get("open"):
            raise ProfileControlError(
                "provider_blocked",
                "HOLD_RELEASE is blocked while the TikHub provider circuit is open",
            )
        local = parse_time(timestamp).astimezone(BEIJING)
        seconds = local.hour * 3600 + local.minute * 60 + local.second
        if seconds >= FULL_DAY_RELEASE_WINDOW_SECONDS:
            raise ProfileControlError(
                "current_activation_hold_release_window",
                "HOLD_RELEASE is allowed only at 00:00:00-00:04:59 Beijing",
            )
        release_day = local.date().isoformat()
        if release_day < str(start_control["not_before_business_day"]):
            raise ProfileControlError(
                "current_activation_hold_release_too_early",
                "HOLD_RELEASE is before its not-before day",
            )
        if (
            seal_control.get("activation_id") != int(active["activation_id"])
            or seal_control.get("roster_snapshot_id") != int(active["roster_snapshot_id"])
            or seal_control.get("roster_snapshot_hash") != active["roster_members_sha256"]
        ):
            raise ProfileControlError(
                "current_hold_release_scope_drift", "HOLD_RELEASE activation or roster drifted"
            )
        control = {
            "contract_version": CURRENT_ACTIVATION_HOLD_CONTRACT,
            "control_purpose": "full_day_release",
            "drain_id": drain_id,
            "activation_id": int(active["activation_id"]),
            "roster_snapshot_id": int(active["roster_snapshot_id"]),
            "roster_snapshot_hash": str(active["roster_members_sha256"]),
            "release_business_day": release_day,
            "sealed_event_id": int(sealed["event_id"]),
            "sealed_event_hash": str(sealed["event_hash"]),
            "final_contract": seal_control,
            "actor": str(actor).strip(),
        }
        if not control["actor"]:
            raise ProfileControlError(
                "current_hold_actor_invalid", "HOLD_RELEASE actor is required"
            )
        try:
            released = release_profile_drain_in_transaction(
                connection, drain_id, now=timestamp, control=control
            )
        except PaidDrainError as exc:
            raise ProfileControlError("current_hold_release_failed", str(exc)) from exc
        if mirror_root is not None:
            event = _hold_event(connection, drain_id=drain_id, event_type="release")
            assert event is not None
            _write_native_event_mirror(event, mirror_root)
        return {"release": released.as_dict()}


def reopen_current_activation_hold(
    *,
    db_path: Path,
    drain_id: str,
    new_drain_id: str,
    build_receipt_sha256: str,
    runtime_root_receipt_sha256: str,
    actor: str,
    reason: str,
    not_before_business_day: str,
    now: str | None = None,
    mirror_root: Path | None = None,
) -> dict[str, Any]:
    if is_formal_database_path(db_path) and mirror_root is None:
        raise ProfileControlError(
            "current_hold_mirror_required", "Formal HOLD_REOPEN requires a private mirror root"
        )
    with connect(db_path) as connection, transaction(connection):
        timestamp = _utc(now)
        _start, start_control, active = _require_current_hold_start(
            connection, drain_id=drain_id, timestamp=timestamp
        )
        existing_release = _hold_event(
            connection, drain_id=drain_id, event_type="release"
        )
        if existing_release is not None:
            existing_control = existing_release["payload"].get("control")
            if (
                not isinstance(existing_control, dict)
                or existing_control.get("contract_version")
                != CURRENT_ACTIVATION_HOLD_CONTRACT
                or existing_control.get("control_purpose") != "reopen_only"
                or existing_control.get("drain_id") != drain_id
                or existing_control.get("activation_id")
                != int(active["activation_id"])
                or existing_control.get("next_drain_id") != new_drain_id
                or existing_control.get("actor") != str(actor).strip()
                or existing_control.get("reason") != str(reason).strip()
            ):
                raise ProfileControlError(
                    "current_hold_idempotency_conflict",
                    "Existing HOLD_REOPEN has a different binding",
                )
            reopened = begin_current_activation_hold_in_transaction(
                connection,
                drain_id=new_drain_id,
                build_receipt_sha256=build_receipt_sha256,
                runtime_root_receipt_sha256=runtime_root_receipt_sha256,
                actor=actor,
                reason=reason,
                not_before_business_day=not_before_business_day,
                now=timestamp,
            )
            if mirror_root is not None:
                new_event = _hold_event(
                    connection, drain_id=new_drain_id, event_type="start"
                )
                assert new_event is not None
                _write_native_event_mirror(existing_release, mirror_root)
                _write_native_event_mirror(new_event, mirror_root)
            return {
                "release": _event_public(existing_release),
                "start": reopened["start"],
            }
        matrix_high_watermarks = start_control.get("matrix_high_watermarks")
        if not isinstance(matrix_high_watermarks, dict):
            raise ProfileControlError(
                "current_hold_matrix_evidence_invalid",
                "HOLD_BEGIN Matrix high-watermark evidence is missing",
            )
        _require_matrix_fence(connection, matrix_high_watermarks)
        sealed = _hold_event(connection, drain_id=drain_id, event_type="sealed")
        if sealed is None:
            raise ProfileControlError(
                "current_hold_reopen_state", "HOLD_REOPEN requires SEALED without RELEASE"
            )
        reopen_control = {
            "contract_version": CURRENT_ACTIVATION_HOLD_CONTRACT,
            "control_purpose": "reopen_only",
            "drain_id": drain_id,
            "activation_id": int(active["activation_id"]),
            "sealed_event_id": int(sealed["event_id"]),
            "sealed_event_hash": str(sealed["event_hash"]),
            "next_drain_id": new_drain_id,
            "actor": str(actor).strip(),
            "reason": str(reason).strip(),
        }
        if not reopen_control["actor"] or not reopen_control["reason"]:
            raise ProfileControlError(
                "current_hold_actor_invalid", "HOLD_REOPEN actor and reason are required"
            )
        try:
            released = release_profile_drain_in_transaction(
                connection,
                drain_id,
                now=timestamp,
                control=reopen_control,
            )
        except PaidDrainError as exc:
            raise ProfileControlError("current_hold_reopen_failed", str(exc)) from exc
        reopened = begin_current_activation_hold_in_transaction(
            connection,
            drain_id=new_drain_id,
            build_receipt_sha256=build_receipt_sha256,
            runtime_root_receipt_sha256=runtime_root_receipt_sha256,
            actor=actor,
            reason=reason,
            not_before_business_day=not_before_business_day,
            now=timestamp,
        )
        if mirror_root is not None:
            old_event = _hold_event(connection, drain_id=drain_id, event_type="release")
            new_event = _hold_event(connection, drain_id=new_drain_id, event_type="start")
            assert old_event is not None and new_event is not None
            _write_native_event_mirror(old_event, mirror_root)
            _write_native_event_mirror(new_event, mirror_root)
        return {"release": released.as_dict(), "start": reopened["start"]}


def _command_binding(
    *, command_id: str, command: str, parameters: Mapping[str, Any]
) -> dict[str, Any]:
    if _COMMAND_ID.fullmatch(str(command_id)) is None:
        raise ProfileControlError(
            "current_hold_command_id_invalid", "Current-hold command ID is invalid"
        )
    if command not in _CURRENT_HOLD_COMMANDS:
        raise ProfileControlError(
            "current_hold_command_invalid", "Current-hold command is unsupported"
        )
    if not isinstance(parameters, Mapping):
        raise ProfileControlError(
            "current_hold_command_invalid", "Current-hold command parameters are required"
        )
    frozen = json.loads(_canonical(dict(parameters)))
    if command == "capture_release":
        from .capture_release_commands import validate_parameters

        frozen = validate_parameters(frozen)
    if command == "forward_only_release" and (
        set(frozen) != {"drain_id", "route_verdict_receipt_id", "scope_start", "actor"}
        or any(not isinstance(frozen.get(key), str) or not frozen[key].strip()
               for key in ("drain_id", "scope_start", "actor"))
        or type(frozen.get("route_verdict_receipt_id")) is not int
        or frozen["route_verdict_receipt_id"] < 1
    ):
        raise ProfileControlError(
            "current_hold_command_scope_invalid",
            "Forward recovery accepts only the current drain, control verdict, business date and actor",
        )
    if command == "transport_primary" and (
        set(frozen) != {"drain_id"}
        or not isinstance(frozen["drain_id"], str)
        or not frozen["drain_id"].strip()
    ):
        raise ProfileControlError(
            "current_hold_command_scope_invalid", "Primary diagnostics accept only the current drain_id",
        )
    if command == "transport_control" and (
        set(frozen) != {"drain_id", "source_verdict_receipt_id", "arm"}
        or not isinstance(frozen["drain_id"], str)
        or not frozen["drain_id"].strip()
        or not isinstance(frozen["source_verdict_receipt_id"], int)
        or frozen["source_verdict_receipt_id"] < 1
        or frozen["arm"] not in {"control_io", "control_legacy"}
    ):
        raise ProfileControlError(
            "current_hold_command_scope_invalid",
            "Control diagnostics require drain_id, source_verdict_receipt_id and a fixed control arm",
        )
    if "db_path" in frozen or "mirror_root" in frozen or "now" in frozen:
        raise ProfileControlError(
            "current_hold_command_scope_invalid",
            "Current-hold commands cannot override database, mirror, or clock scope",
        )
    return {
        "command_id": str(command_id),
        "command": command,
        "parameters": frozen,
    }


def _decode_command_row(row: sqlite3.Row | Mapping[str, Any]) -> dict[str, Any]:
    value = dict(row)
    try:
        details = json.loads(str(value["details_json"]))
    except (KeyError, TypeError, ValueError) as exc:
        raise ProfileControlError(
            "current_hold_command_corrupt", "Current-hold command details are invalid"
        ) from exc
    binding = details.get("binding") if isinstance(details, dict) else None
    if (
        value.get("job_id") != CURRENT_HOLD_COMMAND_JOB
        or not isinstance(binding, dict)
        or details.get("contract_version") != CURRENT_HOLD_COMMAND_CONTRACT
        or details.get("run_id") != int(value["id"])
        or details.get("command_sha256") != _digest(binding)
        or value.get("scheduled_for")
        != f"current-hold-command:{binding.get('command_id', '')}"
    ):
        raise ProfileControlError(
            "current_hold_command_corrupt", "Current-hold command binding is invalid"
        )
    return {**value, "details": details, "binding": binding}


def enqueue_current_activation_hold_command(
    *,
    db_path: Path,
    command_id: str,
    command: str,
    parameters: Mapping[str, Any],
    submitted_at: str | None = None,
) -> dict[str, Any]:
    """Persist one idempotent command; execution always happens after commit."""

    binding = _command_binding(
        command_id=command_id, command=command, parameters=parameters
    )
    scheduled_for = f"current-hold-command:{binding['command_id']}"
    with connect(db_path) as connection, transaction(connection):
        timestamp = _utc(submitted_at)
        _require_schema19(connection)
        if command == "capture_release":
            require_current_process_writer_lock(connection)
            if connection.execute("PRAGMA user_version").fetchone()[0] != 20:
                raise ProfileControlError("capture_release_schema_required", "Capture release actions require schema20")
        if command == "hold_begin":
            binding = {
                **binding,
                "submission_scope": _current_hold_command_submission_scope(
                    connection, at=timestamp
                ),
            }
        existing = connection.execute(
            "SELECT * FROM scheduler_runs WHERE job_id=? AND scheduled_for=?",
            (CURRENT_HOLD_COMMAND_JOB, scheduled_for),
        ).fetchone()
        if existing is not None:
            decoded = _decode_command_row(existing)
            if decoded["binding"] != binding:
                raise ProfileControlError(
                    "current_hold_command_conflict",
                    "Current-hold command ID already has another binding",
                )
            return current_activation_hold_command_status(
                connection, run_id=int(existing["id"])
            )
        cursor = connection.execute(
            "INSERT INTO scheduler_runs(job_id,scheduled_for,status,started_at,"
            "completed_at,details_json) VALUES (?,?,'interrupted',?,?,'{}')",
            (CURRENT_HOLD_COMMAND_JOB, scheduled_for, timestamp, timestamp),
        )
        run_id = int(cursor.lastrowid or 0)
        details = {
            "contract_version": CURRENT_HOLD_COMMAND_CONTRACT,
            "run_id": run_id,
            "state": "queued",
            "binding": binding,
            "command_sha256": _digest(binding),
            "submitted_at": timestamp,
        }
        connection.execute(
            "UPDATE scheduler_runs SET details_json=? WHERE id=? AND status='interrupted'",
            (_canonical(details), run_id),
        )
        return {
            "run_id": run_id,
            "status": "queued",
            "command_id": binding["command_id"],
            "command": binding["command"],
            "submitted_at": timestamp,
        }


def current_activation_hold_command_status(
    connection: sqlite3.Connection, *, run_id: int
) -> dict[str, Any]:
    row = connection.execute(
        "SELECT * FROM scheduler_runs WHERE id=? AND job_id=?",
        (run_id, CURRENT_HOLD_COMMAND_JOB),
    ).fetchone()
    if row is None:
        raise ProfileControlError(
            "current_hold_command_missing", "Current-hold command does not exist"
        )
    decoded = _decode_command_row(row)
    details = decoded["details"]
    binding = decoded["binding"]
    attempts = connection.execute(
        "SELECT id,attempt_number,status,started_at,completed_at FROM "
        "scheduler_run_attempts WHERE scheduler_run_id=? ORDER BY attempt_number",
        (run_id,),
    ).fetchall()
    status = str(decoded["status"])
    return {
        "run_id": run_id,
        "status": "queued" if status == "interrupted" and not attempts else status,
        "command_id": binding["command_id"],
        "command": binding["command"],
        "submitted_at": details["submitted_at"],
        "result": details.get("result"),
        "error": details.get("error"),
        "attempts": [dict(attempt) for attempt in attempts],
    }


def read_current_activation_hold_command(
    *,
    db_path: Path,
    run_id: int,
    read_only: bool = True,
    live_wal: bool = False,
) -> dict[str, Any]:
    read_context = (
        live_wal_read_only_connections()
        if read_only and live_wal
        else nullcontext()
    )
    with read_context:
        with connect(db_path, read_only=read_only) as connection:
            return current_activation_hold_command_status(connection, run_id=run_id)


def _claim_current_hold_command(db_path: Path) -> dict[str, Any] | None:
    with connect(db_path) as connection, transaction(connection):
        rows = connection.execute(
            "SELECT * FROM scheduler_runs WHERE job_id=? AND status='interrupted' "
            "ORDER BY id",
            (CURRENT_HOLD_COMMAND_JOB,),
        ).fetchall()
        selected: dict[str, Any] | None = None
        for row in rows:
            decoded = _decode_command_row(row)
            state = decoded["details"].get("state")
            if state in {"queued", "running"}:
                selected = decoded
                break
        if selected is None:
            return None
        run_id = int(selected["id"])
        attempt_number = int(
            connection.execute(
                "SELECT COALESCE(MAX(attempt_number),0)+1 FROM "
                "scheduler_run_attempts WHERE scheduler_run_id=?",
                (run_id,),
            ).fetchone()[0]
        )
        started_at = now_utc()
        first_claimed_at = selected["details"].get("first_claimed_at", started_at)
        if not isinstance(first_claimed_at, str):
            raise ProfileControlError(
                "current_hold_command_corrupt",
                "Current-hold command first-claim time is invalid",
            )
        first_claimed_at = _utc(first_claimed_at)
        details = {
            **selected["details"],
            "state": "running",
            "started_at": started_at,
            "first_claimed_at": first_claimed_at,
            "attempt_number": attempt_number,
        }
        updated = connection.execute(
            "UPDATE scheduler_runs SET status='running',started_at=?,completed_at=NULL,"
            "details_json=? WHERE id=? AND status='interrupted'",
            (started_at, _canonical(details), run_id),
        )
        if updated.rowcount != 1:
            return None
        attempt = connection.execute(
            "INSERT INTO scheduler_run_attempts(scheduler_run_id,attempt_number,"
            "invocation_source,status,started_at,details_json) "
            "VALUES (?,?,'operator_retry','running',?,?)",
            (run_id, attempt_number, started_at, _canonical(details)),
        )
        return {
            "run_id": run_id,
            "attempt_id": int(attempt.lastrowid or 0),
            "binding": selected["binding"],
            "details": details,
        }


def _finish_current_hold_command(
    *,
    db_path: Path,
    claim: Mapping[str, Any],
    status: str,
    result: Mapping[str, Any] | None = None,
    error: Mapping[str, Any] | None = None,
) -> None:
    with connect(db_path) as connection, transaction(connection):
        completed_at = now_utc()
        details = {
            **dict(claim["details"]),
            "state": status,
            "completed_at": completed_at,
            "result": dict(result) if result is not None else None,
            "error": dict(error) if error is not None else None,
        }
        encoded = _canonical(details)
        attempt = connection.execute(
            "UPDATE scheduler_run_attempts SET status=?,completed_at=?,details_json=? "
            "WHERE id=? AND scheduler_run_id=? AND status='running'",
            (
                status,
                completed_at,
                encoded,
                int(claim["attempt_id"]),
                int(claim["run_id"]),
            ),
        )
        run = connection.execute(
            "UPDATE scheduler_runs SET status=?,completed_at=?,details_json=? "
            "WHERE id=? AND status='running'",
            (status, completed_at, encoded, int(claim["run_id"])),
        )
        if attempt.rowcount != 1 or run.rowcount != 1:
            raise ProfileControlError(
                "current_hold_command_finish_failed",
                "Current-hold command terminal update lost its claim",
            )


def process_current_activation_hold_commands(
    *,
    db_path: Path,
    mirror_root: Path | None = None,
    limit: int = 10,
    scheduler: BaseScheduler | None = None,
) -> dict[str, Any]:
    """Claim and execute a bounded batch of durable current-hold commands."""

    if limit < 1 or limit > 10:
        raise ProfileControlError(
            "current_hold_command_limit_invalid", "Control batch limit must be 1-10"
        )
    processed: list[dict[str, Any]] = []
    dispatch: dict[str, Callable[..., dict[str, Any]]] = {
        "hold_begin": begin_current_activation_hold,
        "hold_build_advance": advance_current_activation_hold_build,
        "hold_seal": seal_current_activation_hold,
        "hold_release": release_current_activation_hold,
        "hold_reopen": reopen_current_activation_hold,
    }
    for _index in range(limit):
        claim = _claim_current_hold_command(db_path)
        if claim is None:
            break
        binding = claim["binding"]
        try:
            command = str(binding["command"])
            call_arguments: dict[str, Any] = {
                "db_path": db_path,
                "mirror_root": mirror_root,
                **dict(binding["parameters"]),
            }
            if command == "hold_begin":
                call_arguments["expected_submission_scope"] = binding.get(
                    "submission_scope"
                )
            if command == "hold_build_advance":
                call_arguments["scheduler"] = scheduler
            if command == "capture_release":
                from .capture_release_commands import run_command

                result = run_command(**call_arguments, command_claim=claim)
            elif command == "transport_primary":
                from .transport_runner import run_primary_command

                result = run_primary_command(
                    **call_arguments, command_claim=claim, scheduler=scheduler,
                )
            elif command == "transport_control":
                from .transport_runner import run_control_command

                result = run_control_command(
                    **call_arguments, command_claim=claim, scheduler=scheduler,
                )
            elif command == "forward_only_release":
                from .forward_recovery import run_forward_release_command

                result = run_forward_release_command(
                    **call_arguments, command_claim=claim, scheduler=scheduler,
                )
            else:
                result = dispatch[command](**call_arguments)
        except Exception as exc:
            error = {
                "type": type(exc).__name__,
                "code": getattr(exc, "code", None),
                "message": str(exc),
            }
            _finish_current_hold_command(
                db_path=db_path, claim=claim, status="failed", error=error
            )
            processed.append(
                {"run_id": int(claim["run_id"]), "status": "failed", "error": error}
            )
        else:
            _finish_current_hold_command(
                db_path=db_path, claim=claim, status="succeeded", result=result
            )
            processed.append(
                {"run_id": int(claim["run_id"]), "status": "succeeded"}
            )
    return {"processed": processed, "count": len(processed)}


def execute_current_activation_hold_commands(
    *,
    db_path: Path,
    limit: int = 10,
    now: str | None = None,
    mirror_root: Path | None = None,
) -> dict[str, Any]:
    """Compatibility name for the bounded writer-owned command executor."""

    del now  # Execution uses the writer clock; production clock override is forbidden.
    return process_current_activation_hold_commands(
        db_path=db_path, mirror_root=mirror_root, limit=limit
    )


CROSS_PROFILE_ABORT_CONTRACT = "cross-profile-abort-restore-v1"


def _abort_require_writer(connection: sqlite3.Connection) -> None:
    if not connection.in_transaction or connection.execute("PRAGMA user_version").fetchone()[0] != 20:
        raise ProfileControlError("profile_abort_transaction_required", "Abort requires schema20 and a caller transaction")
    require_current_process_writer_lock(connection)


def _abort_start_watermarks(connection: sqlite3.Connection) -> dict[str, int]:
    return {
        "send_marker_high_watermark": int(connection.execute(
            "SELECT COALESCE(MAX(id),0) FROM paid_provider_dispatch_events WHERE event_type='send_marked'"
        ).fetchone()[0]),
        "network_start_high_watermark": int(connection.execute(
            "SELECT COALESCE(MAX(id),0) FROM provider_request_start_events"
        ).fetchone()[0]),
    }


def read_cross_profile_abort(connection: sqlite3.Connection, drain_id: str) -> dict[str, Any] | None:
    """Read/verify both halves of the phase-one fence; never grants a permit."""
    rows = connection.execute(
        "SELECT * FROM scheduler_runs WHERE job_id=? AND "
        "json_extract(details_json,'$.payload.action')='cross_profile_abort_requested' AND "
        "json_extract(details_json,'$.payload.drain_id')=? ORDER BY id",
        (CURRENT_HOLD_CONTROL_JOB, drain_id),
    ).fetchall()
    if not rows:
        return None
    if len(rows) != 1:
        raise ProfileControlError("profile_abort_fence_invalid", "Abort fence is not unique")
    fence = _read_hold_control_receipt(connection, rows[0])
    payload = fence["payload"]
    target = activation_by_id(connection, int(payload["target_activation_id"]))
    cancellation = target.get("cancellation")
    start = connection.execute("SELECT * FROM pipeline_paid_drain_events WHERE id=?",
                               (payload["start_event_id"],)).fetchone()
    candidate = connection.execute("SELECT * FROM pipeline_paid_drain_events WHERE id=?",
                                   (payload["candidate_event_id"],)).fetchone()
    if (payload.get("contract_version") != CROSS_PROFILE_ABORT_CONTRACT
            or not cancellation or cancellation["contract_version"] != "acquisition-profile-cancellation-v2"
            or cancellation["cancellation_kind"] != payload["cancellation_kind"]
            or cancellation["cancelled_at"] != fence["recorded_at"]
            or cancellation["metadata"].get("abort_fence_run_id") != fence["run_id"]
            or cancellation["metadata"].get("abort_fence_sha256") != fence["self_sha256"]
            or start is None or start["event_type"] != "start" or start["drain_id"] != drain_id
            or start["event_hash"] != payload["start_event_hash"]
            or candidate is None or candidate["event_hash"] != payload["candidate_event_hash"]
            or candidate["drain_id"] != drain_id or candidate["target_activation_id"] != target["activation_id"]):
        raise ProfileControlError("profile_abort_fence_invalid", "Abort fence/cancellation binding differs")
    binding = json.loads(start["payload_json"])["binding"]
    if (binding["source_activation_id"] != payload["source_activation_id"]
            or binding["target_activation_id"] != target["activation_id"]):
        raise ProfileControlError("profile_abort_fence_invalid", "Abort activation binding differs")
    effective = parse_time(target["effective_at"])
    cancelled = parse_time(cancellation["cancelled_at"])
    if payload["cancellation_kind"] == "pre_effective":
        valid = cancelled < effective
    else:
        eligibility = activation_eligibility(connection, target)
        valid = cancelled >= effective and eligibility["required"] and not eligibility["eligible"]
    if not valid or connection.execute(
        "SELECT 1 FROM paid_provider_dispatch_events WHERE activation_id=? AND event_type='send_marked' LIMIT 1",
        (target["activation_id"],),
    ).fetchone():
        raise ProfileControlError("profile_abort_target_was_current", "Target is not provably never-current/never-sent")
    return {"fence": fence, "cancellation": cancellation, "target": target}


def begin_cross_profile_abort_in_transaction(
    connection: sqlite3.Connection, *, command_id: str, drain_id: str,
    actor: str, reason: str, now: str,
) -> dict[str, Any]:
    """Phase one: atomically fence both profiles and cancel the never-current target."""
    from .paid_drain import _validated_profile_chain

    _abort_require_writer(connection)
    timestamp = _utc(now)
    if not _COMMAND_ID.fullmatch(command_id) or not actor.strip() or not reason.strip():
        raise ProfileControlError("profile_abort_command_invalid", "Explicit command, actor and reason are required")
    existing = read_cross_profile_abort(connection, drain_id)
    if existing:
        payload = existing["fence"]["payload"]
        if (payload["command_id"], payload["actor"], payload["reason"]) != (command_id, actor, reason):
            raise ProfileControlError("profile_abort_idempotency_conflict", "Abort command binding changed")
        return {**existing, "idempotent": True}
    events = _validated_profile_chain(connection)
    if not events or events[-1].drain_id != drain_id or events[-1].event_type == "ABORT_RESTORE":
        raise ProfileControlError("profile_abort_not_candidate", "Abort requires the current candidate drain")
    start = next(event for event in events if event.drain_id == drain_id and event.event_type == "start")
    binding = start.payload["binding"]
    target = activation_by_id(connection, start.target_activation_id)
    source = _active(connection, timestamp)
    eligibility = activation_eligibility(connection, target)
    if (start.payload.get("switch_kind") != "cross_profile" or target.get("cancellation")
            or source["activation_id"] != binding["source_activation_id"]):
        raise ProfileControlError("profile_abort_target_was_current", "Only a never-current cross-profile candidate may abort")
    kind = "pre_effective" if parse_time(timestamp) < parse_time(target["effective_at"]) else "never_eligible_cleanup"
    if kind == "never_eligible_cleanup" and (not eligibility["required"] or eligibility["eligible"]):
        raise ProfileControlError("profile_abort_target_was_current", "Effective target was eligible; abort is forbidden")
    if connection.execute("SELECT 1 FROM paid_provider_dispatch_events WHERE activation_id=? AND event_type='send_marked' LIMIT 1",
                          (target["activation_id"],)).fetchone():
        raise ProfileControlError("profile_abort_target_was_current", "Target has a provider send marker")
    payload = {
        "contract_version": CROSS_PROFILE_ABORT_CONTRACT, "action": "cross_profile_abort_requested",
        "command_id": command_id, "drain_id": drain_id, "actor": actor, "reason": reason,
        "source_activation_id": source["activation_id"], "target_activation_id": target["activation_id"],
        "start_event_id": start.event_id, "start_event_hash": start.event_hash,
        "candidate_event_id": events[-1].event_id, "candidate_event_hash": events[-1].event_hash,
        "cancellation_kind": kind, "start_watermarks": _abort_start_watermarks(connection),
    }
    # A nested savepoint also makes failure atomic if an outer caller catches it.
    connection.execute("SAVEPOINT cross_profile_abort_begin")
    try:
        fence = _record_hold_control_receipt(connection, scheduled_for="cross-abort:" + command_id,
                                             payload=payload, timestamp=timestamp)
        cancel_activation(connection, target["activation_id"], cancelled_at=timestamp,
            actor=actor, reason=reason, cancellation_kind=kind,
            metadata={"abort_fence_run_id": fence["run_id"], "abort_fence_sha256": fence["self_sha256"],
                      "command_id": command_id, "drain_id": drain_id})
        verified = read_cross_profile_abort(connection, drain_id)
        connection.execute("RELEASE cross_profile_abort_begin")
    except BaseException:
        connection.execute("ROLLBACK TO cross_profile_abort_begin")
        connection.execute("RELEASE cross_profile_abort_begin")
        raise
    assert verified is not None
    return {**verified, "idempotent": False}


def _abort_source_release(connection: sqlite3.Connection, payload: Mapping[str, Any], *, at: str) -> dict[str, Any]:
    """Fail closed unless the original source full-day release remains qualified."""
    active = _active(connection, at)
    if active["activation_id"] != payload["source_activation_id"]:
        raise ProfileControlError("profile_abort_source_changed", "Abort cannot authorize another current activation")
    release = connection.execute(
        "SELECT * FROM pipeline_paid_drain_events WHERE target_activation_id=? AND event_type='release' "
        "AND id<? ORDER BY id DESC LIMIT 1", (active["activation_id"], payload["start_event_id"]),
    ).fetchone()
    control = json.loads(release["payload_json"]).get("control", {}) if release else {}
    if (control.get("contract_version") != CURRENT_ACTIVATION_HOLD_CONTRACT
            or control.get("control_purpose") != "full_day_release"
            or control.get("activation_id") != active["activation_id"]
            or control.get("roster_snapshot_id") != active["roster_snapshot_id"]
            or control.get("roster_snapshot_hash") != active["roster_members_sha256"]):
        raise ProfileControlError("profile_abort_source_unqualified", "Source has no matching full-day RELEASE")
    verified = validate_current_hold_release_prerequisites(connection, active=active, release_control=control, at=at)
    # Abort is a fresh admission, not an extension of an expired capacity/budget receipt.
    final = control["final_contract"]
    expected = {key: value["artifact_sha256"] for key, value in verified.items()}
    _current_hold_prerequisites(connection, drain_id=control["drain_id"], generation=final["generation"],
                                active=active, expected=expected, at=at)
    from .provider_budget import budget_summary

    budget = budget_summary(connection, at=at)
    if budget["total_microusd"] >= budget["automatic_limit_microusd"]:
        raise ProfileControlError("profile_abort_budget_exhausted", "Conservative charged/unknown budget is exhausted")
    assert release is not None
    return {"event_id": release["id"], "event_hash": release["event_hash"]}


def validate_cross_profile_abort_terminal(connection: sqlite3.Connection, event: Mapping[str, Any]) -> dict[str, Any]:
    pair = read_cross_profile_abort(connection, str(event["drain_id"]))
    if pair is None:
        raise ProfileControlError("profile_abort_fence_invalid", "Abort terminal has no durable fence")
    fence, cancel = pair["fence"], pair["cancellation"]
    terminal = event["payload"]
    if (terminal.get("contract_version") != CROSS_PROFILE_ABORT_CONTRACT
            or terminal.get("fence_run_id") != fence["run_id"] or terminal.get("fence_sha256") != fence["self_sha256"]
            or terminal.get("cancellation_id") != cancel["cancellation_id"]
            or terminal.get("cancellation_sha256") != cancel["cancellation_sha256"]
            or terminal.get("result") not in {"open", "closed"}
            or event["previous_event_id"] != fence["payload"]["candidate_event_id"]
            or event["previous_event_hash"] != fence["payload"]["candidate_event_hash"]):
        raise ProfileControlError("profile_abort_terminal_invalid", "Abort terminal differs from its fence/cancellation")
    return pair


def complete_cross_profile_abort_in_transaction(
    connection: sqlite3.Connection, *, command_id: str, now: str,
) -> dict[str, Any]:
    """Phase two: after external reconciliation, seal the tail and restore or close."""
    from .paid_drain import DrainReceipt, _append_native_profile_event, _time, _validated_profile_chain, _verify_sealable

    _abort_require_writer(connection)
    timestamp = _utc(now)
    row = connection.execute("SELECT * FROM scheduler_runs WHERE job_id=? AND scheduled_for=?",
                             (CURRENT_HOLD_CONTROL_JOB, "cross-abort:" + command_id)).fetchone()
    if row is None:
        raise ProfileControlError("profile_abort_fence_missing", "BEGIN abort must commit before COMPLETE")
    receipt = _read_hold_control_receipt(connection, row)
    pair = read_cross_profile_abort(connection, receipt["payload"]["drain_id"])
    assert pair is not None
    fence, cancellation = pair["fence"], pair["cancellation"]
    payload = fence["payload"]
    events = _validated_profile_chain(connection)
    terminal = next((event for event in events if event.drain_id == payload["drain_id"] and event.event_type == "ABORT_RESTORE"), None)
    if terminal:
        validate_cross_profile_abort_terminal(connection, terminal.as_dict())
        return {"event": terminal.as_dict(), "idempotent": True}
    if (parse_time(timestamp) < parse_time(fence["recorded_at"])
            or events[-1].event_id != payload["candidate_event_id"]
            or _abort_start_watermarks(connection) != payload["start_watermarks"]):
        raise ProfileControlError("profile_abort_fence_changed", "Abort generation or network starts changed")
    for table in ("scheduler_runs", "scheduler_run_attempts", "fetch_slots"):
        if connection.execute(f"SELECT 1 FROM {table} WHERE status='running' LIMIT 1").fetchone():
            raise ProfileControlError("profile_abort_running", "Abort restore requires zero running work")
    if connection.execute("SELECT 1 FROM capture_work_items WHERE state IN ('leased','running') LIMIT 1").fetchone():
        raise ProfileControlError("profile_abort_running", "Abort restore requires zero leased/running capture work")
    unsettled = connection.execute("""SELECT m.id FROM paid_provider_dispatch_events m
        WHERE m.event_type='send_marked' AND m.activation_id IN (?,?) AND (
         NOT EXISTS (SELECT 1 FROM paid_provider_dispatch_events t WHERE t.dispatch_id=m.dispatch_id
          AND t.event_type IN ('succeeded','failed','billing_unknown'))
         OR NOT EXISTS (SELECT 1 FROM provider_usage_settlements s JOIN provider_usage_settlement_events e
          ON e.settlement_id=s.id WHERE s.provider_usage_id=m.provider_usage_id)) LIMIT 1""",
        (payload["source_activation_id"], payload["target_activation_id"])).fetchone()
    if unsettled:
        raise ProfileControlError("profile_abort_unsettled", "Sent requests require terminal dispatch and a settlement")
    start = next(event for event in events if event.event_id == payload["start_event_id"])
    synthetic = DrainReceipt(run_id=start.event_id, attempt_id=start.event_id, drain_id=start.drain_id,
        event_type="start", sequence=1, created_at=start.created_at, event_hash=start.event_hash,
        previous_event_id=start.previous_event_id, previous_event_hash=start.previous_event_hash, payload=start.payload)
    verification = _verify_sealable(connection, synthetic, checked_at=timestamp)
    result, source_release, reason = "closed", None, None
    try:
        source_release = _abort_source_release(connection, payload, at=timestamp)
        result = "open"
    except (ProfileControlError, KeyError, TypeError, ValueError) as exc:
        reason = getattr(exc, "code", "profile_abort_source_unqualified")
    event = _append_native_profile_event(connection, drain_id=start.drain_id,
        target_activation_id=start.target_activation_id, event_type="ABORT_RESTORE", created_at=_time(timestamp),
        payload={"contract_version": CROSS_PROFILE_ABORT_CONTRACT, "fence_run_id": fence["run_id"],
            "fence_sha256": fence["self_sha256"], "cancellation_id": cancellation["cancellation_id"],
            "cancellation_sha256": cancellation["cancellation_sha256"], "result": result, "reason": reason,
            "source_activation_id": payload["source_activation_id"], "source_release": source_release,
            "verification": verification, "start_watermarks": payload["start_watermarks"],
            "terminal_high_watermark": connection.execute("SELECT COALESCE(MAX(id),0) FROM paid_provider_dispatch_events WHERE event_type IN ('succeeded','failed','billing_unknown','not_sent')").fetchone()[0],
            "settlement_high_watermark": connection.execute("SELECT COALESCE(MAX(id),0) FROM provider_usage_settlement_events").fetchone()[0]})
    validate_cross_profile_abort_terminal(connection, event.as_dict())
    return {"event": event.as_dict(), "idempotent": False}


def begin_cross_profile_switch_in_transaction(
    connection: sqlite3.Connection,
    *,
    drain_id: str,
    target_profile_id: str,
    roster_snapshot_id: int,
    build_receipt_sha256: str,
    runtime_root_receipt_sha256: str,
    actor: str,
    reason: str,
    now: str,
    effective_now: bool = False,
    capture_source_operations: Mapping[str, Any] | None = None,
) -> dict[str, Any]:
    """Atomically append the target activation and close current-profile dispatch."""

    if not connection.in_transaction:
        raise ProfileControlError(
            "profile_control_transaction_required", "BEGIN requires a caller transaction"
        )
    _require_schema19(connection)
    if effective_now and connection.execute("PRAGMA user_version").fetchone()[0] == 20:
        raise ProfileControlError("profile_control_effective_now_forbidden", "Schema20 cross-profile switches require a future business-day boundary")
    timestamp = _utc(now)
    local = parse_time(timestamp).astimezone(BEIJING)
    # This untrusted shape only postpones the clock check. The full source
    # validator below must prove the actual user decision before any write.
    approved_future_shape = (
        connection.execute("PRAGMA user_version").fetchone()[0] == 20
        and target_profile_id == INTEGRATED_PROFILE
        and isinstance(capture_source_operations, Mapping)
        and isinstance(capture_source_operations.get("operations"), Mapping)
        and bool(capture_source_operations["operations"])
        and all(isinstance(value, Mapping)
                and value.get("qualification_kind") == "operator_authorized"
                for value in capture_source_operations["operations"].values())
    )
    if not effective_now and not approved_future_shape and local.timetz().replace(tzinfo=None) < BEGIN_EARLIEST:
        raise ProfileControlError(
            "profile_control_begin_too_early", "Cross-profile BEGIN is allowed from 20:00 Beijing"
        )
    build = _sha(build_receipt_sha256, label="build receipt")
    runtime = _sha(runtime_root_receipt_sha256, label="runtime-root receipt")
    existing = connection.execute(
        """SELECT target_activation_id,payload_json FROM pipeline_paid_drain_events
           WHERE drain_id=? AND event_type='start'""",
        (drain_id,),
    ).fetchone()
    if existing is not None:
        activation = activation_by_id(connection, int(existing["target_activation_id"]))
        try:
            existing_binding = json.loads(str(existing["payload_json"]))["binding"]
        except (KeyError, TypeError, ValueError) as exc:
            raise ProfileControlError(
                "profile_control_chain_invalid", "Existing switch receipt is invalid"
            ) from exc
        state = dispatch_state(connection, at=timestamp)
        if (
            state.state == "invalid"
            or activation["profile_id"] != target_profile_id
            or activation["roster_snapshot_id"] != roster_snapshot_id
            or activation["build_receipt_sha256"] != build
            or existing_binding.get("runtime_root_receipt_sha256") != runtime
            or existing_binding.get("planned_effective_at") != activation["effective_at"]
            or activation.get("cancellation") is not None
        ):
            raise ProfileControlError(
                "profile_control_idempotency_conflict", "Existing switch binding differs"
            )
        if capture_source_operations is not None:
            from . import capture_activation_release

            if activation["metadata"].get(capture_activation_release.METADATA_KEY) != dict(capture_source_operations):
                raise ProfileControlError("profile_control_idempotency_conflict", "Existing source qualification snapshot differs")
            capture_activation_release._verify_frozen(connection, capture_source_operations, at=timestamp)
        return {
            "activation": activation,
            "start": None,
            "superseded_activation_ids": activation["metadata"].get(
                "superseded_activation_ids", []
            ),
            "idempotent": True,
        }

    source = _active(connection, timestamp)
    if source["profile_id"] == target_profile_id:
        raise ProfileControlError(
            "profile_control_not_cross_profile", "Cross-profile BEGIN must change profile"
        )
    snapshot = _target_snapshot(
        connection, profile_id=target_profile_id, roster_snapshot_id=roster_snapshot_id
    )
    effective = timestamp if effective_now else _next_midnight(timestamp)
    frozen_capture_source = None
    if capture_source_operations is not None:
        from . import capture_activation_release

        if target_profile_id != INTEGRATED_PROFILE:
            raise ProfileControlError("profile_control_source_qualification_invalid", "Source qualification is only for an integrated target")
        frozen_capture_source = capture_activation_release.validate_source_snapshot(
            connection, capture_source_operations, at=timestamp,
        )
        if approved_future_shape:
            frozen_source = frozen_capture_source["source_active"]
            if (source["profile_id"] != TIKHUB_PROFILE
                    or frozen_source["activation_id"] != source["activation_id"]
                    or frozen_source["roster_snapshot_id"] != roster_snapshot_id
                    or frozen_source["roster_members_sha256"] != snapshot["members_sha256"]
                    or not all(value.get("qualification_kind") == "operator_authorized"
                               for value in frozen_capture_source["operations"].values())):
                raise ProfileControlError("profile_control_source_qualification_invalid",
                    "Approved future switch requires the same validated Mode B roster")
            # Never effective-now or a backdated business-day receipt. Existing
            # COMPLETE and eligibility checks still require pre-effective seal.
            effective = _utc((parse_time(timestamp) + timedelta(minutes=15)).isoformat())
        if (frozen_capture_source["runtime_bindings"]["build_sha256"] != build
                or frozen_capture_source["runtime_bindings"]["runtime_sha256"] != runtime
                or any(parse_time(value["expires_at"]) <= parse_time(effective)
                       for value in frozen_capture_source["operations"].values())):
            raise ProfileControlError("profile_control_source_qualification_invalid", "Source qualification runtime differs or expires before target activation")
    superseded_activation_ids: list[int] = []
    if effective_now:
        future = connection.execute(
            """SELECT a.id FROM acquisition_profile_activations a
               WHERE julianday(a.effective_at)>julianday(?) AND NOT EXISTS (
                   SELECT 1 FROM activation_cancellations c WHERE c.activation_id=a.id
               ) ORDER BY a.effective_at,a.id""",
            (timestamp,),
        ).fetchall()
        for row in future:
            activation_id = int(row["id"])
            cancel_activation(
                connection,
                activation_id,
                cancelled_at=timestamp,
                actor=actor,
                reason="superseded by immediate cross-profile switch",
                metadata={"drain_id": drain_id, "replacement_profile": target_profile_id},
            )
            superseded_activation_ids.append(activation_id)
    metadata: dict[str, Any] = {
        "contract_version": (
            IMMEDIATE_CONTROL_CONTRACT if effective_now else CONTROL_CONTRACT
        ),
        "switch_kind": "cross_profile",
        "drain_id": drain_id,
        "effective_mode": "approved_future_boundary" if approved_future_shape else "immediate" if effective_now else "next_midnight",
        "superseded_activation_ids": superseded_activation_ids,
    }
    if connection.execute("PRAGMA user_version").fetchone()[0] == 20:
        metadata["eligibility_contract"] = ELIGIBILITY_CONTRACT
    if frozen_capture_source is not None:
        metadata["capture_operation_source"] = frozen_capture_source
    scheduled = connection.execute(
        """SELECT a.id FROM acquisition_profile_activations a
           WHERE a.effective_at=? AND NOT EXISTS (
               SELECT 1 FROM activation_cancellations c WHERE c.activation_id=a.id
           )""",
        (effective,),
    ).fetchone()
    if scheduled is not None:
        occupied = activation_by_id(connection, int(scheduled["id"]))
        if occupied["profile_id"] == source["profile_id"]:
            activation = replace_scheduled_activation(
                connection,
                int(occupied["activation_id"]),
                profile_id=target_profile_id,
                roster_snapshot_id=roster_snapshot_id,
                roster_members_sha256=snapshot["members_sha256"],
                effective_at=effective,
                build_receipt_sha256=build,
                actor=actor,
                reason=reason,
                metadata=metadata,
                created_at=timestamp,
            )
        elif (
            occupied["profile_id"] == target_profile_id
            and occupied["roster_snapshot_id"] == roster_snapshot_id
            and occupied["build_receipt_sha256"] == build
        ):
            if (frozen_capture_source is not None
                    and occupied["metadata"].get("capture_operation_source") != frozen_capture_source):
                raise ProfileControlError("profile_control_schedule_conflict", "Existing target has another source qualification snapshot")
            activation = occupied
        else:
            raise ProfileControlError(
                "profile_control_schedule_conflict",
                "Another cross-profile activation occupies next midnight",
            )
    else:
        activation = append_activation(
            connection,
            profile_id=target_profile_id,
            roster_snapshot_id=roster_snapshot_id,
            roster_members_sha256=snapshot["members_sha256"],
            effective_at=effective,
            build_receipt_sha256=build,
            actor=actor,
            reason=reason,
            metadata=metadata,
            created_at=timestamp,
        )
    binding = {
        "source_activation_id": int(source["activation_id"]),
        "target_activation_id": int(activation["activation_id"]),
        "business_day": local.date().isoformat(),
        "planned_effective_at": effective,
        "build_receipt_sha256": build,
        "runtime_root_receipt_sha256": runtime,
    }
    try:
        start = start_profile_drain_in_transaction(
            connection,
            drain_id,
            binding=binding,
            switch_kind="cross_profile",
            now=timestamp,
        )
    except PaidDrainError as exc:
        raise ProfileControlError("profile_control_begin_failed", str(exc)) from exc
    return {
        "activation": activation,
        "start": start.as_dict(),
        "superseded_activation_ids": superseded_activation_ids,
        "idempotent": False,
    }


def begin_cross_profile_switch(
    *,
    db_path: Path,
    drain_id: str,
    target_profile_id: str,
    roster_snapshot_id: int,
    build_receipt_sha256: str,
    runtime_root_receipt_sha256: str,
    actor: str,
    reason: str,
    now: str | None = None,
    effective_now: bool = False,
    capture_source_operations: Mapping[str, Any] | None = None,
) -> dict[str, Any]:
    with connect(db_path) as connection, transaction(connection):
        return begin_cross_profile_switch_in_transaction(
            connection,
            drain_id=drain_id,
            target_profile_id=target_profile_id,
            roster_snapshot_id=roster_snapshot_id,
            build_receipt_sha256=build_receipt_sha256,
            runtime_root_receipt_sha256=runtime_root_receipt_sha256,
            actor=actor,
            reason=reason,
            now=_utc(now),
            effective_now=effective_now,
            capture_source_operations=capture_source_operations,
        )


def _schedule_same_profile_roster_activation_in_transaction(
    connection: sqlite3.Connection,
    *,
    roster_snapshot_id: int,
    build_receipt_sha256: str,
    runtime_root_receipt_sha256: str,
    actor: str,
    reason: str,
    timestamp: str,
) -> dict[str, Any]:
    if not connection.in_transaction:
        raise ProfileControlError(
            "profile_control_transaction_required",
            "Same-profile roster scheduling requires a caller transaction",
        )

    build = _sha(build_receipt_sha256, label="build receipt")
    runtime = _sha(runtime_root_receipt_sha256, label="runtime-root receipt")
    _require_schema19(connection)
    source = _active(connection, timestamp)
    snapshot = _target_snapshot(
        connection,
        profile_id=str(source["profile_id"]),
        roster_snapshot_id=roster_snapshot_id,
    )
    effective = _next_midnight(timestamp)
    scheduled = connection.execute(
        """SELECT a.* FROM acquisition_profile_activations a
           WHERE a.effective_at=? AND NOT EXISTS (
               SELECT 1 FROM activation_cancellations c WHERE c.activation_id=a.id
           )""",
        (effective,),
    ).fetchone()
    activation_metadata = {
        "contract_version": CONTROL_CONTRACT,
        "switch_kind": "same_profile",
    }
    if scheduled is not None:
        scheduled_value = activation_by_id(connection, int(scheduled["id"]))
        if scheduled_value["profile_id"] != source["profile_id"]:
            raise ProfileControlError(
                "profile_control_cross_switch_pending",
                "A cross-profile activation already occupies next midnight",
            )
        if (
            scheduled_value["roster_snapshot_id"] == roster_snapshot_id
            and scheduled_value["build_receipt_sha256"] == build
        ):
            permit = connection.execute(
                """SELECT id FROM pipeline_paid_drain_events
                   WHERE target_activation_id=? AND event_type='release'
                   ORDER BY id DESC LIMIT 1""",
                (scheduled_value["activation_id"],),
            ).fetchone()
            if permit is None:
                raise ProfileControlError(
                    "profile_control_permit_missing",
                    "Existing scheduled activation has no RELEASE permit",
                )
            return {
                "activation": scheduled_value,
                "release": {"event_id": int(permit["id"])},
                "drain_id": f"same-profile:{scheduled_value['activation_id']}",
                "idempotent": True,
            }
        activation = replace_scheduled_activation(
            connection,
            int(scheduled_value["activation_id"]),
            profile_id=str(source["profile_id"]),
            roster_snapshot_id=roster_snapshot_id,
            roster_members_sha256=snapshot["members_sha256"],
            effective_at=effective,
            build_receipt_sha256=build,
            actor=actor,
            reason=reason,
            metadata=activation_metadata,
            created_at=timestamp,
        )
    else:
        activation = append_activation(
            connection,
            profile_id=str(source["profile_id"]),
            roster_snapshot_id=roster_snapshot_id,
            roster_members_sha256=snapshot["members_sha256"],
            effective_at=effective,
            build_receipt_sha256=build,
            actor=actor,
            reason=reason,
            metadata=activation_metadata,
            created_at=timestamp,
        )
    drain_id = f"same-profile:{activation['activation_id']}"
    try:
        release = issue_activation_permit_in_transaction(
            connection,
            activation_id=int(activation["activation_id"]),
            drain_id=drain_id,
            source_activation_id=int(source["activation_id"]),
            business_day=parse_time(timestamp).astimezone(BEIJING).date().isoformat(),
            planned_effective_at=effective,
            build_receipt_sha256=build,
            runtime_root_receipt_sha256=runtime,
            now=timestamp,
        )
    except PaidDrainError as exc:
        raise ProfileControlError("profile_control_permit_failed", str(exc)) from exc
    return {
        "activation": activation,
        "release": release.as_dict(),
        "drain_id": drain_id,
        "idempotent": False,
    }


def schedule_same_profile_roster_activation(
    *,
    db_path: Path,
    roster_snapshot_id: int,
    build_receipt_sha256: str,
    runtime_root_receipt_sha256: str,
    actor: str,
    reason: str,
    now: str | None = None,
) -> dict[str, Any]:
    """Writer-owned next-midnight roster schedule with a nonblocking permit."""

    with connect(db_path) as connection, transaction(connection):
        return _schedule_same_profile_roster_activation_in_transaction(
            connection,
            roster_snapshot_id=roster_snapshot_id,
            build_receipt_sha256=build_receipt_sha256,
            runtime_root_receipt_sha256=runtime_root_receipt_sha256,
            actor=actor,
            reason=reason,
            timestamp=_utc(now),
        )


def _active_runtime_receipts(
    connection: sqlite3.Connection,
    *,
    source: Mapping[str, Any],
    timestamp: str,
) -> tuple[str, str]:
    state = dispatch_state(connection, at=timestamp)
    if (
        state.state != "open"
        or state.activation_id != int(source["activation_id"])
        or state.permit_event_id is None
    ):
        raise ProfileControlError(
            "profile_control_dispatch_not_open",
            "The active profile has no open activation permit",
        )
    permit = connection.execute(
        "SELECT drain_id FROM pipeline_paid_drain_events WHERE id=? AND event_type='release'",
        (state.permit_event_id,),
    ).fetchone()
    if permit is None:
        raise ProfileControlError(
            "profile_control_permit_missing", "The active RELEASE permit is missing"
        )
    start = connection.execute(
        """SELECT payload_json FROM pipeline_paid_drain_events
           WHERE drain_id=? AND target_activation_id=? AND event_type='start'""",
        (permit["drain_id"], source["activation_id"]),
    ).fetchone()
    try:
        binding = json.loads(str(start["payload_json"]))["binding"]
    except (KeyError, TypeError, ValueError) as exc:
        raise ProfileControlError(
            "profile_control_permit_invalid", "The active permit binding is invalid"
        ) from exc
    if not isinstance(binding, dict):
        raise ProfileControlError(
            "profile_control_permit_invalid", "The active permit binding is invalid"
        )
    build = _sha(str(source["build_receipt_sha256"]), label="build receipt")
    if binding.get("build_receipt_sha256") != build:
        raise ProfileControlError(
            "profile_control_permit_invalid",
            "The active permit does not bind the active build",
        )
    runtime = _sha(
        str(binding.get("runtime_root_receipt_sha256")),
        label="runtime-root receipt",
    )
    return build, runtime


def schedule_accepted_roster_activation_in_transaction(
    connection: sqlite3.Connection,
    *,
    roster_snapshot_id: int,
    actor: str,
    reason: str,
    now: str | None = None,
) -> dict[str, Any]:
    """Schedule an accepted roster only when its family is already active."""

    if not connection.in_transaction:
        raise ProfileControlError(
            "profile_control_transaction_required",
            "Accepted-roster scheduling requires a caller transaction",
        )
    timestamp = _utc(now)
    _require_schema19(connection)
    source = _active(connection, timestamp)
    try:
        snapshot = snapshot_by_id(connection, roster_snapshot_id)
    except Exception as exc:
        raise ProfileControlError(
            "profile_control_roster_missing", "Accepted roster snapshot does not exist"
        ) from exc
    if snapshot["source_family"] != PROFILE_FAMILIES[str(source["profile_id"])]:
        return {
            "scheduled": False,
            "reason": "profile_family_inactive",
            "roster_snapshot_id": roster_snapshot_id,
        }
    build, runtime = _active_runtime_receipts(
        connection, source=source, timestamp=timestamp
    )
    scheduled = _schedule_same_profile_roster_activation_in_transaction(
        connection,
        roster_snapshot_id=roster_snapshot_id,
        build_receipt_sha256=build,
        runtime_root_receipt_sha256=runtime,
        actor=actor,
        reason=reason,
        timestamp=timestamp,
    )
    return {**scheduled, "scheduled": True}


def _cross_start(connection: sqlite3.Connection, drain_id: str) -> sqlite3.Row:
    row = connection.execute(
        """SELECT * FROM pipeline_paid_drain_events
           WHERE drain_id=? AND event_type='start'""",
        (drain_id,),
    ).fetchone()
    if row is None:
        raise ProfileControlError("profile_control_switch_missing", "Cross-profile START is missing")
    payload = json.loads(str(row["payload_json"]))
    if payload.get("switch_kind") != "cross_profile":
        raise ProfileControlError(
            "profile_control_switch_kind_invalid", "Drain is not a cross-profile switch"
        )
    return row


def _write_or_verify_mirror(receipt: Any, root: Path) -> None:
    resolved_root = Path(root).expanduser()
    if not resolved_root.is_absolute() or resolved_root.is_symlink():
        raise ProfileControlError(
            "profile_control_mirror_invalid", "RELEASE mirror root is unsafe"
        )
    target = resolved_root / f"{receipt.drain_id}.{receipt.event_type}.json"
    if target.exists() and not target.is_symlink():
        root_state = resolved_root.stat()
        target_state = target.stat()
        if (
            not stat.S_ISDIR(root_state.st_mode)
            or root_state.st_uid != os.geteuid()
            or stat.S_IMODE(root_state.st_mode) & 0o077
            or not stat.S_ISREG(target_state.st_mode)
            or target_state.st_uid != os.geteuid()
            or stat.S_IMODE(target_state.st_mode) & 0o077
            or target_state.st_nlink != 1
        ):
            raise ProfileControlError(
                "profile_control_mirror_invalid", "Existing RELEASE mirror is unsafe"
            )
        try:
            existing = json.loads(target.read_text(encoding="utf-8"))
        except (OSError, TypeError, ValueError) as exc:
            raise ProfileControlError(
                "profile_control_mirror_invalid", "Existing RELEASE mirror is invalid"
            ) from exc
        if existing.get("event_hash") != receipt.event_hash:
            raise ProfileControlError(
                "profile_control_mirror_conflict", "Existing RELEASE mirror differs"
            )
        return
    write_audit_mirror(receipt, resolved_root)  # type: ignore[arg-type]


def _event_public(event: Mapping[str, Any]) -> dict[str, Any]:
    return {
        "event_id": int(event["event_id"]),
        "target_activation_id": int(event["target_activation_id"]),
        "drain_id": str(event["drain_id"]),
        "event_type": str(event["event_type"]),
        "sequence": int(event["sequence"]),
        "created_at": str(event["created_at"]),
        "event_hash": str(event["event_hash"]),
        "previous_event_id": event.get("previous_event_id"),
        "previous_event_hash": event.get("previous_event_hash"),
        "payload": json.loads(_canonical(event["payload"])),
    }


def _write_private_json_mirror(
    *, root: Path, filename: str, payload: Mapping[str, Any]
) -> Path:
    resolved_root = Path(root).expanduser()
    if not resolved_root.is_absolute() or resolved_root.is_symlink():
        raise ProfileControlError(
            "current_hold_mirror_invalid", "Current-hold mirror root is unsafe"
        )
    resolved_root.mkdir(parents=True, exist_ok=True, mode=0o700)
    root_state = resolved_root.stat()
    if (
        not stat.S_ISDIR(root_state.st_mode)
        or root_state.st_uid != os.geteuid()
        or stat.S_IMODE(root_state.st_mode) & 0o077
    ):
        raise ProfileControlError(
            "current_hold_mirror_invalid",
            "Current-hold mirror root must be private and user-owned",
        )
    target = resolved_root / filename
    body = (_canonical(dict(payload)) + "\n").encode("utf-8")
    flags = os.O_WRONLY | os.O_CREAT | os.O_EXCL
    if hasattr(os, "O_CLOEXEC"):
        flags |= os.O_CLOEXEC
    if hasattr(os, "O_NOFOLLOW"):
        flags |= os.O_NOFOLLOW
    try:
        descriptor = os.open(target, flags, 0o600)
    except FileExistsError:
        if target.is_symlink() or not target.is_file() or target.read_bytes() != body:
            raise ProfileControlError(
                "current_hold_mirror_conflict", "Existing current-hold mirror differs"
            )
        return target
    try:
        offset = 0
        while offset < len(body):
            written = os.write(descriptor, body[offset:])
            if written <= 0:
                raise ProfileControlError(
                    "current_hold_mirror_failed", "Current-hold mirror write was truncated"
                )
            offset += written
        os.fsync(descriptor)
    finally:
        os.close(descriptor)
    directory = os.open(resolved_root, os.O_RDONLY)
    try:
        os.fsync(directory)
    finally:
        os.close(directory)
    target_state = target.stat()
    if (
        not stat.S_ISREG(target_state.st_mode)
        or target_state.st_uid != os.geteuid()
        or stat.S_IMODE(target_state.st_mode) & 0o077
        or target_state.st_nlink != 1
    ):
        raise ProfileControlError(
            "current_hold_mirror_invalid", "Current-hold mirror is not private"
        )
    return target


def _write_native_event_mirror(event: Mapping[str, Any], root: Path) -> Path:
    public = _event_public(event)
    return _write_private_json_mirror(
        root=root,
        filename=(
            f"current-hold.{public['drain_id']}.{public['event_type']}."
            f"{public['event_hash']}.json"
        ),
        payload=public,
    )


def _write_control_receipt_mirror(
    receipt: Mapping[str, Any], root: Path
) -> Path:
    digest = str(receipt.get("self_sha256") or "")
    if _SHA256.fullmatch(digest) is None:
        raise ProfileControlError(
            "current_hold_control_invalid", "Current-hold control receipt hash is invalid"
        )
    return _write_private_json_mirror(
        root=root,
        filename=f"current-hold.control.{digest}.json",
        payload=receipt,
    )


def complete_cross_profile_switch(
    *,
    db_path: Path,
    drain_id: str,
    now: str | None = None,
    mirror_root: Path | None = None,
    stale_after_seconds: int = 600,
    command_claim: Mapping[str, Any] | None = None,
) -> dict[str, Any]:
    """Recover the stopped writer's tail, prove it empty, then SEALED+RELEASE."""

    timestamp = _utc(now)
    with connect(db_path) as connection:
        _require_schema19(connection)
        if command_claim is not None:
            from .capture_release_commands import _current_command

            _current_command(connection, command_claim, {"action": "integrated_complete", "drain_id": drain_id})
        initial_start = _cross_start(connection, drain_id)
        initial_target = activation_by_id(
            connection, int(initial_start["target_activation_id"])
        )
        if (connection.execute("PRAGMA user_version").fetchone()[0] == 20
                and parse_time(timestamp) >= parse_time(initial_target["effective_at"])):
            raise ProfileControlError("profile_control_eligibility_window_closed", "Late COMPLETE cannot backdate target eligibility; use explicit abort")
        if initial_target.get("cancellation") is not None:
            raise ProfileControlError(
                "profile_control_target_cancelled",
                "Cancelled activation cannot be released",
            )
        initial_state = dispatch_state(connection, at=timestamp)
        if initial_state.state == "invalid":
            raise ProfileControlError(
                "profile_control_chain_invalid",
                initial_state.reason or "Paid drain chain is invalid",
            )
        try:
            initial_payload = json.loads(str(initial_start["payload_json"]))
            planned_effective = parse_time(
                initial_payload["binding"]["planned_effective_at"]
            )
        except (KeyError, TypeError, ValueError) as exc:
            raise ProfileControlError(
                "profile_control_chain_invalid", "Cross-profile START binding is invalid"
            ) from exc
    earliest = planned_effective - timedelta(hours=2, minutes=30)
    if parse_time(timestamp) < earliest:
        raise ProfileControlError(
            "profile_control_complete_too_early",
            "Cross-profile COMPLETE is allowed from 21:30 Beijing",
        )
    current_time = parse_time(timestamp).astimezone(timezone.utc)
    if command_claim is None:
        fetch_recovery = recover_stale_fetch_slots(
            db_path=db_path,
            stale_after_seconds=stale_after_seconds,
            current_time=current_time,
        )
        scheduler_recovery = recover_interrupted_scheduler_runs(db_path=db_path)
    else:
        # This runs inside the live Writer. Startup recovery would interrupt
        # real in-flight owners (including this command), not drain them.
        fetch_recovery = {"stale_candidates": 0, "recovered": 0}
        scheduler_recovery = 0
    with connect(db_path) as connection, transaction(connection):
        _require_schema19(connection)
        if command_claim is not None:
            _current_command(connection, command_claim, {"action": "integrated_complete", "drain_id": drain_id})
        start = _cross_start(connection, drain_id)
        target = activation_by_id(connection, int(start["target_activation_id"]))
        if target.get("cancellation") is not None:
            raise ProfileControlError(
                "profile_control_target_cancelled", "Cancelled activation cannot be released"
            )
        running_slots = [
            int(row["id"])
            for row in connection.execute(
                "SELECT id FROM fetch_slots WHERE status='running' ORDER BY id"
            )
        ]
        self_run = int(command_claim["run_id"]) if command_claim is not None else -1
        self_attempt = int(command_claim["attempt_id"]) if command_claim is not None else -1
        running_attempts = [
            {"run_id": int(row["run_id"]), "attempt_id": int(row["attempt_id"])}
            for row in connection.execute(
                """SELECT r.id run_id,a.id attempt_id
                   FROM scheduler_runs r JOIN scheduler_run_attempts a
                     ON a.scheduler_run_id=r.id
                   WHERE (r.status='running' AND r.id<>?) OR (a.status='running' AND a.id<>?)
                   ORDER BY r.id,a.id""", (self_run, self_attempt)
            )
        ]
        if running_slots or running_attempts:
            raise ProfileControlError(
                "profile_control_running_work",
                f"Running provider work remains: slots={running_slots}, attempts={running_attempts}",
            )
        try:
            sealed = seal_profile_drain_in_transaction(connection, drain_id, now=timestamp)
            released = release_profile_drain_in_transaction(connection, drain_id, now=timestamp)
            if mirror_root is not None:
                _write_or_verify_mirror(released, mirror_root)
        except PaidDrainError as exc:
            raise ProfileControlError("profile_control_complete_failed", str(exc)) from exc
        return {
            "activation": target,
            "sealed": sealed.as_dict(),
            "release": released.as_dict(),
            "fetch_recovery": fetch_recovery,
            "scheduler_recovery": scheduler_recovery,
        }


def _access(arguments: argparse.Namespace) -> ResolvedDatabaseAccess:
    if arguments.isolated:
        return resolve_isolated_candidate(arguments.db)
    if arguments.project_root is None:
        raise ProfileControlError(
            "formal_database_identity_unresolved", "Formal mutation requires --project-root"
        )
    try:
        return resolve_installed_database_access(
            DatabaseAccessMode.FORMAL_MUTATION,
            database=arguments.db,
            project_root=arguments.project_root,
        )
    except RuntimeDatabaseError as exc:
        raise ProfileControlError("formal_database_identity_unresolved", str(exc)) from exc


def _parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--db", type=Path, default=DEFAULT_DB)
    parser.add_argument("--project-root", type=Path, default=PROJECT_ROOT)
    parser.add_argument("--isolated", action="store_true")
    parser.add_argument("--at", help="isolated fixtures only")
    commands = parser.add_subparsers(dest="command", required=True)
    begin = commands.add_parser("begin")
    begin.add_argument("--drain-id", required=True)
    begin.add_argument("--target-profile", choices=tuple(PROFILE_FAMILIES), required=True)
    begin.add_argument("--roster-snapshot-id", type=int, required=True)
    begin.add_argument("--build-receipt-sha256", required=True)
    begin.add_argument("--runtime-root-receipt-sha256", required=True)
    begin.add_argument("--actor", required=True)
    begin.add_argument("--reason", required=True)
    begin.add_argument(
        "--effective-now",
        action="store_true",
        help="activate immediately instead of scheduling the next Beijing midnight",
    )
    complete = commands.add_parser("complete")
    complete.add_argument("--drain-id", required=True)
    complete.add_argument("--mirror-root", type=Path)
    return parser


def main(argv: Sequence[str] | None = None) -> int:
    arguments = _parser().parse_args(argv)
    access = _access(arguments)
    if not arguments.isolated and arguments.at is not None:
        raise ProfileControlError(
            "profile_control_clock_override", "Formal profile control cannot override the clock"
        )
    if (
        not arguments.isolated
        and arguments.command == "begin"
        and bool(getattr(arguments, "effective_now", False))
    ):
        raise ProfileControlError(
            "profile_control_effective_now_forbidden",
            "Formal profile control permanently forbids --effective-now",
        )
    mirror_argument = getattr(arguments, "mirror_root", None)
    if not arguments.isolated and arguments.command == "complete" and mirror_argument is None:
        raise ProfileControlError(
            "profile_control_mirror_required", "Formal COMPLETE requires --mirror-root"
        )
    if not arguments.isolated and mirror_argument is not None:
        mirror = mirror_argument.expanduser().resolve(strict=False)
        assert access.project_root is not None
        if mirror == access.project_root or access.project_root in mirror.parents:
            raise ProfileControlError(
                "profile_control_mirror_unsafe",
                "Formal RELEASE mirror must stay outside the installed project",
            )
    lock = nullcontext() if arguments.isolated else acquire_writer_lock(access)
    with lock:
        if arguments.command == "begin":
            result = begin_cross_profile_switch(
                db_path=access.database,
                drain_id=arguments.drain_id,
                target_profile_id=arguments.target_profile,
                roster_snapshot_id=arguments.roster_snapshot_id,
                build_receipt_sha256=arguments.build_receipt_sha256,
                runtime_root_receipt_sha256=arguments.runtime_root_receipt_sha256,
                actor=arguments.actor,
                reason=arguments.reason,
                now=arguments.at,
                effective_now=arguments.effective_now,
            )
        else:
            result = complete_cross_profile_switch(
                db_path=access.database,
                drain_id=arguments.drain_id,
                now=arguments.at,
                mirror_root=arguments.mirror_root,
            )
    print(json.dumps(result, ensure_ascii=False, sort_keys=True, default=str))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
