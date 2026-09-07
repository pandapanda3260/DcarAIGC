"""Shared TikHub dispatch scope and conservative Beijing-day monetary ledger."""

from __future__ import annotations

import hashlib
import json
import math
import sqlite3
import threading
import uuid
from contextlib import contextmanager
from contextvars import ContextVar
from dataclasses import asdict, dataclass, replace
from datetime import date, datetime, timedelta, timezone
from decimal import Decimal, InvalidOperation
from pathlib import Path
from typing import Any, Iterable, Iterator, Mapping
from zoneinfo import ZoneInfo

from .account_roster import RosterError, current_snapshot, require_active_member
from .storage import connect, now_utc, transaction

POLICY_VERSION = "tikhub-global-budget-v3"
DEFAULT_TASK_MAX_AMOUNT_USD = 100.0
TASK_BUDGET_VERSION = "v2"
GLOBAL_MICROUSD = 100_000_000
AUTOMATIC_MICROUSD = 50_000_000
BUDGET_BUCKET_MICROUSD = {
    "discovery": 30_000_000,
    "metrics": 15_000_000,
    "repair": 0,
}
CATEGORY_MICROUSD = {
    # Compatibility categories retained for schema-19 queue receipts.  The
    # authoritative spend gates are the aggregate buckets above.
    "reconcile": 30_000_000, "detail": 15_000_000,
    "metrics": 15_000_000, "comments": 15_000_000,
    "history": 0,
}
PRICES_MICROUSD = {
    "douyin_uid_profile": 1000, "douyin_user_posts": 1000,
    "douyin_video_detail": 1000, "douyin_video_statistics": 1000,
    "douyin_video_comments": 1000,
    "douyin_video_high_quality_play_url": 5000,
    "xiaohongshu_user_posts": 10000, "xiaohongshu_note_detail": 10000,
    "xiaohongshu_note_statistics": 10000, "xiaohongshu_note_comments": 10000,
}
SHANGHAI = ZoneInfo("Asia/Shanghai")
TIKHUB_NETWORK_SLOTS = threading.BoundedSemaphore(4)
BORROW_PRIORITY = ("metrics", "reconcile", "detail", "comments", "history")
INVENTORY_JOB = "provider_queue_inventory:tikhub:"
CLOSEOUT_JOB = "provider_budget_closeout:tikhub:"
PROBE_JOB = "provider_circuit_probe:tikhub"
INCIDENT_AUTH_JOB = "provider_budget_incident:tikhub"
COMPENSATION_AUTH_JOB = "provider_compensation_authorization:tikhub"
COMPENSATION_USE_JOB = "provider_compensation_consumption:tikhub:"
COMPENSATION_GAP_JOB = "provider_compensation_gap:tikhub"
SETTLEMENT_TERMINAL_JOB = "provider_settlement_terminal:tikhub"
FAULT_JOB_PREFIX = "provider_fault_v2"
TRANSPORT_CANDIDATE_JOB = "provider_transport_candidate:tikhub:"
TRANSPORT_BASELINE_JOB = "provider_transport_baseline:tikhub:"
OPERATION_RECOVERY_JOB = "provider_operation_recovery:tikhub:"
STORAGE_RECOVERY_JOB = "provider_storage_recovery:all"
FAULT_SCOPE_KINDS = frozenset(
    {
        "provider_hard",
        "operation",
        "storage_hard",
        "authorization_hard",
        "paid_identity_hold",
    }
)
PROBEABLE_PROVIDER_FAULTS = frozenset(
    {"balance", "application_auth", "provider_outage", "global_quota"}
)
DISCOVERY_OPERATIONS = frozenset(
    {"douyin_uid_profile", "douyin_user_posts", "xiaohongshu_user_posts"}
)


class BudgetBlocked(RuntimeError):
    error_code = "budget_blocked"


class PaidScopeBlocked(BudgetBlocked):
    def __init__(self, code: str, message: str):
        super().__init__(message)
        self.error_code = code


@dataclass(frozen=True)
class PaidScope:
    purpose: str | None = None
    activation_id: int | None = None
    roster_snapshot_id: int | None = None
    roster_snapshot_hash: str | None = None
    scheduler_run_id: int | None = None
    scheduler_attempt_id: int | None = None
    identity_id: int | None = None
    account_id: int | None = None
    content_id: int | None = None
    category: str | None = None
    uid: str | None = None
    platform: str | None = None
    scheduler_owner_token: str | None = None
    scheduler_scan_id: str | None = None
    recovery_probe_id: int | None = None
    incident_authorization_id: int | None = None
    compensation_authorization_id: int | None = None
    paid_scope_identity: str | None = None
    paid_sequence: int = 0
    business_day: str | None = None


_SCOPE: ContextVar[PaidScope] = ContextVar("tikhub_paid_scope", default=PaidScope())
_DEFER_RECOVERY_COMPLETION: ContextVar[bool] = ContextVar(
    "tikhub_defer_recovery_completion", default=False
)


@contextmanager
def paid_scope(
    purpose: str,
    *,
    activation_id: int | None = None,
    roster_snapshot_id: int | None = None,
    roster_snapshot_hash: str | None = None,
    scheduler_run_id: int | None = None,
    scheduler_attempt_id: int | None = None,
    business_day: str | None = None,
    incident_authorization_id: int | None = None,
    compensation_authorization_id: int | None = None,
) -> Iterator[PaidScope]:
    if purpose not in CATEGORY_MICROUSD:
        raise PaidScopeBlocked("unknown_paid_purpose", "Paid purpose is not in the fixed policy")
    parent = _SCOPE.get()
    if business_day is not None:
        try:
            if date.fromisoformat(business_day).isoformat() != business_day:
                raise ValueError
        except (TypeError, ValueError) as error:
            raise PaidScopeBlocked(
                "invalid_business_day", "Paid business day must be YYYY-MM-DD"
            ) from error
    if parent.business_day is not None and business_day not in (
        None,
        parent.business_day,
    ):
        raise PaidScopeBlocked(
            "paid_scope_mismatch", "Nested paid work changed the frozen business day"
        )
    run_id = scheduler_run_id if scheduler_run_id is not None else parent.scheduler_run_id
    attempt_id = scheduler_attempt_id if scheduler_attempt_id is not None else parent.scheduler_attempt_id
    same_owner = (run_id, attempt_id) == (parent.scheduler_run_id, parent.scheduler_attempt_id)
    value = replace(
        parent, purpose="history" if parent.purpose == "history" else purpose,
        activation_id=activation_id if activation_id is not None else parent.activation_id,
        roster_snapshot_id=roster_snapshot_id if roster_snapshot_id is not None else parent.roster_snapshot_id,
        roster_snapshot_hash=roster_snapshot_hash if roster_snapshot_hash is not None else parent.roster_snapshot_hash,
        scheduler_run_id=run_id, scheduler_attempt_id=attempt_id,
        scheduler_owner_token=parent.scheduler_owner_token if same_owner else None,
        scheduler_scan_id=parent.scheduler_scan_id if same_owner else None,
        incident_authorization_id=(
            incident_authorization_id
            if incident_authorization_id is not None
            else parent.incident_authorization_id
        ),
        compensation_authorization_id=(
            compensation_authorization_id
            if compensation_authorization_id is not None
            else parent.compensation_authorization_id
        ),
        business_day=parent.business_day or business_day,
    )
    token = _SCOPE.set(value)
    try:
        yield value
    finally:
        _SCOPE.reset(token)


@contextmanager
def paid_dispatch_owner(
    *,
    job_id: str,
    identity: Mapping[str, Any],
    db_path: Path,
    at: str | None = None,
) -> Iterator[PaidScope]:
    """Give a direct schema-19 paid call a one-shot durable owner.

    Scheduled pipeline work already carries a durable owner and is left
    untouched. This compatibility boundary covers explicit/manual callers and
    maintenance tools so the request ledger never needs nullable ownership.
    """

    current = _SCOPE.get()
    if current.scheduler_run_id is not None or current.scheduler_attempt_id is not None:
        if current.scheduler_run_id is None or current.scheduler_attempt_id is None:
            raise PaidScopeBlocked(
                "attempt_owner_lost", "Run and attempt must be supplied together"
            )
        yield current
        return
    timestamp = at or now_utc()
    with connect(db_path) as connection:
        supports_profiles = connection.execute(
            "SELECT 1 FROM sqlite_master WHERE type='table' "
            "AND name='acquisition_profile_activations'"
        ).fetchone() is not None
    if not supports_profiles:
        yield current
        return

    from . import durable_runs
    from .profile_activations import activation_at

    with connect(db_path) as connection, transaction(connection):
        active = activation_at(connection, timestamp)
        if active is None:
            claim = None
        else:
            if (
                current.activation_id is not None
                and current.activation_id != int(active["activation_id"])
            ):
                raise PaidScopeBlocked(
                    "profile_superseded",
                    "Frozen acquisition activation is no longer effective",
                )
            if (
                current.roster_snapshot_id is not None
                and current.roster_snapshot_id != int(active["roster_snapshot_id"])
            ) or (
                current.roster_snapshot_hash is not None
                and current.roster_snapshot_hash
                != str(active["roster_members_sha256"])
            ):
                raise PaidScopeBlocked(
                    "paid_scope_mismatch",
                    "Frozen roster differs from the active acquisition profile",
                )
            purpose = current.purpose or str(identity.get("purpose") or "")
            if purpose not in CATEGORY_MICROUSD:
                raise PaidScopeBlocked(
                    "unknown_paid_purpose", "Direct paid owner requires a fixed purpose"
                )
            business_day = current.business_day or budget_day(timestamp)
            frozen_identity = {
                **dict(identity),
                "contract_version": "direct-paid-dispatch-owner-v1",
                "request_id": uuid.uuid4().hex,
                "purpose": purpose,
                "business_day": business_day,
                "activation_id": int(active["activation_id"]),
                "profile_id": str(active["profile_id"]),
                "roster_snapshot_id": int(active["roster_snapshot_id"]),
                "roster_snapshot_hash": str(active["roster_members_sha256"]),
            }
            claim = durable_runs.claim_run_in_transaction(
                connection,
                job_id,
                frozen_identity,
                invocation_source="operator_retry",
                now=timestamp,
            )
    if active is None:
        # Preserve the established roster_not_ready/activation_required error
        # emitted by freeze_scope without fabricating an owner.
        yield current
        return
    if claim is None:
        raise PaidScopeBlocked(
            "attempt_owner_lost", "Direct paid dispatch owner could not be claimed"
        )
    owned = replace(
        current,
        purpose=purpose,
        activation_id=int(active["activation_id"]),
        roster_snapshot_id=int(active["roster_snapshot_id"]),
        roster_snapshot_hash=str(active["roster_members_sha256"]),
        scheduler_run_id=claim.scheduler_run_id,
        scheduler_attempt_id=claim.attempt_id,
        business_day=business_day,
    )
    token = _SCOPE.set(owned)
    try:
        try:
            yield owned
        except BaseException as error:
            completed_at = now_utc()
            with connect(db_path) as connection, transaction(connection):
                durable_runs.finish_run_in_transaction(
                    connection,
                    claim,
                    status="failed",
                    summary={
                        "reason": str(getattr(error, "error_code", type(error).__name__))
                    },
                    now=completed_at,
                )
            raise
        else:
            completed_at = now_utc()
            with connect(db_path) as connection, transaction(connection):
                durable_runs.checkpoint(
                    connection, claim, {"complete": True}, now=completed_at
                )
                durable_runs.finish_run_in_transaction(
                    connection,
                    claim,
                    status="succeeded",
                    summary={"reason": "completed"},
                    now=completed_at,
                )
    finally:
        _SCOPE.reset(token)


def micro_usd(value: Any) -> int:
    try:
        amount = Decimal(str(value))
        micro = amount * 1_000_000
        if not amount.is_finite() or amount < 0 or micro != micro.to_integral_value():
            raise ValueError
        return int(micro)
    except (ValueError, InvalidOperation, TypeError) as exc:
        raise BudgetBlocked("Amount must be a finite nonnegative micro-dollar value") from exc


def task_budget_id(task_id: str, provider: str, operation: str) -> str:
    """Return the canonical batch ID for the current task-budget contract.

    The v2 ID starts a new immutable batch while ``provider_usage.task_id``
    continues to enforce one cumulative ceiling across legacy v1 and v2 rows.
    """
    digest = hashlib.sha256(task_id.encode("utf-8")).hexdigest()[:16]
    return f"task-{digest}-{provider.lower()}-{operation}-{TASK_BUDGET_VERSION}"


def task_budget_purpose(task_id: str, operation: str) -> str:
    """Return the versioned unique purpose paired with ``task_budget_id``."""

    digest = hashlib.sha256(task_id.encode("utf-8")).hexdigest()[:16]
    return f"task_{digest}_{operation}_{TASK_BUDGET_VERSION}"


def budget_day(at: str) -> str:
    parsed = datetime.fromisoformat(at.replace("Z", "+00:00"))
    if parsed.tzinfo is None:
        raise BudgetBlocked("Budget time must include a timezone")
    return parsed.astimezone(SHANGHAI).date().isoformat()


def _details(value: str | None) -> dict[str, Any]:
    try:
        result = json.loads(value or "{}")
    except ValueError as exc:
        raise BudgetBlocked("Corrupt provider usage metadata") from exc
    if not isinstance(result, dict):
        raise BudgetBlocked("Provider usage metadata is not an object")
    return result


def _time(at: str) -> datetime:
    value = datetime.fromisoformat(at.replace("Z", "+00:00"))
    if value.tzinfo is None:
        raise BudgetBlocked("Budget evidence timestamps must include a timezone")
    return value.astimezone(SHANGHAI)


def _require_transaction(connection: sqlite3.Connection) -> None:
    if not connection.in_transaction:
        raise BudgetBlocked("Budget evidence requires a caller transaction")


def _write_receipt(
    connection: sqlite3.Connection, job_id: str, details: dict[str, Any], at: str,
    *, status: str = "succeeded",
) -> dict[str, Any]:
    _require_transaction(connection)
    encoded = json.dumps(details, sort_keys=True, separators=(",", ":"), ensure_ascii=False)
    cursor = connection.execute(
        """INSERT INTO scheduler_runs(job_id,scheduled_for,status,started_at,completed_at,details_json)
           VALUES (?,?,?,?,?,?)""",
        (job_id, f"{at}:{uuid.uuid4().hex}", status, at, at if status != "running" else None, encoded),
    )
    return {**details, "id": int(cursor.lastrowid or 0)}


def transport_circuit_decision(
    *, starts: int, uncertain: int, healthy_p95_rate: float = 0.01
) -> str | None:
    """Classify a ten-minute operation sample without opening a circuit.

    A caller may open immediately for the high-rate arm.  The lower-rate arm
    is only a candidate: it still needs a second planner tick, which request
    execution must not fabricate.
    """

    if (
        type(starts) is not int
        or type(uncertain) is not int
        or starts < 0
        or uncertain < 0
        or uncertain > starts
        or not 0 <= healthy_p95_rate <= 1
    ):
        raise ValueError("invalid transport circuit sample")
    if starts == 0:
        return None
    rate = uncertain / starts
    if starts >= 20 and rate >= 0.20:
        return "immediate"
    adaptive_threshold = max(0.05, 3 * min(healthy_p95_rate, 0.02))
    if starts >= 50 and rate >= adaptive_threshold:
        return "candidate"
    return None


def transport_healthy_p95_rate(
    connection: sqlite3.Connection, *, operation: str, at: str
) -> float:
    """Load a seven-complete-day baseline, capped at 2%; otherwise use 1%."""

    row = connection.execute(
        """SELECT status,details_json FROM scheduler_runs WHERE job_id=?
           ORDER BY id DESC LIMIT 1""",
        (f"{TRANSPORT_BASELINE_JOB}{operation}",),
    ).fetchone()
    proof = _details(row["details_json"]) if row is not None else {}
    days = proof.get("complete_business_days")
    value = proof.get("p95_transport_uncertain_rate")
    if (
        row is None
        or row["status"] != "succeeded"
        or proof.get("contract_version") != "transport-healthy-baseline-v1"
        or proof.get("provider") != "tikhub"
        or proof.get("operation") != operation
        or not isinstance(days, list)
        or len(days) != 7
        or any(not isinstance(day, str) for day in days)
        or len(set(days)) != 7
        or not isinstance(value, (int, float))
        or isinstance(value, bool)
        or not math.isfinite(float(value))
        or not 0 <= float(value) <= 1
        or proof.get("valid_through") is None
    ):
        return 0.01
    try:
        today = date.fromisoformat(budget_day(at))
        expected_days = {(today - timedelta(days=offset)).isoformat() for offset in range(1, 8)}
        if set(days) != expected_days or _time(str(proof["valid_through"])) < _time(at):
            return 0.01
    except (BudgetBlocked, TypeError, ValueError):
        return 0.01
    return min(float(value), 0.02)


def _transport_window_counts(
    connection: sqlite3.Connection, *, operation: str, at: str
) -> tuple[int, int]:
    end = _time(at)
    start = end - timedelta(minutes=10)
    starts = uncertain = 0
    for row in connection.execute(
        """SELECT request_attempts,recorded_at,details_json FROM provider_usage
           WHERE lower(provider)='tikhub' AND operation=?
             AND recorded_at>? AND recorded_at<=?""",
        (
            operation,
            start.astimezone(timezone.utc)
            .isoformat(timespec="seconds")
            .replace("+00:00", "Z"),
            end.astimezone(timezone.utc)
            .isoformat(timespec="seconds")
            .replace("+00:00", "Z"),
        ),
    ):
        details = _details(row["details_json"])
        sent_at = details.get("sent_at")
        if row["request_attempts"] != 1 or not isinstance(sent_at, str):
            continue
        try:
            sent = _time(sent_at)
        except (BudgetBlocked, TypeError, ValueError):
            continue
        if not start < sent <= end:
            continue
        starts += 1
        transport = details.get("transport")
        receipt_uncertain = isinstance(transport, Mapping) and any(
            transport.get(key) is False
            for key in ("clean_eof", "length_match", "gzip_crc_ok")
        )
        if details.get("error_code") == "transport_error" or receipt_uncertain:
            uncertain += 1
    return starts, uncertain


def evaluate_transport_operation_fault(
    connection: sqlite3.Connection,
    *,
    operation: str,
    at: str,
    planner_tick_id: str | None = None,
    healthy_p95_rate: float | None = None,
) -> dict[str, Any]:
    """Evaluate one production operation and persist the two-tick arm."""

    _require_transaction(connection)
    starts, uncertain = _transport_window_counts(
        connection, operation=operation, at=at
    )
    baseline = (
        transport_healthy_p95_rate(connection, operation=operation, at=at)
        if healthy_p95_rate is None
        else healthy_p95_rate
    )
    decision_value = transport_circuit_decision(
        starts=starts,
        uncertain=uncertain,
        healthy_p95_rate=baseline,
    )
    rate = uncertain / starts if starts else 0.0
    threshold = max(0.05, 3 * min(baseline, 0.02))
    candidate_job = f"{TRANSPORT_CANDIDATE_JOB}{operation}"
    previous_row = connection.execute(
        """SELECT details_json FROM scheduler_runs WHERE job_id=?
           ORDER BY id DESC LIMIT 1""",
        (candidate_job,),
    ).fetchone()
    previous = _details(previous_row["details_json"]) if previous_row else {}
    consecutive_candidate = False
    if planner_tick_id is not None:
        try:
            previous_recent = (
                isinstance(previous.get("evaluated_at"), str)
                and _time(str(previous["evaluated_at"])) >= _time(at) - timedelta(minutes=10)
            )
        except (BudgetBlocked, TypeError, ValueError):
            previous_recent = False
        consecutive_candidate = bool(
            decision_value == "candidate"
            and previous.get("decision") == "candidate"
            and previous.get("planner_tick_id") != planner_tick_id
            and previous_recent
        )
        if decision_value == "candidate" or previous.get("decision") == "candidate":
            _write_receipt(
                connection,
                candidate_job,
                {
                    "contract_version": "transport-circuit-sample-v1",
                    "provider": "tikhub",
                    "operation": operation,
                    "planner_tick_id": planner_tick_id,
                    "evaluated_at": at,
                    "starts": starts,
                    "uncertain": uncertain,
                    "uncertain_rate": rate,
                    "healthy_p95_rate": baseline,
                    "threshold": threshold,
                    "decision": decision_value,
                },
                at,
            )
    fault = None
    if decision_value == "immediate" or consecutive_candidate:
        arm = "immediate" if decision_value == "immediate" else "two_tick"
        fault = record_fault_state(
            connection,
            scope_kind="operation",
            operation=operation,
            fault_class="transport",
            reason=f"transport_ratio_{arm}",
            usage_id=None,
            at=at,
            state_evidence={
                "contract_version": "transport-ratio-v1",
                "arm": arm,
                "healthy_p95_rate": baseline,
                "threshold": 0.20 if arm == "immediate" else threshold,
            },
        )
    return {
        "operation": operation,
        "starts": starts,
        "uncertain": uncertain,
        "uncertain_rate": rate,
        "decision": decision_value,
        "consecutive_candidate": consecutive_candidate,
        "fault": fault,
    }


def evaluate_transport_faults(
    connection: sqlite3.Connection, *, at: str, planner_tick_id: str
) -> list[dict[str, Any]]:
    """Planner entrypoint for every priced TikHub operation."""

    return [
        evaluate_transport_operation_fault(
            connection,
            operation=operation,
            at=at,
            planner_tick_id=planner_tick_id,
        )
        for operation in sorted(PRICES_MICROUSD)
    ]


def _fault_scope(
    *,
    scope_kind: str,
    provider: str,
    operation: str | None,
    authorization_id: str | int | None,
    paid_identity: str | None,
) -> dict[str, Any]:
    if scope_kind not in FAULT_SCOPE_KINDS:
        raise ValueError("unknown provider fault scope")
    # Storage readiness is a local, provider-independent send prerequisite.
    # Giving it a single global scope prevents one adapter from continuing to
    # receive bytes that cannot be durably retained.
    scope = {
        "scope_kind": scope_kind,
        "provider": "all" if scope_kind == "storage_hard" else provider.lower(),
    }
    if scope_kind == "operation":
        if not operation:
            raise ValueError("operation fault requires an operation")
        scope["operation"] = operation
    elif scope_kind == "authorization_hard":
        if authorization_id in (None, ""):
            raise ValueError("authorization fault requires an authorization id")
        scope["authorization_id"] = str(authorization_id)
    elif scope_kind == "paid_identity_hold":
        if not paid_identity:
            raise ValueError("paid identity hold requires an identity")
        scope["paid_identity"] = paid_identity
    return scope


def _fault_job_prefix(scope: Mapping[str, Any]) -> str:
    digest = hashlib.sha256(
        json.dumps(scope, sort_keys=True, separators=(",", ":")).encode("utf-8")
    ).hexdigest()[:24]
    return f"{FAULT_JOB_PREFIX}:{scope['provider']}:{scope['scope_kind']}:{digest}:"


def _fault_job_id(scope: Mapping[str, Any], fault_class: str) -> str:
    class_digest = hashlib.sha256(fault_class.encode("utf-8")).hexdigest()[:16]
    return f"{_fault_job_prefix(scope)}{class_digest}"


def _v2_fault_states(
    connection: sqlite3.Connection, scope: Mapping[str, Any]
) -> list[dict[str, Any]]:
    """Return the latest append-only event for each fault class in a scope."""

    latest: dict[str, dict[str, Any]] = {}
    for row in connection.execute(
        """SELECT id,details_json FROM scheduler_runs
           WHERE job_id LIKE ? ORDER BY id DESC""",
        (f"{_fault_job_prefix(scope)}%",),
    ):
        details = _details(row["details_json"])
        fault_class = details.get("fault_class")
        if (
            details.get("contract_version") != "provider-fault-v2"
            or not isinstance(fault_class, str)
            or fault_class in latest
            or any(details.get(key) != value for key, value in scope.items())
        ):
            continue
        latest[fault_class] = {**details, "receipt_id": int(row["id"])}
    return list(latest.values())


def _legacy_provider_circuit(connection: sqlite3.Connection) -> dict[str, Any] | None:
    row = connection.execute(
        """SELECT id,details_json FROM scheduler_runs
           WHERE job_id='provider_circuit:tikhub' ORDER BY id DESC LIMIT 1"""
    ).fetchone()
    if row is None:
        return None
    return {**_details(row["details_json"]), "receipt_id": int(row["id"])}


def _legacy_provider_fault_projection(
    legacy: Mapping[str, Any],
) -> dict[str, Any]:
    reason = str(legacy.get("reason") or "legacy_provider_circuit")
    fault_class = {
        "provider_balance_blocked": "balance",
        "provider_auth_blocked": "application_auth",
        "provider_outage": "provider_outage",
        "provider_global_quota": "global_quota",
    }.get(reason, "legacy_unclassified")
    evidence = {
        "legacy_circuit_receipt_id": int(legacy["receipt_id"]),
        "legacy_contract_version": legacy.get("contract_version"),
        "legacy_reason": reason,
    }
    scope = {"scope_kind": "provider_hard", "provider": "tikhub"}
    fingerprint = hashlib.sha256(
        json.dumps(
            {
                "contract_version": "provider-fault-v2",
                "scope": scope,
                "fault_class": fault_class,
                "state_evidence": evidence,
            },
            sort_keys=True,
            separators=(",", ":"),
        ).encode("utf-8")
    ).hexdigest()
    return {
        "contract_version": "provider-fault-v2-legacy-pending",
        **scope,
        "open": True,
        "opened_at": legacy.get("opened_at"),
        "last_failure_at": legacy.get("last_failure_at"),
        "generation": f"legacy:{int(legacy['receipt_id'])}",
        "state_fingerprint": fingerprint,
        "state_evidence": evidence,
        "fault_class": fault_class,
        "reason": reason,
        "usage_id": legacy.get("usage_id"),
        "receipt_id": int(legacy["receipt_id"]),
        "probe_eligible": fault_class in PROBEABLE_PROVIDER_FAULTS,
        "recovery_required": (
            "append_only_legacy_binding"
            if fault_class in PROBEABLE_PROVIDER_FAULTS
            else "operator_classification_receipt"
        ),
    }


def fault_state(
    connection: sqlite3.Connection,
    *,
    scope_kind: str,
    provider: str = "TikHub",
    operation: str | None = None,
    authorization_id: str | int | None = None,
    paid_identity: str | None = None,
    fault_class: str | None = None,
) -> dict[str, Any] | None:
    scope = _fault_scope(
        scope_kind=scope_kind,
        provider=provider,
        operation=operation,
        authorization_id=authorization_id,
        paid_identity=paid_identity,
    )
    states = _v2_fault_states(connection, scope)
    if scope == {"scope_kind": "provider_hard", "provider": "tikhub"}:
        legacy = _legacy_provider_circuit(connection)
        if legacy is not None and legacy.get("open") is True:
            legacy_id = int(legacy["receipt_id"])
            legacy_bound = connection.execute(
                """SELECT 1 FROM scheduler_runs WHERE job_id LIKE ?
                     AND json_valid(details_json)
                     AND json_extract(details_json,'$.contract_version')='provider-fault-v2'
                     AND json_extract(details_json,'$.state_evidence.legacy_circuit_receipt_id')=?
                   LIMIT 1""",
                (f"{_fault_job_prefix(scope)}%", legacy_id),
            ).fetchone() is not None
            if not legacy_bound:
                states.append(_legacy_provider_fault_projection(legacy))
    if fault_class is not None:
        return next(
            (state for state in states if state.get("fault_class") == fault_class),
            None,
        )
    if states:
        open_states = [state for state in states if state.get("open") is True]
        candidates = open_states or states
        return max(candidates, key=lambda state: int(state["receipt_id"]))
    if scope == {"scope_kind": "provider_hard", "provider": "tikhub"}:
        # Frozen schema-19 history remains immutable.  Legacy rows are only a
        # compatibility projection; v2 events never UPDATE or replace them.
        legacy = _legacy_provider_circuit(connection)
        if legacy is not None:
            return legacy
    return None


def record_fault_state(
    connection: sqlite3.Connection,
    *,
    scope_kind: str,
    fault_class: str,
    reason: str,
    usage_id: int | None,
    at: str,
    provider: str = "TikHub",
    operation: str | None = None,
    authorization_id: str | int | None = None,
    paid_identity: str | None = None,
    state_evidence: Mapping[str, Any] | None = None,
) -> dict[str, Any]:
    """Open one durable schema-19 fault scope idempotently by fingerprint."""

    _require_transaction(connection)
    if not fault_class:
        raise ValueError("provider fault class is required")
    scope = _fault_scope(
        scope_kind=scope_kind,
        provider=provider,
        operation=operation,
        authorization_id=authorization_id,
        paid_identity=paid_identity,
    )
    evidence = dict(state_evidence or {"error_code": reason})
    try:
        json.dumps(evidence, sort_keys=True, separators=(",", ":"), allow_nan=False)
    except (TypeError, ValueError) as error:
        raise ValueError("provider fault evidence must be finite JSON") from error
    job_id = _fault_job_id(scope, fault_class)
    fingerprint = hashlib.sha256(
        json.dumps(
            {
                "contract_version": "provider-fault-v2",
                "scope": scope,
                "fault_class": fault_class,
                "state_evidence": evidence,
            },
            sort_keys=True,
            separators=(",", ":"),
        ).encode("utf-8")
    ).hexdigest()
    previous = fault_state(connection, **scope, fault_class=fault_class) or {}
    same_open_fault = (
        previous.get("open") is True
        and previous.get("state_fingerprint") == fingerprint
    )
    if same_open_fault and previous.get("contract_version") == "provider-fault-v2":
        return previous
    details = {
        "contract_version": "provider-fault-v2",
        **scope,
        "open": True,
        "opened_at": at,
        "last_failure_at": at,
        "generation": uuid.uuid4().hex,
        "state_fingerprint": fingerprint,
        "state_evidence": evidence,
        "fault_class": fault_class,
        "reason": reason,
        "usage_id": usage_id,
        "probe_eligible": (
            scope_kind == "provider_hard" and fault_class in PROBEABLE_PROVIDER_FAULTS
        ),
        "recovery_required": (
            "authorized_verified_live_probe"
            if scope_kind == "provider_hard" and fault_class in PROBEABLE_PROVIDER_FAULTS
            else "domain_specific_evidence"
        ),
    }
    return _write_receipt(connection, job_id, details, at, status="partial")


def _resolve_fault_event(
    connection: sqlite3.Connection,
    *,
    scope_kind: str,
    at: str,
    expected_generation: str | None = None,
    expected_fingerprint: str | None = None,
    provider: str = "TikHub",
    operation: str | None = None,
    authorization_id: str | int | None = None,
    paid_identity: str | None = None,
    evidence_id: int | str | None = None,
    fault_class: str | None = None,
) -> bool:
    """Append the close event after the caller has validated its recovery proof."""

    scope = _fault_scope(
        scope_kind=scope_kind,
        provider=provider,
        operation=operation,
        authorization_id=authorization_id,
        paid_identity=paid_identity,
    )
    open_states = [
        state for state in _v2_fault_states(connection, scope)
        if state.get("open") is True
        and (fault_class is None or state.get("fault_class") == fault_class)
    ]
    if len(open_states) != 1:
        return False
    current = open_states[0]
    if not current.get("open"):
        return False
    if expected_generation is not None and current.get("generation") != expected_generation:
        return False
    if expected_fingerprint is not None and current.get("state_fingerprint") != expected_fingerprint:
        return False
    current = {
        key: value for key, value in current.items() if key != "receipt_id"
    }
    current.update(open=False, recovered_at=at, recovery_evidence_id=evidence_id)
    _write_receipt(
        connection,
        _fault_job_id(scope, str(current["fault_class"])),
        current,
        at,
    )
    return True


def resolve_fault_state(
    connection: sqlite3.Connection,
    *,
    scope_kind: str,
    at: str,
    expected_generation: str | None = None,
    expected_fingerprint: str | None = None,
    provider: str = "TikHub",
    operation: str | None = None,
    authorization_id: str | int | None = None,
    paid_identity: str | None = None,
    evidence_id: int | str | None = None,
    fault_class: str | None = None,
) -> bool:
    """Close an account/provider/identity fault with exact immutable evidence."""

    _require_transaction(connection)
    if scope_kind in {"operation", "storage_hard"}:
        raise ValueError("operation and storage faults require their typed recovery contract")
    if (
        not expected_generation
        or not expected_fingerprint
        or evidence_id in (None, "")
        or not fault_class
    ):
        raise ValueError(
            "fault recovery requires generation, fingerprint, fault class and evidence"
        )
    if scope_kind == "provider_hard":
        probe = (
            connection.execute(
                "SELECT status,details_json FROM scheduler_runs WHERE id=? AND job_id=?",
                (evidence_id, PROBE_JOB),
            ).fetchone()
            if _positive_int(evidence_id)
            else None
        )
        proof = _details(probe["details_json"]) if probe is not None else {}
        if (
            probe is None
            or probe["status"] != "succeeded"
            or proof.get("state") != "succeeded"
            or proof.get("circuit_generation") != expected_generation
            or proof.get("state_fingerprint") != expected_fingerprint
            or proof.get("fault_class") != fault_class
        ):
            raise ValueError("provider-hard recovery requires its successful one-shot probe")
    elif scope_kind == "authorization_hard":
        health = (
            connection.execute(
                "SELECT status FROM scheduler_runs WHERE id=?", (evidence_id,)
            ).fetchone()
            if _positive_int(evidence_id)
            else None
        )
        if health is None or health["status"] not in {
            "succeeded",
            "partial",
            "failed",
        }:
            raise ValueError("authorization recovery requires a terminal health receipt")
    return _resolve_fault_event(
        connection,
        scope_kind=scope_kind,
        at=at,
        expected_generation=expected_generation,
        expected_fingerprint=expected_fingerprint,
        provider=provider,
        operation=operation,
        authorization_id=authorization_id,
        paid_identity=paid_identity,
        evidence_id=evidence_id,
        fault_class=fault_class,
    )


def _positive_int(value: Any) -> bool:
    return type(value) is int and value > 0


def resolve_operation_fault(
    connection: sqlite3.Connection,
    *,
    operation: str,
    fault_class: str,
    recovery_evidence: Mapping[str, Any],
    at: str,
) -> dict[str, Any]:
    """Close one operation fault only after its domain-specific recovery gate."""

    _require_transaction(connection)
    current = fault_state(
        connection,
        scope_kind="operation",
        operation=operation,
        fault_class=fault_class,
    )
    if current is None or current.get("open") is not True:
        raise PaidScopeBlocked("operation_recovery_invalid", "Operation fault is not open")
    from .fault_recovery import verify_operation_recovery

    try:
        evidence = verify_operation_recovery(connection, current, recovery_evidence, at)
    except (ValueError, OSError, RuntimeError) as error:
        raise PaidScopeBlocked("operation_recovery_invalid", str(error)) from error
    contract = evidence.get("contract_version")
    valid = False
    if fault_class == "transport" and contract == "transport-operation-requalification-v1":
        campaign_id = evidence.get("campaign_id")
        campaign = connection.execute(
            "SELECT status,details_json FROM scheduler_runs WHERE id=?",
            (campaign_id,),
        ).fetchone() if _positive_int(campaign_id) else None
        campaign_proof = _details(campaign["details_json"]) if campaign is not None else {}
        valid = bool(
            evidence.get("operation") == operation
            and type(evidence.get("sample_size")) is int
            and int(evidence["sample_size"]) >= 200
            and type(evidence.get("uncertain_count")) is int
            and int(evidence["uncertain_count"]) >= 0
            and isinstance(evidence.get("wilson_upper"), (int, float))
            and not isinstance(evidence.get("wilson_upper"), bool)
            and math.isfinite(float(evidence["wilson_upper"]))
            and float(evidence["wilson_upper"]) <= 0.02
            and evidence.get("partial_canonical") == 0
            and evidence.get("raw_readback_count") == evidence.get("sample_size")
            and type(evidence.get("consecutive_complete")) is int
            and int(evidence["consecutive_complete"]) >= 3
            and campaign is not None
            and campaign["status"] == "succeeded"
            and campaign_proof.get("contract_version")
            == "transport-requalification-campaign-v1"
            and campaign_proof.get("operation") == operation
        )
    elif fault_class == "rate_limit" and contract == "rate-operation-recovery-v1":
        try:
            reset_reached = _time(str(evidence.get("reset_at"))) <= _time(at)
        except (BudgetBlocked, TypeError, ValueError):
            reset_reached = False
        quota_id = evidence.get("quota_receipt_id")
        quota = connection.execute(
            "SELECT status,details_json FROM scheduler_runs WHERE id=?",
            (quota_id,),
        ).fetchone() if _positive_int(quota_id) else None
        quota_proof = _details(quota["details_json"]) if quota is not None else {}
        valid = bool(
            evidence.get("operation") == operation
            and reset_reached
            and evidence.get("consecutive_natural_due") == 3
            and evidence.get("rate_errors") == 0
            and quota is not None
            and quota["status"] == "succeeded"
            and quota_proof.get("contract_version") == "provider-quota-window-v1"
            and quota_proof.get("provider") == "tikhub"
            and quota_proof.get("operation") == operation
        )
    elif fault_class == "field_contract" and contract == "field-contract-recovery-v1":
        policy_id = evidence.get("policy_receipt_id")
        policy = connection.execute(
            "SELECT status,details_json FROM scheduler_runs WHERE id=?",
            (policy_id,),
        ).fetchone() if _positive_int(policy_id) else None
        policy_proof = _details(policy["details_json"]) if policy is not None else {}
        valid = bool(
            evidence.get("operation") == operation
            and evidence.get("offline_replay_passed") is True
            and evidence.get("field_canary_passed") is True
            and policy is not None
            and policy["status"] == "succeeded"
            and policy_proof.get("contract_version") == "field-policy-release-v1"
            and policy_proof.get("operation") == operation
        )
    if not valid:
        raise PaidScopeBlocked(
            "operation_recovery_invalid",
            "Operation recovery evidence does not satisfy its fault contract",
        )
    receipt = _write_receipt(
        connection,
        f"{OPERATION_RECOVERY_JOB}{operation}:{fault_class}",
        {
            **evidence,
            "provider": "tikhub",
            "fault_class": fault_class,
            "fault_generation": current["generation"],
            "fault_fingerprint": current["state_fingerprint"],
            "recovered_at": at,
        },
        at,
    )
    closed = _resolve_fault_event(
        connection,
        scope_kind="operation",
        operation=operation,
        fault_class=fault_class,
        expected_generation=str(current["generation"]),
        expected_fingerprint=str(current["state_fingerprint"]),
        evidence_id=int(receipt["id"]),
        at=at,
    )
    if not closed:
        raise PaidScopeBlocked(
            "operation_recovery_stale", "Operation fault changed before recovery"
        )
    return {**receipt, "fault_closed": True}


def resolve_storage_fault(
    connection: sqlite3.Connection,
    *,
    recovery_evidence: Mapping[str, Any],
    at: str,
) -> dict[str, Any]:
    """Close storage_hard only with local durability and capacity proof."""

    _require_transaction(connection)
    current = fault_state(
        connection,
        scope_kind="storage_hard",
        provider="all",
        fault_class="local_evidence_store",
    )
    if current is None or current.get("open") is not True:
        raise PaidScopeBlocked("storage_recovery_invalid", "Storage fault is not open")
    from .fault_recovery import verify_storage_recovery

    try:
        evidence = verify_storage_recovery(connection, current, recovery_evidence, at)
    except (ValueError, OSError, RuntimeError) as error:
        raise PaidScopeBlocked("storage_recovery_invalid", str(error)) from error
    capacity_id = evidence.get("capacity_receipt_id")
    capacity = connection.execute(
        "SELECT status,details_json FROM scheduler_runs WHERE id=?",
        (capacity_id,),
    ).fetchone() if _positive_int(capacity_id) else None
    capacity_proof = _details(capacity["details_json"]) if capacity is not None else {}
    if (
        evidence.get("contract_version") != "storage-recovery-v1"
        or evidence.get("idempotent_write_passed") is not True
        or evidence.get("file_fsync_passed") is not True
        or evidence.get("directory_fsync_passed") is not True
        or evidence.get("hash_readback_passed") is not True
        or capacity is None
        or capacity["status"] != "succeeded"
        or capacity_proof.get("contract_version") != "storage_capacity_receipt_v1"
        or capacity_proof.get("admitted") is not True
    ):
        raise PaidScopeBlocked(
            "storage_recovery_invalid",
            "Storage recovery requires durability, readback and admitted capacity",
        )
    receipt = _write_receipt(
        connection,
        STORAGE_RECOVERY_JOB,
        {
            **evidence,
            "provider": "all",
            "fault_class": "local_evidence_store",
            "fault_generation": current["generation"],
            "fault_fingerprint": current["state_fingerprint"],
            "recovered_at": at,
        },
        at,
    )
    closed = _resolve_fault_event(
        connection,
        scope_kind="storage_hard",
        provider="all",
        fault_class="local_evidence_store",
        expected_generation=str(current["generation"]),
        expected_fingerprint=str(current["state_fingerprint"]),
        evidence_id=int(receipt["id"]),
        at=at,
    )
    if not closed:
        raise PaidScopeBlocked(
            "storage_recovery_stale", "Storage fault changed before recovery"
        )
    return {**receipt, "fault_closed": True}


def require_storage_ready(connection: sqlite3.Connection) -> None:
    current = fault_state(connection, scope_kind="storage_hard", provider="all")
    if current and current.get("open"):
        raise PaidScopeBlocked(
            "storage_hard", "Network starts are blocked by local storage readiness"
        )


def work_state_fingerprint(connection: sqlite3.Connection) -> str:
    """Hash only source work/identity state, never monetary or proof receipts."""
    _require_transaction(connection)
    snapshot = current_snapshot(connection)
    facts: dict[str, Any] = {
        "roster": None if snapshot is None else [snapshot["id"], snapshot["members_sha256"]],
    }
    queries = {
        "accounts": "SELECT id,enabled,updated_at FROM accounts ORDER BY id",
        "identities": "SELECT id,account_id,platform,uid,updated_at FROM account_platform_identities ORDER BY id",
        "contents": "SELECT id,account_id,updated_at FROM content_items ORDER BY id",
        "slots": "SELECT id,status,attempt_count,updated_at FROM fetch_slots ORDER BY id",
        "runs": """SELECT id,status,details_json FROM scheduler_runs
                   WHERE json_extract(details_json,'$.contract_version')='durable-run-v1' ORDER BY id""",
    }
    for name, query in queries.items():
        facts[name] = [tuple(row) for row in connection.execute(query)]
    return hashlib.sha256(json.dumps(facts, sort_keys=True, separators=(",", ":")).encode()).hexdigest()


def record_queue_inventory(
    connection: sqlite3.Connection, *, category: str,
    due_candidate_ids: Iterable[int | str], at: str,
) -> dict[str, Any]:
    """Record a complete caller-enumerated queue while that read view is locked."""
    _require_transaction(connection)
    if category not in CATEGORY_MICROUSD:
        raise BudgetBlocked("Unknown queue category")
    candidates = list(due_candidate_ids)
    if any(type(item) not in (int, str) or not str(item) for item in candidates):
        raise BudgetBlocked("Inventory needs stable candidate identifiers")
    normalized = sorted({str(item) for item in candidates})
    details = {
        "contract_version": "budget-queue-inventory-v1", "category": category,
        "budget_day": budget_day(at), "checked_at": at,
        "due_candidate_ids": normalized, "due_count": len(normalized),
        "candidate_sha256": hashlib.sha256(json.dumps(normalized).encode()).hexdigest(),
        "work_state_fingerprint": work_state_fingerprint(connection),
    }
    return _write_receipt(connection, INVENTORY_JOB + category, details, at)


def required_closeout_rounds(category: str, *, at: str) -> set[tuple[str, str]]:
    """Registration IDs and exact Beijing times, including the 18:00 Matrix round."""
    if category not in CATEGORY_MICROUSD:
        raise BudgetBlocked("Unknown closeout category")
    required = {("matrix_account_metrics", "18:00"), ("matrix_works_refresh", "18:00")}
    if category in {"metrics", "reconcile", "detail", "comments"}:
        required |= {("matrix_works_scan", "02:10")}
        required |= {("matrix_works_refresh", stamp) for stamp in ("06:00", "12:00")}
    if category in {"metrics", "reconcile"}:
        required |= {("matrix_account_metrics", stamp) for stamp in ("02:00", "06:00", "12:00")}
    if category == "reconcile":
        required.add(("tikhub_reconcile", "03:00"))
    if category == "metrics":
        required |= {
            ("metrics_backfill", "06:10"), ("metrics_backfill_close", "07:00"),
            ("metrics_backfill_established", "08:35"),
            ("daily_pipeline_summary", "07:30"), ("daily_report", "08:00"),
        }
        if _time(at).weekday() == 0:
            required.add(("weekly_report", "08:30"))
    return required


def _finished_rounds(
    connection: sqlite3.Connection, category: str, run_ids: Iterable[int], *, at: str,
) -> list[int]:
    day = budget_day(at)
    supplied = set(run_ids)
    found: dict[tuple[str, str], sqlite3.Row] = {}
    for row in connection.execute(
            "SELECT * FROM scheduler_runs WHERE job_id LIKE 'pipeline_round:%' ORDER BY id"):
        details = _details(row["details_json"])
        identity = details.get("identity", {})
        if identity.get("beijing_day") != day or not identity.get("scheduled_at"):
            continue
        scheduled = _time(identity["scheduled_at"])
        if scheduled.date().isoformat() != day:
            continue
        found[(str(row["job_id"]).removeprefix("pipeline_round:"), scheduled.strftime("%H:%M"))] = row
    verified: list[int] = []
    for key in sorted(required_closeout_rounds(category, at=at)):
        row = found.get(key)
        if row is None or row["id"] not in supplied:
            raise BudgetBlocked("A required fixed round has no current receipt")
        details = _details(row["details_json"])
        checkpoint = details.get("checkpoint", {})
        if (row["status"] != "succeeded" or details.get("complete") is not True
                or details.get("contract_version") != "durable-run-v1"
                or checkpoint.get("complete") is not True
                or not details.get("identity", {}).get("round_id")
                or not isinstance(checkpoint.get("child_run_ids"), list)
                or not row["completed_at"] or _time(row["completed_at"]) > _time(at)):
            raise BudgetBlocked("A required fixed round is incomplete")
        for child_id in checkpoint["child_run_ids"]:
            child = connection.execute("SELECT * FROM scheduler_runs WHERE id=?", (child_id,)).fetchone()
            child_details = _details(child["details_json"]) if child is not None else {}
            if (child is None or child["status"] != "succeeded"
                    or child_details.get("complete") is not True
                    or child_details.get("checkpoint", {}).get("complete") is not True):
                raise BudgetBlocked("A fixed round still has incomplete child work")
        verified.append(int(row["id"]))
    return verified


def _no_pending_reservations(connection: sqlite3.Connection, category: str) -> bool:
    return connection.execute(
        """SELECT 1 FROM provider_usage WHERE lower(provider)='tikhub'
           AND json_extract(details_json,'$.category')=?
           AND json_extract(details_json,'$.state') IN ('reserved','sent','billing_unknown') LIMIT 1""",
        (category,),
    ).fetchone() is None


def _due_inventory(connection: sqlite3.Connection, *, at: str) -> dict[str, list[str]]:
    _require_transaction(connection)
    # Import only at the decision boundary; pipeline imports this shared ledger.
    from .pipeline import budget_due_inventory

    inventory = budget_due_inventory(connection, at=at)
    if not isinstance(inventory, dict) or set(inventory) != set(CATEGORY_MICROUSD):
        raise BudgetBlocked("Live queue inventory must cover every protected category")
    if any(not isinstance(values, list) or any(not isinstance(value, str) or not value for value in values)
           for values in inventory.values()):
        raise BudgetBlocked("Live queue inventory has invalid candidate identifiers")
    return inventory


def record_closeout(
    connection: sqlite3.Connection, *, category: str, inventory_ids: Iterable[int],
    round_run_ids: Iterable[int], at: str,
) -> dict[str, Any]:
    _require_transaction(connection)
    if _time(at).strftime("%H:%M") < "18:35":
        raise BudgetBlocked("Quota cannot be lent before Beijing 18:35")
    ids = list(inventory_ids)
    if not ids:
        raise BudgetBlocked("A current atomic inventory receipt is required")
    inventories: list[tuple[int, dict[str, Any]]] = []
    for inventory_id in ids:
        row = connection.execute(
            "SELECT details_json FROM scheduler_runs WHERE id=? AND job_id=? AND status='succeeded'",
            (inventory_id, INVENTORY_JOB + category),
        ).fetchone()
        value = _details(row["details_json"]) if row is not None else {}
        if (value.get("contract_version") != "budget-queue-inventory-v1"
                or value.get("budget_day") != budget_day(at) or value.get("due_count") != 0
                or value.get("due_candidate_ids") != []):
            raise BudgetBlocked("Lending inventory is missing, nonempty or from another day")
        inventories.append((inventory_id, value))
    latest_id, latest = max(inventories, key=lambda item: _time(item[1]["checked_at"]))
    if _time(latest["checked_at"]) != _time(at):
        raise BudgetBlocked("Closeout needs an inventory checked in this decision")
    if _due_inventory(connection, at=at)[category]:
        raise BudgetBlocked("Lender has current due work")
    rounds = _finished_rounds(connection, category, round_run_ids, at=at)
    if not _no_pending_reservations(connection, category):
        raise BudgetBlocked("Lender still has sent, reserved or billing-unknown requests")
    return _write_receipt(connection, CLOSEOUT_JOB + category, {
        "contract_version": "budget-closeout-v1", "category": category,
        "budget_day": budget_day(at), "closed_at": at, "inventory_ids": [latest_id],
        "round_run_ids": rounds, "work_state_fingerprint": work_state_fingerprint(connection),
    }, at)


def _lending_proof(
    connection: sqlite3.Connection, category: str, *, at: str,
) -> dict[str, Any] | None:
    row = connection.execute(
        "SELECT id,details_json FROM scheduler_runs WHERE job_id=? ORDER BY id DESC LIMIT 1",
        (CLOSEOUT_JOB + category,),
    ).fetchone()
    if row is None:
        return None
    proof = _details(row["details_json"])
    if (proof.get("contract_version") != "budget-closeout-v1"
            or proof.get("budget_day") != budget_day(at) or _time(proof["closed_at"]) > _time(at)
            or not _no_pending_reservations(connection, category)):
        return None
    try:
        _finished_rounds(connection, category, proof["round_run_ids"], at=at)
    except BudgetBlocked:
        return None
    return {**proof, "id": row["id"]}


def _borrowing(
    connection: sqlite3.Connection, *, category: str, amount: int, at: str,
    summary: Mapping[str, Any], exclude_usage_id: int | None,
) -> tuple[dict[str, int], dict[str, int], str | None]:
    lent = {name: 0 for name in CATEGORY_MICROUSD}
    received = dict(lent)
    for row in connection.execute("SELECT * FROM provider_usage WHERE lower(provider)='tikhub' AND currency='USD'"):
        if row["id"] == exclude_usage_id or not row["amount"]:
            continue
        details = _details(row["details_json"])
        if details.get("budget_day") != budget_day(at):
            continue
        for lender, value in details.get("borrowed_from", {}).items():
            if lender not in lent or type(value) is not int or value < 0:
                raise BudgetBlocked("Corrupt lending allocation")
            lent[lender] += value
            received[details["category"]] += value
    balances = {
        name: max(0, CATEGORY_MICROUSD[name] - summary["categories_microusd"][name] + received[name] - lent[name])
        for name in CATEGORY_MICROUSD
    }
    need = max(0, amount - balances[category])
    if not need:
        return {}, {}, None
    if _time(at).strftime("%H:%M") < "18:35":
        raise PaidScopeBlocked("category_budget_exhausted", "Protected category quota reached before closeout")
    inventory = _due_inventory(connection, at=at)
    if not inventory[category] and exclude_usage_id is None:
        raise PaidScopeBlocked("category_budget_exhausted", "Borrower has no current due candidate")
    for higher in BORROW_PRIORITY[:BORROW_PRIORITY.index(category)]:
        if inventory[higher]:
            raise PaidScopeBlocked("category_budget_exhausted", "Higher-priority due work takes precedence")
    allocation: dict[str, int] = {}
    proofs: dict[str, int] = {}
    for lender in reversed(BORROW_PRIORITY):
        if lender == category or balances[lender] <= 0 or inventory[lender]:
            continue
        proof = _lending_proof(connection, lender, at=at)
        if proof is None:
            continue
        take = min(need, balances[lender])
        allocation[lender], proofs[lender] = take, int(proof["id"])
        need -= take
        if not need:
            return allocation, proofs, None
    raise PaidScopeBlocked("category_budget_exhausted", "No verified lending closeout covers the shortfall")


def _assert_scheduler_owner(connection: sqlite3.Connection, scope: PaidScope) -> PaidScope:
    if scope.scheduler_run_id is None and scope.scheduler_attempt_id is None:
        return scope
    if scope.scheduler_run_id is None or scope.scheduler_attempt_id is None:
        raise PaidScopeBlocked("attempt_owner_lost", "Run and attempt must be supplied together")
    row = connection.execute(
        """SELECT r.details_json,a.details_json attempt_details,a.attempt_number FROM scheduler_runs r
           JOIN scheduler_run_attempts a ON a.scheduler_run_id=r.id
           WHERE r.id=? AND a.id=? AND r.status='running' AND a.status='running'""",
        (scope.scheduler_run_id, scope.scheduler_attempt_id),
    ).fetchone()
    if row is None:
        raise PaidScopeBlocked("attempt_owner_lost", "Scheduler attempt is no longer active")
    details = _details(row["details_json"])
    attempt = _details(row["attempt_details"])
    owner = details.get("owner", {})
    identity = details.get("identity", {})
    if not isinstance(owner, dict) or not isinstance(identity, dict) or not isinstance(attempt.get("owner"), dict):
        raise PaidScopeBlocked("attempt_owner_lost", "Malformed frozen owner")
    token = owner.get("token")
    scan_id = details.get("scan_id")
    if (details.get("contract_version") != "durable-run-v1"
            or owner.get("attempt_id") != scope.scheduler_attempt_id
            or owner.get("attempt_number") != row["attempt_number"]
            or not isinstance(token, str) or not token
            or not isinstance(scan_id, str) or not scan_id
            or attempt.get("owner", {}).get("token") != token
            or attempt.get("scan_id") != scan_id or attempt.get("identity") != identity
            or (scope.scheduler_owner_token is not None and scope.scheduler_owner_token != token)
            or (scope.scheduler_scan_id is not None and scope.scheduler_scan_id != scan_id)):
        raise PaidScopeBlocked("attempt_owner_lost", "Frozen scheduler owner fence changed")
    if identity.get("purpose") == "history" and scope.purpose != "history":
        raise PaidScopeBlocked("paid_scope_mismatch", "History work must retain history quota")
    for field in (
        "activation_id",
        "roster_snapshot_id",
        "roster_snapshot_hash",
        "business_day",
    ):
        frozen = identity.get(field)
        if frozen is not None and getattr(scope, field) not in (None, frozen):
            raise PaidScopeBlocked("paid_scope_mismatch", "Paid roster differs from the frozen run")
    for field in ("uid", "platform", "account_id", "identity_id", "content_id"):
        frozen = identity.get(field)
        if frozen is not None and getattr(scope, field) not in (None, frozen):
            raise PaidScopeBlocked("identity_conflict", "Paid target differs from the frozen run")
    return replace(scope, scheduler_owner_token=token, scheduler_scan_id=scan_id)


def assert_paid_scope_owner(connection: sqlite3.Connection) -> None:
    """Fence local application of a response; this is not a network/member gate."""
    checked = _assert_scheduler_owner(connection, _SCOPE.get())
    _SCOPE.set(checked)


def renew_paid_owner_lease(connection: sqlite3.Connection, scope: PaidScope, *, at: str) -> None:
    """Schema20 network admission cannot renew an expired or replaced owner."""
    if connection.execute("PRAGMA user_version").fetchone()[0] != 20:
        return
    if scope.scheduler_run_id is None and scope.scheduler_attempt_id is None:
        return  # Existing manual/member guard remains independently authoritative.
    checked = _assert_scheduler_owner(connection, scope)
    from .durable_runs import DurableClaim, LostOwnership, heartbeat

    row = connection.execute("SELECT attempt_number FROM scheduler_run_attempts WHERE id=?",
                             (checked.scheduler_attempt_id,)).fetchone()
    if (row is None or checked.scheduler_run_id is None or checked.scheduler_attempt_id is None
            or checked.scheduler_owner_token is None or checked.scheduler_scan_id is None):
        raise PaidScopeBlocked("attempt_owner_lost", "Paid lease has no frozen owner")
    try:
        heartbeat(connection, DurableClaim(checked.scheduler_run_id, checked.scheduler_attempt_id,
                  int(row[0]), checked.scheduler_owner_token, checked.scheduler_scan_id), now=at)
    except LostOwnership as error:
        raise PaidScopeBlocked("attempt_owner_lost", str(error)) from error


def freeze_scope(
    connection: sqlite3.Connection, *, content_id: int | None,
    account_id: int | None, stage: str, scope: PaidScope | None = None,
) -> PaidScope:
    value = scope or _SCOPE.get()
    value = _assert_scheduler_owner(connection, value)
    active_scope = _SCOPE.get()
    if (active_scope.scheduler_run_id, active_scope.scheduler_attempt_id) == (
            value.scheduler_run_id, value.scheduler_attempt_id):
        _SCOPE.set(replace(
            active_scope, scheduler_owner_token=value.scheduler_owner_token,
            scheduler_scan_id=value.scheduler_scan_id,
        ))
    if content_id is not None:
        row = connection.execute(
            """SELECT i.id identity_id,i.account_id,i.uid,i.platform,c.raw_account_uid
               FROM content_items c JOIN account_platform_identities i
                 ON i.account_id=c.account_id AND i.platform=c.platform WHERE c.id=?""",
            (content_id,),
        ).fetchone()
    else:
        row = connection.execute(
            """SELECT id identity_id,account_id,uid,platform,NULL raw_account_uid
               FROM account_platform_identities WHERE account_id=?""", (account_id,),
        ).fetchone()
    if row is None:
        raise PaidScopeBlocked("identity_unresolved", "Paid target has no managed platform identity")
    if row["raw_account_uid"] and row["uid"] and row["raw_account_uid"] != row["uid"]:
        raise PaidScopeBlocked("identity_conflict", "Content author conflicts with managed identity")
    if value.identity_id is not None and value.identity_id != row["identity_id"]:
        raise PaidScopeBlocked("member_scope_changed", "Paid target identity changed after claim")
    if (value.uid is not None and value.uid != row["uid"]) or (
            value.platform is not None and value.platform != row["platform"]):
        raise PaidScopeBlocked("identity_conflict", "Paid target UID/platform changed after claim")
    try:
        activation = value.activation_id
        if "source_family" in {
            column["name"]
            for column in connection.execute(
                "PRAGMA table_info(account_roster_snapshots)"
            )
        }:
            if activation is None:
                from .profile_activations import activation_at

                active_activation = activation_at(connection, now_utc())
                if active_activation is None:
                    if connection.execute(
                        "SELECT 1 FROM account_roster_snapshots LIMIT 1"
                    ).fetchone() is None:
                        raise RosterError(
                            "roster_not_ready",
                            "No complete roster has been accepted",
                        )
                    raise RosterError(
                        "roster_activation_required",
                        "Paid work requires an effective acquisition activation",
                    )
                activation = int(active_activation["activation_id"])
            member = require_active_member(
                connection,
                int(row["identity_id"]),
                value.roster_snapshot_id,
                value.roster_snapshot_hash,
                require_uid=content_id is None,
                activation=activation,
            )
        else:
            member = require_active_member(
                connection, int(row["identity_id"]), value.roster_snapshot_id,
                value.roster_snapshot_hash, require_uid=content_id is None,
            )
    except RosterError as exc:
        raise PaidScopeBlocked(exc.code, str(exc)) from exc
    category = {
        "discovery": "reconcile", "detail": "detail",
        "media_source_refresh": "detail", "metrics": "metrics", "comments": "comments",
    }.get(stage)
    if category is None:
        raise PaidScopeBlocked("unknown_paid_purpose", "Unknown paid stage")
    if value.purpose is not None:
        category = value.purpose
    frozen = replace(
        value, purpose=value.purpose or category, category=category,
        activation_id=activation,
        roster_snapshot_id=int(member["roster_snapshot_id"]),
        roster_snapshot_hash=str(member["roster_snapshot_hash"]),
        identity_id=int(row["identity_id"]), account_id=int(row["account_id"]),
        content_id=content_id, uid=row["uid"], platform=row["platform"],
    )
    return _assert_scheduler_owner(connection, frozen)


def budget_summary(
    connection: sqlite3.Connection, *, at: str | None = None,
    exclude_usage_id: int | None = None,
) -> dict[str, Any]:
    day = budget_day(at or now_utc())
    categories = {key: 0 for key in CATEGORY_MICROUSD}
    buckets = {key: 0 for key in BUDGET_BUCKET_MICROUSD}
    total = pending = unclassified = 0
    unknown_count = unknown_amount = 0
    unknown_day_count = unknown_day_amount = 0
    unverified_count = unverified_amount = 0
    unverified_day_count = unverified_day_amount = 0
    for row in connection.execute(
        "SELECT * FROM provider_usage WHERE lower(provider)='tikhub' AND currency='USD'"
    ):
        if row["id"] == exclude_usage_id:
            continue
        details = _details(row["details_json"])
        amount = micro_usd(row["amount"])
        usage_day = str(details.get("budget_day") or budget_day(str(row["recorded_at"])))
        if details.get("state") == "billing_unknown":
            unknown_count += 1
            unknown_amount += amount
            if usage_day == day:
                unknown_day_count += 1
                unknown_day_amount += amount
        if details.get("state") == "charged_unverified":
            unverified_count += 1
            unverified_amount += amount
            if usage_day == day:
                unverified_day_count += 1
                unverified_day_amount += amount
        if usage_day != day:
            continue
        total += amount
        category = details.get("category")
        if category in categories:
            categories[category] += amount
        else:
            unclassified += amount
        bucket = details.get("budget_bucket")
        if bucket not in buckets:
            # There is deliberately no independent paid repair allowance.
            # Legacy/history rows still consume the bucket of the operation
            # that was actually sent; a compensation sequence must never
            # disappear into the zero-dollar bookkeeping bucket.
            if str(row["operation"]) in DISCOVERY_OPERATIONS:
                bucket = "discovery"
            elif (
                str(row["operation"]) in PRICES_MICROUSD
                or category in {"detail", "metrics", "comments"}
            ):
                bucket = "metrics"
            else:
                bucket = None
        if bucket in buckets:
            buckets[str(bucket)] += amount
        if details.get("state") in {"reserved", "sent", "billing_unknown"}:
            pending += amount
    return {
        "policy_version": POLICY_VERSION, "budget_day": day,
        "limit_microusd": GLOBAL_MICROUSD, "total_microusd": total,
        "pending_microusd": pending, "legacy_unclassified_microusd": unclassified,
        "categories_microusd": categories, "category_limits_microusd": dict(CATEGORY_MICROUSD),
        "buckets_microusd": buckets,
        "bucket_limits_microusd": dict(BUDGET_BUCKET_MICROUSD),
        "automatic_limit_microusd": AUTOMATIC_MICROUSD,
        "remaining_microusd": max(0, GLOBAL_MICROUSD - total),
        "billing_unknown": {
            "unresolved_count": unknown_count,
            "unresolved_microusd": unknown_amount,
            "budget_day_count": unknown_day_count,
            "budget_day_microusd": unknown_day_amount,
        },
        "charged_unverified": {
            "count": unverified_count,
            "amount_microusd": unverified_amount,
            "budget_day_count": unverified_day_count,
            "budget_day_microusd": unverified_day_amount,
            "provider_bill_verified": False,
        },
        "circuit": circuit_state(connection),
    }


def circuit_state(connection: sqlite3.Connection) -> dict[str, Any] | None:
    return fault_state(connection, scope_kind="provider_hard")


def record_circuit(
    connection: sqlite3.Connection,
    *,
    reason: str,
    usage_id: int | None,
    at: str,
    state_evidence: Mapping[str, Any] | None = None,
) -> dict[str, Any]:
    fault_class = {
        "provider_balance_blocked": "balance",
        "provider_auth_blocked": "application_auth",
        "provider_outage": "provider_outage",
        "provider_global_quota": "global_quota",
    }.get(reason)
    if fault_class is None:
        raise ValueError("only provider-hard failures may open the provider circuit")
    return record_fault_state(
        connection,
        scope_kind="provider_hard",
        fault_class=fault_class,
        reason=reason,
        usage_id=usage_id,
        at=at,
        state_evidence=state_evidence,
    )


def authorize_recovery_probe(
    connection: sqlite3.Connection, *, authorization_ref: str, operation: str,
    at: str, scope_kind: str = "provider_hard",
) -> dict[str, Any]:
    """Explicit operator action: authorize exactly one priced request for 15 minutes."""
    _require_transaction(connection)
    if scope_kind != "provider_hard":
        raise PaidScopeBlocked(
            "recovery_fault_domain_not_probeable",
            "Paid probes are restricted to provider-hard faults",
        )
    current = circuit_state(connection)
    if (not authorization_ref.strip() or operation not in PRICES_MICROUSD
            or not current or not current.get("open") or not current.get("generation")
            or current.get("scope_kind") != "provider_hard"
            or current.get("fault_class") not in PROBEABLE_PROVIDER_FAULTS
            or current.get("probe_eligible") is not True
            or not current.get("state_fingerprint")):
        raise PaidScopeBlocked("recovery_not_authorized", "An open circuit and explicit priced probe authorization are required")
    if current.get("contract_version") == "provider-fault-v2-legacy-pending":
        current = record_fault_state(
            connection,
            scope_kind="provider_hard",
            fault_class=str(current["fault_class"]),
            reason=str(current["reason"]),
            usage_id=(
                int(current["usage_id"])
                if type(current.get("usage_id")) is int
                else None
            ),
            at=at,
            state_evidence=dict(current["state_evidence"]),
        )
    for row in connection.execute("SELECT details_json FROM scheduler_runs WHERE job_id=?", (PROBE_JOB,)):
        pending = _details(row["details_json"])
        if pending.get("circuit_generation") == current["generation"]:
            raise PaidScopeBlocked(
                "recovery_probe_consumed",
                "This provider-hard generation already has its one permitted probe",
            )
        if (pending.get("state") in {"authorized", "reserved", "sent"}
                and _time(pending["expires_at"]) >= _time(at)):
            raise PaidScopeBlocked("recovery_probe_in_flight", "A recovery authorization is already outstanding")
    return _write_receipt(connection, PROBE_JOB, {
        "contract_version": "tikhub-recovery-probe-v2", "state": "authorized",
        "authorization_ref": authorization_ref, "operation": operation,
        "scope_kind": "provider_hard",
        "fault_class": current["fault_class"],
        "state_fingerprint": current["state_fingerprint"],
        "circuit_generation": current["generation"], "authorized_at": at,
        "expires_at": (_time(at) + timedelta(minutes=15)).isoformat(timespec="seconds"),
        "price_microusd": PRICES_MICROUSD[operation], "usage_id": None,
    }, at, status="partial")


def recovery_completion_deferred() -> bool:
    """Whether the current probe caller owns final materialization settlement."""

    return _DEFER_RECOVERY_COMPLETION.get()


@contextmanager
def circuit_recovery_probe(
    probe_id: int, *, defer_completion: bool = False
) -> Iterator[None]:
    """No timer uses this scope; the ledger verifies the persisted one-shot permission."""
    if type(probe_id) is not int or probe_id <= 0:
        raise PaidScopeBlocked("recovery_not_authorized", "A persisted probe ID is required")
    if type(defer_completion) is not bool:
        raise PaidScopeBlocked(
            "recovery_not_authorized", "Probe completion mode must be explicit"
        )
    defer_token = _DEFER_RECOVERY_COMPLETION.set(
        _DEFER_RECOVERY_COMPLETION.get() or defer_completion
    )
    token = _SCOPE.set(replace(_SCOPE.get(), recovery_probe_id=probe_id))
    try:
        yield
    finally:
        _SCOPE.reset(token)
        _DEFER_RECOVERY_COMPLETION.reset(defer_token)


def _check_probe(
    connection: sqlite3.Connection, *, scope: PaidScope, operation: str, at: str,
    usage_id: int | None,
) -> dict[str, Any]:
    row = connection.execute(
        "SELECT details_json FROM scheduler_runs WHERE id=? AND job_id=?",
        (scope.recovery_probe_id, PROBE_JOB),
    ).fetchone()
    proof = _details(row["details_json"]) if row is not None else {}
    circuit = circuit_state(connection) or {}
    expected_state = "authorized" if usage_id is None else "reserved"
    if (proof.get("contract_version") != "tikhub-recovery-probe-v2"
            or proof.get("state") != expected_state or proof.get("operation") != operation
            or proof.get("usage_id") != usage_id or not proof.get("authorization_ref")
            or proof.get("scope_kind") != "provider_hard"
            or proof.get("fault_class") != circuit.get("fault_class")
            or proof.get("state_fingerprint") != circuit.get("state_fingerprint")
            or not circuit.get("open") or proof.get("circuit_generation") != circuit.get("generation")
            or _time(proof["authorized_at"]) > _time(at) or _time(proof["expires_at"]) < _time(at)):
        raise PaidScopeBlocked("recovery_not_authorized", "Recovery proof is expired, consumed or for another circuit")
    return proof


def mark_probe_reserved(
    connection: sqlite3.Connection, *, scope: PaidScope, operation: str,
    usage_id: int, at: str,
) -> None:
    if scope.recovery_probe_id is None:
        return
    proof = _check_probe(connection, scope=scope, operation=operation, at=at, usage_id=None)
    proof.update(state="reserved", usage_id=usage_id, reserved_at=at)
    connection.execute(
        "UPDATE scheduler_runs SET status='running',completed_at=NULL,details_json=? WHERE id=?",
        (json.dumps(proof, sort_keys=True), scope.recovery_probe_id),
    )


def mark_probe_sent(
    connection: sqlite3.Connection, *, scope: PaidScope, operation: str,
    usage_id: int, at: str,
) -> None:
    if scope.recovery_probe_id is None:
        return
    proof = _check_probe(connection, scope=scope, operation=operation, at=at, usage_id=usage_id)
    proof.update(state="sent", sent_at=at)
    connection.execute(
        "UPDATE scheduler_runs SET details_json=? WHERE id=?",
        (json.dumps(proof, sort_keys=True), scope.recovery_probe_id),
    )


def finish_recovery_probe(
    connection: sqlite3.Connection, *, usage_id: int, succeeded: bool, at: str,
    raw_response_id: int | None = None, reason: str | None = None,
) -> None:
    usage = connection.execute("SELECT * FROM provider_usage WHERE id=?", (usage_id,)).fetchone()
    metadata = _details(usage["details_json"]) if usage is not None else {}
    probe_id = metadata.get("recovery_probe_id")
    if probe_id is None:
        return
    row = connection.execute(
        "SELECT details_json FROM scheduler_runs WHERE id=? AND job_id=?", (probe_id, PROBE_JOB),
    ).fetchone()
    proof = _details(row["details_json"]) if row is not None else {}
    if proof.get("usage_id") == usage_id and proof.get("state") in {"succeeded", "failed"}:
        return
    if proof.get("usage_id") != usage_id or proof.get("state") not in {"reserved", "sent"}:
        raise PaidScopeBlocked("recovery_not_authorized", "Probe settlement does not own its reservation")
    if succeeded:
        raw = connection.execute(
            "SELECT * FROM provider_raw_responses WHERE id=?", (raw_response_id,),
        ).fetchone()
        if (proof["state"] != "sent" or metadata.get("state") != "completed"
                or usage is None or usage["request_attempts"] != 1 or raw is None
                or str(raw["provider"]).lower() != "tikhub" or raw["operation"] != proof["operation"]
                or not 200 <= int(raw["http_status"] or 0) < 300):
            raise PaidScopeBlocked("recovery_not_verified", "Recovery requires a settled, successful live response")
    proof.update(state="succeeded" if succeeded else "failed", completed_at=at,
                 raw_response_id=raw_response_id, reason=reason)
    connection.execute(
        "UPDATE scheduler_runs SET status=?,completed_at=?,details_json=? WHERE id=?",
        ("succeeded" if succeeded else "failed", at, json.dumps(proof, sort_keys=True), probe_id),
    )
    if succeeded:
        resolve_fault_state(
            connection,
            scope_kind="provider_hard",
            at=at,
            expected_generation=str(proof["circuit_generation"]),
            expected_fingerprint=str(proof["state_fingerprint"]),
            evidence_id=probe_id,
            fault_class=str(proof["fault_class"]),
        )


def authorize_incident_budget(
    connection: sqlite3.Connection,
    *,
    approval_ref: str,
    owner: str,
    business_day: str,
    bucket: str,
    approved_total_usd: Any,
    approved_bucket_usd: Any,
    expires_at: str,
    at: str,
) -> dict[str, Any]:
    """Persist one explicit Beijing-day authorization above automatic caps."""

    _require_transaction(connection)
    if not approval_ref.strip() or not owner.strip():
        raise PaidScopeBlocked(
            "incident_authorization_invalid", "Incident approval and owner are required"
        )
    if bucket not in {"discovery", "metrics"}:
        raise PaidScopeBlocked(
            "incident_authorization_invalid", "Incident approval has an invalid bucket"
        )
    total_limit = micro_usd(approved_total_usd)
    bucket_limit = micro_usd(approved_bucket_usd)
    if (
        budget_day(at) != business_day
        or _time(expires_at) <= _time(at)
        or budget_day(expires_at) != business_day
        or not AUTOMATIC_MICROUSD < total_limit <= GLOBAL_MICROUSD
        or not BUDGET_BUCKET_MICROUSD[bucket] < bucket_limit <= GLOBAL_MICROUSD
    ):
        raise PaidScopeBlocked(
            "incident_authorization_invalid",
            "Incident approval must bind the current day and explicit 50-100 dollar limits",
        )
    return _write_receipt(
        connection,
        INCIDENT_AUTH_JOB,
        {
            "contract_version": "provider-budget-incident-v1",
            "approval_ref": approval_ref,
            "owner": owner,
            "provider": "tikhub",
            "business_day": business_day,
            "bucket": bucket,
            "approved_total_microusd": total_limit,
            "approved_bucket_microusd": bucket_limit,
            "authorized_at": at,
            "expires_at": expires_at,
        },
        at,
    )


def _incident_limits(
    connection: sqlite3.Connection,
    *,
    scope: PaidScope,
    bucket: str,
    at: str,
) -> tuple[int, int]:
    authorization_id = scope.incident_authorization_id
    if authorization_id is None:
        raise PaidScopeBlocked(
            "incident_authorization_required",
            "An explicit incident authorization is required above automatic caps",
        )
    row = connection.execute(
        """SELECT status,details_json FROM scheduler_runs
           WHERE id=? AND job_id=?""",
        (authorization_id, INCIDENT_AUTH_JOB),
    ).fetchone()
    proof = _details(row["details_json"]) if row is not None else {}
    authorized_at = proof.get("authorized_at")
    expires_at = proof.get("expires_at")
    try:
        time_valid = (
            isinstance(authorized_at, str)
            and isinstance(expires_at, str)
            and _time(authorized_at) <= _time(at) < _time(expires_at)
        )
    except (BudgetBlocked, TypeError, ValueError):
        time_valid = False
    if (
        row is None
        or row["status"] != "succeeded"
        or proof.get("contract_version") != "provider-budget-incident-v1"
        or proof.get("provider") != "tikhub"
        or proof.get("business_day") != budget_day(at)
        or proof.get("bucket") != bucket
        or not proof.get("approval_ref")
        or not proof.get("owner")
        or not time_valid
        or type(proof.get("approved_total_microusd")) is not int
        or type(proof.get("approved_bucket_microusd")) is not int
    ):
        raise PaidScopeBlocked(
            "incident_authorization_invalid",
            "Incident authorization is missing, expired or for another scope",
        )
    return (
        int(proof["approved_total_microusd"]),
        int(proof["approved_bucket_microusd"]),
    )


def record_compensation_gap(
    connection: sqlite3.Connection,
    *,
    original_usage_id: int,
    paid_scope_identity: str,
    operation: str,
    local_replay_exhausted: bool,
    raw_unrecoverable: bool,
    business_gap_due: bool,
    at: str,
) -> dict[str, Any]:
    """Record the three non-monetary prerequisites for a sequence-1 purchase."""

    _require_transaction(connection)
    if (
        not _positive_int(original_usage_id)
        or not paid_scope_identity
        or operation not in PRICES_MICROUSD
        or local_replay_exhausted is not True
        or raw_unrecoverable is not True
        or business_gap_due is not True
    ):
        raise PaidScopeBlocked(
            "compensation_gap_invalid",
            "Compensation requires exhausted replay, unrecoverable raw and a due gap",
        )
    return _write_receipt(
        connection,
        COMPENSATION_GAP_JOB,
        {
            "contract_version": "provider-compensation-gap-v1",
            "provider": "tikhub",
            "original_usage_id": original_usage_id,
            "paid_scope_identity": paid_scope_identity,
            "operation": operation,
            "local_replay_exhausted": True,
            "raw_unrecoverable": True,
            "business_gap_due": True,
            "recorded_at": at,
        },
        at,
    )


def authorize_compensation(
    connection: sqlite3.Connection,
    *,
    original_usage_id: int,
    paid_scope_identity: str,
    operation: str,
    reason: str,
    owner: str,
    gap_evidence_id: int,
    expires_at: str,
    at: str,
) -> dict[str, Any]:
    """Authorize one sequence+1 request for one unresolved original send."""

    _require_transaction(connection)
    if not paid_scope_identity or not reason.strip() or not owner.strip():
        raise PaidScopeBlocked(
            "compensation_authorization_invalid",
            "Compensation requires an original identity, reason and owner",
        )
    original = connection.execute(
        "SELECT * FROM provider_usage WHERE id=?", (original_usage_id,)
    ).fetchone()
    details = _details(original["details_json"]) if original is not None else {}
    settlement = details.get("billing_reconciliation")
    settlement_id = (
        settlement.get("receipt_run_id")
        if isinstance(settlement, Mapping)
        else details.get("settlement_receipt_id")
    )
    settlement_row = (
        connection.execute(
            "SELECT job_id,status,details_json FROM scheduler_runs WHERE id=?",
            (settlement_id,),
        ).fetchone()
        if _positive_int(settlement_id)
        else None
    )
    settlement_proof = (
        _details(settlement_row["details_json"])
        if settlement_row is not None
        else {}
    )
    terminal_settlement = bool(
        settlement_row is not None
        and settlement_row["status"] == "succeeded"
        and settlement_row["job_id"]
        in {"operator_billing_settlement:tikhub", SETTLEMENT_TERMINAL_JOB}
        and settlement_proof.get("usage_id") == original_usage_id
        and (
            details.get("state") == "charged_unverified"
            or (
                details.get("state") == "failed"
                and isinstance(settlement, Mapping)
                and settlement.get("outcome") == "billed"
            )
        )
    )
    gap_row = connection.execute(
        "SELECT status,details_json FROM scheduler_runs WHERE id=? AND job_id=?",
        (gap_evidence_id, COMPENSATION_GAP_JOB),
    ).fetchone()
    gap = _details(gap_row["details_json"]) if gap_row is not None else {}
    valid_gap = bool(
        gap_row is not None
        and gap_row["status"] == "succeeded"
        and gap.get("contract_version") == "provider-compensation-gap-v1"
        and gap.get("original_usage_id") == original_usage_id
        and gap.get("paid_scope_identity") == paid_scope_identity
        and gap.get("operation") == operation
        and gap.get("local_replay_exhausted") is True
        and gap.get("raw_unrecoverable") is True
        and gap.get("business_gap_due") is True
    )
    if (
        original is None
        or str(original["provider"]).lower() != "tikhub"
        or str(original["operation"]) != operation
        or not terminal_settlement
        or not isinstance(settlement_id, int)
        or not valid_gap
        or details.get("paid_scope_identity") != paid_scope_identity
        or _time(expires_at) <= _time(at)
        or budget_day(expires_at) != budget_day(at)
    ):
        raise PaidScopeBlocked(
            "compensation_authorization_invalid",
            "Original settlement, identity, operation or expiry is not eligible",
        )
    for auth_row in connection.execute(
        "SELECT id,details_json FROM scheduler_runs WHERE job_id=?",
        (COMPENSATION_AUTH_JOB,),
    ):
        prior = _details(auth_row["details_json"])
        if prior.get("original_usage_id") != original_usage_id:
            continue
        consumed = connection.execute(
            "SELECT 1 FROM scheduler_runs WHERE job_id=? LIMIT 1",
            (f"{COMPENSATION_USE_JOB}{int(auth_row['id'])}",),
        ).fetchone()
        if consumed is not None:
            raise PaidScopeBlocked(
                "compensation_authorization_consumed",
                "The original request already consumed its only compensation",
            )
        try:
            still_valid = _time(str(prior.get("expires_at"))) >= _time(at)
        except (BudgetBlocked, TypeError, ValueError):
            still_valid = False
        if still_valid:
            raise PaidScopeBlocked(
                "compensation_authorization_invalid",
                "An unexpired compensation authorization already exists",
            )
    return _write_receipt(
        connection,
        COMPENSATION_AUTH_JOB,
        {
            "contract_version": "provider-compensation-authorization-v1",
            "provider": "tikhub",
            "original_usage_id": original_usage_id,
            "settlement_receipt_id": int(settlement_id),
            "gap_evidence_id": gap_evidence_id,
            "paid_scope_identity": paid_scope_identity,
            "operation": operation,
            "next_sequence": 1,
            "reason": reason,
            "owner": owner,
            "authorized_at": at,
            "expires_at": expires_at,
            "max_requests": 1,
            "price_microusd": PRICES_MICROUSD.get(operation),
        },
        at,
    )


def consume_compensation_authorization(
    connection: sqlite3.Connection,
    *,
    authorization_id: int | None,
    paid_scope_identity: str,
    sequence: int,
    operation: str,
    at: str,
) -> dict[str, Any]:
    """Atomically consume the only allowed sequence+1 authorization."""

    _require_transaction(connection)
    if authorization_id is None:
        raise PaidScopeBlocked(
            "compensation_authorization_required",
            "Paid compensation requires an explicit authorization",
        )
    row = connection.execute(
        """SELECT status,details_json FROM scheduler_runs
           WHERE id=? AND job_id=?""",
        (authorization_id, COMPENSATION_AUTH_JOB),
    ).fetchone()
    proof = _details(row["details_json"]) if row is not None else {}
    original = connection.execute(
        "SELECT details_json FROM provider_usage WHERE id=?",
        (proof.get("original_usage_id"),),
    ).fetchone()
    original_details = _details(original["details_json"]) if original is not None else {}
    settlement = original_details.get("billing_reconciliation")
    settlement_row = connection.execute(
        "SELECT job_id,status,details_json FROM scheduler_runs WHERE id=?",
        (proof.get("settlement_receipt_id"),),
    ).fetchone()
    settlement_proof = (
        _details(settlement_row["details_json"])
        if settlement_row is not None
        else {}
    )
    gap_row = connection.execute(
        "SELECT status,details_json FROM scheduler_runs WHERE id=? AND job_id=?",
        (proof.get("gap_evidence_id"), COMPENSATION_GAP_JOB),
    ).fetchone()
    gap = _details(gap_row["details_json"]) if gap_row is not None else {}
    original_id = proof.get("original_usage_id")
    terminal_source = bool(
        settlement_row is not None
        and settlement_row["status"] == "succeeded"
        and settlement_row["job_id"]
        in {"operator_billing_settlement:tikhub", SETTLEMENT_TERMINAL_JOB}
        and settlement_proof.get("usage_id") == original_id
        and (
            original_details.get("state") == "charged_unverified"
            or (
                original_details.get("state") == "failed"
                and isinstance(settlement, Mapping)
                and settlement.get("outcome") == "billed"
                and settlement.get("receipt_run_id")
                == proof.get("settlement_receipt_id")
            )
        )
        and gap_row is not None
        and gap_row["status"] == "succeeded"
        and gap.get("contract_version") == "provider-compensation-gap-v1"
        and gap.get("original_usage_id") == original_id
        and gap.get("paid_scope_identity") == paid_scope_identity
        and gap.get("operation") == operation
        and gap.get("local_replay_exhausted") is True
        and gap.get("raw_unrecoverable") is True
        and gap.get("business_gap_due") is True
    )
    try:
        time_valid = (
            isinstance(proof.get("authorized_at"), str)
            and isinstance(proof.get("expires_at"), str)
            and _time(str(proof["authorized_at"])) <= _time(at)
            < _time(str(proof["expires_at"]))
        )
    except (BudgetBlocked, TypeError, ValueError):
        time_valid = False
    if (
        row is None
        or row["status"] != "succeeded"
        or proof.get("contract_version")
        != "provider-compensation-authorization-v1"
        or proof.get("paid_scope_identity") != paid_scope_identity
        or proof.get("operation") != operation
        or proof.get("next_sequence") != sequence
        or sequence != 1
        or proof.get("max_requests") != 1
        or proof.get("price_microusd") != PRICES_MICROUSD.get(operation)
        or not terminal_source
        or original_details.get("paid_scope_identity") != paid_scope_identity
        or not time_valid
    ):
        raise PaidScopeBlocked(
            "compensation_authorization_invalid",
            "Compensation authorization is missing, stale or for another request",
        )
    for consumed_row in connection.execute(
        """SELECT details_json FROM scheduler_runs
           WHERE job_id LIKE ?""",
        (f"{COMPENSATION_USE_JOB}%",),
    ):
        consumed = _details(consumed_row["details_json"])
        if consumed.get("original_usage_id") == original_id:
            raise PaidScopeBlocked(
                "compensation_authorization_consumed",
                "The original request already consumed its only compensation",
            )
    consumption = {
        "contract_version": "provider-compensation-consumption-v1",
        "authorization_id": authorization_id,
        "original_usage_id": int(proof["original_usage_id"]),
        "paid_scope_identity": paid_scope_identity,
        "operation": operation,
        "sequence": sequence,
        "consumed_at": at,
    }
    try:
        connection.execute(
            """INSERT INTO scheduler_runs(
                   job_id,scheduled_for,status,started_at,completed_at,details_json)
               VALUES (?,'1970-01-01T00:00:00Z','succeeded',?,?,?)""",
            (
                f"{COMPENSATION_USE_JOB}{authorization_id}",
                at,
                at,
                json.dumps(consumption, sort_keys=True),
            ),
        )
    except sqlite3.IntegrityError as error:
        raise PaidScopeBlocked(
            "compensation_authorization_consumed",
            "Compensation authorization has already been consumed",
        ) from error
    return consumption


def classify_legacy_transport_fault(
    connection: sqlite3.Connection, *, expected_receipt_id: int, actor: str, at: str,
) -> dict[str, Any]:
    """Correct a proven legacy request fault without resolving its billing.

    Old writers incorrectly promoted a failed HTTP read to provider-wide
    downtime. Classification appends evidence; the usage, attempt, slot retry
    guard and any genuine provider faults remain untouched.
    """
    _require_transaction(connection)
    if not actor.strip() or not _positive_int(expected_receipt_id):
        raise ValueError("transport classification requires actor and exact legacy receipt")
    legacy = _legacy_provider_circuit(connection)
    if (
        legacy is None or legacy.get("receipt_id") != expected_receipt_id
        or legacy.get("open") is not True
        or legacy.get("contract_version") != "provider-circuit-v1"
        or str(legacy.get("provider", "")).lower() != "tikhub"
        or legacy.get("reason") != "transport_error"
        or not _positive_int(legacy.get("usage_id"))
    ):
        raise ValueError("legacy circuit is not the exact transport-only fault")
    scope = {"scope_kind": "provider_hard", "provider": "tikhub"}
    for existing in _v2_fault_states(connection, scope):
        if (existing.get("state_evidence", {}).get("legacy_circuit_receipt_id")
                == expected_receipt_id):
            return existing
    usage = connection.execute(
        "SELECT * FROM provider_usage WHERE id=?", (legacy["usage_id"],),
    ).fetchone()
    details = _details(usage["details_json"]) if usage is not None else {}
    if (
        usage is None or str(usage["provider"]).lower() != "tikhub"
        or usage["operation"] not in PRICES_MICROUSD or usage["request_attempts"] != 1
        or details.get("state") != "billing_unknown"
        or details.get("error_code") != "transport_error"
        or not _positive_int(details.get("slot_id"))
        or not _positive_int(details.get("attempt_number"))
    ):
        raise ValueError("transport classification lacks original uncertain request")
    attempt = connection.execute(
        "SELECT a.*,s.provider AS slot_provider,s.last_error_code AS slot_guard "
        "FROM fetch_attempts a JOIN fetch_slots s ON s.id=a.slot_id "
        "WHERE a.slot_id=? AND a.attempt_number=?",
        (details["slot_id"], details["attempt_number"]),
    ).fetchone()
    if (
        attempt is None or str(attempt["slot_provider"]).lower() != "tikhub"
        or not attempt["response_finished_at"] or attempt["http_status"] is not None
        or attempt["error_code"] != "transport_error"
        or attempt["slot_guard"] != "billing_unknown_retry_blocked"
    ):
        raise ValueError("uncertain request must retain its exact attempt and retry guard")
    identity = details.get("paid_scope_identity")
    if identity:
        hold = fault_state(connection, scope_kind="paid_identity_hold", paid_identity=identity)
        if not hold or hold.get("open") is not True:
            raise ValueError("uncertain paid identity must remain quarantined")
    evidence = {
        "legacy_circuit_receipt_id": expected_receipt_id,
        "original_usage_id": int(usage["id"]),
        "original_usage_sha256": hashlib.sha256(str(usage["details_json"]).encode()).hexdigest(),
        "original_attempt_id": int(attempt["id"]),
        "slot_id": int(details["slot_id"]),
        "billing_state": "billing_unknown",
        "slot_retry_guard": "billing_unknown_retry_blocked",
        "actor": actor.strip(),
    }
    fault_class = "legacy_transport_request_classified"
    fingerprint = hashlib.sha256(json.dumps(evidence, sort_keys=True).encode()).hexdigest()
    return _write_receipt(connection, _fault_job_id(scope, fault_class), {
        "contract_version": "provider-fault-v2", **scope,
        "fault_class": fault_class, "open": False,
        "generation": f"legacy-transport:{expected_receipt_id}",
        "state_fingerprint": fingerprint, "state_evidence": evidence,
        "reason": "transport_request_fault_not_provider_outage",
        "classified_at": at, "usage_id": int(usage["id"]),
        "probe_eligible": False,
    }, at)


def _legacy_transport_diagnostic_allowed(
    connection: sqlite3.Connection, *, circuit: Mapping[str, Any],
    scope: PaidScope, operation: str, at: str,
) -> bool:
    """Diagnose a proven legacy transport fault without recovering its circuit.

    Old writers opened a provider-wide circuit for IncompleteRead. Its legacy
    projection is deliberately still closed to ordinary callers. Only a fully
    authorized diagnostic member can pass it, with the original usage/attempt
    evidence intact, and never in the presence of any actual provider hard fault.
    Called at reservation and again at the final send boundary.
    """
    if (
        circuit.get("contract_version") != "provider-fault-v2-legacy-pending"
        or circuit.get("fault_class") != "legacy_unclassified"
        or circuit.get("reason") != "transport_error"
        or any(
            state.get("open") is True
            for state in _v2_fault_states(connection, {
                "scope_kind": "provider_hard", "provider": "tikhub",
            })
        )
    ):
        return False
    legacy = _legacy_provider_circuit(connection)
    if (
        legacy is None or legacy.get("open") is not True
        or legacy.get("contract_version") != "provider-circuit-v1"
        or str(legacy.get("provider", "")).lower() != "tikhub"
        or _legacy_provider_fault_projection(legacy) != circuit
        or not _positive_int(legacy.get("usage_id"))
    ):
        return False
    usage = connection.execute(
        "SELECT * FROM provider_usage WHERE id=?", (legacy["usage_id"],),
    ).fetchone()
    if (
        usage is None or str(usage["provider"]).lower() != "tikhub"
        or usage["operation"] not in PRICES_MICROUSD
        or usage["request_attempts"] != 1
    ):
        return False
    details = _details(usage["details_json"])
    if (
        details.get("state") != "billing_unknown"
        or details.get("error_code") != "transport_error"
        or not _positive_int(details.get("slot_id"))
        or not _positive_int(details.get("attempt_number"))
    ):
        return False
    attempt = connection.execute(
        """SELECT a.*, s.provider AS slot_provider FROM fetch_attempts a
           JOIN fetch_slots s ON s.id=a.slot_id
           WHERE a.slot_id=? AND a.attempt_number=?""",
        (details["slot_id"], details["attempt_number"]),
    ).fetchone()
    if (
        attempt is None or str(attempt["slot_provider"]).lower() != "tikhub"
        or not attempt["response_finished_at"]
        or attempt["http_status"] is not None
        or attempt["error_code"] != "transport_error"
    ):
        return False
    from .transport_authority import authorize_transport_fault_diagnostic

    return authorize_transport_fault_diagnostic(
        connection, scope=scope, operation=operation, at=at,
    )


def check_reservation(
    connection: sqlite3.Connection, *, scope: PaidScope, operation: str,
    unit_price: Any, currency: str, at: str, exclude_usage_id: int | None = None,
) -> dict[str, Any]:
    expected = PRICES_MICROUSD.get(operation)
    amount = micro_usd(unit_price)
    if currency != "USD" or expected is None or amount != expected:
        raise PaidScopeBlocked("price_contract_unverified", "Operation has no matching verified TikHub USD price")
    require_storage_ready(connection)
    circuit = circuit_state(connection)
    operation_fault = fault_state(
        connection, scope_kind="operation", operation=operation
    )
    authorization_fault = None
    authorization_id = scope.identity_id or scope.account_id
    if authorization_id is not None:
        authorization_fault = fault_state(
            connection,
            scope_kind="authorization_hard",
            authorization_id=authorization_id,
        )
    if scope.recovery_probe_id is not None:
        _check_probe(connection, scope=scope, operation=operation, at=at, usage_id=exclude_usage_id)
    elif circuit and circuit.get("open") and not _legacy_transport_diagnostic_allowed(
        connection, circuit=circuit, scope=scope, operation=operation, at=at,
    ):
        raise PaidScopeBlocked("provider_circuit_open", "TikHub circuit requires authorized verified recovery")
    if operation_fault and operation_fault.get("open"):
        from .transport_authority import authorize_transport_fault_diagnostic

        # fault_state returns the latest open class, not all open classes. A
        # newer transport event must never hide an older rate/field-contract
        # fault. The only exception is one fully revalidated diagnostic member.
        operation_states = _v2_fault_states(connection, {
            "scope_kind": "operation", "provider": "tikhub", "operation": operation,
        })
        transport_only = all(
            fault.get("fault_class") == "transport"
            for fault in operation_states if fault.get("open") is True
        ) and operation_fault.get("fault_class") == "transport"
        if not transport_only or not authorize_transport_fault_diagnostic(
            connection, scope=scope, operation=operation, at=at,
        ):
            raise PaidScopeBlocked(
                "operation_blocked", "TikHub operation requires domain-specific recovery"
            )
    if authorization_fault and authorization_fault.get("open"):
        raise PaidScopeBlocked(
            "authorization_hard", "TikHub account authorization requires recovery"
        )
    summary = budget_summary(connection, at=at, exclude_usage_id=exclude_usage_id)
    next_total = int(summary["total_microusd"]) + amount
    if next_total > GLOBAL_MICROUSD:
        raise PaidScopeBlocked("global_budget_exhausted", "TikHub Beijing-day $100 ceiling reached")
    category = str(scope.category)
    if category not in CATEGORY_MICROUSD:
        raise PaidScopeBlocked("unknown_paid_purpose", "Missing fixed budget category")
    if category == "history":
        authorization_id = scope.compensation_authorization_id
        consumed = (
            connection.execute(
                "SELECT details_json FROM scheduler_runs WHERE job_id=? LIMIT 1",
                (f"{COMPENSATION_USE_JOB}{authorization_id}",),
            ).fetchone()
            if _positive_int(authorization_id)
            else None
        )
        consumption = _details(consumed["details_json"]) if consumed is not None else {}
        if (
            consumed is None
            or consumption.get("authorization_id") != authorization_id
            or consumption.get("sequence") != 1
            or scope.paid_sequence != 1
            or consumption.get("paid_scope_identity") != scope.paid_scope_identity
            or consumption.get("operation") != operation
        ):
            raise PaidScopeBlocked(
                "repair_budget_exhausted",
                "Historical spend requires a consumed sequence-1 authorization",
            )
    budget_bucket = "discovery" if operation in DISCOVERY_OPERATIONS else "metrics"
    bucket_limit = BUDGET_BUCKET_MICROUSD[budget_bucket]
    next_bucket = int(summary["buckets_microusd"][budget_bucket]) + amount
    if next_total > AUTOMATIC_MICROUSD or next_bucket > bucket_limit:
        try:
            incident_total, incident_bucket = _incident_limits(
                connection, scope=scope, bucket=budget_bucket, at=at
            )
        except PaidScopeBlocked as error:
            if error.error_code == "incident_authorization_required":
                code = (
                    "automatic_budget_exhausted"
                    if next_total > AUTOMATIC_MICROUSD
                    else f"{budget_bucket}_budget_exhausted"
                )
                raise PaidScopeBlocked(code, str(error)) from error
            raise
        if next_total > incident_total:
            raise PaidScopeBlocked(
                "incident_total_budget_exhausted",
                "TikHub incident total authorization ceiling reached",
            )
        if next_bucket > incident_bucket:
            raise PaidScopeBlocked(
                "incident_bucket_budget_exhausted",
                "TikHub incident bucket authorization ceiling reached",
            )
    scope_payload = asdict(scope)
    # These fields were added with the schema-19 dispatch fence.  Keep the
    # frozen schema-18 reservation shape byte-for-byte compatible while every
    # schema-19 paid scope still carries concrete values.
    for optional_profile_field in ("activation_id", "business_day"):
        if scope_payload[optional_profile_field] is None:
            del scope_payload[optional_profile_field]
    return {
        "policy_version": POLICY_VERSION, "state": "reserved",
        "budget_day": budget_day(at), "category": category,
        "budget_bucket": budget_bucket,
        "reserved_microusd": amount, "price_version": "tikhub-verified-2026-08-28",
        "scope": scope_payload, "reserved_at": at, "sent_at": None,
        "borrowed_from": {}, "borrowing_proofs": {},
        "validated_work_fingerprint": None,
        "recovery_probe_id": scope.recovery_probe_id,
    }
