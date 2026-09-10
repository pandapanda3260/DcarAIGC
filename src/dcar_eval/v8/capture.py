"""Idempotent capture slots, provider evidence and fail-closed budgets."""

from __future__ import annotations

import hashlib
import json
import math
import os
import sqlite3
from contextlib import nullcontext
from dataclasses import asdict, dataclass, replace
from datetime import datetime, time, timedelta, timezone
from pathlib import Path
from typing import Any, Callable, Dict, Literal, Mapping, Optional
from zoneinfo import ZoneInfo

from . import capture_singletons, operation_recovery, paid_drain, raw_archive, usage_settlements
from .capture_evidence_preflight import evidence_boundary, prepare_installed_evidence
from .paid_dispatch import (
    TERMINAL_EVENTS,
    close_dispatch_not_sent_in_transaction,
    dispatch_events,
    dispatch_id_for_usage,
    finish_dispatch_in_transaction,
    mark_dispatch_sent_in_transaction,
    reserve_dispatch_in_transaction,
    supports_dispatch_ledger,
)
from .storage import (
    DEFAULT_DB,
    PROJECT_ROOT,
    connect,
    now_utc,
    transaction,
    transaction_metrics_context,
)
from .provider_budget import (
    BudgetBlocked, PaidScope, PaidScopeBlocked, budget_day, check_reservation, freeze_scope,
    consume_compensation_authorization,
    evaluate_transport_operation_fault,
    fault_state,
    TIKHUB_NETWORK_SLOTS, _assert_scheduler_owner, finish_recovery_probe,
    mark_probe_reserved, mark_probe_sent, micro_usd, record_circuit,
    record_fault_state, require_storage_ready, resolve_fault_state,
    paid_dispatch_owner, recovery_completion_deferred, task_budget_id,
)
from .paid_identity import (
    PaidIdentityError,
    PaidRequestIdentity,
    build_paid_request_identity,
    validate_paid_request_identity,
)
from .raw_evidence import (
    PaidSendClaimHeld,
    RawEvidenceError,
    claim_paid_send,
    read_raw_evidence,
    read_raw_json,
    require_path_component,
    write_immutable_json_receipt,
    write_quarantine_evidence,
    write_zstd_raw_evidence,
)
from .provider_transport import CONTRACT_VERSION as TRANSPORT_CONTRACT_VERSION


RAW_ROOT = PROJECT_ROOT / "data" / "cache" / "v8" / "raw_responses"
SHANGHAI = ZoneInfo("Asia/Shanghai")
BILLING_UNKNOWN_SLOT_ERROR = "billing_unknown_retry_blocked"
BILLING_UNKNOWN_SLOT_MESSAGE = (
    "Previous TikHub request billing is unknown; manual reconciliation is required"
)


def _schema20(connection: sqlite3.Connection) -> bool:
    return connection.execute("PRAGMA user_version").fetchone()[0] in {20, 21}


class CaptureError(RuntimeError):
    """Provider or transport failure with explicit retry and billing semantics."""

    def __init__(
        self,
        message: str,
        *,
        retryable: bool,
        error_code: str,
        http_status: Optional[int] = None,
        billed: Optional[bool] = None,
        raw_response: Any = None,
        retry_after_seconds: Optional[float] = None,
        entity_bytes: bytes | None = None,
        transport_receipt: Mapping[str, Any] | None = None,
        transport_partial: bytes | None = None,
    ) -> None:
        super().__init__(message)
        self.retryable = retryable
        self.error_code = error_code
        self.http_status = http_status
        self.billed = billed
        self.raw_response = raw_response
        self.retry_after_seconds = retry_after_seconds
        self.entity_bytes = entity_bytes
        self.transport_receipt = (
            dict(transport_receipt) if transport_receipt is not None else None
        )
        self.transport_partial = transport_partial
        self.scan_attempt_count: Optional[int] = None


class SlotUnavailable(RuntimeError):
    """The requested idempotency slot is running, terminal or already successful."""

    def __init__(
        self,
        message: str,
        *,
        error_code: str = "slot_unavailable",
        slot_id: Optional[int] = None,
    ) -> None:
        super().__init__(message)
        self.error_code = error_code
        self.slot_id = slot_id


class TaskBudgetExhausted(BudgetBlocked):
    """The shared task-level amount ceiling has been exhausted."""

    error_code = "task_budget_exhausted"


class DailyAttemptQuotaExhausted(BudgetBlocked):
    """The budget batch has exhausted its Beijing-day attempt quota."""

    error_code = "budget_daily_quota_exhausted"


class RawResponseIntegrityError(RuntimeError):
    """A stored provider response is absent, unreadable or fails SHA-256 validation."""


def _raw_error_code(error: BaseException) -> str:
    """Planned raw expiry is not a corrupt store or a new send permission."""
    if isinstance(error, raw_archive.RawArchiveError) and str(error).startswith("raw_expired:"):
        return "raw_expired"
    return str(getattr(error, "error_code", "storage_hard"))


def require_tikhub_paid_dispatch_open(
    *,
    operation: str,
    db_path: Path = DEFAULT_DB,
    at: Optional[str] = None,
) -> None:
    """Fail fast before a paid TikHub parent or batch is created.

    This path-level check is an optimization for dispatchers.  The claim and
    send boundaries repeat the check in their own ``BEGIN IMMEDIATE``
    transactions, which remain authoritative when a drain races the caller.
    """

    paid_drain.require_paid_dispatch_open_path(
        db_path=db_path,
        provider="TikHub",
        operation=operation,
        at=at,
    )


@dataclass(frozen=True)
class ProviderResult:
    data: Any
    raw_response: Any
    http_status: int
    billed: bool
    entity_bytes: bytes | None = None
    transport_receipt: Mapping[str, Any] | None = None


@dataclass(frozen=True)
class SlotClaim:
    slot_id: int
    attempt_id: int
    attempt_number: int
    content_id: Optional[int]
    stage: str
    window_key: str
    provider: str
    adapter_version: str
    account_id: Optional[int] = None
    dispatch_scope: Optional[PaidScope] = None
    reserved_usage_id: Optional[int] = None
    reserved_unit_price: float = 0.0
    reserved_currency: str = ""
    dispatch_id: Optional[str] = None
    paid_scope_identity: Optional[str] = None
    paid_execution_identity: Optional[str] = None
    paid_sequence: int = 0
    request_transport: Mapping[str, Any] | None = None
    diagnostic_binding: Mapping[str, Any] | None = None
    diagnostic_member: Mapping[str, Any] | None = None
    paid_request_identity: PaidRequestIdentity | None = None
    authority_sha256: str | None = None
    request_batch_id: int | None = None
    singleton_batch: bool = False
    member_request_identities: tuple[PaidRequestIdentity, ...] = ()
    member_assignment_ids: tuple[int, ...] = ()
    compensation_issuance_ids: Mapping[str, int] | None = None
    compensation_proof_sha256: str | None = None


@dataclass(frozen=True)
class CaptureOutcome:
    slot_id: int
    attempt_id: int
    raw_response_id: int
    data: Any
    billed: bool
    amount: float
    currency: str


@dataclass(frozen=True)
class StoredRawResponse:
    slot_id: int
    raw_response_id: int
    provider: str
    operation: str
    value: Any
    http_status: Optional[int]
    captured_at: str
    sha256: str
    local_path: Path


def canonical_json_bytes(value: Any) -> bytes:
    return (
        json.dumps(
            value,
            ensure_ascii=False,
            sort_keys=True,
            separators=(",", ":"),
            allow_nan=False,
        )
        + "\n"
    ).encode("utf-8")


def _scrub_secrets(value: Any) -> Any:
    secret_names = {
        "authorization",
        "cookie",
        "set-cookie",
        "x-api-key",
        "api-key",
        "api_key",
        "access_token",
        "token",
    }
    if isinstance(value, dict):
        return {
            key: "[REDACTED]"
            if str(key).lower() in secret_names
            else _scrub_secrets(child)
            for key, child in value.items()
        }
    if isinstance(value, list):
        return [_scrub_secrets(child) for child in value]
    return value


def _atomic_bytes(path: Path, value: bytes) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    os.chmod(path.parent, 0o700)
    temporary = path.with_name(f".{path.name}.tmp")
    temporary.write_bytes(value)
    os.chmod(temporary, 0o600)
    temporary.replace(path)


def _utc_iso(value: datetime) -> str:
    return (
        value.astimezone(timezone.utc)
        .isoformat(timespec="seconds")
        .replace("+00:00", "Z")
    )


def _shanghai_day_utc_bounds(recorded_at: str) -> tuple[str, str]:
    try:
        instant = datetime.fromisoformat(recorded_at.replace("Z", "+00:00"))
    except ValueError as exc:
        raise ValueError("recorded_at must be an ISO timestamp") from exc
    if instant.tzinfo is None:
        instant = instant.replace(tzinfo=timezone.utc)
    local_day = instant.astimezone(SHANGHAI).date()
    start = datetime.combine(local_day, time.min, SHANGHAI).astimezone(timezone.utc)
    return _utc_iso(start), _utc_iso(start + timedelta(days=1))


def _validate_task_budget(
    *,
    budget_id: Optional[str],
    task_id: Optional[str],
    task_max_amount: Optional[float],
    provider: str,
    operation: str,
) -> None:
    if task_id is None and task_max_amount is None:
        if budget_id is not None and budget_id.startswith("task-"):
            raise BudgetBlocked("task budget requires task_id and task_max_amount")
        return
    if task_id is None or task_max_amount is None:
        raise ValueError("task_id and task_max_amount must be provided together")
    if not task_id.strip():
        raise ValueError("task_id must not be blank")
    if not math.isfinite(float(task_max_amount)) or float(task_max_amount) <= 0:
        raise ValueError("task_max_amount must be a finite positive number")
    if budget_id is None:
        raise ValueError("task budget requires budget_id")
    expected_budget_id = task_budget_id(task_id, provider, operation)
    if budget_id != expected_budget_id:
        raise BudgetBlocked(
            "task budget id does not match task, provider, and operation"
        )


def load_succeeded_raw_response(
    *,
    stage: str,
    window_key: str,
    db_path: Path = DEFAULT_DB,
    content_id: Optional[int] = None,
    account_id: Optional[int] = None,
    operation: Optional[str] = None,
) -> StoredRawResponse:
    """Read the newest raw response for a successful slot without any provider call."""

    if (content_id is None) == (account_id is None):
        raise ValueError("exactly one of content_id and account_id is required")
    target_column = "content_id" if content_id is not None else "account_id"
    target_value = content_id if content_id is not None else account_id
    operation_clause = " AND pr.operation=?" if operation is not None else ""
    parameters: list[Any] = [target_value, stage, window_key]
    if operation is not None:
        parameters.append(operation)
    with connect(db_path) as connection:
        if _schema20(connection):
            from .capture_compensation import replay_sequence

            compensation = replay_sequence(connection, content_id=content_id, account_id=account_id,
                stage=stage, window_key=window_key, operation=operation)
            if compensation is not None:
                operation_clause += " AND pr.paid_scope_identity=? AND pr.sequence=?"
                parameters.extend(compensation)
        row = connection.execute(
            f"""
            SELECT fs.id slot_id, pr.*, pr.id raw_response_id, pr.provider, pr.operation,
                   pr.local_path, pr.sha256, pr.byte_size, pr.http_status, pr.captured_at
            FROM fetch_slots fs
            JOIN fetch_attempts fa ON {capture_singletons.attempt_slot_sql(connection)}=fs.id
            JOIN provider_raw_responses pr ON pr.fetch_attempt_id=fa.id
            WHERE fs.{target_column}=? AND fs.stage=? AND fs.window_key=?
              AND fs.status='succeeded'{operation_clause}
            ORDER BY fa.attempt_number DESC, pr.id DESC
            LIMIT 1
            """,
            parameters,
        ).fetchone()
        if row is None:
            raise SlotUnavailable("successful slot has no matching raw response")
        resolved, value = _read_verified_raw_response(row, connection=connection)
    return StoredRawResponse(
        slot_id=int(row["slot_id"]),
        raw_response_id=int(row["raw_response_id"]),
        provider=str(row["provider"]),
        operation=str(row["operation"]),
        value=value,
        http_status=int(row["http_status"]) if row["http_status"] is not None else None,
        captured_at=str(row["captured_at"]),
        sha256=str(row["sha256"]),
        local_path=resolved,
    )


def _read_verified_raw_response(
    row: sqlite3.Row, *, connection: sqlite3.Connection | None = None,
) -> tuple[Path, Any]:
    local_path = Path(str(row["local_path"]))
    resolved = local_path if local_path.is_absolute() else PROJECT_ROOT / local_path
    try:
        if connection is not None and _schema20(connection):
            raw_id = int(row["raw_response_id"] if "raw_response_id" in row.keys() else row["id"])
            entity = raw_archive.read_response_entity(connection, raw_id)
            value = json.loads(entity.decode("utf-8", "strict"))
        else:
            value = read_raw_json(
                resolved,
                expected_stored_sha256=str(row["sha256"]),
                expected_stored_size=int(row["byte_size"]),
            )
    except (OSError, RawEvidenceError, UnicodeDecodeError, json.JSONDecodeError) as exc:
        if _raw_error_code(exc) == "raw_expired":
            raise CaptureError("Stored raw has expired under retention policy", retryable=False,
                               error_code="raw_expired", billed=False) from exc
        raise RawResponseIntegrityError(
            f"stored raw response failed integrity validation: {resolved}: {exc}"
        ) from exc
    return resolved, value


def ensure_content_slot(
    connection: sqlite3.Connection,
    *,
    content_id: int,
    stage: str,
    window_key: str,
    provider: str,
    adapter_version: str,
) -> int:
    row = connection.execute(
        """
        SELECT id FROM fetch_slots
        WHERE content_id=? AND stage=? AND window_key=?
        """,
        (content_id, stage, window_key),
    ).fetchone()
    if row is not None:
        return int(row["id"])
    captured_at = now_utc()
    cursor = connection.execute(
        """
        INSERT INTO fetch_slots(
            content_id, stage, window_key, provider, adapter_version,
            status, created_at, updated_at
        ) VALUES (?, ?, ?, ?, ?, 'pending', ?, ?)
        """,
        (
            content_id,
            stage,
            window_key,
            provider,
            adapter_version,
            captured_at,
            captured_at,
        ),
    )
    if cursor.lastrowid is None:
        raise RuntimeError("fetch slot insert returned no id")
    return int(cursor.lastrowid)


def ensure_account_slot(
    connection: sqlite3.Connection,
    *,
    account_id: int,
    stage: str,
    window_key: str,
    provider: str,
    adapter_version: str,
) -> int:
    row = connection.execute(
        """
        SELECT id FROM fetch_slots
        WHERE account_id=? AND stage=? AND window_key=?
        """,
        (account_id, stage, window_key),
    ).fetchone()
    if row is not None:
        return int(row["id"])
    captured_at = now_utc()
    cursor = connection.execute(
        """
        INSERT INTO fetch_slots(
            account_id, stage, window_key, provider, adapter_version,
            status, created_at, updated_at
        ) VALUES (?, ?, ?, ?, ?, 'pending', ?, ?)
        """,
        (
            account_id,
            stage,
            window_key,
            provider,
            adapter_version,
            captured_at,
            captured_at,
        ),
    )
    if cursor.lastrowid is None:
        raise RuntimeError("account fetch slot insert returned no id")
    return int(cursor.lastrowid)


def recover_stale_fetch_slots(
    *,
    db_path: Path = DEFAULT_DB,
    stale_after_seconds: int = 600,
    current_time: Optional[datetime] = None,
) -> Dict[str, int]:
    """Release capture slots abandoned by an interrupted service process."""
    if stale_after_seconds <= 0:
        raise ValueError("stale_after_seconds must be positive")
    current = (current_time or datetime.now(timezone.utc)).astimezone(timezone.utc)
    cutoff = current - timedelta(seconds=stale_after_seconds)
    captured_at = current.isoformat(timespec="seconds").replace("+00:00", "Z")
    cutoff_at = cutoff.isoformat(timespec="seconds").replace("+00:00", "Z")
    with connect(db_path) as connection, transaction(connection):
        rows = connection.execute(
            """
            SELECT id FROM fetch_slots
            WHERE status='running' AND COALESCE(started_at,updated_at) < ?
            ORDER BY id
            """,
            (cutoff_at,),
        ).fetchall()
        if rows:
            for row in rows:
                usages = connection.execute(
                    """SELECT * FROM provider_usage WHERE lower(provider)='tikhub'
                       AND json_extract(details_json,'$.slot_id')=?
                       AND json_extract(details_json,'$.state') IN ('reserved','sent','billing_unknown')""",
                    (row["id"],),
                ).fetchall()
                for usage in usages:
                    dispatch_id = dispatch_id_for_usage(connection, int(usage["id"]))
                    if _release_unsent_usage(
                            connection, usage_id=int(usage["id"]),
                            reason="interrupted_before_dispatch", at=captured_at):
                        if dispatch_id is not None:
                            closed = close_dispatch_not_sent_in_transaction(
                                connection,
                                dispatch_id,
                                reason="interrupted_before_dispatch",
                                created_at=captured_at,
                            )
                            if closed is None:
                                raise RuntimeError(
                                    "Schema 19 stale reservation close was not retained"
                                )
                        continue
                    metadata = json.loads(usage["details_json"])
                    metadata.update(state="billing_unknown", recovered_at=captured_at,
                                    recovery_reason="dispatch_or_billing_not_proven")
                    connection.execute(
                        "UPDATE provider_usage SET details_json=? WHERE id=?",
                        (json.dumps(metadata, sort_keys=True), usage["id"]),
                    )
                    _finish_dispatch_if_open(
                        connection,
                        dispatch_id,
                        outcome="billing_unknown",
                        created_at=captured_at,
                        reason="dispatch_or_billing_not_proven",
                    )
                    record_fault_state(
                        connection,
                        scope_kind="paid_identity_hold",
                        fault_class="billing_unresolved",
                        reason="dispatch_or_billing_not_proven",
                        usage_id=int(usage["id"]),
                        at=captured_at,
                        paid_identity=str(
                            metadata.get("paid_scope_identity")
                            or f"slot:{int(row['id'])}"
                        ),
                    )
            connection.executemany(
                """
                UPDATE fetch_slots SET status='retryable_failed',
                    last_error_code='interrupted',
                    last_error_message='中断后待重试；已发送或计费未知的预占继续保留',
                    finished_at=?, updated_at=?
                WHERE id=? AND status='running'
                """,
                [(captured_at, captured_at, int(row["id"])) for row in rows],
            )
        guarded_slot_ids = _synchronize_billing_unknown_slot_guards(connection)
        if guarded_slot_ids:
            _record_billing_unknown_guard_sync(
                connection, slot_ids=guarded_slot_ids, recorded_at=captured_at
            )
    return {"stale_candidates": len(rows), "recovered": len(rows)}


def _synchronize_billing_unknown_slot_guards(
    connection: sqlite3.Connection,
) -> list[int]:
    """Materialize unresolved billing as an O(1) slot-level retry guard.

    The frozen ledger stores ``slot_id`` in unindexed JSON.  Scan it once
    during startup recovery, not inside every paid claim.  Original provider
    errors remain available on the attempt and usage rows for reconciliation.
    """
    slot_ids = [
        int(row["slot_id"])
        for row in connection.execute(
            """SELECT DISTINCT CAST(json_extract(details_json,'$.slot_id') AS INTEGER) slot_id
               FROM provider_usage
               WHERE lower(provider)='tikhub' AND json_valid(details_json)
                 AND json_type(details_json,'$.slot_id')='integer'
                 AND json_extract(details_json,'$.state')='billing_unknown'
               ORDER BY slot_id"""
        )
    ]
    changed: list[int] = []
    for slot_id in slot_ids:
        cursor = connection.execute(
            """UPDATE fetch_slots
               SET last_error_code=?,last_error_message=?
               WHERE id=? AND COALESCE(last_error_code,'')<>?""",
            (
                BILLING_UNKNOWN_SLOT_ERROR,
                BILLING_UNKNOWN_SLOT_MESSAGE,
                slot_id,
                BILLING_UNKNOWN_SLOT_ERROR,
            ),
        )
        if cursor.rowcount == 1:
            changed.append(slot_id)
    return changed


def _record_billing_unknown_guard_sync(
    connection: sqlite3.Connection, *, slot_ids: list[int], recorded_at: str
) -> None:
    details = {
        "contract_version": "tikhub-billing-unknown-guard-sync-v1",
        "changed_count": len(slot_ids),
        "slot_ids_sha256": hashlib.sha256(
            json.dumps(slot_ids, separators=(",", ":")).encode("utf-8")
        ).hexdigest(),
        "recorded_at": recorded_at,
    }
    parent = connection.execute(
        """INSERT INTO scheduler_runs(
               job_id,scheduled_for,status,started_at,completed_at,details_json)
           VALUES ('billing_unknown_guard_sync:tikhub',?,'succeeded',?,?,?)""",
        (recorded_at, recorded_at, recorded_at, json.dumps(details, sort_keys=True)),
    )
    if parent.lastrowid is None:
        raise RuntimeError("billing unknown guard sync receipt was not created")
    connection.execute(
        """INSERT INTO scheduler_run_attempts(
               scheduler_run_id,attempt_number,invocation_source,status,
               started_at,completed_at,details_json)
           VALUES (?,1,'scheduled','succeeded',?,?,?)""",
        (
            int(parent.lastrowid),
            recorded_at,
            recorded_at,
            json.dumps(details, sort_keys=True),
        ),
    )


def clear_billing_unknown_slot_guard_if_resolved(
    connection: sqlite3.Connection,
    *,
    slot_id: int,
    fallback_error_code: str,
    fallback_error_message: str,
) -> tuple[bool, int]:
    """Release only a derived guard whose complete ledger is now settled."""
    remaining = int(
        connection.execute(
            """SELECT COUNT(*) FROM provider_usage
               WHERE lower(provider)='tikhub' AND json_valid(details_json)
                 AND json_type(details_json,'$.slot_id')='integer'
                 AND CAST(json_extract(details_json,'$.slot_id') AS INTEGER)=?
                 AND json_extract(details_json,'$.state')='billing_unknown'""",
            (slot_id,),
        ).fetchone()[0]
    )
    from .account_cleanup import archived_slot_holds

    remaining += archived_slot_holds(connection).get(slot_id, {}).get("billing_unknown", 0)
    if remaining:
        return False, remaining
    paid_identities = {
        str(row["paid_identity"])
        for row in connection.execute(
            """SELECT json_extract(details_json,'$.paid_scope_identity') paid_identity
               FROM provider_usage
               WHERE lower(provider)='tikhub' AND json_valid(details_json)
                 AND CAST(json_extract(details_json,'$.slot_id') AS INTEGER)=?""",
            (slot_id,),
        )
        if row["paid_identity"]
    }
    paid_identities.add(f"slot:{slot_id}")
    resolved_at = now_utc()
    for paid_identity in paid_identities:
        current_fault = fault_state(
            connection,
            scope_kind="paid_identity_hold",
            paid_identity=paid_identity,
            fault_class="billing_unresolved",
        )
        if current_fault is None or current_fault.get("open") is not True:
            continue
        resolve_fault_state(
            connection,
            scope_kind="paid_identity_hold",
            paid_identity=paid_identity,
            fault_class="billing_unresolved",
            expected_generation=str(current_fault["generation"]),
            expected_fingerprint=str(current_fault["state_fingerprint"]),
            at=resolved_at,
            evidence_id=f"slot:{slot_id}:settled",
        )
    latest = connection.execute(
        """SELECT error_code,error_message FROM fetch_attempts
           WHERE slot_id=? ORDER BY attempt_number DESC LIMIT 1""",
        (slot_id,),
    ).fetchone()
    restored_code = (
        (str(latest["error_code"]) if latest and latest["error_code"] else None)
        or fallback_error_code
    )
    restored_message = (
        (str(latest["error_message"]) if latest and latest["error_message"] else None)
        or fallback_error_message
    )
    cursor = connection.execute(
        """UPDATE fetch_slots SET last_error_code=?,last_error_message=?
           WHERE id=? AND last_error_code=?""",
        (
            restored_code,
            restored_message,
            slot_id,
            BILLING_UNKNOWN_SLOT_ERROR,
        ),
    )
    return cursor.rowcount == 1, 0


def claim_content_slot(
    *,
    db_path: Path,
    content_id: int,
    stage: str,
    window_key: str,
    provider: str,
    adapter_version: str,
    allow_terminal_retry: bool = False,
) -> SlotClaim:
    with connect(db_path) as connection, transaction(connection):
        slot_id = ensure_content_slot(
            connection,
            content_id=content_id,
            stage=stage,
            window_key=window_key,
            provider=provider,
            adapter_version=adapter_version,
        )
        row = connection.execute(
            "SELECT * FROM fetch_slots WHERE id=?", (slot_id,)
        ).fetchone()
        if row is None:
            raise RuntimeError("fetch slot disappeared")
        allowed = {"pending", "retryable_failed"}
        if allow_terminal_retry:
            allowed.add("terminal_failed")
        if row["status"] not in allowed:
            raise SlotUnavailable(
                f"slot {slot_id} is {row['status']}",
                error_code=str(row["last_error_code"] or "slot_unavailable"),
                slot_id=slot_id,
            )
        attempt_number = int(row["attempt_count"]) + 1
        started_at = now_utc()
        cursor = connection.execute(
            """
            INSERT INTO fetch_attempts(slot_id, attempt_number, request_started_at)
            VALUES (?, ?, ?)
            """,
            (slot_id, attempt_number, started_at),
        )
        if cursor.lastrowid is None:
            raise RuntimeError("fetch attempt insert returned no id")
        connection.execute(
            """
            UPDATE fetch_slots
            SET provider=?, adapter_version=?, status='running', attempt_count=?,
                started_at=?, finished_at=NULL,
                last_error_code=CASE WHEN last_error_code=? THEN last_error_code ELSE NULL END,
                last_error_message=CASE WHEN last_error_code=? THEN last_error_message ELSE NULL END,
                updated_at=?
            WHERE id=?
            """,
            (
                provider,
                adapter_version,
                attempt_number,
                started_at,
                BILLING_UNKNOWN_SLOT_ERROR,
                BILLING_UNKNOWN_SLOT_ERROR,
                started_at,
                slot_id,
            ),
        )
    return SlotClaim(
        slot_id=slot_id,
        attempt_id=int(cursor.lastrowid),
        attempt_number=attempt_number,
        content_id=content_id,
        stage=stage,
        window_key=window_key,
        provider=provider,
        adapter_version=adapter_version,
    )


def claim_account_slot(
    *,
    db_path: Path,
    account_id: int,
    stage: str,
    window_key: str,
    provider: str,
    adapter_version: str,
    allow_terminal_retry: bool = False,
) -> SlotClaim:
    with connect(db_path) as connection, transaction(connection):
        slot_id = ensure_account_slot(
            connection,
            account_id=account_id,
            stage=stage,
            window_key=window_key,
            provider=provider,
            adapter_version=adapter_version,
        )
        row = connection.execute(
            "SELECT * FROM fetch_slots WHERE id=?", (slot_id,)
        ).fetchone()
        if row is None:
            raise RuntimeError("account fetch slot disappeared")
        allowed = {"pending", "retryable_failed"}
        if allow_terminal_retry:
            allowed.add("terminal_failed")
        if row["status"] not in allowed:
            raise SlotUnavailable(
                f"slot {slot_id} is {row['status']}",
                error_code=str(row["last_error_code"] or "slot_unavailable"),
                slot_id=slot_id,
            )
        attempt_number = int(row["attempt_count"]) + 1
        started_at = now_utc()
        cursor = connection.execute(
            """
            INSERT INTO fetch_attempts(slot_id, attempt_number, request_started_at)
            VALUES (?, ?, ?)
            """,
            (slot_id, attempt_number, started_at),
        )
        if cursor.lastrowid is None:
            raise RuntimeError("account fetch attempt insert returned no id")
        connection.execute(
            """
            UPDATE fetch_slots
            SET provider=?, adapter_version=?, status='running', attempt_count=?,
                started_at=?, finished_at=NULL,
                last_error_code=CASE WHEN last_error_code=? THEN last_error_code ELSE NULL END,
                last_error_message=CASE WHEN last_error_code=? THEN last_error_message ELSE NULL END,
                updated_at=?
            WHERE id=?
            """,
            (
                provider,
                adapter_version,
                attempt_number,
                started_at,
                BILLING_UNKNOWN_SLOT_ERROR,
                BILLING_UNKNOWN_SLOT_ERROR,
                started_at,
                slot_id,
            ),
        )
    return SlotClaim(
        slot_id=slot_id,
        attempt_id=int(cursor.lastrowid),
        attempt_number=attempt_number,
        content_id=None,
        stage=stage,
        window_key=window_key,
        provider=provider,
        adapter_version=adapter_version,
        account_id=account_id,
    )


def mark_fetch_slot_terminal_failure(
    *,
    db_path: Path,
    slot_id: int,
    error_code: str,
    error_message: str,
) -> Dict[str, Any]:
    """Record a post-fetch business failure without rewriting provider facts."""

    if not error_code.strip():
        raise ValueError("error_code must not be blank")
    captured_at = now_utc()
    with connect(db_path) as connection, transaction(connection):
        row = connection.execute(
            "SELECT * FROM fetch_slots WHERE id=?", (slot_id,)
        ).fetchone()
        if row is None:
            raise RuntimeError(f"fetch slot does not exist: {slot_id}")
        if row["status"] not in {"succeeded", "terminal_failed"}:
            raise RuntimeError(
                f"fetch slot {slot_id} cannot become terminal from {row['status']}"
            )
        finished_at = str(row["finished_at"] or captured_at)
        connection.execute(
            """
            UPDATE fetch_slots
            SET status='terminal_failed',
                last_error_code=CASE WHEN last_error_code=? THEN last_error_code ELSE ? END,
                last_error_message=CASE WHEN last_error_code=? THEN last_error_message ELSE ? END,
                finished_at=?,updated_at=?
            WHERE id=?
            """,
            (
                BILLING_UNKNOWN_SLOT_ERROR,
                error_code,
                BILLING_UNKNOWN_SLOT_ERROR,
                error_message[:500],
                finished_at,
                captured_at,
                slot_id,
            ),
        )
    return {
        "slot_id": slot_id,
        "status": "terminal_failed",
        "error_code": error_code,
        "finished_at": finished_at,
    }


def mark_succeeded_fetch_slot_retryable_failure(
    *,
    db_path: Path,
    slot_id: int,
    error_code: str,
    error_message: str,
) -> Dict[str, Any]:
    """Reopen a successful fetch when only its derived materialization failed.

    The provider attempt and raw response remain the immutable evidence of the
    successful call.  This transition is deliberately narrower than the normal
    capture failure path: callers may only reopen a succeeded slot, and only
    for the shared discovery materializer failure handled by the writer.
    """

    if error_code != "derived_materialization_failed":
        raise ValueError(
            "error_code must be derived_materialization_failed"
        )
    captured_at = now_utc()
    with connect(db_path) as connection, transaction(connection):
        row = connection.execute(
            "SELECT * FROM fetch_slots WHERE id=?", (slot_id,)
        ).fetchone()
        if row is None:
            raise RuntimeError(f"fetch slot does not exist: {slot_id}")
        if row["status"] != "succeeded":
            raise RuntimeError(
                f"fetch slot {slot_id} cannot become retryable from {row['status']}"
            )
        finished_at = str(row["finished_at"] or captured_at)
        connection.execute(
            """
            UPDATE fetch_slots
            SET status='retryable_failed',
                last_error_code=CASE WHEN last_error_code=? THEN last_error_code ELSE ? END,
                last_error_message=CASE WHEN last_error_code=? THEN last_error_message ELSE ? END,
                finished_at=?,updated_at=?
            WHERE id=? AND status='succeeded'
            """,
            (
                BILLING_UNKNOWN_SLOT_ERROR,
                error_code,
                BILLING_UNKNOWN_SLOT_ERROR,
                error_message[:500],
                finished_at,
                captured_at,
                slot_id,
            ),
        )
    return {
        "slot_id": slot_id,
        "status": "retryable_failed",
        "error_code": error_code,
        "finished_at": finished_at,
    }


def activate_pilot_budget(
    budget_id: str,
    *,
    expected_unit_price: float,
    db_path: Path = DEFAULT_DB,
) -> None:
    with connect(db_path) as connection, transaction(connection):
        row = connection.execute(
            "SELECT * FROM provider_budget_batches WHERE id=?", (budget_id,)
        ).fetchone()
        if row is None:
            raise BudgetBlocked(f"budget {budget_id} does not exist")
        if row["status"] != "draft":
            raise BudgetBlocked(
                f"budget {budget_id} is {row['status']}, expected draft"
            )
        if abs(float(row["verified_unit_price"]) - expected_unit_price) > 1e-9:
            raise BudgetBlocked(
                "verified provider price does not match the approved price"
            )
        if not row["price_verified_at"]:
            raise BudgetBlocked("provider price has not been verified")
        connection.execute(
            "UPDATE provider_budget_batches SET status='pilot', updated_at=? WHERE id=?",
            (now_utc(), budget_id),
        )


def _reserve_budget(
    connection: sqlite3.Connection,
    *,
    budget_id: str,
    provider: str,
    operation: str,
    task_id: Optional[str] = None,
    task_max_amount: Optional[float] = None,
    dispatch_scope: Optional[PaidScope] = None,
) -> tuple[int, float, str]:
    _validate_task_budget(
        budget_id=budget_id,
        task_id=task_id,
        task_max_amount=task_max_amount,
        provider=provider,
        operation=operation,
    )
    row = connection.execute(
        "SELECT * FROM provider_budget_batches WHERE id=?", (budget_id,)
    ).fetchone()
    if row is None:
        raise BudgetBlocked(f"budget {budget_id} does not exist")
    if row["status"] not in {"pilot", "approved"}:
        raise BudgetBlocked(f"budget {budget_id} is {row['status']}")
    if row["provider"] != provider or row["operation"] != operation:
        raise BudgetBlocked("budget provider or operation mismatch")
    if task_id is not None:
        if task_max_amount is None:
            raise ValueError("task_id and task_max_amount must be provided together")
        if abs(float(row["max_amount"]) - float(task_max_amount)) > 1e-9:
            raise BudgetBlocked("task budget max_amount does not match runtime ceiling")
    unit_price = float(row["verified_unit_price"])
    # The task ceiling is the authoritative cross-operation stop. Check it
    # before per-operation and daily-attempt limits so callers can distinguish
    # an expected end-of-task stop from a broken budget contract.
    if task_id is not None and task_max_amount is not None:
        task_amount_micro = sum(
            micro_usd(item["amount"]) for item in connection.execute(
                "SELECT amount FROM provider_usage WHERE task_id=?", (task_id,)
            )
        )
        if task_amount_micro + micro_usd(unit_price) > micro_usd(task_max_amount):
            raise TaskBudgetExhausted("task amount ceiling reached")
    consumed_requests = int(row["consumed_requests"])
    consumed_amount = float(row["consumed_amount"])
    if consumed_requests >= int(row["max_billable_requests"]):
        raise BudgetBlocked("billable request ceiling reached")
    if micro_usd(consumed_amount) + micro_usd(unit_price) > micro_usd(row["max_amount"]):
        raise BudgetBlocked("amount ceiling reached")
    recorded_at = now_utc()
    day_start, day_end = _shanghai_day_utc_bounds(recorded_at)
    usage = connection.execute(
        """
        SELECT COALESCE(SUM(request_attempts), 0) attempts
        FROM provider_usage
        WHERE budget_batch_id=? AND recorded_at>=? AND recorded_at<?
        """,
        (budget_id, day_start, day_end),
    ).fetchone()
    daily_attempts = int(usage["attempts"])
    if daily_attempts >= int(row["daily_quota"]):
        raise DailyAttemptQuotaExhausted("daily attempt quota reached")
    total_attempts = int(
        connection.execute(
            "SELECT COALESCE(SUM(request_attempts), 0) FROM provider_usage WHERE budget_batch_id=?",
            (budget_id,),
        ).fetchone()[0]
    )
    if row["status"] == "pilot" and total_attempts >= int(row["pilot_size"]):
        raise BudgetBlocked("pilot sample is complete and awaits quality gate")
    reservation = {"state": "reserved"}
    if provider.lower() == "tikhub":
        if dispatch_scope is None:
            raise BudgetBlocked("TikHub reservation requires an accepted roster dispatch scope")
        from .capture_planning import require_send_route

        require_send_route(connection, scope=dispatch_scope, operation=operation, at=recorded_at)
        from .provider_budget import renew_paid_owner_lease

        renew_paid_owner_lease(connection, dispatch_scope, at=recorded_at)
        reservation = check_reservation(
            connection, scope=dispatch_scope, operation=operation,
            unit_price=unit_price, currency=str(row["currency"]), at=recorded_at,
        )
    cursor = connection.execute(
        """
        INSERT INTO provider_usage(
            task_id, budget_batch_id, provider, operation, request_attempts, billed_requests,
            currency, amount, recorded_at, details_json
        ) VALUES (?, ?, ?, ?, ?, 1, ?, ?, ?, ?)
        """,
        (
            task_id,
            budget_id,
            provider,
            operation,
            0 if dispatch_scope is not None else 1,
            row["currency"],
            unit_price,
            recorded_at,
            json.dumps(reservation, sort_keys=True),
        ),
    )
    if cursor.lastrowid is None:
        raise RuntimeError("provider usage insert returned no id")
    if dispatch_scope is not None:
        mark_probe_reserved(
            connection, scope=dispatch_scope, operation=operation,
            usage_id=int(cursor.lastrowid), at=recorded_at,
        )
    new_attempts = total_attempts + 1
    next_status = (
        "suspended"
        if row["status"] == "pilot" and new_attempts >= int(row["pilot_size"])
        else row["status"]
    )
    connection.execute(
        """
        UPDATE provider_budget_batches
        SET consumed_requests=consumed_requests+1,
            consumed_amount=ROUND(consumed_amount+?, 6), status=?, updated_at=?
        WHERE id=?
        """,
        (unit_price, next_status, recorded_at, budget_id),
    )
    return int(cursor.lastrowid), unit_price, str(row["currency"])


def _settle_budget(
    connection: sqlite3.Connection,
    *,
    usage_id: int,
    budget_id: str,
    unit_price: float,
    billed: Optional[bool],
    details: Dict[str, Any],
) -> None:
    row = connection.execute(
        "SELECT billed_requests,details_json FROM provider_usage WHERE id=?", (usage_id,)
    ).fetchone()
    if row is None:
        raise BudgetBlocked("Provider usage reservation is missing")
    metadata = json.loads(str(row["details_json"] or "{}"))
    metadata.update(details)
    if billed is None:
        metadata["state"] = "billing_unknown"
    details_json = json.dumps(
        metadata, ensure_ascii=False, sort_keys=True, separators=(",", ":")
    )
    if billed is None:
        connection.execute(
            "UPDATE provider_usage SET details_json=? WHERE id=?", (details_json, usage_id)
        )
        return
    if billed:
        connection.execute(
            "UPDATE provider_usage SET details_json=? WHERE id=?",
            (details_json, usage_id),
        )
        return
    if not row["billed_requests"]:
        return
    connection.execute(
        """
        UPDATE provider_usage
        SET billed_requests=0, amount=0, details_json=? WHERE id=?
        """,
        (details_json, usage_id),
    )
    connection.execute(
        """
        UPDATE provider_budget_batches
        SET consumed_requests=MAX(0, consumed_requests-1),
            consumed_amount=MAX(0, ROUND(consumed_amount-?, 6)), updated_at=?
        WHERE id=?
        """,
        (unit_price, now_utc(), budget_id),
    )


def raw_scope_identity(
    *,
    provider: str,
    slot_id: int,
    stage: str,
    window_key: str,
    attempt_number: int,
) -> str:
    """Return the deterministic evidence identity for a non-paid attempt."""

    return hashlib.sha256(
        canonical_json_bytes(
            {
                "provider": provider.lower(),
                "slot_id": slot_id,
                "stage": stage,
                "window_key": window_key,
                "attempt_number": attempt_number,
            }
        )
    ).hexdigest()


def _store_raw_response(
    connection: sqlite3.Connection,
    *,
    claim: SlotClaim,
    operation: str,
    value: Any,
    http_status: Optional[int],
    raw_root: Path,
    entity_bytes: bytes | None = None,
    transport_receipt: Mapping[str, Any] | None = None,
) -> int:
    captured_at = now_utc()
    schema20 = _schema20(connection)
    if claim.request_batch_id is not None:
        attempt = connection.execute("SELECT slot_id,request_batch_id FROM fetch_attempts WHERE id=?", (claim.attempt_id,)).fetchone()
        if not schema20 or attempt is None or tuple(attempt) != (None, claim.request_batch_id):
            raise RawEvidenceError("batch raw requires its exact shared request attempt")
        if claim.singleton_batch:
            capture_singletons.validate_raw_target(connection, batch_id=claim.request_batch_id,
                content_id=claim.content_id, account_id=claim.account_id)
        else:
            claim = replace(claim, content_id=None, account_id=None)
    if schema20 and claim.dispatch_scope is not None and (transport_receipt is None or entity_bytes is None):
        raise RawEvidenceError("schema20 paid raw requires actual entity bytes and transport evidence")
    if entity_bytes is None:
        # Fixture, derived and legacy adapter results have no HTTP byte
        # boundary.  Preserve their historical secret-scrubbed semantics, but
        # they cannot satisfy a live transport qualification receipt.
        entity_bytes = canonical_json_bytes(_scrub_secrets(value))
    else:
        try:
            json.loads(entity_bytes.decode("utf-8", "strict"))
        except (UnicodeDecodeError, json.JSONDecodeError) as exc:
            raise RawEvidenceError(
                "complete HTTP entity is not strict UTF-8 JSON"
            ) from exc
        # A semantic result may intentionally redact provider comment identity.
        # Exact transport bytes are still the raw source of truth and are bound
        # to their receipt instead of being compared to that projection.
        if transport_receipt is not None:
            _validate_complete_transport_receipt(
                transport_receipt,
                entity_bytes=entity_bytes,
                http_status=http_status,
            )
    provider_component = require_path_component(
        claim.provider.lower(), field="provider"
    )
    operation_component = require_path_component(operation, field="operation")
    target = f"batch-{claim.request_batch_id}" if claim.request_batch_id is not None else (
        str(claim.content_id)
        if claim.content_id is not None
        else f"account-{claim.account_id}"
    )
    is_paid_identity = claim.paid_scope_identity is not None
    paid_scope_identity = (
        claim.paid_scope_identity
        or raw_scope_identity(
            provider=claim.provider,
            slot_id=claim.slot_id,
            stage=claim.stage,
            window_key=claim.window_key,
            attempt_number=claim.attempt_number,
        )
    )
    sequence = claim.paid_sequence if is_paid_identity else 0 if schema20 else claim.attempt_number
    response_identity = hashlib.sha256(
        canonical_json_bytes(
            {
                "provider": claim.provider.lower(),
                "operation": operation,
                "paid_scope_identity": paid_scope_identity,
                "sequence": sequence,
            }
        )
    ).hexdigest()
    path = (
        raw_root
        / provider_component
        / target
        / operation_component
        / f"scope-{paid_scope_identity}-sequence-{sequence:04d}.json.zst"
    )
    if schema20:
        return _store_schema20_raw_response(connection, claim=claim, operation=operation,
            entity_bytes=entity_bytes, http_status=http_status, raw_root=raw_root,
            index_path=path.with_name(path.name.removesuffix(".json.zst") + ".response.json"),
            response_identity=response_identity, paid_scope_identity=paid_scope_identity,
            sequence=sequence if is_paid_identity else 0, captured_at=captured_at,
            transport_receipt=transport_receipt)
    receipt = write_zstd_raw_evidence(
        path,
        entity_bytes,
        provider=claim.provider,
        operation=operation,
        response_identity=response_identity,
        paid_scope_identity=paid_scope_identity,
        sequence=sequence,
        evidence_root=raw_root,
    )
    try:
        local_path = str(path.relative_to(PROJECT_ROOT))
    except ValueError:
        local_path = str(path)
    if claim.content_id is None:
        predicate = "content_id IS NULL AND account_id=?"
        target_parameters: tuple[Any, ...] = (claim.account_id,)
    else:
        predicate = "content_id=?"
        target_parameters = (claim.content_id,)
    rows = connection.execute(
        f"""
        SELECT * FROM provider_raw_responses
        WHERE {predicate} AND provider=? AND operation=? AND local_path=?
        ORDER BY id
        """,
        (*target_parameters, claim.provider, operation, local_path),
    ).fetchall()
    if rows:
        if len(rows) != 1:
            raise RawEvidenceError(
                "raw response identity maps to multiple database rows"
            )
        row = rows[0]
        if (
            str(row["sha256"]) != receipt.stored_sha256
            or int(row["byte_size"]) != receipt.stored_size
            or row["http_status"] != http_status
        ):
            raise RawEvidenceError(
                "raw response database receipt conflicts with immutable evidence"
            )
        _read_verified_raw_response(row, connection=connection)
        return int(row["id"])
    try:
        cursor = connection.execute(
            """
            INSERT INTO provider_raw_responses(
                fetch_attempt_id, account_id, content_id, provider, operation, local_path,
                sha256, byte_size, http_status, captured_at, source
            ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, 'live')
            """,
            (
                claim.attempt_id,
                claim.account_id,
                claim.content_id,
                claim.provider,
                operation,
                local_path,
                receipt.stored_sha256,
                receipt.stored_size,
                http_status,
                captured_at,
            ),
        )
    except sqlite3.IntegrityError as exc:
        raise RawEvidenceError(
            "raw response database uniqueness conflicts with immutable evidence"
        ) from exc
    if cursor.lastrowid is None:
        raise RuntimeError("raw response insert returned no id")
    raw_response_id = int(cursor.lastrowid)
    retained = connection.execute(
        "SELECT * FROM provider_raw_responses WHERE id=?", (raw_response_id,)
    ).fetchone()
    if retained is None:
        raise RawEvidenceError("raw response insert was not retained")
    _read_verified_raw_response(retained, connection=connection)
    return raw_response_id


def _store_schema20_raw_response(connection: sqlite3.Connection, *, claim: SlotClaim,
        operation: str, entity_bytes: bytes, http_status: int | None, raw_root: Path,
        index_path: Path, response_identity: str, paid_scope_identity: str, sequence: int,
        captured_at: str, transport_receipt: Mapping[str, Any] | None) -> int:
    """Store one shared body and an immutable per-response recovery index.

    The index does not embed database-generated blob/response IDs: a transaction
    rollback may reuse those IDs for unrelated rows before recovery. Scope,
    attempt, entity and stored checksums provide the durable binding instead.
    """
    if (len(paid_scope_identity) != 64 or any(char not in "0123456789abcdef" for char in paid_scope_identity)
            or type(sequence) is not int or not 0 <= sequence <= 4):
        raise RawEvidenceError("schema20 response requires a valid scope and sequence")
    identity: dict[str, Any] = {"schema": "provider-response-index-v1",
        "response_identity": response_identity, "paid_scope_identity": paid_scope_identity,
        "sequence": sequence, "provider": claim.provider, "operation": operation,
        "fetch_attempt_id": claim.attempt_id, "account_id": claim.account_id,
        "request_batch_id": claim.request_batch_id,
        "content_id": claim.content_id, "http_status": http_status,
        "entity_sha256": hashlib.sha256(entity_bytes).hexdigest(), "entity_size": len(entity_bytes),
        "codec_version": raw_archive.CODEC_VERSION,
        "transport_receipt": dict(transport_receipt) if transport_receipt is not None else None}
    retained = connection.execute("SELECT * FROM provider_raw_responses WHERE paid_scope_identity=? AND sequence=?",
                                   (paid_scope_identity, sequence)).fetchone()
    if retained is not None:
        for field in ("provider", "operation", "fetch_attempt_id", "account_id", "content_id", "http_status"):
            if retained[field] != identity[field]:
                raise RawEvidenceError("raw scope already belongs to another response attempt")
        if raw_archive.read_response_entity(connection, retained["id"]) != entity_bytes:
            raise RawEvidenceError("raw scope already binds another entity")
        captured_at = str(retained["captured_at"])
    stored_at = captured_at
    if index_path.exists() or index_path.is_symlink():
        previous = read_raw_json(index_path)
        if not isinstance(previous, dict) or any(previous.get(key) != value for key, value in identity.items()):
            raise RawEvidenceError("immutable response index conflicts with response identity")
        captured_at = str(previous.get("captured_at"))
        stored_at = str(previous.get("raw_stored_at"))
        raw_archive.day_for_timestamp(captured_at)
        raw_archive.day_for_timestamp(stored_at)
        if retained is not None and (retained["captured_at"] != captured_at or retained["raw_stored_at"] != stored_at):
            raise RawEvidenceError("response index timestamps conflict with database evidence")
    blob_id = raw_archive.put_blob(connection, entity_bytes, live_root=raw_root / "blobs-v1", raw_stored_at=stored_at)
    blob = connection.execute("SELECT * FROM provider_raw_blobs WHERE id=?", (blob_id,)).fetchone()
    if blob is None:
        raise RawEvidenceError("stored blob registration disappeared")
    index = {**identity, "captured_at": captured_at, "raw_stored_at": stored_at,
             "blob_path": blob["hot_path"], "stored_sha256": blob["stored_sha256"], "stored_size": blob["stored_size"]}
    write_immutable_json_receipt(index_path, index, evidence_root=raw_root)
    if retained is None:
        inserted = connection.execute("""INSERT INTO provider_raw_responses(fetch_attempt_id,account_id,content_id,
            provider,operation,local_path,sha256,byte_size,http_status,captured_at,source,paid_scope_identity,sequence,
            raw_blob_id,raw_stored_at) VALUES(?,?,?,?,?,?,?,?,?,?,'live',?,?,?,?)""",
            (claim.attempt_id, claim.account_id, claim.content_id, claim.provider, operation, blob["hot_path"],
             blob["stored_sha256"], blob["stored_size"], http_status, captured_at, paid_scope_identity, sequence, blob_id, stored_at))
        if inserted.lastrowid is None:
            raise RawEvidenceError("raw response insert returned no id")
        raw_response_id = int(inserted.lastrowid)
    else:
        if retained["raw_blob_id"] != blob_id:
            raise RawEvidenceError("response database blob binding conflicts with recovery index")
        raw_response_id = int(retained["id"])
    if transport_receipt is not None:
        stored_receipt = _transport_receipt_with_storage(connection,
            raw_response_id=raw_response_id, transport_receipt=transport_receipt)
        assert stored_receipt is not None
        raw_archive.record_transport_receipt(connection, attempt_id=claim.attempt_id,
            receipt=stored_receipt, raw_response_id=raw_response_id)
    return raw_response_id


def replay_schema20_response_index(connection: sqlite3.Connection, *, index_path: Path,
                                   raw_root: Path) -> int:
    """Recover committed evidence after a C-transaction crash, without HTTP.

    This only restores response/blob/transport registration. It neither marks a
    failed slot successful nor settles billing; those need their own verified
    parser and settlement receipts. An unknown or replaced attempt fails closed.
    """
    if not _schema20(connection) or not connection.in_transaction:
        raise RawEvidenceError("response index replay requires a schema20 writer transaction")
    raw_root = raw_root.absolute()
    index_path = raw_archive._safe_existing(index_path)
    if not index_path.is_relative_to(raw_root):
        raise RawEvidenceError("response index is outside its declared raw root")
    index = read_raw_json(index_path)
    if not isinstance(index, dict) or index.get("schema") != "provider-response-index-v1":
        raise RawEvidenceError("unsupported response recovery index")
    row = connection.execute(f"""SELECT a.id,COALESCE(a.slot_id,s.id) slot_id,a.request_batch_id,a.attempt_number,s.account_id,s.content_id,
        s.stage,s.window_key,s.provider,s.adapter_version FROM fetch_attempts a
        JOIN fetch_slots s ON s.id={capture_singletons.attempt_slot_sql(connection, 'a')}
        WHERE a.id=?""", (index.get("fetch_attempt_id"),)).fetchone()
    attempt = dict(row) if row is not None else None
    if attempt is not None and attempt['request_batch_id'] is not None:
        if index.get('request_batch_id') != attempt['request_batch_id'] or not connection.execute(
            "SELECT 1 FROM fetch_request_executions x JOIN fetch_request_batches b ON b.id=x.batch_id "
            "WHERE x.fetch_attempt_id=? AND x.batch_id=? AND b.operation=? AND b.request_scope_identity=?",
            (attempt['id'], attempt['request_batch_id'], index.get('operation'), index.get('paid_scope_identity'))).fetchone():
            raise RawEvidenceError("batch response index has no exact execution")
        if index.get('content_id') is not None or index.get('account_id') is not None:
            capture_singletons.validate_raw_target(connection, batch_id=attempt['request_batch_id'],
                content_id=index.get('content_id'), account_id=index.get('account_id'))
        else:
            attempt.update(content_id=None, account_id=None)
    if attempt is None or any(attempt[key] != index.get(key) for key in ("account_id", "content_id", "provider")):
        raise RawEvidenceError("response recovery index attempt is missing or changed")
    identity = str(index.get("paid_scope_identity", ""))
    sequence = index.get("sequence")
    if (len(identity) != 64 or any(char not in "0123456789abcdef" for char in identity)
            or type(sequence) is not int or not 0 <= sequence <= 4):
        raise RawEvidenceError("response recovery index has invalid scope")
    operation = require_path_component(str(index.get("operation", "")), field="operation")
    provider = require_path_component(str(attempt["provider"]).lower(), field="provider")
    target = (f"batch-{attempt['request_batch_id']}" if attempt['request_batch_id'] is not None else
              str(attempt["content_id"]) if attempt["content_id"] is not None else f"account-{attempt['account_id']}")
    expected = raw_root / provider / target / operation / f"scope-{identity}-sequence-{sequence:04d}.response.json"
    if expected != index_path or index.get("codec_version") != raw_archive.CODEC_VERSION:
        raise RawEvidenceError("response recovery index path or codec conflicts")
    entity_hash = str(index.get("entity_sha256", ""))
    if len(entity_hash) != 64 or any(char not in "0123456789abcdef" for char in entity_hash):
        raise RawEvidenceError("response recovery index entity hash is invalid")
    blob = connection.execute("SELECT * FROM provider_raw_blobs WHERE entity_sha256=? AND codec_version=?",
                              (entity_hash, raw_archive.CODEC_VERSION)).fetchone()
    if blob is not None:
        if (index.get("blob_path") != blob["hot_path"] or index.get("stored_sha256") != blob["stored_sha256"]
                or index.get("stored_size") != blob["stored_size"] or index.get("entity_size") != blob["entity_size"]):
            raise RawEvidenceError("response recovery index conflicts with registered blob")
        try:
            entity = raw_archive.read_blob(connection, blob["id"])
        except raw_archive.RawArchiveError as error:
            if _raw_error_code(error) == "raw_expired":
                raise CaptureError("Stored raw has expired under retention policy", retryable=False,
                                   error_code="raw_expired", billed=False) from error
            raise
    else:
        blob_path = raw_root / "blobs-v1" / entity_hash[:2] / f"{entity_hash}.{raw_archive.CODEC_VERSION}.zst"
        if index.get("blob_path") != str(blob_path):
            raise RawEvidenceError("response recovery blob is outside the content-addressed root")
        entity = raw_archive._decode_blob({**index, "codec": "zstd"}, raw_archive._read_bytes(blob_path))
    claim = SlotClaim(slot_id=attempt["slot_id"], attempt_id=attempt["id"], attempt_number=attempt["attempt_number"],
        content_id=attempt["content_id"], account_id=attempt["account_id"], stage=attempt["stage"],
        window_key=attempt["window_key"], provider=attempt["provider"], adapter_version=attempt["adapter_version"],
        paid_scope_identity=identity, paid_sequence=sequence, request_batch_id=attempt['request_batch_id'],
        singleton_batch=attempt['request_batch_id'] is not None and
                        (attempt['content_id'] is not None or attempt['account_id'] is not None))
    return _store_raw_response(connection, claim=claim, operation=operation, value=json.loads(entity),
        http_status=index.get("http_status"), raw_root=raw_root, entity_bytes=entity,
        transport_receipt=index.get("transport_receipt"))


def _validate_complete_transport_receipt(
    receipt: Mapping[str, Any],
    *,
    entity_bytes: bytes,
    http_status: Optional[int],
) -> None:
    content_encoding = receipt.get("content_encoding")
    content_length = receipt.get("content_length")
    length_match = receipt.get("length_match")
    required_nonblank = (
        "transport_route_id",
        "route_generation",
        "http_stack",
        "request_host",
    )
    valid = (
        receipt.get("contract_version") == TRANSPORT_CONTRACT_VERSION
        and receipt.get("status") == "succeeded"
        and receipt.get("error_code") is None
        and receipt.get("json_parse_ok") is True
        and receipt.get("clean_eof") is True
        and receipt.get("http_status") == http_status
        and isinstance(http_status, int)
        and not isinstance(http_status, bool)
        and receipt.get("entity_bytes") == len(entity_bytes)
        and receipt.get("entity_sha256")
        == hashlib.sha256(entity_bytes).hexdigest()
        and content_encoding in {"identity", "gzip"}
        and length_match in {None, True}
        and (content_length is None or length_match is True)
        and (
            receipt.get("gzip_crc_ok") is True
            if content_encoding == "gzip"
            else receipt.get("gzip_crc_ok") is None
        )
        and receipt.get("zero_body") is (not entity_bytes)
        and all(
            isinstance(receipt.get(field), str)
            and bool(str(receipt.get(field)).strip())
            for field in required_nonblank
        )
    )
    if not valid:
        raise RawEvidenceError(
            "HTTP entity does not match the complete transport receipt"
        )


def _transport_receipt_with_storage(
    connection: sqlite3.Connection,
    *,
    raw_response_id: int,
    transport_receipt: Mapping[str, Any] | None,
) -> dict[str, Any] | None:
    if transport_receipt is None:
        return None
    row = connection.execute(
        "SELECT local_path,sha256,byte_size FROM provider_raw_responses WHERE id=?",
        (raw_response_id,),
    ).fetchone()
    if row is None:
        raise RawEvidenceError("transport raw response is missing after storage")
    resolved = Path(str(row["local_path"]))
    if not resolved.is_absolute():
        resolved = PROJECT_ROOT / resolved
    if _schema20(connection):
        entity = raw_archive.read_response_entity(connection, raw_response_id)
        entity_sha256, entity_size = hashlib.sha256(entity).hexdigest(), len(entity)
    else:
        loaded = read_raw_evidence(
            resolved,
            expected_stored_sha256=str(row["sha256"]),
            expected_stored_size=int(row["byte_size"]),
        )
        entity_sha256, entity_size = loaded.receipt.entity_sha256, loaded.receipt.entity_size
    receipt = dict(transport_receipt)
    if (
        receipt.get("entity_sha256") != entity_sha256
        or receipt.get("entity_bytes") != entity_size
    ):
        raise RawEvidenceError("stored raw entity does not match transport receipt")
    receipt.update(
        raw_response_id=raw_response_id,
        stored_path=str(row["local_path"]),
        stored_sha256=str(row["sha256"]),
        stored_bytes=int(row["byte_size"]),
    )
    return receipt


def _quarantine_transport_evidence(
    *,
    claim: SlotClaim,
    operation: str,
    raw_root: Path,
    partial: bytes | None,
    complete_entity: bytes | None,
    transport_receipt: Mapping[str, Any] | None,
) -> dict[str, Any] | None:
    if transport_receipt is None:
        return None
    receipt = dict(transport_receipt)
    evidence = partial if partial is not None else complete_entity
    provider_component = require_path_component(
        claim.provider.lower(), field="provider"
    )
    operation_component = require_path_component(operation, field="operation")
    is_paid_identity = claim.paid_scope_identity is not None
    if evidence:
        identity = (
            claim.paid_scope_identity
            or hashlib.sha256(
                canonical_json_bytes(
                    {
                        "provider": provider_component,
                        "slot_id": claim.slot_id,
                        "attempt_number": claim.attempt_number,
                        "operation": operation_component,
                    }
                )
            ).hexdigest()
        )
        sequence = claim.paid_sequence if is_paid_identity else claim.attempt_number
        suffix = "partial" if partial is not None else "entity"
        quarantine_path = (
            raw_root
            / "quarantine"
            / provider_component
            / operation_component
            / f"scope-{identity}-sequence-{sequence:04d}.{suffix}"
        )
        quarantine = write_quarantine_evidence(
            quarantine_path,
            evidence,
            evidence_root=raw_root,
        )
        receipt.update(quarantine_path=str(quarantine.path), zero_body=False)
        if partial is not None:
            receipt.update(
                partial_bytes=quarantine.byte_size,
                partial_sha256=quarantine.sha256,
                quarantine_kind="transport_partial",
            )
        else:
            receipt.update(
                quarantine_kind="complete_entity_storage_failure",
                quarantine_bytes=quarantine.byte_size,
                quarantine_sha256=quarantine.sha256,
            )
    else:
        identity = (
            claim.paid_scope_identity
            or hashlib.sha256(
                canonical_json_bytes(
                    {
                        "provider": provider_component,
                        "slot_id": claim.slot_id,
                        "attempt_number": claim.attempt_number,
                        "operation": operation_component,
                    }
                )
            ).hexdigest()
        )
        sequence = claim.paid_sequence if is_paid_identity else claim.attempt_number
        quarantine_path = (
            raw_root
            / "quarantine"
            / provider_component
            / operation_component
            / f"scope-{identity}-sequence-{sequence:04d}.zero"
        )
        receipt.update(
            quarantine_path=None,
            partial_bytes=0,
            partial_sha256=None,
            zero_body=True,
        )
    receipt_sidecar = write_immutable_json_receipt(
        quarantine_path.with_name(f"{quarantine_path.name}.receipt.json"),
        receipt,
        evidence_root=raw_root,
    )
    receipt.update(
        quarantine_receipt_path=str(receipt_sidecar.path),
        quarantine_receipt_sha256=receipt_sidecar.sha256,
        quarantine_receipt_bytes=receipt_sidecar.byte_size,
    )
    return receipt


def _release_unsent_usage(
    connection: sqlite3.Connection, *, usage_id: int, reason: str, at: str,
) -> bool:
    """A reservation is refundable only before its durable send boundary."""
    row = connection.execute("SELECT * FROM provider_usage WHERE id=?", (usage_id,)).fetchone()
    if row is None or str(row["provider"]).lower() != "tikhub":
        return False
    metadata = json.loads(row["details_json"])
    if (metadata.get("state") != "reserved" or row["request_attempts"] != 0
            or metadata.get("sent_at") is not None or not row["budget_batch_id"]
            or type(metadata.get("slot_id")) is not int
            or type(metadata.get("attempt_number")) is not int):
        return False
    if connection.execute(
            "SELECT 1 FROM fetch_attempts WHERE slot_id=? AND attempt_number=?",
            (metadata["slot_id"], metadata["attempt_number"]),
    ).fetchone() is not None:
        return False
    _settle_budget(
        connection, usage_id=usage_id, budget_id=str(row["budget_batch_id"]),
        unit_price=float(row["amount"]), billed=False,
        details={"state": "not_sent", "error_code": reason, "released_at": at},
    )
    if not recovery_completion_deferred():
        finish_recovery_probe(
            connection, usage_id=usage_id, succeeded=False, at=at, reason=reason
        )
    return True


def _paid_scope_payload(scope: PaidScope) -> dict[str, Any]:
    """Return the canonical non-null scope frozen into dispatch evidence."""

    return {key: value for key, value in asdict(scope).items() if value is not None}


def _legacy_paid_request_identity(
    connection: sqlite3.Connection,
    *,
    content_id: int | None,
    account_id: int | None,
    provider: str,
    operation: str,
    stage: str,
    window_key: str,
) -> PaidRequestIdentity:
    """Compatibility identity for direct callers not yet supplying a request.

    Production provider adapters pass the exact provider parameters.  This
    fallback keeps local fixtures and pre-existing internal callers stable but
    is visibly marked and is never eligible for transport qualification.
    """

    if content_id is not None:
        row = connection.execute(
            "SELECT platform,platform_content_id FROM content_items WHERE id=?",
            (content_id,),
        ).fetchone()
        if row is None:
            raise PaidScopeBlocked("paid_identity_missing", "Paid content is missing")
        platform = str(row["platform"])
        subject = str(row["platform_content_id"])
    else:
        row = connection.execute(
            """
            SELECT id,platform,uid FROM account_platform_identities
            WHERE account_id=? ORDER BY id LIMIT 1
            """,
            (account_id,),
        ).fetchone()
        if row is None or not row["uid"]:
            raise PaidScopeBlocked(
                "paid_identity_missing", "Paid account identity is missing"
            )
        platform = str(row["platform"])
        subject = str(row["uid"])
        if operation == "douyin_user_posts":
            reference = connection.execute(
                """SELECT reference_value FROM account_provider_references
                   WHERE account_identity_id=? AND provider='TikHub' COLLATE NOCASE
                     AND reference_kind='sec_user_id'""",
                (row["id"],),
            ).fetchone()
            if reference is None or not reference["reference_value"]:
                raise PaidScopeBlocked(
                    "paid_identity_missing",
                    "Paid Douyin discovery identity has no provider reference",
                )
            subject = str(reference["reference_value"])
    return build_paid_request_identity(
        provider=provider,
        operation=operation,
        platform=platform,
        subject=subject,
        request_parameters={
            "legacy_inferred": True,
            "stage": stage,
            "window_key": window_key,
        },
        cursor=None,
        due_bucket=window_key,
    )


def _finish_dispatch_if_open(
    connection: sqlite3.Connection,
    dispatch_id: str | None,
    *,
    outcome: Literal["succeeded", "failed", "billing_unknown"],
    created_at: str,
    raw_response_id: int | None = None,
    reason: str | None = None,
) -> None:
    """Close an open dispatch while preserving an earlier recovery terminal."""

    if dispatch_id is None:
        return
    if dispatch_events(connection, dispatch_id)[-1].event_type in TERMINAL_EVENTS:
        return
    finish_dispatch_in_transaction(
        connection,
        dispatch_id,
        outcome=outcome,
        created_at=created_at,
        raw_response_id=raw_response_id,
        reason=reason,
    )


def _validate_batch_members(connection: sqlite3.Connection, *, batch_id: int,
                            identities: tuple[PaidRequestIdentity, ...], assignments: tuple[int, ...],
                            scope: PaidScope, at: str, window_key: str,
                            singleton: bool = False) -> list[dict[str, Any]]:
    from .capture_planning import execution_route_context, require_send_route

    if singleton:
        if len(identities) != 1 or len(assignments) != 1:
            raise PaidScopeBlocked("batch_identity_invalid", "Singleton has exactly one requested member")
        return capture_singletons.validate(connection, batch_id=batch_id, request=identities[0],
            assignment_id=assignments[0], scope=scope, at=at)
    batch = connection.execute("SELECT * FROM fetch_request_batches WHERE id=?", (batch_id,)).fetchone()
    if (batch is None or batch["provider"].lower() != "tikhub" or batch["operation"] != "douyin_video_statistics"
            or batch["sequence"] != 0 or len(identities) not in {1, 2} or len(assignments) != len(identities)):
        raise PaidScopeBlocked("batch_identity_invalid", "Only a frozen singleton/pair statistics batch is supported")
    members = connection.execute("""SELECT m.*,c.platform_content_id,c.platform FROM fetch_request_batch_members m
        JOIN content_items c ON c.id=m.content_id WHERE m.batch_id=? ORDER BY c.platform_content_id""", (batch_id,)).fetchall()
    if len(members) != len(identities):
        raise PaidScopeBlocked("batch_identity_invalid", "Batch member count changed")
    results = []
    for member, identity, assignment in zip(members, identities, assignments):
        identity = validate_paid_request_identity(identity, provider="TikHub", operation=batch["operation"],
            platform="douyin", subject=str(member["platform_content_id"]), due_bucket=window_key)
        member_hash = usage_settlements.member_identity(identity.document)
        if (member["sequence"] != 0 or member["member_scope_identity"] != member_hash
                or identity.document["request_parameters"] != {"aweme_ids": str(member["platform_content_id"])}):
            raise PaidScopeBlocked("batch_identity_invalid", "Frozen member paid identity differs")
        member_scope = freeze_scope(connection, content_id=member["content_id"], account_id=None,
            stage="metrics", scope=replace(scope, content_id=None, account_id=None, identity_id=None, uid=None, platform=None))
        if member_scope.account_id != member["account_id"]:
            raise PaidScopeBlocked("batch_identity_invalid", "Member account changed")
        with execution_route_context(assignment):
            route = require_send_route(connection, scope=member_scope, operation=batch["operation"], at=at)
        if route is None or route["id"] != assignment:
            raise PaidScopeBlocked("route_generation_conflict", "Batch member route changed")
        usage_settlements.require_scope_available(connection, identity=member_hash)
        results.append({"member_id": member["id"], "assignment_id": assignment, "member_identity": member_hash})
    expected = {"aweme_ids": ",".join(str(member["platform_content_id"]) for member in members)}
    if json.loads(batch["parameters_json"]) != expected:
        raise PaidScopeBlocked("batch_identity_invalid", "Frozen batch parameters differ from members")
    return results


def _claim_paid_tikhub(
    *,
    content_id: Optional[int],
    account_id: Optional[int],
    stage: str,
    window_key: str,
    provider: str,
    adapter_version: str,
    operation: str,
    db_path: Path,
    budget_id: Optional[str],
    task_id: Optional[str],
    task_max_amount: Optional[float],
    allow_terminal_retry: bool,
    paid_request_identity: PaidRequestIdentity | None = None,
    request_transport: Mapping[str, Any] | None = None,
    request_batch_id: int | None = None,
    member_request_identities: tuple[PaidRequestIdentity, ...] = (),
    member_assignment_ids: tuple[int, ...] = (),
) -> SlotClaim:
    """Reserve and claim, but create no network attempt until dispatch."""
    from .transport_authority import (
        authorize_diagnostic_request,
        current_diagnostic_request_binding,
        diagnostic_dispatch_binding,
    )

    diagnostic_binding = current_diagnostic_request_binding()
    if budget_id is None:
        raise BudgetBlocked("TikHub network execution requires a verified budget")
    with prepare_installed_evidence(db_path, enabled=diagnostic_binding is None), transaction_metrics_context(
        job_id="tikhub_paid_claim", operation=operation
    ), connect(db_path) as connection, transaction(connection), evidence_boundary(connection):
        claimed_at = now_utc()
        if diagnostic_binding is None:
            paid_drain.require_paid_dispatch_open(
                connection,
                provider=provider,
                operation=operation,
                at=claimed_at,
            )
        if request_transport is not None:
            from .provider_transport import validate_request_transport_binding
            request_transport = validate_request_transport_binding(request_transport)
        if content_id is None and account_id is None:
            raise BudgetBlocked("Paid fetch requires an account or content")
        target_column = "content_id" if content_id is not None else "account_id"
        target_id = content_id if content_id is not None else account_id
        row = connection.execute(
            f"SELECT * FROM fetch_slots WHERE {target_column}=? AND stage=? AND window_key=?",
            (target_id, stage, window_key),
        ).fetchone()
        allowed = {"pending", "retryable_failed"}
        if allow_terminal_retry:
            allowed.add("terminal_failed")
        from . import capture_compensation

        compensation_context = _schema20(connection) and capture_compensation.active_work_id() is not None
        if compensation_context:
            allowed.update({"terminal_failed", "succeeded"})
        if row is not None and row["status"] not in allowed:
            raise SlotUnavailable(
                f"slot {row['id']} is {row['status']}",
                error_code=str(row["last_error_code"] or "slot_unavailable"), slot_id=int(row["id"]),
            )
        if row is not None:
            if row["last_error_code"] == BILLING_UNKNOWN_SLOT_ERROR and not compensation_context:
                raise PaidScopeBlocked(
                    BILLING_UNKNOWN_SLOT_ERROR,
                    BILLING_UNKNOWN_SLOT_MESSAGE,
                )
        request_identity = paid_request_identity or _legacy_paid_request_identity(
            connection,
            content_id=content_id,
            account_id=account_id,
            provider=provider,
            operation=operation,
            stage=stage,
            window_key=window_key,
        )
        try:
            scope = freeze_scope(
                connection, content_id=content_id, account_id=account_id, stage=stage
            )
        except PaidScopeBlocked as error:
            if diagnostic_binding is not None:
                raise PaidScopeBlocked("provider_transport_blocked", str(error)) from error
            raise
        if request_batch_id is not None:
            if not _schema20(connection) or operation != "douyin_video_statistics":
                raise PaidScopeBlocked("batch_identity_invalid", "Batch requires schema20 statistics")
            batch = connection.execute("SELECT * FROM fetch_request_batches WHERE id=?", (request_batch_id,)).fetchone()
            if batch is None or batch["request_scope_identity"] != request_identity.scope_identity:
                raise PaidScopeBlocked("batch_identity_invalid", "Batch request identity differs")
            expected_subject = str(json.loads(batch["parameters_json"])["aweme_ids"])
            if request_identity.document["request_parameters"] != json.loads(batch["parameters_json"]):
                raise PaidScopeBlocked("batch_identity_invalid", "Batch request parameters differ")
            _validate_batch_members(connection, batch_id=request_batch_id, identities=member_request_identities,
                assignments=member_assignment_ids, scope=scope, at=claimed_at, window_key=window_key)
        elif content_id is not None:
            target = connection.execute(
                "SELECT platform_content_id FROM content_items WHERE id=?",
                (content_id,),
            ).fetchone()
            expected_subject = str(target["platform_content_id"]) if target else ""
        elif operation == "douyin_user_posts":
            target = connection.execute(
                """SELECT reference_value FROM account_provider_references
                   WHERE account_identity_id=? AND provider='TikHub' COLLATE NOCASE
                     AND reference_kind='sec_user_id'""",
                (scope.identity_id,),
            ).fetchone()
            expected_subject = str(target["reference_value"]) if target else ""
        else:
            expected_subject = str(scope.uid or "")
        try:
            request_identity = validate_paid_request_identity(
                request_identity,
                provider=provider,
                operation=operation,
                platform=str(scope.platform or ""),
                subject=expected_subject,
                due_bucket=window_key,
            )
        except PaidIdentityError as error:
            raise PaidScopeBlocked(
                "provider_transport_blocked" if diagnostic_binding is not None else "paid_identity_invalid",
                str(error),
            ) from error
        compensation_issuances: dict[str, int] = {}
        compensation_proof = None
        singleton_batch = False
        if _schema20(connection):
            request_identity, compensation_issuances, compensation_proof = capture_compensation.prepare_request(
                connection, request_identity, scope=scope, at=claimed_at)
            try:
                usage_settlements.require_scope_available(connection, identity=request_identity.scope_identity,
                                                           sequence=request_identity.sequence)
                for member_request in member_request_identities or (request_identity,):
                    usage_settlements.require_scope_available(connection,
                        identity=usage_settlements.member_identity(member_request.document), sequence=request_identity.sequence)
            except usage_settlements.SettlementError as error:
                raise PaidScopeBlocked("paid_identity_hold", str(error)) from error
            if request_batch_id is None:
                request_batch_id, assignment_id = capture_singletons.freeze(
                    connection, request=request_identity, scope=scope, at=claimed_at)
                member_request_identities = (request_identity,)
                member_assignment_ids = (assignment_id,)
                singleton_batch = True
        scope = replace(
            scope,
            paid_scope_identity=request_identity.scope_identity,
            paid_sequence=request_identity.sequence,
        )
        diagnostic_member = None
        diagnostic_dispatch = None
        if diagnostic_binding is not None:
            diagnostic_member = authorize_diagnostic_request(
                connection, binding=diagnostic_binding, scope=scope,
                request_identity=request_identity, request_transport=request_transport,
                stage=stage, at=claimed_at,
            )
            diagnostic_dispatch = diagnostic_dispatch_binding(diagnostic_member)
        if request_identity.sequence != 0 and not _schema20(connection):
            consume_compensation_authorization(
                connection,
                authorization_id=scope.compensation_authorization_id,
                paid_scope_identity=request_identity.scope_identity,
                sequence=request_identity.sequence,
                operation=operation,
                at=claimed_at,
            )
        elif request_identity.sequence == 0 and scope.compensation_authorization_id is not None:
            raise PaidScopeBlocked(
                "compensation_authorization_invalid",
                "A compensation authorization may only fund sequence 1",
            )
        authority = None
        if _schema20(connection):
            from . import capture_authorizations
            from .provider_budget import PRICES_MICROUSD

            authority = capture_authorizations.validate_authorization(connection,
                runtime_bindings=capture_authorizations.current_runtime_bindings(connection, operation, claimed_at),
                operation=operation, request_identity=request_identity.scope_identity, at=claimed_at,
                amount_microusd=PRICES_MICROUSD[operation], sequence=request_identity.sequence,
                issuance_ids=compensation_issuances,
                member_identities=tuple(usage_settlements.member_identity(item.document)
                                        for item in member_request_identities or (request_identity,)))
        usage_id, unit_price, currency = _reserve_budget(
            connection, budget_id=budget_id, provider=provider, operation=operation,
            task_id=task_id, task_max_amount=task_max_amount, dispatch_scope=scope,
        )
        if request_batch_id is not None:
            connection.execute("""INSERT INTO admission_reservations(batch_id,state,amount_microusd,
                charge_business_day,created_at,expires_at,updated_at) VALUES(?,'reserved_unsent',?,?,?,?,?)
                ON CONFLICT(batch_id) DO UPDATE SET state='reserved_unsent',updated_at=excluded.updated_at,
                expires_at=excluded.expires_at WHERE admission_reservations.state='released_unsent'""",
                (request_batch_id, micro_usd(unit_price), budget_day(claimed_at), claimed_at,
                 (datetime.fromisoformat(claimed_at.replace('Z','+00:00'))+timedelta(minutes=3)).strftime('%Y-%m-%dT%H:%M:%SZ'), claimed_at))
            if connection.execute("SELECT state FROM admission_reservations WHERE batch_id=?", (request_batch_id,)).fetchone()[0] != 'reserved_unsent':
                raise PaidScopeBlocked("batch_identity_invalid", "Batch reservation was already sent")
        if content_id is not None:
            slot_id = ensure_content_slot(
                connection, content_id=content_id, stage=stage, window_key=window_key,
                provider=provider, adapter_version=adapter_version,
            )
        else:
            assert account_id is not None
            slot_id = ensure_account_slot(
                connection, account_id=account_id, stage=stage, window_key=window_key,
                provider=provider, adapter_version=adapter_version,
            )
        metadata = json.loads(connection.execute(
            "SELECT details_json FROM provider_usage WHERE id=?", (usage_id,)
        ).fetchone()[0])
        attempt_number = int(row["attempt_count"]) + 1 if row is not None else 1
        metadata.update(
            slot_id=slot_id,
            attempt_number=attempt_number,
            paid_scope_identity=request_identity.scope_identity,
            paid_execution_identity=request_identity.execution_identity,
            paid_sequence=request_identity.sequence,
            paid_identity=request_identity.document,
            paid_identity_evidence=(
                "legacy_inferred"
                if request_identity.document["request_parameters"].get(
                    "legacy_inferred"
                )
                else "provider_exact"
            ),
        )
        if _schema20(connection):
            # These fields belong to the v20 admission contract. Historical
            # v19 canary receipts compare their exact frozen usage payload.
            metadata.update(
                authority_sha256=authority["authority_sha256"] if authority is not None else None,
                request_batch_id=request_batch_id,
                compensation_issuance_ids=compensation_issuances,
                compensation_proof_sha256=compensation_proof,
            )
        if request_transport is not None:
            metadata["request_transport"] = request_transport
        if diagnostic_dispatch is not None:
            metadata["diagnostic_member"] = diagnostic_dispatch
        connection.execute(
            "UPDATE provider_usage SET details_json=? WHERE id=?",
            (json.dumps(metadata, sort_keys=True), usage_id),
        )
        dispatch_id: str | None = None
        if supports_dispatch_ledger(connection):
            if (
                scope.activation_id is None
                or scope.scheduler_run_id is None
                or scope.scheduler_attempt_id is None
            ):
                raise PaidScopeBlocked(
                    "paid_dispatch_owner_missing",
                    "Schema 19 paid dispatch requires an activation and scheduler owner",
                )
            dispatch = reserve_dispatch_in_transaction(
                connection,
                provider=provider,
                operation=operation,
                activation_id=scope.activation_id,
                business_day=scope.business_day or budget_day(claimed_at),
                scheduler_run_id=scope.scheduler_run_id,
                scheduler_attempt_id=scope.scheduler_attempt_id,
                scope={
                    **_paid_scope_payload(scope),
                    **({"request_transport": request_transport} if request_transport is not None else {}),
                    **({"diagnostic_member": diagnostic_dispatch} if diagnostic_dispatch is not None else {}),
                },
                created_at=claimed_at,
                provider_usage_id=usage_id,
                fetch_slot_id=slot_id,
                cursor_identity={
                    "paid_scope_identity": request_identity.scope_identity,
                    "paid_execution_identity": request_identity.execution_identity,
                    "sequence": request_identity.sequence,
                    "request": request_identity.document,
                },
                **({"diagnostic_authority": {
                    "request_binding": diagnostic_binding,
                    "scope": scope,
                    "request_identity": request_identity,
                    "request_transport": request_transport,
                    "stage": stage,
                }} if diagnostic_binding is not None else {}),
            )
            if dispatch is None:
                raise RuntimeError("Schema 19 paid dispatch reservation was not retained")
            dispatch_id = dispatch.dispatch_id
        connection.execute(
            """UPDATE fetch_slots SET status='running',provider=?,adapter_version=?,
               started_at=?,finished_at=NULL,last_error_code=NULL,last_error_message=NULL,
               updated_at=? WHERE id=?""",
            (provider, adapter_version, claimed_at, claimed_at, slot_id),
        )
        return SlotClaim(
            slot_id=slot_id,
            attempt_id=0,
            attempt_number=attempt_number,
            content_id=content_id,
            stage=stage,
            window_key=window_key,
            provider=provider,
            adapter_version=adapter_version,
            account_id=account_id,
            dispatch_scope=scope,
            reserved_usage_id=usage_id,
            reserved_unit_price=unit_price,
            reserved_currency=currency,
            dispatch_id=dispatch_id,
            paid_scope_identity=request_identity.scope_identity,
            paid_execution_identity=request_identity.execution_identity,
            paid_sequence=request_identity.sequence,
            request_transport=request_transport,
            diagnostic_binding=diagnostic_binding,
            diagnostic_member=diagnostic_member,
            paid_request_identity=request_identity if diagnostic_binding is not None or _schema20(connection) else None,
            authority_sha256=authority["authority_sha256"] if authority is not None else None,
            request_batch_id=request_batch_id, member_request_identities=member_request_identities,
            singleton_batch=singleton_batch,
            member_assignment_ids=member_assignment_ids,
            compensation_issuance_ids=compensation_issuances,
            compensation_proof_sha256=compensation_proof,
        )


def _paid_slot_owner(
    connection: sqlite3.Connection, claim: SlotClaim, *, check_scheduler: bool = True,
) -> bool:
    if check_scheduler and claim.dispatch_scope is not None:
        try:
            _assert_scheduler_owner(connection, claim.dispatch_scope)
        except PaidScopeBlocked:
            return False
    row = connection.execute(
        "SELECT status,attempt_count FROM fetch_slots WHERE id=?", (claim.slot_id,)
    ).fetchone()
    expected = claim.attempt_number if claim.attempt_id else claim.attempt_number - 1
    if row is None or row["status"] != "running" or row["attempt_count"] != expected:
        return False
    usage = connection.execute(
        """SELECT provider,request_attempts,billed_requests,currency,amount,details_json
           FROM provider_usage
           WHERE id=?""",
        (claim.reserved_usage_id,),
    ).fetchone()
    if usage is None or str(usage["provider"]).lower() != "tikhub":
        return False
    metadata = json.loads(usage["details_json"])
    state = metadata.get("state")
    attempts_match = (
        (state == "reserved" and int(usage["request_attempts"]) == 0)
        or (
            state in {"sent", "billing_unknown"}
            and int(usage["request_attempts"]) == 1
        )
    )
    return (
        metadata.get("slot_id") == claim.slot_id
        and metadata.get("attempt_number") == claim.attempt_number
        and attempts_match
        and int(usage["billed_requests"]) == 1
        and usage["currency"] == claim.reserved_currency
        and micro_usd(usage["amount"]) == micro_usd(claim.reserved_unit_price)
    )


def _mark_paid_sent(
    claim: SlotClaim,
    *,
    operation: str,
    budget_id: str,
    db_path: Path,
) -> SlotClaim:
    try:
        scope = claim.dispatch_scope
        with prepare_installed_evidence(db_path, enabled=claim.diagnostic_binding is None), transaction_metrics_context(
            job_id="tikhub_paid_send",
            scheduler_run_id=(scope.scheduler_run_id if scope else None),
            attempt_id=(scope.scheduler_attempt_id if scope else None),
            operation=operation,
        ), connect(db_path) as connection, transaction(connection), evidence_boundary(connection):
            sent_at = now_utc()
            diagnostic_dispatch = None
            if claim.diagnostic_binding is None:
                paid_drain.require_paid_dispatch_open(
                    connection,
                    provider=claim.provider,
                    operation=operation,
                    at=sent_at,
                )
            else:
                from .transport_authority import (
                    authorize_diagnostic_request,
                    diagnostic_dispatch_binding,
                )
                if scope is None or claim.paid_request_identity is None or claim.diagnostic_member is None:
                    raise PaidScopeBlocked("provider_transport_blocked", "Diagnostic claim lost its request or member")
                member = authorize_diagnostic_request(
                    connection, binding=claim.diagnostic_binding, scope=scope,
                    request_identity=claim.paid_request_identity,
                    request_transport=claim.request_transport, stage=claim.stage, at=sent_at,
                )
                if member != claim.diagnostic_member:
                    raise PaidScopeBlocked("provider_transport_blocked", "Diagnostic member changed while queued")
                diagnostic_dispatch = diagnostic_dispatch_binding(member)
            if not _paid_slot_owner(connection, claim):
                raise PaidScopeBlocked(
                    "provider_transport_blocked" if diagnostic_dispatch is not None else "attempt_owner_lost",
                    "Paid slot is no longer owned",
                )
            reservation = connection.execute(
                "SELECT request_attempts,details_json FROM provider_usage WHERE id=?",
                (claim.reserved_usage_id,),
            ).fetchone()
            if (reservation is None or reservation["request_attempts"] != 0
                    or json.loads(reservation["details_json"]).get("state") != "reserved"):
                raise PaidScopeBlocked(
                    "provider_transport_blocked" if diagnostic_dispatch is not None else "attempt_owner_lost",
                    "Reservation has already crossed the send boundary",
                )
            if diagnostic_dispatch is not None:
                metadata = json.loads(reservation["details_json"])
                request_identity = claim.paid_request_identity
                assert request_identity is not None and scope is not None
                if (
                    metadata.get("diagnostic_member") != diagnostic_dispatch
                    or metadata.get("paid_scope_identity") != request_identity.scope_identity
                    or metadata.get("paid_execution_identity") != request_identity.execution_identity
                    or metadata.get("paid_sequence") != request_identity.sequence
                    or metadata.get("paid_identity") != request_identity.document
                    or metadata.get("scope") != asdict(scope)
                    or claim.dispatch_id is None
                ):
                    raise PaidScopeBlocked("provider_transport_blocked", "Reserved diagnostic request binding changed")
                try:
                    events = dispatch_events(connection, claim.dispatch_id)
                except (RuntimeError, ValueError) as error:
                    raise PaidScopeBlocked("provider_transport_blocked", "Reserved diagnostic dispatch is invalid") from error
                if (
                    len(events) != 1
                    or events[0].scope != {
                        **_paid_scope_payload(scope),
                        "request_transport": claim.request_transport,
                        "diagnostic_member": diagnostic_dispatch,
                    }
                    # Schema 19 keeps a historical RELEASE foreign key; only
                    # the separately bound current START/member authorizes send.
                    or events[0].permit_event_id != member["payload"]["hold_binding"]["dispatch_legacy_release_anchor"]["event_id"]
                    or events[0].scope["diagnostic_member"]["start_event_id"] != member["payload"]["hold_binding"]["start_event_id"]
                    or events[0].scope["diagnostic_member"]["start_event_hash"] != member["payload"]["hold_binding"]["start_event_hash"]
                    or events[0].activation_id != scope.activation_id
                    or events[0].scheduler_run_id != scope.scheduler_run_id
                    or events[0].scheduler_attempt_id != scope.scheduler_attempt_id
                    or events[0].business_day != scope.business_day
                    or events[0].provider.lower() != claim.provider.lower()
                    or events[0].operation != operation
                    or events[0].provider_usage_id != claim.reserved_usage_id
                    or events[0].fetch_slot_id != claim.slot_id
                    or events[0].cursor_identity != {
                        "paid_scope_identity": request_identity.scope_identity,
                        "paid_execution_identity": request_identity.execution_identity,
                        "sequence": request_identity.sequence,
                        "request": request_identity.document,
                    }
                ):
                    raise PaidScopeBlocked("provider_transport_blocked", "Reserved diagnostic dispatch binding changed")
            if claim.request_transport is not None:
                from .provider_transport import validate_request_transport_binding
                validated_transport = validate_request_transport_binding(claim.request_transport)
                if json.loads(reservation["details_json"]).get("request_transport") != validated_transport:
                    raise PaidScopeBlocked("provider_transport_blocked", "Reserved transport binding changed")
            batch = connection.execute(
                "SELECT * FROM provider_budget_batches WHERE id=?", (budget_id,),
            ).fetchone()
            if (batch is None or batch["provider"] != claim.provider or batch["operation"] != operation
                    or batch["currency"] != claim.reserved_currency
                    or micro_usd(batch["verified_unit_price"]) != micro_usd(claim.reserved_unit_price)
                    or (batch["status"] not in {"approved", "pilot"}
                        and not (batch["status"] == "suspended" and batch["pilot_size"] > 0))
                    or micro_usd(batch["consumed_amount"]) > micro_usd(batch["max_amount"])
                    or batch["consumed_requests"] > batch["max_billable_requests"]):
                raise PaidScopeBlocked("budget_batch_changed", "Budget approval or price changed while queued")
            scope = freeze_scope(
                connection, content_id=claim.content_id, account_id=claim.account_id,
                stage=claim.stage, scope=claim.dispatch_scope,
            )
            if scope.business_day is not None and budget_day(sent_at) != scope.business_day:
                raise PaidScopeBlocked(
                    "business_day_expired",
                    "Paid work cannot cross its frozen Beijing business day",
                )
            from .capture_planning import require_send_route

            require_send_route(connection, scope=scope, operation=operation, at=sent_at)
            from .provider_budget import renew_paid_owner_lease

            renew_paid_owner_lease(connection, scope, at=sent_at)
            checked = check_reservation(
                connection, scope=scope, operation=operation,
                unit_price=claim.reserved_unit_price, currency=claim.reserved_currency,
                at=sent_at, exclude_usage_id=claim.reserved_usage_id,
            )
            day_start, day_end = _shanghai_day_utc_bounds(sent_at)
            daily = connection.execute(
                """SELECT COALESCE(SUM(request_attempts),0) FROM provider_usage
                   WHERE budget_batch_id=? AND recorded_at>=? AND recorded_at<?""",
                (budget_id, day_start, day_end),
            ).fetchone()[0]
            quota = connection.execute(
                "SELECT daily_quota FROM provider_budget_batches WHERE id=?", (budget_id,)
            ).fetchone()[0]
            if daily >= quota:
                raise DailyAttemptQuotaExhausted("daily attempt quota reached before dispatch")
            row = connection.execute(
                "SELECT details_json FROM provider_usage WHERE id=?", (claim.reserved_usage_id,)
            ).fetchone()
            metadata = json.loads(row["details_json"])
            # The send day, not a previous reservation day, owns the charge.
            metadata.update(
                budget_day=checked["budget_day"], state="sent", sent_at=sent_at,
                borrowed_from=checked["borrowed_from"],
                borrowing_proofs=checked["borrowing_proofs"],
                validated_work_fingerprint=checked["validated_work_fingerprint"],
            )
            if (
                claim.paid_scope_identity is None
                or claim.paid_execution_identity is None
            ):
                raise PaidSendClaimHeld("paid request identity is missing")
            if _schema20(connection):
                from . import capture_authorizations

                if not claim.authority_sha256 or metadata.get("authority_sha256") != claim.authority_sha256:
                    raise PaidSendClaimHeld("schema20 request lost its admission authority")
                if metadata.get("request_batch_id") != claim.request_batch_id:
                    raise PaidSendClaimHeld("schema20 batch binding changed")
                from . import capture_compensation

                if claim.paid_request_identity is None:
                    raise PaidSendClaimHeld("schema20 compensation request document is missing")
                _, compensation_issuances, compensation_proof = capture_compensation.prepare_request(
                    connection, claim.paid_request_identity, scope=scope, at=sent_at,
                    exclude_usage_id=claim.reserved_usage_id)
                if (compensation_issuances != dict(claim.compensation_issuance_ids or {})
                        or metadata.get("compensation_issuance_ids", {}) != compensation_issuances
                        or compensation_proof != claim.compensation_proof_sha256
                        or metadata.get("compensation_proof_sha256") != compensation_proof):
                    raise PaidSendClaimHeld("schema20 compensation proof changed after reservation")
                member_hashes = tuple(usage_settlements.member_identity(item.document)
                    for item in claim.member_request_identities or (claim.paid_request_identity,)
                    if item is not None)
                batch_members = []
                if claim.request_batch_id is not None:
                    batch_members = _validate_batch_members(connection, batch_id=claim.request_batch_id,
                        identities=claim.member_request_identities, assignments=claim.member_assignment_ids,
                        scope=scope, at=sent_at, window_key=claim.window_key, singleton=claim.singleton_batch)
                    admitted = connection.execute("SELECT * FROM admission_reservations WHERE batch_id=?",
                                                  (claim.request_batch_id,)).fetchone()
                    if (admitted is None or admitted['state'] != 'reserved_unsent' or admitted['expires_at'] <= sent_at
                            or admitted['amount_microusd'] != micro_usd(claim.reserved_unit_price)
                            or admitted['charge_business_day'] != budget_day(sent_at)):
                        raise PaidSendClaimHeld("batch reservation expired or changed")
                authorization = capture_authorizations.validate_authorization(connection,
                    runtime_bindings=capture_authorizations.current_runtime_bindings(connection, operation, sent_at),
                    operation=operation, request_identity=claim.paid_scope_identity, at=sent_at,
                    amount_microusd=micro_usd(claim.reserved_unit_price), sequence=claim.paid_sequence,
                    member_identities=member_hashes, exclude_usage_id=claim.reserved_usage_id,
                    issuance_ids=compensation_issuances,
                    expected_authority_sha256=claim.authority_sha256)
                request_identity = claim.paid_request_identity
                if (request_identity is None or request_identity.scope_identity != claim.paid_scope_identity
                        or request_identity.sequence != claim.paid_sequence
                        or metadata.get("paid_identity") != request_identity.document
                        or metadata.get("paid_scope_identity") != claim.paid_scope_identity):
                    raise PaidSendClaimHeld("schema20 reserved request identity changed before send")
                try:
                    usage_settlements.require_scope_available(connection,
                        identity=claim.paid_scope_identity, sequence=claim.paid_sequence)
                    for member_hash in member_hashes:
                        usage_settlements.require_scope_available(connection,
                            identity=member_hash, sequence=claim.paid_sequence)
                except usage_settlements.SettlementError as error:
                    raise PaidSendClaimHeld(str(error)) from error
            operation_recovery.require_operation_lock(connection, operation=operation)
            if claim.diagnostic_binding is None:
                from .capture_manual import permits_transport_retry

                # Explicit content retries retain the operation fault. Recheck
                # their frozen command at B, under the same send lock, without
                # taking the ordinary recovery owner that may close the fault.
                # Once healthy, an unsent command needs no transport exception.
                operation_fault = fault_state(connection, scope_kind="operation", operation=operation)
                explicit_retry = (scope.category == "metrics" and (operation_fault or {}).get("open") is True
                    and permits_transport_retry(connection, scope=scope, operation=operation, at=sent_at))
                if not explicit_retry:
                    assert claim.reserved_usage_id is not None and claim.paid_scope_identity is not None
                    operation_recovery.claim_operation_probe(connection, operation=operation,
                        usage_id=claim.reserved_usage_id, identity=claim.paid_scope_identity,
                        sequence=claim.paid_sequence, at=sent_at)
            send_claim = claim_paid_send(
                db_path.parent / "paid_send_claims",
                paid_scope_identity=claim.paid_scope_identity,
                sequence=claim.paid_sequence,
                claim={
                    "operation": operation,
                    "provider": claim.provider.lower(),
                    "paid_execution_identity": claim.paid_execution_identity,
                    "provider_usage_id": claim.reserved_usage_id,
                    "slot_id": claim.slot_id,
                    "created_at": sent_at,
                    **({"request_transport": claim.request_transport} if claim.request_transport is not None else {}),
                    **({"diagnostic_member": diagnostic_dispatch} if diagnostic_dispatch is not None else {}),
                },
            )
            metadata.update(
                paid_send_claim_path=str(send_claim.path),
                paid_send_claim_sha256=send_claim.sha256,
                paid_send_claim_bytes=send_claim.byte_size,
            )
            if claim.reserved_usage_id is not None:
                mark_probe_sent(
                    connection, scope=scope, operation=operation,
                    usage_id=claim.reserved_usage_id, at=sent_at,
                )
            if claim.request_batch_id is None:
                cursor = connection.execute(
                    """INSERT INTO fetch_attempts(slot_id,attempt_number,request_started_at)
                       VALUES (?,?,?)""", (claim.slot_id, claim.attempt_number, sent_at))
            else:
                cursor = connection.execute("""INSERT INTO fetch_attempts(request_batch_id,attempt_number,request_started_at)
                    VALUES(?,?,?)""", (claim.request_batch_id, claim.attempt_number, sent_at))
                connection.execute("""INSERT INTO fetch_request_executions(batch_id,fetch_attempt_id,assignment_id,
                    execution_identity,started_at) VALUES(?,?,?,?,?)""",
                    (claim.request_batch_id, cursor.lastrowid, claim.member_assignment_ids[0], claim.paid_execution_identity, sent_at))
                connection.execute("UPDATE admission_reservations SET state='sent_unsettled',updated_at=? WHERE batch_id=?",
                                   (sent_at, claim.request_batch_id))
                from .capture_planning import canonical, digest
                eligibility = {"authority_sha256": claim.authority_sha256, "members": batch_members,
                               "paid_execution_identity": claim.paid_execution_identity}
                proof_id = connection.execute("""INSERT INTO send_boundary_eligibility_receipts(batch_id,assignment_id,
                    readiness_receipt_id,reservation_id,eligible,reason,evidence_json,checked_at,receipt_sha256)
                    VALUES(?,?,?,?,1,'authorized',?,?,?)""",
                    (claim.request_batch_id, claim.member_assignment_ids[0], authorization['readiness_receipt_id'],
                     admitted['id'], canonical(eligibility), sent_at, digest(eligibility))).lastrowid
                connection.executemany("""INSERT INTO send_boundary_member_eligibility_receipts(eligibility_receipt_id,
                    member_id,eligible,reason) VALUES(?,?,1,'authorized')""",
                    [(proof_id, member['member_id']) for member in batch_members])
            if cursor.lastrowid is None:
                raise RuntimeError("Paid fetch attempt insert returned no id")
            connection.execute(
                """UPDATE fetch_slots SET attempt_count=?,started_at=?,updated_at=?
                   WHERE id=? AND status='running'""",
                (claim.attempt_number, sent_at, sent_at, claim.slot_id),
            )
            connection.execute(
                """UPDATE provider_usage SET request_attempts=1,recorded_at=?,details_json=?
                   WHERE id=?""",
                (sent_at, json.dumps(metadata, sort_keys=True), claim.reserved_usage_id),
            )
            if claim.dispatch_id is not None:
                marked = mark_dispatch_sent_in_transaction(
                    connection,
                    claim.dispatch_id,
                    fetch_attempt_id=int(cursor.lastrowid),
                    created_at=sent_at,
                )
                if marked is None:
                    raise RuntimeError("Schema 19 paid send boundary was not retained")
                if _schema20(connection):
                    if claim.paid_request_identity is None:
                        raise PaidSendClaimHeld("schema20 paid request document is missing")
                    assert claim.authority_sha256 is not None
                    try:
                        capture_authorizations.consume_authorized_start(connection,
                            runtime_bindings=capture_authorizations.current_runtime_bindings(connection, operation, sent_at),
                            marker_id=marked.event_id, request_identity=claim.paid_scope_identity,
                            expected_authority_sha256=claim.authority_sha256, operation=operation,
                            amount_microusd=micro_usd(claim.reserved_unit_price),
                            member_identities=member_hashes,
                            issuance_ids=compensation_issuances,
                            sequence=claim.paid_sequence, at=sent_at)
                    except (usage_settlements.SettlementError, sqlite3.IntegrityError) as error:
                        raise PaidSendClaimHeld(str(error)) from error
            elif _schema20(connection):
                raise PaidSendClaimHeld("schema20 request has no canonical send marker")
            return replace(claim, attempt_id=int(cursor.lastrowid), dispatch_scope=scope)
    except Exception as exc:
        # A drain START that wins the BEGIN IMMEDIATE race owns the pre-existing
        # reservation as part of its frozen tail.  Do not create a post-START
        # ``not_sent`` accounting mutation here; drain recovery will close that
        # frozen claim before SEALED can be recorded.
        if getattr(exc, "error_code", None) == "profile_switch_drain":
            raise
        with connect(db_path) as connection, transaction(connection):
            released_at = now_utc()
            identity_held = isinstance(exc, PaidSendClaimHeld)
            owned = _paid_slot_owner(connection, claim, check_scheduler=False)
            released = claim.reserved_usage_id is not None and _release_unsent_usage(
                connection, usage_id=claim.reserved_usage_id,
                reason=getattr(exc, "error_code", type(exc).__name__), at=released_at,
            )
            if released and claim.dispatch_id is not None:
                if claim.request_batch_id is not None:
                    connection.execute("UPDATE admission_reservations SET state='released_unsent',updated_at=? WHERE batch_id=? AND state='reserved_unsent'",
                                       (released_at, claim.request_batch_id))
                closed = close_dispatch_not_sent_in_transaction(
                    connection,
                    claim.dispatch_id,
                    reason=getattr(exc, "error_code", type(exc).__name__),
                    created_at=released_at,
                )
                if closed is None:
                    raise RuntimeError("Schema 19 paid not-sent boundary was not retained")
            if owned and released:
                connection.execute(
                    """UPDATE fetch_slots SET status=?,last_error_code=?,
                       last_error_message=?,finished_at=?,updated_at=? WHERE id=?""",
                    (
                        "terminal_failed" if identity_held else "pending",
                        getattr(exc, "error_code", type(exc).__name__),
                        str(exc)[:500],
                        released_at,
                        released_at,
                        claim.slot_id,
                    ),
                )
        if isinstance(exc, PaidSendClaimHeld):
            raise CaptureError(
                str(exc),
                retryable=False,
                error_code=exc.error_code,
                billed=False,
            ) from exc
        raise


def _execute_claimed_fetch(
    *, claim: SlotClaim, operation: str, call: Callable[[], ProviderResult],
    db_path: Path = DEFAULT_DB, raw_root: Optional[Path] = None,
    budget_id: Optional[str] = None, task_id: Optional[str] = None,
    task_max_amount: Optional[float] = None,
    local_result: Optional[ProviderResult] = None,
) -> CaptureOutcome:
    paid = claim.provider.lower() == "tikhub" and local_result is None
    if paid and claim.dispatch_scope is None:
        raise BudgetBlocked("TikHub network calls require a frozen paid claim")
    # Waiting owns no SQLite connection/transaction. All live checks in
    # _mark_paid_sent run after the shared network permit has been obtained.
    with (TIKHUB_NETWORK_SLOTS if paid else nullcontext()), (
        operation_recovery.operation_probe_lock(db_path=db_path, operation=operation)
        if paid else nullcontext()
    ):
        if local_result is not None:
            def local_call() -> ProviderResult:
                assert local_result is not None
                return local_result
            call = local_call
        return _execute_claimed_fetch_in_slot(
            claim=claim, operation=operation, call=call, db_path=db_path,
            raw_root=raw_root, budget_id=budget_id, task_id=task_id,
            task_max_amount=task_max_amount, network_call=local_result is None,
        )


def _execute_claimed_fetch_in_slot(
    *,
    claim: SlotClaim,
    operation: str,
    call: Callable[[], ProviderResult],
    db_path: Path = DEFAULT_DB,
    raw_root: Optional[Path] = None,
    budget_id: Optional[str] = None,
    task_id: Optional[str] = None,
    task_max_amount: Optional[float] = None,
    network_call: bool = True,
) -> CaptureOutcome:
    resolved_raw_root = RAW_ROOT if raw_root is None else raw_root
    provider = claim.provider
    usage_id: Optional[int] = None
    unit_price = 0.0
    currency = ""
    try:
        if claim.dispatch_scope is None and network_call:
            with connect(db_path) as connection:
                require_storage_ready(connection)
        _validate_task_budget(
            budget_id=budget_id,
            task_id=task_id,
            task_max_amount=task_max_amount,
            provider=provider,
            operation=operation,
        )
        if claim.dispatch_scope is not None:
            if budget_id is None:
                raise BudgetBlocked("Paid claim is missing its budget")
            claim = _mark_paid_sent(
                claim,
                operation=operation,
                budget_id=budget_id,
                db_path=db_path,
            )
            usage_id = claim.reserved_usage_id
            unit_price = claim.reserved_unit_price
            currency = claim.reserved_currency
        elif budget_id is not None:
            with connect(db_path) as connection, transaction(connection):
                usage_id, unit_price, currency = _reserve_budget(
                    connection,
                    budget_id=budget_id,
                    provider=provider,
                    operation=operation,
                    task_id=task_id,
                    task_max_amount=task_max_amount,
                )
    except Exception as exc:
        if claim.dispatch_scope is not None:
            raise
        error_code = str(getattr(exc, "error_code", type(exc).__name__))
        with connect(db_path) as connection, transaction(connection):
            finished_at = now_utc()
            connection.execute(
                """
                UPDATE fetch_attempts
                SET response_finished_at=?, error_code=?, error_message=?
                WHERE id=?
                """,
                (finished_at, error_code, str(exc)[:500], claim.attempt_id),
            )
            connection.execute(
                """
                UPDATE fetch_slots
                SET status='retryable_failed',
                    last_error_code=CASE WHEN last_error_code=? THEN last_error_code ELSE ? END,
                    last_error_message=CASE WHEN last_error_code=? THEN last_error_message ELSE ? END,
                    finished_at=?, updated_at=? WHERE id=?
                """,
                (
                    BILLING_UNKNOWN_SLOT_ERROR,
                    error_code,
                    BILLING_UNKNOWN_SLOT_ERROR,
                    str(exc)[:500],
                    finished_at,
                    finished_at,
                    claim.slot_id,
                ),
            )
        raise

    result: ProviderResult | None = None
    try:
        from .capture_availability import DETAIL_OPERATIONS, record_detail_result
        from .provider_transport import request_transport_context
        with request_transport_context(claim.request_transport):
            result = call()
        with connect(db_path) as connection, transaction(connection):
            owns_slot = claim.dispatch_scope is None or _paid_slot_owner(connection, claim)
            raw_id = _store_raw_response(
                connection,
                claim=claim,
                operation=operation,
                value=result.raw_response,
                http_status=result.http_status,
                raw_root=resolved_raw_root,
                entity_bytes=result.entity_bytes,
                transport_receipt=result.transport_receipt,
            )
            if claim.singleton_batch:
                assert claim.request_batch_id is not None
                capture_singletons.record_disposition(connection, batch_id=claim.request_batch_id,
                    attempt_id=claim.attempt_id, raw_response_id=raw_id, disposition="valid",
                    reason="adapter_result_accepted", at=now_utc())
            if (_schema20(connection) and claim.content_id is not None and operation in DETAIL_OPERATIONS
                    and result.transport_receipt is not None and owns_slot):
                record_detail_result(connection, content_id=claim.content_id, raw_response_id=raw_id,
                                     available=True, recorded_at=now_utc())
            transport_receipt = _transport_receipt_with_storage(
                connection,
                raw_response_id=raw_id,
                transport_receipt=result.transport_receipt,
            )
            if budget_id is not None and usage_id is not None:
                _settle_budget(
                    connection,
                    usage_id=usage_id,
                    budget_id=budget_id,
                    unit_price=unit_price,
                    billed=result.billed,
                    details={
                        "state": "completed",
                        "slot_id": claim.slot_id,
                        "http_status": result.http_status,
                        "raw_response_id": raw_id,
                        **(
                            {"transport": transport_receipt}
                            if transport_receipt is not None
                            else {}
                        ),
                    },
                )
                if claim.request_batch_id is not None:
                    connection.execute("UPDATE admission_reservations SET state='settled',updated_at=? WHERE batch_id=? AND state='sent_unsettled'",
                                       (now_utc(), claim.request_batch_id))
            if (
                claim.dispatch_scope is not None
                and usage_id is not None
                and result.billed is not None
                and not owns_slot
            ):
                clear_billing_unknown_slot_guard_if_resolved(
                    connection,
                    slot_id=claim.slot_id,
                    fallback_error_code="late_provider_result_retryable",
                    fallback_error_message=(
                        "A late TikHub result settled billing; retry may follow normal policy"
                    ),
                )
            finished_at = now_utc()
            _finish_dispatch_if_open(
                connection,
                claim.dispatch_id,
                outcome="succeeded",
                created_at=finished_at,
                raw_response_id=raw_id,
            )
            if (
                claim.dispatch_scope is not None
                and usage_id is not None
                and not recovery_completion_deferred()
            ):
                finish_recovery_probe(
                    connection, usage_id=usage_id, succeeded=owns_slot, at=finished_at,
                    raw_response_id=raw_id,
                    reason=None if owns_slot else "attempt_owner_lost",
                )
            if claim.dispatch_scope is not None and usage_id is not None:
                operation_recovery.finish_operation_probe(connection, operation=operation,
                    usage_id=usage_id, succeeded=owns_slot, at=finished_at, raw_response_id=raw_id)
            connection.execute(
                """
                UPDATE fetch_attempts
                SET response_finished_at=?, http_status=?, billed=?, amount=?, currency=?
                WHERE id=?
                """,
                (
                    finished_at,
                    result.http_status,
                    int(result.billed),
                    unit_price if result.billed else 0.0,
                    currency,
                    claim.attempt_id,
                ),
            )
            if owns_slot:
                connection.execute(
                    """
                    UPDATE fetch_slots
                    SET status='succeeded', finished_at=?, updated_at=? WHERE id=?
                    """,
                    (finished_at, finished_at, claim.slot_id),
                )
    except Exception as exc:
        failure = (
            exc
            if isinstance(exc, CaptureError)
            else CaptureError(
                f"{type(exc).__name__}: {exc}",
                retryable=False,
                error_code=_raw_error_code(exc),
                http_status=result.http_status,
                billed=result.billed,
                entity_bytes=result.entity_bytes,
                transport_receipt=result.transport_receipt,
            )
            if result is not None
            else CaptureError(
                f"{type(exc).__name__}: {exc}",
                retryable=True,
                error_code="unhandled_adapter_error",
            )
        )
        with connect(db_path) as connection, transaction(connection):
            owns_slot = claim.dispatch_scope is None or _paid_slot_owner(connection, claim)
            failure_raw_id: int | None = None
            if (
                result is None
                and isinstance(exc, CaptureError)
                and (
                    failure.raw_response is not None
                    or failure.entity_bytes is not None
                )
            ):
                failure_raw_id = _store_raw_response(
                    connection,
                    claim=claim,
                    operation=operation,
                    value=failure.raw_response,
                    http_status=failure.http_status,
                    raw_root=resolved_raw_root,
                    entity_bytes=failure.entity_bytes,
                    transport_receipt=failure.transport_receipt,
                )
                if (_schema20(connection) and claim.content_id is not None and operation in DETAIL_OPERATIONS
                        and failure.error_code == "content_unavailable" and failure.transport_receipt is not None and owns_slot):
                    record_detail_result(connection, content_id=claim.content_id, raw_response_id=failure_raw_id,
                                         available=False, recorded_at=now_utc())
            try:
                transport_receipt = (
                    _transport_receipt_with_storage(
                        connection,
                        raw_response_id=failure_raw_id,
                        transport_receipt=failure.transport_receipt,
                    )
                    if failure_raw_id is not None
                    else _quarantine_transport_evidence(
                        claim=claim,
                        operation=operation,
                        raw_root=resolved_raw_root,
                        partial=failure.transport_partial,
                        complete_entity=failure.entity_bytes,
                        transport_receipt=failure.transport_receipt,
                    )
                )
                if _schema20(connection) and transport_receipt is not None and failure_raw_id is None:
                    raw_archive.record_transport_receipt(connection, attempt_id=claim.attempt_id,
                                                         receipt=transport_receipt)
            except Exception as evidence_error:
                transport_receipt = dict(failure.transport_receipt or {})
                transport_receipt.update(
                    quarantine_persist_error=type(evidence_error).__name__,
                    quarantine_persist_message=str(evidence_error)[:300],
                )
                failure = CaptureError(
                    f"transport evidence persistence failed: {evidence_error}",
                    retryable=False,
                    error_code=_raw_error_code(evidence_error),
                    http_status=failure.http_status,
                    billed=failure.billed,
                    transport_receipt=transport_receipt,
                )
            settled_billed = (
                None
                if claim.dispatch_scope is not None
                and failure.error_code in {"transport_error", "unhandled_adapter_error"}
                else failure.billed
            )
            if budget_id is not None and usage_id is not None:
                _settle_budget(
                    connection,
                    usage_id=usage_id,
                    budget_id=budget_id,
                    unit_price=unit_price,
                    billed=settled_billed,
                    details={
                        "state": "failed",
                        "slot_id": claim.slot_id,
                        "error_code": failure.error_code,
                        **(
                            {"transport": transport_receipt}
                            if transport_receipt is not None
                            else {}
                        ),
                        **(
                            {"retry_after_seconds": failure.retry_after_seconds}
                            if failure.retry_after_seconds is not None else {}
                        ),
                    },
                )
                if claim.request_batch_id is not None and settled_billed is not None:
                    connection.execute("""UPDATE admission_reservations SET state='settled',updated_at=?
                        WHERE batch_id=? AND state='sent_unsettled'""", (now_utc(), claim.request_batch_id))
            if (
                claim.dispatch_scope is not None
                and usage_id is not None
                and settled_billed is not None
                and not owns_slot
            ):
                clear_billing_unknown_slot_guard_if_resolved(
                    connection,
                    slot_id=claim.slot_id,
                    fallback_error_code=failure.error_code,
                    fallback_error_message=str(failure)[:500],
                )
            finished_at = now_utc()
            _finish_dispatch_if_open(
                connection,
                claim.dispatch_id,
                outcome="billing_unknown" if settled_billed is None else "failed",
                created_at=finished_at,
                raw_response_id=failure_raw_id,
                reason=failure.error_code,
            )
            if claim.singleton_batch:
                assert claim.request_batch_id is not None
                capture_singletons.record_disposition(connection, batch_id=claim.request_batch_id,
                    attempt_id=claim.attempt_id, raw_response_id=failure_raw_id, disposition="unusable",
                    reason=failure.error_code, at=finished_at)
            if failure.error_code == "storage_hard":
                record_fault_state(
                    connection,
                    scope_kind="storage_hard",
                    fault_class="local_evidence_store",
                    reason=failure.error_code,
                    usage_id=usage_id,
                    at=finished_at,
                    state_evidence={
                        "error_code": failure.error_code,
                        "raw_root": str(resolved_raw_root.resolve()),
                    },
                )
            if provider.lower() == "tikhub":
                if failure.error_code in {
                    "provider_balance_blocked",
                    "provider_auth_blocked",
                }:
                    record_circuit(
                        connection,
                        reason=failure.error_code,
                        usage_id=usage_id,
                        at=finished_at,
                        state_evidence={
                            "error_code": failure.error_code,
                            "http_status": failure.http_status,
                            "credential_fingerprint": (
                                failure.transport_receipt.get(
                                    "credential_fingerprint"
                                )
                                if failure.transport_receipt is not None
                                else None
                            ),
                        },
                    )
                elif failure.error_code in {
                    "provider_rate_limited",
                    "rate_limit_exceeded",
                    "field_contract_invalid",
                    "semantic_error",
                } or failure.http_status == 429:
                    record_fault_state(
                        connection,
                        scope_kind="operation",
                        operation=operation,
                        fault_class=(
                            "rate_limit"
                            if failure.http_status == 429
                            or failure.error_code in {
                                "provider_rate_limited", "rate_limit_exceeded"
                            }
                            else "field_contract"
                        ),
                        reason=failure.error_code,
                        usage_id=usage_id,
                        at=finished_at,
                    )
                elif failure.error_code in {
                    "authorization_token_invalid",
                    "authorization_scope_missing",
                    "authorization_refresh_failed",
                } and claim.dispatch_scope is not None:
                    authorization_id = (
                        claim.dispatch_scope.identity_id
                        or claim.dispatch_scope.account_id
                    )
                    if authorization_id is not None:
                        record_fault_state(
                            connection,
                            scope_kind="authorization_hard",
                            authorization_id=authorization_id,
                            fault_class="account_authorization",
                            reason=failure.error_code,
                            usage_id=usage_id,
                            at=finished_at,
                        )
                if usage_id is not None and settled_billed is None:
                    record_fault_state(
                        connection,
                        scope_kind="paid_identity_hold",
                        paid_identity=str(
                            claim.paid_scope_identity or f"slot:{claim.slot_id}"
                        ),
                        fault_class="billing_unresolved",
                        reason=failure.error_code,
                        usage_id=usage_id,
                        at=finished_at,
                    )
                if failure.error_code == "transport_error":
                    evaluate_transport_operation_fault(
                        connection,
                        operation=operation,
                        at=finished_at,
                        actual_failure_usage_id=usage_id,
                    )
            if (
                claim.dispatch_scope is not None
                and usage_id is not None
                and not recovery_completion_deferred()
            ):
                finish_recovery_probe(
                    connection, usage_id=usage_id, succeeded=False, at=finished_at,
                    reason=failure.error_code,
                )
            if claim.dispatch_scope is not None and usage_id is not None:
                operation_recovery.finish_operation_probe(connection, operation=operation,
                    usage_id=usage_id, succeeded=False, at=finished_at)
            next_status = "retryable_failed" if failure.retryable else "terminal_failed"
            connection.execute(
                """
                UPDATE fetch_attempts
                SET response_finished_at=?, http_status=?, billed=?, amount=?, currency=?,
                    error_code=?, error_message=? WHERE id=?
                """,
                (
                    finished_at,
                    failure.http_status,
                    int(settled_billed is True),
                    unit_price if settled_billed is True else 0.0 if settled_billed is False else None,
                    currency,
                    failure.error_code,
                    str(failure)[:500],
                    claim.attempt_id,
                ),
            )
            if owns_slot:
                billing_unknown = (
                    claim.dispatch_scope is not None and settled_billed is None
                )
                slot_error_code = (
                    BILLING_UNKNOWN_SLOT_ERROR
                    if billing_unknown
                    else failure.error_code
                )
                slot_error_message = (
                    BILLING_UNKNOWN_SLOT_MESSAGE
                    if billing_unknown
                    else str(failure)[:500]
                )
                connection.execute(
                    """
                    UPDATE fetch_slots
                    SET status=?,
                        last_error_code=CASE WHEN last_error_code=? THEN last_error_code ELSE ? END,
                        last_error_message=CASE WHEN last_error_code=? THEN last_error_message ELSE ? END,
                        finished_at=?, updated_at=? WHERE id=?
                    """,
                    (
                        next_status,
                        BILLING_UNKNOWN_SLOT_ERROR,
                        slot_error_code,
                        BILLING_UNKNOWN_SLOT_ERROR,
                        slot_error_message,
                        finished_at,
                        finished_at,
                        claim.slot_id,
                    ),
                )
        raise failure from exc
    if not owns_slot:
        raise CaptureError(
            "Late response retained as audit only", retryable=False,
            error_code="attempt_owner_lost", billed=result.billed,
        )
    return CaptureOutcome(
        slot_id=claim.slot_id, attempt_id=claim.attempt_id, raw_response_id=raw_id,
        data=result.data, billed=result.billed,
        amount=unit_price if result.billed else 0.0, currency=currency,
    )


def execute_request_batch(*, batch_id: int, paid_request_identity: PaidRequestIdentity,
                          member_request_identities: tuple[PaidRequestIdentity, ...],
                          member_assignment_ids: tuple[int, ...], call: Callable[[], ProviderResult],
                          request_transport: Mapping[str, Any] | None, budget_id: str,
                          task_id: str, task_max_amount: float, db_path: Path,
                          raw_root: Path | None = None) -> CaptureOutcome:
    """One statistics HTTP request, using the same A/B/C as individual work.

    The existing slot remains a compatibility owner token only. The network
    attempt belongs exclusively to the immutable request batch, and the raw
    response has no carrier content/account identity.
    """
    from .capture_planning import execution_route_context

    with connect(db_path) as connection:
        if not _schema20(connection):
            raise PaidScopeBlocked("batch_identity_invalid", "Request batches require schema20")
        batch = connection.execute("""SELECT b.*,w.envelope_json FROM fetch_request_batches b
            JOIN capture_work_items w ON w.id=b.work_id WHERE b.id=?""", (batch_id,)).fetchone()
        members = connection.execute("""SELECT m.content_id,c.platform_content_id FROM fetch_request_batch_members m
            JOIN content_items c ON c.id=m.content_id WHERE m.batch_id=? ORDER BY c.platform_content_id""", (batch_id,)).fetchall()
        if batch is None or not members or len(members) > 2 or not member_assignment_ids:
            raise PaidScopeBlocked("batch_identity_invalid", "Frozen batch/owner/members are missing")
        envelope = json.loads(batch['envelope_json'])
        window_key = str(envelope['logical_due'])
        operation = str(batch['operation'])
        content_id = int(members[0]['content_id'])
    with execution_route_context(member_assignment_ids[0]):
        claim = _claim_paid_tikhub(content_id=content_id, account_id=None, stage='metrics',
            window_key=window_key, provider='TikHub', adapter_version='tikhub-statistics-batch-v25',
            operation=operation, db_path=db_path, budget_id=budget_id, task_id=task_id,
            task_max_amount=task_max_amount, allow_terminal_retry=False,
            paid_request_identity=paid_request_identity, request_transport=request_transport,
            request_batch_id=batch_id, member_request_identities=member_request_identities,
            member_assignment_ids=member_assignment_ids)
        return _execute_claimed_fetch(claim=claim, operation=operation, call=call, db_path=db_path,
            raw_root=raw_root, budget_id=budget_id, task_id=task_id, task_max_amount=task_max_amount)


def execute_content_fetch(
    *,
    content_id: int,
    stage: str,
    window_key: str,
    provider: str,
    adapter_version: str,
    operation: str,
    call: Callable[[], ProviderResult],
    db_path: Path = DEFAULT_DB,
    raw_root: Optional[Path] = None,
    budget_id: Optional[str] = None,
    task_id: Optional[str] = None,
    task_max_amount: Optional[float] = None,
    allow_terminal_retry: bool = False,
    paid_request_identity: PaidRequestIdentity | None = None,
    request_transport: Mapping[str, Any] | None = None,
) -> CaptureOutcome:
    _validate_task_budget(
        budget_id=budget_id,
        task_id=task_id,
        task_max_amount=task_max_amount,
        provider=provider,
        operation=operation,
    )
    if provider.lower() == "tikhub":
        owner_at = now_utc()
        with paid_dispatch_owner(
            job_id="paid_capture_direct",
            identity={
                "provider": provider.lower(),
                "operation": operation,
                "content_id": content_id,
                "stage": stage,
                "window_key": window_key,
                "purpose": {
                    "discovery": "reconcile",
                    "detail": "detail",
                    "media_source_refresh": "detail",
                    "metrics": "metrics",
                    "comments": "comments",
                }.get(stage),
            },
            db_path=db_path,
            at=owner_at,
        ):
            claim = _claim_paid_tikhub(
                content_id=content_id, account_id=None, stage=stage,
                window_key=window_key, provider=provider,
                adapter_version=adapter_version, operation=operation,
                db_path=db_path, budget_id=budget_id, task_id=task_id,
                task_max_amount=task_max_amount,
                allow_terminal_retry=allow_terminal_retry,
                paid_request_identity=paid_request_identity,
                request_transport=request_transport,
            )
            return _execute_claimed_fetch(
                claim=claim, operation=operation, call=call, db_path=db_path,
                raw_root=raw_root, budget_id=budget_id, task_id=task_id,
                task_max_amount=task_max_amount,
            )
    claim = claim_content_slot(
        db_path=db_path,
        content_id=content_id,
        stage=stage,
        window_key=window_key,
        provider=provider,
        adapter_version=adapter_version,
        allow_terminal_retry=allow_terminal_retry,
    )
    return _execute_claimed_fetch(
        claim=claim,
        operation=operation,
        call=call,
        db_path=db_path,
        raw_root=raw_root,
        budget_id=budget_id,
        task_id=task_id,
        task_max_amount=task_max_amount,
    )


def execute_account_fetch(
    *,
    account_id: int,
    stage: str,
    window_key: str,
    provider: str,
    adapter_version: str,
    operation: str,
    call: Callable[[], ProviderResult],
    db_path: Path = DEFAULT_DB,
    raw_root: Optional[Path] = None,
    budget_id: Optional[str] = None,
    task_id: Optional[str] = None,
    task_max_amount: Optional[float] = None,
    allow_terminal_retry: bool = False,
    paid_request_identity: PaidRequestIdentity | None = None,
    request_transport: Mapping[str, Any] | None = None,
) -> CaptureOutcome:
    _validate_task_budget(
        budget_id=budget_id,
        task_id=task_id,
        task_max_amount=task_max_amount,
        provider=provider,
        operation=operation,
    )
    if provider.lower() == "tikhub":
        owner_at = now_utc()
        with paid_dispatch_owner(
            job_id="paid_capture_direct",
            identity={
                "provider": provider.lower(),
                "operation": operation,
                "account_id": account_id,
                "stage": stage,
                "window_key": window_key,
                "purpose": {
                    "discovery": "reconcile",
                    "detail": "detail",
                    "media_source_refresh": "detail",
                    "metrics": "metrics",
                    "comments": "comments",
                }.get(stage),
            },
            db_path=db_path,
            at=owner_at,
        ):
            claim = _claim_paid_tikhub(
                content_id=None, account_id=account_id, stage=stage,
                window_key=window_key, provider=provider,
                adapter_version=adapter_version, operation=operation,
                db_path=db_path, budget_id=budget_id, task_id=task_id,
                task_max_amount=task_max_amount,
                allow_terminal_retry=allow_terminal_retry,
                paid_request_identity=paid_request_identity,
                request_transport=request_transport,
            )
            return _execute_claimed_fetch(
                claim=claim, operation=operation, call=call, db_path=db_path,
                raw_root=raw_root, budget_id=budget_id, task_id=task_id,
                task_max_amount=task_max_amount,
            )
    claim = claim_account_slot(
        db_path=db_path,
        account_id=account_id,
        stage=stage,
        window_key=window_key,
        provider=provider,
        adapter_version=adapter_version,
        allow_terminal_retry=allow_terminal_retry,
    )
    return _execute_claimed_fetch(
        claim=claim,
        operation=operation,
        call=call,
        db_path=db_path,
        raw_root=raw_root,
        budget_id=budget_id,
        task_id=task_id,
        task_max_amount=task_max_amount,
    )


def execute_derived_content_fetch(
    *, content_id: int, stage: str, window_key: str, provider: str,
    adapter_version: str, operation: str, result: ProviderResult,
    source_raw_response_id: int, db_path: Path = DEFAULT_DB,
    raw_root: Optional[Path] = None,
    allow_terminal_retry: bool = False,
) -> CaptureOutcome:
    """Pure local evidence derivation: accepts a value, never a network closure."""
    if result.billed or not isinstance(result.raw_response, dict):
        raise BudgetBlocked("Derived evidence must be an unbilled local result")
    if result.raw_response.get("source_raw_response_id") != source_raw_response_id:
        raise RawResponseIntegrityError("Derived source raw reference does not match")
    with connect(db_path) as connection:
        source = connection.execute(
            "SELECT * FROM provider_raw_responses WHERE id=?", (source_raw_response_id,)
        ).fetchone()
        content = connection.execute(
            "SELECT account_id,platform,platform_content_id,link_id FROM content_items WHERE id=?", (content_id,)
        ).fetchone()
        if source is None or content is None:
            raise RawResponseIntegrityError("Derived source or content is missing")
        if source["content_id"] not in {None, content_id}:
            raise RawResponseIntegrityError("Derived raw belongs to another content")
        if source["account_id"] is not None and source["account_id"] != content["account_id"]:
            raise RawResponseIntegrityError("Derived raw belongs to another account")
        if source["content_id"] is None:
            attributable = (
                source["account_id"] is not None
                and source["account_id"] == content["account_id"]
            )
            if source["account_id"] is None:
                if (stage != "comments" or source["operation"] != "matrix_works_list"
                        or not 200 <= int(source["http_status"] or 0) < 300
                        or not isinstance(result.data, dict)
                        or type(result.data.get("comment_count")) is not int
                        or result.data.get("comment_count") != 0
                        or result.data.get("comments") != []):
                    raise RawResponseIntegrityError("Global Matrix raw only supports evidenced zero comments")
                identity = connection.execute(
                    """SELECT platform_identity_key FROM content_identities WHERE content_id=?
                       ORDER BY is_primary DESC,id LIMIT 1""", (content_id,),
                ).fetchone()
                subject_key = str(identity[0]) if identity is not None else f"link:{content['link_id']}"
                attributable = source["provider"] == "newrank_matrix" and connection.execute(
                    """SELECT 1 FROM content_metric_observations
                       WHERE content_id=? AND raw_response_id=?
                         AND subject_key=? AND status='available'
                         AND observation_origin='provider_capture'
                         AND source='newrank_matrix'
                         AND COALESCE(json_extract(metadata_json,'$.fields.comment_count.status'),
                                      json_extract(metadata_json,'$.field_status.comment_count.status'),
                                      'provided')='provided'
                         AND (? != 'comments' OR comment_count=0) LIMIT 1""",
                    (content_id, source_raw_response_id, subject_key, stage),
                ).fetchone() is not None
            if not attributable:
                raise RawResponseIntegrityError("Global raw has no evidence for this content")
        if result.raw_response.get("source_sha256") not in {None, source["sha256"]}:
            raise RawResponseIntegrityError("Derived source SHA-256 does not match")
        _read_verified_raw_response(source, connection=connection)
    claim = claim_content_slot(
        db_path=db_path, content_id=content_id, stage=stage, window_key=window_key,
        provider=provider, adapter_version=adapter_version,
        allow_terminal_retry=allow_terminal_retry,
    )
    return _execute_claimed_fetch(
        claim=claim, operation=operation, call=lambda: result,
        db_path=db_path, raw_root=raw_root, local_result=result,
    )


def evaluate_pilot_gate(
    budget_id: str,
    *,
    attempted: int,
    media_recovered: int,
    evidence_ready: int,
    db_path: Path = DEFAULT_DB,
) -> Dict[str, Any]:
    if attempted <= 0:
        raise ValueError("attempted must be positive")
    media_rate = media_recovered / attempted
    evidence_rate = evidence_ready / attempted
    approved = media_rate >= 0.70 and evidence_rate >= 0.60
    with connect(db_path) as connection, transaction(connection):
        row = connection.execute(
            "SELECT status FROM provider_budget_batches WHERE id=?", (budget_id,)
        ).fetchone()
        if row is None:
            raise BudgetBlocked(f"budget {budget_id} does not exist")
        if row["status"] != "suspended":
            raise BudgetBlocked("pilot budget must be suspended before evaluation")
        connection.execute(
            "UPDATE provider_budget_batches SET status=?, updated_at=? WHERE id=?",
            ("approved" if approved else "suspended", now_utc(), budget_id),
        )
    return {
        "attempted": attempted,
        "media_recovered": media_recovered,
        "evidence_ready": evidence_ready,
        "media_recovery_rate": round(media_rate, 4),
        "evidence_ready_rate": round(evidence_rate, 4),
        "approved": approved,
    }
