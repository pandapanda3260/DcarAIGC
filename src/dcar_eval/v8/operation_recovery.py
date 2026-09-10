"""Bounded recovery for transient operation faults using existing fault events."""
from __future__ import annotations

import fcntl
import hashlib
import json
import math
import sqlite3
from contextlib import contextmanager
from contextvars import ContextVar
from datetime import datetime, timedelta, timezone
from email.utils import parsedate_to_datetime
from pathlib import Path
from typing import Iterator

from . import provider_budget as budget
from .operation_contracts import OperationContractError, SUPPORTED_OPERATIONS, require_response
from .storage import PROJECT_ROOT, connect

TEMPORARY_FAULTS = frozenset({"transport", "rate_limit"})
PROBE_LEASE_SECONDS = 120
_LOCK: ContextVar[tuple[str, str] | None] = ContextVar("operation_probe_lock", default=None)


def _states(connection: sqlite3.Connection, operation: str) -> list[dict]:
    return [state for state in budget._v2_fault_states(connection, {
        "scope_kind": "operation", "provider": "tikhub", "operation": operation,
    }) if state.get("open") is True]


def operation_faults_allow_authority(connection: sqlite3.Connection, *, operation: str) -> bool:
    """Qualification may renew offline during transient network cooldowns."""
    return all(state["fault_class"] in TEMPORARY_FAULTS for state in _states(connection, operation))


def _utc(value) -> str:
    return value.astimezone(timezone.utc).isoformat(timespec="seconds").replace("+00:00", "Z")


def _provider_retry_after(connection: sqlite3.Connection, usage_id: int | None,
                          failed_at: str) -> datetime | None:
    row = connection.execute("SELECT details_json FROM provider_usage WHERE id=?", (usage_id,)).fetchone()
    if row is None:
        return None
    return _retry_after(budget._details(row["details_json"]), failed_at)


def _retry_after(details: dict, failed_at: str) -> datetime | None:
    transport = details.get("transport")
    transport = transport if isinstance(transport, dict) else {}
    values = [details.get("retry_after_seconds"), transport.get("retry_after")]
    deadlines = []
    for value in values:
        if value is None or isinstance(value, bool):
            continue
        try:
            seconds = float(value)
            if math.isfinite(seconds) and seconds >= 0:
                deadlines.append(budget._time(failed_at) + timedelta(seconds=seconds))
                continue
        except (TypeError, ValueError, OverflowError):
            pass
        if isinstance(value, str):
            try:
                parsed = parsedate_to_datetime(value)
                if parsed.tzinfo is not None:
                    deadlines.append(parsed)
            except (TypeError, ValueError, OverflowError):
                pass
    return max(deadlines) if deadlines else None


def _usage_failure_deadline(connection: sqlite3.Connection, state: dict) -> datetime | None:
    """Recover failures an older Writer retained without advancing its fault.

    Stream only actual failed sends for this operation. The send-marker and
    dispatch indexes bind completion times without a query per usage. No cache
    survives a transaction: B must see newly committed or late failures.
    """
    ledger = connection.execute("SELECT 1 FROM sqlite_master WHERE type='table' "
        "AND name='paid_provider_dispatch_events'").fetchone() is not None
    completion = "terminal.created_at" if ledger else "NULL"
    joins = """LEFT JOIN paid_provider_dispatch_events sent
        ON sent.provider_usage_id=u.id AND sent.event_type='send_marked'
        LEFT JOIN paid_provider_dispatch_events terminal
        ON terminal.dispatch_id=sent.dispatch_id AND terminal.sequence=3
        AND terminal.event_type IN ('failed','billing_unknown')""" if ledger else ""
    rows = connection.execute(f"""SELECT u.recorded_at,u.details_json,{completion} completed_at
        FROM provider_usage u {joins}
        WHERE lower(u.provider)='tikhub' AND u.operation=? AND u.request_attempts=1
        AND json_valid(u.details_json)
        AND json_extract(u.details_json,'$.state') NOT IN ('reserved','sent','not_sent')
        AND (json_extract(u.details_json,'$.error_code') IN
            ('transport_error','provider_rate_limited','rate_limit_exceeded')
            OR json_extract(u.details_json,'$.transport.http_status')=429
            OR json_extract(u.details_json,'$.http_status')=429
            OR json_extract(u.details_json,'$.transport.clean_eof')=0
            OR json_extract(u.details_json,'$.transport.length_match')=0
            OR json_extract(u.details_json,'$.transport.gzip_crc_ok')=0)""", (state["operation"],))
    deadline = None
    for row in rows:
        details = budget._details(row["details_json"])
        transport = details.get("transport")
        transport = transport if isinstance(transport, dict) else {}
        error = details.get("error_code")
        error = error if isinstance(error, str) else None
        limited = (error in {"provider_rate_limited", "rate_limit_exceeded"}
            or transport.get("http_status") == 429 or details.get("http_status") == 429)
        hard = error in {"storage_hard", "provider_auth_blocked", "provider_balance_blocked",
            "field_contract_invalid", "semantic_error", "recovery_response_contract_invalid",
            "authorization_token_invalid", "authorization_scope_missing", "authorization_refresh_failed"}
        uncertain = error == "transport_error" or any(transport.get(key) is False
            for key in ("clean_eof", "length_match", "gzip_crc_ok"))
        matches = not hard and (limited if state["fault_class"] == "rate_limit" else uncertain and not limited)
        if not matches:
            continue
        timestamps = []
        for value in (row["recorded_at"], details.get("sent_at"), row["completed_at"]):
            if isinstance(value, str):
                try:
                    timestamps.append(budget._time(value))
                except (budget.BudgetBlocked, TypeError, ValueError, OverflowError):
                    pass
        if not timestamps:
            continue
        failed_at = max(timestamps)
        due = failed_at + timedelta(minutes=5)
        provider_due = _retry_after(details, _utc(failed_at))
        if provider_due is not None:
            due = max(due, provider_due)
        deadline = max(deadline, due) if deadline is not None else due
    return deadline


def _ready(connection: sqlite3.Connection, state: dict, at: str) -> bool:
    cooldown = state.get("cooldown", {})
    due = cooldown.get("retry_after") or _utc(
        budget._time(str(state["last_failure_at"])) + timedelta(minutes=5))
    deadline = budget._time(due)
    provider_due = _provider_retry_after(connection, state.get("usage_id"), str(state["last_failure_at"]))
    if provider_due is not None:
        deadline = max(deadline, provider_due)
    retained_due = _usage_failure_deadline(connection, state)
    if retained_due is not None:
        deadline = max(deadline, retained_due)
    lease = state.get("half_open", {})
    return budget._time(at) >= deadline and (
        not lease or budget._time(at) >= budget._time(lease["expires_at"]))


def operation_recovery_due(connection: sqlite3.Connection, *, operation: str, at: str) -> bool:
    """Read-only admission hint; the send boundary must still claim one probe."""
    states = _states(connection, operation)
    return operation in SUPPORTED_OPERATIONS and bool(states) and all(state["fault_class"] in TEMPORARY_FAULTS and _ready(connection, state, at)
                                for state in states)


def _append(connection: sqlite3.Connection, state: dict, at: str) -> None:
    scope = {key: state[key] for key in ("scope_kind", "provider", "operation")}
    budget._write_receipt(connection, budget._fault_job_id(scope, state["fault_class"]),
                          {key: value for key, value in state.items() if key != "receipt_id"},
                          at, status="partial" if state["open"] else "succeeded")


@contextmanager
def operation_probe_lock(*, db_path: Path, operation: str) -> Iterator[None]:
    """A crashed process releases flock; a slow live probe never overlaps another."""
    with connect(db_path) as connection:
        needed = bool(_states(connection, operation))
    if not needed:
        yield
        return
    directory = db_path.parent / "operation_probe_locks"
    directory.mkdir(mode=0o700, parents=True, exist_ok=True)
    name = hashlib.sha256((str(db_path.resolve()) + ":" + operation).encode()).hexdigest()
    with (directory / name).open("a+b") as handle:
        try:
            fcntl.flock(handle.fileno(), fcntl.LOCK_EX | fcntl.LOCK_NB)
        except BlockingIOError:
            # Let B reject the missing lock inside its existing rollback/refund
            # path so a failed contender leaves no reservation or running slot.
            token = _LOCK.set(None)
            try:
                yield
            finally:
                _LOCK.reset(token)
            return
        token = _LOCK.set((str(db_path.resolve()), operation))
        try:
            yield
        finally:
            _LOCK.reset(token)
            fcntl.flock(handle.fileno(), fcntl.LOCK_UN)


def require_operation_lock(connection: sqlite3.Connection, *, operation: str) -> None:
    """Every send through an open operation fault shares the same OS lock."""
    if not _states(connection, operation):
        return
    database = Path(connection.execute("PRAGMA database_list").fetchone()[2]).resolve()
    if _LOCK.get() != (str(database), operation):
        raise budget.PaidScopeBlocked("operation_blocked", "Operation recovery probe is in flight")


def claim_operation_probe(connection: sqlite3.Connection, *, operation: str,
                          usage_id: int, identity: str, sequence: int, at: str) -> None:
    """B: after eligibility checks, before any irreversible paid-send file."""
    states = _states(connection, operation)
    if not states:
        return
    require_operation_lock(connection, operation=operation)
    if sequence != 0 or not operation_recovery_due(connection, operation=operation, at=at):
        raise budget.PaidScopeBlocked("operation_blocked", "Operation cooldown or probe owner blocks this send")
    row = connection.execute("SELECT * FROM provider_usage WHERE id=?", (usage_id,)).fetchone()
    details = budget._details(row["details_json"]) if row is not None else {}
    if (row is None or row["provider"].lower() != "tikhub" or row["operation"] != operation
            or (row["request_attempts"], details.get("state")) not in {(0, "reserved"), (1, "sent")}
            or details.get("paid_scope_identity") != identity):
        raise budget.PaidScopeBlocked("operation_blocked", "Recovery probe has no matching send reservation")
    # Probe only a new identity. Do not spend again on any prior network start,
    # including not-billed failures and unresolved billing from a dead process.
    previous = connection.execute("""SELECT 1 FROM provider_usage WHERE id<>? AND lower(provider)='tikhub'
        AND request_attempts>0 AND json_extract(details_json,'$.paid_scope_identity')=? LIMIT 1""",
        (usage_id, identity)).fetchone()
    if previous is not None:
        raise budget.PaidScopeBlocked("operation_blocked", "Recovery needs a fresh request identity")
    lease = {"usage_id": usage_id, "identity": identity, "started_at": at,
             "expires_at": _utc(budget._time(at) + timedelta(seconds=PROBE_LEASE_SECONDS))}
    for state in states:
        state["half_open"] = lease
        _append(connection, state, at)


def _verified_success(connection: sqlite3.Connection, usage_id: int, raw_response_id: int | None) -> bool:
    from . import raw_archive
    from .raw_evidence import read_raw_evidence
    row = connection.execute("SELECT * FROM provider_usage WHERE id=?", (usage_id,)).fetchone()
    raw = connection.execute("SELECT * FROM provider_raw_responses WHERE id=?", (raw_response_id,)).fetchone()
    if row is None or raw is None or row["request_attempts"] != 1:
        return False
    details = budget._details(row["details_json"])
    transport = details.get("transport", {})
    if (details.get("state") != "completed" or details.get("raw_response_id") != raw_response_id
            or raw["provider"].lower() != "tikhub" or raw["operation"] != row["operation"]
            or transport.get("clean_eof") is not True or transport.get("json_parse_ok") is not True
            or transport.get("length_match") is False or transport.get("gzip_crc_ok") is False
            or type(transport.get("http_status")) is not int or not 200 <= transport["http_status"] < 300):
        return False
    if connection.execute("PRAGMA user_version").fetchone()[0] in {20, 21}:
        entity = raw_archive.read_response_entity(connection, raw_response_id)
    else:
        path = Path(raw["local_path"])
        loaded = read_raw_evidence(path if path.is_absolute() else PROJECT_ROOT / path,
                                   expected_stored_sha256=raw["sha256"],
                                   expected_stored_size=raw["byte_size"])
        entity = loaded.entity_bytes
    if hashlib.sha256(entity).hexdigest() != transport.get("entity_sha256"):
        return False
    try:
        payload = json.loads(entity)
        scope = details.get("scope", {})
        if not isinstance(scope, dict):
            raise OperationContractError("Recovery response has invalid frozen scope metadata")
        require_response(row["operation"], details.get("paid_identity", {}), payload,
                         account_uid=scope.get("uid"))
    except (ValueError, TypeError, AttributeError, RecursionError) as error:
        raise OperationContractError("Recovery response has invalid business evidence") from error
    return True


def finish_operation_probe(connection: sqlite3.Connection, *, operation: str, usage_id: int,
                           at: str, succeeded: bool, raw_response_id: int | None = None) -> None:
    """C transaction: retain billing/identity fences and close only verified probes."""
    states = _states(connection, operation)
    owned = [state for state in states if state.get("half_open", {}).get("usage_id") == usage_id]
    if not owned:
        return
    try:
        success = succeeded and _verified_success(connection, usage_id, raw_response_id)
    except OperationContractError:
        budget.record_fault_state(connection, scope_kind="operation", operation=operation,
            fault_class="field_contract", reason="recovery_response_contract_invalid", usage_id=usage_id, at=at)
        success = False
    for state in states:
        if state["fault_class"] not in TEMPORARY_FAULTS:
            continue
        if state not in owned and (success or state.get("usage_id") != usage_id):
            continue
        if success:
            # A fault opened by another failure after this probe started is not
            # recovery evidence for that newer failure.
            if state not in owned:
                continue
            state.update(open=False, recovered_at=at, recovery_evidence_id=raw_response_id)
            state.pop("half_open", None)
        else:
            failures = int(state.get("cooldown", {}).get("failures", 0)) + 1
            minutes = min(30, 5 * 2 ** min(failures, 3))
            deadline = budget._time(at) + timedelta(minutes=minutes)
            provider_due = _provider_retry_after(connection, usage_id, at)
            if provider_due is not None:
                deadline = max(deadline, provider_due)
            existing_due = state.get("cooldown", {}).get("retry_after")
            if existing_due:
                deadline = max(deadline, budget._time(existing_due))
            state.update(last_failure_at=at, usage_id=usage_id, cooldown={"failures": failures,
                "retry_after": _utc(deadline)})
            state.pop("half_open", None)
        _append(connection, state, at)
