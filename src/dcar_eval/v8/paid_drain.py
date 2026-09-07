"""DB-authoritative schema-18 bridge for paid-provider drain events.

The bridge deliberately reuses ``scheduler_runs`` and its immutable attempt
history.  Every START, SEALED and RELEASE event is inserted and finalized in
one ``BEGIN IMMEDIATE`` transaction.  A process crash can therefore expose
either no event or one terminal, verifiable event; startup recovery never sees
a long-running drain receipt.

External JSON mirrors are audit copies only.  Dispatch authority is derived
solely from the event chain stored in SQLite.
"""

from __future__ import annotations

import hashlib
import json
import os
import re
import sqlite3
import stat
from dataclasses import dataclass
from datetime import date
from pathlib import Path
from typing import Any, Literal, Mapping, Sequence
from zoneinfo import ZoneInfo

from .provider_budget import BudgetBlocked
from .source_routing import parse_time
from .storage import (
    DEFAULT_DB,
    connect,
    is_formal_database_path,
    now_utc,
    transaction,
    transaction_metrics_context,
)

BRIDGE_JOB = "pipeline_paid_drain_bridge"
CONTRACT_VERSION = "pipeline-paid-drain-bridge-v1"
PROFILE_CONTRACT_VERSION = "pipeline-paid-drain-v2"
CURRENT_ACTIVATION_HOLD_CONTRACT = "current_activation_hold_v1"
EVENT_TYPES = ("start", "sealed", "release")
_EVENT_SEQUENCE = {name: index for index, name in enumerate(EVENT_TYPES, start=1)}
_PROFILE_EVENT_SEQUENCE = {**_EVENT_SEQUENCE, "ABORT_RESTORE": 4}
_DRAIN_ID = re.compile(r"[A-Za-z0-9][A-Za-z0-9_.:-]{0,127}")
_SHA256 = re.compile(r"[0-9a-f]{64}")
_BEIJING = ZoneInfo("Asia/Shanghai")
_REQUIRED_BINDING_FIELDS = frozenset(
    {
        "source_activation_id",
        "target_activation_id",
        "business_day",
        "planned_effective_at",
        "build_receipt_sha256",
        "runtime_root_receipt_sha256",
    }
)
_OPTIONAL_BINDING_FIELDS = frozenset({"not_before_business_day"})

# These jobs can own a paid provider call in the current pipeline.  A
# pipeline_round is classified from its frozen identity below, so report-only
# rounds do not hold a drain open.
_PAID_JOB_IDS = frozenset(
    {
        "daily_capture",
        "content_pipeline",
        "comments_refresh",
        "metrics_backfill",
        "history_recovery",
        "history_scan_catalog",
        "pipeline_reconcile",
        "paid_capture_direct",
        "tikhub_reconcile",
        "matrix_works_scan",
        "matrix_account_metrics",
        "transport_diagnostic_operator",
    }
)
_PAID_ROUND_JOBS = frozenset(
    {
        "matrix_works_scan",
        "matrix_account_metrics",
        "tikhub_reconcile",
        "tikhub_works_scan",
        "tikhub_account_metrics",
        "metrics_backfill",
    }
)
_MATRIX_JOBS = frozenset({"matrix_works_scan", "matrix_account_metrics"})


class PaidDrainError(RuntimeError):
    """A drain management operation or bridge receipt is invalid."""


class PaidDrainBlocked(BudgetBlocked):
    """Paid dispatch is closed by a valid drain or invalid bridge chain."""

    error_code = "profile_switch_drain"

    def __init__(
        self,
        message: str,
        *,
        drain_id: str | None = None,
        error_code: str | None = None,
    ) -> None:
        self.drain_id = drain_id
        if error_code is not None:
            self.error_code = error_code
        super().__init__(message)


@dataclass(frozen=True)
class DrainReceipt:
    run_id: int
    attempt_id: int
    drain_id: str
    event_type: Literal["start", "sealed", "release"]
    sequence: int
    created_at: str
    event_hash: str
    previous_event_id: int | None
    previous_event_hash: str | None
    payload: dict[str, Any]

    def as_dict(self) -> dict[str, Any]:
        return {
            "run_id": self.run_id,
            "attempt_id": self.attempt_id,
            "drain_id": self.drain_id,
            "event_type": self.event_type,
            "sequence": self.sequence,
            "created_at": self.created_at,
            "event_hash": self.event_hash,
            "previous_event_id": self.previous_event_id,
            "previous_event_hash": self.previous_event_hash,
            "payload": self.payload,
        }


@dataclass(frozen=True)
class DrainState:
    state: Literal["open", "draining", "sealed", "closed", "invalid"]
    drain_id: str | None = None
    last_event_id: int | None = None
    last_event_hash: str | None = None
    reason: str | None = None
    activation_id: int | None = None
    permit_event_id: int | None = None

    @property
    def paid_dispatch_open(self) -> bool:
        return self.state == "open"


@dataclass(frozen=True)
class _ProfileDrainEvent:
    event_id: int
    target_activation_id: int
    drain_id: str
    event_type: Literal["start", "sealed", "release", "ABORT_RESTORE"]
    sequence: int
    created_at: str
    event_hash: str
    previous_event_id: int | None
    previous_event_hash: str | None
    payload: dict[str, Any]
    contract_version: str
    bridge_run_id: int | None
    bridge_attempt_id: int | None

    def as_dict(self) -> dict[str, Any]:
        return {
            "event_id": self.event_id,
            "target_activation_id": self.target_activation_id,
            "drain_id": self.drain_id,
            "event_type": self.event_type,
            "sequence": self.sequence,
            "created_at": self.created_at,
            "event_hash": self.event_hash,
            "previous_event_id": self.previous_event_id,
            "previous_event_hash": self.previous_event_hash,
            "payload": _clone(self.payload),
            "contract_version": self.contract_version,
            "bridge_run_id": self.bridge_run_id,
            "bridge_attempt_id": self.bridge_attempt_id,
        }


def _json(value: Any) -> str:
    try:
        return json.dumps(
            value,
            ensure_ascii=False,
            sort_keys=True,
            separators=(",", ":"),
            allow_nan=False,
        )
    except (TypeError, ValueError) as exc:
        raise PaidDrainError("paid drain payload is not canonical JSON") from exc


def _clone(value: Any) -> Any:
    return json.loads(_json(value))


def _time(value: str | None) -> str:
    try:
        return (
            parse_time(value or now_utc())
            .isoformat(timespec="seconds")
            .replace("+00:00", "Z")
        )
    except (TypeError, ValueError) as exc:
        raise PaidDrainError("paid drain timestamp must include a timezone") from exc


def _strict_int(value: object, *, minimum: int = 1) -> bool:
    return type(value) is int and value >= minimum


def _digest(value: Any) -> str:
    return hashlib.sha256(_json(value).encode("utf-8")).hexdigest()


def _event_digest(event: Mapping[str, Any]) -> str:
    hashed = {key: value for key, value in event.items() if key != "event_hash"}
    return _digest(hashed)


def _require_transaction(connection: sqlite3.Connection) -> None:
    if not connection.in_transaction:
        raise PaidDrainError("paid drain gate requires a caller BEGIN IMMEDIATE transaction")


def _validate_drain_id(drain_id: str) -> str:
    if not isinstance(drain_id, str) or _DRAIN_ID.fullmatch(drain_id) is None:
        raise PaidDrainError("drain_id is invalid")
    return drain_id


def _validate_binding(binding: Mapping[str, Any]) -> dict[str, Any]:
    if (
        not isinstance(binding, Mapping)
        or not _REQUIRED_BINDING_FIELDS.issubset(binding)
        or not set(binding).issubset(
            _REQUIRED_BINDING_FIELDS | _OPTIONAL_BINDING_FIELDS
        )
    ):
        raise PaidDrainError("paid drain START binding fields are incomplete or unknown")
    frozen = _clone(dict(binding))
    for field in ("source_activation_id", "target_activation_id"):
        value = frozen[field]
        if not ((_strict_int(value)) or (isinstance(value, str) and bool(value.strip()))):
            raise PaidDrainError(f"{field} must be a stable activation identifier")
    try:
        if date.fromisoformat(str(frozen["business_day"])).isoformat() != frozen["business_day"]:
            raise ValueError
    except (TypeError, ValueError) as exc:
        raise PaidDrainError("business_day must be YYYY-MM-DD") from exc
    not_before = frozen.get("not_before_business_day")
    if not_before is not None:
        try:
            if date.fromisoformat(str(not_before)).isoformat() != not_before:
                raise ValueError
        except (TypeError, ValueError) as exc:
            raise PaidDrainError(
                "not_before_business_day must be YYYY-MM-DD"
            ) from exc
    _time(str(frozen["planned_effective_at"]))
    for field in ("build_receipt_sha256", "runtime_root_receipt_sha256"):
        if not isinstance(frozen[field], str) or _SHA256.fullmatch(frozen[field]) is None:
            raise PaidDrainError(f"{field} must be a lowercase SHA-256")
    return frozen


def _max_id(connection: sqlite3.Connection, table: str) -> int:
    # Table names are module constants, never caller-controlled.
    return int(connection.execute(f"SELECT COALESCE(MAX(id),0) FROM {table}").fetchone()[0])


def _optional_max_id(connection: sqlite3.Connection, table: str) -> int:
    # Schema 18 has raw responses but no paid dispatch ledger. These new START
    # fields are prospective evidence, never a reason to rewrite old receipts.
    exists = connection.execute(
        "SELECT 1 FROM sqlite_master WHERE type='table' AND name=?", (table,),
    ).fetchone()
    return _max_id(connection, table) if exists is not None else 0


def _current_tikhub_dispatches(
    connection: sqlite3.Connection, *, high_watermark: int
) -> tuple[list[dict[str, int]], list[dict[str, int]]]:
    """Return strict current-generation reservations and send markers.

    Old rows without a v2/v3 policy/scope/slot identity are deliberately not
    reclassified.  They remain in the accounting ledger, but are not mistaken
    for a network request that was active at this START boundary.
    """

    reserved: list[dict[str, int]] = []
    sent: list[dict[str, int]] = []
    rows = connection.execute(
        "SELECT id,request_attempts,details_json FROM provider_usage "
        "WHERE lower(provider)='tikhub' AND id<=? ORDER BY id",
        (high_watermark,),
    )
    for row in rows:
        try:
            details = json.loads(str(row["details_json"] or "{}"))
        except (TypeError, ValueError):
            continue
        if (
            not isinstance(details, dict)
            or details.get("policy_version") not in {
                "tikhub-global-budget-v2", "tikhub-global-budget-v3",
            }
            or not isinstance(details.get("scope"), dict)
            or not isinstance(details.get("category"), str)
            or not isinstance(details.get("budget_day"), str)
            or not _strict_int(details.get("slot_id"))
            or not _strict_int(details.get("attempt_number"))
        ):
            continue
        entry = {
            "usage_id": int(row["id"]),
            "slot_id": int(details["slot_id"]),
            "attempt_number": int(details["attempt_number"]),
        }
        state = details.get("state")
        attempt = connection.execute(
            "SELECT id FROM fetch_attempts WHERE slot_id=? AND attempt_number=?",
            (entry["slot_id"], entry["attempt_number"]),
        ).fetchone()
        if state == "reserved" and row["request_attempts"] == 0 and attempt is None:
            reserved.append(entry)
        elif state == "sent" and row["request_attempts"] == 1 and attempt is not None:
            sent.append({**entry, "fetch_attempt_id": int(attempt["id"])})
    return reserved, sent


def _run_details(row: sqlite3.Row) -> dict[str, Any]:
    try:
        details = json.loads(str(row["details_json"] or "{}"))
    except (TypeError, ValueError):
        return {}
    return details if isinstance(details, dict) else {}


def _paid_capable_run(row: sqlite3.Row) -> bool:
    job_id = str(row["job_id"])
    if job_id in _PAID_JOB_IDS:
        return True
    if not job_id.startswith("pipeline_round:"):
        return False
    identity = _run_details(row).get("identity")
    return isinstance(identity, dict) and identity.get("job_id") in _PAID_ROUND_JOBS


def _network_requests(details: Mapping[str, Any]) -> int:
    checkpoint = details.get("checkpoint")
    value = checkpoint.get("network_requests", 0) if isinstance(checkpoint, dict) else 0
    return value if type(value) is int and value >= 0 else 0


def _active_paid_attempts(connection: sqlite3.Connection) -> list[dict[str, Any]]:
    entries: list[dict[str, Any]] = []
    rows = connection.execute(
        "SELECT r.id run_id,r.job_id,r.details_json,a.id attempt_id "
        "FROM scheduler_runs r JOIN scheduler_run_attempts a "
        "ON a.scheduler_run_id=r.id AND a.status='running' "
        "WHERE r.status='running' ORDER BY r.id,a.id"
    )
    for row in rows:
        if not _paid_capable_run(row):
            continue
        details = _run_details(row)
        entry: dict[str, Any] = {
            "run_id": int(row["run_id"]),
            "attempt_id": int(row["attempt_id"]),
            "job_id": str(row["job_id"]),
        }
        if row["job_id"] in _MATRIX_JOBS:
            entry["matrix_network_requests"] = _network_requests(details)
        entries.append(entry)
    return entries


def _freeze_start(connection: sqlite3.Connection) -> dict[str, Any]:
    provider_hwm = _max_id(connection, "provider_usage")
    fetch_hwm = _max_id(connection, "fetch_attempts")
    scheduler_hwm = _max_id(connection, "scheduler_runs")
    reserved, sent = _current_tikhub_dispatches(
        connection, high_watermark=provider_hwm
    )
    active_attempts = _active_paid_attempts(connection)
    matrix = [entry for entry in active_attempts if entry["job_id"] in _MATRIX_JOBS]
    return {
        "provider_usage_high_watermark": provider_hwm,
        "fetch_attempt_high_watermark": fetch_hwm,
        "scheduler_run_high_watermark": scheduler_hwm,
        "scheduler_attempt_high_watermark": _max_id(connection, "scheduler_run_attempts"),
        "raw_response_high_watermark": _optional_max_id(connection, "provider_raw_responses"),
        "dispatch_event_high_watermark": _optional_max_id(connection, "paid_provider_dispatch_events"),
        "tikhub_reserved": reserved,
        "tikhub_reserved_sha256": _digest(reserved),
        "tikhub_send_marked": sent,
        "tikhub_send_marked_sha256": _digest(sent),
        "paid_running_attempts": active_attempts,
        "paid_running_attempts_sha256": _digest(active_attempts),
        "matrix_running_dispatches": matrix,
        "matrix_running_dispatches_sha256": _digest(matrix),
    }


def _details_to_receipt(details: Mapping[str, Any]) -> DrainReceipt:
    event = details["event"]
    return DrainReceipt(
        run_id=int(event["bridge_run_id"]),
        attempt_id=int(event["bridge_attempt_id"]),
        drain_id=str(event["drain_id"]),
        event_type=str(event["event_type"]),  # type: ignore[arg-type]
        sequence=int(event["sequence"]),
        created_at=str(event["created_at"]),
        event_hash=str(event["event_hash"]),
        previous_event_id=event.get("previous_event_id"),
        previous_event_hash=event.get("previous_event_hash"),
        payload=_clone(event["payload"]),
    )


def _validated_chain(connection: sqlite3.Connection) -> list[DrainReceipt]:
    receipts: list[DrainReceipt] = []
    active_drain: str | None = None
    previous: DrainReceipt | None = None
    # The immutable attempt is the authority. Include rows discovered through
    # either side of the pair so mutating the scheduler run's job_id/details
    # cannot hide an active drain and reopen dispatch.
    run_ids = {
        int(row[0])
        for row in connection.execute(
            "SELECT id FROM scheduler_runs WHERE job_id=? UNION "
            "SELECT scheduler_run_id FROM scheduler_run_attempts "
            "WHERE json_extract(details_json,'$.contract_version')=?",
            (BRIDGE_JOB, CONTRACT_VERSION),
        )
    }
    rows = [
        connection.execute("SELECT * FROM scheduler_runs WHERE id=?", (run_id,)).fetchone()
        for run_id in sorted(run_ids)
    ]
    for row in rows:
        if row is None:
            raise PaidDrainError("paid drain authoritative attempt lost its run")
        attempts = connection.execute(
            "SELECT * FROM scheduler_run_attempts WHERE scheduler_run_id=? ORDER BY attempt_number",
            (row["id"],),
        ).fetchall()
        if (
            row["job_id"] != BRIDGE_JOB
            or row["status"] != "succeeded"
            or row["completed_at"] is None
            or len(attempts) != 1
            or attempts[0]["attempt_number"] != 1
            or attempts[0]["invocation_source"] != "operator_retry"
            or attempts[0]["status"] != "succeeded"
            or attempts[0]["completed_at"] is None
            or attempts[0]["details_json"] != row["details_json"]
        ):
            raise PaidDrainError("paid drain bridge run/attempt is not one-shot terminal")
        try:
            details = json.loads(str(row["details_json"]))
            event = details["event"]
        except (KeyError, TypeError, ValueError) as exc:
            raise PaidDrainError("paid drain bridge details are malformed") from exc
        if (
            not isinstance(details, dict)
            or details.get("contract_version") != CONTRACT_VERSION
            or details.get("complete") is not True
            or not isinstance(event, dict)
            or event.get("bridge_run_id") != row["id"]
            or event.get("bridge_attempt_id") != attempts[0]["id"]
            or event.get("event_type") not in EVENT_TYPES
            or event.get("sequence") != _EVENT_SEQUENCE[event["event_type"]]
            or event.get("event_hash") != _event_digest(event)
            or not isinstance(event.get("payload"), dict)
        ):
            raise PaidDrainError("paid drain bridge identity or hash is invalid")
        expected_scheduled_for = (
            f"paid-drain:{event['drain_id']}:{event['sequence']}:{event['event_type']}"
        )
        if (
            row["scheduled_for"] != expected_scheduled_for
            or row["started_at"] != event.get("created_at")
            or row["completed_at"] != event.get("created_at")
            or attempts[0]["started_at"] != event.get("created_at")
            or attempts[0]["completed_at"] != event.get("created_at")
            or _time(str(event.get("created_at"))) != event.get("created_at")
        ):
            raise PaidDrainError("paid drain bridge run/attempt timing or key changed")
        receipt = _details_to_receipt(details)
        _validate_drain_id(receipt.drain_id)
        expected_previous_id = None if previous is None else previous.run_id
        expected_previous_hash = None if previous is None else previous.event_hash
        if (
            receipt.previous_event_id != expected_previous_id
            or receipt.previous_event_hash != expected_previous_hash
        ):
            raise PaidDrainError("paid drain event chain is disconnected")
        if receipt.event_type == "start":
            if active_drain is not None:
                raise PaidDrainError("multiple active paid drains exist")
            _validate_drain_id(receipt.drain_id)
            _validate_binding(receipt.payload.get("binding", {}))
            active_drain = receipt.drain_id
        elif receipt.event_type == "sealed":
            if active_drain != receipt.drain_id:
                raise PaidDrainError("SEALED does not follow its active START")
            start = next(
                (
                    item
                    for item in reversed(receipts)
                    if item.drain_id == receipt.drain_id and item.event_type == "start"
                ),
                None,
            )
            if (
                start is None
                or receipt.payload.get("start_event_id") != start.run_id
                or receipt.payload.get("start_event_hash") != start.event_hash
            ):
                raise PaidDrainError("SEALED does not bind its START")
        else:
            if active_drain != receipt.drain_id or previous is None:
                raise PaidDrainError("RELEASE does not follow an active SEALED")
            if (
                previous.event_type != "sealed"
                or previous.drain_id != receipt.drain_id
                or receipt.payload.get("sealed_event_id") != previous.run_id
                or receipt.payload.get("sealed_event_hash") != previous.event_hash
            ):
                raise PaidDrainError("RELEASE does not bind its SEALED event")
            active_drain = None
        receipts.append(receipt)
        previous = receipt
    return receipts


def _uses_profile_drain(connection: sqlite3.Connection) -> bool:
    return int(connection.execute("PRAGMA user_version").fetchone()[0]) >= 19


def _validated_profile_chain(
    connection: sqlite3.Connection,
) -> list[_ProfileDrainEvent]:
    """Verify the schema-19 authority against its immutable bridge evidence."""

    if not _uses_profile_drain(connection):
        return []
    bridge = {receipt.run_id: receipt for receipt in _validated_chain(connection)}
    rows = connection.execute(
        "SELECT * FROM pipeline_paid_drain_events ORDER BY id"
    ).fetchall()
    bridged_rows = [row for row in rows if row["bridge_run_id"] is not None]
    if len(bridged_rows) != len(bridge):
        raise PaidDrainError("schema19 paid drain and bridge event counts differ")
    events: list[_ProfileDrainEvent] = []
    previous: _ProfileDrainEvent | None = None
    from .profile_activations import activation_by_id

    for row in rows:
        bridge_run_id = row["bridge_run_id"]
        receipt = bridge.get(int(bridge_run_id)) if bridge_run_id is not None else None
        try:
            payload = json.loads(str(row["payload_json"]))
        except (TypeError, ValueError) as exc:
            raise PaidDrainError("schema19 paid drain payload is malformed") from exc
        expected_previous_id = previous.event_id if previous is not None else None
        expected_previous_hash = previous.event_hash if previous is not None else None
        expected_contract = (
            CONTRACT_VERSION if bridge_run_id is not None else PROFILE_CONTRACT_VERSION
        )
        common_invalid = (
            row["contract_version"] != expected_contract
            or row["previous_event_id"] != expected_previous_id
            or row["previous_event_hash"] != expected_previous_hash
            or row["event_type"] not in _PROFILE_EVENT_SEQUENCE
            or (row["event_type"] == "ABORT_RESTORE" and (
                bridge_run_id is not None or connection.execute("PRAGMA user_version").fetchone()[0] != 20))
            or int(row["sequence"]) != _PROFILE_EVENT_SEQUENCE[str(row["event_type"])]
            or _time(str(row["created_at"])) != row["created_at"]
        )
        if receipt is not None:
            evidence_invalid = (
                int(row["bridge_attempt_id"] or 0) != receipt.attempt_id
                or row["drain_id"] != receipt.drain_id
                or int(row["sequence"]) != receipt.sequence
                or row["event_type"] != receipt.event_type
                or row["bridge_previous_event_id"] != receipt.previous_event_id
                or row["event_hash"] != receipt.event_hash
                or row["created_at"] != receipt.created_at
                or payload != receipt.payload
            )
        else:
            native_value = {
                "drain_id": row["drain_id"],
                "target_activation_id": int(row["target_activation_id"]),
                "sequence": int(row["sequence"]),
                "event_type": row["event_type"],
                "previous_event_id": row["previous_event_id"],
                "previous_event_hash": row["previous_event_hash"],
                "payload": payload,
                "contract_version": row["contract_version"],
                "created_at": row["created_at"],
            }
            evidence_invalid = (
                row["bridge_attempt_id"] is not None
                or row["bridge_previous_event_id"] is not None
                or row["event_hash"] != _digest(native_value)
            )
        if common_invalid or evidence_invalid:
            raise PaidDrainError("schema19 paid drain authority differs from bridge evidence")
        target_activation_id = int(row["target_activation_id"])
        try:
            activation_by_id(connection, target_activation_id)
        except Exception as exc:
            raise PaidDrainError("schema19 paid drain target activation is invalid") from exc
        event = _ProfileDrainEvent(
            event_id=int(row["id"]),
            target_activation_id=target_activation_id,
            drain_id=str(row["drain_id"]),
            event_type=str(row["event_type"]),  # type: ignore[arg-type]
            sequence=int(row["sequence"]),
            created_at=str(row["created_at"]),
            event_hash=str(row["event_hash"]),
            previous_event_id=row["previous_event_id"],
            previous_event_hash=row["previous_event_hash"],
            payload=payload,
            contract_version=str(row["contract_version"]),
            bridge_run_id=int(bridge_run_id) if bridge_run_id is not None else None,
            bridge_attempt_id=(
                int(row["bridge_attempt_id"])
                if row["bridge_attempt_id"] is not None
                else None
            ),
        )
        if event.event_type != "start":
            start = next(
                (
                    candidate
                    for candidate in reversed(events)
                    if candidate.drain_id == event.drain_id
                    and candidate.event_type == "start"
                ),
                None,
            )
            if start is None or start.target_activation_id != target_activation_id:
                raise PaidDrainError("schema19 paid drain target changed within its chain")
        if event.event_type == "ABORT_RESTORE":
            from .profile_control import validate_cross_profile_abort_terminal

            try:
                validate_cross_profile_abort_terminal(connection, event.as_dict())
            except Exception as exc:
                raise PaidDrainError("abort restore authority is invalid") from exc
        events.append(event)
        previous = event
    return events


def _profile_dispatch_state(
    connection: sqlite3.Connection, *, at: str
) -> DrainState:
    from .profile_activations import activation_at, activation_by_id

    timestamp = _time(at)
    events = _validated_profile_chain(connection)
    try:
        active = activation_at(connection, timestamp)
    except Exception as exc:
        raise PaidDrainError("active acquisition profile cannot be verified") from exc
    last = events[-1] if events else None
    if active is None:
        return DrainState(
            "invalid",
            last_event_id=last.event_id if last else None,
            last_event_hash=last.event_hash if last else None,
            reason="no acquisition profile is active",
        )

    if last is not None and connection.execute("PRAGMA user_version").fetchone()[0] == 20:
        from .profile_control import _abort_source_release, read_cross_profile_abort

        try:
            abort = read_cross_profile_abort(connection, last.drain_id)
            if abort is not None:
                permit_id = None
                if last.event_type == "ABORT_RESTORE" and last.payload.get("result") == "open":
                    try:
                        restored_permit = _abort_source_release(connection, abort["fence"]["payload"], at=timestamp)
                        if restored_permit == last.payload.get("source_release"):
                            permit_id = int(restored_permit["event_id"])
                    except Exception:
                        pass  # Expired source authority must never reopen itself.
                return DrainState("open" if permit_id else "closed", drain_id=last.drain_id,
                    last_event_id=last.event_id, last_event_hash=last.event_hash,
                    activation_id=int(active["activation_id"]), permit_event_id=permit_id,
                    reason=None if permit_id else "cross_profile_abort_fenced")
        except Exception as exc:
            raise PaidDrainError("abort fence cannot be verified") from exc

    # Any unfinished chain is an immediate DB-authoritative fence. A RELEASE
    # never grants another activation permission merely by being the latest
    # global event.
    if last is not None and last.event_type != "release":
        return DrainState(
            "draining" if last.event_type == "start" else "sealed",
            drain_id=last.drain_id,
            last_event_id=last.event_id,
            last_event_hash=last.event_hash,
            activation_id=int(active["activation_id"]),
        )

    cross_starts = [
        event
        for event in events
        if event.event_type == "start"
        and event.payload.get("switch_kind") == "cross_profile"
        and not any(terminal.drain_id == event.drain_id and terminal.event_type == "ABORT_RESTORE" for terminal in events)
    ]
    if cross_starts:
        switch = cross_starts[-1]
        target = activation_by_id(connection, switch.target_activation_id)
        cancellation = target.get("cancellation")
        if cancellation is not None and parse_time(cancellation["cancelled_at"]) <= parse_time(
            timestamp
        ):
            return DrainState(
                "invalid",
                drain_id=switch.drain_id,
                last_event_id=last.event_id if last else None,
                last_event_hash=last.event_hash if last else None,
                reason="cross-profile target activation was cancelled",
                activation_id=int(active["activation_id"]),
            )
        if active["profile_id"] != target["profile_id"]:
            return DrainState(
                "sealed" if last and last.event_type == "release" else "draining",
                drain_id=switch.drain_id,
                last_event_id=last.event_id if last else None,
                last_event_hash=last.event_hash if last else None,
                reason="cross-profile target is not effective",
                activation_id=int(active["activation_id"]),
            )

    metadata = active.get("metadata")
    current_hold_required = (
        isinstance(metadata, dict) and metadata.get("effective_mode") == "immediate"
    )

    def is_current_hold_release(event: _ProfileDrainEvent) -> bool:
        control = event.payload.get("control")
        if not isinstance(control, dict):
            return False
        start = next(
            (
                candidate
                for candidate in events
                if candidate.drain_id == event.drain_id
                and candidate.event_type == "start"
            ),
            None,
        )
        start_control = start.payload.get("control") if start is not None else None
        if not isinstance(start_control, dict):
            return False
        from .forward_recovery import forward_release_matches, is_forward_release

        if is_forward_release(control):
            assert start is not None
            return forward_release_matches(
                control, active=active, start=start.as_dict(), released_at=event.created_at,
            )
        not_before = start_control.get("not_before_business_day")
        try:
            if (
                not isinstance(not_before, str)
                or date.fromisoformat(not_before).isoformat() != not_before
            ):
                return False
        except ValueError:
            return False
        released_local = parse_time(event.created_at).astimezone(_BEIJING)
        seconds = (
            released_local.hour * 3600
            + released_local.minute * 60
            + released_local.second
        )
        return (
            control.get("contract_version") == CURRENT_ACTIVATION_HOLD_CONTRACT
            and control.get("control_purpose") == "full_day_release"
            and control.get("activation_id") == int(active["activation_id"])
            and control.get("roster_snapshot_id") == int(active["roster_snapshot_id"])
            and control.get("roster_snapshot_hash") == active["roster_members_sha256"]
            and control.get("release_business_day") == released_local.date().isoformat()
            and seconds < 5 * 60
            and start_control.get("contract_version")
            == CURRENT_ACTIVATION_HOLD_CONTRACT
            and start_control.get("control_purpose") == "hold_begin"
            and start_control.get("activation_id") == int(active["activation_id"])
            and not_before <= released_local.date().isoformat()
        )

    permit = next(
        (
            event
            for event in reversed(events)
            if event.target_activation_id == int(active["activation_id"])
            and event.event_type == "release"
            and parse_time(event.created_at) <= parse_time(timestamp)
            and (
                not current_hold_required
                or is_current_hold_release(event)
            )
        ),
        None,
    )
    if permit is None:
        return DrainState(
            "closed" if current_hold_required else "invalid",
            last_event_id=last.event_id if last else None,
            last_event_hash=last.event_hash if last else None,
            reason=(
                "current_activation_hold_missing"
                if current_hold_required
                else "active acquisition profile has no matching RELEASE permit"
            ),
            activation_id=int(active["activation_id"]),
        )
    return DrainState(
        "open",
        last_event_id=last.event_id if last else None,
        last_event_hash=last.event_hash if last else None,
        activation_id=int(active["activation_id"]),
        permit_event_id=permit.event_id,
    )


def _state_from_chain(receipts: Sequence[DrainReceipt]) -> DrainState:
    if not receipts or receipts[-1].event_type == "release":
        last = receipts[-1] if receipts else None
        return DrainState(
            "open",
            last_event_id=None if last is None else last.run_id,
            last_event_hash=None if last is None else last.event_hash,
        )
    last = receipts[-1]
    return DrainState(
        "draining" if last.event_type == "start" else "sealed",
        drain_id=last.drain_id,
        last_event_id=last.run_id,
        last_event_hash=last.event_hash,
    )


def dispatch_state(
    connection: sqlite3.Connection, *, at: str | None = None
) -> DrainState:
    """Read the authoritative state; corrupt or unknown chains fail closed."""

    try:
        if _uses_profile_drain(connection):
            return _profile_dispatch_state(connection, at=at or now_utc())
        del at  # A schema-18 drain never expires by clock or business-day rollover.
        return _state_from_chain(_validated_chain(connection))
    except PaidDrainError as exc:
        return DrainState("invalid", reason=str(exc))


def require_paid_dispatch_open(
    connection: sqlite3.Connection,
    *,
    provider: str,
    operation: str,
    at: str | None = None,
) -> DrainState:
    """Final paid gate for a caller-owned ``BEGIN IMMEDIATE`` transaction."""

    _require_transaction(connection)
    if not provider.strip() or not operation.strip():
        raise PaidDrainError("paid provider and operation are required")
    timestamp = _time(at or now_utc())
    state = dispatch_state(connection, at=timestamp)
    if not state.paid_dispatch_open:
        raise PaidDrainBlocked(
            f"paid dispatch is {state.state}: {state.reason or state.drain_id}",
            drain_id=state.drain_id,
            error_code=(
                "roster_not_ready"
                if state.reason == "no acquisition profile is active"
                else "current_activation_hold_missing"
                if state.reason == "current_activation_hold_missing"
                else None
            ),
        )
    if state.permit_event_id is not None and state.activation_id is not None:
        permit = connection.execute(
            "SELECT payload_json FROM pipeline_paid_drain_events "
            "WHERE id=? AND event_type='release'",
            (state.permit_event_id,),
        ).fetchone()
        try:
            payload = json.loads(str(permit["payload_json"])) if permit else None
        except (TypeError, ValueError):
            payload = None
        control = payload.get("control") if isinstance(payload, dict) else None
        from .forward_recovery import is_forward_release, validate_forward_release

        if isinstance(control, dict) and is_forward_release(control):
            from .profile_activations import activation_by_id
            from .profile_control import ProfileControlError

            try:
                if connection.execute("PRAGMA user_version").fetchone()[0] == 20:
                    from .capture_release import validate_current_dispatch_control
                    from .capture_authorizations import AuthorizationError
                    try:
                        validate_current_dispatch_control(
                            connection, active=activation_by_id(connection, state.activation_id),
                            release_control=control, operation=operation, at=timestamp,
                        )
                    except AuthorizationError as exc:
                        raise PaidDrainBlocked(str(exc), drain_id=state.drain_id, error_code=exc.error_code) from exc
                else:
                    validate_forward_release(
                        connection, active=activation_by_id(connection, state.activation_id),
                        release_control=control, at=timestamp,
                    )
            except ProfileControlError as exc:
                raise PaidDrainBlocked(str(exc), drain_id=state.drain_id, error_code=exc.code) from exc
        if (
            isinstance(control, dict)
            and control.get("contract_version")
            == CURRENT_ACTIVATION_HOLD_CONTRACT
            and control.get("control_purpose") == "full_day_release"
        ):
            from .profile_activations import activation_by_id
            from .profile_control import (
                ProfileControlError,
                validate_current_hold_release_prerequisites,
            )

            try:
                validate_current_hold_release_prerequisites(
                    connection,
                    active=activation_by_id(connection, state.activation_id),
                    release_control=control,
                    at=timestamp,
                )
            except ProfileControlError as exc:
                raise PaidDrainBlocked(
                    f"current activation release is no longer qualified: {exc}",
                    drain_id=state.drain_id,
                    error_code=exc.code,
                ) from exc
    return state


def require_paid_dispatch_open_path(
    *,
    db_path: Path = DEFAULT_DB,
    provider: str,
    operation: str,
    at: str | None = None,
) -> None:
    """Early no-side-effect gate; the in-transaction send gate remains final."""

    with transaction_metrics_context(job_id="paid_dispatch_preflight"), connect(
        db_path
    ) as connection, transaction(connection):
        require_paid_dispatch_open(
            connection, provider=provider, operation=operation, at=at
        )


def _existing_event(
    receipts: Sequence[DrainReceipt], drain_id: str, event_type: str
) -> DrainReceipt | None:
    matches = [
        item
        for item in receipts
        if item.drain_id == drain_id and item.event_type == event_type
    ]
    if len(matches) > 1:
        raise PaidDrainError("paid drain event was duplicated")
    return matches[0] if matches else None


def _insert_event(
    connection: sqlite3.Connection,
    *,
    drain_id: str,
    event_type: Literal["start", "sealed", "release"],
    payload: Mapping[str, Any],
    created_at: str,
    previous: DrainReceipt | None,
) -> DrainReceipt:
    _require_transaction(connection)
    sequence = _EVENT_SEQUENCE[event_type]
    scheduled_for = f"paid-drain:{drain_id}:{sequence}:{event_type}"
    seed: dict[str, Any] = {
        "contract_version": CONTRACT_VERSION,
        "complete": False,
        "event": {
            "drain_id": drain_id,
            "event_type": event_type,
            "sequence": sequence,
            "created_at": created_at,
            "payload": _clone(dict(payload)),
            "previous_event_id": None if previous is None else previous.run_id,
            "previous_event_hash": None if previous is None else previous.event_hash,
        },
    }
    cursor = connection.execute(
        "INSERT INTO scheduler_runs(job_id,scheduled_for,status,started_at,details_json) "
        "VALUES (?,?,'running',?,?)",
        (BRIDGE_JOB, scheduled_for, created_at, _json(seed)),
    )
    run_id = int(cursor.lastrowid or 0)
    attempt_cursor = connection.execute(
        "INSERT INTO scheduler_run_attempts(scheduler_run_id,attempt_number,"
        "invocation_source,status,started_at,details_json) "
        "VALUES (?,1,'operator_retry','running',?,?)",
        (run_id, created_at, _json(seed)),
    )
    attempt_id = int(attempt_cursor.lastrowid or 0)
    event = {
        **seed["event"],
        "bridge_run_id": run_id,
        "bridge_attempt_id": attempt_id,
    }
    event["event_hash"] = _event_digest(event)
    details = {"contract_version": CONTRACT_VERSION, "complete": True, "event": event}
    encoded = _json(details)
    attempt = connection.execute(
        "UPDATE scheduler_run_attempts SET status='succeeded',completed_at=?,details_json=? "
        "WHERE id=? AND status='running'",
        (created_at, encoded, attempt_id),
    )
    run = connection.execute(
        "UPDATE scheduler_runs SET status='succeeded',completed_at=?,details_json=? "
        "WHERE id=? AND status='running'",
        (created_at, encoded, run_id),
    )
    if attempt.rowcount != 1 or run.rowcount != 1:
        raise PaidDrainError("paid drain one-shot receipt could not finalize")
    return _details_to_receipt(details)


def _profile_target_for_binding(
    connection: sqlite3.Connection,
    binding: Mapping[str, Any],
    *,
    created_at: str,
    strict: bool,
) -> int:
    """Resolve the concrete schema-19 activation targeted by a drain.

    The profile controller always uses strict binding.  The non-strict branch
    exists only for the generic maintenance-drain API carried forward from
    schema 18: an old symbolic target is normalized to the activation that is
    actually active when the maintenance fence starts.
    """

    from .profile_activations import activation_at, activation_by_id

    candidate = binding.get("target_activation_id")
    if type(candidate) is int:
        try:
            activation_by_id(connection, candidate)
            return candidate
        except Exception:
            if strict:
                raise PaidDrainError("paid drain target activation does not exist")
    if strict:
        raise PaidDrainError("schema19 paid drain target must be an activation ID")
    active = activation_at(connection, created_at)
    if active is None:
        raise PaidDrainError("schema19 maintenance drain has no active activation")
    return int(active["activation_id"])


def _insert_profile_event(
    connection: sqlite3.Connection,
    *,
    target_activation_id: int,
    receipt: DrainReceipt,
) -> int:
    previous = connection.execute(
        "SELECT id,event_hash FROM pipeline_paid_drain_events ORDER BY id DESC LIMIT 1"
    ).fetchone()
    cursor = connection.execute(
        """INSERT INTO pipeline_paid_drain_events(
               drain_id,target_activation_id,sequence,event_type,previous_event_id,
               previous_event_hash,bridge_previous_event_id,bridge_run_id,
               bridge_attempt_id,payload_json,contract_version,event_hash,created_at)
           VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?)""",
        (
            receipt.drain_id,
            target_activation_id,
            receipt.sequence,
            receipt.event_type,
            int(previous["id"]) if previous is not None else None,
            str(previous["event_hash"]) if previous is not None else None,
            receipt.previous_event_id,
            receipt.run_id,
            receipt.attempt_id,
            _json(receipt.payload),
            CONTRACT_VERSION,
            receipt.event_hash,
            receipt.created_at,
        ),
    )
    return int(cursor.lastrowid or 0)


def _append_native_profile_event(
    connection: sqlite3.Connection,
    *,
    drain_id: str,
    target_activation_id: int,
    event_type: Literal["start", "sealed", "release", "ABORT_RESTORE"],
    payload: Mapping[str, Any],
    created_at: str,
) -> _ProfileDrainEvent:
    _require_transaction(connection)
    events = _validated_profile_chain(connection)
    previous = events[-1] if events else None
    sequence = _PROFILE_EVENT_SEQUENCE[event_type]
    value = {
        "drain_id": drain_id,
        "target_activation_id": target_activation_id,
        "sequence": sequence,
        "event_type": event_type,
        "previous_event_id": previous.event_id if previous else None,
        "previous_event_hash": previous.event_hash if previous else None,
        "payload": _clone(dict(payload)),
        "contract_version": PROFILE_CONTRACT_VERSION,
        "created_at": created_at,
    }
    event_hash = _digest(value)
    cursor = connection.execute(
        """INSERT INTO pipeline_paid_drain_events(
               drain_id,target_activation_id,sequence,event_type,previous_event_id,
               previous_event_hash,payload_json,contract_version,event_hash,created_at)
           VALUES (?,?,?,?,?,?,?,?,?,?)""",
        (
            drain_id,
            target_activation_id,
            sequence,
            event_type,
            value["previous_event_id"],
            value["previous_event_hash"],
            _json(value["payload"]),
            PROFILE_CONTRACT_VERSION,
            event_hash,
            created_at,
        ),
    )
    return _ProfileDrainEvent(
        event_id=int(cursor.lastrowid or 0),
        target_activation_id=target_activation_id,
        drain_id=drain_id,
        event_type=event_type,
        sequence=sequence,
        created_at=created_at,
        event_hash=event_hash,
        previous_event_id=value["previous_event_id"],
        previous_event_hash=value["previous_event_hash"],
        payload=value["payload"],
        contract_version=PROFILE_CONTRACT_VERSION,
        bridge_run_id=None,
        bridge_attempt_id=None,
    )


def start_profile_drain_in_transaction(
    connection: sqlite3.Connection,
    drain_id: str,
    *,
    binding: Mapping[str, Any],
    switch_kind: Literal["cross_profile", "same_profile"],
    now: str,
    nonblocking: bool | None = None,
    control: Mapping[str, Any] | None = None,
) -> _ProfileDrainEvent:
    """Append a native schema-19 START without consuming scheduler rows."""

    _require_transaction(connection)
    if not _uses_profile_drain(connection):
        raise PaidDrainError("profile drains require schema 19")
    drain_id = _validate_drain_id(drain_id)
    frozen_binding = _validate_binding(binding)
    frozen_control = _clone(dict(control)) if control is not None else None
    expected_nonblocking = (
        switch_kind == "same_profile" if nonblocking is None else bool(nonblocking)
    )
    created_at = _time(now)
    events = _validated_profile_chain(connection)
    matches = [
        event
        for event in events
        if event.drain_id == drain_id and event.event_type == "start"
    ]
    if matches:
        if (
            len(matches) != 1
            or matches[0].payload.get("binding") != frozen_binding
            or matches[0].payload.get("switch_kind") != switch_kind
            or matches[0].payload.get("nonblocking") is not expected_nonblocking
            or matches[0].payload.get("control") != frozen_control
        ):
            raise PaidDrainError("idempotent START changed its frozen binding")
        return matches[0]
    if events and events[-1].event_type not in {"release", "ABORT_RESTORE"}:
        raise PaidDrainError("another paid drain is already active")
    if events and connection.execute("PRAGMA user_version").fetchone()[0] == 20:
        from .profile_control import read_cross_profile_abort

        if read_cross_profile_abort(connection, events[-1].drain_id) is not None and events[-1].event_type != "ABORT_RESTORE":
            raise PaidDrainError("abort restore must be terminal before a new START")
    target_activation_id = _profile_target_for_binding(
        connection, frozen_binding, created_at=created_at, strict=True
    )
    payload: dict[str, Any] = {
        "binding": frozen_binding,
        "frozen_dispatch": _freeze_start(connection),
        "switch_kind": switch_kind,
        "nonblocking": expected_nonblocking,
    }
    if frozen_control is not None:
        payload["control"] = frozen_control
        payload["control_contract_version"] = frozen_control.get(
            "contract_version"
        )
        payload.update(
            {
                key: value
                for key, value in frozen_control.items()
                if key != "contract_version"
            }
        )
    return _append_native_profile_event(
        connection,
        drain_id=drain_id,
        target_activation_id=target_activation_id,
        event_type="start",
        payload=payload,
        created_at=created_at,
    )


def seal_profile_drain_in_transaction(
    connection: sqlite3.Connection,
    drain_id: str,
    *,
    now: str,
    force_strict: bool = False,
    control: Mapping[str, Any] | None = None,
) -> _ProfileDrainEvent:
    _require_transaction(connection)
    created_at = _time(now)
    events = _validated_profile_chain(connection)
    existing = next(
        (
            event
            for event in events
            if event.drain_id == drain_id and event.event_type == "sealed"
        ),
        None,
    )
    if existing is not None:
        if control is not None and existing.payload.get("control") != _clone(dict(control)):
            raise PaidDrainError("idempotent SEALED changed its frozen control binding")
        return existing
    if not events or events[-1].drain_id != drain_id or events[-1].event_type != "start":
        raise PaidDrainError("SEALED requires this drain's active START")
    start = events[-1]
    if start.payload.get("nonblocking") is True and not force_strict:
        if start.payload.get("switch_kind") != "same_profile":
            raise PaidDrainError("only same-profile permits may be nonblocking")
        from .profile_activations import activation_by_id

        binding = start.payload.get("binding", {})
        source_id = binding.get("source_activation_id")
        if type(source_id) is not int:
            raise PaidDrainError("same-profile permit source activation is invalid")
        source = activation_by_id(connection, source_id)
        target = activation_by_id(connection, start.target_activation_id)
        if source["profile_id"] != target["profile_id"]:
            raise PaidDrainError("nonblocking permit changed acquisition profile")
        verification = {
            "checked_at": created_at,
            "nonblocking": True,
            "source_activation_id": source_id,
            "target_activation_id": start.target_activation_id,
        }
    else:
        # Reuse the schema-18 exact-tail verifier. It only consumes the
        # immutable START payload and does not depend on scheduler bridge IDs.
        synthetic = DrainReceipt(
            run_id=start.event_id,
            attempt_id=start.event_id,
            drain_id=start.drain_id,
            event_type="start",
            sequence=1,
            created_at=start.created_at,
            event_hash=start.event_hash,
            previous_event_id=start.previous_event_id,
            previous_event_hash=start.previous_event_hash,
            payload=start.payload,
        )
        verification = _verify_sealable(connection, synthetic, checked_at=created_at)
    payload: dict[str, Any] = {
        "start_event_id": start.event_id,
        "start_event_hash": start.event_hash,
        "verification": verification,
    }
    if control is not None:
        frozen_control = _clone(dict(control))
        payload["control"] = frozen_control
        payload["control_contract_version"] = frozen_control.get(
            "contract_version"
        )
        payload.update(
            {
                key: value
                for key, value in frozen_control.items()
                if key != "contract_version"
            }
        )
    return _append_native_profile_event(
        connection,
        drain_id=drain_id,
        target_activation_id=start.target_activation_id,
        event_type="sealed",
        payload=payload,
        created_at=created_at,
    )


def verify_profile_drain_sealable(
    connection: sqlite3.Connection,
    drain_id: str,
    *,
    now: str,
) -> dict[str, Any]:
    """Verify a native schema-19 drain tail without appending SEALED."""

    _require_transaction(connection)
    created_at = _time(now)
    events = _validated_profile_chain(connection)
    if not events or events[-1].drain_id != drain_id or events[-1].event_type != "start":
        raise PaidDrainError("sealable verification requires this drain's active START")
    start = events[-1]
    synthetic = DrainReceipt(
        run_id=start.event_id,
        attempt_id=start.event_id,
        drain_id=start.drain_id,
        event_type="start",
        sequence=1,
        created_at=start.created_at,
        event_hash=start.event_hash,
        previous_event_id=start.previous_event_id,
        previous_event_hash=start.previous_event_hash,
        payload=start.payload,
    )
    return _verify_sealable(connection, synthetic, checked_at=created_at)


def release_profile_drain_in_transaction(
    connection: sqlite3.Connection,
    drain_id: str,
    *,
    now: str,
    control: Mapping[str, Any] | None = None,
) -> _ProfileDrainEvent:
    _require_transaction(connection)
    created_at = _time(now)
    events = _validated_profile_chain(connection)
    existing = next(
        (
            event
            for event in events
            if event.drain_id == drain_id and event.event_type == "release"
        ),
        None,
    )
    if existing is not None:
        if control is not None and existing.payload.get("control") != _clone(dict(control)):
            raise PaidDrainError("idempotent RELEASE changed its frozen control binding")
        return existing
    if not events or events[-1].drain_id != drain_id or events[-1].event_type != "sealed":
        raise PaidDrainError("RELEASE requires this drain's valid SEALED event")
    sealed = events[-1]
    from .profile_activations import activation_by_id

    if activation_by_id(connection, sealed.target_activation_id).get("cancellation"):
        raise PaidDrainError("cancelled target activation cannot receive RELEASE")
    payload: dict[str, Any] = {
        "sealed_event_id": sealed.event_id,
        "sealed_event_hash": sealed.event_hash,
    }
    if control is not None:
        frozen_control = _clone(dict(control))
        payload["control"] = frozen_control
        payload["control_contract_version"] = frozen_control.get(
            "contract_version"
        )
        payload.update(
            {
                key: value
                for key, value in frozen_control.items()
                if key != "contract_version"
            }
        )
    return _append_native_profile_event(
        connection,
        drain_id=drain_id,
        target_activation_id=sealed.target_activation_id,
        event_type="release",
        payload=payload,
        created_at=created_at,
    )


def start_paid_drain_in_transaction(
    connection: sqlite3.Connection,
    drain_id: str,
    *,
    binding: Mapping[str, Any],
    switch_kind: Literal["cross_profile", "same_profile", "maintenance"] = "maintenance",
    now: str | None = None,
    strict_target: bool = True,
) -> DrainReceipt:
    """Append START under the caller's transaction.

    Schema 19 writes both the historical one-shot bridge and the new
    activation-bound authority in the same transaction.
    """

    _require_transaction(connection)
    drain_id = _validate_drain_id(drain_id)
    frozen_binding = _validate_binding(binding)
    created_at = _time(now)
    receipts = _validated_chain(connection)
    existing = _existing_event(receipts, drain_id, "start")
    if existing is not None:
        if existing.payload.get("binding") != frozen_binding:
            raise PaidDrainError("idempotent START changed its frozen binding")
        return existing
    if _state_from_chain(receipts).state != "open":
        raise PaidDrainError("another paid drain is already active")
    target_activation_id: int | None = None
    if _uses_profile_drain(connection):
        _validated_profile_chain(connection)
        target_activation_id = _profile_target_for_binding(
            connection,
            frozen_binding,
            created_at=created_at,
            strict=strict_target,
        )
    payload: dict[str, Any] = {
        "binding": frozen_binding,
        "frozen_dispatch": _freeze_start(connection),
    }
    if _uses_profile_drain(connection):
        payload["switch_kind"] = switch_kind
    receipt = _insert_event(
        connection,
        drain_id=drain_id,
        event_type="start",
        payload=payload,
        created_at=created_at,
        previous=receipts[-1] if receipts else None,
    )
    if target_activation_id is not None:
        _insert_profile_event(
            connection,
            target_activation_id=target_activation_id,
            receipt=receipt,
        )
    return receipt


def start_paid_drain(
    drain_id: str,
    *,
    binding: Mapping[str, Any],
    db_path: Path = DEFAULT_DB,
    now: str | None = None,
) -> DrainReceipt:
    """Atomically close dispatch and freeze only the current dispatch tail."""

    with transaction_metrics_context(job_id=BRIDGE_JOB), connect(
        db_path
    ) as connection, transaction(connection):
        return start_paid_drain_in_transaction(
            connection,
            drain_id,
            binding=binding,
            switch_kind="maintenance",
            now=now,
            strict_target=False,
        )


def _usage_state(connection: sqlite3.Connection, usage_id: int) -> str | None:
    row = connection.execute(
        "SELECT details_json FROM provider_usage WHERE id=?", (usage_id,)
    ).fetchone()
    if row is None:
        return None
    try:
        details = json.loads(str(row["details_json"] or "{}"))
    except (TypeError, ValueError):
        return None
    return details.get("state") if isinstance(details, dict) else None


def _verify_sealable(
    connection: sqlite3.Connection, start: DrainReceipt, *, checked_at: str
) -> dict[str, Any]:
    frozen = start.payload.get("frozen_dispatch")
    control = start.payload.get("control")
    current_activation_hold = bool(
        isinstance(control, dict)
        and control.get("contract_version") == CURRENT_ACTIVATION_HOLD_CONTRACT
        and control.get("control_purpose") == "hold_begin"
    )
    if not isinstance(frozen, dict):
        raise PaidDrainError("START frozen dispatch payload is missing")
    for field in (
        "provider_usage_high_watermark",
        "fetch_attempt_high_watermark",
        "scheduler_run_high_watermark",
    ):
        if type(frozen.get(field)) is not int or frozen[field] < 0:
            raise PaidDrainError("START high-watermark payload is invalid")
    for name in (
        "tikhub_reserved",
        "tikhub_send_marked",
        "paid_running_attempts",
        "matrix_running_dispatches",
    ):
        value = frozen.get(name)
        if not isinstance(value, list) or frozen.get(name + "_sha256") != _digest(value):
            raise PaidDrainError(f"START {name} set hash is invalid")

    unresolved_usage: list[int] = []
    for name in ("tikhub_reserved", "tikhub_send_marked"):
        for entry in frozen[name]:
            if not isinstance(entry, dict) or not _strict_int(entry.get("usage_id")):
                raise PaidDrainError(f"START {name} identity is invalid")
            unresolved_states = {"reserved", "sent", None}
            if current_activation_hold:
                unresolved_states.update({"billing_unknown", "charged_unverified"})
            if _usage_state(connection, int(entry["usage_id"])) in unresolved_states:
                unresolved_usage.append(int(entry["usage_id"]))

    unresolved_runs: list[int] = []
    matrix_request_deltas: dict[str, int] = {}
    for entry in frozen["paid_running_attempts"]:
        if (
            not isinstance(entry, dict)
            or not _strict_int(entry.get("run_id"))
            or not _strict_int(entry.get("attempt_id"))
        ):
            raise PaidDrainError("START paid attempt identity is invalid")
        row = connection.execute(
            "SELECT r.status,r.details_json,a.status attempt_status "
            "FROM scheduler_runs r JOIN scheduler_run_attempts a ON a.scheduler_run_id=r.id "
            "WHERE r.id=? AND a.id=?",
            (entry["run_id"], entry["attempt_id"]),
        ).fetchone()
        if row is None or row["status"] == "running" or row["attempt_status"] == "running":
            unresolved_runs.append(int(entry["run_id"]))
            continue
        if "matrix_network_requests" in entry:
            delta = _network_requests(_run_details(row)) - int(entry["matrix_network_requests"])
            matrix_request_deltas[str(entry["run_id"])] = delta
            allowed_deltas = {0} if current_activation_hold else {0, 1}
            if delta not in allowed_deltas:
                raise PaidDrainError(
                    "Matrix dispatch advanced past the current-hold network fence"
                    if current_activation_hold
                    else "Matrix dispatch advanced more than its START in-flight request"
                )

    provider_tail = [
        int(row["id"])
        for row in connection.execute(
            "SELECT id FROM provider_usage WHERE lower(provider)='tikhub' AND id>? ORDER BY id",
            (frozen["provider_usage_high_watermark"],),
        )
    ]
    fetch_tail = [
        int(row["id"])
        for row in connection.execute(
            "SELECT a.id FROM fetch_attempts a JOIN fetch_slots s ON s.id=a.slot_id "
            "WHERE lower(s.provider)='tikhub' AND a.id>? ORDER BY a.id",
            (frozen["fetch_attempt_high_watermark"],),
        )
    ]
    scheduler_tail: list[int] = []
    for row in connection.execute(
        "SELECT id,job_id,status,details_json FROM scheduler_runs "
        "WHERE id>? AND job_id<>? ORDER BY id",
        (frozen["scheduler_run_high_watermark"], BRIDGE_JOB),
    ):
        if (
            current_activation_hold
            and str(row["job_id"]) in _MATRIX_JOBS
            and row["status"] != "running"
            and _network_requests(_run_details(row)) == 0
        ):
            continue
        if _paid_capable_run(row):
            scheduler_tail.append(int(row["id"]))
    if unresolved_usage or unresolved_runs:
        raise PaidDrainError(
            "paid drain has unresolved frozen dispatches: "
            f"usage={unresolved_usage}, runs={unresolved_runs}"
        )
    diagnostic_tail: dict[str, Any] | None = None
    if current_activation_hold:
        from .transport_tail import verify_current_hold_diagnostic_tail

        try:
            diagnostic_tail = verify_current_hold_diagnostic_tail(connection, start, at=checked_at)
        except (RuntimeError, ValueError, TypeError, KeyError, IndexError, OSError) as error:
            raise PaidDrainError(f"current HOLD diagnostic tail is not sealable: {error}") from error
    elif provider_tail or fetch_tail or scheduler_tail:
        raise PaidDrainError(
            "paid drain detected post-START dispatch tail: "
            f"usage={provider_tail}, fetch={fetch_tail}, runs={scheduler_tail}"
        )
    return {
        "checked_at": checked_at,
        "frozen_usage_terminal": True,
        "frozen_runs_terminal": True,
        "post_start_provider_usage_ids": provider_tail,
        "post_start_fetch_attempt_ids": fetch_tail,
        "post_start_paid_run_ids": scheduler_tail,
        "matrix_network_request_deltas": matrix_request_deltas,
        **({"diagnostic_tail": diagnostic_tail} if diagnostic_tail is not None else {}),
    }


def seal_paid_drain_in_transaction(
    connection: sqlite3.Connection,
    drain_id: str,
    *,
    now: str | None = None,
) -> DrainReceipt:
    """Append SEALED under the caller's transaction."""

    _require_transaction(connection)
    drain_id = _validate_drain_id(drain_id)
    created_at = _time(now)
    receipts = _validated_chain(connection)
    existing = _existing_event(receipts, drain_id, "sealed")
    if existing is not None:
        return existing
    state = _state_from_chain(receipts)
    if state.state != "draining" or state.drain_id != drain_id:
        raise PaidDrainError("SEALED requires this drain's active START")
    start = receipts[-1]
    target_activation_id: int | None = None
    if _uses_profile_drain(connection):
        events = _validated_profile_chain(connection)
        if not events or events[-1].bridge_run_id != start.run_id:
            raise PaidDrainError("schema19 START authority is missing")
        target_activation_id = events[-1].target_activation_id
    verification = _verify_sealable(connection, start, checked_at=created_at)
    receipt = _insert_event(
        connection,
        drain_id=drain_id,
        event_type="sealed",
        payload={
            "start_event_id": start.run_id,
            "start_event_hash": start.event_hash,
            "verification": verification,
        },
        created_at=created_at,
        previous=receipts[-1],
    )
    if target_activation_id is not None:
        _insert_profile_event(
            connection,
            target_activation_id=target_activation_id,
            receipt=receipt,
        )
    return receipt


def seal_paid_drain(
    drain_id: str,
    *,
    db_path: Path = DEFAULT_DB,
    now: str | None = None,
) -> DrainReceipt:
    """Prove the frozen dispatches terminal and append a SEALED receipt."""

    with transaction_metrics_context(job_id=BRIDGE_JOB), connect(
        db_path
    ) as connection, transaction(connection):
        return seal_paid_drain_in_transaction(connection, drain_id, now=now)


def release_paid_drain_in_transaction(
    connection: sqlite3.Connection,
    drain_id: str,
    *,
    now: str | None = None,
    mirror_root: Path | None = None,
) -> DrainReceipt:
    """Append RELEASE under the caller's transaction."""

    _require_transaction(connection)
    drain_id = _validate_drain_id(drain_id)
    created_at = _time(now)
    receipts = _validated_chain(connection)
    existing = _existing_event(receipts, drain_id, "release")
    if existing is not None:
        return existing
    state = _state_from_chain(receipts)
    if state.state != "sealed" or state.drain_id != drain_id:
        raise PaidDrainError("RELEASE requires this drain's valid SEALED event")
    sealed = receipts[-1]
    target_activation_id: int | None = None
    if _uses_profile_drain(connection):
        events = _validated_profile_chain(connection)
        if not events or events[-1].bridge_run_id != sealed.run_id:
            raise PaidDrainError("schema19 SEALED authority is missing")
        target_activation_id = events[-1].target_activation_id
        from .profile_activations import activation_by_id

        target = activation_by_id(connection, target_activation_id)
        if target.get("cancellation") is not None:
            raise PaidDrainError("cancelled target activation cannot receive RELEASE")
    receipt = _insert_event(
        connection,
        drain_id=drain_id,
        event_type="release",
        payload={
            "sealed_event_id": sealed.run_id,
            "sealed_event_hash": sealed.event_hash,
        },
        created_at=created_at,
        previous=sealed,
    )
    if target_activation_id is not None:
        _insert_profile_event(
            connection,
            target_activation_id=target_activation_id,
            receipt=receipt,
        )
    if mirror_root is not None:
        write_audit_mirror(receipt, mirror_root)
    return receipt


def release_paid_drain(
    drain_id: str,
    *,
    db_path: Path = DEFAULT_DB,
    now: str | None = None,
    mirror_root: Path | None = None,
) -> DrainReceipt:
    """Append the only DB event that can reopen paid dispatch.

    A formal release requires its audit mirror. The mirror is fsynced while
    the DB transaction is still uncommitted, so no formal DB release can be
    observed before the corresponding immutable file exists.
    """

    if is_formal_database_path(db_path) and mirror_root is None:
        raise PaidDrainError("formal RELEASE requires an audit mirror root")
    with transaction_metrics_context(job_id=BRIDGE_JOB), connect(
        db_path
    ) as connection, transaction(connection):
        return release_paid_drain_in_transaction(
            connection,
            drain_id,
            now=now,
            mirror_root=mirror_root,
        )


def issue_activation_permit_in_transaction(
    connection: sqlite3.Connection,
    *,
    activation_id: int,
    drain_id: str,
    source_activation_id: int,
    business_day: str,
    planned_effective_at: str,
    build_receipt_sha256: str,
    runtime_root_receipt_sha256: str,
    now: str,
) -> _ProfileDrainEvent:
    """Create a complete, target-specific nonblocking permit chain."""

    if not _uses_profile_drain(connection):
        raise PaidDrainError("activation permits require schema 19")
    binding = {
        "source_activation_id": source_activation_id,
        "target_activation_id": activation_id,
        "business_day": business_day,
        "planned_effective_at": planned_effective_at,
        "build_receipt_sha256": build_receipt_sha256,
        "runtime_root_receipt_sha256": runtime_root_receipt_sha256,
    }
    start_profile_drain_in_transaction(
        connection,
        drain_id,
        binding=binding,
        switch_kind="same_profile",
        now=now,
    )
    seal_profile_drain_in_transaction(connection, drain_id, now=now)
    return release_profile_drain_in_transaction(connection, drain_id, now=now)


def write_audit_mirror(receipt: DrainReceipt, root: Path) -> Path:
    """Write one immutable 0600 audit copy; it has no dispatch authority."""

    root = Path(root).expanduser()
    if not root.is_absolute() or root.is_symlink():
        raise PaidDrainError("paid drain mirror root must be absolute and non-symlink")
    root.mkdir(parents=True, exist_ok=True, mode=0o700)
    root_state = root.stat()
    if (
        not stat.S_ISDIR(root_state.st_mode)
        or root_state.st_uid != os.geteuid()
        or stat.S_IMODE(root_state.st_mode) & 0o077
    ):
        raise PaidDrainError("paid drain mirror root must be private and user-owned")
    target = root / f"{receipt.drain_id}.{receipt.event_type}.json"
    body = (_json(receipt.as_dict()) + "\n").encode("utf-8")
    flags = os.O_WRONLY | os.O_CREAT | os.O_EXCL
    if hasattr(os, "O_CLOEXEC"):
        flags |= os.O_CLOEXEC
    if hasattr(os, "O_NOFOLLOW"):
        flags |= os.O_NOFOLLOW
    descriptor = os.open(target, flags, 0o600)
    try:
        with os.fdopen(descriptor, "wb", closefd=True) as stream:
            stream.write(body)
            stream.flush()
            os.fsync(stream.fileno())
        target_state = target.stat()
        if (
            not stat.S_ISREG(target_state.st_mode)
            or target_state.st_uid != os.geteuid()
            or stat.S_IMODE(target_state.st_mode) & 0o077
            or target_state.st_nlink != 1
        ):
            raise PaidDrainError(
                "paid drain mirror must be private, regular, and single-link"
            )
        directory = os.open(root, os.O_RDONLY)
        try:
            os.fsync(directory)
        finally:
            os.close(directory)
    except Exception:
        # The descriptor is owned by fdopen after it succeeds.  If fdopen
        # itself failed, close the still-open descriptor before propagating.
        try:
            os.close(descriptor)
        except OSError:
            pass
        try:
            target.unlink()
        except OSError:
            pass
        raise
    return target
