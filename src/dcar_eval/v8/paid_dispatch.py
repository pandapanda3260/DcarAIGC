"""Append-only request-level evidence for every paid provider dispatch."""

from __future__ import annotations

import hashlib
import json
import re
import sqlite3
import uuid
from dataclasses import dataclass
from typing import Any, Literal, Mapping

from .paid_drain import dispatch_state


CONTRACT_VERSION = "paid-provider-dispatch-v1"
TERMINAL_EVENTS = frozenset({"succeeded", "failed", "billing_unknown", "not_sent"})
_DAY = re.compile(r"\d{4}-\d{2}-\d{2}")


class PaidDispatchError(RuntimeError):
    pass


def _canonical(value: Any) -> str:
    return json.dumps(
        value,
        ensure_ascii=False,
        sort_keys=True,
        separators=(",", ":"),
        allow_nan=False,
    )


def supports_dispatch_ledger(connection: sqlite3.Connection) -> bool:
    return (
        connection.execute(
            "SELECT 1 FROM sqlite_master WHERE type='table' "
            "AND name='paid_provider_dispatch_events'"
        ).fetchone()
        is not None
    )


@dataclass(frozen=True)
class DispatchEvent:
    event_id: int
    dispatch_id: str
    sequence: int
    event_type: Literal[
        "reserved",
        "send_marked",
        "succeeded",
        "failed",
        "billing_unknown",
        "not_sent",
    ]
    provider: str
    operation: str
    activation_id: int
    business_day: str
    permit_event_id: int
    scheduler_run_id: int
    scheduler_attempt_id: int
    scope: dict[str, Any]
    provider_usage_id: int | None
    fetch_slot_id: int | None
    fetch_attempt_id: int | None
    raw_response_id: int | None
    cursor_identity: dict[str, Any]
    previous_event_id: int | None
    previous_event_hash: str | None
    contract_version: str
    event_hash: str
    created_at: str


def _digest(value: Mapping[str, Any]) -> str:
    keys = (
        "dispatch_id",
        "sequence",
        "event_type",
        "provider",
        "operation",
        "activation_id",
        "business_day",
        "permit_event_id",
        "scheduler_run_id",
        "scheduler_attempt_id",
        "scope",
        "provider_usage_id",
        "fetch_slot_id",
        "fetch_attempt_id",
        "raw_response_id",
        "cursor_identity",
        "previous_event_id",
        "previous_event_hash",
        "contract_version",
        "created_at",
    )
    return hashlib.sha256(
        _canonical({key: value.get(key) for key in keys}).encode("utf-8")
    ).hexdigest()


def _event(row: sqlite3.Row | Mapping[str, Any]) -> DispatchEvent:
    value = dict(row)
    scope = json.loads(value.pop("scope_json"))
    cursor_identity = json.loads(value.pop("cursor_identity_json"))
    normalized = {
        **value,
        "scope": scope,
        "cursor_identity": cursor_identity,
    }
    normalized["event_id"] = int(normalized.pop("id"))
    event = DispatchEvent(**normalized)
    if event.contract_version != CONTRACT_VERSION or event.event_hash != _digest(
        event.__dict__
    ):
        raise PaidDispatchError("paid dispatch event digest is invalid")
    return event


def dispatch_events(
    connection: sqlite3.Connection, dispatch_id: str
) -> list[DispatchEvent]:
    events = [
        _event(row)
        for row in connection.execute(
            "SELECT * FROM paid_provider_dispatch_events WHERE dispatch_id=? ORDER BY sequence",
            (dispatch_id,),
        )
    ]
    if not events:
        raise PaidDispatchError("paid dispatch does not exist")
    expected = ["reserved"]
    if len(events) >= 2:
        expected.append(events[1].event_type)
    if len(events) == 3:
        expected.append(events[2].event_type)
    if (
        len(events) > 3
        or [event.sequence for event in events] != list(range(1, len(events) + 1))
        or expected[0] != "reserved"
        or (len(events) >= 2 and events[1].event_type not in {"send_marked", "not_sent"})
        or (
            len(events) == 3
            and (
                events[1].event_type != "send_marked"
                or events[2].event_type
                not in {"succeeded", "failed", "billing_unknown"}
            )
        )
    ):
        raise PaidDispatchError("paid dispatch sequence is invalid")
    for previous, current in zip(events, events[1:]):
        if (
            current.previous_event_id != previous.event_id
            or current.previous_event_hash != previous.event_hash
            or current.provider != previous.provider
            or current.operation != previous.operation
            or current.activation_id != previous.activation_id
            or current.business_day != previous.business_day
            or current.permit_event_id != previous.permit_event_id
            or current.scheduler_run_id != previous.scheduler_run_id
            or current.scheduler_attempt_id != previous.scheduler_attempt_id
            or current.scope != previous.scope
        ):
            raise PaidDispatchError("paid dispatch chain binding changed")
    return events


def _require_transaction(connection: sqlite3.Connection) -> None:
    if not connection.in_transaction:
        raise PaidDispatchError("paid dispatch writes require an active transaction")


def _insert(connection: sqlite3.Connection, value: Mapping[str, Any]) -> DispatchEvent:
    payload = dict(value)
    payload["contract_version"] = CONTRACT_VERSION
    payload["event_hash"] = _digest(payload)
    cursor = connection.execute(
        """INSERT INTO paid_provider_dispatch_events(
               dispatch_id,sequence,event_type,provider,operation,activation_id,
               business_day,permit_event_id,scheduler_run_id,scheduler_attempt_id,
               scope_json,provider_usage_id,fetch_slot_id,fetch_attempt_id,
               raw_response_id,cursor_identity_json,previous_event_id,
               previous_event_hash,contract_version,event_hash,created_at)
           VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)""",
        (
            payload["dispatch_id"],
            payload["sequence"],
            payload["event_type"],
            payload["provider"],
            payload["operation"],
            payload["activation_id"],
            payload["business_day"],
            payload["permit_event_id"],
            payload["scheduler_run_id"],
            payload["scheduler_attempt_id"],
            _canonical(payload["scope"]),
            payload.get("provider_usage_id"),
            payload.get("fetch_slot_id"),
            payload.get("fetch_attempt_id"),
            payload.get("raw_response_id"),
            _canonical(payload.get("cursor_identity") or {}),
            payload.get("previous_event_id"),
            payload.get("previous_event_hash"),
            payload["contract_version"],
            payload["event_hash"],
            payload["created_at"],
        ),
    )
    row = connection.execute(
        "SELECT * FROM paid_provider_dispatch_events WHERE id=?", (cursor.lastrowid,)
    ).fetchone()
    if row is None:
        raise PaidDispatchError("paid dispatch event was not retained")
    return _event(row)


def reserve_dispatch_in_transaction(
    connection: sqlite3.Connection,
    *,
    provider: str,
    operation: str,
    activation_id: int,
    business_day: str,
    scheduler_run_id: int,
    scheduler_attempt_id: int,
    scope: Mapping[str, Any],
    created_at: str,
    provider_usage_id: int | None = None,
    fetch_slot_id: int | None = None,
    cursor_identity: Mapping[str, Any] | None = None,
    diagnostic_authority: Mapping[str, Any] | None = None,
) -> DispatchEvent | None:
    """Bind authorization and ownership before waiting for a network slot."""

    _require_transaction(connection)
    if not supports_dispatch_ledger(connection):
        return None
    if not provider.strip() or not operation.strip() or not _DAY.fullmatch(business_day):
        raise PaidDispatchError("paid dispatch binding is invalid")
    state = dispatch_state(connection, at=created_at)
    if diagnostic_authority is None:
        if (
            not state.paid_dispatch_open
            or state.activation_id != activation_id
            or state.permit_event_id is None
            or "diagnostic_member" in scope
        ):
            raise PaidDispatchError("active acquisition permit does not authorize dispatch")
        permit_event_id = state.permit_event_id
    else:
        from .transport_authority import authorize_diagnostic_request, diagnostic_dispatch_binding

        if set(diagnostic_authority) != {
            "request_binding", "scope", "request_identity", "request_transport", "stage",
        }:
            raise PaidDispatchError("diagnostic dispatch authority is malformed")
        member = authorize_diagnostic_request(
            connection, binding=diagnostic_authority["request_binding"],
            scope=diagnostic_authority["scope"],
            request_identity=diagnostic_authority["request_identity"],
            request_transport=diagnostic_authority["request_transport"],
            stage=diagnostic_authority["stage"], at=created_at,
        )
        hold = member["payload"]["hold_binding"]
        natural_scope = member["payload"]["natural_due"]["scope_identity"]
        if (
            provider.lower() != "tikhub" or operation != member["payload"]["operation"]
            or activation_id != hold["activation_id"] or state.state != "draining"
            or state.drain_id != hold["drain_id"]
            or scope.get("diagnostic_member") != diagnostic_dispatch_binding(member)
            or scheduler_run_id != natural_scope["scheduler_run_id"]
            or scheduler_attempt_id != natural_scope["scheduler_attempt_id"]
            or business_day != natural_scope["business_day"]
            or dict(cursor_identity or {}) != {
                "paid_scope_identity": diagnostic_authority["request_identity"].scope_identity,
                "paid_execution_identity": diagnostic_authority["request_identity"].execution_identity,
                "sequence": 0,
                "request": diagnostic_authority["request_identity"].document,
            }
        ):
            raise PaidDispatchError("diagnostic dispatch changed its durable member binding")
        # This schema-19 FK retains real historical RELEASE lineage. It does
        # not authorize the request: the exact current START/member above does.
        permit_event_id = int(hold["dispatch_legacy_release_anchor"]["event_id"])
    if provider_usage_id is not None:
        usage = connection.execute(
            "SELECT provider,operation FROM provider_usage WHERE id=?",
            (provider_usage_id,),
        ).fetchone()
        if (
            usage is None
            or str(usage["provider"]).lower() != provider.lower()
            or usage["operation"] != operation
        ):
            raise PaidDispatchError("provider usage does not match dispatch")
    return _insert(
        connection,
        {
            "dispatch_id": uuid.uuid4().hex,
            "sequence": 1,
            "event_type": "reserved",
            "provider": provider,
            "operation": operation,
            "activation_id": activation_id,
            "business_day": business_day,
            "permit_event_id": permit_event_id,
            "scheduler_run_id": scheduler_run_id,
            "scheduler_attempt_id": scheduler_attempt_id,
            "scope": dict(scope),
            "provider_usage_id": provider_usage_id,
            "fetch_slot_id": fetch_slot_id,
            "fetch_attempt_id": None,
            "raw_response_id": None,
            "cursor_identity": dict(cursor_identity or {}),
            "previous_event_id": None,
            "previous_event_hash": None,
            "created_at": created_at,
        },
    )


def _append(
    connection: sqlite3.Connection,
    dispatch_id: str,
    event_type: str,
    *,
    created_at: str,
    fetch_attempt_id: int | None = None,
    raw_response_id: int | None = None,
) -> DispatchEvent | None:
    _require_transaction(connection)
    if not supports_dispatch_ledger(connection):
        return None
    events = dispatch_events(connection, dispatch_id)
    previous = events[-1]
    if previous.event_type in TERMINAL_EVENTS:
        if previous.event_type == event_type:
            return previous
        raise PaidDispatchError("paid dispatch is already terminal")
    if previous.event_type == "reserved" and event_type not in {
        "send_marked",
        "not_sent",
    }:
        raise PaidDispatchError("paid dispatch must cross or close the send boundary")
    if previous.event_type == "send_marked" and event_type not in {
        "succeeded",
        "failed",
        "billing_unknown",
    }:
        raise PaidDispatchError("paid dispatch terminal event is invalid")
    return _insert(
        connection,
        {
            **previous.__dict__,
            "sequence": previous.sequence + 1,
            "event_type": event_type,
            "fetch_attempt_id": fetch_attempt_id
            if fetch_attempt_id is not None
            else previous.fetch_attempt_id,
            "raw_response_id": raw_response_id
            if raw_response_id is not None
            else previous.raw_response_id,
            "cursor_identity": previous.cursor_identity,
            "previous_event_id": previous.event_id,
            "previous_event_hash": previous.event_hash,
            "created_at": created_at,
        },
    )


def mark_dispatch_sent_in_transaction(
    connection: sqlite3.Connection,
    dispatch_id: str,
    *,
    fetch_attempt_id: int | None,
    created_at: str,
) -> DispatchEvent | None:
    return _append(
        connection,
        dispatch_id,
        "send_marked",
        fetch_attempt_id=fetch_attempt_id,
        created_at=created_at,
    )


def close_dispatch_not_sent_in_transaction(
    connection: sqlite3.Connection,
    dispatch_id: str,
    *,
    reason: str,
    created_at: str,
) -> DispatchEvent | None:
    # The event type is the durable close reason.  Cursor identity is immutable
    # dispatch ownership evidence and must not be repurposed for diagnostics.
    return _append(
        connection,
        dispatch_id,
        "not_sent",
        created_at=created_at,
    )


def finish_dispatch_in_transaction(
    connection: sqlite3.Connection,
    dispatch_id: str,
    *,
    outcome: Literal["succeeded", "failed", "billing_unknown"],
    created_at: str,
    raw_response_id: int | None = None,
    reason: str | None = None,
) -> DispatchEvent | None:
    # Keep the public diagnostic argument for callers, but never mix it into
    # cursor identity.  A dedicated diagnostic field can be added in a future
    # schema contract if durable free-form reasons become necessary.
    return _append(
        connection,
        dispatch_id,
        outcome,
        raw_response_id=raw_response_id,
        created_at=created_at,
    )


def dispatch_id_for_usage(
    connection: sqlite3.Connection, provider_usage_id: int
) -> str | None:
    if not supports_dispatch_ledger(connection):
        return None
    row = connection.execute(
        """SELECT dispatch_id FROM paid_provider_dispatch_events
           WHERE provider_usage_id=? ORDER BY sequence DESC LIMIT 1""",
        (provider_usage_id,),
    ).fetchone()
    return str(row["dispatch_id"]) if row is not None else None
