"""One requested account/content per schema20 physical request.

The legacy slot remains the scheduler owner, not a second attempt owner. Returned
discovery works are response data and never become pre-send batch members.
"""
from __future__ import annotations

import sqlite3
from typing import Any

from . import capture_planning as planning, usage_settlements
from .paid_identity import PaidRequestIdentity
from .provider_budget import PaidScope, PaidScopeBlocked


def attempt_slot_sql(connection: sqlite3.Connection, alias: str = "fa") -> str:
    """Only one actual send marker may carry a batch attempt's legacy slot."""
    if alias not in {"fa", "a"}:
        raise ValueError("unsupported attempt alias")
    if connection.execute("PRAGMA user_version").fetchone()[0] != 20:
        return f"{alias}.slot_id"
    return (f"COALESCE({alias}.slot_id,(SELECT CASE WHEN count(*)=1 THEN min(d.fetch_slot_id) END "
            f"FROM paid_provider_dispatch_events d WHERE d.fetch_attempt_id={alias}.id "
            "AND d.event_type='send_marked'))")


def freeze(connection: sqlite3.Connection, *, request: PaidRequestIdentity,
           scope: PaidScope, at: str) -> tuple[int, int]:
    if not connection.in_transaction or connection.execute("PRAGMA user_version").fetchone()[0] != 20:
        raise ValueError("singleton freeze requires schema20 writer transaction")
    route = planning.require_send_route(connection, scope=scope,
                                        operation=request.document["operation"], at=at)
    if route is None:
        raise PaidScopeBlocked("route_not_active", "Singleton requires an actual route assignment")
    member = usage_settlements.member_identity(request.document)
    parameters = planning.canonical(request.document["request_parameters"])
    connection.execute("""INSERT OR IGNORE INTO fetch_request_batches(request_scope_identity,sequence,
        provider,operation,parameters_json,created_at) VALUES(?,?,'tikhub',?,?,?)""",
        (request.scope_identity, request.sequence, request.document["operation"], parameters, at))
    batch = connection.execute("SELECT * FROM fetch_request_batches WHERE request_scope_identity=? AND sequence=?",
                               (request.scope_identity, request.sequence)).fetchone()
    assert batch is not None
    connection.execute("""INSERT OR IGNORE INTO fetch_request_batch_members(batch_id,member_scope_identity,
        sequence,content_id,account_id) VALUES(?,?,?,?,?)""",
        (batch["id"], member, request.sequence, scope.content_id, scope.account_id))
    validate(connection, batch_id=batch["id"], request=request, assignment_id=route["id"], scope=scope, at=at)
    return int(batch["id"]), int(route["id"])


def validate(connection: sqlite3.Connection, *, batch_id: int, request: PaidRequestIdentity,
             assignment_id: int, scope: PaidScope, at: str) -> list[dict[str, Any]]:
    batch = connection.execute("SELECT * FROM fetch_request_batches WHERE id=?", (batch_id,)).fetchone()
    members = connection.execute("SELECT * FROM fetch_request_batch_members WHERE batch_id=?", (batch_id,)).fetchall()
    if (batch is None or batch["work_id"] is not None or batch["provider"] != "tikhub"
            or batch["request_scope_identity"] != request.scope_identity or batch["sequence"] != request.sequence
            or batch["operation"] != request.document["operation"]
            or batch["parameters_json"] != planning.canonical(request.document["request_parameters"])
            or len(members) != 1):
        raise PaidScopeBlocked("batch_identity_invalid", "Singleton request or frozen membership changed")
    member = members[0]
    member_hash = usage_settlements.member_identity(request.document)
    if (member["member_scope_identity"] != member_hash or member["sequence"] != request.sequence
            or member["content_id"] != scope.content_id or member["account_id"] != scope.account_id):
        raise PaidScopeBlocked("batch_identity_invalid", "Singleton member target or paid identity changed")
    with planning.execution_route_context(assignment_id):
        route = planning.require_send_route(connection, scope=scope, operation=batch["operation"], at=at)
    if route is None or route["id"] != assignment_id:
        raise PaidScopeBlocked("route_generation_conflict", "Singleton route changed before send")
    return [{"member_id": member["id"], "assignment_id": assignment_id, "member_identity": member_hash}]


def validate_raw_target(connection: sqlite3.Connection, *, batch_id: int,
                        content_id: int | None, account_id: int | None) -> None:
    rows = connection.execute("""SELECT m.content_id,m.account_id FROM fetch_request_batch_members m
        JOIN fetch_request_batches b ON b.id=m.batch_id WHERE m.batch_id=? AND b.work_id IS NULL""", (batch_id,)).fetchall()
    # Content slots retain content only in raw; account membership remains in m.
    if (len(rows) != 1 or rows[0]["content_id"] != content_id
            or (content_id is None and rows[0]["account_id"] != account_id)
            or (content_id is not None and account_id is not None)):
        raise PaidScopeBlocked("batch_identity_invalid", "Singleton raw target differs from requested member")


def record_disposition(connection: sqlite3.Connection, *, batch_id: int, attempt_id: int,
                       raw_response_id: int | None, disposition: str, reason: str, at: str) -> None:
    execution = connection.execute("SELECT 1 FROM fetch_request_executions WHERE batch_id=? AND fetch_attempt_id=?",
                                   (batch_id, attempt_id)).fetchone()
    members = connection.execute("SELECT * FROM fetch_request_batch_members WHERE batch_id=?", (batch_id,)).fetchall()
    if execution is None or len(members) != 1:
        raise PaidScopeBlocked("batch_identity_invalid", "Singleton result lacks its exact send execution")
    if raw_response_id is not None and not connection.execute(
            "SELECT 1 FROM provider_raw_responses WHERE id=? AND fetch_attempt_id=?",
            (raw_response_id, attempt_id)).fetchone():
        raise PaidScopeBlocked("batch_identity_invalid", "Singleton disposition raw belongs to another request")
    evidence = planning.canonical({"contract_version": "capture-singleton-v1", "batch_id": batch_id,
        "fetch_attempt_id": attempt_id, "raw_response_id": raw_response_id, "reason": reason,
        "disposition": disposition, "requested_content_id": members[0]["content_id"],
        "requested_account_id": members[0]["account_id"]})
    previous = connection.execute("SELECT * FROM fetch_request_member_dispositions WHERE member_id=?", (members[0]["id"],)).fetchone()
    if previous is not None and previous["evidence_json"] != evidence:
        raise PaidScopeBlocked("batch_identity_invalid", "Singleton disposition is immutable")
    connection.execute("""INSERT OR IGNORE INTO fetch_request_member_dispositions(member_id,disposition,
        raw_response_id,evidence_json,recorded_at) VALUES(?,?,?,?,?)""",
        (members[0]["id"], disposition, raw_response_id, evidence, at))
