"""Schema20 deterministic capture planning; planning never calls a provider.

Assignments fence execution independently of billing identity.  A plan may be
replayed against a snapshot; only an explicitly active assignment is runnable.
"""
from __future__ import annotations

import hashlib
import json
import sqlite3
from contextlib import contextmanager
from contextvars import ContextVar
from datetime import datetime, timedelta, timezone
from typing import Any, Iterator, Mapping
from zoneinfo import ZoneInfo

BEIJING = ZoneInfo("Asia/Shanghai")
CONTRACT = "capture-planning-v1"
_EXECUTION_ASSIGNMENT: ContextVar[int | None] = ContextVar("capture_route_assignment", default=None)
WORK_STATES = (
    "runnable", "provider_blocked", "paid_identity_hold", "budget_deferred",
    "leased", "running", "terminal",
)

SCHEMA_SQL = """
CREATE TABLE routing_input_changes (
 id INTEGER PRIMARY KEY AUTOINCREMENT,
 change_kind TEXT NOT NULL CHECK(change_kind IN ('roster','policy','authorization')),
 roster_snapshot_id INTEGER REFERENCES account_roster_snapshots(id) ON DELETE RESTRICT,
 previous_id INTEGER REFERENCES routing_input_changes(id) ON DELETE RESTRICT,
 payload_json TEXT NOT NULL CHECK(json_valid(payload_json)),
 effective_at TEXT NOT NULL, recorded_at TEXT NOT NULL,
 change_sha256 TEXT NOT NULL UNIQUE CHECK(length(change_sha256)=64)
);
CREATE TABLE capture_source_plans (
 id INTEGER PRIMARY KEY AUTOINCREMENT,
 roster_change_id INTEGER NOT NULL REFERENCES routing_input_changes(id) ON DELETE RESTRICT,
 business_day TEXT NOT NULL, generation INTEGER NOT NULL CHECK(generation>0),
 mode TEXT NOT NULL CHECK(mode IN ('shadow','active')),
 payload_json TEXT NOT NULL CHECK(json_valid(payload_json)),
 created_at TEXT NOT NULL, plan_sha256 TEXT NOT NULL UNIQUE CHECK(length(plan_sha256)=64),
 UNIQUE(roster_change_id,business_day,generation)
);
CREATE TABLE capture_route_assignments (
 id INTEGER PRIMARY KEY AUTOINCREMENT,
 scope_type TEXT NOT NULL CHECK(scope_type IN ('platform_operation','account','content')),
 scope_key TEXT NOT NULL, provider TEXT NOT NULL, operation TEXT NOT NULL,
 account_id INTEGER REFERENCES accounts(id) ON DELETE RESTRICT,
 content_id INTEGER REFERENCES content_items(id) ON DELETE RESTRICT,
 generation INTEGER NOT NULL CHECK(generation>0),
 previous_assignment_id INTEGER REFERENCES capture_route_assignments(id) ON DELETE RESTRICT,
 source_plan_id INTEGER REFERENCES capture_source_plans(id) ON DELETE RESTRICT,
 route TEXT NOT NULL CHECK(route IN ('legacy','integrated','historical_only')),
 mode TEXT NOT NULL CHECK(mode IN ('active','shadow','disabled')),
 effective_at TEXT NOT NULL, recorded_at TEXT NOT NULL,
 assignment_sha256 TEXT NOT NULL UNIQUE CHECK(length(assignment_sha256)=64),
 UNIQUE(scope_type,scope_key,operation,generation),
 CHECK(provider!='matrix' OR (mode='disabled' AND route='historical_only'))
);
CREATE INDEX idx_capture_route_current ON capture_route_assignments
 (scope_type,scope_key,operation,effective_at DESC,generation DESC);
CREATE TABLE capture_fallback_blueprints (
 id INTEGER PRIMARY KEY AUTOINCREMENT,
 roster_change_id INTEGER NOT NULL REFERENCES routing_input_changes(id) ON DELETE RESTRICT,
 blueprint_sha256 TEXT NOT NULL UNIQUE, created_at TEXT NOT NULL
);
CREATE TABLE capture_fallback_blueprint_members (
 blueprint_id INTEGER NOT NULL REFERENCES capture_fallback_blueprints(id) ON DELETE RESTRICT,
 assignment_id INTEGER NOT NULL REFERENCES capture_route_assignments(id) ON DELETE RESTRICT,
 payload_json TEXT NOT NULL CHECK(json_valid(payload_json)),
 PRIMARY KEY(blueprint_id,assignment_id)
);
CREATE TABLE provider_authorization_states (
 id INTEGER PRIMARY KEY AUTOINCREMENT, provider TEXT NOT NULL, authorization_key TEXT NOT NULL,
 generation INTEGER NOT NULL CHECK(generation>0), status TEXT NOT NULL,
 scope_json TEXT NOT NULL CHECK(json_valid(scope_json)), expires_at TEXT,
 recorded_at TEXT NOT NULL, evidence_sha256 TEXT NOT NULL,
 UNIQUE(provider,authorization_key,generation)
);
CREATE TABLE capture_work_items (
 id INTEGER PRIMARY KEY AUTOINCREMENT,
 work_identity TEXT NOT NULL UNIQUE CHECK(length(work_identity)=64),
 assignment_id INTEGER NOT NULL REFERENCES capture_route_assignments(id) ON DELETE RESTRICT,
 source_plan_id INTEGER REFERENCES capture_source_plans(id) ON DELETE RESTRICT,
 account_id INTEGER REFERENCES accounts(id) ON DELETE RESTRICT,
 content_id INTEGER REFERENCES content_items(id) ON DELETE RESTRICT,
 provider TEXT NOT NULL, operation TEXT NOT NULL,
 due_at TEXT NOT NULL, data_business_day TEXT NOT NULL,
 state TEXT NOT NULL CHECK(state IN ('runnable','provider_blocked','paid_identity_hold',
 'budget_deferred','leased','running','terminal')),
 reason TEXT NOT NULL DEFAULT '', envelope_json TEXT NOT NULL CHECK(json_valid(envelope_json)),
 owner_token TEXT, heartbeat_at TEXT, lease_expires_at TEXT,
 attempt_count INTEGER NOT NULL DEFAULT 0 CHECK(attempt_count>=0),
 created_at TEXT NOT NULL, updated_at TEXT NOT NULL, completed_at TEXT,
 CHECK((state IN ('leased','running'))=(owner_token IS NOT NULL)),
 CHECK((state='terminal')=(completed_at IS NOT NULL))
);
CREATE INDEX idx_capture_work_due ON capture_work_items(state,due_at,id);
CREATE TABLE capture_watermarks (
 id INTEGER PRIMARY KEY AUTOINCREMENT, provider TEXT NOT NULL, operation TEXT NOT NULL,
 scope_key TEXT NOT NULL, work_id INTEGER NOT NULL UNIQUE REFERENCES capture_work_items(id) ON DELETE RESTRICT,
 complete_through TEXT NOT NULL, cursor_json TEXT NOT NULL CHECK(json_valid(cursor_json)),
 evidence_json TEXT NOT NULL CHECK(json_valid(evidence_json)), recorded_at TEXT NOT NULL
);
CREATE INDEX idx_capture_watermark_scope ON capture_watermarks
 (provider,operation,scope_key,complete_through DESC,id DESC);
CREATE TABLE fetch_dead_letters (
 id INTEGER PRIMARY KEY AUTOINCREMENT, work_id INTEGER NOT NULL UNIQUE REFERENCES capture_work_items(id) ON DELETE RESTRICT,
 reason TEXT NOT NULL, envelope_json TEXT NOT NULL CHECK(json_valid(envelope_json)),
 attempts INTEGER NOT NULL CHECK(attempts>=1), created_at TEXT NOT NULL
);
CREATE TABLE fetch_request_batches (
 id INTEGER PRIMARY KEY AUTOINCREMENT, work_id INTEGER REFERENCES capture_work_items(id) ON DELETE RESTRICT,
 request_scope_identity TEXT NOT NULL, sequence INTEGER NOT NULL DEFAULT 0 CHECK(sequence BETWEEN 0 AND 4),
 provider TEXT NOT NULL, operation TEXT NOT NULL, parameters_json TEXT NOT NULL CHECK(json_valid(parameters_json)),
 created_at TEXT NOT NULL, UNIQUE(request_scope_identity,sequence)
);
CREATE TABLE fetch_request_batch_members (
 id INTEGER PRIMARY KEY AUTOINCREMENT, batch_id INTEGER NOT NULL REFERENCES fetch_request_batches(id) ON DELETE RESTRICT,
 member_scope_identity TEXT NOT NULL, sequence INTEGER NOT NULL CHECK(sequence BETWEEN 0 AND 4),
 content_id INTEGER REFERENCES content_items(id) ON DELETE RESTRICT,
 account_id INTEGER REFERENCES accounts(id) ON DELETE RESTRICT,
 UNIQUE(member_scope_identity,sequence), UNIQUE(batch_id,member_scope_identity)
);
CREATE TABLE fetch_request_member_dispositions (
 id INTEGER PRIMARY KEY AUTOINCREMENT, member_id INTEGER NOT NULL REFERENCES fetch_request_batch_members(id) ON DELETE RESTRICT,
 disposition TEXT NOT NULL CHECK(disposition IN ('valid','missing','invalid','unavailable','unusable')),
 raw_response_id INTEGER REFERENCES provider_raw_responses(id) ON DELETE RESTRICT,
 evidence_json TEXT NOT NULL CHECK(json_valid(evidence_json)), recorded_at TEXT NOT NULL,
 UNIQUE(member_id)
);
CREATE TABLE fetch_request_executions (
 id INTEGER PRIMARY KEY AUTOINCREMENT, batch_id INTEGER NOT NULL REFERENCES fetch_request_batches(id) ON DELETE RESTRICT,
 fetch_attempt_id INTEGER NOT NULL UNIQUE REFERENCES fetch_attempts(id) ON DELETE RESTRICT,
 assignment_id INTEGER NOT NULL REFERENCES capture_route_assignments(id) ON DELETE RESTRICT,
 execution_identity TEXT NOT NULL UNIQUE, started_at TEXT NOT NULL
);
CREATE TABLE admission_reservations (
 id INTEGER PRIMARY KEY AUTOINCREMENT, batch_id INTEGER NOT NULL UNIQUE REFERENCES fetch_request_batches(id) ON DELETE RESTRICT,
 state TEXT NOT NULL CHECK(state IN ('reserved_unsent','released_unsent','sent_unsettled','settled')),
 amount_microusd INTEGER NOT NULL CHECK(amount_microusd>=0), charge_business_day TEXT NOT NULL,
 created_at TEXT NOT NULL, expires_at TEXT NOT NULL, updated_at TEXT NOT NULL
);
CREATE TABLE provider_readiness_receipts (
 id INTEGER PRIMARY KEY AUTOINCREMENT, provider TEXT NOT NULL, operation TEXT NOT NULL,
 status TEXT NOT NULL CHECK(status IN ('ready','blocked','diagnostic_only')),
 reason TEXT NOT NULL, evidence_json TEXT NOT NULL CHECK(json_valid(evidence_json)),
 created_at TEXT NOT NULL, expires_at TEXT NOT NULL, receipt_sha256 TEXT NOT NULL UNIQUE
);
CREATE TABLE send_boundary_eligibility_receipts (
 id INTEGER PRIMARY KEY AUTOINCREMENT, batch_id INTEGER NOT NULL REFERENCES fetch_request_batches(id) ON DELETE RESTRICT,
 assignment_id INTEGER NOT NULL REFERENCES capture_route_assignments(id) ON DELETE RESTRICT,
 readiness_receipt_id INTEGER NOT NULL REFERENCES provider_readiness_receipts(id) ON DELETE RESTRICT,
 reservation_id INTEGER NOT NULL REFERENCES admission_reservations(id) ON DELETE RESTRICT,
 eligible INTEGER NOT NULL CHECK(eligible IN (0,1)), reason TEXT NOT NULL,
 evidence_json TEXT NOT NULL CHECK(json_valid(evidence_json)), checked_at TEXT NOT NULL, receipt_sha256 TEXT NOT NULL UNIQUE
);
CREATE TABLE send_boundary_member_eligibility_receipts (
 eligibility_receipt_id INTEGER NOT NULL REFERENCES send_boundary_eligibility_receipts(id) ON DELETE RESTRICT,
 member_id INTEGER NOT NULL REFERENCES fetch_request_batch_members(id) ON DELETE RESTRICT,
 eligible INTEGER NOT NULL CHECK(eligible IN (0,1)), reason TEXT NOT NULL,
 PRIMARY KEY(eligibility_receipt_id,member_id)
);
CREATE TABLE provider_circuit_states (
 id INTEGER PRIMARY KEY AUTOINCREMENT, fault_domain TEXT NOT NULL
 CHECK(fault_domain IN ('provider','operation','storage','authorization','paid_scope')),
 scope_key TEXT NOT NULL, generation INTEGER NOT NULL CHECK(generation>0),
 state TEXT NOT NULL CHECK(state IN ('open','half_open','closed')),
 evidence_json TEXT NOT NULL CHECK(json_valid(evidence_json)), created_at TEXT NOT NULL,
 fingerprint TEXT NOT NULL, UNIQUE(fault_domain,scope_key,generation)
);
CREATE TABLE provider_probe_authorizations (
 id INTEGER PRIMARY KEY AUTOINCREMENT, circuit_id INTEGER NOT NULL REFERENCES provider_circuit_states(id) ON DELETE RESTRICT,
 actor TEXT NOT NULL, expires_at TEXT NOT NULL, max_starts INTEGER NOT NULL CHECK(max_starts=1),
 evidence_json TEXT NOT NULL CHECK(json_valid(evidence_json)), created_at TEXT NOT NULL, UNIQUE(circuit_id)
);
CREATE TABLE provider_probe_authorization_issuances (
 id INTEGER PRIMARY KEY AUTOINCREMENT, authorization_id INTEGER NOT NULL UNIQUE REFERENCES provider_probe_authorizations(id) ON DELETE RESTRICT,
 request_scope_identity TEXT NOT NULL, issued_at TEXT NOT NULL
);
CREATE TABLE operational_alerts (
 id INTEGER PRIMARY KEY AUTOINCREMENT, dedupe_key TEXT NOT NULL, severity TEXT NOT NULL CHECK(severity IN ('P0','P1','P2')),
 scope_json TEXT NOT NULL CHECK(json_valid(scope_json)), evidence_json TEXT NOT NULL CHECK(json_valid(evidence_json)),
 owner TEXT NOT NULL, status TEXT NOT NULL CHECK(status IN ('open','resolved')),
 opened_at TEXT NOT NULL, resolved_at TEXT,
 CHECK((status='resolved')=(resolved_at IS NOT NULL))
);
CREATE UNIQUE INDEX uq_operational_alert_open ON operational_alerts(dedupe_key) WHERE status='open';
CREATE TABLE data_quality_receipts (
 id INTEGER PRIMARY KEY AUTOINCREMENT, scope_key TEXT NOT NULL, cutoff_at TEXT NOT NULL,
 payload_json TEXT NOT NULL CHECK(json_valid(payload_json)), recorded_at TEXT NOT NULL, receipt_sha256 TEXT NOT NULL UNIQUE
);
CREATE TABLE deployment_readiness_receipts (
 id INTEGER PRIMARY KEY AUTOINCREMENT, deployment_id TEXT NOT NULL UNIQUE,
 status TEXT NOT NULL CHECK(status IN ('candidate','accepted','failed')),
 payload_json TEXT NOT NULL CHECK(json_valid(payload_json)), recorded_at TEXT NOT NULL, receipt_sha256 TEXT NOT NULL UNIQUE
);
CREATE UNIQUE INDEX uq_schema20_acceptance ON deployment_readiness_receipts(status) WHERE status='accepted';
CREATE TABLE capture_paid_send_gate_events (
 id INTEGER PRIMARY KEY AUTOINCREMENT, provider TEXT NOT NULL, operation TEXT NOT NULL,
 state TEXT NOT NULL CHECK(state IN ('closed','diagnostic_only','open')),
 reason TEXT NOT NULL, evidence_json TEXT NOT NULL CHECK(json_valid(evidence_json)),
 recorded_at TEXT NOT NULL, event_sha256 TEXT NOT NULL UNIQUE
);
CREATE TABLE capture_completion_events (
 id INTEGER PRIMARY KEY AUTOINCREMENT, state TEXT NOT NULL CHECK(state IN ('completed','invalidated')),
 payload_json TEXT NOT NULL CHECK(json_valid(payload_json)), recorded_at TEXT NOT NULL, event_sha256 TEXT NOT NULL UNIQUE
);
"""


def canonical(value: Any) -> str:
    return json.dumps(value, ensure_ascii=False, sort_keys=True, separators=(",", ":"), allow_nan=False)


def digest(value: Any) -> str:
    return hashlib.sha256(canonical(value).encode()).hexdigest()


def timestamp(value: str) -> str:
    parsed = datetime.fromisoformat(value.replace("Z", "+00:00"))
    if parsed.tzinfo is None:
        raise ValueError("capture timestamp must have timezone")
    return parsed.astimezone(timezone.utc).isoformat(timespec="microseconds").replace("+00:00", "Z")


def current_assignment(connection: sqlite3.Connection, scope_type: str, scope_key: str,
                       operation: str, *, at: str) -> dict[str, Any] | None:
    row = connection.execute(
        "SELECT * FROM capture_route_assignments WHERE scope_type=? AND scope_key=? "
        "AND operation=? AND effective_at<=? ORDER BY effective_at DESC,generation DESC LIMIT 1",
        (scope_type, scope_key, operation, timestamp(at)),
    ).fetchone()
    return dict(row) if row else None


@contextmanager
def execution_route_context(assignment_id: int) -> Iterator[None]:
    token = _EXECUTION_ASSIGNMENT.set(assignment_id)
    try:
        yield
    finally:
        _EXECUTION_ASSIGNMENT.reset(token)


def resolve_route(connection: sqlite3.Connection, *, account_id: int | None,
                  content_id: int | None, operation: str, at: str) -> dict[str, Any] | None:
    if content_id is not None:
        found = current_assignment(connection, "content", str(content_id), operation, at=at)
        if found is not None:
            return found
    if account_id is not None:
        return current_assignment(connection, "account", str(account_id), operation, at=at)
    return None


def require_send_route(connection: sqlite3.Connection, *, scope: Any, operation: str, at: str) -> dict[str, Any] | None:
    """Both admission and the final send transaction re-read the same fence."""
    if connection.execute("PRAGMA user_version").fetchone()[0] != 20:
        return None
    from .provider_budget import PaidScopeBlocked

    assignment = resolve_route(connection, account_id=scope.account_id, content_id=scope.content_id,
                               operation=operation, at=at)
    if assignment is None or assignment["mode"] != "active" or assignment["provider"] != "tikhub":
        raise PaidScopeBlocked("route_not_active", "Scope has no active TikHub route assignment")
    expected = _EXECUTION_ASSIGNMENT.get()
    if (assignment["route"] == "integrated" and expected != assignment["id"]
            or expected is not None and expected != assignment["id"]):
        raise PaidScopeBlocked("route_generation_conflict", "Scope assignment changed before send")
    gate = connection.execute(
        "SELECT state FROM capture_paid_send_gate_events WHERE provider='tikhub' AND operation=? "
        "AND julianday(recorded_at)<=julianday(?) ORDER BY id DESC LIMIT 1", (operation, timestamp(at)),
    ).fetchone()
    if gate is None or gate[0] not in {"open", "diagnostic_only"}:
        raise PaidScopeBlocked("provider_transport_blocked", "Schema20 operation has not passed its send gate")
    return assignment


def legacy_queue_allowed(connection: sqlite3.Connection, *, account_id: int, content_id: int | None,
                         operations: list[str], at: str) -> bool:
    if connection.execute("PRAGMA user_version").fetchone()[0] != 20:
        return True
    for operation in operations:
        assignment = resolve_route(connection, account_id=account_id, content_id=content_id, operation=operation, at=at)
        if assignment is None or assignment["mode"] != "active" or assignment["route"] != "legacy":
            return False
    return True


def seed_legacy_routes(connection: sqlite3.Connection, *, at: str) -> dict[str, Any]:
    """Freeze actual accepted roster and rollback blueprint; activate nothing."""
    from .profile_activations import activation_at
    from .provider_budget import PRICES_MICROUSD

    if not connection.in_transaction:
        raise ValueError("route seeding requires migration transaction")
    active = activation_at(connection, at)
    if active is None:
        return {"status": "no_activation", "assignments": 0}
    if active["profile_id"] != "tikhub_managed_v1":
        return {"status": "unsupported_source_activation", "assignments": 0}
    frozen = {"activation_id": active["activation_id"], "activation_sha256": active["activation_sha256"],
              "roster_snapshot_id": active["roster_snapshot_id"], "roster_members_sha256": active["roster_members_sha256"]}
    stamp = timestamp(at)
    change = connection.execute(
        "INSERT INTO routing_input_changes(change_kind,roster_snapshot_id,payload_json,effective_at,recorded_at,change_sha256) "
        "VALUES ('roster',?,?,?,?,?)", (active["roster_snapshot_id"], canonical(frozen), stamp, stamp, digest(frozen)),
    )
    change_id = int(change.lastrowid or 0)
    members = connection.execute(
        "SELECT i.account_id,i.platform FROM account_roster_members m JOIN account_platform_identities i "
        "ON i.id=m.account_identity_id JOIN accounts a ON a.id=i.account_id "
        "WHERE m.snapshot_id=? ORDER BY i.id", (active["roster_snapshot_id"],),
    ).fetchall()
    assignment_ids = []
    for member in members:
        for operation in sorted(PRICES_MICROUSD):
            if operation.startswith(member["platform"] + "_"):
                assignment_ids.append(assign_route(
                    connection, scope_type="account", scope_key=str(member["account_id"]),
                    provider="tikhub", operation=operation, account_id=member["account_id"],
                    expected_generation=0, route="legacy", mode="active", effective_at=stamp, recorded_at=stamp))
    for platform in ("douyin", "xiaohongshu"):
        for operation in ("works", "accounts"):
            assign_route(connection, scope_type="platform_operation", scope_key=platform,
                         provider="matrix", operation=operation, expected_generation=0, route="historical_only",
                         mode="disabled", effective_at=stamp, recorded_at=stamp)
    blueprint = connection.execute(
        "INSERT INTO capture_fallback_blueprints(roster_change_id,blueprint_sha256,created_at) VALUES (?,?,?)",
        (change_id, digest({"roster": frozen, "assignments": assignment_ids}), stamp),
    )
    for assignment_id in assignment_ids:
        row = connection.execute("SELECT * FROM capture_route_assignments WHERE id=?", (assignment_id,)).fetchone()
        connection.execute("INSERT INTO capture_fallback_blueprint_members(blueprint_id,assignment_id,payload_json) VALUES (?,?,?)",
                           (blueprint.lastrowid, assignment_id, canonical(dict(row))))
    return {"status": "seeded", "roster_change_id": change_id, "blueprint_id": blueprint.lastrowid,
            "assignments": len(assignment_ids), "ordinary_paid_opened": False}


def assign_route(connection: sqlite3.Connection, *, scope_type: str, scope_key: str,
                 provider: str, operation: str, expected_generation: int,
                 route: str, mode: str, effective_at: str, recorded_at: str,
                 account_id: int | None = None, content_id: int | None = None,
                 source_plan_id: int | None = None) -> int:
    """Append a fenced assignment; caller holds the writer transaction."""
    if not connection.in_transaction:
        raise ValueError("route CAS requires writer transaction")
    previous = connection.execute(
        "SELECT id,generation,effective_at FROM capture_route_assignments WHERE scope_type=? "
        "AND scope_key=? AND operation=? ORDER BY generation DESC LIMIT 1",
        (scope_type, scope_key, operation),
    ).fetchone()
    generation = int(previous["generation"]) if previous else 0
    effective = timestamp(effective_at)
    if generation != expected_generation:
        raise ValueError("route_generation_conflict")
    if previous and effective < previous["effective_at"]:
        raise ValueError("route_effective_time_regression")
    values = {
        "scope_type": scope_type, "scope_key": scope_key, "provider": provider,
        "operation": operation, "account_id": account_id, "content_id": content_id,
        "generation": generation + 1, "previous_assignment_id": previous["id"] if previous else None,
        "source_plan_id": source_plan_id, "route": route, "mode": mode,
        "effective_at": effective, "recorded_at": timestamp(recorded_at),
    }
    values["assignment_sha256"] = digest(values)
    cursor = connection.execute(
        f"INSERT INTO capture_route_assignments({','.join(values)}) VALUES ({','.join('?' for _ in values)})",
        tuple(values.values()),
    )
    return int(cursor.lastrowid or 0)


def adaptive_cohorts(accounts: list[Mapping[str, Any]], *, business_day: str) -> list[dict[str, Any]]:
    """Freeze per-platform/history-status 97.5% video-weighted hot cohort."""
    day = datetime.strptime(business_day, "%Y-%m-%d").replace(tzinfo=BEIJING)
    groups: dict[tuple[str, str], list[dict[str, Any]]] = {}
    for account in accounts:
        item = dict(account)
        if not item.get("enabled", True):
            continue
        count = item.get("video_count", 0)
        if isinstance(count, bool) or not isinstance(count, int) or count < 0:
            raise ValueError("cohort video_count must be a non-negative integer")
        groups.setdefault((str(item["platform"]), str(item.get("monitoring_status", "unknown"))), []).append(item)
    result = []
    for (platform, monitoring), members in sorted(groups.items()):
        members.sort(key=lambda item: (-item["video_count"], str(item["uid"])))
        total = sum(item["video_count"] for item in members)
        covered = 0
        for item in members:
            count = item["video_count"]
            newly_accepted = item.get("accepted_at")
            insufficient_history = int(item.get("history_days", 7)) < 7
            if newly_accepted:
                accepted = datetime.fromisoformat(timestamp(str(newly_accepted)).replace("Z", "+00:00"))
                insufficient_history |= day - accepted < timedelta(days=7)
            hot = count > 0 and covered * 1000 < total * 975
            cohort = "hot" if hot or insufficient_history else "warm" if count else "cold"
            covered += count
            interval = {"hot": 60, "warm": 120, "cold": 180}[cohort]
            if platform == "xiaohongshu" and monitoring == "not_monitored" and cohort == "hot":
                interval = 120
            key = platform + ":" + str(item["uid"])
            phase = int(hashlib.sha256(key.encode()).hexdigest()[:16], 16) % (interval * 60)
            result.append({**item, "cohort": cohort, "interval_minutes": interval, "phase_seconds": phase})
    return sorted(result, key=lambda item: (item["platform"], str(item["uid"])))


def discovery_window(*, at: str, complete_through: str | None) -> tuple[str, str]:
    end = datetime.fromisoformat(timestamp(at).replace("Z", "+00:00"))
    start = end - timedelta(hours=72)
    if complete_through:
        watermark = datetime.fromisoformat(timestamp(complete_through).replace("Z", "+00:00"))
        if watermark > end:
            raise ValueError("watermark lies after query end")
        start = max(end - timedelta(days=30), watermark - timedelta(hours=72))
    return timestamp(start.isoformat()), timestamp(end.isoformat())


def refresh_interval(*, published_at: str, at: str, high_value: bool,
                     business_active: bool = False) -> tuple[int, int] | None:
    """Return refresh and overdue seconds, or None when not eligible."""
    age = (datetime.fromisoformat(timestamp(at).replace("Z", "+00:00"))
           - datetime.fromisoformat(timestamp(published_at).replace("Z", "+00:00"))).total_seconds()
    if age < 0:
        return None
    days = int(age // 86400)
    valuable = high_value or business_active
    if days <= 2:
        hours = (2, 3) if valuable else (6, 9)
    elif days <= 7:
        hours = (2, 3) if valuable else (12, 18)
    elif days <= 30:
        hours = (24, 36) if valuable else (72, 96)
    elif days <= 90 and business_active:
        hours = (192, 192)
    else:
        return None
    return hours[0] * 3600, hours[1] * 3600


def advance_watermark(connection: sqlite3.Connection, *, work_id: int, scope_key: str,
                      complete_through: str, evidence: Mapping[str, Any], recorded_at: str) -> int:
    """Advance only a terminal work's complete scan with conserved dispositions."""
    work = connection.execute("SELECT * FROM capture_work_items WHERE id=?", (work_id,)).fetchone()
    if (work is None or work["state"] != "terminal" or evidence.get("complete") is not True
            or evidence.get("terminal_cursor") is not True
            or evidence.get("all_raw_verified") is not True
            or evidence.get("cap_hit") is not False or evidence.get("cursor_loop") is not False):
        raise ValueError("scan is not a complete verified terminal")
    counts: list[int] = []
    for key in ("seen", "valid", "missing", "invalid", "unavailable"):
        value = evidence.get(key)
        if type(value) is not int or value < 0:
            raise ValueError("scan dispositions require non-negative integer counts")
        counts.append(value)
    if counts[0] != sum(counts[1:]):
        raise ValueError("scan dispositions do not conserve seen count")
    through = timestamp(complete_through)
    old = connection.execute("SELECT complete_through FROM capture_watermarks WHERE provider=? "
                             "AND operation=? AND scope_key=? ORDER BY complete_through DESC LIMIT 1",
                             (work["provider"], work["operation"], scope_key)).fetchone()
    if old and through < old[0]:
        raise ValueError("watermark regression")
    prior = connection.execute("SELECT id,evidence_json,complete_through FROM capture_watermarks WHERE work_id=?", (work_id,)).fetchone()
    if prior:
        if prior["evidence_json"] != canonical(evidence) or prior["complete_through"] != through:
            raise ValueError("watermark evidence conflict")
        return int(prior["id"])
    cursor = connection.execute(
        "INSERT INTO capture_watermarks(provider,operation,scope_key,work_id,complete_through,cursor_json,evidence_json,recorded_at) "
        "VALUES (?,?,?,?,?,'null',?,?)",
        (work["provider"], work["operation"], scope_key, work_id, through, canonical(evidence), timestamp(recorded_at)),
    )
    return int(cursor.lastrowid or 0)


def backlog(connection: sqlite3.Connection, *, at: str) -> dict[str, Any]:
    instant = datetime.fromisoformat(timestamp(at).replace("Z", "+00:00"))
    result: dict[str, Any] = {}
    for row in connection.execute(
        "SELECT state,COUNT(*) AS count,MIN(due_at) AS oldest FROM capture_work_items "
        "WHERE state!='terminal' AND due_at<=? GROUP BY state", (timestamp(at),),
    ):
        oldest = datetime.fromisoformat(row["oldest"].replace("Z", "+00:00"))
        result[row["state"]] = {"count": row["count"], "oldest_seconds": (instant - oldest).total_seconds()}
    return result
