"""Schema-20 paid identities and conservative, append-only settlement evidence.

All writes require the caller's transaction. This module never sends requests,
changes historical usage/slot guards, or releases provider/identity holds.
"""

from __future__ import annotations

import hashlib
import json
import sqlite3
from collections import defaultdict
from datetime import datetime, timedelta, timezone
from decimal import Decimal
from typing import Any, Mapping
from zoneinfo import ZoneInfo

from .paid_identity import build_paid_request_identity

SCHEMA_SQL = """
CREATE TABLE IF NOT EXISTS legacy_provider_send_evidences (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    provider_usage_id INTEGER NOT NULL UNIQUE REFERENCES provider_usage(id) ON DELETE RESTRICT,
    fetch_attempt_id INTEGER REFERENCES fetch_attempts(id) ON DELETE RESTRICT,
    scope_identity TEXT,
    source_sha256 TEXT NOT NULL CHECK(length(source_sha256)=64),
    source_json TEXT NOT NULL CHECK(json_valid(source_json)),
    created_at TEXT NOT NULL
);
CREATE TABLE IF NOT EXISTS legacy_paid_scope_exclusions (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    scope_identity TEXT NOT NULL,
    evidence_sha256 TEXT NOT NULL UNIQUE CHECK(length(evidence_sha256)=64),
    evidence_ref TEXT NOT NULL CHECK(length(trim(evidence_ref))>0),
    reason TEXT NOT NULL CHECK(length(trim(reason))>0),
    created_at TEXT NOT NULL
);
CREATE INDEX IF NOT EXISTS idx_legacy_send_scope ON legacy_provider_send_evidences(scope_identity);
CREATE INDEX IF NOT EXISTS idx_settlement_send_usage ON paid_provider_dispatch_events(provider_usage_id)
WHERE event_type='send_marked';
CREATE INDEX IF NOT EXISTS idx_legacy_scope_exclusions
ON legacy_paid_scope_exclusions(scope_identity);
CREATE TABLE IF NOT EXISTS provider_usage_settlements (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    provider_usage_id INTEGER NOT NULL UNIQUE REFERENCES provider_usage(id) ON DELETE RESTRICT,
    provider_send_marker_id INTEGER UNIQUE REFERENCES paid_provider_dispatch_events(id) ON DELETE RESTRICT,
    legacy_send_evidence_id INTEGER UNIQUE REFERENCES legacy_provider_send_evidences(id) ON DELETE RESTRICT,
    scope_identity TEXT,
    compensation_sequence INTEGER NOT NULL DEFAULT 0 CHECK(compensation_sequence BETWEEN 0 AND 4),
    charge_business_day TEXT NOT NULL CHECK(length(charge_business_day)=10),
    currency TEXT NOT NULL,
    amount_microunits INTEGER NOT NULL CHECK(amount_microunits>=0),
    source_sha256 TEXT NOT NULL CHECK(length(source_sha256)=64),
    original_state TEXT NOT NULL,
    created_at TEXT NOT NULL,
    CHECK((provider_send_marker_id IS NOT NULL)+(legacy_send_evidence_id IS NOT NULL)=1)
);
CREATE INDEX IF NOT EXISTS idx_usage_settlement_scope
ON provider_usage_settlements(scope_identity,compensation_sequence);
CREATE TABLE IF NOT EXISTS provider_billing_evidence_claims (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    settlement_id INTEGER NOT NULL REFERENCES provider_usage_settlements(id) ON DELETE RESTRICT,
    evidence_kind TEXT NOT NULL CHECK(evidence_kind IN ('supplier_record','complete_raw_fee_contract')),
    evidence_ref TEXT NOT NULL CHECK(length(trim(evidence_ref))>0),
    evidence_sha256 TEXT NOT NULL UNIQUE CHECK(length(evidence_sha256)=64),
    outcome TEXT NOT NULL CHECK(outcome IN ('charged_verified','refunded')),
    amount_microunits INTEGER NOT NULL CHECK(amount_microunits>=0),
    created_at TEXT NOT NULL
);
CREATE TABLE IF NOT EXISTS provider_usage_settlement_events (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    settlement_id INTEGER NOT NULL REFERENCES provider_usage_settlements(id) ON DELETE RESTRICT,
    sequence INTEGER NOT NULL CHECK(sequence>=1),
    state TEXT NOT NULL CHECK(state IN ('charged_unverified','charged_verified','refunded')),
    amount_microunits INTEGER NOT NULL CHECK(amount_microunits>=0),
    billing_evidence_claim_id INTEGER UNIQUE REFERENCES provider_billing_evidence_claims(id) ON DELETE RESTRICT,
    created_at TEXT NOT NULL,
    UNIQUE(settlement_id,sequence),
    CHECK((state='charged_unverified' AND billing_evidence_claim_id IS NULL)
       OR (state IN ('charged_verified','refunded') AND billing_evidence_claim_id IS NOT NULL))
);
CREATE TABLE IF NOT EXISTS compensation_authorizations (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    authorization_key TEXT NOT NULL UNIQUE,
    original_settlement_id INTEGER NOT NULL REFERENCES provider_usage_settlements(id) ON DELETE RESTRICT,
    scope_kind TEXT NOT NULL CHECK(scope_kind IN ('request','member')),
    scope_identity TEXT NOT NULL,
    next_sequence INTEGER NOT NULL CHECK(next_sequence BETWEEN 1 AND 4),
    reason TEXT NOT NULL CHECK(length(trim(reason))>0),
    owner TEXT NOT NULL CHECK(length(trim(owner))>0),
    gap_evidence_ref TEXT NOT NULL CHECK(length(trim(gap_evidence_ref))>0),
    raw_unrecoverable_reason TEXT NOT NULL CHECK(length(trim(raw_unrecoverable_reason))>0),
    max_amount_microunits INTEGER NOT NULL CHECK(max_amount_microunits>0),
    max_requests INTEGER NOT NULL DEFAULT 1 CHECK(max_requests=1),
    expires_at TEXT NOT NULL,
    created_at TEXT NOT NULL
);
CREATE TABLE IF NOT EXISTS compensation_authorization_decisions (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    authorization_id INTEGER NOT NULL REFERENCES compensation_authorizations(id) ON DELETE RESTRICT,
    decision TEXT NOT NULL CHECK(decision IN ('issued','blocked')),
    reason TEXT NOT NULL,
    created_at TEXT NOT NULL
);
CREATE TABLE IF NOT EXISTS compensation_authorization_issuances (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    authorization_id INTEGER NOT NULL UNIQUE REFERENCES compensation_authorizations(id) ON DELETE RESTRICT,
    decision_id INTEGER NOT NULL UNIQUE REFERENCES compensation_authorization_decisions(id) ON DELETE RESTRICT,
    created_at TEXT NOT NULL
);
CREATE TABLE IF NOT EXISTS provider_paid_scope_claims (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    scope_kind TEXT NOT NULL CHECK(scope_kind IN ('request','member')),
    scope_identity TEXT NOT NULL CHECK(length(scope_identity)=64),
    sequence INTEGER NOT NULL CHECK(sequence BETWEEN 0 AND 4),
    provider_send_marker_id INTEGER NOT NULL REFERENCES paid_provider_dispatch_events(id) ON DELETE RESTRICT,
    authorization_issuance_id INTEGER UNIQUE REFERENCES compensation_authorization_issuances(id) ON DELETE RESTRICT,
    created_at TEXT NOT NULL,
    UNIQUE(scope_identity,sequence),
    CHECK((sequence=0 AND authorization_issuance_id IS NULL)
       OR (sequence>0 AND authorization_issuance_id IS NOT NULL))
);
CREATE TABLE IF NOT EXISTS authorization_issuance_consumptions (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    issuance_id INTEGER NOT NULL UNIQUE REFERENCES compensation_authorization_issuances(id) ON DELETE RESTRICT,
    paid_scope_claim_id INTEGER NOT NULL UNIQUE REFERENCES provider_paid_scope_claims(id) ON DELETE RESTRICT,
    created_at TEXT NOT NULL
);
CREATE UNIQUE INDEX IF NOT EXISTS uq_paid_request_claim_marker
ON provider_paid_scope_claims(provider_send_marker_id) WHERE scope_kind='request';
CREATE TABLE IF NOT EXISTS provider_request_start_events (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    provider_send_marker_id INTEGER NOT NULL UNIQUE REFERENCES paid_provider_dispatch_events(id) ON DELETE RESTRICT,
    request_scope_claim_id INTEGER NOT NULL UNIQUE REFERENCES provider_paid_scope_claims(id) ON DELETE RESTRICT,
    started_at TEXT NOT NULL
);
CREATE TRIGGER IF NOT EXISTS trg_legacy_send_evidence_guard
BEFORE INSERT ON legacy_provider_send_evidences
WHEN NOT EXISTS(SELECT 1 FROM provider_usage WHERE id=NEW.provider_usage_id AND request_attempts>0)
 OR EXISTS(SELECT 1 FROM paid_provider_dispatch_events
           WHERE provider_usage_id=NEW.provider_usage_id AND event_type='send_marked')
BEGIN SELECT RAISE(ABORT,'legacy evidence requires sent usage without a send marker'); END;
CREATE TRIGGER IF NOT EXISTS trg_usage_settlement_evidence_guard
BEFORE INSERT ON provider_usage_settlements
WHEN NOT EXISTS(SELECT 1 FROM provider_usage WHERE id=NEW.provider_usage_id AND request_attempts>0)
 OR (NEW.provider_send_marker_id IS NOT NULL AND NOT EXISTS(
       SELECT 1 FROM paid_provider_dispatch_events WHERE id=NEW.provider_send_marker_id
       AND event_type='send_marked' AND provider_usage_id=NEW.provider_usage_id))
 OR (NEW.legacy_send_evidence_id IS NOT NULL AND NOT EXISTS(
       SELECT 1 FROM legacy_provider_send_evidences WHERE id=NEW.legacy_send_evidence_id
       AND provider_usage_id=NEW.provider_usage_id))
BEGIN SELECT RAISE(ABORT,'settlement evidence does not identify a sent usage'); END;
CREATE TRIGGER IF NOT EXISTS trg_usage_settlement_event_guard
BEFORE INSERT ON provider_usage_settlement_events
WHEN NEW.sequence != COALESCE((SELECT MAX(sequence)+1 FROM provider_usage_settlement_events
                              WHERE settlement_id=NEW.settlement_id),1)
 OR EXISTS(SELECT 1 FROM provider_usage_settlement_events WHERE settlement_id=NEW.settlement_id
           AND state IN ('charged_verified','refunded'))
 OR (NEW.state='charged_unverified' AND (NEW.sequence!=1 OR NEW.amount_microunits !=
       (SELECT amount_microunits FROM provider_usage_settlements WHERE id=NEW.settlement_id)))
 OR (NEW.billing_evidence_claim_id IS NOT NULL AND NOT EXISTS(
       SELECT 1 FROM provider_billing_evidence_claims WHERE id=NEW.billing_evidence_claim_id
       AND settlement_id=NEW.settlement_id AND outcome=NEW.state
       AND amount_microunits=NEW.amount_microunits))
BEGIN SELECT RAISE(ABORT,'settlement transition lacks unique billing evidence'); END;
CREATE TRIGGER IF NOT EXISTS trg_paid_scope_claim_marker_guard
BEFORE INSERT ON provider_paid_scope_claims
WHEN NOT EXISTS(SELECT 1 FROM paid_provider_dispatch_events d JOIN provider_usage u
                 ON u.id=d.provider_usage_id WHERE d.id=NEW.provider_send_marker_id
                 AND d.event_type='send_marked' AND u.request_attempts>0)
 OR EXISTS(SELECT 1 FROM legacy_paid_scope_exclusions WHERE scope_identity=NEW.scope_identity)
 OR (NEW.sequence=0 AND EXISTS(SELECT 1 FROM provider_usage_settlements
                              WHERE scope_identity=NEW.scope_identity))
 OR (NEW.sequence>0 AND NOT EXISTS(
       SELECT 1 FROM compensation_authorization_issuances i
       JOIN compensation_authorizations a ON a.id=i.authorization_id
       WHERE i.id=NEW.authorization_issuance_id AND a.scope_kind=NEW.scope_kind
       AND a.scope_identity=NEW.scope_identity AND a.next_sequence=NEW.sequence
       AND a.expires_at>NEW.created_at AND a.created_at<=NEW.created_at))
BEGIN SELECT RAISE(ABORT,'paid identity lacks send evidence or compensation authority'); END;
CREATE TRIGGER IF NOT EXISTS trg_request_start_guard
BEFORE INSERT ON provider_request_start_events
WHEN NOT EXISTS(SELECT 1 FROM provider_paid_scope_claims WHERE id=NEW.request_scope_claim_id
                AND scope_kind='request' AND provider_send_marker_id=NEW.provider_send_marker_id)
BEGIN SELECT RAISE(ABORT,'network start requires its request claim and send marker'); END;
CREATE TRIGGER IF NOT EXISTS trg_compensation_issuance_guard
BEFORE INSERT ON compensation_authorization_issuances
WHEN NOT EXISTS(SELECT 1 FROM compensation_authorization_decisions WHERE id=NEW.decision_id
                AND authorization_id=NEW.authorization_id AND decision='issued')
 OR COALESCE((SELECT e.state FROM compensation_authorizations a
              JOIN provider_usage_settlement_events e ON e.settlement_id=a.original_settlement_id
              WHERE a.id=NEW.authorization_id ORDER BY e.sequence DESC LIMIT 1),'')!='charged_unverified'
BEGIN SELECT RAISE(ABORT,'compensation issuance requires an issued decision'); END;
CREATE TRIGGER IF NOT EXISTS trg_compensation_consumption_guard
BEFORE INSERT ON authorization_issuance_consumptions
WHEN NOT EXISTS(SELECT 1 FROM provider_paid_scope_claims WHERE id=NEW.paid_scope_claim_id
                AND authorization_issuance_id=NEW.issuance_id AND sequence>0)
BEGIN SELECT RAISE(ABORT,'compensation consumption does not match its paid claim'); END;
"""

_TABLES = (
    "legacy_provider_send_evidences", "legacy_paid_scope_exclusions",
    "provider_usage_settlements", "provider_usage_settlement_events",
    "provider_billing_evidence_claims", "provider_paid_scope_claims",
    "provider_request_start_events", "compensation_authorizations",
    "compensation_authorization_decisions", "compensation_authorization_issuances",
    "authorization_issuance_consumptions",
)
for _table in _TABLES:
    for _action in ("UPDATE", "DELETE"):
        SCHEMA_SQL += f"""
CREATE TRIGGER IF NOT EXISTS trg_{_table}_no_{_action.lower()}
BEFORE {_action} ON {_table}
BEGIN SELECT RAISE(ABORT,'paid evidence is append-only'); END;
"""


class SettlementError(ValueError):
    """Fail-closed identity, source integrity, or accounting contract failure."""


def _require(value: bool, message: str) -> None:
    if not value:
        raise SettlementError(message)


def _transaction(connection: sqlite3.Connection) -> None:
    _require(connection.in_transaction, "An explicit caller transaction is required")


def _json(value: Any) -> str:
    return json.dumps(value, ensure_ascii=False, sort_keys=True, separators=(",", ":"), allow_nan=False)


def _sha(value: Any) -> str:
    return hashlib.sha256(_json(value).encode("utf-8")).hexdigest()


def _utc(value: str) -> str:
    parsed = datetime.fromisoformat(value.replace("Z", "+00:00"))
    _require(parsed.tzinfo is not None, "Timestamp requires a timezone")
    return parsed.astimezone(timezone.utc).replace(microsecond=0).isoformat().replace("+00:00", "Z")


def _amount(value: Any) -> int:
    decimal = Decimal(str(value or 0)) * 1_000_000
    _require(decimal.is_finite() and decimal >= 0 and decimal == decimal.to_integral_value(),
             "Amount must be nonnegative and exactly representable in millionths")
    return int(decimal)


def _row(connection: sqlite3.Connection, sql: str, params: tuple[Any, ...]) -> dict[str, Any]:
    cursor = connection.execute(sql, params)
    value = cursor.fetchone()
    _require(value is not None, "Required evidence row is missing")
    return dict(zip((column[0] for column in cursor.description), value, strict=True))


def scope_identity(*, provider: str, operation: str, platform: str, subject_id: str | int,
                   parameters: Mapping[str, Any], cursor: Any, due_bucket: str,
                   scope_kind: str = "request", request_window: Mapping[str, str] | None = None) -> str:
    """Hash only logical billing inputs; execution/activation versions are absent."""
    _require(scope_kind in {"request", "member"}, "Invalid paid scope kind")
    _require(bool(provider and operation and platform and str(subject_id) and due_bucket),
             "Paid identity requires provider, operation, subject and logical due bucket")
    request = build_paid_request_identity(
        provider=provider, operation=operation, platform=platform, subject=str(subject_id),
        request_parameters=parameters, cursor=cursor, due_bucket=due_bucket, request_window=request_window,
    )
    return request.scope_identity if scope_kind == "request" else _sha(
        {"contract_version": "paid-member-identity-v1", "scope": request.document}
    )


def _source(row: Mapping[str, Any]) -> tuple[dict[str, Any], str, int, str]:
    metadata = json.loads(str(row["details_json"]))
    _require(isinstance(metadata, dict), "Usage metadata must be an object")
    day = metadata.get("budget_day") or datetime.fromisoformat(
        _utc(str(row["recorded_at"])).replace("Z", "+00:00")
    ).astimezone(ZoneInfo("Asia/Shanghai")).date().isoformat()
    _require(isinstance(day, str) and len(day) == 10, "Invalid original charge day")
    datetime.strptime(day, "%Y-%m-%d")
    return metadata, day, _amount(row["amount"]), str(row["currency"] or "UNKNOWN")


def _marker(connection: sqlite3.Connection, usage_id: int) -> int | None:
    values = list(connection.execute(
        "SELECT id FROM paid_provider_dispatch_events WHERE provider_usage_id=? AND event_type='send_marked'",
        (usage_id,),
    ))
    _require(len(values) <= 1, "Usage is ambiguously bound to multiple send markers")
    return int(values[0][0]) if values else None


def record_settlement(connection: sqlite3.Connection, *, usage_id: int, at: str,
                      allow_legacy: bool = False) -> dict[str, Any]:
    """Conservatively close a real send, without asserting supplier verification."""
    _transaction(connection)
    source = _row(connection, "SELECT * FROM provider_usage WHERE id=?", (usage_id,))
    _require(source["request_attempts"] > 0, "Unsent provenance cannot be settled")
    metadata, day, amount, currency = _source(source)
    digest = _sha(source)
    existing = connection.execute(
        "SELECT id,source_sha256 FROM provider_usage_settlements WHERE provider_usage_id=?", (usage_id,),
    ).fetchone()
    if existing:
        _require(existing[1] == digest, "Historical usage changed after settlement")
        return read_settlement(connection, int(existing[0]))
    marker_id = _marker(connection, usage_id)
    legacy_id = None
    if marker_id is None:
        _require(allow_legacy, "A runtime settlement requires a real send marker")
        attempts = list(connection.execute(
            "SELECT id FROM fetch_attempts WHERE slot_id=? AND attempt_number=?",
            (metadata.get("slot_id"), metadata.get("attempt_number")),
        )) if type(metadata.get("slot_id")) is int and type(metadata.get("attempt_number")) is int else []
        result = connection.execute(
            """INSERT INTO legacy_provider_send_evidences
               (provider_usage_id,fetch_attempt_id,scope_identity,source_sha256,source_json,created_at)
               VALUES(?,?,?,?,?,?)""",
            (usage_id, int(attempts[0][0]) if len(attempts) == 1 else None,
             metadata.get("paid_scope_identity"), digest, _json(source), _utc(at)),
        )
        legacy_id = result.lastrowid
    claims = list(connection.execute(
        "SELECT scope_identity,sequence FROM provider_paid_scope_claims WHERE provider_send_marker_id=? AND scope_kind='request'",
        (marker_id,),
    ))
    _require(len(claims) <= 1, "A physical request has more than one request identity")
    identity = claims[0][0] if claims else metadata.get("paid_scope_identity")
    sequence = claims[0][1] if claims else metadata.get("paid_sequence", metadata.get("compensation_sequence", 0))
    _require(type(sequence) is int and 0 <= sequence <= 4, "Invalid source compensation sequence")
    result = connection.execute(
        """INSERT INTO provider_usage_settlements
           (provider_usage_id,provider_send_marker_id,legacy_send_evidence_id,scope_identity,
            compensation_sequence,charge_business_day,currency,amount_microunits,source_sha256,
            original_state,created_at) VALUES(?,?,?,?,?,?,?,?,?,?,?)""",
        (usage_id, marker_id, legacy_id, identity, sequence, day, currency, amount, digest,
         str(metadata.get("state") or "legacy_unknown"), _utc(at)),
    )
    settlement_id = int(result.lastrowid or 0)
    connection.execute(
        """INSERT INTO provider_usage_settlement_events
           (settlement_id,sequence,state,amount_microunits,created_at)
           VALUES(?,1,'charged_unverified',?,?)""", (settlement_id, amount, _utc(at)),
    )
    return read_settlement(connection, settlement_id)


def read_settlement(connection: sqlite3.Connection, settlement_id: int) -> dict[str, Any]:
    result = _row(connection, """SELECT s.*,e.state,e.amount_microunits AS settled_amount_microunits,
                     e.sequence AS event_sequence FROM provider_usage_settlements s
                     JOIN provider_usage_settlement_events e ON e.settlement_id=s.id
                     WHERE s.id=? ORDER BY e.sequence DESC LIMIT 1""", (settlement_id,))
    result["provider_bill_verified"] = result["state"] != "charged_unverified"
    result["releases_paid_hold"] = False
    return result


def migrate_legacy(connection: sqlite3.Connection) -> dict[str, Any]:
    """Freeze and classify the caller's sealed source; original rows are untouched.

    The migration owner installs SCHEMA_SQL first and runs this within the same
    sealed transaction. Filesystem sidecars are separately inventoried through
    record_legacy_exclusion; they can never manufacture a DB sent event.
    """
    _transaction(connection)
    cursor = connection.execute("SELECT * FROM provider_usage ORDER BY id")
    names = [column[0] for column in cursor.description]
    sources = [dict(zip(names, row, strict=True)) for row in cursor]
    groups: dict[str, list[dict[str, Any]]] = {key: [] for key in ("unsent", "marker", "legacy")}
    for source in sources:
        _require(type(source["request_attempts"]) is int and source["request_attempts"] >= 0,
                 "Invalid request attempts in sealed source")
        marker = _marker(connection, int(source["id"]))
        category = "unsent" if source["request_attempts"] == 0 else "marker" if marker else "legacy"
        if category == "unsent":
            _require(marker is None, "Unsent source unexpectedly owns a send marker")
        else:
            record_settlement(connection, usage_id=source["id"], at=source["recorded_at"], allow_legacy=True)
        groups[category].append(source)
    def manifest(rows: list[dict[str, Any]]) -> dict[str, Any]:
        amounts: dict[str, int] = defaultdict(int)
        for row in rows:
            _, day, amount, currency = _source(row)
            amounts[f"{day}:{currency}"] += amount
        return {"count": len(rows), "ids_sha256": _sha([row["id"] for row in rows]),
                "rows_sha256": _sha(rows), "amounts_by_charge_day_currency": dict(sorted(amounts.items()))}
    after = connection.execute("SELECT * FROM provider_usage ORDER BY id")
    _require(sources == [dict(zip(names, row, strict=True)) for row in after], "Usage source changed during migration")
    sent_ids = sorted(row["id"] for key in ("marker", "legacy") for row in groups[key])
    settled_ids = [row[0] for row in connection.execute(
        "SELECT provider_usage_id FROM provider_usage_settlements ORDER BY provider_usage_id"
    )]
    _require(sent_ids == settled_ids, "Sent usage ID set is not conserved")
    actual: dict[str, int] = defaultdict(int)
    for day, currency, amount in connection.execute(
        "SELECT charge_business_day,currency,amount_microunits FROM provider_usage_settlements"
    ):
        actual[f"{day}:{currency}"] += amount
    expected = manifest(groups["marker"] + groups["legacy"])["amounts_by_charge_day_currency"]
    _require(dict(actual) == expected, "Original charge-day/currency amounts are not conserved")
    return {"contract_version": "historical-billing-migration-v1", "source": manifest(sources),
            "classes": {key: manifest(rows) for key, rows in groups.items()},
            "settled_ids_sha256": _sha(settled_ids), "charge_conserved": True,
            "provider_bill_verified": False, "sidecar_inventory_required": True,
            "legacy_exclusions": connection.execute("SELECT COUNT(*) FROM legacy_paid_scope_exclusions").fetchone()[0]}


def record_legacy_exclusion(connection: sqlite3.Connection, *, identity: str, evidence_sha256: str,
                            evidence_ref: str, reason: str, at: str) -> int:
    """Block orphan scope replay without counting it as a paid send or charge."""
    _transaction(connection)
    payload = (identity, evidence_sha256, evidence_ref, reason, _utc(at))
    existing = connection.execute(
        "SELECT id,scope_identity,evidence_sha256,evidence_ref,reason,created_at FROM legacy_paid_scope_exclusions WHERE evidence_sha256=?",
        (evidence_sha256,),
    ).fetchone()
    if existing:
        _require(tuple(existing)[1:] == payload, "Orphan exclusion idempotency conflict")
        return int(existing[0])
    return int(connection.execute(
        "INSERT INTO legacy_paid_scope_exclusions(scope_identity,evidence_sha256,evidence_ref,reason,created_at) VALUES(?,?,?,?,?)",
        payload,
    ).lastrowid or 0)


def claim_paid_scope(connection: sqlite3.Connection, *, identity: str, marker_id: int,
                     at: str, scope_kind: str = "request", sequence: int = 0,
                     issuance_id: int | None = None) -> int:
    """Claim request/member sequence once in the send-marker transaction."""
    _transaction(connection)
    existing = connection.execute(
        "SELECT id,scope_kind,provider_send_marker_id,authorization_issuance_id FROM provider_paid_scope_claims WHERE scope_identity=? AND sequence=?",
        (identity, sequence),
    ).fetchone()
    if existing:
        _require(tuple(existing)[1:] == (scope_kind, marker_id, issuance_id), "Paid identity already purchased")
        return int(existing[0])
    if issuance_id is not None:
        authorization = _row(connection, """SELECT a.*,s.provider_usage_id AS original_usage_id
               FROM compensation_authorization_issuances i
               JOIN compensation_authorizations a ON a.id=i.authorization_id
               JOIN provider_usage_settlements s ON s.id=a.original_settlement_id
               WHERE i.id=?""", (issuance_id,))
        _require(read_settlement(connection, int(authorization["original_settlement_id"]))["state"]
                 == "charged_unverified", "Compensation requires a charged_unverified accounting terminal")
        original = _row(connection, "SELECT provider,operation FROM provider_usage WHERE id=?",
                        (authorization["original_usage_id"],))
        current = _row(connection, """SELECT u.provider,u.operation,u.amount FROM provider_usage u
                      JOIN paid_provider_dispatch_events d ON d.provider_usage_id=u.id WHERE d.id=?""", (marker_id,))
        _require(original["provider"].lower() == current["provider"].lower()
                 and original["operation"] == current["operation"]
                 and _amount(current["amount"]) <= authorization["max_amount_microunits"],
                 "Compensation provider, operation or amount exceeds its authorization")
    _require(not connection.execute(
        "SELECT 1 FROM legacy_provider_send_evidences WHERE scope_identity=?", (identity,),
    ).fetchone() or sequence > 0, "Historical paid identity is held")
    if scope_kind == "request":
        _require(not connection.execute(
            "SELECT 1 FROM provider_paid_scope_claims WHERE provider_send_marker_id=? AND scope_kind='request'",
            (marker_id,),
        ).fetchone(), "A physical request already has its paid identity")
    claim_id = int(connection.execute(
        """INSERT INTO provider_paid_scope_claims
           (scope_kind,scope_identity,sequence,provider_send_marker_id,authorization_issuance_id,created_at)
           VALUES(?,?,?,?,?,?)""", (scope_kind, identity, sequence, marker_id, issuance_id, _utc(at)),
    ).lastrowid or 0)
    if issuance_id is not None:
        connection.execute(
            "INSERT INTO authorization_issuance_consumptions(issuance_id,paid_scope_claim_id,created_at) VALUES(?,?,?)",
            (issuance_id, claim_id, _utc(at)),
        )
    return claim_id


def record_request_start(connection: sqlite3.Connection, *, marker_id: int, request_claim_id: int,
                         at: str) -> int:
    """Count the schema-20 network boundary separately from historical markers."""
    _transaction(connection)
    existing = connection.execute(
        "SELECT id,request_scope_claim_id FROM provider_request_start_events WHERE provider_send_marker_id=?", (marker_id,),
    ).fetchone()
    if existing:
        _require(existing[1] == request_claim_id, "Network start identity conflict")
        return int(existing[0])
    return int(connection.execute(
        "INSERT INTO provider_request_start_events(provider_send_marker_id,request_scope_claim_id,started_at) VALUES(?,?,?)",
        (marker_id, request_claim_id, _utc(at)),
    ).lastrowid or 0)


def member_identity(request: Mapping[str, Any]) -> str:
    """Member identity preserves the already validated paid-identity-v1 document."""
    return _sha({"contract_version": "paid-member-identity-v1", "scope": dict(request)})


def require_scope_available(connection: sqlite3.Connection, *, identity: str,
                            sequence: int = 0) -> None:
    """Read-only preflight; the same check repeats in the send transaction.

    Claim deletion, unknown settlement and a new worker never release identity.
    Compensation remains held until a fresh issuance is checked at the boundary.
    """
    _require(type(sequence) is int and 0 <= sequence <= 4, "Invalid paid sequence")
    _require(len(identity) == 64 and all(c in "0123456789abcdef" for c in identity),
             "Invalid paid scope hash")
    _require(not connection.execute(
        "SELECT 1 FROM provider_paid_scope_claims WHERE scope_identity=? AND sequence=?",
        (identity, sequence)).fetchone(), "Paid identity already purchased")
    if sequence == 0:
        for table in ("provider_usage_settlements", "legacy_provider_send_evidences",
                      "legacy_paid_scope_exclusions"):
            _require(not connection.execute(
                f"SELECT 1 FROM {table} WHERE scope_identity=? LIMIT 1", (identity,)).fetchone(),
                "Historical or settled paid identity is held")


def claim_network_start(connection: sqlite3.Connection, *, marker_id: int,
                        request_identity: str, at: str, sequence: int = 0,
                        member_identities: tuple[str, ...] = (),
                        issuance_ids: Mapping[str, int] | None = None) -> dict[str, Any]:
    """Non-replayable send permit: one request marker and all member identities.

    Unlike ledger replay helpers, this method MUST fail on an existing start or
    claim. A caller may invoke HTTP only after this transaction commits once.
    The savepoint prevents a member conflict from leaving a partial request.
    """
    _transaction(connection)
    identities = (request_identity, *member_identities)
    _require(len(set(identities)) == len(identities), "Request/member identities overlap")
    issuances = dict(issuance_ids or {})
    _require(set(issuances) <= set(identities), "Issuance does not belong to this request")
    _require((sequence == 0 and not issuances)
             or (sequence > 0 and set(issuances) == set(identities)),
             "Compensation requires a fresh issuance for every request/member scope")
    _require(not connection.execute(
        "SELECT 1 FROM provider_request_start_events WHERE provider_send_marker_id=?", (marker_id,)
    ).fetchone(), "Network request already started")
    for identity in identities:
        require_scope_available(connection, identity=identity, sequence=sequence)
    connection.execute("SAVEPOINT paid_network_start")
    try:
        claim_ids = [claim_paid_scope(connection, identity=identity, marker_id=marker_id,
            at=at, scope_kind="request" if index == 0 else "member", sequence=sequence,
            issuance_id=issuances.get(identity)) for index, identity in enumerate(identities)]
        start_id = record_request_start(connection, marker_id=marker_id,
                                        request_claim_id=claim_ids[0], at=at)
        connection.execute("RELEASE SAVEPOINT paid_network_start")
    except BaseException:
        connection.execute("ROLLBACK TO SAVEPOINT paid_network_start")
        connection.execute("RELEASE SAVEPOINT paid_network_start")
        raise
    return {"request_start_id": start_id, "request_claim_id": claim_ids[0],
            "member_claim_ids": claim_ids[1:], "new_start": True}


def reconcile_due(connection: sqlite3.Connection, *, at: str, limit: int = 500) -> dict[str, Any]:
    """Run every five minutes; sent rows are conservatively closed at age ten minutes.

    Two passes before fifteen minutes leave scheduling margin. Evidence-based
    refinement is independent; this never closes dispatch chains or clears holds.
    """
    _transaction(connection)
    _require(1 <= limit <= 10000, "Invalid settlement reconciliation batch")
    cutoff = (datetime.fromisoformat(_utc(at).replace("Z", "+00:00")) - timedelta(minutes=10)).isoformat().replace("+00:00", "Z")
    rows = list(connection.execute(
        """SELECT u.id FROM provider_usage u JOIN paid_provider_dispatch_events d
             ON d.provider_usage_id=u.id AND d.event_type='send_marked'
             LEFT JOIN provider_usage_settlements s ON s.provider_usage_id=u.id
             WHERE u.request_attempts>0 AND s.id IS NULL AND d.created_at<=?
             ORDER BY d.created_at,u.id LIMIT ?""", (cutoff, limit),
    ))
    ids = [record_settlement(connection, usage_id=int(row[0]), at=at)["id"] for row in rows]
    return {"settlement_ids": ids, "count": len(ids), "provider_calls": 0, "releases_paid_hold": False}


def verify_settlement(connection: sqlite3.Connection, *, settlement_id: int, evidence_kind: str,
                      evidence_ref: str, evidence_sha256: str, outcome: str,
                      amount_microunits: int, at: str) -> dict[str, Any]:
    """Record explicitly verified supplier/raw-fee evidence; success alone is invalid."""
    _transaction(connection)
    settlement = read_settlement(connection, settlement_id)
    _require(evidence_kind in {"supplier_record", "complete_raw_fee_contract"}, "Billing evidence kind is not authoritative")
    _require(outcome in {"charged_verified", "refunded"}, "Invalid verified billing outcome")
    _require(amount_microunits == (0 if outcome == "refunded" else settlement["amount_microunits"]),
             "Verified amount must match the original charge or full refund")
    existing = connection.execute(
        "SELECT settlement_id,evidence_kind,evidence_ref,outcome,amount_microunits FROM provider_billing_evidence_claims WHERE evidence_sha256=?",
        (evidence_sha256,),
    ).fetchone()
    expected = (settlement_id, evidence_kind, evidence_ref, outcome, amount_microunits)
    if existing:
        _require(tuple(existing) == expected, "Billing evidence already claimed for another outcome")
        return settlement
    _require(settlement["state"] == "charged_unverified", "Verified accounting is immutable")
    claim_id = connection.execute(
        """INSERT INTO provider_billing_evidence_claims
           (settlement_id,evidence_kind,evidence_ref,evidence_sha256,outcome,amount_microunits,created_at)
           VALUES(?,?,?,?,?,?,?)""", (*expected[:3], evidence_sha256, outcome, amount_microunits, _utc(at)),
    ).lastrowid
    connection.execute(
        """INSERT INTO provider_usage_settlement_events
           (settlement_id,sequence,state,amount_microunits,billing_evidence_claim_id,created_at)
           VALUES(?,?,?,?,?,?)""",
        (settlement_id, settlement["event_sequence"] + 1, outcome, amount_microunits, claim_id, _utc(at)),
    )
    return read_settlement(connection, settlement_id)


def singleton_compensation_document(connection: sqlite3.Connection, *, settlement_id: int,
                                    gap_evidence_ref: str) -> dict[str, Any]:
    """Derive only a proven missing/invalid member's singleton statistics request.

    A new request packing hash must never erase the member's paid lineage. The
    typed reference resolves existing immutable batch/disposition/claim rows.
    """
    from .paid_identity import build_paid_request_identity

    prefix = "batch-member:"
    _require(gap_evidence_ref.startswith(prefix) and gap_evidence_ref[len(prefix):].isdigit(),
             "Singleton compensation requires a concrete batch-member evidence reference")
    member_id = int(gap_evidence_ref[len(prefix):])
    settled = read_settlement(connection, settlement_id)
    row = _row(connection, """SELECT m.*,b.request_scope_identity,b.operation,c.platform_content_id
        FROM fetch_request_batch_members m JOIN fetch_request_batches b ON b.id=m.batch_id
        JOIN content_items c ON c.id=m.content_id WHERE m.id=?""", (member_id,))
    _require(row["operation"] == "douyin_video_statistics" and row["request_scope_identity"] == settled["scope_identity"],
             "Member evidence does not belong to the settled statistics request")
    disposition = connection.execute("SELECT disposition FROM fetch_request_member_dispositions WHERE member_id=? ORDER BY id DESC LIMIT 1",
                                      (member_id,)).fetchone()
    _require(disposition is not None and disposition[0] in {"missing", "invalid", "unusable"},
             "Singleton compensation requires an observed missing/invalid member, not a valid result")
    source = _row(connection, "SELECT details_json FROM provider_usage WHERE id=?", (settled["provider_usage_id"],))
    original = json.loads(source["details_json"]).get("paid_identity")
    _require(isinstance(original, dict), "Original batch request document is absent")
    request = build_paid_request_identity(provider="tikhub", operation=row["operation"], platform="douyin",
        subject=str(row["platform_content_id"]), request_parameters={"aweme_ids": str(row["platform_content_id"])},
        cursor=original["cursor"], request_window=original["request_window"], due_bucket=original["due_bucket"])
    _require(member_identity(request.document) == row["member_scope_identity"] and bool(connection.execute(
        "SELECT 1 FROM provider_paid_scope_claims WHERE scope_kind='member' AND scope_identity=? AND provider_send_marker_id=?",
        (row["member_scope_identity"], settled["provider_send_marker_id"])).fetchone()),
        "Singleton member lacks the original paid claim")
    return request.document


def authorize_compensation(connection: sqlite3.Connection, *, authorization_key: str,
                           settlement_id: int, identity: str, scope_kind: str, owner: str,
                           reason: str, gap_evidence_ref: str, raw_unrecoverable_reason: str,
                           local_replay_exhausted: bool, business_gap_due: bool,
                           max_amount_microunits: int, expires_at: str, at: str,
                           provider_ready: bool, budget_available: bool) -> dict[str, Any]:
    """Issue a single sequence+1 permission, at most four across runs/batches."""
    _transaction(connection)
    settlement = read_settlement(connection, settlement_id)
    _require(settlement["state"] == "charged_unverified",
             "Compensation requires a charged_unverified accounting terminal")
    _require(type(provider_ready) is bool and type(budget_available) is bool,
             "Compensation requires explicit current readiness and budget decisions")
    _require(local_replay_exhausted is True and business_gap_due is True
             and bool(raw_unrecoverable_reason.strip()) and bool(gap_evidence_ref.strip()),
             "Compensation requires an unrecoverable raw reason, exhausted replay and a due business gap")
    _require(_utc(expires_at) > _utc(at), "Compensation authorization is expired")
    _require(bool(authorization_key.strip()) and bool(owner.strip()) and bool(reason.strip())
             and max_amount_microunits > 0, "Compensation requires key, owner, reason and a positive ceiling")
    saved = connection.execute(
        """SELECT id,authorization_key,original_settlement_id,scope_kind,scope_identity,next_sequence,
                  reason,owner,gap_evidence_ref,raw_unrecoverable_reason,max_amount_microunits,expires_at
           FROM compensation_authorizations WHERE authorization_key=?""", (authorization_key,),
    ).fetchone()
    previous_claim = connection.execute(
        "SELECT MAX(sequence) FROM provider_paid_scope_claims WHERE scope_identity=? AND scope_kind=?", (identity, scope_kind),
    ).fetchone()[0]
    member_bound = connection.execute(
        "SELECT 1 FROM provider_paid_scope_claims WHERE scope_identity=? AND scope_kind=? AND provider_send_marker_id=?",
        (identity, scope_kind, settlement["provider_send_marker_id"]),
    ).fetchone()
    owns_scope = settlement["scope_identity"] == identity or bool(member_bound)
    if not owns_scope and scope_kind == "request":
        from .paid_identity import build_paid_request_identity

        document = singleton_compensation_document(connection, settlement_id=settlement_id, gap_evidence_ref=gap_evidence_ref)
        derived = build_paid_request_identity(provider=document["provider"], operation=document["operation"],
            platform=document["platform"], subject=document["subject"], request_parameters=document["request_parameters"],
            cursor=document["cursor"], request_window=document["request_window"], due_bucket=document["due_bucket"])
        owns_scope = derived.scope_identity == identity
    _require(owns_scope, "Compensation scope does not own the original settlement")
    last_sequence = max(int(previous_claim or 0), int(settlement["compensation_sequence"]))
    next_sequence = int(saved[5]) if saved else last_sequence + 1
    if not saved:
        _require(last_sequence < 4, "Compensation retry ceiling reached; preserve cursor in DLQ")
        original_sequence = settlement["compensation_sequence"]
        if scope_kind == "member":
            original_sequence = _row(connection, """SELECT sequence FROM provider_paid_scope_claims
                WHERE scope_identity=? AND scope_kind='member' AND provider_send_marker_id=?""",
                (identity, settlement["provider_send_marker_id"]))["sequence"]
        _require(last_sequence == original_sequence, "Compensation must bind the latest sent sequence's settlement")
    _require(not connection.execute(
        """SELECT 1 FROM compensation_authorizations a JOIN compensation_authorization_issuances i
             ON i.authorization_id=a.id WHERE a.scope_identity=? AND a.next_sequence=?
             AND a.authorization_key!=? AND a.expires_at>?""", (identity, next_sequence, authorization_key, _utc(at)),
    ).fetchone(), "A live authorization already exists for this paid sequence")
    payload = (authorization_key, settlement_id, scope_kind, identity, next_sequence,
               reason, owner, gap_evidence_ref, raw_unrecoverable_reason, max_amount_microunits, _utc(expires_at))
    existing = saved
    if existing:
        _require(tuple(existing)[1:] == payload, "Compensation idempotency conflict")
        auth_id = int(existing[0])
        issuance = connection.execute("SELECT id FROM compensation_authorization_issuances WHERE authorization_id=?", (auth_id,)).fetchone()
        if issuance:
            return {"authorization_id": auth_id, "issuance_id": int(issuance[0]), "sequence": next_sequence, "status": "issued"}
    else:
        auth_id = int(connection.execute(
            """INSERT INTO compensation_authorizations
               (authorization_key,original_settlement_id,scope_kind,scope_identity,next_sequence,
                reason,owner,gap_evidence_ref,raw_unrecoverable_reason,max_amount_microunits,expires_at,created_at)
               VALUES(?,?,?,?,?,?,?,?,?,?,?,?)""", (*payload, _utc(at)),
        ).lastrowid or 0)
    decision = "issued" if provider_ready and budget_available else "blocked"
    decision_id = connection.execute(
        "INSERT INTO compensation_authorization_decisions(authorization_id,decision,reason,created_at) VALUES(?,?,?,?)",
        (auth_id, decision, "eligible" if decision == "issued" else "provider_or_budget_blocked", _utc(at)),
    ).lastrowid
    issuance_id = None
    if decision == "issued":
        issuance_id = connection.execute(
            "INSERT INTO compensation_authorization_issuances(authorization_id,decision_id,created_at) VALUES(?,?,?)",
            (auth_id, decision_id, _utc(at)),
        ).lastrowid
    return {"authorization_id": auth_id, "issuance_id": issuance_id, "sequence": next_sequence, "status": decision}


def validate_compensation_issuance(connection: sqlite3.Connection, *, issuance_id: int,
                                  identity: str, scope_kind: str, sequence: int,
                                  provider: str, operation: str,
                                  amount_microunits: int, at: str) -> dict[str, Any]:
    """A/B preflight: a fresh explicitly issued grant, never merely a settlement."""
    auth = _row(connection, """SELECT a.*,i.created_at AS issued_at,
                   s.provider_usage_id AS original_usage_id
                   FROM compensation_authorization_issuances i
                   JOIN compensation_authorizations a ON a.id=i.authorization_id
                   JOIN provider_usage_settlements s ON s.id=a.original_settlement_id
                   JOIN compensation_authorization_decisions d ON d.id=i.decision_id
                     AND d.authorization_id=a.id AND d.decision='issued'
                   WHERE i.id=?""", (issuance_id,))
    _require((auth["scope_identity"], auth["scope_kind"], auth["next_sequence"])
             == (identity, scope_kind, sequence), "Compensation issuance scope or sequence mismatch")
    _require(_utc(auth["issued_at"]) <= _utc(at) < _utc(auth["expires_at"]),
             "Compensation authorization is expired or not yet issued")
    _require(not connection.execute(
        "SELECT 1 FROM authorization_issuance_consumptions WHERE issuance_id=?",
        (issuance_id,)).fetchone(), "Compensation issuance already consumed")
    settled = read_settlement(connection, int(auth["original_settlement_id"]))
    _require(settled["state"] == "charged_unverified",
             "Compensation requires a charged_unverified accounting terminal")
    original = _row(connection, "SELECT provider,operation FROM provider_usage WHERE id=?",
                    (auth["original_usage_id"],))
    _require(original["provider"].lower() == provider.lower()
             and original["operation"] == operation
             and 0 < amount_microunits <= auth["max_amount_microunits"],
             "Compensation provider, operation or amount exceeds its authorization")
    require_scope_available(connection, identity=identity, sequence=sequence)
    return auth


def consume_compensation(connection: sqlite3.Connection, *, issuance_id: int, marker_id: int,
                         at: str, provider_ready: bool, budget_available: bool) -> int:
    """Recheck live admission then atomically consume the one-use issuance."""
    _transaction(connection)
    _require(provider_ready and budget_available, "Provider or budget blocked; compensation sequence is not consumed")
    auth = _row(connection, """SELECT a.* FROM compensation_authorization_issuances i
                  JOIN compensation_authorizations a ON a.id=i.authorization_id WHERE i.id=?""", (issuance_id,))
    return claim_paid_scope(connection, identity=auth["scope_identity"], marker_id=marker_id,
                            at=at, scope_kind=auth["scope_kind"], sequence=auth["next_sequence"], issuance_id=issuance_id)
