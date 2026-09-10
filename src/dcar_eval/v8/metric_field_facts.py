"""Immutable, bitemporal metric facts and bounded canonical projections.

This module never commits and never buys data. Callers own the transaction.
Historical ingestion proves each observation's raw/attempt/slot lineage using
the same policy-v2 contract as the compatibility reader. Recording a correction
does not change the original value or its capture/expiry time.
"""

from __future__ import annotations

import hashlib
import json
import sqlite3
from datetime import datetime, timedelta, timezone
from typing import Any

from .source_routing import (
    METRIC_FIELDS,
    POLICY_VERSION,
    _field_evidence,
    _field_state,
    _observation_metadata,
    _select_row,
    correction_spec,
    effective_operation,
    effective_provider,
    load_policy,
    metric_freshness_seconds,
    parse_time,
)

TABLES = (
    "content_availability_observations", "content_metric_field_facts",
    "content_metric_corrections", "provider_response_field_evidences",
    "metric_policy_transitions", "content_metric_ttl_transition_events",
    "content_metric_projection_versions", "content_metric_projection_causal_events",
    "content_identity_merge_events",
)


def utc(value: str) -> str:
    """Normalize metric-domain time to UTC seconds; IDs break same-second ties."""
    return parse_time(value).isoformat(timespec="seconds").replace("+00:00", "Z")


def _time_check(column: str) -> str:
    return (
        f"CHECK({column} IS NULL OR (length({column})=20 AND {column} GLOB "
        f"'????-??-??T??:??:??Z' AND julianday({column}) IS NOT NULL))"
    )


_FIELDS_SQL = ",".join(f"'{name}'" for name in METRIC_FIELDS)
SCHEMA_SQL = f"""
CREATE TABLE IF NOT EXISTS content_identity_merge_events (
 id INTEGER PRIMARY KEY AUTOINCREMENT,
 loser_content_id INTEGER NOT NULL UNIQUE REFERENCES content_items(id) ON DELETE RESTRICT,
 winner_content_id INTEGER NOT NULL REFERENCES content_items(id) ON DELETE RESTRICT,
 recorded_at TEXT NOT NULL {_time_check('recorded_at')},
 identity_snapshot_json TEXT NOT NULL CHECK(json_valid(identity_snapshot_json)),
 reason TEXT NOT NULL,
 event_sha256 TEXT NOT NULL UNIQUE CHECK(length(event_sha256)=64),
 CHECK(loser_content_id<>winner_content_id)
);
CREATE INDEX IF NOT EXISTS idx_metric_merge_winner_knowledge
 ON content_identity_merge_events(winner_content_id,recorded_at,loser_content_id);
CREATE TRIGGER IF NOT EXISTS trg_metric_merge_no_cycle BEFORE INSERT ON content_identity_merge_events
WHEN EXISTS (
 WITH RECURSIVE ancestors(id) AS (
 SELECT NEW.winner_content_id UNION ALL
 SELECT e.winner_content_id FROM content_identity_merge_events e JOIN ancestors a ON e.loser_content_id=a.id
 ) SELECT 1 FROM ancestors WHERE id=NEW.loser_content_id
)
BEGIN SELECT RAISE(ABORT,'content identity merge cycle'); END;
CREATE TABLE IF NOT EXISTS content_availability_observations (
 id INTEGER PRIMARY KEY AUTOINCREMENT,
 content_id INTEGER NOT NULL REFERENCES content_items(id) ON DELETE RESTRICT,
 observation_id INTEGER UNIQUE REFERENCES content_metric_observations(id) ON DELETE RESTRICT,
 provider TEXT NOT NULL, operation TEXT NOT NULL,
 availability TEXT NOT NULL CHECK(availability IN ('unknown','available','deleted','private','unavailable','no_permission')),
 captured_at TEXT NOT NULL {_time_check('captured_at')},
 recorded_at TEXT NOT NULL {_time_check('recorded_at')},
 raw_response_id INTEGER REFERENCES provider_raw_responses(id) ON DELETE RESTRICT,
 reason TEXT NOT NULL, evidence_sha256 TEXT NOT NULL UNIQUE CHECK(length(evidence_sha256)=64),
 CHECK(captured_at<=recorded_at)
);
CREATE INDEX IF NOT EXISTS idx_metric_availability_source_capture
 ON content_availability_observations(content_id,provider,operation,captured_at DESC,id DESC);
CREATE TABLE IF NOT EXISTS content_metric_field_facts (
 id INTEGER PRIMARY KEY AUTOINCREMENT,
 content_id INTEGER NOT NULL REFERENCES content_items(id) ON DELETE RESTRICT,
 observation_id INTEGER NOT NULL REFERENCES content_metric_observations(id) ON DELETE RESTRICT,
 field TEXT NOT NULL CHECK(field IN ({_FIELDS_SQL})),
 provider TEXT NOT NULL, operation TEXT NOT NULL,
 value INTEGER CHECK(value IS NULL OR (typeof(value)='integer' AND value>=0)),
 state TEXT NOT NULL CHECK(state IN ('provided','missing','invalid','audit_only','not_applicable','not_requested')),
 reason TEXT NOT NULL, observed_value_json TEXT NOT NULL CHECK(json_valid(observed_value_json)),
 captured_at TEXT NOT NULL {_time_check('captured_at')},
 recorded_at TEXT NOT NULL {_time_check('recorded_at')},
 provider_data_at TEXT {_time_check('provider_data_at')},
 time_basis TEXT NOT NULL CHECK(time_basis IN ('provider_data_at','capture_only')),
 raw_response_id INTEGER REFERENCES provider_raw_responses(id) ON DELETE RESTRICT,
 window_key TEXT NOT NULL, observation_status TEXT NOT NULL,
 observation_origin TEXT NOT NULL, contract_version TEXT NOT NULL,
 fact_sha256 TEXT NOT NULL UNIQUE CHECK(length(fact_sha256)=64),
 UNIQUE(observation_id,field), CHECK(captured_at<=recorded_at),
 CHECK((state='provided' AND value IS NOT NULL) OR (state<>'provided' AND value IS NULL)),
 CHECK((provider_data_at IS NULL AND time_basis='capture_only') OR
       (provider_data_at IS NOT NULL AND time_basis='provider_data_at'))
);
CREATE INDEX IF NOT EXISTS idx_metric_field_source_capture
 ON content_metric_field_facts(content_id,provider,operation,field,captured_at DESC,id DESC);
CREATE INDEX IF NOT EXISTS idx_metric_field_source_bitemporal
 ON content_metric_field_facts(content_id,provider,operation,field,captured_at DESC,recorded_at DESC,id DESC);
CREATE INDEX IF NOT EXISTS idx_metric_field_content_knowledge
 ON content_metric_field_facts(content_id,recorded_at DESC,id DESC);
CREATE INDEX IF NOT EXISTS idx_metric_field_content_capture
 ON content_metric_field_facts(content_id,captured_at DESC,id DESC);
CREATE INDEX IF NOT EXISTS idx_metric_field_window_source_capture
 ON content_metric_field_facts(content_id,window_key,provider,operation,field,captured_at DESC,recorded_at DESC,id DESC);
CREATE INDEX IF NOT EXISTS idx_metric_observations_window_origin_capture
 ON content_metric_observations(content_id,window_key,observation_origin,captured_at DESC,id DESC);
CREATE INDEX IF NOT EXISTS idx_metric_field_window_origin_capture
 ON content_metric_field_facts(content_id,window_key,observation_origin,field,captured_at DESC,recorded_at DESC,id DESC);
CREATE TABLE IF NOT EXISTS content_metric_corrections (
 id INTEGER PRIMARY KEY AUTOINCREMENT,
 target_fact_id INTEGER NOT NULL REFERENCES content_metric_field_facts(id) ON DELETE RESTRICT,
 source_observation_id INTEGER REFERENCES content_metric_observations(id) ON DELETE RESTRICT,
 action TEXT NOT NULL CHECK(action IN ('invalidate','replace')),
 value INTEGER CHECK(value IS NULL OR (typeof(value)='integer' AND value>=0)),
 rule_id TEXT NOT NULL CHECK(length(trim(rule_id))>0),
 recorded_at TEXT NOT NULL {_time_check('recorded_at')},
 correction_sha256 TEXT NOT NULL UNIQUE CHECK(length(correction_sha256)=64),
 UNIQUE(target_fact_id,rule_id),
 CHECK((action='invalidate' AND value IS NULL) OR (action='replace' AND value IS NOT NULL))
);
CREATE INDEX IF NOT EXISTS idx_metric_correction_target_knowledge
 ON content_metric_corrections(target_fact_id,recorded_at DESC,id DESC);
CREATE INDEX IF NOT EXISTS idx_metric_correction_knowledge
 ON content_metric_corrections(recorded_at,id);
CREATE TABLE IF NOT EXISTS provider_response_field_evidences (
 id INTEGER PRIMARY KEY AUTOINCREMENT,
 fact_id INTEGER NOT NULL UNIQUE REFERENCES content_metric_field_facts(id) ON DELETE RESTRICT,
 raw_response_id INTEGER REFERENCES provider_raw_responses(id) ON DELETE RESTRICT,
 state TEXT NOT NULL, raw_value_json TEXT NOT NULL CHECK(json_valid(raw_value_json)),
 provider_data_at TEXT {_time_check('provider_data_at')},
 captured_at TEXT NOT NULL {_time_check('captured_at')},
 recorded_at TEXT NOT NULL {_time_check('recorded_at')},
 evidence_sha256 TEXT NOT NULL UNIQUE CHECK(length(evidence_sha256)=64)
);
CREATE TABLE IF NOT EXISTS metric_policy_transitions (
 id INTEGER PRIMARY KEY AUTOINCREMENT,
 policy_version TEXT NOT NULL, provider TEXT NOT NULL, operation TEXT NOT NULL,
 field TEXT NOT NULL CHECK(field IN ({_FIELDS_SQL})),
 eligibility TEXT NOT NULL CHECK(eligibility IN ('active','historical_only','audit_only','invalid')),
 priority INTEGER NOT NULL CHECK(priority>=0),
 ttl_seconds INTEGER CHECK(ttl_seconds IS NULL OR ttl_seconds>0),
 effective_at TEXT NOT NULL {_time_check('effective_at')},
 recorded_at TEXT NOT NULL {_time_check('recorded_at')},
 contract_receipt_sha256 TEXT NOT NULL CHECK(length(contract_receipt_sha256)=64),
 transition_sha256 TEXT NOT NULL UNIQUE CHECK(length(transition_sha256)=64),
 UNIQUE(policy_version,provider,operation,field,effective_at)
);
CREATE INDEX IF NOT EXISTS idx_metric_policy_source_effective
 ON metric_policy_transitions(policy_version,provider,operation,field,effective_at DESC,id DESC);
CREATE TABLE IF NOT EXISTS content_metric_ttl_transition_events (
 id INTEGER PRIMARY KEY AUTOINCREMENT,
 fact_id INTEGER NOT NULL REFERENCES content_metric_field_facts(id) ON DELETE RESTRICT,
 policy_version TEXT NOT NULL,
 expires_at TEXT NOT NULL {_time_check('expires_at')},
 recorded_at TEXT NOT NULL {_time_check('recorded_at')},
 event_sha256 TEXT NOT NULL UNIQUE CHECK(length(event_sha256)=64),
 UNIQUE(fact_id,policy_version,expires_at)
);
CREATE INDEX IF NOT EXISTS idx_metric_ttl_due
 ON content_metric_ttl_transition_events(expires_at,fact_id);
CREATE TABLE IF NOT EXISTS content_metric_projection_versions (
 id INTEGER PRIMARY KEY AUTOINCREMENT,
 content_id INTEGER NOT NULL REFERENCES content_items(id) ON DELETE RESTRICT,
 policy_version TEXT NOT NULL, window_key TEXT,
 cutoff_at TEXT NOT NULL {_time_check('cutoff_at')},
 knowledge_at TEXT NOT NULL {_time_check('knowledge_at')},
 next_transition_at TEXT {_time_check('next_transition_at')},
 payload_json TEXT NOT NULL CHECK(json_valid(payload_json)),
 causal_set_hash TEXT NOT NULL UNIQUE CHECK(length(causal_set_hash)=64)
);
CREATE INDEX IF NOT EXISTS idx_metric_projection_content_knowledge
 ON content_metric_projection_versions(content_id,policy_version,knowledge_at DESC,id DESC);
CREATE INDEX IF NOT EXISTS idx_metric_projection_window_knowledge
 ON content_metric_projection_versions(content_id,policy_version,window_key,knowledge_at DESC,id DESC);
CREATE TABLE IF NOT EXISTS content_metric_projection_causal_events (
 id INTEGER PRIMARY KEY AUTOINCREMENT,
 causal_set_hash TEXT NOT NULL REFERENCES content_metric_projection_versions(causal_set_hash)
   ON DELETE RESTRICT DEFERRABLE INITIALLY DEFERRED,
 field TEXT NOT NULL CHECK(field IN ({_FIELDS_SQL})),
 selected_fact_id INTEGER REFERENCES content_metric_field_facts(id) ON DELETE RESTRICT,
 latest_fact_id INTEGER REFERENCES content_metric_field_facts(id) ON DELETE RESTRICT,
 correction_id INTEGER REFERENCES content_metric_corrections(id) ON DELETE RESTRICT,
 availability_id INTEGER REFERENCES content_availability_observations(id) ON DELETE RESTRICT,
 freshness_evidence_id INTEGER REFERENCES provider_response_field_evidences(id) ON DELETE RESTRICT,
 policy_transition_id INTEGER REFERENCES metric_policy_transitions(id) ON DELETE RESTRICT,
 ttl_transition_id INTEGER REFERENCES content_metric_ttl_transition_events(id) ON DELETE RESTRICT,
 inputs_json TEXT NOT NULL CHECK(json_valid(inputs_json)),
 UNIQUE(causal_set_hash,field)
);
""" + "\n".join(
    f"""CREATE TRIGGER IF NOT EXISTS trg_{table}_no_{action.lower()}
 BEFORE {action} ON {table} BEGIN
 SELECT RAISE(ABORT,'{table} is append-only'); END;"""
    for table in TABLES for action in ("UPDATE", "DELETE")
)

_OBSERVATION_SQL = """
SELECT o.*,r.id raw_id,r.provider raw_provider,r.operation raw_operation,
 r.content_id raw_content_id,r.account_id raw_account_id,
 r.fetch_attempt_id raw_fetch_attempt_id,a.id raw_attempt_id,a.slot_id raw_attempt_slot_id,
 s.id raw_slot_id,s.stage raw_slot_stage,s.provider raw_slot_provider,
 s.content_id raw_slot_content_id,s.account_id raw_slot_account_id,c.account_id,c.platform
FROM content_metric_observations o JOIN content_items c ON c.id=o.content_id
LEFT JOIN provider_raw_responses r ON r.id=o.raw_response_id
LEFT JOIN fetch_attempts a ON a.id=r.fetch_attempt_id
LEFT JOIN fetch_slots s ON s.id=a.slot_id
"""


def observation_query(connection: sqlite3.Connection) -> str:
    """Shared raw lineage join, including explicit schema20 batch membership."""
    if connection.execute(
        "SELECT 1 FROM sqlite_master WHERE name='fetch_request_batch_members'"
    ).fetchone() is None:
        return _OBSERVATION_SQL
    return _OBSERVATION_SQL.replace(
        "c.account_id,c.platform\nFROM", """c.account_id,c.platform,
 a.request_batch_id raw_attempt_batch_id,b.id raw_batch_id,b.provider raw_batch_provider,
 b.operation raw_batch_operation,(SELECT m.content_id FROM fetch_request_batch_members m
   JOIN fetch_request_member_dispositions d ON d.member_id=m.id
   JOIN fetch_request_executions x ON x.batch_id=m.batch_id AND x.fetch_attempt_id=a.id
   WHERE m.batch_id=b.id AND m.content_id=o.content_id AND d.disposition='valid'
     AND d.raw_response_id=r.id LIMIT 1) raw_batch_member_content_id,
 (SELECT m.account_id FROM fetch_request_batch_members m
   JOIN fetch_request_member_dispositions d ON d.member_id=m.id
   JOIN fetch_request_executions x ON x.batch_id=m.batch_id AND x.fetch_attempt_id=a.id
   WHERE m.batch_id=b.id AND m.content_id IS NULL AND m.account_id=c.account_id
     AND d.disposition='valid' AND d.raw_response_id=r.id LIMIT 1) raw_batch_member_account_id
FROM""",
    ) + """LEFT JOIN fetch_request_batches b ON b.id=a.request_batch_id
"""


def observation_provenance(
    connection: sqlite3.Connection, *, content_id: int, source: str,
    raw_response_id: int | None, metadata_json: str,
) -> dict[str, Any]:
    """Prepare immutable provenance columns before an observation INSERT."""
    sql = observation_query(connection).replace("content_metric_observations o", "observation_input o")
    row = connection.execute(
        "WITH observation_input AS (SELECT ? content_id,? source,? raw_response_id,? metadata_json) " + sql,
        (content_id, source, raw_response_id, metadata_json),
    ).fetchone()
    if row is None:
        raise ValueError("unknown metric content")
    evidence = dict(row)
    metadata = json.loads(metadata_json or "{}")
    provider_time = metadata.get("provider_data_at") if isinstance(metadata, dict) else None
    return dict(
        effective_provider=effective_provider(evidence), provider_operation=effective_operation(evidence),
        provider_data_at=utc(provider_time) if isinstance(provider_time, str) else None,
        time_basis="provider_data_at" if isinstance(provider_time, str) else "capture_only",
        contract_version=POLICY_VERSION, hash_version="original-v1",
    )


def resolve_metric_content_id(
    connection: sqlite3.Connection, content_id: int, *, knowledge_at: str | None = None,
) -> int:
    stamp = utc(knowledge_at or datetime.now(timezone.utc).isoformat())
    row = connection.execute(
        """WITH RECURSIVE lineage(id,depth) AS (
        SELECT ?,0 UNION ALL SELECT e.winner_content_id,l.depth+1
        FROM content_identity_merge_events e JOIN lineage l ON e.loser_content_id=l.id
        WHERE e.recorded_at<=?) SELECT id FROM lineage ORDER BY depth DESC LIMIT 1""",
        (content_id, stamp),
    ).fetchone()
    return int(row[0])


def metric_content_scope(
    connection: sqlite3.Connection, content_id: int, *, knowledge_at: str,
) -> list[int]:
    stamp = utc(knowledge_at)
    winner = resolve_metric_content_id(connection, content_id, knowledge_at=stamp)
    return [int(row[0]) for row in connection.execute(
        """WITH RECURSIVE members(id) AS (SELECT ? UNION ALL
        SELECT e.loser_content_id FROM content_identity_merge_events e JOIN members m ON e.winner_content_id=m.id
        WHERE e.recorded_at<=?) SELECT id FROM members ORDER BY id""", (winner, stamp),
    )]


def append_identity_merge(
    connection: sqlite3.Connection, *, winner_id: int, loser_id: int,
    recorded_at: str, identity_snapshot: dict[str, Any],
) -> int:
    _require_transaction(connection)
    values = dict(loser_content_id=loser_id, winner_content_id=winner_id, recorded_at=utc(recorded_at),
                  identity_snapshot_json=_json(identity_snapshot), reason="identity_upgrade_merge")
    values["event_sha256"] = _hash(values)
    return _insert(connection, "content_identity_merge_events", values, "event_sha256")


def _json(value: Any) -> str:
    return json.dumps(value, ensure_ascii=False, sort_keys=True, separators=(",", ":"))


def _hash(value: Any) -> str:
    return hashlib.sha256(_json(value).encode()).hexdigest()


def _require_transaction(connection: sqlite3.Connection) -> None:
    if not connection.in_transaction:
        raise ValueError("metric facts require an active caller transaction")


def _insert(connection: sqlite3.Connection, table: str, values: dict[str, Any], digest: str) -> int:
    columns = ",".join(values)
    connection.execute(
        f"INSERT INTO {table}({columns}) VALUES ({','.join('?' for _ in values)}) "
        f"ON CONFLICT({digest}) DO NOTHING", tuple(values.values()),
    )
    row = connection.execute(
        f"SELECT id FROM {table} WHERE {digest}=?", (values[digest],),
    ).fetchone()
    if row is None:
        raise ValueError(f"idempotent insert failed for {table}")
    return int(row[0])


def record_correction(
    connection: sqlite3.Connection, target_fact_id: int, *, action: str,
    rule_id: str, recorded_at: str, value: int | None = None,
    source_observation_id: int | None = None,
) -> int:
    """Correct one original fact; corrections can never target corrections."""
    _require_transaction(connection)
    target = connection.execute(
        "SELECT * FROM content_metric_field_facts WHERE id=?", (target_fact_id,),
    ).fetchone()
    stamp = utc(recorded_at)
    if target is None or stamp < str(target["recorded_at"]):
        raise ValueError("correction must reference a known original fact")
    if action not in {"replace", "invalidate"} or not rule_id.strip():
        raise ValueError("invalid correction contract")
    if (action == "replace" and (type(value) is not int or value < 0)) or (
        action == "invalidate" and value is not None
    ):
        raise ValueError("invalid correction value")
    values = dict(target_fact_id=target_fact_id, source_observation_id=source_observation_id,
                  action=action, value=value, rule_id=rule_id, recorded_at=stamp)
    values["correction_sha256"] = _hash(values)
    return _insert(connection, "content_metric_corrections", values, "correction_sha256")


def _ingest(connection: sqlite3.Connection, row: dict[str, Any]) -> dict[str, Any]:
    if row["observation_origin"] == "system_correction":
        spec = correction_spec(row)
        if spec is None:
            raise ValueError(f"invalid historical correction observation {row['id']}")
        targets = connection.execute(
            "SELECT * FROM content_metric_field_facts WHERE observation_id=?",
            (spec["target_observation_id"],),
        ).fetchall()
        if len(targets) != len(METRIC_FIELDS):
            raise ValueError("correction target must be ingested before correction")
        for target in targets:
            if target["content_id"] != row["content_id"] or target["window_key"] != row["window_key"]:
                raise ValueError("correction target content/window mismatch")
            if target["captured_at"] != utc(row["captured_at"]):
                raise ValueError("correction must retain original capture time")
            if target["raw_response_id"] != row["raw_response_id"]:
                raise ValueError("correction must retain original raw reference")
            if target["field"] in spec["fields"]:
                record_correction(
                    connection, int(target["id"]), action=spec["action"],
                    rule_id=spec["rule_id"], recorded_at=row["recorded_at"],
                    value=row[target["field"]] if spec["action"] == "replace" else None,
                    source_observation_id=int(row["id"]),
                )
        return {"observation_id": int(row["id"]), "correction": True, "fields": 0}
    provider, operation = effective_provider(row), effective_operation(row) or "legacy_unknown"
    metadata = json.loads(row["metadata_json"] or "{}")
    if not isinstance(metadata, dict):
        metadata = {}
    provider_time = row.get("provider_data_at") or metadata.get("provider_data_at")
    # createTime/published_at are deliberately not substituted for data time.
    provider_data_at = utc(provider_time) if isinstance(provider_time, str) else None
    captured, recorded = utc(row["captured_at"]), utc(row["recorded_at"])
    if provider_data_at is not None and provider_data_at > captured:
        raise ValueError("provider data time cannot be later than capture")
    for field in METRIC_FIELDS:
        state, value, reason = _field_state(row, field, str(row["platform"]))
        values = dict(
            content_id=int(row["content_id"]), observation_id=int(row["id"]), field=field,
            provider=provider, operation=operation, value=value, state=state, reason=reason,
            observed_value_json=_json(row[field]), captured_at=captured, recorded_at=recorded,
            provider_data_at=provider_data_at,
            time_basis="provider_data_at" if provider_data_at else "capture_only",
            raw_response_id=row["raw_response_id"], window_key=row["window_key"],
            observation_status=row["status"], observation_origin=row["observation_origin"],
            contract_version=POLICY_VERSION,
        )
        values["fact_sha256"] = _hash(values)
        fact_id = _insert(connection, "content_metric_field_facts", values, "fact_sha256")
        evidence = dict(
            fact_id=fact_id, raw_response_id=row["raw_response_id"], state=state,
            raw_value_json=_json(row[field]), provider_data_at=provider_data_at,
            captured_at=captured, recorded_at=recorded,
        )
        evidence["evidence_sha256"] = _hash(evidence)
        _insert(connection, "provider_response_field_evidences", evidence, "evidence_sha256")
    availability = metadata.get("availability", "unknown")
    if availability not in {"unknown", "available", "deleted", "private", "unavailable", "no_permission"}:
        availability = "unknown"
    available = dict(
        content_id=int(row["content_id"]), observation_id=int(row["id"]),
        provider=provider, operation=operation, availability=availability,
        captured_at=captured, recorded_at=recorded, raw_response_id=row["raw_response_id"],
        reason="explicit_provider_availability" if availability != "unknown" else "not_observed",
    )
    available["evidence_sha256"] = _hash(available)
    _insert(connection, "content_availability_observations", available, "evidence_sha256")
    return {"observation_id": int(row["id"]), "provider": provider,
            "operation": operation, "correction": False, "fields": len(METRIC_FIELDS)}


def _record_incremental_anomalies(connection: sqlite3.Connection, observation_id: int) -> None:
    """Compare new provider facts with one prior value in the same stream.

    This is a diagnostic only. It does not change facts, select a maximum,
    schedule a request or scan old observations for historical corrections.
    """
    for fact in connection.execute(
        "SELECT * FROM content_metric_field_facts WHERE observation_id=? AND state='provided'",
        (observation_id,),
    ).fetchall():
        previous = connection.execute(
            "SELECT * FROM content_metric_field_facts WHERE content_id=? AND provider=? "
            "AND operation=? AND field=? AND state='provided' AND id<>? "
            "ORDER BY captured_at DESC,recorded_at DESC,id DESC LIMIT 1",
            (fact["content_id"], fact["provider"], fact["operation"], fact["field"], fact["id"]),
        ).fetchone()
        if previous is None or (previous["captured_at"], previous["recorded_at"], previous["id"]) >= (
            fact["captured_at"], fact["recorded_at"], fact["id"]
        ):
            continue  # First value or an out-of-order arrival is not a jump.
        correction = connection.execute(
            "SELECT action,value FROM content_metric_corrections WHERE target_fact_id=? "
            "AND recorded_at<=? ORDER BY recorded_at DESC,id DESC LIMIT 1",
            (previous["id"], fact["recorded_at"]),
        ).fetchone()
        if correction is not None and correction["action"] == "invalidate":
            continue
        old = int(correction["value"] if correction is not None else previous["value"])
        new = int(fact["value"])
        # Integer comparisons avoid float precision loss for large counters.
        decline = old - new > 100 and (old - new) * 5 > old
        increase = new - old > 1000 and new - old > old * 5
        if not (decline or increase):
            continue
        scope = {key: fact[key] for key in ("content_id", "provider", "operation", "field")}
        evidence = {"contract_version": "capture-new-metric-anomaly-v1",
                    "previous_fact_id": previous["id"], "fact_id": fact["id"],
                    "raw_response_id": fact["raw_response_id"], "previous_value": old, "value": new,
                    "kind": "decline" if decline else "increase", "captured_at": fact["captured_at"],
                    "automatic_correction": False, "provider_calls": 0}
        connection.execute(
            "INSERT OR IGNORE INTO operational_alerts(dedupe_key,severity,scope_json,evidence_json,owner,status,opened_at) "
            "VALUES(?,'P2',?,?,'capture-data','open',?)",
            ("capture:metric-jump:" + fact["fact_sha256"], _json(scope), _json(evidence), fact["recorded_at"]),
        )


def ingest_observation(connection: sqlite3.Connection, observation_id: int, *,
                       record_anomalies: bool = False) -> dict[str, Any]:
    _require_transaction(connection)
    row = connection.execute(observation_query(connection) + " WHERE o.id=?", (observation_id,)).fetchone()
    if row is None:
        raise ValueError("unknown metric observation")
    data = dict(row)
    if data["observation_origin"] == "provider_capture" and effective_provider(data) == "legacy_unknown":
        raise ValueError("new provider facts require proven raw/attempt/slot lineage")
    existing = connection.execute(
        "SELECT 1 FROM content_metric_field_facts WHERE observation_id=? LIMIT 1", (observation_id,),
    ).fetchone()
    result = _ingest(connection, data)
    if (record_anomalies and existing is None and data["observation_origin"] == "provider_capture"
            and data["raw_response_id"] is not None and effective_provider(data) in {"tikhub", "newrank_matrix"}):
        _record_incremental_anomalies(connection, observation_id)
    return result


def migrate_legacy(connection: sqlite3.Connection) -> dict[str, Any]:
    """Stream source IDs without changing observations, raw IDs, or their hashes."""
    _require_transaction(connection)
    result: dict[str, Any] = {"observations": 0, "facts": 0, "corrections": 0,
                              "providers": {}, "origins": {}, "baseline_with_raw": 0,
                              "baseline_without_raw": 0}
    lineage_hash = hashlib.sha256()
    new_columns = {item[1] for item in connection.execute("PRAGMA table_info(content_metric_observations)")}
    has_provenance_columns = {
        "effective_provider", "provider_operation", "provider_data_at", "time_basis",
        "contract_version", "hash_version",
    }.issubset(new_columns)
    for row in connection.execute(
        observation_query(connection) + " ORDER BY (o.observation_origin='system_correction'),o.id"
    ):
        data = dict(row)
        migrated = _ingest(connection, data)
        result["observations"] += 1
        result["facts"] += migrated["fields"]
        result["corrections"] += int(migrated["correction"])
        origin = str(data["observation_origin"])
        result["origins"][origin] = result["origins"].get(origin, 0) + 1
        if origin == "legacy_snapshot_baseline":
            key = "baseline_with_raw" if data["raw_id"] is not None else "baseline_without_raw"
            result[key] += 1
        provider = migrated.get("provider", "system_correction")
        if has_provenance_columns and not migrated["correction"]:
            connection.execute(
                """UPDATE content_metric_observations SET effective_provider=?,provider_operation=?,
                provider_data_at=(SELECT provider_data_at FROM content_metric_field_facts
                                  WHERE observation_id=? LIMIT 1),
                time_basis=(SELECT time_basis FROM content_metric_field_facts
                            WHERE observation_id=? LIMIT 1),contract_version=?,hash_version='original-v1'
                WHERE id=?""",
                (provider, migrated["operation"] if provider != "legacy_unknown" else None,
                 data["id"], data["id"], POLICY_VERSION, data["id"]),
            )
        result["providers"][provider] = result["providers"].get(provider, 0) + 1
        lineage_hash.update(_json([data["id"], data["observation_sha256"],
                                  data["raw_id"], provider, migrated.get("operation")]).encode())
        lineage_hash.update(b"\n")
    result["source_mapping_sha256"] = lineage_hash.hexdigest()
    return result


def record_policy_transition(
    connection: sqlite3.Connection, *, policy_version: str, provider: str,
    operation: str, field: str, eligibility: str, priority: int,
    effective_at: str, recorded_at: str, contract_receipt_sha256: str,
    ttl_seconds: int | None = None,
) -> int:
    _require_transaction(connection)
    if policy_version != POLICY_VERSION and provider == "newrank_matrix" and eligibility == "active":
        raise ValueError("retired Matrix may only participate in historical policy reads")
    values = dict(policy_version=policy_version, provider=provider, operation=operation,
                  field=field, eligibility=eligibility, priority=priority,
                  ttl_seconds=ttl_seconds, effective_at=utc(effective_at),
                  recorded_at=utc(recorded_at), contract_receipt_sha256=contract_receipt_sha256)
    values["transition_sha256"] = _hash(values)
    return _insert(connection, "metric_policy_transitions", values, "transition_sha256")


def _streams() -> list[tuple[str, str]]:
    stages = load_policy()["provider_operation_stages"]
    return [(provider, operation) for provider, operations in stages.items()
            for operation in operations] + [("legacy_unknown", "legacy_unknown")]


def _fact_streams(
    connection: sqlite3.Connection, content_ids: list[int],
) -> list[tuple[str, str]]:
    """Probe each configured stream once before its per-field selectors.

    EXISTS uses the content/provider/operation index prefix and stops at one
    fact, so the work does not grow with observation history. Time, window,
    corrections and field eligibility remain the responsibility of the exact
    selectors: this only removes streams with no facts at any time.
    """
    streams = _streams()
    if not content_ids:
        return []
    rows = connection.execute(
        f"""WITH configured(provider,operation) AS (
            VALUES {','.join('(?,?)' for _ in streams)}
        )
        SELECT provider,operation FROM configured
        WHERE EXISTS (
            SELECT 1 FROM content_metric_field_facts f
            WHERE f.content_id IN ({','.join('?' for _ in content_ids)})
              AND f.provider=configured.provider AND f.operation=configured.operation
        )""",
        [value for stream in streams for value in stream] + content_ids,
    )
    present = {(str(row[0]), str(row[1])) for row in rows}
    return [stream for stream in streams if stream in present]


def _latest_fact(
    connection: sqlite3.Connection, content_id: int | list[int], provider: str, operation: str,
    field: str, cutoff: str, knowledge: str, *, provided: bool | None,
    window_key: str | None = None,
) -> dict[str, Any] | None:
    if isinstance(content_id, list):
        candidates = [_latest_fact(connection, member, provider, operation, field, cutoff, knowledge,
                                   provided=provided, window_key=window_key) for member in content_id]
        return max((row for row in candidates if row is not None),
                   key=lambda row: (row["captured_at"], row["recorded_at"], row["id"]), default=None)
    value_clause = (
        "AND ((x.action='replace') OR (x.id IS NULL AND f.state='provided'))"
        if provided else "AND (f.state NOT IN ('not_requested','audit_only') OR x.id IS NOT NULL)"
        if provided is False else ""
    )
    window_filter = "AND f.window_key=?" if window_key is not None else ""
    parameters: list[Any] = [knowledge, content_id, provider, operation, field, cutoff, knowledge]
    if window_key is not None:
        parameters.append(window_key)
    row = connection.execute(
        f"""SELECT f.*,x.id correction_id,x.action correction_action,x.value correction_value,
                   x.recorded_at correction_recorded_at
        FROM content_metric_field_facts f
        LEFT JOIN content_metric_corrections x ON x.id=(
          SELECT id FROM content_metric_corrections
          WHERE target_fact_id=f.id AND recorded_at<=? ORDER BY recorded_at DESC,id DESC LIMIT 1)
        WHERE f.content_id=? AND f.provider=? AND f.operation=? AND f.field=?
          AND f.captured_at<=? AND f.recorded_at<=? {value_clause} {window_filter}
        ORDER BY f.captured_at DESC,f.recorded_at DESC,f.id DESC LIMIT 1""",
        parameters,
    ).fetchone()
    if row is None:
        return None
    fact = dict(row)
    if fact["correction_action"]:
        fact["state"] = "provided" if fact["correction_action"] == "replace" else "invalid"
        fact["value"] = fact["correction_value"]
        fact["reason"] = "targeted_correction"
    return fact


def select_field_facts(
    connection: sqlite3.Connection, content_id: int, *, cutoff_at: str,
    knowledge_at: str | None = None, policy_version: str = POLICY_VERSION,
    window_key: str | None = None,
    resolve_aliases: bool = True,
) -> dict[str, Any]:
    """At most one latest event and one value per contracted source/field.

    Capture and knowledge cutoffs are separate; no arbitrary history LIMIT N
    can drop a corrected original. Missing/invalid events never renew TTL.
    """
    cutoff, knowledge = utc(cutoff_at), utc(knowledge_at or cutoff_at)
    scope_ids = [content_id]
    if resolve_aliases:
        content_id = resolve_metric_content_id(connection, content_id, knowledge_at=knowledge)
        scope_ids = metric_content_scope(connection, content_id, knowledge_at=knowledge)
    content = connection.execute(
        "SELECT platform,published_at FROM content_items WHERE id=?", (content_id,),
    ).fetchone()
    if content is None:
        raise ValueError("unknown content")
    default_ttl = metric_freshness_seconds(content["published_at"], as_of=cutoff)
    selected: dict[str, Any] = {}
    transitions: list[str] = []
    future_policy = connection.execute(
        """SELECT min(effective_at) FROM metric_policy_transitions
        WHERE policy_version=? AND effective_at>? AND recorded_at<=?""",
        (policy_version, cutoff, knowledge),
    ).fetchone()
    if future_policy is not None and future_policy[0] is not None:
        transitions.append(str(future_policy[0]))
    streams = _fact_streams(connection, scope_ids)
    for field in METRIC_FIELDS:
        inputs: list[dict[str, Any]] = []
        candidates: list[dict[str, Any]] = []
        latest_events: list[dict[str, Any]] = []
        for provider, operation in streams:
            latest = _latest_fact(connection, scope_ids, provider, operation, field,
                                  cutoff, knowledge, provided=False, window_key=window_key)
            value = _latest_fact(connection, scope_ids, provider, operation, field,
                                 cutoff, knowledge, provided=True, window_key=window_key)
            if latest is None and value is None:
                latest = _latest_fact(connection, scope_ids, provider, operation, field,
                                      cutoff, knowledge, provided=None, window_key=window_key)
                if latest is None:
                    continue
            if latest is not None:
                latest_events.append(latest)
            policy = connection.execute(
                """SELECT * FROM metric_policy_transitions WHERE policy_version=?
                AND provider=? AND operation=? AND field=? AND effective_at<=? AND recorded_at<=?
                ORDER BY effective_at DESC,id DESC LIMIT 1""",
                (policy_version, provider, operation, field, cutoff, knowledge),
            ).fetchone()
            eligibility = str(policy["eligibility"]) if policy is not None else (
                "active" if policy_version == POLICY_VERSION
                and provider in {"newrank_matrix", "tikhub"} else "historical_only"
            )
            priority = int(policy["priority"]) if policy is not None else (
                0 if provider == "newrank_matrix" else 1 if provider == "tikhub" else 2
            )
            availability_rows = []
            for member_id in scope_ids:
                availability_filter = ""
                availability_parameters: list[Any] = [member_id, provider, operation, cutoff, knowledge]
                if window_key is not None:
                    availability_filter = """AND observation_id IN (
                        SELECT id FROM content_metric_observations WHERE content_id=? AND window_key=?)"""
                    availability_parameters.extend([member_id, window_key])
                member_availability = connection.execute(
                    f"""SELECT * FROM content_availability_observations WHERE content_id=?
                    AND provider=? AND operation=? AND captured_at<=? AND recorded_at<=?
                    AND availability<>'unknown' AND reason<>'pending_unavailable_confirmation' {availability_filter}
                    ORDER BY captured_at DESC,id DESC LIMIT 1""", availability_parameters,
                ).fetchone()
                if member_availability is not None:
                    availability_rows.append(member_availability)
            availability = max(availability_rows, key=lambda row: (row["captured_at"], row["id"]), default=None)
            inputs.append({"provider": provider, "operation": operation,
                           "latest_fact_id": latest["id"] if latest else None,
                           "value_fact_id": value["id"] if value else None,
                           "correction_id": latest["correction_id"] if latest else None,
                           "value_correction_id": value["correction_id"] if value else None,
                           "policy_transition_id": policy["id"] if policy else None,
                           "availability_id": availability["id"] if availability else None})
            if value is None:
                continue
            if eligibility in {"audit_only", "invalid"}:
                if latest is not None:
                    latest.update(state=eligibility, value=None, reason="field_policy_ineligible")
                continue
            ttl = int(policy["ttl_seconds"] or default_ttl) if policy else default_ttl
            basis = value["provider_data_at"] or value["captured_at"]
            expires = utc((parse_time(basis) + timedelta(seconds=ttl)).isoformat())
            available = availability is None or availability["availability"] == "available"
            fresh = bool(
                eligibility == "active" and latest is not None and latest["id"] == value["id"]
                and latest["state"] == "provided" and available
                and value["observation_status"] != "stale" and cutoff < expires
            )
            candidates.append(dict(
                value, freshness="fresh" if fresh else "stale", expires_at=expires,
                priority=priority, eligibility=eligibility,
                latest_fact_id=latest["id"] if latest else None,
                latest_provider_status=latest["state"] if latest else None,
                availability_id=availability["id"] if availability else None,
                policy_transition_id=policy["id"] if policy else None,
            ))
        if policy_version == POLICY_VERSION:
            # Keep policy-v2's existing provider ranking and newest-requested
            # event semantics; policy-v3 qualifies operation/field separately.
            newest_by_provider: dict[str, dict[str, Any]] = {}
            for event in latest_events:
                if event["state"] in {"not_requested", "audit_only"}:
                    continue
                previous = newest_by_provider.get(event["provider"])
                if previous is None or (event["captured_at"], event["recorded_at"], event["id"]) > (
                    previous["captured_at"], previous["recorded_at"], previous["id"]
                ):
                    newest_by_provider[event["provider"]] = event
            for candidate in candidates:
                latest_provider = newest_by_provider.get(candidate["provider"])
                if latest_provider is not None:
                    candidate["latest_fact_id"] = latest_provider["id"]
                    candidate["latest_provider_status"] = latest_provider["state"]
                    if latest_provider["id"] != candidate["id"]:
                        candidate["freshness"] = "stale"
        fresh_candidates = [item for item in candidates if item["freshness"] == "fresh"]
        if fresh_candidates:
            chosen = min(fresh_candidates, key=lambda x: (
                x["priority"], -parse_time(x["captured_at"]).timestamp(), -x["id"],
            ))
        elif candidates:
            chosen = max(candidates, key=lambda x: (x["captured_at"], x["recorded_at"], x["id"]))
        elif latest_events:
            chosen = dict(max(latest_events, key=lambda x: (x["captured_at"], x["recorded_at"], x["id"])),
                          freshness="unknown", expires_at=None)
        else:
            chosen = {"id": None, "value": None, "state": "missing", "freshness": "unknown",
                      "reason": "no_valid_fact", "expires_at": None}
        if content["platform"] == "xiaohongshu" and field == "view_count":
            chosen = {"id": None, "value": None, "state": "not_applicable", "freshness": "unknown",
                      "reason": "xiaohongshu_exposure_unsupported", "expires_at": None}
        if chosen["freshness"] == "fresh":
            transitions.append(chosen["expires_at"])
            reason = "preferred_fresh_source" if chosen["provider"] == "newrank_matrix" else "fixed_fallback_source"
        elif chosen["state"] == "provided":
            reason = "historical_value_not_current"
        else:
            reason = str(chosen.get("reason") or "no_valid_fact")
        evidence = connection.execute(
            "SELECT id FROM provider_response_field_evidences WHERE fact_id=?", (chosen["id"],),
        ).fetchone() if chosen["id"] is not None else None
        selected[field] = dict(
            value=chosen["value"], status=chosen["state"], freshness=chosen["freshness"],
            reason=reason, selected_fact_id=chosen["id"],
            latest_fact_id=chosen.get("latest_fact_id"), correction_id=chosen.get("correction_id"),
            availability_id=chosen.get("availability_id"),
            policy_transition_id=chosen.get("policy_transition_id"),
            freshness_evidence_id=int(evidence[0]) if evidence else None,
            captured_at=chosen.get("captured_at"), recorded_at=chosen.get("recorded_at"),
            provider_data_at=chosen.get("provider_data_at"), expires_at=chosen.get("expires_at"),
            provider=chosen.get("provider"), operation=chosen.get("operation"),
            observation_id=chosen.get("observation_id"), raw_response_id=chosen.get("raw_response_id"),
            latest_provider_status=chosen.get("latest_provider_status"), inputs=inputs,
        )
    return {"content_id": content_id, "policy_version": policy_version,
            "cutoff_at": cutoff, "knowledge_at": knowledge, "window_key": window_key, "fields": selected,
            "scope_content_ids": scope_ids,
            "next_transition_at": min(transitions) if transitions else None}


def project_content(
    connection: sqlite3.Connection, content_id: int, *, cutoff_at: str,
    knowledge_at: str | None = None, policy_version: str = POLICY_VERSION,
    window_key: str | None = None,
    resolve_aliases: bool = True,
) -> dict[str, Any]:
    _require_transaction(connection)
    payload = select_field_facts(connection, content_id, cutoff_at=cutoff_at,
                                knowledge_at=knowledge_at, policy_version=policy_version, window_key=window_key,
                                resolve_aliases=resolve_aliases)
    content_id = payload["content_id"]
    payload["business_projection"] = business_projection(connection, payload)
    causal_hash = _hash(payload)
    existing = connection.execute(
        "SELECT id,payload_json FROM content_metric_projection_versions WHERE causal_set_hash=?",
        (causal_hash,),
    ).fetchone()
    if existing:
        return dict(json.loads(existing["payload_json"]), projection_id=int(existing["id"]))
    for field, fact in payload["fields"].items():
        ttl_id = None
        if fact["selected_fact_id"] is not None and fact["expires_at"] is not None:
            ttl = dict(fact_id=fact["selected_fact_id"], policy_version=policy_version,
                       expires_at=fact["expires_at"], recorded_at=payload["knowledge_at"])
            # The expiry event is stable across subsequent projections.
            ttl["event_sha256"] = _hash({k: v for k, v in ttl.items() if k != "recorded_at"})
            ttl_id = _insert(connection, "content_metric_ttl_transition_events", ttl, "event_sha256")
        connection.execute(
            """INSERT INTO content_metric_projection_causal_events(
            causal_set_hash,field,selected_fact_id,latest_fact_id,correction_id,availability_id,
            freshness_evidence_id,policy_transition_id,ttl_transition_id,inputs_json)
            VALUES (?,?,?,?,?,?,?,?,?,?)""",
            (causal_hash, field, fact["selected_fact_id"], fact["latest_fact_id"],
             fact["correction_id"], fact["availability_id"], fact["freshness_evidence_id"],
             fact["policy_transition_id"], ttl_id, _json(fact["inputs"])),
        )
    values = dict(content_id=content_id, policy_version=policy_version, window_key=window_key,
                  cutoff_at=payload["cutoff_at"], knowledge_at=payload["knowledge_at"],
                  next_transition_at=payload["next_transition_at"], payload_json=_json(payload),
                  causal_set_hash=causal_hash)
    projection_id = _insert(connection, "content_metric_projection_versions", values, "causal_set_hash")
    return dict(payload, projection_id=projection_id)


def business_projection(
    connection: sqlite3.Connection, payload: dict[str, Any], *, metric_fields: tuple[str, ...] = METRIC_FIELDS,
) -> dict[str, Any] | None:
    """Preserve the existing API/report selector shape using bounded fact IDs."""
    fact_ids: set[int] = set()
    correction_ids: set[int] = set()
    for field in payload["fields"].values():
        if field["selected_fact_id"] is not None:
            fact_ids.add(field["selected_fact_id"])
        if field["correction_id"] is not None:
            correction_ids.add(field["correction_id"])
        for item in field["inputs"]:
            fact_ids.update(item[key] for key in ("latest_fact_id", "value_fact_id") if item[key] is not None)
            correction_ids.update(item[key] for key in ("correction_id", "value_correction_id") if item[key] is not None)
    if not fact_ids:
        return None
    facts = {int(row["id"]): dict(row) for row in connection.execute(
        f"SELECT * FROM content_metric_field_facts WHERE id IN ({','.join('?' for _ in fact_ids)})", sorted(fact_ids),
    )}
    observation_ids = {int(fact["observation_id"]) for fact in facts.values()}
    corrections = {int(row["id"]): dict(row) for row in connection.execute(
        f"SELECT * FROM content_metric_corrections WHERE id IN ({','.join('?' for _ in correction_ids)})",
        sorted(correction_ids),
    )} if correction_ids else {}
    observation_ids.update(int(item["source_observation_id"]) for item in corrections.values()
                           if item["source_observation_id"] is not None)
    observations = {int(row["id"]): dict(row) for row in connection.execute(
        observation_query(connection) + f" WHERE o.id IN ({','.join('?' for _ in observation_ids)})",
        sorted(observation_ids),
    )}
    for original_evidence in observations.values():
        # Source rows/hashes retain their precision; selector evidence is a copy.
        original_evidence["captured_at"] = utc(original_evidence["captured_at"])
        original_evidence["recorded_at"] = utc(original_evidence["recorded_at"])
    for correction in corrections.values():
        if correction["source_observation_id"] is not None:
            continue
        target = facts[correction["target_fact_id"]]
        synthetic = dict(observations[target["observation_id"]])
        synthetic.update(id=-correction["id"], recorded_at=correction["recorded_at"],
                         observation_origin="system_correction", observation_sha256=correction["correction_sha256"])
        observations[synthetic["id"]] = synthetic
    ordered = sorted(observations.values(), key=lambda item: (
        parse_time(item["captured_at"]), parse_time(item["recorded_at"]), item["id"],
    ), reverse=True)
    newest = ordered[0]
    platform = str(newest["platform"])
    selected: dict[str, Any] = {}
    for name, field in payload["fields"].items():
        if name not in metric_fields:
            continue
        source_fact = facts.get(field["selected_fact_id"])
        observation = observations.get(source_fact["observation_id"]) if source_fact else None
        field_correction = corrections.get(field["correction_id"])
        if field_correction is not None:
            observation = observations[field_correction["source_observation_id"] or -field_correction["id"]]
        evidence = _field_evidence(observation, name, platform) if observation else {
            "observation_id": None, "raw_response_id": None, "captured_at": None,
            "recorded_at": None, "observation_sha256": None,
        }
        latest_valid = bool(field["status"] == "provided" and field["latest_fact_id"] == field["selected_fact_id"]
                            and field["provider"] in {"newrank_matrix", "tikhub"}
                            and source_fact is not None and source_fact["observation_status"] != "stale")
        evidence.update(
            value=field["value"], status=field["status"], freshness=field["freshness"], reason=field["reason"],
            effective_provider=field["provider"], effective_operation=field["operation"],
            policy_version=payload["policy_version"], is_latest_valid=latest_valid,
            latest_provider_status=field["latest_provider_status"],
            selected_fact_id=field["selected_fact_id"], correction_id=field["correction_id"],
            provider_data_at=field["provider_data_at"], freshness_evidence_id=field["freshness_evidence_id"],
            freshness_reason=("within_refresh_cycle" if field["freshness"] == "fresh"
                              else "refresh_cycle_expired" if latest_valid
                              else "legacy_or_unknown_provider" if field["provider"] not in {"newrank_matrix", "tikhub"}
                              else "newer_source_missing_or_invalid" if field["latest_provider_status"] in {"missing", "invalid"}
                              else "not_current_valid_fact"),
        )
        selected[name] = evidence
    provided = [field for field in selected.values() if field["status"] == "provided"]
    fresh = [field for field in provided if field["freshness"] == "fresh"]
    exposure = selected["view_count"]
    status = ("available" if exposure["freshness"] == "fresh" else "stale" if exposure["status"] == "provided" else "missing") if platform == "douyin" else (
        "available" if fresh else "stale" if provided else "missing")
    anchor = exposure if exposure["status"] == "provided" else provided[0] if provided else _field_evidence(newest, "view_count", platform)
    anchor_row = observations.get(anchor["observation_id"], newest)
    metadata = _observation_metadata(newest).copy()
    metadata.update(policy_version=payload["policy_version"], fields=selected)
    raw_ids = {field["raw_response_id"] for field in provided}
    scope_ids = payload.get("scope_content_ids", [payload["content_id"]])
    baseline = connection.execute(
        f"""SELECT o.legacy_snapshot_id FROM content_metric_field_facts f
        JOIN content_metric_observations o ON o.id=f.observation_id
        WHERE f.content_id IN ({','.join('?' for _ in scope_ids)})
          AND f.window_key=? AND f.observation_origin='legacy_snapshot_baseline'
          AND f.field='view_count' AND f.captured_at<=? AND f.recorded_at<=?
        ORDER BY f.captured_at DESC,f.recorded_at DESC,f.id DESC LIMIT 1""",
        (*scope_ids, anchor_row["window_key"], payload["cutoff_at"], payload["knowledge_at"]),
    ).fetchone()
    result = dict(
        id=anchor["observation_id"], observation_id=anchor["observation_id"], content_id=payload["content_id"],
        platform=platform, source=platform, status=status,
        captured_at=min((field["captured_at"] for field in provided), key=parse_time, default=newest["captured_at"]),
        recorded_at=max((field["recorded_at"] for field in provided), key=parse_time, default=newest["recorded_at"]),
        window_key=anchor_row["window_key"], raw_response_id=next(iter(raw_ids)) if len(raw_ids) == 1 else None,
        fields=selected, metadata_json=_json(metadata), observation_origin=anchor_row["observation_origin"],
        observation_sha256=anchor_row["observation_sha256"], policy_version=payload["policy_version"],
        legacy_snapshot_id=baseline[0] if baseline else None,
    )
    result.update({name: field["value"] for name, field in selected.items()})
    return result


def read_projection(
    connection: sqlite3.Connection, content_id: int, *, knowledge_at: str,
    policy_version: str = POLICY_VERSION, cutoff_at: str | None = None,
    window_key: str | None = None,
) -> dict[str, Any] | None:
    """Read one indexed version; return None when a TTL transition is due."""
    knowledge, cutoff = utc(knowledge_at), utc(cutoff_at or knowledge_at)
    content_id = resolve_metric_content_id(connection, content_id, knowledge_at=knowledge)
    scope_ids = metric_content_scope(connection, content_id, knowledge_at=knowledge)
    row = connection.execute(
        """SELECT * FROM content_metric_projection_versions
        WHERE content_id=? AND policy_version=? AND window_key IS ? AND knowledge_at<=? AND cutoff_at<=?
        ORDER BY knowledge_at DESC,id DESC LIMIT 1""",
        (content_id, policy_version, window_key, knowledge, cutoff),
    ).fetchone()
    if row is None or (row["next_transition_at"] is not None and row["next_transition_at"] <= cutoff):
        return None
    saved = json.loads(row["payload_json"])
    if saved.get("scope_content_ids", [content_id]) != scope_ids:
        return None
    # A prior projection cannot hide a late arrival, targeted correction or
    # newly known policy. These probes are indexed and do not materialize rows.
    for column, previous, upper, other, other_upper in (
        ("recorded_at", row["knowledge_at"], knowledge, "captured_at", cutoff),
        ("captured_at", row["cutoff_at"], cutoff, "recorded_at", knowledge),
    ):
        if upper <= previous:
            continue
        for member_id in scope_ids:
            if connection.execute(
                f"""SELECT 1 FROM content_metric_field_facts WHERE content_id=?
                AND {column}>? AND {column}<=? AND {other}<=? LIMIT 1""",
                (member_id, previous, upper, other_upper),
            ).fetchone() is not None:
                return None
    if connection.execute(
        f"""SELECT 1 FROM content_metric_corrections x JOIN content_metric_field_facts f ON f.id=x.target_fact_id
        WHERE x.recorded_at>? AND x.recorded_at<=? AND f.content_id IN ({','.join('?' for _ in scope_ids)})
        AND f.captured_at<=? AND f.recorded_at<=? LIMIT 1""",
        (row["knowledge_at"], knowledge, *scope_ids, cutoff, knowledge),
    ).fetchone() is not None:
        return None
    if connection.execute(
        """SELECT 1 FROM metric_policy_transitions WHERE policy_version=? AND recorded_at>?
        AND recorded_at<=? AND effective_at<=? LIMIT 1""",
        (policy_version, row["knowledge_at"], knowledge, cutoff),
    ).fetchone() is not None:
        return None
    return dict(saved, projection_id=int(row["id"]))


def select_metric_projections(
    connection: sqlite3.Connection, content_ids: list[int], *, cutoff_at: str,
    knowledge_at: str, window_key: str | None, metric_fields: tuple[str, ...],
    current_read: bool = False,
) -> dict[int, dict[str, Any]]:
    """Schema20 read path; recomputation stays read-only, including expired TTL."""
    cutoff_at, knowledge_at = utc(cutoff_at), utc(knowledge_at)
    owns_snapshot = not connection.in_transaction
    if owns_snapshot:
        connection.execute("BEGIN")
    result: dict[int, dict[str, Any]] = {}
    try:
        for requested_id in content_ids:
            content_id = resolve_metric_content_id(connection, requested_id, knowledge_at=knowledge_at)
            if connection.execute("SELECT 1 FROM content_items WHERE id=?", (content_id,)).fetchone() is None:
                continue
            payload = read_projection(connection, content_id, knowledge_at=knowledge_at,
                                      cutoff_at=cutoff_at, window_key=window_key)
            if payload is None:
                payload = select_field_facts(connection, content_id, cutoff_at=cutoff_at,
                                             knowledge_at=knowledge_at, window_key=window_key)
            projection = (
                payload.get("business_projection")
                if len(metric_fields) == len(METRIC_FIELDS) else None
            )
            if projection is None:
                projection = business_projection(connection, payload, metric_fields=metric_fields)
            if projection is None:
                if current_read:
                    parameters: list[Any] = [content_id, cutoff_at]
                    window_filter = ""
                    if window_key is not None:
                        window_filter = "AND s.window_key=?"
                        parameters.append(window_key)
                    legacy = connection.execute(
                        f"""SELECT s.* FROM content_metric_snapshots s WHERE s.content_id=?
                        AND julianday(s.captured_at)<=julianday(?) {window_filter}
                        AND NOT EXISTS(SELECT 1 FROM content_metric_observations o WHERE o.content_id=s.content_id)
                        ORDER BY s.captured_at DESC,s.id DESC LIMIT 1""", parameters,
                    ).fetchone()
                    if legacy is not None:
                        fact = dict(legacy)
                        fact.update(id=None, recorded_at=fact["captured_at"], status="stale",
                                    source="legacy_unknown", raw_id=None,
                                    observation_origin="snapshot_only_legacy_unknown")
                        content = dict(connection.execute(
                            "SELECT id,platform,published_at,account_id FROM content_items WHERE id=?", (content_id,),
                        ).fetchone())
                        projection = _select_row(content, [fact], cutoff_at=cutoff_at, metric_fields=metric_fields)
                        if projection is not None:
                            result[requested_id] = projection
                continue
            result[requested_id] = projection
        return result
    finally:
        if owns_snapshot:
            connection.rollback()
