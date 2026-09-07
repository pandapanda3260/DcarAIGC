"""Offline, transactional schema19 -> 20 migration.

Historical payloads and their hashes are copied unchanged.  This module does
not install a database, activate a route, issue a paid permit or mark a release
accepted.  Those are separate writer-owned operations after clone validation.
"""
from __future__ import annotations

import hashlib
import json
import re
import sqlite3
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

MIGRATION_NAME = "integrated-video-capture-v25"
SCHEMA_VERSION = 20
REBUILT_TABLES = (
    "scheduler_runs", "scheduler_run_attempts", "fetch_attempts",
    "provider_raw_responses", "content_metric_observations", "content_identities",
    "acquisition_profile_activations", "activation_cancellations",
    "scan_verification_receipts", "profile_day_coverage_receipts",
    "pipeline_paid_drain_events",
)
_PROFILE_SET = "'matrix_hybrid_v1','tikhub_managed_v1'"


def _abort_control_trigger(name: str, original: str) -> str:
    """Only the v20 abort transition differs from the v19 immutable chain."""
    if name == "trg_activation_cancellations_contract":
        return """
CREATE TRIGGER trg_activation_cancellations_contract BEFORE INSERT ON activation_cancellations
WHEN NOT EXISTS (
 SELECT 1 FROM acquisition_profile_activations a WHERE a.id=NEW.activation_id AND (
  (NEW.cancellation_kind='pre_effective' AND NEW.cancelled_at<a.effective_at)
  OR (NEW.cancellation_kind='never_eligible_cleanup'
   AND NEW.contract_version='acquisition-profile-cancellation-v2'
   AND NEW.cancelled_at>=a.effective_at
   AND json_extract(a.metadata_json,'$.eligibility_contract')='activation-eligibility-v1'
   AND json_extract(NEW.metadata_json,'$.eligibility.required')=1
   AND json_extract(NEW.metadata_json,'$.eligibility.eligible')=0
   AND NOT EXISTS (SELECT 1 FROM paid_provider_dispatch_events d
    WHERE d.activation_id=a.id AND d.event_type='send_marked')
   AND NOT EXISTS (SELECT 1 FROM pipeline_paid_drain_events d
    WHERE d.target_activation_id=a.id AND d.event_type='release' AND d.created_at<a.effective_at)
  )))
BEGIN SELECT RAISE(ABORT, 'activation cancellation eligibility is invalid'); END
"""
    result = original.replace("AND NOT EXISTS (\n          SELECT 1 FROM activation_cancellations c",
        "AND (NEW.event_type='ABORT_RESTORE' OR NOT EXISTS (\n          SELECT 1 FROM activation_cancellations c")
    result = result.replace("julianday(c.cancelled_at)<=julianday(NEW.created_at)\n      )",
                            "julianday(c.cancelled_at)<=julianday(NEW.created_at)\n      ))")
    result = result.replace("AND event_type<>'release'", "AND event_type NOT IN ('release','ABORT_RESTORE')")
    guard = """
OR (NEW.event_type='ABORT_RESTORE' AND (
 NEW.bridge_run_id IS NOT NULL OR NEW.contract_version<>'pipeline-paid-drain-v2'
 OR NOT EXISTS (SELECT 1 FROM pipeline_paid_drain_events p
  WHERE p.id=NEW.previous_event_id AND p.drain_id=NEW.drain_id
   AND p.target_activation_id=NEW.target_activation_id AND p.event_type IN ('start','sealed','release'))
 OR NOT EXISTS (SELECT 1 FROM activation_cancellations c JOIN scheduler_runs r
  ON r.id=json_extract(c.metadata_json,'$.abort_fence_run_id')
  WHERE c.id=json_extract(NEW.payload_json,'$.cancellation_id')
   AND c.activation_id=NEW.target_activation_id
   AND c.cancellation_sha256=json_extract(NEW.payload_json,'$.cancellation_sha256')
   AND c.contract_version='acquisition-profile-cancellation-v2'
   AND r.status='succeeded'
   AND json_extract(r.details_json,'$.self_sha256')=json_extract(NEW.payload_json,'$.fence_sha256')
   AND json_extract(c.metadata_json,'$.abort_fence_sha256')=json_extract(NEW.payload_json,'$.fence_sha256')
   AND r.id=json_extract(NEW.payload_json,'$.fence_run_id'))
 OR json_extract(NEW.payload_json,'$.result') NOT IN ('open','closed')
))
"""
    return result.replace("BEGIN\n    SELECT RAISE", guard + "BEGIN\n    SELECT RAISE")

TRANSPORT_SQL = """
CREATE TABLE fetch_transport_receipts (
 id INTEGER PRIMARY KEY AUTOINCREMENT,
 fetch_attempt_id INTEGER NOT NULL UNIQUE REFERENCES fetch_attempts(id) ON DELETE RESTRICT,
 request_started_at TEXT NOT NULL, headers_received_at TEXT, response_finished_at TEXT NOT NULL,
 content_encoding TEXT, content_length INTEGER CHECK(content_length IS NULL OR content_length>=0),
 http_encoded_bytes INTEGER NOT NULL CHECK(http_encoded_bytes>=0), entity_bytes INTEGER CHECK(entity_bytes IS NULL OR entity_bytes>=0),
 stored_bytes INTEGER CHECK(stored_bytes IS NULL OR stored_bytes>=0),
 encoded_sha256 TEXT NOT NULL, entity_sha256 TEXT, stored_sha256 TEXT,
 clean_eof INTEGER NOT NULL CHECK(clean_eof IN (0,1)), length_match INTEGER CHECK(length_match IN (0,1)),
 gzip_crc_ok INTEGER CHECK(gzip_crc_ok IN (0,1)), json_parse_ok INTEGER NOT NULL CHECK(json_parse_ok IN (0,1)),
 error_class TEXT, route_id TEXT NOT NULL, payload_json TEXT NOT NULL CHECK(json_valid(payload_json)),
 receipt_sha256 TEXT NOT NULL UNIQUE
);
CREATE TABLE transport_quarantine_members (
 id INTEGER PRIMARY KEY AUTOINCREMENT,
 transport_receipt_id INTEGER NOT NULL UNIQUE REFERENCES fetch_transport_receipts(id) ON DELETE RESTRICT,
 path TEXT, byte_size INTEGER NOT NULL CHECK(byte_size>=0), sha256 TEXT NOT NULL,
 CHECK((byte_size=0) OR path IS NOT NULL)
);
CREATE TABLE transport_continuity_permits (
 id INTEGER PRIMARY KEY AUTOINCREMENT, provider TEXT NOT NULL, operation TEXT NOT NULL,
 qualification_sha256 TEXT NOT NULL, build_sha256 TEXT NOT NULL, config_sha256 TEXT NOT NULL,
 start_high_watermark INTEGER NOT NULL, max_starts INTEGER NOT NULL CHECK(max_starts=20),
 expires_at TEXT NOT NULL, created_at TEXT NOT NULL,
 payload_json TEXT NOT NULL CHECK(json_valid(payload_json)), permit_sha256 TEXT NOT NULL UNIQUE
);
CREATE TABLE transport_continuity_permit_members (
 permit_id INTEGER NOT NULL REFERENCES transport_continuity_permits(id) ON DELETE RESTRICT,
 rank INTEGER NOT NULL CHECK(rank BETWEEN 1 AND 20), request_scope_identity TEXT NOT NULL UNIQUE,
 PRIMARY KEY(permit_id,rank)
);
"""

_EXTRA_COLUMNS = {
    "scheduler_runs": """
        root_run_id INTEGER REFERENCES scheduler_runs(id) ON DELETE RESTRICT,
        continuation_sequence INTEGER CHECK(continuation_sequence>=1), charge_business_day TEXT,
        CHECK((root_run_id IS NULL AND continuation_sequence IS NULL AND charge_business_day IS NULL)
           OR (root_run_id IS NOT NULL AND continuation_sequence IS NOT NULL AND charge_business_day IS NOT NULL
               AND length(charge_business_day)=10))""",
    "scheduler_run_attempts": """
        owner_token TEXT, heartbeat_at TEXT, lease_expires_at TEXT,
        CHECK((heartbeat_at IS NULL AND lease_expires_at IS NULL)
           OR (owner_token IS NOT NULL AND heartbeat_at IS NOT NULL AND lease_expires_at>heartbeat_at))""",
    "fetch_attempts": """
        request_batch_id INTEGER REFERENCES fetch_request_batches(id) ON DELETE RESTRICT,
        CHECK((slot_id IS NOT NULL) <> (request_batch_id IS NOT NULL))""",
    "provider_raw_responses": """
        paid_scope_identity TEXT, sequence INTEGER NOT NULL DEFAULT 0 CHECK(sequence BETWEEN 0 AND 4),
        raw_blob_id INTEGER REFERENCES provider_raw_blobs(id) ON DELETE RESTRICT,
        raw_stored_at TEXT, transport_receipt_id INTEGER UNIQUE REFERENCES fetch_transport_receipts(id) ON DELETE RESTRICT""",
    "content_metric_observations": """
        effective_provider TEXT NOT NULL DEFAULT 'legacy_unknown', provider_operation TEXT,
        provider_data_at TEXT, time_basis TEXT NOT NULL DEFAULT 'unknown',
        contract_version TEXT NOT NULL DEFAULT 'legacy-v1', hash_version TEXT NOT NULL DEFAULT 'original-v1'""",
    "content_identities": """
        CHECK((length(created_at)=20 AND created_at GLOB '????-??-??T??:??:??Z')
           OR (length(created_at)=27 AND created_at GLOB '????-??-??T??:??:??.??????Z')),
        CHECK(julianday(created_at) IS NOT NULL)""",
    "activation_cancellations": """
        cancellation_kind TEXT NOT NULL DEFAULT 'pre_effective'
          CHECK(cancellation_kind IN ('pre_effective','never_eligible_cleanup'))""",
}

INDEX_SQL = """
CREATE UNIQUE INDEX uq_scheduler_root_slot ON scheduler_runs(job_id,scheduled_for) WHERE root_run_id IS NULL;
CREATE UNIQUE INDEX uq_scheduler_child_slot ON scheduler_runs(root_run_id,continuation_sequence,charge_business_day) WHERE root_run_id IS NOT NULL;
CREATE UNIQUE INDEX uq_raw_response_paid_scope ON provider_raw_responses(paid_scope_identity,sequence) WHERE paid_scope_identity IS NOT NULL;
CREATE UNIQUE INDEX uq_fetch_batch_attempt ON fetch_attempts(request_batch_id,attempt_number) WHERE request_batch_id IS NOT NULL;
CREATE TRIGGER trg_scheduler_child_root BEFORE INSERT ON scheduler_runs
WHEN NEW.root_run_id IS NOT NULL AND NOT EXISTS
 (SELECT 1 FROM scheduler_runs WHERE id=NEW.root_run_id AND root_run_id IS NULL)
BEGIN SELECT RAISE(ABORT,'scheduler child must reference root'); END;
CREATE TRIGGER trg_scheduler_run_identity BEFORE UPDATE OF job_id,scheduled_for,root_run_id,continuation_sequence,charge_business_day ON scheduler_runs
BEGIN SELECT RAISE(ABORT,'scheduler durable identity is immutable'); END;
CREATE TRIGGER trg_content_identity_value BEFORE UPDATE OF identity_kind,identity_value,platform_identity_key,created_at ON content_identities
BEGIN SELECT RAISE(ABORT,'content identity value and first visibility are immutable'); END;
CREATE TRIGGER trg_metric_observation_source_immutable BEFORE UPDATE OF effective_provider,provider_operation,provider_data_at,time_basis,contract_version,hash_version ON content_metric_observations
BEGIN SELECT RAISE(ABORT,'metric observation provenance is immutable'); END;
"""

ATTEMPT_UPDATE_TRIGGER = """
CREATE TRIGGER trg_scheduler_run_attempts_terminal_update
BEFORE UPDATE ON scheduler_run_attempts
WHEN NOT (
 OLD.status='running' AND OLD.completed_at IS NULL
 AND NEW.id IS OLD.id AND NEW.scheduler_run_id IS OLD.scheduler_run_id
 AND NEW.attempt_number IS OLD.attempt_number AND NEW.invocation_source IS OLD.invocation_source
 AND NEW.started_at IS OLD.started_at AND NEW.owner_token IS OLD.owner_token
 AND (
   (NEW.status IN ('succeeded','failed','skipped','partial','interrupted') AND NEW.completed_at IS NOT NULL
    AND NEW.heartbeat_at IS OLD.heartbeat_at AND NEW.lease_expires_at IS OLD.lease_expires_at)
   OR
   (NEW.status='running' AND NEW.completed_at IS NULL AND NEW.details_json IS OLD.details_json
    AND OLD.owner_token IS NOT NULL AND NEW.heartbeat_at IS NOT NULL
    AND (OLD.heartbeat_at IS NULL OR NEW.heartbeat_at>=OLD.heartbeat_at)
    AND NEW.lease_expires_at>NEW.heartbeat_at
    AND (OLD.lease_expires_at IS NULL OR NEW.lease_expires_at>=OLD.lease_expires_at))
 ))
BEGIN SELECT RAISE(ABORT,'scheduler attempt permits monotonic heartbeat or one terminal update'); END;
"""


def statements(sql: str) -> list[str]:
    result: list[str] = []
    pending = ""
    for line in sql.splitlines(keepends=True):
        pending += line
        if sqlite3.complete_statement(pending):
            result.append(pending.strip())
            pending = ""
    if pending.strip():
        raise ValueError("incomplete migration SQL")
    return result


def _columns(connection: sqlite3.Connection, table: str) -> list[str]:
    return [str(row[1]) for row in connection.execute(f'PRAGMA table_info("{table}")')]


def row_digest(connection: sqlite3.Connection, table: str, columns: list[str]) -> dict[str, Any]:
    """Streaming digest of the original columns; added provenance is separate."""
    quote = ",".join('"' + column + '"' for column in columns)
    info = connection.execute(f'PRAGMA table_info("{table}")').fetchall()
    pk = [str(row[1]) for row in sorted(info, key=lambda row: row[5]) if row[5]]
    order = ",".join('"' + column + '"' for column in pk) if pk else "rowid"
    digest = hashlib.sha256()
    count = 0
    for row in connection.execute(f'SELECT {quote} FROM "{table}" ORDER BY {order}'):
        values = [value.hex() if isinstance(value, bytes) else value for value in row]
        digest.update(json.dumps(values, ensure_ascii=False, separators=(",", ":"), allow_nan=False).encode())
        digest.update(b"\n")
        count += 1
    return {"count": count, "sha256": digest.hexdigest(), "columns": columns}


def _replace_once(sql: str, old: str, new: str) -> str:
    if sql.count(old) != 1:
        raise ValueError("migration source DDL differs: " + old)
    return sql.replace(old, new, 1)


def rebuilt_sql(table: str, old_sql: str) -> str:
    sql = re.sub(r'^(CREATE TABLE(?: IF NOT EXISTS)?)\s+"?' + re.escape(table) + r'"?',
                 r'\1 "' + table + '__v20"', old_sql, count=1)
    if table == "scheduler_runs":
        sql = _replace_once(sql, ",\n    UNIQUE(job_id, scheduled_for)", "")
    elif table == "fetch_attempts":
        sql = _replace_once(sql, "slot_id INTEGER NOT NULL", "slot_id INTEGER")
    elif table == "provider_raw_responses":
        sql = _replace_once(sql, ",\n    UNIQUE(content_id, provider, operation, local_path, sha256)", "")
    elif table in {"acquisition_profile_activations", "scan_verification_receipts", "profile_day_coverage_receipts"}:
        sql = sql.replace(_PROFILE_SET, _PROFILE_SET + ",'integrated_route_v1'")
    elif table == "pipeline_paid_drain_events":
        sql = _replace_once(sql, "sequence BETWEEN 1 AND 3", "sequence BETWEEN 1 AND 4")
        sql = _replace_once(sql, "event_type IN ('start','sealed','release')",
                            "event_type IN ('start','sealed','release','ABORT_RESTORE')")
        sql = _replace_once(sql, "OR (event_type='release' AND sequence=3)",
                            "OR (event_type='release' AND sequence=3)\n        OR (event_type='ABORT_RESTORE' AND sequence=4)")
    if table in _EXTRA_COLUMNS:
        constraint = re.search(r"(?m)^[ \t]{4}(?:UNIQUE|CHECK|FOREIGN KEY)\s*\(", sql)
        if constraint:
            offset = constraint.start()
            sql = sql[:offset] + _EXTRA_COLUMNS[table] + ",\n" + sql[offset:]
        else:
            offset = sql.rfind(")")
            sql = sql[:offset] + ",\n" + _EXTRA_COLUMNS[table] + "\n" + sql[offset:]
    return sql


def _immutable_triggers(connection: sqlite3.Connection, tables: list[str]) -> None:
    mutable = {"capture_work_items", "admission_reservations", "operational_alerts",
               "provider_raw_blobs", "raw_archives"}
    for table in tables:
        if table in mutable:
            continue
        for action in ("UPDATE", "DELETE"):
            name = f"trg_v20_{table}_no_{action.lower()}"
            connection.execute(f'CREATE TRIGGER IF NOT EXISTS "{name}" BEFORE {action} ON "{table}" '
                               "BEGIN SELECT RAISE(ABORT,'capture evidence is append-only'); END")


def migrate(connection: sqlite3.Connection, *, legacy_project_root: Path | None = None,
            migration_blob_root: Path | None = None) -> dict[str, Any]:
    from . import capture_planning, metric_field_facts, raw_archive, usage_settlements
    from .storage import _require_non_formal_connection
    from .schema_v19 import validate_structure as validate_v19

    _require_non_formal_connection(connection)
    if connection.in_transaction:
        raise ValueError("schema20 migration requires no active transaction")
    if connection.execute("PRAGMA user_version").fetchone()[0] != 19:
        raise ValueError("schema20 source must be exactly schema19")
    validate_v19(connection)
    if connection.execute("PRAGMA foreign_key_check").fetchone():
        raise ValueError("schema19 source has foreign key violations")
    all_tables = [str(row[0]) for row in connection.execute(
        "SELECT name FROM sqlite_master WHERE type='table' AND name NOT LIKE 'sqlite_%' ORDER BY name")]
    originals = {table: row_digest(connection, table, _columns(connection, table)) for table in all_tables}
    sequences = {str(row[0]): int(row[1]) for row in connection.execute("SELECT name,seq FROM sqlite_sequence")}
    definitions = {table: connection.execute("SELECT sql FROM sqlite_master WHERE type='table' AND name=?", (table,)).fetchone()[0]
                   for table in REBUILT_TABLES}
    objects = list(connection.execute(
        "SELECT type,name,tbl_name,sql FROM sqlite_master WHERE type IN ('index','trigger') AND sql IS NOT NULL"))
    fk = connection.execute("PRAGMA foreign_keys").fetchone()[0]
    legacy = connection.execute("PRAGMA legacy_alter_table").fetchone()[0]
    connection.execute("PRAGMA foreign_keys=OFF")
    connection.execute("PRAGMA legacy_alter_table=ON")
    try:
        connection.execute("BEGIN IMMEDIATE")
        for fragment in (capture_planning.SCHEMA_SQL, TRANSPORT_SQL, metric_field_facts.SCHEMA_SQL,
                         raw_archive.SCHEMA_SQL, usage_settlements.SCHEMA_SQL):
            for statement in statements(fragment):
                connection.execute(statement)
        for table in REBUILT_TABLES:
            connection.execute(rebuilt_sql(table, definitions[table]))
            columns = ','.join('"' + column + '"' for column in originals[table]["columns"])
            connection.execute(f'INSERT INTO "{table}__v20" ({columns}) SELECT {columns} FROM "{table}"')
        for obj in objects:
            if obj[0] == "trigger" and obj[2] in REBUILT_TABLES:
                connection.execute(f'DROP TRIGGER "{obj[1]}"')
        for table in reversed(REBUILT_TABLES):
            connection.execute(f'DROP TABLE "{table}"')
        for table in REBUILT_TABLES:
            connection.execute(f'ALTER TABLE "{table}__v20" RENAME TO "{table}"')
        connection.execute("UPDATE provider_raw_responses SET paid_scope_identity='legacy:raw:'||id")
        billing = usage_settlements.migrate_legacy(connection)
        metrics = metric_field_facts.migrate_legacy(connection)
        raw = raw_archive.migrate_legacy(connection, legacy_project_root=legacy_project_root or raw_archive.PROJECT_ROOT,
                                         migration_blob_root=migration_blob_root)
        routes = capture_planning.seed_legacy_routes(
            connection, at=datetime.now(timezone.utc).isoformat())
        for obj in objects:
            if obj[2] not in REBUILT_TABLES:
                continue
            if obj[1] == "trg_scheduler_run_attempts_terminal_update":
                connection.execute(ATTEMPT_UPDATE_TRIGGER)
            elif obj[1] in {"trg_activation_cancellations_contract", "trg_paid_drain_events_binding"}:
                connection.execute(_abort_control_trigger(str(obj[1]), str(obj[3])))
            elif obj[1] == "trg_profile_activations_roster":
                connection.execute(str(obj[3]).replace(
                    "WHEN 'tikhub_managed_v1' THEN 'system'",
                    "WHEN 'tikhub_managed_v1' THEN 'system' WHEN 'integrated_route_v1' THEN 'system'"))
            else:
                connection.execute(obj[3])
        for statement in statements(INDEX_SQL):
            connection.execute(statement)
        for table, original in originals.items():
            copied = row_digest(connection, table, original["columns"])
            if copied != original:
                raise ValueError("migration changed historical rows: " + table)
        for name, sequence in sequences.items():
            current = connection.execute("SELECT seq FROM sqlite_sequence WHERE name=?", (name,)).fetchone()
            if current is None:
                connection.execute("INSERT INTO sqlite_sequence(name,seq) VALUES (?,?)", (name, sequence))
            elif int(current[0]) < sequence:
                connection.execute("UPDATE sqlite_sequence SET seq=? WHERE name=?", (sequence, name))
        new_tables = [str(row[0]) for row in connection.execute(
            "SELECT name FROM sqlite_master WHERE type='table' AND name NOT LIKE 'sqlite_%'") if row[0] not in all_tables]
        _immutable_triggers(connection, new_tables)
        connection.execute("INSERT INTO schema_migrations(version,name,applied_at) VALUES (20,?,?)",
                           (MIGRATION_NAME, datetime.now(timezone.utc).isoformat(timespec="seconds").replace("+00:00", "Z")))
        connection.execute("PRAGMA user_version=20")
        violations = connection.execute("PRAGMA foreign_key_check").fetchall()
        if violations:
            raise ValueError("schema20 foreign key violations: " + str(violations[:5]))
        if connection.execute("PRAGMA quick_check").fetchone()[0] != "ok":
            raise ValueError("schema20 quick_check failed")
        connection.commit()
    except BaseException:
        connection.rollback()
        raise
    finally:
        connection.execute(f"PRAGMA foreign_keys={int(fk)}")
        connection.execute(f"PRAGMA legacy_alter_table={int(legacy)}")
    validate_structure(connection)
    return {"contract_version": "schema20-candidate-migration-v1", "status": "candidate",
            "source_version": 19, "target_version": 20, "preserved_tables": originals,
            "preserved_sequences": sequences, "billing": billing, "metrics": metrics, "raw": raw,
            "routes": routes, "new_tables": sorted(new_tables), "provider_calls": 0, "production_acceptance": False}


def validate_structure(connection: sqlite3.Connection) -> None:
    if connection.execute("PRAGMA user_version").fetchone()[0] != 20:
        raise ValueError("schema20 version mismatch")
    row = connection.execute("SELECT name FROM schema_migrations WHERE version=20").fetchone()
    if row is None or row[0] != MIGRATION_NAME:
        raise ValueError("schema20 migration manifest mismatch")
    objects = {str(row[0]) for row in connection.execute("SELECT name FROM sqlite_master")}
    required = {"uq_scheduler_root_slot", "uq_scheduler_child_slot", "uq_raw_response_paid_scope",
                "trg_scheduler_run_attempts_terminal_update", "trg_content_identity_value",
                "capture_work_items", "capture_route_assignments", "content_metric_field_facts",
                "content_metric_projection_versions", "provider_raw_blobs", "provider_usage_settlements"}
    if not required <= objects:
        raise ValueError("schema20 required objects missing: " + str(sorted(required - objects)))
    if connection.execute("PRAGMA foreign_keys").fetchone()[0] != 1:
        raise ValueError("schema20 foreign keys must be enabled")


def validate_lineage(source: sqlite3.Connection, candidate: sqlite3.Connection) -> dict[str, Any]:
    """Recompute pre-acceptance lineage from both DBs, not receipt assertions."""
    from . import metric_field_facts as facts, usage_settlements
    from .source_routing import METRIC_FIELDS, _field_state, effective_operation, effective_provider

    if source.execute("PRAGMA user_version").fetchone()[0] != 19:
        raise ValueError("lineage requires exactly schema19 source")
    validate_structure(candidate)
    if candidate.execute("PRAGMA foreign_key_check").fetchone():
        raise ValueError("candidate foreign keys are invalid")
    preserved: dict[str, Any] = {}
    for row in source.execute("SELECT name FROM sqlite_master WHERE type='table' AND name NOT LIKE 'sqlite_%' ORDER BY name"):
        name = str(row[0])
        if name == "schema_migrations":
            old = [tuple(item) for item in source.execute("SELECT * FROM schema_migrations ORDER BY version")]
            retained = [tuple(item) for item in candidate.execute("SELECT * FROM schema_migrations WHERE version<=19 ORDER BY version")]
            if old != retained:
                raise ValueError("source schema migration history changed")
            continue
        columns = _columns(source, name)
        expected, actual = row_digest(source, name, columns), row_digest(candidate, name, columns)
        if expected != actual:
            raise ValueError("source row lineage differs: " + name)
        preserved[name] = expected
    for name, sequence in source.execute("SELECT name,seq FROM sqlite_sequence"):
        retained = candidate.execute("SELECT seq FROM sqlite_sequence WHERE name=?", (name,)).fetchone()
        if retained is None or retained[0] < sequence:
            raise ValueError("source sequence regressed: " + name)
    # Facts must reproduce every original source field under the same policy;
    # recalculating an arbitrary altered row's own hash is not sufficient.
    expected_count = 0
    actual_facts = iter(candidate.execute("SELECT * FROM content_metric_field_facts ORDER BY observation_id,field"))
    for source_row in source.execute(facts.observation_query(source) + " ORDER BY o.id"):
        observation = dict(source_row)
        if observation["observation_origin"] == "system_correction":
            continue
        metadata = json.loads(observation["metadata_json"] or "{}")
        provider_at = metadata.get("provider_data_at") if isinstance(metadata, dict) else None
        for field in sorted(METRIC_FIELDS):
            actual_row = next(actual_facts, None)
            if actual_row is None:
                raise ValueError("candidate field facts are incomplete")
            actual = dict(actual_row)
            state, value, reason = _field_state(observation, field, str(observation["platform"]))
            expected = {"content_id": int(observation["content_id"]), "observation_id": int(observation["id"]),
                        "field": field, "provider": effective_provider(observation),
                        "operation": effective_operation(observation) or "legacy_unknown", "value": value,
                        "state": state, "reason": reason, "observed_value_json": facts._json(observation[field]),
                        "captured_at": facts.utc(observation["captured_at"]), "recorded_at": facts.utc(observation["recorded_at"]),
                        "provider_data_at": facts.utc(provider_at) if isinstance(provider_at, str) else None,
                        "time_basis": "provider_data_at" if isinstance(provider_at, str) else "capture_only",
                        "raw_response_id": observation["raw_response_id"], "window_key": observation["window_key"],
                        "observation_status": observation["status"], "observation_origin": observation["observation_origin"],
                        "contract_version": facts.POLICY_VERSION}
            expected["fact_sha256"] = facts._hash(expected)
            if {key: actual[key] for key in expected} != expected:
                raise ValueError(f"candidate fact differs from source observation {observation['id']}:{field}")
            expected_count += 1
    if next(actual_facts, None) is not None:
        raise ValueError("candidate has extra field facts")
    # Evidence/projection side effects may not be used to open paid execution.
    empty = ("capture_work_items", "fetch_request_batches", "provider_request_start_events",
             "provider_paid_scope_claims", "capture_paid_send_gate_events", "capture_completion_events",
             "transport_continuity_permits", "compensation_authorizations", "metric_policy_transitions",
             "content_identity_merge_events")
    for table in empty:
        if candidate.execute(f'SELECT 1 FROM "{table}" LIMIT 1').fetchone():
            raise ValueError("migration performed runtime work: " + table)
    for route in candidate.execute("SELECT provider,route,mode FROM capture_route_assignments"):
        if tuple(route) not in {("tikhub", "legacy", "active"), ("matrix", "historical_only", "disabled")}:
            raise ValueError("migration activated a non-legacy route")
    usages = source.execute("SELECT * FROM provider_usage WHERE request_attempts>0 ORDER BY id")
    settlements = iter(candidate.execute("SELECT * FROM provider_usage_settlements ORDER BY provider_usage_id"))
    settled_count = 0
    for usage_row in usages:
        usage = dict(usage_row)
        settlement_row = next(settlements, None)
        if (settlement_row is None or settlement_row["provider_usage_id"] != usage["id"]
                or settlement_row["currency"] != usage["currency"]
                or settlement_row["amount_microunits"] != usage_settlements._amount(usage["amount"])):
            raise ValueError("candidate settlement does not conserve original charge")
        settled_count += 1
    if next(settlements, None) is not None:
        raise ValueError("candidate has extra settlement")
    result = {"contract_version": "schema20-source-lineage-v1", "source_version": 19, "target_version": 20,
              "preserved_tables": preserved, "field_facts": expected_count, "settlements": settled_count,
              "provider_calls": 0, "ordinary_paid_opened": False}
    result["lineage_sha256"] = hashlib.sha256(json.dumps(result, sort_keys=True, separators=(",", ":")).encode()).hexdigest()
    return result
