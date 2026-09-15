"""Explicit offline schema21 -> 22 unified account intake migration.

Preserves all historical rows and account identities. This prepares a candidate;
no startup upgrade, provider call, queue scheduling or release authorization is
performed. Use the companion CLI to create a verified backup and candidate.
"""
from __future__ import annotations

import hashlib
import json
import re
import sqlite3
from datetime import datetime, timezone
from typing import Any

from .schema_v20 import row_digest
from .schema_v21 import MaintenanceContext, _require_maintenance_context

SCHEMA_VERSION = 22
MIGRATION_NAME = "unified-account-intake-v1"
MIGRATION_TABLE = "account_intake_migrations"
PLATFORMS_SQL = "'douyin','xiaohongshu','wechat_channels','kuaishou'"

INTAKE_SQL = f"""CREATE TABLE account_intake_requests (
 id INTEGER PRIMARY KEY AUTOINCREMENT,
 request_key TEXT NOT NULL UNIQUE,
 input_sha256 TEXT NOT NULL CHECK(length(input_sha256)=64),
 preparation_key TEXT NOT NULL,
 platform TEXT NOT NULL CHECK(platform IN ({PLATFORMS_SQL})),
 input_json TEXT NOT NULL CHECK(json_valid(input_json)),
 source_json TEXT NOT NULL CHECK(json_valid(source_json)),
 directory_row_id INTEGER REFERENCES account_directory_rows(id) ON DELETE RESTRICT,
 account_id INTEGER REFERENCES accounts(id) ON DELETE RESTRICT,
 account_identity_id INTEGER REFERENCES account_platform_identities(id) ON DELETE RESTRICT,
 result_json TEXT NOT NULL DEFAULT '{{}}' CHECK(json_valid(result_json)),
 created_at TEXT NOT NULL, updated_at TEXT NOT NULL, completed_at TEXT
)"""
REFERENCE_SQL = f"""CREATE TABLE account_provider_references__v22 (
 account_identity_id INTEGER NOT NULL REFERENCES account_platform_identities(id) ON DELETE CASCADE,
 provider TEXT NOT NULL,
 reference_kind TEXT NOT NULL,
 reference_value TEXT NOT NULL,
 source_raw_response_id INTEGER,
 created_at TEXT NOT NULL,
 updated_at TEXT NOT NULL,
 platform TEXT NOT NULL CHECK(platform IN ({PLATFORMS_SQL})),
 PRIMARY KEY(account_identity_id,provider,reference_kind)
)"""


def _digest(value: Any) -> str:
    return hashlib.sha256(json.dumps(value, ensure_ascii=False, sort_keys=True,
                                    separators=(",", ":"), allow_nan=False).encode()).hexdigest()


def _columns(connection: sqlite3.Connection, table: str) -> list[str]:
    return [str(row[1]) for row in connection.execute(f'PRAGMA table_info("{table}")')]


def _objects(connection: sqlite3.Connection) -> list[tuple[Any, ...]]:
    return [tuple(row) for row in connection.execute(
        "SELECT type,name,tbl_name,sql FROM sqlite_master WHERE name NOT LIKE 'sqlite_%' ORDER BY type,name")]


def _table_digests(connection: sqlite3.Connection) -> dict[str, Any]:
    return {str(row[0]): row_digest(connection, str(row[0]), _columns(connection, str(row[0])))
            for row in connection.execute("SELECT name FROM sqlite_master WHERE type='table' AND name NOT LIKE 'sqlite_%' ORDER BY name")}


def _fetch_sql(old_sql: str) -> str:
    sql, count = re.subn(r'^CREATE TABLE(?: IF NOT EXISTS)?\s+"?fetch_slots"?',
                        'CREATE TABLE fetch_slots__v22', old_sql, count=1)
    if count != 1:
        raise ValueError("schema22 unexpected fetch_slots definition")
    old_stage = "'discovery','detail','metrics','comments','media_source_refresh'"
    old_target = "CHECK((account_id IS NOT NULL) <> (content_id IS NOT NULL))"
    if sql.count(old_stage) != 1 or sql.count(old_target) != 1:
        raise ValueError("schema22 fetch_slots source contract differs")
    sql = sql.replace(old_stage, old_stage + ",'profile_prepare'", 1)
    sql = sql.replace("    stage TEXT", "    intake_request_id INTEGER REFERENCES account_intake_requests(id) ON DELETE RESTRICT,\n    stage TEXT", 1)
    return sql.replace(old_target,
                       "CHECK((account_id IS NOT NULL)+(content_id IS NOT NULL)+(intake_request_id IS NOT NULL)=1),\n"
                       "    CHECK((stage='profile_prepare')=(intake_request_id IS NOT NULL))", 1)


def _assignment_sql(old_sql: str) -> str:
    sql, count = re.subn(r'^CREATE TABLE(?: IF NOT EXISTS)?\s+"?capture_route_assignments"?',
                        'CREATE TABLE capture_route_assignments__v22', old_sql, count=1)
    source = "'platform_operation','account','content'"
    if count != 1 or sql.count(source) != 1:
        raise ValueError("schema22 route assignment source contract differs")
    sql = sql.replace(source, source + ",'intake'", 1)
    anchor = " generation INTEGER"
    if sql.count(anchor) != 1:
        raise ValueError("schema22 route assignment column contract differs")
    sql = sql.replace(anchor, " intake_request_id INTEGER REFERENCES account_intake_requests(id) ON DELETE RESTRICT,\n" + anchor, 1)
    end = sql.rfind(")")
    return sql[:end] + ",\n CHECK((scope_type='intake')=(intake_request_id IS NOT NULL))\n" + sql[end:]


def _index_columns(connection: sqlite3.Connection, index: str) -> list[str]:
    return [str(row[2]) for row in connection.execute(f'PRAGMA index_info("{index}")')]


def validate_structure(connection: sqlite3.Connection) -> None:
    if connection.execute("PRAGMA user_version").fetchone()[0] != SCHEMA_VERSION:
        raise ValueError("schema22 version mismatch")
    manifests = connection.execute("SELECT version,name FROM schema_migrations WHERE version>=22").fetchall()
    if [tuple(row) for row in manifests] != [(22, MIGRATION_NAME)]:
        raise ValueError("schema22 migration manifest mismatch")
    for pragma in ("foreign_keys", "recursive_triggers"):
        if connection.execute("PRAGMA " + pragma).fetchone()[0] != 1:
            raise ValueError("schema22 requires " + pragma)
    required = {
        "account_intake_requests": {"id", "request_key", "input_sha256", "preparation_key", "platform", "input_json", "source_json", "directory_row_id", "account_id", "account_identity_id", "result_json", "created_at", "updated_at", "completed_at"},
        "fetch_slots": {"intake_request_id"}, "capture_work_items": {"intake_request_id"},
        "provider_raw_responses": {"intake_request_id"}, "account_provider_references": {"platform"},
        "fetch_request_batch_members": {"intake_request_id"}, "capture_route_assignments": {"intake_request_id"},
    }
    for table, columns in required.items():
        if not columns <= set(_columns(connection, table)):
            raise ValueError("schema22 required columns missing: " + table)
    objects = {row[1]: row for row in _objects(connection)}
    inherited = {"capture_route_assignments", "content_metric_field_facts", "provider_raw_blobs",
                 "provider_usage_settlements", "uq_scheduler_root_slot", "uq_scheduler_child_slot",
                 "trg_content_identity_value", "account_classification_migrations",
                 "trg_account_classification_migrations_update", "trg_account_classification_migrations_delete"}
    added = {MIGRATION_TABLE, "idx_account_intake_preparation", "uq_fetch_intake_slot",
             "idx_capture_work_intake", "idx_raw_response_intake", "idx_capture_route_intake", "idx_fetch_batch_member_intake", "trg_account_reference_platform_insert",
             "trg_account_reference_platform_update", "trg_account_intake_migrations_update",
             "trg_account_intake_migrations_delete"}
    if not inherited | added <= objects.keys():
        raise ValueError("schema22 required objects missing")
    if _index_columns(connection, "uq_account_provider_reference_value") != ["provider", "platform", "reference_kind", "reference_value"]:
        raise ValueError("schema22 provider reference platform index differs")
    if _index_columns(connection, "uq_fetch_intake_slot") != ["intake_request_id", "stage", "window_key"]:
        raise ValueError("schema22 intake slot index differs")
    slot_sql = re.sub(r"\s+", "", str(objects["fetch_slots"][3]))
    if ("'profile_prepare'" not in slot_sql
            or "CHECK((account_idISNOTNULL)+(content_idISNOTNULL)+(intake_request_idISNOTNULL)=1)" not in slot_sql
            or "CHECK((stage='profile_prepare')=(intake_request_idISNOTNULL))" not in slot_sql):
        raise ValueError("schema22 fetch target contract differs")
    assignment_sql = re.sub(r"\s+", "", str(objects["capture_route_assignments"][3]))
    if ("'intake'" not in assignment_sql
            or "CHECK((scope_type='intake')=(intake_request_idISNOTNULL))" not in assignment_sql):
        raise ValueError("schema22 route intake contract differs")
    for table in ("fetch_slots", "capture_work_items", "provider_raw_responses", "fetch_request_batch_members", "capture_route_assignments"):
        if not any(row[2] == "account_intake_requests" and row[3] == "intake_request_id" and row[4] == "id"
                   for row in connection.execute(f'PRAGMA foreign_key_list("{table}")')):
            raise ValueError("schema22 intake foreign key missing: " + table)
    if connection.execute("SELECT 1 FROM account_provider_references r JOIN account_platform_identities i ON i.id=r.account_identity_id WHERE r.platform!=i.platform LIMIT 1").fetchone():
        raise ValueError("schema22 provider reference platform mismatch")


def migrate(connection: sqlite3.Connection, *, maintenance: MaintenanceContext | None = None) -> dict[str, Any]:
    from .storage import _require_initialization_safety
    from .schema_v21 import validate_structure as validate_source

    if maintenance is None:
        _require_initialization_safety(connection)
    else:
        _require_maintenance_context(connection, maintenance)
    if connection.in_transaction:
        raise ValueError("schema22 migration requires an idle offline connection")
    if connection.execute("PRAGMA query_only").fetchone()[0]:
        raise ValueError("schema22 migration requires a writable offline candidate")
    if connection.execute("PRAGMA user_version").fetchone()[0] == SCHEMA_VERSION:
        proof = migration_proof(connection)
        return {**proof, "status": "unchanged", "sql_writes": 0, "provider_calls": 0}
    validate_source(connection)
    if connection.execute("PRAGMA foreign_key_check").fetchone():
        raise ValueError("schema22 source has foreign key violations")
    before = _table_digests(connection)
    source_schema = _digest(_objects(connection))
    sequence = {str(row[0]): int(row[1]) for row in connection.execute("SELECT name,seq FROM sqlite_sequence")}
    old_objects = _objects(connection)
    fetch_sql = next(str(row[3]) for row in old_objects if row[1] == "fetch_slots")
    expected_reference_columns = ["account_identity_id", "provider", "reference_kind", "reference_value", "source_raw_response_id", "created_at", "updated_at"]
    if _columns(connection, "account_provider_references") != expected_reference_columns:
        raise ValueError("schema22 provider reference source columns differ")
    at = datetime.now(timezone.utc).isoformat(timespec="microseconds").replace("+00:00", "Z")
    fk = int(connection.execute("PRAGMA foreign_keys").fetchone()[0])
    legacy = int(connection.execute("PRAGMA legacy_alter_table").fetchone()[0])
    connection.execute("PRAGMA foreign_keys=OFF")
    connection.execute("PRAGMA legacy_alter_table=ON")
    try:
        connection.execute("BEGIN IMMEDIATE")
        connection.execute(INTAKE_SQL)
        connection.execute("CREATE INDEX idx_account_intake_preparation ON account_intake_requests(preparation_key)")
        for table in ("capture_work_items", "provider_raw_responses", "fetch_request_batch_members"):
            connection.execute(f"ALTER TABLE {table} ADD COLUMN intake_request_id INTEGER REFERENCES account_intake_requests(id) ON DELETE RESTRICT")
        connection.execute("CREATE INDEX idx_capture_work_intake ON capture_work_items(intake_request_id) WHERE intake_request_id IS NOT NULL")
        connection.execute("CREATE INDEX idx_raw_response_intake ON provider_raw_responses(intake_request_id) WHERE intake_request_id IS NOT NULL")
        connection.execute(_fetch_sql(fetch_sql))
        connection.execute(_assignment_sql(next(str(row[3]) for row in old_objects if row[1] == "capture_route_assignments")))
        connection.execute(REFERENCE_SQL)
        for table in ("fetch_slots", "account_provider_references", "capture_route_assignments"):
            quoted = ",".join('"' + name + '"' for name in before[table]["columns"])
            if table == "account_provider_references":
                selected = ",".join('r."' + name + '"' for name in before[table]["columns"])
                connection.execute(f'INSERT INTO {table}__v22 ({quoted},platform) SELECT {selected},i.platform FROM {table} r JOIN account_platform_identities i ON i.id=r.account_identity_id')
            else:
                connection.execute(f'INSERT INTO {table}__v22 ({quoted}) SELECT {quoted} FROM {table}')
            for kind, name, owner, _ in old_objects:
                if kind == "trigger" and owner == table:
                    connection.execute(f'DROP TRIGGER "{name}"')
            connection.execute(f'DROP TABLE "{table}"')
            connection.execute(f'ALTER TABLE "{table}__v22" RENAME TO "{table}"')
            for kind, name, owner, sql in old_objects:
                if kind in {"index", "trigger"} and owner == table and sql and name != "uq_account_provider_reference_value":
                    connection.execute(sql)
        connection.execute("CREATE INDEX idx_capture_route_intake ON capture_route_assignments(intake_request_id) WHERE intake_request_id IS NOT NULL")
        connection.execute("CREATE INDEX idx_fetch_batch_member_intake ON fetch_request_batch_members(intake_request_id) WHERE intake_request_id IS NOT NULL")
        connection.execute("CREATE UNIQUE INDEX uq_account_provider_reference_value ON account_provider_references(provider,platform,reference_kind,reference_value)")
        connection.execute("CREATE UNIQUE INDEX uq_fetch_intake_slot ON fetch_slots(intake_request_id,stage,window_key) WHERE intake_request_id IS NOT NULL")
        for action in ("INSERT", "UPDATE"):
            connection.execute(f"CREATE TRIGGER trg_account_reference_platform_{action.lower()} BEFORE {action} ON account_provider_references "
                               "WHEN NOT EXISTS (SELECT 1 FROM account_platform_identities i WHERE i.id=NEW.account_identity_id AND i.platform=NEW.platform) "
                               "BEGIN SELECT RAISE(ABORT,'provider reference platform does not match identity'); END")
        connection.execute("CREATE TABLE account_intake_migrations (id INTEGER PRIMARY KEY CHECK(id=1), applied_at TEXT NOT NULL, payload_json TEXT NOT NULL CHECK(json_valid(payload_json)), receipt_sha256 TEXT NOT NULL CHECK(length(receipt_sha256)=64))")
        for action in ("UPDATE", "DELETE"):
            connection.execute(f"CREATE TRIGGER trg_account_intake_migrations_{action.lower()} BEFORE {action} ON account_intake_migrations BEGIN SELECT RAISE(ABORT,'account intake migration is immutable'); END")
        for name, seq in sequence.items():
            current = connection.execute("SELECT seq FROM sqlite_sequence WHERE name=?", (name,)).fetchone()
            if current is None:
                connection.execute("INSERT INTO sqlite_sequence(name,seq) VALUES (?,?)", (name, seq))
            elif int(current[0]) < seq:
                connection.execute("UPDATE sqlite_sequence SET seq=? WHERE name=?", (seq, name))
        for table, expected in before.items():
            if row_digest(connection, table, expected["columns"]) != expected:
                raise ValueError("schema22 historical rows changed: " + table)
        connection.execute("INSERT INTO schema_migrations(version,name,applied_at) VALUES (22,?,?)", (MIGRATION_NAME, at))
        connection.execute("PRAGMA user_version=22")
        if connection.execute("PRAGMA foreign_key_check").fetchone():
            raise ValueError("schema22 foreign key validation failed")
        if connection.execute("PRAGMA quick_check").fetchone()[0] != "ok":
            raise ValueError("schema22 quick_check failed")
        receipt = {"contract_version": "account-intake-migration-v1", "status": "candidate",
                   "source_version": 21, "target_version": 22, "migration_name": MIGRATION_NAME,
                   "applied_at": at, "source_schema_sha256": source_schema,
                   "target_schema_sha256": _digest(_objects(connection)), "preserved_tables": before,
                   "preserved_sequences": sequence, "provider_calls": 0,
                   "capture_state_changed": False, "production_acceptance": False}
        receipt["sha256"] = _digest(receipt)
        connection.execute("INSERT INTO account_intake_migrations VALUES (1,?,?,?)",
                           (at, json.dumps(receipt, ensure_ascii=False, sort_keys=True, separators=(",", ":")), receipt["sha256"]))
        connection.commit()
    except BaseException:
        connection.rollback()
        raise
    finally:
        connection.execute(f"PRAGMA foreign_keys={fk}")
        connection.execute(f"PRAGMA legacy_alter_table={legacy}")
    migration_proof(connection)
    return receipt


def migration_proof(connection: sqlite3.Connection) -> dict[str, Any]:
    validate_structure(connection)
    rows = connection.execute("SELECT applied_at,payload_json,receipt_sha256 FROM account_intake_migrations").fetchall()
    if len(rows) != 1:
        raise ValueError("schema22 migration receipt missing or duplicated")
    at, raw, checksum = tuple(rows[0])
    receipt = json.loads(raw)
    body = dict(receipt)
    supplied = body.pop("sha256", None)
    manifest_at = connection.execute("SELECT applied_at FROM schema_migrations WHERE version=22").fetchone()[0]
    if (supplied != checksum or checksum != _digest(body)
            or receipt.get("contract_version") != "account-intake-migration-v1"
            or receipt.get("source_version") != 21 or receipt.get("target_version") != 22
            or receipt.get("migration_name") != MIGRATION_NAME
            or receipt.get("applied_at") != at or at != manifest_at
            or receipt.get("provider_calls") != 0 or receipt.get("capture_state_changed") is not False
            or receipt.get("production_acceptance") is not False
            or receipt.get("target_schema_sha256") != _digest(_objects(connection))):
        raise ValueError("schema22 migration receipt or schema differs")
    return {"contract_version": "account-intake-migration-proof-v1", "schema_version": 22,
            "schema_migration": MIGRATION_NAME, "receipt_sha256": checksum, "applied_at": at,
            "source_schema_sha256": receipt["source_schema_sha256"],
            "target_schema_sha256": receipt["target_schema_sha256"]}


def validate_lineage(source: sqlite3.Connection, candidate: sqlite3.Connection) -> dict[str, Any]:
    """Verify an immediate candidate; later business writes are intentionally rejected."""
    from .schema_v21 import validate_structure as validate_source
    validate_source(source)
    proof = migration_proof(candidate)
    receipt = json.loads(candidate.execute("SELECT payload_json FROM account_intake_migrations").fetchone()[0])
    if _digest(_objects(source)) != receipt["source_schema_sha256"]:
        raise ValueError("schema22 source structure changed")
    expected = _table_digests(source)
    if expected != receipt["preserved_tables"]:
        raise ValueError("schema22 source rows changed")
    for table, original in expected.items():
        if table == "schema_migrations":
            left = [tuple(row) for row in source.execute("SELECT * FROM schema_migrations ORDER BY version")]
            right = [tuple(row) for row in candidate.execute("SELECT * FROM schema_migrations WHERE version<=21 ORDER BY version")]
            if left != right:
                raise ValueError("schema22 historical migration manifest changed")
        elif row_digest(candidate, table, original["columns"]) != original:
            raise ValueError("schema22 retained table changed: " + table)
    source_sequences = {str(row[0]):int(row[1]) for row in source.execute("SELECT name,seq FROM sqlite_sequence")}
    if source_sequences != receipt["preserved_sequences"]:
        raise ValueError("schema22 source sequences changed")
    candidate_sequences = {str(row[0]):int(row[1]) for row in candidate.execute("SELECT name,seq FROM sqlite_sequence")}
    for name in set(source_sequences) | set(candidate_sequences):
        # Rebuilding an empty AUTOINCREMENT table may materialize a zero row.
        # Any issued ID beyond the sealed source denotes subsequent business
        # activity, even if its row was later deleted; rollback must stop.
        if candidate_sequences.get(name,0) != source_sequences.get(name,0):
            raise ValueError("schema22 sequence changed: " + name)
    for table in ("account_intake_requests",):
        if candidate.execute(f"SELECT 1 FROM {table} LIMIT 1").fetchone():
            raise ValueError("schema22 immediate candidate has new business rows")
    if candidate.execute("PRAGMA foreign_key_check").fetchone():
        raise ValueError("schema22 candidate foreign keys invalid")
    return {**proof, "retained_tables_verified": True, "retained_table_count": len(expected)}
