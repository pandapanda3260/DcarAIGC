"""Explicit, additive four-platform repair migration; never schedules backfill."""
from __future__ import annotations

import hashlib
import json
import sqlite3
from datetime import datetime, timezone
from typing import Any

from .schema_v21 import MaintenanceContext, _require_maintenance_context

SCHEMA_VERSION = 23
MIGRATION_NAME = "four-platform-forward-flow-v1"
MIGRATION_TABLE = "four_platform_flow_migrations"


def digest(value: Any) -> str:
    return hashlib.sha256(json.dumps(value, sort_keys=True, ensure_ascii=False,
        separators=(",", ":"), allow_nan=False).encode()).hexdigest()


def objects(connection: sqlite3.Connection) -> list[tuple]:
    return [tuple(row) for row in connection.execute("SELECT type,name,tbl_name,sql FROM sqlite_master "
        "WHERE name NOT LIKE 'sqlite_%' ORDER BY type,name")]


def _revision_triggers(connection: sqlite3.Connection) -> None:
    # These are authority inputs, not works or observations. Identical updates
    # and catalog-derived projections must not invalidate their own new plan.
    tracked = {
        "account_directory_rows": ("account_id", "platform", "uid", "display_account_id", "identity_status", "account_status", "locator_json", "locator_revision"),
        "accounts": ("enabled",),
        "account_platform_identities": ("account_id", "platform", "uid"),
        "account_provider_references": ("account_identity_id", "platform", "provider", "reference_kind", "reference_value", "source_raw_response_id"),
        "account_intake_requests": ("input_sha256", "preparation_key", "account_id", "account_identity_id", "result_json", "completed_at"),
    }
    for table, columns in tracked.items():
        for action in ("INSERT", "UPDATE", "DELETE"):
            changed = " AND (" + " OR ".join(f"OLD.{col} IS NOT NEW.{col}" for col in columns) + ")" if action == "UPDATE" else ""
            connection.execute(f"CREATE TRIGGER trg_catalog_revision_{table}_{action.lower()} AFTER {action} ON {table} "
                "WHEN (SELECT projection_depth FROM capture_catalog_revision WHERE id=1)=0" + changed +
                " BEGIN UPDATE capture_catalog_revision SET revision=revision+1 WHERE id=1; END")
    # Immutable account admission receipts live in the existing scheduler log.
    from .account_operating_receipts import ACCOUNT_STATUS_JOB_ID
    job = ACCOUNT_STATUS_JOB_ID.replace("'", "''")
    for action in ("INSERT", "UPDATE", "DELETE"):
        row = "OLD" if action == "DELETE" else "NEW"
        connection.execute(f"CREATE TRIGGER trg_catalog_revision_admission_{action.lower()} AFTER {action} ON scheduler_runs "
            f"WHEN {row}.job_id='{job}' AND (SELECT projection_depth FROM capture_catalog_revision WHERE id=1)=0 "
            "BEGIN UPDATE capture_catalog_revision SET revision=revision+1 WHERE id=1; END")
    # A referenced profile raw can be revoked or moved. New discovery raw does
    # not invalidate a directory plan. Send-time byte verification remains live.
    for action in ("UPDATE", "DELETE"):
        connection.execute(f"CREATE TRIGGER trg_catalog_revision_profile_raw_{action.lower()} AFTER {action} ON provider_raw_responses "
            "WHEN EXISTS(SELECT 1 FROM account_provider_references WHERE source_raw_response_id=OLD.id) "
            "AND (SELECT projection_depth FROM capture_catalog_revision WHERE id=1)=0 "
            "BEGIN UPDATE capture_catalog_revision SET revision=revision+1 WHERE id=1; END")


def validate_structure(connection: sqlite3.Connection) -> None:
    from .capture_work_index import normalize_objects
    if connection.execute("PRAGMA user_version").fetchone()[0] != SCHEMA_VERSION:
        raise ValueError("schema23 version mismatch")
    manifest = [tuple(row) for row in connection.execute("SELECT version,name FROM schema_migrations WHERE version>=22 ORDER BY version")]
    if manifest != [(22, "unified-account-intake-v1"), (23, MIGRATION_NAME)]:
        raise ValueError("schema23 migration manifest mismatch")
    for pragma in ("foreign_keys", "recursive_triggers"):
        if connection.execute("PRAGMA " + pragma).fetchone()[0] != 1:
            raise ValueError("schema23 requires " + pragma)
    row = connection.execute(f"SELECT payload_json,receipt_sha256 FROM {MIGRATION_TABLE} WHERE id=1").fetchone()
    if row is None:
        raise ValueError("schema23 migration proof missing")
    proof = json.loads(row[0])
    if digest(proof) != row[1] or proof["target_schema_sha256"] != digest(normalize_objects(objects(connection))):
        raise ValueError("schema23 migration structure or proof changed")
    # The revision is a changing catalog input generation, not an immutable
    # migration dependency. Preserve exactly the structural predicates so a
    # prepared inheritance proof survives an unrelated legitimate intake.
    state = connection.execute("SELECT (typeof(revision) IN ('integer','real') AND revision>=0) AS revision_valid,"
        "projection_depth=0 AS projection_idle "
        "FROM capture_catalog_revision WHERE id=1").fetchone()
    if state is None or state[0] != 1 or state[1] != 1:
        raise ValueError("schema23 catalog revision state invalid")


def migration_proof(connection: sqlite3.Connection) -> dict[str, Any]:
    validate_structure(connection)
    row = connection.execute(f"SELECT payload_json,receipt_sha256 FROM {MIGRATION_TABLE} WHERE id=1").fetchone()
    return {**json.loads(row[0]), "receipt_sha256": row[1]}


def migrate(connection: sqlite3.Connection, *, maintenance: MaintenanceContext | None = None) -> dict[str, Any]:
    from .storage import _require_initialization_safety
    from .schema_v22 import migration_proof as source_proof, _table_digests
    if maintenance is None:
        _require_initialization_safety(connection)
    else:
        _require_maintenance_context(connection, maintenance)
    if connection.in_transaction or connection.execute("PRAGMA query_only").fetchone()[0]:
        raise ValueError("schema23 migration requires idle writable offline connection")
    if connection.execute("PRAGMA user_version").fetchone()[0] == SCHEMA_VERSION:
        return {**migration_proof(connection), "status": "unchanged", "sql_writes": 0, "provider_calls": 0}
    parent = source_proof(connection)
    before = _table_digests(connection)
    at = datetime.now(timezone.utc).isoformat()
    try:
        connection.execute("BEGIN IMMEDIATE")
        connection.execute("ALTER TABLE account_directory_rows ADD COLUMN locator_json TEXT NOT NULL DEFAULT '{}' CHECK(json_valid(locator_json) AND json_type(locator_json)='object')")
        connection.execute("ALTER TABLE account_directory_rows ADD COLUMN locator_revision INTEGER NOT NULL DEFAULT 0 CHECK(locator_revision>=0)")
        connection.execute("CREATE TABLE account_preparation_owners (preparation_key TEXT PRIMARY KEY,intake_request_id INTEGER NOT NULL REFERENCES account_intake_requests(id) ON DELETE RESTRICT,updated_at TEXT NOT NULL)")
        connection.execute("CREATE TABLE capture_catalog_revision (id INTEGER PRIMARY KEY CHECK(id=1),revision INTEGER NOT NULL CHECK(revision>=0),projection_depth INTEGER NOT NULL DEFAULT 0 CHECK(projection_depth>=0))")
        connection.execute("INSERT INTO capture_catalog_revision VALUES(1,0,0)")
        connection.execute("CREATE TABLE capture_plan_reuse (reuse_key TEXT PRIMARY KEY CHECK(length(reuse_key)=64),source_plan_id INTEGER NOT NULL REFERENCES capture_source_plans(id) ON DELETE RESTRICT,input_json TEXT NOT NULL CHECK(json_valid(input_json)),created_at TEXT NOT NULL)")
        connection.execute("CREATE TABLE content_link_intakes (id INTEGER PRIMARY KEY AUTOINCREMENT,request_key TEXT NOT NULL UNIQUE,platform TEXT NOT NULL,original_url TEXT NOT NULL,input_json TEXT NOT NULL CHECK(json_valid(input_json)),resolution_json TEXT NOT NULL DEFAULT '{}' CHECK(json_valid(resolution_json)),status TEXT NOT NULL CHECK(status IN ('pending','resolved','conflict')),reason TEXT NOT NULL,content_id INTEGER REFERENCES content_items(id) ON DELETE RESTRICT,created_at TEXT NOT NULL,updated_at TEXT NOT NULL)")
        connection.execute("CREATE TABLE media_source_refresh_proposals (id TEXT PRIMARY KEY,request_key TEXT NOT NULL UNIQUE,content_id INTEGER NOT NULL REFERENCES content_items(id) ON DELETE RESTRICT,source_generation TEXT NOT NULL,platform TEXT NOT NULL,platform_content_id TEXT NOT NULL,detail_operation TEXT NOT NULL,request_limit INTEGER NOT NULL CHECK(request_limit=1),max_amount REAL NOT NULL CHECK(max_amount>0),currency TEXT NOT NULL,status TEXT NOT NULL CHECK(status IN ('prepared','queued','expired')),command_id TEXT,created_at TEXT NOT NULL,expires_at TEXT NOT NULL,updated_at TEXT NOT NULL)")
        connection.execute(f"CREATE TABLE {MIGRATION_TABLE}(id INTEGER PRIMARY KEY CHECK(id=1),applied_at TEXT NOT NULL,payload_json TEXT NOT NULL CHECK(json_valid(payload_json)),receipt_sha256 TEXT NOT NULL CHECK(length(receipt_sha256)=64))")
        for action in ("UPDATE", "DELETE"):
            connection.execute(f"CREATE TRIGGER trg_four_platform_migration_{action.lower()} BEFORE {action} ON {MIGRATION_TABLE} BEGIN SELECT RAISE(ABORT,'four platform migration is immutable'); END")
        _revision_triggers(connection)
        connection.execute("INSERT INTO schema_migrations(version,name,applied_at) VALUES(?,?,?)", (23,MIGRATION_NAME,at))
        connection.execute("PRAGMA user_version=23")
        # Verify original columns and rows, including every request and ledger.
        from .schema_v20 import row_digest
        for table, expected in before.items():
            if table == "schema_migrations":
                continue
            if row_digest(connection, table, expected["columns"]) != expected:
                raise ValueError("schema23 modified historical rows: " + table)
        if connection.execute("PRAGMA foreign_key_check").fetchone():
            raise ValueError("schema23 foreign key violation")
        proof = {"contract":"four-platform-flow-migration-v1", "source_version":22,"target_version":23,
            "parent_receipt_sha256":parent["receipt_sha256"],"retained_tables":before,
            "target_schema_sha256":digest(objects(connection)),"applied_at":at,"provider_calls":0,"backfill_jobs":0}
        connection.execute(f"INSERT INTO {MIGRATION_TABLE} VALUES(1,?,?,?)", (at,json.dumps(proof,ensure_ascii=False,sort_keys=True,separators=(",",":")),digest(proof)))
        validate_structure(connection)
        connection.commit()
    except BaseException:
        connection.rollback()
        raise
    return {**migration_proof(connection),"status":"migrated"}
