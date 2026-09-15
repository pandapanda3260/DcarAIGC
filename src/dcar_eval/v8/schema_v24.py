"""Additive, explicit schema 23 -> 24 duplicate-index migration."""
from __future__ import annotations

import json
import sqlite3
from datetime import datetime, timezone
from functools import lru_cache
from typing import Any

from .schema_v21 import MaintenanceContext, _require_maintenance_context
from .schema_v23 import digest, objects

SCHEMA_VERSION = 24
MIGRATION_NAME = "duplicate-fingerprint-index-v1"
MIGRATION_TABLE = "duplicate_index_migrations"
RUNTIME_TABLES = (
    "duplicate_index_generations", "duplicate_current_fingerprints",
    "duplicate_fingerprint_media", "duplicate_fingerprint_frames",
    "duplicate_match_edges", "duplicate_components", "duplicate_component_members",
    "duplicate_dirty_work", "duplicate_work_staging",
)

DDL = (
    "CREATE INDEX idx_scheduler_attempts_contract_run_v24 ON scheduler_run_attempts(json_extract(details_json,'$.contract_version'),scheduler_run_id)",
    """CREATE TABLE duplicate_index_generations (
        generation_id TEXT PRIMARY KEY, fingerprint_version TEXT NOT NULL,
        rule_digest TEXT NOT NULL, index_contract_version TEXT NOT NULL,
        state TEXT NOT NULL CHECK(state IN ('building','ready','retired')),
        index_revision INTEGER NOT NULL DEFAULT 0 CHECK(index_revision>=0),
        graph_revision INTEGER NOT NULL DEFAULT 0 CHECK(graph_revision>=0),
        graph_worker_owner TEXT, lease_token TEXT, lease_until TEXT,
        created_at TEXT NOT NULL, activated_at TEXT)""",
    "CREATE UNIQUE INDEX uq_duplicate_ready_generation ON duplicate_index_generations(state) WHERE state='ready'",
    """CREATE TABLE duplicate_current_fingerprints (
        generation_id TEXT NOT NULL REFERENCES duplicate_index_generations(generation_id) ON DELETE CASCADE,
        content_id INTEGER NOT NULL REFERENCES content_items(id) ON DELETE CASCADE,
        fingerprint_id INTEGER REFERENCES duplicate_fingerprints(id) ON DELETE RESTRICT,
        source_sha256 TEXT, input_revision INTEGER NOT NULL CHECK(input_revision>=1),
        text_sha256 TEXT COLLATE BINARY,
        input_status TEXT NOT NULL CHECK(input_status IN ('available','unavailable')),
        activated_at TEXT NOT NULL,
        PRIMARY KEY(generation_id,content_id),
        CHECK((input_status='available' AND fingerprint_id IS NOT NULL AND source_sha256 IS NOT NULL)
              OR (input_status='unavailable' AND fingerprint_id IS NULL AND text_sha256 IS NULL)))""",
    "CREATE INDEX idx_duplicate_current_text ON duplicate_current_fingerprints(generation_id,text_sha256,content_id) WHERE text_sha256 IS NOT NULL AND text_sha256<>''",
    "CREATE INDEX idx_duplicate_current_fingerprint ON duplicate_current_fingerprints(generation_id,fingerprint_id,content_id)",
    """CREATE TABLE duplicate_fingerprint_media (
        generation_id TEXT NOT NULL REFERENCES duplicate_index_generations(generation_id) ON DELETE CASCADE,
        fingerprint_id INTEGER NOT NULL REFERENCES duplicate_fingerprints(id) ON DELETE CASCADE,
        media_ordinal INTEGER NOT NULL CHECK(media_ordinal>=0),
        media_sha256 TEXT NOT NULL COLLATE BINARY,
        PRIMARY KEY(generation_id,fingerprint_id,media_ordinal))""",
    "CREATE INDEX idx_duplicate_media_hash ON duplicate_fingerprint_media(generation_id,media_sha256,fingerprint_id)",
    """CREATE TABLE duplicate_fingerprint_frames (
        generation_id TEXT NOT NULL REFERENCES duplicate_index_generations(generation_id) ON DELETE CASCADE,
        fingerprint_id INTEGER NOT NULL REFERENCES duplicate_fingerprints(id) ON DELETE CASCADE,
        frame_ordinal INTEGER NOT NULL CHECK(frame_ordinal>=0),
        phash BLOB NOT NULL CHECK(typeof(phash)='blob' AND length(phash)=8),
        band0 INTEGER NOT NULL CHECK(typeof(band0)='integer' AND band0 BETWEEN 0 AND 65535),
        band1 INTEGER NOT NULL CHECK(typeof(band1)='integer' AND band1 BETWEEN 0 AND 65535),
        band2 INTEGER NOT NULL CHECK(typeof(band2)='integer' AND band2 BETWEEN 0 AND 65535),
        band3 INTEGER NOT NULL CHECK(typeof(band3)='integer' AND band3 BETWEEN 0 AND 65535),
        PRIMARY KEY(generation_id,fingerprint_id,frame_ordinal))""",
    *(f"CREATE INDEX idx_duplicate_frame_band{i} ON duplicate_fingerprint_frames(generation_id,band{i},fingerprint_id,frame_ordinal)" for i in range(4)),
    """CREATE TABLE duplicate_match_edges (
        generation_id TEXT NOT NULL REFERENCES duplicate_index_generations(generation_id) ON DELETE CASCADE,
        left_content_id INTEGER NOT NULL, right_content_id INTEGER NOT NULL,
        left_fingerprint_id INTEGER NOT NULL, right_fingerprint_id INTEGER NOT NULL,
        left_input_revision INTEGER NOT NULL, right_input_revision INTEGER NOT NULL,
        confidence REAL NOT NULL, comparison_json TEXT NOT NULL CHECK(json_valid(comparison_json)),
        edge_revision INTEGER NOT NULL CHECK(edge_revision>=1),
        PRIMARY KEY(generation_id,left_content_id,right_content_id),
        CHECK(left_content_id<right_content_id))""",
    "CREATE INDEX idx_duplicate_edges_right ON duplicate_match_edges(generation_id,right_content_id,left_content_id)",
    """CREATE TABLE duplicate_components (
        generation_id TEXT NOT NULL REFERENCES duplicate_index_generations(generation_id) ON DELETE CASCADE,
        component_id TEXT NOT NULL, canonical_content_id INTEGER NOT NULL,
        component_revision INTEGER NOT NULL CHECK(component_revision>=1),
        state TEXT NOT NULL CHECK(state IN ('ready','dirty')),
        member_count INTEGER NOT NULL CHECK(member_count>=1), updated_at TEXT NOT NULL,
        PRIMARY KEY(generation_id,component_id))""",
    """CREATE TABLE duplicate_component_members (
        generation_id TEXT NOT NULL, content_id INTEGER NOT NULL, component_id TEXT NOT NULL,
        PRIMARY KEY(generation_id,content_id),
        FOREIGN KEY(generation_id,component_id) REFERENCES duplicate_components(generation_id,component_id) ON DELETE CASCADE)""",
    "CREATE INDEX idx_duplicate_component_members ON duplicate_component_members(generation_id,component_id,content_id)",
    """CREATE TABLE duplicate_dirty_work (
        generation_id TEXT NOT NULL REFERENCES duplicate_index_generations(generation_id) ON DELETE CASCADE,
        content_id INTEGER NOT NULL, target_fingerprint_id INTEGER,
        target_input_revision INTEGER NOT NULL CHECK(target_input_revision>=1), old_component_id TEXT,
        reason TEXT NOT NULL, status TEXT NOT NULL CHECK(status IN ('pending','claimed','retryable','ready','failed')),
        attempt_count INTEGER NOT NULL DEFAULT 0 CHECK(attempt_count>=0), retry_at TEXT, lease_token TEXT,
        checkpoint_json TEXT NOT NULL DEFAULT '{}' CHECK(json_valid(checkpoint_json)), last_error TEXT,
        completed_input_revision INTEGER, completed_at TEXT,
        PRIMARY KEY(generation_id,content_id))""",
    "CREATE INDEX idx_duplicate_dirty_claim ON duplicate_dirty_work(generation_id,status,retry_at,content_id)",
    """CREATE TABLE duplicate_work_staging (
        generation_id TEXT NOT NULL REFERENCES duplicate_index_generations(generation_id) ON DELETE CASCADE,
        work_content_id INTEGER NOT NULL, target_input_revision INTEGER NOT NULL,
        chunk_no INTEGER NOT NULL CHECK(chunk_no>=0), record_key TEXT NOT NULL,
        record_type TEXT NOT NULL, input_snapshot_digest TEXT NOT NULL, cursor TEXT,
        result_json TEXT NOT NULL CHECK(json_valid(result_json)), lease_token TEXT NOT NULL, created_at TEXT NOT NULL,
        PRIMARY KEY(generation_id,work_content_id,target_input_revision,chunk_no,record_key))""",
    """CREATE TABLE duplicate_index_migrations (
        id INTEGER PRIMARY KEY CHECK(id=1), applied_at TEXT NOT NULL,
        payload_json TEXT NOT NULL CHECK(json_valid(payload_json)),
        receipt_sha256 TEXT NOT NULL CHECK(length(receipt_sha256)=64))""",
    *(f"CREATE TRIGGER trg_duplicate_index_migration_{action.lower()} BEFORE {action} ON duplicate_index_migrations BEGIN SELECT RAISE(ABORT,'duplicate index migration is immutable'); END" for action in ("UPDATE", "DELETE")),
    *(f"""CREATE TRIGGER trg_duplicate_current_binding_{action.lower()} BEFORE {action} ON duplicate_current_fingerprints
        WHEN NEW.fingerprint_id IS NOT NULL AND NOT EXISTS (
          SELECT 1 FROM duplicate_fingerprints f JOIN duplicate_index_generations g
          ON g.generation_id=NEW.generation_id WHERE f.id=NEW.fingerprint_id
          AND f.content_id=NEW.content_id AND f.source_sha256=NEW.source_sha256
          AND f.fingerprint_version=g.fingerprint_version AND f.text_sha256 IS NEW.text_sha256)
        BEGIN SELECT RAISE(ABORT,'duplicate current fingerprint binding mismatch'); END""" for action in ("INSERT", "UPDATE")),
)


def create_tables(connection: sqlite3.Connection) -> None:
    """Install exactly this schema inside the caller's migration transaction."""
    for statement in DDL:
        connection.execute(statement)
    from .runtime_budget_projection import DDL as budget_projection_ddl
    connection.execute(budget_projection_ddl)


@lru_cache(maxsize=1)
def _declared_additions() -> tuple[tuple, ...]:
    """Ask SQLite to canonicalize this release's DDL once per process."""
    fixture = sqlite3.connect(":memory:")
    try:
        fixture.execute("CREATE TABLE provider_usage(provider,currency,id,amount,recorded_at,operation,details_json)")
        fixture.execute("CREATE TABLE scheduler_run_attempts(details_json,scheduler_run_id)")
        create_tables(fixture)
        return tuple(row for row in objects(fixture) if row[1] not in {"provider_usage", "scheduler_run_attempts"})
    finally:
        fixture.close()


def validate_structure(connection: sqlite3.Connection) -> None:
    if connection.execute("PRAGMA user_version").fetchone()[0] != SCHEMA_VERSION:
        raise ValueError("schema24 version mismatch")
    if [tuple(row) for row in connection.execute("SELECT version,name FROM schema_migrations WHERE version>=22 ORDER BY version")] != [
        (22, "unified-account-intake-v1"), (23, "four-platform-forward-flow-v1"), (24, MIGRATION_NAME)
    ]:
        raise ValueError("schema24 migration manifest mismatch")
    for pragma in ("foreign_keys", "recursive_triggers"):
        if connection.execute("PRAGMA " + pragma).fetchone()[0] != 1:
            raise ValueError("schema24 requires " + pragma)
    row = connection.execute(f"SELECT payload_json,receipt_sha256 FROM {MIGRATION_TABLE} WHERE id=1").fetchone()
    if row is None:
        raise ValueError("schema24 migration proof missing")
    proof = json.loads(row[0])
    if digest(proof) != row[1] or proof["target_schema_sha256"] != digest(objects(connection)):
        raise ValueError("schema24 migration structure or proof changed")
    parent = connection.execute("SELECT payload_json,receipt_sha256 FROM four_platform_flow_migrations WHERE id=1").fetchone()
    if parent is None or parent[1] != proof["parent_receipt_sha256"] or digest(json.loads(parent[0])) != parent[1]:
        raise ValueError("schema24 parent schema23 migration proof changed")
    parent_objects = [tuple(value) for value in proof["source_objects"]]
    live_objects = {row[1]: row for row in objects(connection)}
    if any(tuple(live_objects.get(row[1], ())) != row for row in parent_objects):
        raise ValueError("schema24 inherited schema23 object changed")
    if digest(parent_objects) != proof["source_schema_sha256"]:
        raise ValueError("schema24 inherited object proof changed")
    from .capture_work_index import normalize_objects
    if digest(normalize_objects(parent_objects)) != json.loads(parent[0])["target_schema_sha256"]:
        raise ValueError("schema24 source objects differ from immutable schema23 proof")
    additions = _declared_additions()
    expected_objects = {row[1]: row for row in (*parent_objects, *additions)}
    if len(expected_objects) != len(parent_objects) + len(additions) or live_objects != expected_objects:
        raise ValueError("schema24 declared additions structure mismatch")
    state = connection.execute("SELECT (typeof(revision) IN ('integer','real') AND revision>=0),"
        "projection_depth=0 FROM capture_catalog_revision WHERE id=1").fetchone()
    if state is None or tuple(state) != (1, 1):
        raise ValueError("schema24 catalog revision state invalid")


def migration_proof(connection: sqlite3.Connection) -> dict[str, Any]:
    validate_structure(connection)
    row = connection.execute(f"SELECT payload_json,receipt_sha256 FROM {MIGRATION_TABLE} WHERE id=1").fetchone()
    return {**json.loads(row[0]), "receipt_sha256": row[1]}


def migrate(connection: sqlite3.Connection, *, maintenance: MaintenanceContext | None = None) -> dict[str, Any]:
    from .storage import _require_initialization_safety
    from .schema_v22 import _table_digests
    from .schema_v23 import migration_proof as source_proof
    from .schema_v20 import row_digest
    if maintenance is None:
        _require_initialization_safety(connection)
    else:
        _require_maintenance_context(connection, maintenance)
    if connection.in_transaction or connection.execute("PRAGMA query_only").fetchone()[0]:
        raise ValueError("schema24 migration requires idle writable offline connection")
    if connection.execute("PRAGMA user_version").fetchone()[0] == SCHEMA_VERSION:
        return {**migration_proof(connection), "status": "unchanged", "sql_writes": 0, "provider_calls": 0}
    parent = source_proof(connection)
    before, source_objects = _table_digests(connection), objects(connection)
    at = datetime.now(timezone.utc).isoformat()
    try:
        connection.execute("BEGIN IMMEDIATE")
        create_tables(connection)
        for table, expected in before.items():
            if row_digest(connection, table, expected["columns"]) != expected:
                raise ValueError("schema24 modified historical rows: " + table)
        connection.execute("INSERT INTO schema_migrations(version,name,applied_at) VALUES(?,?,?)", (24, MIGRATION_NAME, at))
        connection.execute("PRAGMA user_version=24")
        if connection.execute("PRAGMA foreign_key_check").fetchone():
            raise ValueError("schema24 foreign key violation")
        proof = {"contract": "duplicate-index-migration-v1", "source_version": 23, "target_version": 24,
                 "parent_receipt_sha256": parent["receipt_sha256"], "retained_tables": before,
                 "source_objects": source_objects, "source_schema_sha256": digest(source_objects),
                 "target_schema_sha256": digest(objects(connection)), "applied_at": at,
                 "provider_calls": 0, "backfill_jobs": 0}
        connection.execute(f"INSERT INTO {MIGRATION_TABLE} VALUES(1,?,?,?)", (at, json.dumps(proof, ensure_ascii=False, sort_keys=True, separators=(",", ":")), digest(proof)))
        validate_structure(connection)
        connection.commit()
    except BaseException:
        connection.rollback()
        raise
    return {**migration_proof(connection), "status": "migrated"}
