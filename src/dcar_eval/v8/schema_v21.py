"""Explicit offline schema20 -> 21 account classification migration.

Only a caller-owned candidate is eligible. No provider, roster admission,
activation, status transition or release operation occurs here.
"""
from __future__ import annotations

import hashlib
import json
import os
import sqlite3
from dataclasses import dataclass
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

from .account_classification import classification_from_source

SCHEMA_VERSION = 21
MIGRATION_NAME = "account-classification-v1"
MIGRATION_TABLE = "account_classification_migrations"
_MIGRATION_SQL = (
    "CREATE TABLE account_classification_migrations (id INTEGER PRIMARY KEY CHECK(id=1), "
    "applied_at TEXT NOT NULL, payload_json TEXT NOT NULL CHECK(json_valid(payload_json)), "
    "receipt_sha256 TEXT NOT NULL CHECK(length(receipt_sha256)=64))",
    "CREATE TRIGGER trg_account_classification_migrations_update BEFORE UPDATE ON account_classification_migrations "
    "BEGIN SELECT RAISE(ABORT,'account classification migration is immutable'); END",
    "CREATE TRIGGER trg_account_classification_migrations_delete BEFORE DELETE ON account_classification_migrations "
    "BEGIN SELECT RAISE(ABORT,'account classification migration is immutable'); END",
)


@dataclass(frozen=True)
class MaintenanceContext:
    """An installer must supply its live formal-mutation lease and sealed backup."""

    access: Any
    backup_path: Path
    backup_sha256: str


def _require_maintenance_context(connection: sqlite3.Connection, context: MaintenanceContext) -> None:
    from .runtime_database import (DatabaseAccessMode, FileIdentity,
                                   _PROCESS_WRITER_LEASES, _PROCESS_WRITER_LEASE_GUARD)
    if not isinstance(context, MaintenanceContext) or context.access.access_mode is not DatabaseAccessMode.FORMAL_MUTATION:
        raise ValueError("schema21 installer requires formal-mutation access")
    main = next(row for row in connection.execute("PRAGMA database_list") if row[1] == "main")
    if not main[2] or not os.path.samefile(main[2], context.access.database):
        raise ValueError("schema21 installer database identity changed")
    with _PROCESS_WRITER_LEASE_GUARD:
        leases = [(fd, lease) for fd, (pid, access, lease) in _PROCESS_WRITER_LEASES.items()
                  if pid == os.getpid() and access == context.access]
        if len(leases) != 1:
            raise ValueError("schema21 installer does not own the formal-mutation writer lease")
        fd, lease = leases[0]
        if (FileIdentity.from_stat(os.fstat(fd)) != lease.identity
                or FileIdentity.from_stat(lease.path.lstat()) != lease.identity
                or FileIdentity.from_stat(context.access.database.stat()) != context.access.database_identity):
            raise ValueError("schema21 installer lease identity changed")
    backup = Path(context.backup_path)
    if not backup.is_file() or backup.is_symlink() or os.path.samefile(backup, context.access.database):
        raise ValueError("schema21 installer requires a separate verified backup")
    with backup.open("rb") as source:
        checksum = hashlib.file_digest(source, "sha256").hexdigest()
    if checksum != context.backup_sha256:
        raise ValueError("schema21 installer backup checksum changed")
    if connection.execute("PRAGMA recursive_triggers").fetchone()[0] != 1:
        raise ValueError("schema21 installer requires recursive triggers")


def _digest(value: Any) -> str:
    return hashlib.sha256(json.dumps(value, ensure_ascii=False, sort_keys=True, default=str,
                                    separators=(",", ":")).encode()).hexdigest()


def _columns(connection: sqlite3.Connection, table: str) -> list[str]:
    return [row[1] for row in connection.execute(f"PRAGMA table_info({table})")]


def _structure(connection: sqlite3.Connection) -> dict[str, Any]:
    objects = [tuple(row) for row in connection.execute(
        "SELECT type,name,tbl_name,sql FROM sqlite_master WHERE name NOT LIKE 'sqlite_%' ORDER BY type,name")]
    return {"schema_sha256": _digest(objects), "accounts_columns": _columns(connection, "accounts"),
            "directory_columns": _columns(connection, "account_directory_rows"),
            "table_counts": {name: connection.execute('SELECT COUNT(*) FROM "' + name.replace('"', '""') + '"').fetchone()[0]
                             for kind, name, _, _ in objects if kind == "table"}}


def validate_structure(connection: sqlite3.Connection) -> None:
    if connection.execute("PRAGMA user_version").fetchone()[0] != SCHEMA_VERSION:
        raise ValueError("schema21 version mismatch")
    row = connection.execute("SELECT name FROM schema_migrations WHERE version=21").fetchone()
    if row is None or row[0] != MIGRATION_NAME:
        raise ValueError("schema21 migration manifest mismatch")
    if {"account_type", "content_direction"} & set(_columns(connection, "accounts")):
        raise ValueError("schema21 obsolete account classification columns remain")
    required = {"account_group", "business_direction"}
    if not required <= set(_columns(connection, "account_directory_rows")):
        raise ValueError("schema21 directory classification columns missing")
    # The capture protocol retains the schema20 objects and immutable receipts.
    objects = {row[0] for row in connection.execute("SELECT name FROM sqlite_master")}
    if not {"capture_work_items", "capture_route_assignments", "content_metric_field_facts",
            "provider_raw_blobs", "provider_usage_settlements", "uq_scheduler_root_slot",
            "uq_scheduler_child_slot", "trg_content_identity_value", MIGRATION_TABLE,
            "trg_account_classification_migrations_update", "trg_account_classification_migrations_delete"} <= objects:
        raise ValueError("schema21 inherited capture objects missing")
    if connection.execute("PRAGMA foreign_keys").fetchone()[0] != 1:
        raise ValueError("schema21 foreign keys must be enabled")


def migrate(connection: sqlite3.Connection, *, maintenance: MaintenanceContext | None = None) -> dict[str, Any]:
    from .storage import _require_initialization_safety
    from .schema_v20 import validate_structure as validate_source
    from .account_directory import ensure_account_directory_schema

    if maintenance is None:
        _require_initialization_safety(connection)
    else:
        _require_maintenance_context(connection, maintenance)
    validate_source(connection)
    if connection.in_transaction:
        raise ValueError("schema21 migration requires an idle offline connection")
    before = _structure(connection)
    retained_columns = [name for name in before["accounts_columns"] if name not in {"account_type", "content_direction"}]
    retained_sql = "SELECT " + ",".join(retained_columns) + " FROM accounts ORDER BY id"
    retained_sha = _digest([tuple(row) for row in connection.execute(retained_sql)])
    removed_sha = _digest([tuple(row) for row in connection.execute("SELECT id,account_type,content_direction FROM accounts ORDER BY id")])
    changed_rows = []
    timestamp = datetime.now(timezone.utc).isoformat(timespec="microseconds").replace("+00:00", "Z")
    connection.execute("BEGIN IMMEDIATE")
    try:
        ensure_account_directory_schema(connection)
        columns = set(_columns(connection, "account_directory_rows"))
        if "account_group" not in columns:
            connection.execute("ALTER TABLE account_directory_rows ADD COLUMN account_group TEXT NOT NULL DEFAULT 'unknown' "
                               "CHECK(account_group IN ('unknown','mixed_edit','innovation','image_text','boutique_ip'))")
        if "business_direction" not in columns:
            connection.execute("ALTER TABLE account_directory_rows ADD COLUMN business_direction TEXT NOT NULL DEFAULT 'unknown' "
                               "CHECK(business_direction IN ('unknown','new_car','used_car_c1','used_car_c2','ai_xiaodong'))")
        for row in connection.execute("SELECT id,account_id,raw_json FROM account_directory_rows ORDER BY id").fetchall():
            classification = classification_from_source(json.loads(row["raw_json"]))
            connection.execute("UPDATE account_directory_rows SET account_group=?,business_direction=? WHERE id=?",
                               (classification["account_group"], classification["business_direction"], row["id"]))
            changed_rows.append({"directory_row_id": row["id"], "account_id": row["account_id"], **classification})
        connection.execute("ALTER TABLE accounts DROP COLUMN account_type")
        connection.execute("ALTER TABLE accounts DROP COLUMN content_direction")
        for statement in _MIGRATION_SQL:
            connection.execute(statement)
        if _digest([tuple(row) for row in connection.execute(retained_sql)]) != retained_sha:
            raise ValueError("schema21 retained account data changed")
        connection.execute("INSERT INTO schema_migrations(version,name,applied_at) VALUES (21,?,?)", (MIGRATION_NAME, timestamp))
        connection.execute("PRAGMA user_version=21")
        validate_structure(connection)
        if connection.execute("PRAGMA foreign_key_check").fetchone():
            raise ValueError("schema21 foreign key validation failed")
        after = _structure(connection)
        after["table_counts"][MIGRATION_TABLE] = 1
        for table, count in before["table_counts"].items():
            if table != "schema_migrations" and after["table_counts"].get(table) != count:
                raise ValueError("schema21 unexpected row count change: " + table)
        receipt = {"contract_version": "account-classification-migration-v1", "status": "candidate",
                   "source_version": 20, "target_version": 21, "migration_name": MIGRATION_NAME,
                   "applied_at": timestamp,
                   "before": before, "after": after, "changed_rows": changed_rows,
                   "retained_account_sha256": retained_sha, "removed_account_columns_sha256": removed_sha,
                   "provider_calls": 0, "capture_state_changed": False, "production_acceptance": False}
        receipt["sha256"] = _digest(receipt)
        connection.execute("INSERT INTO account_classification_migrations VALUES (1,?,?,?)",
                           (timestamp, json.dumps(receipt, ensure_ascii=False, sort_keys=True, separators=(",", ":")), receipt["sha256"]))
        migration_proof(connection)
        connection.commit()
        return receipt
    except BaseException:
        connection.rollback()
        raise


def migration_proof(connection: sqlite3.Connection) -> dict[str, Any]:
    """Validate portable migration provenance, independent of later label edits."""
    validate_structure(connection)
    rows = connection.execute("SELECT * FROM account_classification_migrations").fetchall()
    if len(rows) != 1:
        raise ValueError("schema21 migration receipt missing or duplicated")
    row = rows[0]
    receipt = json.loads(row["payload_json"])
    manifest = connection.execute("SELECT applied_at FROM schema_migrations WHERE version=21").fetchone()
    body = dict(receipt)
    checksum = body.pop("sha256", None)
    if (checksum != row["receipt_sha256"] or checksum != _digest(body)
            or receipt.get("contract_version") != "account-classification-migration-v1"
            or receipt.get("source_version") != 20 or receipt.get("target_version") != 21
            or receipt.get("migration_name") != MIGRATION_NAME
            or receipt.get("applied_at") != row["applied_at"] or row["applied_at"] != manifest[0]
            or receipt.get("provider_calls") != 0 or receipt.get("capture_state_changed") is not False
            or {"account_type", "content_direction"} & set(receipt["after"]["accounts_columns"])
            or not {"account_group", "business_direction"} <= set(receipt["after"]["directory_columns"])):
        raise ValueError("schema21 migration receipt is invalid")
    if _structure(connection)["schema_sha256"] != receipt["after"]["schema_sha256"]:
        raise ValueError("schema21 objects differ from the accepted migration structure")
    return {"contract_version": "account-classification-migration-proof-v1", "schema_version": 21,
            "schema_migration": MIGRATION_NAME, "receipt_sha256": checksum, "applied_at": row["applied_at"],
            "source_schema_sha256": receipt["before"]["schema_sha256"],
            "target_schema_sha256": receipt["after"]["schema_sha256"],
            "migrated_directory_rows": len(receipt["changed_rows"])}


def validate_lineage(source: sqlite3.Connection, candidate: sqlite3.Connection) -> dict[str, Any]:
    """Verify an immediate candidate against its pre-migration source backup."""
    from .schema_v20 import validate_structure as validate_source

    validate_source(source)
    proof = migration_proof(candidate)
    receipt = json.loads(candidate.execute("SELECT payload_json FROM account_classification_migrations").fetchone()[0])
    if _structure(source) != receipt["before"]:
        raise ValueError("schema21 lineage source structure or counts changed")
    retained = receipt["after"]["accounts_columns"]
    sql = "SELECT " + ",".join(retained) + " FROM accounts ORDER BY id"
    if (_digest([tuple(row) for row in source.execute(sql)]) != receipt["retained_account_sha256"]
            or _digest([tuple(row) for row in candidate.execute(sql)]) != receipt["retained_account_sha256"]):
        raise ValueError("schema21 lineage retained account data changed")
    if _digest([tuple(row) for row in source.execute("SELECT id,account_type,content_direction FROM accounts ORDER BY id")]) != receipt["removed_account_columns_sha256"]:
        raise ValueError("schema21 lineage removed columns differ from source")
    if "account_directory_rows" in receipt["before"]["table_counts"]:
        retained_directory = [name for name in receipt["before"]["directory_columns"]
                              if name not in {"account_group", "business_direction"}]
        sql = "SELECT " + ",".join(retained_directory) + " FROM account_directory_rows ORDER BY id"
        if [tuple(row) for row in source.execute(sql)] != [tuple(row) for row in candidate.execute(sql)]:
            raise ValueError("schema21 lineage changed retained directory data")
    actual_classifications = [dict(row) for row in candidate.execute(
        "SELECT id directory_row_id,account_id,account_group,business_direction FROM account_directory_rows ORDER BY id")]
    if actual_classifications != receipt["changed_rows"]:
        raise ValueError("schema21 lineage migrated classification values differ")
    if ([tuple(row) for row in source.execute("SELECT * FROM schema_migrations ORDER BY version")]
            != [tuple(row) for row in candidate.execute("SELECT * FROM schema_migrations WHERE version<=20 ORDER BY version")]):
        raise ValueError("schema21 lineage changed historical migration records")
    for table in receipt["before"]["table_counts"]:
        if table in {"accounts", "account_directory_rows", "schema_migrations"}:
            continue
        quoted = '"' + table.replace('"', '""') + '"'
        left = source.execute("SELECT * FROM " + quoted)
        right = candidate.execute("SELECT * FROM " + quoted)
        while True:
            a, b = left.fetchmany(64), right.fetchmany(64)
            if [tuple(row) for row in a] != [tuple(row) for row in b]:
                raise ValueError("schema21 lineage changed retained table: " + table)
            if not a:
                break
    return {**proof, "retained_tables_verified": True}
