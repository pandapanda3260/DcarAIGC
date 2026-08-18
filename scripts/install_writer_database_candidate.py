#!/usr/bin/env python3
"""Atomically install one pre-validated current-schema writer database candidate.

This is a deliberately narrow cutover tool.  It only accepts the repository's
canonical formal database and operator-freeze lock, consumes an explicitly
hashed candidate from the same filesystem, preserves the old database and its
SQLite sidecars with atomic renames, and writes a non-overwritable receipt.
It does not migrate schemas, activate evaluation-v9, start services, use the
network, or invoke providers.
"""

from __future__ import annotations

import argparse
import fcntl
import hashlib
import json
import os
import re
import sqlite3
import stat
import subprocess
import sys
from collections.abc import Callable, Iterator, Mapping, Sequence
from contextlib import contextmanager
from dataclasses import asdict, dataclass
from datetime import datetime, timezone
from pathlib import Path
from typing import Any


PROJECT_ROOT = Path(__file__).resolve().parents[1]
PACKAGE_ROOT = PROJECT_ROOT / "src" / "dcar_eval"
if str(PACKAGE_ROOT) not in sys.path:
    sys.path.insert(0, str(PACKAGE_ROOT))

from v8.storage import (  # noqa: E402
    CURRENT_SCHEMA_MIGRATION_NAME,
    DEFAULT_DB,
    SCHEMA_MIGRATION_NAMES,
    SCHEMA_VERSION,
    _V16_DROPPED_TABLES,
    _V16_EVALUATION_COPY_COLUMNS,
    _V16_REMOVED_INDEXES,
    configure_connection_safety,
    is_formal_database_path,
    require_schema_compatibility,
    same_database_path,
)


FORMAL_DATABASE = DEFAULT_DB
FORMAL_BACKUP_ROOT = FORMAL_DATABASE.parent / "backups"
CANONICAL_OPERATOR_FREEZE_LOCK = PROJECT_ROOT / "runtime" / "operator-freeze.lock"
RECEIPT_SCHEMA = "dcar-writer-database-candidate-install-v1"
MIGRATION_RECEIPT_SCHEMA = "dcar-v16-offline-migration-v1"
BACKUP_RECEIPT_SCHEMA = "dcar-v16-offline-backup-v1"
HEX_SHA256 = re.compile(r"[0-9a-f]{64}", re.ASCII)
SQLITE_SIDECAR_SUFFIXES = ("-wal", "-shm")
SQLITE_TRANSIENT_SUFFIXES = (*SQLITE_SIDECAR_SUFFIXES, "-journal")
EXPECTED_SOURCE_SCHEMA_VERSION = 15
EXPECTED_SOURCE_MIGRATION = SCHEMA_MIGRATION_NAMES[EXPECTED_SOURCE_SCHEMA_VERSION]
EXPECTED_CANDIDATE_SCHEMA_VERSION = 16
EXPECTED_CANDIDATE_MIGRATION = CURRENT_SCHEMA_MIGRATION_NAME
MIGRATION_ADDED_TABLES: frozenset[str] = frozenset()
MIGRATION_REMOVED_TABLES = frozenset(_V16_DROPPED_TABLES)
MIGRATION_REBUILT_TABLE_COLUMNS = {
    "evaluation_versions": tuple(_V16_EVALUATION_COPY_COLUMNS),
}
MIGRATION_APPENDED_VERSIONS = frozenset({EXPECTED_CANDIDATE_SCHEMA_VERSION})
MAX_MIGRATION_RECEIPT_BYTES = 8 * 1024 * 1024
MAX_BACKUP_RECEIPT_BYTES = 64 * 1024
MIGRATION_LOCK_PAYLOAD = b"dcar-v16-offline-migration-lock-v1\n"
if SCHEMA_VERSION != EXPECTED_CANDIDATE_SCHEMA_VERSION:
    raise RuntimeError(
        "writer candidate installer must be reviewed for schema versions after v16"
    )

# Tests inject failures immediately after every durable state transition.  The
# hook is not exposed by the CLI and therefore cannot weaken production checks.
INSTALL_CHECKPOINTS = (
    "after_preflight",
    "after_backup_directory_created",
    "after_source_database_moved",
    "after_source_wal_moved",
    "after_source_shm_moved",
    "after_candidate_installed",
    "after_installed_file_synced",
    "after_directories_synced",
    "after_post_install_verification",
    "after_receipt_written",
)


class CandidateInstallError(RuntimeError):
    """Raised when a writer database candidate cannot be installed safely."""


@dataclass(frozen=True)
class FileIdentity:
    device: int
    inode: int
    link_count: int
    mode: int
    size: int
    mtime_ns: int


class _MigrationLockLease:
    """Keep the lock held while moving its final binding check into commit."""

    def __init__(
        self,
        *,
        identity: FileIdentity,
        verify_binding: Callable[[], None],
    ) -> None:
        self.identity = identity
        self._verify_binding = verify_binding
        self.commit_verified = False
        self.binding_failed = False

    def verify_for_commit(self) -> None:
        if self.commit_verified:
            return
        try:
            self._verify_binding()
        except BaseException:
            self.binding_failed = True
            raise
        self.commit_verified = True


@dataclass(frozen=True)
class MigrationReceiptContract:
    value: dict[str, Any]
    receipt_identity: FileIdentity
    receipt_sha256: str
    source_sha256: str
    candidate_sha256: str
    migration_lock_path: Path
    migration_lock_identity: FileIdentity
    migration_lock_sha256: str
    backup_receipt_path: Path
    backup_receipt_identity: FileIdentity
    backup_receipt_sha256: str
    backup_path: Path
    backup_identity: FileIdentity
    backup_sha256: str
    source_validation: dict[str, Any]
    candidate_validation: dict[str, Any]
    lineage: dict[str, Any]


def _utc_now() -> str:
    return (
        datetime.now(timezone.utc).isoformat(timespec="seconds").replace("+00:00", "Z")
    )


def _sha256_file(path: Path) -> str:
    digest = hashlib.sha256()
    with path.open("rb") as handle:
        for chunk in iter(lambda: handle.read(1024 * 1024), b""):
            digest.update(chunk)
    return digest.hexdigest()


def _require_sha256(value: str, *, label: str) -> str:
    if HEX_SHA256.fullmatch(value) is None:
        raise CandidateInstallError(f"{label} must be a lowercase SHA-256 digest")
    return value


def _stat_identity(path: Path) -> FileIdentity:
    value = path.lstat()
    return FileIdentity(
        device=value.st_dev,
        inode=value.st_ino,
        link_count=value.st_nlink,
        mode=stat.S_IMODE(value.st_mode),
        size=value.st_size,
        mtime_ns=value.st_mtime_ns,
    )


def _identity_from_stat(value: os.stat_result) -> FileIdentity:
    return FileIdentity(
        device=value.st_dev,
        inode=value.st_ino,
        link_count=value.st_nlink,
        mode=stat.S_IMODE(value.st_mode),
        size=value.st_size,
        mtime_ns=value.st_mtime_ns,
    )


def _path_exists(path: Path) -> bool:
    """Return true for regular paths and broken symlinks alike."""

    return path.exists() or path.is_symlink()


def _require_regular_single_link(path: Path, *, label: str) -> FileIdentity:
    try:
        value = path.lstat()
    except FileNotFoundError as error:
        raise CandidateInstallError(f"{label} is missing: {path}") from error
    if stat.S_ISLNK(value.st_mode) or not stat.S_ISREG(value.st_mode):
        raise CandidateInstallError(f"{label} must be a regular non-symlink file")
    if value.st_nlink != 1:
        raise CandidateInstallError(f"{label} must not be hard-linked")
    # Resolve parent aliases (macOS commonly exposes /var through /private/var),
    # while the lstat check above still rejects a symlink at the protected leaf.
    path.resolve(strict=True)
    return _stat_identity(path)


def _require_directory(path: Path, *, label: str) -> Path:
    try:
        value = path.lstat()
    except FileNotFoundError as error:
        raise CandidateInstallError(f"{label} is missing: {path}") from error
    if stat.S_ISLNK(value.st_mode) or not stat.S_ISDIR(value.st_mode):
        raise CandidateInstallError(f"{label} must be a non-symlink directory")
    return path.resolve(strict=True)


def _is_within(path: Path, root: Path) -> bool:
    return path == root or root in path.parents


def _has_ancestor_identity(path: Path, ancestor: Path) -> bool:
    current = path if path.is_dir() else path.parent
    while not _path_exists(current):
        parent = current.parent
        if parent == current:
            return False
        current = parent
    while True:
        try:
            if os.path.samefile(current, ancestor):
                return True
        except OSError:
            pass
        parent = current.parent
        if parent == current:
            return False
        current = parent


def _require_project_external(path: Path, *, label: str) -> Path:
    resolved = path.resolve(strict=path.exists())
    project = PROJECT_ROOT.resolve(strict=True)
    if _is_within(resolved, project) or _has_ancestor_identity(path, project):
        raise CandidateInstallError(f"{label} must be outside the project root")
    return resolved


def _require_same_resolved_path(value: Any, actual: Path, *, label: str) -> None:
    if not isinstance(value, str):
        raise CandidateInstallError(f"{label} is not a path string")
    declared = Path(value)
    if not declared.is_absolute():
        raise CandidateInstallError(f"{label} must be absolute")
    try:
        matches = same_database_path(declared, actual)
    except OSError as error:
        raise CandidateInstallError(f"{label} cannot be resolved") from error
    if not matches:
        raise CandidateInstallError(f"{label} differs")


def _require_secure_lock_parent(path: Path) -> Path:
    parent = _require_directory(path.parent, label="migration lock parent")
    value = parent.stat()
    if value.st_uid != os.geteuid() or stat.S_IMODE(value.st_mode) != 0o700:
        raise CandidateInstallError(
            "migration lock parent must be owned by the current user with mode 0700"
        )
    return parent


def _same_identity(left: FileIdentity, right: FileIdentity) -> bool:
    return left.device == right.device and left.inode == right.inode


def _require_no_traversal(path: Path, *, label: str) -> None:
    if ".." in path.parts:
        raise CandidateInstallError(f"{label} must not contain path traversal")


def _fingerprint(path: Path) -> dict[str, Any]:
    identity = _stat_identity(path)
    return {**asdict(identity), "sha256": _sha256_file(path)}


def _require_exact_keys(
    value: Mapping[str, Any],
    expected: frozenset[str],
    *,
    label: str,
) -> None:
    if set(value) != expected:
        raise CandidateInstallError(f"{label} fields are not the exact contract")


def _require_mapping(value: Any, *, label: str) -> Mapping[str, Any]:
    if not isinstance(value, dict):
        raise CandidateInstallError(f"{label} must be a JSON object")
    return value


def _require_fingerprint(
    value: Any,
    path: Path,
    *,
    label: str,
) -> dict[str, Any]:
    mapping = _require_mapping(value, label=label)
    fields = frozenset(
        {"device", "inode", "link_count", "mode", "size", "mtime_ns", "sha256"}
    )
    _require_exact_keys(mapping, fields, label=label)
    for key in fields - {"sha256"}:
        item = mapping[key]
        if not isinstance(item, int) or isinstance(item, bool):
            raise CandidateInstallError(f"{label}.{key} must be an integer")
    digest = mapping["sha256"]
    if not isinstance(digest, str):
        raise CandidateInstallError(f"{label}.sha256 must be a string")
    _require_sha256(digest, label=f"{label}.sha256")
    actual = _fingerprint(path)
    if dict(mapping) != actual:
        raise CandidateInstallError(f"{label} no longer matches {path}")
    return actual


def _connect_immutable_database(path: Path, *, label: str) -> sqlite3.Connection:
    try:
        connection = sqlite3.connect(
            f"{path.resolve(strict=True).as_uri()}?mode=ro&immutable=1",
            uri=True,
            timeout=10,
        )
        connection.row_factory = sqlite3.Row
        configure_connection_safety(connection)
        connection.execute("PRAGMA query_only=ON")
        return connection
    except (OSError, sqlite3.Error, RuntimeError) as error:
        raise CandidateInstallError(
            f"{label} cannot be opened read-only: {error}"
        ) from error


def _connect_candidate(path: Path) -> sqlite3.Connection:
    return _connect_immutable_database(path, label="candidate database")


def _pragma_rows(connection: sqlite3.Connection, pragma: str) -> list[sqlite3.Row]:
    return list(connection.execute(pragma).fetchall())


def _validate_candidate_database(path: Path) -> dict[str, Any]:
    for suffix in SQLITE_TRANSIENT_SUFFIXES:
        sidecar = Path(f"{path}{suffix}")
        if _path_exists(sidecar):
            raise CandidateInstallError(
                f"candidate database must be self-contained; {suffix} must be absent"
            )

    connection = _connect_candidate(path)
    try:
        quick_rows = [
            str(row[0]) for row in _pragma_rows(connection, "PRAGMA quick_check")
        ]
        if quick_rows != ["ok"]:
            raise CandidateInstallError(
                f"candidate quick_check failed: {json.dumps(quick_rows)}"
            )
        integrity_rows = [
            str(row[0]) for row in _pragma_rows(connection, "PRAGMA integrity_check")
        ]
        if integrity_rows != ["ok"]:
            raise CandidateInstallError(
                f"candidate integrity_check failed: {json.dumps(integrity_rows)}"
            )
        foreign_keys = [
            list(row) for row in _pragma_rows(connection, "PRAGMA foreign_key_check")
        ]
        if foreign_keys:
            raise CandidateInstallError(
                f"candidate has {len(foreign_keys)} foreign-key violations"
            )
        try:
            require_schema_compatibility(
                connection,
                supported_versions=frozenset({EXPECTED_CANDIDATE_SCHEMA_VERSION}),
            )
        except Exception as error:
            raise CandidateInstallError(
                f"candidate schema compatibility failed: {error}"
            ) from error
        user_version = int(connection.execute("PRAGMA user_version").fetchone()[0])
        migration_rows = connection.execute(
            "SELECT version,name FROM schema_migrations WHERE version=?",
            (EXPECTED_CANDIDATE_SCHEMA_VERSION,),
        ).fetchall()
        maximum = connection.execute(
            "SELECT MAX(version) FROM schema_migrations"
        ).fetchone()[0]
        if (
            user_version != EXPECTED_CANDIDATE_SCHEMA_VERSION
            or len(migration_rows) != 1
            or int(migration_rows[0]["version"])
            != EXPECTED_CANDIDATE_SCHEMA_VERSION
            or str(migration_rows[0]["name"]) != EXPECTED_CANDIDATE_MIGRATION
            or int(maximum) != EXPECTED_CANDIDATE_SCHEMA_VERSION
        ):
            raise CandidateInstallError(
                "candidate current-schema migration identity is not exact"
            )

        return {
            "quick_check": "ok",
            "integrity_check": "ok",
            "foreign_key_violation_count": 0,
            "schema_version": user_version,
            "schema_migration": str(migration_rows[0]["name"]),
        }
    except sqlite3.Error as error:
        raise CandidateInstallError(
            f"candidate database validation failed: {error}"
        ) from error
    finally:
        connection.close()


def _quote_identifier(value: str) -> str:
    return '"' + value.replace('"', '""') + '"'


def _table_names(connection: sqlite3.Connection) -> set[str]:
    return {
        str(row[0])
        for row in connection.execute(
            """
            SELECT name FROM sqlite_master
            WHERE type='table' AND name NOT LIKE 'sqlite_%'
            """
        ).fetchall()
    }


def _table_columns(connection: sqlite3.Connection, table: str) -> list[str]:
    quoted = _quote_identifier(table)
    return [
        str(row["name"])
        for row in connection.execute(f"PRAGMA table_info({quoted})").fetchall()
    ]


def _table_projection_sha256(
    connection: sqlite3.Connection,
    table: str,
    columns: list[str],
) -> str:
    """Hash a table projection using the same stable encoding as migrations."""

    quoted_table = _quote_identifier(table)
    quoted_columns = ",".join(_quote_identifier(column) for column in columns)
    primary = [
        (int(row["pk"]), str(row["name"]))
        for row in connection.execute(f"PRAGMA table_info({quoted_table})").fetchall()
        if int(row["pk"])
    ]
    order_columns = [column for _, column in sorted(primary)] or columns
    order = ",".join(_quote_identifier(column) for column in order_columns)
    digest = hashlib.sha256()
    for row in connection.execute(
        f"SELECT {quoted_columns} FROM {quoted_table} ORDER BY {order}"
    ):
        digest.update(
            json.dumps(
                list(row),
                ensure_ascii=False,
                separators=(",", ":"),
                default=lambda value: {"bytes_hex": bytes(value).hex()},
            ).encode("utf-8")
        )
        digest.update(b"\n")
    return digest.hexdigest()


def _schema_migration_rows(
    connection: sqlite3.Connection,
) -> list[tuple[int, str, str]]:
    return [
        (int(row["version"]), str(row["name"]), str(row["applied_at"]))
        for row in connection.execute(
            "SELECT version,name,applied_at FROM schema_migrations ORDER BY version"
        ).fetchall()
    ]


def _sequence_state(connection: sqlite3.Connection) -> dict[str, int]:
    return {
        str(row["name"]): int(row["seq"])
        for row in connection.execute(
            "SELECT name,seq FROM sqlite_sequence ORDER BY name"
        )
    }


def _projection_manifest(connection: sqlite3.Connection) -> dict[str, Any]:
    tables = _table_names(connection)
    results: dict[str, dict[str, Any]] = {}
    aggregate = hashlib.sha256()
    for table in sorted(tables - {"schema_migrations"}):
        columns = _table_columns(connection, table)
        if table == "evaluation_versions":
            columns = [
                column
                for column in _V16_EVALUATION_COPY_COLUMNS
                if column in columns
            ]
        quoted = _quote_identifier(table)
        count = int(connection.execute(f"SELECT COUNT(*) FROM {quoted}").fetchone()[0])
        digest = _table_projection_sha256(connection, table, columns)
        results[table] = {
            "row_count": count,
            "projection_sha256": digest,
            "projection_columns": columns,
        }
        aggregate.update(table.encode("utf-8"))
        aggregate.update(b"\0")
        aggregate.update(str(count).encode("ascii"))
        aggregate.update(b"\0")
        aggregate.update(digest.encode("ascii"))
        aggregate.update(b"\n")
    manual_count = int(
        connection.execute(
            "SELECT COUNT(*) FROM evaluation_versions "
            "WHERE evaluation_source='manual_review'"
        ).fetchone()[0]
    )
    return {
        "tables": results,
        "aggregate_sha256": aggregate.hexdigest(),
        "evaluation_manual_review_count": manual_count,
        "sqlite_sequence": _sequence_state(connection),
        "schema_migrations": [
            list(row) for row in _schema_migration_rows(connection)
        ],
    }


def _validate_source_database(path: Path) -> dict[str, Any]:
    connection = _connect_immutable_database(path, label="formal source database")
    try:
        quick = [str(row[0]) for row in connection.execute("PRAGMA quick_check")]
        integrity = [
            str(row[0]) for row in connection.execute("PRAGMA integrity_check")
        ]
        foreign_keys = list(connection.execute("PRAGMA foreign_key_check"))
        if quick != ["ok"] or integrity != ["ok"]:
            raise CandidateInstallError(
                "formal source database integrity validation failed"
            )
        if foreign_keys:
            raise CandidateInstallError(
                "formal source database has foreign-key violations: "
                f"{len(foreign_keys)}"
            )
        try:
            version = require_schema_compatibility(
                connection,
                supported_versions=frozenset({EXPECTED_SOURCE_SCHEMA_VERSION}),
            )
        except Exception as error:
            raise CandidateInstallError(
                f"formal source schema compatibility failed: {error}"
            ) from error
        migrations = _schema_migration_rows(connection)
        matches = [row for row in migrations if row[0] == EXPECTED_SOURCE_SCHEMA_VERSION]
        if (
            version != EXPECTED_SOURCE_SCHEMA_VERSION
            or len(matches) != 1
            or matches[0][1] != EXPECTED_SOURCE_MIGRATION
            or max((row[0] for row in migrations), default=0)
            != EXPECTED_SOURCE_SCHEMA_VERSION
        ):
            raise CandidateInstallError("formal source schema identity is not exact v15")
        return {
            "quick_check": "ok",
            "integrity_check": "ok",
            "foreign_key_violation_count": 0,
            "schema_version": version,
            "schema_migration": EXPECTED_SOURCE_MIGRATION,
        }
    except sqlite3.Error as error:
        raise CandidateInstallError(
            f"formal source database validation failed: {error}"
        ) from error
    finally:
        connection.close()


def _validate_source_candidate_lineage(
    source_path: Path,
    candidate_path: Path,
) -> dict[str, Any]:
    """Prove the exact, lossless v15-to-v16 migration contract."""

    source = _connect_immutable_database(source_path, label="formal source database")
    candidate = _connect_candidate(candidate_path)
    try:
        source_manifest = _projection_manifest(source)
        candidate_manifest = _projection_manifest(candidate)
        source_tables = set(source_manifest["tables"])
        candidate_tables = set(candidate_manifest["tables"])
        actual_removals = source_tables - candidate_tables
        actual_additions = candidate_tables - source_tables
        if (
            actual_removals != MIGRATION_REMOVED_TABLES
            or actual_additions != MIGRATION_ADDED_TABLES
        ):
            raise CandidateInstallError(
                "candidate table lineage differs from formal source: "
                f"removed={sorted(actual_removals)},"
                f"expected_removed={sorted(MIGRATION_REMOVED_TABLES)},"
                f"added={sorted(actual_additions)},"
                f"expected_added={sorted(MIGRATION_ADDED_TABLES)}"
            )

        source_columns = _table_columns(source, "evaluation_versions")
        candidate_columns = _table_columns(candidate, "evaluation_versions")
        expected_rebuilt_columns = list(_V16_EVALUATION_COPY_COLUMNS)
        if (
            candidate_columns != expected_rebuilt_columns
            or set(source_columns) - set(expected_rebuilt_columns)
            != {"review_id", "pending_review"}
            or set(expected_rebuilt_columns) - set(source_columns)
        ):
            raise CandidateInstallError(
                "evaluation_versions is not the sole declared v16 rebuild"
            )

        retained: dict[str, Any] = {}
        for table in sorted(candidate_tables):
            before = source_manifest["tables"][table]
            after = candidate_manifest["tables"][table]
            if table != "evaluation_versions" and (
                _table_columns(source, table) != _table_columns(candidate, table)
            ):
                raise CandidateInstallError(
                    f"candidate changed columns for retained table {table}"
                )
            if before != after:
                raise CandidateInstallError(
                    f"candidate changed retained table projection: {table}"
                )
            retained[table] = before

        if (
            source_manifest["evaluation_manual_review_count"]
            != candidate_manifest["evaluation_manual_review_count"]
        ):
            raise CandidateInstallError(
                "candidate changed manual_review evaluation row count"
            )
        expected_sequences = {
            name: value
            for name, value in source_manifest["sqlite_sequence"].items()
            if name not in MIGRATION_REMOVED_TABLES
        }
        if candidate_manifest["sqlite_sequence"] != expected_sequences:
            raise CandidateInstallError(
                "candidate changed retained AUTOINCREMENT sequences"
            )

        source_migrations = [
            tuple(row) for row in source_manifest["schema_migrations"]
        ]
        candidate_migrations = [
            tuple(row) for row in candidate_manifest["schema_migrations"]
        ]
        if candidate_migrations[:-1] != source_migrations:
            raise CandidateInstallError(
                "candidate changed pre-existing schema migration records"
            )
        appended_versions = {
            int(row[0]) for row in candidate_migrations[len(source_migrations) :]
        }
        if (
            len(candidate_migrations) != len(source_migrations) + 1
            or appended_versions != MIGRATION_APPENDED_VERSIONS
            or candidate_migrations[-1][1] != EXPECTED_CANDIDATE_MIGRATION
        ):
            raise CandidateInstallError(
                "candidate must append exactly the v16 migration record"
            )

        object_names = {
            str(row[0])
            for row in candidate.execute(
                "SELECT name FROM sqlite_master WHERE type IN ('table','index')"
            )
        }
        leftovers = object_names & (
            MIGRATION_REMOVED_TABLES | frozenset(_V16_REMOVED_INDEXES)
        )
        if leftovers:
            raise CandidateInstallError(
                f"candidate retained removed review objects: {sorted(leftovers)}"
            )
        schema_objects = _validate_schema_object_lineage(source, candidate)

        return {
            "source_table_count": len(source_tables),
            "candidate_table_count": len(candidate_tables),
            "retained_table_count": len(retained),
            "retained_tables": retained,
            "added_tables": [],
            "source_projection_aggregate_sha256": source_manifest[
                "aggregate_sha256"
            ],
            "candidate_projection_aggregate_sha256": candidate_manifest[
                "aggregate_sha256"
            ],
            "evaluation_versions": retained["evaluation_versions"],
            "manual_review_row_count": candidate_manifest[
                "evaluation_manual_review_count"
            ],
            "sqlite_sequence": candidate_manifest["sqlite_sequence"],
            "appended_migration_versions": sorted(MIGRATION_APPENDED_VERSIONS),
            "removed_tables": sorted(MIGRATION_REMOVED_TABLES),
            "removed_indexes": sorted(_V16_REMOVED_INDEXES),
            "schema_objects": schema_objects,
        }
    except sqlite3.Error as error:
        raise CandidateInstallError(
            f"source/candidate lineage validation failed: {error}"
        ) from error
    finally:
        candidate.close()
        source.close()


def _schema_object_rows(
    connection: sqlite3.Connection,
) -> dict[tuple[str, str], tuple[str, str, str, str]]:
    rows = {
        (str(row[0]), str(row[1])): (
            str(row[0]),
            str(row[1]),
            str(row[2]),
            str(row[3] or ""),
        )
        for row in connection.execute(
            "SELECT type,name,tbl_name,sql FROM sqlite_master "
            "WHERE name NOT LIKE 'sqlite_%' ORDER BY type,name"
        )
    }
    return rows


def _schema_object_manifest(connection: sqlite3.Connection) -> dict[str, Any]:
    rows = [list(row) for row in _schema_object_rows(connection).values()]
    payload = json.dumps(
        rows,
        ensure_ascii=False,
        separators=(",", ":"),
    ).encode("utf-8")
    return {
        "object_count": len(rows),
        "sha256": hashlib.sha256(payload).hexdigest(),
    }


def _validate_schema_object_lineage(
    source: sqlite3.Connection,
    candidate: sqlite3.Connection,
) -> dict[str, Any]:
    source_objects = _schema_object_rows(source)
    candidate_objects = _schema_object_rows(candidate)
    source_keys = set(source_objects)
    candidate_keys = set(candidate_objects)
    expected_removed_names = set(MIGRATION_REMOVED_TABLES) | set(
        _V16_REMOVED_INDEXES
    )
    expected_removed = {
        key for key in source_keys if key[1] in expected_removed_names
    }
    actual_removed = source_keys - candidate_keys
    actual_added = candidate_keys - source_keys
    actual_changed = {
        key
        for key in source_keys & candidate_keys
        if source_objects[key] != candidate_objects[key]
    }
    expected_changed = {("table", "evaluation_versions")}
    if (
        {key[1] for key in expected_removed} != expected_removed_names
        or actual_removed != expected_removed
        or actual_added
        or actual_changed != expected_changed
    ):
        raise CandidateInstallError(
            "candidate schema object lineage is not the exact v15-to-v16 delta: "
            f"removed={sorted(actual_removed)},"
            f"added={sorted(actual_added)},"
            f"changed={sorted(actual_changed)}"
        )
    return {
        "source": _schema_object_manifest(source),
        "candidate": _schema_object_manifest(candidate),
        "added_objects": [],
        "removed_objects": [
            f"{object_type}:{name}"
            for object_type, name in sorted(actual_removed)
        ],
        "changed_objects": [
            f"{object_type}:{name}"
            for object_type, name in sorted(actual_changed)
        ],
    }


def _validate_backup_lineage(
    source_path: Path,
    backup_path: Path,
) -> dict[str, Any]:
    source = _connect_immutable_database(source_path, label="formal source database")
    backup = _connect_immutable_database(backup_path, label="verified backup database")
    try:
        source_manifest = _projection_manifest(source)
        backup_manifest = _projection_manifest(backup)
        if backup_manifest != source_manifest:
            raise CandidateInstallError(
                "verified backup data projection differs from formal source"
            )
        source_schema = _schema_object_manifest(source)
        backup_schema = _schema_object_manifest(backup)
        if backup_schema != source_schema:
            raise CandidateInstallError(
                "verified backup schema objects differ from formal source"
            )
        return {
            "table_count": len(source_manifest["tables"]),
            "projection_aggregate_sha256": source_manifest["aggregate_sha256"],
            "schema_objects": source_schema,
            "sqlite_sequence": source_manifest["sqlite_sequence"],
            "schema_migrations": source_manifest["schema_migrations"],
        }
    finally:
        backup.close()
        source.close()


def _read_json_object(path: Path, *, label: str, maximum_bytes: int) -> dict[str, Any]:
    identity = _require_regular_single_link(path, label=label)
    if identity.size > maximum_bytes:
        raise CandidateInstallError(f"{label} is unexpectedly large")
    try:
        value = json.loads(path.read_text(encoding="utf-8"))
    except (OSError, UnicodeError, json.JSONDecodeError) as error:
        raise CandidateInstallError(f"{label} is not valid UTF-8 JSON") from error
    if not isinstance(value, dict):
        raise CandidateInstallError(f"{label} must be a JSON object")
    return value


def _validate_backup_receipt(
    *,
    receipt_path: Path,
    expected_receipt_sha256: str,
    source_database: Path,
    source_sha256: str,
    backup_path: Path,
    backup_sha256: str,
    migration_lock: Path,
) -> dict[str, Any]:
    if _sha256_file(receipt_path) != expected_receipt_sha256:
        raise CandidateInstallError("backup receipt SHA-256 changed")
    value = _read_json_object(
        receipt_path,
        label="backup receipt",
        maximum_bytes=MAX_BACKUP_RECEIPT_BYTES,
    )
    fields = frozenset(
        {
            "schema_version",
            "source_path",
            "source_sha256",
            "source_byte_size",
            "source_schema_version",
            "source_schema_migration",
            "backup_path",
            "backup_sha256",
            "backup_byte_size",
            "restore_verified",
            "quick_check",
            "integrity_check",
            "foreign_key_violation_count",
            "migration_lock",
            "migration_lock_file",
        }
    )
    _require_exact_keys(value, fields, label="backup receipt")
    _require_same_resolved_path(
        value["source_path"],
        source_database,
        label="backup receipt source path",
    )
    _require_same_resolved_path(
        value["backup_path"],
        backup_path,
        label="backup receipt backup path",
    )
    _require_same_resolved_path(
        value["migration_lock"],
        migration_lock,
        label="backup receipt migration lock path",
    )
    _require_fingerprint(
        value["migration_lock_file"],
        migration_lock,
        label="backup receipt migration lock file",
    )
    expected = {
        "schema_version": BACKUP_RECEIPT_SCHEMA,
        "source_sha256": source_sha256,
        "source_byte_size": source_database.stat().st_size,
        "source_schema_version": EXPECTED_SOURCE_SCHEMA_VERSION,
        "source_schema_migration": EXPECTED_SOURCE_MIGRATION,
        "backup_sha256": backup_sha256,
        "backup_byte_size": backup_path.stat().st_size,
        "restore_verified": True,
        "quick_check": "ok",
        "integrity_check": "ok",
        "foreign_key_violation_count": 0,
    }
    for key, expected_value in expected.items():
        if value[key] != expected_value:
            raise CandidateInstallError(
                f"backup receipt {key} no longer matches its databases"
            )
    return value


@contextmanager
def _exclusive_existing_migration_lock(
    path: Path,
) -> Iterator[_MigrationLockLease]:
    expected_identity = _require_regular_single_link(path, label="migration lock")
    _require_project_external(path, label="migration lock")
    if expected_identity.mode != 0o600:
        raise CandidateInstallError("migration lock permissions must be 0600")
    parent = _require_secure_lock_parent(path)
    parent_descriptor = os.open(
        parent,
        os.O_RDONLY
        | os.O_CLOEXEC
        | getattr(os, "O_DIRECTORY", 0)
        | getattr(os, "O_NOFOLLOW", 0),
    )
    descriptor = -1
    parent_locked = False
    file_locked = False
    try:
        try:
            fcntl.flock(parent_descriptor, fcntl.LOCK_EX | fcntl.LOCK_NB)
        except BlockingIOError as error:
            raise CandidateInstallError(
                "offline migration lock is still held"
            ) from error
        parent_locked = True
        parent_value = os.fstat(parent_descriptor)
        current_parent = parent.stat()
        if (parent_value.st_dev, parent_value.st_ino) != (
            current_parent.st_dev,
            current_parent.st_ino,
        ):
            raise CandidateInstallError("migration lock parent identity changed")
        descriptor = os.open(
            path.name,
            os.O_RDWR | os.O_CLOEXEC | getattr(os, "O_NOFOLLOW", 0),
            dir_fd=parent_descriptor,
        )
        try:
            fcntl.flock(descriptor, fcntl.LOCK_EX | fcntl.LOCK_NB)
        except BlockingIOError as error:
            raise CandidateInstallError(
                "offline migration lock is still held"
            ) from error
        file_locked = True
        value = os.fstat(descriptor)
        path_value = os.stat(
            path.name,
            dir_fd=parent_descriptor,
            follow_symlinks=False,
        )
        identity = _identity_from_stat(value)
        if (
            not stat.S_ISREG(value.st_mode)
            or value.st_nlink != 1
            or value.st_uid != os.geteuid()
            or not _same_identity(identity, expected_identity)
            or (value.st_dev, value.st_ino) != (path_value.st_dev, path_value.st_ino)
        ):
            raise CandidateInstallError("migration lock identity contract differs")
        os.lseek(descriptor, 0, os.SEEK_SET)
        if os.read(descriptor, len(MIGRATION_LOCK_PAYLOAD) + 1) != MIGRATION_LOCK_PAYLOAD:
            raise CandidateInstallError("migration lock content contract differs")
        lease = _MigrationLockLease(
            identity=identity,
            verify_binding=lambda: _assert_existing_lock_binding(
                path,
                parent,
                parent_descriptor,
                descriptor,
            ),
        )
        try:
            yield lease
        except BaseException as body_error:
            if not lease.binding_failed:
                try:
                    _assert_existing_lock_binding(
                        path,
                        parent,
                        parent_descriptor,
                        descriptor,
                    )
                except BaseException as binding_error:
                    raise CandidateInstallError(
                        "migration lock binding failed during recovery: "
                        f"{binding_error}"
                    ) from body_error
            raise
        if not lease.commit_verified:
            lease.verify_for_commit()
    finally:
        try:
            if file_locked:
                fcntl.flock(descriptor, fcntl.LOCK_UN)
        finally:
            if descriptor >= 0:
                os.close(descriptor)
            try:
                if parent_locked:
                    fcntl.flock(parent_descriptor, fcntl.LOCK_UN)
            finally:
                os.close(parent_descriptor)


def _assert_existing_lock_binding(
    path: Path,
    parent: Path,
    parent_descriptor: int,
    descriptor: int,
) -> None:
    parent_fd_value = os.fstat(parent_descriptor)
    parent_path_value = parent.stat()
    if (parent_fd_value.st_dev, parent_fd_value.st_ino) != (
        parent_path_value.st_dev,
        parent_path_value.st_ino,
    ):
        raise CandidateInstallError("migration lock parent identity changed")
    file_fd_value = os.fstat(descriptor)
    file_path_value = os.stat(
        path.name,
        dir_fd=parent_descriptor,
        follow_symlinks=False,
    )
    if (file_fd_value.st_dev, file_fd_value.st_ino) != (
        file_path_value.st_dev,
        file_path_value.st_ino,
    ):
        raise CandidateInstallError("migration lock path identity changed")


def _migration_lock_from_receipt(
    migration_receipt: Path,
    *,
    expected_sha256: str,
) -> Path:
    _require_regular_single_link(migration_receipt, label="migration receipt")
    _require_project_external(migration_receipt, label="migration receipt")
    if _sha256_file(migration_receipt) != expected_sha256:
        raise CandidateInstallError("migration receipt SHA-256 does not match expectation")
    value = _read_json_object(
        migration_receipt,
        label="migration receipt",
        maximum_bytes=MAX_MIGRATION_RECEIPT_BYTES,
    )
    raw_path = value.get("migration_lock")
    if not isinstance(raw_path, str):
        raise CandidateInstallError("migration receipt migration_lock is invalid")
    path = Path(raw_path)
    if not path.is_absolute():
        raise CandidateInstallError("migration receipt migration_lock is not absolute")
    _require_regular_single_link(path, label="migration lock")
    _require_project_external(path, label="migration lock")
    _require_fingerprint(
        value.get("migration_lock_file"),
        path,
        label="migration receipt migration lock file",
    )
    return path


def _load_migration_receipt_contract(
    *,
    migration_receipt: Path,
    expected_migration_receipt_sha256: str,
    formal_database: Path,
    candidate: Path,
    freeze_lock: Path,
    held_migration_lock: Path,
    held_migration_lock_identity: FileIdentity,
) -> MigrationReceiptContract:
    receipt_identity = _require_regular_single_link(
        migration_receipt,
        label="migration receipt",
    )
    _require_project_external(migration_receipt, label="migration receipt")
    receipt_sha256 = _sha256_file(migration_receipt)
    if receipt_sha256 != expected_migration_receipt_sha256:
        raise CandidateInstallError("migration receipt SHA-256 does not match expectation")
    value = _read_json_object(
        migration_receipt,
        label="migration receipt",
        maximum_bytes=MAX_MIGRATION_RECEIPT_BYTES,
    )
    fields = frozenset(
        {
            "schema_version",
            "status",
            "completed_at",
            "from_version",
            "from_migration",
            "to_version",
            "to_migration",
            "formal_source",
            "verified_backup",
            "candidate",
            "lineage",
            "operator_freeze_lock",
            "migration_lock",
            "migration_lock_file",
            "database_handles",
            "receipt",
        }
    )
    _require_exact_keys(value, fields, label="migration receipt")
    expected_scalars = {
        "schema_version": MIGRATION_RECEIPT_SCHEMA,
        "status": "candidate_ready",
        "from_version": EXPECTED_SOURCE_SCHEMA_VERSION,
        "from_migration": EXPECTED_SOURCE_MIGRATION,
        "to_version": EXPECTED_CANDIDATE_SCHEMA_VERSION,
        "to_migration": EXPECTED_CANDIDATE_MIGRATION,
        "database_handles": [],
    }
    for key, expected in expected_scalars.items():
        if value[key] != expected:
            raise CandidateInstallError(f"migration receipt {key} is not exact")
    if not isinstance(value["completed_at"], str) or not value["completed_at"]:
        raise CandidateInstallError("migration receipt completed_at is invalid")
    for key, actual_path in (
        ("operator_freeze_lock", freeze_lock),
        ("receipt", migration_receipt),
    ):
        _require_same_resolved_path(
            value[key],
            actual_path,
            label=f"migration receipt {key}",
        )

    _require_same_resolved_path(
        value["migration_lock"],
        held_migration_lock,
        label="migration receipt migration_lock",
    )
    _require_fingerprint(
        value["migration_lock_file"],
        held_migration_lock,
        label="migration receipt migration_lock_file",
    )
    if _stat_identity(held_migration_lock) != held_migration_lock_identity:
        raise CandidateInstallError("held migration lock path changed")

    source = _require_mapping(value["formal_source"], label="formal_source")
    _require_exact_keys(
        source,
        frozenset({"path", "file", "sidecars", "validation"}),
        label="formal_source",
    )
    _require_same_resolved_path(
        source["path"],
        formal_database,
        label="migration receipt formal source path",
    )
    source_file = _require_fingerprint(
        source["file"],
        formal_database,
        label="formal_source.file",
    )
    source_validation = _validate_source_database(formal_database)
    if source["validation"] != source_validation:
        raise CandidateInstallError("migration receipt formal validation differs")
    sidecars = _require_mapping(source["sidecars"], label="formal_source.sidecars")
    actual_sidecars: dict[str, dict[str, Any]] = {}
    for suffix in SQLITE_TRANSIENT_SUFFIXES:
        sidecar = Path(f"{formal_database}{suffix}")
        if _path_exists(sidecar):
            actual_sidecars[suffix] = _fingerprint(sidecar)
    if dict(sidecars) != actual_sidecars:
        raise CandidateInstallError("migration receipt formal sidecars differ")

    candidate_value = _require_mapping(value["candidate"], label="candidate")
    _require_exact_keys(
        candidate_value,
        frozenset({"path", "file", "validation"}),
        label="candidate",
    )
    _require_same_resolved_path(
        candidate_value["path"],
        candidate,
        label="migration receipt candidate path",
    )
    candidate_file = _require_fingerprint(
        candidate_value["file"],
        candidate,
        label="candidate.file",
    )
    candidate_validation = _validate_candidate_database(candidate)
    if candidate_value["validation"] != candidate_validation:
        raise CandidateInstallError("migration receipt candidate validation differs")

    lineage = _validate_source_candidate_lineage(formal_database, candidate)
    if value["lineage"] != lineage:
        raise CandidateInstallError("migration receipt lineage differs from databases")

    verified_backup = _require_mapping(
        value["verified_backup"],
        label="verified_backup",
    )
    _require_exact_keys(
        verified_backup,
        frozenset(
            {
                "receipt_path",
                "receipt_sha256",
                "path",
                "file",
                "database",
                "validation",
                "lineage",
            }
        ),
        label="verified_backup",
    )
    receipt_path_value = verified_backup["receipt_path"]
    backup_path_value = verified_backup["path"]
    if not isinstance(receipt_path_value, str) or not isinstance(
        backup_path_value, str
    ):
        raise CandidateInstallError("migration receipt backup paths are invalid")
    backup_receipt_path = Path(receipt_path_value)
    backup_path = Path(backup_path_value)
    for path, label in (
        (backup_receipt_path, "backup receipt"),
        (backup_path, "verified backup"),
    ):
        if not path.is_absolute():
            raise CandidateInstallError(f"migration receipt {label} path is not canonical")
        _require_project_external(path, label=label)
    backup_receipt_identity = _require_regular_single_link(
        backup_receipt_path,
        label="backup receipt",
    )
    backup_identity = _require_regular_single_link(
        backup_path,
        label="verified backup",
    )
    _require_fingerprint(
        verified_backup["file"],
        backup_receipt_path,
        label="verified_backup.file",
    )
    _require_fingerprint(
        verified_backup["database"],
        backup_path,
        label="verified_backup.database",
    )
    backup_receipt_sha256 = _sha256_file(backup_receipt_path)
    if verified_backup["receipt_sha256"] != backup_receipt_sha256:
        raise CandidateInstallError("migration receipt backup receipt SHA differs")
    backup_sha256 = _sha256_file(backup_path)
    _validate_backup_receipt(
        receipt_path=backup_receipt_path,
        expected_receipt_sha256=backup_receipt_sha256,
        source_database=formal_database,
        source_sha256=str(source_file["sha256"]),
        backup_path=backup_path,
        backup_sha256=backup_sha256,
        migration_lock=held_migration_lock,
    )
    backup_validation = _validate_source_database(backup_path)
    if verified_backup["validation"] != backup_validation:
        raise CandidateInstallError("migration receipt backup validation differs")
    backup_lineage = _validate_backup_lineage(formal_database, backup_path)
    if verified_backup["lineage"] != backup_lineage:
        raise CandidateInstallError("migration receipt backup lineage differs")

    return MigrationReceiptContract(
        value=value,
        receipt_identity=receipt_identity,
        receipt_sha256=receipt_sha256,
        source_sha256=str(source_file["sha256"]),
        candidate_sha256=str(candidate_file["sha256"]),
        migration_lock_path=held_migration_lock,
        migration_lock_identity=held_migration_lock_identity,
        migration_lock_sha256=_sha256_file(held_migration_lock),
        backup_receipt_path=backup_receipt_path,
        backup_receipt_identity=backup_receipt_identity,
        backup_receipt_sha256=backup_receipt_sha256,
        backup_path=backup_path,
        backup_identity=backup_identity,
        backup_sha256=backup_sha256,
        source_validation=source_validation,
        candidate_validation=candidate_validation,
        lineage=lineage,
    )


def _assert_migration_contract_files_unchanged(
    contract: MigrationReceiptContract,
    *,
    migration_receipt: Path,
) -> None:
    checks = (
        (
            contract.migration_lock_path,
            contract.migration_lock_identity,
            contract.migration_lock_sha256,
            "migration lock",
        ),
        (
            migration_receipt,
            contract.receipt_identity,
            contract.receipt_sha256,
            "migration receipt",
        ),
        (
            contract.backup_receipt_path,
            contract.backup_receipt_identity,
            contract.backup_receipt_sha256,
            "backup receipt",
        ),
        (
            contract.backup_path,
            contract.backup_identity,
            contract.backup_sha256,
            "verified backup",
        ),
    )
    for path, identity, digest, label in checks:
        if (
            not _path_exists(path)
            or _stat_identity(path) != identity
            or _sha256_file(path) != digest
        ):
            raise CandidateInstallError(f"{label} changed before installation")


def _database_handles(databases: Sequence[Path]) -> list[dict[str, Any]]:
    targets: list[str] = []
    for database in databases:
        for path in (
            database,
            *(Path(f"{database}{suffix}") for suffix in SQLITE_TRANSIENT_SUFFIXES),
        ):
            if _path_exists(path):
                targets.append(str(path))
    if not targets:
        return []
    try:
        result = subprocess.run(
            ["lsof", "-nP", *targets],
            check=False,
            capture_output=True,
            text=True,
        )
    except OSError as error:
        raise CandidateInstallError(
            "cannot verify database writer handles with lsof"
        ) from error
    if result.returncode not in {0, 1}:
        raise CandidateInstallError(
            "cannot verify database writer handles with lsof: "
            f"{result.stderr.strip() or 'lsof failed'}"
        )
    handles: list[dict[str, Any]] = []
    for line in result.stdout.splitlines()[1:]:
        parts = line.split()
        if len(parts) < 9:
            continue
        descriptor = parts[3]
        descriptor_match = re.fullmatch(r"\d+([rwu]).*", descriptor)
        if descriptor_match is not None and descriptor_match.group(1) in {
            "r",
            "u",
            "w",
        }:
            try:
                pid = int(parts[1])
            except ValueError:
                pid = -1
            handles.append(
                {
                    "command": parts[0],
                    "pid": pid,
                    "descriptor": descriptor,
                    "path": parts[-1],
                }
            )
    return handles


def _fsync_file(path: Path) -> None:
    descriptor = os.open(path, os.O_RDONLY)
    try:
        os.fsync(descriptor)
    finally:
        os.close(descriptor)


def _fsync_directory(path: Path) -> None:
    flags = os.O_RDONLY | getattr(os, "O_DIRECTORY", 0)
    descriptor = os.open(path, flags)
    try:
        os.fsync(descriptor)
    finally:
        os.close(descriptor)


def _write_json_exclusive(
    path: Path,
    value: Mapping[str, Any],
    *,
    on_created: Callable[[FileIdentity], None] | None = None,
) -> None:
    payload = (
        json.dumps(value, ensure_ascii=False, indent=2, sort_keys=True) + "\n"
    ).encode("utf-8")
    flags = os.O_WRONLY | os.O_CREAT | os.O_EXCL | getattr(os, "O_NOFOLLOW", 0)
    descriptor = os.open(path, flags, 0o600)
    try:
        if on_created is not None:
            on_created(_identity_from_stat(os.fstat(descriptor)))
        written = 0
        while written < len(payload):
            count = os.write(descriptor, payload[written:])
            if count <= 0:
                raise OSError("receipt write made no progress")
            written += count
        os.fsync(descriptor)
    finally:
        os.close(descriptor)
    _fsync_directory(path.parent)


def _checkpoint(name: str, fault_injector: Callable[[str], None] | None) -> None:
    if fault_injector is not None:
        fault_injector(name)


def _preflight_paths(
    *,
    formal_database: Path,
    candidate: Path,
    migration_receipt: Path,
    backup_directory: Path,
    receipt: Path,
    freeze_lock: Path,
) -> tuple[
    FileIdentity,
    FileIdentity,
    FileIdentity,
    FileIdentity,
    FileIdentity,
    Path,
]:
    formal_database = formal_database.absolute()
    candidate = candidate.absolute()
    migration_receipt = migration_receipt.absolute()
    backup_directory = backup_directory.absolute()
    receipt = receipt.absolute()
    freeze_lock = freeze_lock.absolute()
    for path, label in (
        (formal_database, "formal writer database"),
        (candidate, "candidate database"),
        (migration_receipt, "migration receipt"),
        (backup_directory, "backup directory"),
        (receipt, "receipt"),
        (freeze_lock, "operator freeze lock"),
    ):
        _require_no_traversal(path, label=label)

    if not is_formal_database_path(
        formal_database,
        formal_database=FORMAL_DATABASE,
    ):
        raise CandidateInstallError(
            f"formal target must be exactly {FORMAL_DATABASE.resolve(strict=False)}"
        )
    formal_identity = _require_regular_single_link(
        formal_database, label="formal writer database"
    )
    candidate_identity = _require_regular_single_link(
        candidate, label="candidate database"
    )
    _require_project_external(candidate, label="candidate database")
    _require_regular_single_link(migration_receipt, label="migration receipt")
    _require_project_external(migration_receipt, label="migration receipt")
    if _same_identity(formal_identity, candidate_identity) or os.path.samefile(
        formal_database, candidate
    ):
        raise CandidateInstallError(
            "candidate and formal database must have different inodes"
        )
    if formal_identity.device != candidate_identity.device:
        raise CandidateInstallError(
            "candidate and formal database must be on the same filesystem"
        )

    lock_identity = _require_regular_single_link(
        freeze_lock, label="operator freeze lock"
    )
    if freeze_lock.resolve(strict=True) != CANONICAL_OPERATOR_FREEZE_LOCK.resolve(
        strict=False
    ):
        raise CandidateInstallError(
            "formal installation requires the canonical operator freeze lock"
        )
    if lock_identity.mode != 0o600:
        raise CandidateInstallError("operator freeze lock permissions must be 0600")

    backup_root = _require_directory(
        FORMAL_BACKUP_ROOT.absolute(), label="formal backup root"
    )
    if backup_directory.exists() or backup_directory.is_symlink():
        raise CandidateInstallError("backup directory must be new")
    if backup_directory.parent.resolve(strict=False) != backup_root:
        raise CandidateInstallError(
            "backup directory must be a direct child of app/data/backups"
        )
    backup_parent_identity = _stat_identity(backup_root)
    if backup_parent_identity.device != formal_identity.device:
        raise CandidateInstallError(
            "formal backup root must be on the formal database filesystem"
        )

    receipt_parent = _require_directory(receipt.parent, label="receipt parent")
    receipt_parent_identity = _stat_identity(receipt_parent)
    formal_data_root = formal_database.parent.resolve(strict=True)
    if receipt.exists() or receipt.is_symlink():
        raise CandidateInstallError("receipt path must not already exist")
    if receipt_parent == formal_data_root or formal_data_root in receipt_parent.parents:
        raise CandidateInstallError("receipt must be outside the formal app/data tree")
    _require_project_external(receipt, label="install receipt")
    if len(
        {
            path.resolve(strict=False)
            for path in (
                formal_database,
                candidate,
                migration_receipt,
                backup_directory,
                receipt,
                freeze_lock,
            )
        }
    ) != 6:
        raise CandidateInstallError("installer paths must be distinct")

    for suffix in SQLITE_SIDECAR_SUFFIXES:
        sidecar = Path(f"{formal_database}{suffix}")
        if _path_exists(sidecar):
            sidecar_identity = _require_regular_single_link(
                sidecar, label=f"formal database {suffix} sidecar"
            )
            if sidecar_identity.device != formal_identity.device:
                raise CandidateInstallError(
                    f"formal database {suffix} sidecar is on another filesystem"
                )
    source_journal = Path(f"{formal_database}-journal")
    if _path_exists(source_journal):
        raise CandidateInstallError(
            "formal source has a rollback journal; checkpoint it before cutover"
        )
    return (
        formal_identity,
        candidate_identity,
        lock_identity,
        backup_parent_identity,
        receipt_parent_identity,
        receipt_parent,
    )


def _write_failure_marker(
    *,
    backup_directory: Path,
    error: BaseException,
    candidate: Path,
    rollback_errors: Sequence[str] = (),
    quarantined_paths: Sequence[Path] = (),
) -> None:
    if not backup_directory.is_dir():
        return
    marker = backup_directory / "FAILED.json"
    try:
        _write_json_exclusive(
            marker,
            {
                "schema_version": RECEIPT_SCHEMA,
                "status": ("rollback_incomplete" if rollback_errors else "rolled_back"),
                "failed_at": _utc_now(),
                "error": f"{type(error).__name__}: {error}",
                "candidate_trace_path": str(candidate),
                "quarantined_paths": [str(path) for path in quarantined_paths],
                "rollback_errors": list(rollback_errors),
            },
        )
    except Exception:
        # The original exception and rollback outcome remain authoritative.
        return


def install_candidate(
    *,
    formal_database: Path,
    candidate: Path,
    migration_receipt: Path,
    expected_migration_receipt_sha256: str,
    backup_directory: Path,
    receipt: Path,
    freeze_lock: Path,
    fault_injector: Callable[[str], None] | None = None,
) -> dict[str, Any]:
    """Hold the external migration lock for the complete atomic install."""

    expected_digest = _require_sha256(
        expected_migration_receipt_sha256,
        label="expected migration receipt SHA-256",
    )
    formal_database = formal_database.absolute()
    migration_receipt = migration_receipt.absolute()
    if (
        os.environ.get("DCAR_TEST_DENY_FORMAL_DB") == "1"
        and is_formal_database_path(
            formal_database,
            formal_database=DEFAULT_DB,
        )
    ):
        raise CandidateInstallError(
            "test process attempted to open the formal DCar database"
        )
    migration_lock = _migration_lock_from_receipt(
        migration_receipt,
        expected_sha256=expected_digest,
    )
    with _exclusive_existing_migration_lock(migration_lock) as lock_lease:
        return _install_candidate_locked(
            formal_database=formal_database,
            candidate=candidate,
            migration_receipt=migration_receipt,
            expected_migration_receipt_sha256=expected_digest,
            backup_directory=backup_directory,
            receipt=receipt,
            freeze_lock=freeze_lock,
            held_migration_lock=migration_lock,
            held_migration_lock_identity=lock_lease.identity,
            verify_migration_lock_for_commit=lock_lease.verify_for_commit,
            fault_injector=fault_injector,
        )


def _install_candidate_locked(
    *,
    formal_database: Path,
    candidate: Path,
    migration_receipt: Path,
    expected_migration_receipt_sha256: str,
    backup_directory: Path,
    receipt: Path,
    freeze_lock: Path,
    held_migration_lock: Path,
    held_migration_lock_identity: FileIdentity,
    verify_migration_lock_for_commit: Callable[[], None],
    fault_injector: Callable[[str], None] | None = None,
) -> dict[str, Any]:
    """Install ``candidate`` or restore every original path before raising."""

    expected_migration_receipt_sha256 = _require_sha256(
        expected_migration_receipt_sha256,
        label="expected migration receipt SHA-256",
    )
    formal_database = formal_database.absolute()
    candidate = candidate.absolute()
    migration_receipt = migration_receipt.absolute()
    backup_directory = backup_directory.absolute()
    receipt = receipt.absolute()
    freeze_lock = freeze_lock.absolute()
    if (
        os.environ.get("DCAR_TEST_DENY_FORMAL_DB") == "1"
        and is_formal_database_path(
            formal_database,
            formal_database=DEFAULT_DB,
        )
    ):
        raise CandidateInstallError(
            "test process attempted to open the formal DCar database"
        )

    (
        formal_identity,
        candidate_identity,
        lock_identity,
        backup_root_identity,
        receipt_parent_identity,
        receipt_parent,
    ) = _preflight_paths(
        formal_database=formal_database,
        candidate=candidate,
        migration_receipt=migration_receipt,
        backup_directory=backup_directory,
        receipt=receipt,
        freeze_lock=freeze_lock,
    )
    contract = _load_migration_receipt_contract(
        migration_receipt=migration_receipt,
        expected_migration_receipt_sha256=expected_migration_receipt_sha256,
        formal_database=formal_database,
        candidate=candidate,
        freeze_lock=freeze_lock,
        held_migration_lock=held_migration_lock,
        held_migration_lock_identity=held_migration_lock_identity,
    )
    expected_source_sha256 = contract.source_sha256
    expected_candidate_sha256 = contract.candidate_sha256
    source_before = _fingerprint(formal_database)
    candidate_before = _fingerprint(candidate)
    if source_before["sha256"] != expected_source_sha256:
        raise CandidateInstallError("formal source SHA-256 does not match expectation")
    if candidate_before["sha256"] != expected_candidate_sha256:
        raise CandidateInstallError("candidate SHA-256 does not match expectation")
    candidate_validation = contract.candidate_validation
    database_handles = _database_handles((formal_database, candidate))
    if database_handles:
        raise CandidateInstallError(
            "database handles are still open: "
            + json.dumps(database_handles, ensure_ascii=False, sort_keys=True)
        )

    source_sidecars: dict[str, dict[str, Any]] = {}
    source_sidecar_identities: dict[str, FileIdentity] = {}
    for suffix in SQLITE_SIDECAR_SUFFIXES:
        sidecar = Path(f"{formal_database}{suffix}")
        if _path_exists(sidecar):
            source_sidecars[suffix] = _fingerprint(sidecar)
            source_sidecar_identities[suffix] = _stat_identity(sidecar)
    if source_sidecars.get("-wal", {}).get("size", 0) != 0:
        raise CandidateInstallError(
            "formal source WAL must be absent or empty before cutover"
        )
    lineage_validation = contract.lineage

    # Close the validation connection before this final TOCTOU guard.
    if _stat_identity(formal_database) != formal_identity:
        raise CandidateInstallError("formal source identity changed during preflight")
    if _stat_identity(candidate) != candidate_identity:
        raise CandidateInstallError("candidate identity changed during preflight")
    if _sha256_file(formal_database) != expected_source_sha256:
        raise CandidateInstallError("formal source changed during preflight")
    if _sha256_file(candidate) != expected_candidate_sha256:
        raise CandidateInstallError("candidate changed during preflight")
    for suffix in SQLITE_SIDECAR_SUFFIXES:
        sidecar = Path(f"{formal_database}{suffix}")
        identity = source_sidecar_identities.get(suffix)
        if identity is None and _path_exists(sidecar):
            raise CandidateInstallError(
                f"formal database {suffix} sidecar appeared during preflight"
            )
        if identity is not None and (
            not _path_exists(sidecar)
            or _stat_identity(sidecar) != identity
            or _sha256_file(sidecar) != source_sidecars[suffix]["sha256"]
        ):
            raise CandidateInstallError(
                f"formal database {suffix} sidecar changed during preflight"
            )
    if _path_exists(Path(f"{formal_database}-journal")):
        raise CandidateInstallError(
            "formal source rollback journal appeared during preflight"
        )
    for suffix in SQLITE_TRANSIENT_SUFFIXES:
        if _path_exists(Path(f"{candidate}{suffix}")):
            raise CandidateInstallError(
                f"candidate {suffix} sidecar appeared during preflight"
            )
    _assert_migration_contract_files_unchanged(
        contract,
        migration_receipt=migration_receipt,
    )
    database_handles = _database_handles((formal_database, candidate))
    if database_handles:
        raise CandidateInstallError(
            "database handles appeared during preflight: "
            + json.dumps(database_handles, ensure_ascii=False, sort_keys=True)
        )
    if (
        _require_regular_single_link(freeze_lock, label="operator freeze lock")
        != lock_identity
    ):
        raise CandidateInstallError("canonical operator freeze lock changed")
    if not _same_identity(
        _stat_identity(backup_directory.parent), backup_root_identity
    ):
        raise CandidateInstallError("formal backup root changed during preflight")
    if not _same_identity(_stat_identity(receipt_parent), receipt_parent_identity):
        raise CandidateInstallError("receipt parent changed during preflight")
    source_backup = backup_directory / formal_database.name
    moved_sidecars: list[str] = []
    source_moved = False
    candidate_installed = False
    receipt_identity: FileIdentity | None = None
    try:
        _checkpoint("after_preflight", fault_injector)
        backup_directory.mkdir(mode=0o700)
        _fsync_directory(backup_directory.parent)
        _checkpoint("after_backup_directory_created", fault_injector)

        # Recheck immediately before the first destructive rename.  The
        # operator freeze is cooperative; these identity/hash/handle checks
        # make any non-cooperating writer or path replacement fail closed.
        if (
            _stat_identity(formal_database) != formal_identity
            or _sha256_file(formal_database) != expected_source_sha256
        ):
            raise CandidateInstallError("formal source changed before cutover")
        if (
            _stat_identity(candidate) != candidate_identity
            or _sha256_file(candidate) != expected_candidate_sha256
        ):
            raise CandidateInstallError("candidate changed before cutover")
        for suffix in SQLITE_SIDECAR_SUFFIXES:
            sidecar = Path(f"{formal_database}{suffix}")
            identity = source_sidecar_identities.get(suffix)
            if identity is None and _path_exists(sidecar):
                raise CandidateInstallError(
                    f"formal database {suffix} sidecar appeared before cutover"
                )
            if identity is not None and (
                not _path_exists(sidecar)
                or _stat_identity(sidecar) != identity
                or _sha256_file(sidecar) != source_sidecars[suffix]["sha256"]
            ):
                raise CandidateInstallError(
                    f"formal database {suffix} sidecar changed before cutover"
                )
        if _path_exists(Path(f"{formal_database}-journal")):
            raise CandidateInstallError(
                "formal source rollback journal appeared before cutover"
            )
        for suffix in SQLITE_TRANSIENT_SUFFIXES:
            if _path_exists(Path(f"{candidate}{suffix}")):
                raise CandidateInstallError(
                    f"candidate {suffix} sidecar appeared before cutover"
                )
        _assert_migration_contract_files_unchanged(
            contract,
            migration_receipt=migration_receipt,
        )
        database_handles = _database_handles((formal_database, candidate))
        if database_handles:
            raise CandidateInstallError(
                "database handles appeared immediately before cutover: "
                + json.dumps(database_handles, ensure_ascii=False, sort_keys=True)
            )

        os.replace(formal_database, source_backup)
        source_moved = True
        _checkpoint("after_source_database_moved", fault_injector)

        for suffix in SQLITE_SIDECAR_SUFFIXES:
            source_sidecar = Path(f"{formal_database}{suffix}")
            expected_identity = source_sidecar_identities.get(suffix)
            if expected_identity is None and _path_exists(source_sidecar):
                raise CandidateInstallError(
                    f"formal database {suffix} sidecar appeared during cutover"
                )
            if expected_identity is not None:
                if (
                    not _path_exists(source_sidecar)
                    or _stat_identity(source_sidecar) != expected_identity
                ):
                    raise CandidateInstallError(
                        f"formal database {suffix} sidecar changed during cutover"
                    )
                os.replace(
                    source_sidecar, backup_directory / f"{formal_database.name}{suffix}"
                )
                moved_sidecars.append(suffix)
            checkpoint = (
                "after_source_wal_moved"
                if suffix == "-wal"
                else "after_source_shm_moved"
            )
            _checkpoint(checkpoint, fault_injector)

        _fsync_file(source_backup)
        for suffix in moved_sidecars:
            _fsync_file(backup_directory / f"{formal_database.name}{suffix}")
        _fsync_directory(formal_database.parent)
        _fsync_directory(backup_directory)

        if _path_exists(formal_database):
            raise CandidateInstallError(
                "formal database path was recreated during cutover"
            )
        # Creating the destination hard link is atomic and refuses to clobber
        # a path recreated by a racing process.  Unlinking the source consumes
        # the candidate while retaining the same inode at the formal path.
        os.link(candidate, formal_database, follow_symlinks=False)
        try:
            candidate_installed = True
            candidate.unlink()
        except BaseException:
            if _path_exists(formal_database):
                formal_database.unlink()
            candidate_installed = False
            raise
        candidate_installed = True
        _checkpoint("after_candidate_installed", fault_injector)

        os.chmod(formal_database, 0o600)
        _fsync_file(formal_database)
        _checkpoint("after_installed_file_synced", fault_injector)

        for directory in {
            formal_database.parent,
            candidate.parent,
            backup_directory,
            backup_directory.parent,
        }:
            _fsync_directory(directory)
        _checkpoint("after_directories_synced", fault_injector)

        installed = _fingerprint(formal_database)
        if installed["sha256"] != expected_candidate_sha256:
            raise CandidateInstallError(
                "installed database SHA-256 does not match the candidate"
            )
        installed_validation = _validate_candidate_database(formal_database)
        backup = _fingerprint(source_backup)
        if backup["sha256"] != expected_source_sha256:
            raise CandidateInstallError("preserved source backup SHA-256 mismatch")
        backup_sidecars: dict[str, dict[str, Any]] = {}
        for suffix, before in source_sidecars.items():
            path = backup_directory / f"{formal_database.name}{suffix}"
            fingerprint = _fingerprint(path)
            if fingerprint["sha256"] != before["sha256"]:
                raise CandidateInstallError(
                    f"preserved source {suffix} sidecar SHA-256 mismatch"
                )
            backup_sidecars[suffix] = fingerprint
        database_handles = _database_handles((formal_database, candidate))
        if database_handles:
            raise CandidateInstallError(
                "database handles appeared after candidate installation: "
                + json.dumps(database_handles, ensure_ascii=False, sort_keys=True)
            )
        _checkpoint("after_post_install_verification", fault_injector)

        if (
            _require_regular_single_link(freeze_lock, label="operator freeze lock")
            != lock_identity
        ):
            raise CandidateInstallError(
                "canonical operator freeze lock changed before receipt write"
            )
        if not _same_identity(_stat_identity(receipt_parent), receipt_parent_identity):
            raise CandidateInstallError("receipt parent changed before receipt write")
        _assert_migration_contract_files_unchanged(
            contract,
            migration_receipt=migration_receipt,
        )
        for suffix in SQLITE_TRANSIENT_SUFFIXES:
            if _path_exists(Path(f"{formal_database}{suffix}")):
                raise CandidateInstallError(
                    f"installed database {suffix} sidecar appeared before receipt write"
                )

        result: dict[str, Any] = {
            "schema_version": RECEIPT_SCHEMA,
            "status": "installed",
            "completed_at": _utc_now(),
            "formal_database": str(formal_database),
            "candidate_source_path": str(candidate),
            "canonical_operator_freeze_lock": str(freeze_lock),
            "backup_directory": str(backup_directory),
            "expected": {
                "migration_receipt_sha256": expected_migration_receipt_sha256,
                "source_sha256": expected_source_sha256,
                "candidate_sha256": expected_candidate_sha256,
            },
            "migration_receipt": {
                "path": str(migration_receipt),
                "file": _fingerprint(migration_receipt),
                "schema_version": MIGRATION_RECEIPT_SCHEMA,
                "from_version": EXPECTED_SOURCE_SCHEMA_VERSION,
                "to_version": EXPECTED_CANDIDATE_SCHEMA_VERSION,
                "verified_backup_path": str(contract.backup_path),
                "verified_backup_sha256": contract.backup_sha256,
            },
            "before": {
                "database": source_before,
                "sidecars": source_sidecars,
                "validation": contract.source_validation,
            },
            "candidate": {
                "file": candidate_before,
                "validation": candidate_validation,
                "source_lineage": lineage_validation,
            },
            "installed": {
                "file": installed,
                "validation": installed_validation,
            },
            "backup": {
                "database": backup,
                "sidecars": backup_sidecars,
            },
            "database_handles": database_handles,
            "rollback": "not_required",
            "receipt": str(receipt),
        }

        def receipt_created(identity: FileIdentity) -> None:
            nonlocal receipt_identity
            receipt_identity = identity

        _write_json_exclusive(receipt, result, on_created=receipt_created)
        _checkpoint("after_receipt_written", fault_injector)
        verify_migration_lock_for_commit()
        return result
    except BaseException as error:
        rollback_errors: list[str] = []
        quarantined_paths: list[Path] = []
        try:
            if receipt_identity is not None and _path_exists(receipt):
                if not _same_identity(_stat_identity(receipt), receipt_identity):
                    rollback_errors.append(
                        "receipt: installer-owned receipt path changed before rollback"
                    )
                else:
                    receipt.unlink()
                    _fsync_directory(receipt_parent)
        except Exception as rollback_error:
            rollback_errors.append(f"receipt: {rollback_error}")

        rollback_paths_safe = True
        try:
            if candidate_installed or source_moved:
                rollback_handles = _database_handles(
                    (formal_database, candidate)
                )
                if rollback_handles:
                    rollback_paths_safe = False
                    rollback_errors.append(
                        "database handles appeared during rollback: "
                        + json.dumps(
                            rollback_handles, ensure_ascii=False, sort_keys=True
                        )
                    )
        except Exception as rollback_error:
            rollback_paths_safe = False
            rollback_errors.append(f"rollback writer check: {rollback_error}")

        def quarantine(path: Path, *, destination_name: str) -> None:
            destination = backup_directory / destination_name
            if _path_exists(destination):
                raise CandidateInstallError(
                    f"quarantine destination already exists: {destination}"
                )
            os.replace(path, destination)
            _fsync_file(destination)
            quarantined_paths.append(destination)

        if rollback_paths_safe:
            try:
                # Any sidecar created after the candidate appeared belongs to
                # the installed candidate, never to the original database.
                # Isolate it before restoring even one original sidecar.
                if candidate_installed:
                    for suffix in SQLITE_TRANSIENT_SUFFIXES:
                        current = Path(f"{formal_database}{suffix}")
                        if _path_exists(current):
                            quarantine(
                                current,
                                destination_name=(
                                    f"FAILED-installed-{formal_database.name}{suffix}"
                                ),
                            )

                if candidate_installed and _path_exists(formal_database):
                    if not _path_exists(candidate):
                        os.replace(formal_database, candidate)
                        os.chmod(candidate, candidate_identity.mode)
                        _fsync_file(candidate)
                    else:
                        quarantine(
                            formal_database,
                            destination_name=f"FAILED-installed-{formal_database.name}",
                        )
                elif source_moved and _path_exists(formal_database):
                    # A non-cooperating process recreated the path while the
                    # source lived in backup. Preserve it; never overwrite it.
                    quarantine(
                        formal_database,
                        destination_name=f"FAILED-racing-{formal_database.name}",
                    )
            except Exception as rollback_error:
                rollback_errors.append(f"candidate quarantine: {rollback_error}")

        if rollback_paths_safe:
            for suffix in SQLITE_SIDECAR_SUFFIXES:
                try:
                    preserved = backup_directory / f"{formal_database.name}{suffix}"
                    destination = Path(f"{formal_database}{suffix}")
                    if suffix in moved_sidecars:
                        if not _path_exists(preserved):
                            raise CandidateInstallError(
                                f"preserved source {suffix} sidecar is missing"
                            )
                        if _path_exists(destination):
                            quarantine(
                                destination,
                                destination_name=(
                                    f"FAILED-racing-{formal_database.name}{suffix}"
                                ),
                            )
                        os.replace(preserved, destination)
                        _fsync_file(destination)
                    elif suffix not in source_sidecar_identities and _path_exists(
                        destination
                    ):
                        quarantine(
                            destination,
                            destination_name=(
                                f"FAILED-racing-{formal_database.name}{suffix}"
                            ),
                        )
                except Exception as rollback_error:
                    rollback_errors.append(f"sidecar {suffix}: {rollback_error}")
            try:
                journal = Path(f"{formal_database}-journal")
                if _path_exists(journal):
                    quarantine(
                        journal,
                        destination_name=(
                            f"FAILED-racing-{formal_database.name}-journal"
                        ),
                    )
            except Exception as rollback_error:
                rollback_errors.append(f"rollback journal: {rollback_error}")
            try:
                if source_moved:
                    if not _path_exists(source_backup):
                        raise CandidateInstallError(
                            "preserved source database is missing"
                        )
                    if _path_exists(formal_database):
                        raise CandidateInstallError(
                            "formal path is occupied before source restore"
                        )
                    os.replace(source_backup, formal_database)
                    _fsync_file(formal_database)
            except Exception as rollback_error:
                rollback_errors.append(f"source database: {rollback_error}")

            try:
                if source_moved:
                    if (
                        not _path_exists(formal_database)
                        or _sha256_file(formal_database) != expected_source_sha256
                    ):
                        raise CandidateInstallError(
                            "restored source database SHA-256 mismatch"
                        )
                if candidate_installed and not _path_exists(candidate):
                    raise CandidateInstallError(
                        "installed candidate was not restored or quarantined"
                    )
                if _path_exists(candidate) and (
                    _sha256_file(candidate) != expected_candidate_sha256
                ):
                    raise CandidateInstallError(
                        "restored candidate database SHA-256 mismatch"
                    )
                for suffix in SQLITE_SIDECAR_SUFFIXES:
                    destination = Path(f"{formal_database}{suffix}")
                    original_sidecar = source_sidecars.get(suffix)
                    if original_sidecar is None and _path_exists(destination):
                        raise CandidateInstallError(
                            f"rollback left an unexpected formal {suffix} sidecar"
                        )
                    if original_sidecar is not None and (
                        not _path_exists(destination)
                        or _sha256_file(destination) != original_sidecar["sha256"]
                    ):
                        raise CandidateInstallError(
                            f"restored formal {suffix} sidecar SHA-256 mismatch"
                        )
                if _path_exists(Path(f"{formal_database}-journal")):
                    raise CandidateInstallError(
                        "rollback left an unexpected formal rollback journal"
                    )
            except Exception as rollback_error:
                rollback_errors.append(f"rollback validation: {rollback_error}")

        for directory in {
            formal_database.parent,
            candidate.parent,
            backup_directory,
            backup_directory.parent,
        }:
            if directory.is_dir():
                try:
                    _fsync_directory(directory)
                except Exception as rollback_error:
                    rollback_errors.append(f"fsync {directory}: {rollback_error}")
        _write_failure_marker(
            backup_directory=backup_directory,
            error=error,
            candidate=candidate,
            rollback_errors=rollback_errors,
            quarantined_paths=quarantined_paths,
        )
        if rollback_errors:
            raise CandidateInstallError(
                "candidate installation failed and rollback was incomplete: "
                f"original={type(error).__name__}: {error}; "
                f"rollback={json.dumps(rollback_errors, ensure_ascii=False)}"
            ) from error
        raise CandidateInstallError(
            "candidate installation failed and was rolled back: "
            f"{type(error).__name__}: {error}"
        ) from error


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument(
        "--formal-db",
        type=Path,
        required=True,
        help=f"Required exact formal target: {FORMAL_DATABASE}",
    )
    parser.add_argument("--candidate", type=Path, required=True)
    parser.add_argument("--migration-receipt", type=Path, required=True)
    parser.add_argument("--expected-migration-receipt-sha256", required=True)
    parser.add_argument(
        "--backup-dir",
        type=Path,
        required=True,
        help=f"Required new direct child of {FORMAL_BACKUP_ROOT}",
    )
    parser.add_argument(
        "--receipt",
        type=Path,
        required=True,
        help="Required new receipt path outside app/data; created with O_EXCL",
    )
    parser.add_argument(
        "--freeze-lock",
        type=Path,
        required=True,
        help=f"Required canonical lock: {CANONICAL_OPERATOR_FREEZE_LOCK}",
    )
    return parser


def main(argv: Sequence[str] | None = None) -> int:
    arguments = build_parser().parse_args(argv)
    try:
        result = install_candidate(
            formal_database=arguments.formal_db,
            candidate=arguments.candidate,
            migration_receipt=arguments.migration_receipt,
            expected_migration_receipt_sha256=(
                arguments.expected_migration_receipt_sha256
            ),
            backup_directory=arguments.backup_dir,
            receipt=arguments.receipt,
            freeze_lock=arguments.freeze_lock,
        )
    except CandidateInstallError as error:
        print(f"writer candidate installation refused: {error}", file=sys.stderr)
        return 1
    print(json.dumps(result, ensure_ascii=False, sort_keys=True))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
