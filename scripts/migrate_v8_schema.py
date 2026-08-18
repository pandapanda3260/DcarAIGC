#!/usr/bin/env python3
"""Build a verified v16 candidate from the frozen formal v15 database.

This command never migrates or replaces the formal database in place.  It
copies the frozen source with SQLite's backup API, migrates a private staging
file, proves the v15-to-v16 lineage, and then publishes a self-contained
candidate plus an O_EXCL receipt.  Installation remains a separate atomic
operation performed by ``install_writer_database_candidate.py``.
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
import tempfile
from collections.abc import Callable, Mapping, Sequence
from contextlib import contextmanager
from dataclasses import asdict, dataclass
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Iterator


PROJECT_ROOT = Path(__file__).resolve().parents[1]
PACKAGE_ROOT = PROJECT_ROOT / "src" / "dcar_eval"
if str(PACKAGE_ROOT) not in sys.path:
    sys.path.insert(0, str(PACKAGE_ROOT))

from v8.storage import (  # noqa: E402
    CURRENT_SCHEMA_MIGRATION_NAME,
    DEFAULT_DB,
    SCHEMA_MIGRATION_NAMES,
    _V16_DROPPED_TABLES,
    _V16_EVALUATION_COPY_COLUMNS,
    _V16_REMOVED_INDEXES,
    _table_projection_sha256,
    configure_connection_safety,
    initialize_database,
    is_formal_database_path,
    require_schema_compatibility,
    same_database_path,
)


FORMAL_DATABASE = DEFAULT_DB
CANONICAL_OPERATOR_FREEZE_LOCK = PROJECT_ROOT / "runtime" / "operator-freeze.lock"
BACKUP_RECEIPT_SCHEMA = "dcar-v16-offline-backup-v1"
MIGRATION_RECEIPT_SCHEMA = "dcar-v16-offline-migration-v1"
EXPECTED_FROM_VERSION = 15
EXPECTED_TO_VERSION = 16
EXPECTED_FROM_MIGRATION = SCHEMA_MIGRATION_NAMES[EXPECTED_FROM_VERSION]
EXPECTED_TO_MIGRATION = CURRENT_SCHEMA_MIGRATION_NAME
HEX_SHA256 = re.compile(r"[0-9a-f]{64}", re.ASCII)
SQLITE_TRANSIENT_SUFFIXES = ("-wal", "-shm", "-journal")
MAX_BACKUP_RECEIPT_BYTES = 64 * 1024
MIGRATION_LOCK_PAYLOAD = b"dcar-v16-offline-migration-lock-v1\n"
MIGRATION_CHECKPOINTS = (
    "after_preflight",
    "after_backup_copy",
    "after_candidate_migration",
    "after_candidate_validation",
    "after_candidate_promoted",
    "after_receipt_written",
)
BACKUP_CHECKPOINTS = (
    "after_backup_preflight",
    "after_verified_backup_copy",
    "after_verified_backup_validation",
    "after_restore_validation",
    "after_verified_backup_promoted",
    "after_backup_receipt_written",
)


class OfflineMigrationError(RuntimeError):
    """Raised when the offline migration contract cannot be proved."""


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


def _utc_now() -> str:
    return (
        datetime.now(timezone.utc)
        .isoformat(timespec="seconds")
        .replace("+00:00", "Z")
    )


def _path_exists(path: Path) -> bool:
    return path.exists() or path.is_symlink()


def _identity(path: Path) -> FileIdentity:
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


def _sha256_file(path: Path) -> str:
    digest = hashlib.sha256()
    with path.open("rb") as handle:
        for chunk in iter(lambda: handle.read(1024 * 1024), b""):
            digest.update(chunk)
    return digest.hexdigest()


def _fingerprint(path: Path) -> dict[str, Any]:
    return {**asdict(_identity(path)), "sha256": _sha256_file(path)}


def _require_sha256(value: str, *, label: str) -> str:
    if HEX_SHA256.fullmatch(value) is None:
        raise OfflineMigrationError(f"{label} must be a lowercase SHA-256 digest")
    return value


def _require_regular_single_link(path: Path, *, label: str) -> FileIdentity:
    try:
        value = path.lstat()
    except FileNotFoundError as exc:
        raise OfflineMigrationError(f"{label} is missing: {path}") from exc
    if stat.S_ISLNK(value.st_mode) or not stat.S_ISREG(value.st_mode):
        raise OfflineMigrationError(f"{label} must be a regular non-symlink file")
    if value.st_nlink != 1:
        raise OfflineMigrationError(f"{label} must not be hard-linked")
    path.resolve(strict=True)
    return _identity_from_stat(value)


def _require_directory(path: Path, *, label: str) -> Path:
    try:
        value = path.lstat()
    except FileNotFoundError as exc:
        raise OfflineMigrationError(f"{label} is missing: {path}") from exc
    if stat.S_ISLNK(value.st_mode) or not stat.S_ISDIR(value.st_mode):
        raise OfflineMigrationError(f"{label} must be a non-symlink directory")
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
        raise OfflineMigrationError(f"{label} must be outside the project root")
    return resolved


def _require_distinct_paths(paths: Sequence[Path], *, label: str) -> None:
    locations: set[tuple[str, int, int] | tuple[str, int, int, str]] = set()
    for path in paths:
        if _path_exists(path):
            value = os.stat(path)
            location: tuple[str, int, int] | tuple[str, int, int, str] = (
                "existing",
                value.st_dev,
                value.st_ino,
            )
        else:
            parent = _require_directory(path.parent, label=f"{label} path parent")
            value = parent.stat()
            location = ("new", value.st_dev, value.st_ino, path.name)
        if location in locations:
            raise OfflineMigrationError(f"{label} must be distinct by filesystem identity")
        locations.add(location)


def _same_existing_file(left: Path, right: Path) -> bool:
    return same_database_path(left, right)


def _require_secure_lock_parent(path: Path) -> Path:
    parent = _require_directory(path.parent, label="migration lock parent")
    value = parent.stat()
    if value.st_uid != os.geteuid() or stat.S_IMODE(value.st_mode) != 0o700:
        raise OfflineMigrationError(
            "migration lock parent must be owned by the current user with mode 0700"
        )
    return parent


def _fsync_file(path: Path) -> None:
    descriptor = os.open(path, os.O_RDONLY)
    try:
        os.fsync(descriptor)
    finally:
        os.close(descriptor)


def _fsync_directory(path: Path) -> None:
    descriptor = os.open(
        path,
        os.O_RDONLY | getattr(os, "O_DIRECTORY", 0),
    )
    try:
        os.fsync(descriptor)
    finally:
        os.close(descriptor)


def _write_json_exclusive(
    path: Path,
    value: Mapping[str, Any],
) -> FileIdentity:
    payload = (
        json.dumps(value, ensure_ascii=False, indent=2, sort_keys=True) + "\n"
    ).encode("utf-8")
    flags = os.O_WRONLY | os.O_CREAT | os.O_EXCL | getattr(os, "O_NOFOLLOW", 0)
    descriptor = os.open(path, flags, 0o600)
    created_identity = _identity_from_stat(os.fstat(descriptor))
    try:
        written = 0
        while written < len(payload):
            count = os.write(descriptor, payload[written:])
            if count <= 0:
                raise OSError("receipt write made no progress")
            written += count
        os.fsync(descriptor)
        os.close(descriptor)
        descriptor = -1
        current = _identity(path)
        if (current.device, current.inode) != (
            created_identity.device,
            created_identity.inode,
        ):
            raise OSError("receipt path identity changed during write")
        _fsync_directory(path.parent)
        return current
    except BaseException as error:
        close_error: BaseException | None = None
        if descriptor >= 0:
            try:
                os.close(descriptor)
            except BaseException as exc:
                close_error = exc
            finally:
                descriptor = -1
        try:
            if _path_exists(path):
                current = _identity(path)
                if (current.device, current.inode) == (
                    created_identity.device,
                    created_identity.inode,
                ):
                    path.unlink()
                    _fsync_directory(path.parent)
        except BaseException as cleanup_error:
            raise OfflineMigrationError(
                f"receipt write failed and cleanup was not durable: {cleanup_error}"
            ) from error
        if close_error is not None:
            raise OfflineMigrationError(
                f"receipt write failed while closing its file: {close_error}"
            ) from error
        raise
    finally:
        if descriptor >= 0:
            os.close(descriptor)


def _checkpoint(
    name: str,
    fault_injector: Callable[[str], None] | None,
) -> None:
    if fault_injector is not None:
        fault_injector(name)


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
    except OSError as exc:
        raise OfflineMigrationError("cannot verify database handles with lsof") from exc
    if result.returncode not in {0, 1}:
        raise OfflineMigrationError(
            "cannot verify database handles with lsof: "
            f"{result.stderr.strip() or 'lsof failed'}"
        )
    handles: list[dict[str, Any]] = []
    for line in result.stdout.splitlines()[1:]:
        parts = line.split()
        if len(parts) < 9:
            continue
        match = re.fullmatch(r"\d+([rwu]).*", parts[3])
        if match is None:
            continue
        try:
            pid = int(parts[1])
        except ValueError:
            pid = -1
        handles.append(
            {
                "command": parts[0],
                "pid": pid,
                "descriptor": parts[3],
                "path": parts[-1],
            }
        )
    return handles


@contextmanager
def _exclusive_migration_lock(
    path: Path,
    *,
    allow_existing: bool = True,
) -> Iterator[_MigrationLockLease]:
    parent = _require_secure_lock_parent(path)
    _require_project_external(parent / path.name, label="migration lock")
    if path.is_symlink():
        raise OfflineMigrationError("migration lock must not be a symlink")
    parent_flags = (
        os.O_RDONLY
        | os.O_CLOEXEC
        | getattr(os, "O_DIRECTORY", 0)
        | getattr(os, "O_NOFOLLOW", 0)
    )
    try:
        parent_descriptor = os.open(parent, parent_flags)
    except OSError as exc:
        raise OfflineMigrationError(
            f"cannot open migration lock parent: {parent}"
        ) from exc
    descriptor = -1
    parent_locked = False
    file_locked = False
    base_flags = os.O_RDWR | os.O_CLOEXEC | getattr(os, "O_NOFOLLOW", 0)
    created = False
    try:
        try:
            fcntl.flock(parent_descriptor, fcntl.LOCK_EX | fcntl.LOCK_NB)
        except BlockingIOError as exc:
            raise OfflineMigrationError("another offline migration holds the lock") from exc
        parent_locked = True
        parent_value = os.fstat(parent_descriptor)
        current_parent = parent.stat()
        if (parent_value.st_dev, parent_value.st_ino) != (
            current_parent.st_dev,
            current_parent.st_ino,
        ):
            raise OfflineMigrationError("migration lock parent identity changed")
        try:
            descriptor = os.open(
                path.name,
                base_flags | os.O_CREAT | os.O_EXCL,
                0o600,
                dir_fd=parent_descriptor,
            )
            created = True
        except FileExistsError:
            if not allow_existing:
                raise OfflineMigrationError(
                    "initial migration lock path must be new; remove only a verified "
                    "orphan from a failed prepare-backup run"
                )
            try:
                descriptor = os.open(
                    path.name,
                    base_flags,
                    dir_fd=parent_descriptor,
                )
            except OSError as exc:
                raise OfflineMigrationError(
                    f"cannot open existing migration lock: {path}"
                ) from exc
        except OSError as exc:
            raise OfflineMigrationError(f"cannot create migration lock: {path}") from exc
        try:
            fcntl.flock(descriptor, fcntl.LOCK_EX | fcntl.LOCK_NB)
        except BlockingIOError as exc:
            raise OfflineMigrationError("another offline migration holds the lock") from exc
        file_locked = True
        value = os.fstat(descriptor)
        if (
            not stat.S_ISREG(value.st_mode)
            or value.st_nlink != 1
            or value.st_uid != os.geteuid()
        ):
            raise OfflineMigrationError(
                "migration lock must be a current-user regular single-link file"
            )
        path_value = os.stat(
            path.name,
            dir_fd=parent_descriptor,
            follow_symlinks=False,
        )
        if (value.st_dev, value.st_ino) != (path_value.st_dev, path_value.st_ino):
            raise OfflineMigrationError("migration lock path identity changed")
        if created:
            os.fchmod(descriptor, 0o600)
            written = 0
            try:
                while written < len(MIGRATION_LOCK_PAYLOAD):
                    count = os.write(descriptor, MIGRATION_LOCK_PAYLOAD[written:])
                    if count <= 0:
                        raise OSError("migration lock write made no progress")
                    written += count
                os.fsync(descriptor)
                os.fsync(parent_descriptor)
            except BaseException:
                try:
                    current = os.stat(
                        path.name,
                        dir_fd=parent_descriptor,
                        follow_symlinks=False,
                    )
                    if (current.st_dev, current.st_ino) == (value.st_dev, value.st_ino):
                        os.unlink(path.name, dir_fd=parent_descriptor)
                        os.fsync(parent_descriptor)
                finally:
                    raise
        else:
            if stat.S_IMODE(value.st_mode) != 0o600:
                raise OfflineMigrationError("migration lock permissions must be 0600")
            os.lseek(descriptor, 0, os.SEEK_SET)
            payload = os.read(descriptor, len(MIGRATION_LOCK_PAYLOAD) + 1)
            if payload != MIGRATION_LOCK_PAYLOAD:
                raise OfflineMigrationError(
                    "existing migration lock does not have the exact lock contract"
                )
        identity = _identity_from_stat(os.fstat(descriptor))
        lease = _MigrationLockLease(
            identity=identity,
            verify_binding=lambda: _assert_lock_binding(
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
                    _assert_lock_binding(
                        path,
                        parent,
                        parent_descriptor,
                        descriptor,
                    )
                    if created:
                        os.unlink(path.name, dir_fd=parent_descriptor)
                        os.fsync(parent_descriptor)
                except BaseException as binding_error:
                    raise OfflineMigrationError(
                        "migration lock cleanup failed during recovery: "
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


def _assert_lock_binding(
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
        raise OfflineMigrationError("migration lock parent identity changed")
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
        raise OfflineMigrationError("migration lock path identity changed")


def _connect_immutable(path: Path, *, label: str) -> sqlite3.Connection:
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
    except (OSError, sqlite3.Error, RuntimeError) as exc:
        raise OfflineMigrationError(f"{label} cannot be opened read-only: {exc}") from exc


def _migration_rows(connection: sqlite3.Connection) -> list[tuple[int, str, str]]:
    return [
        (int(row["version"]), str(row["name"]), str(row["applied_at"]))
        for row in connection.execute(
            "SELECT version,name,applied_at FROM schema_migrations ORDER BY version"
        )
    ]


def _table_names(connection: sqlite3.Connection) -> set[str]:
    return {
        str(row[0])
        for row in connection.execute(
            "SELECT name FROM sqlite_master "
            "WHERE type='table' AND name NOT LIKE 'sqlite_%'"
        )
    }


def _table_columns(connection: sqlite3.Connection, table: str) -> list[str]:
    quoted = '"' + table.replace('"', '""') + '"'
    return [
        str(row["name"])
        for row in connection.execute(f"PRAGMA table_info({quoted})")
    ]


def _sequence_state(connection: sqlite3.Connection) -> dict[str, int]:
    return {
        str(row["name"]): int(row["seq"])
        for row in connection.execute(
            "SELECT name,seq FROM sqlite_sequence ORDER BY name"
        )
    }


def _validate_database(
    path: Path,
    *,
    expected_version: int,
    expected_migration: str,
    label: str,
) -> dict[str, Any]:
    connection = _connect_immutable(path, label=label)
    try:
        quick = [str(row[0]) for row in connection.execute("PRAGMA quick_check")]
        integrity = [
            str(row[0]) for row in connection.execute("PRAGMA integrity_check")
        ]
        foreign_keys = list(connection.execute("PRAGMA foreign_key_check"))
        if quick != ["ok"]:
            raise OfflineMigrationError(f"{label} quick_check failed: {quick}")
        if integrity != ["ok"]:
            raise OfflineMigrationError(
                f"{label} integrity_check failed: {integrity}"
            )
        if foreign_keys:
            raise OfflineMigrationError(
                f"{label} has {len(foreign_keys)} foreign-key violations"
            )
        try:
            version = require_schema_compatibility(
                connection,
                supported_versions=frozenset({expected_version}),
            )
        except Exception as exc:
            raise OfflineMigrationError(
                f"{label} schema compatibility failed: {exc}"
            ) from exc
        rows = _migration_rows(connection)
        maximum = max((row[0] for row in rows), default=0)
        names = [row[1] for row in rows if row[0] == expected_version]
        if (
            version != expected_version
            or maximum != expected_version
            or names != [expected_migration]
        ):
            raise OfflineMigrationError(f"{label} schema identity is not exact")
        return {
            "quick_check": "ok",
            "integrity_check": "ok",
            "foreign_key_violation_count": 0,
            "schema_version": version,
            "schema_migration": expected_migration,
        }
    finally:
        connection.close()


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
        quoted = '"' + table.replace('"', '""') + '"'
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
        "schema_migrations": [list(row) for row in _migration_rows(connection)],
    }


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
    expected_removed_names = set(_V16_DROPPED_TABLES) | set(_V16_REMOVED_INDEXES)
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
        raise OfflineMigrationError(
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
    source = _connect_immutable(source_path, label="formal source database")
    backup = _connect_immutable(backup_path, label="verified backup database")
    try:
        source_manifest = _projection_manifest(source)
        backup_manifest = _projection_manifest(backup)
        if backup_manifest != source_manifest:
            raise OfflineMigrationError(
                "verified backup data projection differs from formal source"
            )
        source_schema = _schema_object_manifest(source)
        backup_schema = _schema_object_manifest(backup)
        if backup_schema != source_schema:
            raise OfflineMigrationError(
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


def _validate_lineage(source_path: Path, candidate_path: Path) -> dict[str, Any]:
    source = _connect_immutable(source_path, label="formal source database")
    candidate = _connect_immutable(candidate_path, label="candidate database")
    try:
        source_manifest = _projection_manifest(source)
        candidate_manifest = _projection_manifest(candidate)
        source_tables = set(source_manifest["tables"])
        candidate_tables = set(candidate_manifest["tables"])
        expected_candidate_tables = source_tables - set(_V16_DROPPED_TABLES)
        if candidate_tables != expected_candidate_tables:
            raise OfflineMigrationError(
                "candidate table lineage differs from v15 source: "
                f"actual={sorted(candidate_tables)},"
                f"expected={sorted(expected_candidate_tables)}"
            )
        retained: dict[str, Any] = {}
        for table in sorted(expected_candidate_tables):
            before = source_manifest["tables"][table]
            after = candidate_manifest["tables"][table]
            if before != after:
                raise OfflineMigrationError(
                    f"candidate changed retained table projection: {table}"
                )
            retained[table] = before
        if (
            source_manifest["evaluation_manual_review_count"]
            != candidate_manifest["evaluation_manual_review_count"]
        ):
            raise OfflineMigrationError(
                "candidate changed manual_review evaluation row count"
            )
        expected_sequences = {
            name: value
            for name, value in source_manifest["sqlite_sequence"].items()
            if name not in _V16_DROPPED_TABLES
        }
        if candidate_manifest["sqlite_sequence"] != expected_sequences:
            raise OfflineMigrationError(
                "candidate changed retained sqlite_sequence values"
            )
        source_migrations = [tuple(row) for row in source_manifest["schema_migrations"]]
        candidate_migrations = [
            tuple(row) for row in candidate_manifest["schema_migrations"]
        ]
        if candidate_migrations[:-1] != source_migrations:
            raise OfflineMigrationError(
                "candidate changed pre-existing schema migration rows"
            )
        if (
            len(candidate_migrations) != len(source_migrations) + 1
            or candidate_migrations[-1][0] != EXPECTED_TO_VERSION
            or candidate_migrations[-1][1] != EXPECTED_TO_MIGRATION
        ):
            raise OfflineMigrationError(
                "candidate did not append exactly the v16 migration record"
            )
        object_names = {
            str(row[0])
            for row in candidate.execute(
                "SELECT name FROM sqlite_master WHERE type IN ('table','index')"
            )
        }
        leftovers = object_names & (
            set(_V16_DROPPED_TABLES) | set(_V16_REMOVED_INDEXES)
        )
        if leftovers:
            raise OfflineMigrationError(
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
            "appended_migration_versions": [EXPECTED_TO_VERSION],
            "removed_tables": sorted(_V16_DROPPED_TABLES),
            "removed_indexes": sorted(_V16_REMOVED_INDEXES),
            "schema_objects": schema_objects,
        }
    finally:
        candidate.close()
        source.close()


def _read_backup_receipt(
    receipt_path: Path,
    *,
    source_database: Path,
    expected_source_sha256: str,
    migration_lock: Path,
) -> tuple[dict[str, Any], Path, dict[str, Any]]:
    identity = _require_regular_single_link(
        receipt_path,
        label="backup receipt",
    )
    _require_project_external(receipt_path, label="backup receipt")
    if identity.size > MAX_BACKUP_RECEIPT_BYTES:
        raise OfflineMigrationError("backup receipt is unexpectedly large")
    try:
        value = json.loads(receipt_path.read_text(encoding="utf-8"))
    except (OSError, UnicodeError, json.JSONDecodeError) as exc:
        raise OfflineMigrationError("backup receipt is not valid UTF-8 JSON") from exc
    if not isinstance(value, dict):
        raise OfflineMigrationError("backup receipt must be a JSON object")
    required = {
        "schema_version": str,
        "source_path": str,
        "source_sha256": str,
        "source_byte_size": int,
        "source_schema_version": int,
        "source_schema_migration": str,
        "backup_path": str,
        "backup_sha256": str,
        "backup_byte_size": int,
        "restore_verified": bool,
        "quick_check": str,
        "integrity_check": str,
        "foreign_key_violation_count": int,
        "migration_lock": str,
        "migration_lock_file": dict,
    }
    if set(value) != set(required):
        raise OfflineMigrationError(
            "backup receipt fields are not the exact supported contract"
        )
    for key, expected_type in required.items():
        item = value.get(key)
        if not isinstance(item, expected_type) or (
            expected_type is int and isinstance(item, bool)
        ):
            raise OfflineMigrationError(
                f"backup receipt field {key!r} has an invalid type"
            )
    if value["schema_version"] != BACKUP_RECEIPT_SCHEMA:
        raise OfflineMigrationError("backup receipt schema version is unsupported")
    declared_lock = Path(value["migration_lock"])
    if (
        not declared_lock.is_absolute()
        or not _same_existing_file(declared_lock, migration_lock)
        or value["migration_lock_file"] != _fingerprint(migration_lock)
    ):
        raise OfflineMigrationError("backup receipt migration lock binding differs")
    _require_project_external(declared_lock, label="backup receipt migration lock")
    declared_source = Path(value["source_path"])
    try:
        source_path_matches = (
            declared_source.is_absolute()
            and declared_source.resolve(strict=True)
            == source_database.resolve(strict=True)
        )
    except OSError:
        source_path_matches = False
    if not source_path_matches:
        raise OfflineMigrationError("backup receipt source_path does not match formal DB")
    if value["source_sha256"] != expected_source_sha256:
        raise OfflineMigrationError("backup receipt source SHA-256 does not match")
    _require_sha256(value["source_sha256"], label="backup receipt source SHA-256")
    _require_sha256(value["backup_sha256"], label="backup receipt backup SHA-256")
    if (
        value["source_schema_version"] != EXPECTED_FROM_VERSION
        or value["source_schema_migration"] != EXPECTED_FROM_MIGRATION
    ):
        raise OfflineMigrationError("backup receipt source schema is not exact v15")
    if value["restore_verified"] is not True:
        raise OfflineMigrationError("backup receipt restore_verified must be true")
    if (
        value["quick_check"] != "ok"
        or value["integrity_check"] != "ok"
        or value["foreign_key_violation_count"] != 0
    ):
        raise OfflineMigrationError("backup receipt validation verdict is not clean")
    declared_backup = Path(value["backup_path"])
    if not declared_backup.is_absolute():
        raise OfflineMigrationError("backup receipt backup_path must be absolute")
    backup_identity = _require_regular_single_link(
        declared_backup,
        label="verified backup",
    )
    backup_path = declared_backup.resolve(strict=True)
    if declared_backup != backup_path:
        raise OfflineMigrationError("backup receipt backup_path must be canonical")
    _require_project_external(backup_path, label="verified backup")
    if backup_path == source_database:
        raise OfflineMigrationError("verified backup must not be the formal DB")
    source_identity = _identity(source_database)
    if (
        value["source_byte_size"] != source_identity.size
        or value["backup_byte_size"] != backup_identity.size
    ):
        raise OfflineMigrationError("backup receipt byte sizes do not match files")
    if _sha256_file(backup_path) != value["backup_sha256"]:
        raise OfflineMigrationError("verified backup SHA-256 does not match receipt")
    validation = _validate_database(
        backup_path,
        expected_version=EXPECTED_FROM_VERSION,
        expected_migration=EXPECTED_FROM_MIGRATION,
        label="verified backup database",
    )
    lineage = _validate_backup_lineage(source_database, backup_path)
    return value, backup_path, {
        "file": _fingerprint(receipt_path),
        "database": _fingerprint(backup_path),
        "validation": validation,
        "lineage": lineage,
    }


def _copy_database(source: Path, staging: Path) -> None:
    source_connection = _connect_immutable(source, label="formal source database")
    destination = sqlite3.connect(staging, timeout=10)
    try:
        source_connection.backup(destination)
        destination.commit()
    except sqlite3.Error as exc:
        destination.rollback()
        raise OfflineMigrationError(f"SQLite backup into candidate failed: {exc}") from exc
    finally:
        destination.close()
        source_connection.close()


def _migrate_staging_database(
    staging: Path,
    migration_runner: Callable[[sqlite3.Connection], None],
) -> None:
    connection = sqlite3.connect(staging, timeout=10)
    connection.row_factory = sqlite3.Row
    try:
        configure_connection_safety(connection)
        connection.execute("PRAGMA journal_mode=WAL")
        migration_runner(connection)
        if connection.in_transaction:
            raise OfflineMigrationError(
                "migration runner returned with an active transaction"
            )
        connection.execute("PRAGMA wal_checkpoint(TRUNCATE)").fetchone()
        mode = str(connection.execute("PRAGMA journal_mode=DELETE").fetchone()[0])
        if mode.lower() != "delete":
            raise OfflineMigrationError(
                f"candidate journal mode did not become DELETE: {mode}"
            )
    except BaseException:
        if connection.in_transaction:
            connection.rollback()
        raise
    finally:
        connection.close()
    for suffix in SQLITE_TRANSIENT_SUFFIXES:
        if _path_exists(Path(f"{staging}{suffix}")):
            raise OfflineMigrationError(
                f"candidate staging sidecar remained after migration: {suffix}"
            )


def _assert_source_unchanged(
    source: Path,
    *,
    identity: FileIdentity,
    sha256: str,
    sidecars: Mapping[str, dict[str, Any]],
) -> None:
    if _identity(source) != identity or _sha256_file(source) != sha256:
        raise OfflineMigrationError("formal source changed during offline migration")
    for suffix in SQLITE_TRANSIENT_SUFFIXES:
        path = Path(f"{source}{suffix}")
        before = sidecars.get(suffix)
        if before is None:
            if _path_exists(path):
                raise OfflineMigrationError(
                    f"formal source sidecar appeared during migration: {suffix}"
                )
        elif (
            not _path_exists(path)
            or _fingerprint(path) != before
        ):
            raise OfflineMigrationError(
                f"formal source sidecar changed during migration: {suffix}"
            )


def _cleanup_owned_file(
    path: Path | None,
    identity: FileIdentity | None,
    errors: list[str],
) -> None:
    if path is None or identity is None or not _path_exists(path):
        return
    try:
        current = _identity(path)
        if (current.device, current.inode) != (identity.device, identity.inode):
            errors.append(f"cleanup refused changed path: {path}")
            return
        path.unlink()
        _fsync_directory(path.parent)
    except OSError as exc:
        errors.append(f"cleanup failed for {path}: {exc}")


def _cleanup_staging(path: Path | None, errors: list[str]) -> None:
    if path is None:
        return
    for candidate in (
        path,
        *(Path(f"{path}{suffix}") for suffix in SQLITE_TRANSIENT_SUFFIXES),
    ):
        if not _path_exists(candidate):
            continue
        try:
            value = candidate.lstat()
            if stat.S_ISLNK(value.st_mode) or not stat.S_ISREG(value.st_mode):
                errors.append(f"cleanup isolated unsafe staging path: {candidate}")
                continue
            candidate.unlink()
        except OSError as exc:
            errors.append(f"cleanup failed for {candidate}: {exc}")
    try:
        _fsync_directory(path.parent)
    except OSError as exc:
        errors.append(f"cleanup directory sync failed: {exc}")


def prepare_verified_backup(
    *,
    source_database: Path,
    backup: Path,
    expected_source_sha256: str,
    from_version: int,
    freeze_lock: Path,
    migration_lock: Path,
    receipt: Path,
    holder_checker: Callable[[Sequence[Path]], list[dict[str, Any]]] | None = None,
    fault_injector: Callable[[str], None] | None = None,
) -> dict[str, Any]:
    """Create and independently restore-verify a project-external v15 backup."""

    source_database = source_database.absolute()
    backup = backup.absolute()
    freeze_lock = freeze_lock.absolute()
    migration_lock = migration_lock.absolute()
    receipt = receipt.absolute()
    expected_source_sha256 = _require_sha256(
        expected_source_sha256,
        label="expected source SHA-256",
    )
    if (
        os.environ.get("DCAR_TEST_DENY_FORMAL_DB") == "1"
        and (
            is_formal_database_path(
                source_database,
                formal_database=DEFAULT_DB,
            )
        )
    ):
        raise OfflineMigrationError(
            "test process attempted to open the formal DCar database"
        )
    if from_version != EXPECTED_FROM_VERSION:
        raise OfflineMigrationError("verified backup requires exact --from 15")
    if not is_formal_database_path(
        source_database,
        formal_database=FORMAL_DATABASE,
    ):
        raise OfflineMigrationError("source database must be the canonical formal DB")
    source_identity = _require_regular_single_link(
        source_database,
        label="formal source database",
    )
    if _sha256_file(source_database) != expected_source_sha256:
        raise OfflineMigrationError("formal source SHA-256 does not match expectation")
    freeze_identity = _require_regular_single_link(
        freeze_lock,
        label="operator freeze lock",
    )
    if freeze_lock.resolve(strict=True) != CANONICAL_OPERATOR_FREEZE_LOCK.resolve(
        strict=False
    ):
        raise OfflineMigrationError("operator freeze lock is not canonical")
    if freeze_identity.mode != 0o600:
        raise OfflineMigrationError("operator freeze lock permissions must be 0600")

    backup_parent = _require_directory(backup.parent, label="backup parent")
    backup = backup_parent / backup.name
    _require_project_external(backup, label="verified backup")
    receipt_parent = _require_directory(receipt.parent, label="backup receipt parent")
    receipt = receipt_parent / receipt.name
    _require_project_external(receipt, label="backup receipt")
    _require_project_external(migration_lock, label="migration lock")
    if _path_exists(backup):
        raise OfflineMigrationError("verified backup path must be new")
    if _path_exists(receipt):
        raise OfflineMigrationError("backup receipt path must be new")
    source_sidecars: dict[str, dict[str, Any]] = {}
    for suffix in SQLITE_TRANSIENT_SUFFIXES:
        sidecar = Path(f"{source_database}{suffix}")
        if _path_exists(sidecar):
            _require_regular_single_link(
                sidecar,
                label=f"formal source {suffix} sidecar",
            )
            source_sidecars[suffix] = _fingerprint(sidecar)
    if source_sidecars.get("-wal", {}).get("size", 0) != 0:
        raise OfflineMigrationError("formal source WAL must be absent or empty")
    if "-journal" in source_sidecars:
        raise OfflineMigrationError("formal source rollback journal must be absent")
    _require_distinct_paths(
        (
            source_database,
            backup,
            freeze_lock,
            migration_lock,
            receipt,
            *(Path(f"{source_database}{suffix}") for suffix in source_sidecars),
        ),
        label="verified backup paths",
    )

    checker = holder_checker or _database_handles
    staging: Path | None = None
    restore_staging: Path | None = None
    backup_identity: FileIdentity | None = None
    receipt_identity: FileIdentity | None = None
    cleanup_errors: list[str] = []
    try:
        with _exclusive_migration_lock(
            migration_lock,
            allow_existing=False,
        ) as migration_lock_lease:
            handles = checker((source_database,))
            if handles:
                raise OfflineMigrationError(
                    "formal database handles are still open: "
                    + json.dumps(handles, ensure_ascii=False, sort_keys=True)
                )
            _validate_database(
                source_database,
                expected_version=EXPECTED_FROM_VERSION,
                expected_migration=EXPECTED_FROM_MIGRATION,
                label="formal source database",
            )
            _assert_source_unchanged(
                source_database,
                identity=source_identity,
                sha256=expected_source_sha256,
                sidecars=source_sidecars,
            )
            _checkpoint("after_backup_preflight", fault_injector)

            descriptor, raw_staging = tempfile.mkstemp(
                prefix=f".{backup.name}.preparing-",
                dir=backup_parent,
            )
            os.fchmod(descriptor, 0o600)
            os.close(descriptor)
            staging = Path(raw_staging)
            staging_inode = _identity(staging).inode
            _copy_database(source_database, staging)
            if _identity(staging).inode != staging_inode:
                raise OfflineMigrationError("verified backup staging identity changed")
            _checkpoint("after_verified_backup_copy", fault_injector)

            backup_validation = _validate_database(
                staging,
                expected_version=EXPECTED_FROM_VERSION,
                expected_migration=EXPECTED_FROM_MIGRATION,
                label="verified backup database",
            )
            backup_lineage = _validate_backup_lineage(source_database, staging)
            _checkpoint("after_verified_backup_validation", fault_injector)

            descriptor, raw_restore = tempfile.mkstemp(
                prefix=f".{backup.name}.restore-check-",
                dir=backup_parent,
            )
            os.fchmod(descriptor, 0o600)
            os.close(descriptor)
            restore_staging = Path(raw_restore)
            _copy_database(staging, restore_staging)
            restore_validation = _validate_database(
                restore_staging,
                expected_version=EXPECTED_FROM_VERSION,
                expected_migration=EXPECTED_FROM_MIGRATION,
                label="restored backup database",
            )
            restore_lineage = _validate_backup_lineage(
                source_database,
                restore_staging,
            )
            if restore_validation != backup_validation or restore_lineage != backup_lineage:
                raise OfflineMigrationError(
                    "independent restore verification differs from prepared backup"
                )
            _cleanup_staging(restore_staging, cleanup_errors)
            restore_staging = None
            if cleanup_errors:
                raise OfflineMigrationError(
                    f"independent restore cleanup failed: {cleanup_errors}"
                )
            _checkpoint("after_restore_validation", fault_injector)

            _assert_source_unchanged(
                source_database,
                identity=source_identity,
                sha256=expected_source_sha256,
                sidecars=source_sidecars,
            )
            if _identity(freeze_lock) != freeze_identity:
                raise OfflineMigrationError("operator freeze lock changed")
            handles = checker((source_database,))
            if handles:
                raise OfflineMigrationError(
                    "formal database handles appeared during backup: "
                    + json.dumps(handles, ensure_ascii=False, sort_keys=True)
                )
            if _path_exists(backup):
                raise OfflineMigrationError("verified backup path appeared during copy")
            os.link(staging, backup, follow_symlinks=False)
            backup_identity = _identity(backup)
            staging.unlink()
            staging = None
            os.chmod(backup, 0o600)
            _fsync_file(backup)
            _fsync_directory(backup_parent)
            _checkpoint("after_verified_backup_promoted", fault_injector)

            backup_fingerprint = _fingerprint(backup)
            result: dict[str, Any] = {
                "schema_version": BACKUP_RECEIPT_SCHEMA,
                "source_path": str(source_database),
                "source_sha256": expected_source_sha256,
                "source_byte_size": source_identity.size,
                "source_schema_version": EXPECTED_FROM_VERSION,
                "source_schema_migration": EXPECTED_FROM_MIGRATION,
                "backup_path": str(backup),
                "backup_sha256": backup_fingerprint["sha256"],
                "backup_byte_size": backup_fingerprint["size"],
                "restore_verified": True,
                "quick_check": backup_validation["quick_check"],
                "integrity_check": backup_validation["integrity_check"],
                "foreign_key_violation_count": backup_validation[
                    "foreign_key_violation_count"
                ],
                "migration_lock": str(migration_lock),
                "migration_lock_file": {
                    **asdict(migration_lock_lease.identity),
                    "sha256": _sha256_file(migration_lock),
                },
            }
            receipt_identity = _write_json_exclusive(receipt, result)
            _checkpoint("after_backup_receipt_written", fault_injector)
            _assert_source_unchanged(
                source_database,
                identity=source_identity,
                sha256=expected_source_sha256,
                sidecars=source_sidecars,
            )
            if _identity(freeze_lock) != freeze_identity:
                raise OfflineMigrationError(
                    "operator freeze lock changed before backup completion"
                )
            handles = checker((source_database,))
            if handles:
                raise OfflineMigrationError(
                    "formal database handles appeared before backup completion: "
                    + json.dumps(handles, ensure_ascii=False, sort_keys=True)
                )
            migration_lock_lease.verify_for_commit()
            return result
    except BaseException as exc:
        _cleanup_owned_file(receipt, receipt_identity, cleanup_errors)
        _cleanup_owned_file(backup, backup_identity, cleanup_errors)
        _cleanup_staging(restore_staging, cleanup_errors)
        _cleanup_staging(staging, cleanup_errors)
        try:
            _assert_source_unchanged(
                source_database,
                identity=source_identity,
                sha256=expected_source_sha256,
                sidecars=source_sidecars,
            )
        except Exception as source_error:
            cleanup_errors.append(f"source verification failed: {source_error}")
        if cleanup_errors:
            raise OfflineMigrationError(
                "verified backup failed with incomplete cleanup: "
                f"{type(exc).__name__}: {exc}; cleanup={cleanup_errors}"
            ) from exc
        if isinstance(exc, OfflineMigrationError):
            raise
        raise OfflineMigrationError(
            f"verified backup failed: {type(exc).__name__}: {exc}"
        ) from exc


def build_migration_candidate(
    *,
    source_database: Path,
    candidate: Path,
    expected_source_sha256: str,
    from_version: int,
    to_version: int,
    freeze_lock: Path,
    migration_lock: Path,
    backup_receipt: Path,
    receipt: Path,
    holder_checker: Callable[[Sequence[Path]], list[dict[str, Any]]] | None = None,
    fault_injector: Callable[[str], None] | None = None,
    migration_runner: Callable[[sqlite3.Connection], None] | None = None,
) -> dict[str, Any]:
    """Create a verified candidate while leaving ``source_database`` untouched."""

    source_database = source_database.absolute()
    candidate = candidate.absolute()
    freeze_lock = freeze_lock.absolute()
    migration_lock = migration_lock.absolute()
    backup_receipt = backup_receipt.absolute()
    receipt = receipt.absolute()
    expected_source_sha256 = _require_sha256(
        expected_source_sha256,
        label="expected source SHA-256",
    )
    if (
        os.environ.get("DCAR_TEST_DENY_FORMAL_DB") == "1"
        and (
            is_formal_database_path(
                source_database,
                formal_database=DEFAULT_DB,
            )
        )
    ):
        raise OfflineMigrationError(
            "test process attempted to open the formal DCar database"
        )
    if from_version != EXPECTED_FROM_VERSION or to_version != EXPECTED_TO_VERSION:
        raise OfflineMigrationError("offline migration requires exact --from 15 --to 16")
    if not is_formal_database_path(
        source_database,
        formal_database=FORMAL_DATABASE,
    ):
        raise OfflineMigrationError("source database must be the canonical formal DB")
    source_identity = _require_regular_single_link(
        source_database,
        label="formal source database",
    )
    if _sha256_file(source_database) != expected_source_sha256:
        raise OfflineMigrationError("formal source SHA-256 does not match expectation")
    freeze_identity = _require_regular_single_link(
        freeze_lock,
        label="operator freeze lock",
    )
    if freeze_lock.resolve(strict=True) != CANONICAL_OPERATOR_FREEZE_LOCK.resolve(
        strict=False
    ):
        raise OfflineMigrationError("operator freeze lock is not canonical")
    if freeze_identity.mode != 0o600:
        raise OfflineMigrationError("operator freeze lock permissions must be 0600")
    candidate_parent = _require_directory(candidate.parent, label="candidate parent")
    candidate = candidate_parent / candidate.name
    _require_project_external(candidate, label="candidate")
    if _path_exists(candidate):
        raise OfflineMigrationError("candidate path must be new")
    if candidate_parent.stat().st_dev != source_identity.device:
        raise OfflineMigrationError(
            "candidate must share the formal database filesystem for atomic install"
        )
    receipt_parent = _require_directory(receipt.parent, label="receipt parent")
    receipt = receipt_parent / receipt.name
    _require_project_external(receipt, label="migration receipt")
    _require_project_external(migration_lock, label="migration lock")
    if _path_exists(receipt):
        raise OfflineMigrationError("migration receipt path must be new")
    _, preflight_backup_path, _ = _read_backup_receipt(
        backup_receipt,
        source_database=source_database.resolve(strict=True),
        expected_source_sha256=expected_source_sha256,
        migration_lock=migration_lock,
    )
    source_sidecars: dict[str, dict[str, Any]] = {}
    for suffix in SQLITE_TRANSIENT_SUFFIXES:
        path = Path(f"{source_database}{suffix}")
        if _path_exists(path):
            _require_regular_single_link(path, label=f"formal source {suffix} sidecar")
            source_sidecars[suffix] = _fingerprint(path)
    if source_sidecars.get("-wal", {}).get("size", 0) != 0:
        raise OfflineMigrationError("formal source WAL must be absent or empty")
    if "-journal" in source_sidecars:
        raise OfflineMigrationError("formal source rollback journal must be absent")
    _require_distinct_paths(
        (
            source_database,
            candidate,
            freeze_lock,
            backup_receipt,
            preflight_backup_path,
            receipt,
            migration_lock,
            *(Path(f"{source_database}{suffix}") for suffix in source_sidecars),
        ),
        label="offline migration paths",
    )
    checker = holder_checker or _database_handles
    runner = migration_runner or initialize_database

    staging: Path | None = None
    staging_identity: FileIdentity | None = None
    candidate_identity: FileIdentity | None = None
    receipt_identity: FileIdentity | None = None
    cleanup_errors: list[str] = []
    try:
        with _exclusive_migration_lock(migration_lock) as migration_lock_lease:
            handles = checker((source_database,))
            if handles:
                raise OfflineMigrationError(
                    "formal database handles are still open: "
                    + json.dumps(handles, ensure_ascii=False, sort_keys=True)
                )
            source_validation = _validate_database(
                source_database,
                expected_version=EXPECTED_FROM_VERSION,
                expected_migration=EXPECTED_FROM_MIGRATION,
                label="formal source database",
            )
            backup_value, backup_path, backup_validation = _read_backup_receipt(
                backup_receipt,
                source_database=source_database.resolve(strict=True),
                expected_source_sha256=expected_source_sha256,
                migration_lock=migration_lock,
            )
            backup_receipt_identity = _identity(backup_receipt)
            backup_receipt_sha256 = _sha256_file(backup_receipt)
            backup_identity = _identity(backup_path)
            _assert_source_unchanged(
                source_database,
                identity=source_identity,
                sha256=expected_source_sha256,
                sidecars=source_sidecars,
            )
            _checkpoint("after_preflight", fault_injector)

            descriptor, raw_staging = tempfile.mkstemp(
                prefix=f".{candidate.name}.migrating-",
                dir=candidate_parent,
            )
            os.fchmod(descriptor, 0o600)
            os.close(descriptor)
            staging = Path(raw_staging)
            staging_identity = _identity(staging)
            _copy_database(source_database, staging)
            if _identity(staging).inode != staging_identity.inode:
                raise OfflineMigrationError("candidate staging identity changed")
            _checkpoint("after_backup_copy", fault_injector)

            _migrate_staging_database(staging, runner)
            _checkpoint("after_candidate_migration", fault_injector)
            candidate_validation = _validate_database(
                staging,
                expected_version=EXPECTED_TO_VERSION,
                expected_migration=EXPECTED_TO_MIGRATION,
                label="candidate database",
            )
            lineage = _validate_lineage(source_database, staging)
            _checkpoint("after_candidate_validation", fault_injector)

            _assert_source_unchanged(
                source_database,
                identity=source_identity,
                sha256=expected_source_sha256,
                sidecars=source_sidecars,
            )
            if _identity(freeze_lock) != freeze_identity:
                raise OfflineMigrationError("operator freeze lock changed")
            if (
                _identity(backup_receipt) != backup_receipt_identity
                or _sha256_file(backup_receipt) != backup_receipt_sha256
            ):
                raise OfflineMigrationError("backup receipt changed during migration")
            if (
                _identity(backup_path) != backup_identity
                or _sha256_file(backup_path) != backup_value["backup_sha256"]
            ):
                raise OfflineMigrationError("verified backup changed during migration")
            handles = checker((source_database,))
            if handles:
                raise OfflineMigrationError(
                    "formal database handles appeared during migration: "
                    + json.dumps(handles, ensure_ascii=False, sort_keys=True)
                )
            if _path_exists(candidate):
                raise OfflineMigrationError("candidate path appeared during migration")
            os.link(staging, candidate, follow_symlinks=False)
            candidate_identity = _identity(candidate)
            staging.unlink()
            staging = None
            staging_identity = None
            os.chmod(candidate, 0o600)
            _fsync_file(candidate)
            _fsync_directory(candidate_parent)
            _checkpoint("after_candidate_promoted", fault_injector)

            candidate_fingerprint = _fingerprint(candidate)
            result: dict[str, Any] = {
                "schema_version": MIGRATION_RECEIPT_SCHEMA,
                "status": "candidate_ready",
                "completed_at": _utc_now(),
                "from_version": from_version,
                "from_migration": EXPECTED_FROM_MIGRATION,
                "to_version": to_version,
                "to_migration": EXPECTED_TO_MIGRATION,
                "formal_source": {
                    "path": str(source_database),
                    "file": _fingerprint(source_database),
                    "sidecars": source_sidecars,
                    "validation": source_validation,
                },
                "verified_backup": {
                    "receipt_path": str(backup_receipt),
                    "receipt_sha256": backup_receipt_sha256,
                    "path": str(backup_path),
                    **backup_validation,
                },
                "candidate": {
                    "path": str(candidate),
                    "file": candidate_fingerprint,
                    "validation": candidate_validation,
                },
                "lineage": lineage,
                "operator_freeze_lock": str(freeze_lock),
                "migration_lock": str(migration_lock),
                "migration_lock_file": {
                    **asdict(migration_lock_lease.identity),
                    "sha256": _sha256_file(migration_lock),
                },
                "database_handles": [],
                "receipt": str(receipt),
            }
            receipt_identity = _write_json_exclusive(receipt, result)
            _checkpoint("after_receipt_written", fault_injector)
            _assert_source_unchanged(
                source_database,
                identity=source_identity,
                sha256=expected_source_sha256,
                sidecars=source_sidecars,
            )
            if _identity(freeze_lock) != freeze_identity:
                raise OfflineMigrationError(
                    "operator freeze lock changed before migration completion"
                )
            handles = checker((source_database,))
            if handles:
                raise OfflineMigrationError(
                    "formal database handles appeared before migration completion: "
                    + json.dumps(handles, ensure_ascii=False, sort_keys=True)
                )
            migration_lock_lease.verify_for_commit()
            return result
    except BaseException as exc:
        _cleanup_owned_file(receipt, receipt_identity, cleanup_errors)
        _cleanup_owned_file(candidate, candidate_identity, cleanup_errors)
        _cleanup_staging(staging, cleanup_errors)
        try:
            _assert_source_unchanged(
                source_database,
                identity=source_identity,
                sha256=expected_source_sha256,
                sidecars=source_sidecars,
            )
        except Exception as source_error:
            cleanup_errors.append(f"source verification failed: {source_error}")
        if cleanup_errors:
            raise OfflineMigrationError(
                "offline migration failed with incomplete cleanup: "
                f"{type(exc).__name__}: {exc}; cleanup={cleanup_errors}"
            ) from exc
        if isinstance(exc, OfflineMigrationError):
            raise
        raise OfflineMigrationError(
            f"offline migration failed: {type(exc).__name__}: {exc}"
        ) from exc


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        description=__doc__,
        epilog=(
            "Use 'prepare-backup --help' for the verified backup step; "
            "'build-candidate' is an explicit alias for this default mode."
        ),
    )
    parser.add_argument("--source-db", type=Path, required=True)
    parser.add_argument("--candidate", type=Path, required=True)
    parser.add_argument("--expected-source-sha256", required=True)
    parser.add_argument("--from", dest="from_version", type=int, required=True)
    parser.add_argument("--to", dest="to_version", type=int, required=True)
    parser.add_argument("--freeze-lock", type=Path, required=True)
    parser.add_argument("--migration-lock", type=Path, required=True)
    parser.add_argument("--backup-receipt", type=Path, required=True)
    parser.add_argument("--receipt", type=Path, required=True)
    return parser


def build_backup_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        description=(
            "Create a project-external, independently restore-verified v15 "
            "backup and O_EXCL dcar-v16-offline-backup-v1 receipt."
        )
    )
    parser.add_argument("--source-db", type=Path, required=True)
    parser.add_argument("--backup", type=Path, required=True)
    parser.add_argument("--expected-source-sha256", required=True)
    parser.add_argument("--from", dest="from_version", type=int, required=True)
    parser.add_argument("--freeze-lock", type=Path, required=True)
    parser.add_argument("--migration-lock", type=Path, required=True)
    parser.add_argument("--receipt", type=Path, required=True)
    return parser


def main(argv: Sequence[str] | None = None) -> int:
    values = list(sys.argv[1:] if argv is None else argv)
    mode = "build-candidate"
    if values and values[0] in {"prepare-backup", "build-candidate"}:
        mode = values.pop(0)
    try:
        if mode == "prepare-backup":
            arguments = build_backup_parser().parse_args(values)
            result = prepare_verified_backup(
                source_database=arguments.source_db,
                backup=arguments.backup,
                expected_source_sha256=arguments.expected_source_sha256,
                from_version=arguments.from_version,
                freeze_lock=arguments.freeze_lock,
                migration_lock=arguments.migration_lock,
                receipt=arguments.receipt,
            )
        else:
            arguments = build_parser().parse_args(values)
            result = build_migration_candidate(
                source_database=arguments.source_db,
                candidate=arguments.candidate,
                expected_source_sha256=arguments.expected_source_sha256,
                from_version=arguments.from_version,
                to_version=arguments.to_version,
                freeze_lock=arguments.freeze_lock,
                migration_lock=arguments.migration_lock,
                backup_receipt=arguments.backup_receipt,
                receipt=arguments.receipt,
            )
    except OfflineMigrationError as exc:
        print(f"offline schema migration refused: {exc}", file=sys.stderr)
        return 1
    print(json.dumps(result, ensure_ascii=False, sort_keys=True))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
