#!/usr/bin/env python3
"""Atomically install one pre-validated schema-v13 writer database candidate.

This is a deliberately narrow cutover tool.  It only accepts the repository's
canonical formal database and operator-freeze lock, consumes an explicitly
hashed candidate from the same filesystem, preserves the old database and its
SQLite sidecars with atomic renames, and writes a non-overwritable receipt.
It does not migrate schemas, activate evaluation-v9, start services, use the
network, or invoke providers.
"""

from __future__ import annotations

import argparse
import hashlib
import json
import os
import re
import sqlite3
import stat
import subprocess
import sys
from collections.abc import Callable, Mapping, Sequence
from dataclasses import asdict, dataclass
from datetime import datetime, timezone
from pathlib import Path
from typing import Any


PROJECT_ROOT = Path(__file__).resolve().parents[1]
PACKAGE_ROOT = PROJECT_ROOT / "src" / "dcar_eval"
if str(PACKAGE_ROOT) not in sys.path:
    sys.path.insert(0, str(PACKAGE_ROOT))

from v8.release_management_v9 import (  # noqa: E402
    REPORT_VERSION,
    SOURCE_RELEASE_ID,
    SOURCE_RULE_VERSION,
    TAXONOMY_VERSION,
)
from v8.storage import (  # noqa: E402
    CURRENT_SCHEMA_MIGRATION_NAME,
    DEFAULT_DB,
    SCHEMA_MIGRATION_NAMES,
    SCHEMA_VERSION,
    configure_connection_safety,
    metric_observation_sha256,
    require_schema_compatibility,
)


FORMAL_DATABASE = DEFAULT_DB
FORMAL_BACKUP_ROOT = FORMAL_DATABASE.parent / "backups"
CANONICAL_OPERATOR_FREEZE_LOCK = PROJECT_ROOT / "runtime" / "operator-freeze.lock"
RECEIPT_SCHEMA = "dcar-writer-database-candidate-install-v1"
HEX_SHA256 = re.compile(r"[0-9a-f]{64}", re.ASCII)
SQLITE_SIDECAR_SUFFIXES = ("-wal", "-shm")
SQLITE_TRANSIENT_SUFFIXES = (*SQLITE_SIDECAR_SUFFIXES, "-journal")
EXPECTED_SOURCE_SCHEMA_VERSION = 11
EXPECTED_SOURCE_CONTENT_COUNT = 61_800
EXPECTED_SOURCE_METRIC_SNAPSHOT_COUNT = 65_145
EXPECTED_SOURCE_SCHEDULER_RUN_COUNT = 33
MIGRATION_ONLY_TABLES = frozenset(
    {"content_metric_observations", "scheduler_run_attempts"}
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


def _same_identity(left: FileIdentity, right: FileIdentity) -> bool:
    return left.device == right.device and left.inode == right.inode


def _require_no_traversal(path: Path, *, label: str) -> None:
    if ".." in path.parts:
        raise CandidateInstallError(f"{label} must not contain path traversal")


def _fingerprint(path: Path) -> dict[str, Any]:
    identity = _stat_identity(path)
    return {**asdict(identity), "sha256": _sha256_file(path)}


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
                connection, supported_versions=frozenset({SCHEMA_VERSION})
            )
        except Exception as error:
            raise CandidateInstallError(
                f"candidate schema compatibility failed: {error}"
            ) from error
        user_version = int(connection.execute("PRAGMA user_version").fetchone()[0])
        migration_rows = connection.execute(
            "SELECT version,name FROM schema_migrations WHERE version=?",
            (SCHEMA_VERSION,),
        ).fetchall()
        maximum = connection.execute(
            "SELECT MAX(version) FROM schema_migrations"
        ).fetchone()[0]
        if (
            user_version != SCHEMA_VERSION
            or len(migration_rows) != 1
            or int(migration_rows[0]["version"]) != SCHEMA_VERSION
            or str(migration_rows[0]["name"]) != CURRENT_SCHEMA_MIGRATION_NAME
            or int(maximum) != SCHEMA_VERSION
        ):
            raise CandidateInstallError(
                "candidate schema-v13 migration identity is not exact"
            )

        active = connection.execute(
            """
            SELECT id,rule_version,taxonomy_version,status
            FROM evaluation_releases WHERE status='active' ORDER BY id
            """
        ).fetchall()
        expected_active = (
            SOURCE_RELEASE_ID,
            SOURCE_RULE_VERSION,
            TAXONOMY_VERSION,
            "active",
        )
        actual_active = [tuple(str(value) for value in row) for row in active]
        if actual_active != [expected_active]:
            raise CandidateInstallError(
                "candidate must have exactly one active evaluation-v8 source release"
            )
        revision_count = int(
            connection.execute(
                "SELECT COUNT(*) FROM report_revisions WHERE contract_version=?",
                (REPORT_VERSION,),
            ).fetchone()[0]
        )
        if revision_count != 0:
            raise CandidateInstallError(
                "candidate must contain zero v8.6 report revisions before v9 cutover"
            )
        return {
            "quick_check": quick_rows,
            "integrity_check": integrity_rows,
            "foreign_key_violation_count": 0,
            "schema_version": user_version,
            "schema_migration_name": str(migration_rows[0]["name"]),
            "active_release": {
                "id": str(active[0]["id"]),
                "rule_version": str(active[0]["rule_version"]),
                "taxonomy_version": str(active[0]["taxonomy_version"]),
            },
            "v8_6_report_revision_count": revision_count,
            "wal_byte_size": 0,
            "sidecars_absent": list(SQLITE_TRANSIENT_SUFFIXES),
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


def _validate_metric_observation_projection(
    source: sqlite3.Connection,
    candidate: sqlite3.Connection,
) -> dict[str, Any]:
    source_count = int(
        source.execute("SELECT COUNT(*) FROM content_metric_snapshots").fetchone()[0]
    )
    if source_count != EXPECTED_SOURCE_METRIC_SNAPSHOT_COUNT:
        raise CandidateInstallError(
            "formal source metric snapshot count changed: "
            f"expected={EXPECTED_SOURCE_METRIC_SNAPSHOT_COUNT},actual={source_count}"
        )
    candidate_count = int(
        candidate.execute(
            "SELECT COUNT(*) FROM content_metric_observations"
        ).fetchone()[0]
    )
    if candidate_count != source_count:
        raise CandidateInstallError(
            "candidate metric observation baseline count differs from source snapshots: "
            f"snapshots={source_count},observations={candidate_count}"
        )

    rows = candidate.execute(
        """
        SELECT
            s.id snapshot_id,
            s.content_id snapshot_content_id,
            s.captured_at snapshot_captured_at,
            s.window_key snapshot_window_key,
            s.view_count snapshot_view_count,
            s.comment_count snapshot_comment_count,
            s.like_count snapshot_like_count,
            s.share_count snapshot_share_count,
            s.collect_count snapshot_collect_count,
            s.status snapshot_status,
            s.source snapshot_source,
            s.raw_response_id snapshot_raw_response_id,
            s.metadata_json snapshot_metadata_json,
            COALESCE(
                (
                    SELECT ci.platform_identity_key
                    FROM content_identities ci
                    WHERE ci.content_id=s.content_id
                    ORDER BY ci.is_primary DESC,ci.id
                    LIMIT 1
                ),
                'link:' || c.link_id
            ) expected_subject_key,
            o.content_id observation_content_id,
            o.subject_key observation_subject_key,
            o.captured_at observation_captured_at,
            o.window_key observation_window_key,
            o.view_count observation_view_count,
            o.comment_count observation_comment_count,
            o.like_count observation_like_count,
            o.share_count observation_share_count,
            o.collect_count observation_collect_count,
            o.status observation_status,
            o.source observation_source,
            o.raw_response_id observation_raw_response_id,
            o.metadata_json observation_metadata_json,
            o.observation_origin,
            o.legacy_snapshot_id,
            o.observation_sha256
        FROM content_metric_snapshots s
        JOIN content_items c ON c.id=s.content_id
        LEFT JOIN content_metric_observations o ON o.legacy_snapshot_id=s.id
        ORDER BY s.id
        """
    ).fetchall()
    if len(rows) != source_count:
        raise CandidateInstallError(
            "candidate metric observation projection did not cover every source snapshot"
        )
    mismatches = 0
    for row in rows:
        expected_values = (
            row["snapshot_content_id"],
            row["expected_subject_key"],
            row["snapshot_captured_at"],
            row["snapshot_window_key"],
            row["snapshot_view_count"],
            row["snapshot_comment_count"],
            row["snapshot_like_count"],
            row["snapshot_share_count"],
            row["snapshot_collect_count"],
            row["snapshot_status"],
            row["snapshot_source"],
            row["snapshot_raw_response_id"],
            row["snapshot_metadata_json"],
            "legacy_snapshot_baseline",
            row["snapshot_id"],
        )
        actual_values = (
            row["observation_content_id"],
            row["observation_subject_key"],
            row["observation_captured_at"],
            row["observation_window_key"],
            row["observation_view_count"],
            row["observation_comment_count"],
            row["observation_like_count"],
            row["observation_share_count"],
            row["observation_collect_count"],
            row["observation_status"],
            row["observation_source"],
            row["observation_raw_response_id"],
            row["observation_metadata_json"],
            row["observation_origin"],
            row["legacy_snapshot_id"],
        )
        expected_digest = metric_observation_sha256(
            observation_origin="legacy_snapshot_baseline",
            legacy_snapshot_id=int(row["snapshot_id"]),
            subject_key=str(row["expected_subject_key"]),
            captured_at=str(row["snapshot_captured_at"]),
            window_key=str(row["snapshot_window_key"]),
            view_count=row["snapshot_view_count"],
            comment_count=row["snapshot_comment_count"],
            like_count=row["snapshot_like_count"],
            share_count=row["snapshot_share_count"],
            collect_count=row["snapshot_collect_count"],
            status=str(row["snapshot_status"]),
            source=str(row["snapshot_source"]),
            raw_response_id=row["snapshot_raw_response_id"],
            metadata_json=str(row["snapshot_metadata_json"]),
        )
        if (
            expected_values != actual_values
            or row["observation_sha256"] != expected_digest
        ):
            mismatches += 1
    if mismatches:
        raise CandidateInstallError(
            f"candidate metric observation projection has {mismatches} mismatches"
        )
    return {
        "source_snapshot_count": source_count,
        "candidate_observation_count": candidate_count,
        "field_mismatch_count": 0,
    }


def _validate_scheduler_attempt_projection(
    source: sqlite3.Connection,
    candidate: sqlite3.Connection,
) -> dict[str, Any]:
    source_count = int(
        source.execute("SELECT COUNT(*) FROM scheduler_runs").fetchone()[0]
    )
    if source_count != EXPECTED_SOURCE_SCHEDULER_RUN_COUNT:
        raise CandidateInstallError(
            "formal source scheduler run count changed: "
            f"expected={EXPECTED_SOURCE_SCHEDULER_RUN_COUNT},actual={source_count}"
        )
    candidate_count = int(
        candidate.execute("SELECT COUNT(*) FROM scheduler_run_attempts").fetchone()[0]
    )
    mismatch_count = int(
        candidate.execute(
            """
            SELECT COUNT(*)
            FROM scheduler_runs r
            LEFT JOIN scheduler_run_attempts a ON a.scheduler_run_id=r.id
            WHERE a.id IS NULL
               OR a.attempt_number!=1
               OR a.invocation_source!='legacy_migration'
               OR a.status IS NOT r.status
               OR a.started_at IS NOT r.started_at
               OR a.completed_at IS NOT r.completed_at
               OR a.details_json IS NOT r.details_json
            """
        ).fetchone()[0]
    )
    if candidate_count != source_count or mismatch_count:
        raise CandidateInstallError(
            "candidate scheduler attempt baseline differs from source runs: "
            f"runs={source_count},attempts={candidate_count},mismatches={mismatch_count}"
        )
    return {
        "source_run_count": source_count,
        "candidate_attempt_count": candidate_count,
        "field_mismatch_count": 0,
    }


def _validate_source_candidate_lineage(
    source_path: Path,
    candidate_path: Path,
) -> dict[str, Any]:
    """Prove that candidate is the lossless v11-to-v13 migration of source."""

    source = _connect_immutable_database(source_path, label="formal source database")
    candidate = _connect_candidate(candidate_path)
    try:
        try:
            source_version = require_schema_compatibility(
                source,
                supported_versions=frozenset({EXPECTED_SOURCE_SCHEMA_VERSION}),
            )
            candidate_version = require_schema_compatibility(
                candidate, supported_versions=frozenset({SCHEMA_VERSION})
            )
        except Exception as error:
            raise CandidateInstallError(
                f"source/candidate schema lineage is incompatible: {error}"
            ) from error
        source_quick = [str(row[0]) for row in source.execute("PRAGMA quick_check")]
        source_integrity = [
            str(row[0]) for row in source.execute("PRAGMA integrity_check")
        ]
        source_foreign_keys = source.execute("PRAGMA foreign_key_check").fetchall()
        if source_quick != ["ok"] or source_integrity != ["ok"]:
            raise CandidateInstallError(
                "formal source database integrity validation failed"
            )
        if source_foreign_keys:
            raise CandidateInstallError(
                "formal source database has foreign-key violations: "
                f"{len(source_foreign_keys)}"
            )

        source_tables = _table_names(source)
        candidate_tables = _table_names(candidate)
        missing_tables = source_tables - candidate_tables
        allowed_additions = (
            MIGRATION_ONLY_TABLES
            if EXPECTED_SOURCE_SCHEMA_VERSION == 11
            else frozenset()
        )
        unexpected_tables = (candidate_tables - source_tables) - allowed_additions
        absent_additions = allowed_additions - (candidate_tables - source_tables)
        if missing_tables or unexpected_tables or absent_additions:
            raise CandidateInstallError(
                "candidate table lineage differs from formal source: "
                f"missing={sorted(missing_tables)},"
                f"unexpected={sorted(unexpected_tables)},"
                f"absent_migration_tables={sorted(absent_additions)}"
            )

        source_migrations = _schema_migration_rows(source)
        candidate_migrations = _schema_migration_rows(candidate)
        source_by_version = {row[0]: row for row in source_migrations}
        candidate_by_version = {row[0]: row for row in candidate_migrations}
        if any(candidate_by_version.get(row[0]) != row for row in source_migrations):
            raise CandidateInstallError(
                "candidate changed pre-existing schema migration records"
            )
        expected_versions = set(source_by_version) | {12, 13}
        if set(candidate_by_version) != expected_versions:
            raise CandidateInstallError(
                "candidate schema migrations must only append versions 12 and 13"
            )
        for version in (12, 13):
            expected_name = SCHEMA_MIGRATION_NAMES[version]
            row = candidate_by_version.get(version)
            if row is None or row[1] != expected_name:
                raise CandidateInstallError(
                    f"candidate schema migration {version} identity is not exact"
                )

        table_results: dict[str, dict[str, Any]] = {}
        aggregate = hashlib.sha256()
        for table in sorted(source_tables - {"schema_migrations"}):
            source_columns = _table_columns(source, table)
            candidate_columns = _table_columns(candidate, table)
            if candidate_columns != source_columns:
                raise CandidateInstallError(
                    f"candidate changed columns for pre-existing table {table}"
                )
            quoted_table = _quote_identifier(table)
            source_count = int(
                source.execute(f"SELECT COUNT(*) FROM {quoted_table}").fetchone()[0]
            )
            candidate_count = int(
                candidate.execute(f"SELECT COUNT(*) FROM {quoted_table}").fetchone()[0]
            )
            source_digest = _table_projection_sha256(source, table, source_columns)
            candidate_digest = _table_projection_sha256(
                candidate, table, candidate_columns
            )
            if candidate_count != source_count or candidate_digest != source_digest:
                raise CandidateInstallError(
                    f"candidate changed pre-existing table {table}: "
                    f"source_count={source_count},candidate_count={candidate_count},"
                    f"source_digest={source_digest},candidate_digest={candidate_digest}"
                )
            table_results[table] = {
                "row_count": source_count,
                "projection_sha256": source_digest,
            }
            aggregate.update(table.encode("utf-8"))
            aggregate.update(b"\0")
            aggregate.update(str(source_count).encode("ascii"))
            aggregate.update(b"\0")
            aggregate.update(source_digest.encode("ascii"))
            aggregate.update(b"\n")

        content_count = table_results.get("content_items", {}).get("row_count")
        if content_count != EXPECTED_SOURCE_CONTENT_COUNT:
            raise CandidateInstallError(
                "formal source content count changed: "
                f"expected={EXPECTED_SOURCE_CONTENT_COUNT},actual={content_count}"
            )

        source_sequences = {
            str(row["name"]): int(row["seq"])
            for row in source.execute("SELECT name,seq FROM sqlite_sequence").fetchall()
        }
        candidate_sequences = {
            str(row["name"]): int(row["seq"])
            for row in candidate.execute(
                "SELECT name,seq FROM sqlite_sequence"
            ).fetchall()
        }
        sequence_mismatches = {
            name: {"source": value, "candidate": candidate_sequences.get(name)}
            for name, value in source_sequences.items()
            if candidate_sequences.get(name) != value
        }
        if sequence_mismatches:
            raise CandidateInstallError(
                "candidate changed pre-existing AUTOINCREMENT sequences: "
                + json.dumps(sequence_mismatches, ensure_ascii=False, sort_keys=True)
            )

        metric_projection = _validate_metric_observation_projection(source, candidate)
        scheduler_projection = _validate_scheduler_attempt_projection(source, candidate)
        return {
            "source_schema_version": source_version,
            "candidate_schema_version": candidate_version,
            "source_quick_check": source_quick,
            "source_integrity_check": source_integrity,
            "source_foreign_key_violation_count": 0,
            "preexisting_table_count": len(table_results),
            "preexisting_tables": table_results,
            "aggregate_projection_sha256": aggregate.hexdigest(),
            "schema_migrations": [
                {"version": version, "name": name}
                for version, name, _ in candidate_migrations
            ],
            "metric_observation_projection": metric_projection,
            "scheduler_attempt_projection": scheduler_projection,
            "preexisting_sequence_count": len(source_sequences),
        }
    except sqlite3.Error as error:
        raise CandidateInstallError(
            f"source/candidate lineage validation failed: {error}"
        ) from error
    finally:
        candidate.close()
        source.close()


def _database_writer_handles(databases: Sequence[Path]) -> list[dict[str, Any]]:
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
    writers: list[dict[str, Any]] = []
    for line in result.stdout.splitlines()[1:]:
        parts = line.split()
        if len(parts) < 9:
            continue
        descriptor = parts[3]
        descriptor_match = re.fullmatch(r"\d+([rwu]).*", descriptor)
        if descriptor_match is not None and descriptor_match.group(1) in {"u", "w"}:
            try:
                pid = int(parts[1])
            except ValueError:
                pid = -1
            writers.append(
                {
                    "command": parts[0],
                    "pid": pid,
                    "descriptor": descriptor,
                    "path": parts[-1],
                }
            )
    return writers


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
    backup_directory = backup_directory.absolute()
    receipt = receipt.absolute()
    freeze_lock = freeze_lock.absolute()
    for path, label in (
        (formal_database, "formal writer database"),
        (candidate, "candidate database"),
        (backup_directory, "backup directory"),
        (receipt, "receipt"),
        (freeze_lock, "operator freeze lock"),
    ):
        _require_no_traversal(path, label=label)

    if formal_database.resolve(strict=False) != FORMAL_DATABASE.resolve(strict=False):
        raise CandidateInstallError(
            f"formal target must be exactly {FORMAL_DATABASE.resolve(strict=False)}"
        )
    formal_identity = _require_regular_single_link(
        formal_database, label="formal writer database"
    )
    candidate_identity = _require_regular_single_link(
        candidate, label="candidate database"
    )
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
    if candidate == receipt or candidate == backup_directory:
        raise CandidateInstallError("candidate path collides with an output path")

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
    expected_source_sha256: str,
    expected_candidate_sha256: str,
    backup_directory: Path,
    receipt: Path,
    freeze_lock: Path,
    fault_injector: Callable[[str], None] | None = None,
) -> dict[str, Any]:
    """Install ``candidate`` or restore every original path before raising."""

    expected_source_sha256 = _require_sha256(
        expected_source_sha256, label="expected source SHA-256"
    )
    expected_candidate_sha256 = _require_sha256(
        expected_candidate_sha256, label="expected candidate SHA-256"
    )
    formal_database = formal_database.absolute()
    candidate = candidate.absolute()
    backup_directory = backup_directory.absolute()
    receipt = receipt.absolute()
    freeze_lock = freeze_lock.absolute()

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
        backup_directory=backup_directory,
        receipt=receipt,
        freeze_lock=freeze_lock,
    )
    source_before = _fingerprint(formal_database)
    candidate_before = _fingerprint(candidate)
    if source_before["sha256"] != expected_source_sha256:
        raise CandidateInstallError("formal source SHA-256 does not match expectation")
    if candidate_before["sha256"] != expected_candidate_sha256:
        raise CandidateInstallError("candidate SHA-256 does not match expectation")
    candidate_validation = _validate_candidate_database(candidate)
    writers = _database_writer_handles((formal_database, candidate))
    if writers:
        raise CandidateInstallError(
            "database writer handles are still open: "
            + json.dumps(writers, ensure_ascii=False, sort_keys=True)
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
    lineage_validation = _validate_source_candidate_lineage(formal_database, candidate)

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
    writers = _database_writer_handles((formal_database, candidate))
    if writers:
        raise CandidateInstallError(
            "database writer handles appeared during preflight: "
            + json.dumps(writers, ensure_ascii=False, sort_keys=True)
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
        writers = _database_writer_handles((formal_database, candidate))
        if writers:
            raise CandidateInstallError(
                "database writer handles appeared immediately before cutover: "
                + json.dumps(writers, ensure_ascii=False, sort_keys=True)
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
        writers = _database_writer_handles((formal_database, candidate))
        if writers:
            raise CandidateInstallError(
                "database writer handles appeared after candidate installation: "
                + json.dumps(writers, ensure_ascii=False, sort_keys=True)
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
                "source_sha256": expected_source_sha256,
                "candidate_sha256": expected_candidate_sha256,
            },
            "before": {
                "database": source_before,
                "sidecars": source_sidecars,
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
            "writer_handles": writers,
            "rollback": "not_required",
            "receipt": str(receipt),
        }

        def receipt_created(identity: FileIdentity) -> None:
            nonlocal receipt_identity
            receipt_identity = identity

        _write_json_exclusive(receipt, result, on_created=receipt_created)
        _checkpoint("after_receipt_written", fault_injector)
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
                rollback_writers = _database_writer_handles(
                    (formal_database, candidate)
                )
                if rollback_writers:
                    rollback_paths_safe = False
                    rollback_errors.append(
                        "writer handles appeared during rollback: "
                        + json.dumps(
                            rollback_writers, ensure_ascii=False, sort_keys=True
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
    parser.add_argument("--expected-source-sha256", required=True)
    parser.add_argument("--expected-candidate-sha256", required=True)
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
            expected_source_sha256=arguments.expected_source_sha256,
            expected_candidate_sha256=arguments.expected_candidate_sha256,
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
