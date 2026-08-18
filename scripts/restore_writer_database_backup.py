#!/usr/bin/env python3
"""Atomically restore the receipt-bound v15 backup over a frozen v16 database.

This is the reverse cutover companion to ``migrate_v8_schema.py`` and
``install_writer_database_candidate.py``.  It never mutates the verified v15
backup.  The current v16 database and its sidecars are moved into a dedicated
rollback directory, and every failed durable transition restores those exact
v16 files before the command returns an error.
"""

from __future__ import annotations

import argparse
import json
import os
import sqlite3
import stat
import sys
import tempfile
from collections.abc import Callable, Mapping, Sequence
from dataclasses import asdict
from pathlib import Path
from typing import Any


SCRIPT_ROOT = Path(__file__).resolve().parent
if str(SCRIPT_ROOT) not in sys.path:
    sys.path.insert(0, str(SCRIPT_ROOT))

import install_writer_database_candidate as safety  # noqa: E402


FORMAL_DATABASE = safety.FORMAL_DATABASE
FORMAL_BACKUP_ROOT = safety.FORMAL_BACKUP_ROOT
CANONICAL_OPERATOR_FREEZE_LOCK = safety.CANONICAL_OPERATOR_FREEZE_LOCK
RESTORE_RECEIPT_SCHEMA = "dcar-writer-database-v15-restore-v1"
RESTORE_CHECKPOINTS = (
    "after_preflight",
    "after_rollback_directory_created",
    "after_v16_database_moved",
    "after_v16_wal_moved",
    "after_v16_shm_moved",
    "after_v15_installed",
    "after_installed_file_synced",
    "after_directories_synced",
    "after_post_restore_verification",
    "after_restore_receipt_written",
)


def _checkpoint(
    name: str,
    fault_injector: Callable[[str], None] | None,
) -> None:
    if fault_injector is not None:
        fault_injector(name)


def _same_existing_file(left: Path, right: Path) -> bool:
    return safety.same_database_path(left, right)


def _require_distinct_paths(paths: Sequence[Path]) -> None:
    locations: set[tuple[str, int, int] | tuple[str, int, int, str]] = set()
    for path in paths:
        if safety._path_exists(path):
            value = os.stat(path)
            location: tuple[str, int, int] | tuple[str, int, int, str] = (
                "existing",
                value.st_dev,
                value.st_ino,
            )
        else:
            parent = safety._require_directory(
                path.parent,
                label="restore path parent",
            )
            value = parent.stat()
            location = ("new", value.st_dev, value.st_ino, path.name)
        if location in locations:
            raise safety.CandidateInstallError(
                "restore paths must be distinct by filesystem identity"
            )
        locations.add(location)


def _validate_v16(path: Path) -> dict[str, Any]:
    connection = safety._connect_immutable_database(
        path,
        label="current formal v16 database",
    )
    try:
        quick = [str(row[0]) for row in connection.execute("PRAGMA quick_check")]
        integrity = [
            str(row[0]) for row in connection.execute("PRAGMA integrity_check")
        ]
        foreign_keys = list(connection.execute("PRAGMA foreign_key_check"))
        version = safety.require_schema_compatibility(
            connection,
            supported_versions=frozenset({safety.EXPECTED_CANDIDATE_SCHEMA_VERSION}),
        )
        migrations = safety._schema_migration_rows(connection)
        if (
            quick != ["ok"]
            or integrity != ["ok"]
            or foreign_keys
            or version != safety.EXPECTED_CANDIDATE_SCHEMA_VERSION
            or not migrations
            or migrations[-1][:2] != (
                safety.EXPECTED_CANDIDATE_SCHEMA_VERSION,
                safety.EXPECTED_CANDIDATE_MIGRATION,
            )
        ):
            raise safety.CandidateInstallError(
                "current formal database is not an exact clean v16 database"
            )
        return {
            "quick_check": "ok",
            "integrity_check": "ok",
            "foreign_key_violation_count": 0,
            "schema_version": version,
            "schema_migration": safety.EXPECTED_CANDIDATE_MIGRATION,
        }
    except (sqlite3.Error, RuntimeError) as error:
        if isinstance(error, safety.CandidateInstallError):
            raise
        raise safety.CandidateInstallError(
            f"current formal v16 validation failed: {error}"
        ) from error
    finally:
        connection.close()


def _load_backup_contract(
    *,
    receipt_path: Path,
    expected_receipt_sha256: str,
    formal_database: Path,
) -> tuple[dict[str, Any], Path, Path]:
    safety._require_sha256(
        expected_receipt_sha256,
        label="expected backup receipt SHA-256",
    )
    safety._require_regular_single_link(receipt_path, label="backup receipt")
    safety._require_project_external(receipt_path, label="backup receipt")
    if safety._sha256_file(receipt_path) != expected_receipt_sha256:
        raise safety.CandidateInstallError("backup receipt SHA-256 differs")
    value = safety._read_json_object(
        receipt_path,
        label="backup receipt",
        maximum_bytes=safety.MAX_BACKUP_RECEIPT_BYTES,
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
    safety._require_exact_keys(value, fields, label="backup receipt")
    if value["schema_version"] != safety.BACKUP_RECEIPT_SCHEMA:
        raise safety.CandidateInstallError("backup receipt schema is unsupported")
    safety._require_same_resolved_path(
        value["source_path"],
        formal_database,
        label="backup receipt original formal path",
    )
    original_source_sha = value["source_sha256"]
    if not isinstance(original_source_sha, str):
        raise safety.CandidateInstallError("backup receipt source SHA-256 is invalid")
    safety._require_sha256(
        original_source_sha,
        label="backup receipt source SHA-256",
    )
    expected_scalars = {
        "source_schema_version": safety.EXPECTED_SOURCE_SCHEMA_VERSION,
        "source_schema_migration": safety.EXPECTED_SOURCE_MIGRATION,
        "restore_verified": True,
        "quick_check": "ok",
        "integrity_check": "ok",
        "foreign_key_violation_count": 0,
    }
    for key, expected in expected_scalars.items():
        if value[key] != expected:
            raise safety.CandidateInstallError(
                f"backup receipt {key} is not the exact restore contract"
            )

    raw_backup = value["backup_path"]
    raw_lock = value["migration_lock"]
    if not isinstance(raw_backup, str) or not isinstance(raw_lock, str):
        raise safety.CandidateInstallError("backup receipt paths are invalid")
    backup = Path(raw_backup)
    migration_lock = Path(raw_lock)
    if not backup.is_absolute() or not migration_lock.is_absolute():
        raise safety.CandidateInstallError("backup receipt paths must be absolute")
    backup_identity = safety._require_regular_single_link(
        backup,
        label="verified v15 backup",
    )
    safety._require_project_external(backup, label="verified v15 backup")
    safety._require_project_external(migration_lock, label="migration lock")
    if backup_identity.mode != 0o600:
        raise safety.CandidateInstallError("verified v15 backup mode must be 0600")
    if (
        value["backup_byte_size"] != backup_identity.size
        or value["backup_sha256"] != safety._sha256_file(backup)
    ):
        raise safety.CandidateInstallError("verified v15 backup identity differs")
    safety._require_fingerprint(
        value["migration_lock_file"],
        migration_lock,
        label="backup receipt migration lock file",
    )
    backup_validation = safety._validate_source_database(backup)
    if backup_validation["schema_version"] != safety.EXPECTED_SOURCE_SCHEMA_VERSION:
        raise safety.CandidateInstallError("verified backup is not exact v15")
    return value, backup.resolve(strict=True), migration_lock.resolve(strict=True)


def _copy_database(source: Path, destination_path: Path) -> None:
    source_connection = safety._connect_immutable_database(
        source,
        label="verified v15 backup",
    )
    destination = sqlite3.connect(destination_path, timeout=10)
    try:
        source_connection.backup(destination)
        destination.commit()
    except sqlite3.Error as error:
        destination.rollback()
        raise safety.CandidateInstallError(
            f"cannot materialize restore candidate: {error}"
        ) from error
    finally:
        destination.close()
        source_connection.close()


def _cleanup_staging(path: Path | None, errors: list[str]) -> None:
    if path is None:
        return
    for candidate in (
        path,
        *(Path(f"{path}{suffix}") for suffix in safety.SQLITE_TRANSIENT_SUFFIXES),
    ):
        if not safety._path_exists(candidate):
            continue
        try:
            value = candidate.lstat()
            if stat.S_ISLNK(value.st_mode) or not stat.S_ISREG(value.st_mode):
                errors.append(f"unsafe restore staging path remained: {candidate}")
                continue
            candidate.unlink()
        except OSError as error:
            errors.append(f"restore staging cleanup failed: {error}")


def _assert_restore_inputs_unchanged(
    *,
    formal_database: Path,
    formal_fingerprint: Mapping[str, Any],
    formal_validation: Mapping[str, Any],
    source_sidecars: Mapping[str, Mapping[str, Any]],
    freeze_lock: Path,
    freeze_identity: safety.FileIdentity,
    backup: Path,
    backup_identity: safety.FileIdentity,
    backup_sha256: str,
    backup_receipt: Path,
    backup_receipt_identity: safety.FileIdentity,
    backup_receipt_sha256: str,
) -> None:
    if (
        safety._fingerprint(formal_database) != formal_fingerprint
        or _validate_v16(formal_database) != formal_validation
        or safety._stat_identity(freeze_lock) != freeze_identity
        or safety._stat_identity(backup) != backup_identity
        or safety._sha256_file(backup) != backup_sha256
        or safety._stat_identity(backup_receipt) != backup_receipt_identity
        or safety._sha256_file(backup_receipt) != backup_receipt_sha256
    ):
        raise safety.CandidateInstallError("restore inputs changed before cutover")
    for suffix in safety.SQLITE_TRANSIENT_SUFFIXES:
        path = Path(f"{formal_database}{suffix}")
        expected = source_sidecars.get(suffix)
        if expected is None:
            if safety._path_exists(path):
                raise safety.CandidateInstallError(
                    f"formal v16 {suffix} appeared before cutover"
                )
        elif not safety._path_exists(path) or safety._fingerprint(path) != expected:
            raise safety.CandidateInstallError(
                f"formal v16 {suffix} changed before cutover"
            )


def _assert_archived_v16_exact(
    *,
    archived_v16: Path,
    formal_fingerprint: Mapping[str, Any],
    formal_validation: Mapping[str, Any],
    rollback_directory: Path,
    formal_name: str,
    source_sidecars: Mapping[str, Mapping[str, Any]],
    moved_sidecars: set[str],
) -> None:
    if (
        safety._fingerprint(archived_v16) != formal_fingerprint
        or _validate_v16(archived_v16) != formal_validation
    ):
        raise safety.CandidateInstallError(
            "archived v16 database differs from the preflight identity"
        )
    for suffix, expected in source_sidecars.items():
        if suffix not in moved_sidecars:
            continue
        archived = rollback_directory / f"{formal_name}{suffix}"
        if (
            not safety._path_exists(archived)
            or safety._fingerprint(archived) != expected
        ):
            raise safety.CandidateInstallError(
                f"archived v16 {suffix} differs from the preflight identity"
            )


def restore_verified_backup(
    *,
    formal_database: Path,
    expected_formal_v16_sha256: str,
    backup_receipt: Path,
    expected_backup_receipt_sha256: str,
    rollback_directory: Path,
    receipt: Path,
    freeze_lock: Path,
    holder_checker: Callable[[Sequence[Path]], list[dict[str, Any]]] | None = None,
    fault_injector: Callable[[str], None] | None = None,
) -> dict[str, Any]:
    """Restore v15, or put the byte-identical original v16 back before raising."""

    formal_database = formal_database.absolute()
    backup_receipt = backup_receipt.absolute()
    rollback_directory = rollback_directory.absolute()
    receipt = receipt.absolute()
    freeze_lock = freeze_lock.absolute()
    expected_formal_v16_sha256 = safety._require_sha256(
        expected_formal_v16_sha256,
        label="expected formal v16 SHA-256",
    )
    expected_backup_receipt_sha256 = safety._require_sha256(
        expected_backup_receipt_sha256,
        label="expected backup receipt SHA-256",
    )
    if (
        os.environ.get("DCAR_TEST_DENY_FORMAL_DB") == "1"
        and safety.is_formal_database_path(
            formal_database,
            formal_database=safety.DEFAULT_DB,
        )
    ):
        raise safety.CandidateInstallError(
            "test process attempted to open the formal DCar database"
        )
    if not safety.is_formal_database_path(
        formal_database,
        formal_database=FORMAL_DATABASE,
    ):
        raise safety.CandidateInstallError(
            f"formal restore target must be exactly {FORMAL_DATABASE}"
        )
    formal_identity = safety._require_regular_single_link(
        formal_database,
        label="formal v16 database",
    )
    formal_fingerprint = safety._fingerprint(formal_database)
    if formal_fingerprint["sha256"] != expected_formal_v16_sha256:
        raise safety.CandidateInstallError("formal v16 SHA-256 differs")
    formal_validation = _validate_v16(formal_database)

    freeze_identity = safety._require_regular_single_link(
        freeze_lock,
        label="operator freeze lock",
    )
    if (
        not _same_existing_file(freeze_lock, CANONICAL_OPERATOR_FREEZE_LOCK)
        or freeze_identity.mode != 0o600
    ):
        raise safety.CandidateInstallError(
            "restore requires the canonical 0600 operator freeze lock"
        )

    backup_value, backup, migration_lock = _load_backup_contract(
        receipt_path=backup_receipt,
        expected_receipt_sha256=expected_backup_receipt_sha256,
        formal_database=formal_database,
    )
    backup_identity = safety._stat_identity(backup)
    backup_sha256 = safety._sha256_file(backup)
    backup_receipt_identity = safety._stat_identity(backup_receipt)
    backup_receipt_sha256 = safety._sha256_file(backup_receipt)

    rollback_root = safety._require_directory(
        FORMAL_BACKUP_ROOT,
        label="formal rollback root",
    )
    if safety._path_exists(rollback_directory):
        raise safety.CandidateInstallError("rollback directory must be new")
    if rollback_directory.parent.resolve(strict=True) != rollback_root:
        raise safety.CandidateInstallError(
            "rollback directory must be a direct child of the formal backup root"
        )
    receipt_parent = safety._require_directory(
        receipt.parent,
        label="restore receipt parent",
    )
    receipt = receipt_parent / receipt.name
    safety._require_project_external(receipt, label="restore receipt")
    if safety._path_exists(receipt):
        raise safety.CandidateInstallError("restore receipt path must be new")

    source_sidecars: dict[str, dict[str, Any]] = {}
    for suffix in safety.SQLITE_TRANSIENT_SUFFIXES:
        sidecar = Path(f"{formal_database}{suffix}")
        if safety._path_exists(sidecar):
            source_sidecars[suffix] = safety._fingerprint(sidecar)
    if source_sidecars.get("-wal", {}).get("size", 0) != 0:
        raise safety.CandidateInstallError("formal v16 WAL must be absent or empty")
    if "-journal" in source_sidecars:
        raise safety.CandidateInstallError(
            "formal v16 rollback journal must be absent"
        )
    _require_distinct_paths(
        (
            formal_database,
            freeze_lock,
            backup_receipt,
            backup,
            migration_lock,
            receipt,
            *(Path(f"{formal_database}{suffix}") for suffix in source_sidecars),
        )
    )

    checker = holder_checker or safety._database_handles
    staging: Path | None = None
    receipt_identity: safety.FileIdentity | None = None
    source_moved = False
    restored_installed = False
    moved_sidecars: set[str] = set()
    archived_v16 = rollback_directory / formal_database.name
    rollback_errors: list[str] = []

    with safety._exclusive_existing_migration_lock(migration_lock) as lock_lease:
        try:
            safety._require_fingerprint(
                backup_value["migration_lock_file"],
                migration_lock,
                label="held migration lock",
            )
            if checker((formal_database, backup)):
                raise safety.CandidateInstallError(
                    "database handles are open before restore"
                )
            if (
                safety._stat_identity(formal_database) != formal_identity
                or safety._sha256_file(formal_database)
                != expected_formal_v16_sha256
                or safety._stat_identity(backup) != backup_identity
                or safety._sha256_file(backup) != backup_sha256
                or safety._stat_identity(backup_receipt)
                != backup_receipt_identity
                or safety._sha256_file(backup_receipt)
                != backup_receipt_sha256
                or safety._stat_identity(freeze_lock) != freeze_identity
            ):
                raise safety.CandidateInstallError(
                    "restore inputs changed before materialization"
                )

            descriptor, raw_staging = tempfile.mkstemp(
                prefix=f".{formal_database.name}.restore-v15-",
                dir=formal_database.parent,
            )
            os.fchmod(descriptor, 0o600)
            os.close(descriptor)
            staging = Path(raw_staging)
            _copy_database(backup, staging)
            safety._validate_source_database(staging)
            safety._validate_backup_lineage(backup, staging)
            for suffix in safety.SQLITE_TRANSIENT_SUFFIXES:
                if safety._path_exists(Path(f"{staging}{suffix}")):
                    raise safety.CandidateInstallError(
                        f"restore staging retained SQLite sidecar: {suffix}"
                    )
            _checkpoint("after_preflight", fault_injector)

            _assert_restore_inputs_unchanged(
                formal_database=formal_database,
                formal_fingerprint=formal_fingerprint,
                formal_validation=formal_validation,
                source_sidecars=source_sidecars,
                freeze_lock=freeze_lock,
                freeze_identity=freeze_identity,
                backup=backup,
                backup_identity=backup_identity,
                backup_sha256=backup_sha256,
                backup_receipt=backup_receipt,
                backup_receipt_identity=backup_receipt_identity,
                backup_receipt_sha256=backup_receipt_sha256,
            )
            if checker((formal_database, backup)):
                raise safety.CandidateInstallError(
                    "database handles appeared before restore cutover"
                )
            rollback_directory.mkdir(mode=0o700)
            safety._fsync_directory(rollback_root)
            _checkpoint("after_rollback_directory_created", fault_injector)

            os.replace(formal_database, archived_v16)
            source_moved = True
            _assert_archived_v16_exact(
                archived_v16=archived_v16,
                formal_fingerprint=formal_fingerprint,
                formal_validation=formal_validation,
                rollback_directory=rollback_directory,
                formal_name=formal_database.name,
                source_sidecars=source_sidecars,
                moved_sidecars=moved_sidecars,
            )
            _checkpoint("after_v16_database_moved", fault_injector)
            for suffix, checkpoint in (
                ("-wal", "after_v16_wal_moved"),
                ("-shm", "after_v16_shm_moved"),
            ):
                sidecar = Path(f"{formal_database}{suffix}")
                if safety._path_exists(sidecar):
                    os.replace(
                        sidecar,
                        rollback_directory / f"{formal_database.name}{suffix}",
                    )
                    moved_sidecars.add(suffix)
                _checkpoint(checkpoint, fault_injector)

            _assert_archived_v16_exact(
                archived_v16=archived_v16,
                formal_fingerprint=formal_fingerprint,
                formal_validation=formal_validation,
                rollback_directory=rollback_directory,
                formal_name=formal_database.name,
                source_sidecars=source_sidecars,
                moved_sidecars=moved_sidecars,
            )

            os.replace(staging, formal_database)
            staging = None
            restored_installed = True
            _checkpoint("after_v15_installed", fault_injector)
            os.chmod(formal_database, 0o600)
            safety._fsync_file(formal_database)
            _checkpoint("after_installed_file_synced", fault_injector)
            safety._fsync_directory(formal_database.parent)
            safety._fsync_directory(rollback_directory)
            _checkpoint("after_directories_synced", fault_injector)

            restored_validation = safety._validate_source_database(formal_database)
            restored_lineage = safety._validate_backup_lineage(
                backup,
                formal_database,
            )
            for suffix in safety.SQLITE_TRANSIENT_SUFFIXES:
                if safety._path_exists(Path(f"{formal_database}{suffix}")):
                    raise safety.CandidateInstallError(
                        f"restored v15 retained unexpected SQLite sidecar: {suffix}"
                    )
            if checker((formal_database, backup)):
                raise safety.CandidateInstallError(
                    "database handles appeared after restore cutover"
                )
            _assert_archived_v16_exact(
                archived_v16=archived_v16,
                formal_fingerprint=formal_fingerprint,
                formal_validation=formal_validation,
                rollback_directory=rollback_directory,
                formal_name=formal_database.name,
                source_sidecars=source_sidecars,
                moved_sidecars=moved_sidecars,
            )
            safety._require_fingerprint(
                backup_value["migration_lock_file"],
                migration_lock,
                label="held migration lock before restore receipt",
            )
            _checkpoint("after_post_restore_verification", fault_injector)

            result: dict[str, Any] = {
                "schema_version": RESTORE_RECEIPT_SCHEMA,
                "status": "restored_v15",
                "completed_at": safety._utc_now(),
                "before": {
                    "database": formal_fingerprint,
                    "validation": formal_validation,
                    "sidecars": source_sidecars,
                },
                "backup_receipt": {
                    "path": str(backup_receipt),
                    "sha256": backup_receipt_sha256,
                    "schema_version": backup_value["schema_version"],
                    "original_source_sha256": backup_value["source_sha256"],
                },
                "verified_backup": {
                    "path": str(backup),
                    "file": safety._fingerprint(backup),
                    "validation": safety._validate_source_database(backup),
                },
                "installed": {
                    "path": str(formal_database),
                    "file": safety._fingerprint(formal_database),
                    "validation": restored_validation,
                    "lineage": restored_lineage,
                },
                "archived_v16": {
                    "path": str(archived_v16),
                    "file": safety._fingerprint(archived_v16),
                    "sidecars": {
                        suffix: safety._fingerprint(
                            rollback_directory / f"{formal_database.name}{suffix}"
                        )
                        for suffix in sorted(moved_sidecars)
                    },
                },
                "operator_freeze_lock": str(freeze_lock),
                "migration_lock": {
                    "path": str(migration_lock),
                    "file": {
                        **asdict(lock_lease.identity),
                        "sha256": safety._sha256_file(migration_lock),
                    },
                },
                "database_handles": [],
                "rollback": "not_required",
                "receipt": str(receipt),
            }

            def receipt_created(identity: safety.FileIdentity) -> None:
                nonlocal receipt_identity
                receipt_identity = identity

            safety._write_json_exclusive(
                receipt,
                result,
                on_created=receipt_created,
            )
            _checkpoint("after_restore_receipt_written", fault_injector)
            _assert_archived_v16_exact(
                archived_v16=archived_v16,
                formal_fingerprint=formal_fingerprint,
                formal_validation=formal_validation,
                rollback_directory=rollback_directory,
                formal_name=formal_database.name,
                source_sidecars=source_sidecars,
                moved_sidecars=moved_sidecars,
            )
            safety._require_fingerprint(
                backup_value["migration_lock_file"],
                migration_lock,
                label="held migration lock at restore completion",
            )
            if safety._stat_identity(freeze_lock) != freeze_identity:
                raise safety.CandidateInstallError(
                    "operator freeze lock changed during restore"
                )
            lock_lease.verify_for_commit()
            return result
        except BaseException as error:
            try:
                if receipt_identity is not None and safety._path_exists(receipt):
                    if safety._same_identity(
                        safety._stat_identity(receipt),
                        receipt_identity,
                    ):
                        receipt.unlink()
                        safety._fsync_directory(receipt_parent)
                    else:
                        rollback_errors.append(
                            "restore receipt path changed before rollback"
                        )
            except Exception as rollback_error:
                rollback_errors.append(f"restore receipt cleanup: {rollback_error}")

            if source_moved:
                try:
                    if checker((formal_database, archived_v16)):
                        raise safety.CandidateInstallError(
                            "database handle blocked restore rollback"
                        )
                    if safety._path_exists(formal_database):
                        quarantined = (
                            rollback_directory
                            / f"FAILED-restored-v15-{formal_database.name}"
                        )
                        os.replace(formal_database, quarantined)
                    if not safety._path_exists(archived_v16):
                        raise safety.CandidateInstallError(
                            "archived original v16 database is missing"
                        )
                    os.replace(archived_v16, formal_database)
                    for suffix in ("-wal", "-shm"):
                        live = Path(f"{formal_database}{suffix}")
                        archived = (
                            rollback_directory / f"{formal_database.name}{suffix}"
                        )
                        if suffix in moved_sidecars:
                            if safety._path_exists(live):
                                os.replace(
                                    live,
                                    rollback_directory
                                    / (
                                        "FAILED-restored-v15-"
                                        f"{formal_database.name}{suffix}"
                                    ),
                                )
                            if not safety._path_exists(archived):
                                raise safety.CandidateInstallError(
                                    f"archived original v16 {suffix} is missing"
                                )
                            os.replace(archived, live)
                        elif suffix not in source_sidecars and safety._path_exists(live):
                            os.replace(
                                live,
                                rollback_directory
                                / (
                                    "FAILED-restored-v15-"
                                    f"{formal_database.name}{suffix}"
                                ),
                            )
                    live_journal = Path(f"{formal_database}-journal")
                    if safety._path_exists(live_journal):
                        os.replace(
                            live_journal,
                            rollback_directory
                            / f"FAILED-restored-v15-{formal_database.name}-journal",
                        )
                    safety._fsync_file(formal_database)
                    safety._fsync_directory(formal_database.parent)
                    safety._fsync_directory(rollback_directory)
                    if (
                        safety._sha256_file(formal_database)
                        != expected_formal_v16_sha256
                        or _validate_v16(formal_database) != formal_validation
                    ):
                        raise safety.CandidateInstallError(
                            "restored original v16 database validation differs"
                        )
                    for suffix in safety.SQLITE_TRANSIENT_SUFFIXES:
                        live = Path(f"{formal_database}{suffix}")
                        original = source_sidecars.get(suffix)
                        if original is None:
                            if safety._path_exists(live):
                                raise safety.CandidateInstallError(
                                    f"restored original v16 retained unexpected {suffix}"
                                )
                        elif not safety._path_exists(live) or (
                            safety._fingerprint(live) != original
                        ):
                            raise safety.CandidateInstallError(
                                f"restored original v16 {suffix} differs"
                            )
                except Exception as rollback_error:
                    rollback_errors.append(f"formal v16 rollback: {rollback_error}")
            elif restored_installed:
                rollback_errors.append(
                    "restore state is inconsistent: v15 installed before source archive"
                )

            _cleanup_staging(staging, rollback_errors)
            try:
                if (
                    safety._stat_identity(backup) != backup_identity
                    or safety._sha256_file(backup) != backup_sha256
                    or safety._stat_identity(backup_receipt)
                    != backup_receipt_identity
                    or safety._sha256_file(backup_receipt)
                    != backup_receipt_sha256
                ):
                    rollback_errors.append("verified backup inputs changed")
            except Exception as rollback_error:
                rollback_errors.append(f"backup revalidation: {rollback_error}")

            if rollback_errors:
                raise safety.CandidateInstallError(
                    "v15 restore failed and original v16 rollback was incomplete: "
                    f"original={type(error).__name__}: {error}; "
                    f"rollback={json.dumps(rollback_errors, ensure_ascii=False)}"
                ) from error
            raise safety.CandidateInstallError(
                "v15 restore failed and original v16 was restored: "
                f"{type(error).__name__}: {error}"
            ) from error


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--formal-db", type=Path, required=True)
    parser.add_argument("--expected-formal-v16-sha256", required=True)
    parser.add_argument("--backup-receipt", type=Path, required=True)
    parser.add_argument("--expected-backup-receipt-sha256", required=True)
    parser.add_argument("--rollback-dir", type=Path, required=True)
    parser.add_argument("--receipt", type=Path, required=True)
    parser.add_argument("--freeze-lock", type=Path, required=True)
    return parser


def main(argv: Sequence[str] | None = None) -> int:
    arguments = build_parser().parse_args(argv)
    try:
        result = restore_verified_backup(
            formal_database=arguments.formal_db,
            expected_formal_v16_sha256=arguments.expected_formal_v16_sha256,
            backup_receipt=arguments.backup_receipt,
            expected_backup_receipt_sha256=(
                arguments.expected_backup_receipt_sha256
            ),
            rollback_directory=arguments.rollback_dir,
            receipt=arguments.receipt,
            freeze_lock=arguments.freeze_lock,
        )
    except safety.CandidateInstallError as error:
        print(f"writer v15 restore refused: {error}", file=sys.stderr)
        return 1
    print(json.dumps(result, ensure_ascii=False, sort_keys=True))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
