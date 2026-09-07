#!/usr/bin/env python3
"""Create and verify fail-closed SQLite backups of the Dcar auth store.

Every usable backup is a database/manifest pair. Dedupe, retention and restore
verification all use the same strict pair validator; an orphan or a corrupt
manifest can therefore never shadow the last recoverable database.
"""

from __future__ import annotations

import argparse
import calendar
import hashlib
import json
import os
import re
import secrets
import shutil
import sqlite3
import stat
import sys
import time
from pathlib import Path
from typing import Any, NamedTuple
from urllib.parse import quote


TABLES_BY_VERSION: dict[int, frozenset[str]] = {
    0: frozenset({"auth_sessions"}),
    1: frozenset(
        {
            "auth_sessions",
            "auth_users",
            "auth_allowed_phones",
            "auth_challenges",
            "auth_failures",
        }
    ),
    2: frozenset(
        {
            "auth_sessions",
            "auth_users",
            "auth_allowed_phones",
            "auth_challenges",
            "auth_failures",
            "auth_deleted_users",
        }
    ),
    3: frozenset(
        {
            "auth_sessions",
            "auth_users",
            "auth_allowed_phones",
            "auth_challenges",
            "auth_failures",
            "auth_deleted_users",
        }
    ),
}
BACKUP_PREFIX = "dcar-auth-"
MANIFEST_SCHEMA = "dcar-auth-backup-v1"
BACKUP_NAME = re.compile(
    r"^dcar-auth-(?P<stamp>\d{8}T\d{6}Z)-(?P<sha>[0-9a-f]{12})\.sqlite3$"
)
SHA256_RE = re.compile(r"[0-9a-f]{64}")
MANIFEST_KEYS = frozenset(
    {"schema", "created_at", "filename", "sha256", "size_bytes", "counts"}
)
COUNT_KEYS = frozenset({"user_version", "users", "sessions", "allowed_phones"})


class VerifiedPair(NamedTuple):
    database: Path
    manifest: Path
    stamp: str
    sha256: str
    counts: dict[str, int]


def _read_only_uri(path: Path) -> str:
    return f"file:{quote(str(path.resolve()), safe='/')}?mode=ro"


def _assert_regular_file(path: Path, label: str) -> os.stat_result:
    try:
        info = path.lstat()
    except FileNotFoundError as exc:
        raise RuntimeError(f"{label} does not exist") from exc
    if path.is_symlink() or not stat.S_ISREG(info.st_mode) or info.st_nlink != 1:
        raise RuntimeError(f"{label} is not a safe regular file")
    return info


def _sha256(path: Path) -> str:
    digest = hashlib.sha256()
    descriptor = os.open(path, os.O_RDONLY | getattr(os, "O_NOFOLLOW", 0))
    try:
        for chunk in iter(lambda: os.read(descriptor, 1024 * 1024), b""):
            digest.update(chunk)
    finally:
        os.close(descriptor)
    return digest.hexdigest()


def _count(connection: sqlite3.Connection, table: str, tables: set[str]) -> int:
    if table not in tables:
        return 0
    return int(connection.execute(f"SELECT COUNT(*) FROM {table}").fetchone()[0])


def database_version(path: Path) -> int:
    _assert_regular_file(path, "auth database")
    connection = sqlite3.connect(
        _read_only_uri(path), uri=True, timeout=10.0, isolation_level=None
    )
    try:
        return int(connection.execute("PRAGMA user_version").fetchone()[0])
    finally:
        connection.close()


def validate_database(path: Path, expected_version: int) -> dict[str, int]:
    if expected_version not in TABLES_BY_VERSION:
        raise RuntimeError(f"unsupported expected user_version {expected_version}")
    _assert_regular_file(path, "auth database")
    if Path(f"{path}-wal").exists() or Path(f"{path}-shm").exists():
        raise RuntimeError("auth database has WAL sidecars")
    connection = sqlite3.connect(
        _read_only_uri(path), uri=True, timeout=10.0, isolation_level=None
    )
    try:
        connection.execute("PRAGMA busy_timeout=10000")
        journal_mode = str(
            connection.execute("PRAGMA journal_mode").fetchone()[0]
        ).lower()
        if journal_mode != "delete":
            raise RuntimeError("auth backup journal mode is not DELETE")
        if str(connection.execute("PRAGMA quick_check").fetchone()[0]) != "ok":
            raise RuntimeError("auth backup quick_check failed")
        if connection.execute("PRAGMA foreign_key_check").fetchone() is not None:
            raise RuntimeError("auth backup foreign_key_check failed")
        version = int(connection.execute("PRAGMA user_version").fetchone()[0])
        if version != expected_version:
            raise RuntimeError(
                f"auth backup schema version is {version}, expected {expected_version}"
            )
        tables = {
            str(row[0])
            for row in connection.execute(
                "SELECT name FROM sqlite_master WHERE type='table'"
            )
        }
        missing = TABLES_BY_VERSION[expected_version] - tables
        if missing:
            raise RuntimeError(
                "auth backup is missing required tables: " + ", ".join(sorted(missing))
            )
        return {
            "user_version": version,
            "users": _count(connection, "auth_users", tables),
            "sessions": _count(connection, "auth_sessions", tables),
            "allowed_phones": _count(connection, "auth_allowed_phones", tables),
        }
    finally:
        connection.close()


def _read_json_file(path: Path, label: str) -> Any:
    _assert_regular_file(path, label)
    descriptor = os.open(path, os.O_RDONLY | getattr(os, "O_NOFOLLOW", 0))
    try:
        with os.fdopen(descriptor, "r", encoding="utf-8") as handle:
            descriptor = -1
            return json.load(handle)
    except (UnicodeDecodeError, json.JSONDecodeError) as exc:
        raise RuntimeError(f"{label} is not valid JSON") from exc
    finally:
        if descriptor >= 0:
            os.close(descriptor)


def verify_backup_pair(database: Path, expected_version: int) -> VerifiedPair:
    """Validate filename, manifest, bytes and SQLite contents as one contract."""

    match = BACKUP_NAME.fullmatch(database.name)
    if match is None:
        raise RuntimeError("auth backup filename is invalid")
    try:
        time.strptime(match.group("stamp"), "%Y%m%dT%H%M%SZ")
    except ValueError as exc:
        raise RuntimeError("auth backup filename timestamp is invalid") from exc
    database_info = _assert_regular_file(database, "auth backup database")
    manifest = database.with_suffix(".manifest.json")
    payload = _read_json_file(manifest, "auth backup manifest")
    if not isinstance(payload, dict) or set(payload) != MANIFEST_KEYS:
        raise RuntimeError("auth backup manifest has an invalid shape")
    if payload.get("schema") != MANIFEST_SCHEMA:
        raise RuntimeError("auth backup manifest schema is invalid")
    if payload.get("created_at") != match.group("stamp"):
        raise RuntimeError("auth backup manifest timestamp does not match filename")
    if payload.get("filename") != database.name:
        raise RuntimeError("auth backup manifest filename does not match database")
    manifest_sha = payload.get("sha256")
    if not isinstance(manifest_sha, str) or SHA256_RE.fullmatch(manifest_sha) is None:
        raise RuntimeError("auth backup manifest sha256 is invalid")
    if match.group("sha") != manifest_sha[:12]:
        raise RuntimeError("auth backup filename sha256 prefix does not match manifest")
    if payload.get("size_bytes") != database_info.st_size:
        raise RuntimeError("auth backup manifest size does not match database")
    actual_sha = _sha256(database)
    if actual_sha != manifest_sha:
        raise RuntimeError("auth backup sha256 does not match manifest")
    counts = validate_database(database, expected_version)
    manifest_counts = payload.get("counts")
    if (
        not isinstance(manifest_counts, dict)
        or set(manifest_counts) != COUNT_KEYS
        or any(
            type(value) is not int or value < 0 for value in manifest_counts.values()
        )
        or manifest_counts != counts
    ):
        raise RuntimeError("auth backup manifest counts do not match database")
    return VerifiedPair(
        database=database,
        manifest=manifest,
        stamp=match.group("stamp"),
        sha256=actual_sha,
        counts=counts,
    )


def _valid_backup_pairs(backup_dir: Path, expected_version: int) -> list[VerifiedPair]:
    pairs: list[VerifiedPair] = []
    for candidate in backup_dir.iterdir():
        if BACKUP_NAME.fullmatch(candidate.name) is None:
            continue
        try:
            pairs.append(verify_backup_pair(candidate, expected_version))
        except (OSError, RuntimeError, sqlite3.Error):
            # Invalid material is retained for diagnosis but has no operational
            # effect on dedupe or retention.
            continue
    pairs.sort(key=lambda pair: (pair.stamp, pair.database.name))
    return pairs


def _latest_valid_pair(backup_dir: Path, expected_version: int) -> VerifiedPair | None:
    pairs = _valid_backup_pairs(backup_dir, expected_version)
    return pairs[-1] if pairs else None


def _quarantine_collision(path: Path, unique: str) -> None:
    if not path.exists() and not path.is_symlink():
        return
    quarantine = path.with_name(f".{path.name}.invalid-{unique}")
    if quarantine.exists() or quarantine.is_symlink():
        raise RuntimeError("auth backup collision quarantine already exists")
    os.replace(path, quarantine)


def _fsync_directory(path: Path) -> None:
    descriptor = os.open(path, os.O_RDONLY | getattr(os, "O_DIRECTORY", 0))
    try:
        os.fsync(descriptor)
    finally:
        os.close(descriptor)


def _write_all(descriptor: int, encoded: bytes) -> None:
    view = memoryview(encoded)
    while view:
        written = os.write(descriptor, view)
        if written <= 0:
            raise OSError("short write")
        view = view[written:]


def _write_manifest(path: Path, payload: dict[str, object]) -> None:
    descriptor = os.open(
        path,
        os.O_WRONLY | os.O_CREAT | os.O_EXCL | getattr(os, "O_NOFOLLOW", 0),
        0o600,
    )
    try:
        encoded = (
            json.dumps(
                payload, ensure_ascii=True, sort_keys=True, separators=(",", ":")
            )
            + "\n"
        ).encode("utf-8")
        _write_all(descriptor, encoded)
        os.fsync(descriptor)
    finally:
        os.close(descriptor)


def create_backup(
    source_path: Path, backup_dir: Path, expected_version: int
) -> dict[str, object]:
    _assert_regular_file(source_path, "auth store source")
    if not backup_dir.is_dir() or backup_dir.is_symlink():
        raise RuntimeError("auth backup directory is not a safe directory")
    if Path(f"{source_path}-wal").exists() or Path(f"{source_path}-shm").exists():
        raise RuntimeError("auth store source has WAL sidecars")

    stamp = time.strftime("%Y%m%dT%H%M%SZ", time.gmtime())
    unique = f"{os.getpid()}-{secrets.token_hex(6)}"
    partial_path = backup_dir / f".{BACKUP_PREFIX}{stamp}-{unique}.partial"
    descriptor = os.open(
        partial_path,
        os.O_RDWR | os.O_CREAT | os.O_EXCL | getattr(os, "O_NOFOLLOW", 0),
        0o600,
    )
    os.close(descriptor)
    final_path: Path | None = None
    manifest_path: Path | None = None
    manifest_partial: Path | None = None
    database_installed = False
    try:
        source = sqlite3.connect(
            _read_only_uri(source_path), uri=True, timeout=10.0, isolation_level=None
        )
        target: sqlite3.Connection | None = None
        try:
            source.execute("PRAGMA busy_timeout=10000")
            source_mode = str(
                source.execute("PRAGMA journal_mode").fetchone()[0]
            ).lower()
            if source_mode != "delete":
                raise RuntimeError("auth store source journal mode is not DELETE")
            target = sqlite3.connect(partial_path, timeout=10.0, isolation_level=None)
            target.execute("PRAGMA busy_timeout=10000")
            target_mode = str(
                target.execute("PRAGMA journal_mode=DELETE").fetchone()[0]
            ).lower()
            target.execute("PRAGMA synchronous=EXTRA")
            if target_mode != "delete":
                raise RuntimeError("auth backup target rejected DELETE mode")
            if int(target.execute("PRAGMA synchronous").fetchone()[0]) != 3:
                raise RuntimeError("auth backup target rejected synchronous EXTRA")
            source.backup(target, pages=4096, sleep=0.01)
            target.commit()
        finally:
            if target is not None:
                target.close()
            source.close()

        os.chmod(partial_path, 0o600)
        counts = validate_database(partial_path, expected_version)
        if Path(f"{partial_path}-wal").exists() or Path(f"{partial_path}-shm").exists():
            raise RuntimeError("auth backup left WAL sidecars")
        backup_sha256 = _sha256(partial_path)
        matching = _latest_valid_pair(backup_dir, expected_version)
        if matching is not None and matching.sha256 == backup_sha256:
            partial_path.unlink()
            return {
                "status": "unchanged",
                "path": str(matching.database),
                "manifest": str(matching.manifest),
                "sha256": backup_sha256,
                "counts": counts,
            }

        final_path = backup_dir / f"{BACKUP_PREFIX}{stamp}-{backup_sha256[:12]}.sqlite3"
        manifest_path = final_path.with_suffix(".manifest.json")
        if final_path.exists() or manifest_path.exists():
            # A valid matching pair returned above. Anything left at the exact
            # destination is unusable collision material, retained under an
            # explicit quarantine name instead of shadowing this backup.
            _quarantine_collision(manifest_path, unique)
            _quarantine_collision(final_path, unique)
            _fsync_directory(backup_dir)
        with partial_path.open("rb") as backup_file:
            os.fsync(backup_file.fileno())
        os.replace(partial_path, final_path)
        database_installed = True
        _fsync_directory(backup_dir)
        manifest_partial = backup_dir / f".{manifest_path.name}.{unique}.partial"
        _write_manifest(
            manifest_partial,
            {
                "schema": MANIFEST_SCHEMA,
                "created_at": stamp,
                "filename": final_path.name,
                "sha256": backup_sha256,
                "size_bytes": final_path.stat().st_size,
                "counts": counts,
            },
        )
        os.replace(manifest_partial, manifest_path)
        manifest_partial = None
        _fsync_directory(backup_dir)
        verify_backup_pair(final_path, expected_version)
        return {
            "status": "created",
            "path": str(final_path),
            "manifest": str(manifest_path),
            "sha256": backup_sha256,
            "counts": counts,
        }
    except Exception:
        if database_installed and final_path is not None and manifest_path is not None:
            if not manifest_path.exists():
                final_path.unlink(missing_ok=True)
                _fsync_directory(backup_dir)
        raise
    finally:
        partial_path.unlink(missing_ok=True)
        if manifest_partial is not None:
            manifest_partial.unlink(missing_ok=True)


def prune_backups(
    backup_dir: Path,
    keep_days: int,
    expected_version: int = 3,
    now: float | None = None,
) -> list[str]:
    """Delete old verified pairs, while retaining the newest verified pair."""

    if keep_days <= 0:
        return []
    current = time.time() if now is None else now
    pairs = _valid_backup_pairs(backup_dir, expected_version)
    if not pairs:
        return []
    removed: list[str] = []
    for pair in pairs[:-1]:
        created = calendar.timegm(time.strptime(pair.stamp, "%Y%m%dT%H%M%SZ"))
        if current - created > keep_days * 86400:
            # Removing the manifest first can only leave an ignored orphan DB on
            # interruption; it can never leave an apparently usable manifest.
            pair.manifest.unlink()
            pair.database.unlink()
            removed.append(pair.database.name)
    if removed:
        _fsync_directory(backup_dir)
    return removed


def restore_backup_pair(
    database: Path, destination: Path, safety_dir: Path, expected_version: int
) -> dict[str, object]:
    """Replace an OFFLINE store only after preserving its current bytes.

    The release helper owns the service stop and common snapshot lock. Safety
    material is deliberately retained even when the old store is corrupt.
    """
    pair = verify_backup_pair(database, expected_version)
    old_info = _assert_regular_file(destination, "current auth database")
    if database.resolve() == destination.resolve():
        raise RuntimeError("restore source must differ from destination")
    if not safety_dir.is_dir() or safety_dir.is_symlink():
        raise RuntimeError("restore safety directory is not safe")
    stamp = time.strftime("%Y%m%dT%H%M%SZ", time.gmtime())
    evidence_dir = safety_dir / f"before-restore-{stamp}-{secrets.token_hex(6)}"
    evidence_dir.mkdir(mode=0o700)
    preserved: list[dict[str, object]] = []
    for suffix in ("", "-wal", "-shm", "-journal"):
        current = Path(f"{destination}{suffix}")
        if not current.exists() and not current.is_symlink():
            continue
        _assert_regular_file(current, "current auth database material")
        evidence = evidence_dir / current.name
        with current.open("rb") as reader, evidence.open("xb") as writer:
            os.chmod(evidence, 0o600)
            shutil.copyfileobj(reader, writer)
            writer.flush()
            os.fsync(writer.fileno())
        preserved.append({"filename": evidence.name, "sha256": _sha256(evidence)})
    _write_manifest(evidence_dir / "preserved.json", {"files": preserved})
    _fsync_directory(evidence_dir)
    _fsync_directory(safety_dir)
    # Re-check the selected pair immediately before copying it.
    verify_backup_pair(database, expected_version)
    temporary = destination.with_name(
        f".{destination.name}.restore-{secrets.token_hex(6)}"
    )
    try:
        with database.open("rb") as reader, temporary.open("xb") as writer:
            os.chmod(temporary, 0o600)
            if os.geteuid() == 0:
                os.chown(temporary, old_info.st_uid, old_info.st_gid)
            shutil.copyfileobj(reader, writer)
            writer.flush()
            os.fsync(writer.fileno())
        if _sha256(temporary) != pair.sha256:
            raise RuntimeError("restore copy sha256 mismatch")
        validate_database(temporary, expected_version)
        for suffix in ("-wal", "-shm", "-journal"):
            Path(f"{destination}{suffix}").unlink(missing_ok=True)
        os.replace(temporary, destination)
        _fsync_directory(destination.parent)
    finally:
        temporary.unlink(missing_ok=True)
    return {
        "status": "restored_offline",
        "source": str(database),
        "source_sha256": pair.sha256,
        "user_version": expected_version,
        "safety_directory": str(evidence_dir),
    }


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--source", type=Path)
    parser.add_argument("--backup-dir", type=Path)
    parser.add_argument("--keep-days", type=int, default=30)
    parser.add_argument("--expect-user-version", type=int, default=3)
    parser.add_argument(
        "--verify", type=Path, help="strictly validate one backup DB/manifest pair"
    )
    arguments = parser.parse_args()
    try:
        if arguments.verify is not None:
            pair = verify_backup_pair(arguments.verify, arguments.expect_user_version)
            print(
                json.dumps(
                    {
                        "status": "valid",
                        "path": str(pair.database),
                        "manifest": str(pair.manifest),
                        "sha256": pair.sha256,
                        "counts": pair.counts,
                    },
                    ensure_ascii=True,
                    sort_keys=True,
                )
            )
            return 0
        if arguments.source is None or arguments.backup_dir is None:
            parser.error(
                "--source and --backup-dir are required unless --verify is used"
            )
        result = create_backup(
            arguments.source, arguments.backup_dir, arguments.expect_user_version
        )
        result["pruned"] = prune_backups(
            arguments.backup_dir,
            arguments.keep_days,
            arguments.expect_user_version,
        )
    except (OSError, RuntimeError, sqlite3.Error) as exc:
        print(f"auth backup failed: {exc}", file=sys.stderr)
        return 1
    print(json.dumps(result, ensure_ascii=True, sort_keys=True))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
