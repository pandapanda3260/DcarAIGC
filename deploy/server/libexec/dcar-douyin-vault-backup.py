#!/usr/bin/env python3
"""Create one fail-closed SQLite online backup of the Douyin token Vault."""

from __future__ import annotations

import argparse
import hashlib
import json
import os
import secrets
import sqlite3
import sys
import time
from pathlib import Path
from urllib.parse import quote


REQUIRED_TABLES = frozenset(
    {"oauth_states", "douyin_authorizations", "audit_events"}
)


def _read_only_uri(path: Path) -> str:
    return f"file:{quote(str(path.resolve()), safe='/')}?mode=ro"


def _sha256(path: Path) -> str:
    digest = hashlib.sha256()
    with path.open("rb") as source:
        for chunk in iter(lambda: source.read(1024 * 1024), b""):
            digest.update(chunk)
    return digest.hexdigest()


def _validate_database(path: Path) -> dict[str, int]:
    connection = sqlite3.connect(
        _read_only_uri(path), uri=True, timeout=10.0, isolation_level=None
    )
    try:
        connection.execute("PRAGMA busy_timeout=10000")
        journal_mode = str(
            connection.execute("PRAGMA journal_mode").fetchone()[0]
        ).lower()
        if journal_mode != "delete":
            raise RuntimeError("Vault backup journal mode is not DELETE")
        if str(connection.execute("PRAGMA quick_check").fetchone()[0]) != "ok":
            raise RuntimeError("Vault backup quick_check failed")
        if connection.execute("PRAGMA foreign_key_check").fetchone() is not None:
            raise RuntimeError("Vault backup foreign_key_check failed")
        if int(connection.execute("PRAGMA user_version").fetchone()[0]) != 1:
            raise RuntimeError("Vault backup schema version is not 1")
        tables = {
            str(row[0])
            for row in connection.execute(
                "SELECT name FROM sqlite_master WHERE type='table'"
            )
        }
        if not REQUIRED_TABLES.issubset(tables):
            raise RuntimeError("Vault backup is missing required tables")
        invalid_candidates = int(
            connection.execute(
                """
                SELECT COUNT(*) FROM oauth_states
                WHERE candidate_ciphertext IS NOT NULL
                  AND (typeof(candidate_ciphertext)!='blob'
                       OR length(candidate_ciphertext)=0)
                """
            ).fetchone()[0]
        )
        invalid_tokens = int(
            connection.execute(
                """
                SELECT COUNT(*) FROM douyin_authorizations
                WHERE (access_token_ciphertext IS NOT NULL AND
                       (typeof(access_token_ciphertext)!='blob'
                        OR length(access_token_ciphertext)=0))
                   OR (refresh_token_ciphertext IS NOT NULL AND
                       (typeof(refresh_token_ciphertext)!='blob'
                        OR length(refresh_token_ciphertext)=0))
                """
            ).fetchone()[0]
        )
        if invalid_candidates or invalid_tokens:
            raise RuntimeError("Vault backup contains invalid ciphertext records")
        return {
            "oauth_states": int(
                connection.execute("SELECT COUNT(*) FROM oauth_states").fetchone()[0]
            ),
            "douyin_authorizations": int(
                connection.execute(
                    "SELECT COUNT(*) FROM douyin_authorizations"
                ).fetchone()[0]
            ),
            "audit_events": int(
                connection.execute("SELECT COUNT(*) FROM audit_events").fetchone()[0]
            ),
            "ciphertext_records": int(
                connection.execute(
                    """
                    SELECT
                        (SELECT COUNT(*) FROM oauth_states
                         WHERE candidate_ciphertext IS NOT NULL) +
                        (SELECT COUNT(*) FROM douyin_authorizations
                         WHERE access_token_ciphertext IS NOT NULL) +
                        (SELECT COUNT(*) FROM douyin_authorizations
                         WHERE refresh_token_ciphertext IS NOT NULL)
                    """
                ).fetchone()[0]
            ),
        }
    finally:
        connection.close()


def _latest_sha256(backup_dir: Path) -> str | None:
    manifests = sorted(backup_dir.glob("douyin-vault-*.manifest.json"), reverse=True)
    for manifest_path in manifests:
        try:
            payload = json.loads(manifest_path.read_text(encoding="utf-8"))
        except (OSError, json.JSONDecodeError):
            continue
        value = payload.get("sha256") if isinstance(payload, dict) else None
        if isinstance(value, str) and len(value) == 64:
            return value
    return None


def _fsync_directory(path: Path) -> None:
    descriptor = os.open(path, os.O_RDONLY | getattr(os, "O_DIRECTORY", 0))
    try:
        os.fsync(descriptor)
    finally:
        os.close(descriptor)


def _write_manifest(path: Path, payload: dict[str, object]) -> None:
    descriptor = os.open(path, os.O_WRONLY | os.O_CREAT | os.O_EXCL, 0o600)
    try:
        encoded = (
            json.dumps(payload, ensure_ascii=True, sort_keys=True, separators=(",", ":"))
            + "\n"
        ).encode("utf-8")
        os.write(descriptor, encoded)
        os.fsync(descriptor)
    finally:
        os.close(descriptor)


def create_backup(source_path: Path, backup_dir: Path) -> dict[str, object]:
    if not source_path.is_file():
        raise RuntimeError("Douyin Vault source does not exist")
    if not backup_dir.is_dir():
        raise RuntimeError("Douyin Vault backup directory does not exist")
    if Path(f"{source_path}-wal").exists() or Path(f"{source_path}-shm").exists():
        raise RuntimeError("Douyin Vault source has WAL sidecars")

    stamp = time.strftime("%Y%m%dT%H%M%SZ", time.gmtime())
    unique = f"{os.getpid()}-{secrets.token_hex(6)}"
    partial_path = backup_dir / f".douyin-vault-{stamp}-{unique}.partial"
    descriptor = os.open(
        partial_path, os.O_RDWR | os.O_CREAT | os.O_EXCL, 0o600
    )
    os.close(descriptor)

    source = sqlite3.connect(
        _read_only_uri(source_path), uri=True, timeout=10.0, isolation_level=None
    )
    target: sqlite3.Connection | None = None
    try:
        source.execute("PRAGMA busy_timeout=10000")
        source_mode = str(source.execute("PRAGMA journal_mode").fetchone()[0]).lower()
        if source_mode != "delete":
            raise RuntimeError("Douyin Vault source journal mode is not DELETE")
        target = sqlite3.connect(partial_path, timeout=10.0, isolation_level=None)
        target.execute("PRAGMA busy_timeout=10000")
        target_mode = str(
            target.execute("PRAGMA journal_mode=DELETE").fetchone()[0]
        ).lower()
        target.execute("PRAGMA synchronous=EXTRA")
        if target_mode != "delete":
            raise RuntimeError("Douyin Vault backup target rejected DELETE mode")
        if int(target.execute("PRAGMA synchronous").fetchone()[0]) != 3:
            raise RuntimeError("Douyin Vault backup target rejected synchronous EXTRA")
        source.backup(target, pages=4096, sleep=0.01)
        target.commit()
    finally:
        if target is not None:
            target.close()
        source.close()

    os.chmod(partial_path, 0o600)
    counts = _validate_database(partial_path)
    if Path(f"{partial_path}-wal").exists() or Path(f"{partial_path}-shm").exists():
        raise RuntimeError("Douyin Vault backup left WAL sidecars")
    backup_sha256 = _sha256(partial_path)
    if _latest_sha256(backup_dir) == backup_sha256:
        partial_path.unlink()
        return {"status": "unchanged", "sha256": backup_sha256, "counts": counts}

    final_path = backup_dir / f"douyin-vault-{stamp}-{backup_sha256[:12]}.sqlite3"
    manifest_path = final_path.with_suffix(".manifest.json")
    if final_path.exists() or manifest_path.exists():
        raise RuntimeError("Douyin Vault backup destination already exists")
    with partial_path.open("rb") as backup_file:
        os.fsync(backup_file.fileno())
    os.replace(partial_path, final_path)
    _fsync_directory(backup_dir)
    manifest_partial = backup_dir / f".{manifest_path.name}.{unique}.partial"
    _write_manifest(
        manifest_partial,
        {
            "schema": "dcar-douyin-vault-backup-v1",
            "created_at": stamp,
            "filename": final_path.name,
            "sha256": backup_sha256,
            "size_bytes": final_path.stat().st_size,
            "counts": counts,
        },
    )
    os.replace(manifest_partial, manifest_path)
    _fsync_directory(backup_dir)
    return {
        "status": "created",
        "path": str(final_path),
        "manifest": str(manifest_path),
        "sha256": backup_sha256,
        "counts": counts,
    }


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--source", type=Path, required=True)
    parser.add_argument("--backup-dir", type=Path, required=True)
    arguments = parser.parse_args()
    try:
        result = create_backup(arguments.source, arguments.backup_dir)
    except (OSError, RuntimeError, sqlite3.Error) as exc:
        print(f"Douyin Vault backup failed: {exc}", file=sys.stderr)
        return 1
    print(json.dumps(result, ensure_ascii=True, sort_keys=True))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
