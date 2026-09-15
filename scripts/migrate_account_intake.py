#!/usr/bin/env python3
"""Create a backed-up schema22 candidate from an isolated schema21 database.

Never modifies the source, installs a runtime, schedules work, or contacts a
provider. All paths must be explicit and distinct. Repeating the same command
verifies the existing outputs and performs no writes.
"""
from __future__ import annotations

import argparse
import hashlib
import json
import os
from pathlib import Path
import sqlite3
import stat
import sys
import tempfile
from typing import Any

PROJECT_ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(PROJECT_ROOT / "src" / "dcar_eval"))

from v8.schema_v21 import validate_structure as validate_source
from v8.schema_v22 import migrate, migration_proof, validate_lineage
from v8.storage import configure_connection_safety, is_formal_database_path
from v8.runtime_database import resolve_isolated_candidate


def _sha(path: Path) -> str:
    with path.open("rb") as stream:
        return hashlib.file_digest(stream, "sha256").hexdigest()


def _connection(path: Path, *, readonly: bool = False) -> sqlite3.Connection:
    connection = sqlite3.connect(f"{path.as_uri()}?mode={'ro' if readonly else 'rw'}", uri=True)
    connection.row_factory = sqlite3.Row
    configure_connection_safety(connection)
    if readonly:
        connection.execute("PRAGMA query_only=ON")
    return connection


def _validate_paths(paths: list[Path]) -> None:
    for path in paths:
        if not path.is_absolute() or path.is_symlink():
            raise ValueError("explicit absolute non-symlink paths are required")
        if not path.parent.is_dir() or path.parent.is_symlink():
            raise ValueError("output parent must be an existing non-symlink directory")
        if is_formal_database_path(path):
            raise ValueError("formal database paths are forbidden")
        if path.exists():
            info = path.stat()
            if not stat.S_ISREG(info.st_mode) or info.st_nlink != 1:
                raise ValueError("existing paths must be regular single-link files")
    for index, path in enumerate(paths):
        for other in paths[index + 1:]:
            if path.resolve() == other.resolve() or path.exists() and other.exists() and path.samefile(other):
                raise ValueError("source, backup, candidate and report must be distinct")


def _promote(path: Path, target: Path) -> None:
    # Atomic no-clobber publication. The temporary second hard link is removed
    # immediately and is never accepted as an existing input to the command.
    os.link(path, target)
    path.unlink()
    with target.open("rb") as stream:
        os.fsync(stream.fileno())


def create_candidate(*, source: Path, backup: Path, candidate: Path, report: Path) -> dict[str, Any]:
    paths = [source, backup, candidate, report]
    _validate_paths(paths)
    resolve_isolated_candidate(source)
    existing = [path.exists() for path in (backup, candidate, report)]
    if any(existing):
        if not all(existing):
            raise ValueError("partial outputs exist; retain them and choose new output paths")
        receipt = json.loads(report.read_text(encoding="utf-8"))
        if (receipt.get("contract_version") != "account-intake-candidate-v1"
                or receipt.get("source") != str(source) or receipt.get("backup") != str(backup)
                or receipt.get("candidate") != str(candidate)
                or receipt.get("backup_sha256") != _sha(backup)
                or receipt.get("candidate_sha256") != _sha(candidate)):
            raise ValueError("existing candidate outputs differ from their receipt")
        left, original, right = _connection(source, readonly=True), _connection(backup, readonly=True), _connection(candidate, readonly=True)
        try:
            validate_lineage(left, right)
            validate_lineage(original, right)
            if migration_proof(right) != receipt.get("migration_proof"):
                raise ValueError("existing migration proof differs")
        finally:
            left.close(); original.close(); right.close()
        return {**receipt, "status": "unchanged", "sql_writes": 0}
    stages: list[Path] = []
    opened: list[sqlite3.Connection] = []
    try:
        for target in (backup, candidate):
            fd, name = tempfile.mkstemp(prefix=".account-intake-", suffix=".sqlite3", dir=target.parent)
            os.close(fd)
            stages.append(Path(name))
        source_connection = _connection(source, readonly=True); opened.append(source_connection)
        source_connection.execute("BEGIN")
        validate_source(source_connection)
        backup_connection = _connection(stages[0]); opened.append(backup_connection)
        source_connection.backup(backup_connection)
        validate_source(backup_connection)
        candidate_connection = _connection(stages[1]); opened.append(candidate_connection)
        backup_connection.backup(candidate_connection)
        migration = migrate(candidate_connection)
        lineage = validate_lineage(backup_connection, candidate_connection)
        validate_lineage(source_connection, candidate_connection)
        proof = migration_proof(candidate_connection)
        for connection in reversed(opened):
            connection.close()
        opened.clear()
        receipt = {"contract_version": "account-intake-candidate-v1", "status": "candidate",
                   "source": str(source), "backup": str(backup), "candidate": str(candidate),
                   "backup_sha256": _sha(stages[0]), "candidate_sha256": _sha(stages[1]),
                   "migration_proof": proof, "lineage": lineage,
                   "preserved_table_count": len(migration["preserved_tables"]),
                   "provider_calls": 0, "source_modified": False, "production_acceptance": False}
        # Publish the verified backup first; retain it if a later publication
        # fails. Never roll back by deleting a user's existing file.
        _promote(stages[0], backup)
        _promote(stages[1], candidate)
        fd = os.open(report, os.O_WRONLY | os.O_CREAT | os.O_EXCL, 0o600)
        with os.fdopen(fd, "w", encoding="utf-8") as stream:
            json.dump(receipt, stream, ensure_ascii=False, indent=2)
            stream.write("\n"); stream.flush(); os.fsync(stream.fileno())
        return receipt
    finally:
        for connection in reversed(opened):
            connection.close()
        for stage in stages:
            for suffix in ("", "-wal", "-shm", "-journal"):
                path = Path(str(stage) + suffix)
                if path.exists():
                    path.unlink()


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--source", required=True, type=Path)
    parser.add_argument("--backup", required=True, type=Path)
    parser.add_argument("--candidate", required=True, type=Path)
    parser.add_argument("--report", required=True, type=Path)
    args = parser.parse_args()
    os.umask(0o077)
    result = create_candidate(source=args.source, backup=args.backup, candidate=args.candidate, report=args.report)
    print(json.dumps({key: result[key] for key in ("status", "backup", "candidate", "preserved_table_count", "provider_calls", "source_modified")}, ensure_ascii=False))


if __name__ == "__main__":
    main()
