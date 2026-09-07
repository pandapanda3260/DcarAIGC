#!/usr/bin/env python3
"""Inspect an audit log, or preserve evidence and repair only a torn final line.

Run from the matching release with PYTHONPATH=src/dcar_eval. Repair requires a
stopped gateway and no concurrent admin CLI. It never deletes a complete bad
line, grants access, changes the account DB, or certifies identity reconciliation.
"""

from __future__ import annotations

import argparse
import fcntl
import hashlib
import json
import os
import shutil
import sqlite3
import stat
import subprocess
import tempfile
from datetime import datetime, timezone
from pathlib import Path
from typing import Callable

from dcar_auth.store import AuthStore, ChangeLogError

MAX_LOG_BYTES = 64 * 1024 * 1024


class RepairError(RuntimeError):
    pass


def require_gateway_stopped() -> None:
    result = subprocess.run(
        ["/usr/bin/systemctl", "show", "dcar-auth.service", "--property=ActiveState", "--value"],
        capture_output=True, text=True, timeout=10, check=False,
    )
    if result.returncode or result.stdout.strip() not in {"inactive", "failed"}:
        raise RepairError("stop dcar-auth.service and all admin CLI writers before repair")


def regular(path: Path) -> os.stat_result:
    info = path.lstat()
    if not stat.S_ISREG(info.st_mode) or info.st_nlink != 1 or path.parent.is_symlink():
        raise RepairError("source must be a regular, single-link file in a real directory")
    return info


def read_locked(descriptor: int) -> tuple[bytes, os.stat_result]:
    info = os.fstat(descriptor)
    if not stat.S_ISREG(info.st_mode) or info.st_nlink != 1:
        raise RepairError("audit log is not a regular single-link file")
    if stat.S_IMODE(info.st_mode) != 0o600:
        raise RepairError("audit log must have mode 0600")
    if info.st_size > MAX_LOG_BYTES:
        raise RepairError("audit log exceeds 64 MiB; use a separately reviewed offline recovery")
    chunks: list[bytes] = []
    remaining = info.st_size
    while remaining:
        chunk = os.read(descriptor, min(remaining, 64 * 1024))
        if not chunk:
            raise RepairError("audit log changed or ended while being read")
        chunks.append(chunk)
        remaining -= len(chunk)
    return b"".join(chunks), info


def plan_repair(store: AuthStore, original: bytes) -> tuple[str, bytes]:
    if not original or original.endswith(b"\n"):
        store._parse_change_log_text(original.decode("utf-8"))
        return "valid", original
    split = original.rfind(b"\n") + 1
    prefix, tail = original[:split], original[split:]
    # A bad middle record is never converted into apparently valid history.
    store._parse_change_log_text(prefix.decode("utf-8"))
    try:
        json.loads(tail.decode("utf-8"))
    except (UnicodeDecodeError, json.JSONDecodeError):
        return "quarantine_incomplete_tail", prefix
    # A complete JSON event lacking only its delimiter is retained, not dropped.
    # A complete but invalid event is ambiguous/corrupt and must fail closed.
    repaired = original + b"\n"
    store._parse_change_log_text(repaired.decode("utf-8"))
    return "append_missing_newline", repaired


def write_new(path: Path, payload: bytes) -> None:
    descriptor = os.open(path, os.O_WRONLY | os.O_CREAT | os.O_EXCL, 0o600)
    try:
        os.fchmod(descriptor, 0o600)
        view = memoryview(payload)
        while view:
            count = os.write(descriptor, view)
            if count <= 0:
                raise OSError("short evidence write")
            view = view[count:]
        os.fsync(descriptor)
    finally:
        os.close(descriptor)


def fsync_directory(path: Path) -> None:
    descriptor = os.open(path, os.O_RDONLY | getattr(os, "O_DIRECTORY", 0))
    try:
        os.fsync(descriptor)
    finally:
        os.close(descriptor)


def inspect_log(database: Path, log: Path) -> dict[str, object]:
    regular(log)
    descriptor = os.open(log, os.O_RDONLY | getattr(os, "O_NOFOLLOW", 0))
    try:
        fcntl.flock(descriptor, fcntl.LOCK_SH | fcntl.LOCK_NB)
        original, _ = read_locked(descriptor)
        store = AuthStore(database, change_log_path=log)
        action, repaired = plan_repair(store, original)
        entries = store._parse_change_log_text(repaired.decode("utf-8"))
        return {
            "status": action, "bytes": len(original),
            "unconfirmed": sum(entry["state"] != "committed" for entry in entries),
            "sha256": hashlib.sha256(original).hexdigest(),
        }
    finally:
        os.close(descriptor)


def repair_tail(
    database: Path, log: Path, backup_dir: Path,
    *, stopped_check: Callable[[], None] = require_gateway_stopped,
) -> dict[str, object]:
    stopped_check()
    regular(database)
    regular(log)
    if backup_dir.is_symlink() or not backup_dir.is_dir():
        raise RepairError("create a trusted backup directory before repair")
    connection = sqlite3.connect(database.absolute().as_uri() + "?mode=rw", uri=True, timeout=0)
    descriptor: int | None = None
    try:
        # Same lock order as account mutations: DB transaction, then audit flock.
        # Keep the log inode, so a previously-open append FD cannot become orphaned.
        connection.execute("BEGIN IMMEDIATE")
        descriptor = os.open(log, os.O_RDWR | getattr(os, "O_NOFOLLOW", 0))
        fcntl.flock(descriptor, fcntl.LOCK_EX | fcntl.LOCK_NB)
        original, info = read_locked(descriptor)
        store = AuthStore(database, change_log_path=log)
        action, repaired = plan_repair(store, original)
        if action == "valid":
            entries = store._parse_change_log_text(original.decode("utf-8"))
            return {
                "status": "valid", "changed": False,
                "unconfirmed": sum(entry["state"] != "committed" for entry in entries),
            }
        if shutil.disk_usage(backup_dir).free < len(original) * 2 + 1024 * 1024:
            raise RepairError("insufficient space for complete repair evidence")
        evidence = Path(tempfile.mkdtemp(prefix="audit-tail-", dir=backup_dir))
        os.chmod(evidence, 0o700)
        write_new(evidence / "original.log", original)
        receipt: dict[str, object] = {
            "schema": "dcar-auth-log-tail-repair-v1", "action": action,
            "at": datetime.now(timezone.utc).isoformat(), "source": str(log),
            "source_sha256": hashlib.sha256(original).hexdigest(),
            "repaired_sha256": hashlib.sha256(repaired).hexdigest(),
            "source_bytes": len(original), "repaired_bytes": len(repaired),
            "uid": info.st_uid, "gid": info.st_gid, "mode": "0600",
            "reconciliation_required": True, "evidence": str(evidence),
        }
        write_new(evidence / "prepared.json", json.dumps(receipt, sort_keys=True).encode() + b"\n")
        fsync_directory(evidence)
        fsync_directory(backup_dir)
        current = regular(log)
        if (current.st_dev, current.st_ino, current.st_size) != (info.st_dev, info.st_ino, info.st_size):
            raise RepairError("audit log was replaced while preparing repair")
        stopped_check()
        if action == "append_missing_newline":
            os.lseek(descriptor, 0, os.SEEK_END)
            if os.write(descriptor, b"\n") != 1:
                raise RepairError("could not complete final delimiter")
        else:
            os.ftruncate(descriptor, len(repaired))
        os.fsync(descriptor)
        os.lseek(descriptor, 0, os.SEEK_SET)
        actual, _ = read_locked(descriptor)
        if actual != repaired:
            raise RepairError("repaired log verification failed; preserve the evidence")
        store._parse_change_log_text(actual.decode("utf-8"))
        receipt.update(status="repaired_reconciliation_required", changed=True)
        write_new(evidence / "completed.json", json.dumps(receipt, sort_keys=True).encode() + b"\n")
        fsync_directory(evidence)
        return receipt
    finally:
        if descriptor is not None:
            os.close(descriptor)
        connection.rollback()
        connection.close()


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--db", type=Path, required=True)
    parser.add_argument("--change-log", type=Path, required=True)
    parser.add_argument("--repair-tail", action="store_true")
    parser.add_argument("--backup-dir", type=Path)
    arguments = parser.parse_args(argv)
    try:
        if arguments.repair_tail:
            if arguments.backup_dir is None:
                parser.error("--repair-tail requires --backup-dir")
            result = repair_tail(arguments.db, arguments.change_log, arguments.backup_dir)
        else:
            result = inspect_log(arguments.db, arguments.change_log)
    except (OSError, RuntimeError, sqlite3.Error, ChangeLogError, UnicodeError) as exc:
        print(f"audit log remains untrusted: {exc}")
        return 2
    print(json.dumps(result, sort_keys=True))
    return 0 if result["status"] == "valid" and not result.get("unconfirmed") else 3


if __name__ == "__main__":
    raise SystemExit(main())
