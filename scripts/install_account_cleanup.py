"""Install an explicitly verified account-cleanup generation while Writer is stopped.

This is a separate destructive-projection contract, not a schema19 migration.
The caller prepares the candidate and freezes the final source. No process is
stopped here, no schema guard is disabled, and rollback never overwrites writes.
"""
from __future__ import annotations

import argparse
import fcntl
import hashlib
import json
import os
from pathlib import Path
import plistlib
import pwd
import sqlite3
import stat
import subprocess
from datetime import datetime, timezone

CONTRACT = "account-cleanup-install-v1"


def digest_file(path: Path) -> str:
    with path.open("rb") as stream:
        return hashlib.file_digest(stream, "sha256").hexdigest()


def regular(path: Path) -> os.stat_result:
    value = path.lstat()
    if not stat.S_ISREG(value.st_mode) or value.st_nlink != 1 or path.resolve() != path:
        raise ValueError(f"Expected a canonical single-link regular file: {path}")
    return value


def sync_dir(path: Path) -> None:
    descriptor = os.open(path, os.O_RDONLY)
    try:
        os.fsync(descriptor)
    finally:
        os.close(descriptor)


def atomic_json(path: Path, value: dict) -> None:
    temporary = path.with_name(path.name + ".tmp")
    descriptor = os.open(temporary, os.O_WRONLY | os.O_CREAT | os.O_EXCL, 0o600)
    with os.fdopen(descriptor, "w") as stream:
        json.dump(value, stream, ensure_ascii=False, sort_keys=True, indent=2)
        stream.write("\n")
        stream.flush()
        os.fsync(stream.fileno())
    os.replace(temporary, path)
    sync_dir(path.parent)


def verify_database(path: Path, expected: dict) -> None:
    connection = sqlite3.connect(path.as_uri() + "?mode=ro", uri=True)
    try:
        connection.execute("PRAGMA query_only=ON")
        if connection.execute("PRAGMA user_version").fetchone()[0] != 20:
            raise ValueError("Cleanup requires schema20")
        if connection.execute("PRAGMA integrity_check").fetchone()[0] != "ok":
            raise ValueError("Candidate integrity check failed")
        if connection.execute("PRAGMA foreign_key_check").fetchone() is not None:
            raise ValueError("Candidate has a foreign-key violation")
        for table, key in (("accounts", "account_ids"), ("content_items", "content_ids")):
            actual = {row[0] for row in connection.execute(f"SELECT id FROM {table}")}
            if actual != set(expected[key]):
                raise ValueError(f"Candidate {table} scope changed")
        if connection.execute("SELECT count(*) FROM account_directory_rows").fetchone()[0] != 292:
            raise ValueError("Candidate authoritative directory is incomplete")
    finally:
        connection.close()


def install(plan_path: Path, expected_sha256: str) -> dict:
    regular(plan_path)
    if digest_file(plan_path) != expected_sha256:
        raise ValueError("Prepared installation plan changed")
    plan = json.loads(plan_path.read_text())
    if plan.get("contract") != "account-cleanup-prepared-v1":
        raise ValueError("Unrecognized cleanup installation plan")
    account_home = Path(pwd.getpwuid(os.getuid()).pw_dir)
    plist = account_home / "Library/LaunchAgents/cn.tj.dcar.writer-worker.plist"
    installed = plistlib.loads(plist.read_bytes())
    environment = installed["EnvironmentVariables"]
    formal = Path(environment["DCAR_V8_DB"])
    lock_path = Path(environment["DCAR_WRITER_LOCK"])
    if str(formal) != plan["formal_database"]:
        raise ValueError("Prepared plan targets another installed database")
    candidate = Path(plan["candidate"]["path"])
    rollback = Path(plan["rollback_database"])
    receipt_path = Path(plan["install_receipt_path"])
    if rollback.exists() or receipt_path.exists() or candidate == formal:
        raise ValueError("Refusing to reuse a completed/partial installation path")
    for path, checksum in ((formal, plan["frozen_formal_sha256"]), (candidate, plan["candidate"]["sha256"])):
        regular(path)
        if digest_file(path) != checksum:
            raise ValueError(f"Frozen database changed: {path}")
        for suffix in ("-wal", "-journal"):
            sidecar = Path(str(path) + suffix)
            if sidecar.exists() and sidecar.stat().st_size:
                raise ValueError(f"Database still has a live journal: {path}")
    for reference in plan["evidence"]:
        path = Path(reference["path"])
        regular(path)
        if digest_file(path) != reference["sha256"]:
            raise ValueError("Prepared migration/runtime evidence changed")
    # All DB readers must be stopped before replacing the inode. lsof failure
    # other than 'no matches' is not treated as proof of absence.
    handles = subprocess.run(["/usr/sbin/lsof", "-t", "--", str(formal)], capture_output=True, text=True)
    if handles.returncode not in (0, 1) or handles.stdout.strip():
        raise ValueError("Formal database still has open handles")
    regular(lock_path)
    with lock_path.open("r+b") as lease:
        fcntl.flock(lease, fcntl.LOCK_EX | fcntl.LOCK_NB)
        verify_database(candidate, plan["expected_scope"])
        if digest_file(formal) != plan["frozen_formal_sha256"]:
            raise ValueError("Formal database changed before installation")
        original = regular(formal)
        target = regular(candidate)
        if original.st_dev != target.st_dev or rollback.parent.stat().st_dev != original.st_dev:
            raise ValueError("Atomic cleanup cutover requires one filesystem")
        os.rename(formal, rollback)
        try:
            os.rename(candidate, formal)
            sync_dir(formal.parent)
            sync_dir(rollback.parent)
            value = {"contract": CONTRACT, "status": "installed",
                     "installed_at": datetime.now(timezone.utc).isoformat(),
                     "prepared_plan": {"path": str(plan_path), "sha256": expected_sha256},
                     "formal_database": str(formal),
                     "installed": {"device": target.st_dev, "inode": target.st_ino,
                                   "sha256": plan["candidate"]["sha256"]},
                     "rollback_database": str(rollback), "rollback_sha256": plan["frozen_formal_sha256"],
                     "source_database_sha256": plan["source_database_sha256"],
                     "build_receipt": plan["build_receipt"],
                     "expected_scope": plan["expected_scope"],
                     "rollback_policy": "restore only before new writes; otherwise replay deltas or repair forward"}
            atomic_json(receipt_path, value)
            return value
        except BaseException:
            if formal.exists():
                os.rename(formal, candidate)
            os.rename(rollback, formal)
            sync_dir(formal.parent)
            raise


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--prepared-plan", required=True, type=Path)
    parser.add_argument("--expected-sha256", required=True)
    args = parser.parse_args()
    result = install(args.prepared_plan, args.expected_sha256)
    print(json.dumps({key: result[key] for key in ("status", "formal_database", "installed", "rollback_database")}))


if __name__ == "__main__":
    main()
