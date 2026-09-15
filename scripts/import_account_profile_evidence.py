#!/usr/bin/env python3
"""Restore checksum-verified profile evidence into an isolated local database."""
from __future__ import annotations

import argparse
from collections import Counter
import json
import os
from pathlib import Path
import socket
import sqlite3
import sys

from import_account_summary import backup, counts, readonly, sha256, stamp
from preview_account_summary import configure_read_only_paths
from verify_local_account_evidence import private_report


def main():
    p = argparse.ArgumentParser(description=__doc__)
    p.add_argument("--db", type=Path, required=True)
    p.add_argument("--evidence-root", type=Path, required=True, help="Existing local evidence root")
    p.add_argument("--input", type=Path, action="append", required=True, help="Saved response envelopes; repeatable")
    p.add_argument("--evidence-dir", type=Path, required=True, help="New imported evidence files")
    p.add_argument("--report", type=Path, required=True)
    p.add_argument("--backup", type=Path)
    p.add_argument("--apply", action="store_true")
    args = p.parse_args()
    os.umask(0o077)
    files = [args.db, args.report, *args.input, *([args.backup] if args.backup else [])]
    if any(not path.is_absolute() or path.is_symlink() for path in files + [args.evidence_dir, args.evidence_root]):
        p.error("Use explicit absolute, non-symlink paths")
    for index, path in enumerate(files):
        if any(path.resolve() == other.resolve() or path.exists() and other.exists() and path.samefile(other) for other in files[index + 1:]):
            p.error("Database, input, report and backup must be different files")
    if args.apply and not args.backup:
        p.error("--apply requires a new --backup path")
    root = Path(__file__).resolve().parents[1]
    configure_read_only_paths(args.db, code_root=root, evidence_root=args.evidence_root)
    def deny(*_args, **_kwargs):
        raise OSError("Offline evidence import forbids network access")
    socket.socket.connect = socket.socket.connect_ex = socket.create_connection = socket.getaddrinfo = deny
    from v8.runtime_database import resolve_isolated_candidate
    from v8.account_capture_eligibility import derive_capture_eligibility
    from v8.account_profile_evidence_import import CONTRACT, plan_import, apply_import
    access = resolve_isolated_candidate(args.db)
    before_sha = sha256(access.database)
    with readonly(access.database) as connection:
        connection.row_factory = sqlite3.Row
        plans, rows = plan_import(connection, args.input)
        before_counts = counts(connection)
        before_eligible = len(derive_capture_eligibility(connection)["eligible_members"])
    backup_receipt = backup(access.database, args.backup) if args.apply and plans else None
    writes = 0
    if args.apply and plans:
        with sqlite3.connect(access.database) as connection:
            connection.row_factory = sqlite3.Row
            connection.execute("PRAGMA foreign_keys=ON")
            connection.execute("BEGIN IMMEDIATE")
            # Recheck against the locked database; do not trust an earlier plan.
            plans, rows = plan_import(connection, args.input)
            start_changes = connection.total_changes
            apply_import(connection, plans, evidence_dir=args.evidence_dir)
            if connection.execute("PRAGMA foreign_key_check").fetchall():
                raise ValueError("Foreign key check failed")
            writes = connection.total_changes - start_changes
    with readonly(access.database) as connection:
        connection.row_factory = sqlite3.Row
        after_counts = counts(connection)
        after_eligible = len(derive_capture_eligibility(connection)["eligible_members"])
    report = {"contract": CONTRACT, "status": "PASS", "mode": "apply" if args.apply else "dry_run",
              "database": str(access.database), "before_sha256": before_sha, "after_sha256": sha256(access.database),
              "backup": backup_receipt, "counts": dict(Counter(row["status"] for row in rows)),
              "eligible_before": before_eligible, "eligible_after": after_eligible, "writes": writes,
              "before_counts": before_counts, "after_counts": after_counts, "rows": rows,
              "network_requests": 0, "scheduler_started": False, "completed_at": stamp()}
    private_report(args.report, report, database=access.database)
    print(json.dumps({key: report[key] for key in ("status", "mode", "counts", "eligible_before", "eligible_after", "writes")}, ensure_ascii=False))


if __name__ == "__main__":
    main()
