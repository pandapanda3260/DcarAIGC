#!/usr/bin/env python3
"""Verify isolated account capture prerequisites against existing local evidence.

Opens only the explicitly selected candidate database in SQLite read-only mode.
No provider requests, database writes, evidence hydration or runtime admission.
"""
from __future__ import annotations

import argparse
from collections import Counter
from datetime import datetime, timezone
import hashlib
import json
import os
from pathlib import Path
import socket
import sqlite3
import sys
import tempfile

from preview_account_summary import configure_read_only_paths, verify_read_only_evidence_root


def digest(path: Path) -> str:
    with path.open("rb") as handle:
        return hashlib.file_digest(handle, "sha256").hexdigest()


def private_report(path: Path, report: dict, *, database: Path) -> None:
    """Publish one private report atomically, never following an output alias."""
    if not path.is_absolute() or path == database or path.is_symlink():
        raise ValueError("Report must be an explicit file separate from the database")
    if path.exists() and (not path.is_file() or path.samefile(database) or path.stat().st_nlink != 1):
        raise ValueError("Report must not alias an existing database or linked file")
    path.parent.mkdir(mode=0o700, parents=True, exist_ok=True)
    if path.parent != path.parent.resolve(strict=True):
        raise ValueError("Report parent must not traverse a symlink")
    descriptor, temporary = tempfile.mkstemp(prefix=".account-evidence-", dir=path.parent)
    try:
        with os.fdopen(descriptor, "w") as handle:
            json.dump(report, handle, ensure_ascii=False, indent=2)
            handle.write("\n")
            handle.flush()
            os.fsync(handle.fileno())
        os.replace(temporary, path)
    finally:
        if os.path.exists(temporary):
            os.unlink(temporary)


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--db", type=Path, required=True)
    parser.add_argument("--evidence-root", type=Path, required=True)
    parser.add_argument("--raw-response-id", type=int, action="append", default=[],
                        help="Also verify this exact original response and its UID/locator binding; repeatable")
    parser.add_argument("--report", type=Path, required=True)
    args = parser.parse_args()
    root = Path(__file__).resolve().parents[1]
    sys.path.insert(0, str(root / "src" / "dcar_eval"))
    configure_read_only_paths(args.db.resolve(strict=True), code_root=root, evidence_root=args.evidence_root)
    from v8.runtime_database import resolve_isolated_candidate
    access = resolve_isolated_candidate(args.db)

    def deny_network(*_args, **_kwargs):
        raise OSError("Local account evidence verification forbids network access")
    socket.socket.connect = socket.socket.connect_ex = deny_network
    socket.create_connection = socket.getaddrinfo = deny_network
    from v8 import raw_archive, raw_evidence
    from v8.account_capture_eligibility import derive_capture_eligibility, _prove_reference
    verify_read_only_evidence_root(args.evidence_root)

    before = digest(access.database)
    with sqlite3.connect(access.database.as_uri() + "?mode=ro", uri=True) as connection:
        connection.row_factory = sqlite3.Row
        connection.execute("PRAGMA query_only=ON")
        current = derive_capture_eligibility(connection)
        eligible, excluded = current["eligible_members"], current["excluded_members"]
        members = eligible + excluded
        directory_ids = {row[0] for row in connection.execute("SELECT id FROM account_directory_rows")}
        if len(members) != len(directory_ids) or {row["directory_row_id"] for row in members} != directory_ids:
            raise ValueError("Eligibility does not account for every directory exactly once")
        originals = []
        for response_id in sorted(set(args.raw_response_id)):
            row = connection.execute("SELECT * FROM provider_raw_responses WHERE id=?", (response_id,)).fetchone()
            if row is None:
                raise ValueError(f"Raw response {response_id} does not exist")
            raw = dict(row)
            path = raw_archive._safe_existing(raw_archive._source_path(raw["local_path"]))
            loaded = raw_evidence.read_raw_evidence(path, expected_stored_sha256=raw["sha256"],
                                                  expected_stored_size=raw["byte_size"])
            references = [dict(value) for value in connection.execute(
                "SELECT * FROM account_provider_references WHERE source_raw_response_id=? "
                "AND lower(provider)='tikhub' AND reference_kind='sec_user_id'", (response_id,))]
            bindings = []
            for reference in references:
                matches = [member for member in members if member["account_identity_id"] == reference["account_identity_id"]]
                if len(matches) != 1:
                    raise ValueError(f"Raw response {response_id} has no unique directory identity")
                member = matches[0]
                reason, proof = _prove_reference(connection, member, reference)
                if reason != "eligible" or proof["locator_evidence"]["entity_sha256"] != hashlib.sha256(loaded.entity_bytes).hexdigest():
                    raise ValueError(f"Raw response {response_id} UID/locator proof failed: {reason}")
                bindings.append({"directory_row_id": member["directory_row_id"],
                                 "account_identity_id": member["account_identity_id"],
                                 "current_eligible": member["eligible"], "current_reason": member["reason_code"]})
            if not bindings:
                raise ValueError(f"Raw response {response_id} has no bound locator reference")
            originals.append({"raw_response_id": response_id, "path": str(path),
                              "sha256": raw["sha256"], "byte_size": raw["byte_size"],
                              "hash_size_and_uid_locator_verified": True, "bindings": bindings})
        writes = connection.total_changes
    after = digest(access.database)
    if writes or before != after:
        raise ValueError("Candidate database changed during read-only verification")
    report = {"status": "PASS", "checked_at": datetime.now(timezone.utc).isoformat(),
              "database": str(access.database), "database_sha256_before": before,
              "database_sha256_after": after, "writes": writes,
              "code_root": str(root), "evidence_root": str(args.evidence_root),
              "directory_count": len(directory_ids), "eligible_count": len(eligible),
              "excluded_count": len(excluded), "eligible_directory_ids": sorted(row["directory_row_id"] for row in eligible),
              "first_blockers": dict(Counter(row["reason_code"] for row in excluded)),
              "original_evidence": originals, "network_allowed": False, "scheduler_enabled": False,
              "scope": "Account prerequisites and selected original evidence; no live capture or monitoring acceptance"}
    private_report(args.report, report, database=access.database)
    print(json.dumps({key: report[key] for key in ("status", "directory_count", "eligible_count", "excluded_count", "writes")}, ensure_ascii=False))


if __name__ == "__main__":
    main()
