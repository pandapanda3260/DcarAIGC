#!/usr/bin/env python3
"""Compare offline import database with its full pre-import backup."""
from __future__ import annotations

import argparse
import hashlib
import json
from pathlib import Path
import sqlite3

from import_account_summary import readonly, private_json


def table_hash(connection, name):
    digest = hashlib.sha256()
    quoted = '"' + name.replace('"', '""') + '"'
    columns = connection.execute(f"PRAGMA table_info({quoted})").fetchall()
    pk = [r[1] for r in sorted(columns, key=lambda r: r[5]) if r[5]]
    order = ",".join('"' + x.replace('"', '""') + '"' for x in pk) if pk else "rowid"
    count = 0
    for row in connection.execute(f"SELECT * FROM {quoted} ORDER BY {order}"):
        digest.update(json.dumps(tuple(row), ensure_ascii=False, separators=(",", ":"), default=lambda value: {"blob_hex": value.hex()}).encode())
        digest.update(b"\n")
        count += 1
    return {"rows": count, "sha256": digest.hexdigest()}


def audit(before_path, after_path):
    with readonly(before_path) as before, readonly(after_path) as after:
        names = [r[0] for r in before.execute("SELECT name FROM sqlite_master WHERE type='table' ORDER BY name")]
        schema_sql = "SELECT type,name,tbl_name,sql FROM sqlite_master ORDER BY type,name"
        assert before.execute(schema_sql).fetchall() == after.execute(schema_sql).fetchall(), "Schema changed"
        preserved = {}
        for name in names:
            if name in {"accounts", "account_platform_identities", "account_directory_rows", "sqlite_sequence"}:
                continue
            a, b = table_hash(before, name), table_hash(after, name)
            assert a == b, "Historical table changed: " + name
            preserved[name] = b
        immutable = {
            "accounts": ["id", "enabled", "created_at"],
            "account_platform_identities": ["id", "account_id", "platform", "uid", "created_at"],
            "account_directory_rows": ["id", "account_id", "platform", "identity_status", "imported_at"],
        }
        identity_checks = {}
        for name, columns in immutable.items():
            # Directory-only entries may be linked to a newly verified identity;
            # preexisting non-null business associations must stay fixed.
            column_sql = ",".join(columns)
            original = {row[0]: tuple(row) for row in before.execute(f"SELECT {column_sql} FROM {name}")}
            current = {row[0]: tuple(row) for row in after.execute(f"SELECT {column_sql} FROM {name}")}
            for key, value in original.items():
                actual = current.get(key)
                assert actual is not None, f"Deleted {name} primary key {key}"
                assert value == actual, f"Changed immutable {name} association {key}"
            identity_checks[name] = {"preserved_primary_keys": len(original), "after_count": len(current)}
        quick = after.execute("PRAGMA quick_check").fetchall()
        foreign = after.execute("PRAGMA foreign_key_check").fetchall()
        assert quick == [("ok",)] and not foreign, "SQLite integrity failure"
        return {"status": "PASS", "schema_unchanged": True, "quick_check": "ok", "foreign_key_check": "ok", "preserved_tables": preserved, "primary_key_checks": identity_checks}


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--before", type=Path, required=True)
    parser.add_argument("--after", type=Path, required=True)
    parser.add_argument("--report", type=Path, required=True)
    args = parser.parse_args()
    report = audit(args.before, args.after)
    private_json(args.report, report)
    print(json.dumps({"status": report["status"], "unchanged_history_tables": len(report["preserved_tables"]), "primary_keys": report["primary_key_checks"]}))
