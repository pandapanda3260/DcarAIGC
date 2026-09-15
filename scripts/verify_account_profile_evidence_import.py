#!/usr/bin/env python3
"""Verify an offline profile evidence import preserved all unrelated data."""
import argparse
import json
from pathlib import Path

from audit_account_summary_import import table_hash
from import_account_summary import readonly
from verify_local_account_evidence import private_report


def main():
    p = argparse.ArgumentParser(description=__doc__)
    for name in ("before", "after", "import-report", "report"):
        p.add_argument("--" + name, type=Path, required=True)
    args = p.parse_args()
    imported = json.loads(args.import_report.read_text())
    additions = [row for row in imported["rows"] if row["status"] == "imported"]
    with readonly(args.before) as before, readonly(args.after) as after:
        schema = "SELECT type,name,tbl_name,sql FROM sqlite_master ORDER BY type,name"
        assert before.execute(schema).fetchall() == after.execute(schema).fetchall(), "schema changed"
        tables = [row[0] for row in before.execute("SELECT name FROM sqlite_master WHERE type='table' ORDER BY name")]
        preserved = {}
        for table in tables:
            if table in {"provider_raw_responses", "account_provider_references", "sqlite_sequence"}:
                continue
            old, new = table_hash(before, table), table_hash(after, table)
            assert old == new, "unrelated data changed: " + table
            preserved[table] = new
        old = before.execute("SELECT * FROM provider_raw_responses").fetchall()
        new = after.execute("SELECT * FROM provider_raw_responses").fetchall()
        assert set(old) <= set(new), "old raw evidence changed"
        assert len(new) - len(old) == len(additions), "unexpected raw evidence count"
        old_refs = {tuple(row[:3]): tuple(row) for row in before.execute("SELECT * FROM account_provider_references")}
        new_refs = {tuple(row[:3]): tuple(row) for row in after.execute("SELECT * FROM account_provider_references")}
        imported_bindings = {row["identity_id"]: row["raw_response_id"] for row in additions}
        linked_existing = 0
        for key, old in old_refs.items():
            current = new_refs.get(key)
            assert current is not None, "old provider reference deleted"
            if current != old:
                assert old[4] is None and current[4] == imported_bindings.get(key[0]), "old source replaced"
                assert current[:4] + current[5:] == old[:4] + old[5:], "reference fields changed"
                linked_existing += 1
        new_reference_count = len(new_refs) - len(old_refs)
        assert new_reference_count + linked_existing == len(additions), "unexpected reference count"
        assert after.execute("PRAGMA quick_check").fetchall() == [("ok",)]
        assert not after.execute("PRAGMA foreign_key_check").fetchall()
    report = {"status": "PASS", "schema_unchanged": True, "integrity": "ok",
        "unrelated_tables_preserved": len(preserved), "preserved_tables": preserved,
        "existing_evidence_unchanged": True, "new_raw_responses": len(additions),
        "new_provider_references": new_reference_count, "linked_existing_references": linked_existing,
        "new_account_rows": 0}
    private_report(args.report, report, database=args.after)
    print(json.dumps({key: value for key, value in report.items() if key != "preserved_tables"}))


if __name__ == "__main__":
    main()
