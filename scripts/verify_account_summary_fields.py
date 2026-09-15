#!/usr/bin/env python3
"""Verify every input disposition and retained source field after local import."""
import argparse
import collections
import csv
import json
from pathlib import Path
import sqlite3

from import_account_summary import HEADERS, private_json, read_workbook, readonly


def main():
    p = argparse.ArgumentParser(description=__doc__)
    for name in ("db", "before", "xlsx", "metadata", "result", "output"):
        p.add_argument("--" + name, required=True, type=Path)
    args = p.parse_args()
    payload = read_workbook(args.xlsx, args.metadata)
    result = json.loads(args.result.read_text())
    records = {r["sourceRow"]: r for r in payload["records"]}
    dispositions = {r["source_row"]: r for r in result["rows"]}
    assert len(dispositions) == len(result["rows"]) == len(records)
    assert set(records) == set(dispositions)
    before, after = readonly(args.before), readonly(args.db)
    before.row_factory = after.row_factory = sqlite3.Row
    current = {r["id"]: dict(r) for r in after.execute("SELECT * FROM account_directory_rows")}
    old_accounts = {r["id"]: dict(r) for r in before.execute("SELECT * FROM accounts")}
    old_directories = {r["id"]: dict(r) for r in before.execute("SELECT * FROM account_directory_rows")}
    old_identities = {r["id"]: dict(r) for r in before.execute("SELECT * FROM account_platform_identities")}
    def known(value):
        return value not in (None, "", "无", "未知", "—", "-") and not any(s in str(value) for s in ("待核实", "待核验", "待确认"))
    mapped = {"账号名称": "nickname", HEADERS[3]: "display_account_id", "运营人员": "operator_name", "手机号": "phone"}
    accepted, exceptions = [], []
    for number, source in records.items():
        disposition = dispositions[number]
        pending = disposition.get("pending_fields", {})
        if disposition["status"] in {"review", "asset"}:
            assert disposition["source_record"] == {k: source[k] for k in ("raw", "comment", "metadata")}
            assert not any(json.loads(r["raw_json"]).get("account_summary", {}).get("source_row") == number for r in current.values())
        else:
            directory = current[disposition["directory_row_id"]]
            summary = json.loads(directory["raw_json"])["account_summary"]
            assert summary["raw"] == source["raw"]
            assert summary["comment"] == source["comment"]
            assert summary["metadata"] == source["metadata"]
            assert summary["source_sha256"] == payload["sha256"]
            assert summary["source_row"] == number and summary["source_sheet"] == payload["sheet"]
            assert set(summary["fields"]) == set(HEADERS)
            for field, value in source["raw"].items():
                if value in (None, "") or field in pending:
                    old = old_directories.get(directory["id"])
                    if old and field in mapped and known(old[mapped[field]]):
                        assert directory[mapped[field]] == old[mapped[field]], f"Overwrote pending/blank directory {number}/{field}"
                    continue
                expected = value if field == "粉丝" else str(value).strip()
                if field == "uid" and source["raw"]["平台"] == "小红书":
                    expected = expected.lower()
                assert summary["fields"][field] == expected, f"Accepted field mismatch {number}/{field}"
                if field in {HEADERS[3], "uid", "手机号", "使用人证件号码"}:
                    assert isinstance(summary["fields"][field], str), f"Identifier type {number}/{field}"
            aid, iid = disposition.get("account_id"), disposition.get("identity_id")
            if aid in old_accounts:
                account = dict(after.execute("SELECT * FROM accounts WHERE id=?", (aid,)).fetchone())
                for field, column in (("手机号", "phone"), ("运营人员", "operator_name")):
                    if source["raw"][field] in (None, "") or field in pending:
                        assert account[column] == old_accounts[aid][column], f"Overwrote pending/blank subject {number}/{field}"
            if iid in old_identities:
                identity = dict(after.execute("SELECT * FROM account_platform_identities WHERE id=?", (iid,)).fetchone())
                for field, column in (("账号名称", "nickname"), ("是否实名", "real_name_status")):
                    if source["raw"][field] in (None, "") or field in pending:
                        assert identity[column] == old_identities[iid][column], f"Overwrote pending/blank identity {number}/{field}"
            accepted.append(disposition)
        if disposition["status"] == "review" or pending:
            exceptions.append({"Excel行号": number, "平台": source["raw"]["平台"], "账号名称": source["raw"]["账号名称"],
                "uid": source["raw"]["uid"], "账号ID": source["raw"][HEADERS[3]], "处理": disposition["status"],
                "整行待核实原因": disposition.get("reason", ""), "待核实字段": "；".join(f"{k}：{v}" for k, v in pending.items())})
    fields_counter = collections.Counter(k for r in accepted for k in r["pending_fields"])
    untouched_directory = set(old_directories) - {r["directory_row_id"] for r in accepted}
    for did in untouched_directory:
        assert current[did] == old_directories[did]
    report = {"status": "PASS", "source_rows": len(records), "accepted_rows": len(accepted),
        "full_fields_and_comments_preserved": len(accepted), "unresolved_source_records_preserved": len(records) - len(accepted),
        "source_sha256": payload["sha256"], "dispositions": result["counts"],
        "untouched_original_directory_rows": len(untouched_directory),
        "original_subjects_absent_from_import_preserved": len(set(old_accounts) - {r["account_id"] for r in accepted}),
        "new_subjects": result["new_account_count"], "existing_subjects_added_to_directory": result["existing_account_directory_added_count"],
        "new_directory_only_rows": sum(r["status"] == "added" and r["account_id"] is None for r in accepted),
        "accepted_rows_with_pending_fields": sum(bool(r["pending_fields"]) for r in accepted),
        "pending_field_counts": dict(fields_counter),
        "review_reasons": dict(collections.Counter(r["reason"] for r in dispositions.values() if r["status"] == "review")),
        "review_examples": [n for n in (88, 571, 617) if dispositions[n]["status"] == "review"],
        "exceptions_rows": len(exceptions)}
    args.output.mkdir(parents=True, exist_ok=True, mode=0o700)
    private_json(args.output / "field-verification.json", report)
    path = args.output / "待核实明细.csv"
    path.touch(mode=0o600, exist_ok=True)
    with path.open("w", newline="", encoding="utf-8-sig") as f:
        writer = csv.DictWriter(f, fieldnames=list(exceptions[0]))
        writer.writeheader()
        # Prevent names/IDs that begin with =,+,-,@ from executing as formulas
        # when the audit CSV is opened in Excel; DB/source strings are untouched.
        writer.writerows({k: ("'" + v if isinstance(v, str) and v.startswith(("=", "+", "-", "@")) else v) for k, v in row.items()} for row in exceptions)
    path.chmod(0o600)
    before.close(); after.close()
    print(json.dumps(report, ensure_ascii=False))


if __name__ == "__main__":
    main()
