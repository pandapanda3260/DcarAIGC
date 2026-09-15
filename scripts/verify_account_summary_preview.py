#!/usr/bin/env python3
"""Read-only, loopback API acceptance for the isolated account preview."""
import argparse
import collections
import json
from pathlib import Path
import sqlite3
import urllib.error
import urllib.request

from import_account_summary import private_json, readonly


def main():
    p = argparse.ArgumentParser(description=__doc__)
    p.add_argument("--db", type=Path, required=True)
    p.add_argument("--port", type=int, default=4183)
    p.add_argument("--report", type=Path, required=True)
    args = p.parse_args()
    if args.port in {4173, 4174, 8765, 8766, 8767}:
        p.error("Use isolated preview port")
    base = f"http://127.0.0.1:{args.port}"
    def read(path, payload=None):
        request = urllib.request.Request(base + path, data=None if payload is None else json.dumps(payload).encode(), headers={"Content-Type": "application/json"})
        with urllib.request.urlopen(request, timeout=30) as response:
            return json.load(response)
    health = read("/api/v8/health")
    assert health["mode"] == "isolated_account_preview" and health["read_only"] and not health["scheduler_enabled"]
    assert Path(health["database_path"]).samefile(args.db)
    all_items, page = [], 1
    while True:
        result = read("/api/v8/accounts/search", {"scope": "all", "page": page, "page_size": 100})
        all_items.extend(result["items"])
        if len(all_items) >= result["total"]:
            break
        assert result["items"]
        page += 1
    with readonly(args.db) as db:
        db.row_factory = sqlite3.Row
        rows = {r["id"]: dict(r) for r in db.execute("SELECT * FROM account_directory_rows")}
        from preview_account_summary import configure_read_only_paths
        configure_read_only_paths(args.db, code_root=Path(__file__).resolve().parents[1],
                                  evidence_root=Path(health["evidence_root"]))
        from v8.account_capture_eligibility import derive_capture_eligibility
        expected = derive_capture_eligibility(db)
        capture_by_directory = {r["directory_row_id"]: r for r in expected["eligible_members"] + expected["excluded_members"]}
    assert len(all_items) == result["total"] == len(rows)
    assert {i["directory_row_id"] for i in all_items} == set(rows)
    summary_count = 0
    for item in all_items:
        directory = rows[item["directory_row_id"]]
        assert item["account_status"] == directory["account_status"]
        capture = item["automatic_capture"]
        expected_capture = capture_by_directory[item["directory_row_id"]]
        assert capture["eligible"] == expected_capture["eligible"]
        assert capture["reason_code"] == expected_capture["reason_code"]
        assert capture["reason_code"] not in {"account_paused", "account_status_unmarked", "account_status_invalid", "pending_verification"}
        summary = json.loads(directory["raw_json"]).get("account_summary")
        assert len(item["platforms"]) == 1
        identity = item["platforms"][0]
        assert identity["platform"] == directory["platform"]
        assert identity["nickname"] == directory["nickname"]
        if directory["uid"]:
            assert identity["uid"] == directory["uid"] and isinstance(identity["uid"], str)
        assert identity["unique_id"] == (directory["display_account_id"] if directory["display_account_id"] not in {"无", "封"} else "")
        if summary:
            summary_count += 1
            assert item["account_summary"]["fields"] == summary["fields"]
            assert item["account_summary"]["source_row"] == summary["source_row"]
            assert item["account_summary"]["pending_fields"] == list(summary["pending_fields"])
    request = urllib.request.Request(base + "/api/v8/accounts/1", data=b"{}", headers={"Content-Type": "application/json"}, method="PATCH")
    blocked = False
    try:
        urllib.request.urlopen(request, timeout=10)
    except urllib.error.HTTPError as error:
        blocked = error.code == 403
    assert blocked
    report = {"status": "PASS", "preview_url": base + "/accounts", "pages_verified": page,
        "directory_rows": len(all_items), "imported_summaries_verified": summary_count,
        "platform_counts": dict(collections.Counter(i["directory_platform"] for i in all_items)),
        "manual_status_counts": dict(collections.Counter(i["account_status"] for i in all_items)),
        "capture_reason_counts": dict(collections.Counter(i["automatic_capture"]["reason_code"] for i in all_items)),
        "eligible_by_manual_status": dict(collections.Counter(i["account_status"] for i in all_items if i["automatic_capture"]["eligible"])),
        "read_only": True, "scheduler_enabled": False, "write_request_blocked": blocked,
        "database_identity": health["runtime_database_identity"]}
    private_json(args.report, report)
    print(json.dumps(report, ensure_ascii=False))


if __name__ == "__main__":
    main()
