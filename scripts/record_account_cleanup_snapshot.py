#!/usr/bin/env python3
"""Record genuine read-only cleanup publication evidence before final installation hash."""
import argparse
import json
from pathlib import Path
import sqlite3
import sys

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT / "src/dcar_eval"))
from v8.account_cleanup_snapshot import record_candidate, reference, EVIDENCE_ROLES


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--candidate", type=Path, required=True)
    parser.add_argument("--deployment-id", required=True)
    parser.add_argument("--recorded-at", required=True)
    parser.add_argument("--output", type=Path, required=True)
    for role in EVIDENCE_ROLES:
        parser.add_argument("--" + role.replace("_", "-"), type=Path, required=True)
    args = parser.parse_args()
    if args.candidate.is_symlink() or args.candidate.name != "candidate.sqlite3" or not args.candidate.is_file() or args.output.exists():
        parser.error("Use an existing offline candidate.sqlite3 and a new receipt output")
    evidence = {role: reference(getattr(args, role).resolve(strict=True)) for role in EVIDENCE_ROLES}
    with sqlite3.connect(args.candidate) as connection:
        connection.execute("PRAGMA foreign_keys=ON")
        connection.execute("BEGIN IMMEDIATE")
        result = record_candidate(connection, evidence=evidence, deployment_id=args.deployment_id, recorded_at=args.recorded_at)
        connection.commit()
    with args.output.open("x") as stream:
        stream.write(json.dumps(result, ensure_ascii=False, sort_keys=True, indent=2) + "\n")
    print(json.dumps({key: result[key] for key in ("contract_version", "deployment_id", "status", "receipt_sha256", "readonly_publish_eligible")}))


if __name__ == "__main__":
    main()
