#!/usr/bin/env python3
"""Build only an isolated cleanup candidate; never install it or start capture."""

from __future__ import annotations

import argparse
import json
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[1] / "src" / "dcar_eval"))
from v8.account_cleanup import build_candidate  # noqa: E402


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--source-database", type=Path, required=True)
    parser.add_argument("--output-directory", type=Path, required=True)
    parser.add_argument("--approved-plan", type=Path, required=True)
    parser.add_argument("--accounts", type=Path)
    parser.add_argument("--expected-deleted-contents", type=int, required=True)
    parser.add_argument("--expected-source-sha256")
    args = parser.parse_args()
    receipt = build_candidate(
        source_database=args.source_database, output_directory=args.output_directory,
        approved_plan=json.loads(args.approved_plan.read_text()),
        expected_deleted_contents=args.expected_deleted_contents,
        account_payload=json.loads(args.accounts.read_text()) if args.accounts else None,
        expected_source_sha256=args.expected_source_sha256,
    )
    print(json.dumps({key: receipt[key] for key in ("status", "capture_state", "source_backup", "candidate", "historical_paid_scope_exclusions", "verification")}, indent=2))


if __name__ == "__main__":
    main()
