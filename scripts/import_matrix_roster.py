"""Offline bootstrap conversion. No network, service control or production writes."""

from __future__ import annotations

import argparse
import json
from pathlib import Path
from typing import Any

from v8.account_roster import accept_candidate, prepare_candidate
from v8.account_roster_bootstrap import build_bootstrap_envelope
from v8.storage import connect, is_formal_database_path, require_schema_compatibility


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--roster", required=True, type=Path)
    parser.add_argument("--uid-evidence", required=True, type=Path)
    parser.add_argument("--candidate-db", required=True, type=Path)
    parser.add_argument("--output", required=True, type=Path)
    parser.add_argument("--accept", action="store_true")
    args = parser.parse_args()
    if is_formal_database_path(args.candidate_db):
        parser.error("Offline roster bootstrap refuses the production database and its aliases")
    if not args.candidate_db.is_file():
        parser.error("An existing isolated schema18 candidate is required")
    if args.output.exists():
        parser.error("Output must be a new path")
    with connect(args.candidate_db) as connection:
        require_schema_compatibility(connection, supported_versions=frozenset({18}))
        envelope = build_bootstrap_envelope(args.roster, args.uid_evidence, connection)
        args.output.parent.mkdir(parents=True, exist_ok=True)
        with args.output.open("x", encoding="utf-8") as stream:
            json.dump({key: envelope[key] for key in ("payload", "source_path", "source_sha256")},
                      stream, ensure_ascii=False, indent=2)
        args.output.chmod(0o600)
        result: dict[str, Any] = envelope["mapping"]
        if args.accept:
            candidate = prepare_candidate(
                connection, envelope["payload"], source_bytes=args.roster.read_bytes(),
                raw_root=args.output.parent / "raw",
            )
            result = accept_candidate(connection, candidate["candidate_id"])
    summary = {key: result[key] for key in (
        "candidate_id", "snapshot_id", "member_count", "mapped_identity_count",
        "members_sha256", "source_captured_at",
    ) if key in result}
    print(json.dumps({"status": "accepted" if args.accept else "converted", "result": summary}, ensure_ascii=False))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
