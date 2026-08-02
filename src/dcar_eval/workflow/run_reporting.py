from __future__ import annotations

import argparse
import json
from pathlib import Path

from .reporting import (
    DEFAULT_DB,
    DEFAULT_REPORTS_ROOT,
    build_report_revision,
    create_report_run,
    record_provider_usage,
)


def main() -> None:
    parser = argparse.ArgumentParser(description="Generate a versioned v7 report from v5 evaluations.")
    parser.add_argument("--db", type=Path, default=DEFAULT_DB)
    parser.add_argument("--reports-root", type=Path, default=DEFAULT_REPORTS_ROOT)
    parser.add_argument("--run-id")
    parser.add_argument("--provider-usage", type=Path)
    args = parser.parse_args()
    run_id = create_report_run(args.db, args.run_id)
    if args.provider_usage:
        entries = json.loads(args.provider_usage.read_text(encoding="utf-8"))
        record_provider_usage(args.db, run_id, entries)
    report = build_report_revision(args.db, run_id, args.reports_root)
    print(json.dumps({
        "run_id": run_id,
        "revision": report["metadata"]["revision"],
        "output": str(args.reports_root / run_id / "revision_001" / "report.json"),
    }, ensure_ascii=False, indent=2))


if __name__ == "__main__":
    main()
