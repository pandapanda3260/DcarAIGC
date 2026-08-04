#!/usr/bin/env python3
"""Invalidate only unsafe automatic reports attested by a freeze manifest."""

from __future__ import annotations

import argparse
import json
import sys
from pathlib import Path
from typing import Sequence


PROJECT_ROOT = Path(__file__).resolve().parents[1]
PACKAGE_ROOT = PROJECT_ROOT / "src" / "dcar_eval"
if str(PACKAGE_ROOT) not in sys.path:
    sys.path.insert(0, str(PACKAGE_ROOT))

from v8.report_repair import (  # type: ignore[import-not-found] # noqa: E402
    ReportRepairError,
    invalidate_unsafe_automatic_reports,
)


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument(
        "--db",
        type=Path,
        required=True,
        help="Existing schema-v9 SQLite database; there is no default.",
    )
    parser.add_argument(
        "--manifest",
        type=Path,
        required=True,
        help="Freeze manifest containing the exact unsafe report targets.",
    )
    parser.add_argument(
        "--receipt",
        type=Path,
        required=True,
        help="Production release receipt that attests the pre-repair rollback boundary.",
    )
    parser.add_argument(
        "--artifact-root",
        type=Path,
        required=True,
        help="Root used to resolve and verify immutable report file paths.",
    )
    parser.add_argument(
        "--apply",
        action="store_true",
        help="Commit the invalidation; without this flag the command is read-only.",
    )
    parser.add_argument(
        "--close-rollback-window",
        action="store_true",
        help="Required with --apply; acknowledges this is the post-activation commit point.",
    )
    return parser


def main(argv: Sequence[str] | None = None) -> int:
    args = build_parser().parse_args(argv)
    try:
        result = invalidate_unsafe_automatic_reports(
            db_path=args.db.resolve(),
            manifest_path=args.manifest.resolve(),
            receipt_path=args.receipt.resolve(),
            artifact_root=args.artifact_root.resolve(),
            apply=bool(args.apply),
            acknowledge_rollback_window_close=bool(args.close_rollback_window),
        )
    except (ReportRepairError, OSError) as error:
        print(
            json.dumps(
                {
                    "command": "invalidate-unsafe-reports",
                    "ok": False,
                    "error": str(error),
                },
                ensure_ascii=False,
                sort_keys=True,
            ),
            file=sys.stderr,
        )
        return 1
    print(
        json.dumps(
            {
                "command": "invalidate-unsafe-reports",
                "ok": True,
                "result": result,
            },
            ensure_ascii=False,
            sort_keys=True,
        )
    )
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
