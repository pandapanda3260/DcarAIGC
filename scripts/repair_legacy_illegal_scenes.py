#!/usr/bin/env python3
"""Repair frozen legacy automatic evaluations with illegal point/scene pairs."""

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

from v8.legacy_scene_repair import (  # type: ignore[import-not-found,import-untyped] # noqa: E402
    LegacySceneRepairError,
    repair_legacy_illegal_scene_chains,
)
from v8.release_management import (  # type: ignore[import-not-found,import-untyped] # noqa: E402
    ReleaseManagementError,
)


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--db", type=Path, required=True)
    parser.add_argument("--manifest", type=Path, required=True)
    parser.add_argument("--receipt", type=Path, required=True)
    parser.add_argument("--reason", required=True)
    parser.add_argument("--expected-plan-sha256")
    parser.add_argument("--apply", action="store_true")
    parser.add_argument("--close-rollback-window", action="store_true")
    return parser


def main(argv: Sequence[str] | None = None) -> int:
    args = build_parser().parse_args(argv)
    if args.close_rollback_window and not args.apply:
        print(
            json.dumps(
                {
                    "command": "repair-legacy-illegal-scenes",
                    "ok": False,
                    "error": "--close-rollback-window requires --apply",
                },
                ensure_ascii=False,
                sort_keys=True,
            ),
            file=sys.stderr,
        )
        return 1
    try:
        result = repair_legacy_illegal_scene_chains(
            db_path=args.db.resolve(),
            manifest_path=args.manifest.resolve(),
            receipt_path=args.receipt.resolve(),
            operator_reason=str(args.reason),
            apply=bool(args.apply),
            expected_plan_sha256=args.expected_plan_sha256,
            acknowledge_rollback_window_close=bool(args.close_rollback_window),
        )
    except (LegacySceneRepairError, ReleaseManagementError, OSError) as error:
        print(
            json.dumps(
                {
                    "command": "repair-legacy-illegal-scenes",
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
                "command": "repair-legacy-illegal-scenes",
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
