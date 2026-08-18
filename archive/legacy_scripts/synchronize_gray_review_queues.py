#!/usr/bin/env python3
"""Plan or apply the release-bound gray review queue synchronization."""

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

from v8.evaluation import (  # type: ignore[import-untyped] # noqa: E402
    EvaluationError,
    apply_gray_review_queue_sync,
    plan_gray_review_queue_sync,
)


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument(
        "--db",
        type=Path,
        required=True,
        help="Existing SQLite database; there is deliberately no default.",
    )
    parser.add_argument("--apply", action="store_true")
    parser.add_argument("--confirm-plan-sha256")
    return parser


def main(argv: Sequence[str] | None = None) -> int:
    args = build_parser().parse_args(argv)
    db_path = args.db.resolve()
    try:
        if args.apply:
            if not args.confirm_plan_sha256:
                raise EvaluationError("--apply requires --confirm-plan-sha256")
            result = apply_gray_review_queue_sync(
                expected_plan_sha256=args.confirm_plan_sha256,
                db_path=db_path,
            )
            mode = "apply"
        else:
            if args.confirm_plan_sha256:
                raise EvaluationError(
                    "--confirm-plan-sha256 is only valid together with --apply"
                )
            result = plan_gray_review_queue_sync(db_path=db_path)
            mode = "dry-run"
    except (EvaluationError, OSError) as error:
        print(
            json.dumps(
                {
                    "command": "synchronize-gray-review-queues",
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
                "command": "synchronize-gray-review-queues",
                "mode": mode,
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
