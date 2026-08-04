#!/usr/bin/env python3
"""Normalize the legacy content-direction sentinel in one explicit database."""

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

from v8.operations import (  # type: ignore[import-untyped] # noqa: E402
    OperationError,
    normalize_unknown_content_directions,
)


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument(
        "--db",
        type=Path,
        required=True,
        help="Existing SQLite database; there is deliberately no default.",
    )
    return parser


def main(argv: Sequence[str] | None = None) -> int:
    args = build_parser().parse_args(argv)
    db_path = args.db.resolve()
    try:
        result = normalize_unknown_content_directions(db_path=db_path)
    except (OperationError, OSError) as error:
        print(
            json.dumps(
                {
                    "command": "normalize-content-directions",
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
            {"command": "normalize-content-directions", "ok": True, "result": result},
            ensure_ascii=False,
            sort_keys=True,
        )
    )
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
