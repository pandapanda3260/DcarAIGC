#!/usr/bin/env python3
"""Prepare an externally hashed evaluation-v9 release manifest."""

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

from v8 import release_management_v9 as management  # type: ignore[import-untyped] # noqa: E402


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument(
        "--db",
        type=Path,
        required=True,
        help="Existing schema-v13 SQLite database; there is no production default.",
    )
    parser.add_argument(
        "--freeze-lock",
        type=Path,
        default=management.DEFAULT_OPERATOR_FREEZE_LOCK,
        help="Required operator freeze lock when --db is the formal database.",
    )
    parser.add_argument(
        "--isolated-clone",
        action="store_true",
        help="Explicitly identify a non-app/data rehearsal clone.",
    )
    parser.add_argument(
        "--manifest-out",
        type=Path,
        required=True,
        help="New manifest path. Existing files are never overwritten.",
    )
    return parser


def main(argv: Sequence[str] | None = None) -> int:
    arguments = build_parser().parse_args(argv)
    try:
        management.assert_cli_cutover_guard(
            db_path=arguments.db,
            freeze_lock=arguments.freeze_lock,
            isolated_clone=bool(arguments.isolated_clone),
        )
        result = management.prepare_manifest(
            db_path=arguments.db.resolve(),
            manifest_path=arguments.manifest_out.resolve(),
        )
    except Exception as error:
        json.dump(
            {"ok": False, "error": str(error), "error_type": type(error).__name__},
            sys.stderr,
            ensure_ascii=False,
            sort_keys=True,
            separators=(",", ":"),
        )
        sys.stderr.write("\n")
        return 1
    json.dump(
        {"ok": True, "result": result},
        sys.stdout,
        ensure_ascii=False,
        sort_keys=True,
        separators=(",", ":"),
    )
    sys.stdout.write("\n")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
