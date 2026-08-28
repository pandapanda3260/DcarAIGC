#!/usr/bin/env python3
"""Finalize the unfrozen selling-points-v5.3 draft in one transaction."""

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

from v8.selling_point_g0 import materialize_v5_3_draft  # type: ignore[import-not-found] # noqa: E402
from v8.selling_point_label_cards import (  # type: ignore[import-not-found] # noqa: E402
    DEFAULT_LABEL_CARD_PATH,
)
from v8.storage import DEFAULT_DB  # type: ignore[import-not-found] # noqa: E402


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--db", type=Path, default=DEFAULT_DB)
    parser.add_argument("--config", type=Path, default=DEFAULT_LABEL_CARD_PATH)
    parser.add_argument("--receipt", type=Path)
    parser.add_argument("--dry-run", action="store_true")
    return parser


def main(argv: Sequence[str] | None = None) -> int:
    arguments = build_parser().parse_args(argv)
    try:
        result = materialize_v5_3_draft(
            db_path=arguments.db,
            config_path=arguments.config,
            dry_run=bool(arguments.dry_run),
        )
        if arguments.receipt is not None:
            path = arguments.receipt.resolve()
            path.parent.mkdir(parents=True, exist_ok=True)
            with path.open("x", encoding="utf-8") as output:
                json.dump(result, output, ensure_ascii=False, sort_keys=True, indent=2)
                output.write("\n")
    except Exception as error:  # noqa: BLE001 - CLI boundary reports the exact failure
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
