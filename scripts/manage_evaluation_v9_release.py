#!/usr/bin/env python3
"""Operate the manifest-bound evaluation-v9 release lifecycle."""

from __future__ import annotations

import argparse
import json
import sys
from pathlib import Path
from typing import Any, Sequence


PROJECT_ROOT = Path(__file__).resolve().parents[1]
PACKAGE_ROOT = PROJECT_ROOT / "src" / "dcar_eval"
if str(PACKAGE_ROOT) not in sys.path:
    sys.path.insert(0, str(PACKAGE_ROOT))

from v8 import release_management_v9 as management  # type: ignore[import-untyped] # noqa: E402


def _common_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(add_help=False)
    parser.add_argument(
        "--db",
        type=Path,
        required=True,
        help="Existing schema-v13 SQLite database; there is no production default.",
    )
    parser.add_argument("--manifest", type=Path, required=True)
    parser.add_argument("--manifest-sha256", required=True)
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
    return parser


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description=__doc__)
    commands = parser.add_subparsers(dest="command", required=True)
    common = _common_parser()
    for command in ("status", "create", "backfill"):
        commands.add_parser(command, parents=[common])
    verify = commands.add_parser("verify-ready", parents=[common])
    verify.add_argument("--receipt-out", type=Path, required=True)
    activate = commands.add_parser("activate", parents=[common])
    activate.add_argument("--receipt", type=Path, required=True)
    activate.add_argument("--receipt-sha256", required=True)
    abort = commands.add_parser("abort", parents=[common])
    abort.add_argument("--reason", required=True)
    rollback = commands.add_parser("rollback-before-resume", parents=[common])
    rollback.add_argument("--receipt", type=Path, required=True)
    rollback.add_argument("--receipt-sha256", required=True)
    rollback.add_argument("--reason", required=True)
    return parser


def _existing(path: Path, *, label: str) -> Path:
    resolved = path.resolve()
    if not resolved.is_file():
        raise management.ReleaseV9Error(f"{label} must be an existing file: {resolved}")
    return resolved


def _dispatch(arguments: argparse.Namespace) -> dict[str, Any]:
    db_path = _existing(arguments.db, label="database")
    management.assert_cli_cutover_guard(
        db_path=db_path,
        freeze_lock=arguments.freeze_lock,
        isolated_clone=bool(arguments.isolated_clone),
    )
    manifest_path = _existing(arguments.manifest, label="manifest")
    common = {
        "db_path": db_path,
        "manifest_path": manifest_path,
        "manifest_sha256": str(arguments.manifest_sha256),
    }
    command = str(arguments.command)
    if command == "status":
        return management.status(**common)
    if command == "create":
        return management.create(**common)
    if command == "backfill":
        return management.backfill(**common)
    if command == "verify-ready":
        return management.verify_ready(
            **common,
            receipt_path=arguments.receipt_out.resolve(),
        )
    if command == "activate":
        return management.activate(
            **common,
            receipt_path=_existing(arguments.receipt, label="receipt"),
            receipt_sha256=str(arguments.receipt_sha256),
        )
    if command == "abort":
        return management.abort(**common, reason=str(arguments.reason))
    if command == "rollback-before-resume":
        return management.rollback_before_resume(
            **common,
            receipt_path=_existing(arguments.receipt, label="receipt"),
            receipt_sha256=str(arguments.receipt_sha256),
            reason=str(arguments.reason),
        )
    raise management.ReleaseV9Error(f"unsupported command: {command}")


def main(argv: Sequence[str] | None = None) -> int:
    arguments = build_parser().parse_args(argv)
    try:
        result = _dispatch(arguments)
    except Exception as error:
        json.dump(
            {
                "command": str(arguments.command),
                "ok": False,
                "error": str(error),
                "error_type": type(error).__name__,
            },
            sys.stderr,
            ensure_ascii=False,
            sort_keys=True,
            separators=(",", ":"),
        )
        sys.stderr.write("\n")
        return 1
    json.dump(
        {"command": str(arguments.command), "ok": True, "result": result},
        sys.stdout,
        ensure_ascii=False,
        sort_keys=True,
        separators=(",", ":"),
        default=str,
    )
    sys.stdout.write("\n")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
