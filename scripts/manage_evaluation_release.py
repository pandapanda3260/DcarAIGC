#!/usr/bin/env python3
"""Operate the explicit, freeze-bound evaluation release lifecycle."""

from __future__ import annotations

import argparse
import json
import sys
from pathlib import Path
from typing import Any, Sequence, TextIO


PROJECT_ROOT = Path(__file__).resolve().parents[1]
PACKAGE_ROOT = PROJECT_ROOT / "src" / "dcar_eval"
if str(PACKAGE_ROOT) not in sys.path:
    sys.path.insert(0, str(PACKAGE_ROOT))

from v8 import release_management as management  # type: ignore[import-untyped] # noqa: E402


def _common_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(add_help=False)
    parser.add_argument(
        "--db",
        type=Path,
        required=True,
        help="Existing SQLite database; there is deliberately no production default.",
    )
    parser.add_argument(
        "--manifest",
        type=Path,
        required=True,
        help="Freeze-bundle manifest.json used to bind the operation.",
    )
    return parser


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description=__doc__)
    subcommands = parser.add_subparsers(dest="command", required=True)
    common = _common_parser()

    subcommands.add_parser("status", parents=[common], help="Inspect release state.")
    subcommands.add_parser(
        "create", parents=[common], help="Create the approved draft release."
    )
    subcommands.add_parser(
        "backfill", parents=[common], help="Evaluate the exact frozen inventory."
    )

    verify = subcommands.add_parser(
        "verify-ready",
        parents=[common],
        help="Verify readiness and write an immutable semantic receipt.",
    )
    verify.add_argument(
        "--approved-receipt",
        type=Path,
        action="append",
        default=[],
        help="Approved rehearsal receipt; pass exactly two for production.",
    )
    verify.add_argument(
        "--production",
        action="store_true",
        help="Require and compare exactly two approved rehearsal receipts.",
    )
    verify.add_argument(
        "--receipt-out",
        type=Path,
        required=True,
        help="Destination for the verified receipt.",
    )

    activate = subcommands.add_parser(
        "activate",
        parents=[common],
        help="Activate atomically using an existing production receipt.",
    )
    activate.add_argument(
        "--receipt",
        type=Path,
        required=True,
        help="Production receipt emitted by verify-ready --production.",
    )

    abort = subcommands.add_parser(
        "abort", parents=[common], help="Fail a release before activation."
    )
    abort.add_argument("--reason", required=True, help="Non-empty operator reason.")

    rollback = subcommands.add_parser(
        "rollback-before-resume",
        parents=[common],
        help="Rollback a just-activated release before external work resumes.",
    )
    rollback.add_argument(
        "--receipt",
        type=Path,
        required=True,
        help="Production receipt used for the activation.",
    )
    rollback.add_argument(
        "--reason", required=True, help="Non-empty operator rollback reason."
    )
    return parser


def _require_existing_file(path: Path, *, label: str) -> Path:
    resolved = path.resolve()
    if not resolved.is_file():
        raise management.ReleaseManagementError(
            f"{label} must be an existing file: {resolved}"
        )
    return resolved


def _approved_receipts(paths: Sequence[Path], *, required: bool) -> tuple[Path, ...]:
    if len(paths) not in ({2} if required else {0, 2}):
        requirement = "exactly two" if required else "zero or exactly two"
        raise management.ReleaseManagementError(
            f"approved receipts must contain {requirement} paths"
        )
    resolved = tuple(
        _require_existing_file(path, label="approved receipt") for path in paths
    )
    if len(set(resolved)) != len(resolved):
        raise management.ReleaseManagementError(
            "approved receipts must be two distinct files"
        )
    return resolved


def _receipt_output(path: Path, *, inputs: Sequence[Path]) -> Path:
    resolved = path.resolve()
    if not resolved.parent.is_dir():
        raise management.ReleaseManagementError(
            f"receipt output parent must exist: {resolved.parent}"
        )
    if resolved in {item.resolve() for item in inputs}:
        raise management.ReleaseManagementError(
            "receipt output must differ from every approved receipt"
        )
    return resolved


def _dispatch(arguments: argparse.Namespace) -> dict[str, Any]:
    db_path = _require_existing_file(arguments.db, label="database")
    manifest_path = _require_existing_file(arguments.manifest, label="freeze manifest")
    command = str(arguments.command)
    if command == "status":
        return management.status(db_path=db_path)
    if command == "create":
        return management.create(db_path=db_path, manifest_path=manifest_path)
    if command == "backfill":
        return management.backfill(db_path=db_path, manifest_path=manifest_path)
    if command == "verify-ready":
        approved = _approved_receipts(
            arguments.approved_receipt, required=bool(arguments.production)
        )
        if not arguments.production and approved:
            raise management.ReleaseManagementError(
                "rehearsal verification does not accept approved receipts"
            )
        receipt_out = _receipt_output(arguments.receipt_out, inputs=approved)
        return management.verify_ready(
            db_path=db_path,
            manifest_path=manifest_path,
            receipt_path=receipt_out,
            rehearsal_receipt_paths=approved,
            production=bool(arguments.production),
        )
    if command == "activate":
        receipt_path = _require_existing_file(
            arguments.receipt, label="production receipt"
        )
        return management.activate(
            db_path=db_path,
            manifest_path=manifest_path,
            receipt_path=receipt_path,
        )
    if command == "abort":
        return management.abort(
            db_path=db_path,
            manifest_path=manifest_path,
            reason=str(arguments.reason),
        )
    if command == "rollback-before-resume":
        receipt_path = _require_existing_file(
            arguments.receipt, label="release receipt"
        )
        return management.rollback_before_resume(
            db_path=db_path,
            manifest_path=manifest_path,
            receipt_path=receipt_path,
            reason=str(arguments.reason),
        )
    raise management.ReleaseManagementError(f"unsupported command: {command}")


def _write_json(stream: TextIO, value: Any) -> None:
    json.dump(
        value,
        stream,
        ensure_ascii=False,
        sort_keys=True,
        separators=(",", ":"),
        default=str,
    )
    stream.write("\n")


def main(argv: Sequence[str] | None = None) -> int:
    arguments = build_parser().parse_args(argv)
    try:
        result = _dispatch(arguments)
    except Exception as error:
        _write_json(
            sys.stderr,
            {
                "command": arguments.command,
                "error": str(error),
                "error_type": type(error).__name__,
                "ok": False,
            },
        )
        return 1
    _write_json(
        sys.stdout,
        {"command": arguments.command, "ok": True, "result": result},
    )
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
