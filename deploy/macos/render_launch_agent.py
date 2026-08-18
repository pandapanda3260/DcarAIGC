#!/usr/bin/env python3
"""Render and validate the disabled macOS writer LaunchAgent template.

This utility only writes a plist. It never calls launchctl or starts a process.
"""

from __future__ import annotations

import argparse
import os
import plistlib
import re
from datetime import date
from pathlib import Path
from xml.sax.saxutils import escape

HERE = Path(__file__).resolve().parent
TEMPLATE = HERE / "cn.tj.dcar.writer-worker.plist.template"
LABEL = "cn.tj.dcar.writer-worker"
RECONCILE_FROM_PATTERN = re.compile(r"[0-9]{4}-[0-9]{2}-[0-9]{2}\Z")


def validate_reconcile_from(value: str) -> str:
    if RECONCILE_FROM_PATTERN.fullmatch(value) is None:
        raise ValueError("reconcile-from must be exactly YYYY-MM-DD")
    try:
        parsed = date.fromisoformat(value)
    except ValueError as exc:
        raise ValueError("reconcile-from must be a valid calendar date") from exc
    if parsed.isoformat() != value:
        raise ValueError("reconcile-from must be exactly YYYY-MM-DD")
    return value


def render_plist(project_root: Path, home: Path, reconcile_from: str) -> bytes:
    project_root = project_root.expanduser().resolve()
    home = home.expanduser().resolve()
    reconcile_from = validate_reconcile_from(reconcile_from)
    text = TEMPLATE.read_text(encoding="utf-8")
    text = text.replace("__PROJECT_ROOT_XML__", escape(str(project_root)))
    text = text.replace("__HOME_XML__", escape(str(home)))
    text = text.replace("__RECONCILE_FROM_XML__", escape(reconcile_from))
    if any(
        placeholder in text
        for placeholder in (
            "__PROJECT_ROOT_XML__",
            "__HOME_XML__",
            "__RECONCILE_FROM_XML__",
        )
    ):
        raise ValueError("LaunchAgent template contains unresolved placeholders")
    payload = text.encode("utf-8")
    value = plistlib.loads(payload)
    environment = value.get("EnvironmentVariables", {})
    expected = {
        "DCAR_READ_ONLY": "0",
        "DCAR_SCHEDULER_ENABLED": "1",
        "DCAR_STARTUP_CATCHUP_ENABLED": "1",
        "DCAR_DAILY_CAPTURE_RECONCILE_FROM": reconcile_from,
        "DCAR_WORKER_HOST": "127.0.0.1",
        "DCAR_WORKER_PORT": "8766",
    }
    if value.get("Label") != LABEL or value.get("Disabled") is not True:
        raise ValueError("LaunchAgent label or disabled-by-default gate is invalid")
    if any(environment.get(key) != expected_value for key, expected_value in expected.items()):
        raise ValueError("LaunchAgent writer environment is invalid")
    expected_lock = home / "Library/Application Support/DcarAIGC/runtime/writer-worker.lock"
    if environment.get("DCAR_WRITER_LOCK") != str(expected_lock):
        raise ValueError("LaunchAgent writer lock must be outside the repository")
    if any("API_KEY" in str(key) for key in environment):
        raise ValueError("provider credential keys must not be stored in the plist")
    return payload


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--project-root", type=Path, required=True)
    parser.add_argument("--home", type=Path, default=Path.home())
    parser.add_argument("--reconcile-from", required=True)
    destination = parser.add_mutually_exclusive_group(required=True)
    destination.add_argument("--check", action="store_true")
    destination.add_argument("--output", type=Path)
    return parser.parse_args()


def main() -> int:
    args = parse_args()
    project_root = args.project_root.expanduser().resolve()
    wrapper = project_root / "deploy" / "macos" / "run_writer_worker.sh"
    if not wrapper.is_file():
        raise SystemExit(f"worker wrapper is missing: {wrapper}")
    try:
        payload = render_plist(project_root, args.home, args.reconcile_from)
    except ValueError as exc:
        raise SystemExit(str(exc)) from exc
    if args.check:
        print(f"valid disabled LaunchAgent: {LABEL}")
        return 0
    output = args.output.expanduser().resolve()
    output.parent.mkdir(parents=True, exist_ok=True)
    try:
        with output.open("xb") as stream:
            stream.write(payload)
    except FileExistsError as exc:
        raise SystemExit(f"refusing to overwrite existing plist: {output}") from exc
    os.chmod(output, 0o644)
    print(f"rendered disabled LaunchAgent: {output}")
    print("not loaded; review the plist and external writer.env before launchctl")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
