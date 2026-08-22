#!/usr/bin/env python3
"""Render the automatic fail-closed snapshot-publisher LaunchAgent template."""

from __future__ import annotations

import argparse
import os
import plistlib
from pathlib import Path
from xml.sax.saxutils import escape

HERE = Path(__file__).resolve().parent
TEMPLATE = HERE / "cn.tj.dcar.snapshot-publisher.plist.template"
LABEL = "cn.tj.dcar.snapshot-publisher"


def render_plist(project_root: Path, home: Path) -> bytes:
    project_root = project_root.expanduser().resolve()
    home = home.expanduser().resolve()
    text = TEMPLATE.read_text(encoding="utf-8")
    text = text.replace("__PROJECT_ROOT_XML__", escape(str(project_root)))
    text = text.replace("__HOME_XML__", escape(str(home)))
    if "__PROJECT_ROOT_XML__" in text or "__HOME_XML__" in text:
        raise ValueError("snapshot publisher template has unresolved placeholders")
    payload = text.encode("utf-8")
    value = plistlib.loads(payload)
    environment = value.get("EnvironmentVariables", {})
    schedule = value.get("StartCalendarInterval")
    expected = {
        "DCAR_READ_ONLY": "1",
        "DCAR_SCHEDULER_ENABLED": "0",
        "DCAR_STARTUP_CATCHUP_ENABLED": "0",
    }
    if value.get("Label") != LABEL or value.get("Disabled") is not False:
        raise ValueError("snapshot publisher label or enabled state is invalid")
    if schedule != {"Hour": 9, "Minute": 0}:
        raise ValueError("snapshot publisher must start its daily cycle at 09:00")
    if value.get("StartInterval") != 3600:
        raise ValueError("snapshot publisher must reconcile once per hour")
    if value.get("RunAtLoad") is not True or "KeepAlive" in value:
        raise ValueError(
            "snapshot publisher must run at load without becoming a keep-alive job"
        )
    if any(
        environment.get(key) != expected_value
        for key, expected_value in expected.items()
    ):
        raise ValueError("snapshot publisher safety environment is invalid")
    if any("API_KEY" in str(key) or "PASSWORD" in str(key) for key in environment):
        raise ValueError("credentials must not be stored in the publisher plist")
    return payload


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--project-root", type=Path, required=True)
    parser.add_argument("--home", type=Path, default=Path.home())
    destination = parser.add_mutually_exclusive_group(required=True)
    destination.add_argument("--check", action="store_true")
    destination.add_argument("--output", type=Path)
    return parser.parse_args()


def main() -> int:
    arguments = parse_args()
    project_root = arguments.project_root.expanduser().resolve()
    wrapper = project_root / "deploy/macos/run_snapshot_publisher.sh"
    publisher = project_root / "deploy/macos/publish_snapshot.py"
    if not wrapper.is_file() or not publisher.is_file():
        raise SystemExit("snapshot publisher wrapper or implementation is missing")
    payload = render_plist(project_root, arguments.home)
    if arguments.check:
        print(f"valid automatic LaunchAgent: {LABEL} from 09:00 with hourly reconcile")
        return 0
    output = arguments.output.expanduser().resolve()
    output.parent.mkdir(parents=True, exist_ok=True)
    try:
        with output.open("xb") as stream:
            stream.write(payload)
    except FileExistsError as exc:
        raise SystemExit(f"refusing to overwrite existing plist: {output}") from exc
    os.chmod(output, 0o644)
    print(f"rendered automatic LaunchAgent: {output}")
    print("not loaded yet; run launchctl bootstrap after the fail-closed preflight")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
