#!/usr/bin/env python3
"""Render the disabled-by-default Douyin sync SSH tunnel LaunchAgent."""

from __future__ import annotations

import argparse
import os
import plistlib
from pathlib import Path
from xml.sax.saxutils import escape

HERE = Path(__file__).resolve().parent
TEMPLATE = HERE / "cn.tj.dcar.douyin-sync-tunnel.plist.template"
LABEL = "cn.tj.dcar.douyin-sync-tunnel"


def render_plist(project_root: Path, home: Path) -> bytes:
    project_root = project_root.expanduser().resolve()
    home = home.expanduser().resolve()
    text = TEMPLATE.read_text(encoding="utf-8")
    text = text.replace("__PROJECT_ROOT_XML__", escape(str(project_root)))
    text = text.replace("__HOME_XML__", escape(str(home)))
    if "__PROJECT_ROOT_XML__" in text or "__HOME_XML__" in text:
        raise ValueError("Douyin sync tunnel template has unresolved placeholders")
    payload = text.encode("utf-8")
    value = plistlib.loads(payload)
    environment = value.get("EnvironmentVariables", {})
    if value.get("Label") != LABEL or value.get("Disabled") is not True:
        raise ValueError("Douyin sync tunnel must be disabled by default")
    if value.get("RunAtLoad") is not True or value.get("KeepAlive") is not True:
        raise ValueError("Douyin sync tunnel must be a persistent RunAtLoad job")
    if environment.get("DCAR_PROJECT_ROOT") != str(project_root):
        raise ValueError("Douyin sync project root is invalid")
    expected_env = home / "Library/Application Support/DcarAIGC/douyin-sync.env"
    if environment.get("DCAR_DOUYIN_SYNC_ENV_FILE") != str(expected_env):
        raise ValueError("Douyin sync environment path is invalid")
    if any(
        name in str(key).upper()
        for key in environment
        for name in ("MACHINE_KEY", "PASSWORD", "SECRET")
    ):
        raise ValueError("credentials must not be stored in the tunnel plist")
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
    for name in (
        "run_douyin_sync_tunnel.sh",
        "check_douyin_sync_tunnel.sh",
        "start_douyin_sync_tunnel.sh",
        "stop_douyin_sync_tunnel.sh",
    ):
        if not (project_root / "deploy/macos" / name).is_file():
            raise SystemExit(f"missing Douyin sync tunnel script: {name}")
    payload = render_plist(project_root, arguments.home)
    if arguments.check:
        print(f"valid disabled LaunchAgent: {LABEL}")
        return 0
    output = arguments.output.expanduser().resolve()
    output.parent.mkdir(parents=True, exist_ok=True)
    try:
        with output.open("xb") as stream:
            stream.write(payload)
    except FileExistsError as exc:
        raise SystemExit(f"refusing to overwrite existing plist: {output}") from exc
    os.chmod(output, 0o644)
    print(f"rendered disabled LaunchAgent: {output}")
    print("not loaded yet; run start_douyin_sync_tunnel.sh after server setup")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
