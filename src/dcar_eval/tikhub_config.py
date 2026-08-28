"""TikHub credential-file and API-base configuration.

The macOS writer keeps provider secrets outside the repository and exposes only
the path through ``TIKHUB_API_KEY_FILE``.  The same file also owns the approved
API base so a key cannot silently be paired with a different endpoint.
"""

from __future__ import annotations

import os
import stat
import urllib.parse
from pathlib import Path


DEFAULT_TIKHUB_CONFIG_FILE = Path(
    "/Users/mark/Documents/key/DcarKey/dcar.env.local"
)
DEFAULT_TIKHUB_API_BASE = "https://api.tikhub.io"


class TikHubConfigurationError(RuntimeError):
    pass


def _configured_file(
    default_path: Path | None = None,
    *,
    honor_environment: bool,
) -> Path:
    path = default_path or DEFAULT_TIKHUB_CONFIG_FILE
    if honor_environment:
        configured = os.environ.get("TIKHUB_API_KEY_FILE", "").strip()
        if configured:
            path = Path(configured).expanduser()
    return path


def _safe_config_file(path: Path) -> bool:
    if path.is_symlink():
        raise TikHubConfigurationError(f"TikHub 配置文件不得是符号链接：{path}")
    if not path.exists():
        return False
    if not path.is_file():
        raise TikHubConfigurationError(f"TikHub 配置路径不是普通文件：{path}")
    mode = stat.S_IMODE(path.stat().st_mode)
    if mode not in (0o400, 0o600):
        raise TikHubConfigurationError(
            f"TikHub 配置文件权限必须是 0400 或 0600：{path}"
        )
    return True


def _assignment(path: Path, variable: str) -> str:
    if not _safe_config_file(path):
        return ""
    matches: list[str] = []
    for raw_line in path.read_text(encoding="utf-8-sig").splitlines():
        line = raw_line.strip()
        if not line or line.startswith("#"):
            continue
        if line.startswith("export "):
            line = line.removeprefix("export ").lstrip()
        if "=" not in line:
            continue
        name, value = line.split("=", 1)
        if name.strip() == variable:
            matches.append(value.strip().strip("\"'"))
    if len(matches) > 1:
        raise TikHubConfigurationError(f"{variable} 在 {path} 中重复配置")
    return matches[0] if matches else ""


def load_tikhub_api_key(
    default_path: Path | None = None,
    *,
    honor_environment: bool = True,
) -> str:
    if honor_environment:
        direct = os.environ.get("TIKHUB_API_KEY", "").strip()
        if direct:
            return direct
    path = _configured_file(default_path, honor_environment=honor_environment)
    if not _safe_config_file(path):
        raise TikHubConfigurationError(f"供应商凭据文件不存在：{path}")
    secret = _assignment(path, "TIKHUB_API_KEY")
    if secret:
        return secret

    # Preserve compatibility with a one-line, key-only credential file.
    meaningful = [
        line.strip()
        for line in path.read_text(encoding="utf-8-sig").splitlines()
        if line.strip() and not line.lstrip().startswith("#")
    ]
    if len(meaningful) == 1 and "=" not in meaningful[0]:
        return meaningful[0].strip("\"'")
    raise TikHubConfigurationError("TIKHUB_API_KEY 未配置")


def load_tikhub_api_base(
    default_path: Path | None = None,
    *,
    honor_environment: bool = True,
) -> str:
    direct = (
        os.environ.get("TIKHUB_API_BASE", "").strip()
        if honor_environment
        else ""
    )
    path = _configured_file(default_path, honor_environment=honor_environment)
    configured = direct or _assignment(path, "TIKHUB_API_BASE")
    candidate = (configured or DEFAULT_TIKHUB_API_BASE).strip().strip("\"'")
    candidate = candidate.rstrip("/")
    try:
        parsed = urllib.parse.urlsplit(candidate)
        port = parsed.port
    except ValueError as error:
        raise TikHubConfigurationError("TIKHUB_API_BASE 不是合法 URL") from error
    if (
        parsed.scheme.lower() != "https"
        or (parsed.hostname or "").lower() != "api.tikhub.io"
        or port not in (None, 443)
        or parsed.username is not None
        or parsed.password is not None
        or parsed.path not in ("", "/")
        or parsed.query
        or parsed.fragment
    ):
        raise TikHubConfigurationError(
            "TIKHUB_API_BASE 必须是 https://api.tikhub.io"
        )
    return DEFAULT_TIKHUB_API_BASE
