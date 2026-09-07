"""TikHub credential-file and API-base configuration.

The macOS writer keeps provider secrets outside the repository and exposes only
the path through ``TIKHUB_API_KEY_FILE``.  The same file also owns the approved
API base so a key cannot silently be paired with a different endpoint.
"""

from __future__ import annotations

import hashlib
import json
import os
import stat
from pathlib import Path
from typing import Any, Mapping
from urllib.parse import urlsplit


DEFAULT_TIKHUB_CONFIG_FILE = Path(
    "/Users/mark/Documents/key/DcarKey/dcar.env.local"
)
DEFAULT_TIKHUB_API_BASE = "https://api.tikhub.io"
ALLOWED_TIKHUB_API_BASES = frozenset(
    {
        "https://api.tikhub.dev",
        "https://api.tikhub.io",
    }
)
TIKHUB_TRANSPORT_CONTRACT_VERSION = "tikhub-request-transport-v1"
TIKHUB_HTTP_STACK = "urllib-stream-v1"
ALLOWED_TIKHUB_HTTP_STACKS = frozenset(
    {
        "urllib-stream-v1",
        "urllib-legacy-v1",
    }
)


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
    path = _configured_file(default_path, honor_environment=honor_environment)
    file_configured = _assignment(path, "TIKHUB_API_BASE")
    direct = (
        os.environ.get("TIKHUB_API_BASE", "").strip()
        if honor_environment
        else ""
    )
    configured = file_configured or direct
    candidate = (configured or DEFAULT_TIKHUB_API_BASE).strip().strip("\"'")
    if candidate not in ALLOWED_TIKHUB_API_BASES:
        raise TikHubConfigurationError(
            "TIKHUB_API_BASE 必须精确为 https://api.tikhub.dev 或 "
            "https://api.tikhub.io"
        )
    return candidate


def load_tikhub_http_stack(
    default_path: Path | None = None,
    *,
    honor_environment: bool = True,
) -> str:
    path = _configured_file(default_path, honor_environment=honor_environment)
    file_configured = _assignment(path, "TIKHUB_HTTP_STACK")
    direct = (
        os.environ.get("TIKHUB_HTTP_STACK", "").strip()
        if honor_environment
        else ""
    )
    candidate = (file_configured or direct or TIKHUB_HTTP_STACK).strip().strip("\"'")
    if candidate not in ALLOWED_TIKHUB_HTTP_STACKS:
        raise TikHubConfigurationError(
            "TIKHUB_HTTP_STACK 必须精确为 urllib-stream-v1 或 urllib-legacy-v1"
        )
    return candidate


def _canonical_manifest_bytes(value: Mapping[str, Any]) -> bytes:
    return json.dumps(
        value,
        ensure_ascii=False,
        sort_keys=True,
        separators=(",", ":"),
        allow_nan=False,
    ).encode("utf-8")


def resolve_tikhub_transport_manifest(
    default_path: Path | None = None,
    *,
    honor_environment: bool = True,
) -> dict[str, Any]:
    """Freeze the selected non-secret TikHub transport route."""

    api_base = load_tikhub_api_base(
        default_path,
        honor_environment=honor_environment,
    )
    http_stack = load_tikhub_http_stack(
        default_path,
        honor_environment=honor_environment,
    )
    parsed = urlsplit(api_base)
    request_host = str(parsed.hostname or "").lower()
    route_base = f"{parsed.scheme.lower()}://{parsed.netloc.lower()}"
    stack_suffix = "stream-v1" if http_stack == "urllib-stream-v1" else "legacy-v1"
    manifest: dict[str, Any] = {
        "contract_version": TIKHUB_TRANSPORT_CONTRACT_VERSION,
        "api_base": api_base,
        "request_host": request_host,
        "transport_route_id": f"tikhub-{request_host}-{stack_suffix}",
        "http_stack": http_stack,
        "route_generation": (
            "route-config-sha256:"
            + hashlib.sha256(route_base.encode("utf-8")).hexdigest()
        ),
    }
    manifest["config_sha256"] = hashlib.sha256(
        _canonical_manifest_bytes(manifest)
    ).hexdigest()
    return manifest


def validate_current_tikhub_transport_manifest(
    manifest: Mapping[str, Any],
    default_path: Path | None = None,
    *,
    honor_environment: bool = True,
) -> dict[str, Any]:
    """Re-read the configured route and reject any frozen-manifest drift."""

    if not isinstance(manifest, Mapping):
        raise TikHubConfigurationError("TikHub transport manifest 格式无效")
    current = resolve_tikhub_transport_manifest(
        default_path,
        honor_environment=honor_environment,
    )
    if dict(manifest) != current:
        raise TikHubConfigurationError("TikHub transport manifest 与当前 route 配置不一致")
    return current
