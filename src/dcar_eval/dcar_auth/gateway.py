from __future__ import annotations

import asyncio
import hmac
import json
import logging
import os
import posixpath
import re
import sqlite3
from contextlib import asynccontextmanager
from dataclasses import dataclass
from functools import lru_cache
from html import escape
from pathlib import Path
from typing import AsyncIterator, Callable, Mapping, Optional, TypeVar, Union
from urllib.parse import parse_qs, parse_qsl, quote, unquote, urlencode, urlsplit

import httpx
from fastapi import FastAPI, Request
from fastapi.responses import HTMLResponse, JSONResponse, RedirectResponse, StreamingResponse
from starlette.background import BackgroundTask
from starlette.responses import Response

from dcar_auth import store as auth_store
from dcar_auth.store import Principal
from dcar_auth.sms import (
    BAD_NUMBER_CODES,
    RATE_LIMIT_CODES,
    LogSmsSender,
    TencentSmsSender,
    load_sms_credentials,
)


LOGGER = logging.getLogger("dcar-auth")
PROJECT_ROOT = Path(__file__).resolve().parents[3]
SESSION_COOKIE = "dcar_session"
BYPASS_USERNAME = "temporary-bypass"
DEFAULT_SESSION_SECONDS = 12 * 60 * 60
REMEMBER_SESSION_SECONDS = 30 * 24 * 60 * 60
MAX_LOGIN_BODY_BYTES = 16 * 1024
MAX_DOUYIN_BODY_BYTES = 64 * 1024
MAX_CONCURRENT_PASSWORD_WORK = 4
_T = TypeVar("_T")
FORM_CONTENT_TYPE = "application/x-www-form-urlencoded"
SMS_PROVIDERS = frozenset({"tencent", "log"})
AUTH_MARKERS = {
    "/auth/code": "code",
    "/auth/login/code": "login-code",
    "/auth/register": "register",
    "/auth/reset/verify": "reset-verify",
    "/auth/reset/confirm": "reset-confirm",
}
# Error copy for the new endpoints; the login page shows ``detail`` verbatim.
ERROR_COPY = {
    "invalid_phone": "手机号格式不正确",
    "invalid_purpose": "请求无效",
    "invalid_username": "账号为 4–32 位字母、数字或下划线",
    "invalid_password": "密码长度需为 8–64 位",
    "password_too_common": "密码过于常见，请换一个",
    "invalid_code": "验证码不正确或已失效",
    "phone_not_allowed": "该手机号未获授权",
    "phone_not_registered": "该手机号未注册",
    "phone_registered": "该手机号已注册",
    "username_taken": "该账号已被使用",
    "account_disabled": "该账号已停用",
    "rate_limited": "发送太频繁，请稍后再试",
    "too_many_attempts": "尝试次数太多，请稍后再登录",
    "sms_failed": "验证码发送失败，请稍后重试",
    "reset_expired": "验证已失效，请重新获取验证码",
    "origin_mismatch": "登录页面已失效，请刷新后重新登录",
    "unsupported_media_type": "请求格式无效，请刷新页面后重试",
    "length_required": "请求长度缺失",
    "payload_too_large": "登录信息太长，请刷新页面后重新输入",
    "service_unavailable": "暂时无法登录，请稍后重试",
}
# User management: the page prefix and the three endpoints are only open to
# admin / superadmin; endpoint errors are {"detail": 中文, "code": snake_case}.
USER_MANAGEMENT_PAGE_PREFIX = "/users"
USER_LIST_PATH = "/auth/users"
USER_POST_ACTIONS = {
    "/auth/users/update": "user-update",
    "/auth/users/delete": "user-delete",
}
USER_UPDATE_FIELDS = frozenset({"username", "phone", "role", "password"})
USER_DELETE_FIELDS = frozenset({"username"})
PENDING_APPROVAL_PATH = "/pending-approval"
# Account administration includes its roster and authorization interfaces.
# The OAuth callback remains governed by its existing session/state checks.
ACCOUNT_ADMIN_API_PREFIXES = (
    "/api/v8/accounts", "/api/v8/account-roster",
    "/workbench-api/accounts", "/workbench-api/account-roster",
    "/api/douyin/accounts", "/api/douyin/authorizations",
    "/api/douyin/authorization-statuses", "/api/douyin/oauth/start",
)
# Only these document routes expose the data-free workbench to unapproved users.
NEW_USER_PAGES = {
    "/overview": ("概览", "数据概览", "多渠道内容运营核心指标总览与场景分析"),
    "/contents": ("内容", "内容数据", "汇总各平台内容，查看发布与传播表现"),
    "/selling-points": ("卖点", "卖点标准", "查看内容评估的标签定义与分级规则"),
    "/spu-audience": ("SPU人群", "SPU人群", "了解车型、人群与场景的识别规则及数据表现"),
    "/tasks": ("任务", "数据报告任务", "查看数据报告的生成进度与历史记录"),
}
USER_ERROR_RESPONSES: dict[str, tuple[int, str]] = {
    "invalid_payload": (400, "请求格式不正确"),
    "role_invalid": (400, "权限等级无效"),
    "phone_invalid": (400, "手机号格式不正确"),
    "invalid_password": (400, ERROR_COPY["invalid_password"]),
    "password_too_common": (400, ERROR_COPY["password_too_common"]),
    "self_role_change": (400, "不能修改自己的权限等级"),
    "self_password_change": (400, "请通过找回密码修改自己的密码"),
    "self_delete": (400, "不能删除自己的账号"),
    "last_superadmin": (400, "至少保留一个超级管理员"),
    "session_revoked": (401, "登录已失效，请重新登录"),
    "forbidden": (403, "没有权限执行此操作"),
    "target_forbidden": (403, "无权修改该用户"),
    "target_forbidden_delete": (403, "无权删除该用户"),
    "role_forbidden": (403, "无权授予该权限等级"),
    "origin_mismatch": (403, ERROR_COPY["origin_mismatch"]),
    "user_not_found": (404, "用户不存在"),
    "phone_conflict": (409, "该手机号已被其他账号使用"),
    "length_required": (411, ERROR_COPY["length_required"]),
    "payload_too_large": (413, "请求内容过大"),
    "unsupported_media_type": (415, "请求格式不正确"),
    "storage_unavailable": (503, "暂时无法处理，请稍后重试"),
    "too_many_attempts": (429, ERROR_COPY["too_many_attempts"]),
}
PROXY_METHODS = ["GET", "HEAD", "POST", "PUT", "PATCH", "DELETE", "OPTIONS"]
DOUYIN_CALLBACK_PATH = "/oauth/douyin/callback"
DOUYIN_POST_ACTIONS = {
    "/api/douyin/oauth/start": "douyin-oauth-start",
    "/api/douyin/authorizations/reauthorize": "douyin-authorization-reauthorize",
    "/api/douyin/authorizations/unbind": "douyin-authorization-unbind",
}
HOP_BY_HOP_HEADERS = frozenset(
    {
        "connection",
        "keep-alive",
        "proxy-authenticate",
        "proxy-authorization",
        "te",
        "trailer",
        "transfer-encoding",
        "upgrade",
    }
)


def _enabled(name: str, default: bool = False) -> bool:
    value = os.environ.get(name)
    if value is None:
        return default
    return value.strip().lower() in {"1", "true", "yes", "on"}


def _normalized_base_path(value: str) -> str:
    value = value.strip()
    if value in {"", "/"}:
        return ""
    if not value.startswith("/") or value.endswith("/") or "\\" in value:
        raise ValueError("DCAR_AUTH_BASE_PATH must start with '/' and must not end with '/'")
    return value


@dataclass(frozen=True)
class AuthGatewayConfig:
    base_path: str
    web_upstream: str
    api_upstream: str
    session_db_path: Path
    login_template_path: Path
    secure_cookie: bool
    bypass_auth: bool = False
    douyin_upstream: Optional[str] = None
    douyin_edge_key_path: Optional[Path] = None
    session_seconds: int = DEFAULT_SESSION_SECONDS
    remember_session_seconds: int = REMEMBER_SESSION_SECONDS
    throttle_window_seconds: int = 10 * 60
    throttle_max_failures: int = 8
    failure_delay_seconds: float = 0.35
    sms_provider: str = "log"
    sms_credentials_path: Optional[Path] = None
    pepper_path: Optional[Path] = None
    sms_daily_cap: int = 300
    # Append-only security change log next to the account store (outside of
    # it, so restoring a backup never rewinds it).
    change_log_path: Optional[Path] = None
    # Trusted build output; missing configuration keeps every response no-store.
    static_asset_manifest_path: Optional[Path] = None

    def __post_init__(self) -> None:
        object.__setattr__(self, "base_path", _normalized_base_path(self.base_path))
        if self.change_log_path is None:
            object.__setattr__(
                self,
                "change_log_path",
                self.session_db_path.parent / auth_store.CHANGE_LOG_FILENAME,
            )
        if self.sms_provider not in SMS_PROVIDERS:
            raise ValueError("sms_provider must be tencent or log")
        if not self.bypass_auth:
            if self.sms_provider == "log" and self.secure_cookie:
                raise ValueError(
                    "the log SMS provider is only allowed with DCAR_AUTH_SECURE_COOKIE=0"
                )
            if self.sms_provider == "tencent" and self.sms_credentials_path is None:
                raise ValueError("sms_credentials_path is required for the tencent provider")
            if self.pepper_path is None:
                raise ValueError("pepper_path is required unless bypass_auth is enabled")
        if not 1 <= self.sms_daily_cap <= 100_000:
            raise ValueError("sms_daily_cap must be between 1 and 100000")
        for name, value in {
            "web_upstream": self.web_upstream,
            "api_upstream": self.api_upstream,
        }.items():
            parsed = urlsplit(value)
            if parsed.scheme not in {"http", "https"} or not parsed.netloc:
                raise ValueError(f"{name} must be an absolute HTTP URL")
        if self.douyin_upstream:
            normalized_douyin_upstream = self.douyin_upstream.rstrip("/")
            parsed = urlsplit(normalized_douyin_upstream)
            if parsed.scheme not in {"http", "https"} or not parsed.netloc:
                raise ValueError("douyin_upstream must be an absolute HTTP URL")
            if self.douyin_edge_key_path is None:
                raise ValueError(
                    "douyin_edge_key_path is required when douyin_upstream is configured"
                )
            object.__setattr__(self, "douyin_upstream", normalized_douyin_upstream)
        elif self.douyin_edge_key_path is not None:
            raise ValueError(
                "douyin_edge_key_path must not be configured without douyin_upstream"
            )
        if not 60 <= self.session_seconds <= 31 * 24 * 60 * 60:
            raise ValueError("session_seconds must be between 60 seconds and 31 days")
        if not self.session_seconds <= self.remember_session_seconds <= 90 * 24 * 60 * 60:
            raise ValueError("remember_session_seconds must be between session TTL and 90 days")
        if not 1 <= self.throttle_max_failures <= 100:
            raise ValueError("throttle_max_failures must be between 1 and 100")
        if not 60 <= self.throttle_window_seconds <= 24 * 60 * 60:
            raise ValueError("throttle_window_seconds must be between 60 seconds and one day")
        if not 0 <= self.failure_delay_seconds <= 5:
            raise ValueError("failure_delay_seconds must be between 0 and 5 seconds")

    @classmethod
    def from_env(cls) -> "AuthGatewayConfig":
        douyin_upstream = os.environ.get("DCAR_AUTH_DOUYIN_UPSTREAM", "").strip()
        douyin_edge_key_file = os.environ.get(
            "DCAR_AUTH_DOUYIN_EDGE_KEY_FILE", ""
        ).strip()
        credential_root = Path(
            os.environ.get("CREDENTIALS_DIRECTORY", "/run/credentials")
        )
        secure_cookie = _enabled("DCAR_AUTH_SECURE_COOKIE", default=True)
        # Production (secure cookies) defaults to the real channel; the log
        # channel must be requested explicitly and only works locally.
        sms_provider = (
            os.environ.get("DCAR_AUTH_SMS_PROVIDER", "tencent" if secure_cookie else "log")
            .strip()
            .lower()
        )
        sms_credentials_file = os.environ.get(
            "DCAR_AUTH_SMS_CREDENTIALS_FILE", ""
        ).strip()
        pepper_file = os.environ.get("DCAR_AUTH_PEPPER_FILE", "").strip()
        return cls(
            base_path=os.environ.get("DCAR_AUTH_BASE_PATH", ""),
            web_upstream=os.environ.get(
                "DCAR_AUTH_WEB_UPSTREAM", "http://127.0.0.1:4174"
            ).rstrip("/"),
            api_upstream=os.environ.get(
                "DCAR_AUTH_API_UPSTREAM", "http://127.0.0.1:8765"
            ).rstrip("/"),
            session_db_path=Path(
                os.environ.get(
                    "DCAR_AUTH_SESSION_DB",
                    str(PROJECT_ROOT / "runtime" / "auth" / "sessions.sqlite3"),
                )
            ),
            login_template_path=Path(
                os.environ.get(
                    "DCAR_AUTH_LOGIN_TEMPLATE",
                    str(PROJECT_ROOT / "deploy" / "server" / "nginx" / "login.html"),
                )
            ),
            secure_cookie=secure_cookie,
            bypass_auth=_enabled("DCAR_AUTH_BYPASS", default=False),
            douyin_upstream=(douyin_upstream.rstrip("/") if douyin_upstream else None),
            douyin_edge_key_path=(
                Path(douyin_edge_key_file)
                if douyin_edge_key_file
                else credential_root / "douyin-edge-key"
                if douyin_upstream
                else None
            ),
            session_seconds=int(
                os.environ.get("DCAR_AUTH_SESSION_SECONDS", DEFAULT_SESSION_SECONDS)
            ),
            remember_session_seconds=int(
                os.environ.get(
                    "DCAR_AUTH_REMEMBER_SESSION_SECONDS", REMEMBER_SESSION_SECONDS
                )
            ),
            throttle_window_seconds=int(
                os.environ.get("DCAR_AUTH_THROTTLE_WINDOW_SECONDS", 10 * 60)
            ),
            throttle_max_failures=int(
                os.environ.get("DCAR_AUTH_THROTTLE_MAX_FAILURES", 8)
            ),
            failure_delay_seconds=float(
                os.environ.get("DCAR_AUTH_FAILURE_DELAY_SECONDS", 0.35)
            ),
            sms_provider=sms_provider,
            sms_credentials_path=(
                Path(sms_credentials_file)
                if sms_credentials_file
                else credential_root / "sms-tencent"
                if sms_provider == "tencent"
                else None
            ),
            pepper_path=(
                Path(pepper_file) if pepper_file else credential_root / "auth-pepper"
            ),
            sms_daily_cap=int(os.environ.get("DCAR_AUTH_SMS_DAILY_CAP", 300)),
            change_log_path=(
                Path(os.environ["DCAR_AUTH_CHANGE_LOG"])
                if os.environ.get("DCAR_AUTH_CHANGE_LOG", "").strip()
                else None
            ),
            static_asset_manifest_path=(
                Path(os.environ["DCAR_AUTH_STATIC_MANIFEST"])
                if os.environ.get("DCAR_AUTH_STATIC_MANIFEST", "").strip()
                else None
            ),
        )

    @property
    def cookie_path(self) -> str:
        return self.base_path or "/"

    def route(self, path: str) -> str:
        return f"{self.base_path}{path}"


def _stripped_path(path: str, base_path: str) -> Optional[str]:
    if not base_path:
        return path
    if path == base_path:
        return "/"
    if path.startswith(f"{base_path}/"):
        return path[len(base_path) :]
    return None


def _path_has_prefix(path: str, prefix: str) -> bool:
    return path == prefix or path.startswith(f"{prefix}/")


def _account_admin_target(path: str) -> Optional[str]:
    """Classify protected destinations before any HTTPX/ASGI normalization.

    Inspect decoded variants as well as dot/repeated-slash normalization. This
    does not rewrite requests or change authorization for neighboring routes.
    """
    for _ in range(8):
        try:
            forwarded_path = httpx.URL("http://account-guard.invalid" + path).path
        except httpx.InvalidURL:
            forwarded_path = path
        for candidate in (path, forwarded_path):
            normalized = posixpath.normpath("/" + candidate.replace("\\", "/").lstrip("/"))
            if any(_path_has_prefix(normalized, prefix) for prefix in ACCOUNT_ADMIN_API_PREFIXES):
                return normalized
            for page in ("/accounts", "/douyin"):
                if any(_path_has_prefix(normalized, form) for form in (page, page + ".rsc", page + ".json")):
                    return normalized
            # Next data endpoints encode the page after an arbitrary build ID.
            parts = normalized.split("/")
            if (len(parts) >= 5 and parts[1:3] == ["_next", "data"]
                    and parts[4] in {"accounts", "accounts.json", "accounts.rsc", "douyin", "douyin.json"}):
                return normalized
        decoded = unquote(path)
        if decoded == path:
            break
        path = decoded
    return None


def _is_douyin_path(path: str) -> bool:
    return (
        _path_has_prefix(path, "/douyin")
        or _path_has_prefix(path, "/api/douyin")
        or path == DOUYIN_CALLBACK_PATH
    )


def _valid_douyin_callback_navigation(request: Request) -> bool:
    expected_values = {
        "sec-fetch-mode": {"navigate"},
        "sec-fetch-dest": {"document"},
        "sec-fetch-site": {"cross-site", "none"},
    }
    return all(
        header not in request.headers
        or request.headers[header].lower() in allowed
        for header, allowed in expected_values.items()
    )


def _read_edge_key(path: Path) -> str:
    value = path.read_text(encoding="utf-8").strip()
    if not 32 <= len(value) <= 512 or any(ord(character) < 33 for character in value):
        raise RuntimeError("Douyin gateway credential has an invalid format")
    return value


def _web_upstream_path(
    path: str, stripped_path: str, raw_path: bytes
) -> Optional[str]:
    # Vinext keeps page and public-file routes under the configured base path,
    # but serves generated Vite bundles from its root-level /assets directory.
    if stripped_path == "/assets" or stripped_path.startswith("/assets/"):
        segments = stripped_path.split("/")
        if (
            raw_path != path.encode("utf-8")
            or any(segment in {"", ".", ".."} for segment in segments[2:])
            or "\\" in stripped_path
            or "%" in stripped_path
            or any(
                ord(character) < 32 or ord(character) == 127
                for character in stripped_path
            )
        ):
            return None
        return stripped_path
    return path


_HASHED_CODE_ASSET = re.compile(r"assets/[A-Za-z0-9_.-]+-[A-Za-z0-9_-]{8,64}\.(?:js|css)\Z")


@lru_cache(maxsize=4)
def _manifest_code_assets(
    path: Path, identity: tuple[int, int, int, int],
) -> frozenset[str]:
    """Read only published JS/CSS entries, never infer permission from a suffix."""
    del identity  # Cache key changes on atomic build/manifest replacement.
    try:
        manifest = json.loads(path.read_text(encoding="utf-8"))
        if not isinstance(manifest, dict):
            return frozenset()
        client_root = path.parent.parent
        assets: set[str] = set()
        for entry in manifest.values():
            if not isinstance(entry, dict):
                continue
            candidates = [entry.get("file")]
            css = entry.get("css")
            if isinstance(css, list):
                candidates.extend(css)
            for asset in candidates:
                if (isinstance(asset, str) and _HASHED_CODE_ASSET.fullmatch(asset)
                        and (client_root / asset).is_file()
                        and not (client_root / asset).is_symlink()):
                    assets.add("/" + asset)
        return frozenset(assets)
    except (OSError, ValueError):
        return frozenset()


def _is_revalidatable_code_asset(
    request: Request, upstream: httpx.Response, upstream_path: str,
    manifest_path: Optional[Path],
) -> bool:
    """Authentication already ran; only the browser's stored code body is reused."""
    if (manifest_path is None or request.method not in {"GET", "HEAD"}
            or request.url.query or "range" in request.headers
            or upstream.status_code not in {200, 304}
            or not upstream.headers.get("etag") or "set-cookie" in upstream.headers):
        return False
    if upstream.status_code == 200:
        expected = {"text/css"} if upstream_path.endswith(".css") else {
            "application/javascript", "text/javascript", "application/x-javascript",
        }
        if upstream.headers.get("content-type", "").split(";", 1)[0].strip().lower() not in expected:
            return False
    try:
        state = manifest_path.stat()
        if not 0 < state.st_size <= 1024 * 1024:
            return False
        identity = (state.st_dev, state.st_ino, state.st_mtime_ns, state.st_size)
        return upstream_path in _manifest_code_assets(manifest_path, identity)
    except OSError:
        return False


def _safe_return_to(value: str, config: AuthGatewayConfig) -> str:
    fallback = config.route("/overview")
    if not value or not value.startswith("/") or value.startswith("//"):
        return fallback
    decoded = value
    for _ in range(3):
        updated = unquote(decoded)
        if updated == decoded:
            break
        decoded = updated
    if "\\" in decoded or any(ord(character) < 32 for character in decoded):
        return fallback
    parsed = urlsplit(decoded)
    if parsed.scheme or parsed.netloc or parsed.fragment or parsed.path.startswith("//"):
        return fallback
    if any(segment in {".", ".."} for segment in parsed.path.split("/")):
        return fallback
    stripped = _stripped_path(parsed.path, config.base_path)
    if stripped is None or stripped == "/login" or stripped.startswith("/auth/"):
        return fallback
    return decoded


def _request_target(request: Request) -> str:
    target = request.url.path
    if request.url.query:
        target = f"{target}?{request.url.query}"
    return target


def _is_page_navigation(request: Request, stripped: str) -> bool:
    """Only document navigations may receive an HTML redirect after denial."""
    if request.method not in {"GET", "HEAD"}:
        return False
    if any(
        _path_has_prefix(stripped, prefix)
        for prefix in ("/api", "/assets", "/_next", "/__vinext", "/media", "/exports", "/export")
    ) or any("." in segment for segment in stripped.split("/")):
        return False
    # Vinext fetches .rsc files; also reject Next-style Flight/data requests.
    if any(
        header in request.headers
        for header in ("rsc", "next-router-state-tree", "next-router-prefetch", "next-action")
    ) or any(key in request.query_params for key in ("_rsc", "_data")):
        return False
    accept = request.headers.get("accept", "").lower()
    if "text/x-component" in accept:
        return False
    fetch_dest = request.headers.get("sec-fetch-dest", "").lower()
    fetch_mode = request.headers.get("sec-fetch-mode", "").lower()
    if fetch_dest and fetch_dest != "document":
        return False
    if fetch_mode and fetch_mode != "navigate":
        return False
    return "text/html" in accept or fetch_dest == "document"


def _new_user_workbench_html(base_path: str, page_path: str) -> str:
    # A real navigation shell without React/Flight payloads or business queries.
    # Keep it self-contained in the gateway release, including the existing logo.
    icons = (
        '<rect x="3" y="3" width="6" height="6" rx="1.5"/><rect x="11" y="3" width="6" height="4" rx="1.5"/><rect x="3" y="11" width="6" height="6" rx="1.5"/><rect x="11" y="9" width="6" height="8" rx="1.5"/>',
        '<path d="M5 2.75h6l4 4V16a1.5 1.5 0 0 1-1.5 1.5h-7A1.5 1.5 0 0 1 5 16V2.75Z"/><path d="M11 2.75V7h4M7.5 10.5h5M7.5 14h4"/>',
        '<path d="M3.5 9.25V5.5a2 2 0 0 1 2-2h3.75l7.1 7.1a1.75 1.75 0 0 1 0 2.48l-3.27 3.27a1.75 1.75 0 0 1-2.48 0L3.5 9.25Z"/><circle cx="7" cy="7" r="1"/>',
        '<circle cx="7" cy="6.5" r="2.4"/><circle cx="13.5" cy="8" r="1.9"/><path d="M3.2 16.5c.5-2.7 2-4.1 3.8-4.1s3.3 1.4 3.8 4.1M12.4 15.2c.4-1.9 1.5-3 2.9-3 .9 0 1.7.5 2.2 1.4"/>',
        '<path d="M6.5 4h-1A1.5 1.5 0 0 0 4 5.5v10A1.5 1.5 0 0 0 5.5 17h9a1.5 1.5 0 0 0 1.5-1.5v-10A1.5 1.5 0 0 0 14.5 4h-1"/><rect x="7" y="2.5" width="6" height="3" rx="1.2"/><path d="m7 11 2 2 4-4"/>',
    )
    navigation = "".join(
        f'<a href="{escape(base_path + path, quote=True)}"'
        + (' class="active" aria-current="page"' if path == page_path else '')
        + f'><svg viewBox="0 0 20 20" aria-hidden="true">{icon}</svg><span>{copy[0]}</span></a>'
        for (path, copy), icon in zip(NEW_USER_PAGES.items(), icons)
    )
    _, title, description = NEW_USER_PAGES[page_path]
    return """<!doctype html>
<html lang="zh-CN"><head><meta charset="utf-8">
<meta name="viewport" content="width=device-width,initial-scale=1">
<title>__TITLE__ · DCar Insight</title>
<style>
:root{color-scheme:light;--ink:#13262d;--muted:#68777d;--line:#dfe6e8;--navy:#102c35;--brand:#ffcd32}
*{box-sizing:border-box}body{margin:0;background:#f3f6f6;color:var(--ink);font-family:Arial,"PingFang SC","Microsoft YaHei",sans-serif}
button,a{-webkit-tap-highlight-color:transparent}button{font:inherit;cursor:pointer}a,button{touch-action:manipulation}
a:focus-visible,button:focus-visible{outline:3px solid #c88700;outline-offset:4px}
.shell{min-height:100vh;display:grid;grid-template-columns:236px minmax(0,1fr)}
.sidebar{position:sticky;top:0;height:100vh;padding:28px 18px 20px;background:var(--navy);color:#fff;display:flex;flex-direction:column}
.brand{display:flex;align-items:center;gap:12px;padding:0 8px 34px}.brand-logo{width:38px;height:38px;flex:none}
.brand strong,.brand small{display:block}.brand strong{font-size:15px;letter-spacing:.2px}.brand small{margin-top:5px;color:#9bb0b6;font-size:12px}
.nav-label{margin:0 12px 12px;color:#9bb0b6;font-size:12px;font-weight:600}
nav a{display:flex;align-items:center;gap:12px;padding:12px 13px;margin-bottom:6px;border-radius:10px;color:#b5c7cc;text-decoration:none;font-size:14px;font-weight:600;transition:background .15s,color .15s}
nav svg{width:20px;height:20px;flex:none;fill:none;stroke:currentColor;stroke-width:1.8;stroke-linecap:round;stroke-linejoin:round}
nav a:hover{background:#173b45;color:#fff}nav a.active{background:var(--brand);color:#1f2129}
.sidebar-foot{margin-top:auto;border-top:1px solid #ffffff1a;padding:20px 8px 0;display:flex;align-items:center;gap:10px}
.avatar{width:34px;height:34px;border-radius:50%;background:#26434b;color:#d4e1e4;display:grid;place-items:center;font-size:14px}
.account{flex:1}.account strong{display:block;font-size:14px;font-weight:500}.account small{display:block;margin-top:4px;color:#a6bac0;font-size:12px}
#logout{border:0;border-radius:8px;padding:8px;background:transparent;color:#a6bac0}#logout:hover{color:#fff;background:#173b45}#logout svg{display:block;width:19px;height:19px}
main{min-width:0;padding:0 32px 36px}.page-header{max-width:1600px;margin:auto;padding:28px 0 24px;display:flex;justify-content:space-between;align-items:center;gap:20px}
.eyebrow{display:block;margin-bottom:8px;color:#60717a;font-size:12px}h1{margin:0;font-size:28px;line-height:36px;font-weight:600}
.description{margin:8px 0 0;color:#60717a;font-size:14px;line-height:24px}.badge{flex:none;display:flex;align-items:center;gap:7px;padding:8px 12px;border:1px solid #e2e7e7;border-radius:8px;background:#fff;color:#68777d;font-size:13px}
.badge i{width:6px;height:6px;border-radius:50%;background:#bd8722}
.workspace{max-width:1600px;margin:auto}.welcome{display:flex;align-items:center;gap:12px;padding:15px 20px;border:1px solid #eee3bf;border-radius:12px;background:#fffbee;font-size:14px;line-height:24px}
.welcome svg{width:20px;height:20px;flex:none;color:#967126}.welcome strong{font-weight:600}.welcome span{margin-left:12px;color:#766c51}
.empty-panel{margin-top:20px;min-height:520px;min-height:clamp(420px,65vh,680px);padding:52px 24px 36px;border:1px solid var(--line);border-radius:16px;background:#fff;display:flex;flex-direction:column;align-items:center;justify-content:center;text-align:center;box-shadow:0 4px 18px #102c3503}
.lock-icon{width:64px;height:64px;display:grid;place-items:center;background:#fff7d8;border:1px solid #f8e9ae;border-radius:18px;color:#967126;margin-bottom:24px}.lock-icon svg{width:28px;height:28px}
h2{margin:0;font-size:24px;font-weight:600;line-height:1.5;letter-spacing:.2px}.empty-copy{max-width:420px;margin:12px 0 26px;color:var(--muted);font-size:16px;line-height:1.9}
#refresh{display:inline-flex;align-items:center;justify-content:center;gap:8px;min-height:44px;padding:10px 20px;border:1px solid var(--navy);border-radius:9px;background:var(--navy);color:#fff;font-size:14px;font-weight:600;transition:background .15s}#refresh:hover{background:#224650}#refresh svg{width:16px;height:16px}
button:disabled{opacity:.6;cursor:wait}#status{min-height:24px;max-width:440px;margin:16px 0 0;color:#68777d;font-size:14px;line-height:24px}
.auto-note{margin:28px 0 0;display:flex;gap:7px;align-items:center;color:#7c898e;font-size:13px}.auto-note svg{width:14px;height:14px}
.sr-only{position:absolute;width:1px;height:1px;padding:0;margin:-1px;overflow:hidden;clip:rect(0,0,0,0);white-space:nowrap;border:0}
@media(max-width:1000px){.shell{grid-template-columns:212px minmax(0,1fr)}.sidebar{padding-inline:12px}main{padding-inline:24px}.welcome span{display:block;margin-left:0}.page-header{align-items:flex-start}.badge{margin-top:22px}}
@media(max-width:700px){.shell{display:block}.sidebar{position:static;height:auto;padding:20px 16px 12px}.brand{padding:0 4px 20px}.sidebar-foot{position:absolute;right:20px;top:23px;padding:0;border:0}.avatar,.account{display:none}.nav-label{display:none}nav{display:grid;grid-template-columns:repeat(3,minmax(0,1fr));gap:6px}nav a{justify-content:center;gap:7px;margin:0;padding:11px 6px;font-size:14px}nav svg{width:18px;height:18px}main{padding:0 18px 24px}.page-header{padding:24px 0 20px}.badge{display:none}h1{font-size:25px}.description{font-size:14px}.welcome{padding:13px 15px;align-items:flex-start}.empty-panel{margin-top:16px;min-height:440px;padding:40px 20px 28px}h2{font-size:22px}.empty-copy{font-size:16px}.auto-note{font-size:12px}}
@media(prefers-reduced-motion:reduce){*{transition:none!important}}
</style></head><body>
<div class="shell"><aside class="sidebar">
<div class="brand">__BRAND_LOGO__<div><strong>Dcar AIGC</strong><small>开心瓦瓦·运营工作台</small></div></div>
<p class="nav-label">AIGC数据统计</p><nav aria-label="主导航">__NAVIGATION__</nav>
<div class="sidebar-foot"><div class="avatar" aria-hidden="true">新</div><div class="account"><strong>新用户</strong><small>已登录</small></div>
<button id="logout" type="button" title="退出登录" aria-label="退出登录"><svg viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="1.7" stroke-linecap="round" stroke-linejoin="round" aria-hidden="true"><path d="M9 4H5a1 1 0 0 0-1 1v14a1 1 0 0 0 1 1h4M14 8l4 4-4 4M9 12h10"/></svg></button></div></aside>
<main class="main-area" data-section="__SECTION__" data-access="pending">
<header class="page-header"><div><span class="eyebrow">AIGC 数据统计</span><h1>__TITLE__</h1><p class="description">__DESCRIPTION__</p></div><span class="badge"><i aria-hidden="true"></i>待开通权限</span></header>
<div class="workspace"><div class="welcome"><svg viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="1.7" stroke-linecap="round" aria-hidden="true"><circle cx="12" cy="12" r="9"/><path d="m8 12 3 3 5-6"/></svg><div><strong>欢迎来到工作台</strong><span>账号已登录，可以先浏览各个功能页面。</span></div></div>
<section class="empty-panel" aria-labelledby="access-title">
<div class="lock-icon"><svg viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="1.6" stroke-linecap="round" stroke-linejoin="round" aria-hidden="true"><rect x="5" y="10" width="14" height="11" rx="3"/><path d="M8 10V7a4 4 0 0 1 8 0v3M12 14v3"/></svg></div>
<h2 id="access-title">当前内容尚未开通</h2><p class="empty-copy">联系管理员开通权限后，<br>即可查看这里的__TITLE__。</p>
<button id="refresh" type="button"><svg viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="1.8" stroke-linecap="round" stroke-linejoin="round" aria-hidden="true"><path d="M20 7v5h-5M4 17v-5h5"/><path d="M6.1 7a7 7 0 0 1 11.6-1L20 9M4 15l2.3 3A7 7 0 0 0 18 17"/></svg><span>刷新权限</span></button>
<p id="status" role="status" aria-live="polite"></p><p class="auto-note"><svg viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="1.7" stroke-linecap="round" aria-hidden="true"><circle cx="12" cy="12" r="9"/><path d="M12 7v5l3 2"/></svg>权限开通后，本页会自动更新</p>
</section></div></main></div><script>
const basePath=__BASE_PATH_JSON__;
const status=document.getElementById('status');
const refresh=document.getElementById('refresh');
let checking=false;
let leaving=false;
async function checkPermission(manual=false){
if(checking||leaving)return;checking=true;
refresh.disabled=true;refresh.querySelector('span').textContent='正在检查…';
if(manual)status.textContent='';
const controller=new AbortController();const timeout=setTimeout(()=>controller.abort(),10000);
try{
const response=await fetch(basePath+'/auth/session',{cache:'no-store',credentials:'same-origin',signal:controller.signal});
if(leaving)return;
if(response.status===401){leaving=true;location.replace(basePath+'/login?return_to='+encodeURIComponent(location.pathname+location.search));return}
if(!response.ok)throw new Error('session');const session=await response.json();
if(leaving)return;
if(session.authenticated&&['operator','admin','superadmin'].includes(session.role)){leaving=true;location.reload();return}
if(manual)status.textContent='权限还未开通，请联系管理员后再试。';
}catch(error){if(manual&&!leaving)status.textContent='暂时无法检查权限，请稍后重试。';}
finally{clearTimeout(timeout);checking=false;refresh.disabled=false;refresh.querySelector('span').textContent='刷新权限';}}
refresh.onclick=()=>checkPermission(true);
setInterval(()=>{if(document.visibilityState==='visible')checkPermission();},30000);
window.addEventListener('focus',()=>checkPermission());
document.addEventListener('visibilitychange',()=>{if(document.visibilityState==='visible')checkPermission();});
document.getElementById('logout').onclick=async function(){
if(leaving)return;leaving=true;this.disabled=true;status.textContent='';
const controller=new AbortController();const timeout=setTimeout(()=>controller.abort(),10000);
try{const response=await fetch(basePath+'/auth/logout',{method:'POST',credentials:'same-origin',signal:controller.signal,
headers:{'X-Dcar-Request':'logout'}});if(!response.ok)throw new Error('logout');location.assign(basePath+'/login');}
catch(error){leaving=false;this.disabled=false;status.textContent='暂时无法退出，请稍后重试。';}
finally{clearTimeout(timeout);}};
</script></body></html>""".replace(
        "__BASE_PATH_JSON__", json.dumps(base_path).replace("<", "\\u003c")
    ).replace("__NAVIGATION__", navigation).replace("__TITLE__", title).replace(
        "__DESCRIPTION__", description
    ).replace("__SECTION__", page_path[1:]).replace("__BRAND_LOGO__", '<svg class="brand-logo" xmlns="http://www.w3.org/2000/svg" viewBox="0 0 100 100" role="img" aria-labelledby="title">\n  <title id="title">懂车帝 App</title>\n  <rect width="100" height="100" rx="23" fill="#FFCD32"/>\n  <path\n    fill="#1F2129"\n    transform="translate(13.474 18.823) scale(2)"\n    d="M18.2686593 14.6178535C14.4475074 14.6128432 10.6889837 15.0545804 7.03149996 15.9313744 8.11287927 10.7157025 12.746527 6.78641265 18.2774273 6.7935009H18.3262772C23.8342138 6.82357201 28.4340424 10.7557845 29.4991384 15.9580958 25.8449948 15.0729513 22.0889762 14.6224462 18.2686593 14.6178535M18.3638541.000444331963C18.3379678.0000133906399 18.3112465.0000133906399 18.2853602.0000133906399 8.21517191-.0120812971.0125549476 8.17049466.000014624874 18.2406829-.00623349602 23.2158628 1.98993426 27.7334397 5.22363416 31.0343605L10.2334683 26.4416298C9.21346455 25.439997 8.38134526 24.2483921 7.78971804 22.9273558 11.1854161 22.0338609 14.6900871 21.5825208 18.2598914 21.5871135 21.8296957 21.5912887 25.3322791 22.0509793 28.7250545 22.9528246 28.1167264 24.3022523 27.2566332 25.5155683 26.2023928 26.5284742L31.1633769 31.1775701C34.469308 27.8753968 36.5197535 23.3160678 36.5260298 18.2853577 36.5381244 8.24147322 28.3985531.0551395789 18.3638541.000444331963"\n  />\n</svg>')


def _same_origin_post(
    request: Request, expected_marker: str, trusted_proxy: bool = False
) -> bool:
    if not hmac.compare_digest(
        request.headers.get("x-dcar-request", ""), expected_marker
    ):
        return False
    origin = request.headers.get("origin")
    if origin:
        forwarded_proto = request.headers.get("x-forwarded-proto") if trusted_proxy else None
        forwarded_host = request.headers.get("x-forwarded-host") if trusted_proxy else None
        expected = (
            f"{forwarded_proto or request.url.scheme}://"
            f"{forwarded_host or request.headers.get('host', '')}"
        )
        return hmac.compare_digest(origin.rstrip("/"), expected.rstrip("/"))
    return request.headers.get("sec-fetch-site", "none") in {"none", "same-origin"}


def _no_store(response: Response) -> Response:
    response.headers["Cache-Control"] = "no-store"
    response.headers["Pragma"] = "no-cache"
    return response


def _delete_session_cookie(response: Response, config: AuthGatewayConfig) -> None:
    response.delete_cookie(
        SESSION_COOKIE,
        path=config.cookie_path,
        secure=config.secure_cookie,
        httponly=True,
        samesite="lax",
    )


def _auth_error(
    status_code: int,
    code: str,
    *,
    detail: Optional[str] = None,
    headers: Optional[Mapping[str, str]] = None,
    legacy: bool = False,
) -> Response:
    """JSON error for the authentication endpoints.

    ``legacy`` keeps the original ``{"detail": ...}`` shape used by the
    long-standing login/logout contract; new endpoints also carry ``code``.
    """
    payload: dict[str, str] = {"detail": detail or ERROR_COPY[code]}
    if not legacy:
        payload["code"] = code
    response = JSONResponse(payload, status_code=status_code)
    if headers:
        response.headers.update(headers)
    return _no_store(response)


def _client_ip(request: Request, trusted_proxy: bool) -> str:
    if trusted_proxy:
        forwarded = request.headers.get("x-real-ip", "").strip()
        if forwarded:
            return forwarded[:64]
    return (request.client.host if request.client else "unknown")[:64]


class StorageUnavailable(RuntimeError):
    """Credential validity is unknown while its store cannot be read."""


class BodyTooLarge(ValueError):
    pass


async def _bounded_body(request: Request, limit: int) -> bytes:
    """Stop consuming at the limit; never buffer an unbounded request body."""
    body = bytearray()
    async for chunk in request.stream():
        if len(chunk) > limit - len(body):
            raise BodyTooLarge
        body.extend(chunk)
    return bytes(body)


class PasswordWork:
    """One in-flight attempt per account, four expensive workers per process.

    Admission has no await, so it is atomic on the application's event loop.
    A password login holds its slot through failure accounting. Cancelling a
    request never releases a slot while its CPU worker is still running.
    IP abuse is limited by persisted failure accounting, not by this lease:
    different accounts may legitimately share an office's public IP address.
    Production runs exactly one gateway worker, as required by the unit/compose.
    """

    def __init__(self) -> None:
        self.keys: set[str] = set()
        self.active = 0

    @asynccontextmanager
    async def slot(self, username: str) -> AsyncIterator[None]:
        keys = {"user:" + username.lower()}
        if self.active >= MAX_CONCURRENT_PASSWORD_WORK or self.keys.intersection(keys):
            raise auth_store.RateLimited(1)
        self.active += 1
        self.keys.update(keys)
        try:
            yield
        finally:
            self.keys.difference_update(keys)
            self.active -= 1

    @staticmethod
    async def run(function: Callable[..., _T], *args: object) -> _T:
        task = asyncio.create_task(asyncio.to_thread(function, *args))
        cancelled = False
        while not task.done():
            try:
                await asyncio.shield(task)
            except asyncio.CancelledError:
                # Even repeated client cancellation cannot release the lease
                # before the underlying, uncancellable CPU thread completes.
                cancelled = True
        if cancelled:
            # Retrieve any exception so a disconnected client leaves no orphan
            # task warning, then honour cancellation without returning a result.
            task.exception()
            raise asyncio.CancelledError
        return task.result()


async def _bounded_form(
    request: Request, *, legacy: bool = False
) -> Union[Mapping[str, str], Response]:
    """Parse a form body, refusing early on media type and declared length."""
    content_type = request.headers.get("content-type", "")
    media_type = content_type.split(";", 1)[0].strip().lower()
    if media_type != FORM_CONTENT_TYPE:
        return _auth_error(415, "unsupported_media_type", legacy=legacy)
    declared = request.headers.get("content-length")
    if declared is None:
        return _auth_error(411, "length_required", legacy=legacy)
    try:
        declared_length = int(declared)
    except ValueError:
        return _auth_error(411, "length_required", legacy=legacy)
    if declared_length < 0:
        return _auth_error(411, "length_required", legacy=legacy)
    if declared_length > MAX_LOGIN_BODY_BYTES:
        return _auth_error(413, "payload_too_large", legacy=legacy)
    try:
        body = await _bounded_body(request, MAX_LOGIN_BODY_BYTES)
    except BodyTooLarge:
        return _auth_error(413, "payload_too_large", legacy=legacy)
    values = parse_qs(body.decode("utf-8", errors="replace"), keep_blank_values=True)
    return {key: rows[-1] for key, rows in values.items() if rows}


def _connection_tokens(headers: httpx.Headers | Mapping[str, str]) -> set[str]:
    value = headers.get("connection", "")
    return {token.strip().lower() for token in value.split(",") if token.strip()}


def _proxy_headers(
    request: Request, base_path: str, *, strip_all_dcar: bool = False
) -> dict[str, str]:
    blocked = HOP_BY_HOP_HEADERS | _connection_tokens(request.headers)
    headers: dict[str, str] = {}
    for key, value in request.headers.items():
        lower = key.lower()
        if (strip_all_dcar and lower.startswith("x-dcar-")) or lower in blocked or lower in {
            "host",
            "content-length",
            "cookie",
            "authorization",
            "x-dcar-authenticated-user",
            "x-forwarded-for",
            "x-forwarded-host",
            "x-forwarded-prefix",
            "x-forwarded-proto",
            "x-real-ip",
        }:
            continue
        headers[key] = value
    client_ip = request.headers.get("x-real-ip") or (
        request.client.host if request.client else ""
    )
    headers["X-Forwarded-For"] = client_ip
    headers["X-Forwarded-Host"] = request.headers.get("host", "")
    headers["X-Forwarded-Proto"] = request.headers.get(
        "x-forwarded-proto", request.url.scheme
    )
    if base_path:
        headers["X-Forwarded-Prefix"] = base_path
    return headers


def _public_location(
    value: str,
    upstream_base: str,
    *,
    public_base_path: str = "",
    restore_base_path: bool = False,
) -> str:
    location = urlsplit(value)
    upstream = urlsplit(upstream_base)
    if (
        location.scheme.lower() != upstream.scheme.lower()
        or location.netloc.lower() != upstream.netloc.lower()
    ):
        return value
    target = location.path or "/"
    if (
        restore_base_path
        and public_base_path
        and target != public_base_path
        and not target.startswith(f"{public_base_path}/")
    ):
        target = f"{public_base_path}{target if target.startswith('/') else '/' + target}"
    if location.query:
        target = f"{target}?{location.query}"
    if location.fragment:
        target = f"{target}#{location.fragment}"
    return target


def _response_header_pairs(
    upstream: httpx.Response,
    upstream_base: str,
    *,
    public_base_path: str = "",
    restore_base_path: bool = False,
    strip_set_cookie: bool = False,
) -> list[tuple[bytes, bytes]]:
    blocked = HOP_BY_HOP_HEADERS | _connection_tokens(upstream.headers)
    return [
        (
            key.encode("latin-1"),
            (
                _public_location(
                    value,
                    upstream_base,
                    public_base_path=public_base_path,
                    restore_base_path=restore_base_path,
                )
                if key.lower() == "location"
                else value
            ).encode("latin-1"),
        )
        for key, value in upstream.headers.multi_items()
        if key.lower() not in blocked
        and not (strip_set_cookie and key.lower() == "set-cookie")
    ]


def create_app(
    config: Optional[AuthGatewayConfig] = None,
    *,
    web_transport: Optional[httpx.AsyncBaseTransport] = None,
    api_transport: Optional[httpx.AsyncBaseTransport] = None,
    douyin_transport: Optional[httpx.AsyncBaseTransport] = None,
    sms_transport: Optional[httpx.AsyncBaseTransport] = None,
) -> FastAPI:
    resolved = config or AuthGatewayConfig.from_env()
    # Third-party HTTP client logging would echo request lines; keep them quiet
    # so phone numbers, codes and credentials never reach the gateway log.
    logging.getLogger("httpx").setLevel(logging.WARNING)
    logging.getLogger("httpcore").setLevel(logging.WARNING)
    store = auth_store.AuthStore(
        resolved.session_db_path,
        throttle_window_seconds=resolved.throttle_window_seconds,
        throttle_max_failures=resolved.throttle_max_failures,
        sms_daily_cap=resolved.sms_daily_cap,
        change_log_path=resolved.change_log_path,
    )
    trusted_proxy = bool(resolved.base_path)
    password_work = PasswordWork()

    @asynccontextmanager
    async def lifespan(application: FastAPI):
        application.state.sms_sender = None
        if not resolved.bypass_auth:
            if not resolved.login_template_path.is_file():
                raise RuntimeError(
                    f"login template is missing: {resolved.login_template_path}"
                )
            assert resolved.pepper_path is not None
            store.pepper = auth_store.load_pepper(resolved.pepper_path)
            store.initialize()
            store.healthcheck()
            if resolved.sms_provider == "tencent":
                assert resolved.sms_credentials_path is not None
                application.state.sms_sender = TencentSmsSender(
                    load_sms_credentials(resolved.sms_credentials_path),
                    transport=sms_transport,
                )
            else:
                application.state.sms_sender = LogSmsSender()
        application.state.douyin_edge_key = (
            _read_edge_key(resolved.douyin_edge_key_path)
            if resolved.douyin_edge_key_path is not None
            else None
        )
        application.state.web_client = httpx.AsyncClient(
            transport=web_transport,
            timeout=httpx.Timeout(60, connect=5),
            follow_redirects=False,
            trust_env=False,
        )
        application.state.api_client = httpx.AsyncClient(
            transport=api_transport,
            timeout=httpx.Timeout(3600, connect=5),
            follow_redirects=False,
            trust_env=False,
        )
        application.state.douyin_client = (
            httpx.AsyncClient(
                transport=douyin_transport,
                timeout=httpx.Timeout(5, connect=2, read=5, write=5, pool=2),
                follow_redirects=False,
                trust_env=False,
            )
            if resolved.douyin_upstream is not None
            else None
        )
        try:
            yield
        finally:
            await application.state.web_client.aclose()
            await application.state.api_client.aclose()
            if application.state.douyin_client is not None:
                await application.state.douyin_client.aclose()
            if application.state.sms_sender is not None:
                await application.state.sms_sender.aclose()

    application = FastAPI(
        title="Dcar Sentinel authentication gateway",
        docs_url=None,
        redoc_url=None,
        openapi_url=None,
        lifespan=lifespan,
    )

    @application.exception_handler(StorageUnavailable)
    async def storage_unavailable(request: Request, exc: StorageUnavailable) -> Response:
        del request, exc
        # A storage outage must never clear a potentially valid session cookie.
        return user_error("storage_unavailable")

    async def principal_for(request: Request) -> Optional[Principal]:
        if resolved.bypass_auth:
            return Principal(username=BYPASS_USERNAME, role=None, token_sha256="")
        token = request.cookies.get(SESSION_COOKIE, "")
        try:
            return await asyncio.to_thread(store.resolve_principal, token)
        except (OSError, RuntimeError, sqlite3.Error) as exc:
            LOGGER.exception("account store became unavailable")
            raise StorageUnavailable from exc

    def user_error(code: str, *, delete_cookie: bool = False) -> Response:
        status, detail = USER_ERROR_RESPONSES[code]
        public_code = "target_forbidden" if code == "target_forbidden_delete" else code
        response = JSONResponse({"detail": detail, "code": public_code}, status_code=status)
        if delete_cookie:
            _delete_session_cookie(response, resolved)
        return _no_store(response)

    async def bounded_json(
        request: Request,
    ) -> tuple[Optional[dict[str, object]], Optional[Response]]:
        """Content-Type / Content-Length prechecks, then a JSON object body."""
        content_type = request.headers.get("content-type", "")
        if content_type.split(";", 1)[0].strip().lower() != "application/json":
            return None, user_error("unsupported_media_type")
        declared = request.headers.get("content-length")
        if declared is None:
            return None, user_error("length_required")
        try:
            declared_length = int(declared)
        except ValueError:
            return None, user_error("invalid_payload")
        if declared_length < 0:
            return None, user_error("invalid_payload")
        if declared_length > MAX_LOGIN_BODY_BYTES:
            return None, user_error("payload_too_large")
        try:
            body = await _bounded_body(request, MAX_LOGIN_BODY_BYTES)
        except BodyTooLarge:
            return None, user_error("payload_too_large")
        try:
            payload = json.loads(body.decode("utf-8"))
        except (UnicodeDecodeError, ValueError):
            return None, user_error("invalid_payload")
        if not isinstance(payload, dict):
            return None, user_error("invalid_payload")
        return payload, None

    def user_store_error(error: Exception, *, deleting: bool) -> Response:
        if isinstance(error, auth_store.RateLimited):
            response = user_error("too_many_attempts")
            response.headers["Retry-After"] = str(error.retry_after)
            return response
        if isinstance(error, auth_store.SessionRevoked):
            return user_error("session_revoked", delete_cookie=True)
        if isinstance(error, auth_store.TargetForbidden):
            return user_error("target_forbidden_delete" if deleting else "target_forbidden")
        if isinstance(error, (auth_store.PhoneConflict, sqlite3.IntegrityError)):
            return user_error("phone_conflict")
        if isinstance(error, auth_store.AuthStoreError) and error.code in USER_ERROR_RESPONSES:
            return user_error(error.code)
        LOGGER.exception("account store failed during user management")
        return user_error("storage_unavailable")

    async def user_management(request: Request, stripped: str) -> Response:
        """``GET /auth/users``, ``POST /auth/users/update`` and ``/auth/users/delete``.

        Fixed order: bypass 404 → 405 → same-origin marker → 415 → 411/413 →
        400 (shape) → 401 → 403 (role) → field rules → password hash →
        transaction (which re-verifies the session and both roles).
        """
        if resolved.bypass_auth:
            return Response(status_code=404)
        if stripped == USER_LIST_PATH:
            if request.method != "GET":
                return Response(status_code=405, headers={"Allow": "GET"})
            principal = await principal_for(request)
            if principal is None:
                return unauthenticated(request, "/api/auth/users")
            if principal.role not in auth_store.USER_ADMIN_ROLES:
                LOGGER.warning(
                    "user management denied actor=%s role=%s path=%s",
                    principal.username, principal.role, stripped,
                )
                return user_error("forbidden")
            try:
                users = await asyncio.to_thread(store.list_users)
            except (OSError, sqlite3.Error):
                LOGGER.exception("account store failed while listing users")
                return user_error("storage_unavailable")
            return _no_store(
                JSONResponse(
                    {
                        "actor": {"username": principal.username, "role": principal.role},
                        "items": [user.to_public() for user in users],
                    }
                )
            )

        marker = USER_POST_ACTIONS[stripped]
        deleting = marker == "user-delete"
        if request.method != "POST":
            return Response(status_code=405, headers={"Allow": "POST"})
        if not _same_origin_post(request, marker, trusted_proxy=trusted_proxy):
            return user_error("origin_mismatch")
        payload, failure = await bounded_json(request)
        if failure is not None or payload is None:
            return failure or user_error("invalid_payload")
        expected_fields = USER_DELETE_FIELDS if deleting else USER_UPDATE_FIELDS
        if set(payload) != expected_fields or any(
            not isinstance(payload[field], str) for field in expected_fields
        ):
            return user_error("invalid_payload")
        target = str(payload["username"]).strip()
        if not target or len(target) > 128:
            return user_error("invalid_payload")

        principal = await principal_for(request)
        if principal is None:
            return unauthenticated(request, "/api/auth/users")
        if principal.role not in auth_store.USER_ADMIN_ROLES:
            LOGGER.warning(
                "user management denied actor=%s role=%s path=%s",
                principal.username, principal.role, stripped,
            )
            return user_error("forbidden")

        try:
            if deleting:
                await asyncio.to_thread(store.delete_user, principal.token_sha256, target)
                LOGGER.info(
                    "user management actor=%s action=delete target=%s",
                    principal.username, target,
                )
                return _no_store(JSONResponse({}))

            role = str(payload["role"]).strip()
            if role not in auth_store.ROLE_RANK:
                return user_error("role_invalid")
            phone_value = str(payload["phone"]).strip()
            phone: Optional[str] = None
            if phone_value:
                if not auth_store.valid_phone(phone_value):
                    return user_error("phone_invalid")
                phone = phone_value
            password = str(payload["password"])
            password_hash: Optional[str] = None
            if password:
                if target.lower() == principal.username.lower():
                    return user_error("self_password_change")
                problem = auth_store.password_problem(
                    password, username=target, phone=phone or ""
                )
                if problem is not None:
                    return user_error(problem)
                async with password_work.slot(target):
                    password_hash = await password_work.run(auth_store.hash_password, password)
            record = await asyncio.to_thread(
                store.update_user,
                principal.token_sha256,
                target,
                phone=phone,
                role=role,
                password_hash=password_hash,
            )
        except Exception as error:  # noqa: BLE001 - mapped to stable codes; unknown → 503
            response = user_store_error(error, deleting=deleting)
            if response.status_code < 500:
                LOGGER.warning(
                    "user management rejected actor=%s path=%s target=%s reason=%s",
                    principal.username, stripped, target, type(error).__name__,
                )
            return response
        LOGGER.info(
            "user management actor=%s action=update target=%s fields=%s",
            principal.username,
            record.username,
            [field for field in ("phone", "role", "password") if payload[field] != ""],
        )
        return _no_store(JSONResponse({"item": record.to_public()}))

    def role_destination(principal: Principal, return_to: str) -> str:
        target = _safe_return_to(return_to, resolved)
        destination_path = _stripped_path(urlsplit(target).path, resolved.base_path)
        if (principal.role not in auth_store.USER_ADMIN_ROLES and destination_path is not None
                and _account_admin_target(destination_path) is not None):
            return resolved.route("/overview")
        if principal.role == auth_store.ROLE_NEW_USER:
            parsed = urlsplit(target)
            page_path = _stripped_path(parsed.path, resolved.base_path)
            canonical = page_path.removesuffix("/") if page_path else ""
            if canonical not in NEW_USER_PAGES:
                return resolved.route("/overview")
            query = urlencode([
                (key, value) for key, value in parse_qsl(parsed.query, keep_blank_values=True)
                if key not in {"_rsc", "_data"}
            ])
            return resolved.route(canonical) + (f"?{query}" if query else "")
        if urlsplit(target).path == resolved.route(PENDING_APPROVAL_PATH):
            return resolved.route("/overview")
        return target

    async def session_response(
        token: str, ttl: int, *, persistent: bool, redirect_to: str
    ) -> Response:
        principal = await asyncio.to_thread(store.resolve_principal, token)
        if principal is None:
            return user_error("session_revoked", delete_cookie=True)
        response = JSONResponse({"redirect_to": role_destination(principal, redirect_to)})
        response.set_cookie(
            SESSION_COOKIE,
            token,
            max_age=ttl if persistent else None,
            path=resolved.cookie_path,
            secure=resolved.secure_cookie,
            httponly=True,
            samesite="lax",
        )
        return _no_store(response)

    async def guarded_form(
        request: Request, marker: str, *, legacy: bool = False
    ) -> Union[Mapping[str, str], Response]:
        if not _same_origin_post(request, marker, trusted_proxy=trusted_proxy):
            return _auth_error(403, "origin_mismatch", legacy=legacy)
        return await _bounded_form(request, legacy=legacy)

    def store_error(exc: auth_store.AuthStoreError) -> Response:
        if isinstance(exc, auth_store.RateLimited):
            return _auth_error(
                429,
                "too_many_attempts",
                headers={"Retry-After": str(exc.retry_after)},
            )
        if isinstance(exc, auth_store.AccountDisabled):
            return _auth_error(403, "account_disabled")
        if isinstance(exc, auth_store.PhoneNotAllowed):
            return _auth_error(403, "phone_not_allowed")
        if isinstance(exc, auth_store.PhoneNotRegistered):
            return _auth_error(404, "phone_not_registered")
        if isinstance(exc, auth_store.PhoneRegistered):
            return _auth_error(409, "phone_registered")
        if isinstance(exc, auth_store.UsernameTaken):
            return _auth_error(409, "username_taken")
        if isinstance(exc, auth_store.InvalidCode):
            return _auth_error(401, "invalid_code")
        if isinstance(exc, auth_store.ResetExpired):
            return _auth_error(401, "reset_expired")
        LOGGER.error("unexpected account store outcome %s", type(exc).__name__)
        return _auth_error(503, "service_unavailable")

    async def password_login(
        username: str, password: str, client_ip: str, remember: bool, return_to: str
    ) -> Response:
        async with password_work.slot(username):
            stored_hash = await asyncio.to_thread(
                store.begin_password_login, username, client_ip
            )

            def verify() -> bool:
                return auth_store.verify_password(
                    password, stored_hash or auth_store.dummy_hash()
                )

            valid = await password_work.run(verify)
            session: Optional[tuple[str, str]] = None
            ttl = resolved.remember_session_seconds if remember else resolved.session_seconds
            if valid and stored_hash is not None:
                session = await asyncio.to_thread(
                    store.finish_password_login, username, stored_hash, ttl
                )
            if session is None:
                await asyncio.to_thread(store.record_login_failure, username, client_ip)
                if resolved.failure_delay_seconds:
                    await asyncio.sleep(resolved.failure_delay_seconds)
                return _auth_error(
                    401, "invalid_code", detail="账号或密码不正确", legacy=True
                )
            token, _canonical = session
            return await session_response(token, ttl, persistent=remember, redirect_to=return_to)

    async def proxy_request(
        request: Request,
        *,
        upstream_base: str,
        upstream_path: str,
        client: httpx.AsyncClient,
        username: str,
        strip_all_dcar: bool = False,
        extra_headers: Optional[Mapping[str, str]] = None,
        buffered_body: Optional[bytes] = None,
        restore_base_path: bool = False,
        strip_set_cookie: bool = False,
    ) -> Response:
        url = f"{upstream_base}{upstream_path}"
        if request.url.query:
            url = f"{url}?{request.url.query}"

        async def body_stream() -> AsyncIterator[bytes]:
            async for chunk in request.stream():
                yield chunk

        headers = _proxy_headers(
            request, resolved.base_path, strip_all_dcar=strip_all_dcar
        )
        headers["X-Dcar-Authenticated-User"] = username
        if extra_headers:
            headers.update(extra_headers)
        try:
            upstream = await client.send(
                client.build_request(
                    request.method,
                    url,
                    headers=headers,
                    content=(
                        buffered_body
                        if buffered_body is not None
                        else (
                            body_stream()
                            if request.method not in {"GET", "HEAD"}
                            else b""
                        )
                    ),
                ),
                stream=True,
            )
        except httpx.HTTPError:
            LOGGER.exception("upstream request failed: %s", upstream_base)
            return _no_store(
                JSONResponse(
                    {
                        "detail": "系统暂时无法加载数据，请稍后重试",
                        "code": "upstream_unavailable",
                    },
                    status_code=502,
                )
            )
        response = StreamingResponse(
            upstream.aiter_raw(),
            status_code=upstream.status_code,
            background=BackgroundTask(upstream.aclose),
        )
        response.raw_headers = _response_header_pairs(
            upstream,
            upstream_base,
            public_base_path=resolved.base_path,
            restore_base_path=restore_base_path,
            strip_set_cookie=strip_set_cookie,
        )
        if not resolved.bypass_auth:
            # A browser must ask the gateway again after a role change or
            # logout, including for media, Flight payloads and 206/304 replies.
            # Upstream public caching policies cannot override authorization.
            _no_store(response)
            response.headers["Cache-Control"] = "private, no-store"
            if (upstream_base == resolved.web_upstream
                    and _is_revalidatable_code_asset(
                        request, upstream, upstream_path, resolved.static_asset_manifest_path,
                    )):
                # no-cache requires authentication and validator checks on every
                # reuse. ETag/If-None-Match remain end-to-end; a revoked session
                # is denied before this proxy can return a 304.
                response.headers["Cache-Control"] = "private, no-cache"
        return response

    def unauthenticated(request: Request, stripped: str) -> Response:
        response: Response
        if stripped == DOUYIN_CALLBACK_PATH:
            return_to = quote(resolved.route("/douyin"), safe="")
            response = RedirectResponse(
                resolved.route("/login")
                + "?notice=douyin-session-required&return_to="
                + return_to,
                status_code=303,
            )
            response.headers["Referrer-Policy"] = "no-referrer"
        elif stripped == "/api" or stripped.startswith("/api/"):
            response = JSONResponse({"detail": "请先登录"}, status_code=401)
        else:
            target = quote(_request_target(request), safe="")
            response = RedirectResponse(
                f"{resolved.route('/login')}?return_to={target}", status_code=302
            )
        if request.cookies.get(SESSION_COOKIE):
            _delete_session_cookie(response, resolved)
        return _no_store(response)

    def approval_required(request: Request, stripped: str) -> Response:
        if _is_page_navigation(request, stripped):
            response = RedirectResponse(
                resolved.route("/overview"), status_code=303
            )
            response.headers["Referrer-Policy"] = "no-referrer"
            return _no_store(response)
        return _no_store(
            JSONResponse(
                {"detail": "账号尚未授权，请联系管理员", "code": "approval_required"},
                status_code=403,
            )
        )

    @application.api_route("/{path:path}", methods=PROXY_METHODS)
    async def gateway(request: Request, path: str) -> Response:
        del path
        stripped = _stripped_path(request.url.path, resolved.base_path)
        if stripped is None:
            return Response(status_code=404)
        if stripped == "/auth/health":
            if request.method != "GET":
                return Response(status_code=405)
            if not resolved.bypass_auth:
                try:
                    await asyncio.to_thread(store.healthcheck)
                except (OSError, RuntimeError, sqlite3.Error):
                    LOGGER.exception("authentication gateway readiness check failed")
                    return _no_store(
                        JSONResponse({"status": "unavailable"}, status_code=503)
                    )
            return _no_store(JSONResponse({"status": "ok"}))

        if stripped == "/login":
            if request.method != "GET":
                return Response(status_code=405)
            return_to = _safe_return_to(
                request.query_params.get("return_to", ""), resolved
            )
            if resolved.bypass_auth:
                return _no_store(RedirectResponse(return_to, status_code=303))
            principal = await principal_for(request)
            if principal is not None:
                return _no_store(
                    RedirectResponse(role_destination(principal, return_to), status_code=303)
                )
            html = resolved.login_template_path.read_text(encoding="utf-8")
            login_page_response = HTMLResponse(html)
            login_page_response.headers.update(
                {
                    "Content-Security-Policy": (
                        "default-src 'self'; style-src 'unsafe-inline'; "
                        "script-src 'unsafe-inline'; img-src 'self' data:; "
                        "connect-src 'self'; frame-ancestors 'none'; "
                        "base-uri 'none'; form-action 'self'"
                    ),
                    "Referrer-Policy": "no-referrer",
                    "X-Content-Type-Options": "nosniff",
                    "X-Frame-Options": "DENY",
                }
            )
            return _no_store(login_page_response)

        if stripped == "/auth/login":
            if request.method != "POST":
                return Response(status_code=405)
            if resolved.bypass_auth:
                return _no_store(
                    JSONResponse({"redirect_to": resolved.route("/overview")})
                )
            payload = await guarded_form(request, "login", legacy=True)
            if isinstance(payload, Response):
                return payload
            username = payload.get("username", "").strip()
            password = payload.get("password", "")
            return_to = _safe_return_to(payload.get("return_to", ""), resolved)
            if not username or len(username) > 128 or not password or len(password) > 4096:
                return _no_store(
                    JSONResponse({"detail": "账号或密码不正确"}, status_code=401)
                )
            client_ip = _client_ip(request, trusted_proxy)
            try:
                return await password_login(
                    username, password, client_ip, payload.get("remember") == "1", return_to
                )
            except auth_store.RateLimited as exc:
                return _no_store(
                    JSONResponse(
                        {"detail": "尝试次数太多，请稍后再登录"},
                        status_code=429,
                        headers={"Retry-After": str(exc.retry_after)},
                    )
                )
            except auth_store.AccountDisabled:
                return _no_store(
                    JSONResponse({"detail": "该账号已停用"}, status_code=403)
                )
            except (OSError, RuntimeError, sqlite3.Error):
                LOGGER.exception("account store unavailable during login")
                return _no_store(
                    JSONResponse(
                        {"detail": "暂时无法登录，请稍后重试"}, status_code=503
                    )
                )
        if stripped in AUTH_MARKERS:
            if request.method != "POST":
                return Response(status_code=405)
            if resolved.bypass_auth:
                return Response(status_code=404)
            payload = await guarded_form(request, AUTH_MARKERS[stripped])
            if isinstance(payload, Response):
                return payload
            client_ip = _client_ip(request, trusted_proxy)
            phone = payload.get("phone", "").strip()
            code = payload.get("code", "").strip()
            return_to = _safe_return_to(payload.get("return_to", ""), resolved)
            try:
                if stripped == "/auth/code":
                    purpose = payload.get("purpose", "").strip()
                    if purpose not in auth_store.PURPOSES:
                        return _auth_error(400, "invalid_purpose")
                    if not auth_store.valid_phone(phone):
                        return _auth_error(400, "invalid_phone")
                    fresh_code = auth_store.generate_code()
                    try:
                        challenge_id, _bound = await asyncio.to_thread(
                            store.reserve_send, purpose, phone, client_ip, fresh_code
                        )
                    except auth_store.RateLimited as exc:
                        return _auth_error(
                            429,
                            "rate_limited",
                            headers={"Retry-After": str(exc.retry_after)},
                        )
                    outcome = await request.app.state.sms_sender.send(
                        phone, fresh_code
                    )
                    recorded = await asyncio.to_thread(
                        store.finish_send,
                        challenge_id,
                        outcome.status,
                        outcome.provider_code,
                    )
                    LOGGER.info(
                        "sms challenge=%d purpose=%s status=%s provider_code=%s request_id=%s serial_no=%s",
                        challenge_id,
                        purpose,
                        outcome.status,
                        outcome.provider_code,
                        outcome.request_id,
                        outcome.serial_no,
                    )
                    if not recorded:
                        LOGGER.warning(
                            "sms challenge=%d was no longer effective when finished",
                            challenge_id,
                        )
                        return _auth_error(503, "sms_failed")
                    if outcome.status == "sent":
                        return _no_store(JSONResponse({}))
                    if outcome.provider_code in RATE_LIMIT_CODES:
                        return _auth_error(429, "rate_limited")
                    if outcome.provider_code in BAD_NUMBER_CODES:
                        return _auth_error(400, "invalid_phone")
                    return _auth_error(503, "sms_failed")

                if not auth_store.valid_phone(phone) and stripped != "/auth/reset/confirm":
                    return _auth_error(400, "invalid_phone")

                if stripped == "/auth/login/code":
                    if not auth_store.valid_code(code):
                        return _auth_error(401, "invalid_code")
                    remember = payload.get("remember") == "1"
                    ttl = (
                        resolved.remember_session_seconds
                        if remember
                        else resolved.session_seconds
                    )
                    token, _canonical = await asyncio.to_thread(
                        store.login_with_code, phone, code, client_ip, ttl
                    )
                    return await session_response(
                        token, ttl, persistent=remember, redirect_to=return_to
                    )

                if stripped == "/auth/register":
                    username = payload.get("username", "").strip()
                    password = payload.get("password", "")
                    if not auth_store.valid_username(username):
                        return _auth_error(400, "invalid_username")
                    if not auth_store.valid_code(code):
                        return _auth_error(401, "invalid_code")
                    problem = auth_store.password_problem(
                        password, username=username, phone=phone
                    )
                    if problem is not None:
                        return _auth_error(400, problem)
                    challenge_id = await asyncio.to_thread(
                        store.prepare_register, username, phone, code, client_ip
                    )
                    async with password_work.slot(username):
                        password_hash = await password_work.run(auth_store.hash_password, password)
                    ttl = resolved.remember_session_seconds
                    token = await asyncio.to_thread(
                        store.complete_register,
                        username,
                        phone,
                        password_hash,
                        challenge_id,
                        ttl,
                    )
                    return await session_response(
                        token, ttl, persistent=True, redirect_to=return_to
                    )

                if stripped == "/auth/reset/verify":
                    if not auth_store.valid_code(code):
                        return _auth_error(401, "invalid_code")
                    reset_token = await asyncio.to_thread(
                        store.verify_reset, phone, code, client_ip
                    )
                    return _no_store(JSONResponse({"reset_token": reset_token}))

                if stripped == "/auth/reset/confirm":
                    reset_token = payload.get("reset_token", "").strip()
                    password = payload.get("password", "")
                    if not reset_token or len(reset_token) > 128:
                        return _auth_error(401, "reset_expired")
                    bound_username = await asyncio.to_thread(
                        store.peek_ticket, reset_token, client_ip
                    )
                    bound_user = await asyncio.to_thread(
                        store.get_user, bound_username
                    )
                    problem = auth_store.password_problem(
                        password,
                        username=bound_username,
                        phone=(bound_user.phone if bound_user else "") or "",
                    )
                    if problem is not None:
                        return _auth_error(400, problem)
                    async with password_work.slot(bound_username):
                        password_hash = await password_work.run(auth_store.hash_password, password)
                    ttl = resolved.remember_session_seconds
                    token, _canonical = await asyncio.to_thread(
                        store.confirm_reset, reset_token, password_hash, ttl
                    )
                    return await session_response(
                        token, ttl, persistent=True, redirect_to=return_to
                    )
            except auth_store.AuthStoreError as exc:
                return store_error(exc)
            except (OSError, RuntimeError, sqlite3.Error):
                LOGGER.exception("account store unavailable during %s", stripped)
                return _auth_error(503, "service_unavailable")
            return Response(status_code=404)

        if stripped == "/auth/logout":
            if request.method != "POST":
                return Response(status_code=405)
            if not _same_origin_post(
                request, "logout", trusted_proxy=bool(resolved.base_path)
            ):
                return _no_store(
                    JSONResponse(
                        {"detail": "页面已失效，请刷新后再退出"}, status_code=403
                    )
                )
            if resolved.bypass_auth:
                logout_response = JSONResponse(
                    {"redirect_to": resolved.route("/overview")}
                )
                _delete_session_cookie(logout_response, resolved)
                return _no_store(logout_response)
            token = request.cookies.get(SESSION_COOKIE, "")
            try:
                await asyncio.to_thread(store.revoke_session, token)
            except (OSError, RuntimeError, sqlite3.Error):
                LOGGER.exception("account store unavailable during logout")
                return _auth_error(503, "service_unavailable", legacy=True)
            logout_response = JSONResponse({"redirect_to": resolved.route("/login")})
            _delete_session_cookie(logout_response, resolved)
            return _no_store(logout_response)

        if stripped == "/auth/session":
            if request.method != "GET":
                return Response(status_code=405)
            principal = await principal_for(request)
            if principal is None:
                return unauthenticated(request, "/api/auth/session")
            session_payload: dict[str, object] = {
                "authenticated": True,
                "username": principal.username,
            }
            if principal.role is not None:
                session_payload["role"] = principal.role
            return _no_store(JSONResponse(session_payload))

        if stripped == USER_LIST_PATH or stripped in USER_POST_ACTIONS:
            return await user_management(request, stripped)

        if stripped.startswith("/auth/"):
            return Response(status_code=404)

        principal = await principal_for(request)
        account_target = _account_admin_target(stripped)
        if (principal is not None and principal.role not in auth_store.USER_ADMIN_ROLES
                and account_target is not None):
            if (any(_path_has_prefix(account_target, page) for page in ("/accounts", "/douyin"))
                    and _is_page_navigation(request, account_target)):
                return _no_store(RedirectResponse(resolved.route("/overview"), status_code=303))
            return _no_store(JSONResponse(
                {"detail": "仅管理员可访问账号管理", "code": "account_admin_required"}, status_code=403,
            ))
        if principal is not None and principal.role == auth_store.ROLE_NEW_USER:
            if stripped in NEW_USER_PAGES and _is_page_navigation(request, stripped):
                pending_page_response = HTMLResponse(_new_user_workbench_html(resolved.base_path, stripped))
                pending_page_response.headers.update(
                    {
                        "Content-Security-Policy": (
                            "default-src 'none'; style-src 'unsafe-inline'; "
                            "script-src 'unsafe-inline'; connect-src 'self'; "
                            "frame-ancestors 'none'; base-uri 'none'; form-action 'none'"
                        ),
                        "Referrer-Policy": "no-referrer",
                        "X-Content-Type-Options": "nosniff",
                        "X-Frame-Options": "DENY",
                    }
                )
                return _no_store(pending_page_response)
            if stripped.endswith("/") and stripped[:-1] in NEW_USER_PAGES and _is_page_navigation(request, stripped):
                return _no_store(RedirectResponse(
                    role_destination(principal, _request_target(request)), status_code=303
                ))
            return approval_required(request, stripped)
        if _is_douyin_path(stripped) and request.method not in {"GET", "POST"}:
            return _no_store(
                Response(status_code=405, headers={"Allow": "GET, POST"})
            )
        if stripped == DOUYIN_CALLBACK_PATH and request.method != "GET":
            return _no_store(Response(status_code=405, headers={"Allow": "GET"}))
        if principal is None:
            return unauthenticated(request, stripped)
        authenticated_username = principal.username

        if stripped == PENDING_APPROVAL_PATH:
            return _no_store(
                RedirectResponse(resolved.route("/overview"), status_code=303)
            )

        if _path_has_prefix(stripped, USER_MANAGEMENT_PAGE_PREFIX):
            # The page itself is gated here (the shell only hides the entry);
            # bypass has no role and lands on the overview like an operator.
            if principal.role not in auth_store.USER_ADMIN_ROLES:
                LOGGER.warning(
                    "user management page denied actor=%s role=%s path=%s",
                    principal.username, principal.role, stripped,
                )
                return _no_store(
                    RedirectResponse(resolved.route("/overview"), status_code=303)
                )

        if _is_douyin_path(stripped):
            if resolved.bypass_auth:
                return _no_store(
                    JSONResponse(
                        {"detail": "当前模式禁止抖音授权"}, status_code=403
                    )
                )
            if resolved.douyin_upstream is None:
                return Response(status_code=404)
            if stripped == DOUYIN_CALLBACK_PATH:
                if not _valid_douyin_callback_navigation(request):
                    return _no_store(
                        JSONResponse(
                            {"detail": "授权回调来源无效"}, status_code=403
                        )
                    )

            verified_action: Optional[str] = None
            buffered_body: Optional[bytes] = None
            if request.method == "POST":
                verified_action = DOUYIN_POST_ACTIONS.get(stripped)
                if verified_action is None:
                    return Response(status_code=404)
                if not _same_origin_post(
                    request,
                    verified_action,
                    trusted_proxy=bool(resolved.base_path),
                ):
                    return _no_store(
                        JSONResponse(
                            {"detail": "页面已失效，请刷新后重试"}, status_code=403
                        )
                    )
                content_length = request.headers.get("content-length")
                if content_length is None:
                    return _no_store(
                        JSONResponse({"detail": "请求长度缺失"}, status_code=411)
                    )
                try:
                    declared_length = int(content_length)
                except ValueError:
                    return _no_store(
                        JSONResponse({"detail": "请求长度无效"}, status_code=400)
                    )
                if declared_length < 0:
                    return _no_store(
                        JSONResponse({"detail": "请求长度无效"}, status_code=400)
                    )
                if declared_length > MAX_DOUYIN_BODY_BYTES:
                    return _no_store(
                        JSONResponse({"detail": "请求内容过大"}, status_code=413)
                    )
                try:
                    buffered_body = await _bounded_body(request, MAX_DOUYIN_BODY_BYTES)
                except BodyTooLarge:
                    return _no_store(
                        JSONResponse({"detail": "请求内容过大"}, status_code=413)
                    )

            token = request.cookies.get(SESSION_COOKIE, "")
            extra_headers = {
                "X-Dcar-Edge-Key": request.app.state.douyin_edge_key,
                "X-Dcar-Session-Binding": auth_store.session_token_hash(token),
            }
            if verified_action is not None:
                extra_headers["X-Dcar-Verified-Action"] = verified_action
            douyin_client = request.app.state.douyin_client
            if douyin_client is None:
                return Response(status_code=404)
            response = await proxy_request(
                request,
                upstream_base=resolved.douyin_upstream,
                upstream_path=stripped,
                client=douyin_client,
                username=authenticated_username,
                strip_all_dcar=True,
                extra_headers=extra_headers,
                buffered_body=buffered_body,
                restore_base_path=True,
                strip_set_cookie=True,
            )
            if stripped == DOUYIN_CALLBACK_PATH:
                response.headers["Referrer-Policy"] = "no-referrer"
            return response

        if stripped == "/api" or stripped.startswith("/api/"):
            if (
                request.method not in {"GET", "HEAD", "OPTIONS"}
                # HTTPX removes dot segments when constructing the upstream URL.
                # Check that destination too, including encoded dots decoded by ASGI.
                and any(
                    _path_has_prefix(candidate, "/api/v8/selling-points")
                    for candidate in (
                        stripped, httpx.URL(f"{resolved.api_upstream}{stripped}").path
                    )
                )
            ):
                return _no_store(
                    JSONResponse(
                        {
                            "detail": "卖点标准由后台统一维护，当前页面仅供查看。",
                            "code": "selling_points_read_only",
                        },
                        status_code=403,
                    )
                )
            return await proxy_request(
                request,
                upstream_base=resolved.api_upstream,
                upstream_path=stripped,
                client=request.app.state.api_client,
                username=authenticated_username,
                restore_base_path=True,
            )
        web_upstream_path = _web_upstream_path(
            request.url.path,
            stripped,
            request.scope.get("raw_path", request.url.path.encode("utf-8")),
        )
        if web_upstream_path is None:
            return Response(status_code=404)
        return await proxy_request(
            request,
            upstream_base=resolved.web_upstream,
            upstream_path=web_upstream_path,
            client=request.app.state.web_client,
            username=authenticated_username,
            restore_base_path=(web_upstream_path != request.url.path),
        )

    return application


app = create_app()
