from __future__ import annotations

import asyncio
import hashlib
import hmac
import logging
import os
import secrets
import sqlite3
import threading
import time
from contextlib import asynccontextmanager
from dataclasses import dataclass
from pathlib import Path
from typing import AsyncIterator, Mapping, Optional
from urllib.parse import parse_qs, quote, unquote, urlsplit

import httpx
from fastapi import FastAPI, Request
from fastapi.responses import HTMLResponse, JSONResponse, RedirectResponse, StreamingResponse
from passlib.hash import sha512_crypt  # type: ignore[import-untyped]
from starlette.background import BackgroundTask
from starlette.responses import Response


LOGGER = logging.getLogger("dcar-auth")
PROJECT_ROOT = Path(__file__).resolve().parents[3]
SESSION_COOKIE = "dcar_session"
BYPASS_USERNAME = "temporary-bypass"
DEFAULT_SESSION_SECONDS = 12 * 60 * 60
REMEMBER_SESSION_SECONDS = 30 * 24 * 60 * 60
MAX_LOGIN_BODY_BYTES = 16 * 1024
MAX_DOUYIN_BODY_BYTES = 64 * 1024
PROXY_METHODS = ["GET", "HEAD", "POST", "PUT", "PATCH", "DELETE", "OPTIONS"]
DOUYIN_CALLBACK_PATH = "/oauth/douyin/callback"
DOUYIN_POST_ACTIONS = {
    "/api/douyin/accounts/search": "douyin-accounts-search",
    "/api/douyin/oauth/start": "douyin-oauth-start",
    "/api/douyin/authorizations/match": "douyin-authorization-match",
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
DUMMY_SHA512_HASH = sha512_crypt.hash(
    "invalid-password",
    salt="dcarinvalidsalt",
    rounds=5000,
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
    htpasswd_path: Path
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

    def __post_init__(self) -> None:
        object.__setattr__(self, "base_path", _normalized_base_path(self.base_path))
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
        return cls(
            base_path=os.environ.get("DCAR_AUTH_BASE_PATH", ""),
            web_upstream=os.environ.get(
                "DCAR_AUTH_WEB_UPSTREAM", "http://127.0.0.1:4174"
            ).rstrip("/"),
            api_upstream=os.environ.get(
                "DCAR_AUTH_API_UPSTREAM", "http://127.0.0.1:8765"
            ).rstrip("/"),
            htpasswd_path=Path(
                os.environ.get(
                    "DCAR_AUTH_HTPASSWD",
                    str(PROJECT_ROOT / "runtime" / "auth" / "users.htpasswd"),
                )
            ),
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
            secure_cookie=_enabled("DCAR_AUTH_SECURE_COOKIE", default=True),
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
        )

    @property
    def cookie_path(self) -> str:
        return self.base_path or "/"

    def route(self, path: str) -> str:
        return f"{self.base_path}{path}"


class HtpasswdVerifier:
    """Read and verify the SHA-512 crypt format used by the live account file."""

    def __init__(self, path: Path) -> None:
        self.path = path

    def validate_source(self) -> None:
        entries = self._entries()
        if not entries:
            raise RuntimeError(f"authentication account file is empty: {self.path}")
        if any(not value.startswith("$6$") for value in entries.values()):
            raise RuntimeError(
                "authentication account file must use SHA-512 crypt ($6$) hashes"
            )

    def verify(self, username: str, password: str) -> tuple[bool, str]:
        entries = self._entries()
        stored = entries.get(username)
        candidate = stored or DUMMY_SHA512_HASH
        try:
            valid = sha512_crypt.verify(password, candidate)
        except (TypeError, ValueError):
            valid = False
        if stored is None or not valid:
            return False, ""
        return True, self._fingerprint(stored)

    def credential_is_current(self, username: str, fingerprint: str) -> bool:
        stored = self._entries().get(username)
        return stored is not None and hmac.compare_digest(
            self._fingerprint(stored), fingerprint
        )

    def _entries(self) -> dict[str, str]:
        if not self.path.is_file():
            raise RuntimeError(f"authentication account file is missing: {self.path}")
        entries: dict[str, str] = {}
        for raw_line in self.path.read_text(encoding="utf-8").splitlines():
            line = raw_line.strip()
            if not line or line.startswith("#") or ":" not in line:
                continue
            username, password_hash = line.split(":", 1)
            if username:
                entries[username] = password_hash
        return entries

    @staticmethod
    def _fingerprint(stored_hash: str) -> str:
        return hashlib.sha256(stored_hash.encode("utf-8")).hexdigest()


class SessionStore:
    """Server-side session registry; browser cookies contain only opaque tokens."""

    def __init__(self, path: Path) -> None:
        self.path = path

    def initialize(self) -> None:
        self.path.parent.mkdir(parents=True, exist_ok=True, mode=0o700)
        try:
            self.path.parent.chmod(0o700)
        except OSError:
            pass
        with self._connect() as connection:
            connection.execute(
                """
                CREATE TABLE IF NOT EXISTS auth_sessions(
                    token_sha256 TEXT PRIMARY KEY,
                    username TEXT NOT NULL,
                    credential_fingerprint TEXT NOT NULL DEFAULT '',
                    created_at INTEGER NOT NULL,
                    expires_at INTEGER NOT NULL
                )
                """
            )
            columns = {
                str(row["name"])
                for row in connection.execute("PRAGMA table_info(auth_sessions)")
            }
            if "credential_fingerprint" not in columns:
                connection.execute(
                    "ALTER TABLE auth_sessions ADD COLUMN credential_fingerprint TEXT NOT NULL DEFAULT ''"
                )
            connection.execute(
                "CREATE INDEX IF NOT EXISTS auth_sessions_expiry ON auth_sessions(expires_at)"
            )
        try:
            self.path.chmod(0o600)
        except OSError:
            pass

    def create(self, username: str, credential_fingerprint: str, ttl_seconds: int) -> str:
        now = int(time.time())
        token = secrets.token_urlsafe(32)
        with self._connect() as connection:
            connection.execute("DELETE FROM auth_sessions WHERE expires_at<=?", (now,))
            connection.execute(
                """
                INSERT INTO auth_sessions(
                    token_sha256,username,credential_fingerprint,created_at,expires_at
                ) VALUES(?,?,?,?,?)
                """,
                (
                    self._token_hash(token),
                    username,
                    credential_fingerprint,
                    now,
                    now + ttl_seconds,
                ),
            )
        return token

    def resolve(self, token: str) -> Optional[tuple[str, str]]:
        if not token or len(token) > 256:
            return None
        now = int(time.time())
        token_hash = self._token_hash(token)
        with self._connect() as connection:
            row = connection.execute(
                """
                SELECT username,credential_fingerprint,expires_at
                FROM auth_sessions WHERE token_sha256=?
                """,
                (token_hash,),
            ).fetchone()
            if row is None:
                return None
            if int(row["expires_at"]) <= now:
                connection.execute(
                    "DELETE FROM auth_sessions WHERE token_sha256=?", (token_hash,)
                )
                return None
            return str(row["username"]), str(row["credential_fingerprint"])

    def revoke(self, token: str) -> None:
        if not token or len(token) > 256:
            return
        with self._connect() as connection:
            connection.execute(
                "DELETE FROM auth_sessions WHERE token_sha256=?",
                (self._token_hash(token),),
            )

    def healthcheck(self) -> None:
        with self._connect() as connection:
            connection.execute("SELECT 1").fetchone()

    def _connect(self) -> sqlite3.Connection:
        connection = sqlite3.connect(self.path, timeout=10)
        connection.row_factory = sqlite3.Row
        return connection

    @staticmethod
    def _token_hash(token: str) -> str:
        return hashlib.sha256(token.encode("utf-8")).hexdigest()


class LoginAttemptLimiter:
    def __init__(self, window_seconds: int, max_failures: int) -> None:
        self.window_seconds = window_seconds
        self.max_failures = max_failures
        self._failures: dict[str, list[float]] = {}
        self._lock = threading.Lock()

    @staticmethod
    def keys(username: str, client_ip: str) -> tuple[str, str]:
        return f"user:{username.lower()}", f"ip:{client_ip}"

    def retry_after(self, keys: tuple[str, str]) -> int:
        now = time.time()
        with self._lock:
            for key in keys:
                recent = [
                    stamp
                    for stamp in self._failures.get(key, [])
                    if now - stamp < self.window_seconds
                ]
                self._failures[key] = recent
                if len(recent) >= self.max_failures:
                    return max(1, int(self.window_seconds - (now - recent[0])) + 1)
        return 0

    def record_failure(self, keys: tuple[str, str]) -> None:
        now = time.time()
        with self._lock:
            if len(self._failures) > 4096:
                self._failures.clear()
            for key in keys:
                recent = [
                    stamp
                    for stamp in self._failures.get(key, [])
                    if now - stamp < self.window_seconds
                ]
                recent.append(now)
                self._failures[key] = recent[-64:]

    def clear(self, keys: tuple[str, str]) -> None:
        with self._lock:
            for key in keys:
                self._failures.pop(key, None)


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


async def _form_payload(request: Request) -> Optional[Mapping[str, str]]:
    body = await request.body()
    if len(body) > MAX_LOGIN_BODY_BYTES:
        return None
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
) -> FastAPI:
    resolved = config or AuthGatewayConfig.from_env()
    verifier = HtpasswdVerifier(resolved.htpasswd_path)
    sessions = SessionStore(resolved.session_db_path)
    limiter = LoginAttemptLimiter(
        resolved.throttle_window_seconds, resolved.throttle_max_failures
    )

    @asynccontextmanager
    async def lifespan(application: FastAPI):
        if not resolved.bypass_auth:
            verifier.validate_source()
            if not resolved.login_template_path.is_file():
                raise RuntimeError(
                    f"login template is missing: {resolved.login_template_path}"
                )
            sessions.initialize()
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

    application = FastAPI(
        title="Dcar Sentinel authentication gateway",
        docs_url=None,
        redoc_url=None,
        openapi_url=None,
        lifespan=lifespan,
    )

    async def username_for(request: Request) -> Optional[str]:
        if resolved.bypass_auth:
            return BYPASS_USERNAME
        token = request.cookies.get(SESSION_COOKIE, "")
        session = await asyncio.to_thread(sessions.resolve, token)
        if session is None:
            return None
        username, fingerprint = session
        try:
            is_current = await asyncio.to_thread(
                verifier.credential_is_current, username, fingerprint
            )
        except (OSError, RuntimeError):
            LOGGER.exception("credential source became unavailable")
            return None
        if not is_current:
            await asyncio.to_thread(sessions.revoke, token)
            return None
        return username

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
                    {"detail": "系统暂时无法加载数据，请稍后重试"}, status_code=502
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

    @application.api_route("/{path:path}", methods=PROXY_METHODS)
    async def gateway(request: Request, path: str) -> Response:
        del path
        stripped = _stripped_path(request.url.path, resolved.base_path)
        if stripped is None:
            return Response(status_code=404)
        if _is_douyin_path(stripped) and request.method not in {"GET", "POST"}:
            return _no_store(
                Response(status_code=405, headers={"Allow": "GET, POST"})
            )
        if stripped == DOUYIN_CALLBACK_PATH and request.method != "GET":
            return _no_store(Response(status_code=405, headers={"Allow": "GET"}))

        if stripped == "/auth/health":
            if request.method != "GET":
                return Response(status_code=405)
            if not resolved.bypass_auth:
                try:
                    await asyncio.to_thread(verifier.validate_source)
                    await asyncio.to_thread(sessions.healthcheck)
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
            if await username_for(request):
                return _no_store(RedirectResponse(return_to, status_code=303))
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
            if not _same_origin_post(
                request, "login", trusted_proxy=bool(resolved.base_path)
            ):
                return _no_store(
                    JSONResponse(
                        {"detail": "登录页面已失效，请刷新后重新登录"},
                        status_code=403,
                    )
                )
            payload = await _form_payload(request)
            if payload is None:
                return _no_store(
                    JSONResponse(
                        {"detail": "登录信息太长，请刷新页面后重新输入"}, status_code=413
                    )
                )
            username = payload.get("username", "").strip()
            password = payload.get("password", "")
            return_to = _safe_return_to(payload.get("return_to", ""), resolved)
            if not username or len(username) > 128 or not password or len(password) > 4096:
                return _no_store(
                    JSONResponse({"detail": "账号或密码不正确"}, status_code=401)
                )
            client_ip = request.headers.get("x-real-ip") or (
                request.client.host if request.client else "unknown"
            )
            throttle_keys = limiter.keys(username, client_ip)
            retry_after = limiter.retry_after(throttle_keys)
            if retry_after:
                return _no_store(
                    JSONResponse(
                        {"detail": "尝试次数太多，请稍后再登录"},
                        status_code=429,
                        headers={"Retry-After": str(retry_after)},
                    )
                )
            try:
                valid, fingerprint = await asyncio.to_thread(
                    verifier.verify, username, password
                )
            except (OSError, RuntimeError):
                LOGGER.exception("credential source unavailable during login")
                return _no_store(
                    JSONResponse(
                        {"detail": "暂时无法登录，请稍后重试"}, status_code=503
                    )
                )
            if not valid:
                limiter.record_failure(throttle_keys)
                if resolved.failure_delay_seconds:
                    await asyncio.sleep(resolved.failure_delay_seconds)
                return _no_store(
                    JSONResponse({"detail": "账号或密码不正确"}, status_code=401)
                )
            limiter.clear(throttle_keys)
            remember = payload.get("remember") == "1"
            ttl = (
                resolved.remember_session_seconds
                if remember
                else resolved.session_seconds
            )
            token = await asyncio.to_thread(
                sessions.create, username, fingerprint, ttl
            )
            login_success_response = JSONResponse({"redirect_to": return_to})
            login_success_response.set_cookie(
                SESSION_COOKIE,
                token,
                max_age=ttl if remember else None,
                path=resolved.cookie_path,
                secure=resolved.secure_cookie,
                httponly=True,
                samesite="lax",
            )
            return _no_store(login_success_response)

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
            await asyncio.to_thread(sessions.revoke, token)
            logout_response = JSONResponse({"redirect_to": resolved.route("/login")})
            _delete_session_cookie(logout_response, resolved)
            return _no_store(logout_response)

        if stripped == "/auth/session":
            if request.method != "GET":
                return Response(status_code=405)
            authenticated_username = await username_for(request)
            if not authenticated_username:
                return unauthenticated(request, "/api/auth/session")
            return _no_store(
                JSONResponse(
                    {"authenticated": True, "username": authenticated_username}
                )
            )

        if stripped.startswith("/auth/"):
            return Response(status_code=404)

        authenticated_username = await username_for(request)
        if not authenticated_username:
            return unauthenticated(request, stripped)

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
                buffered_body = await request.body()
                if len(buffered_body) > MAX_DOUYIN_BODY_BYTES:
                    return _no_store(
                        JSONResponse({"detail": "请求内容过大"}, status_code=413)
                    )

            token = request.cookies.get(SESSION_COOKIE, "")
            extra_headers = {
                "X-Dcar-Edge-Key": request.app.state.douyin_edge_key,
                "X-Dcar-Session-Binding": SessionStore._token_hash(token),
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
                _no_store(response)
            return response

        if stripped == "/api" or stripped.startswith("/api/"):
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
