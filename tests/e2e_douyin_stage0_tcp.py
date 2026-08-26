from __future__ import annotations

import base64
import hashlib
import json
import os
import secrets
import signal
import socket
import sqlite3
import subprocess
import sys
import tempfile
import time
from pathlib import Path
from typing import Any
from urllib.parse import parse_qs, urlsplit

import httpx
from cryptography.fernet import Fernet
from fastapi import FastAPI, Request
from fastapi.responses import JSONResponse
from passlib.hash import sha512_crypt  # type: ignore[import-untyped]


ROOT = Path(__file__).resolve().parents[1]
TESTS_ROOT = ROOT / "tests"
SRC_ROOT = ROOT / "src" / "dcar_eval"
HOST = "::1"
GATEWAY_URL = "http://[::1]:4173"
CONTROL_URL = "http://[::1]:4175"
OAUTH_URL = "http://[::1]:4199"
API_URL = "http://[::1]:8765"
USERNAME = "stage0-operator"
PASSWORD = "stage0-test-password"
PLATFORM_UID = "123456789"
CLIENT_KEY = "stage0-mock-client-key"
CLIENT_SECRET = "stage0-mock-client-secret-32-bytes-minimum"
OPEN_ID = "stage0-mock-open-id"


sys.path.insert(0, str(ROOT))
sys.path.insert(0, str(SRC_ROOT))

from tests.fixtures.mock_douyin_oauth import (  # noqa: E402
    MockDouyinState,
    create_mock_douyin_oauth,
)


ACCOUNT_CALLS: list[dict[str, Any]] = []
accounts_app = FastAPI(redirect_slashes=False)


@accounts_app.get("/__e2e__/health")
async def accounts_health() -> dict[str, str]:
    return {"status": "ok"}


@accounts_app.get("/__e2e__/calls")
async def accounts_calls() -> dict[str, Any]:
    return {"calls": ACCOUNT_CALLS}


@accounts_app.post("/api/v8/accounts/search")
async def accounts_search(request: Request) -> JSONResponse:
    payload = await request.json()
    ACCOUNT_CALLS.append(payload)
    return JSONResponse(
        {
            "items": [
                {
                    "id": 7,
                    "operator_name": "阶段0运营账号",
                    "enabled": True,
                    "phone": "13800138000",
                    "platforms": [
                        {
                            "platform": "douyin",
                            "uid": PLATFORM_UID,
                            "nickname": "正式抖音账号",
                        },
                        {
                            "platform": "xiaohongshu",
                            "uid": "xhs-e2e-secret",
                            "nickname": "不应透传",
                        },
                    ],
                }
            ],
            "total": 1,
            "page": int(payload.get("page", 1)),
            "page_size": int(payload.get("page_size", 50)),
            "pending_platform_identities": [
                {"platform": "douyin", "uid": "pending-e2e-secret"}
            ],
        }
    )


OAUTH_STATE = MockDouyinState(
    CLIENT_KEY,
    CLIENT_SECRET,
    open_id=OPEN_ID,
    nickname="Mock 抖音账号",
)
oauth_app = create_mock_douyin_oauth(OAUTH_STATE)


@oauth_app.get("/__e2e__/state")
async def oauth_state() -> dict[str, Any]:
    return {
        "status": "ok",
        "token_calls": OAUTH_STATE.token_calls,
        "userinfo_calls": OAUTH_STATE.userinfo_calls,
    }


class ManagedProcess:
    def __init__(
        self,
        *,
        name: str,
        app: str,
        app_dir: Path,
        port: int,
        environment: dict[str, str],
        log_path: Path,
    ) -> None:
        self.name = name
        self.log_path = log_path
        self._log = log_path.open("wb")
        self.process = subprocess.Popen(
            [
                sys.executable,
                "-m",
                "uvicorn",
                app,
                "--app-dir",
                str(app_dir),
                "--host",
                HOST,
                "--port",
                str(port),
                "--workers",
                "1",
                "--no-access-log",
                "--log-level",
                "warning",
            ],
            cwd=ROOT,
            env=environment,
            stdin=subprocess.DEVNULL,
            stdout=self._log,
            stderr=subprocess.STDOUT,
            start_new_session=True,
        )

    def stop(self) -> None:
        if self.process.poll() is None:
            os.killpg(self.process.pid, signal.SIGTERM)
            try:
                self.process.wait(timeout=8)
            except subprocess.TimeoutExpired:
                os.killpg(self.process.pid, signal.SIGKILL)
                self.process.wait(timeout=5)
        self._log.close()

    def assert_running(self) -> None:
        status = self.process.poll()
        if status is not None:
            self._log.flush()
            detail = self.log_path.read_text(encoding="utf-8", errors="replace")
            raise RuntimeError(f"{self.name} exited with {status}:\n{detail[-4000:]}")


def assert_equal(actual: Any, expected: Any, label: str) -> None:
    if actual != expected:
        raise AssertionError(f"{label}: expected {expected!r}, got {actual!r}")


def assert_absent(payload: str | bytes, values: list[str | bytes], label: str) -> None:
    for value in values:
        if isinstance(payload, bytes):
            byte_needle = value if isinstance(value, bytes) else value.encode("utf-8")
            found = byte_needle in payload
        else:
            text_needle = value.decode("utf-8") if isinstance(value, bytes) else value
            found = text_needle in payload
        if found:
            raise AssertionError(f"{label} contains forbidden canary")


def preflight_ports() -> None:
    for port in (4173, 4175, 4199, 8765):
        probe = socket.socket(socket.AF_INET6, socket.SOCK_STREAM)
        try:
            probe.setsockopt(socket.IPPROTO_IPV6, socket.IPV6_V6ONLY, 1)
            probe.bind((HOST, port))
        except OSError as exc:
            raise RuntimeError(f"IPv6 loopback port {port} is unavailable") from exc
        finally:
            probe.close()


def wait_for(
    client: httpx.Client,
    path: str,
    processes: list[ManagedProcess],
    *,
    headers: dict[str, str] | None = None,
) -> httpx.Response:
    deadline = time.monotonic() + 20
    last_error: Exception | None = None
    while time.monotonic() < deadline:
        for process in processes:
            process.assert_running()
        try:
            response = client.get(path, headers=headers)
            if response.status_code < 500:
                return response
        except httpx.HTTPError as exc:
            last_error = exc
        time.sleep(0.1)
    raise RuntimeError(f"timed out waiting for {path}: {last_error}")


def common_environment() -> dict[str, str]:
    environment = os.environ.copy()
    environment["PYTHONPATH"] = os.pathsep.join((str(SRC_ROOT), str(ROOT)))
    environment["NO_PROXY"] = "::1,localhost,127.0.0.1"
    environment["no_proxy"] = environment["NO_PROXY"]
    for name in ("HTTP_PROXY", "HTTPS_PROXY", "ALL_PROXY", "http_proxy", "https_proxy"):
        environment.pop(name, None)
    return environment


def run() -> dict[str, Any]:
    preflight_ports()
    with tempfile.TemporaryDirectory(prefix="dcar-stage0-e2e-") as temporary:
        state_root = Path(temporary)
        credentials = state_root / "credentials"
        credentials.mkdir(mode=0o700)
        vault_root = state_root / "vault"
        vault_root.mkdir(mode=0o700)
        edge_key = secrets.token_urlsafe(48)
        machine_key = secrets.token_urlsafe(48)
        (credentials / "douyin-edge-key").write_text(edge_key, encoding="utf-8")
        (credentials / "douyin-machine-key").write_text(
            machine_key, encoding="utf-8"
        )
        (credentials / "douyin-fernet-keyring").write_text(
            f"1:{Fernet.generate_key().decode('ascii')}\n", encoding="utf-8"
        )
        (credentials / "douyin-open-id-hmac-key").write_text(
            base64.urlsafe_b64encode(os.urandom(32)).decode("ascii"),
            encoding="utf-8",
        )
        for path in credentials.iterdir():
            path.chmod(0o600)

        client_secret_path = state_root / "mock-client-secret"
        client_secret_path.write_text(CLIENT_SECRET, encoding="utf-8")
        client_secret_path.chmod(0o600)
        htpasswd_path = state_root / "users.htpasswd"
        htpasswd_path.write_text(
            f"{USERNAME}:{sha512_crypt.hash(PASSWORD, rounds=5000)}\n",
            encoding="utf-8",
        )
        htpasswd_path.chmod(0o600)
        session_db = state_root / "sessions.sqlite3"
        vault_path = vault_root / "vault.sqlite3"

        base_environment = common_environment()
        fixture_environment = base_environment.copy()
        control_environment = base_environment | {
            "CREDENTIALS_DIRECTORY": str(credentials),
            "DCAR_DOUYIN_PUBLIC_BASE_PATH": "/dcar",
            "DCAR_DOUYIN_VAULT": str(vault_path),
            "DCAR_DOUYIN_API_UPSTREAM": API_URL,
            "DOUYIN_AUTHORIZATION_ENABLED": "1",
            "DCAR_DOUYIN_PROVIDER": "mock",
            "DCAR_DOUYIN_CLIENT_KEY": CLIENT_KEY,
            "DCAR_DOUYIN_CLIENT_SECRET_FILE": str(client_secret_path),
            "DCAR_DOUYIN_PROVIDER_AUTHORIZE_URL": (
                f"{OAUTH_URL}/platform/oauth/connect/"
            ),
            "DCAR_DOUYIN_PROVIDER_TOKEN_URL": f"{OAUTH_URL}/oauth/access_token/",
            "DCAR_DOUYIN_PROVIDER_USERINFO_URL": f"{OAUTH_URL}/oauth/userinfo/",
            "DCAR_DOUYIN_CALLBACK_URL": (
                f"{GATEWAY_URL}/dcar/oauth/douyin/callback"
            ),
        }
        gateway_environment = base_environment | {
            "CREDENTIALS_DIRECTORY": str(credentials),
            "DCAR_AUTH_BASE_PATH": "/dcar",
            "DCAR_AUTH_WEB_UPSTREAM": API_URL,
            "DCAR_AUTH_API_UPSTREAM": API_URL,
            "DCAR_AUTH_DOUYIN_UPSTREAM": CONTROL_URL,
            "DCAR_AUTH_HTPASSWD": str(htpasswd_path),
            "DCAR_AUTH_SESSION_DB": str(session_db),
            "DCAR_AUTH_LOGIN_TEMPLATE": str(
                ROOT / "deploy" / "server" / "nginx" / "login.html"
            ),
            "DCAR_AUTH_SECURE_COOKIE": "0",
            "DCAR_AUTH_FAILURE_DELAY_SECONDS": "0",
        }

        processes: list[ManagedProcess] = []
        clients: list[httpx.Client] = []
        captured_state = ""
        captured_code = ""
        session_token = ""
        try:
            for process in (
                ManagedProcess(
                    name="accounts",
                    app="e2e_douyin_stage0_tcp:accounts_app",
                    app_dir=TESTS_ROOT,
                    port=8765,
                    environment=fixture_environment,
                    log_path=state_root / "accounts.log",
                ),
                ManagedProcess(
                    name="oauth",
                    app="e2e_douyin_stage0_tcp:oauth_app",
                    app_dir=TESTS_ROOT,
                    port=4199,
                    environment=fixture_environment,
                    log_path=state_root / "oauth.log",
                ),
                ManagedProcess(
                    name="control",
                    app="dcar_douyin_control.app:app",
                    app_dir=SRC_ROOT,
                    port=4175,
                    environment=control_environment,
                    log_path=state_root / "control.log",
                ),
                ManagedProcess(
                    name="gateway",
                    app="dcar_auth.gateway:app",
                    app_dir=SRC_ROOT,
                    port=4173,
                    environment=gateway_environment,
                    log_path=state_root / "gateway.log",
                ),
            ):
                processes.append(process)

            transport = httpx.HTTPTransport(retries=0)
            gateway = httpx.Client(
                base_url=GATEWAY_URL,
                follow_redirects=False,
                trust_env=False,
                transport=transport,
                timeout=10,
            )
            control = httpx.Client(
                base_url=CONTROL_URL,
                follow_redirects=False,
                trust_env=False,
                timeout=10,
            )
            oauth = httpx.Client(
                base_url=OAUTH_URL,
                follow_redirects=False,
                trust_env=False,
                timeout=10,
            )
            accounts = httpx.Client(
                base_url=API_URL,
                follow_redirects=False,
                trust_env=False,
                timeout=10,
            )
            clients.extend((gateway, control, oauth, accounts))

            assert_equal(
                wait_for(accounts, "/__e2e__/health", processes).status_code,
                200,
                "accounts health",
            )
            assert_equal(
                wait_for(oauth, "/__e2e__/state", processes).status_code,
                200,
                "oauth health",
            )
            health = wait_for(
                control,
                "/internal/v1/health",
                processes,
                headers={"X-Dcar-Machine-Key": machine_key},
            )
            assert_equal(health.status_code, 200, "control health")
            assert_equal(health.json()["vault"]["journal_mode"], "delete", "journal")
            assert_equal(
                wait_for(gateway, "/dcar/auth/health", processes).status_code,
                200,
                "gateway health",
            )

            assert_equal(control.get("/douyin").status_code, 403, "direct control")
            assert_equal(
                control.get(
                    "/internal/v1/health", headers={"X-Dcar-Edge-Key": edge_key}
                ).status_code,
                403,
                "credential mutual exclusion",
            )
            forged = gateway.get(
                "/dcar/douyin",
                headers={"X-Dcar-Authenticated-User": USERNAME},
            )
            assert_equal(forged.status_code, 302, "forged identity")

            login = gateway.post(
                "/dcar/auth/login",
                data={
                    "username": USERNAME,
                    "password": PASSWORD,
                    "remember": "0",
                    "return_to": "/dcar/accounts/douyin-authorization",
                },
                headers={"Origin": GATEWAY_URL, "X-Dcar-Request": "login"},
            )
            assert_equal(login.status_code, 200, "login")
            assert_equal(
                login.json()["redirect_to"],
                "/dcar/accounts/douyin-authorization",
                "login target",
            )
            set_cookie = login.headers["set-cookie"].lower()
            for attribute in ("httponly", "samesite=lax", "path=/dcar"):
                if attribute not in set_cookie:
                    raise AssertionError(f"login cookie missing {attribute}")
            session_token = gateway.cookies.get("dcar_session") or ""
            if len(session_token) < 32:
                raise AssertionError("gateway did not issue a strong session token")

            page = gateway.get("/dcar/douyin")
            assert_equal(page.status_code, 303, "legacy control entry")
            assert_equal(
                page.headers["location"],
                "/dcar/accounts/douyin-authorization",
                "authorization page target",
            )
            assert_equal(page.headers["cache-control"], "no-store", "entry cache")

            start = gateway.post(
                "/dcar/api/douyin/oauth/start",
                headers={
                    "Origin": GATEWAY_URL,
                    "Content-Type": "application/json",
                    "X-Dcar-Request": "douyin-oauth-start",
                },
                json={"account_id": 7, "platform_uid": PLATFORM_UID},
            )
            assert_equal(start.status_code, 200, "oauth start")
            authorize_url = start.json()["authorize_url"]
            captured_state = parse_qs(urlsplit(authorize_url).query)["state"][0]
            state_digest = hashlib.sha256(captured_state.encode("utf-8")).hexdigest()
            with sqlite3.connect(vault_path) as connection:
                targeted_state = connection.execute(
                    "SELECT account_id,platform_uid FROM oauth_states "
                    "WHERE state_digest=?",
                    (state_digest,),
                ).fetchone()
            assert_equal(
                targeted_state,
                (7, PLATFORM_UID),
                "oauth state target",
            )
            authorized = oauth.get(authorize_url)
            assert_equal(authorized.status_code, 303, "mock authorize")
            callback_url = authorized.headers["location"]
            callback_query = parse_qs(urlsplit(callback_url).query)
            captured_code = callback_query["code"][0]
            callback = gateway.get(
                callback_url,
                headers={
                    "Sec-Fetch-Mode": "navigate",
                    "Sec-Fetch-Dest": "document",
                    "Sec-Fetch-Site": "cross-site",
                },
            )
            assert_equal(callback.status_code, 303, "oauth callback")
            callback_location = urlsplit(callback.headers["location"])
            assert_equal(
                callback_location.path,
                "/dcar/accounts/douyin-authorization",
                "callback target",
            )
            assert_equal(
                parse_qs(callback_location.query),
                {
                    "account_id": ["7"],
                    "platform_uid": [PLATFORM_UID],
                    "notice": ["oauth-completed"],
                },
                "callback target query",
            )
            assert_equal(callback.headers["cache-control"], "no-store", "callback cache")
            assert_equal(
                callback.headers["referrer-policy"], "no-referrer", "callback referrer"
            )
            assert_absent(
                callback.headers["location"] + callback.text,
                [captured_state, captured_code, "access-", "refresh-"],
                "callback response",
            )

            provider_state = oauth.get("/__e2e__/state").json()
            assert_equal(provider_state["token_calls"], 1, "token exchange count")
            assert_equal(provider_state["userinfo_calls"], 1, "userinfo count")
            calls = accounts.get("/__e2e__/calls").json()["calls"]
            assert_equal(len(calls), 2, "target validation count")
            for call in calls:
                assert_equal(call["query"], PLATFORM_UID, "target query forwarding")
                assert_equal(call["platform"], "douyin", "platform enforcement")

            listed = gateway.get("/dcar/api/douyin/authorizations")
            assert_equal(listed.status_code, 200, "authorization list")
            assert_equal(len(listed.json()["items"]), 1, "authorization count")
            authorization = listed.json()["items"][0]
            assert_equal(authorization["status"], "active", "targeted binding state")
            assert_equal(authorization["account_id"], 7, "targeted binding account")
            assert_equal(
                authorization["platform_uid"],
                PLATFORM_UID,
                "targeted binding platform uid",
            )
            assert_absent(
                json.dumps(listed.json()),
                [OPEN_ID, "access-", "refresh-", CLIENT_SECRET],
                "authorization list",
            )
            statuses = gateway.get("/dcar/api/douyin/authorization-statuses")
            assert_equal(statuses.status_code, 200, "authorization statuses")
            assert_equal(len(statuses.json()["items"]), 1, "status count")
            status = statuses.json()["items"][0]
            assert_equal(status["id"], authorization["id"], "status authorization")
            assert_equal(status["account_id"], 7, "status account")
            assert_equal(status["platform_uid"], PLATFORM_UID, "status platform uid")
            assert_equal(status["status"], "active", "status state")
            assert_equal(status["authorized"], True, "authorized projection")
            assert_absent(
                json.dumps(statuses.json()),
                [OPEN_ID, "access-", "refresh-", CLIENT_SECRET],
                "authorization statuses",
            )

            video_page = control.post(
                "/internal/v1/video-list/page",
                headers={"X-Dcar-Machine-Key": machine_key},
                json={
                    "authorization_id": authorization["id"],
                    "cursor": 0,
                    "count": 20,
                },
            )
            assert_equal(video_page.status_code, 503, "separate video-list route")
            assert_equal(
                video_page.json()["detail"],
                "douyin_provider_unavailable",
                "mock mode video-list boundary",
            )

            old_confirm_page = gateway.get("/dcar/douyin/confirm")
            assert_equal(old_confirm_page.status_code, 404, "old confirm page")
            for path, action in (
                ("/dcar/api/douyin/oauth/confirm", "douyin-oauth-confirm"),
                ("/dcar/api/douyin/oauth/reject", "douyin-oauth-reject"),
            ):
                obsolete = gateway.post(
                    path,
                    headers={
                        "Origin": GATEWAY_URL,
                        "Content-Type": "application/json",
                        "X-Dcar-Request": action,
                    },
                    json={},
                )
                assert_equal(obsolete.status_code, 404, f"obsolete route {path}")

            with sqlite3.connect(vault_path) as connection:
                token_row = connection.execute(
                    "SELECT typeof(access_token_ciphertext),"
                    "typeof(refresh_token_ciphertext),"
                    "length(access_token_ciphertext),length(refresh_token_ciphertext) "
                    "FROM douyin_authorizations WHERE id=?",
                    (authorization["id"],),
                ).fetchone()
                assert_equal(token_row[:2], ("blob", "blob"), "ciphertext types")
                if min(int(token_row[2]), int(token_row[3])) < 32:
                    raise AssertionError("encrypted tokens are unexpectedly short")

            replay = gateway.get(
                callback_url,
                headers={
                    "Sec-Fetch-Mode": "navigate",
                    "Sec-Fetch-Dest": "document",
                    "Sec-Fetch-Site": "cross-site",
                },
            )
            assert_equal(replay.status_code, 303, "callback replay")
            provider_state = oauth.get("/__e2e__/state").json()
            assert_equal(provider_state["token_calls"], 1, "replay token count")

            unbound = gateway.post(
                "/dcar/api/douyin/authorizations/unbind",
                headers={
                    "Origin": GATEWAY_URL,
                    "Content-Type": "application/json",
                    "X-Dcar-Request": "douyin-authorization-unbind",
                },
                json={
                    "authorization_id": authorization["id"],
                    "expected_version": authorization["version"],
                },
            )
            assert_equal(unbound.status_code, 200, "authorization unbind")
            after_unbind = gateway.get("/dcar/api/douyin/authorizations").json()
            assert_equal(after_unbind["items"][0]["status"], "unbound", "unbind state")
            after_unbind_statuses = gateway.get(
                "/dcar/api/douyin/authorization-statuses"
            )
            assert_equal(
                after_unbind_statuses.status_code,
                200,
                "authorization statuses after unbind",
            )
            unbound_status = after_unbind_statuses.json()["items"][0]
            assert_equal(unbound_status["status"], "unbound", "unbound projection")
            assert_equal(
                unbound_status["authorized"], False, "unauthorized projection"
            )

            with sqlite3.connect(vault_path) as connection:
                assert_equal(
                    connection.execute("PRAGMA quick_check").fetchone()[0],
                    "ok",
                    "vault quick_check",
                )
                assert_equal(
                    connection.execute("PRAGMA journal_mode").fetchone()[0].lower(),
                    "delete",
                    "vault journal",
                )
            for suffix in ("-wal", "-shm"):
                if Path(str(vault_path) + suffix).exists():
                    raise AssertionError(f"Vault sidecar exists: {suffix}")
            assert_absent(
                vault_path.read_bytes(),
                [
                    captured_state,
                    captured_code,
                    OPEN_ID,
                    "access-",
                    "refresh-",
                    CLIENT_SECRET,
                ],
                "Vault bytes",
            )
            assert_absent(session_db.read_bytes(), [session_token], "Session DB bytes")
        finally:
            for client in clients:
                client.close()
            for process in reversed(processes):
                process.stop()
            forbidden_logs = [
                captured_state,
                captured_code,
                OPEN_ID,
                "access-",
                "refresh-",
                CLIENT_SECRET,
            ]
            for log_path in state_root.glob("*.log"):
                log_text = log_path.read_text(encoding="utf-8", errors="replace")
                assert_absent(
                    log_text,
                    [value for value in forbidden_logs if value],
                    f"{log_path.name} logs",
                )

        return {
            "status": "ok",
            "topology": "gateway->control->api+oauth over IPv6 loopback TCP",
            "oauth_token_calls": 1,
            "oauth_userinfo_calls": 1,
            "vault_journal_mode": "delete",
            "external_requests": 0,
            "cost_cny": 0,
        }


if __name__ == "__main__":
    print(json.dumps(run(), ensure_ascii=False, sort_keys=True))
