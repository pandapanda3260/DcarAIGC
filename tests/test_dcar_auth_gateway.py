from __future__ import annotations

import asyncio
import gzip
import hashlib
import json
import logging
import os
import sqlite3
import sys
import tempfile
import threading
import unittest
from concurrent.futures import ThreadPoolExecutor
from contextlib import ExitStack, contextmanager
from pathlib import Path
from typing import Iterator
from unittest.mock import patch

import httpx
from fastapi import FastAPI, Request
from fastapi.responses import JSONResponse, Response
from fastapi.testclient import TestClient
from httpx import ASGITransport
from passlib.hash import sha512_crypt


REPOSITORY_ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(REPOSITORY_ROOT / "src" / "dcar_eval"))

from dcar_auth import gateway as auth_gateway  # noqa: E402
from dcar_auth import sms as auth_sms  # noqa: E402
from dcar_auth import store as auth_store  # noqa: E402


ORIGIN = "https://dcar.test"
USERNAME = "operator"
PASSWORD = "correct-password"
PHONE = "13800138000"
NEW_PHONE = "13900139000"
EDGE_KEY = "gateway-edge-test-key-32-bytes-minimum"
PEPPER = "0123456789abcdef0123456789abcdef0123456789abcdef"
COMPRESSED_BODY = b"compressed proxy response\n" * 128


def _route(base_path: str, path: str) -> str:
    return f"{base_path}{path}"


def _echo_upstream(name: str) -> FastAPI:
    app = FastAPI()

    @app.api_route(
        "/{path:path}",
        methods=["GET", "HEAD", "POST", "PUT", "PATCH", "DELETE", "OPTIONS"],
    )
    async def echo(request: Request, path: str) -> JSONResponse:
        if request.url.path.endswith("/internal-redirect"):
            return Response(
                status_code=307,
                headers={
                    "Location": (
                        f"http://{name}.test/dcar/overview"
                        "?window=this-week#summary"
                    )
                },
            )
        if request.url.path.endswith("/external-redirect"):
            return Response(
                status_code=307,
                headers={"Location": "https://docs.example/help"},
            )
        if request.url.path.endswith("/relative-redirect"):
            return Response(
                status_code=307,
                headers={"Location": "/dcar/overview"},
            )
        if request.url.path.endswith("/stripped-redirect"):
            destination = {
                "api": "/api/v8/tasks/",
                "douyin": "/douyin/confirm",
                "web": "/assets/app.css",
            }[name]
            return Response(
                status_code=307,
                headers={"Location": f"http://{name}.test{destination}"},
            )
        if request.url.path.endswith("/set-cookie"):
            response = JSONResponse({"upstream": name})
            response.headers.append(
                "Set-Cookie", "dcar_session=evil; Path=/dcar; HttpOnly"
            )
            response.headers.append(
                "Set-Cookie", "douyin_upstream=evil; Path=/dcar"
            )
            return response
        if request.url.path.endswith("/compressed.txt"):
            return Response(
                gzip.compress(COMPRESSED_BODY),
                media_type="text/plain",
                headers={"Content-Encoding": "gzip", "ETag": '"compressed"'},
            )
        if request.url.path.endswith("/range.bin"):
            if request.headers.get("range") != "bytes=0-3":
                return Response(status_code=400)
            return Response(
                b"0123",
                status_code=206,
                media_type="application/octet-stream",
                headers={
                    "Accept-Ranges": "bytes",
                    "Content-Range": "bytes 0-3/10",
                },
            )
        del path
        body = await request.body()
        return JSONResponse(
            {
                "upstream": name,
                "method": request.method,
                "path": request.url.path,
                "query": request.url.query,
                "body": body.decode("utf-8", errors="replace"),
                "authenticated_user": request.headers.get(
                    "x-dcar-authenticated-user"
                ),
                "authorization": request.headers.get("authorization"),
                "cookie": request.headers.get("cookie"),
                "session_binding": request.headers.get(
                    "x-dcar-session-binding"
                ),
                "edge_key": request.headers.get("x-dcar-edge-key"),
                "verified_action": request.headers.get(
                    "x-dcar-verified-action"
                ),
                "forged_custom": request.headers.get("x-dcar-forged-custom"),
                "dcar_request": request.headers.get("x-dcar-request"),
            }
        )

    return app



def _tencent_status(code: str, phone: str = PHONE) -> dict:
    return {
        "Response": {
            "SendStatusSet": [
                {
                    "SerialNo": "2028:f825e6b16e7e4a1f8b1d9e3a",
                    "PhoneNumber": f"+86{phone}",
                    "Fee": 1 if code == "Ok" else 0,
                    "SessionContext": "",
                    "Code": code,
                    "Message": "send success" if code == "Ok" else "failed",
                    "IsoCode": "CN" if code == "Ok" else "",
                }
            ],
            "RequestId": "6f5b2a8e-0000-4000-8000-000000000000",
        }
    }


def _tencent_error(code: str) -> dict:
    return {"Response": {"Error": {"Code": code, "Message": code}, "RequestId": "x"}}


class DcarAuthGatewayTestCase(unittest.TestCase):
    def setUp(self) -> None:
        self.temporary = tempfile.TemporaryDirectory()
        self.addCleanup(self.temporary.cleanup)
        self.root = Path(self.temporary.name)
        self.login_template = self.root / "login.html"
        self.login_template.write_text(
            "<!doctype html><title>Dcar login</title>", encoding="utf-8"
        )
        self.pepper_path = self.root / "pepper"
        self.pepper_path.write_text(PEPPER + "\n", encoding="utf-8")
        self.password_hash = sha512_crypt.using(rounds=5000).hash(PASSWORD)
        self.sms_requests: list[httpx.Request] = []
        self.sms_response: dict[str, object] | bytes = _tencent_status("Ok")
        self.sms_failure: BaseException | None = None
        self.sent_codes: list[tuple[str, str]] = []

    def _store_for(self, config: auth_gateway.AuthGatewayConfig) -> auth_store.AuthStore:
        store = auth_store.AuthStore(
            config.session_db_path,
            pepper=PEPPER.encode("utf-8"),
            throttle_window_seconds=config.throttle_window_seconds,
            throttle_max_failures=config.throttle_max_failures,
            sms_daily_cap=config.sms_daily_cap,
        )
        store.initialize()
        return store

    def _seed(self, config: auth_gateway.AuthGatewayConfig) -> auth_store.AuthStore:
        """Create the fixture operator (with phone) and allow NEW_PHONE."""
        store = self._store_for(config)
        if store.get_user(USERNAME) is None:
            connection = sqlite3.connect(config.session_db_path)
            with connection:
                connection.execute(
                    "INSERT INTO auth_users(username, phone, password_hash, status, role, "
                    "created_at, password_updated_at) VALUES(?,?,?,'active','operator',?,?)",
                    (USERNAME, PHONE, self.password_hash, 1, 1),
                )
            connection.close()
            store.allow_phone(NEW_PHONE, "fixture")
        return store

    def _sms_transport(self) -> httpx.MockTransport:
        def handler(request: httpx.Request) -> httpx.Response:
            self.sms_requests.append(request)
            if self.sms_failure is not None:
                raise self.sms_failure
            payload = json.loads(request.content.decode("utf-8"))
            self.sent_codes.append(
                (payload["PhoneNumberSet"][0].removeprefix("+86"), payload["TemplateParamSet"][0])
            )
            if isinstance(self.sms_response, bytes):
                return httpx.Response(200, content=self.sms_response)
            return httpx.Response(200, json=self.sms_response)

        return httpx.MockTransport(handler)

    def _last_code(self) -> str:
        return self.sent_codes[-1][1]

    def _config(
        self,
        base_path: str,
        *,
        bypass_auth: bool = False,
        douyin_enabled: bool = False,
        static_asset_manifest_path: Path | None = None,
    ) -> auth_gateway.AuthGatewayConfig:
        suffix = "root" if not base_path else base_path.strip("/").replace("/", "-")
        edge_key_path = self.root / f"edge-key-{suffix}"
        if douyin_enabled:
            edge_key_path.write_text(EDGE_KEY + "\n", encoding="utf-8")
        return auth_gateway.AuthGatewayConfig(
            base_path=base_path,
            web_upstream="http://web.test",
            api_upstream="http://api.test",
            session_db_path=self.root / f"sessions-{suffix}.sqlite3",
            login_template_path=self.login_template,
            secure_cookie=True,
            bypass_auth=bypass_auth,
            douyin_upstream=("http://douyin.test" if douyin_enabled else None),
            douyin_edge_key_path=(edge_key_path if douyin_enabled else None),
            session_seconds=3600,
            remember_session_seconds=86400,
            failure_delay_seconds=0,
            sms_provider="tencent",
            sms_credentials_path=self._sms_credentials_path(),
            pepper_path=self.pepper_path,
            static_asset_manifest_path=static_asset_manifest_path,
        )

    def _sms_credentials_path(self) -> Path:
        path = self.root / "sms-tencent"
        if not path.exists():
            # Same shape as the operator's dcar.env.local: quoted values, an
            # unrelated key, comments and blank lines.
            path.write_text(
                "# 9. 腾讯云短信\n"
                'TENCENT_SMS_SECRET_ID="AKIDtest"\n'
                'TENCENT_SMS_SECRET_KEY="secret-test"\n'
                'TENCENT_SMS_SDK_APP_ID="1400000000"\n'
                'TENCENT_SMS_SIGN_NAME="懂车帝"\n'
                'TENCENT_SMS_TEMPLATE_ID="1234567"\n'
                'TENCENT_SMS_REGION="ap-guangzhou"\n'
                'TENCENT_SMS_CODE_TTL_MINUTES="5"\n'
                "\n"
                'TIKHUB_API_KEY="unrelated"\n',
                encoding="utf-8",
            )
        return path

    def test_from_env_uses_systemd_credentials_directory_for_douyin(self) -> None:
        credential_root = self.root / "systemd-credentials"
        with patch.dict(
            os.environ,
            {
                "DCAR_AUTH_DOUYIN_UPSTREAM": "http://127.0.0.1:4175",
                "DCAR_AUTH_SMS_PROVIDER": "tencent",
                "CREDENTIALS_DIRECTORY": str(credential_root),
            },
            clear=True,
        ):
            config = auth_gateway.AuthGatewayConfig.from_env()
        self.assertEqual(
            config.douyin_edge_key_path, credential_root / "douyin-edge-key"
        )
        self.assertEqual(
            config.sms_credentials_path, credential_root / "sms-tencent"
        )
        self.assertEqual(config.pepper_path, credential_root / "auth-pepper")

    @contextmanager
    def _client(
        self,
        base_path: str,
        *,
        bypass_auth: bool = False,
        douyin_enabled: bool = False,
        role: str = auth_store.ROLE_OPERATOR,
    ) -> Iterator[tuple[TestClient, auth_gateway.AuthGatewayConfig]]:
        config = self._config(
            base_path,
            bypass_auth=bypass_auth,
            douyin_enabled=douyin_enabled,
        )
        if not bypass_auth:
            self._seed(config)
            if role != auth_store.ROLE_OPERATOR:
                self._store_for(config).set_role(USERNAME, role, actor="test")
        app = auth_gateway.create_app(
            config,
            web_transport=ASGITransport(app=_echo_upstream("web")),
            api_transport=ASGITransport(app=_echo_upstream("api")),
            douyin_transport=(
                ASGITransport(app=_echo_upstream("douyin"))
                if douyin_enabled
                else None
            ),
            sms_transport=self._sms_transport(),
        )
        with TestClient(app, base_url=ORIGIN) as client:
            yield client, config

    def _login(
        self,
        client: TestClient,
        base_path: str,
        *,
        username: str = USERNAME,
        password: str = PASSWORD,
        return_to: str | None = None,
        origin: str = ORIGIN,
        remember: str = "0",
    ):
        target = return_to or _route(base_path, "/selling-points")
        return client.post(
            _route(base_path, "/auth/login"),
            data={
                "username": username,
                "password": password,
                "remember": remember,
                "return_to": target,
            },
            headers={"Origin": origin, "X-Dcar-Request": "login"},
        )

    def test_unauthenticated_page_redirects_and_api_returns_401(self) -> None:
        for base_path in ("", "/dcar"):
            with self.subTest(base_path=base_path), self._client(base_path) as (
                client,
                _config,
            ):
                page_path = _route(base_path, "/selling-points?window=this-week")
                page = client.get(
                    page_path,
                    headers={"X-Dcar-Authenticated-User": "forged"},
                    follow_redirects=False,
                )
                self.assertEqual(page.status_code, 302)
                self.assertEqual(
                    page.headers.get("location"),
                    _route(base_path, "/login")
                    + "?return_to="
                    + ("%2Fdcar" if base_path else "")
                    + "%2Fselling-points%3Fwindow%3Dthis-week",
                )
                self.assertEqual(page.headers.get("cache-control"), "no-store")

                api = client.get(
                    _route(base_path, "/api/v8/overview"),
                    headers={"X-Dcar-Authenticated-User": "forged"},
                    follow_redirects=False,
                )
                self.assertEqual(api.status_code, 401)
                self.assertIsNone(api.headers.get("location"))
                self.assertEqual(api.json(), {"detail": "请先登录"})
                self.assertEqual(api.headers.get("cache-control"), "no-store")

    def test_selling_point_writes_are_denied_for_all_authorized_roles_before_proxy(self) -> None:
        paths = (
            "/api/v8/selling-points",
            "/api/v8/selling-points/",
            "/api/v8/selling-points/draft",
            "/api/v8/selling-points/draft/",
            "/api/v8/selling-points/draft/point-1",
            "/api/v8/selling-points/publish",
            "/api/v8/%73elling-points/draft",
            "/api%2Fv8/selling-points/draft",
            "/api/v8/anything/%2e%2e/selling-points/draft",
        )
        for base_path in ("", "/dcar"):
            with self.subTest(base_path=base_path), self._client(base_path) as (client, config):
                self._seed_user(config, "lead", role="admin")
                self._seed_user(config, "boss", role="superadmin")
                for username in (USERNAME, "lead", "boss"):
                    self._login_as(client, base_path, username)
                    api_client = client.app.state.api_client
                    with patch.object(api_client, "send", wraps=api_client.send) as upstream:
                        for method in ("POST", "PUT", "PATCH", "DELETE"):
                            for path in paths:
                                with self.subTest(username=username, method=method, path=path):
                                    denied = client.request(
                                        method, _route(base_path, path),
                                        content=b"{invalid-json", follow_redirects=False,
                                    )
                                    self.assertEqual(denied.status_code, 403, denied.text)
                                    self.assertEqual(denied.json(), {
                                        "detail": "卖点标准由后台统一维护，当前页面仅供查看。",
                                        "code": "selling_points_read_only",
                                    })
                                    self.assertEqual(denied.headers["cache-control"], "no-store")
                        upstream.assert_not_called()

    def test_selling_point_reads_and_adjacent_routes_still_proxy(self) -> None:
        for base_path in ("", "/dcar"):
            with self.subTest(base_path=base_path), self._client(base_path) as (client, _config):
                self._login_as(client, base_path, USERNAME)
                api_client = client.app.state.api_client
                with patch.object(api_client, "send", wraps=api_client.send) as upstream:
                    for method in ("GET", "HEAD", "OPTIONS"):
                        for path in (
                            "/api/v8/selling-points?window=this-week",
                            "/api/v8/selling-points/",
                            "/api/v8/selling-points/versions",
                        ):
                            with self.subTest(method=method, path=path):
                                response = client.request(method, _route(base_path, path))
                                self.assertEqual(response.status_code, 200, response.text)
                                if method != "HEAD":
                                    self.assertEqual(response.json()["upstream"], "api")
                                    self.assertEqual(response.json()["method"], method)
                    for path in (
                        "/api/v8/selling-points-report",
                        "/api/v8/selling-points-extra/draft",
                        "/api/v8/tasks",
                    ):
                        response = client.post(_route(base_path, path), json={})
                        self.assertEqual(response.status_code, 200, response.text)
                        self.assertEqual(response.json()["path"], path)
                        self.assertEqual(response.json()["upstream"], "api")
                    self.assertEqual(upstream.await_count, 12)

    def test_selling_point_write_denial_preserves_authentication_and_approval_gates(self) -> None:
        for base_path in ("", "/dcar"):
            with self.subTest(base_path=base_path), self._client(base_path) as (client, config):
                api_client = client.app.state.api_client
                with patch.object(api_client, "send", wraps=api_client.send) as upstream:
                    path = _route(base_path, "/api/v8/selling-points/draft")
                    anonymous = client.post(path, json={}, follow_redirects=False)
                    self.assertEqual(anonymous.status_code, 401)
                    self.assertEqual(anonymous.json(), {"detail": "请先登录"})
                    self._seed_user(config, "newcomer", role="new_user")
                    self._login_as(client, base_path, "newcomer")
                    pending = client.post(path, json={}, follow_redirects=False)
                    self.assertEqual(pending.status_code, 403)
                    self.assertEqual(pending.json()["code"], "approval_required")
                    upstream.assert_not_called()

    def test_bypass_mode_proxies_without_login_or_auth_storage(self) -> None:
        for base_path in ("", "/dcar"):
            with self.subTest(base_path=base_path), self._client(
                base_path, bypass_auth=True
            ) as (client, config):
                page = client.get(_route(base_path, "/selling-points"))
                self.assertEqual(page.status_code, 200)
                self.assertEqual(page.json()["upstream"], "web")
                self.assertEqual(
                    page.json()["authenticated_user"],
                    auth_gateway.BYPASS_USERNAME,
                )

                api = client.get(_route(base_path, "/api/v8/overview"))
                self.assertEqual(api.status_code, 200)
                self.assertEqual(api.json()["upstream"], "api")
                self.assertEqual(
                    api.json()["authenticated_user"],
                    auth_gateway.BYPASS_USERNAME,
                )

                destination = _route(base_path, "/selling-points?window=this-week")
                login_page = client.get(
                    _route(base_path, "/login"),
                    params={"return_to": destination},
                    follow_redirects=False,
                )
                self.assertEqual(login_page.status_code, 303)
                self.assertEqual(login_page.headers["location"], destination)

                stale_login = self._login(client, base_path)
                self.assertEqual(stale_login.status_code, 200)
                self.assertEqual(
                    stale_login.json(),
                    {"redirect_to": _route(base_path, "/overview")},
                )
                self.assertNotIn("set-cookie", stale_login.headers)

                session = client.get(_route(base_path, "/auth/session"))
                self.assertEqual(
                    session.json(),
                    {
                        "authenticated": True,
                        "username": auth_gateway.BYPASS_USERNAME,
                    },
                )

                logout = client.post(
                    _route(base_path, "/auth/logout"),
                    headers={"Origin": ORIGIN, "X-Dcar-Request": "logout"},
                )
                self.assertEqual(logout.status_code, 200)
                self.assertEqual(
                    logout.json(),
                    {"redirect_to": _route(base_path, "/overview")},
                )
                self.assertEqual(
                    client.get(_route(base_path, "/auth/health")).json(),
                    {"status": "ok"},
                )
                self.assertFalse(config.session_db_path.exists())

    def test_douyin_routes_are_disabled_by_default_and_bypass_is_forbidden(
        self,
    ) -> None:
        for base_path in ("", "/dcar"):
            with self.subTest(base_path=base_path), self._client(base_path, role="admin") as (
                client,
                _config,
            ):
                page = client.get(
                    _route(base_path, "/douyin"), follow_redirects=False
                )
                self.assertEqual(page.status_code, 302)
                api = client.get(_route(base_path, "/api/douyin/authorizations"))
                self.assertEqual(api.status_code, 401)
                self.assertEqual(self._login(client, base_path).status_code, 200)
                disabled = client.get(_route(base_path, "/douyin"))
                self.assertEqual(disabled.status_code, 404)

            with self.subTest(base_path=base_path, bypass=True), self._client(
                base_path,
                bypass_auth=True,
                douyin_enabled=True, role="admin",
            ) as (client, _config):
                for path in (
                    "/douyin",
                    "/api/douyin/authorizations",
                    "/oauth/douyin/callback?code=secret&state=secret",
                ):
                    with self.subTest(path=path):
                        response = client.get(_route(base_path, path))
                        self.assertEqual(response.status_code, 403)
                        self.assertEqual(response.json(), {"detail": "当前模式禁止抖音授权"}
                                         if path.startswith("/oauth/") else
                                         {"detail": "仅管理员可访问账号管理", "code": "account_admin_required"})

    def test_douyin_boundary_routes_do_not_capture_adjacent_prefixes(self) -> None:
        for base_path in ("", "/dcar"):
            with self.subTest(base_path=base_path), self._client(
                base_path, douyin_enabled=True
            ) as (client, _config):
                self.assertEqual(self._login(client, base_path).status_code, 200)
                for path in (
                    "/douyin-evil",
                    "/api/douyin-evil",
                    "/oauth/douyin/callback-extra",
                    "/oauth/douyin/callback/",
                ):
                    response = client.get(_route(base_path, path))
                    self.assertEqual(response.status_code, 200)
                    expected = "api" if path.startswith("/api/") else "web"
                    self.assertEqual(response.json()["upstream"], expected)

    def test_unauthenticated_douyin_callback_discards_sensitive_query(self) -> None:
        for base_path in ("", "/dcar"):
            with self.subTest(base_path=base_path), self._client(
                base_path, douyin_enabled=True
            ) as (client, _config):
                response = client.get(
                    _route(
                        base_path,
                        "/oauth/douyin/callback?code=code-canary&state=state-canary",
                    ),
                    headers={
                        "Sec-Fetch-Mode": "navigate",
                        "Sec-Fetch-Dest": "document",
                        "Sec-Fetch-Site": "cross-site",
                    },
                    follow_redirects=False,
                )
                self.assertEqual(response.status_code, 303)
                self.assertEqual(
                    response.headers["location"],
                    _route(base_path, "/login")
                    + "?notice=douyin-session-required&return_to="
                    + ("%2Fdcar%2Fdouyin" if base_path else "%2Fdouyin"),
                )
                self.assertNotIn("code-canary", response.headers["location"])
                self.assertNotIn("state-canary", response.headers["location"])
                self.assertEqual(response.headers["cache-control"], "no-store")
                self.assertEqual(response.headers["referrer-policy"], "no-referrer")

                head = client.head(
                    _route(base_path, "/oauth/douyin/callback"),
                    follow_redirects=False,
                )
                self.assertEqual(head.status_code, 405)
                self.assertIsNone(head.headers.get("location"))

    def test_douyin_callback_requires_top_level_navigation_metadata(self) -> None:
        for base_path in ("", "/dcar"):
            with self.subTest(base_path=base_path), self._client(
                base_path, douyin_enabled=True
            ) as (client, _config):
                self.assertEqual(self._login(client, base_path).status_code, 200)
                for headers in (
                    {"Sec-Fetch-Mode": "cors"},
                    {"Sec-Fetch-Dest": "iframe"},
                    {"Sec-Fetch-Site": "same-origin"},
                ):
                    response = client.get(
                        _route(base_path, "/oauth/douyin/callback"),
                        headers=headers,
                    )
                    self.assertEqual(response.status_code, 403)
                valid = client.get(
                    _route(base_path, "/oauth/douyin/callback"),
                    headers={
                        "Sec-Fetch-Mode": "navigate",
                        "Sec-Fetch-Dest": "document",
                        "Sec-Fetch-Site": "cross-site",
                    },
                )
                self.assertEqual(valid.status_code, 200)
                self.assertEqual(valid.json()["upstream"], "douyin")

    def test_douyin_proxy_validates_origin_and_replaces_all_trusted_headers(
        self,
    ) -> None:
        for base_path in ("", "/dcar"):
            with self.subTest(base_path=base_path), self._client(
                base_path, douyin_enabled=True, role="admin"
            ) as (client, _config):
                self.assertEqual(self._login(client, base_path).status_code, 200)
                start_body = '{"account_id":7,"platform_uid":"123456789"}'
                response = client.post(
                    _route(base_path, "/api/douyin/oauth/start"),
                    content=start_body,
                    headers={
                        "Content-Type": "application/json",
                        "Origin": ORIGIN,
                        "X-Dcar-Request": "douyin-oauth-start",
                        "X-Dcar-Authenticated-User": "attacker",
                        "X-Dcar-Session-Binding": "attacker-binding",
                        "X-Dcar-Edge-Key": "attacker-edge-key",
                        "X-Dcar-Verified-Action": "attacker-action",
                        "X-Dcar-Forged-Custom": "attacker-custom",
                    },
                )
                self.assertEqual(response.status_code, 200)
                payload = response.json()
                self.assertEqual(payload["upstream"], "douyin")
                self.assertEqual(payload["path"], "/api/douyin/oauth/start")
                self.assertEqual(payload["body"], start_body)
                self.assertEqual(payload["authenticated_user"], USERNAME)
                self.assertRegex(payload["session_binding"], r"^[0-9a-f]{64}$")
                self.assertEqual(payload["edge_key"], EDGE_KEY)
                self.assertEqual(
                    payload["verified_action"], "douyin-oauth-start"
                )
                self.assertIsNone(payload["forged_custom"])
                self.assertIsNone(payload["dcar_request"])
                self.assertIsNone(payload["cookie"])

                actions = {
                    "/api/douyin/authorizations/reauthorize": (
                        "douyin-authorization-reauthorize",
                        '{"authorization_id":"auth-1","expected_version":3}',
                    ),
                    "/api/douyin/authorizations/unbind": (
                        "douyin-authorization-unbind",
                        '{"authorization_id":"auth-1","expected_version":3}',
                    ),
                }
                for path, (action, body) in actions.items():
                    routed = client.post(
                        _route(base_path, path),
                        content=body,
                        headers={
                            "Origin": ORIGIN,
                            "X-Dcar-Request": action,
                        },
                    )
                    self.assertEqual(routed.status_code, 200)
                    self.assertEqual(routed.json()["path"], path)
                    self.assertEqual(routed.json()["body"], body)
                    self.assertEqual(routed.json()["verified_action"], action)

                for path, action in (
                    (
                        "/api/douyin/accounts/search",
                        "douyin-accounts-search",
                    ),
                    (
                        "/api/douyin/authorizations/match",
                        "douyin-authorization-match",
                    ),
                    ("/api/douyin/oauth/confirm", "douyin-oauth-confirm"),
                    ("/api/douyin/oauth/reject", "douyin-oauth-reject"),
                ):
                    removed = client.post(
                        _route(base_path, path),
                        content="{}",
                        headers={
                            "Origin": ORIGIN,
                            "X-Dcar-Request": action,
                        },
                    )
                    self.assertEqual(removed.status_code, 404)

                for headers in (
                    {
                        "Origin": "https://evil.example",
                        "X-Dcar-Request": "douyin-oauth-start",
                    },
                    {"Origin": ORIGIN},
                    {
                        "Origin": ORIGIN,
                        "X-Dcar-Request": "douyin-authorization-unbind",
                    },
                ):
                    rejected = client.post(
                        _route(base_path, "/api/douyin/oauth/start"),
                        content=start_body,
                        headers=headers,
                    )
                    self.assertEqual(rejected.status_code, 403)

    def test_douyin_proxy_drops_all_upstream_set_cookie_headers(self) -> None:
        for base_path in ("", "/dcar"):
            with self.subTest(base_path=base_path), self._client(
                base_path, douyin_enabled=True, role="admin"
            ) as (client, _config):
                self.assertEqual(self._login(client, base_path).status_code, 200)
                session_before = client.cookies.get(auth_gateway.SESSION_COOKIE)

                response = client.get(_route(base_path, "/douyin/set-cookie"))

                self.assertEqual(response.status_code, 200)
                self.assertEqual(response.headers.get_list("set-cookie"), [])
                self.assertEqual(
                    client.cookies.get(auth_gateway.SESSION_COOKIE), session_before
                )
                session = client.get(_route(base_path, "/auth/session"))
                self.assertEqual(session.status_code, 200)
                self.assertEqual(session.json()["username"], USERNAME)

    def test_douyin_routes_reject_methods_other_than_get_and_post(self) -> None:
        for base_path in ("", "/dcar"):
            with self.subTest(base_path=base_path), self._client(
                base_path, douyin_enabled=True, role="admin"
            ) as (client, _config):
                target = _route(base_path, "/api/douyin/oauth/start")
                for authenticated in (False, True):
                    if authenticated:
                        self.assertEqual(
                            self._login(client, base_path).status_code, 200
                        )
                    for method in ("HEAD", "PUT", "PATCH", "DELETE", "OPTIONS"):
                        with self.subTest(
                            authenticated=authenticated, method=method
                        ):
                            response = client.request(
                                method,
                                target,
                                content=b"x"
                                * (auth_gateway.MAX_DOUYIN_BODY_BYTES + 1),
                                headers={
                                    "X-Dcar-Authenticated-User": "attacker",
                                    "X-Dcar-Edge-Key": "attacker-edge-key",
                                },
                            )
                            self.assertEqual(response.status_code, 405)
                            self.assertEqual(response.headers["allow"], "GET, POST")
                            self.assertEqual(
                                response.headers["cache-control"], "no-store"
                            )

            with self.subTest(base_path=base_path, bypass=True), self._client(
                base_path, bypass_auth=True, douyin_enabled=True, role="admin"
            ) as (client, _config):
                for method in ("HEAD", "PUT", "PATCH", "DELETE", "OPTIONS"):
                    response = client.request(
                        method,
                        _route(base_path, "/api/douyin/oauth/start"),
                        content=b"untrusted-body",
                    )
                    self.assertEqual(response.status_code, 403)
                    self.assertNotIn("allow", response.headers)

    def test_douyin_post_body_is_bounded_and_requires_content_length(self) -> None:
        with self._client("", douyin_enabled=True, role="admin") as (client, _config):
            self.assertEqual(self._login(client, "").status_code, 200)
            headers = {
                "Origin": ORIGIN,
                "X-Dcar-Request": "douyin-oauth-start",
                "Content-Type": "application/json",
            }
            missing = client.post(
                "/api/douyin/oauth/start",
                content=(chunk for chunk in (b"{}",)),
                headers=headers,
            )
            self.assertEqual(missing.status_code, 411)
            oversized = client.post(
                "/api/douyin/oauth/start",
                content=b"x" * (auth_gateway.MAX_DOUYIN_BODY_BYTES + 1),
                headers=headers,
            )
            self.assertEqual(oversized.status_code, 413)
            accepted = client.post(
                "/api/douyin/oauth/start",
                content=b"x" * auth_gateway.MAX_DOUYIN_BODY_BYTES,
                headers=headers,
            )
            self.assertEqual(accepted.status_code, 200)
            self.assertEqual(
                len(accepted.json()["body"]), auth_gateway.MAX_DOUYIN_BODY_BYTES
            )

    def test_douyin_upstream_requires_a_nonempty_edge_key_file(self) -> None:
        missing_path = self.root / "missing-edge-key"
        config = auth_gateway.AuthGatewayConfig(
            **{
                **self._config("").__dict__,
                "douyin_upstream": "http://douyin.test",
                "douyin_edge_key_path": missing_path,
            }
        )
        app = auth_gateway.create_app(
            config,
            web_transport=ASGITransport(app=_echo_upstream("web")),
            api_transport=ASGITransport(app=_echo_upstream("api")),
            douyin_transport=ASGITransport(app=_echo_upstream("douyin")),
        )
        with self.assertRaises(FileNotFoundError), TestClient(app, base_url=ORIGIN):
            pass

        missing_path.write_text("\n", encoding="utf-8")
        with self.assertRaisesRegex(RuntimeError, "invalid format"), TestClient(
            app, base_url=ORIGIN
        ):
            pass

    def test_proxy_rewrites_only_internal_absolute_redirects(self) -> None:
        with self._client("/dcar", bypass_auth=True) as (client, _config):
            internal = client.get(
                "/dcar/internal-redirect", follow_redirects=False
            )
            self.assertEqual(internal.status_code, 307)
            self.assertEqual(
                internal.headers["location"],
                "/dcar/overview?window=this-week#summary",
            )

            external = client.get(
                "/dcar/external-redirect", follow_redirects=False
            )
            self.assertEqual(
                external.headers["location"], "https://docs.example/help"
            )

            relative = client.get(
                "/dcar/relative-redirect", follow_redirects=False
            )
            self.assertEqual(relative.headers["location"], "/dcar/overview")

        with self._client("/dcar") as (client, _config):
            self.assertEqual(self._login(client, "/dcar").status_code, 200)
            api = client.get(
                "/dcar/api/stripped-redirect", follow_redirects=False
            )
            self.assertEqual(api.status_code, 307)
            self.assertEqual(api.headers["location"], "/dcar/api/v8/tasks/")

        with self._client("/dcar", douyin_enabled=True, role="admin") as (client, _config):
            self.assertEqual(self._login(client, "/dcar").status_code, 200)
            douyin = client.get(
                "/dcar/douyin/stripped-redirect", follow_redirects=False
            )
            self.assertEqual(douyin.status_code, 307)
            self.assertEqual(douyin.headers["location"], "/dcar/douyin/confirm")

        with self._client("/dcar") as (client, _config):
            self.assertEqual(self._login(client, "/dcar").status_code, 200)
            asset = client.get(
                "/dcar/assets/stripped-redirect", follow_redirects=False
            )
            self.assertEqual(asset.status_code, 307)
            self.assertEqual(asset.headers["location"], "/dcar/assets/app.css")

    def test_correct_and_incorrect_login_and_cookie_attributes(self) -> None:
        for base_path in ("", "/dcar"):
            with self.subTest(base_path=base_path), self._client(base_path) as (
                client,
                config,
            ):
                wrong = self._login(client, base_path, password="wrong-password")
                self.assertEqual(wrong.status_code, 401)
                self.assertNotIn("set-cookie", wrong.headers)

                destination = _route(base_path, "/selling-points?window=yesterday")
                correct = self._login(client, base_path, return_to=destination)
                self.assertEqual(correct.status_code, 200)
                self.assertEqual(correct.json(), {"redirect_to": destination})
                cookie = correct.headers.get("set-cookie", "")
                self.assertIn(f"{auth_gateway.SESSION_COOKIE}=", cookie)
                self.assertIn(f"Path={config.cookie_path}", cookie)
                self.assertIn("HttpOnly", cookie)
                self.assertIn("Secure", cookie)
                self.assertIn("SameSite=lax", cookie)
                self.assertNotIn("Max-Age", cookie)

                session = client.get(_route(base_path, "/auth/session"))
                self.assertEqual(session.status_code, 200)
                self.assertEqual(
                    session.json(),
                    {"authenticated": True, "username": USERNAME, "role": "operator"},
                )

                client.post(
                    _route(base_path, "/auth/logout"),
                    headers={"Origin": ORIGIN, "X-Dcar-Request": "logout"},
                )
                remembered = self._login(
                    client,
                    base_path,
                    return_to=destination,
                    remember="1",
                )
                self.assertEqual(remembered.status_code, 200)
                self.assertIn(
                    "Max-Age=86400", remembered.headers.get("set-cookie", "")
                )

    def test_logout_revokes_server_session_and_replayed_cookie_fails(self) -> None:
        for base_path in ("", "/dcar"):
            with self.subTest(base_path=base_path), self._client(base_path) as (
                client,
                config,
            ):
                login = self._login(client, base_path)
                self.assertEqual(login.status_code, 200)
                old_token = client.cookies.get(auth_gateway.SESSION_COOKIE)
                self.assertIsNotNone(old_token)

                logout = client.post(
                    _route(base_path, "/auth/logout"),
                    headers={"Origin": ORIGIN, "X-Dcar-Request": "logout"},
                )
                self.assertEqual(logout.status_code, 200)
                self.assertEqual(
                    logout.json(), {"redirect_to": _route(base_path, "/login")}
                )
                cleared_cookie = logout.headers.get("set-cookie", "")
                self.assertIn(f"Path={config.cookie_path}", cleared_cookie)
                self.assertIn("Max-Age=0", cleared_cookie)

                token_hash = hashlib.sha256(old_token.encode("utf-8")).hexdigest()
                with sqlite3.connect(config.session_db_path) as connection:
                    remaining = connection.execute(
                        "SELECT COUNT(*) FROM auth_sessions WHERE token_sha256=?",
                        (token_hash,),
                    ).fetchone()[0]
                self.assertEqual(remaining, 0)

                client.cookies.clear()
                replay = client.get(
                    _route(base_path, "/auth/session"),
                    headers={
                        "Cookie": f"{auth_gateway.SESSION_COOKIE}={old_token}"
                    },
                )
                self.assertEqual(replay.status_code, 401)
                self.assertEqual(replay.json(), {"detail": "请先登录"})

    def test_return_to_never_allows_an_open_redirect(self) -> None:
        unsafe_targets = (
            "https://evil.example/steal",
            "//evil.example/steal",
            "/\\evil.example/steal",
            "/login",
            "/auth/logout",
            "/safe#https://evil.example",
            "/dcar/%2e%2e/admin",
            "/dcar/a/../login",
            "/dcar/%2e/auth/logout",
        )
        for base_path in ("", "/dcar"):
            fallback = _route(base_path, "/overview")
            allowed = _route(base_path, "/selling-points?window=this-week")
            with self.subTest(base_path=base_path), self._client(base_path) as (
                client,
                _config,
            ):
                accepted = self._login(client, base_path, return_to=allowed)
                self.assertEqual(accepted.status_code, 200)
                self.assertEqual(accepted.json()["redirect_to"], allowed)

                targets = list(unsafe_targets)
                if base_path:
                    targets.append("/overview")
                for target in targets:
                    with self.subTest(base_path=base_path, target=target):
                        response = self._login(
                            client, base_path, return_to=target
                        )
                        self.assertEqual(response.status_code, 200)
                        self.assertEqual(response.json()["redirect_to"], fallback)

    def test_login_and_logout_reject_cross_origin_posts(self) -> None:
        for base_path in ("", "/dcar"):
            with self.subTest(base_path=base_path), self._client(base_path) as (
                client,
                _config,
            ):
                cross_origin_login = self._login(
                    client,
                    base_path,
                    origin="https://evil.example",
                )
                self.assertEqual(cross_origin_login.status_code, 403)
                self.assertEqual(
                    cross_origin_login.json(),
                    {"detail": "登录页面已失效，请刷新后重新登录"},
                )
                self.assertNotIn("set-cookie", cross_origin_login.headers)

                fetch_metadata_login = client.post(
                    _route(base_path, "/auth/login"),
                    data={"username": USERNAME, "password": PASSWORD},
                    headers={
                        "Sec-Fetch-Site": "cross-site",
                        "X-Dcar-Request": "login",
                    },
                )
                self.assertEqual(fetch_metadata_login.status_code, 403)
                self.assertEqual(
                    fetch_metadata_login.json(),
                    {"detail": "登录页面已失效，请刷新后重新登录"},
                )

                missing_marker = client.post(
                    _route(base_path, "/auth/login"),
                    data={"username": USERNAME, "password": PASSWORD},
                    headers={"Origin": ORIGIN},
                )
                self.assertEqual(missing_marker.status_code, 403)
                self.assertEqual(
                    missing_marker.json(),
                    {"detail": "登录页面已失效，请刷新后重新登录"},
                )

                self.assertEqual(self._login(client, base_path).status_code, 200)
                cross_origin_logout = client.post(
                    _route(base_path, "/auth/logout"),
                    headers={
                        "Origin": "https://evil.example",
                        "X-Dcar-Request": "logout",
                    },
                )
                self.assertEqual(cross_origin_logout.status_code, 403)
                self.assertEqual(
                    cross_origin_logout.json(),
                    {"detail": "页面已失效，请刷新后再退出"},
                )
                self.assertEqual(
                    client.get(_route(base_path, "/auth/session")).status_code,
                    200,
                )

    def test_oversized_login_returns_plain_413_contract(self) -> None:
        for base_path in ("", "/dcar"):
            with self.subTest(base_path=base_path), self._client(base_path) as (
                client,
                _config,
            ):
                oversized = self._login(
                    client,
                    base_path,
                    password="x" * (auth_gateway.MAX_LOGIN_BODY_BYTES + 1),
                )
                self.assertEqual(oversized.status_code, 413)
                self.assertEqual(
                    oversized.json(),
                    {"detail": "登录信息太长，请刷新页面后重新输入"},
                )
                self.assertNotIn("set-cookie", oversized.headers)

    def test_login_failures_are_throttled_with_retry_after(self) -> None:
        config = self._config("")
        config = auth_gateway.AuthGatewayConfig(
            **{
                **config.__dict__,
                "throttle_max_failures": 2,
                "failure_delay_seconds": 0,
            }
        )
        app = auth_gateway.create_app(
            config,
            web_transport=ASGITransport(app=_echo_upstream("web")),
            api_transport=ASGITransport(app=_echo_upstream("api")),
        )
        with TestClient(app, base_url=ORIGIN) as client:
            self.assertEqual(
                self._login(client, "", password="wrong").status_code, 401
            )
            self.assertEqual(
                self._login(client, "", password="wrong").status_code, 401
            )
            throttled = self._login(client, "")
            self.assertEqual(throttled.status_code, 429)
            self.assertEqual(
                throttled.json(),
                {"detail": "尝试次数太多，请稍后再登录"},
            )
            self.assertGreater(int(throttled.headers["retry-after"]), 0)

    def test_unavailable_credential_source_returns_plain_503_contract(self) -> None:
        with self._client("") as (client, _config), patch.object(
            auth_store.AuthStore,
            "begin_password_login",
            side_effect=sqlite3.OperationalError("unavailable"),
        ):
            unavailable = self._login(client, "")
        self.assertEqual(unavailable.status_code, 503)
        self.assertEqual(
            unavailable.json(),
            {"detail": "暂时无法登录，请稍后重试"},
        )
        self.assertNotIn("set-cookie", unavailable.headers)

    def test_unavailable_upstream_returns_plain_502_contract(self) -> None:
        def fail_upstream(request: httpx.Request) -> httpx.Response:
            raise httpx.ConnectError("unavailable", request=request)

        config = self._config("")
        self._seed(config)
        app = auth_gateway.create_app(
            config,
            web_transport=httpx.MockTransport(fail_upstream),
            api_transport=ASGITransport(app=_echo_upstream("api")),
        )
        with TestClient(app, base_url=ORIGIN) as client:
            self.assertEqual(self._login(client, "").status_code, 200)
            unavailable = client.get("/overview")
        self.assertEqual(unavailable.status_code, 502)
        self.assertEqual(
            unavailable.json(),
            {
                "detail": "系统暂时无法加载数据，请稍后重试",
                "code": "upstream_unavailable",
            },
        )
        self.assertEqual(unavailable.headers.get("cache-control"), "no-store")

    def test_health_checks_account_source_and_session_store(self) -> None:
        with self._client("") as (client, config):
            healthy = client.get("/auth/health")
            self.assertEqual(healthy.status_code, 200)
            self.assertEqual(healthy.json(), {"status": "ok"})
            with patch.object(
                auth_store.AuthStore,
                "healthcheck",
                side_effect=sqlite3.OperationalError("unavailable"),
            ):
                unavailable = client.get("/auth/health")
            self.assertEqual(unavailable.status_code, 503)
            self.assertEqual(unavailable.json(), {"status": "unavailable"})

    def test_proxy_sets_verified_identity_and_strips_forged_credentials(self) -> None:
        for base_path in ("", "/dcar"):
            with self.subTest(base_path=base_path), self._client(base_path) as (
                client,
                _config,
            ):
                self.assertEqual(self._login(client, base_path).status_code, 200)
                forged_headers = {
                    "X-Dcar-Authenticated-User": "attacker",
                    "Authorization": "Bearer forged-token",
                }

                web = client.get(
                    _route(base_path, "/selling-points?window=this-week"),
                    headers=forged_headers,
                )
                self.assertEqual(web.status_code, 200)
                self.assertEqual(web.json()["upstream"], "web")
                self.assertEqual(
                    web.json()["path"], _route(base_path, "/selling-points")
                )
                self.assertEqual(web.json()["query"], "window=this-week")
                self.assertEqual(web.json()["authenticated_user"], USERNAME)
                self.assertIsNone(web.json()["authorization"])
                self.assertIsNone(web.json()["cookie"])

                api = client.post(
                    _route(base_path, "/api/v8/contents/search?limit=5"),
                    content='{"query":"demo"}',
                    headers={
                        **forged_headers,
                        "Content-Type": "application/json",
                    },
                )
                self.assertEqual(api.status_code, 200)
                self.assertEqual(api.json()["upstream"], "api")
                self.assertEqual(api.json()["path"], "/api/v8/contents/search")
                self.assertEqual(api.json()["query"], "limit=5")
                self.assertEqual(api.json()["body"], '{"query":"demo"}')
                self.assertEqual(api.json()["authenticated_user"], USERNAME)
                self.assertIsNone(api.json()["authorization"])
                self.assertIsNone(api.json()["cookie"])

    def test_proxy_strips_base_path_only_for_generated_web_assets(self) -> None:
        with self._client("/dcar") as (client, _config):
            self.assertEqual(self._login(client, "/dcar").status_code, 200)

            generated_asset = client.get("/dcar/assets/app.css?v=1")
            self.assertEqual(generated_asset.status_code, 200)
            self.assertEqual(generated_asset.json()["upstream"], "web")
            self.assertEqual(generated_asset.json()["path"], "/assets/app.css")
            self.assertEqual(generated_asset.json()["query"], "v=1")

            public_asset = client.get("/dcar/dongchedi-app-icon.svg")
            self.assertEqual(public_asset.status_code, 200)
            self.assertEqual(public_asset.json()["upstream"], "web")
            self.assertEqual(
                public_asset.json()["path"], "/dcar/dongchedi-app-icon.svg"
            )

            for path in ("/dcar/assets2/app.css", "/dcar/_vinext/image"):
                with self.subTest(path=path):
                    response = client.get(path)
                    self.assertEqual(response.status_code, 200)
                    self.assertEqual(response.json()["path"], path)

            for unsafe_path in (
                "/dcar/assets/%2e%2e/secret",
                "/dcar/assets%2F..%2Fsecret",
                "/dcar/assets/%25252e%25252e/secret",
                "/dcar/assets/foo%5cbar.js",
                "/dcar/assets/foo%3fbar.js",
                "/dcar/assets/foo%23bar.js",
                "/dcar/assets/foo%7fbar.js",
                "/dcar/%61ssets/app.css",
                "/dcar/assets/foo%25bar.js",
                "/dcar/assets/foo//bar.js",
            ):
                with self.subTest(unsafe_path=unsafe_path):
                    self.assertEqual(client.get(unsafe_path).status_code, 404)

        with self._client("") as (client, _config):
            self.assertEqual(self._login(client, "").status_code, 200)
            root_asset = client.get("/assets/app.css")
            self.assertEqual(root_asset.status_code, 200)
            self.assertEqual(root_asset.json()["path"], "/assets/app.css")

    def test_proxy_preserves_encoded_and_partial_response_bytes(self) -> None:
        with self._client("/dcar") as (client, _config):
            self.assertEqual(self._login(client, "/dcar").status_code, 200)

            compressed = client.get("/dcar/compressed.txt")
            self.assertEqual(compressed.status_code, 200)
            self.assertEqual(compressed.content, COMPRESSED_BODY)
            self.assertEqual(compressed.headers.get("content-encoding"), "gzip")
            self.assertEqual(compressed.headers.get("etag"), '"compressed"')
            self.assertEqual(
                int(compressed.headers["content-length"]),
                len(gzip.compress(COMPRESSED_BODY)),
            )

            partial = client.get(
                "/dcar/range.bin", headers={"Range": "bytes=0-3"}
            )
            self.assertEqual(partial.status_code, 206)
            self.assertEqual(partial.content, b"0123")
            self.assertEqual(partial.headers.get("accept-ranges"), "bytes")
            self.assertEqual(partial.headers.get("content-range"), "bytes 0-3/10")

    def test_authenticated_proxy_prevents_cache_reuse_after_role_downgrade(self) -> None:
        for base_path in ("", "/dcar"):
            with self.subTest(base_path=base_path):
                seen: list[httpx.Request] = []

                def cacheable_upstream(request: httpx.Request) -> httpx.Response:
                    seen.append(request)
                    status = 200
                    body = b"protected business data"
                    if request.headers.get("if-none-match") or request.headers.get("if-modified-since"):
                        status, body = 304, b""
                    elif request.headers.get("range"):
                        status, body = 206, b"prot"
                    return httpx.Response(
                        status,
                        headers={
                            "Cache-Control": "public, max-age=86400, immutable",
                            "ETag": '"business-data-version"',
                            "Last-Modified": "Fri, 04 Sep 2026 00:00:00 GMT",
                        },
                        stream=httpx.ByteStream(body),
                    )

                config = self._config(base_path, douyin_enabled=True)
                store = self._seed(config)
                transport = httpx.MockTransport(cacheable_upstream)
                app = auth_gateway.create_app(
                    config, web_transport=transport, api_transport=transport,
                    douyin_transport=transport,
                )
                paths = (
                    "/overview", "/overview.rsc", "/assets/app.js",
                    "/api/v8/overview", "/api/v8/media/video/original",
                    "/api/v8/contents/export", "/reports/report.html",
                    "/douyin", "/api/douyin/authorizations", "/oauth/douyin/callback",
                )
                with TestClient(app, base_url=ORIGIN) as client:
                    self.assertEqual(self._login(client, base_path).status_code, 200)
                    original_cookie = client.cookies.get(auth_gateway.SESSION_COOKIE)
                    for path in paths:
                        store.set_role(
                            USERNAME,
                            "admin" if path in {"/douyin", "/api/douyin/authorizations"} else "operator",
                            actor="test",
                        )
                        for headers, expected_status in (
                            ({}, 200),
                            ({"If-None-Match": '"business-data-version"'}, 304),
                            ({"If-Modified-Since": "Fri, 04 Sep 2026 00:00:00 GMT"}, 304),
                            ({"Range": "bytes=0-3"}, 206),
                        ):
                            with self.subTest(path=path, headers=headers):
                                response = client.get(_route(base_path, path), headers=headers)
                                self.assertEqual(response.status_code, expected_status)
                                self.assertEqual(response.headers["cache-control"], "private, no-store")
                                self.assertEqual(response.headers["pragma"], "no-cache")
                                self.assertEqual(response.headers["etag"], '"business-data-version"')
                                self.assertEqual(response.headers["last-modified"], "Fri, 04 Sep 2026 00:00:00 GMT")
                    reached_before_downgrade = len(seen)
                    store.set_role(USERNAME, "new_user", actor="test")
                    for path in paths:
                        for conditional in (
                            {"If-None-Match": '"business-data-version"'},
                            {"If-Modified-Since": "Fri, 04 Sep 2026 00:00:00 GMT"},
                            {"Range": "bytes=0-3", "If-Range": '"business-data-version"'},
                        ):
                            with self.subTest(denied_path=path, headers=conditional):
                                denied = client.get(
                                    _route(base_path, path), headers=conditional,
                                    follow_redirects=False,
                                )
                                self.assertEqual(denied.status_code, 403)
                                self.assertEqual(denied.json()["code"], "account_admin_required" if path in {"/douyin", "/api/douyin/authorizations"} else "approval_required")
                                self.assertEqual(denied.headers["cache-control"], "no-store")
                                self.assertNotIn("etag", denied.headers)
                                self.assertNotIn("last-modified", denied.headers)
                    self.assertEqual(len(seen), reached_before_downgrade)
                    self.assertEqual(client.cookies.get(auth_gateway.SESSION_COOKIE), original_cookie)

    def test_bypass_proxy_keeps_upstream_cache_policy(self) -> None:
        for base_path in ("", "/dcar"):
            with self.subTest(base_path=base_path):
                def cacheable_upstream(request: httpx.Request) -> httpx.Response:
                    del request
                    return httpx.Response(
                        200, headers={"Cache-Control": "public, max-age=86400"},
                        stream=httpx.ByteStream(b"bypass response"),
                    )

                config = self._config(base_path, bypass_auth=True)
                transport = httpx.MockTransport(cacheable_upstream)
                app = auth_gateway.create_app(
                    config, web_transport=transport, api_transport=transport,
                )
                with TestClient(app, base_url=ORIGIN) as client:
                    for path in ("/overview", "/assets/app.js", "/api/v8/overview"):
                        response = client.get(_route(base_path, path))
                        self.assertEqual(response.status_code, 200)
                        self.assertEqual(response.headers["cache-control"], "public, max-age=86400")
                        self.assertNotIn("pragma", response.headers)

    def _code_asset_manifest(self) -> Path:
        client_root = self.root / "client"
        (client_root / "assets").mkdir(parents=True, exist_ok=True)
        (client_root / ".vite").mkdir(exist_ok=True)
        for name in ("entry-AbcD123_.js", "entry-Qwer1234.css", "plain.js", "data-AbcD123_.json"):
            (client_root / "assets" / name).write_text("build code", encoding="utf-8")
        manifest = client_root / ".vite" / "manifest.json"
        manifest.write_text(json.dumps({
            "entry": {"file": "assets/entry-AbcD123_.js", "css": ["assets/entry-Qwer1234.css"]},
            "unhashed": {"file": "assets/plain.js"},
            "json": {"file": "assets/data-AbcD123_.json"},
            "missing": {"file": "assets/missing-AbcD123_.js"},
            "outside": {"file": "../private-AbcD123_.js"},
        }), encoding="utf-8")
        return manifest

    def test_manifest_code_assets_revalidate_after_auth_and_reject_revoked_access(self) -> None:
        manifest = self._code_asset_manifest()
        for base_path in ("", "/dcar"):
            with self.subTest(base_path=base_path):
                seen: list[httpx.Request] = []

                def upstream(request: httpx.Request) -> httpx.Response:
                    seen.append(request)
                    etag = 'W/"AbcD123_"'
                    matched = request.headers.get("if-none-match") == etag
                    return httpx.Response(304 if matched else 200, headers={
                        "ETag": etag, "Content-Type": "application/javascript",
                        "Cache-Control": "public, max-age=31536000, immutable",
                    }, stream=httpx.ByteStream(b"" if matched or request.method == "HEAD" else b"build code"))

                config = self._config(base_path, static_asset_manifest_path=manifest)
                store = self._seed(config)
                transport = httpx.MockTransport(upstream)
                app = auth_gateway.create_app(config, web_transport=transport, api_transport=transport)
                path = _route(base_path, "/assets/entry-AbcD123_.js")
                with TestClient(app, base_url=ORIGIN) as client:
                    self.assertEqual(self._login(client, base_path).status_code, 200)
                    first = client.get(path)
                    self.assertEqual(first.status_code, 200)
                    self.assertEqual(first.content, b"build code")
                    self.assertEqual(first.headers["cache-control"], "private, no-cache")
                    for method in ("GET", "HEAD"):
                        conditional = client.request(method, path, headers={"If-None-Match": first.headers["etag"]})
                        self.assertEqual(conditional.status_code, 304)
                        self.assertEqual(conditional.content, b"")
                        self.assertEqual(conditional.headers["cache-control"], "private, no-cache")
                        self.assertEqual(seen[-1].headers["if-none-match"], first.headers["etag"])
                        self.assertEqual(seen[-1].headers["x-dcar-authenticated-user"], USERNAME)
                    changed = client.get(path, headers={"If-None-Match": 'W/"older123"'})
                    self.assertEqual(changed.status_code, 200)
                    reached_before = len(seen)
                    store.set_role(USERNAME, "new_user", actor="test")
                    denied = client.get(path, headers={"If-None-Match": first.headers["etag"]}, follow_redirects=False)
                    self.assertEqual(denied.status_code, 403)
                    self.assertEqual(denied.headers["cache-control"], "no-store")
                    self.assertNotIn("etag", denied.headers)
                    self.assertEqual(len(seen), reached_before)
                    store.set_role(USERNAME, "operator", actor="test")
                    store.revoke_session(client.cookies.get(auth_gateway.SESSION_COOKIE))
                    signed_out = client.get(path, headers={"If-None-Match": first.headers["etag"]}, follow_redirects=False)
                    self.assertEqual(signed_out.status_code, 302)
                    self.assertEqual(signed_out.headers["cache-control"], "no-store")
                    self.assertNotIn("etag", signed_out.headers)
                    self.assertEqual(len(seen), reached_before)

    def test_manifest_cache_scope_excludes_business_unknown_range_and_unhashed_responses(self) -> None:
        manifest = self._code_asset_manifest()

        def upstream(request: httpx.Request) -> httpx.Response:
            content_type = "text/css" if request.url.path.endswith(".css") else "application/javascript"
            return httpx.Response(200, headers={
                "ETag": 'W/"AbcD123_"', "Content-Type": content_type,
                "Cache-Control": "public, max-age=31536000, immutable",
            }, stream=httpx.ByteStream(b"build code"))

        config = self._config("/dcar", static_asset_manifest_path=manifest)
        self._seed(config)
        transport = httpx.MockTransport(upstream)
        app = auth_gateway.create_app(config, web_transport=transport, api_transport=transport)
        with TestClient(app, base_url=ORIGIN) as client:
            self.assertEqual(self._login(client, "/dcar").status_code, 200)
            css = client.get("/dcar/assets/entry-Qwer1234.css")
            self.assertEqual(css.headers["cache-control"], "private, no-cache")
            for path in (
                "/assets/other-AbcD123_.js", "/assets/missing-AbcD123_.js", "/assets/plain.js",
                "/assets/data-AbcD123_.json", "/assets/entry-AbcD123_.js.map",
                "/assets/entry-AbcD123_.js?version=1", "/overview", "/overview.rsc",
                "/api/v8/overview", "/reports/report.html", "/api/v8/media/video/original",
            ):
                with self.subTest(path=path):
                    response = client.get("/dcar" + path)
                    self.assertEqual(response.headers["cache-control"], "private, no-store")
            ranged = client.get("/dcar/assets/entry-AbcD123_.js", headers={"Range": "bytes=0-3"})
            self.assertEqual(ranged.headers["cache-control"], "private, no-store")

    def test_manifest_replacement_and_invalid_response_fail_closed(self) -> None:
        manifest = self._code_asset_manifest()
        reply = {"status": 200, "headers": {"ETag": 'W/"AbcD123_"', "Content-Type": "application/javascript"}}

        def upstream(request: httpx.Request) -> httpx.Response:
            del request
            return httpx.Response(reply["status"], headers=reply["headers"], stream=httpx.ByteStream(b"code"))

        config = self._config("/dcar", static_asset_manifest_path=manifest)
        self._seed(config)
        transport = httpx.MockTransport(upstream)
        app = auth_gateway.create_app(config, web_transport=transport, api_transport=transport)
        with TestClient(app, base_url=ORIGIN) as client:
            self.assertEqual(self._login(client, "/dcar").status_code, 200)
            path = "/dcar/assets/entry-AbcD123_.js"
            self.assertEqual(client.get(path).headers["cache-control"], "private, no-cache")
            for headers, status in (
                ({"Content-Type": "application/javascript"}, 200),
                ({"ETag": '"error"', "Content-Type": "text/html"}, 200),
                ({"ETag": '"error"', "Content-Type": "application/javascript"}, 404),
                ({"ETag": '"partial"', "Content-Type": "application/javascript"}, 206),
                ({"ETag": '"cookie"', "Content-Type": "application/javascript", "Set-Cookie": "fixture=value"}, 200),
            ):
                reply.update(headers=headers, status=status)
                self.assertEqual(client.get(path).headers["cache-control"], "private, no-store")
            reply.update(status=200, headers={"ETag": 'W/"AbcD123_"', "Content-Type": "application/javascript"})
            replacement = manifest.with_suffix(".next")
            replacement.write_text("{}", encoding="utf-8")
            replacement.replace(manifest)
            self.assertEqual(client.get(path).headers["cache-control"], "private, no-store")
            manifest.unlink()
            self.assertEqual(client.get(path).headers["cache-control"], "private, no-store")
            manifest.write_text("not json", encoding="utf-8")
            self.assertEqual(client.get(path).headers["cache-control"], "private, no-store")

    def test_static_asset_manifest_environment_is_explicit(self) -> None:
        with patch.dict(os.environ, {"DCAR_AUTH_BYPASS": "1"}, clear=True):
            self.assertIsNone(auth_gateway.AuthGatewayConfig.from_env().static_asset_manifest_path)
            os.environ["DCAR_AUTH_STATIC_MANIFEST"] = str(self.root / "manifest.json")
            self.assertEqual(auth_gateway.AuthGatewayConfig.from_env().static_asset_manifest_path, self.root / "manifest.json")

    # ------------------------------------------------------------------
    # Account store bridge
    # ------------------------------------------------------------------

    def test_htpasswd_import_and_export_round_trip(self) -> None:
        config = self._config("")
        store = self._store_for(config)
        source = self.root / "users.htpasswd"
        source.write_text(f"{USERNAME}:{self.password_hash}\n", encoding="utf-8")
        self.assertEqual(store.import_htpasswd(source), 1)
        with self.assertRaisesRegex(auth_store.HtpasswdImportError, "not empty"):
            store.import_htpasswd(source)
        user = store.get_user(USERNAME)
        assert user is not None
        self.assertEqual(user.status, "active")
        self.assertIsNone(user.phone)
        store.set_status(USERNAME, "disabled")
        exported = self.root / "export.htpasswd"
        with self.assertRaises(auth_store.HtpasswdExportError):
            store.export_htpasswd(exported)
        self.assertFalse(exported.exists())
        store.set_status(USERNAME, "active")
        self.assertEqual(store.export_htpasswd(exported), 1)
        self.assertEqual(
            exported.read_text(encoding="utf-8"),
            f"{USERNAME}:{self.password_hash}\n",
        )

    def test_unsupported_htpasswd_content_is_rejected_entirely(self) -> None:
        config = self._config("")
        store = self._store_for(config)
        source = self.root / "users.htpasswd"
        for content in (
            f"{USERNAME}:$apr1$legacy$hash\n",
            f"{USERNAME}:{self.password_hash}\nOPERATOR:{self.password_hash}\n",
            f"temporary-bypass:{self.password_hash}\n",
            "bad line without colon\n",
            f"运营:{self.password_hash}\n",
            "",
        ):
            with self.subTest(content=content[:20]):
                source.write_text(content, encoding="utf-8")
                with self.assertRaises(auth_store.HtpasswdImportError):
                    store.import_htpasswd(source)
                self.assertEqual(store.user_count(), 0)

    # ------------------------------------------------------------------
    # Verification code endpoints
    # ------------------------------------------------------------------

    def _post(
        self,
        client: TestClient,
        base_path: str,
        endpoint: str,
        data: dict[str, str],
        *,
        origin: str = ORIGIN,
        headers: dict[str, str] | None = None,
    ):
        marker = auth_gateway.AUTH_MARKERS[endpoint]
        merged = {"Origin": origin, "X-Dcar-Request": marker}
        if headers:
            merged.update(headers)
        return client.post(_route(base_path, endpoint), data=data, headers=merged)

    def _request_code(self, client: TestClient, base_path: str, phone: str, purpose: str):
        return self._post(
            client, base_path, "/auth/code", {"phone": phone, "purpose": purpose}
        )

    def test_register_creates_account_session_and_uses_json_body_sms(self) -> None:
        for base_path in ("", "/dcar"):
            with self.subTest(base_path=base_path), self._client(base_path) as (
                client,
                config,
            ):
                sent = self._request_code(client, base_path, NEW_PHONE, "register")
                self.assertEqual(sent.status_code, 200, sent.text)
                self.assertEqual(sent.json(), {})
                request = self.sms_requests[-1]
                self.assertEqual(str(request.url), "https://sms.tencentcloudapi.com/")
                self.assertEqual(request.headers["content-type"], "application/json")
                self.assertEqual(request.headers["x-tc-action"], "SendSms")
                self.assertEqual(request.headers["x-tc-version"], "2021-01-11")
                self.assertEqual(request.headers["x-tc-region"], "ap-guangzhou")
                self.assertRegex(request.headers["x-tc-timestamp"], r"^\d{10}$")
                self.assertNotIn(NEW_PHONE, str(request.url))
                sent_payload = json.loads(request.content.decode("utf-8"))
                self.assertEqual(sent_payload["PhoneNumberSet"], [f"+86{NEW_PHONE}"])
                self.assertEqual(sent_payload["SmsSdkAppId"], "1400000000")
                self.assertEqual(sent_payload["SignName"], "懂车帝")
                self.assertEqual(sent_payload["TemplateId"], "1234567")
                self.assertEqual(sent_payload["TemplateParamSet"], [self._last_code(), "5"])
                self.assertRegex(
                    request.headers["authorization"],
                    r"^TC3-HMAC-SHA256 Credential=AKIDtest/\d{4}-\d{2}-\d{2}/sms/tc3_request, "
                    r"SignedHeaders=content-type;host, Signature=[0-9a-f]{64}$",
                )

                registered = self._post(
                    client,
                    base_path,
                    "/auth/register",
                    {
                        "username": "new_operator",
                        "phone": NEW_PHONE,
                        "password": "Long-enough-passphrase",
                        "code": self._last_code(),
                        "return_to": _route(base_path, "/contents"),
                    },
                )
                self.assertEqual(registered.status_code, 200, registered.text)
                self.assertEqual(
                    registered.json(), {"redirect_to": _route(base_path, "/contents")}
                )
                self.assertIn("Max-Age=86400", registered.headers["set-cookie"])
                page = client.get(
                    _route(base_path, "/selling-points"),
                    headers={"Accept": "text/html"}, follow_redirects=False,
                )
                self.assertEqual(page.status_code, 200)
                self.assertIn('data-access="pending"', page.text)
                self.assertNotIn("<script src=", page.text)
                self.assertNotIn("/api/", page.text)
                denied = client.get(_route(base_path, "/api/v8/overview"))
                self.assertEqual(denied.status_code, 403)
                self.assertEqual(denied.json()["code"], "approval_required")
                store = self._store_for(config)
                user = store.get_user("NEW_OPERATOR")
                assert user is not None
                self.assertEqual(user.username, "new_operator")
                self.assertEqual(user.phone, NEW_PHONE)
                self.assertEqual(user.role, "new_user")

    def test_register_rejections_do_not_send_or_consume(self) -> None:
        with self._client("") as (client, config):
            not_allowed = self._request_code(client, "", "13700137000", "register")
            self.assertEqual(not_allowed.status_code, 403)
            self.assertEqual(not_allowed.json()["code"], "phone_not_allowed")
            already = self._request_code(client, "", PHONE, "register")
            self.assertEqual(already.status_code, 409)
            self.assertEqual(already.json()["code"], "phone_registered")
            self.assertEqual(self.sms_requests, [])

            self.assertEqual(
                self._request_code(client, "", NEW_PHONE, "register").status_code, 200
            )
            code = self._last_code()
            with patch.object(auth_store, "hash_password", wraps=auth_store.hash_password) as hashing:
                taken = self._post(
                    client,
                    "",
                    "/auth/register",
                    {"username": "OPERATOR", "phone": NEW_PHONE, "password": "Long-enough-passphrase", "code": code},
                )
                self.assertEqual(taken.status_code, 409)
                self.assertEqual(taken.json()["code"], "username_taken")
                wrong = self._post(
                    client,
                    "",
                    "/auth/register",
                    {"username": "new_operator", "phone": NEW_PHONE, "password": "Long-enough-passphrase", "code": "000000" if code != "000000" else "111111"},
                )
                self.assertEqual(wrong.status_code, 401)
                self.assertEqual(wrong.json()["code"], "invalid_code")
                self.assertEqual(hashing.call_count, 0)
                weak = self._post(
                    client,
                    "",
                    "/auth/register",
                    {"username": "new_operator", "phone": NEW_PHONE, "password": "password", "code": code},
                )
                self.assertEqual(weak.status_code, 400)
                self.assertEqual(weak.json()["code"], "password_too_common")
                contains_phone = self._post(
                    client,
                    "",
                    "/auth/register",
                    {"username": "new_operator", "phone": NEW_PHONE, "password": f"x{NEW_PHONE}y", "code": code},
                )
                self.assertEqual(contains_phone.json()["code"], "password_too_common")
                self.assertEqual(hashing.call_count, 0)
            # The code is still valid after the rejections above.
            ok = self._post(
                client,
                "",
                "/auth/register",
                {"username": "new_operator", "phone": NEW_PHONE, "password": "Long-enough-passphrase", "code": code},
            )
            self.assertEqual(ok.status_code, 200, ok.text)
            store = self._store_for(config)
            self.assertGreaterEqual(store.failure_count(auth_store.AuthStore.ip_key("testclient")), 1)

    def test_code_login_and_disabled_account(self) -> None:
        with self._client("") as (client, config):
            self.assertEqual(self._request_code(client, "", PHONE, "login").status_code, 200)
            login = self._post(
                client,
                "",
                "/auth/login/code",
                {"phone": PHONE, "code": self._last_code(), "remember": "1", "return_to": "/overview"},
            )
            self.assertEqual(login.status_code, 200, login.text)
            self.assertEqual(login.json(), {"redirect_to": "/overview"})
            self.assertEqual(client.get("/auth/session").json()["username"], USERNAME)
            replay = self._post(
                client, "", "/auth/login/code", {"phone": PHONE, "code": self._last_code()}
            )
            self.assertEqual(replay.status_code, 401)
            self.assertEqual(replay.json()["code"], "invalid_code")

            store = self._store_for(config)
            store.set_status(USERNAME, "disabled")
            self.assertEqual(client.get("/auth/session", follow_redirects=False).status_code, 401)
            # Disabling retires the code rows but keeps them in the send ledger,
            # so the 60s per-phone limit still applies before the account check.
            limited = self._request_code(client, "", PHONE, "login")
            self.assertEqual(limited.status_code, 429)
            self.assertEqual(limited.json()["code"], "rate_limited")
            with patch.object(auth_store, "PHONE_SEND_LIMITS", ()):
                disabled_send = self._request_code(client, "", PHONE, "login")
            self.assertEqual(disabled_send.status_code, 403)
            self.assertEqual(disabled_send.json()["code"], "account_disabled")
            password_login = self._login(client, "")
            self.assertEqual(password_login.status_code, 403)
            self.assertEqual(password_login.json(), {"detail": "该账号已停用"})
            unknown = self._request_code(client, "", "13600136000", "login")
            self.assertEqual(unknown.status_code, 404)
            self.assertEqual(unknown.json()["code"], "phone_not_registered")

    def test_password_reset_flow_revokes_old_sessions(self) -> None:
        with self._client("") as (client, config):
            old_login = self._login(client, "", remember="1")
            self.assertEqual(old_login.status_code, 200)
            old_cookie = client.cookies.get(auth_gateway.SESSION_COOKIE)
            client.cookies.clear()

            self.assertEqual(self._request_code(client, "", PHONE, "reset").status_code, 200)
            verified = self._post(
                client, "", "/auth/reset/verify", {"phone": PHONE, "code": self._last_code()}
            )
            self.assertEqual(verified.status_code, 200, verified.text)
            reset_token = verified.json()["reset_token"]
            bad = self._post(
                client, "", "/auth/reset/confirm", {"reset_token": reset_token, "password": "short"}
            )
            self.assertEqual(bad.status_code, 400)
            self.assertEqual(bad.json()["code"], "invalid_password")
            confirmed = self._post(
                client,
                "",
                "/auth/reset/confirm",
                {"reset_token": reset_token, "password": "Brand-new-passphrase", "return_to": "/accounts"},
            )
            self.assertEqual(confirmed.status_code, 200, confirmed.text)
            self.assertEqual(confirmed.json(), {"redirect_to": "/overview"})
            self.assertEqual(client.get("/auth/session").json()["username"], USERNAME)

            again = self._post(
                client, "", "/auth/reset/confirm", {"reset_token": reset_token, "password": "Another-passphrase-1"}
            )
            self.assertEqual(again.status_code, 401)
            self.assertEqual(again.json()["code"], "reset_expired")

            client.cookies.clear()
            stale = client.get(
                "/selling-points",
                headers={"Cookie": f"{auth_gateway.SESSION_COOKIE}={old_cookie}"},
                follow_redirects=False,
            )
            self.assertEqual(stale.status_code, 302)
            self.assertEqual(self._login(client, "", password=PASSWORD).status_code, 401)
            self.assertEqual(self._login(client, "", password="Brand-new-passphrase").status_code, 200)
            store = self._store_for(config)
            exported = self.root / "rollback.htpasswd"
            store.export_htpasswd(exported)
            exported_hash = exported.read_text(encoding="utf-8").split(":", 1)[1].strip()
            self.assertTrue(sha512_crypt.verify("Brand-new-passphrase", exported_hash))
            self.assertFalse(sha512_crypt.verify(PASSWORD, exported_hash))

    def test_sms_provider_outcomes_map_to_status_and_codes(self) -> None:
        with self._client("") as (client, config):
            self.sms_response = _tencent_status("LimitExceeded.PhoneNumberOneHourLimit")
            limited = self._request_code(client, "", PHONE, "login")
            self.assertEqual(limited.status_code, 429)
            self.assertEqual(limited.json()["code"], "rate_limited")
            store = self._store_for(config)
            self.assertEqual(store.challenge_status(1), "rejected")

            self.sms_response = b"<html>gateway error</html>"
            with patch.object(auth_store, "PHONE_SEND_LIMITS", ((60, 10), (3600, 10), (86400, 10))):
                unknown = self._request_code(client, "", PHONE, "login")
                self.assertEqual(unknown.status_code, 503)
                self.assertEqual(unknown.json()["code"], "sms_failed")
                self.assertEqual(store.challenge_status(2), "unknown")
                code_of_unknown = self._last_code()
                rejected = self._post(
                    client, "", "/auth/login/code", {"phone": PHONE, "code": code_of_unknown}
                )
                self.assertEqual(rejected.status_code, 401)

                self.sms_failure = httpx.ReadTimeout("timeout")
                requests_before = len(self.sms_requests)
                timed_out = self._request_code(client, "", PHONE, "login")
                self.assertEqual(timed_out.status_code, 503)
                self.assertEqual(len(self.sms_requests), requests_before + 1)
                self.assertEqual(store.challenge_status(3), "unknown")
                self.sms_failure = None

                self.sms_response = _tencent_error("FailedOperation.InsufficientBalanceInSmsPackage")
                refused = self._request_code(client, "", PHONE, "login")
                self.assertEqual(refused.status_code, 503)
                self.assertEqual(refused.json()["code"], "sms_failed")
                self.assertEqual(store.challenge_status(4), "rejected")

                self.sms_response = _tencent_status("InvalidParameterValue.IncorrectPhoneNumber")
                bad_number = self._request_code(client, "", PHONE, "login")
                self.assertEqual(bad_number.status_code, 400)
                self.assertEqual(bad_number.json()["code"], "invalid_phone")

                self.sms_response = {"Response": {"RequestId": "x"}}
                no_status = self._request_code(client, "", PHONE, "login")
                self.assertEqual(no_status.status_code, 503)
                self.assertEqual(store.challenge_status(6), "unknown")

                self.sms_response = _tencent_status("Ok")
                ok = self._request_code(client, "", PHONE, "login")
                self.assertEqual(ok.status_code, 200)
                login = self._post(
                    client, "", "/auth/login/code", {"phone": PHONE, "code": self._last_code()}
                )
                self.assertEqual(login.status_code, 200, login.text)

    def test_sms_logging_never_contains_secrets(self) -> None:
        secrets_to_hide = (PHONE, "AKIDtest", "secret-test", "懂车帝", "1400000000")
        scenarios = (
            ("ok", _tencent_status("Ok"), None),
            ("error", _tencent_error("AuthFailure.SignatureFailure"), None),
            ("timeout", _tencent_status("Ok"), httpx.ReadTimeout("timeout")),
            ("badjson", b"not json", None),
        )
        for name, response, failure in scenarios:
            with self.subTest(scenario=name), self._client("") as (client, _config):
                self.sms_response = response
                self.sms_failure = failure
                with self.assertLogs(level="DEBUG") as captured:
                    with patch.object(auth_store, "PHONE_SEND_LIMITS", ((60, 5),)):
                        result = self._request_code(client, "", PHONE, "login")
                self.assertIn(result.status_code, {200, 429, 503})
                joined = "\n".join(captured.output)
                for secret in secrets_to_hide:
                    self.assertNotIn(secret, joined)
                if self.sent_codes:
                    self.assertNotIn(self._last_code(), joined)
        self.assertEqual(logging.getLogger("httpx").level, logging.WARNING)
        self.assertEqual(logging.getLogger("httpcore").level, logging.WARNING)

    def test_new_endpoints_enforce_origin_media_type_and_length(self) -> None:
        with self._client("") as (client, _config):
            for endpoint in auth_gateway.AUTH_MARKERS:
                with self.subTest(endpoint=endpoint):
                    cross = self._post(
                        client, "", endpoint, {"phone": PHONE}, origin="https://evil.test"
                    )
                    self.assertEqual(cross.status_code, 403)
                    self.assertEqual(cross.json()["code"], "origin_mismatch")
                    wrong_type = client.post(
                        endpoint,
                        json={"phone": PHONE},
                        headers={"Origin": ORIGIN, "X-Dcar-Request": auth_gateway.AUTH_MARKERS[endpoint]},
                    )
                    self.assertEqual(wrong_type.status_code, 415)
                    too_large = self._post(
                        client, "", endpoint, {"phone": "x" * (auth_gateway.MAX_LOGIN_BODY_BYTES + 1)}
                    )
                    self.assertEqual(too_large.status_code, 413)
                    no_length = client.post(
                        endpoint,
                        content=iter([b"phone=1"]),
                        headers={
                            "Origin": ORIGIN,
                            "X-Dcar-Request": auth_gateway.AUTH_MARKERS[endpoint],
                            "Content-Type": "application/x-www-form-urlencoded",
                        },
                    )
                    self.assertEqual(no_length.status_code, 411)
                    self.assertEqual(client.get(endpoint).status_code, 405)
            bad_purpose = self._post(client, "", "/auth/code", {"phone": PHONE, "purpose": "admin"})
            self.assertEqual(bad_purpose.status_code, 400)
            self.assertEqual(bad_purpose.json()["code"], "invalid_purpose")
            for phone in ("１３８００１３８０００", "1380013800", "23800138000"):
                bad_phone = self._post(client, "", "/auth/code", {"phone": phone, "purpose": "login"})
                self.assertEqual(bad_phone.status_code, 400, phone)
                self.assertEqual(bad_phone.json()["code"], "invalid_phone")
            nul_password = self._post(
                client,
                "",
                "/auth/register",
                {"username": "new_operator", "phone": NEW_PHONE, "password": "passw\x00rd-long", "code": "123456"},
            )
            self.assertEqual(nul_password.status_code, 400)
            self.assertEqual(nul_password.json()["code"], "invalid_password")
            self.assertEqual(self.sms_requests, [])

    def test_new_endpoints_return_404_in_bypass_mode(self) -> None:
        with self._client("", bypass_auth=True) as (client, config):
            for endpoint in auth_gateway.AUTH_MARKERS:
                self.assertEqual(self._post(client, "", endpoint, {"phone": PHONE}).status_code, 404)
            self.assertFalse(config.session_db_path.exists())

    def test_real_ip_header_is_ignored_without_trusted_proxy(self) -> None:
        with self._client("") as (client, config):
            for _ in range(2):
                self.assertEqual(
                    self._login(client, "", password="wrong").status_code, 401
                )
            store = self._store_for(config)
            self.assertEqual(store.failure_count(auth_store.AuthStore.ip_key("testclient")), 2)
            self.assertEqual(store.failure_count(auth_store.AuthStore.ip_key("203.0.113.9")), 0)
            spoofed = client.post(
                "/auth/login",
                data={"username": USERNAME, "password": "wrong", "remember": "0", "return_to": ""},
                headers={"Origin": ORIGIN, "X-Dcar-Request": "login", "X-Real-IP": "203.0.113.9"},
            )
            self.assertEqual(spoofed.status_code, 401)
            self.assertEqual(store.failure_count(auth_store.AuthStore.ip_key("203.0.113.9")), 0)
            self.assertEqual(store.failure_count(auth_store.AuthStore.ip_key("testclient")), 3)

    def test_password_login_skips_hashing_when_throttled(self) -> None:
        config = self._config("")
        config = auth_gateway.AuthGatewayConfig(
            **{**config.__dict__, "throttle_max_failures": 1}
        )
        self._seed(config)
        app = auth_gateway.create_app(
            config,
            web_transport=ASGITransport(app=_echo_upstream("web")),
            api_transport=ASGITransport(app=_echo_upstream("api")),
            sms_transport=self._sms_transport(),
        )
        with TestClient(app, base_url=ORIGIN) as client:
            self.assertEqual(self._login(client, "", password="wrong").status_code, 401)
            with patch.object(auth_store, "verify_password", wraps=auth_store.verify_password) as verify:
                throttled = self._login(client, "")
                self.assertEqual(throttled.status_code, 429)
                self.assertEqual(verify.call_count, 0)

    def test_startup_refuses_log_provider_with_secure_cookie_and_missing_pepper(self) -> None:
        config = self._config("")
        with self.assertRaisesRegex(ValueError, "log SMS provider"):
            auth_gateway.AuthGatewayConfig(**{**config.__dict__, "sms_provider": "log", "sms_credentials_path": None})
        with self.assertRaisesRegex(ValueError, "pepper_path"):
            auth_gateway.AuthGatewayConfig(**{**config.__dict__, "pepper_path": None})
        missing_pepper = auth_gateway.AuthGatewayConfig(
            **{**config.__dict__, "pepper_path": self.root / "missing-pepper"}
        )
        app = auth_gateway.create_app(missing_pepper, sms_transport=self._sms_transport())
        with self.assertRaises(FileNotFoundError):
            with TestClient(app, base_url=ORIGIN):
                pass
        bypass = auth_gateway.AuthGatewayConfig(
            **{**config.__dict__, "bypass_auth": True, "pepper_path": None, "sms_provider": "log", "sms_credentials_path": None}
        )
        with TestClient(auth_gateway.create_app(bypass), base_url=ORIGIN) as client:
            self.assertEqual(client.get("/auth/health").json(), {"status": "ok"})

    def test_health_reports_unsupported_schema_version(self) -> None:
        with self._client("") as (client, config):
            connection = sqlite3.connect(config.session_db_path)
            with connection:
                connection.execute("PRAGMA user_version = 7")
            connection.close()
            self.assertEqual(client.get("/auth/health").status_code, 503)

    def test_tc3_signature_matches_the_official_sdk_vector(self) -> None:
        # Vector produced with tencentcloud-sdk-python 3.1.169
        # (AbstractClient._get_tc3_signature) for these exact inputs.
        # The explicit test ID avoids resembling a deployable cloud credential.
        credentials = auth_sms.SmsCredentials(
            secret_id="unit-test-secret-id",
            secret_key="EXAMPLEKEYexamplekey0123456789ab",
            sdk_app_id="1400000000",
            sign_name="测试签名",
            template_id="1234567",
            region="ap-guangzhou",
            code_ttl_minutes="5",
        )
        body = auth_sms.send_sms_payload(credentials, PHONE, "123456")
        self.assertEqual(
            body,
            '{"PhoneNumberSet": ["+8613800138000"], "SmsSdkAppId": "1400000000", '
            '"SignName": "\\u6d4b\\u8bd5\\u7b7e\\u540d", "TemplateId": "1234567", '
            '"TemplateParamSet": ["123456", "5"]}',
        )
        headers = auth_sms.sign_tc3_request(
            credentials,
            service="sms",
            host="sms.tencentcloudapi.com",
            action="SendSms",
            version="2021-01-11",
            region="ap-guangzhou",
            body=body,
            timestamp=1757000000,
        )
        self.assertEqual(
            headers["Authorization"],
            "TC3-HMAC-SHA256 Credential=unit-test-secret-id/2025-09-04/sms/tc3_request, "
            "SignedHeaders=content-type;host, "
            "Signature=437f328bde71ff8e4fe00f480ed99e1ac74926a5b1a0ee3f0b56ede0a6b4db83",
        )
        self.assertEqual(headers["X-TC-Timestamp"], "1757000000")
        self.assertEqual(headers["Content-Type"], "application/json")
        sender = auth_sms.TencentSmsSender(credentials)
        try:
            request = sender.build_request(PHONE, "123456", timestamp=1757000000)
        finally:
            asyncio.run(sender.aclose())
        self.assertEqual(request.headers["authorization"], headers["Authorization"])
        self.assertEqual(request.content.decode("utf-8"), body)
        single = auth_sms.SmsCredentials("AKID", "KEY", "1400000000", "签名", "1", code_ttl_minutes=None)
        self.assertEqual(json.loads(auth_sms.send_sms_payload(single, PHONE, "654321"))["TemplateParamSet"], ["654321"])

    def test_sms_credentials_loader_accepts_the_env_file_format(self) -> None:
        loaded = auth_sms.load_sms_credentials(self._sms_credentials_path())
        self.assertEqual(
            (loaded.secret_id, loaded.secret_key, loaded.sdk_app_id, loaded.sign_name),
            ("AKIDtest", "secret-test", "1400000000", "懂车帝"),
        )
        self.assertEqual((loaded.template_id, loaded.region, loaded.code_ttl_minutes), ("1234567", "ap-guangzhou", "5"))
        minimal = self.root / "sms-minimal"
        minimal.write_text(
            "export TENCENT_SMS_SECRET_ID=AKIDmin\nTENCENT_SMS_SECRET_KEY='k'\n"
            "TENCENT_SMS_SDK_APP_ID=1400000001\nTENCENT_SMS_SIGN_NAME=签名\nTENCENT_SMS_TEMPLATE_ID=7\n",
            encoding="utf-8",
        )
        loaded = auth_sms.load_sms_credentials(minimal)
        self.assertEqual((loaded.secret_id, loaded.secret_key, loaded.region, loaded.code_ttl_minutes), ("AKIDmin", "k", "ap-guangzhou", None))
        for name, content in (
            ("missing", "TENCENT_SMS_SECRET_ID=a\nTENCENT_SMS_SECRET_KEY=b\n"),
            ("appid", minimal.read_text(encoding="utf-8").replace("1400000001", "app")),
            ("ttl", minimal.read_text(encoding="utf-8") + "TENCENT_SMS_CODE_TTL_MINUTES=10\n"),
            ("control", minimal.read_text(encoding="utf-8").replace("AKIDmin", "AKID\tmin")),
        ):
            with self.subTest(name=name):
                bad = self.root / f"sms-bad-{name}"
                bad.write_text(content, encoding="utf-8")
                with self.assertRaises(RuntimeError):
                    auth_sms.load_sms_credentials(bad)

    # ---------------------------------------- user management and the page gate

    def _seed_user(
        self,
        config: auth_gateway.AuthGatewayConfig,
        username: str,
        *,
        role: str = auth_store.ROLE_OPERATOR,
        phone: str | None = None,
        password: str = PASSWORD,
    ) -> None:
        self._store_for(config).create_user(
            username,
            sha512_crypt.using(rounds=5000).hash(password),
            phone=phone,
            role=role,
            actor="test",
        )

    def _login_as(self, client: TestClient, base_path: str, username: str) -> None:
        response = self._login(client, base_path, username=username)
        self.assertEqual(response.status_code, 200, response.text)

    def _post_users(self, client: TestClient, base_path: str, action: str, payload, **overrides):
        headers = {"Origin": ORIGIN, "X-Dcar-Request": f"user-{action}"}
        headers.update(overrides.pop("headers", {}))
        return client.post(
            _route(base_path, f"/auth/users/{action}"), json=payload, headers=headers, **overrides
        )

    def test_new_user_has_only_data_free_workbench_and_auth_access(self) -> None:
        for base_path in ("", "/dcar"):
            with self.subTest(base_path=base_path), self._client(
                base_path, douyin_enabled=True
            ) as (client, config):
                self._seed_user(config, "newcomer", role="new_user")
                login = self._login(client, base_path, username="newcomer")
                waiting_path = _route(base_path, "/overview")
                self.assertEqual(login.json(), {"redirect_to": _route(base_path, "/selling-points")})
                pages = (
                    "/overview", "/contents", "/selling-points",
                    "/spu-audience", "/tasks",
                )
                web_client = client.app.state.web_client
                api_client = client.app.state.api_client
                with patch.object(web_client, "send", wraps=web_client.send) as web_upstream, \
                        patch.object(api_client, "send", wraps=api_client.send) as api_upstream:
                    for path in pages:
                        with self.subTest(path=path):
                            shell = client.get(
                                _route(base_path, path), headers={"Accept": "text/html"},
                                follow_redirects=False,
                            )
                            self.assertEqual(shell.status_code, 200, shell.text)
                            self.assertIn('data-access="pending"', shell.text)
                            self.assertIn(f'data-section="{path[1:]}"', shell.text)
                            self.assertIn("当前内容尚未开通", shell.text)
                            self.assertIn("刷新权限", shell.text)
                            self.assertIn("退出登录", shell.text)
                            self.assertIn(f"const basePath={json.dumps(base_path)};", shell.text)
                            for destination in pages:
                                self.assertIn(f'href="{_route(base_path, destination)}"', shell.text)
                            self.assertNotIn(f'href="{_route(base_path, "/users")}"', shell.text)
                            self.assertNotIn(f'href="{_route(base_path, "/accounts")}"', shell.text)
                            self.assertNotIn("<script src=", shell.text)
                            self.assertNotIn("/api/", shell.text)
                            self.assertEqual(shell.headers["cache-control"], "no-store")
                            self.assertEqual(shell.headers["x-frame-options"], "DENY")
                    web_upstream.assert_not_called()
                    api_upstream.assert_not_called()
                for path in (
                    "/", "/pending-approval", "/users", "/users/anything", "/accounts",
                    "/douyin", "/oauth/douyin/callback?code=mock&state=mock",
                ):
                    with self.subTest(path=path):
                        page = client.get(
                            _route(base_path, path), headers={"Accept": "text/html"},
                            follow_redirects=False,
                        )
                        self.assertEqual(page.status_code, 303, page.text)
                        self.assertEqual(page.headers["location"], waiting_path)
                        self.assertEqual(page.headers["cache-control"], "no-store")
                session = client.get(_route(base_path, "/auth/session"))
                self.assertEqual(session.json()["role"], "new_user")
                self.assertEqual(client.get(_route(base_path, "/auth/health")).status_code, 200)
                self.assertEqual(client.get(_route(base_path, "/auth/users")).status_code, 403)
                for action in ("update", "delete"):
                    payload = {"username": USERNAME}
                    if action == "update":
                        payload.update({"phone": "", "role": "operator", "password": ""})
                    self.assertEqual(
                        self._post_users(client, base_path, action, payload).status_code,
                        403,
                    )
                logout = client.post(
                    _route(base_path, "/auth/logout"),
                    headers={"Origin": ORIGIN, "X-Dcar-Request": "logout"},
                )
                self.assertEqual(logout.status_code, 200)
                self.assertEqual(client.get(_route(base_path, "/auth/session")).status_code, 401)

    def test_new_user_cannot_proxy_api_resources_rsc_or_mutations(self) -> None:
        for base_path in ("", "/dcar"):
            with self.subTest(base_path=base_path), self._client(
                base_path, douyin_enabled=True
            ) as (client, config):
                self._seed_user(config, "newcomer", role="new_user")
                self._login_as(client, base_path, "newcomer")
                for method, path, headers in (
                    ("GET", "/api/v8/overview", {}),
                    ("GET", "/api/v8/contents/export", {"Accept": "text/html"}),
                    ("GET", "/api%2Fv8/overview", {}),
                    ("GET", "/api/v8/media/protected/video", {"Range": "bytes=0-3"}),
                    ("GET", "/media/video", {"Accept": "text/html"}),
                    ("GET", "/exports/data", {"Accept": "text/html"}),
                    ("GET", "/assets/app.js", {"Accept": "text/html"}),
                    ("GET", "/logo.svg", {"Accept": "image/avif,image/webp,*/*"}),
                    ("GET", "/_next/data/build/overview.json", {"Accept": "text/html"}),
                    ("GET", "/overview.rsc", {"Accept": "text/html"}),
                    ("GET", "/users.rsc", {"Accept": "text/x-component"}),
                    ("GET", "/overview", {"Accept": "text/x-component", "RSC": "1"}),
                    ("GET", "/overview?_rsc=prefetch", {"Accept": "text/html"}),
                    ("GET", "/overview?_data=route", {"Accept": "text/html"}),
                    ("GET", "/overview", {"Accept": "text/html", "Next-Router-State-Tree": "[]"}),
                    ("GET", "/overview", {"Accept": "text/html", "Sec-Fetch-Dest": "empty"}),
                    ("GET", "/overview", {"Accept": "application/json"}),
                    ("GET", "/pending-approval", {"Accept": "text/x-component", "RSC": "1"}),
                    ("GET", "/api/douyin/authorizations", {}),
                    ("POST", "/api/douyin/oauth/start", {"X-Dcar-Request": "douyin-oauth-start"}),
                    ("GET", "/oauth/douyin/callback?code=mock&state=mock", {"Accept": "application/json"}),
                    ("POST", "/contents", {"Accept": "text/html", "Next-Action": "mock"}),
                    ("PUT", "/api/v8/contents", {}),
                    ("PATCH", "/api/v8/contents", {}),
                    ("DELETE", "/api/v8/contents", {}),
                    ("OPTIONS", "/api/douyin/authorizations", {}),
                    ("POST", "/pending-approval", {}),
                ):
                    with self.subTest(method=method, path=path, headers=headers):
                        denied = client.request(
                            method, _route(base_path, path), headers=headers,
                            follow_redirects=False,
                        )
                        self.assertEqual(denied.status_code, 403, denied.text)
                        self.assertEqual(denied.json()["code"], "account_admin_required" if path in {"/api/douyin/authorizations", "/api/douyin/oauth/start"} else "approval_required")
                        self.assertNotIn("location", denied.headers)
                        self.assertEqual(denied.headers["cache-control"], "no-store")
                if base_path:
                    outside = client.get("/api/v8/overview", follow_redirects=False)
                    self.assertEqual(outside.status_code, 404)

    def test_new_user_current_role_controls_existing_session_and_login_return_to(self) -> None:
        for base_path in ("", "/dcar"):
            with self.subTest(base_path=base_path), self._client(base_path) as (client, config):
                self._seed_user(config, "newcomer", role="new_user")
                waiting = _route(base_path, "/pending-approval")
                self._login_as(client, base_path, "newcomer")
                original_cookie = client.cookies.get(auth_gateway.SESSION_COOKIE)
                for return_to, expected in (
                    (_route(base_path, "/contents"), _route(base_path, "/contents")),
                    (_route(base_path, "/contents/?platform=douyin&_rsc=x&_data=y"),
                     _route(base_path, "/contents?platform=douyin")),
                    (_route(base_path, "/api/v8/overview"), _route(base_path, "/overview")),
                    (_route(base_path, "/users.rsc"), _route(base_path, "/overview")),
                    ("//evil.test", _route(base_path, "/overview")),
                    ("/%2e%2e/overview", _route(base_path, "/overview")),
                ):
                    redirected = client.get(
                        _route(base_path, "/login"), params={"return_to": return_to},
                        follow_redirects=False,
                    )
                    self.assertEqual(redirected.headers["location"], expected)
                store = self._store_for(config)
                store.set_role("newcomer", "operator", actor="test")
                self.assertEqual(client.get(_route(base_path, "/auth/session")).json()["role"], "operator")
                self.assertEqual(client.get(_route(base_path, "/api/v8/overview")).status_code, 200)
                admitted = client.get(waiting, follow_redirects=False)
                self.assertEqual(admitted.headers["location"], _route(base_path, "/overview"))
                store.set_role("newcomer", "new_user", actor="test")
                denied = client.get(_route(base_path, "/api/v8/overview"))
                self.assertEqual(denied.status_code, 403)
                self.assertEqual(denied.json()["code"], "approval_required")
                self.assertEqual(client.cookies.get(auth_gateway.SESSION_COOKIE), original_cookie)

    def test_new_user_code_login_and_password_reset_keep_data_access_pending(self) -> None:
        for base_path in ("", "/dcar"):
            for action in ("login", "reset"):
                with self.subTest(base_path=base_path, action=action), self._client(base_path) as (client, config):
                    store = self._store_for(config)
                    store.set_role(USERNAME, "new_user", actor="test")
                    phone = PHONE
                    if action == "reset":
                        phone = NEW_PHONE
                        self._seed_user(config, "new_reset", role="new_user", phone=phone)
                    sent = self._request_code(client, base_path, phone, action)
                    self.assertEqual(sent.status_code, 200, sent.text)
                    if action == "login":
                        response = self._post(
                            client, base_path, "/auth/login/code",
                            {"phone": phone, "code": self._last_code(), "return_to": _route(base_path, "/users")},
                        )
                    else:
                        verified = self._post(
                            client, base_path, "/auth/reset/verify",
                            {"phone": phone, "code": self._last_code()},
                        )
                        self.assertEqual(verified.status_code, 200, verified.text)
                        response = self._post(
                            client, base_path, "/auth/reset/confirm",
                            {"reset_token": verified.json()["reset_token"], "password": "A-new-safe-passphrase",
                             "return_to": _route(base_path, "/api/v8/overview")},
                        )
                    self.assertEqual(response.status_code, 200, response.text)
                    self.assertEqual(response.json(), {"redirect_to": _route(base_path, "/overview")})
                    self.assertEqual(client.get(_route(base_path, "/auth/session")).json()["role"], "new_user")
                    denied = client.get(_route(base_path, "/api/v8/overview"))
                    self.assertEqual(denied.status_code, 403)
                    self.assertEqual(denied.json()["code"], "approval_required")

    def test_user_list_and_page_gate_follow_roles(self) -> None:
        for base_path in ("", "/dcar"):
            with self.subTest(base_path=base_path), self._client(base_path) as (client, config):
                self._seed_user(config, "boss", role="superadmin", phone="13800000001")
                self._seed_user(config, "lead", role="admin")

                anonymous = client.get(_route(base_path, "/auth/users"))
                self.assertEqual(anonymous.status_code, 401)
                self.assertEqual(anonymous.json(), {"detail": "请先登录"})
                page = client.get(_route(base_path, "/users"), follow_redirects=False)
                self.assertEqual(page.status_code, 302)

                self._login_as(client, base_path, USERNAME)
                denied = client.get(_route(base_path, "/auth/users"))
                self.assertEqual(denied.status_code, 403)
                self.assertEqual(denied.json()["code"], "forbidden")
                gate = client.get(_route(base_path, "/users"), follow_redirects=False)
                self.assertEqual(gate.status_code, 303)
                self.assertEqual(gate.headers["location"], _route(base_path, "/overview"))
                nested = client.get(_route(base_path, "/users/anything"), follow_redirects=False)
                self.assertEqual(nested.status_code, 303)
                sibling = client.get(_route(base_path, "/users-report"))
                self.assertEqual(sibling.status_code, 200)
                self.assertEqual(sibling.json()["upstream"], "web")
                session = client.get(_route(base_path, "/auth/session")).json()
                self.assertEqual(session["role"], "operator")
                client.post(
                    _route(base_path, "/auth/logout"),
                    headers={"Origin": ORIGIN, "X-Dcar-Request": "logout"},
                )

                self._login_as(client, base_path, "lead")
                listed = client.get(_route(base_path, "/auth/users"))
                self.assertEqual(listed.status_code, 200)
                body = listed.json()
                self.assertEqual(body["actor"], {"username": "lead", "role": "admin"})
                self.assertEqual(
                    [item["username"] for item in body["items"]], ["lead", "boss", USERNAME]
                )
                self.assertEqual(body["items"][1]["phone"], "13800000001")
                self.assertEqual(body["items"][1]["status"], "active")
                self.assertEqual(body["items"][1]["role"], "superadmin")
                self.assertNotIn("password_hash", body["items"][0])
                self.assertNotIn("active_sessions", body["items"][0])
                self.assertEqual(listed.headers.get("cache-control"), "no-store")
                proxied = client.get(_route(base_path, "/users"))
                self.assertEqual(proxied.status_code, 200)
                self.assertEqual(proxied.json()["authenticated_user"], "lead")
                self.assertEqual(client.post(_route(base_path, "/auth/users")).status_code, 405)

    def test_bypass_mode_hides_user_management_entirely(self) -> None:
        with self._client("", bypass_auth=True) as (client, _config):
            self.assertEqual(client.get("/auth/users").status_code, 404)
            self.assertEqual(
                self._post_users(client, "", "update", {"username": "x"}).status_code, 404
            )
            self.assertNotIn("role", client.get("/auth/session").json())
            gate = client.get("/users", follow_redirects=False)
            self.assertEqual(gate.status_code, 303)

    def test_update_contract_validates_before_touching_the_store(self) -> None:
        with self._client("") as (client, config):
            self._seed_user(config, "lead", role="admin")
            self._login_as(client, "", "lead")
            base = {"username": USERNAME, "phone": "", "role": "operator", "password": ""}

            cross_origin = self._post_users(
                client, "", "update", base, headers={"Origin": "https://evil.test"}
            )
            self.assertEqual(cross_origin.status_code, 403)
            self.assertEqual(cross_origin.json()["code"], "origin_mismatch")

            form = client.post(
                "/auth/users/update",
                data={"username": USERNAME},
                headers={"Origin": ORIGIN, "X-Dcar-Request": "user-update"},
            )
            self.assertEqual(form.status_code, 415)

            no_length = client.post(
                "/auth/users/update",
                content=iter([b"{}"]),
                headers={
                    "Origin": ORIGIN,
                    "X-Dcar-Request": "user-update",
                    "Content-Type": "application/json",
                },
            )
            self.assertEqual(no_length.status_code, 411)
            self.assertEqual(no_length.json()["code"], "length_required")

            oversized = client.post(
                "/auth/users/update",
                content=b"{}",
                headers={
                    "Origin": ORIGIN,
                    "X-Dcar-Request": "user-update",
                    "Content-Type": "application/json",
                    "Content-Length": str(auth_gateway.MAX_LOGIN_BODY_BYTES + 1),
                },
            )
            self.assertEqual(oversized.status_code, 413)

            for broken in (
                {**base, "extra": 1},
                {"username": USERNAME, "phone": None, "role": "operator", "password": ""},
                {"username": USERNAME},
                [base],
            ):
                with self.subTest(broken=broken):
                    self.assertEqual(
                        self._post_users(client, "", "update", broken).json()["code"],
                        "invalid_payload",
                    )
            self.assertEqual(
                self._post_users(client, "", "update", {**base, "role": "root"}).json()["code"],
                "role_invalid",
            )
            self.assertEqual(
                self._post_users(client, "", "update", {**base, "phone": "12345"}).json()["code"],
                "phone_invalid",
            )
            self.assertEqual(
                self._post_users(client, "", "update", {**base, "password": "short"}).json()["code"],
                "invalid_password",
            )
            for common in (f"xx{USERNAME}xx", "qwerty123"):
                self.assertEqual(
                    self._post_users(
                        client, "", "update", {**base, "password": common}
                    ).json()["code"],
                    "password_too_common",
                )
            missing = self._post_users(client, "", "update", {**base, "username": "ghost"})
            self.assertEqual(missing.status_code, 404)
            self.assertEqual(missing.json()["code"], "user_not_found")

    def test_update_enforces_role_hierarchy_and_self_protection(self) -> None:
        with self._client("") as (client, config):
            store = self._store_for(config)
            self._seed_user(config, "boss", role="superadmin")
            self._seed_user(config, "lead", role="admin")
            self._login_as(client, "", "lead")

            promote = self._post_users(
                client, "", "update",
                {"username": USERNAME, "phone": "", "role": "superadmin", "password": ""},
            )
            self.assertEqual(promote.status_code, 403)
            self.assertEqual(promote.json()["code"], "role_forbidden")

            touch_boss = self._post_users(
                client, "", "update",
                {"username": "boss", "phone": "", "role": "superadmin", "password": ""},
            )
            self.assertEqual(touch_boss.json()["code"], "target_forbidden")

            self_role = self._post_users(
                client, "", "update",
                {"username": "LEAD", "phone": "", "role": "operator", "password": ""},
            )
            self.assertEqual(self_role.json()["code"], "self_role_change")
            self_password = self._post_users(
                client, "", "update",
                {"username": "Lead", "phone": "", "role": "admin", "password": "another-secret-1"},
            )
            self.assertEqual(self_password.json()["code"], "self_password_change")
            self_phone = self._post_users(
                client, "", "update",
                {"username": "lead", "phone": "13900000000", "role": "admin", "password": ""},
            )
            self.assertEqual(self_phone.status_code, 200, self_phone.text)
            self.assertEqual(self_phone.json()["item"]["phone"], "13900000000")
            self.assertEqual(client.get("/auth/session").json()["role"], "admin")
            self_delete = self._post_users(client, "", "delete", {"username": "lead"})
            self.assertEqual(self_delete.json()["code"], "self_delete")

            promoted = self._post_users(
                client, "", "update",
                {"username": USERNAME, "phone": "13900000000", "role": "admin", "password": ""},
            )
            self.assertEqual(promoted.status_code, 409)
            self.assertEqual(promoted.json()["code"], "phone_conflict")
            promoted = self._post_users(
                client, "", "update",
                {"username": USERNAME, "phone": "", "role": "admin", "password": "brand-new-secret"},
            )
            self.assertEqual(promoted.status_code, 200, promoted.text)
            self.assertEqual(promoted.json()["item"]["role"], "admin")
            self.assertEqual(store.get_user(USERNAME).role, "admin")
            # The fixture phone was removed and its admission revoked; the old
            # password no longer logs in, the new one does.
            self.assertIsNone(store.get_user(USERNAME).phone)
            self.assertFalse(store.phone_allowed(PHONE))
            self.assertEqual(self._login(client, "", password=PASSWORD).status_code, 401)
            self.assertEqual(self._login(client, "", password="brand-new-secret").status_code, 200)
            actions = [entry["action"] for entry in store.read_changes()]
            self.assertEqual(actions.count("user.update"), 2)

    def test_admin_password_reset_kills_target_sessions_immediately(self) -> None:
        with self._client("") as (client, config):
            self._seed_user(config, "boss", role="superadmin")
            self._login_as(client, "", USERNAME)
            operator_cookie = client.cookies.get(auth_gateway.SESSION_COOKIE)
            client.cookies.clear()
            self._login_as(client, "", "boss")
            reset = self._post_users(
                client, "", "update",
                {"username": USERNAME, "phone": PHONE, "role": "operator", "password": "reset-by-boss-1"},
            )
            self.assertEqual(reset.status_code, 200, reset.text)
            client.cookies.clear()
            replay = client.get(
                "/selling-points",
                headers={"Cookie": f"{auth_gateway.SESSION_COOKIE}={operator_cookie}"},
                follow_redirects=False,
            )
            self.assertEqual(replay.status_code, 302)

    def test_delete_revokes_access_and_phone_admission(self) -> None:
        with self._client("") as (client, config):
            store = self._store_for(config)
            self._seed_user(config, "boss", role="superadmin")
            self._seed_user(config, "lead", role="admin")
            self._seed_user(config, "temp", role="operator", phone="13700000000")
            store.allow_phone("13700000000", "temp", actor="test")
            self._login_as(client, "", "temp")
            temp_cookie = client.cookies.get(auth_gateway.SESSION_COOKIE)
            client.cookies.clear()

            self._login_as(client, "", "lead")
            forbidden = self._post_users(client, "", "delete", {"username": "boss"})
            self.assertEqual(forbidden.status_code, 403)
            self.assertEqual(forbidden.json(), {"detail": "无权删除该用户", "code": "target_forbidden"})
            deleted = self._post_users(client, "", "delete", {"username": "TEMP"})
            self.assertEqual(deleted.status_code, 200, deleted.text)
            self.assertEqual(deleted.json(), {})

            self.assertIsNone(store.get_user("temp"))
            self.assertTrue(store.username_reserved("Temp"))
            self.assertFalse(store.phone_allowed("13700000000"))
            with sqlite3.connect(config.session_db_path) as connection:
                tombstone = connection.execute(
                    "SELECT deleted_by, role, phone FROM auth_deleted_users WHERE username='temp'"
                ).fetchone()
            self.assertEqual(tombstone, ("lead", "operator", "13700000000"))
            client.cookies.clear()
            replay = client.get(
                "/auth/session",
                headers={"Cookie": f"{auth_gateway.SESSION_COOKIE}={temp_cookie}"},
            )
            self.assertEqual(replay.status_code, 401)
            with self.assertRaises(auth_store.UsernameTaken):
                store.create_user("temp", self.password_hash, actor="test")
            self._login_as(client, "", "lead")
            self.assertEqual(
                self._post_users(client, "", "delete", {"username": "temp"}).status_code, 404
            )
            self.assertEqual(
                [entry["action"] for entry in store.read_changes()][-1], "user.delete"
            )

    def test_last_superadmin_and_revoked_session_are_rejected_inside_the_transaction(self) -> None:
        with self._client("") as (client, config):
            store = self._store_for(config)
            self._seed_user(config, "boss", role="superadmin")
            self._seed_user(config, "lead", role="admin")
            self._login_as(client, "", "boss")
            boss_token = client.cookies.get(auth_gateway.SESSION_COOKIE)
            resolved = store.resolve_principal(boss_token)
            self.assertEqual(resolved.role, "superadmin")
            # Race: the request resolved boss's session, then (before the
            # transaction) that session was revoked — the in-transaction
            # re-verification must refuse and clear the cookie.
            with sqlite3.connect(config.session_db_path) as connection:
                connection.execute("DELETE FROM auth_sessions WHERE username='boss'")
            with patch.object(auth_store.AuthStore, "resolve_principal", return_value=resolved):
                revoked = self._post_users(
                    client, "", "update",
                    {"username": "lead", "phone": "", "role": "operator", "password": ""},
                )
            self.assertEqual(revoked.status_code, 401)
            self.assertEqual(revoked.json()["code"], "session_revoked")
            self.assertIn("Max-Age=0", revoked.headers.get("set-cookie", ""))
            self.assertEqual(store.get_user("lead").role, "admin")

            client.cookies.clear()
            self._login_as(client, "", "boss")
            with self.assertRaises(auth_store.LastSuperadmin):
                store.set_role("boss", "admin", actor="test")
            with self.assertRaises(auth_store.LastSuperadmin):
                store.delete_user_cli("boss", actor="test")
            store.set_role("lead", "superadmin", actor="test")
            demote = self._post_users(
                client, "", "update",
                {"username": "lead", "phone": "", "role": "operator", "password": ""},
            )
            self.assertEqual(demote.status_code, 200, demote.text)

    def test_storage_failures_map_to_a_plain_503_contract(self) -> None:
        with self._client("") as (client, config):
            self._seed_user(config, "lead", role="admin")
            self._login_as(client, "", "lead")
            with patch.object(
                auth_store.AuthStore, "list_users", side_effect=sqlite3.OperationalError("locked")
            ):
                listed = client.get("/auth/users")
            self.assertEqual(listed.status_code, 503)
            self.assertEqual(listed.json()["code"], "storage_unavailable")
            with patch.object(
                auth_store.AuthStore, "delete_user", side_effect=sqlite3.DatabaseError("corrupt")
            ):
                deleted = self._post_users(client, "", "delete", {"username": USERNAME})
            self.assertEqual(deleted.status_code, 503)
            self.assertEqual(deleted.json(), {"detail": "暂时无法处理，请稍后重试", "code": "storage_unavailable"})

    def test_change_log_defaults_next_to_the_store_and_follows_the_environment(self) -> None:
        config = self._config("")
        self.assertEqual(config.change_log_path, config.session_db_path.parent / "auth-changes.log")
        with patch.dict(
            os.environ,
            {
                "DCAR_AUTH_SESSION_DB": str(self.root / "env.sqlite3"),
                "DCAR_AUTH_CHANGE_LOG": str(self.root / "elsewhere" / "changes.log"),
                "DCAR_AUTH_SMS_PROVIDER": "tencent",
            },
            clear=True,
        ):
            from_env = auth_gateway.AuthGatewayConfig.from_env()
        self.assertEqual(from_env.change_log_path, self.root / "elsewhere" / "changes.log")

    def test_session_store_outage_preserves_cookie_and_returns_503(self) -> None:
        with self._client("") as (client, _config):
            self.assertEqual(self._login(client, "").status_code, 200)
            token = client.cookies.get(auth_gateway.SESSION_COOKIE)
            with patch.object(
                auth_store.AuthStore, "resolve_principal",
                side_effect=sqlite3.OperationalError("locked"),
            ):
                for path in ("/overview", "/auth/session", "/auth/users", "/api/v8/overview", "/login"):
                    with self.subTest(path=path):
                        result = client.get(path, follow_redirects=False)
                        self.assertEqual(result.status_code, 503)
                        self.assertEqual(result.json()["code"], "storage_unavailable")
                        self.assertEqual(result.headers["cache-control"], "no-store")
                        self.assertNotIn("set-cookie", result.headers)
                        self.assertEqual(client.cookies.get(auth_gateway.SESSION_COOKIE), token)
                result = self._post_users(client, "", "delete", {"username": USERNAME})
                self.assertEqual(result.status_code, 503)
                self.assertNotIn("set-cookie", result.headers)
            self.assertEqual(client.get("/auth/session").status_code, 200)

    def test_password_final_writes_and_logout_map_store_errors_to_503(self) -> None:
        with self._client("") as (client, _config):
            for method, password in (("finish_password_login", PASSWORD), ("record_login_failure", "wrong")):
                with self.subTest(method=method), patch.object(
                    auth_store.AuthStore, method, side_effect=sqlite3.OperationalError("write failed")
                ):
                    result = self._login(client, "", password=password)
                    self.assertEqual(result.status_code, 503)
                    self.assertEqual(result.json(), {"detail": "暂时无法登录，请稍后重试"})
                    self.assertNotIn("set-cookie", result.headers)
            self.assertEqual(self._login(client, "").status_code, 200)
            token = client.cookies.get(auth_gateway.SESSION_COOKIE)
            with patch.object(auth_store.AuthStore, "revoke_session", side_effect=sqlite3.OperationalError("locked")):
                result = client.post("/auth/logout", headers={"Origin": ORIGIN, "X-Dcar-Request": "logout"})
                self.assertEqual(result.status_code, 503)
                self.assertEqual(client.cookies.get(auth_gateway.SESSION_COOKIE), token)
                self.assertNotIn("set-cookie", result.headers)

    def test_concurrent_password_attempts_are_rejected_before_hashing(self) -> None:
        entered, release = threading.Event(), threading.Event()

        def slow_verify(*_args: object) -> bool:
            entered.set()
            if not release.wait(10):
                raise AssertionError("test failed to release password worker")
            return False

        with self._client("") as (client, config), patch.object(
            auth_store, "verify_password", side_effect=slow_verify
        ) as hashing, ThreadPoolExecutor(max_workers=20) as pool:
            first = pool.submit(self._login, client, "", password="wrong")
            try:
                self.assertTrue(entered.wait(5))
                requests = [pool.submit(self._login, client, "", password="wrong") for _ in range(19)]
                results = [request.result(timeout=5) for request in requests]
                self.assertEqual([r.status_code for r in results], [429] * 19)
                self.assertTrue(all(r.headers.get("retry-after") == "1" for r in results))
                self.assertEqual(hashing.call_count, 1)
            finally:
                release.set()
            self.assertEqual(first.result(timeout=5).status_code, 401)
            self.assertEqual(self._store_for(config).failure_count(auth_store.AuthStore.user_key(USERNAME)), 1)

    def test_different_accounts_can_log_in_concurrently_from_the_same_ip(self) -> None:
        both_hashing = threading.Barrier(2, timeout=5)
        original_verify = auth_store.verify_password

        def concurrent_verify(password: str, stored_hash: str) -> bool:
            # Neither request can finish unless the other is also admitted.
            both_hashing.wait()
            return original_verify(password, stored_hash)

        with self._client("") as (client, config):
            self._seed_user(config, "office_colleague", role="operator")

            async def attempt(username: str) -> tuple[int, str]:
                transport = ASGITransport(app=client.app, client=("203.0.113.9", 41234))
                async with httpx.AsyncClient(transport=transport, base_url=ORIGIN) as office_client:
                    response = await office_client.post(
                        "/auth/login",
                        data={"username": username, "password": PASSWORD, "remember": "0"},
                        headers={"Origin": ORIGIN, "X-Dcar-Request": "login"},
                    )
                    self.assertEqual(response.status_code, 200, response.text)
                    session = await office_client.get("/auth/session")
                    self.assertEqual(session.status_code, 200, session.text)
                    return response.status_code, session.json()["username"]

            async def scenario() -> list[tuple[int, str]]:
                return await asyncio.gather(attempt(USERNAME), attempt("office_colleague"))

            with patch.object(auth_store, "verify_password", side_effect=concurrent_verify) as hashing:
                self.assertEqual(
                    asyncio.run(scenario()),
                    [(200, USERNAME), (200, "office_colleague")],
                )
                self.assertEqual(hashing.call_count, 2)

    def test_password_failure_ip_limit_still_covers_different_accounts(self) -> None:
        config = self._config("")
        config = auth_gateway.AuthGatewayConfig(
            **{**config.__dict__, "throttle_max_failures": 2}
        )
        store = self._seed(config)
        self._seed_user(config, "office_colleague", role="operator")
        app = auth_gateway.create_app(
            config,
            web_transport=ASGITransport(app=_echo_upstream("web")),
            api_transport=ASGITransport(app=_echo_upstream("api")),
            sms_transport=self._sms_transport(),
        )
        with TestClient(app, base_url=ORIGIN) as client:
            self.assertEqual(self._login(client, "", password="wrong").status_code, 401)
            self.assertEqual(
                self._login(client, "", username="someone_else", password="wrong").status_code,
                401,
            )
            self.assertEqual(store.failure_count(store.ip_key("testclient")), 2)
            self.assertEqual(store.failure_count(store.user_key("office_colleague")), 0)
            with patch.object(auth_store, "verify_password", wraps=auth_store.verify_password) as hashing:
                blocked = self._login(client, "", username="office_colleague")
                self.assertEqual(blocked.status_code, 429)
                self.assertGreater(int(blocked.headers["retry-after"]), 0)
                hashing.assert_not_called()

    def test_password_work_has_global_cap_and_keeps_cancelled_worker_slot(self) -> None:
        async def scenario() -> None:
            work = auth_gateway.PasswordWork()
            entered, release = threading.Event(), threading.Event()

            def slow() -> None:
                entered.set()
                release.wait(5)

            async def attempt() -> None:
                async with work.slot("alice"):
                    await work.run(slow)

            task = asyncio.create_task(attempt())
            while not entered.is_set():
                await asyncio.sleep(0.001)
            task.cancel()
            await asyncio.sleep(0)
            try:
                with self.assertRaises(auth_store.RateLimited):
                    async with work.slot("ALICE"):
                        self.fail("cancelled hash released its slot too early")
                async with work.slot("b"), work.slot("c"), work.slot("d"):
                    with self.assertRaises(auth_store.RateLimited):
                        async with work.slot("e"):
                            self.fail("global work limit was exceeded")
            finally:
                release.set()
                with self.assertRaises(asyncio.CancelledError):
                    await task
            self.assertEqual(work.active, 0)
            self.assertFalse(work.keys)
        asyncio.run(scenario())

    def test_bounded_body_stops_before_reading_remaining_chunks(self) -> None:
        async def scenario() -> None:
            chunks_read = 0

            async def receive() -> dict:
                nonlocal chunks_read
                chunks_read += 1
                if chunks_read > 2:
                    self.fail("oversized request continued buffering")
                return {"type": "http.request", "body": b"x" * 9000, "more_body": True}

            request = Request({"type": "http", "method": "POST", "headers": [
                (b"content-type", b"application/x-www-form-urlencoded"), (b"content-length", b"1")
            ]}, receive=receive)
            result = await auth_gateway._bounded_form(request)
            self.assertIsInstance(result, Response)
            self.assertEqual(result.status_code, 413)
            self.assertEqual(chunks_read, 2)
        asyncio.run(scenario())

    def test_invalidated_during_sms_dispatch_does_not_report_success(self) -> None:
        with self._client("") as (client, config):
            original = auth_store.AuthStore.finish_send

            def finish(store: auth_store.AuthStore, *args: object) -> bool:
                store.revoke_challenges()
                return original(store, *args)

            with patch.object(auth_store.AuthStore, "finish_send", finish):
                result = self._request_code(client, "", PHONE, "login")
            self.assertEqual(result.status_code, 503)
            self.assertEqual(len(self.sms_requests), 1)
            self.assertEqual(self._post(client, "", "/auth/login/code", {"phone": PHONE, "code": self._last_code()}).status_code, 401)

    def test_tencent_internal_dispatch_errors_are_unknown_and_keep_references(self) -> None:
        async def scenario() -> None:
            credentials = auth_sms.load_sms_credentials(self._sms_credentials_path())
            for code in ("InternalError.Timeout", "InternalError.SendAndRecvFail"):
                for payload in (_tencent_error(code), _tencent_status(code)):
                    sender = auth_sms.TencentSmsSender(credentials, transport=httpx.MockTransport(
                        lambda _request: httpx.Response(200, json=payload)
                    ))
                    try:
                        outcome = await sender.send(PHONE, "123456")
                        self.assertEqual(outcome.status, "unknown")
                        self.assertEqual(outcome.provider_code, code)
                        self.assertTrue(outcome.request_id)
                    finally:
                        await sender.aclose()
            sender = auth_sms.TencentSmsSender(credentials, transport=httpx.MockTransport(
                lambda _request: httpx.Response(200, json=_tencent_status("Ok"))
            ))
            try:
                outcome = await sender.send(PHONE, "123456")
                self.assertEqual(outcome.status, "sent")
                self.assertEqual(outcome.request_id, _tencent_status("Ok")["Response"]["RequestId"])
                self.assertEqual(outcome.serial_no, _tencent_status("Ok")["Response"]["SendStatusSet"][0]["SerialNo"])
            finally:
                await sender.aclose()
        asyncio.run(scenario())


    def test_account_admin_guards_all_page_and_data_forms_without_proxy(self) -> None:
        cases = [
            ("GET", path, {"Accept": "text/html"}, 303)
            for path in ("/accounts", "/accounts/", "/accounts/9", "/accounts/authorization/douyin",
                         "/anything/%2e%2e/accounts", "/%61ccounts", "/douyin")
        ] + [
            ("GET", path, {"Accept": "text/html"}, 403)
            for path in ("/accounts.rsc", "/accounts.json", "/_next/data/build/accounts.json",
                         "/_next/data/build/accounts/9.json", "/api%2fv8/accounts/search",
                         "/api/v8/anything/%2e%2e/accounts/export", "/workbench-api/accounts/export",
                         "/api/v8/%2561ccounts/export", "/api/v8/accounts%2Fexport",
                         "/api/v8/accounts%3fanything", "/accounts.rsc%3f_rsc=fixture",
                         "/accounts.rsc/nested", "/api/v8/accounts%23fragment")
        ] + [
            ("GET", "/accounts", headers, 403) for headers in (
                {}, {"Accept": "text/x-component"}, {"Accept": "text/html", "RSC": "1"},
                {"Accept": "text/html", "Next-Router-State-Tree": "[]"},
                {"Accept": "text/html", "Next-Router-Prefetch": "1"},
                {"Accept": "text/html", "Sec-Fetch-Dest": "empty"},
            )
        ] + [
            (method, path, {}, 403) for method in ("GET", "HEAD", "POST", "PUT", "PATCH", "DELETE", "OPTIONS")
            for path in ("/api/v8/accounts", "/api/v8/accounts/search", "/api/v8/accounts/import",
                         "/api/v8/accounts/export", "/api/v8/accounts/9", "/api/v8/account-roster/import",
                         "/api/v8/account-roster/system/bootstrap", "/workbench-api/account-roster/system/bootstrap",
                         "/api/douyin/oauth/start", "/api/douyin/authorizations/unbind",
                         "/api/douyin/authorization-statuses", "/api/douyin/accounts/search")
        ] + [("POST", "/accounts", {"Accept": "text/html"}, 403),
             ("GET", "/accounts?_rsc=x", {"Accept": "text/html"}, 403),
             ("GET", "/accounts?_data=x", {"Accept": "text/html"}, 403)]
        for base_path in ("", "/dcar"):
            with self.subTest(base_path=base_path), self._client(base_path, douyin_enabled=True) as (client, config):
                self._seed_user(config, "newcomer", role="new_user")
                for username in (USERNAME, "newcomer"):
                    self._login_as(client, base_path, username)
                    with ExitStack() as stack:
                        upstream = [stack.enter_context(patch.object(getattr(client.app.state, name), "send"))
                                    for name in ("web_client", "api_client", "douyin_client")]
                        for method, path, headers, status in cases:
                            with self.subTest(username=username, method=method, path=path, headers=headers):
                                response = client.request(method, _route(base_path, path), headers=headers,
                                                          content=b"invalid JSON", follow_redirects=False)
                                self.assertEqual(response.status_code, status, response.text)
                                self.assertEqual(response.headers["cache-control"], "no-store")
                                if status == 303:
                                    self.assertEqual(response.headers["location"], _route(base_path, "/overview"))
                                elif method != "HEAD":
                                    self.assertEqual(response.json()["code"], "account_admin_required")
                        for send in upstream:
                            send.assert_not_called()


    def test_account_admin_and_superadmin_can_use_accounts_roster_and_exports(self) -> None:
        for base_path in ("", "/dcar"):
            with self._client(base_path, douyin_enabled=True) as (client, config):
                self._seed_user(config, "lead", role="admin")
                self._seed_user(config, "boss", role="superadmin")
                for username in ("lead", "boss"):
                    self._login_as(client, base_path, username)
                    for path in ("/accounts", "/accounts/7", "/accounts.rsc", "/_next/data/build/accounts.json",
                                 "/workbench-api/accounts", "/workbench-api/account-roster/import"):
                        response = client.get(_route(base_path, path))
                        self.assertEqual(response.status_code, 200, response.text)
                        self.assertEqual(response.json()["authenticated_user"], username)
                    for method in ("GET", "POST", "OPTIONS"):
                        for path in ("/api/v8/accounts", "/api/v8/accounts/search", "/api/v8/accounts/export",
                                     "/api/v8/account-roster/import", "/api/v8/account-roster/system/bootstrap"):
                            response = client.request(method, _route(base_path, path))
                            self.assertEqual(response.status_code, 200, response.text)
                            self.assertEqual(response.json()["upstream"], "api")
                    authorization = client.get(_route(base_path, "/api/douyin/authorizations"))
                    self.assertEqual(authorization.status_code, 200)
                    self.assertEqual(authorization.json()["upstream"], "douyin")
                    started = client.post(_route(base_path, "/api/douyin/oauth/start"), content="{}", headers={
                        "Origin": ORIGIN, "X-Dcar-Request": "douyin-oauth-start"})
                    self.assertEqual(started.status_code, 200, started.text)


    def test_account_guard_preserves_operator_business_routes_and_oauth_callback(self) -> None:
        for base_path in ("", "/dcar"):
            with self._client(base_path, douyin_enabled=True) as (client, _config):
                self._login_as(client, base_path, USERNAME)
                for path in ("/overview", "/contents", "/accounts-report", "/api/v8/overview",
                             "/api/v8/contents/search", "/api/v8/contents/export", "/api/v8/accounts-report",
                             "/workbench-api/content-update-jobs", "/workbench-api/contents/9/update-jobs",
                             "/oauth/douyin/callback?code=fixture&state=fixture"):
                    response = client.get(_route(base_path, path))
                    self.assertEqual(response.status_code, 200, response.text)
                    self.assertEqual(response.json()["authenticated_user"], USERNAME)
                response = client.post(_route(base_path, "/workbench-api/contents/9/update-jobs"), json={})
                self.assertEqual(response.status_code, 200)


    def test_account_guard_rechecks_existing_session_and_unknown_roles_fail_closed(self) -> None:
        with self._client("") as (client, config):
            self._store_for(config).set_role(USERNAME, "admin", actor="test")
            self._login_as(client, "", USERNAME)
            cookie = client.cookies.get(auth_gateway.SESSION_COOKIE)
            self.assertEqual(client.get("/api/v8/accounts/search").status_code, 200)
            self._store_for(config).set_role(USERNAME, "operator", actor="test")
            self.assertEqual(client.get("/api/v8/accounts/search").status_code, 403)
            self.assertEqual(client.cookies.get(auth_gateway.SESSION_COOKIE), cookie)
            for role in (None, "future-role"):
                principal = auth_store.Principal(username=USERNAME, role=role, token_sha256="fixture")
                with patch.object(auth_store.AuthStore, "resolve_principal", return_value=principal):
                    with patch.object(client.app.state.api_client, "send") as proxy:
                        self.assertEqual(client.get("/api/v8/accounts/search").status_code, 403)
                        proxy.assert_not_called()
        with self._client("", bypass_auth=True) as (client, _config):
            with patch.object(client.app.state.api_client, "send") as proxy:
                self.assertEqual(client.get("/api/v8/accounts/export").status_code, 403)
                self.assertEqual(client.options("/workbench-api/accounts").status_code, 403)
                response = client.get("/accounts", headers={"Accept": "text/html"}, follow_redirects=False)
                self.assertEqual(response.status_code, 303)
                self.assertEqual(response.headers["location"], "/overview")
                proxy.assert_not_called()


    def test_account_destination_is_removed_from_new_user_navigation_and_login(self) -> None:
        for base_path in ("", "/dcar"):
            with self._client(base_path) as (client, config):
                self._seed_user(config, "newcomer", role="new_user")
                for username in (USERNAME, "newcomer"):
                    response = self._login(client, base_path, username=username,
                                           return_to=_route(base_path, "/accounts/authorization/douyin"))
                    self.assertEqual(response.json()["redirect_to"], _route(base_path, "/overview"))
                with patch.object(client.app.state.web_client, "send") as web:
                    with patch.object(client.app.state.api_client, "send") as api:
                        for page in auth_gateway.NEW_USER_PAGES:
                            response = client.get(_route(base_path, page), headers={"Accept": "text/html"})
                            self.assertEqual(response.status_code, 200, response.text)
                            self.assertNotIn('href="' + _route(base_path, "/accounts"), response.text)
                            self.assertNotIn("运营账号", response.text)
                            self.assertIn("退出登录", response.text)
                        web.assert_not_called()
                        api.assert_not_called()


if __name__ == "__main__":
    unittest.main()
