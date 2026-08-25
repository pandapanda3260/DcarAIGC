from __future__ import annotations

import ast
import base64
import hashlib
import json
import os
import sys
import tempfile
import unittest
from contextlib import contextmanager
from pathlib import Path
from typing import Iterator, cast
from unittest.mock import AsyncMock, patch
from urllib.parse import parse_qs, urlsplit

from cryptography.fernet import Fernet
from fastapi import FastAPI, Request
from fastapi.responses import JSONResponse
from fastapi.testclient import TestClient
from httpx import ASGITransport, Response as HttpxResponse


REPOSITORY_ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(REPOSITORY_ROOT / "src" / "dcar_eval"))

from dcar_douyin_control.app import create_app  # noqa: E402
from dcar_douyin_control.config import DouyinControlConfig  # noqa: E402
from dcar_douyin_control.provider import (  # noqa: E402
    DouyinOAuthClient,
    DouyinProviderError,
    VideoListPage,
)
from dcar_douyin_control.tokens import (  # noqa: E402
    ReauthorizationRequired,
    TokenLifecycleError,
)
from tests.fixtures.mock_douyin_oauth import (  # noqa: E402
    MockDouyinState,
    create_mock_douyin_oauth,
)


EDGE_KEY = "edge-key-for-douyin-control-tests-0001"
MACHINE_KEY = "machine-key-for-douyin-control-tests-01"
USERNAME = "operator"
SESSION_BINDING = "a" * 64
CLIENT_KEY = "mock-client-key"
CLIENT_SECRET = "mock-client-secret-at-least-32-bytes"
PLATFORM_UID = "123456789"
NOW = 1_900_000_000


class AccountUpstream:
    def __init__(self, *, resolver_outcome: str = "matched") -> None:
        self.calls: list[dict[str, object]] = []
        self.resolve_calls: list[list[str]] = []
        self.resolver_outcome = resolver_outcome
        self.app = FastAPI()

        @self.app.post("/api/v8/accounts/search")
        async def search(request: Request) -> JSONResponse:
            payload = await request.json()
            self.calls.append(payload)
            query = str(payload.get("query", ""))
            page = int(payload.get("page", 1))
            page_size = int(payload.get("page_size", 50))
            if query == PLATFORM_UID:
                if page == 1:
                    items = [
                        self._account(
                            account_id=1_000 + index,
                            uid=str(800_000_000 + index),
                            nickname=f"占位{index}",
                        )
                        for index in range(100)
                    ]
                elif page == 2:
                    items = [self._account()]
                else:
                    items = []
                total = 101
            else:
                items = [self._account()]
                total = 1
            return JSONResponse(
                {
                    "items": items,
                    "total": total,
                    "page": page,
                    "page_size": page_size,
                    "pending_platform_identities": [
                        {"platform": "douyin", "uid": "pending-secret"}
                    ],
                }
            )

        @self.app.post("/api/v8/accounts/resolve-douyin-videos")
        async def resolve_douyin_videos(request: Request) -> JSONResponse:
            payload = await request.json()
            video_ids = [str(value) for value in payload.get("video_ids", [])]
            self.resolve_calls.append(video_ids)
            if self.resolver_outcome == "unavailable":
                return JSONResponse({"detail": "resolver unavailable"}, status_code=503)
            if self.resolver_outcome == "matched":
                return JSONResponse(
                    {
                        "status": "matched",
                        "matched_account": {
                            "account_id": 7,
                            "platform_uid": PLATFORM_UID,
                        },
                        "candidate_accounts": [],
                        "unmatched_video_ids": [],
                        "matched_video_count": len(video_ids),
                    }
                )
            if self.resolver_outcome == "ambiguous":
                return JSONResponse(
                    {
                        "status": "ambiguous",
                        "matched_account": None,
                        "candidate_accounts": [
                            {"account_id": 7, "platform_uid": PLATFORM_UID},
                            {"account_id": 8, "platform_uid": "987654321"},
                        ],
                        "unmatched_video_ids": [],
                        "matched_video_count": len(video_ids),
                    }
                )
            if self.resolver_outcome == "unmatched":
                return JSONResponse(
                    {
                        "status": "unmatched",
                        "matched_account": None,
                        "candidate_accounts": [],
                        "unmatched_video_ids": video_ids,
                        "matched_video_count": 0,
                    }
                )
            raise AssertionError(
                f"unsupported resolver outcome: {self.resolver_outcome}"
            )

    @staticmethod
    def _account(
        *,
        account_id: int = 7,
        uid: str = PLATFORM_UID,
        nickname: str = "正式抖音账号",
    ) -> dict[str, object]:
        return {
            "id": account_id,
            "operator_name": "运营账号",
            "enabled": True,
            "phone": "13800138000",
            "account_type": "主播",
            "platforms": [
                {"platform": "douyin", "uid": uid, "nickname": nickname},
                {"platform": "xiaohongshu", "uid": "xhs-secret", "nickname": "小红书"},
            ],
        }


class DouyinControlApiTestCase(unittest.TestCase):
    def setUp(self) -> None:
        self.temporary = tempfile.TemporaryDirectory()
        self.addCleanup(self.temporary.cleanup)
        self.root = Path(self.temporary.name)
        self.edge_path = self.root / "edge.key"
        self.machine_path = self.root / "machine.key"
        self.keyring_path = self.root / "fernet.keys"
        self.hmac_path = self.root / "open-id-hmac.key"
        self.secret_path = self.root / "mock-client-secret"
        self.edge_path.write_text(EDGE_KEY, encoding="utf-8")
        self.machine_path.write_text(MACHINE_KEY, encoding="utf-8")
        self.keyring_path.write_text(
            f"1:{Fernet.generate_key().decode('ascii')}\n", encoding="utf-8"
        )
        self.hmac_path.write_text(
            base64.urlsafe_b64encode(os.urandom(32)).decode("ascii"),
            encoding="utf-8",
        )
        self.secret_path.write_text(CLIENT_SECRET, encoding="utf-8")

    def test_from_env_uses_systemd_credentials_directory(self) -> None:
        credential_root = self.root / "systemd-credentials"
        with patch.dict(
            os.environ,
            {
                "CREDENTIALS_DIRECTORY": str(credential_root),
                "DCAR_DOUYIN_PROXY_URL": "http://127.0.0.1:4176",
            },
            clear=True,
        ):
            config = DouyinControlConfig.from_env()
        self.assertEqual(config.edge_key_path, credential_root / "douyin-edge-key")
        self.assertEqual(
            config.machine_key_path, credential_root / "douyin-machine-key"
        )
        self.assertEqual(
            config.fernet_keyring_path,
            credential_root / "douyin-fernet-keyring",
        )
        self.assertEqual(
            config.open_id_hmac_key_path,
            credential_root / "douyin-open-id-hmac-key",
        )
        self.assertEqual(config.provider_proxy_url, "http://127.0.0.1:4176")

    def test_real_provider_wires_token_manager_and_closes_client(self) -> None:
        config = DouyinControlConfig(
            public_base_path="/dcar",
            vault_path=self.root / "real-vault.sqlite3",
            edge_key_path=self.edge_path,
            machine_key_path=self.machine_path,
            fernet_keyring_path=self.keyring_path,
            open_id_hmac_key_path=self.hmac_path,
            api_upstream="http://api.test",
            authorization_enabled=True,
            provider_mode="douyin",
            client_key=CLIENT_KEY,
            client_secret_path=self.secret_path,
            provider_proxy_url="http://127.0.0.1:4176",
            callback_url="https://dcar.test/dcar/oauth/douyin/callback",
        )
        close = AsyncMock()
        with (
            patch.object(DouyinOAuthClient, "__init__", return_value=None),
            patch.object(DouyinOAuthClient, "aclose", close),
        ):
            app = create_app(
                config, api_transport=ASGITransport(app=AccountUpstream().app)
            )
            with TestClient(app) as client:
                response = client.get(
                    "/internal/v1/health",
                    headers={"X-Dcar-Machine-Key": MACHINE_KEY},
                )
                self.assertEqual(response.status_code, 200)
                self.assertIsNotNone(app.state.token_manager)
        close.assert_awaited_once()

    def _config(
        self,
        base_path: str,
        *,
        enabled: bool = True,
        vault_namespace: str = "",
    ) -> DouyinControlConfig:
        suffix = "root" if not base_path else "dcar"
        if vault_namespace:
            suffix = f"{suffix}-{vault_namespace}"
        return DouyinControlConfig(
            public_base_path=base_path,
            vault_path=self.root / f"vault-{suffix}.sqlite3",
            edge_key_path=self.edge_path,
            machine_key_path=self.machine_path,
            fernet_keyring_path=self.keyring_path,
            open_id_hmac_key_path=self.hmac_path,
            api_upstream="http://api.test",
            authorization_enabled=enabled,
            provider_mode="mock" if enabled else "disabled",
            client_key=CLIENT_KEY if enabled else "",
            client_secret_path=self.secret_path if enabled else None,
            provider_authorize_url=(
                "http://127.0.0.1:4199/platform/oauth/connect/" if enabled else ""
            ),
            provider_token_url=(
                "http://127.0.0.1:4199/oauth/access_token/" if enabled else ""
            ),
            provider_userinfo_url=(
                "http://127.0.0.1:4199/oauth/userinfo/" if enabled else ""
            ),
            callback_url=(
                f"https://dcar.test{base_path}/oauth/douyin/callback" if enabled else ""
            ),
        )

    @staticmethod
    def _headers(
        action: str | None = None,
        *,
        username: str = USERNAME,
        binding: str = SESSION_BINDING,
    ) -> dict[str, str]:
        headers = {
            "X-Dcar-Edge-Key": EDGE_KEY,
            "X-Dcar-Authenticated-User": username,
            "X-Dcar-Session-Binding": binding,
        }
        if action:
            headers["X-Dcar-Verified-Action"] = action
        return headers

    @contextmanager
    def _clients(
        self,
        base_path: str,
        *,
        enabled: bool = True,
        provider_state: MockDouyinState | None = None,
        resolver_outcome: str = "matched",
        vault_namespace: str = "",
    ) -> Iterator[tuple[TestClient, TestClient, AccountUpstream, MockDouyinState]]:
        accounts = AccountUpstream(resolver_outcome=resolver_outcome)
        state = provider_state or MockDouyinState(CLIENT_KEY, CLIENT_SECRET)
        mock_app = create_mock_douyin_oauth(state)
        control_app = create_app(
            self._config(
                base_path,
                enabled=enabled,
                vault_namespace=vault_namespace,
            ),
            api_transport=ASGITransport(app=accounts.app),
            oauth_transport=ASGITransport(app=mock_app) if enabled else None,
            clock=lambda: NOW,
        )
        with (
            TestClient(control_app, base_url="https://control.test") as control,
            TestClient(
                mock_app,
                base_url="http://127.0.0.1:4199",
                follow_redirects=False,
            ) as provider,
        ):
            yield control, provider, accounts, state

    @staticmethod
    def _upstream_target(location: str, base_path: str) -> str:
        parsed = urlsplit(location)
        target = parsed.path
        if base_path:
            if not target.startswith(base_path):
                raise AssertionError(f"public location lost base path: {location}")
            target = target[len(base_path) :] or "/"
        return target + (f"?{parsed.query}" if parsed.query else "")

    def _start_and_callback(
        self,
        control: TestClient,
        provider: TestClient,
        base_path: str,
        *,
        callback_headers: dict[str, str] | None = None,
    ) -> tuple[str, HttpxResponse, str]:
        start = control.post(
            "/api/douyin/oauth/start",
            headers=self._headers("douyin-oauth-start"),
            json={},
        )
        self.assertEqual(start.status_code, 200, start.text)
        authorize_url = start.json()["authorize_url"]
        authorize_query = parse_qs(urlsplit(authorize_url).query)
        self.assertEqual(authorize_query["scope"], ["user_info,video.list"])
        state = authorize_query["state"][0]
        authorized = provider.get(authorize_url)
        self.assertEqual(authorized.status_code, 303)
        callback_target = self._upstream_target(
            authorized.headers["location"], base_path
        )
        callback = control.get(
            callback_target,
            headers=callback_headers or self._headers(),
            follow_redirects=False,
        )
        return state, callback, callback_target

    def test_trust_boundary_pages_and_internal_health(self) -> None:
        for base_path in ("", "/dcar"):
            with (
                self.subTest(base_path=base_path),
                self._clients(base_path) as (
                    control,
                    _provider,
                    _accounts,
                    _state,
                ),
            ):
                self.assertEqual(control.get("/douyin").status_code, 403)
                self.assertEqual(
                    control.get(
                        "/douyin", headers=self._headers(username="temporary-bypass")
                    ).status_code,
                    403,
                )
                page = control.get(
                    "/douyin", headers=self._headers(), follow_redirects=False
                )
                self.assertEqual(page.status_code, 303)
                self.assertEqual(
                    page.headers["location"],
                    f"{base_path}/accounts/douyin-authorization",
                )
                self.assertEqual(page.headers["cache-control"], "no-store")
                self.assertEqual(
                    control.get("/douyin/", headers=self._headers()).status_code, 404
                )
                self.assertEqual(
                    control.head("/douyin", headers=self._headers()).status_code, 405
                )
                health = control.get(
                    "/internal/v1/health",
                    headers={"X-Dcar-Machine-Key": MACHINE_KEY},
                )
                self.assertEqual(health.status_code, 200)
                self.assertEqual(health.json()["vault"]["journal_mode"], "delete")
                self.assertEqual(
                    control.get(
                        "/internal/v1/health",
                        headers={
                            "X-Dcar-Machine-Key": MACHINE_KEY,
                            "X-Dcar-Authenticated-User": USERNAME,
                        },
                    ).status_code,
                    403,
                )

                non_ascii_headers = [
                    (
                        "machine",
                        "/internal/v1/health",
                        "GET",
                        [(b"x-dcar-machine-key", b"\xff")],
                    ),
                    (
                        "edge",
                        "/api/douyin/authorizations",
                        "GET",
                        [
                            (b"x-dcar-edge-key", b"\xff"),
                            (b"x-dcar-authenticated-user", USERNAME.encode()),
                            (b"x-dcar-session-binding", SESSION_BINDING.encode()),
                        ],
                    ),
                    (
                        "action",
                        "/api/douyin/oauth/start",
                        "POST",
                        [
                            (b"x-dcar-edge-key", EDGE_KEY.encode()),
                            (b"x-dcar-authenticated-user", USERNAME.encode()),
                            (b"x-dcar-session-binding", SESSION_BINDING.encode()),
                            (b"x-dcar-verified-action", b"\xff"),
                        ],
                    ),
                ]
                for name, path, method, headers in non_ascii_headers:
                    with self.subTest(base_path=base_path, non_ascii_header=name):
                        response = control.request(
                            method,
                            path,
                            headers=headers,
                            json={} if method == "POST" else None,
                        )
                        self.assertEqual(response.status_code, 403, response.text)
                        self.assertEqual(response.headers["cache-control"], "no-store")

                old_routes = [
                    ("GET", "/douyin/confirm", None),
                    ("POST", "/api/douyin/oauth/confirm", "douyin-oauth-confirm"),
                    ("POST", "/api/douyin/oauth/reject", "douyin-oauth-reject"),
                ]
                for method, path, action in old_routes:
                    with self.subTest(base_path=base_path, old_route=path):
                        response = control.request(
                            method,
                            path,
                            headers=self._headers(action),
                            json={} if method == "POST" else None,
                        )
                        self.assertEqual(
                            response.status_code,
                            404 if method == "GET" else 403,
                            response.text,
                        )

    def test_machine_authorization_projection_and_video_list_page(self) -> None:
        with self._clients("") as (control, provider, accounts, _state):
            _state_value, callback, _callback_target = self._start_and_callback(
                control, provider, ""
            )
            self.assertEqual(callback.status_code, 303)
            self.assertEqual(
                callback.headers["location"],
                "/accounts/douyin-authorization?notice=oauth-completed",
            )
            self.assertEqual(accounts.resolve_calls, [["mock-video-id"]])
            browser_list = control.get(
                "/api/douyin/authorizations", headers=self._headers()
            )
            self.assertEqual(browser_list.status_code, 200, browser_list.text)
            self.assertEqual(len(browser_list.json()["items"]), 1)
            authorization = browser_list.json()["items"][0]
            self.assertEqual(authorization["status"], "active")
            self.assertEqual(authorization["account_id"], 7)
            self.assertEqual(authorization["platform_uid"], PLATFORM_UID)
            authorization_id = authorization["id"]

            listed = control.get(
                "/internal/v1/authorizations",
                headers={"X-Dcar-Machine-Key": MACHINE_KEY},
            )
            self.assertEqual(listed.status_code, 200, listed.text)
            self.assertEqual(
                control.get("/internal/v1/authorizations").status_code, 403
            )
            self.assertEqual(
                control.get(
                    "/internal/v1/authorizations",
                    headers={
                        "X-Dcar-Machine-Key": MACHINE_KEY,
                        "X-Dcar-Edge-Key": EDGE_KEY,
                    },
                ).status_code,
                403,
            )
            self.assertEqual(len(listed.json()["items"]), 1)
            self.assertEqual(
                listed.json()["items"][0]["authorization_id"], authorization_id
            )
            self.assertNotIn("id", listed.json()["items"][0])
            serialized = json.dumps(listed.json(), ensure_ascii=False)
            for forbidden in (
                "mock-open-id",
                "open_id",
                "access_token",
                "refresh_token",
                "nickname",
                "avatar",
                USERNAME,
            ):
                self.assertNotIn(forbidden, serialized)
            self.assertEqual(listed.headers["cache-control"], "no-store")

            manager = AsyncMock()
            manager.video_list_page.return_value = VideoListPage(
                captured_at=NOW,
                cursor=20,
                has_more=True,
                items=[{"item_id": "1234567890123456789", "title": "作品"}],
            )
            cast(FastAPI, control.app).state.token_manager = manager
            page = control.post(
                "/internal/v1/video-list/page",
                headers={
                    "X-Dcar-Machine-Key": MACHINE_KEY,
                    "X-Request-ID": "machine-request-1",
                },
                json={
                    "authorization_id": authorization_id,
                    "cursor": 0,
                    "count": 20,
                },
            )
            self.assertEqual(page.status_code, 200, page.text)
            self.assertEqual(
                page.json(),
                {
                    "captured_at": NOW,
                    "cursor": 20,
                    "has_more": True,
                    "items": [{"item_id": "1234567890123456789", "title": "作品"}],
                },
            )
            self.assertEqual(page.headers["cache-control"], "no-store")
            manager.video_list_page.assert_awaited_once_with(
                authorization_id,
                cursor=0,
                count=20,
                actor="machine:douyin-openapi-sync",
                request_id="machine-request-1",
            )

            unbound = control.post(
                "/api/douyin/authorizations/unbind",
                headers=self._headers(
                    "douyin-authorization-unbind", username="second-operator"
                ),
                json={
                    "authorization_id": authorization_id,
                    "expected_version": authorization["version"],
                },
            )
            self.assertEqual(unbound.status_code, 200, unbound.text)
            listed_after_unbind = control.get(
                "/internal/v1/authorizations",
                headers={"X-Dcar-Machine-Key": MACHINE_KEY},
            )
            self.assertEqual(listed_after_unbind.json(), {"items": []})

    def test_machine_video_list_validation_auth_and_stable_errors(self) -> None:
        authorization_id = "1" * 32
        payload = {
            "authorization_id": authorization_id,
            "cursor": 0,
            "count": 20,
        }
        with self._clients("") as (control, _provider, _accounts, _state):
            unavailable = control.post(
                "/internal/v1/video-list/page",
                headers={"X-Dcar-Machine-Key": MACHINE_KEY},
                json=payload,
            )
            self.assertEqual(unavailable.status_code, 503)
            self.assertEqual(
                unavailable.json(), {"detail": "douyin_provider_unavailable"}
            )
            self.assertEqual(unavailable.headers["cache-control"], "no-store")

            manager = AsyncMock()
            cast(FastAPI, control.app).state.token_manager = manager
            forbidden = control.post("/internal/v1/video-list/page", json=payload)
            self.assertEqual(forbidden.status_code, 403)
            self.assertEqual(forbidden.headers["cache-control"], "no-store")
            mixed_boundary = control.post(
                "/internal/v1/video-list/page",
                headers={
                    "X-Dcar-Machine-Key": MACHINE_KEY,
                    "X-Dcar-Edge-Key": EDGE_KEY,
                },
                json=payload,
            )
            self.assertEqual(mixed_boundary.status_code, 403)

            invalid_payloads = [
                {**payload, "authorization_id": "A" * 32},
                {**payload, "authorization_id": "1" * 31},
                {**payload, "cursor": -1},
                {**payload, "count": 0},
                {**payload, "count": 21},
                {**payload, "unexpected": True},
            ]
            for invalid in invalid_payloads:
                with self.subTest(invalid=invalid):
                    response = control.post(
                        "/internal/v1/video-list/page",
                        headers={"X-Dcar-Machine-Key": MACHINE_KEY},
                        json=invalid,
                    )
                    self.assertEqual(response.status_code, 422, response.text)
                    self.assertEqual(response.headers["cache-control"], "no-store")
            manager.video_list_page.assert_not_awaited()

            cases = [
                (
                    ReauthorizationRequired("refresh_token_expired"),
                    409,
                    "douyin_reauthorization_required",
                ),
                (
                    TokenLifecycleError("authorization_not_found"),
                    404,
                    "douyin_authorization_not_found",
                ),
                (
                    TokenLifecycleError("refresh_busy"),
                    503,
                    "douyin_token_temporarily_unavailable",
                ),
                (
                    TokenLifecycleError("ciphertext_payload_invalid"),
                    502,
                    "douyin_token_error",
                ),
                (
                    DouyinProviderError("video_list", "network_error"),
                    502,
                    "douyin_provider_error",
                ),
            ]
            for error, expected_status, expected_detail in cases:
                with self.subTest(error=error):
                    manager.video_list_page.side_effect = error
                    response = control.post(
                        "/internal/v1/video-list/page",
                        headers={"X-Dcar-Machine-Key": MACHINE_KEY},
                        json=payload,
                    )
                    self.assertEqual(
                        response.status_code, expected_status, response.text
                    )
                    self.assertEqual(response.json()["detail"], expected_detail)
                    self.assertNotIn(str(error), response.text)
                    self.assertEqual(response.headers["cache-control"], "no-store")
                    if isinstance(error, TokenLifecycleError):
                        self.assertEqual(response.json()["reason"], error.reason)
            self.assertEqual(manager.video_list_page.await_count, len(cases))

    def test_account_search_preserves_query_and_returns_only_safe_projection(
        self,
    ) -> None:
        for query in ("", "中文", " ", "%", "_", "甲" * 100):
            with (
                self.subTest(query=query),
                self._clients("") as (
                    control,
                    _provider,
                    accounts,
                    _state,
                ),
            ):
                response = control.post(
                    "/api/douyin/accounts/search",
                    headers=self._headers("douyin-accounts-search"),
                    json={"query": query, "page": 1, "page_size": 50},
                )
                self.assertEqual(response.status_code, 200, response.text)
                self.assertEqual(accounts.calls[-1]["query"], query)
                self.assertEqual(accounts.calls[-1]["platform"], "douyin")
                encoded = json.dumps(response.json(), ensure_ascii=False)
                for forbidden in (
                    "13800138000",
                    "pending-secret",
                    "xhs-secret",
                    "account_type",
                ):
                    self.assertNotIn(forbidden, encoded)
                self.assertEqual(response.json()["items"][0]["uid"], PLATFORM_UID)
        with self._clients("") as (control, _provider, accounts, _state):
            too_long = control.post(
                "/api/douyin/accounts/search",
                headers=self._headers("douyin-accounts-search"),
                json={"query": "甲" * 101, "page": 1, "page_size": 50},
            )
            self.assertEqual(too_long.status_code, 422)
            self.assertEqual(accounts.calls, [])
            url_query = control.post(
                "/api/douyin/accounts/search?query=query-canary",
                headers=self._headers("douyin-accounts-search"),
                json={"query": "", "page": 1, "page_size": 50},
            )
            self.assertEqual(url_query.status_code, 422)
            self.assertEqual(accounts.calls, [])
            extra_field = control.post(
                "/api/douyin/accounts/search",
                headers=self._headers("douyin-accounts-search"),
                json={
                    "query": "",
                    "page": 1,
                    "page_size": 50,
                    "platform": "douyin",
                },
            )
            self.assertEqual(extra_field.status_code, 422)
            self.assertEqual(accounts.calls, [])

    def test_disabled_mode_never_calls_oauth_provider(self) -> None:
        with self._clients("/dcar", enabled=False) as (
            control,
            _provider,
            accounts,
            state,
        ):
            start = control.post(
                "/api/douyin/oauth/start",
                headers=self._headers("douyin-oauth-start"),
                json={},
            )
            self.assertEqual(start.status_code, 409)
            self.assertEqual(accounts.calls, [])
            callback = control.get(
                "/oauth/douyin/callback?code=code-canary&state=" + "s" * 32,
                headers=self._headers(),
                follow_redirects=False,
            )
            self.assertEqual(callback.status_code, 303)
            self.assertNotIn("code-canary", callback.headers["location"])
            self.assertNotIn("s" * 32, callback.headers["location"])
            old_confirm = control.post(
                "/api/douyin/oauth/confirm",
                headers=self._headers("douyin-oauth-confirm"),
                json={},
            )
            self.assertEqual(old_confirm.status_code, 403)
            self.assertEqual(state.token_calls, 0)
            self.assertEqual(state.userinfo_calls, 0)

    def test_oauth_start_requires_fixed_marker_and_empty_object(self) -> None:
        with self._clients("", vault_namespace="start-contract") as (
            control,
            _provider,
            _accounts,
            state,
        ):
            missing_marker = control.post(
                "/api/douyin/oauth/start", headers=self._headers(), json={}
            )
            self.assertEqual(missing_marker.status_code, 403)
            extra = control.post(
                "/api/douyin/oauth/start",
                headers=self._headers("douyin-oauth-start"),
                json={"account_id": 7},
            )
            self.assertEqual(extra.status_code, 422, extra.text)
            started = control.post(
                "/api/douyin/oauth/start",
                headers=self._headers("douyin-oauth-start"),
                json={},
            )
            self.assertEqual(started.status_code, 200, started.text)
            self.assertIn("authorize_url", started.json())
            self.assertEqual(state.token_calls, 0)
            self.assertEqual(state.userinfo_calls, 0)

    def test_oauth_start_returns_conflict_while_callback_is_exchanging(self) -> None:
        with self._clients("", vault_namespace="start-in-progress") as (
            control,
            _provider,
            _accounts,
            state,
        ):
            started = control.post(
                "/api/douyin/oauth/start",
                headers=self._headers("douyin-oauth-start"),
                json={},
            )
            self.assertEqual(started.status_code, 200, started.text)
            raw_state = parse_qs(urlsplit(started.json()["authorize_url"]).query)[
                "state"
            ][0]
            state_digest = hashlib.sha256(raw_state.encode("utf-8")).hexdigest()
            store = cast(FastAPI, control.app).state.store
            store.begin_exchange(
                state_digest,
                USERNAME,
                SESSION_BINDING,
                request_id="callback-in-progress",
                now=NOW,
            )

            blocked = control.post(
                "/api/douyin/oauth/start",
                headers=self._headers("douyin-oauth-start"),
                json={},
            )
            self.assertEqual(blocked.status_code, 409, blocked.text)
            self.assertEqual(blocked.json()["detail"], "已有抖音授权流程正在处理中")
            with store.read_connection() as connection:
                rows = connection.execute(
                    "SELECT state_digest,status FROM oauth_states"
                ).fetchall()
            self.assertEqual(
                [(row["state_digest"], row["status"]) for row in rows],
                [(state_digest, "exchanging")],
            )
            self.assertEqual(state.token_calls, 0)

    def test_mock_oauth_auto_matches_replays_safely_and_unbinds(self) -> None:
        for base_path in ("", "/dcar"):
            with (
                self.subTest(base_path=base_path),
                self._clients(base_path) as (
                    control,
                    provider,
                    accounts,
                    state,
                ),
            ):
                state_value, callback, callback_target = self._start_and_callback(
                    control, provider, base_path
                )
                self.assertEqual(callback.status_code, 303)
                self.assertEqual(
                    callback.headers["location"],
                    f"{base_path}/accounts/douyin-authorization?notice=oauth-completed",
                )
                self.assertNotIn(state_value, callback.headers["location"])
                self.assertNotIn("access-", callback.text)
                self.assertEqual(state.token_calls, 1)
                self.assertEqual(state.userinfo_calls, 1)
                self.assertEqual(accounts.resolve_calls, [["mock-video-id"]])
                listed = control.get(
                    "/api/douyin/authorizations", headers=self._headers()
                )
                self.assertEqual(listed.status_code, 200)
                self.assertEqual(len(listed.json()["items"]), 1)
                item = listed.json()["items"][0]
                self.assertEqual(item["status"], "active")
                self.assertEqual(item["account_id"], 7)
                self.assertEqual(item["platform_uid"], PLATFORM_UID)
                serialized = json.dumps(listed.json())
                self.assertNotIn("access-", serialized)
                self.assertNotIn("refresh-", serialized)
                self.assertNotIn("mock-open-id", serialized)
                statuses = control.get(
                    "/api/douyin/authorization-statuses", headers=self._headers()
                )
                self.assertEqual(statuses.status_code, 200, statuses.text)
                self.assertEqual(
                    statuses.json()["items"][0],
                    {
                        "id": item["id"],
                        "account_id": 7,
                        "platform_uid": PLATFORM_UID,
                        "status": "active",
                        "match_reason": None,
                        "refresh_expires_at": NOW + 30 * 24 * 60 * 60,
                        "needs_reauthorization": False,
                        "updated_at": NOW,
                        "authorized": True,
                        "scopes": ["user_info", "video.list"],
                    },
                )
                self.assertNotIn("access-", json.dumps(statuses.json()))
                self.assertNotIn("refresh-", json.dumps(statuses.json()))
                self.assertNotIn("mock-open-id", json.dumps(statuses.json()))

                replay = control.get(
                    callback_target, headers=self._headers(), follow_redirects=False
                )
                self.assertEqual(replay.status_code, 303)
                self.assertEqual(state.token_calls, 1)

                version_conflict = control.post(
                    "/api/douyin/authorizations/unbind",
                    headers=self._headers("douyin-authorization-unbind"),
                    json={
                        "authorization_id": item["id"],
                        "expected_version": item["version"] + 1,
                    },
                )
                self.assertEqual(version_conflict.status_code, 409)
                unbound = control.post(
                    "/api/douyin/authorizations/unbind",
                    headers=self._headers(
                        "douyin-authorization-unbind", username="replacement-operator"
                    ),
                    json={
                        "authorization_id": item["id"],
                        "expected_version": item["version"],
                    },
                )
                self.assertEqual(unbound.status_code, 200, unbound.text)
                stale = control.post(
                    "/api/douyin/authorizations/unbind",
                    headers=self._headers("douyin-authorization-unbind"),
                    json={
                        "authorization_id": item["id"],
                        "expected_version": item["version"],
                    },
                )
                self.assertEqual(stale.status_code, 404)

                vault_bytes = self._config(base_path).vault_path.read_bytes()
                self.assertNotIn(state_value.encode("utf-8"), vault_bytes)
                self.assertNotIn(b"access-", vault_bytes)
                self.assertNotIn(b"refresh-", vault_bytes)

    def test_auto_match_pending_reasons_and_manual_match(self) -> None:
        reason_by_outcome = {
            "unmatched": "no_match",
            "ambiguous": "ambiguous_match",
            "unavailable": "auto_match_unavailable",
        }
        for outcome, expected_reason in reason_by_outcome.items():
            with (
                self.subTest(outcome=outcome),
                self._clients(
                    "",
                    resolver_outcome=outcome,
                    vault_namespace=outcome,
                ) as (
                    control,
                    provider,
                    accounts,
                    _state,
                ),
            ):
                _state_value, callback, _callback_target = self._start_and_callback(
                    control, provider, ""
                )
                self.assertEqual(callback.status_code, 303, callback.text)
                self.assertEqual(
                    callback.headers["location"],
                    "/accounts/douyin-authorization?notice=oauth-completed",
                )
                listed = control.get(
                    "/api/douyin/authorizations", headers=self._headers()
                )
                self.assertEqual(listed.status_code, 200, listed.text)
                self.assertEqual(len(listed.json()["items"]), 1)
                pending = listed.json()["items"][0]
                self.assertEqual(pending["status"], "pending_match")
                self.assertEqual(pending["match_reason"], expected_reason)
                self.assertIsNone(pending["account_id"])
                self.assertIsNone(pending["platform_uid"])
                self.assertEqual(accounts.resolve_calls, [["mock-video-id"]])

                statuses = control.get(
                    "/api/douyin/authorization-statuses", headers=self._headers()
                )
                self.assertEqual(statuses.status_code, 200, statuses.text)
                self.assertFalse(statuses.json()["items"][0]["authorized"])
                self.assertNotIn("access-", json.dumps(statuses.json()))
                self.assertNotIn("refresh-", json.dumps(statuses.json()))

                stale = control.post(
                    "/api/douyin/authorizations/match",
                    headers=self._headers("douyin-authorization-match"),
                    json={
                        "authorization_id": pending["id"],
                        "account_id": 7,
                        "platform_uid": PLATFORM_UID,
                        "expected_version": pending["version"] + 1,
                    },
                )
                self.assertEqual(stale.status_code, 409, stale.text)

                matched = control.post(
                    "/api/douyin/authorizations/match",
                    headers=self._headers(
                        "douyin-authorization-match", username="manual-operator"
                    ),
                    json={
                        "authorization_id": pending["id"],
                        "account_id": 7,
                        "platform_uid": PLATFORM_UID,
                        "expected_version": pending["version"],
                    },
                )
                self.assertEqual(matched.status_code, 200, matched.text)
                self.assertEqual(matched.json()["authorization"]["status"], "active")
                self.assertEqual(matched.json()["authorization"]["account_id"], 7)
                self.assertEqual(
                    matched.json()["authorization"]["platform_uid"], PLATFORM_UID
                )
                self.assertGreaterEqual(len(accounts.calls), 2)

    def test_reauthorization_requires_current_version_and_same_open_id(self) -> None:
        provider_state = MockDouyinState(CLIENT_KEY, CLIENT_SECRET)
        with self._clients("", provider_state=provider_state) as (
            control,
            provider,
            accounts,
            _state,
        ):
            _state_value, callback, _callback_target = self._start_and_callback(
                control, provider, ""
            )
            self.assertEqual(callback.status_code, 303)
            item = control.get(
                "/api/douyin/authorizations", headers=self._headers()
            ).json()["items"][0]

            stale = control.post(
                "/api/douyin/authorizations/reauthorize",
                headers=self._headers("douyin-authorization-reauthorize"),
                json={
                    "authorization_id": item["id"],
                    "expected_version": item["version"] + 1,
                },
            )
            self.assertEqual(stale.status_code, 409, stale.text)

            reauthorize = control.post(
                "/api/douyin/authorizations/reauthorize",
                headers=self._headers("douyin-authorization-reauthorize"),
                json={
                    "authorization_id": item["id"],
                    "expected_version": item["version"],
                },
            )
            self.assertEqual(reauthorize.status_code, 200, reauthorize.text)
            authorized = provider.get(reauthorize.json()["authorize_url"])
            callback_target = self._upstream_target(authorized.headers["location"], "")
            callback = control.get(
                callback_target, headers=self._headers(), follow_redirects=False
            )
            self.assertEqual(callback.status_code, 303)
            refreshed = control.get(
                "/api/douyin/authorizations", headers=self._headers()
            ).json()["items"][0]
            self.assertEqual(refreshed["id"], item["id"])
            self.assertEqual(refreshed["status"], "active")
            self.assertEqual(refreshed["version"], item["version"] + 1)
            self.assertEqual(accounts.resolve_calls, [["mock-video-id"]])

            reauthorize_again = control.post(
                "/api/douyin/authorizations/reauthorize",
                headers=self._headers("douyin-authorization-reauthorize"),
                json={
                    "authorization_id": refreshed["id"],
                    "expected_version": refreshed["version"],
                },
            )
            self.assertEqual(reauthorize_again.status_code, 200)
            provider_state.open_id = "mock-open-id-wrong-account"
            wrong_authorized = provider.get(reauthorize_again.json()["authorize_url"])
            wrong_target = self._upstream_target(
                wrong_authorized.headers["location"], ""
            )
            wrong = control.get(
                wrong_target, headers=self._headers(), follow_redirects=False
            )
            self.assertEqual(wrong.status_code, 303)
            self.assertEqual(
                wrong.headers["location"],
                "/accounts/douyin-authorization?notice=oauth-conflict",
            )
            preserved = control.get(
                "/api/douyin/authorizations", headers=self._headers()
            ).json()["items"]
            self.assertEqual(len(preserved), 1)
            self.assertEqual(preserved[0]["id"], item["id"])
            self.assertEqual(preserved[0]["status"], "active")

    def test_wrong_session_does_not_consume_state_and_userinfo_can_degrade(
        self,
    ) -> None:
        provider_state = MockDouyinState(CLIENT_KEY, CLIENT_SECRET)
        provider_state.userinfo_failure = True
        with self._clients("", provider_state=provider_state) as (
            control,
            provider,
            _accounts,
            state,
        ):
            start = control.post(
                "/api/douyin/oauth/start",
                headers=self._headers("douyin-oauth-start"),
                json={},
            )
            authorize_url = start.json()["authorize_url"]
            authorized = provider.get(authorize_url)
            callback_target = self._upstream_target(authorized.headers["location"], "")
            wrong = control.get(
                callback_target,
                headers=self._headers(binding="b" * 64),
                follow_redirects=False,
            )
            self.assertEqual(wrong.status_code, 303)
            self.assertEqual(state.token_calls, 0)
            correct = control.get(
                callback_target, headers=self._headers(), follow_redirects=False
            )
            self.assertEqual(correct.status_code, 303)
            self.assertEqual(state.token_calls, 1)
            self.assertEqual(state.userinfo_calls, 1)
            self.assertEqual(
                correct.headers["location"],
                "/accounts/douyin-authorization?notice=oauth-completed",
            )
            listed = control.get("/api/douyin/authorizations", headers=self._headers())
            self.assertEqual(listed.status_code, 200, listed.text)
            self.assertEqual(listed.json()["items"][0]["status"], "active")

    def test_control_package_never_imports_v8_or_exposes_mock_route(self) -> None:
        package_root = REPOSITORY_ROOT / "src" / "dcar_eval" / "dcar_douyin_control"
        for source_path in package_root.glob("*.py"):
            source = source_path.read_text(encoding="utf-8")
            tree = ast.parse(source)
            for node in ast.walk(tree):
                if isinstance(node, ast.Import):
                    names = [alias.name for alias in node.names]
                elif isinstance(node, ast.ImportFrom):
                    names = [node.module or ""]
                else:
                    continue
                self.assertFalse(
                    any(name.startswith("dcar_eval.v8") for name in names),
                    f"{source_path} imports the formal data plane",
                )
            self.assertNotIn("/douyin/mock/", source)


if __name__ == "__main__":
    unittest.main()
