from __future__ import annotations

import ast
import base64
import json
import os
import sys
import tempfile
import unittest
from contextlib import contextmanager
from pathlib import Path
from typing import Iterator
from unittest.mock import AsyncMock, patch
from urllib.parse import parse_qs, urlsplit

from cryptography.fernet import Fernet
from fastapi import FastAPI, Request
from fastapi.responses import JSONResponse
from fastapi.testclient import TestClient
from httpx import ASGITransport


REPOSITORY_ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(REPOSITORY_ROOT / "src" / "dcar_eval"))

from dcar_douyin_control.app import create_app  # noqa: E402
from dcar_douyin_control.config import DouyinControlConfig  # noqa: E402
from dcar_douyin_control.provider import DouyinOAuthClient  # noqa: E402
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
    def __init__(self) -> None:
        self.calls: list[dict[str, object]] = []
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
            app = create_app(config, api_transport=ASGITransport(app=AccountUpstream().app))
            with TestClient(app) as client:
                response = client.get(
                    "/internal/v1/health",
                    headers={"X-Dcar-Machine-Key": MACHINE_KEY},
                )
                self.assertEqual(response.status_code, 200)
                self.assertIsNotNone(app.state.token_manager)
        close.assert_awaited_once()

    def _config(self, base_path: str, *, enabled: bool = True) -> DouyinControlConfig:
        suffix = "root" if not base_path else "dcar"
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
                f"https://dcar.test{base_path}/oauth/douyin/callback"
                if enabled
                else ""
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
    ) -> Iterator[tuple[TestClient, TestClient, AccountUpstream, MockDouyinState]]:
        accounts = AccountUpstream()
        state = provider_state or MockDouyinState(CLIENT_KEY, CLIENT_SECRET)
        mock_app = create_mock_douyin_oauth(state)
        control_app = create_app(
            self._config(base_path, enabled=enabled),
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
    ) -> tuple[str, object, str]:
        start = control.post(
            "/api/douyin/oauth/start",
            headers=self._headers("douyin-oauth-start"),
            json={"account_id": 7, "platform_uid": PLATFORM_UID},
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
            with self.subTest(base_path=base_path), self._clients(base_path) as (
                control,
                _provider,
                _accounts,
                _state,
            ):
                self.assertEqual(control.get("/douyin").status_code, 403)
                self.assertEqual(
                    control.get(
                        "/douyin", headers=self._headers(username="temporary-bypass")
                    ).status_code,
                    403,
                )
                page = control.get("/douyin", headers=self._headers())
                self.assertEqual(page.status_code, 200)
                self.assertIn("抖音账号授权", page.text)
                self.assertIn("script-src 'nonce-", page.headers["content-security-policy"])
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

    def test_account_search_preserves_query_and_returns_only_safe_projection(self) -> None:
        for query in ("", "中文", " ", "%", "_", "甲" * 100):
            with self.subTest(query=query), self._clients("") as (
                control,
                _provider,
                accounts,
                _state,
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
                json={"account_id": 7, "platform_uid": PLATFORM_UID},
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
            confirm = control.post(
                "/api/douyin/oauth/confirm",
                headers=self._headers("douyin-oauth-confirm"),
                json={},
            )
            self.assertEqual(confirm.status_code, 409)
            self.assertEqual(state.token_calls, 0)
            self.assertEqual(state.userinfo_calls, 0)

    def test_mock_oauth_end_to_end_replay_and_unbind(self) -> None:
        for base_path in ("", "/dcar"):
            with self.subTest(base_path=base_path), self._clients(base_path) as (
                control,
                provider,
                accounts,
                state,
            ):
                state_value, callback, callback_target = self._start_and_callback(
                    control, provider, base_path
                )
                self.assertEqual(callback.status_code, 303)
                self.assertEqual(
                    callback.headers["location"], f"{base_path}/douyin/confirm"
                )
                self.assertNotIn(state_value, callback.headers["location"])
                self.assertNotIn("access-", callback.text)
                self.assertEqual(state.token_calls, 1)
                self.assertEqual(state.userinfo_calls, 1)
                self.assertGreaterEqual(
                    sum(call["query"] == PLATFORM_UID for call in accounts.calls), 2
                )

                confirm_page = control.get(
                    "/douyin/confirm", headers=self._headers(), follow_redirects=False
                )
                self.assertEqual(confirm_page.status_code, 200)
                self.assertIn("Mock 抖音账号", confirm_page.text)
                self.assertIn('class="avatar"', confirm_page.text)
                self.assertNotIn("https://avatar.example/mock.png", confirm_page.text)
                self.assertIn(
                    "img-src 'self' data:",
                    confirm_page.headers["content-security-policy"],
                )
                self.assertNotIn(
                    "img-src 'self' data: https:",
                    confirm_page.headers["content-security-policy"],
                )
                self.assertNotIn("access-", confirm_page.text)
                self.assertNotIn("refresh-", confirm_page.text)

                confirmed = control.post(
                    "/api/douyin/oauth/confirm",
                    headers=self._headers("douyin-oauth-confirm"),
                    json={},
                )
                self.assertEqual(confirmed.status_code, 200, confirmed.text)
                item = confirmed.json()["authorization"]
                listed = control.get(
                    "/api/douyin/authorizations", headers=self._headers()
                )
                self.assertEqual(listed.status_code, 200)
                serialized = json.dumps(listed.json())
                self.assertNotIn("access-", serialized)
                self.assertNotIn("refresh-", serialized)
                self.assertNotIn("mock-open-id", serialized)

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
                    headers=self._headers("douyin-authorization-unbind"),
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

    def test_wrong_session_does_not_consume_state_and_userinfo_can_degrade(self) -> None:
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
                json={"account_id": 7, "platform_uid": PLATFORM_UID},
            )
            authorize_url = start.json()["authorize_url"]
            authorized = provider.get(authorize_url)
            callback_target = self._upstream_target(
                authorized.headers["location"], ""
            )
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
            page = control.get("/douyin/confirm", headers=self._headers())
            self.assertEqual(page.status_code, 200)
            self.assertIn("未获取昵称", page.text)

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
