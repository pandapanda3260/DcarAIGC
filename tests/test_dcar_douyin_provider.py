from __future__ import annotations

import json
import sys
import tempfile
import unittest
from pathlib import Path
from unittest.mock import patch
from urllib.parse import parse_qs, urlsplit

import httpx


REPOSITORY_ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(REPOSITORY_ROOT / "src" / "dcar_eval"))

from dcar_douyin_control.config import DouyinControlConfig  # noqa: E402
from dcar_douyin_control.provider import (  # noqa: E402
    DOUYIN_AUTHORIZE_URL,
    DOUYIN_REFRESH_URL,
    DOUYIN_RENEW_REFRESH_URL,
    DOUYIN_TOKEN_URL,
    DOUYIN_USERINFO_URL,
    DOUYIN_VIDEO_LIST_URL,
    DouyinOAuthClient,
    DouyinProviderError,
    TokenBundle,
)


NOW = 1_900_000_000
CLIENT_KEY = "production-client-key"
CLIENT_SECRET = "production-client-secret-at-least-32-bytes"
OPEN_ID = "open-id-for-provider-tests"
ACCESS_TOKEN = "access-token-for-provider-tests"
REFRESH_TOKEN = "refresh-token-for-provider-tests"


class DouyinProviderTestCase(unittest.IsolatedAsyncioTestCase):
    def setUp(self) -> None:
        self.temporary = tempfile.TemporaryDirectory()
        self.addCleanup(self.temporary.cleanup)
        self.root = Path(self.temporary.name)
        self.secret_path = self.root / "client-secret"
        self.secret_path.write_text(CLIENT_SECRET, encoding="utf-8")

    def _config(self, **overrides: object) -> DouyinControlConfig:
        values: dict[str, object] = {
            "public_base_path": "/dcar",
            "vault_path": self.root / "vault.sqlite3",
            "edge_key_path": self.root / "edge-key",
            "machine_key_path": self.root / "machine-key",
            "fernet_keyring_path": self.root / "fernet-keyring",
            "open_id_hmac_key_path": self.root / "open-id-hmac",
            "authorization_enabled": True,
            "provider_mode": "douyin",
            "client_key": CLIENT_KEY,
            "client_secret_path": self.secret_path,
            "provider_proxy_url": "http://127.0.0.1:4176",
            "callback_url": "https://dcar.test/dcar/oauth/douyin/callback",
        }
        values.update(overrides)
        return DouyinControlConfig(**values)  # type: ignore[arg-type]

    @staticmethod
    def _token_payload(**overrides: object) -> dict[str, object]:
        data: dict[str, object] = {
            "error_code": "0",
            "open_id": OPEN_ID,
            "access_token": ACCESS_TOKEN,
            "refresh_token": REFRESH_TOKEN,
            "expires_in": 15 * 24 * 60 * 60,
            "refresh_expires_in": 30 * 24 * 60 * 60,
            "scope": "user_info,video.list",
        }
        data.update(overrides)
        return {"data": data, "message": "success"}

    def test_production_config_requires_fixed_endpoints_https_and_proxy(self) -> None:
        config = self._config()
        self.assertEqual(config.provider_mode, "douyin")
        self.assertEqual(config.provider_proxy_url, "http://127.0.0.1:4176")
        for overrides in (
            {"provider_proxy_url": ""},
            {"provider_proxy_url": "http://proxy.example:4176"},
            {"provider_proxy_url": "http://127.0.0.1:4176/path"},
            {"callback_url": "http://dcar.test/dcar/oauth/douyin/callback"},
            {"provider_token_url": "https://evil.example/oauth/access_token/"},
        ):
            with self.subTest(overrides=overrides), self.assertRaises(ValueError):
                self._config(**overrides)

    def test_owned_client_has_explicit_proxy_and_hardened_http_settings(self) -> None:
        with patch("dcar_douyin_control.provider.httpx.AsyncClient") as async_client:
            DouyinOAuthClient(self._config())
        async_client.assert_called_once()
        kwargs = async_client.call_args.kwargs
        self.assertEqual(kwargs["proxy"], "http://127.0.0.1:4176")
        self.assertFalse(kwargs["trust_env"])
        self.assertFalse(kwargs["follow_redirects"])
        self.assertEqual(kwargs["timeout"].connect, 2)
        self.assertEqual(kwargs["timeout"].read, 10)

    async def test_authorization_and_token_exchange_use_fixed_endpoints(self) -> None:
        requests: list[httpx.Request] = []

        def handler(request: httpx.Request) -> httpx.Response:
            requests.append(request)
            self.assertEqual(str(request.url), DOUYIN_TOKEN_URL)
            self.assertEqual(request.method, "POST")
            form = parse_qs(request.content.decode("utf-8"))
            self.assertEqual(form["client_key"], [CLIENT_KEY])
            self.assertEqual(form["client_secret"], [CLIENT_SECRET])
            self.assertEqual(form["code"], ["single-use-code"])
            self.assertEqual(form["grant_type"], ["authorization_code"])
            return httpx.Response(200, json=self._token_payload())

        client = httpx.AsyncClient(transport=httpx.MockTransport(handler))
        self.addAsyncCleanup(client.aclose)
        provider = DouyinOAuthClient(self._config(), client, clock=lambda: NOW)
        state = "s" * 32
        authorize_url = provider.authorization_url(state, ["user_info", "video.list"])
        parsed = urlsplit(authorize_url)
        self.assertEqual(
            f"{parsed.scheme}://{parsed.netloc}{parsed.path}", DOUYIN_AUTHORIZE_URL
        )
        query = parse_qs(parsed.query)
        self.assertEqual(query["scope"], ["user_info,video.list"])
        self.assertEqual(query["state"], [state])
        self.assertEqual(
            query["redirect_uri"],
            ["https://dcar.test/dcar/oauth/douyin/callback"],
        )
        with self.assertRaises(ValueError):
            provider.authorization_url(
                state, ["user_info", "video.list", "renew_refresh_token"]
            )

        bundle = await provider.exchange_code("single-use-code")
        self.assertEqual(bundle.open_id, OPEN_ID)
        self.assertEqual(bundle.access_expires_at, NOW + 15 * 24 * 60 * 60)
        self.assertEqual(bundle.refresh_expires_at, NOW + 30 * 24 * 60 * 60)
        self.assertEqual(bundle.scopes, ["user_info", "video.list"])
        self.assertEqual(len(requests), 1)

    async def test_userinfo_refresh_and_renew_follow_official_contracts(self) -> None:
        requests: list[httpx.Request] = []

        def handler(request: httpx.Request) -> httpx.Response:
            requests.append(request)
            if str(request.url) == DOUYIN_USERINFO_URL:
                self.assertEqual(request.method, "POST")
                self.assertEqual(request.headers["access-token"], ACCESS_TOKEN)
                self.assertEqual(json.loads(request.content), {})
                return httpx.Response(
                    200,
                    json={
                        "data": {
                            "error_code": 0,
                            "open_id": OPEN_ID,
                            "nickname": "正式账号",
                            "avatar": "https://p3-dy.example/avatar.jpeg",
                        }
                    },
                )
            if str(request.url) == DOUYIN_REFRESH_URL:
                self.assertIn("multipart/form-data", request.headers["content-type"])
                self.assertIn(REFRESH_TOKEN.encode(), request.content)
                return httpx.Response(
                    200,
                    json=self._token_payload(access_token="refreshed-access-token"),
                )
            if str(request.url) == DOUYIN_RENEW_REFRESH_URL:
                self.assertIn("multipart/form-data", request.headers["content-type"])
                self.assertIn(REFRESH_TOKEN.encode(), request.content)
                return httpx.Response(
                    200,
                    json={
                        "data": {
                            "error_code": 0,
                            "refresh_token": "renewed-refresh-token",
                            "expires_in": 30 * 24 * 60 * 60,
                        }
                    },
                )
            raise AssertionError(f"unexpected request: {request.method} {request.url}")

        client = httpx.AsyncClient(transport=httpx.MockTransport(handler))
        self.addAsyncCleanup(client.aclose)
        provider = DouyinOAuthClient(self._config(), client, clock=lambda: NOW)
        original = TokenBundle(
            open_id=OPEN_ID,
            access_token=ACCESS_TOKEN,
            refresh_token=REFRESH_TOKEN,
            access_expires_at=NOW + 1,
            refresh_expires_at=NOW + 2,
            scopes=["user_info", "video.list"],
        )
        profile = await provider.userinfo(original)
        self.assertEqual(
            profile,
            {"nickname": "正式账号", "avatar": "https://p3-dy.example/avatar.jpeg"},
        )
        refreshed = await provider.refresh_access_token(
            REFRESH_TOKEN, expected_open_id=OPEN_ID
        )
        self.assertEqual(refreshed.access_token, "refreshed-access-token")
        renewed = await provider.renew_refresh_token(REFRESH_TOKEN)
        self.assertEqual(renewed.refresh_token, "renewed-refresh-token")
        self.assertEqual(renewed.refresh_expires_at, NOW + 30 * 24 * 60 * 60)
        self.assertEqual(len(requests), 3)

    async def test_video_list_whitelists_fields_and_never_returns_credentials(
        self,
    ) -> None:
        def handler(request: httpx.Request) -> httpx.Response:
            self.assertEqual(request.method, "GET")
            self.assertEqual(request.url.path, urlsplit(DOUYIN_VIDEO_LIST_URL).path)
            self.assertEqual(request.headers["access-token"], ACCESS_TOKEN)
            self.assertEqual(request.url.params["open_id"], OPEN_ID)
            self.assertEqual(request.url.params["cursor"], "20")
            self.assertEqual(request.url.params["count"], "20")
            return httpx.Response(
                200,
                json={
                    "extra": {
                        "error_code": 0,
                        "sub_error_code": 0,
                        "description": "",
                        "sub_description": "",
                        "logid": "safe-log-id",
                        "now": NOW * 1000,
                    },
                    "data": {
                        "error_code": 0,
                        "description": "",
                        "cursor": 40,
                        "has_more": False,
                        "list": [
                            {
                                "video_id": "70000000001",
                                "title": "测试视频",
                                "create_time": 1_700_000_000,
                                "is_top": False,
                                "is_reviewed": True,
                                "video_status": 5,
                                "share_url": "https://www.douyin.com/video/70000000001",
                                "item_id": "opaque/item+id==",
                                "media_type": 2,
                                "cover": "https://p3-dy.example/cover.jpeg",
                                "statistics": {
                                    "forward_count": 10,
                                    "comment_count": 100,
                                    "digg_count": 200,
                                    "download_count": 10,
                                    "play_count": 0,
                                    "share_count": 10,
                                },
                                "access_token": "must-not-cross-boundary",
                                "unknown_provider_field": "must-not-cross-boundary",
                            }
                        ],
                    },
                },
            )

        client = httpx.AsyncClient(transport=httpx.MockTransport(handler))
        self.addAsyncCleanup(client.aclose)
        provider = DouyinOAuthClient(self._config(), client, clock=lambda: NOW)
        page = await provider.video_list_page(
            access_token=ACCESS_TOKEN,
            open_id=OPEN_ID,
            cursor=20,
        )
        self.assertEqual(page.cursor, 40)
        self.assertFalse(page.has_more)
        self.assertEqual(page.items[0]["statistics"]["play_count"], 0)
        serialized = json.dumps(page.as_dict())
        for secret in (
            ACCESS_TOKEN,
            OPEN_ID,
            "must-not-cross-boundary",
            CLIENT_SECRET,
        ):
            self.assertNotIn(secret, serialized)

    async def test_strict_envelope_rejects_provider_error_and_bad_fields(self) -> None:
        responses = iter(
            (
                httpx.Response(
                    400,
                    json={
                        "data": {"error_code": 2100005},
                        "extra": {
                            "error_code": 2100005,
                            "sub_error_code": 0,
                        },
                    },
                ),
                httpx.Response(
                    200,
                    json={
                        "data": {
                            "error_code": 0,
                            "cursor": 0,
                            "has_more": "false",
                            "list": [],
                        },
                        "extra": {"error_code": 0, "sub_error_code": 0},
                    },
                ),
            )
        )
        client = httpx.AsyncClient(
            transport=httpx.MockTransport(lambda _request: next(responses))
        )
        self.addAsyncCleanup(client.aclose)
        provider = DouyinOAuthClient(self._config(), client, clock=lambda: NOW)
        with self.assertRaises(DouyinProviderError) as rejected:
            await provider.video_list_page(
                access_token=ACCESS_TOKEN, open_id=OPEN_ID, cursor=0
            )
        self.assertEqual(rejected.exception.category, "provider_rejected")
        self.assertEqual(rejected.exception.provider_error_code, 2100005)
        self.assertNotIn(ACCESS_TOKEN, str(rejected.exception))
        with self.assertRaises(DouyinProviderError) as malformed:
            await provider.video_list_page(
                access_token=ACCESS_TOKEN, open_id=OPEN_ID, cursor=0
            )
        self.assertEqual(malformed.exception.category, "malformed_response")


if __name__ == "__main__":
    unittest.main()
