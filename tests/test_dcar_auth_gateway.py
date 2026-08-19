from __future__ import annotations

import gzip
import hashlib
import sqlite3
import sys
import tempfile
import unittest
from contextlib import contextmanager
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


ORIGIN = "https://dcar.test"
USERNAME = "operator"
PASSWORD = "correct-password"
COMPRESSED_BODY = b"compressed proxy response\n" * 128
REAL_VALIDATE_SOURCE = auth_gateway.HtpasswdVerifier.validate_source
REAL_VERIFY = auth_gateway.HtpasswdVerifier.verify
REAL_CREDENTIAL_IS_CURRENT = auth_gateway.HtpasswdVerifier.credential_is_current


def _route(base_path: str, path: str) -> str:
    return f"{base_path}{path}"


def _echo_upstream(name: str) -> FastAPI:
    app = FastAPI()

    @app.api_route(
        "/{path:path}",
        methods=["GET", "HEAD", "POST", "PUT", "PATCH", "DELETE", "OPTIONS"],
    )
    async def echo(request: Request, path: str) -> JSONResponse:
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
            }
        )

    return app


class DcarAuthGatewayTestCase(unittest.TestCase):
    def setUp(self) -> None:
        self.temporary = tempfile.TemporaryDirectory()
        self.addCleanup(self.temporary.cleanup)
        self.root = Path(self.temporary.name)
        self.login_template = self.root / "login.html"
        self.login_template.write_text(
            "<!doctype html><title>Dcar login</title>", encoding="utf-8"
        )
        self.htpasswd = self.root / "users.htpasswd"
        # Credential-format compatibility is tested independently from this HTTP
        # contract. These patches keep the gateway-flow suite deterministic on
        # every supported development host.
        self.htpasswd.write_text("operator:test-fixture\n", encoding="utf-8")
        validate_patcher = patch.object(
            auth_gateway.HtpasswdVerifier,
            "validate_source",
            autospec=True,
            return_value=None,
        )
        verify_patcher = patch.object(
            auth_gateway.HtpasswdVerifier,
            "verify",
            autospec=True,
            side_effect=lambda _verifier, username, password: (
                (True, "test-credential-fingerprint")
                if username == USERNAME and password == PASSWORD
                else (False, "")
            ),
        )
        current_patcher = patch.object(
            auth_gateway.HtpasswdVerifier,
            "credential_is_current",
            autospec=True,
            return_value=True,
        )
        validate_patcher.start()
        verify_patcher.start()
        current_patcher.start()
        self.addCleanup(validate_patcher.stop)
        self.addCleanup(verify_patcher.stop)
        self.addCleanup(current_patcher.stop)

    def _config(self, base_path: str) -> auth_gateway.AuthGatewayConfig:
        suffix = "root" if not base_path else base_path.strip("/").replace("/", "-")
        return auth_gateway.AuthGatewayConfig(
            base_path=base_path,
            web_upstream="http://web.test",
            api_upstream="http://api.test",
            htpasswd_path=self.htpasswd,
            session_db_path=self.root / f"sessions-{suffix}.sqlite3",
            login_template_path=self.login_template,
            secure_cookie=True,
            session_seconds=3600,
            remember_session_seconds=86400,
        )

    @contextmanager
    def _client(
        self, base_path: str
    ) -> Iterator[tuple[TestClient, auth_gateway.AuthGatewayConfig]]:
        config = self._config(base_path)
        app = auth_gateway.create_app(
            config,
            web_transport=ASGITransport(app=_echo_upstream("web")),
            api_transport=ASGITransport(app=_echo_upstream("api")),
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
                    {"authenticated": True, "username": USERNAME},
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
            auth_gateway.HtpasswdVerifier,
            "verify",
            side_effect=OSError("unavailable"),
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
            {"detail": "系统暂时无法加载数据，请稍后重试"},
        )
        self.assertEqual(unavailable.headers.get("cache-control"), "no-store")

    def test_health_checks_account_source_and_session_store(self) -> None:
        with self._client("") as (client, config):
            healthy = client.get("/auth/health")
            self.assertEqual(healthy.status_code, 200)
            self.assertEqual(healthy.json(), {"status": "ok"})
            with patch.object(
                auth_gateway.SessionStore,
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
                    _route(base_path, "/api/v8/accounts/search?limit=5"),
                    content='{"query":"demo"}',
                    headers={
                        **forged_headers,
                        "Content-Type": "application/json",
                    },
                )
                self.assertEqual(api.status_code, 200)
                self.assertEqual(api.json()["upstream"], "api")
                self.assertEqual(api.json()["path"], "/api/v8/accounts/search")
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

    def test_sha512_htpasswd_verification_and_credential_rotation(self) -> None:
        stored = sha512_crypt.using(salt="testauthsalt", rounds=5000).hash(PASSWORD)
        self.htpasswd.write_text(f"{USERNAME}:{stored}\n", encoding="utf-8")
        verifier = auth_gateway.HtpasswdVerifier(self.htpasswd)
        REAL_VALIDATE_SOURCE(verifier)
        valid, fingerprint = REAL_VERIFY(verifier, USERNAME, PASSWORD)
        self.assertTrue(valid)
        self.assertTrue(fingerprint)
        self.assertFalse(REAL_VERIFY(verifier, USERNAME, "wrong")[0])
        self.assertFalse(REAL_VERIFY(verifier, "missing", PASSWORD)[0])
        self.assertTrue(REAL_CREDENTIAL_IS_CURRENT(verifier, USERNAME, fingerprint))

        rotated = sha512_crypt.using(
            salt="rotatedauthsalt", rounds=5000
        ).hash("new-password")
        self.htpasswd.write_text(f"{USERNAME}:{rotated}\n", encoding="utf-8")
        self.assertFalse(REAL_CREDENTIAL_IS_CURRENT(verifier, USERNAME, fingerprint))

    def test_unsupported_htpasswd_hash_is_rejected(self) -> None:
        self.htpasswd.write_text(
            f"{USERNAME}:$apr1$legacy$hash\n", encoding="utf-8"
        )
        verifier = auth_gateway.HtpasswdVerifier(self.htpasswd)
        with self.assertRaisesRegex(RuntimeError, "SHA-512 crypt"):
            REAL_VALIDATE_SOURCE(verifier)


if __name__ == "__main__":
    unittest.main()
