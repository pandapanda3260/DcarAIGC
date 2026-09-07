"""Offline public-profile contract and pinned transport fixtures; no DB/network."""

from __future__ import annotations

import json
import socket
import ssl
import time
import types
import unittest
from unittest.mock import MagicMock, patch
from urllib.parse import parse_qs, urlsplit

from v8 import account_profile_public as public
from v8.account_profile_input import ParsedProfile, ProfileInputError, parse_profile_input


UID = "1234567890123456"
SEC = "MS4wLjAB" + "a" * 48
XHS = "64aabbccddeeff0011223344"
OTHER_XHS = "64aabbccddeeff0011223355"
DY_URL = "https://www.douyin.com/user/" + UID
XHS_URL = "https://www.xiaohongshu.com/user/profile/" + XHS
PUBLIC_ADDRESS = (socket.AF_INET, socket.SOCK_STREAM, 6, "", ("93.184.216.34", 443))


def response(body: bytes = b"", status: int = 200, **headers: str) -> public._Response:
    return public._Response(status, tuple(headers.items()), body)


def douyin_body(**changes: object) -> bytes:
    user = {"uid": UID, "sec_uid": SEC, "nickname": "测试账号", "unique_id": "car123"}
    user.update(changes)
    return json.dumps({"status_code": 0, "user": user}, ensure_ascii=False).encode()


def xhs_body(*, uid: object = XHS, nickname: object = "测试账号", extra: str = "") -> bytes:
    state = {"user": {"userPageData": {"basicInfo": {"userId": uid, "nickname": nickname, "redId": "94323641835"}}}}
    return ("<html><script>window.__INITIAL_STATE__=" + json.dumps(state, ensure_ascii=False) + ";</script>" + extra + "</html>").encode()


def protocol() -> types.SimpleNamespace:
    signer = MagicMock()
    signer.get_value.return_value = "fixture-signature"
    return types.SimpleNamespace(
        TTWID_ENDPOINT="https://ttwid.bytedance.com/ttwid/union/register/",
        TTWID_PAYLOAD={"region": "cn", "aid": 1768},
        PROFILE_ENDPOINT="https://www.douyin.com/aweme/v1/web/user/profile/other/",
        USER_AGENT="FixtureBrowser/1.0", BASE_PARAMS={"aid": "6383"},
        ABogus=MagicMock(return_value=signer),
    )


class PublicProfileTest(unittest.TestCase):
    def setUp(self) -> None:
        # Any accidentally unmocked network path fails immediately, including
        # guest setup, DNS resolution and a second connection during TLS setup.
        self.patches = [patch.object(socket, "getaddrinfo", side_effect=AssertionError("fixture must not resolve DNS")),
                        patch.object(socket, "socket", side_effect=AssertionError("fixture must not open a socket"))]
        for item in self.patches:
            item.start()
            self.addCleanup(item.stop)

    def douyin_lookup(self, profile: str = DY_URL, body: bytes | None = None) -> tuple[dict, MagicMock, types.SimpleNamespace]:
        fixture_protocol = protocol()
        with patch.object(public, "_douyin_protocol", return_value=fixture_protocol), patch.object(
            public, "_request_once", side_effect=[response(**{"Set-Cookie": "ttwid=fixture|token; Path=/; Secure"}),
                                                 response(douyin_body() if body is None else body)]
        ) as request:
            result = public.public_profile_lookup(parse_profile_input(profile))
        return dict(result), request, fixture_protocol

    def test_numeric_douyin_uid_verified_and_only_public_profile_requested(self) -> None:
        result, request, fixture_protocol = self.douyin_lookup()
        self.assertEqual(result, {"platform": "douyin", "uid": UID, "sec_user_id": SEC,
                                  "nickname": "测试账号", "display_account_id": "car123"})
        self.assertEqual(request.call_count, 2)
        self.assertEqual(request.call_args_list[0].kwargs["method"], "POST")
        profile_request = request.call_args_list[1]
        self.assertEqual(urlsplit(profile_request.args[0]).path, "/aweme/v1/web/user/profile/other/")
        self.assertEqual(parse_qs(urlsplit(profile_request.args[0]).query)["user_id"], [UID])
        self.assertEqual(profile_request.kwargs["headers"]["Cookie"], "ttwid=fixture|token")
        fixture_protocol.ABogus.assert_called_once()

    def test_sec_profile_uses_sec_user_id_and_verifies_returned_numeric_uid(self) -> None:
        result, request, _ = self.douyin_lookup("https://www.douyin.com/user/" + SEC)
        params = parse_qs(urlsplit(request.call_args_list[1].args[0]).query)
        self.assertEqual(params["sec_user_id"], [SEC])
        self.assertNotIn("user_id", params)
        self.assertEqual(result["uid"], UID)

    def test_douyin_rejects_conflicting_uid_and_sec(self) -> None:
        for profile, body in [(DY_URL, douyin_body(uid="999999999")),
                              ("https://www.douyin.com/user/" + SEC, douyin_body(sec_uid="MS4wLjAB" + "b" * 48))]:
            with self.subTest(profile=profile), self.assertRaises(ProfileInputError) as raised:
                self.douyin_lookup(profile, body)
            self.assertEqual(raised.exception.code, "identity_conflict")

    def test_known_identity_enrichment_is_allowed_but_still_verified(self) -> None:
        parsed = ParsedProfile("douyin", "https://www.douyin.com/user/" + SEC, UID, SEC)
        for returned_uid in [UID, "999999999"]:
            with patch.object(public, "_douyin_protocol", return_value=protocol()), patch.object(
                public, "_request_once", side_effect=[response(**{"Set-Cookie": "ttwid=fixture"}),
                                                     response(douyin_body(uid=returned_uid))]
            ):
                if returned_uid == UID:
                    self.assertEqual(public.public_profile_lookup(parsed)["uid"], UID)
                else:
                    with self.assertRaises(ProfileInputError) as raised:
                        public.public_profile_lookup(parsed)
                    self.assertEqual(raised.exception.code, "identity_conflict")

    def test_duplicate_identity_keys_are_rejected(self) -> None:
        body = douyin_body().replace(b'"uid":', b'"uid":"999999999", "uid":')
        with self.assertRaises(ProfileInputError) as raised:
            self.douyin_lookup(body=body)
        self.assertEqual(raised.exception.code, "public_profile_response_invalid")

    def test_douyin_rejects_noncanonical_uid_missing_nickname_and_challenge(self) -> None:
        for body in [douyin_body(uid=123456), douyin_body(uid="unknown"), douyin_body(sec_uid=""),
                     douyin_body(nickname=" "), b'{"status_code":8,"verify":"captcha"}',
                     b'{"status_code":false,"user":{}}', b"<html>login required</html>"]:
            with self.subTest(body=body), self.assertRaises(ProfileInputError):
                self.douyin_lookup(body=body)

    def test_douyin_guest_failure_never_retries_or_requests_profile(self) -> None:
        for guest in [response(status=403), response(), response(**{"Set-Cookie": "session=secret"}),
                      response(status=302, Location="https://www.douyin.com/login")]:
            with patch.object(public, "_douyin_protocol", return_value=protocol()), patch.object(
                public, "_request_once", return_value=guest
            ) as request, self.assertRaises(ProfileInputError):
                public.public_profile_lookup(parse_profile_input(DY_URL))
            request.assert_called_once()

    def test_missing_gmssl_never_exits_process(self) -> None:
        for error in [SystemExit("missing gmssl"), ModuleNotFoundError("gmssl")]:
            with patch.object(public.importlib, "import_module", side_effect=error), self.assertRaises(ProfileInputError) as raised:
                public.public_profile_lookup(parse_profile_input(DY_URL))
            self.assertEqual(raised.exception.code, "public_profile_dependency_unavailable")
            self.assertIn("依赖", str(raised.exception))

    def test_xhs_basic_info_identity_and_red_id(self) -> None:
        with patch.object(public, "_request_once", return_value=response(xhs_body())) as request:
            result = public.public_profile_lookup(parse_profile_input(XHS_URL))
        self.assertEqual(result, {"platform": "xiaohongshu", "uid": XHS, "sec_user_id": None,
                                  "nickname": "测试账号", "display_account_id": "94323641835"})
        self.assertEqual(request.call_args.args[0], XHS_URL)
        self.assertNotIn("Cookie", request.call_args.kwargs["headers"])

    def test_xhs_page_level_user_id_supported_and_conflicts_rejected(self) -> None:
        body = xhs_body().decode().replace('"basicInfo": {"userId": "' + XHS + '", ',
                                         '"userId": "' + XHS + '", "basicInfo": {')
        with patch.object(public, "_request_once", return_value=response(body.encode())):
            self.assertEqual(public.public_profile_lookup(parse_profile_input(XHS_URL))["uid"], XHS)
        body = xhs_body().decode().replace('"userPageData": {', '"userPageData": {"userId":"' + OTHER_XHS + '",')
        with patch.object(public, "_request_once", return_value=response(body.encode())), self.assertRaises(ProfileInputError) as raised:
            public.public_profile_lookup(parse_profile_input(XHS_URL))
        self.assertEqual(raised.exception.code, "identity_conflict")

    def test_xhs_current_note_queries_identify_empty_homepage(self) -> None:
        # Shape observed by the release owner on an official public homepage;
        # basicInfo omits userId. Empty notes must not prevent verification.
        state = {"user": {"userPageData": {"result": {"success": True},
                 "basicInfo": {"nickname": "测试账号", "redId": "94323641835"},
                 "interactions": [], "tags": [], "tabPublic": {}, "extraInfo": {}},
                 "noteQueries": [{"userId": XHS, "cursor": "", "num": 30} for _ in range(5)],
                 "notes": []}}
        body = ("<script>window.__INITIAL_STATE__=" + json.dumps(state) + ";</script>").encode()
        with patch.object(public, "_request_once", return_value=response(body)):
            self.assertEqual(public.public_profile_lookup(parse_profile_input(XHS_URL))["uid"], XHS)
        state["user"]["noteQueries"][3]["userId"] = OTHER_XHS
        body = ("<script>window.__INITIAL_STATE__=" + json.dumps(state) + ";</script>").encode()
        with patch.object(public, "_request_once", return_value=response(body)), self.assertRaises(ProfileInputError) as raised:
            public.public_profile_lookup(parse_profile_input(XHS_URL))
        self.assertEqual(raised.exception.code, "identity_conflict")

    def test_xhs_note_queries_must_match_any_explicit_page_identity(self) -> None:
        body = xhs_body().decode().replace('"user": {', '"user": {"noteQueries":[{"userId":"' + OTHER_XHS + '"}],')
        with patch.object(public, "_request_once", return_value=response(body.encode())), self.assertRaises(ProfileInputError) as raised:
            public.public_profile_lookup(parse_profile_input(XHS_URL))
        self.assertEqual(raised.exception.code, "identity_conflict")

    def test_xhs_never_uses_note_author_as_homepage_identity(self) -> None:
        state = {"user": {"userPageData": {"basicInfo": {"nickname": "name"}},
                           "notes": [{"author": {"userId": XHS}}]}}
        body = ("<script>window.__INITIAL_STATE__=" + json.dumps(state) + ";</script>").encode()
        with patch.object(public, "_request_once", return_value=response(body)), self.assertRaises(ProfileInputError) as raised:
            public.public_profile_lookup(parse_profile_input(XHS_URL))
        self.assertEqual(raised.exception.code, "identity_unresolved")

    def test_xhs_rejects_mismatch_missing_identity_and_nickname(self) -> None:
        for body in [xhs_body(uid=OTHER_XHS), xhs_body(uid=None), xhs_body(nickname=""),
                     b'<script>window.__INITIAL_STATE__={"user":{"userPageData":{"basicInfo":{"nickname":"name"}}}}</script>',
                     b'<html>captcha login required</html>']:
            with self.subTest(body=body), patch.object(public, "_request_once", return_value=response(body)), self.assertRaises(ProfileInputError):
                public.public_profile_lookup(parse_profile_input(XHS_URL))

    def test_xhs_undefined_handling_never_changes_strings_or_executes_js(self) -> None:
        body = xhs_body(nickname="undefined 测试").decode().replace('"redId":', '"optional": undefined, "redId":')
        with patch.object(public, "_request_once", return_value=response(body.encode())):
            self.assertEqual(public.public_profile_lookup(parse_profile_input(XHS_URL))["nickname"], "undefined 测试")
        for source in ["window.__INITIAL_STATE__=alert(1)",
                       'window.__INITIAL_STATE__={}; fetch("https://example.com")',
                       'window.__INITIAL_STATE__={"user": (() => 1)()}']:
            with self.subTest(source=source), self.assertRaises(ProfileInputError):
                public._initial_state("<script>" + source + "</script>")

    def test_duplicate_initial_state_rejected(self) -> None:
        body = xhs_body(extra=xhs_body().decode())
        with patch.object(public, "_request_once", return_value=response(body)), self.assertRaises(ProfileInputError):
            public.public_profile_lookup(parse_profile_input(XHS_URL))

    def test_xhs_redirect_or_access_denial_does_not_get_followed(self) -> None:
        for status in [301, 401, 403, 429, 500]:
            with patch.object(public, "_request_once", return_value=response(status=status, Location="https://www.xiaohongshu.com/login")) as request, self.assertRaises(ProfileInputError):
                public.public_profile_lookup(parse_profile_input(XHS_URL))
            request.assert_called_once()

    def test_manual_parsed_profile_mismatch_rejected_before_network(self) -> None:
        for parsed in [ParsedProfile("douyin", DY_URL, "99999999", None),
                       ParsedProfile("xiaohongshu", "https://127.0.0.1/", XHS, None)]:
            with self.subTest(parsed=parsed), patch.object(public, "_request_once") as request, self.assertRaises(ProfileInputError):
                public.public_profile_lookup(parsed)
            request.assert_not_called()

    def test_short_link_official_redirect_yields_canonical_full_profile(self) -> None:
        with patch.object(public, "_request_once", return_value=response(status=302, Location=XHS_URL + "?share=tracking")) as request:
            self.assertEqual(public.expand_public_profile_url("https://xhslink.com/a/abc123"), XHS_URL)
        request.assert_called_once()
        self.assertNotIn("headers", request.call_args.kwargs)

    def test_short_link_rejects_other_hosts_platforms_credentials_and_http(self) -> None:
        for destination in ["http://www.douyin.com/user/" + UID, "https://127.0.0.1/", XHS_URL,
                            "https://www.douyin.com.evil.test/user/" + UID,
                            "https://user@www.douyin.com/user/" + UID,
                            "https://www.douyin.com:8443/user/" + UID,
                            "https://www.douyin.com/video/" + UID]:
            with self.subTest(destination=destination), patch.object(public, "_request_once", return_value=response(status=302, Location=destination)) as request, self.assertRaises(ProfileInputError):
                public.expand_public_profile_url("https://v.douyin.com/abc123/")
            request.assert_called_once()

    def test_short_link_limits_loops_hops_and_javascript_redirect(self) -> None:
        for responses in [[response(status=302, Location="https://v.douyin.com/abc123/")],
                          [response(status=302, Location="https://v.douyin.com/" + str(i) + "/") for i in range(10)],
                          [response(b'<script>location="' + DY_URL.encode() + b'"</script>')]]:
            with patch.object(public, "_request_once", side_effect=responses) as request, self.assertRaises(ProfileInputError):
                public.expand_public_profile_url("https://v.douyin.com/abc123/")
            self.assertLessEqual(request.call_count, public.MAX_REDIRECTS)

    def test_transport_timeout_is_clear_and_not_retried(self) -> None:
        with patch.object(public, "_resolve_public", side_effect=TimeoutError("timed out")) as resolve, self.assertRaises(ProfileInputError) as raised:
            public._request_once(XHS_URL, deadline=time.monotonic() + 10)
        self.assertEqual(raised.exception.code, "public_profile_transport_failed")
        resolve.assert_called_once()

    def test_dns_rejects_private_loopback_mixed_and_mapped_addresses(self) -> None:
        for addresses in [[(socket.AF_INET, socket.SOCK_STREAM, 6, "", (ip, 443))]
                          for ip in ["127.0.0.1", "10.1.2.3", "169.254.169.254", "100.64.1.2", "0.0.0.0"]] + [
                              [PUBLIC_ADDRESS, (socket.AF_INET, socket.SOCK_STREAM, 6, "", ("192.168.1.2", 443))],
                              [(socket.AF_INET6, socket.SOCK_STREAM, 6, "", ("::ffff:93.184.216.34", 443, 0, 0))]]:
            with self.subTest(addresses=addresses), patch.object(socket, "getaddrinfo", return_value=addresses), self.assertRaises(ProfileInputError) as raised:
                public._resolve_public("www.xiaohongshu.com", time.monotonic() + 2)
            self.assertEqual(raised.exception.code, "unsafe_profile_destination")

    def test_dns_resolution_is_bounded(self) -> None:
        with patch.object(public._DNS_SLOTS, "acquire", return_value=False), self.assertRaises(ProfileInputError) as raised:
            public._resolve_public("www.douyin.com", time.monotonic() + 2)
        self.assertEqual(raised.exception.code, "public_profile_busy")

    def test_pinned_socket_connects_numeric_ip_and_checks_hostname_tls(self) -> None:
        raw, secure = MagicMock(), MagicMock()
        context = ssl.create_default_context()
        self.assertTrue(context.check_hostname)
        self.assertEqual(context.verify_mode, ssl.CERT_REQUIRED)
        with patch.object(socket, "socket", return_value=raw), patch.object(context, "wrap_socket", return_value=secure) as tls, patch.object(public.ssl, "create_default_context", return_value=context):
            connection = public._PinnedHTTPSConnection("www.douyin.com", PUBLIC_ADDRESS, time.monotonic() + 5)
            connection.connect()
        raw.connect.assert_called_once_with(("93.184.216.34", 443))
        tls.assert_called_once_with(raw, server_hostname="www.douyin.com")
        self.assertIs(connection.sock, secure)
        connection.close()
        self.assertIsNone(connection.sock)
        # Response.makefile can retain the fd after httplib clears its socket.
        connection.abort()
        secure.shutdown.assert_called_once_with(socket.SHUT_RDWR)

    def test_transport_body_and_compression_limits_are_enforced(self) -> None:
        for headers, chunks in [({"Content-Length": str(public.MAX_RESPONSE_BYTES + 1)}, []),
                                ({"Content-Encoding": "gzip"}, []),
                                ({}, [b"x" * (public.MAX_RESPONSE_BYTES + 1)])]:
            connection = MagicMock()
            incoming = connection.getresponse.return_value
            incoming.getheaders.return_value = list(headers.items())
            incoming.getheader.side_effect = lambda name, default=None: headers.get(name, default)
            incoming.read1.side_effect = chunks
            with self.subTest(headers=headers), patch.object(public, "_resolve_public", return_value=PUBLIC_ADDRESS), patch.object(public, "_PinnedHTTPSConnection", return_value=connection), self.assertRaises(ProfileInputError):
                public._request_once(XHS_URL, deadline=time.monotonic() + 5)
            connection.close.assert_called()


if __name__ == "__main__":
    unittest.main()
