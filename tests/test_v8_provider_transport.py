from __future__ import annotations

import gzip
import hashlib
import http.client
import json
import unittest
import urllib.request
from collections.abc import Iterator
from types import SimpleNamespace
from typing import Any
from unittest.mock import patch

from v8.provider_transport import (  # type: ignore[import-untyped]
    CONTRACT_VERSION,
    ProviderTransportError,
    _NoRedirect,
    request_json,
)


URL = "https://api.tikhub.dev/api/v1/example"
ROUTE = "tikhub-dev-stream-v1"
TIMES = (
    "2026-09-05T00:00:00Z",
    "2026-09-05T00:00:01Z",
    "2026-09-05T00:00:02Z",
)


class FakeResponse:
    def __init__(
        self,
        body: bytes,
        *,
        status: int = 200,
        headers: dict[str, str] | None = None,
        response_url: str = URL,
        read_error: BaseException | None = None,
        close_error: BaseException | None = None,
        socket_fixture: Any | None = None,
    ) -> None:
        self.body = body
        self.status = status
        self.headers = headers or {}
        self.response_url = response_url
        self.read_error = read_error
        self.close_error = close_error
        if socket_fixture is not None:
            self.fp = SimpleNamespace(
                raw=SimpleNamespace(_sock=socket_fixture)
            )
        self.offset = 0
        self.read_sizes: list[int] = []
        self.closed = False

    def geturl(self) -> str:
        return self.response_url

    def read(self, size: int = -1) -> bytes:
        self.read_sizes.append(size)
        if size is None or size < 0:
            size = len(self.body) - self.offset
        if self.read_error is not None:
            error, self.read_error = self.read_error, None
            raise error
        block = self.body[self.offset : self.offset + size]
        self.offset += len(block)
        return block

    def close(self) -> None:
        self.closed = True
        if self.close_error is not None:
            raise self.close_error


class FakeOpener:
    def __init__(self, response: FakeResponse) -> None:
        self.response = response
        self.request: urllib.request.Request | None = None
        self.timeout: float | None = None

    def open(self, request: urllib.request.Request, *, timeout: float) -> FakeResponse:
        self.request = request
        self.timeout = timeout
        return self.response


def clock(values: tuple[str, ...] = TIMES) -> Any:
    iterator: Iterator[str] = iter(values)
    return lambda: next(iterator)


class ProviderTransportTest(unittest.TestCase):
    def request(self) -> urllib.request.Request:
        return urllib.request.Request(URL, headers={"Authorization": "fixture"})

    def test_identity_json_is_read_in_chunks_with_complete_receipt(self) -> None:
        body = json.dumps({"ok": True, "items": [1, 2]}, separators=(",", ":")).encode()
        response = FakeResponse(body, headers={"Content-Length": str(len(body))})
        opener = FakeOpener(response)

        result = request_json(
            self.request(),
            route_id=ROUTE,
            timeout=45,
            opener=opener,
            clock=clock(),
            chunk_size=5,
        )

        self.assertEqual(result.status, 200)
        self.assertEqual(result.payload, {"ok": True, "items": [1, 2]})
        self.assertEqual(result.encoded_body, body)
        self.assertEqual(result.entity_body, body)
        self.assertEqual(opener.timeout, 45.0)
        assert opener.request is not None
        self.assertEqual(opener.request.get_header("Accept-encoding"), "gzip")
        self.assertGreater(len(response.read_sizes), 1)
        self.assertTrue(response.closed)
        self.assertEqual(
            result.receipt,
            {
                "contract_version": CONTRACT_VERSION,
                "transport_route_id": ROUTE,
                "route_generation": "unsealed",
                "http_stack": "urllib-stream-v1",
                "tls_version": None,
                "tls_cipher": None,
                "alpn_protocol": None,
                "request_host": "api.tikhub.dev",
                "request_port": None,
                "credential_fingerprint": hashlib.sha256(
                    b"dcar-provider-credential-v1\0fixture"
                ).hexdigest(),
                "request_started_at": TIMES[0],
                "headers_received_at": TIMES[1],
                "response_finished_at": TIMES[2],
                "http_status": 200,
                "content_encoding": "identity",
                "content_length": len(body),
                "http_encoded_bytes": len(body),
                "http_encoded_sha256": hashlib.sha256(body).hexdigest(),
                "entity_bytes": len(body),
                "entity_sha256": hashlib.sha256(body).hexdigest(),
                "clean_eof": True,
                "length_match": True,
                "gzip_crc_ok": None,
                "json_parse_ok": True,
                "json_parse_error": None,
                "partial_bytes": 0,
                "partial_sha256": None,
                "quarantine_path": None,
                "stored_bytes": None,
                "stored_sha256": None,
                "zero_body": False,
                "retry_after": None,
                "status": "succeeded",
                "error_code": None,
                "response_close_error": None,
            },
        )

    def test_gzip_requires_crc_and_reports_encoded_and_entity_hashes(self) -> None:
        entity = b'{"code":200,"data":{"value":1}}'
        encoded = gzip.compress(entity, mtime=0)
        result = request_json(
            self.request(),
            route_id=ROUTE,
            timeout=45,
            opener=FakeOpener(
                FakeResponse(
                    encoded,
                    headers={
                        "Content-Encoding": "gzip",
                        "Content-Length": str(len(encoded)),
                    },
                )
            ),
            clock=clock(),
            chunk_size=7,
        )

        self.assertEqual(result.payload, {"code": 200, "data": {"value": 1}})
        self.assertEqual(result.entity_body, entity)
        self.assertTrue(result.receipt["gzip_crc_ok"])
        self.assertEqual(result.receipt["http_encoded_bytes"], len(encoded))
        self.assertEqual(
            result.receipt["http_encoded_sha256"], hashlib.sha256(encoded).hexdigest()
        )
        self.assertEqual(result.receipt["entity_bytes"], len(entity))
        self.assertEqual(
            result.receipt["entity_sha256"], hashlib.sha256(entity).hexdigest()
        )

    def test_redirect_handler_never_follows_and_cross_host_body_is_not_read(
        self,
    ) -> None:
        handler = _NoRedirect()
        self.assertIsNone(
            handler.redirect_request(None, None, 302, "", {}, "https://evil.invalid")
        )
        response = FakeResponse(
            b'{"redirected":true}',
            status=302,
            headers={"Location": "https://api.tikhub.io/other"},
        )

        with self.assertRaises(ProviderTransportError) as caught:
            request_json(
                self.request(),
                route_id=ROUTE,
                timeout=45,
                opener=FakeOpener(response),
                clock=clock(),
            )

        error = caught.exception
        self.assertEqual(error.error_code, "transport_cross_host_redirect")
        self.assertEqual(error.partial_bytes, b"")
        self.assertEqual(response.read_sizes, [])
        self.assertFalse(error.receipt["clean_eof"])
        self.assertFalse(error.receipt["json_parse_ok"])

    def test_default_urllib_opener_installs_redirect_blocker(self) -> None:
        body = b'{"ok":true}'
        opener = FakeOpener(FakeResponse(body))

        with patch(
            "v8.provider_transport.urllib.request.build_opener", return_value=opener
        ) as build_opener:
            result = request_json(
                self.request(), route_id=ROUTE, timeout=45, clock=clock()
            )

        self.assertEqual(result.payload, {"ok": True})
        build_opener.assert_called_once()
        self.assertIsInstance(build_opener.call_args.args[0], _NoRedirect)

    def test_incomplete_read_never_accepts_even_complete_partial_json(self) -> None:
        partial = b'{"syntactically":"complete"}'
        response = FakeResponse(
            b"",
            headers={"Content-Length": str(len(partial) + 10)},
            read_error=http.client.IncompleteRead(partial, len(partial) + 10),
        )

        with self.assertRaises(ProviderTransportError) as caught:
            request_json(
                self.request(),
                route_id=ROUTE,
                timeout=45,
                opener=FakeOpener(response),
                clock=clock(),
                chunk_size=4,
            )

        error = caught.exception
        self.assertEqual(error.error_code, "transport_incomplete_read")
        self.assertEqual(error.partial_bytes, partial)
        self.assertEqual(error.partial_sha256, hashlib.sha256(partial).hexdigest())
        self.assertEqual(error.receipt["http_encoded_bytes"], len(partial))

    def test_legacy_stack_reads_once_without_gzip_and_records_partial_json(self) -> None:
        partial = b'{"syntactically":"complete"}'
        response = FakeResponse(
            b"",
            headers={"Content-Length": str(len(partial) + 10)},
            read_error=http.client.IncompleteRead(partial, len(partial) + 10),
        )
        opener = FakeOpener(response)

        result = request_json(
            self.request(),
            route_id="tikhub-api.tikhub.dev-legacy-v1",
            http_stack="urllib-legacy-v1",
            timeout=45,
            opener=opener,
            clock=clock(),
            chunk_size=4,
        )

        assert opener.request is not None
        self.assertIsNone(opener.request.get_header("Accept-encoding"))
        self.assertEqual(response.read_sizes, [-1])
        self.assertEqual(result.payload, {"syntactically": "complete"})
        self.assertEqual(result.receipt["http_stack"], "urllib-legacy-v1")
        self.assertFalse(result.receipt["clean_eof"])
        self.assertFalse(result.receipt["length_match"])
        self.assertTrue(result.receipt["json_parse_ok"])
        self.assertTrue(result.receipt["legacy_partial_json_accepted"])
        self.assertEqual(
            result.receipt["http_encoded_sha256"],
            hashlib.sha256(partial).hexdigest(),
        )

    def test_content_length_mismatch_rejects_valid_json(self) -> None:
        body = b'{"ok":true}'
        with self.assertRaises(ProviderTransportError) as caught:
            request_json(
                self.request(),
                route_id=ROUTE,
                timeout=45,
                opener=FakeOpener(
                    FakeResponse(body, headers={"Content-Length": str(len(body) + 1)})
                ),
                clock=clock(),
                chunk_size=3,
            )

        error = caught.exception
        self.assertEqual(error.error_code, "transport_content_length_mismatch")
        self.assertTrue(error.receipt["clean_eof"])
        self.assertFalse(error.receipt["length_match"])
        self.assertFalse(error.receipt["json_parse_ok"])

    def test_corrupt_gzip_crc_is_never_parsed(self) -> None:
        entity = b'{"ok":true}'
        encoded = bytearray(gzip.compress(entity, mtime=0))
        encoded[-1] ^= 0xFF
        wire = bytes(encoded)

        with self.assertRaises(ProviderTransportError) as caught:
            request_json(
                self.request(),
                route_id=ROUTE,
                timeout=45,
                opener=FakeOpener(
                    FakeResponse(
                        wire,
                        headers={
                            "Content-Encoding": "gzip",
                            "Content-Length": str(len(wire)),
                        },
                    )
                ),
                clock=clock(),
                chunk_size=4,
            )

        error = caught.exception
        self.assertEqual(error.error_code, "transport_gzip_invalid")
        self.assertFalse(error.receipt["gzip_crc_ok"])
        self.assertFalse(error.receipt["json_parse_ok"])
        self.assertEqual(error.partial_bytes, wire)

    def test_invalid_utf8_or_json_is_rejected_after_byte_checks(self) -> None:
        for body in (b'{"unterminated":', b'{"value":NaN}', b"\xff"):
            with (
                self.subTest(body=body),
                self.assertRaises(ProviderTransportError) as caught,
            ):
                request_json(
                    self.request(),
                    route_id=ROUTE,
                    timeout=45,
                    opener=FakeOpener(FakeResponse(body)),
                    clock=clock(),
                )
            error = caught.exception
            self.assertEqual(error.error_code, "transport_json_invalid")
            self.assertTrue(error.receipt["clean_eof"])
            self.assertIsNone(error.receipt["length_match"])
            self.assertFalse(error.receipt["json_parse_ok"])
            self.assertEqual(error.partial_bytes, body)

    def test_deep_json_is_a_typed_transport_failure(self) -> None:
        body = (b"[" * 20_000) + b"0" + (b"]" * 20_000)

        with self.assertRaises(ProviderTransportError) as caught:
            request_json(
                self.request(),
                route_id=ROUTE,
                timeout=45,
                opener=FakeOpener(FakeResponse(body)),
                clock=clock(),
            )

        self.assertEqual(caught.exception.error_code, "transport_json_invalid")
        self.assertEqual(caught.exception.partial_bytes, body)
        self.assertEqual(caught.exception.receipt["json_parse_error"], "RecursionError")

    def test_response_close_error_does_not_erase_a_complete_result(self) -> None:
        response = FakeResponse(
            b'{"ok":true}',
            close_error=OSError("fixture close failed"),
        )

        result = request_json(
            self.request(),
            route_id=ROUTE,
            timeout=45,
            opener=FakeOpener(response),
            clock=clock(),
        )

        self.assertEqual(result.payload, {"ok": True})
        self.assertTrue(response.closed)
        self.assertEqual(result.receipt["response_close_error"], "OSError")

    def test_encoded_and_gzip_entity_caps_fail_before_unbounded_growth(self) -> None:
        with self.assertRaises(ProviderTransportError) as encoded_failure:
            request_json(
                self.request(),
                route_id=ROUTE,
                timeout=45,
                opener=FakeOpener(FakeResponse(b'{"value":"0123456789"}')),
                clock=clock(),
                chunk_size=4,
                max_encoded_bytes=8,
                max_entity_bytes=100,
            )
        self.assertEqual(
            encoded_failure.exception.error_code,
            "transport_encoded_too_large",
        )
        self.assertLessEqual(len(encoded_failure.exception.partial_bytes), 12)

        entity = b'{"value":"' + (b"x" * 100) + b'"}'
        encoded = gzip.compress(entity, mtime=0)
        with self.assertRaises(ProviderTransportError) as entity_failure:
            request_json(
                self.request(),
                route_id=ROUTE,
                timeout=45,
                opener=FakeOpener(
                    FakeResponse(encoded, headers={"Content-Encoding": "gzip"})
                ),
                clock=clock(),
                chunk_size=5,
                max_encoded_bytes=100,
                max_entity_bytes=20,
            )
        self.assertEqual(
            entity_failure.exception.error_code,
            "transport_entity_too_large",
        )
        self.assertEqual(entity_failure.exception.partial_bytes, encoded)

    def test_tls_and_alpn_are_attributed_when_exposed_by_http_stack(self) -> None:
        class FakeSocket:
            def version(self) -> str:
                return "TLSv1.3"

            def cipher(self) -> tuple[str, str, int]:
                return ("TLS_AES_256_GCM_SHA384", "TLSv1.3", 256)

            def selected_alpn_protocol(self) -> str:
                return "h2"

        result = request_json(
            self.request(),
            route_id=ROUTE,
            route_generation="route-config-sha256:fixture",
            timeout=45,
            opener=FakeOpener(
                FakeResponse(b'{"ok":true}', socket_fixture=FakeSocket())
            ),
            clock=clock(),
        )

        self.assertEqual(result.receipt["route_generation"], "route-config-sha256:fixture")
        self.assertEqual(result.receipt["tls_version"], "TLSv1.3")
        self.assertEqual(result.receipt["tls_cipher"], "TLS_AES_256_GCM_SHA384")
        self.assertEqual(result.receipt["alpn_protocol"], "h2")


if __name__ == "__main__":
    unittest.main()
