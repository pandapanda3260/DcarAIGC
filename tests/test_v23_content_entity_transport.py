"""Content entity completeness never grants clean-transport/profile qualification."""
import gzip
import hashlib
import http.client
import socket
import ssl
import unittest
import urllib.error
from unittest.mock import patch

from v8 import capture, provider_transport as transport, providers
from tests.test_v8_provider_transport import FakeOpener, FakeResponse, URL, clock


class ContentEntityTransportTest(unittest.TestCase):
    entity = b'{"code":200,"data":{"fixture":true}}'

    def request(self, encoded=None, operation="kuaishou_video_detail", headers=None, **kw):
        import urllib.request
        body = gzip.compress(self.entity, mtime=0) if encoded is None else encoded
        response = FakeResponse(b"", headers=headers or {"Content-Encoding": "gzip"},
                                read_error=http.client.IncompleteRead(body, 1))
        return transport.request_json(urllib.request.Request(URL), route_id="fixture", timeout=45,
            opener=FakeOpener(response), clock=clock(), content_operation=operation, **kw)

    def test_complete_content_keeps_framing_failure_and_strict_qualification_rejects(self):
        for operation in transport.CONTENT_ENTITY_OPERATIONS:
            with self.subTest(operation=operation):
                result = self.request(operation=operation)
                self.assertEqual(result.entity_body, self.entity)
                self.assertFalse(result.receipt["clean_eof"])
                self.assertTrue(transport.usable_content_entity(result.receipt, operation))
                capture._validate_usable_content_receipt(result.receipt, operation=operation,
                    entity_bytes=result.entity_body, http_status=200)
                with self.assertRaises(capture.RawEvidenceError):
                    capture._validate_complete_transport_receipt(result.receipt,
                        entity_bytes=result.entity_body, http_status=200)
                with self.assertRaises(capture.RawEvidenceError):
                    capture._validate_usable_content_receipt(result.receipt, operation="douyin_uid_profile",
                        entity_bytes=result.entity_body, http_status=200)

    def test_noncontent_and_damaged_entities_are_rejected(self):
        encoded = gzip.compress(self.entity, mtime=0)
        for operation in (None, "douyin_uid_profile", "kuaishou_user_posts", "xiaohongshu_note_comments"):
            with self.subTest(operation=operation), self.assertRaises((ValueError, transport.ProviderTransportError)):
                self.request(operation=operation)
        for body in (encoded[:-1], encoded[:-8] + bytes([encoded[-8] ^ 1]) + encoded[-7:],
                     encoded + b"x", encoded + encoded, gzip.compress(b'{"x":NaN}'),
                     gzip.compress(b'{"x":'), gzip.compress(b'\xff')):
            with self.subTest(body_sha=hashlib.sha256(body).hexdigest()), self.assertRaises(transport.ProviderTransportError):
                self.request(body)
        for headers in ({"Content-Encoding": "identity"},
                        {"Content-Encoding": "gzip", "Content-Length": str(len(encoded) + 1)}):
            with self.assertRaises(transport.ProviderTransportError):
                self.request(self.entity if headers["Content-Encoding"] == "identity" else encoded, headers=headers)
        with self.assertRaises(transport.ProviderTransportError):
            self.request(max_encoded_bytes=4)
        with self.assertRaises(transport.ProviderTransportError):
            self.request(max_entity_bytes=4)

    def test_saved_body_uses_exact_original_bytes_and_preserves_original_receipt(self):
        result = self.request()
        original = {**result.receipt, "status": "failed", "error_code": "transport_incomplete_read",
                    "json_parse_ok": False, "entity_sha256": None, "entity_bytes": None}
        saved = dict(original)
        restored = transport.validate_saved_content_entity(result.encoded_body, original,
                                                          operation="kuaishou_video_detail")
        self.assertEqual(original, saved)
        self.assertEqual(restored.entity_body, self.entity)
        self.assertFalse(restored.receipt["clean_eof"])
        with self.assertRaises(ValueError):
            transport.validate_saved_content_entity(result.encoded_body + b"x", original,
                                                     operation="kuaishou_video_detail")

    def test_error_chain_is_durable_and_cannot_leak_arbitrary_exception_text(self):
        import urllib.request
        errors = [socket.gaierror(-2, "secret=https://user:password@host/token?q=secret"),
                  TimeoutError("Bearer fixture-secret"), ssl.SSLError(1, "secret-string")]
        for reason in errors:
            opener = type("Opener", (), {"open": lambda *a, **kw: None})()
            with self.subTest(reason=type(reason).__name__), patch.object(opener, "open", side_effect=urllib.error.URLError(reason)):
                with self.assertRaises(transport.ProviderTransportError) as error:
                    transport.request_json(urllib.request.Request(URL), route_id="fixture", timeout=45,
                                           opener=opener, clock=clock())
                receipt = error.exception.receipt
                self.assertEqual(receipt["reason_type"], type(reason).__name__)
                self.assertEqual(receipt["failure_phase"], "open")
                self.assertNotIn("secret", str(receipt))
                with patch.object(providers, "request_json_transport", side_effect=error.exception):
                    with self.assertRaises(capture.CaptureError) as wrapped:
                        providers._request_json(URL, headers={}, params={}, provider="TikHub")
                    self.assertIn(type(reason).__name__, str(wrapped.exception))
                    self.assertEqual(wrapped.exception.transport_receipt, receipt)


if __name__ == "__main__":
    unittest.main()
