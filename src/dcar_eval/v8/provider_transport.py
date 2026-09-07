"""Strict streaming JSON transport shared by paid provider adapters.

The caller owns URL construction, authorization, route selection, and timeout
policy.  This module owns only the HTTP byte boundary: redirects are never
followed automatically, encoded bytes are read in chunks, and no JSON value is
returned until framing, optional gzip integrity, UTF-8, and JSON parsing all
succeed.
"""

from __future__ import annotations

import hashlib
import http.client
import json
import socket
import ssl
import urllib.error
import urllib.parse
import urllib.request
import zlib
from contextlib import contextmanager
from contextvars import ContextVar
from dataclasses import dataclass
from datetime import datetime, timezone
from typing import Any, Callable, Iterator, Mapping, NoReturn


CONTRACT_VERSION = "provider-json-transport-v1"
DEFAULT_CHUNK_SIZE = 64 * 1024
DEFAULT_MAX_ENCODED_BYTES = 128 * 1024 * 1024
DEFAULT_MAX_ENTITY_BYTES = 512 * 1024 * 1024
ALLOWED_HTTP_STACKS = frozenset({"urllib-stream-v1", "urllib-legacy-v1"})

Clock = Callable[[], str]
_REQUEST_TRANSPORT: ContextVar[dict[str, Any] | None] = ContextVar("frozen_request_transport", default=None)


class RequestTransportBindingError(RuntimeError):
    error_code = "provider_transport_blocked"


def validate_request_transport_binding(binding: Mapping[str, Any]) -> dict[str, Any]:
    """Recheck the selected config before claiming or marking a paid send."""
    from pathlib import Path
    from tikhub_config import (  # type: ignore[import-untyped]
        TikHubConfigurationError,
        validate_current_tikhub_transport_manifest,
    )

    try:
        frozen = json.loads(json.dumps(dict(binding), sort_keys=True, allow_nan=False))
    except (TypeError, ValueError) as error:
        raise RequestTransportBindingError("request transport binding is not canonical JSON") from error
    if set(frozen) != {"manifest", "config_path", "honor_environment"} or type(frozen["honor_environment"]) is not bool:
        raise RequestTransportBindingError("request transport binding is malformed")
    path = frozen["config_path"]
    if path is not None and (not isinstance(path, str) or not Path(path).is_absolute()):
        raise RequestTransportBindingError("transport config path must be absolute")
    try:
        validate_current_tikhub_transport_manifest(
            frozen["manifest"], default_path=Path(path) if path else None,
            honor_environment=frozen["honor_environment"],
        )
    except TikHubConfigurationError as error:
        raise RequestTransportBindingError(str(error)) from error
    return frozen


@contextmanager
def request_transport_context(binding: Mapping[str, Any] | None) -> Iterator[None]:
    # This carries an already-validated immutable request choice, not authority.
    # Both paid gates independently read the binding before entering this scope.
    frozen = json.loads(json.dumps(dict(binding), sort_keys=True, allow_nan=False)) if binding is not None else None
    token = _REQUEST_TRANSPORT.set(frozen)
    try:
        yield
    finally:
        _REQUEST_TRANSPORT.reset(token)


def current_request_transport() -> dict[str, Any] | None:
    value = _REQUEST_TRANSPORT.get()
    return json.loads(json.dumps(value)) if value is not None else None


class _NoRedirect(urllib.request.HTTPRedirectHandler):
    """Return redirect responses to the caller instead of following them."""

    def redirect_request(
        self,
        req: Any,
        fp: Any,
        code: Any,
        msg: Any,
        headers: Any,
        newurl: Any,
    ) -> None:
        return None


@dataclass(frozen=True, slots=True)
class JsonTransportResult:
    """A fully verified HTTP JSON response and its byte-level evidence."""

    status: int
    payload: Any
    encoded_body: bytes
    entity_body: bytes
    receipt: dict[str, Any]

    def __iter__(self):
        """Keep tuple-unpacking compatibility at existing parser call sites."""

        yield self.status
        yield self.payload


class ProviderTransportError(RuntimeError):
    """A transport response failed before it became canonical JSON."""

    def __init__(
        self,
        code: str,
        message: str,
        *,
        partial_bytes: bytes,
        receipt: dict[str, Any],
    ) -> None:
        super().__init__(message)
        self.error_code = code
        self.partial_bytes = partial_bytes
        self.partial_sha256 = hashlib.sha256(partial_bytes).hexdigest()
        self.receipt = receipt


def _now_utc() -> str:
    return (
        datetime.now(timezone.utc).isoformat(timespec="seconds").replace("+00:00", "Z")
    )


def _authority(url: str) -> tuple[str, int | None]:
    parsed = urllib.parse.urlsplit(url)
    return (str(parsed.hostname or "").lower(), parsed.port)


def _credential_fingerprint(request: urllib.request.Request) -> str | None:
    """Return a one-way credential generation marker without retaining secrets."""

    authorization = request.get_header("Authorization")
    if not authorization:
        return None
    return hashlib.sha256(
        b"dcar-provider-credential-v1\0" + str(authorization).encode("utf-8")
    ).hexdigest()


def _header(response: Any, name: str) -> str | None:
    headers = getattr(response, "headers", None)
    if headers is None:
        return None
    value = headers.get(name)
    return str(value) if value is not None else None


def _status(response: Any) -> int:
    value = getattr(response, "status", None)
    if value is None:
        value = getattr(response, "code", None)
    if value is None and callable(getattr(response, "getcode", None)):
        value = response.getcode()
    if isinstance(value, bool) or not isinstance(value, int):
        raise ValueError("HTTP response status is missing or invalid")
    return value


def _finish_failure(
    receipt: dict[str, Any],
    *,
    code: str,
    message: str,
    partial_bytes: bytes,
    clock: Clock,
) -> NoReturn:
    receipt.update(
        status="failed",
        response_finished_at=clock(),
        http_encoded_bytes=len(partial_bytes),
        http_encoded_sha256=hashlib.sha256(partial_bytes).hexdigest(),
        partial_bytes=len(partial_bytes),
        partial_sha256=hashlib.sha256(partial_bytes).hexdigest()
        if partial_bytes
        else None,
        zero_body=not partial_bytes,
        json_parse_error=receipt.get("json_parse_error") or "skipped",
        error_code=code,
    )
    raise ProviderTransportError(
        code,
        message,
        partial_bytes=partial_bytes,
        receipt=receipt,
    )


def _strict_json(entity: bytes) -> Any:
    def reject_constant(value: str) -> NoReturn:
        raise ValueError(f"non-finite JSON constant: {value}")

    return json.loads(entity.decode("utf-8", "strict"), parse_constant=reject_constant)


def _gzip_entity(
    encoded: bytes,
    *,
    chunk_size: int,
    max_entity_bytes: int,
) -> tuple[bytes, bool, bool]:
    decoder = zlib.decompressobj(16 + zlib.MAX_WBITS)
    parts: list[bytes] = []
    expanded = 0
    try:
        for offset in range(0, len(encoded), chunk_size):
            pending = encoded[offset : offset + chunk_size]
            while pending:
                part = decoder.decompress(
                    pending,
                    max(1, max_entity_bytes - expanded + 1),
                )
                parts.append(part)
                expanded += len(part)
                if expanded > max_entity_bytes:
                    return b"".join(parts), False, True
                pending = decoder.unconsumed_tail
        part = decoder.flush(max(1, max_entity_bytes - expanded + 1))
        parts.append(part)
        expanded += len(part)
        if expanded > max_entity_bytes:
            return b"".join(parts), False, True
    except zlib.error:
        return b"".join(parts), False, False
    valid = bool(
        decoder.eof and not decoder.unused_data and not decoder.unconsumed_tail
    )
    return b"".join(parts), valid, False


def _tls_metadata(response: Any) -> tuple[str | None, str | None, str | None]:
    """Best-effort TLS attribution without making it a response-validity gate."""

    candidate = response
    for attribute in ("fp", "raw", "_sock"):
        nested = getattr(candidate, attribute, None)
        if nested is None:
            break
        candidate = nested
    try:
        version = candidate.version() if callable(getattr(candidate, "version", None)) else None
    except Exception:
        version = None
    try:
        cipher_value = candidate.cipher() if callable(getattr(candidate, "cipher", None)) else None
        cipher = str(cipher_value[0]) if isinstance(cipher_value, tuple) and cipher_value else None
    except Exception:
        cipher = None
    try:
        alpn = (
            candidate.selected_alpn_protocol()
            if callable(getattr(candidate, "selected_alpn_protocol", None))
            else None
        )
    except Exception:
        alpn = None
    return (
        str(version) if version is not None else None,
        cipher,
        str(alpn) if alpn is not None else None,
    )


def request_json(
    request: urllib.request.Request,
    *,
    route_id: str,
    route_generation: str = "unsealed",
    http_stack: str = "urllib-stream-v1",
    timeout: float,
    opener: Any | None = None,
    clock: Clock = _now_utc,
    chunk_size: int = DEFAULT_CHUNK_SIZE,
    max_encoded_bytes: int = DEFAULT_MAX_ENCODED_BYTES,
    max_entity_bytes: int = DEFAULT_MAX_ENTITY_BYTES,
) -> JsonTransportResult:
    """Open ``request`` and return JSON only after strict transport validation.

    ``timeout`` is deliberately required: provider adapters must pass their
    frozen policy value (currently 45 seconds) rather than inheriting a hidden
    transport default.  ``opener`` and ``clock`` are injectable so tests never
    need a socket or wall clock.
    """

    if not route_id.strip() or not route_generation.strip() or http_stack not in ALLOWED_HTTP_STACKS:
        raise ValueError("route identity fields must be nonempty")
    if (
        isinstance(timeout, bool)
        or not isinstance(timeout, (int, float))
        or timeout <= 0
    ):
        raise ValueError("timeout must be positive")
    if (
        isinstance(chunk_size, bool)
        or not isinstance(chunk_size, int)
        or chunk_size <= 0
    ):
        raise ValueError("chunk_size must be a positive integer")
    if not callable(clock):
        raise TypeError("clock must be callable")
    for value, field in (
        (max_encoded_bytes, "max_encoded_bytes"),
        (max_entity_bytes, "max_entity_bytes"),
    ):
        if isinstance(value, bool) or not isinstance(value, int) or value <= 0:
            raise ValueError(f"{field} must be a positive integer")

    if http_stack == "urllib-stream-v1":
        request.add_header("Accept-Encoding", "gzip")
    request_url = request.full_url
    request_host, request_port = _authority(request_url)
    if not request_host:
        raise ValueError("request URL must include a host")
    receipt: dict[str, Any] = {
        "contract_version": CONTRACT_VERSION,
        "transport_route_id": route_id,
        "route_generation": route_generation,
        "http_stack": http_stack,
        "tls_version": None,
        "tls_cipher": None,
        "alpn_protocol": None,
        "request_host": request_host,
        "request_port": request_port,
        "credential_fingerprint": _credential_fingerprint(request),
        "request_started_at": clock(),
        "headers_received_at": None,
        "response_finished_at": None,
        "http_status": None,
        "content_encoding": None,
        "content_length": None,
        "http_encoded_bytes": 0,
        "http_encoded_sha256": hashlib.sha256(b"").hexdigest(),
        "entity_bytes": None,
        "entity_sha256": None,
        "clean_eof": False,
        "length_match": None,
        "gzip_crc_ok": None,
        "json_parse_ok": False,
        "json_parse_error": None,
        "partial_bytes": 0,
        "partial_sha256": None,
        "quarantine_path": None,
        "stored_bytes": None,
        "stored_sha256": None,
        "zero_body": None,
        "retry_after": None,
        "status": "running",
        "error_code": None,
        "response_close_error": None,
    }

    transport = opener or urllib.request.build_opener(_NoRedirect())
    try:
        try:
            response = transport.open(request, timeout=float(timeout))
        except urllib.error.HTTPError as error:
            # HTTPError is also the unfollowed response object for 3xx/4xx/5xx.
            response = error
    except (
        OSError,
        urllib.error.URLError,
        http.client.HTTPException,
        TimeoutError,
        ConnectionError,
        socket.gaierror,
        ssl.SSLError,
    ) as error:
        try:
            _finish_failure(
                receipt,
                code="transport_open_failed",
                message=f"provider transport open failed: {type(error).__name__}",
                partial_bytes=b"",
                clock=clock,
            )
        except ProviderTransportError as failure:
            raise failure from error

    try:
        try:
            status = _status(response)
        except ValueError as error:
            try:
                _finish_failure(
                    receipt,
                    code="transport_status_invalid",
                    message=str(error),
                    partial_bytes=b"",
                    clock=clock,
                )
            except ProviderTransportError as failure:
                raise failure from error
        receipt.update(http_status=status, headers_received_at=clock())
        tls_version, tls_cipher, alpn_protocol = _tls_metadata(response)
        receipt.update(
            tls_version=tls_version,
            tls_cipher=tls_cipher,
            alpn_protocol=alpn_protocol,
        )
        receipt["retry_after"] = _header(response, "Retry-After")

        response_url = (
            str(response.geturl())
            if callable(getattr(response, "geturl", None))
            else request_url
        )
        if _authority(response_url) != (request_host, request_port):
            _finish_failure(
                receipt,
                code="transport_cross_host_redirect",
                message="provider transport crossed the frozen request host",
                partial_bytes=b"",
                clock=clock,
            )
        location = _header(response, "Location")
        if 300 <= status < 400 and location is not None:
            redirect_url = urllib.parse.urljoin(request_url, location)
            if _authority(redirect_url) != (request_host, request_port):
                _finish_failure(
                    receipt,
                    code="transport_cross_host_redirect",
                    message="provider transport rejected a cross-host redirect",
                    partial_bytes=b"",
                    clock=clock,
                )

        content_encoding = (
            (_header(response, "Content-Encoding") or "identity").strip().lower()
        )
        receipt["content_encoding"] = content_encoding
        if content_encoding not in {"identity", "gzip"}:
            _finish_failure(
                receipt,
                code="transport_content_encoding_unsupported",
                message=f"unsupported Content-Encoding: {content_encoding}",
                partial_bytes=b"",
                clock=clock,
            )

        declared_length = _header(response, "Content-Length")
        if declared_length is not None:
            normalized_length = declared_length.strip()
            if not normalized_length.isascii() or not normalized_length.isdecimal():
                _finish_failure(
                    receipt,
                    code="transport_content_length_invalid",
                    message="Content-Length must be a nonnegative decimal integer",
                    partial_bytes=b"",
                    clock=clock,
                )
            receipt["content_length"] = int(normalized_length)
            if receipt["content_length"] > max_encoded_bytes:
                _finish_failure(
                    receipt,
                    code="transport_encoded_too_large",
                    message="provider response exceeds the encoded byte limit",
                    partial_bytes=b"",
                    clock=clock,
                )

        chunks: list[bytes] = []
        encoded_size = 0
        parsed_legacy_payload: Any | None = None
        legacy_partial_json_accepted = False
        while http_stack == "urllib-stream-v1":
            try:
                block = response.read(chunk_size)
            except http.client.IncompleteRead as error:
                partial = error.partial if isinstance(error.partial, bytes) else b""
                encoded = b"".join([*chunks, partial])
                receipt["clean_eof"] = False
                if receipt["content_length"] is not None:
                    receipt["length_match"] = len(encoded) == receipt["content_length"]
                try:
                    _finish_failure(
                        receipt,
                        code="transport_incomplete_read",
                        message="provider response ended with IncompleteRead",
                        partial_bytes=encoded,
                        clock=clock,
                    )
                except ProviderTransportError as failure:
                    raise failure from error
            except (
                OSError,
                urllib.error.URLError,
                http.client.HTTPException,
                TimeoutError,
                ConnectionError,
                socket.gaierror,
                ssl.SSLError,
            ) as error:
                encoded = b"".join(chunks)
                receipt["clean_eof"] = False
                try:
                    _finish_failure(
                        receipt,
                        code="transport_read_failed",
                        message=f"provider response read failed: {type(error).__name__}",
                        partial_bytes=encoded,
                        clock=clock,
                    )
                except ProviderTransportError as failure:
                    raise failure from error
            if not isinstance(block, bytes):
                _finish_failure(
                    receipt,
                    code="transport_read_invalid",
                    message="provider response read returned non-bytes",
                    partial_bytes=b"".join(chunks),
                    clock=clock,
                )
            if not block:
                receipt["clean_eof"] = True
                break
            chunks.append(block)
            encoded_size += len(block)
            if encoded_size > max_encoded_bytes:
                _finish_failure(
                    receipt,
                    code="transport_encoded_too_large",
                    message="provider response exceeds the encoded byte limit",
                    partial_bytes=b"".join(chunks),
                    clock=clock,
                )
        if http_stack == "urllib-legacy-v1":
            try:
                block = response.read()
            except http.client.IncompleteRead as error:
                block = error.partial if isinstance(error.partial, bytes) else b""
                receipt["clean_eof"] = False
                try:
                    parsed_legacy_payload = _strict_json(block)
                except (
                    UnicodeDecodeError,
                    json.JSONDecodeError,
                    ValueError,
                    RecursionError,
                    MemoryError,
                ) as parse_error:
                    try:
                        _finish_failure(
                            receipt,
                            code="transport_incomplete_read",
                            message="provider response ended with IncompleteRead",
                            partial_bytes=block,
                            clock=clock,
                        )
                    except ProviderTransportError as failure:
                        raise failure from parse_error
                legacy_partial_json_accepted = True
                receipt["legacy_partial_json_accepted"] = True
            except (
                OSError,
                urllib.error.URLError,
                http.client.HTTPException,
                TimeoutError,
                ConnectionError,
                socket.gaierror,
                ssl.SSLError,
            ) as error:
                try:
                    _finish_failure(
                        receipt,
                        code="transport_read_failed",
                        message=f"provider response read failed: {type(error).__name__}",
                        partial_bytes=b"",
                        clock=clock,
                    )
                except ProviderTransportError as failure:
                    raise failure from error
            else:
                receipt["clean_eof"] = True
            if not isinstance(block, bytes):
                _finish_failure(
                    receipt,
                    code="transport_read_invalid",
                    message="provider response read returned non-bytes",
                    partial_bytes=b"",
                    clock=clock,
                )
            if len(block) > max_encoded_bytes:
                _finish_failure(
                    receipt,
                    code="transport_encoded_too_large",
                    message="provider response exceeds the encoded byte limit",
                    partial_bytes=block,
                    clock=clock,
                )
            chunks.append(block)

        encoded = b"".join(chunks)
        encoded_sha256 = hashlib.sha256(encoded).hexdigest()
        content_length = receipt["content_length"]
        length_match = (
            None if content_length is None else len(encoded) == content_length
        )
        receipt.update(
            http_encoded_bytes=len(encoded),
            http_encoded_sha256=encoded_sha256,
            length_match=length_match,
        )
        if length_match is False and not legacy_partial_json_accepted:
            _finish_failure(
                receipt,
                code="transport_content_length_mismatch",
                message="provider response length does not match Content-Length",
                partial_bytes=encoded,
                clock=clock,
            )

        if content_encoding == "gzip":
            entity, gzip_valid, entity_too_large = _gzip_entity(
                encoded,
                chunk_size=chunk_size,
                max_entity_bytes=max_entity_bytes,
            )
            receipt["gzip_crc_ok"] = gzip_valid
            receipt.update(
                entity_bytes=len(entity),
                entity_sha256=hashlib.sha256(entity).hexdigest(),
            )
            if entity_too_large:
                _finish_failure(
                    receipt,
                    code="transport_entity_too_large",
                    message="provider response exceeds the entity byte limit",
                    partial_bytes=encoded,
                    clock=clock,
                )
            if not gzip_valid:
                _finish_failure(
                    receipt,
                    code="transport_gzip_invalid",
                    message="gzip stream is incomplete or failed CRC validation",
                    partial_bytes=encoded,
                    clock=clock,
                )
        else:
            entity = encoded
            if len(entity) > max_entity_bytes:
                _finish_failure(
                    receipt,
                    code="transport_entity_too_large",
                    message="provider response exceeds the entity byte limit",
                    partial_bytes=encoded,
                    clock=clock,
                )
            receipt.update(
                entity_bytes=len(entity),
                entity_sha256=hashlib.sha256(entity).hexdigest(),
            )

        if parsed_legacy_payload is None:
            try:
                payload = _strict_json(entity)
            except (
                UnicodeDecodeError,
                json.JSONDecodeError,
                ValueError,
                RecursionError,
                MemoryError,
            ) as error:
                receipt["json_parse_error"] = type(error).__name__
                try:
                    _finish_failure(
                        receipt,
                        code="transport_json_invalid",
                        message=f"provider response is not strict UTF-8 JSON: {type(error).__name__}",
                        partial_bytes=encoded,
                        clock=clock,
                    )
                except ProviderTransportError as failure:
                    raise failure from error
        else:
            payload = parsed_legacy_payload

        receipt.update(
            status="succeeded",
            response_finished_at=clock(),
            json_parse_ok=True,
            zero_body=not entity,
        )
        return JsonTransportResult(
            status=status,
            payload=payload,
            encoded_body=encoded,
            entity_body=entity,
            receipt=receipt,
        )
    finally:
        close = getattr(response, "close", None)
        if callable(close):
            try:
                close()
            except Exception as error:
                # EOF/framing were already decided above.  A best-effort close
                # failure must not erase a complete result or its typed failure.
                receipt["response_close_error"] = type(error).__name__
