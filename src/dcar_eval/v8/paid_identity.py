"""Canonical identities for paid provider requests.

The request identity describes what the provider is asked to do.  Runtime
choices such as route, HTTP stack, activation, worker and retry attempt are
deliberately excluded so changing one of them cannot authorize a second
purchase of the same logical request.
"""

from __future__ import annotations

import hashlib
import json
import re
from dataclasses import dataclass
from datetime import datetime, timezone
from typing import Any, Mapping


CONTRACT_VERSION = "paid-scope-identity-v1"
_SENSITIVE_KEY = re.compile(
    r"(?:authorization|api[_-]?key|access[_-]?token|cookie|password|secret)",
    re.IGNORECASE,
)
_DOCUMENT_KEYS = frozenset(
    {
        "contract_version",
        "provider",
        "operation",
        "platform",
        "subject",
        "request_parameters",
        "cursor",
        "request_window",
        "due_bucket",
    }
)


class PaidIdentityError(ValueError):
    """The provider request cannot be represented by the identity contract."""


@dataclass(frozen=True)
class PaidRequestIdentity:
    """A request-level identity plus its compensation execution identity."""

    scope_identity: str
    execution_identity: str
    sequence: int
    document: dict[str, Any]


def _canonical_bytes(value: Any) -> bytes:
    try:
        return json.dumps(
            value,
            ensure_ascii=False,
            sort_keys=True,
            separators=(",", ":"),
            allow_nan=False,
        ).encode("utf-8")
    except (TypeError, ValueError) as error:
        raise PaidIdentityError("paid identity contains a non-JSON value") from error


def _plain_json(value: Any) -> Any:
    """Freeze mappings/sequences into JSON-native values without aliases."""

    return json.loads(_canonical_bytes(value).decode("utf-8"))


def _assert_no_secrets(value: Any, path: str = "request_parameters") -> None:
    if isinstance(value, Mapping):
        for raw_key, child in value.items():
            key = str(raw_key)
            if _SENSITIVE_KEY.search(key):
                raise PaidIdentityError(
                    f"secret-bearing key is forbidden in {path}: {key}"
                )
            _assert_no_secrets(child, f"{path}.{key}")
    elif isinstance(value, (list, tuple)):
        for index, child in enumerate(value):
            _assert_no_secrets(child, f"{path}[{index}]")


def _utc_second(value: str) -> str:
    try:
        parsed = datetime.fromisoformat(value.replace("Z", "+00:00"))
    except (TypeError, ValueError, AttributeError) as error:
        raise PaidIdentityError(
            "request window values must be ISO timestamps"
        ) from error
    if parsed.tzinfo is None:
        raise PaidIdentityError("request window values must include a timezone")
    return (
        parsed.astimezone(timezone.utc)
        .replace(microsecond=0)
        .isoformat()
        .replace("+00:00", "Z")
    )


def build_paid_request_identity(
    *,
    provider: str,
    operation: str,
    platform: str,
    subject: str,
    request_parameters: Mapping[str, Any],
    cursor: Any,
    due_bucket: str,
    request_window: Mapping[str, str] | None = None,
    sequence: int = 0,
) -> PaidRequestIdentity:
    """Build the immutable request and execution identities.

    ``sequence=0`` is the only normal execution.  Values 1--4 are reserved for
    explicitly authorized compensation and remain part of execution identity,
    never request identity.
    """

    provider_value = provider.strip().lower()
    operation_value = operation.strip()
    platform_value = platform.strip().lower()
    subject_value = subject.strip()
    due_value = due_bucket.strip()
    if not all(
        (provider_value, operation_value, platform_value, subject_value, due_value)
    ):
        raise PaidIdentityError("paid identity fields must be nonempty")
    if type(sequence) is not int or not 0 <= sequence <= 4:
        raise PaidIdentityError(
            "paid execution sequence must be an integer from 0 to 4"
        )
    _assert_no_secrets(request_parameters)
    _assert_no_secrets(cursor, "cursor")
    frozen_window: dict[str, str] | None = None
    if request_window is not None:
        allowed = {"start", "end"}
        if not request_window or set(request_window) - allowed:
            raise PaidIdentityError("request window only accepts start and end")
        frozen_window = {
            key: _utc_second(str(value))
            for key, value in sorted(request_window.items())
        }
        if (
            frozen_window.get("start") is not None
            and frozen_window.get("end") is not None
            and frozen_window["start"] >= frozen_window["end"]
        ):
            raise PaidIdentityError("request window must be a nonempty half-open range")
    document = {
        "contract_version": CONTRACT_VERSION,
        "provider": provider_value,
        "operation": operation_value,
        "platform": platform_value,
        "subject": subject_value,
        "request_parameters": _plain_json(dict(request_parameters)),
        "cursor": _plain_json(cursor),
        "request_window": frozen_window,
        "due_bucket": due_value,
    }
    scope_identity = hashlib.sha256(_canonical_bytes(document)).hexdigest()
    execution_identity = hashlib.sha256(
        _canonical_bytes(
            {
                "contract_version": CONTRACT_VERSION,
                "scope_identity": scope_identity,
                "sequence": sequence,
            }
        )
    ).hexdigest()
    return PaidRequestIdentity(
        scope_identity=scope_identity,
        execution_identity=execution_identity,
        sequence=sequence,
        document=document,
    )


def validate_paid_request_identity(
    identity: PaidRequestIdentity,
    *,
    provider: str,
    operation: str,
    platform: str,
    subject: str,
    due_bucket: str,
) -> PaidRequestIdentity:
    """Rebuild an untrusted identity at the paid-claim boundary."""

    if not isinstance(identity, PaidRequestIdentity):
        raise PaidIdentityError("paid identity has an invalid value type")
    document = identity.document
    if not isinstance(document, Mapping) or frozenset(document) != _DOCUMENT_KEYS:
        raise PaidIdentityError("paid identity document shape is invalid")
    parameters = document.get("request_parameters")
    window = document.get("request_window")
    if not isinstance(parameters, Mapping):
        raise PaidIdentityError("paid identity request parameters are invalid")
    if window is not None and not isinstance(window, Mapping):
        raise PaidIdentityError("paid identity request window is invalid")
    rebuilt = build_paid_request_identity(
        provider=str(document.get("provider") or ""),
        operation=str(document.get("operation") or ""),
        platform=str(document.get("platform") or ""),
        subject=str(document.get("subject") or ""),
        request_parameters=parameters,
        cursor=document.get("cursor"),
        due_bucket=str(document.get("due_bucket") or ""),
        request_window=window,
        sequence=identity.sequence,
    )
    if (
        dict(document) != rebuilt.document
        or identity.scope_identity != rebuilt.scope_identity
        or identity.execution_identity != rebuilt.execution_identity
    ):
        raise PaidIdentityError("paid identity hashes do not match its document")
    expected = {
        "provider": provider.strip().lower(),
        "operation": operation.strip(),
        "platform": platform.strip().lower(),
        "subject": subject.strip(),
        "due_bucket": due_bucket.strip(),
    }
    if any(rebuilt.document[key] != value for key, value in expected.items()):
        raise PaidIdentityError("paid identity does not match the frozen request target")
    return rebuilt
