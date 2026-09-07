"""The small consumer contract shared by builder, publisher, installer and API.

Runtime identity v1 keeps its ten database/report fields.  File transport and
retention are bound separately so an old identity cannot imply v2 support.
"""
from __future__ import annotations

from typing import Any

from .media_policy import load_media_policy

ARTIFACT_POLICY: dict[str, Any] = {
    "name": "thin-server-v2",
    "included": "reports-and-small-text-evidence",
    "optional_reuse": "active-same-path-size-sha256-only",
    "on_optional_missing_or_mismatch": "omitted",
    "delete_unlisted": False,
}
MANAGED_ORIGINALS_CONTRACT = "managed-originals-v1"


def descriptor() -> dict[str, str]:
    policy = load_media_policy()
    return {
        "artifact_policy": ARTIFACT_POLICY["name"],
        "media_retention_policy": policy["contract_version"],
        "media_retention_sha256": policy["sha256"],
    }


def validate_descriptor(value: Any) -> dict[str, str]:
    expected = descriptor()
    if not isinstance(value, dict) or value != expected:
        raise ValueError("snapshot_consumer_contract_mismatch")
    return expected
