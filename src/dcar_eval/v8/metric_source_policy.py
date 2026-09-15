"""Immutable field-source contracts; selection never grants provider permission."""
from __future__ import annotations

import hashlib
import json
from functools import lru_cache
from pathlib import Path
from typing import Any

OPERATION_FIELD_POLICY_VERSION = "source-routing-operation-field-v3"
CURRENT_METRIC_POLICY = "source-routing-operation-field-v4"
OPERATION_FIELD_POLICY_VERSIONS = frozenset({OPERATION_FIELD_POLICY_VERSION, CURRENT_METRIC_POLICY})
_POLICY_FILE = Path(__file__).resolve().parents[3] / "config" / "source_routing_operation_field_v3.json"


@lru_cache(maxsize=2)
def _policy(policy_version: str = OPERATION_FIELD_POLICY_VERSION) -> dict[str, Any]:
    if policy_version not in OPERATION_FIELD_POLICY_VERSIONS:
        raise ValueError("unsupported operation-field policy")
    path = _POLICY_FILE if policy_version == OPERATION_FIELD_POLICY_VERSION else _POLICY_FILE.with_name("source_routing_operation_field_v4.json")
    value = json.loads(path.read_text(encoding="utf-8"))
    if value.get("policy_version") != policy_version:
        raise ValueError("unsupported operation-field policy")
    return value


def load_operation_field_policy(*, policy_version: str = OPERATION_FIELD_POLICY_VERSION) -> dict[str, Any]:
    """Return the complete policy as a mutation-safe, freezeable value."""
    return json.loads(json.dumps(_policy(policy_version)))


def field_eligibility(platform: str, provider: str, operation: str, field: str,
                      *, policy_version: str = OPERATION_FIELD_POLICY_VERSION) -> tuple[str, int]:
    """A database transition may restrict this table, never expand eligibility."""
    for rule in _policy(policy_version)["operation_rules"]:
        if (rule["platform"], rule["provider"], rule["operation"]) != (platform, provider, operation):
            continue
        if field in rule["active_fields"]:
            return "active", int(rule["priority"])
        if field in rule["audit_only_fields"]:
            return "audit_only", int(rule["priority"])
    return "historical_only", 2


def field_capability(platform: str, field: str, *, policy_version: str = CURRENT_METRIC_POLICY) -> dict[str, Any]:
    if policy_version != CURRENT_METRIC_POLICY:
        return {"status": "not_applicable" if platform == "xiaohongshu" and field == "view_count" else "supported",
                "auto_collectable": not (platform == "xiaohongshu" and field == "view_count"), "reason": ""}
    value = _policy(policy_version)["field_capabilities"].get(platform, {}).get(field)
    if value is None:
        raise ValueError("unconfigured platform metric field")
    return dict(value)


def auto_collectable_fields(platform: str, *, policy_version: str = CURRENT_METRIC_POLICY) -> tuple[str, ...]:
    fields = ("view_count", "comment_count", "like_count", "share_count", "collect_count")
    return tuple(field for field in fields if field_capability(platform, field, policy_version=policy_version)["auto_collectable"])


def current_policy_binding() -> dict[str, Any]:
    policy = load_operation_field_policy(policy_version=CURRENT_METRIC_POLICY)
    digest = hashlib.sha256(json.dumps(policy, ensure_ascii=False, sort_keys=True, separators=(",", ":"), allow_nan=False).encode()).hexdigest()
    return {"policy_version": CURRENT_METRIC_POLICY, "policy_sha256": digest,
            "field_capabilities": policy["field_capabilities"]}
