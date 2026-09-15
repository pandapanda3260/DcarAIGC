"""Explicit local schema23 authorization, separate from historical schema22."""
from __future__ import annotations

from typing import Any, Mapping
from .account_classification_release import digest
from .account_preparation_authority import ACTIVE_KEYS, OPERATIONS as LEGACY_OPERATIONS, PLATFORMS, STATUSES, parse_time
from .metric_source_policy import current_policy_binding

AUTHORIZATION_CONTRACT = "four-platform-flow-user-authorization-v1"
DECISION_CONTRACT = "four-platform-flow-operator-decision-v1"
OPERATIONS = LEGACY_OPERATIONS | frozenset({"wechat_channels_video_comments"})


def require(condition: bool, reason: str) -> None:
    if not condition:
        raise ValueError("four platform flow authority: " + reason)


def validate_authorization(value: Mapping[str, Any], *, parent_build: Mapping[str, Any],
                           source_tree: Mapping[str, Any], migration: Mapping[str, Any],
                           database: Mapping[str, Any], catalog_policy_sha256: str, at: str) -> None:
    require(value.get("contract") == AUTHORIZATION_CONTRACT and value.get("schema_version") == 23
        and value.get("scope") == "local_writer_forward_flow_only" and value.get("historical_backfill_authorized") is False
        and value.get("operations") == sorted(OPERATIONS) and value.get("platforms") == list(PLATFORMS)
        and value.get("manual_statuses") == list(STATUSES) and value.get("qualification") == "operator_authorized"
        and value.get("business_e2e") == "required" and value.get("transport_qualification") == "not_verified"
        and value.get("publisher_authorized") is False and value.get("remote_database_authorized") is False,
        "authorization expands the approved forward local scope")
    require(value.get("parent_build") == dict(parent_build) and value.get("source_tree") == dict(source_tree)
        and value.get("migration") == dict(migration) and value.get("formal_database") == dict(database)
        and value.get("catalog_policy_sha256") == catalog_policy_sha256
        and value.get("metric_policy") == current_policy_binding(), "authorization source, migration or metric policy differs")
    require(all(isinstance(value.get(key), str) and value[key].strip() for key in
        ("actor", "reason", "user_instruction", "source_thread_id", "issued_at")), "real user instruction provenance required")
    require(parse_time(value["issued_at"]) <= parse_time(at), "authorization is future-dated")
    active = value.get("activation", {})
    require(isinstance(active, dict) and set(active) == set(ACTIVE_KEYS)
        and active.get("profile_id") == "integrated_route_v1", "authorization activation is incomplete")


def decision(evidence: Mapping[str, Any], operation: str, at: str) -> dict[str, Any] | None:
    if operation not in OPERATIONS:
        return None
    proof = evidence.get("four_platform_flow_successor", {})
    require(proof.get("contract") == "four-platform-flow-schema-successor-v1"
        and proof.get("proof_sha256") == digest({k:v for k,v in proof.items() if k != "proof_sha256"}),
        "installed schema23 proof differs")
    install = evidence.get("install", {})
    identity = install.get("installed", {})
    approval = proof.get("authorization_payload", {})
    validate_authorization(approval, parent_build=proof["parent_build"], source_tree=proof["source_tree"],
        migration=proof["migration"], database={"path":install.get("formal_database"),
            "device":identity.get("device"), "inode":identity.get("inode")},
        catalog_policy_sha256=evidence.get("catalog_capture_policy_sha256"), at=at)
    intake = evidence.get("account_intake_successor", {})
    require(proof.get("parent_intake_proof_sha256") == intake.get("proof_sha256")
        and evidence.get("catalog_capture_proof") == intake
        and digest(evidence.get("catalog_capture_policy")) == approval["catalog_policy_sha256"]
        and {key:evidence.get("active", {}).get(key) for key in ACTIVE_KEYS} == approval["activation"]
        and evidence.get("deployment", {}).get("status") == "accepted", "live installed authority differs")
    value = {"contract_version":DECISION_CONTRACT, "local_activation":"approved_by_user",
        "production_rollout":"not_authorized", "scope":approval["scope"], "business_e2e":"required",
        "transport_qualification":"not_verified", "qualification":"operator_authorized",
        "operations":sorted(OPERATIONS), "bindings":approval["activation"],
        "authorization":proof["operation_authorization"], "loaded_build":proof["loaded_build"],
        "source_tree":proof["source_tree"], "four_platform_flow_proof_sha256":proof["proof_sha256"],
        "actor":approval["actor"], "reason":approval["reason"], "issued_at":approval["issued_at"]}
    return {**value, "decision_sha256":digest(value)}
