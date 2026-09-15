"""Exact local capture authorization carried by the verified schema22 build.

This is operator authorization, not a fabricated transport qualification. The
ordinary Writer issuer, budget, paid scope and response checks remain mandatory.
"""
from __future__ import annotations

from datetime import datetime, timezone
import hashlib
import json
from typing import Any, Mapping

EVIDENCE_KEY = "preparation_operation_authority"
AUTHORIZATION_CONTRACT = "account-preparation-user-authorization-v1"
PROOF_CONTRACT = "account-preparation-operator-authority-v1"
DECISION_CONTRACT = "account-preparation-operator-decision-v1"
PLATFORMS = ("douyin", "kuaishou", "wechat_channels", "xiaohongshu")
STATUSES = ("daily", "paused", "unmarked", "weekly")
OPERATIONS = frozenset({
    "douyin_uid_profile", "douyin_sec_profile", "douyin_display_profile",
    "xiaohongshu_user_profile", "xiaohongshu_user_search", "kuaishou_user_profile",
    "wechat_channels_resolve", "wechat_channels_channel_info", "wechat_channels_user_profile",
    "douyin_user_posts", "douyin_video_detail", "douyin_video_statistics", "douyin_video_comments",
    "xiaohongshu_user_posts", "xiaohongshu_note_detail", "xiaohongshu_note_statistics", "xiaohongshu_note_comments",
    "kuaishou_user_posts", "kuaishou_video_detail", "kuaishou_video_statistics",
    "wechat_channels_user_posts", "wechat_channels_video_detail", "wechat_channels_video_statistics",
})
ACTIVE_KEYS = ("activation_id", "profile_id", "roster_snapshot_id", "roster_members_sha256", "activation_sha256")


def _require(condition: bool, message: str) -> None:
    if not condition:
        raise ValueError("Account preparation authority: " + message)


def _digest(value: Any) -> str:
    return hashlib.sha256(json.dumps(value, ensure_ascii=False, sort_keys=True, separators=(',', ':'), allow_nan=False).encode()).hexdigest()


def parse_time(value: str) -> datetime:
    result = datetime.fromisoformat(value.replace('Z', '+00:00'))
    _require(result.utcoffset() is not None, 'authorization time must include its timezone')
    return result.astimezone(timezone.utc)


def validate_authorization(value: Mapping[str, Any], *, parent_build: Mapping[str, Any],
                           source_tree: Mapping[str, Any], migration: Mapping[str, Any],
                           database: Mapping[str, Any], policy_sha256: str, at: str) -> None:
    """Validate a real user instruction and the exact newly checked local scope."""
    _require(isinstance(value, dict) and value.get("contract") == AUTHORIZATION_CONTRACT
             and value.get("scope") == "local_writer_capture_only" and value.get("schema_version") == 22
             and value.get("operations") == sorted(OPERATIONS) and value.get("platforms") == list(PLATFORMS)
             and value.get("manual_statuses") == list(STATUSES)
             and value.get("qualification") == "operator_authorized"
             and value.get("business_e2e") == "required" and value.get("transport_qualification") == "not_verified"
             and value.get("publisher_authorized") is False and value.get("remote_database_authorized") is False,
             "local operation scope changed or claims unproved qualification")
    _require(value.get("parent_build") == dict(parent_build) and value.get("source_tree") == dict(source_tree)
             and value.get("migration") == dict(migration) and value.get("formal_database") == dict(database)
             and value.get("catalog_policy_sha256") == policy_sha256,
             "authorization source, migration, database or policy differs")
    _require(all(isinstance(value.get(key), str) and value[key].strip()
                 for key in ("actor", "reason", "user_instruction", "source_thread_id", "issued_at")),
             "user authorization provenance is incomplete")
    _require(parse_time(value["issued_at"]) <= parse_time(at), "authorization is future-dated")
    active = value.get("activation", {})
    _require(isinstance(active, dict) and set(active) == set(ACTIVE_KEYS)
             and active.get("profile_id") == "integrated_route_v1", "authorization activation is incomplete")


def _decision(evidence: Mapping[str, Any], operation: str, at: str) -> dict[str, Any] | None:
    """Supply exact new authority; absent schema22 proof never falls through as it."""
    if evidence.get("four_platform_flow_successor") is not None:
        from .four_platform_flow_authority import decision
        return decision(evidence, operation, at)
    if operation not in OPERATIONS:
        return None
    proof = evidence.get(EVIDENCE_KEY)
    if proof is None:
        return None
    _require(isinstance(proof, dict) and proof.get("contract") == PROOF_CONTRACT
             and proof.get("proof_sha256") == _digest({k: v for k, v in proof.items() if k != "proof_sha256"}),
             "installed proof changed")
    intake = evidence.get("account_intake_successor", {})
    _require(intake.get("contract") == "account-intake-schema-successor-v1"
             and intake.get("proof_sha256") == _digest({k: v for k, v in intake.items() if k != "proof_sha256"})
             and proof.get("intake_proof_sha256") == intake.get("proof_sha256")
             and proof.get("loaded_build") == intake.get("loaded_build")
             and proof.get("source_tree") == intake.get("source_tree")
             and proof.get("operations") == sorted(OPERATIONS), "installed schema22 scope differs")
    approval = proof.get("authorization_payload", {})
    install = evidence.get("install", {})
    installed_db = install.get("installed", {})
    database = {"path": install.get("formal_database"), "device": installed_db.get("device"), "inode": installed_db.get("inode")}
    validate_authorization(approval, parent_build=intake["parent_build"], source_tree=intake["source_tree"],
                           migration=intake["migration"], database=database,
                           policy_sha256=evidence.get("catalog_capture_policy_sha256"), at=at)
    active = evidence.get("active", {})
    _require({key: active.get(key) for key in ACTIVE_KEYS} == approval["activation"]
             and evidence.get("deployment", {}).get("status") == "accepted"
             and proof.get("transport_manifest") == evidence.get("manifest")
             and proof.get("runtime_root_receipt", {}).get("sha256") == evidence.get("runtime_sha256")
             and _digest(evidence.get("catalog_capture_policy")) == evidence.get("catalog_capture_policy_sha256")
             == intake.get("account_catalog_policy_sha256")
             and evidence.get("catalog_capture_proof") == intake,
             "live activation, transport or catalog proof changed")
    value = {"contract_version": DECISION_CONTRACT, "local_activation": "approved_by_user",
             "production_rollout": "not_authorized",
             "scope": "local_writer_capture_only", "business_e2e": "required", "transport_qualification": "not_verified",
             "qualification": "operator_authorized", "operations": sorted(OPERATIONS),
             "bindings": approval["activation"], "authorization": proof["authorization"],
             "loaded_build": proof["loaded_build"], "source_tree": proof["source_tree"],
             "preparation_authority_proof_sha256": proof["proof_sha256"],
             "actor": approval["actor"], "reason": approval["reason"], "issued_at": approval["issued_at"]}
    return {**value, "decision_sha256": _digest(value)}


def decision(evidence: Mapping[str, Any], operation: str, at: str) -> dict[str, Any] | None:
    # The source bootstrap imports validation above before application modules.
    # Paid dispatch uses the existing typed authority error after bootstrap.
    from .capture_authorizations import AuthorizationError
    try:
        return _decision(evidence, operation, at)
    except (ValueError, KeyError, TypeError) as error:
        raise AuthorizationError(str(error)) from error
