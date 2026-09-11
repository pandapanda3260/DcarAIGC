"""One explicitly approved profile operation on the verified installed release.

The installed source verifier reads the private approval and validates its full
source/build/database bindings before returning ``profile_operation_authority``.
This read-only adapter validates the resulting evidence and supplies a separate
operator decision. It neither changes historical cleanup authority nor opens a
gate, purchases a sample, or claims transport/business qualification.
"""
from __future__ import annotations

from pathlib import Path
from typing import Any, Mapping

from . import capture_authorizations as auth
from .source_routing import parse_time

EVIDENCE_KEY = "profile_operation_authority"
PROOF_CONTRACT = "account-profile-operator-authority-v1"
AUTHORIZATION_CONTRACT = "account-profile-user-authorization-v1"
DECISION_CONTRACT = "account-profile-operator-decision-v1"
OPERATIONS = frozenset({"douyin_uid_profile"})
ACTIVE_KEYS = ("activation_id", "profile_id", "roster_snapshot_id", "roster_members_sha256", "activation_sha256")
RUNTIME_KEYS = ("build_sha256", "runtime_sha256", "config_sha256")


def _require(value: bool, message: str) -> None:
    if not value:
        raise auth.AuthorizationError("Account profile authority: " + message)


def _sha(value: Any) -> bool:
    return (isinstance(value, str) and len(value) == 64
            and all(character in "0123456789abcdef" for character in value))


def _reference(value: Any) -> bool:
    return (isinstance(value, dict) and set(value) in ({"path", "sha256"}, {"path", "sha256", "byte_size"})
            and isinstance(value["path"], str) and Path(value["path"]).is_absolute()
            and _sha(value["sha256"])
            and ("byte_size" not in value or (type(value["byte_size"]) is int and value["byte_size"] > 0)))


def _verified_proof(evidence: Mapping[str, Any], at: str) -> dict[str, Any] | None:
    proof = evidence.get(EVIDENCE_KEY)
    if proof is None:
        return None
    _require(isinstance(proof, dict), "installed proof is not an object")
    assert isinstance(proof, dict)
    _require(proof.get("contract") == PROOF_CONTRACT
             and proof.get("operations") == sorted(OPERATIONS)
             and proof.get("proof_sha256") == auth.digest({key: value for key, value in proof.items()
                                                          if key != "proof_sha256"}),
             "installed proof changed or expanded the operation scope")
    approval = proof.get("authorization_payload")
    _require(isinstance(approval, dict), "explicit user approval is absent")
    assert isinstance(approval, dict)
    _require(approval.get("contract") == AUTHORIZATION_CONTRACT
             and approval.get("operations") == sorted(OPERATIONS)
             and approval.get("production_rollout") == "approved_by_user"
             and approval.get("business_e2e") == "required"
             and approval.get("transport_qualification") == "not_verified",
             "approval scope is invalid or overclaims qualification")
    _require(all(isinstance(approval.get(key), str) and approval[key].strip()
                 for key in ("actor", "reason", "user_instruction", "source_thread_id", "issued_at")),
             "approval provenance is incomplete")
    _require(all(_reference(proof.get(key)) for key in
                 ("authorization", "loaded_build", "parent_build", "source_tree", "runtime_root_receipt"))
             and approval.get("parent_build") == proof["parent_build"]
             and approval.get("source_tree") == proof["source_tree"]
             and proof["loaded_build"] != proof["parent_build"],
             "approval is not bound to the distinct installed source successor")
    try:
        time_valid = (parse_time(approval["issued_at"]) <= parse_time(proof["issued_at"])
                      <= parse_time(at))
    except (KeyError, TypeError, ValueError) as error:
        raise auth.AuthorizationError("Account profile authority: invalid approval time") from error
    _require(time_valid, "approval or installed proof is future-dated")
    active = evidence.get("active", {})
    _require(evidence.get("deployment", {}).get("status") == "accepted"
             and active.get("profile_id") == "integrated_route_v1"
             and all(key in active for key in ACTIVE_KEYS)
             and all(_sha(evidence.get(key)) for key in RUNTIME_KEYS),
             "current installed activation is incomplete")
    _require(proof["runtime_root_receipt"]["sha256"] == evidence["runtime_sha256"]
             and proof.get("config_sha256") == evidence["config_sha256"]
             and proof.get("transport_manifest") == evidence.get("manifest"),
             "runtime root, configuration or transport changed")
    policy = evidence.get("catalog_capture_policy")
    catalog_proof = evidence.get("catalog_capture_proof")
    _require(isinstance(policy, dict) and isinstance(catalog_proof, dict)
             and catalog_proof.get("proof_sha256") == auth.digest({key: value for key, value in catalog_proof.items()
                                                                 if key != "proof_sha256"})
             and proof.get("inherited_authority_proof_sha256") == catalog_proof["proof_sha256"]
             and approval.get("catalog_policy_sha256") == proof.get("catalog_policy_sha256")
             == evidence.get("catalog_capture_policy_sha256") == auth.digest(policy),
             "approved account-directory policy or its inherited proof changed")
    install = evidence.get("install", {})
    installed_database = install.get("installed", {})
    database = approval.get("formal_database")
    _require(isinstance(database, dict) and set(database) == {"path", "device", "inode"}
             and isinstance(database.get("path"), str) and Path(database["path"]).is_absolute()
             and type(database.get("device")) is int and type(database.get("inode")) is int
             and database == {"path": install.get("formal_database"),
                              "device": installed_database.get("device"),
                              "inode": installed_database.get("inode")},
             "approval belongs to another installed database")
    return proof


def decision(evidence: Mapping[str, Any], operation: str, at: str) -> dict[str, Any] | None:
    """Return new profile authority only; the four original decisions stay intact.

Gate runtime bindings retain the existing installed RELEASE identity. The new
decision digest additionally binds the separately approved current build and
source tree, so no historical four-operation approval can authorize this call.
"""
    if operation not in OPERATIONS:
        return None
    proof = _verified_proof(evidence, at)
    if proof is None:
        return None
    approval = proof["authorization_payload"]
    value = {"contract_version": DECISION_CONTRACT,
             "production_rollout": "approved_by_user", "business_e2e": "required",
             "transport_qualification": "not_verified", "qualification": "operator_authorized",
             "approved_target_profile": "integrated_route_v1", "operations": sorted(OPERATIONS),
             "bindings": {key: evidence["active"][key] for key in ACTIVE_KEYS},
             "runtime_bindings": {key: evidence[key] for key in RUNTIME_KEYS},
             "transport_manifest": proof["transport_manifest"],
             "profile_operation_authority_proof_sha256": proof["proof_sha256"],
             "authorization": proof["authorization"], "loaded_build": proof["loaded_build"],
             "source_tree": proof["source_tree"], "catalog_policy_sha256": proof["catalog_policy_sha256"],
             "actor": approval["actor"], "reason": approval["reason"], "issued_at": approval["issued_at"]}
    return {**value, "decision_sha256": auth.digest(value)}
