"""Portable, read-only publication evidence for an approved account cleanup.

A candidate publication receipt grants no Writer, provider, E2E or coverage
qualification. The independent cleanup operator contract controls paid capture.
"""
from __future__ import annotations

import hashlib
import json
from pathlib import Path
import re
import sqlite3
from typing import Any, Mapping

from .capture_authorizations import digest
from .profile_activations import activation_by_id

CONTRACT = "account-cleanup-readonly-deployment-v1"
EVIDENCE_ROLES = ("cleanup_migration", "source_authority", "cleanup_build", "cleanup_runtime", "cleanup_prepared")
ACTIVE_KEYS = ("activation_id", "profile_id", "activation_sha256", "roster_snapshot_id", "roster_members_sha256")
HASH = re.compile(r"[0-9a-f]{64}\Z")


def require(condition: bool, message: str) -> None:
    if not condition:
        raise ValueError(message)


def reference(path: Path) -> dict[str, Any]:
    require(path.is_absolute() and not path.is_symlink() and path.resolve(strict=True) == path, "Cleanup proof path must be canonical")
    body = path.read_bytes()
    require(path.stat().st_nlink == 1 and len(body) <= 16 * 1024 * 1024, "Cleanup proof must be a small ordinary JSON file")
    return {"path": str(path), "sha256": hashlib.sha256(body).hexdigest(), "byte_size": len(body)}


def _objects(evidence: Mapping[str, Any], project_root: Path | None) -> dict[str, Any]:
    require(set(evidence) == set(EVIDENCE_ROLES), "Cleanup proof roles differ")
    result = {}
    for role, ref in evidence.items():
        path = Path(ref["path"])
        require(project_root is None or not path.is_relative_to(project_root.resolve()), "Cleanup evidence must be private and external")
        require(reference(path) == dict(ref), "Cleanup evidence file changed")
        value = json.loads(path.read_text())
        require(isinstance(value, dict), "Cleanup evidence must be an object")
        result[role] = value
    return result


def _unsigned(value: Mapping[str, Any], key: str) -> None:
    require(value.get(key) == digest({k: v for k, v in value.items() if k != key}), "Cleanup evidence digest differs")


def _ranges(ids: list[int]) -> list[list[int]]:
    result = []
    for value in sorted(set(ids)):
        if result and value == result[-1][1] + 1:
            result[-1][1] = value
        else:
            result.append([value, value])
    return result


def _directory(connection: sqlite3.Connection, attachment: str) -> list[list[Any]]:
    return [list(row) for row in connection.execute(
        "SELECT source_row,account_id,platform,uid FROM account_directory_rows WHERE source_sha256=? ORDER BY source_row", (attachment,))]


def _contents(connection: sqlite3.Connection, maximum: int) -> list[list[Any]]:
    return [list(row) for row in connection.execute(
        "SELECT id,account_id,platform,platform_content_id FROM content_items WHERE id<=? ORDER BY id", (maximum,))]


def _selection(connection: sqlite3.Connection, snapshot: int) -> list[dict[str, Any]]:
    return [dict(zip(("account_identity_id", "account_id", "platform", "uid"), row)) for row in connection.execute(
        "SELECT i.id,i.account_id,i.platform,i.uid FROM account_roster_members m "
        "JOIN account_platform_identities i ON i.id=m.account_identity_id WHERE m.snapshot_id=? ORDER BY i.id", (snapshot,))]


def _lineage(objects: Mapping[str, Any], refs: Mapping[str, Any], payload: Mapping[str, Any]) -> None:
    migration, source, build_envelope, runtime_envelope, prepared = (objects[k] for k in EVIDENCE_ROLES)
    _unsigned(migration, "receipt_sha256")
    _unsigned(source, "snapshot_sha256")
    _unsigned(prepared, "receipt_sha256")
    for envelope, contract in ((build_envelope, "sealed-build-receipt-v1"), (runtime_envelope, "runtime-root-binding-v1")):
        require(envelope.get("contract_version") == contract and envelope.get("payload_sha256") == digest(envelope["payload"]), "Cleanup build/runtime envelope differs")
    build, runtime = build_envelope["payload"], runtime_envelope["payload"]
    generation = build["account_cleanup_generation"]
    require(migration.get("contract") == "account-cleanup-projection-v1" and migration.get("status") == "candidate_verified", "Cleanup migration is not verified")
    require(source.get("contract") == "account-cleanup-source-authority-v1" and prepared.get("contract") == "account-cleanup-runtime-preparation-v1", "Cleanup preparation contract differs")
    require(source["source_database_sha256"] == migration["source_backup"]["sha256"] == prepared["source_sha256"] == generation["source_database_sha256"], "Cleanup source database differs")
    require(generation["migration_receipt"] == refs["cleanup_migration"] and generation["source_authority"] == refs["source_authority"]
            and build["runtime_root_receipt"] == refs["cleanup_runtime"], "Cleanup build evidence lineage differs")
    require(prepared["source_authority_sha256"] == refs["source_authority"]["sha256"]
            and prepared["build_receipt_sha256"] == refs["cleanup_build"]["sha256"]
            and prepared["runtime_root_receipt_sha256"] == refs["cleanup_runtime"]["sha256"], "Cleanup prepared file bindings differ")
    require(prepared["active"] == {k: payload["bindings"][k] for k in ACTIVE_KEYS}
            and prepared["selection_sha256"] == source["selection_sha256"] == generation["selection_sha256"] == payload["selection_sha256"]
            and prepared["generation_id"] == generation["generation_id"] == payload["generation_id"], "Cleanup prepared generation differs")
    require(prepared["paid_gates_issued"] is False and runtime["status"] == build["status"] == "succeeded"
            and runtime["binding_stage"] == "prepared_installation", "Cleanup read-only proof overclaims installation or capture")
    require(generation["config_sha256"] == payload["bindings"]["config_sha256"]
            and migration["source_attachment_sha256"] == payload["baseline"]["source_attachment_sha256"], "Cleanup attachment/config differs")
    require(len(migration["selection"]["removed_account_ids"]) == 175
            and len(migration["selection"]["removed_content_ids"]) == 18014
            and payload["baseline"]["removed_account_ids"] == migration["selection"]["removed_account_ids"]
            and payload["baseline"]["removed_content_ranges"] == _ranges(migration["selection"]["removed_content_ids"]), "Cleanup deletion projection differs")


def record_candidate(connection: sqlite3.Connection, *, evidence: Mapping[str, Any], deployment_id: str, recorded_at: str) -> dict[str, Any]:
    require(connection.in_transaction, "Cleanup publication recording requires a transaction")
    require(connection.execute("SELECT 1 FROM deployment_readiness_receipts LIMIT 1").fetchone() is None, "Cleanup publication generation must be empty")
    objects = _objects(evidence, None)
    migration, source, build, runtime, prepared = (objects[k] for k in EVIDENCE_ROLES)
    attachment = migration["source_attachment_sha256"]
    directory = _directory(connection, attachment)
    require(len(directory) == 292 and migration["directory"]["matched_count"] == 168
            and migration["directory"]["created_count"] == 98 and migration["directory"]["unresolved_count"] == 26, "Cleanup directory baseline differs")
    maximum = max(connection.execute("SELECT COALESCE(MAX(id),0) FROM content_items").fetchone()[0], max(migration["selection"]["removed_content_ids"]))
    contents = _contents(connection, maximum)
    require(len(contents) == 63496 and sum(row[1] is None for row in contents) == 93, "Cleanup retained content baseline differs")
    verified_ids = [row[0] for row in connection.execute("SELECT account_id FROM account_directory_rows WHERE source_sha256=? AND identity_status='existing_verified' ORDER BY account_id", (attachment,))]
    require(len(verified_ids) == 168 and sum(row[1] in set(verified_ids) for row in contents) == 63403, "Cleanup confirmed historical scope differs")
    bindings = {**prepared["active"], "build_sha256": evidence["cleanup_build"]["sha256"],
                "runtime_sha256": evidence["cleanup_runtime"]["sha256"], "config_sha256": prepared["release_control"]["config_sha256"]}
    baseline = {"source_attachment_sha256": attachment, "directory_count": 292, "directory_sha256": digest(directory),
                "content_max_id": maximum, "content_count": 63496, "content_sha256": digest(contents),
                "confirmed_account_ids": verified_ids, "confirmed_content_count": 63403, "unassociated_content_count": 93,
                "removed_account_ids": migration["selection"]["removed_account_ids"],
                "removed_content_ranges": _ranges(migration["selection"]["removed_content_ids"])}
    payload = {"contract_version": CONTRACT, "schema_version": 20, "generation_id": prepared["generation_id"],
               "bindings": bindings, "selection_sha256": source["selection_sha256"], "member_count": prepared["member_count"],
               "evidence": dict(evidence), "baseline": baseline, "coverage_complete": False,
               "business_e2e": "deferred_by_user", "transport_qualification": "not_verified", "paid_authority": False}
    _lineage(objects, evidence, payload)
    envelope = {"deployment_id": deployment_id, "status": "candidate", "payload": payload, "recorded_at": recorded_at}
    receipt_sha = digest(envelope)
    connection.execute("INSERT INTO deployment_readiness_receipts(deployment_id,status,payload_json,recorded_at,receipt_sha256) VALUES (?,?,?,?,?)",
                       (deployment_id, "candidate", json.dumps(payload, ensure_ascii=False, sort_keys=True, separators=(",", ":")), recorded_at, receipt_sha))
    return validate(connection, deployment_id=deployment_id)


def is_cleanup(connection: sqlite3.Connection, deployment_id: str | None = None) -> bool:
    row = connection.execute("SELECT payload_json FROM deployment_readiness_receipts " + ("WHERE deployment_id=? " if deployment_id else "") + "ORDER BY id DESC LIMIT 1", (deployment_id,) if deployment_id else ()).fetchone()
    return row is not None and json.loads(row[0]).get("contract_version") == CONTRACT


def validate(connection: sqlite3.Connection, *, deployment_id: str | None = None, project_root: Path | None = None,
             verify_files: bool = True, expected_bindings: Mapping[str, Any] | None = None, require_accepted: bool = False) -> dict[str, Any]:
    require(not require_accepted, "Read-only cleanup candidate does not claim production capture acceptance")
    schema_version = connection.execute("PRAGMA user_version").fetchone()[0]
    classification = None
    if schema_version == 21:
        from .schema_v21 import migration_proof, validate_structure
        validate_structure(connection)
        classification = migration_proof(connection)
    else:
        require(schema_version == 20, "Cleanup publication requires schema20 or a proved schema21 migration")
    row = connection.execute("SELECT deployment_id,status,payload_json,recorded_at,receipt_sha256 FROM deployment_readiness_receipts " + ("WHERE deployment_id=? " if deployment_id else "") + "ORDER BY id DESC LIMIT 1", (deployment_id,) if deployment_id else ()).fetchone()
    require(row is not None, "Cleanup publication receipt missing")
    identifier, status, raw, recorded_at, receipt_sha = row
    payload = json.loads(raw)
    require(payload.get("contract_version") == CONTRACT and payload.get("schema_version") == 20 and status == "candidate", "Cleanup publication contract/status differs")
    require(receipt_sha == digest({"deployment_id": identifier, "status": status, "payload": payload, "recorded_at": recorded_at}), "Cleanup publication receipt digest differs")
    require(payload.get("coverage_complete") is False and payload.get("paid_authority") is False
            and payload.get("business_e2e") == "deferred_by_user" and payload.get("transport_qualification") == "not_verified", "Cleanup publication overclaims qualification")
    bindings = payload["bindings"]
    require(all(HASH.fullmatch(str(bindings.get(k))) for k in ("build_sha256", "runtime_sha256", "config_sha256", "activation_sha256", "roster_members_sha256")), "Cleanup binding hash invalid")
    require(all(bindings.get(k) == v for k, v in (expected_bindings or {}).items()), "Cleanup expected runtime binding differs")
    active = activation_by_id(connection, int(bindings["activation_id"]))
    require(all(active[k] == bindings[k] for k in ACTIVE_KEYS) and active.get("cancellation") is None
            and active["build_receipt_sha256"] == bindings["build_sha256"], "Cleanup activation differs")
    require(active["metadata"].get("account_cleanup") == {"contract": "account-cleanup-generation-v1", "generation_id": payload["generation_id"], "selection_sha256": payload["selection_sha256"]}, "Cleanup activation metadata differs")
    members = _selection(connection, bindings["roster_snapshot_id"])
    require(len(members) == payload["member_count"] == 104 and digest(members) == payload["selection_sha256"], "Cleanup roster selection differs")
    base = payload["baseline"]
    directory = _directory(connection, base["source_attachment_sha256"])
    require(len(directory) == base["directory_count"] == 292 and digest(directory) == base["directory_sha256"], "Cleanup imported directory changed")
    contents = _contents(connection, base["content_max_id"])
    require(len(contents) == base["content_count"] == 63496 and digest(contents) == base["content_sha256"], "Cleanup preserved content ownership changed")
    require(sum(row[1] is None for row in contents) == base["unassociated_content_count"] == 93
            and sum(row[1] in set(base["confirmed_account_ids"]) for row in contents) == base["confirmed_content_count"] == 63403, "Cleanup historical counts differ")
    require(len(base["removed_account_ids"]) == 175, "Cleanup removed subject scope differs")
    for account in base["removed_account_ids"]:
        require(connection.execute("SELECT 1 FROM accounts WHERE id=?", (account,)).fetchone() is None, "Removed cleanup subject returned")
    require(sum(end - start + 1 for start, end in base["removed_content_ranges"]) == 18014, "Cleanup removed content scope differs")
    for start, end in base["removed_content_ranges"]:
        require(connection.execute("SELECT 1 FROM content_items WHERE id BETWEEN ? AND ? LIMIT 1", (start, end)).fetchone() is None, "Removed cleanup content returned")
    require(set(payload["evidence"]) == set(EVIDENCE_ROLES), "Cleanup evidence roles differ")
    for ref in payload["evidence"].values():
        require(set(ref) == {"path", "sha256", "byte_size"} and Path(ref["path"]).is_absolute() and HASH.fullmatch(str(ref["sha256"])) and type(ref["byte_size"]) is int and 0 < ref["byte_size"] <= 16 * 1024 * 1024, "Cleanup portable private reference invalid")
    if verify_files:
        _lineage(_objects(payload["evidence"], project_root), payload["evidence"], payload)
    result = {"contract_version": CONTRACT, "deployment_id": identifier, "status": status, "receipt_sha256": receipt_sha,
            "validation_scope": "readonly_cleanup", "deployment_eligible": False, "readonly_publish_eligible": True,
            "coverage_complete": False, "bindings": bindings, "evidence": payload["evidence"],
            "release_decision": None, "storage_policy": None, "e2e_status": "deferred", "transport_qualification": "not_verified",
            "selection_sha256": payload["selection_sha256"], "baseline": base,
            "unmet_evidence": ["business_e2e_deferred_by_user", "transport_qualification_not_verified", "coverage_not_complete"]}
    if classification is not None:
        result.update(schema_version=21, schema_migration="account-classification-v1",
                      account_classification_migration=classification)
    return result
