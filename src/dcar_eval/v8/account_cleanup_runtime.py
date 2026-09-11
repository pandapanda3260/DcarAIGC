"""Explicit cleanup-generation authority; never a schema19 migration or sample qualification.

Private, sealed source snapshots carry prior operator authority without keeping
the old large database online. Provider admission still uses the ordinary
operator gate, Writer lease, paid-drain chain, budgets and paid-scope ledger.
"""
from __future__ import annotations

import hashlib
import json
import os
from pathlib import Path
import sqlite3
from typing import Any, Mapping

from . import account_roster, capture_authorizations as auth, forward_recovery, paid_drain, provider_budget
from .metric_field_facts import utc
from .profile_activations import activation_at
from .runtime_paths import _raw_file, source_root
from .source_routing import parse_time
from .storage import PROJECT_ROOT

GENERATION = "account-cleanup-generation-v1"
SOURCE = "account-cleanup-source-authority-v1"
DECISION = "account-cleanup-operator-decision-v1"
CONTROL = "account-cleanup-release-control-v1"
OPERATIONS = frozenset({"douyin_user_posts", "douyin_video_detail", "douyin_video_statistics", "douyin_video_comments"})
ACTIVE_KEYS = ("activation_id", "profile_id", "roster_snapshot_id", "roster_members_sha256", "activation_sha256")
MEMBER_KEYS = ("account_identity_id", "account_id", "platform", "uid")
GATE_KEYS = ("provider", "operation", "state", "reason", "evidence_json", "recorded_at")
READY_KEYS = ("provider", "operation", "status", "reason", "evidence_json", "created_at", "expires_at")


def require(value: bool, message: str) -> None:
    if not value:
        raise auth.AuthorizationError(message)


def private_object(reference: Mapping[str, Any]) -> dict[str, Any]:
    path = Path(reference["path"])
    require(not path.is_relative_to(PROJECT_ROOT.resolve()), "Cleanup receipt must be external")
    body = _raw_file(path, private=True)
    require(hashlib.sha256(body).hexdigest() == reference["sha256"], "Cleanup receipt SHA differs")
    value = json.loads(body)
    require(isinstance(value, dict), "Cleanup receipt must be an object")
    return value


def file_sha(path: Path) -> str:
    result = hashlib.sha256()
    with path.open("rb") as stream:
        for block in iter(lambda: stream.read(1024 * 1024), b""):
            result.update(block)
    return result.hexdigest()


def member_shape(row: Mapping[str, Any]) -> dict[str, Any]:
    return {key: row[key] for key in MEMBER_KEYS}


def valid_sec(value: str) -> bool:
    return value.startswith("MS4wLjAB") and 40 <= len(value) <= 128


def validate_source(value: Mapping[str, Any], *, at: str) -> None:
    require(value.get("contract") == SOURCE and value.get("snapshot_sha256") == auth.digest(
        {k: v for k, v in value.items() if k != "snapshot_sha256"}), "Cleanup source capsule changed")
    frozen = value["frozen_at"]
    require(parse_time(frozen) <= parse_time(at), "Cleanup source is future dated")
    decision = private_object(value["decision_receipt"])
    require(decision.get("contract_version") == "v25-user-release-decision-v1"
            and decision.get("production_rollout") == "approved_by_user"
            and decision.get("business_e2e") == "deferred_by_user"
            and decision.get("transport_qualification") == "not_verified"
            and decision.get("approved_target_profile") == "integrated_route_v1"
            and set(value["operations"]) == OPERATIONS <= set(decision["operations"])
            and value["transport_manifest"] == decision["transport_manifest"],
            "Cleanup source lacks original scoped operator approval")
    for operation, pair in value["operations"].items():
        gate, ready = pair["gate"], pair["readiness"]
        payload, evidence = json.loads(gate["evidence_json"]), json.loads(ready["evidence_json"])
        require(gate["event_sha256"] == auth.digest({k: gate[k] for k in GATE_KEYS})
                and ready["receipt_sha256"] == auth.digest({k: ready[k] for k in READY_KEYS})
                and gate["state"] == "open" and ready["status"] == "ready"
                and gate["provider"] == ready["provider"] == "tikhub"
                and gate["operation"] == ready["operation"] == payload["operation"] == operation
                and payload["contract"] == auth.CONTRACT
                and evidence["contract"] == auth.READINESS_CONTRACT
                and evidence["qualification"] == "operator_authorized"
                and payload["readiness_receipt_id"] == ready["id"]
                and payload["readiness_receipt_sha256"] == ready["receipt_sha256"]
                and payload["bindings"] == evidence["bindings"]
                and payload["scope_hash"] == evidence["scope_hash"] == auth.scope_hash(
                    runtime_bindings=payload["bindings"], provider="tikhub", operation=operation)
                and payload["release_decision_sha256"] == evidence["release_decision_sha256"] == value["decision_receipt"]["sha256"]
                and payload["release_event_id"] == evidence["release_event_id"] == value["source_release_event_id"]
                and payload["transport_manifest_sha256"] == evidence["transport_manifest_sha256"] == auth.digest(value["transport_manifest"])
                and all(payload["bindings"][k] == value["source_active"][k] for k in ACTIVE_KEYS if k != "activation_sha256")
                and parse_time(payload["issued_at"]) == parse_time(ready["created_at"]) == parse_time(gate["recorded_at"])
                and parse_time(payload["expires_at"]) == parse_time(ready["expires_at"])
                and parse_time(payload["issued_at"]) <= parse_time(frozen) < parse_time(payload["expires_at"]),
                "Cleanup source gate/readiness or freeze-time authority differs")
        require(all(body.get("business_e2e") == "deferred_by_user" and body.get("transport_qualification") == "not_verified"
                    for body in (payload, evidence)), "Cleanup source overclaims qualification")
        bucket = "discovery" if operation in provider_budget.DISCOVERY_OPERATIONS else "metrics"
        require(payload["budget"] == {"total_microusd": provider_budget.AUTOMATIC_MICROUSD,
                "bucket": bucket, "bucket_microusd": provider_budget.BUDGET_BUCKET_MICROUSD[bucket]},
                "Cleanup operator budget must preserve the existing automatic limits")
    source = {r["account_identity_id"]: r for r in value["source_members"]}
    require(len(source) == len(value["source_members"])
            and len({(r["platform"], r["uid"]) for r in source.values()}) == len(source),
            "Cleanup source contains duplicate identities")
    expected = sorted([member_shape(r) for r in source.values()
                       if r["platform"] == "douyin" and r["update_status"] in {"日更", "周更"}
                       and r.get("sec_user_id_sha256")], key=lambda r: r["account_identity_id"])
    require(bool(expected) and expected == value["eligible_members"]
            and value["selection_sha256"] == auth.digest(expected), "Cleanup eligible member intersection differs")


def export_source_authority(connection: sqlite3.Connection, directory_document: Mapping[str, Any], *,
                            source_database_sha256: str, at: str) -> dict[str, Any]:
    require(connection.execute("PRAGMA query_only").fetchone()[0] == 1, "Source authority export must be read-only")
    database = Path(connection.execute("PRAGMA database_list").fetchone()[2])
    require(file_sha(database) == source_database_sha256, "Source database SHA differs")
    active = activation_at(connection, at)
    require(active is not None and active["profile_id"] == "integrated_route_v1", "Source integrated activation is missing")
    roster = account_roster.runtime_snapshot(connection, active)
    members = account_roster.get_current_members(connection, roster["id"])
    directory = {}
    for record in directory_document["records"]:
        row = record["raw"]
        uid = str(row.get("UID") or "").strip()
        if row.get("平台") != "抖音" or not uid.isdigit():
            continue
        key = ("douyin", uid)
        require(key not in directory, "Directory contains duplicate numeric UID")
        directory[key] = row
    matched = []
    for row in members:
        uid = str(row.get("identity_uid") or row.get("uid") or "")
        item = directory.get((row["platform"], uid))
        if item is None:
            continue
        references = {r[0] for r in connection.execute(
            "SELECT reference_value FROM account_provider_references WHERE account_identity_id=? AND lower(provider)='tikhub' AND reference_kind='sec_user_id'",
            (row["account_identity_id"],))}
        sec = next(iter(references)) if len(references) == 1 else ""
        matched.append({"account_identity_id": row["account_identity_id"], "account_id": row["account_id"],
                        "platform": row["platform"], "uid": uid, "update_status": item.get("更新状态"),
                        "sec_user_id_sha256": hashlib.sha256(sec.encode()).hexdigest() if valid_sec(sec) else None})
    matched.sort(key=lambda r: r["account_identity_id"])
    eligible = [member_shape(r) for r in matched if r["update_status"] in {"日更", "周更"} and r["sec_user_id_sha256"]]
    state = paid_drain.dispatch_state(connection, at=at)
    require(state.paid_dispatch_open and state.activation_id == active["activation_id"] and state.permit_event_id is not None,
            "Source paid drain is not open on this activation")
    operations = {}
    for operation in sorted(OPERATIONS):
        gate = connection.execute("SELECT * FROM capture_paid_send_gate_events WHERE provider='tikhub' AND operation=? ORDER BY id DESC LIMIT 1", (operation,)).fetchone()
        ready = connection.execute("SELECT * FROM provider_readiness_receipts WHERE provider='tikhub' AND operation=? ORDER BY id DESC LIMIT 1", (operation,)).fetchone()
        require(gate is not None and ready is not None, "Source operation has no actual gate/readiness")
        operations[operation] = {"gate": dict(gate), "readiness": dict(ready)}
    decision_ref = None
    expected_decision = json.loads(next(iter(operations.values()))["gate"]["evidence_json"])["release_decision_sha256"]
    for row in connection.execute("SELECT payload_json FROM deployment_readiness_receipts ORDER BY id DESC"):
        ref = json.loads(row[0]).get("evidence", {}).get("release_decision")
        if isinstance(ref, dict) and ref.get("sha256") == expected_decision:
            decision_ref = {"path": ref["path"], "sha256": ref["sha256"]}
            break
    require(decision_ref is not None, "Original private operator decision reference is unavailable")
    decision = private_object(decision_ref)
    result = {"contract": SOURCE, "source_database_sha256": source_database_sha256, "frozen_at": utc(at),
              "source_active": {k: active[k] for k in ACTIVE_KEYS}, "source_roster_member_count": len(members),
              "source_directory_sha256": directory_document["sha256"], "source_members": matched,
              "eligible_members": eligible, "selection_sha256": auth.digest(eligible), "operations": operations,
              "decision_receipt": decision_ref, "source_release_event_id": state.permit_event_id,
              "transport_manifest": decision["transport_manifest"]}
    result["snapshot_sha256"] = auth.digest(result)
    validate_source(result, at=at)
    return result


def installed_evidence(connection: sqlite3.Connection, *, at: str, maintenance_only: bool = False) -> dict[str, Any]:
    from .runtime_database import load_installed_writer_contract, require_current_process_writer_lock

    require_current_process_writer_lock(connection)
    schema_version = connection.execute("PRAGMA user_version").fetchone()[0]
    require(schema_version in {20, 21}, "Cleanup release requires schema20 or its verified classification successor")
    installed = load_installed_writer_contract(required=True)
    require(installed is not None and installed.project_root.resolve() == PROJECT_ROOT.resolve(),
            "Cleanup Writer installation belongs to another project")
    environment = installed.payload["EnvironmentVariables"]
    require(all(environment.get(k) == os.environ.get(k) and os.environ.get(k) for k in (
        "DCAR_ACCOUNT_CLEANUP_INSTALL_RECEIPT", "DCAR_LOADED_BUILD_RECEIPT"))
        and not environment.get("DCAR_LOADED_BUILD_ID"), "Cleanup process differs from installed Writer")
    loaded = os.environ.get("DCAR_LOADED_BUILD_ID", "")
    require(loaded.startswith("sha256:") and len(loaded) == 71, "Cleanup loaded build identity is missing")
    build_sha = loaded[7:]
    build_path = Path(os.environ["DCAR_LOADED_BUILD_RECEIPT"])
    build = forward_recovery._private_receipt(build_path, build_sha, "sealed-build-receipt-v1")
    generation = build.get("account_cleanup_generation", {})
    require(generation.get("contract") == GENERATION and bool(generation.get("generation_id"))
            and build.get("status") == "succeeded"
            and build.get("schema_contract", {}).get("code_schema") == schema_version
            and build.get("schema_contract", {}).get("formal_schema") == schema_version
            and not build.get("postmigration_lineage"),
            "Cleanup build schema differs from the installed generation")
    runtime_sha = build["runtime_root_receipt"]["sha256"]
    live = forward_recovery._runtime_identity(connection, {
        "build_receipt_sha256": build_sha, "runtime_root_receipt_sha256": runtime_sha})
    require(Path(live["database_path"]) == installed.database.resolve(), "Cleanup formal database differs")
    critical = build["critical_files"]
    required = {"src/dcar_eval/v8/" + name + ".py" for name in (
        "account_cleanup_runtime", "capture_release", "capture_operator_release", "provider_budget",
        "paid_dispatch", "paid_drain", "capture_authorizations", "runtime_database", "runtime_paths")}
    require(required <= set(critical), "Cleanup build omits safety code from critical inventory")
    tree = private_object(generation["source_tree"])
    require(tree.get("contract") == "writer-source-tree-v1"
            and tree.get("source_root") == str(source_root(PROJECT_ROOT)), "Cleanup source inventory root differs")
    files = {r["path"]: r for r in tree["files"]}
    require(len(files) == len(tree["files"]) and all(files.get(p, {}).get("sha256") == sha for p, sha in critical.items()),
            "Cleanup critical source does not match the full inventory")
    install_path = Path(os.environ["DCAR_ACCOUNT_CLEANUP_INSTALL_RECEIPT"])
    install_body = _raw_file(install_path, private=True)
    install_sha = hashlib.sha256(install_body).hexdigest()
    install = json.loads(install_body)
    classification_proof = None
    manual_scope_proof = None
    metric_gap_proof = None
    profile_operation_authority = None
    profile_compensation_authority = None
    catalog_policy_evidence = {}
    if schema_version == 21:
        from . import account_classification_release, schema_v21
        schema_v21.validate_structure(connection)
        inherited = account_classification_release.verify_inheritance(
            build=build, build_ref=account_classification_release.reference(build_path),
            install_path=install_path, database=installed.database,
            source=source_root(PROJECT_ROOT), at=at)
        migration_install = account_classification_release.object_at(build["account_classification_successor"]["migration"])
        require(schema_v21.migration_proof(connection) == migration_install.get("migration_proof"),
                "classification database migration differs from installed receipt")
        classification_proof = inherited["proof"]
        manual_scope_proof = inherited.get("manual_content_scope_proof")
        metric_gap_proof = inherited.get("metric_gap_proof")
        profile_operation_authority = inherited.get("profile_operation_authority")
        profile_compensation_authority = inherited.get("profile_compensation_authority")
        catalog_policy_evidence = {key: inherited[key] for key in (
            "catalog_capture_policy", "catalog_capture_policy_sha256", "catalog_capture_proof") if key in inherited}
        parent = inherited["parent_build"]
        build_sha = inherited["parent_build_ref"]["sha256"]
        build_path = Path(inherited["parent_build_ref"]["path"])
        runtime_sha = parent["runtime_root_receipt"]["sha256"]
        generation = parent["account_cleanup_generation"]
    else:
        require(build.get("account_classification_successor") is None,
                "classification successor cannot run against unmigrated schema20")
    require(install.get("contract") == "account-cleanup-install-v1" and install.get("status") == "installed"
            and install["formal_database"] == live["database_path"]
            and all(install["installed"][k] == live["database_" + k] for k in ("device", "inode"))
            and install["build_receipt"] == {"path": str(build_path), "sha256": build_sha}
            and parse_time(install["installed_at"]) <= parse_time(at), "Cleanup installed proof or inode differs")
    migration = private_object(generation["migration_receipt"])
    require(migration.get("contract") == "account-cleanup-projection-v1"
            and migration.get("receipt_sha256") == auth.digest({k: v for k, v in migration.items() if k != "receipt_sha256"})
            and migration.get("status") == "candidate_verified"
            and migration.get("verification") == {"foreign_key_check": "ok", "integrity_check": "ok",
                                                  "projected_values_sha256_verified": True}
            and migration["source_backup"]["sha256"] == generation["source_database_sha256"] == install["source_database_sha256"]
            and install["expected_scope"]["migration_receipt_sha256"] == generation["migration_receipt"]["sha256"],
            "Cleanup projection receipt or source binding differs")
    capsule = private_object(generation["source_authority"])
    validate_source(capsule, at=at)
    require(capsule["source_database_sha256"] == generation["source_database_sha256"]
            and capsule["source_directory_sha256"] == migration["source_attachment_sha256"]
            and capsule["selection_sha256"] == generation["selection_sha256"] == install["expected_scope"]["selection_sha256"],
            "Cleanup attachment, source authority or member selection differs")
    active = activation_at(connection, at)
    require(active is not None and {k: active[k] for k in ACTIVE_KEYS} == install["expected_scope"]["active"]
            and active["build_receipt_sha256"] == build_sha and active["profile_id"] == "integrated_route_v1"
            and active.get("metadata", {}).get("account_cleanup") == {
                "contract": GENERATION, "generation_id": generation["generation_id"],
                "selection_sha256": generation["selection_sha256"]}, "Cleanup activation is not the installed generation")
    snapshot = account_roster.runtime_snapshot(connection, active)
    if catalog_policy_evidence:
        # The original snapshot remains historical runtime evidence. Resolve
        # live identity/locator changes per catalog member, not as a global
        # failure that would prevent all other accounts from being planned.
        original = {m["account_identity_id"]: m for m in capsule["eligible_members"]}
        members = [{**dict(row), "account_id": original.get(row["account_identity_id"], {}).get("account_id")}
            for row in connection.execute("SELECT * FROM account_roster_members WHERE snapshot_id=?",
                (snapshot["id"],))]
    else:
        members = account_roster.get_current_members(connection, snapshot["id"])
    actual = sorted([{"account_identity_id": r["account_identity_id"], "account_id": r["account_id"],
                      "platform": r["platform"], "uid": r.get("identity_uid") or r.get("uid")} for r in members],
                    key=lambda r: r["account_identity_id"])
    require(actual == capsule["eligible_members"],
            "Cleanup active roster exceeds or changes the approved valid member intersection")
    # An operator pause narrows live eligibility, not the immutable authorized
    # identity set. Require its ordinary append-only state event; the per-member
    # claim and pre-send gates still reject enabled=0 independently.
    from .account_states import state_events
    for member in ([] if catalog_policy_evidence else members):
        if member["enabled"] == 1:
            continue
        events = state_events(connection, member["account_identity_id"])
        latest = events[-1] if events else {}
        require(member["enabled"] == 0 and latest.get("new_enabled") is False
                and latest.get("activation_id") == active["activation_id"]
                and parse_time(latest["effective_at"]) <= parse_time(at)
                and parse_time(latest["created_at"]) <= parse_time(at),
                "Cleanup disabled member lacks its recorded operator pause")
    source_members = {r["account_identity_id"]: r for r in capsule["source_members"]}
    # The new policy validates live identity/locator evidence per task. Keep
    # the original capsule and its frozen roster intact as historical proof.
    for row in ([] if catalog_policy_evidence else actual):
        refs = {r[0] for r in connection.execute(
            "SELECT reference_value FROM account_provider_references WHERE account_identity_id=? AND lower(provider)='tikhub' AND reference_kind='sec_user_id'",
            (row["account_identity_id"],))}
        sec = next(iter(refs)) if len(refs) == 1 else ""
        require(valid_sec(sec) and hashlib.sha256(sec.encode()).hexdigest() == source_members[row["account_identity_id"]]["sec_user_id_sha256"],
                "Cleanup account reference changed or is no longer valid")
    manifest = forward_recovery._route()
    require(manifest == generation["transport_manifest"] == capsule["transport_manifest"]
            and all(json.loads(p["gate"]["evidence_json"])["bindings"]["config_receipt_sha256"] == generation["config_sha256"]
                    for p in capsule["operations"].values()), "Cleanup transport or approved config changed")
    guard = forward_recovery._live_guards(connection, at=at, check_capacity=not maintenance_only) if not maintenance_only else {}
    proof = {"contract": GENERATION, "generation_id": generation["generation_id"],
             "selection_sha256": generation["selection_sha256"], "source_authority": generation["source_authority"],
             "install_receipt_sha256": install_sha, "active": {k: active[k] for k in ACTIVE_KEYS},
             "build_sha256": build_sha, "runtime_sha256": runtime_sha, "config_sha256": generation["config_sha256"],
             "transport_manifest": manifest, "actor": generation["actor"], "reason": generation["reason"],
             "issued_at": generation["issued_at"]}
    proof["proof_sha256"] = auth.digest(proof)
    evidence = {"active": active, "build_sha256": build_sha, "runtime_sha256": runtime_sha,
                "config_sha256": generation["config_sha256"], "manifest": manifest,
                "migration": migration, "install": install, "storage_policy": guard.get("capacity", {}),
                "activation_successor": None, "code_successor": None, "account_cleanup_generation": proof}
    if classification_proof is not None:
        evidence["account_classification_successor"] = classification_proof
    if manual_scope_proof is not None:
        evidence["manual_content_scope_successor"] = manual_scope_proof
    if metric_gap_proof is not None:
        evidence["metric_gap_successor"] = metric_gap_proof
    evidence.update(catalog_policy_evidence)
    if profile_operation_authority is not None:
        evidence["profile_operation_authority"] = profile_operation_authority
    if profile_compensation_authority is not None:
        evidence["profile_compensation_authority"] = profile_compensation_authority
    decision = cleanup_decision(evidence)
    evidence["deployment"] = {"status": "accepted", "contract_version": GENERATION,
                              "release_decision": decision, "bindings": {**proof["active"],
                                  **{k: evidence[k] for k in ("build_sha256", "runtime_sha256", "config_sha256")}}}
    return evidence


def cleanup_decision(evidence: Mapping[str, Any]) -> dict[str, Any]:
    proof = evidence["account_cleanup_generation"]
    require(proof.get("contract") == GENERATION and proof["proof_sha256"] == auth.digest(
        {k: v for k, v in proof.items() if k != "proof_sha256"}), "Cleanup runtime proof changed")
    decision = {"contract_version": DECISION, "generation_id": proof["generation_id"],
                "selection_sha256": proof["selection_sha256"], "source_authority_sha256": proof["source_authority"]["sha256"],
                "production_rollout": "approved_by_user", "business_e2e": "deferred_by_user",
                "transport_qualification": "not_verified", "qualification": "operator_authorized",
                "approved_target_profile": "integrated_route_v1", "operations": sorted(OPERATIONS),
                "bindings": proof["active"], "runtime_bindings": {k: proof[k] for k in ("build_sha256", "runtime_sha256", "config_sha256")},
                "transport_manifest": proof["transport_manifest"], "actor": proof["actor"],
                "reason": proof["reason"], "issued_at": proof["issued_at"]}
    decision["decision_sha256"] = auth.digest(decision)
    return decision


def validate_decision(evidence: Mapping[str, Any], operation: str, at: str) -> dict[str, Any] | None:
    decision = cleanup_decision(evidence)
    require(evidence["deployment"].get("status") == "accepted"
            and evidence["deployment"]["release_decision"] == decision
            and decision["bindings"] == {k: evidence["active"][k] for k in ACTIVE_KEYS}
            and decision["runtime_bindings"] == {k: evidence[k] for k in ("build_sha256", "runtime_sha256", "config_sha256")}
            and decision["transport_manifest"] == evidence["manifest"]
            and parse_time(decision["issued_at"]) <= parse_time(at)
            and bool(str(decision["actor"]).strip()) and bool(str(decision["reason"]).strip()),
            "Cleanup operator decision differs from installed runtime")
    validate_source(private_object(evidence["account_cleanup_generation"]["source_authority"]), at=at)
    return decision if operation in OPERATIONS else None


def release_control(evidence: Mapping[str, Any]) -> dict[str, Any]:
    proof = evidence["account_cleanup_generation"]
    return {"contract": CONTROL, "generation_id": proof["generation_id"], "selection_sha256": proof["selection_sha256"],
            "active": {k: evidence["active"][k] for k in ACTIVE_KEYS}, "transport_manifest": evidence["manifest"],
            **{k: evidence[k] for k in ("build_sha256", "runtime_sha256", "config_sha256")}}


def validate_control(connection: sqlite3.Connection, evidence: Mapping[str, Any], control: Mapping[str, Any], *, at: str) -> None:
    require(dict(control) == release_control(evidence), "Cleanup RELEASE control differs from the installed generation")
    forward_recovery._live_guards(connection, at=at, check_capacity=False)


def bootstrap_operator(connection: sqlite3.Connection, *, evidence: Mapping[str, Any], operation: str,
                       at: str) -> dict[str, Any]:
    """Issue only the first inherited operator gate through normal admission."""
    from .capture_operator_release import publish

    require(connection.in_transaction, "Cleanup operator initialization requires a writer transaction")
    require(operation in OPERATIONS and validate_decision(evidence, operation, at) is not None,
            "Cleanup operation lacks its inherited authority")
    require(connection.execute("SELECT 1 FROM capture_paid_send_gate_events WHERE lower(provider)='tikhub' AND operation=? LIMIT 1",
                               (operation,)).fetchone() is None,
            "Cleanup initialization must not reopen or replace an existing gate")
    return {"status": "initialized", **publish(connection, evidence=evidence, operation=operation, at=at)}
