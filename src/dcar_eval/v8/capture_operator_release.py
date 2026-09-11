"""Explicit deployment-bound production authority, never statistical qualification.

The private decision is verified by the installed deployment validator. Every
admission still verifies that installation and the live dispatch controls. A
24-hour gate is renewable only after its operation was explicitly opened.
"""
from __future__ import annotations

import sqlite3
from datetime import timedelta
from typing import Any, Mapping

from . import capture_authorizations as auth, provider_budget
from .metric_field_facts import utc
from .source_routing import parse_time

DECISION_CONTRACT = "v25-user-release-decision-v1"
ISSUANCE_CONTRACT = "capture-operator-operation-issuance-v1"
QUALIFICATION = "operator_authorized"
ACTIVE_KEYS = ("activation_id", "profile_id", "roster_snapshot_id", "roster_members_sha256", "activation_sha256")
RUNTIME_KEYS = ("build_sha256", "runtime_sha256", "config_sha256")
GATE_KEYS = ("provider", "operation", "state", "reason", "evidence_json", "recorded_at")
READY_KEYS = ("provider", "operation", "status", "reason", "evidence_json", "created_at", "expires_at")


def _require(condition: bool, message: str) -> None:
    if not condition:
        raise auth.AuthorizationError(message)


def _decision(evidence: Mapping[str, Any], operation: str, at: str) -> dict[str, Any] | None:
    from . import capture_release as release
    from .account_profile_authority import OPERATIONS as PROFILE_OPERATIONS, decision as profile_decision

    if operation in PROFILE_OPERATIONS:
        return profile_decision(evidence, operation, at)

    decision = evidence["deployment"].get("release_decision")
    if decision is None:
        return None
    if isinstance(decision, dict) and decision.get("contract_version") == "account-cleanup-operator-decision-v1":
        from .account_cleanup_runtime import validate_decision
        return validate_decision(evidence, operation, at)
    _require(isinstance(decision, dict)
             and decision.get("contract_version") == DECISION_CONTRACT
             and decision.get("production_rollout") == "approved_by_user"
             and decision.get("business_e2e") == "deferred_by_user"
             and decision.get("transport_qualification") == "not_verified",
             "Production release decision is invalid")
    operations = decision.get("operations")
    _require(isinstance(operations, list) and bool(operations)
             and all(isinstance(item, str) for item in operations)
             and len(set(operations)) == len(operations)
             and set(operations) <= release.CONTINUITY_OPERATIONS,
             "Production release operation scope is invalid")
    if operation not in operations:
        return None
    checksum = decision.get("decision_sha256")
    _require(isinstance(checksum, str) and len(checksum) == 64
             and all(char in "0123456789abcdef" for char in checksum)
             and bool(str(decision.get("actor", "")).strip())
             and bool(str(decision.get("reason", "")).strip())
             and parse_time(decision["issued_at"]) <= parse_time(at),
             "Production release decision identity or time is invalid")
    code_successor = evidence.get("code_successor")
    runtime = {key: evidence[key] for key in RUNTIME_KEYS}
    if code_successor is not None:
        _require(code_successor["runtime_bindings"] == runtime
                 and code_successor["origin_runtime_bindings"] == decision["runtime_bindings"]
                 and code_successor["active"] == {key: evidence["active"][key] for key in ACTIVE_KEYS}
                 and code_successor["decision_receipt"] is not None,
                 "Production code successor is not bound to the installed generation")
        runtime = code_successor["origin_runtime_bindings"]
    _require(evidence["deployment"]["status"] == "accepted"
             and decision.get("runtime_bindings") == runtime
             and decision.get("transport_manifest") == evidence["manifest"],
             "Production release deployment, runtime or transport changed")
    active = evidence["active"]
    source = active
    if active["profile_id"] == "integrated_route_v1":
        successor = evidence.get("activation_successor")
        from .account_roster_capture import METADATA_KEY
        account_roster = METADATA_KEY in active.get("metadata", {})
        snapshot = active.get("metadata", {}).get(METADATA_KEY if account_roster else "capture_operation_source", {})
        frozen = snapshot.get("operations", {}).get(operation, {})
        _require(isinstance(successor, dict)
                 and successor.get("contract") == "capture-installed-activation-successor-v1"
                 and successor.get("target_active") == {key: active[key] for key in ACTIVE_KEYS}
                 and operation in successor.get("operations", {})
                 and decision.get("approved_target_profile") == "integrated_route_v1"
                 and frozen.get("qualification_kind") == QUALIFICATION
                 and frozen.get("release_decision") == decision,
                 "Integrated operation lacks its explicit deployment decision successor")
        assert isinstance(successor, dict)
        if account_roster:
            # The installed validator has already verified every immutable
            # roster transition, its origin decision, routes and RELEASE.
            proof = successor.get("operator_roster_source", {})
            source = frozen.get("source_evidence", {}).get("active", {})
            _require(proof.get("decision_sha256") == decision["decision_sha256"]
                     and proof.get("source_active") == {key: source[key] for key in ACTIVE_KEYS}
                     and operation in proof.get("operations", [])
                     and bool(proof.get("chain_snapshot_sha256s"))
                     and proof["chain_snapshot_sha256s"][0] == snapshot.get("snapshot_sha256"),
                     "Account roster lacks its verified operator decision chain")
        else:
            source = snapshot["source_active"]
            _require(all(active[key] == source[key] for key in ("roster_snapshot_id", "roster_members_sha256")),
                     "Production release successor roster changed")
    _require(source["profile_id"] == "tikhub_managed_v1"
             and all(decision.get("bindings", {}).get(key) == source[key] for key in ACTIVE_KEYS),
             "Production release activation or roster changed")
    return dict(decision)


def _bindings(evidence: Mapping[str, Any], decision: Mapping[str, Any], operation: str,
              release_event_id: int) -> dict[str, Any]:
    identity = {"contract": ISSUANCE_CONTRACT, "decision_sha256": decision["decision_sha256"],
                "operation": operation, "active": {key: evidence["active"][key] for key in ACTIVE_KEYS},
                "runtime": {key: evidence[key] for key in RUNTIME_KEYS},
                "transport_manifest_sha256": auth.digest(evidence["manifest"]),
                "release_event_id": release_event_id}
    if evidence.get("code_successor") is not None:
        identity["code_successor_sha256"] = evidence["code_successor"]["proof_sha256"]
    # Wire compatibility only; this digest is explicitly an operator issuance,
    # not a continuity permit or a qualification receipt.
    return {**{key: evidence["active"][key] for key in ACTIVE_KEYS if key != "activation_sha256"},
            "build_receipt_sha256": evidence["build_sha256"],
            "runtime_root_receipt_sha256": evidence["runtime_sha256"],
            "config_receipt_sha256": evidence["config_sha256"],
            "continuity_permit_sha256": auth.digest(identity)}


def authority(connection: sqlite3.Connection, *, evidence: Mapping[str, Any],
              operation: str, at: str) -> dict[str, Any] | None:
    from . import capture_release as release

    decision = _decision(evidence, operation, at)
    if decision is None:
        return None
    release.require_current_process_writer_lock(connection)
    release_id = release._native_control(connection, evidence, at=at)
    _require((provider_budget.circuit_state(connection) or {}).get("open") is not True,
             "Provider circuit blocks operator release")
    # A retained production decision may be renewed while its operation is
    # unhealthy. This is business authority, not fault recovery: readiness and
    # every reservation/send still enforce the operation circuit separately.
    result = {"decision": decision, "bindings": _bindings(evidence, decision, operation, release_id),
              "release_event_id": release_id}
    from .account_roster_capture import METADATA_KEY
    source = evidence["active"].get("metadata", {}).get(METADATA_KEY)
    if source is not None:
        result["account_roster_snapshot_sha256"] = source["snapshot_sha256"]
    return result


def _proof(value: Mapping[str, Any]) -> dict[str, Any]:
    result = {"release_decision_sha256": value["decision"]["decision_sha256"],
            "release_event_id": value["release_event_id"],
            "business_e2e": value["decision"]["business_e2e"], "transport_qualification": "not_verified"}
    if "account_roster_snapshot_sha256" in value:
        result["account_roster_snapshot_sha256"] = value["account_roster_snapshot_sha256"]
    return result


def publish(connection: sqlite3.Connection, *, evidence: Mapping[str, Any], operation: str,
            at: str) -> dict[str, Any]:
    _require(connection.in_transaction, "Operator gate issuance requires a writer transaction")
    value = authority(connection, evidence=evidence, operation=operation, at=at)
    _require(value is not None, "Operation has no explicit production release decision")
    assert value is not None
    bindings = value["bindings"]
    scope = auth.scope_hash(runtime_bindings=bindings, provider="tikhub", operation=operation)
    expires = utc((parse_time(at) + timedelta(hours=24)).isoformat())
    manifest_sha = auth.digest(evidence["manifest"])
    ready_evidence = {"contract": auth.READINESS_CONTRACT, "bindings": bindings, "scope_hash": scope,
                      "qualification": QUALIFICATION, **_proof(value),
                      "continuity_permit_sha256": bindings["continuity_permit_sha256"],
                      "transport_manifest_sha256": manifest_sha}
    ready = {"provider": "tikhub", "operation": operation, "status": "ready",
             "reason": "user-approved-production-release", "evidence_json": auth.canonical(ready_evidence),
             "created_at": utc(at), "expires_at": expires}
    ready_sha = auth.digest(ready)
    connection.execute(f"INSERT OR IGNORE INTO provider_readiness_receipts({','.join(ready)},receipt_sha256) VALUES ({','.join('?' for _ in range(len(ready)+1))})",
                       (*ready.values(), ready_sha))
    ready_id = connection.execute("SELECT id FROM provider_readiness_receipts WHERE receipt_sha256=?", (ready_sha,)).fetchone()[0]
    bucket = "discovery" if operation in provider_budget.DISCOVERY_OPERATIONS else "metrics"
    payload = {"contract": auth.CONTRACT, "bindings": bindings, "scope_hash": scope, "operation": operation,
               "issued_at": utc(at), "expires_at": expires, "readiness_receipt_id": ready_id,
               "readiness_receipt_sha256": ready_sha, "transport_manifest_sha256": manifest_sha, **_proof(value),
               "budget": {"total_microusd": provider_budget.AUTOMATIC_MICROUSD, "bucket": bucket,
                          "bucket_microusd": provider_budget.BUDGET_BUCKET_MICROUSD[bucket]}}
    from .account_roster_capture import operation_budget
    inherited = operation_budget(evidence["active"], operation)
    if inherited is not None:
        payload["budget"] = inherited
    gate = {"provider": "tikhub", "operation": operation, "state": "open", "reason": ready["reason"],
            "evidence_json": auth.canonical(payload), "recorded_at": utc(at)}
    gate_sha = auth.digest(gate)
    connection.execute(f"INSERT OR IGNORE INTO capture_paid_send_gate_events({','.join(gate)},event_sha256) VALUES ({','.join('?' for _ in range(len(gate)+1))})",
                       (*gate.values(), gate_sha))
    return {"readiness_receipt_id": ready_id, "gate_sha256": gate_sha, "qualification": QUALIFICATION,
            **_proof(value), "ordinary_paid_authorized": True, "coverage_complete": False,
            "provider_calls": 0, "activation_id": bindings["activation_id"], "expires_at": expires}


def _verify_pair(value: Mapping[str, Any], *, evidence: Mapping[str, Any], operation: str,
                 readiness_evidence: Mapping[str, Any], gate_payload: Mapping[str, Any]) -> None:
    bindings = value["bindings"]
    scope = auth.scope_hash(runtime_bindings=bindings, provider="tikhub", operation=operation)
    for body in (readiness_evidence, gate_payload):
        _require(body.get("bindings") == bindings and body.get("scope_hash") == scope
                 and body.get("transport_manifest_sha256") == auth.digest(evidence["manifest"])
                 and all(body.get(key) == expected for key, expected in _proof(value).items()),
                 "Operator release decision, dispatch or runtime evidence changed")
    _require(readiness_evidence.get("qualification") == QUALIFICATION
             and readiness_evidence.get("contract") == auth.READINESS_CONTRACT
             and gate_payload.get("contract") == auth.CONTRACT
             and gate_payload.get("operation") == operation
             and readiness_evidence.get("continuity_permit_sha256") == bindings["continuity_permit_sha256"]
             and parse_time(gate_payload["expires_at"]) - parse_time(gate_payload["issued_at"]) == timedelta(hours=24),
             "Operator issuance cannot claim qualification or extend its gate lifetime")


def validate_request(connection: sqlite3.Connection, *, runtime_bindings: Mapping[str, Any],
                     operation: str, at: str, readiness_evidence: Mapping[str, Any],
                     gate_payload: Mapping[str, Any]) -> None:
    from . import capture_release as release

    evidence = release._installed_evidence(connection, at=at)
    value = authority(connection, evidence=evidence, operation=operation, at=at)
    _require(value is not None and value["bindings"] == dict(runtime_bindings),
             "Operator release authority is absent or changed")
    assert value is not None
    _verify_pair(value, evidence=evidence, operation=operation,
                 readiness_evidence=readiness_evidence, gate_payload=gate_payload)


def _gate_evidence(connection: sqlite3.Connection, gate: Mapping[str, Any], *,
                   value: Mapping[str, Any], evidence: Mapping[str, Any], operation: str) -> dict[str, Any]:
    _require(gate["state"] == "open" and gate["provider"] == "tikhub" and gate["operation"] == operation
             and gate["event_sha256"] == auth.digest({key: gate[key] for key in GATE_KEYS}),
             "Prior operator gate is closed or changed")
    payload = auth._payload(gate["evidence_json"])
    ready = connection.execute("SELECT * FROM provider_readiness_receipts WHERE id=?", (payload.get("readiness_receipt_id"),)).fetchone()
    _require(ready is not None and ready["provider"] == "tikhub" and ready["operation"] == operation
             and ready["status"] == "ready" and ready["receipt_sha256"] == payload.get("readiness_receipt_sha256")
             and ready["receipt_sha256"] == auth.digest({key: ready[key] for key in READY_KEYS})
             and utc(ready["created_at"]) == utc(gate["recorded_at"]) == utc(payload["issued_at"])
             and utc(ready["expires_at"]) == utc(payload["expires_at"]),
             "Prior operator readiness is missing or changed")
    _verify_pair(value, evidence=evidence, operation=operation,
                 readiness_evidence=auth._payload(ready["evidence_json"]), gate_payload=payload)
    return {"gate": dict(gate), "readiness": dict(ready), "expires_at": payload["expires_at"]}


def maintain(connection: sqlite3.Connection, *, evidence: Mapping[str, Any], operation: str,
             latest: Mapping[str, Any], at: str) -> dict[str, Any] | None:
    payload = auth._payload(latest["evidence_json"])
    if "release_decision_sha256" not in payload:
        return None
    _require(latest["state"] == "open", "Operator maintenance cannot reopen a closed gate")
    value = authority(connection, evidence=evidence, operation=operation, at=at)
    _require(value is not None, "Previously approved operation lost its production decision")
    assert value is not None
    proof = _gate_evidence(connection, latest, value=value, evidence=evidence, operation=operation)
    newest = connection.execute("SELECT id FROM provider_readiness_receipts WHERE provider='tikhub' AND operation=? ORDER BY id DESC LIMIT 1", (operation,)).fetchone()
    _require(newest is not None and newest[0] == proof["readiness"]["id"]
             and parse_time(latest["recorded_at"]) <= parse_time(at),
             "Operator maintenance readiness was superseded or future-dated")
    if parse_time(proof["expires_at"]) - parse_time(at) <= timedelta(hours=6):
        return {"status": "renewed", **publish(connection, evidence=evidence, operation=operation, at=at)}
    return {"status": "fresh", "qualification": QUALIFICATION, **_proof(value), "expires_at": proof["expires_at"]}


def snapshot(connection: sqlite3.Connection, *, evidence: Mapping[str, Any], operation: str,
             at: str) -> dict[str, Any]:
    """Freeze an actual open source gate, without producing a qualification."""
    from . import capture_release as release

    value = authority(connection, evidence=evidence, operation=operation, at=at)
    _require(value is not None and evidence["active"]["profile_id"] == "tikhub_managed_v1",
             "Operator source requires the current approved Mode B deployment")
    assert value is not None
    _require(value["decision"].get("approved_target_profile") == "integrated_route_v1",
             "User decision does not authorize the integrated successor")
    gate = connection.execute("SELECT * FROM capture_paid_send_gate_events WHERE provider='tikhub' AND operation=? ORDER BY id DESC LIMIT 1", (operation,)).fetchone()
    _require(gate is not None, "Source operator gate was never explicitly opened")
    proof = _gate_evidence(connection, gate, value=value, evidence=evidence, operation=operation)
    newest = connection.execute("SELECT id FROM provider_readiness_receipts WHERE provider='tikhub' AND operation=? ORDER BY id DESC LIMIT 1", (operation,)).fetchone()
    _require(newest is not None and newest[0] == proof["readiness"]["id"]
             and parse_time(gate["recorded_at"]) <= parse_time(at) < parse_time(proof["expires_at"]),
             "Source operator gate expired or readiness was superseded")
    source = {key: evidence[key] for key in ("active", *RUNTIME_KEYS, "manifest")}
    source["deployment"] = {"status": "accepted", "release_decision": value["decision"]}
    result = {"contract": "capture-operation-qualification-snapshot-v1", "operation": operation,
              "qualification_kind": QUALIFICATION, "release_decision": value["decision"],
              "release_event_id": value["release_event_id"], "frozen_at": utc(at),
              "expires_at": proof["expires_at"], "manifest": evidence["manifest"],
              "transport_code_sha256": release._transport_code(), "runtime_bindings": release._native_runtime(evidence),
              "authority_bindings": value["bindings"], "source_evidence": source,
              "gate_id": gate["id"], "gate_sha256": gate["event_sha256"],
              "readiness_id": proof["readiness"]["id"], "readiness_sha256": proof["readiness"]["receipt_sha256"],
              "transport_qualification": "not_verified", "business_e2e": "deferred_by_user"}
    result["snapshot_sha256"] = auth.digest(result)
    validate_frozen(connection, result, at=at)
    return result


def validate_frozen(connection: sqlite3.Connection, frozen: Mapping[str, Any], *, at: str,
                    require_unexpired: bool = True,
                    portable_deployment: Mapping[str, Any] | None = None) -> None:
    """Reverify the real private decision and immutable source evidence.

Only the frozen source gate has a TTL. A valid target's user authorization
does not expire because its predecessor's 24-hour gate did.

The snapshot installer alone can supply an already verified portable deployment
for read-only succession. Writer/admission callers retain private-file checks.
"""
    from . import capture_release as release

    _require(frozen.get("contract") == "capture-operation-qualification-snapshot-v1"
             and frozen.get("qualification_kind") == QUALIFICATION
             and frozen.get("snapshot_sha256") == auth.digest({key: value for key, value in frozen.items() if key != "snapshot_sha256"})
             and frozen.get("transport_code_sha256") == release._transport_code()
             and frozen.get("transport_qualification") == "not_verified"
             and frozen.get("business_e2e") == "deferred_by_user"
             and parse_time(frozen["frozen_at"]) <= parse_time(at),
             "Frozen operator source changed or claims qualification")
    deployment = (release._release_tools().validate_deployment_receipt(connection, project_root=release.PROJECT_ROOT)
                  if portable_deployment is None else portable_deployment)
    decision = deployment.get("release_decision")
    _require(deployment.get("status") == "accepted" and decision == frozen.get("release_decision")
             and isinstance(decision, dict) and decision.get("approved_target_profile") == "integrated_route_v1",
             "Frozen operator source lacks the verified private integrated decision")
    evidence = frozen["source_evidence"]
    _require(evidence["active"]["profile_id"] == "tikhub_managed_v1"
             and evidence["deployment"].get("release_decision") == decision
             and frozen["runtime_bindings"] == release._native_runtime(evidence)
             and frozen["manifest"] == evidence["manifest"], "Frozen operator source runtime changed")
    verified = _decision(evidence, frozen["operation"], frozen["frozen_at"])
    _require(verified is not None, "Frozen operation is outside the production decision")
    assert verified is not None
    value = {"decision": verified, "release_event_id": frozen["release_event_id"],
             "bindings": _bindings(evidence, verified, frozen["operation"], frozen["release_event_id"])}
    gate = connection.execute("SELECT * FROM capture_paid_send_gate_events WHERE id=?", (frozen["gate_id"],)).fetchone()
    _require(gate is not None and gate["event_sha256"] == frozen["gate_sha256"], "Frozen source gate changed")
    proof = _gate_evidence(connection, gate, value=value, evidence=evidence, operation=frozen["operation"])
    _require(proof["readiness"]["id"] == frozen["readiness_id"]
             and proof["readiness"]["receipt_sha256"] == frozen["readiness_sha256"]
             and value["bindings"] == frozen["authority_bindings"]
             and utc(proof["expires_at"]) == utc(frozen["expires_at"])
             and parse_time(gate["recorded_at"]) <= parse_time(frozen["frozen_at"]) < parse_time(proof["expires_at"])
             and (not require_unexpired or parse_time(at) < parse_time(proof["expires_at"])),
             "Frozen operator source issuance expired or changed")
