#!/usr/bin/env python3
"""Issue verified schema20 deployment receipts; never open a paid gate.

The CLI only bootstraps a stopped installed Writer's candidate under its real
writer lock. Acceptance is a Writer-owned callable using actual native traffic.
"""
from __future__ import annotations

import argparse
import hashlib
import json
import os
import sqlite3
import sys
from pathlib import Path
from typing import Any, Mapping

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT / "src/dcar_eval"))

from v8 import capture, capture_release, forward_recovery, raw_archive  # noqa: E402
from v8.profile_activations import activation_at  # noqa: E402
from v8.runtime_database import (  # noqa: E402
    DatabaseAccessMode, acquire_writer_lock, load_installed_writer_contract,
    require_current_process_writer_lock, resolve_installed_database_access,
)
from v8.source_routing import parse_time  # noqa: E402
from v8.storage import now_utc  # noqa: E402
from v8.transport_receipts import read_transport_receipt  # noqa: E402

# Writer imports this file by its sealed absolute path, without a scripts path.
contract = capture_release._release_tools()
sealer = sys.modules["seal_r0_receipts"]


def _require(value: bool, reason: str) -> None:
    if not value:
        raise contract.ReleaseContractError(reason)


def _ref(path: Path, project_root: Path, *, allow_schema20_migration: bool = False) -> dict[str, Any]:
    # Private/single-link receipts plus their real byte hash, never caller SHA.
    sealer._read_private_json(path, allow_schema20_migration=allow_schema20_migration)
    return contract.verified_reference({"path": str(path), "sha256": sealer._sha256_file(path),
                                        "result": "passed"}, project_root=project_root)


def _writer(connection: sqlite3.Connection, at: str) -> dict[str, object]:
    _require(connection.in_transaction, "deployment issuance requires a writer transaction")
    _require(connection.execute("PRAGMA user_version").fetchone()[0] == 20, "deployment requires exact schema20")
    parse_time(at)
    return require_current_process_writer_lock(connection)


def _append(connection: sqlite3.Connection, *, deployment_id: str, status: str,
            payload: Mapping[str, Any], at: str, project_root: Path) -> dict[str, Any]:
    _require(bool(deployment_id.strip()) and len(deployment_id) <= 128, "invalid deployment identity")
    encoded = json.dumps(payload, ensure_ascii=False, sort_keys=True, separators=(",", ":"), allow_nan=False)
    previous = connection.execute("SELECT * FROM deployment_readiness_receipts WHERE deployment_id=?", (deployment_id,)).fetchone()
    if previous is not None:
        _require(previous["status"] == status and json.loads(previous["payload_json"]) == payload,
                 "deployment id already binds different evidence")
        return {**contract.validate_deployment_receipt(connection, deployment_id=deployment_id,
                require_accepted=status == "accepted", project_root=project_root), "idempotent": True,
                "ordinary_paid_authorized": False}
    _require(connection.execute("SELECT 1 FROM deployment_readiness_receipts WHERE status='accepted'").fetchone() is None,
             "an accepted deployment already exists; issuance cannot supersede it")
    connection.execute("SAVEPOINT issue_v20_deployment")
    try:
        checksum = contract.deployment_digest(deployment_id=deployment_id, status=status, payload=payload, recorded_at=at)
        connection.execute("""INSERT INTO deployment_readiness_receipts(deployment_id,status,payload_json,
            recorded_at,receipt_sha256) VALUES(?,?,?,?,?)""", (deployment_id, status, encoded, at, checksum))
        validated = contract.validate_deployment_receipt(connection, deployment_id=deployment_id,
            require_accepted=status == "accepted", project_root=project_root)
        connection.execute("RELEASE SAVEPOINT issue_v20_deployment")
        return {**validated, "idempotent": False, "ordinary_paid_authorized": False}
    except BaseException:
        connection.execute("ROLLBACK TO SAVEPOINT issue_v20_deployment")
        connection.execute("RELEASE SAVEPOINT issue_v20_deployment")
        raise


def issue_candidate(connection: sqlite3.Connection, *, deployment_id: str, project_root: Path,
                    preinstall_build_receipt: Path, install_receipt: Path,
                    storage_policy: Mapping[str, Any], at: str) -> dict[str, Any]:
    """Install pair first, candidate next, postinstall seal last: no hash cycle."""
    lease = _writer(connection, at)
    pre_ref = _ref(preinstall_build_receipt, project_root)
    build = sealer._read_receipt(preinstall_build_receipt, contract_version=sealer.SEALED_BUILD_CONTRACT)
    _require(build.get("status") == "succeeded" and build["schema_contract"] == sealer._schema_contract(19, 20),
             "candidate requires its verified formal19/code20 preinstall build")
    _require(build["git"] == sealer._git_record(project_root, allow_working_tree=True), "tested preinstall source drifted")
    install_ref = _ref(install_receipt, project_root)
    install = contract._json_reference(install_ref)
    _require(install.get("schema_version") == "dcar-writer-database-v20-install-v1" and install.get("status") == "installed",
             "candidate requires an actual schema20 install")
    _require(install["formal_database"] == lease["database_path"]
             and all(install["installed"]["file"][key] == lease[f"database_{key}"] for key in ("device", "inode"))
             and build["installed_runtime"]["database"]["path"] == lease["database_path"],
             "installed database inode/path differs from sealed pair")
    _require(install["code_identity"] == build["git"]["code_identity"], "migration and tested build code differ")
    migration_ref = _ref(Path(install["migration_receipt"]["path"]), project_root, allow_schema20_migration=True)
    migration = contract._json_reference(migration_ref, allow_schema20_migration=True)
    rollback_ref = _ref(Path(migration["verified_backup"]["receipt_path"]), project_root)
    runtime_ref = _ref(Path(build["runtime_root_receipt"]["path"]), project_root)
    _require(runtime_ref["sha256"] == build["runtime_root_receipt"]["sha256"], "preinstall runtime receipt changed")
    sealer._read_receipt(Path(runtime_ref["path"]), contract_version=sealer.RUNTIME_ROOT_CONTRACT)
    checks_ref = _ref(Path(build["test_results_receipt"]["path"]), project_root)
    _require(checks_ref["sha256"] == build["test_results_receipt"]["sha256"], "preinstall full checks changed")
    archive_ref = contract.verified_reference({**build["source_archive"], "result": "passed"}, project_root=project_root)
    contract.validate_legacy_raw_files(migration["legacy_raw"])
    active = activation_at(connection, at)
    _require(active is not None and active["profile_id"] == "tikhub_managed_v1", "candidate retains the installed Mode B activation")
    assert active is not None
    _require(connection.execute("""SELECT 1 FROM capture_paid_send_gate_events g WHERE g.state='open'
        AND g.id=(SELECT max(x.id) FROM capture_paid_send_gate_events x WHERE x.provider=g.provider AND x.operation=g.operation)""").fetchone() is None,
        "candidate cannot be issued over ordinary open paid gates")
    config = {"source_archive_sha256": archive_ref["sha256"], "transport_manifest": forward_recovery._route(),
              "critical_configuration": {key: value for key, value in build["critical_files"].items() if key.startswith("config/")}}
    payload = {"contract_version": contract.CONTRACT_VERSION, "schema_version": 20,
        "schema_migration": contract.SCHEMA_MIGRATIONS[20], "coverage_complete": False,
        "storage_policy": contract.validate_storage_policy(dict(storage_policy)),
        "bindings": {**{key: active[key] for key in ("activation_id", "profile_id", "roster_snapshot_id", "roster_members_sha256", "activation_sha256")},
                     "build_sha256": pre_ref["sha256"], "runtime_sha256": runtime_ref["sha256"],
                     "config_sha256": contract.digest(config)},
        "configuration_evidence": config,
        "evidence": {"source_archive": archive_ref, "migration": migration_ref, "rollback": rollback_ref,
                     "full_checks": checks_ref, "install": install_ref}}
    return _append(connection, deployment_id=deployment_id, status="candidate", payload=payload, at=at, project_root=project_root)


def _actual_e2e(connection: sqlite3.Connection, *, cohort_id: int, evidence: Mapping[str, Any], at: str) -> dict[str, Any]:
    """Derive a completed bounded native request and its own business facts."""
    cohort = read_transport_receipt(connection, cohort_id)
    capture_release._check_native_cohort(connection, cohort, evidence=evidence, at=at, require_unexpired=False)
    members = capture_release._native_members(connection, cohort)
    rows = connection.execute("""SELECT id,dispatch_id FROM paid_provider_dispatch_events
        WHERE event_type='send_marked' AND lower(provider)='tikhub' AND operation=? AND id>?
        ORDER BY id LIMIT 200""", (cohort["payload"]["operation"], cohort["payload"]["start_high_watermark"])).fetchall()
    from v8.paid_dispatch import dispatch_events
    for row in rows:
        events = dispatch_events(connection, row["dispatch_id"])
        if len(events) != 3 or events[-1].event_type != "succeeded" or events[-1].raw_response_id is None:
            continue
        sent, terminal = events[1], events[-1]
        _require(sent.event_id == row["id"] and parse_time(terminal.created_at) <= parse_time(at), "native send/terminal identity differs")
        usage = connection.execute("SELECT * FROM provider_usage WHERE id=?", (sent.provider_usage_id,)).fetchone()
        metadata = json.loads(usage["details_json"])
        capture_release._native_sample(connection, cohort, sent=sent, metadata=metadata, at=at, members=members)
        _require(usage["request_attempts"] == 1 and metadata.get("state") == "completed", "native request has not settled its actual result")
        raw = connection.execute("SELECT * FROM provider_raw_responses WHERE id=? AND fetch_attempt_id=?",
                                 (terminal.raw_response_id, sent.fetch_attempt_id)).fetchone()
        _require(raw is not None and raw["operation"] == cohort["payload"]["operation"], "native raw lineage differs")
        assert raw is not None
        entity = raw_archive.read_response_entity(connection, raw["id"])
        capture._validate_complete_transport_receipt(metadata["transport"], entity_bytes=entity, http_status=raw["http_status"])
        contents = [r[0] for r in connection.execute("SELECT DISTINCT content_id FROM content_metric_field_facts WHERE raw_response_id=? ORDER BY content_id", (raw["id"],))]
        if not contents:
            continue
        return {"contract_version": "v25-bounded-end-to-end-v1", "status": "passed",
            "native_cohort_id": cohort_id, "native_cohort_sha256": cohort["self_sha256"],
            "fetch_attempt_ids": [sent.fetch_attempt_id], "raw_response_ids": [raw["id"]], "content_ids": contents,
            "marker_id": sent.event_id, "marker_sha256": sent.event_hash,
            "terminal_sha256": terminal.event_hash, "entity_sha256": hashlib.sha256(entity).hexdigest(),
            "checked_at": at, "coverage_complete": False}
    raise contract.ReleaseContractError("bounded E2E requires a successful native request with its own materialized field facts")


def issue_accepted(connection: sqlite3.Connection, *, deployment_id: str, candidate_id: str,
                   native_cohort_id: int, e2e_receipt_path: Path, at: str) -> dict[str, Any]:
    _writer(connection, at)
    evidence = capture_release._installed_evidence(connection, at=at)
    project = capture_release.PROJECT_ROOT
    old = connection.execute("SELECT * FROM deployment_readiness_receipts WHERE deployment_id=?", (deployment_id,)).fetchone()
    if old is not None:
        retained = json.loads(old["payload_json"])
        _require(old["status"] == "accepted" and retained.get("candidate_id") == candidate_id
                 and retained.get("native_cohort_id") == native_cohort_id
                 and retained["evidence"]["bounded_e2e"]["path"] == str(e2e_receipt_path), "accepted deployment id belongs to another proof")
        return {**contract.validate_deployment_receipt(connection, deployment_id=deployment_id,
                require_accepted=True, project_root=project), "idempotent": True,
                "ordinary_paid_authorized": False}
    candidate = evidence["deployment"]
    _require(candidate["status"] == "candidate" and candidate["deployment_id"] == candidate_id, "acceptance requires the current installed candidate")
    row = connection.execute("SELECT payload_json FROM deployment_readiness_receipts WHERE deployment_id=?", (candidate_id,)).fetchone()
    payload = json.loads(row[0])
    contract.validate_storage_policy(payload["storage_policy"], require_forecast=True)
    result = _actual_e2e(connection, cohort_id=native_cohort_id, evidence=evidence, at=at)
    from v8.raw_evidence import write_immutable_json_receipt
    _require(e2e_receipt_path.is_absolute() and not e2e_receipt_path.is_relative_to(project.resolve()), "E2E receipt must be project-external")
    if e2e_receipt_path.exists():
        retained = sealer._read_private_json(e2e_receipt_path)
        _require(parse_time(retained["checked_at"]) <= parse_time(at), "retained E2E is from the future")
        # Post-fsync/pre-commit recovery may see additional facts from this same
        # raw. Preserve the original nonempty proved subset, never widen it.
        retained_contents = retained.get("content_ids")
        _require(isinstance(retained_contents, list) and bool(retained_contents)
                 and all(item in result["content_ids"] for item in retained_contents),
                 "retained E2E facts no longer belong to the completed native raw")
        result["content_ids"] = retained_contents
        result["checked_at"] = retained["checked_at"]
    write_immutable_json_receipt(e2e_receipt_path, result, evidence_root=e2e_receipt_path.parent)
    payload.update(candidate_id=candidate_id, native_cohort_id=native_cohort_id)
    payload["evidence"]["bounded_e2e"] = _ref(e2e_receipt_path, project)
    return _append(connection, deployment_id=deployment_id, status="accepted", payload=payload, at=at, project_root=project)


def issue_deferred_acceptance(connection: sqlite3.Connection, *, deployment_id: str, candidate_id: str,
                              operations: list[str], actor: str, reason: str,
                              decision_receipt_path: Path, at: str) -> dict[str, Any]:
    """Persist the explicit user decision; do not qualify transport or open gates.

    Only the durable Writer command supplies the destination and clock. The
    installed verifier, not command parameters, supplies every runtime binding.
    """
    _writer(connection, at)
    _require(bool(deployment_id.strip()) and len(deployment_id) <= 128, "invalid deployment identity")
    _require(isinstance(operations, list) and bool(operations)
             and all(isinstance(op, str) for op in operations)
             and len(operations) == len(set(operations))
             and set(operations).issubset(capture_release.CONTINUITY_OPERATIONS), "invalid approved operations")
    for value, maximum in ((actor, 128), (reason, 2000)):
        _require(isinstance(value, str) and bool(value.strip()) and len(value) <= maximum,
                 "release decision requires its actual actor and reason")
    operations = sorted(operations)
    evidence = capture_release._installed_evidence(connection, at=at)
    project = capture_release.PROJECT_ROOT
    _require(decision_receipt_path.is_absolute() and not decision_receipt_path.is_relative_to(project.resolve()),
             "release decision must be project-external")
    runtime_bindings = {key: evidence[key] for key in ("build_sha256", "runtime_sha256", "config_sha256")}
    old = connection.execute("SELECT status,payload_json FROM deployment_readiness_receipts WHERE deployment_id=?", (deployment_id,)).fetchone()
    if old is not None:
        retained = json.loads(old[1])
        _require(old[0] == "accepted" and retained.get("acceptance_mode") == contract.DEFERRED_ACCEPTANCE
                 and retained.get("candidate_id") == candidate_id, "accepted deployment id belongs to another proof")
        validated = contract.validate_deployment_receipt(connection, deployment_id=deployment_id,
            require_accepted=True, project_root=project)
        decision = validated["release_decision"]
        _require(validated["evidence"]["release_decision"]["path"] == str(decision_receipt_path)
                 and all(decision[key] == value for key, value in {
                     "operations": operations, "actor": actor, "reason": reason,
                     "runtime_bindings": runtime_bindings, "transport_manifest": evidence["manifest"],
                 }.items()), "accepted deployment id binds another user decision or runtime")
        return {**validated, "idempotent": True, "ordinary_paid_authorized": False}
    candidate = evidence["deployment"]
    _require(candidate["status"] == "candidate" and candidate["deployment_id"] == candidate_id,
             "deferred acceptance requires the current installed candidate")
    _require(all(evidence["active"][key] == candidate["bindings"][key] for key in (
        "activation_id", "profile_id", "roster_snapshot_id", "roster_members_sha256", "activation_sha256")),
        "deferred acceptance requires the candidate's current activation")
    row = connection.execute("SELECT payload_json FROM deployment_readiness_receipts WHERE deployment_id=?", (candidate_id,)).fetchone()
    _require(row is not None, "deferred acceptance candidate is missing")
    assert row is not None
    payload = json.loads(row[0])
    contract.validate_storage_policy(payload["storage_policy"], require_forecast=True)
    build_ref = _ref(Path(os.environ.get("DCAR_LOADED_BUILD_RECEIPT", "")), project)
    _require(build_ref["sha256"] == runtime_bindings["build_sha256"], "loaded build changed before release decision")
    build = sealer._read_receipt(Path(build_ref["path"]), contract_version=sealer.SEALED_BUILD_CONTRACT)
    runtime_ref = _ref(Path(build["runtime_root_receipt"]["path"]), project)
    _require(runtime_ref["sha256"] == runtime_bindings["runtime_sha256"], "loaded runtime changed before release decision")
    decision = {"contract_version": contract.RELEASE_DECISION_CONTRACT,
        "business_e2e": "deferred_by_user", "production_rollout": "approved_by_user",
        "transport_qualification": "not_verified", "approved_target_profile": "integrated_route_v1",
        "actor": actor, "reason": reason,
        "operations": operations, "issued_at": at, "candidate_id": candidate_id,
        "candidate_receipt_sha256": candidate["receipt_sha256"], "bindings": candidate["bindings"],
        "runtime_bindings": runtime_bindings, "transport_manifest": evidence["manifest"],
        "runtime_evidence": {"build": build_ref, "runtime": runtime_ref}}
    if decision_receipt_path.exists():
        retained = sealer._read_private_json(decision_receipt_path)
        _require(parse_time(retained["issued_at"]) <= parse_time(at), "retained release decision is from the future")
        decision["issued_at"] = retained["issued_at"]
        _require(retained == decision, "immutable release decision binds another authorization or runtime")
    from v8.raw_evidence import write_immutable_json_receipt
    write_immutable_json_receipt(decision_receipt_path, decision, evidence_root=decision_receipt_path.parent)
    payload.update(acceptance_mode=contract.DEFERRED_ACCEPTANCE, candidate_id=candidate_id)
    payload["evidence"]["release_decision"] = contract.verified_reference({
        "path": str(decision_receipt_path), "sha256": sealer._sha256_file(decision_receipt_path),
        "result": "deferred_by_user"}, project_root=project)
    return _append(connection, deployment_id=deployment_id, status="accepted", payload=payload, at=at, project_root=project)


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--deployment-id", required=True)
    parser.add_argument("--project-root", type=Path, required=True)
    parser.add_argument("--preinstall-build-receipt", type=Path, required=True)
    parser.add_argument("--install-receipt", type=Path, required=True)
    parser.add_argument("--storage-receipt", type=Path, required=True)
    args = parser.parse_args()
    installed = load_installed_writer_contract(required=True)
    assert installed is not None
    access = resolve_installed_database_access(DatabaseAccessMode.WRITER, database=installed.database,
        project_root=args.project_root, installed=installed,
        environ={"DCAR_PROJECT_ROOT": str(installed.project_root), "DCAR_V8_DB": str(installed.database),
                 "DCAR_WRITER_LOCK": str(installed.writer_lock)})
    with acquire_writer_lock(access):
        sealer._require_no_database_holders(installed.database)
        policy = sealer._read_private_json(args.storage_receipt)
        with sqlite3.connect(installed.database) as connection:
            connection.row_factory = sqlite3.Row
            connection.execute("PRAGMA foreign_keys=ON")
            connection.execute("BEGIN IMMEDIATE")
            result = issue_candidate(connection, deployment_id=args.deployment_id, project_root=args.project_root,
                preinstall_build_receipt=args.preinstall_build_receipt, install_receipt=args.install_receipt,
                storage_policy=policy, at=now_utc())
    print(json.dumps(result, ensure_ascii=False, sort_keys=True))


if __name__ == "__main__":
    main()
