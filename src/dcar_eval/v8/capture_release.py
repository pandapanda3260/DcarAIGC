"""Installed schema20 release evidence and fail-closed issuance boundaries.

No function invokes a provider. Candidate continuity is a fixed natural-work
allowlist, never ordinary qualification or whole-day readiness. Source19
qualification remains a transport_receipts(kind=qualification) record, not a
second ledger and not a primary route verdict.

Continuity and native renewal freeze existing legacy natural scopes, never
create work and never substitute integrated work.
"""
from __future__ import annotations

import hashlib
import importlib.util
import json
import os
import sqlite3
import sys
from datetime import timedelta
from pathlib import Path
from typing import Any, Mapping

from . import capture_authorizations as auth, forward_recovery, provider_budget
from . import paid_dispatch, paid_drain, raw_archive
from .metric_field_facts import utc
from .profile_activations import activation_at
from .runtime_database import load_installed_writer_contract, require_current_process_writer_lock
from .source_routing import parse_time
from .storage import PROJECT_ROOT
from .transport_receipts import append_transport_receipt, read_transport_receipt

CONTINUITY_CONTRACT = "capture-transport-continuity-v1"
QUALIFICATION_CONTRACT = "capture-source-operation-qualification-v1"
COHORT_CONTRACT = "capture-operation-cohort-v1"
CONTINUITY_OPERATIONS = frozenset({"douyin_user_posts", "xiaohongshu_user_posts",
    "douyin_video_detail", "douyin_video_statistics", "douyin_video_comments",
    "xiaohongshu_note_detail", "xiaohongshu_note_statistics", "xiaohongshu_note_comments"})
_PERMIT_KEYS = ("provider", "operation", "qualification_sha256", "build_sha256", "config_sha256",
                "start_high_watermark", "max_starts", "expires_at", "created_at", "payload_json")


def _transport_code() -> dict[str, str]:
    result = {}
    for relative in ("src/dcar_eval/v8/provider_transport.py", "src/dcar_eval/tikhub_config.py"):
        path = PROJECT_ROOT / relative
        _require(path.is_file() and not path.is_symlink(), "Transport implementation is unavailable")
        result[relative] = hashlib.sha256(path.read_bytes()).hexdigest()
    return result


def _require(condition: bool, message: str) -> None:
    if not condition:
        raise auth.AuthorizationError(message)


def _object(value: str) -> dict[str, Any]:
    result = json.loads(value)
    _require(isinstance(result, dict), "Release evidence must be an object")
    return dict(result)


def _private_json(reference: Mapping[str, Any], *, allow_schema20_migration: bool = False) -> dict[str, Any]:
    from .raw_evidence import _read_single_regular
    from .receipt_sizes import receipt_read_limit, validate_receipt_size
    path = Path(str(reference.get("path", "")))
    _require(path.is_absolute() and path.resolve(strict=True) == path and not path.is_symlink()
             and not path.is_relative_to(PROJECT_ROOT.resolve()), "Release evidence must be private and external")
    body = _read_single_regular(path, max_bytes=receipt_read_limit(allow_schema20_migration=allow_schema20_migration))
    _require(hashlib.sha256(body).hexdigest() == reference.get("sha256"), "Release evidence SHA differs")
    value = _object(body.decode())
    try:
        validate_receipt_size(value, len(body), allow_schema20_migration=allow_schema20_migration)
    except ValueError as error:
        raise auth.AuthorizationError(str(error)) from error
    return value


def _release_tools() -> Any:
    # Load only the installed, inventory-verified tool; never a caller path.
    name = "_dcar_capture_release_validator"
    if name not in sys.modules:
        sealer_path = PROJECT_ROOT / "scripts/seal_r0_receipts.py"
        if "seal_r0_receipts" not in sys.modules:
            spec = importlib.util.spec_from_file_location("seal_r0_receipts", sealer_path)
            _require(spec is not None and spec.loader is not None, "Installed sealer is unavailable")
            assert spec is not None and spec.loader is not None
            module = importlib.util.module_from_spec(spec)
            sys.modules[spec.name] = module
            spec.loader.exec_module(module)
        spec = importlib.util.spec_from_file_location(name, PROJECT_ROOT / "scripts/v20_release_contract.py")
        _require(spec is not None and spec.loader is not None, "Installed release validator is unavailable")
        assert spec is not None and spec.loader is not None
        module = importlib.util.module_from_spec(spec)
        sys.modules[name] = module
        spec.loader.exec_module(module)
    return sys.modules[name]


def _installed_evidence(connection: sqlite3.Connection, *, at: str,
                        maintenance_only: bool = False) -> dict[str, Any]:
    require_current_process_writer_lock(connection)
    _require(connection.execute("PRAGMA user_version").fetchone()[0] == 20, "Release requires exact schema20")
    installed = load_installed_writer_contract(required=True)
    _require(installed is not None and installed.project_root.resolve() == PROJECT_ROOT.resolve(),
             "Installed writer checkout differs")
    assert installed is not None
    loaded = os.environ.get("DCAR_LOADED_BUILD_ID", "")
    _require(loaded.startswith("sha256:") and len(loaded) == 71, "Loaded sealed build is missing")
    environment = installed.payload["EnvironmentVariables"]
    # The installed wrapper forbids an inherited ID and derives it from the
    # configured receipt. Its real byte hash is verified immediately below.
    _require(isinstance(environment, dict) and not environment.get("DCAR_LOADED_BUILD_ID")
             and environment.get("DCAR_LOADED_BUILD_RECEIPT") == os.environ.get("DCAR_LOADED_BUILD_RECEIPT"),
             "Loaded process differs from installed writer")
    build_sha = loaded[7:]
    build = forward_recovery._private_receipt(Path(os.environ.get("DCAR_LOADED_BUILD_RECEIPT", "")),
                                             build_sha, "sealed-build-receipt-v1")
    runtime_sha = build["runtime_root_receipt"]["sha256"]
    live = forward_recovery._runtime_identity(connection, {
        "build_receipt_sha256": build_sha, "runtime_root_receipt_sha256": runtime_sha})
    _require(Path(live["database_path"]) == installed.database.resolve(), "Writer database is not the installed database")
    _require(build["schema_contract"]["code_schema"] == 20 and build["schema_contract"]["formal_schema"] == 20,
             "Loaded build is not an installed schema20 release")
    _require(build.get("git", {}).get("mode") == "working-tree-source-v1", "Schema20 needs its tested source archive")
    forward_recovery._successor_archive(build, live=True)
    from .capture_code_successor import current_proof
    code_successor = current_proof(connection, project_root=PROJECT_ROOT,
        build_path=Path(os.environ["DCAR_LOADED_BUILD_RECEIPT"]), at=at) if build.get("code_successor_plan") is not None else None
    validator = _release_tools()
    deployment = validator.validate_deployment_receipt(connection, project_root=PROJECT_ROOT,
                                                       maintenance_only=maintenance_only)
    _require(deployment["status"] in {"candidate", "accepted"}, "Deployment is not an install candidate")
    active = activation_at(connection, at)
    _require(active is not None, "Current activation is missing")
    assert active is not None
    activation_successor = None
    if any(active[key] != deployment["bindings"][key] for key in
           ("activation_id", "profile_id", "roster_snapshot_id", "roster_members_sha256", "activation_sha256")):
        _require(active["profile_id"] == "integrated_route_v1", "Deployment activation changed")
        from .capture_activation_release import validate_installed_activation_successor
        activation_successor = validate_installed_activation_successor(connection, source_deployment=deployment,
            current_active=active, runtime_bindings=(code_successor["origin_runtime_bindings"] if code_successor is not None else
                {"build_sha256": build_sha, "runtime_sha256": runtime_sha, "config_sha256": deployment["bindings"]["config_sha256"]}),
            manifest=forward_recovery._route(), at=at)
    lineage = build["postmigration_lineage"]
    for key, evidence_key in (("migration_receipt", "migration"), ("install_receipt", "install")):
        _require(lineage[key]["sha256"] == deployment["evidence"][evidence_key]["sha256"],
                 "Installed sealed build has another migration/install lineage")
    migration = _private_json(deployment["evidence"]["migration"], allow_schema20_migration=True)
    install = _private_json(deployment["evidence"]["install"])
    _require(install["formal_database"] == live["database_path"]
             and all(install["installed"]["file"][key] == live[f"database_{key}"] for key in ("device", "inode"))
             and migration["candidate"]["file"]["sha256"] == install["expected"]["candidate_sha256"],
             "Installed inode or migration candidate differs")
    # Deployment binds an immutable preinstall ancestor, not the build which
    # embeds that receipt: build -> deployment -> build is forbidden.
    prior_ref = lineage["previous_build_receipt"]
    ancestor = None
    seen: set[str] = set()
    for _ in range(16):
        prior_sha = str(prior_ref["sha256"])
        _require(prior_sha not in seen, "Sealed ancestor chain is cyclic")
        seen.add(prior_sha)
        previous = forward_recovery._private_receipt(Path(prior_ref["path"]), prior_sha, "sealed-build-receipt-v1")
        if prior_sha == deployment["bindings"]["build_sha256"]:
            ancestor = previous
            break
        prior_lineage = previous.get("postmigration_lineage", {})
        _require(all(prior_lineage.get(key) == lineage[key] for key in ("migration_receipt", "install_receipt")),
                 "Ancestor changes the migration/install pair")
        prior_ref = prior_lineage["previous_build_receipt"]
    _require(ancestor is not None and ancestor["schema_contract"]["formal_schema"] == 19
             and ancestor["schema_contract"]["code_schema"] == 20
             and ancestor["runtime_root_receipt"]["sha256"] == deployment["bindings"]["runtime_sha256"],
             "Deployment does not identify the preinstall ancestor")
    _require(build["source_archive"]["sha256"] == (code_successor["plan_payload"]["source_archive"]["sha256"] if code_successor is not None
             else deployment["evidence"]["source_archive"]["sha256"]),
             "Loaded source is not the deployment tested source")
    # The deployment validator rechecks live local capacity and the exact policy
    # identity for candidates as well as accepted releases. Legacy archives stay
    # readable, but they no longer define the installed retention contract.
    capacity = deployment["storage_policy"]
    manifest = forward_recovery._route()
    # Local cleanup must be able to free space and resolve a storage circuit.
    # All provider paths use the strict default and still require this gate.
    if not maintenance_only:
        provider_budget.require_storage_ready(connection)
    return {"deployment": deployment, "active": active, "build_sha256": build_sha,
            "runtime_sha256": runtime_sha, "config_sha256": deployment["bindings"]["config_sha256"],
            "manifest": manifest, "migration": migration, "install": install, "storage_policy": capacity,
            "activation_successor": activation_successor, "code_successor": code_successor}


def _source_samples(connection: sqlite3.Connection, *, operation: str, high_watermark: int,
                    release_id: int, manifest: Mapping[str, Any], at: str, raw_root: Path,
                    native_cohort: Mapping[str, Any] | None = None) -> dict[str, Any]:
    """Recompute the first 200 actual ordinary starts, never a supplied ID list."""
    from .capture import _validate_complete_transport_receipt
    from .paid_identity import build_paid_request_identity
    from .raw_evidence import read_raw_evidence
    rows = connection.execute("""SELECT id,dispatch_id FROM paid_provider_dispatch_events
        WHERE id>? AND lower(provider)='tikhub' AND operation=? AND event_type='send_marked'
        ORDER BY id LIMIT 200""", (high_watermark, operation)).fetchall()
    _require(len(rows) == 200, "Source operation has fewer than 200 fixed natural starts")
    native_members = _native_members(connection, native_cohort) if native_cohort is not None else None
    evidence = []
    identities: set[str] = set()
    for row in rows:
        events = paid_dispatch.dispatch_events(connection, row["dispatch_id"])
        sent, terminal = events[1], events[-1]
        _require(sent.event_id == row["id"] and terminal.event_type == "succeeded"
                 and sent.permit_event_id == release_id and sent.scheduler_run_id is not None
                 and terminal.raw_response_id is not None and parse_time(terminal.created_at) <= parse_time(at),
                 "Source fixed sample lacks a successful ordinary dispatch chain")
        usage = connection.execute("SELECT * FROM provider_usage WHERE id=?", (sent.provider_usage_id,)).fetchone()
        metadata = _object(usage["details_json"])
        document = metadata.get("paid_identity", {})
        request = build_paid_request_identity(**{key: document[key] for key in (
            "provider", "operation", "platform", "subject", "request_parameters", "cursor", "due_bucket", "request_window")})
        _require(request.document == document and request.scope_identity == metadata.get("paid_scope_identity")
                 and request.scope_identity not in identities and document["operation"] == operation,
                 "Source sample paid identity is missing, duplicated or changed")
        identities.add(request.scope_identity)
        if native_cohort is not None:
            _native_sample(connection, native_cohort, sent=sent, metadata=metadata, at=at, members=native_members)
        owner = connection.execute("SELECT job_id,scheduled_for FROM scheduler_runs WHERE id=?",
                                   (sent.scheduler_run_id,)).fetchone()
        _require(owner is not None and not str(owner["job_id"]).startswith(("transport", "operator", "manual"))
                 and not metadata.get("diagnostic_member") and metadata.get("paid_sequence", 0) == 0
                 and usage["request_attempts"] == 1 and metadata.get("state") == "completed",
                 "Source sample is diagnostic, repeated, or not naturally owned")
        transport = metadata.get("transport", {})
        _require(all(transport.get(key) == manifest[key] for key in
                     ("request_host", "transport_route_id", "route_generation", "http_stack")),
                 "Source sample transport differs from continuity route")
        raw = connection.execute("SELECT * FROM provider_raw_responses WHERE id=?", (terminal.raw_response_id,)).fetchone()
        _require(raw is not None and raw["fetch_attempt_id"] == sent.fetch_attempt_id
                 and raw["operation"] == operation and raw["provider"].lower() == "tikhub", "Source raw lineage differs")
        if connection.execute("PRAGMA user_version").fetchone()[0] == 20:
            entity = raw_archive.read_response_entity(connection, raw["id"])
        else:
            path = Path(raw["local_path"])
            entity = read_raw_evidence(path if path.is_absolute() else raw_root / path,
                expected_stored_sha256=raw["sha256"], expected_stored_size=raw["byte_size"]).entity_bytes
        _validate_complete_transport_receipt(transport, entity_bytes=entity, http_status=raw["http_status"])
        evidence.append({"marker_id": sent.event_id, "marker_hash": sent.event_hash,
                         "terminal_hash": terminal.event_hash, "raw_id": raw["id"], "raw_sha256": raw["sha256"]})
    return {"sample_count": 200, "complete_count": 200, "uncertain_count": 0,
            "wilson_upper_95": 0.018846005918320894, "samples_sha256": auth.digest(evidence),
            "first_marker_id": rows[0]["id"], "last_marker_id": rows[-1]["id"],
            "last_response_at": terminal.created_at}


def freeze_operation_cohort(connection: sqlite3.Connection, *, operation: str, at: str,
                            mirror_root: Path, after_qualification_id: int | None = None) -> dict[str, Any]:
    """Freeze before collection; neither sample IDs nor a retrospective watermark are accepted."""
    _require(connection.in_transaction, "Cohort freeze requires a writer transaction")
    require_current_process_writer_lock(connection)
    version = connection.execute("PRAGMA user_version").fetchone()[0]
    if version == 20:
        return _freeze_native_cohort(connection, operation=operation, at=at,
            mirror_root=mirror_root, after_qualification_id=after_qualification_id)
    _require(version == 19 and operation in provider_budget.PRICES_MICROUSD,
             "native_operation_cohort_unsupported: only the source19 collector is implemented")
    active = activation_at(connection, at)
    _require(active is not None, "Cohort requires an active profile")
    state = paid_drain.dispatch_state(connection, at=at)
    _require(state.paid_dispatch_open and state.permit_event_id is not None, "Cohort requires the current immutable RELEASE")
    paid_drain.require_paid_dispatch_open(connection, provider="tikhub", operation=operation, at=at)
    prior = connection.execute("""SELECT id FROM scheduler_runs WHERE job_id='transport_receipt:cohort'
        AND json_extract(details_json,'$.payload.contract_version')=?
        AND json_extract(details_json,'$.payload.operation')=?
        AND json_extract(details_json,'$.payload.schema_version')=? ORDER BY id DESC LIMIT 1""",
        (COHORT_CONTRACT, operation, version)).fetchone()
    if prior is not None and after_qualification_id is None:
        return read_transport_receipt(connection, prior["id"])
    if after_qualification_id is not None:
        qualification = read_transport_receipt(connection, after_qualification_id)
        _require(prior is not None and qualification["kind"] == "qualification"
                 and qualification["payload"].get("cohort_receipt_id") == prior["id"]
                 and qualification["payload"].get("schema_version") == version
                 and qualification["payload"].get("operation") == operation,
                 "Next cohort must follow the previously frozen qualified cohort")
        high = int(qualification["payload"]["last_marker_id"])
    else:
        high = connection.execute("SELECT coalesce(max(id),0) FROM paid_provider_dispatch_events WHERE event_type='send_marked' AND lower(provider)='tikhub' AND operation=?",
                                  (operation,)).fetchone()[0]
    payload = {"contract_version": COHORT_CONTRACT, "schema_version": version, "operation": operation,
        "sample_size": 200, "start_high_watermark": high, "release_event_id": state.permit_event_id,
        "transport_manifest": forward_recovery._route(), "transport_code_sha256": _transport_code(),
        "source_build_id": os.environ.get("DCAR_LOADED_BUILD_ID"), "active": active,
        "after_qualification_id": after_qualification_id}
    return append_transport_receipt(connection, kind="cohort", identity_key=f"operation:{version}:{operation}:{high}",
                                    payload=payload, at=at, mirror_root=mirror_root)


def _cohort_samples(connection: sqlite3.Connection, cohort: Mapping[str, Any], *, at: str,
                    raw_root: Path) -> dict[str, Any]:
    payload = cohort["payload"]
    _require(cohort["kind"] == "cohort" and payload.get("contract_version") == COHORT_CONTRACT
             and payload.get("sample_size") == 200 and payload.get("transport_code_sha256") == _transport_code(),
             "Operation samples have no prior fixed cohort or transport changed")
    first = connection.execute("""SELECT created_at FROM paid_provider_dispatch_events WHERE id>? AND event_type='send_marked'
        AND lower(provider)='tikhub' AND operation=? ORDER BY id LIMIT 1""",
        (payload["start_high_watermark"], payload["operation"])).fetchone()
    _require(first is None or parse_time(first[0]) >= parse_time(cohort["recorded_at"]),
             "Operation cohort was retrospectively selected after its first start")
    return _source_samples(connection, operation=payload["operation"], high_watermark=payload["start_high_watermark"],
        release_id=payload["release_event_id"], manifest=payload["transport_manifest"], at=at, raw_root=raw_root,
        native_cohort=cohort if payload.get("schema_version") == 20 else None)


def _record_qualification(connection: sqlite3.Connection, *, cohort_receipt_id: int,
                          at: str, mirror_root: Path, version: int) -> dict[str, Any]:
    _require(version == 19, "Source qualification requires schema19")
    _require(connection.in_transaction, "Qualification requires a writer transaction")
    require_current_process_writer_lock(connection)
    _require(connection.execute("PRAGMA user_version").fetchone()[0] == version, "Qualification schema differs")
    cohort = read_transport_receipt(connection, cohort_receipt_id)
    frozen = cohort["payload"]
    _require(frozen.get("schema_version") == version, "Cohort belongs to another schema")
    operation = frozen["operation"]
    state = paid_drain.dispatch_state(connection, at=at)
    _require(state.paid_dispatch_open and state.permit_event_id == frozen["release_event_id"], "Source qualification release changed")
    paid_drain.require_paid_dispatch_open(connection, provider="tikhub", operation=operation, at=at)
    manifest = forward_recovery._route()
    _require(manifest == frozen["transport_manifest"] and os.environ.get("DCAR_LOADED_BUILD_ID") == frozen["source_build_id"],
             "Operation cohort runtime changed")
    samples = _cohort_samples(connection, cohort, at=at, raw_root=PROJECT_ROOT)
    payload = {"contract_version": QUALIFICATION_CONTRACT, "operation": operation,
        "schema_version": version, "cohort_receipt_id": cohort_receipt_id, "cohort_receipt_sha256": cohort["self_sha256"],
        "start_high_watermark": frozen["start_high_watermark"], "release_event_id": frozen["release_event_id"],
        "transport_manifest": manifest, "source_build_id": os.environ.get("DCAR_LOADED_BUILD_ID"),
        "transport_code_sha256": _transport_code(),
        "expires_at": utc((parse_time(samples["last_response_at"]) + timedelta(hours=24)).isoformat()), **samples}
    _require(parse_time(at) < parse_time(payload["expires_at"]), "Collected operation sample has already expired")
    return append_transport_receipt(connection, kind="qualification",
        identity_key=f"operation:{version}:{operation}:{cohort_receipt_id}", payload=payload, at=at, mirror_root=mirror_root)


def record_source_operation_qualification(connection: sqlite3.Connection, *, cohort_receipt_id: int,
    at: str, mirror_root: Path) -> dict[str, Any]:
    return _record_qualification(connection, cohort_receipt_id=cohort_receipt_id, at=at, mirror_root=mirror_root, version=19)


def record_native_operation_qualification(connection: sqlite3.Connection, *, cohort_receipt_id: int,
    at: str, mirror_root: Path) -> dict[str, Any]:
    """Qualify only the latest immutable native cohort, using its actual 200 starts."""
    _require(connection.in_transaction, "Qualification requires a writer transaction")
    evidence = _installed_evidence(connection, at=at)
    cohort = read_transport_receipt(connection, cohort_receipt_id)
    operation = cohort["payload"].get("operation")
    _check_native_cohort(connection, cohort, evidence=evidence, at=at, require_unexpired=False)
    existing = _latest_native_qualification(connection, operation=operation, activation_id=evidence["active"]["activation_id"])
    if existing is not None and existing["payload"].get("cohort_receipt_id") == cohort_receipt_id:
        return _native_qualification(connection, existing, cohort=cohort, evidence=evidence, at=at)
    samples = _cohort_samples(connection, cohort, at=at, raw_root=PROJECT_ROOT)
    frozen = cohort["payload"]
    payload = {"contract_version": QUALIFICATION_CONTRACT, "schema_version": 20, "operation": operation,
        "cohort_receipt_id": cohort_receipt_id, "cohort_receipt_sha256": cohort["self_sha256"],
        "release_event_id": frozen["release_event_id"], "start_high_watermark": frozen["start_high_watermark"],
        "transport_manifest": evidence["manifest"], "transport_code_sha256": _transport_code(),
        "runtime_bindings": frozen["runtime_bindings"],
        "expires_at": utc((parse_time(samples["last_response_at"]) + timedelta(hours=24)).isoformat()), **samples}
    _require(parse_time(at) < parse_time(payload["expires_at"]), "Collected native sample has already expired")
    return append_transport_receipt(connection, kind="qualification", identity_key=f"operation:20:{operation}:{cohort_receipt_id}",
        payload=payload, at=at, mirror_root=mirror_root)


def _qualification(connection: sqlite3.Connection, evidence: Mapping[str, Any], *, receipt_id: int,
                   operation: str, at: str) -> dict[str, Any]:
    receipt = read_transport_receipt(connection, receipt_id)
    payload = receipt["payload"]
    _require(receipt["kind"] == "qualification" and payload.get("contract_version") == QUALIFICATION_CONTRACT
             and payload.get("schema_version") == 19 and payload.get("operation") == operation
             and payload.get("transport_manifest") == evidence["manifest"]
             and payload.get("transport_code_sha256") == _transport_code()
             and parse_time(receipt["recorded_at"]) <= parse_time(at) < parse_time(payload["expires_at"]),
             "Source qualification is missing, expired or not operation-qualified")
    backup_ref = evidence["deployment"]["evidence"]["rollback"]
    backup = _private_json(backup_ref)
    source_path = Path(backup["backup_path"])
    with sqlite3.connect(f"{source_path.as_uri()}?mode=ro&immutable=1", uri=True) as source:
        source.row_factory = sqlite3.Row
        _require(source.execute("PRAGMA user_version").fetchone()[0] == 19, "Qualification source is not sealed schema19")
        old = read_transport_receipt(source, receipt_id)
        _require(old == receipt, "Qualification did not exist in the immutable migration source")
        cohort = read_transport_receipt(source, payload["cohort_receipt_id"])
        _require(cohort["self_sha256"] == payload["cohort_receipt_sha256"], "Source qualification cohort changed")
        sample = _cohort_samples(source, cohort, at=receipt["recorded_at"],
            raw_root=Path(evidence["migration"]["legacy_raw"]["legacy_project_root"]["path"]))
        _require(all(payload.get(key) == value for key, value in sample.items()), "Source qualification sample evidence changed")
        original_release = source.execute("SELECT event_hash FROM pipeline_paid_drain_events WHERE id=? AND event_type='release'",
                                          (payload["release_event_id"],)).fetchone()
    current_release = connection.execute("SELECT event_hash FROM pipeline_paid_drain_events WHERE id=? AND event_type='release'",
                                         (payload["release_event_id"],)).fetchone()
    _require(original_release is not None and current_release is not None and tuple(original_release) == tuple(current_release),
             "Migration source RELEASE was not preserved")
    return receipt


def _permit(connection: sqlite3.Connection, *, operation: str, at: str,
            evidence: Mapping[str, Any]) -> tuple[dict[str, Any], dict[str, Any]]:
    _require(evidence["active"]["profile_id"] == "tikhub_managed_v1",
             "Continuity authorizes Mode B legacy only; integrated capture is not qualified")
    _require(operation in CONTINUITY_OPERATIONS, "legacy_continuity_operation_unsupported")
    row = connection.execute("SELECT * FROM transport_continuity_permits WHERE provider='tikhub' AND operation=? ORDER BY id DESC LIMIT 1", (operation,)).fetchone()
    _require(row is not None, "Operation continuity permit is missing")
    assert row is not None
    permit = dict(row)
    _require(permit["permit_sha256"] == auth.digest({key: permit[key] for key in _PERMIT_KEYS}), "Continuity permit hash differs")
    payload = _object(permit["payload_json"])
    _require(payload.get("contract") == CONTINUITY_CONTRACT and payload.get("source") == "legacy_natural_due"
             and permit["max_starts"] == 20 and parse_time(permit["created_at"]) <= parse_time(at) < parse_time(permit["expires_at"])
             and permit["build_sha256"] == evidence["build_sha256"] and permit["config_sha256"] == evidence["config_sha256"]
             and payload.get("runtime_sha256") == evidence["runtime_sha256"]
             and payload.get("transport_manifest") == evidence["manifest"], "Continuity permit expired or runtime changed")
    _require(payload.get("active") == {key: evidence["active"][key] for key in
        ("activation_id", "profile_id", "roster_snapshot_id", "roster_members_sha256", "activation_sha256")}, "Continuity activation changed")
    qualification = _qualification(connection, evidence, receipt_id=payload["qualification_receipt_id"], operation=operation, at=at)
    _require(qualification["self_sha256"] == permit["qualification_sha256"], "Continuity source qualification differs")
    members = [dict(item) for item in connection.execute("SELECT rank,request_scope_identity FROM transport_continuity_permit_members WHERE permit_id=? ORDER BY rank", (permit["id"],))]
    proofs = payload.get("natural_members")
    _require([item["rank"] for item in members] == list(range(1, 21)) and auth.digest(members) == payload.get("members_sha256")
             and isinstance(proofs, list) and len(proofs) == 20, "Fixed continuity membership changed")
    assert isinstance(proofs, list)
    for member, proof in zip(members, proofs):
        _require(member["request_scope_identity"] == proof.get("paid_scope_identity")
                 and auth.digest(proof.get("request_document")) == member["request_scope_identity"]
                 and proof.get("operation") == operation and proof.get("sequence") == 0,
                 "Fixed continuity natural identity differs")
    return permit, payload


def _latest_native(connection: sqlite3.Connection, *, operation: str, kind: str,
                    activation_id: int | None = None) -> dict[str, Any] | None:
    row = connection.execute("""SELECT id FROM scheduler_runs WHERE job_id=?
        AND json_extract(details_json,'$.payload.schema_version')=20
        AND json_extract(details_json,'$.payload.operation')=?
        AND (? IS NULL OR json_extract(details_json,'$.payload.runtime_bindings.active.activation_id')=?)
        ORDER BY id DESC LIMIT 1""", (f"transport_receipt:{kind}", operation, activation_id, activation_id)).fetchone()
    return read_transport_receipt(connection, row[0]) if row is not None else None


def _latest_native_qualification(connection: sqlite3.Connection, *, operation: str,
                                 activation_id: int | None = None) -> dict[str, Any] | None:
    return _latest_native(connection, operation=operation, kind="qualification", activation_id=activation_id)


def _native_runtime(evidence: Mapping[str, Any]) -> dict[str, Any]:
    return {"active": {key: evidence["active"][key] for key in
        ("activation_id", "profile_id", "roster_snapshot_id", "roster_members_sha256", "activation_sha256")},
        **{key: evidence[key] for key in ("build_sha256", "runtime_sha256", "config_sha256")}}


def _native_members(connection: sqlite3.Connection, cohort: Mapping[str, Any]) -> list[dict[str, Any]]:
    """The immutable first-200 stream, frozen one naturally available rank at a time."""
    rows = connection.execute("""SELECT id FROM scheduler_runs WHERE job_id='transport_receipt:member_permit'
        AND json_extract(details_json,'$.payload.contract')='capture-native-member-v1'
        AND json_extract(details_json,'$.payload.cohort_id')=? ORDER BY id""", (cohort["receipt_id"],)).fetchall()
    members = []
    for row in rows:
        receipt = read_transport_receipt(connection, row[0])
        member = dict(receipt["payload"])
        _require(member["cohort_sha256"] == cohort["self_sha256"], "Native member cohort changed")
        member["recorded_at"] = receipt["recorded_at"]
        members.append(member)
    _require(len(members) <= 200 and [item["rank"] for item in members] == list(range(1, len(members)+1))
             and len({item["proof"]["paid_scope_identity"] for item in members}) == len(members), "Native fixed rank stream changed")
    return members


def _native_control(connection: sqlite3.Connection, evidence: Mapping[str, Any], *, at: str) -> int:
    """Validate the unchanged RELEASE without recursing through an expired gate."""
    profile = evidence["active"]["profile_id"]
    _require(profile in {"tikhub_managed_v1", "integrated_route_v1"}, "Native qualification requires an installed TikHub profile")
    if profile == "integrated_route_v1":
        _require(evidence.get("activation_successor") is not None, "Integrated native qualification requires its legal installed successor")
    state = paid_drain.dispatch_state(connection, at=at)
    _require(state.paid_dispatch_open and state.activation_id == evidence["active"]["activation_id"]
             and state.permit_event_id is not None, "A drain or changed activation blocks native collection")
    row = connection.execute("SELECT payload_json FROM pipeline_paid_drain_events WHERE id=? AND event_type='release'",
                             (state.permit_event_id,)).fetchone()
    _require(row is not None, "Native collection has no current RELEASE")
    control = _object(row[0]).get("control", {})
    if forward_recovery.is_forward_release(control):
        forward_recovery.validate_forward_release(connection, active=evidence["active"], release_control=control,
                                                   at=at, check_runtime=False)
        _require(control["route_control"]["selected_route"] == evidence["manifest"], "Current RELEASE transport differs")
    assert state.permit_event_id is not None
    return state.permit_event_id


def _check_native_cohort(connection: sqlite3.Connection, cohort: Mapping[str, Any], *, evidence: Mapping[str, Any],
                         at: str, require_unexpired: bool = True) -> None:
    payload = cohort["payload"]
    latest = _latest_native(connection, operation=payload.get("operation", ""), kind="cohort",
        activation_id=evidence["active"]["activation_id"])
    _require(latest is not None and latest["receipt_id"] == cohort["receipt_id"] and cohort["kind"] == "cohort"
             and payload.get("contract_version") == COHORT_CONTRACT and payload.get("schema_version") == 20
             and payload.get("source") == "native_legacy_natural_due" and payload.get("sample_size") == 200
             and payload.get("operation") in CONTINUITY_OPERATIONS, "Native cohort is missing, superseded or unsupported")
    _require(payload.get("runtime_bindings") == _native_runtime(evidence)
             and payload.get("transport_manifest") == evidence["manifest"]
             and payload.get("transport_code_sha256") == _transport_code(), "Native cohort runtime or route changed")
    _require(parse_time(cohort["recorded_at"]) <= parse_time(at)
             and (not require_unexpired or parse_time(at) < parse_time(payload["expires_at"])), "Native cohort expired or not yet issued")
    _require(payload["release_event_id"] == _native_control(connection, evidence, at=at), "Native RELEASE changed")
    _require(payload.get("selection_rule") == "existing-legacy-A-admission-order-v1", "Native selection rule changed")
    for member in _native_members(connection, cohort):
        proof = member["proof"]
        _require(auth.digest(proof.get("request_document")) == proof.get("paid_scope_identity")
                 and proof.get("operation") == payload["operation"] and proof.get("sequence") == 0,
                 "Native frozen identity changed")
        if evidence["active"]["profile_id"] == "integrated_route_v1":
            from .capture_integrated_natural_due import native_route
            route_id = native_route(connection, proof, at=at)
        else:
            route_id = _legacy_route(connection, proof, at=at)["id"]
        _require(route_id == member["legacy_assignment_id"], "Native route generation changed")


def _freeze_native_cohort(connection: sqlite3.Connection, *, operation: str, at: str,
                          mirror_root: Path, after_qualification_id: int | None) -> dict[str, Any]:
    from . import providers
    evidence = _installed_evidence(connection, at=at)
    _require(operation in CONTINUITY_OPERATIONS, "Native operation is not a supported legacy request")
    release_id = _native_control(connection, evidence, at=at)
    prior = _latest_native(connection, operation=operation, kind="cohort", activation_id=evidence["active"]["activation_id"])
    qualification = _latest_native_qualification(connection, operation=operation, activation_id=evidence["active"]["activation_id"])
    if after_qualification_id is not None:
        _require(qualification is not None and qualification["receipt_id"] == after_qualification_id,
                 "Renewal must follow the latest qualification, never an older batch")
    if prior is not None:
        prior_payload = prior["payload"]
        completed = qualification is not None and qualification["payload"].get("cohort_receipt_id") == prior["receipt_id"]
        same_runtime = prior_payload.get("runtime_bindings") == _native_runtime(evidence) and prior_payload.get("transport_manifest") == evidence["manifest"]
        if not completed and same_runtime and parse_time(at) < parse_time(prior_payload["expires_at"]):
            _check_native_cohort(connection, prior, evidence=evidence, at=at)
            # An unfinished fixed cohort is immutable, including failed members.
            return prior
    transport = providers._freeze_tikhub_transport()
    _require(transport is not None and transport["manifest"] == evidence["manifest"], "Native transport changed")
    high = connection.execute("SELECT coalesce(max(id),0) FROM paid_provider_dispatch_events WHERE event_type='send_marked' AND lower(provider)='tikhub' AND operation=?", (operation,)).fetchone()[0]
    payload = {"contract_version": COHORT_CONTRACT, "schema_version": 20, "source": "native_legacy_natural_due",
        "operation": operation, "sample_size": 200, "start_high_watermark": high, "release_event_id": release_id,
        "runtime_bindings": _native_runtime(evidence), "transport_manifest": evidence["manifest"],
        "transport_code_sha256": _transport_code(), "request_transport": transport,
        "selection_rule": "existing-legacy-A-admission-order-v1",
        "previous_cohort_id": prior["receipt_id"] if prior else None,
        "after_qualification_id": qualification["receipt_id"] if qualification else None,
        "expires_at": utc((parse_time(at)+timedelta(hours=24)).isoformat())}
    receipt = append_transport_receipt(connection, kind="cohort",
        identity_key=f"operation:20:{operation}:{high}:{auth.digest(payload)}", payload=payload, at=at, mirror_root=mirror_root)
    _publish_gate(connection, operation=operation, at=at, ordinary=False)
    return receipt


def _freeze_native_member(connection: sqlite3.Connection, cohort: Mapping[str, Any], *, rank: int, at: str,
                          request_identity: str | None = None) -> None:
    from .transport_due_candidates import list_legacy_due_candidates
    _require(connection.in_transaction, "Native member freeze requires a writer transaction")
    members = _native_members(connection, cohort)
    if rank <= len(members):
        return  # Reentry can only reuse this immutable rank; never choose a substitute.
    _require(rank == len(members)+1 and rank <= 200, "Native rank cannot skip the fixed stream")
    payload = cohort["payload"]
    if members:
        previous = members[-1]["proof"]["paid_scope_identity"]
        terminal = connection.execute("""SELECT d.event_type FROM provider_request_start_events e
            JOIN provider_paid_scope_claims c ON c.id=e.request_scope_claim_id
            JOIN paid_provider_dispatch_events sent ON sent.id=e.provider_send_marker_id
            JOIN paid_provider_dispatch_events d ON d.dispatch_id=sent.dispatch_id
            WHERE c.scope_identity=? AND c.sequence=0 ORDER BY d.id DESC LIMIT 1""", (previous,)).fetchone()
        _require(terminal is not None and terminal[0] in {"succeeded", "failed", "billing_unknown"}, "Previous native rank has no terminal actual start")
    if payload["runtime_bindings"]["active"]["profile_id"] == "integrated_route_v1":
        _require(request_identity is not None, "Integrated native members can only be frozen by their existing A transaction")
        assert request_identity is not None
        from .capture_integrated_natural_due import validate_native_due_request
        candidates = [{"proof": validate_native_due_request(connection, request_identity=request_identity, operation=payload["operation"], at=at)}]
    else:
        candidates = list_legacy_due_candidates(connection, operation=payload["operation"], at=at)
    for candidate in candidates:
        proof = candidate["proof"]
        if request_identity is not None and proof["paid_scope_identity"] != request_identity:
            continue  # A already owns this exact natural request, not a caller-selected video.
        if payload["runtime_bindings"]["active"]["profile_id"] == "integrated_route_v1":
            from .capture_integrated_natural_due import native_route
            route_id = native_route(connection, proof, at=at)
        else:
            route_id = _legacy_route(connection, proof, at=at)["id"]
        identity = proof["paid_scope_identity"]
        if connection.execute("SELECT 1 FROM provider_usage WHERE json_extract(details_json,'$.paid_scope_identity')=? LIMIT 1", (identity,)).fetchone():
            continue
        if connection.execute("SELECT 1 FROM provider_paid_scope_claims WHERE scope_identity=?", (identity,)).fetchone():
            continue
        append_transport_receipt(connection, kind="member_permit", identity_key=f"native:{cohort['receipt_id']}:{rank}",
            payload={"contract": "capture-native-member-v1", "cohort_id": cohort["receipt_id"],
                "cohort_sha256": cohort["self_sha256"], "rank": rank, "proof": proof, "legacy_assignment_id": route_id},
            at=at, mirror_root=Path(cohort["mirror"]["path"]).parent)
        return
    raise auth.AuthorizationError("missing_frozen_work: no existing unpurchased legacy request is currently due")


def _native_qualification(connection: sqlite3.Connection, receipt: Mapping[str, Any], *, cohort: Mapping[str, Any],
                          evidence: Mapping[str, Any], at: str) -> dict[str, Any]:
    payload = receipt["payload"]
    _require(receipt["kind"] == "qualification" and payload.get("contract_version") == QUALIFICATION_CONTRACT
             and payload.get("schema_version") == 20 and payload.get("operation") == cohort["payload"]["operation"]
             and payload.get("cohort_receipt_id") == cohort["receipt_id"] and payload.get("cohort_receipt_sha256") == cohort["self_sha256"]
             and payload.get("runtime_bindings") == _native_runtime(evidence)
             and payload.get("transport_manifest") == evidence["manifest"] and payload.get("transport_code_sha256") == _transport_code()
             and parse_time(receipt["recorded_at"]) <= parse_time(at) < parse_time(payload["expires_at"]),
             "Native qualification expired, changed or belongs to an older batch")
    samples = _cohort_samples(connection, cohort, at=receipt["recorded_at"], raw_root=PROJECT_ROOT)
    _require(all(payload.get(key) == value for key, value in samples.items()), "Native sample evidence changed")
    _require(payload["expires_at"] == utc((parse_time(samples["last_response_at"])+timedelta(hours=24)).isoformat()),
             "Native qualification extended the actual sample lifetime")
    return dict(receipt)


def _native_authority(connection: sqlite3.Connection, *, operation: str, at: str,
                      evidence: Mapping[str, Any]) -> tuple[dict[str, Any], dict[str, Any]] | None:
    cohort = _latest_native(connection, operation=operation, kind="cohort", activation_id=evidence["active"]["activation_id"])
    if cohort is None:
        return None
    qualification = _latest_native_qualification(connection, operation=operation, activation_id=evidence["active"]["activation_id"])
    qualified = qualification is not None and qualification["payload"].get("cohort_receipt_id") == cohort["receipt_id"]
    _check_native_cohort(connection, cohort, evidence=evidence, at=at, require_unexpired=not qualified)
    if qualified:
        assert qualification is not None
        qualification = _native_qualification(connection, qualification, cohort=cohort, evidence=evidence, at=at)
    receipt = qualification if qualified else cohort
    assert receipt is not None
    return {"permit_sha256": receipt["self_sha256"], "expires_at": receipt["payload"]["expires_at"],
            "qualification_sha256": receipt["self_sha256"]}, {
        "native_cohort_id": cohort["receipt_id"], "native_qualified": qualified, "qualification_receipt_id": receipt["receipt_id"],
        "release_event_id": cohort["payload"]["release_event_id"]}


def validate_native_natural_request(connection: sqlite3.Connection, *, operation: str, cohort_id: int,
                                    request_identity: str, at: str, sequence: int = 0) -> dict[str, Any]:
    _require(sequence == 0, "Native fixed sampling cannot compensate or repeat a member")
    cohort = read_transport_receipt(connection, cohort_id)
    evidence = _installed_evidence(connection, at=at)
    _check_native_cohort(connection, cohort, evidence=evidence, at=at)
    _require(cohort["payload"]["operation"] == operation, "Native operation differs")
    # Existing natural owner/cursor validator is shared with fixed20, not a new owner.
    members = _native_members(connection, cohort)
    if request_identity not in {item["proof"]["paid_scope_identity"] for item in members}:
        _freeze_native_member(connection, cohort, rank=len(members)+1, at=at, request_identity=request_identity)
        members = _native_members(connection, cohort)
    payload = {**cohort["payload"], "source": "legacy_natural_due", "natural_members": [item["proof"] for item in members]}
    payload["legacy_assignment_ids"] = {item["proof"]["paid_scope_identity"]: item["legacy_assignment_id"] for item in members}
    if evidence["active"]["profile_id"] == "integrated_route_v1":
        from .capture_integrated_natural_due import validate_native_due_request
        proof = validate_native_due_request(connection, request_identity=request_identity, operation=operation, at=at)
        _require(proof in payload["natural_members"], "Integrated native A/B proof changed")
    else:
        proof = validate_continuity_natural_request(connection, permit={"payload_json": auth.canonical(payload)},
            request_identity=request_identity, at=at)
    count = connection.execute("SELECT count(*) FROM paid_provider_dispatch_events WHERE id>? AND provider='tikhub' AND operation=? AND event_type='send_marked'",
        (payload["start_high_watermark"], operation)).fetchone()[0]
    _require(count < 200, "Native fixed start cap is exhausted")
    return proof


def _native_sample(connection: sqlite3.Connection, cohort: Mapping[str, Any], *, sent: Any,
                    metadata: Mapping[str, Any], at: str, members: list[dict[str, Any]] | None = None) -> None:
    payload = cohort["payload"]
    identity = metadata.get("paid_scope_identity")
    fixed_members = members if members is not None else _native_members(connection, cohort)
    matching = [item for item in fixed_members if item["proof"]["paid_scope_identity"] == identity]
    _require(len(matching) == 1 and sent.scheduler_run_id == matching[0]["proof"]["source_run_id"]
             and parse_time(matching[0]["recorded_at"]) <= parse_time(sent.created_at)
             and parse_time(cohort["recorded_at"]) <= parse_time(sent.created_at) < parse_time(payload["expires_at"]),
             "Native start is outside the fixed membership or lifetime")
    start = connection.execute("""SELECT e.id FROM provider_request_start_events e JOIN provider_paid_scope_claims c
        ON c.id=e.request_scope_claim_id WHERE e.provider_send_marker_id=? AND c.scope_identity=? AND c.sequence=0""",
        (sent.event_id, identity)).fetchone()
    _require(start is not None, "Native sample has no actual schema20 A/B start")
    gates = connection.execute("""SELECT * FROM capture_paid_send_gate_events WHERE provider='tikhub' AND operation=?
        AND json_extract(evidence_json,'$.native_cohort_id')=? ORDER BY id""", (payload["operation"], cohort["receipt_id"])).fetchall()
    authorities = set()
    for gate in gates:
        gate_payload = _object(gate["evidence_json"])
        ready = connection.execute("SELECT * FROM provider_readiness_receipts WHERE id=?", (gate_payload["readiness_receipt_id"],)).fetchone()
        _require(gate["event_sha256"] == auth.digest({key: gate[key] for key in ("provider", "operation", "state", "reason", "evidence_json", "recorded_at")})
                 and ready is not None and ready["receipt_sha256"] == auth.digest({key: ready[key] for key in
                    ("provider", "operation", "status", "reason", "evidence_json", "created_at", "expires_at")}), "Native admission receipt changed")
        if gate["state"] == "diagnostic_only" and parse_time(gate_payload["issued_at"]) <= parse_time(sent.created_at) < parse_time(gate_payload["expires_at"]):
            ready_payload = _object(ready["evidence_json"])
            _require(ready_payload.get("qualification") == "native_candidate"
                     and gate_payload["bindings"]["continuity_permit_sha256"] == cohort["self_sha256"]
                     and ready_payload["bindings"] == gate_payload["bindings"], "Native sample was not admitted by its fixed diagnostic manifest")
            authorities.add(auth.digest({"gate_id": gate["id"], "gate_sha256": gate["event_sha256"],
                "readiness_id": ready["id"], "readiness_sha256": ready["receipt_sha256"], "bindings": gate_payload["bindings"]}))
    _require(metadata.get("authority_sha256") in authorities, "Native start has no matching actual gate admission")


def native_member(connection: sqlite3.Connection, *, cohort_id: int, rank: int, at: str,
                   local_replay: bool = False) -> tuple[dict[str, Any], dict[str, Any], list[int]]:
    """Load one fixed native member for the existing executor/local materializer."""
    _require(type(rank) is int and 1 <= rank <= 200, "Native rank must be within the fixed 200")
    cohort = read_transport_receipt(connection, cohort_id)
    evidence = _installed_evidence(connection, at=at, maintenance_only=local_replay)
    _check_native_cohort(connection, cohort, evidence=evidence, at=at)
    if not local_replay:
        _freeze_native_member(connection, cohort, rank=rank, at=at)
    payload = {**cohort["payload"], "natural_members": [item["proof"] for item in _native_members(connection, cohort)]}
    _require(rank <= len(payload["natural_members"]), "Native local rank has not been frozen")
    proof = payload["natural_members"][rank-1]
    if not local_replay:
        validate_native_natural_request(connection, operation=payload["operation"], cohort_id=cohort_id,
            request_identity=proof["paid_scope_identity"], at=at)
        return payload, proof, []
    row = connection.execute("""SELECT d.dispatch_id FROM provider_request_start_events e
        JOIN provider_paid_scope_claims c ON c.id=e.request_scope_claim_id
        JOIN paid_provider_dispatch_events d ON d.id=e.provider_send_marker_id
        WHERE c.scope_identity=? AND c.sequence=0 AND d.id>?""",
        (proof["paid_scope_identity"], payload["start_high_watermark"])).fetchone()
    _require(row is not None, "Native local replay has no actual start")
    events = paid_dispatch.dispatch_events(connection, row[0])
    _require(len(events) == 3 and events[-1].event_type == "succeeded", "Native local replay requires a successful raw terminal")
    sent, terminal = events[1:]
    usage = connection.execute("SELECT details_json FROM provider_usage WHERE id=?", (sent.provider_usage_id,)).fetchone()
    metadata = _object(usage[0])
    _native_sample(connection, cohort, sent=sent, metadata=metadata, at=at)
    from .capture import _validate_complete_transport_receipt
    raw = connection.execute("SELECT id,http_status,fetch_attempt_id FROM provider_raw_responses WHERE id=?", (terminal.raw_response_id,)).fetchone()
    _require(raw is not None and raw["fetch_attempt_id"] == sent.fetch_attempt_id, "Native local raw lineage changed")
    _validate_complete_transport_receipt(metadata["transport"], entity_bytes=raw_archive.read_response_entity(connection, raw["id"]), http_status=raw["http_status"])
    assert sent.fetch_attempt_id is not None
    return payload, proof, [sent.fetch_attempt_id]


def _legacy_route(connection: sqlite3.Connection, proof: Mapping[str, Any], *, at: str) -> dict[str, Any]:
    from .capture_planning import resolve_route
    scope = proof["scope_identity"]
    route = resolve_route(connection, account_id=scope["account_id"], content_id=scope["content_id"], operation=proof["operation"], at=at)
    _require(route is not None and route["provider"] == "tikhub" and route["route"] == "legacy" and route["mode"] == "active",
             "Continuity target is not on an active legacy route")
    assert route is not None
    return route


def validate_continuity_natural_request(connection: sqlite3.Connection, *, permit: Mapping[str, Any],
                                      request_identity: str, at: str) -> dict[str, Any]:
    """Rederive the frozen natural owner/cursor at both A and B, without writes."""
    from .paid_identity import build_paid_request_identity
    from .provider_budget import PaidScope
    from .transport_natural_due import NaturalDueError, validate_natural_due_request
    payload = _object(permit["payload_json"])
    matching = [item for item in payload.get("natural_members", []) if item.get("paid_scope_identity") == request_identity]
    _require(payload.get("source") == "legacy_natural_due" and len(matching) == 1, "Request has no frozen legacy natural proof")
    proof = matching[0]
    document = proof["request_document"]
    request = build_paid_request_identity(provider=document["provider"], operation=document["operation"],
        platform=document["platform"], subject=document["subject"], request_parameters=document["request_parameters"],
        cursor=document["cursor"], due_bucket=document["due_bucket"], request_window=document["request_window"], sequence=0)
    _require(request.scope_identity == request_identity, "Frozen natural request identity changed")
    try:
        current = validate_natural_due_request(connection, scope=PaidScope(**proof["scope_identity"]),
            request_identity=request, stage=proof["stage"], at=at)
    except NaturalDueError as error:
        raise auth.AuthorizationError(f"{error.code}: {error}") from error
    _require(current == proof, "Natural due owner, cursor, or activation changed")
    route = _legacy_route(connection, proof, at=at)
    _require(payload.get("legacy_assignment_ids", {}).get(request_identity) == route["id"], "Legacy route generation changed")
    return proof


def _authority_bindings(evidence: Mapping[str, Any], permit: Mapping[str, Any], payload: Mapping[str, Any]) -> dict[str, Any]:
    return {**{key: evidence["active"][key] for key in ("activation_id", "profile_id", "roster_snapshot_id", "roster_members_sha256")},
        "build_receipt_sha256": evidence["build_sha256"], "runtime_root_receipt_sha256": evidence["runtime_sha256"],
        "config_receipt_sha256": evidence["config_sha256"], "continuity_permit_sha256": permit["permit_sha256"]}


def current_runtime_bindings(connection: sqlite3.Connection, operation: str, at: str) -> dict[str, Any]:
    try:
        evidence = _installed_evidence(connection, at=at)
        from .capture_operator_release import authority as operator_authority
        approved = operator_authority(connection, evidence=evidence, operation=operation, at=at)
        if approved is not None:
            return dict(approved["bindings"])
        native = _native_authority(connection, operation=operation, at=at, evidence=evidence)
        if native is None and evidence["active"]["profile_id"] == "integrated_route_v1":
            from .capture_activation_release import current_runtime_bindings as successor_bindings
            return successor_bindings(connection, operation, at)
        permit, payload = native if native is not None else _permit(connection, operation=operation, at=at, evidence=evidence)
        return _authority_bindings(evidence, permit, payload)
    except auth.AuthorizationError:
        raise
    except (KeyError, TypeError, ValueError, OSError, sqlite3.Error, RuntimeError) as error:
        raise auth.AuthorizationError(f"Installed continuity evidence is invalid: {error}") from error


def validate_current_dispatch_control(connection: sqlite3.Connection, *, active: Mapping[str, Any],
    release_control: Mapping[str, Any], operation: str, at: str) -> None:
    try:
        if active["profile_id"] == "integrated_route_v1":
            # Native recovery has its own fixed manifest and must remain usable
            # when the inherited source-operation TTL has expired.
            current_runtime_bindings(connection, operation, at)
            return
        forward_recovery.validate_forward_release(connection, active=active, release_control=release_control, at=at, check_runtime=False)
        evidence = _installed_evidence(connection, at=at)
        from .capture_operator_release import authority as operator_authority
        approved = operator_authority(connection, evidence=evidence, operation=operation, at=at)
        if approved is not None:
            _require(evidence["manifest"] == release_control["route_control"]["selected_route"], "Operator RELEASE transport changed")
            return
        native = _native_authority(connection, operation=operation, at=at, evidence=evidence)
        if native is not None:
            source_release_id = native[1]["release_event_id"]
        else:
            _, payload = _permit(connection, operation=operation, at=at, evidence=evidence)
            source_release_id = read_transport_receipt(connection, payload["qualification_receipt_id"])["payload"]["release_event_id"]
        state = paid_drain.dispatch_state(connection, at=at)
        _require(state.paid_dispatch_open and state.activation_id == active["activation_id"]
                 and state.permit_event_id == source_release_id,
                 "A later drain or another source RELEASE blocks continuity")
        _require(evidence["manifest"] == release_control["route_control"]["selected_route"], "Source RELEASE transport changed")
    except auth.AuthorizationError:
        raise
    except (KeyError, TypeError, ValueError, OSError, sqlite3.Error, RuntimeError) as error:
        raise auth.AuthorizationError(f"Schema20 dispatch continuity is invalid: {error}") from error


def freeze_continuity_permit(connection: sqlite3.Connection, *, operation: str,
    qualification_receipt_id: int, at: str) -> dict[str, Any]:
    """Freeze first twenty existing unpurchased legacy scopes; never fill gaps."""
    from . import providers, transport_due_candidates
    _require(connection.in_transaction, "Continuity issuance requires a writer transaction")
    evidence = _installed_evidence(connection, at=at)
    _require(evidence["active"]["profile_id"] == "tikhub_managed_v1", "Continuity authorizes Mode B legacy only")
    _require(operation in CONTINUITY_OPERATIONS, "legacy_continuity_operation_unsupported")
    qualification = _qualification(connection, evidence, receipt_id=qualification_receipt_id, operation=operation, at=at)
    existing = connection.execute("SELECT id FROM transport_continuity_permits WHERE provider='tikhub' AND operation=?", (operation,)).fetchone()
    if existing is not None:
        permit, payload = _permit(connection, operation=operation, at=at, evidence=evidence)
        _require(payload["qualification_receipt_id"] == qualification_receipt_id, "Frozen samples cannot be replaced")
        return {"permit_id": permit["id"], "permit_sha256": permit["permit_sha256"], "ordinary_paid_authorized": False, "provider_calls": 0}
    candidates = transport_due_candidates.list_legacy_due_candidates(connection, operation=operation, at=at)
    proofs, routes = [], {}
    for candidate in candidates:
        proof = candidate["proof"]
        route = _legacy_route(connection, proof, at=at)
        identity = proof["paid_scope_identity"]
        # A previously reserved request is not a fresh natural sample, including
        # legacy unknowns that have not yet acquired a schema20 scope claim.
        if connection.execute("SELECT 1 FROM provider_usage WHERE json_extract(details_json,'$.paid_scope_identity')=? LIMIT 1", (identity,)).fetchone():
            continue
        if connection.execute("SELECT 1 FROM provider_paid_scope_claims WHERE scope_identity=?", (identity,)).fetchone():
            continue
        proofs.append(proof)
        routes[identity] = route["id"]
        if len(proofs) == 20:
            break
    _require(len(proofs) == 20, "missing_frozen_work: twenty existing unpurchased legacy requests are not available")
    members = [{"rank": rank, "request_scope_identity": proof["paid_scope_identity"]} for rank, proof in enumerate(proofs, 1)]
    _require(len(routes) == 20, "Natural due candidates contain duplicate request identities")
    transport = providers._freeze_tikhub_transport()
    _require(transport is not None and transport["manifest"] == evidence["manifest"], "Continuity transport changed")
    payload = {"contract": CONTINUITY_CONTRACT, "source": "legacy_natural_due", "qualification_receipt_id": qualification_receipt_id,
        "active": {key: evidence["active"][key] for key in ("activation_id", "profile_id", "roster_snapshot_id", "roster_members_sha256", "activation_sha256")},
        "runtime_sha256": evidence["runtime_sha256"], "transport_manifest": evidence["manifest"], "request_transport": transport,
        "natural_members": proofs, "legacy_assignment_ids": routes, "members_sha256": auth.digest(members)}
    values = {"provider": "tikhub", "operation": operation, "qualification_sha256": qualification["self_sha256"],
        "build_sha256": evidence["build_sha256"], "config_sha256": evidence["config_sha256"],
        "start_high_watermark": connection.execute("SELECT coalesce(max(id),0) FROM provider_request_start_events").fetchone()[0],
        "max_starts": 20, "expires_at": min(utc(qualification["payload"]["expires_at"]), utc((parse_time(at)+timedelta(hours=24)).isoformat())),
        "created_at": utc(at), "payload_json": auth.canonical(payload)}
    checksum = auth.digest(values)
    identifier = int(connection.execute(f"INSERT INTO transport_continuity_permits({','.join(values)},permit_sha256) VALUES ({','.join('?' for _ in range(len(values)+1))})", (*values.values(), checksum)).lastrowid or 0)
    connection.executemany("INSERT INTO transport_continuity_permit_members(permit_id,rank,request_scope_identity) VALUES(?,?,?)", [(identifier, item["rank"], item["request_scope_identity"]) for item in members])
    return {"permit_id": identifier, "permit_sha256": checksum, "ordinary_paid_authorized": False, "provider_calls": 0}


def _continuity_complete(connection: sqlite3.Connection, permit: Mapping[str, Any], *, rank: int | None = None) -> list[int]:
    from .capture import _validate_complete_transport_receipt
    from .raw_evidence import canonical_json_bytes
    manifest = _object(permit["payload_json"])["transport_manifest"]
    rows = connection.execute("""SELECT m.rank,e.id start_id,d.fetch_attempt_id,d.raw_response_id,d.dispatch_id,
        d.provider,d.operation,t.clean_eof,t.json_parse_ok,t.error_class,t.payload_json transport_json,t.receipt_sha256
        FROM transport_continuity_permit_members m
        LEFT JOIN provider_paid_scope_claims c ON c.scope_identity=m.request_scope_identity AND c.scope_kind='request' AND c.sequence=0
        LEFT JOIN provider_request_start_events e ON e.request_scope_claim_id=c.id
        LEFT JOIN paid_provider_dispatch_events sent ON sent.id=e.provider_send_marker_id
        LEFT JOIN paid_provider_dispatch_events d ON d.dispatch_id=sent.dispatch_id AND d.event_type='succeeded'
        LEFT JOIN fetch_transport_receipts t ON t.fetch_attempt_id=d.fetch_attempt_id
        WHERE m.permit_id=? AND (? IS NULL OR m.rank=?) ORDER BY m.rank""", (permit["id"], rank, rank)).fetchall()
    expected = list(range(1,21)) if rank is None else [rank]
    _require(rank is None or type(rank) is int and 1 <= rank <= 20, "Continuity rank is invalid")
    _require([row["rank"] for row in rows] == expected, "Continuity must retain exactly its fixed ranks")
    attempts = []
    for row in rows:
        _require(row["start_id"] is not None and row["start_id"] > permit["start_high_watermark"]
                 and row["raw_response_id"] is not None and row["clean_eof"] == 1
                 and row["json_parse_ok"] == 1 and row["error_class"] is None
                 and row["provider"].lower() == permit["provider"] and row["operation"] == permit["operation"],
                 "Continuity is not twenty complete successful starts")
        events = paid_dispatch.dispatch_events(connection, row["dispatch_id"])
        _require(len(events) == 3 and events[-1].event_type == "succeeded", "Continuity dispatch chain is incomplete")
        payload = _object(row["transport_json"])
        _require(hashlib.sha256(canonical_json_bytes(payload)).hexdigest() == row["receipt_sha256"]
                 and payload["fetch_attempt_id"] == row["fetch_attempt_id"]
                 and all(payload["transport"].get(key) == manifest[key] for key in
                         ("request_host", "transport_route_id", "route_generation", "http_stack")),
                 "Continuity transport bytes or route differ")
        raw = connection.execute("SELECT raw_blob_id,http_status FROM provider_raw_responses WHERE id=? AND fetch_attempt_id=?",
                                 (row["raw_response_id"], row["fetch_attempt_id"])).fetchone()
        _require(raw is not None and raw[0] is not None, "Continuity response has no committed raw blob")
        entity = raw_archive.read_blob(connection, raw[0])
        _validate_complete_transport_receipt(payload["transport"], entity_bytes=entity, http_status=raw[1])
        attempts.append(row["fetch_attempt_id"])
    return attempts


def _publish_gate(connection: sqlite3.Connection, *, operation: str, at: str, ordinary: bool) -> dict[str, Any]:
    _require(connection.in_transaction, "Gate issuance requires a writer transaction")
    evidence = _installed_evidence(connection, at=at)
    if ordinary:
        from . import capture_operator_release
        if capture_operator_release.authority(connection, evidence=evidence, operation=operation, at=at) is not None:
            return capture_operator_release.publish(connection, evidence=evidence, operation=operation, at=at)
    native = _native_authority(connection, operation=operation, at=at, evidence=evidence)
    permit, permit_payload = native if native is not None else _permit(connection, operation=operation, at=at, evidence=evidence)
    bindings = _authority_bindings(evidence, permit, permit_payload)
    expires = permit["expires_at"]
    if ordinary:
        _require(evidence["deployment"]["status"] == "accepted", "Candidate cannot enable ordinary paid capture")
        if native is None:
            _continuity_complete(connection, permit)
        else:
            _require(permit_payload["native_qualified"], "Native candidate cannot enable ordinary capture before 200 actual starts")
    elif native is not None:
        _require(not permit_payload["native_qualified"], "Completed native qualification cannot be reused as diagnostic permission")
    scope = auth.scope_hash(runtime_bindings=bindings, provider="tikhub", operation=operation)
    manifest_sha = auth.digest(evidence["manifest"])
    ready_evidence = {"contract": auth.READINESS_CONTRACT, "bindings": bindings, "scope_hash": scope,
        "qualification": "qualified" if ordinary else "native_candidate" if native is not None else "continuity_candidate",
        "continuity_permit_sha256": bindings["continuity_permit_sha256"], "transport_manifest_sha256": manifest_sha,
        "origin_continuity_permit_sha256": permit["permit_sha256"],
        "qualification_receipt_id": permit_payload["qualification_receipt_id"],
        "qualification_receipt_sha256": permit["qualification_sha256"]}
    ready = {"provider": "tikhub", "operation": operation, "status": "ready" if ordinary else "diagnostic_only",
        "reason": "continuity-qualified" if ordinary else "fixed-natural-continuity",
        "evidence_json": auth.canonical(ready_evidence), "created_at": utc(at), "expires_at": expires}
    ready_sha = auth.digest(ready)
    previous = connection.execute("SELECT id FROM provider_readiness_receipts WHERE receipt_sha256=?", (ready_sha,)).fetchone()
    if previous is None:
        cursor = connection.execute(f"INSERT INTO provider_readiness_receipts({','.join(ready)},receipt_sha256) VALUES ({','.join('?' for _ in range(len(ready)+1))})", (*ready.values(), ready_sha))
        ready_id = cursor.lastrowid
    else:
        ready_id = previous[0]
    bucket = "discovery" if operation in provider_budget.DISCOVERY_OPERATIONS else "metrics"
    payload = {"contract": auth.CONTRACT, "bindings": bindings, "scope_hash": scope, "operation": operation,
        "issued_at": utc(at), "expires_at": expires, "readiness_receipt_id": ready_id,
        "readiness_receipt_sha256": ready_sha, "transport_manifest_sha256": manifest_sha,
        "budget": {"total_microusd": provider_budget.AUTOMATIC_MICROUSD, "bucket": bucket,
                   "bucket_microusd": provider_budget.BUDGET_BUCKET_MICROUSD[bucket]}}
    if native is not None:
        payload["native_cohort_id"] = permit_payload["native_cohort_id"]
    gate = {"provider": "tikhub", "operation": operation, "state": "open" if ordinary else "diagnostic_only",
        "reason": ready["reason"], "evidence_json": auth.canonical(payload), "recorded_at": utc(at)}
    gate_sha = auth.digest(gate)
    connection.execute(f"INSERT OR IGNORE INTO capture_paid_send_gate_events({','.join(gate)},event_sha256) VALUES ({','.join('?' for _ in range(len(gate)+1))})", (*gate.values(), gate_sha))
    return {"readiness_receipt_id": ready_id, "gate_sha256": gate_sha, "qualification": ready_evidence["qualification"],
            "ordinary_paid_authorized": ordinary, "coverage_complete": False, "provider_calls": 0,
            "expires_at": expires}


def publish_continuity_gate(connection: sqlite3.Connection, *, operation: str, at: str) -> dict[str, Any]:
    return _publish_gate(connection, operation=operation, at=at, ordinary=False)


def publish_operation_gate(connection: sqlite3.Connection, *, operation: str, at: str,
                           mirror_root: Path | None = None) -> dict[str, Any]:
    return _publish_gate(connection, operation=operation, at=at, ordinary=True)


def renew_operation_gate(connection: sqlite3.Connection, *, operation: str, at: str,
                         mirror_root: Path | None = None) -> dict[str, Any]:
    """Renew from the latest verified native batch; repeated calls cannot extend TTL."""
    _require(connection.in_transaction, "Gate renewal requires a writer transaction")
    evidence = _installed_evidence(connection, at=at)
    native = _native_authority(connection, operation=operation, at=at, evidence=evidence)
    _require(native is not None and native[1]["native_qualified"], "Native operation has no completed current 200-start qualification")
    return _publish_gate(connection, operation=operation, at=at, ordinary=True)


def snapshot_operation_qualification(connection: sqlite3.Connection, operation: str, at: str) -> dict[str, Any]:
    """Freeze valid source evidence for a separately authorized profile successor."""
    evidence = _installed_evidence(connection, at=at)
    _require(evidence["deployment"]["status"] == "accepted", "Qualification snapshot requires accepted deployment")
    from . import capture_operator_release
    if capture_operator_release.authority(connection, evidence=evidence, operation=operation, at=at) is not None:
        return capture_operator_release.snapshot(connection, evidence=evidence, operation=operation, at=at)
    native = _native_authority(connection, operation=operation, at=at, evidence=evidence)
    permit, payload = native if native is not None else _permit(connection, operation=operation, at=at, evidence=evidence)
    _require(native is None or payload["native_qualified"], "Diagnostic qualification cannot authorize a successor")
    if native is None:
        _continuity_complete(connection, permit)
    bindings = _authority_bindings(evidence, permit, payload)
    gate = connection.execute("SELECT * FROM capture_paid_send_gate_events WHERE provider='tikhub' AND operation=? ORDER BY id DESC LIMIT 1", (operation,)).fetchone()
    _require(gate is not None and gate["state"] == "open", "Source ordinary gate is not open")
    gate_payload = _object(gate["evidence_json"])
    _require(gate_payload["bindings"] == bindings and parse_time(gate_payload["issued_at"]) <= parse_time(at) < parse_time(gate_payload["expires_at"]), "Source gate bindings expired or changed")
    ready = connection.execute("SELECT * FROM provider_readiness_receipts WHERE id=?", (gate_payload["readiness_receipt_id"],)).fetchone()
    latest_ready = connection.execute("SELECT id FROM provider_readiness_receipts WHERE provider='tikhub' AND operation=? ORDER BY id DESC LIMIT 1", (operation,)).fetchone()
    _require(ready is not None and latest_ready is not None and ready["id"] == latest_ready[0], "Source readiness is missing or superseded")
    source_evidence = {key: evidence[key] for key in ("active", "build_sha256", "runtime_sha256", "config_sha256", "manifest")}
    if native is None:
        source_evidence.update(deployment={"evidence": {"rollback": evidence["deployment"]["evidence"]["rollback"]}},
            migration={"legacy_raw": {"legacy_project_root": evidence["migration"]["legacy_raw"]["legacy_project_root"]}})
    snapshot = {"contract": "capture-operation-qualification-snapshot-v1", "operation": operation,
        "qualification_kind": "native20" if native is not None else "source19_continuity20",
        "qualification_receipt_id": payload["qualification_receipt_id"], "qualification_receipt_sha256": permit["qualification_sha256"],
        "cohort_id": payload.get("native_cohort_id"), "expires_at": permit["expires_at"],
        "manifest": evidence["manifest"], "transport_code_sha256": _transport_code(), "runtime_bindings": _native_runtime(evidence),
        "authority_bindings": bindings, "source_evidence": source_evidence,
        "gate_id": gate["id"], "gate_sha256": gate["event_sha256"],
        "readiness_id": ready["id"], "readiness_sha256": ready["receipt_sha256"]}
    if native is not None:
        snapshot["cohort_sha256"] = read_transport_receipt(connection, payload["native_cohort_id"])["self_sha256"]
    snapshot["snapshot_sha256"] = auth.digest(snapshot)
    validate_frozen_operation_qualification(connection, snapshot, at)
    return snapshot


def validate_frozen_operation_qualification(connection: sqlite3.Connection, snapshot: Mapping[str, Any], at: str) -> None:
    """Immutable source proof only; successor owns current activation/installed checks."""
    if snapshot.get("qualification_kind") == "operator_authorized":
        from .capture_operator_release import validate_frozen
        validate_frozen(connection, snapshot, at=at)
        return
    _require(snapshot.get("contract") == "capture-operation-qualification-snapshot-v1"
             and snapshot.get("snapshot_sha256") == auth.digest({key: value for key, value in snapshot.items() if key != "snapshot_sha256"})
             and snapshot.get("transport_code_sha256") == _transport_code()
             and parse_time(at) < parse_time(snapshot["expires_at"]), "Frozen operation snapshot changed or expired")
    evidence = snapshot["source_evidence"]
    _require(snapshot["runtime_bindings"] == _native_runtime(evidence) and snapshot["manifest"] == evidence["manifest"], "Frozen source runtime binding differs")
    if snapshot["qualification_kind"] == "native20":
        cohort = read_transport_receipt(connection, snapshot["cohort_id"])
        _require(cohort["self_sha256"] == snapshot["cohort_sha256"], "Frozen source cohort differs")
        qualification = _native_qualification(connection, read_transport_receipt(connection, snapshot["qualification_receipt_id"]),
            cohort=cohort, evidence=evidence, at=at)
    else:
        _require(snapshot["qualification_kind"] == "source19_continuity20", "Unknown frozen qualification kind")
        qualification = _qualification(connection, evidence, receipt_id=snapshot["qualification_receipt_id"], operation=snapshot["operation"], at=at)
        row = connection.execute("SELECT * FROM transport_continuity_permits WHERE permit_sha256=?", (snapshot["authority_bindings"]["continuity_permit_sha256"],)).fetchone()
        _require(row is not None, "Frozen source continuity is missing")
        permit = dict(row)
        frozen = _object(permit["payload_json"])
        _require(permit["permit_sha256"] == auth.digest({key: permit[key] for key in _PERMIT_KEYS})
                 and frozen["active"] == snapshot["runtime_bindings"]["active"]
                 and permit["build_sha256"] == evidence["build_sha256"]
                 and permit["config_sha256"] == evidence["config_sha256"]
                 and frozen["runtime_sha256"] == evidence["runtime_sha256"]
                 and frozen["transport_manifest"] == evidence["manifest"]
                 and permit["qualification_sha256"] == qualification["self_sha256"]
                 and _authority_bindings(evidence, permit, frozen) == snapshot["authority_bindings"],
                 "Frozen continuity source scope differs")
        _continuity_complete(connection, dict(row))
    _require(qualification["self_sha256"] == snapshot["qualification_receipt_sha256"]
             and qualification["payload"]["operation"] == snapshot["operation"], "Frozen qualification hash or operation differs")
    gate = connection.execute("SELECT * FROM capture_paid_send_gate_events WHERE id=?", (snapshot["gate_id"],)).fetchone()
    ready = connection.execute("SELECT * FROM provider_readiness_receipts WHERE id=?", (snapshot["readiness_id"],)).fetchone()
    _require(gate is not None and ready is not None and gate["state"] == "open" and ready["status"] == "ready"
             and gate["event_sha256"] == snapshot["gate_sha256"] == auth.digest({key: gate[key] for key in ("provider", "operation", "state", "reason", "evidence_json", "recorded_at")})
             and ready["receipt_sha256"] == snapshot["readiness_sha256"] == auth.digest({key: ready[key] for key in ("provider", "operation", "status", "reason", "evidence_json", "created_at", "expires_at")}), "Frozen ordinary gate or readiness changed")
    gate_payload, ready_payload = _object(gate["evidence_json"]), _object(ready["evidence_json"])
    _require(gate_payload["bindings"] == ready_payload["bindings"] == snapshot["authority_bindings"]
             and gate["provider"] == ready["provider"] == "tikhub"
             and gate["operation"] == ready["operation"] == gate_payload["operation"] == snapshot["operation"]
             and gate_payload["readiness_receipt_id"] == ready["id"] and gate_payload["readiness_receipt_sha256"] == ready["receipt_sha256"]
             and ready_payload["qualification"] == "qualified"
             and ready_payload["qualification_receipt_sha256"] == snapshot["qualification_receipt_sha256"]
             and parse_time(gate_payload["issued_at"]) <= parse_time(at) < parse_time(gate_payload["expires_at"])
             and parse_time(ready["created_at"]) <= parse_time(at) < parse_time(ready["expires_at"]), "Frozen ordinary authorization expired or differs")


def _previously_enabled_operation(connection: sqlite3.Connection, *, operation: str,
                                  evidence: Mapping[str, Any], at: str) -> dict[str, Any]:
    """Verify past *actual* ordinary authority, not an operator flag or bare open."""
    row = connection.execute("SELECT * FROM capture_paid_send_gate_events WHERE provider='tikhub' AND operation=? AND state='open' ORDER BY id DESC LIMIT 1", (operation,)).fetchone()
    _require(row is not None, "Operation has never been explicitly qualified and enabled")
    gate = dict(row)
    payload = _object(gate["evidence_json"])
    expected_runtime = {**{key: evidence["active"][key] for key in
        ("activation_id", "profile_id", "roster_snapshot_id", "roster_members_sha256")},
        "build_receipt_sha256": evidence["build_sha256"], "runtime_root_receipt_sha256": evidence["runtime_sha256"],
        "config_receipt_sha256": evidence["config_sha256"]}
    _require(gate["event_sha256"] == auth.digest({key: gate[key] for key in ("provider", "operation", "state", "reason", "evidence_json", "recorded_at")})
             and payload.get("contract") == auth.CONTRACT and payload.get("operation") == operation
             and all(payload.get("bindings", {}).get(key) == value for key, value in expected_runtime.items())
             and parse_time(gate["recorded_at"]) <= parse_time(at), "Prior ordinary authority belongs to another runtime or activation")
    issued = gate["recorded_at"]
    ready = connection.execute("SELECT * FROM provider_readiness_receipts WHERE id=?", (payload["readiness_receipt_id"],)).fetchone()
    _require(ready is not None and ready["provider"] == "tikhub" and ready["operation"] == operation and ready["status"] == "ready"
             and ready["receipt_sha256"] == payload["readiness_receipt_sha256"] == auth.digest({key: ready[key] for key in
                 ("provider", "operation", "status", "reason", "evidence_json", "created_at", "expires_at")}), "Prior ordinary readiness changed")
    ready_payload = _object(ready["evidence_json"])
    _require(ready_payload.get("contract") == auth.READINESS_CONTRACT and ready_payload.get("qualification") == "qualified"
             and ready_payload.get("bindings") == payload["bindings"] and ready_payload.get("transport_manifest_sha256") == auth.digest(evidence["manifest"]),
             "Prior ordinary operation was not actually qualified")
    if ready_payload.get("activation_successor") is not None:
        from .capture_activation_release import _target_authority
        _, bindings, expires = _target_authority(connection, operation=operation, at=issued)
    else:
        qualification = read_transport_receipt(connection, ready_payload["qualification_receipt_id"])
        if qualification["payload"].get("schema_version") == 20:
            cohort = read_transport_receipt(connection, qualification["payload"]["cohort_receipt_id"])
            qualification = _native_qualification(connection, qualification, cohort=cohort, evidence=evidence, at=issued)
            bindings = {**expected_runtime, "continuity_permit_sha256": qualification["self_sha256"]}
            expires = qualification["payload"]["expires_at"]
        else:
            permit, frozen = _permit(connection, operation=operation, at=issued, evidence=evidence)
            _continuity_complete(connection, permit)
            bindings, expires = _authority_bindings(evidence, permit, frozen), permit["expires_at"]
        _require(ready_payload["qualification_receipt_sha256"] == qualification["self_sha256"], "Prior operation qualification changed")
    _require(bindings == payload["bindings"] and payload["scope_hash"] == auth.scope_hash(runtime_bindings=bindings, provider="tikhub", operation=operation)
             and payload["issued_at"] == gate["recorded_at"] and utc(payload["expires_at"]) == utc(expires)
             and parse_time(ready["created_at"]) <= parse_time(issued) < parse_time(ready["expires_at"]), "Prior operation issuance differs from its proved qualification")
    return {"gate_id": gate["id"], "gate_sha256": gate["event_sha256"], "expires_at": expires}


def maintain_operation_qualifications(connection: sqlite3.Connection, *, at: str,
                                      mirror_root: Path) -> dict[str, Any]:
    """Existing maintenance tick: no HTTP, first enablement, new work or retries.

    Six hours before expiry (or after downtime), an already enabled operation
    may collect the fixed next 200 through its normal A transactions. The same
    tick verifies completed evidence and renews; a failed rank is never replaced.
    """
    _require(connection.in_transaction, "Qualification maintenance requires a writer transaction")
    require_current_process_writer_lock(connection)
    result: dict[str, Any] = {"contract": "capture-operation-maintenance-v1", "provider_calls": 0,
        "coverage_complete": False, "operations": {}}
    try:
        evidence = _installed_evidence(connection, at=at)
        _require(evidence["deployment"]["status"] == "accepted", "Candidate deployment cannot automatically enable capture")
        _native_control(connection, evidence, at=at)
        _require((provider_budget.circuit_state(connection) or {}).get("open") is not True, "Provider circuit blocks automatic qualification")
    except (RuntimeError, ValueError, OSError, sqlite3.Error) as error:
        return {**result, "status": "blocked", "reason": str(error)}
    for operation in sorted(CONTINUITY_OPERATIONS):
        latest = connection.execute("SELECT * FROM capture_paid_send_gate_events WHERE provider='tikhub' AND operation=? ORDER BY id DESC LIMIT 1", (operation,)).fetchone()
        if latest is None or latest["state"] not in {"open", "diagnostic_only"}:
            result["operations"][operation] = {"status": "not_enabled"}
            continue
        connection.execute("SAVEPOINT native_qualification_maintenance")
        try:
            _require((provider_budget.fault_state(connection, scope_kind="operation", operation=operation) or {}).get("open") is not True,
                     "Operation circuit blocks automatic qualification")
            from .capture_operator_release import maintain as maintain_operator_release
            approved = maintain_operator_release(connection, evidence=evidence, operation=operation, latest=latest, at=at)
            if approved is not None:
                result["operations"][operation] = approved
                continue
            enabled = _previously_enabled_operation(connection, operation=operation, evidence=evidence, at=at)
            cohort = _latest_native(connection, operation=operation, kind="cohort", activation_id=evidence["active"]["activation_id"])
            qualification = _latest_native_qualification(connection, operation=operation, activation_id=evidence["active"]["activation_id"])
            if latest["state"] == "diagnostic_only":
                diagnostic = _object(latest["evidence_json"])
                _require(cohort is not None and diagnostic.get("contract") == auth.CONTRACT
                         and diagnostic.get("native_cohort_id") == cohort["receipt_id"]
                         and diagnostic.get("bindings", {}).get("continuity_permit_sha256") == cohort["self_sha256"]
                         and latest["event_sha256"] == auth.digest({key: latest[key] for key in
                             ("provider", "operation", "state", "reason", "evidence_json", "recorded_at")})
                         and parse_time(latest["recorded_at"]) <= parse_time(at),
                         "Latest diagnostic gate is not the fixed native renewal cohort")
            complete = cohort is not None and qualification is not None and qualification["payload"].get("cohort_receipt_id") == cohort["receipt_id"]
            if cohort is not None and not complete and parse_time(at) < parse_time(cohort["payload"]["expires_at"]):
                _check_native_cohort(connection, cohort, evidence=evidence, at=at)
                count = connection.execute("SELECT count(*) FROM paid_provider_dispatch_events WHERE id>? AND lower(provider)='tikhub' AND operation=? AND event_type='send_marked'",
                    (cohort["payload"]["start_high_watermark"], operation)).fetchone()[0]
                if count < 200:
                    value = {"status": "collecting", "cohort_id": cohort["receipt_id"], "actual_starts": count}
                else:
                    record_native_operation_qualification(connection, cohort_receipt_id=cohort["receipt_id"], at=at, mirror_root=mirror_root)
                    value = {"status": "renewed", **renew_operation_gate(connection, operation=operation, at=at)}
            elif complete and qualification is not None and parse_time(at) < parse_time(qualification["payload"]["expires_at"]) and latest["state"] == "diagnostic_only":
                value = {"status": "renewed", **renew_operation_gate(connection, operation=operation, at=at)}
            elif parse_time(enabled["expires_at"]) - parse_time(at) <= timedelta(hours=6):
                frozen = freeze_operation_cohort(connection, operation=operation, at=at, mirror_root=mirror_root)
                value = {"status": "collecting", "cohort_id": frozen["receipt_id"], "actual_starts": 0}
            else:
                value = {"status": "fresh", "expires_at": enabled["expires_at"]}
        except (KeyError, TypeError, RuntimeError, ValueError, OSError, sqlite3.Error) as error:
            connection.execute("ROLLBACK TO native_qualification_maintenance")
            value = {"status": "blocked", "reason": str(error)}
        finally:
            connection.execute("RELEASE native_qualification_maintenance")
        result["operations"][operation] = value
    return {**result, "status": "checked"}
