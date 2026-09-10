"""Bounded, append-only schema20 code successors with immutable acceptance.

This is not another deployment acceptance or a profile cutover. Historical
acceptance stays immutable; a postseal decision binds the new loaded generation.
"""
from __future__ import annotations

import hashlib
import json
import sqlite3
import subprocess
import sys
from contextlib import contextmanager
from contextvars import ContextVar
from pathlib import Path
from typing import Any, Iterator, Mapping

from . import account_code_successor as account, capture_authorizations as auth, forward_recovery, paid_drain
from .profile_activations import activation_at
from .source_routing import parse_time
from .runtime_paths import source_root, verified_git

PLAN = "capture-planner-budget-successor-plan-v1"
DECISION = "capture-planner-budget-successor-decision-v1"
PROOF = "capture-postaccepted-code-successor-proof-v1"
PLAN_V2 = "capture-runtime-successor-plan-v2"
DECISION_V2 = "capture-runtime-successor-decision-v2"
PROOF_V2 = "capture-postaccepted-code-successor-proof-v2"
V2_TESTS = frozenset({"tests/test_v8_capture_code_successor_v2.py",
    "tests/test_v8_capture_runtime_lease.py", "tests/test_v20_evidence_hash_cache.py",
    "tests/test_macos_snapshot_publisher_recovery.py"})
V2_CHECKS = frozenset({"capture_runtime_lease", "code_successor_v2", "evidence_hash_cache", "publisher_recovery"})
MAX_CHAIN_DEPTH = 16
_CHAIN: ContextVar[tuple[str, ...]] = ContextVar("capture_successor_chain", default=())
BUSINESS = "src/dcar_eval/v8/capture_runtime.py"
BATCHES = "src/dcar_eval/v8/capture_batches.py"
V2_ADDITIONAL = frozenset({BATCHES, "deploy/macos/run_snapshot_publisher.sh"})
PIPELINE = "src/dcar_eval/v8/pipeline.py"
PIPELINE_SHA256 = "7290f25c2a88235a033cc0dd8d6a8099d5bb1774632434c9eab1d5e032840195"
PLUMBING = frozenset({"src/dcar_eval/v8/capture_code_successor.py", "src/dcar_eval/v8/capture_release.py",
    "src/dcar_eval/v8/capture_operator_release.py", "scripts/seal_r0_receipts.py", "scripts/v20_release_contract.py",
    "scripts/issue_v20_code_successor.py", "scripts/build_server_snapshot.py", "deploy/macos/publish_snapshot.py",
    "deploy/server/install_snapshot.py"})
PRIVATE_ROLES = frozenset({"plan", "previous_build", "source_archive", "full_checks", "build", "runtime"})
ACTIVE = ("activation_id", "profile_id", "roster_snapshot_id", "roster_members_sha256", "activation_sha256")
_CHECKED: ContextVar[dict[str, Any] | None] = ContextVar("planner_budget_checked_successor", default=None)


def _require(value: bool, message: str) -> None:
    if not value:
        raise auth.AuthorizationError("code_successor: " + message)


def _tools() -> tuple[Any, Any]:
    from .capture_release import _release_tools
    contract = _release_tools()
    return sys.modules["seal_r0_receipts"], contract


def _ref(path: Path) -> dict[str, Any]:
    sealer, contract = _tools()
    sealer._read_private_json(path)
    return {**contract.verified_reference({"path": str(path), "sha256": sealer._sha256_file(path)}),
            "byte_size": path.stat().st_size}


def historical_test_git(project: Path, git: Mapping[str, Any]) -> dict[str, Any] | None:
    checked = _CHECKED.get()
    if checked is not None and str(project.resolve()) == checked["project_root"] and (
        dict(git) == checked["previous_build"]["git"]
        or (checked.get("historical") and dict(git) == checked["plan"]["git"])
    ):
        return dict(git)
    return None


def _source_changes(project: Path, before: Mapping[str, Any], after: Mapping[str, Any], *,
                    live: bool = True, parent_delta: bool = False, version: int = 1) -> dict[str, Any]:
    _require(before["git"]["head"] == after["git"]["head"], "Git HEAD must not change")
    left = forward_recovery._successor_archive(before)
    right = forward_recovery._successor_archive(after, live=live)
    changes: dict[str, Any] = {}
    for name in sorted(set(left) | set(right)):
        if name not in left or name not in right:
            try:
                original = verified_git(source_root(project), "show", str(after["git"]["head"]) + ":" + name) or None
            except subprocess.CalledProcessError:
                original = None
        else:
            original = None
        old, new = left.get(name, original), right.get(name, original)
        if old == new:
            continue
        _require(name in {BUSINESS, PIPELINE} or name in PLUMBING or (version == 2 and name in V2_TESTS | V2_ADDITIONAL), "source changed outside the exact approved successor boundary")
        _require(isinstance(new, bytes), "source deletion is not permitted")
        assert isinstance(new, bytes)
        if name == PIPELINE:
            _require(hashlib.sha256(new).hexdigest() == PIPELINE_SHA256, "pipeline differs from the one tested integrated dispatch fix")
        changes[name] = {"before_sha256": hashlib.sha256(old).hexdigest() if old is not None else None,
                         "after_sha256": hashlib.sha256(new).hexdigest()}
    if parent_delta and version == 2:
        _require(BUSINESS in changes and PIPELINE not in changes,
                 "runtime successor must change capture_runtime and retain the installed pipeline")
    elif parent_delta:
        _require(PIPELINE in changes and BUSINESS not in changes, "installed parent may only receive the tested pipeline fix and release plumbing")
    else:
        _require(BUSINESS in changes, "the tested planner-budget business change is absent")
    return changes


def _accepted(connection: sqlite3.Connection) -> tuple[dict[str, Any], dict[str, Any]]:
    _, contract = _tools()
    rows = connection.execute("SELECT * FROM deployment_readiness_receipts WHERE status='accepted'").fetchall()
    _require(len(rows) == 1, "the original unique accepted deployment is required")
    row = dict(rows[0])
    payload = json.loads(row["payload_json"])
    _require(row["receipt_sha256"] == contract.deployment_digest(deployment_id=row["deployment_id"], status="accepted",
        payload=payload, recorded_at=row["recorded_at"]), "original accepted digest changed")
    return row, payload


def _current_control(connection: sqlite3.Connection, at: str) -> tuple[dict[str, Any], dict[str, Any]]:
    active = activation_at(connection, at)
    _require(active is not None and active["profile_id"] == "integrated_route_v1", "only the already integrated activation may continue")
    assert active is not None
    state = paid_drain.dispatch_state(connection, at=at)
    _require(state.paid_dispatch_open and state.activation_id == active["activation_id"] and state.permit_event_id is not None,
             "current RELEASE/drain is not open")
    row = connection.execute("SELECT * FROM pipeline_paid_drain_events WHERE id=? AND event_type='release'", (state.permit_event_id,)).fetchone()
    _require(row is not None, "current RELEASE is absent")
    assert row is not None
    return {key: active[key] for key in ACTIVE}, {"id": row["id"], "event_hash": row["event_hash"]}


@contextmanager
def using_plan(connection: sqlite3.Connection, reference: Mapping[str, Any], *, project_root: Path,
               at: str, _historical: bool = False) -> Iterator[dict[str, Any]]:
    sealer, contract = _tools()
    reference = contract.verified_reference(dict(reference), project_root=project_root)
    plan = sealer._read_private_json(Path(reference["path"]))
    from . import runtime_source_successor as source
    if plan.get("contract") == source.PLAN:
        with source.using_plan(connection, reference, project_root=project_root, at=at, historical=_historical) as checked:
            yield checked
        return
    if plan.get("contract") == account.PLAN:
        with account.using_plan(connection, reference, project_root=project_root, at=at, historical=_historical) as checked:
            yield checked
        return
    version = 2 if plan.get("contract") == PLAN_V2 else 1
    _require(plan.get("contract") in {PLAN, PLAN_V2} and plan.get("business_module") == BUSINESS
             and plan.get("plumbing_allowlist") == sorted(PLUMBING)
             and plan.get("project_root") == str(project_root.resolve())
             and parse_time(plan["issued_at"]) <= parse_time(at), "plan contract, source root or time differs")
    if version == 2:
        _require(plan.get("test_allowlist") == sorted(V2_TESTS)
                 and plan.get("additional_source_allowlist") == sorted(V2_ADDITIONAL)
                 and plan.get("required_checks") == sorted(V2_CHECKS)
                 and plan.get("change_scope") == "capture_runtime_lease_v1"
                 and "installed_parent" in plan, "runtime successor scope or installed parent is absent")
    row, _ = _accepted(connection)
    _require(plan["source_deployment"] == {"deployment_id": row["deployment_id"], "receipt_sha256": row["receipt_sha256"]},
             "plan points to another accepted deployment")
    active, control = _current_control(connection, at)
    account.control_precheck(plan, active, control)
    previous_ref = contract.verified_reference(plan["previous_build"], project_root=project_root)
    previous = sealer._read_receipt(Path(previous_ref["path"]), contract_version=sealer.SEALED_BUILD_CONTRACT)
    _require(previous["schema_contract"] == sealer._schema_contract(20, 20), "previous build is not installed schema20")
    source = contract.verified_reference(plan["source_archive"], project_root=project_root)
    _require(_historical or plan["git"] == sealer._git_record(project_root, allow_working_tree=True), "live source changed after plan")
    _require(_source_changes(project_root, previous, {"git": plan["git"], "source_archive": source}, live=not _historical, version=version) == plan["changes"],
             "source delta changed after plan")
    checked = {"project_root": str(project_root.resolve()), "previous_build": previous, "plan": plan,
               "reference": reference, "historical": _historical}
    token = _CHECKED.set(checked)
    try:
        checks_ref = contract.verified_reference(plan["full_checks"], project_root=project_root)
        checks = sealer._read_receipt(Path(checks_ref["path"]), contract_version=sealer.TEST_RESULTS_CONTRACT)
        _require(checks["git"] == plan["git"], "tests do not bind the planned source")
        sealer._verify_test_results_payload(project_root, checks)
        _require({"planner_budget", "code_successor"} <= set(checks["results"]), "actual focused successor results are missing")
        if version == 2:
            _require(V2_CHECKS <= set(checks["results"]), "actual runtime successor focused results are missing")
        if PIPELINE in plan["changes"]:
            _require("installed_parent" in plan and "integrated_pipeline" in checks["results"], "actual integrated pipeline result or installed parent is missing")
        previous_checks = sealer._read_receipt(Path(previous["test_results_receipt"]["path"]), contract_version=sealer.TEST_RESULTS_CONTRACT)
        contract.verified_reference(previous["test_results_receipt"], project_root=project_root)
        _require(all(checks["results"].get(key) == value for key, value in previous_checks["results"].items()),
                 "prior full-regression logs changed")
        if "installed_parent" in plan:
            parent_ref = contract.verified_reference(plan["installed_parent"], project_root=project_root)
            parent_proof = current_proof(connection, project_root=project_root, build_path=Path(parent_ref["path"]), at=at, _historical=True)
            _require(parent_proof is not None and parent_proof["build_reference"]["sha256"] == parent_ref["sha256"]
                     and parent_proof["plan_payload"]["previous_build"] == plan["previous_build"], "installed parent does not descend from the original accepted build")
            assert parent_proof is not None
            _require(version == 2 or "installed_parent" not in parent_proof["plan_payload"],
                     "legacy pipeline successor may only follow the first reviewed successor")
            parent_build = sealer._read_receipt(Path(parent_ref["path"]), contract_version=sealer.SEALED_BUILD_CONTRACT)
            parent_checks = sealer._read_receipt(Path(parent_build["test_results_receipt"]["path"]), contract_version=sealer.TEST_RESULTS_CONTRACT)
            _require(all(checks["results"].get(key) == value for key, value in parent_checks["results"].items()), "installed parent test logs changed")
            _require(_source_changes(project_root, parent_build, {"git": plan["git"], "source_archive": source},
                                     parent_delta=True, live=not _historical, version=version)
                     == plan["installed_parent_changes"], "installed parent source delta changed")
            checked["installed_parent_proof"] = parent_proof
        deployment = contract.validate_deployment_receipt(connection, deployment_id=row["deployment_id"],
            project_root=project_root, require_accepted=True)
        decision = deployment.get("release_decision")
        _require(isinstance(decision, dict) and decision["decision_sha256"] == plan["source_decision_sha256"]
                 and decision["runtime_evidence"]["build"]["sha256"] == previous_ref["sha256"]
                 and decision["operations"] == plan["operations"]
                 and decision["transport_manifest"] == plan["manifest"] == forward_recovery._route(),
                 "original user decision or transport scope changed")
        checked["deployment"] = deployment
        account.control_postcheck(connection, plan=plan, deployment=deployment, at=at)
        yield checked
    finally:
        _CHECKED.reset(token)


def _installed_build_path() -> Path | None:
    from .runtime_database import load_installed_writer_contract
    installed = load_installed_writer_contract(required=False)
    if installed is None:
        return None
    environment = installed.payload.get("EnvironmentVariables")
    _require(isinstance(environment, dict), "installed writer environment is invalid")
    assert isinstance(environment, dict)
    name = environment.get("DCAR_LOADED_BUILD_RECEIPT")
    return Path(name) if isinstance(name, str) and name else None


@contextmanager
def deployment_context(connection: sqlite3.Connection, *, project_root: Path) -> Iterator[bool]:
    if _CHECKED.get() is not None:
        yield False
        return
    path = _installed_build_path()
    if path is None:
        yield False
        return
    sealer, _ = _tools()
    build = sealer._read_receipt(path, contract_version=sealer.SEALED_BUILD_CONTRACT)
    reference = build.get("code_successor_plan")
    if reference is None:
        yield False
        return
    from .storage import now_utc
    from .capture_evidence_preflight import evidence_time
    at = evidence_time(now_utc())
    with account.runtime_context(connection, build=build, build_ref=_ref(path), at=at), using_plan(
        connection, reference, project_root=project_root, at=at
    ):
        yield True


def prepare_plan(connection: sqlite3.Connection, *, project_root: Path, previous_build: Path,
                 evidence_dir: Path, tests: Mapping[str, Path], actor: str, reason: str, at: str,
                 transition: str | None = None) -> dict[str, Any]:
    if transition is not None:
        from . import runtime_source_successor as source
        if transition == source.TRANSITION:
            return source.prepare_plan(connection, project_root=project_root, previous_build=previous_build,
                evidence_dir=evidence_dir, tests=tests, actor=actor, reason=reason, at=at)
        _require(transition == account.TRANSITION, "unknown explicit source transition")
        return account.prepare_plan(connection, project_root=project_root, previous_build=previous_build,
            evidence_dir=evidence_dir, tests=tests, actor=actor, reason=reason, at=at)
    from .runtime_database import require_current_process_writer_lock
    require_current_process_writer_lock(connection)
    sealer, contract = _tools()
    _require(bool(actor.strip()) and bool(reason.strip()), "actual release actor and reason are required")
    old_ref = _ref(previous_build)
    installed_path = _installed_build_path()
    _require(installed_path is not None and _ref(installed_path)["sha256"] == old_ref["sha256"],
             "requested parent differs from the actually installed build")
    old = sealer._read_receipt(previous_build, contract_version=sealer.SEALED_BUILD_CONTRACT)
    row, payload = _accepted(connection)
    decision = sealer._read_private_json(Path(payload["evidence"]["release_decision"]["path"]))
    installed_parent = None
    parent_build = None
    if "code_successor_plan" in old:
        parent_proof = current_proof(connection, project_root=project_root, build_path=previous_build, at=at, _historical=True)
        _require(parent_proof is not None, "installed parent has no released successor proof")
        assert parent_proof is not None
        installed_parent, parent_build = old_ref, old
        old_ref = parent_proof["plan_payload"]["previous_build"]
        old = sealer._read_receipt(Path(old_ref["path"]), contract_version=sealer.SEALED_BUILD_CONTRACT)
    _require(decision["runtime_evidence"]["build"]["sha256"] == old_ref["sha256"], "only the current original accepted build may be succeeded")
    parent = sealer._private_evidence_parent(evidence_dir, project_root)
    sealer._create_evidence_dir(evidence_dir, parent)
    git = sealer._git_record(project_root, allow_working_tree=True)
    archive = evidence_dir / sealer.WORKING_TREE_ARCHIVE
    sealer._write_source_archive(project_root, archive, git)
    source = sealer._source_archive_record(archive, git)
    checks = sealer._test_results_payload(project_root, paths=tests, git_record=git)
    checks_path = evidence_dir / sealer.TEST_RESULTS_FILENAME
    sealer._write_exclusive(checks_path, sealer._envelope(sealer.TEST_RESULTS_CONTRACT, checks))
    active, control = _current_control(connection, at)
    version = 2 if parent_build is not None else 1
    plan = {"contract": PLAN_V2 if version == 2 else PLAN, "project_root": str(project_root.resolve()), "business_module": BUSINESS,
        "plumbing_allowlist": sorted(PLUMBING), "source_deployment": {"deployment_id": row["deployment_id"], "receipt_sha256": row["receipt_sha256"]},
        "source_decision_sha256": payload["evidence"]["release_decision"]["sha256"], "previous_build": old_ref,
        "git": git, "source_archive": source, "full_checks": _ref(checks_path), "changes": _source_changes(project_root, old, {"git": git, "source_archive": source}, version=version),
        "active": active, "release": control, "operations": decision["operations"], "manifest": decision["transport_manifest"],
        "actor": actor, "reason": reason, "issued_at": at, "business_e2e": "deferred_by_user", "transport_qualification": "not_verified"}
    if installed_parent is not None:
        assert parent_build is not None
        plan["installed_parent"] = installed_parent
        plan["installed_parent_changes"] = _source_changes(project_root, parent_build, {"git": git, "source_archive": source}, parent_delta=True, version=version)
        plan.update(test_allowlist=sorted(V2_TESTS), additional_source_allowlist=sorted(V2_ADDITIONAL),
                    required_checks=sorted(V2_CHECKS), change_scope="capture_runtime_lease_v1")
    plan_path = evidence_dir / "code-successor-plan.json"
    sealer._write_exclusive(plan_path, plan)
    reference = _ref(plan_path)
    with using_plan(connection, reference, project_root=project_root, at=at):
        pass
    return reference


def _decision_row(connection: sqlite3.Connection, build_sha: str) -> sqlite3.Row | None:
    from . import runtime_source_successor as source
    rows = connection.execute("""SELECT * FROM scheduler_runs WHERE job_id='transport_receipt:campaign_terminal'
        AND json_extract(details_json,'$.payload.contract') IN (?,?,?,?)
        AND json_extract(details_json,'$.payload.build_sha256')=? ORDER BY id""", (DECISION, DECISION_V2, account.DECISION, source.DECISION, build_sha)).fetchall()
    _require(len(rows) <= 1, "duplicate postseal decisions")
    return rows[0] if rows else None


def _decision_payload(plan: Mapping[str, Any], *, plan_sha: str, build_sha: str, runtime_sha: str, at: str) -> dict[str, Any]:
    from . import runtime_source_successor as source
    from .transport_receipts import _timestamp
    at = _timestamp(at)
    result = {"contract": account.DECISION if plan["contract"] == account.PLAN else (DECISION_V2 if plan["contract"] == PLAN_V2 else DECISION), "source_deployment": plan["source_deployment"],
        "source_decision_sha256": plan["source_decision_sha256"], "plan_sha256": plan_sha,
        "build_sha256": build_sha, "runtime_sha256": runtime_sha, "active": plan["active"],
        "release": plan["release"], "operations": plan["operations"], "manifest": plan["manifest"],
        "actor": plan["actor"], "reason": plan["reason"], "issued_at": at,
        "authorization": "inherit_existing_user_release_only", "business_e2e": "deferred_by_user",
        "transport_qualification": "not_verified", "schema_migration_repeated": False}
    if plan["contract"] == account.PLAN:
        result.update(parent_build_sha256=plan["installed_parent"]["sha256"], transition=account.TRANSITION,
                      changes_sha256=auth.digest(plan["changes"]), required_checks=sorted(account.REQUIRED_CHECKS))
    if plan["contract"] == PLAN_V2:
        result.update(parent_build_sha256=plan["installed_parent"]["sha256"],
                      change_scope=plan["change_scope"], required_checks=plan["required_checks"])
    if plan["contract"] == source.PLAN:
        result.update(contract=source.DECISION, parent_build_sha256=plan["installed_parent"]["sha256"],
                      transition=source.TRANSITION, changes_sha256=auth.digest(plan["changes"]),
                      source_root=plan["source_root"], source_tree_sha256=plan["source_tree"]["sha256"],
                      required_checks=sorted(source.REQUIRED_CHECKS))
    return result


@contextmanager
def _chain_link(identity: str) -> Iterator[None]:
    chain = _CHAIN.get()
    _require(identity not in chain and len(chain) < MAX_CHAIN_DEPTH, "successor parent chain is cyclic or too deep")
    token = _CHAIN.set((*chain, identity))
    try:
        yield
    finally:
        _CHAIN.reset(token)


def current_proof(connection: sqlite3.Connection, *, project_root: Path, build_path: Path | None = None,
                  at: str, require_decision: bool = True, _historical: bool = False) -> dict[str, Any] | None:
    path = build_path or _installed_build_path()
    if path is None:
        return None
    with _chain_link(_ref(path)["sha256"]):
        return _current_proof(connection, project_root=project_root, build_path=path, at=at,
                              require_decision=require_decision, _historical=_historical)


def _current_proof(connection: sqlite3.Connection, *, project_root: Path, build_path: Path,
                   at: str, require_decision: bool, _historical: bool) -> dict[str, Any] | None:
    sealer, contract = _tools()
    path = build_path or _installed_build_path()
    if path is None:
        return None
    build_ref = _ref(path)
    build = sealer._read_receipt(path, contract_version=sealer.SEALED_BUILD_CONTRACT)
    plan_ref = build.get("code_successor_plan")
    if plan_ref is None:
        return None
    with account.runtime_context(connection, build=build, build_ref=build_ref, at=at,
        require_decision=require_decision), using_plan(
        connection, plan_ref, project_root=project_root, at=at, _historical=_historical
    ) as checked:
        plan, deployment = checked["plan"], checked["deployment"]
        previous = checked["previous_build"]
        _require(build["status"] == "succeeded" and build["schema_contract"] == sealer._schema_contract(20, 20)
                 and build["git"] == plan["git"] and build["source_archive"]["sha256"] == plan["source_archive"]["sha256"]
                 and build["deployment_readiness"] == deployment
                 and build["postmigration_lineage"]["previous_build_receipt"]["sha256"] == plan.get("installed_parent", plan["previous_build"])["sha256"]
                 and all(build["postmigration_lineage"][key] == previous["postmigration_lineage"][key]
                         for key in ("install_receipt", "migration_receipt")), "sealed successor changed source or install lineage")
        if _historical:
            archive = forward_recovery._successor_archive(build)
            critical = {}
            paths = sealer.V20_ACCOUNT_CRITICAL_FILES if plan["contract"] == account.PLAN else sealer.V20_LEGACY_CRITICAL_FILES
            _require(set(build["critical_files"]) == {p.as_posix() for p in paths},
                     "historical runtime critical inventory changed")
            for relative in paths:
                name = relative.as_posix()
                body = archive.get(name)
                if name not in archive:
                    body = verified_git(source_root(project_root), "show", str(build["git"]["head"]) + ":" + name)
                _require(isinstance(body, bytes), "historical critical source is absent")
                assert isinstance(body, bytes)
                critical[name] = hashlib.sha256(body).hexdigest()
        else:
            critical = sealer._critical_files(project_root, sealer.V20_CRITICAL_FILES)
        _require(build["critical_files"] == critical, "loaded critical files differ from sealed source")
        checks = sealer._read_receipt(Path(build["test_results_receipt"]["path"]), contract_version=sealer.TEST_RESULTS_CONTRACT)
        contract.verified_reference(build["test_results_receipt"], project_root=project_root)
        sealer._verify_test_results_payload(project_root, checks)
        planned_checks = sealer._read_receipt(Path(plan["full_checks"]["path"]), contract_version=sealer.TEST_RESULTS_CONTRACT)
        _require(checks == planned_checks, "postseal test content differs from approved plan")
        runtime_ref = contract.verified_reference(build["runtime_root_receipt"], project_root=project_root)
        runtime = sealer._read_receipt(Path(runtime_ref["path"]), contract_version=sealer.RUNTIME_ROOT_CONTRACT)
        install = contract._json_reference(deployment["evidence"]["install"])
        # Publication validates a detached snapshot's ledger against the real
        # installed DB identity; the replica's inode is intentionally different.
        actual_db = Path(runtime["formal_database"]["path"])
        from .runtime_database import load_installed_writer_contract
        installed = load_installed_writer_contract(required=False)
        _require(installed is None or installed.database.resolve() == actual_db.resolve(), "installed formal database path differs")
        identity = actual_db.stat()
        _require(runtime["action"] == "retain" and runtime["mutation_counts"] == {"moved_files": 0, "copied_bytes": 0, "deleted_files": 0}
                 and runtime["formal_database"]["user_version"] == 20
                 and runtime["formal_database"]["migration"] == {"version": 20, "name": "integrated-video-capture-v25"}
                 and install["formal_database"] == str(actual_db.resolve())
                 and all(runtime["formal_database"][key] == install["installed"]["file"][key] == value
                         for key, value in (("device", identity.st_dev), ("inode", identity.st_ino)))
                 and runtime["installed_runtime"]["database"]["path"] == str(actual_db.resolve()),
                 "new runtime does not retain the real installed database")
        origin_runtime = deployment["release_decision"]["runtime_bindings"]
        from .capture_activation_release import validate_installed_activation_successor
        active = activation_at(connection, at)
        assert active is not None
        validate_installed_activation_successor(connection, source_deployment=deployment, current_active=active,
            runtime_bindings=origin_runtime, manifest=plan["manifest"], at=at)
        row = _decision_row(connection, build_ref["sha256"])
        _require(row is not None or not require_decision, "postseal successor decision has not been issued")
        decision = None
        if row is not None:
            from .transport_receipts import read_transport_receipt
            decision = dict(read_transport_receipt(connection, row["id"]))
            expected = _decision_payload(plan, plan_sha=plan_ref["sha256"], build_sha=build_ref["sha256"],
                runtime_sha=runtime_ref["sha256"], at=decision["recorded_at"])
            _require(decision["payload"] == expected and parse_time(plan["issued_at"]) <= parse_time(decision["recorded_at"]) <= parse_time(at),
                     "postseal decision scope, identity or time differs")
        references = {"plan": plan_ref, "previous_build": plan.get("installed_parent", plan["previous_build"]), "source_archive": plan["source_archive"],
            "full_checks": plan["full_checks"], "build": build_ref, "runtime": runtime_ref}
        from . import runtime_source_successor as source
        if plan["contract"] == source.PLAN:
            references["source_tree"] = plan["source_tree"]
        private = [{"role": key, "path": value["path"], "sha256": value["sha256"], "byte_size": Path(value["path"]).stat().st_size}
                   for key, value in sorted(references.items())]
        proof = {"contract": account.PROOF if plan["contract"] == account.PLAN else (PROOF_V2 if plan["contract"] == PLAN_V2 else PROOF), "source_deployment_sha256": deployment["receipt_sha256"],
            "origin_runtime_bindings": origin_runtime,
            "runtime_bindings": {"build_sha256": build_ref["sha256"], "runtime_sha256": runtime_ref["sha256"], "config_sha256": origin_runtime["config_sha256"]},
            "active": plan["active"], "release_event_id": plan["release"]["id"], "release_event_hash": plan["release"]["event_hash"],
            "manifest": plan["manifest"], "plan_payload": plan, "plan_reference": plan_ref,
            "decision_receipt": decision, "build_reference": build_ref, "runtime_reference": runtime_ref,
            "private_references": private}
        if plan["contract"] == source.PLAN:
            proof["contract"] = source.PROOF
        if "account_control" in checked:
            proof["roster_successor_contract"] = "account-roster-code-plan-successor-v1"
            control = checked["account_control"]
            proof.update(active=control["active"], release_event_id=control["release"]["id"],
                         release_event_hash=control["release"]["event_hash"])
            if control["roster_successor"] is not None:
                proof["roster_successor"] = control["roster_successor"]
        if "installed_parent_proof" in checked:
            proof["installed_parent_proof"] = checked["installed_parent_proof"]
        return {**proof, "proof_sha256": auth.digest(proof)}


def issue_decision(connection: sqlite3.Connection, *, project_root: Path, build_path: Path, mirror_root: Path, at: str) -> dict[str, Any]:
    from .runtime_database import require_current_process_writer_lock
    from .transport_receipts import append_transport_receipt
    require_current_process_writer_lock(connection)
    _require(connection.in_transaction, "decision issuance requires the writer transaction")
    proof = current_proof(connection, project_root=project_root, build_path=build_path, at=at, require_decision=False)
    _require(proof is not None, "build has no verified successor plan")
    assert proof is not None
    if proof["decision_receipt"] is not None:
        return proof
    payload = _decision_payload(proof["plan_payload"], plan_sha=proof["plan_reference"]["sha256"],
        build_sha=proof["runtime_bindings"]["build_sha256"], runtime_sha=proof["runtime_bindings"]["runtime_sha256"], at=at)
    append_transport_receipt(connection, kind="campaign_terminal", identity_key=payload["contract"] + ":" + payload["build_sha256"],
        payload=payload, at=at, mirror_root=mirror_root)
    result = current_proof(connection, project_root=project_root, build_path=build_path, at=at)
    assert result is not None
    return result


def validate_portable(connection: sqlite3.Connection, proof: Mapping[str, Any], *, deployment: Mapping[str, Any], at: str) -> dict[str, Any]:
    """Server-only validation against its exact immutable DB and manifest chain."""
    with _chain_link(str(proof["build_reference"]["sha256"])):
        return _validate_portable(connection, proof, deployment=deployment, at=at)


def _validate_portable(connection: sqlite3.Connection, proof: Mapping[str, Any], *, deployment: Mapping[str, Any], at: str) -> dict[str, Any]:
    from . import runtime_source_successor as source
    if proof.get("contract") == source.PROOF:
        return source.validate_portable(connection, proof, deployment=deployment, at=at)
    if proof.get("contract") == account.PROOF:
        return account.validate_portable(connection, proof, deployment=deployment, at=at)
    _require(proof.get("contract") in {PROOF, PROOF_V2} and proof.get("proof_sha256") == auth.digest({k: v for k, v in proof.items() if k != "proof_sha256"}),
             "portable successor digest changed")
    plan = proof["plan_payload"]
    version = 2 if plan.get("contract") == PLAN_V2 else 1
    _require(proof["contract"] == (PROOF_V2 if version == 2 else PROOF), "portable plan and proof versions differ")
    plan_bytes = (json.dumps(plan, ensure_ascii=False, indent=2, sort_keys=True) + "\n").encode("utf-8")
    _require(hashlib.sha256(plan_bytes).hexdigest() == proof["plan_reference"]["sha256"]
             and len(plan_bytes) == proof["plan_reference"]["byte_size"]
             and proof["build_reference"]["sha256"] == proof["runtime_bindings"]["build_sha256"]
             and proof["runtime_reference"]["sha256"] == proof["runtime_bindings"]["runtime_sha256"],
             "portable plan or sealed runtime references differ")
    refs = {"plan": proof["plan_reference"], "previous_build": plan.get("installed_parent", plan["previous_build"]), "source_archive": plan["source_archive"],
            "full_checks": plan["full_checks"], "build": proof["build_reference"], "runtime": proof["runtime_reference"]}
    seen: set[str] = set()
    for entry in proof["private_references"]:
        role = entry["role"]
        _require(role in PRIVATE_ROLES and role not in seen and type(entry["byte_size"]) is int and entry["byte_size"] > 0
                 and all(entry[key] == refs[role][key] for key in ("path", "sha256")), "portable private reference differs")
        seen.add(role)
    _require(seen == PRIVATE_ROLES, "portable private reference role is missing")
    active, control = _current_control(connection, at)
    account.control_precheck(plan, active, control)
    _require(deployment["status"] == "accepted" and proof["source_deployment_sha256"] == deployment["receipt_sha256"]
             and plan["source_deployment"] == {"deployment_id": deployment["deployment_id"], "receipt_sha256": deployment["receipt_sha256"]}
             and proof["active"] == plan["active"]
             and plan["release"] == {"id": proof["release_event_id"], "event_hash": proof["release_event_hash"]}
             and plan["operations"] == deployment["release_decision"]["operations"]
             and plan["source_decision_sha256"] == deployment["release_decision"]["decision_sha256"]
             and proof["origin_runtime_bindings"] == deployment["release_decision"]["runtime_bindings"]
             and proof["manifest"] == plan["manifest"] == deployment["release_decision"]["transport_manifest"]
             and proof["runtime_bindings"]["config_sha256"] == proof["origin_runtime_bindings"]["config_sha256"],
             "portable successor changed authorization, transport or current activation")
    account.control_postcheck(connection, plan=plan, deployment=deployment, at=at, portable=True)
    _require(plan.get("contract") in {PLAN, PLAN_V2} and plan.get("business_module") == BUSINESS
             and plan.get("plumbing_allowlist") == sorted(PLUMBING) and BUSINESS in plan["changes"]
             and set(plan["changes"]) <= PLUMBING | {BUSINESS, PIPELINE} | (V2_TESTS | V2_ADDITIONAL if version == 2 else set()),
             "portable source boundary differs")
    if version == 2:
        _require(plan.get("test_allowlist") == sorted(V2_TESTS)
                 and plan.get("additional_source_allowlist") == sorted(V2_ADDITIONAL)
                 and plan.get("required_checks") == sorted(V2_CHECKS)
                 and plan.get("change_scope") == "capture_runtime_lease_v1"
                 and "installed_parent" in plan, "portable runtime successor scope differs")
    if "installed_parent" in plan:
        parent = proof.get("installed_parent_proof")
        _require(isinstance(parent, dict) and (version == 2 or "installed_parent" not in parent["plan_payload"])
                 and parent["build_reference"] == plan["installed_parent"]
                 and parent["plan_payload"]["previous_build"] == plan["previous_build"], "portable installed parent differs")
        assert isinstance(parent, dict)
        validate_portable(connection, parent, deployment=deployment, at=at)
        changes = plan["installed_parent_changes"]
        if version == 2:
            _require(BUSINESS in changes and set(changes) <= PLUMBING | {BUSINESS} | V2_TESTS | V2_ADDITIONAL,
                     "portable runtime successor changes the installed pipeline or another module")
            _require(all(plan["changes"].get(name, {}).get("after_sha256") == delta["after_sha256"]
                         for name, delta in changes.items()), "portable parent delta differs from source archive delta")
            parent_changes = parent["plan_payload"]["changes"]
            _require(all(delta["before_sha256"] == parent_changes[name]["after_sha256"]
                         for name, delta in changes.items() if name in parent_changes), "portable parent source hashes differ")
            if PIPELINE in parent_changes:
                _require(plan["changes"].get(PIPELINE) == parent_changes[PIPELINE], "portable installed pipeline changed")
        else:
            _require(PIPELINE in changes and set(changes) <= PLUMBING | {PIPELINE}
                     and changes[PIPELINE]["after_sha256"] == PIPELINE_SHA256
                     and plan["changes"][PIPELINE]["after_sha256"] == PIPELINE_SHA256,
                     "portable installed parent does not carry the tested pipeline delta")
    else:
        _require(PIPELINE not in plan["changes"] and "installed_parent_proof" not in proof,
                 "portable pipeline successor is missing its installed parent")
    receipt = proof["decision_receipt"]
    _require(isinstance(receipt, dict), "portable successor has no decision")
    row = _decision_row(connection, proof["runtime_bindings"]["build_sha256"])
    _require(row is not None, "portable decision is not in the immutable snapshot")
    assert row is not None
    details = json.loads(row["details_json"])
    from .transport_receipts import _digest as transport_digest
    _require(details == receipt and row["status"] == "succeeded" and row["id"] == receipt["receipt_id"]
             and receipt["payload_sha256"] == transport_digest(receipt["payload"])
             and receipt["self_sha256"] == transport_digest({key: value for key, value in receipt.items() if key not in {"self_sha256", "mirror"}}),
             "portable decision ledger hash differs")
    _require(receipt["payload"] == _decision_payload(plan, plan_sha=proof["plan_reference"]["sha256"],
        build_sha=proof["runtime_bindings"]["build_sha256"], runtime_sha=proof["runtime_bindings"]["runtime_sha256"], at=receipt["recorded_at"])
        and parse_time(plan["issued_at"]) <= parse_time(receipt["recorded_at"]) <= parse_time(at), "portable decision content or time differs")
    from .capture_activation_release import validate_installed_activation_successor
    current = activation_at(connection, at)
    assert current is not None
    validate_installed_activation_successor(connection, source_deployment=deployment, current_active=current,
        runtime_bindings=proof["origin_runtime_bindings"], manifest=proof["manifest"], at=at, portable=True)
    return dict(proof)
