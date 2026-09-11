"""Account profile recovery code successor preserving the original profile permit.

Code-only account profile recovery successor; retain complete Publisher, controls, metric, catalog and historical proofs.

This stdlib verifier is entered only after bootstrap verifies every source file.
No migration, service action, paid gate, database write or qualification occurs.
The source delta remains unusable until it is frozen after review.
"""
from __future__ import annotations

from datetime import datetime, timedelta
import hashlib
import importlib.util
from pathlib import Path
from typing import Any, Mapping

CONTRACT = "account-profile-recovery-code-successor-v1"
TRANSITION = "account-profile-recovery-20260910-v1"
CHECK_CONTRACT = "account-profile-recovery-check-v1"
MODULE = "src/dcar_eval/v8/account_profile_recovery_release.py"
CATALOG_MODULE = "src/dcar_eval/v8/account_catalog_capture_release.py"
METRIC_MODULE = "src/dcar_eval/v8/metric_gap_release.py"
CLASSIFICATION_MODULE = "src/dcar_eval/v8/account_classification_release.py"
CONTROLS_MODULE = "src/dcar_eval/v8/control_simplification_release.py"
PUBLISHER_MODULE = "src/dcar_eval/v8/publisher_snapshot_release.py"
PROFILE_MODULE = "src/dcar_eval/v8/account_profile_release.py"
CAPACITY_MODULE = "src/dcar_eval/v8/publisher_capacity_release.py"
PARENT_VERIFIER_MODULES = (PROFILE_MODULE, CAPACITY_MODULE, CLASSIFICATION_MODULE, PUBLISHER_MODULE, CONTROLS_MODULE, METRIC_MODULE, CATALOG_MODULE,
                           "src/dcar_eval/v8/manual_content_scope_release.py")
PARENT_BUILD_SHA256 = "9a2a6ed1dff356027f366ea471c8f321489476ec07de2b4e62342cd27cde530a"
REQUIRED_CHECKS = frozenset({"account_profile_recovery_behavior", "account_profile_recovery_release"})
REVIEWED_CHANGES: dict[str, dict[str, str | None]] = {'scripts/prepare_account_profile_recovery_release.py': {'before_sha256': None, 'after_sha256': '8e24853e0e56a1d8d0817d312b5935a42db2571fed1fd1f2d266ee17cc5b5bb2'}, 'src/dcar_eval/v8/account_classification_release.py': {'before_sha256': '12844615c60d98abe9e4c136082f91a868289d347cb3065c3c76add8c6dfe7e7', 'after_sha256': '750bb3382f4bafdfbd5c3492119c36860fcb2a2daf6e6a3c09758add05d1b9ce'}, 'src/dcar_eval/v8/account_cleanup_runtime.py': {'before_sha256': 'b69104b2a191f666fdef3909bdab97fcd8109966bb6459938bbe61201a68fc75', 'after_sha256': '489e9c943c4a3c70c7866ff63bc7e09f4d9088f48e8f54ba44d2502205bea1cf'}, 'src/dcar_eval/v8/account_profile_recovery.py': {'before_sha256': None, 'after_sha256': '7b7ec012aa64b7559a1e5703dc3b903f29ed44fc7b348b5e36e092005d028071'}, 'src/dcar_eval/v8/capture_compensation.py': {'before_sha256': '2d15edbedd8a030bd497744a4bccec101b08724f53c6712c92a32ac32c06a7fe', 'after_sha256': '2457daf5e1907799b0f6b14818e667acaa76ddb01d375e84170a0dc0ff870fcf'}, 'src/dcar_eval/v8/capture_release_commands.py': {'before_sha256': '8297c8a53c7d703ca4265752d205f4fa0d17b4d07e4f08049960b9d29d5087b1', 'after_sha256': 'a3e485f2cfa2c26a66f88ccc971d0ab026e7de0d0c67dcddd070636f3468eede'}, 'tests/test_account_profile_recovery_release.py': {'before_sha256': None, 'after_sha256': 'e45c6fe6943d8751670dac49d6cd857e2c0da6306dc295641b51b0997236a89f'}, 'tests/test_v8_catalog_profile_compensation.py': {'before_sha256': None, 'after_sha256': '009de5310dff1fbbf514c4b1cff2a77dd3a028bc502f0241cdfd8ddacbf1c81b'}}
AUTHORIZATION_CONTRACT = "account-profile-compensation-user-authorization-v1"
AUTHORITY_CONTRACT = "account-profile-compensation-authority-v1"
BASELINE_SHA256 = "dad90e9d01d834a8081e47de819c847fcb6d6f272d73cf7a5a664bb9ceaf5d9f"
COMPENSATION_TARGETS = ((2492, 99833, 127367), (2496, 99834, 127369),
                        (2580, 99859, 127393), (2647, 99887, 127421))
_LOADED_SOURCE = Path(__file__).read_bytes()


def _load(path: Path, name: str, body: bytes | None = None):
    spec = importlib.util.spec_from_file_location(name, path)
    if spec is None or spec.loader is None:
        raise ValueError("publisher release: verifier unavailable")
    result = importlib.util.module_from_spec(spec)
    if body is None:
        spec.loader.exec_module(result)
    else:
        exec(compile(body, str(path), "exec"), result.__dict__)
    return result


# Share the existing checked-file, JSON, manifest and hash primitives. The
# catalog verifier itself is unchanged by this successor's source delta.
_shared = _load(Path(__file__).with_name("account_catalog_capture_release.py"), "publisher_release_shared")
require, digest, raw = _shared.require, _shared.digest, _shared.raw
reference, object_at, payload_at, records = _shared.reference, _shared.object_at, _shared.payload_at, _shared.records


def _ready() -> None:
    require(len(PARENT_BUILD_SHA256) == 64 and all(c in "0123456789abcdef" for c in PARENT_BUILD_SHA256),
            "PENDING: published parent hash has not been reviewed")


def source_changes(parent: Mapping[str, Any], current: Mapping[str, Any]) -> dict[str, Any]:
    _ready()
    left, right = records(parent), records(current)
    require(MODULE not in left and MODULE in right
            and right[MODULE]["sha256"] == hashlib.sha256(_LOADED_SOURCE).hexdigest(), "account profile recovery module differs")
    changes = {}
    for name in sorted(set(left) | set(right)):
        old, new = left.get(name), right.get(name)
        if old == new:
            continue
        require(old is None or new is None or old["mode"] == new["mode"], "source mode changed")
        old_sha, new_sha = (row["sha256"] if row else None for row in (old, new))
        require(old_sha != new_sha, "metadata changed without source change")
        changes[name] = {"before_sha256": old_sha, "after_sha256": new_sha}
    require(bool(REVIEWED_CHANGES) and MODULE not in REVIEWED_CHANGES, "PENDING: final account profile recovery delta is not frozen")
    require(not (((set(PARENT_VERIFIER_MODULES) - {CLASSIFICATION_MODULE}) | {"src/dcar_eval/v8/account_profile_authority.py"}) & changes.keys()),
            "published inheritance verifiers must remain unchanged")
    expected = {**REVIEWED_CHANGES,
                MODULE: {"before_sha256": None, "after_sha256": hashlib.sha256(_LOADED_SOURCE).hexdigest()}}
    require(changes == expected, "source delta differs from reviewed account profile recovery transition")
    return changes


def parent_context(parent_ref: Mapping[str, Any], *, install_path: Path, database: Path,
                   at: str | None = None) -> tuple[dict[str, Any], dict[str, Any]]:
    _ready()
    require(parent_ref.get("sha256") == PARENT_BUILD_SHA256, "installed parent is not the reviewed build")
    parent = payload_at(parent_ref)
    require(parent.get("status") == "succeeded"
            and parent.get("schema_contract") == {"code_schema": 21, "formal_schema": 21}
            and isinstance(parent.get("account_catalog_capture_successor"), dict)
            and isinstance(parent.get("metric_gap_successor"), dict)
            and isinstance(parent.get("control_simplification_successor"), dict)
            and isinstance(parent.get("publisher_snapshot_successor"), dict)
            and isinstance(parent.get("publisher_capacity_successor"), dict)
            and isinstance(parent.get("account_profile_successor"), dict)
            and "account_profile_recovery_successor" not in parent,
            "parent must be the published schema21 Publisher generation")
    source = Path(parent["source_root"])
    # The historical dispatcher assumes bootstrap already hashed its dynamic
    # dependencies. Check each verifier it can reach before entering that chain.
    bodies = {}
    for name in PARENT_VERIFIER_MODULES:
        bodies[name] = raw(source / name, private=False)
        require(hashlib.sha256(bodies[name]).hexdigest() == parent["critical_files"].get(name),
                "parent verifier changed: " + name)
    verifier = _load(source / CLASSIFICATION_MODULE, "account_profile_recovery_parent_classification",
                     bodies[CLASSIFICATION_MODULE])
    inherited = verifier.verify_inheritance(build=parent, build_ref=parent_ref, install_path=install_path,
                                           database=database, source=source, at=at)
    require(isinstance(inherited.get("catalog_capture_proof"), dict)
            and inherited.get("catalog_capture_policy") == _shared.ACCOUNT_CATALOG_POLICY
            and inherited.get("catalog_capture_policy_sha256") == digest(_shared.ACCOUNT_CATALOG_POLICY),
            "complete verified catalog inheritance is required")
    publisher = inherited.get("publisher_snapshot_proof")
    controls = inherited.get("control_simplification_proof")
    require(isinstance(publisher, dict) and publisher.get("loaded_build") == parent["publisher_capacity_successor"]["parent_build"]
            and publisher.get("parent_build") == parent["publisher_snapshot_successor"]["parent_build"]
            and isinstance(controls, dict)
            and controls.get("loaded_build") == parent["publisher_snapshot_successor"]["parent_build"]
            and isinstance(inherited.get("metric_gap_proof"), dict)
            and inherited.get("publisher_capacity_proof", {}).get("loaded_build") == parent["account_profile_successor"]["parent_build"]
            and inherited.get("account_profile_proof", {}).get("loaded_build") == dict(parent_ref)
            and inherited.get("profile_operation_authority", {}).get("loaded_build") == dict(parent_ref)
            and inherited["profile_operation_authority"].get("authorization") == parent["account_profile_successor"]["authorization"],
            "complete verified Publisher inheritance is required")
    return parent, inherited


def cohort_targets(cohort_ref: Mapping[str, Any], database: Path) -> tuple[list[int], list[dict[str, int]]]:
    """Bind the four reviewed failed requests to the unchanged original 111."""
    require(cohort_ref.get("sha256") == BASELINE_SHA256, "original profile cohort differs")
    cohort = object_at(cohort_ref)
    rows = cohort.get("work")
    identity = database.stat()
    require(cohort.get("count") == 111 and isinstance(rows, list) and len(rows) == 111
            and cohort.get("database_identity") == {"path": str(database), "device": identity.st_dev, "inode": identity.st_ino},
            "original profile cohort or database identity differs")
    require(all(isinstance(row, dict) and all(type(row.get(key)) is int and row[key] > 0
                for key in ("id", "source_plan_id", "identity_id")) for row in rows), "cohort work is invalid")
    by_id = {row["id"]: row for row in rows}
    require(len(by_id) == 111 and all(work_id in by_id for work_id, _, _ in COMPENSATION_TARGETS),
            "compensation target is outside the original cohort")
    targets = [{"work_id": work_id, "source_plan_id": by_id[work_id]["source_plan_id"],
                "identity_id": by_id[work_id]["identity_id"], "usage_id": usage_id, "raw_response_id": raw_id}
               for work_id, usage_id, raw_id in COMPENSATION_TARGETS]
    return sorted(by_id), targets


def verify_inheritance(*, build: Mapping[str, Any], build_ref: Mapping[str, Any],
                       install_path: Path, database: Path, source: Path,
                       at: str | None = None) -> dict[str, Any]:
    plan = build.get("account_profile_recovery_successor", {})
    require(plan.get("contract") == CONTRACT and plan.get("transition") == TRANSITION, "account profile recovery successor contract differs")
    require(dict(build) == payload_at(build_ref), "loaded build differs")
    parent, inherited = parent_context(plan["parent_build"], install_path=install_path, database=database, at=at)
    allowed = {"source_root", "git", "critical_files", "code_successor_plan", "account_cleanup_generation",
               "account_profile_recovery_successor", "created_at", "validation_scope"}
    require({k: v for k, v in build.items() if k not in allowed}
            == {k: v for k, v in parent.items() if k not in allowed}, "catalog policy, schema or historical authority changed")
    generation = build["account_cleanup_generation"]
    require({k: v for k, v in generation.items() if k != "source_tree"}
            == {k: v for k, v in parent["account_cleanup_generation"].items() if k != "source_tree"},
            "legacy operator authority changed")
    require(build["source_root"] == str(source) and str(source) != parent["source_root"]
            and generation["source_tree"] == plan["source_tree"], "new source binding differs")
    tree = object_at(plan["source_tree"])
    original = object_at(parent["account_cleanup_generation"]["source_tree"])
    require(tree["source_root"] == str(source) and tree["git"] == build["git"]
            and original["source_root"] == parent["source_root"] and original["git"] == parent["git"], "source manifest differs")
    changes = source_changes(original, tree)
    require(plan.get("changes") == changes, "reviewed account profile recovery change list differs")
    require(object_at(build["code_successor_plan"]) == {
        "contract": "account-cleanup-source-plan-v1", "transition": "account-cleanup-0907-v1",
        "project_root": build["project_root"], "source_root": str(source), "git": build["git"],
        "source_tree": plan["source_tree"]}, "source plan differs")
    require(build["critical_files"] == {name: row["sha256"] for name, row in records(tree).items()
            if name.startswith(("src/", "config/")) and name.endswith((".py", ".json"))}, "critical source inventory differs")
    require(plan.get("schema_migration_repeated") is False and plan.get("provider_qualification_repeated") is False
            and plan.get("database_writes") == 0 and plan.get("paid_gates_reopened") is False
            and plan.get("business_scope_change") == "none"
            and plan.get("transport_qualification") == "not_verified"
            and plan.get("production_rollout") == "approved_by_user", "code-only release scope differs")
    require(isinstance(plan.get("actor"), str) and bool(plan["actor"].strip())
            and isinstance(plan.get("reason"), str) and bool(plan["reason"].strip()), "release owner and reason required")
    require(plan.get("inherited_catalog_proof_sha256") == inherited["catalog_capture_proof"]["proof_sha256"],
            "inherited catalog proof differs")
    require(plan.get("inherited_control_simplification_proof_sha256")
            == inherited["control_simplification_proof"]["proof_sha256"], "inherited controls proof differs")
    require(plan.get("inherited_publisher_snapshot_proof_sha256")
            == inherited["publisher_snapshot_proof"]["proof_sha256"], "inherited Publisher proof differs")
    for plan_key, proof_key in (("inherited_publisher_capacity_proof_sha256", "publisher_capacity_proof"),
                                ("inherited_account_profile_proof_sha256", "account_profile_proof"),
                                ("inherited_profile_operation_authority_sha256", "profile_operation_authority")):
        require(plan.get(plan_key) == inherited[proof_key]["proof_sha256"], "inherited profile authority or capacity proof differs")
    authorization = object_at(plan["authorization"])
    original_work_ids, targets = cohort_targets(authorization.get("original_cohort", {}), database)
    expected = {"contract": AUTHORIZATION_CONTRACT, "production_rollout": "approved_by_user",
        "business_e2e": "required", "transport_qualification": "not_verified",
        "parent_build": plan["parent_build"], "source_tree": plan["source_tree"],
        "catalog_policy_sha256": inherited["catalog_capture_policy_sha256"],
        "original_cohort_sha256": BASELINE_SHA256, "original_work_ids": original_work_ids,
        "max_starts": 4, "max_total_microusd": 4000, "max_amount_microusd": 1000,
        "targets": targets, "actor": plan["actor"], "reason": plan["reason"], "issued_at": plan["issued_at"]}
    require(all(authorization.get(key) == value for key, value in expected.items())
            and all(isinstance(authorization.get(key), str) and authorization[key].strip()
                    for key in ("user_instruction", "source_thread_id", "expires_at")),
            "bounded profile compensation authorization differs")
    issued = datetime.fromisoformat(plan["issued_at"].replace("Z", "+00:00"))
    expires = datetime.fromisoformat(authorization["expires_at"].replace("Z", "+00:00"))
    require(expires.tzinfo is not None and issued < expires <= issued + timedelta(hours=24),
            "compensation authorization expiry differs")
    previous = datetime.fromisoformat(parent["created_at"].replace("Z", "+00:00"))
    require(issued.tzinfo is not None and previous <= issued and build["created_at"] == plan["issued_at"], "invalid successor time")
    if at is not None:
        require(issued <= datetime.fromisoformat(at.replace("Z", "+00:00")), "future dated successor")
    checks = plan.get("checks", {})
    require(set(checks) == REQUIRED_CHECKS, "focused checks are incomplete")
    for name, ref in checks.items():
        report = object_at(ref)
        require(report.get("contract") == CHECK_CONTRACT and report.get("name") == name
                and report.get("status") == "passed" and report.get("exit_code") == 0
                and report.get("changes") == changes and report.get("command"), "focused check does not bind source")
        require(reference(Path(report["output"]["path"])) == report["output"], "check output changed")
    proof = {"contract": CONTRACT, "transition": TRANSITION, "loaded_build": dict(build_ref),
             "parent_build": plan["parent_build"], "source_tree": plan["source_tree"], "changes": changes,
             "checks": checks, "issued_at": plan["issued_at"], "actor": plan["actor"], "reason": plan["reason"],
             "inherited_catalog_proof_sha256": plan["inherited_catalog_proof_sha256"],
             "inherited_control_simplification_proof_sha256": plan["inherited_control_simplification_proof_sha256"],
             "inherited_publisher_snapshot_proof_sha256": plan["inherited_publisher_snapshot_proof_sha256"],
             "authorization": plan["authorization"],
             **{key: plan[key] for key in ("inherited_publisher_capacity_proof_sha256", "inherited_account_profile_proof_sha256", "inherited_profile_operation_authority_sha256")}}
    proof["proof_sha256"] = digest(proof)
    authority = {"contract": AUTHORITY_CONTRACT, "authorization": plan["authorization"],
        "authorization_payload": authorization, "loaded_build": dict(build_ref),
        "source_tree": plan["source_tree"], "parent_build": plan["parent_build"],
        "catalog_policy_sha256": inherited["catalog_capture_policy_sha256"],
        "profile_authority_proof_sha256": inherited["profile_operation_authority"]["proof_sha256"]}
    authority["proof_sha256"] = digest(authority)
    # Preserve the original profile decision bytes and gate; this new authority
    # is consulted only for an explicit, bounded compensation command.
    return {**inherited, "account_profile_recovery_proof": proof, "profile_compensation_authority": authority}
