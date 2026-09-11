"""Explicit account profile successor; retain complete Publisher, controls, metric, catalog and historical proofs.

This stdlib verifier is entered only after bootstrap verifies every source file.
Preparation adds only an explicit profile authorization proof; no migration, gate, DB write or qualification occurs.
The source delta remains unusable until it is frozen after review.
"""
from __future__ import annotations

from datetime import datetime
import hashlib
import importlib.util
from pathlib import Path
from typing import Any, Mapping

CONTRACT = "account-profile-code-successor-v1"
TRANSITION = "account-profile-20260910-v1"
CHECK_CONTRACT = "account-profile-check-v1"
MODULE = "src/dcar_eval/v8/account_profile_release.py"
CATALOG_MODULE = "src/dcar_eval/v8/account_catalog_capture_release.py"
METRIC_MODULE = "src/dcar_eval/v8/metric_gap_release.py"
CLASSIFICATION_MODULE = "src/dcar_eval/v8/account_classification_release.py"
CONTROLS_MODULE = "src/dcar_eval/v8/control_simplification_release.py"
CAPACITY_MODULE = "src/dcar_eval/v8/publisher_capacity_release.py"
PUBLISHER_MODULE = "src/dcar_eval/v8/publisher_snapshot_release.py"
PARENT_VERIFIER_MODULES = (CAPACITY_MODULE, CLASSIFICATION_MODULE, PUBLISHER_MODULE, CONTROLS_MODULE, METRIC_MODULE, CATALOG_MODULE,
                           "src/dcar_eval/v8/manual_content_scope_release.py")
PARENT_BUILD_SHA256 = "ad7062a70e1526029892135ba0532f903f7e230c93dc96a87c1219b97b06bd3b"
REQUIRED_CHECKS = frozenset({"account_profile_behavior", "account_profile_release"})
REVIEWED_CHANGES: dict[str, dict[str, str | None]] = {'scripts/prepare_account_profile_release.py': {'before_sha256': None, 'after_sha256': '06969d589c7a657147767159b0d8836d90d67547c11366906f64f0cdccfdc85a'}, 'src/dcar_eval/v8/account_classification_release.py': {'before_sha256': '8e9b3421d2b91c45508c1bce2d9b302b63a1e59a79b38aea0baef135550085b2', 'after_sha256': '12844615c60d98abe9e4c136082f91a868289d347cb3065c3c76add8c6dfe7e7'}, 'src/dcar_eval/v8/account_cleanup_runtime.py': {'before_sha256': 'ce5443bf91e6feb8c211e388b3982f97bcf506340fe3a0ec4e6f29c5950f1a9d', 'after_sha256': 'b69104b2a191f666fdef3909bdab97fcd8109966bb6459938bbe61201a68fc75'}, 'src/dcar_eval/v8/account_profile_authority.py': {'before_sha256': None, 'after_sha256': '6b9ebc9fd44d0d6d468c370140215f913f8f38fd4ea6a43b987fa0d683d7a5fc'}, 'src/dcar_eval/v8/capture_operator_release.py': {'before_sha256': '60408bf2cc1b05aea2250ee585b0a6707e681b3cd807c54194990186ce52917a', 'after_sha256': '4b35bed554323a95bf46dbf2d0c785c19465fcf6eec695721a054edb0cbbd416'}, 'src/dcar_eval/v8/capture_release.py': {'before_sha256': '84e7458d6aa43af7dcb120d4abd37b767a719582fafd19d55a618a45fbdd1cca', 'after_sha256': '098ecff52683f42ba691bc18063b622d2035495adbc5bdff568565b678498fb8'}, 'src/dcar_eval/v8/capture_runtime.py': {'before_sha256': '2725ac1498c46a8bcd4d8260ec24551a39bd0b4f7a607d3f0a0d07bc733555ac', 'after_sha256': '13f1c75c2b91754d2bc0020021e5592bf2c750df038ba962c8d4d858170cdf1e'}, 'tests/test_account_profile_release.py': {'before_sha256': None, 'after_sha256': '2827ab77a321ec0828024a76381d6965f757b62af738324ad2a8a3ec6f902e33'}, 'tests/test_v8_account_profile_authority.py': {'before_sha256': None, 'after_sha256': 'e7698d9cb900a7fddbb280347ab6896bfafda4ecfaeebedf1cff8a51b55d8e08'}, 'tests/test_v8_catalog_profile_execution.py': {'before_sha256': None, 'after_sha256': '0f7ed4c640bf8fef3a394494db24d61714593131f595f2b68a31f1b2f5d809e6'}}
AUTHORIZATION_CONTRACT = "account-profile-user-authorization-v1"
AUTHORITY_CONTRACT = "account-profile-operator-authority-v1"
OPERATIONS = ["douyin_uid_profile"]
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
            and right[MODULE]["sha256"] == hashlib.sha256(_LOADED_SOURCE).hexdigest(), "account profile module differs")
    changes = {}
    for name in sorted(set(left) | set(right)):
        old, new = left.get(name), right.get(name)
        if old == new:
            continue
        require(old is None or new is None or old["mode"] == new["mode"], "source mode changed")
        old_sha, new_sha = (row["sha256"] if row else None for row in (old, new))
        require(old_sha != new_sha, "metadata changed without source change")
        changes[name] = {"before_sha256": old_sha, "after_sha256": new_sha}
    require(bool(REVIEWED_CHANGES) and MODULE not in REVIEWED_CHANGES, "PENDING: final account profile delta is not frozen")
    require(not ((set(PARENT_VERIFIER_MODULES) - {CLASSIFICATION_MODULE}) & changes.keys()),
            "published inheritance verifiers must remain unchanged")
    expected = {**REVIEWED_CHANGES,
                MODULE: {"before_sha256": None, "after_sha256": hashlib.sha256(_LOADED_SOURCE).hexdigest()}}
    require(changes == expected, "source delta differs from reviewed account profile transition")
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
            and "account_profile_successor" not in parent,
            "parent must be the published schema21 Publisher generation")
    source = Path(parent["source_root"])
    # The historical dispatcher assumes bootstrap already hashed its dynamic
    # dependencies. Check each verifier it can reach before entering that chain.
    bodies = {}
    for name in PARENT_VERIFIER_MODULES:
        bodies[name] = raw(source / name, private=False)
        require(hashlib.sha256(bodies[name]).hexdigest() == parent["critical_files"].get(name),
                "parent verifier changed: " + name)
    verifier = _load(source / CLASSIFICATION_MODULE, "account_profile_parent_classification",
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
            and inherited.get("publisher_capacity_proof", {}).get("loaded_build") == dict(parent_ref),
            "complete verified Publisher inheritance is required")
    return parent, inherited


def verify_inheritance(*, build: Mapping[str, Any], build_ref: Mapping[str, Any],
                       install_path: Path, database: Path, source: Path,
                       at: str | None = None) -> dict[str, Any]:
    plan = build.get("account_profile_successor", {})
    require(plan.get("contract") == CONTRACT and plan.get("transition") == TRANSITION, "account profile successor contract differs")
    require(dict(build) == payload_at(build_ref), "loaded build differs")
    parent, inherited = parent_context(plan["parent_build"], install_path=install_path, database=database, at=at)
    allowed = {"source_root", "git", "critical_files", "code_successor_plan", "account_cleanup_generation",
               "account_profile_successor", "created_at", "validation_scope"}
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
    require(plan.get("changes") == changes, "reviewed account profile change list differs")
    require(object_at(build["code_successor_plan"]) == {
        "contract": "account-cleanup-source-plan-v1", "transition": "account-cleanup-0907-v1",
        "project_root": build["project_root"], "source_root": str(source), "git": build["git"],
        "source_tree": plan["source_tree"]}, "source plan differs")
    require(build["critical_files"] == {name: row["sha256"] for name, row in records(tree).items()
            if name.startswith(("src/", "config/")) and name.endswith((".py", ".json"))}, "critical source inventory differs")
    require(plan.get("schema_migration_repeated") is False and plan.get("provider_qualification_repeated") is False
            and plan.get("database_writes") == 0 and plan.get("paid_gates_reopened") is False
            and plan.get("business_scope_change") == "explicit_profile_operation"
            and plan.get("business_e2e") == "required"
            and plan.get("transport_qualification") == "not_verified"
            and plan.get("production_rollout") == "approved_by_user", "profile release scope differs")
    require(isinstance(plan.get("actor"), str) and bool(plan["actor"].strip())
            and isinstance(plan.get("reason"), str) and bool(plan["reason"].strip()), "release owner and reason required")
    require(plan.get("inherited_catalog_proof_sha256") == inherited["catalog_capture_proof"]["proof_sha256"],
            "inherited catalog proof differs")
    require(plan.get("inherited_control_simplification_proof_sha256")
            == inherited["control_simplification_proof"]["proof_sha256"], "inherited controls proof differs")
    require(plan.get("inherited_publisher_snapshot_proof_sha256")
            == inherited["publisher_snapshot_proof"]["proof_sha256"], "inherited Publisher proof differs")
    require(plan.get("inherited_publisher_capacity_proof_sha256")
            == inherited["publisher_capacity_proof"]["proof_sha256"], "inherited capacity proof differs")
    authorization = object_at(plan["authorization"])
    identity = database.stat()
    expected_bindings = {
        "contract": AUTHORIZATION_CONTRACT, "operations": OPERATIONS,
        "production_rollout": "approved_by_user", "business_e2e": "required",
        "transport_qualification": "not_verified", "scope": "account_catalog_eligible",
        "parent_build": plan["parent_build"], "source_tree": plan["source_tree"],
        "catalog_policy_sha256": inherited["catalog_capture_policy_sha256"],
        "formal_database": {"path": str(database), "device": identity.st_dev, "inode": identity.st_ino},
        "actor": plan["actor"], "reason": plan["reason"], "issued_at": plan["issued_at"]}
    require(all(authorization.get(key) == value for key, value in expected_bindings.items())
            and isinstance(authorization.get("user_instruction"), str) and bool(authorization["user_instruction"].strip())
            and isinstance(authorization.get("source_thread_id"), str) and bool(authorization["source_thread_id"].strip()),
            "explicit profile authorization scope or binding differs")
    issued = datetime.fromisoformat(plan["issued_at"].replace("Z", "+00:00"))
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
             "inherited_publisher_capacity_proof_sha256": plan["inherited_publisher_capacity_proof_sha256"],
             "authorization": plan["authorization"]}
    proof["proof_sha256"] = digest(proof)
    authority = {"contract": AUTHORITY_CONTRACT, "operations": OPERATIONS,
        "authorization": plan["authorization"], "authorization_payload": authorization,
        "loaded_build": dict(build_ref), "source_tree": plan["source_tree"],
        "runtime_root_receipt": parent["runtime_root_receipt"],
        "config_sha256": parent["account_cleanup_generation"]["config_sha256"],
        "transport_manifest": parent["account_cleanup_generation"]["transport_manifest"],
        "catalog_policy_sha256": inherited["catalog_capture_policy_sha256"], "parent_build": plan["parent_build"],
        "inherited_authority_proof_sha256": inherited["catalog_capture_proof"]["proof_sha256"],
        "issued_at": plan["issued_at"]}
    authority["proof_sha256"] = digest(authority)
    return {**inherited, "account_profile_proof": proof, "profile_operation_authority": authority}
