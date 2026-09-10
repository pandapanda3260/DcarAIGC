"""Code-only publisher capacity successor; retain complete Publisher, controls, metric, catalog and historical proofs.

This stdlib verifier is entered only after bootstrap verifies every source file.
No migration, service action, paid gate, database write or qualification occurs.
The source delta remains unusable until it is frozen after review.
"""
from __future__ import annotations

from datetime import datetime
import hashlib
import importlib.util
from pathlib import Path
from typing import Any, Mapping

CONTRACT = "publisher-capacity-code-successor-v1"
TRANSITION = "publisher-capacity-20260910-v1"
CHECK_CONTRACT = "publisher-capacity-check-v1"
MODULE = "src/dcar_eval/v8/publisher_capacity_release.py"
CATALOG_MODULE = "src/dcar_eval/v8/account_catalog_capture_release.py"
METRIC_MODULE = "src/dcar_eval/v8/metric_gap_release.py"
CLASSIFICATION_MODULE = "src/dcar_eval/v8/account_classification_release.py"
CONTROLS_MODULE = "src/dcar_eval/v8/control_simplification_release.py"
PUBLISHER_MODULE = "src/dcar_eval/v8/publisher_snapshot_release.py"
PARENT_VERIFIER_MODULES = (CLASSIFICATION_MODULE, PUBLISHER_MODULE, CONTROLS_MODULE, METRIC_MODULE, CATALOG_MODULE,
                           "src/dcar_eval/v8/manual_content_scope_release.py")
PARENT_BUILD_SHA256 = "34e308e25200e4064e913d606bfefa6505d541d55f12dd8c5e4ff87946501a18"
REQUIRED_CHECKS = frozenset({"publisher_capacity_behavior", "publisher_capacity_release"})
REVIEWED_CHANGES: dict[str, dict[str, str | None]] = {'deploy/macos/publish_snapshot.py': {'before_sha256': 'f7668a8e84aec2cfe80238970d19426a61561591fa63c989ff3d21ec430c29df', 'after_sha256': '09256cf1682296694aa305ae2236eeb8db462e91bc4f4e35adeb38f618a24150'}, 'scripts/prepare_publisher_capacity_release.py': {'before_sha256': None, 'after_sha256': 'f18593c9253d1358130da82f1af275610f8f1a9a6d2b6243940f793975ede344'}, 'src/dcar_eval/v8/account_classification_release.py': {'before_sha256': '06246691cfd4ecfe76db29737d5c6b1728bbfa6b063416c17c38692bf0930bf9', 'after_sha256': '8e9b3421d2b91c45508c1bce2d9b302b63a1e59a79b38aea0baef135550085b2'}, 'tests/test_macos_snapshot_bundle_delta.py': {'before_sha256': '4c1a1e2e6406124216046d83bf5ed77f9d0206a142a70ff85ebd896c258efb6b', 'after_sha256': 'd01fcee1660566a59db71d61eb69d95ea547a5f0ddfbc93b49ebba2ba1b2d3f0'}, 'tests/test_macos_snapshot_publisher.py': {'before_sha256': '609cdf2e4ec6154b96895bf3ff0cbf76eb75d752ae7e4b206d0410092ce0942c', 'after_sha256': '3f1fa038940492f9b9bd9274bbf8b8f6bc58712259822bb3721c0ba3a001f026'}, 'tests/test_publisher_capacity_release.py': {'before_sha256': None, 'after_sha256': 'a63eb39f5fcca5034ef99f9be4b95179909fd8c83ef0805c80943ab49008defb'}}
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
            and right[MODULE]["sha256"] == hashlib.sha256(_LOADED_SOURCE).hexdigest(), "publisher capacity module differs")
    changes = {}
    for name in sorted(set(left) | set(right)):
        old, new = left.get(name), right.get(name)
        if old == new:
            continue
        require(old is None or new is None or old["mode"] == new["mode"], "source mode changed")
        old_sha, new_sha = (row["sha256"] if row else None for row in (old, new))
        require(old_sha != new_sha, "metadata changed without source change")
        changes[name] = {"before_sha256": old_sha, "after_sha256": new_sha}
    require(bool(REVIEWED_CHANGES) and MODULE not in REVIEWED_CHANGES, "PENDING: final publisher capacity delta is not frozen")
    require(not ((set(PARENT_VERIFIER_MODULES) - {CLASSIFICATION_MODULE}) & changes.keys()),
            "published inheritance verifiers must remain unchanged")
    expected = {**REVIEWED_CHANGES,
                MODULE: {"before_sha256": None, "after_sha256": hashlib.sha256(_LOADED_SOURCE).hexdigest()}}
    require(changes == expected, "source delta differs from reviewed publisher capacity transition")
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
            and "publisher_capacity_successor" not in parent,
            "parent must be the published schema21 Publisher generation")
    source = Path(parent["source_root"])
    # The historical dispatcher assumes bootstrap already hashed its dynamic
    # dependencies. Check each verifier it can reach before entering that chain.
    bodies = {}
    for name in PARENT_VERIFIER_MODULES:
        bodies[name] = raw(source / name, private=False)
        require(hashlib.sha256(bodies[name]).hexdigest() == parent["critical_files"].get(name),
                "parent verifier changed: " + name)
    verifier = _load(source / CLASSIFICATION_MODULE, "publisher_capacity_parent_classification",
                     bodies[CLASSIFICATION_MODULE])
    inherited = verifier.verify_inheritance(build=parent, build_ref=parent_ref, install_path=install_path,
                                           database=database, source=source, at=at)
    require(isinstance(inherited.get("catalog_capture_proof"), dict)
            and inherited.get("catalog_capture_policy") == _shared.ACCOUNT_CATALOG_POLICY
            and inherited.get("catalog_capture_policy_sha256") == digest(_shared.ACCOUNT_CATALOG_POLICY),
            "complete verified catalog inheritance is required")
    publisher = inherited.get("publisher_snapshot_proof")
    controls = inherited.get("control_simplification_proof")
    require(isinstance(publisher, dict) and publisher.get("loaded_build") == dict(parent_ref)
            and publisher.get("parent_build") == parent["publisher_snapshot_successor"]["parent_build"]
            and isinstance(controls, dict)
            and controls.get("loaded_build") == parent["publisher_snapshot_successor"]["parent_build"]
            and isinstance(inherited.get("metric_gap_proof"), dict),
            "complete verified Publisher inheritance is required")
    return parent, inherited


def verify_inheritance(*, build: Mapping[str, Any], build_ref: Mapping[str, Any],
                       install_path: Path, database: Path, source: Path,
                       at: str | None = None) -> dict[str, Any]:
    plan = build.get("publisher_capacity_successor", {})
    require(plan.get("contract") == CONTRACT and plan.get("transition") == TRANSITION, "publisher capacity successor contract differs")
    require(dict(build) == payload_at(build_ref), "loaded build differs")
    parent, inherited = parent_context(plan["parent_build"], install_path=install_path, database=database, at=at)
    allowed = {"source_root", "git", "critical_files", "code_successor_plan", "account_cleanup_generation",
               "publisher_capacity_successor", "created_at", "validation_scope"}
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
    require(plan.get("changes") == changes, "reviewed publisher capacity change list differs")
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
             "inherited_publisher_snapshot_proof_sha256": plan["inherited_publisher_snapshot_proof_sha256"]}
    proof["proof_sha256"] = digest(proof)
    return {**inherited, "publisher_capacity_proof": proof}
