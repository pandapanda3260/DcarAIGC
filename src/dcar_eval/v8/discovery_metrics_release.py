"""Exact discovery_metrics-only successor; inherit the installed daily release unchanged.

Bootstrap verifies the complete new tree before this module is entered. Parent
verification executes the immutable published verifier and retains every proof;
this module neither opens the database nor grants or renews any authority.
"""
from __future__ import annotations

from datetime import datetime
import hashlib
import importlib.util
from pathlib import Path
import stat
from typing import Any, Mapping

CONTRACT = "discovery-metrics-code-successor-v1"
TRANSITION = "discovery-metrics-20260912-v2"
CHECK_CONTRACT = "discovery-metrics-release-check-v1"
MODULE = "src/dcar_eval/v8/discovery_metrics_release.py"
PARENT_BUILD_SHA256 = "bdc2baee628b10ca32d5603b17eed407e88d1e54d189bd3a612adfe6e09acc8f"
PARENT_TRANSITION = "overview-four-platforms-20260912-v1"
CLASSIFICATION_MODULE = "src/dcar_eval/v8/account_classification_release.py"
DAILY_MODULE = "src/dcar_eval/v8/daily_pipeline_release.py"
OVERVIEW_MODULE = "src/dcar_eval/v8/overview_release.py"
REQUIRED_CHECKS = frozenset({"discovery_metrics_behavior", "discovery_metrics_release"})
INHERITED_PROOFS = {
    "inherited_catalog_proof_sha256": "catalog_capture_proof",
    "inherited_control_simplification_proof_sha256": "control_simplification_proof",
    "inherited_publisher_snapshot_proof_sha256": "publisher_snapshot_proof",
    "inherited_publisher_capacity_proof_sha256": "publisher_capacity_proof",
    "inherited_account_profile_proof_sha256": "account_profile_proof",
    "inherited_profile_operation_authority_sha256": "profile_operation_authority",
    "profile_compensation_authority_sha256": "profile_compensation_authority",
    "account_profile_recovery_proof_sha256": "account_profile_recovery_proof",
    "inherited_metric_gap_proof_sha256": "metric_gap_proof",
    "inherited_manual_content_scope_proof_sha256": "manual_content_scope_proof",
    "inherited_account_classification_proof_sha256": "proof",
    "inherited_previous_daily_pipeline_proof_sha256": "previous_daily_pipeline_proof",
    "inherited_published_daily_pipeline_proof_sha256": "published_daily_pipeline_proof",
    "inherited_daily_pipeline_proof_sha256": "daily_pipeline_proof",
    "inherited_overview_proof_sha256": "overview_proof",
}
REVIEWED_CHANGES: dict[str, dict[str, str | None]] = {'config/source_routing_matrix_first_v2.json': {'before_sha256': '6d72825740f311d10f2cf8ed46dff0b6f28b8cc4c6a8f6e957b7c5a21c67c58a', 'after_sha256': '34ceefa89c1b24ee75d92465bac47c327d70d2b751db29c4a33e775bdf94eb7e'}, 'docs/v8/discovery_metrics.md': {'before_sha256': None, 'after_sha256': '846ac338e7c1126f9834dda17dbfa8aa320da827b47267633a1881fb8de8b45c'}, 'scripts/deploy_discovery_metrics_release.py': {'before_sha256': None, 'after_sha256': 'cbbac009e61a2bb7627b9c1ac11ae1d95ad3f29501a2676c0084f52003547246'}, 'scripts/prepare_discovery_metrics_release.py': {'before_sha256': None, 'after_sha256': '2879e13df28514aa9406279303065f1906c49bf3de9f8f8df597259d00f54d80'}, 'src/dcar_eval/v8/account_classification_release.py': {'before_sha256': '395c4890b59f285618276703b08ecad2c793470461cc0c4462790ac001c01425', 'after_sha256': '93b07bf6a24f890c986c6cff42c6f08c87b00d238e68cbe86995be904cb8fed6'}, 'src/dcar_eval/v8/capture_runtime.py': {'before_sha256': '13f1c75c2b91754d2bc0020021e5592bf2c750df038ba962c8d4d858170cdf1e', 'after_sha256': '0c10f95fb7b545775b17fe3b2283eb9fe470134f63468f1b4865e6159c6ad027'}, 'src/dcar_eval/v8/provider_updates.py': {'before_sha256': '660c91f1a99ead65f3a3156883fecc172f9f3bec4703368e42ad37d8ae20c043', 'after_sha256': '4c369f4053c3d6b5eea2b47fe45831aa8e9b16409a192a64ef8c27029a7558b8'}, 'src/dcar_eval/v8/providers.py': {'before_sha256': '70aba5a58f26b10d1702027e57608ad62a245f92001a595fc06e81a6f3406435', 'after_sha256': '63915d274b6216666a78be415267559928267a97901664015e2d591aa8497ba1'}, 'tests/test_deploy_discovery_metrics_release.py': {'before_sha256': None, 'after_sha256': 'cf8fb94ec0ef0ef064b9659b3c156f54cab19e63b2ae13bb8c1cb3e0900cfc21'}, 'tests/test_discovery_metrics_flow.py': {'before_sha256': None, 'after_sha256': '3ec2862d91f734b10cb2e3e695e2fdd41b6ffd5ecfbb4af7500c78f05878c0a2'}, 'tests/test_discovery_metrics_release.py': {'before_sha256': None, 'after_sha256': 'b37e5b09039ff0b042da5fbdb69d42a53786112402130250ceb486e46b1f9654'}, 'tests/test_metric_supplement_routing.py': {'before_sha256': None, 'after_sha256': '5c4c09a7e467be537db03fe462e5c1cfb89c629385e872b11ea8518b652368d2'}}

_LOADED_SOURCE = Path(__file__).read_bytes()


def _load(path: Path, name: str, body: bytes | None = None):
    spec = importlib.util.spec_from_file_location(name, path)
    if spec is None or spec.loader is None:
        raise ValueError("discovery_metrics release: verifier unavailable")
    result = importlib.util.module_from_spec(spec)
    if body is None:
        body = path.read_bytes()
    exec(compile(body, str(path), "exec"), result.__dict__)
    return result


_shared = _load(Path(__file__).with_name("account_catalog_capture_release.py"), "discovery_metrics_release_shared")
require, digest, raw = _shared.require, _shared.digest, _shared.raw
reference, object_at, payload_at, records = _shared.reference, _shared.object_at, _shared.payload_at, _shared.records


def source_changes(parent: Mapping[str, Any], current: Mapping[str, Any]) -> dict[str, Any]:
    left, right = records(parent), records(current)
    require(MODULE not in left and MODULE in right
            and right[MODULE]["sha256"] == hashlib.sha256(_LOADED_SOURCE).hexdigest(),
            "discovery_metrics verifier must be new and match the loaded source")
    changes = {}
    for name in sorted(set(left) | set(right)):
        old, new = left.get(name), right.get(name)
        if old == new:
            continue
        require(old is None or new is None or old["mode"] == new["mode"], "source mode changed")
        old_sha, new_sha = (row["sha256"] if row else None for row in (old, new))
        require(old_sha != new_sha, "metadata changed without source change")
        changes[name] = {"before_sha256": old_sha, "after_sha256": new_sha}
    require(bool(REVIEWED_CHANGES) and MODULE not in REVIEWED_CHANGES,
            "PENDING: discovery_metrics delta is not frozen")
    expected = {**REVIEWED_CHANGES,
                MODULE: {"before_sha256": None, "after_sha256": hashlib.sha256(_LOADED_SOURCE).hexdigest()}}
    require(changes == expected, "source delta differs from the reviewed discovery_metrics transition")
    return changes


def parent_context(parent_ref: Mapping[str, Any], *, install_path: Path, database: Path,
                   at: str | None = None) -> tuple[dict[str, Any], dict[str, Any]]:
    require(parent_ref.get("sha256") == PARENT_BUILD_SHA256, "installed parent is not the reviewed build")
    parent = payload_at(parent_ref)
    require(parent.get("status") == "succeeded"
            and parent.get("schema_contract") == {"code_schema": 21, "formal_schema": 21}
            and parent.get("discovery_metrics_successor") is None
            and parent.get("overview_successor", {}).get("contract") == "overview-code-successor-v1"
            and parent["overview_successor"].get("transition") == PARENT_TRANSITION,
            "parent must be the installed schema21 daily metric release")
    source = Path(parent["source_root"])
    # Verify the original parent's complete inventory before executing its
    # unchanged dispatcher, including every dynamically imported dependency.
    tree = object_at(parent["account_cleanup_generation"]["source_tree"])
    require(tree.get("source_root") == str(source) and tree.get("git") == parent.get("git"),
            "parent source manifest binding differs")
    bodies = {}
    for name, row in records(tree).items():
        body = raw(source / name, private=False)
        require(hashlib.sha256(body).hexdigest() == row["sha256"] and len(body) == row["byte_size"]
                and stat.S_IMODE((source / name).stat().st_mode) == row["mode"],
                "published parent source changed: " + name)
        if name in (CLASSIFICATION_MODULE, DAILY_MODULE, OVERVIEW_MODULE):
            require(hashlib.sha256(body).hexdigest() == parent["critical_files"].get(name),
                    "parent verifier critical binding differs")
            bodies[name] = body
    require(set(bodies) == {CLASSIFICATION_MODULE, DAILY_MODULE, OVERVIEW_MODULE}, "parent verifier inventory is incomplete")
    verifier = _load(source / CLASSIFICATION_MODULE, "discovery_metrics_parent_classification", bodies[CLASSIFICATION_MODULE])
    inherited = verifier.verify_inheritance(build=parent, build_ref=parent_ref, install_path=install_path,
                                           database=database, source=source, at=at)
    for name in INHERITED_PROOFS.values():
        proof = inherited.get(name)
        require(isinstance(proof, dict) and proof.get("proof_sha256")
                == digest({key: value for key, value in proof.items() if key != "proof_sha256"}),
                "complete historical proof is required: " + name)
    proof = inherited["overview_proof"]
    require(proof.get("loaded_build") == dict(parent_ref)
            and proof.get("parent_build") == parent["overview_successor"]["parent_build"]
            and proof.get("transition") == PARENT_TRANSITION,
            "published daily metric proof binding differs")
    return parent, inherited


def verify_inheritance(*, build: Mapping[str, Any], build_ref: Mapping[str, Any],
                       install_path: Path, database: Path, source: Path,
                       at: str | None = None) -> dict[str, Any]:
    plan = build.get("discovery_metrics_successor", {})
    require(plan.get("contract") == CONTRACT and plan.get("transition") == TRANSITION, "discovery_metrics successor contract differs")
    require(dict(build) == payload_at(build_ref), "loaded build differs")
    parent, inherited = parent_context(plan["parent_build"], install_path=install_path, database=database, at=at)
    allowed = {"source_root", "git", "critical_files", "code_successor_plan", "account_cleanup_generation",
               "discovery_metrics_successor", "created_at", "validation_scope"}
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
    require(plan.get("changes") == changes, "reviewed discovery_metrics change list differs")
    require(object_at(build["code_successor_plan"]) == {
        "contract": "account-cleanup-source-plan-v1", "transition": "account-cleanup-0907-v1",
        "project_root": build["project_root"], "source_root": str(source), "git": build["git"],
        "source_tree": plan["source_tree"]}, "source plan differs")
    require(build["critical_files"] == {name: row["sha256"] for name, row in records(tree).items()
            if name.startswith(("src/", "config/")) and name.endswith((".py", ".json"))}, "critical source inventory differs")
    require("authorization" not in plan
            and plan.get("schema_migration_repeated") is False and plan.get("provider_qualification_repeated") is False
            and plan.get("database_writes") == 0 and plan.get("paid_gates_reopened") is False
            and plan.get("business_scope_change") == "none"
            and plan.get("transport_qualification") == "not_verified"
            and plan.get("production_rollout") == "approved_by_user", "code-only release scope differs")
    require(isinstance(plan.get("actor"), str) and bool(plan["actor"].strip())
            and isinstance(plan.get("reason"), str) and bool(plan["reason"].strip()), "release owner and reason required")
    for field, name in INHERITED_PROOFS.items():
        require(plan.get(field) == inherited[name]["proof_sha256"], "inherited proof differs: " + name)
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
             **{field: plan[field] for field in INHERITED_PROOFS}}
    proof["proof_sha256"] = digest(proof)
    return {**inherited, "discovery_metrics_proof": proof}
