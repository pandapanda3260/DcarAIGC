"""Exact overview-only successor; inherit the installed daily release unchanged.

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

CONTRACT = "overview-code-successor-v1"
TRANSITION = "overview-four-platforms-20260912-v1"
CHECK_CONTRACT = "overview-release-check-v1"
MODULE = "src/dcar_eval/v8/overview_release.py"
PARENT_BUILD_SHA256 = "e5d53ba78459301648fbe080aa26beff810b135edd2f91021ea9bbc1f51931e8"
PARENT_TRANSITION = "daily-metric-validity-20260911-v1"
CLASSIFICATION_MODULE = "src/dcar_eval/v8/account_classification_release.py"
DAILY_MODULE = "src/dcar_eval/v8/daily_pipeline_release.py"
REQUIRED_CHECKS = frozenset({"overview_behavior", "overview_release"})
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
}
REVIEWED_CHANGES: dict[str, dict[str, str | None]] = {'scripts/deploy_overview_release.py': {'before_sha256': None, 'after_sha256': 'cee0ac8ee37bb9aeb1c3c38cf649662e3029492399fc5d9e8e42ece23a90215b'}, 'scripts/prepare_overview_release.py': {'before_sha256': None, 'after_sha256': '86d05ec297603e0ebc4d73479a013fced4b07cfebbea9f59644db78e5d72eaa5'}, 'src/dcar_eval/v8/account_classification_release.py': {'before_sha256': '068d55f272b24e2b5c6751b1072b5c32bfe7ec6dc8f936c6bcf9d0f1ee6e18cb', 'after_sha256': '395c4890b59f285618276703b08ecad2c793470461cc0c4462790ac001c01425'}, 'src/dcar_eval/v8/api.py': {'before_sha256': '70932c7dace4a6e554b14cdc6acfde7f2947cc50fec0b73d2302c88475dafeb5', 'after_sha256': '8588c8b479c4333a99769216da609a285098501e48139b6f7006d7c471eb195b'}, 'src/dcar_eval/v8/insights.py': {'before_sha256': '49480cb2a73f854f21bf0fefd1df4f465120f60690041055199a6f2068e971a9', 'after_sha256': '83a9988c7f6f6d93ba786f4704d42c7f2e17e66950c86e3e42912e7de1c3014b'}, 'tests/test_deploy_overview_release.py': {'before_sha256': None, 'after_sha256': 'b5367d166a51499c8c51032ec2904dffa90fd28b69a0193e30abe56e124f421d'}, 'tests/test_overview_release.py': {'before_sha256': None, 'after_sha256': '34b4f57f7576f5f4f2249b1f496f88237752acd64d0f8998535b3b4410f3d68c'}, 'tests/test_v8_api.py': {'before_sha256': '7dc456967db89afbecc8160fdade56c994e298deb2bde45c17f047e9f893ed76', 'after_sha256': '348e7ca02d70b9144fc7c3963040517c83777994134df78e5f5f9f8e74e34ddc'}, 'tests/test_v8_overview_selling_points.py': {'before_sha256': 'bf9d2b8758b3eb2c90769ec6591c4b95f8767db18bdc5bb88b982a1cd6c8cb0e', 'after_sha256': '72ebe0eca1865d71faaef8ba72efc40cd613064c353968d0783fa5e11c6240e8'}}
_LOADED_SOURCE = Path(__file__).read_bytes()


def _load(path: Path, name: str, body: bytes | None = None):
    spec = importlib.util.spec_from_file_location(name, path)
    if spec is None or spec.loader is None:
        raise ValueError("overview release: verifier unavailable")
    result = importlib.util.module_from_spec(spec)
    if body is None:
        body = path.read_bytes()
    exec(compile(body, str(path), "exec"), result.__dict__)
    return result


_shared = _load(Path(__file__).with_name("account_catalog_capture_release.py"), "overview_release_shared")
require, digest, raw = _shared.require, _shared.digest, _shared.raw
reference, object_at, payload_at, records = _shared.reference, _shared.object_at, _shared.payload_at, _shared.records


def source_changes(parent: Mapping[str, Any], current: Mapping[str, Any]) -> dict[str, Any]:
    left, right = records(parent), records(current)
    require(MODULE not in left and MODULE in right
            and right[MODULE]["sha256"] == hashlib.sha256(_LOADED_SOURCE).hexdigest(),
            "overview verifier must be new and match the loaded source")
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
            "PENDING: overview delta is not frozen")
    expected = {**REVIEWED_CHANGES,
                MODULE: {"before_sha256": None, "after_sha256": hashlib.sha256(_LOADED_SOURCE).hexdigest()}}
    require(changes == expected, "source delta differs from the reviewed overview transition")
    return changes


def parent_context(parent_ref: Mapping[str, Any], *, install_path: Path, database: Path,
                   at: str | None = None) -> tuple[dict[str, Any], dict[str, Any]]:
    require(parent_ref.get("sha256") == PARENT_BUILD_SHA256, "installed parent is not the reviewed build")
    parent = payload_at(parent_ref)
    require(parent.get("status") == "succeeded"
            and parent.get("schema_contract") == {"code_schema": 21, "formal_schema": 21}
            and parent.get("overview_successor") is None
            and parent.get("daily_pipeline_successor", {}).get("contract") == "daily-pipeline-code-successor-v1"
            and parent["daily_pipeline_successor"].get("transition") == PARENT_TRANSITION,
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
        if name in (CLASSIFICATION_MODULE, DAILY_MODULE):
            require(hashlib.sha256(body).hexdigest() == parent["critical_files"].get(name),
                    "parent verifier critical binding differs")
            bodies[name] = body
    require(set(bodies) == {CLASSIFICATION_MODULE, DAILY_MODULE}, "parent verifier inventory is incomplete")
    verifier = _load(source / CLASSIFICATION_MODULE, "overview_parent_classification", bodies[CLASSIFICATION_MODULE])
    inherited = verifier.verify_inheritance(build=parent, build_ref=parent_ref, install_path=install_path,
                                           database=database, source=source, at=at)
    for name in INHERITED_PROOFS.values():
        proof = inherited.get(name)
        require(isinstance(proof, dict) and proof.get("proof_sha256")
                == digest({key: value for key, value in proof.items() if key != "proof_sha256"}),
                "complete historical proof is required: " + name)
    proof = inherited["daily_pipeline_proof"]
    require(proof.get("loaded_build") == dict(parent_ref)
            and proof.get("parent_build") == parent["daily_pipeline_successor"]["parent_build"]
            and proof.get("transition") == PARENT_TRANSITION,
            "published daily metric proof binding differs")
    return parent, inherited


def verify_inheritance(*, build: Mapping[str, Any], build_ref: Mapping[str, Any],
                       install_path: Path, database: Path, source: Path,
                       at: str | None = None) -> dict[str, Any]:
    plan = build.get("overview_successor", {})
    require(plan.get("contract") == CONTRACT and plan.get("transition") == TRANSITION, "overview successor contract differs")
    require(dict(build) == payload_at(build_ref), "loaded build differs")
    parent, inherited = parent_context(plan["parent_build"], install_path=install_path, database=database, at=at)
    allowed = {"source_root", "git", "critical_files", "code_successor_plan", "account_cleanup_generation",
               "overview_successor", "created_at", "validation_scope"}
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
    require(plan.get("changes") == changes, "reviewed overview change list differs")
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
    return {**inherited, "overview_proof": proof}
