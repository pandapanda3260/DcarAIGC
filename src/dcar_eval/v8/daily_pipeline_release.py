"""Code-only daily pipeline successor retaining all published proofs and consumed authorizations.

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

CONTRACT = "daily-pipeline-code-successor-v1"
TRANSITION = "daily-metric-validity-20260911-v1"
PARENT_TRANSITION = "daily-pipeline-20260911-v1"
CHECK_CONTRACT = "daily-pipeline-check-v1"
MODULE = "src/dcar_eval/v8/daily_pipeline_release.py"
CATALOG_MODULE = "src/dcar_eval/v8/account_catalog_capture_release.py"
METRIC_MODULE = "src/dcar_eval/v8/metric_gap_release.py"
CLASSIFICATION_MODULE = "src/dcar_eval/v8/account_classification_release.py"
CONTROLS_MODULE = "src/dcar_eval/v8/control_simplification_release.py"
PUBLISHER_MODULE = "src/dcar_eval/v8/publisher_snapshot_release.py"
RECOVERY_MODULE = "src/dcar_eval/v8/account_profile_recovery_release.py"
PROFILE_MODULE = "src/dcar_eval/v8/account_profile_release.py"
CAPACITY_MODULE = "src/dcar_eval/v8/publisher_capacity_release.py"
PARENT_VERIFIER_MODULES = (RECOVERY_MODULE, PROFILE_MODULE, CAPACITY_MODULE, CLASSIFICATION_MODULE,
                           PUBLISHER_MODULE, CONTROLS_MODULE, METRIC_MODULE, CATALOG_MODULE,
                           "src/dcar_eval/v8/manual_content_scope_release.py")
PARENT_SUCCESSORS = ("account_profile_recovery_successor", "account_profile_successor",
                     "publisher_capacity_successor", "publisher_snapshot_successor",
                     "control_simplification_successor", "metric_gap_successor",
                     "account_catalog_capture_successor", "manual_content_scope_successor",
                     "account_classification_successor")
# Retain the deployed adapter's eight field names; also bind the older proofs.
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
}
PARENT_BUILD_SHA256 = "f5851e8c94031f7d5bd56784840b45fb57eb83df344a1203c5cee9b3eeb92005"
PREVIOUS_BUILD_SHA256 = "7f83f8b695f6f7f4e7eb474ec04c4667718c0f3e5c46f9364459292e2484b9cb"
PARENT_DAILY_MODULE_SHA256 = "e83252355a59480b0b9d726a6a9c6698dbc23e15edcb346e81b8bc324edc1ce9"
RECOVERY_BUILD_SHA256 = "e994617379b578b0a68a6d0e3c15bdae58ffe9d2006be553d6b6d6ccf0ae67c3"
REQUIRED_CHECKS = frozenset({"daily_metric_validity_behavior", "daily_metric_validity_release"})
# Freeze only the final reviewed daily-report delta; an empty set rejects preparation.
REVIEWED_CHANGES: dict[str, dict[str, str | None]] = {'config/source_routing_operation_field_v3.json': {'before_sha256': None, 'after_sha256': '3f0c059a01708d67dce070d893016ed49c53b5e9f25b216938ec8a0752460837'}, 'scripts/deploy_daily_metric_validity_release.py': {'before_sha256': None, 'after_sha256': '4ccb1c9d4197ffcea4ffac8a019a6aa61324b436ac3c94f486f9f66a66d45cfb'}, 'scripts/prepare_daily_metric_validity_release.py': {'before_sha256': None, 'after_sha256': 'c30e232a16522d87e556b6d84eb284bc2efe8ecb3fabab7ddf126002975b75fe'}, 'src/dcar_eval/v8/metric_field_facts.py': {'before_sha256': '0e8381e39c077e53bbaf2030cc25df0c0f5aa3384cc0d7fe646397ec341dcfb2', 'after_sha256': '6f9793a852d45bcd134603bbfc4e323bedceec6aa36df8867d268eef948e457c'}, 'src/dcar_eval/v8/metric_source_policy.py': {'before_sha256': None, 'after_sha256': '02e7b23a77c1de2f5f7ac078e381e1b9b932757ed5f991a2ae96c2e1bfb31c49'}, 'src/dcar_eval/v8/report_inputs.py': {'before_sha256': 'b919d81487a794ed953185d92d2655b6951c5a8c16750c84a728654cdce70b9b', 'after_sha256': 'd55c0834939aa9b28977905b4a1962d7649d0280a736ad042ccbfc4ab9131484'}, 'src/dcar_eval/v8/report_metric_validity.py': {'before_sha256': None, 'after_sha256': '6da1db512240489d249f002bd321e80ab953424f4e1218f2eb5f1f189d16f1a1'}, 'src/dcar_eval/v8/reports.py': {'before_sha256': '7f98be9047d9b9bf06ce5b8772861dbd503d214e1693b49ccc5d5e5ea7d8ae54', 'after_sha256': '5ce93f5e159ad6248617805c53d573fc97f622f1a2b35ee4622320d680be5e39'}, 'src/dcar_eval/v8/source_routing.py': {'before_sha256': '3f7c516151b28cf4b6ed49372285100550a70fb5bd5e1c804d1622455182ae85', 'after_sha256': '6658fc01a01b5094e3af8d338b0d159b951109141372036cb0cb18de718e7162'}, 'tests/test_daily_pipeline_release.py': {'before_sha256': '749537cc0b21b265c2196fad50ee730af6fe3483eac33f6125b6b5dae0efdfbb', 'after_sha256': '781d249693be3ff66c499a75a3c343c89fb3bbc046780b0d3b367341300c5d4d'}, 'tests/test_deploy_daily_metric_validity_release.py': {'before_sha256': None, 'after_sha256': '6de9c11af5961a7648b477aa90ce36eb7b72fb59290b1bf7418747b6ad09dc38'}, 'tests/test_report_metric_validity.py': {'before_sha256': None, 'after_sha256': '4d9598d6cee7b2730a8dadb65dbfa2c84212d15c482c777108d707917a95f3e6'}, 'tests/test_v21_operation_field_policy.py': {'before_sha256': None, 'after_sha256': '3edc8a49eda5212a8394db87a55b857ba62e75dcf1595b654d2545df7544afc7'}}
_LOADED_SOURCE = Path(__file__).read_bytes()


def _load(path: Path, name: str, body: bytes | None = None):
    spec = importlib.util.spec_from_file_location(name, path)
    if spec is None or spec.loader is None:
        raise ValueError("daily pipeline release: verifier unavailable")
    result = importlib.util.module_from_spec(spec)
    if body is None:
        spec.loader.exec_module(result)
    else:
        exec(compile(body, str(path), "exec"), result.__dict__)
    return result


# Share the existing checked-file, JSON, manifest and hash primitives. The
# catalog verifier itself is unchanged by this successor's source delta.
_shared = _load(Path(__file__).with_name("account_catalog_capture_release.py"), "daily_pipeline_release_shared")
require, digest, raw = _shared.require, _shared.digest, _shared.raw
reference, object_at, payload_at, records = _shared.reference, _shared.object_at, _shared.payload_at, _shared.records


def _ready() -> None:
    require(len(PARENT_BUILD_SHA256) == 64 and all(c in "0123456789abcdef" for c in PARENT_BUILD_SHA256),
            "PENDING: published parent hash has not been reviewed")


def source_changes(parent: Mapping[str, Any], current: Mapping[str, Any]) -> dict[str, Any]:
    _ready()
    left, right = records(parent), records(current)
    require(MODULE in left and MODULE in right
            and left[MODULE]["sha256"] == PARENT_DAILY_MODULE_SHA256
            and right[MODULE]["sha256"] == hashlib.sha256(_LOADED_SOURCE).hexdigest(), "daily pipeline module differs")
    changes = {}
    for name in sorted(set(left) | set(right)):
        old, new = left.get(name), right.get(name)
        if old == new:
            continue
        require(old is None or new is None or old["mode"] == new["mode"], "source mode changed")
        old_sha, new_sha = (row["sha256"] if row else None for row in (old, new))
        require(old_sha != new_sha, "metadata changed without source change")
        changes[name] = {"before_sha256": old_sha, "after_sha256": new_sha}
    require(bool(REVIEWED_CHANGES) and MODULE not in REVIEWED_CHANGES, "PENDING: final daily pipeline delta is not frozen")
    require(not (set(PARENT_VERIFIER_MODULES) & changes.keys()),
            "published inheritance verifiers must remain unchanged")
    require("src/dcar_eval/v8/account_profile_authority.py" not in changes,
            "published profile authority must remain unchanged")
    expected = {**REVIEWED_CHANGES,
                MODULE: {"before_sha256": PARENT_DAILY_MODULE_SHA256, "after_sha256": hashlib.sha256(_LOADED_SOURCE).hexdigest()}}
    require(changes == expected, "source delta differs from reviewed daily pipeline transition")
    return changes


def parent_context(parent_ref: Mapping[str, Any], *, install_path: Path, database: Path,
                   at: str | None = None) -> tuple[dict[str, Any], dict[str, Any]]:
    _ready()
    require(parent_ref.get("sha256") == PARENT_BUILD_SHA256, "installed parent is not the reviewed build")
    parent = payload_at(parent_ref)
    require(parent.get("status") == "succeeded"
            and parent.get("schema_contract") == {"code_schema": 21, "formal_schema": 21}
            and all(isinstance(parent.get(key), dict) for key in PARENT_SUCCESSORS)
            and isinstance(parent.get("daily_pipeline_successor"), dict),
            "parent must be the published schema21 daily pipeline generation")
    previous_plan = parent["daily_pipeline_successor"]
    require(previous_plan.get("contract") == CONTRACT and previous_plan.get("transition") == PARENT_TRANSITION
            and previous_plan.get("parent_build", {}).get("sha256") == PREVIOUS_BUILD_SHA256
            and isinstance(previous_plan.get("inherited_previous_daily_pipeline_proof_sha256"), str)
            and "inherited_published_daily_pipeline_proof_sha256" not in previous_plan
            and parent["critical_files"].get(MODULE) == PARENT_DAILY_MODULE_SHA256,
            "published daily parent generation or verifier binding differs")
    source = Path(parent["source_root"])
    # The historical dispatcher assumes bootstrap already hashed its dynamic
    # dependencies. Check each verifier it can reach before entering that chain.
    bodies = {}
    for name in (*PARENT_VERIFIER_MODULES, MODULE):
        bodies[name] = raw(source / name, private=False)
        require(hashlib.sha256(bodies[name]).hexdigest() == parent["critical_files"].get(name),
                "parent verifier changed: " + name)
    verifier = _load(source / CLASSIFICATION_MODULE, "daily_pipeline_parent_classification",
                     bodies[CLASSIFICATION_MODULE])
    inherited = verifier.verify_inheritance(build=parent, build_ref=parent_ref, install_path=install_path,
                                           database=database, source=source, at=at)
    previous_proof = inherited.get("daily_pipeline_proof")
    require(isinstance(previous_proof, dict)
            and previous_proof.get("contract") == CONTRACT and previous_proof.get("transition") == PARENT_TRANSITION
            and previous_proof.get("parent_build") == previous_plan["parent_build"]
            and isinstance(inherited.get("previous_daily_pipeline_proof"), dict)
            and "published_daily_pipeline_proof" not in inherited,
            "complete previous daily pipeline proof is required")
    inherited = {key: value for key, value in inherited.items() if key != "daily_pipeline_proof"}
    inherited["published_daily_pipeline_proof"] = previous_proof
    older_proof = inherited["previous_daily_pipeline_proof"]
    require(older_proof.get("contract") == CONTRACT
            and older_proof.get("transition") == PARENT_TRANSITION
            and older_proof.get("parent_build", {}).get("sha256") == RECOVERY_BUILD_SHA256
            and older_proof.get("proof_sha256") == previous_plan["inherited_previous_daily_pipeline_proof_sha256"],
            "older daily pipeline proof binding differs")
    require(isinstance(inherited.get("catalog_capture_proof"), dict)
            and inherited.get("catalog_capture_policy") == _shared.ACCOUNT_CATALOG_POLICY
            and inherited.get("catalog_capture_policy_sha256") == digest(_shared.ACCOUNT_CATALOG_POLICY),
            "complete verified catalog inheritance is required")
    expected_loaded = {
        "account_profile_recovery_proof": older_proof["parent_build"],
        "profile_compensation_authority": older_proof["parent_build"],
        "previous_daily_pipeline_proof": previous_plan["parent_build"],
        "published_daily_pipeline_proof": dict(parent_ref),
        "account_profile_proof": parent["account_profile_recovery_successor"]["parent_build"],
        "profile_operation_authority": parent["account_profile_recovery_successor"]["parent_build"],
        "publisher_capacity_proof": parent["account_profile_successor"]["parent_build"],
        "publisher_snapshot_proof": parent["publisher_capacity_successor"]["parent_build"],
        "control_simplification_proof": parent["publisher_snapshot_successor"]["parent_build"],
        "metric_gap_proof": parent["control_simplification_successor"]["parent_build"],
        "catalog_capture_proof": parent["metric_gap_successor"]["parent_build"],
        "manual_content_scope_proof": parent["account_catalog_capture_successor"]["parent_build"],
        "proof": parent["manual_content_scope_successor"]["parent_build"],
    }
    for name, loaded in expected_loaded.items():
        proof = inherited.get(name)
        require(isinstance(proof, dict) and proof.get("loaded_build") == loaded
                and proof.get("proof_sha256") == digest({k: v for k, v in proof.items() if k != "proof_sha256"}),
                "complete historical proof binding differs: " + name)
    require(inherited["profile_operation_authority"].get("authorization")
            == parent["account_profile_successor"]["authorization"]
            and inherited["profile_compensation_authority"].get("authorization")
            == parent["account_profile_recovery_successor"]["authorization"]
            and inherited["account_profile_recovery_proof"].get("authorization")
            == parent["account_profile_recovery_successor"]["authorization"]
            and inherited["profile_compensation_authority"].get("profile_authority_proof_sha256")
            == inherited["profile_operation_authority"]["proof_sha256"],
            "published profile authorizations changed")
    require(inherited.get("parent_build_ref") == parent["account_classification_successor"]["parent_build"]
            and isinstance(inherited.get("parent_build"), dict), "original cleanup parent binding differs")
    return parent, inherited


def verify_inheritance(*, build: Mapping[str, Any], build_ref: Mapping[str, Any],
                       install_path: Path, database: Path, source: Path,
                       at: str | None = None) -> dict[str, Any]:
    plan = build.get("daily_pipeline_successor", {})
    require(plan.get("contract") == CONTRACT and plan.get("transition") == TRANSITION, "daily pipeline successor contract differs")
    require(dict(build) == payload_at(build_ref), "loaded build differs")
    parent, inherited = parent_context(plan["parent_build"], install_path=install_path, database=database, at=at)
    allowed = {"source_root", "git", "critical_files", "code_successor_plan", "account_cleanup_generation",
               "daily_pipeline_successor", "created_at", "validation_scope"}
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
    require(plan.get("changes") == changes, "reviewed daily pipeline change list differs")
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
    return {**inherited, "daily_pipeline_proof": proof}
