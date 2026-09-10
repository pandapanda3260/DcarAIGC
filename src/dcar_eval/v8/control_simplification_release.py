"""Code-only controls successor; retain complete metric, catalog and historical proofs.

This stdlib verifier is entered only after bootstrap verifies every source file.
No migration, service action, paid gate, database write or qualification occurs.
PENDING is intentionally unusable until the parent is published and reviewed.
"""
from __future__ import annotations

from datetime import datetime
import hashlib
import importlib.util
from pathlib import Path
from typing import Any, Mapping

CONTRACT = "control-simplification-code-successor-v1"
TRANSITION = "control-simplification-20260910-v1"
CHECK_CONTRACT = "control-simplification-check-v1"
MODULE = "src/dcar_eval/v8/control_simplification_release.py"
CATALOG_MODULE = "src/dcar_eval/v8/account_catalog_capture_release.py"
METRIC_MODULE = "src/dcar_eval/v8/metric_gap_release.py"
CLASSIFICATION_MODULE = "src/dcar_eval/v8/account_classification_release.py"
PARENT_VERIFIER_MODULES = (CLASSIFICATION_MODULE, METRIC_MODULE, CATALOG_MODULE,
                           "src/dcar_eval/v8/manual_content_scope_release.py")
PARENT_BUILD_SHA256 = 'f0f30b20cb3aa38b7ff06f534a65dcf84eb3e3889ae0595249d05028d3fb21d2'
REQUIRED_CHECKS = frozenset({"controls_behavior", "controls_release"})
REVIEWED_CHANGES: dict[str, dict[str, str | None]] = {'scripts/prepare_control_simplification_release.py': {'before_sha256': None, 'after_sha256': '67c58dc59f47188c4bc754f9a6f4823dad55119bfd8e92f3c4d84aee7772e263'}, 'src/dcar_eval/v8/account_classification_release.py': {'before_sha256': 'f575fee2a9d147ae42f2c7cbc74fcb0a11e969e9175e23a515cef862da2dc0ae', 'after_sha256': '5a0060e3ca9f71530d7f9e0731d724d8fbb8c51189229e516a7afb50c22ec31b'}, 'src/dcar_eval/v8/capture.py': {'before_sha256': '097f2193d3485fd8bb1f265e467e368c3095f61561f56ec7414cd43a9df0ec65', 'after_sha256': '4ca9e70fc62cb88f3d9388e0924e827cab7927514bf49d9c9869c337e08b91dd'}, 'src/dcar_eval/v8/capture_authorizations.py': {'before_sha256': '9dbb6b63880e0464156ceef77f81b44b258b779de025ee68073707dd942d6510', 'after_sha256': '3e1e86ac52e670999785c5c957d487761229e71be81eb92e7a0972e0fbefc02d'}, 'src/dcar_eval/v8/capture_compensation.py': {'before_sha256': '4292a7b2cd10771ece16bf5c91694ef7f8dd7a8d73c6d04fa1dbe6d82cdc8797', 'after_sha256': '2d15edbedd8a030bd497744a4bccec101b08724f53c6712c92a32ac32c06a7fe'}, 'src/dcar_eval/v8/capture_release.py': {'before_sha256': 'fe95cfacb4ae5e2943a3c3fa8575b9354259cf7b0f0e622d6ab8a6f9d4a80513', 'after_sha256': '84e7458d6aa43af7dcb120d4abd37b767a719582fafd19d55a618a45fbdd1cca'}, 'src/dcar_eval/v8/capture_runtime.py': {'before_sha256': '621cb6c4b31edffed18932e776712450c2fb26812b95c02b9c55cd0c156fd299', 'after_sha256': '2725ac1498c46a8bcd4d8260ec24551a39bd0b4f7a607d3f0a0d07bc733555ac'}, 'src/dcar_eval/v8/operation_contracts.py': {'before_sha256': None, 'after_sha256': 'd127c505a63119131e5d68084b67b78398465f3a71cc71a67a89672427c9c6e5'}, 'src/dcar_eval/v8/operation_recovery.py': {'before_sha256': None, 'after_sha256': 'e1341496cf97bb7073b0b0e05738f634adc57b8e49c744a6e81f753646d5188a'}, 'src/dcar_eval/v8/pipeline.py': {'before_sha256': '9e454851c4686203c1bd298e48ad90f019e5d3a25ca8d9e2e23d03014a6f062e', 'after_sha256': 'bad54204948b41f941ae27a827135ee14e4465ad6e3075904437caf2a7e52c81'}, 'src/dcar_eval/v8/provider_budget.py': {'before_sha256': '6330c625a841f6945154ca8f31dbb5bb494761dfb1037ef6ccdd1b03926cdb38', 'after_sha256': 'fff7b595b11942a608ef8a5fc76d16cf26c49c75dba4e7a060278bf1f6a8a710'}, 'src/dcar_eval/v8/scheduler.py': {'before_sha256': '9548742425a6e343ec34580d04ad9b8a745542a0568532f6176fc8e7e64e4f5b', 'after_sha256': '448cf14b1041867fb448193f3e43e60d1a1256e081f9cfa9129735010cf744c0'}, 'src/dcar_eval/v8/tikhub_scan.py': {'before_sha256': '7a48424d29bbb195d596b5b7127946d1b336694ec6b5c5fe31e905638bd850f9', 'after_sha256': '790c9a1bd9226d2be56ad0894274f42f9ae31436433c508809f3d8df9433f77d'}, 'src/dcar_eval/v8/work_readiness.py': {'before_sha256': 'cc25cb205edaebba02fdd3362c967d59d5c1449fc03dc95492473ec2f1fa0b41', 'after_sha256': 'b049bfae2fcfb58193ae181eafeaefb24ea2226be1325697ec361b6b61f7c045'}, 'tests/test_control_simplification_release.py': {'before_sha256': None, 'after_sha256': '5832bf05947017349d50b7947ef170d823036a7e6189bda374d5aab8a0688e3e'}, 'tests/test_run_paid_source_refresh_canary.py': {'before_sha256': '3c02825b6afb49a131e2f4217b9a1919a34475aad4364b1455ef24ca5b93fc70', 'after_sha256': 'd6e1527f26adcff834f00f4a26159051b4beae88ff580d1eb99e69f42c1bbdc9'}, 'tests/test_v8_account_cleanup_runtime.py': {'before_sha256': '7453c4b7c26012501be6a50d131a4114affcbf871996daad30890042b2880d90', 'after_sha256': 'f56cddfc6a28161c04ebc2dcefd2ffb80bac76ccdb214a73d4c6a0a4a2915701'}, 'tests/test_v8_capture_activation_release.py': {'before_sha256': 'a65675e097e582b860f38ba8309e793cdbedc98ef9bf19ae62700bc7b1aaf289', 'after_sha256': '7288f7ce4907e6fd8cd62e9acbe2adbabd210c22770f58670972ab1e90e2816a'}, 'tests/test_v8_cleanup_day_readiness.py': {'before_sha256': '4c75542c064ce30833af316d94385ca311d5909e50b2907e40a9d0a1c71fcc9e', 'after_sha256': '8381a4844f7047fe758ccca06903f8536acfa0c9bb9a8eda5b31082218f8142d'}, 'tests/test_v8_diagnostic_budget_boundary.py': {'before_sha256': 'f78ca48317eb4f57c3e56194cbf47bab140f2053bfae594617cc4daef4ff3e86', 'after_sha256': '95a5d1ab055d4d5ddfdf764e6f1dc5c6904902b878bc606cfa4c708d20316194'}, 'tests/test_v8_manual_content_compensation.py': {'before_sha256': '93b9cdecf30ee43bd9a2be2cf56ebc1e4e93627363c9e45ab4fc8421e4374846', 'after_sha256': '1b90e48f2c8b92d0fc1e7ec3c023aa62085a08b6bf7a823c9e0ebe8d4c1a0c50'}, 'tests/test_v8_manual_transport_retry.py': {'before_sha256': 'be3f236764d9ee8724f0d501701b05e56480daed6cee84424b5432207c8e6380', 'after_sha256': 'b1f6390528555d56c113a368acdc9998046abd74e95f6f525bd109a0e9038177'}, 'tests/test_v8_operation_contracts.py': {'before_sha256': None, 'after_sha256': 'f704b26c602cf6fc01081be8abd08238c8110470f74b748b5738f57d26b241d7'}, 'tests/test_v8_operation_recovery.py': {'before_sha256': None, 'after_sha256': '025cb7a3c9860100ce86e01c3575922b73eb8cda90257ea72b6a15f9ecb375fc'}, 'tests/test_v8_provider_budget.py': {'before_sha256': '913df6e2d0fd123b48de96b38079f1d0d183263fd5edf13844a0b4731451690d', 'after_sha256': '9134eb269b22e6b5c478a12ff798ad964461aa48b0945a4ecf5cbb003882fba8'}}
_LOADED_SOURCE = Path(__file__).read_bytes()


def _load(path: Path, name: str, body: bytes | None = None):
    spec = importlib.util.spec_from_file_location(name, path)
    if spec is None or spec.loader is None:
        raise ValueError("control release: verifier unavailable")
    result = importlib.util.module_from_spec(spec)
    if body is None:
        spec.loader.exec_module(result)
    else:
        exec(compile(body, str(path), "exec"), result.__dict__)
    return result


# Share the existing checked-file, JSON, manifest and hash primitives. The
# catalog verifier itself is unchanged by this successor's source delta.
_shared = _load(Path(__file__).with_name("account_catalog_capture_release.py"), "control_release_shared")
require, digest, raw = _shared.require, _shared.digest, _shared.raw
reference, object_at, payload_at, records = _shared.reference, _shared.object_at, _shared.payload_at, _shared.records


def _ready() -> None:
    require(len(PARENT_BUILD_SHA256) == 64 and all(c in "0123456789abcdef" for c in PARENT_BUILD_SHA256),
            "PENDING: published parent hash has not been reviewed")


def source_changes(parent: Mapping[str, Any], current: Mapping[str, Any]) -> dict[str, Any]:
    _ready()
    left, right = records(parent), records(current)
    require(MODULE not in left and MODULE in right
            and right[MODULE]["sha256"] == hashlib.sha256(_LOADED_SOURCE).hexdigest(), "controls module differs")
    changes = {}
    for name in sorted(set(left) | set(right)):
        old, new = left.get(name), right.get(name)
        if old == new:
            continue
        require(old is None or new is None or old["mode"] == new["mode"], "source mode changed")
        old_sha, new_sha = (row["sha256"] if row else None for row in (old, new))
        require(old_sha != new_sha, "metadata changed without source change")
        changes[name] = {"before_sha256": old_sha, "after_sha256": new_sha}
    require(bool(REVIEWED_CHANGES) and MODULE not in REVIEWED_CHANGES, "PENDING: final controls delta is not frozen")
    require(not ({CATALOG_MODULE, METRIC_MODULE} & changes.keys()),
            "published metric and catalog verifiers must remain unchanged")
    expected = {**REVIEWED_CHANGES,
                MODULE: {"before_sha256": None, "after_sha256": hashlib.sha256(_LOADED_SOURCE).hexdigest()}}
    require(changes == expected, "source delta differs from reviewed controls transition")
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
            and "control_simplification_successor" not in parent,
            "parent must be the published schema21 metric generation")
    source = Path(parent["source_root"])
    # The historical dispatcher assumes bootstrap already hashed its dynamic
    # dependencies. Check each verifier it can reach before entering that chain.
    bodies = {}
    for name in PARENT_VERIFIER_MODULES:
        bodies[name] = raw(source / name, private=False)
        require(hashlib.sha256(bodies[name]).hexdigest() == parent["critical_files"].get(name),
                "parent verifier changed: " + name)
    verifier = _load(source / CLASSIFICATION_MODULE, "controls_parent_classification",
                     bodies[CLASSIFICATION_MODULE])
    inherited = verifier.verify_inheritance(build=parent, build_ref=parent_ref, install_path=install_path,
                                           database=database, source=source, at=at)
    require(isinstance(inherited.get("catalog_capture_proof"), dict)
            and inherited.get("catalog_capture_policy") == _shared.ACCOUNT_CATALOG_POLICY
            and inherited.get("catalog_capture_policy_sha256") == digest(_shared.ACCOUNT_CATALOG_POLICY),
            "complete verified catalog inheritance is required")
    metric = inherited.get("metric_gap_proof")
    require(isinstance(metric, dict) and metric.get("loaded_build") == dict(parent_ref)
            and metric.get("parent_build") == parent["metric_gap_successor"]["parent_build"],
            "complete verified metric inheritance is required")
    return parent, inherited


def verify_inheritance(*, build: Mapping[str, Any], build_ref: Mapping[str, Any],
                       install_path: Path, database: Path, source: Path,
                       at: str | None = None) -> dict[str, Any]:
    plan = build.get("control_simplification_successor", {})
    require(plan.get("contract") == CONTRACT and plan.get("transition") == TRANSITION, "controls successor contract differs")
    require(dict(build) == payload_at(build_ref), "loaded build differs")
    parent, inherited = parent_context(plan["parent_build"], install_path=install_path, database=database, at=at)
    allowed = {"source_root", "git", "critical_files", "code_successor_plan", "account_cleanup_generation",
               "control_simplification_successor", "created_at", "validation_scope"}
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
    require(plan.get("changes") == changes, "reviewed controls change list differs")
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
             "inherited_catalog_proof_sha256": plan["inherited_catalog_proof_sha256"]}
    proof["proof_sha256"] = digest(proof)
    return {**inherited, "control_simplification_proof": proof}
