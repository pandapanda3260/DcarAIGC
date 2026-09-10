"""One reviewed schema21 code successor; retains the original capture authority.

The new source and real focused checks are bound separately from the preserved
classification migration, roster, provider qualification and operator controls.
This verifier is stdlib-only and runs after the source bootstrap hashes pass.
"""
from __future__ import annotations

from datetime import datetime
import hashlib
import importlib.util
import json
import os
from pathlib import Path
import stat
from typing import Any, Mapping

CONTRACT = "metric-gap-code-successor-v1"
TRANSITION = "metric-gap-20260910-v1"
CHECK_CONTRACT = "metric-gap-check-v1"
MODULE = "src/dcar_eval/v8/metric_gap_release.py"
PARENT_BUILD_SHA256 = "03cce26b56de1b80b73c41476cf83352d00cc8fcccde4b8c3fe4ec046ff237ce"
REQUIRED_CHECKS = frozenset({"metric_recovery", "metric_commands", "metric_release"})
# Filled only after the final candidate diff is reviewed; own source is bound
# by the complete source manifest without a self-referential hash constant.
REVIEWED_CHANGES: dict[str, dict[str, str | None]] = {'scripts/prepare_metric_gap_release.py': {'before_sha256': None, 'after_sha256': '716c7ea041f5816cacdd4fcaa2b3778382f2c431b6bd09cef2b31fc1840dae35'}, 'src/dcar_eval/v8/account_classification_release.py': {'before_sha256': 'd0e1a97ffc0b21440bf69334d2a18be447f2bbd2cf27bf92f79806a25291d4ae', 'after_sha256': 'f575fee2a9d147ae42f2c7cbc74fcb0a11e969e9175e23a515cef862da2dc0ae'}, 'src/dcar_eval/v8/account_cleanup_runtime.py': {'before_sha256': '85b317e2869988fd26d917379a16b6c5d9fdc6e91f7db13cabaaf68f29b2824f', 'after_sha256': 'ce5443bf91e6feb8c211e388b3982f97bcf506340fe3a0ec4e6f29c5950f1a9d'}, 'src/dcar_eval/v8/api.py': {'before_sha256': 'b0ddc43076794e7657d722cd4d8131a9a58f1c6d628ebdcf507597f15b18ab06', 'after_sha256': '70932c7dace4a6e554b14cdc6acfde7f2947cc50fec0b73d2302c88475dafeb5'}, 'src/dcar_eval/v8/capture_commands.py': {'before_sha256': 'c9e89d288d95ee6fe3602c9bd218bd7f089e1328891851bb6ee675eca80717e9', 'after_sha256': '16b767198ddc9acb12d99b7773a87d341f27b67123914afe1d47a3cf41204ef0'}, 'src/dcar_eval/v8/capture_compensation.py': {'before_sha256': '0121c8fed929e8c8c5011c1c9da7a7a7f1918ebd7f2dc4a6f9ee0da07c0ec1e6', 'after_sha256': '4292a7b2cd10771ece16bf5c91694ef7f8dd7a8d73c6d04fa1dbe6d82cdc8797'}, 'src/dcar_eval/v8/capture_manual.py': {'before_sha256': 'edc071d3636f9ad00bd0dee1279131ae0d24c0b252df9694be92bd7aad898ac1', 'after_sha256': 'a4755f061ade4476f6a4ff717431326fb7b21e7434717d98b90c1e9497afce18'}, 'src/dcar_eval/v8/capture_operator_release.py': {'before_sha256': '0a78eddffa3f54530e507eceddb067cc5c12c3062a2d08ba030b96a0c83db88b', 'after_sha256': '60408bf2cc1b05aea2250ee585b0a6707e681b3cd807c54194990186ce52917a'}, 'src/dcar_eval/v8/capture_runtime.py': {'before_sha256': '9930bb01a29e8fd5df08e52cab24f6f1ff3566ae9a803ca4c9b7d93942713884', 'after_sha256': '621cb6c4b31edffed18932e776712450c2fb26812b95c02b9c55cd0c156fd299'}, 'src/dcar_eval/v8/provider_budget.py': {'before_sha256': 'db8c7d2406a7d14b6e7b365d638397d42b96d08c107ca179d8cd38a319d9e46b', 'after_sha256': '6330c625a841f6945154ca8f31dbb5bb494761dfb1037ef6ccdd1b03926cdb38'}, 'src/dcar_eval/v8/work_readiness.py': {'before_sha256': '24e8fec495bcd51385b25e0e0d6cd2c7771943478e5b5fee89f600bb97e86536', 'after_sha256': 'cc25cb205edaebba02fdd3362c967d59d5c1449fc03dc95492473ec2f1fa0b41'}, 'tests/test_metric_gap_release.py': {'before_sha256': None, 'after_sha256': 'f11bc0773059abc9256ba842b5628808fc3a427adb92aac62b4eece53b13945b'}, 'tests/test_v8_manual_content_commands.py': {'before_sha256': '5687a8da86dc00d6ab1926f30c14a3ece980afba331e66764f5b3005ce5af190', 'after_sha256': 'f08aabbacfe338adff247bd1bea8736a506e96fdefed119889102a6e1d4d0213'}, 'tests/test_v8_manual_content_compensation.py': {'before_sha256': None, 'after_sha256': '93b9cdecf30ee43bd9a2be2cf56ebc1e4e93627363c9e45ab4fc8421e4374846'}, 'tests/test_v8_manual_transport_retry.py': {'before_sha256': None, 'after_sha256': 'be3f236764d9ee8724f0d501701b05e56480daed6cee84424b5432207c8e6380'}}
_LOADED_SOURCE = Path(__file__).read_bytes()


def require(condition: bool, message: str) -> None:
    if not condition:
        raise ValueError("metric gap release: " + message)


def digest(value: Any) -> str:
    return hashlib.sha256(json.dumps(value, ensure_ascii=False, sort_keys=True,
        separators=(",", ":"), allow_nan=False).encode()).hexdigest()


def raw(path: Path, *, private: bool = True) -> bytes:
    before = path.lstat()
    require(path.is_absolute() and path.resolve(strict=True) == path and stat.S_ISREG(before.st_mode)
        and before.st_nlink == 1 and before.st_uid == os.geteuid() and not before.st_mode & 0o022
        and (not private or stat.S_IMODE(before.st_mode) == 0o600)
        and 0 <= before.st_size <= 16 * 1024 * 1024, "unsafe receipt or source")
    with os.fdopen(os.open(path, os.O_RDONLY | os.O_NOFOLLOW), "rb") as stream:
        body, opened = stream.read(16 * 1024 * 1024 + 1), os.fstat(stream.fileno())
    def identity(value):
        return value.st_dev, value.st_ino, value.st_size, value.st_mtime_ns, value.st_ctime_ns
    require(identity(before) == identity(opened) == identity(path.lstat()) and len(body) == before.st_size,
        "receipt or source changed while read")
    return body


def reference(path: Path) -> dict[str, Any]:
    body = raw(path)
    return {"path": str(path), "sha256": hashlib.sha256(body).hexdigest(), "byte_size": len(body)}


def object_at(ref: Mapping[str, Any]) -> dict[str, Any]:
    body = raw(Path(ref["path"]))
    require(hashlib.sha256(body).hexdigest() == ref.get("sha256")
        and ("byte_size" not in ref or len(body) == ref["byte_size"]), "receipt reference differs")
    def unique(pairs):
        value = dict(pairs)
        require(len(value) == len(pairs), "duplicate receipt key")
        return value
    def invalid(value):
        raise ValueError("metric gap release: non-finite receipt value")
    result = json.loads(body, object_pairs_hook=unique, parse_constant=invalid)
    require(isinstance(result, dict), "receipt is not an object")
    return result


def payload_at(ref: Mapping[str, Any], contract: str = "sealed-build-receipt-v1") -> dict[str, Any]:
    envelope = object_at(ref)
    value = envelope.get("payload")
    require(envelope.get("contract_version") == contract and isinstance(value, dict)
        and envelope.get("payload_sha256") == digest(value), "build envelope differs")
    return value


def records(tree: Mapping[str, Any]) -> dict[str, Any]:
    require(tree.get("contract") == "writer-source-tree-v1" and isinstance(tree.get("files"), list),
        "source tree contract differs")
    result = {}
    for item in tree["files"]:
        name = item["path"]
        require(isinstance(name, str) and not name.startswith("/") and name not in result
            and all(part not in {"", ".", "..", ".git"} for part in name.split("/")), "unsafe source member")
        require(set(item) == {"path", "sha256", "byte_size", "mode"}
            and isinstance(item["sha256"], str) and len(item["sha256"]) == 64
            and type(item["byte_size"]) is int and item["byte_size"] >= 0
            and type(item["mode"]) is int and not item["mode"] & 0o022, "invalid source record")
        result[name] = item
    return result


def source_changes(parent: Mapping[str, Any], current: Mapping[str, Any]) -> dict[str, Any]:
    left, right = records(parent), records(current)
    require(MODULE not in left and MODULE in right
        and right[MODULE]["sha256"] == hashlib.sha256(_LOADED_SOURCE).hexdigest(), "successor module differs")
    changes = {}
    for name in sorted(set(left) | set(right)):
        old, new = left.get(name), right.get(name)
        if old == new:
            continue
        require(old is None or new is None or old["mode"] == new["mode"], "source mode changed")
        old_sha, new_sha = (row["sha256"] if row else None for row in (old, new))
        require(old_sha != new_sha, "metadata changed without source change")
        changes[name] = {"before_sha256": old_sha, "after_sha256": new_sha}
    require(bool(REVIEWED_CHANGES) and MODULE not in REVIEWED_CHANGES, "final source review is not frozen")
    expected = {**REVIEWED_CHANGES,
        MODULE: {"before_sha256": None, "after_sha256": hashlib.sha256(_LOADED_SOURCE).hexdigest()}}
    require(changes == expected, "source delta differs from reviewed code-only fix")
    return changes


def parent_context(parent_ref: Mapping[str, Any], *, install_path: Path, database: Path,
                   at: str | None = None) -> tuple[dict[str, Any], dict[str, Any]]:
    require(parent_ref.get("sha256") == PARENT_BUILD_SHA256, "installed parent is not the reviewed build")
    parent = payload_at(parent_ref)
    require(parent.get("schema_contract") == {"code_schema": 21, "formal_schema": 21}
        and parent.get("status") == "succeeded" and parent.get("account_catalog_capture_successor")
        and parent.get("manual_content_scope_successor")
        and not parent.get("metric_gap_successor"),
        "parent must be the reviewed schema21 generation")
    # Only load the exact original verifier from its immutable parent source.
    source = Path(parent["source_root"])
    catalog_path = source / "src/dcar_eval/v8/account_catalog_capture_release.py"
    require(hashlib.sha256(raw(catalog_path, private=False)).hexdigest()
        == parent["critical_files"].get("src/dcar_eval/v8/account_catalog_capture_release.py"),
        "parent catalog verifier changed")
    manual_path = source / "src/dcar_eval/v8/manual_content_scope_release.py"
    require(hashlib.sha256(raw(manual_path, private=False)).hexdigest()
        == parent["critical_files"].get("src/dcar_eval/v8/manual_content_scope_release.py"),
        "parent manual verifier changed")
    module_path = source / "src/dcar_eval/v8/account_classification_release.py"
    body = raw(module_path, private=False)
    require(hashlib.sha256(body).hexdigest() == parent["critical_files"].get("src/dcar_eval/v8/account_classification_release.py"),
        "parent verifier changed")
    spec = importlib.util.spec_from_file_location("metric_gap_parent_classification", module_path)
    require(spec is not None and spec.loader is not None, "parent verifier unavailable")
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    inherited = module.verify_inheritance(build=parent, build_ref=parent_ref, install_path=install_path,
        database=database, source=source, at=at)
    return parent, inherited


def verify_inheritance(*, build: Mapping[str, Any], build_ref: Mapping[str, Any],
                       install_path: Path, database: Path, source: Path,
                       at: str | None = None) -> dict[str, Any]:
    plan = build.get("metric_gap_successor", {})
    require(plan.get("contract") == CONTRACT and plan.get("transition") == TRANSITION,
        "code successor contract differs")
    require(dict(build) == payload_at(build_ref), "loaded build differs")
    parent, inherited = parent_context(plan["parent_build"], install_path=install_path, database=database, at=at)
    allowed = {"source_root", "git", "critical_files", "code_successor_plan", "account_cleanup_generation",
               "metric_gap_successor", "created_at", "validation_scope"}
    require({k: v for k, v in build.items() if k not in allowed}
        == {k: v for k, v in parent.items() if k not in allowed}, "schema or inherited authority changed")
    generation = build["account_cleanup_generation"]
    require({k: v for k, v in generation.items() if k != "source_tree"}
        == {k: v for k, v in parent["account_cleanup_generation"].items() if k != "source_tree"},
        "capture authority changed")
    require(build["source_root"] == str(source) and str(source) != parent["source_root"]
        and generation["source_tree"] == plan["source_tree"], "new source binding differs")
    tree = object_at(plan["source_tree"])
    original = object_at(parent["account_cleanup_generation"]["source_tree"])
    require(tree["source_root"] == str(source) and tree["git"] == build["git"], "source manifest differs")
    changes = source_changes(original, tree)
    require(plan.get("changes") == changes, "reviewed change list differs")
    require(object_at(build["code_successor_plan"]) == {
        "contract": "account-cleanup-source-plan-v1", "transition": "account-cleanup-0907-v1",
        "project_root": build["project_root"], "source_root": str(source), "git": build["git"],
        "source_tree": plan["source_tree"]}, "source plan differs")
    critical = {name: row["sha256"] for name, row in records(tree).items()
        if name.startswith(("src/", "config/")) and name.endswith((".py", ".json"))}
    require(build["critical_files"] == critical, "critical source inventory differs")
    require(plan.get("schema_migration_repeated") is False and plan.get("provider_qualification_repeated") is False
        and plan.get("production_rollout") == "approved_by_user"
        and plan.get("transport_qualification") == "not_verified"
        and isinstance(plan.get("actor"), str) and bool(plan["actor"].strip())
        and isinstance(plan.get("reason"), str) and bool(plan["reason"].strip()), "release scope differs")
    issued = datetime.fromisoformat(plan["issued_at"].replace("Z", "+00:00"))
    parent_issued = datetime.fromisoformat(parent["created_at"].replace("Z", "+00:00"))
    require(issued.tzinfo is not None and parent_issued <= issued and build["created_at"] == plan["issued_at"],
        "invalid code successor time")
    if at is not None:
        require(issued <= datetime.fromisoformat(at.replace("Z", "+00:00")), "future dated code successor")
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
        "checks": checks, "issued_at": plan["issued_at"], "actor": plan["actor"], "reason": plan["reason"]}
    proof["proof_sha256"] = digest(proof)
    # Keep the original catalog, manual and classification proofs and authority bindings valid.
    return {**inherited, "metric_gap_proof": proof}
