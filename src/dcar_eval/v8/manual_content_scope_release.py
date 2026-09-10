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

CONTRACT = "manual-content-scope-code-successor-v1"
TRANSITION = "manual-content-scope-20260909-v1"
CHECK_CONTRACT = "manual-content-scope-check-v1"
MODULE = "src/dcar_eval/v8/manual_content_scope_release.py"
PARENT_BUILD_SHA256 = "bd21ad1bd90222e7cc5c31c728a7d4f38a154bda3c02835983743919b09c6cbd"
REQUIRED_CHECKS = frozenset({"manual_scope", "manual_release"})
# Filled only after the final candidate diff is reviewed; own source is bound
# by the complete source manifest without a self-referential hash constant.
REVIEWED_CHANGES: dict[str, dict[str, str | None]] = {'scripts/prepare_manual_content_scope_release.py': {'before_sha256': None, 'after_sha256': '35a96dfdbfbbf9b168939eb62ccbd330d37f7716eda598fd0d6944d7d984acb5'}, 'src/dcar_eval/v8/account_classification_release.py': {'before_sha256': '7cda367081fd390a7c61666889a4ac392d7fa470cd619e0c5dcb34d08569ee2b', 'after_sha256': '8ce70731996555a79257ffc9bc6ff777c741a2f73593fc0ea624cbe405fdce88'}, 'src/dcar_eval/v8/account_cleanup_runtime.py': {'before_sha256': '6f42486f2b64881501b5db1c131a424284d4982fa73150069c72008005458075', 'after_sha256': '2998321668c5329901cc8b8db54778306a3636f5ea32f349a4609a35f6687610'}, 'src/dcar_eval/v8/api.py': {'before_sha256': '14628d5e20845dfb74a24898d6cd3d8988e3e2c20968970888c5b7b758d9d22b', 'after_sha256': '91b8e136b9760b6502ecaba820d9a05848d35e024ea01f2aace7fee3d436b7d0'}, 'src/dcar_eval/v8/capture_batches.py': {'before_sha256': '2104d792fa157efc0f082d513f652ee5bc074772f0338f43806d61cf1673717a', 'after_sha256': '7c3e121e36cb748ad1dcba3be019ffd5a2b7d2ef404f696eb7b73390c16bd927'}, 'src/dcar_eval/v8/capture_commands.py': {'before_sha256': 'f0fc1340660baa085e182a981ae2c8dabdec442ef30878570c379e8ad8b672d4', 'after_sha256': 'c9e89d288d95ee6fe3602c9bd218bd7f089e1328891851bb6ee675eca80717e9'}, 'src/dcar_eval/v8/capture_manual.py': {'before_sha256': None, 'after_sha256': 'edc071d3636f9ad00bd0dee1279131ae0d24c0b252df9694be92bd7aad898ac1'}, 'src/dcar_eval/v8/capture_planning.py': {'before_sha256': '5686038dc5063da561834f307f0f3cff7d4a29c6c6a9d4ccadaacd7e824ef853', 'after_sha256': '03e6e81e049412089ff1fc04b09691bb38f7d0223ee641b7bfd7ab975d7c59ab'}, 'src/dcar_eval/v8/capture_runtime.py': {'before_sha256': '615469f748e8f81842c9f76f39534b86e048f84c434eda02b4cd126255fcd4e2', 'after_sha256': '6a3f084430217108e998d01d10ae6799e542d3fbb071aa10a3e5a2c99dcb5675'}, 'src/dcar_eval/v8/provider_budget.py': {'before_sha256': 'c481366d4fd19c1e402ff10d1d26823ef203207d85ca9ee587221f258b6155cc', 'after_sha256': 'd496237d14f72406b20c3586506f96d6076db6e4f8d3872537ab15a261dae26d'}, 'src/dcar_eval/v8/provider_updates.py': {'before_sha256': '5f388a78b61887f6b3e081a6bb37a8a84363b42b8b3e776db790f564d48aa3d8', 'after_sha256': '56cc07c186d987ce591b3b7066ec5d2fcb250fc160735c166f919fd5ebe3b373'}, 'src/dcar_eval/v8/providers.py': {'before_sha256': '7ea7f2bbc10eb81ca19a6c26cef7c77e763a8e30f22c0b21632ab0c5181cb66d', 'after_sha256': '70aba5a58f26b10d1702027e57608ad62a245f92001a595fc06e81a6f3406435'}, 'tests/test_manual_content_scope_release.py': {'before_sha256': None, 'after_sha256': '60a57838dd5d170579ad6e32ef733738de91e59e53acd3a889db8f8529ce579a'}, 'tests/test_v8_manual_content_commands.py': {'before_sha256': None, 'after_sha256': 'fd95b11cb028f424dceac19094e84858da288257ad6636ef2acffdb84afa62b8'}, 'tests/test_v8_manual_content_scope.py': {'before_sha256': None, 'after_sha256': '4ba5fec0b2bb14235ce09fa697c70fdb41c002d8703b59f46eea8323f3c14e98'}}
_LOADED_SOURCE = Path(__file__).read_bytes()


def require(condition: bool, message: str) -> None:
    if not condition:
        raise ValueError("manual content release: " + message)


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
        raise ValueError("manual content release: non-finite receipt value")
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
        and parent.get("status") == "succeeded" and not parent.get("manual_content_scope_successor"),
        "parent must be the reviewed schema21 generation")
    # Only load the exact original verifier from its immutable parent source.
    source = Path(parent["source_root"])
    module_path = source / "src/dcar_eval/v8/account_classification_release.py"
    body = raw(module_path, private=False)
    require(hashlib.sha256(body).hexdigest() == parent["critical_files"].get("src/dcar_eval/v8/account_classification_release.py"),
        "parent verifier changed")
    spec = importlib.util.spec_from_file_location("manual_release_parent_classification", module_path)
    require(spec is not None and spec.loader is not None, "parent verifier unavailable")
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    inherited = module.verify_inheritance(build=parent, build_ref=parent_ref, install_path=install_path,
        database=database, source=source, at=at)
    return parent, inherited


def verify_inheritance(*, build: Mapping[str, Any], build_ref: Mapping[str, Any],
                       install_path: Path, database: Path, source: Path,
                       at: str | None = None) -> dict[str, Any]:
    plan = build.get("manual_content_scope_successor", {})
    require(plan.get("contract") == CONTRACT and plan.get("transition") == TRANSITION,
        "code successor contract differs")
    require(dict(build) == payload_at(build_ref), "loaded build differs")
    parent, inherited = parent_context(plan["parent_build"], install_path=install_path, database=database, at=at)
    allowed = {"source_root", "git", "critical_files", "code_successor_plan", "account_cleanup_generation",
               "manual_content_scope_successor", "created_at", "validation_scope"}
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
    # Keep the original classification proof and authority bindings valid.
    return {**inherited, "manual_content_scope_proof": proof}
