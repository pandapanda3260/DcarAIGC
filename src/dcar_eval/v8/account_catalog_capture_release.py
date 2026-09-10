"""Reviewed account-catalog policy successor with inherited execution controls.

The user approved deriving automatic membership from managed account eligibility.
This is a new business-scope policy, not a claim that the legacy roster authorized
new members. Original migration, provider qualification, budgets and operator
controls remain immutable. The stdlib verifier runs after all source hashes pass.
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

CONTRACT = "account-catalog-capture-policy-successor-v1"
TRANSITION = "account-catalog-capture-20260910-v1"
CHECK_CONTRACT = "account-catalog-capture-check-v1"
MODULE = "src/dcar_eval/v8/account_catalog_capture_release.py"
PARENT_BUILD_SHA256 = "d07a1984d6a8f306cd4da838970afe2daa93043a3612e3ed251006f403882f98"
REQUIRED_CHECKS = frozenset({"catalog_scope", "catalog_release"})
# Filled only after the final candidate diff is reviewed; own source is bound
# by the complete source manifest without a self-referential hash constant.
REVIEWED_CHANGES: dict[str, dict[str, str | None]] = {'deploy/macos/publish_snapshot.py': {'before_sha256': 'f80a0bb7d5fc88cba7b6bc2e9809c1a6ac3d3de96f2065dfe97ca6c75132a923', 'after_sha256': '7e1d5ce593fe2c8cf0c79664291e69e65dcf72dfdd7a5ae4d2286d9af9650da2'}, 'scripts/prepare_account_catalog_capture_release.py': {'before_sha256': None, 'after_sha256': '514129e03c41e93e49b6b51ba44f7066d4bf44641011bee0beb1ca50d3769032'}, 'src/dcar_eval/v8/account_capture_eligibility.py': {'before_sha256': None, 'after_sha256': '6dddf135030730bf05826ac07f6edec837ec91c8920d7aa69ab28b01606b045e'}, 'src/dcar_eval/v8/account_catalog_capture.py': {'before_sha256': None, 'after_sha256': 'cc54afe42e063d683b0363350dc5008ebf49f289514be1a6ed7d80874de05408'}, 'src/dcar_eval/v8/account_classification_release.py': {'before_sha256': '8ce70731996555a79257ffc9bc6ff777c741a2f73593fc0ea624cbe405fdce88', 'after_sha256': 'd0e1a97ffc0b21440bf69334d2a18be447f2bbd2cf27bf92f79806a25291d4ae'}, 'src/dcar_eval/v8/account_cleanup_runtime.py': {'before_sha256': '2998321668c5329901cc8b8db54778306a3636f5ea32f349a4609a35f6687610', 'after_sha256': '85b317e2869988fd26d917379a16b6c5d9fdc6e91f7db13cabaaf68f29b2824f'}, 'src/dcar_eval/v8/account_creation.py': {'before_sha256': '2012524182790e8e47a9b0115ecdf9a21de931eb4555bbbe4158f75da768e312', 'after_sha256': '1144bfcd934052a195517a73795b8dcee152dccec9c569e8b859dfee2cf61163'}, 'src/dcar_eval/v8/account_directory.py': {'before_sha256': '8e0c12ddf4c82aadb6bb184343d6d843f39478d1b5713ddbb258684b6b39479b', 'after_sha256': 'd5e984160463ac7ca73db5c47456625e4bfb28e2f8ee15c6e6d4cc5d16f8a389'}, 'src/dcar_eval/v8/account_directory_status.py': {'before_sha256': '90f07ab7d556a5dad2c7af4aee46e5a6cf5cfd9b5e1b70cdd461a648bc16cb72', 'after_sha256': '7b40a7efde9e652d7255376aff59e8f50e20e5897a804517745d4274080be3a3'}, 'src/dcar_eval/v8/account_operating_status.py': {'before_sha256': 'ca5e8ee2c60541973ff85e46ad50d8f5915736224a3b7ade9f6be3030665f515', 'after_sha256': '61be52696a0756f2fb4b850bfbddc96d377376e7c53af66ae0ffce6bda2bbe5a'}, 'src/dcar_eval/v8/api.py': {'before_sha256': '91b8e136b9760b6502ecaba820d9a05848d35e024ea01f2aace7fee3d436b7d0', 'after_sha256': 'b0ddc43076794e7657d722cd4d8131a9a58f1c6d628ebdcf507597f15b18ab06'}, 'src/dcar_eval/v8/capture_batches.py': {'before_sha256': '7c3e121e36cb748ad1dcba3be019ffd5a2b7d2ef404f696eb7b73390c16bd927', 'after_sha256': '4c47f2b91bf15fb0030b8784b8feda025d04b17aecea4aecbc96f4d5e5b62d94'}, 'src/dcar_eval/v8/capture_day_coverage.py': {'before_sha256': None, 'after_sha256': '8fdd2687a02116c5c2945e7c40ef60b0a07a4fde0b51e9fce547fd9bc21efb2f'}, 'src/dcar_eval/v8/capture_metric_cycles.py': {'before_sha256': None, 'after_sha256': 'b16e4d2315e35924ee4f2c972255e9dc2a2cf7ab9ef3d38bcf1eb792cdd7fbfa'}, 'src/dcar_eval/v8/capture_planning.py': {'before_sha256': '03e6e81e049412089ff1fc04b09691bb38f7d0223ee641b7bfd7ab975d7c59ab', 'after_sha256': 'ec8c526504dddb63d5e3a6609807548f9c37ebec2a12f6e1cc4364cd05552687'}, 'src/dcar_eval/v8/capture_runtime.py': {'before_sha256': '6a3f084430217108e998d01d10ae6799e542d3fbb071aa10a3e5a2c99dcb5675', 'after_sha256': '9930bb01a29e8fd5df08e52cab24f6f1ff3566ae9a803ca4c9b7d93942713884'}, 'src/dcar_eval/v8/pipeline.py': {'before_sha256': '00d04fed1e41a4a8737abff239582357d01efad2e1d5d365dbf97aac6e45a861', 'after_sha256': '9e454851c4686203c1bd298e48ad90f019e5d3a25ca8d9e2e23d03014a6f062e'}, 'src/dcar_eval/v8/provider_budget.py': {'before_sha256': 'd496237d14f72406b20c3586506f96d6076db6e4f8d3872537ab15a261dae26d', 'after_sha256': 'db8c7d2406a7d14b6e7b365d638397d42b96d08c107ca179d8cd38a319d9e46b'}, 'src/dcar_eval/v8/provider_updates.py': {'before_sha256': '56cc07c186d987ce591b3b7066ec5d2fcb250fc160735c166f919fd5ebe3b373', 'after_sha256': '660c91f1a99ead65f3a3156883fecc172f9f3bec4703368e42ad37d8ae20c043'}, 'src/dcar_eval/v8/report_export.py': {'before_sha256': '75b07bec17af69138da8335e29a8767c6a330a0a2944172600f4fb9d5fd02564', 'after_sha256': 'c646665deb2dd1e593f1703501aabe895416652786652d6802d050b94291674e'}, 'src/dcar_eval/v8/reports.py': {'before_sha256': '8fa250bc1aa79f32444a5f9cd3e43228464e92e5f4569024c19a8dcfaa4a0237', 'after_sha256': '7f98be9047d9b9bf06ce5b8772861dbd503d214e1693b49ccc5d5e5ea7d8ae54'}, 'src/dcar_eval/v8/runtime_receipts.py': {'before_sha256': 'ab430ff282e1ce670758696cea405baff0508642ca442ee6f32018d031e0129a', 'after_sha256': '8d0ba2297297e8ccddbffd41dc74156e1b0621e93a95a9df13a5f747e809a85e'}, 'src/dcar_eval/v8/scan_receipts.py': {'before_sha256': 'e751a89c794b9488ee3157f0800c3b7a2055776edf3fc93fc35dc69f87c2389a', 'after_sha256': 'ef25eb7fc6a794647ab352127b66a654deaf7da9d14bcb57eddf1416a0df6cb7'}, 'tests/test_account_catalog_capture.py': {'before_sha256': None, 'after_sha256': '230bdd88866434e3c402ca7e7c999a21f176f9b063dd2cb8b748df0017635e47'}, 'tests/test_account_catalog_capture_release.py': {'before_sha256': None, 'after_sha256': '740a2c9a1c958d9b0755b50b771c4ccb7a8d8889d4b085b07bc3afc31fdd71a4'}, 'tests/test_macos_snapshot_publisher.py': {'before_sha256': '4ca51aba873f50da29e22a1aae9b4bf6ee052dbe89c2dec897ba5506bf6e4d07', 'after_sha256': 'e2affe3625c70fb0c008002fc445426ba339fa097bd1d45c8935b2fe62ab554c'}, 'tests/test_v8_account_capture_eligibility.py': {'before_sha256': None, 'after_sha256': '8e2350d19b28b085018d4d070ef91576f99732175af2a202dc8511eab31f8cdd'}, 'tests/test_v8_account_catalog_operations.py': {'before_sha256': None, 'after_sha256': '670a66a35ccdcddc61a1a883bd94239c5f57cf6bf4341a114327864be88ad779'}, 'tests/test_v8_account_cleanup.py': {'before_sha256': 'cb7747ddf7e02d586ff643fd659ba7f8d3d81b63da88ce7f23b60655b0949d83', 'after_sha256': '072a339245f2f5a4ab955b9093aa93661f963360964ca88cb9494300b471fcc5'}, 'tests/test_v8_capture_day_coverage.py': {'before_sha256': None, 'after_sha256': '55785793428591d3211b0a53f3fb914e68e13411f601e82ddd9b24a2b26e7122'}, 'tests/test_v8_capture_forward_only.py': {'before_sha256': None, 'after_sha256': '15d726461edca003cabc7186fca9eccb698b1b8770738f9b3ee63f799add5db7'}, 'tests/test_v8_catalog_capture_planner.py': {'before_sha256': None, 'after_sha256': '8cb1fed529d4389bcb77bd6cfb0e041d6d7d17e724a6477b60b3eb7be5e48d59'}, 'tests/test_v8_cleanup_day_readiness.py': {'before_sha256': None, 'after_sha256': '4c75542c064ce30833af316d94385ca311d5909e50b2907e40a9d0a1c71fcc9e'}, 'tests/test_v8_cleanup_readiness.py': {'before_sha256': None, 'after_sha256': '387d7f9b817c521996e3a9954fcd0902c1a01b06e41a33c01501f359fbcd94f8'}, 'tests/test_v8_local_content_analysis.py': {'before_sha256': None, 'after_sha256': '3b6924a0ca3f88ac31091a377f3c0e5257772d3e81d01d2e7b63fa5105db5e6f'}, 'tests/test_v8_manual_content_commands.py': {'before_sha256': 'fd95b11cb028f424dceac19094e84858da288257ad6636ef2acffdb84afa62b8', 'after_sha256': '5687a8da86dc00d6ab1926f30c14a3ece980afba331e66764f5b3005ce5af190'}, 'tests/test_v8_metric_cycle_continuity.py': {'before_sha256': None, 'after_sha256': 'a01173e681eb5c22b51e7f670546af7f62181e3db401545273bb4dd74547cddf'}, 'tests/test_v8_provider_budget.py': {'before_sha256': 'c81641cf30432fc3ca96d354113b752e93903c1a68e5044ca7055e16591d8332', 'after_sha256': '913df6e2d0fd123b48de96b38079f1d0d183263fd5edf13844a0b4731451690d'}, 'tests/test_v8_reconcile_materialization.py': {'before_sha256': '19a3d8586a475f8ba5d03ce01bef151be00360e93abcab171b00da82ab988671', 'after_sha256': '7a3cdfa2b54e18cb4638efa09a836a086a94a9f4701cb0d1d8d32e41e8ffa7e2'}, 'tests/test_v8_report_export.py': {'before_sha256': 'b2ea6a54dcba3973e72e257a2baf57d9af0c64ee635e44c300570c9ef5ce532b', 'after_sha256': '8a69f672513c556c57b687ee79daacc6ff65aa56b84a9dc29b85353408cfb34b'}, 'tests/test_v8_reports.py': {'before_sha256': 'd45c686aa412ec38e2a4bf197242dbcb62c3904ce1d8811ae9b99f73d97a2e10', 'after_sha256': 'a9c43ecf897d649e45a6268f34c5884e3b0a6363b235ea54570209a93a9384ad'}, 'tests/test_v8_scan_receipts.py': {'before_sha256': '5dd5d4f53a5199ba68ee7522246ee89d0fc056394ce4f553f3ec6767fa2c7690', 'after_sha256': '2da35980923644eebe0e11885c227b577aa47c94160df021523880a1c520db39'}}
ACCOUNT_CATALOG_POLICY = {
    "contract": "account-catalog-automatic-capture-policy-v1",
    "statuses": ["daily", "weekly"],
    "identity": "existing_verified",
    "locator_required": True,
    "legacy_enabled_ignored": True,
    "legacy_membership_ignored": True,
    "pending_labels": "blocked_with_reason",
}
_LOADED_SOURCE = Path(__file__).read_bytes()


def require(condition: bool, message: str) -> None:
    if not condition:
        raise ValueError("account catalog release: " + message)


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
        raise ValueError("account catalog release: non-finite receipt value")
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
    require(changes == expected, "source delta differs from reviewed catalog policy change")
    return changes


def parent_context(parent_ref: Mapping[str, Any], *, install_path: Path, database: Path,
                   at: str | None = None) -> tuple[dict[str, Any], dict[str, Any]]:
    require(parent_ref.get("sha256") == PARENT_BUILD_SHA256, "installed parent is not the reviewed build")
    parent = payload_at(parent_ref)
    require(parent.get("schema_contract") == {"code_schema": 21, "formal_schema": 21}
        and parent.get("status") == "succeeded" and isinstance(parent.get("manual_content_scope_successor"), dict)
        and not parent.get("account_catalog_capture_successor"),
        "parent must be the reviewed schema21 manual-content generation")
    # Only load the exact original verifier from its immutable parent source.
    source = Path(parent["source_root"])
    module_path = source / "src/dcar_eval/v8/manual_content_scope_release.py"
    body = raw(module_path, private=False)
    require(hashlib.sha256(body).hexdigest() == parent["critical_files"].get("src/dcar_eval/v8/manual_content_scope_release.py"),
        "parent verifier changed")
    spec = importlib.util.spec_from_file_location("catalog_release_parent_manual", module_path)
    require(spec is not None and spec.loader is not None, "parent verifier unavailable")
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    inherited = module.verify_inheritance(build=parent, build_ref=parent_ref, install_path=install_path,
        database=database, source=source, at=at)
    return parent, inherited


def verify_inheritance(*, build: Mapping[str, Any], build_ref: Mapping[str, Any],
                       install_path: Path, database: Path, source: Path,
                       at: str | None = None) -> dict[str, Any]:
    plan = build.get("account_catalog_capture_successor", {})
    require(plan.get("contract") == CONTRACT and plan.get("transition") == TRANSITION,
        "catalog policy successor contract differs")
    require(dict(build) == payload_at(build_ref), "loaded build differs")
    parent, inherited = parent_context(plan["parent_build"], install_path=install_path, database=database, at=at)
    allowed = {"source_root", "git", "critical_files", "code_successor_plan", "account_cleanup_generation",
               "account_catalog_capture_successor", "created_at", "validation_scope"}
    require({k: v for k, v in build.items() if k not in allowed}
        == {k: v for k, v in parent.items() if k not in allowed}, "schema or inherited execution controls changed")
    generation = build["account_cleanup_generation"]
    require({k: v for k, v in generation.items() if k != "source_tree"}
        == {k: v for k, v in parent["account_cleanup_generation"].items() if k != "source_tree"},
        "legacy operator authority changed")
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
    require(plan.get("account_catalog_policy") == ACCOUNT_CATALOG_POLICY
        and plan.get("account_catalog_policy_sha256") == digest(ACCOUNT_CATALOG_POLICY)
        and plan.get("business_scope_change") == "approved_by_user"
        and plan.get("legacy_execution_controls") == "inherited_unchanged",
        "catalog business policy or approval differs")
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
        "checks": checks, "issued_at": plan["issued_at"], "actor": plan["actor"], "reason": plan["reason"],
        "account_catalog_policy": plan["account_catalog_policy"],
        "account_catalog_policy_sha256": plan["account_catalog_policy_sha256"],
        "business_scope_change": plan["business_scope_change"],
        "legacy_execution_controls": plan["legacy_execution_controls"]}
    proof["proof_sha256"] = digest(proof)
    # The new membership authority is explicit and separate from preserved
    # historical execution controls; consumers must check this verified proof.
    return {**inherited, "catalog_capture_proof": proof,
        "catalog_capture_policy": dict(ACCOUNT_CATALOG_POLICY),
        "catalog_capture_policy_sha256": digest(ACCOUNT_CATALOG_POLICY)}
