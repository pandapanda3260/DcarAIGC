#!/usr/bin/env python3
"""Prepare a Account-profile-recovery successor retaining every historical proof; no DB/service/provider writes.

Reuse existing packaging primitives and focused-check execution. Parent pin and
reviewed delta must be frozen in the verifier before either command can pass.
Only independent source, private evidence and paired plist proposals are created.
"""
from __future__ import annotations

import argparse
from datetime import datetime, timedelta, timezone
import importlib.util
import json
import os
from pathlib import Path
import plistlib
import shutil
import sys
import tempfile

sys.dont_write_bytecode = True
MODULE = "src/dcar_eval/v8/account_profile_recovery_release.py"


def packaging(checkout):
    path = checkout / "scripts/prepare_account_catalog_capture_release.py"
    spec = importlib.util.spec_from_file_location("account_profile_recovery_packaging_primitives", path)
    if spec is None or spec.loader is None:
        raise ValueError("Inherited catalog packaging is unavailable")
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    # verified_inputs/check are generic except for this verifier-path selector.
    # Never call the catalog prepare action: it would reissue catalog policy.
    module.MODULE = MODULE
    return module


def check(args):
    return packaging(args.checkout).check(args)


def prepare(args):
    base = packaging(args.checkout)
    module, parent_ref, parent, installed_bytes, installed, database, install_path, before_tree, changes = base.verified_inputs(args)
    _, inherited = module.parent_context(parent_ref, install_path=install_path, database=database)
    if args.source_root.exists() or args.evidence_root.exists():
        raise ValueError("New independent source and evidence directories are required")
    data = Path(parent["project_root"])
    for other in (data, args.checkout, args.evidence_root):
        if args.source_root == other or args.source_root.is_relative_to(other) or other.is_relative_to(args.source_root):
            raise ValueError("Sealed source must be independent of data, candidate and evidence")
    if args.evidence_root.is_relative_to(data) or args.evidence_root.is_relative_to(args.checkout):
        raise ValueError("Evidence must be outside data and candidate")
    checks = {}
    for value in args.check_report:
        name, separator, filename = value.partition("=")
        if not separator or name in checks:
            raise ValueError("Checks must be unique name=absolute_path arguments")
        ref = module.reference(Path(filename))
        report = module.object_at(ref)
        if (report.get("contract") != module.CHECK_CONTRACT or report.get("name") != name
                or report.get("status") != "passed" or report.get("exit_code") != 0
                or report.get("changes") != changes or not report.get("command")
                or module.reference(Path(report["output"]["path"])) != report["output"]):
            raise ValueError("Focused check does not bind this exact source")
        checks[name] = ref
    if set(checks) != module.REQUIRED_CHECKS:
        raise ValueError("Required focused checks are incomplete")
    publisher_bytes = module.raw(args.publisher_plist, private=False)
    publisher = plistlib.loads(publisher_bytes)
    env = publisher.get("EnvironmentVariables", {})
    if (publisher.get("Label") != "cn.tj.dcar.snapshot-publisher"
            or env.get("DCAR_WRITER_SOURCE_ROOT") != parent["source_root"]
            or env.get("DCAR_PROJECT_ROOT") != str(data) or env.get("DCAR_V8_DB") != str(database)
            or publisher.get("ProgramArguments") != [str(Path(parent["source_root"]) / "deploy/macos/run_snapshot_publisher.sh")]):
        raise ValueError("Publisher differs from the published parent")
    shutil.copytree(args.checkout, args.source_root, symlinks=True, copy_function=shutil.copy2)
    tree = base.inventory(args.source_root)
    if tree["files"] != before_tree["files"] or tree["git"] != before_tree["git"]:
        raise ValueError("Sealed copy differs from the tested candidate")
    args.evidence_root.mkdir(mode=0o700, parents=True)
    tree_ref = base.write(args.evidence_root / "source-tree.json", tree)
    plan_ref = base.write(args.evidence_root / "source-plan.json", {
        "contract": "account-cleanup-source-plan-v1", "transition": "account-cleanup-0907-v1",
        "project_root": str(data), "source_root": str(args.source_root), "git": tree["git"], "source_tree": tree_ref})
    at = datetime.now(timezone.utc).isoformat()
    original_ref = module.reference(args.cohort_evidence)
    original_work_ids, targets = module.cohort_targets(original_ref, database)
    cohort_ref = base.write_bytes(args.evidence_root / "original-cohort.json", module.raw(args.cohort_evidence))
    if cohort_ref["sha256"] != original_ref["sha256"]:
        raise ValueError("Original cohort changed while sealing its evidence")
    authorization = {"contract": module.AUTHORIZATION_CONTRACT, "production_rollout": "approved_by_user",
        "business_e2e": "required", "transport_qualification": "not_verified",
        "actor": args.actor, "reason": args.reason, "user_instruction": args.user_instruction,
        "source_thread_id": args.source_thread_id, "issued_at": at,
        "expires_at": (datetime.fromisoformat(at) + timedelta(hours=24)).isoformat(),
        "original_cohort": cohort_ref, "original_cohort_sha256": cohort_ref["sha256"],
        "original_work_ids": original_work_ids, "targets": targets,
        "max_starts": 4, "max_total_microusd": 4000, "max_amount_microusd": 1000,
        "parent_build": parent_ref, "source_tree": tree_ref,
        "catalog_policy_sha256": inherited["catalog_capture_policy_sha256"]}
    authorization_ref = base.write(args.evidence_root / "compensation-authorization.json", authorization)
    successor = {"contract": module.CONTRACT, "transition": module.TRANSITION,
        "parent_build": parent_ref, "source_tree": tree_ref, "changes": changes, "checks": checks,
        "actor": args.actor, "reason": args.reason, "issued_at": at, "authorization": authorization_ref,
        "production_rollout": "approved_by_user", "transport_qualification": "not_verified",
        "schema_migration_repeated": False, "provider_qualification_repeated": False,
        "database_writes": 0, "paid_gates_reopened": False, "business_scope_change": "none",
        "inherited_catalog_proof_sha256": inherited["catalog_capture_proof"]["proof_sha256"],
        "inherited_control_simplification_proof_sha256": inherited["control_simplification_proof"]["proof_sha256"],
        "inherited_publisher_snapshot_proof_sha256": inherited["publisher_snapshot_proof"]["proof_sha256"],
        **{key: inherited[proof_key]["proof_sha256"] for key, proof_key in (
            ("inherited_publisher_capacity_proof_sha256", "publisher_capacity_proof"),
            ("inherited_account_profile_proof_sha256", "account_profile_proof"),
            ("inherited_profile_operation_authority_sha256", "profile_operation_authority"))}}
    build = {**parent, "source_root": str(args.source_root), "git": tree["git"],
        "critical_files": {row["path"]: row["sha256"] for row in tree["files"]
            if row["path"].startswith(("src/", "config/")) and row["path"].endswith((".py", ".json"))},
        "code_successor_plan": plan_ref,
        "account_cleanup_generation": {**parent["account_cleanup_generation"], "source_tree": tree_ref},
        "account_profile_recovery_successor": successor, "created_at": at,
        "validation_scope": "reviewed Account profile recovery delta; complete Publisher, controls, metric, catalog and historical operator proofs inherited"}
    child_ref = base.write(args.evidence_root / "build.json", {
        "contract_version": "sealed-build-receipt-v1", "payload": build, "payload_sha256": module.digest(build)})
    proof = module.verify_inheritance(build=build, build_ref=child_ref, install_path=install_path,
                                     database=database, source=args.source_root, at=at)
    proposal = {**installed, "EnvironmentVariables": {**installed["EnvironmentVariables"],
        "DCAR_LOADED_BUILD_RECEIPT": child_ref["path"], "DCAR_WRITER_SOURCE_ROOT": str(args.source_root),
        "PYTHONPATH": str(args.source_root / "src/dcar_eval") + os.pathsep + str(args.source_root / "scripts")},
        "ProgramArguments": [str(args.source_root / "deploy/macos/run_writer_worker.sh")]}
    publisher_next = {**publisher, "EnvironmentVariables": {**env, "DCAR_WRITER_SOURCE_ROOT": str(args.source_root)},
        "ProgramArguments": [str(args.source_root / "deploy/macos/run_snapshot_publisher.sh")]}
    with tempfile.TemporaryDirectory() as tmp:
        home = Path(tmp).resolve()
        fixture = home / "Library/LaunchAgents/cn.tj.dcar.writer-worker.plist"
        fixture.parent.mkdir(parents=True)
        fixture.write_bytes(plistlib.dumps(proposal))
        fixture.chmod(0o600)
        bootstrap = base.load(args.source_root / "src/dcar_eval/v8/runtime_paths.py", "account_profile_recovery_frozen_bootstrap")
        verification = bootstrap.verify_source_before_import(data=data, source=args.source_root,
            build_receipt=Path(child_ref["path"]), home=home)
    if (module.raw(args.installed_plist, private=False) != installed_bytes
            or module.raw(args.publisher_plist, private=False) != publisher_bytes
            or base.inventory(args.checkout) != before_tree):
        raise ValueError("Installed pair or candidate changed during preparation")
    result = {"contract": "account-profile-recovery-install-proposal-v1", "status": "prepared", "created_at": at,
        "installed_plist": str(args.installed_plist),
        "before_plist": base.write_bytes(args.evidence_root / "writer.before.plist", installed_bytes),
        "next_plist": base.write_bytes(args.evidence_root / "writer.next.plist", plistlib.dumps(proposal, sort_keys=True)),
        "publisher": {"installed_plist": str(args.publisher_plist),
            "before_plist": base.write_bytes(args.evidence_root / "publisher.before.plist", publisher_bytes),
            "next_plist": base.write_bytes(args.evidence_root / "publisher.next.plist", plistlib.dumps(publisher_next, sort_keys=True))},
        "parent_build": parent_ref, "child_build": child_ref, "formal_database": str(database),
        "database_identity": {"device": database.stat().st_dev, "inode": database.stat().st_ino},
        "bootstrap_verification": verification,
        "inherited_catalog_proof_sha256": proof["catalog_capture_proof"]["proof_sha256"],
        "inherited_control_simplification_proof_sha256": proof["control_simplification_proof"]["proof_sha256"],
        "inherited_publisher_snapshot_proof_sha256": proof["publisher_snapshot_proof"]["proof_sha256"],
        "account_profile_recovery_proof_sha256": proof["account_profile_recovery_proof"]["proof_sha256"],
        "profile_compensation_authority_sha256": proof["profile_compensation_authority"]["proof_sha256"],
        "authorization": authorization_ref,
        **{key: proof[proof_key]["proof_sha256"] for key, proof_key in (
            ("inherited_publisher_capacity_proof_sha256", "publisher_capacity_proof"),
            ("inherited_account_profile_proof_sha256", "account_profile_proof"),
            ("inherited_profile_operation_authority_sha256", "profile_operation_authority"))},
        "database_writes": 0, "provider_calls": 0, "services_changed": False, "paid_gates_reopened": False,
        "schema_migration_repeated": False, "business_scope_change": "none"}
    return {"proposal": base.write(args.evidence_root / "install-proposal.json", result), **result}


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    commands = parser.add_subparsers(dest="action", required=True)
    for action in ("check", "prepare"):
        command = commands.add_parser(action)
        for name in ("checkout", "parent-build", "installed-plist"):
            command.add_argument("--" + name, type=lambda value: Path(value).resolve(strict=True), required=True)
    check_parser, prepare_parser = commands.choices["check"], commands.choices["prepare"]
    check_parser.add_argument("--name", required=True)
    check_parser.add_argument("--output", type=lambda value: Path(value).absolute(), required=True)
    check_parser.add_argument("command", nargs=argparse.REMAINDER)
    for name in ("source-root", "evidence-root"):
        prepare_parser.add_argument("--" + name, type=lambda value: Path(value).absolute(), required=True)
    prepare_parser.add_argument("--publisher-plist", type=lambda value: Path(value).resolve(strict=True), required=True)
    prepare_parser.add_argument("--check-report", action="append", default=[])
    prepare_parser.add_argument("--actor", required=True)
    prepare_parser.add_argument("--reason", required=True)
    prepare_parser.add_argument("--user-instruction", required=True)
    prepare_parser.add_argument("--source-thread-id", required=True)
    prepare_parser.add_argument("--cohort-evidence", type=lambda value: Path(value).resolve(strict=True), required=True)
    args = parser.parse_args()
    print(json.dumps(check(args) if args.action == "check" else prepare(args), ensure_ascii=False, sort_keys=True))


if __name__ == "__main__":
    main()
