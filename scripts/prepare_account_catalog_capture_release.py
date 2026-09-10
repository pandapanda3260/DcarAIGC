#!/usr/bin/env python3
"""Prepare the reviewed schema21 account-catalog policy update without DB writes.

check executes the supplied focused test argv and records the exact source.
prepare produces an independent sealed source, child build and reversible plist
proposal, then verifies the real bootstrap against a temporary proposal home.
The approved business scope is explicit. Neither action changes services, the
formal database, existing provider gates, budgets or qualification.
"""
from __future__ import annotations

import argparse
import hashlib
from datetime import datetime, timezone
import importlib.util
import json
import os
from pathlib import Path
import plistlib
import shutil
import subprocess
import sys
import tempfile

sys.dont_write_bytecode = True
ROOT = Path(__file__).resolve().parents[1]
MODULE = "src/dcar_eval/v8/account_catalog_capture_release.py"


def load(path, name):
    spec = importlib.util.spec_from_file_location(name, path)
    if spec is None or spec.loader is None:
        raise ValueError("Reviewed source module unavailable")
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    return module


base = load(ROOT / "scripts/prepare_account_classification_release.py", "catalog_release_packaging_primitives")
write, write_bytes, inventory = base.write, base.write_bytes, base.inventory


def verified_inputs(args):
    module = load(args.checkout / MODULE, "reviewed_catalog_release")
    parent_ref = module.reference(args.parent_build)
    installed_bytes = module.raw(args.installed_plist, private=False)
    installed = plistlib.loads(installed_bytes)
    environment = installed.get("EnvironmentVariables", {})
    install_path = Path(environment.get("DCAR_ACCOUNT_CLEANUP_INSTALL_RECEIPT", ""))
    database = Path(environment.get("DCAR_V8_DB", ""))
    parent, _ = module.parent_context(parent_ref, install_path=install_path, database=database)
    source = Path(parent["source_root"])
    if (environment.get("DCAR_LOADED_BUILD_RECEIPT") != str(args.parent_build)
            or environment.get("DCAR_WRITER_SOURCE_ROOT") != str(source)
            or environment.get("DCAR_PROJECT_ROOT") != parent["project_root"]
            or installed.get("ProgramArguments") != [str(source / "deploy/macos/run_writer_worker.sh")]):
        raise ValueError("Installed Writer no longer matches the reviewed predecessor")
    tree = inventory(args.checkout)
    changes = module.source_changes(module.object_at(parent["account_cleanup_generation"]["source_tree"]), tree)
    return module, parent_ref, parent, installed_bytes, installed, database, install_path, tree, changes


def check(args):
    module, _, _, _, _, _, _, before, changes = verified_inputs(args)
    if args.name not in module.REQUIRED_CHECKS or not args.command:
        raise ValueError("A required focused check name and concrete argv are required")
    command = args.command[1:] if args.command[0] == "--" else args.command
    if not command:
        raise ValueError("A concrete check command is required")
    environment = dict(os.environ)
    for name in ("DCAR_WRITER_SOURCE_ROOT", "DCAR_PROJECT_ROOT", "DCAR_LOADED_BUILD_ID", "DCAR_LOADED_BUILD_RECEIPT",
                 "DCAR_ACCOUNT_CLEANUP_INSTALL_RECEIPT", "DCAR_V8_DB", "DCAR_WRITER_LOCK"):
        environment.pop(name, None)
    environment.update(PYTHONDONTWRITEBYTECODE="1",
        PYTHONPATH=str(args.checkout / "src/dcar_eval") + os.pathsep + str(args.checkout / "tests"))
    result = subprocess.run(command, cwd=args.checkout, env=environment, capture_output=True)
    log = write_bytes(args.output.with_suffix(".log"), result.stdout + result.stderr)
    if inventory(args.checkout) != before:
        raise ValueError("Source changed during the focused check; no passing receipt issued")
    receipt = {"contract": module.CHECK_CONTRACT, "name": args.name,
        "status": "passed" if result.returncode == 0 else "failed", "exit_code": result.returncode,
        "command": command, "changes": changes, "output": log,
        "completed_at": datetime.now(timezone.utc).isoformat()}
    write(args.output, receipt)
    if result.returncode:
        raise ValueError("Focused check failed; see " + str(args.output.with_suffix('.log')))
    return receipt


def prepare(args):
    module, parent_ref, parent, installed_bytes, installed, database, install_path, checkout_tree, changes = verified_inputs(args)
    if args.source_root.exists() or args.evidence_root.exists():
        raise ValueError("New source and evidence directories are required")
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
    # Source is a complete independent copy, including reviewed uncommitted
    # changes; no linked worktree, Git alternates or shared object hardlinks.
    shutil.copytree(args.checkout, args.source_root, symlinks=True, copy_function=shutil.copy2)
    tree = inventory(args.source_root)
    if tree["files"] != checkout_tree["files"] or tree["git"] != checkout_tree["git"]:
        raise ValueError("Sealed copy differs from the tested candidate")
    args.evidence_root.mkdir(mode=0o700, parents=True)
    tree_ref = write(args.evidence_root / "source-tree.json", tree)
    source_plan = {"contract": "account-cleanup-source-plan-v1", "transition": "account-cleanup-0907-v1",
        "project_root": str(data), "source_root": str(args.source_root), "git": tree["git"], "source_tree": tree_ref}
    plan_ref = write(args.evidence_root / "source-plan.json", source_plan)
    at = datetime.now(timezone.utc).isoformat()
    successor = {"contract": module.CONTRACT, "transition": module.TRANSITION,
        "parent_build": parent_ref, "source_tree": tree_ref, "changes": changes, "checks": checks,
        "actor": args.actor, "reason": args.reason, "issued_at": at,
        "production_rollout": "approved_by_user", "transport_qualification": "not_verified",
        "schema_migration_repeated": False, "provider_qualification_repeated": False,
        "account_catalog_policy": module.ACCOUNT_CATALOG_POLICY,
        "account_catalog_policy_sha256": module.digest(module.ACCOUNT_CATALOG_POLICY),
        "business_scope_change": "approved_by_user", "legacy_execution_controls": "inherited_unchanged"}
    build = {**parent, "source_root": str(args.source_root), "git": tree["git"],
        "critical_files": {row["path"]: row["sha256"] for row in tree["files"]
            if row["path"].startswith(("src/", "config/")) and row["path"].endswith((".py", ".json"))},
        "code_successor_plan": plan_ref,
        "account_cleanup_generation": {**parent["account_cleanup_generation"], "source_tree": tree_ref},
        "account_catalog_capture_successor": successor, "created_at": at,
        "validation_scope": "approved account-catalog automatic membership; original provider and operator controls preserved"}
    child_ref = write(args.evidence_root / "build.json", {"contract_version": "sealed-build-receipt-v1",
        "payload": build, "payload_sha256": module.digest(build)})
    module.verify_inheritance(build=build, build_ref=child_ref, install_path=install_path,
        database=database, source=args.source_root, at=at)
    proposal = {**installed, "EnvironmentVariables": {**installed["EnvironmentVariables"],
        "DCAR_LOADED_BUILD_RECEIPT": child_ref["path"], "DCAR_WRITER_SOURCE_ROOT": str(args.source_root),
        "PYTHONPATH": str(args.source_root / "src/dcar_eval") + os.pathsep + str(args.source_root / "scripts")},
        "ProgramArguments": [str(args.source_root / "deploy/macos/run_writer_worker.sh")]}
    before_ref = write_bytes(args.evidence_root / "writer.before.plist", installed_bytes)
    next_ref = write_bytes(args.evidence_root / "writer.next.plist", plistlib.dumps(proposal, sort_keys=True))
    with tempfile.TemporaryDirectory() as tmp:
        home = Path(tmp).resolve()
        fixture = home / "Library/LaunchAgents/cn.tj.dcar.writer-worker.plist"
        fixture.parent.mkdir(parents=True)
        fixture.write_bytes(plistlib.dumps(proposal))
        fixture.chmod(0o600)
        bootstrap = load(args.source_root / "src/dcar_eval/v8/runtime_paths.py", "catalog_release_frozen_bootstrap")
        verification = bootstrap.verify_source_before_import(data=data, source=args.source_root,
            build_receipt=Path(child_ref["path"]), home=home)
    publisher_proposal = None
    if args.publisher_plist:
        body = module.raw(args.publisher_plist, private=False)
        publisher = plistlib.loads(body)
        env = publisher.get("EnvironmentVariables", {})
        official_parent = [str(Path(parent["source_root"]) / "deploy/macos/run_snapshot_publisher.sh")]
        reviewed_retention_overlay = (hashlib.sha256(body).hexdigest() ==
            "f8d9710c1af9cdf7fc5bf668b3c274a8bcaa142d6de9c35f7ffa5ac4a8e45c76")
        if (publisher.get("Label") != "cn.tj.dcar.snapshot-publisher"
                or env.get("DCAR_WRITER_SOURCE_ROOT") != parent["source_root"]
                or env.get("DCAR_PROJECT_ROOT") != str(data) or env.get("DCAR_V8_DB") != str(database)
                or (publisher.get("ProgramArguments") != official_parent and not reviewed_retention_overlay)):
            raise ValueError("Publisher differs from the installed parent source")
        next_publisher = {**publisher, "EnvironmentVariables": {**env, "DCAR_WRITER_SOURCE_ROOT": str(args.source_root)},
            "ProgramArguments": [str(args.source_root / "deploy/macos/run_snapshot_publisher.sh")]}
        publisher_proposal = {"installed_plist": str(args.publisher_plist),
            "before_plist": write_bytes(args.evidence_root / "publisher.before.plist", body),
            "next_plist": write_bytes(args.evidence_root / "publisher.next.plist", plistlib.dumps(next_publisher, sort_keys=True))}
    if module.raw(args.installed_plist, private=False) != installed_bytes or inventory(args.checkout) != checkout_tree:
        raise ValueError("Installed Writer or reviewed candidate changed while preparing")
    result = {"contract": "account-catalog-capture-install-proposal-v1", "status": "prepared", "created_at": at,
        "installed_plist": str(args.installed_plist), "before_plist": before_ref, "next_plist": next_ref,
        "parent_build": parent_ref, "child_build": child_ref, "formal_database": str(database),
        "database_identity": {"device": database.stat().st_dev, "inode": database.stat().st_ino},
        "bootstrap_verification": verification, "publisher": publisher_proposal,
        "database_writes": 0, "provider_calls": 0, "services_changed": False,
        "business_scope_change": "approved_by_user",
        "account_catalog_policy": module.ACCOUNT_CATALOG_POLICY,
        "account_catalog_policy_sha256": module.digest(module.ACCOUNT_CATALOG_POLICY),
        "legacy_execution_controls": "inherited_unchanged",
        "install_steps": ["Drain current work, then stop Writer and paired Publisher once.",
            "Confirm both stopped, acquire the formal maintenance lease and recheck all active work.",
            "Create and verify a private SQLite backup and control-table evidence without replacing the formal DB.",
            "Verify current plists still equal saved before copies and all proposal hashes.",
            "Atomically replace only the paired plists and start Writer on the new source/build.",
            "Verify health, exact loaded build and unchanged schema21 database inode; then start Publisher.",
            "On startup failure, stop the new services and restore paired before plists. Keep the current database; do not restore stale data."]}
    return {"proposal": write(args.evidence_root / "install-proposal.json", result), **result}


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
    args = parser.parse_args()
    print(json.dumps(check(args) if args.action == "check" else prepare(args), ensure_ascii=False, sort_keys=True))


if __name__ == "__main__":
    main()
