#!/usr/bin/env python3
"""Review-bound, offline code packaging; never touches a database or services.

check runs one supplied argv without a shell and records the exact source delta.
prepare copies an independent Git source tree, emits new immutable receipts and
an installable plist proposal, then runs the real stdlib bootstrap against that
proposal. Existing receipt files and the installed plist are never modified.
"""
from __future__ import annotations

import argparse
from datetime import datetime, timezone
import hashlib
import importlib.util
import json
import os
from pathlib import Path
import plistlib
import stat
import subprocess
import sys
import tempfile

sys.dont_write_bytecode = True
MODULE = "src/dcar_eval/v8/account_classification_release.py"


def load(path, name):
    spec = importlib.util.spec_from_file_location(name, path)
    if spec is None or spec.loader is None:
        raise ValueError("Cannot load reviewed source verifier")
    result = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(result)
    return result


def write(path, value):
    body = (json.dumps(value, ensure_ascii=False, sort_keys=True, separators=(",", ":")) + "\n").encode()
    return write_bytes(path, body)


def write_bytes(path, body):
    descriptor = os.open(path, os.O_WRONLY | os.O_CREAT | os.O_EXCL, 0o600)
    with os.fdopen(descriptor, "wb") as stream:
        stream.write(body)
        stream.flush()
        os.fsync(stream.fileno())
    return {"path": str(path), "sha256": hashlib.sha256(body).hexdigest(), "byte_size": len(body)}


def inventory(root):
    paths = load(root / "src/dcar_eval/v8/runtime_paths.py", "reviewed_runtime_paths")
    def git(*args):
        return paths.verified_git(root, *args)
    status = git("status", "--porcelain=v1", "--untracked-files=all")
    record = {"mode": "working-tree-source-v1", "head": git("rev-parse", "HEAD").decode().strip(),
              "tree": git("rev-parse", "HEAD^{tree}").decode().strip(),
              "branch": git("symbolic-ref", "--short", "HEAD").decode().strip(),
              "status_porcelain_sha256": hashlib.sha256(status).hexdigest()}
    names = sorted({os.fsdecode(name) for name in git("ls-files", "--cached", "--others", "--exclude-standard", "-z").split(b"\0") if name})
    records = []
    for name in names:
        path = root / name
        if not path.exists() and not path.is_symlink():
            continue
        body = paths._raw_file(path, limit=16 * 1024 * 1024)
        records.append({"path": name, "sha256": hashlib.sha256(body).hexdigest(),
                        "byte_size": len(body), "mode": stat.S_IMODE(path.stat().st_mode)})
    if git("status", "--porcelain=v1", "--untracked-files=all") != status:
        raise ValueError("Source changed while collecting its manifest")
    return {"contract": "writer-source-tree-v1", "source_root": str(root), "git": record, "files": records}


def verified_inputs(checkout, parent_build, parent_install):
    module = load(checkout / MODULE, "reviewed_account_classification_successor")
    build_ref, install_ref = module.reference(parent_build), module.reference(parent_install)
    module.require(build_ref["sha256"] == module.PARENT_BUILD_SHA256
                   and install_ref["sha256"] == module.PARENT_INSTALL_SHA256, "reviewed parent pins differ")
    parent = module.payload_at(build_ref, "sealed-build-receipt-v1")
    original = module.object_at(parent["account_cleanup_generation"]["source_tree"])
    current = inventory(checkout)
    module.source_changes(original, current)
    return module, build_ref, install_ref, parent, current


def validate_installed_writer(module, installed_plist, parent, parent_install):
    """Accept the original generation or its already verified code-only sibling."""
    current_plist = module.raw(installed_plist, private=False)
    installed = plistlib.loads(current_plist)
    environment = installed.get("EnvironmentVariables", {})
    install = module.object_at(module.reference(parent_install))
    database, data = Path(install["formal_database"]), Path(parent["project_root"])
    source = Path(environment.get("DCAR_WRITER_SOURCE_ROOT", ""))
    if (environment.get("DCAR_ACCOUNT_CLEANUP_INSTALL_RECEIPT") != str(parent_install)
            or environment.get("DCAR_V8_DB") != str(database)
            or environment.get("DCAR_PROJECT_ROOT") != str(data)
            or not source.is_absolute() or source.resolve(strict=True) != source
            or installed.get("ProgramArguments") != [str(source / "deploy/macos/run_writer_worker.sh")]):
        raise ValueError("Installed Writer differs from the original cleanup authority")
    loaded_ref = module.reference(Path(environment["DCAR_LOADED_BUILD_RECEIPT"]))
    if loaded_ref["sha256"] not in module.APPROVED_PREDECESSOR_BUILD_SHA256:
        raise ValueError("Installed predecessor build has not been explicitly reviewed")
    loaded = module.payload_at(loaded_ref, "sealed-build-receipt-v1")
    generation, original = loaded.get("account_cleanup_generation", {}), parent["account_cleanup_generation"]
    if (loaded.get("schema_contract") != {"code_schema": 20, "formal_schema": 20}
            or loaded.get("runtime_root_receipt") != parent.get("runtime_root_receipt")
            or loaded.get("source_root") != str(source)
            or {k: v for k, v in generation.items() if k != "source_tree"}
                != {k: v for k, v in original.items() if k != "source_tree"}):
        raise ValueError("Installed Writer does not inherit the original capture authority")
    # This is the currently installed bootstrap, not candidate application code.
    # Check its sealed hash before loading it, then verify its complete source.
    bootstrap_path = source / "src/dcar_eval/v8/runtime_paths.py"
    if hashlib.sha256(module.raw(bootstrap_path, private=False)).hexdigest() != loaded["critical_files"].get("src/dcar_eval/v8/runtime_paths.py"):
        raise ValueError("Installed bootstrap differs from its sealed build")
    bootstrap = load(bootstrap_path, "installed_classification_predecessor_bootstrap")
    with tempfile.TemporaryDirectory() as tmp:
        home = Path(tmp).resolve()
        fixture = home / "Library/LaunchAgents/cn.tj.dcar.writer-worker.plist"
        fixture.parent.mkdir(parents=True)
        fixture.write_bytes(current_plist)
        fixture.chmod(0o600)
        bootstrap.verify_source_before_import(data=data, source=source, build_receipt=Path(loaded_ref["path"]), home=home)
    return current_plist, installed


def check(args):
    module, _, _, parent, before = verified_inputs(args.checkout, args.parent_build, args.parent_install)
    original = module.object_at(parent["account_cleanup_generation"]["source_tree"])
    changes = module.source_changes(original, before)
    if args.name not in module.REQUIRED_CHECKS or not args.command:
        raise ValueError("A required check name and concrete command argv are required")
    command = args.command[1:] if args.command[0] == "--" else args.command
    environment = dict(os.environ)
    for name in ("DCAR_WRITER_SOURCE_ROOT", "DCAR_PROJECT_ROOT", "DCAR_LOADED_BUILD_ID", "DCAR_LOADED_BUILD_RECEIPT",
                 "DCAR_ACCOUNT_CLEANUP_INSTALL_RECEIPT", "DCAR_V8_DB", "DCAR_WRITER_LOCK"):
        environment.pop(name, None)
    environment.update(PYTHONDONTWRITEBYTECODE="1", PYTHONPATH=str(args.checkout / "src/dcar_eval") + os.pathsep + str(args.checkout / "tests"))
    result = subprocess.run(command, cwd=args.checkout, env=environment, capture_output=True)
    output = result.stdout + result.stderr
    log = write_bytes(args.output.with_suffix(".log"), output)
    if inventory(args.checkout) != before:
        raise ValueError("Source changed during check; no passing receipt issued")
    receipt = {"contract": "account-classification-check-v1", "name": args.name,
               "status": "passed" if result.returncode == 0 else "failed", "exit_code": result.returncode,
               "command": command, "changes": changes, "output": log,
               "completed_at": datetime.now(timezone.utc).isoformat()}
    write(args.output, receipt)
    if result.returncode:
        raise ValueError(f"{args.name} failed; see {args.output.with_suffix('.log')}")
    return receipt


def prepare(args):
    module, build_ref, install_ref, parent, checkout_tree = verified_inputs(args.checkout, args.parent_build, args.parent_install)
    if args.source_root.exists() or args.evidence_root.exists():
        raise ValueError("New source and evidence directories are required")
    data = Path(parent["project_root"])
    if args.source_root == data or args.source_root.is_relative_to(data) or data.is_relative_to(args.source_root):
        raise ValueError("Immutable source must be independent of the mutable data project")
    if args.evidence_root.is_relative_to(data) or args.evidence_root.is_relative_to(args.source_root):
        raise ValueError("Private evidence must be external to mutable project and sealed source")
    install = module.object_at(install_ref)
    database = Path(install["formal_database"])
    current_plist, installed = validate_installed_writer(module, args.installed_plist, parent, args.parent_install)
    environment = installed.get("EnvironmentVariables", {})
    if (environment.get("DCAR_ACCOUNT_CLEANUP_INSTALL_RECEIPT") != str(args.parent_install)
            or environment.get("DCAR_V8_DB") != str(database)
            or environment.get("DCAR_PROJECT_ROOT") != str(data)):
        raise ValueError("Installed Writer changed from the reviewed parent")
    migration_ref = module.reference(args.migration)
    migration = module.object_at(migration_ref)
    if (migration.get("previous_writer_plist_sha256") != hashlib.sha256(current_plist).hexdigest()
            or migration.get("previous_loaded_build") != module.reference(Path(environment["DCAR_LOADED_BUILD_RECEIPT"]))):
        raise ValueError("Installed Writer changed after transactional migration")
    args.source_root.parent.mkdir(parents=True, exist_ok=True)
    subprocess.run(["git", "clone", "--local", "--no-hardlinks", "--branch", checkout_tree["git"]["branch"],
                    str(args.checkout), str(args.source_root)], check=True, capture_output=True,
                   env={**os.environ, "GIT_CONFIG_NOSYSTEM": "1", "GIT_CONFIG_GLOBAL": os.devnull})
    paths = load(args.checkout / "src/dcar_eval/v8/runtime_paths.py", "packaging_runtime_paths")
    tracked = {os.fsdecode(name) for name in paths.verified_git(args.source_root, "ls-files", "-z").split(b"\0") if name}
    live_names = {row["path"] for row in checkout_tree["files"]}
    for name in tracked - live_names:
        (args.source_root / name).unlink()
    for record in checkout_tree["files"]:
        source, target = args.checkout / record["path"], args.source_root / record["path"]
        body = paths._raw_file(source, limit=16 * 1024 * 1024)
        if hashlib.sha256(body).hexdigest() != record["sha256"]:
            raise ValueError("Reviewed source changed during packaging")
        target.parent.mkdir(parents=True, exist_ok=True)
        target.write_bytes(body)
        target.chmod(record["mode"])
    current = inventory(args.source_root)
    original = module.object_at(parent["account_cleanup_generation"]["source_tree"])
    changes = module.source_changes(original, current)
    if current["files"] != checkout_tree["files"] or current["git"] != checkout_tree["git"]:
        raise ValueError("Independent clone differs from reviewed checkout")
    args.evidence_root.mkdir(mode=0o700, parents=True)
    checks = {}
    for value in args.check_report:
        name, sep, path = value.partition("=")
        if not sep or name in checks:
            raise ValueError("Checks must be unique name=absolute_path arguments")
        ref = module.reference(Path(path))
        report = module.object_at(ref)
        if (report.get("name") != name or report.get("status") != "passed" or report.get("exit_code") != 0
                or report.get("changes") != changes or report.get("contract") != "account-classification-check-v1"):
            raise ValueError("Check is not a real passing report on the exact reviewed source")
        output = report.get("output", {})
        if module.reference(Path(output["path"])) != output:
            raise ValueError("Check output was modified")
        checks[name] = ref
    if set(checks) != module.REQUIRED_CHECKS:
        raise ValueError("Required checks are incomplete")
    tree_ref = write(args.evidence_root / "source-tree.json", current)
    source_plan = {"contract": "account-cleanup-source-plan-v1", "transition": "account-cleanup-0907-v1",
                   "project_root": str(data), "source_root": str(args.source_root), "git": current["git"], "source_tree": tree_ref}
    source_plan_ref = write(args.evidence_root / "source-plan.json", source_plan)
    at = datetime.now(timezone.utc).isoformat()
    successor = {"contract": module.CONTRACT, "transition": module.TRANSITION,
        "parent_build": build_ref, "parent_install": install_ref, "source_tree": tree_ref, "changes": changes,
        "checks": checks, "migration": migration_ref, "production_rollout": "approved_by_user",
        "issued_at": at, "actor": args.actor, "reason": args.reason,
        "business_e2e": "deferred_by_user", "transport_qualification": "not_verified"}
    generation = {**parent["account_cleanup_generation"], "source_tree": tree_ref}
    build = {**parent, "source_root": str(args.source_root), "git": current["git"],
             "critical_files": {row["path"]: row["sha256"] for row in current["files"]
                                if row["path"].startswith(("src/", "config/")) and row["path"].endswith((".py", ".json"))},
             "code_successor_plan": source_plan_ref, "account_cleanup_generation": generation,
             "account_classification_successor": successor, "schema_contract": {"code_schema": 21, "formal_schema": 21}, "created_at": at,
             "validation_scope": "reviewed schema21 classification migration; original activation, RELEASE and operator scopes retained"}
    child_ref = write(args.evidence_root / "build.json", {"contract_version": "sealed-build-receipt-v1",
                                                        "payload": build, "payload_sha256": module.digest(build)})
    module.verify_inheritance(build=build, build_ref=child_ref, install_path=args.parent_install,
                              database=database, source=args.source_root, at=at)
    proposal = dict(installed)
    proposal["EnvironmentVariables"] = dict(environment)
    proposal["EnvironmentVariables"].update(DCAR_LOADED_BUILD_RECEIPT=child_ref["path"],
        DCAR_WRITER_SOURCE_ROOT=str(args.source_root),
        PYTHONPATH=str(args.source_root / "src/dcar_eval") + os.pathsep + str(args.source_root / "scripts"))
    proposal["ProgramArguments"] = [str(args.source_root / "deploy/macos/run_writer_worker.sh")]
    before_ref = write_bytes(args.evidence_root / "writer.before.plist", current_plist)
    next_ref = write_bytes(args.evidence_root / "writer.next.plist", plistlib.dumps(proposal, sort_keys=True))
    with tempfile.TemporaryDirectory() as tmp:
        home = Path(tmp).resolve()
        fixture_plist = home / "Library/LaunchAgents/cn.tj.dcar.writer-worker.plist"
        fixture_plist.parent.mkdir(parents=True)
        fixture_plist.write_bytes(plistlib.dumps(proposal))
        fixture_plist.chmod(0o600)
        verified = load(args.source_root / "src/dcar_eval/v8/runtime_paths.py", "frozen_runtime_paths")
        result = verified.verify_source_before_import(data=data, source=args.source_root,
            build_receipt=Path(child_ref["path"]), home=home)
    publisher_proposal = None
    if args.publisher_plist is not None:
        publisher_bytes = module.raw(args.publisher_plist, private=False)
        publisher = plistlib.loads(publisher_bytes)
        publisher_environment = publisher.get("EnvironmentVariables", {})
        if (publisher.get("Label") != "cn.tj.dcar.snapshot-publisher"
                or publisher_environment.get("DCAR_WRITER_SOURCE_ROOT") not in {parent["source_root"], environment["DCAR_WRITER_SOURCE_ROOT"]}
                or publisher_environment.get("DCAR_PROJECT_ROOT") != str(data)
                or publisher_environment.get("DCAR_V8_DB") != str(database)
                or publisher.get("ProgramArguments") != [str(Path(publisher_environment.get("DCAR_WRITER_SOURCE_ROOT", "")) / "deploy/macos/run_snapshot_publisher.sh")]):
            raise ValueError("Publisher does not match the installed parent source")
        next_publisher = {**publisher, "EnvironmentVariables": {**publisher_environment,
            "DCAR_WRITER_SOURCE_ROOT": str(args.source_root)},
            "ProgramArguments": [str(args.source_root / "deploy/macos/run_snapshot_publisher.sh")]}
        publisher_proposal = {"installed_plist": str(args.publisher_plist),
            "before_plist": write_bytes(args.evidence_root / "publisher.before.plist", publisher_bytes),
            "next_plist": write_bytes(args.evidence_root / "publisher.next.plist", plistlib.dumps(next_publisher, sort_keys=True))}
    plan = {"contract": "account-classification-install-proposal-v1", "status": "prepared",
            "created_at": at, "installed_plist": str(args.installed_plist), "before_plist": before_ref,
            "next_plist": next_ref, "parent_build": build_ref, "parent_install": install_ref,
            "child_build": child_ref, "formal_database": str(database), "database_identity": {
                "device": database.stat().st_dev, "inode": database.stat().st_ino},
            "bootstrap_verification": result, "publisher": publisher_proposal,
            "database_writes": "classification migration recorded separately", "services_changed": False, "migration": migration_ref,
            "install_steps": ["Wait for concurrent maintenance and profile writes to finish.",
                "Stop Writer with launchctl; require its process and DB handles to be gone.",
                "Verify the installed plist still equals writer.before.plist and all proposal hashes.",
                "Atomically replace only the Writer plist with writer.next.plist, preserving the original install receipt.",
                "Start Writer, verify bootstrap, API, exact loaded build and unchanged DB inode.",
                "On startup failure, keep capture stopped and perform the explicit verified inverse classification migration before restoring prior code; never overwrite newer data."]}
    return {"proposal": write(args.evidence_root / "install-proposal.json", plan), **plan}


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    sub = parser.add_subparsers(dest="action", required=True)
    for action in ("check", "prepare"):
        target = sub.add_parser(action)
        for name in ("checkout", "parent-build", "parent-install"):
            target.add_argument("--" + name, type=lambda value: Path(value).resolve(strict=True), required=True)
    check_parser, prepare_parser = sub.choices["check"], sub.choices["prepare"]
    check_parser.add_argument("--name", required=True)
    check_parser.add_argument("--output", type=lambda value: Path(value).absolute(), required=True)
    check_parser.add_argument("command", nargs=argparse.REMAINDER)
    for name in ("source-root", "evidence-root", "installed-plist", "migration"):
        prepare_parser.add_argument("--" + name, type=lambda value: Path(value).absolute(), required=True)
    prepare_parser.add_argument("--publisher-plist", type=lambda value: Path(value).resolve(strict=True))
    prepare_parser.add_argument("--check-report", action="append", default=[])
    prepare_parser.add_argument("--actor", required=True)
    prepare_parser.add_argument("--reason", required=True)
    args = parser.parse_args()
    print(json.dumps(check(args) if args.action == "check" else prepare(args), ensure_ascii=False, sort_keys=True))


if __name__ == "__main__":
    main()
