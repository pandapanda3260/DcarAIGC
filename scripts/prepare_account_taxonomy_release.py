#!/usr/bin/env python3
"""Freeze/check/prepare a bounded taxonomy successor; install only on explicit activate.

preflight is read-only. prepare writes only independent proposal files. activate
and rollback require the installed maintenance lock and zero running jobs/leases,
never write the formal database, start services, or modify the Publisher.
"""

# ruff: noqa: E402
from __future__ import annotations

import argparse
from datetime import datetime, timezone
import hashlib
import json
import os
from pathlib import Path
import plistlib
import re
import sqlite3
import subprocess
import sys
import tempfile

sys.dont_write_bytecode = True
ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT / "src/dcar_eval"))
sys.path.insert(0, str(ROOT / "scripts"))
from v8 import (
    account_taxonomy_code_successor as release,
    four_platform_flow_release as flow,
)
from v8.runtime_paths import verified_git, verify_source_before_import
from v8.runtime_database import (
    DatabaseAccessMode,
    hold_formal_mutation,
    observe_writer_lock,
    resolve_installed_database_access,
    load_installed_writer_contract,
)
from prepare_account_intake_release import write, write_bytes, checks_at

PROPOSAL = "account-taxonomy-install-proposal-v1"


def now():
    return datetime.now(timezone.utc).isoformat()


def external(path, *roots):
    release.require(
        path.is_absolute()
        and path.resolve() == path
        and all(
            path != root
            and not path.is_relative_to(root)
            and not root.is_relative_to(path)
            for root in roots
        ),
        "new evidence/source path must be independent and without symlinks",
    )


def freeze(args):
    parent_ref = release.reference(args.parent_build)
    parent = release.payload_at(parent_ref, "sealed-build-receipt-v1")
    original = Path(parent["source_root"])
    tree = release.object_at(parent["account_cleanup_generation"]["source_tree"])
    release.require(
        release.FIELD not in parent
        and parent.get("schema_contract") == {"code_schema": 23, "formal_schema": 23}
        and release.inventory(original) == tree,
        "complete immutable schema23 parent required",
    )
    external(
        args.source_root,
        ROOT,
        original,
        Path(parent["project_root"]),
        args.overlay_root,
    )
    external(
        args.source_tree, ROOT, original, args.source_root, Path(parent["project_root"])
    )
    release.require(
        not args.source_root.exists() and not args.source_tree.exists(),
        "new source and manifest paths required",
    )
    overlays = {}
    parent_records = release.records(tree)
    for name in sorted(release.ALLOWED_FILES):
        path = args.overlay_root / name
        if path.is_file():
            body = release.raw(path, private=False)
            if hashlib.sha256(body).hexdigest() != parent_records.get(name, {}).get(
                "sha256"
            ):
                overlays[name] = (body, path.stat().st_mode & 0o777)
    release.require(
        release.REQUIRED_CHANGES <= overlays.keys(),
        "overlay omits required taxonomy changes",
    )
    args.source_root.parent.mkdir(parents=True, exist_ok=True)
    subprocess.run(
        [
            "git",
            "clone",
            "--local",
            "--no-hardlinks",
            "--branch",
            tree["git"]["branch"],
            str(original),
            str(args.source_root),
        ],
        check=True,
        capture_output=True,
        env={**os.environ, "GIT_CONFIG_NOSYSTEM": "1", "GIT_CONFIG_GLOBAL": os.devnull},
    )
    tracked = {
        os.fsdecode(name)
        for name in verified_git(args.source_root, "ls-files", "-z").split(b"\0")
        if name
    }
    for name in tracked - parent_records.keys():
        (args.source_root / name).unlink()
    for row in tree["files"]:
        body = release.raw(original / row["path"], private=False)
        release.require(
            hashlib.sha256(body).hexdigest() == row["sha256"],
            "parent changed during freeze",
        )
        target = args.source_root / row["path"]
        target.parent.mkdir(parents=True, exist_ok=True)
        target.write_bytes(body)
        target.chmod(row["mode"])
    for name, (body, mode) in overlays.items():
        target = args.source_root / name
        target.parent.mkdir(parents=True, exist_ok=True)
        target.write_bytes(body)
        target.chmod(mode)
    candidate = release.inventory(args.source_root)
    changes = release.source_changes(tree, candidate)
    release.require(
        release.inventory(original) == tree, "parent source changed during freeze"
    )
    args.source_tree.parent.mkdir(parents=True, exist_ok=True)
    return {
        "status": "frozen",
        "source_tree": write(args.source_tree, candidate),
        "changes": changes,
        "unrelated_checkout_changes_copied": False,
        "services_changed": False,
    }


def candidate(parent_ref, tree_ref):
    parent = release.payload_at(parent_ref, "sealed-build-receipt-v1")
    tree = release.inventory(ROOT)
    release.require(
        release.object_at(tree_ref) == tree, "run from the exact frozen candidate"
    )
    changes = release.source_changes(
        release.object_at(parent["account_cleanup_generation"]["source_tree"]), tree
    )
    return parent, tree, changes


def check(args):
    parent_ref, tree_ref = (
        release.reference(args.parent_build),
        release.reference(args.source_tree),
    )
    parent, tree, changes = candidate(parent_ref, tree_ref)
    review_ref = release.reference(args.review_manifest)
    release.verify_review(review_ref, parent_ref=parent_ref, changes=changes)
    command = release.test_command(args.name, parent)
    external(
        args.output, ROOT, Path(parent["source_root"]), Path(parent["project_root"])
    )
    environment = {k: v for k, v in os.environ.items() if not k.startswith("DCAR_")}
    environment.update(
        PYTHONDONTWRITEBYTECODE="1",
        PYTHONPATH=os.pathsep.join(
            str(ROOT / name) for name in ("src/dcar_eval", "tests", "scripts")
        ),
        DCAR_SCHEDULER_ENABLED="0",
        DCAR_STARTUP_CATCHUP_ENABLED="0",
        DCAR_TEST_DENY_FORMAL_DB="1",
    )
    result = subprocess.run(command, cwd=ROOT, env=environment, capture_output=True)
    args.output.parent.mkdir(parents=True, exist_ok=True)
    output = write_bytes(args.output.with_suffix(".log"), result.stdout + result.stderr)
    release.require(release.inventory(ROOT) == tree, "source changed while testing")
    summary = re.search(rb"Ran (\d+) tests? in ", result.stdout + result.stderr)
    skipped = re.search(rb"skipped=(\d+)", result.stdout + result.stderr)
    test_count, skipped_tests = (
        int(summary[1]) if summary else 0,
        int(skipped[1]) if skipped else 0,
    )
    passed = result.returncode == 0 and test_count > 0 and skipped_tests == 0
    record = {
        "contract": release.CHECK_CONTRACT,
        "name": args.name,
        "source_tree": tree_ref,
        "changes": changes,
        "parent_build": parent_ref,
        "review_manifest": review_ref,
        "test_count": test_count,
        "skipped_tests": skipped_tests,
        "command": command,
        "status": "passed" if passed else "failed",
        "exit_code": result.returncode,
        "output": output,
        "completed_at": now(),
    }
    ref = write(args.output, record)
    release.require(
        passed,
        "check failed or skipped; inspect " + str(args.output.with_suffix(".log")),
    )
    return {"report": ref, "status": "passed"}


def parent_plist_binding(plist, parent_ref, parent, *, database=None, install=None):
    env = plist.get("EnvironmentVariables", {})
    found_database, found_install = (
        Path(env.get("DCAR_V8_DB", "")),
        Path(env.get("DCAR_ACCOUNT_CLEANUP_INSTALL_RECEIPT", "")),
    )
    release.require(
        plist.get("Label") == "cn.tj.dcar.writer-worker"
        and plist.get("WorkingDirectory") == parent["project_root"]
        and env.get("DCAR_PROJECT_ROOT") == parent["project_root"]
        and env.get("DCAR_WRITER_SOURCE_ROOT") == parent["source_root"]
        and release.reference(Path(env.get("DCAR_LOADED_BUILD_RECEIPT", "")))
        == parent_ref
        and plist.get("ProgramArguments")
        == [str(Path(parent["source_root"]) / "deploy/macos/run_writer_worker.sh")]
        and (database is None or database == found_database)
        and (install is None or install == found_install),
        "installed Writer is not the exact requested parent",
    )
    return found_database, found_install


def installed_target(path, *, database, data, payloads):
    installed = load_installed_writer_contract(required=True)
    release.require(
        installed.plist_path == path
        and installed.database == database
        and installed.project_root == data
        and installed.payload in payloads,
        "target is not the actual installed Writer contract",
    )
    return installed


def installed_context(args):
    parent_ref, tree_ref = (
        release.reference(args.parent_build),
        release.reference(args.source_tree),
    )
    parent, tree, changes = candidate(parent_ref, tree_ref)
    body = release.raw(args.installed_plist, private=False)
    plist = plistlib.loads(body)
    database, install = parent_plist_binding(plist, parent_ref, parent)
    installed_target(
        args.installed_plist,
        database=database,
        data=Path(parent["project_root"]),
        payloads=(plist,),
    )
    at = now()
    with flow.readonly_database(database) as connection:
        connection.execute("BEGIN")
        _, inherited = release.parent_context(
            parent_ref,
            install_path=install,
            database=database,
            at=at,
            connection=connection,
        )
        jobs = release.quiescence(connection, at=at)
    return {
        "parent_ref": parent_ref,
        "parent": parent,
        "tree_ref": tree_ref,
        "tree": tree,
        "changes": changes,
        "body": body,
        "plist": plist,
        "database": database,
        "install": install,
        "inherited": inherited,
        "jobs": jobs,
    }


def preflight(args):
    value = installed_context(args)
    access = resolve_installed_database_access(
        DatabaseAccessMode.FORMAL_MUTATION,
        database=value["database"],
        project_root=Path(value["parent"]["project_root"]),
    )
    lock = observe_writer_lock(access)
    checks = checks_at(args.check_report)
    review_ref = release.reference(args.review_manifest)
    release.verify_review(
        review_ref, parent_ref=value["parent_ref"], changes=value["changes"]
    )
    if checks:
        release.verify_checks(
            checks,
            tree_ref=value["tree_ref"],
            changes=value["changes"],
            parent_ref=value["parent_ref"],
            review_ref=review_ref,
        )
    return {
        "status": "verified",
        "mode": "read-only",
        "parent_build": value["parent_ref"],
        "source_tree": value["tree_ref"],
        "changes": value["changes"],
        "review_manifest": review_ref,
        "checks_ready": bool(checks),
        "writer_lock": lock,
        "running_jobs_or_leases": value["jobs"],
        "activation_ready": bool(checks)
        and not lock["held"]
        and not any(value["jobs"].values()),
        "database_writes": 0,
        "services_changed": False,
        "publisher_changed": False,
    }


def next_plist(before, source, child_ref):
    return {
        **before,
        "ProgramArguments": [str(source / "deploy/macos/run_writer_worker.sh")],
        "EnvironmentVariables": {
            **before["EnvironmentVariables"],
            "DCAR_WRITER_SOURCE_ROOT": str(source),
            "DCAR_LOADED_BUILD_RECEIPT": child_ref["path"],
            "PYTHONPATH": os.pathsep.join(
                str(source / name) for name in ("src/dcar_eval", "scripts")
            ),
        },
    }


def bootstrap(plist, child_ref, data):
    with tempfile.TemporaryDirectory() as temporary:
        home = Path(temporary).resolve()
        path = home / "Library/LaunchAgents/cn.tj.dcar.writer-worker.plist"
        path.parent.mkdir(parents=True)
        path.write_bytes(plistlib.dumps(plist))
        path.chmod(0o600)
        return verify_source_before_import(
            data=data, source=ROOT, build_receipt=Path(child_ref["path"]), home=home
        )


def prepare(args):
    value = installed_context(args)
    parent, tree, tree_ref = value["parent"], value["tree"], value["tree_ref"]
    data, database = Path(parent["project_root"]), value["database"]
    checks = checks_at(args.check_report)
    review_ref = release.reference(args.review_manifest)
    release.verify_checks(
        checks,
        tree_ref=tree_ref,
        changes=value["changes"],
        parent_ref=value["parent_ref"],
        review_ref=review_ref,
    )
    external(args.evidence_root, ROOT, data, Path(parent["source_root"]))
    release.require(
        not args.evidence_root.exists() and args.actor.strip() and args.reason.strip(),
        "new evidence and provenance required",
    )
    args.evidence_root.mkdir(mode=0o700, parents=True)
    at, identity = now(), database.stat()
    plan = {
        "contract": release.CONTRACT,
        "scope": release.SCOPE,
        "parent_build": value["parent_ref"],
        "source_tree": tree_ref,
        "changes": value["changes"],
        "checks": checks,
        "review_manifest": review_ref,
        "issued_at": at,
        "actor": args.actor,
        "reason": args.reason,
        "schema_migration_repeated": False,
        "database_writes": 0,
        "paid_gates_reopened": False,
        "publisher_authorized": False,
        "remote_database_authorized": False,
        "parent_inheritance_sha256": release.digest(value["inherited"]),
        "database_identity": {
            "path": str(database),
            "device": identity.st_dev,
            "inode": identity.st_ino,
        },
    }
    source_plan = write(
        args.evidence_root / "source-plan.json",
        {
            "contract": "account-cleanup-source-plan-v1",
            "transition": "account-cleanup-0907-v1",
            "project_root": str(data),
            "source_root": str(ROOT),
            "git": tree["git"],
            "source_tree": tree_ref,
        },
    )
    child = {
        **parent,
        "source_root": str(ROOT),
        "git": tree["git"],
        "critical_files": {
            name: row["sha256"]
            for name, row in release.records(tree).items()
            if name.startswith(("src/", "config/")) and name.endswith((".py", ".json"))
        },
        "code_successor_plan": source_plan,
        "account_cleanup_generation": {
            **parent["account_cleanup_generation"],
            "source_tree": tree_ref,
        },
        release.FIELD: plan,
        "created_at": at,
        "validation_scope": release.SCOPE,
    }
    child_ref = write(
        args.evidence_root / "build.json",
        {
            "contract_version": "sealed-build-receipt-v1",
            "payload": child,
            "payload_sha256": release.digest(child),
        },
    )
    release.verify_inheritance(
        build=child,
        build_ref=child_ref,
        install_path=value["install"],
        database=database,
        source=ROOT,
        at=at,
    )
    proposed = next_plist(value["plist"], ROOT, child_ref)
    proof = bootstrap(proposed, child_ref, data)
    release.require(
        release.raw(args.installed_plist, private=False) == value["body"],
        "installed Writer changed during preparation",
    )
    installed_target(
        args.installed_plist, database=database, data=data, payloads=(value["plist"],)
    )
    proposal = {
        "contract": PROPOSAL,
        "status": "prepared",
        "prepared_at": at,
        "parent_build": value["parent_ref"],
        "parent_install": release.reference(value["install"]),
        "child_build": child_ref,
        "source_tree": tree_ref,
        "source_root": str(ROOT),
        "project_root": str(data),
        "database_identity": plan["database_identity"],
        "review_manifest": review_ref,
        "installed_plist": str(args.installed_plist),
        "before_plist": write_bytes(
            args.evidence_root / "writer.before.plist", value["body"]
        ),
        "next_plist": write_bytes(
            args.evidence_root / "writer.next.plist",
            plistlib.dumps(proposed, sort_keys=True),
        ),
        "bootstrap_verification": proof,
        "database_writes": 0,
        "services_changed": False,
        "publisher_changed": False,
    }
    return {
        "proposal": write(args.evidence_root / "install-proposal.json", proposal),
        **proposal,
    }


def verified_proposal(path):
    proposal = release.object_at(release.reference(path))
    release.require(
        proposal.get("contract") == PROPOSAL
        and proposal.get("status") == "prepared"
        and proposal.get("source_root") == str(ROOT)
        and proposal.get("database_writes") == 0
        and proposal.get("services_changed") is False
        and proposal.get("publisher_changed") is False,
        "verified taxonomy proposal required",
    )
    child = release.payload_at(proposal["child_build"], "sealed-build-receipt-v1")
    release.require(
        child[release.FIELD]["parent_build"] == proposal["parent_build"]
        and child[release.FIELD]["database_identity"] == proposal["database_identity"]
        and child[release.FIELD]["source_tree"] == proposal["source_tree"]
        and child[release.FIELD]["review_manifest"] == proposal["review_manifest"]
        and child["project_root"] == proposal["project_root"],
        "proposal and child differ",
    )
    for key in ("before_plist", "next_plist", "parent_install"):
        release.require(
            release.reference(Path(proposal[key]["path"])) == proposal[key],
            "proposal reference changed",
        )
    before = release.raw(Path(proposal["before_plist"]["path"]), private=False)
    parent = release.payload_at(proposal["parent_build"], "sealed-build-receipt-v1")
    parent_plist_binding(
        plistlib.loads(before),
        proposal["parent_build"],
        parent,
        database=Path(proposal["database_identity"]["path"]),
        install=Path(proposal["parent_install"]["path"]),
    )
    expected = plistlib.dumps(
        next_plist(plistlib.loads(before), ROOT, proposal["child_build"]),
        sort_keys=True,
    )
    release.require(
        release.raw(Path(proposal["next_plist"]["path"])) == expected,
        "proposal expands Writer settings",
    )
    installed_target(
        Path(proposal["installed_plist"]),
        database=Path(proposal["database_identity"]["path"]),
        data=Path(proposal["project_root"]),
        payloads=(plistlib.loads(before), plistlib.loads(expected)),
    )
    return proposal, child, before, expected


def replace_plist(path, *, expected, body):
    release.require(
        release.raw(path, private=False) == expected,
        "installed plist changed before replacement",
    )
    descriptor, temporary = tempfile.mkstemp(
        prefix=".account-taxonomy-", dir=path.parent
    )
    try:
        with os.fdopen(descriptor, "wb") as stream:
            stream.write(body)
            stream.flush()
            os.fsync(stream.fileno())
        os.replace(temporary, path)
        directory = os.open(path.parent, os.O_RDONLY)
        try:
            os.fsync(directory)
        finally:
            os.close(directory)
    finally:
        if os.path.exists(temporary):
            os.unlink(temporary)


def activate(args):
    proposal, child, before, after = verified_proposal(args.proposal)
    data, database, installed = (
        Path(proposal["project_root"]),
        Path(proposal["database_identity"]["path"]),
        Path(proposal["installed_plist"]),
    )
    external(
        args.output_dir,
        ROOT,
        data,
        Path(
            release.payload_at(proposal["parent_build"], "sealed-build-receipt-v1")[
                "source_root"
            ]
        ),
    )
    with (
        hold_formal_mutation(database, project_root=data) as access,
        flow.readonly_database(database) as connection,
    ):
        release.require(
            access.installed.plist_path == installed,
            "maintenance lease is for another installed Writer",
        )
        installed_target(
            installed,
            database=database,
            data=data,
            payloads=(plistlib.loads(before), plistlib.loads(after)),
        )
        connection.execute("BEGIN")
        release.require(
            not any(release.quiescence(connection, at=now()).values()),
            "running jobs or valid leases block activation",
        )
        release.verify_inheritance(
            build=child,
            build_ref=proposal["child_build"],
            install_path=Path(proposal["parent_install"]["path"]),
            database=database,
            source=ROOT,
            at=now(),
            connection=connection,
        )
        current = release.raw(installed, private=False)
        release.require(
            current in (before, after),
            "installed Writer differs from parent and proposed child",
        )
        preflight_path = args.output_dir / "activation-preflight.json"
        if preflight_path.exists():
            pre = release.object_at(release.reference(preflight_path))
            release.require(
                pre.get("proposal") == release.reference(args.proposal),
                "activation recovery proposal differs",
            )
            flow.verify_backup(pre["backup"])
        else:
            release.require(
                current == before and not args.output_dir.exists(),
                "new activation directory and exact parent required",
            )
            args.output_dir.mkdir(mode=0o700, parents=True)
            backup = args.output_dir / "before.sqlite3"
            os.close(os.open(backup, os.O_WRONLY | os.O_CREAT | os.O_EXCL, 0o600))
            with sqlite3.connect(backup) as target:
                connection.backup(target)
                release.require(
                    target.execute("PRAGMA integrity_check").fetchone()[0] == "ok"
                    and target.execute("PRAGMA foreign_key_check").fetchone() is None,
                    "backup failed integrity verification",
                )
            pre = {
                "contract": "account-taxonomy-activation-preflight-v1",
                "proposal": release.reference(args.proposal),
                "backup": flow.file_reference(backup),
                "created_at": now(),
            }
            write(preflight_path, pre)
        proof = bootstrap(plistlib.loads(after), proposal["child_build"], data)
        if current == before:
            replace_plist(installed, expected=before, body=after)
        receipt = {
            "contract": "account-taxonomy-writer-install-v1",
            "status": "installed_stopped",
            "proposal": pre["proposal"],
            "backup": pre["backup"],
            "child_build": proposal["child_build"],
            "bootstrap_verification": proof,
            "database_writes": 0,
            "services_started": False,
            "publisher_changed": False,
        }
        output = args.output_dir / "install.json"
        if output.exists():
            release.require(
                release.object_at(release.reference(output)) == receipt,
                "activation receipt differs",
            )
        else:
            write(output, receipt)
        return receipt


def rollback(args):
    proposal, _, before, after = verified_proposal(args.proposal)
    data, database, installed = (
        Path(proposal["project_root"]),
        Path(proposal["database_identity"]["path"]),
        Path(proposal["installed_plist"]),
    )
    external(
        args.output,
        ROOT,
        data,
        Path(
            release.payload_at(proposal["parent_build"], "sealed-build-receipt-v1")[
                "source_root"
            ]
        ),
    )
    pre = release.object_at(
        release.reference(args.activation_dir / "activation-preflight.json")
    )
    release.require(
        pre.get("proposal") == release.reference(args.proposal),
        "rollback activation proposal differs",
    )
    flow.verify_backup(pre["backup"])
    with (
        hold_formal_mutation(database, project_root=data) as access,
        flow.readonly_database(database) as connection,
    ):
        release.require(
            access.installed.plist_path == installed,
            "maintenance lease is for another installed Writer",
        )
        installed_target(
            installed,
            database=database,
            data=data,
            payloads=(plistlib.loads(before), plistlib.loads(after)),
        )
        connection.execute("BEGIN")
        release.require(
            not any(release.quiescence(connection, at=now()).values()),
            "running jobs or valid leases block rollback",
        )
        release.parent_context(
            proposal["parent_build"],
            install_path=Path(proposal["parent_install"]["path"]),
            database=database,
            at=now(),
            connection=connection,
        )
        current = release.raw(installed, private=False)
        release.require(
            current in (before, after), "Writer is no longer this transition"
        )
        if current == after:
            replace_plist(installed, expected=after, body=before)
    receipt = {
        "contract": "account-taxonomy-writer-rollback-v1",
        "status": "parent_installed_stopped",
        "proposal": pre["proposal"],
        "database_restored": False,
        "database_writes": 0,
        "services_started": False,
        "publisher_changed": False,
    }
    if args.output.exists():
        release.require(
            release.object_at(release.reference(args.output)) == receipt,
            "rollback receipt differs",
        )
    else:
        write(args.output, receipt)
    return receipt


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    commands = parser.add_subparsers(dest="mode", required=True)
    frozen = commands.add_parser("freeze")
    for name in ("parent-build", "overlay-root", "source-root", "source-tree"):
        frozen.add_argument("--" + name, type=Path, required=True)
    checking = commands.add_parser("check")
    for name in ("parent-build", "source-tree", "review-manifest", "output"):
        checking.add_argument("--" + name, type=Path, required=True)
    checking.add_argument("--name", choices=sorted(release.CHECKS), required=True)
    for mode in ("preflight", "prepare"):
        command = commands.add_parser(mode)
        for name in (
            "parent-build",
            "source-tree",
            "review-manifest",
            "installed-plist",
        ):
            command.add_argument("--" + name, type=Path, required=True)
        command.add_argument("--check-report", action="append", default=[])
        if mode == "prepare":
            command.add_argument("--evidence-root", type=Path, required=True)
            command.add_argument("--actor", required=True)
            command.add_argument("--reason", required=True)
    installing = commands.add_parser("activate")
    installing.add_argument("--proposal", type=Path, required=True)
    installing.add_argument("--output-dir", type=Path, required=True)
    restoring = commands.add_parser("rollback")
    for name in ("proposal", "activation-dir", "output"):
        restoring.add_argument("--" + name, type=Path, required=True)
    args = parser.parse_args()
    for value in vars(args).values():
        if isinstance(value, Path):
            release.require(
                value.is_absolute() and value.resolve() == value,
                "absolute nonsymlink arguments required",
            )
    print(json.dumps(globals()[args.mode](args), ensure_ascii=False, sort_keys=True))


if __name__ == "__main__":
    main()
