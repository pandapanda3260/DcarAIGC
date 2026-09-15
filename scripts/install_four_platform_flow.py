#!/usr/bin/env python3
"""Migrate schema22 in place under its stopped Writer maintenance lease.

install and recover-receipt never change a launch file or issue provider gates.
activate installs only an already verified local Writer proposal and leaves
starting the service to the operator. Remote/publisher installation is excluded.
"""
from __future__ import annotations
import argparse
from datetime import datetime, timezone
import hashlib
import json
import os
from pathlib import Path
import plistlib
import sqlite3
import sys
import tempfile

sys.dont_write_bytecode = True
ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT/"src/dcar_eval"))
sys.path.insert(0, str(ROOT/"scripts"))
from v8 import four_platform_flow_release as release, schema_v22, schema_v23
from v8.runtime_database import hold_formal_mutation
from install_account_classification import connect, write, write_recovered


def now():
    return datetime.now(timezone.utc).isoformat()


def checks_at(values):
    checks = {}
    for value in values:
        name, separator, path = value.partition("=")
        release.require(separator and name not in checks, "checks must be unique name=absolute_path")
        checks[name] = release.reference(Path(path))
    return checks


def verified_inputs(args, *, connection=None):
    parent_ref, install_ref = release.reference(args.parent_build), release.reference(args.parent_install)
    checked = release.verify_candidate_source(source=ROOT, parent_build_ref=parent_ref, checks=checks_at(args.check_report))
    release.require(checked["source_tree_ref"] == release.reference(args.source_tree), "requested source manifest differs")
    installed = release.validate_installed_writer(installed_plist=args.installed_plist, parent_build_ref=parent_ref,
        parent_install_ref=install_ref, database=args.database, project_root=args.project_root, at=now(), connection=connection)
    return {"authority_build":parent_ref, "authority_install":install_ref,
        "source_tree":checked["source_tree_ref"], "changes":checked["changes"], "checks":checked["checks"],
        "previous_loaded_build":parent_ref, "previous_writer_plist_sha256":hashlib.sha256(installed["bytes"]).hexdigest(),
        "parent_inheritance_sha256":release.digest(installed["inherited"])}


def installation_receipt(preflight, proof):
    value = {key:preflight[key] for key in ("formal_database", "database_identity", "authority_build", "authority_install",
        "source_tree", "changes", "checks", "previous_loaded_build", "previous_writer_plist_sha256", "backup",
        "parent_migration_proof", "parent_inheritance_sha256")}
    value.update(contract=release.INSTALL_CONTRACT, status="migrated", from_schema=22, to_schema=23,
        migration_proof=proof, migrated_at=proof["applied_at"], preserved_tables_verified=True, paid_gates_issued=0)
    return {**value, "receipt_sha256":release.digest(value)}


def install(args):
    release.require(not args.output_dir.exists() and not args.output_dir.is_symlink(), "new evidence directory required")
    checked = verified_inputs(args)
    args.output_dir.mkdir(mode=0o700, parents=True)
    backup = args.output_dir/"before.sqlite3"
    with hold_formal_mutation(args.database, project_root=args.project_root) as access:
        release.require(verified_inputs(args) == checked, "parent or checked source changed before maintenance")
        before = access.database.stat()
        with connect(access.database) as connection:
            parent_proof = schema_v22.migration_proof(connection)
            os.close(os.open(backup, os.O_WRONLY | os.O_CREAT | os.O_EXCL, 0o600))
            with sqlite3.connect(backup) as destination:
                connection.backup(destination)
            backup_ref = release.file_reference(backup)
            with connect(backup, read_only=True) as original:
                release.require(schema_v22.migration_proof(original) == parent_proof
                    and original.execute("PRAGMA integrity_check").fetchone()[0] == "ok"
                    and verified_inputs(args, connection=original) == checked, "independent schema22 backup verification failed")
            preflight = {"contract":"four-platform-flow-preflight-v1", "formal_database":str(access.database),
                "project_root":str(args.project_root), "installed_plist":str(args.installed_plist),
                "database_identity":{"device":before.st_dev, "inode":before.st_ino}, "database_mode":before.st_mode,
                "backup":backup_ref, "parent_migration_proof":parent_proof, **checked, "prepared_at":now()}
            write(args.output_dir/"installation-preflight.json", preflight)
            schema_v23.migrate(connection, maintenance=schema_v23.MaintenanceContext(access, backup, backup_ref["sha256"]))
            after = access.database.stat()
            release.require((after.st_dev,after.st_ino,after.st_mode) == (before.st_dev,before.st_ino,before.st_mode)
                and connection.execute("PRAGMA integrity_check").fetchone()[0] == "ok", "migration changed database identity or integrity")
            receipt = installation_receipt(preflight, schema_v23.migration_proof(connection))
            release.verify_migration(connection, receipt)
            connection.execute("PRAGMA wal_checkpoint(TRUNCATE)")
        return write(args.output_dir/"migration-install.json", receipt)


def recover(args):
    preflight = release.object_at(release.reference(args.output_dir/"installation-preflight.json"))
    release.require(preflight.get("contract") == "four-platform-flow-preflight-v1", "original schema23 preflight required")
    database, backup = Path(preflight["formal_database"]), Path(preflight["backup"]["path"])
    release.require(release.file_reference(backup) == preflight["backup"], "recovery backup changed")
    arguments = argparse.Namespace(parent_build=Path(preflight["authority_build"]["path"]),
        parent_install=Path(preflight["authority_install"]["path"]), source_tree=Path(preflight["source_tree"]["path"]),
        check_report=[name+"="+ref["path"] for name,ref in preflight["checks"].items()],
        installed_plist=Path(preflight["installed_plist"]), database=database, project_root=Path(preflight["project_root"]))
    with hold_formal_mutation(database, project_root=arguments.project_root), connect(backup, read_only=True) as original:
        checked = verified_inputs(arguments, connection=original)
        release.require(all(preflight.get(key) == value for key,value in checked.items())
            and schema_v22.migration_proof(original) == preflight["parent_migration_proof"], "recovery parent/source changed")
        identity = database.stat()
        release.require({"device":identity.st_dev,"inode":identity.st_ino} == preflight["database_identity"]
            and identity.st_mode == preflight["database_mode"], "recovery database identity changed")
        with connect(database) as live:
            receipt = installation_receipt(preflight, schema_v23.migration_proof(live))
            release.verify_migration(live, receipt)
        return write_recovered(args.output_dir/"migration-install.json", receipt)


def activate(args):
    proposal = release.object_at(release.reference(args.proposal))
    release.require(proposal.get("contract") == "four-platform-flow-install-proposal-v1"
        and proposal.get("status") == "prepared" and proposal.get("local_capture_authorized") is True
        and proposal.get("publisher_activation_authorized") is False, "verified local schema23 proposal required")
    database, installed_path = Path(proposal["formal_database"]), Path(proposal["installed_plist"])
    child_ref = proposal["child_build"]
    child = release.payload_at(child_ref, "sealed-build-receipt-v1")
    release.require(Path(child["source_root"]) == ROOT, "activate from the exact frozen source")
    next_body = release.raw(Path(proposal["next_plist"]["path"]))
    release.require(release.reference(Path(proposal["next_plist"]["path"])) == proposal["next_plist"], "proposed Writer plist changed")
    with hold_formal_mutation(database, project_root=Path(child["project_root"])):
        installed_body = release.raw(installed_path, private=False)
        release.require(installed_body in (release.raw(Path(proposal["before_plist"]["path"])), next_body)
            and release.reference(Path(proposal["before_plist"]["path"])) == proposal["before_plist"], "installed Writer changed after preparation")
        release.verify_inheritance(build=child, build_ref=child_ref, install_path=Path(proposal["parent_install"]["path"]),
            database=database, source=ROOT, at=now())
        from v8.runtime_paths import verify_source_before_import
        with tempfile.TemporaryDirectory() as temporary:
            home = Path(temporary).resolve(); target = home/"Library/LaunchAgents/cn.tj.dcar.writer-worker.plist"
            target.parent.mkdir(parents=True); target.write_bytes(next_body); target.chmod(0o600)
            bootstrap = verify_source_before_import(data=Path(child["project_root"]), source=ROOT,
                build_receipt=Path(child_ref["path"]), home=home)
        if installed_body != next_body:
            descriptor, temporary = tempfile.mkstemp(prefix=".four-platform-writer-", dir=installed_path.parent)
            try:
                with os.fdopen(descriptor, "wb") as stream:
                    stream.write(next_body); stream.flush(); os.fsync(stream.fileno())
                os.replace(temporary, installed_path)
                directory = os.open(installed_path.parent, os.O_RDONLY)
                try: os.fsync(directory)
                finally: os.close(directory)
            finally:
                if os.path.exists(temporary): os.unlink(temporary)
    return write_recovered(args.output, {"contract":"four-platform-flow-writer-install-v1", "status":"installed_stopped",
        "installed_plist":str(installed_path), "installed_plist_sha256":hashlib.sha256(next_body).hexdigest(),
        "child_build":child_ref, "bootstrap_verification":bootstrap, "completed_at":now(), "services_started":False,
        "provider_calls":0, "publisher_changed":False}, ignore_time=True)


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    sub = parser.add_subparsers(dest="action", required=True)
    installing = sub.add_parser("install")
    for name in ("database", "project-root", "installed-plist", "parent-build", "parent-install", "source-tree", "output-dir"):
        installing.add_argument("--"+name, type=Path, required=True)
    installing.add_argument("--check-report", action="append", default=[])
    sub.add_parser("recover-receipt").add_argument("--output-dir", type=Path, required=True)
    activation = sub.add_parser("activate")
    activation.add_argument("--proposal", type=Path, required=True)
    activation.add_argument("--output", type=Path, required=True)
    args = parser.parse_args()
    for value in vars(args).values():
        if isinstance(value, Path): release.require(value.is_absolute() and value.resolve() == value, "absolute nonsymlink paths required")
    print(json.dumps({"install":install,"recover-receipt":recover,"activate":activate}[args.action](args), ensure_ascii=False))


if __name__ == "__main__":
    main()
