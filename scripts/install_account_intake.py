#!/usr/bin/env python3
"""Explicit schema22 installation under a stopped Writer's maintenance lease.

This command never starts capture, changes an installed plist or grants paid
provider authority. Source/check receipts and a sealed SQLite backup are
required. Only the existing database inode is migrated; failed DDL rolls back.
"""
from __future__ import annotations

import argparse
from datetime import datetime, timezone
import hashlib
import json
import os
from pathlib import Path
import sqlite3
import sys

sys.dont_write_bytecode = True

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT / "src/dcar_eval"))
sys.path.insert(0, str(ROOT / "scripts"))
from v8 import account_intake_release as release, schema_v21, schema_v22
from v8.runtime_database import hold_formal_mutation
from install_account_classification import connect, sha, write, write_recovered


def _now() -> str:
    return datetime.now(timezone.utc).isoformat(timespec="microseconds").replace("+00:00", "Z")


def _verified_inputs(args) -> dict:
    parent_ref, install_ref = release.reference(args.parent_build), release.reference(args.parent_install)
    source_ref = release.reference(args.source_tree)
    checks = {}
    for argument in args.check_report:
        name, separator, filename = argument.partition("=")
        if not separator or name in checks:
            raise ValueError("Checks must be unique name=path values")
        checks[name] = release.reference(Path(filename))
    source = release.verify_candidate_source(source=ROOT, parent_build_ref=parent_ref, checks=checks)
    if source["source_tree_ref"] != source_ref:
        raise ValueError("Requested source manifest differs from focused checks")
    installed = release.validate_installed_writer(installed_plist=args.installed_plist, parent_build_ref=parent_ref,
        parent_install_ref=install_ref, database=args.database, project_root=args.project_root, at=_now())
    parent, plist_body = installed["parent"], installed["bytes"]
    return {"authority_build": parent_ref, "authority_install": install_ref,
        "source_tree": source_ref, "changes": source["changes"], "checks": checks,
        "previous_loaded_build": parent_ref, "previous_writer_plist_sha256": hashlib.sha256(plist_body).hexdigest(),
        "expected_classification_proof": release.object_at(parent["account_classification_successor"]["migration"])["migration_proof"]}


def install_receipt(preflight: dict, migrated: dict, proof: dict, inherited: dict) -> dict:
    result = {key: preflight[key] for key in ("from_schema", "to_schema", "formal_database", "database_identity",
        "authority_build", "authority_install", "previous_loaded_build", "previous_writer_plist_sha256",
        "source_tree", "checks", "backup")}
    result.update(contract=release.INSTALL_CONTRACT, status="migrated", migration_proof=proof,
        inherited_classification_proof=inherited, preserved_tables_verified=True,
        paid_gates_issued=0, migrated_at=migrated["applied_at"])
    result["receipt_sha256"] = release.digest(result)
    return result


def _same_identity(path: Path, expected: dict) -> bool:
    current = path.stat()
    return {"device": current.st_dev, "inode": current.st_ino} == expected


def _same_restored_state(connection: sqlite3.Connection, original: sqlite3.Connection) -> bool:
    sequence = lambda db: [tuple(row) for row in db.execute("SELECT name,seq FROM sqlite_sequence ORDER BY name")]
    return (schema_v22._objects(connection) == schema_v22._objects(original)
        and schema_v22._table_digests(connection) == schema_v22._table_digests(original)
        and sequence(connection) == sequence(original)
        and connection.execute("PRAGMA user_version").fetchone()[0] == 21
        and connection.execute("PRAGMA integrity_check").fetchone()[0] == "ok"
        and connection.execute("PRAGMA foreign_key_check").fetchone() is None)


def install(args) -> dict:
    if args.output_dir.exists() or args.output_dir.is_symlink():
        raise ValueError("Installation evidence directory already exists")
    checked = _verified_inputs(args)
    args.output_dir.mkdir(mode=0o700, parents=True)
    backup = args.output_dir / "before.sqlite3"
    with hold_formal_mutation(args.database, project_root=args.project_root) as access:
        if _verified_inputs(args) != checked:
            raise ValueError("Checked source or installed Writer changed before maintenance")
        before_stat = access.database.stat()
        identity = {"device": before_stat.st_dev, "inode": before_stat.st_ino}
        with connect(access.database) as connection:
            inherited = schema_v21.migration_proof(connection)
            if inherited != checked["expected_classification_proof"]:
                raise ValueError("Database does not carry the original schema21 installation proof")
            fd = os.open(backup, os.O_WRONLY | os.O_CREAT | os.O_EXCL, 0o600)
            os.close(fd)
            with sqlite3.connect(backup) as destination:
                connection.backup(destination)
            backup_sha = sha(backup)
            with connect(backup, read_only=True) as original:
                if schema_v21.migration_proof(original) != inherited or original.execute("PRAGMA integrity_check").fetchone()[0] != "ok":
                    raise ValueError("Independent schema21 backup verification failed")
            preflight = {"contract": "account-intake-preflight-v1", "from_schema": 21, "to_schema": 22,
                "formal_database": str(access.database), "project_root": str(args.project_root),
                "installed_plist": str(args.installed_plist), "database_identity": identity,
                "database_mode": before_stat.st_mode, "backup": {"path": str(backup), "sha256": backup_sha},
                **checked, "prepared_at": _now()}
            write(args.output_dir / "installation-preflight.json", preflight)
            migrated = schema_v22.migrate(connection, maintenance=schema_v22.MaintenanceContext(access, backup, backup_sha))
            with connect(backup, read_only=True) as original:
                schema_v22.validate_lineage(original, connection)
            if (not _same_identity(access.database, identity) or access.database.stat().st_mode != before_stat.st_mode
                    or connection.execute("PRAGMA integrity_check").fetchone()[0] != "ok"):
                raise ValueError("Post-migration integrity or file identity changed; keep Writer stopped")
            proof = schema_v22.migration_proof(connection)
            after_inherited = release.inherited_classification_proof(connection)
            if inherited != after_inherited:
                raise ValueError("Original schema21 evidence changed; keep Writer stopped")
            connection.execute("PRAGMA wal_checkpoint(TRUNCATE)")
        return write(args.output_dir / "migration-install.json", install_receipt(preflight, migrated, proof, after_inherited))


def recover(args, *, rollback: bool = False) -> dict:
    preflight = release.object_at(release.reference(args.output_dir / "installation-preflight.json"))
    if preflight.get("contract") != "account-intake-preflight-v1":
        raise ValueError("Recovery requires the original schema22 installation preflight")
    database, project = Path(preflight["formal_database"]), Path(preflight["project_root"])
    backup, plist = Path(preflight["backup"]["path"]), Path(preflight["installed_plist"])
    if (backup.is_symlink() or os.path.samefile(backup, database) or sha(backup) != preflight["backup"]["sha256"]
            or hashlib.sha256(release.raw(plist, private=False)).hexdigest() != preflight["previous_writer_plist_sha256"]
            or release.object_at(preflight["source_tree"]) != release.inventory(ROOT)):
        raise ValueError("Recovery source, backup or previous installed Writer changed")
    checked = _verified_inputs(argparse.Namespace(parent_build=Path(preflight["authority_build"]["path"]),
        parent_install=Path(preflight["authority_install"]["path"]), source_tree=Path(preflight["source_tree"]["path"]),
        check_report=[name+"="+ref["path"] for name,ref in preflight["checks"].items()],
        installed_plist=plist, database=database, project_root=project))
    if any(preflight.get(key) != value for key,value in checked.items()):
        raise ValueError("Recovery checked authority or source changed")
    with hold_formal_mutation(database, project_root=project) as access:
        if (not _same_identity(access.database, preflight["database_identity"])
                or access.database.stat().st_mode != preflight["database_mode"]):
            raise ValueError("Recovery database inode or permissions changed")
        with connect(database) as connection, connect(backup, read_only=True) as original:
            if schema_v21.migration_proof(original) != preflight["expected_classification_proof"]:
                raise ValueError("Recovery backup schema21 proof changed")
            rollback_backup = args.output_dir / "before-rollback.sqlite3"
            if rollback and connection.execute("PRAGMA user_version").fetchone()[0] == 21:
                if not _same_restored_state(connection, original):
                    raise ValueError("Restored database differs from verified backup")
                with connect(rollback_backup, read_only=True) as previous:
                    schema_v22.validate_lineage(original, previous)
            else:
                schema_v22.validate_lineage(original, connection)
                inherited = release.inherited_classification_proof(connection)
                if inherited != preflight["expected_classification_proof"]:
                    raise ValueError("Installed schema21 inheritance changed")
                if not rollback:
                    migrated = json.loads(connection.execute("SELECT payload_json FROM account_intake_migrations").fetchone()[0])
                    receipt = install_receipt(preflight, migrated, schema_v22.migration_proof(connection), inherited)
                    return write_recovered(args.output_dir / "migration-install.json", receipt)
                fd = os.open(rollback_backup, os.O_WRONLY | os.O_CREAT | os.O_EXCL, 0o600)
                os.close(fd)
                with sqlite3.connect(rollback_backup) as destination:
                    connection.backup(destination)
                original.backup(connection)
                connection.execute("PRAGMA wal_checkpoint(TRUNCATE)")
                if (not _same_restored_state(connection, original)
                        or not _same_identity(database, preflight["database_identity"])
                        or database.stat().st_mode != preflight["database_mode"]):
                    raise ValueError("Rollback verification failed; keep Writer stopped")
            result = {"contract": "account-intake-rollback-v1", "status": "restored", "formal_database": str(database),
                "database_identity": preflight["database_identity"], "restored_backup": preflight["backup"],
                "schema22_backup": {"path": str(rollback_backup), "sha256": sha(rollback_backup)},
                "schema_version": 21, "paid_gates_issued": 0, "completed_at": _now()}
            return write_recovered(args.output_dir / "rollback.json", result, ignore_time=True)


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    sub = parser.add_subparsers(dest="action", required=True)
    install_parser = sub.add_parser("install")
    for name in ("database", "project-root", "installed-plist", "parent-build", "parent-install", "source-tree"):
        install_parser.add_argument("--" + name, type=lambda value: Path(value).absolute(), required=True)
    install_parser.add_argument("--output-dir", type=lambda value: Path(value).absolute(), required=True)
    install_parser.add_argument("--check-report", action="append", default=[])
    for action in ("recover-receipt", "rollback"):
        child = sub.add_parser(action)
        child.add_argument("--output-dir", type=lambda value: Path(value).absolute(), required=True)
    args = parser.parse_args()
    result = install(args) if args.action == "install" else recover(args, rollback=args.action == "rollback")
    print(json.dumps(result, ensure_ascii=False))


if __name__ == "__main__":
    main()
