#!/usr/bin/env python3
"""Import a reviewed workbook into the stopped, installed schema22/23 Writer.

Run this command from the exact installed frozen source after migration and
plist switching, before starting the Writer. A separate verified SQLite backup
is mandatory. The default is a transactionally rolled-back dry run. This command
does not start services, change deployment state or contact any provider.
"""
from __future__ import annotations

import argparse
import hashlib
import json
import os
from pathlib import Path
import sqlite3
import sys
from typing import Any

sys.dont_write_bytecode = True

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT / "src/dcar_eval"))
sys.path.insert(0, str(ROOT / "scripts"))

from import_account_summary import (IMPORT_TABLES, authorize_import, backup, counts,
                                    read_workbook, sha256, stamp)
from v8 import account_intake_release as release, runtime_database
from v8.account_summary_import import import_account_summary
from v8.storage import configure_connection_safety

CONTRACT = "installed-account-summary-import-v1"


def _paths(args) -> None:
    files = [args.database, args.xlsx, args.backup, args.report] + ([args.metadata] if args.metadata else [])
    for path in [*files, args.project_root, ROOT]:
        if not path.is_absolute() or path.is_symlink() or path.resolve() != path:
            raise ValueError("Use explicit canonical absolute non-symlink paths")
    for index, path in enumerate(files):
        for other in files[index + 1:]:
            if path == other or path.exists() and other.exists() and path.samefile(other):
                raise ValueError("Database, workbook, metadata, backup and report must be distinct files")
    if args.report.exists() or args.backup.exists():
        raise ValueError("Backup and report destinations must be new; refusing overwrite")
    if not args.xlsx.is_file() or args.metadata and not args.metadata.is_file():
        raise ValueError("Workbook and optional metadata must be existing regular files")


def _installed_authority(args, installed, *, connection: sqlite3.Connection, at: str) -> dict[str, Any]:
    environment = installed.payload.get("EnvironmentVariables", {})
    if (installed.project_root != args.project_root or installed.database != args.database
            or environment.get("DCAR_WRITER_SOURCE_ROOT") != str(ROOT)
            or installed.payload.get("ProgramArguments") != [str(ROOT / "deploy/macos/run_writer_worker.sh")]
            or environment.get("DCAR_PROJECT_ROOT") != str(args.project_root)
            or environment.get("DCAR_V8_DB") != str(args.database)):
        raise ValueError("Import source or database differs from the installed Writer")
    version = connection.execute("PRAGMA user_version").fetchone()[0]
    if version not in {22, 23}:
        raise ValueError("Installed account import requires migrated schema22 or schema23")
    build_path = Path(environment.get("DCAR_LOADED_BUILD_RECEIPT", ""))
    install_path = Path(environment.get("DCAR_ACCOUNT_CLEANUP_INSTALL_RECEIPT", ""))
    if not build_path.is_absolute() or not install_path.is_absolute():
        raise ValueError("Installed Writer build and installation receipts are required")
    build_ref = release.reference(build_path)
    build = release.payload_at(build_ref, "sealed-build-receipt-v1")
    if version == 23:
        from v8 import four_platform_flow_release as current_release
        plan = build.get(current_release.FIELD, {})
    else:
        current_release = release
        plan = build.get("account_intake_successor", {})
    if not plan.get("source_tree") or release.object_at(plan["source_tree"]) != release.inventory(ROOT):
        raise ValueError("Installed frozen source differs from its complete checked inventory")
    inherited = current_release.verify_inheritance(build=build, build_ref=build_ref, install_path=install_path,
        database=args.database, source=ROOT, at=at, connection=connection)
    return {"loaded_build": build_ref, "installation": release.reference(install_path),
        "source_tree": plan["source_tree"], "source_root": str(ROOT),
        "intake_proof_sha256": inherited["intake_proof"]["proof_sha256"],
        **({"four_platform_flow_proof_sha256": inherited["four_platform_flow_proof"]["proof_sha256"]} if version == 23 else {}),
        "installed_plist_sha256": hashlib.sha256(installed.plist_path.read_bytes()).hexdigest()}


def _protected(connection: sqlite3.Connection) -> dict[str, Any]:
    """Small account tables only; authorizer protects all unrelated history rows."""
    schema = [tuple(row) for row in connection.execute("SELECT type,name,tbl_name,sql FROM sqlite_master ORDER BY type,name")]
    tables = {table: {row["id"]: dict(row) for row in connection.execute('SELECT * FROM "' + table + '" ORDER BY id')}
              for table in IMPORT_TABLES}
    return {"schema": schema, "counts": counts(connection), "tables": tables,
        "sequence": dict(connection.execute("SELECT name,seq FROM sqlite_sequence ORDER BY name")),
        "schema_version": connection.execute("PRAGMA user_version").fetchone()[0],
        "catalog_revision": dict(connection.execute("SELECT * FROM capture_catalog_revision WHERE id=1").fetchone())
            if connection.execute("SELECT 1 FROM sqlite_master WHERE type='table' AND name='capture_catalog_revision'").fetchone() else None}


def _verify_preserved(before: dict[str, Any], after: dict[str, Any]) -> None:
    if before["schema"] != after["schema"] or before["schema_version"] not in {22, 23} or after["schema_version"] != before["schema_version"]:
        raise ValueError("Import changed the installed schema")
    for name, count in before["counts"].items():
        if name not in IMPORT_TABLES | {"sqlite_sequence"} and after["counts"].get(name) != count:
            raise ValueError("Import changed unrelated table counts: " + name)
    if before["schema_version"] == 23:
        prior, current = before["catalog_revision"], after["catalog_revision"]
        if (prior is None or current is None or current["id"] != prior["id"] or prior["projection_depth"] != 0
                or current["projection_depth"] != 0 or current["revision"] < prior["revision"]):
            raise ValueError("Import changed catalog projection authority")
    mutable = {"accounts": {"phone", "phone_normalized", "operator_name", "updated_at"},
        "account_platform_identities": {"nickname", "real_name_status", "updated_at"}}
    for table, original in before["tables"].items():
        current = after["tables"][table]
        if not original.keys() <= current.keys():
            raise ValueError("Import lost existing primary keys: " + table)
        for identity, row in original.items():
            changed = current[identity]
            if table in mutable and any(changed[key] != value for key, value in row.items() if key not in mutable[table]):
                raise ValueError("Import changed existing account identity, association or history: " + table)
            if table == "account_directory_rows" and (changed["platform"] != row["platform"]
                    or row["account_id"] is not None and changed["account_id"] != row["account_id"]
                    or row["uid"] not in (None, "") and changed["uid"] != row["uid"]):
                raise ValueError("Import retargeted an existing directory account")
            if table == "account_intake_requests" and changed != row:
                raise ValueError("Import changed an existing immutable intake request")
        previous_sequence = before["sequence"].get(table, 0)
        if any(identity <= previous_sequence for identity in current.keys() - original.keys()):
            raise ValueError("Import reused an earlier account primary key: " + table)
    for table in set(before["sequence"]) | set(after["sequence"]):
        left, right = before["sequence"].get(table, 0), after["sequence"].get(table, 0)
        if right < left or table not in IMPORT_TABLES and left != right:
            raise ValueError("Import changed or rewound an unrelated sequence: " + table)


def _write_report(descriptor: int, report: dict[str, Any]) -> None:
    body = (json.dumps(report, ensure_ascii=False, indent=2, default=str) + "\n").encode()
    os.lseek(descriptor, 0, os.SEEK_SET)
    view = memoryview(body)
    while view:
        written = os.write(descriptor, view)
        if written <= 0:
            raise OSError("Report write did not advance")
        view = view[written:]
    os.ftruncate(descriptor, len(body))
    os.fsync(descriptor)


def run_import(args) -> dict[str, Any]:
    """Own one real maintenance lease, backup, import transaction and receipt."""
    _paths(args)
    payload = read_workbook(args.xlsx, args.metadata)
    # The resolver rejects an active Writer lease and a different installed DB.
    # No source candidate is copied into the formal database at any point.
    with runtime_database.hold_formal_mutation(args.database, project_root=args.project_root) as access:
        installed = runtime_database.load_installed_writer_contract(required=True)
        if installed != access.installed:
            raise ValueError("Installed Writer changed while acquiring maintenance lease")
        connection = sqlite3.connect(access.database)
        connection.row_factory = sqlite3.Row
        committed = False
        descriptor = None
        try:
            configure_connection_safety(connection)
            at = stamp()
            authority = _installed_authority(args, installed, connection=connection, at=at)
            receipt = backup(access.database, args.backup)
            args.report.parent.mkdir(parents=True, exist_ok=True, mode=0o700)
            descriptor = os.open(args.report, os.O_RDWR | os.O_CREAT | os.O_EXCL, 0o600)
            connection.execute("BEGIN IMMEDIATE")
            before = _protected(connection)
            if (sha256(args.xlsx) != payload["sha256"] or args.metadata and sha256(args.metadata) != payload["metadata_sha256"]):
                raise ValueError("Workbook or metadata changed after parsing")
            connection.set_authorizer(authorize_import)
            result = import_account_summary(connection, payload, imported_at=at)
            after = _protected(connection)
            _verify_preserved(before, after)
            if [row[0] for row in connection.execute("PRAGMA quick_check")] != ["ok"]:
                raise ValueError("Post-import quick_check failed")
            if connection.execute("PRAGMA foreign_key_check").fetchone() is not None:
                raise ValueError("Post-import foreign key check failed")
            current = access.database.stat()
            if (current.st_dev, current.st_ino) != (access.database_identity.device, access.database_identity.inode):
                raise ValueError("Installed database file identity changed")
            if current.st_mode & 0o777 != access.database_identity.mode or current.st_nlink != access.database_identity.nlink:
                raise ValueError("Installed database file permissions or links changed")
            if runtime_database.load_installed_writer_contract(required=True) != installed or _installed_authority(args, installed, connection=connection, at=stamp()) != authority:
                raise ValueError("Installed Writer authority changed during import")
            report = {**result, "contract": CONTRACT, "status": "validated", "mode": "apply" if args.apply else "dry_run",
                "transaction_committed": False, "input_path": str(args.xlsx), "input_sha256": payload["sha256"],
                "metadata_sha256": payload.get("metadata_sha256"), "database_identity": access.health_identity(),
                "authority": authority, "backup": receipt, "before_counts": before["counts"], "after_counts": after["counts"],
                "schema_unchanged": True, "existing_primary_keys_preserved": True, "existing_bindings_preserved": True,
                "unrelated_history_write_guard": True, "sequence_checks": "ok", "quick_check": "ok", "foreign_key_check": "ok",
                "database_writes": result.get("writes", 0) if args.apply else 0, "network_requests": 0,
                "provider_calls": 0, "services_started": False, "completed_at": stamp()}
            _write_report(descriptor, report)  # A report failure here still rolls back.
            if args.apply:
                connection.commit(); committed = True
                report.update(status="applied", transaction_committed=True)
            else:
                connection.rollback()
                # The small protected row sets also prove sequence/metadata rollback.
                restored = _protected(connection)
                if restored != before:
                    raise ValueError("Dry-run transaction did not restore its complete protected state")
                report.update(status="rolled_back", rollback_verified=True)
            _write_report(descriptor, report)
            return report
        except BaseException as error:
            if not committed:
                connection.rollback()
            if committed:
                raise RuntimeError("Account import committed, but final report failed; retain backup and inspect the reserved report") from error
            raise
        finally:
            if descriptor is not None:
                os.close(descriptor)
            connection.close()


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--database", "--db", dest="database", type=Path, required=True)
    parser.add_argument("--project-root", type=Path, required=True)
    parser.add_argument("--xlsx", type=Path, required=True)
    parser.add_argument("--metadata", type=Path)
    parser.add_argument("--backup", type=Path, required=True)
    parser.add_argument("--report", type=Path, required=True)
    parser.add_argument("--apply", action="store_true")
    result = run_import(parser.parse_args())
    print(json.dumps({key: result[key] for key in ("status", "mode", "counts", "database_writes", "quick_check", "foreign_key_check")}, ensure_ascii=False))


if __name__ == "__main__":
    main()
