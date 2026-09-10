#!/usr/bin/env python3
"""Apply the reviewed schema21 change under the installed Writer lease.

Rehearsal never opens the formal database writable. Installation requires a
sealed independent backup, the exact reviewed source and passing checks, and a
stopped Writer. It preserves the database inode and every unrelated table.
"""
from __future__ import annotations

import argparse
from datetime import datetime, timezone
import hashlib
import json
import os
from pathlib import Path
import plistlib
import shutil
import sqlite3
import sys

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT / "src/dcar_eval"))
from v8 import schema_v21
from v8 import account_classification_release as release
from v8.runtime_database import hold_formal_mutation


def sha(path: Path) -> str:
    with path.open("rb") as stream:
        return hashlib.file_digest(stream, "sha256").hexdigest()


def write(path: Path, value: dict) -> dict:
    body = (json.dumps(value, ensure_ascii=False, sort_keys=True, separators=(",", ":")) + "\n").encode()
    fd = os.open(path, os.O_WRONLY | os.O_CREAT | os.O_EXCL, 0o600)
    with os.fdopen(fd, "wb") as stream:
        stream.write(body)
        stream.flush()
        os.fsync(stream.fileno())
    directory_fd = os.open(path.parent, os.O_RDONLY)
    try:
        os.fsync(directory_fd)
    finally:
        os.close(directory_fd)
    return {"path": str(path), "sha256": hashlib.sha256(body).hexdigest(), "byte_size": len(body)}


def quote(name: str) -> str:
    return '"' + name.replace('"', '""') + '"'


def preserved_state(connection: sqlite3.Connection) -> dict:
    """Hash all row values outside the two approved account metadata changes."""
    result = {}
    for (table,) in connection.execute("SELECT name FROM sqlite_master WHERE type='table' AND name NOT LIKE 'sqlite_%' ORDER BY name").fetchall():
        if table == schema_v21.MIGRATION_TABLE:
            continue
        info = list(connection.execute("PRAGMA table_info(" + quote(table) + ")"))
        columns = [r[1] for r in info]
        if table == "accounts":
            columns = [c for c in columns if c not in {"account_type", "content_direction"}]
        if table == "account_directory_rows":
            columns = [c for c in columns if c not in {"account_group", "business_direction"}]
        primary = [r[1] for r in sorted(info, key=lambda r: r[5]) if r[5]]
        order = ",".join(map(quote, primary)) if primary else "rowid"
        where = " WHERE version<>21" if table == "schema_migrations" else ""
        digest = hashlib.sha256(repr(columns).encode())
        count = 0
        for row in connection.execute("SELECT " + ",".join(map(quote, columns)) + " FROM " + quote(table) + where + " ORDER BY " + order):
            digest.update(repr(tuple(row)).encode())
            digest.update(b"\n")
            count += 1
        result[table] = {"columns": columns, "count": count, "sha256": digest.hexdigest()}
    return result


def connect(path: Path, *, read_only: bool = False) -> sqlite3.Connection:
    connection = sqlite3.connect(path.as_uri() + ("?mode=ro" if read_only else "?mode=rw"), uri=True)
    connection.row_factory = sqlite3.Row
    connection.execute("PRAGMA foreign_keys=ON")
    connection.execute("PRAGMA recursive_triggers=ON")
    if read_only:
        connection.execute("PRAGMA query_only=ON")
    return connection


def rehearse(args) -> dict:
    args.output_dir.mkdir(mode=0o700, parents=True, exist_ok=False)
    candidate = args.output_dir / "candidate.sqlite3"
    shutil.copyfile(args.source, candidate)
    candidate.chmod(0o600)
    with connect(candidate) as connection:
        before = preserved_state(connection)
        migrated = schema_v21.migrate(connection)
        after = preserved_state(connection)
        if before != after:
            raise ValueError("Unrelated data changed during rehearsal")
        if connection.execute("PRAGMA integrity_check").fetchone()[0] != "ok":
            raise ValueError("Rehearsal integrity check failed")
        proof = schema_v21.migration_proof(connection)
    result = {"contract": "account-classification-rehearsal-v1", "status": "passed",
              "source": {"path": str(args.source), "sha256": sha(args.source)},
              "candidate": {"path": str(candidate), "sha256": sha(candidate)},
              "migration": migrated, "migration_proof": proof,
              "preserved_tables": before, "preserved_tables_verified": True,
              "provider_calls": 0, "completed_at": datetime.now(timezone.utc).isoformat()}
    return write(args.output_dir / "rehearsal.json", result)


def install(args) -> dict:
    if args.output_dir.exists():
        raise ValueError("Installation evidence directory already exists")
    args.output_dir.mkdir(mode=0o700, parents=True)
    plist_body = args.installed_plist.read_bytes()
    installed = plistlib.loads(plist_body)
    environment = installed["EnvironmentVariables"]
    if (environment.get("DCAR_V8_DB") != str(args.database)
            or environment.get("DCAR_PROJECT_ROOT") != str(args.project_root)):
        raise ValueError("Installed Writer targets a different database")
    parent_build = release.reference(args.parent_build)
    parent_install = release.reference(args.parent_install)
    if parent_build["sha256"] != release.PARENT_BUILD_SHA256 or parent_install["sha256"] != release.PARENT_INSTALL_SHA256:
        raise ValueError("Original capture authority differs from reviewed release")
    # Running all four check commands before install is mandatory. The source
    # inventory check rejects any edit since those commands actually passed.
    import prepare_account_classification_release as package
    _, _, _, _, inventory = package.verified_inputs(ROOT, args.parent_build, args.parent_install)
    origin = release.payload_at(parent_build, "sealed-build-receipt-v1")
    verified_plist, _ = package.validate_installed_writer(release, args.installed_plist, origin, args.parent_install)
    if verified_plist != plist_body:
        raise ValueError("Installed Writer changed during preflight")
    changes = release.source_changes(release.object_at(origin["account_cleanup_generation"]["source_tree"]), inventory)
    checks = {}
    for value in args.check_report:
        name, sep, filename = value.partition("=")
        if not sep or name in checks:
            raise ValueError("Checks must be unique name=path values")
        reference = release.reference(Path(filename))
        report = release.object_at(reference)
        if (report.get("contract") != "account-classification-check-v1" or report.get("name") != name
                or report.get("status") != "passed" or report.get("exit_code") != 0
                or report.get("changes") != changes
                or release.reference(Path(report["output"]["path"])) != report["output"]):
            raise ValueError("Passing check does not bind exact reviewed source")
        checks[name] = reference
    if set(checks) != release.REQUIRED_CHECKS:
        raise ValueError("Required checks are incomplete")
    backup = args.output_dir / "before.sqlite3"
    with hold_formal_mutation(args.database, project_root=args.project_root) as access:
        if args.installed_plist.read_bytes() != plist_body:
            raise ValueError("Installed Writer changed before maintenance")
        with connect(args.database) as connection:
            if connection.execute("PRAGMA user_version").fetchone()[0] != 20:
                raise ValueError("Formal migration requires exact schema20")
            with sqlite3.connect(backup) as destination:
                connection.backup(destination)
            backup.chmod(0o600)
            backup_sha = sha(backup)
            before = preserved_state(connection)
            preflight = {"contract": "account-classification-preflight-v1", "from_schema": 20, "to_schema": 21,
                "formal_database": str(args.database), "project_root": str(args.project_root),
                "installed_plist": str(args.installed_plist),
                "database_identity": {"device": access.database.stat().st_dev, "inode": access.database.stat().st_ino},
                "authority_build": parent_build, "authority_install": parent_install,
                "previous_loaded_build": release.reference(Path(environment["DCAR_LOADED_BUILD_RECEIPT"])),
                "previous_writer_plist_sha256": hashlib.sha256(plist_body).hexdigest(),
                "backup": {"path": str(backup), "sha256": backup_sha},
                "preserved_tables": before, "checks": checks, "changes": changes,
                "prepared_at": datetime.now(timezone.utc).isoformat()}
            write(args.output_dir / "installation-preflight.json", preflight)
            # The migration itself rolls back before commit if its own exact
            # structure, values or foreign-key invariants fail.
            migrated = schema_v21.migrate(connection, maintenance=schema_v21.MaintenanceContext(access, backup, backup_sha))
            after = preserved_state(connection)
            if before != after:
                write(args.output_dir / "preservation-failure.json", {"before": before, "after": after})
                raise ValueError("Unrelated data changed; keep Writer stopped and use verified recovery")
            if connection.execute("PRAGMA integrity_check").fetchone()[0] != "ok":
                raise ValueError("Post-migration integrity check failed; keep Writer stopped")
            proof = schema_v21.migration_proof(connection)
            connection.execute("PRAGMA wal_checkpoint(TRUNCATE)")
        return write(args.output_dir / "migration-install.json", install_receipt(preflight, migrated, proof))


def install_receipt(preflight: dict, migrated: dict, proof: dict) -> dict:
    result = {key: preflight[key] for key in ("from_schema", "to_schema", "formal_database", "database_identity",
        "authority_build", "authority_install", "previous_loaded_build", "previous_writer_plist_sha256", "backup",
        "preserved_tables", "checks")}
    result.update(contract="account-classification-install-v1", status="migrated", migration=migrated,
        migration_proof=proof, preserved_tables_verified=True, paid_gates_issued=0,
        migrated_at=migrated["applied_at"])
    result["receipt_sha256"] = release.digest(result)
    return result


def write_recovered(path: Path, expected: dict, *, ignore_time: bool = False) -> dict:
    """Preserve an incomplete external write after DB provenance is established."""
    if path.exists():
        ref = release.reference(path)
        try:
            actual = release.object_at(ref)
        except json.JSONDecodeError:
            quarantine = path.with_name(path.name + ".incomplete-" + str(datetime.now(timezone.utc).timestamp()).replace(".", "-"))
            path.rename(quarantine)
        else:
            comparison = lambda value: {k: v for k, v in value.items() if not (ignore_time and k == "completed_at")}
            if comparison(actual) != comparison(expected):
                raise ValueError("Existing recovery receipt differs")
            return ref
    return write(path, expected)


def rollback_receipt(preflight: dict, rollback_backup: Path) -> dict:
    return {"contract": "account-classification-rollback-v1", "status": "restored",
        "database_identity": preflight["database_identity"], "formal_database": preflight["formal_database"],
        "restored_backup": preflight["backup"], "schema21_backup": {"path": str(rollback_backup), "sha256": sha(rollback_backup)},
        "schema_version": 20, "paid_gates_issued": 0, "completed_at": datetime.now(timezone.utc).isoformat()}


def recover(args, *, rollback: bool = False) -> dict:
    """Recover a receipt, or restore the exact preflight state before any later writes."""
    preflight = release.object_at(release.reference(args.output_dir / "installation-preflight.json"))
    if (preflight.get("contract") != "account-classification-preflight-v1"
            or preflight.get("authority_build", {}).get("sha256") != release.PARENT_BUILD_SHA256
            or preflight.get("authority_install", {}).get("sha256") != release.PARENT_INSTALL_SHA256
            or preflight.get("changes") != release.approved_changes()):
        raise ValueError("Recovery does not bind the reviewed installation")
    import prepare_account_classification_release as package
    package.verified_inputs(ROOT, Path(preflight["authority_build"]["path"]), Path(preflight["authority_install"]["path"]))
    database, project = Path(preflight["formal_database"]), Path(preflight["project_root"])
    backup = Path(preflight["backup"]["path"])
    if backup.is_symlink() or os.path.samefile(backup, database) or sha(backup) != preflight["backup"]["sha256"]:
        raise ValueError("Recovery backup changed")
    plist_path = Path(preflight["installed_plist"])
    if hashlib.sha256(plist_path.read_bytes()).hexdigest() != preflight["previous_writer_plist_sha256"]:
        raise ValueError("Restore the verified previous Writer plist while stopped before recovery")
    for name, ref in preflight["checks"].items():
        report = release.object_at(ref)
        if (report.get("status") != "passed" or report.get("exit_code") != 0
                or report.get("name") != name or report.get("changes") != preflight["changes"]
                or release.reference(Path(report["output"]["path"])) != report["output"]):
            raise ValueError("Recovery check evidence changed")
    if set(preflight["checks"]) != release.REQUIRED_CHECKS:
        raise ValueError("Recovery checks incomplete")
    with hold_formal_mutation(database, project_root=project) as access:
        identity = {"device": database.stat().st_dev, "inode": database.stat().st_ino}
        if identity != preflight["database_identity"] or hashlib.sha256(plist_path.read_bytes()).hexdigest() != preflight["previous_writer_plist_sha256"]:
            raise ValueError("Recovery database or installed Writer changed")
        with connect(database) as connection, connect(backup, read_only=True) as original:
            if preserved_state(connection) != preflight["preserved_tables"] or preserved_state(original) != preflight["preserved_tables"]:
                raise ValueError("Later business data exists; recovery must not overwrite it")
            rollback_backup = args.output_dir / "before-rollback.sqlite3"
            if rollback and connection.execute("PRAGMA user_version").fetchone()[0] == 20:
                from v8.schema_v20 import validate_structure
                validate_structure(connection)
                if (schema_v21._structure(connection) != schema_v21._structure(original)
                        or [tuple(row) for row in connection.execute("SELECT id,account_type,content_direction FROM accounts ORDER BY id")]
                            != [tuple(row) for row in original.execute("SELECT id,account_type,content_direction FROM accounts ORDER BY id")]
                        or not rollback_backup.is_file() or rollback_backup.is_symlink()
                        or connection.execute("PRAGMA integrity_check").fetchone()[0] != "ok"):
                    raise ValueError("Previously restored database differs from the original backup")
                with connect(rollback_backup, read_only=True) as previous:
                    schema_v21.validate_lineage(original, previous)
                    if preserved_state(previous) != preflight["preserved_tables"]:
                        raise ValueError("Rollback evidence changed")
                return write_recovered(args.output_dir / "rollback.json", rollback_receipt(preflight, rollback_backup), ignore_time=True)
            schema_v21.validate_lineage(original, connection)
            proof = schema_v21.migration_proof(connection)
            migrated = json.loads(connection.execute("SELECT payload_json FROM account_classification_migrations").fetchone()[0])
            if connection.execute("PRAGMA integrity_check").fetchone()[0] != "ok":
                raise ValueError("Recovery database integrity differs")
            if not rollback:
                expected = install_receipt(preflight, migrated, proof)
                return write_recovered(args.output_dir / "migration-install.json", expected)
            if rollback_backup.exists():
                raise ValueError("Rollback evidence already exists")
            with sqlite3.connect(rollback_backup) as destination:
                connection.backup(destination)
            rollback_backup.chmod(0o600)
            original.backup(connection)
            connection.execute("PRAGMA wal_checkpoint(TRUNCATE)")
            from v8.schema_v20 import validate_structure
            validate_structure(connection)
            if preserved_state(connection) != preflight["preserved_tables"] or database.stat().st_ino != identity["inode"]:
                raise ValueError("Rollback verification failed; keep Writer stopped")
            return write_recovered(args.output_dir / "rollback.json", rollback_receipt(preflight, rollback_backup), ignore_time=True)


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    sub = parser.add_subparsers(dest="action", required=True)
    p = sub.add_parser("rehearse")
    p.add_argument("--source", type=lambda v: Path(v).resolve(strict=True), required=True)
    p.add_argument("--output-dir", type=lambda v: Path(v).absolute(), required=True)
    p = sub.add_parser("install")
    for name in ("database", "project-root", "installed-plist", "parent-build", "parent-install"):
        p.add_argument("--" + name, type=lambda v: Path(v).resolve(strict=True), required=True)
    p.add_argument("--output-dir", type=lambda v: Path(v).absolute(), required=True)
    p.add_argument("--check-report", action="append", default=[])
    for action in ("recover-receipt", "rollback"):
        p = sub.add_parser(action)
        p.add_argument("--output-dir", type=lambda v: Path(v).resolve(strict=True), required=True)
    args = parser.parse_args()
    result = rehearse(args) if args.action == "rehearse" else install(args) if args.action == "install" else recover(args, rollback=args.action == "rollback")
    print(json.dumps(result, ensure_ascii=False))


if __name__ == "__main__":
    main()
