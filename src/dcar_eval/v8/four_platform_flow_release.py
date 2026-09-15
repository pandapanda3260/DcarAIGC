"""Schema23 successor of the actual installed schema22 Writer.

The immutable schema22 verifier runs only against the sealed pre-migration
backup. Its proofs are preserved verbatim, and schema23 proves their retention
through its separate migration. Neither checks nor inheritance grant paid gates.
"""
from __future__ import annotations

from contextlib import contextmanager
from functools import lru_cache
import hashlib
import importlib
import os
from pathlib import Path
import plistlib
import sqlite3
import stat
import sys
import types
from typing import Any, Mapping

from .account_classification_release import digest, object_at, payload_at, raw, records, reference
from .account_intake_release import inventory
from .account_intake_code_successor import _parent_module

CONTRACT = "four-platform-flow-schema-successor-v1"
FIELD = "four_platform_flow_successor"
CHECK_CONTRACT = "four-platform-flow-release-check-v1"
INSTALL_CONTRACT = "four-platform-flow-install-v1"
CHECKS = frozenset({"flow_schema", "flow_execution", "flow_metrics_reports", "flow_frontend", "flow_release"})
MODULE = "src/dcar_eval/v8/four_platform_flow_release.py"
REQUIRED_SOURCE = frozenset({MODULE, "src/dcar_eval/v8/schema_v23.py",
    "src/dcar_eval/v8/four_platform_flow_authority.py", "src/dcar_eval/v8/metric_source_policy.py",
    "config/source_routing_operation_field_v4.json", "config/report_contract_v8_10.json",
    "scripts/prepare_four_platform_flow_release.py", "scripts/install_four_platform_flow.py"})
CODE_ONLY_REQUIRED_SOURCE = REQUIRED_SOURCE | frozenset({
    "src/dcar_eval/v8/runtime_phase_timing.py", "src/dcar_eval/v8/capture_repair.py",
    "scripts/run_capture_repair.py", "deploy/macos/run_writer_worker.sh",
    "src/dcar_eval/v8/capture_transport_recovery.py", "src/dcar_eval/v8/capture_repair_fixed.py",
    "src/dcar_eval/v8/capture_compensation.py",
    "src/dcar_eval/v8/capture_discovery_recovery.py"})

# A same-schema follow-up may repair execution ordering and metric planning,
# plus one exact additive index; no historical migration payload, route, price,
# budget or payment primitive may be rewritten.
CODE_REPAIR_FILES = frozenset({MODULE,
    "scripts/prepare_four_platform_flow_release.py",
    "docs/four-platform-flow-release.md",
    "docs/four-platform-media-recovery.md",
    "scripts/install_capture_work_index.py",
    "src/dcar_eval/v8/capture_work_index.py",
    "src/dcar_eval/v8/schema_v23.py",
    "tests/test_capture_work_index_install.py",
    "tests/test_v23_capture_work_index.py",
    "src/dcar_eval/v8/capture_runtime.py",
    "src/dcar_eval/v8/capture_metric_cycles.py",
    "src/dcar_eval/v8/account_catalog_capture.py",
    "src/dcar_eval/v8/media_source_refresh.py",
    "src/dcar_eval/v8/capture_commands.py",
    "src/dcar_eval/v8/media_work_queue.py",
    "app/web/app/contents/MediaPendingWork.tsx",
    "app/web/tests/media-pending-work.test.mjs",
    "tests/test_four_platform_flow_release.py",
    "tests/test_v23_capture_plan_reuse.py",
    "tests/test_v23_metric_cycle_context.py",
    "tests/test_v23_capture_queue_fairness.py",
    "tests/test_v23_media_source_refresh.py",
    "tests/test_v8_capture_rolling_scheduler.py",
    "tests/test_v8_media_work_queue.py"})

CODE_ONLY_CONTRACT = "four-platform-flow-code-only-successor-v1"
CODE_ONLY_SCOPE = "bounded_forward_discovery_coverage_recovery"
# Recover gaps only within recorded automatic-capture eligibility periods.
# Keep the 72-hour overlap, cap recovery/rechecks at 30 days, and inherit all
# schema, routes, prices, operation authority and payment primitives unchanged.
CODE_ONLY_REPAIR_FILES = frozenset({MODULE,
    "docs/four-platform-flow-release.md",
    "tests/test_four_platform_flow_release.py",
    "src/dcar_eval/v8/capture_runtime.py",
    "src/dcar_eval/v8/capture_discovery_recovery.py",
    "src/dcar_eval/v8/capture_day_coverage.py",
    "src/dcar_eval/v8/providers.py",
    "tests/test_v23_discovery_recovery.py"})


def _validate_code_chain(build_ref: Mapping[str, Any]) -> None:
    """Reject malformed/cyclic metadata before importing ancestor verifiers.

    This is only a recursion guard. Each ancestor's own immutable verifier still
    proves its source, migration, authorization and (where present) index.
    """
    seen_paths, seen_hashes = set(), set()
    current = build_ref
    for _ in range(16):
        require(isinstance(current, Mapping) and isinstance(current.get("path"), str)
            and isinstance(current.get("sha256"), str), "invalid code predecessor chain")
        require(current["path"] not in seen_paths and current["sha256"] not in seen_hashes,
            "cyclic code predecessor chain")
        seen_paths.add(current["path"]); seen_hashes.add(current["sha256"])
        value = payload_at(current, "sealed-build-receipt-v1")
        plan = value.get(FIELD, {})
        require(value.get("status") == "succeeded"
            and value.get("schema_contract") == {"code_schema":23,"formal_schema":23}
            and isinstance(plan, Mapping) and plan.get("contract") == CONTRACT,
            "invalid schema23 code predecessor chain")
        previous = plan.get("code_predecessor")
        if previous is None:
            return
        require(isinstance(previous, Mapping), "invalid code predecessor chain")
        current = previous.get("build")
    require(False, "code predecessor chain exceeds depth limit")


def inherited_index_reference(parent: Mapping[str, Any], inherited: Mapping[str, Any]) -> dict:
    """Read the index reference only after the predecessor's verifier succeeded."""
    plan = parent[FIELD]
    proof = inherited["four_platform_flow_proof"]
    require(all(proof.get(key) == value for key, value in plan.items()),
        "verified code predecessor plan differs")
    repair = plan.get("code_predecessor")
    require(isinstance(repair, Mapping), "code-only predecessor has no installed index")
    index = repair.get("inherited_index_install") if repair.get("contract") == CODE_ONLY_CONTRACT else repair.get("index_install")
    require(isinstance(index, Mapping), "code-only predecessor has no installed index")
    return dict(index)


def code_parent_context(build_ref: Mapping[str, Any], *, install_path: Path,
                        database: Path, at: str | None = None, connection=None,
                        origin_backup: Mapping[str, Any] | None = None) -> tuple[dict, dict]:
    """Verify the exact schema23 predecessor with its own immutable verifier."""
    _validate_code_chain(build_ref)
    parent = payload_at(build_ref, "sealed-build-receipt-v1")
    plan = parent.get(FIELD, {})
    require(parent.get("schema_contract") == {"code_schema":23,"formal_schema":23}
        and plan.get("contract") == CONTRACT,
        "code repair requires an installed schema23 build")
    require(origin_backup is None or plan.get("code_predecessor") is None,
        "a chained code predecessor must verify the actual live schema")
    source = Path(parent["source_root"])
    tree = object_at(parent["account_cleanup_generation"]["source_tree"])
    require(inventory(source) == tree and parent["critical_files"].get(MODULE)
        == records(tree)[MODULE]["sha256"], "schema23 code predecessor source changed")
    # Keep the isolated module alive so its generation-checked backup hash cache
    # survives repeated live checks; every invocation still rechecks all source bytes.
    name = "_dcar_flow_code_parent_" + build_ref["sha256"]
    if name not in sys.modules:
        package = types.ModuleType(name)
        package.__path__ = [str(source/"src/dcar_eval/v8")]
        package.__package__ = name
        sys.modules[name] = package
    verifier = importlib.import_module(name + ".four_platform_flow_release")
    if origin_backup is None:
        inherited = verifier.verify_inheritance(build=parent, build_ref=build_ref,
            install_path=install_path, database=database, source=source, at=at, connection=connection)
    else:
        backup = verify_backup(origin_backup)
        require(not os.path.samefile(backup, database), "schema23 origin backup is live database")
        # The origin verifies its actual schema23 snapshot. The successor below
        # separately proves the sole additive index on the live schema23 database.
        with readonly_database(backup) as original:
            inherited = verifier.verify_inheritance(build=parent, build_ref=build_ref,
                install_path=install_path, database=database, source=source, at=at, connection=original)
        verify_backup(origin_backup)
    require(inventory(source) == tree and inherited["four_platform_flow_proof"]["loaded_build"] == dict(build_ref),
        "schema23 code predecessor proof changed")
    return parent, inherited


def verify_index_install(index_ref: Mapping[str, Any], *, origin_ref: Mapping[str, Any],
                         source_tree_ref: Mapping[str, Any], checks: Mapping[str, Any],
                         database: Path, connection=None) -> dict:
    """Bind a single additive performance index to a sealed offline install.

    The original migration payload and all table definitions remain unchanged.
    Current business rows may advance after install; their preservation was
    checked under the real maintenance lock and is retained in the receipt.
    """
    from .capture_work_index import INDEX_NAME, INDEX_OBJECT, INDEX_SQL
    from . import schema_v23
    value = object_at(index_ref); identity = database.stat()
    require(value.get("contract") == "capture-work-index-install-v1" and value.get("status") == "installed"
        and value.get("receipt_sha256") == digest({k:v for k,v in value.items() if k != "receipt_sha256"})
        and value.get("formal_database") == str(database)
        and value.get("database_identity") == {"device":identity.st_dev,"inode":identity.st_ino}
        and value.get("code_predecessor_build") == dict(origin_ref)
        and value.get("source_tree") == dict(source_tree_ref) and value.get("checks") == dict(checks)
        and value.get("index_sql") == INDEX_SQL and value.get("index_name") == INDEX_NAME
        and value.get("schema_version") == 23 and value.get("schema_migration_repeated") is False
        and value.get("business_rows_changed") == value.get("provider_calls") == value.get("backfill_jobs") == 0
        and value.get("retained_tables_verified") is True, "performance index install proof differs")
    backup = verify_backup(value["backup"])
    require(not os.path.samefile(backup, database), "performance index backup is live database")
    with readonly_database(backup) as original:
        before = schema_v23.objects(original)
        require(not any(row[1] == INDEX_NAME for row in before)
            and schema_v23.migration_proof(original) == value["original_migration_proof"]
            and digest(before) == value["before_schema_sha256"], "performance index origin differs")
    expected = sorted([*before, tuple(INDEX_OBJECT)], key=lambda row:(row[0],row[1]))
    def verify_live(live):
        require(schema_v23.objects(live) == expected
            and digest(expected) == value["after_schema_sha256"]
            and schema_v23.migration_proof(live) == value["original_migration_proof"],
            "live schema differs from sole approved performance index")
    if connection is None:
        with readonly_database(database) as live: verify_live(live)
    else: verify_live(connection)
    verify_backup(value["backup"])
    return value


def code_repair_changes(parent: Mapping[str, Any], tree: Mapping[str, Any], *, code_only: bool = False) -> dict[str, Any]:
    left, right = records(object_at(parent["account_cleanup_generation"]["source_tree"])), records(tree)
    if code_only:
        require(CODE_ONLY_REQUIRED_SOURCE <= set(right), "code-only source omits required repair execution files")
    changed = {name for name in left.keys() | right.keys() if left.get(name) != right.get(name)}
    allowed = CODE_ONLY_REPAIR_FILES if code_only else CODE_REPAIR_FILES
    require(MODULE in changed and changed <= allowed, "code repair exceeds reviewed execution files")
    require(all(name in right and (name not in left or left[name]["mode"] == right[name]["mode"])
        for name in changed), "code repair deleted files or changed permissions")
    return {name:{"before_sha256":left[name]["sha256"] if name in left else None,
                  "after_sha256":right[name]["sha256"]} for name in sorted(changed)}


def require(condition: bool, reason: str) -> None:
    if not condition:
        raise ValueError("four platform flow release: " + reason)


def file_reference(path: Path) -> dict[str, Any]:
    """Stream a private SQLite backup without a receipt-sized memory limit."""
    before = path.lstat()
    require(path.is_absolute() and path.resolve(strict=True) == path and stat.S_ISREG(before.st_mode)
        and before.st_nlink == 1 and before.st_uid == os.geteuid()
        and stat.S_IMODE(before.st_mode) == 0o600, "unsafe backup file")
    checksum = hashlib.sha256()
    with os.fdopen(os.open(path, os.O_RDONLY | os.O_NOFOLLOW), "rb") as stream:
        for chunk in iter(lambda: stream.read(1024 * 1024), b""):
            checksum.update(chunk)
        opened = os.fstat(stream.fileno())
    key = lambda value: (value.st_dev, value.st_ino, value.st_size, value.st_mtime_ns, value.st_ctime_ns)
    require(key(before) == key(opened) == key(path.lstat()), "backup changed during verification")
    return {"path": str(path), "sha256": checksum.hexdigest(), "byte_size": before.st_size}


def _backup_identity(path: Path) -> tuple:
    value = path.lstat()
    return (value.st_dev, value.st_ino, value.st_size, value.st_mtime_ns, value.st_ctime_ns,
            value.st_mode, value.st_uid, value.st_nlink)


@lru_cache(maxsize=8)
def _verify_backup_generation(path: str, checksum: str, size: int, identity: tuple) -> None:
    candidate = Path(path)
    require(file_reference(candidate) == {"path":path,"sha256":checksum,"byte_size":size}
        and _backup_identity(candidate) == identity, "sealed schema22 backup changed")


def verify_backup(ref: Mapping[str, Any]) -> Path:
    """Hash once per unchanged file generation; every call rechecks identity.

    Backups can be gigabytes. A later permission, inode, size, mtime or ctime
    change invalidates this bounded process-local cache before any proof reuse.
    """
    path = Path(ref["path"]); before = _backup_identity(path)
    _verify_backup_generation(str(path),ref["sha256"],ref["byte_size"],before)
    require(_backup_identity(path) == before, "backup changed during verification")
    return path


@contextmanager
def readonly_database(path: Path):
    connection = sqlite3.connect(path.as_uri() + "?mode=ro", uri=True)
    connection.row_factory = sqlite3.Row
    connection.execute("PRAGMA foreign_keys=ON")
    connection.execute("PRAGMA recursive_triggers=ON")
    try:
        yield connection
    finally:
        connection.close()


def source_changes(parent: Mapping[str, Any], current: Mapping[str, Any]) -> dict[str, Any]:
    left, right = records(parent), records(current)
    require(REQUIRED_SOURCE <= set(right), "source omits required schema23 execution or installation code")
    changes = {}
    for name in sorted(set(left) | set(right)):
        old, new = left.get(name), right.get(name)
        if old == new:
            continue
        require(old is None or new is None or old["mode"] == new["mode"], "source permissions changed")
        changes[name] = {"before_sha256": old["sha256"] if old else None,
                         "after_sha256": new["sha256"] if new else None}
    require(MODULE in changes, "new successor source missing from reviewed delta")
    return changes


def parent_context(parent_ref: Mapping[str, Any], *, install_path: Path, database: Path,
                   at: str | None = None, connection=None) -> tuple[dict, dict]:
    parent = payload_at(parent_ref, "sealed-build-receipt-v1")
    require(parent.get("status") == "succeeded" and FIELD not in parent and
        parent.get("schema_contract") == {"code_schema": 22, "formal_schema": 22}, "parent must be an installed schema22 build")
    source = Path(parent["source_root"])
    tree = object_at(parent["account_cleanup_generation"]["source_tree"])
    require(inventory(source) == tree, "immutable parent source changed")
    entry = "src/dcar_eval/v8/account_intake_release.py"
    require(parent["critical_files"].get(entry) == records(tree)[entry]["sha256"], "parent verifier not source-bound")
    with _parent_module(source, parent_ref["sha256"]) as verifier:
        inherited = verifier.verify_inheritance(build=parent, build_ref=parent_ref,
            install_path=install_path, database=database, source=source, at=at, connection=connection)
    require(inventory(source) == tree and inherited.get("preparation_operation_authority"),
        "parent local operation authority or source changed")
    return parent, inherited


def validate_installed_writer(*, installed_plist: Path, parent_build_ref: Mapping[str, Any],
                             parent_install_ref: Mapping[str, Any], database: Path,
                             project_root: Path, at: str | None = None, connection=None) -> dict:
    body = raw(installed_plist, private=False)
    installed = plistlib.loads(body)
    parent, inherited = parent_context(parent_build_ref, install_path=Path(parent_install_ref["path"]),
        database=database, at=at, connection=connection)
    env = installed.get("EnvironmentVariables", {})
    require(reference(Path(parent_install_ref["path"])) == parent_install_ref and
        installed.get("Label") == "cn.tj.dcar.writer-worker" and installed.get("WorkingDirectory") == str(project_root)
        and env.get("DCAR_PROJECT_ROOT") == str(project_root) == parent["project_root"]
        and env.get("DCAR_V8_DB") == str(database)
        and env.get("DCAR_ACCOUNT_CLEANUP_INSTALL_RECEIPT") == parent_install_ref["path"]
        and env.get("DCAR_WRITER_SOURCE_ROOT") == parent["source_root"]
        and reference(Path(env.get("DCAR_LOADED_BUILD_RECEIPT", ""))) == parent_build_ref
        and installed.get("ProgramArguments") == [str(Path(parent["source_root"])/"deploy/macos/run_writer_worker.sh")],
        "installed Writer is not the exact parent schema22 build")
    return {"bytes": body, "payload": installed, "parent": parent, "inherited": inherited}


def verify_candidate_source(*, source: Path, parent_build_ref: Mapping[str, Any],
                            checks: Mapping[str, Mapping[str, Any]]) -> dict:
    parent = payload_at(parent_build_ref, "sealed-build-receipt-v1")
    require(parent.get("schema_contract") == {"code_schema": 22, "formal_schema": 22} and FIELD not in parent,
        "candidate parent is not schema22")
    data, original = Path(parent["project_root"]), Path(parent["source_root"])
    require(source.resolve(strict=True) == source and source.is_absolute()
        and all(source != other and not source.is_relative_to(other) and not other.is_relative_to(source)
            for other in (data, original)), "candidate source must be independent")
    tree = inventory(source)
    changes = source_changes(object_at(parent["account_cleanup_generation"]["source_tree"]), tree)
    require(set(checks) == CHECKS, "required schema23 focused checks missing")
    refs = []
    for name, ref in checks.items():
        check = object_at(ref)
        require(check.get("contract") == CHECK_CONTRACT and check.get("name") == name
            and check.get("status") == "passed" and check.get("exit_code") == 0
            and check.get("changes") == changes and check.get("command")
            and object_at(check.get("source_tree", {})) == tree
            and reference(Path(check["output"]["path"])) == check["output"], "check differs from final source or output")
        refs.append(check["source_tree"])
    require(all(ref == refs[0] for ref in refs), "checks bind different source manifests")
    return {"parent": parent, "source_tree": tree, "source_tree_ref": refs[0], "changes": changes, "checks": dict(checks)}


def verify_migration(connection, migration: Mapping[str, Any]) -> dict:
    from .schema_v23 import migration_proof
    from .schema_v20 import row_digest
    actual = migration_proof(connection)
    require(actual == migration.get("migration_proof") and actual["source_version"] == 22
        and actual["target_version"] == 23 and actual["provider_calls"] == actual["backfill_jobs"] == 0,
        "installed schema23 migration proof differs")
    for table in ("account_intake_migrations", "account_classification_migrations"):
        expected = actual["retained_tables"][table]
        require(row_digest(connection, table, expected["columns"]) == expected, "historical migration receipt changed")
    require(actual["parent_receipt_sha256"] == migration["parent_migration_proof"]["receipt_sha256"],
        "schema22 parent receipt is not the migrated source")
    return actual


def verify_inheritance(*, build: Mapping[str, Any], build_ref: Mapping[str, Any], install_path: Path,
                       database: Path, source: Path, at: str | None = None, connection=None) -> dict:
    from .runtime_evidence_context import reuse_inheritance
    reused = reuse_inheritance(connection=connection, build=build, build_ref=build_ref,
        install_path=install_path, database=database, source=source, at=at)
    if reused is not None:
        return reused
    if build.get("account_taxonomy_code_successor") is not None:
        from .account_taxonomy_code_successor import verify_inheritance as verify_taxonomy
        return verify_taxonomy(build=build, build_ref=build_ref, install_path=install_path,
            database=database, source=source, at=at, connection=connection)
    from . import four_platform_flow_authority as authority
    from .schema_v22 import migration_proof as parent_migration_proof
    plan = build.get(FIELD, {})
    require(plan.get("contract") == CONTRACT and dict(build) == payload_at(build_ref, "sealed-build-receipt-v1"),
        "loaded schema23 build differs")
    checked = verify_candidate_source(source=source, parent_build_ref=plan["parent_build"], checks=plan["checks"])
    parent = checked["parent"]
    allowed = {"source_root", "git", "critical_files", "code_successor_plan", "account_cleanup_generation",
               FIELD, "schema_contract", "created_at", "validation_scope"}
    require({k:v for k,v in build.items() if k not in allowed} == {k:v for k,v in parent.items() if k not in allowed}
        and {k:v for k,v in build["account_cleanup_generation"].items() if k != "source_tree"}
            == {k:v for k,v in parent["account_cleanup_generation"].items() if k != "source_tree"},
        "original execution controls or parent proofs changed")
    tree_ref, tree = checked["source_tree_ref"], checked["source_tree"]
    require(build["schema_contract"] == {"code_schema": 23, "formal_schema": 23}
        and build["source_root"] == str(source) and build["git"] == tree["git"]
        and build["account_cleanup_generation"]["source_tree"] == plan["source_tree"] == tree_ref
        and plan["changes"] == checked["changes"] and build["critical_files"] == {name: row["sha256"]
            for name, row in records(tree).items() if name.startswith(("src/", "config/")) and name.endswith((".py", ".json"))},
        "schema23 source/schema identity differs")
    require(object_at(build["code_successor_plan"]) == {"contract":"account-cleanup-source-plan-v1",
        "transition":"account-cleanup-0907-v1", "project_root":build["project_root"], "source_root":str(source),
        "git":tree["git"], "source_tree":tree_ref}, "schema23 source plan differs")
    identity = database.stat()
    migration = object_at(plan["migration"])
    migration_tree, migration_checks = tree_ref, plan["checks"]
    repair = plan.get("code_predecessor")
    from .capture_work_index import INDEX_NAME
    def has_index(live):
        return live.execute("SELECT 1 FROM sqlite_master WHERE name=?", (INDEX_NAME,)).fetchone() is not None
    if connection is None:
        with readonly_database(database) as live: index_present = has_index(live)
    else: index_present = has_index(connection)
    require(not index_present or (isinstance(repair, Mapping)
        and (repair.get("index_install") or repair.get("inherited_index_install"))),
        "performance index requires its own sealed installation proof")
    if isinstance(repair, Mapping) and repair.get("contract") == CODE_ONLY_CONTRACT:
        require(index_present and repair.get("index_install") is None,
            "code-only successor requires the inherited installed index")
        origin, origin_inherited = code_parent_context(repair["build"], install_path=install_path,
            database=database, at=at, connection=connection)
        origin_plan = origin[FIELD]
        inherited_index = inherited_index_reference(origin, origin_inherited)
        require(type(repair.get("database_writes")) is int
            and repair.get("schema_migration_repeated") is False
            and repair == {"contract":CODE_ONLY_CONTRACT, "build":repair["build"],
            "proof_sha256":origin_inherited["four_platform_flow_proof"]["proof_sha256"],
            "changes":code_repair_changes(origin, tree, code_only=True), "scope":CODE_ONLY_SCOPE,
            "schema_migration_repeated":False, "database_writes":0,
            "inherited_index_install":inherited_index}
            and all(origin_plan[key] == plan[key] for key in ("parent_build","parent_install","migration"))
            and source != Path(origin["source_root"])
            and authority.parse_time(origin["created_at"]) <= authority.parse_time(plan["issued_at"]),
            "code-only repair changed its verified predecessor, index or migration")
        # The direct predecessor may itself be a code/index successor. Its own
        # verifier has just checked the unchanged original migration receipt;
        # use that proven origin, never the direct predecessor's newer checks.
        verified_migration = object_at(origin_inherited["four_platform_flow_proof"]["migration"])
        require(verified_migration == migration, "verified predecessor migration changed")
        migration_tree, migration_checks = verified_migration["source_tree"], verified_migration["checks"]
    elif repair is not None:
        require(isinstance(repair, Mapping) and repair.get("inherited_index_install") is None,
            "legacy code repair cannot inherit an index")
        index_ref = repair.get("index_install")
        index = verify_index_install(index_ref, origin_ref=repair["build"], source_tree_ref=tree_ref,
            checks=plan["checks"], database=database, connection=connection) if index_ref is not None else None
        origin, origin_inherited = code_parent_context(repair["build"], install_path=install_path,
            database=database, at=at, connection=connection, origin_backup=index["backup"] if index else None)
        origin_plan = origin[FIELD]
        require(origin_plan.get("code_predecessor") is None,
            "chained code repair requires the code-only contract")
        if index is not None:
            require(index["origin_proof_sha256"] == origin_inherited["four_platform_flow_proof"]["proof_sha256"]
                and index["parent_build"] == plan["parent_build"]
                and index["parent_install"] == plan["parent_install"], "performance index parent binding differs")
        require(repair == {"build":repair["build"],
            "proof_sha256":origin_inherited["four_platform_flow_proof"]["proof_sha256"],
            "changes":code_repair_changes(origin, tree), "scope":"queue_fairness_and_metric_plan_reuse",
            "schema_migration_repeated":False, "database_writes":1 if index else 0,
            **({"index_install":index_ref} if index else {})}
            and all(origin_plan[key] == plan[key] for key in ("parent_build","parent_install","migration"))
            and source != Path(origin["source_root"])
            and authority.parse_time(origin["created_at"]) <= authority.parse_time(plan["issued_at"]),
            "code repair changed its immutable schema23 origin or scope")
        migration_tree, migration_checks = origin_plan["source_tree"], origin_plan["checks"]
    require(migration.get("contract") == INSTALL_CONTRACT and migration.get("status") == "migrated"
        and migration.get("from_schema") == 22 and migration.get("to_schema") == 23
        and migration.get("formal_database") == str(database)
        and migration.get("database_identity") == {"device":identity.st_dev, "inode":identity.st_ino}
        and migration.get("authority_build") == plan["parent_build"]
        and migration.get("authority_install") == plan["parent_install"] == reference(install_path)
        and migration.get("source_tree") == migration_tree and migration.get("checks") == migration_checks
        and migration.get("paid_gates_issued") == 0 and migration.get("preserved_tables_verified") is True
        and migration.get("receipt_sha256") == digest({k:v for k,v in migration.items() if k != "receipt_sha256"}),
        "schema23 installation proof differs")
    backup = verify_backup(migration["backup"])
    require(not os.path.samefile(backup, database), "sealed backup is the live database")
    # Old verifiers retain the formal path/inode check while reading only their
    # actual schema22 database snapshot, never a disguised schema23 connection.
    with readonly_database(backup) as original:
        _, inherited = parent_context(plan["parent_build"], install_path=install_path, database=database,
            at=at, connection=original)
        require(parent_migration_proof(original) == migration["parent_migration_proof"]
            and digest(inherited) == migration["parent_inheritance_sha256"], "schema22 backup lineage differs")
    verify_backup(migration["backup"])
    if connection is None:
        with readonly_database(database) as live:
            verify_migration(live, migration)
    else:
        verify_migration(connection, migration)
    approval = object_at(plan["operation_authorization"])
    authority.validate_authorization(approval, parent_build=plan["parent_build"], source_tree=tree_ref,
        migration=plan["migration"], database={"path":str(database), **migration["database_identity"]},
        catalog_policy_sha256=inherited["catalog_capture_policy_sha256"], at=at or plan["issued_at"])
    require(build["created_at"] == plan["issued_at"] and approval["issued_at"] == plan["issued_at"]
        and authority.parse_time(migration["migrated_at"]) <= authority.parse_time(plan["issued_at"]), "schema23 issuance order differs")
    proof = {**plan, "loaded_build":dict(build_ref), "authorization_payload":approval,
        "parent_intake_proof_sha256":inherited["intake_proof"]["proof_sha256"]}
    proof["proof_sha256"] = digest(proof)
    return {**inherited, "four_platform_flow_proof":proof}
