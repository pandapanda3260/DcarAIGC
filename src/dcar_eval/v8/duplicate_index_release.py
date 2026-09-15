"""Receipt-bound schema24 successor; original authority is verified on schema23.

The historical verifier always reads a real frozen schema23 backup. No version
PRAGMA or database facade is disguised. This release adds dedupe state and short
write transactions; it does not issue provider gates or historical capture work.
"""
from __future__ import annotations

from contextlib import contextmanager
from datetime import datetime
import json
import os
from pathlib import Path
import sqlite3
import shutil
import importlib
import sys
import types
from typing import Any, Mapping

from .account_classification_release import digest, object_at, payload_at, records, reference
from .account_intake_release import inventory
from .pure_source_normalization import normalization_request
from .four_platform_flow_release import code_parent_context, file_reference, readonly_database

CONTRACT = "duplicate-index-schema-successor-v1"
FIELD = "duplicate_index_successor"
CODE_FIELD = "duplicate_index_code_successor"
CODE_CONTRACT = "duplicate-index-code-successor-v1"
CHECK_CONTRACT = "duplicate-index-release-check-v1"
INSTALL_CONTRACT = "duplicate-index-install-v1"
CHECKS = frozenset({"schema_index", "graph_recovery", "integration", "locks", "performance", "release"})
MODULE = "src/dcar_eval/v8/duplicate_index_release.py"
REQUIRED_SOURCE = frozenset({MODULE, *(
    "src/dcar_eval/v8/" + name + ".py" for name in (
        "schema_v24", "duplicate_index", "duplicate_graph", "duplicate_runtime",
        "duplicate_readiness", "duplicate_index_build", "snapshot_schema_successor",
        "runtime_evidence_context", "runtime_budget_projection", "pure_source_normalization", "pure_source_inventory",
        "runtime_proof_workers")),
    "scripts/prepare_duplicate_index_release.py", "scripts/install_duplicate_index_release.py"})


def require(condition: bool, message: str) -> None:
    if not condition:
        raise ValueError("duplicate index release: " + message)


def require_capacity(path: Path, *, required_bytes: int = 0) -> dict:
    usage = shutil.disk_usage(path)
    require(usage.free >= max(10 * 1024 ** 3, required_bytes) and usage.used / usage.total < 0.9,
            'insufficient disk capacity for recoverable migration')
    return {'free_bytes': usage.free, 'used_fraction': usage.used / usage.total, 'required_bytes': required_bytes}


def source_changes(parent_tree, tree, *, schema_transition=True):
    left, right = records(parent_tree), records(tree)
    require(REQUIRED_SOURCE <= right.keys(), "required implementation or release file missing")
    result = {}
    for name in sorted(left.keys() | right.keys()):
        before, after = left.get(name), right.get(name)
        if before == after:
            continue
        require(before is None or after is None or before["mode"] == after["mode"],
                "source file permissions changed")
        result[name] = {"before_sha256": before["sha256"] if before else None,
                        "after_sha256": after["sha256"] if after else None}
    require(MODULE in result if schema_transition else bool(result),
            "schema24 successor is missing from the checked change")
    return result


def verify_candidate_source(*, source: Path, parent_build_ref, checks):
    parent = payload_at(parent_build_ref, "sealed-build-receipt-v1")
    schema = parent.get("schema_contract")
    code_only = schema == {"code_schema": 24, "formal_schema": 24}
    require((schema == {"code_schema": 23, "formal_schema": 23} or
             code_only and parent.get(FIELD, {}).get("contract") == CONTRACT)
            and parent.get("status") == "succeeded", "parent must be an installed schema23 or schema24 build")
    original, data = Path(parent['source_root']), Path(parent['project_root'])
    require(source.is_absolute() and source.resolve(strict=True) == source
            and all(source != path and not source.is_relative_to(path) and not path.is_relative_to(source)
                    for path in (original, data)), "independent source required")
    tree = inventory(source)
    changes = source_changes(object_at(parent["account_cleanup_generation"]["source_tree"]), tree,
                             schema_transition=not code_only)
    require(set(checks) == CHECKS, "all six final-source checks are required")
    references = []
    for name, ref in checks.items():
        check = object_at(ref)
        require(check.get("contract") == CHECK_CONTRACT and check.get("name") == name
                and check.get("status") == "passed" and check.get("exit_code") == 0
                and check.get("changes") == changes and check.get("command")
                and object_at(check.get("source_tree", {})) == tree
                and reference(Path(check["output"]["path"])) == check["output"],
                "check is missing, failed, or belongs to a different final source")
        references.append(check["source_tree"])
    require(all(ref == references[0] for ref in references), "check manifests differ")
    return {"parent": parent, "source_tree": tree, "source_tree_ref": references[0],
            "changes": changes, "checks": dict(checks)}


def verified_backup(ref):
    """Full check at each preparation; no newly introduced TTL/generation cache."""
    path = Path(ref["path"])
    require(file_reference(path) == dict(ref), "frozen schema23 backup changed")
    require(not Path(str(path) + "-wal").exists() and not Path(str(path) + "-journal").exists(),
            "frozen backup has mutable SQLite sidecars")
    return path


@contextmanager
def readonly_frozen(ref):
    """Immutable is restricted to a sealed, inactive file with no WAL/journal."""
    path = verified_backup(ref)
    connection = sqlite3.connect(path.as_uri() + '?mode=ro&immutable=1', uri=True)
    connection.row_factory = sqlite3.Row
    connection.execute('PRAGMA foreign_keys=ON')
    connection.execute('PRAGMA recursive_triggers=ON')
    connection.execute('PRAGMA query_only=ON')
    try:
        yield connection
    finally:
        connection.close()
    verified_backup(ref)


def parent_context(parent_ref, *, install_path, database, backup_ref, at=None):
    backup = Path(backup_ref['path'])
    require(not os.path.samefile(backup, database), "parent backup cannot be the live database")
    with readonly_frozen(backup_ref) as original:
        require(original.execute("PRAGMA user_version").fetchone()[0] == 23,
                "parent verifier requires an actual schema23 database")
        result = code_parent_context(parent_ref, install_path=install_path,
                                     database=database, at=at, connection=original)
    # readonly_frozen rechecks identity and digest after closing the verifier.
    return result


def verify_migration(connection, migration):
    from . import schema_v24
    from .schema_v20 import row_digest
    proof = schema_v24.migration_proof(connection)
    require(migration.get("contract") == INSTALL_CONTRACT and migration.get("status") == "migrated"
            and migration.get("from_schema") == 23 and migration.get("to_schema") == 24
            and migration.get("migration_proof") == proof
            and migration.get("paid_gates_issued") == 0 and migration.get("preserved_tables_verified") is True
            and migration.get("receipt_sha256") == digest({k: v for k, v in migration.items() if k != "receipt_sha256"}),
            "schema24 installation proof differs")
    for table in ("account_intake_migrations", "account_classification_migrations", "four_platform_flow_migrations"):
        expected = proof["retained_tables"][table]
        require(row_digest(connection, table, expected["columns"]) == expected,
                "historical immutable migration evidence changed")
    return proof


@normalization_request()
def duplicate_code_parent_context(parent_ref, *, install_path, database, at=None, connection=None):
    """Verify a code predecessor with its frozen verifier and the actual schema24.

    Code releases never replace the schema23 backup or rewrite the original
    migration receipt. Each predecessor's source is checked anew before and
    after invoking its own verifier; only loaded Python modules are reused.
    """
    seen, cursor = set(), parent_ref
    for _ in range(32):
        require(cursor.get('sha256') not in seen, 'cyclic schema24 code lineage')
        seen.add(cursor.get('sha256'))
        item = payload_at(cursor, 'sealed-build-receipt-v1')
        require(item.get('schema_contract') == {'code_schema': 24, 'formal_schema': 24}
                and item.get(FIELD, {}).get('contract') == CONTRACT, 'code predecessor must be schema24')
        if CODE_FIELD not in item:
            break
        require(item[CODE_FIELD].get('contract') == CODE_CONTRACT, 'code predecessor contract differs')
        cursor = item[CODE_FIELD]['parent_build']
    else:
        raise ValueError('duplicate index release: schema24 code lineage is too deep')
    parent = payload_at(parent_ref, 'sealed-build-receipt-v1')
    source = Path(parent['source_root'])
    tree = object_at(parent['account_cleanup_generation']['source_tree'])
    require(inventory(source) == tree and parent['critical_files'].get(MODULE)
            == records(tree)[MODULE]['sha256'], 'schema24 code predecessor source changed')
    name = '_dcar_duplicate_code_parent_' + parent_ref['sha256']
    if name not in sys.modules:
        package = types.ModuleType(name)
        package.__path__ = [str(source / 'src/dcar_eval/v8')]
        package.__package__ = name
        sys.modules[name] = package
    verifier = importlib.import_module(name + '.duplicate_index_release')
    inherited = verifier.verify_inheritance(build=parent, build_ref=parent_ref,
        install_path=install_path, database=database, source=source, at=at, connection=connection)
    require(inventory(source) == tree and inherited['duplicate_index_proof']['loaded_build'] == dict(parent_ref),
            'schema24 code predecessor proof changed')
    return parent, inherited


def _verify_code_successor(*, build, build_ref, install_path, database, source, at, connection):
    plan = build[CODE_FIELD]
    require(plan.get('contract') == CODE_CONTRACT and plan.get('engine') in {'mih', 'full_scan'}
            and dict(build) == payload_at(build_ref, 'sealed-build-receipt-v1'), 'loaded schema24 code build differs')
    checked = verify_candidate_source(source=source, parent_build_ref=plan['parent_build'], checks=plan['checks'])
    parent, tree, tree_ref = checked['parent'], checked['source_tree'], checked['source_tree_ref']
    require(parent.get('schema_contract') == {'code_schema': 24, 'formal_schema': 24}, 'code successor requires schema24')
    allowed = {'source_root', 'git', 'critical_files', 'code_successor_plan', 'account_cleanup_generation',
               CODE_FIELD, 'created_at', 'validation_scope'}
    require({k: v for k, v in build.items() if k not in allowed} == {k: v for k, v in parent.items() if k not in allowed}
            and {k: v for k, v in build['account_cleanup_generation'].items() if k != 'source_tree'}
            == {k: v for k, v in parent['account_cleanup_generation'].items() if k != 'source_tree'},
            'code successor changed migration history or existing authority')
    require(build['source_root'] == str(source) and build['git'] == tree['git']
            and build['account_cleanup_generation']['source_tree'] == plan['source_tree'] == tree_ref
            and plan['changes'] == checked['changes']
            and build['critical_files'] == {name: row['sha256'] for name, row in records(tree).items()
                                           if name.startswith(('src/', 'config/')) and name.endswith(('.py', '.json'))},
            'code successor source differs')
    require(object_at(build['code_successor_plan']) == {'contract': 'account-cleanup-source-plan-v1',
            'transition': 'account-cleanup-0907-v1', 'project_root': build['project_root'],
            'source_root': str(source), 'git': tree['git'], 'source_tree': tree_ref}, 'code source plan differs')
    require(plan.get('schema_migration_repeated') is False and plan.get('database_writes') == 0
            and plan.get('paid_gates_issued') == 0, 'code successor may not migrate data or issue authority')
    _, inherited = duplicate_code_parent_context(plan['parent_build'], install_path=install_path,
                                       database=database, at=at, connection=connection)
    require(plan.get('parent_proof_sha256') == digest(inherited), 'code predecessor authority proof differs')
    require(build['created_at'] == plan['issued_at']
            and datetime.fromisoformat(parent['created_at'].replace('Z', '+00:00'))
            <= datetime.fromisoformat(plan['issued_at'].replace('Z', '+00:00')), 'code issuance order differs')
    code_proof = {**plan, 'loaded_build': dict(build_ref)}
    code_proof['proof_sha256'] = digest(code_proof)
    proof = {**inherited['duplicate_index_proof'], 'loaded_build': dict(build_ref), 'code_successor': code_proof}
    proof['proof_sha256'] = digest({k: v for k, v in proof.items() if k != 'proof_sha256'})
    return {**inherited, 'duplicate_index_proof': proof, 'duplicate_index_code_proof': code_proof}


@normalization_request()
def verify_inheritance(*, build: Mapping[str, Any], build_ref: Mapping[str, Any], install_path: Path,
                       database: Path, source: Path, at: str | None = None, connection=None) -> dict:
    from .runtime_evidence_context import reuse_inheritance
    reused = reuse_inheritance(connection=connection, build=build, build_ref=build_ref,
                              install_path=install_path, database=database, source=source, at=at)
    if reused is not None:
        return reused
    if CODE_FIELD in build:
        return _verify_code_successor(build=build, build_ref=build_ref, install_path=install_path,
            database=database, source=source, at=at, connection=connection)
    plan = build.get(FIELD, {})
    require(plan.get("contract") == CONTRACT and plan.get("engine") in {"mih", "full_scan"}
            and dict(build) == payload_at(build_ref, "sealed-build-receipt-v1"), "loaded schema24 build differs")
    checked = verify_candidate_source(source=source, parent_build_ref=plan["parent_build"], checks=plan["checks"])
    parent, tree, tree_ref = checked["parent"], checked["source_tree"], checked["source_tree_ref"]
    allowed = {"source_root", "git", "critical_files", "code_successor_plan", "account_cleanup_generation",
               FIELD, "schema_contract", "created_at", "validation_scope"}
    require({k: v for k, v in build.items() if k not in allowed} == {k: v for k, v in parent.items() if k not in allowed}
            and {k: v for k, v in build["account_cleanup_generation"].items() if k != "source_tree"}
            == {k: v for k, v in parent["account_cleanup_generation"].items() if k != "source_tree"},
            "source transition changed existing provider authority or execution controls")
    require(build.get("schema_contract") == {"code_schema": 24, "formal_schema": 24}
            and build["source_root"] == str(source) and build["git"] == tree["git"]
            and build["account_cleanup_generation"]["source_tree"] == plan["source_tree"] == tree_ref
            and plan["changes"] == checked["changes"]
            and build["critical_files"] == {name: row["sha256"] for name, row in records(tree).items()
                                           if name.startswith(("src/", "config/")) and name.endswith((".py", ".json"))},
            "schema24 source or critical inventory differs")
    require(object_at(build["code_successor_plan"]) == {"contract": "account-cleanup-source-plan-v1",
            "transition": "account-cleanup-0907-v1", "project_root": build["project_root"],
            "source_root": str(source), "git": tree["git"], "source_tree": tree_ref}, "source plan differs")
    migration = object_at(plan["migration"])
    identity = database.stat()
    require(migration.get("formal_database") == str(database)
            and migration.get("database_identity") == {"device": identity.st_dev, "inode": identity.st_ino}
            and migration.get("authority_build") == plan["parent_build"]
            and migration.get("authority_install") == plan["parent_install"] == reference(install_path)
            and migration.get("source_tree") == tree_ref and migration.get("checks") == plan["checks"],
            "schema24 migration belongs to another installation or source")
    _, inherited = parent_context(plan["parent_build"], install_path=install_path, database=database,
                                  backup_ref=migration["backup"], at=at)
    require(digest(inherited) == migration["parent_inheritance_sha256"], "schema23 authority proof differs")
    if connection is None:
        with readonly_database(database) as live:
            verify_migration(live, migration)
    else:
        verify_migration(connection, migration)
    require(build["created_at"] == plan["issued_at"]
            and datetime.fromisoformat(migration["migrated_at"].replace("Z", "+00:00"))
            <= datetime.fromisoformat(plan["issued_at"].replace("Z", "+00:00")), "issuance order differs")
    proof = {**plan, "loaded_build": dict(build_ref), "parent_proof_sha256": digest(inherited)}
    proof["proof_sha256"] = digest(proof)
    return {**inherited, "duplicate_index_proof": proof}


def active_engine() -> str:
    """Production mode is part of the sealed loaded receipt, never a bare flag."""
    loaded = os.environ.get("DCAR_LOADED_BUILD_ID")
    if not loaded:
        return "mih"  # Explicit isolated fixtures/candidates have no installed receipt.
    path = Path(os.environ["DCAR_LOADED_BUILD_RECEIPT"])
    ref = reference(path)
    require(loaded == "sha256:" + ref["sha256"], "loaded engine receipt changed")
    build = payload_at(ref, "sealed-build-receipt-v1")
    engine = build.get(CODE_FIELD, build.get(FIELD, {})).get("engine")
    require(build.get("schema_contract") == {"code_schema": 24, "formal_schema": 24}
            and build.get("source_root") == str(Path(__file__).resolve().parents[3])
            and engine in {"mih", "full_scan"}, "engine is not bound to the loaded schema24 source")
    return engine


def mark_traffic_started(connection) -> None:
    """Durably close the schema23 restore path before any Writer work starts.

    This sentinel survives work checkpoints and schema24 full-scan fallback.
    It uses the generation staging namespace reserved for installation state.
    """
    from .storage import transaction, now_utc
    require(not connection.in_transaction, 'traffic marker requires an idle Writer connection')
    require(connection.execute('PRAGMA user_version').fetchone()[0] == 24, 'traffic marker requires schema24')
    row = connection.execute("SELECT generation_id FROM duplicate_index_generations WHERE state='ready'").fetchone()
    require(row is not None, 'traffic cannot start without a ready duplicate generation')
    if connection.execute("SELECT 1 FROM duplicate_work_staging WHERE generation_id=? AND work_content_id=0 "
        "AND record_type='runtime_traffic_started'", (row[0],)).fetchone():
        return
    at = now_utc()
    loaded = os.environ.get('DCAR_LOADED_BUILD_ID', 'isolated-fixture')
    with transaction(connection):
        connection.execute("INSERT OR IGNORE INTO duplicate_work_staging VALUES(?,0,0,0,'runtime_traffic_started',"
            "'runtime_traffic_started',?,NULL,?,'runtime',?)",
            (row[0], digest({'loaded_build': loaded}), json.dumps({'started_at': at, 'loaded_build': loaded}), at))
