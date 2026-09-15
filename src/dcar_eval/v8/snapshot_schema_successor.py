"""Portable schema23/24 facts, verified against genuine immutable ancestors.

The publisher checks installed files with the original release verifier. The
receiver verifies the preserved ledgers and recomputes this snapshot's facts;
neither a capsule nor a source receipt grants remote installation authority.
"""
from __future__ import annotations

import json
import re
import sqlite3
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Mapping

from .schema_v20 import row_digest
from .schema_v23 import digest

FLOW_SCHEMA_VERSION = 23
FLOW_SCHEMA_MIGRATION = "four-platform-forward-flow-v1"
DUPLICATE_SCHEMA_VERSION = 24
DUPLICATE_SCHEMA_MIGRATION = "duplicate-fingerprint-index-v1"
SOURCE_CONTRACT = "snapshot-source-release-successor-v1"
HASH = re.compile(r"[0-9a-f]{64}\Z")


def require(condition: bool, message: str) -> None:
    if not condition:
        raise ValueError(message)


def _retained(connection, table, expected):
    require(row_digest(connection, table, expected["columns"]) == expected,
            "portable immutable ancestor changed: " + table)


def _ledger(connection, table, version, name, *, embedded=False):
    rows = connection.execute(f"SELECT applied_at,payload_json,receipt_sha256 FROM {table}").fetchall()
    require(len(rows) == 1, "portable migration ledger missing: " + table)
    at, raw, checksum = rows[0]
    value = json.loads(raw)
    body = dict(value)
    supplied = body.pop("sha256", None) if embedded else checksum
    require(supplied == checksum == digest(body), "portable migration ledger digest differs: " + table)
    manifest = connection.execute("SELECT name,applied_at FROM schema_migrations WHERE version=?", (version,)).fetchone()
    require(manifest is not None and tuple(manifest) == (name, at) and value.get("applied_at") == at,
            "portable migration manifest differs: " + table)
    require(value.get("source_version") == version - 1 and value.get("target_version") == version
            and value.get("provider_calls") == 0, "portable migration version/authority differs")
    return value, checksum, at


def migration_chain(connection: sqlite3.Connection) -> dict[str, dict[str, Any]]:
    """Validate live successor structure and explicitly retained ancestor rows."""
    version = connection.execute("PRAGMA user_version").fetchone()[0]
    require(version in {23, 24}, "portable successor requires actual schema23 or schema24")
    result = {}
    if version == 24:
        from .schema_v24 import migration_proof
        successor = migration_proof(connection)
        for table in ("four_platform_flow_migrations", "account_intake_migrations", "account_classification_migrations"):
            _retained(connection, table, successor["retained_tables"][table])
        result["duplicate_index_migration"] = successor
        flow, checksum, _ = _ledger(connection, "four_platform_flow_migrations", 23, FLOW_SCHEMA_MIGRATION)
        require(flow.get("contract") == "four-platform-flow-migration-v1" and successor["parent_receipt_sha256"] == checksum,
                "portable schema24 parent differs")
        flow = {**flow, "receipt_sha256": checksum}
    else:
        from .schema_v23 import migration_proof
        flow = migration_proof(connection)
    result["four_platform_flow_migration"] = flow
    _retained(connection, "account_intake_migrations", flow["retained_tables"]["account_intake_migrations"])
    intake, checksum, at = _ledger(connection, "account_intake_migrations", 22, "unified-account-intake-v1", embedded=True)
    require(intake.get("contract_version") == "account-intake-migration-v1"
            and intake.get("migration_name") == "unified-account-intake-v1"
            and intake.get("capture_state_changed") is False and intake.get("production_acceptance") is False
            and flow["parent_receipt_sha256"] == checksum, "portable intake predecessor differs")
    result["account_intake_migration"] = {"contract_version": "account-intake-migration-proof-v1", "schema_version": 22,
        "schema_migration": "unified-account-intake-v1", "receipt_sha256": checksum, "applied_at": at,
        "source_schema_sha256": intake["source_schema_sha256"], "target_schema_sha256": intake["target_schema_sha256"]}
    _retained(connection, "account_classification_migrations", intake["preserved_tables"]["account_classification_migrations"])
    classification, checksum, at = _ledger(connection, "account_classification_migrations", 21, "account-classification-v1", embedded=True)
    require(classification.get("contract_version") == "account-classification-migration-v1"
            and classification.get("migration_name") == "account-classification-v1"
            and classification.get("capture_state_changed") is False
            and not {"account_type", "content_direction"} & set(classification["after"]["accounts_columns"])
            and {"account_group", "business_direction"} <= set(classification["after"]["directory_columns"])
            and intake["source_schema_sha256"] == classification["after"]["schema_sha256"],
            "portable classification predecessor differs")
    result["account_classification_migration"] = {"contract_version": "account-classification-migration-proof-v1",
        "schema_version": 21, "schema_migration": "account-classification-v1", "receipt_sha256": checksum,
        "applied_at": at, "source_schema_sha256": classification["before"]["schema_sha256"],
        "target_schema_sha256": classification["after"]["schema_sha256"],
        "migrated_directory_rows": len(classification["changed_rows"])}
    return result


def _tables(connection, names):
    return {name: row_digest(connection, name, [row[1] for row in connection.execute(f'PRAGMA table_info("{name}")')])
            for name in names}


def publication_evidence(connection: sqlite3.Connection) -> dict[str, Any]:
    chain = migration_chain(connection)
    flow = chain["four_platform_flow_migration"]
    result = {"four_platform_flow": {"contract_version": "four-platform-flow-publication-v1", "schema_version": 23,
        "schema_migration": FLOW_SCHEMA_MIGRATION, "migration_receipt_sha256": flow["receipt_sha256"],
        "parent_receipt_sha256": flow["parent_receipt_sha256"],
        "tables": _tables(connection, ("account_directory_rows", "account_preparation_owners", "capture_catalog_revision",
            "capture_plan_reuse", "content_link_intakes", "media_source_refresh_proposals"))}}
    if "duplicate_index_migration" in chain:
        from .duplicate_index import active_generation, validate_postings
        from .schema_v24 import RUNTIME_TABLES
        generation = active_generation(connection)
        require(generation is not None, "schema24 snapshot requires a ready duplicate generation")
        posting_check = validate_postings(connection)
        result["duplicate_index"] = {"contract_version": "duplicate-index-publication-v1", "schema_version": 24,
            "schema_migration": DUPLICATE_SCHEMA_MIGRATION,
            "migration_receipt_sha256": chain["duplicate_index_migration"]["receipt_sha256"],
            "parent_receipt_sha256": flow["receipt_sha256"], "generation_id": generation["generation_id"],
            "fingerprint_version": generation["fingerprint_version"], "rule_digest": generation["rule_digest"],
            "index_contract_version": generation["index_contract_version"], "posting_check": posting_check,
            "tables": _tables(connection, RUNTIME_TABLES)}
    return result


def validate_publication_shape(publication: Mapping[str, Any], version: int) -> None:
    expected = {"four_platform_flow": (23, FLOW_SCHEMA_MIGRATION, "four-platform-flow-publication-v1")}
    if version == 24:
        expected["duplicate_index"] = (24, DUPLICATE_SCHEMA_MIGRATION, "duplicate-index-publication-v1")
    for name, (number, migration, contract) in expected.items():
        value = publication.get(name)
        require(isinstance(value, dict) and value.get("schema_version") == number and value.get("schema_migration") == migration
                and value.get("contract_version") == contract and isinstance(value.get("tables"), dict),
                "portable publication section missing or invalid: " + name)
        require(all(HASH.fullmatch(str(value.get(key))) for key in ("migration_receipt_sha256", "parent_receipt_sha256")),
                "portable publication migration digest differs")
        for table in value["tables"].values():
            require(isinstance(table, dict) and set(table) == {"count", "sha256", "columns"}
                    and type(table["count"]) is int and table["count"] >= 0 and HASH.fullmatch(str(table["sha256"]))
                    and isinstance(table["columns"], list), "portable publication table digest invalid")
    if version == 23:
        require(publication.get("duplicate_index") is None, "schema23 cannot publish a duplicate generation")


def verify_publication(connection, proof, source_receipt):
    chain = migration_chain(connection)
    require(all(proof.get(key) == value for key, value in chain.items()), "portable successor migration chain differs")
    require(isinstance(source_receipt, dict), "portable successor requires a sealed snapshot source receipt")
    publication = source_receipt.get("publication_evidence", {})
    actual = publication_evidence(connection)
    require(all(publication.get(key) == value for key, value in actual.items()), "portable successor snapshot rows differ")
    validate_source_release(proof.get("snapshot_source_release"), proof, chain)


def installed_source_release(connection, deployment, chain, *, project_root: Path):
    """Verify the actual installed source and its original frozen predecessor."""
    from .runtime_database import load_installed_writer_contract
    from .account_classification_release import payload_at, reference
    installed = load_installed_writer_contract(required=True)
    require(installed is not None and installed.project_root.resolve() == project_root.resolve(), "snapshot source Writer project differs")
    environment = installed.payload["EnvironmentVariables"]
    build_ref = reference(Path(environment["DCAR_LOADED_BUILD_RECEIPT"]))
    build = payload_at(build_ref, "sealed-build-receipt-v1")
    version = connection.execute("PRAGMA user_version").fetchone()[0]
    require(build.get("schema_contract") == {"code_schema": version, "formal_schema": version}, "snapshot source installed schema differs")
    from . import duplicate_index_release, four_platform_flow_release
    module = duplicate_index_release if version == 24 else four_platform_flow_release
    field = module.FIELD
    plan = build[field]
    original_install = reference(Path(environment["DCAR_ACCOUNT_CLEANUP_INSTALL_RECEIPT"]))
    require(plan["parent_install"] == original_install, "snapshot source original installation differs")
    at = datetime.now(timezone.utc).isoformat()
    inherited = module.verify_inheritance(build=build, build_ref=build_ref, install_path=Path(original_install["path"]),
        database=installed.database, source=Path(build["source_root"]), at=at, connection=connection)
    original_build_sha = inherited["parent_build_ref"]["sha256"]
    require(original_build_sha == deployment["bindings"]["build_sha256"], "snapshot source original cleanup build differs")
    value = {"contract_version": SOURCE_CONTRACT, "schema_version": version,
        "migration_receipt_sha256": chain["duplicate_index_migration" if version == 24 else "four_platform_flow_migration"]["receipt_sha256"],
        "deployment_receipt_sha256": deployment["receipt_sha256"], "original_cleanup_build_sha256": original_build_sha,
        "loaded_build": build_ref, "original_install": original_install, "parent_build": plan["parent_build"],
        "successor_install": plan["migration"], "source_tree": build["account_cleanup_generation"]["source_tree"],
        "inheritance_sha256": digest(inherited), "verification_scope": "frozen_original_parent_and_current_snapshot",
        "remote_installation": "not_verified", "paid_authority": False}
    value["proof_sha256"] = digest(value)
    return value


def validate_source_release(value, deployment, chain):
    require(isinstance(value, dict), "snapshot source release proof is missing")
    require(set(value) == {"contract_version", "schema_version", "migration_receipt_sha256", "deployment_receipt_sha256",
        "original_cleanup_build_sha256", "loaded_build", "original_install", "parent_build", "successor_install",
        "source_tree", "inheritance_sha256", "verification_scope", "remote_installation", "paid_authority", "proof_sha256"},
        "snapshot source release fields differ")
    body = dict(value)
    checksum = body.pop("proof_sha256", None)
    version = 24 if "duplicate_index_migration" in chain else 23
    require(value.get("contract_version") == SOURCE_CONTRACT and value.get("schema_version") == version
            and checksum == digest(body) and value.get("verification_scope") == "frozen_original_parent_and_current_snapshot"
            and value.get("remote_installation") == "not_verified" and value.get("paid_authority") is False
            and value.get("deployment_receipt_sha256") == deployment["receipt_sha256"]
            and value.get("original_cleanup_build_sha256") == deployment["bindings"]["build_sha256"]
            and value.get("migration_receipt_sha256") == chain["duplicate_index_migration" if version == 24 else "four_platform_flow_migration"]["receipt_sha256"],
            "snapshot source release binding differs")
    for name in ("loaded_build", "original_install", "parent_build", "successor_install", "source_tree"):
        ref = value.get(name)
        require(isinstance(ref, dict) and set(ref) == {"path", "sha256", "byte_size"}
                and Path(str(ref.get("path", ""))).is_absolute() and HASH.fullmatch(str(ref.get("sha256")))
                and type(ref.get("byte_size")) is int and 0 < ref["byte_size"] <= 16 * 1024 * 1024,
                "snapshot source release reference invalid")
    require(HASH.fullmatch(str(value.get("inheritance_sha256"))), "snapshot source inheritance digest invalid")
