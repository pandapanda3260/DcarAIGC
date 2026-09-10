"""Read-only, fail-closed schema20 release evidence contracts.

Candidate evidence can seal code but is never production acceptance. No helper
in this module inserts acceptance, enables a provider, or manufactures coverage.
"""
from __future__ import annotations

import hashlib
import json
import re
import sqlite3
import os
import stat
from contextlib import closing
from datetime import datetime
from functools import lru_cache
from pathlib import Path
from typing import Any, Mapping

CONTRACT_VERSION = "v25-bounded-deployment-readiness-v2-local-retention"
LOCAL_STORAGE_SCHEMA = "raw-local-storage-v1"
LOCAL_STORAGE_POLICY = "local-seven-complete-beijing-days-v1"
_STORAGE_IDENTITY_KEYS = (
    "schema", "policy", "root", "device", "inode", "daily_stored_p95",
    "required_free_bytes", "forecast_known",
)
SCHEMA_MIGRATIONS = {19: "dual-acquisition-profile-roster-v1", 20: "integrated-video-capture-v25"}
_HASH = re.compile(r"[0-9a-f]{64}")
_REQUIRED_REFS = ("source_archive", "migration", "rollback", "full_checks", "install")
DEFERRED_ACCEPTANCE = "user_authorized_deferred_e2e"
RELEASE_DECISION_CONTRACT = "v25-user-release-decision-v1"


class ReleaseContractError(ValueError):
    pass


def digest(value: Any) -> str:
    return hashlib.sha256(json.dumps(value, ensure_ascii=False, sort_keys=True,
                                    separators=(",", ":"), allow_nan=False).encode()).hexdigest()


def deployment_digest(*, deployment_id: str, status: str, payload: Mapping[str, Any], recorded_at: str) -> str:
    return digest(dict(deployment_id=deployment_id, status=status, payload=dict(payload), recorded_at=recorded_at))


def _file_version(metadata: os.stat_result) -> tuple[int, ...]:
    # ctime catches a same-size rewrite even when its mtime was restored. This
    # is a file-content cache only; no runtime, database or gate decision is cached.
    return (metadata.st_dev, metadata.st_ino, metadata.st_nlink, metadata.st_mode,
            metadata.st_uid, metadata.st_gid, metadata.st_size,
            metadata.st_mtime_ns, metadata.st_ctime_ns)


@lru_cache(maxsize=256)
def _evidence_file_digest(name: str, version: tuple[int, ...]) -> str:
    path = Path(name)
    flags = os.O_RDONLY | getattr(os, "O_NOFOLLOW", 0) | getattr(os, "O_CLOEXEC", 0)
    descriptor = os.open(path, flags)
    checksum = hashlib.sha256()
    with os.fdopen(descriptor, "rb") as handle:
        if _file_version(os.fstat(handle.fileno())) != version:
            raise ReleaseContractError("release evidence changed before hashing")
        for chunk in iter(lambda: handle.read(1024 * 1024), b""):
            checksum.update(chunk)
        if _file_version(os.fstat(handle.fileno())) != version:
            raise ReleaseContractError("release evidence changed while hashing")
    if path.is_symlink() or path.resolve(strict=True) != path or _file_version(path.lstat()) != version:
        raise ReleaseContractError("release evidence changed after hashing")
    return checksum.hexdigest()


def verified_reference(value: Any, *, project_root: Path | None = None) -> dict[str, Any]:
    if not isinstance(value, dict) or not isinstance(value.get("path"), str) or not _HASH.fullmatch(str(value.get("sha256"))):
        raise ReleaseContractError("evidence requires an exact path and SHA-256")
    path = Path(value["path"])
    from v8.capture_evidence_preflight import observe_file
    observe_file(path)
    if not path.is_absolute() or path.is_symlink() or path.resolve(strict=True) != path:
        raise ReleaseContractError("release evidence path must be canonical")
    if project_root is not None and path.is_relative_to(project_root.resolve()):
        raise ReleaseContractError("release evidence must be project-external")
    metadata = path.lstat()
    if not stat.S_ISREG(metadata.st_mode) or metadata.st_nlink != 1:
        raise ReleaseContractError("release evidence must be a regular single-link file")
    version = _file_version(metadata)
    # Only immutable, owned evidence may use the bounded cache. Preserve the
    # existing validation for other files by hashing them on every access.
    reader = (_evidence_file_digest if metadata.st_uid in {0, os.geteuid()}
              and not metadata.st_mode & 0o022 else _evidence_file_digest.__wrapped__)
    checksum = reader(str(path), version)
    if path.is_symlink() or path.resolve(strict=True) != path or _file_version(path.lstat()) != version:
        raise ReleaseContractError("release evidence changed during verification")
    if checksum != value["sha256"]:
        raise ReleaseContractError("release evidence SHA-256 differs")
    return dict(value)


def _json_reference(reference: Mapping[str, Any], *, allow_schema20_migration: bool = False) -> dict[str, Any]:
    from v8.receipt_sizes import receipt_read_limit, validate_receipt_size
    path = Path(reference["path"])
    size = path.stat().st_size
    limit = receipt_read_limit(allow_schema20_migration=allow_schema20_migration)
    if size > limit:
        raise ReleaseContractError("JSON release receipt exceeds the contract size")
    with path.open("rb") as handle:
        body = handle.read(limit + 1)
    value = json.loads(body)
    try:
        validate_receipt_size(value, len(body), allow_schema20_migration=allow_schema20_migration)
    except ValueError as error:
        raise ReleaseContractError(str(error)) from error
    if not isinstance(value, dict):
        raise ReleaseContractError("JSON release receipt must be an object")
    return value


def validate_storage_policy(capacity: Any, *, require_forecast: bool = False,
                            maintenance_only: bool = False) -> dict[str, Any]:
    """Recheck the actual local disk without reinterpreting old archive receipts.

    Free-space samples are live observations. Only the policy, owned directory,
    and capacity requirement are returned as deterministic release identity.
    """
    from v8.local_raw_retention import inspect_local_storage, validate_local_storage

    if (not isinstance(capacity, dict) or capacity.get("schema") != LOCAL_STORAGE_SCHEMA
            or capacity.get("policy") != LOCAL_STORAGE_POLICY):
        raise ReleaseContractError("deployment requires the explicit local retention storage contract")
    if (not isinstance(capacity.get("root"), str)
            or type(capacity.get("daily_stored_p95")) is not int
            or capacity["daily_stored_p95"] < 0
            or type(capacity.get("forecast_known")) is not bool
            or any(type(capacity.get(key)) is not int or capacity[key] <= 0
                   for key in ("device", "inode", "required_free_bytes"))):
        raise ReleaseContractError("deployment local storage capacity is invalid")
    probe = capacity.get("io_probe")
    if (capacity.get("physical_io_verified") is not True or not isinstance(probe, dict)
            or probe.get("schema") != "raw-local-io-probe-v1"
            or not _HASH.fullmatch(str(probe.get("sha256")))
            or type(probe.get("byte_size")) is not int or probe["byte_size"] != 4096
            or any(probe.get(key) != capacity[key] for key in ("device", "inode"))
            or not isinstance(probe.get("verified_at"), str)):
        raise ReleaseContractError("deployment local storage lacks a qualified I/O probe bound to this root")
    try:
        if datetime.fromisoformat(probe["verified_at"].replace("Z", "+00:00")).tzinfo is None:
            raise ValueError("naive probe timestamp")
    except ValueError as error:
        raise ReleaseContractError("deployment local I/O probe timestamp is invalid") from error
    try:
        inspect = inspect_local_storage if maintenance_only else validate_local_storage
        current = inspect(live_root=Path(capacity["root"]), daily_stored_p95=capacity["daily_stored_p95"])
    except (ValueError, RuntimeError, OSError) as error:
        raise ReleaseContractError(f"deployment local storage is not ready: {error}") from error
    if any(current.get(key) != capacity.get(key) for key in _STORAGE_IDENTITY_KEYS):
        raise ReleaseContractError("deployment local storage identity or capacity requirement changed")
    if require_forecast and current["forecast_known"] is not True:
        raise ReleaseContractError("accepted deployment requires a nonzero local storage capacity forecast")
    return {**{key: current[key] for key in _STORAGE_IDENTITY_KEYS},
            "physical_io_verified": True, "io_probe": dict(probe)}


def legacy_raw_manifest(connection: sqlite3.Connection, *, legacy_project_root: Path,
                        migration_blob_root: Path) -> dict[str, Any]:
    """Bind candidate-owned hardlink copies and original root; never alter raw."""
    roots = {}
    for name, path in (("legacy_project_root", legacy_project_root), ("migration_blob_root", migration_blob_root)):
        if not path.is_absolute() or path.is_symlink() or path.resolve(strict=True) != path or not path.is_dir():
            raise ReleaseContractError("raw migration root must be a canonical directory")
        metadata = path.stat()
        roots[name] = {"path": str(path), "device": metadata.st_dev, "inode": metadata.st_ino}
        if name == "migration_blob_root" and (metadata.st_uid != os.geteuid() or stat.S_IMODE(metadata.st_mode) != 0o700):
            raise ReleaseContractError("migration blobs require a private owned directory")
    # hot_path is not indexed. One ledger scan preserves duplicate detection
    # without a full-table SELECT for every copied raw file.
    ledger: dict[str, list[tuple[Any, Any, Any]]] = {}
    for blob_id, hot_path, checksum, size in connection.execute(
        "SELECT id,hot_path,stored_sha256,stored_size FROM provider_raw_blobs"
    ):
        ledger.setdefault(hot_path, []).append((blob_id, checksum, size))
    files = []
    for path in sorted(migration_blob_root.iterdir()):
        if path.is_symlink() or not path.is_file() or path.stat().st_nlink != 1:
            raise ReleaseContractError("migration blob is not a regular single-link file")
        rows = ledger.get(str(path), [])
        if len(rows) != 1:
            raise ReleaseContractError("migration copy lacks exactly one ledger identity")
        blob_id, checksum, size = rows[0]
        verified_reference({"path": str(path), "sha256": checksum})
        if path.stat().st_size != size:
            raise ReleaseContractError("migration copy byte size differs")
        files.append({"raw_blob_id": blob_id, "path": str(path), "sha256": checksum, "byte_size": size})
    linked, quarantined = connection.execute(
        "SELECT count(raw_blob_id),sum(CASE WHEN raw_blob_id IS NULL THEN 1 ELSE 0 END) FROM provider_raw_responses",
    ).fetchone()
    return {"contract_version": "v25-legacy-raw-migration-v1", **roots, "copies": files,
            "copy_count": len(files), "copy_bytes": sum(item["byte_size"] for item in files),
            "linked_responses": linked, "quarantined_responses": quarantined or 0}


def validate_legacy_raw_files(manifest: Mapping[str, Any]) -> None:
    for key in ("legacy_project_root", "migration_blob_root"):
        entry = manifest[key]
        path = Path(entry["path"])
        if path.is_symlink() or not path.is_dir() or path.resolve(strict=True) != path:
            raise ReleaseContractError("raw migration directory identity changed")
        current = path.stat()
        if (current.st_dev, current.st_ino) != (entry["device"], entry["inode"]):
            raise ReleaseContractError("raw migration directory identity changed")
    root = Path(manifest["migration_blob_root"]["path"])
    expected = {row["path"] for row in manifest["copies"]}
    if {str(path) for path in root.iterdir()} != expected:
        raise ReleaseContractError("raw migration copy inventory changed")
    for row in manifest["copies"]:
        if Path(row["path"]).parent != root:
            raise ReleaseContractError("raw migration copy escapes owned root")
        verified_reference(row)
        if Path(row["path"]).stat().st_size != row["byte_size"]:
            raise ReleaseContractError("raw migration copy size changed")


def validate_migration_candidate(*, source: Path, candidate: Path, receipt: Path,
                                 project_root: Path) -> dict[str, Any]:
    """Pre-install pair validation without a circular receipt inside candidate."""
    from v8.schema_v20 import validate_lineage
    from writer_database_safety import code_identity, sha256_file

    reference = verified_reference({"path": str(receipt), "sha256": sha256_file(receipt)}, project_root=project_root)
    migration = _json_reference(reference, allow_schema20_migration=True)
    if any(migration.get(key) != expected for key, expected in {
        "schema_version": "dcar-v20-offline-migration-v1", "status": "candidate_ready",
        "from_version": 19, "to_version": 20, "from_migration": SCHEMA_MIGRATIONS[19],
        "to_migration": SCHEMA_MIGRATIONS[20],
    }.items()):
        raise ReleaseContractError("pre-install receipt is not an exact 19->20 migration")
    for key, path in (("formal_source", source), ("candidate", candidate)):
        if migration[key]["path"] != str(path.resolve(strict=True)):
            raise ReleaseContractError("migration database path differs")
        verified_reference({"path": str(path), "sha256": migration[key]["file"]["sha256"]})
        if any(Path(str(path) + suffix).exists() for suffix in ("-wal", "-journal")):
            raise ReleaseContractError("sealed migration source/candidate has a transient sidecar")
    if migration["code_identity"] != code_identity(project_root):
        raise ReleaseContractError("migration and sealed executable source differ")
    backup_ref = verified_reference({"path": migration["verified_backup"]["receipt_path"],
                                     "sha256": migration["verified_backup"]["receipt_sha256"]}, project_root=project_root)
    backup = _json_reference(backup_ref)
    verified_reference({"path": backup["backup_path"], "sha256": backup["backup_sha256"]}, project_root=project_root)
    with closing(sqlite3.connect(source.as_uri() + "?mode=ro&immutable=1", uri=True)) as before, closing(sqlite3.connect(
        candidate.as_uri() + "?mode=ro&immutable=1", uri=True,
    )) as after, before, after:
        for connection in (before, after):
            connection.row_factory = sqlite3.Row
            connection.execute("PRAGMA foreign_keys=ON")
            connection.execute("PRAGMA query_only=ON")
        lineage = validate_lineage(before, after)
    if migration["lineage"] != lineage:
        raise ReleaseContractError("pre-install migration lineage changed")
    validate_legacy_raw_files(migration["legacy_raw"])
    return {"contract_version": CONTRACT_VERSION, "status": "candidate", "deployment_eligible": False,
            "coverage_complete": False, "migration_receipt": reference, "rollback": backup_ref,
            "candidate_sha256": migration["candidate"]["file"]["sha256"],
            "unmet_evidence": ["installed_acceptance", "local_storage_capacity", "bounded_e2e"]}


def _validate_evidence_contents(connection: sqlite3.Connection, evidence: Mapping[str, Any], *,
                                project_root: Path | None, accepted: bool, deferred: bool = False) -> None:
    migration = _json_reference(evidence["migration"], allow_schema20_migration=True)
    if any(migration.get(key) != expected for key, expected in {
        "schema_version": "dcar-v20-offline-migration-v1", "status": "candidate_ready",
        "from_version": 19, "to_version": 20, "from_migration": SCHEMA_MIGRATIONS[19],
        "to_migration": SCHEMA_MIGRATIONS[20],
    }.items()) or not migration.get("lineage") or not migration.get("code_identity"):
        raise ReleaseContractError("migration receipt is not a verified exact 19->20 candidate")
    backup = _json_reference(evidence["rollback"])
    if any(backup.get(key) != expected for key, expected in {
        "schema_version": "dcar-v20-offline-backup-v1", "source_schema_version": 19,
        "source_schema_migration": SCHEMA_MIGRATIONS[19], "restore_verified": True,
        "quick_check": "ok", "integrity_check": "ok", "foreign_key_violation_count": 0,
    }.items()):
        raise ReleaseContractError("rollback evidence must be the verified schema19 source backup")
    verified_reference({"path": backup["backup_path"], "sha256": backup["backup_sha256"]}, project_root=project_root)
    # These references are real offline tool receipts, not caller assertions.
    backup_binding = migration["verified_backup"]
    if backup_binding["receipt_sha256"] != evidence["rollback"]["sha256"]:
        raise ReleaseContractError("migration and verified rollback backup differ")
    tests = _json_reference(evidence["full_checks"])
    from seal_r0_receipts import TEST_RESULTS_CONTRACT, _source_archive_record, _validate_envelope, _verify_test_results_payload

    test_payload = _validate_envelope(tests, contract_version=TEST_RESULTS_CONTRACT)
    if project_root is None:
        raise ReleaseContractError("release checks require the exact source project root")
    _verify_test_results_payload(project_root, test_payload)
    _source_archive_record(Path(evidence["source_archive"]["path"]), test_payload["git"])
    # Post-install code updates retain the original migration/install pair;
    # their newly tested source archive need not be the old migration build.
    install = _json_reference(evidence["install"])
    if (install.get("schema_version") != "dcar-writer-database-v20-install-v1" or install.get("status") != "installed"
            or install.get("code_identity") != migration["code_identity"]
            or install["expected"]["migration_receipt_sha256"] != evidence["migration"]["sha256"]):
        raise ReleaseContractError("deployment lacks the exact installed 19->20 migration pair")
    # The explicit user decision defers only business E2E. Every technical
    # source/migration/install/checks/rollback validation above still runs.
    if deferred or (not accepted and "bounded_e2e" not in evidence):
        return
    e2e = _json_reference(evidence["bounded_e2e"])
    if e2e.get("contract_version") != "v25-bounded-end-to-end-v1" or e2e.get("status") != "passed":
        raise ReleaseContractError("bounded end-to-end receipt contract differs")
    attempts = e2e.get("fetch_attempt_ids")
    if not isinstance(attempts, list) or not attempts or any(type(item) is not int or item <= 0 for item in attempts):
        raise ReleaseContractError("bounded end-to-end requires persisted fetch attempts")
    for attempt_id in attempts:
        row = connection.execute(
            "SELECT t.clean_eof,t.json_parse_ok,EXISTS(SELECT 1 FROM provider_raw_responses r "
            "WHERE r.fetch_attempt_id=t.fetch_attempt_id) FROM fetch_transport_receipts t WHERE t.fetch_attempt_id=?",
            (attempt_id,),
        ).fetchone()
        if row is None or tuple(row) != (1, 1, 1):
            raise ReleaseContractError("bounded end-to-end transport/raw evidence is incomplete")
    contents = e2e.get("content_ids")
    if not isinstance(contents, list) or not contents:
        raise ReleaseContractError("bounded end-to-end requires persisted content facts")
    for content_id in contents:
        if type(content_id) is not int or connection.execute(
            "SELECT 1 FROM content_metric_field_facts WHERE content_id=? LIMIT 1", (content_id,),
        ).fetchone() is None:
            raise ReleaseContractError("bounded end-to-end field facts are missing")
    if accepted:
        install = _json_reference(evidence["install"])
        if install.get("schema_version") != "dcar-writer-database-v20-install-v1" or install.get("status") != "installed":
            raise ReleaseContractError("accepted deployment lacks a real schema20 install")
        if install["expected"]["migration_receipt_sha256"] != evidence["migration"]["sha256"]:
            raise ReleaseContractError("accepted deployment install/migration pair differs")


def _validate_release_decision(connection: sqlite3.Connection, *, payload: Mapping[str, Any],
                               reference: Mapping[str, Any], recorded_at: str,
                               project_root: Path | None) -> dict[str, Any]:
    """An auditable release authorization, never a fabricated qualification."""
    from seal_r0_receipts import (
        RUNTIME_ROOT_CONTRACT, SEALED_BUILD_CONTRACT, TEST_RESULTS_CONTRACT,
        _read_private_json, _read_receipt, _schema_contract, _verify_test_results_payload,
    )
    from v8.capture_release import CONTINUITY_OPERATIONS
    from v8.source_routing import parse_time

    decision = _read_private_json(Path(reference["path"]))
    expected = {"contract_version": RELEASE_DECISION_CONTRACT, "business_e2e": "deferred_by_user",
                "production_rollout": "approved_by_user", "transport_qualification": "not_verified",
                "approved_target_profile": "integrated_route_v1"}
    if any(decision.get(key) != value for key, value in expected.items()):
        raise ReleaseContractError("release decision must explicitly defer E2E without claiming qualification")
    if set(decision) != {*expected, "actor", "reason", "operations", "issued_at", "candidate_id",
                         "candidate_receipt_sha256", "bindings", "runtime_bindings", "transport_manifest", "runtime_evidence"}:
        raise ReleaseContractError("release decision fields differ")
    for key, maximum in (("actor", 128), ("reason", 2000)):
        if not isinstance(decision[key], str) or not decision[key].strip() or len(decision[key]) > maximum:
            raise ReleaseContractError("release decision requires its actual actor and reason")
    operations = decision["operations"]
    if (not isinstance(operations, list) or not operations or any(not isinstance(op, str) for op in operations)
            or operations != sorted(set(operations)) or not set(operations).issubset(CONTINUITY_OPERATIONS)):
        raise ReleaseContractError("release decision operations are invalid")
    cursor = connection.execute("SELECT status,payload_json,recorded_at,receipt_sha256 FROM deployment_readiness_receipts WHERE deployment_id=?",
                                (decision["candidate_id"],))
    row = cursor.fetchone()
    if row is None or row[0] != "candidate" or decision["candidate_id"] != payload.get("candidate_id"):
        raise ReleaseContractError("release decision candidate differs")
    candidate = json.loads(row[1])
    if (row[3] != decision["candidate_receipt_sha256"] or row[3] != deployment_digest(
            deployment_id=decision["candidate_id"], status="candidate", payload=candidate, recorded_at=row[2])):
        raise ReleaseContractError("release decision candidate digest differs")
    original = dict(payload)
    original.pop("acceptance_mode", None)
    original.pop("candidate_id", None)
    original["evidence"] = {key: value for key, value in payload["evidence"].items() if key != "release_decision"}
    if (original != candidate or decision["bindings"] != candidate["bindings"]
            or payload.get("coverage_complete") is not False):
        raise ReleaseContractError("deferred acceptance changed its candidate evidence or coverage")
    if not parse_time(row[2]) <= parse_time(decision["issued_at"]) <= parse_time(recorded_at):
        raise ReleaseContractError("release decision timestamp differs from its candidate/acceptance")
    runtime = decision["runtime_bindings"]
    if (not isinstance(runtime, dict) or set(runtime) != {"build_sha256", "runtime_sha256", "config_sha256"}
            or any(not _HASH.fullmatch(str(value)) for value in runtime.values())
            or runtime["config_sha256"] != candidate["bindings"]["config_sha256"]
            or decision["transport_manifest"] != candidate["configuration_evidence"]["transport_manifest"]):
        raise ReleaseContractError("release decision runtime/config/transport binding differs")
    refs = decision["runtime_evidence"]
    if not isinstance(refs, dict) or set(refs) != {"build", "runtime"}:
        raise ReleaseContractError("release decision requires the installed runtime evidence")
    for key in ("build", "runtime"):
        verified_reference(refs[key], project_root=project_root)
        if refs[key].get("sha256") != runtime[f"{key}_sha256"] or refs[key].get("result") != "passed":
            raise ReleaseContractError("release decision loaded runtime hash differs")
    build = _read_receipt(Path(refs["build"]["path"]), contract_version=SEALED_BUILD_CONTRACT)
    runtime_body = _read_receipt(Path(refs["runtime"]["path"]), contract_version=RUNTIME_ROOT_CONTRACT)
    formal = runtime_body.get("formal_database", {})
    installed_runtime = runtime_body.get("installed_runtime", {})
    from seal_r0_receipts import ROOT_PATHS
    roots = runtime_body.get("roots", {})
    if (project_root is None or runtime_body.get("action") != "retain"
            or runtime_body.get("mutation_counts") != {"moved_files": 0, "copied_bytes": 0, "deleted_files": 0}
            or runtime_body.get("project_root") != str(project_root.resolve())
            or set(roots) != {"project", *ROOT_PATHS}
            or any(roots[name]["identity"]["path"] != str(project_root / relative)
                   for name, relative in {"project": Path("."), **ROOT_PATHS}.items())
            or installed_runtime.get("working_directory") != str(project_root)
            or formal.get("user_version") != 20
            or formal.get("migration") != {"version": 20, "name": SCHEMA_MIGRATIONS[20]}
            or formal.get("quick_check") != ["ok"] or formal.get("integrity_check") != ["ok"]
            or formal.get("foreign_key_error_count") != 0
            or any(formal.get(key) != installed_runtime["database"].get(key)
                   or formal.get(key) != build["installed_runtime"]["database"].get(key)
                   for key in ("path", "device", "inode"))):
        raise ReleaseContractError("release decision runtime root/retention/database contract differs")
    sealed_candidate = build["deployment_readiness"]
    if (build.get("status") != "succeeded" or build["schema_contract"] != _schema_contract(20, 20)
            or build["runtime_root_receipt"]["sha256"] != runtime["runtime_sha256"]
            or sealed_candidate["deployment_id"] != decision["candidate_id"]
            or sealed_candidate["receipt_sha256"] != row[3]
            or sealed_candidate["bindings"] != decision["bindings"]
            or build["source_archive"]["sha256"] != candidate["evidence"]["source_archive"]["sha256"]
            or any(build["postmigration_lineage"][f"{key}_receipt"]["sha256"] != candidate["evidence"][key]["sha256"]
                   for key in ("migration", "install"))):
        raise ReleaseContractError("release decision installed build does not seal this candidate and pair")
    if project_root is None:
        raise ReleaseContractError("release decision checks require the exact source project root")
    test_payloads = []
    for test_ref in (build["test_results_receipt"], candidate["evidence"]["full_checks"]):
        verified_reference(test_ref, project_root=project_root)
        tests = _read_receipt(Path(test_ref["path"]), contract_version=TEST_RESULTS_CONTRACT)
        _verify_test_results_payload(project_root, tests)
        test_payloads.append(tests)
    # Postseal legitimately creates a new timestamped envelope for the same
    # source and real logs. Verify each file hash, then compare its semantics.
    if any(test_payloads[0][key] != test_payloads[1][key] for key in ("git", "results")):
        raise ReleaseContractError("release decision installed and candidate test results differ")
    return {**decision, "decision_sha256": reference["sha256"]}


def validate_deployment_receipt(
    connection: sqlite3.Connection, *, deployment_id: str | None = None,
    require_accepted: bool = False, expected_bindings: Mapping[str, Any] | None = None,
    project_root: Path | None = None, maintenance_only: bool = False,
) -> dict[str, Any]:
    """Validate one persisted candidate/accepted receipt and real external files.

    ``expected_bindings`` binds the caller's exact build/runtime/config identities;
    both statuses recheck the explicitly selected local storage policy now. The
    Writer's maintenance-only path verifies every identity but does not confer
    admission while freeing space. A full business day is not an acceptance condition.
    """
    version = int(connection.execute("PRAGMA user_version").fetchone()[0])
    if version not in {20, 21}:
        raise ReleaseContractError("deployment receipt requires schema20 or its verified classification successor")
    from v8 import account_cleanup_snapshot
    if account_cleanup_snapshot.is_cleanup(connection, deployment_id):
        try:
            return account_cleanup_snapshot.validate(connection, deployment_id=deployment_id,
                project_root=project_root, expected_bindings=expected_bindings, require_accepted=require_accepted)
        except (ValueError, KeyError, TypeError, OSError, sqlite3.Error) as exc:
            raise ReleaseContractError("cleanup read-only deployment proof is invalid") from exc
    if version == 21:
        raise ReleaseContractError("schema21 requires the preserved cleanup and classification migration proof")
    if project_root is not None:
        from v8.capture_code_successor import deployment_context
        with deployment_context(connection, project_root=project_root) as inherited:
            if inherited:
                return validate_deployment_receipt(connection, deployment_id=deployment_id,
                    require_accepted=require_accepted, expected_bindings=expected_bindings,
                    project_root=project_root, maintenance_only=maintenance_only)
    cursor = connection.execute(
        "SELECT deployment_id,status,payload_json,recorded_at,receipt_sha256 FROM deployment_readiness_receipts "
        + ("WHERE deployment_id=? " if deployment_id else "") + "ORDER BY id DESC LIMIT 1",
        (deployment_id,) if deployment_id else (),
    )
    row = cursor.fetchone()
    if row is None:
        raise ReleaseContractError("schema20 deployment receipt is missing")
    value = dict(zip([item[0] for item in cursor.description], row, strict=True))
    try:
        payload = json.loads(value["payload_json"])
        if not isinstance(payload, dict) or payload.get("contract_version") != CONTRACT_VERSION:
            raise ReleaseContractError("deployment payload contract differs")
        expected = deployment_digest(deployment_id=value["deployment_id"], status=value["status"],
                                     payload=payload, recorded_at=value["recorded_at"])
        if value["receipt_sha256"] != expected or value["status"] not in {"candidate", "accepted", "failed"}:
            raise ReleaseContractError("deployment receipt digest or status differs")
        if payload.get("schema_version") != 20 or payload.get("schema_migration") != SCHEMA_MIGRATIONS[20]:
            raise ReleaseContractError("deployment schema identity differs")
        bindings = payload.get("bindings")
        if not isinstance(bindings, dict):
            raise ReleaseContractError("deployment bindings are missing")
        for key in ("build_sha256", "runtime_sha256", "config_sha256", "activation_sha256", "roster_members_sha256"):
            if not _HASH.fullmatch(str(bindings.get(key))):
                raise ReleaseContractError(f"deployment {key} is invalid")
        for key, expected_value in (expected_bindings or {}).items():
            if bindings.get(key) != expected_value:
                raise ReleaseContractError(f"deployment binding differs: {key}")
        from v8.profile_activations import activation_by_id

        active = activation_by_id(connection, int(bindings["activation_id"]))
        for key in ("profile_id", "roster_snapshot_id", "roster_members_sha256", "activation_sha256"):
            if active[key] != bindings.get(key):
                raise ReleaseContractError(f"deployment activation binding differs: {key}")
        if active.get("cancellation") is not None:
            raise ReleaseContractError("deployment activation has been cancelled")
        references = payload.get("evidence")
        if not isinstance(references, dict):
            raise ReleaseContractError("deployment evidence is missing")
        verified = {key: verified_reference(references[key], project_root=project_root) for key in _REQUIRED_REFS}
        for key in ("migration", "rollback", "full_checks", "install"):
            if verified[key].get("result") != "passed":
                raise ReleaseContractError(f"deployment evidence did not pass: {key}")
        accepted = value["status"] == "accepted"
        if require_accepted and not accepted:
            raise ReleaseContractError("candidate deployment evidence is not production acceptance")
        capacity = validate_storage_policy(payload.get("storage_policy"), require_forecast=accepted,
                                           maintenance_only=maintenance_only)
        mode = payload.get("acceptance_mode")
        deferred = mode == DEFERRED_ACCEPTANCE
        if mode not in {None, DEFERRED_ACCEPTANCE}:
            raise ReleaseContractError("deployment acceptance mode differs")
        decision = None
        if deferred:
            if not accepted or "bounded_e2e" in references or "native_cohort_id" in payload:
                raise ReleaseContractError("deferred acceptance cannot also claim real E2E")
            verified["release_decision"] = verified_reference(references["release_decision"], project_root=project_root)
            if verified["release_decision"].get("result") != "deferred_by_user":
                raise ReleaseContractError("release decision must not be relabelled passed")
            decision = _validate_release_decision(connection, payload=payload, reference=verified["release_decision"],
                                                  recorded_at=value["recorded_at"], project_root=project_root)
        elif "release_decision" in references:
            raise ReleaseContractError("release decision requires explicit deferred acceptance")
        if "bounded_e2e" in references or (accepted and not deferred):
            verified["bounded_e2e"] = verified_reference(references["bounded_e2e"], project_root=project_root)
            if verified["bounded_e2e"].get("result") != "passed":
                raise ReleaseContractError("bounded end-to-end evidence did not pass")
        if accepted:
            verified["install"] = verified_reference(references["install"], project_root=project_root)
            if verified["install"].get("result") != "passed":
                raise ReleaseContractError("deployment install evidence did not pass")
        _validate_evidence_contents(connection, verified, project_root=project_root, accepted=accepted, deferred=deferred)
        coverage_complete = False
        if payload.get("coverage_complete") is True:
            from v8.runtime_receipts import current_activation_readiness

            coverage_complete = current_activation_readiness(connection, at=value["recorded_at"])["data_readiness"] is True
            if not coverage_complete:
                raise ReleaseContractError("claimed full-day coverage is not supported by the quality receipt")
        return {"contract_version": CONTRACT_VERSION, "deployment_id": value["deployment_id"],
                "status": value["status"], "receipt_sha256": value["receipt_sha256"],
                "validation_scope": "maintenance_only" if maintenance_only else "release",
                "deployment_eligible": accepted and not maintenance_only, "coverage_complete": coverage_complete,
                "bindings": bindings, "evidence": verified, "storage_policy": capacity,
                "release_decision": decision,
                "e2e_status": "deferred" if deferred else ("passed" if accepted else "not_verified"),
                "unmet_evidence": (["local_storage_admission_not_evaluated"] if maintenance_only else [])
                    + ([] if accepted else ["installed_acceptance", "bounded_e2e"]
                       + ([] if capacity["forecast_known"] else ["local_storage_capacity_forecast"])),
                }
    except (KeyError, TypeError, ValueError, OSError, RuntimeError, sqlite3.Error) as error:
        if isinstance(error, ReleaseContractError):
            raise
        raise ReleaseContractError("deployment receipt evidence is invalid") from error
