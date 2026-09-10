#!/usr/bin/env python3
"""Build a fail-closed read-replica bundle from live SQLite databases.

The database files use SQLite's online backup API. Required artifacts are
independent local copies captured while collecting the manifest. Publisher
transfers those copies separately; optional reuse and managed originals are
never copied. The server verifies the manifest before changing active data.
"""

from __future__ import annotations

import argparse
import hashlib
import json
import os
import re
import shutil
import signal
import sqlite3
import stat
import sys
import tempfile
import threading
from contextlib import contextmanager
from datetime import datetime, timezone
from pathlib import Path, PurePosixPath
from typing import Any, Iterable, Mapping, Optional

sys.path.insert(0, str(Path(__file__).resolve().parents[1] / "src/dcar_eval"))
from v8 import media_completion, media_lifecycle, raw_evidence  # noqa: E402
from v8.artifact_paths import using_artifact_root  # noqa: E402
from v8.artifact_paths import (  # noqa: E402
    ArtifactPathError, RUNTIME_EVIDENCE_ALIAS_CONTRACT, RUNTIME_EVIDENCE_DIRECTORY,
    runtime_evidence_aliases, runtime_evidence_source,
)
from v8.media import MediaProcessingError  # noqa: E402
from v8.snapshot_contract import (  # noqa: E402
    ARTIFACT_POLICY,
    MANAGED_ORIGINALS_CONTRACT,
    descriptor,
)

BUNDLE_SCHEMA = "dcar-read-replica-snapshot-v2"
DATABASE_NAMES = frozenset({"dcar_insight.sqlite3", "web_mvp.sqlite3"})
ARTIFACT_POLICY_NAME = "thin-server-v2"
OPTIONAL_REUSE_EVIDENCE_TYPES = ("media",)
FROZEN_ARTIFACT_DIRECTORY = "frozen-artifacts"
FROZEN_ARTIFACT_CONTRACT = "snapshot-frozen-artifacts-v1"
ARTIFACT_FREE_SPACE_RESERVE = 256 * 1024 * 1024
TEXT_SUFFIXES = frozenset({".json", ".jsonl", ".txt", ".md", ".csv", ".srt", ".vtt"})
LEGACY_LARGE_BINARY_SUFFIXES = frozenset({
    ".mp4", ".mov", ".m4v", ".webm", ".mkv", ".avi", ".mp3", ".wav", ".m4a",
})
REQUIRED_RUNTIME_ARTIFACTS = (
    "data/cache/.comment_hash_salt",
    "data/cache/.platform_user_salt",
)
FORMAL_STATE_ROOT = Path.home() / "Library/Application Support/DcarAIGC"
SHA256_RE = re.compile(r"[0-9a-f]{64}")
RUNTIME_IDENTITY_SCHEMA = "dcar-runtime-identity-v1"
EXPECTED_REPORT_VERSION = "dcar-content-operations-report-v8.9"
EXPECTED_DATABASE_SCHEMA_VERSION = 19
EXPECTED_DATABASE_SCHEMA_MIGRATION = "dual-acquisition-profile-roster-v1"
CLASSIFICATION_SCHEMA_VERSION = 21
CLASSIFICATION_SCHEMA_MIGRATION = "account-classification-v1"
EXPECTED_ACTIVE_RELEASE_ID = "evaluation-v9__selling-points-v5.2"
EXPECTED_ACTIVE_RELEASE_STATUS = "active"
EXPECTED_RULE_VERSION = "evaluation-v9"
EXPECTED_TAXONOMY_VERSION = "selling-points-v5.2"
EXPECTED_TAXONOMY_STATUS = "published"


class SnapshotBuildError(RuntimeError):
    """The requested bundle could not be proved internally consistent."""


class _FrozenArtifacts(dict):
    """Only required files enter this map; optional media has a plain dict."""

    def __init__(self, root: Path):
        super().__init__()
        self.root = root
        root.mkdir(mode=0o700)
        for name in ("cache", "reports"):
            (root / name).mkdir(mode=0o700)
        self.remaining_bytes = shutil.disk_usage(root).free - ARTIFACT_FREE_SPACE_RESERVE
        self.source_paths: dict[Path, Path] = {}

    def capture(self, project_root: Path, canonical: str, root_name: str,
                relative: str) -> tuple[Path, int, str]:
        """Pin the source with no-follow openat, copy bytes, then hash the copy.

        Source replacement after opening is safe. In-place modification during
        copying is rejected. No hardlink can share a writable source inode.
        """
        directory_flags = os.O_RDONLY | os.O_DIRECTORY | os.O_NOFOLLOW
        directory = os.open(project_root, directory_flags)
        source = None
        target = self.root / root_name / relative
        owned = False
        try:
            parts = PurePosixPath(canonical).parts
            for part in parts[:-1]:
                child = os.open(part, directory_flags, dir_fd=directory)
                os.close(directory)
                directory = child
            source = os.open(parts[-1], os.O_RDONLY | os.O_NOFOLLOW | os.O_NONBLOCK, dir_fd=directory)
            before = os.fstat(source)
            if not stat.S_ISREG(before.st_mode):
                raise SnapshotBuildError(f"artifact source is not a regular file: {canonical}")
            if before.st_size > self.remaining_bytes:
                raise SnapshotBuildError("insufficient local space to freeze snapshot artifacts")
            target.parent.mkdir(parents=True, mode=0o700, exist_ok=True)
            digest = hashlib.sha256()
            copied = 0
            with target.open("xb") as destination:
                owned = True
                while block := os.read(source, 1024 * 1024):
                    copied += len(block)
                    if copied > before.st_size:
                        raise SnapshotBuildError(f"artifact changed while freezing: {canonical}")
                    destination.write(block)
                    digest.update(block)
                after = os.fstat(source)
                if (copied != before.st_size or any(getattr(before, key) != getattr(after, key)
                        for key in ("st_dev", "st_ino", "st_size", "st_mtime_ns"))):
                    raise SnapshotBuildError(f"artifact changed while freezing: {canonical}")
                destination.flush()
                os.fsync(destination.fileno())
                os.fchmod(destination.fileno(), 0o400)
            self.remaining_bytes -= copied
            self.source_paths[target] = project_root / canonical
            return target, copied, digest.hexdigest()
        except Exception:
            # This unpublished tree belongs only to the current build.
            if owned:
                target.unlink(missing_ok=True)
            raise
        finally:
            if source is not None:
                os.close(source)
            os.close(directory)


def _utc_now() -> str:
    return (
        datetime.now(timezone.utc).isoformat(timespec="seconds").replace("+00:00", "Z")
    )


def _sha256(path: Path) -> str:
    digest = hashlib.sha256()
    with path.open("rb") as handle:
        for block in iter(lambda: handle.read(1024 * 1024), b""):
            digest.update(block)
    return digest.hexdigest()


def _fsync_file(path: Path) -> None:
    with path.open("rb") as handle:
        os.fsync(handle.fileno())


def _fsync_directory(path: Path) -> None:
    descriptor = os.open(path, os.O_RDONLY)
    try:
        os.fsync(descriptor)
    finally:
        os.close(descriptor)


def _connect_read_only(path: Path) -> sqlite3.Connection:
    connection = sqlite3.connect(f"{path.resolve().as_uri()}?mode=ro", uri=True)
    connection.row_factory = sqlite3.Row
    connection.execute("PRAGMA foreign_keys=ON")
    connection.execute("PRAGMA query_only=ON")
    return connection


def _table_exists(connection: sqlite3.Connection, table: str) -> bool:
    return (
        connection.execute(
            "SELECT 1 FROM sqlite_master WHERE type='table' AND name=?", (table,)
        ).fetchone()
        is not None
    )


def _runtime_identity(snapshot_db: Path, *, expected_user_version: int = EXPECTED_DATABASE_SCHEMA_VERSION) -> dict[str, Any]:
    """Freeze the exact report/schema/release identity from the backup DB."""

    with _connect_read_only(snapshot_db) as connection:
        user_version = int(connection.execute("PRAGMA user_version").fetchone()[0])
        try:
            migration_rows = connection.execute(
                "SELECT name FROM schema_migrations WHERE version=?",
                (user_version,),
            ).fetchall()
            max_migration = connection.execute(
                "SELECT MAX(version) FROM schema_migrations"
            ).fetchone()[0]
            release_rows = connection.execute(
                """
                SELECT er.id,er.rule_version,er.taxonomy_version,
                       er.matcher_rule_sha256,er.status release_status,
                       tv.status taxonomy_status
                FROM evaluation_releases er
                JOIN taxonomy_versions tv ON tv.version=er.taxonomy_version
                WHERE er.status='active'
                ORDER BY er.id
                """
            ).fetchall()
        except sqlite3.Error as exc:
            raise SnapshotBuildError(
                "snapshot database lacks the required runtime identity tables"
            ) from exc
    if len(migration_rows) != 1 or max_migration != user_version:
        raise SnapshotBuildError(
            "snapshot database has an ambiguous schema migration identity"
        )
    if len(release_rows) != 1:
        raise SnapshotBuildError(
            "snapshot database must have exactly one active evaluation release"
        )
    release = release_rows[0]
    matcher_rule_sha256 = str(release["matcher_rule_sha256"])
    identity = {
        "schema": RUNTIME_IDENTITY_SCHEMA,
        "report_version": EXPECTED_REPORT_VERSION,
        "database_schema_version": user_version,
        "database_schema_migration": str(migration_rows[0]["name"]),
        "active_release_id": str(release["id"]),
        "active_release_status": str(release["release_status"]),
        "rule_version": str(release["rule_version"]),
        "taxonomy_version": str(release["taxonomy_version"]),
        "taxonomy_status": str(release["taxonomy_status"]),
        "matcher_rule_sha256": matcher_rule_sha256,
    }
    versions = {19: EXPECTED_DATABASE_SCHEMA_MIGRATION, 20: "integrated-video-capture-v25", 21: "account-classification-v1"}
    if expected_user_version not in versions:
        raise SnapshotBuildError("snapshot requires explicit schema19, schema20 or schema21")
    expected = {
        "schema": RUNTIME_IDENTITY_SCHEMA,
        "report_version": EXPECTED_REPORT_VERSION,
        "database_schema_version": expected_user_version,
        "database_schema_migration": versions[expected_user_version],
        "active_release_id": EXPECTED_ACTIVE_RELEASE_ID,
        "active_release_status": EXPECTED_ACTIVE_RELEASE_STATUS,
        "rule_version": EXPECTED_RULE_VERSION,
        "taxonomy_version": EXPECTED_TAXONOMY_VERSION,
        "taxonomy_status": EXPECTED_TAXONOMY_STATUS,
    }
    for key, value in expected.items():
        if identity.get(key) != value:
            raise SnapshotBuildError(
                f"snapshot runtime identity mismatch for {key}: "
                f"{identity.get(key)!r}, expected {value!r}"
            )
    if SHA256_RE.fullmatch(matcher_rule_sha256) is None:
        raise SnapshotBuildError(
            "snapshot active release has an invalid matcher_rule_sha256"
        )
    return identity


def _validate_database(
    path: Path, *, expected_user_version: Optional[int]
) -> dict[str, Any]:
    with _connect_read_only(path) as connection:
        quick_rows = [
            str(row[0]) for row in connection.execute("PRAGMA quick_check").fetchall()
        ]
        if quick_rows != ["ok"]:
            raise SnapshotBuildError(
                f"SQLite quick_check failed for {path.name}: {quick_rows[:5]}"
            )
        foreign_key_rows = connection.execute("PRAGMA foreign_key_check").fetchall()
        if foreign_key_rows:
            raise SnapshotBuildError(
                f"SQLite foreign_key_check failed for {path.name}: "
                f"{len(foreign_key_rows)} violation(s)"
            )
        user_version = int(connection.execute("PRAGMA user_version").fetchone()[0])
        if expected_user_version is not None and user_version != expected_user_version:
            raise SnapshotBuildError(
                f"unexpected schema for {path.name}: {user_version}, "
                f"expected {expected_user_version}"
            )
        page_count = int(connection.execute("PRAGMA page_count").fetchone()[0])
        page_size = int(connection.execute("PRAGMA page_size").fetchone()[0])
    return {
        "quick_check": "ok",
        "foreign_key_violations": 0,
        "user_version": user_version,
        "page_count": page_count,
        "page_size": page_size,
    }


def _online_backup(
    source_path: Path,
    target_path: Path,
    *,
    expected_user_version: Optional[int],
) -> dict[str, Any]:
    if not source_path.is_file() or source_path.is_symlink():
        raise SnapshotBuildError(f"database is not a regular file: {source_path}")
    target_path.parent.mkdir(parents=True, exist_ok=True)
    with _connect_read_only(source_path) as source:
        target = sqlite3.connect(target_path)
        try:
            source.backup(target, pages=4096, sleep=0.01)
            target.execute("PRAGMA journal_mode=DELETE")
            target.commit()
        finally:
            target.close()
    validation = _validate_database(
        target_path, expected_user_version=expected_user_version
    )
    _fsync_file(target_path)
    return {
        "name": target_path.name,
        "bundle_path": f"databases/{target_path.name}",
        "byte_size": target_path.stat().st_size,
        "sha256": _sha256(target_path),
        **validation,
    }


def _normalize_project_relative(value: str) -> tuple[str, str, str]:
    if not value or "\x00" in value or "\n" in value or "\r" in value:
        raise SnapshotBuildError("artifact path contains an unsafe character")
    if "\\" in value:
        raise SnapshotBuildError(f"artifact path must use POSIX separators: {value}")
    path = PurePosixPath(value)
    if path.is_absolute() or any(part in {"", ".", ".."} for part in path.parts):
        raise SnapshotBuildError(f"artifact path is not project-relative: {value}")
    if path.parts[:2] == ("data", "cache") and len(path.parts) > 2:
        return "cache", str(PurePosixPath(*path.parts[2:])), str(path)
    if path.parts[0] == "reports" and len(path.parts) > 1:
        return "reports", str(PurePosixPath(*path.parts[1:])), str(path)
    raise SnapshotBuildError(
        f"online artifact must be below data/cache or reports: {value}"
    )


def _iter_project_paths(value: Any, *, project_root: Path) -> Iterable[str]:
    if isinstance(value, str):
        if value.startswith("data/cache/") or value.startswith("reports/"):
            yield value
        elif Path(value).is_absolute():
            try:
                relative = Path(value).relative_to(project_root)
            except (OSError, ValueError):
                return
            canonical = relative.as_posix()
            if canonical.startswith("data/cache/") or canonical.startswith("reports/"):
                yield canonical
        return
    if isinstance(value, list):
        for item in value:
            yield from _iter_project_paths(item, project_root=project_root)
        return
    if isinstance(value, Mapping):
        for item in value.values():
            yield from _iter_project_paths(item, project_root=project_root)


def _read_json_paths(path: Path, *, project_root: Path) -> list[str]:
    if path.suffix.lower() != ".json":
        return []
    try:
        value = json.loads(path.read_text(encoding="utf-8"))
    except (OSError, UnicodeDecodeError, json.JSONDecodeError) as exc:
        raise SnapshotBuildError(f"artifact JSON cannot be parsed: {path}") from exc
    return list(_iter_project_paths(value, project_root=project_root))


def _project_reference(value: Any, *, project_root: Path) -> str:
    if not isinstance(value, str):
        raise SnapshotBuildError("artifact reference path must be a string")
    path = Path(value)
    if path.is_absolute():
        try:
            value = path.relative_to(project_root).as_posix()
        except ValueError as exc:
            raise SnapshotBuildError("required artifact is outside the writer root") from exc
    return _normalize_project_relative(value)[2]


def _declared_file_references(value: Any) -> Iterable[tuple[str, Any, Any]]:
    """Find hash-bound file pointers, not operational roots or archive directories."""
    if isinstance(value, Mapping):
        path = next((value[key] for key in ("project_path", "local_path", "path")
                     if isinstance(value.get(key), str)), None)
        if path is not None and "sha256" in value:
            yield path, value["sha256"], value.get("byte_size")
        for item in value.values():
            yield from _declared_file_references(item)
    elif isinstance(value, list):
        for item in value:
            yield from _declared_file_references(item)


def _declared_project_reference(
    value: str, digest: Any, size: Any, *, project_root: Path,
    aliases: dict[str, dict[str, Any]],
) -> str:
    path = Path(value)
    if not path.is_absolute() or path.is_relative_to(project_root):
        return _project_reference(value, project_root=project_root)
    try:
        relative = runtime_evidence_source(value, str(FORMAL_STATE_ROOT))
        parent = path.parent.lstat()
        if (path != path.resolve(strict=True) or not stat.S_ISDIR(parent.st_mode)
                or parent.st_uid != os.geteuid() or parent.st_mode & 0o077
                or not isinstance(digest, str) or SHA256_RE.fullmatch(digest) is None
                or type(size) is not int or not 0 <= size <= raw_evidence.MAX_SIDECAR_BYTES):
            raise SnapshotBuildError("external runtime evidence identity is unsafe")
        body = raw_evidence._read_single_regular(path, max_bytes=raw_evidence.MAX_SIDECAR_BYTES)
        if len(body) != size or hashlib.sha256(body).hexdigest() != digest:
            raise SnapshotBuildError("external runtime evidence SHA-256 or size drifted")
        payload = json.loads(body)
        if not isinstance(payload, dict) or raw_evidence.canonical_json_bytes(payload) != body:
            raise SnapshotBuildError("external runtime evidence is not canonical JSON")
        if relative.startswith("evidence/"):
            from v8.runtime_receipts import _verify_self_hash
            _verify_self_hash(payload)
            expected_contract = ("scan-verification-evidence-v2" if path.name.startswith("scan-verification-v2.")
                                 else "profile-day-coverage-evidence-v3")
            if payload.get("contract_version") != expected_contract or path.name.split(".")[-2] != digest:
                raise SnapshotBuildError("external runtime evidence contract or filename differs")
        else:
            from v8.transport_receipts import CONTRACT_VERSION, _digest, _mirror_path
            unsigned = dict(payload)
            self_sha = unsigned.pop("self_sha256", None)
            if (payload.get("contract_version") != CONTRACT_VERSION or self_sha != _digest(unsigned)
                    or payload.get("payload_sha256") != _digest(payload.get("payload"))
                    or _mirror_path(root=path.parent, kind=payload["kind"], identity_key=payload["identity_key"]) != path):
                raise SnapshotBuildError("external transport receipt contract or filename differs")
        canonical = RUNTIME_EVIDENCE_DIRECTORY + "/" + digest + ".json"
        destination = project_root / canonical
        if destination != destination.resolve(strict=False):
            raise SnapshotBuildError("runtime evidence mirror path is unsafe")
        receipt = raw_evidence.write_immutable_json_receipt(destination, payload, evidence_root=destination.parent)
        if receipt.sha256 != digest or receipt.byte_size != size:
            raise SnapshotBuildError("runtime evidence mirror bytes changed")
        alias = {"source_path": value, "project_path": canonical, "sha256": digest, "byte_size": size}
        if value in aliases and aliases[value] != alias:
            raise SnapshotBuildError("external runtime evidence alias identity conflict")
        aliases[value] = alias
        return canonical
    except (ArtifactPathError, RuntimeError, OSError, ValueError, KeyError, TypeError) as exc:
        raise SnapshotBuildError(f"external runtime evidence is invalid: {value}") from exc


def _private_deployment_directory(deployment: Mapping[str, Any] | None, *, project_root: Path,
                                  code_successor: Mapping[str, Any] | None = None) -> dict[str, Any] | None:
    """Classify only exact files already validated as deployment evidence."""
    if deployment is None:
        return None
    from v20_release_contract import verified_reference
    from seal_r0_receipts import (
        SEALED_BUILD_CONTRACT, TEST_RESULTS_CONTRACT, _read_receipt,
        _test_results_payload, _verify_test_results_payload,
    )

    references = []
    def register(role: str, reference: Mapping[str, Any], *, parent_sha256: str | None = None) -> None:
        checked = verified_reference(dict(reference), project_root=project_root)
        entry = {"role": role, "path": checked["path"], "sha256": checked["sha256"],
                 "byte_size": Path(checked["path"]).stat().st_size}
        if parent_sha256 is not None:
            entry["parent_sha256"] = parent_sha256
        references.append(entry)
    from v8.account_cleanup_snapshot import CONTRACT as CLEANUP_CONTRACT, EVIDENCE_ROLES
    roles = EVIDENCE_ROLES if deployment.get("contract_version") == CLEANUP_CONTRACT else ("source_archive", "migration", "rollback", "full_checks", "install", "bounded_e2e", "release_decision")
    for key in roles:
        if key in deployment["evidence"]:
            register("deployment." + key, deployment["evidence"][key])
    decision = deployment.get("release_decision")
    if decision is not None:
        for key in ("build", "runtime"):
            register("decision." + key, decision["runtime_evidence"][key])
        anchor = decision["runtime_evidence"]["build"]
        build = _read_receipt(Path(anchor["path"]), contract_version=SEALED_BUILD_CONTRACT)
        check_ref = verified_reference(build["test_results_receipt"], project_root=project_root)
        checks = _read_receipt(Path(check_ref["path"]), contract_version=TEST_RESULTS_CONTRACT)
        if code_successor is None:
            _verify_test_results_payload(project_root, checks)
        elif checks != _test_results_payload(project_root,
                paths={name: Path(record["path"]) for name, record in checks["results"].items()},
                git_record=checks["git"]):
            raise SnapshotBuildError("historical deployment test evidence drifted")
        register("decision.build.full_checks", check_ref, parent_sha256=anchor["sha256"])
    if code_successor is not None:
        for reference in code_successor["private_references"]:
            register("code_successor." + reference["role"], reference)
    return {"contract_version": "private-deployment-references-v1", "deployment_id": deployment["deployment_id"],
            "deployment_receipt_sha256": deployment["receipt_sha256"],
            "references": sorted(references, key=lambda item: item["role"])}


def _private_reference_index(directory: Mapping[str, Any] | None) -> dict[str, dict[str, Any]]:
    result: dict[str, dict[str, Any]] = {}
    for entry in directory["references"] if directory is not None else []:
        identity = {key: entry[key] for key in ("path", "sha256", "byte_size")}
        previous = result.get(entry["path"])
        if previous is not None and previous != identity:
            raise SnapshotBuildError("private deployment reference identities conflict")
        result[entry["path"]] = identity
    return result


def _is_private_reference(path: str, digest: Any, size: Any,
                          private_references: Mapping[str, Mapping[str, Any]]) -> bool:
    reference = private_references.get(path)
    if reference is None:
        return False
    if digest != reference["sha256"] or size is not None and (type(size) is not int or size != reference["byte_size"]):
        raise SnapshotBuildError("private deployment reference SHA-256 or size differs")
    return True


def _json_references(path: Path, *, project_root: Path,
                     aliases: dict[str, dict[str, Any]] | None = None,
                     private_references: Mapping[str, Mapping[str, Any]] | None = None) -> Iterable[tuple[str, Any, Any]]:
    try:
        body = json.loads(path.read_text(encoding="utf-8"))
    except (OSError, UnicodeError, ValueError) as exc:
        raise SnapshotBuildError(f"artifact JSON cannot be parsed: {path}") from exc
    declared: set[str] = set()
    for value, digest, size in _declared_file_references(body):
        if _is_private_reference(value, digest, size, private_references or {}):
            continue
        # A file pointer is a contract, unlike arbitrary source text in raw JSON.
        canonical = _declared_project_reference(value, digest, size, project_root=project_root,
                                                aliases=aliases if aliases is not None else {})
        declared.add(canonical)
        yield canonical, digest, size
    for canonical in _iter_project_paths(body, project_root=project_root):
        if canonical not in declared:
            yield canonical, None, None


def _forbid_managed_original_transfer(canonical: str) -> None:
    parts = PurePosixPath(canonical).parts
    if any(part == "managed-v1" and parts[index + 3:index + 4] == ("originals",)
           for index, part in enumerate(parts)):
        raise SnapshotBuildError(f"unregistered managed original cannot be transferred: {canonical}")


def _add_artifact(
    files: dict[tuple[str, str], dict[str, Any]],
    pending_json: list[Path],
    *,
    project_root: Path,
    relative_path: str,
    expected_sha256: Optional[str] = None,
    expected_byte_size: Optional[int] = None,
    disposition_reason: Optional[str] = None,
) -> None:
    root_name, root_relative, canonical = _normalize_project_relative(relative_path)
    _forbid_managed_original_transfer(canonical)
    if expected_sha256 is not None and (
        not isinstance(expected_sha256, str) or not SHA256_RE.fullmatch(expected_sha256)
    ):
        raise SnapshotBuildError(f"artifact lacks a registered SHA-256: {canonical}")
    if expected_byte_size is not None and (
        type(expected_byte_size) is not int or expected_byte_size < 0
    ):
        raise SnapshotBuildError(f"artifact byte size drifted: {canonical}")
    key = (root_name, root_relative)
    existing = files.get(key)
    if isinstance(files, _FrozenArtifacts) and existing is not None:
        if ((expected_sha256 is not None and expected_sha256 != existing["sha256"])
                or (expected_byte_size is not None and expected_byte_size != existing["byte_size"])
                or disposition_reason != existing.get("reason")):
            raise SnapshotBuildError(f"artifact identity conflict: {canonical}")
        return
    candidate = project_root / canonical
    if (
        candidate.is_symlink()
        or not candidate.is_file()
        or candidate != candidate.resolve()
    ):
        raise SnapshotBuildError(
            f"referenced artifact is missing or unsafe: {canonical}"
        )
    if isinstance(files, _FrozenArtifacts):
        candidate, byte_size, sha256 = files.capture(project_root, canonical, root_name, root_relative)
    else:
        byte_size = candidate.stat().st_size
        sha256 = _sha256(candidate)
    if expected_sha256 is not None:
        if sha256 != expected_sha256:
            raise SnapshotBuildError(f"artifact SHA-256 drifted: {canonical}")
    if expected_byte_size is not None:
        if byte_size != expected_byte_size:
            raise SnapshotBuildError(f"artifact byte size drifted: {canonical}")
    item = {
        "root": root_name,
        "path": root_relative,
        "project_path": canonical,
        "byte_size": byte_size,
        "sha256": sha256,
    }
    if disposition_reason is not None:
        item["reason"] = disposition_reason
    if existing is not None and existing != item:
        raise SnapshotBuildError(f"artifact identity conflict: {canonical}")
    if existing is None:
        files[key] = item
        if candidate.suffix.lower() == ".json":
            pending_json.append(candidate)


def _add_legacy_comment_directory(
    files: dict[tuple[str, str], dict[str, Any]],
    pending_json: list[Path],
    *,
    project_root: Path,
    relative_path: str,
    expected_sha256: str,
    expected_byte_size: Optional[int],
) -> None:
    """Expand a legacy hash-bound comments directory into required files."""
    _root_name, _root_relative, canonical = _normalize_project_relative(relative_path)
    _forbid_managed_original_transfer(canonical)
    candidate = project_root / canonical
    if candidate.is_symlink() or not candidate.is_dir() or candidate != candidate.resolve():
        raise SnapshotBuildError(
            f"referenced comment directory is missing or unsafe: {canonical}"
        )
    children: list[Path] = []
    for child in candidate.rglob("*"):
        if child.is_symlink() or child != child.resolve():
            raise SnapshotBuildError(
                f"referenced comment directory contains an unsafe member: {canonical}"
            )
        if child.is_dir():
            continue
        if not child.is_file():
            raise SnapshotBuildError(
                f"referenced comment directory contains a non-file member: {canonical}"
            )
        children.append(child)
    children.sort(key=lambda child: child.relative_to(candidate).as_posix())
    if not children:
        raise SnapshotBuildError(
            f"referenced comment directory has no files: {canonical}"
        )
    digest = hashlib.sha256()
    byte_size = 0
    for child in children:
        relative = child.relative_to(candidate).as_posix()
        child_sha256 = _sha256(child)
        child_size = child.stat().st_size
        byte_size += child_size
        digest.update(relative.encode("utf-8"))
        digest.update(b"\0")
        digest.update(child_sha256.encode("ascii"))
        digest.update(b"\0")
        _add_artifact(
            files,
            pending_json,
            project_root=project_root,
            relative_path=child.relative_to(project_root).as_posix(),
            expected_sha256=child_sha256,
            expected_byte_size=child_size,
        )
    if not SHA256_RE.fullmatch(expected_sha256) or digest.hexdigest() != expected_sha256:
        raise SnapshotBuildError(
            f"comment directory SHA-256 drifted: {canonical}"
        )
    if (
        expected_byte_size is None
        or expected_byte_size < 0
        or byte_size != expected_byte_size
    ):
        raise SnapshotBuildError(
            f"comment directory byte size drifted: {canonical}"
        )


def _add_registered_optional(
    files: dict[tuple[str, str], dict[str, Any]],
    *,
    relative_path: str,
    expected_sha256: str,
    expected_byte_size: Optional[int],
    reason: str,
) -> None:
    root_name, root_relative, canonical = _normalize_project_relative(relative_path)
    _forbid_managed_original_transfer(canonical)
    if not SHA256_RE.fullmatch(expected_sha256):
        raise SnapshotBuildError(
            f"optional artifact lacks a registered SHA-256: {canonical}"
        )
    if expected_byte_size is None or expected_byte_size < 0:
        raise SnapshotBuildError(
            f"optional artifact lacks a registered byte size: {canonical}"
        )
    key = (root_name, root_relative)
    item = {
        "root": root_name,
        "path": root_relative,
        "project_path": canonical,
        "byte_size": expected_byte_size,
        "sha256": expected_sha256,
        "reason": reason,
    }
    existing = files.get(key)
    if existing is not None and existing != item:
        raise SnapshotBuildError(f"artifact identity conflict: {canonical}")
    files[key] = item


def _managed_originals(
    connection: sqlite3.Connection, *, project_root: Path,
    files: dict[tuple[str, str], dict[str, Any]], pending_json: list[Path],
) -> tuple[dict[str, Any], dict[str, dict[str, Any]], set[str]]:
    """Verify ownership before any JSON can nominate an original for transfer."""
    bundles: list[dict[str, Any]] = []
    members: dict[str, dict[str, Any]] = {}
    evidence_roots: set[str] = set()
    artifact_rows = connection.execute("SELECT * FROM evidence_artifacts ORDER BY id").fetchall()
    by_path: dict[str, set[int]] = {}
    for row in artifact_rows:
        try:
            canonical = _project_reference(row["local_path"], project_root=project_root)
        except SnapshotBuildError:
            if row["status"] == "available" or row["artifact_type"] == "media_lifecycle_manifest":
                raise
            continue
        by_path.setdefault(canonical, set()).add(row["id"])

    def required(reference: Mapping[str, Any], *, role: str | None = None) -> dict[str, Any]:
        name = _project_reference(reference["path"], project_root=project_root)
        _add_artifact(files, pending_json, project_root=project_root, relative_path=name,
                      expected_sha256=reference["sha256"],
                      expected_byte_size=reference["byte_size"])
        item = {"project_path": name, "sha256": reference["sha256"],
                "byte_size": reference["byte_size"]}
        if role is not None:
            item["role"] = role
        return item

    with using_artifact_root(project_root):
        for row in artifact_rows:
            if row["artifact_type"] != "media_lifecycle_manifest":
                continue
            try:
                metadata = json.loads(row["metadata_json"])
                bundle = media_lifecycle.load_bundle(connection, metadata["media_lifecycle"]["bundle_id"])
                original = media_lifecycle.original_artifact(connection, bundle)
                manifest, state = bundle["manifest"], bundle["state"]
                source = connection.execute("SELECT * FROM evidence_artifacts WHERE id=?",
                                            (manifest["source"]["artifact_id"],)).fetchone()
                if source is None or any(source[key] != value for key, value in
                        manifest["source"].items() if key in {
                            "artifact_type", "sha256", "byte_size", "captured_at",
                            "created_at", "processor_version", "local_path"}):
                    raise SnapshotBuildError("managed source identity changed")
                control = required({"path": row["local_path"], "sha256": row["sha256"],
                                    "byte_size": row["byte_size"]})
                required({"path": source["local_path"], "sha256": source["sha256"],
                          "byte_size": source["byte_size"]})
                evidence_root = _project_reference(str(bundle["evidence_root"]) + "/sentinel",
                                                   project_root=project_root).rsplit("/", 1)[0] + "/"
                evidence_roots.add(evidence_root)
                declared_members = []
                for member in manifest["members"]:
                    canonical = _project_reference(
                        str(bundle["originals_root"] / member["relative_path"]), project_root=project_root)
                    if canonical in members or by_path.get(canonical, set()) - {original["id"]}:
                        raise SnapshotBuildError("managed original ownership is shared")
                    root, path, canonical = _normalize_project_relative(canonical)
                    item = {key: member[key] for key in ("member_id", "index", "kind", "sha256", "byte_size")}
                    item.update({"root": root, "path": path, "project_path": canonical})
                    members[canonical] = item
                    declared_members.append(item)
                original_path = _project_reference(original["local_path"], project_root=project_root)
                if original["artifact_type"] == "media":
                    if original_path not in members:
                        raise SnapshotBuildError("managed video is not a registered member")
                elif original["artifact_type"] == "media_manifest":
                    required({"path": original["local_path"], "sha256": original["sha256"],
                              "byte_size": original["byte_size"]})
                else:
                    raise SnapshotBuildError("managed original artifact type is invalid")
                proofs: list[dict[str, Any]] = []
                if state.get("completion_receipt") is not None:
                    completion = media_completion._verify(connection, bundle)
                    for reference in completion["evidence_files"]:
                        proofs.append(required(reference, role=reference["role"]))
                elif state["storage_state"] != "hot":
                    raise SnapshotBuildError("archived managed originals lack completion proof")
                for role in ("archive_receipt", "hot_release_receipt", "restore_receipt", "deletion_receipt"):
                    reference = state.get(role)
                    if reference is None:
                        if (role == "archive_receipt" and state["storage_state"] != "hot"
                                or role == "deletion_receipt" and state["storage_state"] == "expired"):
                            raise SnapshotBuildError(f"managed originals lack {role}")
                        continue
                    proof_row, proof_ref = media_completion._artifact_ref(
                        connection, reference["artifact_id"], manifest["content_id"], role=role,
                        artifact_type="media_lifecycle_receipt", evidence_root=bundle["evidence_root"])
                    if any(proof_row[key] != reference[target] for key, target in
                           (("local_path", "path"), ("sha256", "sha256"), ("byte_size", "byte_size"))):
                        raise SnapshotBuildError(f"managed {role} pointer changed")
                    body = media_completion._read_object(proof_ref)
                    if body.get("bundle_id") != bundle["bundle_id"] or body.get("contract") != "media-retention-v1":
                        raise SnapshotBuildError(f"managed {role} binding changed")
                    if role in {"archive_receipt", "restore_receipt", "deletion_receipt"}:
                        if (body.get("manifest_sha256") != bundle["manifest_sha256"]
                                or body.get("members") != manifest["members"]):
                            raise SnapshotBuildError(f"managed {role} member identity changed")
                    if role == "archive_receipt" and (
                            body.get("operation") != "archive_full_restore_verified"
                            or body.get("full_decode") is not True
                            or body.get("archive_key") != state["archive_key"]
                            or body.get("completion_receipt") != state["completion_receipt"]):
                        raise SnapshotBuildError("managed archive proof is incomplete")
                    if role == "deletion_receipt" and (
                            body.get("operation") != "permanent_delete"
                            or any(body.get(key) != state[key] for key in
                                   ("archive_verified_at", "delete_due_at", "deleted_at"))):
                        raise SnapshotBuildError("managed deletion proof is incomplete")
                    proofs.append(required(proof_ref, role=role))
                bundles.append({
                    "bundle_id": bundle["bundle_id"], "content_id": manifest["content_id"],
                    "control_artifact_id": bundle["control_artifact_id"],
                    "original_artifact_id": original["id"], "manifest": control,
                    **{key: state.get(key) for key in
                       ("storage_state", "operation_state", "archive_verified_at", "delete_due_at", "deleted_at")},
                    "members": declared_members,
                    "proofs": sorted(proofs, key=lambda item: (item["role"], item["project_path"])),
                })
            except (media_lifecycle.LifecycleError, media_completion.CompletionBlocked,
                    MediaProcessingError, KeyError, TypeError, ValueError, OSError) as exc:
                raise SnapshotBuildError(f"managed-original validation failed for artifact {row['id']}: {exc}") from exc
    if any(item["project_path"] in members for item in files.values()):
        raise SnapshotBuildError("managed original appears in required evidence")
    return ({"contract_version": MANAGED_ORIGINALS_CONTRACT,
             "bundles": sorted(bundles, key=lambda item: item["bundle_id"])}, members, evidence_roots)


def _collect_artifacts(
    snapshot_db: Path, *, project_root: Path, aliases: dict[str, dict[str, Any]] | None = None,
    private_references: Mapping[str, Mapping[str, Any]] | None = None,
    frozen_root: Path | None = None,
) -> tuple[list[dict[str, Any]], list[dict[str, Any]], dict[str, Any]]:
    aliases = aliases if aliases is not None else {}
    files: dict[tuple[str, str], dict[str, Any]] = _FrozenArtifacts(frozen_root) if frozen_root else {}
    optional_reuse: dict[tuple[str, str], dict[str, Any]] = {}
    pending_json: list[Path] = []
    legacy_download_manifests: set[str] = set()
    for relative_path in REQUIRED_RUNTIME_ARTIFACTS:
        _add_artifact(
            files,
            pending_json,
            project_root=project_root,
            relative_path=relative_path,
        )
    with _connect_read_only(snapshot_db) as connection:
        managed, managed_members, evidence_roots = _managed_originals(
            connection, project_root=project_root, files=files, pending_json=pending_json)
        for row in connection.execute(
            "SELECT local_path,sha256,byte_size FROM provider_raw_responses"
        ):
            canonical = _project_reference(
                row["local_path"], project_root=project_root
            )
            _add_artifact(
                files,
                pending_json,
                project_root=project_root,
                relative_path=canonical,
                expected_sha256=row["sha256"],
                expected_byte_size=row["byte_size"],
            )
            if canonical.endswith(".json.zst"):
                raw_path = project_root / canonical
                sidecar_path = raw_evidence.sidecar_path_for(raw_path)
                _add_artifact(
                    files,
                    pending_json,
                    project_root=project_root,
                    relative_path=_project_reference(
                        str(sidecar_path), project_root=project_root
                    ),
                )
                try:
                    raw_evidence.read_raw_evidence(
                        raw_path,
                        expected_stored_sha256=str(row["sha256"]),
                        expected_stored_size=int(row["byte_size"]),
                    )
                except raw_evidence.RawEvidenceError as exc:
                    raise SnapshotBuildError(
                        f"compressed provider raw evidence is invalid: {canonical}"
                    ) from exc
        for row in connection.execute("SELECT source_path,source_sha256 FROM account_roster_snapshots"):
            _add_artifact(files, pending_json, project_root=project_root,
                          relative_path=_project_reference(row["source_path"], project_root=project_root),
                          expected_sha256=row["source_sha256"])
        for row in connection.execute("SELECT details_json FROM scheduler_runs"):
            try:
                details = json.loads(row["details_json"])
            except (TypeError, ValueError) as exc:
                raise SnapshotBuildError("scheduler receipt JSON is invalid") from exc
            for path, digest, size in _declared_file_references(details):
                if _is_private_reference(path, digest, size, private_references or {}):
                    continue
                canonical = _declared_project_reference(path, digest, size, project_root=project_root, aliases=aliases)
                if canonical in managed_members:
                    member = managed_members[canonical]
                    if digest != member["sha256"] or size not in {None, member["byte_size"]}:
                        raise SnapshotBuildError("managed original receipt identity changed")
                    continue
                _add_artifact(files, pending_json, project_root=project_root, relative_path=canonical,
                              expected_sha256=digest, expected_byte_size=size)
        if _table_exists(connection, "report_files"):
            for row in connection.execute(
                """
                SELECT local_path,sha256,byte_size FROM report_files
                WHERE status='available' ORDER BY local_path
                """
            ):
                _add_artifact(
                    files,
                    pending_json,
                    project_root=project_root,
                    relative_path=_project_reference(row["local_path"], project_root=project_root),
                    expected_sha256=str(row["sha256"] or ""),
                    expected_byte_size=int(row["byte_size"]),
                )
        if _table_exists(connection, "report_revisions"):
            for row in connection.execute(
                "SELECT report_json_path,report_sha256 FROM report_revisions"
            ):
                _add_artifact(
                    files,
                    pending_json,
                    project_root=project_root,
                    relative_path=_project_reference(row["report_json_path"], project_root=project_root),
                    expected_sha256=str(row["report_sha256"] or ""),
                )
        if _table_exists(connection, "evidence_artifacts"):
            for row in connection.execute(
                """
                SELECT artifact_type,local_path,sha256,byte_size
                FROM evidence_artifacts
                WHERE status='available' ORDER BY local_path
                """
            ):
                artifact_type = str(row["artifact_type"])
                canonical = _project_reference(row["local_path"], project_root=project_root)
                if canonical in managed_members:
                    member = managed_members[canonical]
                    if row["sha256"] != member["sha256"] or row["byte_size"] != member["byte_size"]:
                        raise SnapshotBuildError("managed original artifact identity changed")
                    continue
                suffix = PurePosixPath(canonical).suffix.lower()
                if artifact_type == "media_manifest" and not any(
                        canonical.startswith(root) for root in evidence_roots):
                    legacy_download_manifests.add(canonical)
                destination = (
                    optional_reuse
                    if artifact_type in OPTIONAL_REUSE_EVIDENCE_TYPES
                    else files
                )
                if (destination is files and suffix in LEGACY_LARGE_BINARY_SUFFIXES
                        and not any(canonical.startswith(root) for root in evidence_roots)
                        and not canonical.startswith("reports/")):
                    destination = optional_reuse
                if artifact_type in OPTIONAL_REUSE_EVIDENCE_TYPES:
                    _add_registered_optional(
                        optional_reuse,
                        relative_path=canonical,
                        expected_sha256=str(row["sha256"] or ""),
                        expected_byte_size=(
                            int(row["byte_size"])
                            if row["byte_size"] is not None
                            else None
                        ),
                        reason="large_binary",
                    )
                    continue
                if artifact_type == "comments" and (project_root / canonical).is_dir():
                    _add_legacy_comment_directory(
                        files,
                        pending_json,
                        project_root=project_root,
                        relative_path=canonical,
                        expected_sha256=str(row["sha256"] or ""),
                        expected_byte_size=(
                            int(row["byte_size"])
                            if row["byte_size"] is not None
                            else None
                        ),
                    )
                    continue
                _add_artifact(
                    destination,
                    pending_json if destination is files else [],
                    project_root=project_root,
                    relative_path=canonical,
                    expected_sha256=str(row["sha256"] or ""),
                    expected_byte_size=(
                        int(row["byte_size"]) if row["byte_size"] is not None else None
                    ),
                )
    parsed_json: set[Path] = set()
    while pending_json:
        artifact = pending_json.pop()
        if artifact in parsed_json:
            continue
        parsed_json.add(artifact)
        for referenced, digest, size in _json_references(artifact, project_root=project_root, aliases=aliases,
                                                       private_references=private_references):
            root_name, root_relative, _canonical = _normalize_project_relative(
                referenced
            )
            if _canonical in managed_members:
                member = managed_members[_canonical]
                if (digest is not None and digest != member["sha256"]
                        or size is not None and size != member["byte_size"]):
                    raise SnapshotBuildError("managed original JSON identity changed")
                continue
            if (root_name, root_relative) in files:
                existing = files[(root_name, root_relative)]
                if (digest is not None and digest != existing["sha256"]
                        or size is not None and size != existing["byte_size"]):
                    raise SnapshotBuildError(f"artifact reference identity changed: {referenced}")
                continue
            if (root_name, root_relative) in optional_reuse:
                existing = optional_reuse[(root_name, root_relative)]
                if (digest is not None and digest != existing["sha256"]
                        or size is not None and size != existing["byte_size"]):
                    raise SnapshotBuildError(f"optional artifact reference identity changed: {referenced}")
                continue
            suffix = PurePosixPath(referenced).suffix.lower()
            legacy_binary = (
                suffix in LEGACY_LARGE_BINARY_SUFFIXES
                or ((files.source_paths.get(artifact, artifact) if isinstance(files, _FrozenArtifacts)
                     else artifact).relative_to(project_root).as_posix() in legacy_download_manifests
                    and suffix not in TEXT_SUFFIXES)
            )
            if (root_name == "reports" or not legacy_binary
                    or any(_canonical.startswith(root) for root in evidence_roots)):
                _add_artifact(
                    files,
                    pending_json,
                    project_root=project_root,
                    relative_path=referenced,
                    expected_sha256=digest,
                    expected_byte_size=size,
                )
            else:
                _add_artifact(
                    optional_reuse,
                    [],
                    project_root=project_root,
                    relative_path=referenced,
                    expected_sha256=digest,
                    expected_byte_size=size,
                    disposition_reason="large_binary",
                )
    for identity in tuple(optional_reuse):
        if identity in files:
            optional_reuse.pop(identity)
    return (
        sorted(files.values(), key=lambda item: (item["root"], item["path"])),
        sorted(
            optional_reuse.values(), key=lambda item: (item["root"], item["path"])
        ),
        managed,
    )


def _freshness(snapshot_db: Path) -> dict[str, Any]:
    output: dict[str, Any] = {}
    with _connect_read_only(snapshot_db) as connection:
        if _table_exists(connection, "content_items"):
            row = connection.execute(
                """
                SELECT COUNT(*) content_count,
                       MAX(published_at) latest_published_at,
                       MAX(imported_at) latest_imported_at
                FROM content_items
                """
            ).fetchone()
            output.update(dict(row) if row is not None else {})
        if _table_exists(connection, "scheduler_runs"):
            output["scheduler_jobs"] = [
                dict(row)
                for row in connection.execute(
                    """
                    SELECT s.job_id,s.scheduled_for,s.status,s.completed_at
                    FROM scheduler_runs s JOIN (
                        SELECT job_id,MAX(scheduled_for) scheduled_for
                        FROM scheduler_runs GROUP BY job_id
                    ) latest
                      ON latest.job_id=s.job_id
                     AND latest.scheduled_for=s.scheduled_for
                    ORDER BY s.job_id
                    """
                )
            ]
    return output


def _artifact_set_sha256(files: Iterable[Mapping[str, Any]]) -> str:
    digest = hashlib.sha256()
    for item in files:
        digest.update(
            (
                f"{item['root']}\0{item['path']}\0{item['byte_size']}\0"
                f"{item['sha256']}\n"
            ).encode("utf-8")
        )
    return digest.hexdigest()


def _write_from0_lists(bundle_root: Path, files: Iterable[Mapping[str, Any]]) -> None:
    grouped: dict[str, list[str]] = {"cache": [], "reports": []}
    for item in files:
        grouped[str(item["root"])].append(str(item["path"]))
    for root_name, values in grouped.items():
        target = bundle_root / f"{root_name}-files-from0"
        payload = b"".join(value.encode("utf-8") + b"\0" for value in values)
        target.write_bytes(payload)
        _fsync_file(target)


@contextmanager
def _cleanup_on_termination():
    """launchctl SIGTERM must unwind this build's private temporary tree."""
    if threading.current_thread() is not threading.main_thread():
        yield
        return
    previous = signal.getsignal(signal.SIGTERM)

    def terminate(signum, _frame):
        raise SystemExit(128 + signum)

    signal.signal(signal.SIGTERM, terminate)
    try:
        yield
    finally:
        signal.signal(signal.SIGTERM, previous)


@_cleanup_on_termination()
def build_snapshot(
    *,
    project_root: Path,
    database: Path,
    output: Path,
    legacy_database: Optional[Path] = None,
    expected_user_version: Optional[int] = None,
    deployment_id: Optional[str] = None,
    require_accepted_deployment: bool = False,
) -> dict[str, Any]:
    project_root = project_root.resolve()
    database = database.resolve()
    legacy_database = legacy_database.resolve() if legacy_database else None
    output = output.resolve()
    if output.exists():
        raise SnapshotBuildError(f"output already exists: {output}")
    output.parent.mkdir(parents=True, exist_ok=True)
    temporary = Path(
        tempfile.mkdtemp(prefix=f".{output.name}.", dir=str(output.parent))
    )
    try:
        database_dir = temporary / "databases"
        database_dir.mkdir(mode=0o700)
        databases = [
            _online_backup(
                database,
                database_dir / "dcar_insight.sqlite3",
                expected_user_version=expected_user_version,
            )
        ]
        if legacy_database is not None:
            databases.append(
                _online_backup(
                    legacy_database,
                    database_dir / "web_mvp.sqlite3",
                    expected_user_version=None,
                )
            )
        runtime_identity = _runtime_identity(
            database_dir / "dcar_insight.sqlite3",
            expected_user_version=expected_user_version or EXPECTED_DATABASE_SCHEMA_VERSION,
        )
        deployment_readiness = None
        code_successor = None
        if runtime_identity["database_schema_version"] in {20, 21}:
            from v20_release_contract import ReleaseContractError, validate_deployment_receipt

            try:
                with _connect_read_only(database_dir / "dcar_insight.sqlite3") as connection:
                    from v8.capture_code_successor import current_proof

                    from v8.account_cleanup_snapshot import is_cleanup, validate as validate_cleanup
                    cleanup = is_cleanup(connection, deployment_id)
                    if runtime_identity["database_schema_version"] == 21 and not cleanup:
                        raise SnapshotBuildError("schema21 requires inherited cleanup and classification migration proof")
                    if not cleanup:
                        code_successor = current_proof(connection, project_root=project_root, at=_utc_now())
                    if runtime_identity["database_schema_version"] == 21:
                        deployment_readiness = validate_cleanup(connection, deployment_id=deployment_id, project_root=project_root)
                        if (deployment_readiness.get("schema_version") != 21
                                or deployment_readiness.get("schema_migration") != CLASSIFICATION_SCHEMA_MIGRATION
                                or not isinstance(deployment_readiness.get("account_classification_migration"), dict)):
                            raise SnapshotBuildError("schema21 classification migration proof is missing")
                        if require_accepted_deployment and deployment_readiness.get("status") != "accepted":
                            raise SnapshotBuildError("schema21 deployment has not been accepted")
                    else:
                        deployment_readiness = validate_deployment_receipt(
                            connection, deployment_id=deployment_id, require_accepted=require_accepted_deployment,
                            project_root=project_root,
                        )
            except ReleaseContractError as exc:
                raise SnapshotBuildError(str(exc)) from exc
        elif deployment_id is not None or require_accepted_deployment:
            raise SnapshotBuildError("deployment readiness selection requires explicit schema20 or schema21")
        aliases: dict[str, dict[str, Any]] = {}
        private_directory = _private_deployment_directory(deployment_readiness, project_root=project_root,
            code_successor=code_successor)
        files, optional_reuse_files, managed_originals = _collect_artifacts(
            database_dir / "dcar_insight.sqlite3", project_root=project_root, aliases=aliases,
            private_references=_private_reference_index(private_directory),
            frozen_root=temporary / FROZEN_ARTIFACT_DIRECTORY,
        )
        _write_from0_lists(temporary, files)
        main_database = databases[0]
        snapshot_id = (
            datetime.now(timezone.utc).strftime("%Y%m%dT%H%M%SZ")
            + "-"
            + str(main_database["sha256"])[:12]
        )
        manifest: dict[str, Any] = {
            "schema": BUNDLE_SCHEMA,
            "snapshot_id": snapshot_id,
            "created_at": _utc_now(),
            "writer_project_root": str(project_root),
            "runtime_identity": runtime_identity,
            "snapshot_contract": descriptor(),
            "databases": databases,
            "freshness": _freshness(database_dir / "dcar_insight.sqlite3"),
            "artifact_policy": dict(ARTIFACT_POLICY),
            "managed_originals": managed_originals,
            "files": files,
            "file_count": len(files),
            "file_byte_size": sum(int(item["byte_size"]) for item in files),
            "file_set_sha256": _artifact_set_sha256(files),
            "local_artifact_source": {"contract": FROZEN_ARTIFACT_CONTRACT,
                                      "directory": FROZEN_ARTIFACT_DIRECTORY},
            "optional_reuse_files": optional_reuse_files,
            "optional_reuse_file_count": len(optional_reuse_files),
            "optional_reuse_byte_size": sum(
                int(item["byte_size"]) for item in optional_reuse_files
            ),
            "optional_reuse_set_sha256": _artifact_set_sha256(
                optional_reuse_files
            ),
        }
        if aliases:
            manifest["runtime_evidence_aliases"] = {
                "contract_version": RUNTIME_EVIDENCE_ALIAS_CONTRACT,
                "writer_state_root": str(FORMAL_STATE_ROOT),
                "files": sorted(aliases.values(), key=lambda row: row["source_path"]),
            }
            runtime_evidence_aliases(manifest)
        if deployment_readiness is not None:
            manifest["deployment_readiness"] = deployment_readiness
            manifest["private_deployment_references"] = private_directory
        if code_successor is not None:
            manifest["code_successor"] = code_successor
        manifest_bytes = (
            json.dumps(manifest, ensure_ascii=False, indent=2, sort_keys=True) + "\n"
        ).encode("utf-8")
        manifest_path = temporary / "manifest.json"
        manifest_path.write_bytes(manifest_bytes)
        (temporary / "manifest.sha256").write_text(
            hashlib.sha256(manifest_bytes).hexdigest() + "  manifest.json\n",
            encoding="ascii",
        )
        _fsync_file(manifest_path)
        _fsync_file(temporary / "manifest.sha256")
        _fsync_directory(database_dir)
        _fsync_directory(temporary)
        os.replace(temporary, output)
        _fsync_directory(output.parent)
        return manifest
    except BaseException:
        shutil.rmtree(temporary, ignore_errors=True)
        raise


def _parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--project-root", type=Path, default=Path.cwd())
    parser.add_argument("--db", type=Path, required=True)
    parser.add_argument("--legacy-db", type=Path)
    parser.add_argument("--output", type=Path, required=True)
    parser.add_argument("--expected-user-version", type=int)
    parser.add_argument("--deployment-id")
    parser.add_argument("--require-accepted-deployment", action="store_true")
    return parser


def main() -> int:
    arguments = _parser().parse_args()
    try:
        manifest = build_snapshot(
            project_root=arguments.project_root,
            database=arguments.db,
            legacy_database=arguments.legacy_db,
            output=arguments.output,
            expected_user_version=arguments.expected_user_version,
            deployment_id=arguments.deployment_id,
            require_accepted_deployment=arguments.require_accepted_deployment,
        )
    except SnapshotBuildError as exc:
        raise SystemExit(f"snapshot build refused: {exc}") from exc
    print(
        json.dumps(
            {
                "snapshot_id": manifest["snapshot_id"],
                "database_sha256": manifest["databases"][0]["sha256"],
                "file_count": manifest["file_count"],
                "file_byte_size": manifest["file_byte_size"],
            },
            ensure_ascii=False,
            sort_keys=True,
        )
    )
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
