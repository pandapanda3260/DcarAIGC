"""Fail-closed lifecycle management for evaluation-v8 releases.

This module is intentionally an operator-facing service boundary.  It never
creates a database, never calls providers, and never exposes an HTTP surface.
All mutations require an explicit database and freeze manifest.
"""

from __future__ import annotations

import hashlib
import json
import os
import re
import sqlite3
import tempfile
from collections import Counter
from collections.abc import Iterator, Mapping, Sequence
from contextlib import contextmanager
from dataclasses import dataclass
from pathlib import Path
from typing import Any

from .evaluation import (
    EVIDENCE_VERSION,
    INCLUDE_MIN,
    REVIEW_MIN,
    V8_RULE_VERSION,
    _current_evidence_state,
    _evaluate_content,
    canonical_json,
)
from .matcher_dsl import V5_2_POINT_SPEC, taxonomy_matcher_sha256
from .storage import (
    SCHEMA_VERSION,
    SchemaMigrationError,
    configure_connection_safety,
    now_utc,
    require_schema_compatibility,
    transaction,
)
from .taxonomy import TaxonomyError, serialize_point_row


SOURCE_TAXONOMY_VERSION = "selling-points-v5.1"
SOURCE_RELEASE_ID = "evaluation-v8__selling-points-v5.1"
SOURCE_RULE_VERSION = "evaluation-v8"
TARGET_TAXONOMY_VERSION = "selling-points-v5.2"
TARGET_RELEASE_ID = "evaluation-v8__selling-points-v5.2"
POINT_IDS = frozenset(V5_2_POINT_SPEC)
BATCH_SIZE = 250
FREEZE_SCHEMA_VERSION = "dcar-v9-freeze-manifest-v1"
RECEIPT_SCHEMA_VERSION = "dcar-evaluation-release-ready-v2"
AUDIT_PREFIX = "release-backfill"
BACKFILL_PROTECTED_CONTRACT = "backfill-protected-v2"
ACTIVATION_STABLE_CONTRACT = "activation-stable-v2"
HEX_SHA256 = re.compile(r"[0-9a-f]{64}", re.ASCII)
EVIDENCE_COMPONENT_KEYS = frozenset(
    {
        "detail_raw_sha256",
        "text_sha256",
        "media_sha256",
        "asr_sha256",
        "ocr_sha256",
        "comments_version_sha256",
        "manual_evidence_sha256",
    }
)
ENVELOPE_STATES = frozenset({"exact", "stale", "absent"})
PROJECT_ROOT = Path(__file__).resolve().parents[3]


class ReleaseManagementError(RuntimeError):
    """Raised when a release lifecycle operation cannot proceed safely."""


@dataclass(frozen=True)
class FrozenContent:
    content_id: int
    evidence_sha256: str
    components: Mapping[str, Any]
    envelope_state: str


@dataclass(frozen=True)
class FrozenArtifact:
    row: Mapping[str, Any]


@dataclass(frozen=True)
class FrozenTableAnchor:
    table: str
    columns: tuple[str, ...]
    lifecycle_stable_columns: tuple[str, ...]
    where: str
    parameters: tuple[Any, ...]
    row_count: int
    rows_sha256: str
    lifecycle_stable_rows_sha256: str


@dataclass(frozen=True)
class FreezeManifest:
    path: Path
    sha256: str
    logical_snapshot_sha256: str
    source_database: Path
    freeze_lock: Path
    database_backup: Path
    database_backup_sha256: str
    content_inventory_sha256: str
    artifact_inventory_sha256: str
    content_high_water: int
    contents: tuple[FrozenContent, ...]
    artifacts: tuple[FrozenArtifact, ...]
    table_counts: Mapping[str, int]
    content_columns: tuple[str, ...]
    content_rows_sha256: str
    content_stable_columns: tuple[str, ...]
    content_stable_rows_sha256: str
    table_anchors: tuple[FrozenTableAnchor, ...]


def _sha256_bytes(value: bytes) -> str:
    return hashlib.sha256(value).hexdigest()


def _sha256_file(path: Path) -> str:
    digest = hashlib.sha256()
    try:
        with path.open("rb") as handle:
            for chunk in iter(lambda: handle.read(1024 * 1024), b""):
                digest.update(chunk)
    except OSError as error:
        raise ReleaseManagementError(
            f"cannot hash frozen file {path}: {error}"
        ) from error
    return digest.hexdigest()


def _fingerprint_disk_path(path: Path) -> dict[str, Any]:
    if path.is_file():
        return {
            "disk_state": "file",
            "actual_byte_size": path.stat().st_size,
            "actual_sha256": _sha256_file(path),
            "actual_file_count": 1,
        }
    if path.is_dir():
        digest = hashlib.sha256()
        byte_size = 0
        files = sorted(child for child in path.rglob("*") if child.is_file())
        for child in files:
            relative = child.relative_to(path).as_posix()
            child_size = child.stat().st_size
            child_sha256 = _sha256_file(child)
            byte_size += child_size
            digest.update(relative.encode("utf-8"))
            digest.update(b"\0")
            digest.update(child_sha256.encode("ascii"))
            digest.update(b"\0")
        return {
            "disk_state": "directory",
            "actual_byte_size": byte_size,
            "actual_sha256": digest.hexdigest(),
            "actual_file_count": len(files),
        }
    return {
        "disk_state": "missing",
        "actual_byte_size": None,
        "actual_sha256": None,
        "actual_file_count": None,
    }


def _require_component_map(value: Any, *, content_id: int) -> dict[str, Any]:
    if not isinstance(value, Mapping) or set(value) != EVIDENCE_COMPONENT_KEYS:
        raise ReleaseManagementError(
            f"content {content_id} must have the seven freeze-v1 evidence components"
        )
    components = dict(value)
    for key, component in components.items():
        if component is not None:
            _require_sha256(component, label=f"content {content_id} {key}")
    return components


def _read_json_object(path: Path, *, label: str) -> tuple[dict[str, Any], bytes]:
    try:
        payload = path.read_bytes()
        value = json.loads(payload.decode("utf-8"))
    except (OSError, UnicodeDecodeError, json.JSONDecodeError, ValueError) as error:
        raise ReleaseManagementError(f"cannot read {label}: {error}") from error
    if not isinstance(value, dict):
        raise ReleaseManagementError(f"{label} must be a JSON object")
    return value, payload


def _require_sha256(value: Any, *, label: str) -> str:
    normalized = str(value or "")
    if HEX_SHA256.fullmatch(normalized) is None:
        raise ReleaseManagementError(f"{label} must be a lowercase SHA-256")
    return normalized


def _frozen_anchor_filter(
    connection: sqlite3.Connection,
    *,
    table: str,
    logical_snapshot_sha256: str,
) -> tuple[str, tuple[Any, ...]]:
    """Select only rows that existed at freeze when later phases append rows."""

    if table == "taxonomy_versions":
        return "WHERE version<>?", (TARGET_TAXONOMY_VERSION,)
    if table == "selling_points":
        return (
            "WHERE taxonomy_id NOT IN "
            "(SELECT id FROM taxonomy_versions WHERE version=?)",
            (TARGET_TAXONOMY_VERSION,),
        )
    if table == "selling_point_scenes":
        return (
            "WHERE selling_point_id NOT IN ("
            "SELECT sp.id FROM selling_points sp "
            "JOIN taxonomy_versions tv ON tv.id=sp.taxonomy_id "
            "WHERE tv.version=?"
            ")",
            (TARGET_TAXONOMY_VERSION,),
        )
    if table in {"evidence_envelopes", "evaluation_versions"}:
        maximum = int(
            connection.execute(f'SELECT COALESCE(MAX(id),0) FROM "{table}"').fetchone()[
                0
            ]
        )
        return "WHERE id<=?", (maximum,)
    if table == "evaluation_releases":
        return "WHERE id<>?", (TARGET_RELEASE_ID,)
    if table == "evaluation_matches":
        maximum = int(
            connection.execute(
                "SELECT COALESCE(MAX(id),0) FROM evaluation_versions"
            ).fetchone()[0]
        )
        return "WHERE evaluation_id<=?", (maximum,)
    if table == "migration_audit":
        prefix = (
            f"{AUDIT_PREFIX}__{TARGET_RELEASE_ID}__{logical_snapshot_sha256[:12]}__%"
        )
        return "WHERE id NOT LIKE ?", (prefix,)
    if table == "schema_migrations":
        maximum = int(
            connection.execute(
                "SELECT COALESCE(MAX(version),0) FROM schema_migrations"
            ).fetchone()[0]
        )
        return "WHERE version<=?", (maximum,)
    return "", ()


def _load_freeze_manifest(path: Path) -> FreezeManifest:
    path = path.resolve()
    if (path.parent / "SUPERSEDED.txt").exists():
        raise ReleaseManagementError("freeze bundle is marked SUPERSEDED")
    value, payload = _read_json_object(path, label="freeze manifest")
    if value.get("schema_version") != FREEZE_SCHEMA_VERSION:
        raise ReleaseManagementError("unsupported freeze manifest schema")
    manifest_sha256 = _sha256_bytes(payload)
    logical_sha256 = _require_sha256(
        value.get("logical_snapshot_sha256"), label="logical snapshot hash"
    )
    source_database_text = str(value.get("source_database") or "")
    freeze_lock_text = str(value.get("freeze_lock") or "")
    if not source_database_text or not freeze_lock_text:
        raise ReleaseManagementError("freeze manifest is missing source database/lock")
    source_database = Path(source_database_text).resolve()
    if not Path(freeze_lock_text).is_file():
        raise ReleaseManagementError("operator freeze lock is missing")
    backup = value.get("database_backup")
    inventory_summary = value.get("content_evidence_inventory")
    artifact_summary = value.get("evidence_artifact_inventory")
    if (
        not isinstance(backup, Mapping)
        or not isinstance(inventory_summary, Mapping)
        or not isinstance(artifact_summary, Mapping)
    ):
        raise ReleaseManagementError(
            "freeze manifest is missing database/inventory data"
        )
    database_backup_sha256 = _require_sha256(
        backup.get("sha256"), label="frozen database hash"
    )
    backup_name = str(backup.get("path") or "")
    if not backup_name:
        raise ReleaseManagementError("frozen database backup path is required")
    backup_path = (path.parent / backup_name).resolve()
    if _sha256_file(backup_path) != database_backup_sha256:
        raise ReleaseManagementError("frozen database backup hash mismatch")
    inventory_sha256 = _require_sha256(
        inventory_summary.get("sha256"), label="content inventory hash"
    )
    inventory_name = str(inventory_summary.get("path") or "")
    if not inventory_name:
        raise ReleaseManagementError("content inventory path is required")
    inventory_path = (path.parent / inventory_name).resolve()
    try:
        inventory_payload = inventory_path.read_bytes()
    except OSError as error:
        raise ReleaseManagementError(
            f"cannot read content evidence inventory: {error}"
        ) from error
    if _sha256_bytes(inventory_payload) != inventory_sha256:
        raise ReleaseManagementError("content evidence inventory hash mismatch")

    rows: list[FrozenContent] = []
    seen: set[int] = set()
    for line_number, raw_line in enumerate(inventory_payload.splitlines(), 1):
        if not raw_line.strip():
            continue
        try:
            row = json.loads(raw_line.decode("utf-8"))
        except (UnicodeDecodeError, json.JSONDecodeError, ValueError) as error:
            raise ReleaseManagementError(
                f"invalid content inventory row {line_number}: {error}"
            ) from error
        if not isinstance(row, dict):
            raise ReleaseManagementError(
                f"content inventory row {line_number} is not an object"
            )
        content_id = int(row.get("content_id") or 0)
        if content_id < 1 or content_id in seen:
            raise ReleaseManagementError(
                f"content inventory row {line_number} has invalid content_id"
            )
        seen.add(content_id)
        evidence_sha256 = _require_sha256(
            row.get("current_evidence_sha256"),
            label=f"content {content_id} evidence hash",
        )
        components = _require_component_map(
            row.get("components"), content_id=content_id
        )
        envelope_state = str(row.get("envelope_state") or "")
        if envelope_state not in ENVELOPE_STATES:
            raise ReleaseManagementError(
                f"content {content_id} has invalid envelope_state"
            )
        rows.append(
            FrozenContent(
                content_id=content_id,
                evidence_sha256=evidence_sha256,
                components=components,
                envelope_state=envelope_state,
            )
        )
    rows.sort(key=lambda item: item.content_id)
    expected_count = int(inventory_summary.get("row_count") or 0)
    expected_high_water = int(inventory_summary.get("max_content_id") or 0)
    if not rows or len(rows) != expected_count:
        raise ReleaseManagementError("content inventory row count mismatch")
    if rows[-1].content_id != expected_high_water:
        raise ReleaseManagementError("content inventory high-water mismatch")
    expected_minimum = int(inventory_summary.get("min_content_id") or 0)
    if rows[0].content_id != expected_minimum:
        raise ReleaseManagementError("content inventory minimum mismatch")
    expected_states = inventory_summary.get("envelope_states")
    actual_states = Counter(item.envelope_state for item in rows)
    if not isinstance(expected_states, Mapping) or {
        str(key): int(count) for key, count in expected_states.items()
    } != dict(sorted(actual_states.items())):
        raise ReleaseManagementError("content inventory envelope-state counts mismatch")

    artifact_sha256 = _require_sha256(
        artifact_summary.get("sha256"), label="artifact inventory hash"
    )
    artifact_name = str(artifact_summary.get("path") or "")
    if not artifact_name:
        raise ReleaseManagementError("artifact inventory path is required")
    artifact_path = (path.parent / artifact_name).resolve()
    try:
        artifact_payload = artifact_path.read_bytes()
    except OSError as error:
        raise ReleaseManagementError(
            f"cannot read artifact inventory: {error}"
        ) from error
    if _sha256_bytes(artifact_payload) != artifact_sha256:
        raise ReleaseManagementError("artifact inventory hash mismatch")
    artifacts: list[FrozenArtifact] = []
    artifact_ids: set[int] = set()
    disk_cache: dict[str, dict[str, Any]] = {}
    integrity_counts: Counter[str] = Counter()
    for line_number, raw_line in enumerate(artifact_payload.splitlines(), 1):
        if not raw_line.strip():
            continue
        try:
            artifact = json.loads(raw_line.decode("utf-8"))
        except (UnicodeDecodeError, json.JSONDecodeError, ValueError) as error:
            raise ReleaseManagementError(
                f"invalid artifact inventory row {line_number}: {error}"
            ) from error
        if not isinstance(artifact, dict):
            raise ReleaseManagementError(
                f"artifact inventory row {line_number} is not an object"
            )
        artifact_id = int(artifact.get("id") or 0)
        if artifact_id < 1 or artifact_id in artifact_ids:
            raise ReleaseManagementError(
                f"artifact inventory row {line_number} has invalid id"
            )
        artifact_ids.add(artifact_id)
        local_path = str(artifact.get("local_path") or "")
        resolved_path = Path(local_path)
        if not resolved_path.is_absolute():
            resolved_path = PROJECT_ROOT / resolved_path
        cache_key = str(resolved_path)
        actual = disk_cache.get(cache_key)
        if actual is None:
            actual = _fingerprint_disk_path(resolved_path)
            disk_cache[cache_key] = actual
        if any(artifact.get(key) != actual[key] for key in actual):
            raise ReleaseManagementError(
                f"artifact inventory disk state changed for row {artifact_id}"
            )
        integrity = str(artifact.get("integrity") or "")
        integrity_counts[integrity] += 1
        artifacts.append(FrozenArtifact(row=artifact))
    if len(artifacts) != int(artifact_summary.get("row_count") or 0):
        raise ReleaseManagementError("artifact inventory row count mismatch")
    if len(disk_cache) != int(artifact_summary.get("unique_local_paths") or 0):
        raise ReleaseManagementError("artifact inventory unique-path count mismatch")
    expected_integrity = artifact_summary.get("integrity_states")
    if not isinstance(expected_integrity, Mapping) or {
        str(key): int(count) for key, count in expected_integrity.items()
    } != dict(sorted(integrity_counts.items())):
        raise ReleaseManagementError("artifact inventory integrity counts mismatch")

    table_snapshot = value.get("table_snapshot")
    if not isinstance(table_snapshot, Mapping):
        raise ReleaseManagementError("freeze manifest lacks table snapshots")
    table_counts: dict[str, int] = {}
    for table, summary in table_snapshot.items():
        if isinstance(summary, Mapping) and summary.get("count") is not None:
            table_counts[str(table)] = int(summary["count"])
    content_summary = table_snapshot.get("content_items")
    if not isinstance(content_summary, Mapping):
        raise ReleaseManagementError("freeze manifest lacks content_items snapshot")
    expected_content_rows_sha256 = _require_sha256(
        content_summary.get("rows_sha256"), label="frozen content_items rows hash"
    )
    with _existing_connection(backup_path, read_only=True) as frozen_connection:
        content_columns = tuple(
            str(row[1])
            for row in frozen_connection.execute("PRAGMA table_info(content_items)")
        )
        frozen_count, frozen_rows_sha256 = _rows_sha256(
            frozen_connection,
            table="content_items",
            columns=content_columns,
        )
        if (
            frozen_count != int(content_summary.get("count") or 0)
            or frozen_rows_sha256 != expected_content_rows_sha256
        ):
            raise ReleaseManagementError(
                "frozen database content_items do not match the manifest snapshot"
            )
        content_stable_columns = tuple(
            column
            for column in content_columns
            if column != "evaluation_content_direction"
        )
        _, content_stable_rows_sha256 = _rows_sha256(
            frozen_connection,
            table="content_items",
            columns=content_stable_columns,
            normalize_unknown_direction=True,
        )
        anchors: list[FrozenTableAnchor] = []
        frozen_tables = [
            str(row[0])
            for row in frozen_connection.execute(
                """
                SELECT name FROM sqlite_master
                WHERE type='table' AND name NOT LIKE 'sqlite_%' ORDER BY name
                """
            )
        ]
        for table in frozen_tables:
            summary = table_snapshot.get(table)
            if not isinstance(summary, Mapping):
                raise ReleaseManagementError(
                    f"freeze manifest lacks {table} table snapshot"
                )
            expected_rows_sha256 = _require_sha256(
                summary.get("rows_sha256"),
                label=f"frozen {table} rows hash",
            )
            columns = tuple(
                str(row[1])
                for row in frozen_connection.execute(f'PRAGMA table_info("{table}")')
            )
            stable_columns = tuple(
                column
                for column in columns
                if not (
                    table == "content_items"
                    and column == "evaluation_content_direction"
                )
                and not (
                    table == "taxonomy_versions"
                    and column in {"status", "published_at"}
                )
                and not (
                    table == "evaluation_releases"
                    and column
                    in {
                        "status",
                        "updated_at",
                        "activated_at",
                        "retired_at",
                        "failure_reason",
                    }
                )
            )
            where, parameters = _frozen_anchor_filter(
                frozen_connection,
                table=table,
                logical_snapshot_sha256=logical_sha256,
            )
            count, rows_sha256 = _rows_sha256(
                frozen_connection,
                table=table,
                columns=columns,
                where=where,
                parameters=parameters,
            )
            if (
                count != int(summary.get("count") or 0)
                or rows_sha256 != expected_rows_sha256
            ):
                raise ReleaseManagementError(
                    f"frozen database {table} does not match its manifest snapshot"
                )
            _, lifecycle_rows_sha256 = _rows_sha256(
                frozen_connection,
                table=table,
                columns=columns,
                where=where,
                parameters=parameters,
                normalize_unknown_direction=table == "content_items",
            )
            stable_count, stable_rows_sha256 = _rows_sha256(
                frozen_connection,
                table=table,
                columns=stable_columns,
                where=where,
                parameters=parameters,
                normalize_unknown_direction=table == "content_items",
            )
            if stable_count != count:
                raise ReleaseManagementError(
                    f"frozen database {table} stable projection changed row count"
                )
            anchors.append(
                FrozenTableAnchor(
                    table=table,
                    columns=columns,
                    lifecycle_stable_columns=stable_columns,
                    where=where,
                    parameters=parameters,
                    row_count=count,
                    rows_sha256=lifecycle_rows_sha256,
                    lifecycle_stable_rows_sha256=stable_rows_sha256,
                )
            )
    return FreezeManifest(
        path=path.resolve(),
        sha256=manifest_sha256,
        logical_snapshot_sha256=logical_sha256,
        source_database=source_database,
        freeze_lock=Path(freeze_lock_text).resolve(),
        database_backup=backup_path,
        database_backup_sha256=database_backup_sha256,
        content_inventory_sha256=inventory_sha256,
        artifact_inventory_sha256=artifact_sha256,
        content_high_water=expected_high_water,
        contents=tuple(rows),
        artifacts=tuple(artifacts),
        table_counts=table_counts,
        content_columns=content_columns,
        content_rows_sha256=expected_content_rows_sha256,
        content_stable_columns=content_stable_columns,
        content_stable_rows_sha256=content_stable_rows_sha256,
        table_anchors=tuple(anchors),
    )


@contextmanager
def _existing_connection(
    path: Path, *, read_only: bool
) -> Iterator[sqlite3.Connection]:
    connection: sqlite3.Connection | None = None
    mode = "ro" if read_only else "rw"
    try:
        connection = sqlite3.connect(
            f"{path.resolve().as_uri()}?mode={mode}", uri=True, timeout=30
        )
        connection.row_factory = sqlite3.Row
        configure_connection_safety(connection)
        connection.execute("PRAGMA busy_timeout=30000")
        if read_only:
            connection.execute("PRAGMA query_only=ON")
        yield connection
    except sqlite3.Error as error:
        access = "read-only" if read_only else "read-write"
        raise ReleaseManagementError(
            f"cannot access existing database {access}: {error}"
        ) from error
    finally:
        if connection is not None:
            connection.close()


def _require_v9(connection: sqlite3.Connection) -> None:
    try:
        require_schema_compatibility(
            connection, supported_versions=frozenset({SCHEMA_VERSION})
        )
    except SchemaMigrationError as error:
        raise ReleaseManagementError(
            f"complete schema v{SCHEMA_VERSION} is required"
        ) from error
    required_columns = {
        "release_id",
        "parent_evaluation_id",
        "review_id",
        "matcher_rule_sha256",
    }
    columns = {
        str(row[1])
        for row in connection.execute("PRAGMA table_info(evaluation_versions)")
    }
    if required_columns - columns:
        raise ReleaseManagementError("schema v9 evaluation columns are incomplete")
    indexes = {
        str(row["name"]): str(row["sql"] or "")
        for row in connection.execute(
            """
            SELECT name,sql FROM sqlite_master
            WHERE type='index' AND tbl_name='evaluation_versions'
            """
        )
    }
    required_indexes = {
        "uq_evaluation_automatic_idempotency": "evaluation_source='automatic'",
        "uq_evaluation_manual_idempotency": "evaluation_source='manual_review'",
        "uq_evaluation_migrated_parent_idempotency": "parent_evaluation_id IS NOT NULL",
    }
    if "uq_evaluation_idempotency" in indexes or any(
        name not in indexes or predicate not in indexes[name]
        for name, predicate in required_indexes.items()
    ):
        raise ReleaseManagementError("schema v9 evaluation indexes are incomplete")
    if connection.execute("PRAGMA foreign_key_check").fetchall():
        raise ReleaseManagementError("schema v9 has foreign-key violations")
    if str(connection.execute("PRAGMA integrity_check").fetchone()[0]) != "ok":
        raise ReleaseManagementError("schema v9 integrity check failed")


def _single_row(
    connection: sqlite3.Connection,
    query: str,
    parameters: Sequence[Any],
    *,
    label: str,
) -> sqlite3.Row:
    rows = connection.execute(query, parameters).fetchall()
    if len(rows) != 1:
        raise ReleaseManagementError(f"exactly one {label} is required")
    return rows[0]


def _target_taxonomy(
    connection: sqlite3.Connection, *, expected_status: str
) -> tuple[sqlite3.Row, str]:
    taxonomy = _single_row(
        connection,
        "SELECT * FROM taxonomy_versions WHERE version=?",
        (TARGET_TAXONOMY_VERSION,),
        label=TARGET_TAXONOMY_VERSION,
    )
    if str(taxonomy["status"]) != expected_status:
        raise ReleaseManagementError(
            f"{TARGET_TAXONOMY_VERSION} must be {expected_status}"
        )
    rows = connection.execute(
        "SELECT * FROM selling_points WHERE taxonomy_id=? ORDER BY code",
        (taxonomy["id"],),
    ).fetchall()
    codes = {str(row["code"]) for row in rows}
    if len(rows) != len(POINT_IDS) or codes != POINT_IDS:
        raise ReleaseManagementError(
            f"target taxonomy must contain {len(POINT_IDS)} approved points"
        )
    if any(int(row["enabled"]) != 1 for row in rows):
        raise ReleaseManagementError("all target taxonomy points must be enabled")
    rules: dict[str, Mapping[str, Any]] = {}
    try:
        for row in rows:
            point = serialize_point_row(connection, taxonomy, row)
            matcher_rule = point["matcher_rule"]
            if not isinstance(matcher_rule, Mapping):
                raise ReleaseManagementError(
                    f"selling point {row['code']} has no matcher rule"
                )
            rules[str(row["code"])] = matcher_rule
    except TaxonomyError as error:
        raise ReleaseManagementError(str(error)) from error
    return taxonomy, taxonomy_matcher_sha256(
        rules, point_spec=V5_2_POINT_SPEC
    )


def _target_taxonomy_semantic_sha256(
    connection: sqlite3.Connection, taxonomy: sqlite3.Row
) -> str:
    points: list[dict[str, Any]] = []
    rows = connection.execute(
        "SELECT * FROM selling_points WHERE taxonomy_id=? ORDER BY code",
        (taxonomy["id"],),
    ).fetchall()
    try:
        for row in rows:
            point = serialize_point_row(connection, taxonomy, row)
            points.append({**point, "enabled": bool(row["enabled"])})
    except TaxonomyError as error:
        raise ReleaseManagementError(str(error)) from error
    payload = {
        "version": str(taxonomy["version"]),
        "definition": str(taxonomy["definition"] or ""),
        "source_path": taxonomy["source_path"],
        "source_sha256": taxonomy["source_sha256"],
        "points": points,
    }
    return _sha256_bytes(canonical_json(payload).encode("utf-8"))


def _require_frozen_table_anchors(
    connection: sqlite3.Connection,
    manifest: FreezeManifest,
    *,
    allow_lifecycle_drift: bool,
) -> None:
    current_tables = {
        str(row[0])
        for row in connection.execute(
            """
            SELECT name FROM sqlite_master
            WHERE type='table' AND name NOT LIKE 'sqlite_%'
            """
        )
    }
    for anchor in manifest.table_anchors:
        if anchor.table not in current_tables:
            raise ReleaseManagementError(
                f"frozen table disappeared after freeze: {anchor.table}"
            )
        current_columns = {
            str(row[1])
            for row in connection.execute(f'PRAGMA table_info("{anchor.table}")')
        }
        if set(anchor.columns) - current_columns:
            raise ReleaseManagementError(
                f"frozen columns disappeared from {anchor.table}"
            )
        columns = (
            anchor.lifecycle_stable_columns if allow_lifecycle_drift else anchor.columns
        )
        expected_sha256 = (
            anchor.lifecycle_stable_rows_sha256
            if allow_lifecycle_drift
            else anchor.rows_sha256
        )
        count, rows_sha256 = _rows_sha256(
            connection,
            table=anchor.table,
            columns=columns,
            where=anchor.where,
            parameters=anchor.parameters,
            normalize_unknown_direction=anchor.table == "content_items",
        )
        if count != anchor.row_count or rows_sha256 != expected_sha256:
            raise ReleaseManagementError(
                f"frozen {anchor.table} rows changed after freeze"
            )


def _require_manifest_identity(
    connection: sqlite3.Connection,
    manifest: FreezeManifest,
    *,
    allow_evaluation_cache_drift: bool = False,
) -> None:
    _require_v9(connection)
    ids = [
        int(row[0])
        for row in connection.execute("SELECT id FROM content_items ORDER BY id")
    ]
    expected_ids = [item.content_id for item in manifest.contents]
    if ids != expected_ids:
        raise ReleaseManagementError(
            "database content IDs differ from the frozen content inventory"
        )
    if ids[-1] != manifest.content_high_water:
        raise ReleaseManagementError("database content high-water has changed")
    _require_frozen_table_anchors(
        connection,
        manifest,
        allow_lifecycle_drift=allow_evaluation_cache_drift,
    )


def _freeze_v1_and_current_evidence_state(
    connection: sqlite3.Connection, content_id: int
) -> tuple[dict[str, Any], str, dict[str, Any], str]:
    _, current_components, current_sha256 = _current_evidence_state(
        connection, content_id, rule_version=V8_RULE_VERSION
    )
    freeze_components = dict(current_components)
    return (
        freeze_components,
        current_sha256,
        current_components,
        current_sha256,
    )


def _verify_freeze_v1_state(
    connection: sqlite3.Connection, contents: Sequence[FrozenContent]
) -> None:
    for item in contents:
        components, evidence_sha256, _, _ = _freeze_v1_and_current_evidence_state(
            connection, item.content_id
        )
        if evidence_sha256 != item.evidence_sha256:
            raise ReleaseManagementError(
                f"content {item.content_id} evidence changed after freeze"
            )
        if canonical_json(components) != canonical_json(item.components):
            raise ReleaseManagementError(
                f"content {item.content_id} evidence components changed after freeze"
            )


def _verify_artifact_inventory(
    connection: sqlite3.Connection, manifest: FreezeManifest
) -> None:
    current = connection.execute(
        """
        SELECT id,content_id,artifact_type,local_path,status,byte_size,sha256,created_at
        FROM evidence_artifacts ORDER BY id
        """
    ).fetchall()
    if len(current) != len(manifest.artifacts):
        raise ReleaseManagementError("database artifact inventory row count changed")
    keys = (
        "id",
        "content_id",
        "artifact_type",
        "local_path",
        "status",
        "byte_size",
        "sha256",
        "created_at",
    )
    for row, frozen in zip(current, manifest.artifacts, strict=True):
        if any(row[key] != frozen.row.get(key) for key in keys):
            raise ReleaseManagementError(
                f"database artifact inventory changed at row {frozen.row.get('id')}"
            )


def _pure_read_preflight(
    db_path: Path,
    manifest: FreezeManifest,
    *,
    allow_evaluation_cache_drift: bool = False,
) -> None:
    with _existing_connection(db_path, read_only=True) as connection:
        before = connection.total_changes
        _require_manifest_identity(
            connection,
            manifest,
            allow_evaluation_cache_drift=allow_evaluation_cache_drift,
        )
        _verify_freeze_v1_state(connection, manifest.contents)
        _verify_artifact_inventory(connection, manifest)
        if connection.total_changes != before:
            raise ReleaseManagementError("release preflight attempted to write")


def _release_row(connection: sqlite3.Connection, release_id: str) -> sqlite3.Row:
    row = connection.execute(
        "SELECT * FROM evaluation_releases WHERE id=?", (release_id,)
    ).fetchone()
    if row is None:
        raise ReleaseManagementError(f"evaluation release does not exist: {release_id}")
    return row


def _require_release_contract(release: sqlite3.Row, *, matcher_sha256: str) -> None:
    if (
        str(release["id"]) != TARGET_RELEASE_ID
        or str(release["rule_version"]) != V8_RULE_VERSION
        or str(release["taxonomy_version"]) != TARGET_TAXONOMY_VERSION
        or str(release["matcher_rule_sha256"]) != matcher_sha256
    ):
        raise ReleaseManagementError("evaluation release contract is inconsistent")


def _audit_prefix(manifest: FreezeManifest) -> str:
    return (
        f"{AUDIT_PREFIX}__{TARGET_RELEASE_ID}__{manifest.logical_snapshot_sha256[:12]}"
    )


def _audit_summary(
    manifest: FreezeManifest, *, created: int = 0, reused: int = 0
) -> dict[str, Any]:
    return {
        "release_id": TARGET_RELEASE_ID,
        "freeze_manifest_sha256": manifest.sha256,
        "content_inventory_sha256": manifest.content_inventory_sha256,
        "content_high_water": manifest.content_high_water,
        "target_count": len(manifest.contents),
        "batch_size": BATCH_SIZE,
        "created": created,
        "reused": reused,
        "freeze_source_database": str(manifest.source_database),
    }


def _checkpoint(_name: str) -> None:
    """Patchable fault-injection point for lifecycle transaction tests."""


def _batched(
    values: Sequence[FrozenContent], size: int = BATCH_SIZE
) -> Iterator[tuple[FrozenContent, ...]]:
    for offset in range(0, len(values), size):
        yield tuple(values[offset : offset + size])


def _cas_release_status(
    connection: sqlite3.Connection,
    *,
    release_id: str,
    old_status: str,
    new_status: str,
    captured_at: str,
    extra_sql: str = "",
    extra_parameters: Sequence[Any] = (),
) -> None:
    cursor = connection.execute(
        f"""
        UPDATE evaluation_releases
        SET status=?,updated_at=?{extra_sql}
        WHERE id=? AND status=?
        """,
        (new_status, captured_at, *extra_parameters, release_id, old_status),
    )
    if cursor.rowcount != 1:
        raise ReleaseManagementError(
            f"release status CAS failed: {release_id} {old_status}->{new_status}"
        )


def _cas_taxonomy_status(
    connection: sqlite3.Connection,
    *,
    version: str,
    old_status: str,
    new_status: str,
    published_at: str | None = None,
) -> None:
    cursor = connection.execute(
        """
        UPDATE taxonomy_versions SET status=?,published_at=COALESCE(?,published_at)
        WHERE version=? AND status=?
        """,
        (new_status, published_at, version, old_status),
    )
    if cursor.rowcount != 1:
        raise ReleaseManagementError(
            f"taxonomy status CAS failed: {version} {old_status}->{new_status}"
        )


def _legacy_window(connection: sqlite3.Connection) -> tuple[sqlite3.Row, sqlite3.Row]:
    taxonomy = _single_row(
        connection,
        "SELECT * FROM taxonomy_versions WHERE version=? AND status='published'",
        (SOURCE_TAXONOMY_VERSION,),
        label="published legacy taxonomy",
    )
    release = _single_row(
        connection,
        "SELECT * FROM evaluation_releases WHERE status='active'",
        (),
        label="active evaluation release",
    )
    if (
        str(release["id"]) != SOURCE_RELEASE_ID
        or str(release["rule_version"]) != SOURCE_RULE_VERSION
        or str(release["taxonomy_version"]) != SOURCE_TAXONOMY_VERSION
    ):
        raise ReleaseManagementError(
            f"source release {SOURCE_RELEASE_ID} must be active"
        )
    return taxonomy, release


def _next_audit_id(connection: sqlite3.Connection, manifest: FreezeManifest) -> str:
    prefix = _audit_prefix(manifest)
    count = int(
        connection.execute(
            "SELECT COUNT(*) FROM migration_audit WHERE id LIKE ?", (f"{prefix}__%",)
        ).fetchone()[0]
    )
    return f"{prefix}__attempt-{count + 1:03d}"


def _json_safe(value: Any) -> Any:
    if isinstance(value, bytes):
        return {"bytes_hex": value.hex()}
    return value


def _rows_sha256(
    connection: sqlite3.Connection,
    *,
    table: str,
    columns: Sequence[str],
    where: str = "",
    parameters: Sequence[Any] = (),
    normalize_unknown_direction: bool = False,
) -> tuple[int, str]:
    quoted_columns = ",".join(f'"{column}"' for column in columns)
    query = f'SELECT {quoted_columns} FROM "{table}" {where} ORDER BY {quoted_columns}'
    digest = hashlib.sha256()
    count = 0
    direction_index = (
        columns.index("manual_content_direction")
        if normalize_unknown_direction and "manual_content_direction" in columns
        else None
    )
    for row in connection.execute(query, parameters):
        values = list(row)
        if direction_index is not None and values[direction_index] == "unknown":
            values[direction_index] = None
        digest.update(
            canonical_json([_json_safe(value) for value in values]).encode("utf-8")
        )
        digest.update(b"\n")
        count += 1
    return count, digest.hexdigest()


def _protected_state(
    connection: sqlite3.Connection,
    manifest: FreezeManifest,
    *,
    envelope_max_id: int | None = None,
    activation_stable: bool = False,
) -> dict[str, Any]:
    """Hash every row outside the lifecycle's explicit write allowlist."""

    if envelope_max_id is None:
        envelope_max_id = int(
            connection.execute(
                "SELECT COALESCE(MAX(id),0) FROM evidence_envelopes"
            ).fetchone()[0]
        )
    table_names = [
        str(row[0])
        for row in connection.execute(
            """
            SELECT name FROM sqlite_master
            WHERE type='table' AND name NOT LIKE 'sqlite_%' ORDER BY name
            """
        )
    ]
    tables: dict[str, dict[str, Any]] = {}
    for table in table_names:
        columns = [
            str(row[1]) for row in connection.execute(f'PRAGMA table_info("{table}")')
        ]
        where = ""
        parameters: tuple[Any, ...] = ()
        if table == "content_items":
            columns = [
                column for column in columns if column != "evaluation_content_direction"
            ]
        elif table == "taxonomy_versions" and activation_stable:
            columns = [
                column for column in columns if column not in {"status", "published_at"}
            ]
        elif table == "evaluation_versions":
            where = "WHERE release_id<>?"
            parameters = (TARGET_RELEASE_ID,)
        elif table == "evaluation_matches":
            where = (
                "WHERE evaluation_id NOT IN "
                "(SELECT id FROM evaluation_versions WHERE release_id=?)"
            )
            parameters = (TARGET_RELEASE_ID,)
        elif table == "evidence_envelopes":
            where = "WHERE id<=?"
            parameters = (envelope_max_id,)
        elif table == "evaluation_releases":
            if activation_stable:
                columns = [
                    "id",
                    "rule_version",
                    "taxonomy_version",
                    "matcher_rule_sha256",
                    "created_at",
                ]
            where = "WHERE id<>?"
            parameters = (TARGET_RELEASE_ID,)
        elif table == "migration_audit":
            where = "WHERE id NOT LIKE ?"
            parameters = (f"{_audit_prefix(manifest)}__%",)
        count, rows_sha256 = _rows_sha256(
            connection,
            table=table,
            columns=columns,
            where=where,
            parameters=parameters,
            normalize_unknown_direction=table == "content_items",
        )
        tables[table] = {"count": count, "rows_sha256": rows_sha256}
    sequence_columns = ["name", "seq"]
    sequence_count, sequence_sha256 = _rows_sha256(
        connection,
        table="sqlite_sequence",
        columns=sequence_columns,
        where=(
            "WHERE name NOT IN "
            "('evaluation_versions','evidence_envelopes','migration_audit')"
        ),
    )
    schema_rows = [
        [_json_safe(value) for value in row]
        for row in connection.execute(
            """
            SELECT type,name,tbl_name,sql FROM sqlite_master
            WHERE name NOT LIKE 'sqlite_autoindex_%'
            ORDER BY type,name,tbl_name,sql
            """
        )
    ]
    payload = {
        "contract": (
            ACTIVATION_STABLE_CONTRACT
            if activation_stable
            else BACKFILL_PROTECTED_CONTRACT
        ),
        "envelope_max_id": envelope_max_id,
        "tables": tables,
        "sqlite_sequence": {
            "count": sequence_count,
            "rows_sha256": sequence_sha256,
        },
        "sqlite_master_sha256": _sha256_bytes(
            canonical_json(schema_rows).encode("utf-8")
        ),
    }
    if activation_stable:
        unmanaged_releases = [
            list(row)
            for row in connection.execute(
                """
                SELECT id,status,updated_at,activated_at,retired_at,failure_reason
                FROM evaluation_releases
                WHERE id NOT IN (?,?) ORDER BY id
                """,
                (TARGET_RELEASE_ID, SOURCE_RELEASE_ID),
            )
        ]
        unmanaged_taxonomies = [
            list(row)
            for row in connection.execute(
                """
                SELECT version,status,published_at FROM taxonomy_versions
                WHERE version NOT IN (?,?) ORDER BY version
                """,
                (SOURCE_TAXONOMY_VERSION, TARGET_TAXONOMY_VERSION),
            )
        ]
        payload["unmanaged_lifecycle_sha256"] = _sha256_bytes(
            canonical_json(
                {
                    "evaluation_releases": unmanaged_releases,
                    "taxonomy_versions": unmanaged_taxonomies,
                }
            ).encode("utf-8")
        )
    payload["state_sha256"] = _sha256_bytes(canonical_json(payload).encode("utf-8"))
    return payload


def _require_protected_state(
    connection: sqlite3.Connection,
    manifest: FreezeManifest,
    expected: Mapping[str, Any],
) -> None:
    envelope_max_id = int(expected.get("envelope_max_id") or 0)
    actual = _protected_state(connection, manifest, envelope_max_id=envelope_max_id)
    if canonical_json(actual) != canonical_json(expected):
        raise ReleaseManagementError("protected database state changed during backfill")


def _require_activation_stable_state(
    connection: sqlite3.Connection,
    manifest: FreezeManifest,
    expected: Mapping[str, Any],
) -> None:
    envelope_max_id = int(expected.get("envelope_max_id") or 0)
    actual = _protected_state(
        connection,
        manifest,
        envelope_max_id=envelope_max_id,
        activation_stable=True,
    )
    if canonical_json(actual) != canonical_json(expected):
        raise ReleaseManagementError(
            "protected database state changed across the activation window"
        )


def _start_audit(connection: sqlite3.Connection, manifest: FreezeManifest) -> str:
    running = connection.execute(
        """
        SELECT * FROM migration_audit
        WHERE id LIKE ? AND status='running' ORDER BY started_at,id
        """,
        (f"{_audit_prefix(manifest)}__%",),
    ).fetchall()
    if len(running) > 1:
        raise ReleaseManagementError("multiple running backfill audits exist")
    if running:
        row = running[0]
        if (
            str(row["baseline_id"])
            != f"dcar-v9-freeze:{manifest.logical_snapshot_sha256}"
            or str(row["source_database"]) != str(manifest.database_backup)
            or str(row["source_sha256"]) != manifest.database_backup_sha256
        ):
            raise ReleaseManagementError("running backfill audit contract differs")
        summary = _json_object(row["summary_json"], label="backfill audit summary")
        protected_state = summary.get("protected_state")
        activation_stable_state = summary.get("activation_stable_state")
        if not isinstance(protected_state, Mapping) or not isinstance(
            activation_stable_state, Mapping
        ):
            raise ReleaseManagementError("running backfill audit lacks protected state")
        _require_protected_state(connection, manifest, protected_state)
        _require_activation_stable_state(connection, manifest, activation_stable_state)
        return str(row["id"])
    audit_id = _next_audit_id(connection, manifest)
    prior = connection.execute(
        """
        SELECT summary_json FROM migration_audit
        WHERE id LIKE ? ORDER BY started_at,id LIMIT 1
        """,
        (f"{_audit_prefix(manifest)}__%",),
    ).fetchone()
    if prior is None:
        protected_state = _protected_state(connection, manifest)
        activation_stable_state = _protected_state(
            connection,
            manifest,
            envelope_max_id=int(protected_state["envelope_max_id"]),
            activation_stable=True,
        )
    else:
        prior_summary = _json_object(
            prior["summary_json"], label="prior backfill audit summary"
        )
        prior_protected_state = prior_summary.get("protected_state")
        prior_activation_stable_state = prior_summary.get("activation_stable_state")
        if not isinstance(prior_protected_state, Mapping) or not isinstance(
            prior_activation_stable_state, Mapping
        ):
            raise ReleaseManagementError("prior backfill audit lacks protected state")
        _require_protected_state(connection, manifest, prior_protected_state)
        _require_activation_stable_state(
            connection, manifest, prior_activation_stable_state
        )
        protected_state = dict(prior_protected_state)
        activation_stable_state = dict(prior_activation_stable_state)
    summary = _audit_summary(manifest)
    summary.update(
        {
            "rehearsal_run_id": os.urandom(16).hex(),
            "target_database": str(
                Path(connection.execute("PRAGMA database_list").fetchone()[2]).resolve()
            ),
            "protected_state": protected_state,
            "activation_stable_state": activation_stable_state,
            "target_db_pre_hash": _sha256_bytes(
                canonical_json(
                    {
                        "target_database": str(
                            Path(
                                connection.execute("PRAGMA database_list").fetchone()[2]
                            ).resolve()
                        ),
                        "protected_state": protected_state,
                    }
                ).encode("utf-8")
            ),
        }
    )
    connection.execute(
        """
        INSERT INTO migration_audit(
            id,baseline_id,source_database,source_sha256,status,summary_json,started_at
        ) VALUES (?,?,?,?, 'running',?,?)
        """,
        (
            audit_id,
            f"dcar-v9-freeze:{manifest.logical_snapshot_sha256}",
            str(manifest.database_backup),
            manifest.database_backup_sha256,
            canonical_json(summary),
            now_utc(),
        ),
    )
    return audit_id


def _mark_audit_failed(db_path: Path, audit_id: str, reason: str) -> None:
    with (
        _existing_connection(db_path, read_only=False) as connection,
        transaction(connection),
    ):
        row = connection.execute(
            "SELECT summary_json FROM migration_audit WHERE id=? AND status='running'",
            (audit_id,),
        ).fetchone()
        if row is None:
            raise ReleaseManagementError("backfill audit is no longer running")
        summary = _json_object(row["summary_json"], label="backfill audit summary")
        summary["error"] = reason
        cursor = connection.execute(
            """
            UPDATE migration_audit
            SET status='failed',summary_json=?,completed_at=?
            WHERE id=? AND status='running'
            """,
            (
                canonical_json(summary),
                now_utc(),
                audit_id,
            ),
        )
        if cursor.rowcount != 1:
            raise ReleaseManagementError("backfill audit failure CAS failed")


def _require_operational_baseline(
    connection: sqlite3.Connection, manifest: FreezeManifest
) -> dict[str, int]:
    result: dict[str, int] = {}
    for table in ("provider_usage", "scheduler_runs", "report_revisions"):
        if table not in manifest.table_counts:
            raise ReleaseManagementError(f"freeze manifest lacks {table} baseline")
        count = int(connection.execute(f"SELECT COUNT(*) FROM {table}").fetchone()[0])
        if count != manifest.table_counts[table]:
            raise ReleaseManagementError(f"{table} changed after freeze")
        result[table] = count
    for table, join in (
        ("evaluation_versions", "WHERE release_id<>?"),
        (
            "evaluation_matches",
            "WHERE evaluation_id NOT IN (SELECT id FROM evaluation_versions WHERE release_id=?)",
        ),
    ):
        if table in manifest.table_counts:
            count = int(
                connection.execute(
                    f"SELECT COUNT(*) FROM {table} {join}", (TARGET_RELEASE_ID,)
                ).fetchone()[0]
            )
            if count != manifest.table_counts[table]:
                raise ReleaseManagementError(
                    f"legacy {table} rows changed after freeze"
                )
    return result


def status(*, db_path: Path, release_id: str = TARGET_RELEASE_ID) -> dict[str, Any]:
    """Inspect release state without creating or mutating the database."""

    with _existing_connection(db_path, read_only=True) as connection:
        _require_v9(connection)
        release = connection.execute(
            "SELECT * FROM evaluation_releases WHERE id=?", (release_id,)
        ).fetchone()
        counts = connection.execute(
            """
            SELECT evaluation_source,
                   COUNT(*) AS total,
                   SUM(CASE WHEN invalidated_at IS NULL THEN 1 ELSE 0 END) AS valid
            FROM evaluation_versions WHERE release_id=?
            GROUP BY evaluation_source ORDER BY evaluation_source
            """,
            (release_id,),
        ).fetchall()
        audits = connection.execute(
            """
            SELECT * FROM migration_audit WHERE id LIKE ? ORDER BY started_at,id
            """,
            (f"{AUDIT_PREFIX}__{release_id}__%",),
        ).fetchall()
        return {
            "release": dict(release) if release is not None else None,
            "evaluation_counts": [dict(row) for row in counts],
            "active_releases": [
                dict(row)
                for row in connection.execute(
                    "SELECT * FROM evaluation_releases WHERE status='active' ORDER BY id"
                )
            ],
            "published_taxonomies": [
                dict(row)
                for row in connection.execute(
                    "SELECT * FROM taxonomy_versions WHERE status='published' ORDER BY version"
                )
            ],
            "audits": [dict(row) for row in audits],
        }


def create(
    *, db_path: Path, manifest_path: Path, release_id: str = TARGET_RELEASE_ID
) -> dict[str, Any]:
    """Create the fixed evaluation-v8/v5.2 release in draft status."""

    if release_id != TARGET_RELEASE_ID:
        raise ReleaseManagementError("only the approved target release id is allowed")
    manifest = _load_freeze_manifest(manifest_path)
    _pure_read_preflight(db_path, manifest)
    with (
        _existing_connection(db_path, read_only=False) as connection,
        transaction(connection),
    ):
        _require_manifest_identity(connection, manifest)
        _verify_freeze_v1_state(connection, manifest.contents)
        _verify_artifact_inventory(connection, manifest)
        _legacy_window(connection)
        _, matcher_sha256 = _target_taxonomy(connection, expected_status="draft")
        by_id = connection.execute(
            "SELECT * FROM evaluation_releases WHERE id=?", (release_id,)
        ).fetchone()
        by_pair = connection.execute(
            """
            SELECT * FROM evaluation_releases
            WHERE rule_version=? AND taxonomy_version=?
            """,
            (V8_RULE_VERSION, TARGET_TAXONOMY_VERSION),
        ).fetchone()
        if by_id is not None or by_pair is not None:
            if (
                by_id is None
                or by_pair is None
                or str(by_id["id"]) != str(by_pair["id"])
            ):
                raise ReleaseManagementError(
                    "target release id or version pair is occupied"
                )
            _require_release_contract(by_id, matcher_sha256=matcher_sha256)
            if str(by_id["status"]) != "draft":
                raise ReleaseManagementError("existing target release is not draft")
        else:
            captured_at = now_utc()
            connection.execute(
                """
                INSERT INTO evaluation_releases(
                    id,rule_version,taxonomy_version,matcher_rule_sha256,status,
                    created_at,updated_at
                ) VALUES (?,?,?,?, 'draft',?,?)
                """,
                (
                    release_id,
                    V8_RULE_VERSION,
                    TARGET_TAXONOMY_VERSION,
                    matcher_sha256,
                    captured_at,
                    captured_at,
                ),
            )
    return status(db_path=db_path, release_id=release_id)


def backfill(
    *, db_path: Path, manifest_path: Path, release_id: str = TARGET_RELEASE_ID
) -> dict[str, Any]:
    """Evaluate the exact frozen inventory in committed batches of 250."""

    if release_id != TARGET_RELEASE_ID:
        raise ReleaseManagementError("only the approved target release id is allowed")
    manifest = _load_freeze_manifest(manifest_path)
    _pure_read_preflight(db_path, manifest)
    audit_id = ""
    try:
        with (
            _existing_connection(db_path, read_only=False) as connection,
            transaction(connection),
        ):
            _require_manifest_identity(connection, manifest)
            _verify_freeze_v1_state(connection, manifest.contents)
            _verify_artifact_inventory(connection, manifest)
            _, matcher_sha256 = _target_taxonomy(connection, expected_status="draft")
            release = _release_row(connection, release_id)
            _require_release_contract(release, matcher_sha256=matcher_sha256)
            release_status = str(release["status"])
            if release_status == "draft":
                _cas_release_status(
                    connection,
                    release_id=release_id,
                    old_status="draft",
                    new_status="backfilling",
                    captured_at=now_utc(),
                )
            elif release_status != "backfilling":
                raise ReleaseManagementError("release must be draft or backfilling")
            audit_id = _start_audit(connection, manifest)

        created = 0
        reused = 0
        batch_count = 0
        for batch in _batched(manifest.contents):
            with (
                _existing_connection(db_path, read_only=False) as connection,
                transaction(connection),
            ):
                _require_manifest_identity(connection, manifest)
                _verify_freeze_v1_state(connection, batch)
                _verify_artifact_inventory(connection, manifest)
                _, matcher_sha256 = _target_taxonomy(
                    connection, expected_status="draft"
                )
                release = _release_row(connection, release_id)
                _require_release_contract(release, matcher_sha256=matcher_sha256)
                if str(release["status"]) != "backfilling":
                    raise ReleaseManagementError("release left backfilling state")
                for item in batch:
                    result = _evaluate_content(
                        item.content_id,
                        db_path=db_path,
                        source="automatic",
                        _release_id=release_id,
                        _connection=connection,
                    )
                    if result.created:
                        created += 1
                    else:
                        reused += 1
                batch_count += 1
                evaluated = int(
                    connection.execute(
                        """
                        SELECT COUNT(*) FROM evaluation_versions
                        WHERE release_id=? AND evaluation_source='automatic'
                          AND invalidated_at IS NULL
                        """,
                        (release_id,),
                    ).fetchone()[0]
                )
                audit = connection.execute(
                    "SELECT summary_json FROM migration_audit WHERE id=? AND status='running'",
                    (audit_id,),
                ).fetchone()
                if audit is None:
                    raise ReleaseManagementError("running backfill audit disappeared")
                summary = _json_object(
                    audit["summary_json"], label="backfill audit summary"
                )
                protected_state = summary.get("protected_state")
                if not isinstance(protected_state, Mapping):
                    raise ReleaseManagementError("backfill audit lacks protected state")
                _require_protected_state(connection, manifest, protected_state)
                summary.update({"created": created, "reused": reused})
                summary.update(
                    {"completed_batches": batch_count, "evaluated_count": evaluated}
                )
                cursor = connection.execute(
                    """
                    UPDATE migration_audit SET summary_json=?
                    WHERE id=? AND status='running'
                    """,
                    (canonical_json(summary), audit_id),
                )
                if cursor.rowcount != 1:
                    raise ReleaseManagementError("backfill audit update CAS failed")
                _checkpoint("backfill_batch_committed")
        return {
            "release_id": release_id,
            "audit_id": audit_id,
            "batch_size": BATCH_SIZE,
            "batch_count": batch_count,
            "created": created,
            "reused": reused,
            "target_count": len(manifest.contents),
        }
    except Exception as error:
        if audit_id:
            try:
                _mark_audit_failed(db_path, audit_id, str(error))
            except Exception as audit_error:
                raise ReleaseManagementError(
                    f"backfill failed ({error}); audit finalization failed ({audit_error})"
                ) from error
        raise


def _json_object(value: Any, *, label: str) -> dict[str, Any]:
    try:
        parsed = json.loads(str(value))
    except (json.JSONDecodeError, TypeError, ValueError) as error:
        raise ReleaseManagementError(f"{label} is not valid JSON") from error
    if not isinstance(parsed, dict):
        raise ReleaseManagementError(f"{label} must be a JSON object")
    return parsed


def _legacy_migration_semantic_sha256(connection: sqlite3.Connection) -> str:
    projections: dict[str, Any] = {}
    filters: dict[str, tuple[str, tuple[Any, ...]]] = {
        "evaluation_versions": ("WHERE release_id<>?", (TARGET_RELEASE_ID,)),
        "evaluation_matches": (
            "WHERE evaluation_id NOT IN ("
            "SELECT id FROM evaluation_versions WHERE release_id=?"
            ")",
            (TARGET_RELEASE_ID,),
        ),
    }
    for table in (
        "evaluation_versions",
        "evaluation_matches",
        "review_queue",
        "evaluation_reviews",
        "review_reopen_events",
        "manual_evidence",
        "report_revisions",
        "report_files",
    ):
        columns = tuple(
            str(row[1]) for row in connection.execute(f'PRAGMA table_info("{table}")')
        )
        where, parameters = filters.get(table, ("", ()))
        count, rows_sha256 = _rows_sha256(
            connection,
            table=table,
            columns=columns,
            where=where,
            parameters=parameters,
        )
        projections[table] = {"count": count, "rows_sha256": rows_sha256}
    projections["legacy_releases"] = [
        list(row)
        for row in connection.execute(
            """
            SELECT id,rule_version,taxonomy_version,matcher_rule_sha256
            FROM evaluation_releases WHERE id<>? ORDER BY id
            """,
            (TARGET_RELEASE_ID,),
        )
    ]
    return _sha256_bytes(canonical_json(projections).encode("utf-8"))


def _semantic_core(
    connection: sqlite3.Connection,
    manifest: FreezeManifest,
    *,
    expected_taxonomy_status: str,
) -> dict[str, Any]:
    _require_manifest_identity(
        connection,
        manifest,
        allow_evaluation_cache_drift=expected_taxonomy_status == "published",
    )
    _verify_freeze_v1_state(connection, manifest.contents)
    _verify_artifact_inventory(connection, manifest)
    taxonomy, matcher_sha256 = _target_taxonomy(
        connection, expected_status=expected_taxonomy_status
    )
    taxonomy_semantic_sha256 = _target_taxonomy_semantic_sha256(connection, taxonomy)
    release = _release_row(connection, TARGET_RELEASE_ID)
    _require_release_contract(release, matcher_sha256=matcher_sha256)
    evaluations = connection.execute(
        "SELECT * FROM evaluation_versions WHERE release_id=? ORDER BY content_id,id",
        (TARGET_RELEASE_ID,),
    ).fetchall()
    if len(evaluations) != len(manifest.contents):
        raise ReleaseManagementError(
            "target release does not have one evaluation per content"
        )
    expected_ids = [item.content_id for item in manifest.contents]
    actual_ids = [int(row["content_id"]) for row in evaluations]
    if actual_ids != expected_ids or len(set(actual_ids)) != len(actual_ids):
        raise ReleaseManagementError("target release content coverage is not exact")
    if any(
        str(row["evaluation_source"]) != "automatic"
        or row["parent_evaluation_id"] is not None
        or row["review_id"] is not None
        or row["invalidated_at"] is not None
        or row["invalidation_reason"] is not None
        for row in evaluations
    ):
        raise ReleaseManagementError(
            "target release contains non-automatic or invalid rows"
        )

    evaluation_by_id = {int(row["id"]): row for row in evaluations}
    match_rows: dict[int, list[sqlite3.Row]] = {
        evaluation_id: [] for evaluation_id in evaluation_by_id
    }
    for match in connection.execute(
        """
        SELECT m.* FROM evaluation_matches m
        JOIN evaluation_versions e ON e.id=m.evaluation_id
        WHERE e.release_id=?
        ORDER BY m.evaluation_id,
                 CASE m.match_role WHEN 'primary' THEN 0 ELSE 1 END,
                 m.selling_point_code
        """,
        (TARGET_RELEASE_ID,),
    ):
        match_rows[int(match["evaluation_id"])].append(match)
    allowed_scenes: dict[str, set[str]] = {}
    for row in connection.execute(
        """
        SELECT sp.code,sps.scene FROM selling_points sp
        JOIN taxonomy_versions tv ON tv.id=sp.taxonomy_id
        JOIN selling_point_scenes sps ON sps.selling_point_id=sp.id
        WHERE tv.version=? ORDER BY sp.code,sps.scene
        """,
        (TARGET_TAXONOMY_VERSION,),
    ):
        allowed_scenes.setdefault(str(row["code"]), set()).add(str(row["scene"]))

    semantic_rows: list[dict[str, Any]] = []
    evidence_levels: Counter[str] = Counter()
    evaluation_statuses: Counter[str] = Counter()
    directions: Counter[str] = Counter()
    primary_codes: Counter[str] = Counter()
    included_count = 0
    pending_count = 0
    for frozen, evaluation in zip(manifest.contents, evaluations, strict=True):
        _, _, current_components, current_evidence_sha256 = (
            _freeze_v1_and_current_evidence_state(connection, frozen.content_id)
        )
        if str(evaluation["evidence_sha256"]) != current_evidence_sha256:
            raise ReleaseManagementError(
                f"content {frozen.content_id} v2 evidence hash does not match evaluation"
            )
        envelope_id = evaluation["evidence_envelope_id"]
        if envelope_id is None:
            raise ReleaseManagementError(
                f"content {frozen.content_id} evaluation has no evidence envelope"
            )
        envelope = connection.execute(
            "SELECT * FROM evidence_envelopes WHERE id=?", (envelope_id,)
        ).fetchone()
        if (
            envelope is None
            or int(envelope["content_id"]) != frozen.content_id
            or str(envelope["schema_version"]) != EVIDENCE_VERSION
            or str(envelope["evidence_sha256"]) != current_evidence_sha256
            or canonical_json(
                _json_object(envelope["components_json"], label="envelope components")
            )
            != canonical_json(current_components)
            or any(
                envelope[key] != current_components[key]
                for key in EVIDENCE_COMPONENT_KEYS
            )
        ):
            raise ReleaseManagementError(
                f"content {frozen.content_id} evidence envelope is inconsistent"
            )
        if (
            str(evaluation["rule_version"]) != V8_RULE_VERSION
            or str(evaluation["taxonomy_version"]) != TARGET_TAXONOMY_VERSION
            or str(evaluation["matcher_rule_sha256"]) != matcher_sha256
        ):
            raise ReleaseManagementError(
                f"content {frozen.content_id} release fields are inconsistent"
            )

        payload = _json_object(
            evaluation["payload_json"], label=f"content {frozen.content_id} payload"
        )
        matches: list[dict[str, Any]] = []
        primary_matches = 0
        for match in match_rows[int(evaluation["id"])]:
            code = str(match["selling_point_code"])
            scene = str(match["scene"])
            role = str(match["match_role"])
            evidence = _json_object(
                match["evidence_json"], label=f"content {frozen.content_id} match"
            )
            if code not in POINT_IDS or scene not in allowed_scenes.get(code, set()):
                raise ReleaseManagementError(
                    f"content {frozen.content_id} has an illegal point/scene match"
                )
            if (
                str(evidence.get("id") or "") != code
                or str(evidence.get("scene") or "") != scene
                or int(evidence.get("score") or 0) != int(match["score"] or 0)
            ):
                raise ReleaseManagementError(
                    f"content {frozen.content_id} match evidence is inconsistent"
                )
            primary_matches += int(role == "primary")
            matches.append(
                {
                    "selling_point_code": code,
                    "scene": scene,
                    "match_role": role,
                    "score": match["score"],
                    "evidence": evidence,
                }
            )
        primary_code = evaluation["primary_selling_point_code"]
        if (
            len(matches) > 3
            or sum(int(item["match_role"] == "secondary") for item in matches) > 2
        ):
            raise ReleaseManagementError(
                f"content {frozen.content_id} has too many selling-point matches"
            )
        if primary_code is None:
            if (
                matches
                or evaluation["selling_point_score"] is not None
                or int(evaluation["selling_point_included"])
            ):
                raise ReleaseManagementError(
                    f"content {frozen.content_id} has matches without a primary point"
                )
        elif (
            primary_matches != 1
            or not matches
            or matches[0]["match_role"] != "primary"
            or matches[0]["selling_point_code"] != str(primary_code)
        ):
            raise ReleaseManagementError(
                f"content {frozen.content_id} primary match is inconsistent"
            )
        elif (
            matches[0]["score"] != evaluation["selling_point_score"]
            or matches[0]["scene"] != evaluation["content_direction"]
        ):
            raise ReleaseManagementError(
                f"content {frozen.content_id} primary score/scene is inconsistent"
            )
        payload_matches = payload.get("matches")
        if not isinstance(payload_matches, list) or len(payload_matches) != len(
            matches
        ):
            raise ReleaseManagementError(
                f"content {frozen.content_id} payload matches are inconsistent"
            )
        evidence_by_code = {
            str(item["selling_point_code"]): item["evidence"] for item in matches
        }
        if any(
            not isinstance(item, Mapping)
            or canonical_json(item)
            != canonical_json(evidence_by_code.get(str(item.get("id") or "")))
            for item in payload_matches
        ):
            raise ReleaseManagementError(
                f"content {frozen.content_id} payload/match evidence differs"
            )
        expected_payload = {
            "evaluation_status": evaluation["evaluation_status"],
            "evidence_level": evaluation["evidence_level"],
            "primary_selling_point_id": str(primary_code or ""),
            "selling_point_score": evaluation["selling_point_score"],
            "selling_point_included": bool(evaluation["selling_point_included"]),
            "pending_review": bool(evaluation["pending_review"]),
            "content_direction": evaluation["content_direction"],
            "content_automotive_score": evaluation["content_automotive_score"],
            "audience_automotive_score": evaluation["audience_automotive_score"],
            "acquisition_potential": evaluation["acquisition_potential_score"],
            "evaluation_source": "automatic",
            "release_id": TARGET_RELEASE_ID,
        }
        if any(payload.get(key) != value for key, value in expected_payload.items()):
            raise ReleaseManagementError(
                f"content {frozen.content_id} payload columns are inconsistent"
            )
        included = bool(evaluation["selling_point_included"])
        pending = bool(evaluation["pending_review"])
        evidence_level = str(evaluation["evidence_level"])
        evaluation_status = str(evaluation["evaluation_status"])
        score = evaluation["selling_point_score"]
        if evidence_level in {"V0", "V1"}:
            gate_is_valid = (
                evaluation_status == "insufficient_evidence"
                and primary_code is None
                and score is None
                and not included
                and pending
            )
        else:
            expected_included = score is not None and int(score) >= INCLUDE_MIN
            expected_pending = (
                score is not None and REVIEW_MIN <= int(score) < INCLUDE_MIN
            )
            gate_is_valid = (
                evidence_level in {"V2", "V3"}
                and evaluation_status == "evaluated"
                and included == expected_included
                and pending == expected_pending
            )
        if not gate_is_valid:
            raise ReleaseManagementError(
                f"content {frozen.content_id} inclusion gate is inconsistent"
            )
        evidence_levels[evidence_level] += 1
        evaluation_statuses[evaluation_status] += 1
        directions[str(evaluation["content_direction"])] += 1
        primary_codes[str(primary_code or "none")] += 1
        included_count += int(included)
        pending_count += int(pending)
        semantic_rows.append(
            {
                "content_id": frozen.content_id,
                "freeze_text_contract": "freeze-text-v1",
                "current_text_contract": "text-evidence-v2",
                "evidence_sha256": current_evidence_sha256,
                "evidence_components": current_components,
                "evidence_envelope_schema": envelope["schema_version"],
                "evaluation_status": evaluation["evaluation_status"],
                "evidence_level": evaluation["evidence_level"],
                "primary_selling_point_code": primary_code,
                "selling_point_score": evaluation["selling_point_score"],
                "selling_point_included": included,
                "content_direction": evaluation["content_direction"],
                "content_automotive_score": evaluation["content_automotive_score"],
                "audience_automotive_score": evaluation["audience_automotive_score"],
                "acquisition_potential_score": evaluation[
                    "acquisition_potential_score"
                ],
                "pending_review": pending,
                "payload": payload,
                "matches": matches,
            }
        )
    integrity = str(connection.execute("PRAGMA integrity_check").fetchone()[0])
    foreign_keys = connection.execute("PRAGMA foreign_key_check").fetchall()
    if integrity != "ok" or foreign_keys:
        raise ReleaseManagementError("database integrity checks failed")
    operational_counts = _require_operational_baseline(connection, manifest)
    metrics = {
        "evaluation_status": dict(sorted(evaluation_statuses.items())),
        "evidence_level": dict(sorted(evidence_levels.items())),
        "content_direction": dict(sorted(directions.items())),
        "primary_selling_point": dict(sorted(primary_codes.items())),
        "included": included_count,
        "pending_review": pending_count,
    }
    content_ids = [item.content_id for item in manifest.contents]
    freeze_v1_rows = [
        {
            "content_id": item.content_id,
            "evidence_sha256": item.evidence_sha256,
            "components": item.components,
            "envelope_state": item.envelope_state,
        }
        for item in manifest.contents
    ]
    v2_evidence_rows = [
        {
            "content_id": item["content_id"],
            "evidence_sha256": item["evidence_sha256"],
            "components": item["evidence_components"],
            "envelope_schema": item["evidence_envelope_schema"],
        }
        for item in semantic_rows
    ]
    evaluation_result_rows = [
        {
            key: value
            for key, value in item.items()
            if key
            not in {
                "freeze_text_contract",
                "current_text_contract",
                "evidence_components",
                "evidence_envelope_schema",
            }
        }
        for item in semantic_rows
    ]
    implementation_hashes = {
        name: _sha256_file(Path(__file__).with_name(name))
        for name in (
            "evaluation.py",
            "matcher_dsl.py",
            "release_management.py",
            "taxonomy.py",
        )
    }
    return {
        "freeze_manifest_sha256": manifest.sha256,
        "logical_snapshot_sha256": manifest.logical_snapshot_sha256,
        "content_inventory_sha256": manifest.content_inventory_sha256,
        "artifact_inventory_sha256": manifest.artifact_inventory_sha256,
        "database_backup_sha256": manifest.database_backup_sha256,
        "freeze_evidence_contract": "freeze-text-v1",
        "evaluation_evidence_contract": "text-evidence-v2",
        "implementation_sha256": _sha256_bytes(
            canonical_json(implementation_hashes).encode("utf-8")
        ),
        "implementation_files": implementation_hashes,
        "release": {
            "id": TARGET_RELEASE_ID,
            "rule_version": V8_RULE_VERSION,
            "taxonomy_version": TARGET_TAXONOMY_VERSION,
            "matcher_rule_sha256": matcher_sha256,
        },
        "target_taxonomy_semantic_sha256": taxonomy_semantic_sha256,
        "target_count": len(manifest.contents),
        "content_high_water": manifest.content_high_water,
        "frozen_envelope_states": dict(
            sorted(Counter(item.envelope_state for item in manifest.contents).items())
        ),
        "operational_counts": operational_counts,
        "metrics": metrics,
        "content_ids_sha256": _sha256_bytes(
            canonical_json(content_ids).encode("utf-8")
        ),
        "freeze_v1_semantic_sha256": _sha256_bytes(
            canonical_json(freeze_v1_rows).encode("utf-8")
        ),
        "v2_evidence_semantic_sha256": _sha256_bytes(
            canonical_json(v2_evidence_rows).encode("utf-8")
        ),
        "evaluation_result_semantic_sha256": _sha256_bytes(
            canonical_json(evaluation_result_rows).encode("utf-8")
        ),
        "legacy_migration_semantic_sha256": _legacy_migration_semantic_sha256(
            connection
        ),
        "semantic_sha256": _sha256_bytes(canonical_json(semantic_rows).encode("utf-8")),
    }


def _receipt_payload(
    core: Mapping[str, Any], execution: Mapping[str, Any]
) -> dict[str, Any]:
    core_value = dict(core)
    execution_value = dict(execution)
    return {
        "schema_version": RECEIPT_SCHEMA_VERSION,
        "generated_at": now_utc(),
        "core_sha256": _sha256_bytes(canonical_json(core_value).encode("utf-8")),
        "execution_sha256": _sha256_bytes(
            canonical_json(execution_value).encode("utf-8")
        ),
        "core": core_value,
        "execution": execution_value,
    }


def _read_receipt(path: Path) -> dict[str, Any]:
    receipt, _ = _read_json_object(path, label="release receipt")
    if receipt.get("schema_version") != RECEIPT_SCHEMA_VERSION:
        raise ReleaseManagementError("unsupported release receipt schema")
    core = receipt.get("core")
    execution = receipt.get("execution")
    if not isinstance(core, Mapping) or not isinstance(execution, Mapping):
        raise ReleaseManagementError("release receipt has no semantic core")
    expected = _sha256_bytes(canonical_json(core).encode("utf-8"))
    if receipt.get("core_sha256") != expected:
        raise ReleaseManagementError("release receipt core hash mismatch")
    expected_execution = _sha256_bytes(canonical_json(execution).encode("utf-8"))
    if receipt.get("execution_sha256") != expected_execution:
        raise ReleaseManagementError("release receipt execution hash mismatch")
    return receipt


def _write_receipt(path: Path, payload: Mapping[str, Any]) -> dict[str, Any]:
    path = path.resolve()
    if not path.parent.is_dir():
        raise ReleaseManagementError("release receipt parent directory does not exist")
    if path.exists():
        existing = _read_receipt(path)
        if canonical_json(existing["core"]) != canonical_json(
            payload["core"]
        ) or canonical_json(existing["execution"]) != canonical_json(
            payload["execution"]
        ):
            raise ReleaseManagementError("refusing to overwrite a different receipt")
        return existing
    encoded = (
        json.dumps(payload, ensure_ascii=False, indent=2, sort_keys=True) + "\n"
    ).encode("utf-8")
    temporary_name = ""
    try:
        with tempfile.NamedTemporaryFile(
            mode="wb", dir=path.parent, prefix=f".{path.name}.", delete=False
        ) as handle:
            handle.write(encoded)
            handle.flush()
            os.fsync(handle.fileno())
            temporary_name = handle.name
        os.replace(temporary_name, path)
    except OSError as error:
        if temporary_name:
            Path(temporary_name).unlink(missing_ok=True)
        raise ReleaseManagementError(
            f"cannot write release receipt: {error}"
        ) from error
    return dict(payload)


def _latest_running_audit(
    connection: sqlite3.Connection, manifest: FreezeManifest
) -> sqlite3.Row:
    return _single_row(
        connection,
        """
        SELECT * FROM migration_audit
        WHERE id=(
            SELECT id FROM migration_audit
            WHERE id LIKE ? AND status='running'
            ORDER BY started_at DESC,id DESC LIMIT 1
        )
        """,
        (f"{_audit_prefix(manifest)}__%",),
        label="running release backfill audit",
    )


def _latest_succeeded_audit(
    connection: sqlite3.Connection, manifest: FreezeManifest
) -> sqlite3.Row:
    return _single_row(
        connection,
        """
        SELECT * FROM migration_audit
        WHERE id=(
            SELECT id FROM migration_audit
            WHERE id LIKE ? AND status='succeeded'
            ORDER BY completed_at DESC,id DESC LIMIT 1
        )
        """,
        (f"{_audit_prefix(manifest)}__%",),
        label="succeeded release backfill audit",
    )


def _audit_execution(audit: sqlite3.Row) -> tuple[dict[str, Any], dict[str, Any]]:
    summary = _json_object(audit["summary_json"], label="backfill audit summary")
    protected_state = summary.get("protected_state")
    stable_state = summary.get("activation_stable_state")
    required = ("rehearsal_run_id", "target_database", "target_db_pre_hash")
    if (
        not isinstance(protected_state, Mapping)
        or any(not summary.get(key) for key in required)
        or not isinstance(stable_state, Mapping)
    ):
        raise ReleaseManagementError("backfill audit lacks execution attestation")
    return summary, {
        "audit_id": str(audit["id"]),
        "rehearsal_run_id": str(summary["rehearsal_run_id"]),
        "target_database": str(summary["target_database"]),
        "target_db_pre_hash": str(summary["target_db_pre_hash"]),
        "activation_stable_state_sha256": str(stable_state["state_sha256"]),
    }


def _require_stored_receipt_attestation(
    receipt: Mapping[str, Any],
    *,
    expected_mode: str,
    expected_database: Path | None = None,
) -> dict[str, Any]:
    """Bind a receipt to its succeeded audit instead of trusting JSON alone."""

    core = receipt.get("core")
    execution = receipt.get("execution")
    if not isinstance(core, Mapping) or not isinstance(execution, Mapping):
        raise ReleaseManagementError("release receipt attestation is incomplete")
    if str(execution.get("mode") or "") != expected_mode:
        raise ReleaseManagementError(
            f"release receipt must be a {expected_mode} receipt"
        )
    target_database_text = str(execution.get("target_database") or "")
    audit_id = str(execution.get("audit_id") or "")
    if not target_database_text or not audit_id:
        raise ReleaseManagementError("release receipt has no database/audit identity")
    target_database = Path(target_database_text).resolve()
    if expected_database is not None and target_database != expected_database.resolve():
        raise ReleaseManagementError("release receipt belongs to a different database")
    if not target_database.is_file():
        raise ReleaseManagementError(
            f"attested release database no longer exists: {target_database}"
        )
    with _existing_connection(target_database, read_only=True) as connection:
        _require_v9(connection)
        audit = connection.execute(
            "SELECT * FROM migration_audit WHERE id=?", (audit_id,)
        ).fetchone()
        if audit is None or str(audit["status"]) != "succeeded":
            raise ReleaseManagementError("release receipt audit is not succeeded")
        summary = _json_object(
            audit["summary_json"], label="attested backfill audit summary"
        )
        if (
            str(audit["baseline_id"])
            != f"dcar-v9-freeze:{core.get('logical_snapshot_sha256')}"
            or str(audit["source_sha256"])
            != str(core.get("database_backup_sha256") or "")
            or summary.get("receipt_core_sha256") != receipt.get("core_sha256")
            or summary.get("receipt_execution_sha256")
            != receipt.get("execution_sha256")
            or canonical_json(summary.get("semantic_core")) != canonical_json(core)
            or canonical_json(summary.get("receipt_execution"))
            != canonical_json(execution)
        ):
            raise ReleaseManagementError(
                "release receipt does not match its succeeded audit"
            )
    return dict(execution)


def _require_production_receipt_chain(
    receipt: Mapping[str, Any], *, expected_database: Path
) -> dict[str, Any]:
    execution = _require_stored_receipt_attestation(
        receipt,
        expected_mode="production",
        expected_database=expected_database,
    )
    approved = execution.get("approved_rehearsals")
    if not isinstance(approved, list) or len(approved) != 2:
        raise ReleaseManagementError(
            "production receipt must attest two rehearsal receipts"
        )
    identities: set[tuple[str, str, str]] = set()
    for index, record in enumerate(approved, 1):
        if not isinstance(record, Mapping):
            raise ReleaseManagementError(
                f"approved rehearsal attestation {index} is invalid"
            )
        path = Path(str(record.get("receipt_path") or "")).resolve()
        if not path.is_file() or _sha256_file(path) != str(
            record.get("receipt_file_sha256") or ""
        ):
            raise ReleaseManagementError(
                f"approved rehearsal receipt {index} changed or disappeared"
            )
        rehearsal = _read_receipt(path)
        if (
            rehearsal.get("core_sha256") != record.get("core_sha256")
            or rehearsal.get("execution_sha256") != record.get("execution_sha256")
            or canonical_json(rehearsal.get("core"))
            != canonical_json(receipt.get("core"))
        ):
            raise ReleaseManagementError(
                f"approved rehearsal receipt {index} no longer matches production"
            )
        rehearsal_execution = _require_stored_receipt_attestation(
            rehearsal, expected_mode="rehearsal"
        )
        identity = (
            str(rehearsal_execution["rehearsal_run_id"]),
            str(rehearsal_execution["target_database"]),
            str(rehearsal_execution["target_db_pre_hash"]),
        )
        if any(
            str(record.get(key) or "") != value
            for key, value in zip(
                ("rehearsal_run_id", "target_database", "target_db_pre_hash"),
                identity,
                strict=True,
            )
        ):
            raise ReleaseManagementError(
                f"approved rehearsal receipt {index} identity changed"
            )
        identities.add(identity)
    if len(identities) != 2:
        raise ReleaseManagementError(
            "production receipt does not contain two independent rehearsals"
        )
    return execution


def _attested_activation_stable_state(
    connection: sqlite3.Connection,
    manifest: FreezeManifest,
    receipt: Mapping[str, Any],
) -> Mapping[str, Any]:
    execution = receipt.get("execution")
    if not isinstance(execution, Mapping):
        raise ReleaseManagementError("production receipt execution is missing")
    audit = connection.execute(
        "SELECT * FROM migration_audit WHERE id=?",
        (str(execution.get("audit_id") or ""),),
    ).fetchone()
    if audit is None or str(audit["status"]) != "succeeded":
        raise ReleaseManagementError("production receipt audit is not succeeded")
    summary = _json_object(
        audit["summary_json"], label="production backfill audit summary"
    )
    stable_state = summary.get("activation_stable_state")
    if (
        not isinstance(stable_state, Mapping)
        or summary.get("receipt_core_sha256") != receipt.get("core_sha256")
        or summary.get("receipt_execution_sha256") != receipt.get("execution_sha256")
        or canonical_json(summary.get("semantic_core"))
        != canonical_json(receipt.get("core"))
        or canonical_json(summary.get("receipt_execution")) != canonical_json(execution)
    ):
        raise ReleaseManagementError("production receipt audit attestation changed")
    _require_activation_stable_state(connection, manifest, stable_state)
    return stable_state


def verify_ready(
    *,
    db_path: Path,
    manifest_path: Path,
    receipt_path: Path,
    rehearsal_receipt_paths: Sequence[Path] = (),
    production: bool = False,
    release_id: str = TARGET_RELEASE_ID,
) -> dict[str, Any]:
    """Validate the complete backfill and CAS it from backfilling to ready."""

    if release_id != TARGET_RELEASE_ID:
        raise ReleaseManagementError("only the approved target release id is allowed")
    manifest = _load_freeze_manifest(manifest_path)
    _pure_read_preflight(db_path, manifest)
    receipt: dict[str, Any]
    with (
        _existing_connection(db_path, read_only=False) as connection,
        transaction(connection),
    ):
        release = _release_row(connection, release_id)
        if str(release["status"]) not in {"backfilling", "ready"}:
            raise ReleaseManagementError("release must be backfilling or ready")
        audit = (
            _latest_running_audit(connection, manifest)
            if str(release["status"]) == "backfilling"
            else _latest_succeeded_audit(connection, manifest)
        )
        audit_summary, base_execution = _audit_execution(audit)
        protected_state = audit_summary["protected_state"]
        activation_stable_state = audit_summary.get("activation_stable_state")
        if not isinstance(protected_state, Mapping) or not isinstance(
            activation_stable_state, Mapping
        ):
            raise ReleaseManagementError("backfill audit lacks protected state")
        _require_protected_state(connection, manifest, protected_state)
        _require_activation_stable_state(connection, manifest, activation_stable_state)
        core = _semantic_core(connection, manifest, expected_taxonomy_status="draft")
        if production:
            if db_path.resolve() != manifest.source_database:
                raise ReleaseManagementError(
                    "production mode database does not match freeze source"
                )
            if (
                len(rehearsal_receipt_paths) != 2
                or len({path.resolve() for path in rehearsal_receipt_paths}) != 2
            ):
                raise ReleaseManagementError(
                    "production readiness requires two distinct rehearsal receipts"
                )
            rehearsal_receipts = [
                _read_receipt(path) for path in rehearsal_receipt_paths
            ]
            if any(
                canonical_json(item["core"]) != canonical_json(core)
                for item in rehearsal_receipts
            ):
                raise ReleaseManagementError(
                    "rehearsal receipts do not match the production semantic core"
                )
            executions = [
                _require_stored_receipt_attestation(item, expected_mode="rehearsal")
                for item in rehearsal_receipts
            ]
            if (
                len({str(item["rehearsal_run_id"]) for item in executions}) != 2
                or len({str(item["target_database"]) for item in executions}) != 2
                or len({str(item["target_db_pre_hash"]) for item in executions}) != 2
                or any(
                    Path(str(item["target_database"])).resolve() == db_path.resolve()
                    for item in executions
                )
            ):
                raise ReleaseManagementError(
                    "rehearsal receipts are not independent executions"
                )
            execution = {
                **base_execution,
                "mode": "production",
                "approved_rehearsals": [
                    {
                        "receipt_path": str(path.resolve()),
                        "receipt_file_sha256": _sha256_file(path.resolve()),
                        "core_sha256": str(item["core_sha256"]),
                        "execution_sha256": str(item["execution_sha256"]),
                        "audit_id": str(item["execution"]["audit_id"]),
                        "rehearsal_run_id": str(item["execution"]["rehearsal_run_id"]),
                        "target_database": str(item["execution"]["target_database"]),
                        "target_db_pre_hash": str(
                            item["execution"]["target_db_pre_hash"]
                        ),
                    }
                    for path, item in zip(
                        rehearsal_receipt_paths, rehearsal_receipts, strict=True
                    )
                ],
            }
        elif db_path.resolve() == manifest.source_database:
            raise ReleaseManagementError("production database requires production=True")
        elif rehearsal_receipt_paths:
            raise ReleaseManagementError(
                "rehearsal receipts are accepted only for the production database"
            )
        else:
            execution = {
                **base_execution,
                "mode": "rehearsal",
                "approved_rehearsals": [],
            }
        receipt = _receipt_payload(core, execution)
        if str(release["status"]) == "backfilling":
            captured_at = now_utc()
            _cas_release_status(
                connection,
                release_id=release_id,
                old_status="backfilling",
                new_status="ready",
                captured_at=captured_at,
            )
            summary = audit_summary
            summary["receipt_core_sha256"] = receipt["core_sha256"]
            summary["receipt_execution_sha256"] = receipt["execution_sha256"]
            summary["semantic_core"] = core
            summary["receipt_execution"] = execution
            cursor = connection.execute(
                """
                UPDATE migration_audit
                SET status='succeeded',summary_json=?,completed_at=?
                WHERE id=? AND status='running'
                """,
                (canonical_json(summary), captured_at, audit["id"]),
            )
            if cursor.rowcount != 1:
                raise ReleaseManagementError("backfill audit success CAS failed")
            _checkpoint("ready_and_audit_succeeded")
        else:
            if (
                audit_summary.get("receipt_core_sha256") != receipt["core_sha256"]
                or audit_summary.get("receipt_execution_sha256")
                != receipt["execution_sha256"]
                or canonical_json(audit_summary.get("semantic_core"))
                != canonical_json(core)
                or canonical_json(audit_summary.get("receipt_execution"))
                != canonical_json(execution)
            ):
                raise ReleaseManagementError(
                    "ready release audit does not attest the current receipt"
                )
    stored = _write_receipt(receipt_path, receipt)
    return {
        "release_id": release_id,
        "status": "ready",
        "receipt_path": str(receipt_path.resolve()),
        "core_sha256": stored["core_sha256"],
        "core": stored["core"],
    }


def _require_receipt_core(receipt: Mapping[str, Any], core: Mapping[str, Any]) -> None:
    if canonical_json(receipt.get("core")) != canonical_json(core):
        raise ReleaseManagementError(
            "approved receipt no longer matches database semantics"
        )


def _refresh_direction_cache_from_target(
    connection: sqlite3.Connection, *, expected_count: int
) -> None:
    cursor = connection.execute(
        """
        UPDATE content_items AS c
        SET evaluation_content_direction=(
            SELECT e.content_direction FROM evaluation_versions e
            WHERE e.content_id=c.id AND e.release_id=?
              AND e.evaluation_source='automatic' AND e.invalidated_at IS NULL
            ORDER BY e.evaluated_at DESC,e.id DESC LIMIT 1
        )
        WHERE EXISTS(
            SELECT 1 FROM evaluation_versions e
            WHERE e.content_id=c.id AND e.release_id=?
              AND e.evaluation_source='automatic' AND e.invalidated_at IS NULL
        )
        """,
        (TARGET_RELEASE_ID, TARGET_RELEASE_ID),
    )
    if cursor.rowcount != expected_count:
        raise ReleaseManagementError("target direction-cache refresh coverage mismatch")


def _require_active_invariants(
    connection: sqlite3.Connection, *, expected_count: int
) -> None:
    active = connection.execute(
        "SELECT id FROM evaluation_releases WHERE status='active'"
    ).fetchall()
    published = connection.execute(
        "SELECT version FROM taxonomy_versions WHERE status='published'"
    ).fetchall()
    if [str(row["id"]) for row in active] != [TARGET_RELEASE_ID]:
        raise ReleaseManagementError(
            "activation did not leave exactly one target release"
        )
    if [str(row["version"]) for row in published] != [TARGET_TAXONOMY_VERSION]:
        raise ReleaseManagementError(
            f"activation did not publish exactly {TARGET_TAXONOMY_VERSION}"
        )
    legacy_release = _release_row(connection, SOURCE_RELEASE_ID)
    legacy_taxonomy = _single_row(
        connection,
        "SELECT * FROM taxonomy_versions WHERE version=?",
        (SOURCE_TAXONOMY_VERSION,),
        label="legacy taxonomy",
    )
    if (
        str(legacy_release["status"]) != "retired"
        or str(legacy_taxonomy["status"]) != "retired"
    ):
        raise ReleaseManagementError("activation did not retire the legacy release")
    mismatch = int(
        connection.execute(
            """
            SELECT COUNT(*) FROM content_items c
            LEFT JOIN evaluation_versions e
              ON e.content_id=c.id AND e.release_id=?
             AND e.evaluation_source='automatic' AND e.invalidated_at IS NULL
            WHERE e.id IS NULL
               OR c.evaluation_content_direction IS NOT e.content_direction
            """,
            (TARGET_RELEASE_ID,),
        ).fetchone()[0]
    )
    if (
        mismatch
        or int(connection.execute("SELECT COUNT(*) FROM content_items").fetchone()[0])
        != expected_count
    ):
        raise ReleaseManagementError("activated direction cache is inconsistent")


def activate(
    *,
    db_path: Path,
    manifest_path: Path,
    receipt_path: Path,
    release_id: str = TARGET_RELEASE_ID,
) -> dict[str, Any]:
    """Atomically publish v5.2, activate its v8 release, and refresh directions."""

    if release_id != TARGET_RELEASE_ID:
        raise ReleaseManagementError("only the approved target release id is allowed")
    manifest = _load_freeze_manifest(manifest_path)
    receipt = _read_receipt(receipt_path)
    _require_production_receipt_chain(receipt, expected_database=db_path)
    _pure_read_preflight(db_path, manifest, allow_evaluation_cache_drift=True)
    with (
        _existing_connection(db_path, read_only=False) as connection,
        transaction(connection),
    ):
        stable_state = _attested_activation_stable_state(connection, manifest, receipt)
        release = _release_row(connection, release_id)
        release_status = str(release["status"])
        expected_taxonomy_status = (
            "published" if release_status == "active" else "draft"
        )
        _, matcher_sha256 = _target_taxonomy(
            connection, expected_status=expected_taxonomy_status
        )
        _require_release_contract(release, matcher_sha256=matcher_sha256)
        if release_status == "active":
            core = _semantic_core(
                connection, manifest, expected_taxonomy_status="published"
            )
            _require_receipt_core(receipt, core)
            _require_active_invariants(
                connection, expected_count=len(manifest.contents)
            )
            _checkpoint("activation_idempotent_verified")
            return status(db_path=db_path, release_id=release_id)
        if release_status != "ready":
            raise ReleaseManagementError("target release must be ready")
        _legacy_window(connection)
        core = _semantic_core(connection, manifest, expected_taxonomy_status="draft")
        _require_receipt_core(receipt, core)
        captured_at = now_utc()
        _cas_release_status(
            connection,
            release_id=SOURCE_RELEASE_ID,
            old_status="active",
            new_status="retired",
            captured_at=captured_at,
            extra_sql=",retired_at=?",
            extra_parameters=(captured_at,),
        )
        _checkpoint("activation_legacy_release_retired")
        _cas_taxonomy_status(
            connection,
            version=SOURCE_TAXONOMY_VERSION,
            old_status="published",
            new_status="retired",
        )
        _checkpoint("activation_legacy_taxonomy_retired")
        _cas_taxonomy_status(
            connection,
            version=TARGET_TAXONOMY_VERSION,
            old_status="draft",
            new_status="published",
            published_at=captured_at,
        )
        _checkpoint("activation_target_taxonomy_published")
        _cas_release_status(
            connection,
            release_id=release_id,
            old_status="ready",
            new_status="active",
            captured_at=captured_at,
            extra_sql=",activated_at=?,failure_reason=NULL",
            extra_parameters=(captured_at,),
        )
        _checkpoint("activation_target_release_active")
        _refresh_direction_cache_from_target(
            connection, expected_count=len(manifest.contents)
        )
        _checkpoint("activation_direction_cache_refreshed")
        _require_active_invariants(connection, expected_count=len(manifest.contents))
        _require_activation_stable_state(connection, manifest, stable_state)
        _require_v9(connection)
        if connection.execute("PRAGMA foreign_key_check").fetchall():
            raise ReleaseManagementError("activation introduced foreign-key violations")
        if str(connection.execute("PRAGMA integrity_check").fetchone()[0]) != "ok":
            raise ReleaseManagementError("activation integrity check failed")
        _checkpoint("activation_final_invariants")
    return status(db_path=db_path, release_id=release_id)


def _require_reason(reason: str) -> str:
    normalized = reason.strip()
    if not normalized:
        raise ReleaseManagementError("a non-empty operator reason is required")
    return normalized


def abort(
    *,
    db_path: Path,
    manifest_path: Path,
    reason: str,
    release_id: str = TARGET_RELEASE_ID,
) -> dict[str, Any]:
    """Fail a non-active release and invalidate its partial automatic history."""

    if release_id != TARGET_RELEASE_ID:
        raise ReleaseManagementError("only the approved target release id is allowed")
    reason = _require_reason(reason)
    manifest = _load_freeze_manifest(manifest_path)
    with (
        _existing_connection(db_path, read_only=False) as connection,
        transaction(connection),
    ):
        _require_v9(connection)
        _require_manifest_identity(connection, manifest)
        release = _release_row(connection, release_id)
        _, matcher_sha256 = _target_taxonomy(connection, expected_status="draft")
        _require_release_contract(release, matcher_sha256=matcher_sha256)
        release_status = str(release["status"])
        if release_status in {"active", "retired"}:
            raise ReleaseManagementError("active/retired releases cannot be aborted")
        if connection.execute(
            """
            SELECT 1 FROM evaluation_versions
            WHERE release_id=? AND evaluation_source<>'automatic' LIMIT 1
            """,
            (release_id,),
        ).fetchone():
            raise ReleaseManagementError(
                "target release contains non-automatic history"
            )
        captured_at = now_utc()
        connection.execute(
            """
            UPDATE evaluation_versions
            SET invalidated_at=?,invalidation_reason=?
            WHERE release_id=? AND evaluation_source='automatic'
              AND invalidated_at IS NULL
            """,
            (captured_at, reason, release_id),
        )
        if release_status != "failed":
            _cas_release_status(
                connection,
                release_id=release_id,
                old_status=release_status,
                new_status="failed",
                captured_at=captured_at,
                extra_sql=",failure_reason=?",
                extra_parameters=(reason,),
            )
        running_audits = connection.execute(
            """
            SELECT id FROM migration_audit
            WHERE id LIKE ? AND status='running' ORDER BY id
            """,
            (f"{_audit_prefix(manifest)}__%",),
        ).fetchall()
        for audit in running_audits:
            audit_row = connection.execute(
                "SELECT summary_json FROM migration_audit WHERE id=?", (audit["id"],)
            ).fetchone()
            if audit_row is None:
                raise ReleaseManagementError("abort audit disappeared")
            summary = _json_object(
                audit_row["summary_json"], label="backfill audit summary"
            )
            summary["error"] = reason
            cursor = connection.execute(
                """
                UPDATE migration_audit
                SET status='failed',summary_json=?,completed_at=?
                WHERE id=? AND status='running'
                """,
                (
                    canonical_json(summary),
                    captured_at,
                    audit["id"],
                ),
            )
            if cursor.rowcount != 1:
                raise ReleaseManagementError("abort audit CAS failed")
        _checkpoint("abort_release_failed")
    return status(db_path=db_path, release_id=release_id)


def _require_rollback_boundary(
    connection: sqlite3.Connection, manifest: FreezeManifest
) -> None:
    _require_manifest_identity(connection, manifest, allow_evaluation_cache_drift=True)
    _require_operational_baseline(connection, manifest)
    if connection.execute(
        "SELECT 1 FROM report_revisions WHERE release_id=? LIMIT 1",
        (TARGET_RELEASE_ID,),
    ).fetchone():
        raise ReleaseManagementError("target release already has report revisions")
    if connection.execute(
        """
        SELECT 1 FROM evaluation_versions
        WHERE release_id=? AND evaluation_source<>'automatic' LIMIT 1
        """,
        (TARGET_RELEASE_ID,),
    ).fetchone():
        raise ReleaseManagementError("target release already has review lineage")
    if connection.execute(
        """
        SELECT 1 FROM review_queue q JOIN evaluation_versions e ON e.id=q.evaluation_id
        WHERE e.release_id=? LIMIT 1
        """,
        (TARGET_RELEASE_ID,),
    ).fetchone():
        raise ReleaseManagementError("target release already has review queue writes")


def _refresh_direction_cache_from_legacy(
    connection: sqlite3.Connection, *, expected_count: int
) -> None:
    cursor = connection.execute(
        """
        UPDATE content_items AS c
        SET evaluation_content_direction=COALESCE((
            SELECT e.content_direction FROM evaluation_versions e
            WHERE e.content_id=c.id AND e.release_id=? AND e.invalidated_at IS NULL
            ORDER BY e.evaluated_at DESC,e.id DESC LIMIT 1
        ),'unknown')
        """,
        (SOURCE_RELEASE_ID,),
    )
    if cursor.rowcount != expected_count:
        raise ReleaseManagementError("legacy direction-cache restore coverage mismatch")


def rollback_before_resume(
    *,
    db_path: Path,
    manifest_path: Path,
    receipt_path: Path,
    reason: str,
    release_id: str = TARGET_RELEASE_ID,
) -> dict[str, Any]:
    """Rollback a just-activated release only while the frozen boundary still holds."""

    if release_id != TARGET_RELEASE_ID:
        raise ReleaseManagementError("only the approved target release id is allowed")
    reason = _require_reason(reason)
    manifest = _load_freeze_manifest(manifest_path)
    receipt = _read_receipt(receipt_path)
    _require_production_receipt_chain(receipt, expected_database=db_path)
    _pure_read_preflight(db_path, manifest, allow_evaluation_cache_drift=True)
    with (
        _existing_connection(db_path, read_only=False) as connection,
        transaction(connection),
    ):
        stable_state = _attested_activation_stable_state(connection, manifest, receipt)
        release = _release_row(connection, release_id)
        _, matcher_sha256 = _target_taxonomy(connection, expected_status="published")
        _require_release_contract(release, matcher_sha256=matcher_sha256)
        if str(release["status"]) != "active":
            raise ReleaseManagementError(
                "only the active target release can be rolled back"
            )
        legacy = _release_row(connection, SOURCE_RELEASE_ID)
        if str(legacy["status"]) != "retired":
            raise ReleaseManagementError("legacy release is not in the rollback state")
        legacy_taxonomy = _single_row(
            connection,
            "SELECT * FROM taxonomy_versions WHERE version=?",
            (SOURCE_TAXONOMY_VERSION,),
            label="legacy taxonomy",
        )
        if str(legacy_taxonomy["status"]) != "retired":
            raise ReleaseManagementError("legacy taxonomy is not in the rollback state")
        _require_rollback_boundary(connection, manifest)
        core = _semantic_core(
            connection, manifest, expected_taxonomy_status="published"
        )
        _require_receipt_core(receipt, core)
        captured_at = now_utc()
        connection.execute(
            """
            UPDATE evaluation_versions
            SET invalidated_at=?,invalidation_reason=?
            WHERE release_id=? AND evaluation_source='automatic'
              AND invalidated_at IS NULL
            """,
            (captured_at, reason, release_id),
        )
        _checkpoint("rollback_target_evaluations_invalidated")
        _cas_release_status(
            connection,
            release_id=release_id,
            old_status="active",
            new_status="retired",
            captured_at=captured_at,
            extra_sql=",retired_at=?,failure_reason=?",
            extra_parameters=(captured_at, reason),
        )
        _checkpoint("rollback_target_release_retired")
        _cas_taxonomy_status(
            connection,
            version=TARGET_TAXONOMY_VERSION,
            old_status="published",
            new_status="retired",
        )
        _checkpoint("rollback_target_taxonomy_retired")
        _cas_taxonomy_status(
            connection,
            version=SOURCE_TAXONOMY_VERSION,
            old_status="retired",
            new_status="published",
        )
        _checkpoint("rollback_legacy_taxonomy_published")
        _cas_release_status(
            connection,
            release_id=SOURCE_RELEASE_ID,
            old_status="retired",
            new_status="active",
            captured_at=captured_at,
            extra_sql=",retired_at=NULL",
        )
        _checkpoint("rollback_legacy_release_active")
        _refresh_direction_cache_from_legacy(
            connection, expected_count=len(manifest.contents)
        )
        _checkpoint("rollback_direction_cache_restored")
        active = connection.execute(
            "SELECT id FROM evaluation_releases WHERE status='active'"
        ).fetchall()
        published = connection.execute(
            "SELECT version FROM taxonomy_versions WHERE status='published'"
        ).fetchall()
        if [str(row["id"]) for row in active] != [SOURCE_RELEASE_ID] or [
            str(row["version"]) for row in published
        ] != [SOURCE_TAXONOMY_VERSION]:
            raise ReleaseManagementError("rollback final state is inconsistent")
        if connection.execute("PRAGMA foreign_key_check").fetchall():
            raise ReleaseManagementError("rollback introduced foreign-key violations")
        _require_activation_stable_state(connection, manifest, stable_state)
        _checkpoint("rollback_final_invariants")
    return status(db_path=db_path, release_id=release_id)


__all__ = [
    "BATCH_SIZE",
    "ReleaseManagementError",
    "TARGET_RELEASE_ID",
    "abort",
    "activate",
    "backfill",
    "create",
    "rollback_before_resume",
    "status",
    "verify_ready",
]
