"""Focused, fail-closed lifecycle for the evaluation-v9 rule release.

The v9 release deliberately reuses the already-published selling-points-v5.2
taxonomy.  This module therefore never mutates taxonomy tables and never calls
network or provider code.  Every mutating phase is bound to an externally
hashed manifest produced from an immutable schema-v13 snapshot.
"""

from __future__ import annotations

import hashlib
import json
import os
import re
import sqlite3
import subprocess
import tempfile
from collections.abc import Iterator, Mapping, Sequence
from contextlib import contextmanager
from pathlib import Path
from typing import Any

from . import evaluation
from .storage import (
    DEFAULT_DB,
    PROJECT_ROOT,
    SCHEMA_VERSION,
    SchemaMigrationError,
    configure_connection_safety,
    now_utc,
    require_schema_compatibility,
    transaction,
)


SOURCE_RELEASE_ID = "evaluation-v8__selling-points-v5.2"
TARGET_RELEASE_ID = "evaluation-v9__selling-points-v5.2"
SOURCE_RULE_VERSION = "evaluation-v8"
TARGET_RULE_VERSION = "evaluation-v9"
TAXONOMY_VERSION = "selling-points-v5.2"
REPORT_VERSION = "dcar-content-operations-report-v8.6"
MANIFEST_SCHEMA = "dcar-evaluation-v9-manifest-v1"
RECEIPT_SCHEMA = "dcar-evaluation-v9-ready-v1"
DEFAULT_OPERATOR_FREEZE_LOCK = PROJECT_ROOT / "runtime" / "operator-freeze.lock"
BATCH_SIZE = 250
HEX_SHA256 = re.compile(r"[0-9a-f]{64}", re.ASCII)
QUEUE_REASONS = (
    "evaluation_gray_zone",
    "manual_conclusion_conflict",
    "media_processing_incomplete",
    "media_evidence_missing",
    "stale_local_evidence",
    "legacy_content_unavailable",
    "legacy_nonautomatic_point_scene_conflict_v1",
)
ACTIVE_QUEUE_STATUSES = (
    "pending",
    "manual_required",
    "in_review",
    "terminal_failed",
)
EVALUATION_COMPARE_COLUMNS = (
    "taxonomy_version",
    "matcher_rule_sha256",
    "evaluation_source",
    "evaluation_status",
    "evidence_level",
    "primary_selling_point_code",
    "selling_point_score",
    "selling_point_included",
    "content_direction",
    "content_automotive_score",
    "audience_automotive_score",
    "acquisition_potential_score",
)
EVIDENCE_COMPONENT_KEYS = (
    "detail_raw_sha256",
    "text_sha256",
    "media_sha256",
    "asr_sha256",
    "ocr_sha256",
    "comments_version_sha256",
    "manual_evidence_sha256",
)
PROTECTED_TABLES = (
    "content_items",
    "provider_raw_responses",
    "evidence_artifacts",
    "comment_evidence_versions",
    "comment_user_scores",
    "manual_evidence",
    "provider_usage",
    "evaluation_reviews",
    "review_reopen_events",
    "review_queue",
    "report_revisions",
)


class ReleaseV9Error(RuntimeError):
    """Raised when a v9 release operation cannot proceed safely."""


def _database_writer_handles(database: Path) -> list[dict[str, Any]]:
    targets = [database, Path(f"{database}-wal"), Path(f"{database}-shm")]
    existing = [str(path) for path in targets if path.exists()]
    if not existing:
        return []
    try:
        result = subprocess.run(
            ["lsof", "-nP", *existing],
            check=False,
            capture_output=True,
            text=True,
        )
    except OSError as error:
        raise ReleaseV9Error(
            "cannot verify formal database writer handles with lsof"
        ) from error
    if result.returncode not in {0, 1}:
        raise ReleaseV9Error(
            "cannot verify formal database writer handles: "
            f"{result.stderr.strip() or 'lsof failed'}"
        )
    writers: list[dict[str, Any]] = []
    for line in result.stdout.splitlines()[1:]:
        parts = line.split()
        if len(parts) < 9:
            continue
        descriptor = parts[3]
        descriptor_match = re.fullmatch(r"\d+([rwu]).*", descriptor)
        if descriptor_match is not None and descriptor_match.group(1) in {"u", "w"}:
            writers.append(
                {
                    "command": parts[0],
                    "pid": int(parts[1]),
                    "descriptor": descriptor,
                    "path": parts[-1],
                }
            )
    return writers


def assert_cli_cutover_guard(
    *,
    db_path: Path,
    freeze_lock: Path,
    isolated_clone: bool,
) -> None:
    """Require an operator freeze for formal DBs or an explicit isolated clone."""

    database = db_path.resolve()
    formal_database = DEFAULT_DB.resolve()
    formal_data_root = formal_database.parent
    if database != formal_database:
        if not isolated_clone:
            raise ReleaseV9Error(
                "non-formal databases require the explicit --isolated-clone flag"
            )
        if formal_data_root == database.parent or formal_data_root in database.parents:
            raise ReleaseV9Error(
                "--isolated-clone cannot target the formal app/data directory"
            )
        return
    if isolated_clone:
        raise ReleaseV9Error("the formal database cannot be marked as an isolated clone")
    lock_candidate = freeze_lock.expanduser()
    if lock_candidate.is_symlink() or not lock_candidate.is_file():
        lock = lock_candidate.resolve()
        raise ReleaseV9Error(f"operator freeze lock is missing or unsafe: {lock}")
    lock = lock_candidate.resolve()
    if lock != DEFAULT_OPERATOR_FREEZE_LOCK.resolve():
        raise ReleaseV9Error(
            "formal database requires the canonical operator freeze lock: "
            f"{DEFAULT_OPERATOR_FREEZE_LOCK.resolve()}"
        )
    writers = _database_writer_handles(database)
    if writers:
        raise ReleaseV9Error(
            "formal database writer handles are still open: "
            + _canonical_json(writers)
        )


def _canonical_json(value: Any) -> str:
    return json.dumps(
        value,
        ensure_ascii=False,
        sort_keys=True,
        separators=(",", ":"),
        allow_nan=False,
    )


def _sha256_bytes(value: bytes) -> str:
    return hashlib.sha256(value).hexdigest()


def _sha256_file(path: Path) -> str:
    digest = hashlib.sha256()
    try:
        with path.open("rb") as handle:
            for chunk in iter(lambda: handle.read(1024 * 1024), b""):
                digest.update(chunk)
    except OSError as error:
        raise ReleaseV9Error(f"cannot hash file {path}: {error}") from error
    return digest.hexdigest()


def _require_sha256(value: str, *, label: str) -> str:
    if HEX_SHA256.fullmatch(value) is None:
        raise ReleaseV9Error(f"{label} must be a lowercase SHA-256")
    return value


def _checkpoint(_name: str) -> None:
    """Patchable fault-injection boundary used by lifecycle tests."""


@contextmanager
def _connect(path: Path, *, read_only: bool) -> Iterator[sqlite3.Connection]:
    resolved = path.resolve()
    if not resolved.is_file():
        raise ReleaseV9Error(f"database must be an existing file: {resolved}")
    connection: sqlite3.Connection | None = None
    try:
        mode = "ro" if read_only else "rw"
        connection = sqlite3.connect(
            f"{resolved.as_uri()}?mode={mode}", uri=True, timeout=30
        )
        connection.row_factory = sqlite3.Row
        configure_connection_safety(connection)
        connection.execute("PRAGMA busy_timeout=30000")
        if read_only:
            connection.execute("PRAGMA query_only=ON")
        yield connection
    except sqlite3.Error as error:
        access = "read-only" if read_only else "read-write"
        raise ReleaseV9Error(f"cannot access database {access}: {error}") from error
    finally:
        if connection is not None:
            connection.close()


def _require_schema(connection: sqlite3.Connection) -> None:
    try:
        require_schema_compatibility(
            connection, supported_versions=frozenset({SCHEMA_VERSION})
        )
    except SchemaMigrationError as error:
        raise ReleaseV9Error(
            f"complete schema v{SCHEMA_VERSION} is required"
        ) from error
    if str(connection.execute("PRAGMA quick_check").fetchone()[0]) != "ok":
        raise ReleaseV9Error("database quick_check failed")
    if connection.execute("PRAGMA foreign_key_check").fetchall():
        raise ReleaseV9Error("database has foreign-key violations")


def _require_schema_identity(connection: sqlite3.Connection) -> None:
    if int(connection.execute("PRAGMA user_version").fetchone()[0]) != SCHEMA_VERSION:
        raise ReleaseV9Error(f"schema v{SCHEMA_VERSION} is required")
    migration = connection.execute(
        "SELECT version,name FROM schema_migrations ORDER BY version DESC LIMIT 1"
    ).fetchone()
    if migration is None or int(migration["version"]) != SCHEMA_VERSION:
        raise ReleaseV9Error("schema migration identity changed during lifecycle")


def _require_integrity(connection: sqlite3.Connection) -> None:
    if str(connection.execute("PRAGMA integrity_check").fetchone()[0]) != "ok":
        raise ReleaseV9Error("database integrity_check failed")
    if connection.execute("PRAGMA foreign_key_check").fetchall():
        raise ReleaseV9Error("database has foreign-key violations")


def _json_safe(value: Any) -> Any:
    if isinstance(value, bytes):
        return {"bytes_hex": value.hex()}
    return value


def _rows_digest(
    connection: sqlite3.Connection,
    table: str,
    *,
    where: str = "",
    parameters: Sequence[Any] = (),
    selected_columns: Sequence[str] | None = None,
) -> dict[str, Any]:
    available_columns = tuple(
        str(row[1]) for row in connection.execute(f'PRAGMA table_info("{table}")')
    )
    if not available_columns:
        raise ReleaseV9Error(f"required table does not exist: {table}")
    columns = tuple(selected_columns or available_columns)
    if not columns or set(columns) - set(available_columns):
        raise ReleaseV9Error(f"invalid digest columns for table: {table}")
    projection = ",".join(f'"{column}"' for column in columns)
    order_by = ",".join(f'"{column}"' for column in columns)
    digest = hashlib.sha256()
    count = 0
    for row in connection.execute(
        f'SELECT {projection} FROM "{table}" {where} ORDER BY {order_by}',
        tuple(parameters),
    ):
        encoded = _canonical_json(
            {column: _json_safe(row[column]) for column in columns}
        ).encode("utf-8")
        digest.update(len(encoded).to_bytes(8, "big"))
        digest.update(encoded)
        count += 1
    return {
        "row_count": count,
        "rows_sha256": digest.hexdigest(),
    }


def _report_revision_count(connection: sqlite3.Connection) -> int:
    return int(
        connection.execute(
            "SELECT COUNT(*) FROM report_revisions WHERE contract_version=?",
            (REPORT_VERSION,),
        ).fetchone()[0]
    )


def _release(connection: sqlite3.Connection, release_id: str) -> sqlite3.Row:
    row = connection.execute(
        "SELECT * FROM evaluation_releases WHERE id=?", (release_id,)
    ).fetchone()
    if row is None:
        raise ReleaseV9Error(f"release does not exist: {release_id}")
    return row


def _require_source_contract(connection: sqlite3.Connection) -> sqlite3.Row:
    active = connection.execute(
        "SELECT * FROM evaluation_releases WHERE status='active' ORDER BY id"
    ).fetchall()
    if len(active) != 1 or str(active[0]["id"]) != SOURCE_RELEASE_ID:
        raise ReleaseV9Error(
            f"exactly one active source release {SOURCE_RELEASE_ID} is required"
        )
    source = active[0]
    if (
        str(source["rule_version"]) != SOURCE_RULE_VERSION
        or str(source["taxonomy_version"]) != TAXONOMY_VERSION
        or HEX_SHA256.fullmatch(str(source["matcher_rule_sha256"])) is None
    ):
        raise ReleaseV9Error("source release contract is inconsistent")
    taxonomy = connection.execute(
        "SELECT * FROM taxonomy_versions WHERE version=?", (TAXONOMY_VERSION,)
    ).fetchall()
    if len(taxonomy) != 1 or str(taxonomy[0]["status"]) != "published":
        raise ReleaseV9Error("selling-points-v5.2 must be uniquely published")
    # Loading the runtime recomputes and checks the materialized matcher hash.
    evaluation._load_release_runtime(connection, source)
    if _report_revision_count(connection):
        raise ReleaseV9Error("v8.6 report revisions already exist")
    return source


def _rule_evidence(
    connection: sqlite3.Connection, content_id: int, rule_version: str
) -> tuple[dict[str, Any], str]:
    _artifacts, components, evidence_sha256 = evaluation._current_evidence_state(
        connection, content_id, rule_version=rule_version
    )
    if not isinstance(components, Mapping):
        raise ReleaseV9Error(f"content {content_id} has invalid evidence components")
    return dict(components), str(evidence_sha256)


def _latest_source_evaluations(
    connection: sqlite3.Connection,
    *,
    content_ids: Sequence[int] | None = None,
) -> dict[int, sqlite3.Row]:
    parameters: list[Any] = [SOURCE_RELEASE_ID]
    content_filter = ""
    if content_ids is not None:
        if not content_ids:
            return {}
        content_filter = f" AND e.content_id IN ({','.join('?' for _ in content_ids)})"
        parameters.extend(content_ids)
    rows = connection.execute(
        f"""
        WITH ranked AS (
            SELECT e.*,
                   ROW_NUMBER() OVER (
                       PARTITION BY e.content_id
                       ORDER BY e.evaluated_at DESC,e.id DESC
                   ) selector_rank
            FROM evaluation_versions e
            WHERE e.release_id=? AND e.evaluation_source='automatic'
              AND e.invalidated_at IS NULL
              {content_filter}
        )
        SELECT * FROM ranked WHERE selector_rank=1 ORDER BY content_id
        """,
        tuple(parameters),
    ).fetchall()
    return {int(row["content_id"]): row for row in rows}


def _matches(connection: sqlite3.Connection, evaluation_id: int) -> list[dict[str, Any]]:
    return [
        dict(row)
        for row in connection.execute(
            """
            SELECT selling_point_code,scene,match_role,score,evidence_json
            FROM evaluation_matches WHERE evaluation_id=?
            ORDER BY CASE match_role WHEN 'primary' THEN 0 ELSE 1 END,
                     selling_point_code
            """,
            (evaluation_id,),
        )
    ]


def _source_projection(row: sqlite3.Row) -> dict[str, Any]:
    payload = json.loads(str(row["payload_json"]))
    if not isinstance(payload, dict):
        raise ReleaseV9Error(f"evaluation {row['id']} payload must be an object")
    return {
        key: _json_safe(row[key])
        for key in (*EVALUATION_COMPARE_COLUMNS, "pending_review", "payload_json")
    } | {"payload": payload}


def _envelope_matches(
    envelope: sqlite3.Row | None,
    *,
    content_id: int,
    evidence_sha256: str,
    components: Mapping[str, Any],
) -> bool:
    return bool(
        envelope is not None
        and int(envelope["content_id"]) == content_id
        and str(envelope["schema_version"]) == evaluation.EVIDENCE_VERSION
        and str(envelope["evidence_sha256"]) == evidence_sha256
        and _canonical_json(json.loads(str(envelope["components_json"])))
        == _canonical_json(components)
        and all(envelope[key] == components[key] for key in EVIDENCE_COMPONENT_KEYS)
    )


def _inventory(connection: sqlite3.Connection) -> list[dict[str, Any]]:
    source = _latest_source_evaluations(connection)
    content_ids = sorted(source)
    if not content_ids:
        raise ReleaseV9Error("source latest automatic inventory must not be empty")
    inventory: list[dict[str, Any]] = []
    for content_id in content_ids:
        row = source[content_id]
        if (
            str(row["rule_version"]) != SOURCE_RULE_VERSION
            or str(row["taxonomy_version"]) != TAXONOMY_VERSION
            or str(row["evaluation_source"]) != "automatic"
            or row["parent_evaluation_id"] is not None
            or row["review_id"] is not None
        ):
            raise ReleaseV9Error(
                f"source evaluation {row['id']} is not a valid automatic row"
            )
        source_components, source_sha256 = _rule_evidence(
            connection, content_id, SOURCE_RULE_VERSION
        )
        v9_components, v9_sha256 = _rule_evidence(
            connection, content_id, TARGET_RULE_VERSION
        )
        if str(row["evidence_sha256"]) != source_sha256:
            raise ReleaseV9Error(
                f"content {content_id} source evaluation is stale for current evidence"
            )
        envelope = connection.execute(
            "SELECT * FROM evidence_envelopes WHERE id=?",
            (row["evidence_envelope_id"],),
        ).fetchone()
        if not _envelope_matches(
            envelope,
            content_id=content_id,
            evidence_sha256=source_sha256,
            components=source_components,
        ):
            raise ReleaseV9Error(f"content {content_id} source envelope is inconsistent")
        manual_excluded = source_components.get("manual_evidence_sha256") is not None
        if v9_components.get("manual_evidence_sha256") is not None:
            raise ReleaseV9Error(
                f"content {content_id} v9 evidence must exclude manual evidence"
            )
        inventory.append(
            {
                "content_id": content_id,
                "source_evaluation_id": int(row["id"]),
                "source_evidence_envelope_id": int(row["evidence_envelope_id"]),
                "source_evidence_sha256": source_sha256,
                "source_evidence_components": source_components,
                "source_evaluation": _source_projection(row),
                "source_matches": _matches(connection, int(row["id"])),
                "v9_evidence_sha256": v9_sha256,
                "v9_evidence_components": v9_components,
                "manual_evidence_excluded": manual_excluded,
            }
        )
    return inventory


def _protected_state(connection: sqlite3.Connection) -> dict[str, Any]:
    state = {table: _rows_digest(connection, table) for table in PROTECTED_TABLES}
    state["source_release"] = _rows_digest(
        connection,
        "evaluation_releases",
        where="WHERE id=?",
        parameters=(SOURCE_RELEASE_ID,),
    )
    state["source_evaluations"] = _rows_digest(
        connection,
        "evaluation_versions",
        where="WHERE release_id=?",
        parameters=(SOURCE_RELEASE_ID,),
    )
    state["source_matches"] = _rows_digest(
        connection,
        "evaluation_matches",
        where=(
            "WHERE evaluation_id IN (SELECT id FROM evaluation_versions "
            "WHERE release_id=?)"
        ),
        parameters=(SOURCE_RELEASE_ID,),
    )
    state["source_envelopes"] = _rows_digest(
        connection,
        "evidence_envelopes",
        where=(
            "WHERE id IN (SELECT evidence_envelope_id FROM evaluation_versions "
            "WHERE release_id=?)"
        ),
        parameters=(SOURCE_RELEASE_ID,),
    )
    state["taxonomy_versions"] = _rows_digest(
        connection,
        "taxonomy_versions",
        where="WHERE version=?",
        parameters=(TAXONOMY_VERSION,),
    )
    state["taxonomy_points"] = _rows_digest(
        connection,
        "selling_points",
        where=(
            "WHERE taxonomy_id=(SELECT id FROM taxonomy_versions WHERE version=?)"
        ),
        parameters=(TAXONOMY_VERSION,),
    )
    state["taxonomy_scenes"] = _rows_digest(
        connection,
        "selling_point_scenes",
        where=(
            "WHERE selling_point_id IN (SELECT sp.id FROM selling_points sp "
            "JOIN taxonomy_versions tv ON tv.id=sp.taxonomy_id WHERE tv.version=?)"
        ),
        parameters=(TAXONOMY_VERSION,),
    )
    return state


def _columns_without(
    connection: sqlite3.Connection, table: str, excluded: set[str]
) -> tuple[str, ...]:
    return tuple(
        str(row[1])
        for row in connection.execute(f'PRAGMA table_info("{table}")')
        if str(row[1]) not in excluded
    )


def _activation_stable_state(
    connection: sqlite3.Connection,
    *,
    protected_state: Mapping[str, Any] | None = None,
) -> dict[str, Any]:
    """State activation is allowed to change only in explicitly omitted fields."""

    protected = dict(protected_state or _protected_state(connection))
    state = {
        key: value
        for key, value in protected.items()
        if key not in {"content_items", "review_queue", "source_release"}
    }
    state["content_items"] = _rows_digest(
        connection,
        "content_items",
        selected_columns=_columns_without(
            connection, "content_items", {"evaluation_content_direction"}
        ),
    )
    state["review_queue"] = _rows_digest(
        connection,
        "review_queue",
        selected_columns=_columns_without(
            connection, "review_queue", {"status", "updated_at", "resolved_at"}
        ),
    )
    return state


def _fast_guard(connection: sqlite3.Connection) -> dict[str, Any]:
    source = _release(connection, SOURCE_RELEASE_ID)
    selector = connection.execute(
        """
        WITH ranked AS (
            SELECT id,content_id,
                   ROW_NUMBER() OVER (
                       PARTITION BY content_id ORDER BY evaluated_at DESC,id DESC
                   ) selector_rank
            FROM evaluation_versions
            WHERE release_id=? AND evaluation_source='automatic'
              AND invalidated_at IS NULL
        )
        SELECT COUNT(*),COALESCE(MAX(content_id),0),COALESCE(MAX(id),0),
               COALESCE(SUM(id),0)
        FROM ranked WHERE selector_rank=1
        """,
        (SOURCE_RELEASE_ID,),
    ).fetchone()
    active = connection.execute(
        "SELECT id FROM evaluation_releases WHERE status='active' ORDER BY id"
    ).fetchall()
    return {
        "active_release_ids": [str(row["id"]) for row in active],
        "source_release": {
            "id": source["id"],
            "rule_version": source["rule_version"],
            "taxonomy_version": source["taxonomy_version"],
            "matcher_rule_sha256": source["matcher_rule_sha256"],
            "status": source["status"],
        },
        "content_items": list(
            connection.execute(
                "SELECT COUNT(*),COALESCE(MAX(id),0) FROM content_items"
            ).fetchone()
        ),
        "source_selector": {
            "count": int(selector[0]),
            "content_high_water": int(selector[1]),
            "evaluation_id_high_water": int(selector[2]),
            "evaluation_id_sum": int(selector[3]),
        },
        "provider_usage": list(
            connection.execute(
                "SELECT COUNT(*),COALESCE(MAX(id),0) FROM provider_usage"
            ).fetchone()
        ),
        "manual_evidence": list(
            connection.execute(
                "SELECT COUNT(*),COALESCE(MAX(id),0) FROM manual_evidence"
            ).fetchone()
        ),
        "v8_6_report_revision_count": _report_revision_count(connection),
        "taxonomy": list(
            connection.execute(
                """
                SELECT version,status,source_sha256 FROM taxonomy_versions
                WHERE version=?
                """,
                (TAXONOMY_VERSION,),
            ).fetchone()
        ),
    }


def _require_fast_guard(
    connection: sqlite3.Connection, manifest: Mapping[str, Any]
) -> None:
    expected = manifest.get("fast_guard")
    if not isinstance(expected, Mapping) or _canonical_json(
        _fast_guard(connection)
    ) != _canonical_json(expected):
        raise ReleaseV9Error("protected source guard changed during backfill")


def _require_activation_stable(
    connection: sqlite3.Connection, manifest: Mapping[str, Any]
) -> None:
    expected = manifest.get("activation_stable_state")
    if not isinstance(expected, Mapping) or _canonical_json(
        _activation_stable_state(connection)
    ) != _canonical_json(expected):
        raise ReleaseV9Error("database crossed the rollback-before-resume boundary")


def _legacy_queue_snapshot(connection: sqlite3.Connection) -> list[dict[str, Any]]:
    placeholders = ",".join("?" for _ in QUEUE_REASONS)
    statuses = ",".join("?" for _ in ACTIVE_QUEUE_STATUSES)
    return [
        dict(row)
        for row in connection.execute(
            f"""
            SELECT * FROM review_queue
            WHERE reason_code IN ({placeholders}) AND status IN ({statuses})
            ORDER BY id
            """,
            (*QUEUE_REASONS, *ACTIVE_QUEUE_STATUSES),
        )
    ]


def _manifest_legacy_queue_snapshot(
    manifest: Mapping[str, Any],
) -> list[dict[str, Any]]:
    rows = manifest.get("legacy_active_review_queues")
    if not isinstance(rows, list):
        raise ReleaseV9Error("manifest legacy queue snapshot is invalid")
    output: list[dict[str, Any]] = []
    seen_ids: set[int] = set()
    for item in rows:
        if not isinstance(item, Mapping):
            raise ReleaseV9Error("manifest legacy queue row is invalid")
        try:
            queue_id = int(item["id"])
            reason_code = str(item["reason_code"])
            status = str(item["status"])
        except (KeyError, TypeError, ValueError) as error:
            raise ReleaseV9Error("manifest legacy queue row is invalid") from error
        if (
            queue_id in seen_ids
            or reason_code not in QUEUE_REASONS
            or status not in ACTIVE_QUEUE_STATUSES
        ):
            raise ReleaseV9Error("manifest legacy queue row is outside the v9 policy")
        seen_ids.add(queue_id)
        output.append(dict(item))
    if [int(item["id"]) for item in output] != sorted(seen_ids):
        raise ReleaseV9Error("manifest legacy queue rows must be unique and ordered")
    return output


def _require_legacy_queue_snapshot(
    connection: sqlite3.Connection, manifest: Mapping[str, Any]
) -> list[dict[str, Any]]:
    expected = _manifest_legacy_queue_snapshot(manifest)
    if _canonical_json(_legacy_queue_snapshot(connection)) != _canonical_json(expected):
        raise ReleaseV9Error("legacy queue snapshot changed after manifest freeze")
    return expected


def prepare_manifest(*, db_path: Path, manifest_path: Path) -> dict[str, Any]:
    """Freeze the exact active-v8 automatic inventory without writing the DB."""

    db_path = db_path.resolve()
    manifest_path = manifest_path.resolve()
    if not manifest_path.parent.is_dir():
        raise ReleaseV9Error("manifest output parent must exist")
    if manifest_path.exists():
        raise ReleaseV9Error("refusing to overwrite an existing manifest")
    wal = Path(f"{db_path}-wal")
    before_wal_size = wal.stat().st_size if wal.exists() else 0
    if before_wal_size:
        raise ReleaseV9Error("database WAL must be empty before freezing")
    before_sha256 = _sha256_file(db_path)
    start_data_version = end_data_version = -1
    with _connect(db_path, read_only=True) as connection:
        connection.execute("BEGIN")
        start_data_version = int(connection.execute("PRAGMA data_version").fetchone()[0])
        _require_schema(connection)
        source = _require_source_contract(connection)
        occupied = connection.execute(
            """
            SELECT id FROM evaluation_releases
            WHERE id=? OR (rule_version=? AND taxonomy_version=?)
            """,
            (TARGET_RELEASE_ID, TARGET_RULE_VERSION, TAXONOMY_VERSION),
        ).fetchall()
        if occupied:
            raise ReleaseV9Error("evaluation-v9 target release is already occupied")
        inventory = _inventory(connection)
        protected = _protected_state(connection)
        stable = _activation_stable_state(
            connection, protected_state=protected
        )
        fast_guard = _fast_guard(connection)
        queues = _legacy_queue_snapshot(connection)
        manifest = {
            "schema_version": MANIFEST_SCHEMA,
            "generated_at": now_utc(),
            "source_database": str(db_path),
            "source_database_sha256": before_sha256,
            "database_user_version": SCHEMA_VERSION,
            "source_release": {
                "id": SOURCE_RELEASE_ID,
                "rule_version": SOURCE_RULE_VERSION,
                "taxonomy_version": TAXONOMY_VERSION,
                "matcher_rule_sha256": str(source["matcher_rule_sha256"]),
            },
            "target_release": {
                "id": TARGET_RELEASE_ID,
                "rule_version": TARGET_RULE_VERSION,
                "taxonomy_version": TAXONOMY_VERSION,
                "matcher_rule_sha256": str(source["matcher_rule_sha256"]),
            },
            "taxonomy": {
                "version": TAXONOMY_VERSION,
                "status": "published",
                "write_policy": "immutable-reuse",
            },
            "manual_evidence_policy": "excluded-from-evaluation-v9",
            "v8_6_report_revision_count": 0,
            "content_count": len(inventory),
            "content_high_water": inventory[-1]["content_id"],
            "inventory_sha256": _sha256_bytes(
                _canonical_json(inventory).encode("utf-8")
            ),
            "inventory": inventory,
            "protected_state": protected,
            "activation_stable_state": stable,
            "fast_guard": fast_guard,
            "legacy_active_review_queues": queues,
        }
        end_data_version = int(connection.execute("PRAGMA data_version").fetchone()[0])
        connection.rollback()
    after_wal_size = wal.stat().st_size if wal.exists() else 0
    if (
        _sha256_file(db_path) != before_sha256
        or after_wal_size != before_wal_size
        or end_data_version != start_data_version
    ):
        raise ReleaseV9Error(
            "database changed while preparing the consistent manifest snapshot"
        )
    encoded = (json.dumps(manifest, ensure_ascii=False, indent=2, sort_keys=True) + "\n").encode(
        "utf-8"
    )
    _atomic_write(manifest_path, encoded)
    manifest_sha256 = _sha256_bytes(encoded)
    return {
        "manifest_path": str(manifest_path),
        "manifest_sha256": manifest_sha256,
        "source_database_sha256": before_sha256,
        "content_count": len(inventory),
        "manual_evidence_excluded_count": sum(
            int(bool(item["manual_evidence_excluded"])) for item in inventory
        ),
    }


def _atomic_write(path: Path, encoded: bytes) -> None:
    temporary = ""
    try:
        with tempfile.NamedTemporaryFile(
            mode="wb", dir=path.parent, prefix=f".{path.name}.", delete=False
        ) as handle:
            handle.write(encoded)
            handle.flush()
            os.fsync(handle.fileno())
            temporary = handle.name
        os.replace(temporary, path)
    except OSError as error:
        if temporary:
            Path(temporary).unlink(missing_ok=True)
        raise ReleaseV9Error(f"cannot write {path}: {error}") from error


def _load_manifest(path: Path, expected_sha256: str) -> dict[str, Any]:
    expected_sha256 = _require_sha256(expected_sha256, label="manifest SHA-256")
    path = path.resolve()
    if not path.is_file():
        raise ReleaseV9Error(f"manifest must be an existing file: {path}")
    payload = path.read_bytes()
    if _sha256_bytes(payload) != expected_sha256:
        raise ReleaseV9Error("manifest file SHA-256 mismatch")
    try:
        manifest = json.loads(payload.decode("utf-8"))
    except (UnicodeDecodeError, json.JSONDecodeError) as error:
        raise ReleaseV9Error(f"manifest is not valid JSON: {error}") from error
    if not isinstance(manifest, dict) or manifest.get("schema_version") != MANIFEST_SCHEMA:
        raise ReleaseV9Error("unsupported v9 manifest schema")
    inventory = manifest.get("inventory")
    if not isinstance(inventory, list) or not inventory:
        raise ReleaseV9Error("manifest inventory must be non-empty")
    if manifest.get("inventory_sha256") != _sha256_bytes(
        _canonical_json(inventory).encode("utf-8")
    ):
        raise ReleaseV9Error("manifest inventory hash mismatch")
    try:
        content_ids = [int(item["content_id"]) for item in inventory]
    except (KeyError, TypeError, ValueError) as error:
        raise ReleaseV9Error("manifest inventory content IDs are invalid") from error
    if content_ids != sorted(set(content_ids)):
        raise ReleaseV9Error(
            "manifest inventory content IDs must be unique and strictly increasing"
        )
    if (
        int(manifest.get("content_count") or -1) != len(inventory)
        or int(manifest.get("content_high_water") or -1) != content_ids[-1]
    ):
        raise ReleaseV9Error("manifest inventory count or high-water is invalid")
    if manifest.get("manual_evidence_policy") != "excluded-from-evaluation-v9":
        raise ReleaseV9Error("manifest manual-evidence policy is invalid")
    for key in ("protected_state", "activation_stable_state", "fast_guard"):
        if not isinstance(manifest.get(key), Mapping):
            raise ReleaseV9Error(f"manifest {key} is invalid")
    _manifest_legacy_queue_snapshot(manifest)
    return manifest


def _require_manifest_database(db_path: Path, manifest: Mapping[str, Any]) -> None:
    if Path(str(manifest.get("source_database"))).resolve() != db_path.resolve():
        raise ReleaseV9Error("manifest is bound to a different database")
    if int(manifest.get("database_user_version") or 0) != SCHEMA_VERSION:
        raise ReleaseV9Error("manifest schema version is inconsistent")
    expected = {
        "id": TARGET_RELEASE_ID,
        "rule_version": TARGET_RULE_VERSION,
        "taxonomy_version": TAXONOMY_VERSION,
    }
    target = manifest.get("target_release")
    if not isinstance(target, Mapping) or any(target.get(key) != value for key, value in expected.items()):
        raise ReleaseV9Error("manifest target release contract is invalid")


def _require_protected(
    connection: sqlite3.Connection, manifest: Mapping[str, Any]
) -> None:
    expected = manifest.get("protected_state")
    if not isinstance(expected, Mapping) or _canonical_json(_protected_state(connection)) != _canonical_json(expected):
        raise ReleaseV9Error("protected source state changed after manifest freeze")
    _require_legacy_queue_snapshot(connection, manifest)
    if _report_revision_count(connection):
        raise ReleaseV9Error("v8.6 report revision boundary has been crossed")


def _inventory_rows(manifest: Mapping[str, Any]) -> list[dict[str, Any]]:
    rows = manifest["inventory"]
    assert isinstance(rows, list)
    return [dict(row) for row in rows]


def _require_inventory(
    connection: sqlite3.Connection,
    manifest: Mapping[str, Any],
    *,
    items: Sequence[Mapping[str, Any]] | None = None,
) -> None:
    all_manifest_items = _inventory_rows(manifest)
    candidates = list(items) if items is not None else all_manifest_items
    candidate_ids = [int(item["content_id"]) for item in candidates]
    source = _latest_source_evaluations(
        connection,
        content_ids=candidate_ids if items is not None else None,
    )
    if items is None:
        expected_ids = [int(item["content_id"]) for item in all_manifest_items]
        actual_ids = sorted(source)
        if actual_ids != expected_ids:
            missing = sorted(set(actual_ids) - set(expected_ids))[:10]
            extra = sorted(set(expected_ids) - set(actual_ids))[:10]
            raise ReleaseV9Error(
                "manifest inventory is not the exact source automatic selector; "
                f"omitted_from_manifest={missing}, missing_from_source={extra}"
            )
    for item in candidates:
        content_id = int(item["content_id"])
        row = source.get(content_id)
        if row is None or int(row["id"]) != int(item["source_evaluation_id"]):
            raise ReleaseV9Error(f"content {content_id} source evaluation changed")
        source_components, source_sha = _rule_evidence(
            connection, content_id, SOURCE_RULE_VERSION
        )
        v9_components, v9_sha = _rule_evidence(
            connection, content_id, TARGET_RULE_VERSION
        )
        if (
            source_sha != item["source_evidence_sha256"]
            or _canonical_json(source_components)
            != _canonical_json(item["source_evidence_components"])
            or v9_sha != item["v9_evidence_sha256"]
            or _canonical_json(v9_components)
            != _canonical_json(item["v9_evidence_components"])
        ):
            raise ReleaseV9Error(f"content {content_id} evidence changed")


def _target_contract(
    connection: sqlite3.Connection, *, allowed_statuses: set[str]
) -> sqlite3.Row:
    target = _release(connection, TARGET_RELEASE_ID)
    if (
        str(target["rule_version"]) != TARGET_RULE_VERSION
        or str(target["taxonomy_version"]) != TAXONOMY_VERSION
        or str(target["matcher_rule_sha256"])
        != str(_release(connection, SOURCE_RELEASE_ID)["matcher_rule_sha256"])
        or str(target["status"]) not in allowed_statuses
    ):
        raise ReleaseV9Error("target release contract or state is inconsistent")
    return target


def status(
    *, db_path: Path, manifest_path: Path, manifest_sha256: str
) -> dict[str, Any]:
    manifest = _load_manifest(manifest_path, manifest_sha256)
    _require_manifest_database(db_path, manifest)
    with _connect(db_path, read_only=True) as connection:
        _require_schema(connection)
        target = connection.execute(
            "SELECT * FROM evaluation_releases WHERE id=?", (TARGET_RELEASE_ID,)
        ).fetchone()
        counts = [
            dict(row)
            for row in connection.execute(
                """
                SELECT evaluation_source,COUNT(*) total,
                       SUM(CASE WHEN invalidated_at IS NULL THEN 1 ELSE 0 END) valid
                FROM evaluation_versions WHERE release_id=? GROUP BY evaluation_source
                ORDER BY evaluation_source
                """,
                (TARGET_RELEASE_ID,),
            )
        ]
        return {
            "source_release": dict(_release(connection, SOURCE_RELEASE_ID)),
            "target_release": dict(target) if target is not None else None,
            "target_evaluation_counts": counts,
            "manifest_sha256": manifest_sha256,
            "v8_6_report_revision_count": _report_revision_count(connection),
        }


def create(
    *, db_path: Path, manifest_path: Path, manifest_sha256: str
) -> dict[str, Any]:
    manifest = _load_manifest(manifest_path, manifest_sha256)
    _require_manifest_database(db_path, manifest)
    with _connect(db_path, read_only=False) as connection, transaction(connection):
        _require_schema(connection)
        _require_source_contract(connection)
        _require_protected(connection, manifest)
        _require_inventory(connection, manifest)
        existing = connection.execute(
            "SELECT * FROM evaluation_releases WHERE id=?", (TARGET_RELEASE_ID,)
        ).fetchone()
        by_pair = connection.execute(
            "SELECT * FROM evaluation_releases WHERE rule_version=? AND taxonomy_version=?",
            (TARGET_RULE_VERSION, TAXONOMY_VERSION),
        ).fetchone()
        matcher_sha = str(manifest["target_release"]["matcher_rule_sha256"])
        if existing is None and by_pair is None:
            if _sha256_file(db_path.resolve()) != manifest["source_database_sha256"]:
                raise ReleaseV9Error("database file SHA-256 differs from frozen source")
            captured_at = now_utc()
            connection.execute(
                """
                INSERT INTO evaluation_releases(
                    id,rule_version,taxonomy_version,matcher_rule_sha256,status,
                    created_at,updated_at
                ) VALUES (?,?,?,?, 'draft',?,?)
                """,
                (
                    TARGET_RELEASE_ID,
                    TARGET_RULE_VERSION,
                    TAXONOMY_VERSION,
                    matcher_sha,
                    captured_at,
                    captured_at,
                ),
            )
        elif (
            existing is None
            or by_pair is None
            or str(existing["id"]) != str(by_pair["id"])
        ):
            raise ReleaseV9Error("target release id or version pair is occupied")
        target = _target_contract(connection, allowed_statuses={"draft"})
        if str(target["matcher_rule_sha256"]) != matcher_sha:
            raise ReleaseV9Error("target matcher SHA differs from manifest")
        _checkpoint("create_draft")
    return status(
        db_path=db_path,
        manifest_path=manifest_path,
        manifest_sha256=manifest_sha256,
    )


def _batches(
    rows: Sequence[dict[str, Any]], size: int | None = None
) -> Iterator[tuple[dict[str, Any], ...]]:
    size = BATCH_SIZE if size is None else size
    if size <= 0:
        raise ReleaseV9Error("batch size must be positive")
    for offset in range(0, len(rows), size):
        yield tuple(rows[offset : offset + size])


def _cas_status(
    connection: sqlite3.Connection, old: str, new: str, *, extra: str = "", parameters: Sequence[Any] = ()
) -> None:
    captured_at = now_utc()
    cursor = connection.execute(
        f"UPDATE evaluation_releases SET status=?,updated_at=?{extra} WHERE id=? AND status=?",
        (new, captured_at, *parameters, TARGET_RELEASE_ID, old),
    )
    if cursor.rowcount != 1:
        raise ReleaseV9Error(f"target release {old}->{new} CAS failed")


def backfill(
    *, db_path: Path, manifest_path: Path, manifest_sha256: str
) -> dict[str, Any]:
    manifest = _load_manifest(manifest_path, manifest_sha256)
    _require_manifest_database(db_path, manifest)
    with _connect(db_path, read_only=False) as connection, transaction(connection):
        _require_schema(connection)
        _require_source_contract(connection)
        _require_protected(connection, manifest)
        _require_inventory(connection, manifest)
        target = _target_contract(connection, allowed_statuses={"draft", "backfilling"})
        if str(target["status"]) == "draft":
            _cas_status(connection, "draft", "backfilling")
        _checkpoint("backfill_started")
    created = reused = completed_batches = 0
    rows = _inventory_rows(manifest)
    baseline_provider_rows = int(
        manifest["protected_state"]["provider_usage"]["row_count"]
    )
    current_provider_rows = baseline_provider_rows
    for batch in _batches(rows):
        with _connect(db_path, read_only=True) as connection:
            _require_schema_identity(connection)
            _target_contract(connection, allowed_statuses={"backfilling"})
            _require_fast_guard(connection, manifest)
            _require_inventory(connection, manifest, items=batch)
        for item in batch:
            result = evaluation.evaluate_release_content(
                int(item["content_id"]),
                release_id=TARGET_RELEASE_ID,
                db_path=db_path,
            )
            created += int(result.created)
            reused += int(not result.created)
        with _connect(db_path, read_only=True) as connection:
            _require_schema_identity(connection)
            _target_contract(connection, allowed_statuses={"backfilling"})
            _require_fast_guard(connection, manifest)
            _require_inventory(connection, manifest, items=batch)
            current_provider_rows = int(
                connection.execute("SELECT COUNT(*) FROM provider_usage").fetchone()[0]
            )
        completed_batches += 1
        _checkpoint("backfill_batch_verified")
    return {
        "release_id": TARGET_RELEASE_ID,
        "status": "backfilling",
        "target_count": len(rows),
        "batch_size": BATCH_SIZE,
        "completed_batches": completed_batches,
        "created": created,
        "reused": reused,
        "provider_usage_rows_added": current_provider_rows
        - baseline_provider_rows,
    }


def _normalized_payload(value: str) -> dict[str, Any]:
    payload = json.loads(value)
    if not isinstance(payload, dict):
        raise ReleaseV9Error("evaluation payload must be an object")
    payload.pop("pending_review", None)
    payload.pop("release_id", None)
    return payload


def _target_semantic_core(
    connection: sqlite3.Connection,
    manifest: Mapping[str, Any],
    *,
    allowed_statuses: set[str] | None = None,
    require_protected: bool = True,
) -> dict[str, Any]:
    if require_protected:
        _require_protected(connection, manifest)
    _require_inventory(connection, manifest)
    target = _target_contract(
        connection,
        allowed_statuses=allowed_statuses or {"backfilling", "ready"},
    )
    rows = connection.execute(
        "SELECT * FROM evaluation_versions WHERE release_id=? ORDER BY content_id,id",
        (TARGET_RELEASE_ID,),
    ).fetchall()
    inventory = _inventory_rows(manifest)
    if len(rows) != len(inventory):
        raise ReleaseV9Error("target release evaluation coverage is not exact")
    actual_ids = [int(row["content_id"]) for row in rows]
    expected_ids = [int(item["content_id"]) for item in inventory]
    if actual_ids != expected_ids or len(set(actual_ids)) != len(actual_ids):
        raise ReleaseV9Error("target release has missing or extra evaluations")
    semantic_rows: list[dict[str, Any]] = []
    for item, row in zip(inventory, rows, strict=True):
        content_id = int(item["content_id"])
        if (
            str(row["evaluation_source"]) != "automatic"
            or row["parent_evaluation_id"] is not None
            or row["review_id"] is not None
            or row["invalidated_at"] is not None
            or bool(row["pending_review"])
            or str(row["rule_version"]) != TARGET_RULE_VERSION
            or str(row["taxonomy_version"]) != TAXONOMY_VERSION
            or str(row["matcher_rule_sha256"]) != str(target["matcher_rule_sha256"])
            or str(row["evidence_sha256"]) != item["v9_evidence_sha256"]
        ):
            raise ReleaseV9Error(f"content {content_id} target evaluation is invalid")
        envelope = connection.execute(
            "SELECT * FROM evidence_envelopes WHERE id=?",
            (row["evidence_envelope_id"],),
        ).fetchone()
        if not _envelope_matches(
            envelope,
            content_id=content_id,
            evidence_sha256=str(item["v9_evidence_sha256"]),
            components=item["v9_evidence_components"],
        ) or envelope is None or envelope["manual_evidence_sha256"] is not None:
            raise ReleaseV9Error(f"content {content_id} v9 envelope is invalid")
        payload = json.loads(str(row["payload_json"]))
        if (
            not isinstance(payload, dict)
            or bool(payload.get("pending_review"))
            or payload.get("release_id") != TARGET_RELEASE_ID
            or payload.get("evaluation_source") != "automatic"
        ):
            raise ReleaseV9Error(f"content {content_id} v9 payload is invalid")
        target_projection = {key: row[key] for key in EVALUATION_COMPARE_COLUMNS}
        source_projection = item["source_evaluation"]
        if not bool(item["manual_evidence_excluded"]):
            if any(
                target_projection[key] != source_projection[key]
                for key in EVALUATION_COMPARE_COLUMNS
            ):
                raise ReleaseV9Error(
                    f"content {content_id} changed semantics outside v9 allowances"
                )
            if _normalized_payload(str(row["payload_json"])) != _normalized_payload(
                str(source_projection["payload_json"])
            ):
                raise ReleaseV9Error(
                    f"content {content_id} payload changed outside v9 allowances"
                )
            if _matches(connection, int(row["id"])) != item["source_matches"]:
                raise ReleaseV9Error(
                    f"content {content_id} matches changed outside v9 allowances"
                )
        semantic_rows.append(
            {
                "content_id": content_id,
                "evaluation": target_projection,
                "evidence_sha256": row["evidence_sha256"],
                "payload": payload,
                "matches": _matches(connection, int(row["id"])),
                "manual_evidence_excluded": bool(item["manual_evidence_excluded"]),
            }
        )
    target_queue_count = int(
        connection.execute(
            """
            SELECT COUNT(*) FROM review_queue q
            JOIN evaluation_versions e ON e.id=q.evaluation_id
            WHERE e.release_id=?
            """,
            (TARGET_RELEASE_ID,),
        ).fetchone()[0]
    )
    target_review_count = int(
        connection.execute(
            """
            SELECT COUNT(*) FROM evaluation_reviews r
            JOIN evaluation_versions e ON e.id=r.resulting_evaluation_id
            WHERE e.release_id=?
            """,
            (TARGET_RELEASE_ID,),
        ).fetchone()[0]
    )
    if target_queue_count or target_review_count:
        raise ReleaseV9Error("target release created manual queue or review rows")
    _require_integrity(connection)
    return {
        "inventory_sha256": manifest["inventory_sha256"],
        "source_release_id": SOURCE_RELEASE_ID,
        "target_release_id": TARGET_RELEASE_ID,
        "rule_version": TARGET_RULE_VERSION,
        "taxonomy_version": TAXONOMY_VERSION,
        "matcher_rule_sha256": str(target["matcher_rule_sha256"]),
        "content_count": len(inventory),
        "pending_review_count": 0,
        "manual_queue_count": 0,
        "manual_review_count": 0,
        "provider_usage": manifest["protected_state"]["provider_usage"],
        "manual_evidence": manifest["protected_state"]["manual_evidence"],
        "semantic_sha256": _sha256_bytes(
            _canonical_json(semantic_rows).encode("utf-8")
        ),
    }


def _receipt_payload(core: Mapping[str, Any], manifest_file_sha256: str) -> dict[str, Any]:
    core_value = dict(core)
    return {
        "schema_version": RECEIPT_SCHEMA,
        "generated_at": now_utc(),
        "manifest_file_sha256": manifest_file_sha256,
        "core_sha256": _sha256_bytes(_canonical_json(core_value).encode("utf-8")),
        "core": core_value,
    }


def _write_receipt(path: Path, payload: Mapping[str, Any]) -> dict[str, Any]:
    path = path.resolve()
    if not path.parent.is_dir():
        raise ReleaseV9Error("receipt output parent must exist")
    if path.exists():
        try:
            existing = json.loads(path.read_text(encoding="utf-8"))
        except (OSError, UnicodeDecodeError, json.JSONDecodeError) as error:
            raise ReleaseV9Error(f"existing receipt is invalid: {error}") from error
        existing_core = existing.get("core") if isinstance(existing, dict) else None
        if (
            not isinstance(existing, dict)
            or existing.get("schema_version") != RECEIPT_SCHEMA
            or existing.get("manifest_file_sha256")
            != payload.get("manifest_file_sha256")
            or not isinstance(existing_core, Mapping)
            or existing.get("core_sha256")
            != _sha256_bytes(_canonical_json(existing_core).encode("utf-8"))
            or _canonical_json(existing_core)
            != _canonical_json(payload.get("core"))
        ):
            raise ReleaseV9Error(
                "existing receipt differs from the current manifest binding"
            )
        return existing
    encoded = (json.dumps(payload, ensure_ascii=False, indent=2, sort_keys=True) + "\n").encode("utf-8")
    _atomic_write(path, encoded)
    return dict(payload)


def verify_ready(
    *,
    db_path: Path,
    manifest_path: Path,
    manifest_sha256: str,
    receipt_path: Path,
) -> dict[str, Any]:
    manifest = _load_manifest(manifest_path, manifest_sha256)
    _require_manifest_database(db_path, manifest)
    with _connect(db_path, read_only=False) as connection, transaction(connection):
        _require_schema(connection)
        target = _target_contract(
            connection, allowed_statuses={"backfilling", "ready"}
        )
        core = _target_semantic_core(connection, manifest)
        payload = _receipt_payload(core, manifest_sha256)
        if str(target["status"]) == "backfilling":
            _cas_status(connection, "backfilling", "ready")
            _checkpoint("verify_ready")
    stored = _write_receipt(receipt_path, payload)
    return {
        "release_id": TARGET_RELEASE_ID,
        "status": "ready",
        "receipt_path": str(receipt_path.resolve()),
        "receipt_sha256": _sha256_file(receipt_path.resolve()),
        "core_sha256": stored["core_sha256"],
        "core": stored["core"],
    }


def _load_receipt(path: Path, expected_sha256: str, manifest_sha256: str) -> dict[str, Any]:
    expected_sha256 = _require_sha256(expected_sha256, label="receipt SHA-256")
    path = path.resolve()
    if not path.is_file() or _sha256_file(path) != expected_sha256:
        raise ReleaseV9Error("receipt file SHA-256 mismatch")
    try:
        receipt = json.loads(path.read_text(encoding="utf-8"))
    except (OSError, UnicodeDecodeError, json.JSONDecodeError) as error:
        raise ReleaseV9Error(f"receipt is not valid JSON: {error}") from error
    if not isinstance(receipt, dict) or receipt.get("schema_version") != RECEIPT_SCHEMA:
        raise ReleaseV9Error("unsupported v9 receipt schema")
    core = receipt.get("core")
    if (
        not isinstance(core, Mapping)
        or receipt.get("manifest_file_sha256") != manifest_sha256
        or receipt.get("core_sha256")
        != _sha256_bytes(_canonical_json(core).encode("utf-8"))
    ):
        raise ReleaseV9Error("receipt semantic hash or manifest binding is invalid")
    return receipt


def _restore_legacy_queues(
    connection: sqlite3.Connection, manifest: Mapping[str, Any]
) -> None:
    rows = _manifest_legacy_queue_snapshot(manifest)
    for item in rows:
        cursor = connection.execute(
            """
            UPDATE review_queue
            SET evaluation_id=?,priority=?,status=?,assigned_to=?,created_at=?,
                updated_at=?,resolved_at=?
            WHERE id=? AND content_id=? AND reason_code=? AND status='resolved'
            """,
            (
                item["evaluation_id"],
                item["priority"],
                item["status"],
                item["assigned_to"],
                item["created_at"],
                item["updated_at"],
                item["resolved_at"],
                item["id"],
                item["content_id"],
                item["reason_code"],
            ),
        )
        if cursor.rowcount != 1:
            raise ReleaseV9Error(f"legacy review queue {item['id']} cannot be restored")
    _require_legacy_queue_snapshot(connection, manifest)


def _refresh_direction_cache(connection: sqlite3.Connection, release_id: str) -> int:
    cursor = connection.execute(
        """
        UPDATE content_items AS c
        SET evaluation_content_direction=COALESCE((
            SELECT e.content_direction FROM evaluation_versions e
            WHERE e.content_id=c.id AND e.release_id=?
              AND e.evaluation_source='automatic' AND e.invalidated_at IS NULL
            ORDER BY e.evaluated_at DESC,e.id DESC LIMIT 1
        ),'unknown')
        """,
        (release_id,),
    )
    expected = int(connection.execute("SELECT COUNT(*) FROM content_items").fetchone()[0])
    if cursor.rowcount != expected:
        raise ReleaseV9Error("direction cache refresh did not cover every content row")
    return expected


def activate(
    *,
    db_path: Path,
    manifest_path: Path,
    manifest_sha256: str,
    receipt_path: Path,
    receipt_sha256: str,
) -> dict[str, Any]:
    manifest = _load_manifest(manifest_path, manifest_sha256)
    _require_manifest_database(db_path, manifest)
    receipt = _load_receipt(receipt_path, receipt_sha256, manifest_sha256)
    with _connect(db_path, read_only=False) as connection, transaction(connection):
        _require_schema(connection)
        _require_source_contract(connection)
        _require_protected(connection, manifest)
        target = _target_contract(connection, allowed_statuses={"ready"})
        core = _target_semantic_core(connection, manifest)
        if _canonical_json(core) != _canonical_json(receipt["core"]):
            raise ReleaseV9Error("receipt no longer matches target release semantics")
        legacy_queues = _require_legacy_queue_snapshot(connection, manifest)
        captured_at = now_utc()
        source_cursor = connection.execute(
            """
            UPDATE evaluation_releases
            SET status='retired',updated_at=?,retired_at=?
            WHERE id=? AND status='active'
            """,
            (captured_at, captured_at, SOURCE_RELEASE_ID),
        )
        if source_cursor.rowcount != 1:
            raise ReleaseV9Error("source active-to-retired CAS failed")
        _checkpoint("activation_source_retired")
        _cas_status(
            connection,
            "ready",
            "active",
            extra=",activated_at=?,failure_reason=NULL",
            parameters=(captured_at,),
        )
        _checkpoint("activation_target_active")
        placeholders = ",".join("?" for _ in QUEUE_REASONS)
        statuses = ",".join("?" for _ in ACTIVE_QUEUE_STATUSES)
        queue_cursor = connection.execute(
            f"""
            UPDATE review_queue
            SET status='resolved',resolved_at=?,updated_at=?
            WHERE reason_code IN ({placeholders}) AND status IN ({statuses})
            """,
            (captured_at, captured_at, *QUEUE_REASONS, *ACTIVE_QUEUE_STATUSES),
        )
        if queue_cursor.rowcount != len(legacy_queues):
            raise ReleaseV9Error("legacy queue close count differs from manifest")
        remaining_legacy_queues = int(
            connection.execute(
                f"""
                SELECT COUNT(*) FROM review_queue
                WHERE reason_code IN ({placeholders}) AND status IN ({statuses})
                """,
                (*QUEUE_REASONS, *ACTIVE_QUEUE_STATUSES),
            ).fetchone()[0]
        )
        if remaining_legacy_queues:
            raise ReleaseV9Error("legacy queues remained active after v9 activation")
        _checkpoint("activation_legacy_queues_closed")
        content_count = _refresh_direction_cache(connection, TARGET_RELEASE_ID)
        _checkpoint("activation_direction_cache_refreshed")
        active = connection.execute(
            "SELECT id FROM evaluation_releases WHERE status='active'"
        ).fetchall()
        if [str(row["id"]) for row in active] != [TARGET_RELEASE_ID]:
            raise ReleaseV9Error("activation did not leave exactly one v9 release active")
        if str(target["taxonomy_version"]) != TAXONOMY_VERSION:
            raise ReleaseV9Error("activation changed the shared taxonomy contract")
        _require_integrity(connection)
        _checkpoint("activation_integrity_verified")
    return {
        "release_id": TARGET_RELEASE_ID,
        "status": "active",
        "content_direction_rows": content_count,
        "receipt_sha256": receipt_sha256,
    }


def abort(
    *,
    db_path: Path,
    manifest_path: Path,
    manifest_sha256: str,
    reason: str,
) -> dict[str, Any]:
    reason = reason.strip()
    if not reason:
        raise ReleaseV9Error("abort reason must not be empty")
    manifest = _load_manifest(manifest_path, manifest_sha256)
    _require_manifest_database(db_path, manifest)
    drift_summary: dict[str, Any] = {}
    with _connect(db_path, read_only=False) as connection, transaction(connection):
        _require_schema(connection)
        active = connection.execute(
            "SELECT id FROM evaluation_releases WHERE status='active' ORDER BY id"
        ).fetchall()
        source = _release(connection, SOURCE_RELEASE_ID)
        expected_source = manifest["source_release"]
        if [str(row["id"]) for row in active] != [SOURCE_RELEASE_ID] or any(
            str(source[key]) != str(expected_source[key])
            for key in (
                "id",
                "rule_version",
                "taxonomy_version",
                "matcher_rule_sha256",
            )
        ):
            raise ReleaseV9Error(
                "abort requires the manifest source release to remain active"
            )
        target = _target_contract(
            connection, allowed_statuses={"draft", "backfilling", "ready", "failed"}
        )
        try:
            current_guard = _protected_state(connection)
            expected_guard = manifest["protected_state"]
            if not isinstance(expected_guard, Mapping):
                raise ReleaseV9Error("manifest protected state is invalid")
            drift_summary = {
                key: {
                    "expected": expected_guard.get(key),
                    "actual": current_guard.get(key),
                }
                for key in sorted(set(expected_guard) | set(current_guard))
                if _canonical_json(expected_guard.get(key))
                != _canonical_json(current_guard.get(key))
            }
        except Exception as error:
            drift_summary = {"guard_error": str(error)}
        if str(target["status"]) != "failed":
            captured_at = now_utc()
            failure = _canonical_json(
                {
                    "reason": reason,
                    "protected_drift": drift_summary,
                    "manifest_sha256": manifest_sha256,
                }
            )
            connection.execute(
                """
                UPDATE evaluation_versions
                SET invalidated_at=?,invalidation_reason=?
                WHERE release_id=? AND invalidated_at IS NULL
                """,
                (captured_at, failure, TARGET_RELEASE_ID),
            )
            _cas_status(
                connection,
                str(target["status"]),
                "failed",
                extra=",failure_reason=?",
                parameters=(failure,),
            )
            _checkpoint("abort_failed")
    return {
        "release_id": TARGET_RELEASE_ID,
        "status": "failed",
        "reason": reason,
        "protected_drift": drift_summary,
    }


def rollback_before_resume(
    *,
    db_path: Path,
    manifest_path: Path,
    manifest_sha256: str,
    receipt_path: Path,
    receipt_sha256: str,
    reason: str,
) -> dict[str, Any]:
    reason = reason.strip()
    if not reason:
        raise ReleaseV9Error("rollback reason must not be empty")
    manifest = _load_manifest(manifest_path, manifest_sha256)
    _require_manifest_database(db_path, manifest)
    receipt = _load_receipt(receipt_path, receipt_sha256, manifest_sha256)
    with _connect(db_path, read_only=False) as connection, transaction(connection):
        _require_schema(connection)
        if _report_revision_count(connection):
            raise ReleaseV9Error("v8.6 report revisions exist; rollback is forbidden")
        source = _release(connection, SOURCE_RELEASE_ID)
        target = _target_contract(connection, allowed_statuses={"active"})
        if str(source["status"]) != "retired" or str(target["status"]) != "active":
            raise ReleaseV9Error("releases are not at the rollback-before-resume boundary")
        if str(target["matcher_rule_sha256"]) != str(
            manifest["target_release"]["matcher_rule_sha256"]
        ):
            raise ReleaseV9Error("target release changed after activation")
        _require_activation_stable(connection, manifest)
        current_core = _target_semantic_core(
            connection,
            manifest,
            allowed_statuses={"active"},
            require_protected=False,
        )
        if _canonical_json(current_core) != _canonical_json(receipt["core"]):
            raise ReleaseV9Error("target semantics changed after activation")
        captured_at = now_utc()
        _cas_status(
            connection,
            "active",
            "retired",
            extra=",retired_at=?,failure_reason=?",
            parameters=(captured_at, reason),
        )
        source_cursor = connection.execute(
            """
            UPDATE evaluation_releases
            SET status='active',updated_at=?,retired_at=NULL
            WHERE id=? AND status='retired'
            """,
            (captured_at, SOURCE_RELEASE_ID),
        )
        if source_cursor.rowcount != 1:
            raise ReleaseV9Error("source retired-to-active CAS failed")
        _restore_legacy_queues(connection, manifest)
        content_count = _refresh_direction_cache(connection, SOURCE_RELEASE_ID)
        active = connection.execute(
            "SELECT id FROM evaluation_releases WHERE status='active'"
        ).fetchall()
        if [str(row["id"]) for row in active] != [SOURCE_RELEASE_ID]:
            raise ReleaseV9Error("rollback did not restore the source release")
        _require_integrity(connection)
        _checkpoint("rollback_integrity_verified")
    return {
        "release_id": TARGET_RELEASE_ID,
        "status": "retired",
        "restored_release_id": SOURCE_RELEASE_ID,
        "content_direction_rows": content_count,
        "reason": reason,
    }
