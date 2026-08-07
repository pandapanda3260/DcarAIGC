"""Append-only repair for frozen legacy evaluations with illegal point/scene pairs."""

from __future__ import annotations

import hashlib
import json
import re
import sqlite3
from collections import Counter, deque
from collections.abc import Mapping, Sequence
from dataclasses import dataclass
from pathlib import Path
from typing import Any

from .evaluation import canonical_json
from .evaluation_selectors import review_anchor_evaluation
from .release_management_v5_1 import (
    FreezeManifest,
    ReleaseManagementError,
    TARGET_RELEASE_ID,
    TARGET_TAXONOMY_VERSION,
    _existing_connection,
    _json_safe,
    _load_freeze_manifest,
    _protected_state,
    _read_receipt,
    _require_production_receipt_chain,
    _require_v9,
    _rows_sha256,
    _semantic_core,
    _target_taxonomy,
    _target_taxonomy_semantic_sha256,
)
from .report_repair import (
    INVALIDATION_EVENT_TYPE as REPORT_INVALIDATION_EVENT_TYPE,
    INVALIDATION_REASON as REPORT_INVALIDATION_REASON,
    ReportRepairBoundary,
    _event_payload as report_invalidation_event_payload,
    _load_manifest_boundary as load_report_repair_boundary,
)
from .storage import (
    LEGACY_V6_RELEASE_ID,
    LEGACY_V7_RELEASE_ID,
    now_utc,
    transaction,
)


REPAIR_CONTRACT = "legacy-illegal-point-scene-repair-v1"
QUEUE_REASON_CODE = "legacy_nonautomatic_point_scene_conflict_v1"
INVALIDATION_REASON_PREFIX = "legacy_automatic_illegal_point_scene_chain_v1"
HEX_SHA256 = re.compile(r"[0-9a-f]{64}", re.ASCII)
LEGACY_RELEASE_IDS = (LEGACY_V6_RELEASE_ID, LEGACY_V7_RELEASE_ID)
APPROVED_LOGICAL_SNAPSHOT_SHA256 = (
    "5ab48e57278d4169460b022b6fa93a6b05cc3930d0dd42c61ff401e2f1365b0b"
)
APPROVED_PLAN_EXPECTATIONS: Mapping[str, Any] = {
    "automatic_evaluation_count": 1514,
    "automatic_content_count": 713,
    "automatic_by_release": {
        LEGACY_V6_RELEASE_ID: 696,
        LEGACY_V7_RELEASE_ID: 818,
    },
    "automatic_evaluation_ids_sha256": (
        "49ca365c5f67c0bf2bcb7bf831c57fc9b0276efe1a2bda8513a6ede1fb5142c8"
    ),
    "automatic_content_ids_sha256": (
        "7557a3c88ea9dab574f510db566808d4df7aef3f85b56783f69c6d3409c38ca7"
    ),
    "nonautomatic_evaluation_count": 5,
    "nonautomatic_content_count": 4,
    "nonautomatic_evaluation_ids_sha256": (
        "a2d416d7a8be4626ea35f8ffb9f65fabff2532a2e3e208e06102574e81bbfcad"
    ),
    "nonautomatic_content_ids_sha256": (
        "6b9b08917af32d1b8585dc91d9c0dad1acbf095adea86d40efada0a602f6c6da"
    ),
    "illegal_match_count": 1728,
}
PROTECTED_TABLES = (
    "provider_usage",
    "scheduler_runs",
    "report_tasks",
    "report_revisions",
    "report_files",
    "task_events",
    "evidence_artifacts",
)


class LegacySceneRepairError(RuntimeError):
    """Raised before commit when the frozen repair boundary cannot be proven."""


@dataclass(frozen=True)
class LegacySceneRepairBoundary:
    manifest: FreezeManifest
    report_boundary: ReportRepairBoundary
    content_ids: tuple[int, ...]
    evaluation_high_water: int
    frozen_evaluation_count: int
    frozen_match_count: int
    frozen_review_queue_count: int

    @property
    def audit_id(self) -> str:
        return f"{REPAIR_CONTRACT}__{self.manifest.logical_snapshot_sha256}"


def _sha256_json(value: Any) -> str:
    return hashlib.sha256(canonical_json(value).encode("utf-8")).hexdigest()


def _require_sha256(value: Any, *, label: str) -> str:
    normalized = str(value or "")
    if HEX_SHA256.fullmatch(normalized) is None:
        raise LegacySceneRepairError(f"{label} must be a lowercase SHA-256")
    return normalized


def _require_reason(value: str) -> str:
    normalized = value.strip()
    if not normalized:
        raise LegacySceneRepairError("operator reason is required")
    if len(normalized) > 500 or any(ord(character) < 32 for character in normalized):
        raise LegacySceneRepairError(
            "operator reason must be at most 500 printable characters"
        )
    return normalized


def _read_json_object(path: Path, *, label: str) -> dict[str, Any]:
    try:
        value = json.loads(path.read_text(encoding="utf-8"))
    except (OSError, UnicodeDecodeError, json.JSONDecodeError) as error:
        raise LegacySceneRepairError(f"cannot read {label}: {error}") from error
    if not isinstance(value, dict):
        raise LegacySceneRepairError(f"{label} must be a JSON object")
    return value


def _load_boundary(manifest_path: Path) -> LegacySceneRepairBoundary:
    manifest_path = manifest_path.resolve()
    try:
        manifest = _load_freeze_manifest(manifest_path)
        report_boundary = load_report_repair_boundary(manifest_path)
    except (ReleaseManagementError, RuntimeError) as error:
        raise LegacySceneRepairError(str(error)) from error
    raw = _read_json_object(manifest_path, label="freeze manifest")
    if hashlib.sha256(manifest_path.read_bytes()).hexdigest() != manifest.sha256:
        raise LegacySceneRepairError("freeze manifest changed while it was validated")
    snapshot = raw.get("table_snapshot")
    if not isinstance(snapshot, Mapping):
        raise LegacySceneRepairError("freeze manifest table snapshot is missing")
    evaluation = snapshot.get("evaluation_versions")
    matches = snapshot.get("evaluation_matches")
    queues = snapshot.get("review_queue")
    if not all(isinstance(item, Mapping) for item in (evaluation, matches, queues)):
        raise LegacySceneRepairError("freeze manifest legacy table anchors are missing")
    assert isinstance(evaluation, Mapping)
    assert isinstance(matches, Mapping)
    assert isinstance(queues, Mapping)
    try:
        high_water = int(evaluation["max_id"])
        evaluation_count = int(evaluation["count"])
        match_count = int(matches["count"])
        queue_count = int(queues["count"])
    except (KeyError, TypeError, ValueError) as error:
        raise LegacySceneRepairError(
            "freeze manifest legacy table anchors are invalid"
        ) from error
    content_ids = tuple(item.content_id for item in manifest.contents)
    if (
        high_water < 1
        or evaluation_count < 1
        or len(content_ids) != len(set(content_ids))
        or tuple(sorted(content_ids)) != content_ids
    ):
        raise LegacySceneRepairError("freeze manifest high-water boundary is invalid")
    if report_boundary.manifest_sha256 != manifest.sha256:
        raise LegacySceneRepairError("report repair boundary uses another manifest")
    return LegacySceneRepairBoundary(
        manifest=manifest,
        report_boundary=report_boundary,
        content_ids=content_ids,
        evaluation_high_water=high_water,
        frozen_evaluation_count=evaluation_count,
        frozen_match_count=match_count,
        frozen_review_queue_count=queue_count,
    )


def _row_projection_hash(
    connection: sqlite3.Connection,
    query: str,
    parameters: Sequence[Any] = (),
) -> str:
    return _sha256_json(
        [list(row) for row in connection.execute(query, parameters).fetchall()]
    )


def _table_projection_hash(
    connection: sqlite3.Connection,
    table: str,
    *,
    where: str = "",
    parameters: Sequence[Any] = (),
) -> str:
    columns = [
        str(row[1]) for row in connection.execute(f'PRAGMA table_info("{table}")')
    ]
    if not columns:
        raise LegacySceneRepairError(f"required table is missing: {table}")
    order = ",".join(f'"{column}"' for column in columns)
    return _row_projection_hash(
        connection,
        f'SELECT * FROM "{table}" {where} ORDER BY {order}',
        parameters,
    )


def _query_rows_sha256(
    connection: sqlite3.Connection,
    query: str,
    parameters: Sequence[Any] = (),
) -> tuple[int, str]:
    digest = hashlib.sha256()
    count = 0
    for row in connection.execute(query, parameters):
        digest.update(
            canonical_json([_json_safe(value) for value in row]).encode("utf-8")
        )
        digest.update(b"\n")
        count += 1
    return count, digest.hexdigest()


def _target_semantic_sha256(connection: sqlite3.Connection) -> str:
    try:
        taxonomy, _ = _target_taxonomy(connection, expected_status="published")
        return _target_taxonomy_semantic_sha256(connection, taxonomy)
    except ReleaseManagementError as error:
        raise LegacySceneRepairError(str(error)) from error


def _require_report_repair_completed(
    connection: sqlite3.Connection,
    boundary: LegacySceneRepairBoundary,
) -> tuple[int, ...]:
    report_boundary = boundary.report_boundary
    expected_keys = {target.key for target in report_boundary.targets}
    actual_keys = {
        (str(row["task_id"]), int(row["revision"]))
        for row in connection.execute(
            """
            SELECT rr.task_id,rr.revision FROM report_revisions rr
            JOIN report_tasks rt ON rt.id=rr.task_id
            WHERE rr.contract_version='dcar-content-operations-report-v8.3'
              AND rt.creation_source='automatic'
              AND rr.release_id<>?
            ORDER BY rr.task_id,rr.revision
            """,
            (TARGET_RELEASE_ID,),
        )
    }
    if actual_keys != expected_keys:
        raise LegacySceneRepairError(
            "legacy scene repair requires the exact manifest report repair set"
        )
    event_ids: list[int] = []
    for target in report_boundary.targets:
        row = connection.execute(
            """
            SELECT invalidated_at,invalidation_reason,release_id
            FROM report_revisions WHERE task_id=? AND revision=?
            """,
            target.key,
        ).fetchone()
        if (
            row is None
            or str(row["release_id"]) != LEGACY_V7_RELEASE_ID
            or row["invalidated_at"] is None
            or str(row["invalidation_reason"]) != REPORT_INVALIDATION_REASON
        ):
            raise LegacySceneRepairError(
                f"manifest report repair is incomplete: {target.key}"
            )
        payload = report_invalidation_event_payload(
            target, manifest_sha256=report_boundary.manifest_sha256
        )
        events = connection.execute(
            """
            SELECT id,message,payload_json,created_at FROM task_events
            WHERE task_id=? AND event_type=? AND payload_json=?
            """,
            (target.task_id, REPORT_INVALIDATION_EVENT_TYPE, payload),
        ).fetchall()
        if (
            len(events) != 1
            or str(events[0]["created_at"]) != str(row["invalidated_at"])
            or str(events[0]["message"])
            != f"revision {target.revision} invalidated from freeze manifest"
        ):
            raise LegacySceneRepairError(
                f"manifest report repair event is incomplete: {target.key}"
            )
        event_ids.append(int(events[0]["id"]))
    ordered = tuple(sorted(event_ids))
    if len(ordered) != len(set(ordered)) or any(
        current != ordered[0] + index for index, current in enumerate(ordered)
    ):
        raise LegacySceneRepairError(
            "manifest report repair event ids are not one contiguous batch"
        )
    return ordered


def _receipt_core(
    receipt: Mapping[str, Any], boundary: LegacySceneRepairBoundary
) -> Mapping[str, Any]:
    core = receipt.get("core")
    if not isinstance(core, Mapping):
        raise LegacySceneRepairError("production receipt semantic core is missing")
    if core.get("freeze_manifest_sha256") != boundary.manifest.sha256:
        raise LegacySceneRepairError("production receipt belongs to another manifest")
    release = core.get("release")
    if not isinstance(release, Mapping) or (
        str(release.get("id") or ""),
        str(release.get("rule_version") or ""),
        str(release.get("taxonomy_version") or ""),
    ) != (TARGET_RELEASE_ID, "evaluation-v8", TARGET_TAXONOMY_VERSION):
        raise LegacySceneRepairError(
            "production receipt does not attest target release"
        )
    return core


def _activation_stable_state(
    connection: sqlite3.Connection,
    *,
    receipt: Mapping[str, Any],
) -> Mapping[str, Any]:
    execution = receipt.get("execution")
    if not isinstance(execution, Mapping):
        raise LegacySceneRepairError("production receipt execution is missing")
    audit_id = str(execution.get("audit_id") or "")
    if not audit_id:
        raise LegacySceneRepairError("production receipt audit identity is missing")
    audit = connection.execute(
        "SELECT summary_json FROM migration_audit WHERE id=?", (audit_id,)
    ).fetchone()
    if audit is None:
        raise LegacySceneRepairError("production receipt audit is missing")
    try:
        summary = json.loads(str(audit["summary_json"]))
    except json.JSONDecodeError as error:
        raise LegacySceneRepairError(
            "production receipt audit JSON is invalid"
        ) from error
    state = (
        summary.get("activation_stable_state") if isinstance(summary, dict) else None
    )
    if not isinstance(state, Mapping):
        raise LegacySceneRepairError(
            "production receipt audit has no activation stable state"
        )
    state_sha256 = _require_sha256(
        state.get("state_sha256"), label="activation stable state hash"
    )
    material = dict(state)
    material.pop("state_sha256", None)
    if _sha256_json(material) != state_sha256:
        raise LegacySceneRepairError("activation stable state hash changed")
    if execution.get("activation_stable_state_sha256") != state_sha256:
        raise LegacySceneRepairError(
            "production receipt does not bind the activation stable state"
        )
    return state


def _normalized_report_revision_anchor(
    connection: sqlite3.Connection,
    *,
    boundary: LegacySceneRepairBoundary,
) -> tuple[int, str]:
    columns = [
        str(row[1]) for row in connection.execute("PRAGMA table_info(report_revisions)")
    ]
    if not boundary.report_boundary.targets:
        return _rows_sha256(
            connection,
            table="report_revisions",
            columns=tuple(columns),
        )
    target_predicate = " OR ".join(
        "(task_id=? AND revision=?)" for _ in boundary.report_boundary.targets
    )
    parameters: list[Any] = []
    for target in boundary.report_boundary.targets:
        parameters.extend(target.key)
    expressions = [
        (
            f'CASE WHEN {target_predicate} THEN NULL ELSE "{column}" END AS "{column}"'
            if column in {"invalidated_at", "invalidation_reason"}
            else f'"{column}"'
        )
        for column in columns
    ]
    order_by = ",".join(f'"{column}"' for column in columns)
    # The composite primary key is the first two columns, so this preserves the
    # exact row order used by release_management._rows_sha256.
    return _query_rows_sha256(
        connection,
        f"SELECT {','.join(expressions)} FROM report_revisions ORDER BY {order_by}",
        tuple(parameters) * 2,
    )


def _normalized_sequence_anchor(
    connection: sqlite3.Connection,
    *,
    report_event_ids: Sequence[int],
) -> tuple[int, str]:
    if report_event_ids:
        ordered = tuple(sorted(report_event_ids))
        base_task_event_sequence = ordered[0] - 1
        current = connection.execute(
            "SELECT seq FROM sqlite_sequence WHERE name='task_events'"
        ).fetchone()
        if current is None or int(current["seq"]) != ordered[-1]:
            raise LegacySceneRepairError(
                "task event sequence differs from the approved report repair batch"
            )
    else:
        base_task_event_sequence = 0
    return _query_rows_sha256(
        connection,
        """
        SELECT name,
               CASE WHEN name='task_events' AND ? > 0 THEN ? ELSE seq END
        FROM sqlite_sequence
        WHERE name NOT IN (
            'evaluation_versions','evidence_envelopes','migration_audit'
        )
        ORDER BY name,2
        """,
        (len(report_event_ids), base_task_event_sequence),
    )


def _require_pristine_attested_state(
    connection: sqlite3.Connection,
    *,
    boundary: LegacySceneRepairBoundary,
    receipt: Mapping[str, Any],
    report_event_ids: Sequence[int],
) -> dict[str, Any]:
    expected = _activation_stable_state(connection, receipt=receipt)
    envelope_max_id = int(expected.get("envelope_max_id") or 0)
    actual = _protected_state(
        connection,
        boundary.manifest,
        envelope_max_id=envelope_max_id,
        activation_stable=True,
    )
    expected_tables = expected.get("tables")
    actual_tables = actual.get("tables")
    if not isinstance(expected_tables, Mapping) or not isinstance(
        actual_tables, Mapping
    ):
        raise LegacySceneRepairError("activation stable table anchors are missing")
    if set(expected_tables) != set(actual_tables):
        raise LegacySceneRepairError("activation stable table set changed")
    allowed_drift = {"report_revisions", "task_events"}
    for table in expected_tables:
        if table in allowed_drift:
            continue
        if canonical_json(actual_tables[table]) != canonical_json(
            expected_tables[table]
        ):
            raise LegacySceneRepairError(
                f"protected table changed after activation: {table}"
            )
    report_count, report_sha256 = _normalized_report_revision_anchor(
        connection, boundary=boundary
    )
    if canonical_json(
        {"count": report_count, "rows_sha256": report_sha256}
    ) != canonical_json(expected_tables.get("report_revisions")):
        raise LegacySceneRepairError(
            "report revisions contain changes outside the approved repair"
        )
    task_event_columns = tuple(
        str(row[1]) for row in connection.execute("PRAGMA table_info(task_events)")
    )
    event_where = ""
    event_parameters: tuple[Any, ...] = ()
    if report_event_ids:
        event_where = f"WHERE id NOT IN ({','.join('?' for _ in report_event_ids)})"
        event_parameters = tuple(report_event_ids)
    event_count, event_sha256 = _rows_sha256(
        connection,
        table="task_events",
        columns=task_event_columns,
        where=event_where,
        parameters=event_parameters,
    )
    if canonical_json(
        {"count": event_count, "rows_sha256": event_sha256}
    ) != canonical_json(expected_tables.get("task_events")):
        raise LegacySceneRepairError(
            "task events contain changes outside the approved report repair"
        )
    sequence_count, sequence_sha256 = _normalized_sequence_anchor(
        connection, report_event_ids=report_event_ids
    )
    if canonical_json(
        {"count": sequence_count, "rows_sha256": sequence_sha256}
    ) != canonical_json(expected.get("sqlite_sequence")):
        raise LegacySceneRepairError(
            "SQLite sequences contain changes outside the approved report repair"
        )
    for key in (
        "contract",
        "envelope_max_id",
        "sqlite_master_sha256",
        "unmanaged_lifecycle_sha256",
    ):
        if actual.get(key) != expected.get(key):
            raise LegacySceneRepairError(f"activation stable boundary changed: {key}")
    return {
        "state_sha256": str(expected["state_sha256"]),
        "legacy_evaluation_count": int(expected_tables["evaluation_versions"]["count"]),
        "legacy_evaluation_rows_sha256": str(
            expected_tables["evaluation_versions"]["rows_sha256"]
        ),
        "legacy_match_count": int(expected_tables["evaluation_matches"]["count"]),
        "legacy_match_rows_sha256": str(
            expected_tables["evaluation_matches"]["rows_sha256"]
        ),
    }


def _require_target_receipt_semantics(
    connection: sqlite3.Connection,
    *,
    boundary: LegacySceneRepairBoundary,
    receipt: Mapping[str, Any],
    report_event_ids: Sequence[int],
) -> None:
    """Rebuild the pre-report-repair snapshot and reuse the release semantic gate."""

    expected_core = receipt.get("core")
    if not isinstance(expected_core, Mapping):
        raise LegacySceneRepairError("production receipt semantic core is missing")
    restored = sqlite3.connect(":memory:")
    try:
        restored.row_factory = sqlite3.Row
        database_rows = connection.execute("PRAGMA database_list").fetchall()
        main_database = next(
            (
                Path(str(row["file"])).resolve()
                for row in database_rows
                if str(row["name"]) == "main" and str(row["file"])
            ),
            None,
        )
        if main_database is None:
            raise LegacySceneRepairError("target semantic source database is missing")
        # The caller's BEGIN IMMEDIATE prevents concurrent writers. A separate
        # read connection can still backup the committed WAL snapshot without
        # the self-wait caused by backing up the write-owning connection.
        with _existing_connection(main_database, read_only=True) as reader:
            reader.backup(restored)
        restored.execute("PRAGMA foreign_keys=ON")
        if report_event_ids:
            placeholders = ",".join("?" for _ in report_event_ids)
            deleted = restored.execute(
                f"DELETE FROM task_events WHERE id IN ({placeholders})",
                tuple(report_event_ids),
            )
            if deleted.rowcount != len(report_event_ids):
                raise LegacySceneRepairError(
                    "approved report repair events could not be restored in snapshot"
                )
            base_sequence = min(report_event_ids) - 1
            sequence = restored.execute(
                "UPDATE sqlite_sequence SET seq=? WHERE name='task_events'",
                (base_sequence,),
            )
            if sequence.rowcount != 1:
                raise LegacySceneRepairError(
                    "approved report repair sequence could not be restored in snapshot"
                )
        for target in boundary.report_boundary.targets:
            reverted = restored.execute(
                """
                UPDATE report_revisions
                SET invalidated_at=NULL,invalidation_reason=NULL
                WHERE task_id=? AND revision=?
                """,
                target.key,
            )
            if reverted.rowcount != 1:
                raise LegacySceneRepairError(
                    f"approved report revision could not be restored: {target.key}"
                )
        try:
            actual_core = _semantic_core(
                restored,
                boundary.manifest,
                expected_taxonomy_status="published",
            )
        except ReleaseManagementError as error:
            raise LegacySceneRepairError(str(error)) from error
    except (sqlite3.Error, ReleaseManagementError) as error:
        raise LegacySceneRepairError(
            f"cannot verify target receipt semantics: {error}"
        ) from error
    finally:
        restored.close()
    if canonical_json(actual_core) != canonical_json(expected_core):
        raise LegacySceneRepairError(
            "active v8 evaluation semantics differ from the production receipt"
        )


def _require_legacy_migration_invariants(
    connection: sqlite3.Connection,
    *,
    boundary: LegacySceneRepairBoundary,
) -> None:
    invalid_release_rows = int(
        connection.execute(
            """
            SELECT COUNT(*) FROM evaluation_versions
            WHERE id<=? AND release_id IN (?,?) AND NOT (
                (release_id=? AND rule_version='evaluation-v6'
                 AND taxonomy_version='selling-points-v5.0')
                OR
                (release_id=? AND rule_version='evaluation-v7'
                 AND taxonomy_version='selling-points-v5.0')
            )
            """,
            (
                boundary.evaluation_high_water,
                *LEGACY_RELEASE_IDS,
                LEGACY_V6_RELEASE_ID,
                LEGACY_V7_RELEASE_ID,
            ),
        ).fetchone()[0]
    )
    invalid_parent_rows = int(
        connection.execute(
            """
            SELECT COUNT(*) FROM evaluation_versions child
            LEFT JOIN evaluation_versions parent
              ON parent.id=child.parent_evaluation_id
            WHERE child.id<=? AND child.release_id IN (?,?) AND (
                (child.release_id=? AND child.parent_evaluation_id IS NOT NULL)
                OR
                (child.release_id=? AND child.parent_evaluation_id IS NOT NULL AND (
                    parent.id IS NULL OR parent.release_id<>?
                    OR parent.content_id<>child.content_id
                    OR parent.evaluation_source<>child.evaluation_source
                ))
            )
            """,
            (
                boundary.evaluation_high_water,
                *LEGACY_RELEASE_IDS,
                LEGACY_V6_RELEASE_ID,
                LEGACY_V7_RELEASE_ID,
                LEGACY_V6_RELEASE_ID,
            ),
        ).fetchone()[0]
    )
    mismatched_match_scenes = int(
        connection.execute(
            """
            SELECT COUNT(*) FROM evaluation_matches m
            JOIN evaluation_versions e ON e.id=m.evaluation_id
            WHERE e.id<=? AND e.release_id IN (?,?)
              AND m.scene<>e.content_direction
            """,
            (boundary.evaluation_high_water, *LEGACY_RELEASE_IDS),
        ).fetchone()[0]
    )
    if invalid_release_rows or invalid_parent_rows or mismatched_match_scenes:
        raise LegacySceneRepairError("legacy v9 migration invariants are not satisfied")


def _require_approved_plan(
    boundary: LegacySceneRepairBoundary,
    plan: Mapping[str, Any],
) -> None:
    if boundary.manifest.logical_snapshot_sha256 != APPROVED_LOGICAL_SNAPSHOT_SHA256:
        raise LegacySceneRepairError(
            "legacy scene repair is not approved for this logical snapshot"
        )
    mismatches = {
        key: {"expected": expected, "actual": plan.get(key)}
        for key, expected in APPROVED_PLAN_EXPECTATIONS.items()
        if canonical_json(plan.get(key)) != canonical_json(expected)
    }
    if mismatches:
        raise LegacySceneRepairError(
            f"legacy scene repair differs from approved freeze facts: {mismatches}"
        )


def _require_runtime_boundary(
    connection: sqlite3.Connection,
    *,
    boundary: LegacySceneRepairBoundary,
    receipt: Mapping[str, Any],
    require_exact_frozen_coverage: bool,
) -> tuple[str, tuple[int, ...]]:
    try:
        _require_v9(connection)
    except ReleaseManagementError as error:
        raise LegacySceneRepairError(str(error)) from error
    active = connection.execute(
        "SELECT * FROM evaluation_releases WHERE status='active'"
    ).fetchall()
    if len(active) != 1 or (
        str(active[0]["id"]),
        str(active[0]["rule_version"]),
        str(active[0]["taxonomy_version"]),
    ) != (TARGET_RELEASE_ID, "evaluation-v8", TARGET_TAXONOMY_VERSION):
        raise LegacySceneRepairError("target v8 release must be uniquely active")
    legacy_statuses = {
        str(row["id"]): str(row["status"])
        for row in connection.execute(
            """
            SELECT id,status FROM evaluation_releases
            WHERE id IN (?,?) ORDER BY id
            """,
            LEGACY_RELEASE_IDS,
        )
    }
    if legacy_statuses != {
        LEGACY_V6_RELEASE_ID: "retired",
        LEGACY_V7_RELEASE_ID: "retired",
    }:
        raise LegacySceneRepairError("legacy v6 and v7 releases must be retired")
    taxonomy_statuses = {
        str(row["version"]): str(row["status"])
        for row in connection.execute(
            """
            SELECT version,status FROM taxonomy_versions
            WHERE version IN ('selling-points-v5.0','selling-points-v5.1')
            """
        )
    }
    if taxonomy_statuses != {
        "selling-points-v5.0": "retired",
        "selling-points-v5.1": "published",
    }:
        raise LegacySceneRepairError("taxonomy v5.1 must be the published taxonomy")
    core = _receipt_core(receipt, boundary)
    target_semantic_sha256 = _target_semantic_sha256(connection)
    if core.get("target_taxonomy_semantic_sha256") != target_semantic_sha256:
        raise LegacySceneRepairError("target taxonomy changed after release receipt")
    report_event_ids = _require_report_repair_completed(connection, boundary)
    frozen_evaluations = connection.execute(
        "SELECT COUNT(*),MAX(id) FROM evaluation_versions WHERE id<=?",
        (boundary.evaluation_high_water,),
    ).fetchone()
    if (
        int(frozen_evaluations[0]) != boundary.frozen_evaluation_count
        or int(frozen_evaluations[1]) != boundary.evaluation_high_water
    ):
        raise LegacySceneRepairError("frozen evaluation high-water changed")
    frozen_matches = int(
        connection.execute(
            """
            SELECT COUNT(*) FROM evaluation_matches m
            JOIN evaluation_versions e ON e.id=m.evaluation_id
            WHERE e.id<=?
            """,
            (boundary.evaluation_high_water,),
        ).fetchone()[0]
    )
    if frozen_matches != boundary.frozen_match_count:
        raise LegacySceneRepairError("frozen evaluation match count changed")
    target_ids = tuple(
        int(row[0])
        for row in connection.execute(
            """
            SELECT content_id FROM evaluation_versions
            WHERE release_id=? AND evaluation_source='automatic'
              AND invalidated_at IS NULL
            ORDER BY content_id
            """,
            (TARGET_RELEASE_ID,),
        )
    )
    if require_exact_frozen_coverage:
        if target_ids != boundary.content_ids:
            raise LegacySceneRepairError(
                "active v8 frozen-content coverage is not exact"
            )
    elif not set(boundary.content_ids).issubset(target_ids):
        raise LegacySceneRepairError(
            "active v8 no longer covers every frozen content item"
        )
    illegal_target_matches = int(
        connection.execute(
            """
            SELECT COUNT(*) FROM evaluation_matches m
            JOIN evaluation_versions e ON e.id=m.evaluation_id
            WHERE e.release_id=? AND e.invalidated_at IS NULL
              AND NOT EXISTS (
                SELECT 1 FROM taxonomy_versions tv
                JOIN selling_points sp ON sp.taxonomy_id=tv.id
                JOIN selling_point_scenes sps ON sps.selling_point_id=sp.id
                WHERE tv.version=? AND sp.enabled=1
                  AND sp.code=m.selling_point_code AND sps.scene=m.scene
              )
            """,
            (TARGET_RELEASE_ID, TARGET_TAXONOMY_VERSION),
        ).fetchone()[0]
    )
    if illegal_target_matches:
        raise LegacySceneRepairError("active v8 contains illegal point/scene matches")
    _require_legacy_migration_invariants(connection, boundary=boundary)
    return target_semantic_sha256, report_event_ids


def _allowed_scenes(connection: sqlite3.Connection) -> dict[str, set[str]]:
    allowed: dict[str, set[str]] = {}
    for row in connection.execute(
        """
        SELECT sp.code,sps.scene FROM taxonomy_versions tv
        JOIN selling_points sp ON sp.taxonomy_id=tv.id
        JOIN selling_point_scenes sps ON sps.selling_point_id=sp.id
        WHERE tv.version=? AND tv.status='published' AND sp.enabled=1
        ORDER BY sp.code,sps.scene
        """,
        (TARGET_TAXONOMY_VERSION,),
    ):
        allowed.setdefault(str(row["code"]), set()).add(str(row["scene"]))
    if not allowed or any(not scenes for scenes in allowed.values()):
        raise LegacySceneRepairError("published target taxonomy has no scene rules")
    return allowed


def _illegal_legacy_matches(
    connection: sqlite3.Connection,
    *,
    boundary: LegacySceneRepairBoundary,
) -> list[dict[str, Any]]:
    allowed = _allowed_scenes(connection)
    rows: list[dict[str, Any]] = []
    for row in connection.execute(
        """
        SELECT e.id evaluation_id,e.content_id,e.release_id,e.evaluation_source,
               e.parent_evaluation_id,e.invalidated_at,e.invalidation_reason,
               m.selling_point_code,m.scene,m.match_role
        FROM evaluation_versions e
        JOIN evaluation_matches m ON m.evaluation_id=e.id
        WHERE e.id<=? AND e.release_id IN (?,?)
        ORDER BY e.id,m.selling_point_code
        """,
        (boundary.evaluation_high_water, *LEGACY_RELEASE_IDS),
    ):
        value = dict(row)
        if str(value["scene"]) not in allowed.get(
            str(value["selling_point_code"]), set()
        ):
            rows.append(value)
    return rows


def _automatic_chain_closure(
    connection: sqlite3.Connection,
    *,
    boundary: LegacySceneRepairBoundary,
    seed_ids: set[int],
) -> tuple[int, ...]:
    rows = connection.execute(
        """
        SELECT id,parent_evaluation_id,invalidated_at,invalidation_reason
        FROM evaluation_versions
        WHERE id<=? AND release_id IN (?,?) AND evaluation_source='automatic'
        ORDER BY id
        """,
        (boundary.evaluation_high_water, *LEGACY_RELEASE_IDS),
    ).fetchall()
    by_id = {int(row["id"]): row for row in rows}
    neighbors: dict[int, set[int]] = {evaluation_id: set() for evaluation_id in by_id}
    for row in rows:
        evaluation_id = int(row["id"])
        parent = row["parent_evaluation_id"]
        if parent is None or int(parent) not in by_id:
            continue
        parent_id = int(parent)
        neighbors[evaluation_id].add(parent_id)
        neighbors[parent_id].add(evaluation_id)
    missing = seed_ids - by_id.keys()
    if missing:
        raise LegacySceneRepairError(
            f"automatic illegal seeds are outside legacy graph: {sorted(missing)}"
        )
    closure = set(seed_ids)
    pending = deque(sorted(seed_ids))
    while pending:
        current = pending.popleft()
        for neighbor in sorted(neighbors[current]):
            if neighbor not in closure:
                closure.add(neighbor)
                pending.append(neighbor)
    for evaluation_id in sorted(closure):
        row = by_id[evaluation_id]
        if row["invalidated_at"] is not None or row["invalidation_reason"] is not None:
            raise LegacySceneRepairError(
                f"automatic repair chain already has another invalidation: {evaluation_id}"
            )
    return tuple(sorted(closure))


def _candidate_rows_hash(
    connection: sqlite3.Connection,
    evaluation_ids: Sequence[int],
) -> str:
    if not evaluation_ids:
        return _sha256_json([])
    placeholders = ",".join("?" for _ in evaluation_ids)
    return _row_projection_hash(
        connection,
        f"SELECT * FROM evaluation_versions WHERE id IN ({placeholders}) ORDER BY id",
        tuple(evaluation_ids),
    )


def _queue_anchors(
    connection: sqlite3.Connection, content_ids: Sequence[int]
) -> tuple[tuple[int, int], ...]:
    anchors: list[tuple[int, int]] = []
    for content_id in content_ids:
        active = connection.execute(
            """
            SELECT * FROM evaluation_versions
            WHERE content_id=? AND release_id=? AND invalidated_at IS NULL
            ORDER BY evaluated_at DESC,id DESC LIMIT 1
            """,
            (content_id, TARGET_RELEASE_ID),
        ).fetchone()
        review_anchor = review_anchor_evaluation(connection, content_id)
        if (
            active is None
            or review_anchor is None
            or int(active["id"]) != int(review_anchor["id"])
        ):
            raise LegacySceneRepairError(
                f"review anchor is not the active v8 evaluation: {content_id}"
            )
        anchors.append((content_id, int(active["id"])))
    return tuple(anchors)


def _build_pristine_plan(
    connection: sqlite3.Connection,
    *,
    boundary: LegacySceneRepairBoundary,
    receipt: Mapping[str, Any],
    operator_reason: str,
    target_semantic_sha256: str,
    activation_attestation: Mapping[str, Any],
) -> dict[str, Any]:
    if connection.execute(
        "SELECT 1 FROM review_queue WHERE reason_code=? LIMIT 1",
        (QUEUE_REASON_CODE,),
    ).fetchone():
        raise LegacySceneRepairError(
            "legacy scene conflict queue exists without a successful repair audit"
        )
    if (
        int(connection.execute("SELECT COUNT(*) FROM review_queue").fetchone()[0])
        != boundary.frozen_review_queue_count
    ):
        raise LegacySceneRepairError("review queue changed before legacy scene repair")
    illegal = _illegal_legacy_matches(connection, boundary=boundary)
    automatic_seed_ids = {
        int(row["evaluation_id"])
        for row in illegal
        if row["evaluation_source"] == "automatic" and row["invalidated_at"] is None
    }
    automatic_ids = _automatic_chain_closure(
        connection, boundary=boundary, seed_ids=automatic_seed_ids
    )
    automatic_content_ids = tuple(
        int(row[0])
        for row in connection.execute(
            f"""
            SELECT DISTINCT content_id FROM evaluation_versions
            WHERE id IN ({",".join("?" for _ in automatic_ids)}) ORDER BY content_id
            """,
            automatic_ids,
        )
    )
    nonautomatic_ids = tuple(
        sorted(
            {
                int(row["evaluation_id"])
                for row in illegal
                if row["evaluation_source"] != "automatic"
                and row["invalidated_at"] is None
            }
        )
    )
    nonautomatic_content_ids = tuple(
        sorted(
            {
                int(row["content_id"])
                for row in illegal
                if row["evaluation_source"] != "automatic"
                and row["invalidated_at"] is None
            }
        )
    )
    if not automatic_ids or not nonautomatic_ids:
        raise LegacySceneRepairError(
            "legacy illegal scene repair set is unexpectedly empty"
        )
    anchors = _queue_anchors(connection, nonautomatic_content_ids)
    release_counts = Counter(
        str(row[0])
        for row in connection.execute(
            f"""
            SELECT release_id FROM evaluation_versions
            WHERE id IN ({",".join("?" for _ in automatic_ids)}) ORDER BY id
            """,
            automatic_ids,
        )
    )
    illegal_projection = [
        {
            "evaluation_id": int(row["evaluation_id"]),
            "content_id": int(row["content_id"]),
            "release_id": str(row["release_id"]),
            "evaluation_source": str(row["evaluation_source"]),
            "selling_point_code": str(row["selling_point_code"]),
            "scene": str(row["scene"]),
            "match_role": str(row["match_role"]),
        }
        for row in illegal
    ]
    receipt_core_sha256 = _require_sha256(
        receipt.get("core_sha256"), label="production receipt core hash"
    )
    plan: dict[str, Any] = {
        "contract": REPAIR_CONTRACT,
        "freeze_manifest_sha256": boundary.manifest.sha256,
        "logical_snapshot_sha256": boundary.manifest.logical_snapshot_sha256,
        "database_backup_sha256": boundary.manifest.database_backup_sha256,
        "receipt_core_sha256": receipt_core_sha256,
        "target_taxonomy_semantic_sha256": target_semantic_sha256,
        "activation_stable_state_sha256": activation_attestation["state_sha256"],
        "attested_legacy_evaluation_count": activation_attestation[
            "legacy_evaluation_count"
        ],
        "attested_legacy_evaluation_rows_sha256": activation_attestation[
            "legacy_evaluation_rows_sha256"
        ],
        "attested_legacy_match_count": activation_attestation["legacy_match_count"],
        "attested_legacy_match_rows_sha256": activation_attestation[
            "legacy_match_rows_sha256"
        ],
        "evaluation_high_water": boundary.evaluation_high_water,
        "operator_reason": operator_reason,
        "automatic_evaluation_ids": list(automatic_ids),
        "automatic_evaluation_ids_sha256": _sha256_json(list(automatic_ids)),
        "automatic_content_ids": list(automatic_content_ids),
        "automatic_content_ids_sha256": _sha256_json(list(automatic_content_ids)),
        "automatic_evaluation_count": len(automatic_ids),
        "automatic_content_count": len(automatic_content_ids),
        "automatic_by_release": dict(sorted(release_counts.items())),
        "nonautomatic_evaluation_ids": list(nonautomatic_ids),
        "nonautomatic_evaluation_ids_sha256": _sha256_json(list(nonautomatic_ids)),
        "nonautomatic_content_ids": list(nonautomatic_content_ids),
        "nonautomatic_content_ids_sha256": _sha256_json(list(nonautomatic_content_ids)),
        "nonautomatic_evaluation_count": len(nonautomatic_ids),
        "nonautomatic_content_count": len(nonautomatic_content_ids),
        "queue_anchors": [
            {"content_id": content_id, "evaluation_id": evaluation_id}
            for content_id, evaluation_id in anchors
        ],
        "illegal_match_count": len(illegal_projection),
        "illegal_match_projection_sha256": _sha256_json(illegal_projection),
        "legacy_match_rows_sha256": _row_projection_hash(
            connection,
            """
            SELECT m.* FROM evaluation_matches m
            JOIN evaluation_versions e ON e.id=m.evaluation_id
            WHERE e.id<=? ORDER BY m.evaluation_id,m.selling_point_code
            """,
            (boundary.evaluation_high_water,),
        ),
        "nonautomatic_rows_sha256": _candidate_rows_hash(connection, nonautomatic_ids),
        "review_queue_rows_sha256": _table_projection_hash(connection, "review_queue"),
        "direction_cache_sha256": _row_projection_hash(
            connection,
            f"""
            SELECT id,evaluation_content_direction FROM content_items
            WHERE id IN ({",".join("?" for _ in boundary.content_ids)}) ORDER BY id
            """,
            boundary.content_ids,
        ),
        "protected_table_sha256": {
            table: _table_projection_hash(connection, table)
            for table in PROTECTED_TABLES
        },
    }
    plan["plan_sha256"] = _sha256_json(plan)
    _require_approved_plan(boundary, plan)
    return plan


def _invalidation_reason(operator_reason: str) -> str:
    return f"{INVALIDATION_REASON_PREFIX}: {operator_reason}"


def _verify_repair_owned_state(
    connection: sqlite3.Connection,
    *,
    boundary: LegacySceneRepairBoundary,
    plan: Mapping[str, Any],
    captured_at: str,
    allow_queue_lifecycle_progress: bool = False,
) -> None:
    automatic_ids = tuple(int(value) for value in plan["automatic_evaluation_ids"])
    nonautomatic_ids = tuple(
        int(value) for value in plan["nonautomatic_evaluation_ids"]
    )
    reason = _invalidation_reason(str(plan["operator_reason"]))
    placeholders = ",".join("?" for _ in automatic_ids)
    repaired = connection.execute(
        f"""
        SELECT id,invalidated_at,invalidation_reason FROM evaluation_versions
        WHERE id IN ({placeholders}) ORDER BY id
        """,
        automatic_ids,
    ).fetchall()
    if len(repaired) != len(automatic_ids) or any(
        str(row["invalidated_at"]) != captured_at
        or str(row["invalidation_reason"]) != reason
        for row in repaired
    ):
        raise LegacySceneRepairError("automatic legacy repair rows are incomplete")
    nonautomatic = connection.execute(
        f"""
        SELECT id,invalidated_at,invalidation_reason FROM evaluation_versions
        WHERE id IN ({",".join("?" for _ in nonautomatic_ids)}) ORDER BY id
        """,
        nonautomatic_ids,
    ).fetchall()
    if len(nonautomatic) != len(nonautomatic_ids) or any(
        row["invalidated_at"] is not None or row["invalidation_reason"] is not None
        for row in nonautomatic
    ):
        raise LegacySceneRepairError("non-automatic legacy conflicts were modified")
    valid_illegal_automatic = sum(
        row["evaluation_source"] == "automatic" and row["invalidated_at"] is None
        for row in _illegal_legacy_matches(connection, boundary=boundary)
    )
    if valid_illegal_automatic:
        raise LegacySceneRepairError("valid legacy automatic illegal matches remain")
    expected_anchors = {
        int(item["content_id"]): int(item["evaluation_id"])
        for item in plan["queue_anchors"]
    }
    queue_rows = connection.execute(
        """
        SELECT * FROM review_queue WHERE reason_code=? ORDER BY content_id
        """,
        (QUEUE_REASON_CODE,),
    ).fetchall()
    queue_invalid = len(queue_rows) != len(expected_anchors)
    for row in queue_rows:
        content_id = int(row["content_id"])
        evaluation = connection.execute(
            """
            SELECT content_id,release_id,invalidated_at
            FROM evaluation_versions WHERE id=?
            """,
            (row["evaluation_id"],),
        ).fetchone()
        queue_invalid = queue_invalid or (
            content_id not in expected_anchors
            or int(row["priority"]) != 100
            or str(row["created_at"]) != captured_at
            or evaluation is None
            or int(evaluation["content_id"]) != content_id
            or str(evaluation["release_id"]) != TARGET_RELEASE_ID
            or evaluation["invalidated_at"] is not None
        )
        if not allow_queue_lifecycle_progress:
            queue_invalid = queue_invalid or (
                int(row["evaluation_id"] or 0) != expected_anchors[content_id]
                or str(row["status"]) != "manual_required"
                or str(row["updated_at"]) != captured_at
            )
    if queue_invalid:
        raise LegacySceneRepairError(
            "legacy non-automatic review queues are incomplete"
        )
    if not allow_queue_lifecycle_progress and int(
        connection.execute("SELECT COUNT(*) FROM review_queue").fetchone()[0]
    ) != boundary.frozen_review_queue_count + len(expected_anchors):
        raise LegacySceneRepairError("legacy repair changed unrelated review queues")


def _normalized_legacy_evaluation_anchor(
    connection: sqlite3.Connection,
    automatic_ids: Sequence[int],
) -> tuple[int, str]:
    columns = [
        str(row[1])
        for row in connection.execute("PRAGMA table_info(evaluation_versions)")
    ]
    predicate = f"id IN ({','.join('?' for _ in automatic_ids)})"
    expressions = [
        (
            f'CASE WHEN {predicate} THEN NULL ELSE "{column}" END AS "{column}"'
            if column in {"invalidated_at", "invalidation_reason"}
            else f'"{column}"'
        )
        for column in columns
    ]
    order_by = ",".join(f'"{column}"' for column in columns)
    return _query_rows_sha256(
        connection,
        f"SELECT {','.join(expressions)} FROM evaluation_versions "
        f"WHERE release_id<>? ORDER BY {order_by}",
        tuple(automatic_ids) * 2 + (TARGET_RELEASE_ID,),
    )


def _verify_permanent_history(
    connection: sqlite3.Connection,
    *,
    boundary: LegacySceneRepairBoundary,
    plan: Mapping[str, Any],
) -> None:
    automatic_ids = tuple(int(value) for value in plan["automatic_evaluation_ids"])
    evaluation_count, evaluation_sha256 = _normalized_legacy_evaluation_anchor(
        connection, automatic_ids
    )
    if evaluation_count != int(
        plan["attested_legacy_evaluation_count"]
    ) or evaluation_sha256 != str(plan["attested_legacy_evaluation_rows_sha256"]):
        raise LegacySceneRepairError(
            "legacy evaluation history changed outside the approved invalidation"
        )
    match_columns = tuple(
        str(row[1])
        for row in connection.execute("PRAGMA table_info(evaluation_matches)")
    )
    match_count, match_sha256 = _rows_sha256(
        connection,
        table="evaluation_matches",
        columns=match_columns,
        where=(
            "WHERE evaluation_id NOT IN "
            "(SELECT id FROM evaluation_versions WHERE release_id=?)"
        ),
        parameters=(TARGET_RELEASE_ID,),
    )
    if match_count != int(plan["attested_legacy_match_count"]) or match_sha256 != str(
        plan["attested_legacy_match_rows_sha256"]
    ):
        raise LegacySceneRepairError("legacy evaluation matches changed")
    if (
        int(
            connection.execute(
                "SELECT COUNT(*) FROM evaluation_versions WHERE id<=?",
                (boundary.evaluation_high_water,),
            ).fetchone()[0]
        )
        != boundary.frozen_evaluation_count
    ):
        raise LegacySceneRepairError("legacy evaluation history row count changed")


def _verify_immutable_postconditions(
    connection: sqlite3.Connection,
    *,
    boundary: LegacySceneRepairBoundary,
    plan: Mapping[str, Any],
) -> None:
    _verify_permanent_history(connection, boundary=boundary, plan=plan)
    if (
        int(
            connection.execute(
                "SELECT COUNT(*) FROM evaluation_versions WHERE id<=?",
                (boundary.evaluation_high_water,),
            ).fetchone()[0]
        )
        != boundary.frozen_evaluation_count
    ):
        raise LegacySceneRepairError("legacy evaluation history row count changed")
    if (
        int(
            connection.execute(
                """
                SELECT COUNT(*) FROM evaluation_matches m
                JOIN evaluation_versions e ON e.id=m.evaluation_id
                WHERE e.id<=?
                """,
                (boundary.evaluation_high_water,),
            ).fetchone()[0]
        )
        != boundary.frozen_match_count
    ):
        raise LegacySceneRepairError("legacy evaluation match row count changed")
    current_match_hash = _row_projection_hash(
        connection,
        """
        SELECT m.* FROM evaluation_matches m
        JOIN evaluation_versions e ON e.id=m.evaluation_id
        WHERE e.id<=? ORDER BY m.evaluation_id,m.selling_point_code
        """,
        (boundary.evaluation_high_water,),
    )
    if current_match_hash != plan["legacy_match_rows_sha256"]:
        raise LegacySceneRepairError("legacy evaluation matches were modified")
    nonautomatic_ids = tuple(
        int(value) for value in plan["nonautomatic_evaluation_ids"]
    )
    if (
        _candidate_rows_hash(connection, nonautomatic_ids)
        != plan["nonautomatic_rows_sha256"]
    ):
        raise LegacySceneRepairError("non-automatic legacy rows were modified")
    existing_queue_hash = _table_projection_hash(
        connection,
        "review_queue",
        where="WHERE reason_code<>?",
        parameters=(QUEUE_REASON_CODE,),
    )
    if existing_queue_hash != plan["review_queue_rows_sha256"]:
        raise LegacySceneRepairError("pre-existing review queues were modified")
    direction_hash = _row_projection_hash(
        connection,
        f"""
        SELECT id,evaluation_content_direction FROM content_items
        WHERE id IN ({",".join("?" for _ in boundary.content_ids)}) ORDER BY id
        """,
        boundary.content_ids,
    )
    if direction_hash != plan["direction_cache_sha256"]:
        raise LegacySceneRepairError("content direction cache was modified")
    for table, expected in plan["protected_table_sha256"].items():
        if _table_projection_hash(connection, str(table)) != expected:
            raise LegacySceneRepairError(f"repair modified protected table: {table}")


def _load_successful_audit(
    connection: sqlite3.Connection, boundary: LegacySceneRepairBoundary
) -> sqlite3.Row | None:
    return connection.execute(
        "SELECT * FROM migration_audit WHERE id=?", (boundary.audit_id,)
    ).fetchone()


def _reuse_successful_audit(
    connection: sqlite3.Connection,
    *,
    boundary: LegacySceneRepairBoundary,
    audit: sqlite3.Row,
    receipt: Mapping[str, Any],
    target_semantic_sha256: str,
    operator_reason: str,
    expected_plan_sha256: str | None,
) -> dict[str, Any]:
    if str(audit["status"]) != "succeeded":
        raise LegacySceneRepairError("legacy scene repair audit is not succeeded")
    try:
        summary = json.loads(str(audit["summary_json"]))
    except json.JSONDecodeError as error:
        raise LegacySceneRepairError(
            "legacy scene repair audit JSON is invalid"
        ) from error
    if not isinstance(summary, dict) or summary.get("contract") != REPAIR_CONTRACT:
        raise LegacySceneRepairError("legacy scene repair audit contract is invalid")
    plan = summary.get("plan")
    if not isinstance(plan, dict):
        raise LegacySceneRepairError("legacy scene repair audit has no plan")
    stored_plan_sha256 = _require_sha256(
        summary.get("plan_sha256"), label="stored repair plan hash"
    )
    material = dict(plan)
    embedded = material.pop("plan_sha256", None)
    if embedded != stored_plan_sha256 or _sha256_json(material) != stored_plan_sha256:
        raise LegacySceneRepairError("legacy scene repair audit plan hash changed")
    if str(plan.get("operator_reason") or "") != operator_reason:
        raise LegacySceneRepairError("operator reason differs from successful repair")
    if expected_plan_sha256 is not None and expected_plan_sha256 != stored_plan_sha256:
        raise LegacySceneRepairError("expected repair plan hash differs from audit")
    _require_approved_plan(boundary, plan)
    captured_at = str(summary.get("captured_at") or "")
    if not captured_at:
        raise LegacySceneRepairError("legacy scene repair audit has no timestamp")
    expected_boundary = {
        "freeze_manifest_sha256": boundary.manifest.sha256,
        "logical_snapshot_sha256": boundary.manifest.logical_snapshot_sha256,
        "database_backup_sha256": boundary.manifest.database_backup_sha256,
        "receipt_core_sha256": _require_sha256(
            receipt.get("core_sha256"), label="production receipt core hash"
        ),
        "target_taxonomy_semantic_sha256": target_semantic_sha256,
    }
    if any(plan.get(key) != value for key, value in expected_boundary.items()):
        raise LegacySceneRepairError("successful repair audit boundary changed")
    stable_state = _activation_stable_state(connection, receipt=receipt)
    stable_tables = stable_state.get("tables")
    if not isinstance(stable_tables, Mapping):
        raise LegacySceneRepairError("receipt activation stable tables are missing")
    evaluation_anchor = stable_tables.get("evaluation_versions")
    match_anchor = stable_tables.get("evaluation_matches")
    if not isinstance(evaluation_anchor, Mapping) or not isinstance(
        match_anchor, Mapping
    ):
        raise LegacySceneRepairError("receipt legacy table anchors are missing")
    attested_plan = {
        "activation_stable_state_sha256": stable_state.get("state_sha256"),
        "attested_legacy_evaluation_count": evaluation_anchor.get("count"),
        "attested_legacy_evaluation_rows_sha256": evaluation_anchor.get("rows_sha256"),
        "attested_legacy_match_count": match_anchor.get("count"),
        "attested_legacy_match_rows_sha256": match_anchor.get("rows_sha256"),
    }
    if any(plan.get(key) != value for key, value in attested_plan.items()):
        raise LegacySceneRepairError(
            "successful repair audit does not match receipt table anchors"
        )
    if (
        str(audit["baseline_id"])
        != f"dcar-v9-freeze:{boundary.manifest.logical_snapshot_sha256}"
        or Path(str(audit["source_database"])).resolve()
        != boundary.manifest.database_backup.resolve()
        or str(audit["source_sha256"]) != boundary.manifest.database_backup_sha256
        or str(audit["started_at"]) != captured_at
        or str(audit["completed_at"]) != captured_at
        or summary.get("invalidation_reason") != _invalidation_reason(operator_reason)
        or int(summary.get("invalidated_count") or -1)
        != int(plan["automatic_evaluation_count"])
        or int(summary.get("queues_inserted") or -1)
        != int(plan["nonautomatic_content_count"])
    ):
        raise LegacySceneRepairError("successful repair audit provenance changed")
    _verify_repair_owned_state(
        connection,
        boundary=boundary,
        plan=plan,
        captured_at=captured_at,
        allow_queue_lifecycle_progress=True,
    )
    _verify_permanent_history(connection, boundary=boundary, plan=plan)
    return {
        "mode": "applied",
        "reused": True,
        "audit_id": boundary.audit_id,
        "plan": plan,
        "plan_sha256": stored_plan_sha256,
        "invalidated_count": 0,
        "queues_inserted": 0,
        "rollback_window_closed": True,
    }


def _run(
    connection: sqlite3.Connection,
    *,
    boundary: LegacySceneRepairBoundary,
    receipt: Mapping[str, Any],
    operator_reason: str,
    apply: bool,
    expected_plan_sha256: str | None,
) -> dict[str, Any]:
    audit = _load_successful_audit(connection, boundary)
    target_semantic_sha256, report_event_ids = _require_runtime_boundary(
        connection,
        boundary=boundary,
        receipt=receipt,
        require_exact_frozen_coverage=audit is None,
    )
    if audit is not None:
        return _reuse_successful_audit(
            connection,
            boundary=boundary,
            audit=audit,
            receipt=receipt,
            target_semantic_sha256=target_semantic_sha256,
            operator_reason=operator_reason,
            expected_plan_sha256=expected_plan_sha256,
        )
    _require_target_receipt_semantics(
        connection,
        boundary=boundary,
        receipt=receipt,
        report_event_ids=report_event_ids,
    )
    activation_attestation = _require_pristine_attested_state(
        connection,
        boundary=boundary,
        receipt=receipt,
        report_event_ids=report_event_ids,
    )
    plan = _build_pristine_plan(
        connection,
        boundary=boundary,
        receipt=receipt,
        operator_reason=operator_reason,
        target_semantic_sha256=target_semantic_sha256,
        activation_attestation=activation_attestation,
    )
    plan_sha256 = str(plan["plan_sha256"])
    if not apply:
        return {
            "mode": "dry-run",
            "reused": False,
            "audit_id": boundary.audit_id,
            "plan": plan,
            "plan_sha256": plan_sha256,
            "invalidated_count": 0,
            "queues_inserted": 0,
            "rollback_window_closed": False,
        }
    if expected_plan_sha256 != plan_sha256:
        raise LegacySceneRepairError("expected repair plan hash does not match dry-run")
    captured_at = now_utc()
    invalidation_reason = _invalidation_reason(operator_reason)
    automatic_ids = tuple(int(value) for value in plan["automatic_evaluation_ids"])
    updated = 0
    for index, evaluation_id in enumerate(automatic_ids, 1):
        cursor = connection.execute(
            """
            UPDATE evaluation_versions
            SET invalidated_at=?,invalidation_reason=?
            WHERE id=? AND evaluation_source='automatic'
              AND invalidated_at IS NULL AND invalidation_reason IS NULL
            """,
            (captured_at, invalidation_reason, evaluation_id),
        )
        if cursor.rowcount != 1:
            raise LegacySceneRepairError(
                f"automatic legacy invalidation CAS failed: {evaluation_id}"
            )
        updated += 1
        if index == 1:
            _repair_checkpoint("first-automatic-invalidated")
    queue_count = 0
    for anchor in plan["queue_anchors"]:
        connection.execute(
            """
            INSERT INTO review_queue(
                content_id,evaluation_id,reason_code,priority,status,created_at,updated_at
            ) VALUES (?,?,?,100,'manual_required',?,?)
            """,
            (
                int(anchor["content_id"]),
                int(anchor["evaluation_id"]),
                QUEUE_REASON_CODE,
                captured_at,
                captured_at,
            ),
        )
        queue_count += 1
    _repair_checkpoint("review-queues-inserted")
    _verify_repair_owned_state(
        connection,
        boundary=boundary,
        plan=plan,
        captured_at=captured_at,
    )
    _verify_immutable_postconditions(connection, boundary=boundary, plan=plan)
    summary = {
        "contract": REPAIR_CONTRACT,
        "captured_at": captured_at,
        "invalidation_reason": invalidation_reason,
        "plan_sha256": plan_sha256,
        "plan": plan,
        "invalidated_count": updated,
        "queues_inserted": queue_count,
    }
    connection.execute(
        """
        INSERT INTO migration_audit(
            id,baseline_id,source_database,source_sha256,status,summary_json,
            started_at,completed_at
        ) VALUES (?,?,?,?, 'succeeded',?,?,?)
        """,
        (
            boundary.audit_id,
            f"dcar-v9-freeze:{boundary.manifest.logical_snapshot_sha256}",
            str(boundary.manifest.database_backup),
            boundary.manifest.database_backup_sha256,
            canonical_json(summary),
            captured_at,
            captured_at,
        ),
    )
    _repair_checkpoint("audit-inserted")
    if connection.execute("PRAGMA foreign_key_check").fetchall():
        raise LegacySceneRepairError("legacy scene repair created foreign-key errors")
    if str(connection.execute("PRAGMA integrity_check").fetchone()[0]) != "ok":
        raise LegacySceneRepairError("legacy scene repair failed integrity check")
    return {
        "mode": "apply",
        "reused": False,
        "audit_id": boundary.audit_id,
        "plan": plan,
        "plan_sha256": plan_sha256,
        "invalidated_count": updated,
        "queues_inserted": queue_count,
        "rollback_window_closed": True,
    }


def repair_legacy_illegal_scene_chains(
    *,
    db_path: Path,
    manifest_path: Path,
    receipt_path: Path,
    operator_reason: str,
    apply: bool = False,
    expected_plan_sha256: str | None = None,
    acknowledge_rollback_window_close: bool = False,
) -> dict[str, Any]:
    """Dry-run or atomically apply the frozen legacy illegal-scene plan."""

    db_path = db_path.resolve()
    manifest_path = manifest_path.resolve()
    receipt_path = receipt_path.resolve()
    operator_reason = _require_reason(operator_reason)
    if expected_plan_sha256 is not None:
        expected_plan_sha256 = _require_sha256(
            expected_plan_sha256, label="expected repair plan hash"
        )
    if apply and not acknowledge_rollback_window_close:
        raise LegacySceneRepairError(
            "applying legacy scene repair closes the release rollback window; "
            "explicit acknowledgement is required"
        )
    if acknowledge_rollback_window_close and not apply:
        raise LegacySceneRepairError(
            "rollback-window acknowledgement is only valid with apply"
        )
    if apply and expected_plan_sha256 is None:
        raise LegacySceneRepairError("apply requires the dry-run plan SHA-256")
    boundary = _load_boundary(manifest_path)
    try:
        receipt = _read_receipt(receipt_path)
        _require_production_receipt_chain(receipt, expected_database=db_path)
    except ReleaseManagementError as error:
        raise LegacySceneRepairError(str(error)) from error
    try:
        with _existing_connection(db_path, read_only=not apply) as connection:
            before_changes = connection.total_changes
            if apply:
                with transaction(connection):
                    return _run(
                        connection,
                        boundary=boundary,
                        receipt=receipt,
                        operator_reason=operator_reason,
                        apply=True,
                        expected_plan_sha256=expected_plan_sha256,
                    )
            result = _run(
                connection,
                boundary=boundary,
                receipt=receipt,
                operator_reason=operator_reason,
                apply=False,
                expected_plan_sha256=expected_plan_sha256,
            )
            if connection.total_changes != before_changes:
                raise LegacySceneRepairError("dry-run attempted to write the database")
            return result
    except ReleaseManagementError as error:
        raise LegacySceneRepairError(str(error)) from error


def _repair_checkpoint(_name: str) -> None:
    """Patchable failure-injection point for atomicity tests."""
