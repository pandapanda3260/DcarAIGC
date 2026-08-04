"""Fail-closed repair for unsafe automatic report revisions in a freeze manifest."""

from __future__ import annotations

import hashlib
import json
import re
import sqlite3
from collections.abc import Iterator, Mapping
from contextlib import contextmanager
from dataclasses import dataclass
from pathlib import Path
from typing import Any

from .release_management import (
    ReleaseManagementError,
    _attested_activation_stable_state,
    _load_freeze_manifest,
    _read_receipt,
    _require_production_receipt_chain,
)
from .storage import LEGACY_V7_RELEASE_ID, SCHEMA_VERSION, now_utc, transaction


FREEZE_SCHEMA_VERSION = "dcar-v9-freeze-manifest-v1"
UNSAFE_CONTRACT_VERSION = "dcar-content-operations-report-v8.3"
UNSAFE_RULE_VERSION = "evaluation-v7"
UNSAFE_TAXONOMY_VERSION = "selling-points-v5.0"
INVALIDATION_REASON = "pre_freeze_startup_catchup_legacy_matcher_without_report_gate"
INVALIDATION_EVENT_TYPE = "report_revision_invalidated"
TARGET_RELEASE_ID = "evaluation-v8__selling-points-v5.1"
HEX_SHA256 = re.compile(r"[0-9a-f]{64}", re.ASCII)


class ReportRepairError(RuntimeError):
    """Raised before committing when the repair boundary cannot be proven."""


@dataclass(frozen=True)
class UnsafeReportTarget:
    task_id: str
    revision: int
    contract_version: str
    rule_version: str
    taxonomy_version: str
    report_json_path: str
    report_sha256: str
    created_at: str
    creation_source: str
    task_status: str
    task_started_at: str
    task_completed_at: str
    scheduler_run_ids: tuple[int, ...]

    @property
    def key(self) -> tuple[str, int]:
        return self.task_id, self.revision


REPORT_FILE_COLUMNS = (
    "id",
    "task_id",
    "revision",
    "file_kind",
    "local_path",
    "sha256",
    "byte_size",
    "status",
    "error_message",
    "created_at",
)


@dataclass(frozen=True)
class ReportRepairBoundary:
    manifest_sha256: str
    targets: tuple[UnsafeReportTarget, ...]
    frozen_report_files: Mapping[tuple[str, int], tuple[tuple[Any, ...], ...]]


def _canonical_json(value: Mapping[str, Any]) -> str:
    return json.dumps(
        value,
        ensure_ascii=False,
        sort_keys=True,
        separators=(",", ":"),
    )


def _sha256_file(path: Path) -> str:
    digest = hashlib.sha256()
    try:
        with path.open("rb") as handle:
            for chunk in iter(lambda: handle.read(1024 * 1024), b""):
                digest.update(chunk)
    except OSError as error:
        raise ReportRepairError(f"cannot hash file {path}: {error}") from error
    return digest.hexdigest()


def _read_json_object(path: Path, *, label: str) -> dict[str, Any]:
    try:
        value = json.loads(path.read_text(encoding="utf-8"))
    except (OSError, UnicodeDecodeError, json.JSONDecodeError) as error:
        raise ReportRepairError(f"cannot read {label}: {error}") from error
    if not isinstance(value, dict):
        raise ReportRepairError(f"{label} must be a JSON object")
    return value


def _require_text(value: Any, *, label: str) -> str:
    normalized = str(value or "")
    if not normalized:
        raise ReportRepairError(f"{label} is required")
    return normalized


def _require_sha256(value: Any, *, label: str) -> str:
    normalized = str(value or "")
    if HEX_SHA256.fullmatch(normalized) is None:
        raise ReportRepairError(f"{label} must be a lowercase SHA-256")
    return normalized


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
        connection.execute("PRAGMA foreign_keys=ON")
        connection.execute("PRAGMA busy_timeout=30000")
        if read_only:
            connection.execute("PRAGMA query_only=ON")
        yield connection
    except sqlite3.Error as error:
        access = "read-only" if read_only else "read-write"
        raise ReportRepairError(
            f"cannot access existing database {access}: {error}"
        ) from error
    finally:
        if connection is not None:
            connection.close()


def _parse_target(value: Any, *, index: int) -> UnsafeReportTarget:
    if not isinstance(value, Mapping):
        raise ReportRepairError(f"unsafe report target {index} must be an object")
    try:
        revision = int(value.get("revision") or 0)
        scheduler_run_ids = tuple(int(item) for item in value["scheduler_run_ids"])
    except (KeyError, TypeError, ValueError) as error:
        raise ReportRepairError(
            f"unsafe report target {index} has invalid numeric fields"
        ) from error
    if revision < 1 or any(item < 1 for item in scheduler_run_ids):
        raise ReportRepairError(
            f"unsafe report target {index} has invalid numeric fields"
        )
    if tuple(sorted(set(scheduler_run_ids))) != scheduler_run_ids:
        raise ReportRepairError(
            f"unsafe report target {index} scheduler ids must be unique and sorted"
        )
    if (
        value.get("invalidated_at") is not None
        or value.get("invalidation_reason") is not None
    ):
        raise ReportRepairError(
            f"unsafe report target {index} was already invalid at freeze"
        )
    target = UnsafeReportTarget(
        task_id=_require_text(value.get("task_id"), label="target task id"),
        revision=revision,
        contract_version=_require_text(
            value.get("contract_version"), label="target contract version"
        ),
        rule_version=_require_text(
            value.get("rule_version"), label="target rule version"
        ),
        taxonomy_version=_require_text(
            value.get("taxonomy_version"), label="target taxonomy version"
        ),
        report_json_path=_require_text(
            value.get("report_json_path"), label="target report path"
        ),
        report_sha256=_require_sha256(
            value.get("report_sha256"), label="target report hash"
        ),
        created_at=_require_text(value.get("created_at"), label="target created_at"),
        creation_source=_require_text(
            value.get("creation_source"), label="target creation source"
        ),
        task_status=_require_text(value.get("task_status"), label="target task status"),
        task_started_at=_require_text(
            value.get("task_started_at"), label="target task started_at"
        ),
        task_completed_at=_require_text(
            value.get("task_completed_at"), label="target task completed_at"
        ),
        scheduler_run_ids=scheduler_run_ids,
    )
    if (
        target.contract_version != UNSAFE_CONTRACT_VERSION
        or target.rule_version != UNSAFE_RULE_VERSION
        or target.taxonomy_version != UNSAFE_TAXONOMY_VERSION
        or target.creation_source != "automatic"
    ):
        raise ReportRepairError(
            f"unsafe report target {index} does not match the approved legacy tuple"
        )
    relative = Path(target.report_json_path)
    if relative.is_absolute() or ".." in relative.parts:
        raise ReportRepairError(f"unsafe report target {index} path is not relative")
    return target


def _scheduler_run_ids(connection: sqlite3.Connection) -> dict[str, tuple[int, ...]]:
    values: dict[str, list[int]] = {}
    for row in connection.execute(
        "SELECT id,details_json FROM scheduler_runs ORDER BY id"
    ):
        try:
            details = json.loads(str(row["details_json"] or "{}"))
        except json.JSONDecodeError:
            continue
        task_id = details.get("task_id") if isinstance(details, dict) else None
        if task_id:
            values.setdefault(str(task_id), []).append(int(row["id"]))
    return {key: tuple(items) for key, items in values.items()}


def _frozen_unsafe_targets(
    connection: sqlite3.Connection,
) -> tuple[UnsafeReportTarget, ...]:
    run_ids = _scheduler_run_ids(connection)
    rows = connection.execute(
        """
        SELECT rr.task_id,rr.revision,rr.contract_version,rr.rule_version,
               rr.taxonomy_version,rr.report_json_path,rr.report_sha256,
               rr.created_at,rt.creation_source,rt.task_status,
               rt.started_at task_started_at,rt.completed_at task_completed_at
        FROM report_revisions rr
        JOIN report_tasks rt ON rt.id=rr.task_id
        WHERE rt.creation_source='automatic'
          AND rr.invalidated_at IS NULL
          AND rr.contract_version=?
          AND rr.rule_version=?
          AND rr.taxonomy_version=?
        ORDER BY rr.task_id,rr.revision
        """,
        (
            UNSAFE_CONTRACT_VERSION,
            UNSAFE_RULE_VERSION,
            UNSAFE_TAXONOMY_VERSION,
        ),
    ).fetchall()
    return tuple(
        UnsafeReportTarget(
            task_id=str(row["task_id"]),
            revision=int(row["revision"]),
            contract_version=str(row["contract_version"]),
            rule_version=str(row["rule_version"]),
            taxonomy_version=str(row["taxonomy_version"]),
            report_json_path=str(row["report_json_path"]),
            report_sha256=str(row["report_sha256"]),
            created_at=str(row["created_at"]),
            creation_source=str(row["creation_source"]),
            task_status=str(row["task_status"]),
            task_started_at=str(row["task_started_at"]),
            task_completed_at=str(row["task_completed_at"]),
            scheduler_run_ids=run_ids.get(str(row["task_id"]), ()),
        )
        for row in rows
    )


def _report_file_projection(
    connection: sqlite3.Connection, target: UnsafeReportTarget
) -> tuple[tuple[Any, ...], ...]:
    rows = connection.execute(
        """
        SELECT * FROM report_files
        WHERE task_id=? AND revision=? ORDER BY id
        """,
        target.key,
    ).fetchall()
    return tuple(tuple(row[column] for column in REPORT_FILE_COLUMNS) for row in rows)


def _load_manifest_boundary(manifest_path: Path) -> ReportRepairBoundary:
    manifest_path = manifest_path.resolve()
    try:
        full_manifest = _load_freeze_manifest(manifest_path)
    except ReleaseManagementError as error:
        raise ReportRepairError(str(error)) from error
    manifest = _read_json_object(manifest_path, label="freeze manifest")
    if _sha256_file(manifest_path) != full_manifest.sha256:
        raise ReportRepairError("freeze manifest changed while it was validated")
    if manifest.get("schema_version") != FREEZE_SCHEMA_VERSION:
        raise ReportRepairError("unsupported freeze manifest schema")
    freeze_lock = Path(
        _require_text(manifest.get("freeze_lock"), label="freeze lock")
    ).resolve()
    if not freeze_lock.is_file():
        raise ReportRepairError("operator freeze lock is missing")
    backup = manifest.get("database_backup")
    if not isinstance(backup, Mapping):
        raise ReportRepairError("freeze manifest database backup is missing")
    backup_name = _require_text(backup.get("path"), label="frozen database path")
    backup_path = (manifest_path.parent / backup_name).resolve()
    try:
        backup_path.relative_to(manifest_path.parent)
    except ValueError as error:
        raise ReportRepairError(
            "frozen database must stay inside its bundle"
        ) from error
    expected_backup_sha256 = _require_sha256(
        backup.get("sha256"), label="frozen database hash"
    )
    if _sha256_file(backup_path) != expected_backup_sha256:
        raise ReportRepairError("frozen database hash mismatch")

    summary = manifest.get("database_summary")
    if not isinstance(summary, Mapping):
        raise ReportRepairError("freeze manifest database summary is missing")
    raw_targets = summary.get("unsafe_automatic_report_revisions")
    if not isinstance(raw_targets, list) or not raw_targets:
        raise ReportRepairError("freeze manifest has no unsafe automatic reports")
    targets = tuple(
        _parse_target(value, index=index) for index, value in enumerate(raw_targets, 1)
    )
    keys = [target.key for target in targets]
    if len(keys) != len(set(keys)):
        raise ReportRepairError("freeze manifest has duplicate unsafe report targets")
    if tuple(sorted(targets, key=lambda item: item.key)) != targets:
        raise ReportRepairError("freeze manifest unsafe reports must be sorted")
    with _existing_connection(backup_path, read_only=True) as frozen:
        frozen_targets = _frozen_unsafe_targets(frozen)
        frozen_report_files = {
            target.key: _report_file_projection(frozen, target) for target in targets
        }
    if frozen_targets != targets:
        raise ReportRepairError(
            "freeze manifest unsafe reports do not match the frozen database"
        )
    if any(not rows for rows in frozen_report_files.values()):
        raise ReportRepairError("frozen unsafe report has no report files")
    return ReportRepairBoundary(
        manifest_sha256=full_manifest.sha256,
        targets=targets,
        frozen_report_files=frozen_report_files,
    )


def _require_v9(connection: sqlite3.Connection) -> None:
    user_version = int(connection.execute("PRAGMA user_version").fetchone()[0])
    migration = connection.execute(
        "SELECT name FROM schema_migrations WHERE version=?", (SCHEMA_VERSION,)
    ).fetchall()
    if (
        user_version != SCHEMA_VERSION
        or len(migration) != 1
        or str(migration[0]["name"]) != "release-bound-evaluation-schema"
    ):
        raise ReportRepairError(f"complete schema v{SCHEMA_VERSION} is required")
    revision_columns = {
        str(row[1]) for row in connection.execute("PRAGMA table_info(report_revisions)")
    }
    if "release_id" not in revision_columns:
        raise ReportRepairError("schema v9 report release lineage is incomplete")
    if connection.execute("PRAGMA foreign_key_check").fetchall():
        raise ReportRepairError("schema v9 has foreign-key violations")


def _safe_artifact_path(root: Path, relative_value: str) -> Path:
    relative = Path(relative_value)
    if relative.is_absolute() or ".." in relative.parts:
        raise ReportRepairError(f"report artifact path is unsafe: {relative_value}")
    resolved = (root / relative).resolve()
    try:
        resolved.relative_to(root)
    except ValueError as error:
        raise ReportRepairError(
            f"report artifact escapes artifact root: {relative_value}"
        ) from error
    return resolved


def _event_payload(target: UnsafeReportTarget, *, manifest_sha256: str) -> str:
    return _canonical_json(
        {
            "freeze_manifest_sha256": manifest_sha256,
            "reason": INVALIDATION_REASON,
            "release_id": LEGACY_V7_RELEASE_ID,
            "report_sha256": target.report_sha256,
            "revision": target.revision,
            "scheduler_run_ids": list(target.scheduler_run_ids),
        }
    )


def _preflight_current_database(
    connection: sqlite3.Connection,
    *,
    boundary: ReportRepairBoundary,
    artifact_root: Path,
) -> tuple[str, int]:
    targets = boundary.targets
    _require_v9(connection)
    active_releases = connection.execute(
        """
        SELECT id,rule_version,taxonomy_version FROM evaluation_releases
        WHERE status='active'
        """
    ).fetchall()
    if len(active_releases) != 1 or (
        str(active_releases[0]["id"]),
        str(active_releases[0]["rule_version"]),
        str(active_releases[0]["taxonomy_version"]),
    ) != (TARGET_RELEASE_ID, "evaluation-v8", "selling-points-v5.1"):
        raise ReportRepairError(
            "unsafe reports may only be invalidated after the v8 release is active"
        )
    legacy_release = connection.execute(
        "SELECT status FROM evaluation_releases WHERE id=?",
        (LEGACY_V7_RELEASE_ID,),
    ).fetchone()
    if legacy_release is None or str(legacy_release["status"]) != "retired":
        raise ReportRepairError("legacy v7 release must be retired")
    rows = connection.execute(
        """
        SELECT rr.*,rt.creation_source,rt.task_status,
               rt.started_at task_started_at,rt.completed_at task_completed_at
        FROM report_revisions rr
        JOIN report_tasks rt ON rt.id=rr.task_id
        WHERE rt.creation_source='automatic'
          AND rr.contract_version=?
          AND rr.rule_version=?
          AND rr.taxonomy_version=?
        ORDER BY rr.task_id,rr.revision
        """,
        (
            UNSAFE_CONTRACT_VERSION,
            UNSAFE_RULE_VERSION,
            UNSAFE_TAXONOMY_VERSION,
        ),
    ).fetchall()
    expected_keys = {target.key for target in targets}
    actual_keys = {(str(row["task_id"]), int(row["revision"])) for row in rows}
    if actual_keys != expected_keys:
        missing = sorted(expected_keys - actual_keys)
        extra = sorted(actual_keys - expected_keys)
        raise ReportRepairError(
            f"current unsafe report set differs from manifest; missing={missing}, extra={extra}"
        )
    rows_by_key = {(str(row["task_id"]), int(row["revision"])): row for row in rows}
    states: list[str] = []
    verified_file_count = 0
    for target in targets:
        row = rows_by_key[target.key]
        static_actual = (
            str(row["contract_version"]),
            str(row["rule_version"]),
            str(row["taxonomy_version"]),
            str(row["report_json_path"]),
            str(row["report_sha256"]),
            str(row["created_at"]),
            str(row["creation_source"]),
            str(row["release_id"]),
        )
        static_expected = (
            target.contract_version,
            target.rule_version,
            target.taxonomy_version,
            target.report_json_path,
            target.report_sha256,
            target.created_at,
            target.creation_source,
            LEGACY_V7_RELEASE_ID,
        )
        if static_actual != static_expected:
            raise ReportRepairError(
                f"report revision no longer matches freeze target: {target.key}"
            )
        invalidated_at = row["invalidated_at"]
        invalidation_reason = row["invalidation_reason"]
        if invalidated_at is None and invalidation_reason is None:
            state = "pending"
            task_actual = (
                str(row["task_status"]),
                str(row["task_started_at"]),
                str(row["task_completed_at"]),
            )
            task_expected = (
                target.task_status,
                target.task_started_at,
                target.task_completed_at,
            )
            if task_actual != task_expected:
                raise ReportRepairError(
                    f"report task changed before invalidation: {target.task_id}"
                )
        elif invalidated_at is not None and invalidation_reason == INVALIDATION_REASON:
            state = "completed"
        else:
            raise ReportRepairError(
                f"report revision has an unexpected invalidation state: {target.key}"
            )
        states.append(state)

        files = connection.execute(
            """
            SELECT * FROM report_files
            WHERE task_id=? AND revision=? ORDER BY id
            """,
            target.key,
        ).fetchall()
        actual_file_projection = tuple(
            tuple(file_row[column] for column in REPORT_FILE_COLUMNS)
            for file_row in files
        )
        if actual_file_projection != boundary.frozen_report_files[target.key]:
            raise ReportRepairError(
                f"report file rows differ from frozen database: {target.key}"
            )
        json_files = [row for row in files if row["file_kind"] == "report-json"]
        if len(json_files) != 1:
            raise ReportRepairError(
                f"report revision must have one report-json file: {target.key}"
            )
        json_file = json_files[0]
        if (
            str(json_file["local_path"]) != target.report_json_path
            or str(json_file["sha256"]) != target.report_sha256
        ):
            raise ReportRepairError(
                f"report-json metadata differs from manifest: {target.key}"
            )
        for file_row in files:
            if str(file_row["status"]) != "available":
                raise ReportRepairError(
                    f"report file is not available: {file_row['id']}"
                )
            artifact = _safe_artifact_path(artifact_root, str(file_row["local_path"]))
            if not artifact.is_file():
                raise ReportRepairError(f"report artifact is missing: {artifact}")
            if artifact.stat().st_size != int(file_row["byte_size"]):
                raise ReportRepairError(f"report artifact size mismatch: {artifact}")
            if _sha256_file(artifact) != str(file_row["sha256"]):
                raise ReportRepairError(f"report artifact hash mismatch: {artifact}")
            verified_file_count += 1

        payload = _event_payload(target, manifest_sha256=boundary.manifest_sha256)
        events = connection.execute(
            """
            SELECT payload_json,created_at FROM task_events
            WHERE task_id=? AND event_type=?
            ORDER BY id
            """,
            (target.task_id, INVALIDATION_EVENT_TYPE),
        ).fetchall()
        matching_events = [
            event for event in events if event["payload_json"] == payload
        ]
        related_events = []
        for event in events:
            try:
                value = json.loads(str(event["payload_json"]))
            except json.JSONDecodeError as error:
                raise ReportRepairError(
                    f"invalid report invalidation event payload: {target.task_id}"
                ) from error
            if isinstance(value, dict) and value.get("revision") == target.revision:
                related_events.append(event)
        if related_events != matching_events:
            raise ReportRepairError(
                f"conflicting report invalidation event exists: {target.key}"
            )
        if state == "pending" and matching_events:
            raise ReportRepairError(
                f"pending revision already has an invalidation event: {target.key}"
            )
        if state == "completed":
            if len(matching_events) != 1:
                raise ReportRepairError(
                    f"completed revision must have one invalidation event: {target.key}"
                )
            if str(matching_events[0]["created_at"]) != str(invalidated_at):
                raise ReportRepairError(
                    f"revision and invalidation event timestamps differ: {target.key}"
                )
    unique_states = set(states)
    if len(unique_states) != 1:
        raise ReportRepairError("unsafe report batch is partially invalidated")
    return states[0], verified_file_count


def invalidate_unsafe_automatic_reports(
    *,
    db_path: Path,
    manifest_path: Path,
    receipt_path: Path,
    artifact_root: Path,
    apply: bool = False,
    acknowledge_rollback_window_close: bool = False,
) -> dict[str, Any]:
    """Validate and optionally invalidate exactly the manifest-listed reports."""

    db_path = db_path.resolve()
    manifest_path = manifest_path.resolve()
    receipt_path = receipt_path.resolve()
    artifact_root = artifact_root.resolve()
    if not artifact_root.is_dir():
        raise ReportRepairError(f"artifact root does not exist: {artifact_root}")
    if apply and not acknowledge_rollback_window_close:
        raise ReportRepairError(
            "applying report invalidation closes the release rollback window; "
            "explicit acknowledgement is required"
        )
    boundary = _load_manifest_boundary(manifest_path)
    try:
        receipt = _read_receipt(receipt_path)
        _require_production_receipt_chain(receipt, expected_database=db_path)
    except ReleaseManagementError as error:
        raise ReportRepairError(str(error)) from error
    with _existing_connection(db_path, read_only=not apply) as connection:
        if apply:
            with transaction(connection):
                state, verified_file_count = _preflight_current_database(
                    connection, boundary=boundary, artifact_root=artifact_root
                )
                updated = 0
                created_events = 0
                if state == "pending":
                    try:
                        manifest = _load_freeze_manifest(manifest_path)
                        if manifest.sha256 != boundary.manifest_sha256:
                            raise ReportRepairError(
                                "freeze manifest changed before report invalidation"
                            )
                        _attested_activation_stable_state(connection, manifest, receipt)
                    except ReleaseManagementError as error:
                        raise ReportRepairError(str(error)) from error
                    captured_at = now_utc()
                    for index, target in enumerate(boundary.targets, 1):
                        cursor = connection.execute(
                            """
                            UPDATE report_revisions
                            SET invalidated_at=?,invalidation_reason=?
                            WHERE task_id=? AND revision=?
                              AND invalidated_at IS NULL
                              AND invalidation_reason IS NULL
                            """,
                            (
                                captured_at,
                                INVALIDATION_REASON,
                                target.task_id,
                                target.revision,
                            ),
                        )
                        if cursor.rowcount != 1:
                            raise ReportRepairError(
                                f"report invalidation CAS failed: {target.key}"
                            )
                        _repair_checkpoint(f"target-{index}-updated")
                        connection.execute(
                            """
                            INSERT INTO task_events(
                                task_id,event_type,message,payload_json,created_at
                            ) VALUES (?,?,?,?,?)
                            """,
                            (
                                target.task_id,
                                INVALIDATION_EVENT_TYPE,
                                f"revision {target.revision} invalidated from freeze manifest",
                                _event_payload(
                                    target,
                                    manifest_sha256=boundary.manifest_sha256,
                                ),
                                captured_at,
                            ),
                        )
                        updated += 1
                        created_events += 1
                    final_state, _ = _preflight_current_database(
                        connection, boundary=boundary, artifact_root=artifact_root
                    )
                    if final_state != "completed":
                        raise ReportRepairError(
                            "unsafe report invalidation did not reach completed state"
                        )
                return {
                    "mode": "apply",
                    "target_count": len(boundary.targets),
                    "pending_count": (
                        len(boundary.targets) if state == "pending" else 0
                    ),
                    "already_invalidated_count": (
                        len(boundary.targets) if state == "completed" else 0
                    ),
                    "invalidated_count": updated,
                    "events_inserted": created_events,
                    "verified_file_count": verified_file_count,
                    "invalidation_reason": INVALIDATION_REASON,
                    "freeze_manifest_sha256": boundary.manifest_sha256,
                    "rollback_window_closed": True,
                }
        state, verified_file_count = _preflight_current_database(
            connection, boundary=boundary, artifact_root=artifact_root
        )
        if state == "pending":
            try:
                manifest = _load_freeze_manifest(manifest_path)
                if manifest.sha256 != boundary.manifest_sha256:
                    raise ReportRepairError(
                        "freeze manifest changed during report preflight"
                    )
                _attested_activation_stable_state(connection, manifest, receipt)
            except ReleaseManagementError as error:
                raise ReportRepairError(str(error)) from error
    return {
        "mode": "dry-run",
        "target_count": len(boundary.targets),
        "pending_count": len(boundary.targets) if state == "pending" else 0,
        "already_invalidated_count": (
            len(boundary.targets) if state == "completed" else 0
        ),
        "invalidated_count": 0,
        "events_inserted": 0,
        "verified_file_count": verified_file_count,
        "invalidation_reason": INVALIDATION_REASON,
        "freeze_manifest_sha256": boundary.manifest_sha256,
        "rollback_window_closed": state == "completed",
    }


def _repair_checkpoint(_name: str) -> None:
    """Patchable failure-injection point for atomicity tests."""
