"""Immutable v8 report tasks, revisions and run-scoped export artifacts."""

from __future__ import annotations

import csv
import hashlib
import html
import json
import os
import shutil
import sqlite3
import subprocess
import tempfile
import unicodedata
import uuid
from collections import Counter
from datetime import date, datetime, time, timedelta, timezone
from pathlib import Path
from typing import Any, Dict, Iterable, List, Mapping, Optional, Sequence
from zoneinfo import ZoneInfo

from .audience_classifier import EVIDENCE_WINDOW_DAYS
from .audience_rate import active_classifier_state, build_channel_audience_rates
from .contracts import (
    CURRENT_REPORT_EVIDENCE_VERSION,
    CURRENT_REPORT_VERSION,
    CURRENT_REPORT_RULE_VERSION,
    LEGACY_CONTRACT_PATHS,
    REPORT_RULE_VERSIONS,
    expected_terminal_task_status,
    load_contract,
    quality_gate_failures,
    quantity_metric,
    ratio_metric,
    validate_report,
)
from .evaluation_selectors import (
    EvaluationSelectorError,
    active_release as selected_active_release,
    effective_direction,
    formal_eligible_release_evaluations,
)
from .duplicates import FINGERPRINT_VERSION, THRESHOLDS, duplicate_metric_decision
from .insights import CHANNELS, SCENES, build_channel_conclusions
from .media_state import media_terminal_states
from .report_export import formula_safe_csv_value
from .storage import (
    DEFAULT_DB,
    PROJECT_ROOT,
    SCHEMA_VERSION,
    SchemaMigrationError,
    connect,
    now_utc,
    require_schema_compatibility,
    transaction,
)


REPORTS_ROOT = PROJECT_ROOT / "reports" / "runs" / "v8"
SHANGHAI = ZoneInfo("Asia/Shanghai")
TASK_TYPES = {"daily", "weekly", "custom"}
RUNNABLE_STATUSES = {"queued", "partial", "failed", "interrupted"}
IMPLICIT_RUN_STATUSES = {"queued", "failed", "interrupted"}
_REPORT_ID_BATCH_SIZE = 500

_QUALITY_GATE_LABELS = {
    "discovery_coverage": "账号采集完成率",
    "detail_coverage": "内容详情采集完成率",
    "metrics_freshness": "播放和互动数据更新率",
    "evaluation_coverage": "卖点评估完成率",
    "core_artifact_coverage": "语音和画面文字识别完成率",
    "media_terminal_coverage": "视频和图片处理完成率",
    "duplicate_fingerprint_coverage": "重复内容识别完成率",
    "weekly_comment_coverage": "评论采集完成率",
    "duplicate_calibration_ready": "重复内容规则校验",
    "pipeline_observation": "每日抓取观测完整度",
}

_TASK_STATUS_LABELS = {
    "queued": "排队中",
    "running": "生成中",
    "succeeded": "已完成",
    "partial": "部分完成",
    "failed": "生成失败",
    "interrupted": "已中断",
    "cancel_requested": "正在取消",
    "cancelled": "已取消",
}


class ReportTaskError(RuntimeError):
    pass


class TaskCancelled(ReportTaskError):
    pass


def _report_id_batches(
    connection: sqlite3.Connection,
    ids: Sequence[int],
    *,
    reserved_parameters: int = 0,
) -> Iterable[list[int]]:
    """Yield bounded ID batches below the live SQLite variable limit."""

    if reserved_parameters < 0:
        raise ValueError("reserved_parameters must be non-negative")
    variable_limit = connection.getlimit(sqlite3.SQLITE_LIMIT_VARIABLE_NUMBER)
    available_parameters = variable_limit - reserved_parameters
    if available_parameters < 1:
        raise ReportTaskError(
            "SQLite variable limit is too low for a report query"
        )
    batch_size = min(_REPORT_ID_BATCH_SIZE, available_parameters)
    for offset in range(0, len(ids), batch_size):
        yield [int(value) for value in ids[offset : offset + batch_size]]


def _task_completion_message(report: Mapping[str, Any]) -> str:
    if report.get("task", {}).get("task_status") != "partial":
        return "报告已生成"
    data_quality = report.get("data_quality")
    if not isinstance(data_quality, Mapping):
        return "报告已生成，但缺少数据质量说明"
    summary = report.get("summary_metrics")
    publications = (
        summary.get("publication_count", {}).get("value")
        if isinstance(summary, Mapping)
        else None
    )
    contract = load_contract(report_version=str(report.get("report_version") or ""))
    failures = quality_gate_failures(
        data_quality,
        data_quality_details=(
            report.get("data_quality_details")
            if isinstance(report.get("data_quality_details"), Mapping)
            else None
        ),
        contract=contract,
        enforce_boolean_quality_gates=bool(publications),
    )
    details: List[str] = []
    for failure in failures:
        key = str(failure["key"])
        label = _QUALITY_GATE_LABELS.get(key, "其他数据检查")
        if failure["kind"] == "boolean":
            details.append(label)
            continue
        actual = failure["actual"]
        required = float(failure["required"])
        if isinstance(actual, (int, float)) and not isinstance(actual, bool):
            details.append(f"{label} {float(actual):g}%（要求至少 {required:g}%）")
        else:
            details.append(f"{label}缺失（要求至少 {required:g}%）")
    if not details:
        details.append("历史报告未保留细分原因")
    return "报告已生成，但以下数据未达到要求：" + "、".join(details)


def assert_report_runtime_ready(connection) -> Dict[str, Any]:
    """Return the pinned current release or fail before any report-side write."""

    try:
        require_schema_compatibility(
            connection, supported_versions=frozenset({SCHEMA_VERSION})
        )
    except SchemaMigrationError as error:
        raise ReportTaskError(str(error)) from error
    try:
        release = selected_active_release(connection)
    except EvaluationSelectorError as error:
        raise ReportTaskError(str(error)) from error
    assert release is not None
    value = dict(release)
    if str(value["rule_version"]) != CURRENT_REPORT_RULE_VERSION:
        raise ReportTaskError(
            "active evaluation release does not match the current report contract"
        )
    taxonomy = connection.execute(
        """
        SELECT version FROM taxonomy_versions
        WHERE version=? AND status='published'
        """,
        (value["taxonomy_version"],),
    ).fetchone()
    if taxonomy is None:
        raise ReportTaskError("active release taxonomy must be published")
    unsafe_automatic_revisions = int(
        connection.execute(
            """
            SELECT COUNT(*) FROM report_revisions rr
            JOIN report_tasks rt ON rt.id=rr.task_id
            WHERE rr.contract_version=?
              AND rr.invalidated_at IS NULL
              AND rt.creation_source='automatic'
              AND rr.release_id<>?
            """,
            (CURRENT_REPORT_VERSION, value["id"]),
        ).fetchone()[0]
    )
    if unsafe_automatic_revisions:
        raise ReportTaskError(
            "report runtime blocked by "
            f"{unsafe_automatic_revisions} automatic current-contract revision(s) "
            "outside the active release"
        )
    return value


def _require_pinned_report_release(connection, expected: Mapping[str, Any]) -> None:
    current = assert_report_runtime_ready(connection)
    for field in (
        "id",
        "rule_version",
        "taxonomy_version",
        "matcher_rule_sha256",
    ):
        if current[field] != expected[field]:
            raise ReportTaskError(
                "active evaluation release changed while the report was generated"
            )


def request_task_cancel(task_id: str, *, db_path: Path = DEFAULT_DB) -> Dict[str, Any]:
    with connect(db_path) as connection, transaction(connection):
        task = connection.execute(
            "SELECT * FROM report_tasks WHERE id=?", (task_id,)
        ).fetchone()
        if task is None:
            raise ReportTaskError(f"task does not exist: {task_id}")
        if "已由发现补跑后的新任务替代" in str(task["message"] or ""):
            raise ReportTaskError(f"superseded task cannot be cancelled: {task_id}")
        current = str(task["task_status"])
        if current == "running":
            next_status, message = "cancel_requested", "正在取消，当前步骤完成后会停止"
        elif current in RUNNABLE_STATUSES:
            next_status, message = "cancelled", "任务已取消"
        elif current == "cancel_requested":
            next_status, message = current, "取消请求已存在"
        else:
            raise ReportTaskError(f"task {task_id} cannot be cancelled from {current}")
        captured_at = now_utc()
        connection.execute(
            """
            UPDATE report_tasks SET task_status=?, message=?,
                completed_at=CASE WHEN ?='cancelled' THEN ? ELSE completed_at END,
                updated_at=? WHERE id=?
            """,
            (next_status, message, next_status, captured_at, captured_at, task_id),
        )
        connection.execute(
            """
            INSERT INTO task_events(task_id,event_type,message,payload_json,created_at)
            VALUES (?, ?, ?, '{}', ?)
            """,
            (
                task_id,
                "cancel_requested"
                if next_status == "cancel_requested"
                else "cancelled",
                message,
                captured_at,
            ),
        )
    return get_task(task_id, db_path=db_path)


def resume_task(task_id: str, *, db_path: Path = DEFAULT_DB) -> Dict[str, Any]:
    with connect(db_path) as connection, transaction(connection):
        task = connection.execute(
            "SELECT task_status,message FROM report_tasks WHERE id=?", (task_id,)
        ).fetchone()
        if task is None:
            raise ReportTaskError(f"task does not exist: {task_id}")
        if "已由发现补跑后的新任务替代" in str(task["message"] or ""):
            raise ReportTaskError(f"superseded task cannot be resumed: {task_id}")
        if task["task_status"] != "cancelled":
            raise ReportTaskError(
                f"task {task_id} cannot be resumed from {task['task_status']}"
            )
        captured_at = now_utc()
        connection.execute(
            """
            UPDATE report_tasks SET task_status='queued', progress=0,
                message='任务已恢复，等待重新生成报告', started_at=NULL,
                completed_at=NULL, updated_at=? WHERE id=?
            """,
            (captured_at, task_id),
        )
        connection.execute(
            """
            INSERT INTO task_events(task_id,event_type,message,payload_json,created_at)
            VALUES (?, 'resumed', '任务已恢复', '{}', ?)
            """,
            (task_id, captured_at),
        )
    return get_task(task_id, db_path=db_path)


def retry_task(task_id: str, *, db_path: Path = DEFAULT_DB) -> Dict[str, Any]:
    """Queue a new immutable revision without changing any previous revision."""

    with connect(db_path) as connection, transaction(connection):
        task = connection.execute(
            "SELECT task_status,message FROM report_tasks WHERE id=?", (task_id,)
        ).fetchone()
        if task is None:
            raise ReportTaskError(f"task does not exist: {task_id}")
        if "已由发现补跑后的新任务替代" in str(task["message"] or ""):
            raise ReportTaskError(f"superseded task cannot be retried: {task_id}")
        if task["task_status"] not in {"succeeded", "partial", "failed", "interrupted"}:
            raise ReportTaskError(
                f"task {task_id} cannot create a revision from {task['task_status']}"
            )
        captured_at = now_utc()
        connection.execute(
            """
            UPDATE report_tasks SET task_status='queued', progress=0,
                message='已请求重新生成报告', started_at=NULL, completed_at=NULL,
                updated_at=? WHERE id=?
            """,
            (captured_at, task_id),
        )
        connection.execute(
            """
            INSERT INTO task_events(task_id,event_type,message,payload_json,created_at)
            VALUES (?, 'retry_requested', '已请求重新生成报告', '{}', ?)
            """,
            (task_id, captured_at),
        )
    return get_task(task_id, db_path=db_path)


def _acknowledge_cancel(task_id: str, *, db_path: Path) -> bool:
    with connect(db_path) as connection, transaction(connection):
        task = connection.execute(
            "SELECT task_status FROM report_tasks WHERE id=?", (task_id,)
        ).fetchone()
        if task is None or task["task_status"] != "cancel_requested":
            return False
        captured_at = now_utc()
        connection.execute(
            """
            UPDATE report_tasks SET task_status='cancelled', message='任务已取消',
                completed_at=?, updated_at=? WHERE id=?
            """,
            (captured_at, captured_at, task_id),
        )
        connection.execute(
            """
            INSERT INTO task_events(task_id,event_type,message,payload_json,created_at)
            VALUES (?, 'cancelled', '任务已取消', '{}', ?)
            """,
            (task_id, captured_at),
        )
    return True


def _date(value: str) -> date:
    try:
        return date.fromisoformat(value)
    except ValueError as exc:
        raise ReportTaskError(f"invalid ISO date: {value}") from exc


def period_bounds(period_start: str, period_end: str) -> tuple[str, str]:
    start = _date(period_start)
    end = _date(period_end)
    if start > end:
        raise ReportTaskError("period_start must not be after period_end")
    start_local = datetime.combine(start, time.min, SHANGHAI)
    end_local = datetime.combine(end + timedelta(days=1), time.min, SHANGHAI)
    return (
        start_local.astimezone(timezone.utc)
        .isoformat(timespec="seconds")
        .replace("+00:00", "Z"),
        end_local.astimezone(timezone.utc)
        .isoformat(timespec="seconds")
        .replace("+00:00", "Z"),
    )


def _task_id(task_type: str, period_start: str, period_end: str, source: str) -> str:
    if source == "automatic":
        prefix = "D" if task_type == "daily" else "W"
        return (
            f"D8-{prefix}-{period_start.replace('-', '')}-{period_end.replace('-', '')}"
        )
    return f"D8-C-{uuid.uuid4().hex[:12].upper()}"


def create_task(
    *,
    task_type: str,
    period_start: str,
    period_end: str,
    creation_source: str,
    name: Optional[str] = None,
    db_path: Path = DEFAULT_DB,
) -> Dict[str, Any]:
    if task_type not in TASK_TYPES:
        raise ReportTaskError(f"unsupported task type: {task_type}")
    if creation_source not in {"automatic", "manual"}:
        raise ReportTaskError("creation_source must be automatic or manual")
    start = _date(period_start)
    end = _date(period_end)
    period_bounds(period_start, period_end)
    if task_type == "daily" and start != end:
        raise ReportTaskError("daily task must cover exactly one calendar day")
    if task_type == "weekly" and (
        start.weekday() != 0 or end != start + timedelta(days=6)
    ):
        raise ReportTaskError("weekly task must cover Monday through Sunday")
    if creation_source == "manual" and task_type != "custom":
        raise ReportTaskError("manual report creation must use custom task type")
    if creation_source == "automatic" and task_type == "custom":
        raise ReportTaskError("automatic report creation supports daily or weekly only")
    created_at = now_utc()
    if creation_source == "manual":
        _, period_end_utc = period_bounds(period_start, period_end)
        created = datetime.fromisoformat(created_at.replace("Z", "+00:00"))
        closed_at = datetime.fromisoformat(period_end_utc.replace("Z", "+00:00"))
        if created < closed_at:
            raise ReportTaskError("manual report period must be closed before creation")
    task_id = _task_id(task_type, period_start, period_end, creation_source)
    display_name = (
        name.strip()
        if name and name.strip()
        else (
            f"{period_start} 日报"
            if task_type == "daily"
            else f"{period_start} 至 {period_end} 周报"
            if task_type == "weekly"
            else f"{period_start} 至 {period_end} 自定义报告"
        )
    )
    with connect(db_path) as connection, transaction(connection):
        if creation_source == "automatic":
            existing = connection.execute(
                """
                SELECT * FROM report_tasks
                WHERE task_type=? AND period_start=? AND period_end=? AND creation_source='automatic'
                """,
                (task_type, period_start, period_end),
            ).fetchone()
            if existing is not None:
                return dict(existing)
        connection.execute(
            """
            INSERT INTO report_tasks(
                id, task_type, name, period_start, period_end, creation_source,
                task_status, progress, message, created_at, updated_at
            ) VALUES (?, ?, ?, ?, ?, ?, 'queued', 0, '等待生成报告', ?, ?)
            """,
            (
                task_id,
                task_type,
                display_name,
                period_start,
                period_end,
                creation_source,
                created_at,
                created_at,
            ),
        )
        connection.execute(
            """
            INSERT INTO task_events(task_id, event_type, message, payload_json, created_at)
            VALUES (?, 'created', '报告任务已创建', '{}', ?)
            """,
            (task_id, created_at),
        )
    return get_task(task_id, db_path=db_path)


def _task_revision_read_model(connection, task_id: str) -> Dict[str, Any]:
    try:
        active = selected_active_release(connection)
    except EvaluationSelectorError as error:
        raise ReportTaskError(str(error)) from error
    assert active is not None
    revisions = connection.execute(
        """
        SELECT rr.*,er.status release_status
        FROM report_revisions rr
        JOIN evaluation_releases er ON er.id=rr.release_id
        WHERE rr.task_id=? ORDER BY rr.revision DESC
        """,
        (task_id,),
    ).fetchall()
    revision_values: List[Dict[str, Any]] = []
    for row in revisions:
        files = connection.execute(
            """
            SELECT id,file_kind,byte_size,status,error_message,created_at
            FROM report_files WHERE task_id=? AND revision=? ORDER BY file_kind
            """,
            (task_id, row["revision"]),
        ).fetchall()
        value = dict(row)
        value["files"] = [dict(item) for item in files]
        value["revision_state"] = "historical"
        revision_values.append(value)

    current = next(
        (
            value
            for value in revision_values
            if value["invalidated_at"] is None
            and value["release_id"] == active["id"]
            and value["contract_version"] == CURRENT_REPORT_VERSION
            and value["rule_version"] == CURRENT_REPORT_RULE_VERSION
            and value["rule_version"] == active["rule_version"]
            and value["taxonomy_version"] == active["taxonomy_version"]
        ),
        None,
    )
    stale = None
    if current is None:
        stale = next(
            (
                value
                for value in revision_values
                if value["invalidated_at"] is None
                and value["release_id"] == active["id"]
                and value["contract_version"] != CURRENT_REPORT_VERSION
                and value["contract_version"] in LEGACY_CONTRACT_PATHS
                and value["rule_version"] == active["rule_version"]
                and value["taxonomy_version"] == active["taxonomy_version"]
                and REPORT_RULE_VERSIONS.get(value["contract_version"])
                == value["rule_version"]
            ),
            None,
        )
    if current is None and stale is None:
        stale = next(
            (
                value
                for value in revision_values
                if value["invalidated_at"] is None
                and value["release_status"] == "retired"
                and value["contract_version"] in LEGACY_CONTRACT_PATHS
                and REPORT_RULE_VERSIONS.get(value["contract_version"])
                == value["rule_version"]
            ),
            None,
        )
    if current is not None:
        current["revision_state"] = "current"
    if stale is not None:
        stale["revision_state"] = "stale"
    return {
        "revisions": revision_values,
        "current_valid_revision": current,
        "stale_display_revision": stale,
        "display_effective_revision": current or stale,
        "historical_revision_count": sum(
            value["revision_state"] != "current" for value in revision_values
        ),
        "revision_count": len(revision_values),
    }


def list_tasks(*, db_path: Path = DEFAULT_DB) -> List[Dict[str, Any]]:
    with connect(db_path) as connection:
        tasks = connection.execute(
            """
            SELECT t.*,
                   (SELECT COUNT(*) FROM task_contents tc
                    WHERE tc.task_id=t.id AND tc.inclusion_status='included')
                       content_count,
                   (SELECT COUNT(*) FROM task_contents tc
                    WHERE tc.task_id=t.id
                      AND tc.inclusion_status='excluded_missing_boundary')
                       missing_boundary_count
            FROM report_tasks t ORDER BY t.created_at DESC,t.id DESC
            """
        ).fetchall()
        values: List[Dict[str, Any]] = []
        for task in tasks:
            value = dict(task)
            value.update(_task_revision_read_model(connection, str(task["id"])))
            values.append(value)
    return values


def get_task(task_id: str, *, db_path: Path = DEFAULT_DB) -> Dict[str, Any]:
    with connect(db_path) as connection:
        task = connection.execute(
            "SELECT * FROM report_tasks WHERE id=?", (task_id,)
        ).fetchone()
        if task is None:
            raise ReportTaskError(f"task does not exist: {task_id}")
        events = connection.execute(
            "SELECT * FROM task_events WHERE task_id=? ORDER BY id", (task_id,)
        ).fetchall()
        counts = connection.execute(
            """
            SELECT inclusion_status,COUNT(*) count FROM task_contents
            WHERE task_id=? GROUP BY inclusion_status
            """,
            (task_id,),
        ).fetchall()
        revision_model = _task_revision_read_model(connection, task_id)
    value = dict(task)
    value["events"] = [dict(row) for row in events]
    value.update(revision_model)
    value["content_counts"] = {
        str(row["inclusion_status"]): int(row["count"]) for row in counts
    }
    return value


def _snapshot_task_contents(connection, task: Mapping[str, Any]) -> None:
    existing = int(
        connection.execute(
            "SELECT COUNT(*) FROM task_contents WHERE task_id=?", (task["id"],)
        ).fetchone()[0]
    )
    if existing and str(task["creation_source"]) != "automatic":
        return
    start_utc, end_utc = period_bounds(
        str(task["period_start"]), str(task["period_end"])
    )
    included = connection.execute(
        """
        SELECT id FROM content_items
        WHERE published_at>=? AND published_at<? ORDER BY published_at, id
        """,
        (start_utc, end_utc),
    ).fetchall()
    for row in included:
        connection.execute(
            """
            INSERT INTO task_contents(task_id, content_id, inclusion_status, reason)
            VALUES (?, ?, 'included', '发布日期位于任务自然日边界内')
            ON CONFLICT(task_id,content_id) DO UPDATE SET
                inclusion_status='included',reason=excluded.reason
            """,
            (task["id"], row["id"]),
        )
    missing = connection.execute(
        "SELECT id FROM content_items WHERE published_at IS NULL ORDER BY id"
    ).fetchall()
    for row in missing:
        connection.execute(
            """
            INSERT INTO task_contents(task_id, content_id, inclusion_status, reason)
            VALUES (?, ?, 'excluded_missing_boundary', '发布日期缺失，不能归入报告区间')
            ON CONFLICT(task_id,content_id) DO NOTHING
            """,
            (task["id"], row["id"]),
        )


def _collection_cutoff_at(
    task: Mapping[str, Any], *, generated_at: str
) -> str:
    """Return the evidence cutoff independently from revision generation time."""

    task_type = str(task["task_type"])
    creation_source = str(task["creation_source"])
    if creation_source == "automatic" and task_type in {"daily", "weekly"}:
        cutoff_day = _date(str(task["period_end"])) + timedelta(days=1)
        cutoff_time = time(8, 0) if task_type == "daily" else time(8, 30)
        cutoff = datetime.combine(cutoff_day, cutoff_time, SHANGHAI)
    else:
        created_at = task.get("created_at")
        if not isinstance(created_at, str) or not created_at:
            raise ReportTaskError("manual report task is missing its fixed created_at")
        try:
            cutoff = datetime.fromisoformat(created_at.replace("Z", "+00:00"))
        except ValueError as error:
            raise ReportTaskError(
                f"invalid report task created_at timestamp: {created_at}"
            ) from error
        if cutoff.tzinfo is None:
            raise ReportTaskError("report task created_at timestamp must include timezone")
    return (
        cutoff.astimezone(timezone.utc)
        .isoformat(timespec="seconds")
        .replace("+00:00", "Z")
    )


def _latest_metric_observations_at(
    connection, ids: Sequence[int], *, cutoff_at: str
) -> Dict[int, Dict[str, Any]]:
    if not ids:
        return {}
    output: Dict[int, Dict[str, Any]] = {}
    for batch in _report_id_batches(
        connection, ids, reserved_parameters=1
    ):
        placeholders = ",".join("?" for _ in batch)
        rows = connection.execute(
            f"""
            SELECT * FROM content_metric_observations
            WHERE content_id IN ({placeholders})
              AND julianday(captured_at)<=julianday(?)
            ORDER BY content_id,julianday(captured_at) DESC,id DESC
            """,
            [*batch, cutoff_at],
        ).fetchall()
        for row in rows:
            content_id = int(row["content_id"])
            if content_id not in output:
                output[content_id] = dict(row)
    return output


def _metric_freshness_detail(
    connection,
    ids: Sequence[int],
    *,
    cutoff_at: str,
    minimum_percentage: float,
    latest_observations: Optional[Mapping[int, Mapping[str, Any]]] = None,
) -> Dict[str, Any]:
    latest = (
        dict(latest_observations)
        if latest_observations is not None
        else _latest_metric_observations_at(connection, ids, cutoff_at=cutoff_at)
    )
    eligible_count = len(ids)
    cutoff = datetime.fromisoformat(cutoff_at.replace("Z", "+00:00"))
    freshness_start = cutoff - timedelta(hours=36)
    fresh_count = sum(
        1
        for observation in latest.values()
        if observation["status"] == "available"
        and freshness_start
        <= datetime.fromisoformat(
            str(observation["captured_at"]).replace("Z", "+00:00")
        )
        <= cutoff
    )
    percentage = _percentage(fresh_count, eligible_count)
    if eligible_count == 0:
        status = "not_applicable"
        reason = "所选时间内没有发布内容"
    elif percentage is not None and percentage >= minimum_percentage:
        status = "available"
        reason = ""
    else:
        status = "below_threshold"
        reason = (
            f"截止统计前 36 小时内有更新的数据占 "
            f"{float(percentage or 0):.2f}%，低于至少 {minimum_percentage:g}% 的要求"
        )
    return {
        "status": status,
        "fresh_count": fresh_count,
        "as_of_snapshot_count": len(latest),
        "eligible_count": eligible_count,
        "percentage": percentage,
        "window_hours": 36,
        "eligible_basis": "all_window_contents",
        "reason": reason,
    }


def _percentage(numerator: int, denominator: int) -> Optional[float]:
    return round(numerator * 100 / denominator, 2) if denominator else None


def _dimension(items: Iterable[str], total: int) -> List[Dict[str, Any]]:
    counts = Counter(items)
    return [
        {"key": key, "count": count, "percentage": _percentage(count, total)}
        for key, count in sorted(counts.items())
    ]


_DISCOVERY_TERMINAL_REASONS = frozenset(
    {"window_start_reached", "provider_exhausted"}
)
_DISCOVERY_SUCCESS_PAGE_STATUSES = frozenset({"succeeded", "already_succeeded"})
_DISCOVERY_SUCCESS_RULE = (
    "identity.status=succeeded; stopped_reason in "
    "{window_start_reached,provider_exhausted}; pages are contiguous and non-empty; "
    "every page status in {succeeded,already_succeeded}, has no missing published_at "
    "or derived failure; provider_exhausted requires final has_more=false"
)


def _daily_capture_occurrence_for(report_day: date) -> str:
    occurrence = datetime.combine(report_day + timedelta(days=1), time(2, 0), SHANGHAI)
    return (
        occurrence.astimezone(timezone.utc)
        .isoformat(timespec="seconds")
        .replace("+00:00", "Z")
    )


def _parse_daily_capture_receipt(details_json: str) -> Dict[str, Any]:
    try:
        details = json.loads(details_json)
    except (TypeError, json.JSONDecodeError):
        details = None
    details_valid = isinstance(details, Mapping)
    if not details_valid:
        details = {}
    monitored_value = details.get("monitored_accounts")
    monitored_valid = (
        int(monitored_value)
        if isinstance(monitored_value, int)
        and not isinstance(monitored_value, bool)
        and monitored_value >= 0
        else None
    )
    monitored_accounts = monitored_valid or 0
    discovery = details.get("discovery")
    rows = discovery if isinstance(discovery, list) else []
    discovery_valid = isinstance(discovery, list) and all(
        isinstance(item, Mapping) for item in discovery
    )
    by_identity: Dict[tuple[int, str], List[Mapping[str, Any]]] = {}
    discovered_content_count = 0
    for item in rows:
        if not isinstance(item, Mapping):
            continue
        account_id = item.get("account_id")
        platform = item.get("platform")
        if (
            isinstance(account_id, bool)
            or not isinstance(account_id, int)
            or platform not in {"douyin", "xiaohongshu"}
        ):
            continue
        by_identity.setdefault((account_id, str(platform)), []).append(item)
        for key in ("inserted", "updated"):
            value = item.get(key)
            if isinstance(value, int) and not isinstance(value, bool) and value > 0:
                discovered_content_count += value
    content_count_observed = all(
        all(
            isinstance(item.get(key), int)
            and not isinstance(item.get(key), bool)
            and int(item[key]) >= 0
            for key in ("inserted", "updated")
        )
        for item in rows
        if isinstance(item, Mapping)
    )

    successful: set[tuple[int, str]] = set()
    for identity, items in by_identity.items():
        if len(items) != 1:
            continue
        item = items[0]
        pages = item.get("pages")
        page_numbers = (
            [page.get("page") for page in pages if isinstance(page, Mapping)]
            if isinstance(pages, list)
            else []
        )
        pages_valid = (
            isinstance(pages, list)
            and bool(pages)
            and page_numbers == list(range(1, len(pages) + 1))
            and all(
                isinstance(page, Mapping)
                and page.get("status") in _DISCOVERY_SUCCESS_PAGE_STATUSES
                and int(page.get("missing_published_at_count") or 0) == 0
                and not (
                    isinstance(page.get("derived_stages"), Mapping)
                    and page["derived_stages"].get("failures")
                )
                for page in pages
            )
        )
        terminal_valid = item.get("stopped_reason") in _DISCOVERY_TERMINAL_REASONS
        if (
            terminal_valid
            and item.get("stopped_reason") == "provider_exhausted"
            and isinstance(pages, list)
            and pages
        ):
            terminal_valid = (
                isinstance(pages[-1], Mapping)
                and pages[-1].get("has_more") is False
            )
        if (
            item.get("status") == "succeeded"
            and terminal_valid
            and pages_valid
        ):
            successful.add(identity)
    roster_count = len(by_identity)
    roster_matches_monitored = roster_count == monitored_accounts
    receipt_valid = bool(
        details_valid
        and monitored_valid is not None
        and discovery_valid
        and roster_matches_monitored
        and all(len(items) == 1 for items in by_identity.values())
    )
    fully_observed = receipt_valid and len(successful) == monitored_accounts
    return {
        "valid": receipt_valid,
        "covered": len(successful) if roster_matches_monitored else 0,
        "expected": max(monitored_accounts, roster_count),
        "roster_count": roster_count,
        "roster_matches_monitored": roster_matches_monitored,
        "fully_observed": fully_observed,
        "discovered_content_count": discovered_content_count,
        "empty_discovery": (
            fully_observed and content_count_observed and discovered_content_count == 0
        ),
    }


def _automatic_capture_observation_start_date(
    connection, *, generated_at: Optional[str] = None
) -> Optional[date]:
    parameters: List[Any] = []
    generated_filter = ""
    if generated_at is not None:
        generated_filter = " AND julianday(sra.started_at)<=julianday(?)"
        parameters.append(generated_at)
    row = connection.execute(
        f"""
        SELECT sr.scheduled_for
        FROM scheduler_run_attempts sra
        JOIN scheduler_runs sr ON sr.id=sra.scheduler_run_id
        WHERE sr.job_id='daily_capture'
          AND sra.invocation_source='scheduled'
          {generated_filter}
        ORDER BY sr.scheduled_for,sra.attempt_number
        LIMIT 1
        """,
        parameters,
    ).fetchone()
    if row is None:
        return None
    try:
        occurrence = datetime.fromisoformat(
            str(row["scheduled_for"]).replace("Z", "+00:00")
        )
    except ValueError as exc:
        raise ReportTaskError("scheduled daily capture occurrence is invalid") from exc
    if occurrence.tzinfo is None:
        raise ReportTaskError("scheduled daily capture occurrence lacks timezone")
    return occurrence.astimezone(SHANGHAI).date() - timedelta(days=1)


def _daily_capture_receipts(
    connection, *, generated_at: str
) -> Dict[str, Dict[str, Any]]:
    receipts: Dict[str, Dict[str, Any]] = {}
    for row in connection.execute(
            """
            SELECT scheduled_for,status,details_json FROM scheduler_runs
            WHERE job_id='daily_capture' AND completed_at IS NOT NULL
              AND status IN ('succeeded','partial')
              AND julianday(completed_at)<=julianday(?)
            ORDER BY scheduled_for
            """,
            (generated_at,),
        ).fetchall():
        receipt = _parse_daily_capture_receipt(str(row["details_json"] or "{}"))
        if receipt["valid"]:
            receipts[str(row["scheduled_for"])] = receipt
    return receipts


def _pipeline_observation_detail(
    connection,
    task: Mapping[str, Any],
    *,
    generated_at: str,
    receipts: Optional[Mapping[str, Mapping[str, Any]]] = None,
) -> Dict[str, Any]:
    start = _date(str(task["period_start"]))
    end = _date(str(task["period_end"]))
    observation_start = _automatic_capture_observation_start_date(
        connection, generated_at=generated_at
    )
    capture_receipts = receipts or _daily_capture_receipts(
        connection, generated_at=generated_at
    )
    expected_dates: List[str] = []
    legacy_unobserved_dates: List[str] = []
    pipeline_gap_dates: List[str] = []
    zero_content_dates: List[str] = []
    current = start
    while current <= end:
        day = current.isoformat()
        expected_dates.append(day)
        receipt = capture_receipts.get(_daily_capture_occurrence_for(current))
        if receipt is None:
            if observation_start is None or current < observation_start:
                legacy_unobserved_dates.append(day)
            else:
                pipeline_gap_dates.append(day)
        elif bool(receipt.get("empty_discovery")):
            zero_content_dates.append(day)
        current += timedelta(days=1)
    return {
        "status": (
            "complete"
            if not legacy_unobserved_dates and not pipeline_gap_dates
            else "incomplete"
        ),
        "capture_observation_start_date": (
            observation_start.isoformat() if observation_start is not None else None
        ),
        "expected_dates": expected_dates,
        "legacy_unobserved_dates": legacy_unobserved_dates,
        "pipeline_gap_dates": pipeline_gap_dates,
        "zero_content_dates": zero_content_dates,
    }


def _nearest_discovery_roster_count(
    receipts: Mapping[str, Mapping[str, Any]], target_occurrence: str
) -> int:
    target = datetime.fromisoformat(target_occurrence.replace("Z", "+00:00"))
    candidates: List[tuple[datetime, int]] = []
    for scheduled_for, receipt in receipts.items():
        if not bool(receipt.get("roster_matches_monitored")):
            continue
        expected = int(receipt.get("expected") or 0)
        if expected <= 0:
            continue
        try:
            occurrence = datetime.fromisoformat(scheduled_for.replace("Z", "+00:00"))
        except ValueError:
            continue
        if occurrence <= target:
            candidates.append((occurrence, expected))
    if candidates:
        return max(candidates)[1]
    future_candidates: List[tuple[datetime, int]] = []
    for scheduled_for, receipt in receipts.items():
        if not bool(receipt.get("roster_matches_monitored")):
            continue
        expected = int(receipt.get("expected") or 0)
        if expected <= 0:
            continue
        try:
            occurrence = datetime.fromisoformat(scheduled_for.replace("Z", "+00:00"))
        except ValueError:
            continue
        if occurrence > target:
            future_candidates.append((occurrence, expected))
    return min(future_candidates)[1] if future_candidates else 0


def _discovery_coverage_detail(
    connection,
    task: Mapping[str, Any],
    *,
    generated_at: str,
    minimum_percentage: float,
) -> Dict[str, Any]:
    receipts = _daily_capture_receipts(connection, generated_at=generated_at)
    start = _date(str(task["period_start"]))
    end = _date(str(task["period_end"]))
    covered_count = 0
    expected_count = 0
    observed = 0
    validation_failures = 0
    missing_dates: List[str] = []
    active_identity_count = int(
        connection.execute(
            """
            SELECT COUNT(*) FROM account_platform_identities api
            JOIN accounts a ON a.id=api.account_id
            WHERE a.enabled=1 AND api.platform IN ('douyin','xiaohongshu')
            """
        ).fetchone()[0]
    )
    current = start
    while current <= end:
        occurrence = _daily_capture_occurrence_for(current)
        receipt = receipts.get(occurrence)
        if receipt is None or int(receipt.get("expected") or 0) <= 0:
            inherited_roster = _nearest_discovery_roster_count(receipts, occurrence)
            expected_count += inherited_roster or active_identity_count
            missing_dates.append(current.isoformat())
        elif not bool(receipt["roster_matches_monitored"]):
            expected_count += int(receipt["expected"])
            validation_failures += 1
            missing_dates.append(current.isoformat())
        else:
            observed += 1
            expected_count += int(receipt["expected"])
            covered_count += min(
                int(receipt["covered"]), int(receipt["expected"])
            )
        current += timedelta(days=1)

    if expected_count > 0:
        percentage = round(covered_count * 100 / expected_count, 2)
        status = "available" if percentage >= minimum_percentage else "below_threshold"
    else:
        if active_identity_count == 0:
            percentage = None
            status = "not_applicable"
        else:
            percentage = 0.0
            status = "below_threshold"
    if status == "available":
        reason = ""
    elif status == "not_applicable":
        reason = "所选时间内没有需要采集的平台账号"
    elif expected_count <= 0:
        reason = "没有找到可核对的历史账号采集记录，因此按 0% 计算"
    else:
        percentage_value = float(percentage) if percentage is not None else 0.0
        reason = (
            f"所选时间内账号采集完成率为 {percentage_value:.2f}%（{covered_count}/"
            f"{expected_count}），低于至少 {minimum_percentage:g}% 的要求"
        )
    return {
        "status": status,
        "covered_identity_occurrence_count": covered_count,
        "eligible_identity_occurrence_count": expected_count,
        "percentage": percentage,
        "eligible_basis": "scheduled_daily_capture_identity_occurrences",
        "expected_occurrence_count": (end - start).days + 1,
        "observed_occurrence_count": observed,
        "missing_occurrence_dates": missing_dates,
        "roster_validation_failures": validation_failures,
        "success_rule": _DISCOVERY_SUCCESS_RULE,
        "reason": reason,
    }


def _report_audience_rates(
    connection,
    conclusion_rows: List[Dict[str, Any]],
    *,
    window_end_utc: str,
    report_cutoff_at: str,
) -> Dict[str, Dict[str, Any]]:
    """Per-slice automotive_user_rate for one report window.

    The classifier state resolves through the shared calibration gate. An
    uncalibrated classifier may publish only a fully classified sample; an
    incomplete user classification never becomes a zero-valued percentage.
    """

    window_end = datetime.fromisoformat(window_end_utc.replace("Z", "+00:00"))
    evidence_window_start = (
        (window_end - timedelta(days=EVIDENCE_WINDOW_DAYS))
        .isoformat(timespec="seconds")
        .replace("+00:00", "Z")
    )
    return build_channel_audience_rates(
        connection,
        conclusion_rows,
        classifier_state=active_classifier_state(connection),
        evidence_window_start=evidence_window_start,
        evidence_window_end=window_end_utc,
        report_cutoff_at=report_cutoff_at,
        warm_up=True,
        channels=CHANNELS,
        scenes=SCENES,
    )


def _build_report_data(
    connection,
    task: Mapping[str, Any],
    *,
    release: Mapping[str, Any],
    revision: int,
    generated_at: str,
    files: List[Dict[str, Any]],
) -> Dict[str, Any]:
    taxonomy = connection.execute(
        "SELECT * FROM taxonomy_versions WHERE version=? AND status='published'",
        (release["taxonomy_version"],),
    ).fetchone()
    if taxonomy is None:
        raise ReportTaskError("active release taxonomy must be published")
    content_rows = connection.execute(
        """
        SELECT c.*, COALESCE(a.account_type, c.legacy_account_type, 'unknown') account_type,
               a.content_direction account_content_direction
        FROM task_contents tc JOIN content_items c ON c.id=tc.content_id
        LEFT JOIN accounts a ON a.id=c.account_id
        WHERE tc.task_id=? AND tc.inclusion_status='included'
        ORDER BY c.published_at, c.id
        """,
        (task["id"],),
    ).fetchall()
    contents = [dict(row) for row in content_rows]
    ids = [int(row["id"]) for row in content_rows]
    collection_cutoff_at = _collection_cutoff_at(task, generated_at=generated_at)
    snapshots = _latest_metric_observations_at(
        connection, ids, cutoff_at=collection_cutoff_at
    )
    contract = load_contract(report_version=CURRENT_REPORT_VERSION)
    coverage_thresholds = contract["required_coverage_thresholds"]
    metric_display_thresholds = contract["metric_display_coverage_thresholds"]
    evaluation_minimum = float(coverage_thresholds["evaluation_coverage"])
    fingerprint_minimum = float(
        coverage_thresholds["duplicate_fingerprint_coverage"]
    )
    view_minimum = float(metric_display_thresholds["view_count"])
    comment_minimum = float(metric_display_thresholds["comment_count"])
    freshness_contract = contract["required_quality_details"]["metrics_freshness"]
    discovery_contract = contract["required_quality_details"]["discovery_coverage"]
    discovery_detail = _discovery_coverage_detail(
        connection,
        task,
        generated_at=generated_at,
        minimum_percentage=float(discovery_contract["minimum_percentage"]),
    )
    pipeline_observation = _pipeline_observation_detail(
        connection,
        task,
        generated_at=generated_at,
    )
    freshness_detail = _metric_freshness_detail(
        connection,
        ids,
        cutoff_at=collection_cutoff_at,
        minimum_percentage=float(freshness_contract["minimum_percentage"]),
        latest_observations=snapshots,
    )
    eligible_evaluations: Dict[int, Dict[str, Any]] = {}
    for batch in _report_id_batches(
        connection, ids, reserved_parameters=1
    ):
        eligible_evaluations.update(
            formal_eligible_release_evaluations(
                connection, str(release["id"]), batch
            )
        )
    included_evaluations = {
        content_id: value
        for content_id, value in eligible_evaluations.items()
        if int(value["selling_point_included"]) == 1
    }
    for content in contents:
        content_id = int(content["id"])
        formal_content = {**content, "evaluation_content_direction": None}
        content["resolved_direction"] = effective_direction(
            formal_content, eligible_evaluations.get(content_id)
        )
    total = len(contents)
    eval_ready = len(eligible_evaluations)
    vertical = sum(
        1
        for value in eligible_evaluations.values()
        if value["content_automotive_score"] is not None
        and int(value["content_automotive_score"]) >= 60
    )
    selling = len(included_evaluations)
    active_accounts = len(
        {
            int(row["account_id"])
            for row in content_rows
            if row["account_id"] is not None
        }
    )
    detail_ready = 0
    fingerprint_ready = 0
    duplicate_calibration_ready = False
    if ids:
        for batch in _report_id_batches(connection, ids):
            placeholders = ",".join("?" for _ in batch)
            detail_ready += int(
                connection.execute(
                    f"""
                    SELECT COUNT(DISTINCT content_id) FROM fetch_slots
                    WHERE content_id IN ({placeholders})
                      AND stage='detail' AND status='succeeded'
                    """,
                    batch,
                ).fetchone()[0]
            )
        for batch in _report_id_batches(
            connection, ids, reserved_parameters=1
        ):
            placeholders = ",".join("?" for _ in batch)
            fingerprint_ready += int(
                connection.execute(
                    f"""
                    SELECT COUNT(DISTINCT content_id) FROM duplicate_fingerprints
                    WHERE content_id IN ({placeholders}) AND fingerprint_version=?
                    """,
                    [*batch, FINGERPRINT_VERSION],
                ).fetchone()[0]
            )
        duplicate_calibration_ready = (
            connection.execute(
                """
            SELECT 1 FROM duplicate_calibration_runs
            WHERE fingerprint_version=? AND thresholds_json=? AND status='passed'
            LIMIT 1
            """,
                (
                    FINGERPRINT_VERSION,
                    json.dumps(
                        THRESHOLDS,
                        ensure_ascii=False,
                        sort_keys=True,
                        separators=(",", ":"),
                    ),
                ),
            ).fetchone()
            is not None
        )
    recent_comment_ids: set[int] = set()
    if ids:
        comment_cutoff = (
            (
                datetime.fromisoformat(collection_cutoff_at.replace("Z", "+00:00"))
                - timedelta(days=8)
            )
            .isoformat()
            .replace("+00:00", "Z")
        )
        for batch in _report_id_batches(
            connection, ids, reserved_parameters=2
        ):
            placeholders = ",".join("?" for _ in batch)
            recent_comment_ids.update(
                int(row["content_id"])
                for row in connection.execute(
                    f"""
                    SELECT DISTINCT content_id FROM comment_evidence_versions
                    WHERE content_id IN ({placeholders}) AND status='available'
                      AND captured_at>=? AND captured_at<=?
                    """,
                    [*batch, comment_cutoff, collection_cutoff_at],
                ).fetchall()
            )
    media_content_ids = [
        int(content["id"])
        for content in contents
        if content["content_type"] in {"video", "image"}
    ]
    media_states = media_terminal_states(
        connection,
        str(release["id"]),
        media_content_ids,
    )
    media_terminal_ids = {
        content_id
        for content_id, state in media_states.items()
        if state in {"complete", "terminal_insufficient", "terminal_failed"}
    }
    media_terminal_coverage = (
        _percentage(len(media_terminal_ids), len(media_content_ids))
        if media_content_ids
        else 100.0
    )
    weekly_comment_coverage = (
        _percentage(len(recent_comment_ids), total)
        if task["task_type"] == "weekly" and total
        else 100.0
    )
    duplicate_status, duplicate_coverage, duplicate_reason = (
        duplicate_metric_decision(
            total,
            fingerprint_ready,
            duplicate_calibration_ready,
            threshold=fingerprint_minimum,
        )
    )
    evaluation_coverage = (
        round(eval_ready * 100 / total, 2) if total else 100.0
    )
    data_quality: Dict[str, Any] = {
        "discovery_coverage": discovery_detail["percentage"],
        "detail_coverage": _percentage(detail_ready, total) if total else 100.0,
        "metrics_freshness": freshness_detail["percentage"],
        "evaluation_coverage": evaluation_coverage,
        "core_artifact_coverage": 100.0,
        "media_terminal_coverage": media_terminal_coverage,
        "duplicate_fingerprint_coverage": (
            duplicate_coverage if duplicate_coverage is not None else 100.0
        ),
        "duplicate_calibration_ready": duplicate_calibration_ready,
        "weekly_comment_coverage": weekly_comment_coverage,
    }
    task_status = expected_terminal_task_status(
        data_quality,
        data_quality_details={
            "discovery_coverage": discovery_detail,
            "metrics_freshness": freshness_detail,
            "pipeline_observation": pipeline_observation,
        },
        enforce_boolean_quality_gates=bool(total),
    )
    view_values = [
        int(value["view_count"])
        for value in snapshots.values()
        if value["view_count"] is not None
    ]
    comment_values = [
        int(value["comment_count"])
        for value in snapshots.values()
        if value["comment_count"] is not None
    ]
    view_metric_coverage = _percentage(len(view_values), total)
    all_historical = bool(snapshots) and all(
        str(value["source"]).startswith("migrated_") for value in snapshots.values()
    )
    view_status = (
        "not_applicable"
        if total == 0
        else "missing"
        if not view_values
        else "stale"
        if all_historical
        else "available"
        if (view_metric_coverage or 0) >= view_minimum
        else "below_threshold"
    )
    comment_status = (
        "not_applicable"
        if total == 0
        else "missing"
        if not comment_values
        else "available"
        if len(comment_values) * 100 / total >= comment_minimum
        else "below_threshold"
    )
    eval_status = (
        "not_applicable"
        if total == 0
        else "available"
        if evaluation_coverage >= evaluation_minimum
        else "below_threshold"
    )
    evaluation_reason = (
        "卖点评估完成率为 "
        f"{evaluation_coverage:.2f}%，低于至少 {evaluation_minimum:g}% 的要求"
        if eval_status == "below_threshold"
        else ""
    )
    view_reason = (
        "曝光量来自之前保存的数据，不能当作当前实时数据"
        if all_historical
        else f"有曝光量的数据占 {view_metric_coverage:.2f}%，低于至少 "
        f"{view_minimum:g}% 的要求"
        if view_status == "below_threshold" and view_metric_coverage is not None
        else ""
    )
    comment_metric_coverage = _percentage(len(comment_values), total)
    comment_reason = (
        "现有数据只有互动人数，没有评论总数"
        if comment_status == "missing"
        else f"有评论数的数据占 {comment_metric_coverage:.2f}%，低于至少 "
        f"{comment_minimum:g}% 的要求"
        if comment_status == "below_threshold" and comment_metric_coverage is not None
        else ""
    )
    point_counts = Counter(
        str(value["primary_selling_point_code"])
        for value in included_evaluations.values()
        if value["primary_selling_point_code"]
    )
    duplicate_rows = connection.execute(
        """
        SELECT d.*, duplicate.link_id duplicate_link_id, original.link_id original_link_id
        FROM duplicate_relations d
        JOIN content_items duplicate ON duplicate.id=d.duplicate_content_id
        JOIN content_items original ON original.id=d.original_content_id
        WHERE d.duplicate_content_id IN (
            SELECT content_id FROM task_contents
            WHERE task_id=? AND inclusion_status='included'
        )
          AND d.status='confirmed'
          AND d.id=(
              SELECT d2.id FROM duplicate_relations d2
              WHERE d2.duplicate_content_id=d.duplicate_content_id AND d2.status='confirmed'
              ORDER BY d2.confidence DESC,d2.id LIMIT 1
          )
        ORDER BY d.id
        """,
        (task["id"],),
    ).fetchall()
    duplicate_by_content = {
        int(row["duplicate_content_id"]): dict(row) for row in duplicate_rows
    }
    capture_rows = connection.execute(
        """
        SELECT fs.stage, fs.status, COUNT(*) count FROM fetch_slots fs
        WHERE fs.content_id IN (
            SELECT content_id FROM task_contents
            WHERE task_id=? AND inclusion_status='included'
        ) GROUP BY fs.stage, fs.status ORDER BY fs.stage, fs.status
        """,
        (task["id"],),
    ).fetchall()
    start_utc, end_utc = period_bounds(
        str(task["period_start"]), str(task["period_end"])
    )
    cost_rows = connection.execute(
        """
        SELECT provider, operation, currency, SUM(request_attempts) request_attempts,
               SUM(billed_requests) billed_requests, ROUND(SUM(amount), 6) amount
        FROM provider_usage WHERE recorded_at>=? AND recorded_at<?
        GROUP BY provider, operation, currency ORDER BY provider, operation
        """,
        (start_utc, end_utc),
    ).fetchall()
    tier_rows = connection.execute(
        "SELECT code, tier, label FROM selling_points WHERE taxonomy_id=?",
        (taxonomy["id"],),
    ).fetchall()
    tier_by_code = {str(row["code"]): row["tier"] for row in tier_rows}
    label_by_code = {str(row["code"]): str(row["label"]) for row in tier_rows}
    conclusion_rows: List[Dict[str, Any]] = []
    for content in contents:
        content_id = int(content["id"])
        evaluation = eligible_evaluations.get(content_id)
        snapshot = snapshots.get(content_id)
        conclusion_rows.append(
            {
                "content_id": content_id,
                "platform": str(content["platform"]),
                "content_direction": content["resolved_direction"],
                "evidence_level": evaluation["evidence_level"] if evaluation else None,
                "selling_point_included": (
                    bool(int(evaluation["selling_point_included"]))
                    if evaluation
                    else False
                ),
                "primary_tier": (
                    tier_by_code.get(str(evaluation["primary_selling_point_code"]))
                    if evaluation and evaluation["primary_selling_point_code"]
                    else None
                ),
                "content_automotive_score": (
                    evaluation["content_automotive_score"] if evaluation else None
                ),
                "audience_automotive_score": (
                    evaluation["audience_automotive_score"] if evaluation else None
                ),
                "acquisition_potential_score": (
                    evaluation["acquisition_potential_score"] if evaluation else None
                ),
                "view_count": snapshot["view_count"] if snapshot else None,
            }
        )
    channel_conclusions = build_channel_conclusions(
        conclusion_rows,
        audience_rates=_report_audience_rates(
            connection,
            conclusion_rows,
            window_end_utc=end_utc,
            report_cutoff_at=collection_cutoff_at,
        ),
    )
    details: List[Dict[str, Any]] = []
    for content in contents:
        content_id = int(content["id"])
        evaluation = eligible_evaluations.get(content_id)
        snapshot = snapshots.get(content_id)
        duplicate = duplicate_by_content.get(content_id)
        details.append(
            {
                "content_id": content_id,
                "platform_content_id": content["platform_content_id"],
                "link_id": content["link_id"],
                "platform": content["platform"],
                "content_type": content["content_type"],
                "published_at": content["published_at"],
                "canonical_url": content["canonical_url"],
                "title": content["title"],
                "account_uid": content["raw_account_uid"],
                "account_name": content["raw_account_name"],
                "account_type": content["account_type"],
                "content_direction": evaluation["content_direction"]
                if evaluation
                else content["resolved_direction"],
                "evidence_level": evaluation["evidence_level"] if evaluation else None,
                "primary_selling_point_code": evaluation["primary_selling_point_code"]
                if evaluation
                else None,
                "primary_selling_point_label": (
                    label_by_code.get(str(evaluation["primary_selling_point_code"]))
                    if evaluation and evaluation["primary_selling_point_code"]
                    else None
                ),
                "selling_point_score": evaluation["selling_point_score"]
                if evaluation
                else None,
                "content_automotive_score": evaluation["content_automotive_score"]
                if evaluation
                else None,
                "view_count": snapshot["view_count"] if snapshot else None,
                "comment_count": snapshot["comment_count"] if snapshot else None,
                "duplicate_original_link_id": duplicate["original_link_id"]
                if duplicate
                else None,
                "duplicate_method": duplicate["method"] if duplicate else None,
                "duplicate_confidence": duplicate["confidence"] if duplicate else None,
                "evaluation_current": evaluation is not None,
            }
        )
    return {
        "report_version": CURRENT_REPORT_VERSION,
        "rule_version": str(release["rule_version"]),
        "taxonomy_version": str(taxonomy["version"]),
        "evidence_version": CURRENT_REPORT_EVIDENCE_VERSION,
        "metadata": {
            "task_id": task["id"],
            "revision": revision,
            "generated_at": generated_at,
            "collection_cutoff_at": collection_cutoff_at,
        },
        "scope": {
            "period_start": f"{task['period_start']}T00:00:00+08:00",
            "period_end": f"{(date.fromisoformat(str(task['period_end'])) + timedelta(days=1)).isoformat()}T00:00:00+08:00",
            "timezone": "Asia/Shanghai",
        },
        "task": {
            "task_type": task["task_type"],
            "creation_source": task["creation_source"],
            "task_status": task_status,
            "name": task["name"],
        },
        "data_quality": data_quality,
        "data_quality_details": {
            "discovery_coverage": discovery_detail,
            "metrics_freshness": freshness_detail,
            "pipeline_observation": pipeline_observation,
        },
        "summary_metrics": {
            "publication_count": quantity_metric(
                total, unit="content", status="available"
            ),
            "active_account_count": quantity_metric(
                active_accounts, unit="account", status="available"
            ),
            "view_count": quantity_metric(
                sum(view_values) if view_values else None,
                unit="view",
                status=view_status,
                coverage_percentage=view_metric_coverage,
                reason=view_reason,
            ),
            "comment_count": quantity_metric(
                sum(comment_values) if comment_values else None,
                unit="comment",
                status=comment_status,
                coverage_percentage=comment_metric_coverage,
                reason=comment_reason,
            ),
            "verticality_rate": ratio_metric(
                vertical if total else None,
                total,
                status=eval_status,
                eligible_count=eval_ready,
                coverage_percentage=data_quality["evaluation_coverage"],
                reason=evaluation_reason,
            ),
            "selling_point_coverage_rate": ratio_metric(
                selling if total else None,
                total,
                status=eval_status,
                eligible_count=eval_ready,
                coverage_percentage=data_quality["evaluation_coverage"],
                reason=evaluation_reason,
            ),
            "duplicate_rate": ratio_metric(
                len(duplicate_rows) if total else None,
                total,
                status=duplicate_status,
                eligible_count=fingerprint_ready,
                coverage_percentage=duplicate_coverage,
                reason=duplicate_reason,
            ),
            "estimated_new_users": quantity_metric(
                None,
                unit="person",
                status="not_applicable" if total == 0 else "not_calculable",
                reason="系统目前没有可靠的业务预估模型",
            ),
            "estimated_reactivated_users": quantity_metric(
                None,
                unit="person",
                status="not_applicable" if total == 0 else "not_calculable",
                reason="系统目前没有可靠的业务预估模型",
            ),
            "estimated_leads": quantity_metric(
                None,
                unit="lead",
                status="not_applicable" if total == 0 else "not_calculable",
                reason="系统目前没有可靠的业务预估模型",
            ),
        },
        "channels": channel_conclusions,
        "platform_dimensions": _dimension(
            (str(row["platform"]) for row in contents), total
        ),
        "account_type_dimensions": _dimension(
            (str(row["account_type"]) for row in contents), total
        ),
        "content_direction_dimensions": _dimension(
            (str(row["resolved_direction"]) for row in contents),
            total,
        ),
        "selling_point_dimensions": [
            {"code": code, "count": count, "percentage": _percentage(count, total)}
            for code, count in sorted(point_counts.items())
        ],
        "duplicates": [dict(row) for row in duplicate_rows],
        "capture_summary": [dict(row) for row in capture_rows],
        "provider_costs": [dict(row) for row in cost_rows],
        "content_details": details,
        "files": files,
    }


_CONCLUSION_METRIC_LABELS = (
    ("selling_point_count_share", "卖点条数占比"),
    ("core_selling_point_count_share", "核心卖点条数占比"),
    ("selling_point_exposure_share", "卖点曝光占比"),
    ("core_selling_point_exposure_share", "核心卖点曝光占比"),
    ("content_verticality", "内容垂直度"),
    ("automotive_user_rate", "互动用户汽车兴趣占比"),
    ("acquisition_potential", "内容拉新效果预估"),
)
_UNPUBLISHED_STATUS_LABELS = {
    "below_threshold": "暂不显示",
    "missing": "暂无数据",
    "not_applicable": "无适用内容",
    "not_calculable": "暂时无法计算",
    "stale": "数据需要更新",
}
#: 按 reason 关键词给出更准确的短语（2026-08-07 决策：按成因显示，
#: 不再使用泛化样本提示）。关键词与 audience_rate/insights 的
#: reason 文案保持一致，改动需同步 web 端 unavailableShortLabel。
_UNPUBLISHED_REASON_LABELS = (
    ("评论还没有采集", "评论未采集"),
    ("所选时间内没有评论互动", "无评论互动"),
    ("有评论，但无法识别评论用户", "无可识别用户"),
    ("重复内容识别规则还没完成校验", "重复内容识别规则还没完成校验"),
    ("完成重复内容识别的数据占", "重复内容识别还没完成"),
    ("能够识别用户的评论占", "部分评论用户无法识别"),
    ("能识别用户身份的评论占", "部分评论用户无法识别"),
    ("完成用户分类的比例为", "用户分类未完成"),
    ("去掉重复用户后只有", "互动用户人数不足"),
    ("去重后只有", "互动用户人数不足"),
    ("已完成分类的曝光", "部分曝光还没完成分类"),
    ("用户分类结果还没完成校验", "用户分类结果还没完成校验"),
    ("系统暂时无法按用户汇总", "暂时无法按用户汇总"),
    ("没有曝光量大于 0", "暂无有效曝光"),
    ("资料足够，可以评分", "暂无可评分内容"),
    ("感知指纹尚未完成定标", "重复内容识别规则还没完成校验"),
    ("感知指纹覆盖率", "重复内容识别还没完成"),
    ("用户身份覆盖率", "部分用户身份信息不足"),
    ("用户分类覆盖率", "用户分类未完成"),
    ("低于 30 人门槛", "互动用户少于 30 人"),
    ("可归类有效曝光", "部分曝光还没完成分类"),
    ("分类器定标未通过", "用户分类结果还没完成校验"),
    ("用户级汽车兴趣占比尚未接入", "暂时无法按用户汇总"),
    ("未提供阅读数", "平台未提供阅读数"),
    ("有效曝光数据", "有效曝光待补齐"),
    ("没有一级评论互动", "无评论互动"),
    ("评论尚未采集", "评论未采集"),
    ("无可识别的用户身份", "无可识别用户"),
    ("评分证据门槛", "暂无可评分内容"),
    ("兴趣分类尚未运行", "系统还没完成用户分类"),
)
_PUBLISHED_METRIC_STATUSES = {"available", "sample_only"}


def _conclusion_cell(metric: Mapping[str, Any]) -> str:
    """Status-aware display value: never fabricates an unpublished number."""

    published = metric.get("status") in _PUBLISHED_METRIC_STATUSES
    if metric.get("kind") == "quantity" and published:
        if metric.get("value") is not None:
            return str(metric["value"])
    elif metric.get("kind") == "score" and published:
        if metric.get("value") is not None:
            value = f"{metric['value']}%"
            if metric.get("status") == "sample_only":
                return f"{value}（仅供参考）"
            return value
    elif published and metric.get("percentage") is not None:
        value = f"{metric['percentage']}%"
        if metric.get("status") == "sample_only":
            return f"{value}（仅供参考）"
        return value
    reason = str(metric.get("reason") or "")
    for needle, label in _UNPUBLISHED_REASON_LABELS:
        if needle in reason:
            return label
    return _UNPUBLISHED_STATUS_LABELS.get(str(metric.get("status")), "暂不可计算")


def _markdown(report: Mapping[str, Any]) -> str:
    metrics = report["summary_metrics"]
    period_start = str(report["scope"]["period_start"])[:10]
    period_end_exclusive = date.fromisoformat(str(report["scope"]["period_end"])[:10])
    period_end = (period_end_exclusive - timedelta(days=1)).isoformat()
    lines = [
        f"# {report['task']['name']}",
        "",
        f"- 报告版次：第 {report['metadata']['revision']} 版",
        f"- 任务状态：{_TASK_STATUS_LABELS.get(str(report['task']['task_status']), '未知')}",
        f"- 统计日期：{period_start} 至 {period_end}（包含开始和结束当天）",
        "",
        "## 概览",
        "",
        f"- 发布内容：{metrics['publication_count']['value']} 条",
        f"- 重复内容率：{_conclusion_cell(metrics['duplicate_rate'])}",
        f"- 发布账号：{metrics['active_account_count']['value']} 个",
        f"- 阅读 / 播放：{_conclusion_cell(metrics['view_count'])}",
        f"- 评论数：{_conclusion_cell(metrics['comment_count'])}",
        f"- 内容垂直度：{_conclusion_cell(metrics['verticality_rate'])}",
        f"- 卖点覆盖率：{_conclusion_cell(metrics['selling_point_coverage_rate'])}",
        "",
    ]
    channels = report.get("channels") or {}
    if channels:
        lines.extend(["## 渠道结论", ""])
        for platform, _label in CHANNELS:
            channel = channels.get(platform)
            if not channel:
                continue
            scene_groups = channel.get("scenes") or {}
            groups = [("汇总", channel.get("summary") or {})]
            for scene, _scene_label in SCENES:
                group = scene_groups.get(scene) or {}
                groups.append((str(group.get("label") or scene), group))
            lines.append(
                f"### {channel.get('label') or platform}渠道"
                f"（报告期内发布 {channel.get('publication_count', 0)} 条）"
            )
            lines.append("")
            header = " | ".join(title for title, _group in groups)
            lines.append(f"| 指标 | {header} |")
            lines.append("| --- |" + " --- |" * len(groups))
            for key, metric_label in _CONCLUSION_METRIC_LABELS:
                cells = []
                for _title, group in groups:
                    metric = (group.get("metrics") or {}).get(key) or {}
                    cells.append(_conclusion_cell(metric))
                lines.append(f"| {metric_label} | " + " | ".join(cells) + " |")
            lines.append("")
        lines.extend(
            [
                "同一互动用户只计算一次。样本太少时不显示比例；"
                "详细人数和覆盖率可在渠道结论表中查看。",
                "",
            ]
        )
    lines.extend(["## 数据完整度", ""])
    quality_details = report.get("data_quality_details")
    freshness_detail = (
        quality_details.get("metrics_freshness")
        if isinstance(quality_details, Mapping)
        else None
    )
    discovery_detail = (
        quality_details.get("discovery_coverage")
        if isinstance(quality_details, Mapping)
        else None
    )
    pipeline_observation = (
        quality_details.get("pipeline_observation")
        if isinstance(quality_details, Mapping)
        else None
    )
    for key, value in report["data_quality"].items():
        if key == "discovery_coverage" and isinstance(discovery_detail, Mapping):
            if discovery_detail.get("status") == "not_applicable":
                lines.append(f"- {_QUALITY_GATE_LABELS.get(key, '其他数据检查')}: 无适用内容")
            else:
                lines.append(
                    f"- {_QUALITY_GATE_LABELS.get(key, '其他数据检查')}: 账号采集 "
                    f"{discovery_detail.get('covered_identity_occurrence_count', 0)}/"
                    f"{discovery_detail.get('eligible_identity_occurrence_count', 0)}"
                    f" · 每日采集 "
                    f"{discovery_detail.get('observed_occurrence_count', 0)}/"
                    f"{discovery_detail.get('expected_occurrence_count', 0)}"
                    f" · {value}%"
                )
        elif key == "metrics_freshness" and isinstance(freshness_detail, Mapping):
            if freshness_detail.get("status") == "not_applicable":
                lines.append(f"- {_QUALITY_GATE_LABELS.get(key, '其他数据检查')}: 无适用内容")
            else:
                lines.append(
                    f"- {_QUALITY_GATE_LABELS.get(key, '其他数据检查')}: {freshness_detail.get('fresh_count', 0)}/"
                    f"{freshness_detail.get('eligible_count', 0)} · {value}%"
                )
        elif isinstance(value, bool):
            lines.append(
                f"- {_QUALITY_GATE_LABELS.get(key, '其他数据检查')}: "
                f"{'通过' if value else '未通过'}"
            )
        elif value is None:
            lines.append(f"- {_QUALITY_GATE_LABELS.get(key, '其他数据检查')}: 无适用内容")
        else:
            lines.append(f"- {_QUALITY_GATE_LABELS.get(key, '其他数据检查')}: {value}%")
    if isinstance(pipeline_observation, Mapping):
        legacy_dates = list(pipeline_observation.get("legacy_unobserved_dates") or [])
        gap_dates = list(pipeline_observation.get("pipeline_gap_dates") or [])
        if legacy_dates:
            lines.append("- 自动抓取启用前未观测日期：" + "、".join(map(str, legacy_dates)))
        if gap_dates:
            lines.append("- 自动抓取流程缺口日期：" + "、".join(map(str, gap_dates)))
        if not legacy_dates and not gap_dates:
            lines.append("- 每日抓取观测完整度：通过")
    lines.extend(
        ["", "系统目前没有可靠的业务预估模型，因此不会生成推测值。", ""]
    )
    return "\n".join(lines)


_SVG_PLATFORM_LABELS = {
    "douyin": "抖音",
    "xiaohongshu": "小红书",
    "wechat_channels": "视频号",
    "kuaishou": "快手",
}
_SVG_ACCOUNT_TYPE_LABELS = {
    "mixed_edit": "混剪",
    "original": "原创",
    "boutique_ip": "精品 IP",
    "unknown": "未识别",
}
_SVG_DIRECTION_LABELS = {
    "unknown": "待补齐",
    "new_car": "新车",
    "media": "媒体",
    "used_car": "二手车",
    "other": "其他",
}
_SVG_TASK_STATUS_LABELS = {
    "succeeded": "已完成",
    "partial": "部分完成",
}


def _svg_float(value: Any, *, default: float = 0.0) -> float:
    if isinstance(value, bool):
        return default
    try:
        return float(value)
    except (TypeError, ValueError):
        return default


def _svg_percentage(value: Any) -> float:
    return max(0.0, min(100.0, _svg_float(value)))


def _svg_dimensions(report: Mapping[str, Any], key: str) -> Dict[str, Dict[str, Any]]:
    rows = report.get(key)
    if not isinstance(rows, Sequence) or isinstance(rows, (str, bytes)):
        return {}
    result: Dict[str, Dict[str, Any]] = {}
    for item in rows:
        if not isinstance(item, Mapping):
            continue
        item_key = str(item.get("key") or "unknown")
        result[item_key] = {
            "count": max(0, int(_svg_float(item.get("count")))),
            "percentage": _svg_percentage(item.get("percentage")),
        }
    return result


def _svg_display_text(value: Any, *, max_units: int) -> str:
    text = "".join(
        character
        for character in str(value or "")
        if character in {"\t", "\n", "\r"}
        or "\u0020" <= character <= "\ud7ff"
        or "\ue000" <= character <= "\ufffd"
        or "\U00010000" <= character <= "\U0010ffff"
    ).strip()
    result: List[str] = []
    units = 0
    for character in text:
        character_units = (
            2 if unicodedata.east_asian_width(character) in {"W", "F"} else 1
        )
        if units + character_units > max_units:
            return html.escape("".join(result).rstrip() + "…")
        result.append(character)
        units += character_units
    return html.escape("".join(result))


def _svg_count_font_size(
    value: int, *, large: int, medium: int, small: int
) -> int:
    length = len(f"{value:,}")
    if length <= 7:
        return large
    if length <= 10:
        return medium
    return small


def _svg_period(report: Mapping[str, Any]) -> tuple[str, str, int]:
    scope = report.get("scope")
    if not isinstance(scope, Mapping):
        return "统计周期未声明", "截止时间未声明", 0
    try:
        start = date.fromisoformat(str(scope.get("period_start"))[:10])
        end_exclusive = date.fromisoformat(str(scope.get("period_end"))[:10])
        end_inclusive = end_exclusive - timedelta(days=1)
        days = max(0, (end_exclusive - start).days)
        if start.year == end_inclusive.year:
            range_label = (
                f"{start:%Y.%m.%d}—{end_inclusive:%m.%d}"
            )
        else:
            range_label = f"{start:%Y.%m.%d}—{end_inclusive:%Y.%m.%d}"
        cutoff_label = f"{end_exclusive:%Y-%m-%d} 00:00"
        metadata = report.get("metadata")
        raw_cutoff = (
            metadata.get("collection_cutoff_at")
            if isinstance(metadata, Mapping)
            else None
        )
        if isinstance(raw_cutoff, str) and raw_cutoff:
            try:
                cutoff = datetime.fromisoformat(raw_cutoff.replace("Z", "+00:00"))
                if cutoff.tzinfo is not None:
                    timezone_name = str(scope.get("timezone") or "Asia/Shanghai")
                    try:
                        display_timezone = ZoneInfo(timezone_name)
                    except (KeyError, ValueError):
                        display_timezone = SHANGHAI
                    cutoff_label = cutoff.astimezone(display_timezone).strftime(
                        "%Y-%m-%d %H:%M"
                    )
            except ValueError:
                pass
        return range_label, cutoff_label, days
    except (TypeError, ValueError):
        return "统计周期未声明", "截止时间未声明", 0


def _svg_quality_thresholds(report: Mapping[str, Any]) -> Dict[str, float]:
    contract = load_contract(report_version=str(report.get("report_version") or ""))
    thresholds = {
        str(key): float(value)
        for key, value in contract.get("required_coverage_thresholds", {}).items()
    }
    for key, specification in contract.get("required_quality_details", {}).items():
        if isinstance(specification, Mapping):
            thresholds[str(key)] = float(specification["minimum_percentage"])
    return thresholds


def render_summary_svg(report: Mapping[str, Any]) -> str:
    """Render the share image as a self-contained channel insight summary.

    Structural dimensions are safe to show for a partial report. Business-effect
    metrics are deliberately excluded: the task status, not the handful of
    quality values visible in this compact image, remains the publication truth.
    """

    metrics = report.get("summary_metrics")
    metrics = metrics if isinstance(metrics, Mapping) else {}
    publication_metric = metrics.get("publication_count")
    account_metric = metrics.get("active_account_count")
    publication = max(
        0,
        int(
            _svg_float(
                publication_metric.get("value")
                if isinstance(publication_metric, Mapping)
                else 0
            )
        ),
    )
    active_accounts = max(
        0,
        int(
            _svg_float(
                account_metric.get("value")
                if isinstance(account_metric, Mapping)
                else 0
            )
        ),
    )

    task = report.get("task")
    task = task if isinstance(task, Mapping) else {}
    metadata = report.get("metadata")
    metadata = metadata if isinstance(metadata, Mapping) else {}
    title = _svg_display_text(task.get("name") or "自定义报告", max_units=54)
    raw_status = str(task.get("task_status") or "")
    status = _SVG_TASK_STATUS_LABELS.get(raw_status, "报告已生成")
    revision = max(1, int(_svg_float(metadata.get("revision"), default=1)))

    platforms = _svg_dimensions(report, "platform_dimensions")
    platform_rows = sorted(
        platforms.items(), key=lambda item: item[1]["percentage"], reverse=True
    )
    if platform_rows:
        major_key, major = platform_rows[0]
    else:
        major_key, major = "unknown", {"count": 0, "percentage": 0.0}
    if len(platform_rows) == 2:
        minor_key, minor = platform_rows[1]
    elif len(platform_rows) > 2:
        minor_key = "other"
        minor = {
            "count": sum(item["count"] for _, item in platform_rows[1:]),
            "percentage": sum(
                item["percentage"] for _, item in platform_rows[1:]
            ),
        }
    else:
        minor_key, minor = "other", {"count": 0, "percentage": 0.0}
    major_label = _SVG_PLATFORM_LABELS.get(major_key, major_key)
    minor_label = _SVG_PLATFORM_LABELS.get(minor_key, "其他平台")
    major_percentage = _svg_percentage(major["percentage"])
    minor_percentage = _svg_percentage(minor["percentage"])

    account_types = _svg_dimensions(report, "account_type_dimensions")
    account_order = ("mixed_edit", "original", "boutique_ip", "unknown")
    account_colors = {
        "mixed_edit": "#2db8ad",
        "original": "#ffcd32",
        "boutique_ip": "#8ddcd5",
        "unknown": "#84969b",
    }
    account_segments: List[str] = []
    account_legend: List[str] = []
    segment_x = 415.0
    for index, key in enumerate(account_order):
        value = account_types.get(key, {"count": 0, "percentage": 0.0})
        percentage = _svg_percentage(value["percentage"])
        width = 330 * percentage / 100
        if width > 0:
            account_segments.append(
                f'<rect x="{segment_x:.2f}" y="236" width="{width:.2f}" '
                f'height="42" fill="{account_colors[key]}"/>'
            )
            segment_x += width
        y = 326 + index * 56
        account_legend.append(
            f'<circle cx="428" cy="{y - 6}" r="7" fill="{account_colors[key]}"/>'
            f'<text class="t" x="446" y="{y}" font-size="17">'
            f'{_SVG_ACCOUNT_TYPE_LABELS[key]}</text>'
            f'<text class="num" x="738" y="{y}" font-size="19" '
            f'text-anchor="end" fill="{account_colors[key]}">{percentage:.2f}%</text>'
        )

    directions = _svg_dimensions(report, "content_direction_dimensions")
    direction_order = tuple(
        key
        for key in ("unknown", "new_car", "media", "used_car", "other")
        if _svg_percentage(directions.get(key, {}).get("percentage")) > 0
    )
    direction_rows: List[str] = []
    direction_colors = {
        "unknown": "#ffcd32",
        "new_car": "#2db8ad",
        "media": "#58c9c0",
        "used_car": "#8ddcd5",
        "other": "#84969b",
    }
    for index, key in enumerate(direction_order[:5]):
        percentage = _svg_percentage(directions[key]["percentage"])
        y = 238 + index * 43
        color = direction_colors[key]
        bar_width = 210 * percentage / 100
        bar_radius = min(7.0, bar_width / 2)
        direction_rows.append(
            f'<text class="font" x="790" y="{y}" font-size="16" '
            f'fill="{color}">{_SVG_DIRECTION_LABELS[key]}</text>'
            f'<rect x="875" y="{y - 14}" width="210" height="14" rx="7" '
            f'fill="#294a53"/>'
            f'<rect x="875" y="{y - 14}" width="{bar_width:.2f}" '
            f'height="14" rx="{bar_radius:.2f}" fill="{color}"/>'
            f'<text class="num" x="1158" y="{y}" font-size="17" '
            f'text-anchor="end" fill="{color}">{percentage:.2f}%</text>'
        )
    if not direction_rows:
        direction_rows.append(
            '<text class="m" x="790" y="250" font-size="16">暂无内容方向数据</text>'
        )

    quality = report.get("data_quality")
    quality = quality if isinstance(quality, Mapping) else {}
    thresholds = _svg_quality_thresholds(report)
    quality_items = (
        ("discovery_coverage", "账号采集"),
        ("metrics_freshness", "数据更新"),
        ("evaluation_coverage", "卖点评估"),
        ("detail_coverage", "详情采集"),
        ("media_terminal_coverage", "媒体处理"),
        ("duplicate_fingerprint_coverage", "重复识别"),
    )
    quality_cells: List[str] = []
    visible_thresholds: List[float] = []
    for index, (key, label) in enumerate(quality_items):
        row, column = divmod(index, 3)
        x = 790 + column * 123
        label_y = 514 + row * 46
        value_y = 537 + row * 46
        raw_value = quality.get(key)
        has_value = isinstance(raw_value, (int, float)) and not isinstance(
            raw_value, bool
        )
        quality_value = _svg_percentage(raw_value) if has_value else 0.0
        threshold = thresholds.get(key)
        if threshold is not None:
            visible_thresholds.append(threshold)
        passed = (
            has_value
            and threshold is not None
            and quality_value >= threshold
        )
        color = "#2db8ad" if passed else "#ffcd32" if has_value else "#84969b"
        value_label = f"{quality_value:.2f}%" if has_value else "—"
        quality_cells.append(
            f'<text class="m" x="{x + 61}" y="{label_y}" font-size="12" '
            f'text-anchor="middle">{label}</text>'
            f'<text class="num" x="{x + 61}" y="{value_y}" font-size="18" '
            f'text-anchor="middle" fill="{color}">{value_label}</text>'
        )
    one_threshold = (
        visible_thresholds
        and len({round(value, 4) for value in visible_thresholds}) == 1
    )
    threshold_label = (
        f"各项要求至少 {visible_thresholds[0]:g}%"
        if one_threshold
        else "数据完整度按报告规则检查"
    )

    unknown_percentage = _svg_percentage(
        directions.get("unknown", {}).get("percentage")
    )
    new_car_percentage = _svg_percentage(
        directions.get("new_car", {}).get("percentage")
    )
    if publication:
        insight_parts = [
            (
                f"内容高度集中于{major_label}"
                if major_percentage >= 80
                else f"最大渠道为{major_label}（{major_percentage:.2f}%）"
            )
        ]
        if 35 <= new_car_percentage <= 45:
            insight_parts.append("新车内容约四成")
        elif new_car_percentage > 0:
            insight_parts.append(f"新车内容占 {new_car_percentage:.2f}%")
        if unknown_percentage > 50:
            insight_parts.append("超过一半内容方向待补齐")
        elif unknown_percentage > 0:
            insight_parts.append(f"{unknown_percentage:.2f}% 内容方向待补齐")
        insight = "已纳入内容中，" + "；".join(insight_parts)
    else:
        insight = "报告期内没有发布内容，当前无法分析渠道和内容分布"
    insight = _svg_display_text(insight, max_units=104)
    period_label, cutoff_label, period_days = _svg_period(report)
    timezone_value = str(
        (report.get("scope") or {}).get("timezone") or "Asia/Shanghai"
    )
    timezone_label = "北京时间" if timezone_value == "Asia/Shanghai" else "当地时间"
    contract = load_contract(report_version=str(report.get("report_version") or ""))
    failures = quality_gate_failures(
        quality,
        data_quality_details=(
            report.get("data_quality_details")
            if isinstance(report.get("data_quality_details"), Mapping)
            else None
        ),
        contract=contract,
        enforce_boolean_quality_gates=bool(publication),
    )
    estimated_metrics = tuple(
        metrics.get(key)
        for key in (
            "estimated_new_users",
            "estimated_reactivated_users",
            "estimated_leads",
        )
    )
    estimated_statuses = {
        str(metric.get("status"))
        for metric in estimated_metrics
        if isinstance(metric, Mapping)
    }
    if estimated_statuses and estimated_statuses <= _PUBLISHED_METRIC_STATUSES:
        effect_boundary = "拉新、拉活和线索数据可以显示"
    elif "not_calculable" in estimated_statuses:
        effect_boundary = "拉新、拉活和线索暂时无法计算"
    else:
        effect_boundary = "拉新、拉活和线索暂不显示"
    quality_boundary = (
        f"{len(failures)} 项数据未达到要求"
        if failures
        else "数据检查已通过"
    )
    publication_card_size = _svg_count_font_size(
        publication, large=29, medium=24, small=19
    )
    publication_inline_size = _svg_count_font_size(
        publication, large=19, medium=16, small=13
    )
    publication_donut_size = _svg_count_font_size(
        publication, large=18, medium=15, small=12
    )
    account_card_size = _svg_count_font_size(
        active_accounts, large=29, medium=24, small=19
    )

    return f"""<svg xmlns="http://www.w3.org/2000/svg" width="1200" height="675" viewBox="0 0 1200 675">
<rect width="1200" height="675" fill="#102c35"/>
<style>
.font{{font-family:Arial,'PingFang SC','Microsoft YaHei',sans-serif}}
.t{{font-family:Arial,'PingFang SC','Microsoft YaHei',sans-serif;fill:#f7f8f5}}
.m{{font-family:Arial,'PingFang SC','Microsoft YaHei',sans-serif;fill:#9eb0b5}}
.num{{font-family:Inter,Arial,'PingFang SC','Microsoft YaHei',sans-serif;font-weight:700}}
</style>
<rect x="40" y="32" width="6" height="70" rx="3" fill="#ffcd32"/>
<text class="t" x="62" y="52" font-size="17">DCar Insight · 渠道与内容结构</text>
<text class="t" x="62" y="96" font-size="30" font-weight="700">{title}</text>
<rect x="1018" y="30" width="142" height="36" rx="18" fill="#ffcd32"/>
<text x="1089" y="54" font-family="Arial,'PingFang SC','Microsoft YaHei',sans-serif" font-size="16" font-weight="700" text-anchor="middle" fill="#13262d">{status}</text>
<text class="m" x="1158" y="91" font-size="13" text-anchor="end">第 {revision} 版</text>
<text class="t" x="40" y="144" font-size="20" font-weight="600">{insight}</text>
<line x1="40" y1="166" x2="1160" y2="166" stroke="#ffcd32" stroke-width="1.5"/>

<text class="t" x="40" y="204" font-size="18" font-weight="700">平台结构</text>
<text class="num" x="40" y="276" font-size="54" fill="#f7f8f5">{major_percentage:.2f}%</text>
<text class="t" x="42" y="311" font-size="25" font-weight="700">{html.escape(major_label)}</text>
<text x="42" y="340" font-family="Arial,'PingFang SC','Microsoft YaHei',sans-serif" font-size="{publication_inline_size}" font-weight="700" fill="#2db8ad">{major['count']:,} 条</text>
<circle cx="287" cy="280" r="60" fill="none" stroke="#294a53" stroke-width="18"/>
<circle cx="287" cy="280" r="60" fill="none" stroke="#2db8ad" stroke-width="18" pathLength="100" stroke-dasharray="{major_percentage:.2f} {100 - major_percentage:.2f}" stroke-linecap="butt" transform="rotate(-90 287 280)"/>
<text class="m" x="287" y="276" font-size="12" text-anchor="middle">已纳入内容</text>
<text class="num" x="287" y="300" font-size="{publication_donut_size}" text-anchor="middle" fill="#f7f8f5">{publication:,}</text>
<line x1="40" y1="372" x2="360" y2="372" stroke="#31505a"/>
<text class="num" x="42" y="405" font-size="24" fill="#f7f8f5">{minor_percentage:.2f}%</text>
<text class="t" x="130" y="405" font-size="17">{html.escape(minor_label)}</text>
<text class="m" x="358" y="405" font-size="15" text-anchor="end">{minor['count']:,} 条</text>
<rect x="40" y="442" width="148" height="76" rx="13" fill="#173b45"/>
<text class="num" x="58" y="482" font-size="{publication_card_size}" fill="#f7f8f5">{publication:,}</text>
<text class="m" x="58" y="505" font-size="13">发布内容</text>
<rect x="206" y="442" width="154" height="76" rx="13" fill="#173b45"/>
<text class="num" x="224" y="482" font-size="{account_card_size}" fill="#f7f8f5">{active_accounts:,}</text>
<text class="m" x="224" y="505" font-size="13">发布账号</text>
<text class="m" x="40" y="552" font-size="12">结构分布仅描述本报告已纳入内容</text>

<line x1="390" y1="190" x2="390" y2="594" stroke="#31505a"/>
<text class="t" x="415" y="204" font-size="18" font-weight="700">账号类型构成</text>
<rect x="415" y="236" width="330" height="42" rx="8" fill="#294a53"/>
<clipPath id="account-stack"><rect x="415" y="236" width="330" height="42" rx="8"/></clipPath>
<g clip-path="url(#account-stack)">{''.join(account_segments)}</g>
{''.join(account_legend)}
<text class="m" x="415" y="552" font-size="12">按发布内容统计账号类型占比</text>

<line x1="765" y1="190" x2="765" y2="594" stroke="#31505a"/>
<text class="t" x="790" y="204" font-size="18" font-weight="700">内容方向</text>
{''.join(direction_rows)}
<line x1="790" y1="458" x2="1160" y2="458" stroke="#31505a"/>
<text class="t" x="790" y="480" font-size="17" font-weight="700">数据完整度 · {threshold_label}</text>
<text x="1158" y="480" font-family="Arial,'PingFang SC','Microsoft YaHei',sans-serif" font-size="11" text-anchor="end" fill="#ffcd32">{quality_boundary}</text>
<rect x="790" y="492" width="370" height="101" rx="12" fill="#173b45" stroke="#31505a"/>
{''.join(quality_cells)}
<text class="m" x="975" y="610" font-size="11" text-anchor="middle">{effect_boundary}</text>

<line x1="40" y1="620" x2="1160" y2="620" stroke="#31505a"/>
<text class="m" x="40" y="651" font-size="13">报告周期 {period_days} 天 · {period_label}</text>
<text class="m" x="1160" y="651" font-size="13" text-anchor="end">数据统计至 {cutoff_label} · {timezone_label}</text>
</svg>
"""


def _write_csv(path: Path, rows: Sequence[Mapping[str, Any]]) -> None:
    fields = [
        "content_id",
        "platform_content_id",
        "link_id",
        "platform",
        "content_type",
        "published_at",
        "canonical_url",
        "title",
        "account_uid",
        "account_name",
        "account_type",
        "content_direction",
        "evidence_level",
        "primary_selling_point_code",
        "primary_selling_point_label",
        "selling_point_score",
        "content_automotive_score",
        "view_count",
        "comment_count",
        "duplicate_original_link_id",
        "duplicate_method",
        "duplicate_confidence",
        "evaluation_current",
    ]
    with path.open("w", encoding="utf-8-sig", newline="") as handle:
        writer = csv.DictWriter(handle, fieldnames=fields)
        writer.writeheader()
        writer.writerows(
            {
                field: formula_safe_csv_value(value)
                for field, value in row.items()
            }
            for row in rows
        )


def _write_channel_csv(path: Path, channels: Mapping[str, Any]) -> None:
    """Flatten channel/scene conclusions into one row per slice metric."""

    fields = [
        "platform",
        "platform_label",
        "scope",
        "scope_label",
        "publication_count",
        "metric",
        "metric_label",
        "kind",
        "status",
        "value",
        "numerator",
        "denominator",
        "percentage",
        "eligible_count",
        "coverage_percentage",
        "reason",
        "identity_coverage_percentage",
        "candidate_user_count",
        "classified_user_count",
        "classification_coverage_percentage",
        "comment_collection_coverage_percentage",
        "captured_comment_count",
        "declared_comment_count",
        "capped_content_count",
        "audience_definition_version",
        "classifier_version",
        "user_key_version",
        "evidence_window_start",
        "evidence_window_end",
        "report_cutoff_at",
        "warm_up",
    ]
    rows: List[Dict[str, Any]] = []
    for platform, _label in CHANNELS:
        channel = channels.get(platform) or {}
        scene_groups = channel.get("scenes") or {}
        groups = [("summary", channel.get("summary") or {})]
        for scene, _scene_label in SCENES:
            groups.append((scene, scene_groups.get(scene) or {}))
        for scope, group in groups:
            quality = group.get("audience_quality") or {}
            for key, metric_label in _CONCLUSION_METRIC_LABELS:
                metric = (group.get("metrics") or {}).get(key) or {}
                audience = key == "automotive_user_rate" and bool(quality)
                rows.append(
                    {
                        "platform": platform,
                        "platform_label": channel.get("label") or platform,
                        "scope": scope,
                        "scope_label": group.get("label") or scope,
                        "publication_count": group.get("publication_count"),
                        "metric": key,
                        "metric_label": metric_label,
                        "kind": metric.get("kind"),
                        "status": metric.get("status"),
                        "value": metric.get("value"),
                        "numerator": metric.get("numerator"),
                        "denominator": metric.get("denominator"),
                        "percentage": metric.get("percentage"),
                        "eligible_count": metric.get("eligible_count"),
                        "coverage_percentage": metric.get("coverage_percentage"),
                        "reason": metric.get("reason"),
                        "identity_coverage_percentage": (
                            quality.get("identity_coverage_percentage")
                            if audience
                            else None
                        ),
                        "candidate_user_count": (
                            quality.get("candidate_user_count") if audience else None
                        ),
                        "classified_user_count": (
                            quality.get("classified_user_count") if audience else None
                        ),
                        "classification_coverage_percentage": (
                            quality.get("classification_coverage_percentage")
                            if audience
                            else None
                        ),
                        "comment_collection_coverage_percentage": (
                            quality.get("comment_collection_coverage_percentage")
                            if audience
                            else None
                        ),
                        "captured_comment_count": (
                            quality.get("captured_comment_count") if audience else None
                        ),
                        "declared_comment_count": (
                            quality.get("declared_comment_count") if audience else None
                        ),
                        "capped_content_count": (
                            quality.get("capped_content_count") if audience else None
                        ),
                        "audience_definition_version": (
                            quality.get("audience_definition_version")
                            if audience
                            else None
                        ),
                        "classifier_version": (
                            quality.get("classifier_version") if audience else None
                        ),
                        "user_key_version": (
                            quality.get("user_key_version") if audience else None
                        ),
                        "evidence_window_start": (
                            quality.get("evidence_window_start") if audience else None
                        ),
                        "evidence_window_end": (
                            quality.get("evidence_window_end") if audience else None
                        ),
                        "report_cutoff_at": (
                            quality.get("report_cutoff_at") if audience else None
                        ),
                        "warm_up": quality.get("warm_up") if audience else None,
                    }
                )
    with path.open("w", encoding="utf-8-sig", newline="") as handle:
        writer = csv.DictWriter(handle, fieldnames=fields)
        writer.writeheader()
        writer.writerows(rows)


def _valid_summary_png(path: Path) -> bool:
    """Reject square Quick Look thumbnails that crop the 1200x675 report SVG."""

    try:
        payload = path.read_bytes()
    except OSError:
        return False
    header = payload[:24]
    return (
        len(header) == 24
        and header[:8] == b"\x89PNG\r\n\x1a\n"
        and int.from_bytes(header[16:20], "big") == 1200
        and int.from_bytes(header[20:24], "big") == 675
        and len(payload) > 1024
    )


def render_summary_png(svg_path: Path, png_path: Path) -> bool:
    """Render the report SVG without changing its aspect ratio.

    macOS ``qlmanage -t`` produces a square thumbnail and crops this report.
    Prefer real SVG renderers; if none is installed the caller keeps the SVG as
    the lossless image artifact instead of publishing a malformed PNG.
    """

    commands = []
    if renderer := shutil.which("sips"):
        commands.append(
            [renderer, "-s", "format", "png", str(svg_path), "--out", str(png_path)]
        )
    if renderer := shutil.which("rsvg-convert"):
        commands.append([renderer, "-o", str(png_path), str(svg_path)])
    if renderer := shutil.which("magick"):
        commands.append([renderer, str(svg_path), str(png_path)])
    if renderer := shutil.which("convert"):
        commands.append([renderer, str(svg_path), str(png_path)])

    for command in commands:
        png_path.unlink(missing_ok=True)
        try:
            result = subprocess.run(
                command,
                capture_output=True,
                timeout=30,
                check=False,
            )
        except (OSError, subprocess.TimeoutExpired):
            continue
        if result.returncode == 0 and _valid_summary_png(png_path):
            return True
    png_path.unlink(missing_ok=True)
    return False


def _sha256(path: Path) -> str:
    return hashlib.sha256(path.read_bytes()).hexdigest()


def _relative(path: Path) -> str:
    project_root = PROJECT_ROOT.resolve()
    resolved = path.resolve()
    try:
        return str(resolved.relative_to(project_root))
    except ValueError as exc:
        raise ReportTaskError(
            f"report artifact must stay within PROJECT_ROOT: {path}"
        ) from exc


def _resolve_reports_root(reports_root: Path) -> Path:
    project_root = PROJECT_ROOT.resolve()
    candidate = (
        reports_root if reports_root.is_absolute() else project_root / reports_root
    ).resolve()
    try:
        relative = candidate.relative_to(project_root)
    except ValueError as exc:
        raise ReportTaskError(
            f"reports_root must stay within PROJECT_ROOT: {reports_root}"
        ) from exc
    if not relative.parts:
        raise ReportTaskError("reports_root must be a directory below PROJECT_ROOT")
    return candidate


def _record_task_failure(
    task_id: str,
    exc: Exception,
    *,
    db_path: Path,
) -> None:
    failed_at = now_utc()
    message = "报告生成失败，请重新生成；如果仍然失败，请联系管理员。"
    with connect(db_path) as connection, transaction(connection):
        task = connection.execute(
            "SELECT 1 FROM report_tasks WHERE id=?", (task_id,)
        ).fetchone()
        if task is None:
            return
        connection.execute(
            """
            UPDATE report_tasks SET task_status='failed', message=?, completed_at=?, updated_at=?
            WHERE id=?
            """,
            (message, failed_at, failed_at, task_id),
        )
        connection.execute(
            """
            INSERT INTO task_events(task_id, event_type, message, payload_json, created_at)
            VALUES (?, 'failed', ?, '{}', ?)
            """,
            (task_id, message, failed_at),
        )


def advance_task_progress(
    task_id: str,
    *,
    progress: int,
    message: str,
    db_path: Path = DEFAULT_DB,
) -> None:
    """Publish a coarse run stage so a polling client can render live progress.

    Only a running task is touched: a cancel request or a terminal status must
    never be overwritten by a stage update that raced with it.
    """

    updated_at = now_utc()
    with connect(db_path) as connection, transaction(connection):
        connection.execute(
            """
            UPDATE report_tasks SET progress=?, message=?, updated_at=?
            WHERE id=? AND task_status='running'
            """,
            (max(0, min(100, int(progress))), message, updated_at, task_id),
        )


def run_task(
    task_id: str,
    *,
    db_path: Path = DEFAULT_DB,
    reports_root: Path = REPORTS_ROOT,
) -> Dict[str, Any]:
    release_value: Dict[str, Any]
    with connect(db_path) as connection, transaction(connection):
        task = connection.execute(
            "SELECT * FROM report_tasks WHERE id=?", (task_id,)
        ).fetchone()
        if task is None:
            raise ReportTaskError(f"task does not exist: {task_id}")
        if task["task_status"] not in RUNNABLE_STATUSES:
            raise ReportTaskError(
                f"task {task_id} is not runnable from {task['task_status']}"
            )
        release_value = assert_report_runtime_ready(connection)
    temporary: Optional[Path] = None
    target: Optional[Path] = None
    target_created_by_run = False
    owns_run = False
    try:
        with connect(db_path) as connection, transaction(connection):
            task = connection.execute(
                "SELECT * FROM report_tasks WHERE id=?", (task_id,)
            ).fetchone()
            if task is None:
                raise ReportTaskError(f"task does not exist: {task_id}")
            if task["task_status"] not in RUNNABLE_STATUSES:
                raise ReportTaskError(
                    f"task {task_id} is not runnable from {task['task_status']}"
                )
            owns_run = True
            _snapshot_task_contents(connection, task)
            revision = int(
                connection.execute(
                    "SELECT COALESCE(MAX(revision), 0)+1 FROM report_revisions WHERE task_id=?",
                    (task_id,),
                ).fetchone()[0]
            )
            started_at = now_utc()
            connection.execute(
                """
                UPDATE report_tasks SET task_status='running', progress=10,
                    message='正在整理报告日期内的内容', started_at=?, completed_at=NULL, updated_at=?
                WHERE id=?
                """,
                (started_at, started_at, task_id),
            )
            connection.execute(
                """
                INSERT INTO task_events(task_id, event_type, message, payload_json, created_at)
                VALUES (?, 'started', '开始生成报告', ?, ?)
                """,
                (task_id, json.dumps({"revision": revision}), started_at),
            )
            task_value = dict(task)

        resolved_reports_root = _resolve_reports_root(reports_root)
        target = (
            resolved_reports_root / task_id / f"revision_{revision:03d}"
        ).resolve()
        try:
            target.relative_to(resolved_reports_root)
        except ValueError as exc:
            raise ReportTaskError(
                f"task report path must stay within reports_root: {task_id}"
            ) from exc
        if target.exists():
            raise ReportTaskError(
                f"immutable revision directory already exists: {target}"
            )
        target.parent.mkdir(parents=True, exist_ok=True)
        temporary = Path(tempfile.mkdtemp(prefix=f".{target.name}-", dir=target.parent))
        final_paths = {
            "report-json": target / "report.json",
            "report-markdown": target / "report.md",
            "content-csv": target / "content_details.csv",
            "channel-csv": target / "channel_conclusions.csv",
            "summary-svg": target / "core_summary.svg",
            "summary-png": target / "core_summary.png",
        }
        planned_files = [
            {"file_kind": kind, "path": _relative(path), "status": "available"}
            for kind, path in final_paths.items()
            if kind != "summary-png"
        ]
        generated_at = now_utc()
        advance_task_progress(
            task_id,
            progress=35,
            message="正在统计数据并生成结论",
            db_path=db_path,
        )
        with connect(db_path) as connection:
            report = _build_report_data(
                connection,
                task_value,
                release=release_value,
                revision=revision,
                generated_at=generated_at,
                files=planned_files,
            )
        advance_task_progress(
            task_id,
            progress=65,
            message="正在生成报告文件",
            db_path=db_path,
        )
        temp_paths = {kind: temporary / path.name for kind, path in final_paths.items()}
        temp_paths["report-markdown"].write_text(_markdown(report), encoding="utf-8")
        temp_paths["content-csv"].parent.mkdir(parents=True, exist_ok=True)
        _write_csv(temp_paths["content-csv"], report["content_details"])
        _write_channel_csv(temp_paths["channel-csv"], report["channels"])
        temp_paths["summary-svg"].write_text(
            render_summary_svg(report), encoding="utf-8"
        )
        png_available = render_summary_png(
            temp_paths["summary-svg"], temp_paths["summary-png"]
        )
        if png_available:
            report["files"].append(
                {
                    "file_kind": "summary-png",
                    "path": _relative(final_paths["summary-png"]),
                    "status": "available",
                }
            )
        advance_task_progress(
            task_id,
            progress=85,
            message="正在检查并保存报告版本",
            db_path=db_path,
        )
        validate_report(report)
        if str(report["rule_version"]) != str(release_value["rule_version"]) or str(
            report["taxonomy_version"]
        ) != str(release_value["taxonomy_version"]):
            raise ReportTaskError(
                "report release changed after the evaluation release was pinned"
            )
        temp_paths["report-json"].write_text(
            json.dumps(report, ensure_ascii=False, indent=2) + "\n", encoding="utf-8"
        )
        if _acknowledge_cancel(task_id, db_path=db_path):
            raise TaskCancelled("任务已在写入 revision 前取消")
        if target.exists():
            raise ReportTaskError(
                f"immutable revision directory already exists: {target}"
            )
        os.replace(temporary, target)
        target_created_by_run = True
        available_paths = {
            kind: path for kind, path in final_paths.items() if path.is_file()
        }
        report_sha = _sha256(final_paths["report-json"])
        completed_at = now_utc()
        terminal_status = str(report["task"]["task_status"])
        with connect(db_path) as connection, transaction(connection):
            _require_pinned_report_release(connection, release_value)
            current_status = connection.execute(
                "SELECT task_status FROM report_tasks WHERE id=?", (task_id,)
            ).fetchone()
            if (
                current_status is not None
                and current_status["task_status"] == "cancel_requested"
            ):
                raise TaskCancelled("任务已在 revision 登记前取消")
            connection.execute(
                """
                INSERT INTO report_revisions(
                    task_id, revision, release_id, contract_version, rule_version,
                    taxonomy_version, report_json_path, report_sha256, created_at
                ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
                """,
                (
                    task_id,
                    revision,
                    release_value["id"],
                    CURRENT_REPORT_VERSION,
                    release_value["rule_version"],
                    release_value["taxonomy_version"],
                    _relative(final_paths["report-json"]),
                    report_sha,
                    completed_at,
                ),
            )
            for kind, path in available_paths.items():
                connection.execute(
                    """
                    INSERT INTO report_files(
                        id, task_id, revision, file_kind, local_path, sha256,
                        byte_size, status, created_at
                    ) VALUES (?, ?, ?, ?, ?, ?, ?, 'available', ?)
                    """,
                    (
                        uuid.uuid4().hex,
                        task_id,
                        revision,
                        kind,
                        _relative(path),
                        _sha256(path),
                        path.stat().st_size,
                        completed_at,
                    ),
                )
            connection.execute(
                """
                UPDATE report_tasks SET task_status=?, progress=100, message=?,
                    completed_at=?, updated_at=? WHERE id=?
                """,
                (
                    terminal_status,
                    _task_completion_message(report),
                    completed_at,
                    completed_at,
                    task_id,
                ),
            )
            connection.execute(
                """
                INSERT INTO task_events(task_id, event_type, message, payload_json, created_at)
                VALUES (?, 'completed', ?, ?, ?)
                """,
                (
                    task_id,
                    f"第 {revision} 版报告已生成",
                    json.dumps({"revision": revision, "status": terminal_status}),
                    completed_at,
                ),
            )
        return report
    except TaskCancelled:
        if temporary is not None and temporary.exists():
            shutil.rmtree(temporary)
        if target_created_by_run and target is not None and target.exists():
            shutil.rmtree(target)
        _acknowledge_cancel(task_id, db_path=db_path)
        raise
    except Exception as exc:
        if temporary is not None and temporary.exists():
            shutil.rmtree(temporary)
        if target_created_by_run and target is not None and target.exists():
            revision_registered = True
            try:
                with connect(db_path) as connection:
                    revision_registered = (
                        connection.execute(
                            """
                        SELECT 1 FROM report_revisions
                        WHERE task_id=? AND revision=?
                        """,
                            (task_id, revision),
                        ).fetchone()
                        is not None
                    )
            except Exception:
                # Preserve the immutable output if registration state cannot be proven.
                revision_registered = True
            if not revision_registered:
                shutil.rmtree(target)
        if owns_run:
            _record_task_failure(task_id, exc, db_path=db_path)
        raise


def create_and_run_task(
    *,
    task_type: str,
    period_start: str,
    period_end: str,
    creation_source: str,
    name: Optional[str] = None,
    db_path: Path = DEFAULT_DB,
    reports_root: Path = REPORTS_ROOT,
) -> Dict[str, Any]:
    with connect(db_path) as connection:
        assert_report_runtime_ready(connection)
    task = create_task(
        task_type=task_type,
        period_start=period_start,
        period_end=period_end,
        creation_source=creation_source,
        name=name,
        db_path=db_path,
    )
    if task["task_status"] in IMPLICIT_RUN_STATUSES:
        run_task(str(task["id"]), db_path=db_path, reports_root=reports_root)
    return get_task(str(task["id"]), db_path=db_path)
