"""Immutable v8 report tasks, revisions and run-scoped export artifacts."""

from __future__ import annotations

import csv
import hashlib
import html
import json
import os
import shutil
import subprocess
import tempfile
import uuid
from collections import Counter
from datetime import date, datetime, time, timedelta, timezone
from pathlib import Path
from typing import Any, Dict, Iterable, List, Mapping, Optional, Sequence
from zoneinfo import ZoneInfo

from .contracts import (
    CURRENT_REPORT_VERSION,
    expected_terminal_task_status,
    quantity_metric,
    ratio_metric,
    validate_report,
)
from .evaluation import EVIDENCE_VERSION, RULE_VERSION
from .duplicates import FINGERPRINT_VERSION, THRESHOLDS
from .storage import DEFAULT_DB, PROJECT_ROOT, connect, now_utc, transaction


REPORTS_ROOT = PROJECT_ROOT / "reports" / "runs" / "v8"
SHANGHAI = ZoneInfo("Asia/Shanghai")
TASK_TYPES = {"daily", "weekly", "custom"}
RUNNABLE_STATUSES = {"queued", "partial", "failed", "interrupted"}


class ReportTaskError(RuntimeError):
    pass


class TaskCancelled(ReportTaskError):
    pass


def request_task_cancel(task_id: str, *, db_path: Path = DEFAULT_DB) -> Dict[str, Any]:
    with connect(db_path) as connection, transaction(connection):
        task = connection.execute(
            "SELECT * FROM report_tasks WHERE id=?", (task_id,)
        ).fetchone()
        if task is None:
            raise ReportTaskError(f"task does not exist: {task_id}")
        current = str(task["task_status"])
        if current == "running":
            next_status, message = "cancel_requested", "已请求取消，等待当前安全点"
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
                "cancel_requested" if next_status == "cancel_requested" else "cancelled",
                message,
                captured_at,
            ),
        )
    return get_task(task_id, db_path=db_path)


def resume_task(task_id: str, *, db_path: Path = DEFAULT_DB) -> Dict[str, Any]:
    with connect(db_path) as connection, transaction(connection):
        task = connection.execute(
            "SELECT task_status FROM report_tasks WHERE id=?", (task_id,)
        ).fetchone()
        if task is None:
            raise ReportTaskError(f"task does not exist: {task_id}")
        if task["task_status"] != "cancelled":
            raise ReportTaskError(
                f"task {task_id} cannot be resumed from {task['task_status']}"
            )
        captured_at = now_utc()
        connection.execute(
            """
            UPDATE report_tasks SET task_status='queued', progress=0,
                message='任务已恢复，等待生成新 revision', started_at=NULL,
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
            "SELECT task_status FROM report_tasks WHERE id=?", (task_id,)
        ).fetchone()
        if task is None:
            raise ReportTaskError(f"task does not exist: {task_id}")
        if task["task_status"] not in {"succeeded", "partial", "failed", "interrupted"}:
            raise ReportTaskError(
                f"task {task_id} cannot create a revision from {task['task_status']}"
            )
        captured_at = now_utc()
        connection.execute(
            """
            UPDATE report_tasks SET task_status='queued', progress=0,
                message='已请求生成新 revision', started_at=NULL, completed_at=NULL,
                updated_at=? WHERE id=?
            """,
            (captured_at, task_id),
        )
        connection.execute(
            """
            INSERT INTO task_events(task_id,event_type,message,payload_json,created_at)
            VALUES (?, 'retry_requested', '已请求生成新 revision', '{}', ?)
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
            UPDATE report_tasks SET task_status='cancelled', message='任务已在安全点取消',
                completed_at=?, updated_at=? WHERE id=?
            """,
            (captured_at, captured_at, task_id),
        )
        connection.execute(
            """
            INSERT INTO task_events(task_id,event_type,message,payload_json,created_at)
            VALUES (?, 'cancelled', '任务已在安全点取消', '{}', ?)
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
        start_local.astimezone(timezone.utc).isoformat(timespec="seconds").replace("+00:00", "Z"),
        end_local.astimezone(timezone.utc).isoformat(timespec="seconds").replace("+00:00", "Z"),
    )


def _task_id(task_type: str, period_start: str, period_end: str, source: str) -> str:
    if source == "automatic":
        prefix = "D" if task_type == "daily" else "W"
        return f"D8-{prefix}-{period_start.replace('-', '')}-{period_end.replace('-', '')}"
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
    if task_type == "weekly" and (start.weekday() != 0 or end != start + timedelta(days=6)):
        raise ReportTaskError("weekly task must cover Monday through Sunday")
    if creation_source == "manual" and task_type != "custom":
        raise ReportTaskError("manual report creation must use custom task type")
    task_id = _task_id(task_type, period_start, period_end, creation_source)
    created_at = now_utc()
    display_name = name.strip() if name and name.strip() else (
        f"{period_start} 日报" if task_type == "daily"
        else f"{period_start} 至 {period_end} 周报" if task_type == "weekly"
        else f"{period_start} 至 {period_end} 自定义报告"
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
                task_id, task_type, display_name, period_start, period_end,
                creation_source, created_at, created_at,
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


def get_task(task_id: str, *, db_path: Path = DEFAULT_DB) -> Dict[str, Any]:
    with connect(db_path) as connection:
        task = connection.execute("SELECT * FROM report_tasks WHERE id=?", (task_id,)).fetchone()
        if task is None:
            raise ReportTaskError(f"task does not exist: {task_id}")
        events = connection.execute(
            "SELECT * FROM task_events WHERE task_id=? ORDER BY id", (task_id,)
        ).fetchall()
        revisions = connection.execute(
            "SELECT * FROM report_revisions WHERE task_id=? ORDER BY revision DESC",
            (task_id,),
        ).fetchall()
        revision_values: List[Dict[str, Any]] = []
        for row in revisions:
            files = connection.execute(
                """
                SELECT id, file_kind, byte_size, status, error_message, created_at
                FROM report_files WHERE task_id=? AND revision=? ORDER BY file_kind
                """,
                (task_id, row["revision"]),
            ).fetchall()
            value = dict(row)
            value["files"] = [dict(item) for item in files]
            revision_values.append(value)
        counts = connection.execute(
            """
            SELECT inclusion_status, COUNT(*) count FROM task_contents
            WHERE task_id=? GROUP BY inclusion_status
            """,
            (task_id,),
        ).fetchall()
    value = dict(task)
    value["events"] = [dict(row) for row in events]
    value["revisions"] = revision_values
    value["content_counts"] = {str(row["inclusion_status"]): int(row["count"]) for row in counts}
    return value


def _snapshot_task_contents(connection, task: Mapping[str, Any]) -> None:
    existing = int(
        connection.execute(
            "SELECT COUNT(*) FROM task_contents WHERE task_id=?", (task["id"],)
        ).fetchone()[0]
    )
    if existing:
        return
    start_utc, end_utc = period_bounds(str(task["period_start"]), str(task["period_end"]))
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
            """,
            (task["id"], row["id"]),
        )


def _latest_rows(connection, table: str, ids: Sequence[int], order: str) -> Dict[int, Dict[str, Any]]:
    if not ids:
        return {}
    placeholders = ",".join("?" for _ in ids)
    validity = " AND invalidated_at IS NULL" if table == "evaluation_versions" else ""
    rows = connection.execute(
        f"SELECT * FROM {table} WHERE content_id IN ({placeholders}){validity} ORDER BY {order}",
        ids,
    ).fetchall()
    output: Dict[int, Dict[str, Any]] = {}
    for row in rows:
        content_id = int(row["content_id"])
        if content_id not in output:
            output[content_id] = dict(row)
    return output


def _percentage(numerator: int, denominator: int) -> Optional[float]:
    return round(numerator * 100 / denominator, 2) if denominator else None


def _dimension(items: Iterable[str], total: int) -> List[Dict[str, Any]]:
    counts = Counter(items)
    return [
        {"key": key, "count": count, "percentage": _percentage(count, total)}
        for key, count in sorted(counts.items())
    ]


def _metric_eligible(published_at: Optional[str], generated_at: datetime) -> bool:
    if not published_at:
        return False
    try:
        published = datetime.fromisoformat(published_at.replace("Z", "+00:00"))
    except ValueError:
        return False
    return published <= generated_at <= published + timedelta(days=30)


def _build_report_data(
    connection,
    task: Mapping[str, Any],
    *,
    revision: int,
    generated_at: str,
    files: List[Dict[str, Any]],
) -> Dict[str, Any]:
    taxonomy = connection.execute(
        """
        SELECT * FROM taxonomy_versions WHERE status='published'
        ORDER BY published_at DESC, created_at DESC LIMIT 1
        """
    ).fetchone()
    if taxonomy is None:
        raise ReportTaskError("published taxonomy is required")
    content_rows = connection.execute(
        """
        SELECT c.*, COALESCE(a.account_type, c.legacy_account_type, 'unknown') account_type,
               COALESCE(c.manual_content_direction, c.evaluation_content_direction,
                        a.content_direction, 'unknown') resolved_direction
        FROM task_contents tc JOIN content_items c ON c.id=tc.content_id
        LEFT JOIN accounts a ON a.id=c.account_id
        WHERE tc.task_id=? AND tc.inclusion_status='included'
        ORDER BY c.published_at, c.id
        """,
        (task["id"],),
    ).fetchall()
    contents = [dict(row) for row in content_rows]
    ids = [int(row["id"]) for row in content_rows]
    evaluations = _latest_rows(connection, "evaluation_versions", ids, "evaluated_at DESC, id DESC")
    snapshots = _latest_rows(connection, "content_metric_snapshots", ids, "captured_at DESC, id DESC")
    total = len(contents)
    current_evaluations = {
        content_id: value for content_id, value in evaluations.items()
        if value["rule_version"] == RULE_VERSION
        and value["taxonomy_version"] == taxonomy["version"]
    }
    eval_ready = sum(
        1 for value in current_evaluations.values() if value["evidence_level"] in {"V2", "V3"}
    )
    vertical = sum(
        1 for value in current_evaluations.values()
        if value["evidence_level"] in {"V2", "V3"}
        and value["content_automotive_score"] is not None
        and int(value["content_automotive_score"]) >= 60
    )
    selling = sum(
        1 for value in current_evaluations.values()
        if value["evidence_level"] in {"V2", "V3"} and int(value["selling_point_included"]) == 1
    )
    active_accounts = len({int(row["account_id"]) for row in content_rows if row["account_id"] is not None})
    detail_ready = 0
    fingerprint_ready = 0
    duplicate_calibration_ready = False
    if ids:
        placeholders = ",".join("?" for _ in ids)
        detail_ready = int(
            connection.execute(
                f"""
                SELECT COUNT(DISTINCT content_id) FROM fetch_slots
                WHERE content_id IN ({placeholders}) AND stage='detail' AND status='succeeded'
                """,
                ids,
            ).fetchone()[0]
        )
        fingerprint_ready = int(
            connection.execute(
                f"""
                SELECT COUNT(DISTINCT content_id) FROM duplicate_fingerprints
                WHERE content_id IN ({placeholders}) AND fingerprint_version=?
                """,
                [*ids, FINGERPRINT_VERSION],
            ).fetchone()[0]
        )
        duplicate_calibration_ready = connection.execute(
            """
            SELECT 1 FROM duplicate_calibration_runs
            WHERE fingerprint_version=? AND thresholds_json=? AND status='passed'
            LIMIT 1
            """,
            (
                FINGERPRINT_VERSION,
                json.dumps(THRESHOLDS, ensure_ascii=False, sort_keys=True, separators=(",", ":")),
            ),
        ).fetchone() is not None
    generated_dt = datetime.fromisoformat(generated_at.replace("Z", "+00:00"))
    metric_eligible_ids = {
        int(row["id"]) for row in content_rows
        if _metric_eligible(row["published_at"], generated_dt)
    }
    fresh_metric_ids = {
        content_id for content_id, value in snapshots.items()
        if content_id in metric_eligible_ids
        and value["status"] == "available"
        and datetime.fromisoformat(str(value["captured_at"]).replace("Z", "+00:00"))
        >= generated_dt - timedelta(hours=36)
    }
    metrics_freshness = (
        _percentage(len(fresh_metric_ids), len(metric_eligible_ids))
        if metric_eligible_ids else 100.0
    )
    routed_media_ids: set[int] = set()
    recent_comment_ids: set[int] = set()
    if ids:
        placeholders = ",".join("?" for _ in ids)
        routed_media_ids = {
            int(row["content_id"])
            for row in connection.execute(
                f"""
                SELECT DISTINCT content_id FROM review_queue
                WHERE content_id IN ({placeholders})
                  AND status IN ('manual_required','resolved','terminal_failed')
                """,
                ids,
            ).fetchall()
        }
        comment_cutoff = (
            datetime.fromisoformat(generated_at.replace("Z", "+00:00"))
            - timedelta(days=8)
        ).isoformat().replace("+00:00", "Z")
        recent_comment_ids = {
            int(row["content_id"])
            for row in connection.execute(
                f"""
                SELECT DISTINCT content_id FROM comment_evidence_versions
                WHERE content_id IN ({placeholders}) AND status='available'
                  AND captured_at>=? AND captured_at<=?
                """,
                [*ids, comment_cutoff, generated_at],
            ).fetchall()
        }
    media_terminal_ids = {
        content_id for content_id, value in current_evaluations.items()
        if value["evidence_level"] in {"V2", "V3"}
    } | routed_media_ids
    media_terminal_coverage = _percentage(len(media_terminal_ids), total) if total else 100.0
    weekly_comment_coverage = (
        _percentage(len(recent_comment_ids), total)
        if task["task_type"] == "weekly" and total else 100.0
    )
    data_quality = {
        "discovery_coverage": 100.0,
        "detail_coverage": _percentage(detail_ready, total) if total else 100.0,
        "metrics_freshness": metrics_freshness,
        "evaluation_coverage": _percentage(eval_ready, total) if total else 100.0,
        "core_artifact_coverage": 100.0,
        "media_terminal_coverage": media_terminal_coverage,
        "duplicate_fingerprint_coverage": (
            _percentage(fingerprint_ready, total) if duplicate_calibration_ready and total else 100.0 if not total else 0.0
        ),
        "weekly_comment_coverage": weekly_comment_coverage,
    }
    task_status = expected_terminal_task_status(data_quality)
    metric_coverage = _percentage(len(snapshots), total)
    view_values = [int(value["view_count"]) for value in snapshots.values() if value["view_count"] is not None]
    comment_values = [int(value["comment_count"]) for value in snapshots.values() if value["comment_count"] is not None]
    all_historical = bool(snapshots) and all(
        str(value["source"]).startswith("migrated_") for value in snapshots.values()
    )
    view_status = (
        "not_applicable" if total == 0 else "missing" if not view_values
        else "stale" if all_historical else "available" if (metric_coverage or 0) >= 90
        else "below_threshold"
    )
    comment_status = (
        "not_applicable" if total == 0 else "missing" if not comment_values
        else "available" if len(comment_values) * 100 / total >= 90 else "below_threshold"
    )
    eval_status = (
        "not_applicable" if total == 0 else "available"
        if float(data_quality["evaluation_coverage"] or 0) >= 95 else "below_threshold"
    )
    point_counts = Counter(
        str(value["primary_selling_point_code"])
        for value in current_evaluations.values()
        if value["primary_selling_point_code"] and int(value["selling_point_included"]) == 1
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
    review_rows = connection.execute(
        """
        SELECT rq.status, COUNT(*) count FROM review_queue rq
        WHERE rq.content_id IN (
            SELECT content_id FROM task_contents
            WHERE task_id=? AND inclusion_status='included'
        ) GROUP BY rq.status ORDER BY rq.status
        """,
        (task["id"],),
    ).fetchall()
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
    start_utc, end_utc = period_bounds(str(task["period_start"]), str(task["period_end"]))
    cost_rows = connection.execute(
        """
        SELECT provider, operation, currency, SUM(request_attempts) request_attempts,
               SUM(billed_requests) billed_requests, ROUND(SUM(amount), 6) amount
        FROM provider_usage WHERE recorded_at>=? AND recorded_at<?
        GROUP BY provider, operation, currency ORDER BY provider, operation
        """,
        (start_utc, end_utc),
    ).fetchall()
    details: List[Dict[str, Any]] = []
    for content in contents:
        content_id = int(content["id"])
        evaluation = current_evaluations.get(content_id)
        snapshot = snapshots.get(content_id)
        duplicate = duplicate_by_content.get(content_id)
        details.append(
            {
                "content_id": content_id,
                "link_id": content["link_id"],
                "platform": content["platform"],
                "published_at": content["published_at"],
                "canonical_url": content["canonical_url"],
                "title": content["title"],
                "account_uid": content["raw_account_uid"],
                "account_name": content["raw_account_name"],
                "account_type": content["account_type"],
                "content_direction": evaluation["content_direction"] if evaluation else content["resolved_direction"],
                "evidence_level": evaluation["evidence_level"] if evaluation else None,
                "primary_selling_point_code": evaluation["primary_selling_point_code"] if evaluation else None,
                "selling_point_score": evaluation["selling_point_score"] if evaluation else None,
                "content_automotive_score": evaluation["content_automotive_score"] if evaluation else None,
                "view_count": snapshot["view_count"] if snapshot else None,
                "comment_count": snapshot["comment_count"] if snapshot else None,
                "duplicate_original_link_id": duplicate["original_link_id"] if duplicate else None,
                "duplicate_method": duplicate["method"] if duplicate else None,
                "duplicate_confidence": duplicate["confidence"] if duplicate else None,
                "evaluation_current": evaluation is not None,
            }
        )
    return {
        "report_version": CURRENT_REPORT_VERSION,
        "rule_version": RULE_VERSION,
        "taxonomy_version": str(taxonomy["version"]),
        "evidence_version": EVIDENCE_VERSION,
        "metadata": {"task_id": task["id"], "revision": revision, "generated_at": generated_at},
        "scope": {
            "period_start": f"{task['period_start']}T00:00:00+08:00",
            "period_end": f"{(date.fromisoformat(str(task['period_end'])) + timedelta(days=1)).isoformat()}T00:00:00+08:00",
            "timezone": "Asia/Shanghai",
        },
        "task": {
            "task_type": task["task_type"], "creation_source": task["creation_source"],
            "task_status": task_status, "name": task["name"],
        },
        "data_quality": data_quality,
        "summary_metrics": {
            "publication_count": quantity_metric(total, unit="content", status="available"),
            "active_account_count": quantity_metric(active_accounts, unit="account", status="available"),
            "view_count": quantity_metric(
                sum(view_values) if view_values else None, unit="view", status=view_status,
                coverage_percentage=metric_coverage,
                reason="存量曝光为一次性历史快照，不能解释为当前实时值" if all_historical else "",
            ),
            "comment_count": quantity_metric(
                sum(comment_values) if comment_values else None, unit="comment", status=comment_status,
                coverage_percentage=_percentage(len(comment_values), total),
                reason="存量 valid_unique_commenters 不是评论量，未转换为评论数",
            ),
            "verticality_rate": ratio_metric(
                vertical if total else None, total, status=eval_status,
                eligible_count=eval_ready, coverage_percentage=data_quality["evaluation_coverage"],
            ),
            "selling_point_coverage_rate": ratio_metric(
                selling if total else None, total, status=eval_status,
                eligible_count=eval_ready, coverage_percentage=data_quality["evaluation_coverage"],
            ),
            "duplicate_rate": ratio_metric(
                len(duplicate_rows) if total else None,
                total,
                status=(
                    "not_applicable" if total == 0 else "available"
                    if float(data_quality["duplicate_fingerprint_coverage"] or 0) >= 95
                    else "below_threshold"
                ),
                eligible_count=fingerprint_ready,
                coverage_percentage=data_quality["duplicate_fingerprint_coverage"],
                reason=(
                    "感知指纹定标未通过或覆盖不足，重复率暂不发布"
                    if total and float(data_quality["duplicate_fingerprint_coverage"] or 0) < 95 else ""
                ),
            ),
            "estimated_new_users": quantity_metric(
                None, unit="person", status="not_applicable" if total == 0 else "not_calculable",
                reason="v8 首版没有经过验证的业务预估模型",
            ),
            "estimated_reactivated_users": quantity_metric(
                None, unit="person", status="not_applicable" if total == 0 else "not_calculable",
                reason="v8 首版没有经过验证的业务预估模型",
            ),
            "estimated_leads": quantity_metric(
                None, unit="lead", status="not_applicable" if total == 0 else "not_calculable",
                reason="v8 首版没有经过验证的业务预估模型",
            ),
        },
        "platform_dimensions": _dimension((str(row["platform"]) for row in contents), total),
        "account_type_dimensions": _dimension((str(row["account_type"]) for row in contents), total),
        "content_direction_dimensions": _dimension(
            (
                str(current_evaluations[int(row["id"])]["content_direction"])
                if int(row["id"]) in current_evaluations else str(row["resolved_direction"])
                for row in contents
            ),
            total,
        ),
        "selling_point_dimensions": [
            {"code": code, "count": count, "percentage": _percentage(count, total)}
            for code, count in sorted(point_counts.items())
        ],
        "duplicates": [dict(row) for row in duplicate_rows],
        "review_summary": [dict(row) for row in review_rows],
        "capture_summary": [dict(row) for row in capture_rows],
        "provider_costs": [dict(row) for row in cost_rows],
        "content_details": details,
        "files": files,
    }


def _markdown(report: Mapping[str, Any]) -> str:
    metrics = report["summary_metrics"]
    lines = [
        f"# {report['task']['name']}", "",
        f"- 报告合同：{report['report_version']}",
        f"- 评估规则：{report['rule_version']}",
        f"- 卖点标准：{report['taxonomy_version']}",
        f"- 任务：{report['metadata']['task_id']} / revision {report['metadata']['revision']}",
        f"- 任务状态：{report['task']['task_status']}",
        f"- 统计区间：{report['scope']['period_start']} 至 {report['scope']['period_end']}（右开）", "",
        "## 概览", "",
        f"- 发布内容：{metrics['publication_count']['value']} 条",
        f"- 重复内容率：{metrics['duplicate_rate']['percentage'] if metrics['duplicate_rate']['percentage'] is not None else '暂不可计算'}%",
        f"- 发布账号：{metrics['active_account_count']['value']} 个",
        f"- 阅读 / 播放：{metrics['view_count']['value'] if metrics['view_count']['value'] is not None else '暂不可计算'}",
        f"- 评论数：{metrics['comment_count']['value'] if metrics['comment_count']['value'] is not None else '暂不可计算'}",
        f"- 内容垂直度：{metrics['verticality_rate']['percentage'] if metrics['verticality_rate']['percentage'] is not None else '暂不可计算'}%",
        f"- 卖点覆盖率：{metrics['selling_point_coverage_rate']['percentage'] if metrics['selling_point_coverage_rate']['percentage'] is not None else '暂不可计算'}%", "",
        "## 数据质量", "",
    ]
    for key, value in report["data_quality"].items():
        lines.append(f"- {key}: {value}%")
    lines.extend(["", "三项业务预估在 v8 首版恒为 `not_calculable`，不会生成推测值。", ""])
    return "\n".join(lines)


def _svg(report: Mapping[str, Any]) -> str:
    metrics = report["summary_metrics"]
    publication = metrics["publication_count"]["value"]
    verticality = metrics["verticality_rate"]["percentage"]
    selling = metrics["selling_point_coverage_rate"]["percentage"]
    title = html.escape(str(report["task"]["name"]))
    return f"""<svg xmlns="http://www.w3.org/2000/svg" width="1200" height="675" viewBox="0 0 1200 675">
<rect width="1200" height="675" fill="#102c35"/><rect x="70" y="62" width="10" height="92" rx="5" fill="#d9ff57"/>
<style>.t{{font-family:-apple-system,BlinkMacSystemFont,'PingFang SC',sans-serif;fill:#fff}}.m{{fill:#91a9b0}}</style>
<text class="t" x="110" y="105" font-size="24">DCar Insight · v8</text><text class="t" x="110" y="148" font-size="38" font-weight="700">{title}</text>
<text class="t m" x="75" y="220" font-size="18">{html.escape(str(report['scope']['period_start']))} — {html.escape(str(report['scope']['period_end']))}</text>
<rect x="75" y="278" width="320" height="230" rx="18" fill="#173e48"/><rect x="440" y="278" width="320" height="230" rx="18" fill="#173e48"/><rect x="805" y="278" width="320" height="230" rx="18" fill="#173e48"/>
<text class="t m" x="110" y="330" font-size="18">发布内容</text><text class="t" x="110" y="420" font-size="72" font-weight="700">{publication}</text><text class="t m" x="110" y="465" font-size="17">条</text>
<text class="t m" x="475" y="330" font-size="18">内容垂直度</text><text class="t" x="475" y="420" font-size="72" font-weight="700">{verticality if verticality is not None else '—'}</text><text class="t m" x="475" y="465" font-size="17">%</text>
<text class="t m" x="840" y="330" font-size="18">卖点覆盖率</text><text class="t" x="840" y="420" font-size="72" font-weight="700">{selling if selling is not None else '—'}</text><text class="t m" x="840" y="465" font-size="17">%</text>
<text class="t m" x="75" y="600" font-size="16">状态 {html.escape(str(report['task']['task_status']))} · 缺失与低覆盖均按合同显式标记</text>
</svg>
"""


def _write_csv(path: Path, rows: Sequence[Mapping[str, Any]]) -> None:
    fields = [
        "content_id", "link_id", "platform", "published_at", "canonical_url", "title",
        "account_uid", "account_name", "account_type", "content_direction", "evidence_level",
        "primary_selling_point_code", "selling_point_score", "content_automotive_score",
        "view_count", "comment_count", "duplicate_original_link_id", "duplicate_method",
        "duplicate_confidence", "evaluation_current",
    ]
    with path.open("w", encoding="utf-8-sig", newline="") as handle:
        writer = csv.DictWriter(handle, fieldnames=fields)
        writer.writeheader()
        writer.writerows(rows)


def _render_png(svg_path: Path, png_path: Path) -> bool:
    renderer = shutil.which("qlmanage")
    if renderer is None:
        return False
    try:
        result = subprocess.run(
            [renderer, "-t", "-s", "1600", "-o", str(svg_path.parent), str(svg_path)],
            capture_output=True,
            timeout=30,
            check=False,
        )
    except (OSError, subprocess.TimeoutExpired):
        return False
    generated = svg_path.with_name(svg_path.name + ".png")
    if result.returncode != 0 or not generated.is_file() or generated.stat().st_size <= 1024:
        return False
    generated.replace(png_path)
    return True


def _sha256(path: Path) -> str:
    return hashlib.sha256(path.read_bytes()).hexdigest()


def _relative(path: Path) -> str:
    return str(path.relative_to(PROJECT_ROOT))


def run_task(
    task_id: str,
    *,
    db_path: Path = DEFAULT_DB,
    reports_root: Path = REPORTS_ROOT,
) -> Dict[str, Any]:
    blocked_message: Optional[str] = None
    with connect(db_path) as connection, transaction(connection):
        task = connection.execute("SELECT * FROM report_tasks WHERE id=?", (task_id,)).fetchone()
        if task is None:
            raise ReportTaskError(f"task does not exist: {task_id}")
        if task["task_status"] not in RUNNABLE_STATUSES:
            raise ReportTaskError(f"task {task_id} is not runnable from {task['task_status']}")
        pending_gray_reviews = int(
            connection.execute(
                """
                SELECT COUNT(*) FROM review_queue
                WHERE reason_code='evaluation_gray_zone'
                  AND status IN ('pending','manual_required','in_review')
                """
            ).fetchone()[0]
        )
        formal_release_exists = connection.execute(
            """
            SELECT 1 FROM report_revisions
            WHERE contract_version=? AND invalidated_at IS NULL
            LIMIT 1
            """,
            (CURRENT_REPORT_VERSION,),
        ).fetchone() is not None
        if pending_gray_reviews and not formal_release_exists:
            blocked_message = f"正式报告已阻断：仍有 {pending_gray_reviews} 条灰区内容未完成人工复核"
            blocked_at = now_utc()
            connection.execute(
                """
                UPDATE report_tasks SET task_status='failed', progress=0, message=?,
                    completed_at=?, updated_at=? WHERE id=?
                """,
                (blocked_message, blocked_at, blocked_at, task_id),
            )
            connection.execute(
                """
                INSERT INTO task_events(task_id, event_type, message, payload_json, created_at)
                VALUES (?, 'review_gate_blocked', ?, ?, ?)
                """,
                (
                    task_id,
                    blocked_message,
                    json.dumps({"pending_gray_reviews": pending_gray_reviews}),
                    blocked_at,
                ),
            )
    if blocked_message is not None:
        raise ReportTaskError(blocked_message)
    with connect(db_path) as connection, transaction(connection):
        task = connection.execute("SELECT * FROM report_tasks WHERE id=?", (task_id,)).fetchone()
        if task is None:
            raise ReportTaskError(f"task does not exist: {task_id}")
        if task["task_status"] not in RUNNABLE_STATUSES:
            raise ReportTaskError(f"task {task_id} is not runnable from {task['task_status']}")
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
                message='正在生成不可变报告', started_at=?, completed_at=NULL, updated_at=?
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

    target = reports_root / task_id / f"revision_{revision:03d}"
    target.parent.mkdir(parents=True, exist_ok=True)
    temporary = Path(tempfile.mkdtemp(prefix=f".{target.name}-", dir=target.parent))
    final_paths = {
        "report-json": target / "report.json",
        "report-markdown": target / "report.md",
        "content-csv": target / "content_details.csv",
        "summary-svg": target / "core_summary.svg",
        "summary-png": target / "core_summary.png",
    }
    planned_files = [
        {"file_kind": kind, "path": _relative(path), "status": "available"}
        for kind, path in final_paths.items() if kind != "summary-png"
    ]
    try:
        generated_at = now_utc()
        with connect(db_path) as connection:
            report = _build_report_data(
                connection, task_value, revision=revision,
                generated_at=generated_at, files=planned_files,
            )
        temp_paths = {kind: temporary / path.name for kind, path in final_paths.items()}
        temp_paths["report-markdown"].write_text(_markdown(report), encoding="utf-8")
        temp_paths["content-csv"].parent.mkdir(parents=True, exist_ok=True)
        _write_csv(temp_paths["content-csv"], report["content_details"])
        temp_paths["summary-svg"].write_text(_svg(report), encoding="utf-8")
        png_available = _render_png(temp_paths["summary-svg"], temp_paths["summary-png"])
        if png_available:
            report["files"].append(
                {
                    "file_kind": "summary-png",
                    "path": _relative(final_paths["summary-png"]),
                    "status": "available",
                }
            )
        validate_report(report)
        temp_paths["report-json"].write_text(
            json.dumps(report, ensure_ascii=False, indent=2) + "\n", encoding="utf-8"
        )
        if _acknowledge_cancel(task_id, db_path=db_path):
            raise TaskCancelled("任务已在写入 revision 前取消")
        if target.exists():
            raise ReportTaskError(f"immutable revision directory already exists: {target}")
        os.replace(temporary, target)
        available_paths = {
            kind: path for kind, path in final_paths.items() if path.is_file()
        }
        report_sha = _sha256(final_paths["report-json"])
        completed_at = now_utc()
        terminal_status = str(report["task"]["task_status"])
        with connect(db_path) as connection, transaction(connection):
            current_status = connection.execute(
                "SELECT task_status FROM report_tasks WHERE id=?", (task_id,)
            ).fetchone()
            if current_status is not None and current_status["task_status"] == "cancel_requested":
                raise TaskCancelled("任务已在 revision 登记前取消")
            connection.execute(
                """
                INSERT INTO report_revisions(
                    task_id, revision, contract_version, rule_version, taxonomy_version,
                    report_json_path, report_sha256, created_at
                ) VALUES (?, ?, ?, ?, ?, ?, ?, ?)
                """,
                (
                    task_id, revision, CURRENT_REPORT_VERSION, RULE_VERSION,
                    report["taxonomy_version"], _relative(final_paths["report-json"]),
                    report_sha, completed_at,
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
                        uuid.uuid4().hex, task_id, revision, kind, _relative(path),
                        _sha256(path), path.stat().st_size, completed_at,
                    ),
                )
            connection.execute(
                """
                UPDATE report_tasks SET task_status=?, progress=100, message=?,
                    completed_at=?, updated_at=? WHERE id=?
                """,
                (
                    terminal_status,
                    "报告已生成；必需覆盖率不足" if terminal_status == "partial" else "报告已生成",
                    completed_at, completed_at, task_id,
                ),
            )
            connection.execute(
                """
                INSERT INTO task_events(task_id, event_type, message, payload_json, created_at)
                VALUES (?, 'completed', ?, ?, ?)
                """,
                (
                    task_id, f"revision {revision} 已生成",
                    json.dumps({"revision": revision, "status": terminal_status}), completed_at,
                ),
            )
        return report
    except TaskCancelled:
        if temporary.exists():
            shutil.rmtree(temporary)
        if target.exists():
            shutil.rmtree(target)
        _acknowledge_cancel(task_id, db_path=db_path)
        raise
    except Exception as exc:
        if temporary.exists():
            shutil.rmtree(temporary)
        failed_at = now_utc()
        with connect(db_path) as connection, transaction(connection):
            connection.execute(
                """
                UPDATE report_tasks SET task_status='failed', message=?, completed_at=?, updated_at=?
                WHERE id=?
                """,
                (str(exc)[:1000], failed_at, failed_at, task_id),
            )
            connection.execute(
                """
                INSERT INTO task_events(task_id, event_type, message, payload_json, created_at)
                VALUES (?, 'failed', ?, '{}', ?)
                """,
                (task_id, str(exc)[:1000], failed_at),
            )
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
    task = create_task(
        task_type=task_type,
        period_start=period_start,
        period_end=period_end,
        creation_source=creation_source,
        name=name,
        db_path=db_path,
    )
    if task["task_status"] in RUNNABLE_STATUSES:
        run_task(str(task["id"]), db_path=db_path, reports_root=reports_root)
    return get_task(str(task["id"]), db_path=db_path)
