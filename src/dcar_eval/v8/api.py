"""FastAPI application for DCar Insight v8 with temporary v7 read compatibility."""

from __future__ import annotations

import json
import logging
import os
import re
import sqlite3
from contextlib import asynccontextmanager
from datetime import datetime, time, timedelta, timezone
from pathlib import Path
from typing import Any, Dict, List, Optional
from urllib.parse import unquote
from zoneinfo import ZoneInfo

from apscheduler.schedulers.background import BackgroundScheduler  # type: ignore[import-untyped]
from fastapi import FastAPI, HTTPException, Request
from fastapi.middleware.cors import CORSMiddleware
from fastapi.responses import FileResponse, JSONResponse, Response
from pydantic import BaseModel, Field

from .capture import BudgetBlocked, CaptureError, SlotUnavailable
from .contracts import CURRENT_REPORT_VERSION, quantity_metric, ratio_metric
from .duplicates import FINGERPRINT_VERSION, THRESHOLDS
from .evaluation import RULE_VERSION, EvaluationError, resolve_review
from .insights import build_channel_conclusions
from .media import MediaProcessingError
from .operations import (
    OperationError,
    content_identity,
    export_accounts_csv,
    export_contents_csv,
    import_accounts,
    import_contents,
    update_account,
    update_content,
    upsert_account,
    upsert_content,
)
from .providers import ProviderConfigurationError, retry_content_media, update_content_data
from .reports import (
    REPORTS_ROOT,
    ReportTaskError,
    create_and_run_task,
    get_task,
    request_task_cancel,
    retry_task,
    resume_task,
    run_task,
)
from .scheduler import install_jobs, startup_catchup
from .storage import (
    DEFAULT_DB,
    PROJECT_ROOT,
    connect,
    initialize_database,
    now_utc,
    transaction,
)
from .taxonomy import (
    TaxonomyError,
    create_point,
    delete_point,
    ensure_draft,
    list_points,
    publish_draft,
    update_point,
)


LOGGER = logging.getLogger("dcar.api")
API_DB_PATH = Path(os.environ.get("DCAR_V8_DB", str(DEFAULT_DB)))
API_REPORTS_ROOT = Path(os.environ.get("DCAR_V8_REPORTS_ROOT", str(REPORTS_ROOT)))
LEGACY_DB = PROJECT_ROOT / "app" / "data" / "web_mvp.sqlite3"
SHANGHAI = ZoneInfo("Asia/Shanghai")
LEGACY_REPORT_VERSION = "channel-structured-conclusions-v7.0"
LEGACY_EXPORTS = {
    "report-json": "report.json",
    "report-markdown": "report.md",
    "douyin-csv": "douyin_content_details.csv",
    "xiaohongshu-csv": "xiaohongshu_content_details.csv",
    "summary-image": "core_summary.png",
}
DOUYIN_URL_RE = re.compile(r"https?://(?:www\.)?douyin\.com/", re.I)
XHS_URL_RE = re.compile(r"https?://(?:www\.)?xiaohongshu\.com/", re.I)
UID_RE = re.compile(r"^\d{6,24}$")


class InputValidationRequest(BaseModel):
    channel: str = Field(max_length=32)
    text: str = Field(max_length=2_000_000)


class AccountSearchRequest(BaseModel):
    query: str = Field(default="", max_length=100)
    account_type: Optional[str] = Field(default=None, max_length=32)
    content_direction: Optional[str] = Field(default=None, max_length=32)
    platform: Optional[str] = Field(default=None, max_length=32)
    page: int = Field(default=1, ge=1)
    page_size: int = Field(default=50, ge=1, le=100)


class ContentSearchRequest(BaseModel):
    query: str = Field(default="", max_length=200)
    platform: Optional[str] = Field(default=None, max_length=32)
    account_type: Optional[str] = Field(default=None, max_length=32)
    content_direction: Optional[str] = Field(default=None, max_length=32)
    review_status: Optional[str] = Field(default=None, max_length=32)
    page: int = Field(default=1, ge=1)
    page_size: int = Field(default=50, ge=1, le=100)


class ReviewResolveRequest(BaseModel):
    base_evaluation_id: int = Field(ge=1)
    decision: str = Field(max_length=40)
    reason: str = Field(min_length=1, max_length=2000)
    reviewer: str = Field(min_length=1, max_length=100)
    evidence_type: str = Field(min_length=1, max_length=60)
    evidence_text: str = Field(min_length=1, max_length=10000)
    primary_selling_point_code: Optional[str] = Field(default=None, max_length=8)
    selling_point_score: Optional[int] = Field(default=None, ge=0, le=100)
    selling_point_included: Optional[bool] = None
    content_automotive_score: Optional[int] = Field(default=None, ge=0, le=100)
    content_direction: Optional[str] = Field(default=None, max_length=32)


class SellingPointMutationRequest(BaseModel):
    code: Optional[str] = Field(default=None, max_length=3)
    tier: str = Field(max_length=16)
    label: str = Field(min_length=1, max_length=300)
    definition: str = Field(default="", max_length=2000)
    positive_evidence: List[str] = Field(default_factory=list, max_length=100)
    negative_evidence: List[str] = Field(default_factory=list, max_length=100)
    boundary_rules: List[str] = Field(default_factory=list, max_length=100)
    scenes: List[str] = Field(min_length=1, max_length=3)


class TaskCreateRequest(BaseModel):
    period_start: str = Field(pattern=r"^\d{4}-\d{2}-\d{2}$")
    period_end: str = Field(pattern=r"^\d{4}-\d{2}-\d{2}$")
    name: Optional[str] = Field(default=None, max_length=200)


class AccountIdentityRequest(BaseModel):
    platform: str = Field(max_length=32)
    uid: str = Field(min_length=1, max_length=128)
    nickname: str = Field(default="", max_length=200)
    real_name_status: str = Field(default="unknown", max_length=16)


class AccountMutationRequest(BaseModel):
    phone: str = Field(min_length=1, max_length=50)
    operator_name: str = Field(default="", max_length=100)
    account_type: str = Field(default="unknown", max_length=32)
    content_direction: str = Field(default="unknown", max_length=32)
    enabled: bool = True
    platforms: List[AccountIdentityRequest] = Field(default_factory=list, max_length=4)


class ContentMutationRequest(BaseModel):
    platform: str = Field(max_length=32)
    platform_content_id: Optional[str] = Field(default=None, max_length=128)
    canonical_url: str = Field(min_length=1, max_length=3000)
    published_at: Optional[str] = Field(default=None, max_length=50)
    title: str = Field(default="", max_length=1000)
    body: str = Field(default="", max_length=20000)
    content_type: str = Field(default="unknown", max_length=32)
    account_uid: str = Field(default="", max_length=128)
    account_name: str = Field(default="", max_length=200)
    account_type: str = Field(default="unknown", max_length=32)
    content_direction: str = Field(default="unknown", max_length=32)


class BulkImportRequest(BaseModel):
    source_name: str = Field(min_length=1, max_length=300)
    rows: List[Dict[str, Any]] = Field(max_length=10000)


class MediaRetryRequest(BaseModel):
    allow_paid_refresh: bool = False


class MediaProcessingSearchRequest(BaseModel):
    status: Optional[str] = Field(default=None, max_length=32)
    processor_type: Optional[str] = Field(default=None, max_length=40)
    content_id: Optional[int] = Field(default=None, ge=1)
    page: int = Field(default=1, ge=1)
    page_size: int = Field(default=50, ge=1, le=100)


def _legacy_connect() -> sqlite3.Connection:
    connection = sqlite3.connect(LEGACY_DB, timeout=10)
    connection.row_factory = sqlite3.Row
    return connection


def _safe_project_path(value: str) -> Path:
    path = (PROJECT_ROOT / value).resolve()
    root = PROJECT_ROOT.resolve()
    if path != root and root not in path.parents:
        raise HTTPException(status_code=400, detail="文件路径超出项目目录")
    return path


def _legacy_formal_report_path() -> Path:
    with _legacy_connect() as connection:
        row = connection.execute(
            """
            SELECT r.output_path
            FROM formal_baseline b JOIN runs r ON r.id=b.run_id
            WHERE b.singleton_id=1 AND r.status='completed' AND r.report_stale=0
            """
        ).fetchone()
        if row is None:
            row = connection.execute(
                """
                SELECT output_path FROM runs
                WHERE status='completed' AND report_version=? AND report_stale=0
                  AND output_path IS NOT NULL
                ORDER BY updated_at DESC LIMIT 1
                """,
                (LEGACY_REPORT_VERSION,),
            ).fetchone()
    if row is None or not row["output_path"]:
        raise HTTPException(status_code=404, detail="尚无可用的 v7 正式报告")
    path = _safe_project_path(str(row["output_path"]))
    if not path.exists():
        raise HTTPException(status_code=404, detail="v7 正式报告文件不存在")
    return path


def _legacy_report() -> Dict[str, Any]:
    return json.loads(_legacy_formal_report_path().read_text(encoding="utf-8"))


def _legacy_runs(limit: int = 20) -> List[Dict[str, Any]]:
    with _legacy_connect() as connection:
        rows = connection.execute(
            "SELECT * FROM runs ORDER BY created_at DESC LIMIT ?", (limit,)
        ).fetchall()
    return [dict(row) for row in rows]


def _legacy_run(run_id: str) -> Optional[Dict[str, Any]]:
    with _legacy_connect() as connection:
        row = connection.execute("SELECT * FROM runs WHERE id=?", (run_id,)).fetchone()
    return dict(row) if row else None


def _legacy_overview() -> Dict[str, Any]:
    report = _legacy_report()
    channels = report["channels"]
    return {
        "status": "ready",
        "report_version": report["report_version"],
        "rule_version": report["rule_version"],
        "generated_at": report["metadata"]["generated_at"],
        "run_id": report["metadata"]["run_id"],
        "revision": report["metadata"]["revision"],
        "run_summary": report["run_summary"],
        "channels": {
            name: {
                "denominator": channels[name]["denominator"],
                "count_distribution": channels[name]["count_distribution"],
                "verticality": channels[name]["verticality"],
            }
            for name in ("douyin", "xiaohongshu")
        },
        "workflow": {
            "mode": "legacy_v7_read_only",
            "provider_refresh_enabled": False,
            "actual_acquisition_connected": False,
            "formal_baseline": True,
        },
        "recent_runs": _legacy_runs(8),
    }


def _validate_legacy_inputs(channel: str, text: str) -> Dict[str, Any]:
    items = [line.strip() for line in text.replace(",", "\n").splitlines() if line.strip()]
    unique = list(dict.fromkeys(items))
    valid: List[str] = []
    invalid: List[Dict[str, str]] = []
    for item in unique:
        if channel == "douyin":
            accepted = bool(DOUYIN_URL_RE.search(item) or UID_RE.fullmatch(item))
            reason = "不是抖音链接或6-24位数字UID"
        elif channel == "xiaohongshu":
            accepted = bool(XHS_URL_RE.search(item))
            reason = "不是小红书内容链接"
        else:
            accepted = False
            reason = "渠道只支持douyin或xiaohongshu"
        if accepted:
            valid.append(item)
        else:
            invalid.append({"value": item[:200], "reason": reason})
    return {
        "channel": channel,
        "total": len(items),
        "unique": len(unique),
        "valid_count": len(valid),
        "invalid_count": len(invalid),
        "valid": valid,
        "invalid": invalid,
        "can_start": bool(valid) and not invalid,
        "mode": "validation_only",
    }


def _utc_text(value: datetime) -> str:
    return value.astimezone(timezone.utc).isoformat(timespec="seconds").replace("+00:00", "Z")


def _windows(now: Optional[datetime] = None) -> Dict[str, tuple[datetime, datetime]]:
    current = (now or datetime.now(SHANGHAI)).astimezone(SHANGHAI)
    today = datetime.combine(current.date(), time.min, tzinfo=SHANGHAI)
    this_week = today - timedelta(days=today.weekday())
    return {
        "yesterday": (today - timedelta(days=1), today),
        "this_week": (this_week, current),
        "last_week": (this_week - timedelta(days=7), this_week),
    }


def _window_summary(connection: sqlite3.Connection, start: datetime, end: datetime) -> Dict[str, Any]:
    start_utc, end_utc = _utc_text(start), _utc_text(end)
    content_rows = connection.execute(
        """
        SELECT c.id, c.account_id, c.platform, c.manual_content_direction,
               c.evaluation_content_direction, a.content_direction account_content_direction
        FROM content_items c
        LEFT JOIN accounts a ON a.id=c.account_id
        WHERE c.published_at >= ? AND c.published_at < ?
        """,
        (start_utc, end_utc),
    ).fetchall()
    content_ids = [int(row["id"]) for row in content_rows]
    total = len(content_ids)
    active_accounts = len({int(row["account_id"]) for row in content_rows if row["account_id"] is not None})
    if content_ids:
        placeholders = ",".join("?" for _ in content_ids)
        evaluations = connection.execute(
            f"""
            SELECT ev.*, sp.tier primary_tier FROM evaluation_versions ev
            JOIN (
                SELECT content_id, MAX(id) id
                FROM evaluation_versions
                WHERE content_id IN ({placeholders}) AND invalidated_at IS NULL
                  AND rule_version=?
                  AND taxonomy_version=(
                      SELECT version FROM taxonomy_versions
                      WHERE status='published'
                      ORDER BY published_at DESC, created_at DESC LIMIT 1
                  )
                GROUP BY content_id
            ) latest ON latest.id=ev.id
            LEFT JOIN taxonomy_versions tv ON tv.version=ev.taxonomy_version
            LEFT JOIN selling_points sp
              ON sp.taxonomy_id=tv.id AND sp.code=ev.primary_selling_point_code
            """,
            [*content_ids, RULE_VERSION],
        ).fetchall()
        metrics = connection.execute(
            f"""
            SELECT ms.* FROM content_metric_snapshots ms
            WHERE ms.content_id IN ({placeholders})
              AND ms.id=(
                  SELECT ms2.id FROM content_metric_snapshots ms2
                  WHERE ms2.content_id=ms.content_id
                  ORDER BY ms2.captured_at DESC, ms2.id DESC LIMIT 1
              )
            """,
            content_ids,
        ).fetchall()
        duplicate_count = int(
            connection.execute(
                f"""
                SELECT COUNT(DISTINCT duplicate_content_id) FROM duplicate_relations
                WHERE status='confirmed' AND duplicate_content_id IN ({placeholders})
                """,
                content_ids,
            ).fetchone()[0]
        )
        fingerprint_count = int(
            connection.execute(
                f"""
                SELECT COUNT(DISTINCT content_id) FROM duplicate_fingerprints
                WHERE fingerprint_version=? AND content_id IN ({placeholders})
                """,
                [FINGERPRINT_VERSION, *content_ids],
            ).fetchone()[0]
        )
        duplicate_calibrated = connection.execute(
            """
            SELECT 1 FROM duplicate_calibration_runs
            WHERE fingerprint_version=? AND thresholds_json=? AND status='passed' LIMIT 1
            """,
            (
                FINGERPRINT_VERSION,
                json.dumps(THRESHOLDS, ensure_ascii=False, sort_keys=True, separators=(",", ":")),
            ),
        ).fetchone() is not None
    else:
        evaluations, metrics = [], []
        duplicate_count = fingerprint_count = 0
        duplicate_calibrated = False
    evaluation_values = [dict(row) for row in evaluations]
    metric_values = [dict(row) for row in metrics]
    evaluation_by_content = {
        int(row["content_id"]): row for row in evaluation_values
    }
    metric_by_content = {int(row["content_id"]): row for row in metric_values}
    conclusion_rows: List[Dict[str, Any]] = []
    for content_row in content_rows:
        content = dict(content_row)
        content_id = int(content["id"])
        evaluation = evaluation_by_content.get(content_id, {})
        metric = metric_by_content.get(content_id, {})
        conclusion_rows.append(
            {
                "content_id": content_id,
                "platform": str(content["platform"]),
                "content_direction": str(
                    content.get("manual_content_direction")
                    or evaluation.get("content_direction")
                    or content.get("evaluation_content_direction")
                    or content.get("account_content_direction")
                    or "unknown"
                ),
                "evidence_level": evaluation.get("evidence_level"),
                "selling_point_included": bool(evaluation.get("selling_point_included")),
                "primary_tier": evaluation.get("primary_tier"),
                "content_automotive_score": evaluation.get("content_automotive_score"),
                "audience_automotive_score": evaluation.get("audience_automotive_score"),
                "acquisition_potential_score": evaluation.get("acquisition_potential_score"),
                "view_count": metric.get("view_count"),
            }
        )
    eligible = sum(1 for row in evaluation_values if row["evidence_level"] in {"V2", "V3"})
    vertical = sum(
        1 for row in evaluation_values
        if row["evidence_level"] in {"V2", "V3"}
        and row["content_automotive_score"] is not None
        and int(row["content_automotive_score"]) >= 60
    )
    selling = sum(1 for row in evaluation_values if int(row["selling_point_included"]) == 1)
    metric_coverage = round(len(metric_values) * 100 / total, 2) if total else None
    views = sum(int(row["view_count"] or 0) for row in metric_values)
    comments = sum(int(row["comment_count"] or 0) for row in metric_values if row["comment_count"] is not None)
    ratio_status = "not_applicable" if total == 0 else "available" if eligible * 100 / total >= 95 else "below_threshold"
    duplicate_coverage = round(fingerprint_count * 100 / total, 2) if total else None
    duplicate_status = (
        "not_applicable" if total == 0 else "available"
        if duplicate_calibrated and (duplicate_coverage or 0) >= 95 else "below_threshold"
    )
    return {
        "period_start": start.isoformat(),
        "period_end": end.isoformat(),
        "eligible_count": eligible,
        "unassociated_content_count": sum(1 for row in content_rows if row["account_id"] is None),
        "metrics": {
            "publication_count": quantity_metric(total, unit="content", status="available"),
            "active_account_count": quantity_metric(active_accounts, unit="account", status="available"),
            "view_count": quantity_metric(
                views if metric_values else None,
                unit="view",
                status="not_applicable" if total == 0 else "missing" if not metric_values else "available",
                coverage_percentage=metric_coverage,
            ),
            "comment_count": quantity_metric(
                comments if metric_values and any(row["comment_count"] is not None for row in metric_values) else None,
                unit="comment",
                status="not_applicable" if total == 0 else "missing",
                coverage_percentage=metric_coverage,
                reason="历史 valid_unique_commenters 不等于评论量" if total else "",
            ),
            "verticality_rate": ratio_metric(
                vertical if total else None, total, status=ratio_status,
                eligible_count=eligible,
                coverage_percentage=round(eligible * 100 / total, 2) if total else None,
            ),
            "selling_point_coverage_rate": ratio_metric(
                selling if total else None, total, status=ratio_status,
                eligible_count=eligible,
                coverage_percentage=round(eligible * 100 / total, 2) if total else None,
            ),
            "duplicate_rate": ratio_metric(
                duplicate_count if total else None,
                total,
                status=duplicate_status,
                eligible_count=fingerprint_count,
                coverage_percentage=duplicate_coverage,
                reason=(
                    "感知指纹定标未通过或覆盖不足，重复率暂不发布"
                    if duplicate_status == "below_threshold" else ""
                ),
            ),
            "estimated_new_users": quantity_metric(None, unit="person", status="not_applicable" if total == 0 else "not_calculable", reason="首版没有已验证模型"),
            "estimated_reactivated_users": quantity_metric(None, unit="person", status="not_applicable" if total == 0 else "not_calculable", reason="首版没有已验证模型"),
            "estimated_leads": quantity_metric(None, unit="lead", status="not_applicable" if total == 0 else "not_calculable", reason="首版没有已验证模型"),
        },
        "channels": build_channel_conclusions(conclusion_rows),
        "empty_explanation": (
            "所选窗口没有进入有效监控范围的已发布内容，并非抓取故障。"
            if total == 0 else ""
        ),
    }


def v8_overview() -> Dict[str, Any]:
    with connect(API_DB_PATH) as connection:
        windows = {
            key: _window_summary(connection, start, end)
            for key, (start, end) in _windows().items()
        }
        quality = {
            "missing_published_at": int(
                connection.execute("SELECT COUNT(*) FROM content_items WHERE published_at IS NULL").fetchone()[0]
            ),
            "pending_reviews": int(
                connection.execute("SELECT COUNT(*) FROM review_queue WHERE status IN ('pending','manual_required')").fetchone()[0]
            ),
            "terminal_reviews": int(
                connection.execute("SELECT COUNT(*) FROM review_queue WHERE status='terminal_failed'").fetchone()[0]
            ),
            "duplicate_fingerprint_coverage": round(
                int(connection.execute(
                    "SELECT COUNT(DISTINCT content_id) FROM duplicate_fingerprints WHERE fingerprint_version=?",
                    (FINGERPRINT_VERSION,),
                ).fetchone()[0]) * 100
                / max(1, int(connection.execute("SELECT COUNT(*) FROM content_items").fetchone()[0])),
                2,
            ),
            "duplicate_calibration_ready": connection.execute(
                """
                SELECT 1 FROM duplicate_calibration_runs
                WHERE fingerprint_version=? AND thresholds_json=? AND status='passed' LIMIT 1
                """,
                (
                    FINGERPRINT_VERSION,
                    json.dumps(THRESHOLDS, ensure_ascii=False, sort_keys=True, separators=(",", ":")),
                ),
            ).fetchone() is not None,
            "confirmed_duplicate_count": int(
                connection.execute(
                    "SELECT COUNT(DISTINCT duplicate_content_id) FROM duplicate_relations WHERE status='confirmed'"
                ).fetchone()[0]
            ),
        }
    return {
        "status": "ready",
        "report_version": CURRENT_REPORT_VERSION,
        "generated_at": now_utc(),
        "timezone": "Asia/Shanghai",
        "windows": windows,
        "data_quality": quality,
    }


def _account_search(payload: AccountSearchRequest) -> Dict[str, Any]:
    where: List[str] = []
    parameters: List[Any] = []
    if payload.query:
        where.append(
            "(a.phone LIKE ? OR a.phone_normalized LIKE ? OR a.operator_name LIKE ? OR "
            "EXISTS (SELECT 1 FROM account_platform_identities i "
            "WHERE i.account_id=a.id AND (i.uid LIKE ? OR i.nickname LIKE ?)))"
        )
        pattern = f"%{payload.query}%"
        phone_digits = re.sub(r"\D", "", payload.query)
        normalized_pattern = f"%{phone_digits}%" if phone_digits else pattern
        parameters.extend([pattern, normalized_pattern, pattern, pattern, pattern])
    if payload.account_type:
        where.append("a.account_type=?")
        parameters.append(payload.account_type)
    if payload.content_direction:
        where.append("a.content_direction=?")
        parameters.append(payload.content_direction)
    if payload.platform:
        where.append(
            "EXISTS (SELECT 1 FROM account_platform_identities i "
            "WHERE i.account_id=a.id AND i.platform=?)"
        )
        parameters.append(payload.platform)
    where_sql = f"WHERE {' AND '.join(where)}" if where else ""
    offset = (payload.page - 1) * payload.page_size
    with connect(API_DB_PATH) as connection:
        total = int(
            connection.execute(
                f"SELECT COUNT(*) FROM accounts a {where_sql}", parameters
            ).fetchone()[0]
        )
        account_rows = connection.execute(
            f"""
            SELECT a.* FROM accounts a {where_sql}
            ORDER BY a.updated_at DESC, a.id DESC
            LIMIT ? OFFSET ?
            """,
            [*parameters, payload.page_size, offset],
        ).fetchall()
        items: List[Dict[str, Any]] = []
        for row in account_rows:
            identities = connection.execute(
                """
                SELECT platform, uid, nickname, real_name_status
                FROM account_platform_identities
                WHERE account_id=? ORDER BY platform
                """,
                (row["id"],),
            ).fetchall()
            items.append(
                {
                    "id": row["id"],
                    "phone": row["phone"],
                    "operator_name": row["operator_name"],
                    "account_type": row["account_type"],
                    "content_direction": row["content_direction"],
                    "enabled": bool(row["enabled"]),
                    "platforms": [dict(identity) for identity in identities],
                    "updated_at": row["updated_at"],
                }
            )
        legacy_unassociated = int(
            connection.execute(
                "SELECT COUNT(*) FROM content_items WHERE account_id IS NULL"
            ).fetchone()[0]
        )
        pending_identities = connection.execute(
            """
            SELECT platform, uid, nickname, content_count,
                   first_published_at, last_published_at
            FROM pending_platform_identities
            ORDER BY content_count DESC, platform, uid
            LIMIT 100
            """
        ).fetchall()
        pending_identity_count = int(
            connection.execute(
                "SELECT COUNT(*) FROM pending_platform_identities"
            ).fetchone()[0]
        )
    return {
        "items": items,
        "total": total,
        "page": payload.page,
        "page_size": payload.page_size,
        "legacy_unassociated_content_count": legacy_unassociated,
        "pending_platform_identity_count": pending_identity_count,
        "pending_platform_identities": [dict(row) for row in pending_identities],
    }


def _content_search(payload: ContentSearchRequest) -> Dict[str, Any]:
    where: List[str] = []
    parameters: List[Any] = []
    if payload.query:
        where.append(
            "(c.link_id LIKE ? OR c.title LIKE ? OR c.raw_account_uid LIKE ? "
            "OR c.raw_account_name LIKE ? OR c.canonical_url LIKE ?)"
        )
        pattern = f"%{payload.query}%"
        parameters.extend([pattern] * 5)
    if payload.platform:
        where.append("c.platform=?")
        parameters.append(payload.platform)
    if payload.account_type:
        where.append("COALESCE(a.account_type, c.legacy_account_type, 'unknown')=?")
        parameters.append(payload.account_type)
    if payload.content_direction:
        where.append(
            "COALESCE(c.manual_content_direction, c.evaluation_content_direction, "
            "ev.content_direction, a.content_direction, 'unknown')=?"
        )
        parameters.append(payload.content_direction)
    if payload.review_status == "pending":
        where.append("rq.pending_count > 0")
    elif payload.review_status == "terminal_failed":
        where.append("rq.terminal_count > 0")
    elif payload.review_status == "resolved":
        where.append("rq.pending_count = 0 AND rq.terminal_count = 0")
    where_sql = f"WHERE {' AND '.join(where)}" if where else ""
    from_sql = """
        FROM content_items c
        LEFT JOIN accounts a ON a.id=c.account_id
        LEFT JOIN evaluation_versions ev ON ev.id=(
            SELECT ev2.id FROM evaluation_versions ev2
            WHERE ev2.content_id=c.id AND ev2.invalidated_at IS NULL
            ORDER BY ev2.evaluated_at DESC, ev2.id DESC LIMIT 1
        )
        LEFT JOIN (
            SELECT content_id,
                   SUM(CASE WHEN status IN ('pending','manual_required','in_review') THEN 1 ELSE 0 END) pending_count,
                   SUM(CASE WHEN status='terminal_failed' THEN 1 ELSE 0 END) terminal_count
            FROM review_queue GROUP BY content_id
        ) rq ON rq.content_id=c.id
        LEFT JOIN review_queue current_rq ON current_rq.id=(
            SELECT rq2.id FROM review_queue rq2
            WHERE rq2.content_id=c.id
            ORDER BY CASE rq2.status
                WHEN 'in_review' THEN 0 WHEN 'pending' THEN 1
                WHEN 'manual_required' THEN 2 WHEN 'terminal_failed' THEN 3 ELSE 4 END,
                rq2.priority DESC, rq2.id DESC LIMIT 1
        )
        LEFT JOIN content_metric_snapshots ms ON ms.id=(
            SELECT ms2.id FROM content_metric_snapshots ms2
            WHERE ms2.content_id=c.id ORDER BY ms2.captured_at DESC, ms2.id DESC LIMIT 1
        )
        LEFT JOIN duplicate_relations duplicate ON duplicate.id=(
            SELECT d2.id FROM duplicate_relations d2
            WHERE d2.duplicate_content_id=c.id AND d2.status='confirmed'
            ORDER BY d2.id LIMIT 1
        )
        LEFT JOIN content_items original ON original.id=duplicate.original_content_id
    """
    offset = (payload.page - 1) * payload.page_size
    with connect(API_DB_PATH) as connection:
        total = int(
            connection.execute(
                f"SELECT COUNT(*) {from_sql} {where_sql}", parameters
            ).fetchone()[0]
        )
        rows = connection.execute(
            f"""
            SELECT c.id, c.link_id, c.platform, c.platform_content_id,
                   c.canonical_url, c.published_at, c.title, c.body, c.content_type,
                   c.raw_account_uid, c.raw_account_name,
                   COALESCE(a.account_type, c.legacy_account_type, 'unknown') account_type,
                   COALESCE(c.manual_content_direction, c.evaluation_content_direction,
                            ev.content_direction, a.content_direction, 'unknown') content_direction,
                   ev.primary_selling_point_code, ev.evidence_level,
                   ev.content_automotive_score, ev.pending_review,
                   ms.view_count, ms.comment_count, ms.like_count, ms.share_count,
                   ms.collect_count, ms.captured_at metrics_captured_at,
                   original.link_id duplicate_original_link_id,
                   current_rq.id review_queue_id, current_rq.status review_status,
                   COALESCE(rq.pending_count, 0) pending_review_count,
                   COALESCE(rq.terminal_count, 0) terminal_review_count
            {from_sql} {where_sql}
            ORDER BY c.published_at IS NULL, c.published_at DESC, c.id DESC
            LIMIT ? OFFSET ?
            """,
            [*parameters, payload.page_size, offset],
        ).fetchall()
    return {
        "items": [dict(row) for row in rows],
        "total": total,
        "page": payload.page,
        "page_size": payload.page_size,
    }


def _selling_point_list() -> Dict[str, Any]:
    with connect(API_DB_PATH) as connection:
        taxonomy = connection.execute(
            """
            SELECT * FROM taxonomy_versions WHERE status='published'
            ORDER BY published_at DESC, created_at DESC LIMIT 1
            """
        ).fetchone()
        if taxonomy is None:
            return {"taxonomy": None, "items": []}
        rows = connection.execute(
            """
            SELECT sp.*,
                   COUNT(DISTINCT CASE WHEN em.match_role='primary' THEN ev.content_id END) primary_hits,
                   COUNT(DISTINCT ev.content_id) total_hits
            FROM selling_points sp
            LEFT JOIN evaluation_matches em ON em.selling_point_code=sp.code
            LEFT JOIN evaluation_versions ev
              ON ev.id=em.evaluation_id AND ev.invalidated_at IS NULL
            WHERE sp.taxonomy_id=?
            GROUP BY sp.id
            ORDER BY substr(sp.code, 1, 1), CAST(substr(sp.code, 2) AS INTEGER)
            """,
            (taxonomy["id"],),
        ).fetchall()
        items: List[Dict[str, Any]] = []
        for row in rows:
            scenes = connection.execute(
                "SELECT scene FROM selling_point_scenes WHERE selling_point_id=? ORDER BY scene",
                (row["id"],),
            ).fetchall()
            items.append(
                {
                    "code": row["code"],
                    "tier": row["tier"],
                    "label": row["label"],
                    "definition": row["definition"],
                    "scenes": [scene["scene"] for scene in scenes],
                    "enabled": bool(row["enabled"]),
                    "primary_hits": row["primary_hits"],
                    "total_hits": row["total_hits"],
                }
            )
    return {
        "taxonomy": {
            "id": taxonomy["id"],
            "version": taxonomy["version"],
            "status": taxonomy["status"],
            "published_at": taxonomy["published_at"],
        },
        "items": items,
    }


def _read_local_json(local_path: str) -> Dict[str, Any]:
    path = _safe_project_path(local_path)
    if not path.is_file() or path.suffix.lower() != ".json":
        return {}
    try:
        value = json.loads(path.read_text(encoding="utf-8"))
    except (OSError, json.JSONDecodeError):
        return {}
    return value if isinstance(value, dict) else {}


def _artifact_media_paths(row: sqlite3.Row) -> List[Path]:
    path = _safe_project_path(str(row["local_path"]))
    if path.suffix.lower() != ".json":
        return [path] if path.is_file() else []
    value = _read_local_json(str(row["local_path"]))
    candidates: List[str] = []
    if value.get("video_path"):
        candidates.append(str(value["video_path"]))
    candidates.extend(
        str(item) for item in value.get("image_paths", []) if isinstance(item, str)
    )
    paths: List[Path] = []
    for candidate in candidates:
        resolved = _safe_project_path(candidate)
        if resolved.is_file():
            paths.append(resolved)
    return paths


def _content_evidence(content_id: int) -> Dict[str, Any]:
    with connect(API_DB_PATH) as connection:
        content = connection.execute(
            "SELECT * FROM content_items WHERE id=?", (content_id,)
        ).fetchone()
        if content is None:
            raise HTTPException(status_code=404, detail="内容不存在")
        evaluation = connection.execute(
            """
            SELECT * FROM evaluation_versions
            WHERE content_id=? AND invalidated_at IS NULL
            ORDER BY evaluated_at DESC, id DESC LIMIT 1
            """,
            (content_id,),
        ).fetchone()
        artifact_rows = connection.execute(
            """
            SELECT * FROM evidence_artifacts
            WHERE content_id=? AND status='available'
            ORDER BY id DESC
            """,
            (content_id,),
        ).fetchall()
        latest: Dict[str, sqlite3.Row] = {}
        for row in artifact_rows:
            latest.setdefault(str(row["artifact_type"]), row)
        media_row = next(
            (latest[kind] for kind in ("media", "media_manifest") if kind in latest),
            None,
        )
        asr_row = next(
            (
                latest[kind]
                for kind in ("asr", "transcript", "media_transcript")
                if kind in latest
            ),
            None,
        )
        ocr_row = next(
            (latest[kind] for kind in ("ocr", "media_ocr") if kind in latest),
            None,
        )
        comment_version = connection.execute(
            """
            SELECT * FROM comment_evidence_versions
            WHERE content_id=? ORDER BY captured_at DESC, id DESC LIMIT 1
            """,
            (content_id,),
        ).fetchone()
        comment_rows = [] if comment_version is None else connection.execute(
            """
            SELECT body, like_count, published_at FROM comments
            WHERE evidence_version_id=?
            ORDER BY COALESCE(like_count,0) DESC, id LIMIT 20
            """,
            (comment_version["id"],),
        ).fetchall()
        stored_comment_count = 0 if comment_version is None else int(
            connection.execute(
                "SELECT COUNT(*) FROM comments WHERE evidence_version_id=?",
                (comment_version["id"],),
            ).fetchone()[0]
        )
        processing = connection.execute(
            """
            SELECT id,processor_type,processor_version,status,attempt_count,
                   error_message,updated_at
            FROM media_processing_slots WHERE content_id=?
            ORDER BY updated_at DESC,id DESC
            """,
            (content_id,),
        ).fetchall()
        review = connection.execute(
            """
            SELECT id,reason_code,status,priority,assigned_to,updated_at
            FROM review_queue WHERE content_id=?
            ORDER BY CASE status WHEN 'in_review' THEN 0 WHEN 'pending' THEN 1
                WHEN 'manual_required' THEN 2 ELSE 3 END, id DESC LIMIT 1
            """,
            (content_id,),
        ).fetchone()
    media_items: List[Dict[str, Any]] = []
    if media_row is not None:
        for index, path in enumerate(_artifact_media_paths(media_row)):
            suffix = path.suffix.lower()
            media_items.append(
                {
                    "artifact_id": int(media_row["id"]),
                    "index": index,
                    "kind": "video" if suffix in {".mp4", ".mov", ".m4v", ".webm"} else "image",
                    "name": path.name,
                    "url": f"/api/v8/contents/{content_id}/evidence/files/{media_row['id']}/{index}",
                }
            )
    asr_payload = _read_local_json(str(asr_row["local_path"])) if asr_row else {}
    ocr_payload = _read_local_json(str(ocr_row["local_path"])) if ocr_row else {}
    evaluation_payload = (
        json.loads(str(evaluation["payload_json"])) if evaluation is not None else None
    )
    return {
        "content": {
            key: content[key]
            for key in (
                "id", "link_id", "platform", "canonical_url", "title", "body",
                "content_type", "published_at", "raw_account_uid", "raw_account_name",
            )
        },
        "base_evaluation_id": int(evaluation["id"]) if evaluation is not None else None,
        "evaluation": evaluation_payload,
        "media": media_items,
        "asr": {
            "status": asr_payload.get("status") or ("missing" if asr_row is None else "available"),
            "model": asr_payload.get("model"),
            "text": asr_payload.get("text") or "",
        },
        "ocr": {
            "status": ocr_payload.get("status") or ("missing" if ocr_row is None else "available"),
            "observation_count": ocr_payload.get("ocr_observation_count")
            or ocr_payload.get("source_count") or 0,
            "text": ocr_payload.get("combined_text") or "",
        },
        "comments": {
            "status": str(comment_version["status"]) if comment_version else "missing",
            "captured_at": comment_version["captured_at"] if comment_version else None,
            "declared_count": comment_version["comment_count"] if comment_version else None,
            "stored_count": stored_comment_count,
            "top_items": [dict(row) for row in comment_rows],
        },
        "processing_slots": [dict(row) for row in processing],
        "review": dict(review) if review is not None else None,
    }


def _recover_interrupted_tasks() -> int:
    with connect(API_DB_PATH) as connection:
        cursor = connection.execute(
            """
            UPDATE report_tasks
            SET task_status='interrupted', message='服务重启，任务已中断，可重试', updated_at=?
            WHERE task_status IN ('running','cancel_requested')
            """,
            (now_utc(),),
        )
        connection.commit()
        return int(cursor.rowcount)


@asynccontextmanager
async def lifespan(app: FastAPI):
    with connect(API_DB_PATH) as connection:
        initialize_database(connection)
    app.state.recovered_tasks = _recover_interrupted_tasks()
    scheduler: Optional[BackgroundScheduler] = None
    if os.environ.get("DCAR_SCHEDULER_ENABLED") == "1":
        scheduler = BackgroundScheduler(timezone="Asia/Shanghai")
        install_jobs(scheduler, db_path=API_DB_PATH, reports_root=API_REPORTS_ROOT)
        app.state.catchup_results = startup_catchup(
            db_path=API_DB_PATH, reports_root=API_REPORTS_ROOT
        )
        scheduler.start()
    else:
        app.state.catchup_results = []
    app.state.scheduler = scheduler
    try:
        yield
    finally:
        if scheduler is not None:
            scheduler.shutdown(wait=False)


app = FastAPI(title="DCar Insight API", version="8.2", lifespan=lifespan)
app.add_middleware(
    CORSMiddleware,
    allow_origin_regex=r"http://(?:localhost|127\.0\.0\.1):\d{2,5}",
    allow_credentials=False,
    allow_methods=["GET", "POST", "PATCH", "DELETE", "OPTIONS"],
    allow_headers=["Content-Type"],
)


@app.middleware("http")
async def privacy_safe_request_log(request: Request, call_next):
    response = await call_next(request)
    LOGGER.info("%s %s %s", request.method, request.url.path, response.status_code)
    return response


@app.get("/api/v8/health")
def v8_health() -> Dict[str, Any]:
    return {
        "status": "ok",
        "mode": "local_v8",
        "report_version": CURRENT_REPORT_VERSION,
        "database": API_DB_PATH.name,
    }


@app.get("/api/v8/overview")
def get_v8_overview() -> Dict[str, Any]:
    return v8_overview()


@app.get("/api/v8/tasks")
def get_v8_tasks() -> Dict[str, Any]:
    with connect(API_DB_PATH) as connection:
        rows = connection.execute(
            """
            SELECT t.*,
                   (SELECT COUNT(*) FROM task_contents tc WHERE tc.task_id=t.id AND tc.inclusion_status='included') content_count,
                   (SELECT COUNT(*) FROM task_contents tc WHERE tc.task_id=t.id AND tc.inclusion_status='excluded_missing_boundary') missing_boundary_count,
                   (SELECT COUNT(*) FROM report_revisions rr WHERE rr.task_id=t.id) revision_count
            FROM report_tasks t
            ORDER BY t.created_at DESC, t.id DESC
            """
        ).fetchall()
    return {"items": [dict(row) for row in rows], "total": len(rows)}


@app.get("/api/v8/scheduler")
def get_v8_scheduler_status() -> Dict[str, Any]:
    with connect(API_DB_PATH) as connection:
        rows = connection.execute(
            """
            SELECT sr.* FROM scheduler_runs sr
            JOIN (
                SELECT job_id, MAX(scheduled_for) scheduled_for
                FROM scheduler_runs GROUP BY job_id
            ) latest ON latest.job_id=sr.job_id AND latest.scheduled_for=sr.scheduled_for
            ORDER BY sr.job_id
            """
        ).fetchall()
    return {
        "enabled": os.environ.get("DCAR_SCHEDULER_ENABLED") == "1",
        "jobs": [dict(row) for row in rows],
    }


@app.post("/api/v8/tasks")
def create_v8_task(payload: TaskCreateRequest) -> Dict[str, Any]:
    try:
        return create_and_run_task(
            task_type="custom",
            period_start=payload.period_start,
            period_end=payload.period_end,
            creation_source="manual",
            name=payload.name,
            db_path=API_DB_PATH,
            reports_root=API_REPORTS_ROOT,
        )
    except ReportTaskError as exc:
        raise HTTPException(status_code=409, detail=str(exc)) from exc


@app.get("/api/v8/tasks/{task_id}")
def get_v8_task(task_id: str) -> Dict[str, Any]:
    try:
        return get_task(task_id, db_path=API_DB_PATH)
    except ReportTaskError as exc:
        raise HTTPException(status_code=404, detail=str(exc)) from exc


@app.post("/api/v8/tasks/{task_id}/retry")
def retry_v8_task(task_id: str) -> Dict[str, Any]:
    try:
        retry_task(task_id, db_path=API_DB_PATH)
        run_task(task_id, db_path=API_DB_PATH, reports_root=API_REPORTS_ROOT)
        return get_task(task_id, db_path=API_DB_PATH)
    except ReportTaskError as exc:
        raise HTTPException(status_code=409, detail=str(exc)) from exc


@app.post("/api/v8/tasks/{task_id}/cancel")
def cancel_v8_task(task_id: str) -> Dict[str, Any]:
    try:
        return request_task_cancel(task_id, db_path=API_DB_PATH)
    except ReportTaskError as exc:
        raise HTTPException(status_code=409, detail=str(exc)) from exc


@app.post("/api/v8/tasks/{task_id}/resume")
def resume_v8_task(task_id: str) -> Dict[str, Any]:
    try:
        resume_task(task_id, db_path=API_DB_PATH)
        run_task(task_id, db_path=API_DB_PATH, reports_root=API_REPORTS_ROOT)
        return get_task(task_id, db_path=API_DB_PATH)
    except ReportTaskError as exc:
        raise HTTPException(status_code=409, detail=str(exc)) from exc


@app.get("/api/v8/tasks/{task_id}/revisions/{revision}/report")
def get_v8_task_report(task_id: str, revision: int) -> Dict[str, Any]:
    with connect(API_DB_PATH) as connection:
        row = connection.execute(
            "SELECT report_json_path FROM report_revisions WHERE task_id=? AND revision=?",
            (task_id, revision),
        ).fetchone()
    if row is None:
        raise HTTPException(status_code=404, detail="报告 revision 不存在")
    path = _safe_project_path(str(row["report_json_path"]))
    if not path.is_file():
        raise HTTPException(status_code=410, detail="报告文件已登记但本地缺失")
    return json.loads(path.read_text(encoding="utf-8"))


@app.get("/api/v8/tasks/{task_id}/revisions/{revision}/files/{file_kind}")
def download_v8_task_file(task_id: str, revision: int, file_kind: str) -> FileResponse:
    kinds = ["summary-png", "summary-svg"] if file_kind == "summary-image" else [file_kind]
    placeholders = ",".join("?" for _ in kinds)
    with connect(API_DB_PATH) as connection:
        rows = connection.execute(
            f"""
            SELECT * FROM report_files
            WHERE task_id=? AND revision=? AND file_kind IN ({placeholders}) AND status='available'
            """,
            (task_id, revision, *kinds),
        ).fetchall()
    by_kind = {str(row["file_kind"]): row for row in rows}
    row = next((by_kind[kind] for kind in kinds if kind in by_kind), None)
    if row is None:
        raise HTTPException(status_code=404, detail="报告文件不存在")
    path = _safe_project_path(str(row["local_path"]))
    if not path.is_file():
        raise HTTPException(status_code=410, detail="报告文件已登记但本地缺失")
    media_types = {
        "report-json": "application/json", "report-markdown": "text/markdown",
        "content-csv": "text/csv", "summary-svg": "image/svg+xml", "summary-png": "image/png",
    }
    return FileResponse(
        path,
        media_type=media_types.get(str(row["file_kind"]), "application/octet-stream"),
        filename=path.name,
    )


@app.post("/api/v8/accounts/search")
def search_v8_accounts(payload: AccountSearchRequest) -> Dict[str, Any]:
    return _account_search(payload)


@app.post("/api/v8/accounts")
def create_v8_account(payload: AccountMutationRequest) -> Dict[str, Any]:
    try:
        return upsert_account(payload.model_dump(), db_path=API_DB_PATH)
    except OperationError as exc:
        raise HTTPException(status_code=409, detail=str(exc)) from exc


@app.patch("/api/v8/accounts/{account_id}")
def patch_v8_account(account_id: int, payload: AccountMutationRequest) -> Dict[str, Any]:
    try:
        return update_account(account_id, payload.model_dump(), db_path=API_DB_PATH)
    except OperationError as exc:
        raise HTTPException(status_code=409, detail=str(exc)) from exc


@app.post("/api/v8/accounts/import")
def import_v8_accounts(payload: BulkImportRequest) -> Dict[str, Any]:
    return import_accounts(payload.rows, source_name=payload.source_name, db_path=API_DB_PATH)


@app.get("/api/v8/accounts/export")
def export_v8_accounts() -> Response:
    return Response(
        export_accounts_csv(db_path=API_DB_PATH),
        media_type="text/csv; charset=utf-8",
        headers={"Content-Disposition": 'attachment; filename="dcar-accounts.csv"'},
    )


@app.post("/api/v8/contents/search")
def search_v8_contents(payload: ContentSearchRequest) -> Dict[str, Any]:
    return _content_search(payload)


@app.post("/api/v8/contents/validate")
def validate_v8_contents(payload: BulkImportRequest) -> Dict[str, Any]:
    items: List[Dict[str, Any]] = []
    valid = 0
    for index, row in enumerate(payload.rows, start=1):
        try:
            identity = content_identity(
                str(row.get("platform") or ""),
                str(row.get("canonical_url") or row.get("url") or ""),
                row.get("platform_content_id"),
            )
            items.append({"row": index, "status": "valid", **identity})
            valid += 1
        except OperationError as exc:
            items.append({"row": index, "status": "rejected", "reason": str(exc)})
    return {"total": len(payload.rows), "valid": valid, "rejected": len(payload.rows) - valid, "items": items}


@app.post("/api/v8/contents")
def create_v8_content(payload: ContentMutationRequest) -> Dict[str, Any]:
    try:
        return upsert_content(payload.model_dump(), db_path=API_DB_PATH)
    except OperationError as exc:
        raise HTTPException(status_code=409, detail=str(exc)) from exc


@app.patch("/api/v8/contents/{content_id}")
def patch_v8_content(content_id: int, payload: ContentMutationRequest) -> Dict[str, Any]:
    try:
        return update_content(content_id, payload.model_dump(), db_path=API_DB_PATH)
    except OperationError as exc:
        raise HTTPException(status_code=409, detail=str(exc)) from exc


@app.post("/api/v8/contents/import")
def import_v8_contents(payload: BulkImportRequest) -> Dict[str, Any]:
    return import_contents(payload.rows, source_name=payload.source_name, db_path=API_DB_PATH)


@app.post("/api/v8/contents/{content_id}/update-data")
def update_v8_content_data(content_id: int) -> Dict[str, Any]:
    try:
        return update_content_data(content_id, db_path=API_DB_PATH)
    except ProviderConfigurationError as exc:
        raise HTTPException(status_code=409, detail=str(exc)) from exc


@app.get("/api/v8/contents/{content_id}/evidence")
def get_v8_content_evidence(content_id: int) -> Dict[str, Any]:
    return _content_evidence(content_id)


@app.get("/api/v8/contents/{content_id}/evidence/files/{artifact_id}/{index}")
def get_v8_content_evidence_file(
    content_id: int, artifact_id: int, index: int
) -> FileResponse:
    with connect(API_DB_PATH) as connection:
        row = connection.execute(
            """
            SELECT * FROM evidence_artifacts
            WHERE id=? AND content_id=? AND artifact_type IN ('media','media_manifest')
              AND status='available'
            """,
            (artifact_id, content_id),
        ).fetchone()
    if row is None:
        raise HTTPException(status_code=404, detail="媒体证据不存在")
    paths = _artifact_media_paths(row)
    if index < 0 or index >= len(paths):
        raise HTTPException(status_code=404, detail="媒体证据文件不存在")
    return FileResponse(paths[index])


@app.post("/api/v8/contents/{content_id}/media/retry")
def retry_v8_content_media(
    content_id: int, payload: MediaRetryRequest
) -> Dict[str, Any]:
    try:
        return retry_content_media(
            content_id,
            allow_paid_refresh=payload.allow_paid_refresh,
            db_path=API_DB_PATH,
        )
    except (
        ProviderConfigurationError,
        MediaProcessingError,
        CaptureError,
        SlotUnavailable,
        BudgetBlocked,
    ) as exc:
        raise HTTPException(status_code=409, detail=str(exc)) from exc


@app.post("/api/v8/media-processing/search")
def search_v8_media_processing(
    payload: MediaProcessingSearchRequest,
) -> Dict[str, Any]:
    where: List[str] = []
    parameters: List[Any] = []
    if payload.status:
        where.append("m.status=?")
        parameters.append(payload.status)
    if payload.processor_type:
        where.append("m.processor_type=?")
        parameters.append(payload.processor_type)
    if payload.content_id:
        where.append("m.content_id=?")
        parameters.append(payload.content_id)
    where_sql = f"WHERE {' AND '.join(where)}" if where else ""
    offset = (payload.page - 1) * payload.page_size
    with connect(API_DB_PATH) as connection:
        total = int(
            connection.execute(
                f"SELECT COUNT(*) FROM media_processing_slots m {where_sql}", parameters
            ).fetchone()[0]
        )
        rows = connection.execute(
            f"""
            SELECT m.id,m.content_id,c.link_id,c.platform,m.processor_type,
                   m.processor_version,m.status,m.attempt_count,m.error_message,m.updated_at
            FROM media_processing_slots m
            JOIN content_items c ON c.id=m.content_id
            {where_sql}
            ORDER BY m.updated_at DESC,m.id DESC LIMIT ? OFFSET ?
            """,
            [*parameters, payload.page_size, offset],
        ).fetchall()
    return {
        "items": [dict(row) for row in rows],
        "total": total,
        "page": payload.page,
        "page_size": payload.page_size,
    }


@app.get("/api/v8/contents/export")
def export_v8_contents() -> Response:
    return Response(
        export_contents_csv(db_path=API_DB_PATH),
        media_type="text/csv; charset=utf-8",
        headers={"Content-Disposition": 'attachment; filename="dcar-contents.csv"'},
    )


@app.get("/api/v8/selling-points")
def get_v8_selling_points() -> Dict[str, Any]:
    return _selling_point_list()


@app.get("/api/v8/reviews")
def get_v8_reviews(status: Optional[str] = None, limit: int = 100) -> Dict[str, Any]:
    if limit < 1 or limit > 500:
        raise HTTPException(status_code=422, detail="limit 必须在 1 到 500 之间")
    where = "WHERE rq.status=?" if status else ""
    parameters: List[Any] = [status] if status else []
    with connect(API_DB_PATH) as connection:
        rows = connection.execute(
            f"""
            SELECT rq.*, c.link_id, c.platform, c.canonical_url, c.title,
                   c.raw_account_name, ev.evidence_level, ev.primary_selling_point_code,
                   ev.selling_point_score, ev.content_automotive_score, ev.payload_json
            FROM review_queue rq
            JOIN content_items c ON c.id=rq.content_id
            LEFT JOIN evaluation_versions ev ON ev.id=(
                SELECT ev2.id FROM evaluation_versions ev2
                WHERE ev2.content_id=c.id AND ev2.invalidated_at IS NULL
                ORDER BY ev2.evaluated_at DESC, ev2.id DESC LIMIT 1
            )
            {where}
            ORDER BY CASE rq.status
                WHEN 'in_review' THEN 0 WHEN 'pending' THEN 1
                WHEN 'manual_required' THEN 2 ELSE 3 END,
                rq.priority DESC, rq.id
            LIMIT ?
            """,
            [*parameters, limit],
        ).fetchall()
        totals = connection.execute(
            "SELECT status, COUNT(*) count FROM review_queue GROUP BY status"
        ).fetchall()
    items: List[Dict[str, Any]] = []
    for row in rows:
        value = dict(row)
        value["evaluation"] = json.loads(str(value.pop("payload_json") or "{}"))
        items.append(value)
    return {
        "items": items,
        "total": sum(int(row["count"]) for row in totals),
        "returned": len(items),
        "status_counts": {str(row["status"]): int(row["count"]) for row in totals},
    }


@app.post("/api/v8/reviews/{queue_id}/start")
def start_v8_review(queue_id: int) -> Dict[str, Any]:
    with connect(API_DB_PATH) as connection, transaction(connection):
        row = connection.execute(
            "SELECT status,content_id FROM review_queue WHERE id=?", (queue_id,)
        ).fetchone()
        if row is None:
            raise HTTPException(status_code=404, detail="复核任务不存在")
        if row["status"] not in {"pending", "manual_required"}:
            raise HTTPException(status_code=409, detail=f"当前状态 {row['status']} 不能开始复核")
        connection.execute(
            "UPDATE review_queue SET status='in_review', updated_at=? WHERE id=?",
            (now_utc(), queue_id),
        )
        evaluation = connection.execute(
            """
            SELECT id FROM evaluation_versions
            WHERE content_id=? AND invalidated_at IS NULL
            ORDER BY evaluated_at DESC,id DESC LIMIT 1
            """,
            (row["content_id"],),
        ).fetchone()
    return {
        "id": queue_id,
        "status": "in_review",
        "base_evaluation_id": int(evaluation["id"]) if evaluation is not None else None,
    }


@app.post("/api/v8/reviews/{queue_id}/resolve")
def resolve_v8_review(queue_id: int, payload: ReviewResolveRequest) -> Dict[str, Any]:
    overrides = {
        key: value
        for key, value in {
            "primary_selling_point_code": payload.primary_selling_point_code,
            "selling_point_score": payload.selling_point_score,
            "selling_point_included": payload.selling_point_included,
            "content_automotive_score": payload.content_automotive_score,
            "content_direction": payload.content_direction,
        }.items()
        if value is not None
    }
    try:
        result = resolve_review(
            queue_id,
            decision=payload.decision,
            reason=payload.reason,
            reviewer=payload.reviewer,
            evidence_type=payload.evidence_type,
            evidence_text=payload.evidence_text,
            base_evaluation_id=payload.base_evaluation_id,
            overrides=overrides,
            db_path=API_DB_PATH,
        )
    except EvaluationError as exc:
        raise HTTPException(status_code=409, detail=str(exc)) from exc
    return {
        "queue_id": queue_id,
        "status": "terminal_failed" if payload.decision == "terminal_unavailable" else "resolved",
        "evaluation_id": result.evaluation_id,
        "evidence_level": result.evidence_level,
        "evidence_sha256": result.evidence_sha256,
    }


@app.post("/api/v8/selling-points/draft")
def create_v8_selling_point_draft() -> Dict[str, Any]:
    try:
        return ensure_draft(db_path=API_DB_PATH)
    except TaxonomyError as exc:
        raise HTTPException(status_code=409, detail=str(exc)) from exc


@app.get("/api/v8/selling-points/draft")
def get_v8_selling_point_draft() -> Dict[str, Any]:
    return list_points(status="draft", db_path=API_DB_PATH)


@app.post("/api/v8/selling-points/items")
def create_v8_selling_point(payload: SellingPointMutationRequest) -> Dict[str, Any]:
    if payload.code is None:
        raise HTTPException(status_code=422, detail="新增卖点必须提供 code")
    try:
        return create_point(payload.model_dump(), db_path=API_DB_PATH)
    except (TaxonomyError, sqlite3.IntegrityError) as exc:
        raise HTTPException(status_code=409, detail=str(exc)) from exc


@app.patch("/api/v8/selling-points/items/{code}")
def update_v8_selling_point(code: str, payload: SellingPointMutationRequest) -> Dict[str, Any]:
    try:
        return update_point(code, payload.model_dump(), db_path=API_DB_PATH)
    except (TaxonomyError, sqlite3.IntegrityError) as exc:
        raise HTTPException(status_code=409, detail=str(exc)) from exc


@app.delete("/api/v8/selling-points/items/{code}")
def delete_v8_selling_point(code: str) -> Dict[str, Any]:
    try:
        delete_point(code, db_path=API_DB_PATH)
    except TaxonomyError as exc:
        raise HTTPException(status_code=409, detail=str(exc)) from exc
    return {"code": code, "deleted": True}


@app.post("/api/v8/selling-points/publish")
def publish_v8_selling_points() -> Dict[str, Any]:
    try:
        return publish_draft(db_path=API_DB_PATH)
    except TaxonomyError as exc:
        raise HTTPException(status_code=409, detail=str(exc)) from exc


@app.get("/api/v7/history/reports")
def v7_history_reports() -> Dict[str, Any]:
    with _legacy_connect() as connection:
        rows = connection.execute(
            """
            SELECT rr.run_id, rr.revision, rr.created_at, rr.report_json_path,
                   rr.report_markdown_path, rr.summary_image_path, rr.output_sha256
            FROM report_revisions rr
            ORDER BY rr.created_at DESC, rr.run_id, rr.revision DESC
            """
        ).fetchall()
    return {"report_version": LEGACY_REPORT_VERSION, "revisions": [dict(row) for row in rows]}


@app.get("/api/v7/history/reports/{run_id}/revisions/{revision}")
def v7_history_report(run_id: str, revision: int) -> Dict[str, Any]:
    with _legacy_connect() as connection:
        row = connection.execute(
            "SELECT report_json_path FROM report_revisions WHERE run_id=? AND revision=?",
            (run_id, revision),
        ).fetchone()
    if row is None:
        raise HTTPException(status_code=404, detail="历史报告不存在")
    return json.loads(_safe_project_path(str(row["report_json_path"])).read_text(encoding="utf-8"))


# Temporary read compatibility for the existing v7 frontend. These routes are removed at cutover.
@app.get("/api/health")
def legacy_health() -> Dict[str, Any]:
    return {"status": "ok", "mode": "legacy_v7_read_only", "report_version": LEGACY_REPORT_VERSION}


@app.get("/api/overview")
def legacy_overview() -> Dict[str, Any]:
    return _legacy_overview()


@app.get("/api/report/latest")
def legacy_latest_report() -> Dict[str, Any]:
    return _legacy_report()


@app.get("/api/runs")
def legacy_runs() -> Dict[str, Any]:
    return {"runs": _legacy_runs()}


@app.get("/api/runs/{run_id}")
def legacy_run(run_id: str) -> Dict[str, Any]:
    value = _legacy_run(run_id)
    if value is None:
        raise HTTPException(status_code=404, detail="任务不存在")
    return value


@app.get("/api/runs/{run_id}/report")
def legacy_run_report(run_id: str) -> Dict[str, Any]:
    value = _legacy_run(run_id)
    if value is None or not value.get("output_path"):
        raise HTTPException(status_code=404, detail="任务报告不存在")
    return json.loads(_safe_project_path(str(value["output_path"])).read_text(encoding="utf-8"))


@app.get("/api/files/{file_key}")
def legacy_file(file_key: str):
    filename = LEGACY_EXPORTS.get(unquote(file_key))
    if filename is None:
        raise HTTPException(status_code=404, detail="导出文件不存在")
    path = _legacy_formal_report_path().parent / filename
    if not path.exists():
        raise HTTPException(status_code=404, detail="导出文件不存在")
    return FileResponse(path, filename=path.name)


@app.post("/api/inputs/validate")
def legacy_validate_inputs(payload: InputValidationRequest) -> Dict[str, Any]:
    return _validate_legacy_inputs(payload.channel, payload.text)


@app.post("/api/runs/{legacy_path:path}")
def reject_legacy_writes(legacy_path: str) -> JSONResponse:
    return JSONResponse(
        status_code=409,
        content={
            "error": "v7 写操作已停用",
            "migration_target": "/api/v8",
            "legacy_path": f"/api/runs/{legacy_path}",
        },
    )
