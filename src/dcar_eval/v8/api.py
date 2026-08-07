"""FastAPI application for DCar Insight v8 with temporary v7 read compatibility."""

from __future__ import annotations

import json
import logging
import os
import re
import sqlite3
import threading
from contextlib import asynccontextmanager
from dataclasses import dataclass
from datetime import datetime, time, timedelta, timezone
from pathlib import Path
from typing import Any, Dict, List, Optional
from urllib.parse import unquote
from zoneinfo import ZoneInfo

from apscheduler.schedulers.background import BackgroundScheduler  # type: ignore[import-untyped]
from fastapi import APIRouter, FastAPI, HTTPException, Request
from fastapi.middleware.cors import CORSMiddleware
from fastapi.responses import FileResponse, JSONResponse, Response
from pydantic import BaseModel, ConfigDict, Field

from .capture import (
    BudgetBlocked,
    CaptureError,
    SlotUnavailable,
    recover_stale_fetch_slots,
)
from .contracts import CURRENT_REPORT_VERSION, quantity_metric, ratio_metric
from .duplicates import FINGERPRINT_VERSION, THRESHOLDS
from .evaluation import RULE_VERSION as RULE_VERSION
from .evaluation import EvaluationError, reopen_review, resolve_review
from .evaluation_selectors import (
    DISPLAY_EFFECTIVE_EVALUATIONS_CTE,
    EvaluationSelectorError,
    FORMAL_CURRENT_EVALUATIONS_CTE,
    active_release,
    display_effective_evaluation,
    effective_direction,
    effective_direction_sql,
    review_anchor_evaluation,
)
from .audience_rate import active_classifier_state, build_channel_audience_rates
from .insights import CHANNELS, SCENES, build_channel_conclusions
from .media import MediaProcessingError, recover_stale_media_processing_slots
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
from .providers import (
    ProviderConfigurationError,
    retry_content_media,
    update_content_data,
)
from .reports import (
    REPORTS_ROOT,
    ReportTaskError,
    assert_report_runtime_ready,
    create_and_run_task,
    get_task,
    list_tasks,
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
    TaxonomyValidationError,
    create_point,
    delete_point,
    ensure_draft,
    list_points,
    publish_draft,
    serialize_point_row,
    update_point,
)


LOGGER = logging.getLogger("dcar.api")
DEFAULT_LEGACY_DB = PROJECT_ROOT / "app" / "data" / "web_mvp.sqlite3"
DEFAULT_OPERATOR_FREEZE_LOCK = PROJECT_ROOT / "runtime" / "operator-freeze.lock"
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


def _enabled(name: str) -> bool:
    return os.environ.get(name, "0").strip() == "1"


@dataclass(frozen=True, slots=True)
class ApiConfig:
    db_path: Path
    reports_root: Path
    legacy_db_path: Path
    operator_freeze_lock: Path
    scheduler_enabled: bool = False
    startup_catchup_enabled: bool = False

    @classmethod
    def from_env(cls) -> "ApiConfig":
        freeze_value = os.environ.get("DCAR_OPERATOR_FREEZE_LOCK") or os.environ.get(
            "DCAR_FREEZE_LOCK"
        )
        return cls(
            db_path=Path(os.environ.get("DCAR_V8_DB", str(DEFAULT_DB))),
            reports_root=Path(
                os.environ.get("DCAR_V8_REPORTS_ROOT", str(REPORTS_ROOT))
            ),
            legacy_db_path=Path(
                os.environ.get("DCAR_LEGACY_DB", str(DEFAULT_LEGACY_DB))
            ),
            operator_freeze_lock=Path(
                freeze_value or str(DEFAULT_OPERATOR_FREEZE_LOCK)
            ),
            scheduler_enabled=_enabled("DCAR_SCHEDULER_ENABLED"),
            startup_catchup_enabled=_enabled("DCAR_STARTUP_CATCHUP_ENABLED"),
        )

    @property
    def effective_startup_catchup_enabled(self) -> bool:
        return self.scheduler_enabled and self.startup_catchup_enabled


def _request_config(request: Request) -> ApiConfig:
    config = getattr(request.app.state, "config", None)
    if not isinstance(config, ApiConfig):
        raise RuntimeError("FastAPI application is missing ApiConfig")
    return config


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


class ReviewReopenRequest(BaseModel):
    reason: str = Field(min_length=1, max_length=2000)
    reopened_by: str = Field(min_length=1, max_length=100)


class SellingPointMutationRequest(BaseModel):
    model_config = ConfigDict(extra="forbid")

    code: Optional[str] = Field(default=None, max_length=3)
    tier: str = Field(max_length=16)
    label: str = Field(min_length=1, max_length=300)
    definition: str = Field(default="", max_length=2000)
    matcher_rule: Dict[str, Any]


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


class ContentPatchRequest(BaseModel):
    model_config = ConfigDict(extra="forbid")

    platform: Optional[str] = Field(default=None, max_length=32)
    platform_content_id: Optional[str] = Field(default=None, max_length=128)
    canonical_url: Optional[str] = Field(default=None, max_length=3000)
    published_at: Optional[str] = Field(default=None, max_length=50)
    title: Optional[str] = Field(default=None, max_length=1000)
    body: Optional[str] = Field(default=None, max_length=20000)
    content_type: Optional[str] = Field(default=None, max_length=32)
    account_uid: Optional[str] = Field(default=None, max_length=128)
    account_name: Optional[str] = Field(default=None, max_length=200)
    account_type: Optional[str] = Field(default=None, max_length=32)
    content_direction: Optional[str] = Field(default=None, max_length=32)


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


def _legacy_connect(legacy_db_path: Path) -> sqlite3.Connection:
    connection = sqlite3.connect(legacy_db_path, timeout=10)
    connection.row_factory = sqlite3.Row
    return connection


def _safe_project_path(value: str) -> Path:
    path = (PROJECT_ROOT / value).resolve()
    root = PROJECT_ROOT.resolve()
    if path != root and root not in path.parents:
        raise HTTPException(status_code=400, detail="文件路径超出项目目录")
    return path


def _legacy_formal_report_path(legacy_db_path: Path) -> Path:
    with _legacy_connect(legacy_db_path) as connection:
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


def _legacy_report(legacy_db_path: Path) -> Dict[str, Any]:
    return json.loads(
        _legacy_formal_report_path(legacy_db_path).read_text(encoding="utf-8")
    )


def _legacy_runs(legacy_db_path: Path, limit: int = 20) -> List[Dict[str, Any]]:
    with _legacy_connect(legacy_db_path) as connection:
        rows = connection.execute(
            "SELECT * FROM runs ORDER BY created_at DESC LIMIT ?", (limit,)
        ).fetchall()
    return [dict(row) for row in rows]


def _legacy_run(legacy_db_path: Path, run_id: str) -> Optional[Dict[str, Any]]:
    with _legacy_connect(legacy_db_path) as connection:
        row = connection.execute("SELECT * FROM runs WHERE id=?", (run_id,)).fetchone()
    return dict(row) if row else None


def _legacy_overview(legacy_db_path: Path) -> Dict[str, Any]:
    report = _legacy_report(legacy_db_path)
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
        "recent_runs": _legacy_runs(legacy_db_path, 8),
    }


def _validate_legacy_inputs(channel: str, text: str) -> Dict[str, Any]:
    items = [
        line.strip() for line in text.replace(",", "\n").splitlines() if line.strip()
    ]
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
    return (
        value.astimezone(timezone.utc)
        .isoformat(timespec="seconds")
        .replace("+00:00", "Z")
    )


def _windows(now: Optional[datetime] = None) -> Dict[str, tuple[datetime, datetime]]:
    current = (now or datetime.now(SHANGHAI)).astimezone(SHANGHAI)
    today = datetime.combine(current.date(), time.min, tzinfo=SHANGHAI)
    this_week = today - timedelta(days=today.weekday())
    return {
        "yesterday": (today - timedelta(days=1), today),
        "this_week": (this_week, current),
        "last_week": (this_week - timedelta(days=7), this_week),
    }


def _window_summary(
    connection: sqlite3.Connection, start: datetime, end: datetime
) -> Dict[str, Any]:
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
    active_accounts = len(
        {
            int(row["account_id"])
            for row in content_rows
            if row["account_id"] is not None
        }
    )
    if content_ids:
        placeholders = ",".join("?" for _ in content_ids)
        active_release(connection)
        evaluations = connection.execute(
            f"""
            WITH {FORMAL_CURRENT_EVALUATIONS_CTE}
            SELECT ev.*, sp.tier primary_tier FROM evaluation_versions ev
            JOIN formal_current_evaluations latest ON latest.id=ev.id
            LEFT JOIN taxonomy_versions tv ON tv.version=ev.taxonomy_version
            LEFT JOIN selling_points sp
              ON sp.taxonomy_id=tv.id AND sp.code=ev.primary_selling_point_code
            WHERE ev.content_id IN ({placeholders})
            """,
            content_ids,
        ).fetchall()
        metrics = connection.execute(
            f"""
            SELECT ms.* FROM content_metric_snapshots ms
            WHERE ms.content_id IN ({placeholders})
              AND ms.id=(
                  SELECT ms2.id FROM content_metric_snapshots ms2
                  WHERE ms2.content_id=ms.content_id
                  ORDER BY
                    CASE WHEN ms2.view_count IS NOT NULL AND ms2.view_count>0
                      THEN 0 ELSE 1 END,
                    ms2.captured_at DESC, ms2.id DESC LIMIT 1
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
        duplicate_calibrated = (
            connection.execute(
                """
            SELECT 1 FROM duplicate_calibration_runs
            WHERE fingerprint_version=? AND thresholds_json=? AND status='passed' LIMIT 1
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
    else:
        evaluations, metrics = [], []
        duplicate_count = fingerprint_count = 0
        duplicate_calibrated = False
    evaluation_values = [dict(row) for row in evaluations]
    metric_values = [dict(row) for row in metrics]
    evaluation_by_content = {int(row["content_id"]): row for row in evaluation_values}
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
                "content_direction": effective_direction(content, evaluation),
                "evidence_level": evaluation.get("evidence_level"),
                "selling_point_included": bool(
                    evaluation.get("selling_point_included")
                ),
                "primary_tier": evaluation.get("primary_tier"),
                "content_automotive_score": evaluation.get("content_automotive_score"),
                "audience_automotive_score": evaluation.get(
                    "audience_automotive_score"
                ),
                "acquisition_potential_score": evaluation.get(
                    "acquisition_potential_score"
                ),
                "view_count": metric.get("view_count"),
            }
        )
    eligible = sum(
        1 for row in evaluation_values if row["evidence_level"] in {"V2", "V3"}
    )
    vertical = sum(
        1
        for row in evaluation_values
        if row["evidence_level"] in {"V2", "V3"}
        and row["content_automotive_score"] is not None
        and int(row["content_automotive_score"]) >= 60
    )
    selling = sum(
        1 for row in evaluation_values if int(row["selling_point_included"]) == 1
    )
    metric_coverage = round(len(metric_values) * 100 / total, 2) if total else None
    views = sum(int(row["view_count"] or 0) for row in metric_values)
    comments = sum(
        int(row["comment_count"] or 0)
        for row in metric_values
        if row["comment_count"] is not None
    )
    ratio_status = (
        "not_applicable"
        if total == 0
        else "available"
        if eligible * 100 / total >= 95
        else "below_threshold"
    )
    duplicate_coverage = round(fingerprint_count * 100 / total, 2) if total else None
    duplicate_status = (
        "not_applicable"
        if total == 0
        else "available"
        if duplicate_calibrated and (duplicate_coverage or 0) >= 95
        else "below_threshold"
    )
    return {
        "period_start": start.isoformat(),
        "period_end": end.isoformat(),
        "eligible_count": eligible,
        "unassociated_content_count": sum(
            1 for row in content_rows if row["account_id"] is None
        ),
        "metrics": {
            "publication_count": quantity_metric(
                total, unit="content", status="available"
            ),
            "active_account_count": quantity_metric(
                active_accounts, unit="account", status="available"
            ),
            "view_count": quantity_metric(
                views if metric_values else None,
                unit="view",
                status="not_applicable"
                if total == 0
                else "missing"
                if not metric_values
                else "available",
                coverage_percentage=metric_coverage,
            ),
            "comment_count": quantity_metric(
                comments
                if metric_values
                and any(row["comment_count"] is not None for row in metric_values)
                else None,
                unit="comment",
                status="not_applicable" if total == 0 else "missing",
                coverage_percentage=metric_coverage,
                reason="历史 valid_unique_commenters 不等于评论量" if total else "",
            ),
            "verticality_rate": ratio_metric(
                vertical if total else None,
                total,
                status=ratio_status,
                eligible_count=eligible,
                coverage_percentage=round(eligible * 100 / total, 2) if total else None,
            ),
            "selling_point_coverage_rate": ratio_metric(
                selling if total else None,
                total,
                status=ratio_status,
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
                    if duplicate_status == "below_threshold"
                    else ""
                ),
            ),
            "estimated_new_users": quantity_metric(
                None,
                unit="person",
                status="not_applicable" if total == 0 else "not_calculable",
                reason="首版没有已验证模型",
            ),
            "estimated_reactivated_users": quantity_metric(
                None,
                unit="person",
                status="not_applicable" if total == 0 else "not_calculable",
                reason="首版没有已验证模型",
            ),
            "estimated_leads": quantity_metric(
                None,
                unit="lead",
                status="not_applicable" if total == 0 else "not_calculable",
                reason="首版没有已验证模型",
            ),
        },
        "channels": build_channel_conclusions(
            conclusion_rows,
            audience_rates=_audience_rates(connection, conclusion_rows, end),
        ),
        "empty_explanation": (
            "所选窗口没有进入有效监控范围的已发布内容，并非抓取故障。"
            if total == 0
            else ""
        ),
    }


def _audience_rates(
    connection: sqlite3.Connection,
    conclusion_rows: List[Dict[str, Any]],
    window_end: datetime,
) -> Dict[str, Dict[str, Any]]:
    """Compute per-slice automotive_user_rate for the overview window.

    The classifier state defaults to ``rejected`` until a gold-set calibration
    marks it approved/conservative, so the rate stays ``below_threshold`` (no
    published percentage) before calibration — never a fabricated value.
    """

    evidence_window_end = _utc_text(window_end)
    evidence_window_start = _utc_text(window_end - timedelta(days=90))
    return build_channel_audience_rates(
        connection,
        conclusion_rows,
        classifier_state=_active_classifier_state(connection),
        evidence_window_start=evidence_window_start,
        evidence_window_end=evidence_window_end,
        report_cutoff_at=now_utc(),
        warm_up=True,
        channels=CHANNELS,
        scenes=SCENES,
    )


def _active_classifier_state(connection: sqlite3.Connection) -> str:
    """Resolve the calibrated classifier state through the shared gate."""

    return active_classifier_state(connection)


def v8_overview(db_path: Path) -> Dict[str, Any]:
    with connect(db_path) as connection:
        windows = {
            key: _window_summary(connection, start, end)
            for key, (start, end) in _windows().items()
        }
        quality = {
            "missing_published_at": int(
                connection.execute(
                    "SELECT COUNT(*) FROM content_items WHERE published_at IS NULL"
                ).fetchone()[0]
            ),
            "pending_reviews": int(
                connection.execute(
                    "SELECT COUNT(*) FROM review_queue WHERE status IN ('pending','manual_required')"
                ).fetchone()[0]
            ),
            "terminal_reviews": int(
                connection.execute(
                    "SELECT COUNT(*) FROM review_queue WHERE status='terminal_failed'"
                ).fetchone()[0]
            ),
            "duplicate_fingerprint_coverage": round(
                int(
                    connection.execute(
                        "SELECT COUNT(DISTINCT content_id) FROM duplicate_fingerprints WHERE fingerprint_version=?",
                        (FINGERPRINT_VERSION,),
                    ).fetchone()[0]
                )
                * 100
                / max(
                    1,
                    int(
                        connection.execute(
                            "SELECT COUNT(*) FROM content_items"
                        ).fetchone()[0]
                    ),
                ),
                2,
            ),
            "duplicate_calibration_ready": connection.execute(
                """
                SELECT 1 FROM duplicate_calibration_runs
                WHERE fingerprint_version=? AND thresholds_json=? AND status='passed' LIMIT 1
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
            is not None,
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


def _account_search(payload: AccountSearchRequest, *, db_path: Path) -> Dict[str, Any]:
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
    with connect(db_path) as connection:
        active_release(connection)
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
                SELECT api.platform, api.uid, api.nickname, api.real_name_status,
                       NULL follower_count,
                       COUNT(c.id) content_count
                FROM account_platform_identities api
                LEFT JOIN content_items c
                  ON c.account_id=api.account_id AND c.platform=api.platform
                WHERE api.account_id=?
                GROUP BY api.id, api.platform, api.uid, api.nickname,
                         api.real_name_status
                ORDER BY api.platform
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


def _content_search(payload: ContentSearchRequest, *, db_path: Path) -> Dict[str, Any]:
    where: List[str] = []
    parameters: List[Any] = []
    direction_sql = effective_direction_sql()
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
        where.append(f"{direction_sql}=?")
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
        LEFT JOIN display_effective_evaluations ev ON ev.content_id=c.id
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
    with connect(db_path) as connection:
        total = int(
            connection.execute(
                f"WITH {DISPLAY_EFFECTIVE_EVALUATIONS_CTE} "
                f"SELECT COUNT(*) {from_sql} {where_sql}",
                parameters,
            ).fetchone()[0]
        )
        rows = connection.execute(
            f"""
            WITH {DISPLAY_EFFECTIVE_EVALUATIONS_CTE}
            SELECT c.id, c.link_id, c.platform, c.platform_content_id,
                   c.canonical_url, c.published_at, c.title, c.body, c.content_type,
                   c.raw_account_uid, c.raw_account_name,
                   COALESCE(a.account_type, c.legacy_account_type, 'unknown') account_type,
                   {direction_sql} content_direction,
                   ev.primary_selling_point_code, ev.evidence_level,
                   ev.content_automotive_score, ev.pending_review,
                   ev.id display_evaluation_id,
                   ev.release_id evaluation_release_id,
                   COALESCE(ev.evaluation_freshness, 'missing') evaluation_freshness,
                   CASE WHEN ev.evaluation_freshness='stale' THEN 1 ELSE 0 END evaluation_is_stale,
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


def _selling_point_list(*, db_path: Path) -> Dict[str, Any]:
    with connect(db_path) as connection:
        taxonomies = connection.execute(
            """
            SELECT * FROM taxonomy_versions WHERE status='published'
            ORDER BY published_at, created_at, id
            """
        ).fetchall()
        if not taxonomies:
            return {"taxonomy": None, "items": []}
        if len(taxonomies) != 1:
            raise TaxonomyError("multiple published taxonomies exist")
        taxonomy = taxonomies[0]
        try:
            release = active_release(connection)
        except EvaluationSelectorError as error:
            raise TaxonomyError(str(error)) from error
        assert release is not None
        if str(release["taxonomy_version"]) != str(taxonomy["version"]):
            raise TaxonomyError(
                "active evaluation release does not match published taxonomy"
            )
        rows = connection.execute(
            f"""
            WITH {FORMAL_CURRENT_EVALUATIONS_CTE},
            resolved_matches AS (
                SELECT em.selling_point_code, em.match_role, ev.content_id,
                       em.scene
                FROM formal_current_evaluations ev
                JOIN evaluation_matches em ON em.evaluation_id=ev.id
            )
            SELECT sp.*,
                   COUNT(DISTINCT CASE WHEN rm.match_role='primary' THEN rm.content_id END) primary_hits,
                   COUNT(DISTINCT rm.content_id) total_hits,
                   COUNT(DISTINCT CASE WHEN rm.scene='used_car' AND rm.match_role='primary' THEN rm.content_id END) used_car_primary_hits,
                   COUNT(DISTINCT CASE WHEN rm.scene='used_car' THEN rm.content_id END) used_car_total_hits,
                   COUNT(DISTINCT CASE WHEN rm.scene='new_car' AND rm.match_role='primary' THEN rm.content_id END) new_car_primary_hits,
                   COUNT(DISTINCT CASE WHEN rm.scene='new_car' THEN rm.content_id END) new_car_total_hits,
                   COUNT(DISTINCT CASE WHEN rm.scene='media' AND rm.match_role='primary' THEN rm.content_id END) media_primary_hits,
                   COUNT(DISTINCT CASE WHEN rm.scene='media' THEN rm.content_id END) media_total_hits
            FROM selling_points sp
            LEFT JOIN resolved_matches rm ON rm.selling_point_code=sp.code
            WHERE sp.taxonomy_id=?
            GROUP BY sp.id
            ORDER BY substr(sp.code, 1, 1), CAST(substr(sp.code, 2) AS INTEGER)
            """,
            (taxonomy["id"],),
        ).fetchall()
        items: List[Dict[str, Any]] = []
        for row in rows:
            point = serialize_point_row(connection, taxonomy, row)
            items.append(
                {
                    **point,
                    "enabled": bool(row["enabled"]),
                    "primary_hits": row["primary_hits"],
                    "total_hits": row["total_hits"],
                    "scene_hits": {
                        scene: {
                            "primary_hits": row[f"{scene}_primary_hits"],
                            "total_hits": row[f"{scene}_total_hits"],
                        }
                        for scene in ("used_car", "new_car", "media")
                    },
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


def _ocr_payload_text(payload: Dict[str, Any]) -> str:
    combined = str(payload.get("combined_text") or "").strip()
    if combined:
        return combined
    texts: List[str] = []
    observations = payload.get("observations")
    if not isinstance(observations, list):
        return ""
    for item in observations:
        if not isinstance(item, dict):
            continue
        text = "\n".join(str(item.get("text") or "").splitlines()).strip()
        if text and text not in texts:
            texts.append(text)
    return "\n".join(texts)


def _content_evidence(content_id: int, *, db_path: Path) -> Dict[str, Any]:
    with connect(db_path) as connection:
        content = connection.execute(
            "SELECT * FROM content_items WHERE id=?", (content_id,)
        ).fetchone()
        if content is None:
            raise HTTPException(status_code=404, detail="内容不存在")
        evaluation = display_effective_evaluation(connection, content_id)
        review_anchor = review_anchor_evaluation(connection, content_id)
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
        comment_rows = (
            []
            if comment_version is None
            else connection.execute(
                """
            SELECT body, like_count, published_at FROM comments
            WHERE evidence_version_id=?
            ORDER BY COALESCE(like_count,0) DESC, id LIMIT 20
            """,
                (comment_version["id"],),
            ).fetchall()
        )
        stored_comment_count = (
            0
            if comment_version is None
            else int(
                connection.execute(
                    "SELECT COUNT(*) FROM comments WHERE evidence_version_id=?",
                    (comment_version["id"],),
                ).fetchone()[0]
            )
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
                    "kind": "video"
                    if suffix in {".mp4", ".mov", ".m4v", ".webm"}
                    else "image",
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
                "id",
                "link_id",
                "platform",
                "canonical_url",
                "title",
                "body",
                "content_type",
                "published_at",
                "raw_account_uid",
                "raw_account_name",
            )
        },
        "base_evaluation_id": int(review_anchor["id"])
        if review_anchor is not None
        else None,
        "display_evaluation_id": int(evaluation["id"])
        if evaluation is not None
        else None,
        "evaluation_freshness": str(evaluation["evaluation_freshness"])
        if evaluation is not None
        else "missing",
        "evaluation_is_stale": evaluation["evaluation_freshness"] == "stale"
        if evaluation is not None
        else False,
        "evaluation": evaluation_payload,
        "media": media_items,
        "asr": {
            "status": asr_payload.get("status")
            or ("missing" if asr_row is None else "available"),
            "model": asr_payload.get("model"),
            "text": asr_payload.get("text") or "",
        },
        "ocr": {
            "status": ocr_payload.get("status")
            or ("missing" if ocr_row is None else "available"),
            "observation_count": ocr_payload.get("ocr_observation_count")
            or ocr_payload.get("source_count")
            or 0,
            "text": _ocr_payload_text(ocr_payload),
        },
        "comments": {
            "status": str(comment_version["status"]) if comment_version else "missing",
            "captured_at": comment_version["captured_at"] if comment_version else None,
            "declared_count": comment_version["comment_count"]
            if comment_version
            else None,
            "stored_count": stored_comment_count,
            "top_items": [dict(row) for row in comment_rows],
        },
        "processing_slots": [dict(row) for row in processing],
        "review": dict(review) if review is not None else None,
    }


def _recover_interrupted_tasks(*, db_path: Path) -> int:
    with connect(db_path) as connection:
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


def _run_startup_catchup(
    app: FastAPI,
    *,
    db_path: Path,
    reports_root: Path,
) -> None:
    """Run potentially slow supplier catch-up without blocking API startup."""
    try:
        app.state.catchup_results = startup_catchup(
            db_path=db_path, reports_root=reports_root
        )
    except Exception as exc:
        LOGGER.exception("startup catch-up failed")
        app.state.catchup_error = str(exc)
        app.state.catchup_status = "failed"
    else:
        app.state.catchup_status = "succeeded"


@asynccontextmanager
async def lifespan(app: FastAPI):
    config = getattr(app.state, "config", None)
    if not isinstance(config, ApiConfig):
        raise RuntimeError("FastAPI application is missing ApiConfig")
    if (
        config.db_path.resolve() == DEFAULT_DB.resolve()
        and config.operator_freeze_lock.exists()
    ):
        raise RuntimeError(
            "production startup blocked by operator freeze lock: "
            f"{config.operator_freeze_lock}"
        )
    with connect(config.db_path) as connection:
        initialize_database(connection)
    app.state.recovered_fetch_slots = recover_stale_fetch_slots(db_path=config.db_path)
    app.state.recovered_media_slots = recover_stale_media_processing_slots(
        db_path=config.db_path
    )
    app.state.recovered_tasks = _recover_interrupted_tasks(db_path=config.db_path)
    app.state.scheduler_requested = config.scheduler_enabled
    app.state.scheduler_enabled = False
    app.state.startup_catchup_requested = config.startup_catchup_enabled
    app.state.startup_catchup_enabled = False
    app.state.report_runtime_ready = None
    app.state.report_runtime_error = None
    scheduler: Optional[BackgroundScheduler] = None
    report_runtime_ready = False
    if config.scheduler_enabled:
        try:
            with connect(config.db_path) as connection:
                assert_report_runtime_ready(connection)
        except Exception as exc:
            LOGGER.error("scheduler blocked by report runtime gate: %s", exc)
            app.state.report_runtime_ready = False
            app.state.report_runtime_error = str(exc)
        else:
            report_runtime_ready = True
            app.state.report_runtime_ready = True
    if config.scheduler_enabled and report_runtime_ready:
        scheduler = BackgroundScheduler(timezone="Asia/Shanghai")
        install_jobs(
            scheduler,
            db_path=config.db_path,
            reports_root=config.reports_root,
        )
        scheduler.start()
        app.state.scheduler_enabled = True
    if config.effective_startup_catchup_enabled and report_runtime_ready:
        app.state.startup_catchup_enabled = True
        app.state.catchup_status = "running"
        app.state.catchup_results = []
        app.state.catchup_error = None
        catchup_thread = threading.Thread(
            target=_run_startup_catchup,
            args=(app,),
            kwargs={
                "db_path": config.db_path,
                "reports_root": config.reports_root,
            },
            name="dcar-startup-catchup",
            daemon=True,
        )
        app.state.catchup_thread = catchup_thread
        catchup_thread.start()
    else:
        app.state.catchup_status = (
            "blocked"
            if config.effective_startup_catchup_enabled and not report_runtime_ready
            else "disabled"
        )
        app.state.catchup_results = []
        app.state.catchup_error = (
            app.state.report_runtime_error
            if app.state.catchup_status == "blocked"
            else None
        )
    app.state.scheduler = scheduler
    try:
        yield
    finally:
        if scheduler is not None:
            scheduler.shutdown(wait=False)


router = APIRouter()


async def privacy_safe_request_log(request: Request, call_next):
    response = await call_next(request)
    LOGGER.info("%s %s %s", request.method, request.url.path, response.status_code)
    return response


def create_app(config: Optional[ApiConfig] = None) -> FastAPI:
    application = FastAPI(title="DCar Insight API", version="8.3", lifespan=lifespan)
    application.state.config = config or ApiConfig.from_env()
    application.add_middleware(
        CORSMiddleware,
        allow_origin_regex=r"http://(?:localhost|127\.0\.0\.1):\d{2,5}",
        allow_credentials=False,
        allow_methods=["GET", "POST", "PATCH", "DELETE", "OPTIONS"],
        allow_headers=["Content-Type"],
    )
    application.middleware("http")(privacy_safe_request_log)
    application.include_router(router)
    return application


@router.get("/api/v8/health")
def v8_health(request: Request) -> Dict[str, Any]:
    config = _request_config(request)
    return {
        "status": "ok",
        "mode": "local_v8",
        "report_version": CURRENT_REPORT_VERSION,
        "database": config.db_path.name,
    }


@router.get("/api/v8/overview")
def get_v8_overview(request: Request) -> Dict[str, Any]:
    return v8_overview(_request_config(request).db_path)


@router.get("/api/v8/tasks")
def get_v8_tasks(request: Request) -> Dict[str, Any]:
    items = list_tasks(db_path=_request_config(request).db_path)
    return {"items": items, "total": len(items)}


@router.get("/api/v8/scheduler")
def get_v8_scheduler_status(request: Request) -> Dict[str, Any]:
    config = _request_config(request)
    with connect(config.db_path) as connection:
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
        "requested": bool(getattr(request.app.state, "scheduler_requested", False)),
        "enabled": bool(getattr(request.app.state, "scheduler_enabled", False)),
        "report_runtime": {
            "ready": getattr(request.app.state, "report_runtime_ready", None),
            "error": getattr(request.app.state, "report_runtime_error", None),
        },
        "startup_catchup": {
            "requested": bool(
                getattr(request.app.state, "startup_catchup_requested", False)
            ),
            "enabled": bool(
                getattr(request.app.state, "startup_catchup_enabled", False)
            ),
            "status": getattr(request.app.state, "catchup_status", "unknown"),
            "error": getattr(request.app.state, "catchup_error", None),
            "results": getattr(request.app.state, "catchup_results", []),
        },
        "jobs": [dict(row) for row in rows],
        "media_slot_recovery": getattr(
            request.app.state,
            "recovered_media_slots",
            {
                "stale_candidates": 0,
                "recovered": 0,
                "retryable_failed": 0,
                "terminal_failed": 0,
                "cas_conflicts": 0,
                "exhausted_normalized": 0,
            },
        ),
        "fetch_slot_recovery": getattr(
            request.app.state,
            "recovered_fetch_slots",
            {"stale_candidates": 0, "recovered": 0},
        ),
    }


@router.post("/api/v8/tasks")
def create_v8_task(request: Request, payload: TaskCreateRequest) -> Dict[str, Any]:
    config = _request_config(request)
    try:
        return create_and_run_task(
            task_type="custom",
            period_start=payload.period_start,
            period_end=payload.period_end,
            creation_source="manual",
            name=payload.name,
            db_path=config.db_path,
            reports_root=config.reports_root,
        )
    except ReportTaskError as exc:
        raise HTTPException(status_code=409, detail=str(exc)) from exc


@router.get("/api/v8/tasks/{task_id}")
def get_v8_task(request: Request, task_id: str) -> Dict[str, Any]:
    try:
        return get_task(task_id, db_path=_request_config(request).db_path)
    except ReportTaskError as exc:
        raise HTTPException(status_code=404, detail=str(exc)) from exc


@router.post("/api/v8/tasks/{task_id}/retry")
def retry_v8_task(request: Request, task_id: str) -> Dict[str, Any]:
    config = _request_config(request)
    try:
        retry_task(task_id, db_path=config.db_path)
        run_task(task_id, db_path=config.db_path, reports_root=config.reports_root)
        return get_task(task_id, db_path=config.db_path)
    except ReportTaskError as exc:
        raise HTTPException(status_code=409, detail=str(exc)) from exc


@router.post("/api/v8/tasks/{task_id}/cancel")
def cancel_v8_task(request: Request, task_id: str) -> Dict[str, Any]:
    try:
        return request_task_cancel(task_id, db_path=_request_config(request).db_path)
    except ReportTaskError as exc:
        raise HTTPException(status_code=409, detail=str(exc)) from exc


@router.post("/api/v8/tasks/{task_id}/resume")
def resume_v8_task(request: Request, task_id: str) -> Dict[str, Any]:
    config = _request_config(request)
    try:
        resume_task(task_id, db_path=config.db_path)
        run_task(task_id, db_path=config.db_path, reports_root=config.reports_root)
        return get_task(task_id, db_path=config.db_path)
    except ReportTaskError as exc:
        raise HTTPException(status_code=409, detail=str(exc)) from exc


@router.get("/api/v8/tasks/{task_id}/revisions/{revision}/report")
def get_v8_task_report(request: Request, task_id: str, revision: int) -> Dict[str, Any]:
    with connect(_request_config(request).db_path) as connection:
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


@router.get("/api/v8/tasks/{task_id}/revisions/{revision}/files/{file_kind}")
def download_v8_task_file(
    request: Request, task_id: str, revision: int, file_kind: str
) -> FileResponse:
    kinds = (
        ["summary-png", "summary-svg"] if file_kind == "summary-image" else [file_kind]
    )
    placeholders = ",".join("?" for _ in kinds)
    with connect(_request_config(request).db_path) as connection:
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
        "report-json": "application/json",
        "report-markdown": "text/markdown",
        "content-csv": "text/csv",
        "channel-csv": "text/csv",
        "summary-svg": "image/svg+xml",
        "summary-png": "image/png",
    }
    return FileResponse(
        path,
        media_type=media_types.get(str(row["file_kind"]), "application/octet-stream"),
        filename=path.name,
    )


@router.post("/api/v8/accounts/search")
def search_v8_accounts(
    request: Request, payload: AccountSearchRequest
) -> Dict[str, Any]:
    return _account_search(payload, db_path=_request_config(request).db_path)


@router.post("/api/v8/accounts")
def create_v8_account(
    request: Request, payload: AccountMutationRequest
) -> Dict[str, Any]:
    try:
        return upsert_account(
            payload.model_dump(), db_path=_request_config(request).db_path
        )
    except OperationError as exc:
        raise HTTPException(status_code=409, detail=str(exc)) from exc


@router.patch("/api/v8/accounts/{account_id}")
def patch_v8_account(
    request: Request, account_id: int, payload: AccountMutationRequest
) -> Dict[str, Any]:
    try:
        return update_account(
            account_id,
            payload.model_dump(),
            db_path=_request_config(request).db_path,
        )
    except OperationError as exc:
        raise HTTPException(status_code=409, detail=str(exc)) from exc


@router.post("/api/v8/accounts/import")
def import_v8_accounts(request: Request, payload: BulkImportRequest) -> Dict[str, Any]:
    return import_accounts(
        payload.rows,
        source_name=payload.source_name,
        db_path=_request_config(request).db_path,
    )


@router.get("/api/v8/accounts/export")
def export_v8_accounts(request: Request) -> Response:
    return Response(
        export_accounts_csv(db_path=_request_config(request).db_path),
        media_type="text/csv; charset=utf-8",
        headers={"Content-Disposition": 'attachment; filename="dcar-accounts.csv"'},
    )


@router.post("/api/v8/contents/search")
def search_v8_contents(
    request: Request, payload: ContentSearchRequest
) -> Dict[str, Any]:
    return _content_search(payload, db_path=_request_config(request).db_path)


@router.post("/api/v8/contents/validate")
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
    return {
        "total": len(payload.rows),
        "valid": valid,
        "rejected": len(payload.rows) - valid,
        "items": items,
    }


@router.post("/api/v8/contents")
def create_v8_content(
    request: Request, payload: ContentMutationRequest
) -> Dict[str, Any]:
    try:
        return upsert_content(
            payload.model_dump(), db_path=_request_config(request).db_path
        )
    except OperationError as exc:
        raise HTTPException(status_code=409, detail=str(exc)) from exc


@router.patch("/api/v8/contents/{content_id}")
def patch_v8_content(
    request: Request, content_id: int, payload: ContentPatchRequest
) -> Dict[str, Any]:
    updates = payload.model_dump(exclude_unset=True)
    if not updates:
        raise HTTPException(status_code=422, detail="至少提交一个要修改的字段")
    try:
        return update_content(
            content_id,
            updates,
            db_path=_request_config(request).db_path,
        )
    except OperationError as exc:
        raise HTTPException(status_code=409, detail=str(exc)) from exc


@router.post("/api/v8/contents/import")
def import_v8_contents(request: Request, payload: BulkImportRequest) -> Dict[str, Any]:
    return import_contents(
        payload.rows,
        source_name=payload.source_name,
        db_path=_request_config(request).db_path,
    )


@router.post("/api/v8/contents/{content_id}/update-data")
def update_v8_content_data(request: Request, content_id: int) -> Dict[str, Any]:
    try:
        return update_content_data(content_id, db_path=_request_config(request).db_path)
    except ProviderConfigurationError as exc:
        raise HTTPException(status_code=409, detail=str(exc)) from exc


@router.get("/api/v8/contents/{content_id}/evidence")
def get_v8_content_evidence(request: Request, content_id: int) -> Dict[str, Any]:
    return _content_evidence(content_id, db_path=_request_config(request).db_path)


@router.get("/api/v8/contents/{content_id}/evidence/files/{artifact_id}/{index}")
def get_v8_content_evidence_file(
    request: Request, content_id: int, artifact_id: int, index: int
) -> FileResponse:
    with connect(_request_config(request).db_path) as connection:
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


@router.post("/api/v8/contents/{content_id}/media/retry")
def retry_v8_content_media(
    request: Request, content_id: int, payload: MediaRetryRequest
) -> Dict[str, Any]:
    try:
        return retry_content_media(
            content_id,
            allow_paid_refresh=payload.allow_paid_refresh,
            db_path=_request_config(request).db_path,
        )
    except (
        ProviderConfigurationError,
        MediaProcessingError,
        CaptureError,
        SlotUnavailable,
        BudgetBlocked,
    ) as exc:
        raise HTTPException(status_code=409, detail=str(exc)) from exc


@router.post("/api/v8/media-processing/search")
def search_v8_media_processing(
    request: Request, payload: MediaProcessingSearchRequest
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
    with connect(_request_config(request).db_path) as connection:
        total = int(
            connection.execute(
                f"SELECT COUNT(*) FROM media_processing_slots m {where_sql}", parameters
            ).fetchone()[0]
        )
        status_rows = connection.execute(
            f"""
            SELECT m.status, COUNT(*) count
            FROM media_processing_slots m
            {where_sql}
            GROUP BY m.status
            """,
            parameters,
        ).fetchall()
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
        "slot_status_counts": {
            status: next(
                (int(row["count"]) for row in status_rows if row["status"] == status),
                0,
            )
            for status in (
                "pending",
                "running",
                "succeeded",
                "retryable_failed",
                "terminal_failed",
            )
        },
        "page": payload.page,
        "page_size": payload.page_size,
    }


@router.get("/api/v8/contents/export")
def export_v8_contents(request: Request) -> Response:
    return Response(
        export_contents_csv(db_path=_request_config(request).db_path),
        media_type="text/csv; charset=utf-8",
        headers={"Content-Disposition": 'attachment; filename="dcar-contents.csv"'},
    )


@router.get("/api/v8/selling-points")
def get_v8_selling_points(request: Request) -> Dict[str, Any]:
    try:
        return _selling_point_list(db_path=_request_config(request).db_path)
    except TaxonomyError as exc:
        raise HTTPException(status_code=409, detail=str(exc)) from exc


@router.get("/api/v8/reviews")
def get_v8_reviews(
    request: Request, status: Optional[str] = None, limit: int = 100
) -> Dict[str, Any]:
    if limit < 1 or limit > 500:
        raise HTTPException(status_code=422, detail="limit 必须在 1 到 500 之间")
    where = "WHERE rq.status=?" if status else ""
    parameters: List[Any] = [status] if status else []
    with connect(_request_config(request).db_path) as connection:
        active_release(connection)
        rows = connection.execute(
            f"""
            WITH {DISPLAY_EFFECTIVE_EVALUATIONS_CTE}
            SELECT rq.*, c.link_id, c.platform, c.canonical_url, c.title,
                   c.raw_account_name, ev.evidence_level, ev.primary_selling_point_code,
                   ev.selling_point_score, ev.content_automotive_score, ev.payload_json,
                   ev.id display_evaluation_id,
                   COALESCE(ev.evaluation_freshness, 'missing') evaluation_freshness,
                   CASE WHEN ev.evaluation_freshness='stale' THEN 1 ELSE 0 END evaluation_is_stale
            FROM review_queue rq
            JOIN content_items c ON c.id=rq.content_id
            LEFT JOIN display_effective_evaluations ev ON ev.content_id=c.id
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


@router.post("/api/v8/reviews/{queue_id}/start")
def start_v8_review(request: Request, queue_id: int) -> Dict[str, Any]:
    with (
        connect(_request_config(request).db_path) as connection,
        transaction(connection),
    ):
        row = connection.execute(
            "SELECT status,content_id,evaluation_id FROM review_queue WHERE id=?",
            (queue_id,),
        ).fetchone()
        if row is None:
            raise HTTPException(status_code=404, detail="复核任务不存在")
        if row["status"] not in {"pending", "manual_required"}:
            raise HTTPException(
                status_code=409, detail=f"当前状态 {row['status']} 不能开始复核"
            )
        evaluation = review_anchor_evaluation(connection, int(row["content_id"]))
        if evaluation is None:
            raise HTTPException(status_code=409, detail="复核内容没有可用评估")
        updated = connection.execute(
            """
            UPDATE review_queue
            SET status='in_review', evaluation_id=?, updated_at=?
            WHERE id=? AND status IN ('pending','manual_required')
            """,
            (evaluation["id"], now_utc(), queue_id),
        )
        if updated.rowcount != 1:
            raise HTTPException(status_code=409, detail="复核任务状态已更新，请刷新")
    return {
        "id": queue_id,
        "status": "in_review",
        "base_evaluation_id": int(evaluation["id"]),
    }


@router.post("/api/v8/reviews/{queue_id}/reopen")
def reopen_v8_review(
    request: Request, queue_id: int, payload: ReviewReopenRequest
) -> Dict[str, Any]:
    try:
        result = reopen_review(
            queue_id,
            reason=payload.reason,
            reopened_by=payload.reopened_by,
            db_path=_request_config(request).db_path,
        )
    except EvaluationError as exc:
        raise HTTPException(status_code=409, detail=str(exc)) from exc
    return {
        "id": result.queue_id,
        "status": "in_review",
        "reopen_event_id": result.event_id,
        "base_evaluation_id": result.base_evaluation_id,
    }


@router.post("/api/v8/reviews/{queue_id}/resolve")
def resolve_v8_review(
    request: Request, queue_id: int, payload: ReviewResolveRequest
) -> Dict[str, Any]:
    override_fields = {
        "primary_selling_point_code",
        "selling_point_score",
        "selling_point_included",
        "content_automotive_score",
        "content_direction",
    }
    overrides = {
        key: getattr(payload, key)
        for key in override_fields.intersection(payload.model_fields_set)
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
            db_path=_request_config(request).db_path,
        )
    except EvaluationError as exc:
        raise HTTPException(status_code=409, detail=str(exc)) from exc
    return {
        "queue_id": queue_id,
        "status": "terminal_failed"
        if payload.decision == "terminal_unavailable"
        else "resolved",
        "evaluation_id": result.evaluation_id,
        "evidence_level": result.evidence_level,
        "evidence_sha256": result.evidence_sha256,
    }


@router.post("/api/v8/selling-points/draft")
def create_v8_selling_point_draft(request: Request) -> Dict[str, Any]:
    try:
        return ensure_draft(db_path=_request_config(request).db_path)
    except TaxonomyError as exc:
        raise HTTPException(status_code=409, detail=str(exc)) from exc


@router.get("/api/v8/selling-points/draft")
def get_v8_selling_point_draft(request: Request) -> Dict[str, Any]:
    try:
        return list_points(status="draft", db_path=_request_config(request).db_path)
    except TaxonomyError as exc:
        raise HTTPException(status_code=409, detail=str(exc)) from exc


@router.post("/api/v8/selling-points/items")
def create_v8_selling_point(
    request: Request, payload: SellingPointMutationRequest
) -> Dict[str, Any]:
    if payload.code is None:
        raise HTTPException(status_code=422, detail="新增卖点必须提供 code")
    try:
        return create_point(
            payload.model_dump(), db_path=_request_config(request).db_path
        )
    except TaxonomyValidationError as exc:
        raise HTTPException(status_code=422, detail=str(exc)) from exc
    except (TaxonomyError, sqlite3.IntegrityError) as exc:
        raise HTTPException(status_code=409, detail=str(exc)) from exc


@router.patch("/api/v8/selling-points/items/{code}")
def update_v8_selling_point(
    request: Request, code: str, payload: SellingPointMutationRequest
) -> Dict[str, Any]:
    try:
        return update_point(
            code,
            payload.model_dump(),
            db_path=_request_config(request).db_path,
        )
    except TaxonomyValidationError as exc:
        raise HTTPException(status_code=422, detail=str(exc)) from exc
    except (TaxonomyError, sqlite3.IntegrityError) as exc:
        raise HTTPException(status_code=409, detail=str(exc)) from exc


@router.delete("/api/v8/selling-points/items/{code}")
def delete_v8_selling_point(request: Request, code: str) -> Dict[str, Any]:
    try:
        delete_point(code, db_path=_request_config(request).db_path)
    except TaxonomyValidationError as exc:
        raise HTTPException(status_code=422, detail=str(exc)) from exc
    except TaxonomyError as exc:
        raise HTTPException(status_code=409, detail=str(exc)) from exc
    return {"code": code, "deleted": True}


@router.post("/api/v8/selling-points/publish")
def publish_v8_selling_points(request: Request) -> Dict[str, Any]:
    try:
        return publish_draft(db_path=_request_config(request).db_path)
    except TaxonomyError as exc:
        raise HTTPException(status_code=409, detail=str(exc)) from exc


@router.get("/api/v7/history/reports")
def v7_history_reports(request: Request) -> Dict[str, Any]:
    with _legacy_connect(_request_config(request).legacy_db_path) as connection:
        rows = connection.execute(
            """
            SELECT rr.run_id, rr.revision, rr.created_at, rr.report_json_path,
                   rr.report_markdown_path, rr.summary_image_path, rr.output_sha256
            FROM report_revisions rr
            ORDER BY rr.created_at DESC, rr.run_id, rr.revision DESC
            """
        ).fetchall()
    return {
        "report_version": LEGACY_REPORT_VERSION,
        "revisions": [dict(row) for row in rows],
    }


@router.get("/api/v7/history/reports/{run_id}/revisions/{revision}")
def v7_history_report(request: Request, run_id: str, revision: int) -> Dict[str, Any]:
    with _legacy_connect(_request_config(request).legacy_db_path) as connection:
        row = connection.execute(
            "SELECT report_json_path FROM report_revisions WHERE run_id=? AND revision=?",
            (run_id, revision),
        ).fetchone()
    if row is None:
        raise HTTPException(status_code=404, detail="历史报告不存在")
    return json.loads(
        _safe_project_path(str(row["report_json_path"])).read_text(encoding="utf-8")
    )


# Temporary read compatibility for the existing v7 frontend. These routes are removed at cutover.
@router.get("/api/health")
def legacy_health() -> Dict[str, Any]:
    return {
        "status": "ok",
        "mode": "legacy_v7_read_only",
        "report_version": LEGACY_REPORT_VERSION,
    }


@router.get("/api/overview")
def legacy_overview(request: Request) -> Dict[str, Any]:
    return _legacy_overview(_request_config(request).legacy_db_path)


@router.get("/api/report/latest")
def legacy_latest_report(request: Request) -> Dict[str, Any]:
    return _legacy_report(_request_config(request).legacy_db_path)


@router.get("/api/runs")
def legacy_runs(request: Request) -> Dict[str, Any]:
    return {"runs": _legacy_runs(_request_config(request).legacy_db_path)}


@router.get("/api/runs/{run_id}")
def legacy_run(request: Request, run_id: str) -> Dict[str, Any]:
    value = _legacy_run(_request_config(request).legacy_db_path, run_id)
    if value is None:
        raise HTTPException(status_code=404, detail="任务不存在")
    return value


@router.get("/api/runs/{run_id}/report")
def legacy_run_report(request: Request, run_id: str) -> Dict[str, Any]:
    value = _legacy_run(_request_config(request).legacy_db_path, run_id)
    if value is None or not value.get("output_path"):
        raise HTTPException(status_code=404, detail="任务报告不存在")
    return json.loads(
        _safe_project_path(str(value["output_path"])).read_text(encoding="utf-8")
    )


@router.get("/api/files/{file_key}")
def legacy_file(request: Request, file_key: str):
    filename = LEGACY_EXPORTS.get(unquote(file_key))
    if filename is None:
        raise HTTPException(status_code=404, detail="导出文件不存在")
    path = (
        _legacy_formal_report_path(_request_config(request).legacy_db_path).parent
        / filename
    )
    if not path.exists():
        raise HTTPException(status_code=404, detail="导出文件不存在")
    return FileResponse(path, filename=path.name)


@router.post("/api/inputs/validate")
def legacy_validate_inputs(payload: InputValidationRequest) -> Dict[str, Any]:
    return _validate_legacy_inputs(payload.channel, payload.text)


@router.post("/api/runs/{legacy_path:path}")
def reject_legacy_writes(legacy_path: str) -> JSONResponse:
    return JSONResponse(
        status_code=409,
        content={
            "error": "v7 写操作已停用",
            "migration_target": "/api/v8",
            "legacy_path": f"/api/runs/{legacy_path}",
        },
    )


app = create_app()
