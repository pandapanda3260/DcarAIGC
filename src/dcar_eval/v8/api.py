"""FastAPI application for DCar Insight v8 with temporary v7 read compatibility."""

from __future__ import annotations

import csv
import fcntl
import hashlib
import json
import logging
import os
import re
import sqlite3
import tempfile
import threading
from contextlib import asynccontextmanager, contextmanager
from dataclasses import dataclass
from datetime import date, datetime, time, timedelta, timezone
from functools import lru_cache
from pathlib import Path
from typing import Any, Dict, List, Mapping, Optional
from urllib.parse import quote, unquote
from zoneinfo import ZoneInfo

from apscheduler.schedulers.background import BackgroundScheduler  # type: ignore[import-untyped]
from fastapi import APIRouter, BackgroundTasks, FastAPI, HTTPException, Request
from fastapi.middleware.cors import CORSMiddleware
from fastapi.responses import FileResponse, JSONResponse, Response
from pydantic import BaseModel, ConfigDict, Field

from .capture import (
    BudgetBlocked,
    CaptureError,
    SlotUnavailable,
    recover_stale_fetch_slots,
)
from .contracts import (
    CURRENT_REPORT_VERSION,
    load_contract,
    quantity_metric,
    ratio_metric,
)
from .duplicates import FINGERPRINT_VERSION, THRESHOLDS, duplicate_metric_decision
from .evaluation_selectors import (
    DISPLAY_EFFECTIVE_EVALUATIONS_CTE,
    EvaluationSelectorError,
    FORMAL_CURRENT_EVALUATIONS_CTE,
    active_release,
    display_effective_evaluation,
    effective_direction,
    effective_direction_sql,
    formal_eligible_release_evaluations,
)
from .audience_rate import active_classifier_state, build_channel_audience_rates
from .spu_audience import (
    STAT_PLATFORMS,
    STAT_WINDOWS,
    SpuAudienceError,
    associate_single_content,
    build_stats as build_spu_audience_stats,
    content_labels as spu_content_labels,
    default_llm_hook as default_spu_llm_hook,
    domain_ready as spu_domain_ready,
    list_assets as list_spu_audience_assets,
    recover_orphan_association_runs,
    resolve_incremental_since,
    run_association as run_spu_association,
    start_association_run,
    upsert_spu,
)
from .insights import CHANNELS, SCENES, build_channel_conclusions
from .media import (
    MediaProcessingError,
    processor_versions,
    recover_stale_media_processing_slots,
)
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
from .report_export import (
    build_report_detail_workbook,
    build_report_download_bundle,
    report_bundle_filename,
)
from .reports import (
    IMPLICIT_RUN_STATUSES,
    REPORTS_ROOT,
    ReportTaskError,
    TaskCancelled,
    assert_report_runtime_ready,
    create_task,
    get_task,
    list_tasks,
    request_task_cancel,
    render_summary_png,
    retry_task,
    resume_task,
    run_task,
)
from .scheduler import (
    install_jobs,
    recover_interrupted_scheduler_runs,
    startup_catchup,
)
from .storage import (
    DEFAULT_DB,
    PROJECT_ROOT,
    SCHEMA_VERSION,
    connect,
    initialize_database,
    is_formal_database_path,
    now_utc,
    require_schema_compatibility,
    schema_compatibility_state,
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
DEFAULT_WRITER_LOCK = PROJECT_ROOT / "runtime" / "writer-worker.lock"
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
RUNTIME_IDENTITY_SCHEMA = "dcar-runtime-identity-v1"
DAILY_CAPTURE_RECONCILE_INTERVAL_SECONDS = 3600


def _enabled(name: str) -> bool:
    return os.environ.get(name, "0").strip() == "1"


def _optional_strict_iso_date(name: str) -> date | None:
    raw = os.environ.get(name)
    if raw is None:
        return None
    if re.fullmatch(r"\d{4}-\d{2}-\d{2}", raw) is None:
        raise RuntimeError(f"{name} must use strict YYYY-MM-DD format")
    try:
        return date.fromisoformat(raw)
    except ValueError as exc:
        raise RuntimeError(f"{name} must be a valid YYYY-MM-DD date") from exc


@dataclass(frozen=True, slots=True)
class ApiConfig:
    db_path: Path
    reports_root: Path
    legacy_db_path: Path
    operator_freeze_lock: Path
    writer_lock: Path = DEFAULT_WRITER_LOCK
    scheduler_enabled: bool = False
    startup_catchup_enabled: bool = False
    read_only: bool = False
    daily_capture_reconcile_from: date | None = None

    @classmethod
    def from_env(cls) -> "ApiConfig":
        freeze_value = os.environ.get("DCAR_OPERATOR_FREEZE_LOCK") or os.environ.get(
            "DCAR_FREEZE_LOCK"
        )
        scheduler_enabled = _enabled("DCAR_SCHEDULER_ENABLED")
        reconcile_from = _optional_strict_iso_date(
            "DCAR_DAILY_CAPTURE_RECONCILE_FROM"
        )
        config = cls(
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
            writer_lock=Path(
                os.environ.get("DCAR_WRITER_LOCK", str(DEFAULT_WRITER_LOCK))
            ),
            scheduler_enabled=scheduler_enabled,
            startup_catchup_enabled=_enabled("DCAR_STARTUP_CATCHUP_ENABLED"),
            read_only=_enabled("DCAR_READ_ONLY"),
            daily_capture_reconcile_from=reconcile_from,
        )
        config.validate_daily_capture_reconcile_contract()
        return config

    def validate_daily_capture_reconcile_contract(self) -> None:
        """Reject reconcile settings that cannot be honored by this runtime."""

        if self.daily_capture_reconcile_from is not None:
            if not self.scheduler_enabled:
                raise RuntimeError(
                    "DCAR_DAILY_CAPTURE_RECONCILE_FROM requires "
                    "DCAR_SCHEDULER_ENABLED=1"
                )
            if self.read_only:
                raise RuntimeError(
                    "DCAR_DAILY_CAPTURE_RECONCILE_FROM requires writable mode"
                )
        if self.scheduler_enabled and self.daily_capture_reconcile_from is None:
            raise RuntimeError(
                "DCAR_DAILY_CAPTURE_RECONCILE_FROM is required when "
                "DCAR_SCHEDULER_ENABLED=1"
            )

    @property
    def effective_startup_catchup_enabled(self) -> bool:
        return self.scheduler_enabled and self.startup_catchup_enabled

    @property
    def effective_daily_capture_reconcile_from(self) -> date | None:
        self.validate_daily_capture_reconcile_contract()
        return self.daily_capture_reconcile_from


def _request_config(request: Request) -> ApiConfig:
    config = getattr(request.app.state, "config", None)
    if not isinstance(config, ApiConfig):
        raise RuntimeError("FastAPI application is missing ApiConfig")
    return config


def _connect_for_request(request: Request) -> sqlite3.Connection:
    config = _request_config(request)
    return connect(config.db_path, read_only=config.read_only)


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


SELLING_POINT_NONE = "__none__"


class ContentSearchRequest(BaseModel):
    query: str = Field(default="", max_length=200)
    platform: Optional[str] = Field(default=None, max_length=32)
    account_type: Optional[str] = Field(default=None, max_length=32)
    content_direction: Optional[str] = Field(default=None, max_length=32)
    selling_point: Optional[str] = Field(default=None, max_length=8)
    spu_series: Optional[str] = Field(default=None, max_length=120)
    audience: Optional[str] = Field(default=None, max_length=16)
    scene: Optional[str] = Field(default=None, max_length=16)
    page: int = Field(default=1, ge=1)
    page_size: int = Field(default=50, ge=1, le=100)


class SellingPointMutationRequest(BaseModel):
    model_config = ConfigDict(extra="forbid")

    code: Optional[str] = Field(default=None, max_length=3)
    tier: str = Field(max_length=16)
    label: str = Field(min_length=1, max_length=300)
    definition: str = Field(default="", max_length=2000)
    matcher_rule: Dict[str, Any]


class SpuAliasPayload(BaseModel):
    alias: str = Field(min_length=1, max_length=60)
    alias_type: str = Field(default="official", max_length=16)
    ambiguous: bool = False


class SpuUpsertRequest(BaseModel):
    model_config = ConfigDict(extra="forbid")

    spu_id: Optional[str] = Field(default=None, max_length=160)
    brand: str = Field(min_length=1, max_length=60)
    series: str = Field(min_length=1, max_length=80)
    trim_label: Optional[str] = Field(default=None, max_length=120)
    model_year: Optional[int] = Field(default=None, ge=1990, le=2100)
    powertrain: str = Field(default="", max_length=16)
    body_style: str = Field(default="", max_length=16)
    price_low: Optional[float] = Field(default=None, ge=0)
    price_high: Optional[float] = Field(default=None, ge=0)
    enabled: bool = True
    audience_primary: Optional[str] = Field(default=None, max_length=8)
    audience_secondary: Optional[str] = Field(default=None, max_length=8)
    basis: str = Field(default="页面配置", max_length=300)
    aliases: List[SpuAliasPayload] = Field(default_factory=list)


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
    if _enabled("DCAR_READ_ONLY"):
        if not legacy_db_path.is_file():
            raise RuntimeError(
                f"read-only legacy SQLite database is missing: {legacy_db_path}"
            )
        connection = sqlite3.connect(
            f"{legacy_db_path.resolve().as_uri()}?mode=ro&immutable=1",
            uri=True,
            timeout=10,
        )
        connection.execute("PRAGMA query_only = ON")
    else:
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
    connection: sqlite3.Connection,
    start: datetime,
    end: datetime,
    *,
    report_cutoff_at: Optional[str] = None,
) -> Dict[str, Any]:
    contract = load_contract(report_version=CURRENT_REPORT_VERSION)
    coverage_thresholds = contract["required_coverage_thresholds"]
    metric_display_thresholds = contract["metric_display_coverage_thresholds"]
    evaluation_minimum = float(coverage_thresholds["evaluation_coverage"])
    fingerprint_minimum = float(
        coverage_thresholds["duplicate_fingerprint_coverage"]
    )
    view_minimum = float(metric_display_thresholds["view_count"])
    comment_minimum = float(metric_display_thresholds["comment_count"])
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
        release = active_release(connection)
        assert release is not None
        eligible_by_content = formal_eligible_release_evaluations(
            connection, str(release["id"]), content_ids
        )
        tier_by_code = {
            str(row["code"]): row["tier"]
            for row in connection.execute(
                """
                SELECT sp.code,sp.tier FROM selling_points sp
                JOIN taxonomy_versions tv ON tv.id=sp.taxonomy_id
                WHERE tv.version=?
                """,
                (release["taxonomy_version"],),
            ).fetchall()
        }
        evaluations = [
            {
                **value,
                "primary_tier": tier_by_code.get(
                    str(value["primary_selling_point_code"])
                )
                if value.get("primary_selling_point_code")
                else None,
            }
            for value in eligible_by_content.values()
        ]
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
        formal_content = {**content, "evaluation_content_direction": None}
        conclusion_rows.append(
            {
                "content_id": content_id,
                "platform": str(content["platform"]),
                "content_direction": effective_direction(formal_content, evaluation),
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
    view_values = [
        int(row["view_count"]) for row in metric_values if row["view_count"] is not None
    ]
    comment_values = [
        int(row["comment_count"])
        for row in metric_values
        if row["comment_count"] is not None
    ]
    view_coverage = round(len(view_values) * 100 / total, 2) if total else None
    comment_coverage = round(len(comment_values) * 100 / total, 2) if total else None
    views = sum(view_values)
    comments = sum(comment_values)
    evaluation_coverage = round(eligible * 100 / total, 2) if total else None
    ratio_status = (
        "not_applicable"
        if total == 0
        else "available"
        if (evaluation_coverage or 0) >= evaluation_minimum
        else "below_threshold"
    )
    evaluation_reason = (
        f"正式评估覆盖率为 {evaluation_coverage:.2f}%，低于 "
        f"{evaluation_minimum:g}% 发布阈值"
        if ratio_status == "below_threshold" and evaluation_coverage is not None
        else ""
    )
    duplicate_status, duplicate_coverage, duplicate_reason = duplicate_metric_decision(
        total,
        fingerprint_count,
        duplicate_calibrated,
        threshold=fingerprint_minimum,
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
                views if view_values else None,
                unit="view",
                status="not_applicable"
                if total == 0
                else "missing"
                if not view_values
                else "available"
                if (view_coverage or 0) >= view_minimum
                else "below_threshold",
                coverage_percentage=view_coverage,
                reason=(
                    f"曝光量快照覆盖率为 {view_coverage:.2f}%，低于 "
                    f"{view_minimum:g}% 发布阈值"
                    if view_values and (view_coverage or 0) < view_minimum
                    else ""
                ),
            ),
            "comment_count": quantity_metric(
                comments if comment_values else None,
                unit="comment",
                status="not_applicable"
                if total == 0
                else "missing"
                if not comment_values
                else "available"
                if (comment_coverage or 0) >= comment_minimum
                else "below_threshold",
                coverage_percentage=comment_coverage,
                reason=(
                    f"评论量快照覆盖率为 {comment_coverage:.2f}%，低于 "
                    f"{comment_minimum:g}% 发布阈值"
                    if comment_values and (comment_coverage or 0) < comment_minimum
                    else ""
                ),
            ),
            "verticality_rate": ratio_metric(
                vertical if total else None,
                total,
                status=ratio_status,
                eligible_count=eligible,
                coverage_percentage=evaluation_coverage,
                reason=evaluation_reason,
            ),
            "selling_point_coverage_rate": ratio_metric(
                selling if total else None,
                total,
                status=ratio_status,
                eligible_count=eligible,
                coverage_percentage=evaluation_coverage,
                reason=evaluation_reason,
            ),
            "duplicate_rate": ratio_metric(
                duplicate_count if total else None,
                total,
                status=duplicate_status,
                eligible_count=fingerprint_count,
                coverage_percentage=duplicate_coverage,
                reason=duplicate_reason,
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
            audience_rates=_audience_rates(
                connection,
                conclusion_rows,
                end,
                report_cutoff_at=report_cutoff_at,
            ),
        ),
    }


def _audience_rates(
    connection: sqlite3.Connection,
    conclusion_rows: List[Dict[str, Any]],
    window_end: datetime,
    *,
    report_cutoff_at: Optional[str] = None,
) -> Dict[str, Dict[str, Any]]:
    """Compute per-slice automotive_user_rate for the overview window.

    An uncalibrated classifier may publish only a complete classified sample;
    missing identities or classifications always keep the percentage hidden.
    """

    evidence_window_end = _utc_text(window_end)
    evidence_window_start = _utc_text(window_end - timedelta(days=90))
    return build_channel_audience_rates(
        connection,
        conclusion_rows,
        classifier_state=_active_classifier_state(connection),
        evidence_window_start=evidence_window_start,
        evidence_window_end=evidence_window_end,
        report_cutoff_at=report_cutoff_at or now_utc(),
        warm_up=True,
        channels=CHANNELS,
        scenes=SCENES,
    )


def _active_classifier_state(connection: sqlite3.Connection) -> str:
    """Resolve the calibrated classifier state through the shared gate."""

    return active_classifier_state(connection)


def _parse_timestamp(value: Optional[str]) -> Optional[datetime]:
    if not value:
        return None
    try:
        parsed = datetime.fromisoformat(str(value).replace("Z", "+00:00"))
    except ValueError:
        return None
    if parsed.tzinfo is None:
        parsed = parsed.replace(tzinfo=timezone.utc)
    return parsed.astimezone(timezone.utc)


def _data_freshness(
    connection: sqlite3.Connection,
    *,
    current_at: Optional[datetime] = None,
) -> Dict[str, Any]:
    """Summarize daily-capture freshness without treating backfills as daily runs."""

    latest_published_at = connection.execute(
        """
        SELECT MAX(published_at) FROM content_items
        WHERE platform IN ('douyin','xiaohongshu')
        """
    ).fetchone()[0]
    last_successful_capture_at = connection.execute(
        """
        SELECT MAX(finished_at) FROM fetch_slots
        WHERE stage='discovery' AND status='succeeded'
          AND window_key GLOB
              '[0-9][0-9][0-9][0-9]-[0-9][0-9]-[0-9][0-9]*'
        """
    ).fetchone()[0]
    latest_capture_row = connection.execute(
        """
        SELECT scheduled_for,status,completed_at
        FROM scheduler_runs
        WHERE job_id='daily_capture'
        ORDER BY scheduled_for DESC,id DESC
        LIMIT 1
        """
    ).fetchone()

    captured_at = _parse_timestamp(last_successful_capture_at)
    reference = current_at or datetime.now(timezone.utc)
    if reference.tzinfo is None:
        reference = reference.replace(tzinfo=timezone.utc)
    reference = reference.astimezone(timezone.utc)
    if captured_at is not None:
        status = (
            "current" if reference - captured_at <= timedelta(hours=36) else "stale"
        )
    elif latest_published_at:
        status = "stale"
    else:
        status = "unknown"

    return {
        "status": status,
        "latest_published_at": latest_published_at,
        "last_successful_capture_at": last_successful_capture_at,
        "latest_capture_run": dict(latest_capture_row)
        if latest_capture_row is not None
        else None,
    }


def _database_state(connection: sqlite3.Connection) -> Dict[str, Any]:
    """Return cheap snapshot identity fields used by deployment smoke checks."""

    compatibility = schema_compatibility_state(connection)
    user_version = int(connection.execute("PRAGMA user_version").fetchone()[0])
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
    if len(release_rows) != 1:
        raise RuntimeError(
            "database identity requires exactly one active evaluation release"
        )
    release = release_rows[0]
    runtime_identity = {
        "schema": RUNTIME_IDENTITY_SCHEMA,
        "report_version": CURRENT_REPORT_VERSION,
        "database_schema_version": user_version,
        "database_schema_migration": compatibility["actual_migration_name"],
        "active_release_id": str(release["id"]),
        "active_release_status": str(release["release_status"]),
        "rule_version": str(release["rule_version"]),
        "taxonomy_version": str(release["taxonomy_version"]),
        "taxonomy_status": str(release["taxonomy_status"]),
        "matcher_rule_sha256": str(release["matcher_rule_sha256"]),
    }
    return {
        "content_count": int(
            connection.execute("SELECT COUNT(*) FROM content_items").fetchone()[0]
        ),
        "latest_published_at": connection.execute(
            "SELECT MAX(published_at) FROM content_items"
        ).fetchone()[0],
        "user_version": user_version,
        "schema_compatibility": compatibility,
        "runtime_identity": runtime_identity,
    }


def _file_sha256(path: Path) -> str:
    digest = hashlib.sha256()
    with path.open("rb") as handle:
        for block in iter(lambda: handle.read(1024 * 1024), b""):
            digest.update(block)
    return digest.hexdigest()


@lru_cache(maxsize=2048)
def _cached_file_sha256(path_value: str, byte_size: int, mtime_ns: int) -> str:
    del byte_size, mtime_ns
    return _file_sha256(Path(path_value))


def v8_overview(db_path: Path, *, read_only: bool = False) -> Dict[str, Any]:
    overview_now = datetime.now(SHANGHAI)
    report_cutoff_at = now_utc()
    with connect(db_path, read_only=read_only) as connection:
        windows = {
            key: _window_summary(
                connection,
                start,
                end,
                report_cutoff_at=report_cutoff_at,
            )
            for key, (start, end) in _windows(overview_now).items()
        }
        quality = {
            "missing_published_at": int(
                connection.execute(
                    "SELECT COUNT(*) FROM content_items WHERE published_at IS NULL"
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
        data_freshness = _data_freshness(
            connection,
            current_at=overview_now,
        )
    return {
        "status": "ready",
        "report_version": CURRENT_REPORT_VERSION,
        "generated_at": report_cutoff_at,
        "timezone": "Asia/Shanghai",
        "windows": windows,
        "data_quality": quality,
        "data_freshness": data_freshness,
    }


def _account_search(
    payload: AccountSearchRequest, *, db_path: Path, read_only: bool = False
) -> Dict[str, Any]:
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
    with connect(db_path, read_only=read_only) as connection:
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


def _content_search(
    payload: ContentSearchRequest, *, db_path: Path, read_only: bool = False
) -> Dict[str, Any]:
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
    if payload.selling_point == SELLING_POINT_NONE:
        where.append("ev.primary_selling_point_code IS NULL")
    elif payload.selling_point:
        where.append("ev.primary_selling_point_code=?")
        parameters.append(payload.selling_point)
    # SPU/人群/场景 标签筛选：只在 v14 关联域可用时生效（只读副本可能仍是 v13）
    spu_filters: List[str] = []
    spu_parameters: List[Any] = []
    if payload.spu_series == SELLING_POINT_NONE:
        spu_filters.append(
            "NOT EXISTS (SELECT 1 FROM content_spu_links sl WHERE sl.content_id=c.id"
            " AND sl.invalidated_at IS NULL AND sl.is_primary=1)"
        )
    elif payload.spu_series:
        spu_filters.append(
            "EXISTS (SELECT 1 FROM content_spu_links sl JOIN spu_catalog sc2 ON sc2.spu_id=sl.spu_id"
            " WHERE sl.content_id=c.id AND sl.invalidated_at IS NULL AND sl.is_primary=1"
            " AND sc2.series_slug=?)"
        )
        spu_parameters.append(payload.spu_series)
    if payload.audience == SELLING_POINT_NONE:
        spu_filters.append(
            "NOT EXISTS (SELECT 1 FROM content_audience_links al WHERE al.content_id=c.id"
            " AND al.invalidated_at IS NULL)"
        )
    elif payload.audience:
        spu_filters.append(
            "EXISTS (SELECT 1 FROM content_audience_links al WHERE al.content_id=c.id"
            " AND al.invalidated_at IS NULL AND al.audience_code=?)"
        )
        spu_parameters.append(payload.audience)
    if payload.scene == SELLING_POINT_NONE:
        spu_filters.append(
            "NOT EXISTS (SELECT 1 FROM content_scene_links cl WHERE cl.content_id=c.id"
            " AND cl.invalidated_at IS NULL)"
        )
    elif payload.scene:
        spu_filters.append(
            "EXISTS (SELECT 1 FROM content_scene_links cl WHERE cl.content_id=c.id"
            " AND cl.invalidated_at IS NULL AND cl.scene_code=?)"
        )
        spu_parameters.append(payload.scene)
    where_sql = f"WHERE {' AND '.join(where)}" if where else ""
    from_sql = """
        FROM content_items c
        LEFT JOIN accounts a ON a.id=c.account_id
        LEFT JOIN display_effective_evaluations ev ON ev.content_id=c.id
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
    with connect(db_path, read_only=read_only) as connection:
        labels_ready = spu_domain_ready(connection)
        if labels_ready and spu_filters:
            where.extend(spu_filters)
            parameters.extend(spu_parameters)
            where_sql = f"WHERE {' AND '.join(where)}"
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
                   ev.content_automotive_score,
                   ev.id display_evaluation_id,
                   ev.release_id evaluation_release_id,
                   COALESCE(ev.evaluation_freshness, 'missing') evaluation_freshness,
                   CASE WHEN ev.evaluation_freshness='stale' THEN 1 ELSE 0 END evaluation_is_stale,
                   ms.view_count, ms.comment_count, ms.like_count, ms.share_count,
                   ms.collect_count, ms.captured_at metrics_captured_at,
                   original.link_id duplicate_original_link_id
            {from_sql} {where_sql}
            ORDER BY c.published_at IS NULL, c.published_at DESC, c.id DESC
            LIMIT ? OFFSET ?
            """,
            [*parameters, payload.page_size, offset],
        ).fetchall()
        items = [dict(row) for row in rows]
        tag_labels = (
            spu_content_labels(connection, [int(item["id"]) for item in items])
            if labels_ready
            else {}
        )
    for item in items:
        entry = tag_labels.get(int(item["id"])) or {
            "spu": None,
            "spu_secondary_count": 0,
            "spu_gray_count": 0,
            "audience": None,
            "scenes": [],
        }
        item["spu"] = entry["spu"]
        item["spu_secondary_count"] = entry["spu_secondary_count"]
        item["spu_gray_count"] = entry["spu_gray_count"]
        item["audience"] = entry["audience"]
        item["scenes"] = entry["scenes"]
    return {
        "items": items,
        "total": total,
        "page": payload.page,
        "page_size": payload.page_size,
    }


_SELLING_POINT_STAT_CHANNELS = ("douyin", "xiaohongshu")
_SELLING_POINT_STAT_SCENES = ("used_car", "new_car", "media")
_LATEST_METRICS_CTE = """
latest_metrics AS (
    SELECT ms.content_id, ms.view_count
    FROM content_metric_snapshots ms
    WHERE ms.id=(
        SELECT ms2.id FROM content_metric_snapshots ms2
        WHERE ms2.content_id=ms.content_id
        ORDER BY ms2.captured_at DESC, ms2.id DESC LIMIT 1
    )
)
""".strip()


def _selling_point_window_stats(
    connection: sqlite3.Connection,
) -> tuple[Dict[str, Any], Dict[str, Dict[str, Any]]]:
    """Per-window selling point hit/exposure stats plus scene/channel denominators.

    Windows reuse the overview boundaries (Asia/Shanghai). Count denominators are
    all publications in the window; exposure denominators only include contents
    with a positive latest view_count snapshot (valid exposure), mirroring the
    overview conclusion rules.
    """

    direction_sql = effective_direction_sql()
    windows_meta: Dict[str, Any] = {}
    point_windows: Dict[str, Dict[str, Any]] = {}
    for window_key, (start, end) in _windows().items():
        start_utc, end_utc = _utc_text(start), _utc_text(end)
        scene_denominators: Dict[str, Any] = {
            scene: {
                channel: {"publication_count": 0, "valid_exposure_views": 0}
                for channel in _SELLING_POINT_STAT_CHANNELS
            }
            for scene in _SELLING_POINT_STAT_SCENES
        }
        # Correlated indexed lookups instead of joining the materialized
        # formal/latest CTEs: SQLite nest-loops those materializations without
        # an index (full scan per content row), which made each window query
        # take seconds on a ~60k-content library.  Semantics are unchanged:
        # latest valid active-release evaluation and latest metric snapshot.
        denominator_rows = connection.execute(
            f"""
            SELECT {direction_sql} direction, c.platform,
                   COUNT(*) publication_count,
                   SUM(CASE WHEN COALESCE(lm.view_count,0)>0 THEN lm.view_count ELSE 0 END) valid_exposure_views
            FROM content_items c
            LEFT JOIN accounts a ON a.id=c.account_id
            LEFT JOIN evaluation_versions ev ON ev.id=(
                SELECT ev2.id FROM evaluation_versions ev2
                WHERE ev2.content_id=c.id
                  AND ev2.release_id=(
                    SELECT id FROM evaluation_releases WHERE status='active'
                  )
                  AND ev2.invalidated_at IS NULL
                ORDER BY ev2.evaluated_at DESC, ev2.id DESC LIMIT 1
            )
            LEFT JOIN content_metric_snapshots lm ON lm.id=(
                SELECT ms2.id FROM content_metric_snapshots ms2
                WHERE ms2.content_id=c.id
                ORDER BY ms2.captured_at DESC, ms2.id DESC LIMIT 1
            )
            WHERE c.published_at >= ? AND c.published_at < ?
            GROUP BY direction, c.platform
            """,
            (start_utc, end_utc),
        ).fetchall()
        for row in denominator_rows:
            scene, channel = str(row["direction"]), str(row["platform"])
            if (
                scene not in scene_denominators
                or channel not in _SELLING_POINT_STAT_CHANNELS
            ):
                continue
            scene_denominators[scene][channel] = {
                "publication_count": int(row["publication_count"] or 0),
                "valid_exposure_views": int(row["valid_exposure_views"] or 0),
            }
        windows_meta[window_key] = {
            "period_start": start.isoformat(),
            "period_end": end.isoformat(),
            "scene_denominators": scene_denominators,
        }
        hit_rows = connection.execute(
            f"""
            WITH {FORMAL_CURRENT_EVALUATIONS_CTE},
            {_LATEST_METRICS_CTE},
            matched AS (
                SELECT DISTINCT em.selling_point_code code, em.scene, em.match_role,
                       ev.content_id, c.platform
                FROM formal_current_evaluations ev
                JOIN evaluation_matches em ON em.evaluation_id=ev.id
                JOIN content_items c ON c.id=ev.content_id
                WHERE c.published_at >= ? AND c.published_at < ?
            )
            SELECT m.code, m.scene, m.platform,
                   COUNT(DISTINCT CASE WHEN m.match_role='primary' THEN m.content_id END) primary_hits,
                   COUNT(DISTINCT m.content_id) total_hits,
                   SUM(CASE WHEN m.match_role='primary' AND COALESCE(lm.view_count,0)>0 THEN lm.view_count ELSE 0 END) primary_views
            FROM matched m
            LEFT JOIN latest_metrics lm ON lm.content_id=m.content_id
            GROUP BY m.code, m.scene, m.platform
            """,
            (start_utc, end_utc),
        ).fetchall()
        for row in hit_rows:
            scene, channel = str(row["scene"]), str(row["platform"])
            if scene not in _SELLING_POINT_STAT_SCENES:
                continue
            scene_map = point_windows.setdefault(str(row["code"]), {}).setdefault(
                window_key, {}
            )
            entry = scene_map.setdefault(
                scene,
                {
                    "primary_hits": 0,
                    "total_hits": 0,
                    "channels": {
                        key: {"primary_hits": 0, "primary_views": 0}
                        for key in _SELLING_POINT_STAT_CHANNELS
                    },
                },
            )
            entry["primary_hits"] += int(row["primary_hits"] or 0)
            entry["total_hits"] += int(row["total_hits"] or 0)
            if channel in entry["channels"]:
                entry["channels"][channel] = {
                    "primary_hits": int(row["primary_hits"] or 0),
                    "primary_views": int(row["primary_views"] or 0),
                }
    return windows_meta, point_windows


def _selling_point_list(*, db_path: Path, read_only: bool = False) -> Dict[str, Any]:
    with connect(db_path, read_only=read_only) as connection:
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
        windows_meta, point_windows = _selling_point_window_stats(connection)
        items: List[Dict[str, Any]] = []
        for row in rows:
            point = serialize_point_row(connection, taxonomy, row)
            items.append(
                {
                    **point,
                    "enabled": bool(row["enabled"]),
                    "primary_hits": row["primary_hits"],
                    "total_hits": row["total_hits"],
                    "window_hits": point_windows.get(str(row["code"]), {}),
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
        "windows": windows_meta,
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


def _replica_artifact_path(value: str, *, read_only: bool) -> Path:
    try:
        return _safe_project_path(value)
    except HTTPException:
        if not read_only or not Path(value).is_absolute():
            raise
        normalized = value.replace("\\", "/")
        for marker in ("/data/cache/", "/reports/"):
            if marker in normalized:
                suffix = normalized.split(marker, 1)[1]
                return _safe_project_path(marker.strip("/") + "/" + suffix)
        raise


def _artifact_media_paths(
    row: sqlite3.Row, *, read_only: bool = False
) -> List[Path]:
    try:
        path = _replica_artifact_path(str(row["local_path"]), read_only=read_only)
    except (HTTPException, OSError):
        return []
    if path.suffix.lower() != ".json":
        if not path.is_file():
            return []
        if read_only:
            expected_sha = str(row["sha256"] or "")
            expected_size = row["byte_size"]
            try:
                metadata = path.stat()
                if re.fullmatch(r"[0-9a-f]{64}", expected_sha) is None:
                    return []
                if expected_size is not None and metadata.st_size != int(
                    expected_size
                ):
                    return []
                if (
                    _cached_file_sha256(
                        str(path), metadata.st_size, metadata.st_mtime_ns
                    )
                    != expected_sha
                ):
                    return []
            except OSError:
                return []
        return [path]
    # Legacy media manifests do not register per-child hashes in SQLite. The
    # read replica must not serve a same-path but stale child file as evidence.
    if read_only:
        return []
    try:
        value = json.loads(path.read_text(encoding="utf-8"))
    except (OSError, UnicodeDecodeError, json.JSONDecodeError):
        return []
    if not isinstance(value, dict):
        return []
    candidates: List[str] = []
    if value.get("video_path"):
        candidates.append(str(value["video_path"]))
    candidates.extend(
        str(item) for item in value.get("image_paths", []) if isinstance(item, str)
    )
    paths: List[Path] = []
    for candidate in candidates:
        try:
            resolved = _replica_artifact_path(candidate, read_only=read_only)
            if resolved.is_file():
                paths.append(resolved)
        except (HTTPException, OSError):
            continue
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


def _content_evidence(
    content_id: int, *, db_path: Path, read_only: bool = False
) -> Dict[str, Any]:
    with connect(db_path, read_only=read_only) as connection:
        content = connection.execute(
            "SELECT * FROM content_items WHERE id=?", (content_id,)
        ).fetchone()
        if content is None:
            raise HTTPException(status_code=404, detail="内容不存在")
        evaluation = display_effective_evaluation(connection, content_id)
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
    media_items: List[Dict[str, Any]] = []
    media_availability = {
        "status": "missing",
        "reason": "数据库没有可用的媒体证据记录。",
    }
    if media_row is not None:
        media_paths = _artifact_media_paths(media_row, read_only=read_only)
        for index, path in enumerate(media_paths):
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
        if media_paths:
            media_availability = {"status": "available", "reason": ""}
        elif read_only:
            media_availability = {
                "status": "omitted",
                "reason": "大媒体未随线上薄快照发布，现网也没有可安全复用的同路径同哈希文件。",
            }
        else:
            media_availability = {
                "status": "missing",
                "reason": "本地媒体证据文件缺失。",
            }
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
        "media_availability": media_availability,
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
    """Run report-only catch-up without blocking API startup."""
    try:
        results = startup_catchup(
            db_path=db_path, reports_root=reports_root
        )
    except Exception as exc:
        LOGGER.exception("startup catch-up failed")
        app.state.catchup_error = str(exc)
        app.state.catchup_status = "failed"
    else:
        app.state.catchup_results = results
        statuses = {str(item.get("status") or "") for item in results}
        if "failed" in statuses:
            app.state.catchup_status = "failed"
        elif statuses.intersection({"deferred", "skipped_duplicate"}):
            app.state.catchup_status = "deferred"
        elif statuses <= {
            "succeeded",
            "partial",
            "skipped",
        }:
            app.state.catchup_status = "succeeded"
        else:
            app.state.catchup_status = "failed"


@contextmanager
def _writer_process_lock(path: Path, *, enabled: bool):
    """Hold the single-writer lock for the scheduler process lifetime."""

    if not enabled:
        yield
        return
    path.parent.mkdir(parents=True, exist_ok=True)
    if path.is_symlink():
        raise RuntimeError(f"writer lock path must not be a symlink: {path}")
    descriptor = os.open(path, os.O_CREAT | os.O_RDWR | os.O_CLOEXEC, 0o600)
    try:
        try:
            fcntl.flock(descriptor, fcntl.LOCK_EX | fcntl.LOCK_NB)
        except BlockingIOError as exc:
            raise RuntimeError(
                f"another Dcar scheduler already holds the writer lock: {path}"
            ) from exc
        os.ftruncate(descriptor, 0)
        os.write(descriptor, f"pid={os.getpid()}\n".encode("ascii"))
        os.fsync(descriptor)
        yield
    finally:
        try:
            fcntl.flock(descriptor, fcntl.LOCK_UN)
        finally:
            os.close(descriptor)


def _uses_formal_database(path: Path) -> bool:
    """Return whether one runtime points at the checked-out formal database."""

    return is_formal_database_path(path, formal_database=DEFAULT_DB)


@asynccontextmanager
async def _lifespan_runtime(app: FastAPI):
    config = getattr(app.state, "config", None)
    if not isinstance(config, ApiConfig):
        raise RuntimeError("FastAPI application is missing ApiConfig")
    if config.read_only and (
        config.scheduler_enabled or config.startup_catchup_enabled
    ):
        raise RuntimeError(
            "read-only replica cannot enable scheduler or startup catch-up"
        )
    if config.read_only:
        if not config.db_path.is_file():
            raise RuntimeError(
                f"read-only replica database is missing: {config.db_path}"
            )
        with connect(config.db_path, read_only=True) as connection:
            require_schema_compatibility(
                connection, supported_versions=frozenset({SCHEMA_VERSION})
            )
            connection.execute("SELECT 1 FROM content_items LIMIT 1").fetchone()
        app.state.database_sha256 = _file_sha256(config.db_path)
        app.state.recovered_fetch_slots = {
            "stale_candidates": 0,
            "recovered": 0,
            "skipped": "read_only",
        }
        app.state.recovered_media_slots = {
            "stale_candidates": 0,
            "recovered": 0,
            "retryable_failed": 0,
            "terminal_failed": 0,
            "cas_conflicts": 0,
            "exhausted_normalized": 0,
            "skipped": "read_only",
        }
        app.state.recovered_tasks = 0
        app.state.recovered_scheduler_runs = 0
    else:
        if _uses_formal_database(config.db_path):
            if not config.db_path.is_file():
                raise RuntimeError(
                    f"formal SQLite database is missing: {config.db_path}"
                )
            # The formal database is migrated only by the explicit offline
            # migration command.  Runtime startup must fail closed without
            # opening a writable SQLite connection (which would create WAL/
            # SHM sidecars before a schema mismatch can be reported).
            with connect(config.db_path, read_only=True) as connection:
                try:
                    require_schema_compatibility(
                        connection,
                        supported_versions=frozenset({SCHEMA_VERSION}),
                    )
                except RuntimeError as exc:
                    raise RuntimeError(
                        "formal database schema is incompatible; "
                        "offline schema migration is required: "
                        f"{exc}"
                    ) from exc
        else:
            with connect(config.db_path) as connection:
                initialize_database(connection)
        app.state.recovered_scheduler_runs = recover_interrupted_scheduler_runs(
            db_path=config.db_path
        )
        app.state.recovered_fetch_slots = recover_stale_fetch_slots(
            db_path=config.db_path
        )
        app.state.recovered_media_slots = recover_stale_media_processing_slots(
            db_path=config.db_path,
            processor_version_by_type=processor_versions(),
        )
        app.state.recovered_tasks = _recover_interrupted_tasks(db_path=config.db_path)
        app.state.recovered_association_runs = recover_orphan_association_runs(
            db_path=config.db_path
        )
        app.state.database_sha256 = None
    app.state.scheduler_requested = config.scheduler_enabled
    app.state.scheduler_enabled = False
    app.state.startup_catchup_requested = config.startup_catchup_enabled
    app.state.startup_catchup_enabled = False
    app.state.daily_capture_reconcile_enabled = False
    app.state.daily_capture_reconcile_effective_from = (
        config.daily_capture_reconcile_from.isoformat()
        if config.daily_capture_reconcile_from is not None
        else None
    )
    app.state.report_runtime_ready = None
    app.state.report_runtime_error = None
    scheduler: Optional[BackgroundScheduler] = None
    if config.scheduler_enabled:
        try:
            with connect(config.db_path) as connection:
                assert_report_runtime_ready(connection)
        except Exception as exc:
            LOGGER.error(
                "automatic report jobs blocked by report runtime gate: %s", exc
            )
            app.state.report_runtime_ready = False
            app.state.report_runtime_error = str(exc)
        else:
            app.state.report_runtime_ready = True
    if config.scheduler_enabled:
        reconcile_effective_date = config.effective_daily_capture_reconcile_from
        if reconcile_effective_date is None:
            raise RuntimeError(
                "daily capture reconcile requires an effective date when "
                "scheduler is enabled"
            )
        scheduler = BackgroundScheduler(timezone="Asia/Shanghai")
        install_jobs(
            scheduler,
            db_path=config.db_path,
            reports_root=config.reports_root,
            capture_call_override=None,
            reconcile_effective_date=reconcile_effective_date,
        )
        scheduler.start()
        app.state.scheduler_enabled = True
        app.state.daily_capture_reconcile_enabled = True
    if config.effective_startup_catchup_enabled:
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
        app.state.catchup_status = "disabled"
        app.state.catchup_results = []
        app.state.catchup_error = None
    app.state.scheduler = scheduler
    try:
        yield
    finally:
        if scheduler is not None:
            scheduler.shutdown(wait=False)


@asynccontextmanager
async def lifespan(app: FastAPI):
    config = getattr(app.state, "config", None)
    if not isinstance(config, ApiConfig):
        raise RuntimeError("FastAPI application is missing ApiConfig")
    # Validate before the freeze check, writer-lock context, or any database
    # access so an impossible reconcile setting cannot leave runtime sidecars.
    config.validate_daily_capture_reconcile_contract()
    if _uses_formal_database(config.db_path) and config.operator_freeze_lock.exists():
        # Check the operator freeze before the scheduler lock context: entering
        # that context creates/truncates the lock file even when DB startup is
        # subsequently rejected.
        raise RuntimeError(
            "production startup blocked by operator freeze lock: "
            f"{config.operator_freeze_lock}"
        )
    app.state.writer_lock_path = str(config.writer_lock)
    app.state.writer_lock_held = False
    with _writer_process_lock(config.writer_lock, enabled=config.scheduler_enabled):
        app.state.writer_lock_held = config.scheduler_enabled
        try:
            async with _lifespan_runtime(app):
                yield
        finally:
            app.state.writer_lock_held = False


router = APIRouter()

READ_ONLY_POST_PATHS = frozenset(
    {
        "/api/v8/accounts/search",
        "/api/v8/contents/search",
        "/api/v8/contents/validate",
        "/api/v8/media-processing/search",
        "/api/inputs/validate",
    }
)


async def privacy_safe_request_log(request: Request, call_next):
    response = await call_next(request)
    LOGGER.info("%s %s %s", request.method, request.url.path, response.status_code)
    return response


async def read_only_replica_guard(request: Request, call_next):
    config = _request_config(request)
    allowed = request.method in {"GET", "HEAD", "OPTIONS"} or (
        request.method == "POST" and request.url.path in READ_ONLY_POST_PATHS
    )
    if config.read_only and not allowed:
        return JSONResponse(
            status_code=403,
            content={
                "detail": "线上只读副本禁止写入；请在本地唯一写入端操作后重新发布快照。"
            },
        )
    return await call_next(request)


def create_app(config: Optional[ApiConfig] = None) -> FastAPI:
    application = FastAPI(title="DCar Insight API", version="8.6", lifespan=lifespan)
    application.state.config = config or ApiConfig.from_env()
    application.add_middleware(
        CORSMiddleware,
        allow_origin_regex=r"http://(?:localhost|127\.0\.0\.1):\d{2,5}",
        allow_credentials=False,
        allow_methods=["GET", "POST", "PATCH", "DELETE", "OPTIONS"],
        allow_headers=["Content-Type"],
    )
    application.middleware("http")(privacy_safe_request_log)
    application.middleware("http")(read_only_replica_guard)
    application.include_router(router)
    return application


@router.get("/api/v8/health")
def v8_health(request: Request) -> Dict[str, Any]:
    config = _request_config(request)
    with connect(config.db_path, read_only=config.read_only) as connection:
        data_freshness = _data_freshness(connection)
        database_state = _database_state(connection)
        database_state["sha256"] = getattr(request.app.state, "database_sha256", None)
    return {
        "status": "ok",
        "mode": "read_only_replica" if config.read_only else "local_v8",
        "read_only": config.read_only,
        "report_version": CURRENT_REPORT_VERSION,
        "database": config.db_path.name,
        "database_state": database_state,
        "data_freshness": data_freshness,
    }


@router.get("/api/v8/overview")
def get_v8_overview(request: Request) -> Dict[str, Any]:
    config = _request_config(request)
    return v8_overview(config.db_path, read_only=config.read_only)


@router.get("/api/v8/tasks")
def get_v8_tasks(request: Request) -> Dict[str, Any]:
    items = list_tasks(db_path=_request_config(request).db_path)
    return {"items": items, "total": len(items)}


@router.get("/api/v8/scheduler")
def get_v8_scheduler_status(request: Request) -> Dict[str, Any]:
    config = _request_config(request)
    with connect(config.db_path, read_only=config.read_only) as connection:
        rows = connection.execute(
            """
            SELECT sr.*,
                   (
                       SELECT COUNT(*) FROM scheduler_run_attempts sra
                       WHERE sra.scheduler_run_id=sr.id
                   ) attempt_count,
                   (
                       SELECT MAX(attempt_number) FROM scheduler_run_attempts sra
                       WHERE sra.scheduler_run_id=sr.id
                   ) latest_attempt_number,
                   (
                       SELECT invocation_source FROM scheduler_run_attempts sra
                       WHERE sra.scheduler_run_id=sr.id
                       ORDER BY attempt_number DESC LIMIT 1
                   ) latest_invocation_source
            FROM scheduler_runs sr
            JOIN (
                SELECT job_id, MAX(scheduled_for) scheduled_for
                FROM scheduler_runs GROUP BY job_id
            ) latest ON latest.job_id=sr.job_id AND latest.scheduled_for=sr.scheduled_for
            ORDER BY sr.job_id
            """
        ).fetchall()
        data_freshness = _data_freshness(connection)
    return {
        "read_only": config.read_only,
        "requested": bool(getattr(request.app.state, "scheduler_requested", False)),
        "enabled": bool(getattr(request.app.state, "scheduler_enabled", False)),
        "writer_lock": {
            "path": getattr(request.app.state, "writer_lock_path", None),
            "held": bool(getattr(request.app.state, "writer_lock_held", False)),
        },
        "report_runtime": {
            "ready": getattr(request.app.state, "report_runtime_ready", None),
            "error": getattr(request.app.state, "report_runtime_error", None),
        },
        "startup_catchup": {
            "mode": "report_only",
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
        "daily_capture_reconcile": {
            "mode": "current_day_only",
            "enabled": bool(
                getattr(
                    request.app.state,
                    "daily_capture_reconcile_enabled",
                    False,
                )
            ),
            "effective_from": getattr(
                request.app.state,
                "daily_capture_reconcile_effective_from",
                None,
            ),
            "interval_seconds": DAILY_CAPTURE_RECONCILE_INTERVAL_SECONDS,
        },
        "data_freshness": data_freshness,
        "jobs": [dict(row) for row in rows],
        "scheduler_run_recovery": {
            "interrupted": int(
                getattr(request.app.state, "recovered_scheduler_runs", 0)
            )
        },
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


# Report generation is minutes long, so it never runs inside the request that
# asks for it: the endpoint returns a queued task and the workbench polls the
# task read model for progress. One lock keeps the single SQLite writer serial.
_TASK_RUN_LOCK = threading.Lock()


def _run_task_in_background(
    task_id: str, *, db_path: Path, reports_root: Path
) -> None:
    with _TASK_RUN_LOCK:
        try:
            run_task(task_id, db_path=db_path, reports_root=reports_root)
        except TaskCancelled:
            LOGGER.info("report task cancelled while generating: %s", task_id)
        except Exception:
            # run_task already records the failure on the task; keep the worker alive.
            LOGGER.exception("background report generation failed: %s", task_id)


def _queue_task_run(
    background: BackgroundTasks, task_id: str, config: ApiConfig
) -> None:
    background.add_task(
        _run_task_in_background,
        task_id,
        db_path=config.db_path,
        reports_root=config.reports_root,
    )


@router.post("/api/v8/tasks")
def create_v8_task(
    request: Request, payload: TaskCreateRequest, background: BackgroundTasks
) -> Dict[str, Any]:
    config = _request_config(request)
    try:
        with connect(config.db_path) as connection:
            assert_report_runtime_ready(connection)
        task = create_task(
            task_type="custom",
            period_start=payload.period_start,
            period_end=payload.period_end,
            creation_source="manual",
            name=payload.name,
            db_path=config.db_path,
        )
    except ReportTaskError as exc:
        raise HTTPException(status_code=409, detail=str(exc)) from exc
    if str(task["task_status"]) in IMPLICIT_RUN_STATUSES:
        _queue_task_run(background, str(task["id"]), config)
    return task


@router.get("/api/v8/tasks/{task_id}")
def get_v8_task(request: Request, task_id: str) -> Dict[str, Any]:
    try:
        return get_task(task_id, db_path=_request_config(request).db_path)
    except ReportTaskError as exc:
        raise HTTPException(status_code=404, detail=str(exc)) from exc


@router.post("/api/v8/tasks/{task_id}/retry")
def retry_v8_task(
    request: Request, task_id: str, background: BackgroundTasks
) -> Dict[str, Any]:
    config = _request_config(request)
    try:
        task = retry_task(task_id, db_path=config.db_path)
    except ReportTaskError as exc:
        raise HTTPException(status_code=409, detail=str(exc)) from exc
    _queue_task_run(background, task_id, config)
    return task


@router.post("/api/v8/tasks/{task_id}/cancel")
def cancel_v8_task(request: Request, task_id: str) -> Dict[str, Any]:
    try:
        return request_task_cancel(task_id, db_path=_request_config(request).db_path)
    except ReportTaskError as exc:
        raise HTTPException(status_code=409, detail=str(exc)) from exc


@router.post("/api/v8/tasks/{task_id}/resume")
def resume_v8_task(
    request: Request, task_id: str, background: BackgroundTasks
) -> Dict[str, Any]:
    config = _request_config(request)
    try:
        task = resume_task(task_id, db_path=config.db_path)
    except ReportTaskError as exc:
        raise HTTPException(status_code=409, detail=str(exc)) from exc
    _queue_task_run(background, task_id, config)
    return task


@router.get("/api/v8/tasks/{task_id}/revisions/{revision}/report")
def get_v8_task_report(request: Request, task_id: str, revision: int) -> Dict[str, Any]:
    with _connect_for_request(request) as connection:
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
    with _connect_for_request(request) as connection:
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


def _verified_report_file(row: Mapping[str, Any]) -> tuple[Path, bytes]:
    path = _safe_project_path(str(row["local_path"]))
    if not path.is_file():
        raise HTTPException(status_code=410, detail="报告文件已登记但本地缺失")
    try:
        payload = path.read_bytes()
    except OSError as exc:
        raise HTTPException(status_code=410, detail="报告文件读取失败") from exc
    expected_size = int(row["byte_size"])
    expected_sha256 = str(row["sha256"])
    if len(payload) != expected_size or hashlib.sha256(payload).hexdigest() != expected_sha256:
        raise HTTPException(status_code=409, detail="报告文件完整性校验失败")
    return path, payload


@router.get("/api/v8/tasks/{task_id}/revisions/{revision}/download")
def download_v8_task_report(
    request: Request, task_id: str, revision: int
) -> Response:
    """Download one immutable revision as an image + native XLSX ZIP bundle."""

    with _connect_for_request(request) as connection:
        task = connection.execute(
            """
            SELECT t.id,t.name,t.period_start,t.period_end,t.task_status,t.created_at,
                   rr.created_at revision_created_at
            FROM report_revisions rr
            JOIN report_tasks t ON t.id=rr.task_id
            WHERE rr.task_id=? AND rr.revision=?
            """,
            (task_id, revision),
        ).fetchone()
        files = connection.execute(
            """
            SELECT file_kind,local_path,sha256,byte_size
            FROM report_files
            WHERE task_id=? AND revision=? AND status='available'
              AND file_kind IN ('summary-svg','summary-png','content-csv','channel-csv')
            """,
            (task_id, revision),
        ).fetchall()
    if task is None:
        raise HTTPException(status_code=404, detail="报告 revision 不存在")

    by_kind = {str(row["file_kind"]): row for row in files}
    content_row = by_kind.get("content-csv")
    if content_row is None:
        raise HTTPException(status_code=404, detail="报告内容明细不存在")
    _, content_csv = _verified_report_file(content_row)

    channel_csv = None
    if channel_row := by_kind.get("channel-csv"):
        _, channel_csv = _verified_report_file(channel_row)

    image_extension: str
    image_bytes: bytes
    if svg_row := by_kind.get("summary-svg"):
        svg_path, svg_bytes = _verified_report_file(svg_row)
        with tempfile.TemporaryDirectory(prefix="dcar-report-download-") as directory:
            rendered = Path(directory) / "core_summary.png"
            if render_summary_png(svg_path, rendered):
                image_extension = "png"
                image_bytes = rendered.read_bytes()
            else:
                image_extension = "svg"
                image_bytes = svg_bytes
    elif png_row := by_kind.get("summary-png"):
        _, image_bytes = _verified_report_file(png_row)
        image_extension = "png"
    else:
        raise HTTPException(status_code=404, detail="报告图片不存在")

    task_value = dict(task)
    try:
        workbook = build_report_detail_workbook(
            task=task_value,
            revision=revision,
            content_csv=content_csv,
            channel_csv=channel_csv,
        )
        bundle = build_report_download_bundle(
            image_extension=image_extension,
            image_bytes=image_bytes,
            workbook_bytes=workbook,
        )
    except (UnicodeDecodeError, csv.Error, ValueError) as exc:
        LOGGER.exception(
            "report bundle generation failed: task=%s revision=%s", task_id, revision
        )
        raise HTTPException(status_code=500, detail="报告下载包生成失败") from exc

    filename = report_bundle_filename(
        task_name=str(task["name"]),
        task_id=task_id,
    )
    encoded_filename = quote(filename, safe="")
    return Response(
        content=bundle,
        media_type="application/zip",
        headers={
            "Content-Disposition": (
                f'attachment; filename="report.zip"; filename*=UTF-8\'\'{encoded_filename}'
            ),
            "Cache-Control": "private, no-store",
            "X-Content-Type-Options": "nosniff",
        },
    )


@router.post("/api/v8/accounts/search")
def search_v8_accounts(
    request: Request, payload: AccountSearchRequest
) -> Dict[str, Any]:
    config = _request_config(request)
    return _account_search(
        payload,
        db_path=config.db_path,
        read_only=config.read_only,
    )


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
    config = _request_config(request)
    return _content_search(
        payload,
        db_path=config.db_path,
        read_only=config.read_only,
    )


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
    db_path = _request_config(request).db_path
    try:
        result = update_content_data(content_id, db_path=db_path)
    except ProviderConfigurationError as exc:
        raise HTTPException(status_code=409, detail=str(exc)) from exc
    try:
        # 数据更新后即时补算 车型/人群/场景 标签，让内容列表立刻反映最新证据
        associate_single_content(content_id, db_path=db_path)
    except (SpuAudienceError, sqlite3.Error):
        LOGGER.exception("内容 %s 更新后补算三标签失败（不影响数据更新结果）", content_id)
    return result


@router.get("/api/v8/contents/{content_id}/evidence")
def get_v8_content_evidence(request: Request, content_id: int) -> Dict[str, Any]:
    config = _request_config(request)
    return _content_evidence(
        content_id,
        db_path=config.db_path,
        read_only=config.read_only,
    )


@router.get("/api/v8/contents/{content_id}/evidence/files/{artifact_id}/{index}")
def get_v8_content_evidence_file(
    request: Request, content_id: int, artifact_id: int, index: int
) -> FileResponse:
    with _connect_for_request(request) as connection:
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
    config = _request_config(request)
    paths = _artifact_media_paths(row, read_only=config.read_only)
    if not paths:
        if config.read_only:
            raise HTTPException(
                status_code=410,
                detail="媒体证据未随线上薄快照发布，且现网没有同路径同哈希文件可复用",
            )
        raise HTTPException(status_code=404, detail="媒体证据文件不存在")
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
    with _connect_for_request(request) as connection:
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
        config = _request_config(request)
        return _selling_point_list(
            db_path=config.db_path,
            read_only=config.read_only,
        )
    except TaxonomyError as exc:
        raise HTTPException(status_code=409, detail=str(exc)) from exc


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


@router.get("/api/v8/spu-audience/assets")
def get_v8_spu_audience_assets(request: Request) -> Dict[str, Any]:
    config = _request_config(request)
    try:
        return list_spu_audience_assets(
            db_path=config.db_path, read_only=config.read_only
        )
    except SpuAudienceError as exc:
        raise HTTPException(status_code=409, detail=str(exc)) from exc


@router.get("/api/v8/spu-audience/stats")
def get_v8_spu_audience_stats(
    request: Request, window: str = "all", platform: str = ""
) -> Dict[str, Any]:
    config = _request_config(request)
    if window not in STAT_WINDOWS:
        raise HTTPException(status_code=422, detail=f"不支持的统计窗口：{window}")
    if platform and platform not in STAT_PLATFORMS:
        raise HTTPException(status_code=422, detail=f"不支持的统计平台：{platform}")
    try:
        return build_spu_audience_stats(
            db_path=config.db_path,
            window=window,
            platform=platform,
            read_only=config.read_only,
        )
    except SpuAudienceError as exc:
        raise HTTPException(status_code=409, detail=str(exc)) from exc


@router.post("/api/v8/spu-audience/associate")
def run_v8_spu_association(
    request: Request, background: BackgroundTasks, mode: str = "full"
) -> Dict[str, Any]:
    """启动后台刷新任务并立即返回；进度经 assets 接口的 last_run 轮询。

    mode=full 全量重算；mode=incremental 只补算上次成功刷新之后新增/重评估
    的 V2/V3 内容（页面打开时自动触发的就是增量），首次运行没有成功记录
    时增量自动升级为全量；mode=yesterday/this_week/last_week 只重算发布
    时间落在对应统计窗口内的 V2/V3 内容（页面「刷新数据」弹窗四选一）。
    """

    if mode not in {"full", "incremental", "yesterday", "this_week", "last_week"}:
        raise HTTPException(status_code=422, detail=f"不支持的刷新方式：{mode}")
    config = _request_config(request)
    since: Optional[str] = None
    scope_window: Optional[str] = None
    if mode == "incremental":
        with connect(config.db_path, read_only=config.read_only) as connection:
            since = resolve_incremental_since(connection)
    elif mode != "full":
        scope_window = mode
    try:
        run_id = start_association_run(db_path=config.db_path)
    except SpuAudienceError as exc:
        raise HTTPException(status_code=409, detail=str(exc)) from exc
    background.add_task(
        _run_spu_association_job, config.db_path, run_id, since, scope_window
    )
    if since:
        resolved_mode = "incremental"
    elif scope_window:
        resolved_mode = f"window:{scope_window}"
    else:
        resolved_mode = "full"
    return {
        "run_id": run_id,
        "status": "running",
        "mode": resolved_mode,
    }


def _run_spu_association_job(
    db_path: Path,
    run_id: int,
    since: Optional[str] = None,
    scope_window: Optional[str] = None,
) -> None:
    """后台执行刷新；失败结论由 run_association 写回运行记录，这里补日志。

    规则链后自动挂 LLM 补空（B 链）：key 缺失时 hook 为 None，纯规则运行；
    LLM 阶段异常由 run_association 记入 summary_json.llm，不影响刷新状态。
    """

    try:
        run_spu_association(
            db_path=db_path, run_id=run_id, since=since,
            scope_window=scope_window,
            llm_hook=default_spu_llm_hook(),
        )
    except Exception:  # noqa: BLE001 —— 运行记录已标记 failed，仅记录堆栈
        LOGGER.exception("SPU 数据刷新后台任务失败 run_id=%s", run_id)


@router.post("/api/v8/spu-audience/spu")
def upsert_v8_spu(request: Request, payload: SpuUpsertRequest) -> Dict[str, Any]:
    try:
        return upsert_spu(
            payload.model_dump(), db_path=_request_config(request).db_path
        )
    except SpuAudienceError as exc:
        raise HTTPException(status_code=422, detail=str(exc)) from exc


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
