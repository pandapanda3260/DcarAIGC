"""In-process APScheduler jobs with database idempotency and bounded catch-up."""

from __future__ import annotations

import json
import threading
import time as time_module
from concurrent.futures import ThreadPoolExecutor
from dataclasses import dataclass
from datetime import datetime, time, timedelta, timezone
from pathlib import Path
from typing import Any, Callable, Dict, List, Mapping, Optional
from zoneinfo import ZoneInfo

from apscheduler.schedulers.background import BackgroundScheduler  # type: ignore[import-untyped]
from apscheduler.triggers.cron import CronTrigger  # type: ignore[import-untyped]

from .capture import ProviderResult, ensure_content_slot
from .duplicates import run_duplicate_fingerprint_queue
from .evaluation import evaluate_incremental
from .evaluation_selectors import review_anchor_evaluation
from .media import run_media_download_queue, run_media_processing_queue
from .providers import STAGE_CONFIG, discover_account_content, update_content_data
from .reports import (
    REPORTS_ROOT,
    ReportTaskError,
    assert_report_runtime_ready,
    create_and_run_task,
)
from .storage import DEFAULT_DB, connect, now_utc, transaction


SHANGHAI = ZoneInfo("Asia/Shanghai")
DAILY_CAPTURE_MAX_AMOUNT = 3.0
DAILY_CAPTURE_CONTENT_LIMIT = 1200
DAILY_DISCOVERY_MAX_PAGES = 20
DAILY_CAPTURE_WORKERS = 4
DAILY_CAPTURE_MAX_ATTEMPTS = 2
DAILY_CAPTURE_RETRY_DELAY_SECONDS = 1.0
CAPTURE_CIRCUIT_BREAK_CODES = frozenset(
    {"provider_balance_blocked", "provider_auth_blocked", "budget_blocked"}
)


@dataclass(frozen=True)
class JobDefinition:
    job_id: str
    hour: int
    minute: int
    day_of_week: Optional[str] = None


JOBS = (
    JobDefinition("daily_capture", 2, 0),
    JobDefinition("daily_media_download", 2, 20),
    JobDefinition("daily_media_processing", 3, 0),
    JobDefinition("daily_media_cutoff", 7, 30),
    JobDefinition("daily_report", 8, 0),
    JobDefinition("weekly_report", 8, 30, "mon"),
)
REPORT_JOB_IDS = frozenset({"daily_report", "weekly_report"})


class SchedulerJobError(RuntimeError):
    pass


def _require_report_job_runtime_ready(*, db_path: Path) -> Dict[str, Any]:
    with connect(db_path) as connection:
        return assert_report_runtime_ready(connection)


def _scheduled_iso(value: datetime) -> str:
    if value.tzinfo is None:
        value = value.replace(tzinfo=SHANGHAI)
    return (
        value.astimezone(timezone.utc)
        .isoformat(timespec="seconds")
        .replace("+00:00", "Z")
    )


def latest_occurrence(job: JobDefinition, now: datetime) -> datetime:
    local = now.astimezone(SHANGHAI)
    candidate = datetime.combine(local.date(), time(job.hour, job.minute), SHANGHAI)
    if job.day_of_week == "mon":
        candidate -= timedelta(days=candidate.weekday())
    if candidate > local:
        candidate -= timedelta(days=7 if job.day_of_week else 1)
    return candidate


def _claim_run(job_id: str, scheduled_for: datetime, *, db_path: Path) -> bool:
    key = _scheduled_iso(scheduled_for)
    started_at = now_utc()
    with connect(db_path) as connection, transaction(connection):
        cursor = connection.execute(
            """
            INSERT INTO scheduler_runs(
                job_id, scheduled_for, status, started_at, details_json
            ) VALUES (?, ?, 'running', ?, '{}')
            ON CONFLICT(job_id, scheduled_for) DO NOTHING
            """,
            (job_id, key, started_at),
        )
        return cursor.rowcount == 1


def _finish_run(
    job_id: str,
    scheduled_for: datetime,
    *,
    status: str,
    details: Dict[str, Any],
    db_path: Path,
) -> None:
    with connect(db_path) as connection, transaction(connection):
        connection.execute(
            """
            UPDATE scheduler_runs SET status=?, completed_at=?, details_json=?
            WHERE job_id=? AND scheduled_for=? AND status='running'
            """,
            (
                status,
                now_utc(),
                json.dumps(details, ensure_ascii=False, sort_keys=True),
                job_id,
                _scheduled_iso(scheduled_for),
            ),
        )
        if connection.execute("SELECT changes()").fetchone()[0] != 1:
            raise SchedulerJobError(
                f"scheduler run is no longer active: {job_id} "
                f"{_scheduled_iso(scheduled_for)}"
            )


def prepare_due_capture_slots(
    scheduled_for: datetime,
    *,
    db_path: Path = DEFAULT_DB,
) -> Dict[str, Any]:
    local_day = scheduled_for.astimezone(SHANGHAI).date()
    iso_week = local_day.isocalendar()
    week_key = f"{iso_week.year}-W{iso_week.week:02d}"
    day_key = local_day.isoformat()
    window_start = scheduled_for.astimezone(timezone.utc) - timedelta(days=30)
    with connect(db_path) as connection, transaction(connection):
        rows = connection.execute(
            """
            SELECT c.id, c.platform, c.published_at
            FROM content_items c JOIN accounts a ON a.id=c.account_id
            JOIN account_platform_identities api
              ON api.account_id=a.id AND api.platform=c.platform
            WHERE a.enabled=1 AND c.platform IN ('douyin','xiaohongshu')
            ORDER BY (c.published_at IS NULL) ASC, c.published_at DESC, c.id DESC
            """
        ).fetchall()
        counts = {"detail": 0, "metrics": 0, "comments": 0}
        for row in rows:
            platform = str(row["platform"])
            detail_provider, detail_adapter, _, _ = STAGE_CONFIG[(platform, "detail")]
            metrics_provider, metrics_adapter, _, _ = STAGE_CONFIG[
                (platform, "metrics")
            ]
            comments_provider, comments_adapter, _, _ = STAGE_CONFIG[
                (platform, "comments")
            ]
            lifetime = connection.execute(
                """
                SELECT 1 FROM fetch_slots
                WHERE content_id=? AND stage='detail' AND window_key='lifetime'
                  AND status='succeeded' LIMIT 1
                """,
                (row["id"],),
            ).fetchone()
            if lifetime is None:
                ensure_content_slot(
                    connection,
                    content_id=int(row["id"]),
                    stage="detail",
                    window_key="lifetime",
                    provider=detail_provider,
                    adapter_version=detail_adapter,
                )
                counts["detail"] += 1
            try:
                published = datetime.fromisoformat(
                    str(row["published_at"]).replace("Z", "+00:00")
                )
            except ValueError:
                continue
            if published < window_start:
                continue
            ensure_content_slot(
                connection,
                content_id=int(row["id"]),
                stage="metrics",
                window_key=day_key,
                provider=metrics_provider,
                adapter_version=metrics_adapter,
            )
            counts["metrics"] += 1
            if local_day.weekday() == 0:
                ensure_content_slot(
                    connection,
                    content_id=int(row["id"]),
                    stage="comments",
                    window_key=week_key,
                    provider=comments_provider,
                    adapter_version=comments_adapter,
                )
                counts["comments"] += 1
    return {"monitored_contents": len(rows), "prepared_slots": counts}


def _select_due_capture_contents(
    scheduled_for: datetime,
    *,
    db_path: Path,
    content_limit: int,
) -> List[Dict[str, Any]]:
    """Select only work that is due, with Beijing reporting windows first."""

    local_day = scheduled_for.astimezone(SHANGHAI).date()
    day_key = local_day.isoformat()
    iso_week = local_day.isocalendar()
    week_key = f"{iso_week.year}-W{iso_week.week:02d}"
    scheduled_utc = scheduled_for.astimezone(timezone.utc)
    window_start = scheduled_utc - timedelta(days=30)
    today_start = datetime.combine(local_day, time.min, SHANGHAI).astimezone(
        timezone.utc
    )
    yesterday_start = today_start - timedelta(days=1)
    week_start = today_start - timedelta(days=local_day.weekday())

    with connect(db_path) as connection:
        rows = connection.execute(
            """
            SELECT c.*,
              (
                SELECT MAX(f.updated_at) FROM fetch_slots f
                WHERE f.content_id=c.id
                  AND f.stage IN ('detail','metrics','comments')
              ) last_capture_touched_at
            FROM content_items c JOIN accounts a ON a.id=c.account_id
            WHERE a.enabled=1 AND c.platform IN ('douyin','xiaohongshu')
            """
        ).fetchall()
        slots = connection.execute(
            """
            SELECT f.*,
              (
                SELECT pr.source
                FROM fetch_attempts fa
                JOIN provider_raw_responses pr ON pr.fetch_attempt_id=fa.id
                WHERE fa.slot_id=f.id
                ORDER BY fa.attempt_number DESC, pr.id DESC LIMIT 1
              ) raw_source
            FROM fetch_slots f
            WHERE (f.stage='detail' AND f.window_key='lifetime')
               OR (f.stage='metrics' AND f.window_key=?)
               OR (f.stage='comments' AND f.window_key=?)
            """,
            (day_key, week_key),
        ).fetchall()
        valid_metric_contents = {
            int(row["content_id"])
            for row in connection.execute(
                """
                SELECT DISTINCT content_id FROM content_metric_snapshots
                WHERE window_key=? AND status='available' AND view_count IS NOT NULL
                """,
                (day_key,),
            ).fetchall()
        }
        comment_evidence_contents = {
            int(row["content_id"])
            for row in connection.execute(
                """
                SELECT DISTINCT content_id FROM comment_evidence_versions
                WHERE iso_week=?
                """,
                (week_key,),
            ).fetchall()
        }

    slots_by_key = {
        (int(slot["content_id"]), str(slot["stage"]), str(slot["window_key"])): dict(
            slot
        )
        for slot in slots
        if slot["content_id"] is not None
    }

    def needs_work(slot: Optional[Mapping[str, Any]], *, storage_ready: bool) -> bool:
        if slot is None:
            return True
        status = str(slot["status"])
        if status in {"pending", "retryable_failed"}:
            return True
        if status in {"running", "terminal_failed"}:
            return False
        if status != "succeeded":
            return False
        if str(slot["provider"]) == "legacy-cache":
            return False
        return not (
            str(slot.get("raw_source") or "") in {"live_applied", "derived_applied"}
            and storage_ready
        )

    due: List[Dict[str, Any]] = []
    for row in rows:
        content = dict(row)
        content_id = int(content["id"])
        detail_slot = slots_by_key.get((content_id, "detail", "lifetime"))
        detail_needed = needs_work(detail_slot, storage_ready=True)
        try:
            published = datetime.fromisoformat(
                str(content["published_at"]).replace("Z", "+00:00")
            ).astimezone(timezone.utc)
        except (TypeError, ValueError):
            published = None
        within_monitoring_window = bool(
            published is not None and published >= window_start
        )
        metrics_slot = slots_by_key.get((content_id, "metrics", day_key))
        comments_slot = slots_by_key.get((content_id, "comments", week_key))
        metrics_needed = within_monitoring_window and needs_work(
            metrics_slot, storage_ready=content_id in valid_metric_contents
        )
        comments_needed = within_monitoring_window and needs_work(
            comments_slot, storage_ready=content_id in comment_evidence_contents
        )
        if not (detail_needed or metrics_needed or comments_needed):
            continue
        if published is not None and yesterday_start <= published < today_start:
            priority = 0
        elif published is not None and today_start <= published < scheduled_utc:
            priority = 1
        elif published is not None and week_start <= published < yesterday_start:
            priority = 2
        else:
            priority = 3
        content.update(
            {
                "detail_needed": detail_needed,
                "metrics_needed": metrics_needed,
                "comments_needed": comments_needed,
                "_capture_priority": priority,
                "_published_sort": published.timestamp()
                if published is not None
                else float("inf"),
            }
        )
        due.append(content)

    def sort_key(content: Mapping[str, Any]) -> tuple[Any, ...]:
        priority = int(content["_capture_priority"])
        if priority < 3:
            return (
                priority,
                float(content["_published_sort"]),
                int(content["id"]),
            )
        return (
            priority,
            str(content.get("last_capture_touched_at") or ""),
            float(content["_published_sort"]),
            int(content["id"]),
        )

    due.sort(key=sort_key)
    return due[:content_limit]


def run_due_capture(
    scheduled_for: datetime,
    *,
    db_path: Path = DEFAULT_DB,
    call_override: Optional[Callable[[str, Mapping[str, Any]], ProviderResult]] = None,
    max_amount: float = DAILY_CAPTURE_MAX_AMOUNT,
    content_limit: int = DAILY_CAPTURE_CONTENT_LIMIT,
) -> Dict[str, Any]:
    if max_amount <= 0 or content_limit <= 0:
        raise SchedulerJobError("每日采集预算和内容队列上限必须为正数")
    local_day = scheduled_for.astimezone(SHANGHAI).date()
    task_id = f"daily-capture-{local_day.isoformat()}-bjt"
    with connect(db_path) as connection:
        identities = connection.execute(
            """
            SELECT api.*, a.enabled FROM account_platform_identities api
            JOIN accounts a ON a.id=api.account_id
            WHERE a.enabled=1 AND api.platform IN ('douyin','xiaohongshu')
            ORDER BY api.account_id, api.platform
            """
        ).fetchall()
    if not identities:
        return {
            "status": "skipped",
            "reason": "没有已启用的抖音或小红书账号",
            "monitored_accounts": 0,
            "discovery": [],
            "content_updates": [],
            "provider_cost": 0.0,
        }

    discovery: List[Dict[str, Any]] = []
    blocked_providers: set[str] = set()
    discovery_start = datetime.combine(
        local_day - timedelta(days=1), time.min, SHANGHAI
    ).astimezone(timezone.utc)
    discovery_end = scheduled_for.astimezone(timezone.utc)
    for identity in identities:
        provider = "TikHub"
        if provider in blocked_providers:
            discovery.append(
                {
                    "account_id": identity["account_id"],
                    "platform": identity["platform"],
                    "status": "circuit_break_skipped",
                }
            )
            continue
        cursor: Any = None
        seen_cursors: set[str] = set()
        pages: List[Dict[str, Any]] = []
        stopped_reason = "window_start_reached"
        account_status = "succeeded"
        for page_number in range(1, DAILY_DISCOVERY_MAX_PAGES + 1):
            cursor_key = json.dumps(
                cursor, ensure_ascii=False, sort_keys=True, default=str
            )
            if cursor_key in seen_cursors:
                stopped_reason = "cursor_repeated"
                break
            seen_cursors.add(cursor_key)
            window_key = (
                f"{local_day.isoformat()}:{identity['platform']}:page:{page_number}"
            )
            page = None
            page_error: Optional[Exception] = None
            for attempt_number in range(1, DAILY_CAPTURE_MAX_ATTEMPTS + 1):
                try:
                    page = discover_account_content(
                        int(identity["account_id"]),
                        str(identity["platform"]),
                        str(identity["uid"]),
                        as_of=local_day,
                        cursor=cursor,
                        window_key=window_key,
                        published_start=discovery_start,
                        published_end=discovery_end,
                        db_path=db_path,
                        call_override=call_override,
                        task_id=task_id,
                        task_max_amount=max_amount,
                    )
                    page_error = None
                    break
                except Exception as exc:
                    page_error = exc
                    should_retry = bool(getattr(exc, "retryable", False)) and (
                        getattr(exc, "error_code", "")
                        not in CAPTURE_CIRCUIT_BREAK_CODES
                    )
                    if not should_retry or attempt_number >= DAILY_CAPTURE_MAX_ATTEMPTS:
                        break
                    if call_override is None:
                        time_module.sleep(DAILY_CAPTURE_RETRY_DELAY_SECONDS)
            if page_error is not None or page is None:
                page_failure = page_error or SchedulerJobError("账号发现未返回结果")
                error_code = getattr(
                    page_failure, "error_code", type(page_failure).__name__
                )
                pages.append(
                    {
                        "page": page_number,
                        "status": "failed",
                        "error_code": error_code,
                        "message": str(page_failure),
                        "provider_cost": float(
                            getattr(page_failure, "provider_cost", 0.0) or 0.0
                        ),
                    }
                )
                account_status = "failed"
                stopped_reason = str(error_code)
                if error_code in CAPTURE_CIRCUIT_BREAK_CODES:
                    blocked_providers.add(provider)
                break
            pages.append({"page": page_number, **page})
            if page.get("status") == "partial":
                account_status = "partial"
            published_values: List[datetime] = []
            for value in page.get("page_published_at") or []:
                try:
                    published_values.append(
                        datetime.fromisoformat(str(value).replace("Z", "+00:00"))
                    )
                except ValueError:
                    continue
            if published_values and min(published_values) <= discovery_start:
                break
            next_cursor = page.get("next_cursor")
            if not page.get("has_more") or next_cursor in (None, ""):
                stopped_reason = "provider_exhausted"
                break
            cursor = next_cursor
        else:
            stopped_reason = "page_limit_reached"
            account_status = "partial"
        discovery.append(
            {
                "account_id": identity["account_id"],
                "platform": identity["platform"],
                "status": account_status,
                "pages": pages,
                "stopped_reason": stopped_reason,
                "inserted": sum(int(page.get("inserted") or 0) for page in pages),
                "updated": sum(int(page.get("updated") or 0) for page in pages),
                "provider_cost": round(
                    sum(float(page.get("provider_cost") or 0) for page in pages), 6
                ),
            }
        )

    with connect(db_path) as connection:
        eligible_contents = int(
            connection.execute(
                """
                SELECT COUNT(*)
                FROM content_items c JOIN accounts a ON a.id=c.account_id
                WHERE a.enabled=1 AND c.platform IN ('douyin','xiaohongshu')
                """
            ).fetchone()[0]
        )
    contents = _select_due_capture_contents(
        scheduled_for,
        db_path=db_path,
        content_limit=content_limit,
    )

    provider_blocked = threading.Event()
    if "TikHub" in blocked_providers:
        provider_blocked.set()

    def update_one_content(content: Mapping[str, Any]) -> Dict[str, Any]:
        if provider_blocked.is_set():
            return {"content_id": content["id"], "status": "circuit_break_skipped"}
        stages: List[str] = []
        if content["detail_needed"]:
            stages.append("detail")
        if content["metrics_needed"]:
            stages.append("metrics")
        if content["comments_needed"]:
            stages.append("comments")
        if not stages:
            return {"content_id": content["id"], "status": "already_succeeded"}
        try:
            result = update_content_data(
                int(content["id"]),
                as_of=local_day,
                db_path=db_path,
                call_override=call_override,
                stages=stages,
                process_media=False,
                task_id=task_id,
                task_max_amount=max_amount,
            )
        except Exception as exc:
            error_code = getattr(exc, "error_code", type(exc).__name__)
            if error_code in CAPTURE_CIRCUIT_BREAK_CODES:
                provider_blocked.set()
            return {
                "content_id": content["id"],
                "status": "failed",
                "error_code": error_code,
                "message": str(exc),
            }
        blocked = next(
            (
                item.get("error_code")
                for item in result["stages"]
                if item.get("error_code") in CAPTURE_CIRCUIT_BREAK_CODES
            ),
            None,
        )
        if blocked:
            provider_blocked.set()
        return result

    workers = min(DAILY_CAPTURE_WORKERS, max(1, len(contents)))
    with ThreadPoolExecutor(
        max_workers=workers, thread_name_prefix="dcar-capture"
    ) as pool:
        content_updates = list(pool.map(update_one_content, map(dict, contents)))

    retry_targets: List[tuple[int, Dict[str, Any], List[str]]] = []
    for index, (content, result) in enumerate(zip(contents, content_updates)):
        retry_stages = sorted(
            {
                str(stage["stage"])
                for stage in result.get("stages", [])
                if stage.get("status") == "failed"
                and stage.get("retryable") is True
                and stage.get("error_code") not in CAPTURE_CIRCUIT_BREAK_CODES
                and stage.get("stage") in {"detail", "metrics", "comments"}
            }
        )
        if retry_stages:
            retry_targets.append((index, dict(content), retry_stages))

    if retry_targets:
        if call_override is None:
            time_module.sleep(DAILY_CAPTURE_RETRY_DELAY_SECONDS)

        def retry_content_stages(
            target: tuple[int, Dict[str, Any], List[str]],
        ) -> tuple[int, Dict[str, Any]]:
            index, content, retry_stages = target
            initial = content_updates[index]
            try:
                retry_result = update_content_data(
                    int(content["id"]),
                    as_of=local_day,
                    db_path=db_path,
                    call_override=call_override,
                    stages=retry_stages,
                    process_media=False,
                    task_id=task_id,
                    task_max_amount=max_amount,
                )
            except Exception:
                return index, initial
            retried_by_stage = {
                str(stage["stage"]): stage for stage in retry_result.get("stages", [])
            }
            merged_stages = [
                retried_by_stage.get(str(stage.get("stage")), stage)
                for stage in initial.get("stages", [])
            ]
            remaining_failed = any(
                stage.get("status") == "failed" for stage in merged_stages
            )
            return index, {
                **initial,
                "status": "partial" if remaining_failed else "succeeded",
                "stages": merged_stages,
                "provider_cost": round(
                    float(initial.get("provider_cost") or 0)
                    + float(retry_result.get("provider_cost") or 0),
                    6,
                ),
            }

        retry_workers = min(DAILY_CAPTURE_WORKERS, len(retry_targets))
        with ThreadPoolExecutor(
            max_workers=retry_workers, thread_name_prefix="dcar-capture-retry"
        ) as pool:
            for index, retry_result in pool.map(retry_content_stages, retry_targets):
                content_updates[index] = retry_result
    if provider_blocked.is_set():
        blocked_providers.add("TikHub")

    provider_cost = sum(
        float(item.get("provider_cost") or 0) for item in discovery + content_updates
    )
    failed = sum(
        item.get("status") in {"failed", "partial"}
        for item in discovery + content_updates
    )
    return {
        "status": "failed" if failed else "succeeded",
        "task_id": task_id,
        "budget_max_amount": max_amount,
        "content_limit": content_limit,
        "monitored_accounts": len(identities),
        "monitored_contents": len(contents),
        "eligible_contents": eligible_contents,
        "discovery": discovery,
        "content_updates": content_updates,
        "blocked_providers": sorted(blocked_providers),
        "failed_operations": failed,
        "provider_cost": round(provider_cost, 6),
    }


def run_media_cutoff(
    scheduled_for: datetime,
    *,
    db_path: Path = DEFAULT_DB,
) -> Dict[str, Any]:
    """Route unfinished same-day media to review, then evaluate only final evidence state."""

    local_day = scheduled_for.astimezone(SHANGHAI).date()
    start = datetime.combine(local_day, time.min, SHANGHAI).astimezone(timezone.utc)
    end = start + timedelta(days=1)
    start_iso = start.isoformat(timespec="seconds").replace("+00:00", "Z")
    end_iso = end.isoformat(timespec="seconds").replace("+00:00", "Z")
    with connect(db_path) as connection:
        rows = connection.execute(
            """
            SELECT c.id, c.content_type,
              EXISTS(
                SELECT 1 FROM evidence_artifacts ea
                WHERE ea.content_id=c.id AND ea.artifact_type IN ('media','media_manifest')
                  AND ea.status='available'
              ) media_ready,
              EXISTS(
                SELECT 1 FROM evidence_artifacts ea
                WHERE ea.content_id=c.id AND ea.artifact_type='asr' AND ea.status='available'
              ) asr_ready,
              EXISTS(
                SELECT 1 FROM evidence_artifacts ea
                WHERE ea.content_id=c.id AND ea.artifact_type='ocr' AND ea.status='available'
              ) ocr_ready
            FROM content_items c
            WHERE c.platform IN ('douyin','xiaohongshu')
              AND c.content_type IN ('video','image')
              AND (
                (c.source_group='' AND c.imported_at>=? AND c.imported_at<?)
                OR EXISTS(
                    SELECT 1 FROM provider_raw_responses pr
                    WHERE pr.content_id=c.id AND pr.operation IN (
                        'douyin_video_detail','xiaohongshu_note_detail'
                    ) AND pr.captured_at>=? AND pr.captured_at<?
                )
              )
            ORDER BY c.id
            """,
            (start_iso, end_iso, start_iso, end_iso),
        ).fetchall()
    complete_ids: List[int] = []
    incomplete_ids: List[int] = []
    for row in rows:
        complete = bool(row["media_ready"] and row["ocr_ready"])
        if row["content_type"] == "video":
            complete = bool(complete and row["asr_ready"])
        (complete_ids if complete else incomplete_ids).append(int(row["id"]))
    with connect(db_path) as connection, transaction(connection):
        for content_id in incomplete_ids:
            queue_evaluation = review_anchor_evaluation(connection, content_id)
            connection.execute(
                """
                INSERT INTO review_queue(
                    content_id, evaluation_id, reason_code, priority, status,
                    created_at, updated_at
                ) VALUES (?, ?, 'media_processing_incomplete', 90, 'manual_required', ?, ?)
                ON CONFLICT(content_id, reason_code) DO UPDATE SET
                    evaluation_id=excluded.evaluation_id,
                    status=CASE
                        WHEN review_queue.status IN ('resolved','terminal_failed')
                        THEN review_queue.status ELSE 'manual_required' END,
                    updated_at=excluded.updated_at
                """,
                (
                    content_id,
                    queue_evaluation["id"] if queue_evaluation is not None else None,
                    now_utc(),
                    now_utc(),
                ),
            )
        for content_id in complete_ids:
            connection.execute(
                """
                UPDATE review_queue SET status='resolved', resolved_at=?, updated_at=?
                WHERE content_id=? AND reason_code='media_processing_incomplete'
                  AND status NOT IN ('resolved','terminal_failed')
                """,
                (now_utc(), now_utc(), content_id),
            )
    evaluation = evaluate_incremental(db_path=db_path)
    total = len(rows)
    coverage = round(len(complete_ids) / total, 4) if total else 1.0
    return {
        "window_start": start_iso,
        "window_end": end_iso,
        "candidates": total,
        "complete": len(complete_ids),
        "manual_required": len(incomplete_ids),
        "terminal_coverage": coverage,
        "threshold": 0.95,
        "threshold_status": "available" if coverage >= 0.95 else "below_threshold",
        "evaluation": evaluation,
    }


def _run_job_action(
    job_id: str,
    scheduled_for: datetime,
    *,
    db_path: Path,
    reports_root: Path,
    capture_call_override: Optional[
        Callable[[str, Mapping[str, Any]], ProviderResult]
    ] = None,
) -> tuple[str, Dict[str, Any]]:
    if job_id in REPORT_JOB_IDS:
        _require_report_job_runtime_ready(db_path=db_path)
    local_date = scheduled_for.astimezone(SHANGHAI).date()
    fresh_start = datetime.combine(local_date - timedelta(days=1), time.min, SHANGHAI)
    fresh_start_iso = _scheduled_iso(fresh_start)
    fresh_end_iso = _scheduled_iso(scheduled_for)
    if job_id == "daily_capture":
        details = run_due_capture(
            scheduled_for, db_path=db_path, call_override=capture_call_override
        )
        return str(details.pop("status")), details
    if job_id == "daily_media_download":
        fresh_content = run_media_download_queue(
            limit=100,
            db_path=db_path,
            published_start=fresh_start_iso,
            published_end=fresh_end_iso,
        )
        status = (
            "failed"
            if int(fresh_content.get("retryable_failed", 0)) > 0
            else "succeeded"
        )
        return status, {"fresh_content": fresh_content}
    if job_id == "daily_media_processing":
        media = run_media_processing_queue(
            limit=100,
            db_path=db_path,
            published_start=fresh_start_iso,
            published_end=fresh_end_iso,
        )
        duplicates = run_duplicate_fingerprint_queue(limit=500, db_path=db_path)
        status = (
            "failed"
            if int(media.get("retryable_failed", 0)) > 0
            or int(duplicates.get("failed", 0)) > 0
            else "succeeded"
        )
        return status, {"media": media, "duplicates": duplicates}
    if job_id == "daily_media_cutoff":
        return "succeeded", run_media_cutoff(scheduled_for, db_path=db_path)
    if job_id == "daily_report":
        target = local_date - timedelta(days=1)
        task = create_and_run_task(
            task_type="daily",
            period_start=target.isoformat(),
            period_end=target.isoformat(),
            creation_source="automatic",
            db_path=db_path,
            reports_root=reports_root,
        )
        return "succeeded", {"task_id": task["id"], "task_status": task["task_status"]}
    if job_id == "weekly_report":
        end = local_date - timedelta(days=1)
        start = end - timedelta(days=6)
        task = create_and_run_task(
            task_type="weekly",
            period_start=start.isoformat(),
            period_end=end.isoformat(),
            creation_source="automatic",
            db_path=db_path,
            reports_root=reports_root,
        )
        return "succeeded", {"task_id": task["id"], "task_status": task["task_status"]}
    raise SchedulerJobError(f"unknown scheduler job: {job_id}")


def execute_job(
    job_id: str,
    scheduled_for: datetime,
    *,
    db_path: Path = DEFAULT_DB,
    reports_root: Path = REPORTS_ROOT,
    capture_call_override: Optional[
        Callable[[str, Mapping[str, Any]], ProviderResult]
    ] = None,
) -> Dict[str, Any]:
    if job_id not in {job.job_id for job in JOBS}:
        raise SchedulerJobError(f"unknown scheduler job: {job_id}")
    if job_id in REPORT_JOB_IDS:
        try:
            _require_report_job_runtime_ready(db_path=db_path)
        except ReportTaskError as error:
            return {
                "job_id": job_id,
                "status": "deferred",
                "reason": "report_runtime_not_ready",
                "error": str(error),
            }
    if not _claim_run(job_id, scheduled_for, db_path=db_path):
        return {"job_id": job_id, "status": "skipped_duplicate"}
    try:
        status, details = _run_job_action(
            job_id,
            scheduled_for,
            db_path=db_path,
            reports_root=reports_root,
            capture_call_override=capture_call_override,
        )
    except Exception as exc:
        _finish_run(
            job_id,
            scheduled_for,
            status="failed",
            details={"error": str(exc)},
            db_path=db_path,
        )
        raise
    _finish_run(job_id, scheduled_for, status=status, details=details, db_path=db_path)
    return {"job_id": job_id, "status": status, "details": details}


def _live_job(
    job_id: str,
    *,
    db_path: Path,
    reports_root: Path,
) -> None:
    definition = next(job for job in JOBS if job.job_id == job_id)
    occurrence = latest_occurrence(definition, datetime.now(SHANGHAI))
    execute_job(job_id, occurrence, db_path=db_path, reports_root=reports_root)


def install_jobs(
    scheduler: BackgroundScheduler,
    *,
    db_path: Path = DEFAULT_DB,
    reports_root: Path = REPORTS_ROOT,
) -> None:
    for job in JOBS:
        scheduler.add_job(
            _live_job,
            CronTrigger(
                hour=job.hour,
                minute=job.minute,
                day_of_week=job.day_of_week,
                timezone=SHANGHAI,
            ),
            id=job.job_id,
            replace_existing=True,
            kwargs={
                "job_id": job.job_id,
                "db_path": db_path,
                "reports_root": reports_root,
            },
            coalesce=True,
            max_instances=1,
            misfire_grace_time=3600,
        )


def startup_catchup(
    *,
    now: Optional[datetime] = None,
    db_path: Path = DEFAULT_DB,
    reports_root: Path = REPORTS_ROOT,
) -> List[Dict[str, Any]]:
    current = now or datetime.now(SHANGHAI)
    results: List[Dict[str, Any]] = []
    for job in JOBS:
        latest = latest_occurrence(job, current)
        with connect(db_path) as connection:
            previous = connection.execute(
                """
                SELECT scheduled_for FROM scheduler_runs
                WHERE job_id=? AND status IN ('succeeded','skipped')
                ORDER BY scheduled_for DESC LIMIT 1
                """,
                (job.job_id,),
            ).fetchone()
        occurrences = [latest]
        if previous is not None:
            previous_dt = datetime.fromisoformat(
                str(previous["scheduled_for"]).replace("Z", "+00:00")
            ).astimezone(SHANGHAI)
            step = timedelta(days=7 if job.day_of_week else 1)
            occurrences = []
            candidate = previous_dt + step
            lower_bound = current - timedelta(days=28 if job.day_of_week else 7)
            while candidate <= latest:
                if candidate >= lower_bound:
                    occurrences.append(candidate)
                candidate += step
        for occurrence in occurrences:
            try:
                results.append(
                    execute_job(
                        job.job_id,
                        occurrence,
                        db_path=db_path,
                        reports_root=reports_root,
                    )
                )
            except Exception as exc:
                results.append(
                    {"job_id": job.job_id, "status": "failed", "error": str(exc)}
                )
    return results
