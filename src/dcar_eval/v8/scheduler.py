"""In-process APScheduler jobs with database idempotency and bounded catch-up."""

from __future__ import annotations

import json
import threading
import time as time_module
from collections import Counter
from concurrent.futures import ThreadPoolExecutor
from dataclasses import dataclass
from datetime import date, datetime, time, timedelta, timezone
from pathlib import Path
from typing import Any, Callable, Dict, List, Mapping, Optional
from zoneinfo import ZoneInfo

from apscheduler.schedulers.background import BackgroundScheduler  # type: ignore[import-untyped]
from apscheduler.triggers.cron import CronTrigger  # type: ignore[import-untyped]

from .capture import ProviderResult, ensure_content_slot
from .contracts import load_contract
from .duplicates import run_duplicate_fingerprint_queue
from .evaluation import evaluate_content
from .media import (
    MEDIA_QUEUE_BATCH_LIMIT,
    run_media_download_queue,
    run_media_processing_queue,
)
from .media_state import media_terminal_state_details
from .providers import STAGE_CONFIG, discover_account_content, update_content_data
from .reports import (
    REPORTS_ROOT,
    ReportTaskError,
    assert_report_runtime_ready,
    create_and_run_task,
)
from .storage import (
    BACKFILL_SOURCE_GROUPS,
    COMMENT_COLLECTION_VERSION,
    DEFAULT_DB,
    connect,
    now_utc,
    transaction,
)


SHANGHAI = ZoneInfo("Asia/Shanghai")
#: 2026-08-07 调整：$3/1200 条追不上报表 90% 指标新鲜度门槛（当天 1,014 条
#: 指标刷新被 budget_blocked 顺延，metrics_freshness 跌到 22%，日报恒为
#: 部分完成）。上调总预算并把指标刷新排到详情/评论之前执行——statistics
#: 单价极低（douyin $0.001/条），3000 条上限下指标段最多花 $3，剩余预算
#: 天然留给贵操作，便宜的新鲜度刷新不再被饿死。
DAILY_CAPTURE_MAX_AMOUNT = 8.0
DAILY_CAPTURE_CONTENT_LIMIT = 3000
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
TERMINAL_RUN_STATUSES = frozenset({"succeeded", "partial", "skipped"})
RETRYABLE_RUN_STATUSES = frozenset({"failed", "interrupted"})
ATTEMPT_TERMINAL_STATUSES = TERMINAL_RUN_STATUSES | RETRYABLE_RUN_STATUSES
INVOCATION_SOURCES = frozenset(
    {"scheduled", "startup_report_catchup", "operator_retry"}
)


class SchedulerJobError(RuntimeError):
    pass


@dataclass(frozen=True)
class RunClaim:
    scheduler_run_id: int
    attempt_id: int
    attempt_number: int


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


def _claim_run(
    job_id: str,
    scheduled_for: datetime,
    *,
    db_path: Path,
    allow_retry: bool,
    invocation_source: str,
) -> Optional[RunClaim]:
    key = _scheduled_iso(scheduled_for)
    started_at = now_utc()
    if invocation_source not in INVOCATION_SOURCES:
        raise SchedulerJobError(
            f"unsupported scheduler invocation source: {invocation_source}"
        )
    with connect(db_path) as connection, transaction(connection):
        existing = connection.execute(
            """
            SELECT id,status FROM scheduler_runs
            WHERE job_id=? AND scheduled_for=?
            """,
            (job_id, key),
        ).fetchone()
        if existing is None:
            cursor = connection.execute(
                """
                INSERT INTO scheduler_runs(
                    job_id, scheduled_for, status, started_at, details_json
                ) VALUES (?, ?, 'running', ?, '{}')
                """,
                (job_id, key, started_at),
            )
            if cursor.lastrowid is None:
                raise SchedulerJobError("scheduler occurrence insert returned no id")
            scheduler_run_id = int(cursor.lastrowid)
            attempt_number = 1
        else:
            scheduler_run_id = int(existing["id"])
            current_status = str(existing["status"])
            if current_status == "running" or current_status in TERMINAL_RUN_STATUSES:
                return None
            if current_status not in RETRYABLE_RUN_STATUSES:
                raise SchedulerJobError(
                    f"unsupported scheduler run status: {current_status}"
                )
            if not allow_retry:
                return None
            attempt_number = int(
                connection.execute(
                    """
                    SELECT COALESCE(MAX(attempt_number),0)+1
                    FROM scheduler_run_attempts WHERE scheduler_run_id=?
                    """,
                    (scheduler_run_id,),
                ).fetchone()[0]
            )
            updated = connection.execute(
                """
                UPDATE scheduler_runs
                SET status='running',started_at=?,completed_at=NULL,details_json='{}'
                WHERE id=? AND status IN ('failed','interrupted')
                """,
                (started_at, scheduler_run_id),
            )
            if updated.rowcount != 1:
                return None
        attempt = connection.execute(
            """
            INSERT INTO scheduler_run_attempts(
                scheduler_run_id,attempt_number,invocation_source,status,
                started_at,details_json
            ) VALUES (?, ?, ?, 'running', ?, '{}')
            """,
            (scheduler_run_id, attempt_number, invocation_source, started_at),
        )
        if attempt.lastrowid is None:
            raise SchedulerJobError("scheduler attempt insert returned no id")
        return RunClaim(
            scheduler_run_id=scheduler_run_id,
            attempt_id=int(attempt.lastrowid),
            attempt_number=attempt_number,
        )


def _finish_run(
    claim: RunClaim,
    *,
    status: str,
    details: Dict[str, Any],
    db_path: Path,
) -> None:
    if status not in ATTEMPT_TERMINAL_STATUSES:
        raise SchedulerJobError(f"unsupported scheduler terminal status: {status}")
    completed_at = now_utc()
    details_json = json.dumps(details, ensure_ascii=False, sort_keys=True)
    with connect(db_path) as connection, transaction(connection):
        attempt = connection.execute(
            """
            UPDATE scheduler_run_attempts
            SET status=?,completed_at=?,details_json=?
            WHERE id=? AND scheduler_run_id=? AND status='running'
            """,
            (
                status,
                completed_at,
                details_json,
                claim.attempt_id,
                claim.scheduler_run_id,
            ),
        )
        if attempt.rowcount != 1:
            raise SchedulerJobError(
                f"scheduler attempt is no longer active: {claim.attempt_id}"
            )
        run = connection.execute(
            """
            UPDATE scheduler_runs SET status=?, completed_at=?, details_json=?
            WHERE id=? AND status='running'
            """,
            (
                status,
                completed_at,
                details_json,
                claim.scheduler_run_id,
            ),
        )
        if run.rowcount != 1:
            raise SchedulerJobError(
                f"scheduler run is no longer active: {claim.scheduler_run_id}"
            )


def recover_interrupted_scheduler_runs(*, db_path: Path = DEFAULT_DB) -> int:
    """Fence attempts left running by a previous single-writer process."""

    completed_at = now_utc()
    details_json = json.dumps(
        {"reason": "writer_process_restarted"},
        ensure_ascii=False,
        sort_keys=True,
    )
    with connect(db_path) as connection, transaction(connection):
        attempts = connection.execute(
            """
            SELECT id,scheduler_run_id FROM scheduler_run_attempts
            WHERE status='running' ORDER BY id
            """
        ).fetchall()
        running_run_ids = {
            int(row["id"])
            for row in connection.execute(
                "SELECT id FROM scheduler_runs WHERE status='running'"
            )
        }
        running_attempt_run_ids = {
            int(attempt["scheduler_run_id"]) for attempt in attempts
        }
        if running_run_ids != running_attempt_run_ids:
            raise SchedulerJobError(
                "running scheduler occurrences and attempts are inconsistent"
            )
        for attempt in attempts:
            updated_attempt = connection.execute(
                """
                UPDATE scheduler_run_attempts
                SET status='interrupted',completed_at=?,details_json=?
                WHERE id=? AND status='running'
                """,
                (completed_at, details_json, int(attempt["id"])),
            )
            updated_run = connection.execute(
                """
                UPDATE scheduler_runs
                SET status='interrupted',completed_at=?,details_json=?
                WHERE id=? AND status='running'
                """,
                (completed_at, details_json, int(attempt["scheduler_run_id"])),
            )
            if updated_attempt.rowcount != 1 or updated_run.rowcount != 1:
                raise SchedulerJobError(
                    "scheduler interruption recovery lost its active attempt"
                )
        return len(attempts)


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
                connection.execute(
                    """
                    INSERT INTO comment_capture_runs(
                        content_id,window_key,collection_version,provider,
                        adapter_version,status,created_at,updated_at
                    ) VALUES (?,?,?,?,?,'pending',?,?)
                    ON CONFLICT(content_id,window_key) DO NOTHING
                    """,
                    (
                        int(row["id"]),
                        week_key,
                        COMMENT_COLLECTION_VERSION,
                        comments_provider,
                        f"{comments_adapter}+{COMMENT_COLLECTION_VERSION}",
                        now_utc(),
                        now_utc(),
                    ),
                )
                counts["comments"] += int(
                    connection.execute("SELECT changes()").fetchone()[0]
                )
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
                SELECT MAX(touched_at) FROM (
                    SELECT f.updated_at touched_at FROM fetch_slots f
                    WHERE f.content_id=c.id AND f.stage IN ('detail','metrics')
                    UNION ALL
                    SELECT r.updated_at touched_at FROM comment_capture_runs r
                    WHERE r.content_id=c.id
                )
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
            """,
            (day_key,),
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
        comment_runs = {
            int(row["content_id"]): dict(row)
            for row in connection.execute(
                """
                SELECT * FROM comment_capture_runs WHERE window_key=?
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
        metrics_needed = within_monitoring_window and needs_work(
            metrics_slot, storage_ready=content_id in valid_metric_contents
        )
        comment_run = comment_runs.get(content_id)
        comment_status = str(comment_run["status"]) if comment_run else ""
        stale_running = False
        if comment_run is not None and comment_status == "running":
            try:
                touched = datetime.fromisoformat(
                    str(comment_run["updated_at"]).replace("Z", "+00:00")
                ).astimezone(timezone.utc)
                stale_running = touched < scheduled_utc - timedelta(hours=2)
            except (TypeError, ValueError):
                stale_running = True
        comments_needed = within_monitoring_window and (
            comment_run is None
            or comment_status in {"pending", "retryable_failed"}
            or stale_running
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
                account_status = "partial"
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
            if not page.get("has_more"):
                stopped_reason = "provider_exhausted"
                break
            if next_cursor in (None, ""):
                stopped_reason = "missing_next_cursor"
                account_status = "partial"
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

    # ---- 指标优先段：先刷完全部到期指标（便宜、保新鲜度），再跑详情/评论 ----
    metrics_targets = [dict(c) for c in contents if c["metrics_needed"]]
    metrics_refreshed: set[int] = set()
    metrics_attempted: set[int] = set()
    metrics_retry_ids: set[int] = set()
    metrics_first_summary = {
        "attempted": len(metrics_targets),
        "succeeded": 0,
        "budget_blocked": 0,
        "failed": 0,
    }

    def refresh_metrics_only(content: Mapping[str, Any]) -> Dict[str, Any]:
        if provider_blocked.is_set():
            return {"content_id": content["id"], "status": "circuit_break_skipped"}
        try:
            result = update_content_data(
                int(content["id"]),
                as_of=local_day,
                db_path=db_path,
                call_override=call_override,
                stages=["metrics"],
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
            }
        blocked_code = next(
            (
                item.get("error_code")
                for item in result.get("stages", [])
                if item.get("error_code") in CAPTURE_CIRCUIT_BREAK_CODES
            ),
            None,
        )
        if blocked_code:
            provider_blocked.set()
        return result

    if metrics_targets:
        metrics_workers = min(DAILY_CAPTURE_WORKERS, max(1, len(metrics_targets)))
        with ThreadPoolExecutor(
            max_workers=metrics_workers, thread_name_prefix="dcar-metrics-first"
        ) as pool:
            metrics_first_results = list(
                pool.map(refresh_metrics_only, metrics_targets)
            )
        for content, result in zip(metrics_targets, metrics_first_results):
            content_id = int(content["id"])
            if result.get("status") == "circuit_break_skipped":
                continue
            metrics_attempted.add(content_id)
            stage_rows = result.get("stages") or []
            metric_stage = next(
                (row for row in stage_rows if row.get("stage") == "metrics"), None
            )
            if metric_stage is not None and metric_stage.get("status") == "succeeded":
                metrics_refreshed.add(content_id)
                metrics_first_summary["succeeded"] += 1
                continue
            error_code = str(
                (metric_stage or {}).get("error_code")
                or result.get("error_code")
                or ""
            )
            if error_code == "budget_blocked":
                metrics_first_summary["budget_blocked"] += 1
            else:
                metrics_first_summary["failed"] += 1
            retryable = bool((metric_stage or {}).get("retryable"))
            if (
                (retryable or error_code == "budget_blocked")
                and error_code not in CAPTURE_CIRCUIT_BREAK_CODES
            ):
                metrics_retry_ids.add(content_id)
    # ---- 指标优先段结束；未刷成功的指标在主循环里按剩余总预算继续尝试 ----

    def update_one_content(content: Mapping[str, Any]) -> Dict[str, Any]:
        if provider_blocked.is_set():
            return {"content_id": content["id"], "status": "circuit_break_skipped"}
        stages: List[str] = []
        if content["detail_needed"]:
            stages.append("detail")
        if content["metrics_needed"] and int(content["id"]) not in metrics_attempted:
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
        if int(content["id"]) in metrics_retry_ids and "metrics" not in retry_stages:
            retry_stages = sorted({*retry_stages, "metrics"})
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
            initial_stage_rows = initial.get("stages", []) or []
            initial_stage_names = {
                str(stage.get("stage")) for stage in initial_stage_rows
            }
            merged_stages = [
                retried_by_stage.get(str(stage.get("stage")), stage)
                for stage in initial_stage_rows
            ] + [
                row
                for name, row in retried_by_stage.items()
                if name not in initial_stage_names
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

    reported_provider_cost = sum(
        float(item.get("provider_cost") or 0) for item in discovery + content_updates
    )
    with connect(db_path) as connection:
        ledger_provider_cost = float(
            connection.execute(
                """
                SELECT COALESCE(SUM(amount), 0) FROM provider_usage
                WHERE task_id=? AND currency='USD'
                """,
                (task_id,),
            ).fetchone()[0]
            or 0
        )
    provider_cost = max(
        round(reported_provider_cost, 6), round(ledger_provider_cost, 6)
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
        "metrics_first": metrics_first_summary,
        "discovery": discovery,
        "content_updates": content_updates,
        "blocked_providers": sorted(blocked_providers),
        "failed_operations": failed,
        "provider_cost": provider_cost,
    }


def run_media_cutoff(
    scheduled_for: datetime,
    *,
    db_path: Path = DEFAULT_DB,
) -> Dict[str, Any]:
    """Evaluate completed media DAGs, then retire legacy media review rows."""

    local_day = scheduled_for.astimezone(SHANGHAI).date()
    start = datetime.combine(local_day, time.min, SHANGHAI).astimezone(timezone.utc)
    end = start + timedelta(days=1)
    start_iso = start.isoformat(timespec="seconds").replace("+00:00", "Z")
    end_iso = end.isoformat(timespec="seconds").replace("+00:00", "Z")
    with connect(db_path) as connection:
        coverage_rows = connection.execute(
            """
            SELECT c.id
            FROM content_items c
            WHERE c.platform IN ('douyin','xiaohongshu')
              AND c.content_type IN ('video','image')
              -- 全量历史回溯批量入库的内容不适用“当日媒体截止”闸门：
              -- 它们的媒体与评估由 range_backfill local-evidence 阶段按窗口推进，
              -- 标记清除后自然回归本闸门管辖。
              AND COALESCE(c.source_group,'') NOT IN (?,?)
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
            (*BACKFILL_SOURCE_GROUPS, start_iso, end_iso, start_iso, end_iso),
        ).fetchall()
        backlog_rows = connection.execute(
            """
            SELECT DISTINCT c.id
            FROM content_items c
            JOIN evidence_artifacts ea ON ea.content_id=c.id
            WHERE c.platform IN ('douyin','xiaohongshu')
              AND c.content_type IN ('video','image')
              AND COALESCE(c.source_group,'') NOT IN (?,?)
              AND ea.artifact_type='media_source'
              AND ea.status='available'
            ORDER BY c.id
            """,
            BACKFILL_SOURCE_GROUPS,
        ).fetchall()
        releases = connection.execute(
            "SELECT id FROM evaluation_releases WHERE status='active' ORDER BY id"
        ).fetchall()
        if len(releases) != 1:
            raise SchedulerJobError(
                "daily media cutoff requires exactly one active evaluation release"
            )
        release_id = str(releases[0]["id"])
        coverage_content_ids = [int(row["id"]) for row in coverage_rows]
        backlog_content_ids = [int(row["id"]) for row in backlog_rows]
        state_content_ids = list(
            dict.fromkeys([*coverage_content_ids, *backlog_content_ids])
        )
        initial_states = media_terminal_state_details(
            connection, release_id, state_content_ids
        )

    evaluation_results: List[Dict[str, Any]] = []
    evaluation_content_ids = [
        content_id
        for content_id in backlog_content_ids
        if initial_states[content_id].reason == "evaluation_pending"
    ]
    for content_id in evaluation_content_ids:
        evaluation = evaluate_content(
            content_id,
            db_path=db_path,
            expected_active_release_id=release_id,
        )
        evaluation_results.append(
            {
                "content_id": content_id,
                "evaluation_id": evaluation.evaluation_id,
                "evidence_level": evaluation.evidence_level,
                "created": evaluation.created,
            }
        )

    resolved_queue_rows = 0
    with connect(db_path) as connection, transaction(connection):
        active_releases = connection.execute(
            "SELECT id FROM evaluation_releases WHERE status='active' ORDER BY id"
        ).fetchall()
        if len(active_releases) != 1 or str(active_releases[0]["id"]) != release_id:
            raise SchedulerJobError(
                "active evaluation release changed during daily media cutoff"
            )
        final_state_content_ids = list(
            dict.fromkeys([*coverage_content_ids, *evaluation_content_ids])
        )
        final_states = media_terminal_state_details(
            connection, release_id, final_state_content_ids
        )
        if final_state_content_ids:
            placeholders = ",".join("?" for _ in final_state_content_ids)
            resolved_at = now_utc()
            updated = connection.execute(
                f"""
                UPDATE review_queue
                SET status='resolved',resolved_at=COALESCE(resolved_at,?),updated_at=?
                WHERE content_id IN ({placeholders})
                  AND reason_code IN (
                    'media_processing_incomplete','media_evidence_missing',
                    'stale_local_evidence','legacy_content_unavailable'
                  )
                  AND status<>'resolved'
                """,
                (resolved_at, resolved_at, *final_state_content_ids),
            )
            resolved_queue_rows = int(updated.rowcount)

    state_counts = Counter(
        final_states[content_id].state for content_id in coverage_content_ids
    )
    normalized_state_counts = {
        "complete": int(state_counts["complete"]),
        "terminal_insufficient": int(state_counts["terminal_insufficient"]),
        "terminal_failed": int(state_counts["terminal_failed"]),
        "pending": int(state_counts["pending"]),
    }
    terminal = (
        normalized_state_counts["complete"]
        + normalized_state_counts["terminal_insufficient"]
        + normalized_state_counts["terminal_failed"]
    )
    total = len(coverage_content_ids)
    coverage = round(terminal * 100 / total, 2) if total else 100.0
    threshold = float(
        load_contract()["required_coverage_thresholds"]["media_terminal_coverage"]
    )
    return {
        "window_start": start_iso,
        "window_end": end_iso,
        "candidates": total,
        "state_counts": normalized_state_counts,
        "pending": normalized_state_counts["pending"],
        "terminal": terminal,
        "terminal_coverage": coverage,
        "threshold": threshold,
        "threshold_status": (
            "available" if coverage >= threshold else "below_threshold"
        ),
        "evaluation": {
            "candidates": len(evaluation_results),
            "created": sum(int(item["created"]) for item in evaluation_results),
            "reused": sum(int(not item["created"]) for item in evaluation_results),
            "results": evaluation_results,
        },
        "resolved_media_queue_rows": resolved_queue_rows,
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
    if job_id == "daily_capture":
        details = run_due_capture(
            scheduled_for, db_path=db_path, call_override=capture_call_override
        )
        return str(details.pop("status")), details
    if job_id == "daily_media_download":
        fresh_content = run_media_download_queue(
            limit=MEDIA_QUEUE_BATCH_LIMIT,
            db_path=db_path,
        )
        status = (
            "failed"
            if int(fresh_content.get("retryable_failed", 0)) > 0
            or bool(fresh_content.get("truncated"))
            else "succeeded"
        )
        return status, {"fresh_content": fresh_content}
    if job_id == "daily_media_processing":
        media = run_media_processing_queue(
            limit=MEDIA_QUEUE_BATCH_LIMIT,
            db_path=db_path,
        )
        duplicates = run_duplicate_fingerprint_queue(limit=500, db_path=db_path)
        status = (
            "failed"
            if int(media.get("retryable_failed", 0)) > 0
            or bool(media.get("truncated"))
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
        task_status = str(task["task_status"])
        if task_status not in {"succeeded", "partial"}:
            raise SchedulerJobError(
                f"daily report returned non-terminal task status: {task_status}"
            )
        return task_status, {"task_id": task["id"], "task_status": task_status}
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
        task_status = str(task["task_status"])
        if task_status not in {"succeeded", "partial"}:
            raise SchedulerJobError(
                f"weekly report returned non-terminal task status: {task_status}"
            )
        return task_status, {"task_id": task["id"], "task_status": task_status}
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
    allow_retry: bool = False,
    invocation_source: str = "scheduled",
) -> Dict[str, Any]:
    if job_id not in {job.job_id for job in JOBS}:
        raise SchedulerJobError(f"unknown scheduler job: {job_id}")
    if invocation_source not in INVOCATION_SOURCES:
        raise SchedulerJobError(
            f"unsupported scheduler invocation source: {invocation_source}"
        )
    if invocation_source == "startup_report_catchup":
        if job_id not in REPORT_JOB_IDS:
            raise SchedulerJobError("startup_report_catchup is report-only")
        if not allow_retry:
            raise SchedulerJobError(
                "startup_report_catchup requires allow_retry=True"
            )
    if allow_retry and job_id not in REPORT_JOB_IDS and invocation_source != "operator_retry":
        raise SchedulerJobError(
            "non-report scheduler retries require operator_retry authorization"
        )
    if invocation_source == "operator_retry" and not allow_retry:
        raise SchedulerJobError("operator_retry requires allow_retry=True")
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
    claim = _claim_run(
        job_id,
        scheduled_for,
        db_path=db_path,
        allow_retry=allow_retry,
        invocation_source=invocation_source,
    )
    if claim is None:
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
            claim,
            status="failed",
            details={"error": str(exc)},
            db_path=db_path,
        )
        raise
    _finish_run(claim, status=status, details=details, db_path=db_path)
    return {
        "job_id": job_id,
        "status": status,
        "attempt_number": claim.attempt_number,
        "details": details,
    }


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


def _report_occurrence(
    job_id: str,
    *,
    period_start: date,
    period_end: date,
) -> datetime:
    if job_id == "daily_report":
        return datetime.combine(period_end + timedelta(days=1), time(8, 0), SHANGHAI)
    if job_id == "weekly_report":
        return datetime.combine(period_end + timedelta(days=1), time(8, 30), SHANGHAI)
    raise SchedulerJobError(f"not a report job: {job_id}")


def _report_catchup_occurrences(
    *, current: datetime, db_path: Path
) -> List[tuple[str, datetime]]:
    """Return every due failed/interrupted or content-backed missing report slot."""

    current = current.astimezone(SHANGHAI)
    candidates: set[tuple[str, datetime]] = set()
    with connect(db_path) as connection:
        for row in connection.execute(
            """
            SELECT job_id,scheduled_for FROM scheduler_runs
            WHERE job_id IN ('daily_report','weekly_report')
              AND status IN ('failed','interrupted')
            ORDER BY scheduled_for,job_id
            """
        ):
            occurrence = datetime.fromisoformat(
                str(row["scheduled_for"]).replace("Z", "+00:00")
            ).astimezone(SHANGHAI)
            if occurrence <= current:
                candidates.add((str(row["job_id"]), occurrence))

        automatic_tasks = connection.execute(
            """
            SELECT task_type,period_start,period_end FROM report_tasks
            WHERE creation_source='automatic' AND task_type IN ('daily','weekly')
            ORDER BY period_start,period_end
            """
        ).fetchall()
        daily_anchor: Optional[date] = None
        weekly_anchor: Optional[date] = None
        for task in automatic_tasks:
            task_type = str(task["task_type"])
            start = date.fromisoformat(str(task["period_start"]))
            end = date.fromisoformat(str(task["period_end"]))
            job_id = "daily_report" if task_type == "daily" else "weekly_report"
            occurrence = _report_occurrence(
                job_id, period_start=start, period_end=end
            )
            if occurrence <= current:
                candidates.add((job_id, occurrence))
            if task_type == "daily" and (daily_anchor is None or start < daily_anchor):
                daily_anchor = start
            if task_type == "weekly" and (
                weekly_anchor is None or start < weekly_anchor
            ):
                weekly_anchor = start

        latest_daily = latest_occurrence(
            next(job for job in JOBS if job.job_id == "daily_report"), current
        )
        if daily_anchor is None:
            daily_anchor = latest_daily.date() - timedelta(days=1)
        content_days = [
            date.fromisoformat(str(row["report_day"]))
            for row in connection.execute(
                """
                SELECT DISTINCT date(datetime(published_at), '+8 hours') report_day
                FROM content_items
                WHERE published_at IS NOT NULL
                  AND date(datetime(published_at), '+8 hours')>=?
                ORDER BY report_day
                """,
                (daily_anchor.isoformat(),),
            )
            if row["report_day"] is not None
        ]
        existing = {
            (str(row["job_id"]), str(row["scheduled_for"]))
            for row in connection.execute(
                """
                SELECT job_id,scheduled_for FROM scheduler_runs
                WHERE job_id IN ('daily_report','weekly_report')
                """
            )
        }
        for report_day in content_days:
            occurrence = _report_occurrence(
                "daily_report", period_start=report_day, period_end=report_day
            )
            if (
                occurrence <= current
                and ("daily_report", _scheduled_iso(occurrence)) not in existing
            ):
                candidates.add(("daily_report", occurrence))

        if weekly_anchor is None:
            weekly_anchor = daily_anchor + timedelta(
                days=(-daily_anchor.weekday()) % 7
            )
        if weekly_anchor is not None:
            weeks = {
                report_day - timedelta(days=report_day.weekday())
                for report_day in content_days
                if report_day >= weekly_anchor
            }
            for week_start in weeks:
                week_end = week_start + timedelta(days=6)
                occurrence = _report_occurrence(
                    "weekly_report", period_start=week_start, period_end=week_end
                )
                if (
                    occurrence <= current
                    and ("weekly_report", _scheduled_iso(occurrence)) not in existing
                ):
                    candidates.add(("weekly_report", occurrence))

    terminal: set[tuple[str, str]] = set()
    with connect(db_path) as connection:
        terminal = {
            (str(row["job_id"]), str(row["scheduled_for"]))
            for row in connection.execute(
                """
                SELECT job_id,scheduled_for FROM scheduler_runs
                WHERE job_id IN ('daily_report','weekly_report')
                  AND status IN ('running','succeeded','partial','skipped')
                """
            )
        }
    return sorted(
        (
            (job_id, occurrence)
            for job_id, occurrence in candidates
            if (job_id, _scheduled_iso(occurrence)) not in terminal
        ),
        key=lambda item: (item[1], item[0]),
    )


def startup_catchup(
    *,
    now: Optional[datetime] = None,
    db_path: Path = DEFAULT_DB,
    reports_root: Path = REPORTS_ROOT,
) -> List[Dict[str, Any]]:
    """Catch up report-only occurrences; this path never runs provider/media jobs."""

    current = (now or datetime.now(SHANGHAI)).astimezone(SHANGHAI)
    results: List[Dict[str, Any]] = []
    for job_id, occurrence in _report_catchup_occurrences(
        current=current, db_path=db_path
    ):
        try:
            result = execute_job(
                job_id,
                occurrence,
                db_path=db_path,
                reports_root=reports_root,
                allow_retry=True,
                invocation_source="startup_report_catchup",
            )
            result["scheduled_for"] = _scheduled_iso(occurrence)
            results.append(result)
        except Exception as exc:
            results.append(
                {
                    "job_id": job_id,
                    "scheduled_for": _scheduled_iso(occurrence),
                    "status": "failed",
                    "error": str(exc),
                }
            )
    return results
