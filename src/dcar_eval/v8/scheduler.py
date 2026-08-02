"""In-process APScheduler jobs with database idempotency and bounded catch-up."""

from __future__ import annotations

import json
from dataclasses import dataclass
from datetime import datetime, time, timedelta, timezone
from pathlib import Path
from typing import Any, Callable, Dict, List, Mapping, Optional
from zoneinfo import ZoneInfo

from apscheduler.schedulers.background import BackgroundScheduler  # type: ignore[import-untyped]
from apscheduler.triggers.cron import CronTrigger  # type: ignore[import-untyped]

from .backfill import run_daily_backfill_batch
from .capture import ProviderResult, ensure_content_slot
from .evaluation import evaluate_incremental
from .media import run_media_download_queue, run_media_processing_queue
from .providers import STAGE_CONFIG, discover_account_content, update_content_data
from .reports import REPORTS_ROOT, create_and_run_task
from .storage import DEFAULT_DB, connect, now_utc, transaction


SHANGHAI = ZoneInfo("Asia/Shanghai")
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


class SchedulerJobError(RuntimeError):
    pass


def _scheduled_iso(value: datetime) -> str:
    if value.tzinfo is None:
        value = value.replace(tzinfo=SHANGHAI)
    return value.astimezone(timezone.utc).isoformat(timespec="seconds").replace("+00:00", "Z")


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
        row = connection.execute(
            "SELECT * FROM scheduler_runs WHERE job_id=? AND scheduled_for=?",
            (job_id, key),
        ).fetchone()
        if row is not None and row["status"] in {"succeeded", "skipped"}:
            return False
        if row is None:
            connection.execute(
                """
                INSERT INTO scheduler_runs(
                    job_id, scheduled_for, status, started_at, details_json
                ) VALUES (?, ?, 'running', ?, '{}')
                """,
                (job_id, key, started_at),
            )
        else:
            connection.execute(
                """
                UPDATE scheduler_runs SET status='running', started_at=?, completed_at=NULL,
                    details_json='{}' WHERE id=?
                """,
                (started_at, row["id"]),
            )
    return True


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
            WHERE job_id=? AND scheduled_for=?
            """,
            (
                status, now_utc(), json.dumps(details, ensure_ascii=False, sort_keys=True),
                job_id, _scheduled_iso(scheduled_for),
            ),
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
            ORDER BY c.id
            """
        ).fetchall()
        counts = {"detail": 0, "metrics": 0, "comments": 0}
        for row in rows:
            platform = str(row["platform"])
            detail_provider, detail_adapter, _, _ = STAGE_CONFIG[(platform, "detail")]
            metrics_provider, metrics_adapter, _, _ = STAGE_CONFIG[(platform, "metrics")]
            comments_provider, comments_adapter, _, _ = STAGE_CONFIG[(platform, "comments")]
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
                    connection, content_id=int(row["id"]), stage="detail",
                    window_key="lifetime", provider=detail_provider, adapter_version=detail_adapter,
                )
                counts["detail"] += 1
            try:
                published = datetime.fromisoformat(str(row["published_at"]).replace("Z", "+00:00"))
            except ValueError:
                continue
            if published < window_start:
                continue
            ensure_content_slot(
                connection, content_id=int(row["id"]), stage="metrics",
                window_key=day_key, provider=metrics_provider, adapter_version=metrics_adapter,
            )
            counts["metrics"] += 1
            if local_day.weekday() == 0:
                ensure_content_slot(
                    connection, content_id=int(row["id"]), stage="comments",
                    window_key=week_key, provider=comments_provider, adapter_version=comments_adapter,
                )
                counts["comments"] += 1
    return {"monitored_contents": len(rows), "prepared_slots": counts}


def run_due_capture(
    scheduled_for: datetime,
    *,
    db_path: Path = DEFAULT_DB,
    call_override: Optional[Callable[[str, Mapping[str, Any]], ProviderResult]] = None,
) -> Dict[str, Any]:
    local_day = scheduled_for.astimezone(SHANGHAI).date()
    window_start = scheduled_for.astimezone(timezone.utc) - timedelta(days=30)
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
            "status": "skipped", "reason": "没有已启用的抖音或小红书账号",
            "monitored_accounts": 0, "discovery": [], "content_updates": [],
            "provider_cost": 0.0,
        }

    discovery: List[Dict[str, Any]] = []
    blocked_providers: set[str] = set()
    for identity in identities:
        provider = "TikHub" if identity["platform"] == "douyin" else "Rnote"
        if provider in blocked_providers:
            discovery.append(
                {
                    "account_id": identity["account_id"], "platform": identity["platform"],
                    "status": "circuit_break_skipped",
                }
            )
            continue
        try:
            result = discover_account_content(
                int(identity["account_id"]), str(identity["platform"]), str(identity["uid"]),
                as_of=local_day, db_path=db_path, call_override=call_override,
            )
        except Exception as exc:
            error_code = getattr(exc, "error_code", type(exc).__name__)
            result = {
                "account_id": identity["account_id"], "platform": identity["platform"],
                "status": "failed", "error_code": error_code, "message": str(exc),
                "provider_cost": 0.0,
            }
            if error_code in {"provider_balance_blocked", "provider_auth_blocked", "budget_blocked"}:
                blocked_providers.add(provider)
        discovery.append(result)

    with connect(db_path) as connection:
        contents = connection.execute(
            """
            SELECT c.*,
              EXISTS(
                SELECT 1 FROM fetch_slots f WHERE f.content_id=c.id
                  AND f.stage='detail' AND f.window_key='lifetime' AND f.status='succeeded'
              ) detail_succeeded,
              EXISTS(
                SELECT 1 FROM fetch_slots f WHERE f.content_id=c.id
                  AND f.stage='detail' AND f.window_key='lifetime' AND f.status='terminal_failed'
              ) detail_terminal
            FROM content_items c JOIN accounts a ON a.id=c.account_id
            WHERE a.enabled=1 AND c.platform IN ('douyin','xiaohongshu')
            ORDER BY c.id
            """
        ).fetchall()

    content_updates: List[Dict[str, Any]] = []
    for content in contents:
        provider = "TikHub" if content["platform"] == "douyin" else "Rnote"
        if provider in blocked_providers:
            content_updates.append(
                {"content_id": content["id"], "status": "circuit_break_skipped"}
            )
            continue
        stages: List[str] = []
        if not content["detail_succeeded"] and not content["detail_terminal"]:
            stages.append("detail")
        try:
            published = datetime.fromisoformat(str(content["published_at"]).replace("Z", "+00:00"))
        except (TypeError, ValueError):
            published = None
        if published is not None and published >= window_start:
            stages.extend(["metrics", "comments"])
        if not stages:
            continue
        result = update_content_data(
            int(content["id"]), as_of=local_day, db_path=db_path,
            call_override=call_override, stages=stages, process_media=False,
        )
        content_updates.append(result)
        blocked = next(
            (
                item.get("error_code") for item in result["stages"]
                if item.get("error_code") in {
                    "provider_balance_blocked", "provider_auth_blocked", "budget_blocked"
                }
            ),
            None,
        )
        if blocked:
            blocked_providers.add(provider)

    provider_cost = sum(float(item.get("provider_cost") or 0) for item in discovery + content_updates)
    failed = sum(item.get("status") in {"failed", "partial"} for item in discovery + content_updates)
    return {
        "status": "failed" if failed else "succeeded",
        "monitored_accounts": len(identities), "monitored_contents": len(contents),
        "discovery": discovery, "content_updates": content_updates,
        "blocked_providers": sorted(blocked_providers), "failed_operations": failed,
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
            evaluation = connection.execute(
                """
                SELECT id FROM evaluation_versions WHERE content_id=?
                  AND invalidated_at IS NULL
                ORDER BY evaluated_at DESC, id DESC LIMIT 1
                """,
                (content_id,),
            ).fetchone()
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
                    evaluation["id"] if evaluation is not None else None,
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
    local_date = scheduled_for.astimezone(SHANGHAI).date()
    if job_id == "daily_capture":
        details = run_due_capture(
            scheduled_for, db_path=db_path, call_override=capture_call_override
        )
        return str(details.pop("status")), details
    if job_id == "daily_media_download":
        return "succeeded", {
            "backfill": run_daily_backfill_batch(limit=20, db_path=db_path),
            "fresh_content": run_media_download_queue(limit=100, db_path=db_path),
        }
    if job_id == "daily_media_processing":
        return "succeeded", run_media_processing_queue(limit=100, db_path=db_path)
    if job_id == "daily_media_cutoff":
        return "succeeded", run_media_cutoff(scheduled_for, db_path=db_path)
    if job_id == "daily_report":
        target = local_date - timedelta(days=1)
        task = create_and_run_task(
            task_type="daily", period_start=target.isoformat(), period_end=target.isoformat(),
            creation_source="automatic", db_path=db_path, reports_root=reports_root,
        )
        return "succeeded", {"task_id": task["id"], "task_status": task["task_status"]}
    if job_id == "weekly_report":
        end = local_date - timedelta(days=1)
        start = end - timedelta(days=6)
        task = create_and_run_task(
            task_type="weekly", period_start=start.isoformat(), period_end=end.isoformat(),
            creation_source="automatic", db_path=db_path, reports_root=reports_root,
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
    if not _claim_run(job_id, scheduled_for, db_path=db_path):
        return {"job_id": job_id, "status": "skipped_duplicate"}
    try:
        status, details = _run_job_action(
            job_id, scheduled_for, db_path=db_path, reports_root=reports_root,
            capture_call_override=capture_call_override,
        )
    except Exception as exc:
        _finish_run(
            job_id, scheduled_for, status="failed",
            details={"error": str(exc)}, db_path=db_path,
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
                hour=job.hour, minute=job.minute, day_of_week=job.day_of_week,
                timezone=SHANGHAI,
            ),
            id=job.job_id,
            replace_existing=True,
            kwargs={"job_id": job.job_id, "db_path": db_path, "reports_root": reports_root},
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
                        job.job_id, occurrence, db_path=db_path, reports_root=reports_root
                    )
                )
            except Exception as exc:
                results.append({"job_id": job.job_id, "status": "failed", "error": str(exc)})
    return results
