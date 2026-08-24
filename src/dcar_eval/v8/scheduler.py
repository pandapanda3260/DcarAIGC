"""In-process APScheduler jobs with database idempotency and bounded catch-up."""

from __future__ import annotations

import json
import math
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
from apscheduler.triggers.interval import (  # type: ignore[import-untyped]
    IntervalTrigger,
)

from .capture import ProviderResult, ensure_content_slot
from .contracts import load_contract
from .duplicates import (
    FINGERPRINT_VERSION,
    RELATION_METHOD,
    run_duplicate_fingerprint_queue,
)
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
    _automatic_capture_observation_start_date,
    assert_report_runtime_ready,
    create_and_run_task,
    create_task,
    retry_task,
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
#: Daily provider spend is a task-wide ceiling across every operation.  The
#: amount is reserved under BEGIN IMMEDIATE in capture.py, so concurrent
#: workers cannot split the allowance by provider operation.
DAILY_CAPTURE_MAX_AMOUNT = 20.0
DAILY_CAPTURE_CONTENT_LIMIT = 3000
DAILY_DISCOVERY_MAX_PAGES = 20
DAILY_CAPTURE_WORKERS = 4
DAILY_CAPTURE_MAX_ATTEMPTS = 2
DAILY_CAPTURE_RETRY_DELAY_SECONDS = 1.0
DAILY_CAPTURE_DISCOVERY_QUALITY_PERCENT = 90
DAILY_CAPTURE_CONTENT_QUALITY_PERCENT = 60
CAPTURE_PROVIDER_FATAL_CODES = frozenset(
    {"provider_balance_blocked", "provider_auth_blocked"}
)
CAPTURE_BUDGET_FATAL_CODES = frozenset(
    {"budget_blocked", "budget_daily_quota_exhausted"}
)
CAPTURE_ALLOWED_STOP_CODES = frozenset({"task_budget_exhausted"})
CAPTURE_CIRCUIT_BREAK_CODES = frozenset(
    CAPTURE_PROVIDER_FATAL_CODES
    | CAPTURE_BUDGET_FATAL_CODES
    | CAPTURE_ALLOWED_STOP_CODES
)


class _CaptureCircuit:
    """Thread-safe stop signal retaining the structured trigger evidence."""

    def __init__(self) -> None:
        self._event = threading.Event()
        self._lock = threading.Lock()
        self._counts: Counter[str] = Counter()

    def is_set(self) -> bool:
        return self._event.is_set()

    def record(self, error_code: Any) -> bool:
        code = str(error_code or "")
        if code not in CAPTURE_CIRCUIT_BREAK_CODES:
            return False
        with self._lock:
            self._counts[code] += 1
            self._event.set()
        return True

    def record_result(self, result: Mapping[str, Any]) -> None:
        stages = list(result.get("stages") or [])
        if stages:
            for stage in stages:
                if isinstance(stage, Mapping):
                    self.record(stage.get("error_code"))
            return
        self.record(result.get("error_code"))

    def snapshot(self) -> Dict[str, int]:
        with self._lock:
            return {
                code: int(self._counts.get(code, 0))
                for code in sorted(CAPTURE_CIRCUIT_BREAK_CODES)
            }


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
DOUYIN_OPENAPI_RECONCILE_JOB_ID = "douyin_openapi_reconcile"
DOUYIN_OPENAPI_RECONCILE_GUARD_JOB_ID = "douyin_openapi_reconcile_guard"
DOUYIN_OPENAPI_RECONCILE_HOUR = 2
DAILY_CAPTURE_START_DELAY_MINUTES = 10
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
    retrying_terminal: bool = False
    retry_terminal_if_completed_before: Optional[datetime] = None
    terminal_retry_started_at: Optional[datetime] = None


@dataclass(frozen=True)
class CurrentDayRunState:
    status: str
    started_at: Optional[datetime]
    completed_at: Optional[datetime]


DouyinOpenApiRunner = Callable[..., Mapping[str, Any]]


_STALE_DEPENDENCY_RETRY_REASON = "stale_dependency_completion"


def _stale_retry_details(
    completed_before: datetime, *, retry_started_at: datetime
) -> Dict[str, str]:
    return {
        "retry_reason": _STALE_DEPENDENCY_RETRY_REASON,
        "dependency_completed_at": _scheduled_iso(completed_before),
        "retry_started_at": _scheduled_iso(retry_started_at),
    }


def _stale_retry_completed_before(value: Any) -> Optional[datetime]:
    if not isinstance(value, Mapping):
        return None
    if value.get("retry_reason") != _STALE_DEPENDENCY_RETRY_REASON:
        return None
    completed_value = value.get("dependency_completed_at")
    if not isinstance(completed_value, str) or not completed_value:
        raise SchedulerJobError("stale dependency retry marker is incomplete")
    try:
        completed_at = datetime.fromisoformat(completed_value.replace("Z", "+00:00"))
    except ValueError as exc:
        raise SchedulerJobError("stale dependency retry marker is invalid") from exc
    if completed_at.tzinfo is None:
        raise SchedulerJobError("stale dependency retry marker lacks timezone")
    return completed_at.astimezone(timezone.utc)


def _stale_retry_started_at(value: Any) -> Optional[datetime]:
    if not isinstance(value, Mapping):
        return None
    if value.get("retry_reason") != _STALE_DEPENDENCY_RETRY_REASON:
        return None
    started_value = value.get("retry_started_at")
    if not isinstance(started_value, str) or not started_value:
        raise SchedulerJobError("stale dependency retry start marker is incomplete")
    try:
        started_at = datetime.fromisoformat(started_value.replace("Z", "+00:00"))
    except ValueError as exc:
        raise SchedulerJobError("stale dependency retry start marker is invalid") from exc
    if started_at.tzinfo is None:
        raise SchedulerJobError("stale dependency retry start marker lacks timezone")
    return started_at.astimezone(timezone.utc)


def _require_report_job_runtime_ready(*, db_path: Path) -> Dict[str, Any]:
    with connect(db_path) as connection:
        return assert_report_runtime_ready(connection)


def _weekly_daily_dependency(
    scheduled_for: datetime, *, db_path: Path
) -> Dict[str, Any]:
    report_day = scheduled_for.astimezone(SHANGHAI).date() - timedelta(days=1)
    daily_occurrence = datetime.combine(
        report_day + timedelta(days=1), time(8, 0), SHANGHAI
    )
    with connect(db_path) as connection:
        run = connection.execute(
            """
            SELECT status,completed_at FROM scheduler_runs
            WHERE job_id='daily_report' AND scheduled_for=?
            """,
            (_scheduled_iso(daily_occurrence),),
        ).fetchone()
        task = connection.execute(
            """
            SELECT task_status,completed_at FROM report_tasks
            WHERE task_type='daily' AND period_start=? AND period_end=?
              AND creation_source='automatic'
            """,
            (report_day.isoformat(), report_day.isoformat()),
        ).fetchone()
    ready = bool(
        run is not None
        and str(run["status"]) in {"succeeded", "partial"}
        and run["completed_at"]
        and task is not None
        and str(task["task_status"]) in {"succeeded", "partial"}
        and task["completed_at"]
    )
    return {
        "ready": ready,
        "report_day": report_day.isoformat(),
        "daily_scheduled_for": _scheduled_iso(daily_occurrence),
        "daily_run_status": None if run is None else str(run["status"]),
        "daily_task_status": None if task is None else str(task["task_status"]),
    }


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


def current_day_daily_capture_guard(
    *,
    now: datetime,
    effective_from: date,
    db_path: Path,
    reports_root: Path,
    capture_call_override: Optional[
        Callable[[str, Mapping[str, Any]], ProviderResult]
    ],
) -> Dict[str, Any]:
    """Run today's logical 02:00 capture after the OpenAPI completion buffer.

    The OpenAPI reconciliation owns the same 02:00 coverage boundary and starts
    first.  The daily capture remains logically scheduled for 02:00 so a
    successful receipt covers its complete discovery interval; if OpenAPI is
    still running or failed after this short buffer, normal TikHub fallback is
    used.  This deliberately does not use :func:`latest_occurrence`, which
    would point at yesterday before today's slot.
    """

    local_now = now.astimezone(SHANGHAI)
    if local_now.date() < effective_from:
        return {
            "job_id": "daily_capture",
            "status": "before_effective_date",
            "effective_from": effective_from.isoformat(),
        }
    occurrence = datetime.combine(local_now.date(), time(2, 0), SHANGHAI)
    not_before = occurrence + timedelta(minutes=DAILY_CAPTURE_START_DELAY_MINUTES)
    if local_now < not_before:
        return {
            "job_id": "daily_capture",
            "status": "before_today_slot",
            "scheduled_for": _scheduled_iso(occurrence),
        }
    occurrence_key = _scheduled_iso(occurrence)
    with connect(db_path) as connection:
        existing = connection.execute(
            """
            SELECT status FROM scheduler_runs
            WHERE job_id='daily_capture' AND scheduled_for=?
            """,
            (occurrence_key,),
        ).fetchone()
    if existing is not None:
        return {
            "job_id": "daily_capture",
            "status": "already_attempted",
            "scheduled_for": occurrence_key,
            "existing_status": str(existing["status"]),
        }
    return execute_job(
        "daily_capture",
        occurrence,
        db_path=db_path,
        reports_root=reports_root,
        capture_call_override=capture_call_override,
        allow_retry=False,
        invocation_source="scheduled",
    )


def _current_day_run_state(
    job_id: str, occurrence: datetime, *, db_path: Path
) -> Optional[CurrentDayRunState]:
    with connect(db_path) as connection:
        row = connection.execute(
            """
            SELECT status,started_at,completed_at FROM scheduler_runs
            WHERE job_id=? AND scheduled_for=?
            """,
            (job_id, _scheduled_iso(occurrence)),
        ).fetchone()
    if row is None:
        return None
    timestamps: Dict[str, Optional[datetime]] = {}
    for field in ("started_at", "completed_at"):
        timestamps[field] = None
        if row[field] is None:
            continue
        try:
            parsed = datetime.fromisoformat(
                str(row[field]).replace("Z", "+00:00")
            )
        except ValueError as exc:
            raise SchedulerJobError(
                f"invalid {job_id} {field}: {row[field]}"
            ) from exc
        if parsed.tzinfo is None:
            raise SchedulerJobError(f"{job_id} {field} must include timezone")
        timestamps[field] = parsed.astimezone(timezone.utc)
    return CurrentDayRunState(
        status=str(row["status"]),
        started_at=timestamps["started_at"],
        completed_at=timestamps["completed_at"],
    )


def current_day_pipeline_guard(
    *,
    now: datetime,
    effective_from: date,
    db_path: Path,
    reports_root: Path,
    capture_call_override: Optional[
        Callable[[str, Mapping[str, Any]], ProviderResult]
    ],
) -> Dict[str, Any]:
    """Reconcile today's due pipeline in dependency order without backfill.

    Paid capture retries automatically only after a process interruption. A
    terminal capture is never re-run. Unpaid downstream failures retry on the
    next hourly pass, so sleep cannot silently omit the rest of today's chain.
    """

    local_now = now.astimezone(SHANGHAI)
    if local_now.date() < effective_from:
        return {
            "status": "before_effective_date",
            "effective_from": effective_from.isoformat(),
            "results": [],
        }

    capture_occurrence = datetime.combine(local_now.date(), time(2, 0), SHANGHAI)
    if local_now < capture_occurrence:
        return {"status": "before_today_slot", "results": []}

    results: List[Dict[str, Any]] = []
    capture_state = _current_day_run_state(
        "daily_capture", capture_occurrence, db_path=db_path
    )
    capture_status = None if capture_state is None else capture_state.status
    try:
        if capture_status == "interrupted":
            capture_result = execute_job(
                "daily_capture",
                capture_occurrence,
                db_path=db_path,
                reports_root=reports_root,
                capture_call_override=capture_call_override,
                allow_retry=True,
                invocation_source="scheduled",
            )
        else:
            capture_result = current_day_daily_capture_guard(
                now=local_now,
                effective_from=effective_from,
                db_path=db_path,
                reports_root=reports_root,
                capture_call_override=capture_call_override,
            )
        results.append(capture_result)
    except Exception as exc:
        return {
            "status": "blocked",
            "blocked_on": "daily_capture",
            "error": str(exc),
            "results": results,
        }

    capture_state = _current_day_run_state(
        "daily_capture", capture_occurrence, db_path=db_path
    )
    capture_status = None if capture_state is None else capture_state.status
    if capture_status in {None, "running"}:
        return {
            "status": "waiting",
            "blocked_on": "daily_capture",
            "results": results,
        }
    if capture_state is None or capture_state.completed_at is None:
        return {
            "status": "blocked",
            "blocked_on": "daily_capture",
            "error": "daily_capture terminal run is missing completed_at",
            "results": results,
        }

    previous_job_id = "daily_capture"
    previous_state = capture_state
    for definition in JOBS[1:]:
        if definition.day_of_week == "mon" and local_now.weekday() != 0:
            continue
        occurrence = datetime.combine(
            local_now.date(), time(definition.hour, definition.minute), SHANGHAI
        )
        if local_now < occurrence:
            break
        if previous_state.status not in (
            TERMINAL_RUN_STATUSES | RETRYABLE_RUN_STATUSES
        ):
            return {
                "status": "waiting",
                "blocked_on": previous_job_id,
                "results": results,
            }
        if previous_state.completed_at is None:
            return {
                "status": "blocked",
                "blocked_on": previous_job_id,
                "error": f"{previous_job_id} terminal run is missing completed_at",
                "results": results,
            }
        existing_state = _current_day_run_state(
            definition.job_id, occurrence, db_path=db_path
        )
        existing_status = None if existing_state is None else existing_state.status
        terminal_is_stale = (
            existing_state is not None
            and existing_state.status in TERMINAL_RUN_STATUSES
            and existing_state.completed_at is not None
            and (
                existing_state.started_at is None
                or existing_state.started_at < previous_state.completed_at
                or existing_state.completed_at < previous_state.completed_at
            )
        )
        if existing_status in TERMINAL_RUN_STATUSES and not terminal_is_stale:
            result = {
                "job_id": definition.job_id,
                "status": "already_attempted",
                "scheduled_for": _scheduled_iso(occurrence),
                "existing_status": existing_status,
            }
        elif existing_status == "running":
            return {
                "status": "waiting",
                "blocked_on": definition.job_id,
                "results": results,
            }
        else:
            invocation_source = (
                "startup_report_catchup"
                if definition.job_id in REPORT_JOB_IDS
                else "scheduled"
            )
            try:
                result = execute_job(
                    definition.job_id,
                    occurrence,
                    db_path=db_path,
                    reports_root=reports_root,
                    capture_call_override=capture_call_override,
                    allow_retry=(
                        definition.job_id in REPORT_JOB_IDS
                        or existing_status in RETRYABLE_RUN_STATUSES
                        or terminal_is_stale
                    ),
                    invocation_source=invocation_source,
                    retry_terminal_if_completed_before=(
                        previous_state.completed_at
                        if terminal_is_stale or existing_status == "interrupted"
                        else None
                    ),
                )
            except Exception as exc:
                return {
                    "status": "blocked",
                    "blocked_on": definition.job_id,
                    "error": str(exc),
                    "results": results,
                }
        results.append(result)
        previous_job_id = definition.job_id
        refreshed_state = _current_day_run_state(
            definition.job_id, occurrence, db_path=db_path
        )
        if refreshed_state is None or refreshed_state.status not in TERMINAL_RUN_STATUSES:
            return {
                "status": "waiting",
                "blocked_on": definition.job_id,
                "results": results,
            }
        previous_state = refreshed_state

    return {"status": "reconciled", "results": results}


def _claim_run(
    job_id: str,
    scheduled_for: datetime,
    *,
    db_path: Path,
    allow_retry: bool,
    invocation_source: str,
    retry_terminal_if_completed_before: Optional[datetime] = None,
) -> Optional[RunClaim]:
    key = _scheduled_iso(scheduled_for)
    started_at = now_utc()
    claim_started_at = datetime.fromisoformat(started_at.replace("Z", "+00:00"))
    retrying_terminal = False
    terminal_retry_started_at: Optional[datetime] = None
    if invocation_source not in INVOCATION_SOURCES:
        raise SchedulerJobError(
            f"unsupported scheduler invocation source: {invocation_source}"
        )
    with connect(db_path) as connection, transaction(connection):
        existing = connection.execute(
            """
            SELECT id,status,started_at,completed_at,details_json FROM scheduler_runs
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
            if current_status == "running":
                return None
            try:
                existing_details = json.loads(str(existing["details_json"] or "{}"))
            except json.JSONDecodeError as exc:
                raise SchedulerJobError(
                    f"invalid scheduler retry details for {job_id}"
                ) from exc
            persisted_retry_before = _stale_retry_completed_before(existing_details)
            persisted_retry_started_at = _stale_retry_started_at(existing_details)
            if retry_terminal_if_completed_before is None:
                retry_terminal_if_completed_before = persisted_retry_before
            terminal_retry_started_at = (
                persisted_retry_started_at or claim_started_at
            )
            retrying_terminal = current_status in TERMINAL_RUN_STATUSES
            if retrying_terminal:
                if retry_terminal_if_completed_before is None:
                    return None
                if job_id == "daily_capture":
                    raise SchedulerJobError(
                        "terminal daily_capture runs cannot be retried automatically"
                    )
                if not allow_retry:
                    raise SchedulerJobError(
                        "terminal downstream retry requires allow_retry=True"
                    )
                completed_value = existing["completed_at"]
                if completed_value is None:
                    raise SchedulerJobError(
                        f"terminal {job_id} run is missing completed_at"
                    )
                try:
                    completed_at = datetime.fromisoformat(
                        str(completed_value).replace("Z", "+00:00")
                    )
                except ValueError as exc:
                    raise SchedulerJobError(
                        f"invalid terminal {job_id} completed_at: {completed_value}"
                    ) from exc
                if completed_at.tzinfo is None:
                    raise SchedulerJobError(
                        f"terminal {job_id} completed_at must include timezone"
                    )
                started_value = existing["started_at"]
                started_at_value: Optional[datetime] = None
                if started_value is not None:
                    try:
                        started_at_value = datetime.fromisoformat(
                            str(started_value).replace("Z", "+00:00")
                        )
                    except ValueError as exc:
                        raise SchedulerJobError(
                            f"invalid terminal {job_id} started_at: {started_value}"
                        ) from exc
                    if started_at_value.tzinfo is None:
                        raise SchedulerJobError(
                            f"terminal {job_id} started_at must include timezone"
                        )
                dependency_completed = retry_terminal_if_completed_before.astimezone(
                    timezone.utc
                )
                if (
                    started_at_value is not None
                    and started_at_value.astimezone(timezone.utc)
                    >= dependency_completed
                    and completed_at.astimezone(timezone.utc) >= dependency_completed
                ):
                    return None
            elif (
                current_status in RETRYABLE_RUN_STATUSES
                and persisted_retry_before is not None
            ):
                retrying_terminal = True
            if current_status not in RETRYABLE_RUN_STATUSES:
                if not retrying_terminal:
                    raise SchedulerJobError(
                        f"unsupported scheduler run status: {current_status}"
                    )
            if not allow_retry:
                return None
            if (
                invocation_source == "scheduled"
                and job_id == "daily_capture"
                and current_status != "interrupted"
            ):
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
            if retrying_terminal:
                assert retry_terminal_if_completed_before is not None
                assert terminal_retry_started_at is not None
                retry_details_json = json.dumps(
                    _stale_retry_details(
                        retry_terminal_if_completed_before,
                        retry_started_at=terminal_retry_started_at,
                    ),
                    ensure_ascii=False,
                    sort_keys=True,
                )
                updated = connection.execute(
                    """
                    UPDATE scheduler_runs
                    SET status='running',started_at=?,completed_at=NULL,details_json=?
                    WHERE id=? AND status=? AND completed_at=?
                    """,
                    (
                        started_at,
                        retry_details_json,
                        scheduler_run_id,
                        current_status,
                        str(existing["completed_at"]),
                    ),
                )
            else:
                retry_details_json = "{}"
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
            ) VALUES (?, ?, ?, 'running', ?, ?)
            """,
            (
                scheduler_run_id,
                attempt_number,
                invocation_source,
                started_at,
                retry_details_json if existing is not None else "{}",
            ),
        )
        if attempt.lastrowid is None:
            raise SchedulerJobError("scheduler attempt insert returned no id")
        return RunClaim(
            scheduler_run_id=scheduler_run_id,
            attempt_id=int(attempt.lastrowid),
            attempt_number=attempt_number,
            retrying_terminal=(existing is not None and retrying_terminal),
            retry_terminal_if_completed_before=(
                retry_terminal_if_completed_before
                if existing is not None and retrying_terminal
                else None
            ),
            terminal_retry_started_at=(
                terminal_retry_started_at
                if existing is not None and retrying_terminal
                else None
            ),
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
    with connect(db_path) as connection, transaction(connection):
        attempts = connection.execute(
            """
            SELECT id,scheduler_run_id,details_json FROM scheduler_run_attempts
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
            try:
                interrupted_details = json.loads(str(attempt["details_json"] or "{}"))
            except json.JSONDecodeError as exc:
                raise SchedulerJobError(
                    "running scheduler attempt has invalid details"
                ) from exc
            if not isinstance(interrupted_details, dict):
                raise SchedulerJobError(
                    "running scheduler attempt details must be an object"
                )
            interrupted_details["reason"] = "writer_process_restarted"
            details_json = json.dumps(
                interrupted_details,
                ensure_ascii=False,
                sort_keys=True,
            )
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


def _default_douyin_openapi_runner(
    *, scheduled_for: datetime, db_path: Path
) -> Mapping[str, Any]:
    # Keep the scheduler importable while the OpenAPI integration is disabled.
    # The concrete writer-side implementation is loaded only when this job runs.
    from .douyin_openapi_sync import run_douyin_openapi_reconcile

    return run_douyin_openapi_reconcile(
        scheduled_for=scheduled_for,
        db_path=db_path,
    )


def _default_douyin_openapi_environment_present() -> bool:
    from .douyin_openapi_client import douyin_sync_environment_present

    return douyin_sync_environment_present()


def _validate_douyin_openapi_details(
    value: Mapping[str, Any],
) -> tuple[str, Dict[str, Any]]:
    details = dict(value)
    if set(details) != {"window_start", "coverage_end", "accounts"}:
        raise SchedulerJobError("Douyin OpenAPI receipt fields are invalid")
    window_start = details.get("window_start")
    coverage_end = details.get("coverage_end")
    accounts = details.get("accounts")
    if not isinstance(window_start, str) or not window_start:
        raise SchedulerJobError("Douyin OpenAPI receipt is missing window_start")
    if not isinstance(coverage_end, str) or not coverage_end:
        raise SchedulerJobError("Douyin OpenAPI receipt is missing coverage_end")
    if not isinstance(accounts, list):
        raise SchedulerJobError("Douyin OpenAPI receipt accounts must be a list")

    account_statuses: List[str] = []
    seen_identities: set[tuple[int, str]] = set()
    for account in accounts:
        if not isinstance(account, Mapping):
            raise SchedulerJobError("Douyin OpenAPI account receipt must be an object")
        required_fields = {
            "account_id",
            "platform_uid",
            "status",
            "coverage_start",
            "coverage_end",
            "coverage_complete",
            "pagination_complete",
            "materialization_complete",
            "pages_fetched",
            "items_discovered",
        }
        account_fields = set(account)
        if not required_fields <= account_fields or not account_fields <= (
            required_fields | {"error_code"}
        ):
            raise SchedulerJobError("Douyin OpenAPI account receipt fields are invalid")
        account_id = account.get("account_id")
        platform_uid = account.get("platform_uid")
        status = account.get("status")
        if (
            isinstance(account_id, bool)
            or not isinstance(account_id, int)
            or account_id <= 0
            or not isinstance(platform_uid, str)
            or not 6 <= len(platform_uid) <= 24
            or not platform_uid.isdigit()
        ):
            raise SchedulerJobError("Douyin OpenAPI account identity is invalid")
        identity = (account_id, platform_uid)
        if identity in seen_identities:
            raise SchedulerJobError("Douyin OpenAPI account receipt is duplicated")
        seen_identities.add(identity)
        if status not in {"succeeded", "partial", "failed"}:
            raise SchedulerJobError("Douyin OpenAPI account status is invalid")
        error_code = account.get("error_code")
        if (status == "succeeded" and "error_code" in account) or (
            status != "succeeded"
            and (
                not isinstance(error_code, str)
                or not error_code
                or len(error_code) > 128
                or any(
                    character not in "abcdefghijklmnopqrstuvwxyz0123456789_"
                    for character in error_code
                )
            )
        ):
            raise SchedulerJobError("Douyin OpenAPI account error_code is invalid")
        if account.get("coverage_start") != window_start:
            raise SchedulerJobError("Douyin OpenAPI account coverage_start drifted")
        if account.get("coverage_end") != coverage_end:
            raise SchedulerJobError("Douyin OpenAPI account coverage_end drifted")
        for field in (
            "coverage_complete",
            "pagination_complete",
            "materialization_complete",
        ):
            if not isinstance(account.get(field), bool):
                raise SchedulerJobError(
                    f"Douyin OpenAPI account {field} must be boolean"
                )
        for field in ("pages_fetched", "items_discovered"):
            number = account.get(field)
            if isinstance(number, bool) or not isinstance(number, int) or number < 0:
                raise SchedulerJobError(
                    f"Douyin OpenAPI account {field} must be non-negative"
                )
        if status == "succeeded" and (
            account.get("coverage_complete") is not True
            or account.get("pagination_complete") is not True
            or account.get("materialization_complete") is not True
            or account.get("pages_fetched", 0) < 1
        ):
            raise SchedulerJobError("Douyin OpenAPI succeeded receipt is incomplete")
        account_statuses.append(str(status))

    if not account_statuses:
        run_status = "skipped"
    elif all(status == "failed" for status in account_statuses):
        run_status = "failed"
    elif all(status == "succeeded" for status in account_statuses) and all(
        account.get("coverage_complete") is True
        and account.get("pagination_complete") is True
        and account.get("materialization_complete") is True
        for account in accounts
    ):
        run_status = "succeeded"
    else:
        run_status = "partial"
    details["status"] = run_status
    return run_status, details


def execute_douyin_openapi_reconcile(
    scheduled_for: datetime,
    *,
    db_path: Path = DEFAULT_DB,
    runner: Optional[DouyinOpenApiRunner] = None,
    allow_retry: bool = False,
) -> Dict[str, Any]:
    """Execute the independent OpenAPI occurrence outside the fixed pipeline."""

    if runner is None and not _default_douyin_openapi_environment_present():
        return {
            "job_id": DOUYIN_OPENAPI_RECONCILE_JOB_ID,
            "status": "deferred",
            "reason": "douyin_sync_environment_not_installed",
        }
    claim = _claim_run(
        DOUYIN_OPENAPI_RECONCILE_JOB_ID,
        scheduled_for,
        db_path=db_path,
        allow_retry=allow_retry,
        invocation_source="scheduled",
    )
    if claim is None:
        return {
            "job_id": DOUYIN_OPENAPI_RECONCILE_JOB_ID,
            "status": "skipped_duplicate",
        }
    action = runner or _default_douyin_openapi_runner
    try:
        raw_details = action(scheduled_for=scheduled_for, db_path=db_path)
        if not isinstance(raw_details, Mapping):
            raise SchedulerJobError("Douyin OpenAPI runner must return an object")
        status, details = _validate_douyin_openapi_details(raw_details)
    except Exception:
        _finish_run(
            claim,
            status="failed",
            details={"error_code": "douyin_openapi_reconcile_failed"},
            db_path=db_path,
        )
        raise
    _finish_run(claim, status=status, details=details, db_path=db_path)
    return {
        "job_id": DOUYIN_OPENAPI_RECONCILE_JOB_ID,
        "status": status,
        "attempt_number": claim.attempt_number,
        "details": details,
    }


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


def _douyin_openapi_receipts_for_day(
    local_day: date,
    *,
    db_path: Path,
    required_start: datetime,
    required_end: datetime,
) -> Dict[tuple[int, str], Dict[str, Any]]:
    occurrence = datetime.combine(
        local_day,
        time(DOUYIN_OPENAPI_RECONCILE_HOUR, 0),
        SHANGHAI,
    )
    with connect(db_path) as connection:
        row = connection.execute(
            """
            SELECT status,details_json FROM scheduler_runs
            WHERE job_id=? AND scheduled_for=?
            """,
            (
                DOUYIN_OPENAPI_RECONCILE_JOB_ID,
                _scheduled_iso(occurrence),
            ),
        ).fetchone()
    if row is None or str(row["status"]) not in {"succeeded", "partial"}:
        return {}
    try:
        details = json.loads(str(row["details_json"] or "{}"))
    except json.JSONDecodeError:
        return {}
    if not isinstance(details, Mapping):
        return {}
    window_start = details.get("window_start")
    coverage_end = details.get("coverage_end")
    accounts = details.get("accounts")
    if (
        not isinstance(window_start, str)
        or not window_start
        or not isinstance(coverage_end, str)
        or not coverage_end
        or not isinstance(accounts, list)
    ):
        return {}
    try:
        receipt_start = datetime.fromisoformat(window_start.replace("Z", "+00:00"))
        receipt_end = datetime.fromisoformat(coverage_end.replace("Z", "+00:00"))
    except ValueError:
        return {}
    if (
        receipt_start.tzinfo is None
        or receipt_end.tzinfo is None
        or receipt_start.astimezone(timezone.utc)
        > required_start.astimezone(timezone.utc)
        or receipt_end.astimezone(timezone.utc)
        < required_end.astimezone(timezone.utc)
    ):
        return {}

    receipts: Dict[tuple[int, str], Dict[str, Any]] = {}
    invalid_identities: set[tuple[int, str]] = set()
    for value in accounts:
        if not isinstance(value, Mapping):
            continue
        account_id = value.get("account_id")
        platform_uid = value.get("platform_uid")
        if (
            isinstance(account_id, bool)
            or not isinstance(account_id, int)
            or account_id <= 0
            or not isinstance(platform_uid, str)
            or not platform_uid
        ):
            continue
        identity = (account_id, platform_uid)
        if identity in receipts:
            invalid_identities.add(identity)
            continue
        pages_fetched = value.get("pages_fetched")
        items_discovered = value.get("items_discovered")
        complete = (
            value.get("status") == "succeeded"
            and value.get("coverage_start") == window_start
            and value.get("coverage_end") == coverage_end
            and value.get("coverage_complete") is True
            and value.get("pagination_complete") is True
            and value.get("materialization_complete") is True
            and not isinstance(pages_fetched, bool)
            and isinstance(pages_fetched, int)
            and pages_fetched >= 1
            and not isinstance(items_discovered, bool)
            and isinstance(items_discovered, int)
            and items_discovered >= 0
        )
        if complete:
            receipts[identity] = dict(value)
    for identity in invalid_identities:
        receipts.pop(identity, None)
    return receipts


def _synthetic_openapi_discovery(
    identity: Mapping[str, Any],
    receipt: Mapping[str, Any],
) -> Dict[str, Any]:
    pages_fetched = int(receipt["pages_fetched"])
    pages = [
        {
            "page": 1,
            "status": "succeeded",
            "has_more": False,
            "source_pages_fetched": pages_fetched,
            "missing_published_at_count": 0,
            "derived_stages": {"failures": []},
            "provider_cost": 0.0,
        }
    ]
    return {
        "account_id": int(identity["account_id"]),
        "platform": "douyin",
        "platform_uid": str(identity["uid"]),
        "provider": "DouyinOpenAPI",
        "status": "succeeded",
        "pages": pages,
        "stopped_reason": "provider_exhausted",
        "inserted": 0,
        "updated": 0,
        "items_discovered": int(receipt["items_discovered"]),
        "coverage_start": str(receipt["coverage_start"]),
        "coverage_end": str(receipt["coverage_end"]),
        "provider_cost": 0.0,
    }


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
    capture_circuit = _CaptureCircuit()
    discovery_start = datetime.combine(
        local_day - timedelta(days=1), time.min, SHANGHAI
    ).astimezone(timezone.utc)
    discovery_end = scheduled_for.astimezone(timezone.utc)
    openapi_receipts = _douyin_openapi_receipts_for_day(
        local_day,
        db_path=db_path,
        required_start=discovery_start,
        required_end=discovery_end,
    )
    for identity in identities:
        openapi_receipt = openapi_receipts.get(
            (int(identity["account_id"]), str(identity["uid"]))
        )
        if str(identity["platform"]) == "douyin" and openapi_receipt is not None:
            discovery.append(
                _synthetic_openapi_discovery(identity, openapi_receipt)
            )
            continue
        if capture_circuit.is_set():
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
                capture_circuit.record(error_code)
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

    # ---- 指标优先段：先刷完全部到期指标（便宜、保新鲜度），再跑详情/评论 ----
    metrics_targets = [dict(c) for c in contents if c["metrics_needed"]]
    metrics_refreshed: set[int] = set()
    metrics_attempted: set[int] = set()
    metrics_retry_ids: set[int] = set()
    metrics_first_results: List[Dict[str, Any]] = []
    metrics_first_summary = {
        "attempted": len(metrics_targets),
        "succeeded": 0,
        "budget_blocked": 0,
        "task_budget_exhausted": 0,
        "budget_daily_quota_exhausted": 0,
        "failed": 0,
    }

    def refresh_metrics_only(content: Mapping[str, Any]) -> Dict[str, Any]:
        if capture_circuit.is_set():
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
            capture_circuit.record(error_code)
            return {
                "content_id": content["id"],
                "status": "failed",
                "error_code": error_code,
            }
        capture_circuit.record_result(result)
        return result

    if metrics_targets:
        metrics_workers = min(DAILY_CAPTURE_WORKERS, max(1, len(metrics_targets)))
        with ThreadPoolExecutor(
            max_workers=metrics_workers, thread_name_prefix="dcar-metrics-first"
        ) as pool:
            metrics_first_results = list(pool.map(refresh_metrics_only, metrics_targets))
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
            if error_code in {
                "budget_blocked",
                "task_budget_exhausted",
                "budget_daily_quota_exhausted",
            }:
                metrics_first_summary[error_code] += 1
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
        if capture_circuit.is_set():
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
            capture_circuit.record(error_code)
            return {
                "content_id": content["id"],
                "status": "failed",
                "error_code": error_code,
                "message": str(exc),
            }
        capture_circuit.record_result(result)
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
            except Exception as exc:
                error_code = getattr(exc, "error_code", type(exc).__name__)
                capture_circuit.record(error_code)
                return index, {
                    **initial,
                    "retry_error_code": str(error_code),
                    "retry_error_message": str(exc)[:500],
                }
            capture_circuit.record_result(retry_result)
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
                if any(
                    stage.get("stage") == "metrics"
                    and stage.get("status") == "succeeded"
                    for stage in retry_result.get("stages", [])
                ):
                    metrics_refreshed.add(int(contents[index]["id"]))
    unresolved_metrics = {
        int(content["id"]) for content in metrics_targets
    } - metrics_refreshed
    metrics_first_summary["final_succeeded"] = len(metrics_refreshed)
    metrics_first_summary["final_unresolved"] = len(unresolved_metrics)

    reported_provider_cost = sum(
        float(item.get("provider_cost") or 0)
        for item in discovery + metrics_first_results + content_updates
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
    reported_provider_cost = round(reported_provider_cost, 6)
    ledger_provider_cost = round(ledger_provider_cost, 6)
    provider_cost = max(reported_provider_cost, ledger_provider_cost)
    incomplete_discovery = sum(
        item.get("status") != "succeeded" for item in discovery
    )
    incomplete_contents = sum(
        item.get("status") not in {"succeeded", "already_succeeded"}
        for item in content_updates
    )
    completed_content_ids = {
        int(item["content_id"])
        for item in content_updates
        if item.get("content_id") is not None
        and item.get("status") in {"succeeded", "already_succeeded"}
    }
    incomplete_content_ids = {
        int(item["content_id"])
        for item in content_updates
        if item.get("content_id") is not None
        and item.get("status") not in {"succeeded", "already_succeeded"}
    }
    # A metrics-first result and its later content result describe the same
    # selected content. Count each content once when both have the same outcome,
    # while retaining a metric-only success/failure that the content result does
    # not represent (for example an ``already_succeeded`` main-loop result after
    # a failed metrics-first attempt).
    successful_operations = (
        len(discovery)
        - incomplete_discovery
        + len(content_updates)
        - incomplete_contents
        + len(metrics_refreshed - completed_content_ids)
    )
    failed_operations = (
        incomplete_discovery
        + incomplete_contents
        + len(unresolved_metrics - incomplete_content_ids)
    )
    circuit_break_counts = capture_circuit.snapshot()
    provider_fatal_count = sum(
        circuit_break_counts.get(code, 0) for code in CAPTURE_PROVIDER_FATAL_CODES
    )
    blocked_providers = ["TikHub"] if provider_fatal_count else []
    details = {
        "task_id": task_id,
        "budget_max_amount": max_amount,
        "content_limit": content_limit,
        "monitored_accounts": len(identities),
        "monitored_contents": len(contents),
        "eligible_contents": eligible_contents,
        "metrics_first": metrics_first_summary,
        "discovery": discovery,
        "content_updates": content_updates,
        "blocked_providers": blocked_providers,
        "circuit_break_counts": circuit_break_counts,
        "successful_operations": successful_operations,
        "failed_operations": failed_operations,
        "reported_provider_cost": reported_provider_cost,
        "ledger_provider_cost": ledger_provider_cost,
        "provider_cost": provider_cost,
    }
    quality_gate = daily_capture_quality_gate(details)
    fatal_count = sum(
        circuit_break_counts.get(code, 0)
        for code in CAPTURE_PROVIDER_FATAL_CODES | CAPTURE_BUDGET_FATAL_CODES
    )
    allowed_stop_count = sum(
        circuit_break_counts.get(code, 0) for code in CAPTURE_ALLOWED_STOP_CODES
    )
    if fatal_count or successful_operations == 0 or not quality_gate["passed"]:
        status = "failed"
    elif failed_operations or allowed_stop_count:
        status = "partial"
    else:
        status = "succeeded"
    details["status"] = status
    details["quality_gate"] = quality_gate
    return details


def daily_capture_quality_gate(details: Mapping[str, Any]) -> Dict[str, Any]:
    """Evaluate an occurrence for rollout acceptance without rewriting status."""

    discovery = list(details.get("discovery") or [])
    content_updates = list(details.get("content_updates") or [])
    monitored_accounts = int(details.get("monitored_accounts") or 0)
    monitored_contents = int(details.get("monitored_contents") or 0)
    discovery_succeeded = sum(
        item.get("status") == "succeeded" for item in discovery
    )
    content_succeeded = sum(
        item.get("status") in {"succeeded", "already_succeeded"}
        for item in content_updates
    )
    reported_provider_cost = details.get("reported_provider_cost")
    if reported_provider_cost is None:
        # The verified 2026-08-11 occurrence predates the split cost fields and
        # records only the ledger-authoritative ``provider_cost`` total. Rebuild
        # its return-payload subtotal from the exact rows in details without
        # pretending that the two sources were equal.
        reported_provider_cost = sum(
            float(item.get("provider_cost") or 0)
            for group in (
                discovery,
                list(details.get("metrics_first_results") or []),
                content_updates,
            )
            for item in group
        )
    reported_cost = round(float(reported_provider_cost or 0), 6)
    ledger_source_present = (
        details.get("ledger_provider_cost") is not None
        or details.get("provider_cost") is not None
    )
    ledger_cost = round(
        float(details.get("ledger_provider_cost", details.get("provider_cost", 0)) or 0),
        6,
    )
    budget_declaration_present = details.get("budget_max_amount") is not None
    budget_max = float(
        details.get("budget_max_amount", DAILY_CAPTURE_MAX_AMOUNT)
    )
    raw_circuit_counts = details.get("circuit_break_counts") or {}
    circuit_break_counts = {
        str(code): max(0, int(count or 0))
        for code, count in (
            raw_circuit_counts.items()
            if isinstance(raw_circuit_counts, Mapping)
            else []
        )
    }
    provider_fatal_count = sum(
        circuit_break_counts.get(code, 0) for code in CAPTURE_PROVIDER_FATAL_CODES
    )
    budget_fatal_count = sum(
        circuit_break_counts.get(code, 0) for code in CAPTURE_BUDGET_FATAL_CODES
    )
    successful_operations = int(
        details.get(
            "successful_operations",
            discovery_succeeded + content_succeeded,
        )
        or 0
    )
    ledger_exactly_matches_reported = abs(reported_cost - ledger_cost) <= 1e-6
    checks = {
        "accounts_complete": len(discovery) == monitored_accounts,
        "contents_complete": len(content_updates) == monitored_contents,
        "discovery_rate": (
            monitored_accounts > 0
            and discovery_succeeded * 100
            >= monitored_accounts * DAILY_CAPTURE_DISCOVERY_QUALITY_PERCENT
        ),
        "content_rate": (
            monitored_contents == 0
            or content_succeeded * 100
            >= monitored_contents * DAILY_CAPTURE_CONTENT_QUALITY_PERCENT
        ),
        "providers_unblocked": (
            not list(details.get("blocked_providers") or [])
            and provider_fatal_count == 0
        ),
        "budget_contract_unblocked": budget_fatal_count == 0,
        "successful_operations_present": successful_operations > 0,
        "ledger_source_present": ledger_source_present,
        "budget_declaration_present": budget_declaration_present,
        # provider_usage is the authoritative billed ledger. Per-operation
        # return payloads can structurally under-report billed retries and
        # post-fetch failures, so equality is diagnostic rather than a gate.
        "ledger_covers_reported": ledger_cost + 1e-9 >= reported_cost,
        "cost_values_valid": all(
            math.isfinite(value) and value >= 0
            for value in (reported_cost, ledger_cost, budget_max)
        ),
        "budget_contract": (
            0 < budget_max <= DAILY_CAPTURE_MAX_AMOUNT + 1e-9
        ),
        "within_budget": (
            ledger_cost <= budget_max + 1e-9
            and ledger_cost <= DAILY_CAPTURE_MAX_AMOUNT + 1e-9
        ),
    }
    return {
        "passed": all(checks.values()),
        "checks": checks,
        "discovery_succeeded": discovery_succeeded,
        "monitored_accounts": monitored_accounts,
        "content_succeeded": content_succeeded,
        "monitored_contents": monitored_contents,
        "discovery_threshold_percent": DAILY_CAPTURE_DISCOVERY_QUALITY_PERCENT,
        "content_threshold_percent": DAILY_CAPTURE_CONTENT_QUALITY_PERCENT,
        "declared_budget_max_amount": budget_max,
        "authorized_budget_max_amount": DAILY_CAPTURE_MAX_AMOUNT,
        "circuit_break_counts": circuit_break_counts,
        "cost_reconciliation": {
            "reported_provider_cost": reported_cost,
            "ledger_provider_cost": ledger_cost,
            "ledger_minus_reported": round(ledger_cost - reported_cost, 6),
            "ledger_exactly_matches_reported": ledger_exactly_matches_reported,
        },
    }


def _daily_media_cohort(
    scheduled_for: datetime, *, db_path: Path
) -> tuple[List[int], str, str]:
    """Return the report-day content cohort for an automatic run on D."""

    local_day = scheduled_for.astimezone(SHANGHAI).date()
    report_day = local_day - timedelta(days=1)
    start = datetime.combine(report_day, time.min, SHANGHAI).astimezone(timezone.utc)
    end = start + timedelta(days=1)
    start_iso = start.isoformat(timespec="seconds").replace("+00:00", "Z")
    end_iso = end.isoformat(timespec="seconds").replace("+00:00", "Z")
    with connect(db_path) as connection:
        rows = connection.execute(
            """
            SELECT c.id
            FROM content_items c
            WHERE c.platform IN ('douyin','xiaohongshu')
              AND c.content_type IN ('video','image')
              AND COALESCE(c.source_group,'') NOT IN (?,?)
              AND c.published_at>=? AND c.published_at<?
            ORDER BY c.id
            """,
            (*BACKFILL_SOURCE_GROUPS, start_iso, end_iso),
        ).fetchall()
    return [int(row["id"]) for row in rows], start_iso, end_iso


def run_media_cutoff(
    scheduled_for: datetime,
    *,
    db_path: Path = DEFAULT_DB,
) -> Dict[str, Any]:
    """Evaluate completed media DAGs at the daily cutoff."""

    coverage_content_ids, start_iso, end_iso = _daily_media_cohort(
        scheduled_for, db_path=db_path
    )
    with connect(db_path) as connection:
        releases = connection.execute(
            "SELECT id FROM evaluation_releases WHERE status='active' ORDER BY id"
        ).fetchall()
        if len(releases) != 1:
            raise SchedulerJobError(
                "daily media cutoff requires exactly one active evaluation release"
            )
        release_id = str(releases[0]["id"])
        initial_states = media_terminal_state_details(
            connection, release_id, coverage_content_ids
        )

    evaluation_results: List[Dict[str, Any]] = []
    evaluation_content_ids = [
        content_id
        for content_id in coverage_content_ids
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
    }


def _retry_terminal_report_task_if_stale(
    task: Mapping[str, Any],
    *,
    retry_started_at: Optional[datetime],
    db_path: Path,
) -> None:
    if retry_started_at is None or str(task["task_status"]) not in {
        "succeeded",
        "partial",
    }:
        return
    completed_value = task.get("completed_at")
    if not isinstance(completed_value, str) or not completed_value:
        raise SchedulerJobError("terminal report task is missing completed_at")
    try:
        completed_at = datetime.fromisoformat(completed_value.replace("Z", "+00:00"))
    except ValueError as exc:
        raise SchedulerJobError(
            f"invalid terminal report task completed_at: {completed_value}"
        ) from exc
    if completed_at.tzinfo is None:
        raise SchedulerJobError("terminal report task completed_at must include timezone")
    if completed_at.astimezone(timezone.utc) < retry_started_at.astimezone(timezone.utc):
        retry_task(str(task["id"]), db_path=db_path)


def _run_job_action(
    job_id: str,
    scheduled_for: datetime,
    *,
    db_path: Path,
    reports_root: Path,
    capture_call_override: Optional[
        Callable[[str, Mapping[str, Any]], ProviderResult]
    ] = None,
    retry_terminal_if_completed_before: Optional[datetime] = None,
    terminal_retry_started_at: Optional[datetime] = None,
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
        cohort_content_ids, _window_start, _window_end = _daily_media_cohort(
            scheduled_for, db_path=db_path
        )
        fresh_content = run_media_download_queue(
            limit=MEDIA_QUEUE_BATCH_LIMIT,
            db_path=db_path,
            scope_content_ids=cohort_content_ids,
        )
        status = (
            "failed"
            if int(fresh_content.get("retryable_failed", 0)) > 0
            or bool(fresh_content.get("truncated"))
            else "succeeded"
        )
        return status, {"fresh_content": fresh_content}
    if job_id == "daily_media_processing":
        cohort_content_ids, _window_start, _window_end = _daily_media_cohort(
            scheduled_for, db_path=db_path
        )
        media = run_media_processing_queue(
            limit=MEDIA_QUEUE_BATCH_LIMIT,
            db_path=db_path,
            scope_content_ids=cohort_content_ids,
        )
        duplicates = run_duplicate_fingerprint_queue(
            limit=500,
            db_path=db_path,
            scope_content_ids=cohort_content_ids,
        )
        status = (
            "failed"
            if int(media.get("retryable_failed", 0)) > 0
            or bool(media.get("truncated"))
            or int(duplicates.get("failed", 0)) > 0
            or bool(duplicates.get("truncated"))
            else "succeeded"
        )
        return status, {"media": media, "duplicates": duplicates}
    if job_id == "daily_media_cutoff":
        return "succeeded", run_media_cutoff(scheduled_for, db_path=db_path)
    if job_id == "daily_report":
        target = local_date - timedelta(days=1)
        if retry_terminal_if_completed_before is not None:
            existing_task = create_task(
                task_type="daily",
                period_start=target.isoformat(),
                period_end=target.isoformat(),
                creation_source="automatic",
                db_path=db_path,
            )
            _retry_terminal_report_task_if_stale(
                existing_task,
                retry_started_at=terminal_retry_started_at,
                db_path=db_path,
            )
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
        if retry_terminal_if_completed_before is not None:
            existing_task = create_task(
                task_type="weekly",
                period_start=start.isoformat(),
                period_end=end.isoformat(),
                creation_source="automatic",
                db_path=db_path,
            )
            _retry_terminal_report_task_if_stale(
                existing_task,
                retry_started_at=terminal_retry_started_at,
                db_path=db_path,
            )
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
    retry_terminal_if_completed_before: Optional[datetime] = None,
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
    if (
        allow_retry
        and job_id not in REPORT_JOB_IDS
        and invocation_source not in {"operator_retry", "scheduled"}
    ):
        raise SchedulerJobError(
            "non-report scheduler retry source is not authorized"
        )
    if invocation_source == "operator_retry" and not allow_retry:
        raise SchedulerJobError("operator_retry requires allow_retry=True")
    if retry_terminal_if_completed_before is not None:
        if job_id == "daily_capture":
            raise SchedulerJobError(
                "terminal daily_capture runs cannot be retried automatically"
            )
        if not allow_retry:
            raise SchedulerJobError(
                "terminal downstream retry requires allow_retry=True"
            )
    if job_id == "weekly_report":
        dependency = _weekly_daily_dependency(scheduled_for, db_path=db_path)
        if not dependency["ready"]:
            return {
                "job_id": job_id,
                "status": "deferred",
                "reason": "weekly_daily_dependency_not_ready",
                "dependency": dependency,
            }
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
        retry_terminal_if_completed_before=retry_terminal_if_completed_before,
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
            retry_terminal_if_completed_before=(
                claim.retry_terminal_if_completed_before
                if claim.retrying_terminal
                else None
            ),
            terminal_retry_started_at=(
                claim.terminal_retry_started_at if claim.retrying_terminal else None
            ),
        )
    except Exception as exc:
        failure_details: Dict[str, Any] = {"error": str(exc)}
        if claim.retrying_terminal:
            if (
                claim.retry_terminal_if_completed_before is None
                or claim.terminal_retry_started_at is None
            ):
                raise SchedulerJobError(
                    "stale terminal retry claim lost its retry marker"
                ) from exc
            failure_details.update(
                _stale_retry_details(
                    claim.retry_terminal_if_completed_before,
                    retry_started_at=claim.terminal_retry_started_at,
                )
            )
        _finish_run(
            claim,
            status="failed",
            details=failure_details,
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
    capture_call_override: Optional[
        Callable[[str, Mapping[str, Any]], ProviderResult]
    ],
) -> None:
    definition = next(job for job in JOBS if job.job_id == job_id)
    occurrence = latest_occurrence(definition, datetime.now(SHANGHAI))
    execute_job(
        job_id,
        occurrence,
        db_path=db_path,
        reports_root=reports_root,
        capture_call_override=capture_call_override,
    )


def _daily_capture_guard_job(
    *,
    effective_from: date,
    db_path: Path,
    reports_root: Path,
    capture_call_override: Optional[
        Callable[[str, Mapping[str, Any]], ProviderResult]
    ],
) -> None:
    current_day_daily_capture_guard(
        now=datetime.now(SHANGHAI),
        effective_from=effective_from,
        db_path=db_path,
        reports_root=reports_root,
        capture_call_override=capture_call_override,
    )


def _current_day_pipeline_guard_job(
    *,
    effective_from: date,
    db_path: Path,
    reports_root: Path,
    capture_call_override: Optional[
        Callable[[str, Mapping[str, Any]], ProviderResult]
    ],
) -> None:
    current_day_pipeline_guard(
        now=datetime.now(SHANGHAI),
        effective_from=effective_from,
        db_path=db_path,
        reports_root=reports_root,
        capture_call_override=capture_call_override,
    )


def _douyin_openapi_live_job(
    *,
    db_path: Path,
    runner: Optional[DouyinOpenApiRunner],
) -> None:
    definition = JobDefinition(
        DOUYIN_OPENAPI_RECONCILE_JOB_ID,
        DOUYIN_OPENAPI_RECONCILE_HOUR,
        0,
    )
    occurrence = latest_occurrence(definition, datetime.now(SHANGHAI))
    execute_douyin_openapi_reconcile(
        occurrence,
        db_path=db_path,
        runner=runner,
    )


def douyin_openapi_reconcile_guard(
    *,
    now: datetime,
    effective_from: date,
    db_path: Path,
    runner: Optional[DouyinOpenApiRunner] = None,
) -> Dict[str, Any]:
    local_now = now.astimezone(SHANGHAI)
    if local_now.date() < effective_from:
        return {"status": "before_effective_date"}
    occurrence = datetime.combine(
        local_now.date(),
        time(DOUYIN_OPENAPI_RECONCILE_HOUR, 0),
        SHANGHAI,
    )
    if local_now < occurrence:
        return {"status": "before_today_slot"}
    return execute_douyin_openapi_reconcile(
        occurrence,
        db_path=db_path,
        runner=runner,
        allow_retry=True,
    )


def _douyin_openapi_reconcile_guard_job(
    *,
    effective_from: date,
    db_path: Path,
    runner: Optional[DouyinOpenApiRunner],
) -> None:
    douyin_openapi_reconcile_guard(
        now=datetime.now(SHANGHAI),
        effective_from=effective_from,
        db_path=db_path,
        runner=runner,
    )


def install_jobs(
    scheduler: BackgroundScheduler,
    *,
    db_path: Path,
    reports_root: Path,
    capture_call_override: Optional[
        Callable[[str, Mapping[str, Any]], ProviderResult]
    ],
    reconcile_effective_date: date,
    douyin_openapi_runner: Optional[DouyinOpenApiRunner] = None,
) -> None:
    for job in JOBS:
        callback: Callable[..., None]
        if job.job_id == "daily_capture":
            callback = _daily_capture_guard_job
            kwargs = {
                "effective_from": reconcile_effective_date,
                "db_path": db_path,
                "reports_root": reports_root,
                "capture_call_override": capture_call_override,
            }
        else:
            callback = _current_day_pipeline_guard_job
            kwargs = {
                "effective_from": reconcile_effective_date,
                "db_path": db_path,
                "reports_root": reports_root,
                "capture_call_override": capture_call_override,
            }
        trigger_minute = (
            DAILY_CAPTURE_START_DELAY_MINUTES
            if job.job_id == "daily_capture"
            else job.minute
        )
        scheduler.add_job(
            callback,
            CronTrigger(
                hour=job.hour,
                minute=trigger_minute,
                day_of_week=job.day_of_week,
                timezone=SHANGHAI,
            ),
            id=job.job_id,
            replace_existing=True,
            kwargs=kwargs,
            coalesce=True,
            max_instances=1,
            misfire_grace_time=3600,
        )
    scheduler.add_job(
        _douyin_openapi_live_job,
        CronTrigger(
            hour=DOUYIN_OPENAPI_RECONCILE_HOUR,
            minute=0,
            timezone=SHANGHAI,
        ),
        id=DOUYIN_OPENAPI_RECONCILE_JOB_ID,
        replace_existing=True,
        kwargs={
            "db_path": db_path,
            "runner": douyin_openapi_runner,
        },
        coalesce=True,
        max_instances=1,
        misfire_grace_time=3600,
    )
    scheduler.add_job(
        _douyin_openapi_reconcile_guard_job,
        IntervalTrigger(hours=1, timezone=SHANGHAI),
        id=DOUYIN_OPENAPI_RECONCILE_GUARD_JOB_ID,
        replace_existing=True,
        kwargs={
            "effective_from": reconcile_effective_date,
            "db_path": db_path,
            "runner": douyin_openapi_runner,
        },
        next_run_time=datetime.now(SHANGHAI),
        coalesce=True,
        max_instances=1,
        misfire_grace_time=None,
    )
    scheduler.add_job(
        _current_day_pipeline_guard_job,
        IntervalTrigger(hours=1, timezone=SHANGHAI),
        id="daily_capture_reconcile",
        replace_existing=True,
        kwargs={
            "effective_from": reconcile_effective_date,
            "db_path": db_path,
            "reports_root": reports_root,
            "capture_call_override": capture_call_override,
        },
        next_run_time=datetime.now(SHANGHAI),
        coalesce=True,
        max_instances=1,
        misfire_grace_time=None,
    )
    scheduler.add_job(
        _report_reconcile_job,
        IntervalTrigger(hours=1, timezone=SHANGHAI),
        id="report_reconcile",
        replace_existing=True,
        kwargs={"db_path": db_path, "reports_root": reports_root},
        next_run_time=datetime.now(SHANGHAI),
        coalesce=True,
        max_instances=1,
        misfire_grace_time=None,
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


def _report_period(job_id: str, occurrence: datetime) -> tuple[date, date]:
    end = occurrence.astimezone(SHANGHAI).date() - timedelta(days=1)
    if job_id == "daily_report":
        return end, end
    if job_id == "weekly_report":
        return end - timedelta(days=6), end
    raise SchedulerJobError(f"not a report job: {job_id}")


def _report_duplicate_input_retry_before(
    job_id: str, occurrence: datetime, *, db_path: Path
) -> Optional[datetime]:
    period_start, period_end = _report_period(job_id, occurrence)
    start_utc = datetime.combine(period_start, time.min, SHANGHAI).astimezone(
        timezone.utc
    )
    end_utc = datetime.combine(
        period_end + timedelta(days=1), time.min, SHANGHAI
    ).astimezone(timezone.utc)
    start_key = _scheduled_iso(start_utc)
    end_key = _scheduled_iso(end_utc)
    with connect(db_path) as connection:
        run = connection.execute(
            """
            SELECT status,completed_at FROM scheduler_runs
            WHERE job_id=? AND scheduled_for=?
            """,
            (job_id, _scheduled_iso(occurrence)),
        ).fetchone()
        task_type = "daily" if job_id == "daily_report" else "weekly"
        task = connection.execute(
            """
            SELECT task_status,completed_at FROM report_tasks
            WHERE task_type=? AND period_start=? AND period_end=?
              AND creation_source='automatic'
            """,
            (task_type, period_start.isoformat(), period_end.isoformat()),
        ).fetchone()
        watermark = connection.execute(
            """
            SELECT MAX(changed_at) FROM (
                SELECT df.created_at AS changed_at
                FROM duplicate_fingerprints df
                JOIN content_items c ON c.id=df.content_id
                WHERE df.fingerprint_version=?
                  AND c.published_at>=? AND c.published_at<?
                UNION ALL
                SELECT dr.created_at AS changed_at
                FROM duplicate_relations dr
                JOIN content_items c ON c.id=dr.duplicate_content_id
                WHERE dr.method=?
                  AND c.published_at>=? AND c.published_at<?
            )
            """,
            (
                FINGERPRINT_VERSION,
                start_key,
                end_key,
                RELATION_METHOD,
                start_key,
                end_key,
            ),
        ).fetchone()[0]
    if (
        run is None
        or str(run["status"]) not in TERMINAL_RUN_STATUSES
        or not run["completed_at"]
        or task is None
        or str(task["task_status"]) not in {"succeeded", "partial"}
        or not task["completed_at"]
        or watermark is None
    ):
        return None

    def parse_utc(value: Any, label: str) -> datetime:
        try:
            parsed = datetime.fromisoformat(str(value).replace("Z", "+00:00"))
        except ValueError as exc:
            raise SchedulerJobError(f"invalid {label}: {value}") from exc
        if parsed.tzinfo is None:
            raise SchedulerJobError(f"{label} must include timezone")
        return parsed.astimezone(timezone.utc)

    watermark_at = parse_utc(watermark, "duplicate input watermark")
    task_completed_at = parse_utc(task["completed_at"], "report task completed_at")
    if watermark_at <= task_completed_at:
        return None
    run_completed_at = parse_utc(run["completed_at"], "report run completed_at")
    return max(watermark_at, run_completed_at + timedelta(seconds=1))


def _report_catchup_occurrences(
    *, current: datetime, db_path: Path
) -> List[tuple[str, datetime]]:
    """Return due failed/interrupted and natural-day missing report slots."""

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
        observation_anchor = _automatic_capture_observation_start_date(connection)
        daily_anchor: Optional[date] = observation_anchor
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
            if task_type == "daily" and daily_anchor is None:
                daily_anchor = start

        latest_daily = latest_occurrence(
            next(job for job in JOBS if job.job_id == "daily_report"), current
        )
        if daily_anchor is None:
            daily_anchor = latest_daily.date() - timedelta(days=1)
        existing = {
            (str(row["job_id"]), str(row["scheduled_for"]))
            for row in connection.execute(
                """
                SELECT job_id,scheduled_for FROM scheduler_runs
                WHERE job_id IN ('daily_report','weekly_report')
                """
            )
        }
        if observation_anchor is not None:
            report_days: List[date] = []
            report_day = daily_anchor
            latest_report_day = latest_daily.date() - timedelta(days=1)
            while report_day <= latest_report_day:
                report_days.append(report_day)
                report_day += timedelta(days=1)
        else:
            report_days = [
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
        for report_day in report_days:
            occurrence = _report_occurrence(
                "daily_report", period_start=report_day, period_end=report_day
            )
            if (
                occurrence <= current
                and ("daily_report", _scheduled_iso(occurrence)) not in existing
            ):
                candidates.add(("daily_report", occurrence))

        if observation_anchor is not None:
            week_starts = []
            week_start = daily_anchor - timedelta(days=daily_anchor.weekday())
            while _report_occurrence(
                "weekly_report",
                period_start=week_start,
                period_end=week_start + timedelta(days=6),
            ) <= current:
                week_starts.append(week_start)
                week_start += timedelta(days=7)
        else:
            week_starts = sorted(
                {
                    report_day - timedelta(days=report_day.weekday())
                    for report_day in report_days
                }
            )
        for week_start in week_starts:
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
    stale_terminal: set[tuple[str, str]] = set()
    for job_id, scheduled_for in terminal:
        occurrence = datetime.fromisoformat(
            scheduled_for.replace("Z", "+00:00")
        ).astimezone(SHANGHAI)
        if occurrence > current:
            continue
        if _report_duplicate_input_retry_before(
            job_id, occurrence, db_path=db_path
        ) is not None:
            candidates.add((job_id, occurrence))
            stale_terminal.add((job_id, scheduled_for))
    return sorted(
        (
            (job_id, occurrence)
            for job_id, occurrence in candidates
            if (job_id, _scheduled_iso(occurrence)) not in terminal
            or (job_id, _scheduled_iso(occurrence)) in stale_terminal
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
            retry_terminal_if_completed_before = (
                _report_duplicate_input_retry_before(
                    job_id, occurrence, db_path=db_path
                )
            )
            result = execute_job(
                job_id,
                occurrence,
                db_path=db_path,
                reports_root=reports_root,
                allow_retry=True,
                invocation_source="startup_report_catchup",
                retry_terminal_if_completed_before=retry_terminal_if_completed_before,
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


def _report_reconcile_job(*, db_path: Path, reports_root: Path) -> None:
    startup_catchup(
        now=datetime.now(SHANGHAI),
        db_path=db_path,
        reports_root=reports_root,
    )
