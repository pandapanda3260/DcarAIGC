"""Matrix-first scheduling and bounded queues over the existing ledgers.

The production scheduler only registers this generation. Legacy scheduler
functions remain readable for old receipts, but are not scheduled or chained.
"""

from __future__ import annotations

import json
import sqlite3
from concurrent.futures import ThreadPoolExecutor
from contextlib import AbstractContextManager, contextmanager, nullcontext
from contextvars import copy_context
from datetime import date, datetime, time, timedelta, timezone
from pathlib import Path
from threading import Event, RLock, Thread
from time import monotonic, sleep
from typing import Any, Callable, Mapping, Sequence
from zoneinfo import ZoneInfo

from . import durable_runs
from .capture_planning import legacy_queue_allowed
from .account_roster import (
    RosterError,
    current_snapshot,
    get_current_members,
    require_active_member,
    runtime_snapshot,
)
from .capture import ProviderResult
from .automatic_scope import automatic_from_date, automatic_scope, automatic_start_at, within_automatic_scope
from .duplicates import FINGERPRINT_VERSION, _current_source_state, run_duplicate_fingerprint_queue
from .evaluation import evaluate_content
from .media import run_media_download_queue, run_media_processing_queue
from .media_lifecycle import LifecycleError, current_bundle
from .media_state import media_terminal_state_details
from .paid_drain import (
    PaidDrainBlocked,
    dispatch_state as paid_dispatch_state,
    require_paid_dispatch_open,
)
from .provider_budget import (
    DEFAULT_TASK_MAX_AMOUNT_USD,
    TIKHUB_NETWORK_CONCURRENCY,
    budget_summary,
    evaluate_transport_faults,
    micro_usd,
    paid_scope,
)
from .provider_updates import missing_metric_fields, refresh_account_profile, refresh_content_metrics
from .providers import capture_content_comments_live, update_content_data
from .work_readiness import WorkReadinessPass, record_work_blocked
from .reconcile_control import current_reconcile_budget, reconcile_budget_scope
from .source_routing import load_policy, metric_cycle_key, metric_refresh_due, parse_time
from .storage import (
    DEFAULT_DB,
    connect,
    now_utc,
    transaction,
    transaction_metrics_context,
)

BEIJING = ZoneInfo("Asia/Shanghai")
PIPELINE_VERSION = "matrix-first-pipeline-v1"
ACTIVATION_JOB = "matrix_pipeline_activation"
MATRIX_PROFILE = "matrix_hybrid_v1"
TIKHUB_PROFILE = "tikhub_managed_v1"
RETIRED_JOB_IDS = frozenset({
    "daily_capture", "daily_media_download", "daily_media_processing", "daily_media_cutoff",
    "daily_capture_reconcile", "history_recovery",
})
QUEUE_KINDS = {"content_pipeline", "metrics_backfill", "comments_refresh", "history_recovery"}
HISTORY_START = "2026-08-02T16:00:00Z"
LOCAL_PROCESSING_LOCK = RLock()
LOCAL_ANALYSIS_JOB = "local_content_analysis"
QUEUE_RESERVATION_LOCKS = {kind: RLock() for kind in QUEUE_KINDS}
MEDIA_BLOCKED_REASONS = frozenset({"restore_required", "expired_non_replayable", "original_unavailable"})
CRON_ROUNDS = {
    "matrix_account_metrics": ("matrix_account_metrics", "2,6,12,18", 0, None),
    "matrix_works_scan": ("matrix_works_scan", "2", 10, None),
    "matrix_works_refresh": ("matrix_works_scan", "6,12,18", 0, None),
    "tikhub_account_metrics": ("tikhub_account_metrics", "2,6,12,18", 0, None),
    "tikhub_works_scan": ("tikhub_works_scan", "2", 10, None),
    "tikhub_works_refresh": ("tikhub_works_scan", "6,12,18", 0, None),
    "tikhub_reconcile": ("tikhub_reconcile", "3", 0, None),
    "metrics_backfill": ("metrics_backfill", "6", 10, None),
    "metrics_backfill_close": ("metrics_backfill", "7", 0, None),
    "metrics_backfill_established": ("metrics_backfill", "8", 35, None),
    "daily_pipeline_summary": ("daily_pipeline_summary", "7", 30, None),
    "daily_report": ("daily_report", "8", 0, None),
    "weekly_report": ("weekly_report", "8", 30, "mon"),
}
SHARED_CRON_REGISTRATIONS = frozenset({
    "tikhub_reconcile",
    "metrics_backfill",
    "metrics_backfill_close",
    "metrics_backfill_established",
    "daily_pipeline_summary",
    "daily_report",
    "weekly_report",
})
PROFILE_CRON_REGISTRATIONS = {
    MATRIX_PROFILE: SHARED_CRON_REGISTRATIONS | frozenset({
        "matrix_account_metrics", "matrix_works_scan", "matrix_works_refresh",
    }),
    TIKHUB_PROFILE: SHARED_CRON_REGISTRATIONS | frozenset({
        "tikhub_account_metrics", "tikhub_works_scan", "tikhub_works_refresh",
    }),
}
PAID_RECONCILE_REGISTRATIONS = frozenset({
    "matrix_account_metrics",
    "matrix_works_scan",
    "matrix_works_refresh",
    "tikhub_account_metrics",
    "tikhub_works_scan",
    "tikhub_works_refresh",
    "tikhub_reconcile",
    "metrics_backfill",
    "metrics_backfill_close",
    "metrics_backfill_established",
})
# Integrated workers own paid capture; legacy cron keeps only report/control
# rounds. Keep the explicit profile key so unknown profiles still fail closed.
PROFILE_CRON_REGISTRATIONS["integrated_route_v1"] = (
    SHARED_CRON_REGISTRATIONS - PAID_RECONCILE_REGISTRATIONS
)
PAID_RECONCILE_CUTOFF = time(20, 0)
DISPATCH_DEFERRED_JOB = "pipeline_dispatch_deferred"
DISPATCH_DEFERRED_CONTRACT = "pipeline-dispatch-deferred-v1"
PAID_DISPATCH_JOBS = frozenset({
    "matrix_works_scan",
    "matrix_account_metrics",
    "tikhub_account_metrics",
    "tikhub_works_scan",
    "tikhub_reconcile",
    "metrics_backfill",
    "content_pipeline",
    "comments_refresh",
    "history_recovery",
})
PIPELINE_RECOVERY_DELAYS = (0, 5, 10, 20, 40, 60)
PIPELINE_TERMINAL_STATUSES = frozenset({
    "succeeded", "partial", "failed", "skipped", "interrupted",
})
TIKHUB_DISCOVERY_WORKERS = 2
SCHEDULER_CONTROL_EXECUTOR = "control"
SCHEDULER_REPORT_EXECUTOR = "report"
SCHEDULER_RECONCILE_EXECUTOR = "reconcile"
CONTROL_CRON_REGISTRATIONS = frozenset({
    "daily_pipeline_summary", "daily_report", "weekly_report",
})


class PipelineFinalizationError(RuntimeError):
    def __init__(self, run_id: int, attempt_id: int, stage: str):
        self.run_id = run_id
        self.attempt_id = attempt_id
        self.stage = stage
        super().__init__(
            f"pipeline round {run_id} attempt {attempt_id} could not finalize at {stage}"
        )


def _error_reason(error: BaseException) -> str:
    return str(
        getattr(error, "error_code", None)
        or getattr(error, "code", None)
        or type(error).__name__
    )


def _iso(value: datetime) -> str:
    return value.astimezone(timezone.utc).isoformat(timespec="seconds").replace("+00:00", "Z")


def _scheduled_round_at(registration_id: str, local_now: datetime) -> datetime | None:
    """Return the latest due slot for one registration on the current Beijing day."""

    _job_id, hours, minute, weekday = CRON_ROUNDS[registration_id]
    local = local_now.astimezone(BEIJING)
    if weekday == "mon" and local.weekday() != 0:
        return None
    hour = max(
        (int(value) for value in hours.split(",") if (int(value), minute) <= (local.hour, local.minute)),
        default=-1,
    )
    if hour < 0:
        return None
    return datetime.combine(local.date(), time(hour, minute), BEIJING)


def _record_dispatch_deferred(
    connection: sqlite3.Connection,
    *,
    active: Mapping[str, Any],
    job_id: str,
    registration_id: str,
    due_slot: str,
    at: str,
    blocked_reason: str = "paid_dispatch_blocked",
) -> dict[str, Any]:
    """Write one immutable deferral inside the caller's gate transaction."""

    if not connection.in_transaction:
        raise durable_runs.DurableRunError(
            "dispatch deferred receipt requires a caller transaction"
        )
    state = paid_dispatch_state(connection, at=at)
    if state.paid_dispatch_open:
        # A failed runtime/provider authority is not a profile-switch drain.
        # Never append an impossible open drain receipt or mutate an old one.
        return {"status": "skipped", "complete": False, "reason": blocked_reason,
                "deferred_reason": "paid_authority_blocked"}
    drain_id = state.drain_id or "invalid-paid-drain-chain"
    business_day = parse_time(due_slot).astimezone(BEIJING).date().isoformat()
    if registration_id in {"content_pipeline", "comments_refresh"}:
        # An interval wakeup is not a new business scope while the same drain
        # still owns the day. Preserve cron due-slot identities unchanged.
        due_slot = _iso(datetime.combine(date.fromisoformat(business_day), time.min, BEIJING))
    identity = {
        "drain_id": drain_id,
        "activation_id": int(active["activation_run_id"]),
        "business_day": business_day,
        "registration_id": registration_id,
        "due_slot": due_slot,
    }
    details = {
        "contract_version": DISPATCH_DEFERRED_CONTRACT,
        "complete": False,
        "status": "dispatch_deferred",
        "reason": "dispatch_deferred",
        "deferred_reason": "profile_switch_drain",
        "job_id": job_id,
        "identity": identity,
        "drain_state": state.state,
    }
    encoded = json.dumps(
        details,
        ensure_ascii=False,
        sort_keys=True,
        separators=(",", ":"),
        allow_nan=False,
    )
    scheduled_for = "deferred:" + durable_runs.scan_identity(
        DISPATCH_DEFERRED_JOB, identity,
    )
    row = connection.execute(
        "SELECT id,status,details_json FROM scheduler_runs "
        "WHERE job_id=? AND scheduled_for=?" + durable_runs.root_run_predicate(connection),
        (DISPATCH_DEFERRED_JOB, scheduled_for),
    ).fetchone()
    if row is None:
        cursor = connection.execute(
            "INSERT INTO scheduler_runs(job_id,scheduled_for,status,started_at,"
            "completed_at,details_json) VALUES (?,?,'skipped',?,?,?)",
            (DISPATCH_DEFERRED_JOB, scheduled_for, at, at, encoded),
        )
        run_id = int(cursor.lastrowid or 0)
        attempt = connection.execute(
            "INSERT INTO scheduler_run_attempts(scheduler_run_id,attempt_number,"
            "invocation_source,status,started_at,completed_at,details_json) "
            "VALUES (?,1,'scheduled','skipped',?,?,?)",
            (run_id, at, at, encoded),
        )
        attempt_id = int(attempt.lastrowid or 0)
    else:
        observed = json.loads(row["details_json"])
        if observed.get("drain_state") not in {"draining", "sealed", "closed", "invalid"}:
            raise durable_runs.DurableRunError("dispatch deferred receipt drain state is invalid")
        # drain_state describes the first observation, not a mutable identity
        # field. START -> SEALED remains blocked and must preserve that receipt.
        details["drain_state"] = observed["drain_state"]
        encoded = json.dumps(details, ensure_ascii=False, sort_keys=True,
                             separators=(",", ":"), allow_nan=False)
        if row["status"] != "skipped" or row["details_json"] != encoded:
            raise durable_runs.DurableRunError(
                "dispatch deferred receipt identity changed"
            )
        attempts = connection.execute(
            "SELECT id,status,details_json FROM scheduler_run_attempts "
            "WHERE scheduler_run_id=? ORDER BY attempt_number",
            (row["id"],),
        ).fetchall()
        if (
            len(attempts) != 1
            or attempts[0]["status"] != "skipped"
            or attempts[0]["details_json"] != encoded
        ):
            raise durable_runs.DurableRunError(
                "dispatch deferred attempt is not one-shot terminal"
            )
        run_id = int(row["id"])
        attempt_id = int(attempts[0]["id"])
    return {
        "status": "skipped",
        "complete": False,
        "reason": "dispatch_deferred",
        "deferred_reason": "profile_switch_drain",
        "deferred_run_id": run_id,
        "deferred_attempt_id": attempt_id,
        **identity,
    }


def _require_job_dispatch_open(
    connection: sqlite3.Connection, *, provider: str, job_id: str, at: str,
) -> None:
    """Gate scheduler ownership, not a provider send or operation qualification.

    Schema20 operation evidence is evaluated by the actual request's A/B gates.
    A job may own work while its provider is blocked or collecting a fixed
    diagnostic cohort; treating that job name as an operation prevents either
    path from ever obtaining its natural owner.
    """
    if connection.execute("PRAGMA user_version").fetchone()[0] not in {20, 21}:
        require_paid_dispatch_open(connection, provider=provider, operation=job_id, at=at)
        return
    if not connection.in_transaction:
        raise durable_runs.DurableRunError("job dispatch gate requires a caller transaction")
    state = paid_dispatch_state(connection, at=at)
    if not state.paid_dispatch_open:
        raise PaidDrainBlocked(
            f"paid dispatch is {state.state}: {state.reason or state.drain_id}",
            drain_id=state.drain_id,
            error_code=("roster_not_ready" if state.reason == "no acquisition profile is active"
                        else "current_activation_hold_missing" if state.reason == "current_activation_hold_missing"
                        else None),
        )


def _dispatch_deferred_receipt(
    *,
    db_path: Path,
    active: Mapping[str, Any],
    job_id: str,
    registration_id: str,
    due_slot: str,
    at: str,
) -> dict[str, Any] | None:
    """Gate paid work and write one terminal deferral per frozen due slot."""

    provider = (
        "newrank_matrix"
        if job_id in {"matrix_works_scan", "matrix_account_metrics"}
        else "TikHub"
    )
    with transaction_metrics_context(
        job_id="pipeline_paid_dispatch_gate",
        registration_id=registration_id,
    ), connect(db_path) as connection, transaction(connection):
        try:
            _require_job_dispatch_open(
                connection,
                provider=provider,
                job_id=job_id,
                at=at,
            )
        except PaidDrainBlocked as error:
            return _record_dispatch_deferred(
                connection,
                active=active,
                job_id=job_id,
                registration_id=registration_id,
                due_slot=due_slot,
                at=at,
                blocked_reason=_error_reason(error),
            )
    return None


def _claim_paid_run_or_defer(
    job_id: str,
    identity: Mapping[str, Any],
    *,
    db_path: Path,
    active: Mapping[str, Any] | None,
    registration_id: str,
    due_slot: str,
    at: str,
    initial_checkpoint: Mapping[str, Any] | None = None,
    scope_key: Mapping[str, Any] | None = None,
) -> tuple[durable_runs.DurableClaim | None, dict[str, Any] | None]:
    """Linearize the drain gate with creation/resume of one paid run."""

    provider = (
        "newrank_matrix"
        if identity.get("job_id") in {"matrix_works_scan", "matrix_account_metrics"}
        or job_id in {"matrix_works_scan", "matrix_account_metrics"}
        else "TikHub"
    )
    with transaction_metrics_context(
        job_id=job_id,
        registration_id=registration_id,
    ), connect(db_path) as connection, transaction(connection):
        try:
            _require_job_dispatch_open(
                connection,
                provider=provider,
                job_id=job_id,
                at=at,
            )
        except PaidDrainBlocked as error:
            if active is None:
                return None, {
                    "status": "skipped",
                    "complete": False,
                    "reason": "dispatch_deferred",
                    "deferred_reason": "profile_switch_drain",
                }
            return None, _record_dispatch_deferred(
                connection,
                active=active,
                job_id=str(identity.get("job_id", job_id)),
                registration_id=registration_id,
                due_slot=due_slot,
                at=at,
                blocked_reason=_error_reason(error),
            )
        return durable_runs.claim_run_in_transaction(
            connection,
            job_id,
            identity,
            initial_checkpoint=initial_checkpoint,
            now=at,
            scope_key=scope_key,
        ), None


def _is_current_beijing_timestamp(value: object, *, day_start: datetime, day_end: datetime) -> bool:
    try:
        parsed = parse_time(str(value))
    except (AttributeError, TypeError, ValueError):
        return False
    return day_start <= parsed.astimezone(BEIJING) < day_end


def _resume_scope_is_current(job_id: str, identity: Mapping[str, Any], *, at: str) -> bool:
    """Fence provider and parent resumes to the current Beijing business day."""

    if job_id in {"history_recovery", "history_scan_catalog"}:
        return False
    if not _scope_is_after_automatic_start(job_id, identity):
        return False
    local = parse_time(at).astimezone(BEIJING)
    day_start = datetime.combine(local.date(), time.min, BEIJING)
    day_end = day_start + timedelta(days=1)
    provider = str(identity.get("provider", "")).lower()
    if provider == "newrank_matrix":
        kind = identity.get("kind")
        if kind == "accounts":
            try:
                return date.fromisoformat(str(identity.get("rank_date"))) == local.date() - timedelta(days=1)
            except (AttributeError, TypeError, ValueError):
                return False
        if kind == "works":
            return _is_current_beijing_timestamp(
                identity.get("overall_end_at"), day_start=day_start, day_end=day_end,
            )
        return False
    if provider == "tikhub":
        return _is_current_beijing_timestamp(
            identity.get("window_end"), day_start=day_start, day_end=day_end,
        )
    if job_id.startswith("pipeline_round:"):
        return identity.get("beijing_day") == local.date().isoformat()
    return True


def _scope_is_after_automatic_start(job_id: str, identity: Mapping[str, Any]) -> bool:
    if automatic_from_date() is None:
        return True
    if job_id in {"history_recovery", "history_scan_catalog"}:
        return False
    provider = str(identity.get("provider", "")).lower()
    if provider == "tikhub":
        return within_automatic_scope(identity.get("window_start"))
    if provider == "newrank_matrix":
        return within_automatic_scope(identity.get("rank_date") if identity.get("kind") == "accounts"
                                      else identity.get("overall_start_at", identity.get("start_at")))
    if job_id.startswith("pipeline_round:"):
        return within_automatic_scope(identity.get("beijing_day"))
    if job_id in QUEUE_KINDS:
        return within_automatic_scope(identity.get("created_for"))
    return True


def _fair_platform_order(
    items: Sequence[Any], platform_for: Callable[[Any], object],
) -> list[Any]:
    """Alternate the two TikHub platforms without moving other queue slots.

    Callers pass items in oldest-due order. The platform whose oldest head
    appears first starts the round; the platform name is the stable tie-break.
    """
    ordered = list(items)
    groups: dict[str, list[Any]] = {}
    first_slots: dict[str, int] = {}
    platform_slots: list[int] = []
    for slot, item in enumerate(ordered):
        platform = str(platform_for(item) or "").lower()
        if platform not in {"douyin", "xiaohongshu"}:
            continue
        groups.setdefault(platform, []).append(item)
        first_slots.setdefault(platform, slot)
        platform_slots.append(slot)
    if len(groups) < 2:
        return ordered

    platforms = sorted(groups, key=lambda value: (first_slots[value], value))
    offsets = dict.fromkeys(platforms, 0)
    alternated: list[Any] = []
    while len(alternated) < len(platform_slots):
        for platform in platforms:
            offset = offsets[platform]
            if offset < len(groups[platform]):
                alternated.append(groups[platform][offset])
                offsets[platform] = offset + 1
    for slot, item in zip(platform_slots, alternated, strict=True):
        ordered[slot] = item
    return ordered


def _tikhub_child_observation(
    scope: Mapping[str, Any], *, db_path: Path,
) -> tuple[int, str, int | None] | None:
    scheduled_for = "scan:" + durable_runs.scan_identity("tikhub_reconcile", scope)
    with connect(db_path) as connection:
        row = connection.execute(
            "SELECT id,status,details_json FROM scheduler_runs "
            "WHERE job_id='tikhub_reconcile' AND scheduled_for=?",
            (scheduled_for,),
        ).fetchone()
        if row is None:
            return None
        details = json.loads(row["details_json"])
        if (
            details.get("contract_version") != durable_runs.CONTRACT_VERSION
            or details.get("identity") != dict(scope)
        ):
            raise durable_runs.DurableRunError("TikHub child identity changed")
        attempts = connection.execute(
            "SELECT id FROM scheduler_run_attempts "
            "WHERE scheduler_run_id=? AND status='running' ORDER BY id",
            (row["id"],),
        ).fetchall()
    if len(attempts) > 1:
        raise durable_runs.DurableRunError("TikHub child has multiple running attempts")
    return (
        int(row["id"]),
        str(row["status"]),
        int(attempts[0]["id"]) if attempts else None,
    )


def _recover_tikhub_child_after_worker_error(
    scope: Mapping[str, Any],
    before: tuple[int, str, int | None] | None,
    error: Exception,
    *,
    db_path: Path,
    at: str,
) -> dict[str, Any] | None:
    """Fence only a newly claimed child attempt and make it safely resumable."""

    after = _tikhub_child_observation(scope, db_path=db_path)
    if after is None:
        return None
    run_id, status, attempt_id = after
    if status != "running" or type(attempt_id) is not int:
        return None
    if before is not None and (
        before[0] != run_id or before[1] == "running" or before[2] == attempt_id
    ):
        return None
    if not durable_runs.recover_run(
        run_id,
        expected_attempt_id=attempt_id,
        db_path=db_path,
        reason="tikhub_discovery_worker_exception",
        now=at,
    ):
        return None
    recovered = _tikhub_child_observation(scope, db_path=db_path)
    if recovered != (run_id, "interrupted", None):
        raise durable_runs.DurableRunError("TikHub child recovery did not close its attempt")
    return {
        "scheduler_run_id": run_id,
        "status": "interrupted",
        "complete": False,
        "reason": _error_reason(error),
        "recovered_after_worker_error": True,
    }


def _run_tikhub_discovery_workers(
    members: Sequence[Mapping[str, Any]],
    run_member: Callable[[Mapping[str, Any]], Mapping[str, Any]],
) -> list[dict[str, Any]]:
    """Run the fair discovery queue with two bounded network workers.

    Futures are consumed in dispatch order so the durable parent receipt remains
    deterministic. Each worker owns one account-page invocation; SQLite writes
    stay serialized by ``storage.transaction`` and no transaction spans the
    executor.
    """

    ordered = list(members)
    if not ordered:
        return []

    def invoke(member: Mapping[str, Any]) -> dict[str, Any]:
        # Preserve the former sequential contract: an exception aborts this
        # parent dispatch so it will replay the deterministic child identities.
        # Returning an anonymous partial here would let the next parent attempt
        # forget a child whose durable run was created just before the error.
        return dict(run_member(member))

    with ThreadPoolExecutor(
        max_workers=min(TIKHUB_DISCOVERY_WORKERS, len(ordered)),
        thread_name_prefix="tikhub-discovery",
    ) as executor:
        # A paid scope is a ContextVar. Threads do not inherit it implicitly,
        # so each submitted account must receive its own copy of the caller's
        # frozen round/budget ownership context.
        futures = [executor.submit(copy_context().run, invoke, member) for member in ordered]
        return [future.result() for future in futures]


def _identity_platforms(db_path: Path, identity_ids: Sequence[int]) -> dict[int, str]:
    values = sorted({value for value in identity_ids if type(value) is int and value > 0})
    if not values:
        return {}
    placeholders = ",".join("?" for _ in values)
    with connect(db_path) as connection:
        return {
            int(row["id"]): str(row["platform"])
            for row in connection.execute(
                f"SELECT id,platform FROM account_platform_identities WHERE id IN ({placeholders})",
                values,
            )
        }


def _is_sqlite_busy_or_locked(error: BaseException) -> bool:
    if not isinstance(error, sqlite3.Error):
        return False
    code = getattr(error, "sqlite_errorcode", None)
    if isinstance(code, int):
        return (code & 0xFF) in {
            getattr(sqlite3, "SQLITE_BUSY", 5),
            getattr(sqlite3, "SQLITE_LOCKED", 6),
        }
    message = str(error).lower()
    return (
        "database is locked" in message
        or "database table is locked" in message
        or "database schema is locked" in message
        or "database is busy" in message
    )


def _pipeline_round_state(
    run_id: int, attempt_id: int, *, db_path: Path,
) -> tuple[dict[str, Any], int | None, str | None]:
    with connect(db_path) as connection:
        row = connection.execute(
            "SELECT r.id,r.status,r.details_json,active.id AS active_attempt_id,"
            "expected.status AS expected_attempt_status "
            "FROM scheduler_runs r "
            "LEFT JOIN scheduler_run_attempts active "
            "ON active.scheduler_run_id=r.id AND active.status='running' "
            "LEFT JOIN scheduler_run_attempts expected "
            "ON expected.scheduler_run_id=r.id AND expected.id=? "
            "WHERE r.id=?",
            (attempt_id, run_id),
        ).fetchone()
    if row is None or row["expected_attempt_status"] is None:
        raise PipelineFinalizationError(run_id, attempt_id, "state_read")
    result = dict(row)
    result["details"] = json.loads(result["details_json"])
    active_attempt_id = result.pop("active_attempt_id")
    expected_attempt_status = str(result.pop("expected_attempt_status"))
    return (
        result,
        int(active_attempt_id) if active_attempt_id is not None else None,
        expected_attempt_status,
    )


def _recover_pipeline_round_attempt(
    claim: durable_runs.DurableClaim,
    *,
    db_path: Path,
    stage: str,
) -> dict[str, Any]:
    """Fence one failed parent attempt; only the recovery transaction is retried."""

    delays = (0,) if current_reconcile_budget() is not None else PIPELINE_RECOVERY_DELAYS
    for delay in delays:
        if delay:
            sleep(delay)
        try:
            recovered = durable_runs.recover_run(
                claim.scheduler_run_id,
                expected_attempt_id=claim.attempt_id,
                db_path=db_path,
                reason="pipeline_dispatch_finalize_failed",
            )
        except Exception as error:
            if _is_sqlite_busy_or_locked(error):
                continue
            raise
        row, active_attempt_id, attempt_status = _pipeline_round_state(
            claim.scheduler_run_id, claim.attempt_id, db_path=db_path,
        )
        if recovered:
            if (
                row["status"] != "interrupted"
                or active_attempt_id is not None
                or attempt_status != "interrupted"
            ):
                raise PipelineFinalizationError(
                    claim.scheduler_run_id, claim.attempt_id, "recovery_invariant",
                )
            return {
                "status": "interrupted",
                "complete": False,
                "round_run_id": claim.scheduler_run_id,
                "reason": "pipeline_dispatch_finalize_failed",
            }

        if row["status"] != "running":
            if row["status"] not in PIPELINE_TERMINAL_STATUSES:
                raise PipelineFinalizationError(
                    claim.scheduler_run_id, claim.attempt_id, "unsupported_status",
                )
            if active_attempt_id is not None or attempt_status == "running":
                raise PipelineFinalizationError(
                    claim.scheduler_run_id, claim.attempt_id, "terminal_with_running_attempt",
                )
            details = row["details"]
            return {
                "status": row["status"],
                "complete": (
                    row["status"] == "succeeded"
                    and details.get("checkpoint", {}).get("complete") is True
                ),
                "round_run_id": claim.scheduler_run_id,
                "reason": "pipeline_dispatch_terminal_observed",
            }
        if active_attempt_id == claim.attempt_id:
            if attempt_status == "running":
                owner = row["details"].get("owner", {})
                if (
                    owner.get("attempt_id") != claim.attempt_id
                    or owner.get("token") != claim.owner_token
                ):
                    raise durable_runs.LostOwnership(
                        "pipeline round owner fence changed"
                    )
                continue
            raise PipelineFinalizationError(
                claim.scheduler_run_id, claim.attempt_id, "owner_status_mismatch",
            )
        if active_attempt_id is not None:
            raise durable_runs.LostOwnership("pipeline round active attempt changed")
        raise PipelineFinalizationError(
            claim.scheduler_run_id, claim.attempt_id, "running_without_active_attempt",
        )
    raise PipelineFinalizationError(claim.scheduler_run_id, claim.attempt_id, stage)


def _finalize_pipeline_round(
    claim: durable_runs.DurableClaim,
    *,
    db_path: Path,
    timestamp: str,
    status: str,
    checkpoint_state: Mapping[str, Any] | None,
    summary: Mapping[str, Any],
) -> dict[str, Any]:
    """Checkpoint once, finish once, then recover the parent through its owner fence."""

    stage = "checkpoint" if checkpoint_state is not None else "finish"
    try:
        if checkpoint_state is not None:
            with connect(db_path) as connection, transaction(connection):
                durable_runs.checkpoint(connection, claim, checkpoint_state, now=timestamp)
            stage = "finish"
        durable_runs.finish_run(
            claim,
            status=status,
            db_path=db_path,
            now=timestamp,
            next_resume_at=(
                _iso(parse_time(timestamp) + timedelta(minutes=5))
                if status == "partial" else None
            ),
            summary=summary,
        )
    except Exception as error:
        recovered = _recover_pipeline_round_attempt(
            claim, db_path=db_path, stage=stage,
        )
        if _is_sqlite_busy_or_locked(error):
            return recovered
        raise
    return {
        "status": status,
        "complete": status == "succeeded",
        "round_run_id": claim.scheduler_run_id,
    }


def _control_round_pending(
    job: str, scope_key: Mapping[str, Any], *, db_path: Path,
) -> dict[str, Any] | None:
    """Do not repeatedly claim unchanged queued work on the control executor."""
    if current_reconcile_budget() is None:
        return None
    with connect(db_path) as connection:
        row = connection.execute(
            "SELECT id,status,details_json FROM scheduler_runs "
            "WHERE job_id=? AND scheduled_for=?" + durable_runs.root_run_predicate(connection),
            (job, "scan:" + durable_runs.scan_identity(job, scope_key)),
        ).fetchone()
    if row is None:
        return None
    details = json.loads(row["details_json"])
    cp = details.get("checkpoint", {})
    if row["status"] == "interrupted" and cp.get("complete") is True:
        return None  # A terminal checkpoint still needs its owner-fenced receipt.
    children = cp.get("child_run_ids", [])
    ready = (
        cp.get("started") is True and bool(children)
        and not cp.get("remaining_profiles")
        and all(durable_runs.get_run(cid, db_path=db_path)["details"].get("complete")
                for cid in children)
    )
    if ready and row["status"] in {"partial", "interrupted"}:
        return None
    complete = details.get("complete") is True
    return {
        "round_run_id": int(row["id"]), "status": row["status"],
        "complete": complete,
        "reason": "round_already_claimed_or_finished" if complete else "execution_queued",
    }


def _reconcile_current_day_rounds(
    *,
    at: str,
    db_path: Path,
    reports_root: Path,
    call_override=None,
    matrix_client=None,
) -> list[dict[str, Any]]:
    """Run one max-due slot per registration, ordered by its planned Beijing time."""

    local = parse_time(at).astimezone(BEIJING)
    tie_priority = {
        "matrix_account_metrics": 0,
        "tikhub_account_metrics": 0,
        "matrix_works_refresh": 1,
        "tikhub_works_refresh": 1,
    }
    candidates = []
    for registration_id, (job_id, _hours, _minute, _weekday) in CRON_ROUNDS.items():
        scheduled = _scheduled_round_at(registration_id, local)
        if scheduled is not None:
            candidates.append((scheduled, tie_priority.get(registration_id, 2), registration_id, job_id))
    candidates.sort(key=lambda item: (item[0], item[1], item[2]))

    results: list[dict[str, Any]] = []
    for scheduled, _priority, registration_id, job_id in candidates:
        budget = current_reconcile_budget()
        if budget is not None and not budget.take():
            break
        planned = _iso(scheduled)
        with connect(db_path) as connection:
            slot_active = activation(connection, at=planned)
        slot_profile = str(
            slot_active.get("profile_id", MATRIX_PROFILE)
            if slot_active is not None else MATRIX_PROFILE
        )
        if (
            slot_active is not None
            and registration_id not in PROFILE_CRON_REGISTRATIONS[slot_profile]
        ):
            results.append({
                "registration_id": registration_id,
                "job_id": job_id,
                "scheduled_at": planned,
                "status": "skipped",
                "complete": True,
                "reason": "profile_not_scheduled",
                "profile_id": slot_profile,
            })
            continue
        if budget is not None and job_id in {"daily_report", "weekly_report"}:
            results.append({
                "registration_id": registration_id, "job_id": job_id,
                "scheduled_at": planned, "status": "skipped", "complete": False,
                "reason": "report_executor_owned",
            })
            continue
        if local.time() >= PAID_RECONCILE_CUTOFF and registration_id in PAID_RECONCILE_REGISTRATIONS:
            results.append({
                "registration_id": registration_id,
                "job_id": job_id,
                "scheduled_at": planned,
                "status": "skipped",
                "complete": False,
                "reason": "paid_round_reconcile_cutoff",
            })
            continue
        try:
            result = dispatch(
                job_id,
                registration_id=registration_id,
                db_path=db_path,
                reports_root=reports_root,
                at=at,
                call_override=call_override,
                matrix_client=matrix_client,
            )
            results.append({
                "registration_id": registration_id,
                "job_id": job_id,
                "scheduled_at": planned,
                **result,
            })
        except Exception as error:
            results.append({
                "registration_id": registration_id,
                "job_id": job_id,
                "scheduled_at": planned,
                "status": "partial",
                "complete": False,
                "reason": _error_reason(error),
            })
    return results


def _fingerprint_ready(connection, content_id: int) -> bool:
    try:
        _source, source_sha = _current_source_state(connection, content_id)
    except LifecycleError as error:
        if error.error_code != "managed_source_pending":
            raise
        return False  # A new source awaits enrollment; never fall back to an old bundle.
    return connection.execute(
        "SELECT 1 FROM duplicate_fingerprints WHERE content_id=? AND fingerprint_version=? AND source_sha256=?",
        (content_id, FINGERPRINT_VERSION, source_sha),
    ).fetchone() is not None


def _media_work_blockers(connection, content_ids: Sequence[int], *, states=None) -> dict[str, Any]:
    if states is None:
        release = connection.execute("SELECT id FROM evaluation_releases WHERE status='active'").fetchone()
        states = media_terminal_state_details(connection, release["id"], content_ids) if release else {}
    blocked = {}
    for cid in content_ids:
        state = states.get(cid)
        if state is None or state.reason not in MEDIA_BLOCKED_REASONS:
            continue
        bundle = current_bundle(connection, cid)
        blocked[str(cid)] = {"reason": state.reason, "bundle_id": bundle["bundle_id"] if bundle else None}
    return blocked


def _request_local_restores(blocked: Mapping[str, Any], *, db_path: Path) -> dict[str, Any]:
    """Only enqueue a local prerequisite. No download, supplier slot or retry."""
    from .media_retention import request_restore

    result = {cid: dict(value) for cid, value in blocked.items()}
    for cid, value in result.items():
        if value["reason"] != "restore_required" or not value.get("bundle_id"):
            continue
        try:
            with connect(db_path) as connection:
                bundle = current_bundle(connection, int(cid))
            if bundle is None or bundle["bundle_id"] != value["bundle_id"]:
                value["restore_error"] = "restore_source_mismatch"
                continue
            request = bundle["state"].get("restore_request")
            # A completed durable enqueue is reused; a crash gap without run_id
            # is settled by request_restore's idempotent queue publication.
            if not request or request.get("status") != "pending" or not request.get("run_id"):
                request = request_restore(int(cid), value["bundle_id"], "reprocess", db_path=db_path)
            value["restore_run_id"] = request["run_id"]
        except Exception as error:
            value["restore_error"] = _error_reason(error)
    return result


#: Consecutive whole-batch local failures on one round before an alert opens.
LOCAL_BATCH_ALERT_FAILURES = 3


def _open_local_batch_alert(connection: sqlite3.Connection, *, claim: Any, kind: str, reason: str,
                            failures: int, at: str) -> None:
    """Surface a round whose local batch keeps raising; schema20 alerts only."""
    if connection.execute(
        "SELECT 1 FROM sqlite_master WHERE type='table' AND name='operational_alerts'"
    ).fetchone() is None:
        return
    connection.execute(
        "INSERT OR IGNORE INTO operational_alerts(dedupe_key,severity,scope_json,evidence_json,owner,status,opened_at) "
        "VALUES(?,'P1',?,?,'content-pipeline','open',?)",
        (f"content-pipeline-local:{claim.scan_id}",
         json.dumps({"kind": kind, "scheduler_run_id": claim.scheduler_run_id, "scan_id": claim.scan_id}, sort_keys=True),
         json.dumps({"reason": reason, "consecutive_failures": failures}, sort_keys=True), at))


def _supports_profile_activations(connection: sqlite3.Connection) -> bool:
    return connection.execute(
        "SELECT 1 FROM sqlite_master "
        "WHERE type='table' AND name='acquisition_profile_activations'"
    ).fetchone() is not None


def activation(
    connection: sqlite3.Connection, *, at: str | None = None,
) -> dict[str, Any] | None:
    """Return the effective runtime activation.

    Schema 19 is authoritative and never consults the retired scheduler-run
    activation.  The latter remains readable only while opening a schema-18
    database for paired migration/rollback compatibility.
    """
    if _supports_profile_activations(connection):
        from .profile_activations import activation_at

        value = activation_at(connection, at or now_utc())
        if value is None:
            return None
        return {
            **value,
            "mode": "active",
            "activation_run_id": int(value["activation_id"]),
            "cutover_at": value["effective_at"],
            "roster_snapshot_hash": value["roster_members_sha256"],
        }
    row = connection.execute(
        "SELECT id,details_json FROM scheduler_runs WHERE job_id=? AND status='succeeded' ORDER BY id DESC LIMIT 1",
        (ACTIVATION_JOB,),
    ).fetchone()
    if row is None:
        return None
    value = json.loads(row["details_json"])
    if value.get("contract_version") != PIPELINE_VERSION:
        return None
    return {**value, "activation_run_id": row["id"]}


def _acquisition_activation_id(
    connection: sqlite3.Connection, at: str
) -> int | None:
    """Resolve the schema-19 runtime activation; schema 18 has no such table."""
    table = connection.execute(
        "SELECT 1 FROM sqlite_master "
        "WHERE type='table' AND name='acquisition_profile_activations'"
    ).fetchone()
    if table is None:
        return None
    from .profile_activations import activation_at

    active = activation_at(connection, at)
    if active is None:
        raise RosterError(
            "roster_activation_required",
            "Paid work requires an effective acquisition activation",
        )
    return int(active["activation_id"])


def _scope(
    db_path: Path,
    frozen_roster: Mapping[str, Any] | None = None,
    *,
    at: str | None = None,
) -> tuple[dict[str, Any], list[dict[str, Any]]]:
    snapshot: dict[str, Any] | None
    with connect(db_path) as connection:
        if _supports_profile_activations(connection):
            if frozen_roster is None:
                active = activation(connection, at=at)
                if active is None:
                    raise RosterError(
                        "roster_activation_required",
                        "An effective acquisition activation is required",
                    )
            else:
                activation_id = frozen_roster.get("activation_id")
                if type(activation_id) is not int:
                    raise RosterError(
                        "roster_activation_required",
                        "Frozen work must bind an acquisition activation",
                    )
                from .profile_activations import activation_by_id

                active = activation_by_id(connection, activation_id)
                if active.get("cancellation") is not None:
                    raise RosterError(
                        "profile_superseded", "Frozen acquisition activation was cancelled"
                    )
                for field, expected in (
                    ("profile_id", active["profile_id"]),
                    ("roster_snapshot_id", active["roster_snapshot_id"]),
                    ("roster_snapshot_hash", active["roster_members_sha256"]),
                ):
                    if frozen_roster.get(field) != expected:
                        raise RosterError(
                            "roster_scope_mismatch",
                            "Frozen acquisition scope differs from its activation",
                        )
            snapshot = runtime_snapshot(connection, active)
        elif frozen_roster is None:
            snapshot = current_snapshot(connection)
        else:
            row = connection.execute("SELECT * FROM account_roster_snapshots WHERE id=?", (frozen_roster["roster_snapshot_id"],)).fetchone()
            snapshot = dict(row) if row else None
            if snapshot is None or snapshot["members_sha256"] != frozen_roster["roster_snapshot_hash"]:
                raise RosterError("roster_scope_mismatch", "Frozen roster no longer matches its accepted evidence")
        if snapshot is None:
            raise RosterError("roster_not_ready", "A complete accepted roster is required before queue creation")
        members = get_current_members(connection, snapshot_id=snapshot["id"], enabled_only=True)
        if frozen_roster is not None and "eligible_identity_ids" in frozen_roster:
            allowed = set(frozen_roster["eligible_identity_ids"])
            members = [member for member in members if member["identity_id"] in allowed]
        return snapshot, members


def _activation_is_current(
    connection: sqlite3.Connection,
    frozen: Mapping[str, Any],
    *,
    at: str,
) -> bool:
    """Check the execution-time epoch without changing the frozen due scope."""
    if not _supports_profile_activations(connection):
        return True
    activation_id = frozen.get("activation_id")
    if type(activation_id) is not int:
        return False
    effective = activation(connection, at=at)
    return effective is not None and int(effective["activation_id"]) == activation_id


def _activation_identity(active: Mapping[str, Any]) -> dict[str, Any]:
    if "activation_id" not in active:
        return {}
    return {
        "activation_id": int(active["activation_id"]),
        "activation_sha256": str(active["activation_sha256"]),
        "profile_id": str(active["profile_id"]),
    }


def _queue_candidates(kind: str, *, at: str, db_path: Path,
                      age_band: str = "all", include_claimed: bool = False,
                      frozen_roster: Mapping[str, Any] | None = None,
                      blocked_media: dict[str, Any] | None = None) -> list[dict[str, Any]]:
    snapshot, _members = _scope(db_path, frozen_roster, at=at)
    day = parse_time(at).astimezone(BEIJING).date()
    week = day.isocalendar()
    week_key = f"{week.year}-W{week.week:02d}"
    floor = automatic_start_at()
    floor_iso = floor.isoformat() if floor is not None else None
    with connect(db_path) as connection:
        rows = connection.execute(
            "SELECT c.*,i.id identity_id FROM content_items c JOIN accounts a ON a.id=c.account_id "
            "JOIN account_platform_identities i ON i.account_id=a.id AND i.platform=c.platform "
            "JOIN account_roster_members m ON m.account_identity_id=i.id AND m.snapshot_id=? "
            "WHERE a.enabled=1 AND c.platform IN ('douyin','xiaohongshu') "
            "AND (? IS NULL OR julianday(c.published_at)>=julianday(?))",
            (snapshot["id"], floor_iso, floor_iso),
        ).fetchall()
        reserved_ids: set[int] = set()
        jobs = ("content_pipeline", "history_recovery") if kind in {"content_pipeline", "history_recovery"} else (kind,)
        for run in connection.execute("SELECT details_json FROM scheduler_runs WHERE job_id IN (%s) AND status IN ('running','partial','interrupted')" % ",".join("?" for _ in jobs), jobs):
            details = json.loads(run["details_json"])
            if (details.get("contract_version") == durable_runs.CONTRACT_VERSION
                    and _scope_is_after_automatic_start(str(details.get("identity", {}).get("kind", kind)), details.get("identity", {}))):
                reserved_ids.update(details.get("checkpoint", {}).get("pending_ids", []))
        release = connection.execute("SELECT id FROM evaluation_releases WHERE status='active'").fetchone()
        media_states = (media_terminal_state_details(connection, release["id"], [int(row["id"]) for row in rows])
                        if release is not None and kind in {"content_pipeline", "history_recovery"} else {})
        result = []
        for row in rows:
            if not within_automatic_scope(row["published_at"]):
                continue
            if frozen_roster is not None and "eligible_identity_ids" in frozen_roster and row["identity_id"] not in frozen_roster["eligible_identity_ids"]:
                continue
            content = dict(row)
            cid = int(row["id"])
            from .providers import STAGE_CONFIG

            stages = (["comments"] if kind == "comments_refresh" else
                      ["detail", "metrics"] if kind == "metrics_backfill" else
                      ["detail", "metrics", "comments"])
            if not legacy_queue_allowed(
                connection, account_id=int(row["account_id"]), content_id=cid,
                operations=[STAGE_CONFIG[(str(row["platform"]), stage)][2] for stage in stages], at=at,
            ):
                continue
            if cid in reserved_ids and not include_claimed:
                continue
            try:
                published_day = parse_time(row["published_at"]).astimezone(BEIJING).date() if row["published_at"] else None
                age = (day - published_day).days if published_day is not None else None
            except (TypeError, ValueError):
                age = None
            historical = str(row["source_group"] or "") == "history-backfill" or (age is not None and age > 30)
            content["historical"] = historical
            content["age"] = age
            if str(row["source_group"] or "") == "history-archive":
                continue
            if kind in {"content_pipeline", "history_recovery"} and age is not None and parse_time(row["published_at"]) < parse_time(HISTORY_START):
                continue  # Pre-recovery history is not an implicit new purchase.
            if historical and kind != "history_recovery":
                continue  # Automatic queues never spend on historical content.
            if kind == "history_recovery" and not historical:
                continue
            if kind == "metrics_backfill":
                if age is None or not 0 <= age <= 30:
                    continue
                # Due failed phases retain their original cycle on following
                # days; a budget block never advances last success.
                debt = connection.execute(
                    "SELECT window_key FROM fetch_slots WHERE content_id=? AND stage='metrics' "
                    "AND status IN ('pending','retryable_failed') AND window_key LIKE 'matrix-first:%' "
                    "ORDER BY created_at,id LIMIT 1", (cid,),
                ).fetchone()
                if not metric_refresh_due(cid, row["published_at"], as_of=at) and debt is None:
                    continue
                if not missing_metric_fields(connection, cid, at=at):
                    continue
                content["cycle_key"] = (str(debt["window_key"]).rsplit(":", 1)[0] if debt else
                                        metric_cycle_key(cid, row["published_at"], as_of=at))
                if debt is None and ((age_band == "recent" and age > 7)
                                     or (age_band == "established" and age <= 7)):
                    continue
                missing = set(missing_metric_fields(connection, cid, at=at))
                rules = load_policy()["metric_supplement_groups"][row["platform"]]
                unpaid = [rule for rule in rules if missing & set(rule["fields"]) and not connection.execute(
                    "SELECT 1 FROM fetch_slots WHERE content_id=? AND stage='metrics' AND window_key=? AND status='succeeded'",
                    (cid, f"{content['cycle_key']}:{rule['name']}"),
                ).fetchone()]
                if not unpaid:
                    continue  # Missing fields stay visible; do not rebuy a successful route/cycle.
            elif kind == "comments_refresh":
                if age is None or not 0 <= age <= 30:
                    continue
                runs = connection.execute("SELECT window_key,status FROM comment_capture_runs WHERE content_id=? ORDER BY created_at DESC,id DESC", (cid,)).fetchall()
                debt = next((item for item in reversed(runs) if item["status"] in {"pending", "retryable_failed", "running"}), None)
                current = next((item for item in runs if item["window_key"] == week_key), None)
                if debt is not None:
                    content["comment_window"] = debt["window_key"]
                    content["comment_as_of"] = date.fromisocalendar(int(debt["window_key"][:4]), int(debt["window_key"][6:]), 1).isoformat()
                    result.append(content)
                    continue
                if current is not None and current["status"] in {"succeeded", "terminal_failed", "running"}:
                    continue
                if current is None and runs and cid % 7 != day.weekday():
                    continue
                content["comment_window"], content["comment_as_of"] = week_key, day.isoformat()
            else:
                detail = connection.execute(
                    "SELECT status FROM fetch_slots WHERE content_id=? AND stage='detail' "
                    "AND window_key IN ('lifetime','xhs-type-probe-v1') "
                    "ORDER BY CASE window_key WHEN 'lifetime' THEN 0 ELSE 1 END LIMIT 1",
                    (cid,),
                ).fetchone()
                state = media_states.get(cid)
                if state is not None and state.reason in MEDIA_BLOCKED_REASONS:
                    if blocked_media is not None:
                        blocked_media.update(_media_work_blockers(connection, [cid], states=media_states))
                    continue
                has_fingerprint = _fingerprint_ready(connection, cid)
                if detail is not None and detail["status"] == "succeeded" and state is not None and state.state in {"complete", "terminal_insufficient", "terminal_failed"} and has_fingerprint:
                    continue
                if detail is not None and detail["status"] == "terminal_failed" and state is not None and state.state != "pending":
                    continue
            result.append(content)
    return sorted(result, key=lambda item: (bool(item["historical"]), item["age"] if item["age"] is not None else -1, int(item["id"])))


def allocate_candidates(candidates: Sequence[dict[str, Any]], limit: int) -> list[dict[str, Any]]:
    """Apply the stable bounded queue order without reserving historical seats."""
    if limit < 1 or limit > 500:
        raise ValueError("queue limit must be 1..500")
    return list(candidates[:limit])


def _cached_queue_slot(connection, content_id: int, stage: str, window: str, operation: str) -> bool:
    return connection.execute(
        "SELECT 1 FROM fetch_slots fs WHERE content_id=? AND stage=? AND window_key=? "
        "AND status='succeeded' AND (provider='legacy-cache' OR EXISTS ("
        "SELECT 1 FROM fetch_attempts fa JOIN provider_raw_responses pr ON pr.fetch_attempt_id=fa.id "
        "WHERE fa.slot_id=fs.id AND pr.operation=?))", (content_id, stage, window, operation),
    ).fetchone() is not None


def _candidate_work_targets(connection, kind: str, item: Mapping[str, Any], *, at: str) -> list[dict[str, Any]]:
    from . import providers
    from .comment_paging import page_window_key

    cid = int(item["id"])
    platform = str(item["platform"])
    category = "history" if kind == "history_recovery" or item.get("historical") else {
        "metrics_backfill": "metrics", "comments_refresh": "comments",
    }.get(kind, "detail")
    targets = []
    if kind == "metrics_backfill":
        missing = set(missing_metric_fields(connection, cid, at=at))
        cycle = item.get("cycle_key") or metric_cycle_key(cid, item["published_at"], as_of=at)
        for rule in load_policy()["metric_supplement_groups"][platform]:
            if not missing.intersection(rule["fields"]):
                continue
            targets.append({"stage": "metrics", "window_key": f"{cycle}:{rule['name']}",
                            "operation": providers.STAGE_CONFIG[(platform, rule["stage"])][2],
                            "group": rule["name"]})
    elif kind == "comments_refresh":
        operation = providers.STAGE_CONFIG[(platform, "comments")][2]
        week = str(item.get("comment_window") or date.fromisoformat(item["comment_as_of"]).strftime("%G-W%V"))
        row = connection.execute(
            "SELECT p.next_cursor_json,p.has_more FROM comment_capture_runs r "
            "JOIN comment_capture_pages p ON p.capture_run_id=r.id "
            "WHERE r.content_id=? AND r.window_key=? ORDER BY p.page_number DESC LIMIT 1",
            (cid, week),
        ).fetchone()
        if row is not None and not row["has_more"]:
            return []  # Local final folding only; no next provider cursor.
        cursor = json.loads(row["next_cursor_json"]) if row and row["next_cursor_json"] else None
        window = page_window_key(week, cursor)
        if row is None and _cached_queue_slot(connection, cid, "comments", week, operation):
            window = week  # Preserve the historical first-page cache fallback.
        if row is None and providers._zero_comment_metric_result(
            cid, metric_window_key=str(item["comment_as_of"]),
            db_path=Path(connection.execute("PRAGMA database_list").fetchone()[2]),
        ) is not None:
            return []
        targets.append({"stage": "comments", "window_key": window,
                        "operation": operation})
    else:
        operation = providers.STAGE_CONFIG[(platform, "detail")][2]
        probe_cached = platform == "xiaohongshu" and _cached_queue_slot(
            connection, cid, "detail", providers.XHS_TYPE_PROBE_WINDOW, operation,
        )
        if probe_cached and item.get("content_type") == "image":
            return []  # Probe replay and image lifetime derivation are local.
        window = (providers.XHS_TYPE_PROBE_WINDOW if platform == "xiaohongshu"
                  and item.get("content_type") not in {"image", "video"} else "lifetime")
        targets.append({"stage": "detail", "window_key": window,
                        "operation": operation})
    return [{**target, "category": category, "content_id": cid,
             "account_id": item.get("account_id"), "identity_id": item.get("identity_id"),
             "local_replay": _cached_queue_slot(connection, cid, target["stage"], target["window_key"], target["operation"])}
            for target in targets]


def _partition_queue_readiness(
    kind: str, candidates: Sequence[dict[str, Any]], *, at: str, db_path: Path,
    record: bool = True,
) -> tuple[list[dict[str, Any]], dict[str, Any]]:
    runnable = []
    blocked: dict[str, Any] = {}
    with connect(db_path) as connection:
        view = WorkReadinessPass(connection, at=at)
        media_prerequisites = (
            _media_work_blockers(connection, [int(item["id"]) for item in candidates])
            if kind in {"content_pipeline", "history_recovery"} and candidates else {}
        )
        for candidate in candidates:
            if str(candidate["id"]) in media_prerequisites:
                # Old claimed work must reach the established restore/terminal
                # path before an unrelated provider fault can freeze it.
                runnable.append(dict(candidate))
                continue
            if kind != "history_recovery" and candidate.get("historical"):
                runnable.append(dict(candidate))  # Finalize legacy disabled-history debt locally.
                continue
            targets = _candidate_work_targets(connection, kind, candidate, at=at)
            ready = []
            blocked_groups = []
            outstanding = 0
            for target in targets:
                if kind == "metrics_backfill" and target["local_replay"] and connection.execute(
                    "SELECT 1 FROM fetch_slots fs JOIN fetch_attempts fa ON fa.slot_id=fs.id "
                    "JOIN provider_raw_responses pr ON pr.fetch_attempt_id=fa.id "
                    "WHERE fs.content_id=? AND fs.stage=? AND fs.window_key=? "
                    "AND fs.status='succeeded' AND pr.operation=? AND pr.source IN ('live_applied','derived_applied')",
                    (target["content_id"], target["stage"], target["window_key"], target["operation"]),
                ).fetchone():
                    continue  # A completed group cannot keep a blocked sibling waking.
                outstanding += 1
                if target["local_replay"]:
                    ready.append(target)
                    continue
                assessment = view.assess(**{key: target[key] for key in (
                    "operation", "category", "content_id", "account_id", "identity_id", "stage", "window_key",
                )})
                if assessment["runnable"]:
                    ready.append(target)
                else:
                    blocked[assessment["work_scope_sha256"]] = assessment
                    if "group" in target:
                        blocked_groups.append(target["group"])
                    if record:
                        with transaction(connection):
                            record_work_blocked(connection, assessment=assessment, at=at)
            if not outstanding or ready:
                item = dict(candidate)
                if kind == "metrics_backfill" and blocked_groups:
                    item["allowed_metric_groups"] = [
                        rule["name"] for rule in load_policy()["metric_supplement_groups"][item["platform"]]
                        if rule["name"] not in blocked_groups
                    ]
                runnable.append(item)
    return runnable, blocked


def _frozen_queue_items(connection, checkpoint: Mapping[str, Any]) -> list[dict[str, Any]]:
    """Reload mutable content without replacing the frozen identity/cycle."""
    pending = set(checkpoint.get("pending_ids", []))
    items = []
    for frozen in checkpoint.get("items", []):
        if int(frozen["id"]) not in pending:
            continue
        row = connection.execute("SELECT * FROM content_items WHERE id=?", (frozen["id"],)).fetchone()
        if not within_automatic_scope(row["published_at"] if row else None):
            continue
        # Missing targets must reach the original terminal/error path, not be
        # hidden behind an unrelated provider outage forever.
        items.append({**(dict(row) if row else {}), **frozen})
    return items


def _run_blocked_local_work(
    kind: str, items: Sequence[dict[str, Any]], *, db_path: Path, local_runner,
) -> dict[str, Any]:
    """No queue claim or paid scope: only cache replay and local processing."""
    if kind not in {"content_pipeline", "history_recovery", "comments_refresh"}:
        return {}
    outcomes: dict[str, Any] = {}
    for item in items:
        cid = int(item["id"])
        try:
            if kind == "comments_refresh":
                result = capture_content_comments_live(
                    cid, as_of=date.fromisoformat(item["comment_as_of"]),
                    db_path=db_path, cache_only=True,
                )
            else:
                result = update_content_data(
                    cid, stages=["detail"], process_media=False,
                    db_path=db_path, cache_only=True,
                )
            outcomes[str(cid)] = {key: result[key] for key in ("status", "reason", "stop_reason") if key in result}
        except Exception as error:
            outcomes[str(cid)] = {"status": "partial", "reason": _error_reason(error)}
    if items and kind in {"content_pipeline", "history_recovery"}:
        try:
            result = local_runner([int(item["id"]) for item in items], db_path=db_path)
            _request_local_restores(result.get("blocked_media", {}), db_path=db_path)
            outcomes["local"] = {key: result[key] for key in ("terminal_ids", "pending_ids", "errors") if key in result}
        except Exception as error:
            outcomes["local"] = {"status": "partial", "reason": _error_reason(error)}
    return outcomes


_RETRYABLE_PAID_GUARD_ERRORS = frozenset({
    "provider_balance_blocked",
    "provider_auth_blocked",
    "provider_circuit_open",
    "provider_blocked",
    "provider_transport_blocked",
    "billing_unknown_retry_blocked",
    "budget_blocked",
    "task_budget_exhausted",
    "budget_daily_quota_exhausted",
    "category_budget_exhausted",
    "discovery_budget_exhausted",
    "metrics_budget_exhausted",
    "automatic_budget_exhausted",
    "repair_budget_exhausted",
    "global_budget_exhausted",
    "incident_total_budget_exhausted",
    "incident_bucket_budget_exhausted",
    "incident_authorization_invalid",
    "compensation_authorization_required",
    "compensation_authorization_invalid",
    "compensation_authorization_consumed",
    "compensation_gap_invalid",
    "paid_identity_hold",
    "operation_blocked",
    "storage_hard",
    "authorization_hard",
})


def _paid_result_is_terminal(result: Mapping[str, Any]) -> bool:
    failed_stages = [
        stage for stage in result.get("stages", [])
        if stage.get("status") == "failed"
    ]
    if failed_stages:
        return all(
            stage.get("retryable") is False
            and stage.get("error_code") not in _RETRYABLE_PAID_GUARD_ERRORS
            for stage in failed_stages
        )
    return result.get("status") not in {
        "partial", "failed", "retryable_failed", "incomplete",
    }


def run_local_batch(content_ids: Sequence[int], *, db_path: Path) -> dict[str, Any]:
    """Cached media/local analysis is independent of provider availability."""
    with LOCAL_PROCESSING_LOCK:
        return _run_local_batch(content_ids, db_path=db_path)


def _run_local_batch(content_ids: Sequence[int], *, db_path: Path) -> dict[str, Any]:
    ids = list(dict.fromkeys(content_ids))
    downloads = run_media_download_queue(limit=len(ids), scope_content_ids=ids, db_path=db_path)
    processing = run_media_processing_queue(limit=len(ids), scope_content_ids=ids, db_path=db_path)
    with connect(db_path) as connection:
        releases = connection.execute("SELECT id FROM evaluation_releases WHERE status='active'").fetchall()
        if len(releases) != 1:
            raise RuntimeError("content pipeline requires one active evaluation release")
        release_id = releases[0]["id"]
        states = media_terminal_state_details(connection, release_id, ids)
    evaluation_results = []
    errors = []
    processed_ids = {int(item["content_id"]) for item in processing.get("results", []) if item.get("status") == "evidence_ready"}
    for cid in ids:
        if states[cid].reason != "evaluation_pending" and cid not in processed_ids:
            continue
        try:
            result = evaluate_content(cid, db_path=db_path, expected_active_release_id=release_id)
            evaluation_results.append({"content_id": cid, "evaluation_id": result.evaluation_id, "created": result.created})
        except Exception as error:
            errors.append({"content_id": cid, "reason": type(error).__name__})
    fingerprints = run_duplicate_fingerprint_queue(limit=len(ids), scope_content_ids=ids, db_path=db_path)
    with connect(db_path) as connection, transaction(connection):
        final_states = media_terminal_state_details(connection, release_id, ids)
        complete = []
        for cid in ids:
            if final_states[cid].state in {"complete", "terminal_insufficient", "terminal_failed"} and _fingerprint_ready(connection, cid):
                complete.append(cid)
                if final_states[cid].state == "complete":
                    connection.execute("UPDATE content_items SET source_group='',updated_at=? WHERE id=? AND source_group='history-backfill'", (now_utc(), cid))
        blocked = _media_work_blockers(connection, ids, states=final_states)
    return {"downloads": downloads, "processing": processing, "evaluation": evaluation_results,
            "fingerprints": fingerprints, "errors": errors, "terminal_ids": complete,
            "blocked_media": blocked,
            "pending_ids": [cid for cid in ids if cid not in complete]}


def _local_analysis_scope(*, db_path: Path, at: str) -> tuple[dict[str, Any], list[dict[str, Any]]]:
    """Reuse the runtime roster without making local evidence a paid route claim."""
    from importlib import import_module
    from .content_scope import canonical_content_predicate

    directory_scope = None
    try:
        catalog = import_module(".account_catalog_capture", __package__)
    except ModuleNotFoundError as error:
        if error.name != f"{__package__}.account_catalog_capture":
            raise
    else:
        with connect(db_path) as connection:
            if catalog.installed_policy(connection, at=at) is not None:
                from .account_capture_eligibility import derive_capture_eligibility

                directory_scope = derive_capture_eligibility(connection)
    if directory_scope is None:
        snapshot, members = _scope(db_path, at=at)
    else:
        # A verified successor policy owns local business eligibility too.
        # Do not intersect it with the legacy roster or accounts.enabled bit.
        members = directory_scope["eligible_members"]
        snapshot = {"id": None, "members_sha256": directory_scope["selection_sha256"],
                    "scope_kind": "account_directory"}
    eligible = {int(item["identity_id"]) for item in members if item["enabled"] and item["uid"]}
    with connect(db_path) as connection:
        rows = connection.execute(
            "SELECT c.*,i.id identity_id FROM content_items c "
            "JOIN accounts a ON a.id=c.account_id "
            "JOIN account_platform_identities i ON i.account_id=c.account_id AND i.platform=c.platform "
            "WHERE " + ("a.enabled=1" if directory_scope is None else "1=1") +
            " AND c.platform IN ('douyin','xiaohongshu') "
            "AND c.content_type IN ('video','image') "
            "AND COALESCE(c.source_group,'') NOT IN ('history-backfill','history-archive') "
            "AND julianday(c.published_at)<=julianday(?) "
            "AND julianday(c.published_at)>=julianday(?)-30 AND "
            + canonical_content_predicate(connection, "c") +
            " AND (EXISTS (SELECT 1 FROM fetch_slots f WHERE f.content_id=c.id "
            "AND f.stage='detail' AND f.status='succeeded') OR EXISTS ("
            "SELECT 1 FROM evidence_artifacts e WHERE e.content_id=c.id AND e.status='available' "
            "AND e.artifact_type IN ('media_source','media','media_manifest','media_lifecycle_manifest'))) "
            "ORDER BY c.published_at,c.id", (at, at),
        ).fetchall()
    return snapshot, [dict(row) for row in rows if row["identity_id"] in eligible
                      and within_automatic_scope(row["published_at"])]


def _local_analysis_identity(connection, content: Mapping[str, Any], release_id: str) -> dict[str, Any]:
    from .media import processor_versions

    source = connection.execute(
        "SELECT id,sha256 FROM evidence_artifacts WHERE content_id=? AND artifact_type='media_source' "
        "AND status='available' ORDER BY id DESC LIMIT 1", (content["id"],),
    ).fetchone()
    original = None if source else connection.execute(
        "SELECT id,sha256 FROM evidence_artifacts WHERE content_id=? AND artifact_type IN ('media','media_manifest') "
        "AND status='available' ORDER BY id DESC LIMIT 1", (content["id"],),
    ).fetchone()
    # A source/release change gets a new scope; day rollover and restart do not.
    # A managed bundle created by downloading this source is an OUTPUT, not a
    # new input identity: otherwise normal completion/restart strands its run.
    identity = {"contract_version": "local-content-analysis-v1", "content_id": int(content["id"]),
            "release_id": release_id, "source": dict(source) if source else None,
            "original": dict(original) if original else None,
            "processors": processor_versions(), "fingerprint_version": FINGERPRINT_VERSION}
    return {**identity, "input_sha256": durable_runs.scan_identity(LOCAL_ANALYSIS_JOB, identity)}


def _local_analysis_current(connection, content_id: int, *, snapshot: Mapping[str, Any],
                            active: Mapping[str, Any], at: str):
    """Recheck one identity/content without rescanning the entire directory."""
    from .content_scope import canonical_content_predicate

    row = connection.execute(
        "SELECT c.*,i.id identity_id FROM content_items c JOIN account_platform_identities i "
        "ON i.account_id=c.account_id AND i.platform=c.platform WHERE c.id=? AND "
        + canonical_content_predicate(connection, "c"), (content_id,),
    ).fetchone()
    if (row is None or not within_automatic_scope(row["published_at"])
            or row["source_group"] in {"history-backfill", "history-archive"}
            or row["content_type"] not in {"video", "image"}
            or not timedelta(0) <= parse_time(at) - parse_time(row["published_at"]) <= timedelta(days=30)):
        return None, snapshot
    try:
        if snapshot.get("scope_kind") == "account_directory":
            from .account_capture_eligibility import require_directory_capture_member

            require_directory_capture_member(connection, int(row["identity_id"]))
            current_scope = snapshot
        else:
            member = require_active_member(connection, int(row["identity_id"]), activation=active)
            current_scope = {"id": member["roster_snapshot_id"], "members_sha256": member["roster_snapshot_hash"]}
    except ValueError:
        return None, snapshot
    return dict(row), current_scope


@contextmanager
def _local_analysis_lease(claim: durable_runs.DurableClaim, *, db_path: Path):
    """ASR may exceed the ordinary durable lease; renew until processing yields."""
    stopped = Event()
    failures: list[Exception] = []

    def renew():
        while not stopped.wait(durable_runs.HEARTBEAT_SECONDS):
            try:
                with connect(db_path) as connection, transaction(connection, priority="heartbeat"):
                    if not stopped.is_set():
                        durable_runs.heartbeat(connection, claim, now=now_utc())
            except Exception as error:
                failures.append(error)
                return

    worker = Thread(target=renew, name=f"local-analysis-lease-{claim.scheduler_run_id}", daemon=True)
    worker.start()
    try:
        yield
        if failures:
            raise durable_runs.LostOwnership("local analysis lease could not be renewed") from failures[0]
    finally:
        stopped.set()
        worker.join(timeout=1)


def run_local_content_analysis(*, db_path: Path = DEFAULT_DB, at: str | None = None,
                               automatic_from: date | None = None, limit: int = 20,
                               time_limit_seconds: float = 60) -> dict[str, Any]:
    """Resume integrated local media/evaluation debt without any provider dispatch.

    Download uses only stored source URLs. Missing/expired sources stay explicit;
    this worker never calls update_content_data, retry_content_media or paid_scope.
    """
    from .runtime_database import require_current_process_writer_lock

    if not 1 <= limit <= 500 or time_limit_seconds <= 0:
        raise ValueError("local analysis requires limit 1..500 and positive time limit")
    timestamp = at or now_utc()
    deadline = monotonic() + time_limit_seconds
    with automatic_scope(automatic_from), LOCAL_PROCESSING_LOCK:
        with connect(db_path) as connection:
            if connection.execute("PRAGMA user_version").fetchone()[0] not in {20, 21}:
                return {"status": "skipped", "reason": "schema19_legacy", "provider_calls": 0}
            require_current_process_writer_lock(connection)
            active = activation(connection, at=timestamp)
        if active is None or active.get("mode") != "active" or active.get("profile_id") != "integrated_route_v1":
            return {"status": "skipped", "reason": "integrated_profile_required", "provider_calls": 0}
        if not within_automatic_scope(timestamp):
            return {"status": "skipped", "reason": "automatic_before_start", "provider_calls": 0}
        snapshot, rows = _local_analysis_scope(db_path=db_path, at=timestamp)
        with connect(db_path) as connection, transaction(connection):
            running = [int(row[0]) for row in connection.execute(
                "SELECT id FROM scheduler_runs WHERE job_id=? AND status='running'", (LOCAL_ANALYSIS_JOB,),
            )]
            recovered = durable_runs.recover_expired_leases(connection, now=timestamp, owned_run_ids=running)
            release = connection.execute("SELECT id FROM evaluation_releases WHERE status='active'").fetchone()
            if release is None:
                raise RuntimeError("local analysis requires an active evaluation release")
            release_id = str(release["id"])
            states = media_terminal_state_details(connection, release_id, [int(row["id"]) for row in rows])
            candidates = [row for row in rows if states[int(row["id"])].state != "complete"
                          or not _fingerprint_ready(connection, int(row["id"]))]
        results: list[dict[str, Any]] = []
        for row in candidates:
            if len(results) >= limit or monotonic() >= deadline:
                break
            # Pause/removal and source changes are re-read before each item,
            # including previously interrupted work from an earlier day.
            item_at = timestamp if at is not None else now_utc()
            with connect(db_path) as connection:
                current_active = activation(connection, at=item_at)
                current_release = connection.execute("SELECT id FROM evaluation_releases WHERE status='active'").fetchone()
                if (current_active is None or current_active.get("profile_id") != "integrated_route_v1"
                        or current_active.get("mode") != "active" or current_release is None
                        or current_release["id"] != release_id):
                    break
                current, current_snapshot_value = _local_analysis_current(
                    connection, int(row["id"]), snapshot=snapshot, active=current_active, at=item_at)
                if current is None:
                    continue
                cid = int(current["id"])
                identity = _local_analysis_identity(connection, current, release_id)
                failed = connection.execute(
                    "SELECT id FROM scheduler_runs WHERE job_id=? AND status='failed' "
                    "AND json_extract(details_json,'$.identity.input_sha256')=? ORDER BY id DESC LIMIT 1",
                    (LOCAL_ANALYSIS_JOB, identity["input_sha256"]),
                ).fetchone()
                current_state = media_terminal_state_details(connection, release_id, [cid])[cid]
                # A separately repaired original or reset stage can become
                # runnable without changing its source URL/release identity.
                claim_identity = {**identity, **({"recovery_after_run_id": int(failed["id"])}
                    if failed is not None and current_state.state != "terminal_failed" else {})}
            claim = durable_runs.claim_run(LOCAL_ANALYSIS_JOB, claim_identity, db_path=db_path, now=item_at,
                initial_checkpoint={"complete": False, "content_id": cid})
            if claim is None:
                continue
            previous = durable_runs.get_run(claim.scheduler_run_id, db_path=db_path)["details"]["checkpoint"]
            local: dict[str, Any] = {}
            error_reason: str | None = None
            try:
                with _local_analysis_lease(claim, db_path=db_path):
                    with connect(db_path) as connection:
                        before = media_terminal_state_details(connection, release_id, [cid])
                        blocked = _media_work_blockers(connection, [cid], states=before)
                    if blocked or before[cid].reason == "source_missing":
                        local = {"blocked_media": blocked}
                    else:
                        local = run_local_batch([cid], db_path=db_path)
                    _request_local_restores(local.get("blocked_media", {}), db_path=db_path)
            except durable_runs.LostOwnership:
                raise
            except Exception as error:
                error_reason = _error_reason(error)
            finished_at = item_at if at is not None else now_utc()
            with connect(db_path) as connection, transaction(connection):
                state = media_terminal_state_details(connection, release_id, [cid])[cid]
                source_changed = _local_analysis_identity(connection, current, release_id) != identity
                terminal_failed = state.state == "terminal_failed" and not source_changed
                complete = (state.state in {"complete", "terminal_insufficient"}
                            and _fingerprint_ready(connection, cid) and not source_changed and error_reason is None)
                reason = ("media_source_changed" if source_changed else error_reason or state.reason)
                failures = 0 if complete else int(previous.get("consecutive_failures", 0)) + 1
                stage_errors = [item for key in ("downloads", "processing", "fingerprints")
                                for item in local.get(key, {}).get("results", []) if item.get("error")]
                receipt = {"content_id": cid, "state": state.state, "reason": reason,
                           "complete": complete, "consecutive_failures": failures,
                           "roster_snapshot_id": current_snapshot_value["id"],
                           "roster_snapshot_hash": current_snapshot_value["members_sha256"],
                           "scope_kind": current_snapshot_value.get("scope_kind", "runtime_roster"),
                           "provider_calls": 0, "errors": local.get("errors", []) + stage_errors,
                           "next_action": "media_source_refresh_required" if reason in {"source_missing", "download_terminal_failed"}
                                          else "original_media_required" if reason in MEDIA_BLOCKED_REASONS else None}
                durable_runs.checkpoint(connection, claim, receipt, now=finished_at)
                if failures >= LOCAL_BATCH_ALERT_FAILURES:
                    _open_local_batch_alert(connection, claim=claim, kind=LOCAL_ANALYSIS_JOB,
                                          reason=reason, failures=failures, at=finished_at)
                status = "succeeded" if complete else "failed" if terminal_failed or source_changed else "partial"
                durable_runs.finish_run_in_transaction(connection, claim, status=status, now=finished_at,
                    summary={"content_id": cid, "state": state.state, "reason": reason, "provider_calls": 0},
                    next_resume_at=_iso(parse_time(finished_at) + timedelta(
                        seconds=min(300 * 2 ** min(max(failures - 1, 0), 7), 21600))) if status == "partial" else None)
            results.append({**receipt, "scheduler_run_id": claim.scheduler_run_id, "status": status})
        complete = not candidates or len(results) == len(candidates) and all(item["complete"] for item in results)
        return {"status": "succeeded" if complete else "partial", "complete": complete,
                "reason": "queue_empty_not_discovery_complete" if not candidates else None,
                "eligible": len(rows), "candidates": len(candidates), "processed": len(results),
                "recovered": recovered, "provider_calls": 0, "results": results,
                "roster_snapshot_id": snapshot["id"]}


def run_content_batch(
    kind: str, *, db_path: Path = DEFAULT_DB, at: str | None = None, limit: int = 100,
    call_override: Callable[[str, Mapping[str, Any]], ProviderResult] | None = None,
    local_runner: Callable[..., dict[str, Any]] = run_local_batch,
    resume_run_id: int | None = None,
    age_band: str = "all",
    frozen_roster: Mapping[str, Any] | None = None,
) -> dict[str, Any]:
    if kind not in QUEUE_KINDS:
        raise ValueError("unsupported pipeline queue")
    timestamp = at or now_utc()
    scope_blocked: dict[str, Any] = {}
    first_day = automatic_from_date()
    blocked_work: dict[str, Any] = {}
    blocked_local: dict[str, Any] = {}
    runnable_by_id: dict[int, dict[str, Any]] | None = None
    claim: durable_runs.DurableClaim | None
    dispatch_active: dict[str, Any] | None
    if resume_run_id is None:
        candidates: list[dict[str, Any]] = []
        identity: dict[str, Any] = {}
        claim = None
        # A queue identity includes its creation timestamp, so the durable
        # uniqueness key alone cannot reserve overlapping fresh batches. Keep
        # this lock only across selection and claim; provider/local work runs
        # after it is released.
        with QUEUE_RESERVATION_LOCKS[kind]:
            snapshot, _members = _scope(db_path, frozen_roster, at=timestamp)
            with connect(db_path) as connection:
                queue_active = activation(connection, at=timestamp)
            if queue_active is None:
                return {
                    "status": "skipped", "complete": False,
                    "reason": "pipeline_activation_required",
                }
            due_candidates = _queue_candidates(
                kind,
                at=timestamp,
                db_path=db_path,
                age_band=age_band,
                frozen_roster=frozen_roster,
                blocked_media=scope_blocked,
            )
            ready, blocked_work = _partition_queue_readiness(
                kind, due_candidates, at=timestamp, db_path=db_path,
            )
            candidates = allocate_candidates(ready, limit)
            runnable_by_id = {int(item["id"]): item for item in candidates}
            ready_ids = {int(item["id"]) for item in ready}
            blocked_items = [item for item in due_candidates if int(item["id"]) not in ready_ids][:limit]
            if candidates:
                identity = {
                    "pipeline_version": PIPELINE_VERSION,
                    "kind": kind,
                    "roster_snapshot_id": snapshot["id"],
                    "roster_snapshot_hash": snapshot["members_sha256"],
                    "created_for": timestamp,
                    **({"automatic_from_date": first_day.isoformat()} if first_day else {}),
                    "candidate_ids": [int(item["id"]) for item in candidates],
                    **_activation_identity(queue_active),
                }
                # Freeze only dispatch identity, not mutable titles/bodies or
                # large API responses. The bounded batch has at most 500
                # compact item receipts.
                items = [{
                    key: item[key]
                    for key in (
                        "id", "identity_id", "historical", "cycle_key", "comment_as_of",
                    )
                    if key in item
                } for item in candidates]
                initial = {
                    "pending_ids": identity["candidate_ids"],
                    "items": items,
                    "results": {},
                    "complete": False,
                }
                dispatch_active = queue_active
                claim, deferred = _claim_paid_run_or_defer(
                    kind,
                    identity,
                    db_path=db_path,
                    active=dispatch_active,
                    registration_id=str(
                        frozen_roster.get("registration_id", kind)
                        if frozen_roster else kind
                    ),
                    due_slot=str(
                        frozen_roster.get("scheduled_at", timestamp)
                        if frozen_roster else timestamp
                    ),
                    at=timestamp,
                    initial_checkpoint=initial,
                )
                if deferred is not None:
                    return deferred
        scope_blocked = _request_local_restores(scope_blocked, db_path=db_path)
        blocked_local = _run_blocked_local_work(kind, blocked_items, db_path=db_path, local_runner=local_runner)
        if not candidates:
            if blocked_work:
                return {"status": "blocked", "complete": False, "candidates": 0,
                        "reason": "work_not_ready", "blocked_work": blocked_work,
                        "blocked_media": scope_blocked, "local": blocked_local}
            if scope_blocked:
                return {"status": "blocked", "complete": False, "candidates": 0,
                        "reason": "media_local_prerequisite", "blocked_media": scope_blocked}
            return {"status": "succeeded", "complete": True, "candidates": 0, "reason": "queue_empty_not_discovery_complete"}
    else:
        previous = durable_runs.get_run(resume_run_id, db_path=db_path)
        identity = previous["details"]["identity"]
        if identity.get("kind") != kind:
            raise ValueError("resume queue kind mismatch")
        if not _scope_is_after_automatic_start(kind, identity):
            return {"status": "skipped", "complete": False, "reason": "automatic_before_start"}
        with connect(db_path) as connection:
            dispatch_active = activation(connection, at=timestamp)
            should_assess = (
                _activation_is_current(connection, identity, at=timestamp)
                and parse_time(str(identity["created_for"])).astimezone(BEIJING).date()
                == parse_time(timestamp).astimezone(BEIJING).date()
            )
            frozen_items = _frozen_queue_items(connection, previous["details"]["checkpoint"])
        if automatic_from_date() is not None and not frozen_items:
            return {"status": "skipped", "complete": False, "reason": "automatic_before_start"}
        if should_assess and frozen_items and all(item.get("platform") for item in frozen_items):
            ready, blocked_work = _partition_queue_readiness(
                kind, frozen_items, at=timestamp, db_path=db_path,
            )
            runnable_by_id = {int(item["id"]): item for item in ready}
            blocked_local = _run_blocked_local_work(
                kind, [item for item in frozen_items if int(item["id"]) not in runnable_by_id][:limit],
                db_path=db_path, local_runner=local_runner,
            )
            if not ready and blocked_work:
                return {"status": "blocked", "complete": False, "reason": "work_not_ready",
                        "scheduler_run_id": resume_run_id, "blocked_work": blocked_work,
                        "local": blocked_local}
        claim, deferred = _claim_paid_run_or_defer(
            kind,
            identity,
            db_path=db_path,
            active=dispatch_active,
            registration_id=str(identity.get("kind", kind)),
            due_slot=str(identity.get("created_for", timestamp)),
            at=timestamp,
        )
        if deferred is not None:
            return deferred
    if claim is None:
        return {"status": "skipped", "complete": False, "reason": "not_due_or_claimed"}
    checkpoint = durable_runs.get_run(claim.scheduler_run_id, db_path=db_path)["details"]["checkpoint"]
    pending = list(checkpoint["pending_ids"])
    by_id = {int(item["id"]): item for item in checkpoint["items"]}
    results = dict(checkpoint["results"])
    blocked = dict(checkpoint.get("blocked_media", {}))
    local_batch_failures = int(checkpoint.get("local_batch_failures", 0))
    local_batch_error: str | None = None
    if automatic_from_date() is not None:
        with connect(db_path) as connection:
            eligible = {int(item["id"]) for item in _frozen_queue_items(connection, checkpoint)}
        excluded = [cid for cid in pending if cid not in eligible]
        pending = [cid for cid in pending if cid in eligible]
        results.update({str(cid): {"status": "skipped", "reason": "automatic_before_start"} for cid in excluded})
    if kind != "history_recovery":
        skipped_history = [cid for cid in pending if by_id[cid].get("historical")]
        if skipped_history:
            pending = [cid for cid in pending if cid not in skipped_history]
            results.update({
                str(cid): {
                    "status": "skipped",
                    "reason": "automatic_history_disabled",
                }
                for cid in skipped_history
            })
            with connect(db_path) as connection, transaction(connection):
                durable_runs.checkpoint(
                    connection,
                    claim,
                    {"pending_ids": pending, "results": results},
                    now=timestamp,
                )
    frozen_day = (
        parse_time(str(identity["created_for"])).astimezone(BEIJING).date()
        if identity.get("created_for")
        else None
    )
    business_day = frozen_day.isoformat() if frozen_day is not None else None
    current_day = parse_time(timestamp).astimezone(BEIJING).date()
    if frozen_day is not None and frozen_day < current_day:
        results.update({
            str(cid): {"status": "skipped", "reason": "business_day_expired"}
            for cid in pending
        })
        pending = []
    profile_activation_id = identity.get("activation_id")
    profile_activation_error: RosterError | None = None
    with connect(db_path) as connection:
        if not _activation_is_current(connection, identity, at=timestamp):
            profile_activation_error = RosterError(
                "profile_superseded",
                "Frozen acquisition activation is no longer effective",
            )
        elif profile_activation_id is None:
            try:
                profile_activation_id = _acquisition_activation_id(
                    connection, timestamp
                )
            except RosterError as error:
                profile_activation_error = error
    local_ids: dict[str, list[int]] = {"history": [], "detail": []}
    delay = 300.0
    for cid in list(pending):
        if runnable_by_id is not None and cid not in runnable_by_id:
            continue
        item = {**by_id[cid], **(runnable_by_id or {}).get(cid, {})}
        # Recheck after claim as well as selection: old durable debt and a
        # concurrent archive must not fall through to paid detail refresh.
        if kind in {"content_pipeline", "history_recovery"}:
            with connect(db_path) as connection:
                current_blocked = _media_work_blockers(connection, [cid])
            if current_blocked:
                blocked.update(_request_local_restores(current_blocked, db_path=db_path))
                pending.remove(cid)
                results[str(cid)] = {"status": "blocked", **blocked[str(cid)]}
                with connect(db_path) as connection, transaction(connection):
                    durable_runs.checkpoint(connection, claim, {"pending_ids": pending, "results": results,
                                                              "blocked_media": blocked}, now=timestamp)
                continue
        purpose = "history" if item.get("historical") or kind == "history_recovery" else {"metrics_backfill": "metrics", "comments_refresh": "comments"}.get(kind, "detail")
        try:
            if profile_activation_error is not None:
                raise profile_activation_error
            with connect(db_path) as connection:
                require_active_member(
                    connection,
                    int(item["identity_id"]),
                    int(identity["roster_snapshot_id"]),
                    str(identity["roster_snapshot_hash"]),
                    activation=profile_activation_id,
                )
            with paid_scope(purpose, activation_id=profile_activation_id,
                            roster_snapshot_id=identity["roster_snapshot_id"], roster_snapshot_hash=identity["roster_snapshot_hash"],
                            scheduler_run_id=claim.scheduler_run_id, scheduler_attempt_id=claim.attempt_id,
                            business_day=business_day):
                task_id = "pipeline:" + claim.scan_id
                if kind == "metrics_backfill":
                    result = refresh_content_metrics(cid, db_path=db_path, at=timestamp, cycle_key=item.get("cycle_key"), task_id=task_id, task_max_amount=DEFAULT_TASK_MAX_AMOUNT_USD, call_override=call_override,
                                                     allowed_groups=item.get("allowed_metric_groups"))
                elif kind == "comments_refresh":
                    result = capture_content_comments_live(cid, as_of=date.fromisoformat(item["comment_as_of"]), db_path=db_path,
                                                           task_id=task_id, task_max_amount=DEFAULT_TASK_MAX_AMOUNT_USD, call_override=call_override)
                else:
                    result = update_content_data(cid, stages=["detail"], process_media=False, db_path=db_path,
                                                 task_id=task_id, task_max_amount=DEFAULT_TASK_MAX_AMOUNT_USD, call_override=call_override)
            results[str(cid)] = {key: result[key] for key in ("status", "reason", "stop_reason", "cycle_key", "missing_fields", "request_cycle_complete", "deferred_groups", "provider_cost", "capture_run_id") if key in result}
            if result.get("stages"):
                results[str(cid)]["stages"] = [{key: stage[key] for key in ("stage", "status", "error_code", "retryable", "amount") if key in stage} for stage in result["stages"]]
            if kind in {"content_pipeline", "history_recovery"}:
                local_ids[purpose].append(cid)
            elif ((kind == "metrics_backfill" and result.get("request_cycle_complete"))
                  or (kind == "comments_refresh" and result.get("status") in {"succeeded", "already_succeeded"})):
                pending.remove(cid)
        except Exception as error:
            reason = _error_reason(error)
            terminal = reason == "profile_superseded"
            results[str(cid)] = {
                "status": "skipped" if terminal else "partial", "reason": reason,
            }
            delay = max(delay, float(getattr(error, "retry_after_seconds", None) or 0))
            # A provider circuit never prevents already-cached local work.
            if kind in {"content_pipeline", "history_recovery"}:
                local_ids[purpose].append(cid)
            elif terminal:
                pending.remove(cid)
        with connect(db_path) as connection, transaction(connection):
            durable_runs.checkpoint(connection, claim, {"pending_ids": pending, "results": results}, now=timestamp)
    local: dict[str, Any] = {}
    for purpose, ids in local_ids.items():
        if not ids:
            continue
        try:
            with paid_scope(purpose, activation_id=profile_activation_id,
                            roster_snapshot_id=identity["roster_snapshot_id"], roster_snapshot_hash=identity["roster_snapshot_hash"],
                            scheduler_run_id=claim.scheduler_run_id, scheduler_attempt_id=claim.attempt_id,
                            business_day=business_day):
                with connect(db_path) as connection, transaction(connection):
                    durable_runs.assert_owner(connection, claim)
                local_result = local_runner(ids, db_path=db_path)
            paid_complete = {
                cid for cid in local_result.get("terminal_ids", [])
                if _paid_result_is_terminal(results.get(str(cid), {}))
            }
            blocked.update(_request_local_restores(local_result.get("blocked_media", {}), db_path=db_path))
            pending = [cid for cid in pending if cid not in paid_complete and str(cid) not in blocked]
            local[purpose] = {"terminal_ids": local_result.get("terminal_ids", []), "pending_ids": local_result.get("pending_ids", []),
                              "errors": local_result.get("errors", []), "fingerprint_failures": local_result.get("fingerprints", {}).get("failed", 0)}
            local_batch_failures = 0
        except Exception as error:
            # The whole local batch failed, so nothing left ``pending``.  Keep the
            # round resumable (a fixed precondition heals it on the next resume)
            # but make the reason visible and count consecutive failures instead
            # of silently re-running every five minutes until the day rolls over.
            local_batch_error = _error_reason(error)
            local_batch_failures += 1
            local[purpose] = {"status": "partial", "reason": local_batch_error}
    with connect(db_path) as connection, transaction(connection):
        durable_runs.checkpoint(connection, claim, {"pending_ids": pending, "results": results, "local": local,
                                                  "blocked_media": blocked, "complete": not pending and not blocked,
                                                  "local_batch_failures": local_batch_failures,
                                                  "local_batch_error": local_batch_error}, now=timestamp)
        if local_batch_error and local_batch_failures >= LOCAL_BATCH_ALERT_FAILURES:
            _open_local_batch_alert(connection, claim=claim, kind=kind, reason=local_batch_error,
                                    failures=local_batch_failures, at=timestamp)
    status = "partial" if pending else "failed" if blocked else "succeeded"
    superseded = bool(results) and any(
        item.get("reason") == "profile_superseded" for item in results.values()
    )
    result = durable_runs.finish_run(claim, status=status, db_path=db_path,
                                     summary={"candidates": len(by_id), "remaining": len(pending), "blocked_media_count": len(blocked),
                                              "reason": "media_local_prerequisite" if blocked else
                                                        "profile_superseded" if superseded else
                                                        "local_batch_error" if local_batch_error else None,
                                              "local_batch_error": local_batch_error,
                                              "local_batch_failures": local_batch_failures},
                                     next_resume_at=_iso(parse_time(timestamp) + timedelta(seconds=delay)) if pending else None, now=timestamp)
    missing_fields = {cid: item["missing_fields"] for cid, item in results.items() if item.get("missing_fields")}
    return {"status": "partial" if pending or missing_fields else "blocked" if blocked or scope_blocked or blocked_work else "succeeded",
            "complete": not pending and not blocked and not scope_blocked and not blocked_work,
            "reason": "profile_superseded" if superseded else "local_batch_error" if local_batch_error else None,
            "local_batch_error": local_batch_error,
            "blocked_media": {**scope_blocked, **blocked},
            "blocked_work": blocked_work, "blocked_local": blocked_local,
            "quality_missing_fields": missing_fields, "scheduler_run_id": claim.scheduler_run_id, "details": result}


def run_matrix_round(
    kind: str,
    *,
    at: str,
    db_path: Path,
    client=None,
    frozen_roster: Mapping[str, Any] | None = None,
    execution_at: str | None = None,
) -> dict[str, Any]:
    from .matrix_scan import run_matrix_scan

    snapshot, _members = _scope(db_path, frozen_roster, at=at)
    local = parse_time(at).astimezone(BEIJING)
    params = {
        "db_path": db_path,
        "roster_snapshot_id": snapshot["id"],
        "roster_snapshot_hash": snapshot["members_sha256"],
        "client": client,
        "now": execution_at or at,
        **({
            "activation_id": frozen_roster["activation_id"],
            "profile_id": frozen_roster["profile_id"],
        } if frozen_roster and "activation_id" in frozen_roster else {}),
    }
    results = []
    if kind == "accounts":
        if not within_automatic_scope((local.date() - timedelta(days=1)).isoformat()):
            return {"status": "skipped", "complete": True, "reason": "automatic_before_start", "scans": []}
        for platform in ("douyin", "xiaohongshu"):
            results.append(run_matrix_scan("accounts", platform, purpose="daily-account-metrics", rank_date=(local.date() - timedelta(days=1)).isoformat(), **params))
    else:
        end = datetime.combine(local.date(), time.min, BEIJING) if local.hour == 2 else local
        start = end - timedelta(days=30 if local.hour == 2 else 2)
        floor = automatic_start_at()
        if floor is not None:
            start = max(start, floor)
        if start >= end:
            return {"status": "skipped", "complete": True, "reason": "automatic_before_start", "scans": []}
        cursor = end
        while cursor > start:
            previous = max(start, datetime.combine(cursor.date(), time.min, BEIJING))
            if previous == cursor:
                previous = max(start, cursor - timedelta(days=1))
            for platform in ("douyin", "xiaohongshu"):
                results.append(run_matrix_scan("works", platform, purpose="daily-works" if local.hour == 2 else "incremental-works", start_at=_iso(previous), end_at=_iso(cursor), overall_start_at=_iso(start), overall_end_at=_iso(end), **params))
            cursor = previous
    complete = bool(results) and all(item.get("complete") for item in results)
    return {"status": "succeeded" if complete else "partial", "complete": complete, "scans": results}


def _day_windows(start: datetime, end: datetime) -> list[tuple[str, str]]:
    windows = []
    cursor = end.astimezone(BEIJING)
    start = start.astimezone(BEIJING)
    while cursor > start:
        previous = max(start, datetime.combine(cursor.date(), time.min, BEIJING))
        if previous == cursor:
            previous = max(start, cursor - timedelta(days=1))
        windows.append((_iso(previous), _iso(cursor)))
        cursor = previous
    return windows


def seed_history_work(*, db_path: Path, at: str) -> list[int]:
    """Create durable initial/new-member and outage scopes, without network.

    A rolling seven-day scan never erases an older unfinished catalog. Initial
    member windows end at cutover; later members end at first acceptance.
    """
    snapshot, members = _scope(db_path, at=at)
    with connect(db_path) as connection:
        active = activation(connection, at=at)
        if active is None:
            return []
        if "activation_id" in active:
            bootstrap = connection.execute(
                "SELECT a.roster_snapshot_id,a.effective_at,s.accepted_at "
                "FROM acquisition_profile_activations a "
                "JOIN account_roster_snapshots s ON s.id=a.roster_snapshot_id "
                "WHERE a.profile_id=? ORDER BY a.effective_at,a.id LIMIT 1",
                (active["profile_id"],),
            ).fetchone()
        else:
            bootstrap = None
        catalogs = [json.loads(row[0]) for row in connection.execute(
            "SELECT details_json FROM scheduler_runs WHERE job_id='history_scan_catalog' ORDER BY id")]
        known = {int(value) for item in catalogs for value in item.get("identity", {}).get("initial_member_ids", [])}
        first_membership = {int(row["account_identity_id"]): dict(row) for row in connection.execute(
            "SELECT m.account_identity_id,MIN(s.id) snapshot_id,MIN(s.accepted_at) accepted_at "
            "FROM account_roster_members m JOIN account_roster_snapshots s ON s.id=m.snapshot_id "
            "GROUP BY m.account_identity_id")}
        old_scans = [json.loads(row[0]) for row in connection.execute(
            "SELECT details_json FROM scheduler_runs WHERE job_id='tikhub_reconcile' AND status='succeeded'")]
    cutover = max(
        parse_time(str(bootstrap["effective_at"])),
        parse_time(str(bootstrap["accepted_at"])),
    ) if bootstrap is not None else parse_time(active["cutover_at"])
    bootstrap_id = int(
        bootstrap["roster_snapshot_id"] if bootstrap is not None
        else active["roster_snapshot_id"]
    )
    base = {
        "roster_snapshot_id": snapshot["id"],
        "roster_snapshot_hash": snapshot["members_sha256"],
        **_activation_identity(active),
    }
    requests: list[dict[str, Any]] = []
    new_members = [member for member in members if member["identity_id"] not in known and member["uid"]]
    matrix_ends: set[str] = set()
    for member in new_members:
        first = first_membership[member["identity_id"]]
        end = cutover if first["snapshot_id"] <= bootstrap_id else parse_time(first["accepted_at"])
        if end <= parse_time(HISTORY_START):
            continue
        matrix_ends.add(_iso(end))
        requests.append({**base, "provider": "tikhub", "identity_id": member["identity_id"],
                         "purpose": "history", "window_start": HISTORY_START, "window_end": _iso(end)})
        recent_end = datetime.combine(parse_time(at).astimezone(BEIJING).date(), time.min, BEIJING)
        requests.append({**base, "provider": "tikhub", "identity_id": member["identity_id"],
                         "purpose": "reconcile", "window_start": _iso(recent_end - timedelta(days=7)), "window_end": _iso(recent_end)})
    # Global Matrix pages come first, giving directories/metrics priority.
    matrix_requests = []
    if active.get("profile_id", MATRIX_PROFILE) == MATRIX_PROFILE:
        for matrix_end in sorted(matrix_ends, reverse=True):
            for start_day, end_day in _day_windows(
                parse_time(HISTORY_START), parse_time(matrix_end)
            ):
                for platform in ("douyin", "xiaohongshu"):
                    matrix_requests.append({
                        **base,
                        "provider": "newrank_matrix",
                        "kind": "works",
                        "platform": platform,
                        "purpose": "history-initial",
                        "start_at": start_day,
                        "end_at": end_day,
                        "overall_start_at": HISTORY_START,
                        "overall_end_at": matrix_end,
                    })
    requests = matrix_requests + requests
    if new_members and requests:
        catalog_identity = {**base, "pipeline_version": PIPELINE_VERSION, "kind": "initial",
                            "initial_member_ids": [int(item["identity_id"]) for item in new_members]}
        _enqueue_catalog(catalog_identity, requests, at=at, db_path=db_path)
    # Capture only the older-than-rolling-window gap. Already-created catalogs
    # keep their frozen gap even if a newer complete scan advances a watermark.
    gap_end = datetime.combine(parse_time(at).astimezone(BEIJING).date(), time.min, BEIJING) - timedelta(days=7)
    gap_requests = []
    for member in members:
        if not member["uid"]:
            continue
        ends = [parse_time(item["identity"]["window_end"]) for item in old_scans
                if item.get("complete") and str(item.get("identity", {}).get("provider", "")).lower() == "tikhub"
                and item["identity"].get("identity_id") == member["identity_id"]]
        ends.extend(parse_time(spec["window_end"]) for catalog in catalogs
                    for spec in catalog.get("checkpoint", {}).get("items", [])
                    if spec.get("provider") == "tikhub" and spec.get("purpose") == "history"
                    and spec.get("identity_id") == member["identity_id"])
        start = max(ends, default=max(cutover, parse_time(first_membership[member["identity_id"]]["accepted_at"])))
        if start < gap_end:
            gap_requests.append({**base, "provider": "tikhub", "identity_id": member["identity_id"],
                                 "purpose": "history", "window_start": _iso(start), "window_end": _iso(gap_end)})
    if gap_requests:
        _enqueue_catalog({**base, "pipeline_version": PIPELINE_VERSION, "kind": "outage",
                          "gap_end": _iso(gap_end)}, gap_requests, at=at, db_path=db_path)
    with connect(db_path) as connection:
        return [int(row[0]) for row in connection.execute(
            "SELECT id FROM scheduler_runs WHERE job_id='history_scan_catalog' AND status IN ('partial','interrupted') ORDER BY COALESCE(completed_at,started_at),id")]


def _enqueue_catalog(identity, requests, *, at: str, db_path: Path) -> None:
    with connect(db_path) as connection:
        dispatch_active = activation(connection, at=at)
        if _supports_profile_activations(connection) and (
            dispatch_active is None
            or identity.get("activation_id") != dispatch_active["activation_id"]
            or identity.get("profile_id") != dispatch_active["profile_id"]
        ):
            raise RosterError(
                "roster_activation_required",
                "History catalogs must bind the effective acquisition activation",
            )
    claim, deferred = _claim_paid_run_or_defer(
        "history_scan_catalog",
        identity,
        db_path=db_path,
        active=dispatch_active,
        registration_id="history_recovery",
        due_slot=at,
        at=at,
        initial_checkpoint={
            "items": requests,
            "pending_indices": list(range(len(requests))),
            "children": {},
            "complete": False,
        },
    )
    if deferred is not None:
        return
    if claim is not None:
        durable_runs.finish_run(claim, status="partial", db_path=db_path, now=at, next_resume_at=at, summary={"reason": "queued"})


def run_history_catalog(run_id: int, *, db_path: Path, at: str, limit: int = 20,
                        call_override=None, matrix_client=None) -> dict[str, Any]:
    from .matrix_scan import run_matrix_scan
    from .tikhub_scan import run_account_scan

    previous = durable_runs.get_run(run_id, db_path=db_path)
    previous_identity = previous["details"]["identity"]
    with connect(db_path) as connection:
        dispatch_active = activation(connection, at=at)
        superseded = not _activation_is_current(
            connection, previous_identity, at=at,
        )
    if superseded:
        terminal_claim = durable_runs.claim_run(
            "history_scan_catalog", previous_identity, db_path=db_path, now=at,
        )
        if terminal_claim is not None:
            durable_runs.finish_run(
                terminal_claim,
                status="failed",
                db_path=db_path,
                now=at,
                summary={"reason": "profile_superseded"},
            )
        return {
            "scheduler_run_id": run_id,
            "status": "skipped",
            "complete": True,
            "reason": "profile_superseded",
        }
    claim, deferred = _claim_paid_run_or_defer(
        "history_scan_catalog",
        previous_identity,
        db_path=db_path,
        active=dispatch_active,
        registration_id="history_recovery",
        due_slot=str(previous["details"]["identity"].get("created_for", at)),
        at=at,
    )
    if deferred is not None:
        return {**deferred, "scheduler_run_id": run_id}
    if claim is None:
        return {"status": "skipped", "complete": previous["details"].get("complete", False), "scheduler_run_id": run_id}
    state = durable_runs.get_run(run_id, db_path=db_path)["details"]["checkpoint"]
    pending = list(state["pending_indices"])
    children = dict(state["children"])
    due_indices: set[int] = set()
    due_at = parse_time(at)
    for index in list(pending):
        child_id = children.get(str(index), {}).get("scheduler_run_id")
        if child_id is None:
            due_indices.add(index)
            continue
        old_child = durable_runs.get_run(child_id, db_path=db_path)
        if old_child["details"].get("complete"):
            pending.remove(index)
            continue
        due = old_child["details"].get("next_resume_at")
        if old_child["status"] != "running" and (not due or parse_time(due) <= due_at):
            due_indices.add(index)

    tikhub_identity_ids = [
        state["items"][index].get("identity_id")
        for index in due_indices
        if str(state["items"][index].get("provider", "")).lower() == "tikhub"
    ]
    platforms = _identity_platforms(db_path, tikhub_identity_ids)
    dispatch_order = _fair_platform_order(
        pending,
        lambda index: (
            state["items"][index].get("platform")
            or platforms.get(state["items"][index].get("identity_id"))
        )
        if index in due_indices
        and str(state["items"][index].get("provider", "")).lower() == "tikhub"
        else None,
    )
    processed = 0
    for index in dispatch_order:
        if index not in due_indices:
            continue
        if processed >= limit:
            break
        processed += 1
        spec = dict(state["items"][index])
        provider = spec.pop("provider")
        try:
            if provider == "newrank_matrix":
                child = run_matrix_scan(db_path=db_path, client=matrix_client, now=at, **spec)
            else:
                keys = (
                    "identity_id", "window_start", "window_end", "purpose",
                    "roster_snapshot_id", "roster_snapshot_hash",
                    "activation_id", "profile_id", "activation_sha256",
                )
                child = run_account_scan(
                    db_path=db_path, call_override=call_override, max_pages=1, now=at,
                    **{key: spec[key] for key in keys if key in spec},
                )
            children[str(index)] = {key: child[key] for key in ("scheduler_run_id", "status", "complete", "reason") if key in child}
            if child.get("complete"):
                pending.remove(index)
        except Exception as error:
            children[str(index)] = {"status": "partial", "reason": _error_reason(error)}
        with connect(db_path) as connection, transaction(connection):
            durable_runs.checkpoint(connection, claim, {"pending_indices": pending, "children": children}, now=at)
        if children[str(index)].get("reason") in {"budget_partial", "provider_circuit_open", "global_budget_exhausted", "category_budget_exhausted"}:
            break
    with connect(db_path) as connection, transaction(connection):
        durable_runs.checkpoint(connection, claim, {"pending_indices": pending, "children": children, "complete": not pending}, now=at)
    durable_runs.finish_run(claim, status="partial" if pending else "succeeded", db_path=db_path, now=at,
                            next_resume_at=_iso(parse_time(at) + timedelta(minutes=5)) if pending else None,
                            summary={"remaining_scans": len(pending)})
    return {"scheduler_run_id": run_id, "status": "partial" if pending else "succeeded", "complete": not pending, "remaining_scans": len(pending)}


def resume_due_work(*, at: str, db_path: Path, limit: int = 20,
                    reports_root: Path | None = None, call_override=None,
                    matrix_client=None) -> list[dict[str, Any]]:
    from .matrix_scan import run_matrix_scan
    from .tikhub_scan import resume_account_scan

    with connect(db_path) as connection:
        rows = connection.execute("SELECT id,job_id,status,details_json FROM scheduler_runs WHERE status IN ('partial','interrupted') ORDER BY COALESCE(completed_at,started_at),id").fetchall()
        schema20 = connection.execute("PRAGMA user_version").fetchone()[0] in {20, 21}
        effective_activation = activation(connection, at=at)
        effective_activation_id = (
            int(effective_activation["activation_id"])
            if effective_activation is not None
            and "activation_id" in effective_activation else None
        )
        child_activations: dict[int, tuple[int, str]] = {}
        operator_children: set[int] = set()
        for parent in connection.execute(
            "SELECT details_json FROM scheduler_runs "
            "WHERE job_id LIKE 'pipeline_round:%'"
        ):
            parent_details = json.loads(parent["details_json"])
            parent_identity = parent_details.get("identity", {})
            if parent_identity.get("registration_id") not in CRON_ROUNDS:
                # On-demand campaigns retain their original durable command
                # owner. An interval wakeup cannot recreate or borrow it.
                operator_children.update(
                    child_id for child_id in parent_details.get("checkpoint", {}).get("child_run_ids", [])
                    if type(child_id) is int
                )
            activation_id = parent_identity.get("activation_id")
            profile_id = parent_identity.get("profile_id")
            if type(activation_id) is not int or not isinstance(profile_id, str):
                continue
            for child_id in parent_details.get("checkpoint", {}).get("child_run_ids", []):
                if type(child_id) is int:
                    child_activations[child_id] = (activation_id, profile_id)
    candidates = []
    for row in rows:
        details = json.loads(row["details_json"])
        if details.get("contract_version") != durable_runs.CONTRACT_VERSION:
            continue
        identity = details.get("identity", {})
        if int(row["id"]) in operator_children or (
            row["job_id"].startswith("pipeline_round:")
            and identity.get("registration_id") not in CRON_ROUNDS
        ):
            continue
        if (schema20 and str(identity.get("provider", "")).lower() == "tikhub"
                and identity.get("purpose") == "reconcile" and int(row["id"]) not in child_activations):
            # Old invalid interval scans have no frozen parent to resume.
            # Keep their receipts; local raw debt has its separate replay path.
            continue
        if not _scope_is_after_automatic_start(str(row["job_id"]), identity):
            continue
        inherited = child_activations.get(int(row["id"]))
        activation_id = identity.get("activation_id")
        profile_id = identity.get("profile_id")
        if inherited is not None and activation_id is None:
            activation_id, profile_id = inherited
        superseded_scope = (
            type(activation_id) is int
            and activation_id != effective_activation_id
        )
        if (
            not superseded_scope
            and not _resume_scope_is_current(row["job_id"], identity, at=at)
        ):
            continue
        checkpoint = details.get("checkpoint", {})
        finalization_only = (
            row["job_id"].startswith("pipeline_round:")
            and row["status"] == "interrupted"
            and checkpoint.get("complete") is True
        )
        if details.get("complete") and not finalization_only:
            continue
        if details.get("next_resume_at") and parse_time(details["next_resume_at"]) > parse_time(at):
            continue
        candidates.append((row, details, identity, activation_id, profile_id))

    tikhub_identity_ids = [
        identity.get("identity_id")
        for _row, _details, identity, _activation_id, _profile_id in candidates
        if str(identity.get("provider", "")).lower() == "tikhub"
    ]
    platforms = _identity_platforms(db_path, tikhub_identity_ids)
    candidates = _fair_platform_order(
        candidates,
        lambda item: (
            item[2].get("platform") or platforms.get(item[2].get("identity_id"))
        )
        if str(item[2].get("provider", "")).lower() == "tikhub"
        else None,
    )
    results: list[dict[str, Any]] = []
    for row, details, identity, activation_id, profile_id in candidates:
        if len(results) >= limit:
            break
        budget = current_reconcile_budget()
        if budget is not None:
            # Control only reconciles parent receipts. Ordinary workers retain
            # ownership of paid pages, media, comments and historical purchases.
            if not row["job_id"].startswith("pipeline_round:"):
                continue
            if identity.get("job_id") in {"daily_report", "weekly_report"}:
                continue
            if not budget.take():
                break
        try:
            with connect(db_path) as connection:
                superseded = (
                    type(activation_id) is int
                    and not _activation_is_current(
                        connection, {"activation_id": activation_id}, at=at,
                    )
                )
            if superseded and (
                identity.get("provider") == "newrank_matrix"
                or str(identity.get("provider", "")).lower() == "tikhub"
                or row["job_id"] == "history_scan_catalog"
            ):
                claim = durable_runs.claim_run(
                    str(row["job_id"]), identity, db_path=db_path, now=at,
                )
                if claim is not None:
                    durable_runs.finish_run(
                        claim,
                        status="failed",
                        db_path=db_path,
                        now=at,
                        summary={"reason": "profile_superseded"},
                    )
                results.append({
                    "scheduler_run_id": int(row["id"]),
                    "status": "skipped",
                    "complete": True,
                    "reason": "profile_superseded",
                })
                continue
            if identity.get("provider") == "newrank_matrix":
                keys = ("purpose", "start_at", "end_at", "rank_date", "roster_snapshot_id", "roster_snapshot_hash", "activation_id", "profile_id", "overall_start_at", "overall_end_at")
                result = run_matrix_scan(identity["kind"], identity["platform"], db_path=db_path, now=at, client=matrix_client, **{key: identity[key] for key in keys if key in identity})
            elif str(identity.get("provider", "")).lower() == "tikhub":
                with paid_scope(
                    str(identity.get("purpose", "reconcile")),
                    activation_id=activation_id if type(activation_id) is int else None,
                    roster_snapshot_id=identity.get("roster_snapshot_id"),
                    roster_snapshot_hash=identity.get("roster_snapshot_hash"),
                ):
                    result = resume_account_scan(
                        row["id"], db_path=db_path, max_pages=1, now=at,
                        call_override=call_override,
                    )
            elif row["job_id"] == "history_scan_catalog":
                result = run_history_catalog(row["id"], db_path=db_path, at=at, call_override=call_override, matrix_client=matrix_client)
            elif row["job_id"] in QUEUE_KINDS:
                result = run_content_batch(row["job_id"], db_path=db_path, at=at, resume_run_id=row["id"], call_override=call_override)
            elif row["job_id"].startswith("pipeline_round:"):
                if reports_root is None:
                    from .reports import REPORTS_ROOT
                    reports_root = REPORTS_ROOT
                result = dispatch(identity["job_id"], registration_id=identity["registration_id"],
                                  db_path=db_path, reports_root=reports_root, at=at, resume_round_id=row["id"], call_override=call_override, matrix_client=matrix_client)
            else:
                continue
            results.append(result)
        except Exception as error:
            reason = _error_reason(error)
            if reason == "profile_switch_drain":
                results.append({
                    "scheduler_run_id": row["id"],
                    "status": "skipped",
                    "complete": False,
                    "reason": "dispatch_deferred",
                    "deferred_reason": "profile_switch_drain",
                })
            else:
                results.append({
                    "scheduler_run_id": row["id"],
                    "status": "partial",
                    "reason": reason,
                })
    return results


def queue_backlog_summary(*, db_path: Path = DEFAULT_DB, at: str | None = None) -> dict[str, Any]:
    """Current work inventory, not a count of historical blocked receipts.

    Units are (queue kind, content). Partly runnable metric groups stay in the
    runnable partition; blocked operations are reported separately. Coverage
    and expected discovery denominators are never adjusted by this inventory.
    """
    timestamp = at or now_utc()
    by_kind: dict[str, Any] = {}
    reason_items: dict[str, set[tuple[str, int]]] = {}
    for kind in sorted(QUEUE_KINDS):
        media: dict[str, Any] = {}
        candidates = _queue_candidates(
            kind, at=timestamp, db_path=db_path, include_claimed=True, blocked_media=media,
        )
        ready, blocked = _partition_queue_readiness(
            kind, candidates, at=timestamp, db_path=db_path, record=False,
        )
        all_ids = {int(item["id"]) for item in candidates} | {int(cid) for cid in media}
        ready_ids = {int(item["id"]) for item in ready} - {int(cid) for cid in media}
        for assessment in blocked.values():
            cid = int(assessment["work_scope"]["content_id"])
            reason_items.setdefault(assessment["reason"], set()).add((kind, cid))
        for media_cid, detail in media.items():
            reason_items.setdefault(str(detail.get("reason", "media_local_prerequisite")), set()).add((kind, int(media_cid)))
        by_kind[kind] = {
            "total": len(all_ids), "runnable": len(ready_ids),
            "blocked": len(all_ids - ready_ids), "blocked_operation_count": len(blocked),
        }
    return {
        "contract_version": "queue-backlog-v1", "captured_at": timestamp,
        "unit": "queue_kind_content", "by_kind": by_kind,
        "by_reason": {reason: len(items) for reason, items in sorted(reason_items.items())},
        **{key: sum(row[key] for row in by_kind.values())
           for key in ("total", "runnable", "blocked", "blocked_operation_count")},
    }


def pipeline_summary(*, db_path: Path = DEFAULT_DB, at: str | None = None) -> dict[str, Any]:
    timestamp = at or now_utc()
    with connect(db_path) as connection:
        active = activation(connection, at=timestamp)
        roster = (
            runtime_snapshot(connection, active)
            if active is not None and "activation_id" in active
            else current_snapshot(connection)
        )
        members = get_current_members(
            connection, snapshot_id=roster["id"] if roster else None,
        )
        runs = []
        for row in connection.execute(
            "SELECT id,job_id,status,details_json FROM scheduler_runs "
            "WHERE json_valid(details_json) "
            "AND json_extract(details_json,'$.contract_version')=? ORDER BY id DESC LIMIT 500",
            (durable_runs.CONTRACT_VERSION,),
        ):
            value = json.loads(row["details_json"])
            if value.get("contract_version") != durable_runs.CONTRACT_VERSION:
                continue
            cp = value.get("checkpoint", {})
            runs.append({"id": row["id"], "job_id": row["job_id"], "status": row["status"], "complete": value.get("complete"),
                         "scope": value.get("identity"), "counts": cp.get("counts"), "raw_row_count": cp.get("raw_row_count"),
                         "pending_count": len(cp.get("pending_ids", [])), "blocked_media": cp.get("blocked_media", {}),
                         "next_resume_at": value.get("next_resume_at"), "reason": value.get("summary", {}).get("reason")})
        from .runtime_receipts import latest_runtime_coverage
        coverage = latest_runtime_coverage(connection, at=timestamp)
        return {"contract_version": PIPELINE_VERSION, "captured_at": timestamp,
                "activation_id": active.get("activation_id") if active else None,
                "profile_id": active.get("profile_id") if active else MATRIX_PROFILE,
                "roster_snapshot_id": roster["id"] if roster else None, "roster_members": len(members),
                "eligible_members": sum(bool(item["enabled"] and item["uid"]) for item in members),
                "monitoring_members": sum(item.get("monitoring_status") == "monitored" for item in members),
                "budget": budget_summary(connection, at=timestamp), "runs": runs,
                "work_backlog": queue_backlog_summary(db_path=db_path, at=timestamp) if active and roster else None,
                "discovery_coverage": coverage,
                "discovery_complete": coverage["complete"]}


def _report_round_is_complete(
    job_id: str, scheduled: datetime, result: Mapping[str, Any], *, db_path: Path,
) -> bool:
    status = result.get("status")
    if status in {"succeeded", "partial", "skipped"}:
        return True
    if status != "skipped_duplicate":
        return False
    with connect(db_path) as connection:
        row = connection.execute(
            "SELECT status FROM scheduler_runs WHERE job_id=? AND scheduled_for=?" + durable_runs.root_run_predicate(connection),
            (job_id, _iso(scheduled)),
        ).fetchone()
    return row is not None and row["status"] in {"succeeded", "partial", "skipped"}


def _run_tikhub_account_profiles(
    *,
    at: str,
    db_path: Path,
    call_override,
    frozen_roster: Mapping[str, Any] | None,
) -> dict[str, Any]:
    """Refresh account-only TikHub facts without invoking Matrix."""
    _snapshot_value, members = _scope(db_path, frozen_roster, at=at)
    with connect(db_path) as connection:
        if connection.execute("PRAGMA user_version").fetchone()[0] in {20, 21}:
            members = [member for member in members if member["platform"] == "douyin" and legacy_queue_allowed(
                connection, account_id=int(member["account_id"]), content_id=None,
                operations=["douyin_uid_profile"], at=at)]
    profiles: list[dict[str, Any]] = []
    for member in members:
        try:
            profiles.append(
                refresh_account_profile(
                    member["identity_id"],
                    db_path=db_path,
                    at=at,
                    call_override=call_override,
                )
            )
        except Exception as error:
            profiles.append({
                "identity_id": member["identity_id"],
                "status": "partial",
                "reason": _error_reason(error),
            })
    complete = all(
        item.get("status") in {"succeeded", "skipped"}
        or item.get("request_cycle_complete")
        for item in profiles
    )
    return {
        "status": "succeeded" if complete else "partial",
        "complete": complete,
        "profiles": profiles,
    }


def _run_tikhub_discovery_round(
    *,
    at: str,
    db_path: Path,
    call_override,
    frozen_roster: Mapping[str, Any] | None,
    full_reconcile: bool,
) -> dict[str, Any]:
    from . import tikhub_scan

    snapshot, members = _scope(db_path, frozen_roster, at=at)
    planned = parse_time(
        str(frozen_roster.get("scheduled_at", at)) if frozen_roster else at
    ).astimezone(BEIJING)
    if full_reconcile:
        end = datetime.combine(planned.date(), time.min, BEIJING)
        start = end - timedelta(days=7)
    elif planned.hour == 2:
        end = datetime.combine(planned.date(), time.min, BEIJING)
        start = end - timedelta(days=30)
    else:
        end = planned
        start = end - timedelta(days=2)
    floor = automatic_start_at()
    if floor is not None:
        start = max(start, floor)
    if start >= end:
        return {"status": "skipped", "complete": True, "reason": "automatic_before_start", "scans": []}
    window_start, window_end = _iso(start), _iso(end)
    with connect(db_path) as connection:
        if connection.execute("PRAGMA user_version").fetchone()[0] in {20, 21}:
            members = [member for member in members if legacy_queue_allowed(
                connection, account_id=int(member["account_id"]), content_id=None,
                operations=[str(member["platform"]) + "_user_posts"], at=at)]
    eligible_members = _fair_platform_order(
        [member for member in members if member["uid"]],
        lambda member: member.get("platform"),
    )
    child_epoch: dict[str, Any]
    if frozen_roster is not None and "activation_id" in frozen_roster:
        child_epoch = {
            "activation_id": int(frozen_roster["activation_id"]),
            "profile_id": str(frozen_roster["profile_id"]),
            "activation_sha256": str(frozen_roster["activation_sha256"]),
        }
    else:
        with connect(db_path) as connection:
            active = activation(connection, at=at)
        child_epoch = (
            {
                "activation_id": int(active["activation_id"]),
                "profile_id": str(active["profile_id"]),
                "activation_sha256": str(active["activation_sha256"]),
            }
            if active is not None and "activation_id" in active
            else {}
        )

    def run_member(member: Mapping[str, Any]) -> dict[str, Any]:
        child_scope = {
            "contract_version": tikhub_scan.CONTRACT_VERSION,
            "provider": "TikHub",
            "purpose": "reconcile",
            "identity_id": int(member["identity_id"]),
            "account_id": int(member["account_id"]),
            "platform": str(member["platform"]),
            "uid": str(member["uid"]),
            "roster_snapshot_id": int(snapshot["id"]),
            "roster_snapshot_hash": str(snapshot["members_sha256"]),
            **child_epoch,
            "window_start": window_start,
            "window_end": window_end,
            "task_id": None,
            "task_max_microusd": micro_usd(DEFAULT_TASK_MAX_AMOUNT_USD),
        } if member.get("account_id") is not None else None
        before = (
            _tikhub_child_observation(child_scope, db_path=db_path)
            if child_scope is not None
            else None
        )
        try:
            return dict(tikhub_scan.run_account_scan(
                member["identity_id"],
                window_start=window_start,
                window_end=window_end,
                purpose="reconcile",
                roster_snapshot_id=snapshot["id"],
                roster_snapshot_hash=snapshot["members_sha256"],
                **child_epoch,
                db_path=db_path,
                max_pages=1,
                now=at,
                call_override=call_override,
            ))
        except Exception as error:
            recovered = (
                _recover_tikhub_child_after_worker_error(
                    child_scope, before, error, db_path=db_path, at=at,
                )
                if child_scope is not None
                else None
            )
            if recovered is not None:
                return recovered
            raise

    results = _run_tikhub_discovery_workers(eligible_members, run_member)
    complete = all(item.get("complete") for item in results)
    return {
        "status": "succeeded" if complete else "partial",
        "complete": complete,
        "scans": results,
        "window_start": window_start,
        "window_end": window_end,
    }


def _queue_checkpoint_progressed(
    before: Mapping[str, Any],
    after: Mapping[str, Any],
) -> bool:
    return (
        set(before.get("pending_ids", [])) != set(after.get("pending_ids", []))
        or (before.get("complete") is not True and after.get("complete") is True)
    )


def _queue_batch_summary(result: Mapping[str, Any]) -> dict[str, Any]:
    return {
        key: result[key]
        for key in ("status", "complete", "reason", "scheduler_run_id", "blocked_work")
        if key in result and result[key] is not None
        and (key != "blocked_work" or bool(result[key]))
    }


def _dispatch(job_id: str, *, db_path: Path, reports_root: Path, at: str | None = None,
              call_override=None, registration_id: str | None = None,
              matrix_client=None, frozen_roster: Mapping[str, Any] | None = None) -> dict[str, Any]:
    def invoke() -> dict[str, Any]:
        return _dispatch_body(
            job_id, db_path=db_path, reports_root=reports_root, at=at,
            call_override=call_override, registration_id=registration_id,
            matrix_client=matrix_client, frozen_roster=frozen_roster,
        )
    if job_id != "pipeline_reconcile":
        return invoke()
    timestamp = at or now_utc()
    with connect(db_path) as connection:
        active = activation(connection, at=timestamp)
    if active is None or active.get("mode") != "active":
        return {"status": "skipped", "reason": "pipeline_activation_required"}
    claim = durable_runs.claim_run(
        "pipeline_reconcile_slice",
        {"contract_version": "reconcile-slice-v1", "at": timestamp},
        db_path=db_path, now=timestamp,
    )
    if claim is None:
        return {"status": "skipped", "complete": False, "reason": "reconcile_slice_already_claimed"}
    with reconcile_budget_scope() as budget:
        try:
            result = invoke()
        except Exception as error:
            durable_runs.finish_run(
                claim, db_path=db_path, now=timestamp, status="partial",
                next_resume_at=_iso(parse_time(timestamp) + timedelta(minutes=5)),
                summary={"reason": _error_reason(error), "processed_work_units": budget.used},
            )
            raise
        progress = {
            "processed_work_units": budget.used, "max_work_units": budget.max_items,
            "time_limit_seconds": 60, "time_limit_reached": budget.expired,
            "continuation_required": budget.remaining == 0 or not result.get("complete"),
            "next_due_at": _iso(parse_time(timestamp) + timedelta(minutes=5)),
        }
        result["slice"] = progress
        # A succeeded slice is a control-plane receipt, never a data-health
        # assertion. Original pending scopes and their identities remain intact.
        with connect(db_path) as connection, transaction(connection):
            durable_runs.checkpoint(connection, claim, {"complete": True, **progress}, now=timestamp)
        durable_runs.finish_run(
            claim, db_path=db_path, now=timestamp, status="succeeded",
            summary={**progress, "data_complete": result.get("complete") is True},
        )
        return result


def _replay_materialization_debt(*, db_path: Path, at: str) -> list[dict[str, Any]]:
    from .tikhub_scan import resume_local_materialization

    budget = current_reconcile_budget()
    if budget is None:
        raise RuntimeError("local debt replay requires a bounded reconcile slice")
    floor = automatic_start_at()
    floor_iso = floor.isoformat() if floor is not None else None
    with connect(db_path) as connection:
        rows = connection.execute(
            "SELECT id,job_id,details_json FROM scheduler_runs WHERE (status IN ('partial','interrupted') "
            "OR (status='failed' AND json_valid(details_json) "
            "AND json_extract(details_json,'$.summary.reason')='profile_superseded')) "
            "AND job_id IN ('tikhub_reconcile','history_recovery') "
            "AND json_valid(details_json) "
            "AND json_type(details_json,'$.checkpoint.pending_materialization')='object' "
            "AND (? IS NULL OR (job_id<>'history_recovery' "
            "AND julianday(json_extract(details_json,'$.identity.window_start'))>=julianday(?))) "
            "ORDER BY id LIMIT 50",
            (floor_iso, floor_iso),
        ).fetchall()
    results = []
    for row in rows:
        if not _scope_is_after_automatic_start(str(row["job_id"]), json.loads(row["details_json"]).get("identity", {})):
            continue
        available = budget.remaining
        if available == 0:
            break
        try:
            result = resume_local_materialization(
                int(row["id"]), db_path=db_path, now=at,
                max_items=available, deadline=budget.deadline,
            )
        except Exception as error:
            result = {
                "scheduler_run_id": int(row["id"]), "status": "partial",
                "complete": False, "reason": _error_reason(error), "processed_items": 0,
            }
        processed = max(1, int(result.get("processed_items", 0)))
        # The materializer checks the same deadline before each item. Its
        # completed work is accounted even if the final fsync crosses the edge.
        budget.account_completed(processed)
        results.append(result)
    return results


def _dispatch_body(job_id: str, *, db_path: Path, reports_root: Path, at: str | None = None,
              call_override=None, registration_id: str | None = None, matrix_client=None,
              frozen_roster: Mapping[str, Any] | None = None) -> dict[str, Any]:
    timestamp = at or now_utc()
    with connect(db_path) as connection:
        active = activation(connection, at=timestamp)
    if active is None or active.get("mode") != "active":
        return {"status": "skipped", "reason": "pipeline_activation_required"}
    if current_reconcile_budget() is not None and job_id in PAID_DISPATCH_JOBS:
        return {"status": "partial", "complete": False, "reason": "execution_queued"}
    if job_id in {"daily_report", "weekly_report"}:
        from .scheduler import execute_job
        local = parse_time(timestamp).astimezone(BEIJING)
        scheduled = datetime.combine(local.date(), time(8, 30 if job_id == "weekly_report" else 0), BEIJING)
        result = execute_job(job_id, scheduled, db_path=db_path, reports_root=reports_root, allow_retry=True)
        return {
            **result,
            "complete": _report_round_is_complete(
                job_id, scheduled, result, db_path=db_path,
            ),
        }
    if job_id in {"matrix_works_scan", "matrix_account_metrics"}:
        planned_at = str(frozen_roster.get("scheduled_at", timestamp)) if frozen_roster else timestamp
        result = run_matrix_round(
            "works" if job_id == "matrix_works_scan" else "accounts",
            at=planned_at,
            execution_at=timestamp,
            db_path=db_path,
            client=matrix_client,
            frozen_roster=frozen_roster,
        )
        if job_id == "matrix_account_metrics":
            _snapshot, members = _scope(db_path, frozen_roster, at=planned_at)
            profiles = []
            for member in members:
                try:
                    profiles.append(refresh_account_profile(member["identity_id"], db_path=db_path, at=timestamp, call_override=call_override))
                except Exception as error:
                    profiles.append({"identity_id": member["identity_id"], "status": "partial", "reason": _error_reason(error)})
            result["profiles"] = profiles
            result["complete"] = result.get("complete", False) and all(item.get("status") in {"succeeded", "skipped"} or item.get("request_cycle_complete") for item in profiles)
            if not result["complete"]:
                result["status"] = "partial"
        return result
    if job_id == "tikhub_account_metrics":
        return _run_tikhub_account_profiles(
            at=timestamp,
            db_path=db_path,
            call_override=call_override,
            frozen_roster=frozen_roster,
        )
    if job_id in QUEUE_KINDS:
        if job_id == "content_pipeline":
            with connect(db_path) as connection, transaction(connection):
                evaluate_transport_faults(
                    connection,
                    at=timestamp,
                    planner_tick_id=f"content_pipeline:{timestamp}",
                )
        # The five-minute workers resume their own debt, not just select fresh
        # candidates and leave the old batch reserved until the hourly guard.
        floor = automatic_start_at()
        floor_iso = floor.isoformat() if floor is not None else None
        with connect(db_path) as connection:
            older = connection.execute(
                "SELECT id FROM scheduler_runs WHERE job_id=? AND status IN ('partial','interrupted') "
                "AND json_extract(details_json,'$.identity.kind')=? "
                "AND (json_extract(details_json,'$.next_resume_at') IS NULL "
                "OR julianday(json_extract(details_json,'$.next_resume_at'))<=julianday(?)) "
                "AND (? IS NULL OR julianday(json_extract(details_json,'$.identity.created_for'))>=julianday(?)) "
                "ORDER BY COALESCE(completed_at,started_at),id LIMIT 1",
                (job_id, job_id, timestamp, floor_iso, floor_iso),
            ).fetchone()
        if older is not None:
            before = durable_runs.get_run(
                int(older["id"]), db_path=db_path,
            )["details"]["checkpoint"]
            resumed_batch = run_content_batch(
                job_id,
                db_path=db_path,
                at=timestamp,
                call_override=call_override,
                resume_run_id=int(older["id"]),
            )
            if (resumed_batch.get("scheduler_run_id") != int(older["id"])
                    and resumed_batch.get("reason") != "automatic_before_start"):
                return resumed_batch
            after = durable_runs.get_run(
                int(older["id"]), db_path=db_path,
            )["details"]["checkpoint"]
            if job_id == "history_recovery" or _queue_checkpoint_progressed(before, after):
                return resumed_batch
            band = (
                "established"
                if registration_id == "metrics_backfill_established"
                else "recent" if job_id == "metrics_backfill" else "all"
            )
            fresh = run_content_batch(
                job_id,
                db_path=db_path,
                at=timestamp,
                call_override=call_override,
                age_band=band,
                frozen_roster=frozen_roster,
            )
            return {**resumed_batch, "fresh_batch": _queue_batch_summary(fresh)}
        if job_id == "history_recovery":
            catalogs = seed_history_work(db_path=db_path, at=timestamp)
            result = run_content_batch(job_id, db_path=db_path, at=timestamp, call_override=call_override)
            result["catalogs"] = [run_history_catalog(cid, db_path=db_path, at=timestamp, call_override=call_override, matrix_client=matrix_client)
                                  for cid in catalogs[:1]]
            return result
        band = "established" if registration_id == "metrics_backfill_established" else "recent" if job_id == "metrics_backfill" else "all"
        return run_content_batch(job_id, db_path=db_path, at=timestamp, call_override=call_override, age_band=band, frozen_roster=frozen_roster)
    if job_id in {"tikhub_reconcile", "tikhub_works_scan"}:
        return _run_tikhub_discovery_round(
            at=timestamp,
            db_path=db_path,
            call_override=call_override,
            frozen_roster=frozen_roster,
            full_reconcile=job_id == "tikhub_reconcile",
        )
    if job_id == "pipeline_reconcile":
        local_replay = _replay_materialization_debt(db_path=db_path, at=timestamp)
        rounds = _reconcile_current_day_rounds(
            at=timestamp,
            db_path=db_path,
            reports_root=reports_root,
            call_override=call_override,
            matrix_client=matrix_client,
        )
        resumed = resume_due_work(
            at=timestamp,
            db_path=db_path,
            reports_root=reports_root,
            call_override=call_override,
            matrix_client=matrix_client,
        )
        healthy = all(
            item.get("status") in {"succeeded", "skipped"} and item.get("complete") is not False
            for item in [*local_replay, *resumed, *rounds]
            if item.get("reason") not in {
                "paid_round_reconcile_cutoff",
                "dispatch_deferred",
                "report_executor_owned",
            }
        )
        deferred_due_slots = [
            {
                "registration_id": item.get("registration_id"),
                "scheduled_at": item.get("scheduled_at", item.get("due_slot")),
            }
            for item in [*resumed, *rounds]
            if item.get("reason") == "dispatch_deferred"
        ]
        try:
            from .runtime_receipts import refresh_runtime_receipts
            receipt_refresh = refresh_runtime_receipts(
                db_path=db_path,
                cutoff_at=timestamp,
            )
            scan_refresh = receipt_refresh.get("scan_receipts", {})
            refresh_ready = receipt_refresh.get("status") == "succeeded" or (
                receipt_refresh.get("status") == "skipped"
                and receipt_refresh.get("reason") == "profile_day_anchor_not_due"
            )
            receipt_healthy = (
                refresh_ready
                and not scan_refresh.get("errors")
                and not scan_refresh.get("limit_reached")
            )
            if not receipt_healthy:
                receipt_refresh = {
                    **receipt_refresh,
                    "status": "partial",
                    "reason": "runtime_receipt_refresh_incomplete",
                }
        except Exception as error:
            receipt_healthy = False
            receipt_refresh = {
                "status": "partial",
                "reason": _error_reason(error),
                "error": str(error),
            }
        healthy = healthy and receipt_healthy
        return {"status": "succeeded" if healthy else "partial", "complete": healthy,
                "local_replay": local_replay,
                "resumed": resumed, "rounds": rounds,
                "deferred_count": len(deferred_due_slots),
                "deferred_due_slots": deferred_due_slots,
                "runtime_receipts": receipt_refresh}
    if job_id == "daily_pipeline_summary":
        planned_at = str(frozen_roster.get("scheduled_at", timestamp)) if frozen_roster else timestamp
        summary = pipeline_summary(db_path=db_path, at=planned_at)
        claim = durable_runs.claim_run(job_id, {"at": planned_at, "version": PIPELINE_VERSION}, db_path=db_path)
        if claim is not None:
            with connect(db_path) as connection, transaction(connection):
                durable_runs.checkpoint(connection, claim, {"complete": True})
            durable_runs.finish_run(claim, status="succeeded", summary=summary, db_path=db_path)
        return {**summary, "status": "succeeded", "complete": True}
    raise ValueError("unsupported pipeline job")


def _legacy_runtime_authority(db_path: Path) -> AbstractContextManager[None]:
    """Bind live verification to the existing scheduler, never issue permission.

    Discovery workers already copy ContextVars. Each schema20 A/B therefore
    revalidates the installed evidence; schema19 retains its existing contract.
    """
    with connect(db_path) as connection:
        if connection.execute("PRAGMA user_version").fetchone()[0] not in {20, 21}:
            return nullcontext()
    from . import capture_authorizations, capture_release
    return capture_authorizations.runtime_authority(capture_release.current_runtime_bindings)


def dispatch(job_id: str, *, db_path: Path, reports_root: Path, at: str | None = None,
             call_override=None, registration_id: str | None = None,
             resume_round_id: int | None = None, matrix_client=None,
             automatic_from: date | None = None) -> dict[str, Any]:
    """Serialize report generation without blocking provider or local pipeline work."""

    from .scheduler import PIPELINE_REPORT_EXECUTION_LOCK

    arguments = {
        "db_path": db_path,
        "reports_root": reports_root,
        "at": at,
        "call_override": call_override,
        "registration_id": registration_id,
        "resume_round_id": resume_round_id,
        "matrix_client": matrix_client,
    }
    with automatic_scope(automatic_from), _legacy_runtime_authority(db_path):
        if job_id in {"daily_report", "weekly_report"}:
            with PIPELINE_REPORT_EXECUTION_LOCK:
                return _dispatch_locked(job_id, **arguments)
        return _dispatch_locked(job_id, **arguments)


def _dispatch_locked(job_id: str, *, db_path: Path, reports_root: Path, at: str | None = None,
                     call_override=None, registration_id: str | None = None,
                     resume_round_id: int | None = None, matrix_client=None) -> dict[str, Any]:
    """Cron rounds have immutable scopes; partial children are not replaced."""
    timestamp = at or now_utc()
    key = registration_id or job_id
    first_day = automatic_from_date()
    gate_job_id, gate_key = job_id, key
    preview: Mapping[str, Any] | None = None
    if resume_round_id is not None:
        preview = durable_runs.get_run(resume_round_id, db_path=db_path)["details"]["identity"]
        gate_key = str(preview["registration_id"])
        gate_job_id = str(preview["job_id"])
        due_slot = str(preview["scheduled_at"])
        if gate_key not in CRON_ROUNDS:
            return {"status": "skipped", "complete": False, "reason": "operator_executor_owned"}
    elif gate_key in CRON_ROUNDS:
        local = parse_time(timestamp).astimezone(BEIJING)
        due = _scheduled_round_at(gate_key, local)
        if due is None:
            return {"status": "skipped", "reason": "round_not_due", "complete": False}
        due_slot = _iso(due)
    else:
        due_slot = _iso(parse_time(timestamp))

    if not within_automatic_scope(due_slot):
        return {"status": "skipped", "complete": True, "reason": "automatic_before_start"}
    if gate_job_id in {"daily_report", "weekly_report"}:
        from .scheduler import _report_period
        report_start, _report_end = _report_period(gate_job_id, parse_time(due_slot))
        if not within_automatic_scope(report_start.isoformat()):
            return {"status": "skipped", "complete": True, "reason": "automatic_before_start"}

    with connect(db_path) as connection:
        planned_active = activation(connection, at=due_slot)
        execution_active = activation(connection, at=timestamp)
        has_profiles = _supports_profile_activations(connection)
    if planned_active is None or planned_active.get("mode") != "active":
        return {
            "status": "skipped", "reason": "pipeline_activation_required",
            "complete": False,
        }
    active_profile = str(planned_active.get("profile_id", MATRIX_PROFILE))
    if gate_key in CRON_ROUNDS and gate_key not in PROFILE_CRON_REGISTRATIONS[active_profile]:
        return {
            "status": "skipped",
            "reason": "profile_not_scheduled",
            "complete": True,
            "profile_id": active_profile,
            "scheduled_at": due_slot,
        }
    frozen_activation_id = (
        preview.get("activation_id") if preview is not None
        else planned_active.get("activation_id")
    )
    paid_superseded = bool(
        has_profiles
        and gate_job_id in PAID_DISPATCH_JOBS
        and (
            type(frozen_activation_id) is not int
            or execution_active is None
            or int(execution_active["activation_id"]) != frozen_activation_id
            or int(planned_active["activation_id"]) != frozen_activation_id
        )
    )
    if gate_job_id in PAID_DISPATCH_JOBS and not paid_superseded:
        deferred = _dispatch_deferred_receipt(
            db_path=db_path,
            active=planned_active,
            job_id=gate_job_id,
            registration_id=gate_key,
            due_slot=due_slot,
            at=timestamp,
        )
        if deferred is not None:
            return deferred
    if key not in CRON_ROUNDS:
        interval_result = _dispatch(job_id, db_path=db_path, reports_root=reports_root, at=timestamp, call_override=call_override, matrix_client=matrix_client)
        if job_id == "content_pipeline":
            interval_result["resumed"] = resume_due_work(at=timestamp, db_path=db_path, reports_root=reports_root, limit=5,
                                                call_override=call_override, matrix_client=matrix_client)
        return interval_result
    if resume_round_id is None:
        snapshot, round_members = _scope(db_path, at=due_slot)
        scheduled = parse_time(due_slot).astimezone(BEIJING)
        if scheduled is None:
            return {"status": "skipped", "reason": "round_not_due", "complete": False}
        hour, minute = scheduled.hour, scheduled.minute
        identity = {"pipeline_version": PIPELINE_VERSION, "beijing_day": scheduled.date().isoformat(),
                    "round_id": f"{key}:{hour:02d}:{minute:02d}", "registration_id": key, "job_id": job_id,
                    "scheduled_at": _iso(scheduled), "roster_snapshot_id": snapshot["id"], "roster_snapshot_hash": snapshot["members_sha256"],
                    **({"automatic_from_date": first_day.isoformat()} if first_day else {}),
                    **_activation_identity(planned_active),
                    "eligible_identity_ids": sorted(int(member["identity_id"]) for member in round_members
                                                    if member["enabled"] and member["uid"] and member["platform"] in {"douyin", "xiaohongshu"})}
    else:
        identity = durable_runs.get_run(resume_round_id, db_path=db_path)["details"]["identity"]
        key, job_id = identity["registration_id"], identity["job_id"]
    scope_key = {
        field: identity[field]
        for field in (
            "pipeline_version", "beijing_day", "registration_id", "scheduled_at",
            "activation_id", "profile_id",
        )
        if field in identity
    }
    initial_checkpoint = {
        "child_run_ids": [], "remaining_profiles": [],
        "started": False, "complete": False,
    }
    if current_reconcile_budget() is not None and job_id in PAID_DISPATCH_JOBS:
        queued = _control_round_pending("pipeline_round:" + key, scope_key, db_path=db_path)
        if queued is not None:
            return queued
    if job_id in PAID_DISPATCH_JOBS and not paid_superseded:
        claim, deferred = _claim_paid_run_or_defer(
            "pipeline_round:" + key,
            identity,
            db_path=db_path,
            active=planned_active,
            registration_id=key,
            due_slot=str(identity["scheduled_at"]),
            at=timestamp,
            scope_key=scope_key,
            initial_checkpoint=initial_checkpoint,
        )
        if deferred is not None:
            return deferred
    else:
        claim = durable_runs.claim_run(
            "pipeline_round:" + key,
            identity,
            db_path=db_path,
            now=timestamp,
            scope_key=scope_key,
            initial_checkpoint=initial_checkpoint,
        )
    if claim is None:
        with connect(db_path) as connection:
            row = connection.execute("SELECT id,status,details_json FROM scheduler_runs WHERE job_id=? AND scheduled_for=?" + durable_runs.root_run_predicate(connection),
                                     ("pipeline_round:" + key, "scan:" + durable_runs.scan_identity("pipeline_round:" + key, scope_key))).fetchone()
        return {"status": row["status"] if row else "skipped", "complete": bool(row and json.loads(row["details_json"]).get("complete")),
                "round_run_id": row["id"] if row else None, "reason": "round_already_claimed_or_finished"}
    frozen_details = durable_runs.get_run(claim.scheduler_run_id, db_path=db_path)["details"]
    identity, cp = frozen_details["identity"], frozen_details["checkpoint"]
    if paid_superseded:
        finalized = _finalize_pipeline_round(
            claim,
            db_path=db_path,
            timestamp=timestamp,
            status="succeeded",
            checkpoint_state={**cp, "complete": True},
            summary={"reason": "profile_superseded", "result_status": "skipped"},
        )
        return {
            **finalized,
            "status": "skipped",
            "complete": True,
            "reason": "profile_superseded",
            "profile_id": identity.get("profile_id"),
        }
    if cp.get("complete") is True:
        finalized = _finalize_pipeline_round(
            claim,
            db_path=db_path,
            timestamp=timestamp,
            status="succeeded",
            checkpoint_state=None,
            summary={"reason": "completed_checkpoint_finalized"},
        )
        if finalized["status"] == "succeeded":
            finalized["reason"] = "completed_checkpoint_finalized"
        return finalized
    result: dict[str, Any] = {}
    complete = False
    if current_reconcile_budget() is not None and job_id in PAID_DISPATCH_JOBS:
        # Reconcile registers due work and seals completed child receipts. It
        # must never execute a paid page or a slow profile refresh inline.
        children = cp.get("child_run_ids", [])
        complete = (
            cp.get("started") is True and bool(children)
            and not cp.get("remaining_profiles")
            and all(durable_runs.get_run(cid, db_path=db_path)["details"].get("complete")
                    for cid in children)
        )
        finalized = _finalize_pipeline_round(
            claim, db_path=db_path, timestamp=timestamp,
            status="succeeded" if complete else "partial",
            checkpoint_state={**cp, "complete": complete},
            summary={"reason": "child_checkpoint_reconciled" if complete else "execution_queued"},
        )
        return {**finalized, "reason": "child_checkpoint_reconciled" if complete else "execution_queued"}
    try:
        if cp["started"] and cp["child_run_ids"]:
            complete = all(durable_runs.get_run(cid, db_path=db_path)["details"].get("complete") for cid in cp["child_run_ids"])
            profiles = []
            for identity_id in cp.get("remaining_profiles", []):
                with paid_scope("metrics", activation_id=identity.get("activation_id"),
                                roster_snapshot_id=identity["roster_snapshot_id"], roster_snapshot_hash=identity["roster_snapshot_hash"],
                                scheduler_run_id=claim.scheduler_run_id, scheduler_attempt_id=claim.attempt_id,
                                business_day=identity["beijing_day"]):
                    profiles.append(refresh_account_profile(identity_id, db_path=db_path, at=timestamp, call_override=call_override))
            cp["remaining_profiles"] = [item["identity_id"] for item in profiles if item.get("status") not in {"succeeded", "skipped"} and not item.get("request_cycle_complete")]
            complete = complete and not cp["remaining_profiles"]
            result = {"complete": complete, "status": "succeeded" if complete else "partial", "reason": "child_checkpoint_reconciled"}
        else:
            with paid_scope("metrics" if job_id in {"metrics_backfill", "matrix_account_metrics", "tikhub_account_metrics"} else "reconcile",
                            activation_id=identity.get("activation_id"),
                            roster_snapshot_id=identity["roster_snapshot_id"], roster_snapshot_hash=identity["roster_snapshot_hash"],
                            scheduler_run_id=claim.scheduler_run_id, scheduler_attempt_id=claim.attempt_id,
                            business_day=identity["beijing_day"]):
                result = _dispatch(job_id, db_path=db_path, reports_root=reports_root, at=timestamp,
                                   call_override=call_override, registration_id=key, matrix_client=matrix_client, frozen_roster=identity)
            children = [item["scheduler_run_id"] for item in result.get("scans", []) if item.get("scheduler_run_id")]
            if result.get("scheduler_run_id"):
                children.append(result["scheduler_run_id"])
            cp.update(started=True, child_run_ids=children,
                      remaining_profiles=[item["identity_id"] for item in result.get("profiles", []) if item.get("status") not in {"succeeded", "skipped"} and not item.get("request_cycle_complete")])
            complete = result.get("complete") is True
    except Exception as error:
        result = {"status": "partial", "reason": _error_reason(error)}
    finalized = _finalize_pipeline_round(
        claim,
        db_path=db_path,
        timestamp=timestamp,
        status="succeeded" if complete else "partial",
        checkpoint_state={**cp, "complete": complete},
        summary={"result_status": result.get("status"), "reason": result.get("reason"),
                 "quality_missing_fields": result.get("quality_missing_fields", {})},
    )
    if finalized["status"] == "interrupted":
        return finalized
    return {**result, **finalized}


def _capture_v25_job(*, kind: str, db_path: Path,
                     automatic_from: date | None = None, at: str | None = None) -> dict[str, Any]:
    """Installed-writer entry point; legacy installs never enter new workers."""
    from . import capture_authorizations, capture_quality, capture_release, capture_runtime
    from .runtime_database import require_current_process_writer_lock

    timestamp = at or now_utc()
    with connect(db_path) as connection:
        if connection.execute("PRAGMA user_version").fetchone()[0] not in {20, 21}:
            return {"status": "skipped", "reason": "schema19_legacy", "provider_calls": 0}
        require_current_process_writer_lock(connection)
    with automatic_scope(automatic_from), capture_authorizations.runtime_authority(capture_release.current_runtime_bindings):
        if kind in {"plan", "execute", "maintenance"}:
            from .account_roster_capture import activate_prepared_roster_capture_in_transaction
            with connect(db_path) as connection, transaction(connection):
                activate_prepared_roster_capture_in_transaction(connection, at=timestamp)
        if kind == "plan":
            return capture_runtime.plan_tick(db_path, timestamp)
        if kind == "execute":
            from .capture_commands import process_commands
            process_commands(db_path=db_path, at=timestamp)
            return capture_runtime.run_ready(db_path, timestamp, max_items=TIKHUB_NETWORK_CONCURRENCY)
        if kind == "maintenance":
            from .capture_evidence_preflight import evidence_boundary, prepare_installed_evidence
            result = capture_quality.maintenance_tick(db_path=db_path, at=timestamp)
            # Qualification renewal is a separate, evidence-bound control
            # action. Quality measurement itself never opens paid gates.
            with prepare_installed_evidence(db_path), connect(db_path) as connection, transaction(connection), evidence_boundary(connection):
                qualification = capture_release.maintain_operation_qualifications(
                    connection, at=at or now_utc(),
                    mirror_root=db_path.resolve().parent / "current-hold-control")
            return {**result, "qualification_maintenance": qualification}
        if kind == "archive":
            return capture_quality.local_retention_tick(db_path=db_path, at=timestamp)
        if kind == "reconcile":
            day = datetime.fromisoformat(timestamp.replace("Z", "+00:00")).astimezone(BEIJING).date()
            return capture_quality.reconcile_day(business_day=(day-timedelta(days=1)).isoformat(),
                                                 db_path=db_path, at=timestamp)
    raise ValueError("unknown capture v25 job")


def install_pipeline_jobs(scheduler, *, db_path: Path, reports_root: Path, call_override=None,
                          authorization_runner=None, authorization_effective_date: date | None = None) -> None:
    from apscheduler.triggers.cron import CronTrigger  # type: ignore[import-untyped]
    from apscheduler.triggers.interval import IntervalTrigger  # type: ignore[import-untyped]
    from .scheduler import _report_reconcile_job, _douyin_openapi_live_job, _douyin_openapi_reconcile_guard_job
    from .media_retention import install_lifecycle_jobs

    # Remove only explicitly retired registrations, never historic DB receipts.
    for job in list(scheduler.get_jobs()):
        if job.id in RETIRED_JOB_IDS:
            scheduler.remove_job(job.id)
    for key, (job_id, hour, minute, weekday) in CRON_ROUNDS.items():
        control_job = key in CONTROL_CRON_REGISTRATIONS
        scheduler.add_job(dispatch, CronTrigger(hour=hour, minute=minute, day_of_week=weekday, timezone=BEIJING),
                          id=key, replace_existing=True, kwargs={"job_id": job_id, "registration_id": key, "db_path": db_path, "reports_root": reports_root, "call_override": call_override, "automatic_from": authorization_effective_date},
                          coalesce=True, max_instances=1,
                          misfire_grace_time=None if control_job else 3600,
                          executor=(SCHEDULER_REPORT_EXECUTOR if job_id in {"daily_report", "weekly_report"}
                                    else SCHEDULER_CONTROL_EXECUTOR if control_job else "default"))
    for key, minutes in (("content_pipeline", 5), ("comments_refresh", 5), ("pipeline_reconcile", 5)):
        scheduler.add_job(dispatch, IntervalTrigger(minutes=minutes, timezone=BEIJING), id=key, replace_existing=True,
                          kwargs={"job_id": key, "db_path": db_path, "reports_root": reports_root, "call_override": call_override, "automatic_from": authorization_effective_date},
                          coalesce=True, max_instances=1, misfire_grace_time=None,
                          executor=SCHEDULER_RECONCILE_EXECUTOR if key == "pipeline_reconcile" else "default",
                          next_run_time=datetime.now(BEIJING))
    scheduler.add_job(_report_reconcile_job, IntervalTrigger(hours=1, timezone=BEIJING), id="report_reconcile", replace_existing=True,
                      kwargs={"db_path": db_path, "reports_root": reports_root,
                              "effective_from": authorization_effective_date}, coalesce=True, max_instances=1,
                      misfire_grace_time=None, executor=SCHEDULER_REPORT_EXECUTOR,
                      next_run_time=datetime.now(BEIJING))
    scheduler.add_job(_douyin_openapi_live_job, CronTrigger(hour=2, minute=0, timezone=BEIJING),
                      id="douyin_openapi_reconcile", replace_existing=True,
                      kwargs={"db_path": db_path, "runner": authorization_runner},
                      coalesce=True, max_instances=1, misfire_grace_time=3600,
                      executor=SCHEDULER_CONTROL_EXECUTOR)
    scheduler.add_job(_douyin_openapi_reconcile_guard_job, IntervalTrigger(hours=1, timezone=BEIJING),
                      id="douyin_openapi_reconcile_guard", replace_existing=True,
                      kwargs={"db_path": db_path, "runner": authorization_runner,
                              "effective_from": authorization_effective_date or datetime.now(BEIJING).date()},
                      coalesce=True, max_instances=1, misfire_grace_time=None,
                      executor=SCHEDULER_CONTROL_EXECUTOR,
                      next_run_time=datetime.now(BEIJING))
    install_lifecycle_jobs(scheduler, db_path=db_path)
    with connect(db_path) as connection:
        integrated_schema = connection.execute("PRAGMA user_version").fetchone()[0] in {20, 21}
    if integrated_schema:
        scheduler.add_job(run_local_content_analysis, IntervalTrigger(minutes=5, timezone=BEIJING),
            id=LOCAL_ANALYSIS_JOB, replace_existing=True,
            kwargs={"db_path": db_path, "automatic_from": authorization_effective_date},
            coalesce=True, max_instances=1, misfire_grace_time=None, executor="default",
            next_run_time=datetime.now(BEIJING))
        for kind, seconds, executor in (
            ("plan", 300, SCHEDULER_CONTROL_EXECUTOR),
            ("execute", 10, "default"),
            ("maintenance", 300, SCHEDULER_RECONCILE_EXECUTOR),
            ("archive", 300, SCHEDULER_RECONCILE_EXECUTOR),
        ):
            scheduler.add_job(_capture_v25_job, IntervalTrigger(seconds=seconds, timezone=BEIJING),
                id="capture_v25_"+kind, replace_existing=True,
                kwargs={"kind": kind, "db_path": db_path, "automatic_from": authorization_effective_date},
                coalesce=True, max_instances=1, misfire_grace_time=None, executor=executor,
                next_run_time=datetime.now(BEIJING))
        scheduler.add_job(_capture_v25_job, CronTrigger(hour=1, minute=50, timezone=BEIJING),
            id="capture_v25_reconcile", replace_existing=True,
            kwargs={"kind": "reconcile", "db_path": db_path, "automatic_from": authorization_effective_date},
            coalesce=True, max_instances=1, misfire_grace_time=None, executor=SCHEDULER_RECONCILE_EXECUTOR)
