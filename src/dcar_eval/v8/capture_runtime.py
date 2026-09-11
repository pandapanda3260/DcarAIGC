"""Schema20 five-minute planner and one-request executor.

Planning freezes the accepted roster/cohort and creates no provider traffic.
Execution reuses the existing paid boundary and provider parsing/storage code.
Every invocation handles at most one page/request, retaining its logical due
and cursor across restarts. No route, policy or worker token enters paid identity.
"""
from __future__ import annotations

import json
import math
import sqlite3
from datetime import date, datetime, timedelta, timezone
from concurrent.futures import ThreadPoolExecutor
from functools import partial
from pathlib import Path
from typing import Any, Callable, Collection, Iterator, Mapping, Sequence
from contextvars import copy_context
from contextlib import contextmanager, nullcontext
from threading import Event, Thread
from zoneinfo import ZoneInfo

from . import account_metrics, capture, capture_planning as planning, durable_runs, providers, raw_archive, tikhub_scan
from .profile_activations import activation_at
from .provider_budget import TIKHUB_NETWORK_CONCURRENCY, paid_scope
from .storage import DEFAULT_DB, connect, now_utc, transaction
from .work_readiness import WorkReadinessPass
from .source_routing import load_policy
from .automatic_scope import within_automatic_scope
from .content_scope import canonical_content_predicate

CONTRACT = "capture-runtime-v1"
JOB = "capture_integrated_work"
BEIJING = ZoneInfo("Asia/Shanghai")
MAX_PAGES = 32
TASK_CAP_USD = 50.0

RETRY_BACKOFF_BASE_SECONDS = 300
RETRY_BACKOFF_MAX_SECONDS = 6 * 60 * 60
RETRY_ALERT_FAILURES = 6


def retry_backoff(consecutive_failures: int) -> timedelta:
    """Cap before exponentiation; successful pages are not failures."""
    exponent = min(max(int(consecutive_failures), 1) - 1, 7)
    return timedelta(seconds=min(RETRY_BACKOFF_BASE_SECONDS * (2 ** exponent), RETRY_BACKOFF_MAX_SECONDS))


def _retry_schedule(connection: sqlite3.Connection, *, work: Mapping[str, Any],
                    claim: durable_runs.DurableClaim, final_state: str,
                    reason: str, at: str) -> tuple[str, int]:
    """Keep retry metadata in the durable checkpoint, outside the paid identity.

    Charge-day children already inherit the previous checkpoint. Older attempts
    without this field start at zero; total lifetime attempt_count is not evidence
    of consecutive failures and must never be substituted.
    """
    row = connection.execute("SELECT details_json FROM scheduler_runs WHERE id=?", (claim.scheduler_run_id,)).fetchone()
    checkpoint = json.loads(row[0])["checkpoint"]
    previous = checkpoint.get("capture_consecutive_failures", 0)
    if type(previous) is not int or previous < 0:
        raise ValueError("invalid capture retry checkpoint")
    if final_state in {"terminal", "runnable"}:
        return str(work["due_at"]), 0
    if final_state not in {"provider_blocked", "budget_deferred"}:
        return str(work["due_at"]), previous
    failures = previous + 1
    if failures >= RETRY_ALERT_FAILURES:
        connection.execute(
            "INSERT OR IGNORE INTO operational_alerts(dedupe_key,severity,scope_json,evidence_json,owner,status,opened_at) "
            "VALUES(?,'P2',?,?,'capture-runtime','open',?)",
            (f"capture-stuck:{work['id']}",
             planning.canonical({"work_id": work["id"], "account_id": work["account_id"], "operation": work["operation"]}),
             planning.canonical({"consecutive_failures": failures, "state": final_state, "reason": reason}),
             planning.timestamp(at)))
    return planning.timestamp((_time(at) + retry_backoff(failures)).isoformat()), failures


def execution_profile_allowed(active: Mapping[str, Any] | None) -> bool:
    return active is not None and active["profile_id"] == "integrated_route_v1"


def _time(at: str) -> datetime:
    return datetime.fromisoformat(planning.timestamp(at).replace("Z", "+00:00"))


def _stamp(at: datetime) -> str:
    return at.astimezone(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")


def _require20(connection: sqlite3.Connection) -> None:
    if connection.execute("PRAGMA user_version").fetchone()[0] not in {20, 21}:
        raise ValueError("capture runtime requires schema20")


def _business_day(at: str) -> str:
    return _time(at).astimezone(BEIJING).date().isoformat()


def _bucket(at: str, interval: int, phase: int = 0) -> str:
    seconds = int(_time(at).timestamp())
    return _stamp(datetime.fromtimestamp(((seconds-phase)//interval)*interval+phase, timezone.utc))


def _cohort_plan(connection: sqlite3.Connection, active: dict[str, Any], *, at: str,
                 shadow: bool) -> dict[str, Any]:
    local = _time(at).astimezone(BEIJING)
    plan_day = local.date() - timedelta(days=int((local.hour, local.minute) < (0, 10)))
    binding = {key: active[key] for key in ("activation_id", "profile_id", "activation_sha256",
                                           "roster_snapshot_id", "roster_members_sha256")}
    from . import account_catalog_capture as catalog
    policy = catalog.installed_policy(connection, at=at)
    snapshot = catalog.freeze_snapshot(connection, policy=policy) if policy is not None else None
    if snapshot is not None:
        binding["catalog_snapshot_sha256"] = snapshot["snapshot_sha256"]
        binding["catalog_mode"] = "shadow" if shadow else "active"
        if not shadow:
            catalog.synchronize_enabled(connection, snapshot, activation_id=active["activation_id"], at=at)
    binding_hash = planning.digest(binding)
    connection.execute("INSERT OR IGNORE INTO routing_input_changes(change_kind,roster_snapshot_id,payload_json,effective_at,recorded_at,change_sha256) VALUES('roster',?,?,?,?,?)",
                       (active["roster_snapshot_id"], planning.canonical(binding), planning.timestamp(at), planning.timestamp(at), binding_hash))
    change_id = connection.execute("SELECT id FROM routing_input_changes WHERE change_sha256=?", (binding_hash,)).fetchone()[0]
    previous = connection.execute("SELECT * FROM capture_source_plans WHERE roster_change_id=? AND business_day=? ORDER BY generation DESC LIMIT 1", (change_id, plan_day.isoformat())).fetchone()
    if previous is not None:
        return {"id": previous["id"], **json.loads(previous["payload_json"])}
    end = datetime.combine(plan_day, datetime.min.time(), BEIJING)
    start = end - timedelta(days=7)
    if snapshot is not None:
        members = []
        for member in snapshot["eligibility"]["eligible_members"]:
            count = connection.execute("SELECT count(*) FROM content_items WHERE account_id=? AND platform=? "
                "AND content_type='video' AND julianday(published_at)>=julianday(?) AND julianday(published_at)<julianday(?)",
                (member["account_id"], member["platform"], _stamp(start), _stamp(end))).fetchone()[0]
            members.append({**member, "video_count": count})
    else:
        members = connection.execute(
        """SELECT i.id identity_id,i.account_id,i.platform,i.uid,i.created_at,a.enabled,
           m.monitoring_status,(SELECT MIN(s.accepted_at) FROM account_roster_members rm
             JOIN account_roster_snapshots s ON s.id=rm.snapshot_id
             WHERE rm.account_identity_id=i.id) accepted_at,
           (SELECT count(*) FROM content_items c WHERE c.account_id=i.account_id
             AND c.platform=i.platform AND c.content_type='video'
             AND julianday(c.published_at)>=julianday(?) AND julianday(c.published_at)<julianday(?)) video_count
           FROM account_roster_members m JOIN account_platform_identities i ON i.id=m.account_identity_id
           JOIN accounts a ON a.id=i.account_id WHERE m.snapshot_id=? ORDER BY i.id""",
        (_stamp(start), _stamp(end), active["roster_snapshot_id"])).fetchall()
    inputs: list[Mapping[str, Any]] = [{**dict(row), "history_days": max(0, min(7, (end-_time(row["created_at"])).days))}
              for row in members if row["platform"] in {"douyin", "xiaohongshu"} and row["uid"]]
    cohort = planning.adaptive_cohorts(inputs, business_day=plan_day.isoformat())
    payload = {"contract_version": CONTRACT, **binding, "business_day": plan_day.isoformat(),
               "count_start": _stamp(start), "count_end_exclusive": _stamp(end),
               "cohort": cohort, "shadow": shadow}
    if snapshot is not None:
        payload["catalog_snapshot"] = snapshot
    cursor = connection.execute("INSERT INTO capture_source_plans(roster_change_id,business_day,generation,mode,payload_json,created_at,plan_sha256) VALUES(?,?,1,?,?,?,?)",
        (change_id, plan_day.isoformat(), "shadow" if shadow else "active", planning.canonical(payload), planning.timestamp(at), planning.digest(payload)))
    return {"id": cursor.lastrowid, **payload}


def _reason_state(reason: str) -> str:
    if ("identity" in reason or "billing_unknown" in reason or reason in {
            "cursor_loop", "page_cap_hit", "invalid_items", "content_type_unverified",
            "invalid_response", "invalid_total", "raw_response_integrity_error"}):
        return "paid_identity_hold"
    if "budget" in reason or "quota" in reason:
        return "budget_deferred"
    return "provider_blocked"


def _record_readiness_block(connection: sqlite3.Connection, work: Mapping[str, Any], *,
                            state: str, reason: str, at: str) -> None:
    """Delay another readiness check, never authorize an attempt or clear a HOLD.

    This is not an execution failure: no claim/checkpoint/streak is created.
    Permanent authorization/route blocks remain blocked until their own checks
    pass. Paid identity holds are excluded from automatic reconsideration.
    """
    due = (planning.timestamp((_time(at) + timedelta(seconds=RETRY_BACKOFF_BASE_SECONDS)).isoformat())
           if state in {"provider_blocked", "budget_deferred"} else work["due_at"])
    connection.execute("UPDATE capture_work_items SET state=?,reason=?,due_at=?,updated_at=? WHERE id=?",
        (state, reason, due, planning.timestamp(at), work["id"]))


def _readiness(connection: sqlite3.Connection, envelope: dict[str, Any], *, at: str,
               readiness_pass: WorkReadinessPass | None = None) -> tuple[str, str]:
    command_id = envelope.get("manual_command_run_id")
    if command_id is not None:
        from . import capture_manual
        try:
            specification = capture_manual.validate_command(connection, command_id, content_id=envelope["content_id"],
                operation=envelope["operation"], stage=envelope["capture_stage"])
            if any(envelope.get(key) != specification[key] for key in ("task_id", "task_max_amount")):
                return "paid_identity_hold", "manual_task_budget_changed"
            assignment = capture_manual.assignment_for_command(connection, command_id,
                content_id=envelope["content_id"], operation=envelope["operation"], at=at)
        except (ValueError, RuntimeError) as error:
            return "paid_identity_hold", str(getattr(error, "error_code", type(error).__name__))
    elif envelope.get("catalog_plan_id") is not None:
        from . import account_catalog_capture as catalog
        try:
            assignment = catalog.assignment_for_plan(connection, envelope["catalog_plan_id"],
                identity_id=envelope["identity_id"], operation=envelope["operation"], at=at)
        except (ValueError, RuntimeError) as error:
            return "provider_blocked", str(getattr(error, "error_code", type(error).__name__))
    else:
        assignment = planning.resolve_route(connection, account_id=envelope["account_id"],
            content_id=envelope.get("content_id"), operation=envelope["operation"], at=at)
    if (assignment is None or assignment["id"] != envelope["assignment_id"]
            or assignment["mode"] != "active" or assignment["route"] != "integrated"):
        return "provider_blocked", "route_generation_conflict"
    if envelope["stage"] == "discovery" and envelope["operation"] == "douyin_user_posts":
        ref = connection.execute("SELECT reference_value FROM account_provider_references WHERE account_identity_id=? AND provider='TikHub' COLLATE NOCASE AND reference_kind='sec_user_id'",
                                 (envelope["identity_id"],)).fetchone()
        if ref is None or not providers._valid_douyin_sec_user_id(str(ref[0])):
            return "provider_blocked", "reference_profile_required"
    gate = connection.execute("SELECT state FROM capture_paid_send_gate_events WHERE provider='tikhub' AND operation=? AND julianday(recorded_at)<=julianday(?) ORDER BY id DESC LIMIT 1",
                              (envelope["operation"], planning.timestamp(at))).fetchone()
    # This is planning readiness, not HTTP permission. Candidate work must be
    # allowed to reach A/B, where the immutable twenty-request allowlist and
    # actual installed-runtime bindings are verified on every physical send.
    if gate is None or gate[0] not in {"open", "diagnostic_only"}:
        return "provider_blocked", "provider_transport_blocked"
    if envelope.get("compensation"):
        from .capture_compensation import readiness

        return readiness(connection, envelope, at=at)
    current_pass = readiness_pass if readiness_pass is not None else WorkReadinessPass(connection, at=at)
    with paid_scope(envelope["category"], manual_command_run_id=command_id,
                    catalog_plan_id=envelope.get("catalog_plan_id")):
        assessment = current_pass.assess(operation=envelope["operation"],
            category=envelope["category"], account_id=envelope["account_id"],
            content_id=envelope.get("content_id"), identity_id=envelope["identity_id"],
            stage=envelope["capture_stage"], window_key=_page_window(envelope),
            manual_command_run_id=command_id)
    return ("runnable", "") if assessment["runnable"] else (_reason_state(assessment["reason"]), assessment["reason"])


def _page_window(envelope: dict[str, Any]) -> str:
    due = envelope["logical_due"]
    if envelope["stage"] in {"discovery", "comments"}:
        return due + ":cursor:" + planning.digest(envelope.get("cursor"))[:24]
    return due


def _discovery_pending(connection: sqlite3.Connection, plan: Mapping[str, Any],
                       member: Mapping[str, Any], *, operation: str,
                       logical_due: str, at: str) -> bool:
    """Isolate an old held scan only from a strictly later natural business day.

    A HOLD remains immutable and cannot be retried here. A capped/cyclic
    terminal scan also blocks another purchase that day. All other unfinished
    work still owns the account's scan. A new day must have its own frozen
    cohort and actual phased due bucket; crossing midnight alone is insufficient.
    """
    pending = connection.execute(
        "SELECT * FROM capture_work_items WHERE account_id=? AND operation=? "
        "AND (state!='terminal' OR reason IN ('page_cap_hit','cursor_loop'))",
        (member["account_id"], operation)).fetchall()
    if not pending:
        return False
    if any(row["state"] != "paid_identity_hold" and not (
            row["state"] == "terminal" and row["reason"] in {"page_cap_hit", "cursor_loop"})
            for row in pending):
        return True
    try:
        interval, phase = member["interval_minutes"], member["phase_seconds"]
        day = _time(at).astimezone(BEIJING).date()
        if (type(interval) is not int or interval not in {60, 120, 180}
                or type(phase) is not int or not 0 <= phase < interval * 60
                or operation != member["platform"] + "_user_posts"
                or plan["business_day"] != day.isoformat()
                or member not in plan["cohort"]
                or logical_due != "discovery:" + _bucket(at, interval * 60, phase)):
            return True
        bucket = _time(logical_due.removeprefix("discovery:"))
        if bucket.astimezone(BEIJING).date() != day:
            return True
        for row in pending:
            old = json.loads(row["envelope_json"])
            old_due = old["logical_due"]
            old_bucket = _time(old_due.removeprefix("discovery:"))
            old_day = date.fromisoformat(row["data_business_day"])
            old_end = _time(old["window_end"])
            identity = planning.digest({"provider": "tikhub", "operation": operation,
                "subject": f"account:{member['identity_id']}", "logical_due": old_due})
            if (row["provider"] != "tikhub" or row["content_id"] is not None
                    or row["owner_token"] is not None or old.get("kind") is not None
                    or "compensation" in old or old["contract_version"] != CONTRACT
                    or old["stage"] != "discovery" or old["capture_stage"] != "discovery"
                    or old["source_stage"] != "discovery" or row["source_plan_id"] is None
                    or old["category"] != "reconcile" or old["content_id"] is not None
                    or old["operation"] != operation or old["assignment_id"] != row["assignment_id"]
                    or old["source_plan_id"] != row["source_plan_id"]
                    or any(old[key] != member[key] for key in ("identity_id", "account_id", "platform", "uid"))
                    or row["work_identity"] != identity
                    or old_due != "discovery:" + _stamp(old_bucket)
                    or row["data_business_day"] != old_day.isoformat()
                    or day <= old_day or day <= old_bucket.astimezone(BEIJING).date()
                    or day <= old_end.astimezone(BEIJING).date()
                    or day <= _time(row["updated_at"]).astimezone(BEIJING).date()
                    or _time(old["window_start"]) >= old_end or old_bucket > old_end):
                return True
    except (KeyError, TypeError, ValueError, AttributeError, OverflowError):
        # Ambiguous dates or identities cannot establish a new natural cycle.
        return True
    return False


def _enqueue(connection: sqlite3.Connection, plan: dict[str, Any], member: dict[str, Any], *,
             stage: str, operation: str, logical_due: str, at: str, content: sqlite3.Row | None = None,
             source_stage: str | None = None, readiness_pass: WorkReadinessPass | None = None,
             manual_command_run_id: int | None = None) -> bool:
    specification = None
    if manual_command_run_id is not None:
        from . import capture_manual
        if content is None:
            raise ValueError("manual content command requires a content target")
        specification = capture_manual.validate_command(connection, manual_command_run_id,
            content_id=content["id"], operation=operation, stage=stage)
        assignment = capture_manual.assignment_for_command(connection, manual_command_run_id,
            content_id=content["id"], operation=operation, at=at, create=True)
    elif plan.get("catalog_snapshot") is not None:
        from . import account_catalog_capture as catalog
        assignment = catalog.assignment_for_plan(connection, plan["id"], identity_id=member["identity_id"],
            operation=operation, at=at, create=True)
    else:
        assignment = planning.resolve_route(connection, account_id=member["account_id"],
            content_id=content["id"] if content else None, operation=operation, at=at)
    if assignment is None or assignment["route"] != "integrated" or assignment["mode"] != "active":
        return False
    account_stage = stage in {"discovery", "account_metrics"}
    envelope = {"contract_version": CONTRACT, "identity_id": member["identity_id"],
        "account_id": member["account_id"], "content_id": content["id"] if content else None,
        "platform": member["platform"], "uid": member["uid"], "stage": stage,
        "capture_stage": "discovery" if account_stage else stage,
        "category": "reconcile" if stage == "discovery" else "metrics" if stage == "account_metrics" else stage,
        "operation": operation, "logical_due": logical_due,
        "source_stage": source_stage or stage,
        "assignment_id": assignment["id"], "source_plan_id": plan["id"],
        "activation_id": plan["activation_id"], "profile_id": plan["profile_id"],
        "roster_snapshot_id": plan["roster_snapshot_id"], "roster_members_sha256": plan["roster_members_sha256"],
        "cursor": 0 if member["platform"] == "douyin" else "", "page_count": 0,
        "raw_ids": [], "seen_cursors": [], "counts": {"seen": 0, "valid": 0, "missing": 0, "invalid": 0, "unavailable": 0}}
    if specification is not None:
        envelope.update(kind=specification["kind"], manual_command_run_id=manual_command_run_id,
            manual_command_run_ids=[manual_command_run_id], task_id=specification["task_id"],
            task_max_amount=specification["task_max_amount"])
    elif plan.get("catalog_snapshot") is not None:
        envelope["catalog_plan_id"] = plan["id"]
    identity = planning.digest({"provider": "tikhub", "operation": operation,
                               "subject": f"content:{content['id']}" if content else f"account:{member['identity_id']}",
                               "logical_due": logical_due})
    if connection.execute("SELECT 1 FROM capture_work_items WHERE work_identity=?", (identity,)).fetchone():
        return False
    if stage == "discovery":
        if content is not None or _discovery_pending(connection, plan, member,
                operation=operation, logical_due=logical_due, at=at):
            return False
    elif stage == "metrics" and manual_command_run_id is None:
        from .capture_metric_cycles import metric_cycle_pending
        if content is None or metric_cycle_pending(connection, plan, member, content,
                operation, source_stage, logical_due, at):
            return False
    elif not (specification is not None and specification["kind"] == "metrics_update") and connection.execute("SELECT 1 FROM capture_work_items WHERE account_id=? AND content_id IS ? AND operation=? AND state!='terminal' LIMIT 1",
                            (member["account_id"], envelope["content_id"], operation)).fetchone():
        return False
    if stage == "discovery":
        old = connection.execute("SELECT complete_through FROM capture_watermarks WHERE provider='tikhub' AND operation=? AND scope_key=? ORDER BY complete_through DESC LIMIT 1",
                                 (operation, f"{member['platform']}:{member['uid']}")).fetchone()
        envelope["window_start"], envelope["window_end"] = planning.discovery_window(at=at, complete_through=old[0] if old else None, forward_only=True)
    state, reason = _readiness(connection, envelope, at=at, readiness_pass=readiness_pass)
    cursor = connection.execute("""INSERT OR IGNORE INTO capture_work_items(work_identity,assignment_id,source_plan_id,
        account_id,content_id,provider,operation,due_at,data_business_day,state,reason,envelope_json,created_at,updated_at)
        VALUES(?,?,?,?,?,'tikhub',?,?,?,?,?,?,?,?)""",
        (identity, assignment["id"], plan["id"], member["account_id"], envelope["content_id"], operation,
         planning.timestamp(at), _business_day(at), state, reason, planning.canonical(envelope), planning.timestamp(at), planning.timestamp(at)))
    return cursor.rowcount == 1


def plan_tick(db_path: Path = DEFAULT_DB, at: str | None = None, shadow: bool = False) -> dict[str, Any]:
    """Freeze one daily plan and idempotently enqueue currently due scopes."""
    at = at or now_utc()
    with connect(db_path) as connection, transaction(connection):
        _require20(connection)
        active = activation_at(connection, at)
        if active is None:
            return {"status": "no_activation", "created": 0, "provider_calls": 0}
        shadow = shadow or active["profile_id"] != "integrated_route_v1"
        plan = _cohort_plan(connection, active, at=at, shadow=shadow)
        if shadow:
            return {"status": "shadow", "plan_id": plan["id"], "accounts": len(plan["cohort"]), "created": 0, "provider_calls": 0}
        from . import account_catalog_capture as catalog
        snapshot = plan.get("catalog_snapshot")
        policy = catalog.installed_policy(connection, at=at) if snapshot is not None else None
        context = catalog.planning_validation(connection, policy, snapshot) if policy is not None else nullcontext()
        with context:
            return _plan_due(connection, plan, at=at)


def _plan_due(connection: sqlite3.Connection, plan: dict[str, Any], *, at: str) -> dict[str, Any]:
    # Planning writes no paid usage: reuse budget/unknown snapshots only
    # within this transaction. Execution and subsequent ticks recheck fresh.
    readiness_pass = WorkReadinessPass(connection, at=at)
    created = 0
    for member in plan["cohort"]:
        platform = member["platform"]
        due = "discovery:" + _bucket(at, member["interval_minutes"]*60, member["phase_seconds"])
        if not _discovery_pending(connection, plan, member,
                operation=platform+"_user_posts", logical_due=due, at=at):
            created += _enqueue(connection, plan, member, stage="discovery", operation=platform+"_user_posts", logical_due=due, at=at,
                                readiness_pass=readiness_pass)
        if platform == "douyin":
            created += _enqueue(connection, plan, member, stage="account_metrics", operation="douyin_uid_profile",
                                logical_due="account-metrics:"+_bucket(at, 6*3600), at=at, readiness_pass=readiness_pass)
        day_start = _stamp(datetime.combine(_time(at).astimezone(BEIJING).date(), datetime.min.time(), BEIJING))
        contents = connection.execute("""SELECT c.*,(EXISTS(SELECT 1 FROM task_contents tc JOIN report_tasks t ON t.id=tc.task_id
            WHERE tc.content_id=c.id AND (t.task_status IN ('queued','running','cancel_requested')
            OR julianday(t.completed_at)>=julianday(?))) OR EXISTS(SELECT 1 FROM capture_work_items w
            WHERE w.content_id=c.id AND json_extract(w.envelope_json,'$.kind')='manual_update'
            AND (w.state!='terminal' OR julianday(w.completed_at)>=julianday(?)))) business_active
            FROM content_items c WHERE c.account_id=? AND c.platform=? AND """ + canonical_content_predicate(connection, alias="c"),
            (day_start, day_start, member["account_id"], platform)).fetchall()
        for content in contents:
            if not within_automatic_scope(content["published_at"] or content["created_at"]):
                continue
            if content["published_at"] and (_time(at)-_time(content["published_at"])).days > 90:
                continue
            detailed = connection.execute("SELECT 1 FROM fetch_slots WHERE content_id=? AND stage='detail' AND window_key='lifetime' AND status='succeeded' LIMIT 1", (content["id"],)).fetchone()
            if detailed is None:
                created += _enqueue(connection, plan, member, stage="detail", operation=providers.STAGE_CONFIG[(platform,"detail")][2],
                                    logical_due="lifetime", at=at, content=content, readiness_pass=readiness_pass)
            if not content["published_at"]:
                continue
            interval = planning.refresh_interval(published_at=content["published_at"], at=at,
                high_value=False, business_active=bool(content["business_active"]))
            if interval is not None:
                for group in load_policy()["metric_supplement_groups"][platform]:
                    source_stage = str(group["stage"])
                    created += _enqueue(connection, plan, member, stage="metrics",
                        operation=providers.STAGE_CONFIG[(platform,source_stage)][2], source_stage=source_stage,
                        logical_due="metrics:"+_bucket(at, interval[0])+":"+str(group["name"]), at=at, content=content,
                        readiness_pass=readiness_pass)
                if content["business_active"]:
                    week = _time(at).astimezone(BEIJING).date().isocalendar()
                    created += _enqueue(connection, plan, member, stage="comments", operation=providers.STAGE_CONFIG[(platform,"comments")][2],
                        logical_due=f"{week.year}-W{week.week:02d}", at=at, content=content, readiness_pass=readiness_pass)
    reconsidered = 0
    for row in connection.execute("SELECT id,state,reason,envelope_json,due_at FROM capture_work_items WHERE state IN ('provider_blocked','budget_deferred') AND due_at<=? ORDER BY due_at,id LIMIT 500", (planning.timestamp(at),)).fetchall():
        state, reason = _readiness(connection, json.loads(row["envelope_json"]), at=at, readiness_pass=readiness_pass)
        if state != "runnable":
            _record_readiness_block(connection, dict(row), state=state, reason=reason, at=at)
            reconsidered += 1
        elif state != row["state"] or reason != row["reason"]:
            connection.execute("UPDATE capture_work_items SET state=?,reason=?,updated_at=? WHERE id=?", (state, reason, planning.timestamp(at), row["id"]))
            reconsidered += 1
    return {"status": "planned", "plan_id": plan["id"], "created": created,
            "reconsidered": reconsidered, "provider_calls": 0}


def _raw_for_page(envelope: dict[str, Any], db_path: Path) -> capture.StoredRawResponse:
    return capture.load_succeeded_raw_response(db_path=db_path, account_id=envelope["account_id"],
        stage="discovery", window_key=_page_window(envelope), operation=envelope["operation"])


def _verify_raws(db_path: Path, raw_ids: list[int]) -> None:
    if not raw_ids or len(raw_ids) > MAX_PAGES:
        raise ValueError("bounded verified raw evidence required")
    with connect(db_path) as connection:
        for raw_id in raw_ids:
            raw_archive.read_response_entity(connection, raw_id)


def _discovery_page(envelope: dict[str, Any], *, db_path: Path, at: str) -> dict[str, Any]:
    result = providers.discover_account_content(envelope["account_id"], envelope["platform"], envelope["uid"],
        as_of=_time(at).astimezone(BEIJING).date(), cursor=envelope["cursor"], window_key=_page_window(envelope),
        published_start=_time(envelope["window_start"]), published_end=_time(envelope["window_end"])-timedelta(microseconds=1),
        task_id="capture-v25:"+_business_day(at), task_max_amount=TASK_CAP_USD, db_path=db_path,
        materialize_discovery_detail=False, materialize_existing_discovery_stages=True)
    raw = _raw_for_page(envelope, db_path)
    items, more, next_cursor, _ = tikhub_scan._page(raw, envelope["platform"])
    counts = dict(envelope["counts"])
    inventory = dict(envelope.get("video_inventory", {}))
    repeated = int(envelope.get("repeated_video_appearances", 0))
    for item in items:
        proof = tikhub_scan._item_evidence(envelope["platform"], item)
        counts["seen"] += 1
        counts["invalid" if proof["event_tuple"] is None else "valid"] += 1
        if proof["event_tuple"] is not None and tikhub_scan._item(envelope["platform"], item)["content_type"] == "video":
            published = proof["published_at"]
            if _time(envelope["window_start"]) <= _time(published) < _time(envelope["window_end"]):
                identifier = proof["platform_content_id"]
                if identifier in inventory:
                    repeated += 1
                else:
                    inventory[identifier] = {"published_at": published, "first_captured_at": raw.captured_at,
                                             "raw_response_id": raw.raw_response_id}
    new = {**envelope, "counts": counts, "page_count": envelope["page_count"]+1,
           "video_inventory": inventory, "repeated_video_appearances": repeated,
           "raw_ids": [*envelope["raw_ids"], raw.raw_response_id],
           "seen_cursors": [*envelope["seen_cursors"], planning.canonical(envelope["cursor"])]}
    _verify_raws(db_path, new["raw_ids"])
    loop = more and (next_cursor is None or planning.canonical(next_cursor) in new["seen_cursors"])
    cap_hit = more and new["page_count"] >= MAX_PAGES
    complete = not more and result["status"] in {"succeeded", "already_succeeded"} and counts["invalid"] == 0
    new["cursor"] = next_cursor
    evidence = {"complete": complete, "terminal_cursor": not more, "all_raw_verified": True,
                "disposition": "complete" if complete else "partial",
                "cap_hit": cap_hit, "cursor_loop": loop, **counts, "raw_response_ids": new["raw_ids"],
                "window_start": envelope["window_start"], "window_end": envelope["window_end"],
                "platform": envelope["platform"], "account_id": envelope["account_id"],
                "identity_id": envelope["identity_id"], "operation": envelope["operation"],
                "video_inventory": inventory, "repeated_video_appearances": repeated,
                "inventory_contract": "verified-provider-scan-inventory-v1"}
    reason = "cursor_loop" if loop else "page_cap_hit" if cap_hit else "invalid_items" if counts["invalid"] else ""
    terminal_partial = bool((loop or cap_hit) and counts["invalid"] == 0
                            and result["status"] in {"succeeded", "already_succeeded"})
    return {"complete": complete, "continuation": more and not reason,
            "terminal_partial": terminal_partial,
            "envelope": new, "evidence": evidence, "reason": reason, "provider_cost": result["provider_cost"]}


def _content_request(envelope: dict[str, Any], *, db_path: Path, at: str) -> dict[str, Any]:
    with connect(db_path) as connection:
        content = dict(connection.execute("SELECT * FROM content_items WHERE id=?", (envelope["content_id"],)).fetchone())
    platform, stage = envelope["platform"], envelope["stage"]
    source_stage = envelope.get("source_stage", stage)
    if platform == "xiaohongshu" and content["content_type"] not in {"video", "image"}:
        raise capture.CaptureError("XHS type requires separately assigned discovery evidence", retryable=False, error_code="content_type_unverified", billed=False)
    cursor = (dict(envelope["cursor"]) if isinstance(envelope["cursor"], dict) else {"cursor": envelope["cursor"]}) if stage == "comments" else None
    request = providers._douyin_request(source_stage, content["platform_content_id"], cursor) if platform == "douyin" else providers._xhs_request(source_stage, content["platform_content_id"], content["content_type"], cursor=cursor)
    params = request[1]
    window = _page_window(envelope)
    price = providers.STAGE_CONFIG[(platform,source_stage)][3]
    def normalize(parsed: capture.ProviderResult) -> capture.ProviderResult:
        if stage == "metrics" and source_stage == "detail":
            if (parsed.data.get("account_uid") and content.get("raw_account_uid")
                    and str(parsed.data["account_uid"]) != str(content["raw_account_uid"])):
                raise capture.CaptureError("counter detail author conflicts with identity", retryable=False,
                    error_code="identity_conflict", billed=parsed.billed, http_status=parsed.http_status,
                    raw_response=parsed.raw_response, entity_bytes=parsed.entity_bytes, transport_receipt=parsed.transport_receipt)
            values = parsed.data.get("metrics")
            if not isinstance(values, Mapping):
                raise capture.CaptureError("counter detail omitted metrics", retryable=False,
                    error_code="invalid_response", billed=parsed.billed, http_status=parsed.http_status,
                    raw_response=parsed.raw_response, entity_bytes=parsed.entity_bytes, transport_receipt=parsed.transport_receipt)
            return capture.ProviderResult(dict(values), parsed.raw_response, parsed.http_status,
                                          parsed.billed, parsed.entity_bytes, parsed.transport_receipt)
        return parsed
    task_id = envelope.get("task_id", "capture-v25:"+_business_day(at))
    task_cap = envelope.get("task_max_amount", TASK_CAP_USD)
    budget = providers._budget_for_call(provider="TikHub", operation=envelope["operation"], price=price,
        task_id=task_id, task_max_amount=task_cap, db_path=db_path)
    try:
        stored = capture.load_succeeded_raw_response(db_path=db_path, content_id=content["id"], stage=stage, window_key=window, operation=envelope["operation"])
    except capture.SlotUnavailable:
        key = providers._load_key(providers.TIKHUB_KEY_FILE, "TIKHUB_API_KEY")
        raw_call = partial(providers._douyin_call, source_stage, content["platform_content_id"], key, cursor=cursor) if platform == "douyin" else partial(providers._xhs_call, source_stage, content["platform_content_id"], key, content["content_type"], cursor=cursor)
        def call() -> capture.ProviderResult:
            return normalize(raw_call())
        outcome = capture.execute_content_fetch(content_id=content["id"], stage=stage, window_key=window,
            provider="TikHub", adapter_version=providers.STAGE_CONFIG[(platform,source_stage)][1], operation=envelope["operation"],
            db_path=db_path, call=call, request_transport=providers._freeze_tikhub_transport(), budget_id=budget,
            task_id=task_id, task_max_amount=task_cap,
            paid_request_identity=providers._paid_request_identity(operation=envelope["operation"], platform=platform,
                subject=content["platform_content_id"], params=params, cursor=cursor, due_bucket=window))
    else:
        parsed = normalize(providers._parse_douyin_stage_payload(source_stage, content["platform_content_id"], stored.value, status=stored.http_status or 200) if platform == "douyin" else providers._parse_xhs_stage_payload(source_stage, content["platform_content_id"], content["content_type"], stored.value, status=stored.http_status or 200))
        outcome = capture.CaptureOutcome(stored.slot_id, 0, stored.raw_response_id, parsed.data, False, 0.0, "USD")
    providers._store_stage_result(content, stage, window, outcome, db_path=db_path)
    with connect(db_path) as connection:
        raw_archive.read_response_entity(connection, outcome.raw_response_id)
    more = stage == "comments" and bool(outcome.data.get("has_more"))
    next_cursor = outcome.data.get("next_cursor_params") if platform == "xiaohongshu" else outcome.data.get("next_cursor")
    seen = [*envelope["seen_cursors"], planning.canonical(envelope["cursor"])]
    loop = more and (next_cursor is None or planning.canonical(next_cursor) in seen)
    cap_hit = more and envelope["page_count"]+1 >= MAX_PAGES
    return {"complete": not more, "continuation": more and not loop and not cap_hit,
        "envelope": {**envelope, "cursor": next_cursor, "seen_cursors": seen,
                     "page_count": envelope["page_count"]+1, "raw_ids": [*envelope["raw_ids"], outcome.raw_response_id]},
        "evidence": {"raw_response_ids": [*envelope["raw_ids"], outcome.raw_response_id], "all_raw_verified": True},
        "reason": "cursor_loop" if loop else "page_cap_hit" if cap_hit else "", "provider_cost": outcome.amount}


def _account_request(envelope: dict[str, Any], *, db_path: Path, at: str) -> dict[str, Any]:
    # The existing verified Douyin profile operation supplies both a reference
    # and follower statistics; no XHS profile contract is enabled implicitly.
    operation = "douyin_uid_profile"
    window = envelope["logical_due"]
    budget = providers._budget_for_call(provider="TikHub", operation=operation, price=providers.TIKHUB_PRICE,
        task_id="capture-v25:"+_business_day(at), task_max_amount=TASK_CAP_USD, db_path=db_path)
    try:
        raw = capture.load_succeeded_raw_response(db_path=db_path, account_id=envelope["account_id"], stage="discovery", window_key=window, operation=operation)
        raw_id, value, cost, captured_at = raw.raw_response_id, raw.value, 0.0, raw.captured_at
    except capture.SlotUnavailable:
        key = providers._load_key(providers.TIKHUB_KEY_FILE, "TIKHUB_API_KEY")
        outcome = capture.execute_account_fetch(account_id=envelope["account_id"], stage="discovery", window_key=window,
            provider="TikHub", adapter_version="tikhub-profile-v25", operation=operation,
            call=partial(providers._douyin_reference_call, envelope["uid"], key),
            request_transport=providers._freeze_tikhub_transport(), db_path=db_path, budget_id=budget,
            task_id="capture-v25:"+_business_day(at), task_max_amount=TASK_CAP_USD,
            paid_request_identity=providers._paid_request_identity(operation=operation, platform="douyin", subject=envelope["uid"],
                params={"uid": envelope["uid"]}, cursor=None, due_bucket=window))
        raw_id, cost = outcome.raw_response_id, outcome.amount
        with connect(db_path) as connection:
            value = json.loads(raw_archive.read_response_entity(connection, raw_id))
            captured_at = connection.execute("SELECT captured_at FROM provider_raw_responses WHERE id=?", (raw_id,)).fetchone()[0]
    normalized = account_metrics.parse_tikhub_profile(value, platform="douyin", uid=envelope["uid"])
    follower_status = normalized["field_status"]["follower_count"]["status"]
    if follower_status != "provided":
        raise account_metrics.AccountMetricError("profile_follower_count_" + follower_status)
    reference = providers._parse_douyin_reference_payload(value, status=200).data.get("reference")
    with connect(db_path) as connection, transaction(connection):
        account_metrics.persist_account_metric_observation(connection, account_identity_id=envelope["identity_id"],
            provider="tikhub", raw_response_id=raw_id, normalized=normalized, captured_at=captured_at)
        if providers._valid_douyin_sec_user_id(str(reference or "")):
            connection.execute("INSERT INTO account_provider_references(account_identity_id,provider,reference_kind,reference_value,source_raw_response_id,created_at,updated_at) VALUES(?,'TikHub','sec_user_id',?,?,?,?) ON CONFLICT(account_identity_id,provider,reference_kind) DO UPDATE SET reference_value=excluded.reference_value,source_raw_response_id=excluded.source_raw_response_id,updated_at=excluded.updated_at",
                               (envelope["identity_id"], reference, raw_id, captured_at, captured_at))
    return {"complete": True, "continuation": False, "envelope": envelope,
            "evidence": {"raw_response_ids": [raw_id], "all_raw_verified": True}, "reason": "", "provider_cost": cost}


def _execute_one(envelope: dict[str, Any], *, db_path: Path, at: str) -> dict[str, Any]:
    if envelope["stage"] == "discovery":
        return _discovery_page(envelope, db_path=db_path, at=at)
    if envelope["stage"] == "account_metrics":
        return _account_request(envelope, db_path=db_path, at=at)
    return _content_request(envelope, db_path=db_path, at=at)


def recover_capture_leases(connection: sqlite3.Connection, *, at: str) -> int:
    """Recover only real capture work, never legacy scanner/command owners.

    Match the immutable work identity and checkpoint id, including charge-day
    children. Do not require the work's mutable owner token: a crash may occur
    after the durable claim and before that token is written to the work row.
    """
    if connection.execute("PRAGMA user_version").fetchone()[0] not in {20, 21}:
        return 0
    owned = [int(row[0]) for row in connection.execute(
        "SELECT r.id FROM scheduler_runs r JOIN capture_work_items w "
        "ON w.work_identity=json_extract(r.details_json,'$.identity.work_identity') "
        "AND w.id=json_extract(r.details_json,'$.checkpoint.work_id') "
        "WHERE r.job_id=? AND r.status='running'", (JOB,),
    )]
    return durable_runs.recover_expired_leases(connection, now=at, owned_run_ids=owned)


def _recover_work(connection: sqlite3.Connection, *, at: str) -> int:
    recover_capture_leases(connection, at=at)
    recovered = connection.execute("""UPDATE capture_work_items SET state='runnable',reason='lease_recovered',
        owner_token=NULL,heartbeat_at=NULL,lease_expires_at=NULL,updated_at=?
        WHERE state IN ('leased','running') AND lease_expires_at<=?
        AND NOT EXISTS(SELECT 1 FROM scheduler_run_attempts a
          WHERE a.owner_token=capture_work_items.owner_token AND a.status='running')""",
        (planning.timestamp(at), planning.timestamp(at)))
    return recovered.rowcount


def _renew_work_lease(connection: sqlite3.Connection, work_id: int | Sequence[int],
                      claim: durable_runs.DurableClaim, *, at: str) -> None:
    """Renew both fences atomically; an expired or replaced owner cannot revive."""
    work_ids = (work_id,) if isinstance(work_id, int) else tuple(work_id)
    if not work_ids or len(set(work_ids)) != len(work_ids) or any(type(value) is not int or value < 1 for value in work_ids):
        raise ValueError("capture heartbeat requires unique positive work IDs")
    durable_runs.assert_owner(connection, claim)
    durable_runs.heartbeat(connection, claim, now=at)
    timestamp = planning.timestamp(at)
    expires = planning.timestamp((_time(at) + timedelta(seconds=durable_runs.LEASE_SECONDS)).isoformat())
    placeholders = ",".join("?" for _ in work_ids)
    updated = connection.execute(f"""UPDATE capture_work_items SET heartbeat_at=?,lease_expires_at=?
        WHERE id IN ({placeholders}) AND state='running' AND owner_token=? AND lease_expires_at>=?""",
        (timestamp, expires, *work_ids, claim.owner_token, timestamp))
    if updated.rowcount != len(work_ids):
        raise durable_runs.LostOwnership("capture work lease expired or changed owner")


@contextmanager
def _maintain_work_lease(db_path: Path, work_id: int | Sequence[int],
                         claim: durable_runs.DurableClaim) -> Iterator[Callable[[], None]]:
    """Keep this live worker fenced while provider preparation and HTTP run."""
    stopped = Event()
    failures: list[Exception] = []

    def check() -> None:
        if failures:
            raise durable_runs.LostOwnership("capture worker could not maintain ownership") from failures[0]

    def renew() -> None:
        with connect(db_path) as connection, transaction(connection, priority="heartbeat"):
            if not stopped.is_set():
                # Read the clock after acquiring the writer transaction lock.
                _renew_work_lease(connection, work_id, claim, at=now_utc())

    def maintain() -> None:
        while not stopped.wait(durable_runs.HEARTBEAT_SECONDS):
            try:
                renew()
            except Exception as error:
                failures.append(error)
                return

    renew()
    worker = Thread(target=maintain, name=f"capture-lease-{work_id}", daemon=True)
    worker.start()
    try:
        yield check
    finally:
        stopped.set()
        # A queued heartbeat rechecks stopped after obtaining the lock; it can
        # neither revive a finished attempt nor delay worker shutdown on a lock.
        worker.join(timeout=1)


def _claim_work(connection: sqlite3.Connection, work: dict[str, Any], *, at: str) -> durable_runs.DurableClaim | None:
    identity = {"work_identity": work["work_identity"], "business_day": work["data_business_day"]}
    catalog_plan_id = json.loads(work["envelope_json"]).get("catalog_plan_id")
    if catalog_plan_id is not None:
        identity["catalog_plan_id"] = catalog_plan_id
    scheduled = "scan:" + durable_runs.scan_identity(JOB, identity)
    root = connection.execute("SELECT id FROM scheduler_runs WHERE job_id=? AND scheduled_for=? AND root_run_id IS NULL",
                              (JOB, scheduled)).fetchone()
    today = _business_day(at)
    if root is not None and today != work["data_business_day"]:
        child = connection.execute("SELECT continuation_sequence FROM scheduler_runs WHERE root_run_id=? AND charge_business_day=? ORDER BY continuation_sequence DESC LIMIT 1",
                                   (root[0], today)).fetchone()
        sequence = child[0] if child else connection.execute("SELECT COALESCE(MAX(continuation_sequence),0)+1 FROM scheduler_runs WHERE root_run_id=?", (root[0],)).fetchone()[0]
        return durable_runs.claim_child(connection, root_run_id=root[0], continuation_sequence=sequence,
                                        charge_business_day=today, now=at)
    return durable_runs.claim_run_in_transaction(connection, JOB, identity, now=at,
        initial_checkpoint={"complete": False, "work_id": work["id"]})


def _attempt_usage(connection: sqlite3.Connection, attempt_id: int) -> tuple[float, int]:
    row = connection.execute("""SELECT COALESCE(SUM(u.amount),0),count(*) FROM provider_usage u WHERE u.id IN
        (SELECT provider_usage_id FROM paid_provider_dispatch_events WHERE scheduler_attempt_id=?
         AND event_type='send_marked')""", (attempt_id,)).fetchone()
    return float(row[0]), int(row[1])


def run_one(db_path: Path = DEFAULT_DB, at: str | None = None) -> dict[str, Any]:
    """Dispatch statistics through its shared two-member request boundary."""
    from . import capture_batches
    at = at or now_utc()
    with connect(db_path) as connection:
        _require20(connection)
        row = connection.execute("""SELECT id,operation,envelope_json FROM capture_work_items WHERE state='runnable'
            AND due_at<=? ORDER BY due_at,id LIMIT 1""", (planning.timestamp(at),)).fetchone()
    if row is not None and json.loads(row["envelope_json"]).get("compensation"):
        from .capture_compensation import run_authorized_work

        return run_authorized_work(db_path, int(row["id"]), at)
    if (row is not None and row["operation"] == capture_batches.OPERATION
            and json.loads(row["envelope_json"]).get("manual_command_run_id") is None):
        return capture_batches.run_one(db_path, at)
    return _run_single(db_path, at)


def _run_single(db_path: Path, at: str, *, compensation_work_id: int | None = None,
                manual_work_id: int | None = None) -> dict[str, Any]:
    """Claim and process one runnable work; blocked work never enters HTTP."""
    if manual_work_id is not None and (type(manual_work_id) is not int or manual_work_id < 1
            or compensation_work_id is not None):
        raise ValueError("manual work ID must be positive and cannot select compensation work")
    at = at or now_utc()
    with connect(db_path) as connection, transaction(connection):
        _require20(connection)
        active = activation_at(connection, at)
        if not execution_profile_allowed(active):
            return {"status": "shadow", "provider_calls": 0}
        if manual_work_id is None:
            _recover_work(connection, at=at)
        only = ""
        ids: tuple[int, ...] = ()
        if compensation_work_id is not None:
            only = " AND id=? AND json_type(envelope_json,'$.compensation')='object'"
            ids = (compensation_work_id,)
        elif manual_work_id is not None:
            only = (" AND id=? AND json_type(envelope_json,'$.manual_command_run_id')='integer'"
                    " AND json_extract(envelope_json,'$.kind')='metrics_update'"
                    " AND json_type(envelope_json,'$.compensation') IS NULL")
            ids = (manual_work_id,)
        normal_operation = "" if compensation_work_id is not None else (
            " AND (operation<>'douyin_video_statistics' OR json_type(envelope_json,'$.manual_command_run_id')='integer')")
        row = connection.execute("""SELECT * FROM capture_work_items WHERE state='runnable' AND due_at<=?
            """ + normal_operation + only + " ORDER BY due_at,id LIMIT 1", (planning.timestamp(at), *ids)).fetchone()
        if row is None:
            return {"status": "idle", "provider_calls": 0}
        work = dict(row)
        envelope = json.loads(work["envelope_json"])
        if manual_work_id is not None:
            from . import capture_manual
            specification = capture_manual.validate_command(connection, envelope["manual_command_run_id"],
                content_id=work["content_id"], operation=work["operation"], stage="metrics")
            if (specification["kind"] != "metrics_update" or envelope["content_id"] != work["content_id"]
                    or envelope["operation"] != work["operation"]
                    or not any(target["operation"] == work["operation"]
                        and target["logical_due"] == envelope["logical_due"]
                        and target["source_stage"] == envelope["source_stage"]
                        for target in specification["targets"])):
                raise ValueError("manual execution target differs from its frozen metrics command")
        state, reason = _readiness(connection, envelope, at=at)
        if state != "runnable":
            _record_readiness_block(connection, work, state=state, reason=reason, at=now_utc())
            return {"status": state, "reason": reason, "work_id": work["id"], "provider_calls": 0}
        claim_at = now_utc()
        claim = _claim_work(connection, work, at=claim_at)
        if claim is None:
            return {"status": "not_due_or_owned", "work_id": work["id"], "provider_calls": 0}
        connection.execute("UPDATE capture_work_items SET state='running',owner_token=?,heartbeat_at=?,lease_expires_at=?,attempt_count=attempt_count+1,updated_at=? WHERE id=?",
            (claim.owner_token, planning.timestamp(claim_at), planning.timestamp((_time(claim_at)+timedelta(seconds=durable_runs.LEASE_SECONDS)).isoformat()), planning.timestamp(claim_at), work["id"]))
    with _maintain_work_lease(db_path, work["id"], claim) as check_lease:
        result: dict[str, Any]
        try:
            from .capture_compensation import execution_context

            context = execution_context(work["id"]) if envelope.get("compensation") else nullcontext()
            with context, planning.execution_route_context(envelope["assignment_id"]), paid_scope(envelope["category"],
                    activation_id=envelope["activation_id"], roster_snapshot_id=envelope["roster_snapshot_id"],
                    roster_snapshot_hash=envelope["roster_members_sha256"], scheduler_run_id=claim.scheduler_run_id,
                    scheduler_attempt_id=claim.attempt_id, business_day=_business_day(claim_at),
                    manual_command_run_id=envelope.get("manual_command_run_id"),
                    catalog_plan_id=envelope.get("catalog_plan_id")):
                check_lease()
                result = _execute_one(envelope, db_path=db_path, at=claim_at)
            if result["complete"] or result["continuation"]:
                result["envelope"].pop("compensation", None)
        except Exception as error:
            reason = str(getattr(error, "error_code", type(error).__name__))
            with connect(db_path) as connection:
                cost, sent = _attempt_usage(connection, claim.attempt_id)
            if sent or isinstance(error, (raw_archive.RawArchiveError, capture.RawResponseIntegrityError)):
                reason = "paid_identity_hold:" + reason
            result = {"complete": False, "continuation": False, "envelope": envelope,
                      "evidence": {"error": type(error).__name__, "message": str(error)[:500]},
                      "reason": reason, "provider_cost": cost}
        check_lease()
        if result["complete"]:
            _verify_raws(db_path, result["evidence"].get("raw_response_ids", []))
        with connect(db_path) as connection, transaction(connection):
            check_lease()
            finished_at = now_utc()
            _renew_work_lease(connection, work["id"], claim, at=finished_at)
            evidence = {"contract_version": CONTRACT, "work_id": work["id"], **result["evidence"]}
            connection.execute("INSERT OR IGNORE INTO data_quality_receipts(scope_key,cutoff_at,payload_json,recorded_at,receipt_sha256) VALUES(?,?,?,?,?)",
                (f"capture-scan:{work['id']}", planning.timestamp(finished_at), planning.canonical(evidence), planning.timestamp(finished_at), planning.digest(evidence)))
            terminal_partial = bool(result.get("terminal_partial")) and envelope["stage"] == "discovery"
            final_state = "terminal" if result["complete"] or terminal_partial else "runnable" if result["continuation"] else _reason_state(result["reason"])
            if final_state == "paid_identity_hold" or terminal_partial:
                if final_state == "paid_identity_hold":
                    connection.execute("INSERT OR IGNORE INTO fetch_dead_letters(work_id,reason,envelope_json,attempts,created_at) VALUES(?,?,?,?,?)",
                        (work["id"], result["reason"], planning.canonical(result["envelope"]), work["attempt_count"]+1, planning.timestamp(finished_at)))
                connection.execute("INSERT OR IGNORE INTO operational_alerts(dedupe_key,severity,scope_json,evidence_json,owner,status,opened_at) VALUES(?,'P1',?,?,'capture-runtime','open',?)",
                    (f"capture-incomplete:{work['id']}", planning.canonical({"work_id": work["id"], "account_id": envelope["account_id"], "operation": envelope["operation"]}),
                     planning.canonical({**evidence, "complete": False, "reason": result["reason"]}), planning.timestamp(finished_at)))
            resume_at = _stamp(_time(finished_at)+timedelta(minutes=5))
            retry_due, consecutive_failures = _retry_schedule(
                connection, work=work, claim=claim, final_state=final_state,
                reason=result["reason"], at=finished_at)
            next_due = planning.timestamp(resume_at) if result["continuation"] else retry_due
            if final_state in {"provider_blocked", "budget_deferred"}:
                resume_at = next_due
            result["envelope"] = preserve_manual_work_context(connection, work_id=work["id"], envelope=result["envelope"])
            connection.execute("UPDATE capture_work_items SET state=?,reason=?,envelope_json=?,owner_token=NULL,heartbeat_at=NULL,lease_expires_at=NULL,completed_at=?,updated_at=?,due_at=? WHERE id=? AND owner_token=?",
                (final_state, result["reason"], planning.canonical(result["envelope"]), planning.timestamp(finished_at) if final_state == "terminal" else None,
                 planning.timestamp(finished_at), next_due, work["id"], claim.owner_token))
            if result["complete"] and envelope["stage"] == "discovery":
                planning.advance_watermark(connection, work_id=work["id"], scope_key=f"{envelope['platform']}:{envelope['uid']}",
                    complete_through=envelope["window_end"], evidence=result["evidence"], recorded_at=finished_at)
            durable_runs.checkpoint(connection, claim, {"complete": result["complete"], "last_result": evidence,
                "capture_consecutive_failures": consecutive_failures}, now=finished_at)
            # A durable "partial" means resumable. A bounded incomplete scan
            # must instead finish unsuccessfully, without another paid retry.
            durable_runs.finish_run_in_transaction(connection, claim,
                status="succeeded" if result["complete"] else "failed" if terminal_partial else "partial",
                summary={"work_id": work["id"], "reason": result["reason"],
                         "disposition": "complete" if result["complete"] else "partial"},
                next_resume_at=resume_at if final_state != "terminal" else None, now=finished_at)
        return {"status": final_state, "work_id": work["id"], "complete": result["complete"],
                "reason": result["reason"], "provider_cost": result["provider_cost"], "bounded_requests": 1}


def manual_work_spec(connection: sqlite3.Connection, *, content_id: int,
                     kind: str, at: str, allowed_groups: Collection[str] | None = None,
                     task_id: str | None = None, task_max_amount: float | None = None,
                     cycle_key: str | None = None, retry_transport_fault: bool = False) -> dict[str, Any]:
    """Freeze the existing logical capture buckets; no work, claims or HTTP."""
    _require20(connection)
    if kind not in {"manual_update", "media_retry", "metrics_update"}:
        raise ValueError("unsupported manual capture command")
    if type(retry_transport_fault) is not bool or (retry_transport_fault and kind != "metrics_update"):
        raise ValueError("transport retry is an explicit metrics-only boolean option")
    content = connection.execute("SELECT c.* FROM content_items c WHERE c.id=? AND " +
        canonical_content_predicate(connection, alias="c"), (content_id,)).fetchone()
    if content is None:
        raise LookupError("content_not_found")
    platform = content["platform"]
    if platform not in {"douyin", "xiaohongshu"}:
        raise ValueError("unsupported content platform")
    from .capture_manual import freeze_target
    from .source_routing import metric_cycle_key
    frozen_target = freeze_target(connection, content_id)
    if task_id is not None and (not isinstance(task_id, str) or not task_id.strip() or len(task_id) > 200):
        raise ValueError("invalid manual task ID")
    if cycle_key is not None and (not isinstance(cycle_key, str) or not cycle_key.strip() or len(cycle_key) > 200):
        raise ValueError("invalid manual metric cycle")
    cap = providers.DEFAULT_TASK_MAX_AMOUNT_USD if task_max_amount is None else task_max_amount
    if type(cap) not in {int, float} or not math.isfinite(cap) or cap <= 0:
        raise ValueError("manual task budget must be a finite positive amount")
    rules = load_policy()["metric_supplement_groups"][platform]
    names = {rule["name"] for rule in rules}
    if allowed_groups is not None and (isinstance(allowed_groups, (str, bytes))
            or any(not isinstance(group, str) or group not in names for group in allowed_groups)):
        raise ValueError("unknown metric supplement group")
    allowed = None if allowed_groups is None else sorted(set(allowed_groups))
    if kind == "media_retry" and (allowed_groups is not None or cycle_key is not None):
        raise ValueError("media retry does not accept metric options")
    if kind == "media_retry":
        from .media import _has_managed_history, _managed_bundle
        from .media_lifecycle import LifecycleError
        if _managed_bundle(connection, content_id) is not None or _has_managed_history(connection, content_id):
            raise LifecycleError("explicit_reacquire_contract_not_bound")
    targets = [] if kind == "metrics_update" else [{"stage": "detail", "operation": providers.STAGE_CONFIG[(platform, "detail")][2],
                "source_stage": "detail", "logical_due": "lifetime"}]
    if kind == "metrics_update":
        cycle_key = cycle_key or metric_cycle_key(content_id, content["published_at"], as_of=at)
        for group in rules:
            if allowed is not None and group["name"] not in allowed:
                continue
            source_stage = str(group["stage"])
            targets.append({"stage": "metrics", "operation": providers.STAGE_CONFIG[(platform, source_stage)][2],
                "source_stage": source_stage, "logical_due": f"{cycle_key}:{group['name']}", "group": group["name"]})
        if not targets:
            raise ValueError("metrics update requires at least one metric group")
    elif kind == "manual_update" and content["published_at"]:
        interval = planning.refresh_interval(published_at=content["published_at"], at=at,
                                             high_value=False, business_active=True)
        if interval is not None:
            for group in rules:
                if allowed is not None and group["name"] not in allowed:
                    continue
                stage = str(group["stage"])
                targets.append({"stage": "metrics", "operation": providers.STAGE_CONFIG[(platform, stage)][2],
                    "source_stage": stage, "group": group["name"],
                    "logical_due": (cycle_key or "metrics:" + _bucket(at, interval[0])) + ":" + str(group["name"])})
            week = _time(at).astimezone(BEIJING).date().isocalendar()
            targets.append({"stage": "comments", "operation": providers.STAGE_CONFIG[(platform, "comments")][2],
                "source_stage": "comments", "logical_due": f"{week.year}-W{week.week:02d}"})
    specification = {"content_id": content_id, "account_id": content["account_id"], "platform": platform,
            "kind": kind, "targets": targets, "frozen_target": frozen_target,
            "allowed_groups": allowed, "cycle_key": cycle_key,
            "task_id": task_id or f"manual-content:{_business_day(at)}:{content_id}", "task_max_amount": float(cap)}
    if retry_transport_fault:
        from .capture_manual import freeze_transport_retry
        specification["retry_transport_fault"] = True
        specification["transport_retry"] = freeze_transport_retry(connection, specification, at=at)
    return specification


def enqueue_manual_work(connection: sqlite3.Connection, *, specification: Mapping[str, Any],
                        command_run_id: int, at: str) -> dict[str, Any]:
    """Writer command executor: link frozen due work, never assign routes or send.

    An unfinished operation wins over the requested current bucket. A previously
    completed identical bucket is linked for local receipt/replay, not repurchased.
    """
    _require20(connection)
    if not connection.in_transaction:
        raise ValueError("manual enqueue requires a writer transaction")
    active = activation_at(connection, at)
    if active is None or active["profile_id"] != "integrated_route_v1":
        return {"status": "blocked", "reason": "integrated_profile_not_active", "work_ids": [], "provider_calls": 0}
    content = connection.execute("SELECT c.* FROM content_items c WHERE c.id=? AND " +
        canonical_content_predicate(connection, alias="c"), (specification["content_id"],)).fetchone()
    if content is None or content["account_id"] != specification["account_id"] or content["platform"] != specification["platform"]:
        return {"status": "blocked", "reason": "content_identity_changed", "work_ids": [], "provider_calls": 0}
    from . import capture_manual
    retained_specification = capture_manual.validate_command(connection, command_run_id, content_id=content["id"])
    if dict(specification) != retained_specification:
        raise ValueError("manual specification changed after submission")
    member = specification["frozen_target"]
    plan = {"id": None, **{key: active[key] for key in
        ("activation_id", "profile_id", "roster_snapshot_id", "roster_members_sha256")}}
    work_ids: list[int] = []
    blocked: list[str] = []
    created = 0
    for target in specification["targets"]:
        operation = target["operation"]
        identity = planning.digest({"provider": "tikhub", "operation": operation,
            "subject": f"content:{content['id']}", "logical_due": target["logical_due"]})
        work = None if specification["kind"] == "metrics_update" else connection.execute("""SELECT * FROM capture_work_items
            WHERE account_id=? AND content_id=? AND operation=? AND state!='terminal'
            ORDER BY id LIMIT 1""", (content["account_id"], content["id"], operation)).fetchone()
        if work is None:
            work = connection.execute("SELECT * FROM capture_work_items WHERE work_identity=?", (identity,)).fetchone()
        if work is None:
            created += _enqueue(connection, plan, member, stage=target["stage"], operation=operation,
                logical_due=target["logical_due"], at=at, content=content, source_stage=target["source_stage"],
                manual_command_run_id=command_run_id)
            work = connection.execute("SELECT * FROM capture_work_items WHERE work_identity=?", (identity,)).fetchone()
        if work is None:
            blocked.append(operation + ":work_not_enqueued")
            continue
        envelope = json.loads(work["envelope_json"])
        envelope["manual_command_run_ids"] = sorted(set(envelope.get("manual_command_run_ids", [])) | {command_run_id})
        # Running/terminal work is linked only. Never replace an in-flight
        # authority or reopen an unknown/invalid paid identity to buy it again.
        retry_replaces_blocked_command = (specification.get("retry_transport_fault") is True
            and work["work_identity"] == identity and work["state"] == "provider_blocked")
        if work["state"] not in {"running", "leased", "terminal", "paid_identity_hold"} and (
            not envelope.get("manual_command_run_id") or retry_replaces_blocked_command
        ):
            if envelope.get("request_batch_id") is not None:
                blocked.append(operation + ":existing_batch_scope_retained")
            else:
                assignment = capture_manual.assignment_for_command(connection, command_run_id,
                    content_id=content["id"], operation=operation, at=at, create=True)
                envelope.pop("catalog_plan_id", None)
                envelope.update(kind=specification["kind"], manual_command_run_id=command_run_id,
                    task_id=specification["task_id"], task_max_amount=specification["task_max_amount"],
                    assignment_id=assignment["id"], source_plan_id=None,
                    **{key: plan[key] for key in ("activation_id", "profile_id", "roster_snapshot_id", "roster_members_sha256")})
                state, reason = _readiness(connection, envelope, at=at)
                connection.execute("UPDATE capture_work_items SET assignment_id=?,source_plan_id=NULL,state=?,reason=?,due_at=? WHERE id=?",
                    (assignment["id"], state, reason, planning.timestamp(at), work["id"]))
        connection.execute("UPDATE capture_work_items SET envelope_json=?,updated_at=? WHERE id=?",
            (planning.canonical(envelope), planning.timestamp(at), work["id"]))
        work_ids.append(int(work["id"]))
    return {"status": "queued" if work_ids else "blocked", "reason": ";".join(blocked),
            "work_ids": sorted(set(work_ids)), "created": created, "provider_calls": 0}


def preserve_manual_work_context(connection: sqlite3.Connection, *, work_id: int,
                                 envelope: Mapping[str, Any]) -> dict[str, Any]:
    """Merge a concurrent manual association without altering any paid identity."""
    row = connection.execute("SELECT envelope_json FROM capture_work_items WHERE id=?", (work_id,)).fetchone()
    if row is None:
        raise ValueError("capture work is missing")
    current = json.loads(row[0])
    result = dict(envelope)
    if current.get("manual_command_run_ids"):
        result["manual_command_run_ids"] = sorted(set(result.get("manual_command_run_ids", [])) |
                                                  set(current.get("manual_command_run_ids", [])))
    return result


def run_ready(db_path: Path = DEFAULT_DB, at: str | None = None, *, max_items: int = TIKHUB_NETWORK_CONCURRENCY) -> dict[str, Any]:
    """One bounded worker batch; the existing provider limiter remains final."""
    if type(max_items) is not int or not 1 <= max_items <= TIKHUB_NETWORK_CONCURRENCY:
        raise ValueError(f"one worker batch must contain 1..{TIKHUB_NETWORK_CONCURRENCY} requests")
    with ThreadPoolExecutor(max_workers=max_items) as pool:
        futures = [pool.submit(copy_context().run, run_one, db_path, at) for _ in range(max_items)]
        results = [future.result() for future in futures]
    return {"status": "batch_complete", "max_requests": max_items, "results": results,
            "provider_cost": sum(float(row.get("provider_cost", 0.0)) for row in results)}
