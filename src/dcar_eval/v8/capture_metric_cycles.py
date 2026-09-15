"""Read-only isolation of an old automatic metric HOLD from a new due cycle.

False means only that no old work owns this new cycle. It never grants send
permission; callers must retain route, readiness, budget and paid-send checks.
"""
from __future__ import annotations

import json
import sqlite3
from contextlib import contextmanager
from contextvars import ContextVar
from datetime import datetime, time, timedelta, timezone
from types import MappingProxyType
from typing import Any, Mapping
from zoneinfo import ZoneInfo

from . import account_roster, capture_planning as planning, profile_activations
from .automatic_scope import within_automatic_scope
from .content_scope import canonical_content_predicate
from .providers import STAGE_CONFIG
from .source_routing import load_policy

BEIJING = ZoneInfo("Asia/Shanghai")
CONTRACT = "capture-runtime-v1"
_BINDINGS = ("activation_id", "profile_id", "activation_sha256",
             "roster_snapshot_id", "roster_members_sha256")
_MEMBER = ("identity_id", "account_id", "platform", "uid")
_PLANNING_PLANS: ContextVar[dict[str, Any] | None] = ContextVar("metric_cycle_planning_plans", default=None)


@contextmanager
def planning_validation(connection: sqlite3.Connection, *, plan: Mapping[str, Any] | None = None):
    """Reuse parsed immutable plans only in this verified writer transaction.

    Every access still compares the exact persisted JSON, hash, mode and day.
    A caller cannot retain this cache across a page, connection or transaction.
    Account eligibility and old work/slot ownership are never cached here.
    """
    if not connection.in_transaction:
        raise ValueError("metric cycle planning requires a transaction")
    token = _PLANNING_PLANS.set({"connection": connection, "plans": {}})
    try:
        bound = None
        if plan is not None:
            stored = _stored_plan(connection, plan["id"])
            if planning.canonical(stored) != planning.canonical(dict(plan)):
                raise ValueError("planning input differs from persisted source plan")
            bound = _immutable_json(stored)
            _PLANNING_PLANS.get()["bound_plan"] = bound
        yield bound
        if not connection.in_transaction:
            raise ValueError("metric cycle planning transaction ended")
    finally:
        _PLANNING_PLANS.reset(token)


def _immutable_json(value: Any) -> Any:
    if isinstance(value, dict):
        return MappingProxyType({key: _immutable_json(item) for key, item in value.items()})
    if isinstance(value, list):
        return tuple(_immutable_json(item) for item in value)
    return value


def _same_plan(connection: sqlite3.Connection, stored: dict[str, Any], supplied: Mapping[str, Any]) -> bool:
    """Only the page's deeply immutable, verified binding skips serialization.

    Mutable inputs retain the original strict canonical JSON comparison; object
    identity or a caller-supplied digest cannot prove such an input is unchanged.
    """
    context = _PLANNING_PLANS.get()
    if (context is not None and context["connection"] is connection and connection.in_transaction
            and supplied is context.get("bound_plan")
            and context["plans"].get(stored["id"], (None, None))[1] is stored):
        return True
    return planning.canonical(stored) == planning.canonical(dict(supplied))


def _time(value: str) -> datetime:
    parsed = datetime.fromisoformat(value.replace("Z", "+00:00"))
    if parsed.tzinfo is None:
        raise ValueError("metric cycle timestamps must include a timezone")
    return parsed.astimezone(timezone.utc)


def _stamp(value: datetime) -> str:
    return value.astimezone(timezone.utc).isoformat(timespec="seconds").replace("+00:00", "Z")


def _stored_plan(connection: sqlite3.Connection, plan_id: int) -> dict[str, Any]:
    if type(plan_id) is not int or plan_id <= 0:
        raise ValueError("automatic metrics require a persisted source plan")
    row = connection.execute("SELECT * FROM capture_source_plans WHERE id=?", (plan_id,)).fetchone()
    if row is None or row["mode"] != "active":
        raise ValueError("source plan is absent or inactive")
    context = _PLANNING_PLANS.get()
    cache = (context["plans"] if context is not None and context["connection"] is connection
             and connection.in_transaction else None)
    signature = (row["payload_json"], row["plan_sha256"], row["mode"], row["business_day"])
    if cache is not None and plan_id in cache:
        previous_signature, previous_plan = cache[plan_id]
        if previous_signature != signature:
            raise ValueError("source plan changed during planning")
        return previous_plan
    payload = json.loads(row["payload_json"])
    if (payload.get("shadow") is not False or payload.get("contract_version") != CONTRACT
            or payload["business_day"] != row["business_day"]
            or planning.digest(payload) != row["plan_sha256"]):
        raise ValueError("source plan evidence differs")
    result = {"id": row["id"], **payload}
    if cache is not None:
        cache[plan_id] = (signature, result)
    return result


def verified_planning_plan(connection: sqlite3.Connection, plan_id: int) -> dict[str, Any]:
    """Share this page's validated evidence, retaining the exact database fence."""
    context = _PLANNING_PLANS.get()
    if (context is None or context["connection"] is not connection or not connection.in_transaction
            or context.get("bound_plan", {}).get("id") != plan_id):
        raise ValueError("source plan has no active immutable planning binding")
    return _stored_plan(connection, plan_id)


def require_planning_comparison(connection: sqlite3.Connection,
                                original_plan: Mapping[str, Any],
                                comparison_plan: Mapping[str, Any]) -> None:
    """Bind a planning loop to this transaction's exact immutable comparison.

    Recheck the persisted signature and the original JSON once before any
    writes. Equal-looking copies, stale contexts and mutated caller inputs are
    not proof that the two representations still describe the same plan.
    """
    context = _PLANNING_PLANS.get()
    if (context is None or context["connection"] is not connection
            or not connection.in_transaction
            or comparison_plan is not context.get("bound_plan")
            or not isinstance(original_plan, dict)):
        raise ValueError("planning comparison has no current immutable binding")
    stored = verified_planning_plan(connection, original_plan.get("id"))
    if not _same_plan(connection, stored, original_plan):
        raise ValueError("planning input differs from its immutable comparison")


def _business_active(connection: sqlite3.Connection, content_id: int, at: datetime) -> bool:
    day_start = _stamp(datetime.combine(at.astimezone(BEIJING).date(), time.min, BEIJING))
    return bool(connection.execute(
        """SELECT EXISTS(SELECT 1 FROM task_contents tc JOIN report_tasks t ON t.id=tc.task_id
           WHERE tc.content_id=? AND (t.task_status IN ('queued','running','cancel_requested')
           OR julianday(t.completed_at)>=julianday(?))) OR EXISTS(
           SELECT 1 FROM capture_work_items w WHERE w.content_id=?
           AND json_extract(w.envelope_json,'$.kind')='manual_update'
           AND (w.state!='terminal' OR julianday(w.completed_at)>=julianday(?)))""",
        (content_id, day_start, content_id, day_start)).fetchone()[0])


def metric_cycle_pending(connection: sqlite3.Connection, plan: Mapping[str, Any],
                         member: Mapping[str, Any], content: Mapping[str, Any],
                         operation: str, source_stage: str | None,
                         logical_due: str, at: str) -> bool:
    """Retain backpressure except for strictly older, immutable automatic HOLDs.

Eligibility or evidence ambiguity fails closed. The function issues SELECTs
only; in particular it never changes a HOLD, slot, batch, usage or qualification.
Catalog eligibility deliberately does not reuse the old accounts.enabled flag.
"""
    try:
        now = _time(at)
        current_plan = _stored_plan(connection, plan["id"])
        if not _same_plan(connection, current_plan, plan):
            return True
        active = profile_activations.activation_at(connection, at)
        local = now.astimezone(BEIJING)
        plan_day = local.date() - timedelta(days=int((local.hour, local.minute) < (0, 10)))
        if (active is None or active["profile_id"] != "integrated_route_v1"
                or any(current_plan[key] != active[key] for key in _BINDINGS)
                or current_plan["business_day"] != plan_day.isoformat()
                or dict(member) not in current_plan["cohort"]):
            return True
        if current_plan.get("catalog_snapshot") is not None:
            # This public API is supplied by the unified directory-capture change.
            from .account_catalog_capture import validate_plan_member
            admitted = validate_plan_member(connection, plan["id"], member["identity_id"], at=at)
        else:
            admitted = account_roster.require_active_member(connection, member["identity_id"],
                current_plan["roster_snapshot_id"], current_plan["roster_members_sha256"])
        if any(admitted[key] != member[key] for key in ("account_id", "platform", "uid")):
            return True
        row = connection.execute("SELECT c.* FROM content_items c WHERE c.id=? AND " +
            canonical_content_predicate(connection, alias="c"), (content["id"],)).fetchone()
        if (row is None or any(row[key] != content[key] for key in
                ("id", "account_id", "platform", "published_at"))
                or row["account_id"] != member["account_id"] or row["platform"] != member["platform"]
                or not row["published_at"] or not within_automatic_scope(row["published_at"])):
            return True
        interval = planning.refresh_interval(published_at=row["published_at"], at=at,
            high_value=False, business_active=_business_active(connection, row["id"], now))
        if interval is None:
            return True
        seconds = interval[0]
        bucket = datetime.fromtimestamp((int(now.timestamp()) // seconds) * seconds, timezone.utc)
        stage = source_stage or "metrics"
        groups = load_policy()["metric_supplement_groups"][member["platform"]]
        matching = [group for group in groups if group["stage"] == stage
            and STAGE_CONFIG[(member["platform"], stage)][2] == operation
            and logical_due == f"metrics:{_stamp(bucket)}:{group['name']}"]
        if len(matching) != 1:
            return True
        group = matching[0]["name"]
        identity = planning.digest({"provider": "tikhub", "operation": operation,
            "subject": f"content:{row['id']}", "logical_due": logical_due})
        if connection.execute("SELECT 1 FROM capture_work_items WHERE work_identity=?", (identity,)).fetchone():
            return True
        pending = connection.execute("SELECT * FROM capture_work_items WHERE account_id=? "
            "AND content_id=? AND operation=? AND state!='terminal'",
            (member["account_id"], row["id"], operation)).fetchall()
        for old_row in pending:
            old = json.loads(old_row["envelope_json"])
            if (old_row["state"] != "paid_identity_hold" or old_row["owner_token"] is not None
                    or old_row["provider"] != "tikhub" or old.get("kind") is not None
                    or old.get("manual_command_run_id") is not None or old.get("manual_command_run_ids")
                    or "compensation" in old or old.get("contract_version") != CONTRACT
                    or old.get("stage") != "metrics" or old.get("capture_stage") != "metrics"
                    or old.get("category") != "metrics" or old.get("source_stage") != stage
                    or old.get("operation") != operation or old.get("content_id") != row["id"]
                    or old.get("assignment_id") != old_row["assignment_id"]
                    or old.get("source_plan_id") != old_row["source_plan_id"]
                    or any(old[key] != member[key] for key in _MEMBER)):
                return True
            previous_plan = _stored_plan(connection, old_row["source_plan_id"])
            if (any(old[key] != previous_plan[key] for key in _BINDINGS if key != "activation_sha256")
                    or not any(all(value[key] == old[key] for key in _MEMBER)
                               for value in previous_plan["cohort"])):
                return True
            old_due = old["logical_due"]
            if not old_due.startswith("metrics:") or not old_due.endswith(":" + group):
                return True
            old_bucket = _time(old_due[len("metrics:"):-len(":" + group)])
            old_identity = planning.digest({"provider": "tikhub", "operation": operation,
                "subject": f"content:{row['id']}", "logical_due": old_due})
            if (old_due != f"metrics:{_stamp(old_bucket)}:{group}"
                    or old_row["work_identity"] != old_identity or bucket <= old_bucket
                    or bucket <= _time(old_row["updated_at"]) or _time(old_row["updated_at"]) > now
                    or _time(old_row["created_at"]) > _time(old_row["updated_at"])
                    or old_bucket > _time(old_row["created_at"])):
                return True
        return False
    except (KeyError, TypeError, ValueError, AttributeError, OverflowError,
            ImportError, RuntimeError, sqlite3.Error):
        return True
