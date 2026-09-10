"""Read-only isolation of an old automatic metric HOLD from a new due cycle.

False means only that no old work owns this new cycle. It never grants send
permission; callers must retain route, readiness, budget and paid-send checks.
"""
from __future__ import annotations

import json
import sqlite3
from datetime import datetime, time, timedelta, timezone
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
    payload = json.loads(row["payload_json"])
    if (payload.get("shadow") is not False or payload.get("contract_version") != CONTRACT
            or payload["business_day"] != row["business_day"]
            or planning.digest(payload) != row["plan_sha256"]):
        raise ValueError("source plan evidence differs")
    return {"id": row["id"], **payload}


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
        if planning.canonical(current_plan) != planning.canonical(dict(plan)):
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
