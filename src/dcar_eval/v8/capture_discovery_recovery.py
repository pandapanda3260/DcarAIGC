"""Freeze bounded discovery recovery from existing business and scan evidence.

No state is written here. An active source plan proves admission; creation of an
identity or an imported directory label does not. Legacy enabled transitions
describe actual exclusions, including the catalog planner's derived eligibility
projection. The catalog's manual daily/weekly/paused labels are not inputs.
"""
from __future__ import annotations

import json
import sqlite3
from datetime import datetime, timedelta, timezone
from typing import Any, Mapping
from zoneinfo import ZoneInfo

from . import account_states, capture_planning as planning
from .automatic_scope import automatic_start_at

CONTRACT = "discovery-recovery-v1"
MEMBER_KEYS = ("identity_id", "account_id", "platform", "uid")
BEIJING = ZoneInfo("Asia/Shanghai")
OVERLAP = timedelta(hours=72)
MAX_LOOKBACK = timedelta(days=30)


def _time(value: str) -> datetime:
    return datetime.fromisoformat(planning.timestamp(value).replace("Z", "+00:00"))


def _stamp(value: datetime) -> str:
    return planning.timestamp(value.isoformat())


def _identity(value: Mapping[str, Any]) -> tuple[Any, ...]:
    return tuple(value.get(key) for key in MEMBER_KEYS)


def _merge(intervals: list[tuple[datetime, datetime]]) -> list[tuple[datetime, datetime]]:
    merged: list[tuple[datetime, datetime]] = []
    for lower, upper in sorted(intervals):
        if lower >= upper:
            continue
        if merged and lower <= merged[-1][1]:
            merged[-1] = (merged[-1][0], max(upper, merged[-1][1]))
        else:
            merged.append((lower, upper))
    return merged


def _clip(intervals: list[tuple[datetime, datetime]], lower: datetime,
          upper: datetime) -> list[tuple[datetime, datetime]]:
    return _merge([(max(start, lower), min(end, upper)) for start, end in intervals])


def _subtract(intervals: list[tuple[datetime, datetime]],
              covered: list[tuple[datetime, datetime]]) -> list[tuple[datetime, datetime]]:
    result = []
    merged = _merge(covered)
    for lower, upper in intervals:
        cursor = lower
        for start, end in merged:
            if end <= cursor:
                continue
            if start >= upper:
                break
            if start > cursor:
                result.append((cursor, min(start, upper)))
            cursor = max(cursor, end)
            if cursor >= upper:
                break
        if cursor < upper:
            result.append((cursor, upper))
    return result


def _encoded(intervals: list[tuple[datetime, datetime]]) -> list[list[str]]:
    return [[_stamp(start), _stamp(end)] for start, end in intervals]


def prepare_scopes(connection: sqlite3.Connection, plan: Mapping[str, Any], *,
                   at: str) -> dict[int, dict[str, Any]]:
    """Read active plan history once, then freeze each current member's scope.

    Prior scope before the first exact active-plan membership is unknown and
    deliberately excluded. It must never be inferred from today's account row.
    """
    end = _time(at)
    targets = {_identity(member): member for member in plan["cohort"]}
    first: dict[tuple[Any, ...], dict[str, Any]] = {}
    rows = connection.execute("SELECT id,created_at,payload_json,plan_sha256 FROM capture_source_plans "
        "WHERE mode='active' AND julianday(created_at)<=julianday(?) ORDER BY julianday(created_at),id", (at,))
    for row in rows:
        try:
            value = json.loads(row["payload_json"])
            if (not isinstance(value, dict) or value.get("contract_version") != "capture-runtime-v1"
                    or value.get("shadow") is not False or planning.digest(value) != row["plan_sha256"]):
                continue
            for member in value.get("cohort", []):
                key = _identity(member)
                if key in targets and key not in first:
                    first[key] = {"kind": "active_capture_source_plan", "id": row["id"],
                        "created_at": planning.timestamp(row["created_at"]), "plan_sha256": row["plan_sha256"]}
        except (TypeError, ValueError, AttributeError):
            # An invalid historical plan cannot expand a current business scope.
            continue
    global_start = automatic_start_at()
    scopes = {}
    for key, member in targets.items():
        identity_id = member["identity_id"]
        admission = first.get(key)
        if admission is None:
            raise ValueError("discovery recovery requires a persisted active admission plan")
        lower = _time(admission["created_at"])
        if global_start is not None:
            lower = max(lower, global_start.astimezone(timezone.utc))
        events = account_states.state_events(connection, identity_id)
        visible = [event for event in events
                   if _time(event["effective_at"]) <= end and _time(event["created_at"]) <= end]
        account = connection.execute("SELECT a.enabled FROM accounts a JOIN account_platform_identities i "
            "ON i.account_id=a.id WHERE i.id=? AND a.id=? AND i.platform=? AND i.uid=?",
            (identity_id, member["account_id"], member["platform"], member["uid"])).fetchone()
        if account is None:
            raise ValueError("discovery recovery account identity changed")
        # Membership proves the initial enabled baseline if the transition log
        # has no events. With events, their first old value is the earlier state.
        enabled = bool(visible[0]["old_enabled"]) if visible else bool(account["enabled"])
        intervals = []
        cursor = lower
        for event in visible:
            instant = _time(event["effective_at"])
            if instant <= lower:
                enabled = event["new_enabled"]
                continue
            if enabled:
                intervals.append((cursor, instant))
            cursor, enabled = instant, event["new_enabled"]
        if enabled:
            intervals.append((cursor, end))
        evidence = {"admission": admission,
            "automatic_start": _stamp(global_start) if global_start is not None else None,
            "state_events": [{key: event[key] for key in ("event_id", "event_sha256", "effective_at")}
                             for event in visible]}
        scopes[identity_id] = {**{name: member[name] for name in MEMBER_KEYS},
            "scope_start": _stamp(lower), "enabled_intervals": _encoded(_clip(intervals, lower, end)),
            "scope_evidence": evidence}
    return scopes


def _completed_windows(connection: sqlite3.Connection, member: Mapping[str, Any], *,
                       at: str) -> list[dict[str, Any]]:
    """Reuse terminal, identity-bound watermark evidence without reopening raw.

    Watermarks are created only after the runtime verifies all raw pages. Their
    conserved dispositions and exact frozen window are checked again here.
    This is planning evidence, not a replacement for day-coverage raw validation.
    """
    operation = member["platform"] + "_user_posts"
    rows = connection.execute("SELECT m.id watermark_id,m.complete_through,m.evidence_json,m.recorded_at,"
        "w.id work_id,w.account_id,w.operation,w.provider,w.envelope_json,w.state,w.reason,w.completed_at "
        "FROM capture_watermarks m JOIN capture_work_items w ON w.id=m.work_id "
        "WHERE m.provider='tikhub' AND m.operation=? AND m.scope_key=? "
        "AND julianday(m.recorded_at)<=julianday(?) ORDER BY m.complete_through,m.id",
        (operation, f"{member['platform']}:{member['uid']}", at))
    result = []
    for row in rows:
        try:
            env, evidence = json.loads(row["envelope_json"]), json.loads(row["evidence_json"])
            if (row["state"] != "terminal" or row["reason"] or not row["completed_at"]
                    or _time(row["completed_at"]) > _time(at)
                    or row["account_id"] != member["account_id"] or row["operation"] != operation
                    or row["provider"] != "tikhub" or _identity(env) != _identity(member)
                    or env.get("stage") != "discovery" or env.get("content_id") is not None
                    or not all(evidence.get(name) is True for name in ("complete", "terminal_cursor", "all_raw_verified"))
                    or not all(evidence.get(name) is False for name in ("cap_hit", "cursor_loop"))
                    or any(evidence.get(name) != env.get(name) for name in
                           ("identity_id", "account_id", "platform", "operation", "window_start", "window_end"))):
                continue
            counts = [evidence.get(name) for name in ("seen", "valid", "missing", "invalid", "unavailable")]
            if any(type(count) is not int or count < 0 for count in counts) or counts[0] != sum(counts[1:]):
                continue
            lower, upper = _time(env["window_start"]), _time(env["window_end"])
            if lower >= upper or upper > _time(at) or _time(row["complete_through"]) != upper:
                continue
            if env.get("recovery_contract") is not None:
                if (env["recovery_contract"] != CONTRACT
                        or evidence.get("published_intervals") != env.get("published_intervals")):
                    continue
                intervals = [(_time(start), _time(end)) for start, end in env["published_intervals"]]
                if not intervals or any(start < lower or end > upper or start >= end for start, end in intervals):
                    continue
            else:
                intervals = [(lower, upper)]
            result.append({"watermark_id": row["watermark_id"], "work_id": row["work_id"],
                           "complete_through": upper, "intervals": _merge(intervals)})
        except (KeyError, TypeError, ValueError, AttributeError):
            continue
    return result


def freeze_window(connection: sqlite3.Connection, member: Mapping[str, Any], *, at: str,
                  scope: Mapping[str, Any]) -> dict[str, Any]:
    """Choose one daily broad scan, otherwise recover from the last completion.

    The caller persists this result in its ordinary natural discovery work.
    Retries use that same envelope, preserving its window and paid identity.
    """
    if _identity(scope) != _identity(member):
        raise ValueError("discovery recovery scope identity differs")
    end = _time(at)
    scope_start = _time(scope["scope_start"])
    enabled = [(_time(start), _time(upper)) for start, upper in scope["enabled_intervals"]]
    enabled = _clip(enabled, scope_start, end)
    horizon = end - MAX_LOOKBACK
    midnight = end.astimezone(BEIJING).replace(hour=0, minute=0, second=0, microsecond=0)
    operation = member["platform"] + "_user_posts"
    daily = connection.execute("SELECT envelope_json FROM capture_work_items WHERE account_id=? "
        "AND operation=? AND provider='tikhub' AND julianday(created_at)>=julianday(?) "
        "AND julianday(created_at)<=julianday(?) "
        "AND json_extract(envelope_json,'$.stage')='discovery' "
        "AND json_extract(envelope_json,'$.recovery_contract')=? "
        "AND json_extract(envelope_json,'$.discovery_mode')='daily_recheck'",
        (member["account_id"], operation, _stamp(midnight), at, CONTRACT))
    already_daily = any(_identity(json.loads(row[0])) == _identity(member) for row in daily)
    completed = _completed_windows(connection, member, at=at)
    latest = max((record["complete_through"] for record in completed), default=None)
    mode = "daily_recheck" if not already_daily else "incremental"
    start = horizon if not already_daily else end - OVERLAP
    if already_daily and latest is not None and latest < end - OVERLAP:
        mode = "restart_gap"
        start = latest - OVERLAP
    start = max(start, horizon, scope_start)
    selected = _clip(enabled, start, end)
    if selected:
        start = selected[0][0]
    else:
        start = end
    covered = [interval for record in completed for interval in record["intervals"]]
    gaps = _subtract(_clip(enabled, scope_start, horizon), covered)
    return {"recovery_contract": CONTRACT, "discovery_mode": mode,
        "window_start": _stamp(start), "window_end": _stamp(end),
        "published_intervals": _encoded(selected), "scope_start": scope["scope_start"],
        "scope_evidence": dict(scope["scope_evidence"]),
        "bounded_out_gaps": _encoded(gaps),
        "last_complete_through": _stamp(latest) if latest is not None else None}
