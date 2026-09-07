"""Read-only proof that a paid TikHub request is naturally due.

The returned receipt is diagnostic evidence, not permission to send.  Callers
must rederive it immediately before the transport boundary and compare it with
the signed permit.  No mutable checkpoint aggregate is hashed: only the frozen
target and the cursor/page facts needed for the concrete request are retained.
"""

from __future__ import annotations

import hashlib
import json
import sqlite3
from collections.abc import Mapping
from datetime import date, datetime, time, timedelta, timezone
from typing import Any, NoReturn
from zoneinfo import ZoneInfo

from . import durable_runs, providers, tikhub_scan
from .account_metrics import select_account_metrics
from .account_roster import RosterError, require_active_member
from .comment_paging import cursor_sha256, page_window_key
from .paid_identity import PaidRequestIdentity, build_paid_request_identity
from .profile_activations import ProfileActivationError, activation_at
from .provider_budget import (
    DEFAULT_TASK_MAX_AMOUNT_USD,
    PaidScope,
    PaidScopeBlocked,
    _assert_scheduler_owner,
    micro_usd,
)
from .source_routing import load_policy, parse_time


CONTRACT_VERSION = "transport-natural-due-v1"
BEIJING = ZoneInfo("Asia/Shanghai")
QUEUE_JOBS = frozenset({"content_pipeline", "metrics_backfill", "comments_refresh"})
PROFILE_JOBS = frozenset({"matrix_account_metrics", "tikhub_account_metrics"})
OPERATOR_REGISTRATION = "transport_primary_on_demand"


class NaturalDueError(RuntimeError):
    """The supplied request cannot be proved due from current durable state."""

    error_code = "natural_due_blocked"

    def __init__(self, code: str, message: str) -> None:
        super().__init__(message)
        self.code = code


def _fail(code: str, message: str) -> NoReturn:
    raise NaturalDueError(code, message)


def _canonical(value: Any) -> str:
    try:
        return json.dumps(
            value,
            ensure_ascii=False,
            sort_keys=True,
            separators=(",", ":"),
            allow_nan=False,
        )
    except (TypeError, ValueError) as error:
        raise NaturalDueError(
            "natural_due_invalid", "Natural-due evidence is not canonical JSON"
        ) from error


def _frozen(value: Any) -> Any:
    return json.loads(_canonical(value))


def _sha(value: Any) -> str:
    return hashlib.sha256(_canonical(value).encode("utf-8")).hexdigest()


def _object(value: Any, *, label: str) -> dict[str, Any]:
    if not isinstance(value, Mapping):
        _fail("natural_due_invalid", f"{label} must be an object")
    return dict(value)


def _timestamp(value: Any, *, label: str) -> datetime:
    try:
        return parse_time(str(value))
    except (TypeError, ValueError, AttributeError) as error:
        raise NaturalDueError(
            "natural_due_time_invalid", f"{label} must be a timezone-aware timestamp"
        ) from error


def _iso(value: datetime) -> str:
    return (
        value.astimezone(timezone.utc)
        .isoformat(timespec="seconds")
        .replace("+00:00", "Z")
    )


def _day(value: datetime) -> str:
    return value.astimezone(BEIJING).date().isoformat()


def _same_current_day(value: Any, *, now: datetime, label: str) -> datetime:
    parsed = _timestamp(value, label=label)
    if parsed > now:
        _fail("natural_due_future", f"{label} is in the future")
    if _day(parsed) != _day(now):
        _fail("natural_due_day_expired", f"{label} is outside the current Beijing day")
    return parsed


def _load_source_run(
    connection: sqlite3.Connection, scope: PaidScope
) -> tuple[PaidScope, sqlite3.Row, dict[str, Any], dict[str, Any], dict[str, Any]]:
    if not isinstance(scope, PaidScope):
        _fail("natural_due_scope_invalid", "Paid scope has an invalid type")
    if scope.scheduler_run_id is None or scope.scheduler_attempt_id is None:
        _fail("natural_due_owner_missing", "Natural-due proof requires a scheduler owner")
    try:
        checked = _assert_scheduler_owner(connection, scope)
    except PaidScopeBlocked as error:
        raise NaturalDueError(
            "natural_due_owner_invalid", "Scheduler owner is no longer current"
        ) from error
    row = connection.execute(
        "SELECT id,job_id,scheduled_for,status,details_json FROM scheduler_runs WHERE id=?",
        (checked.scheduler_run_id,),
    ).fetchone()
    if row is None or row["status"] != "running":
        _fail("natural_due_owner_invalid", "Natural-due source run is not running")
    try:
        details = _object(json.loads(row["details_json"]), label="run details")
        identity = _object(details.get("identity"), label="run identity")
        checkpoint = _object(details.get("checkpoint"), label="run checkpoint")
    except (TypeError, ValueError, json.JSONDecodeError) as error:
        raise NaturalDueError(
            "natural_due_source_invalid", "Natural-due source run is malformed"
        ) from error
    if details.get("contract_version") != durable_runs.CONTRACT_VERSION:
        _fail("natural_due_source_invalid", "Natural-due source contract is unsupported")
    try:
        scan_id = durable_runs.scan_identity(str(row["job_id"]), identity)
    except durable_runs.DurableRunError as error:
        raise NaturalDueError(
            "natural_due_source_invalid", "Natural-due source identity is invalid"
        ) from error
    stable = details.get("scope_key")
    scheduled_scan = (
        durable_runs.scan_identity(str(row["job_id"]), stable)
        if isinstance(stable, Mapping)
        else scan_id
    )
    if (
        details.get("scan_id") != scan_id
        or checked.scheduler_scan_id != scan_id
        or row["scheduled_for"] != "scan:" + scheduled_scan
        or details.get("complete") is not False
        or checkpoint.get("complete") is not False
    ):
        _fail("natural_due_source_invalid", "Natural-due source identity changed")
    return checked, row, details, identity, checkpoint


def _current_activation(
    connection: sqlite3.Connection,
    *,
    identity: Mapping[str, Any],
    scope: PaidScope,
    at: str,
) -> dict[str, Any]:
    try:
        active = activation_at(connection, at)
    except ProfileActivationError as error:
        raise NaturalDueError(
            "natural_due_activation_invalid", "Acquisition activation chain is invalid"
        ) from error
    if active is None:
        _fail("natural_due_activation_invalid", "No current acquisition activation")
    expected = {
        "activation_id": int(active["activation_id"]),
        "profile_id": str(active["profile_id"]),
        "activation_sha256": str(active["activation_sha256"]),
        "roster_snapshot_id": int(active["roster_snapshot_id"]),
        "roster_snapshot_hash": str(active["roster_members_sha256"]),
    }
    if any(identity.get(key) != value for key, value in expected.items()):
        _fail(
            "natural_due_activation_invalid",
            "Frozen run is not bound to the current activation and roster",
        )
    scope_expected = {
        "activation_id": expected["activation_id"],
        "roster_snapshot_id": expected["roster_snapshot_id"],
        "roster_snapshot_hash": expected["roster_snapshot_hash"],
    }
    if any(getattr(scope, key) != value for key, value in scope_expected.items()):
        _fail("natural_due_scope_mismatch", "Paid scope changed its activation or roster")
    return active


def _member(
    connection: sqlite3.Connection,
    *,
    scope: PaidScope,
    active: Mapping[str, Any],
    identity_id: int,
) -> dict[str, Any]:
    try:
        member = require_active_member(
            connection,
            identity_id,
            int(active["roster_snapshot_id"]),
            str(active["roster_members_sha256"]),
            activation=active,
        )
    except RosterError as error:
        raise NaturalDueError(
            "natural_due_nonmember", "Paid target is not a current active-roster member"
        ) from error
    expected = {
        "identity_id": identity_id,
        "account_id": int(member["account_id"]),
        "uid": str(member["uid"]),
        "platform": str(member["platform"]),
    }
    if any(getattr(scope, key) != value for key, value in expected.items()):
        _fail("natural_due_scope_mismatch", "Paid target differs from the frozen member")
    return {**dict(member), **expected}


def _require_scope_kind(
    scope: PaidScope,
    *,
    purpose: str,
    category: str,
    stage: str,
    content_id: int | None,
    business_day: str,
) -> None:
    if (
        scope.purpose != purpose
        or scope.category != category
        or scope.content_id != content_id
        or scope.business_day != business_day
        or scope.paid_sequence != 0
    ):
        _fail("natural_due_scope_mismatch", "Paid scope does not match natural due work")
    expected_stage = "discovery" if content_id is None else category
    if stage != expected_stage:
        _fail("natural_due_stage_mismatch", "Paid stage does not match natural due work")


def _content(
    connection: sqlite3.Connection,
    *,
    scope: PaidScope,
    member: Mapping[str, Any],
) -> dict[str, Any]:
    if scope.content_id is None:
        _fail("natural_due_scope_mismatch", "Content queue request has no content target")
    rows = connection.execute(
        "SELECT * FROM content_items WHERE id=? AND account_id=? AND platform=?",
        (scope.content_id, member["account_id"], member["platform"]),
    ).fetchall()
    if len(rows) != 1:
        _fail("natural_due_target_missing", "Frozen content target no longer exists")
    content = dict(rows[0])
    raw_uid = content.get("raw_account_uid")
    if raw_uid not in (None, "", member["uid"]):
        _fail("natural_due_scope_mismatch", "Content author differs from roster member")
    return content


def _slot_already_succeeded(
    connection: sqlite3.Connection,
    *,
    content_id: int | None,
    account_id: int,
    stage: str,
    window_key: str,
) -> bool:
    column, target = (
        ("content_id", content_id) if content_id is not None else ("account_id", account_id)
    )
    row = connection.execute(
        f"SELECT status FROM fetch_slots WHERE {column}=? AND stage=? AND window_key=?",
        (target, stage, window_key),
    ).fetchone()
    return row is not None and row["status"] == "succeeded"


def _expected_identity(
    request_identity: PaidRequestIdentity,
    *,
    operation: str,
    platform: str,
    subject: str,
    parameters: Mapping[str, Any],
    cursor: Any,
    due_bucket: str,
    request_window: Mapping[str, str] | None = None,
) -> PaidRequestIdentity:
    if not isinstance(request_identity, PaidRequestIdentity):
        _fail("natural_due_request_invalid", "Paid request identity has an invalid type")
    if request_identity.sequence != 0:
        _fail("natural_due_sequence_invalid", "Natural due only permits sequence zero")
    try:
        expected = build_paid_request_identity(
            provider="TikHub",
            operation=operation,
            platform=platform,
            subject=subject,
            request_parameters=parameters,
            cursor=cursor,
            due_bucket=due_bucket,
            request_window=request_window,
            sequence=0,
        )
    except (TypeError, ValueError) as error:
        raise NaturalDueError(
            "natural_due_request_invalid", "Expected paid request is invalid"
        ) from error
    if request_identity != expected:
        _fail(
            "natural_due_request_mismatch",
            "Paid request document is not the request currently naturally due",
        )
    return expected


def _comment_state(
    connection: sqlite3.Connection,
    *,
    content_id: int,
    comment_as_of: str,
) -> tuple[str, Any, int]:
    try:
        requested_day = date.fromisoformat(comment_as_of)
    except (TypeError, ValueError) as error:
        raise NaturalDueError(
            "natural_due_source_invalid", "Frozen comment day is invalid"
        ) from error
    iso = requested_day.isocalendar()
    week = f"{iso.year}-W{iso.week:02d}"
    run = connection.execute(
        "SELECT id,status FROM comment_capture_runs WHERE content_id=? AND window_key=?",
        (content_id, week),
    ).fetchone()
    if run is None:
        return week, None, 0
    if run["status"] in {"succeeded", "terminal_failed"}:
        _fail("natural_due_not_pending", "Comment capture is already terminal")
    page = connection.execute(
        "SELECT page_number,next_cursor_json,has_more FROM comment_capture_pages "
        "WHERE capture_run_id=? ORDER BY page_number DESC LIMIT 1",
        (run["id"],),
    ).fetchone()
    if page is None:
        return week, None, 0
    if not bool(page["has_more"]):
        _fail("natural_due_not_pending", "Comment provider has no next page")
    try:
        cursor = json.loads(page["next_cursor_json"])
    except (TypeError, ValueError, json.JSONDecodeError) as error:
        raise NaturalDueError(
            "natural_due_cursor_invalid", "Stored comment continuation is invalid"
        ) from error
    if not isinstance(cursor, Mapping) or not cursor:
        _fail("natural_due_cursor_invalid", "Stored comment continuation is invalid")
    return week, dict(cursor), int(page["page_number"])


def _queue_due(
    connection: sqlite3.Connection,
    *,
    scope: PaidScope,
    request_identity: PaidRequestIdentity,
    stage: str,
    at: str,
    now: datetime,
    row: sqlite3.Row,
    identity: Mapping[str, Any],
    checkpoint: Mapping[str, Any],
) -> tuple[PaidRequestIdentity, str, dict[str, Any]]:
    job = str(row["job_id"])
    if identity.get("kind") != job or job not in QUEUE_JOBS:
        _fail("natural_due_source_rejected", "Queue source is not an enabled natural scope")
    created = _same_current_day(identity.get("created_for"), now=now, label="queue created_for")
    business_day = _day(created)
    active = _current_activation(connection, identity=identity, scope=scope, at=at)

    if scope.content_id is None or isinstance(scope.content_id, bool):
        _fail("natural_due_scope_mismatch", "Queue scope has no content ID")
    content_id = int(scope.content_id)
    candidate_ids = identity.get("candidate_ids")
    pending_ids = checkpoint.get("pending_ids")
    items = checkpoint.get("items")
    if not isinstance(candidate_ids, list) or not isinstance(pending_ids, list) or not isinstance(items, list):
        _fail("natural_due_source_invalid", "Queue membership evidence is malformed")
    if content_id not in candidate_ids or content_id not in pending_ids:
        _fail("natural_due_not_pending", "Content is not pending in its frozen queue")
    matches = [item for item in items if isinstance(item, Mapping) and item.get("id") == content_id]
    if len(matches) != 1:
        _fail("natural_due_source_invalid", "Frozen queue item is missing or ambiguous")
    frozen_item = dict(matches[0])
    identity_id = frozen_item.get("identity_id")
    if isinstance(identity_id, bool) or not isinstance(identity_id, int):
        _fail("natural_due_source_invalid", "Frozen queue member is invalid")
    if frozen_item.get("historical") is True:
        _fail("natural_due_history_excluded", "History purchases are outside this diagnostic scope")
    member = _member(
        connection, scope=scope, active=active, identity_id=identity_id
    )
    content = _content(connection, scope=scope, member=member)
    item = {**content, **frozen_item}

    expected_stage = {
        "content_pipeline": "detail",
        "metrics_backfill": "metrics",
        "comments_refresh": "comments",
    }[job]
    _require_scope_kind(
        scope,
        purpose=expected_stage,
        category=expected_stage,
        stage=stage,
        content_id=content_id,
        business_day=business_day,
    )
    if stage != expected_stage:
        _fail("natural_due_stage_mismatch", "Queue kind cannot issue this stage")

    # This is the production queue's current target derivation.  It performs
    # only reads and uses the frozen item cycle/day over current local evidence.
    from .pipeline import _candidate_work_targets

    targets = _candidate_work_targets(connection, job, item, at=at)
    candidates: list[tuple[PaidRequestIdentity, dict[str, Any]]] = []
    for target in targets:
        if target.get("stage") != stage or target.get("local_replay"):
            continue
        operation = str(target["operation"])
        window = str(target["window_key"])
        cursor: Any = None
        group: str | None = None
        if stage == "comments":
            week, cursor, previous_page = _comment_state(
                connection,
                content_id=content_id,
                comment_as_of=str(frozen_item.get("comment_as_of") or ""),
            )
            window = page_window_key(week, cursor)
            if target["window_key"] != window:
                continue
        else:
            previous_page = 0
        if stage == "metrics":
            group = str(target.get("group") or "")
            rule = next(
                (
                    value
                    for value in load_policy()["metric_supplement_groups"][member["platform"]]
                    if value["name"] == group
                ),
                None,
            )
            if rule is None:
                continue
            source_stage = str(rule["stage"])
        else:
            source_stage = stage
        platform_content_id = str(content["platform_content_id"])
        if member["platform"] == "douyin":
            _, parameters = providers._douyin_request(
                source_stage, platform_content_id, cursor=cursor
            )
        else:
            _, parameters = providers._xhs_request(
                source_stage,
                platform_content_id,
                str(content["content_type"]),
                cursor=cursor,
            )
        expected = build_paid_request_identity(
            provider="TikHub",
            operation=operation,
            platform=str(member["platform"]),
            subject=platform_content_id,
            request_parameters=parameters,
            cursor=cursor,
            due_bucket=window,
            sequence=0,
        )
        proof = {
            "kind": "queue",
            "queue_kind": job,
            "created_for": _iso(created),
            "frozen_item": _frozen(frozen_item),
            "target": {
                "stage": stage,
                "window_key": window,
                "operation": operation,
                **({"group": group} if group else {}),
            },
        }
        if stage == "comments":
            proof["cursor"] = _frozen(cursor)
            proof["cursor_sha256"] = cursor_sha256(cursor)
            proof["previous_page_number"] = previous_page
        candidates.append((expected, proof))

    match = next((value for value in candidates if value[0] == request_identity), None)
    if match is None:
        _fail(
            "natural_due_request_mismatch",
            "Paid request is not any currently due frozen queue target",
        )
    expected, proof = match
    if _slot_already_succeeded(
        connection,
        content_id=content_id,
        account_id=int(member["account_id"]),
        stage=stage,
        window_key=str(expected.document["due_bucket"]),
    ):
        _fail("natural_due_not_pending", "Paid queue slot already succeeded")
    return expected, str(member["uid"]), proof


def _operator_due_time(
    connection: sqlite3.Connection, binding: Mapping[str, Any], *, now: datetime,
    require_running: bool = False,
) -> datetime:
    """Prove a real writer command, never turn a supplied timestamp into due work.

    Terminal evidence readers may verify the same original command after it has
    finished; live natural-due validation additionally requires its current owner.
    """
    from .profile_control import _decode_command_row
    from .transport_campaign import CONTROL_ARMS
    from .transport_members import OPERATOR_JOB, primary_operator_identity
    from .transport_receipts import read_transport_receipt

    ids = (binding.get("command_run_id"), binding.get("command_attempt_id"), binding.get("campaign_receipt_id"))
    if any(type(value) is not int or value <= 0 for value in ids):
        _fail("natural_due_operator_invalid", "On-demand command identifiers are invalid")
    row = connection.execute("SELECT * FROM scheduler_runs WHERE id=?", (ids[0],)).fetchone()
    attempt = connection.execute(
        "SELECT * FROM scheduler_run_attempts WHERE id=? AND scheduler_run_id=?",
        (ids[1], ids[0]),
    ).fetchone()
    if row is None or attempt is None:
        _fail("natural_due_operator_invalid", "On-demand command owner is missing")
    command = _decode_command_row(row)
    details = command["details"]
    campaign = read_transport_receipt(connection, int(binding["campaign_receipt_id"]))
    campaign_payload = campaign["payload"]
    arm = campaign_payload.get("arm") if campaign["kind"] == "campaign" else None
    operator = _object(binding.get("operator_claim"), label="on-demand operator")
    if arm == "primary":
        expected_command = "transport_primary"
        expected_parameters = {"drain_id": campaign_payload["hold_binding"]["drain_id"]}
        expected_owner = {"campaign_receipt_id": ids[2], "claim": operator}
        actual_owner = details.get("primary_operator")
    elif arm in CONTROL_ARMS:
        expected_command = "transport_control"
        expected_parameters = {
            "drain_id": campaign_payload["hold_binding"]["drain_id"],
            "source_verdict_receipt_id": campaign_payload["source_verdict_receipt_id"],
            "arm": arm,
        }
        expected_owner = {"arm": arm, "campaign_receipt_id": ids[2], "claim": operator}
        actual_owner = details.get("control_operator")
    else:
        _fail("natural_due_operator_invalid", "On-demand campaign arm is invalid")
    if (
        command["binding"].get("command") != expected_command
        or command["binding"].get("parameters") != expected_parameters
        or binding.get("command_sha256") != details.get("command_sha256")
        or actual_owner != expected_owner
        or attempt["invocation_source"] != "operator_retry"
        or attempt["attempt_number"] != details.get("attempt_number")
        or attempt["started_at"] != row["started_at"]
        or attempt["started_at"] != details.get("started_at")
        or connection.execute(
            "SELECT MAX(id) FROM scheduler_run_attempts WHERE scheduler_run_id=?", (ids[0],)
        ).fetchone()[0] != ids[1]
        or (require_running and (row["status"] != "running" or attempt["status"] != "running"))
    ):
        _fail("natural_due_operator_invalid", "On-demand command binding or owner changed")
    claim = durable_runs.DurableClaim(**operator)
    operator_row = connection.execute(
        "SELECT * FROM scheduler_runs WHERE id=?", (claim.scheduler_run_id,)
    ).fetchone()
    operator_attempt = connection.execute(
        "SELECT * FROM scheduler_run_attempts WHERE id=? AND scheduler_run_id=?",
        (claim.attempt_id, claim.scheduler_run_id),
    ).fetchone()
    expected = primary_operator_identity(campaign)
    operator_details = json.loads(operator_row["details_json"]) if operator_row is not None else {}
    if (
        operator_row is None or operator_row["job_id"] != OPERATOR_JOB
        or operator_details.get("identity") != expected
        or claim.scan_id != durable_runs.scan_identity(OPERATOR_JOB, expected)
        or operator_details.get("scan_id") != claim.scan_id
        or operator_details.get("owner") != {
            "attempt_id": claim.attempt_id, "attempt_number": claim.attempt_number, "token": claim.owner_token,
        }
        or operator_attempt is None or operator_attempt["attempt_number"] != claim.attempt_number
        or (require_running and (operator_row["status"] != "running" or operator_attempt["status"] != "running"))
    ):
        _fail("natural_due_operator_invalid", "On-demand operator differs from its campaign")
    scheduled = _same_current_day(attempt["started_at"], now=now, label="on-demand command started_at")
    if _timestamp(details.get("submitted_at"), label="command submitted_at") > scheduled:
        _fail("natural_due_operator_invalid", "On-demand command started before submission")
    return scheduled


def _round_schedule(
    identity: Mapping[str, Any], *, now: datetime, active: Mapping[str, Any],
    connection: sqlite3.Connection | None = None, require_running: bool = False,
) -> tuple[str, datetime]:
    from .pipeline import CRON_ROUNDS, PROFILE_CRON_REGISTRATIONS

    registration = identity.get("registration_id")
    job = identity.get("job_id")
    if registration == OPERATOR_REGISTRATION:
        if connection is None:
            _fail("natural_due_operator_invalid", "On-demand scheduling requires durable command proof")
        binding = _object(identity.get("operator_due"), label="on-demand binding")
        scheduled = _operator_due_time(connection, binding, now=now, require_running=require_running)
        if (
            job != "tikhub_works_scan" or identity.get("source") != "operator"
            or identity.get("due_kind") != "on_demand"
            or identity.get("scheduled_at") != _iso(scheduled)
            or identity.get("beijing_day") != _day(scheduled)
            or identity.get("round_id") != f"{registration}:{binding['command_run_id']}:{binding['command_attempt_id']}"
            or any(identity.get(key) != active.get(key) for key in (
                "activation_id", "profile_id", "roster_snapshot_id",
            ))
        ):
            _fail("natural_due_operator_invalid", "On-demand parent identity changed")
        return registration, scheduled
    if not isinstance(registration, str) or registration not in CRON_ROUNDS:
        _fail("natural_due_source_rejected", "Pipeline round registration is not fixed")
    registered_job, raw_hours, minute, weekday = CRON_ROUNDS[registration]
    if job != registered_job:
        _fail("natural_due_source_rejected", "Pipeline round job changed its registration")
    if registration not in PROFILE_CRON_REGISTRATIONS.get(str(active["profile_id"]), frozenset()):
        _fail("natural_due_source_rejected", "Pipeline round is disabled for the active profile")
    scheduled = _same_current_day(
        identity.get("scheduled_at"), now=now, label="pipeline scheduled_at"
    )
    local = scheduled.astimezone(BEIJING)
    hours = {int(value) for value in str(raw_hours).split(",")}
    if local.hour not in hours or local.minute != int(minute) or local.second != 0:
        _fail("natural_due_source_rejected", "Pipeline round is not a natural cron slot")
    if weekday is not None and local.strftime("%a").lower()[:3] != str(weekday):
        _fail("natural_due_source_rejected", "Pipeline round weekday is not due")
    if (
        identity.get("beijing_day") != local.date().isoformat()
        or identity.get("round_id")
        != f"{registration}:{local.hour:02d}:{local.minute:02d}"
    ):
        _fail("natural_due_source_invalid", "Pipeline round identity is malformed")
    return registration, scheduled


def _profile_due(
    connection: sqlite3.Connection,
    *,
    scope: PaidScope,
    request_identity: PaidRequestIdentity,
    stage: str,
    at: str,
    now: datetime,
    row: sqlite3.Row,
    identity: Mapping[str, Any],
    checkpoint: Mapping[str, Any],
) -> tuple[PaidRequestIdentity, str, dict[str, Any]]:
    active = _current_activation(connection, identity=identity, scope=scope, at=at)
    registration, scheduled = _round_schedule(identity, now=now, active=active)
    if (
        str(row["job_id"]) != "pipeline_round:" + registration
        or identity.get("job_id") not in PROFILE_JOBS
    ):
        _fail("natural_due_source_rejected", "Pipeline round is not an account profile round")
    eligible = identity.get("eligible_identity_ids")
    if not isinstance(eligible, list) or scope.identity_id not in eligible:
        _fail("natural_due_nonmember", "Account is not eligible in the frozen profile round")
    if scope.identity_id is None or isinstance(scope.identity_id, bool):
        _fail("natural_due_scope_mismatch", "Account profile scope has no identity")
    started = checkpoint.get("started")
    remaining = checkpoint.get("remaining_profiles")
    if type(started) is not bool or not isinstance(remaining, list):
        _fail("natural_due_source_invalid", "Profile checkpoint is malformed")
    if started and scope.identity_id not in remaining:
        _fail("natural_due_not_pending", "Account profile is not pending on resume")
    member = _member(
        connection,
        scope=scope,
        active=active,
        identity_id=int(scope.identity_id),
    )
    business_day = str(identity["beijing_day"])
    _require_scope_kind(
        scope,
        purpose="metrics",
        category="metrics",
        stage=stage,
        content_id=None,
        business_day=business_day,
    )
    if member["platform"] != "douyin":
        _fail("natural_due_source_rejected", "Only Douyin profile refresh is enabled")
    metrics = select_account_metrics(connection, [int(scope.identity_id)], cutoff_at=at)
    follower = metrics.get(int(scope.identity_id), {}).get("metric_fields", {}).get(
        "follower_count", {}
    )
    if not isinstance(follower, Mapping) or follower.get("freshness") == "fresh":
        _fail("natural_due_not_pending", "Account follower profile is already fresh")
    window = "matrix-first:profile:" + business_day
    expected = _expected_identity(
        request_identity,
        operation="douyin_uid_profile",
        platform="douyin",
        subject=str(member["uid"]),
        parameters={"uid": str(member["uid"])},
        cursor=None,
        due_bucket=window,
    )
    if _slot_already_succeeded(
        connection,
        content_id=None,
        account_id=int(member["account_id"]),
        stage="discovery",
        window_key=window,
    ):
        _fail("natural_due_not_pending", "Account profile slot already succeeded")
    proof = {
        "kind": "account_profile",
        "registration_id": registration,
        "round_id": str(identity["round_id"]),
        "scheduled_at": _iso(scheduled),
        "identity_id": int(scope.identity_id),
        "follower_evidence": {
            key: follower.get(key)
            for key in (
                "status",
                "freshness",
                "observation_id",
                "raw_response_id",
                "captured_at",
                "recorded_at",
            )
        },
    }
    return expected, str(member["uid"]), proof


def _expected_scan_window(parent: Mapping[str, Any]) -> tuple[str, str]:
    planned = _timestamp(parent.get("scheduled_at"), label="scan parent scheduled_at")
    local = planned.astimezone(BEIJING)
    if parent.get("registration_id") == OPERATOR_REGISTRATION:
        # The user-triggered business window ends at the actual command time,
        # not at a future cron slot and not at a caller-chosen retry timestamp.
        end = planned
        start = end - timedelta(days=30)
    elif parent.get("job_id") == "tikhub_reconcile":
        end = datetime.combine(local.date(), time.min, BEIJING)
        start = end - timedelta(days=7)
    elif parent.get("job_id") == "tikhub_works_scan" and local.hour == 2:
        end = datetime.combine(local.date(), time.min, BEIJING)
        start = end - timedelta(days=30)
    elif parent.get("job_id") == "tikhub_works_scan":
        end = local
        start = end - timedelta(days=2)
    else:
        _fail("natural_due_source_rejected", "Parent round cannot create TikHub scans")
    if parent.get("registration_id") != OPERATOR_REGISTRATION and "automatic_from_date" in parent:
        raw_floor = parent["automatic_from_date"]
        try:
            if not isinstance(raw_floor, str):
                raise ValueError("automatic floor must be a date")
            floor_day = date.fromisoformat(raw_floor)
            if floor_day.isoformat() != raw_floor:
                raise ValueError("automatic floor must be canonical")
        except ValueError:
            _fail("natural_due_source_invalid", "Frozen automatic start date is invalid")
        start = max(start, datetime.combine(floor_day, time.min, BEIJING))
        if start >= end:
            _fail("natural_due_source_invalid", "Frozen automatic window is empty")
    return _iso(start), _iso(end)


def _scan_parent(
    connection: sqlite3.Connection,
    *,
    child_run_id: int,
    child_identity: Mapping[str, Any],
    child_attempt: int,
    active: Mapping[str, Any],
    member: Mapping[str, Any],
    now: datetime,
) -> dict[str, Any]:
    matches: list[dict[str, Any]] = []
    for row in connection.execute(
        "SELECT id,job_id,scheduled_for,status,details_json FROM scheduler_runs "
        "WHERE job_id LIKE 'pipeline_round:%' ORDER BY id DESC"
    ):
        try:
            details = _object(json.loads(row["details_json"]), label="parent details")
            parent = _object(details.get("identity"), label="parent identity")
            checkpoint = _object(details.get("checkpoint"), label="parent checkpoint")
            registration, scheduled = _round_schedule(
                parent, now=now, active=active, connection=connection, require_running=True,
            )
        except (NaturalDueError, TypeError, ValueError, json.JSONDecodeError):
            continue
        if (
            row["job_id"] != "pipeline_round:" + registration
            or parent.get("job_id") not in {"tikhub_reconcile", "tikhub_works_scan"}
            or parent.get("activation_id") != child_identity.get("activation_id")
            or parent.get("roster_snapshot_id") != child_identity.get("roster_snapshot_id")
            or parent.get("roster_snapshot_hash") != child_identity.get("roster_snapshot_hash")
            or child_identity.get("identity_id") not in parent.get("eligible_identity_ids", [])
            or details.get("complete") is not False
            or checkpoint.get("complete") is not False
        ):
            continue
        start, end = _expected_scan_window(parent)
        expected_child = {
            "contract_version": tikhub_scan.CONTRACT_VERSION,
            "provider": "TikHub",
            "purpose": "reconcile",
            "identity_id": int(member["identity_id"]),
            "account_id": int(member["account_id"]),
            "platform": str(member["platform"]),
            "uid": str(member["uid"]),
            "roster_snapshot_id": int(active["roster_snapshot_id"]),
            "roster_snapshot_hash": str(active["roster_members_sha256"]),
            "window_start": start,
            "window_end": end,
            "task_id": None,
            "task_max_microusd": micro_usd(DEFAULT_TASK_MAX_AMOUNT_USD),
            "activation_id": int(active["activation_id"]),
            "profile_id": str(active["profile_id"]),
            "activation_sha256": str(active["activation_sha256"]),
        }
        if dict(child_identity) != expected_child:
            continue
        linked = child_run_id in checkpoint.get("child_run_ids", [])
        first = (
            child_attempt == 1
            and row["status"] == "running"
            and checkpoint.get("started") is False
        )
        if (child_attempt == 1 and not (first or linked)) or (
            child_attempt > 1 and not linked
        ):
            continue
        matches.append(
            {
                "run_id": int(row["id"]),
                "scan_id": str(details.get("scan_id") or ""),
                "scheduled_for": str(row["scheduled_for"]),
                "scheduled_at": _iso(scheduled),
                "identity_sha256": _sha(parent),
                "registration_id": registration,
                "link_kind": "existing_child_link" if linked else "running_first_dispatch",
            }
        )
    if len(matches) != 1:
        _fail(
            "natural_due_parent_missing",
            "TikHub scan has no unique same-day natural pipeline parent",
        )
    return matches[0]


def _scan_due(
    connection: sqlite3.Connection,
    *,
    scope: PaidScope,
    request_identity: PaidRequestIdentity,
    stage: str,
    at: str,
    now: datetime,
    row: sqlite3.Row,
    details: Mapping[str, Any],
    identity: Mapping[str, Any],
    checkpoint: Mapping[str, Any],
) -> tuple[PaidRequestIdentity, str, dict[str, Any]]:
    if (
        row["job_id"] != "tikhub_reconcile"
        or identity.get("contract_version") != tikhub_scan.CONTRACT_VERSION
        or identity.get("provider") != "TikHub"
        or identity.get("purpose") != "reconcile"
    ):
        _fail(
            "natural_due_history_excluded",
            "Manual, legacy, and history scans are outside this diagnostic scope",
        )
    if stage != "discovery":
        _fail("natural_due_stage_mismatch", "TikHub account scan requires discovery stage")
    window_end = _same_current_day(
        identity.get("window_end"), now=now, label="scan window_end"
    )
    start = _timestamp(identity.get("window_start"), label="scan window_start")
    if start >= window_end:
        _fail("natural_due_source_invalid", "TikHub scan window is empty")
    next_resume = details.get("next_resume_at")
    if next_resume is not None and _timestamp(next_resume, label="scan next_resume_at") > now:
        _fail("natural_due_future", "TikHub scan resume is not due")
    active = _current_activation(connection, identity=identity, scope=scope, at=at)
    raw_identity_id = identity.get("identity_id")
    if isinstance(raw_identity_id, bool) or not isinstance(raw_identity_id, int):
        _fail("natural_due_source_invalid", "TikHub scan identity is invalid")
    member = _member(
        connection,
        scope=scope,
        active=active,
        identity_id=raw_identity_id,
    )
    if (
        identity.get("account_id") != member["account_id"]
        or identity.get("uid") != member["uid"]
        or identity.get("platform") != member["platform"]
    ):
        _fail("natural_due_scope_mismatch", "TikHub scan target changed")
    _require_scope_kind(
        scope,
        purpose="reconcile",
        category="reconcile",
        stage=stage,
        content_id=None,
        business_day=_day(window_end),
    )
    owner = _object(details.get("owner"), label="scan owner")
    attempt_number = owner.get("attempt_number")
    if isinstance(attempt_number, bool) or not isinstance(attempt_number, int) or attempt_number < 1:
        _fail("natural_due_source_invalid", "TikHub scan attempt is invalid")
    parent = _scan_parent(
        connection,
        child_run_id=int(row["id"]),
        child_identity=identity,
        child_attempt=attempt_number,
        active=active,
        member=member,
        now=now,
    )
    if checkpoint.get("pending_raw") is not None or checkpoint.get("pending_materialization") is not None:
        _fail("natural_due_not_pending", "TikHub scan must finish local replay before another purchase")
    generation = checkpoint.get("generation")
    page_number = checkpoint.get("page_number")
    cursor = checkpoint.get("cursor")
    if (
        isinstance(generation, bool)
        or not isinstance(generation, int)
        or generation < 0
        or isinstance(page_number, bool)
        or not isinstance(page_number, int)
        or page_number < 0
    ):
        _fail("natural_due_cursor_invalid", "TikHub scan cursor generation is invalid")
    try:
        normalized_cursor = tikhub_scan._cursor(
            str(member["platform"]), cursor, initial=page_number == 0
        )
    except tikhub_scan.TikHubScanError as error:
        raise NaturalDueError(
            "natural_due_cursor_invalid", "TikHub scan cursor is invalid"
        ) from error
    request_window = {
        "start": str(identity["window_start"]),
        "end": str(identity["window_end"]),
    }
    operation = str(request_identity.document.get("operation") or "")
    if operation == "douyin_uid_profile":
        if member["platform"] != "douyin" or checkpoint.get("reference") not in (None, ""):
            _fail("natural_due_not_pending", "TikHub reference lookup is not due")
        cached = connection.execute(
            "SELECT reference_value FROM account_provider_references "
            "WHERE account_identity_id=? AND provider='TikHub' COLLATE NOCASE AND reference_kind='sec_user_id'",
            (member["identity_id"],),
        ).fetchone()
        if cached is not None and providers._valid_douyin_sec_user_id(
            str(cached["reference_value"])
        ):
            _fail("natural_due_not_pending", "TikHub account reference is already cached")
        window = (
            f"reference:{member['identity_id']}:"
            f"{tikhub_scan._digest(str(member['uid']))}:v1"
        )
        expected = _expected_identity(
            request_identity,
            operation=operation,
            platform="douyin",
            subject=str(member["uid"]),
            parameters={"uid": str(member["uid"])},
            cursor=None,
            due_bucket=window,
            request_window=request_window,
        )
        proof_cursor: Any = None
        proof_kind = "account_reference"
    else:
        expected_operation = (
            "douyin_user_posts"
            if member["platform"] == "douyin"
            else "xiaohongshu_user_posts"
        )
        if operation != expected_operation:
            _fail("natural_due_request_mismatch", "TikHub scan operation is not due")
        if member["platform"] == "douyin":
            reference = checkpoint.get("reference")
            cached = connection.execute(
                "SELECT reference_value FROM account_provider_references "
                "WHERE account_identity_id=? AND provider='TikHub' COLLATE NOCASE AND reference_kind='sec_user_id'",
                (member["identity_id"],),
            ).fetchone()
            if (
                not isinstance(reference, str)
                or not providers._valid_douyin_sec_user_id(reference)
                or cached is None
                or str(cached["reference_value"]) != reference
            ):
                _fail("natural_due_reference_invalid", "Douyin list request has no proven reference")
            subject = reference
            parameters = {
                "sec_user_id": reference,
                "max_cursor": normalized_cursor or 0,
                "count": 20,
                "sort_type": 0,
            }
        else:
            subject = str(member["uid"])
            parameters = {
                "user_id": str(member["uid"]),
                "cursor": normalized_cursor or "",
            }
        window = (
            f"scan:{details['scan_id']}:g{generation}:p{page_number}:"
            f"{tikhub_scan._digest(normalized_cursor)}"
        )
        expected = _expected_identity(
            request_identity,
            operation=expected_operation,
            platform=str(member["platform"]),
            subject=subject,
            parameters=parameters,
            cursor=normalized_cursor,
            due_bucket=window,
            request_window=request_window,
        )
        proof_cursor = normalized_cursor
        proof_kind = "user_posts"
    if _slot_already_succeeded(
        connection,
        content_id=None,
        account_id=int(member["account_id"]),
        stage="discovery",
        window_key=str(expected.document["due_bucket"]),
    ):
        _fail("natural_due_not_pending", "TikHub scan request already succeeded")
    proof = {
        "kind": proof_kind,
        "natural_parent": parent,
        "generation": generation,
        "page_number": page_number,
        "cursor": _frozen(proof_cursor),
        "cursor_sha256": _sha(proof_cursor),
        "frozen_window": _frozen(request_window),
    }
    return expected, str(member["uid"]), proof


def _scope_identity(scope: PaidScope) -> dict[str, Any]:
    return {
        "scheduler_run_id": scope.scheduler_run_id,
        "scheduler_attempt_id": scope.scheduler_attempt_id,
        "scheduler_owner_token": scope.scheduler_owner_token,
        "scheduler_scan_id": scope.scheduler_scan_id,
        "activation_id": scope.activation_id,
        "roster_snapshot_id": scope.roster_snapshot_id,
        "roster_snapshot_hash": scope.roster_snapshot_hash,
        "identity_id": scope.identity_id,
        "account_id": scope.account_id,
        "content_id": scope.content_id,
        "uid": scope.uid,
        "platform": scope.platform,
        "purpose": scope.purpose,
        "category": scope.category,
        "business_day": scope.business_day,
    }


def validate_natural_due_request(
    connection: sqlite3.Connection,
    *,
    scope: PaidScope,
    request_identity: PaidRequestIdentity,
    stage: str,
    at: str,
) -> dict[str, Any]:
    """Rederive one exact sequence-zero request from current durable state.

    The function performs SQLite reads only.  It neither reserves budget nor
    creates a fetch slot, attempt, dispatch record, manifest, or network call.
    """

    if not isinstance(connection, sqlite3.Connection):
        _fail("natural_due_connection_invalid", "A live SQLite connection is required")
    if stage not in {"detail", "metrics", "comments", "discovery"}:
        _fail("natural_due_stage_mismatch", "Paid stage is outside natural due scope")
    now = _timestamp(at, label="natural-due at")
    checked, row, details, identity, checkpoint = _load_source_run(connection, scope)
    job = str(row["job_id"])
    if job == "history_recovery" or identity.get("purpose") == "history":
        _fail("natural_due_history_excluded", "History purchases are excluded")
    if job == "paid_capture_direct" or identity.get("contract_version") == "direct-paid-dispatch-owner-v1":
        _fail("natural_due_source_rejected", "Direct/manual paid owner is not natural due")

    if job in QUEUE_JOBS:
        expected, account_uid, proof = _queue_due(
            connection,
            scope=checked,
            request_identity=request_identity,
            stage=stage,
            at=at,
            now=now,
            row=row,
            identity=identity,
            checkpoint=checkpoint,
        )
    elif job.startswith("pipeline_round:"):
        expected, account_uid, proof = _profile_due(
            connection,
            scope=checked,
            request_identity=request_identity,
            stage=stage,
            at=at,
            now=now,
            row=row,
            identity=identity,
            checkpoint=checkpoint,
        )
    elif job == "tikhub_reconcile":
        expected, account_uid, proof = _scan_due(
            connection,
            scope=checked,
            request_identity=request_identity,
            stage=stage,
            at=at,
            now=now,
            row=row,
            details=details,
            identity=identity,
            checkpoint=checkpoint,
        )
    else:
        _fail("natural_due_source_rejected", "Scheduler owner is not a natural paid source")

    document = _frozen(expected.document)
    document_sha256 = _sha(document)
    if (
        expected != request_identity
        or document_sha256 != expected.scope_identity
        or checked.paid_scope_identity not in (None, expected.scope_identity)
    ):
        _fail("natural_due_request_mismatch", "Paid request identity changed after derivation")
    if proof.get("kind") == "queue":
        natural_scheduled_for = str(proof["created_for"])
    elif proof.get("kind") == "account_profile":
        natural_scheduled_for = str(proof["scheduled_at"])
    else:
        parent = _object(proof.get("natural_parent"), label="natural parent proof")
        natural_scheduled_for = str(parent["scheduled_at"])
    return _frozen(
        {
            "contract_version": CONTRACT_VERSION,
            "source_run_id": int(row["id"]),
            "scan_id": str(details["scan_id"]),
            "source_identity_sha256": _sha(identity),
            "source_scheduled_for": str(row["scheduled_for"]),
            "scheduled_for": natural_scheduled_for,
            "scope_identity": _scope_identity(checked),
            "account_uid": account_uid,
            "stage": stage,
            "operation": str(document["operation"]),
            "request_document_sha256": document_sha256,
            "request_document": document,
            "paid_scope_identity": expected.scope_identity,
            "sequence": 0,
            "proof": proof,
        }
    )
