"""Prepare the complete natural primary inventory without executing provider work.

Only the latest due slot per actual registration is created. Existing current-day
natural rounds retain their frozen identities and links. An operator checkpoint
records every acquired owner; a second call returns that exact inventory and never
reclaims a completed/failed member or fills gaps after outcomes become known.
"""

from __future__ import annotations

import json
import sqlite3
from dataclasses import asdict
from datetime import datetime
from typing import Any, Mapping, NoReturn

from apscheduler.schedulers.base import STATE_PAUSED, BaseScheduler  # type: ignore[import-untyped]

from . import durable_runs, pipeline, providers, tikhub_scan, transport_natural_due as natural
from .account_roster import get_current_members, require_active_member, runtime_snapshot
from .provider_budget import DEFAULT_TASK_MAX_AMOUNT_USD, micro_usd
from .runtime_database import require_current_process_writer_lock
from .source_routing import parse_time
from .transport_due_candidates import list_primary_due_candidates
from .transport_members import DiagnosticMemberError, _current_campaign, _members, _operator_binding

CONTRACT_VERSION = "primary-due-inventory-v1"
_SCOPE_FIELDS = (
    "pipeline_version", "beijing_day", "registration_id", "scheduled_at",
    "activation_id", "profile_id",
)


def _fail(code: str, message: str) -> NoReturn:
    raise DiagnosticMemberError(code, message)


def _details(row: sqlite3.Row, identity: Mapping[str, Any]) -> dict[str, Any]:
    value = json.loads(row["details_json"])
    if (
        value.get("contract_version") != durable_runs.CONTRACT_VERSION
        or value.get("identity") != dict(identity)
        or value.get("scan_id") != durable_runs.scan_identity(row["job_id"], identity)
        or not isinstance(value.get("checkpoint"), dict)
        or type(value.get("complete")) is not bool
        or type(value["checkpoint"].get("complete")) is not bool
    ):
        _fail("diagnostic_inventory_changed", "Natural inventory frozen contract changed")
    return value


def _row(connection: sqlite3.Connection, job: str, key: Mapping[str, Any]) -> sqlite3.Row | None:
    return connection.execute(
        "SELECT * FROM scheduler_runs WHERE job_id=? AND scheduled_for=?",
        (job, "scan:" + durable_runs.scan_identity(job, key)),
    ).fetchone()


def _blocked(row: sqlite3.Row, details: Mapping[str, Any], at: str) -> str | None:
    if row["status"] == "running":
        _fail("diagnostic_foreign_owner", "Natural inventory has a foreign live owner")
    checkpoint = details["checkpoint"]
    if details["complete"] or checkpoint["complete"] or row["status"] not in {"partial", "interrupted"}:
        return "terminal"
    if checkpoint.get("pending_raw") is not None or checkpoint.get("pending_materialization") is not None:
        return "pending_local"
    due = details.get("next_resume_at")
    if (row["status"] == "partial" and due is None) or (due is not None and parse_time(str(due)) > parse_time(at)):
        return "resume_not_due"
    return None


def _parent_identity(registration: str, scheduled: datetime, active: Mapping[str, Any], ids: list[int]) -> dict[str, Any]:
    return {
        "pipeline_version": pipeline.PIPELINE_VERSION,
        "beijing_day": scheduled.astimezone(pipeline.BEIJING).date().isoformat(),
        "round_id": f"{registration}:{scheduled.hour:02d}:{scheduled.minute:02d}",
        "registration_id": registration, "job_id": pipeline.CRON_ROUNDS[registration][0],
        "scheduled_at": pipeline._iso(scheduled),
        "roster_snapshot_id": active["roster_snapshot_id"],
        "roster_snapshot_hash": active["roster_members_sha256"],
        "activation_id": active["activation_id"], "activation_sha256": active["activation_sha256"],
        "profile_id": active["profile_id"], "eligible_identity_ids": ids,
    }


def _parent_plans(connection: sqlite3.Connection, active: Mapping[str, Any], ids: list[int], at: str) -> list[dict[str, Any]]:
    now = parse_time(at)
    local = now.astimezone(pipeline.BEIJING)
    plans: dict[tuple[str, str], dict[str, Any]] = {}
    roster_ids = {
        int(member["identity_id"])
        for member in get_current_members(connection, snapshot_id=int(active["roster_snapshot_id"]))
        if member["uid"] and member["platform"] in {"douyin", "xiaohongshu"}
    }
    for registration in sorted(pipeline.PROFILE_CRON_REGISTRATIONS[str(active["profile_id"])]):
        if pipeline.CRON_ROUNDS[registration][0] not in {"tikhub_reconcile", "tikhub_works_scan"}:
            continue
        scheduled = pipeline._scheduled_round_at(registration, local)
        if scheduled is None:
            continue
        planned_active = pipeline.activation(connection, at=pipeline._iso(scheduled))
        if planned_active is None or planned_active["activation_id"] != active["activation_id"]:
            continue
        identity = _parent_identity(registration, scheduled, active, ids)
        natural._round_schedule(identity, now=now, active=active)
        plans[(registration, identity["scheduled_at"])] = identity
    # Keep actual earlier incomplete rounds; never synthesize missed cron hours.
    for row in connection.execute("SELECT * FROM scheduler_runs WHERE job_id LIKE 'pipeline_round:%' ORDER BY id"):
        details = json.loads(row["details_json"])
        identity = details.get("identity", {})
        if (
            identity.get("activation_id") != active["activation_id"]
            or identity.get("beijing_day") != local.date().isoformat()
            or identity.get("job_id") not in {"tikhub_reconcile", "tikhub_works_scan"}
        ):
            continue
        try:
            registration, scheduled = natural._round_schedule(identity, now=now, active=active)
        except natural.NaturalDueError:
            continue  # Manual/future/disabled registrations are not natural parents.
        if row["job_id"] != "pipeline_round:" + registration:
            continue
        frozen_ids = identity.get("eligible_identity_ids")
        if (
            not isinstance(frozen_ids, list)
            or any(type(identity_id) is not int or identity_id not in roster_ids for identity_id in frozen_ids)
            or sorted(set(frozen_ids)) != frozen_ids
        ):
            _fail("diagnostic_inventory_changed", "Frozen natural parent roster is invalid")
        expected = _parent_identity(registration, scheduled.astimezone(pipeline.BEIJING), active, frozen_ids)
        _details(row, expected)
        key = {field: expected[field] for field in _SCOPE_FIELDS}
        if row["scheduled_for"] != "scan:" + durable_runs.scan_identity(row["job_id"], key) or details.get("scope_key") != key:
            _fail("diagnostic_inventory_changed", "Natural parent scope key changed")
        plans[(registration, expected["scheduled_at"])] = expected
    return sorted(plans.values(), key=lambda value: (parse_time(value["scheduled_at"]), value["registration_id"]))


def _operator_plan(
    connection: sqlite3.Connection, *, campaign_id: int, operator: durable_runs.DurableClaim,
    active: Mapping[str, Any], ids: list[int], at: str,
) -> dict[str, Any] | None:
    """Use the committed explicit command; internal cron callers stay unchanged."""
    rows = connection.execute(
        "SELECT * FROM scheduler_runs WHERE job_id='current_activation_hold_command' "
        "AND (json_extract(details_json,'$.primary_operator.campaign_receipt_id')=? "
        "OR json_extract(details_json,'$.control_operator.campaign_receipt_id')=?)",
        (campaign_id, campaign_id),
    ).fetchall()
    if not rows:
        return None
    if len(rows) != 1:
        _fail("diagnostic_operator_command_changed", "Campaign has multiple command owners")
    row = rows[0]
    details = json.loads(row["details_json"])
    attempt = connection.execute(
        "SELECT id FROM scheduler_run_attempts WHERE scheduler_run_id=? ORDER BY id DESC LIMIT 1",
        (row["id"],),
    ).fetchone()
    if attempt is None:
        _fail("diagnostic_operator_command_changed", "On-demand command has no claimed attempt")
    binding = {
        "command_run_id": row["id"], "command_attempt_id": attempt["id"],
        "command_sha256": details.get("command_sha256"), "campaign_receipt_id": campaign_id,
        "operator_claim": asdict(operator),
    }
    scheduled = natural._operator_due_time(connection, binding, now=parse_time(at), require_running=True)
    parent = _parent_identity("tikhub_works_scan", scheduled.astimezone(pipeline.BEIJING), active, ids)
    parent.update({
        "registration_id": natural.OPERATOR_REGISTRATION,
        "round_id": f"{natural.OPERATOR_REGISTRATION}:{row['id']}:{attempt['id']}",
        "source": "operator", "due_kind": "on_demand", "operator_due": binding,
    })
    natural._round_schedule(parent, now=parse_time(at), active=active, connection=connection, require_running=True)
    return parent


def _child_scope(member: Mapping[str, Any], parent: Mapping[str, Any], active: Mapping[str, Any]) -> dict[str, Any]:
    start, end = natural._expected_scan_window(parent)
    return {
        "contract_version": tikhub_scan.CONTRACT_VERSION, "provider": "TikHub", "purpose": "reconcile",
        "identity_id": int(member["identity_id"]), "account_id": int(member["account_id"]),
        "platform": str(member["platform"]), "uid": str(member["uid"]),
        "roster_snapshot_id": active["roster_snapshot_id"], "roster_snapshot_hash": active["roster_members_sha256"],
        "window_start": start, "window_end": end, "task_id": None,
        "task_max_microusd": micro_usd(DEFAULT_TASK_MAX_AMOUNT_USD),
        "activation_id": active["activation_id"], "profile_id": active["profile_id"],
        "activation_sha256": active["activation_sha256"],
    }


def _initial_checkpoint(reference: str) -> dict[str, Any]:
    return {
        "cursor": 0, "generation": 0, "page_number": 0,
        "counts": dict.fromkeys(tikhub_scan.DISPOSITIONS, 0), "raw_items": 0,
        "last_manifest": None, "pending_raw": None, "reference": reference,
        "provider_transient": None, "provider_next_cursor": None, "completion_reason": None,
        "qualifying_old_page_count": 0, "pending_materialization": None, "complete": False,
    }


def _verify_saved(connection: sqlite3.Connection, saved: Mapping[str, Any], *, campaign_id: int, operator: Mapping[str, Any], writer: Mapping[str, Any]) -> None:
    if (
        saved.get("contract_version") != CONTRACT_VERSION or saved.get("campaign_receipt_id") != campaign_id
        or saved.get("operator") != dict(operator) or saved.get("writer") != dict(writer)
    ):
        _fail("diagnostic_inventory_changed", "Prepared inventory binding changed")
    for value in [*saved["parent_claims"], *saved["child_claims"]]:
        claim = durable_runs.DurableClaim(**value)
        row = connection.execute("SELECT * FROM scheduler_runs WHERE id=?", (claim.scheduler_run_id,)).fetchone()
        if row is None:
            _fail("diagnostic_inventory_changed", "Prepared run disappeared")
        details = json.loads(row["details_json"])
        owner = details.get("owner", {})
        if details.get("scan_id") != claim.scan_id or owner != {
            "attempt_id": claim.attempt_id, "attempt_number": claim.attempt_number, "token": claim.owner_token,
        }:
            _fail("diagnostic_foreign_owner", "Prepared inventory was claimed by another owner")
        if row["status"] == "running":
            durable_runs.assert_owner(connection, claim)


def prepare_primary_due_inventory(
    connection: sqlite3.Connection, *, campaign_receipt_id: int,
    operator_claim: durable_runs.DurableClaim, scheduler: BaseScheduler, at: str,
) -> dict[str, Any]:
    """Atomically acquire natural owners, never fetch, issue permits or select 20.

    The summary's parent/child claim dictionaries are the only owners acquired
    here. The coordinator must close every acquired attempt, including children
    not selected into the campaign. Missing references and local replay remain
    explicit exclusions. Failure rolls back this entire preparation savepoint.
    """
    if not connection.in_transaction:
        _fail("diagnostic_transaction_required", "Inventory preparation requires a caller transaction")
    if not isinstance(scheduler, BaseScheduler) or scheduler.state != STATE_PAUSED:
        _fail("diagnostic_scheduler_not_paused", "Inventory preparation requires the actual paused scheduler")
    writer = require_current_process_writer_lock(connection)
    campaign = _current_campaign(connection, campaign_receipt_id, at)
    operator = _operator_binding(connection, operator_claim, campaign)
    operator_details = durable_runs.assert_owner(connection, operator_claim)
    saved = operator_details["checkpoint"].get("primary_due_inventory")
    if saved is not None:
        _verify_saved(connection, saved, campaign_id=campaign_receipt_id, operator=operator, writer=writer)
        return dict(saved)
    if _members(connection, campaign_receipt_id):
        _fail("diagnostic_inventory_already_sampled", "Inventory must be frozen before any primary permits are issued")
    active = pipeline.activation(connection, at=at)
    assert active is not None  # _current_campaign verified the current HOLD activation.
    snapshot = runtime_snapshot(connection, active)
    members = [member for member in get_current_members(connection, snapshot_id=snapshot["id"], enabled_only=True)
               if member["uid"] and member["platform"] in {"douyin", "xiaohongshu"}]
    members.sort(key=lambda member: int(member["identity_id"]))
    ids = [int(member["identity_id"]) for member in members]
    for member in members:
        require_active_member(connection, member["identity_id"], snapshot["id"], snapshot["members_sha256"], activation=active)
    operator_plan = _operator_plan(
        connection, campaign_id=campaign_receipt_id, operator=operator_claim,
        active=active, ids=ids, at=at,
    )
    plans = [operator_plan] if operator_plan is not None else _parent_plans(connection, active, ids, at)
    summary: dict[str, Any] = {
        "contract_version": CONTRACT_VERSION, "campaign_receipt_id": campaign_receipt_id,
        "prepared_at": at, "operator": operator, "writer": writer,
        "eligible_identity_ids": ids, "parent_claims": [], "child_claims": [],
        "candidate_run_ids": [], "skipped": [],
    }
    connection.execute("SAVEPOINT primary_due_inventory")
    try:
        for parent in plans:
            registration = parent["registration_id"]
            job = "pipeline_round:" + registration
            key = {field: parent[field] for field in _SCOPE_FIELDS}
            existing = _row(connection, job, key)
            if existing is not None:
                reason = _blocked(existing, _details(existing, parent), at)
                if reason:
                    summary["skipped"].append({"registration_id": registration, "run_id": existing["id"], "reason": reason})
                    continue
            parent_claim = durable_runs.claim_run_in_transaction(
                connection, job, parent, scope_key=key, now=at, invocation_source="operator_retry",
                initial_checkpoint={"child_run_ids": [], "remaining_profiles": [], "started": False, "complete": False},
            )
            if parent_claim is None:
                _fail("diagnostic_inventory_changed", "Natural parent could not be claimed")
            summary["parent_claims"].append(asdict(parent_claim))
            parent_state = durable_runs.assert_owner(connection, parent_claim)["checkpoint"]
            links = list(parent_state["child_run_ids"])
            for member in members:
                if member["identity_id"] not in parent["eligible_identity_ids"]:
                    continue  # Never expand an existing round after an account is enabled.
                exclusion = {"registration_id": registration, "identity_id": member["identity_id"]}
                if member["platform"] != "douyin":
                    summary["skipped"].append({**exclusion, "reason": "outside_primary_operation"})
                    continue
                scope = _child_scope(member, parent, active)
                child = _row(connection, "tikhub_reconcile", scope)
                state = None
                if child is not None:
                    details = _details(child, scope)
                    state = details["checkpoint"]
                    reason = _blocked(child, details, at)
                    if reason:
                        summary["skipped"].append({**exclusion, "run_id": child["id"], "reason": reason})
                        continue
                    if child["id"] not in links:
                        _fail("diagnostic_inventory_changed", "Existing child is not linked to its natural parent")
                cached = connection.execute(
                    "SELECT reference_value FROM account_provider_references WHERE account_identity_id=? "
                    "AND provider='TikHub' COLLATE NOCASE AND reference_kind='sec_user_id'", (member["identity_id"],),
                ).fetchone()
                reference = cached["reference_value"] if cached is not None else None
                if reference is None:
                    summary["skipped"].append({**exclusion, "reason": "missing_cached_reference"})
                    continue
                if not isinstance(reference, str) or not providers._valid_douyin_sec_user_id(reference):
                    _fail("diagnostic_reference_invalid", "Cached Douyin reference is malformed")
                initialize_reference = state is not None and state.get("reference") in (None, "")
                if state is not None and state.get("reference") != reference and not (
                    initialize_reference and state.get("page_number") == 0
                    and state.get("generation") == 0 and type(state.get("cursor")) is int and state["cursor"] == 0
                ):
                    _fail("diagnostic_reference_changed", "Existing scan reference differs from its cached exact reference")
                child_claim = durable_runs.claim_run_in_transaction(
                    connection, "tikhub_reconcile", scope, now=at, invocation_source="operator_retry",
                    initial_checkpoint=_initial_checkpoint(reference),
                )
                if child_claim is None:
                    _fail("diagnostic_inventory_changed", "Natural child could not be claimed")
                if initialize_reference:
                    durable_runs.checkpoint(connection, child_claim, {"reference": reference}, now=at)
                summary["child_claims"].append(asdict(child_claim))
                if child_claim.scheduler_run_id not in links:
                    links.append(child_claim.scheduler_run_id)
            if links != parent_state["child_run_ids"]:
                durable_runs.checkpoint(connection, parent_claim, {"child_run_ids": links}, now=at)
        candidates = list_primary_due_candidates(connection, at=at)
        owned = {claim["scheduler_run_id"] for claim in summary["child_claims"]}
        if any(item["scope"].scheduler_run_id not in owned for item in candidates):
            _fail("diagnostic_foreign_owner", "Natural candidate is outside this operator's inventory")
        summary["candidate_run_ids"] = [item["scope"].scheduler_run_id for item in candidates]
        if scheduler.state != STATE_PAUSED:
            _fail("diagnostic_scheduler_not_paused", "Scheduler resumed during inventory preparation")
        durable_runs.checkpoint(connection, operator_claim, {"primary_due_inventory": summary}, now=at)
    except BaseException:
        connection.execute("ROLLBACK TO primary_due_inventory")
        connection.execute("RELEASE primary_due_inventory")
        raise
    connection.execute("RELEASE primary_due_inventory")
    return summary
