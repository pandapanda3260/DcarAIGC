"""Fixed primary member permits: database, mirror, natural scope and writer bound.

This is an issuer/verifier, not a network runner or a drain exception. The final
capture boundary must use the verifier and bind its receipt in paid dispatch.
"""

from __future__ import annotations

import json
import sqlite3
from dataclasses import asdict
from pathlib import Path
from typing import Any, Mapping

from . import durable_runs
from .paid_dispatch import dispatch_events
from .paid_identity import PaidRequestIdentity
from .provider_budget import PRICES_MICROUSD, PaidScope
from .provider_transport import validate_request_transport_binding
from .runtime_database import require_current_process_writer_lock
from .source_routing import parse_time
from .transport_campaign import CONTRACT_VERSION as CAMPAIGN_CONTRACT
from .transport_campaign import CONTROL_ARMS, same_hold_lineage
from .transport_hold_binding import diagnostic_dispatch_binding, read_current_diagnostic_hold
from .transport_natural_due import validate_natural_due_request
from .transport_receipts import append_transport_receipt, read_transport_receipt

CONTRACT_VERSION = "current_hold_diagnostic_permit_v1"
OPERATOR_JOB = "transport_diagnostic_operator"


class DiagnosticMemberError(RuntimeError):
    error_code = "provider_transport_blocked"

    def __init__(self, code: str, message: str) -> None:
        super().__init__(message)
        self.code = code


def primary_operator_identity(campaign: Mapping[str, Any]) -> dict[str, Any]:
    """The named operator command cannot choose a target, rank or route."""
    payload = campaign["payload"]
    hold = payload["hold_binding"]
    return {
        "contract_version": "transport-primary-operator-v1",
        "campaign_receipt_id": campaign["receipt_id"],
        "campaign_receipt_sha256": campaign["self_sha256"],
        "actor": hold["actor"], "activation_id": hold["activation_id"],
        "roster_snapshot_id": hold["roster_snapshot_id"],
        "roster_snapshot_hash": hold["roster_snapshot_hash"],
    }


def _current_campaign(connection: sqlite3.Connection, receipt_id: int, at: str) -> dict[str, Any]:
    campaign = read_transport_receipt(connection, receipt_id)
    payload = campaign["payload"]
    if (
        campaign["kind"] != "campaign" or payload.get("contract_version") != CAMPAIGN_CONTRACT
        or payload.get("arm") not in {"primary", *CONTROL_ARMS} or payload.get("sample_limit") != 20
        or payload.get("rank_start") != 1 or payload.get("operation") != "douyin_user_posts"
        or payload.get("scope_sequence") != 0
        or payload.get("unit_price_microusd") != PRICES_MICROUSD["douyin_user_posts"]
        or payload.get("max_cost_microusd") != 20 * PRICES_MICROUSD["douyin_user_posts"]
    ):
        raise DiagnosticMemberError("diagnostic_campaign_invalid", "Campaign is not the fixed primary sample")
    if not parse_time(payload["issued_at"]) <= parse_time(at) < parse_time(payload["expires_at"]):
        raise DiagnosticMemberError("diagnostic_campaign_expired", "Primary campaign is not within its issue window")
    hold = payload["hold_binding"]
    read_current_diagnostic_hold(connection, drain_id=hold["drain_id"], at=at, expected=hold)
    validate_request_transport_binding(payload["request_transport"])
    cohort = read_transport_receipt(connection, payload["cohort_receipt_id"])
    cohort_hold = cohort["payload"].get("hold_binding") if cohort["kind"] == "cohort" else None
    if (
        cohort["self_sha256"] != payload["cohort_receipt_sha256"]
        or cohort["kind"] != "cohort"
        or not isinstance(cohort_hold, Mapping)
        or (
            cohort_hold != hold
            if payload.get("arm") == "primary"
            else not same_hold_lineage(cohort_hold, hold)
        )
    ):
        raise DiagnosticMemberError("diagnostic_cohort_changed", "Primary cohort receipt binding changed")
    return campaign


def _operator_binding(
    connection: sqlite3.Connection, claim: durable_runs.DurableClaim, campaign: Mapping[str, Any],
) -> dict[str, Any]:
    details = durable_runs.assert_owner(connection, claim)
    expected = primary_operator_identity(campaign)
    if (
        details["identity"] != expected
        or claim.scan_id != durable_runs.scan_identity(OPERATOR_JOB, expected)
        or details.get("complete") is not False
    ):
        raise DiagnosticMemberError("diagnostic_operator_invalid", "Current owner is not this campaign's named operator")
    row = connection.execute(
        "SELECT invocation_source FROM scheduler_run_attempts WHERE id=?", (claim.attempt_id,),
    ).fetchone()
    if row is None or row["invocation_source"] != "operator_retry":
        raise DiagnosticMemberError("diagnostic_operator_invalid", "Diagnostic operator was not explicitly invoked")
    return {"claim": asdict(claim), "identity": expected}


def _members(connection: sqlite3.Connection, campaign_id: int) -> list[dict[str, Any]]:
    rows = connection.execute(
        "SELECT id FROM scheduler_runs WHERE job_id='transport_receipt:member_permit' "
        "AND json_extract(details_json,'$.payload.campaign_receipt_id')=? ORDER BY id",
        (campaign_id,),
    ).fetchall()
    members = [read_transport_receipt(connection, int(row["id"])) for row in rows]
    if any(
        row["payload"].get("contract_version") != CONTRACT_VERSION
        or row["payload"].get("rank") != index
        for index, row in enumerate(members, 1)
    ):
        raise DiagnosticMemberError("diagnostic_member_sequence_invalid", "Primary member ranks are not an immutable prefix")
    return members


def issue_primary_member_batch(
    connection: sqlite3.Connection, *, campaign_receipt_id: int,
    operator_claim: durable_runs.DurableClaim, at: str, mirror_root: Path,
) -> list[dict[str, Any]]:
    """Freeze all first 20 currently proven page scopes before any outcome exists.

    Insufficient naturally due members means no permits, never extra purchases.
    The runner must prepare the due scheduler inventory before invoking this
    issuer; it may not pass a hand-selected candidate list or a smaller cap.
    """
    if not connection.in_transaction:
        raise DiagnosticMemberError("diagnostic_transaction_required", "Member issue requires a transaction")
    writer = require_current_process_writer_lock(connection)
    campaign = _current_campaign(connection, campaign_receipt_id, at)
    operator = _operator_binding(connection, operator_claim, campaign)
    payload = campaign["payload"]
    existing = _members(connection, campaign_receipt_id)
    if existing:
        if len(existing) != 20 or any(
            item["payload"].get("operator") != operator or item["payload"].get("writer") != writer
            or item["payload"].get("campaign_receipt_sha256") != campaign["self_sha256"]
            for item in existing
        ):
            raise DiagnosticMemberError("diagnostic_member_batch_changed", "Existing primary batch cannot be replaced")
        return existing
    from .transport_due_candidates import list_primary_due_candidates

    cohort = read_transport_receipt(connection, payload["cohort_receipt_id"])
    allowed = set(cohort["payload"]["selected_identity_ids"])
    arm = str(payload["arm"])
    candidates = []
    for item in list_primary_due_candidates(connection, at=at):
        if item["scope"].identity_id not in allowed:
            continue
        identity = item["request_identity"].scope_identity
        used = connection.execute(
            "SELECT 1 FROM provider_usage WHERE lower(provider)='tikhub' "
            "AND json_extract(details_json,'$.paid_scope_identity')=? LIMIT 1", (identity,),
        ).fetchone()
        permitted = connection.execute(
            "SELECT 1 FROM scheduler_runs WHERE job_id='transport_receipt:member_permit' "
            "AND json_extract(details_json,'$.payload.paid_scope_identity')=? LIMIT 1", (identity,),
        ).fetchone()
        if arm != "primary" and (used or permitted):
            continue
        candidates.append(item)
        if len(candidates) == 20:
            break
    if len(candidates) != 20:
        raise DiagnosticMemberError("diagnostic_insufficient_natural_due", "Fewer than 20 frozen-cohort pages are naturally due")
    identities = [item["request_identity"].scope_identity for item in candidates]
    if len(set(identities)) != 20:
        raise DiagnosticMemberError("diagnostic_duplicate_scope", "Natural-due inventory has duplicate paid scopes")
    # A used leading candidate is an integrity/ownership conflict, not a reason
    # to select rank 21 after inspecting results.
    for identity in identities:
        used = connection.execute(
            "SELECT 1 FROM provider_usage WHERE lower(provider)='tikhub' "
            "AND json_extract(details_json,'$.paid_scope_identity')=? LIMIT 1", (identity,),
        ).fetchone()
        permitted = connection.execute(
            "SELECT 1 FROM scheduler_runs WHERE job_id='transport_receipt:member_permit' "
            "AND json_extract(details_json,'$.payload.paid_scope_identity')=? LIMIT 1", (identity,),
        ).fetchone()
        if arm == "primary" and (used or permitted):
            raise DiagnosticMemberError("diagnostic_scope_already_used", "A leading candidate already has usage or a permit")
    issued = []
    # A caller catching an FS/DB exception must not accidentally commit a
    # partial batch. Orphan mirrors remain immutable evidence, not permission.
    connection.execute("SAVEPOINT diagnostic_primary_batch")
    try:
        for rank, item in enumerate(candidates, 1):
            permit = {
                "contract_version": CONTRACT_VERSION, "campaign_receipt_id": campaign_receipt_id,
                "campaign_receipt_sha256": campaign["self_sha256"], "hold_binding": payload["hold_binding"],
                "rank": rank, "arm": arm, "operation": "douyin_user_posts",
                "paid_scope_identity": item["request_identity"].scope_identity, "sequence": 0,
                "natural_due": item["proof"], "request_transport": payload["request_transport"],
                "operator": operator, "writer": writer, "actor": payload["actor"],
                "unit_price_microusd": payload["unit_price_microusd"],
                "campaign_max_cost_microusd": payload["max_cost_microusd"],
                "issued_at": parse_time(at).isoformat(), "expires_at": payload["expires_at"],
            }
            issued.append(append_transport_receipt(
                connection, kind="member_permit", identity_key=f"{arm}:{campaign_receipt_id}:{rank:02}",
                payload=permit, at=at, mirror_root=mirror_root,
            ))
    except BaseException:
        connection.execute("ROLLBACK TO diagnostic_primary_batch")
        connection.execute("RELEASE diagnostic_primary_batch")
        raise
    connection.execute("RELEASE diagnostic_primary_batch")
    return issued


def validate_primary_member_for_send(
    connection: sqlite3.Connection, *, member_receipt_id: int,
    operator_claim: durable_runs.DurableClaim, scope: PaidScope,
    request_identity: PaidRequestIdentity, request_transport: Mapping[str, Any], at: str,
) -> dict[str, Any]:
    """Rederive one fixed member immediately before claim/send, without writes."""
    member = read_transport_receipt(connection, member_receipt_id)
    payload = member["payload"]
    if member["kind"] != "member_permit" or payload.get("contract_version") != CONTRACT_VERSION:
        raise DiagnosticMemberError("diagnostic_permit_invalid", "Receipt is not a current-HOLD diagnostic permit")
    campaign = _current_campaign(connection, payload["campaign_receipt_id"], at)
    if (
        payload.get("campaign_receipt_sha256") != campaign["self_sha256"]
        or payload.get("hold_binding") != campaign["payload"]["hold_binding"]
        or payload.get("arm") != campaign["payload"]["arm"] or payload.get("sequence") != 0
        or payload.get("operation") != "douyin_user_posts"
        or payload.get("actor") != campaign["payload"]["actor"]
        or payload.get("unit_price_microusd") != campaign["payload"]["unit_price_microusd"]
        or payload.get("campaign_max_cost_microusd") != campaign["payload"]["max_cost_microusd"]
        or payload.get("expires_at") != campaign["payload"]["expires_at"]
        or payload.get("operator") != _operator_binding(connection, operator_claim, campaign)
        or payload.get("writer") != require_current_process_writer_lock(connection)
        or payload.get("request_transport") != dict(request_transport)
        or payload.get("request_transport") != campaign["payload"]["request_transport"]
        or not parse_time(payload["issued_at"]) <= parse_time(at) < parse_time(payload["expires_at"])
    ):
        raise DiagnosticMemberError("diagnostic_permit_changed", "Diagnostic permit writer/owner/route/expiry changed")
    members = _members(connection, campaign["receipt_id"])
    rank = payload.get("rank")
    if type(rank) is not int or not 1 <= rank <= 20 or len(members) != 20 or members[rank - 1] != member:
        raise DiagnosticMemberError("diagnostic_permit_rank_invalid", "Permit is not in the complete frozen primary sample")
    # The fixed prefix is issued together, but its effective starts are serial.
    # A failure remains in its rank; it cannot be replaced by a later member.
    for predecessor in members[:rank - 1]:
        dispatches = connection.execute(
            "SELECT DISTINCT dispatch_id FROM paid_provider_dispatch_events "
            "WHERE json_extract(scope_json,'$.diagnostic_member.receipt_id')=?",
            (predecessor["receipt_id"],),
        ).fetchall()
        if len(dispatches) != 1:
            raise DiagnosticMemberError("diagnostic_rank_not_due", "Earlier primary rank has not completed its effective start")
        events = dispatch_events(connection, str(dispatches[0]["dispatch_id"]))
        expected_dispatch_binding = diagnostic_dispatch_binding(predecessor)
        if (
            events[-1].event_type not in {"succeeded", "failed", "billing_unknown"}
            or sum(event.event_type == "send_marked" for event in events) != 1
            or events[-1].provider.lower() != "tikhub"
            or events[-1].operation != "douyin_user_posts"
            or events[-1].activation_id != payload["hold_binding"]["activation_id"]
            or events[-1].permit_event_id != payload["hold_binding"]["dispatch_legacy_release_anchor"]["event_id"]
            or events[-1].scope.get("diagnostic_member") != expected_dispatch_binding
            or events[-1].cursor_identity.get("paid_scope_identity") != predecessor["payload"]["paid_scope_identity"]
            or events[-1].cursor_identity.get("sequence") != 0
            or parse_time(events[0].created_at) < parse_time(predecessor["payload"]["issued_at"])
            or parse_time(events[-1].created_at) > parse_time(at)
        ):
            raise DiagnosticMemberError("diagnostic_rank_not_due", "Earlier primary rank has no exact terminal dispatch receipt")
    proof = validate_natural_due_request(
        connection, scope=scope, request_identity=request_identity, stage="discovery", at=at,
    )
    if proof != payload.get("natural_due") or request_identity.scope_identity != payload.get("paid_scope_identity"):
        raise DiagnosticMemberError("diagnostic_permit_scope_changed", "Natural-due scope changed since member issue")
    return json.loads(json.dumps(member, sort_keys=True, allow_nan=False))
