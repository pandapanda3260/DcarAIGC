"""Fixed primary route verdict, explicitly distinct from operation qualification.

Reads all twenty original ranks and current evidence. No caller-supplied counts,
fallback, provider invocation, fault clearing, or RELEASE is possible here.
"""

from __future__ import annotations

import hashlib
import sqlite3
from pathlib import Path
from typing import Any

from . import paid_drain
from .raw_evidence import canonical_json_bytes
from .runtime_database import require_current_process_writer_lock
from .source_routing import parse_time
from .transport_accounting import read_primary_member_accounting
from .transport_campaign import CONTRACT_VERSION as CAMPAIGN_CONTRACT
from .transport_evidence import DiagnosticEvidenceError
from .transport_hold_binding import read_current_diagnostic_hold
from .transport_owner_evidence import read_primary_campaign_owners
from .transport_receipts import append_transport_receipt, read_transport_receipt

CONTRACT_VERSION = "transport-primary-route-verdict-v1"


def record_primary_route_verdict(
    connection: sqlite3.Connection, campaign_receipt_id: int, *, at: str, mirror_root: Path,
) -> dict[str, Any]:
    """Persist one evidence-derived verdict without enabling any paid operation."""
    if not connection.in_transaction:
        raise DiagnosticEvidenceError("Route verdict requires a caller transaction")
    require_current_process_writer_lock(connection)
    campaign = read_transport_receipt(connection, campaign_receipt_id)
    header = campaign["payload"]
    arm = str(header.get("arm") or "primary")
    if campaign["kind"] != "campaign" or header.get("contract_version") != CAMPAIGN_CONTRACT:
        raise DiagnosticEvidenceError("Primary route verdict requires its frozen campaign")
    hold = read_current_diagnostic_hold(connection, drain_id=header["hold_binding"]["drain_id"],
                                        at=at, expected=header["hold_binding"])
    owners = read_primary_campaign_owners(connection, campaign_receipt_id, at=at)
    terminal = read_transport_receipt(connection, owners["terminal_receipt_id"])
    members = [read_primary_member_accounting(connection, item["member_receipt_id"], at=at)
               for item in owners["members"]]
    if len(members) != 20 or [item["rank"] for item in members] != list(range(1, 21)):
        raise DiagnosticEvidenceError("Primary route verdict cannot change the twenty-rank denominator")
    if any(not item["accounting_terminal"] for item in members):
        raise DiagnosticEvidenceError("Primary route verdict requires accounted diagnostic terminals")
    # An otherwise valid campaign cannot hide orphan requests/owners elsewhere
    # in the same HOLD, including a previous build generation's failed samples.
    paid_drain.verify_profile_drain_sealable(connection, hold["drain_id"], now=at)
    starts = sum(item["effective_starts"] for item in members)
    complete = sum(item.get("response_complete", False) for item in members)
    usable = sum(item["state"] == "succeeded" and item.get("response_complete") is True
                 and owner["materialization_run_id"] is not None
                 for item, owner in zip(members, owners["members"], strict=True))
    unknown = sum(item["state"] == "billing_unknown" for item in members)
    uncertain = sum(item["effective_starts"] == 1 and item.get("response_complete") is not True for item in members)
    passed = starts == 20 and usable == 20
    status = "passed" if passed else "incomplete" if starts < 20 else "failed"
    next_action = (
        "freeze_operation_samples"
        if passed else "remain_blocked"
        if starts < 20 or arm != "primary" else "run_disjoint_control_arms"
    )
    payload = {
        "contract_version": CONTRACT_VERSION, "campaign_receipt_id": campaign_receipt_id,
        "campaign_receipt_sha256": campaign["self_sha256"], "hold_binding": hold,
        "campaign_terminal_id": terminal["receipt_id"], "campaign_terminal_sha256": terminal["self_sha256"],
        "owner_evidence_sha256": hashlib.sha256(canonical_json_bytes(owners)).hexdigest(),
        "member_evidence_sha256": hashlib.sha256(canonical_json_bytes(members)).hexdigest(),
        "status": status, "sample_limit": 20, "effective_starts": starts,
        "response_complete_count": complete, "usable_page_count": usable,
        "transport_uncertain_count": uncertain, "original_billing_unknown_count": unknown,
        "accounted_microusd": sum(item.get("amount_microusd", 0) for item in members),
        "route_passed": passed,
        "selected_route": header["request_transport"]["manifest"] if passed else None,
        "next_action": next_action,
        "operation_qualified": False, "required_operation_sample_size": 200,
        "ordinary_paid_authorized": False,
    }
    key = (
        f"primary-route:{campaign_receipt_id}"
        if arm == "primary"
        else f"{arm}-route:{campaign_receipt_id}"
    )
    existing = connection.execute(
        "SELECT id FROM scheduler_runs WHERE job_id='transport_receipt:route_verdict' AND scheduled_for=?", (key,),
    ).fetchone()
    if existing is not None:
        prior = read_transport_receipt(connection, existing["id"])
        if prior["payload"] != payload or parse_time(prior["recorded_at"]) > parse_time(at):
            raise DiagnosticEvidenceError("Primary route verdict evidence changed after finalization")
        return prior
    return append_transport_receipt(connection, kind="route_verdict", identity_key=key,
                                    payload=payload, at=at, mirror_root=mirror_root)
