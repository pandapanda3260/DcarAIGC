"""Conservative, evidence-backed accounting of completed HOLD diagnostics.

This is not a supplier bill confirmation. It retains the entire original charge,
unknown fetch-attempt billing, and retry guards. It does not authorize compensation.
"""

from __future__ import annotations

import hashlib
import json
import sqlite3
from pathlib import Path
from typing import Any

from apscheduler.schedulers.base import STATE_PAUSED, BaseScheduler  # type: ignore[import-untyped]

from . import paid_drain
from .capture import BILLING_UNKNOWN_SLOT_ERROR
from .provider_budget import PRICES_MICROUSD, micro_usd
from .raw_evidence import canonical_json_bytes
from .runtime_database import require_current_process_writer_lock
from .source_routing import parse_time
from .transport_campaign import CONTROL_ARMS
from .transport_evidence import DiagnosticEvidenceError, read_primary_member_evidence
from .transport_receipts import append_transport_receipt, read_transport_receipt

CONTRACT_VERSION = "diagnostic-conservative-accounting-v1"
_META_KEY = "diagnostic_accounting"


def _require(condition: bool, message: str) -> None:
    if not condition:
        raise DiagnosticEvidenceError(message)


def _sha(value: Any) -> str:
    return hashlib.sha256(canonical_json_bytes(value)).hexdigest()


def _member_arm(member: dict[str, Any]) -> str:
    arm = str(member["payload"].get("arm") or "primary")
    _require(arm == "primary" or arm in CONTROL_ARMS, "Diagnostic member arm is invalid")
    return arm


def _snapshot(connection: sqlite3.Connection, evidence: dict[str, Any]) -> dict[str, Any]:
    usage = connection.execute("SELECT * FROM provider_usage WHERE id=?", (evidence["usage_id"],)).fetchone()
    metadata = json.loads(usage["details_json"])
    attempt = connection.execute("SELECT * FROM fetch_attempts WHERE id=?", (evidence["fetch_attempt_id"],)).fetchone()
    slot = connection.execute("SELECT * FROM fetch_slots WHERE id=?", (attempt["slot_id"],)).fetchone()
    _require(usage["request_attempts"] == 1 and usage["billed_requests"] == 1 and usage["currency"] == "USD"
             and micro_usd(usage["amount"]) == PRICES_MICROUSD[usage["operation"]]
             and attempt["billed"] == 0 and attempt["amount"] is None and attempt["currency"] == "USD"
             and slot["status"] != "running" and slot["last_error_code"] == BILLING_UNKNOWN_SLOT_ERROR,
             "Unknown diagnostic no longer retains its original full reservation and retry guard")
    normalized = dict(metadata)
    normalized.pop(_META_KEY, None)
    normalized["state"] = "billing_unknown"
    return {
        "usage": {key: usage[key] for key in usage.keys() if key != "details_json"},
        "metadata_sha256": _sha(normalized),
        "fetch_attempt": dict(attempt),
        "slot_guard": {key: slot[key] for key in ("id", "status", "last_error_code", "last_error_message")},
        "evidence": evidence,
    }


def _closed_campaign(connection: sqlite3.Connection, member: dict[str, Any], at: str) -> dict[str, Any]:
    payload = member["payload"]
    arm = _member_arm(member)
    row = connection.execute(
        "SELECT id FROM scheduler_runs WHERE job_id='transport_receipt:campaign_terminal' AND scheduled_for=?",
        (f"{arm}-execution:{payload['campaign_receipt_id']}",),
    ).fetchone()
    _require(row is not None, "Diagnostic campaign has not finished")
    terminal = read_transport_receipt(connection, row["id"])
    claim = payload["operator"]["claim"]
    _require(terminal["payload"]["operator_claim"] == claim
             and terminal["payload"]["campaign_receipt_sha256"] == payload["campaign_receipt_sha256"]
             and any(item == {"receipt_id": member["receipt_id"], "receipt_sha256": member["self_sha256"],
                              "rank": payload["rank"]} for item in terminal["payload"]["members"])
             and parse_time(terminal["recorded_at"]) <= parse_time(at),
             "Diagnostic terminal receipt differs from member ownership")
    owner = connection.execute(
        "SELECT r.status,r.details_json,a.status attempt_status,a.completed_at FROM scheduler_runs r "
        "JOIN scheduler_run_attempts a ON a.scheduler_run_id=r.id WHERE r.id=? AND a.id=?",
        (claim["scheduler_run_id"], claim["attempt_id"]),
    ).fetchone()
    _require(owner is not None and owner["status"] == "succeeded" and owner["attempt_status"] == "succeeded"
             and owner["completed_at"] is not None, "Diagnostic operator has not reached its original terminal")
    details = json.loads(owner["details_json"])
    _require(details["checkpoint"].get("primary_terminal_receipt_id") == terminal["receipt_id"],
             "Diagnostic operator terminal receipt changed")
    return terminal


def read_primary_member_accounting(
    connection: sqlite3.Connection, member_receipt_id: int, *, at: str,
) -> dict[str, Any]:
    """Re-read bytes and immutable accounting; never trust mutable state alone."""
    evidence = read_primary_member_evidence(connection, member_receipt_id, at=at)
    if evidence["state"] != "billing_unknown":
        return {**evidence, "accounting_terminal": evidence["state"] in {"succeeded", "failed", "not_sent", "not_started"},
                "provider_bill_verified": evidence.get("billing_settled", False)}
    usage = connection.execute("SELECT details_json FROM provider_usage WHERE id=?", (evidence["usage_id"],)).fetchone()
    metadata = json.loads(usage["details_json"])
    if metadata.get("state") == "billing_unknown" and _META_KEY not in metadata:
        return {**evidence, "accounting_terminal": False, "provider_bill_verified": False}
    binding = metadata.get(_META_KEY)
    _require(metadata.get("state") == "charged_unverified" and isinstance(binding, dict),
             "Unknown diagnostic state lacks immutable conservative accounting")
    receipt = read_transport_receipt(connection, binding["receipt_id"])
    payload = receipt["payload"]
    member = read_transport_receipt(connection, member_receipt_id)
    terminal = _closed_campaign(connection, member, at)
    _require(receipt["kind"] == "accounting_terminal"
             and receipt["identity_key"] == f"{_member_arm(member)}-unknown:{member_receipt_id}"
             and receipt["self_sha256"] == binding["receipt_sha256"]
             and binding == {"receipt_id": receipt["receipt_id"], "receipt_sha256": receipt["self_sha256"],
                             "contract_version": CONTRACT_VERSION}
             and payload.get("contract_version") == CONTRACT_VERSION
             and payload.get("member_receipt_id") == member_receipt_id
             and payload.get("member_receipt_sha256") == member["self_sha256"]
             and payload.get("campaign_terminal_id") == terminal["receipt_id"]
             and payload.get("campaign_terminal_sha256") == terminal["self_sha256"]
             and payload.get("hold_binding") == member["payload"]["hold_binding"]
             and payload.get("provider_bill_verified") is False
             and payload.get("accounting_policy") == "retain_full_reserved_amount"
             and payload.get("compensation_authorized") is False
             and payload.get("snapshot") == _snapshot(connection, evidence)
             and parse_time(terminal["recorded_at"]) <= parse_time(receipt["recorded_at"]) <= parse_time(at),
             "Conservative accounting no longer matches its immutable source evidence")
    return {**evidence, "accounting_terminal": True, "provider_bill_verified": False,
            "accounting_state": "charged_unverified", "accounting_receipt_id": receipt["receipt_id"],
            "accounting_receipt_sha256": receipt["self_sha256"]}


def settle_primary_member_unknown(
    connection: sqlite3.Connection, member_receipt_id: int, *, scheduler: BaseScheduler,
    at: str, mirror_root: Path,
) -> dict[str, Any]:
    """Account once under the actual paused writer; no invoice/retry side effects."""
    _require(connection.in_transaction, "Conservative accounting requires a caller transaction")
    require_current_process_writer_lock(connection)
    _require(isinstance(scheduler, BaseScheduler) and scheduler.state == STATE_PAUSED,
             "Conservative diagnostic accounting requires the paused scheduler")
    member = read_transport_receipt(connection, member_receipt_id)
    hold = member["payload"]["hold_binding"]
    state = paid_drain.dispatch_state(connection, at=at)
    _require(state.state == "draining" and state.drain_id == hold["drain_id"]
             and state.last_event_id == hold["start_event_id"] and state.last_event_hash == hold["start_event_hash"],
             "Diagnostic accounting requires its current unsealed START")
    evidence = read_primary_member_evidence(connection, member_receipt_id, at=at)
    _require(evidence["state"] == "billing_unknown", "Conservative accounting only applies to unknown diagnostic sends")
    usage = connection.execute("SELECT details_json FROM provider_usage WHERE id=?", (evidence["usage_id"],)).fetchone()
    metadata = json.loads(usage["details_json"])
    if metadata.get("state") != "billing_unknown" or _META_KEY in metadata:
        return read_primary_member_accounting(connection, member_receipt_id, at=at)
    terminal = _closed_campaign(connection, member, at)
    snapshot = _snapshot(connection, evidence)
    connection.execute("SAVEPOINT diagnostic_accounting")
    try:
        receipt = append_transport_receipt(
            connection, kind="accounting_terminal", identity_key=f"{_member_arm(member)}-unknown:{member_receipt_id}",
            payload={"contract_version": CONTRACT_VERSION, "member_receipt_id": member_receipt_id,
                     "member_receipt_sha256": member["self_sha256"], "hold_binding": hold,
                     "campaign_terminal_id": terminal["receipt_id"], "campaign_terminal_sha256": terminal["self_sha256"],
                     "provider_bill_verified": False, "accounting_policy": "retain_full_reserved_amount",
                     "compensation_authorized": False, "snapshot": snapshot}, at=at, mirror_root=mirror_root,
        )
        metadata.update({"state": "charged_unverified", _META_KEY: {
            "receipt_id": receipt["receipt_id"], "receipt_sha256": receipt["self_sha256"], "contract_version": CONTRACT_VERSION,
        }})
        updated = connection.execute(
            "UPDATE provider_usage SET details_json=? WHERE id=? AND details_json=?",
            (canonical_json_bytes(metadata).decode().rstrip("\n"), evidence["usage_id"], usage["details_json"]),
        )
        _require(updated.rowcount == 1, "Unknown diagnostic changed during conservative accounting")
        result = read_primary_member_accounting(connection, member_receipt_id, at=at)
    except BaseException:
        connection.execute("ROLLBACK TO diagnostic_accounting")
        connection.execute("RELEASE diagnostic_accounting")
        raise
    connection.execute("RELEASE diagnostic_accounting")
    return result


def settle_closed_hold_unknowns(
    connection: sqlite3.Connection, *, drain_id: str, scheduler: BaseScheduler,
    at: str, mirror_root: Path,
) -> list[dict[str, Any]]:
    """Close only verified terminal diagnostic owners in this active HOLD.

    Inspect all owners before any accounting mutation. Missing terminals and
    active owners cannot be repaired by this helper. Original dispatch unknowns,
    full reserved charges and request retry guards remain unchanged.
    """
    from .transport_owner_evidence import read_primary_campaign_owners

    _require(connection.in_transaction, "HOLD accounting requires the caller transaction")
    require_current_process_writer_lock(connection)
    _require(isinstance(scheduler, BaseScheduler) and scheduler.state == STATE_PAUSED,
             "HOLD accounting requires the paused Writer")
    state = paid_drain.dispatch_state(connection, at=at)
    _require(state.state == "draining" and state.drain_id == drain_id,
             "HOLD accounting requires the current unsealed START")
    pending: list[int] = []
    for row in connection.execute(
        "SELECT id FROM scheduler_runs WHERE job_id='transport_receipt:campaign' "
        "AND json_extract(details_json,'$.payload.hold_binding.start_event_hash')=? ORDER BY id",
        (state.last_event_hash,),
    ):
        campaign = read_transport_receipt(connection, row["id"])
        _require(campaign["payload"]["hold_binding"]["drain_id"] == drain_id,
                 "Diagnostic campaign belongs to a different HOLD")
        owners = read_primary_campaign_owners(connection, row["id"], at=at)
        for member in owners["members"]:
            evidence = read_primary_member_accounting(connection, member["member_receipt_id"], at=at)
            if not evidence["accounting_terminal"]:
                _require(evidence["state"] == "billing_unknown", "Only an unknown terminal can receive conservative accounting")
                pending.append(member["member_receipt_id"])
    results = []
    for member_id in pending:
        result = settle_primary_member_unknown(connection, member_id, scheduler=scheduler, at=at, mirror_root=mirror_root)
        results.append({"member_receipt_id": member_id, "receipt_id": result["accounting_receipt_id"],
                        "receipt_sha256": result["accounting_receipt_sha256"], "provider_bill_verified": False})
    return results
