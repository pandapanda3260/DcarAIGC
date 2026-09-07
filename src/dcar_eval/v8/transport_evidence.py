"""Read-only, historical diagnostic dispatch/byte evidence.

Unlike the send gate, this reader accepts an expired member that was valid when
sent and never requires its former owner to be running. It does not authorize
SEAL/RELEASE, settle unknown charges, or qualify an operation.
"""

from __future__ import annotations

import hashlib
import json
import sqlite3
from pathlib import Path
from typing import Any

from . import capture, paid_drain
from .paid_dispatch import dispatch_events
from .paid_identity import build_paid_request_identity
from .provider_budget import PRICES_MICROUSD, micro_usd
from .raw_evidence import (
    MAX_RAW_BYTES, MAX_SIDECAR_BYTES, PAID_SEND_CLAIM_SCHEMA,
    _read_single_regular, canonical_json_bytes, read_raw_evidence,
)
from .source_routing import parse_time
from .transport_campaign import CONTRACT_VERSION as CAMPAIGN_CONTRACT
from .transport_hold_binding import diagnostic_dispatch_binding
from .transport_members import CONTRACT_VERSION as MEMBER_CONTRACT
from .transport_receipts import read_transport_receipt


class DiagnosticEvidenceError(RuntimeError):
    pass


def _require(condition: bool, message: str) -> None:
    if not condition:
        raise DiagnosticEvidenceError(message)


def _file(path: str, sha256: str, size: int, *, limit: int) -> bytes:
    body = _read_single_regular(Path(path), max_bytes=limit)
    _require(len(body) == size and hashlib.sha256(body).hexdigest() == sha256,
             "Diagnostic evidence file differs from its immutable hash/size")
    return body


def read_primary_member_evidence(
    connection: sqlite3.Connection, member_receipt_id: int, *, at: str,
) -> dict[str, Any]:
    member = read_transport_receipt(connection, member_receipt_id)
    payload = member["payload"]
    campaign = read_transport_receipt(connection, payload["campaign_receipt_id"])
    hold = payload["hold_binding"]
    natural = payload["natural_due"]
    scope = natural["scope_identity"]
    document = natural["request_document"]
    operation = payload["operation"]
    request = build_paid_request_identity(**{key: document[key] for key in (
        "provider", "operation", "platform", "subject", "request_parameters", "cursor", "due_bucket", "request_window",
    )})
    _require(
        member["kind"] == "member_permit" and payload.get("contract_version") == MEMBER_CONTRACT
        and campaign["kind"] == "campaign" and campaign["payload"].get("contract_version") == CAMPAIGN_CONTRACT
        and payload["campaign_receipt_sha256"] == campaign["self_sha256"]
        and hold == campaign["payload"]["hold_binding"]
        and payload["request_transport"] == campaign["payload"]["request_transport"]
        and payload["arm"] == campaign["payload"]["arm"] and payload["sequence"] == 0
        and request.document == document and request.scope_identity == payload["paid_scope_identity"]
        and operation == "douyin_user_posts" and 1 <= payload["rank"] <= 20
        and payload["unit_price_microusd"] == PRICES_MICROUSD[operation],
        "Member differs from its frozen primary campaign",
    )
    chain = paid_drain._validated_profile_chain(connection)
    starts = [event for event in chain if event.event_id == hold["start_event_id"]]
    _require(len(starts) == 1, "Diagnostic START is missing")
    start = starts[0]
    anchor = hold["dispatch_legacy_release_anchor"]
    _require(
        start.event_type == "start" and start.event_hash == hold["start_event_hash"]
        and start.drain_id == hold["drain_id"] and start.target_activation_id == hold["activation_id"]
        and start.previous_event_id == anchor["event_id"] and start.previous_event_hash == anchor["event_hash"]
        and anchor["role"] == "historical_lineage_only"
        and any(event.event_id == anchor["event_id"] and event.event_type == "release"
                and event.event_hash == anchor["event_hash"] and event.target_activation_id == hold["activation_id"]
                for event in chain),
        "Diagnostic START or historical RELEASE lineage changed",
    )
    member_binding = diagnostic_dispatch_binding(member)
    rows = connection.execute(
        "SELECT DISTINCT dispatch_id FROM paid_provider_dispatch_events "
        "WHERE json_extract(scope_json,'$.diagnostic_member.receipt_id')=?", (member_receipt_id,),
    ).fetchall()
    usage_rows = connection.execute(
        "SELECT * FROM provider_usage WHERE json_extract(details_json,'$.diagnostic_member.receipt_id')=?",
        (member_receipt_id,),
    ).fetchall()
    if not rows:
        _require(not usage_rows, "Unexplained member usage without dispatch")
        return {"member_receipt_id": member_receipt_id, "rank": payload["rank"],
                "state": "not_started", "effective_starts": 0, "qualified": False}
    _require(len(rows) == 1 and len(usage_rows) == 1, "Member has duplicate or missing dispatch/usage")
    events = dispatch_events(connection, rows[0]["dispatch_id"])
    first, terminal = events[0], events[-1]
    usage = usage_rows[0]
    metadata = json.loads(usage["details_json"])
    _require(
        first.provider.lower() == "tikhub" and first.operation == operation
        and first.activation_id == hold["activation_id"]
        and first.scheduler_run_id == scope["scheduler_run_id"]
        and first.scheduler_attempt_id == scope["scheduler_attempt_id"]
        and first.business_day == scope["business_day"]
        and first.permit_event_id == anchor["event_id"]
        and first.scope.get("diagnostic_member") == member_binding
        and first.scope.get("request_transport") == payload["request_transport"]
        and first.provider_usage_id == usage["id"]
        and usage["provider"].lower() == "tikhub" and usage["operation"] == operation
        and usage["currency"] == "USD" and metadata.get("diagnostic_member") == member_binding
        and metadata.get("request_transport") == payload["request_transport"]
        and metadata.get("slot_id") == first.fetch_slot_id
        and metadata.get("paid_scope_identity") == payload["paid_scope_identity"]
        and metadata.get("paid_execution_identity") == request.execution_identity
        and metadata.get("paid_sequence") == 0 and metadata.get("paid_identity") == document
        and first.cursor_identity == {
            "paid_scope_identity": payload["paid_scope_identity"],
            "paid_execution_identity": metadata.get("paid_execution_identity"), "sequence": 0, "request": document,
        }
        and parse_time(payload["issued_at"]) <= parse_time(first.created_at) < parse_time(payload["expires_at"])
        and parse_time(terminal.created_at) <= parse_time(at),
        "Member dispatch independent columns or request identity changed",
    )
    _require(terminal.event_type in {"not_sent", "succeeded", "failed", "billing_unknown"},
             "Member dispatch is still in flight")
    if terminal.event_type == "not_sent":
        _require(len(events) == 2 and usage["request_attempts"] == 0 and usage["billed_requests"] == 0
                 and micro_usd(usage["amount"]) == 0 and metadata.get("state") == "not_sent"
                 and terminal.fetch_attempt_id is None and terminal.raw_response_id is None
                 and metadata.get("paid_send_claim_path") is None,
                 "Unsent diagnostic contains a charge, marker or attempt")
        return {"member_receipt_id": member_receipt_id, "rank": payload["rank"], "state": "not_sent",
                "usage_id": usage["id"], "dispatch_event_ids": [event.event_id for event in events],
                "effective_starts": 0, "amount_microusd": 0, "qualified": False}

    _require(len(events) == 3 and events[1].event_type == "send_marked" and usage["request_attempts"] == 1,
             "Effective diagnostic start is not unique")
    sent = events[1]
    _require(all(event.provider_usage_id == first.provider_usage_id and event.fetch_slot_id == first.fetch_slot_id
                 and event.cursor_identity == first.cursor_identity for event in events)
             and terminal.fetch_attempt_id == sent.fetch_attempt_id,
             "Diagnostic dispatch chain resource identity changed")
    _require(parse_time(payload["issued_at"]) <= parse_time(sent.created_at) < parse_time(payload["expires_at"])
             and parse_time(first.created_at) <= parse_time(sent.created_at) <= parse_time(terminal.created_at)
             and metadata.get("sent_at") == sent.created_at,
             "Diagnostic was sent outside its permit window")
    attempt = connection.execute("SELECT * FROM fetch_attempts WHERE id=?", (sent.fetch_attempt_id,),).fetchone()
    slot = connection.execute("SELECT * FROM fetch_slots WHERE id=?", (sent.fetch_slot_id,),).fetchone()
    _require(attempt is not None and slot is not None and attempt["slot_id"] == slot["id"]
             and attempt["attempt_number"] == metadata.get("attempt_number")
             and slot["account_id"] == scope["account_id"] and slot["content_id"] is None
             and slot["window_key"] == document["due_bucket"] and slot["stage"] == "discovery"
             and slot["provider"].lower() == "tikhub" and attempt["response_finished_at"] is not None,
             "Diagnostic fetch attempt or slot differs from its member")
    main = next(row for row in connection.execute("PRAGMA database_list") if row[1] == "main")
    claim_root = Path(main[2]).parent / "paid_send_claims"
    paid_id = payload["paid_scope_identity"]
    expected_path = claim_root / paid_id[:2] / f"{paid_id}.sequence-00000000.claim.json"
    _require(Path(metadata["paid_send_claim_path"]).resolve() == expected_path.resolve(), "Send claim path is not canonical")
    claim_body = _file(str(expected_path), metadata["paid_send_claim_sha256"], metadata["paid_send_claim_bytes"], limit=MAX_SIDECAR_BYTES)
    expected_claim = {
        "schema": PAID_SEND_CLAIM_SCHEMA, "paid_scope_identity": paid_id, "sequence": 0,
        "claim": {"operation": operation, "provider": "tikhub",
                  "paid_execution_identity": metadata["paid_execution_identity"],
                  "provider_usage_id": usage["id"], "slot_id": slot["id"], "created_at": sent.created_at,
                  "request_transport": payload["request_transport"], "diagnostic_member": member_binding},
    }
    _require(claim_body == canonical_json_bytes(expected_claim), "Send claim payload changed")
    transport = metadata.get("transport")
    _require(isinstance(transport, dict), "Diagnostic transport receipt is missing")
    manifest = payload["request_transport"]["manifest"]
    _require(all(transport.get(key) == manifest[key] for key in (
        "request_host", "transport_route_id", "route_generation", "http_stack",
    )), "Diagnostic response route differs from its member")
    response_complete = False
    if terminal.raw_response_id is not None:
        raw = connection.execute("SELECT * FROM provider_raw_responses WHERE id=?", (terminal.raw_response_id,),).fetchone()
        _require(raw is not None and raw["fetch_attempt_id"] == attempt["id"]
                 and raw["account_id"] == scope["account_id"] and raw["provider"].lower() == "tikhub"
                 and raw["operation"] == operation, "Diagnostic raw identity changed")
        raw_path = Path(raw["local_path"])
        if not raw_path.is_absolute():
            raw_path = capture.PROJECT_ROOT / raw_path
        stored = read_raw_evidence(raw_path)
        _require(stored.receipt.stored_sha256 == raw["sha256"] and stored.receipt.stored_size == raw["byte_size"],
                 "Diagnostic raw storage hash/size changed")
        _require(transport.get("raw_response_id") == raw["id"]
                 and transport.get("stored_path") == raw["local_path"]
                 and transport.get("stored_sha256") == raw["sha256"]
                 and transport.get("stored_bytes") == raw["byte_size"],
                 "Diagnostic raw differs from the transport storage receipt")
        capture._validate_complete_transport_receipt(transport, entity_bytes=stored.entity_bytes, http_status=raw["http_status"])
        response_complete = True
    else:
        _require(terminal.event_type != "succeeded", "Successful diagnostic has no raw")
        receipt_keys = {"quarantine_receipt_path", "quarantine_receipt_sha256", "quarantine_receipt_bytes"}
        receipt_body = _file(transport["quarantine_receipt_path"], transport["quarantine_receipt_sha256"],
                             transport["quarantine_receipt_bytes"], limit=MAX_SIDECAR_BYTES)
        _require(receipt_body == canonical_json_bytes({key: value for key, value in transport.items() if key not in receipt_keys}),
                 "Diagnostic quarantine receipt differs from usage")
        if transport.get("zero_body") is True:
            _require(transport.get("partial_bytes") == 0 and transport.get("quarantine_path") is None,
                     "Zero-byte evidence includes unexplained partial bytes")
        else:
            _file(transport["quarantine_path"], transport["partial_sha256"], transport["partial_bytes"], limit=MAX_RAW_BYTES)

    amount = micro_usd(usage["amount"])
    # Unknown accounting needs its own durable settlement verifier; this reader
    # never upgrades it from a mutated usage state or guesses supplier billing.
    settled = terminal.event_type != "billing_unknown"
    if settled:
        _require(metadata.get("state") in {"completed", "failed"}
                 and usage["billed_requests"] in (0, 1) and attempt["billed"] == usage["billed_requests"]
                 and amount == (PRICES_MICROUSD[operation] if usage["billed_requests"] else 0)
                 and attempt["amount"] is not None and micro_usd(attempt["amount"]) == amount,
                 "Known diagnostic charge does not close against its fetch attempt")
    return {
        "member_receipt_id": member_receipt_id, "rank": payload["rank"], "state": terminal.event_type,
        "usage_id": usage["id"], "fetch_attempt_id": attempt["id"], "raw_response_id": terminal.raw_response_id,
        "dispatch_event_ids": [event.event_id for event in events], "effective_starts": 1,
        "send_claim_sha256": metadata["paid_send_claim_sha256"], "response_complete": response_complete,
        "billing_settled": settled, "amount_microusd": amount, "qualified": False,
    }
