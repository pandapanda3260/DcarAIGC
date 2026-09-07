"""Historical, read-only closure of a primary campaign's original owners.

Partial means the one-page diagnostic yielded its remaining natural work; it
does not mean a successful page may leave local materialization unfinished.
This reader neither resumes owners nor settles charges or releases the HOLD.
"""

from __future__ import annotations

import json
import re
import sqlite3
from dataclasses import asdict
from typing import Any

from . import durable_runs, pipeline, tikhub_scan, transport_natural_due as natural
from .raw_evidence import MAX_SIDECAR_BYTES
from .source_routing import parse_time
from .transport_evidence import DiagnosticEvidenceError, _file, read_primary_member_evidence
from .transport_campaign import CONTROL_ARMS
from .transport_members import OPERATOR_JOB, _members, primary_operator_identity
from .transport_preparation import CONTRACT_VERSION as INVENTORY_CONTRACT
from .transport_receipts import read_transport_receipt
from .transport_runner import CONTRACT_VERSION as EXECUTION_CONTRACT

CONTRACT_VERSION = "transport-primary-owner-evidence-v1"
_SCOPE_FIELDS = (
    "pipeline_version", "beijing_day", "registration_id", "scheduled_at", "activation_id", "profile_id",
)
_EPOCH_FIELDS = ("activation_id", "activation_sha256", "profile_id", "roster_snapshot_id", "roster_snapshot_hash")


class DiagnosticOwnerEvidenceError(DiagnosticEvidenceError):
    pass


def _require(condition: bool, message: str) -> None:
    if not condition:
        raise DiagnosticOwnerEvidenceError(message)


def _claim(value: dict[str, Any]) -> durable_runs.DurableClaim:
    claim = durable_runs.DurableClaim(**value)
    _require(all(type(value[key]) is int and value[key] > 0 for key in (
        "scheduler_run_id", "attempt_id", "attempt_number",
    )) and isinstance(claim.owner_token, str) and bool(claim.owner_token)
        and isinstance(claim.scan_id, str), "Original durable claim is malformed")
    return claim


def _terminal_owner(
    connection: sqlite3.Connection, claim: durable_runs.DurableClaim, *, job: str,
    at: str, invocation: str = "operator_retry", historical: bool = False,
) -> dict[str, Any]:
    run = connection.execute("SELECT * FROM scheduler_runs WHERE id=?", (claim.scheduler_run_id,)).fetchone()
    attempts = connection.execute(
        "SELECT * FROM scheduler_run_attempts WHERE scheduler_run_id=? ORDER BY attempt_number,id",
        (claim.scheduler_run_id,),
    ).fetchall()
    _require(run is not None and bool(attempts), "Original owner run/attempt is missing")
    matches = [row for row in attempts if row["id"] == claim.attempt_id]
    _require(len(matches) == 1, "Original owner attempt is missing")
    attempt = matches[0]
    _require((historical or attempt["id"] == attempts[-1]["id"]) and attempt["attempt_number"] == claim.attempt_number
             and all(row["status"] != "running" for row in attempts),
             "Original owner has a running or later attempt")
    if historical and attempt["id"] != attempts[-1]["id"]:
        # The immutable original attempt remains the paid authority even after
        # an independently verified local-only continuation replaces the row.
        run = dict(run) | {key: attempt[key] for key in ("status", "started_at", "completed_at", "details_json")}
    details = json.loads(run["details_json"])
    state, identity = details["checkpoint"], details["identity"]
    _require(
        run["job_id"] == job and run["status"] in {"succeeded", "partial", "failed", "interrupted"}
        and run["status"] == attempt["status"] and run["details_json"] == attempt["details_json"]
        and attempt["invocation_source"] == invocation
        and run["started_at"] == attempt["started_at"] == details["claimed_at"]
        and run["completed_at"] == attempt["completed_at"] == details["completed_at"]
        and parse_time(run["started_at"]) <= parse_time(run["completed_at"]) <= parse_time(at)
        and details["contract_version"] == durable_runs.CONTRACT_VERSION
        and details["scan_id"] == claim.scan_id == durable_runs.scan_identity(job, identity)
        and details["owner"] == {"token": claim.owner_token, "attempt_id": claim.attempt_id,
                                 "attempt_number": claim.attempt_number}
        and type(state["complete"]) is bool and details["complete"] is state["complete"]
        and (run["status"] == "succeeded") == state["complete"]
        and run["scheduled_for"] == "scan:" + durable_runs.scan_identity(job, details.get("scope_key", identity)),
        "Original owner terminal identity, token or immutable attempt changed",
    )
    return {"claim": asdict(claim), "row": dict(run), "details": details}


def _owner_chain(
    connection: sqlite3.Connection, source: dict[str, Any], *, at: str, invocation: str,
) -> list[dict[str, Any]]:
    rows = connection.execute(
        "SELECT * FROM scheduler_run_attempts WHERE scheduler_run_id=? AND attempt_number>=? ORDER BY attempt_number,id",
        (source["claim"]["scheduler_run_id"], source["claim"]["attempt_number"]),
    ).fetchall()
    chain: list[dict[str, Any]] = []
    for index, row in enumerate(rows):
        details = json.loads(row["details_json"])
        owner = details["owner"]
        claim = _claim({"scheduler_run_id": row["scheduler_run_id"], "attempt_id": row["id"],
                        "attempt_number": row["attempt_number"], "owner_token": owner["token"], "scan_id": details["scan_id"]})
        _require(row["attempt_number"] == source["claim"]["attempt_number"] + index,
                 "Local owner chain skipped an attempt")
        item = _terminal_owner(connection, claim, job=source["row"]["job_id"], at=at,
                               invocation=invocation, historical=index != len(rows) - 1)
        _require(item["details"]["identity"] == source["details"]["identity"]
                 and (not chain or parse_time(chain[-1]["row"]["completed_at"]) <= parse_time(row["started_at"])),
                 "Local owner continuation changed identity or overlapped")
        chain.append(item)
    return chain


def _no_paid_continuation(connection: sqlite3.Connection, attempt_ids: list[int]) -> None:
    for attempt_id in attempt_ids:
        _require(connection.execute(
            "SELECT 1 FROM provider_usage WHERE json_extract(details_json,'$.scope.scheduler_attempt_id')=? LIMIT 1",
            (attempt_id,),
        ).fetchone() is None and connection.execute(
            "SELECT 1 FROM paid_provider_dispatch_events WHERE scheduler_attempt_id=? LIMIT 1", (attempt_id,),
        ).fetchone() is None, "Local-only continuation has a paid reservation or dispatch")


def _parent(connection: sqlite3.Connection, claim: durable_runs.DurableClaim, *, at: str, prepared: str) -> dict[str, Any]:
    row = connection.execute("SELECT job_id FROM scheduler_runs WHERE id=?", (claim.scheduler_run_id,)).fetchone()
    _require(row is not None, "Inventory parent is missing")
    owner = _terminal_owner(connection, claim, job=row["job_id"], at=at)
    details = owner["details"]
    identity = details["identity"]
    registration, _ = natural._round_schedule(
        identity, now=parse_time(prepared), active=identity, connection=connection,
    )
    _require(row["job_id"] == "pipeline_round:" + registration
             and identity["job_id"] in {"tikhub_reconcile", "tikhub_works_scan"}
             and identity["pipeline_version"] == pipeline.PIPELINE_VERSION
             and details.get("scope_key") == {field: identity[field] for field in _SCOPE_FIELDS}
             and owner["row"]["status"] == "partial"
             and details["summary"].get("reason") == "diagnostic_inventory_yield"
             and details["summary"].get("diagnostic_only") is True,
             "Prepared natural parent was changed or not yielded")
    return owner


def _source_member(member: dict[str, Any], source: dict[str, Any], parent: dict[str, Any]) -> None:
    payload = member["payload"]
    due, claim = payload["natural_due"], source["claim"]
    proof, scope = due["proof"], due["scope_identity"]
    identity, parent_identity = source["details"]["identity"], parent["details"]["identity"]
    parent_proof = proof["natural_parent"]
    reference = source["details"]["checkpoint"]["reference"]
    start, end = natural._expected_scan_window(parent_identity)
    _require(
        due["contract_version"] == natural.CONTRACT_VERSION and due["stage"] == "discovery"
        and due["operation"] == "douyin_user_posts" and due["sequence"] == 0
        and due["paid_scope_identity"] == payload["paid_scope_identity"]
        and due["source_run_id"] == scope["scheduler_run_id"] == claim["scheduler_run_id"]
        and due["scan_id"] == scope["scheduler_scan_id"] == claim["scan_id"]
        and scope["scheduler_attempt_id"] == claim["attempt_id"]
        and scope["scheduler_owner_token"] == claim["owner_token"]
        and due["source_identity_sha256"] == natural._sha(identity)
        and due["source_scheduled_for"] == source["row"]["scheduled_for"]
        and proof["kind"] == "user_posts" and proof["cursor_sha256"] == natural._sha(proof["cursor"])
        and parent_proof == {
            "run_id": parent["claim"]["scheduler_run_id"], "scan_id": parent["claim"]["scan_id"],
            "scheduled_for": parent["row"]["scheduled_for"], "scheduled_at": parent_identity["scheduled_at"],
            "identity_sha256": natural._sha(parent_identity), "registration_id": parent_identity["registration_id"],
            "link_kind": "existing_child_link",
        }
        and due["scheduled_for"] == parent_identity["scheduled_at"]
        and proof["frozen_window"] == {"start": start, "end": end}
        and due["request_document"]["request_window"] == proof["frozen_window"]
        and due["request_document"]["due_bucket"] == tikhub_scan._page_key(_claim(claim), proof)
        and due["request_document"]["cursor"] == proof["cursor"]
        and due["request_document"]["subject"] == reference
        and due["request_document"]["request_parameters"] == {
            "sec_user_id": reference, "max_cursor": proof["cursor"] or 0, "count": 20, "sort_type": 0,
        }
        and claim["scheduler_run_id"] in parent["details"]["checkpoint"]["child_run_ids"]
        and identity["identity_id"] in parent_identity["eligible_identity_ids"]
        and (identity["window_start"], identity["window_end"]) == (start, end)
        and all(identity[key] == parent_identity[key] for key in _EPOCH_FIELDS)
        and due["account_uid"] == identity["uid"]
        and all(scope[key] == identity[key] for key in (
            "activation_id", "roster_snapshot_id", "roster_snapshot_hash", "identity_id", "account_id", "uid", "platform", "purpose",
        ))
        and scope["content_id"] is None and scope["category"] == "reconcile"
        and scope["business_day"] == parent_identity["beijing_day"],
        "Member source claim, natural parent or frozen request changed",
    )


def _materialization(
    connection: sqlite3.Connection, source: dict[str, Any], member: dict[str, Any], evidence: dict[str, Any],
) -> dict[str, Any]:
    source_chain = source.get("local_chain", [source])
    final_source = source_chain[-1]
    details, claim = final_source["details"], _claim(source["claim"])
    state, scope = details["checkpoint"], details["identity"]
    due = member["payload"]["natural_due"]
    proof = due["proof"]
    head = state["last_manifest"]
    manifest = json.loads(_file(head["path"], head["sha256"], head["byte_size"], limit=MAX_SIDECAR_BYTES))
    receipt = manifest["raw"]
    raw = connection.execute(
        "SELECT r.*,a.slot_id FROM provider_raw_responses r JOIN fetch_attempts a ON a.id=r.fetch_attempt_id WHERE r.id=?",
        (evidence["raw_response_id"],),
    ).fetchone()
    _require(
        raw is not None and receipt == {"kind": "page", "window_key": due["request_document"]["due_bucket"],
                                       "raw_response_id": raw["id"], "slot_id": raw["slot_id"],
                                       "sha256": raw["sha256"], "captured_at": raw["captured_at"]}
        and manifest["contract_version"] == tikhub_scan.CONTRACT_VERSION
        and manifest["scan_id"] == claim.scan_id and manifest["scope"] == scope
        and manifest["generation"] == proof["generation"] == state["generation"]
        and manifest["page_number"] == proof["page_number"] == state["page_number"] - 1
        and manifest["request_cursor"] == proof["cursor"]
        and manifest["execution_next_cursor"] == state["cursor"]
        and manifest["provider_next_cursor"] == state["provider_next_cursor"]
        and state["last_raw_response_id"] == raw["id"]
        and state["complete"] == (manifest["completion_reason"] is not None)
        and state["completion_reason"] == manifest["completion_reason"],
        "Successful member manifest is not its exact one-page continuation",
    )
    eligible = [item for item in manifest["items"] if isinstance(item, dict)
                and item.get("reason") == "" and type(item.get("content_id")) is int]
    identity = tikhub_scan._materialization_identity(
        claim, scope, receipt, generation=proof["generation"], page_number=proof["page_number"],
        eligible_indexes=[item["index"] for item in eligible],
    )
    rows = connection.execute(
        "SELECT * FROM scheduler_runs WHERE job_id=? "
        "AND json_extract(details_json,'$.identity.parent_scheduler_run_id')=? "
        "AND (json_extract(details_json,'$.identity.raw_response_id')=? OR "
        "(json_extract(details_json,'$.identity.generation')=? AND json_extract(details_json,'$.identity.page_number')=?))",
        (tikhub_scan.MATERIALIZATION_JOB, claim.scheduler_run_id, raw["id"], proof["generation"], proof["page_number"]),
    ).fetchall()
    _require(len(rows) == 1, "Successful page has missing or extra materialization children")
    first_attempt = connection.execute(
        "SELECT * FROM scheduler_run_attempts WHERE scheduler_run_id=? ORDER BY attempt_number,id LIMIT 1", (rows[0]["id"],),
    ).fetchone()
    child_details = json.loads(first_attempt["details_json"])
    owner = child_details["owner"]
    child_claim = _claim({"scheduler_run_id": rows[0]["id"], "scan_id": child_details["scan_id"],
                          "attempt_id": first_attempt["id"], "attempt_number": first_attempt["attempt_number"],
                          "owner_token": owner["token"]})
    first_child = _terminal_owner(connection, child_claim, job=tikhub_scan.MATERIALIZATION_JOB,
                                 at=source["row"]["completed_at"], invocation="scheduled", historical=True)
    child_chain = _owner_chain(connection, first_child, at=final_source["row"]["completed_at"], invocation="scheduled")
    _require(len(child_chain) == len(source_chain), "Local-only recovery has unexplained source or materializer attempts")
    original_pending = source["details"]["checkpoint"].get("pending_materialization")
    if len(source_chain) > 1:
        _require(original_pending == {"identity": identity, "after_materialization": {
            "complete": state["complete"], "completion_reason": state["completion_reason"],
        }}, "Local recovery is not the original pending page")
    previous_indexes: list[int] = []
    previous_hashes: list[str] = []
    for index, (source_owner, child_owner) in enumerate(zip(source_chain, child_chain, strict=True)):
        child_checkpoint = child_owner["details"]["checkpoint"]
        _require(child_owner["details"]["identity"] == identity
                 and child_checkpoint["parent_attempt_id"] == source_owner["claim"]["attempt_id"]
                 and parse_time(source_owner["row"]["started_at"]) <= parse_time(child_owner["row"]["started_at"])
                 <= parse_time(child_owner["row"]["completed_at"]) <= parse_time(source_owner["row"]["completed_at"]),
                 "Local child is not bound to its exact source attempt")
        if index:
            source_checkpoint = source_owner["details"]["checkpoint"]
            base = source["details"]["checkpoint"]
            variable = {"pending_materialization", "complete", "completion_reason"}
            _require({key: value for key, value in source_checkpoint.items() if key not in variable}
                     == {key: value for key, value in base.items() if key not in variable}
                     and source_checkpoint["pending_materialization"] == (None if index == len(source_chain) - 1 else original_pending)
                     and child_checkpoint.get("progress_contract_version") == tikhub_scan.MATERIALIZATION_PROGRESS_CONTRACT_VERSION
                     and child_checkpoint["raw_response_id"] == raw["id"]
                     and child_checkpoint["eligible_item_count"] == len(identity["eligible_indexes"])
                     and type(child_checkpoint["next_item_offset"]) is int
                     and child_checkpoint["completed_indexes"] == identity["eligible_indexes"][:child_checkpoint["next_item_offset"]]
                     and len(child_checkpoint["item_result_sha256"]) == child_checkpoint["next_item_offset"]
                     and child_checkpoint["completed_indexes"][:len(previous_indexes)] == previous_indexes
                     and child_checkpoint["item_result_sha256"][:len(previous_hashes)] == previous_hashes
                     and all(re.fullmatch(r"[0-9a-f]{64}", value) is not None for value in child_checkpoint["item_result_sha256"]),
                     "Continuation has no complete same-page local progress contract")
            previous_indexes = child_checkpoint["completed_indexes"]
            previous_hashes = child_checkpoint["item_result_sha256"]
        if index < len(source_chain) - 1:
            _require(source_owner["row"]["status"] == child_owner["row"]["status"] == "partial"
                     and source_owner["details"]["checkpoint"]["pending_materialization"] == original_pending
                     and source_owner["details"]["checkpoint"]["completion_reason"]
                     == source["details"]["checkpoint"]["completion_reason"],
                     "Local continuation resumed a non-pending owner")
    _no_paid_continuation(connection, [item["claim"]["attempt_id"] for item in source_chain[1:]]
                          + [item["claim"]["attempt_id"] for item in child_chain])
    child = child_chain[-1]
    child_state = child["details"]["checkpoint"]
    _require(child["details"]["identity"] == identity and child_claim.attempt_number == 1
             and child["row"]["status"] == "succeeded" and child_state["complete"] is True
             and child_state["raw_response_id"] == raw["id"]
             and re.fullmatch(r"[0-9a-f]{64}", child_state["result_sha256"]) is not None
             and parse_time(source["row"]["started_at"]) <= parse_time(child["row"]["started_at"]),
             "Page materialization identity, source owner or terminal result changed")
    if len(child_chain) > 1:
        _require(previous_indexes == identity["eligible_indexes"] and child_state["result_sha256"] == tikhub_scan._digest({
            "raw_response_id": raw["id"], "eligible_indexes": previous_indexes, "item_result_sha256": previous_hashes,
        }), "Local recovery terminal progress digest changed")
    return {
        "materialization_run_id": child_claim.scheduler_run_id, "materialization_attempt_id": child["claim"]["attempt_id"],
        "materialization_attempt_ids": [item["claim"]["attempt_id"] for item in child_chain],
        "materialization_started_at": first_child["row"]["started_at"],
        "source_completed_at": final_source["row"]["completed_at"],
        "materialization_identity": identity, "manifest_receipt": head, "raw_receipt": receipt,
        "eligible_content_ids": sorted({item["content_id"] for item in eligible}),
    }


def _read(connection: sqlite3.Connection, campaign_receipt_id: int, *, at: str) -> dict[str, Any]:
    campaign = read_transport_receipt(connection, campaign_receipt_id)
    arm = str(campaign["payload"].get("arm") or "primary")
    terminal_key = (
        f"primary-execution:{campaign_receipt_id}"
        if arm == "primary"
        else f"{arm}-execution:{campaign_receipt_id}"
    )
    row = connection.execute(
        "SELECT id FROM scheduler_runs WHERE job_id='transport_receipt:campaign_terminal' AND scheduled_for=?",
        (terminal_key,),
    ).fetchone()
    _require(row is not None, "Campaign has no immutable command terminal receipt")
    terminal = read_transport_receipt(connection, row["id"])
    payload = terminal["payload"]
    _require(payload["contract_version"] == EXECUTION_CONTRACT and payload["campaign_receipt_id"] == campaign_receipt_id
             and payload["campaign_receipt_sha256"] == campaign["self_sha256"]
             and payload["hold_binding"] == campaign["payload"]["hold_binding"]
             and payload["sample_limit"] == 20 and payload["qualified"] is False
             and parse_time(payload["closed_at"]) <= parse_time(terminal["recorded_at"]) <= parse_time(at),
             "Campaign terminal differs from its frozen campaign")
    operator_claim = _claim(payload["operator_claim"])
    operator = _terminal_owner(connection, operator_claim, job=OPERATOR_JOB, at=at)
    operator_binding = {"claim": asdict(operator_claim), "identity": primary_operator_identity(campaign)}
    state = operator["details"]["checkpoint"]
    members = _members(connection, campaign_receipt_id)
    if (
        arm in CONTROL_ARMS
        and payload["members"] == []
        and payload["results"] == []
        and payload["sample_complete"] is False
        and payload["effective_starts"] == 0
        and payload["charged_microusd"] == 0
        and payload["unresolved_billing_count"] == 0
    ):
        failure = payload.get("failure")
        _require(
            not members
            and isinstance(failure, dict)
            and isinstance(failure.get("type"), str)
            and isinstance(failure.get("message"), str)
            and operator["details"]["identity"] == operator_binding["identity"]
            and state["complete"] is False
            and "primary_due_inventory" not in state
            and state.get("primary_results", []) == []
            and state.get("blocked_terminal_receipt_id", terminal["receipt_id"]) == terminal["receipt_id"]
            and connection.execute(
                "SELECT 1 FROM provider_usage "
                "WHERE json_extract(details_json,'$.diagnostic_member.campaign_receipt_id')=? LIMIT 1",
                (campaign_receipt_id,),
            ).fetchone() is None
            and parse_time(operator["row"]["started_at"]) <= parse_time(operator["row"]["completed_at"])
            <= parse_time(payload["closed_at"]) <= parse_time(terminal["recorded_at"]),
            "Zero-start control terminal is not an exact failed owner closure",
        )
        return {
            "contract_version": CONTRACT_VERSION, "campaign_receipt_id": campaign_receipt_id,
            "terminal_receipt_id": terminal["receipt_id"], "terminal_receipt_sha256": terminal["self_sha256"],
            "paid_run_ids": [operator_claim.scheduler_run_id],
            "paid_attempt_ids": [operator_claim.attempt_id],
            "materialization_run_ids": [],
            "materialization_attempt_ids": [],
            "members": [],
        }
    inventory = state["primary_due_inventory"]
    _require(operator["details"]["identity"] == operator_binding["identity"] and state["complete"] is True
             and state["primary_terminal_receipt_id"] == terminal["receipt_id"]
             and state["primary_results"] == payload["results"]
             and inventory["contract_version"] == INVENTORY_CONTRACT
             and inventory["campaign_receipt_id"] == campaign_receipt_id and inventory["operator"] == operator_binding
             and parse_time(operator["row"]["started_at"]) <= parse_time(inventory["prepared_at"])
             <= parse_time(payload["closed_at"]) <= parse_time(operator["row"]["completed_at"]),
             "Original operator inventory or terminal checkpoint changed")
    parents = {_claim(entry).scheduler_run_id: _parent(connection, _claim(entry), at=payload["closed_at"],
                                                      prepared=inventory["prepared_at"])
               for entry in inventory["parent_claims"]}
    recoverable = {result["source_run_id"] for result in payload["results"]
                   if result["materialized"] is False and result["raw_response_id"] is not None
                   and result["dispatch_terminal"] == "succeeded"}
    children = {_claim(entry).scheduler_run_id: _terminal_owner(connection, _claim(entry), job="tikhub_reconcile",
                                                               at=payload["closed_at"], historical=entry["scheduler_run_id"] in recoverable)
                for entry in inventory["child_claims"]}
    for source_id in recoverable:
        _require(source_id in children, "Recovery source is outside original inventory")
        source = children[source_id]
        source["local_chain"] = _owner_chain(connection, source, at=at, invocation="operator_retry")
    all_owners = [operator, *parents.values(), *children.values()]
    _require(len(parents) == len(inventory["parent_claims"]) and len(children) == len(inventory["child_claims"])
             and len({owner["claim"]["scheduler_run_id"] for owner in all_owners}) == len(all_owners)
             and len(inventory["candidate_run_ids"]) == len(children)
             and set(inventory["candidate_run_ids"]) == set(children), "Prepared owner inventory is duplicated or incomplete")
    for parent in parents.values():
        identity = parent["details"]["identity"]
        _require(all(identity[key] == payload["hold_binding"][key] for key in (
            "activation_id", "profile_id", "roster_snapshot_id", "roster_snapshot_hash",
        )), "Inventory parent belongs to another HOLD epoch")
    for owner in [*parents.values(), *children.values()]:
        _require(parse_time(owner["row"]["started_at"]) == parse_time(inventory["prepared_at"]),
                 "Prepared owner was not claimed by this inventory")
    for child in children.values():
        identity = child["details"]["identity"]
        checkpoint = child.get("local_chain", [child])[-1]["details"]["checkpoint"]
        matches = [parent for parent in parents.values()
                   if child["claim"]["scheduler_run_id"] in parent["details"]["checkpoint"]["child_run_ids"]
                   and identity["identity_id"] in parent["details"]["identity"]["eligible_identity_ids"]
                   and all(identity[key] == parent["details"]["identity"][key] for key in _EPOCH_FIELDS)
                   and (identity["window_start"], identity["window_end"])
                   == natural._expected_scan_window(parent["details"]["identity"])]
        _require(len(matches) == 1 and identity["identity_id"] in inventory["eligible_identity_ids"]
                 and identity["contract_version"] == tikhub_scan.CONTRACT_VERSION
                 and identity["provider"] == "TikHub" and identity["platform"] == "douyin"
                 and identity["purpose"] == "reconcile" and identity["task_id"] is None
                 and checkpoint.get("pending_raw") is None and checkpoint.get("pending_materialization") is None,
                 "Prepared source has changed its natural parent or has pending materialization")
    _require(len(members) == 20 and payload["members"] == [
        {"receipt_id": member["receipt_id"], "receipt_sha256": member["self_sha256"], "rank": member["payload"]["rank"]}
        for member in members
    ], "Terminal fixed member batch changed")
    results = payload["results"]
    _require(1 <= len(results) <= 20 and [result["rank"] for result in results] == list(range(1, len(results) + 1)),
             "Command results are not a fixed-rank prefix")
    selected: set[int] = set()
    member_owners = []
    for member in members:
        item = member["payload"]
        source_id = item["natural_due"]["source_run_id"]
        _require(source_id in children and source_id not in selected and item["operator"] == operator_binding
                 and item["writer"] == inventory["writer"], "Member has a foreign or duplicated source owner")
        selected.add(source_id)
        source = children[source_id]
        proof = item["natural_due"]["proof"]
        _source_member(member, source, parents[proof["natural_parent"]["run_id"]])
        evidence = read_primary_member_evidence(connection, member["receipt_id"], at=payload["closed_at"])
        checkpoint = source["details"]["checkpoint"]
        result: dict[str, Any] = {
            "member_receipt_id": member["receipt_id"], "rank": item["rank"], "source_run_id": source_id,
            "source_attempt_id": source["claim"]["attempt_id"], "state": evidence["state"],
            "raw_response_id": evidence.get("raw_response_id"), "materialization_run_id": None,
            "materialization_attempt_id": None, "eligible_content_ids": [],
            "materialization_attempt_ids": [],
            "source_attempt_ids": [owner["claim"]["attempt_id"] for owner in source.get("local_chain", [source])],
        }
        if evidence["state"] == "succeeded":
            result.update(_materialization(connection, source, member, evidence))
        else:
            _require(all(checkpoint[field] == proof[field] for field in ("generation", "page_number", "cursor"))
                     and checkpoint["complete"] is False,
                     "Failed or unused member advanced its frozen cursor")
        rank = item["rank"]
        if rank <= len(results):
            recorded = results[rank - 1]
            _require(recorded["member_receipt_id"] == member["receipt_id"] and recorded["source_run_id"] == source_id
                     and recorded["effective_starts"] == evidence["effective_starts"]
                     and recorded["raw_response_id"] == evidence.get("raw_response_id")
                     and recorded["materialized"] == (evidence["state"] == "succeeded" and checkpoint.get("pending_materialization") is None)
                     and recorded["response_complete"] == evidence.get("response_complete", False)
                     and recorded["dispatch_terminal"] == ("not_reserved" if evidence["state"] == "not_started" else evidence["state"])
                     and recorded["scan_complete"] == checkpoint["complete"],
                     "Member outcome differs from command terminal results")
            result["campaign_materialized"] = recorded["materialized"]
        else:
            _require(evidence["state"] == "not_started", "Unrecorded member has transport activity")
        member_owners.append(result)
    for child_id, child in children.items():
        if child_id not in selected:
            _require(child["row"]["status"] == "partial"
                     and child["details"]["summary"].get("reason") == "diagnostic_inventory_yield"
                     and child["details"]["summary"].get("diagnostic_only") is True,
                     "Unselected inventory owner was not yielded")
    return {
        "contract_version": CONTRACT_VERSION, "campaign_receipt_id": campaign_receipt_id,
        "terminal_receipt_id": terminal["receipt_id"], "terminal_receipt_sha256": terminal["self_sha256"],
        "paid_run_ids": sorted(owner["claim"]["scheduler_run_id"] for owner in all_owners),
        "paid_attempt_ids": sorted(item["claim"]["attempt_id"] for owner in all_owners for item in owner.get("local_chain", [owner])),
        "materialization_run_ids": sorted(row["materialization_run_id"] for row in member_owners
                                          if row["materialization_run_id"] is not None),
        "materialization_attempt_ids": sorted(attempt_id for row in member_owners for attempt_id in row["materialization_attempt_ids"]),
        "members": member_owners,
    }


def read_primary_campaign_owners(connection: sqlite3.Connection, campaign_receipt_id: int, *, at: str) -> dict[str, Any]:
    """Return exact closed owner sets, never authorize or mutate historical work."""
    try:
        return _read(connection, campaign_receipt_id, at=at)
    except (KeyError, TypeError, ValueError, IndexError) as error:
        raise DiagnosticOwnerEvidenceError("Malformed campaign owner evidence") from error
