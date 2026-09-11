"""One approved compensation for each frozen, known-billing profile failure.

The installed verifier binds the user statement and the original 111-task
baseline. A Writer command supplies only a work ID. Complete error bodies prove
that local replay cannot fill the missing profile; they never erase the original
charge, raw, request claim, task identity or logical due. Unknown sends stay held.
"""
from __future__ import annotations

from datetime import timedelta
import json
import sqlite3
from typing import Any, Mapping

from . import account_profile_authority as profile, capture_authorizations as auth
from . import capture_compensation as compensation, capture_release as release
from . import provider_budget as budget, usage_settlements as ledger
from .paid_identity import build_paid_request_identity
from .runtime_database import require_current_process_writer_lock
from .source_routing import parse_time

CONTRACT = "account-profile-compensation-authority-v1"
STATEMENT_CONTRACT = "account-profile-compensation-user-authorization-v1"
GAP_CONTRACT = "account-profile-compensation-gap-v1"
RESULT_CONTRACT = "account-profile-compensation-enqueue-v1"
BASELINE_SHA256 = "dad90e9d01d834a8081e47de819c847fcb6d6f272d73cf7a5a664bb9ceaf5d9f"
OPERATION = "douyin_uid_profile"
MAX_TARGETS = 4
UNIT_MICROUSD = 1000
TARGET_KEYS = {"work_id", "source_plan_id", "identity_id", "usage_id", "raw_response_id"}


def _require(value: Any, message: str) -> None:
    if not value:
        raise budget.PaidScopeBlocked("profile_compensation_not_authorized", message)


def _object(value: Any) -> dict[str, Any]:
    result = json.loads(value) if isinstance(value, str) else value
    _require(isinstance(result, dict), "Profile compensation evidence is not an object")
    return dict(result)


def _row(connection: sqlite3.Connection, table: str, identifier: int) -> dict[str, Any]:
    # All callers use literal table names; IDs are SQL parameters.
    row = connection.execute("SELECT * FROM " + table + " WHERE id=?", (identifier,)).fetchone()
    _require(row is not None, "Profile compensation source evidence is absent")
    return dict(row)


def _authority(evidence: Mapping[str, Any], work_id: int, at: str):
    proof = _object(evidence.get("profile_compensation_authority"))
    _require(proof.get("contract") == CONTRACT and proof.get("proof_sha256") == auth.digest(
        {key: value for key, value in proof.items() if key != "proof_sha256"}), "Profile compensation proof changed")
    approval = _object(proof.get("authorization_payload"))
    _require(approval.get("contract") == STATEMENT_CONTRACT
             and approval.get("production_rollout") == "approved_by_user"
             and approval.get("business_e2e") == "required"
             and approval.get("transport_qualification") == "not_verified"
             and all(isinstance(approval.get(key), str) and approval[key].strip()
                     for key in ("actor", "reason", "user_instruction", "source_thread_id", "issued_at", "expires_at")),
             "Profile compensation lacks its explicit bounded user statement")
    issued, expires, now = parse_time(approval["issued_at"]), parse_time(approval["expires_at"]), parse_time(at)
    _require(issued <= now < expires <= issued + timedelta(hours=24), "Profile compensation statement expired or future-dated")
    _require(all(profile._reference(proof.get(key)) for key in ("authorization", "loaded_build", "source_tree", "parent_build"))
             and approval.get("source_tree") == proof["source_tree"]
             and approval.get("parent_build") == proof["parent_build"]
             and proof["loaded_build"] != proof["parent_build"]
             and profile._reference(approval.get("original_cohort"))
             and approval["original_cohort"]["sha256"] == approval.get("original_cohort_sha256") == BASELINE_SHA256,
             "Profile compensation source/build or original cohort binding changed")
    inherited = evidence.get("profile_operation_authority", {})
    _require(profile.decision(evidence, OPERATION, at) is not None
             and proof.get("profile_authority_proof_sha256") == inherited.get("proof_sha256")
             and proof.get("catalog_policy_sha256") == approval.get("catalog_policy_sha256")
             == evidence.get("catalog_capture_policy_sha256"), "Profile operation/catalog authority changed")
    originals, targets = approval.get("original_work_ids"), approval.get("targets")
    _require(isinstance(originals, list) and len(originals) == 111 and len(set(originals)) == 111
             and all(type(value) is int and value > 0 for value in originals)
             and isinstance(targets, list) and 1 <= len(targets) <= MAX_TARGETS
             and all(isinstance(target, dict) and set(target) == TARGET_KEYS
                     and all(type(value) is int and value > 0 for value in target.values())
                     and target["work_id"] in originals for target in targets)
             and all(len({target[key] for target in targets}) == len(targets)
                     for key in ("work_id", "identity_id", "usage_id", "raw_response_id"))
             and type(approval.get("max_starts")) is int and approval["max_starts"] == len(targets)
             and type(approval.get("max_amount_microusd")) is int and approval["max_amount_microusd"] == UNIT_MICROUSD
             and type(approval.get("max_total_microusd")) is int
             and approval["max_total_microusd"] == len(targets) * UNIT_MICROUSD,
             "Profile compensation scope or cost ceiling expanded")
    matches = [target for target in targets if target["work_id"] == work_id]
    _require(len(matches) == 1, "Work is not one of the explicitly authorized original failures")
    return proof, approval, matches[0]


def _failed_source(connection: sqlite3.Connection, target: Mapping[str, int], *, at: str, enqueued: bool = False):
    from . import account_catalog_capture as catalog, raw_archive
    from .capture_runtime import _page_window

    work = _row(connection, "capture_work_items", target["work_id"])
    envelope = _object(work["envelope_json"])
    work_eligible = ((work["state"] == "paid_identity_hold" and not work["owner_token"] and not envelope.get("compensation"))
                     if not enqueued else work["state"] in {"runnable", "running", "paid_identity_hold", "terminal"}
                     and isinstance(envelope.get("compensation"), dict))
    _require(work_eligible
             and work["operation"] == OPERATION and work["provider"] == "tikhub" and work["content_id"] is None
             and work["source_plan_id"] == envelope.get("catalog_plan_id") == target["source_plan_id"]
             and envelope.get("identity_id") == target["identity_id"]
             and envelope.get("account_id") == work["account_id"] and envelope.get("operation") == OPERATION
             and envelope.get("stage") == "account_metrics" and envelope.get("capture_stage") == "discovery"
             and envelope.get("category") == "metrics" and envelope.get("content_id") is None
             and envelope.get("manual_command_run_id") is None,
             "Only the exact held original catalog profile work may be compensated once")
    assignment = catalog.assignment_for_plan(connection, target["source_plan_id"], identity_id=target["identity_id"],
        operation=OPERATION, at=at, use_planning_cache=False)
    _require(assignment["id"] == work["assignment_id"], "Original catalog route changed")
    usage = _row(connection, "provider_usage", target["usage_id"])
    details = _object(usage["details_json"])
    transport = _object(details.get("transport"))
    scope = _object(details.get("scope"))
    amount = budget.micro_usd(usage["amount"])
    _require(usage["provider"].lower() == "tikhub" and usage["operation"] == OPERATION and usage["currency"] == "USD"
             and usage["request_attempts"] == 1 and details.get("state") == "failed" and details.get("paid_sequence") == 0
             and details.get("error_code") in {"upstream_error", "provider_retry_requested"}
             and ((usage["billed_requests"] == 1 and amount == UNIT_MICROUSD and transport.get("http_status") == 200)
                  or (usage["billed_requests"] == 0 and amount == 0 and transport.get("http_status") == 400))
             and scope.get("catalog_plan_id") == target["source_plan_id"]
             and scope.get("identity_id") == target["identity_id"] and scope.get("account_id") == work["account_id"]
             and transport.get("status") == "succeeded" and transport.get("clean_eof") is True
             and transport.get("json_parse_ok") is True and not transport.get("error_code")
             and transport.get("length_match") is not False and transport.get("gzip_crc_ok") is not False
             and transport.get("raw_response_id") == target["raw_response_id"],
             "Unknown billing, incomplete transport or unrelated usage cannot receive profile compensation")
    document = _object(details.get("paid_identity"))
    request = build_paid_request_identity(provider=document["provider"], operation=document["operation"],
        platform=document["platform"], subject=document["subject"], request_parameters=document["request_parameters"],
        cursor=document["cursor"], request_window=document["request_window"], due_bucket=document["due_bucket"])
    _require(document == request.document and request.scope_identity == details.get("paid_scope_identity")
             and document["operation"] == OPERATION and document["provider"].lower() == "tikhub"
             and document["platform"] == "douyin" and document["subject"] == envelope.get("uid")
             and document["request_parameters"] == {"uid": envelope.get("uid")}
             and document["due_bucket"] == _page_window(envelope), "Original paid request identity/due changed")
    raw = _row(connection, "provider_raw_responses", target["raw_response_id"])
    attempt = _row(connection, "fetch_attempts", raw["fetch_attempt_id"])
    receipt = _row(connection, "fetch_transport_receipts", raw["transport_receipt_id"])
    batch = _row(connection, "fetch_request_batches", details["request_batch_id"])
    _require(raw["operation"] == OPERATION and raw["provider"].lower() == "tikhub"
             and raw["account_id"] == work["account_id"] and raw["http_status"] == transport["http_status"]
             and raw["paid_scope_identity"] == request.scope_identity and raw["sequence"] == 0
             and attempt["request_batch_id"] == batch["id"] and attempt["http_status"] == raw["http_status"]
             and attempt["error_code"] == details["error_code"]
             and receipt["fetch_attempt_id"] == attempt["id"] and receipt["clean_eof"] == receipt["json_parse_ok"] == 1
             and not receipt["error_class"] and batch["operation"] == OPERATION
             and batch["request_scope_identity"] == request.scope_identity and batch["sequence"] == 0
             and _object(batch["parameters_json"]) == document["request_parameters"],
             "Original raw, fetch, transport and request batch do not form one complete failed request")
    body = raw_archive.read_response_entity(connection, raw["id"])
    response = _object(body.decode())
    if raw["http_status"] == 200:
        data = _object(response.get("data"))
        _require(response.get("code") == 200 and type(data.get("status_code")) is int and data["status_code"] != 0
                 and details["error_code"] == "upstream_error", "Stored raw is not the approved upstream error response")
    else:
        _require(_object(response.get("detail")).get("code") == 400
                 and details["error_code"] == "provider_retry_requested", "Stored raw is not the approved zero-charge retry response")
    marker = connection.execute("SELECT id FROM paid_provider_dispatch_events WHERE provider_usage_id=? AND event_type='send_marked'",
                                (usage["id"],)).fetchall()
    _require(len(marker) == 1, "Original usage does not have one actual paid send marker")
    identities = {"request": request.scope_identity, "member": ledger.member_identity(document)}
    claims = [dict(row) for row in connection.execute("SELECT scope_kind,scope_identity,sequence FROM provider_paid_scope_claims WHERE provider_send_marker_id=?", (marker[0][0],))]
    _require(len(claims) == 2 and {row["scope_kind"]: row["scope_identity"] for row in claims} == identities
             and all(row["sequence"] == 0 for row in claims), "Original request/member claims are absent or already compensated")
    for kind, identity in identities.items():
        sequence = connection.execute("SELECT max(sequence) FROM provider_paid_scope_claims WHERE scope_identity=? AND scope_kind=?",
                                      (identity, kind)).fetchone()[0]
        _require(sequence in ({0, 1} if enqueued else {0}), "A later paid sequence already exists")
    return work, envelope, usage, raw, details, identities


def validate_enqueued_authority(connection: sqlite3.Connection, *, work_id: int,
                                compensation_proof: Mapping[str, Any], at: str) -> None:
    """Revalidate the installed approval at readiness and every A/B boundary."""
    evidence = release._installed_evidence(connection, at=at)
    authority, _, target = _authority(evidence, work_id, at)
    _require(compensation_proof.get("profile_compensation_authorization_sha256") == authority["authorization"]["sha256"]
             and compensation_proof.get("sequence") == 1, "Profile compensation statement changed")
    _, _, usage, raw, _, _ = _failed_source(connection, target, at=at, enqueued=True)
    _require(usage["id"] == target["usage_id"] and raw["id"] == target["raw_response_id"], "Profile compensation target changed")
    grants = compensation_proof.get("issuance_ids", {})
    _require(isinstance(grants, dict) and len(grants) == 2, "Profile compensation grant pair is missing")
    for identifier in grants.values():
        grant = compensation._grant(connection, identifier)
        _require(grant["provider_usage_id"] == target["usage_id"] and grant["next_sequence"] == 1
                 and grant["max_amount_microunits"] == UNIT_MICROUSD,
                 "Profile compensation grant no longer binds the approved failure")


def enqueue_profile_compensation(connection: sqlite3.Connection, *, work_id: int, at: str) -> dict[str, Any]:
    """Enqueue one explicit sequence-1 recovery; never call a provider here."""
    _require(type(work_id) is int and work_id > 0 and connection.in_transaction,
             "Profile compensation requires a bounded Writer transaction")
    require_current_process_writer_lock(connection)
    evidence = release._installed_evidence(connection, at=at)
    proof, approval, target = _authority(evidence, work_id, at)
    key = "profile-compensation:" + proof["authorization"]["sha256"] + ":" + str(work_id)
    previous = connection.execute("SELECT payload_json,receipt_sha256 FROM data_quality_receipts WHERE scope_key=?", (key,)).fetchone()
    if previous is not None:
        saved = _object(previous[0])
        _require(saved.get("contract") == RESULT_CONTRACT and saved.get("target") == target
                 and saved.get("authorization_sha256") == proof["authorization"]["sha256"]
                 and auth.digest(saved) == previous[1], "Profile compensation enqueue receipt changed")
        return {**saved["result"], "idempotent": True, "provider_calls": 0}
    work, envelope, usage, raw, details, identities = _failed_source(connection, target, at=at)
    existing = connection.execute("SELECT 1 FROM compensation_authorizations a JOIN provider_usage_settlements s "
        "ON s.id=a.original_settlement_id WHERE s.provider_usage_id=? LIMIT 1", (usage["id"],)).fetchone()
    _require(existing is None, "This original request already received an explicit compensation decision")
    used = connection.execute("SELECT count(*) FROM data_quality_receipts WHERE scope_key LIKE ? AND json_extract(payload_json,'$.contract')=?",
        ("profile-compensation:" + proof["authorization"]["sha256"] + ":%", RESULT_CONTRACT)).fetchone()[0]
    _require(used < approval["max_starts"], "Profile compensation aggregate attempt ceiling reached")
    scope = budget.PaidScope(purpose="metrics", category="metrics", identity_id=target["identity_id"],
        account_id=work["account_id"], content_id=None, catalog_plan_id=target["source_plan_id"],
        platform="douyin", uid=envelope["uid"])
    budget.check_reservation(connection, scope=scope, operation=OPERATION, unit_price="0.001", currency="USD", at=at)
    gap = {"contract": GAP_CONTRACT, "target": target, "work_identity": work["work_identity"],
        "logical_due": envelope["logical_due"], "original_usage_sha256": auth.digest(usage),
        "raw_response_sha256": raw["sha256"], "transport_receipt_id": raw["transport_receipt_id"],
        "local_replay_exhausted": True, "business_gap_due": True,
        "raw_unrecoverable_reason": "verified_complete_error_response_contains_no_successful_profile",
        "authorization_sha256": proof["authorization"]["sha256"], "recorded_at": at}
    gap_sha = auth.digest(gap)
    connection.execute("SAVEPOINT explicit_profile_compensation")
    try:
        connection.execute("INSERT INTO data_quality_receipts(scope_key,cutoff_at,payload_json,recorded_at,receipt_sha256) VALUES(?,?,?,?,?)",
            (key + ":gap", at, auth.canonical(gap), at, gap_sha))
        settlement = ledger.record_settlement(connection, usage_id=usage["id"], at=at)
        _require(settlement["original_state"] == "failed" and settlement["compensation_sequence"] == 0
                 and settlement["amount_microunits"] == budget.micro_usd(usage["amount"]), "Original accounting changed")
        grants = {}
        for kind, identity in identities.items():
            grants[kind] = ledger.authorize_compensation(connection, authorization_key=key + ":" + kind,
                settlement_id=settlement["id"], identity=identity, scope_kind=kind, owner=approval["actor"],
                reason=approval["reason"], gap_evidence_ref="data-quality-receipt:" + gap_sha,
                raw_unrecoverable_reason=gap["raw_unrecoverable_reason"], local_replay_exhausted=True,
                business_gap_due=True, max_amount_microunits=UNIT_MICROUSD, expires_at=approval["expires_at"],
                at=at, provider_ready=True, budget_available=True)
            _require(grants[kind]["status"] == "issued" and grants[kind]["sequence"] == 1,
                     "Only one sequence-1 compensation is approved")
        with auth.runtime_authority(release.current_runtime_bindings):
            result = compensation.enqueue_authorized_compensation(connection, work_id=work_id,
                request_issuance_id=grants["request"]["issuance_id"], member_issuance_id=grants["member"]["issuance_id"], at=at,
                profile_authorization_sha256=proof["authorization"]["sha256"])
        result.update(authorization_sha256=proof["authorization"]["sha256"], settlement_id=settlement["id"],
            original_usage_id=usage["id"], original_raw_response_id=raw["id"], max_amount_microusd=UNIT_MICROUSD,
            provider_bill_verified=False, original_usage_preserved=True)
        saved = {"contract": RESULT_CONTRACT, "target": target, "authorization_sha256": proof["authorization"]["sha256"], "result": result}
        connection.execute("INSERT INTO data_quality_receipts(scope_key,cutoff_at,payload_json,recorded_at,receipt_sha256) VALUES(?,?,?,?,?)",
            (key, at, auth.canonical(saved), at, auth.digest(saved)))
        connection.execute("RELEASE explicit_profile_compensation")
        return result
    except BaseException:
        connection.execute("ROLLBACK TO explicit_profile_compensation")
        connection.execute("RELEASE explicit_profile_compensation")
        raise
