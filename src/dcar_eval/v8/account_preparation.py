"""Persistent preparation of incomplete accounts through the capture queue.

Preparation has its own immutable input scope. A content-ready roster is never
used as permission to resolve an account; ordinary send, budget and lease gates
still apply. Planning and replay do not call a provider.
"""
from __future__ import annotations

import json
import sqlite3
from contextlib import contextmanager
from contextvars import ContextVar
from threading import get_ident
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Mapping

from . import account_intake, capture_planning as planning, platform_adapters as adapters
from .provider_budget import PaidScope, PaidScopeBlocked, PRICES_MICROUSD, budget_day
from .storage import connect, transaction, now_utc

CONTRACT = "account-preparation-plan-v1"
TASK_CAP_USD = 10.0
MAX_AUTOMATIC_PREPARATION_RETRIES = 2
SCOPE_FIELDS = ("intake_request_id", "preparation_plan_id", "preparation_key", "preparation_subject")
_PLANNING_VALIDATION = ContextVar("preparation_planning_validation", default=None)


def _blocked(code: str) -> PaidScopeBlocked:
    return PaidScopeBlocked(code, code)


def _responses(connection: sqlite3.Connection, request: Mapping[str, Any]) -> list[dict[str, Any]]:
    from .raw_archive import read_response_entity
    result = json.loads(request["result_json"])
    rows = []
    for entry in result.get("preparation_responses", []):
        raw = connection.execute("SELECT * FROM provider_raw_responses WHERE id=?", (entry["raw_response_id"],)).fetchone()
        if (raw is None or raw["intake_request_id"] != request["id"] or raw["operation"] != entry["operation"]):
            raise _blocked("preparation_raw_target_changed")
        rows.append({**entry, "payload": json.loads(read_response_entity(connection, int(raw["id"])))})
    return rows


def _current_request(connection: sqlite3.Connection, intake_id: int) -> dict[str, Any]:
    row = connection.execute("SELECT * FROM account_intake_requests WHERE id=?", (intake_id,)).fetchone()
    if row is None:
        raise _blocked("preparation_request_missing")
    request = dict(row)
    value = json.loads(request["input_json"])
    # The journal hashes the exact normalized input, including supplied fields.
    if planning.digest(value) != request["input_sha256"]:
        raise _blocked("preparation_input_changed")
    from .account_directory_reconciliation import validate_request_directory
    try:
        validate_request_directory(connection, request)
    except ValueError:
        raise _blocked("preparation_input_changed") from None
    if request["directory_row_id"]:
        directory = connection.execute("SELECT platform,uid,account_id,display_account_id FROM account_directory_rows WHERE id=?", (request["directory_row_id"],)).fetchone()
        if (directory is None or directory["platform"] != request["platform"]
                or request["account_id"] is not None and directory["account_id"] != request["account_id"]
                or value.get("uid") and directory["uid"] not in (None, "", value["uid"])
                or not value.get("uid") and value.get("display_account_id") and directory["display_account_id"] != value["display_account_id"]):
            raise _blocked("preparation_input_changed")
    return request


def _policy(connection: sqlite3.Connection, at: str) -> Mapping[str, Any] | None:
    from .account_catalog_capture import installed_policy
    value = installed_policy(connection, at=at, use_planning_cache=False)
    return value if value and value.get("account_preparation") == CONTRACT else None


def _planning_cache(connection: sqlite3.Connection, *, at: str, plan_id: int):
    value = _PLANNING_VALIDATION.get()
    return value if (value is not None and value["connection"] is connection
        and connection.in_transaction and value["at"] == at and value["plan_id"] in (None, plan_id)
        and value["thread_id"] == get_ident()) else None


@contextmanager
def _planning_validation(connection: sqlite3.Connection, *, at: str, plan_id: int | None,
                         policy: Mapping[str, Any] | None):
    """Reuse only static evidence inside this synchronous writer planning loop.

    The caller already verified installation policy. The enclosed loop only
    appends routes/work or updates work readiness; it cannot commit, change
    policy/source plans, or perform
    a paid send. Request inputs, activation, routes, gates, retry settlements and
    budget are still read for every member. Nothing is reused by another pass.
    """
    if not connection.in_transaction:
        raise ValueError("preparation planning validation requires a writer transaction")
    token = _PLANNING_VALIDATION.set({"connection": connection, "at": at, "plan_id": plan_id,
        "thread_id": get_ident(), "policy": dict(policy) if policy is not None else None,
        "policy_sha256": planning.digest(policy) if policy is not None else None,
        "validated_plans": {}})
    try:
        yield
        if not connection.in_transaction:
            raise ValueError("preparation planning transaction ended inside validation context")
    finally:
        _PLANNING_VALIDATION.reset(token)


@contextmanager
def planning_reconsideration(connection: sqlite3.Connection, *, at: str):
    """Recheck queued preparation work using one fresh static-policy proof.

    Existing tasks may belong to several immutable source plans. Each plan is
    independently decoded, hashed and indexed on first use in this pass.
    """
    with _planning_validation(connection, at=at, plan_id=None, policy=_policy(connection, at)):
        yield


def _validated_plan(connection: sqlite3.Connection, *, plan_id: int, at: str,
                    for_payment: bool):
    # Physical-send validation always reopens installation/source evidence, even
    # if its caller happens to run inside a planning context.
    cached = None if for_payment else _planning_cache(connection, at=at, plan_id=plan_id)
    if cached is not None and plan_id in cached["validated_plans"]:
        return cached["validated_plans"][plan_id]
    row = connection.execute("SELECT * FROM capture_source_plans WHERE id=?", (plan_id,)).fetchone()
    policy = cached["policy"] if cached is not None else _policy(connection, at)
    if row is None or policy is None:
        raise _blocked("preparation_policy_unavailable")
    plan = json.loads(row["payload_json"])
    policy_sha = cached["policy_sha256"] if cached is not None else planning.digest(policy)
    if (plan.get("contract") != CONTRACT or row["mode"] != "active"
            or planning.digest(plan) != row["plan_sha256"] or plan.get("policy_sha256") != policy_sha):
        raise _blocked("preparation_plan_changed")
    indexed = {}
    for member in plan["members"]:
        indexed.setdefault(member["intake_request_id"], []).append(member)
    result = (plan, indexed)
    if cached is not None:
        cached["validated_plans"][plan_id] = result
    return result


def validate_paid_target(connection: sqlite3.Connection, scope: PaidScope, *, at: str, for_payment: bool = False) -> dict[str, Any]:
    if (connection.execute("PRAGMA user_version").fetchone()[0] not in {22, 23, 24}
            or type(scope.intake_request_id) is not int or type(scope.preparation_plan_id) is not int
            or scope.catalog_plan_id is not None or scope.manual_command_run_id is not None):
        raise _blocked("preparation_scope_invalid")
    plan, indexed = _validated_plan(connection, plan_id=scope.preparation_plan_id, at=at, for_payment=for_payment)
    from .profile_activations import activation_at
    active = activation_at(connection, at)
    if active is None or any(plan.get(key) != active.get(key) for key in
            ("activation_id", "profile_id", "roster_snapshot_id", "roster_members_sha256")):
        raise _blocked("profile_superseded")
    request = _current_request(connection, scope.intake_request_id)
    if _has_owner_table(connection):
        owner = connection.execute("SELECT intake_request_id FROM account_preparation_owners WHERE preparation_key=?",
                                   (account_intake.preparation_key(json.loads(request["input_json"])),)).fetchone()
        if owner is not None and owner[0] != request["id"]:
            raise _blocked("preparation_owner_changed")
    selected = indexed.get(scope.intake_request_id, [])
    if (len(selected) != 1 or selected[0]["input_sha256"] != request["input_sha256"]
            or request["preparation_key"] != scope.preparation_key or request["completed_at"] is not None):
        raise _blocked("preparation_scope_changed")
    target = adapters.next_profile_request(json.loads(request["input_json"]), responses=_responses(connection, request))
    if target is None or target != selected[0]["target"] or target["subject"] != scope.preparation_subject:
        raise _blocked("preparation_step_changed")
    attempt = selected[0].get("attempt", {})
    if attempt.get("proof"):
        proof = attempt["proof"]
        if for_payment and proof.get("kind") == "replay":
            raise _blocked("preparation_replay_network_forbidden")
        if (_retry_evidence(connection, previous_work_id=proof["previous_work_id"], request=request, target=target) != proof
                or planning.timestamp(at) < attempt["due_at"]):
            raise _blocked("preparation_retry_evidence_changed")
    return target


def assignment_for_scope(connection: sqlite3.Connection, scope: PaidScope, *, operation: str, at: str) -> dict[str, Any]:
    target = validate_paid_target(connection, scope, at=at)
    assignment = planning.current_assignment(connection, "intake", str(scope.intake_request_id), operation, at=at)
    if (target["operation"] != operation or assignment is None
            or assignment["intake_request_id"] != scope.intake_request_id
            or assignment["source_plan_id"] != scope.preparation_plan_id):
        raise _blocked("preparation_route_changed")
    return assignment


def readiness(connection: sqlite3.Connection, envelope: Mapping[str, Any], *, at: str) -> tuple[str, str]:
    from .work_readiness import WorkReadinessPass
    scope = PaidScope(purpose="reconcile", category="reconcile", **{key: envelope.get(key) for key in SCOPE_FIELDS})
    try:
        assignment = assignment_for_scope(connection, scope, operation=envelope["operation"], at=at)
        if (assignment["id"] != envelope["assignment_id"] or assignment["mode"] != "active"
                or assignment["route"] != "integrated"):
            raise _blocked("preparation_route_changed")
        gate = connection.execute("SELECT state FROM capture_paid_send_gate_events WHERE provider='tikhub' AND operation=? AND julianday(recorded_at)<=julianday(?) ORDER BY id DESC LIMIT 1", (envelope["operation"], at)).fetchone()
        if gate is None or gate[0] not in {"open", "diagnostic_only"}:
            raise _blocked("provider_transport_blocked")
        assessment = WorkReadinessPass(connection, at=at)._base_assessment(envelope["operation"], "reconcile", manual_scope=scope)
        return ("runnable", "") if assessment["runnable"] else ("provider_blocked", assessment["reason"])
    except (ValueError, RuntimeError) as error:
        return "provider_blocked", str(getattr(error, "error_code", "preparation_input_invalid"))


def _retry_evidence(connection: sqlite3.Connection, *, previous_work_id: int,
                    request: Mapping[str, Any], target: Mapping[str, Any]) -> dict[str, Any]:
    """Read exact immutable failure/billing evidence; never create fee claims."""
    from . import capture, capture_singletons, raw_archive, usage_settlements
    previous = connection.execute("SELECT * FROM capture_work_items WHERE id=?", (previous_work_id,)).fetchone()
    if (previous is None or previous["state"] in {"running", "leased"}
            or previous["owner_token"] is not None or previous["intake_request_id"] != request["id"]):
        raise _blocked("preparation_retry_work_not_finished")
    envelope = json.loads(previous["envelope_json"])
    if (envelope.get("request") != target or envelope.get("preparation_key") != request["preparation_key"]
            or envelope.get("stage") != "profile_prepare"):
        raise _blocked("preparation_retry_scope_changed")
    attempt = connection.execute("""WITH matching_slots AS (
        SELECT id,status FROM fetch_slots
        WHERE intake_request_id=? AND stage='profile_prepare' AND window_key=?
    ), candidate_attempts AS (
        SELECT a.id attempt_id,s.id slot_id
        FROM matching_slots s JOIN fetch_attempts a ON a.slot_id=s.id
        UNION
        SELECT a.id attempt_id,s.id slot_id
        FROM matching_slots s CROSS JOIN paid_provider_dispatch_events d CROSS JOIN fetch_attempts a
        WHERE d.fetch_slot_id=s.id AND d.event_type='send_marked'
          AND a.id=d.fetch_attempt_id AND a.slot_id IS NULL
          AND (SELECT count(*) FROM paid_provider_dispatch_events other
               WHERE other.fetch_attempt_id=a.id AND other.event_type='send_marked')=1
    ) SELECT a.*,s.status slot_status,s.id fetch_slot_id
      FROM candidate_attempts candidate JOIN fetch_attempts a ON a.id=candidate.attempt_id
      JOIN matching_slots s ON s.id=candidate.slot_id
      ORDER BY a.attempt_number DESC,a.id DESC LIMIT 1""", (request["id"], envelope["logical_due"])).fetchone()
    if attempt is None:
        raise _blocked("preparation_retry_attempt_missing")
    rows = connection.execute("SELECT * FROM provider_raw_responses WHERE fetch_attempt_id=? AND intake_request_id=? AND operation=? AND provider='tikhub' COLLATE NOCASE ORDER BY id", (attempt["id"], request["id"], target["operation"])).fetchall()
    if len(rows) != 1 or rows[0]["account_id"] is not None or rows[0]["content_id"] is not None:
        raise _blocked("preparation_retry_raw_missing")
    raw = rows[0]
    transport = connection.execute("SELECT * FROM fetch_transport_receipts WHERE id=? AND fetch_attempt_id=?", (raw["transport_receipt_id"], attempt["id"])).fetchone()
    if transport is None:
        raise _blocked("preparation_retry_transport_incomplete")
    entity = raw_archive.read_response_entity(connection, int(raw["id"]))
    transport_payload = json.loads(transport["payload_json"])
    if transport_payload.get("fetch_attempt_id") != attempt["id"] or raw["http_status"] != attempt["http_status"]:
        raise _blocked("preparation_retry_transport_incomplete")
    try:
        capture._validate_complete_transport_receipt(transport_payload["transport"], entity_bytes=entity, http_status=raw["http_status"])
    except (KeyError, ValueError, capture.RawEvidenceError) as error:
        raise _blocked("preparation_retry_transport_incomplete") from error
    base = {"previous_work_id": previous_work_id, "previous_work_updated_at": previous["updated_at"],
        "raw_response_id": raw["id"], "fetch_attempt_id": attempt["id"], "fetch_slot_id": attempt["fetch_slot_id"],
        "raw_sha256": raw["sha256"], "transport_receipt_sha256": transport["receipt_sha256"],
        "previous_logical_due": envelope["logical_due"]}
    error_code, next_target = None, None
    try:
        next_target = adapters.next_profile_request(json.loads(request["input_json"]), responses=[*_responses(connection, request),
            {"operation": target["operation"], "payload": json.loads(entity), "raw_response_id": raw["id"]}])
    except adapters.PlatformAdapterError as error:
        error_code = error.error_code
    if attempt["slot_status"] == "succeeded" and error_code is None:
        return {**base, "kind": "replay"}
    if (request["platform"] == target.get("platform") == "wechat_channels"
            and target["operation"] == "wechat_channels_resolve"
            and attempt["slot_status"] == "terminal_failed" and attempt["error_code"] == "invalid_finder_candidate"
            and raw["http_status"] == 200 and error_code is None and next_target is not None
            and next_target["operation"] == "wechat_channels_channel_info"):
        # A corrected pure parser can consume the original full resolver entity.
        # This is not another provider attempt and never rewrites the failed slot.
        return {**base, "kind": "replay", "replay_reason": "wechat_resolver_parser_repair"}
    if (attempt["slot_status"] != "retryable_failed"
            or attempt["error_code"] not in {"provider_business_failure", "provider_rate_limited", "rate_limit_exceeded", "http_429"}
            or error_code != "provider_business_failure"):
        raise _blocked("preparation_retry_not_transient")
    return {**base, "kind": "retry", **_verified_retry_settlement(connection, attempt=attempt, raw=raw)}


def _verified_retry_settlement(connection: sqlite3.Connection, *, attempt: Mapping[str, Any],
                               raw: Mapping[str, Any]) -> dict[str, Any]:
    from . import usage_settlements
    settlement_rows = connection.execute("""SELECT s.id,d.id send_marker_id FROM provider_usage_settlements s
        JOIN paid_provider_dispatch_events d ON d.id=s.provider_send_marker_id
        JOIN provider_request_start_events n ON n.provider_send_marker_id=d.id
        WHERE d.event_type='send_marked' AND d.fetch_attempt_id=? AND s.scope_identity=? AND s.compensation_sequence=?""",
        (attempt["id"], raw["paid_scope_identity"], raw["sequence"])).fetchall()
    if len(settlement_rows) != 1:
        raise _blocked("preparation_billing_unverified")
    settlement = usage_settlements.read_settlement(connection, int(settlement_rows[0]["id"]))
    if not settlement["provider_bill_verified"] or settlement["state"] not in {"charged_verified", "refunded"}:
        raise _blocked("preparation_billing_unverified")
    return {"settlement_id": settlement["id"],
        "settlement_state": settlement["state"], "settlement_event_sequence": settlement["event_sequence"],
        "settlement_source_sha256": settlement["source_sha256"], "send_marker_id": settlement_rows[0]["send_marker_id"]}


def _next_attempt(connection: sqlite3.Connection, *, request: Mapping[str, Any],
                  target: Mapping[str, Any], at: str, active: Mapping[str, Any] | None = None) -> dict[str, Any]:
    """Stable bounded generations prevent every planner tick from repurchasing."""
    from .capture_runtime import retry_backoff
    rows = connection.execute("""SELECT w.* FROM capture_work_items w JOIN capture_source_plans p ON p.id=w.source_plan_id
        WHERE w.intake_request_id=? AND w.operation=? AND p.mode='active' ORDER BY w.id DESC""",
        (request["id"], target["operation"])).fetchall()
    previous = next((row for row in rows if json.loads(row["envelope_json"]).get("request") == target), None)
    if previous is None:
        return {"generation": 0, "due_at": planning.timestamp(request["created_at"])}
    envelope = json.loads(previous["envelope_json"])
    generation = envelope.get("preparation_attempt_generation", 0)
    if type(generation) is not int or not 0 <= generation <= MAX_AUTOMATIC_PREPARATION_RETRIES:
        raise _blocked("preparation_retry_generation_invalid")
    changed_authority = active is not None and any(envelope.get(k) != active.get(k) for k in
        ("activation_id", "profile_id", "roster_snapshot_id", "roster_members_sha256"))
    if changed_authority:
        if previous["owner_token"] is not None or previous["state"] in {"running", "leased"}:
            raise _blocked("preparation_previous_revision_running")
        attempts = connection.execute("""SELECT 1 FROM fetch_attempts a LEFT JOIN fetch_slots s ON s.id=a.slot_id
            LEFT JOIN fetch_request_batch_members m ON m.batch_id=a.request_batch_id
            WHERE (s.intake_request_id=? AND s.window_key=?) OR m.intake_request_id=? LIMIT 1""",
            (request["id"], envelope["logical_due"], request["id"])).fetchone()
        sends = connection.execute("SELECT 1 FROM paid_provider_dispatch_events WHERE event_type='send_marked' AND json_extract(scope_json,'$.intake_request_id')=? LIMIT 1",
                                   (request["id"],)).fetchone()
        if attempts is None and sends is None:
            return {"generation": generation, "due_at": previous["due_at"],
                    "authority_previous_work_id": previous["id"], "logical_due": envelope["logical_due"]}
        # A new authority binding never forgets an old send. Complete immutable
        # raw is replayable; unknown transport/billing remains an explicit hold.
        proof = _retry_evidence(connection, previous_work_id=int(previous["id"]), request=request, target=target)
        if proof["kind"] == "replay":
            return {"generation": generation, "due_at": previous["due_at"], "proof": proof,
                    "authority_previous_work_id": previous["id"]}
        if generation >= MAX_AUTOMATIC_PREPARATION_RETRIES:
            raise _blocked("preparation_retry_exhausted")
        due = datetime.fromisoformat(str(previous["updated_at"]).replace("Z", "+00:00")) + retry_backoff(generation + 1)
        return {"generation": generation + 1, "due_at": planning.timestamp(due.astimezone(timezone.utc).isoformat()), "proof": proof}
    if previous["state"] not in {"paid_identity_hold", "terminal"}:
        return {"generation": generation, "due_at": previous["due_at"],
                **envelope.get("preparation_authority_recovery", {}),
                **({"proof": envelope["preparation_retry_proof"]} if envelope.get("preparation_retry_proof") else {})}
    if generation >= MAX_AUTOMATIC_PREPARATION_RETRIES:
        raise _blocked("preparation_retry_exhausted")
    proof = _retry_evidence(connection, previous_work_id=int(previous["id"]), request=request, target=target)
    due = datetime.fromisoformat(str(previous["updated_at"]).replace("Z", "+00:00")) + retry_backoff(generation + 1)
    return {"generation": generation + 1, "due_at": planning.timestamp(due.astimezone(timezone.utc).isoformat()),
            "proof": proof}


def _require_previous_revisions_closed(connection: sqlite3.Connection, request: Mapping[str, Any]) -> None:
    """A new intake revision is not permission to forget an unresolved send.

    Check both equivalent locators and earlier locators of this directory. Old
    runnable work becomes unsendable through its completed/stale intake scope;
    a live lease, send without a verified bill, or unbound attempt still blocks.
    """
    from . import paid_dispatch, usage_settlements
    key = account_intake.preparation_key(json.loads(request["input_json"]))
    prior = [row for row in connection.execute("SELECT * FROM account_intake_requests WHERE id<>? AND platform=? ORDER BY id",
             (request["id"], request["platform"])) if account_intake.preparation_key(json.loads(row["input_json"])) == key
             or request["directory_row_id"] is not None and row["directory_row_id"] == request["directory_row_id"]]
    for old in prior:
        works = connection.execute("SELECT * FROM capture_work_items WHERE intake_request_id=?", (old["id"],)).fetchall()
        if any(row["owner_token"] is not None or row["state"] in {"running", "leased"} for row in works):
            raise _blocked("preparation_previous_revision_running")
        if works and old["completed_at"] is None:
            try:
                _current_request(connection, old["id"])
            except PaidScopeBlocked:
                pass  # A superseded locator cannot acquire a new send permit.
            else:
                raise _blocked("preparation_previous_revision_active")
        attempts = {row[0] for row in connection.execute("""SELECT DISTINCT a.id FROM fetch_attempts a
            LEFT JOIN fetch_slots s ON s.id=a.slot_id
            LEFT JOIN fetch_request_batch_members m ON m.batch_id=a.request_batch_id
            WHERE s.intake_request_id=? OR m.intake_request_id=?""", (old["id"], old["id"]))}
        dispatch_ids = {row[0] for row in connection.execute("""SELECT DISTINCT d.dispatch_id
            FROM paid_provider_dispatch_events d LEFT JOIN fetch_slots s ON s.id=d.fetch_slot_id
            WHERE json_extract(d.scope_json,'$.intake_request_id')=? OR s.intake_request_id=?
              OR d.fetch_attempt_id IN (SELECT a.id FROM fetch_attempts a
                JOIN fetch_request_batch_members m ON m.batch_id=a.request_batch_id WHERE m.intake_request_id=?)""",
            (old["id"], old["id"], old["id"]))}
        covered_attempts = set()
        for dispatch_id in dispatch_ids:
            events = paid_dispatch.dispatch_events(connection, dispatch_id)
            covered_attempts.update(event.fetch_attempt_id for event in events if event.fetch_attempt_id is not None)
            if events[-1].event_type == "not_sent":
                continue
            if events[-1].event_type not in {"succeeded", "failed", "billing_unknown"}:
                raise _blocked("preparation_previous_revision_billing_unverified")
            marker = next((event for event in events if event.event_type == "send_marked"), None)
            if marker is None:
                raise _blocked("preparation_previous_revision_billing_unverified")
            rows = connection.execute("""SELECT s.id FROM provider_usage_settlements s
                JOIN provider_request_start_events n ON n.provider_send_marker_id=s.provider_send_marker_id
                WHERE s.provider_send_marker_id=? AND s.provider_usage_id=?""",
                (marker.event_id, marker.provider_usage_id)).fetchall()
            if len(rows) != 1:
                raise _blocked("preparation_previous_revision_billing_unverified")
            settlement = usage_settlements.read_settlement(connection, rows[0][0])
            if not settlement["provider_bill_verified"] or settlement["state"] not in {"charged_verified", "refunded"}:
                raise _blocked("preparation_previous_revision_billing_unverified")
        if attempts - covered_attempts:
            raise _blocked("preparation_previous_revision_billing_unverified")


def _existing_revision_work(connection: sqlite3.Connection, member: Mapping[str, Any], *, shadow: bool,
                            active: Mapping[str, Any] | None = None) -> bool:
    """Adopt pre-revision work identities as well as current ones without writes."""
    rows = connection.execute("""SELECT w.envelope_json FROM capture_work_items w
        JOIN capture_source_plans p ON p.id=w.source_plan_id
        WHERE w.intake_request_id=? AND w.operation=? AND p.mode=?""",
        (member["intake_request_id"], member["target"]["operation"], "shadow" if shadow else "active"))
    return any((value := json.loads(row[0])).get("request") == member["target"]
        and value.get("preparation_attempt_generation", 0) == member["attempt"]["generation"]
        and (active is None or all(value.get(k) == active.get(k) for k in
             ("activation_id", "profile_id", "roster_snapshot_id", "roster_members_sha256"))) for row in rows)


def _has_owner_table(connection: sqlite3.Connection) -> bool:
    return connection.execute("SELECT 1 FROM sqlite_master WHERE type='table' AND name='account_preparation_owners'").fetchone() is not None


def _claim_owner(connection: sqlite3.Connection, request: Mapping[str, Any], *, at: str) -> None:
    if not _has_owner_table(connection):
        return  # schema22 compatibility; the transaction still serializes work.
    key = account_intake.preparation_key(json.loads(request["input_json"]))
    owner = connection.execute("SELECT intake_request_id FROM account_preparation_owners WHERE preparation_key=?", (key,)).fetchone()
    if owner is None:
        connection.execute("INSERT INTO account_preparation_owners(preparation_key,intake_request_id,updated_at) VALUES(?,?,?)", (key, request["id"], at))
    elif owner[0] != request["id"]:
        # Caller has already checked previous revisions, leases and settlements.
        connection.execute("UPDATE account_preparation_owners SET intake_request_id=?,updated_at=? WHERE preparation_key=? AND intake_request_id=?",
                           (request["id"], at, key, owner[0]))


def _reuse_existing_profile(connection: sqlite3.Connection, request: Mapping[str, Any],
                           evidence: Mapping[str, Any] | None, *, at: str) -> bool:
    """Finish an unsent request when its original account's raw proof recovers.

    This is deliberately narrower than ordinary preparation: only the exact
    canonical UID and its already-proven locator can borrow existing evidence.
    Additional display handles or unverified references still need preparation.
    Started work and every billing/raw disposition retain their recovery path.
    """
    from hashlib import sha256
    if not evidence or not evidence.get("eligible"):
        return False
    value, result = json.loads(request["input_json"]), json.loads(request["result_json"])
    if (request["completed_at"] is not None or result.get("status") != "accepted"
            or any(request[field] != evidence.get(field) for field in
                   ("account_id", "account_identity_id", "directory_row_id", "platform"))
            or not value.get("uid") or value["uid"] != evidence.get("uid")
            or value.get("display_account_id") or value.get("profile_url")):
        return False
    refs = value.get("references", {})
    if any(kind != evidence.get("locator_kind") or
           sha256(locator.encode()).hexdigest() != evidence.get("locator_sha256")
           for kind, locator in refs.items()):
        return False
    proof = evidence.get("locator_evidence", {})
    if proof.get("kind") not in {"provider_profile_raw", "prepared_profile_chain"}:
        return False  # A ready/import label or admission alone is not a raw profile.
    works = connection.execute("SELECT * FROM capture_work_items WHERE intake_request_id=?", (request["id"],)).fetchall()
    if any(work["owner_token"] is not None or work["attempt_count"] != 0
           or work["state"] not in {"runnable", "provider_blocked", "budget_deferred"} for work in works):
        return False
    if (connection.execute("SELECT 1 FROM fetch_slots WHERE intake_request_id=? LIMIT 1", (request["id"],)).fetchone()
            or connection.execute("SELECT 1 FROM provider_raw_responses WHERE intake_request_id=? LIMIT 1", (request["id"],)).fetchone()
            or connection.execute("SELECT 1 FROM fetch_request_batch_members WHERE intake_request_id=? LIMIT 1", (request["id"],)).fetchone()
            or connection.execute("SELECT 1 FROM paid_provider_dispatch_events WHERE json_extract(scope_json,'$.intake_request_id')=? LIMIT 1", (request["id"],)).fetchone()):
        return False
    try:
        _require_previous_revisions_closed(connection, request)
    except PaidScopeBlocked:
        return False
    result.pop("preparation_error", None)
    result.update(status="ready", action="existing_profile_reused", activation_status="prepared",
                  uid=value["uid"], existing_profile_evidence=dict(proof),
                  message="已有有效主页资料，已复用并接入后续采集计划。")
    connection.execute("UPDATE account_intake_requests SET result_json=?,updated_at=?,completed_at=? WHERE id=?",
        (planning.canonical(result), at, at, request["id"]))
    connection.execute("""UPDATE capture_work_items SET state='terminal',reason='existing_profile_evidence_reused',
        completed_at=?,updated_at=? WHERE intake_request_id=?""", (planning.timestamp(at), planning.timestamp(at), request["id"]))
    return True


def _never_sent_step_reservation_evidence(connection: sqlite3.Connection, *, request: Mapping[str, Any],
                                     target: Mapping[str, Any]) -> dict[str, Any] | None:
    """Prove zero sends for the current step, retaining earlier resolver evidence.

    All matching slot, physical scope and batch evidence must agree. Other
    steps have distinct immutable request/window identities and are untouched.
    This proof grants no send permission; the original work rechecks readiness.
    """
    from . import paid_dispatch, providers, usage_settlements
    from .provider_budget import micro_usd
    if target.get("platform") != request["platform"] or request["completed_at"] is not None:
        return None
    works = connection.execute("""SELECT w.* FROM capture_work_items w
        JOIN capture_source_plans p ON p.id=w.source_plan_id
        WHERE w.intake_request_id=? AND w.operation=? AND p.mode='active' ORDER BY w.id DESC""",
        (request["id"], target["operation"])).fetchall()
    work = next((row for row in works if json.loads(row["envelope_json"]).get("request") == target), None)
    if work is None:
        return None
    envelope = json.loads(work["envelope_json"])
    if (work["provider"].lower() != "tikhub" or work["state"] != "paid_identity_hold" or work["reason"] != "paid_identity_hold" or work["owner_token"] is not None
            or envelope.get("stage") != "profile_prepare" or envelope.get("request") != target
            or envelope.get("preparation_key") != request["preparation_key"]
            or envelope.get("intake_request_id") != request["id"]
            or envelope.get("operation") != target["operation"]
            or envelope.get("preparation_subject") != target["subject"]
            or envelope.get("source_plan_id") != work["source_plan_id"]
            or envelope.get("preparation_plan_id") != work["source_plan_id"]
            or envelope.get("assignment_id") != work["assignment_id"]):
        return None
    if target != adapters.next_profile_request(json.loads(request["input_json"]), responses=_responses(connection, request)):
        return None
    generation = envelope.get("preparation_attempt_generation", 0)
    if type(generation) is not int or not 0 <= generation <= MAX_AUTOMATIC_PREPARATION_RETRIES:
        return None
    retry = envelope.get("preparation_retry_proof")
    if bool(retry) != (generation > 0):
        return None
    if retry and (retry.get("kind") != "retry" or _retry_evidence(connection,
            previous_work_id=retry["previous_work_id"], request=request, target=target) != retry):
        return None
    if connection.execute("SELECT 1 FROM capture_work_items WHERE intake_request_id=? AND (owner_token IS NOT NULL OR state IN ('running','leased')) LIMIT 1", (request["id"],)).fetchone():
        return None
    slots = connection.execute("SELECT * FROM fetch_slots WHERE intake_request_id=? AND stage='profile_prepare' AND window_key=?",
                               (request["id"], envelope["logical_due"])).fetchall()
    if len(slots) != 1:
        return None
    slot = slots[0]
    if (slot["provider"].lower() != "tikhub" or slot["account_id"] is not None or slot["content_id"] is not None
            or slot["status"] != "terminal_failed" or slot["attempt_count"] != 0
            or slot["stage"] != "profile_prepare" or slot["window_key"] != envelope["logical_due"]
            or slot["last_error_code"] != "paid_identity_hold"
            or slot["last_error_message"] != "batch reservation expired or changed"):
        return None
    identity = providers._paid_request_identity(operation=target["operation"], platform=target["platform"],
        subject=target["subject"], params=target["params"], cursor=None, due_bucket=envelope["logical_due"])
    member_identity = usage_settlements.member_identity(identity.document)
    batches = connection.execute("SELECT * FROM fetch_request_batches WHERE request_scope_identity=? AND sequence=0",
                                 (identity.scope_identity,)).fetchall()
    if len(batches) != 1:
        return None
    batch = batches[0]
    members = connection.execute("SELECT * FROM fetch_request_batch_members WHERE batch_id=?", (batch["id"],)).fetchall()
    if len(members) != 1 or members[0]["intake_request_id"] != request["id"]:
        return None
    member = members[0]
    if (batch is None or batch["work_id"] is not None or batch["provider"] != "tikhub"
            or batch["operation"] != target["operation"] or batch["request_scope_identity"] != identity.scope_identity
            or batch["parameters_json"] != planning.canonical(target["params"]) or batch["sequence"] != 0
            or member["member_scope_identity"] != member_identity or member["sequence"] != 0
            or member["account_id"] is not None or member["content_id"] is not None
            or connection.execute("SELECT COUNT(*) FROM fetch_request_batch_members WHERE batch_id=?", (batch["id"],)).fetchone()[0] != 1):
        return None
    admission = connection.execute("SELECT * FROM admission_reservations WHERE batch_id=?", (batch["id"],)).fetchone()
    if (admission is None or admission["state"] != "released_unsent"
            or admission["amount_microusd"] != PRICES_MICROUSD.get(target["operation"])):
        return None
    expires = planning.timestamp(admission["expires_at"])
    if planning.timestamp(admission["created_at"]) > expires:
        return None
    scopes = (identity.scope_identity, member_identity)
    if (connection.execute("SELECT 1 FROM fetch_attempts WHERE slot_id=? OR request_batch_id=? LIMIT 1", (slot["id"], batch["id"])).fetchone()
            or connection.execute("SELECT 1 FROM provider_raw_responses WHERE paid_scope_identity IN (?,?) OR (intake_request_id=? AND operation=? AND paid_scope_identity IS NULL) LIMIT 1", (*scopes, request["id"], target["operation"])).fetchone()
            or connection.execute("SELECT 1 FROM fetch_request_executions WHERE batch_id=? LIMIT 1", (batch["id"],)).fetchone()
            or connection.execute("SELECT 1 FROM fetch_request_member_dispositions WHERE member_id=? LIMIT 1", (member["id"],)).fetchone()):
        return None
    for table in ("provider_paid_scope_claims", "provider_usage_settlements", "legacy_provider_send_evidences", "legacy_paid_scope_exclusions"):
        if connection.execute(f"SELECT 1 FROM {table} WHERE scope_identity IN (?,?) LIMIT 1", scopes).fetchone():
            return None
    dispatch_ids = [row[0] for row in connection.execute("""SELECT DISTINCT dispatch_id FROM paid_provider_dispatch_events
        WHERE fetch_slot_id=? OR json_extract(cursor_identity_json,'$.paid_scope_identity') IN (?,?)
          OR (json_extract(scope_json,'$.intake_request_id')=? AND operation=?
              AND (json_extract(cursor_identity_json,'$.request.due_bucket')=?
                   OR json_extract(cursor_identity_json,'$.paid_scope_identity') IS NULL))""",
        (slot["id"], *scopes, request["id"], target["operation"], envelope["logical_due"]))]
    if not dispatch_ids:
        return None
    usage_ids, closed_events, closed_times = set(), [], []
    for dispatch_id in dispatch_ids:
        events = paid_dispatch.dispatch_events(connection, dispatch_id)
        if [event.event_type for event in events] != ["reserved", "not_sent"]:
            return None
        for event in events:
            if (event.fetch_slot_id != slot["id"] or event.fetch_attempt_id is not None or event.raw_response_id is not None
                    or any(event.scope.get(key) != envelope[key] for key in SCOPE_FIELDS)
                    or event.provider.lower() != "tikhub"
                    or event.operation != target["operation"] or event.provider_usage_id is None
                    or event.business_day != admission["charge_business_day"]
                    or budget_day(event.created_at) != admission["charge_business_day"]
                    or event.cursor_identity.get("paid_scope_identity") != identity.scope_identity
                    or event.cursor_identity.get("paid_execution_identity") != identity.execution_identity
                    or event.cursor_identity.get("sequence") != 0 or event.cursor_identity.get("request") != identity.document
                    or connection.execute("SELECT 1 FROM provider_request_start_events WHERE provider_send_marker_id=? LIMIT 1", (event.event_id,)).fetchone()
                    or connection.execute("SELECT 1 FROM provider_paid_scope_claims WHERE provider_send_marker_id=? LIMIT 1", (event.event_id,)).fetchone()
                    or connection.execute("SELECT 1 FROM scheduler_run_attempts WHERE id=? AND status='running'", (event.scheduler_attempt_id,)).fetchone()):
                return None
            usage_ids.add(event.provider_usage_id)
        closed_events.append(events[-1].event_id)
        closed_times.append(planning.timestamp(events[-1].created_at))
    if max(closed_times) < expires or planning.timestamp(admission["updated_at"]) != max(closed_times):
        return None
    usages = connection.execute("""SELECT * FROM provider_usage
        WHERE json_extract(details_json,'$.slot_id')=? OR json_extract(details_json,'$.request_batch_id')=?
          OR json_extract(details_json,'$.paid_scope_identity') IN (?,?)
          OR (json_extract(details_json,'$.scope.intake_request_id')=? AND operation=?
              AND json_extract(details_json,'$.paid_scope_identity') IS NULL)""",
        (slot["id"], batch["id"], *scopes, request["id"], target["operation"])).fetchall()
    if {row["id"] for row in usages} != usage_ids:
        return None
    for usage in usages:
        detail = json.loads(usage["details_json"])
        quote = connection.execute("SELECT * FROM provider_budget_batches WHERE id=?", (usage["budget_batch_id"],)).fetchone()
        if (quote is None or quote["provider"].lower() != "tikhub" or quote["operation"] != target["operation"]
                or quote["currency"] != "USD" or usage["currency"] != "USD"
                or micro_usd(quote["verified_unit_price"]) != admission["amount_microusd"]
                or detail.get("budget_day") != admission["charge_business_day"]
                or any(detail.get("scope", {}).get(key) != envelope[key] for key in SCOPE_FIELDS)
                or type(detail.get("paid_sequence")) is not int or detail["paid_sequence"] != 0
                or detail.get("paid_execution_identity") != identity.execution_identity
                or detail.get("paid_identity") != identity.document):
            return None
        if (usage["provider"].lower() != "tikhub" or usage["operation"] != target["operation"]
                or usage["amount"] != 0 or usage["request_attempts"] != 0 or usage["billed_requests"] != 0
                or detail.get("state") != "not_sent" or detail.get("sent_at") not in (None, "")
                or detail.get("paid_scope_identity") != identity.scope_identity
                or detail.get("slot_id") != slot["id"] or detail.get("request_batch_id") != batch["id"]
                or connection.execute("SELECT 1 FROM provider_usage_settlements WHERE provider_usage_id=?", (usage["id"],)).fetchone()
                or connection.execute("SELECT 1 FROM legacy_provider_send_evidences WHERE provider_usage_id=?", (usage["id"],)).fetchone()):
            return None
    return {"contract": "expired-unsent-preparation-step-reservation-v1", "operation": target["operation"],
            "preparation_attempt_generation": generation, "work_id": work["id"], "slot_id": slot["id"],
            "batch_id": batch["id"], "admission_id": admission["id"], "input_sha256": request["input_sha256"],
            "previous_work_updated_at": work["updated_at"], "previous_slot_updated_at": slot["updated_at"],
            "request_scope_identity": identity.scope_identity, "closed_event_ids": sorted(closed_events),
            "usage_ids": sorted(usage_ids), "logical_due": envelope["logical_due"],
            "previous_responses_sha256": planning.digest(json.loads(request["result_json"]).get("preparation_responses", []))}



def _never_sent_reservation_evidence(connection: sqlite3.Connection, *, request: Mapping[str, Any],
                                     target: Mapping[str, Any]) -> dict[str, Any] | None:
    """Prove the narrow expired singleton reservation failure never sent once."""
    if connection.execute("PRAGMA user_version").fetchone()[0] in {23, 24}:
        return _never_sent_step_reservation_evidence(connection, request=request, target=target)
    from . import paid_dispatch, providers, usage_settlements
    if target.get("operation") != "douyin_uid_profile" or request["platform"] != "douyin":
        return None
    works = connection.execute("SELECT * FROM capture_work_items WHERE intake_request_id=?", (request["id"],)).fetchall()
    if len(works) != 1:
        return None
    work = works[0]
    envelope = json.loads(work["envelope_json"])
    if (work["state"] != "paid_identity_hold" or work["reason"] != "paid_identity_hold" or work["owner_token"] is not None
            or envelope.get("stage") != "profile_prepare" or envelope.get("request") != target
            or envelope.get("preparation_key") != request["preparation_key"]
            or envelope.get("preparation_attempt_generation", 0) != 0 or envelope.get("preparation_retry_proof")):
        return None
    slots = connection.execute("SELECT * FROM fetch_slots WHERE intake_request_id=?", (request["id"],)).fetchall()
    if len(slots) != 1:
        return None
    slot = slots[0]
    if (slot["status"] != "terminal_failed" or slot["attempt_count"] != 0
            or slot["stage"] != "profile_prepare" or slot["window_key"] != envelope["logical_due"]
            or slot["last_error_code"] != "paid_identity_hold"
            or slot["last_error_message"] != "batch reservation expired or changed"):
        return None
    members = connection.execute("SELECT * FROM fetch_request_batch_members WHERE intake_request_id=?", (request["id"],)).fetchall()
    if len(members) != 1:
        return None
    member = members[0]
    batch = connection.execute("SELECT * FROM fetch_request_batches WHERE id=?", (member["batch_id"],)).fetchone()
    identity = providers._paid_request_identity(operation=target["operation"], platform=target["platform"],
        subject=target["subject"], params=target["params"], cursor=None, due_bucket=envelope["logical_due"])
    member_identity = usage_settlements.member_identity(identity.document)
    if (batch is None or batch["work_id"] is not None or batch["provider"] != "tikhub"
            or batch["operation"] != target["operation"] or batch["request_scope_identity"] != identity.scope_identity
            or batch["parameters_json"] != planning.canonical(target["params"]) or batch["sequence"] != 0
            or member["member_scope_identity"] != member_identity or member["sequence"] != 0
            or member["account_id"] is not None or member["content_id"] is not None
            or connection.execute("SELECT COUNT(*) FROM fetch_request_batch_members WHERE batch_id=?", (batch["id"],)).fetchone()[0] != 1):
        return None
    admission = connection.execute("SELECT * FROM admission_reservations WHERE batch_id=?", (batch["id"],)).fetchone()
    if admission is None or admission["state"] != "released_unsent":
        return None
    scopes = (identity.scope_identity, member_identity)
    if (connection.execute("SELECT 1 FROM fetch_attempts WHERE slot_id=? OR request_batch_id=? LIMIT 1", (slot["id"], batch["id"])).fetchone()
            or connection.execute("SELECT 1 FROM provider_raw_responses WHERE intake_request_id=? OR paid_scope_identity IN (?,?) LIMIT 1", (request["id"], *scopes)).fetchone()
            or connection.execute("SELECT 1 FROM fetch_request_executions WHERE batch_id=? LIMIT 1", (batch["id"],)).fetchone()
            or connection.execute("SELECT 1 FROM fetch_request_member_dispositions WHERE member_id=? LIMIT 1", (member["id"],)).fetchone()):
        return None
    for table in ("provider_paid_scope_claims", "provider_usage_settlements", "legacy_provider_send_evidences", "legacy_paid_scope_exclusions"):
        if connection.execute(f"SELECT 1 FROM {table} WHERE scope_identity IN (?,?) LIMIT 1", scopes).fetchone():
            return None
    dispatch_ids = [row[0] for row in connection.execute("""SELECT DISTINCT dispatch_id FROM paid_provider_dispatch_events
        WHERE json_extract(scope_json,'$.intake_request_id')=? OR fetch_slot_id=?
          OR json_extract(cursor_identity_json,'$.paid_scope_identity') IN (?,?)""", (request["id"], slot["id"], *scopes))]
    if not dispatch_ids:
        return None
    usage_ids, closed_events = set(), []
    for dispatch_id in dispatch_ids:
        events = paid_dispatch.dispatch_events(connection, dispatch_id)
        if [event.event_type for event in events] != ["reserved", "not_sent"]:
            return None
        for event in events:
            if (event.fetch_slot_id != slot["id"] or event.fetch_attempt_id is not None or event.raw_response_id is not None
                    or event.scope.get("intake_request_id") != request["id"] or event.provider.lower() != "tikhub"
                    or event.operation != target["operation"] or event.provider_usage_id is None
                    or event.cursor_identity.get("paid_scope_identity") != identity.scope_identity
                    or event.cursor_identity.get("sequence") != 0 or event.cursor_identity.get("request") != identity.document
                    or connection.execute("SELECT 1 FROM provider_request_start_events WHERE provider_send_marker_id=? LIMIT 1", (event.event_id,)).fetchone()
                    or connection.execute("SELECT 1 FROM provider_paid_scope_claims WHERE provider_send_marker_id=? LIMIT 1", (event.event_id,)).fetchone()
                    or connection.execute("SELECT 1 FROM scheduler_run_attempts WHERE id=? AND status='running'", (event.scheduler_attempt_id,)).fetchone()):
                return None
            usage_ids.add(event.provider_usage_id)
        closed_events.append(events[-1].event_id)
    usages = connection.execute("""SELECT * FROM provider_usage WHERE json_extract(details_json,'$.scope.intake_request_id')=?
        OR json_extract(details_json,'$.slot_id')=? OR json_extract(details_json,'$.request_batch_id')=?
        OR json_extract(details_json,'$.paid_scope_identity') IN (?,?)""", (request["id"], slot["id"], batch["id"], *scopes)).fetchall()
    if {row["id"] for row in usages} != usage_ids:
        return None
    for usage in usages:
        detail = json.loads(usage["details_json"])
        if (usage["amount"] != 0 or usage["request_attempts"] != 0 or usage["billed_requests"] != 0
                or detail.get("state") != "not_sent" or detail.get("sent_at") not in (None, "")
                or detail.get("paid_scope_identity") != identity.scope_identity
                or detail.get("slot_id") != slot["id"] or detail.get("request_batch_id") != batch["id"]
                or connection.execute("SELECT 1 FROM provider_usage_settlements WHERE provider_usage_id=?", (usage["id"],)).fetchone()
                or connection.execute("SELECT 1 FROM legacy_provider_send_evidences WHERE provider_usage_id=?", (usage["id"],)).fetchone()):
            return None
    return {"contract": "expired-unsent-preparation-reservation-v1", "work_id": work["id"], "slot_id": slot["id"],
            "batch_id": batch["id"], "admission_id": admission["id"], "input_sha256": request["input_sha256"],
            "previous_work_updated_at": work["updated_at"], "previous_slot_updated_at": slot["updated_at"],
            "request_scope_identity": identity.scope_identity, "closed_event_ids": sorted(closed_events),
            "usage_ids": sorted(usage_ids), "logical_due": envelope["logical_due"]}


def _recover_never_sent_preparation(connection: sqlite3.Connection, *, request: Mapping[str, Any],
                                    target: Mapping[str, Any], at: str) -> bool:
    """Atomically reopen the original unsent slot; paid admission stays unchanged."""
    proof = _never_sent_reservation_evidence(connection, request=request, target=target)
    if proof is None:
        return False
    result = json.loads(request["result_json"])
    history = result.get("unsent_reservation_recoveries", [])
    proof_sha = planning.digest(proof)
    if any(item.get("proof_sha256") == proof_sha for item in history):
        return False
    result["unsent_reservation_recoveries"] = [*history, {"at": at, "proof_sha256": proof_sha, "evidence": proof}]
    result.pop("preparation_error", None)
    connection.execute("UPDATE fetch_slots SET status='retryable_failed',updated_at=? WHERE id=?", (at, proof["slot_id"]))
    envelope = json.loads(connection.execute("SELECT envelope_json FROM capture_work_items WHERE id=?", (proof["work_id"],)).fetchone()[0])
    state, reason = readiness(connection, envelope, at=at)
    connection.execute("UPDATE capture_work_items SET state=?,reason=?,completed_at=NULL,updated_at=? WHERE id=?",
        (state, reason, planning.timestamp(at), proof["work_id"]))
    connection.execute("UPDATE account_intake_requests SET result_json=?,updated_at=? WHERE id=?",
        (planning.canonical(result), at, request["id"]))
    return True


def _load_resolver_parser_replay(connection: sqlite3.Connection, *, envelope: Mapping[str, Any],
                                request: Mapping[str, Any], target: Mapping[str, Any]) -> tuple[int, Any]:
    """Read a newly validated resolver entity without changing its failed slot."""
    from . import raw_archive
    proof = envelope.get("preparation_retry_proof", {})
    if (not isinstance(proof, Mapping) or proof.get("kind") != "replay"
            or proof.get("replay_reason") != "wechat_resolver_parser_repair"
            or target.get("operation") != "wechat_channels_resolve"
            or envelope.get("replay_raw_response_id") != proof.get("raw_response_id")
            or envelope.get("logical_due") != proof.get("previous_logical_due")
            or type(proof.get("previous_work_id")) is not int):
        raise _blocked("preparation_replay_evidence_changed")
    if _retry_evidence(connection, previous_work_id=proof["previous_work_id"], request=request, target=target) != proof:
        raise _blocked("preparation_replay_evidence_changed")
    raw_id = proof["raw_response_id"]
    return raw_id, json.loads(raw_archive.read_response_entity(connection, raw_id))


def prepare_profile_reuse(connection: sqlite3.Connection, *, active: Mapping[str, Any], at: str) -> dict[str, Any]:
    """Build fresh profile proofs in a stable read snapshot, outside the writer lock.

    This proof completes only never-started preparation. It grants no send
    authority, and the consuming writer must validate its catalog generation.
    """
    from . import catalog_revision
    from .account_capture_eligibility import derive_capture_eligibility
    revision = catalog_revision.revision(connection)
    candidates = account_intake.preparation_inputs(connection)
    identities = {row["account_identity_id"] for row in candidates if row["account_identity_id"] is not None}
    members = {}
    if identities:
        try:
            members = {row["account_identity_id"]: row for row in
                derive_capture_eligibility(connection)["eligible_members"] if row["account_identity_id"] in identities}
        except (ValueError, sqlite3.OperationalError):
            pass  # Missing/invalid bytes never complete preparation.
    return {"catalog_revision": revision, "at": at, "active_sha256": planning.digest(dict(active)),
            "members": members}


def enqueue_pending(connection: sqlite3.Connection, *, active: Mapping[str, Any], at: str, shadow: bool = False,
                    reuse_proof: Mapping[str, Any] | None = None) -> dict[str, Any]:
    if not connection.in_transaction:
        raise ValueError("preparation planning requires a writer transaction")
    schema = connection.execute("PRAGMA user_version").fetchone()[0]
    if schema not in {22, 23, 24}:
        return {"created": 0, "pending": 0, "blocked": []}
    if schema >= 23 and reuse_proof is not None:
        from . import catalog_revision
        from .profile_activations import activation_at
        if (reuse_proof.get("catalog_revision") != catalog_revision.revision(connection)
                or reuse_proof.get("at") != at or reuse_proof.get("active_sha256") != planning.digest(dict(active))
                or activation_at(connection, at) != dict(active)):
            raise _blocked("preparation_reuse_proof_changed")
    candidates = account_intake.preparation_inputs(connection)
    # Existing Douyin/Xiaohongshu UIDs need the shortest verified profile chain.
    # Keep this priority stable across entry methods and planning ticks.
    candidates.sort(key=lambda row: (not (row["value"].get("platform") in {"douyin", "xiaohongshu"}
        and adapters.valid_uid(row["value"]["platform"], row["value"].get("uid"))), row["id"]))
    policy = _policy(connection, at)
    shadow = shadow or policy is None
    reusable = {}
    if not shadow:
        if schema >= 23:
            reusable = dict(reuse_proof["members"]) if reuse_proof is not None else {}
        elif any(row["account_identity_id"] is not None for row in candidates):
            # Preserve schema22's installed recovery behavior. Schema23's
            # runtime always supplies proofs built outside this transaction.
            from .account_capture_eligibility import derive_capture_eligibility
            try:
                reusable = {row["account_identity_id"]: row for row in
                    derive_capture_eligibility(connection)["eligible_members"]}
            except (ValueError, sqlite3.OperationalError):
                pass
    members, blocked, selected, invalid, reused, recovered_unsent = [], [], {}, set(), set(), 0
    work_owners = {row[0] for row in connection.execute("SELECT DISTINCT intake_request_id FROM capture_work_items WHERE intake_request_id IS NOT NULL")}
    # Preserve an existing active owner's chain when another entry submits the
    # same locator; otherwise the oldest current request owns this revision.
    for candidate in candidates:
        try:
            request = _current_request(connection, candidate["id"])
            if _reuse_existing_profile(connection, request, reusable.get(request["account_identity_id"]), at=at):
                reused.add(candidate["id"])
                continue
        except (ValueError, RuntimeError):
            invalid.add(candidate["id"])
            continue
        key = account_intake.preparation_key(candidate["value"])
        previous = selected.get(key)
        if previous is None or (previous["id"] not in work_owners and candidate["id"] in work_owners):
            selected[key] = candidate
    for candidate in candidates:
        if candidate["id"] in reused:
            continue
        if candidate["id"] not in invalid and selected.get(account_intake.preparation_key(candidate["value"]), {}).get("id") != candidate["id"]:
            continue
        try:
            request = _current_request(connection, candidate["id"])
            target = adapters.next_profile_request(candidate["value"], responses=_responses(connection, request))
            if target is None:
                continue
            if target["operation"] not in PRICES_MICROUSD:
                raise _blocked("operation_price_unverified")
            _require_previous_revisions_closed(connection, request)
            if not shadow and _recover_never_sent_preparation(connection, request=request, target=target, at=at):
                recovered_unsent += 1
                candidate = request = _current_request(connection, candidate["id"])
            attempt = _next_attempt(connection, request=request, target=target, at=at, active=active)
            _claim_owner(connection, request, at=at)
            result = json.loads(candidate["result_json"])
            if "preparation_error" in result:
                result.pop("preparation_error")
                connection.execute("UPDATE account_intake_requests SET result_json=?,updated_at=? WHERE id=?", (planning.canonical(result), at, candidate["id"]))
            members.append({"intake_request_id": request["id"], "input_sha256": request["input_sha256"],
                "preparation_key": request["preparation_key"], "target": target, "attempt": attempt})
        except (ValueError, RuntimeError) as error:
            reason = str(getattr(error, "error_code", type(error).__name__))
            blocked.append({"intake_request_id": candidate["id"], "reason": reason})
            result = json.loads(candidate["result_json"])
            if result.get("preparation_error") != reason:
                result["preparation_error"] = reason
                connection.execute("UPDATE account_intake_requests SET result_json=?,updated_at=? WHERE id=?", (planning.canonical(result), at, candidate["id"]))
    if not members:
        return {"created": 0, "pending": len(candidates) - len(reused), "blocked": blocked,
                **({"reused": len(reused)} if reused else {}),
                **({"recovered_unsent": recovered_unsent} if recovered_unsent else {})}
    plan = {"contract": CONTRACT, "members": members, "shadow": shadow,
        "policy_sha256": planning.digest(policy) if policy else None,
        **{key: active[key] for key in ("activation_id", "profile_id", "roster_snapshot_id", "roster_members_sha256")}}
    sha = planning.digest(plan)
    row = connection.execute("SELECT id FROM capture_source_plans WHERE plan_sha256=?", (sha,)).fetchone()
    stamp = planning.timestamp(at)
    day = budget_day(at)
    if row is None:
        connection.execute("INSERT OR IGNORE INTO routing_input_changes(change_kind,roster_snapshot_id,payload_json,effective_at,recorded_at,change_sha256) VALUES('policy',?,?,?,?,?)", (active["roster_snapshot_id"], planning.canonical(plan), stamp, stamp, sha))
        change_id = connection.execute("SELECT id FROM routing_input_changes WHERE change_sha256=?", (sha,)).fetchone()[0]
        cursor = connection.execute("INSERT INTO capture_source_plans(roster_change_id,business_day,generation,mode,payload_json,created_at,plan_sha256) VALUES(?,?,1,?,?,?,?)", (change_id, day, "shadow" if shadow else "active", planning.canonical(plan), stamp, sha))
        plan_id = int(cursor.lastrowid)
    else:
        plan_id = int(row[0])
    created = 0
    # One paid preparation per normalized locator, regardless of entry source.
    selected_keys = set()
    with _planning_validation(connection, at=at, plan_id=plan_id, policy=policy):
        for member in members:
            key, intake_id, target = member["preparation_key"], member["intake_request_id"], member["target"]
            if key in selected_keys:
                continue
            selected_keys.add(key)
            operation = target["operation"]
            attempt = member["attempt"]
            identity_body = {"contract": CONTRACT, "preparation_key": key, "target": target, "shadow": shadow,
                "preparation_revision": intake_id, "activation_binding": {k: active.get(k) for k in
                    ("activation_id", "profile_id", "roster_snapshot_id", "roster_members_sha256")}}
            if attempt["generation"]:
                identity_body["preparation_attempt_generation"] = attempt["generation"]
            identity = planning.digest(identity_body)
            if (_existing_revision_work(connection, member, shadow=shadow, active=active)
                    or connection.execute("SELECT 1 FROM capture_work_items WHERE work_identity=?", (identity,)).fetchone()):
                continue
            previous = planning.current_assignment(connection, "intake", str(intake_id), operation, at=at)
            assignment_id = planning.assign_route(connection, scope_type="intake", scope_key=str(intake_id), provider="tikhub", operation=operation,
                expected_generation=previous["generation"] if previous else 0, route="integrated", mode="shadow" if shadow else "active",
                effective_at=at, recorded_at=at, intake_request_id=intake_id, source_plan_id=plan_id)
            envelope = {"contract_version": CONTRACT, "stage": "profile_prepare", "capture_stage": "profile_prepare", "category": "reconcile",
                "account_id": None, "content_id": None, "identity_id": None, "uid": None, "platform": target["platform"],
                "intake_request_id": intake_id, "preparation_plan_id": plan_id, "preparation_key": key,
                "preparation_revision": intake_id,
                "preparation_subject": target["subject"], "request": target, "operation": operation,
                "preparation_attempt_generation": attempt["generation"],
                "assignment_id": assignment_id, "source_plan_id": plan_id,
                "logical_due": attempt.get("logical_due") or "prepare:"+key+":"+planning.digest(target)+":revision:"+str(intake_id),
                "task_id": "account-preparation:"+day, "task_max_amount": TASK_CAP_USD,
                **{field: plan[field] for field in ("activation_id", "profile_id", "roster_snapshot_id", "roster_members_sha256")}}
            proof = attempt.get("proof")
            if attempt.get("authority_previous_work_id"):
                envelope["preparation_authority_recovery"] = {k: attempt[k] for k in ("authority_previous_work_id", "logical_due") if k in attempt}
            if proof is not None:
                envelope["preparation_retry_proof"] = proof
                if proof["kind"] == "replay":
                    envelope["logical_due"] = proof["previous_logical_due"]
                    envelope["replay_raw_response_id"] = proof["raw_response_id"]
                else:
                    envelope["logical_due"] += ":attempt:" + str(attempt["generation"])
            state, reason = readiness(connection, envelope, at=at)
            connection.execute("INSERT INTO capture_work_items(work_identity,assignment_id,source_plan_id,intake_request_id,provider,operation,due_at,data_business_day,state,reason,envelope_json,created_at,updated_at) VALUES(?,?,?,?,'tikhub',?,?,?,?,?,?,?,?)", (identity, assignment_id, plan_id, intake_id, operation, attempt["due_at"], day, state, reason, planning.canonical(envelope), stamp, stamp))
            if attempt.get("authority_previous_work_id") and not attempt.get("proof"):
                connection.execute("UPDATE capture_work_items SET state='terminal',reason='profile_superseded',updated_at=?,completed_at=? WHERE id=? AND owner_token IS NULL",
                                   (stamp, stamp, attempt["authority_previous_work_id"]))
            created += 1
    return {"created": created, "pending": len(candidates) - len(reused), "blocked": blocked, "plan_id": plan_id, "shadow": shadow,
            **({"reused": len(reused)} if reused else {}),
            **({"recovered_unsent": recovered_unsent} if recovered_unsent else {})}


def _validated_profile_result(response: Any, *, value: Mapping[str, Any],
                              responses: list[Mapping[str, Any]], target: Mapping[str, Any]):
    """Hand business failures and their complete bytes to the normal raw writer.

    Validation here is pure: the paid capture layer persists either the success
    or CaptureError entity before committing a terminal slot disposition. A 200
    business failure must never become a reusable succeeded preparation slot.
    """
    from .capture import CaptureError, ProviderResult
    try:
        adapters.next_profile_request(value, responses=[*responses,
            {"operation": target["operation"], "payload": response.payload}], require_raw_evidence=False)
    except Exception as error:
        code = str(getattr(error, "error_code", "profile_contract_invalid"))
        raise CaptureError(str(error), retryable=code == "provider_business_failure" or response.status == 429,
            error_code=code, http_status=response.status, billed=response.status == 200,
            raw_response=response.payload, entity_bytes=response.entity_body,
            transport_receipt=response.receipt) from error
    return ProviderResult(data={}, raw_response=response.payload, http_status=response.status,
        billed=response.status == 200, entity_bytes=response.entity_body, transport_receipt=response.receipt)


def execute_step(envelope: dict[str, Any], *, db_path: Path, at: str) -> dict[str, Any]:
    from . import capture, providers, raw_archive
    from .provider_budget import assert_paid_scope_owner
    intake_id, target = envelope["intake_request_id"], envelope["request"]
    operation, window = target["operation"], envelope["logical_due"]
    with connect(db_path) as connection:
        request = _current_request(connection, intake_id)
        responses = _responses(connection, request)
        if adapters.next_profile_request(json.loads(request["input_json"]), responses=responses) != target:
            raise _blocked("preparation_step_changed")
    try:
        if envelope.get("preparation_retry_proof", {}).get("replay_reason") == "wechat_resolver_parser_repair":
            with connect(db_path) as connection:
                raw_id, payload = _load_resolver_parser_replay(connection, envelope=envelope,
                    request=_current_request(connection, intake_id), target=target)
            cost = 0.0
        else:
            raw = capture.load_succeeded_raw_response(db_path=db_path, intake_request_id=intake_id,
                stage="profile_prepare", window_key=window, operation=operation)
            raw_id, payload, cost = raw.raw_response_id, raw.value, 0.0
        if envelope.get("replay_raw_response_id") not in (None, raw_id):
            raise _blocked("preparation_replay_raw_changed")
    except capture.SlotUnavailable:
        if envelope.get("replay_raw_response_id") is not None:
            raise _blocked("preparation_replay_raw_missing")
        budget_id = providers._budget_for_call(provider="TikHub", operation=operation,
            price=PRICES_MICROUSD[operation]/1_000_000, task_id=envelope["task_id"], task_max_amount=TASK_CAP_USD, db_path=db_path)
        def call():
            binding = providers._freeze_tikhub_transport()
            key = providers._load_key(providers.TIKHUB_KEY_FILE, "TIKHUB_API_KEY")
            result = providers._request_json(binding["manifest"]["api_base"]+target["path"],
                headers={"Authorization": "Bearer "+key}, params=target["params"] if target["method"] == "GET" else {},
                provider="TikHub", method=target["method"], body=target["params"] if target["method"] == "POST" else None)
            return _validated_profile_result(result, value=json.loads(request["input_json"]),
                responses=responses, target=target)
        outcome = capture.execute_intake_fetch(intake_request_id=intake_id, stage="profile_prepare", window_key=window,
            provider="TikHub", adapter_version=adapters.CONTRACT_VERSION, operation=operation, call=call,
            request_transport=providers._freeze_tikhub_transport(), db_path=db_path, budget_id=budget_id,
            task_id=envelope["task_id"], task_max_amount=TASK_CAP_USD,
            paid_request_identity=providers._paid_request_identity(operation=operation, platform=target["platform"], subject=target["subject"], params=target["params"], cursor=None, due_bucket=window))
        raw_id, cost = outcome.raw_response_id, outcome.amount
        with connect(db_path) as connection:
            payload = json.loads(raw_archive.read_response_entity(connection, raw_id))
    responses.append({"operation": operation, "payload": payload, "raw_response_id": raw_id})
    next_target = adapters.next_profile_request(json.loads(request["input_json"]), responses=responses)
    with connect(db_path) as connection, transaction(connection):
        assert_paid_scope_owner(connection)
        _current_request(connection, intake_id)
        if next_target is None:
            profile = adapters.normalize_profile(target["platform"], json.loads(request["input_json"]), payload, prior_responses=responses)
            applied = account_intake.apply_prepared_profile(connection, intake_id, profile, raw_id, at)
            outcome = {"status": "ready", "account_id": applied["account_id"]}
        else:
            result = json.loads(request["result_json"])
            result["preparation_responses"] = [{key: item[key] for key in ("operation", "raw_response_id")} for item in responses]
            connection.execute("UPDATE account_intake_requests SET result_json=?,updated_at=? WHERE id=?", (planning.canonical(result), at, intake_id))
            outcome = {"status": "preparing", "next_operation": next_target["operation"]}
    return {"complete": True, "continuation": False, "envelope": envelope, "reason": "", "provider_cost": cost,
        "evidence": {"raw_response_ids": [item["raw_response_id"] for item in responses], "all_raw_verified": True, **outcome}}
