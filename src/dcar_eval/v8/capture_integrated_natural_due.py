"""Read-only natural-request proof for the existing integrated A/B stream.

This is not a scheduler or a paid authorization. Installation, current RELEASE,
fixed cohort admission and billing fences remain the caller's responsibility.
Completed cohort members retain route evidence; only the current A/B request
must still have its exact live durable owner and persisted page cursor.
"""
from __future__ import annotations

import json
import sqlite3
from collections.abc import Mapping
from dataclasses import replace
from typing import Any

from . import capture_batches, capture_planning as planning, capture_runtime as runtime
from . import durable_runs, provider_budget, providers, usage_settlements
from .account_roster import require_active_member
from .paid_identity import PaidRequestIdentity
from .profile_activations import activation_at
from .source_routing import parse_time
from .transport_natural_due import NaturalDueError, _scope_identity

CONTRACT = "capture-integrated-natural-due-v1"
OPERATIONS = frozenset({
    "douyin_user_posts", "douyin_video_detail", "douyin_video_statistics", "douyin_video_comments",
    "xiaohongshu_user_posts", "xiaohongshu_note_detail", "xiaohongshu_note_statistics", "xiaohongshu_note_comments",
})
ACTIVE_KEYS = ("activation_id", "profile_id", "activation_sha256", "roster_snapshot_id", "roster_members_sha256")


def _require(condition: Any, message: str) -> None:
    if not condition:
        raise NaturalDueError("integrated_natural_due_invalid", message)


def _object(value: Any) -> dict[str, Any]:
    result = json.loads(value) if isinstance(value, str) else value
    _require(isinstance(result, Mapping), "Persisted natural evidence must be an object")
    return dict(result)


def _active(connection: sqlite3.Connection, at: str) -> dict[str, Any]:
    _require(connection.execute("PRAGMA user_version").fetchone()[0] in {20, 21}, "Natural integrated work requires schema20")
    active = activation_at(connection, at)
    _require(active is not None and active["profile_id"] == "integrated_route_v1", "Current activation is not integrated")
    assert active is not None
    return {key: active[key] for key in ACTIVE_KEYS}


def _member(connection: sqlite3.Connection, work: Mapping[str, Any], active: Mapping[str, Any], *, at: str) -> dict[str, Any]:
    envelope = _object(work["envelope_json"])
    _require(envelope.get("contract_version") == runtime.CONTRACT and not envelope.get("compensation"), "Work is not normal integrated work")
    for key in ("activation_id", "profile_id", "roster_snapshot_id", "roster_members_sha256"):
        _require(envelope.get(key) == active[key], "Frozen work activation changed")
    for key in ("account_id", "content_id", "operation", "assignment_id", "source_plan_id"):
        _require(envelope.get(key) == work[key], "Work envelope differs from row")
    identity = connection.execute("SELECT * FROM account_platform_identities WHERE id=?", (envelope["identity_id"],)).fetchone()
    _require(identity is not None, "Managed identity disappeared")
    assert identity is not None
    for key in ("account_id", "platform", "uid"):
        _require(identity[key] == envelope[key], "Managed account identity changed")
    require_active_member(connection, identity["id"], active["roster_snapshot_id"], active["roster_members_sha256"], activation=active["activation_id"])
    plan = connection.execute("SELECT * FROM capture_source_plans WHERE id=?", (work["source_plan_id"],)).fetchone()
    _require(plan is not None and plan["mode"] == "active", "Work has no active frozen source plan")
    assert plan is not None
    payload = _object(plan["payload_json"])
    _require(planning.digest(payload) == plan["plan_sha256"] and all(payload.get(key) == active[key] for key in ACTIVE_KEYS), "Frozen source plan differs")
    _require(any(item.get("identity_id") == identity["id"] and item.get("uid") == identity["uid"] for item in payload["cohort"]), "Work identity is outside its cohort")
    subject = f"account:{identity['id']}"
    platform_content_id = None
    if work["content_id"] is not None:
        content = connection.execute("SELECT * FROM content_items WHERE id=?", (work["content_id"],)).fetchone()
        _require(content is not None and content["account_id"] == identity["account_id"] and content["platform"] == identity["platform"]
                 and content["raw_account_uid"] in (None, "", identity["uid"]), "Content ownership changed")
        assert content is not None
        platform_content_id = str(content["platform_content_id"])
        subject = f"content:{content['id']}"
    expected_work = planning.digest({"provider": "tikhub", "operation": work["operation"], "subject": subject, "logical_due": envelope["logical_due"]})
    _require(work["work_identity"] == expected_work and work["provider"] == "tikhub", "Frozen work identity changed")
    route = planning.resolve_route(connection, account_id=work["account_id"], content_id=work["content_id"], operation=work["operation"], at=at)
    _require(route is not None and route["id"] == work["assignment_id"] and route["provider"] == "tikhub"
             and route["route"] == "integrated" and route["mode"] == "active", "Integrated member route changed")
    assert route is not None
    return {"work_id": work["id"], "work_identity": work["work_identity"], "source_plan_id": work["source_plan_id"],
            "source_plan_sha256": plan["plan_sha256"], "identity_id": identity["id"], "account_id": work["account_id"],
            "content_id": work["content_id"], "platform": identity["platform"], "uid": identity["uid"],
            "platform_content_id": platform_content_id, "assignment_id": route["id"], "assignment_sha256": route["assignment_sha256"]}


def _single_identity(connection: sqlite3.Connection, work: Mapping[str, Any]) -> PaidRequestIdentity:
    envelope = _object(work["envelope_json"])
    platform, operation, stage = envelope["platform"], envelope["operation"], envelope["stage"]
    window = runtime._page_window(envelope)
    cursor: Any = None
    if stage == "discovery":
        _require(operation == platform + "_user_posts", "Discovery operation differs")
        cursor = envelope["cursor"] or (0 if platform == "douyin" else "")
        subject = envelope["uid"]
        if platform == "douyin":
            row = connection.execute("SELECT reference_value FROM account_provider_references WHERE account_identity_id=? AND provider='TikHub' COLLATE NOCASE AND reference_kind='sec_user_id'", (envelope["identity_id"],)).fetchone()
            _require(row is not None and providers._valid_douyin_sec_user_id(str(row[0])), "Discovery requires an existing account reference; no implicit profile call")
            assert row is not None
            subject = str(row[0])
            params = {"sec_user_id": subject, "max_cursor": cursor, "count": 20, "sort_type": 0}
        else:
            params = {"user_id": subject, "cursor": cursor}
        _require(parse_time(envelope["window_start"]) < parse_time(envelope["window_end"]), "Discovery window invalid")
    else:
        _require(stage in {"detail", "metrics", "comments"}, "Unsupported natural stage")
        content = connection.execute("SELECT * FROM content_items WHERE id=?", (work["content_id"],)).fetchone()
        _require(content is not None, "Content disappeared")
        assert content is not None
        subject = str(content["platform_content_id"])
        source_stage = envelope.get("source_stage", stage)
        _require(providers.STAGE_CONFIG[(platform, source_stage)][2] == operation, "Content stage operation changed")
        if stage == "comments":
            cursor = dict(envelope["cursor"]) if isinstance(envelope["cursor"], dict) else {"cursor": envelope["cursor"]}
        request = providers._douyin_request(source_stage, subject, cursor) if platform == "douyin" else providers._xhs_request(source_stage, subject, content["content_type"], cursor=cursor)
        params = request[1]
    return providers._paid_request_identity(operation=operation, platform=platform, subject=subject, params=params, cursor=cursor, due_bucket=window)


def _live_work(work: Mapping[str, Any], *, token: str, at: str) -> None:
    _require(work["state"] == "running" and work["owner_token"] == token
             and work["lease_expires_at"] is not None and parse_time(work["lease_expires_at"]) >= parse_time(at)
             and parse_time(work["due_at"]) <= parse_time(at), "Work is not due under the current live lease")


def _validate(connection: sqlite3.Connection, request_identity: str, operation: str, at: str) -> dict[str, Any]:
    _require(operation in OPERATIONS and isinstance(request_identity, str) and len(request_identity) == 64, "Unsupported native operation or identity")
    active = _active(connection, at)
    scope = provider_budget._assert_scheduler_owner(connection, provider_budget._SCOPE.get())
    _require(scope.scheduler_run_id is not None and scope.scheduler_attempt_id is not None and scope.paid_sequence == 0
             and scope.compensation_authorization_id is None, "Natural A requires its normal live scheduler owner")
    _require(scope.activation_id == active["activation_id"] and scope.roster_snapshot_id == active["roster_snapshot_id"]
             and scope.roster_snapshot_hash == active["roster_members_sha256"], "Paid scope activation changed")
    row = connection.execute("SELECT r.*,a.lease_expires_at,a.owner_token attempt_owner FROM scheduler_runs r JOIN scheduler_run_attempts a ON a.scheduler_run_id=r.id WHERE r.id=? AND a.id=?", (scope.scheduler_run_id, scope.scheduler_attempt_id)).fetchone()
    _require(row is not None and row["job_id"] == runtime.JOB and row["attempt_owner"] == scope.scheduler_owner_token
             and row["lease_expires_at"] is not None and parse_time(row["lease_expires_at"]) >= parse_time(at), "Integrated durable owner or lease changed")
    assert row is not None
    details = _object(row["details_json"])
    identity, checkpoint = _object(details["identity"]), _object(details["checkpoint"])
    _require(details.get("complete") is False and checkpoint.get("complete") is False
             and durable_runs.scan_identity(runtime.JOB, identity) == scope.scheduler_scan_id, "Durable frozen identity changed")
    work_row = connection.execute("SELECT * FROM capture_work_items WHERE work_identity=?", (identity["work_identity"],)).fetchone()
    _require(work_row is not None and checkpoint.get("work_id") == work_row["id"], "Durable source has no frozen work")
    assert work_row is not None
    work = dict(work_row)
    _require(work["operation"] == operation, "Current owner is for another operation")
    root = row if row["root_run_id"] is None else connection.execute("SELECT * FROM scheduler_runs WHERE id=?", (row["root_run_id"],)).fetchone()
    _require(root is not None and root["scheduled_for"] == "scan:" + durable_runs.scan_identity(runtime.JOB, {"work_identity": work["work_identity"], "business_day": work["data_business_day"]})
             and row["scheduled_for"] == root["scheduled_for"], "Durable parent identity changed")
    envelope = _object(work["envelope_json"])
    works = [work]
    batch_evidence: dict[str, Any] | None = None
    if operation == capture_batches.OPERATION:
        batch = connection.execute("SELECT * FROM fetch_request_batches WHERE id=?", (envelope.get("request_batch_id"),)).fetchone()
        _require(batch is not None and batch["work_id"] == work["id"] and batch["provider"] == "tikhub"
                 and batch["operation"] == operation and batch["sequence"] == 0, "Statistics request has no real frozen batch")
        assert batch is not None
        works = [dict(item) for item in connection.execute("SELECT * FROM capture_work_items WHERE json_extract(envelope_json,'$.request_batch_id')=? ORDER BY content_id", (batch["id"],))]
        members = [dict(item) for item in connection.execute("SELECT * FROM fetch_request_batch_members WHERE batch_id=? ORDER BY content_id", (batch["id"],))]
        _require(1 <= len(works) <= 2 and len(works) == len(members), "Batch has missing or extra work/member")
        ids = []
        for item, member in zip(works, members, strict=True):
            member_env = _object(item["envelope_json"])
            _require(item["content_id"] == member["content_id"] and item["account_id"] == member["account_id"] and member["sequence"] == 0
                     and item["operation"] == operation and member_env["stage"] == "metrics" and member_env.get("source_stage", "metrics") == "metrics"
                     and member_env["logical_due"] == envelope["logical_due"], "Frozen statistics member changed")
            content = connection.execute("SELECT platform_content_id FROM content_items WHERE id=?", (item["content_id"],)).fetchone()
            _require(content is not None, "Batch content disappeared")
            assert content is not None
            ids.append(str(content[0]))
            member_request = capture_batches._identity([str(content[0])], envelope["logical_due"])
            _require(member["member_scope_identity"] == usage_settlements.member_identity(member_request.document), "Frozen member billing identity changed")
        request = capture_batches._identity(ids, envelope["logical_due"])
        _require(batch["request_scope_identity"] == request.scope_identity and _object(batch["parameters_json"]) == request.document["request_parameters"], "Frozen batch request changed")
        batch_evidence = {"batch_id": batch["id"], "request_scope_identity": batch["request_scope_identity"], "members": members}
    else:
        _require(envelope.get("request_batch_id") is None, "Single request cannot borrow a batch")
        request = _single_identity(connection, work)
        batch = connection.execute("SELECT * FROM fetch_request_batches WHERE request_scope_identity=? AND sequence=0", (request.scope_identity,)).fetchone()
        _require(batch is not None and batch["work_id"] is None and batch["provider"] == "tikhub" and batch["operation"] == operation
                 and _object(batch["parameters_json"]) == request.document["request_parameters"], "Single request has no actual A-frozen batch")
        assert batch is not None
        members = [dict(item) for item in connection.execute("SELECT * FROM fetch_request_batch_members WHERE batch_id=?", (batch["id"],))]
        _require(len(members) == 1 and members[0]["sequence"] == 0 and members[0]["account_id"] == work["account_id"]
                 and members[0]["content_id"] == work["content_id"]
                 and members[0]["member_scope_identity"] == usage_settlements.member_identity(request.document), "Single frozen member differs")
        batch_evidence = {"batch_id": batch["id"], "request_scope_identity": batch["request_scope_identity"], "members": members}
    _require(request.scope_identity == request_identity, "Request differs from persisted natural parameters/cursor")
    _require(connection.execute("SELECT 1 FROM provider_raw_responses WHERE paid_scope_identity=? AND sequence=0 LIMIT 1", (request_identity,)).fetchone() is None, "Request already has local response evidence")
    proofs = []
    for item in works:
        _live_work(item, token=str(scope.scheduler_owner_token), at=at)
        proofs.append(_member(connection, item, active, at=at))
    primary = next(item for item in proofs if item["work_id"] == work["id"])
    checked_scope = replace(scope, account_id=primary["account_id"], content_id=primary["content_id"], identity_id=primary["identity_id"],
                            platform=primary["platform"], uid=primary["uid"], category=envelope["category"])
    _require(scope.purpose == envelope["category"], "Paid work category changed")
    proof = {"contract_version": CONTRACT, "source": "integrated_natural_due", "source_run_id": row["id"],
             "scan_id": scope.scheduler_scan_id, "source_identity_sha256": planning.digest(identity),
             "source_scheduled_for": row["scheduled_for"], "scope_identity": _scope_identity(checked_scope),
             "stage": envelope["capture_stage"], "operation": operation, "sequence": 0, "active": active,
             "paid_scope_identity": request.scope_identity, "request_document": request.document,
             "request_document_sha256": planning.digest(request.document), "primary_work_id": work["id"],
             "members": proofs, "batch": batch_evidence,
             "window": {key: envelope.get(key) for key in ("logical_due", "cursor", "window_start", "window_end")}}
    proof["proof_sha256"] = planning.digest(proof)
    return proof


def validate_native_due_request(connection: sqlite3.Connection, request_identity: str, operation: str, at: str) -> dict[str, Any]:
    """Reconstruct this original A/B request from its actual running work."""
    try:
        return _validate(connection, request_identity, operation, at)
    except NaturalDueError:
        raise
    except (KeyError, TypeError, ValueError, provider_budget.PaidScopeBlocked) as error:
        raise NaturalDueError("integrated_natural_due_invalid", "Integrated natural evidence is malformed or no longer owned") from error


def native_route(connection: sqlite3.Connection, proof: Mapping[str, Any], at: str) -> int:
    """Recheck frozen membership/routes even after the sample work completes."""
    try:
        _require(proof.get("contract_version") == CONTRACT and proof.get("source") == "integrated_natural_due"
                 and proof.get("sequence") == 0 and proof.get("operation") in OPERATIONS, "Unsupported integrated proof")
        _require(proof.get("proof_sha256") == planning.digest({key: value for key, value in proof.items() if key != "proof_sha256"})
                 and planning.digest(proof["request_document"]) == proof["paid_scope_identity"], "Frozen proof digest changed")
        active = _active(connection, at)
        _require(proof["active"] == active and 1 <= len(proof["members"]) <= 2, "Native cohort activation/membership changed")
        primary = None
        for member in proof["members"]:
            row = connection.execute("SELECT * FROM capture_work_items WHERE id=?", (member["work_id"],)).fetchone()
            _require(row is not None and row["operation"] == proof["operation"], "Frozen native member disappeared")
            assert row is not None
            _require(_member(connection, dict(row), active, at=at) == member, "Frozen native identity or route changed")
            if row["id"] == proof["primary_work_id"]:
                primary = int(member["assignment_id"])
        _require(primary is not None, "Primary native work is missing")
        assert primary is not None
        return primary
    except NaturalDueError:
        raise
    except (KeyError, TypeError, ValueError) as error:
        raise NaturalDueError("integrated_natural_due_invalid", "Frozen integrated route evidence is malformed") from error
