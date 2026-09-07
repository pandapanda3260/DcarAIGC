"""Two-member Douyin statistics batches using the shared paid-send boundary.

One physical response retains its original bytes and batch attempt. Per-member
dispositions prove which exact content may consume that response. Repacking a
missing/invalid member never resets its stable member identity.
"""
from __future__ import annotations

import json
import sqlite3
from functools import partial
from datetime import timedelta
from pathlib import Path
from typing import Any, Sequence

from . import capture, capture_planning as planning, durable_runs, providers, raw_archive, usage_settlements
from .metric_observations import persist_metric_observation
from .paid_identity import PaidRequestIdentity, build_paid_request_identity
from .provider_budget import assert_paid_scope_owner, paid_scope
from .storage import DEFAULT_DB, connect, now_utc, transaction

OPERATION = "douyin_video_statistics"
CONTRACT = "douyin-statistics-batch-v1"
MAX_MEMBERS = 2


def _identity(ids: Sequence[str], due: str) -> PaidRequestIdentity:
    joined = ",".join(sorted(ids))
    return build_paid_request_identity(provider="TikHub", operation=OPERATION, platform="douyin",
        subject=joined, request_parameters={"aweme_ids": joined}, cursor=None, due_bucket=due)


def _batch(connection: sqlite3.Connection, batch_id: int) -> dict[str, Any]:
    row = connection.execute("SELECT * FROM fetch_request_batches WHERE id=?", (batch_id,)).fetchone()
    if row is None or row["operation"] != OPERATION or row["provider"] != "tikhub" or row["sequence"] != 0:
        raise ValueError("batch is not normal Douyin statistics sequence zero")
    return dict(row)


def freeze_batch(connection: sqlite3.Connection, *, work_ids: Sequence[int], at: str) -> dict[str, Any]:
    """Freeze a pair or odd final singleton; never rewrite an existing batch."""
    if connection.execute("PRAGMA user_version").fetchone()[0] != 20 or not connection.in_transaction:
        raise ValueError("batch freeze requires a schema20 writer transaction")
    if not 1 <= len(work_ids) <= MAX_MEMBERS or len(set(work_ids)) != len(work_ids):
        raise ValueError("statistics batches contain one or two unique members")
    members: list[dict[str, Any]] = []
    for work_id in work_ids:
        row = connection.execute("""SELECT w.*,c.platform_content_id,c.platform FROM capture_work_items w
            JOIN content_items c ON c.id=w.content_id WHERE w.id=?""", (work_id,)).fetchone()
        if row is None or row["operation"] != OPERATION or row["platform"] != "douyin":
            raise ValueError("statistics work identity or platform mismatch")
        envelope = json.loads(row["envelope_json"])
        if envelope.get("stage") != "metrics" or envelope.get("source_stage", "metrics") != "metrics":
            raise ValueError("statistics batch cannot contain other request stages")
        member_identity = _identity([str(row["platform_content_id"])], envelope["logical_due"])
        members.append({"work": dict(row), "envelope": envelope, "identity": member_identity,
                        "member_scope_identity": usage_settlements.member_identity(member_identity.document)})
    members.sort(key=lambda value: value["work"]["platform_content_id"])
    first = members[0]["envelope"]
    for member in members:
        if any(member["envelope"].get(key) != first.get(key) for key in
               ("logical_due", "activation_id", "profile_id", "roster_snapshot_id", "roster_members_sha256")):
            raise ValueError("statistics batch cannot mix logical due or roster activation")
    request = _identity([m["work"]["platform_content_id"] for m in members], first["logical_due"])
    previous = connection.execute("SELECT id FROM fetch_request_batches WHERE request_scope_identity=? AND sequence=0", (request.scope_identity,)).fetchone()
    if previous is None:
        usage_settlements.require_scope_available(connection, identity=request.scope_identity)
        for member in members:
            usage_settlements.require_scope_available(connection, identity=member["member_scope_identity"])
            if connection.execute("SELECT 1 FROM fetch_request_batch_members WHERE member_scope_identity=? AND sequence=0", (member["member_scope_identity"],)).fetchone():
                raise ValueError("member identity belongs to a prior frozen batch")
        cursor = connection.execute("""INSERT INTO fetch_request_batches(work_id,request_scope_identity,sequence,provider,
            operation,parameters_json,created_at) VALUES(?,?,0,'tikhub',?,?,?)""",
            (members[0]["work"]["id"], request.scope_identity, OPERATION,
             planning.canonical(request.document["request_parameters"]), planning.timestamp(at)))
        batch_id = int(cursor.lastrowid or 0)
        for member in members:
            connection.execute("""INSERT INTO fetch_request_batch_members(batch_id,member_scope_identity,sequence,
                content_id,account_id) VALUES(?,?,0,?,?)""", (batch_id, member["member_scope_identity"],
                    member["work"]["content_id"], member["work"]["account_id"]))
    else:
        batch_id = int(previous[0])
        retained = {row[0] for row in connection.execute("SELECT member_scope_identity FROM fetch_request_batch_members WHERE batch_id=?", (batch_id,))}
        if retained != {member["member_scope_identity"] for member in members}:
            raise ValueError("frozen batch member set changed")
    for member in members:
        envelope = {**member["envelope"], "request_batch_id": batch_id}
        connection.execute("UPDATE capture_work_items SET envelope_json=?,updated_at=? WHERE id=?",
                           (planning.canonical(envelope), planning.timestamp(at), member["work"]["id"]))
    return {"batch_id": batch_id, "request_identity": request,
            "members": members, "member_identities": tuple(m["identity"] for m in members),
            "assignment_ids": tuple(int(m["envelope"]["assignment_id"]) for m in members)}


def parse_statistics_members(payload: Any, requested_ids: Sequence[str]) -> dict[str, dict[str, Any]]:
    """Match IDs, not array order; malformed/duplicate/missing rows never fill zero."""
    if not 1 <= len(requested_ids) <= MAX_MEMBERS or len(set(requested_ids)) != len(requested_ids):
        raise ValueError("invalid requested member set")
    try:
        data = providers._tikhub_douyin_data(payload)
    except capture.CaptureError as error:
        return {identifier: {"disposition": "unusable", "reason": error.error_code, "metrics": None}
                for identifier in requested_ids}
    items = data.get("statistics_list") if isinstance(data, dict) else None
    if not isinstance(items, list):
        return {identifier: {"disposition": "invalid", "reason": "statistics_list_missing", "metrics": None} for identifier in requested_ids}
    result: dict[str, dict[str, Any]] = {}
    for identifier in requested_ids:
        matching = [item for item in items if isinstance(item, dict) and type(item.get("aweme_id")) in {int, str}
                    and str(item["aweme_id"]) == identifier]
        if not matching:
            result[identifier] = {"disposition": "missing", "reason": "requested_id_absent", "metrics": None}
        elif len(matching) != 1:
            result[identifier] = {"disposition": "invalid", "reason": "duplicate_requested_id", "metrics": None}
        else:
            # Reuse existing strict numeric normalization and operation contract.
            isolated = {"code": 200, "data": {"statistics_list": matching}}
            try:
                metrics = providers._parse_douyin_stage_payload("metrics", identifier, isolated).data
            except capture.CaptureError as error:
                result[identifier] = {"disposition": "invalid", "reason": error.error_code, "metrics": None}
            else:
                result[identifier] = {"disposition": "valid", "reason": "requested_id_and_view_verified", "metrics": metrics}
    return result


def materialize_batch(connection: sqlite3.Connection, *, batch_id: int,
                      raw_response_id: int, at: str) -> dict[str, Any]:
    """Persist dispositions first, then project only valid member/raw proofs."""
    if not connection.in_transaction:
        raise ValueError("batch materialization requires writer transaction")
    batch = _batch(connection, batch_id)
    raw = connection.execute("""SELECT r.*,a.request_batch_id,a.slot_id FROM provider_raw_responses r
        JOIN fetch_attempts a ON a.id=r.fetch_attempt_id WHERE r.id=?""", (raw_response_id,)).fetchone()
    if (raw is None or raw["request_batch_id"] != batch_id or raw["slot_id"] is not None
            or str(raw["provider"]).lower() != "tikhub" or raw["operation"] != OPERATION
            or raw["content_id"] is not None or raw["account_id"] is not None
            or raw["paid_scope_identity"] != batch["request_scope_identity"] or raw["sequence"] != 0):
        raise ValueError("batch raw attempt or shared-response lineage mismatch")
    if not connection.execute("SELECT 1 FROM fetch_request_executions WHERE batch_id=? AND fetch_attempt_id=?", (batch_id, raw["fetch_attempt_id"])).fetchone():
        raise ValueError("batch raw lacks request execution evidence")
    receipt = connection.execute("SELECT * FROM fetch_transport_receipts WHERE id=? AND fetch_attempt_id=?",
        (raw["transport_receipt_id"], raw["fetch_attempt_id"])).fetchone()
    if receipt is None or not receipt["clean_eof"] or not receipt["json_parse_ok"] or receipt["length_match"] == 0:
        raise ValueError("batch raw lacks complete verified transport evidence")
    members = connection.execute("""SELECT m.*,c.platform_content_id FROM fetch_request_batch_members m
        JOIN content_items c ON c.id=m.content_id WHERE m.batch_id=? ORDER BY c.platform_content_id""", (batch_id,)).fetchall()
    requested = [str(row["platform_content_id"]) for row in members]
    if planning.canonical({"aweme_ids": ",".join(requested)}) != batch["parameters_json"]:
        raise ValueError("batch member identifiers no longer match frozen request")
    parsed = parse_statistics_members(json.loads(raw_archive.read_response_entity(connection, raw_response_id)), requested)
    recorded_at = max(planning.timestamp(at), planning.timestamp(raw["captured_at"]))
    work = connection.execute("SELECT envelope_json FROM capture_work_items WHERE id=?", (batch["work_id"],)).fetchone()
    if work is None:
        raise ValueError("batch owner work is missing")
    due = json.loads(work[0])["logical_due"]
    output = []
    for member in members:
        decision = parsed[str(member["platform_content_id"])]
        evidence = {"contract_version": CONTRACT, "batch_id": batch_id, "content_id": member["content_id"],
            "platform_content_id": str(member["platform_content_id"]), "raw_response_id": raw_response_id,
            "reason": decision["reason"], "disposition": decision["disposition"]}
        encoded = planning.canonical(evidence)
        old = connection.execute("SELECT * FROM fetch_request_member_dispositions WHERE member_id=?", (member["id"],)).fetchone()
        if old is not None and (old["raw_response_id"] != raw_response_id or old["evidence_json"] != encoded):
            raise ValueError("member disposition conflicts with immutable result")
        connection.execute("""INSERT OR IGNORE INTO fetch_request_member_dispositions(member_id,disposition,
            raw_response_id,evidence_json,recorded_at) VALUES(?,?,?,?,?)""", (member["id"], decision["disposition"], raw_response_id, encoded, recorded_at))
        if decision["disposition"] == "valid":
            values = decision["metrics"]
            persist_metric_observation(connection, content_id=member["content_id"], captured_at=raw["captured_at"],
                window_key=due, view_count=values.get("view_count"), like_count=values.get("like_count"),
                comment_count=values.get("comment_count"), share_count=values.get("share_count"), collect_count=values.get("collect_count"),
                status="available", source="tikhub", provider="tikhub", platform="douyin", raw_response_id=raw_response_id,
                metadata_json=planning.canonical({"operation": OPERATION, "fields": values["_field_status"],
                    "batch_id": batch_id, "member_id": member["id"]}), recorded_at=recorded_at)
        output.append({"member_id": member["id"], **evidence})
    return {"batch_id": batch_id, "raw_response_id": raw_response_id, "members": output,
            "complete": all(row["disposition"] == "valid" for row in output), "raw_verified": True}


def _statistics_call(ids: Sequence[str], key: str) -> capture.ProviderResult:
    response = providers._request_json(providers._tikhub_url("/api/v1/douyin/app/v3/fetch_video_statistics"),
        headers={"Authorization": f"Bearer {key}"}, params={"aweme_ids": ",".join(sorted(ids))}, provider="TikHub")
    status, payload = response
    # Preserve every complete original byte; per-member parsing happens after
    # raw persistence even when one requested member is missing or malformed.
    try:
        providers._tikhub_douyin_data(payload)
    except capture.CaptureError as error:
        if error.billed is None:
            providers._raise_with_transport(error, response)
        billed = error.billed
    else:
        billed = True
    return providers._with_transport(response, capture.ProviderResult(payload, payload, status, billed))


def execute_batch(frozen: dict[str, Any], *, db_path: Path, at: str,
                  lease_claim: durable_runs.DurableClaim | None = None) -> dict[str, Any]:
    """Dispatch through capture's shared A/B/C hook, or replay its existing raw."""
    batch_id = int(frozen["batch_id"])
    with connect(db_path) as connection:
        raw = connection.execute("""SELECT r.id FROM provider_raw_responses r JOIN fetch_attempts a ON a.id=r.fetch_attempt_id
            WHERE a.request_batch_id=? ORDER BY r.id DESC LIMIT 1""", (batch_id,)).fetchone()
    cost = 0.0
    if raw is None:
        ids = [m["work"]["platform_content_id"] for m in frozen["members"]]
        from .capture_runtime import _business_day
        task_id = "capture-v25:" + _business_day(at)
        budget = providers._budget_for_call(provider="TikHub", operation=OPERATION, price=providers.TIKHUB_PRICE,
            task_id=task_id, task_max_amount=50.0, db_path=db_path)
        key = providers._load_key(providers.TIKHUB_KEY_FILE, "TIKHUB_API_KEY")
        outcome = capture.execute_request_batch(batch_id=batch_id, paid_request_identity=frozen["request_identity"],
            member_request_identities=frozen["member_identities"], member_assignment_ids=frozen["assignment_ids"],
            call=partial(_statistics_call, ids, key), request_transport=providers._freeze_tikhub_transport(),
            budget_id=budget, task_id=task_id, task_max_amount=50.0, db_path=db_path)
        raw_response_id, cost = outcome.raw_response_id, outcome.amount
    else:
        raw_response_id = int(raw[0])
    try:
        with connect(db_path) as connection, transaction(connection):
            materialized_at = now_utc() if lease_claim is not None else at
            if lease_claim is not None:
                from .capture_runtime import _renew_work_lease

                _renew_work_lease(connection, tuple(member["work"]["id"] for member in frozen["members"]),
                                  lease_claim, at=materialized_at)
            assert_paid_scope_owner(connection)
            result = materialize_batch(connection, batch_id=batch_id, raw_response_id=raw_response_id, at=materialized_at)
    except (raw_archive.RawArchiveError, ValueError) as error:
        raise capture.CaptureError(str(error), retryable=False, error_code="raw_response_integrity_error") from error
    return {**result, "provider_cost": cost, "provider_calls": int(raw is None)}


def run_one(db_path: Path = DEFAULT_DB, at: str | None = None) -> dict[str, Any]:
    """Own and execute one pair; blocked/held members never produce empty sends."""
    from . import capture_runtime as runtime
    from .profile_activations import activation_at
    at = at or now_utc()
    with connect(db_path) as connection, transaction(connection):
        runtime._require20(connection)
        active = activation_at(connection, at)
        if not runtime.execution_profile_allowed(active):
            return {"status": "shadow", "provider_calls": 0}
        runtime._recover_work(connection, at=at)
        candidates = connection.execute("""SELECT * FROM capture_work_items WHERE operation=? AND state='runnable'
            AND due_at<=? ORDER BY due_at,id LIMIT 64""", (OPERATION, planning.timestamp(at))).fetchall()
        selected: list[int] = []
        first: dict[str, Any] | None = None
        for row in candidates:
            if not runtime.execution_work_allowed(int(row["id"])):
                continue
            envelope = json.loads(row["envelope_json"])
            state, reason = runtime._readiness(connection, envelope, at=at)
            if state != "runnable":
                connection.execute("UPDATE capture_work_items SET state=?,reason=?,updated_at=? WHERE id=?",
                                   (state, reason, planning.timestamp(at), row["id"]))
                continue
            if first is None:
                first = envelope
            if any(envelope.get(key) != first.get(key) for key in
                   ("logical_due", "activation_id", "roster_snapshot_id", "roster_members_sha256", "request_batch_id")):
                continue
            selected.append(int(row["id"]))
            if len(selected) == MAX_MEMBERS:
                break
        if not selected:
            return {"status": "idle", "provider_calls": 0}
        try:
            frozen = freeze_batch(connection, work_ids=selected, at=at)
        except (ValueError, usage_settlements.SettlementError) as error:
            for work_id in selected:
                connection.execute("UPDATE capture_work_items SET state='paid_identity_hold',reason=?,updated_at=? WHERE id=?",
                    ("batch_identity_hold:"+str(error), planning.timestamp(at), work_id))
            return {"status": "paid_identity_hold", "provider_calls": 0, "reason": str(error)}
        owner = frozen["members"][0]["work"]
        claim_at = now_utc()
        claim = runtime._claim_work(connection, owner, at=claim_at)
        if claim is None:
            return {"status": "not_due_or_owned", "provider_calls": 0}
        for member in frozen["members"]:
            connection.execute("""UPDATE capture_work_items SET state='running',owner_token=?,heartbeat_at=?,lease_expires_at=?,
                attempt_count=attempt_count+1,updated_at=? WHERE id=? AND state='runnable'""",
                (claim.owner_token, planning.timestamp(claim_at), planning.timestamp((runtime._time(claim_at)+timedelta(seconds=durable_runs.LEASE_SECONDS)).isoformat()),
                 planning.timestamp(claim_at), member["work"]["id"]))
    with runtime._maintain_work_lease(db_path, selected, claim) as check_lease:
        first = frozen["members"][0]["envelope"]
        try:
            with paid_scope("metrics", activation_id=first["activation_id"], roster_snapshot_id=first["roster_snapshot_id"],
                    roster_snapshot_hash=first["roster_members_sha256"], scheduler_run_id=claim.scheduler_run_id,
                    scheduler_attempt_id=claim.attempt_id, business_day=runtime._business_day(claim_at)):
                check_lease()
                result = execute_batch(frozen, db_path=db_path, at=claim_at, lease_claim=claim)
        except Exception as error:
            with connect(db_path) as connection:
                cost, sent = runtime._attempt_usage(connection, claim.attempt_id)
            reason = str(getattr(error, "error_code", type(error).__name__))
            result = {"complete": False, "members": [], "provider_cost": cost,
                      "provider_calls": sent,
                      "reason": "paid_identity_hold:"+reason if sent or isinstance(error, usage_settlements.SettlementError) else reason}
        with connect(db_path) as connection, transaction(connection):
            check_lease()
            finished_at = now_utc()
            runtime._renew_work_lease(connection, selected, claim, at=finished_at)
            dispositions = {item["content_id"]: item for item in result["members"]}
            for member in frozen["members"]:
                work = member["work"]
                decision = dispositions.get(work["content_id"])
                valid = decision is not None and decision["disposition"] == "valid"
                reason = "" if valid else "batch_member_"+decision["disposition"] if decision else result.get("reason", "batch_unusable")
                state = "terminal" if valid else "paid_identity_hold" if decision else runtime._reason_state(reason)
                connection.execute("""UPDATE capture_work_items SET state=?,reason=?,owner_token=NULL,heartbeat_at=NULL,
                    lease_expires_at=NULL,completed_at=?,updated_at=? WHERE id=? AND owner_token=?""",
                    (state, reason, planning.timestamp(finished_at) if valid else None, planning.timestamp(finished_at), work["id"], claim.owner_token))
                if state == "paid_identity_hold":
                    connection.execute("INSERT OR IGNORE INTO fetch_dead_letters(work_id,reason,envelope_json,attempts,created_at) VALUES(?,?,?,?,?)",
                        (work["id"], reason, work["envelope_json"], int(work["attempt_count"])+1, planning.timestamp(finished_at)))
            proof = {"contract_version": CONTRACT, "batch_id": frozen["batch_id"], **result}
            connection.execute("INSERT OR IGNORE INTO data_quality_receipts(scope_key,cutoff_at,payload_json,recorded_at,receipt_sha256) VALUES(?,?,?,?,?)",
                (f"capture-batch:{frozen['batch_id']}", planning.timestamp(finished_at), planning.canonical(proof), planning.timestamp(finished_at), planning.digest(proof)))
            durable_runs.checkpoint(connection, claim, {"complete": result["complete"], "batch_id": frozen["batch_id"], "result": proof}, now=finished_at)
            durable_runs.finish_run_in_transaction(connection, claim, status="succeeded" if result["complete"] else "partial",
                summary=proof, next_resume_at=runtime._stamp(runtime._time(finished_at)+timedelta(minutes=5)) if not result["complete"] else None, now=finished_at)
        return {"status": "terminal" if result["complete"] else "partial", "batch_id": frozen["batch_id"],
                "member_count": len(frozen["members"]), "provider_cost": result["provider_cost"], "bounded_requests": 1,
                "provider_calls": result["provider_calls"],
                "complete": result["complete"], "members": result["members"]}
