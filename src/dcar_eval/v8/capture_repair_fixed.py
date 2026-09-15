"""Exact operator-authorized sample stages, outside the ordinary due queue.

The sealed repair CLI owns the installed Writer lock. This module creates real
manual authority and a separate durable owner, then uses the unchanged paid
admission, sending, parsing and materialization boundaries. It never opens a
provider gate or resolves an unrelated HOLD. Its explicit free-failure retry
uses the existing paired, single-use compensation contract.
"""
from __future__ import annotations

from concurrent.futures import ThreadPoolExecutor
from contextlib import contextmanager
from contextvars import copy_context
from datetime import timedelta
import json
import os
from pathlib import Path
from threading import Event, Thread
from typing import Any, Mapping

from . import capture, capture_authorizations, capture_commands, capture_manual
from . import capture_planning as planning, capture_release, durable_runs, providers
from .provider_budget import PRICES_MICROUSD, paid_scope
from .profile_activations import activation_at
from .runtime_database import require_current_process_writer_lock
from .runtime_evidence_context import inheritance_boundary, prepare_inheritance
from .source_routing import parse_time
from .storage import connect, now_utc, transaction, transaction_metrics_context

CONTRACT = "fixed-sample-repair-v1"
JOB = "capture_fixed_repair_stage"
# (content, platform, stored stage, supplier stage). KS detail is local recovery.
STAGES = {
    "xhs_detail": (82117, "xiaohongshu", "detail", "detail"),
    "ks_metrics": (82116, "kuaishou", "metrics", "metrics"),
    "xhs_metrics": (82117, "xiaohongshu", "metrics", "metrics"),
    "wx_metrics": (82120, "wechat_channels", "metrics", "metrics"),
    "dy_detail_metrics": (82078, "douyin", "metrics", "detail"),
    "dy_statistics": (82078, "douyin", "metrics", "metrics"),
}
FIRST_METRICS = ("ks_metrics", "xhs_metrics", "wx_metrics", "dy_detail_metrics")


class FixedRepairRejected(RuntimeError):
    pass


def _require(value: Any, message: str) -> None:
    if not value:
        raise FixedRepairRejected(message)


def freeze_plan(connection, *, repair_run_id: str, window_key: str,
                authorization: str, at: str, expires_at: str) -> dict[str, Any]:
    """Read-only freeze; the caller seals this document with the repair plan."""
    _require(all(isinstance(s, str) and 0 < len(s.strip()) <= 200
        for s in (repair_run_id, window_key, authorization)), "repair identity/authorization missing")
    _require(parse_time(at) < parse_time(expires_at) <= parse_time(at) + timedelta(hours=24),
        "fixed repair window must expire within one day")
    active = activation_at(connection, at)
    _require(active is not None and active["profile_id"] == "integrated_route_v1", "installed profile differs")
    stages = {}
    for key, (content_id, platform, stage, source_stage) in STAGES.items():
        target = capture_manual.freeze_target(connection, content_id)
        _require(target["platform"] == platform, "fixed sample platform differs")
        operation = providers.STAGE_CONFIG[(platform, source_stage)][2]
        stages[key] = {"target": target, "stage": stage, "source_stage": source_stage,
            "operation": operation, "window_key": window_key + ":" + key,
            "price_microusd": PRICES_MICROUSD[operation]}
    db = Path(connection.execute("PRAGMA database_list").fetchone()[2]).resolve()
    return {"contract_version": CONTRACT, "repair_run_id": repair_run_id,
        "authorization": authorization, "issued_at": at, "expires_at": expires_at,
        "window_key": window_key, "task_id": "fixed-repair:" + repair_run_id,
        "task_max_amount": 1.0, "max_extra_requests_per_stage": 1,
        "database": {"path": str(db), "device": db.stat().st_dev, "inode": db.stat().st_ino},
        "activation": {k: active[k] for k in ("activation_id", "profile_id", "activation_sha256",
            "roster_snapshot_id", "roster_members_sha256")}, "stages": stages}


def validate_plan(connection, plan: Mapping[str, Any], *, at: str) -> None:
    _require(plan.get("contract_version") == CONTRACT and set(plan.get("stages", {})) == set(STAGES),
        "fixed repair stages differ")
    _require(plan.get("task_max_amount") == 1.0 and plan.get("max_extra_requests_per_stage") == 1,
        "fixed repair budget differs")
    expected = freeze_plan(connection, repair_run_id=plan["repair_run_id"], window_key=plan["window_key"],
        authorization=plan["authorization"], at=plan["issued_at"], expires_at=plan["expires_at"])
    _require(dict(plan) == expected, "frozen repair target, route price or database changed")
    _require(parse_time(plan["issued_at"]) <= parse_time(at) < parse_time(plan["expires_at"]),
        "fixed repair plan expired")
    active = activation_at(connection, at)
    _require(active is not None and all(active.get(k) == v for k, v in plan["activation"].items()),
        "fixed repair active generation changed")
    _require(sum(s["price_microusd"] * 2 for s in plan["stages"].values()) <= 1_000_000,
        "current route cost exceeds the fixed repair cap")


def _spec(plan, key):
    stage = plan["stages"][key]
    target = stage["target"]
    return {"content_id": target["content_id"], "account_id": target["account_id"],
        "platform": target["platform"], "frozen_target": target,
        "kind": "metrics_update" if stage["stage"] == "metrics" else "manual_update",
        "targets": [{"stage": stage["stage"], "source_stage": stage["source_stage"],
            "operation": stage["operation"], "logical_due": stage["window_key"]}],
        "task_id": plan["task_id"], "task_max_amount": plan["task_max_amount"],
        "fixed_repair": {"contract": CONTRACT, "plan_sha256": planning.digest(plan), "stage_key": key}}


def _command(connection, plan, key, at):
    spec = _spec(plan, key)
    command = capture_commands.persist_specification(connection, specification=spec, at=at)
    run_id = command["run_id"]
    row = connection.execute("SELECT status FROM scheduler_runs WHERE id=?", (run_id,)).fetchone()
    if row[0] == "interrupted":
        claim = durable_runs.claim_run_in_transaction(connection, capture_commands.JOB,
            {"contract_version": capture_commands.CONTRACT, "specification": spec},
            invocation_source="operator_retry", now=at)
        _require(claim is not None, "manual authority already owned")
        durable_runs.checkpoint(connection, claim, {"complete": True,
            "result": {"status": "succeeded", "reason": "fixed_direct_authority", "work_ids": []}}, now=at)
        durable_runs.finish_run_in_transaction(connection, claim, status="succeeded", now=at,
            summary={"provider_calls": 0, "fixed_direct_authority": True})
    _require(capture_manual.validate_command(connection, run_id, content_id=spec["content_id"],
        stage=plan["stages"][key]["stage"], operation=plan["stages"][key]["operation"]) == spec,
        "terminal manual command differs")
    assignment = capture_manual.assignment_for_command(connection, run_id, content_id=spec["content_id"],
        operation=plan["stages"][key]["operation"], at=at, create=True)
    return run_id, assignment["id"]


@contextmanager
def _lease(db_path: Path, claim):
    stopped = Event()
    errors = []
    def check():
        if errors:
            raise durable_runs.LostOwnership("fixed repair lease lost") from errors[0]
    def maintain():
        while not stopped.wait(durable_runs.HEARTBEAT_SECONDS):
            try:
                with connect(db_path) as c, transaction(c, priority="heartbeat"):
                    if not stopped.is_set():
                        durable_runs.assert_owner(c, claim)
                        durable_runs.heartbeat(c, claim, now=now_utc())
            except Exception as error:
                errors.append(error)
                return
    worker = Thread(target=maintain, name="fixed-repair-lease", daemon=True)
    worker.start()
    try:
        yield check
    finally:
        stopped.set()
        worker.join(timeout=1)


def _normalize(result, stage):
    if stage["stage"] == "metrics" and stage["source_stage"] == "detail":
        values = result.data.get("metrics")
        if not isinstance(values, Mapping):
            raise capture.CaptureError("detail omitted metric counters", retryable=False,
                error_code="invalid_response", billed=result.billed, raw_response=result.raw_response,
                http_status=result.http_status, entity_bytes=result.entity_bytes, transport_receipt=result.transport_receipt)
        return capture.ProviderResult({**dict(values), "_detail_projection": dict(result.data)},
            result.raw_response, result.http_status, result.billed, result.entity_bytes, result.transport_receipt)
    return result


def _execute_stage(plan, key, *, db_path: Path, cancelled: Event):
    stage = plan["stages"][key]
    target = stage["target"]
    with connect(db_path) as c:
        validate_plan(c, plan, at=now_utc())
        content = dict(c.execute("SELECT * FROM content_items WHERE id=?", (target["content_id"],)).fetchone())
    try:
        stored = capture.load_succeeded_raw_response(content_id=target["content_id"], stage=stage["stage"],
            window_key=stage["window_key"], operation=stage["operation"], db_path=db_path)
    except capture.SlotUnavailable:
        _require(not cancelled.is_set(), "repair cancelled before paid request")
        provider, adapter, operation, price = providers.STAGE_CONFIG[(target["platform"], stage["source_stage"])]
        budget = providers._budget_for_call(provider=provider, operation=operation, price=price,
            task_id=plan["task_id"], task_max_amount=plan["task_max_amount"], db_path=db_path)
        subject = providers._content_subject(content)
        params = providers._content_request_params(target["platform"], stage["source_stage"], subject, content["content_type"])
        key_value = providers._load_key(providers.TIKHUB_KEY_FILE, "TIKHUB_API_KEY")
        def call():
            return _normalize(providers._content_call(target["platform"], stage["source_stage"],
                subject, key_value, content["content_type"], expected_uid=target["uid"]), stage)
        outcome = capture.execute_content_fetch(content_id=target["content_id"], stage=stage["stage"],
            window_key=stage["window_key"], provider=provider, adapter_version=adapter, operation=operation,
            call=call, db_path=db_path, budget_id=budget, task_id=plan["task_id"],
            task_max_amount=plan["task_max_amount"], request_transport=providers._freeze_tikhub_transport(),
            paid_request_identity=providers._paid_request_identity(operation=operation, platform=target["platform"],
                subject=target["platform_content_id"], params=params, cursor=None, due_bucket=stage["window_key"]))
        replayed = False
    else:
        parsed = _normalize(providers._parse_content_payload(target["platform"], stage["source_stage"],
            providers._content_subject(content), content["content_type"], stored.value,
            status=stored.http_status or 200, expected_uid=target["uid"]), stage)
        outcome = capture.CaptureOutcome(stored.slot_id, 0, stored.raw_response_id,
            {**dict(parsed.data), "_evidence_captured_at": stored.captured_at}, False, 0.0, "USD")
        replayed = True
    providers._store_stage_result(content, stage["stage"], stage["window_key"], outcome, db_path=db_path)
    return {"stage_key": key, "status": "succeeded", "replayed": replayed,
        "raw_response_id": outcome.raw_response_id, "provider_cost": outcome.amount,
        "provider_calls": int(not replayed)}


def run_stage(plan: Mapping[str, Any], stage_key: str, *, db_path: Path,
              cancelled: Event | None = None) -> dict[str, Any]:
    """Execute one frozen stage once; repeated calls reuse committed completion.

    Caller must be the installed sealed repair CLI with its Writer lock held.
    Neither a historical HOLD nor the daily queue participates in this claim.
    """
    _require(os.environ.get("DCAR_WRITER_ENTRY") == "repair", "sealed repair entry required")
    _require(stage_key in STAGES, "unknown fixed repair stage")
    plan = json.loads(json.dumps(plan))
    cancelled = cancelled if cancelled is not None else Event()
    _require(not cancelled.is_set(), "repair cancelled before claim")
    def authority(connection, operation, at):
        validate_plan(connection, plan, at=at)
        _require(not cancelled.is_set(), "repair cancelled before paid boundary")
        _require(operation == plan["stages"][stage_key]["operation"], "paid operation differs from fixed stage")
        return capture_release.current_runtime_bindings(connection, operation, at)
    with prepare_inheritance(db_path), capture_authorizations.runtime_authority(authority):
        with transaction_metrics_context(job_id=JOB, operation=plan["stages"][stage_key]["operation"]), \
                connect(db_path) as c, transaction(c), inheritance_boundary(c):
            require_current_process_writer_lock(c)
            at = now_utc()
            validate_plan(c, plan, at=at)
            command_id, assignment_id = _command(c, plan, stage_key, at)
            active = plan["activation"]
            target = plan["stages"][stage_key]["target"]
            identity = {"contract_version": CONTRACT, "plan_sha256": planning.digest(plan), "stage_key": stage_key,
                "manual_command_run_id": command_id, "business_day": parse_time(at).astimezone(providers.SHANGHAI).date().isoformat(),
                "activation_id": active["activation_id"], "roster_snapshot_id": active["roster_snapshot_id"],
                "roster_snapshot_hash": active["roster_members_sha256"], **target}
            scheduled = "scan:" + durable_runs.scan_identity(JOB, identity)
            old = c.execute("SELECT status,details_json FROM scheduler_runs WHERE job_id=? AND scheduled_for=?", (JOB, scheduled)).fetchone()
            if old is not None and old[0] == "succeeded":
                result = json.loads(old[1])["checkpoint"]["result"]
                # Bind the cached receipt to this exact slot and operation,
                # not merely to any valid raw-response ID in the database.
                stage = plan["stages"][stage_key]
                stored = capture.load_succeeded_raw_response(content_id=target["content_id"],
                    stage=stage["stage"], window_key=stage["window_key"], operation=stage["operation"], db_path=db_path)
                _require(stored.raw_response_id == result["raw_response_id"], "completed repair raw identity differs")
                return {**result, "replayed": True, "provider_calls": 0, "provider_cost": 0.0}
            claim = durable_runs.claim_run_in_transaction(c, JOB, identity, invocation_source="operator_retry", now=now_utc())
            _require(claim is not None, "fixed stage already owned or requires authorized compensation")
        with _lease(db_path, claim) as check, planning.execution_route_context(assignment_id), \
                paid_scope(plan["stages"][stage_key]["stage"], activation_id=active["activation_id"],
                    roster_snapshot_id=active["roster_snapshot_id"], roster_snapshot_hash=active["roster_members_sha256"],
                    scheduler_run_id=claim.scheduler_run_id, scheduler_attempt_id=claim.attempt_id,
                    business_day=identity["business_day"], manual_command_run_id=command_id):
            try:
                check()
                result = _execute_stage(plan, stage_key, db_path=db_path, cancelled=cancelled)
                check()
            except Exception as error:
                with connect(db_path) as c, transaction(c):
                    durable_runs.heartbeat(c, claim, now=now_utc())
                    durable_runs.finish_run_in_transaction(c, claim, status="interrupted", now=now_utc(),
                        summary={"reason": str(getattr(error, "error_code", type(error).__name__))})
                raise
            with connect(db_path) as c, transaction(c):
                check()
                durable_runs.checkpoint(c, claim, {"complete": True, "result": result}, now=now_utc())
                durable_runs.finish_run_in_transaction(c, claim, status="succeeded", now=now_utc())
            return result


def run_fixed_stages(plan: Mapping[str, Any], *, db_path: Path,
                     cancelled: Event | None = None) -> list[dict[str, Any]]:
    """XHS detail, four first metrics concurrently, then the extra DY stats."""
    cancelled = cancelled if cancelled is not None else Event()
    def one(key):
        try:
            return run_stage(plan, key, db_path=db_path, cancelled=cancelled)
        except Exception as error:
            return {"stage_key": key, "status": "blocked", "reason": str(getattr(error, "error_code", type(error).__name__)),
                "message": str(error)[:300], "provider_calls": None}
    result = [one("xhs_detail")]
    with ThreadPoolExecutor(max_workers=4, thread_name_prefix="fixed-metrics") as pool:
        futures = [pool.submit(copy_context().run, one, key) for key in FIRST_METRICS]
        result.extend(f.result() for f in futures)
    result.append(one("dy_statistics"))
    return result


def run_authorized_retry(plan: Mapping[str, Any], stage_key: str, *, work_id: int, db_path: Path,
                         cancelled: Event | None = None):
    """Consume an existing paired sequence-1 work proof; never issue a grant."""
    from . import capture_compensation, capture_runtime
    _require(os.environ.get("DCAR_WRITER_ENTRY") == "repair", "sealed repair entry required")
    _require(stage_key in STAGES, "unknown fixed repair stage")
    cancelled = cancelled if cancelled is not None else Event()
    def authority(connection, operation, at):
        validate_plan(connection, plan, at=at)
        _require(not cancelled.is_set(), "repair cancelled before retry boundary")
        _require(operation == plan["stages"][stage_key]["operation"], "retry operation differs")
        return capture_release.current_runtime_bindings(connection, operation, at)
    with prepare_inheritance(db_path), capture_authorizations.runtime_authority(authority):
        with connect(db_path) as c, transaction(c), inheritance_boundary(c):
            require_current_process_writer_lock(c)
            validate_plan(c, plan, at=now_utc())
            stage = plan["stages"][stage_key]
            proof = capture_compensation._validate_proof(c, work_id, at=now_utc())
            content = dict(c.execute("SELECT * FROM content_items WHERE id=?", (stage["target"]["content_id"],)).fetchone())
            params = providers._content_request_params(stage["target"]["platform"], stage["source_stage"],
                providers._content_subject(content), content["content_type"])
            request = providers._paid_request_identity(operation=stage["operation"], platform=stage["target"]["platform"],
                subject=stage["target"]["platform_content_id"], params=params, cursor=None, due_bucket=stage["window_key"])
            _require(proof["sequence"] == 1 and proof["content_id"] == stage["target"]["content_id"]
                and proof["operation"] == stage["operation"]
                and proof["request_document"] == request.document
                and proof["request_identity"] == request.scope_identity, "retry is outside fixed stage")
            work = c.execute("SELECT envelope_json FROM capture_work_items WHERE id=?", (work_id,)).fetchone()
            envelope = json.loads(work[0])
            _require(envelope.get("task_id") == plan["task_id"] and envelope.get("task_max_amount") == 1.0,
                "retry task cap differs")
        return capture_runtime._run_single(db_path, now_utc(), compensation_work_id=work_id)


def _unbilled_failure(connection, plan, stage_key, usage_id, *, at):
    """Verify original fixed owner, exact bytes and explicit supplier no-charge.

    This deliberately requires clean EOF even if other content operations can
    recover a verified entity from an incomplete transport.
    """
    from . import raw_archive
    from .kuaishou_adapter import DETAIL_PATH
    validate_plan(connection, plan, at=at)
    _require(stage_key == "ks_metrics", "only the fixed KS metrics free failure is authorized")
    stage = plan["stages"][stage_key]
    target = stage["target"]
    row = connection.execute("SELECT * FROM provider_usage WHERE id=?", (usage_id,)).fetchone()
    _require(row is not None, "original fixed usage missing")
    usage = dict(row)
    details = json.loads(usage["details_json"])
    scope = details.get("scope", {})
    _require(usage["operation"] == stage["operation"] and usage["provider"].lower() == "tikhub"
        and usage["task_id"] == plan["task_id"] and usage["request_attempts"] == 1
        and usage["billed_requests"] == 0 and usage["amount"] == 0 and usage["currency"] == "USD"
        and details.get("state") == "failed" and details.get("error_code") == "provider_retry_requested"
        and details.get("paid_sequence") == 0 and details.get("sent_at"), "not an original known-free retryable send")
    command_id = scope.get("manual_command_run_id")
    _require(capture_manual.validate_command(connection, command_id, content_id=target["content_id"],
        stage=stage["stage"], operation=stage["operation"]) == _spec(plan, stage_key), "original fixed command differs")
    run = connection.execute("SELECT * FROM scheduler_runs WHERE id=?", (scope.get("scheduler_run_id"),)).fetchone()
    attempt = connection.execute("SELECT scheduler_run_id,status FROM scheduler_run_attempts WHERE id=?",
        (scope.get("scheduler_attempt_id"),)).fetchone()
    _require(run is not None and run["job_id"] == JOB and run["status"] in {"interrupted", "succeeded"}
        and attempt is not None and attempt[0] == run["id"] and attempt[1] != "running", "original fixed owner is not terminal")
    identity = json.loads(run["details_json"])["identity"]
    active = plan["activation"]
    expected = {"contract_version": CONTRACT, "plan_sha256": planning.digest(plan), "stage_key": stage_key,
        "manual_command_run_id": command_id, "business_day": scope.get("business_day"),
        "activation_id": active["activation_id"], "roster_snapshot_id": active["roster_snapshot_id"],
        "roster_snapshot_hash": active["roster_members_sha256"], **target}
    _require(identity == expected and run["scheduled_for"] == "scan:" + durable_runs.scan_identity(JOB, expected)
        and all(scope.get(k) == target[k] for k in ("account_id", "content_id", "identity_id", "platform", "uid")),
        "original usage does not belong to this frozen fixed execution")
    content = dict(connection.execute("SELECT * FROM content_items WHERE id=?", (target["content_id"],)).fetchone())
    params = providers._content_request_params(target["platform"], stage["source_stage"],
        providers._content_subject(content), content["content_type"])
    request = providers._paid_request_identity(operation=stage["operation"], platform=target["platform"],
        subject=target["platform_content_id"], params=params, cursor=None, due_bucket=stage["window_key"])
    _require(details.get("paid_identity") == request.document and details.get("paid_scope_identity") == request.scope_identity,
        "original fixed request parameters/window changed")
    markers = connection.execute("SELECT id FROM paid_provider_dispatch_events WHERE provider_usage_id=? AND event_type='send_marked' AND scheduler_run_id=? AND scheduler_attempt_id=?",
        (usage_id, run["id"], scope["scheduler_attempt_id"])).fetchall()
    _require(len(markers) == 1 and connection.execute("SELECT 1 FROM provider_paid_scope_claims WHERE provider_send_marker_id=? AND scope_kind='request' AND scope_identity=? AND sequence=0",
        (markers[0][0], request.scope_identity)).fetchone(), "original physical send identity missing")
    raw_id = details.get("transport", {}).get("raw_response_id")
    raw = connection.execute("SELECT * FROM provider_raw_responses WHERE id=?", (raw_id,)).fetchone()
    _require(raw is not None and raw["operation"] == stage["operation"] and raw["paid_scope_identity"] == request.scope_identity
        and raw["sequence"] == 0 and raw["http_status"] == 400, "original complete raw missing")
    transport = raw_archive.response_entity_integrity(connection, raw_id)
    body = raw_archive.read_response_entity(connection, raw_id)
    capture._validate_usable_content_receipt(transport, operation=stage["operation"], entity_bytes=body, http_status=400)
    _require(transport.get("clean_eof") is True and transport.get("status") == "succeeded"
        and transport.get("error_code") is None, "retry requires original clean EOF")
    payload = json.loads(body)
    detail = payload.get("detail", {})
    message = str(detail.get("message", "")).lower()
    zh = str(detail.get("message_zh", ""))
    _require(detail.get("code") == 400 and detail.get("router") == DETAIL_PATH and detail.get("params") == params
        and (("please retry" in message and "won't be charged" in message)
             or ("请重试" in zh and "不会被扣费" in zh)), "complete supplier response does not explicitly permit an unbilled retry")
    return {"usage": usage, "request": request, "command_id": command_id, "raw_response_id": raw_id,
        "original_run_id": run["id"]}


def validate_unbilled_retry_work(connection, work, envelope, original, *, at):
    """Narrow runnable-work admission used by the existing compensation path."""
    binding = envelope.get("fixed_unbilled_retry", {})
    proof = _unbilled_failure(connection, binding.get("plan", {}), "ks_metrics", original["id"], at=at)
    plan = binding["plan"]
    stage = plan["stages"]["ks_metrics"]
    _require(binding.get("original_usage_id") == original["id"] and not work["owner_token"]
        and envelope.get("manual_command_run_id") == proof["command_id"]
        and envelope.get("logical_due") == stage["window_key"]
        and envelope.get("task_id") == plan["task_id"] and envelope.get("task_max_amount") == 1.0
        and work["content_id"] == stage["target"]["content_id"], "fixed retry work differs from original authority")


def prepare_unbilled_retry(plan, stage_key, *, db_path: Path, cancelled: Event | None = None):
    """Create at most sequence 1 on the original fixed request, atomically."""
    from . import capture_compensation, capture_runtime, usage_settlements as ledger, provider_budget
    _require(os.environ.get("DCAR_WRITER_ENTRY") == "repair", "sealed repair entry required")
    _require(stage_key == "ks_metrics", "only KS metrics has an authorized free retry")
    cancelled = cancelled if cancelled is not None else Event()
    def authority(connection, operation, at):
        validate_plan(connection, plan, at=at)
        _require(not cancelled.is_set() and operation == plan["stages"][stage_key]["operation"],
            "retry preparation cancelled or operation differs")
        return capture_release.current_runtime_bindings(connection, operation, at)
    with prepare_inheritance(db_path), capture_authorizations.runtime_authority(authority), \
            connect(db_path) as c, transaction(c), inheritance_boundary(c):
        require_current_process_writer_lock(c)
        at = now_utc()
        validate_plan(c, plan, at=at)
        _require(not cancelled.is_set(), "repair cancelled before retry preparation")
        stage = plan["stages"][stage_key]
        rows = c.execute("SELECT id FROM provider_usage WHERE task_id=? AND operation=? AND json_extract(details_json,'$.paid_identity.due_bucket')=? AND json_extract(details_json,'$.paid_sequence')=0 AND request_attempts>0",
            (plan["task_id"], stage["operation"], stage["window_key"])).fetchall()
        _require(len(rows) == 1, "exactly one original fixed request is required")
        original = _unbilled_failure(c, plan, stage_key, rows[0][0], at=at)
        request = original["request"]
        _require(not c.execute("SELECT 1 FROM provider_paid_scope_claims WHERE scope_identity=? AND sequence>0",
            (request.scope_identity,)).fetchone(), "fixed stage extra request already sent")
        command = original["command_id"]
        # These existing checks inspect real current faults, category capacity,
        # identity and manual authority; no circuit or gate is cleared.
        scope = provider_budget.freeze_scope(c, content_id=stage["target"]["content_id"], account_id=None,
            stage=stage["stage"], scope=provider_budget.PaidScope(purpose="metrics", manual_command_run_id=command))
        provider_budget.check_reservation(c, scope=scope, operation=stage["operation"],
            unit_price=stage["price_microusd"] / 1_000_000, currency="USD", at=at)
        work_result = capture_runtime.enqueue_manual_work(c, specification=_spec(plan, stage_key), command_run_id=command, at=at)
        _require(len(work_result["work_ids"]) == 1, "fixed retry must reconstruct exactly one work")
        work_id = work_result["work_ids"][0]
        work, envelope = capture_compensation._work(c, work_id)
        _require(work["state"] in {"runnable", "provider_blocked", "budget_deferred", "paid_identity_hold"}
            and not work["owner_token"] and envelope.get("manual_command_run_id") == command,
            "fixed retry work is owned or differs")
        envelope["fixed_unbilled_retry"] = {"plan": dict(plan), "original_usage_id": original["usage"]["id"]}
        c.execute("UPDATE capture_work_items SET envelope_json=? WHERE id=?", (planning.canonical(envelope), work_id))
        validate_unbilled_retry_work(c, work, envelope, original["usage"], at=at)
        settled = ledger.record_settlement(c, usage_id=original["usage"]["id"], at=at)
        _require(settled["compensation_sequence"] == 0 and settled["amount_microunits"] == 0,
            "fixed retry must retain the original zero settlement")
        issued = {}
        for kind, identity in (("request", request.scope_identity), ("member", ledger.member_identity(request.document))):
            grant = ledger.authorize_compensation(c,
                authorization_key=f"fixed-unbilled:{planning.digest(plan)}:{stage_key}:{kind}",
                settlement_id=settled["id"], identity=identity, scope_kind=kind, owner=plan["authorization"],
                reason="fixed sample authorized one additional request after explicit supplier free failure",
                gap_evidence_ref=f"raw-response:{original['raw_response_id']}",
                raw_unrecoverable_reason="complete HTTP400 explicitly requests retry and contains no metric result",
                local_replay_exhausted=True, business_gap_due=True,
                max_amount_microunits=stage["price_microusd"], expires_at=plan["expires_at"], at=at,
                provider_ready=True, budget_available=True)
            _require(grant["sequence"] == 1 and grant["status"] == "issued", "fixed extra sequence must be one")
            issued[kind] = grant["issuance_id"]
        # Enqueue verifies current installed authorization and both grants in
        # this same transaction; any gate/budget rejection rolls everything back.
        result = capture_compensation.enqueue_authorized_compensation(c, work_id=work_id,
            request_issuance_id=issued["request"], member_issuance_id=issued["member"], at=at)
        return {**result, "original_usage_id": original["usage"]["id"], "settlement_id": settled["id"]}


def run_unbilled_retry(plan, stage_key, *, db_path: Path, cancelled: Event | None = None):
    """Run the single explicit retry, then close the original stage by raw reuse."""
    cancelled = cancelled if cancelled is not None else Event()
    _require(os.environ.get("DCAR_WRITER_ENTRY") == "repair" and stage_key == "ks_metrics",
        "sealed fixed KS retry entry required")
    with connect(db_path) as c:
        validate_plan(c, plan, at=now_utc())
    stage = plan["stages"][stage_key]
    try:
        capture.load_succeeded_raw_response(content_id=stage["target"]["content_id"], stage=stage["stage"],
            window_key=stage["window_key"], operation=stage["operation"], db_path=db_path)
    except capture.SlotUnavailable:
        pass
    else:
        # Also handles a crash after retry C committed but before the original
        # direct run was closed. Reusing this exact success cannot spend again.
        return {**run_stage(plan, stage_key, db_path=db_path, cancelled=cancelled),
            "actual_retry_provider_calls": 0, "actual_retry_provider_amount": 0.0}
    prepared = prepare_unbilled_retry(plan, stage_key, db_path=db_path, cancelled=cancelled)
    result = run_authorized_retry(plan, stage_key, work_id=prepared["work_id"], db_path=db_path, cancelled=cancelled)
    with connect(db_path) as c:
        sent = c.execute("SELECT request_attempts,amount FROM provider_usage WHERE task_id=? AND operation=? AND json_extract(details_json,'$.paid_identity.due_bucket')=? AND json_extract(details_json,'$.paid_sequence')=1 AND request_attempts>0",
            (plan["task_id"], stage["operation"], stage["window_key"])).fetchall()
    calls = sum(row[0] for row in sent)
    amount = None if any(row[1] is None for row in sent) else sum(float(row[1]) for row in sent)
    actual = {"actual_retry_provider_calls": calls, "actual_retry_provider_amount": amount,
        "provider_calls": calls, "provider_cost": amount}
    if result.get("status") != "terminal":
        return {"stage_key": stage_key, "status": "blocked", "retry": result, "preparation": prepared, **actual}
    completed = run_stage(plan, stage_key, db_path=db_path, cancelled=cancelled)
    return {**completed, "retry": result, "preparation": prepared, "completion": completed, **actual}
