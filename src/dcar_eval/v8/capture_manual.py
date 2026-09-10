"""Immediate content scope for persisted, explicitly requested capture work.

Automatic account membership is a scheduling preference. A manual command names
its own existing content and operations; it still uses the normal Writer, paid
request identity, budget, provider and transport boundaries. Its routes live in
a separate key space and never enable an account or alter an automatic route.
"""
from __future__ import annotations

import json
import math
import sqlite3
from datetime import timedelta
from typing import Any

from . import capture_planning as planning, durable_runs
from .content_scope import canonical_content_predicate

TRANSPORT_RETRY_CONTRACT = "manual-content-transport-retry-v1"
TRANSPORT_RETRY_TTL = timedelta(minutes=90)
TRANSPORT_RETRY_COOLDOWN = timedelta(minutes=5)


def _transport_history(connection: sqlite3.Connection, operation: str) -> tuple[int, list[tuple[int, str]], bool]:
    """Read real sends, including faults repeated under the same circuit generation."""
    high_watermark = 0
    uncertain: list[tuple[int, str]] = []
    in_flight = False
    for row in connection.execute(
        "SELECT id,recorded_at,request_attempts,details_json FROM provider_usage "
        "WHERE lower(provider)='tikhub' AND operation=? ORDER BY id", (operation,),
    ):
        high_watermark = int(row["id"])
        details = json.loads(row["details_json"])
        in_flight |= details.get("state") in {"reserved", "sent"}
        transport = details.get("transport") or {}
        if row["request_attempts"] == 1 and (details.get("error_code") == "transport_error" or any(
            transport.get(key) is False for key in ("clean_eof", "length_match", "gzip_crc_ok")
        )):
            uncertain.append((int(row["id"]), str(details.get("sent_at") or row["recorded_at"])))
    return high_watermark, uncertain, in_flight


def freeze_transport_retry(connection: sqlite3.Connection, specification: dict[str, Any], *, at: str) -> dict[str, Any]:
    """Freeze one requested metrics send; never close or renew a provider fault."""
    from . import provider_budget, providers
    from .source_routing import parse_time

    targets = specification["targets"]
    if (specification["kind"] != "metrics_update" or len(targets) != 1
            or targets[0]["stage"] != "metrics" or specification["task_max_amount"] > 3):
        raise _blocked("manual_transport_retry_invalid", "Transport retry requires one metrics group and a task budget at most USD 3")
    target, content = targets[0], specification["frozen_target"]
    operation = target["operation"]
    states = provider_budget._v2_fault_states(connection, {
        "scope_kind": "operation", "provider": "tikhub", "operation": operation,
    })
    opened = [state for state in states if state.get("open") is True]
    if len(opened) != 1 or opened[0].get("fault_class") != "transport":
        raise _blocked("manual_transport_retry_invalid", "Transport retry requires exactly one open operation transport fault")
    fault = opened[0]
    provider_budget.require_storage_ready(connection)
    if (provider_budget.circuit_state(connection) or {}).get("open") is True or (
        provider_budget.fault_state(connection, scope_kind="authorization_hard",
            authorization_id=content["identity_id"]) or {}
    ).get("open") is True:
        raise _blocked("manual_transport_retry_invalid", "Provider or account authorization fault blocks a transport retry")
    high_watermark, uncertain, in_flight = _transport_history(connection, operation)
    latest_failure = max([parse_time(fault["opened_at"]), *[parse_time(item[1]) for item in uncertain]])
    if in_flight or parse_time(at) - latest_failure < TRANSPORT_RETRY_COOLDOWN:
        raise _blocked("manual_transport_retry_cooldown", "Transport retry requires no in-flight request and five minutes since the latest uncertain send")
    if content["platform"] == "douyin":
        _, params = providers._douyin_request(target["source_stage"], content["platform_content_id"])
    else:
        row = connection.execute("SELECT content_type FROM content_items WHERE id=?", (content["content_id"],)).fetchone()
        _, params = providers._xhs_request("metrics", content["platform_content_id"], row[0])
    identity = providers._paid_request_identity(operation=operation, platform=content["platform"],
        subject=content["platform_content_id"], params=params, cursor=None, due_bucket=target["logical_due"])
    return {"contract": TRANSPORT_RETRY_CONTRACT, "operation": operation,
        "fault_generation": fault["generation"], "fault_fingerprint": fault["state_fingerprint"],
        "usage_high_watermark": high_watermark, "issued_at": at,
        "expires_at": (parse_time(at) + TRANSPORT_RETRY_TTL).isoformat(),
        "paid_scope_identity": identity.scope_identity, "max_requests": 1,
        "reason": "用户明确请求补采指定内容；仅重试已冷却的历史传输故障，保留自动采集暂停和所有费用去重约束"}


def permits_transport_retry(connection: sqlite3.Connection, *, scope: Any, operation: str, at: str) -> bool:
    """Revalidate the exact durable command at queue readiness and both paid gates."""
    from . import provider_budget
    from .source_routing import parse_time

    if scope.manual_command_run_id is None or scope.content_id is None:
        return False
    spec = validate_command(connection, scope.manual_command_run_id,
        content_id=scope.content_id, operation=operation, stage="metrics")
    if spec.get("retry_transport_fault") is not True:
        return False
    proof = spec.get("transport_retry")
    if (not isinstance(proof, dict) or proof.get("contract") != TRANSPORT_RETRY_CONTRACT
            or proof.get("operation") != operation or proof.get("max_requests") != 1
            or spec["kind"] != "metrics_update" or len(spec["targets"]) != 1
            or not 0 < spec["task_max_amount"] <= 3
            or scope.category != "metrics" or scope.paid_sequence != 0
            or scope.account_id != spec["frozen_target"]["account_id"]
            or scope.identity_id != spec["frozen_target"]["identity_id"]
            or scope.paid_scope_identity != proof.get("paid_scope_identity")
            or type(proof.get("usage_high_watermark")) is not int):
        raise _blocked("manual_transport_retry_invalid", "Transport retry does not match the frozen content request")
    issued, expires, now = parse_time(proof["issued_at"]), parse_time(proof["expires_at"]), parse_time(at)
    if not issued <= now < expires or expires - issued != TRANSPORT_RETRY_TTL:
        raise _blocked("manual_transport_retry_expired", "The explicit transport retry window expired")
    states = provider_budget._v2_fault_states(connection, {
        "scope_kind": "operation", "provider": "tikhub", "operation": operation,
    })
    opened = [state for state in states if state.get("open") is True]
    if (len(opened) != 1 or opened[0].get("fault_class") != "transport"
            or opened[0].get("generation") != proof["fault_generation"]
            or opened[0].get("state_fingerprint") != proof["fault_fingerprint"]):
        raise _blocked("manual_transport_retry_stale", "Operation fault changed after this retry was requested")
    _, uncertain, _ = _transport_history(connection, operation)
    if any(identifier > proof["usage_high_watermark"] for identifier, _ in uncertain):
        raise _blocked("manual_transport_retry_stale", "A new uncertain operation response stopped this retry batch")
    latest_failure = max([parse_time(opened[0]["opened_at"]), *[parse_time(item[1]) for item in uncertain]])
    if issued - latest_failure < TRANSPORT_RETRY_COOLDOWN:
        raise _blocked("manual_transport_retry_cooldown", "The frozen retry preceded the five-minute cooldown")
    return True


def _blocked(code: str, message: str):
    from .provider_budget import PaidScopeBlocked
    return PaidScopeBlocked(code, message)


def freeze_target(connection: sqlite3.Connection, content_id: int) -> dict[str, Any]:
    """Resolve a managed content identity without consulting automatic status."""
    if type(content_id) is not int or content_id <= 0:
        raise _blocked("manual_target_invalid", "Manual capture requires an existing content ID")
    row = connection.execute(
        "SELECT c.id content_id,c.account_id,c.platform,c.platform_content_id,"
        "c.raw_account_uid,i.id identity_id,i.uid FROM content_items c "
        "JOIN account_platform_identities i ON i.account_id=c.account_id AND i.platform=c.platform "
        "WHERE c.id=? AND " + canonical_content_predicate(connection, alias="c"),
        (content_id,),
    ).fetchone()
    if row is None or row["platform"] not in {"douyin", "xiaohongshu"} or not row["platform_content_id"]:
        raise _blocked("identity_unresolved", "Manual content has no supported managed identity")
    if row["raw_account_uid"] and row["uid"] and str(row["raw_account_uid"]) != str(row["uid"]):
        raise _blocked("identity_conflict", "Content author conflicts with managed identity")
    return {"content_id": int(row["content_id"]), "account_id": int(row["account_id"]),
            "identity_id": int(row["identity_id"]), "platform": str(row["platform"]),
            "platform_content_id": str(row["platform_content_id"]), "uid": str(row["uid"] or "")}


def validate_command(connection: sqlite3.Connection, command_run_id: int, *,
                     content_id: int, operation: str | None = None,
                     stage: str | None = None) -> dict[str, Any]:
    """Read the real command and recheck its exact target at admission/send."""
    from .capture_commands import CONTRACT, JOB

    if type(command_run_id) is not int or command_run_id <= 0:
        raise _blocked("manual_command_invalid", "A persisted manual command is required")
    row = connection.execute("SELECT * FROM scheduler_runs WHERE id=? AND job_id=?",
                             (command_run_id, JOB)).fetchone()
    if row is None or row["status"] == "failed":
        raise _blocked("manual_command_invalid", "Manual command is missing or failed")
    try:
        details = json.loads(row["details_json"])
        identity = details["identity"]
        spec = identity["specification"]
        scan_id = durable_runs.scan_identity(JOB, identity)
        if (details["contract_version"] != durable_runs.CONTRACT_VERSION
                or identity["contract_version"] != CONTRACT
                or details["scan_id"] != scan_id or row["scheduled_for"] != "scan:" + scan_id
                or spec["kind"] not in {"manual_update", "media_retry", "metrics_update"}
                or not isinstance(spec["targets"], list) or not spec["targets"]
                or spec["content_id"] != content_id
                or not isinstance(spec["frozen_target"], dict)):
            raise ValueError("manual command contract mismatch")
        if (not isinstance(spec["task_id"], str) or not spec["task_id"].strip()
                or type(spec["task_max_amount"]) not in (int, float)
                or not math.isfinite(spec["task_max_amount"]) or spec["task_max_amount"] <= 0):
            raise ValueError("manual command budget is not frozen")
    except (KeyError, TypeError, ValueError) as error:
        raise _blocked("manual_command_invalid", "Manual command has no intact frozen content scope") from error
    target = freeze_target(connection, content_id)
    if (spec["frozen_target"] != target or spec["account_id"] != target["account_id"]
            or spec["platform"] != target["platform"]):
        raise _blocked("identity_conflict", "Manual content identity changed since the request")
    from .providers import STAGE_CONFIG
    for item in spec["targets"]:
        if (not isinstance(item, dict) or item.get("stage") not in {"detail", "metrics", "comments"}
                or (target["platform"], item.get("source_stage")) not in STAGE_CONFIG
                or STAGE_CONFIG[(target["platform"], item["source_stage"])][2] != item.get("operation")):
            raise _blocked("manual_operation_not_requested", "Manual command contains an unsupported content operation")
    matches = [item for item in spec["targets"] if isinstance(item, dict)
               and (operation is None or item.get("operation") == operation)
               and (stage is None or item.get("stage") == stage)]
    if not matches:
        raise _blocked("manual_operation_not_requested", "Operation is outside the requested content update")
    return spec


def assignment_for_command(connection: sqlite3.Connection, command_run_id: int, *,
                           content_id: int, operation: str, at: str,
                           create: bool = False) -> dict[str, Any]:
    """Use an immediate command-local route, invisible to automatic lookup."""
    spec = validate_command(connection, command_run_id, content_id=content_id, operation=operation)
    key = f"manual-command:{command_run_id}:content:{content_id}"
    assignment = planning.current_assignment(connection, "content", key, operation, at=at)
    if assignment is None and create:
        from .runtime_database import require_current_process_writer_lock
        require_current_process_writer_lock(connection)
        planning.assign_route(connection, scope_type="content", scope_key=key,
            provider="tikhub", operation=operation, expected_generation=0,
            route="integrated", mode="active", effective_at=at, recorded_at=at,
            account_id=spec["account_id"], content_id=content_id)
        assignment = planning.current_assignment(connection, "content", key, operation, at=at)
    if (assignment is None or assignment["account_id"] != spec["account_id"]
            or assignment["content_id"] != content_id or assignment["source_plan_id"] is not None
            or assignment["route"] != "integrated" or assignment["mode"] != "active"
            or assignment["provider"] != "tikhub"):
        raise _blocked("route_not_active", "Requested content has no matching manual route")
    return assignment
