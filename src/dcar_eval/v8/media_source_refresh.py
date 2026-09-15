"""One explicitly reviewed source refresh through the existing paid command lane.

Preparation only freezes a quote. Execution persists one manual command. The
ordinary capture worker owns dispatch, raw evidence, billing and uncertainty.
"""
from __future__ import annotations

from datetime import timedelta
import json
import re
import sqlite3
import uuid
from typing import Any, Mapping

from .capture_singletons import attempt_slot_sql
from .source_routing import parse_time
from .storage import now_utc, transaction

CONTRACT = "media-source-refresh-v1"
TTL = timedelta(hours=1)
TABLE = "media_source_refresh_proposals"


class MediaSourceRefreshError(ValueError):
    def __init__(self, code: str, message: str):
        self.error_code = code
        super().__init__(message)


def _error(code: str, message: str):
    raise MediaSourceRefreshError(code, message)


def source_generation(connection: sqlite3.Connection, content_id: int) -> str:
    source = connection.execute("SELECT sha256 FROM evidence_artifacts WHERE content_id=? AND artifact_type='media_source' AND status='available' ORDER BY id DESC LIMIT 1", (content_id,)).fetchone()
    return str(source[0]) if source else "missing"


def _require_current(connection: sqlite3.Connection, content_id: int, generation: str) -> None:
    if generation != "missing" and (not isinstance(generation, str) or not re.fullmatch(r"[0-9a-f]{64}", generation)):
        _error("media_source_generation_invalid", "来源版本无效，请重新读取待办。")
    if source_generation(connection, content_id) != generation:
        _error("media_source_changed", "媒体来源已经变化，请重新读取待办。")


def _row(connection: sqlite3.Connection, task_id: str) -> dict[str, Any]:
    if connection.execute("PRAGMA user_version").fetchone()[0] < 23:
        _error("media_source_refresh_upgrade_required", "请先安装新版媒体来源更新服务。")
    row = connection.execute(f"SELECT * FROM {TABLE} WHERE id=?", (task_id,)).fetchone()
    if row is None:
        _error("media_source_refresh_not_found", "没有找到本次媒体来源更新任务。")
    return dict(row)


def _scope(connection: sqlite3.Connection, content_id: int) -> dict[str, Any]:
    from .capture_manual import freeze_target
    from .media_work_queue import local_media_supported
    target = freeze_target(connection, content_id)
    content = connection.execute("SELECT content_type FROM content_items WHERE id=?", (content_id,)).fetchone()
    if not local_media_supported(target["platform"], content["content_type"]):
        _error("media_capability_unverified", "该作品类型尚未通过媒体处理验证，请先核查类型。")
    return target


def _require_reacquire_boundary(connection: sqlite3.Connection, content_id: int) -> None:
    from .media import _managed_bundle, _has_managed_history
    bundle = _managed_bundle(connection, content_id)
    if bundle is not None and bundle["state"]["storage_state"] != "expired":
        _error("original_restore_required" if bundle["state"].get("archive_verified_at") else "original_already_available",
            "原件尚可查阅或免费恢复，请先使用已保存的原件。")
    if bundle is None and _has_managed_history(connection, content_id):
        _error("managed_source_pending", "当前新来源尚未完成登记，请先处理已有来源。")


def _public(row: Mapping[str, Any]) -> dict[str, Any]:
    return {"task_id": row["id"], "content_id": row["content_id"], "source_generation": row["source_generation"],
        "request_limit": row["request_limit"], "max_amount": row["max_amount"], "currency": row["currency"],
        "status": row["status"], "run_id": int(row["command_id"]) if row["command_id"] else None,
        "expires_at": row["expires_at"], "provider_calls": 0}


def _has_sent(connection: sqlite3.Connection, task_id: str) -> bool:
    if connection.execute("SELECT 1 FROM provider_usage WHERE task_id=? AND request_attempts>0 LIMIT 1", (task_id,)).fetchone():
        return True
    # Any physical send is enough to exhaust authority, even if the usage
    # projection is damaged or duplicate send evidence cannot identify one raw.
    return connection.execute("SELECT 1 FROM paid_provider_dispatch_events d JOIN provider_usage u ON u.id=d.provider_usage_id WHERE u.task_id=? AND d.event_type='send_marked' LIMIT 1", (task_id,)).fetchone() is not None


def _can_retire_unsent(connection: sqlite3.Connection, row: Mapping[str, Any]) -> bool:
    """Fail closed on evidence or active ownership; never inspect raw files.

    request_attempts is committed with the physical send marker. Even failed,
    unbilled or uncertain sends consume the single approved request.
    """
    sent = _has_sent(connection, row["id"])
    running = connection.execute("SELECT 1 FROM fetch_slots WHERE content_id=? AND stage='detail' AND window_key=? AND status='running' LIMIT 1",
        (row["content_id"], logical_due(row))).fetchone()
    if sent or running:
        return False
    raw = connection.execute(f"SELECT 1 FROM provider_raw_responses r JOIN fetch_attempts a ON a.id=r.fetch_attempt_id JOIN fetch_slots s ON s.id={attempt_slot_sql(connection, 'a')} WHERE r.content_id=? AND s.content_id=? AND s.stage='detail' AND s.window_key=? LIMIT 1",
        (row["content_id"], row["content_id"], logical_due(row))).fetchone()
    active_work = connection.execute("SELECT 1 FROM capture_work_items WHERE content_id=? AND state IN ('running','leased') AND json_extract(envelope_json,'$.task_id')=? LIMIT 1", (row["content_id"], row["id"])).fetchone()
    return not (raw or active_work)


def read_proposal(connection: sqlite3.Connection, *, task_id: str, at: str | None = None) -> dict[str, Any]:
    """Project quote expiry without renewing authority or changing a work row."""
    row = _row(connection, task_id)
    expired = row["status"] == "expired" or parse_time(at or now_utc()) >= parse_time(row["expires_at"])
    can_requote = expired and _can_retire_unsent(connection, row)
    return {**_public(row), "status": "expired" if can_requote else row["status"],
        "can_requote": can_requote,
        "reason": "media_source_refresh_expired" if can_requote else ""}


def prepare_media_source_refresh(connection: sqlite3.Connection, *, content_id: int, request_id: str,
                                source_generation: str, at: str | None = None) -> dict[str, Any]:
    from .runtime_database import require_current_process_writer_lock
    from .providers import STAGE_CONFIG
    require_current_process_writer_lock(connection)
    at = at or now_utc()
    if not isinstance(request_id, str) or not 1 <= len(request_id) <= 200:
        _error("media_source_refresh_request_invalid", "本次请求编号无效，请重新打开待办。")
    with transaction(connection):
        if connection.execute("PRAGMA user_version").fetchone()[0] < 23:
            _error("media_source_refresh_upgrade_required", "请先安装新版媒体来源更新服务。")
        # This explicit prepare action may replace an expired, never-sent
        # quote. Reads and background polling never renew authorizations.
        waiting = connection.execute(f"SELECT * FROM {TABLE} WHERE content_id=? AND status IN ('prepared','queued') AND julianday(expires_at)<=julianday(?)", (content_id, at)).fetchall()
        for old in waiting:
            if _can_retire_unsent(connection, old):
                connection.execute(f"UPDATE {TABLE} SET status='expired',updated_at=? WHERE id=?", (at, old["id"]))
        previous = connection.execute(f"SELECT * FROM {TABLE} WHERE request_key=?", (request_id,)).fetchone()
        if previous is not None:
            if previous["content_id"] != content_id or previous["source_generation"] != source_generation:
                _error("media_source_refresh_request_conflict", "该请求编号已用于另一条来源，请重新读取待办。")
            return read_proposal(connection, task_id=previous["id"], at=at)
        _require_current(connection, content_id, source_generation)
        target = _scope(connection, content_id)
        _require_reacquire_boundary(connection, content_id)
        # A second tab/request cannot purchase a second task for unchanged bytes.
        existing = connection.execute(f"SELECT * FROM {TABLE} WHERE content_id=? AND source_generation=? AND status='queued' ORDER BY created_at DESC LIMIT 1", (content_id, source_generation)).fetchone()
        if existing:
            return read_proposal(connection, task_id=existing["id"], at=at)
        operation, price = STAGE_CONFIG[(target["platform"], "detail")][2:4]
        identifier = "media-refresh:" + uuid.uuid4().hex
        connection.execute(f"INSERT INTO {TABLE}(id,request_key,content_id,source_generation,platform,platform_content_id,detail_operation,request_limit,max_amount,currency,status,command_id,created_at,expires_at,updated_at) VALUES(?,?,?,?,?,?,?,1,?,'USD','prepared',NULL,?,?,?)",
            (identifier, request_id, content_id, source_generation, target["platform"], target["platform_content_id"], operation, price, at, (parse_time(at) + TTL).isoformat(), at))
        return read_proposal(connection, task_id=identifier, at=at)


def _proof(row: Mapping[str, Any]) -> dict[str, Any]:
    return {"contract": CONTRACT, "proposal_id": row["id"], "source_generation": row["source_generation"],
        "request_limit": 1, "expires_at": row["expires_at"], "operation": row["detail_operation"]}


def logical_due(row: Mapping[str, Any]) -> str:
    return "media-source-refresh:" + row["id"] + ":" + row["source_generation"]


def _successful_raw(connection: sqlite3.Connection, proposal: Mapping[str, Any]) -> bool:
    row = connection.execute(f"SELECT r.* FROM provider_raw_responses r JOIN fetch_attempts a ON a.id=r.fetch_attempt_id JOIN fetch_slots s ON s.id={attempt_slot_sql(connection, 'a')} WHERE r.content_id=? AND r.provider='TikHub' AND r.operation=? AND s.stage='detail' AND s.window_key=? AND s.status='succeeded' AND a.error_code IS NULL AND r.http_status=200 ORDER BY r.id DESC LIMIT 1",
        (proposal["content_id"], proposal["detail_operation"], logical_due(proposal))).fetchone()
    if row is None:
        return False
    from .capture import _read_verified_raw_response
    _read_verified_raw_response(row, connection=connection)
    return True


def manual_spec(connection: sqlite3.Connection, *, content_id: int, task_id: str, at: str) -> dict[str, Any]:
    from .providers import STAGE_CONFIG
    row = _row(connection, task_id)
    if row["status"] == "expired":
        _error("media_source_refresh_expired", "本次来源更新确认已过期，请重新读取待办。")
    if row["content_id"] != content_id or row["status"] not in {"prepared", "queued"}:
        _error("media_source_refresh_scope_invalid", "本次任务与作品不一致，请重新读取待办。")
    replay = row["status"] == "queued" and _successful_raw(connection, row)
    sent = _has_sent(connection, row["id"])
    if sent and not replay:
        # A sent/uncertain task is a hold even after TTL, never an unsent
        # expiry that the scheduler can close and the operator can requote.
        _error("media_source_refresh_request_limit", "本次来源更新请求已发送，请核查结果，不会重复调用。")
    if not replay and parse_time(at) >= parse_time(row["expires_at"]):
        _error("media_source_refresh_expired", "本次来源更新确认已过期，请重新读取待办。")
    current = connection.execute("SELECT id FROM evidence_artifacts WHERE content_id=? AND artifact_type='media_source' AND status='available' ORDER BY id DESC LIMIT 1", (content_id,)).fetchone()
    if not (replay and current and authorized_source_request(connection, content_id, int(current[0])) == row["id"]):
        _require_current(connection, content_id, row["source_generation"])
    target = _scope(connection, content_id)
    operation, price = STAGE_CONFIG[(target["platform"], "detail")][2:4]
    if (target["platform"] != row["platform"] or target["platform_content_id"] != row["platform_content_id"]
            or operation != row["detail_operation"] or price != row["max_amount"] or row["currency"] != "USD"):
        _error("media_source_refresh_quote_changed", "作品身份或费用已经变化，请重新读取待办。")
    return {"content_id": content_id, "account_id": target["account_id"], "platform": target["platform"],
        "kind": "media_source_refresh", "frozen_target": target,
        "targets": [{"stage": "detail", "source_stage": "detail", "operation": operation, "logical_due": logical_due(row)}],
        "allowed_groups": None, "cycle_key": None, "task_id": row["id"], "task_max_amount": float(row["max_amount"]),
        "media_source_refresh": _proof(row)}


def execute_media_source_refresh(connection: sqlite3.Connection, *, content_id: int, task_id: str,
                                source_generation: str, at: str | None = None) -> dict[str, Any]:
    from .capture_commands import persist_specification
    from .runtime_database import require_current_process_writer_lock
    require_current_process_writer_lock(connection)
    at = at or now_utc()
    with transaction(connection):
        row = _row(connection, task_id)
        if row["content_id"] != content_id or row["source_generation"] != source_generation:
            _error("media_source_refresh_scope_invalid", "本次任务与来源不一致，请重新读取待办。")
        if row["status"] == "queued":
            return read_proposal(connection, task_id=task_id, at=at)
        spec = manual_spec(connection, content_id=content_id, task_id=task_id, at=at)
        _require_reacquire_boundary(connection, content_id)
        other = connection.execute(f"SELECT id FROM {TABLE} WHERE content_id=? AND source_generation=? AND status='queued'", (content_id, source_generation)).fetchone()
        if other is not None:
            _error("media_source_refresh_already_queued", "这份来源已有更新任务，请查看更新记录。")
        result = persist_specification(connection, specification=spec, at=at)
        connection.execute(f"UPDATE {TABLE} SET status='queued',command_id=?,updated_at=? WHERE id=? AND status='prepared'", (str(result["run_id"]), at, task_id))
        return read_proposal(connection, task_id=task_id, at=at)


def validate_command_proposal(connection: sqlite3.Connection, specification: Mapping[str, Any], command_run_id: int) -> None:
    row = _row(connection, specification["task_id"])
    if row["status"] != "queued" or row["command_id"] != str(command_run_id):
        _error("media_source_refresh_authorization_missing", "来源更新尚未得到本次执行确认。")
    expected = manual_spec(connection, content_id=specification["content_id"], task_id=row["id"], at=now_utc())
    if dict(specification) != expected:
        _error("media_source_refresh_scope_invalid", "已确认的媒体更新任务发生变化。")
    replay = _successful_raw(connection, row)
    if not replay:
        _require_reacquire_boundary(connection, specification["content_id"])
    # Retrying a no-send block is safe. Any physical send, including an
    # unbilled/uncertain response, consumes this explicitly quoted one request.
    if _has_sent(connection, row["id"]) and not replay:
        _error("media_source_refresh_request_limit", "本次来源更新请求已发送，请核查结果，不会重复调用。")


def authorized_source_request(connection: sqlite3.Connection, content_id: int, source_artifact_id: int) -> str | None:
    """Prove a NEW source is the exact successful output of one approved command.

    This is a local evidence check after dispatch, so it neither extends quote
    expiry nor permits another request. It never reopens an old media bundle.
    """
    if connection.execute("PRAGMA user_version").fetchone()[0] < 23:
        return None
    source = connection.execute("SELECT * FROM evidence_artifacts WHERE id=? AND content_id=? AND artifact_type='media_source' AND status='available'", (source_artifact_id, content_id)).fetchone()
    if source is None:
        return None
    current = connection.execute("SELECT id FROM evidence_artifacts WHERE content_id=? AND artifact_type='media_source' AND status='available' ORDER BY id DESC LIMIT 1", (content_id,)).fetchone()
    if current is None or current[0] != source_artifact_id:
        return None
    raw_id = json.loads(source["metadata_json"]).get("raw_response_id")
    raw = connection.execute(f"SELECT r.*,s.window_key,s.status slot_status FROM provider_raw_responses r JOIN fetch_attempts a ON a.id=r.fetch_attempt_id JOIN fetch_slots s ON s.id={attempt_slot_sql(connection, 'a')} WHERE r.id=? AND r.content_id=? AND s.stage='detail'", (raw_id, content_id)).fetchone()
    if raw is None or raw["slot_status"] != "succeeded" or raw["provider"] != "TikHub" or raw["http_status"] != 200:
        return None
    rows = connection.execute(f"SELECT * FROM {TABLE} WHERE content_id=? AND status='queued' AND detail_operation=?", (content_id, raw["operation"])).fetchall()
    from .capture_commands import JOB, CONTRACT as COMMAND_CONTRACT
    from .durable_runs import scan_identity, CONTRACT_VERSION
    from .capture_manual import freeze_target
    for proposal in rows:
        if raw["window_key"] != logical_due(proposal) or source["sha256"] == proposal["source_generation"]:
            continue
        command = connection.execute("SELECT * FROM scheduler_runs WHERE id=? AND job_id=?", (proposal["command_id"], JOB)).fetchone()
        if command is None or command["status"] == "failed":
            continue
        details = json.loads(command["details_json"])
        identity = details.get("identity", {})
        spec = identity.get("specification", {})
        if (details.get("contract_version") == CONTRACT_VERSION and identity.get("contract_version") == COMMAND_CONTRACT
                and details.get("scan_id") == scan_identity(JOB, identity)
                and command["scheduled_for"] == "scan:" + details["scan_id"]
                and spec.get("kind") == "media_source_refresh" and spec.get("task_id") == proposal["id"]
                and spec.get("media_source_refresh") == _proof(proposal)
                and spec.get("frozen_target") == freeze_target(connection, content_id)):
            return str(proposal["id"])
    return None
