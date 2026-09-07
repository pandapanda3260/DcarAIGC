"""Prepare same-profile roster succession without purchasing or extending proof.

The append-only activation metadata stores preparation, never paid permission.
Only the real Writer, after the target is current, issues target-bound gates.
"""

from __future__ import annotations

import json
import sqlite3
from collections.abc import Mapping, Sequence
from typing import Any
from uuid import uuid4

from . import account_roster, capture_authorizations as auth, capture_release as release
from . import (
    capture_activation_release as successor,
    paid_drain,
    provider_budget,
    capture_planning,
)
from .metric_field_facts import utc
from .profile_activations import (
    INTEGRATED_PROFILE,
    activation_at,
    activation_by_id,
    append_activation,
    replace_scheduled_activation,
)
from .runtime_database import require_current_process_writer_lock
from .source_routing import parse_time
from zoneinfo import ZoneInfo

BEIJING = ZoneInfo("Asia/Shanghai")

CONTRACT = "account-roster-capture-source-v1"
ELIGIBILITY_CONTRACT = "account-roster-capture-eligibility-v1"
METADATA_KEY = "account_roster_capture_source"
CODE_SUCCESSOR_CONTRACT = "account-roster-code-plan-successor-v1"
_GATE_KEYS = (
    "provider",
    "operation",
    "state",
    "reason",
    "evidence_json",
    "recorded_at",
)
_READY_KEYS = (
    "provider",
    "operation",
    "status",
    "reason",
    "evidence_json",
    "created_at",
    "expires_at",
)
_PLATFORM_OPERATIONS = {
    "douyin": {"douyin_user_posts", "douyin_video_detail", "douyin_video_statistics"},
    "xiaohongshu": {
        "xiaohongshu_user_posts",
        "xiaohongshu_note_detail",
        "xiaohongshu_note_statistics",
    },
}


class AccountRosterCaptureError(ValueError):
    code = "account_roster_capture_unavailable"

    def __init__(self, message: str):
        self.technical_reason = message
        if (
            "过期" in message
            or "expired" in message.lower()
            or "覆盖生效时间" in message
        ):
            public = "采集授权不足以覆盖账号生效时间，请稍后重试"
        elif "独立采集路由限制" in message:
            public = "该账号已有采集限制，不能自动覆盖"
        elif "尚无完整采集路由" in message:
            public = "该账号尚未具备完整的采集设置，暂不能恢复"
        elif "在安排后已改变" in message or "首次签发前" in message:
            public = "账号采集设置在提交后被修改，请重试"
        elif "主页身份资料不完整" in message or "有效平台身份" in message:
            public = "主页识别资料不完整，请重新提交主页链接"
        else:
            public = "采集服务尚未就绪，请稍后重试"
        super().__init__(f"尚不能安排采集，本次未保存：{public}")


def _require(value: bool, message: str) -> None:
    if not value:
        raise auth.AuthorizationError(message)


def _shape(active: Mapping[str, Any]) -> dict[str, Any]:
    value = active.get("metadata", {}).get(METADATA_KEY)
    _require(
        isinstance(value, dict) and value.get("contract") == CONTRACT,
        "账号名单缺少冻结采集资格",
    )
    assert isinstance(value, dict)
    _require(
        value.get("snapshot_sha256")
        == auth.digest({k: v for k, v in value.items() if k != "snapshot_sha256"}),
        "账号名单资格校验失败",
    )
    _require(
        active["profile_id"] == INTEGRATED_PROFILE
        and value["source_active"]["profile_id"] == INTEGRATED_PROFILE
        and value["target_roster"]
        == {
            "id": active["roster_snapshot_id"],
            "members_sha256": active["roster_members_sha256"],
        }
        and value["effective_at"] == active["effective_at"]
        and parse_time(value["frozen_at"]) < parse_time(active["effective_at"]),
        "账号名单资格范围不一致",
    )
    _require(
        bool(value["operations"])
        and set(value["operations"]) <= release.CONTINUITY_OPERATIONS
        and set(value["operations"]) == set(value["source_gates"]),
        "账号名单采集操作范围不完整",
    )
    return value


def frozen_operation(active: Mapping[str, Any], operation: str) -> dict[str, Any]:
    key = (
        METADATA_KEY
        if METADATA_KEY in active.get("metadata", {})
        else successor.METADATA_KEY
    )
    return dict(active["metadata"][key]["operations"][operation])


def operation_budget(
    active: Mapping[str, Any], operation: str
) -> dict[str, Any] | None:
    if METADATA_KEY not in active.get("metadata", {}):
        return None
    return dict(_shape(active)["source_gates"][operation]["budget"])


def _origin_runtime(evidence: Mapping[str, Any]) -> dict[str, Any]:
    code = evidence.get("code_successor")
    return dict(code["origin_runtime_bindings"] if code else successor._runtime(evidence))


def _operator(qualification: Mapping[str, Any]) -> bool:
    return qualification.get("qualification_kind") == "operator_authorized"


def _verify_qualification(connection: sqlite3.Connection, qualification: Mapping[str, Any], *,
                          at: str, deployment: Mapping[str, Any] | None = None) -> None:
    if _operator(qualification):
        from .capture_operator_release import validate_frozen
        validate_frozen(connection, qualification, at=at, require_unexpired=False,
                        portable_deployment=deployment)
    else:
        release.validate_frozen_operation_qualification(connection, qualification, at=at)


def _ordinary_pair(
    connection: sqlite3.Connection,
    *,
    operation: str,
    bindings: Mapping[str, Any],
    at: str,
    frozen: Mapping[str, Any] | None = None,
) -> dict[str, Any]:
    if frozen is None:
        gate = connection.execute(
            "SELECT * FROM capture_paid_send_gate_events WHERE provider='tikhub' AND operation=? AND recorded_at<=? ORDER BY id DESC LIMIT 1",
            (operation, utc(at)),
        ).fetchone()
        ready = connection.execute(
            "SELECT * FROM provider_readiness_receipts WHERE provider='tikhub' AND operation=? AND created_at<=? ORDER BY id DESC LIMIT 1",
            (operation, utc(at)),
        ).fetchone()
    else:
        gate = connection.execute(
            "SELECT * FROM capture_paid_send_gate_events WHERE id=?",
            (frozen["gate_id"],),
        ).fetchone()
        ready = connection.execute(
            "SELECT * FROM provider_readiness_receipts WHERE id=?",
            (frozen["readiness_id"],),
        ).fetchone()
    _require(gate is not None and ready is not None, "采集开关或就绪凭据缺失")
    assert gate is not None and ready is not None
    payload, evidence = (
        json.loads(gate["evidence_json"]),
        json.loads(ready["evidence_json"]),
    )
    scope = auth.scope_hash(
        runtime_bindings=bindings, provider="tikhub", operation=operation
    )
    _require(
        gate["event_sha256"] == auth.digest({key: gate[key] for key in _GATE_KEYS})
        and ready["receipt_sha256"]
        == auth.digest({key: ready[key] for key in _READY_KEYS})
        and gate["provider"] == ready["provider"] == "tikhub"
        and gate["operation"]
        == ready["operation"]
        == payload.get("operation")
        == operation
        and gate["state"] == "open"
        and ready["status"] == "ready"
        and payload.get("contract") == auth.CONTRACT
        and evidence.get("contract") == auth.READINESS_CONTRACT
        and payload.get("bindings") == evidence.get("bindings") == dict(bindings)
        and payload.get("scope_hash") == evidence.get("scope_hash") == scope
        and evidence.get("qualification") in {"qualified", "operator_authorized"}
        and payload.get("readiness_receipt_id") == ready["id"]
        and payload.get("readiness_receipt_sha256") == ready["receipt_sha256"]
        and evidence.get("continuity_permit_sha256")
        == bindings["continuity_permit_sha256"]
        and evidence.get("transport_manifest_sha256")
        == payload.get("transport_manifest_sha256")
        and isinstance(payload.get("transport_manifest_sha256"), str)
        and len(payload["transport_manifest_sha256"]) == 64,
        "采集凭据损坏、暂停或尚未具备普通采集资格",
    )
    _require(
        utc(payload["issued_at"]) == utc(gate["recorded_at"])
        and parse_time(gate["recorded_at"])
        <= parse_time(at)
        < parse_time(payload["expires_at"])
        and parse_time(ready["created_at"])
        <= parse_time(at)
        < parse_time(ready["expires_at"]),
        "现有采集资格已过期",
    )
    budget = payload.get("budget", {})
    bucket = (
        "discovery" if operation in provider_budget.DISCOVERY_OPERATIONS else "metrics"
    )
    _require(
        type(budget.get("total_microusd")) is int
        and 0 < budget["total_microusd"] <= provider_budget.AUTOMATIC_MICROUSD
        and type(budget.get("bucket_microusd")) is int
        and 0
        < budget["bucket_microusd"]
        <= provider_budget.BUDGET_BUCKET_MICROUSD[bucket]
        and budget.get("bucket") == bucket,
        "采集预算凭据不合法",
    )
    value = {
        "gate_id": gate["id"],
        "gate_sha256": gate["event_sha256"],
        "readiness_id": ready["id"],
        "readiness_sha256": ready["receipt_sha256"],
        "bindings": dict(bindings),
        "budget": budget,
        "expires_at": min(utc(payload["expires_at"]), utc(ready["expires_at"])),
    }
    if evidence["qualification"] == "operator_authorized":
        _require(
            payload.get("release_decision_sha256") == evidence.get("release_decision_sha256")
            and isinstance(payload.get("release_decision_sha256"), str)
            and len(payload["release_decision_sha256"]) == 64
            and type(payload.get("release_event_id")) is int
            and payload["release_event_id"] == evidence.get("release_event_id")
            and all(body.get("transport_qualification") == "not_verified"
                    and body.get("business_e2e") == "deferred_by_user" for body in (payload, evidence)),
            "人工采集授权凭据不完整",
        )
        value["operator_issuance"] = {key: payload[key] for key in ("release_decision_sha256", "release_event_id")}
    _require(frozen is None or value == dict(frozen), "冻结的采集开关或预算已改变")
    return value


def _source_pair_continuity(connection: sqlite3.Connection, *, operation: str,
                            frozen: Mapping[str, Any], at: str,
                            before: bool = False) -> dict[str, Any]:
    """Accept only uninterrupted renewals of the same operator authority/budget.

    Check every intervening gate AND readiness row: a HOLD followed by reopen is
    not a renewal and cannot silently revive an already prepared roster.
    """
    comparison = "<" if before else "<="
    gates = connection.execute(
        f"SELECT * FROM capture_paid_send_gate_events WHERE provider='tikhub' AND operation=? AND id>=? AND recorded_at{comparison}? ORDER BY id",
        (operation, frozen["gate_id"], utc(at)),
    ).fetchall()
    ready = connection.execute(
        f"SELECT id FROM provider_readiness_receipts WHERE provider='tikhub' AND operation=? AND id>=? AND created_at{comparison}? ORDER BY id",
        (operation, frozen["readiness_id"], utc(at)),
    ).fetchall()
    _require(bool(gates) and bool(ready), "首次签发前采集凭据缺失")
    linked = []
    current: dict[str, Any] = {}
    for gate in gates:
        payload = json.loads(gate["evidence_json"])
        reference = {"gate_id": gate["id"], "readiness_id": payload.get("readiness_receipt_id")}
        # Frozen-reference equality is checked below; at issuance even a later
        # expired operator gate must still be a valid original 24-hour pair.
        current = _ordinary_pair(connection, operation=operation, bindings=frozen["bindings"],
                                 at=gate["recorded_at"])
        _require(current["gate_id"] == reference["gate_id"]
                 and current["readiness_id"] == reference["readiness_id"],
                 "首次签发前采集凭据时间或关联已改变")
        _require(current["bindings"] == frozen["bindings"]
                 and current["budget"] == frozen["budget"]
                 and current.get("operator_issuance") == frozen.get("operator_issuance"),
                 "首次签发前采集授权或预算已改变")
        if "operator_issuance" not in frozen:
            _require(current == dict(frozen), "名单生效前采集资格已改变")
        else:
            _require((parse_time(payload["expires_at"]) - parse_time(payload["issued_at"])).total_seconds() == 86400,
                     "人工采集开关期限已改变")
        linked.append(current["readiness_id"])
    _require(linked == [row[0] for row in ready], "首次签发前就绪凭据被暂停或替换")
    if "operator_issuance" not in frozen:
        _require(parse_time(at) < parse_time(current["expires_at"]), "采集资格不足以覆盖生效时间")
    return current


def activation_eligibility(
    connection: sqlite3.Connection, active: Mapping[str, Any]
) -> dict[str, Any]:
    """Non-recursive pre-effective check, scoped only to prepared roster rows."""
    result = {
        "contract_version": ELIGIBILITY_CONTRACT,
        "required": True,
        "eligible": False,
        "activation_id": active["activation_id"],
    }
    try:
        value = _shape(active)
        source = activation_by_id(connection, value["source_active"]["activation_id"])
        _require(
            source.get("cancellation") is None
            and successor._active(source) == value["source_active"]
            and source["activation_id"] < active["activation_id"],
            "名单来源已改变或取消",
        )
        rows = connection.execute(
            "SELECT * FROM pipeline_paid_drain_events WHERE drain_id=? ORDER BY id",
            (active["metadata"]["drain_id"],),
        ).fetchall()
        _require(
            [row["event_type"] for row in rows] == ["start", "sealed", "release"],
            "名单启用凭据不完整",
        )
        start, sealed, released = rows
        for index, row in enumerate(rows):
            record = {
                key: row[key]
                for key in (
                    "drain_id",
                    "target_activation_id",
                    "sequence",
                    "event_type",
                    "previous_event_id",
                    "previous_event_hash",
                    "contract_version",
                    "created_at",
                )
            }
            record["payload"] = json.loads(row["payload_json"])
            _require(
                row["event_hash"] == auth.digest(record)
                and row["contract_version"] == "pipeline-paid-drain-v2"
                and row["bridge_run_id"] is None
                and row["target_activation_id"] == active["activation_id"]
                and row["sequence"] == index + 1
                and parse_time(value["frozen_at"])
                <= parse_time(row["created_at"])
                < parse_time(active["effective_at"])
                and (
                    not index
                    or (
                        row["previous_event_id"] == rows[index - 1]["id"]
                        and row["previous_event_hash"] == rows[index - 1]["event_hash"]
                    )
                ),
                "名单启用凭据损坏",
            )
        binding = json.loads(start["payload_json"])["binding"]
        sealed_payload, released_payload = (
            json.loads(sealed["payload_json"]),
            json.loads(released["payload_json"]),
        )
        verification = sealed_payload.get("verification", {})
        _require(
            binding.get("source_activation_id") == source["activation_id"]
            and binding.get("target_activation_id") == active["activation_id"]
            and binding.get("planned_effective_at") == active["effective_at"]
            and binding.get("build_receipt_sha256")
            == active["build_receipt_sha256"]
            == value["runtime_bindings"]["build_sha256"]
            and binding.get("runtime_root_receipt_sha256")
            == value["runtime_bindings"]["runtime_sha256"]
            and verification.get("nonblocking") is True
            and verification.get("source_activation_id") == source["activation_id"]
            and verification.get("target_activation_id") == active["activation_id"]
            and sealed_payload.get("start_event_id") == start["id"]
            and sealed_payload.get("start_event_hash") == start["event_hash"]
            and released_payload.get("sealed_event_id") == sealed["id"]
            and released_payload.get("sealed_event_hash") == sealed["event_hash"],
            "名单启用来源绑定不一致",
        )
        # A hold/changed source release before midnight cannot be hidden by a
        # previously completed future permit. Later holds never rewind history.
        last = connection.execute(
            "SELECT event_type FROM pipeline_paid_drain_events WHERE created_at<? ORDER BY id DESC LIMIT 1",
            (active["effective_at"],),
        ).fetchone()
        prior = connection.execute(
            "SELECT id,event_hash FROM pipeline_paid_drain_events WHERE target_activation_id=? AND event_type='release' AND created_at<? ORDER BY id DESC LIMIT 1",
            (source["activation_id"], active["effective_at"]),
        ).fetchone()
        _require(
            last is not None
            and last[0] == "release"
            and prior is not None
            and dict(prior)
            == {
                "id": value["source_release"]["event_id"],
                "event_hash": value["source_release"]["event_hash"],
            },
            "名单生效前采集已暂停或来源放行已改变",
        )
        for operation, frozen in value["source_gates"].items():
            _source_pair_continuity(connection, operation=operation, frozen=frozen,
                                    at=active["effective_at"], before=True)
            if not _operator(value["operations"][operation]):
                _require(parse_time(active["effective_at"]) < parse_time(value["operations"][operation]["expires_at"]),
                         "采集资格不足以覆盖生效时间")
        _verify_route_predecessors(connection, value, before=active["effective_at"])
        return {
            **result,
            "eligible": True,
            "release_event_id": released["id"],
            "release_event_hash": released["event_hash"],
            "sealed_event_id": sealed["id"],
        }
    except (auth.AuthorizationError, KeyError, TypeError, ValueError):
        return result


def validate_installed_successor(
    connection: sqlite3.Connection,
    *,
    source_deployment: Mapping[str, Any],
    current_active: Mapping[str, Any],
    runtime_bindings: Mapping[str, Any],
    manifest: Mapping[str, Any],
    at: str,
    portable: bool = False,
    execution_runtime_bindings: Mapping[str, Any] | None = None,
) -> dict[str, Any]:
    """Verify each immutable predecessor, terminating at the original switch."""
    active = current_active
    first = _shape(active)
    checkpoint = at
    chain = []
    while METADATA_KEY in active.get("metadata", {}):
        value = _shape(active)
        eligibility = activation_eligibility(connection, active)
        _require(
            eligibility["eligible"] is True and active.get("cancellation") is None,
            "名单没有合法的生效前资格链",
        )
        _require(
            value.get("origin_runtime_bindings", value["runtime_bindings"]) == dict(runtime_bindings)
            and (execution_runtime_bindings is None or value["runtime_bindings"] == dict(execution_runtime_bindings))
            and value["manifest"] == dict(manifest)
            and value["transport_code_sha256"] == release._transport_code()
            and source_deployment.get("status") == "accepted"
            and all(
                source_deployment.get(key) == expected
                for key, expected in value["source_deployment"].items()
            )
            and parse_time(value["frozen_at"]) <= parse_time(checkpoint),
            "名单来源安装、运行环境或传输路径已改变",
        )
        for operation, qualification in value["operations"].items():
            _verify_qualification(connection, qualification, at=value["frozen_at"], deployment=source_deployment)
            pair = value["source_gates"][operation]
            _ordinary_pair(
                connection,
                operation=operation,
                bindings=pair["bindings"],
                at=value["frozen_at"],
                frozen=pair,
            )
            _require(
                all(
                    pair["bindings"][key] == value["source_active"][key]
                    for key in (
                        "activation_id",
                        "profile_id",
                        "roster_snapshot_id",
                        "roster_members_sha256",
                    )
                ),
                "来源采集开关绑定到另一名单",
            )
            _require(all(pair["bindings"][wire] == value["runtime_bindings"][key]
                         for wire, key in (("build_receipt_sha256", "build_sha256"),
                                           ("runtime_root_receipt_sha256", "runtime_sha256"),
                                           ("config_receipt_sha256", "config_sha256"))),
                     "来源采集开关代码版本已改变")
            if _operator(qualification):
                from .capture_operator_release import _bindings
                source_evidence = {"active": value["source_active"], **value["runtime_bindings"],
                                   "manifest": value["manifest"]}
                if value.get("source_code_successor_sha256"):
                    source_evidence["code_successor"] = {"proof_sha256": value["source_code_successor_sha256"]}
                decision = qualification["release_decision"]
                _require(pair.get("operator_issuance") == {
                    "release_decision_sha256": decision["decision_sha256"],
                    "release_event_id": value["source_release"]["event_id"]}
                    and pair["bindings"] == _bindings(source_evidence, decision, operation, value["source_release"]["event_id"]),
                    "名单来源人工采集授权已改变")
        chain.append(value["snapshot_sha256"])
        checkpoint = value["frozen_at"]
        active = activation_by_id(connection, value["source_active"]["activation_id"])
    # Existing validator retains the exact original Mode-B frozen qualification,
    # accepted install and strict START/SEALED/RELEASE proof. No copied permit.
    successor.validate_installed_activation_successor(
        connection,
        source_deployment=source_deployment,
        current_active=active,
        runtime_bindings=runtime_bindings,
        manifest=manifest,
        at=checkpoint,
        # This caller already received the verified deployment. Passing it into
        # historical operator validation avoids re-entering code-plan validation.
        portable=True,
    )
    eligibility = activation_eligibility(connection, current_active)
    state = paid_drain.dispatch_state(connection, at=at)
    _require(
        state.paid_dispatch_open
        and state.activation_id == current_active["activation_id"]
        and state.permit_event_id == eligibility["release_event_id"],
        "当前名单采集尚未放行",
    )
    proof = {
        "contract": successor.SUCCESSOR_CONTRACT,
        "source_snapshot_sha256": first["snapshot_sha256"],
        "source_deployment_sha256": source_deployment["receipt_sha256"],
        "target_active": successor._active(current_active),
        "runtime_bindings": dict(runtime_bindings),
        "manifest_sha256": auth.digest(manifest),
        "target_release_event_id": eligibility["release_event_id"],
        "target_release_event_hash": eligibility["release_event_hash"],
        "operations": {
            operation: {
                "snapshot_sha256": qualification["snapshot_sha256"],
                "expires_at": min(
                    utc(qualification["expires_at"]),
                    first["source_gates"][operation]["expires_at"],
                ),
            }
            for operation, qualification in first["operations"].items()
        },
    }
    operator_sources = [value for value in first["operations"].values() if _operator(value)]
    if operator_sources:
        origin = operator_sources[0]
        decision = origin["release_decision"]
        _require(all(item["release_decision"] == decision for item in operator_sources)
                 and decision == source_deployment.get("release_decision"), "名单人工采集授权来源不一致")
        proof["operator_roster_source"] = {
            "decision_sha256": decision["decision_sha256"],
            "source_active": successor._active(origin["source_evidence"]["active"]),
            "operations": sorted(item["operation"] for item in operator_sources),
            "chain_snapshot_sha256s": chain,
        }
    return {**proof, "successor_sha256": auth.digest(proof)}


def validate_code_plan_roster_successor(
    connection: sqlite3.Connection, *, source_active: Mapping[str, Any],
    source_release: Mapping[str, Any], current_active: Mapping[str, Any],
    source_deployment: Mapping[str, Any], origin_runtime_bindings: Mapping[str, Any],
    runtime_bindings: Mapping[str, Any], manifest: Mapping[str, Any],
    operations: Sequence[str], at: str, portable: bool = False,
) -> dict[str, Any]:
    """Bridge one sealed code plan to a real, immutable later account roster.

    Never resolves installed evidence or another code plan; callers must verify
    their actual successful code decision and private deployment first.
    """
    _require(successor._active(current_active) == successor._active(activation_at(connection, at) or {}),
             "代码授权目标不是当前账号名单")
    current = current_active
    chain = []
    boundary = None
    while successor._active(current) != dict(source_active):
        value = _shape(current)
        _require(type(value["source_active"]["activation_id"]) is int
                 and value["source_active"]["activation_id"] < current["activation_id"],
                 "账号名单前代关联不合法")
        _require(set(value["operations"]) <= set(operations), "账号名单扩大了代码授权操作范围")
        _require(value["runtime_bindings"] == dict(runtime_bindings), "账号名单来自另一代码版本")
        _require(all(_operator(item) for item in value["operations"].values()),
                 "代码后继不能冒用其他采集资格")
        chain.append(value["snapshot_sha256"])
        boundary = value["source_release"]
        current = activation_by_id(connection, value["source_active"]["activation_id"])
    _require(bool(chain) and boundary == {"event_id": source_release["id"], "event_hash": source_release["event_hash"]},
             "账号名单未连接到原代码放行记录")
    proof = validate_installed_successor(connection, source_deployment=source_deployment,
        current_active=current_active, runtime_bindings=origin_runtime_bindings, manifest=manifest,
        at=at, portable=portable)
    operator_source = proof.get("operator_roster_source", {})
    _require(operator_source.get("decision_sha256") == source_deployment["release_decision"]["decision_sha256"],
             "代码后继人工授权决定已改变")
    result = {"contract": CODE_SUCCESSOR_CONTRACT, "source_active": dict(source_active),
              "source_release": dict(source_release), "target_active": successor._active(current_active),
              "target_release": {"id": proof["target_release_event_id"], "event_hash": proof["target_release_event_hash"]},
              "decision_sha256": operator_source["decision_sha256"], "operations": sorted(proof["operations"]),
              "chain_snapshot_sha256s": chain}
    return {**result, "proof_sha256": auth.digest(result)}


def _admission_members(
    connection: sqlite3.Connection,
    snapshot_id: int,
    source_snapshot_id: int,
    account_id: int | None,
    additional_identity_ids: set[int] | None = None,
) -> list[dict[str, Any]]:
    rows = connection.execute(
        "SELECT i.account_id,i.id identity_id,i.platform,i.uid FROM account_roster_members m JOIN account_platform_identities i ON i.id=m.account_identity_id JOIN accounts a ON a.id=i.account_id WHERE m.snapshot_id=? AND a.enabled=1 ORDER BY i.id",
        (snapshot_id,),
    ).fetchall()
    old = {
        row[0]
        for row in connection.execute(
            "SELECT account_identity_id FROM account_roster_members WHERE snapshot_id=?",
            (source_snapshot_id,),
        )
    }
    members = [
        dict(row)
        for row in rows
        if (
            row["account_id"] == account_id
            if account_id is not None
            else row["identity_id"] not in old
        )
        or row["identity_id"] in (additional_identity_ids or set())
    ]
    _require(
        account_id is None
        or sum(member["account_id"] == account_id for member in members) == 1,
        "新增或恢复账号未在目标启用名单中",
    )
    from .providers import _valid_douyin_sec_user_id

    for member in members:
        _require(
            member["platform"] in _PLATFORM_OPERATIONS and bool(member["uid"]),
            "该账号尚未具备有效平台身份",
        )
        if member["platform"] == "douyin":
            ref = connection.execute(
                "SELECT reference_value FROM account_provider_references WHERE account_identity_id=? AND provider='TikHub' COLLATE NOCASE AND reference_kind='sec_user_id'",
                (member["identity_id"],),
            ).fetchone()
            _require(
                ref is not None and _valid_douyin_sec_user_id(str(ref[0])),
                "抖音主页身份资料不完整，请重新识别主页后重试",
            )
    return members


def _content_routes(
    connection: sqlite3.Connection,
    account_id: int,
    operation: str,
    before: str | None = None,
) -> list[dict[str, Any]]:
    cutoff = " AND julianday(r.recorded_at)<julianday(?)" if before else ""
    successor_cutoff = " AND julianday(n.recorded_at)<julianday(?)" if before else ""
    args: tuple[Any, ...] = (
        (account_id, operation, before, before) if before else (account_id, operation)
    )
    return [
        dict(row)
        for row in connection.execute(
            "SELECT r.* FROM capture_route_assignments r JOIN content_items c ON c.id=r.content_id WHERE r.scope_type='content' AND c.account_id=? AND r.operation=?"
            + cutoff
            + " AND NOT EXISTS (SELECT 1 FROM capture_route_assignments n WHERE n.scope_type=r.scope_type AND n.scope_key=r.scope_key AND n.operation=r.operation AND n.generation>r.generation"
            + successor_cutoff
            + ") ORDER BY r.id",
            args,
        )
    ]


def _valid_route(row: Mapping[str, Any], at: str) -> None:
    _require(
        row["assignment_sha256"]
        == auth.digest(
            {k: v for k, v in row.items() if k not in {"id", "assignment_sha256"}}
        )
        and row["provider"] == "tikhub"
        and row["route"] == "integrated"
        and row["mode"] == "active"
        and parse_time(row["effective_at"]) <= parse_time(at),
        "账号或作品已有独立采集路由限制，请先完成路由核查",
    )


def _route_preparation(
    connection: sqlite3.Connection,
    members: list[dict[str, Any]],
    operations: set[str],
    at: str,
) -> list[dict[str, Any]]:
    """New members need account routes: the executor has no platform fallback."""
    result = []
    for member in members:
        for operation in sorted(
            op for op in operations if op.startswith(member["platform"] + "_")
        ):
            row = connection.execute(
                "SELECT * FROM capture_route_assignments WHERE scope_type='account' AND scope_key=? AND operation=? ORDER BY generation DESC LIMIT 1",
                (str(member["account_id"]), operation),
            ).fetchone()
            previous = dict(row) if row is not None else None
            if previous is not None:
                _valid_route(previous, at)
                _require(
                    previous["account_id"] == member["account_id"]
                    and previous["content_id"] is None,
                    "账号路由绑定已改变",
                )
            content = _content_routes(connection, member["account_id"], operation)
            for override in content:
                _valid_route(override, at)
            result.append(
                {
                    "account_id": member["account_id"],
                    "identity_id": member["identity_id"],
                    "operation": operation,
                    "previous": previous,
                    "content_predecessors": content,
                }
            )
    return result


def _verify_route_predecessors(
    connection: sqlite3.Connection,
    value: Mapping[str, Any],
    *,
    before: str | None = None,
) -> None:
    for route in value["routes"]:
        cutoff = " AND julianday(recorded_at)<julianday(?)" if before else ""
        args: tuple[Any, ...] = (str(route["account_id"]), route["operation"])
        if before:
            args += (before,)
        row = connection.execute(
            "SELECT * FROM capture_route_assignments WHERE scope_type='account' AND scope_key=? AND operation=?"
            + cutoff
            + " ORDER BY generation DESC LIMIT 1",
            args,
        ).fetchone()
        _require(
            (dict(row) if row is not None else None) == route["previous"]
            and _content_routes(
                connection, route["account_id"], route["operation"], before
            )
            == route["content_predecessors"],
            "账号或作品采集路由在安排后已改变，不能自动覆盖",
        )


def _activate_routes(
    connection: sqlite3.Connection, value: Mapping[str, Any], at: str
) -> None:
    for route in value["routes"]:
        if route["previous"] is not None:
            continue
        account = connection.execute(
            "SELECT enabled FROM accounts WHERE id=?", (route["account_id"],)
        ).fetchone()
        # Pause is immediate and independent; never create a route for an account
        # that was paused while this midnight activation was waiting.
        if account is None or not account[0]:
            continue
        capture_planning.assign_route(
            connection,
            scope_type="account",
            scope_key=str(route["account_id"]),
            provider="tikhub",
            operation=route["operation"],
            expected_generation=0,
            route="integrated",
            mode="active",
            account_id=route["account_id"],
            effective_at=at,
            recorded_at=at,
        )


def _prepare(
    connection: sqlite3.Connection,
    *,
    roster_snapshot_id: int,
    actor: str,
    reason: str,
    timestamp: str,
    account_id: int | None,
) -> dict[str, Any]:
    from .profile_control import CONTROL_CONTRACT, _next_midnight

    _require(
        connection.execute("PRAGMA user_version").fetchone()[0] == 20,
        "当前数据库不支持名单资格继承",
    )
    evidence = release._installed_evidence(connection, at=timestamp)
    source = evidence["active"]
    _require(
        source["profile_id"] == INTEGRATED_PROFILE,
        "当前采集模式尚不支持新增账号自动启用，请先完成采集模式升级",
    )
    # Also rejects unqualified integrated sources even if an installation reader
    # happens to accept activation equality at its own deployment boundary.
    successor.validate_installed_activation_successor(
        connection,
        source_deployment=evidence["deployment"],
        current_active=source,
        runtime_bindings=_origin_runtime(evidence),
        manifest=evidence["manifest"],
        at=timestamp,
    )
    state = paid_drain.dispatch_state(connection, at=timestamp)
    _require(
        state.paid_dispatch_open and state.activation_id == source["activation_id"],
        "当前采集处于暂停放行状态",
    )
    snapshot = account_roster.snapshot_by_id(connection, roster_snapshot_id)
    _require(snapshot["source_family"] == "system", "新增账号必须使用系统账号名单")
    effective = _next_midnight(timestamp)
    scheduled = connection.execute(
        "SELECT id FROM acquisition_profile_activations a WHERE effective_at=? AND NOT EXISTS (SELECT 1 FROM activation_cancellations c WHERE c.activation_id=a.id)",
        (effective,),
    ).fetchone()
    carry = set()
    if scheduled is not None:
        pending = activation_by_id(connection, scheduled[0])
        pending_value = _shape(pending)
        _require(
            pending_value["source_active"] == successor._active(source)
            and activation_eligibility(connection, pending)["eligible"],
            "明日零点已有其他名单切换或原设置已失效",
        )
        # Replacing tonight's complete snapshot must retain previously admitted
        # members (A, then A+B), while leaving unrelated active overrides alone.
        carry = {route["identity_id"] for route in pending_value["routes"]}
    members = _admission_members(
        connection, roster_snapshot_id, source["roster_snapshot_id"], account_id, carry
    )
    platforms = {member["platform"] for member in members}
    required = (
        set().union(*(_PLATFORM_OPERATIONS[platform] for platform in platforms))
        if platforms
        else set()
    )
    operations, source_gates = {}, {}
    for operation in sorted(release.CONTINUITY_OPERATIONS):
        latest = connection.execute(
            "SELECT * FROM capture_paid_send_gate_events WHERE provider='tikhub' AND operation=? AND recorded_at<=? ORDER BY id DESC LIMIT 1",
            (operation, utc(timestamp)),
        ).fetchone()
        is_current = (
            latest is not None
            and json.loads(latest["evidence_json"])
            .get("bindings", {})
            .get("activation_id")
            == source["activation_id"]
        )
        if operation not in required and not (is_current and latest["state"] == "open"):
            continue
        bindings = release.current_runtime_bindings(connection, operation, timestamp)
        pair = _ordinary_pair(
            connection, operation=operation, bindings=bindings, at=timestamp
        )
        from .capture_operator_release import authority as operator_authority
        operator_value = operator_authority(connection, evidence=evidence, operation=operation, at=timestamp)
        native = None if operator_value is not None else release._native_authority(
            connection, operation=operation, at=timestamp, evidence=evidence
        )
        qualification = (
            release.snapshot_operation_qualification(
                connection, operation=operation, at=timestamp
            )
            if native is not None
            else frozen_operation(source, operation)
        )
        _verify_qualification(connection, qualification, at=timestamp, deployment=evidence["deployment"])
        if operator_value is not None:
            _require(_operator(qualification) and pair.get("operator_issuance") == {
                "release_decision_sha256": operator_value["decision"]["decision_sha256"],
                "release_event_id": operator_value["release_event_id"]}, "采集授权来源不一致")
            from .capture_operator_release import _gate_evidence
            gate = connection.execute("SELECT * FROM capture_paid_send_gate_events WHERE id=?", (pair["gate_id"],)).fetchone()
            _gate_evidence(connection, gate, value=operator_value, evidence=evidence, operation=operation)
        else:
            _require("operator_issuance" not in pair and not _operator(qualification), "采集资格不能混用")
            _verify_qualification(connection, qualification, at=effective)
            _require(parse_time(effective) < parse_time(pair["expires_at"])
                     and parse_time(effective) < parse_time(qualification["expires_at"]),
                     "现有采集资格将在明日零点前过期，请完成资格更新后重试")
        operations[operation], source_gates[operation] = qualification, pair
    _require(
        bool(operations) and required <= set(operations), "新增平台尚未具备完整采集资格"
    )
    code_proof = evidence.get("code_successor")
    if code_proof is not None:
        _require(code_proof.get("roster_successor_contract") == CODE_SUCCESSOR_CONTRACT
                 and set(operations) <= set(code_proof["plan_payload"]["operations"])
                 and all(_operator(item) for item in operations.values()),
                 "当前采集版本尚不支持账号名单更新")
    release_row = connection.execute(
        "SELECT event_hash FROM pipeline_paid_drain_events WHERE id=? AND event_type='release'",
        (state.permit_event_id,),
    ).fetchone()
    _require(release_row is not None, "当前采集放行凭据缺失")
    value = {
        "contract": CONTRACT,
        "frozen_at": utc(timestamp),
        "effective_at": effective,
        "source_active": successor._active(source),
        "target_roster": {
            "id": snapshot["id"],
            "members_sha256": snapshot["members_sha256"],
        },
        "runtime_bindings": successor._runtime(evidence),
        "origin_runtime_bindings": _origin_runtime(evidence),
        "source_code_successor_sha256": (evidence.get("code_successor") or {}).get("proof_sha256"),
        "manifest": evidence["manifest"],
        "transport_code_sha256": release._transport_code(),
        "source_deployment": {
            key: evidence["deployment"][key]
            for key in ("deployment_id", "receipt_sha256", "bindings")
        },
        "source_release": {
            "event_id": state.permit_event_id,
            "event_hash": release_row[0],
        },
        "operations": operations,
        "source_gates": source_gates,
        "routes": _route_preparation(connection, members, set(operations), timestamp),
    }
    value["snapshot_sha256"] = auth.digest(value)
    scheduled = connection.execute(
        "SELECT id FROM acquisition_profile_activations a WHERE effective_at=? AND NOT EXISTS (SELECT 1 FROM activation_cancellations c WHERE c.activation_id=a.id)",
        (effective,),
    ).fetchone()
    drain_id = f"account-roster:{uuid4()}"
    metadata = {
        "contract_version": CONTROL_CONTRACT,
        "switch_kind": "same_profile",
        "eligibility_contract": ELIGIBILITY_CONTRACT,
        "drain_id": drain_id,
        METADATA_KEY: value,
    }
    arguments = dict(
        profile_id=INTEGRATED_PROFILE,
        roster_snapshot_id=roster_snapshot_id,
        roster_members_sha256=snapshot["members_sha256"],
        effective_at=effective,
        build_receipt_sha256=evidence["build_sha256"],
        actor=actor,
        reason=reason,
        metadata=metadata,
        created_at=timestamp,
    )
    if scheduled is not None:
        prior = activation_by_id(connection, scheduled[0])
        old = _shape(prior)
        _require(
            old["source_active"] == value["source_active"]
            and activation_eligibility(connection, prior)["eligible"],
            "明日零点已有其他名单切换或原资格已失效",
        )
        if (
            old["target_roster"] == value["target_roster"]
            and old["operations"] == operations
            and old["source_gates"] == source_gates
            and old["routes"] == value["routes"]
        ):
            return {"scheduled": True, "activation": prior, "idempotent": True}
        active = replace_scheduled_activation(
            connection, prior["activation_id"], **arguments
        )
    else:
        active = append_activation(connection, **arguments)
    event = paid_drain.issue_activation_permit_in_transaction(
        connection,
        activation_id=active["activation_id"],
        drain_id=drain_id,
        source_activation_id=source["activation_id"],
        business_day=parse_time(timestamp).astimezone(BEIJING).date().isoformat(),
        planned_effective_at=effective,
        build_receipt_sha256=evidence["build_sha256"],
        runtime_root_receipt_sha256=evidence["runtime_sha256"],
        now=timestamp,
    )
    _require(
        activation_eligibility(connection, active)["eligible"], "名单启用凭据未能完成"
    )
    return {
        "scheduled": True,
        "activation": active,
        "release": {"event_id": event.event_id},
        "idempotent": False,
    }


def schedule_account_roster_capture_in_transaction(
    connection: sqlite3.Connection,
    *,
    roster_snapshot_id: int,
    actor: str,
    reason: str,
    now: str | None = None,
    account_id: int | None = None,
) -> dict[str, Any]:
    """Caller owns account/roster transaction; failures never leave preparation."""
    from .storage import now_utc

    try:
        _require(connection.in_transaction, "名单安排必须与账号保存在同一事务")
        require_current_process_writer_lock(connection)
        connection.execute("SAVEPOINT account_roster_capture_prepare")
        try:
            result = _prepare(
                connection,
                roster_snapshot_id=roster_snapshot_id,
                actor=actor,
                reason=reason,
                timestamp=now or now_utc(),
                account_id=account_id,
            )
        except Exception:
            connection.execute("ROLLBACK TO account_roster_capture_prepare")
            raise
        finally:
            connection.execute("RELEASE account_roster_capture_prepare")
        return result
    except (
        auth.AuthorizationError,
        KeyError,
        TypeError,
        ValueError,
        RuntimeError,
        sqlite3.Error,
    ) as error:
        raise AccountRosterCaptureError(str(error)) from error


def validate_current_account_capture(
    connection: sqlite3.Connection, *, account_id: int, now: str | None = None
) -> dict[str, Any]:
    """Read-only precommit validation for restoration inside the active roster."""
    from .storage import now_utc

    try:
        _require(connection.in_transaction, "恢复账号必须在保存事务内验证")
        require_current_process_writer_lock(connection)
        timestamp = now or now_utc()
        evidence = release._installed_evidence(connection, at=timestamp)
        source = evidence["active"]
        _require(
            source["profile_id"] == INTEGRATED_PROFILE,
            "当前采集模式尚不支持恢复账号自动采集",
        )
        members = _admission_members(
            connection,
            source["roster_snapshot_id"],
            source["roster_snapshot_id"],
            account_id,
        )
        required = _PLATFORM_OPERATIONS[members[0]["platform"]]
        for operation in required:
            bindings = release.current_runtime_bindings(
                connection, operation, timestamp
            )
            pair = _ordinary_pair(connection, operation=operation, bindings=bindings, at=timestamp)
            if "operator_issuance" in pair:
                from .capture_operator_release import authority, _gate_evidence
                value = authority(connection, evidence=evidence, operation=operation, at=timestamp)
                _require(value is not None, "当前账号没有有效的人工采集授权")
                assert value is not None
                gate = connection.execute("SELECT * FROM capture_paid_send_gate_events WHERE id=?", (pair["gate_id"],)).fetchone()
                _gate_evidence(connection, gate, value=value, evidence=evidence, operation=operation)
        routes = _route_preparation(connection, members, required, timestamp)
        _require(
            all(route["previous"] is not None for route in routes),
            "当前账号尚无完整采集路由，不能直接恢复",
        )
        return {
            "validated": True,
            "activation_id": source["activation_id"],
            "account_id": account_id,
            "provider_calls": 0,
        }
    except (
        auth.AuthorizationError,
        KeyError,
        TypeError,
        ValueError,
        RuntimeError,
        sqlite3.Error,
    ) as error:
        raise AccountRosterCaptureError(str(error)) from error


def activate_prepared_roster_capture_in_transaction(
    connection: sqlite3.Connection, *, at: str
) -> dict[str, Any]:
    """Writer tick only: one atomic, idempotent issuance after actual activation."""
    _require(connection.in_transaction, "名单凭据签发必须位于 Writer 事务")
    require_current_process_writer_lock(connection)
    active = activation_at(connection, at)
    if active is None or METADATA_KEY not in active.get("metadata", {}):
        return {"status": "skipped", "provider_calls": 0}
    value = _shape(active)
    already = set()
    # A historical issuance is permanent evidence that this activation was
    # bootstrapped. Never reopen a subsequently held gate or reset native renewal.
    for operation in value["operations"]:
        if _operator(value["operations"][operation]):
            rows = connection.execute(
                "SELECT * FROM provider_readiness_receipts WHERE provider='tikhub' AND operation=? AND reason='user-approved-production-release' AND json_extract(evidence_json,'$.account_roster_snapshot_sha256')=? ORDER BY id",
                (operation, value["snapshot_sha256"]),
            ).fetchall()
            for row in rows:
                payload = json.loads(row["evidence_json"])
                bindings = payload.get("bindings", {})
                _require(all(bindings.get(key) == active[key] for key in ("activation_id", "profile_id", "roster_snapshot_id", "roster_members_sha256"))
                         and payload.get("release_decision_sha256") == value["operations"][operation]["release_decision"]["decision_sha256"],
                         "已签发账号名单授权身份已改变")
                gate = connection.execute(
                    "SELECT * FROM capture_paid_send_gate_events WHERE provider='tikhub' AND operation=? AND reason='user-approved-production-release' AND json_extract(evidence_json,'$.readiness_receipt_id')=?",
                    (operation, row["id"]),
                ).fetchone()
                _require(gate is not None and gate["event_sha256"] == auth.digest({key: gate[key] for key in _GATE_KEYS})
                         and row["receipt_sha256"] == auth.digest({key: row[key] for key in _READY_KEYS}),
                         "已签发账号名单授权凭据损坏")
                gate_payload = json.loads(gate["evidence_json"])
                _require(gate_payload.get("account_roster_snapshot_sha256") == value["snapshot_sha256"]
                         and gate_payload.get("readiness_receipt_sha256") == row["receipt_sha256"]
                         and gate_payload.get("bindings") == bindings
                         and gate_payload.get("release_decision_sha256") == payload["release_decision_sha256"]
                         and gate_payload.get("budget") == value["source_gates"][operation]["budget"],
                         "已签发账号名单授权关联或预算已改变")
                already.add(operation)
                break
            continue
        rows = connection.execute(
            "SELECT * FROM provider_readiness_receipts WHERE provider='tikhub' AND operation=? AND reason='integrated-activation-qualified'",
            (operation,),
        ).fetchall()
        for row in rows:
            payload = json.loads(row["evidence_json"])
            proof = payload.get("activation_successor", {})
            if (
                proof.get("target_active") != successor._active(active)
                or proof.get("source_snapshot_sha256") != value["snapshot_sha256"]
            ):
                continue
            _require(
                row["receipt_sha256"]
                == auth.digest({key: row[key] for key in _READY_KEYS})
                and proof.get("successor_sha256")
                == auth.digest(
                    {k: v for k, v in proof.items() if k != "successor_sha256"}
                ),
                "已签发名单凭据损坏",
            )
            gate = connection.execute(
                "SELECT * FROM capture_paid_send_gate_events WHERE provider='tikhub' AND operation=? AND reason='integrated-activation-qualified' AND json_extract(evidence_json,'$.readiness_receipt_id')=?",
                (operation, row["id"]),
            ).fetchone()
            _require(
                gate is not None
                and gate["event_sha256"]
                == auth.digest({key: gate[key] for key in _GATE_KEYS}),
                "已签发名单开关缺失或损坏",
            )
            already.add(operation)
            break
    if already == set(value["operations"]):
        return {
            "status": "already_issued",
            "activation_id": active["activation_id"],
            "provider_calls": 0,
        }
    _require(not already, "名单采集凭据仅部分签发，需要核查")
    connection.execute("SAVEPOINT account_roster_capture_issue")
    try:
        # A hold can arrive after midnight but before the first Writer tick.
        # Verify every latest source pair before publishing any target pair.
        for operation, frozen in value["source_gates"].items():
            _source_pair_continuity(connection, operation=operation, frozen=frozen, at=at)
        _verify_route_predecessors(connection, value)
        _activate_routes(connection, value, at)
        issued = {
            operation: successor.publish_target_operation_gate(
                connection, operation=operation, at=at
            )
            for operation in sorted(value["operations"])
        }
    except Exception:
        connection.execute("ROLLBACK TO account_roster_capture_issue")
        raise
    finally:
        connection.execute("RELEASE account_roster_capture_issue")
    return {
        "status": "issued",
        "activation_id": active["activation_id"],
        "operations": issued,
        "provider_calls": 0,
    }
