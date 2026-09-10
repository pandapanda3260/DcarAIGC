"""Manual publishing labels and atomic account pause/resume operations.

Publishing frequency is an operator label, never a provider scheduling rule.
The existing main-database operator receipts persist labels without a DDL change.
"""

from __future__ import annotations

import json
import sqlite3
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Callable, Mapping
from uuid import uuid4
from zoneinfo import ZoneInfo

from .account_operating_receipts import (
    AccountOperatingStatusError as AccountOperatingStatusError,
    find_status_request,
    latest_admission_member,
    load_update_frequencies,
    record_status_receipt,
    validate_status_request_id,
)
from .account_states import set_account_enabled_in_transaction
from .account_roster import SYSTEM_SOURCE_FAMILY, latest_family_snapshot
from .system_roster import current_system_members, remove_system_member, upsert_system_members


ACCOUNT_STATUSES = frozenset({"daily", "weekly", "paused"})
ScheduleActivation = Callable[
    [sqlite3.Connection, Mapping[str, Any]], Mapping[str, Any]
]


def account_operating_status(account: Mapping[str, Any]) -> str:
    """Project an account with its optional verified operator frequency."""

    value = dict(account)
    if not bool(value["enabled"]):
        return "paused"
    frequency = value.get("update_frequency")
    return str(frequency) if frequency in {"daily", "weekly"} else "unmarked"


def _uses_fixed_cleanup_scope(
    connection: sqlite3.Connection, identity: Mapping[str, Any], *,
    activation_id: int | None, status: str, at: str,
) -> bool:
    """A status toggle cannot rebuild the cleanup's already authorized roster.

    The installed evidence validates the exact frozen member intersection and
    permits recorded operator pauses. Resuming still passes the ordinary API
    gate/route validation after enabled is changed inside the same transaction.
    """
    if connection.execute("PRAGMA user_version").fetchone()[0] not in {20, 21}:
        return False
    from . import account_cleanup_runtime as cleanup
    from .capture_authorizations import AuthorizationError
    from .profile_activations import activation_at

    active = activation_at(connection, at)
    if active is None or "account_cleanup" not in active.get("metadata", {}):
        return False
    try:
        evidence = cleanup.installed_evidence(connection, at=at, maintenance_only=True)
        if activation_id != evidence["active"]["activation_id"]:
            raise AccountOperatingStatusError("account_cleanup_activation_changed", "当前账号名单已变更，请刷新后重试。")
        capsule = cleanup.private_object(evidence["account_cleanup_generation"]["source_authority"])
        target = {"account_identity_id": identity["id"], "account_id": identity["account_id"],
                  "platform": identity["platform"], "uid": identity["uid"]}
        directory = connection.execute(
            "SELECT identity_status FROM account_directory_rows WHERE account_id=? AND platform=? AND uid=?",
            (identity["account_id"], identity["platform"], identity["uid"]),
        ).fetchone()
        if status != "paused" and (target not in capsule["eligible_members"]
                or directory is None or directory["identity_status"] != "existing_verified"):
            raise AccountOperatingStatusError("account_cleanup_scope_not_admitted", "此账号尚未加入当前采集名单，暂不能恢复采集。")
        # Pausing can only narrow live eligibility, including an account outside
        # this approved set. Never mutate a roster or add a new paid scope.
        return True
    except AuthorizationError as error:
        raise AccountOperatingStatusError("account_cleanup_status_unavailable", "当前账号名单校验未通过，请稍后重试。") from error


def _resume_member(
    connection: sqlite3.Connection, identity: Mapping[str, Any]
) -> dict[str, Any]:
    previous = connection.execute(
        """SELECT m.* FROM account_roster_members m
           JOIN account_roster_snapshots s ON s.id=m.snapshot_id
           WHERE m.account_identity_id=? AND s.source_family='system'
           ORDER BY s.source_captured_at DESC,s.id DESC LIMIT 1""",
        (identity["id"],),
    ).fetchone()
    historical = dict(previous) if previous is not None else {}
    if not historical:
        admission = latest_admission_member(connection, int(identity["account_id"]))
        if admission is not None:
            return admission
    metadata = json.loads(historical.get("metadata_json", "{}"))
    return {
        "platform": identity["platform"],
        "uid": identity["uid"],
        "nickname": metadata.get("nickname") or identity["nickname"],
        "profile_ref": historical.get("profile_ref"),
        "monitoring_status": historical.get("monitoring_status", "unknown"),
        "authorization_status": historical.get("authorization_status", "unknown"),
        "monitoring_started_at": historical.get("monitoring_started_at"),
        "metadata": metadata,
    }


def update_account_operating_status_in_transaction(
    connection: sqlite3.Connection,
    account_id: int,
    value: Mapping[str, Any],
    *,
    raw_root: Path,
    actor: str,
    reason: str,
    activation_id: int | None = None,
    schedule_activation: ScheduleActivation | None = None,
    admission: Mapping[str, Any] | None = None,
) -> dict[str, Any]:
    """Apply operating edits, state event and next-roster change as one unit.

    The caller owns the transaction. The savepoint also rolls back local edits
    when the caller catches an activation failure and continues its transaction.
    """

    from .operations import update_account_in_transaction

    if not connection.in_transaction:
        raise AccountOperatingStatusError(
            "account_status_transaction_required", "账号状态更新需要已有事务"
        )
    supplied = dict(value)
    status = supplied.pop("account_status", None)
    supplied_request_id = supplied.pop("status_request_id", None)
    if "account_status" in value and (not isinstance(status, str) or status not in ACCOUNT_STATUSES):
        raise AccountOperatingStatusError(
            "account_status_invalid", "账号状态必须是日更、周更或暂停"
        )
    if status is not None:
        if not actor.strip() or not reason.strip():
            raise AccountOperatingStatusError(
                "account_status_reason_required", "账号状态更新需要操作人和原因"
            )
        if "enabled" in supplied:
            if type(supplied["enabled"]) is not bool or supplied["enabled"] != (status != "paused"):
                raise AccountOperatingStatusError(
                    "account_status_conflict", "账号状态与采集开关冲突"
                )
            supplied.pop("enabled")
    elif "status_request_id" in value:
        raise AccountOperatingStatusError(
            "account_status_request_invalid", "账号状态请求编号需要同时提供账号状态"
        )
    request_id = (
        validate_status_request_id(supplied_request_id)
        if "status_request_id" in value else str(uuid4())
    )
    request = {"account_status": status, "fields": supplied}
    if admission is not None:
        request["admission"] = dict(admission)

    connection.execute("SAVEPOINT account_operating_status")
    try:
        existing = find_status_request(connection, request_id=request_id) if status is not None else None
        if existing is not None:
            payload = existing["payload"]
            if (
                payload["account_id"] != account_id
                or payload["request"] != request
                or payload["actor"] != actor
                or payload["reason"] != reason
            ):
                raise AccountOperatingStatusError(
                    "account_status_request_conflict", "该请求编号已用于不同的账号修改，请刷新后重试"
                )
            result = dict(payload["result"])
            result["status_replayed"] = True
            connection.execute("RELEASE account_operating_status")
            return result
        before = connection.execute("SELECT * FROM accounts WHERE id=?", (account_id,)).fetchone()
        if before is None:
            raise AccountOperatingStatusError("account_not_found", "账号不存在")
        previous_frequency = load_update_frequencies(connection, [account_id])[account_id]
        frequency = status if status in {"daily", "weekly"} else previous_frequency
        result = update_account_in_transaction(
            connection, account_id, supplied, actor=actor, reason=reason,
            activation_id=activation_id,
        )
        roster_change: Mapping[str, Any] | None = None
        catalog_policy: Mapping[str, Any] | None = None
        if status is not None:
            identity = connection.execute(
                "SELECT * FROM account_platform_identities WHERE account_id=?", (account_id,)
            ).fetchone()
            if identity is None:
                raise AccountOperatingStatusError(
                    "account_identity_not_found", "账号缺少平台身份，不能更新账号状态"
                )
            timestamp = datetime.now(timezone.utc).isoformat(timespec="microseconds").replace("+00:00", "Z")
            from .account_catalog_capture import installed_policy
            catalog_policy = installed_policy(connection, at=timestamp)
            if catalog_policy is not None:
                from .account_directory import update_directory_operating_fields
                directory = connection.execute(
                    "SELECT * FROM account_directory_rows WHERE account_id=?", (account_id,),
                ).fetchone()
                if (directory is None or directory["identity_status"] != "existing_verified"
                        or directory["platform"] != identity["platform"] or directory["uid"] != identity["uid"]):
                    raise AccountOperatingStatusError(
                        "account_directory_identity_changed", "账号身份尚未核验，请先完善主页信息。"
                    )
                # Saving an operating label does not require a ready locator,
                # a provider gate or membership in the historical roster.
                update_directory_operating_fields(connection, account_id, {**supplied, "account_status": status})
            fixed_cleanup_scope = catalog_policy is None and _uses_fixed_cleanup_scope(
                connection, identity, activation_id=activation_id, status=status, at=timestamp,
            )
            set_account_enabled_in_transaction(
                connection, int(identity["id"]), enabled=status != "paused",
                effective_at=timestamp, created_at=timestamp, actor=actor, reason=reason,
                activation_id=activation_id,
                metadata={
                    "operation": "update_account_status", "account_status": status,
                    "previous_update_frequency": previous_frequency,
                },
            )
            # Changing daily <-> weekly on an enabled account only changes its
            # manual label. It must not silently repair/rebuild a roster.
            label_only = admission is None and bool(before["enabled"]) and previous_frequency in {"daily", "weekly"} and status != "paused"
            if not label_only and not fixed_cleanup_scope and catalog_policy is None:
                members = current_system_members(connection)
                is_member = any(
                    member["platform"] == identity["platform"] and member["uid"] == identity["uid"]
                    for member in members
                )
                if status == "paused" and is_member:
                    roster_change = remove_system_member(
                        connection, account_id, raw_root=raw_root, actor=actor, reason=reason
                    )
                elif status != "paused" and not is_member:
                    member = _resume_member(connection, identity)
                    if admission is not None:
                        incoming = dict(admission["member"])
                        member["metadata"] = {
                            **dict(member.get("metadata") or {}),
                            **dict(incoming.pop("metadata", {}) or {}),
                        }
                        member.update({key: value for key, value in incoming.items() if value not in (None, "")})
                    roster_change = upsert_system_members(
                        connection, [member],
                        raw_root=raw_root, actor=actor, reason=reason,
                    )
                    if roster_change.get("status") != "accepted":
                        raise AccountOperatingStatusError(
                            "account_resume_identity_invalid", "账号身份不完整，无法恢复到系统名单"
                        )
                elif status != "paused" and (admission is not None or not bool(before["enabled"])):
                    active_snapshot = admission.get("active_snapshot_id") if admission is not None else None
                    if admission is None and activation_id is not None:
                        active_activation = connection.execute(
                            "SELECT roster_snapshot_id FROM acquisition_profile_activations WHERE id=?",
                            (activation_id,),
                        ).fetchone()
                        if active_activation is not None:
                            active_snapshot = active_activation["roster_snapshot_id"]
                    in_active = active_snapshot is not None and connection.execute(
                        "SELECT 1 FROM account_roster_members WHERE snapshot_id=? AND account_identity_id=?",
                        (active_snapshot, identity["id"]),
                    ).fetchone() is not None
                    if not in_active:
                        latest = latest_family_snapshot(connection, SYSTEM_SOURCE_FAMILY)
                        assert latest is not None
                        # Accepted membership can predate its activation. Both
                        # direct status restores and creation must schedule it.
                        roster_change = {"status": "accepted", "snapshot_id": int(latest["id"]),
                                         "activation_status": "pending_activation", "reused_snapshot": True}
            if roster_change is not None:
                if status == "paused":
                    # enabled=0 is the immediate capture/statistics gate. Do not
                    # let missing/expired activation qualification undo a pause.
                    roster_change = {**roster_change, "activation_status": "not_scheduled",
                                     "schedule_skipped_reason": "account_paused"}
                else:
                    if schedule_activation is not None:
                        roster_change = schedule_activation(
                            connection, {**roster_change, "account_id": account_id}
                        )
                    if not isinstance(roster_change, Mapping) or roster_change.get("activation_status") not in {"scheduled", "active"}:
                        raise AccountOperatingStatusError(
                            "account_activation_not_scheduled", "名单激活尚未安排成功，账号本次没有保存，请稍后重试。"
                        )
                    if roster_change["activation_status"] == "scheduled":
                        try:
                            scheduled_time = datetime.fromisoformat(
                                str(roster_change.get("scheduled_effective_at")).replace("Z", "+00:00")
                            )
                            if scheduled_time.utcoffset() is None or scheduled_time <= datetime.now(timezone.utc):
                                raise ValueError("activation time is not an aware future instant")
                        except (TypeError, ValueError) as error:
                            raise AccountOperatingStatusError(
                                "account_activation_time_invalid", "名单激活时间无效，账号本次没有保存，请稍后重试。"
                            ) from error
        after = connection.execute("SELECT * FROM accounts WHERE id=?", (account_id,)).fetchone()
        assert after is not None
        result = {
            **result,
            "account_status": account_operating_status({**dict(after), "update_frequency": frequency}),
            "update_frequency": frequency,
            "enabled": bool(after["enabled"]),
        }
        if status == "paused":
            result["activation_status"] = "disabled"
            result["message"] = "账号已暂停，立即停用采集并退出新统计；历史数据保留。"
        elif status is not None:
            result["message"] = "账号状态已保存；日更、周更仅为人工运营标记。"
        if roster_change is not None:
            result["roster_change"] = dict(roster_change)
            if status == "paused":
                result["message"] += " 系统名单移除已记录，未安排新的名单激活。"
            else:
                result["activation_status"] = roster_change["activation_status"]
                for field in ("snapshot_id", "scheduled_activation_id", "scheduled_effective_at"):
                    if field in roster_change:
                        result[field] = roster_change[field]
                if roster_change["activation_status"] == "scheduled":
                    beijing_time = scheduled_time.astimezone(ZoneInfo("Asia/Shanghai")).strftime("%Y-%m-%d %H:%M")
                    result["message"] += f" 已安排于北京时间 {beijing_time} 激活名单。"
                else:
                    result["message"] += " 系统名单已生效。"
        if admission is not None:
            result.update({
                "account_id": account_id,
                "creation_action": admission["action"],
                "request_id": request_id,
                "platform": identity["platform"],
                "uid": identity["uid"],
                "action": admission["action"],
            })
            active_snapshot_id = admission.get("active_snapshot_id")
            active_member = catalog_policy is None and active_snapshot_id is not None and connection.execute(
                "SELECT 1 FROM account_roster_members WHERE snapshot_id=? AND account_identity_id=?",
                (active_snapshot_id, identity["id"]),
            ).fetchone() is not None
            result["activation_status"] = (
                "disabled" if status == "paused" else
                str(roster_change.get("activation_status", "pending_activation"))
                if roster_change is not None else
                "active" if active_member else "pending_activation"
            )
            if roster_change is not None:
                for field in ("snapshot_id", "scheduled_activation_id", "scheduled_effective_at"):
                    if field in roster_change:
                        result[field] = roster_change[field]
            if status == "paused":
                result["message"] = "账号已保存为暂停，不会采集，也不进入当前统计；历史数据保留。"
            elif result["activation_status"] == "active":
                result["message"] = "账号已恢复到当前生效名单，运营状态已保存。"
            elif result["activation_status"] == "scheduled":
                beijing_time = scheduled_time.astimezone(ZoneInfo("Asia/Shanghai")).strftime("%Y-%m-%d %H:%M")
                label = "日更" if status == "daily" else "周更"
                result["message"] = f"账号已保存为{label}；已安排于北京时间 {beijing_time} 激活名单，激活后开始采集。"
            else:
                result["message"] = "账号和运营状态已保存，加入待激活系统名单；名单激活后才开始采集。"
        if catalog_policy is not None:
            result["directory_row_id"] = int(directory["id"])
            result["activation_status"] = "disabled" if status == "paused" else "pending_verification"
            result["message"] = (
                "账号已暂停自动更新，历史内容和数据保留。" if status == "paused" else
                "账号状态已保存，系统会自动核验并更新采集状态，无需另行加入名单。"
            )
            result["automatic_capture"] = {
                "eligible": False, "reason_code": "account_paused" if status == "paused" else "pending_verification",
                "reason_label": "账号已暂停" if status == "paused" else "等待系统核验",
            }
        if status is not None:
            result["status_request_id"] = request_id
            record_status_receipt(
                connection, request_id=request_id, account_id=account_id,
                account_identity_id=int(identity["id"]), requested_status=status,
                update_frequency=frequency, request=request, actor=actor, reason=reason,
                before={"enabled": bool(before["enabled"]), "update_frequency": previous_frequency},
                after={"enabled": bool(after["enabled"]), "update_frequency": frequency},
                result=result, timestamp=timestamp,
            )
    except Exception:
        connection.execute("ROLLBACK TO account_operating_status")
        connection.execute("RELEASE account_operating_status")
        raise
    else:
        connection.execute("RELEASE account_operating_status")
        return result
