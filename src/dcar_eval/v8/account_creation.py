"""Atomic explicit admission of new or historical system-managed accounts."""

from __future__ import annotations

import sqlite3
from pathlib import Path
from typing import Any, Mapping
from urllib.parse import urlsplit
from uuid import UUID, uuid4

from .account_operating_receipts import (
    AccountOperatingStatusError,
    find_status_request,
    load_admission_members,
    load_update_frequencies,
)
from .account_operating_status import (
    ACCOUNT_STATUSES,
    ScheduleActivation,
    account_operating_status,
    update_account_operating_status_in_transaction,
)
from .account_roster import SYSTEM_SOURCE_FAMILY, RosterError, normalize_member
from .operations import create_account_in_transaction, normalize_phone
from .system_roster import current_system_members


def validate_creation_request_id(value: str | None) -> str:
    if value is None:
        return str(uuid4())
    try:
        if str(UUID(value)) != value:
            raise ValueError
    except (TypeError, ValueError, AttributeError) as error:
        raise AccountOperatingStatusError(
            "account_create_request_invalid", "新增账号的请求编号必须是 UUID"
        ) from error
    return value


def replay_account_creation(
    connection: sqlite3.Connection, *, request_id: str, request_context: Mapping[str, Any]
) -> dict[str, Any] | None:
    """Read a verified previous command before resolving a URL or admitting again."""
    existing = find_status_request(connection, request_id=request_id)
    if existing is None:
        return None
    payload = existing["payload"]
    admission = payload["request"].get("admission")
    if not isinstance(admission, dict) or admission.get("input") != dict(request_context):
        raise AccountOperatingStatusError(
            "account_create_request_conflict", "该请求编号已用于不同的新增内容，请重新提交"
        )
    result = dict(payload["result"])
    result["replayed"] = True
    # Replaying an older command must neither undo later edits nor imply that
    # its historical state still applies now.
    account_id = int(payload["account_id"])
    current = connection.execute("SELECT * FROM accounts WHERE id=?", (account_id,)).fetchone()
    if current is None:
        raise AccountOperatingStatusError("account_not_found", "原请求对应的账号不存在")
    frequency = load_update_frequencies(connection, [account_id])[account_id]
    from .account_catalog_capture import installed_policy
    from .storage import now_utc
    catalog_policy = installed_policy(connection, at=now_utc())
    directory = (connection.execute("SELECT account_status FROM account_directory_rows WHERE account_id=?", (account_id,)).fetchone()
                 if catalog_policy is not None else None)
    result["current_account_status"] = (
        directory["account_status"] if directory is not None else
        account_operating_status({**dict(current), "update_frequency": frequency})
    )
    if directory is not None and directory["account_status"] in {"daily", "weekly", "unmarked"}:
        frequency = directory["account_status"] if directory["account_status"] != "unmarked" else None
    result["current_enabled"] = bool(current["enabled"])
    result["original_account_status"] = result["account_status"]
    result["original_enabled"] = result["enabled"]
    result["account_status"] = result["current_account_status"]
    result["enabled"] = result["current_enabled"]
    result["update_frequency"] = frequency
    if result["account_status"] != result["original_account_status"]:
        result["message"] = "该新增请求此前已完成；账号后来已被修改，本次没有覆盖后续修改。"
        result["original_activation_status"] = result.get("activation_status")
        for key in ("activation_status", "scheduled_effective_at", "scheduled_activation_id", "roster_change"):
            result.pop(key, None)
    if result["account_status"] == "paused" or (catalog_policy is None and not result["enabled"]):
        result["activation_status"] = "disabled"
    elif catalog_policy is not None and not result["enabled"]:
        result["activation_status"] = "pending_verification"
        result["automatic_capture"] = {"eligible": False, "reason_code": "pending_verification", "reason_label": "等待系统核验"}
    return result


def _check_profile_identity(
    connection: sqlite3.Connection, member: Mapping[str, Any], identity_id: int | None
) -> None:
    """Apply roster reference conflicts even when paused admission seals no roster."""
    references: set[str] = set()
    if member["profile_ref"]:
        references.update((member["profile_ref"], urlsplit(member["profile_ref"]).path.rsplit("/", 1)[-1]))
    sec_user_id = member["metadata"].get("sec_user_id")
    profile_token = urlsplit(str(member.get("profile_ref") or "")).path.rsplit("/", 1)[-1]
    if member["platform"] == "douyin" and profile_token.startswith("MS4w") and sec_user_id and profile_token != sec_user_id:
        raise RosterError("identity_conflict", "Profile URL and sec_user_id identify different accounts")
    if sec_user_id:
        references.add(sec_user_id)
    if references:
        placeholders = ",".join("?" for _ in references)
        matches = connection.execute(
            f"""SELECT DISTINCT r.account_identity_id FROM account_provider_references r
                WHERE r.reference_kind IN ('sec_uid','sec_user_id','user_id','profile_id','profile_url')
                AND r.reference_value IN ({placeholders})""", tuple(sorted(references)),
        )
        if any(row[0] != identity_id for row in matches):
            raise RosterError("identity_conflict", "Profile reference belongs to another identity")
    if identity_id is not None and sec_user_id:
        old = connection.execute(
            "SELECT reference_value FROM account_provider_references WHERE account_identity_id=? "
            "AND provider='tikhub' COLLATE NOCASE AND reference_kind='sec_user_id'", (identity_id,),
        ).fetchall()
        if any(row[0] != sec_user_id for row in old):
            raise RosterError("identity_conflict", "An existing sec_user_id cannot be replaced")
    for bound_id, saved in load_admission_members(connection).items():
        prior = saved["member"]
        prior_refs = {prior["profile_ref"], urlsplit(prior["profile_ref"]).path.rsplit("/", 1)[-1]} if prior.get("profile_ref") else set()
        prior_sec = prior.get("metadata", {}).get("sec_user_id")
        if prior_sec:
            prior_refs.add(prior_sec)
        if references.intersection(prior_refs) and bound_id != identity_id:
            raise RosterError("identity_conflict", "Profile was already bound by a paused-account receipt")
        if bound_id == identity_id and prior_sec and sec_user_id and prior_sec != sec_user_id:
            raise RosterError("identity_conflict", "A verified paused-account sec_user_id cannot be replaced")


def normalize_creation_member(connection: sqlite3.Connection, member: Mapping[str, Any]) -> dict[str, Any]:
    """Canonicalize new XHS IDs without rewriting existing historical spellings."""
    normalized = normalize_member(member, source_family=SYSTEM_SOURCE_FAMILY)
    if normalized["platform"] == "xiaohongshu":
        matches = connection.execute(
            "SELECT uid FROM account_platform_identities WHERE platform='xiaohongshu' AND uid=? COLLATE NOCASE",
            (normalized["uid"],),
        ).fetchall()
        if len(matches) > 1:
            raise RosterError("identity_conflict", "Several historical identities differ only by UID case")
        normalized["uid"] = str(matches[0]["uid"]) if matches else str(normalized["uid"]).lower()
        normalized["member_key"] = "uid:xiaohongshu:" + normalized["uid"]
    return normalized


def create_managed_account_in_transaction(
    connection: sqlite3.Connection,
    member: Mapping[str, Any],
    *,
    account_status: str,
    phone: str | None = None,
    operator_name: str | None = None,
    request_id: str | None = None,
    request_context: Mapping[str, Any] | None = None,
    raw_root: Path,
    actor: str,
    reason: str,
    activation_id: int | None = None,
    active_snapshot_id: int | None = None,
    schedule_activation: ScheduleActivation | None = None,
) -> dict[str, Any]:
    """Keep identity, operating receipt, state event and activation in one unit."""
    if not connection.in_transaction:
        raise AccountOperatingStatusError("account_status_transaction_required", "账号新增需要已有事务")
    if account_status not in ACCOUNT_STATUSES:
        raise AccountOperatingStatusError("account_status_invalid", "请选择日更、周更或暂停")
    request_id = validate_creation_request_id(request_id)
    normalized = normalize_creation_member(connection, member)
    fields = {key: value.strip() for key, value in {"phone": phone, "operator_name": operator_name}.items()
              if isinstance(value, str) and value.strip()}
    normalize_phone(fields.get("phone"))
    context = dict(request_context) if request_context is not None else {
        "member": normalized, "account_status": account_status, **fields,
    }
    replay = replay_account_creation(connection, request_id=request_id, request_context=context)
    if replay is not None:
        return replay
    from .account_catalog_capture import installed_policy
    from .storage import now_utc
    catalog_policy = installed_policy(connection, at=now_utc())
    connection.execute("SAVEPOINT account_creation")
    try:
        existing = connection.execute(
            """SELECT i.id identity_id,i.account_id,a.enabled
               FROM account_platform_identities i JOIN accounts a ON a.id=i.account_id
               WHERE i.platform=? AND i.uid=?""", (normalized["platform"], normalized["uid"]),
        ).fetchone()
        in_latest = catalog_policy is None and any(
            row["platform"] == normalized["platform"] and row["uid"] == normalized["uid"]
            for row in current_system_members(connection))
        in_active = catalog_policy is None and existing is not None and active_snapshot_id is not None and connection.execute(
            "SELECT 1 FROM account_roster_members WHERE snapshot_id=? AND account_identity_id=?",
            (active_snapshot_id, existing["identity_id"]),
        ).fetchone() is not None
        if existing is not None and existing["enabled"] and (in_latest or in_active):
            raise RosterError("system_member_exists", "System roster member already exists")
        if catalog_policy is not None and existing is not None:
            directory = connection.execute(
                "SELECT identity_status,account_status FROM account_directory_rows WHERE account_id=?",
                (existing["account_id"],),
            ).fetchone()
            if directory is not None and directory["identity_status"] == "existing_verified" and directory["account_status"] in {"daily", "weekly"}:
                raise RosterError("system_member_exists", "账号已存在，请在账号页修改状态。")
        _check_profile_identity(connection, normalized, int(existing["identity_id"]) if existing else None)
        if existing is None:
            account_id = int(create_account_in_transaction(connection, normalized)["id"])
            action = "created"
        else:
            account_id = int(existing["account_id"])
            action = "restored" if account_status != "paused" else "saved_paused"
        if catalog_policy is not None:
            from .account_directory import admit_directory_account
            # The enclosing savepoint rolls this provisional directory entry
            # back unless the identity-bound operating receipt also succeeds.
            admit_directory_account(connection, account_id=account_id, member=normalized,
                                    account_status=account_status, request_id=request_id, at=now_utc())
        result = update_account_operating_status_in_transaction(
            connection, account_id,
            {"account_status": account_status, "status_request_id": request_id, **fields},
            raw_root=raw_root, actor=actor, reason=reason, activation_id=activation_id,
            schedule_activation=schedule_activation,
            admission={"member": normalized, "input": context, "action": action,
                       "active_snapshot_id": active_snapshot_id},
        )
        if catalog_policy is not None and account_status != "paused":
            from .account_capture_eligibility import derive_capture_eligibility
            from .account_catalog_capture import materialize_proven_locators
            members = [row for row in derive_capture_eligibility(connection)["eligible_members"]
                       if row["account_id"] == account_id]
            materialize_proven_locators(connection, members, at=now_utc())
    except Exception:
        connection.execute("ROLLBACK TO account_creation")
        connection.execute("RELEASE account_creation")
        raise
    connection.execute("RELEASE account_creation")
    return result
