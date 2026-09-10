"""The operator's current account directory, separate from paid capture admission.

Workbook rows without a verified platform identity remain real directory rows;
they never receive a fabricated UID or a capture-enabled account.
"""

from __future__ import annotations

import json
import hashlib
import re
import sqlite3
from collections import Counter
from typing import Any, Mapping

from .account_classification import (
    classification_from_source, has_classification_columns,
)


DIRECTORY_SCHEMA = """CREATE TABLE IF NOT EXISTS account_directory_rows (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    source_sha256 TEXT NOT NULL,
    source_name TEXT NOT NULL,
    source_sheet TEXT NOT NULL,
    source_row INTEGER NOT NULL,
    account_id INTEGER REFERENCES accounts(id) ON DELETE RESTRICT,
    platform TEXT NOT NULL,
    uid TEXT,
    nickname TEXT NOT NULL DEFAULT '',
    display_account_id TEXT NOT NULL DEFAULT '',
    phone TEXT NOT NULL DEFAULT '',
    operator_name TEXT NOT NULL DEFAULT '',
    account_group TEXT NOT NULL DEFAULT 'unknown'
        CHECK(account_group IN ('unknown','mixed_edit','innovation','image_text','boutique_ip')),
    business_direction TEXT NOT NULL DEFAULT 'unknown'
        CHECK(business_direction IN ('unknown','new_car','used_car_c1','used_car_c2','ai_xiaodong')),
    account_status TEXT NOT NULL CHECK(account_status IN ('daily','weekly','paused','unmarked')),
    identity_status TEXT NOT NULL CHECK(identity_status IN ('existing_verified','uid_unverified','identity_missing')),
    raw_json TEXT NOT NULL,
    imported_at TEXT NOT NULL,
    updated_at TEXT NOT NULL,
    UNIQUE(source_sha256,source_row),
    UNIQUE(account_id)
)"""
PLATFORMS = {"抖音": "douyin", "小红书": "xiaohongshu", "视频号": "wechat_channels", "快手": "kuaishou"}
STATUSES = {"日更": "daily", "周更": "weekly", "暂停": "paused", "": "unmarked"}


def ensure_account_directory_schema(connection: sqlite3.Connection) -> None:
    """Create the additive directory table; the caller owns migration authority."""
    connection.execute(DIRECTORY_SCHEMA)


def has_account_directory(connection: sqlite3.Connection) -> bool:
    present = connection.execute(
        "SELECT 1 FROM sqlite_master WHERE type='table' AND name='account_directory_rows'"
    ).fetchone()
    return bool(present and connection.execute("SELECT 1 FROM account_directory_rows LIMIT 1").fetchone())


def _text(value: Any) -> str:
    return "" if value is None else str(value).strip()


def import_account_directory(
    connection: sqlite3.Connection, payload: Mapping[str, Any], *, imported_at: str,
) -> dict[str, Any]:
    """Install a complete reviewed directory inside the offline builder transaction.

    Existing exact platform/UID identities retain their IDs. New numeric UIDs
    create disabled identities; missing IDs are directory-only. Phone and display
    account identifiers are never used for ownership matching. This function does
    not seal a runtime roster, activate routes, fetch data, or commit.
    """
    if not connection.in_transaction:
        raise ValueError("Account directory import requires the caller's transaction")
    ensure_account_directory_schema(connection)
    source_sha = _text(payload.get("sha256"))
    if not re.fullmatch(r"[0-9a-f]{64}", source_sha):
        raise ValueError("Account directory requires its source SHA256")
    records = payload.get("records")
    if not isinstance(records, list) or not records:
        raise ValueError("Account directory requires nonempty source rows")
    old = connection.execute("SELECT source_sha256,COUNT(*) FROM account_directory_rows GROUP BY source_sha256").fetchall()
    if old:
        if len(old) == 1 and old[0][0] == source_sha and old[0][1] == len(records):
            return {"status": "unchanged", "source_sha256": source_sha, "total": len(records), "row_count": len(records)}
        raise ValueError("A different account directory is already installed")
    counts: Counter[str] = Counter()
    result_rows: list[dict[str, Any]] = []
    seen: set[tuple[str, str]] = set()
    for record in records:
        raw = dict(record["raw"])
        classification = classification_from_source(raw)
        platform = PLATFORMS.get(_text(raw.get("平台")))
        if platform is None:
            raise ValueError(f"Unsupported platform at source row {record['sourceRow']}")
        requested_uid = _text(raw.get("UID"))
        uid = requested_uid if platform == "douyin" and re.fullmatch(r"[0-9]{6,24}", requested_uid) else None
        status_text = _text(raw.get("更新状态"))
        if status_text not in STATUSES:
            raise ValueError(f"Unsupported account status at source row {record['sourceRow']}")
        status = STATUSES[status_text]
        phone, operator = _text(raw.get("手机号")), _text(raw.get("姓名"))
        nickname = _text(raw.get("昵称"))
        account_id = None
        identity_id = None
        identity_status = "identity_missing"
        if uid is not None:
            key = (platform, uid)
            if key in seen:
                raise ValueError("Duplicate platform UID in source directory")
            seen.add(key)
            identities = connection.execute(
                "SELECT i.id,i.account_id,a.enabled FROM account_platform_identities i "
                "JOIN accounts a ON a.id=i.account_id WHERE i.platform=? AND i.uid=?", key
            ).fetchall()
            if len(identities) > 1:
                raise ValueError("Platform UID resolves to multiple existing identities")
            digits = re.sub(r"\D", "", phone)
            phone_normalized = digits if 7 <= len(digits) <= 20 else None
            if identities:
                identity_id, account_id, was_enabled = identities[0]
                # A directory label cannot resume a paused identity or authorize
                # paid requests. Missing frequency and explicit pause stop it.
                enabled = int(bool(was_enabled) and status in {"daily", "weekly"})
                connection.execute(
                    "UPDATE accounts SET phone=?,phone_normalized=?,operator_name=?,enabled=?,updated_at=? WHERE id=?",
                    (phone, phone_normalized, operator, enabled, imported_at, account_id),
                )
                identity_status = "existing_verified"
            else:
                cursor = connection.execute(
                    "INSERT INTO accounts(phone,phone_normalized,operator_name,enabled,created_at,updated_at) VALUES (?,?,?,0,?,?)",
                    (phone, phone_normalized, operator, imported_at, imported_at),
                )
                account_id = int(cursor.lastrowid)
                cursor = connection.execute(
                    "INSERT INTO account_platform_identities(account_id,platform,uid,nickname,source,created_at,updated_at) "
                    "VALUES (?,?,?,?,'account_directory',?,?)",
                    (account_id, platform, uid, nickname, imported_at, imported_at),
                )
                identity_id = int(cursor.lastrowid)
                identity_status = "uid_unverified"
        cursor = connection.execute(
            "INSERT INTO account_directory_rows(source_sha256,source_name,source_sheet,source_row,account_id,platform,uid,"
            "nickname,display_account_id,phone,operator_name,account_status,identity_status,raw_json,imported_at,updated_at,account_group,business_direction) "
            "VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)",
            (source_sha, _text(payload.get("source")), _text(payload.get("sheet")), int(record["sourceRow"]),
             account_id, platform, uid, nickname, _text(raw.get("ID_抖音号")), phone, operator, status, identity_status,
             json.dumps(raw, ensure_ascii=False, sort_keys=True), imported_at, imported_at,
             classification["account_group"], classification["business_direction"]),
        )
        counts[identity_status] += 1
        result_rows.append({"source_row": int(record["sourceRow"]), "directory_row_id": int(cursor.lastrowid),
                            "account_id": account_id, "identity_id": identity_id, "identity_status": identity_status})
    return {"status": "imported", "source_sha256": source_sha, "total": len(records), "row_count": len(records),
            "matched_count": counts["existing_verified"], "created_count": counts["uid_unverified"],
            "unresolved_count": counts["identity_missing"], **counts, "rows": result_rows}


def directory_account_items(
    connection: sqlite3.Connection, *, roster: Mapping[str, Any],
    update_frequencies: Mapping[int, str | None], admission_members: Mapping[int, Mapping[str, Any]],
    query: str = "", platform: str | None = None, account_status: str | None = None,
    account_group: str | None = None, business_direction: str | None = None,
) -> list[dict[str, Any]]:
    from .operations import account_read_model

    result = []
    query = query.strip().casefold()
    for row in connection.execute("SELECT * FROM account_directory_rows ORDER BY source_row,id"):
        entry = dict(row)
        if platform and entry["platform"] != platform:
            continue
        if account_status and entry["account_status"] != account_status:
            continue
        if query and query not in " ".join(_text(entry.get(field)) for field in
            ("uid", "nickname", "display_account_id", "phone", "operator_name")).casefold():
            continue
        account = connection.execute("SELECT * FROM accounts WHERE id=?", (entry["account_id"],)).fetchone()
        if account is not None:
            value = account_read_model(connection, account, roster=roster,
                                       update_frequencies=update_frequencies, admission_members=admission_members)
        else:
            value = {"id": -int(entry["id"]), "phone": entry["phone"], "operator_name": entry["operator_name"],
                     "enabled": False,
                     "updated_at": entry["updated_at"], "platforms": [{"id": None, "platform": entry["platform"],
                     "uid": None, "nickname": entry["nickname"], "real_name_status": "unknown", "content_count": 0,
                     "follower_count": None, "platform_work_count": None, "data_status": "not_collected"}]}
        classification = {"account_group": entry["account_group"], "business_direction": entry["business_direction"]}
        if account_group and classification["account_group"] != account_group:
            continue
        if business_direction and classification["business_direction"] != business_direction:
            continue
        value.update(directory_row_id=entry["id"], directory_identity_status=entry["identity_status"],
                     directory_platform=entry["platform"], directory_uid=entry["uid"],
                     **classification,
                     account_status=entry["account_status"],
                     update_frequency=entry["account_status"] if entry["account_status"] in {"daily", "weekly"} else None)
        for identity in value["platforms"]:
            # The supplied directory remains the display authority. Provider
            # metric refreshes must not silently restore obsolete labels.
            identity["nickname"] = entry["nickname"]
            identity["unique_id"] = entry["display_account_id"] if entry["display_account_id"] not in {"无", "封"} else ""
        result.append(value)
    return result


def update_directory_operating_fields(connection: sqlite3.Connection, account_id: int, values: Mapping[str, Any]) -> None:
    """Keep visible directory fields consistent after an authorized account edit."""
    if not has_account_directory(connection):
        return
    allowed = {key: values[key] for key in ("phone", "operator_name", "account_status") if key in values}
    if not allowed:
        return
    connection.execute("UPDATE account_directory_rows SET " + ",".join(f"{field}=?" for field in allowed)
                       + " WHERE account_id=?", (*allowed.values(), account_id))


def admit_directory_account(
    connection: sqlite3.Connection, *, account_id: int, member: Mapping[str, Any],
    account_status: str, request_id: str, at: str,
) -> None:
    """Admit a resolved identity within its explicit profile-create transaction.

    This is not provider discovery: the caller has completed profile conflict
    checks. In catalog mode the caller admits before sealing the operating
    receipt, with both changes inside the same savepoint; legacy mode calls
    after route admission. Re-resolving an imported unverified UID upgrades it.
    """
    if not has_classification_columns(connection):
        return
    if not connection.in_transaction:
        raise ValueError("Directory admission requires the account transaction")
    account = connection.execute("SELECT * FROM accounts WHERE id=?", (account_id,)).fetchone()
    identity = connection.execute("SELECT * FROM account_platform_identities WHERE account_id=?", (account_id,)).fetchone()
    if account is None or identity is None or (identity["platform"], identity["uid"]) != (member["platform"], member["uid"]):
        raise ValueError("Directory admission identity does not match the saved account")
    metadata = member.get("metadata") or {}
    nickname = _text(member.get("nickname"))
    display_id = _text(metadata.get("display_account_id") or member.get("display_account_id"))
    existing = connection.execute("SELECT id FROM account_directory_rows WHERE account_id=?", (account_id,)).fetchone()
    if existing is not None:
        connection.execute(
            "UPDATE account_directory_rows SET identity_status='existing_verified',account_status=?,"
            "nickname=?,display_account_id=?,phone=?,operator_name=?,updated_at=? WHERE account_id=?",
            (account_status, nickname, display_id, account["phone"], account["operator_name"], at, account_id),
        )
        return
    next_row = connection.execute("SELECT COALESCE(MAX(source_row),1)+1 FROM account_directory_rows").fetchone()[0]
    source_sha = hashlib.sha256(("manual-account-add:" + request_id).encode()).hexdigest()
    connection.execute(
        "INSERT INTO account_directory_rows(source_sha256,source_name,source_sheet,source_row,account_id,platform,uid,"
        "nickname,display_account_id,phone,operator_name,account_status,identity_status,raw_json,imported_at,updated_at) "
        "VALUES (?,'manual-account-add','',?,?,?,?,?,?,?,?,?,'existing_verified',?,?,?)",
        (source_sha, next_row, account_id, identity["platform"], identity["uid"], nickname, display_id,
         account["phone"], account["operator_name"], account_status,
         json.dumps({"request_id": request_id, "member": dict(member)}, ensure_ascii=False, sort_keys=True), at, at),
    )
