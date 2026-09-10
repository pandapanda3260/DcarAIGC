"""The account directory owns operating classifications, never capture policy."""

from __future__ import annotations

import re
import sqlite3
from typing import Any, Mapping

ACCOUNT_GROUPS = {"unknown": "未填写", "mixed_edit": "混剪号", "innovation": "创新号",
                  "image_text": "图文号", "boutique_ip": "精品IP号"}
BUSINESS_DIRECTIONS = {"unknown": "未填写", "new_car": "新车", "used_car_c1": "二手车C1",
                       "used_car_c2": "二手车C2", "ai_xiaodong": "AI小懂"}
CLASSIFICATION_FIELDS = frozenset({"account_group", "business_direction"})


def normalize_classification(field: str, value: Any) -> str:
    options = ACCOUNT_GROUPS if field == "account_group" else BUSINESS_DIRECTIONS if field == "business_direction" else None
    if options is None or not isinstance(value, str) or value not in options:
        raise ValueError(f"{field} 分类无效，请刷新后重新选择。")
    return value


def classification_from_source(raw: Mapping[str, Any]) -> dict[str, str]:
    """Import explicit reviewed labels; do not guess from the obsolete enums."""
    result = {}
    for field, source, options in (("account_group", "质量标签", ACCOUNT_GROUPS),
                                   ("business_direction", "业务标签", BUSINESS_DIRECTIONS)):
        label = str(raw.get(source) or "").strip()
        aliases = {text: key for key, text in options.items()}
        aliases.update({"": "unknown", "未知": "unknown", "待分类": "unknown", "未填写": "unknown"})
        if field == "business_direction":
            aliases["媒体-AI小懂"] = "ai_xiaodong"
        if label not in aliases:
            raise ValueError(f"名单{source}存在未识别值: {label}")
        result[field] = aliases[label]
    return result


def has_classification_columns(connection: sqlite3.Connection) -> bool:
    return CLASSIFICATION_FIELDS <= {row[1] for row in connection.execute("PRAGMA table_info(account_directory_rows)")}


def classification_sql(connection: sqlite3.Connection, field: str, *, account_alias: str = "a") -> str:
    if field not in CLASSIFICATION_FIELDS or not re.fullmatch(r"[A-Za-z_][A-Za-z0-9_]*", account_alias):
        raise ValueError("Invalid classification SQL field or alias")
    if not has_classification_columns(connection):
        return "'unknown'"
    return f"COALESCE((SELECT ad.{field} FROM account_directory_rows ad WHERE ad.account_id={account_alias}.id),'unknown')"


def classification_updated_at_sql(connection: sqlite3.Connection, *, account_alias: str = "a") -> str:
    if not re.fullmatch(r"[A-Za-z_][A-Za-z0-9_]*", account_alias):
        raise ValueError("Invalid account SQL alias")
    if not has_classification_columns(connection):
        return f"{account_alias}.updated_at"
    directory_time = f"(SELECT ad.updated_at FROM account_directory_rows ad WHERE ad.account_id={account_alias}.id)"
    return (f"CASE WHEN julianday({directory_time})>julianday({account_alias}.updated_at) "
            f"OR {account_alias}.updated_at IS NULL THEN {directory_time} ELSE {account_alias}.updated_at END")


def classification_for_account(connection: sqlite3.Connection, account_id: int) -> dict[str, str]:
    if has_classification_columns(connection):
        row = connection.execute("SELECT account_group,business_direction FROM account_directory_rows WHERE account_id=?", (account_id,)).fetchone()
        if row:
            return {"account_group": row[0], "business_direction": row[1]}
    return {"account_group": "unknown", "business_direction": "unknown"}


def update_classification_in_transaction(connection: sqlite3.Connection, account_id: int,
                                         values: Mapping[str, Any]) -> dict[str, Any]:
    """Save either linked or unlinked directory rows without touching capture."""
    from .storage import now_utc

    if not connection.in_transaction:
        raise ValueError("账号分类更新需要已有事务")
    if not has_classification_columns(connection):
        raise ValueError("账号分类尚未完成升级，请稍后重试。")
    updates = {field: normalize_classification(field, values[field]) for field in CLASSIFICATION_FIELDS if field in values}
    row = connection.execute("SELECT * FROM account_directory_rows WHERE " + ("id=?" if account_id < 0 else "account_id=?"),
                             (-account_id if account_id < 0 else account_id,)).fetchone()
    if row is None:
        raise ValueError("该账号不在当前账号清单中。")
    if account_id < 0 and (row["account_id"] is not None or row["identity_status"] != "identity_missing"):
        raise ValueError("账号身份已完善，请刷新后重试。")
    changed = {field: value for field, value in updates.items() if row[field] != value}
    if changed:
        connection.execute("UPDATE account_directory_rows SET " + ",".join(f"{field}=?" for field in changed)
                           + ",updated_at=? WHERE id=?", (*changed.values(), now_utc(), row["id"]))
    return {"id": account_id, "directory_row_id": row["id"],
            **{field: updates.get(field, row[field]) for field in CLASSIFICATION_FIELDS},
            "message": "账号分类已更新。"}
