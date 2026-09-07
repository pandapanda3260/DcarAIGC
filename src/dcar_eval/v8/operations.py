"""Account/content write models, deterministic identity upserts and CSV exports."""

from __future__ import annotations

import csv
import hashlib
import io
import json
import re
import sqlite3
import uuid
from contextlib import contextmanager
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Dict, Iterator, List, Mapping, Optional, Sequence
from urllib.parse import urlsplit, urlunsplit

from .account_operating_status import account_operating_status
from .account_operating_receipts import load_admission_members, load_update_frequencies
from .account_states import (
    AccountStateError,
    set_account_enabled_in_transaction,
)
from .content_scope import canonical_content_predicate
from .evaluation_selectors import (
    DISPLAY_EFFECTIVE_EVALUATIONS_CTE,
    active_release,
    effective_direction_sql,
)
from .migration import generate_link_id, normalize_timestamp
from .metric_observations import merge_metric_snapshots
from .report_export import build_accounts_workbook, formula_safe_csv_value
from .source_routing import select_content_metrics
from .storage import DEFAULT_DB, connect, now_utc, transaction, write_lock


PLATFORMS = {"douyin", "xiaohongshu", "wechat_channels", "kuaishou"}
ACCOUNT_TYPES = {"boutique_ip", "original", "mixed_edit", "unknown"}
DIRECTIONS = {"new_car", "used_car", "media", "other", "unknown"}
REAL_NAME_STATUSES = {"yes", "no", "unknown"}
# 账号平台身份 + 关联内容量：账号页表格（api._account_search）与账号表格导出
# （export_accounts_xlsx）共用的取数口径，改一处两边同时生效。
# SQL only supplies identity/local counts; account_metrics owns provider statistics.
ACCOUNT_IDENTITY_STATS_SQL = """
    SELECT api.id, api.platform, api.uid, api.nickname, api.real_name_status,
           NULL follower_count, NULL platform_work_count,
           NULL avatar_url, NULL unique_id, NULL data_date,
           'not_collected' data_status,
           COUNT(c.id) content_count
    FROM account_platform_identities api
    LEFT JOIN content_items c
      ON c.account_id=api.account_id AND c.platform=api.platform
    WHERE api.account_id=?
    GROUP BY api.id, api.platform, api.uid, api.nickname,
             api.real_name_status
    ORDER BY api.platform
"""
CONTENT_PATCH_FIELDS = frozenset(
    {
        "platform",
        "platform_content_id",
        "canonical_url",
        "published_at",
        "title",
        "body",
        "content_type",
        "account_uid",
        "account_name",
        "account_type",
        "content_direction",
    }
)
CONTENT_CHILD_REKEY_TABLES = (
    "fetch_slots",
    "provider_raw_responses",
    "comment_evidence_versions",
    "comment_capture_runs",
    "evidence_artifacts",
    "evidence_envelopes",
    "media_processing_slots",
    "duplicate_fingerprints",
    # SPU 关联域（schema v14/v15）：派生标签，随内容改键搬到存活方。
    # 它们都带 invalidated_at + rule_version，重跑关联即可重建，
    # 但合并时必须跟着走，否则旧 content_id 上的关联会成为孤儿。
    "content_spu_links",
    "content_audience_links",
    "content_scene_links",
    "llm_judgements",
)
CONTENT_CHILD_MERGE_POLICIES = {
    **{table: "rekey" for table in CONTENT_CHILD_REKEY_TABLES},
    "content_metric_snapshots": "canonical_projection_merge",
    "content_metric_observations": "immutable_fact_rekey",
    "content_identities": "special_rekey",
    "content_aliases": "special_rekey",
    "comment_user_scores": "special_rekey",
    "duplicate_relations": "special_relation",
    "evaluation_versions": "protected_history",
    "task_contents": "protected_history",
}
DOUYIN_ID_RE = re.compile(r"(?:/video/|[?&]modal_id=)(\d{6,24})(?:[/?&#]|$)", re.I)
XHS_ID_RE = re.compile(
    r"/(?:explore|discovery/item)/(?:[^/?#]*/)?([0-9a-f]{24})(?:[/?#]|$)", re.I
)


class OperationError(RuntimeError):
    pass


class IdentityConflictError(OperationError):
    error_code = "identity_conflict"

    def __init__(self, message: str, *, provider_cost: float = 0.0) -> None:
        super().__init__(message)
        self.provider_cost = provider_cost


@contextmanager
def _content_write_transaction(
    db_path: Path, *, connection: Optional[sqlite3.Connection] = None
) -> Iterator[sqlite3.Connection]:
    """Commit only the conflict marker when an identity merge fails closed."""

    if connection is not None:
        if not connection.in_transaction:
            raise OperationError("content write requires an active caller transaction")
        database = connection.execute("PRAGMA database_list").fetchone()
        if database is None or Path(str(database[2])).resolve() != db_path.resolve():
            raise OperationError("content transaction belongs to a different database")
        # Durable page ingestion owns fencing, row writes and cursor advance.
        # Never commit part of that page from an inner content operation.
        yield connection
        return
    # Standalone writer: hold the process write lock for the whole
    # BEGIN..COMMIT span, exactly like ``storage.transaction`` does.  The
    # conflict-marker commit on IdentityConflictError is preserved as is.
    with write_lock(), connect(db_path) as connection:
        connection.execute("BEGIN IMMEDIATE")
        try:
            yield connection
        except IdentityConflictError:
            connection.commit()
            raise
        except Exception:
            connection.rollback()
            raise
        else:
            connection.commit()


def normalize_phone(value: Any) -> tuple[str, Optional[str]]:
    display = str(value or "").strip()
    if not display:
        return "", None
    digits = re.sub(r"\D", "", display)
    if len(digits) == 13 and digits.startswith("86"):
        digits = digits[2:]
    if not 7 <= len(digits) <= 20:
        raise OperationError("手机号必须包含 7 到 20 位数字")
    return display, digits


def normalize_url(value: Any) -> str:
    text = str(value or "").strip()
    try:
        parsed = urlsplit(text)
    except ValueError as exc:
        raise OperationError("链接格式无效") from exc
    if parsed.scheme not in {"http", "https"} or not parsed.netloc:
        raise OperationError("链接必须是完整的 http/https URL")
    host = parsed.hostname.lower() if parsed.hostname else ""
    port = f":{parsed.port}" if parsed.port else ""
    path = re.sub(r"/{2,}", "/", parsed.path).rstrip("/") or "/"
    return urlunsplit(("https", f"{host}{port}", path, "", ""))


def content_identity(
    platform: str, url: str, explicit_id: Any = None
) -> Dict[str, Any]:
    if platform not in PLATFORMS:
        raise OperationError(f"不支持的平台：{platform}")
    canonical = normalize_url(url)
    content_id = str(explicit_id or "").strip()
    if platform == "douyin":
        match = DOUYIN_ID_RE.search(str(url))
        content_id = content_id or (match.group(1) if match else "")
        if not re.fullmatch(r"\d{6,24}", content_id):
            raise OperationError("抖音链接必须能解析出数字作品 ID，短链需先展开")
    elif platform == "xiaohongshu":
        match = XHS_ID_RE.search(str(url))
        content_id = (content_id or (match.group(1) if match else "")).lower()
        if not re.fullmatch(r"[0-9a-f]{24}", content_id):
            raise OperationError("小红书链接必须能解析出 24 位笔记 ID，短链需先展开")
    elif content_id and len(content_id) > 128:
        raise OperationError("平台内容 ID 不能超过 128 个字符")
    normalized_hash = hashlib.sha256(canonical.encode("utf-8")).hexdigest()
    key = (
        f"{platform}:{content_id}"
        if content_id
        else f"{platform}:url:{normalized_hash}"
    )
    return {
        "platform": platform,
        "platform_content_id": content_id or None,
        "canonical_url": canonical,
        "normalized_url_hash": normalized_hash,
        "identity_key": key,
    }


def _enum(value: Any, allowed: set[str], field: str, default: str = "unknown") -> str:
    normalized = str(value or default).strip()
    if normalized not in allowed:
        raise OperationError(f"{field} 的值无效：{normalized}")
    return normalized


def _identity_rows(value: Mapping[str, Any]) -> List[Dict[str, str]]:
    rows: List[Dict[str, str]] = []
    raw = value.get("platforms") or []
    if not isinstance(raw, list):
        raise OperationError("platforms 必须是数组")
    for item in raw:
        if not isinstance(item, Mapping):
            raise OperationError("平台身份必须是对象")
        platform = str(item.get("platform") or "")
        uid = str(item.get("uid") or "").strip()
        if platform not in PLATFORMS or not uid:
            raise OperationError("平台身份必须包含有效 platform 和 uid")
        rows.append(
            {
                "platform": platform,
                "uid": uid,
                "nickname": str(item.get("nickname") or "").strip(),
                "real_name_status": _enum(
                    item.get("real_name_status"),
                    REAL_NAME_STATUSES,
                    "real_name_status",
                ),
            }
        )
    if len(rows) > 1:
        raise OperationError("一个账号只能绑定一个平台身份；不同平台必须分别建档")
    return rows


def _expand_flat_account_row(value: Mapping[str, Any]) -> Dict[str, Any]:
    expanded = dict(value)
    if "platforms" in expanded:
        return expanded
    platforms: List[Dict[str, str]] = []
    for platform in sorted(PLATFORMS):
        uid = str(expanded.get(f"{platform}_uid") or "").strip()
        if not uid:
            continue
        platforms.append(
            {
                "platform": platform,
                "uid": uid,
                "nickname": str(expanded.get(f"{platform}_nickname") or "").strip(),
                "real_name_status": str(
                    expanded.get(f"{platform}_real_name_status") or "unknown"
                ).strip(),
            }
        )
    expanded["platforms"] = platforms
    enabled = expanded.get("enabled")
    if isinstance(enabled, str):
        expanded["enabled"] = enabled.strip().lower() not in {
            "0",
            "false",
            "no",
            "否",
            "停用",
        }
    return expanded


@contextmanager
def _account_write_connection(
    db_path: Path, connection: sqlite3.Connection | None
) -> Iterator[sqlite3.Connection]:
    if connection is not None:
        if not connection.in_transaction:
            raise OperationError("账号更新需要已有事务")
        yield connection
        return
    with connect(db_path) as owned, transaction(owned):
        yield owned


def _save_account(
    value: Mapping[str, Any],
    *,
    db_path: Path,
    target_account_id: Optional[int] = None,
    state_actor: str | None = None,
    state_reason: str | None = None,
    state_activation_id: int | None = None,
    connection: sqlite3.Connection | None = None,
) -> Dict[str, Any]:
    """Internal stable-identity upsert; a phone is never an ownership key."""

    supplied = dict(value)
    if target_account_id is None and supplied.get("id") is not None:
        target_account_id = int(supplied["id"])
    identities = _identity_rows(supplied)
    captured_at = now_utc()
    with _account_write_connection(db_path, connection) as connection:
        account = None
        existing = None
        if target_account_id is not None:
            account = connection.execute(
                "SELECT * FROM accounts WHERE id=?", (target_account_id,)
            ).fetchone()
            if account is None:
                raise OperationError("账号不存在")
            existing = connection.execute(
                "SELECT * FROM account_platform_identities WHERE account_id=?",
                (target_account_id,),
            ).fetchone()
            if "platforms" in supplied:
                if len(identities) != (1 if existing is not None else 0):
                    raise OperationError("平台身份由矩阵通名册维护，不能在本地新增或移除")
                if existing is not None and (
                    identities[0]["platform"] != existing["platform"]
                    or identities[0]["uid"] != existing["uid"]
                ):
                    raise OperationError("平台身份由矩阵通名册维护，不能在本地替换")
        elif identities:
            existing = connection.execute(
                "SELECT * FROM account_platform_identities WHERE platform=? AND uid=?",
                (identities[0]["platform"], identities[0]["uid"]),
            ).fetchone()
            if existing is not None:
                account = connection.execute(
                    "SELECT * FROM accounts WHERE id=?", (existing["account_id"],)
                ).fetchone()
        else:
            raise OperationError("至少填写一个平台和账号编号；手机号不再作为账号标识")

        merged = dict(account) if account is not None else {}
        for field in (
            "phone", "operator_name", "account_type", "content_direction", "enabled"
        ):
            if field not in supplied:
                continue
            # Provider/internal retries must not erase local operating properties
            # with empty metadata. Explicit ID edits may intentionally clear them.
            if account is not None and target_account_id is None:
                if supplied[field] in (None, "") or (
                    field in {"account_type", "content_direction"}
                    and supplied[field] == "unknown"
                ):
                    continue
            merged[field] = supplied[field]
        phone, normalized = normalize_phone(merged.get("phone"))
        account_type = _enum(merged.get("account_type"), ACCOUNT_TYPES, "account_type")
        direction = _enum(merged.get("content_direction"), DIRECTIONS, "content_direction")
        enabled = bool(merged.get("enabled", True))
        state_event_identity_id: int | None = None
        persisted_enabled = enabled
        schema_version = int(connection.execute("PRAGMA user_version").fetchone()[0])
        if (
            account is not None
            and target_account_id is not None
            and "enabled" in supplied
            and schema_version >= 19
        ):
            if existing is None:
                raise OperationError("账号缺少平台身份，不能修改采集开关")
            state_event_identity_id = int(existing["id"])
            persisted_enabled = bool(account["enabled"])
        local_values = (
            phone,
            normalized,
            str(merged.get("operator_name") or "").strip(),
            account_type,
            direction,
            int(persisted_enabled),
        )
        if account is None:
            cursor = connection.execute(
                """
                INSERT INTO accounts(
                    phone, phone_normalized, operator_name, account_type,
                    content_direction, enabled, created_at, updated_at
                ) VALUES (?, ?, ?, ?, ?, ?, ?, ?)
                """,
                (*local_values, captured_at, captured_at),
            )
            if cursor.lastrowid is None:
                raise RuntimeError("account insert returned no id")
            account_id = int(cursor.lastrowid)
            action = "inserted"
        else:
            account_id = int(account["id"])
            connection.execute(
                """
                UPDATE accounts SET phone=?, phone_normalized=?, operator_name=?,
                    account_type=?, content_direction=?, enabled=?, updated_at=?
                WHERE id=?
                """,
                (*local_values, captured_at, account_id),
            )
            action = "updated"
        if identities:
            identity = identities[0]
            if existing is None:
                connection.execute(
                    """
                    INSERT INTO account_platform_identities(
                        account_id, platform, uid, nickname, real_name_status,
                        source, created_at, updated_at
                    ) VALUES (?, ?, ?, ?, ?, 'manual', ?, ?)
                    """,
                    (
                        account_id, identity["platform"], identity["uid"],
                        identity["nickname"], identity["real_name_status"],
                        captured_at, captured_at,
                    ),
                )
            else:
                connection.execute(
                    """
                    UPDATE account_platform_identities
                    SET nickname=?, real_name_status=?, updated_at=? WHERE id=?
                    """,
                    (
                        identity["nickname"] or existing["nickname"],
                        identity["real_name_status"]
                        if identity["real_name_status"] != "unknown"
                        else existing["real_name_status"],
                        captured_at,
                        existing["id"],
                    ),
                )
            # Only stable matches claim still-unassociated facts. Local operating
            # edits never detach content or rewrite an identity/provider reference.
            connection.execute(
                """
                UPDATE content_items SET account_id=?, updated_at=?
                WHERE account_id IS NULL AND platform=? AND raw_account_uid=?
                """,
                (account_id, captured_at, identity["platform"], identity["uid"]),
            )
        if state_event_identity_id is not None:
            state_at = (
                datetime.now(timezone.utc)
                .isoformat(timespec="microseconds")
                .replace("+00:00", "Z")
            )
            try:
                set_account_enabled_in_transaction(
                    connection,
                    state_event_identity_id,
                    enabled=enabled,
                    effective_at=state_at,
                    created_at=state_at,
                    actor=state_actor if state_actor is not None else "operator",
                    reason=(
                        state_reason
                        if state_reason is not None
                        else "local account enabled-state update"
                    ),
                    activation_id=state_activation_id,
                    metadata={"operation": "update_account"},
                )
            except AccountStateError as error:
                raise OperationError(str(error)) from error
    return {"id": account_id, "action": action}


def upsert_account(
    value: Mapping[str, Any], *, db_path: Path = DEFAULT_DB
) -> Dict[str, Any]:
    return _save_account(value, db_path=db_path)


def create_account_in_transaction(
    connection: sqlite3.Connection, identity: Mapping[str, Any]
) -> Dict[str, Any]:
    """Create an initially disabled identity; the status service owns admission.

    This deliberately cannot update an existing account or change its enabled
    flag. The caller must finish its operating-status command in this transaction.
    """
    if not connection.in_transaction:
        raise OperationError("账号新增需要已有事务")
    rows = _identity_rows({"platforms": [dict(identity)]})
    if connection.execute(
        "SELECT 1 FROM account_platform_identities WHERE platform=? AND uid=?",
        (rows[0]["platform"], rows[0]["uid"]),
    ).fetchone() is not None:
        raise OperationError("该平台身份已存在，不能重复建档")
    return _save_account(
        {"platforms": rows, "enabled": False}, db_path=DEFAULT_DB,
        connection=connection,
    )


def update_account(
    account_id: int,
    value: Mapping[str, Any],
    *,
    db_path: Path = DEFAULT_DB,
    actor: str = "operator",
    reason: str = "local account enabled-state update",
    activation_id: int | None = None,
) -> Dict[str, Any]:
    allowed = {"phone", "operator_name", "account_type", "content_direction", "enabled"}
    if set(value) - allowed:
        raise OperationError("本地只能编辑手机号、运营信息和采集开关；身份请在矩阵通维护")
    return _save_account(
        value,
        db_path=db_path,
        target_account_id=account_id,
        state_actor=actor,
        state_reason=reason,
        state_activation_id=activation_id,
    )


def update_account_in_transaction(
    connection: sqlite3.Connection,
    account_id: int,
    value: Mapping[str, Any],
    *,
    actor: str = "operator",
    reason: str = "local account enabled-state update",
    activation_id: int | None = None,
) -> Dict[str, Any]:
    """Apply ordinary local edits using the caller's account/roster transaction."""

    allowed = {"phone", "operator_name", "account_type", "content_direction", "enabled"}
    if set(value) - allowed:
        raise OperationError("本地只能编辑手机号、运营信息和采集开关；身份请在矩阵通维护")
    return _save_account(
        value,
        db_path=DEFAULT_DB,
        target_account_id=account_id,
        state_actor=actor,
        state_reason=reason,
        state_activation_id=activation_id,
        connection=connection,
    )


def import_accounts(
    rows: Sequence[Mapping[str, Any]], *, source_name: str, db_path: Path = DEFAULT_DB
) -> Dict[str, Any]:
    batch_id = f"account-{uuid.uuid4().hex}"
    captured_at = now_utc()
    normalized_keys: List[Optional[str]] = []
    last_by_key: Dict[str, int] = {}
    expanded_rows = [_expand_flat_account_row(row) for row in rows]
    for index, row in enumerate(expanded_rows, start=1):
        try:
            normalize_phone(row.get("phone"))
            identities = _identity_rows(row)
            key = (
                f"account:{int(row['id'])}"
                if row.get("id") is not None
                else f"identity:{identities[0]['platform']}:{identities[0]['uid']}"
                if identities else None
            )
        except OperationError:
            key = None
        normalized_keys.append(key)
        if key:
            last_by_key[key] = index
    with connect(db_path) as connection, transaction(connection):
        connection.execute(
            """
            INSERT INTO import_batches(id, entity_type, source_name, status, total_rows, created_at)
            VALUES (?, 'account', ?, 'previewed', ?, ?)
            """,
            (batch_id, source_name, len(rows), captured_at),
        )
    counts = CounterResult()
    for index, row in enumerate(expanded_rows, start=1):
        key = normalized_keys[index - 1]
        if key and last_by_key[key] != index:
            status, entity_id, reason = (
                "duplicate_in_file",
                None,
                "同文件后续行覆盖本行",
            )
            counts.rejected += 1
        else:
            try:
                result = upsert_account(row, db_path=db_path)
                status = str(result["action"])
                entity_id = int(result["id"])
                reason = ""
                counts.inserted += int(status == "inserted")
                counts.updated += int(status == "updated")
            except Exception as exc:
                status, entity_id, reason = "rejected", None, str(exc)
                counts.rejected += 1
        with connect(db_path) as connection, transaction(connection):
            connection.execute(
                """
                INSERT INTO import_rows(
                    batch_id, source_row, status, entity_id, identity_key, raw_json, reason
                ) VALUES (?, ?, ?, ?, ?, ?, ?)
                """,
                (
                    batch_id,
                    index,
                    status,
                    entity_id,
                    key or "",
                    json.dumps(dict(row), ensure_ascii=False),
                    reason[:1000],
                ),
            )
    with connect(db_path) as connection, transaction(connection):
        connection.execute(
            """
            UPDATE import_batches SET status='committed', inserted_rows=?, updated_rows=?,
                rejected_rows=?, committed_at=? WHERE id=?
            """,
            (counts.inserted, counts.updated, counts.rejected, now_utc(), batch_id),
        )
    return {"batch_id": batch_id, **counts.as_dict()}


class CounterResult:
    def __init__(self) -> None:
        self.inserted = 0
        self.updated = 0
        self.rejected = 0

    def as_dict(self) -> Dict[str, int]:
        return {
            "inserted_rows": self.inserted,
            "updated_rows": self.updated,
            "rejected_rows": self.rejected,
        }


def _account_for_uid(connection, platform: str, uid: str) -> Optional[int]:
    if not uid:
        return None
    row = connection.execute(
        "SELECT account_id FROM account_platform_identities WHERE platform=? AND uid=?",
        (platform, uid),
    ).fetchone()
    return int(row["account_id"]) if row else None


def reconcile_content_account_identity(
    connection: sqlite3.Connection, content_id: int,
) -> Optional[int]:
    """Associate a content row by its exact platform/UID, without a pending list."""
    content = connection.execute(
        "SELECT platform,raw_account_uid FROM content_items WHERE id=?", (content_id,)
    ).fetchone()
    if content is None:
        raise OperationError("内容不存在")
    account_id = _account_for_uid(connection, str(content["platform"]),
                                  str(content["raw_account_uid"] or "").strip())
    connection.execute("UPDATE content_items SET account_id=? WHERE id=?", (account_id, content_id))
    return account_id


def _merge_unique_children(connection, table: str, survivor: int, loser: int) -> None:
    rows = connection.execute(
        f"SELECT id FROM {table} WHERE content_id=?", (loser,)
    ).fetchall()
    for row in rows:
        connection.execute(
            f"UPDATE {table} SET content_id=? WHERE id=?", (survivor, row["id"])
        )


_PROTECTED_CONTENT_HISTORY_TABLES = (
    "evaluation_versions",
    "task_contents",
)


def _protected_content_history(
    connection: sqlite3.Connection, content_id: int
) -> Dict[str, int]:
    return {
        table: int(
            connection.execute(
                f"SELECT COUNT(*) FROM {table} WHERE content_id=?", (content_id,)
            ).fetchone()[0]
        )
        for table in _PROTECTED_CONTENT_HISTORY_TABLES
    }


def _record_identity_conflict(
    connection: sqlite3.Connection,
    original: sqlite3.Row,
    duplicate: sqlite3.Row,
    *,
    histories: Mapping[int, Mapping[str, int]],
    reason: str,
) -> None:
    evidence = {
        "error_code": IdentityConflictError.error_code,
        "reason": reason,
        "contents": [
            {
                "id": int(row["id"]),
                "link_id": str(row["link_id"]),
                "platform": str(row["platform"]),
                "platform_content_id": row["platform_content_id"],
                "canonical_url": str(row["canonical_url"]),
                "protected_history": dict(histories[int(row["id"])]),
            }
            for row in (original, duplicate)
        ],
    }
    connection.execute(
        """
        INSERT INTO duplicate_relations(
            duplicate_content_id,original_content_id,method,confidence,
            evidence_json,status,created_at
        ) VALUES (?,?,'identity_conflict',1.0,?,'pending_review',?)
        ON CONFLICT(duplicate_content_id,original_content_id,method) DO UPDATE SET
            status='pending_review'
        """,
        (
            duplicate["id"],
            original["id"],
            json.dumps(
                evidence,
                ensure_ascii=False,
                sort_keys=True,
                separators=(",", ":"),
            ),
            now_utc(),
        ),
    )


def _raise_identity_conflict(
    connection: sqlite3.Connection,
    original: sqlite3.Row,
    duplicate: sqlite3.Row,
    *,
    histories: Mapping[int, Mapping[str, int]],
    reason: str,
) -> None:
    _record_identity_conflict(
        connection,
        original,
        duplicate,
        histories=histories,
        reason=reason,
    )
    raise IdentityConflictError(
        "identity_conflict: 内容身份冲突，已记录重复内容关系，请在内容页重复提醒处处理"
    )


def merge_content_records(connection, first_id: int, second_id: int) -> int:
    schema20 = connection.execute("PRAGMA user_version").fetchone()[0] == 20
    if schema20:
        from .metric_field_facts import resolve_metric_content_id
        first_id = resolve_metric_content_id(connection, first_id)
        second_id = resolve_metric_content_id(connection, second_id)
        if first_id == second_id:
            return first_id
    rows = connection.execute(
        "SELECT * FROM content_items WHERE id IN (?,?) ORDER BY created_at, id",
        (first_id, second_id),
    ).fetchall()
    if len(rows) != 2:
        raise OperationError("需要合并的内容记录不存在")
    original, duplicate = rows[0], rows[1]
    histories = {
        int(row["id"]): _protected_content_history(connection, int(row["id"]))
        for row in rows
    }
    has_history = {
        content_id: any(count > 0 for count in counts.values())
        for content_id, counts in histories.items()
    }
    existing_conflict = connection.execute(
        """
        SELECT 1 FROM duplicate_relations
        WHERE status='pending_review'
          AND ((duplicate_content_id=? AND original_content_id=?)
            OR (duplicate_content_id=? AND original_content_id=?))
        """,
        (first_id, second_id, second_id, first_id),
    ).fetchone()
    if existing_conflict is not None:
        _raise_identity_conflict(
            connection,
            original,
            duplicate,
            histories=histories,
            reason="existing_pending_identity_conflict",
        )
    if has_history[int(original["id"])] and has_history[int(duplicate["id"])]:
        _raise_identity_conflict(
            connection,
            original,
            duplicate,
            histories=histories,
            reason="both_contents_have_protected_history",
        )
    survivor, loser = (
        (duplicate, original)
        if has_history[int(duplicate["id"])]
        else (original, duplicate)
    )
    survivor_id, loser_id = int(survivor["id"]), int(loser["id"])
    if any(histories[loser_id].values()):
        raise RuntimeError("identity merge selected a protected-history loser")

    if schema20:
        return _merge_content_records_schema20(connection, survivor, loser, histories=histories)

    connection.execute("SAVEPOINT merge_content_records")
    try:
        connection.execute(
            "UPDATE content_aliases SET content_id=? WHERE content_id=?",
            (survivor_id, loser_id),
        )
        connection.execute(
            """
            INSERT INTO content_aliases(alias_link_id, content_id, reason, created_at)
            VALUES (?, ?, 'identity_upgrade_merge', ?)
            ON CONFLICT(alias_link_id) DO UPDATE SET content_id=excluded.content_id
            """,
            (loser["link_id"], survivor_id, now_utc()),
        )
        identities = connection.execute(
            "SELECT * FROM content_identities WHERE content_id=?", (loser_id,)
        ).fetchall()
        for identity in identities:
            connection.execute(
                """
                INSERT INTO content_identities(
                    content_id, identity_kind, identity_value, platform_identity_key,
                    is_primary, created_at
                ) VALUES (?, ?, ?, ?, ?, ?)
                ON CONFLICT(platform_identity_key) DO NOTHING
                """,
                (
                    survivor_id,
                    identity["identity_kind"],
                    identity["identity_value"],
                    identity["platform_identity_key"],
                    identity["is_primary"],
                    identity["created_at"],
                ),
            )
        connection.execute(
            "DELETE FROM content_identities WHERE content_id=?", (loser_id,)
        )
        for table in CONTENT_CHILD_REKEY_TABLES:
            _merge_unique_children(connection, table, survivor_id, loser_id)
        merge_metric_snapshots(connection, survivor_id=survivor_id, loser_id=loser_id)
        score_rows = connection.execute(
            "SELECT * FROM comment_user_scores WHERE content_id=?", (loser_id,)
        ).fetchall()
        for row in score_rows:
            connection.execute(
                """
                INSERT INTO comment_user_scores(
                    content_id,evidence_version_id,anonymous_user_key,
                    audience_automotive_score,action_intent_score,evaluated_at
                ) VALUES (?,?,?,?,?,?)
                ON CONFLICT(content_id,anonymous_user_key) DO UPDATE SET
                    evidence_version_id=excluded.evidence_version_id,
                    audience_automotive_score=excluded.audience_automotive_score,
                    action_intent_score=excluded.action_intent_score,
                    evaluated_at=excluded.evaluated_at
                """,
                (
                    survivor_id,
                    row["evidence_version_id"],
                    row["anonymous_user_key"],
                    row["audience_automotive_score"],
                    row["action_intent_score"],
                    row["evaluated_at"],
                ),
            )
        connection.execute(
            "DELETE FROM comment_user_scores WHERE content_id=?", (loser_id,)
        )
        connection.execute(
            """
            DELETE FROM duplicate_relations
            WHERE duplicate_content_id IN (?,?) AND original_content_id IN (?,?)
            """,
            (survivor_id, loser_id, survivor_id, loser_id),
        )
        for column in ("duplicate_content_id", "original_content_id"):
            relations = connection.execute(
                f"SELECT id FROM duplicate_relations WHERE {column}=?", (loser_id,)
            ).fetchall()
            for relation in relations:
                connection.execute(
                    f"UPDATE duplicate_relations SET {column}=? WHERE id=?",
                    (survivor_id, relation["id"]),
                )
        connection.execute("DELETE FROM content_items WHERE id=?", (loser_id,))
    except sqlite3.IntegrityError as exc:
        connection.execute("ROLLBACK TO merge_content_records")
        connection.execute("RELEASE merge_content_records")
        _record_identity_conflict(
            connection,
            original,
            duplicate,
            histories=histories,
            reason=f"merge_constraint_conflict:{type(exc).__name__}",
        )
        raise IdentityConflictError(
            "identity_conflict: 内容身份合并无法无损完成，已记录重复内容关系，请在内容页重复提醒处处理"
        ) from exc
    except Exception:
        connection.execute("ROLLBACK TO merge_content_records")
        connection.execute("RELEASE merge_content_records")
        raise
    else:
        connection.execute("RELEASE merge_content_records")
        return survivor_id


def _merge_content_records_schema20(
    connection: sqlite3.Connection, survivor: sqlite3.Row, loser: sqlite3.Row,
    *, histories: Mapping[int, Mapping[str, int]],
) -> int:
    """Append an alias while retaining immutable evidence on original IDs."""
    from .metric_field_facts import append_identity_merge, project_content, resolve_metric_content_id
    survivor_id, loser_id = int(survivor["id"]), int(loser["id"])
    if survivor["platform"] != loser["platform"]:
        raise OperationError("不能合并不同平台的内容")
    stamp = now_utc()
    identities = [dict(row) for row in connection.execute(
        "SELECT * FROM content_identities WHERE content_id IN (?,?) ORDER BY id", (survivor_id, loser_id),
    )]
    connection.execute("SAVEPOINT merge_content_records_v20")
    try:
        event_id = append_identity_merge(
            connection, winner_id=survivor_id, loser_id=loser_id, recorded_at=stamp,
            identity_snapshot={"winner": dict(survivor), "loser": dict(loser), "identities": identities},
        )
        connection.execute(
            """INSERT INTO content_aliases(alias_link_id,content_id,reason,created_at)
            VALUES (?,?,'identity_upgrade_merge',?) ON CONFLICT(alias_link_id) DO NOTHING""",
            (loser["link_id"], survivor_id, stamp),
        )
        alias = connection.execute("SELECT content_id FROM content_aliases WHERE alias_link_id=?", (loser["link_id"],)).fetchone()
        if alias is None or resolve_metric_content_id(connection, int(alias[0]), knowledge_at=stamp) != survivor_id:
            raise ValueError("content alias conflicts with merge event")
        # Identity payload and created_at remain immutable. The materialized
        # primary lookup may move; the event preserves both pre-merge rows.
        connection.execute("UPDATE content_identities SET content_id=?,is_primary=0 WHERE content_id=?",
                           (survivor_id, loser_id))
        for row in (survivor, loser):
            aliases = [("canonical_url", str(row["canonical_url"]),
                        f"{row['platform']}:url:{hashlib.sha256(normalize_url(row['canonical_url']).encode()).hexdigest()}")]
            if row["platform_content_id"] is not None:
                aliases.append(("platform_content_id", str(row["platform_content_id"]),
                                f"{row['platform']}:{row['platform_content_id']}"))
            for kind, value, key in aliases:
                connection.execute(
                    """INSERT INTO content_identities(content_id,identity_kind,identity_value,
                    platform_identity_key,is_primary,created_at) VALUES (?,?,?,?,0,?)
                    ON CONFLICT(platform_identity_key) DO NOTHING""",
                    (survivor_id, kind, value, key, stamp),
                )
        connection.execute("UPDATE content_items SET platform_content_id=NULL,normalized_url_hash=NULL WHERE id=?", (loser_id,))
        connection.execute("""UPDATE content_items SET platform_content_id=COALESCE(platform_content_id,?),
                            normalized_url_hash=COALESCE(normalized_url_hash,?) WHERE id=?""",
                           (loser["platform_content_id"], loser["normalized_url_hash"], survivor_id))
        connection.execute(
            """INSERT INTO duplicate_relations(duplicate_content_id,original_content_id,method,
            confidence,evidence_json,status,created_at) VALUES (?,?,'identity_merge',1.0,?,'confirmed',?)""",
            (loser_id, survivor_id, json.dumps({"merge_event_id": event_id}, sort_keys=True), stamp),
        )
        project_content(connection, survivor_id, cutoff_at=stamp)
    except Exception as exc:
        connection.execute("ROLLBACK TO merge_content_records_v20")
        connection.execute("RELEASE merge_content_records_v20")
        _record_identity_conflict(connection, survivor, loser, histories=histories,
                                  reason="schema20_merge_evidence_failed")
        raise IdentityConflictError("identity_conflict: 内容合并证据事务失败") from exc
    connection.execute("RELEASE merge_content_records_v20")
    return survivor_id


def _text_sha256(title: Any, body: Any) -> Optional[str]:
    normalized = " ".join(f"{title or ''}\n{body or ''}".lower().split())
    if len(normalized) < 12:
        return None
    return hashlib.sha256(normalized.encode("utf-8")).hexdigest()


def _rebuild_text_duplicate_groups(
    connection: sqlite3.Connection,
    fingerprints: set[str],
    *,
    touched_content_ids: set[int],
) -> None:
    groups: Dict[str, List[sqlite3.Row]] = {
        fingerprint: [] for fingerprint in fingerprints
    }
    if groups:
        candidates = connection.execute(
            """
            SELECT id,title,body,published_at,imported_at
            FROM content_items ORDER BY id
            """
        ).fetchall()
        for row in candidates:
            fingerprint = _text_sha256(row["title"], row["body"])
            if fingerprint in groups:
                groups[fingerprint].append(row)

    affected_ids = set(touched_content_ids)
    for group in groups.values():
        affected_ids.update(int(row["id"]) for row in group)
    if affected_ids:
        ordered_ids = sorted(affected_ids)
        placeholders = ",".join("?" for _ in ordered_ids)
        connection.execute(
            f"""
            DELETE FROM duplicate_relations
            WHERE method='text_sha256'
              AND (duplicate_content_id IN ({placeholders})
                   OR original_content_id IN ({placeholders}))
            """,
            (*ordered_ids, *ordered_ids),
        )

    for fingerprint in sorted(groups):
        group = groups[fingerprint]
        if len(group) < 2:
            continue
        ordered = sorted(
            group,
            key=lambda row: (
                row["published_at"] or row["imported_at"],
                row["id"],
            ),
        )
        original = int(ordered[0]["id"])
        for row in ordered[1:]:
            connection.execute(
                """
                INSERT INTO duplicate_relations(
                    duplicate_content_id, original_content_id, method, confidence,
                    evidence_json, status, created_at
                ) VALUES (?, ?, 'text_sha256', 1.0, ?, 'confirmed', ?)
                """,
                (
                    row["id"],
                    original,
                    json.dumps({"sha256": fingerprint}, ensure_ascii=False),
                    now_utc(),
                ),
            )


def upsert_content(
    value: Mapping[str, Any],
    *,
    db_path: Path = DEFAULT_DB,
    source_group_on_insert: str = "",
    connection: Optional[sqlite3.Connection] = None,
) -> Dict[str, Any]:
    if source_group_on_insert not in {"", "history-archive", "history-backfill"}:
        raise OperationError("新内容内部来源分组无效")
    platform = str(value.get("platform") or "")
    identity = content_identity(
        platform,
        str(value.get("canonical_url") or value.get("url") or ""),
        value.get("platform_content_id"),
    )
    published_raw = value.get("published_at")
    published = (
        normalize_timestamp(published_raw) if published_raw not in (None, "") else None
    )
    if published_raw not in (None, "") and published is None:
        raise OperationError("发布日期必须是 ISO 时间或 Unix 秒")
    account_type = _enum(value.get("account_type"), ACCOUNT_TYPES, "account_type")
    direction = _enum(value.get("content_direction"), DIRECTIONS, "content_direction")
    content_type = str(value.get("content_type") or "unknown").strip() or "unknown"
    uid = str(value.get("account_uid") or "").strip()
    captured_at = now_utc()
    with _content_write_transaction(db_path, connection=connection) as connection:
        by_id = None
        if identity["platform_content_id"]:
            by_id = connection.execute(
                "SELECT * FROM content_items WHERE platform=? AND platform_content_id=?",
                (platform, identity["platform_content_id"]),
            ).fetchone()
        by_url = connection.execute(
            "SELECT * FROM content_items WHERE platform=? AND normalized_url_hash=?",
            (platform, identity["normalized_url_hash"]),
        ).fetchone()
        if connection.execute("PRAGMA user_version").fetchone()[0] == 20:
            from .metric_field_facts import resolve_metric_content_id
            identity_match = connection.execute(
                "SELECT content_id FROM content_identities WHERE platform_identity_key=?", (identity["identity_key"],),
            ).fetchone()
            if by_id is None and identity_match is not None:
                by_id = connection.execute("SELECT * FROM content_items WHERE id=?", (
                    resolve_metric_content_id(connection, int(identity_match[0])),
                )).fetchone()
            if by_id is not None:
                by_id = connection.execute("SELECT * FROM content_items WHERE id=?", (
                    resolve_metric_content_id(connection, int(by_id["id"])),
                )).fetchone()
            if by_url is not None:
                by_url = connection.execute("SELECT * FROM content_items WHERE id=?", (
                    resolve_metric_content_id(connection, int(by_url["id"])),
                )).fetchone()
        affected_text_ids = {
            int(row["id"]) for row in (by_id, by_url) if row is not None
        }
        affected_text_fingerprints = {
            fingerprint
            for row in (by_id, by_url)
            if row is not None
            for fingerprint in (_text_sha256(row["title"], row["body"]),)
            if fingerprint is not None
        }
        if (
            by_id is not None
            and by_url is not None
            and int(by_id["id"]) != int(by_url["id"])
        ):
            content_id = merge_content_records(
                connection, int(by_id["id"]), int(by_url["id"])
            )
            current = connection.execute(
                "SELECT * FROM content_items WHERE id=?", (content_id,)
            ).fetchone()
        else:
            current = by_id or by_url
        if current is None:
            effective_uid = uid
            account_id = _account_for_uid(connection, platform, effective_uid)
            link_id = generate_link_id(connection, str(identity["identity_key"]))
            cursor = connection.execute(
                """
                INSERT INTO content_items(
                    link_id, platform, platform_content_id, canonical_url, normalized_url_hash,
                    account_id, raw_account_uid, raw_account_name, legacy_account_type,
                    title, body, content_type, published_at, published_at_raw,
                    manual_content_direction, source_group,
                    imported_at, created_at, updated_at
                ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                """,
                (
                    link_id,
                    platform,
                    identity["platform_content_id"],
                    identity["canonical_url"],
                    identity["normalized_url_hash"],
                    account_id,
                    effective_uid or None,
                    str(value.get("account_name") or "").strip() or None,
                    account_type,
                    str(value.get("title") or "").strip(),
                    str(value.get("body") or "").strip(),
                    content_type,
                    published,
                    str(published_raw) if published_raw not in (None, "") else None,
                    None if direction == "unknown" else direction,
                    source_group_on_insert,
                    captured_at,
                    captured_at,
                    captured_at,
                ),
            )
            if cursor.lastrowid is None:
                raise RuntimeError("content insert returned no id")
            content_id = int(cursor.lastrowid)
            action = "inserted"
        else:
            content_id = int(current["id"])
            preserve_existing_content_fields = bool(
                value.get("_preserve_existing_content_fields")
            )
            incoming_title = str(value.get("title") or "").strip()
            incoming_body = str(value.get("body") or "").strip()
            effective_title = (
                str(current["title"] or "").strip() or incoming_title
                if preserve_existing_content_fields
                else incoming_title
            )
            effective_body = (
                str(current["body"] or "").strip() or incoming_body
                if preserve_existing_content_fields
                else incoming_body
            )
            effective_published = (
                current["published_at"] or published
                if preserve_existing_content_fields
                else published
            )
            effective_published_raw = (
                current["published_at_raw"]
                or (
                    str(published_raw)
                    if published_raw not in (None, "")
                    else None
                )
                if preserve_existing_content_fields
                else (
                    str(published_raw)
                    if published_raw not in (None, "")
                    else None
                )
            )
            effective_uid = uid or str(current["raw_account_uid"] or "").strip()
            account_id = _account_for_uid(connection, platform, effective_uid)
            effective_account_type = (
                str(current["legacy_account_type"] or "unknown")
                if account_type == "unknown"
                else account_type
            )
            effective_content_type = (
                str(current["content_type"] or "unknown")
                if content_type == "unknown"
                else content_type
            )
            effective_direction = (
                current["manual_content_direction"]
                if direction == "unknown"
                else direction
            )
            connection.execute(
                """
                UPDATE content_items SET platform_content_id=?, canonical_url=?,
                    normalized_url_hash=?, account_id=?,
                    raw_account_uid=?, raw_account_name=?, legacy_account_type=?,
                    title=?, body=?, content_type=?, published_at=?, published_at_raw=?,
                    manual_content_direction=?, updated_at=? WHERE id=?
                """,
                (
                    identity["platform_content_id"],
                    identity["canonical_url"],
                    identity["normalized_url_hash"],
                    account_id,
                    effective_uid or None,
                    str(
                        value.get("account_name") or current["raw_account_name"] or ""
                    ).strip()
                    or None,
                    effective_account_type,
                    effective_title,
                    effective_body,
                    effective_content_type,
                    effective_published,
                    effective_published_raw,
                    effective_direction,
                    captured_at,
                    content_id,
                ),
            )
            action = "updated"
        connection.execute(
            "UPDATE content_identities SET is_primary=0 WHERE content_id=?",
            (content_id,),
        )
        kind = (
            "platform_content_id"
            if identity["platform_content_id"]
            else "canonical_url"
        )
        identity_value = str(
            identity["platform_content_id"] or identity["canonical_url"]
        )
        connection.execute(
            """
            INSERT INTO content_identities(
                content_id, identity_kind, identity_value, platform_identity_key,
                is_primary, created_at
            ) VALUES (?, ?, ?, ?, 1, ?)
            ON CONFLICT(platform_identity_key) DO UPDATE SET content_id=excluded.content_id, is_primary=1
            """,
            (content_id, kind, identity_value, identity["identity_key"], captured_at),
        )
        updated_content = connection.execute(
            "SELECT title,body FROM content_items WHERE id=?", (content_id,)
        ).fetchone()
        if updated_content is None:
            raise RuntimeError("content upsert target disappeared")
        if current is not None:
            from .duplicates import _normalize_text

            old_text = _normalize_text(f"{current['title']}\n{current['body']}")
            new_text = _normalize_text(f"{updated_content['title']}\n{updated_content['body']}")
            if old_text != new_text:
                connection.execute(
                    """
                    DELETE FROM duplicate_relations
                    WHERE method='fingerprint_v1'
                      AND (duplicate_content_id=? OR original_content_id=?)
                    """,
                    (content_id, content_id),
                )
        new_text_fingerprint = _text_sha256(
            updated_content["title"], updated_content["body"]
        )
        if new_text_fingerprint is not None:
            affected_text_fingerprints.add(new_text_fingerprint)
        affected_text_ids.add(content_id)
        if (current is None or len(affected_text_ids) > 1
                or _text_sha256(current["title"], current["body"]) != new_text_fingerprint
                or current["published_at"] != effective_published):
            _rebuild_text_duplicate_groups(
                connection,
                affected_text_fingerprints,
                touched_content_ids=affected_text_ids,
            )
        reconcile_content_account_identity(connection, content_id)
    return {"id": content_id, "action": action}


def normalize_unknown_content_directions(*, db_path: Path) -> Dict[str, Any]:
    """Replace the legacy unknown sentinel with NULL without touching row timestamps."""

    if not db_path.is_file():
        raise OperationError(f"数据库不存在：{db_path}")

    def distribution(connection: sqlite3.Connection) -> Dict[str, int]:
        return {
            str(row["direction"]): int(row["count"])
            for row in connection.execute(
                """
                SELECT COALESCE(manual_content_direction,'null') direction,
                       COUNT(*) count
                FROM content_items
                GROUP BY COALESCE(manual_content_direction,'null')
                ORDER BY direction
                """
            )
        }

    with connect(db_path) as connection, transaction(connection):
        before = distribution(connection)
        total_rows = int(
            connection.execute("SELECT COUNT(*) FROM content_items").fetchone()[0]
        )
        cursor = connection.execute(
            """
            UPDATE content_items SET manual_content_direction=NULL
            WHERE manual_content_direction='unknown'
            """
        )
        after = distribution(connection)
    if sum(before.values()) != total_rows or sum(after.values()) != total_rows:
        raise RuntimeError("content direction normalization changed the row count")
    return {
        "total_rows": total_rows,
        "updated_rows": cursor.rowcount,
        "before": before,
        "after": after,
    }


def update_content(
    content_id: int, value: Mapping[str, Any], *, db_path: Path = DEFAULT_DB
) -> Dict[str, Any]:
    updates = dict(value)
    unexpected = set(updates) - CONTENT_PATCH_FIELDS
    if unexpected:
        raise OperationError(f"不支持修改的内容字段：{sorted(unexpected)}")
    if not updates:
        raise OperationError("至少提交一个要修改的字段")
    original_content_id = content_id
    captured_at = now_utc()
    with _content_write_transaction(db_path) as connection:
        if connection.execute("PRAGMA user_version").fetchone()[0] == 20:
            from .metric_field_facts import resolve_metric_content_id
            content_id = resolve_metric_content_id(connection, content_id)
        row = connection.execute(
            "SELECT * FROM content_items WHERE id=?", (content_id,)
        ).fetchone()
        if row is None:
            raise OperationError("内容不存在")
        affected_text_ids = {content_id}
        affected_text_fingerprints = {
            fingerprint
            for fingerprint in (_text_sha256(row["title"], row["body"]),)
            if fingerprint is not None
        }
        identity_fields = {"platform", "platform_content_id", "canonical_url"}
        identity = None
        identity_merged = False
        if identity_fields & updates.keys():
            identity = content_identity(
                str(updates.get("platform", row["platform"]) or ""),
                str(updates.get("canonical_url", row["canonical_url"]) or ""),
                updates.get("platform_content_id", row["platform_content_id"]),
            )
            candidate_ids: set[int] = set()
            if identity["platform_content_id"]:
                by_id = connection.execute(
                    """
                    SELECT id FROM content_items
                    WHERE platform=? AND platform_content_id=?
                    """,
                    (identity["platform"], identity["platform_content_id"]),
                ).fetchone()
                if by_id is not None:
                    candidate_ids.add(int(by_id["id"]))
            by_url = connection.execute(
                """
                SELECT id FROM content_items
                WHERE platform=? AND normalized_url_hash=?
                """,
                (identity["platform"], identity["normalized_url_hash"]),
            ).fetchone()
            if by_url is not None:
                candidate_ids.add(int(by_url["id"]))
            external_candidate_ids = sorted(candidate_ids - {content_id})
            if len(external_candidate_ids) > 1:
                for candidate_id in external_candidate_ids:
                    candidate = connection.execute(
                        "SELECT * FROM content_items WHERE id=?", (candidate_id,)
                    ).fetchone()
                    if candidate is None:
                        raise RuntimeError("identity candidate disappeared")
                    original, duplicate = sorted(
                        (row, candidate),
                        key=lambda value: (value["created_at"], value["id"]),
                    )
                    histories = {
                        int(value["id"]): _protected_content_history(
                            connection, int(value["id"])
                        )
                        for value in (original, duplicate)
                    }
                    _record_identity_conflict(
                        connection,
                        original,
                        duplicate,
                        histories=histories,
                        reason="multiple_identity_candidates",
                    )
                raise IdentityConflictError(
                    "identity_conflict: 平台内容 ID 与链接命中不同内容，已记录重复内容关系，请在内容页重复提醒处处理"
                )
            for candidate_id in external_candidate_ids:
                affected_text_ids.add(candidate_id)
                candidate = connection.execute(
                    """
                    SELECT platform,raw_account_uid,title,body
                    FROM content_items WHERE id=?
                    """,
                    (candidate_id,),
                ).fetchone()
                if candidate is not None:
                    candidate_fingerprint = _text_sha256(
                        candidate["title"], candidate["body"]
                    )
                    if candidate_fingerprint is not None:
                        affected_text_fingerprints.add(candidate_fingerprint)
                content_id = merge_content_records(connection, content_id, candidate_id)
                identity_merged = True
            row = connection.execute(
                "SELECT * FROM content_items WHERE id=?", (content_id,)
            ).fetchone()
            if row is None:
                raise RuntimeError("identity merge survivor disappeared")

        columns: Dict[str, Any] = {}
        if identity is not None:
            columns.update(
                {
                    "platform": identity["platform"],
                    "platform_content_id": identity["platform_content_id"],
                    "canonical_url": identity["canonical_url"],
                    "normalized_url_hash": identity["normalized_url_hash"],
                }
            )
        if "published_at" in updates:
            published_raw = updates["published_at"]
            published = (
                normalize_timestamp(published_raw)
                if published_raw not in (None, "")
                else None
            )
            if published_raw not in (None, "") and published is None:
                raise OperationError("发布日期必须是 ISO 时间或 Unix 秒")
            columns["published_at"] = published
            columns["published_at_raw"] = (
                str(published_raw) if published_raw not in (None, "") else None
            )
        for request_field, column in (
            ("title", "title"),
            ("body", "body"),
            ("account_uid", "raw_account_uid"),
            ("account_name", "raw_account_name"),
        ):
            if request_field in updates:
                normalized = str(updates[request_field] or "").strip()
                columns[column] = (
                    normalized
                    if request_field in {"title", "body"}
                    else normalized or None
                )
        if "content_type" in updates:
            columns["content_type"] = (
                str(updates["content_type"] or "unknown").strip() or "unknown"
            )
        if "account_type" in updates:
            columns["legacy_account_type"] = _enum(
                updates["account_type"], ACCOUNT_TYPES, "account_type"
            )
        if "content_direction" in updates:
            direction = _enum(
                updates["content_direction"], DIRECTIONS, "content_direction"
            )
            columns["manual_content_direction"] = (
                None if direction == "unknown" else direction
            )

        assignments = [f"{column}=?" for column in columns]
        assignments.append("updated_at=?")
        connection.execute(
            f"UPDATE content_items SET {', '.join(assignments)} WHERE id=?",
            (*columns.values(), captured_at, content_id),
        )
        if identity is not None:
            connection.execute(
                "UPDATE content_identities SET is_primary=0 WHERE content_id=?",
                (content_id,),
            )
            kind = (
                "platform_content_id"
                if identity["platform_content_id"]
                else "canonical_url"
            )
            identity_value = str(
                identity["platform_content_id"] or identity["canonical_url"]
            )
            connection.execute(
                """
                INSERT INTO content_identities(
                    content_id,identity_kind,identity_value,platform_identity_key,
                    is_primary,created_at
                ) VALUES (?,?,?,?,1,?)
                ON CONFLICT(platform_identity_key) DO UPDATE SET
                    content_id=excluded.content_id,is_primary=1
                """,
                (
                    content_id,
                    kind,
                    identity_value,
                    identity["identity_key"],
                    captured_at,
                ),
            )
        if {"title", "body"} & updates.keys():
            connection.execute(
                """
                DELETE FROM duplicate_relations
                WHERE method='fingerprint_v1'
                  AND (duplicate_content_id=? OR original_content_id=?)
                """,
                (content_id, content_id),
            )
        if {"title", "body", "published_at"} & updates.keys() or identity_merged:
            updated_content = connection.execute(
                "SELECT title,body FROM content_items WHERE id=?", (content_id,)
            ).fetchone()
            if updated_content is None:
                raise RuntimeError("content update target disappeared")
            new_text_fingerprint = _text_sha256(
                updated_content["title"], updated_content["body"]
            )
            if new_text_fingerprint is not None:
                affected_text_fingerprints.add(new_text_fingerprint)
            affected_text_ids.add(content_id)
            _rebuild_text_duplicate_groups(
                connection,
                affected_text_fingerprints,
                touched_content_ids=affected_text_ids,
            )
        if {
            "platform",
            "account_uid",
            "account_name",
        } & updates.keys() or identity_merged:
            reconcile_content_account_identity(connection, content_id)
    result: Dict[str, Any] = {"id": content_id, "action": "updated"}
    if content_id != original_content_id:
        result["merged_from_id"] = original_content_id
    return result


def import_contents(
    rows: Sequence[Mapping[str, Any]], *, source_name: str, db_path: Path = DEFAULT_DB
) -> Dict[str, Any]:
    batch_id = f"content-{uuid.uuid4().hex}"
    captured_at = now_utc()
    keys: List[Optional[str]] = []
    last_by_key: Dict[str, int] = {}
    for index, row in enumerate(rows, start=1):
        try:
            identity = content_identity(
                str(row.get("platform") or ""),
                str(row.get("canonical_url") or row.get("url") or ""),
                row.get("platform_content_id"),
            )
            key = str(identity["identity_key"])
        except OperationError:
            key = None
        keys.append(key)
        if key:
            last_by_key[key] = index
    with connect(db_path) as connection, transaction(connection):
        connection.execute(
            """
            INSERT INTO import_batches(id, entity_type, source_name, status, total_rows, created_at)
            VALUES (?, 'content', ?, 'previewed', ?, ?)
            """,
            (batch_id, source_name, len(rows), captured_at),
        )
    counts = CounterResult()
    for index, row in enumerate(rows, start=1):
        key = keys[index - 1]
        if key and last_by_key[key] != index:
            status, entity_id, reason = (
                "duplicate_in_file",
                None,
                "同文件后续行覆盖本行",
            )
            counts.rejected += 1
        else:
            try:
                result = upsert_content(row, db_path=db_path)
                status, entity_id, reason = str(result["action"]), int(result["id"]), ""
                counts.inserted += int(status == "inserted")
                counts.updated += int(status == "updated")
            except Exception as exc:
                status, entity_id, reason = "rejected", None, str(exc)
                counts.rejected += 1
        with connect(db_path) as connection, transaction(connection):
            connection.execute(
                """
                INSERT INTO import_rows(
                    batch_id, source_row, status, entity_id, identity_key, raw_json, reason
                ) VALUES (?, ?, ?, ?, ?, ?, ?)
                """,
                (
                    batch_id,
                    index,
                    status,
                    entity_id,
                    key or "",
                    json.dumps(dict(row), ensure_ascii=False),
                    reason[:1000],
                ),
            )
    with connect(db_path) as connection, transaction(connection):
        connection.execute(
            """
            UPDATE import_batches SET status='committed', inserted_rows=?, updated_rows=?,
                rejected_rows=?, committed_at=? WHERE id=?
            """,
            (counts.inserted, counts.updated, counts.rejected, now_utc(), batch_id),
        )
    return {"batch_id": batch_id, **counts.as_dict()}



def account_status_predicate(
    status: str | None, frequencies: Mapping[int, str | None]
) -> tuple[str, list[Any]]:
    """Filter saved accounts by their manual state, independently of membership."""
    if status is None:
        return "1", []
    if status == "paused":
        return "a.enabled=0", []
    if status == "unmarked":
        marked = [account_id for account_id, value in frequencies.items() if value is not None]
        return "a.enabled=1 AND a.id NOT IN (SELECT value FROM json_each(?))", [json.dumps(marked)]
    if status in {"daily", "weekly"}:
        matching = [account_id for account_id, value in frequencies.items() if value == status]
        return "a.enabled=1 AND a.id IN (SELECT value FROM json_each(?))", [json.dumps(matching)]
    raise OperationError("账号状态必须是日更、周更、暂停或待标记")


def account_read_model(
    connection: sqlite3.Connection,
    account: Mapping[str, Any],
    *,
    roster: Mapping[str, Any] | None = None,
    update_frequencies: Mapping[int, str | None] | None = None,
    admission_members: Mapping[int, Mapping[str, Any]] | None = None,
) -> Dict[str, Any]:
    from .account_roster import account_metadata, runtime_account_summary
    from .account_metrics import select_account_metrics

    runtime = dict(roster or runtime_account_summary(connection))
    frequencies = (
        update_frequencies if update_frequencies is not None
        else load_update_frequencies(connection, [int(account["id"])])
    )
    frequency = frequencies.get(int(account["id"]))
    source_family = str(runtime.get("source_family") or "matrix")
    snapshot_id = runtime.get("snapshot_id") if runtime.get("ready") else None
    metadata = account_metadata(
        connection,
        int(account["id"]),
        source_family=source_family,
        snapshot_id=int(snapshot_id) if snapshot_id is not None else None,
        latest_if_unspecified=bool(runtime.get("ready")),
    )
    identities = [
        dict(row)
        for row in connection.execute(
            ACCOUNT_IDENTITY_STATS_SQL.replace(
                "AND c.platform=api.platform", f"AND c.platform=api.platform AND {canonical_content_predicate(connection)}",
            ), (account["id"],),
        )
    ]
    if len(identities) > 1:
        raise OperationError("账号模型尚未完成单平台升级，不能合并展示")
    statistics = select_account_metrics(connection, [int(row["id"]) for row in identities])
    admissions = admission_members if admission_members is not None else load_admission_members(connection)
    for identity in identities:
        admitted = admissions.get(int(identity["id"]), {})
        admission = admitted.get("member", {})
        if admitted and (admitted.get("account_id") != account["id"]
                         or admission.get("platform") != identity["platform"]
                         or admission.get("uid") != identity["uid"]):
            raise OperationError("账号主页资料与平台身份不一致，暂时无法读取")
        admitted_metadata = admission.get("metadata") or {}
        identity_metadata = dict(metadata)
        for field, admitted_value in (
            ("nickname", admission.get("nickname") or admitted_metadata.get("nickname")),
            ("profile_ref", admission.get("profile_ref")),
            ("unique_id", admitted_metadata.get("display_account_id")),
        ):
            if not identity_metadata.get(field) and admitted_value:
                identity_metadata[field] = admitted_value
        identity.update(statistics.get(int(identity["id"]), {}))
        if identity_metadata.get("nickname"):
            identity["nickname"] = identity_metadata["nickname"]
        identity.update({
            key: identity_metadata.get(key)
            for key in (
                "matrix_account_id", "profile_ref", "monitoring_status",
                "authorization_status", "avatar_url", "unique_id",
            )
        })
        statistic_metadata = identity.get("statistic_identity_metadata", {})
        for field in ("nickname", "avatar_url"):
            if statistic_metadata.get(field):
                identity[field] = statistic_metadata[field]
        if statistic_metadata.get("display_account_id"):
            identity["unique_id"] = statistic_metadata["display_account_id"]
    return {
        "id": account["id"], "phone": account["phone"],
        "operator_name": account["operator_name"], "account_type": account["account_type"],
        "content_direction": account["content_direction"], "enabled": bool(account["enabled"]),
        "platforms": identities, "updated_at": account["updated_at"],
        "account_status": account_operating_status({**dict(account), "update_frequency": frequency}),
        "update_frequency": frequency,
    }


def export_accounts_xlsx(
    *,
    douyin_authorization_targets: Optional[Sequence[tuple[int, str, str]]] = None,
    account_status: str | None = None,
    read_only: bool = False,
    db_path: Path = DEFAULT_DB,
) -> bytes:
    """\u5bfc\u51fa\u8d26\u53f7\u8868\u683c\uff08xlsx\uff09\uff0c\u6392\u5e8f\u4e0e\u8d26\u53f7\u9875\u4e00\u81f4\uff08\u6700\u8fd1\u66f4\u65b0\u5728\u524d\uff09\u3002

    douyin_authorization_targets \u662f\u524d\u7aef\u968f\u4e0b\u8f7d\u8bf7\u6c42\u5e26\u6765\u7684
    (account_id, platform_uid, state) \u7cbe\u786e\u76ee\u6807\uff0cstate \u4ec5\u5141\u8bb8
    authorized/needs_reauthorization\uff1bNone \u8868\u793a\u72b6\u6001\u4e0d\u53ef\u7528\uff0c\u7a7a\u96c6\u8868\u793a
    \u72b6\u6001\u53ef\u7528\u4f46\u6ca1\u6709\u6388\u6743\u76ee\u6807\u3002\u91cd\u590d\u76ee\u6807\u4ee5 needs_reauthorization \u4f18\u5148\u3002
    """

    authorization_targets: Optional[Dict[tuple[int, str], str]]
    if douyin_authorization_targets is None:
        authorization_targets = None
    else:
        authorization_targets = {}
        for account_id, platform_uid, state in douyin_authorization_targets:
            normalized_state = str(state)
            if normalized_state not in {"authorized", "needs_reauthorization"}:
                raise ValueError("unsupported Douyin authorization export state")
            target = (int(account_id), str(platform_uid))
            if (
                normalized_state == "needs_reauthorization"
                or target not in authorization_targets
            ):
                authorization_targets[target] = normalized_state
    accounts: List[Dict[str, Any]] = []
    with connect(db_path, read_only=read_only) as connection:
        from .account_roster import runtime_account_summary

        roster = runtime_account_summary(connection)
        frequencies = load_update_frequencies(connection)
        admissions = load_admission_members(connection)
        predicate, parameters = account_status_predicate(account_status, frequencies)
        account_rows = connection.execute(
            f"SELECT a.* FROM accounts a WHERE {predicate} ORDER BY a.updated_at DESC, a.id DESC",
            parameters,
        ).fetchall()
        for account in account_rows:
            accounts.append(account_read_model(connection, account, roster=roster,
                                               update_frequencies=frequencies, admission_members=admissions))
    return build_accounts_workbook(
        accounts,
        douyin_authorization_targets=authorization_targets,
        exported_at=now_utc(),
    )


def export_contents_csv(*, db_path: Path = DEFAULT_DB) -> bytes:
    from .statistics_scope import content_statistics_scope_sql

    output = io.StringIO()
    fields = [
        "link_id",
        "platform",
        "platform_content_id",
        "published_at",
        "canonical_url",
        "title",
        "account_uid",
        "account_name",
        "account_type",
        "content_direction",
        "primary_selling_point_code",
        "content_automotive_score",
        "evaluation_freshness",
        "view_count",
        "comment_count",
        "duplicate_original_link_id",
    ]
    writer = csv.DictWriter(output, fieldnames=fields)
    writer.writeheader()
    direction_sql = effective_direction_sql()
    with connect(db_path) as connection:
        active_release(connection)
        rows = connection.execute(
            f"""
            WITH {DISPLAY_EFFECTIVE_EVALUATIONS_CTE}
            SELECT c.*, COALESCE(a.account_type,c.legacy_account_type,'unknown') account_type,
                   {direction_sql} direction,
                   ev.primary_selling_point_code, ev.content_automotive_score,
                   COALESCE(ev.evaluation_freshness,'missing') evaluation_freshness,
                   original.link_id duplicate_original_link_id
            FROM content_items c LEFT JOIN accounts a ON a.id=c.account_id
            LEFT JOIN display_effective_evaluations ev ON ev.content_id=c.id
            LEFT JOIN duplicate_relations d ON d.id=(SELECT id FROM duplicate_relations WHERE duplicate_content_id=c.id AND status='confirmed' ORDER BY id LIMIT 1)
            LEFT JOIN content_items original ON original.id=d.original_content_id
            WHERE {canonical_content_predicate(connection)}
              AND {content_statistics_scope_sql("c")}
            ORDER BY c.id
            """
        ).fetchall()
        metrics = select_content_metrics(connection, [int(item["id"]) for item in rows])
        for item in rows:
            writer.writerow(
                {
                    field: formula_safe_csv_value(value)
                    for field, value in {
                        "link_id": item["link_id"],
                        "platform": item["platform"],
                        "platform_content_id": item["platform_content_id"],
                        "published_at": item["published_at"],
                        "canonical_url": item["canonical_url"],
                        "title": item["title"],
                        "account_uid": item["raw_account_uid"],
                        "account_name": item["raw_account_name"],
                        "account_type": item["account_type"],
                        "content_direction": item["direction"],
                        "primary_selling_point_code": item[
                            "primary_selling_point_code"
                        ],
                        "content_automotive_score": item[
                            "content_automotive_score"
                        ],
                        "evaluation_freshness": item["evaluation_freshness"],
                        "view_count": metrics.get(int(item["id"]), {}).get("view_count"),
                        "comment_count": metrics.get(int(item["id"]), {}).get("comment_count"),
                        "duplicate_original_link_id": item[
                            "duplicate_original_link_id"
                        ],
                    }.items()
                }
            )
    return ("\ufeff" + output.getvalue()).encode("utf-8")
