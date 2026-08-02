"""Account/content write models, deterministic identity upserts and CSV exports."""

from __future__ import annotations

import csv
import hashlib
import io
import json
import re
import sqlite3
import uuid
from pathlib import Path
from typing import Any, Dict, List, Mapping, Optional, Sequence
from urllib.parse import urlsplit, urlunsplit

from .migration import generate_link_id, normalize_timestamp
from .storage import DEFAULT_DB, connect, now_utc, transaction


PLATFORMS = {"douyin", "xiaohongshu", "wechat_channels", "kuaishou"}
ACCOUNT_TYPES = {"boutique_ip", "original", "mixed_edit", "unknown"}
DIRECTIONS = {"new_car", "used_car", "media", "other", "unknown"}
REAL_NAME_STATUSES = {"yes", "no", "unknown"}
DOUYIN_ID_RE = re.compile(r"(?:/video/|[?&]modal_id=)(\d{6,24})(?:[/?&#]|$)", re.I)
XHS_ID_RE = re.compile(r"/(?:explore|discovery/item)/(?:[^/?#]*/)?([0-9a-f]{24})(?:[/?#]|$)", re.I)


class OperationError(RuntimeError):
    pass


def normalize_phone(value: Any) -> tuple[str, str]:
    display = str(value or "").strip()
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


def content_identity(platform: str, url: str, explicit_id: Any = None) -> Dict[str, Any]:
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
    key = f"{platform}:{content_id}" if content_id else f"{platform}:url:{normalized_hash}"
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
                    item.get("real_name_status"), REAL_NAME_STATUSES,
                    "real_name_status",
                ),
            }
        )
    if len({row["platform"] for row in rows}) != len(rows):
        raise OperationError("同一账号每个平台只能有一个身份")
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
        expanded["enabled"] = enabled.strip().lower() not in {"0", "false", "no", "否", "停用"}
    return expanded


def upsert_account(value: Mapping[str, Any], *, db_path: Path = DEFAULT_DB) -> Dict[str, Any]:
    phone, normalized = normalize_phone(value.get("phone"))
    identities = _identity_rows(value)
    account_type = _enum(value.get("account_type"), ACCOUNT_TYPES, "account_type")
    direction = _enum(value.get("content_direction"), DIRECTIONS, "content_direction")
    enabled = bool(value.get("enabled", True))
    captured_at = now_utc()
    with connect(db_path) as connection, transaction(connection):
        account = connection.execute(
            "SELECT * FROM accounts WHERE phone_normalized=?", (normalized,)
        ).fetchone()
        if account is None:
            cursor = connection.execute(
                """
                INSERT INTO accounts(
                    phone, phone_normalized, operator_name, account_type,
                    content_direction, enabled, created_at, updated_at
                ) VALUES (?, ?, ?, ?, ?, ?, ?, ?)
                """,
                (
                    phone, normalized, str(value.get("operator_name") or "").strip(),
                    account_type, direction, int(enabled), captured_at, captured_at,
                ),
            )
            if cursor.lastrowid is None:
                raise RuntimeError("account insert returned no id")
            account_id = int(cursor.lastrowid)
            action = "inserted"
        else:
            account_id = int(account["id"])
            connection.execute(
                """
                UPDATE accounts SET phone=?, operator_name=?, account_type=?,
                    content_direction=?, enabled=?, updated_at=? WHERE id=?
                """,
                (
                    phone, str(value.get("operator_name") or "").strip(), account_type,
                    direction, int(enabled), captured_at, account_id,
                ),
            )
            action = "updated"
        for identity in identities:
            conflict = connection.execute(
                """
                SELECT account_id FROM account_platform_identities
                WHERE platform=? AND uid=?
                """,
                (identity["platform"], identity["uid"]),
            ).fetchone()
            if conflict is not None and int(conflict["account_id"]) != account_id:
                raise OperationError(
                    f"{identity['platform']} UID {identity['uid']} 已属于其他手机号"
                )
        incoming_platforms = {identity["platform"] for identity in identities}
        existing_identities = {
            str(row["platform"]): row
            for row in connection.execute(
                "SELECT * FROM account_platform_identities WHERE account_id=?", (account_id,)
            ).fetchall()
        }
        for platform, existing in existing_identities.items():
            if platform not in incoming_platforms:
                connection.execute(
                    "DELETE FROM account_platform_identities WHERE id=?", (existing["id"],)
                )
        for identity in identities:
            existing = existing_identities.get(identity["platform"])
            if existing is not None and existing["uid"] == identity["uid"]:
                connection.execute(
                    """
                    UPDATE account_platform_identities
                    SET nickname=?, real_name_status=?, updated_at=? WHERE id=?
                    """,
                    (
                        identity["nickname"], identity["real_name_status"], captured_at,
                        existing["id"],
                    ),
                )
            else:
                if existing is not None:
                    connection.execute(
                        "DELETE FROM account_platform_identities WHERE id=?", (existing["id"],)
                    )
                connection.execute(
                    """
                    INSERT INTO account_platform_identities(
                        account_id, platform, uid, nickname, real_name_status,
                        source, created_at, updated_at
                    ) VALUES (?, ?, ?, ?, ?, 'manual', ?, ?)
                    """,
                    (
                        account_id, identity["platform"], identity["uid"], identity["nickname"],
                        identity["real_name_status"], captured_at, captured_at,
                    ),
                )
            connection.execute(
                "DELETE FROM pending_platform_identities WHERE platform=? AND uid=?",
                (identity["platform"], identity["uid"]),
            )
        connection.execute(
            """
            UPDATE content_items SET account_id=NULL, updated_at=?
            WHERE account_id=? AND NOT EXISTS (
                SELECT 1 FROM account_platform_identities api
                WHERE api.account_id=? AND api.platform=content_items.platform
                  AND api.uid=content_items.raw_account_uid
            )
            """,
            (captured_at, account_id, account_id),
        )
        connection.execute(
            """
            UPDATE content_items SET account_id=?, updated_at=?
            WHERE account_id IS NULL AND EXISTS (
                SELECT 1 FROM account_platform_identities api
                WHERE api.account_id=? AND api.platform=content_items.platform
                  AND api.uid=content_items.raw_account_uid
            )
            """,
            (account_id, captured_at, account_id),
        )
    return {"id": account_id, "action": action}


def update_account(account_id: int, value: Mapping[str, Any], *, db_path: Path = DEFAULT_DB) -> Dict[str, Any]:
    with connect(db_path) as connection:
        row = connection.execute("SELECT * FROM accounts WHERE id=?", (account_id,)).fetchone()
        if row is None:
            raise OperationError("账号不存在")
        platforms = connection.execute(
            "SELECT platform, uid, nickname, real_name_status FROM account_platform_identities WHERE account_id=?",
            (account_id,),
        ).fetchall()
    merged = {**dict(row), "platforms": [dict(item) for item in platforms], **dict(value)}
    result = upsert_account(merged, db_path=db_path)
    if int(result["id"]) != account_id:
        raise OperationError("修改后的手机号已属于其他账号")
    return result


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
            _, key = normalize_phone(row.get("phone"))
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
            status, entity_id, reason = "duplicate_in_file", None, "同文件后续行覆盖本行"
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
                    batch_id, index, status, entity_id, key or "",
                    json.dumps(dict(row), ensure_ascii=False), reason[:1000],
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


def _merge_unique_children(connection, table: str, survivor: int, loser: int) -> None:
    rows = connection.execute(f"SELECT id FROM {table} WHERE content_id=?", (loser,)).fetchall()
    for row in rows:
        try:
            connection.execute(
                f"UPDATE {table} SET content_id=? WHERE id=?", (survivor, row["id"])
            )
        except sqlite3.IntegrityError:
            connection.execute(f"DELETE FROM {table} WHERE id=?", (row["id"],))


def merge_content_records(connection, first_id: int, second_id: int) -> int:
    rows = connection.execute(
        "SELECT * FROM content_items WHERE id IN (?,?) ORDER BY created_at, id",
        (first_id, second_id),
    ).fetchall()
    if len(rows) != 2:
        raise OperationError("需要合并的内容记录不存在")
    survivor, loser = rows[0], rows[1]
    survivor_id, loser_id = int(survivor["id"]), int(loser["id"])
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
                survivor_id, identity["identity_kind"], identity["identity_value"],
                identity["platform_identity_key"], identity["is_primary"], identity["created_at"],
            ),
        )
    connection.execute("DELETE FROM content_identities WHERE content_id=?", (loser_id,))
    for table in (
        "fetch_slots", "provider_raw_responses", "content_metric_snapshots", "comment_evidence_versions",
        "evidence_artifacts", "evidence_envelopes", "media_processing_slots",
    ):
        _merge_unique_children(connection, table, survivor_id, loser_id)
    evaluations = connection.execute(
        "SELECT id FROM evaluation_versions WHERE content_id=? ORDER BY id", (loser_id,)
    ).fetchall()
    for evaluation in evaluations:
        try:
            connection.execute(
                "UPDATE evaluation_versions SET content_id=? WHERE id=?",
                (survivor_id, evaluation["id"]),
            )
        except sqlite3.IntegrityError:
            existing = connection.execute(
                """
                SELECT id FROM evaluation_versions survivor
                WHERE survivor.content_id=? AND (
                    survivor.rule_version, survivor.taxonomy_version, survivor.evidence_sha256
                )=(SELECT rule_version, taxonomy_version, evidence_sha256
                   FROM evaluation_versions WHERE id=?)
                """,
                (survivor_id, evaluation["id"]),
            ).fetchone()
            if existing:
                connection.execute(
                    "UPDATE review_queue SET evaluation_id=? WHERE evaluation_id=?",
                    (existing["id"], evaluation["id"]),
                )
                connection.execute(
                    "UPDATE evaluation_reviews SET previous_evaluation_id=? WHERE previous_evaluation_id=?",
                    (existing["id"], evaluation["id"]),
                )
                connection.execute(
                    "UPDATE evaluation_reviews SET resulting_evaluation_id=? WHERE resulting_evaluation_id=?",
                    (existing["id"], evaluation["id"]),
                )
                connection.execute("DELETE FROM evaluation_versions WHERE id=?", (evaluation["id"],))
    connection.execute("UPDATE evaluation_reviews SET content_id=? WHERE content_id=?", (survivor_id, loser_id))
    connection.execute("UPDATE manual_evidence SET content_id=? WHERE content_id=?", (survivor_id, loser_id))
    queue_rows = connection.execute("SELECT id FROM review_queue WHERE content_id=?", (loser_id,)).fetchall()
    for row in queue_rows:
        try:
            connection.execute("UPDATE review_queue SET content_id=? WHERE id=?", (survivor_id, row["id"]))
        except sqlite3.IntegrityError:
            connection.execute("UPDATE evaluation_reviews SET queue_id=NULL WHERE queue_id=?", (row["id"],))
            connection.execute("DELETE FROM review_queue WHERE id=?", (row["id"],))
    score_rows = connection.execute(
        "SELECT * FROM comment_user_scores WHERE content_id=?", (loser_id,)
    ).fetchall()
    for row in score_rows:
        connection.execute(
            """
            INSERT INTO comment_user_scores(
                content_id, evidence_version_id, anonymous_user_key,
                audience_automotive_score, action_intent_score, evaluated_at
            ) VALUES (?, ?, ?, ?, ?, ?)
            ON CONFLICT(content_id, anonymous_user_key) DO UPDATE SET
                evidence_version_id=excluded.evidence_version_id,
                audience_automotive_score=excluded.audience_automotive_score,
                action_intent_score=excluded.action_intent_score,
                evaluated_at=excluded.evaluated_at
            """,
            (
                survivor_id, row["evidence_version_id"], row["anonymous_user_key"],
                row["audience_automotive_score"], row["action_intent_score"], row["evaluated_at"],
            ),
        )
    connection.execute("DELETE FROM comment_user_scores WHERE content_id=?", (loser_id,))
    connection.execute(
        "DELETE FROM duplicate_relations WHERE duplicate_content_id IN (?,?) AND original_content_id IN (?,?)",
        (survivor_id, loser_id, survivor_id, loser_id),
    )
    for column in ("duplicate_content_id", "original_content_id"):
        relations = connection.execute(
            f"SELECT id FROM duplicate_relations WHERE {column}=?", (loser_id,)
        ).fetchall()
        for relation in relations:
            try:
                connection.execute(
                    f"UPDATE duplicate_relations SET {column}=? WHERE id=?",
                    (survivor_id, relation["id"]),
                )
            except sqlite3.IntegrityError:
                connection.execute("DELETE FROM duplicate_relations WHERE id=?", (relation["id"],))
    task_rows = connection.execute("SELECT * FROM task_contents WHERE content_id=?", (loser_id,)).fetchall()
    for row in task_rows:
        connection.execute(
            """
            INSERT INTO task_contents(task_id, content_id, inclusion_status, reason)
            VALUES (?, ?, ?, ?) ON CONFLICT(task_id, content_id) DO NOTHING
            """,
            (row["task_id"], survivor_id, row["inclusion_status"], row["reason"]),
        )
    connection.execute("DELETE FROM task_contents WHERE content_id=?", (loser_id,))
    connection.execute("DELETE FROM content_items WHERE id=?", (loser_id,))
    return survivor_id


def _rebuild_text_duplicate_group(connection, content_id: int) -> None:
    content = connection.execute(
        "SELECT title, body FROM content_items WHERE id=?", (content_id,)
    ).fetchone()
    if content is None:
        return
    normalized = " ".join(f"{content['title']}\n{content['body']}".lower().split())
    if len(normalized) < 12:
        return
    fingerprint = hashlib.sha256(normalized.encode("utf-8")).hexdigest()
    candidates = connection.execute(
        "SELECT id, title, body, published_at, imported_at FROM content_items ORDER BY id"
    ).fetchall()
    group = [
        row for row in candidates
        if hashlib.sha256(
            " ".join(f"{row['title']}\n{row['body']}".lower().split()).encode("utf-8")
        ).hexdigest() == fingerprint
    ]
    ids = [int(row["id"]) for row in group]
    if ids:
        placeholders = ",".join("?" for _ in ids)
        connection.execute(
            f"DELETE FROM duplicate_relations WHERE method='text_sha256' AND duplicate_content_id IN ({placeholders})",
            ids,
        )
    if len(group) < 2:
        return
    ordered = sorted(group, key=lambda row: (row["published_at"] or row["imported_at"], row["id"]))
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
                row["id"], original,
                json.dumps({"sha256": fingerprint}, ensure_ascii=False), now_utc(),
            ),
        )


def upsert_content(value: Mapping[str, Any], *, db_path: Path = DEFAULT_DB) -> Dict[str, Any]:
    platform = str(value.get("platform") or "")
    identity = content_identity(
        platform, str(value.get("canonical_url") or value.get("url") or ""),
        value.get("platform_content_id"),
    )
    published_raw = value.get("published_at")
    published = normalize_timestamp(published_raw) if published_raw not in (None, "") else None
    if published_raw not in (None, "") and published is None:
        raise OperationError("发布日期必须是 ISO 时间或 Unix 秒")
    account_type = _enum(value.get("account_type"), ACCOUNT_TYPES, "account_type")
    direction = _enum(value.get("content_direction"), DIRECTIONS, "content_direction")
    uid = str(value.get("account_uid") or "").strip()
    captured_at = now_utc()
    with connect(db_path) as connection, transaction(connection):
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
        if by_id is not None and by_url is not None and int(by_id["id"]) != int(by_url["id"]):
            content_id = merge_content_records(connection, int(by_id["id"]), int(by_url["id"]))
            current = connection.execute("SELECT * FROM content_items WHERE id=?", (content_id,)).fetchone()
        else:
            current = by_id or by_url
        account_id = _account_for_uid(connection, platform, uid)
        if current is None:
            link_id = generate_link_id(connection, str(identity["identity_key"]))
            cursor = connection.execute(
                """
                INSERT INTO content_items(
                    link_id, platform, platform_content_id, canonical_url, normalized_url_hash,
                    account_id, raw_account_uid, raw_account_name, legacy_account_type,
                    title, body, content_type, published_at, published_at_raw,
                    manual_content_direction, imported_at, created_at, updated_at
                ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                """,
                (
                    link_id, platform, identity["platform_content_id"], identity["canonical_url"],
                    identity["normalized_url_hash"], account_id, uid or None,
                    str(value.get("account_name") or "").strip() or None, account_type,
                    str(value.get("title") or "").strip(), str(value.get("body") or "").strip(),
                    str(value.get("content_type") or "unknown").strip(), published,
                    str(published_raw) if published_raw not in (None, "") else None,
                    direction, captured_at, captured_at, captured_at,
                ),
            )
            if cursor.lastrowid is None:
                raise RuntimeError("content insert returned no id")
            content_id = int(cursor.lastrowid)
            action = "inserted"
        else:
            content_id = int(current["id"])
            connection.execute(
                """
                UPDATE content_items SET platform_content_id=?, canonical_url=?,
                    normalized_url_hash=?, account_id=COALESCE(?, account_id),
                    raw_account_uid=?, raw_account_name=?, legacy_account_type=?,
                    title=?, body=?, content_type=?, published_at=?, published_at_raw=?,
                    manual_content_direction=?, updated_at=? WHERE id=?
                """,
                (
                    identity["platform_content_id"], identity["canonical_url"],
                    identity["normalized_url_hash"], account_id, uid or current["raw_account_uid"],
                    str(value.get("account_name") or current["raw_account_name"] or "").strip() or None,
                    account_type, str(value.get("title") or "").strip(),
                    str(value.get("body") or "").strip(),
                    str(value.get("content_type") or current["content_type"] or "unknown").strip(),
                    published, str(published_raw) if published_raw not in (None, "") else None,
                    direction, captured_at, content_id,
                ),
            )
            action = "updated"
        connection.execute(
            "UPDATE content_identities SET is_primary=0 WHERE content_id=?", (content_id,)
        )
        kind = "platform_content_id" if identity["platform_content_id"] else "canonical_url"
        identity_value = str(identity["platform_content_id"] or identity["canonical_url"])
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
        connection.execute(
            """
            DELETE FROM duplicate_relations
            WHERE method='fingerprint_v1'
              AND (duplicate_content_id=? OR original_content_id=?)
            """,
            (content_id, content_id),
        )
        _rebuild_text_duplicate_group(connection, content_id)
    return {"id": content_id, "action": action}


def update_content(content_id: int, value: Mapping[str, Any], *, db_path: Path = DEFAULT_DB) -> Dict[str, Any]:
    with connect(db_path) as connection:
        row = connection.execute("SELECT * FROM content_items WHERE id=?", (content_id,)).fetchone()
        if row is None:
            raise OperationError("内容不存在")
    merged = {
        "platform": row["platform"], "platform_content_id": row["platform_content_id"],
        "canonical_url": row["canonical_url"], "account_uid": row["raw_account_uid"],
        "account_name": row["raw_account_name"], "account_type": row["legacy_account_type"],
        "title": row["title"], "body": row["body"], "content_type": row["content_type"],
        "published_at": row["published_at"], "content_direction": row["manual_content_direction"],
        **dict(value),
    }
    result = upsert_content(merged, db_path=db_path)
    if int(result["id"]) != content_id:
        return {**result, "merged_from_id": content_id}
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
            status, entity_id, reason = "duplicate_in_file", None, "同文件后续行覆盖本行"
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
                    batch_id, index, status, entity_id, key or "",
                    json.dumps(dict(row), ensure_ascii=False), reason[:1000],
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


def export_accounts_csv(*, db_path: Path = DEFAULT_DB) -> bytes:
    output = io.StringIO()
    fields = [
        "phone", "operator_name", "account_type", "content_direction", "enabled",
        "douyin_uid", "douyin_nickname", "douyin_real_name_status",
        "xiaohongshu_uid", "xiaohongshu_nickname", "xiaohongshu_real_name_status",
        "wechat_channels_uid", "wechat_channels_nickname", "wechat_channels_real_name_status",
        "kuaishou_uid", "kuaishou_nickname", "kuaishou_real_name_status",
    ]
    writer = csv.DictWriter(output, fieldnames=fields)
    writer.writeheader()
    with connect(db_path) as connection:
        accounts = connection.execute("SELECT * FROM accounts ORDER BY id").fetchall()
        for account in accounts:
            row: Dict[str, Any] = {field: "" for field in fields}
            row.update(
                {
                    "phone": account["phone"], "operator_name": account["operator_name"],
                    "account_type": account["account_type"],
                    "content_direction": account["content_direction"], "enabled": account["enabled"],
                }
            )
            identities = connection.execute(
                "SELECT * FROM account_platform_identities WHERE account_id=?", (account["id"],)
            ).fetchall()
            for identity in identities:
                prefix = str(identity["platform"])
                row[f"{prefix}_uid"] = identity["uid"]
                row[f"{prefix}_nickname"] = identity["nickname"]
                row[f"{prefix}_real_name_status"] = identity["real_name_status"]
            writer.writerow(row)
    return ("\ufeff" + output.getvalue()).encode("utf-8")


def export_contents_csv(*, db_path: Path = DEFAULT_DB) -> bytes:
    output = io.StringIO()
    fields = [
        "link_id", "platform", "platform_content_id", "published_at", "canonical_url",
        "title", "account_uid", "account_name", "account_type", "content_direction",
        "primary_selling_point_code", "content_automotive_score", "view_count", "comment_count",
        "duplicate_original_link_id",
    ]
    writer = csv.DictWriter(output, fieldnames=fields)
    writer.writeheader()
    with connect(db_path) as connection:
        rows = connection.execute(
            """
            SELECT c.*, COALESCE(a.account_type,c.legacy_account_type,'unknown') account_type,
                   COALESCE(c.manual_content_direction,c.evaluation_content_direction,a.content_direction,'unknown') direction,
                   ev.primary_selling_point_code, ev.content_automotive_score,
                   ms.view_count, ms.comment_count, original.link_id duplicate_original_link_id
            FROM content_items c LEFT JOIN accounts a ON a.id=c.account_id
            LEFT JOIN evaluation_versions ev ON ev.id=(
                SELECT id FROM evaluation_versions
                WHERE content_id=c.id AND invalidated_at IS NULL
                ORDER BY evaluated_at DESC,id DESC LIMIT 1
            )
            LEFT JOIN content_metric_snapshots ms ON ms.id=(SELECT id FROM content_metric_snapshots WHERE content_id=c.id ORDER BY captured_at DESC,id DESC LIMIT 1)
            LEFT JOIN duplicate_relations d ON d.id=(SELECT id FROM duplicate_relations WHERE duplicate_content_id=c.id AND status='confirmed' ORDER BY id LIMIT 1)
            LEFT JOIN content_items original ON original.id=d.original_content_id
            ORDER BY c.id
            """
        ).fetchall()
        for item in rows:
            writer.writerow(
                {
                    "link_id": item["link_id"], "platform": item["platform"],
                    "platform_content_id": item["platform_content_id"],
                    "published_at": item["published_at"], "canonical_url": item["canonical_url"],
                    "title": item["title"], "account_uid": item["raw_account_uid"],
                    "account_name": item["raw_account_name"], "account_type": item["account_type"],
                    "content_direction": item["direction"],
                    "primary_selling_point_code": item["primary_selling_point_code"],
                    "content_automotive_score": item["content_automotive_score"],
                    "view_count": item["view_count"], "comment_count": item["comment_count"],
                    "duplicate_original_link_id": item["duplicate_original_link_id"],
                }
            )
    return ("\ufeff" + output.getvalue()).encode("utf-8")
