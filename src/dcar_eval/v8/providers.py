"""Verified supplier adapters for one-row and scheduled v8 capture updates."""

from __future__ import annotations

import json
import socket
import ssl
import urllib.error
import urllib.parse
import urllib.request
from datetime import date, datetime
from functools import partial
from pathlib import Path
from typing import Any, Callable, Dict, List, Mapping, Optional, Sequence
from zoneinfo import ZoneInfo

from workflow.privacy import CommentHasher  # type: ignore[import-not-found]

from .capture import (
    CaptureError,
    ProviderResult,
    SlotUnavailable,
    execute_account_fetch,
    execute_content_fetch,
)
from .evaluation import evaluate_content, upsert_comment_user_scores
from .operations import upsert_content
from .storage import DEFAULT_DB, connect, now_utc, transaction


SHANGHAI = ZoneInfo("Asia/Shanghai")
TIKHUB_KEY_FILE = Path("/Users/mark/Documents/key/DcarKey/TikHub.env.local")
RNOTE_KEY_FILE = Path("/Users/mark/Documents/key/DcarKey/Rnote.env.local")
TIKHUB_BASE = "https://api.tikhub.io"
RNOTE_BASE = "https://rnote.dev/api/v2/crawler/note"
TIKHUB_PRICE = 0.001
RNOTE_PRICE = 0.008
PRICE_VERIFIED_AT = "2026-08-02T13:55:00Z"
DISCOVERY_PRICE_VERIFIED_AT = "2026-08-02T14:49:00Z"


STAGE_CONFIG = {
    ("douyin", "detail"): ("TikHub", "tikhub-detail-v8.0", "douyin_video_detail", TIKHUB_PRICE),
    ("douyin", "metrics"): ("TikHub", "tikhub-statistics-v8.0", "douyin_video_statistics", TIKHUB_PRICE),
    ("douyin", "comments"): ("TikHub", "tikhub-comments-v8.0", "douyin_video_comments", TIKHUB_PRICE),
    ("xiaohongshu", "detail"): ("Rnote", "rnote-detail-v8.0", "xiaohongshu_note_detail", RNOTE_PRICE),
    ("xiaohongshu", "metrics"): ("Rnote", "rnote-metrics-v8.0", "xiaohongshu_note_metrics", RNOTE_PRICE),
    ("xiaohongshu", "comments"): ("Rnote", "rnote-comments-v8.0", "xiaohongshu_note_comments", RNOTE_PRICE),
}


class ProviderConfigurationError(RuntimeError):
    pass


def _load_key(path: Path, variable: str) -> str:
    if not path.is_file():
        raise ProviderConfigurationError(f"供应商凭据文件不存在：{path}")
    text = path.read_text(encoding="utf-8-sig").strip()
    for line in text.splitlines():
        line = line.strip()
        if not line or line.startswith("#"):
            continue
        if "=" in line:
            name, value = line.split("=", 1)
            if name.strip() == variable:
                secret = value.strip().strip("\"'")
                if secret:
                    return secret
        elif len(text.splitlines()) == 1:
            return line.strip("\"'")
    raise ProviderConfigurationError(f"{variable} 未配置")


def ensure_operational_budget(
    *, provider: str, operation: str, price: float, db_path: Path
) -> str:
    budget_id = f"operational-{provider.lower()}-{operation}-v1"
    captured_at = now_utc()
    max_requests = 1000 if provider == "TikHub" else 100
    daily_quota = 100 if provider == "TikHub" else 20
    with connect(db_path) as connection, transaction(connection):
        row = connection.execute(
            "SELECT * FROM provider_budget_batches WHERE id=?", (budget_id,)
        ).fetchone()
        if row is None:
            connection.execute(
                """
                INSERT INTO provider_budget_batches(
                    id, purpose, provider, operation, currency, verified_unit_price,
                    max_billable_requests, max_amount, pilot_size, daily_quota,
                    price_verified_at, status, created_at, updated_at
                ) VALUES (?, ?, ?, ?, 'USD', ?, ?, ?, 0, ?, ?, 'approved', ?, ?)
                """,
                (
                    budget_id, f"v8_{operation}", provider, operation, price,
                    max_requests, round(max_requests * price, 6), daily_quota,
                    PRICE_VERIFIED_AT, captured_at, captured_at,
                ),
            )
        else:
            if (
                row["provider"] != provider or row["operation"] != operation
                or abs(float(row["verified_unit_price"]) - price) > 1e-9
            ):
                raise ProviderConfigurationError("现有供应商预算与已核验价格不一致")
            if row["status"] not in {"approved", "pilot"}:
                raise ProviderConfigurationError(f"供应商预算状态为 {row['status']}，已阻断调用")
    return budget_id


def _request_json(
    url: str,
    *,
    headers: Mapping[str, str],
    params: Mapping[str, Any],
    provider: str,
) -> tuple[int, Any]:
    query = urllib.parse.urlencode({key: value for key, value in params.items() if value is not None})
    request = urllib.request.Request(
        f"{url}?{query}", headers={**headers, "Accept": "application/json", "User-Agent": "DCar-Insight-v8/1.0"}
    )
    try:
        with urllib.request.urlopen(request, timeout=45) as response:
            return int(response.status), json.loads(response.read().decode("utf-8", "replace"))
    except urllib.error.HTTPError as exc:
        status = int(exc.code)
        try:
            payload = json.loads(exc.read().decode("utf-8", "replace"))
        except json.JSONDecodeError:
            payload = {"error": f"HTTP {status}"}
        provider_blocked = status in {401, 402, 403}
        terminal = status in {400, 404, 410, 422}
        raise CaptureError(
            f"{provider} HTTP {status}", retryable=provider_blocked or (not terminal and (status in {408, 429} or status >= 500)),
            error_code="provider_balance_blocked" if status == 402 else "provider_auth_blocked" if provider_blocked else f"http_{status}",
            http_status=status, billed=False, raw_response=payload,
        ) from exc
    except (urllib.error.URLError, TimeoutError, socket.gaierror, ssl.SSLError) as exc:
        raise CaptureError(
            f"{provider} transport error: {type(exc).__name__}",
            retryable=True, error_code="transport_error",
        ) from exc


def _tikhub_data(payload: Any) -> Any:
    if not isinstance(payload, dict) or payload.get("code") != 200:
        message = payload.get("message_zh") if isinstance(payload, dict) else "invalid response"
        raise CaptureError(
            f"TikHub semantic error: {message}", retryable=True,
            error_code="semantic_error", http_status=200,
            billed=False, raw_response=payload,
        )
    return payload.get("data")


def _find_aweme(value: Any, aweme_id: str) -> Optional[Dict[str, Any]]:
    if isinstance(value, dict):
        if str(value.get("aweme_id") or "") == aweme_id and (
            "desc" in value or "statistics" in value or "author" in value
        ):
            return value
        for child in value.values():
            found = _find_aweme(child, aweme_id)
            if found:
                return found
    elif isinstance(value, list):
        for child in value:
            found = _find_aweme(child, aweme_id)
            if found:
                return found
    return None


def _first_int(value: Mapping[str, Any], *names: str) -> Optional[int]:
    for name in names:
        raw = value.get(name)
        if isinstance(raw, bool) or raw is None:
            continue
        try:
            return int(str(raw).replace(",", ""))
        except ValueError:
            continue
    return None


def _mapping(value: Any) -> Mapping[str, Any]:
    return value if isinstance(value, dict) else {}


def _find_string(value: Any, name: str) -> Optional[str]:
    if isinstance(value, dict):
        candidate = value.get(name)
        if candidate not in (None, ""):
            return str(candidate)
        for child in value.values():
            found = _find_string(child, name)
            if found:
                return found
    elif isinstance(value, list):
        for child in value:
            found = _find_string(child, name)
            if found:
                return found
    return None


def _collect_dicts(value: Any, identity_name: str) -> List[Mapping[str, Any]]:
    found: List[Mapping[str, Any]] = []
    seen: set[str] = set()

    def visit(child: Any) -> None:
        if isinstance(child, dict):
            identity = child.get(identity_name)
            if identity not in (None, ""):
                key = str(identity)
                if key not in seen:
                    seen.add(key)
                    found.append(child)
                return
            for nested in child.values():
                visit(nested)
        elif isinstance(child, list):
            for nested in child:
                visit(nested)

    visit(value)
    return found


def _douyin_reference_call(uid: str, key: str) -> ProviderResult:
    status, payload = _request_json(
        f"{TIKHUB_BASE}/api/v1/douyin/web/encrypt_uid_to_sec_user_id",
        headers={"Authorization": f"Bearer {key}"}, params={"uid": uid}, provider="TikHub",
    )
    data = _tikhub_data(payload)
    reference = _find_string(data, "sec_user_id")
    if not reference:
        raise CaptureError(
            "TikHub UID conversion omitted sec_user_id", retryable=True,
            error_code="invalid_response", http_status=status, billed=True, raw_response=payload,
        )
    return ProviderResult({"reference": reference}, payload, status, True)


def _douyin_discovery_call(sec_user_id: str, key: str) -> ProviderResult:
    status, payload = _request_json(
        f"{TIKHUB_BASE}/api/v1/douyin/app/v3/fetch_user_post_videos",
        headers={"Authorization": f"Bearer {key}"},
        params={"sec_user_id": sec_user_id, "max_cursor": 0, "count": 20, "sort_type": 0},
        provider="TikHub",
    )
    data = _tikhub_data(payload)
    items = []
    for item in _collect_dicts(data, "aweme_id"):
        aweme_id = str(item["aweme_id"])
        author = _mapping(item.get("author"))
        items.append(
            {
                "platform": "douyin",
                "platform_content_id": aweme_id,
                "canonical_url": f"https://www.douyin.com/video/{aweme_id}",
                "title": str(item.get("desc") or ""),
                "body": str(item.get("desc") or ""),
                "published_at": item.get("create_time"),
                "content_type": "image" if item.get("images") else "video",
                "account_name": str(author.get("nickname") or ""),
            }
        )
    return ProviderResult({"items": items}, payload, status, True)


def _rnote_discovery_call(uid: str, key: str) -> ProviderResult:
    status, payload = _request_json(
        "https://rnote.dev/api/v2/crawler/user/posted",
        headers={"X-API-Key": key}, params={"user_id": uid, "cursor": "", "num": 20},
        provider="Rnote",
    )
    if not isinstance(payload, dict) or status != 200:
        raise CaptureError(
            "Rnote user posts returned invalid response", retryable=True,
            error_code="invalid_response", http_status=status, billed=False, raw_response=payload,
        )
    data = payload.get("data", payload)
    items = []
    for item in _collect_dicts(data, "note_id"):
        note_id = str(item["note_id"])
        items.append(
            {
                "platform": "xiaohongshu",
                "platform_content_id": note_id,
                "canonical_url": f"https://www.xiaohongshu.com/explore/{note_id}",
                "title": str(item.get("title") or ""),
                "body": str(item.get("desc") or item.get("description") or ""),
                "published_at": item.get("time") or item.get("publish_time") or item.get("published_at"),
                "content_type": "video" if item.get("type") in {"video", 1, "1"} else "image",
                "account_name": str(item.get("nickname") or ""),
            }
        )
    return ProviderResult({"items": items}, payload, status, True)


def _douyin_call(stage: str, content_id: str, key: str) -> ProviderResult:
    endpoints: Dict[str, tuple[str, Dict[str, Any]]] = {
        "detail": ("/api/v1/douyin/app/v3/fetch_one_video", {"aweme_id": content_id}),
        "metrics": ("/api/v1/douyin/app/v3/fetch_video_statistics", {"aweme_ids": content_id}),
        "comments": (
            "/api/v1/douyin/app/v3/fetch_video_comments",
            {"aweme_id": content_id, "cursor": 0, "count": 20},
        ),
    }
    endpoint, params = endpoints[stage]
    status, payload = _request_json(
        f"{TIKHUB_BASE}{endpoint}", headers={"Authorization": f"Bearer {key}"},
        params=params, provider="TikHub",
    )
    data = _tikhub_data(payload)
    if stage == "metrics":
        items = data.get("statistics_list") if isinstance(data, dict) else None
        item = items[0] if isinstance(items, list) and items else None
        if not isinstance(item, dict) or str(item.get("aweme_id")) != content_id:
            raise CaptureError(
                "TikHub statistics did not contain requested content", retryable=True,
                error_code="invalid_response", http_status=status, billed=True,
                raw_response=payload,
            )
        normalized: Dict[str, Any] = {
            "view_count": _first_int(item, "play_count"),
            "comment_count": None,
            "like_count": _first_int(item, "digg_count"),
            "share_count": _first_int(item, "share_count"),
            "collect_count": None,
        }
        return ProviderResult(normalized, payload, status, True)
    if stage == "detail":
        item = _find_aweme(data, content_id)
        if item is None:
            raise CaptureError(
                "TikHub detail did not contain requested content", retryable=False,
                error_code="content_unavailable", http_status=status, billed=True,
                raw_response=payload,
            )
        author = _mapping(item.get("author"))
        published = _first_int(item, "create_time")
        normalized = {
            "title": str(item.get("desc") or ""), "body": str(item.get("desc") or ""),
            "published_at": datetime.fromtimestamp(published, tz=ZoneInfo("UTC")).isoformat().replace("+00:00", "Z") if published else None,
            "account_uid": str(author.get("uid") or ""),
            "account_name": str(author.get("nickname") or ""),
            "content_type": "video",
        }
        return ProviderResult(normalized, payload, status, True)
    page = data if isinstance(data, dict) else {}
    hasher = CommentHasher()
    sanitized: List[Dict[str, Any]] = []
    for item in page.get("comments") or []:
        if not isinstance(item, dict):
            continue
        comment_user = _mapping(item.get("user"))
        raw_user = str(comment_user.get("sec_uid") or comment_user.get("uid") or comment_user.get("unique_id") or "")
        text = " ".join(str(item.get("text") or "").split())[:2000]
        if not text:
            continue
        sanitized.append(
            {
                "platform_comment_id": str(item.get("cid") or ""),
                "anonymous_user_key": hasher.user_key("douyin", content_id, raw_user),
                "body": text,
                "published_at": datetime.fromtimestamp(int(item["create_time"]), tz=ZoneInfo("UTC")).isoformat().replace("+00:00", "Z") if item.get("create_time") else None,
                "like_count": _first_int(item, "digg_count"),
                "parent_comment_id": str(item.get("reply_id") or "") or None,
            }
        )
    safe_payload = {
        "code": 200,
        "data": {
            "total": _first_int(page, "total"), "has_more": bool(page.get("has_more")),
            "cursor": page.get("cursor"), "comments": sanitized,
            "privacy_note": "用户身份已按内容 HMAC-SHA256 匿名化；昵称、头像和主页字段未保存。",
        },
    }
    return ProviderResult(
        {"comment_count": _first_int(page, "total"), "comments": sanitized},
        safe_payload, status, True,
    )


def _rnote_unwrap(payload: Any) -> Any:
    if not isinstance(payload, dict):
        raise CaptureError("Rnote invalid response", retryable=True, error_code="invalid_response")
    if payload.get("success") is False:
        raise CaptureError(
            f"Rnote semantic error: {payload.get('error') or payload.get('detail')}",
            retryable=True, error_code="semantic_error", http_status=200,
            billed=bool(payload.get("billed")), raw_response=payload,
        )
    value = payload.get("data")
    if isinstance(value, dict) and "success" in value:
        if value.get("success") is False:
            raise CaptureError(
                f"Rnote upstream error: {value.get('msg') or value.get('message')}",
                retryable=True, error_code="upstream_error", http_status=200,
                billed=bool(payload.get("billed")), raw_response=payload,
            )
        value = value.get("data")
    return value


def _find_note(value: Any, note_id: str) -> Optional[Dict[str, Any]]:
    if isinstance(value, dict):
        if str(value.get("id") or value.get("note_id") or value.get("noteId") or "").lower() == note_id.lower():
            return value
        for child in value.values():
            found = _find_note(child, note_id)
            if found:
                return found
    elif isinstance(value, list):
        for child in value:
            found = _find_note(child, note_id)
            if found:
                return found
    return None


def _rnote_call(stage: str, content_id: str, key: str, content_type: str) -> ProviderResult:
    endpoint = "comments" if stage == "comments" else "video" if content_type == "video" else "image"
    params: Dict[str, Any] = {"note_id": content_id}
    if stage == "comments":
        params.update({"sort_strategy": "latest_v2"})
    status, payload = _request_json(
        f"{RNOTE_BASE}/{endpoint}", headers={"X-API-Key": key}, params=params, provider="Rnote"
    )
    payload_mapping = payload if isinstance(payload, dict) else {}
    data = _rnote_unwrap(payload)
    if stage == "comments":
        page = data if isinstance(data, dict) else {}
        raw_comments = page.get("comments") or page.get("comment_list") or []
        hasher = CommentHasher()
        comments: List[Dict[str, Any]] = []
        for item in raw_comments if isinstance(raw_comments, list) else []:
            if not isinstance(item, dict):
                continue
            comment_user = _mapping(item.get("user"))
            raw_user = str(comment_user.get("user_id") or comment_user.get("id") or item.get("user_id") or "")
            text = " ".join(str(item.get("content") or item.get("text") or "").split())[:2000]
            if text:
                comments.append(
                    {
                        "platform_comment_id": str(item.get("id") or item.get("comment_id") or ""),
                        "anonymous_user_key": hasher.user_key("xiaohongshu", content_id, raw_user),
                        "body": text, "published_at": None,
                        "like_count": _first_int(item, "like_count", "liked_count"),
                        "parent_comment_id": str(item.get("parent_comment_id") or "") or None,
                    }
                )
        total = _first_int(page, "total", "comment_count", "comments_count")
        safe = {"success": True, "data": {"total": total, "comments": comments, "privacy_note": "用户身份已匿名化"}}
        return ProviderResult({"comment_count": total, "comments": comments}, safe, status, bool(payload_mapping.get("billed", True)))
    note = _find_note(data, content_id)
    if note is None:
        raise CaptureError(
            "Rnote detail did not contain requested note", retryable=False,
            error_code="content_unavailable", http_status=status,
            billed=bool(payload_mapping.get("billed", True)), raw_response=payload,
        )
    note_user = _mapping(note.get("user") or note.get("author"))
    if stage == "detail":
        timestamp = _first_int(note, "time", "publish_time", "create_time")
        normalized: Dict[str, Any] = {
            "title": str(note.get("title") or ""),
            "body": str(note.get("desc") or note.get("description") or note.get("content") or ""),
            "published_at": datetime.fromtimestamp(timestamp, tz=ZoneInfo("UTC")).isoformat().replace("+00:00", "Z") if timestamp else None,
            "account_uid": str(note_user.get("user_id") or note_user.get("id") or note_user.get("uid") or ""),
            "account_name": str(note_user.get("nickname") or note_user.get("name") or ""),
            "content_type": content_type,
        }
    else:
        normalized = {
            "view_count": _first_int(note, "view_count", "read_count", "views"),
            "comment_count": _first_int(note, "comment_count", "comments_count"),
            "like_count": _first_int(note, "liked_count", "like_count", "likes"),
            "share_count": _first_int(note, "share_count", "shared_count"),
            "collect_count": _first_int(note, "collected_count", "collect_count", "favorite_count"),
        }
    return ProviderResult(normalized, payload, status, bool(payload_mapping.get("billed", True)))


def _store_stage_result(
    content: Mapping[str, Any],
    stage: str,
    window_key: str,
    outcome,
    *,
    db_path: Path,
) -> None:
    data = outcome.data
    captured_at = now_utc()
    with connect(db_path) as connection, transaction(connection):
        if stage == "detail":
            connection.execute(
                """
                UPDATE content_items SET title=COALESCE(NULLIF(?,''),title),
                    body=COALESCE(NULLIF(?,''),body),
                    published_at=COALESCE(?,published_at),
                    raw_account_uid=COALESCE(NULLIF(?,''),raw_account_uid),
                    raw_account_name=COALESCE(NULLIF(?,''),raw_account_name),
                    content_type=COALESCE(NULLIF(?,''),content_type), updated_at=? WHERE id=?
                """,
                (
                    data.get("title"), data.get("body"), data.get("published_at"),
                    data.get("account_uid"), data.get("account_name"), data.get("content_type"),
                    captured_at, content["id"],
                ),
            )
        elif stage == "metrics":
            connection.execute(
                """
                INSERT INTO content_metric_snapshots(
                    content_id, captured_at, window_key, view_count, comment_count,
                    like_count, share_count, collect_count, status, source,
                    raw_response_id, metadata_json
                ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, 'available', ?, ?, '{}')
                ON CONFLICT(content_id, window_key, source) DO UPDATE SET
                    captured_at=excluded.captured_at, view_count=excluded.view_count,
                    comment_count=excluded.comment_count, like_count=excluded.like_count,
                    share_count=excluded.share_count, collect_count=excluded.collect_count,
                    raw_response_id=excluded.raw_response_id
                """,
                (
                    content["id"], captured_at, window_key, data.get("view_count"),
                    data.get("comment_count"), data.get("like_count"), data.get("share_count"),
                    data.get("collect_count"), content["platform"], outcome.raw_response_id,
                ),
            )
        elif stage == "comments":
            raw = connection.execute(
                "SELECT local_path, sha256 FROM provider_raw_responses WHERE id=?",
                (outcome.raw_response_id,),
            ).fetchone()
            if raw is None:
                raise RuntimeError("comment raw response is missing")
            cursor = connection.execute(
                """
                INSERT INTO comment_evidence_versions(
                    content_id, captured_at, iso_week, source, local_path,
                    sha256, comment_count, status, created_at
                ) VALUES (?, ?, ?, ?, ?, ?, ?, 'available', ?)
                ON CONFLICT(content_id, iso_week, sha256) DO NOTHING
                """,
                (
                    content["id"], captured_at, window_key, content["platform"],
                    raw["local_path"], raw["sha256"], data.get("comment_count"), captured_at,
                ),
            )
            evidence_id = cursor.lastrowid
            if evidence_id is None:
                row = connection.execute(
                    "SELECT id FROM comment_evidence_versions WHERE content_id=? AND iso_week=? AND sha256=?",
                    (content["id"], window_key, raw["sha256"]),
                ).fetchone()
                evidence_id = row["id"]
            for item in data.get("comments") or []:
                connection.execute(
                    """
                    INSERT INTO comments(
                        evidence_version_id, platform_comment_id, anonymous_user_key,
                        body, published_at, like_count, parent_comment_id, raw_json
                    ) VALUES (?, ?, ?, ?, ?, ?, ?, '{}')
                    ON CONFLICT(evidence_version_id, platform_comment_id) DO NOTHING
                    """,
                    (
                        evidence_id, item.get("platform_comment_id") or None,
                        item.get("anonymous_user_key") or None, item.get("body") or "",
                        item.get("published_at"), item.get("like_count"), item.get("parent_comment_id"),
                    ),
                )
    if stage == "comments" and evidence_id is not None:
        import analyze_douyin_tikhub_v6 as scoring  # type: ignore[import-not-found]

        rows = []
        for item in data.get("comments") or []:
            key = str(item.get("anonymous_user_key") or "")
            text = str(item.get("body") or "")
            if key and text:
                rows.append(
                    {
                        "anonymous_user_key": key,
                        "audience_automotive_score": scoring.audience_user_score(text, context_automotive=True),
                        "action_intent_score": scoring.action_user_score(text, context_automotive=True),
                    }
                )
        upsert_comment_user_scores(
            int(content["id"]), int(evidence_id), rows, db_path=db_path
        )


def discover_account_content(
    account_id: int,
    platform: str,
    uid: str,
    *,
    as_of: Optional[date] = None,
    db_path: Path = DEFAULT_DB,
    call_override: Optional[
        Callable[[str, Mapping[str, Any]], ProviderResult]
    ] = None,
) -> Dict[str, Any]:
    if platform not in {"douyin", "xiaohongshu"}:
        return {
            "account_id": account_id, "platform": platform, "status": "unsupported",
            "inserted": 0, "updated": 0, "provider_cost": 0.0,
        }
    target_day = as_of or datetime.now(SHANGHAI).date()
    with connect(db_path) as connection:
        identity = connection.execute(
            """
            SELECT * FROM account_platform_identities
            WHERE account_id=? AND platform=? AND uid=?
            """,
            (account_id, platform, uid),
        ).fetchone()
        if identity is None:
            raise ProviderConfigurationError("账号平台身份不存在或已变化")

    costs = 0.0
    reference = uid
    reference_status = "not_required"
    if platform == "douyin":
        with connect(db_path) as connection:
            stored = connection.execute(
                """
                SELECT reference_value FROM account_provider_references
                WHERE account_identity_id=? AND provider='TikHub'
                  AND reference_kind='sec_user_id'
                """,
                (identity["id"],),
            ).fetchone()
        if stored is not None:
            reference = str(stored["reference_value"])
            reference_status = "cached"
        else:
            operation = "douyin_uid_to_sec_user_id"
            budget_id = ensure_operational_budget(
                provider="TikHub", operation=operation, price=TIKHUB_PRICE, db_path=db_path
            )
            call = (
                partial(call_override, "resolve_account", dict(identity))
                if call_override is not None
                else partial(_douyin_reference_call, uid, _load_key(TIKHUB_KEY_FILE, "TIKHUB_API_KEY"))
            )
            outcome = execute_account_fetch(
                account_id=account_id, stage="discovery", window_key="reference-lifetime",
                provider="TikHub", adapter_version="tikhub-uid-reference-v8.0",
                operation=operation, call=call, db_path=db_path, budget_id=budget_id,
            )
            reference = str(outcome.data.get("reference") or "")
            if not reference:
                raise ProviderConfigurationError("账号 UID 转换结果为空")
            with connect(db_path) as connection, transaction(connection):
                connection.execute(
                    """
                    INSERT INTO account_provider_references(
                        account_identity_id, provider, reference_kind, reference_value,
                        source_raw_response_id, created_at, updated_at
                    ) VALUES (?, 'TikHub', 'sec_user_id', ?, ?, ?, ?)
                    ON CONFLICT(account_identity_id,provider,reference_kind) DO UPDATE SET
                        reference_value=excluded.reference_value,
                        source_raw_response_id=excluded.source_raw_response_id,
                        updated_at=excluded.updated_at
                    """,
                    (identity["id"], reference, outcome.raw_response_id, now_utc(), now_utc()),
                )
            costs += outcome.amount
            reference_status = "resolved"

    provider = "TikHub" if platform == "douyin" else "Rnote"
    price = TIKHUB_PRICE if platform == "douyin" else RNOTE_PRICE
    operation = "douyin_user_posts" if platform == "douyin" else "xiaohongshu_user_posts"
    adapter = "tikhub-user-posts-v8.0" if platform == "douyin" else "rnote-user-posts-v8.0"
    budget_id = ensure_operational_budget(
        provider=provider, operation=operation, price=price, db_path=db_path
    )
    call = (
        partial(call_override, "discover_content", dict(identity))
        if call_override is not None
        else partial(_douyin_discovery_call, reference, _load_key(TIKHUB_KEY_FILE, "TIKHUB_API_KEY"))
        if platform == "douyin"
        else partial(_rnote_discovery_call, uid, _load_key(RNOTE_KEY_FILE, "RNOTE_API_KEY"))
    )
    try:
        outcome = execute_account_fetch(
            account_id=account_id, stage="discovery", window_key=target_day.isoformat(),
            provider=provider, adapter_version=adapter, operation=operation,
            call=call, db_path=db_path, budget_id=budget_id,
        )
    except SlotUnavailable:
        return {
            "account_id": account_id, "platform": platform, "status": "already_succeeded",
            "reference_status": reference_status, "inserted": 0, "updated": 0,
            "provider_cost": round(costs, 6),
        }
    costs += outcome.amount
    inserted = 0
    updated = 0
    for item in outcome.data.get("items") or []:
        value = {**dict(item), "account_uid": uid, "account_name": item.get("account_name") or identity["nickname"]}
        result = upsert_content(value, db_path=db_path)
        inserted += int(result["action"] == "inserted")
        updated += int(result["action"] == "updated")
    return {
        "account_id": account_id, "platform": platform, "status": "succeeded",
        "reference_status": reference_status, "inserted": inserted, "updated": updated,
        "provider_cost": round(costs, 6),
    }


def update_content_data(
    content_id: int,
    *,
    as_of: Optional[date] = None,
    db_path: Path = DEFAULT_DB,
    call_override: Optional[Callable[[str, Mapping[str, Any]], ProviderResult]] = None,
    stages: Optional[Sequence[str]] = None,
) -> Dict[str, Any]:
    with connect(db_path) as connection:
        row = connection.execute("SELECT * FROM content_items WHERE id=?", (content_id,)).fetchone()
        if row is None:
            raise ProviderConfigurationError("内容不存在")
        content = dict(row)
        succeeded_windows = {
            (str(slot["stage"]), str(slot["window_key"]))
            for slot in connection.execute(
                """
                SELECT stage, window_key FROM fetch_slots
                WHERE content_id=? AND status='succeeded'
                  AND stage IN ('detail','metrics','comments')
                """,
                (content_id,),
            ).fetchall()
        }
    platform = str(content["platform"])
    if platform not in {"douyin", "xiaohongshu"}:
        raise ProviderConfigurationError("视频号和快手首版只支持人工导入，未配置自动数据源")
    local_date = as_of or datetime.now(SHANGHAI).date()
    iso = local_date.isocalendar()
    requested_stages = [] if ("detail", "lifetime") in succeeded_windows else [
        ("detail", "lifetime")
    ]
    requested_stages.extend(
        [("metrics", local_date.isoformat()), ("comments", f"{iso.year}-W{iso.week:02d}")]
    )
    if stages is not None:
        requested = set(stages)
        invalid = requested - {"detail", "metrics", "comments"}
        if invalid:
            raise ProviderConfigurationError(f"未知抓取阶段：{','.join(sorted(invalid))}")
        requested_stages = [item for item in requested_stages if item[0] in requested]
    outcomes: List[Dict[str, Any]] = []
    for stage, window_key in requested_stages:
        if (stage, window_key) in succeeded_windows:
            outcomes.append(
                {
                    "stage": stage,
                    "status": "already_succeeded",
                    "message": "该内容在同一时间窗已有成功槽，未重复调用供应商",
                }
            )
            continue
        provider, adapter_version, operation, price = STAGE_CONFIG[(platform, stage)]
        budget_id = ensure_operational_budget(
            provider=provider, operation=operation, price=price, db_path=db_path
        )
        try:
            if call_override is not None:
                call = partial(call_override, stage, content)
            elif platform == "douyin":
                key = _load_key(TIKHUB_KEY_FILE, "TIKHUB_API_KEY")
                call = partial(_douyin_call, stage, str(content["platform_content_id"]), key)
            else:
                key = _load_key(RNOTE_KEY_FILE, "RNOTE_API_KEY")
                call = partial(
                    _rnote_call, stage, str(content["platform_content_id"]), key,
                    str(content["content_type"]),
                )
            outcome = execute_content_fetch(
                content_id=content_id, stage=stage, window_key=window_key,
                provider=provider, adapter_version=adapter_version, operation=operation,
                call=call, db_path=db_path, budget_id=budget_id,
            )
            _store_stage_result(content, stage, window_key, outcome, db_path=db_path)
            outcomes.append(
                {"stage": stage, "status": "succeeded", "billed": outcome.billed, "amount": outcome.amount, "currency": outcome.currency}
            )
        except SlotUnavailable as exc:
            outcomes.append({"stage": stage, "status": "already_succeeded", "message": str(exc)})
        except Exception as exc:
            error_code = getattr(exc, "error_code", type(exc).__name__)
            outcomes.append(
                {
                    "stage": stage,
                    "status": "failed",
                    "error_code": error_code,
                    "message": str(exc),
                }
            )
            if error_code in {"provider_balance_blocked", "provider_auth_blocked", "budget_blocked"}:
                break
    evaluation = evaluate_content(content_id, db_path=db_path)
    failed = any(item["status"] == "failed" for item in outcomes)
    return {
        "content_id": content_id,
        "status": "partial" if failed else "succeeded",
        "stages": outcomes,
        "evaluation_id": evaluation.evaluation_id,
        "evaluation_created": evaluation.created,
        "provider_cost": round(sum(float(item.get("amount") or 0) for item in outcomes), 6),
        "currency": "USD",
    }
