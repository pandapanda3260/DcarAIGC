#!/usr/bin/env python3
"""Collect and cache Rnote content/comments for the three-proposition pilot.

The cache is deliberately provider-independent and privacy-minimised:

* API keys, raw API responses, nicknames and raw user/comment IDs are never saved.
* User and comment IDs are replaced with salted HMAC hashes.
* Successful terminal collections are reused on later runs, so rescoring does not
  spend API balance again.
* The 5+5 base sample is preserved.  Notes below the 20-user gate consume the
  existing same-stratum replacement queue in its frozen order.
"""

from __future__ import annotations

import argparse
import csv
import hashlib
import hmac
import http.client
import json
import os
import re
import secrets
import socket
import ssl
import time
import urllib.error
import urllib.parse
import urllib.request
from dataclasses import asdict, dataclass
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Iterable, Iterator, Mapping, Sequence

from project_paths import ARCHIVE_PROCESSED_DIR, RNOTE_CACHE_DIR, XHS_INPUT_DIR

from collect_xhs_public import (
    clean_note_url,
    detail_object,
    fetch_html,
    first_string,
    parse_initial_state,
    validate_input_url,
)


KEY_ROOT = Path("/Users/mark/Documents/key/DcarKey")
API_BASE = "https://rnote.dev/api/v2/crawler/note"
CACHE_SCHEMA = "rnote-cache-v1.0"
COLLECTOR_VERSION = "rnote-pilot-collector-v1.0"
MIN_VALID_COMMENTERS = 20
TERMINAL_STOP_REASONS = {
    "target_valid_users",
    "end_of_comments",
    "confirmed_empty",
    "three_pages_without_new_users",
    "max_pages",
    "max_raw_comments",
}
SPAM_PATTERNS = (
    re.compile(pattern, re.IGNORECASE)
    for pattern in (
        r"\b(?:vx|v信|v\+|wechat)\b",
        r"(?:加|留|私信).{0,5}(?:微信|vx|v信|联系方式)",
        r"互赞|互粉|回关|求赞|求关注|刷赞|刷粉",
        r"抽奖口令|中奖请|联系客服|官方客服",
        r"接推广|接广|商务合作|代理招募",
    )
)
SPAM_PATTERNS = tuple(SPAM_PATTERNS)


class CollectorError(RuntimeError):
    """A safe, user-facing collection failure."""


class FatalProviderError(CollectorError):
    """Authentication, credit, or quota failure that must stop the whole batch."""


class RequestBudgetExceeded(CollectorError):
    pass


@dataclass(frozen=True)
class RnoteCursor:
    cursor: str = ""
    index: int = 0
    pageArea: str = "UNFOLDED"

    def fingerprint(self) -> str:
        return json.dumps(asdict(self), ensure_ascii=False, sort_keys=True)


@dataclass
class ApiResult:
    data: Any
    http_status: int
    billed: bool
    debug_id: str | None


@dataclass
class RequestBudget:
    maximum: int | None
    used: int = 0

    def consume(self) -> None:
        if self.maximum is not None and self.used >= self.maximum:
            raise RequestBudgetExceeded(
                f"Rnote request budget reached ({self.used}/{self.maximum})"
            )
        self.used += 1


def utc_now() -> str:
    return datetime.now(timezone.utc).replace(microsecond=0).isoformat()


def atomic_write_text(path: Path, text: str, *, mode: int = 0o600) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    temporary = path.with_name(path.name + ".tmp")
    temporary.write_text(text, encoding="utf-8")
    os.chmod(temporary, mode)
    os.replace(temporary, path)


def write_json(path: Path, value: Any) -> None:
    atomic_write_text(
        path,
        json.dumps(value, ensure_ascii=False, indent=2, sort_keys=True) + "\n",
    )


def read_json(path: Path, default: Any = None) -> Any:
    if not path.exists():
        return default
    return json.loads(path.read_text(encoding="utf-8"))


def write_jsonl(path: Path, rows: Iterable[Mapping[str, Any]]) -> None:
    text = "".join(
        json.dumps(dict(row), ensure_ascii=False, sort_keys=True) + "\n" for row in rows
    )
    atomic_write_text(path, text)


def read_jsonl(path: Path) -> list[dict[str, Any]]:
    if not path.exists():
        return []
    result: list[dict[str, Any]] = []
    for number, line in enumerate(path.read_text(encoding="utf-8").splitlines(), 1):
        if not line.strip():
            continue
        value = json.loads(line)
        if not isinstance(value, dict):
            raise CollectorError(f"{path}:{number} is not an object")
        result.append(value)
    return result


def load_key(path: Path) -> str:
    if not path.exists():
        raise CollectorError(f"Rnote key file not found: {path}")
    text = path.read_text(encoding="utf-8-sig").strip()
    if "=" in text:
        name, value = text.split("=", 1)
        if name.strip() not in {"RNOTE_API_KEY", "X_API_KEY", "API_KEY"}:
            raise CollectorError("Rnote key file has an unsupported variable name")
        text = value.strip().strip("\"'")
    if not text.startswith("sk-") or any(character.isspace() for character in text):
        raise CollectorError(
            "Rnote key file must contain a single sk-... key or RNOTE_API_KEY=sk-..."
        )
    return text


class CacheStore:
    def __init__(self, root: Path) -> None:
        self.root = root
        self.notes_root = root / "notes"
        self.root.mkdir(parents=True, exist_ok=True)
        self.notes_root.mkdir(parents=True, exist_ok=True)
        os.chmod(self.root, 0o700)
        os.chmod(self.notes_root, 0o700)
        self.salt = self._load_or_create_salt()

    def _load_or_create_salt(self) -> bytes:
        path = self.root / ".hash_salt"
        if path.exists():
            value = path.read_bytes()
            if len(value) < 32:
                raise CollectorError("cache hash salt is invalid")
            return value
        value = secrets.token_bytes(32)
        descriptor = os.open(path, os.O_WRONLY | os.O_CREAT | os.O_EXCL, 0o600)
        with os.fdopen(descriptor, "wb") as handle:
            handle.write(value)
        return value

    def digest(self, namespace: str, raw: str) -> str:
        message = f"{namespace}\0{raw}".encode("utf-8", "replace")
        return hmac.new(self.salt, message, hashlib.sha256).hexdigest()

    def note_dir(self, note_id: str) -> Path:
        if not re.fullmatch(r"[0-9a-fA-F]{24}", note_id):
            raise CollectorError(f"invalid note_id: {note_id}")
        path = self.notes_root / note_id.lower()
        path.mkdir(parents=True, exist_ok=True)
        os.chmod(path, 0o700)
        return path


class RnoteClient:
    def __init__(
        self,
        api_key: str,
        budget: RequestBudget,
        *,
        timeout: float = 45.0,
        delay: float = 0.25,
        retries: int = 1,
    ) -> None:
        self.api_key = api_key
        self.budget = budget
        self.timeout = timeout
        self.delay = delay
        self.retries = retries
        self.request_log: list[dict[str, Any]] = []

    @staticmethod
    def _unwrap(body: Any) -> tuple[Any, bool, str | None]:
        if not isinstance(body, dict):
            raise CollectorError("Rnote returned a non-object JSON response")
        billed = bool(body.get("billed"))
        debug_id = body.get("debug_id") if isinstance(body.get("debug_id"), str) else None
        if body.get("success") is False:
            message = body.get("error") or body.get("detail") or "outer success=false"
            if re.search(r"(?:balance|credit|quota|api.?key|auth|余额|额度|密钥|鉴权)", str(message), re.I):
                raise FatalProviderError(f"Rnote fatal provider error: {message}")
            raise CollectorError(f"Rnote semantic error: {message}")
        layer = body.get("data")
        if isinstance(layer, dict) and "success" in layer:
            if layer.get("success") is False or layer.get("code") not in (None, 0, "0"):
                message = layer.get("msg") or layer.get("message") or layer.get("code")
                if re.search(r"(?:balance|credit|quota|api.?key|auth|余额|额度|密钥|鉴权)", str(message), re.I):
                    raise FatalProviderError(f"Rnote fatal provider error: {message}")
                raise CollectorError(f"Rnote upstream error: {message}")
            if isinstance(layer.get("debug_id"), str):
                debug_id = layer["debug_id"]
            layer = layer.get("data")
        if layer is None:
            raise CollectorError("Rnote response contains no data")
        return layer, billed, debug_id

    @staticmethod
    def _error_message(body: bytes, status: int) -> str:
        try:
            value = json.loads(body.decode("utf-8", "replace"))
        except json.JSONDecodeError:
            return f"HTTP {status}"
        if isinstance(value, dict):
            message = value.get("detail") or value.get("error") or value.get("message")
            if isinstance(message, dict):
                message = message.get("message") or message.get("detail") or str(message)
            if message:
                return f"HTTP {status}: {message}"
        return f"HTTP {status}"

    def get(self, endpoint: str, params: Mapping[str, Any]) -> ApiResult:
        safe_endpoint = endpoint.rsplit("/", 1)[-1]
        last_error: Exception | None = None
        for attempt in range(self.retries + 1):
            self.budget.consume()
            query = urllib.parse.urlencode(
                {key: value for key, value in params.items() if value is not None}
            )
            request = urllib.request.Request(
                f"{endpoint}?{query}",
                headers={
                    "Accept": "application/json",
                    "X-API-Key": self.api_key,
                    "User-Agent": "Codex-Rnote-Pilot/1.0",
                },
            )
            started = time.monotonic()
            status = 0
            billed = False
            try:
                with urllib.request.urlopen(request, timeout=self.timeout) as response:
                    status = response.status
                    body_bytes = response.read()
                body = json.loads(body_bytes.decode("utf-8"))
                data, billed, debug_id = self._unwrap(body)
                self.request_log.append(
                    {
                        "at": utc_now(),
                        "endpoint": safe_endpoint,
                        "http_status": status,
                        "billed": billed,
                        "elapsed_seconds": round(time.monotonic() - started, 3),
                    }
                )
                if self.delay:
                    time.sleep(self.delay)
                return ApiResult(data=data, http_status=status, billed=billed, debug_id=debug_id)
            except urllib.error.HTTPError as exc:
                status = exc.code
                message = self._error_message(exc.read(), status)
                last_error = FatalProviderError(message) if status in {401, 402, 403} else CollectorError(message)
                self.request_log.append(
                    {
                        "at": utc_now(),
                        "endpoint": safe_endpoint,
                        "http_status": status,
                        "billed": False,
                        "elapsed_seconds": round(time.monotonic() - started, 3),
                    }
                )
                retryable = status in {408, 429} or status >= 500
                if isinstance(last_error, FatalProviderError) or not retryable or attempt >= self.retries:
                    raise last_error
                time.sleep(min(4.0, 0.75 * (2**attempt)))
            except (
                urllib.error.URLError,
                TimeoutError,
                ConnectionError,
                socket.gaierror,
                ssl.SSLError,
                http.client.IncompleteRead,
                json.JSONDecodeError,
            ) as exc:
                last_error = CollectorError(f"Rnote transport error: {type(exc).__name__}")
                self.request_log.append(
                    {
                        "at": utc_now(),
                        "endpoint": safe_endpoint,
                        "http_status": status,
                        "billed": billed,
                        "elapsed_seconds": round(time.monotonic() - started, 3),
                    }
                )
                if attempt >= self.retries:
                    raise last_error
                time.sleep(min(4.0, 0.75 * (2**attempt)))
        raise last_error or CollectorError("Rnote request failed")


def safe_int(value: Any) -> int | None:
    if isinstance(value, bool) or value is None:
        return None
    if isinstance(value, int):
        return value
    if isinstance(value, float) and value.is_integer():
        return int(value)
    if isinstance(value, str):
        cleaned = value.strip().replace(",", "")
        if cleaned.isdigit():
            return int(cleaned)
    return None


def first_value(mapping: Mapping[str, Any], names: Sequence[str]) -> Any:
    for name in names:
        value = mapping.get(name)
        if value not in (None, "", [], {}):
            return value
    return None


def https_url(value: Any) -> str:
    if not isinstance(value, str):
        return ""
    value = value.strip()
    if value.startswith("http://"):
        return "https://" + value[7:]
    return value if value.startswith("https://") else ""


def unique_strings(values: Iterable[str]) -> list[str]:
    result: list[str] = []
    seen: set[str] = set()
    for value in values:
        value = value.strip()
        if value and value not in seen:
            seen.add(value)
            result.append(value)
    return result


def find_note(payload: Any, note_id: str) -> tuple[dict[str, Any], dict[str, Any]]:
    """Return (note, surrounding container) from Rnote's loose response schema."""

    containers = payload if isinstance(payload, list) else [payload]
    for container in containers:
        if not isinstance(container, dict):
            continue
        candidates = container.get("note_list")
        if not isinstance(candidates, list):
            candidates = [container]
        for candidate in candidates:
            if not isinstance(candidate, dict):
                continue
            returned_id = first_value(candidate, ("id", "note_id", "noteId"))
            if str(returned_id).lower() == note_id.lower():
                return candidate, container
    raise CollectorError("Rnote detail did not contain the requested note_id")


def tag_names(note: Mapping[str, Any]) -> list[str]:
    result: list[str] = []
    for key in ("hash_tag", "topics", "tags", "tag_list"):
        value = note.get(key)
        if isinstance(value, dict):
            value = list(value.values())
        if not isinstance(value, list):
            continue
        for entry in value:
            if isinstance(entry, str):
                result.append(entry)
            elif isinstance(entry, dict):
                text = first_value(entry, ("name", "title", "tag_name", "tagName"))
                if isinstance(text, str):
                    result.append(text)
    return unique_strings(result)


def image_items(note: Mapping[str, Any]) -> list[dict[str, Any]]:
    raw = first_value(note, ("images_list", "image_list", "images", "imageList"))
    if not isinstance(raw, list):
        return []
    result: list[dict[str, Any]] = []
    for position, entry in enumerate(raw):
        if not isinstance(entry, dict):
            continue
        url = ""
        for name in (
            "url_size_large",
            "original",
            "origin_img",
            "url",
            "urlDefault",
            "urlPre",
        ):
            url = https_url(entry.get(name))
            if url:
                break
        if not url:
            continue
        result.append(
            {
                "index": safe_int(entry.get("index"))
                if safe_int(entry.get("index")) is not None
                else position,
                "url": url,
                "width": safe_int(entry.get("width")),
                "height": safe_int(entry.get("height")),
            }
        )
    return result


def collect_media_urls(value: Any, path: str = "", depth: int = 0) -> list[dict[str, str]]:
    if depth > 8:
        return []
    result: list[dict[str, str]] = []
    if isinstance(value, str):
        url = https_url(value)
        path_lower = path.lower()
        if url and any(
            token in path_lower
            for token in ("video", "stream", "master", "h264", "h265", "url")
        ):
            result.append({"path": path, "url": url})
    elif isinstance(value, dict):
        for key, child in value.items():
            child_path = f"{path}.{key}" if path else str(key)
            result.extend(collect_media_urls(child, child_path, depth + 1))
    elif isinstance(value, list):
        for index, child in enumerate(value):
            result.extend(collect_media_urls(child, f"{path}[{index}]", depth + 1))
    unique: list[dict[str, str]] = []
    seen: set[str] = set()
    for entry in result:
        if entry["url"] not in seen:
            seen.add(entry["url"])
            unique.append(entry)
    return unique


def normalize_content(
    payload: Any,
    *,
    row: Mapping[str, str],
    endpoint_type: str,
    store: CacheStore,
) -> dict[str, Any]:
    note_id = row["note_id"]
    note, container = find_note(payload, note_id)
    user = note.get("user") if isinstance(note.get("user"), dict) else {}
    if not user and isinstance(container.get("user"), dict):
        user = container["user"]
    author_id = first_value(user, ("userid", "id", "user_id", "userId"))
    author_hash = (
        store.digest(f"xiaohongshu:{note_id}:user", str(author_id))
        if author_id
        else None
    )

    interaction_names = {
        "likes": ("liked_count", "likedCount", "likes"),
        "collects": ("collected_count", "collectedCount", "collects"),
        "comments": ("comments_count", "comment_count", "commentCount"),
        "shares": ("shared_count", "share_count", "shareCount"),
        "views": ("view_count", "viewCount", "views"),
    }
    interactions = {
        key: safe_int(first_value(note, names)) for key, names in interaction_names.items()
    }
    media_sections = [
        note.get(name)
        for name in (
            "video",
            "video_info",
            "video_info_v2",
            "videoInfo",
            "media_stream",
            "mediaStream",
            "stream",
        )
        if note.get(name) is not None
    ]
    video_urls: list[dict[str, str]] = []
    for section_index, section in enumerate(media_sections):
        video_urls.extend(collect_media_urls(section, f"media[{section_index}]"))
    deduped_video_urls: list[dict[str, str]] = []
    seen_video: set[str] = set()
    for entry in video_urls:
        if entry["url"] not in seen_video:
            seen_video.add(entry["url"])
            deduped_video_urls.append(entry)

    return {
        "schema_version": CACHE_SCHEMA,
        "provider": "rnote",
        "collector_version": COLLECTOR_VERSION,
        "collected_at": utc_now(),
        "sample_attempt_id": row["sample_attempt_id"],
        "note_id": note_id.lower(),
        "url": clean_note_url(row["url"]),
        "endpoint_type": endpoint_type,
        "note_type": str(first_value(note, ("type", "note_type", "noteType")) or ""),
        "title": str(first_value(note, ("title", "note_title", "noteTitle")) or ""),
        "desc": str(first_value(note, ("desc", "description", "content")) or ""),
        "tags": tag_names(note),
        "images": image_items(note),
        "video_urls": deduped_video_urls,
        "published_at": safe_int(first_value(note, ("time", "publish_time", "published_at"))),
        "last_updated_at": safe_int(
            first_value(note, ("last_update_time", "update_time", "updated_at"))
        ),
        "interactions": interactions,
        "author_hash": author_hash,
    }


def load_public_types(path: Path) -> dict[str, str]:
    result: dict[str, str] = {}
    for row in read_jsonl(path):
        note_id = str(row.get("note_id") or "").lower()
        note_type = str(row.get("note_type") or "").lower()
        if note_id and note_type:
            result[note_id] = note_type
    return result


def detect_note_type(
    row: Mapping[str, str], public_types: dict[str, str], *, timeout: int
) -> tuple[str, str]:
    note_id = row["note_id"].lower()
    known = public_types.get(note_id, "")
    if "video" in known:
        return "video", "existing_public_cache"
    if known:
        return "image", "existing_public_cache"
    try:
        validate_input_url(row["url"], note_id)
        status, html, reason = fetch_html(row["url"], timeout)
        if status != 200 or not html:
            return "image", f"public_probe_failed:{status or reason}"
        state = parse_initial_state(html)
        detail = detail_object(state, note_id)
        note = detail.get("note") if isinstance(detail.get("note"), dict) else {}
        note_type = first_string(note, ("type", "noteType")).lower()
        if note_type:
            public_types[note_id] = note_type
        return ("video" if "video" in note_type else "image"), "public_probe"
    except (KeyError, ValueError):
        return "image", "public_probe_unavailable"


def normalize_text(value: Any) -> str:
    if not isinstance(value, str):
        return ""
    return re.sub(r"\s+", " ", value).strip()


def semantic_text_after_mentions(text: str) -> str:
    without_mentions = re.sub(r"@[\w\-\u4e00-\u9fff]+", " ", text)
    return "".join(
        character
        for character in without_mentions
        if character.isalnum() or "\u4e00" <= character <= "\u9fff"
    )


def base_exclusion_reason(
    *, text: str, user_hash: str | None, author_hash: str | None
) -> str | None:
    if not user_hash:
        return "missing_user_identity"
    if author_hash and user_hash == author_hash:
        return "author_comment"
    if not text:
        return "empty_text"
    if not semantic_text_after_mentions(text):
        return "no_semantic_text"
    if any(pattern.search(text) for pattern in SPAM_PATTERNS):
        return "spam_or_bait"
    return None


def comment_list(page: Mapping[str, Any]) -> list[dict[str, Any]]:
    for key in ("comments", "comment_list", "sub_comments", "list"):
        value = page.get(key)
        if isinstance(value, list):
            return [item for item in value if isinstance(item, dict)]
    return []


def normalize_comment(
    comment: Mapping[str, Any],
    *,
    note_id: str,
    store: CacheStore,
    author_hash: str | None,
    level: int,
    parent_comment_hash: str | None,
    retrieval_order: int,
) -> dict[str, Any]:
    text = normalize_text(first_value(comment, ("content", "text", "comment")))
    user = comment.get("user") if isinstance(comment.get("user"), dict) else {}
    raw_user_id = first_value(user, ("userid", "id", "user_id", "userId"))
    user_hash = (
        store.digest(f"xiaohongshu:{note_id}:user", str(raw_user_id))
        if raw_user_id
        else None
    )
    raw_comment_id = first_value(comment, ("id", "comment_id", "commentId"))
    fallback_identity = "|".join(
        (
            note_id,
            str(raw_user_id or ""),
            str(first_value(comment, ("time", "created_at", "create_time")) or ""),
            text,
        )
    )
    comment_hash = store.digest("comment", str(raw_comment_id or fallback_identity))
    exclusion = base_exclusion_reason(
        text=text, user_hash=user_hash, author_hash=author_hash
    )
    return {
        "schema_version": CACHE_SCHEMA,
        "note_id": note_id,
        "comment_hash": comment_hash,
        "user_hash": user_hash,
        "parent_comment_hash": parent_comment_hash,
        "level": level,
        "text": text,
        "created_at": safe_int(
            first_value(comment, ("time", "created_at", "create_time", "createTime"))
        ),
        "like_count": safe_int(first_value(comment, ("like_count", "likeCount", "likes"))),
        "retrieval_order": retrieval_order,
        "base_exclusion_reason": exclusion,
        "exclusion_reason": exclusion,
        "valid_for_audience": exclusion is None,
    }


def normalize_comment_tree(
    comments: Sequence[Mapping[str, Any]],
    *,
    note_id: str,
    store: CacheStore,
    author_hash: str | None,
    start_order: int,
) -> tuple[list[dict[str, Any]], list[dict[str, Any]]]:
    result: list[dict[str, Any]] = []
    thread_meta: list[dict[str, Any]] = []
    order = start_order
    for top in comments:
        top_record = normalize_comment(
            top,
            note_id=note_id,
            store=store,
            author_hash=author_hash,
            level=1,
            parent_comment_hash=None,
            retrieval_order=order,
        )
        order += 1
        result.append(top_record)
        raw_id = first_value(top, ("id", "comment_id", "commentId"))
        children = first_value(top, ("sub_comments", "subComments", "sub_comment_list"))
        child_rows = [child for child in children if isinstance(child, dict)] if isinstance(children, list) else []
        thread_meta.append(
            {
                "comment_hash": top_record["comment_hash"],
                "reported_sub_comment_count": safe_int(
                    first_value(top, ("sub_comment_count", "subCommentCount"))
                )
                or 0,
                "embedded_sub_comment_count": len(child_rows),
                # raw_id is deliberately kept in memory only, never serialized.
                "_raw_id": str(raw_id) if raw_id else None,
            }
        )
        for child in child_rows:
            result.append(
                normalize_comment(
                    child,
                    note_id=note_id,
                    store=store,
                    author_hash=author_hash,
                    level=2,
                    parent_comment_hash=top_record["comment_hash"],
                    retrieval_order=order,
                )
            )
            order += 1
    return result, thread_meta


def apply_duplicate_filter(records: list[dict[str, Any]]) -> None:
    text_users: dict[str, set[str]] = {}
    for record in records:
        record["exclusion_reason"] = record.get("base_exclusion_reason")
        if record.get("base_exclusion_reason") is None and record.get("user_hash"):
            normalized = re.sub(r"\W+", "", record.get("text", "")).lower()
            if len(normalized) >= 12:
                text_users.setdefault(normalized, set()).add(record["user_hash"])
    copied = {text for text, users in text_users.items() if len(users) >= 3}
    for record in records:
        normalized = re.sub(r"\W+", "", record.get("text", "")).lower()
        if record.get("base_exclusion_reason") is None and normalized in copied:
            record["exclusion_reason"] = "duplicate_copy_text"
        record["valid_for_audience"] = record.get("exclusion_reason") is None


def deduplicate_comments(records: Iterable[dict[str, Any]]) -> list[dict[str, Any]]:
    result: list[dict[str, Any]] = []
    seen: set[str] = set()
    for record in sorted(records, key=lambda row: row.get("retrieval_order", 0)):
        identity = str(record.get("comment_hash") or "")
        if not identity or identity in seen:
            continue
        seen.add(identity)
        result.append(record)
    apply_duplicate_filter(result)
    return result


def valid_user_hashes(records: Iterable[Mapping[str, Any]]) -> set[str]:
    return {
        str(row["user_hash"])
        for row in records
        if row.get("valid_for_audience") and row.get("user_hash")
    }


def parse_cursor(value: Any, previous: RnoteCursor | None = None) -> RnoteCursor | None:
    if value in (None, "", {}):
        return None
    parsed = value
    if isinstance(value, str):
        stripped = value.strip()
        if stripped.startswith("{"):
            try:
                parsed = json.loads(stripped)
            except json.JSONDecodeError as exc:
                raise CollectorError("Rnote returned an invalid JSON cursor") from exc
        else:
            return RnoteCursor(
                cursor=stripped,
                index=(previous.index + 1 if previous else 1),
                pageArea=(previous.pageArea if previous else "UNFOLDED"),
            )
    if not isinstance(parsed, dict):
        raise CollectorError("Rnote returned an unsupported cursor value")
    cursor = str(parsed.get("cursor") or "")
    index = safe_int(parsed.get("index"))
    page_area = str(parsed.get("pageArea") or parsed.get("page_area") or "UNFOLDED")
    if not cursor:
        return None
    return RnoteCursor(
        cursor=cursor,
        index=index if index is not None else (previous.index + 1 if previous else 1),
        pageArea=page_area,
    )


def platform_comment_count(content: Mapping[str, Any] | None, page: Mapping[str, Any] | None) -> int | None:
    candidates: list[Any] = []
    if page:
        candidates.extend(page.get(key) for key in ("comment_count", "comment_count_l1"))
    if content and isinstance(content.get("interactions"), dict):
        candidates.append(content["interactions"].get("comments"))
    for candidate in candidates:
        value = safe_int(candidate)
        if value is not None:
            return value
    return None


def collect_content(
    row: Mapping[str, str],
    *,
    note_dir: Path,
    store: CacheStore,
    client: RnoteClient,
    public_types: dict[str, str],
    public_timeout: int,
    refresh: bool,
) -> tuple[dict[str, Any] | None, dict[str, Any]]:
    content_path = note_dir / "content.json"
    metadata_path = note_dir / "collection.json"
    metadata = read_json(metadata_path, {}) or {}
    if content_path.exists() and not refresh:
        return read_json(content_path), metadata

    preferred, detection = detect_note_type(row, public_types, timeout=public_timeout)
    endpoints = [preferred, "video" if preferred == "image" else "image"]
    errors: list[str] = []
    for endpoint_type in endpoints:
        endpoint = f"{API_BASE}/{endpoint_type}"
        before = len(client.request_log)
        try:
            result = client.get(endpoint, {"note_id": row["note_id"]})
            content = normalize_content(
                result.data,
                row=row,
                endpoint_type=endpoint_type,
                store=store,
            )
            write_json(content_path, content)
            metadata["schema_version"] = CACHE_SCHEMA
            metadata["note_id"] = row["note_id"].lower()
            metadata["sample_attempt_id"] = row["sample_attempt_id"]
            metadata["content"] = {
                "status": "complete",
                "endpoint_type": endpoint_type,
                "note_type_detection": detection,
                "collected_at": utc_now(),
                "requests": client.request_log[before:],
            }
            metadata["updated_at"] = utc_now()
            write_json(metadata_path, metadata)
            return content, metadata
        except (CollectorError, RequestBudgetExceeded) as exc:
            if isinstance(exc, RequestBudgetExceeded):
                raise
            if isinstance(exc, FatalProviderError):
                raise
            errors.append(f"{endpoint_type}:{exc}")
    metadata["schema_version"] = CACHE_SCHEMA
    metadata["note_id"] = row["note_id"].lower()
    metadata["sample_attempt_id"] = row["sample_attempt_id"]
    metadata["content"] = {
        "status": "failed",
        "note_type_detection": detection,
        "error": " | ".join(errors)[:800],
        "collected_at": utc_now(),
    }
    metadata["updated_at"] = utc_now()
    write_json(metadata_path, metadata)
    return None, metadata


def collect_comments(
    row: Mapping[str, str],
    *,
    note_dir: Path,
    store: CacheStore,
    client: RnoteClient,
    content: Mapping[str, Any] | None,
    target_valid: int,
    max_pages: int,
    max_raw: int,
    refresh: bool,
) -> tuple[list[dict[str, Any]], dict[str, Any]]:
    comments_path = note_dir / "comments.jsonl"
    metadata_path = note_dir / "collection.json"
    metadata = read_json(metadata_path, {}) or {}
    comments_meta = metadata.get("comments") if isinstance(metadata.get("comments"), dict) else {}

    if refresh:
        records: list[dict[str, Any]] = []
        cursor = RnoteCursor()
        seen_cursors: set[str] = set()
        pages = 0
        no_new_pages = 0
    else:
        records = deduplicate_comments(read_jsonl(comments_path))
        stop_reason = comments_meta.get("stop_reason")
        if stop_reason in TERMINAL_STOP_REASONS:
            return records, metadata
        cursor_value = comments_meta.get("next_cursor")
        cursor = (
            RnoteCursor(**cursor_value)
            if isinstance(cursor_value, dict)
            else RnoteCursor()
        )
        seen_cursors = set(comments_meta.get("seen_cursors") or [])
        pages = safe_int(comments_meta.get("pages_fetched")) or 0
        no_new_pages = safe_int(comments_meta.get("consecutive_pages_without_new_users")) or 0

    note_id = row["note_id"].lower()
    author_hash = content.get("author_hash") if content else None
    stop_reason = ""
    last_page: dict[str, Any] | None = None
    error: str | None = None
    request_log_start = len(client.request_log)

    try:
        while True:
            before_users = valid_user_hashes(records)
            result = client.get(
                f"{API_BASE}/comments",
                {
                    "note_id": note_id,
                    "cursor": cursor.cursor,
                    "index": cursor.index,
                    "pageArea": cursor.pageArea,
                    "sort_strategy": "latest_v2",
                },
            )
            if not isinstance(result.data, dict):
                raise CollectorError("Rnote comments payload is not an object")
            page = result.data
            last_page = page
            page_comments = comment_list(page)
            normalized, _thread_meta = normalize_comment_tree(
                page_comments,
                note_id=note_id,
                store=store,
                author_hash=author_hash,
                start_order=len(records),
            )
            records = deduplicate_comments([*records, *normalized])
            pages += 1
            after_users = valid_user_hashes(records)
            if len(after_users) == len(before_users):
                no_new_pages += 1
            else:
                no_new_pages = 0

            has_more = bool(page.get("has_more") or page.get("hasMore"))
            next_cursor = parse_cursor(page.get("cursor"), cursor)
            if len(after_users) >= target_valid:
                stop_reason = "target_valid_users"
            elif not has_more:
                stop_reason = "confirmed_empty" if not records else "end_of_comments"
            elif no_new_pages >= 3:
                stop_reason = "three_pages_without_new_users"
            elif pages >= max_pages:
                stop_reason = "max_pages"
            elif len(records) >= max_raw:
                stop_reason = "max_raw_comments"
            elif next_cursor is None:
                stop_reason = "missing_next_cursor"
                error = "has_more=true but no usable next cursor"
            elif next_cursor.fingerprint() in seen_cursors:
                stop_reason = "repeated_cursor"
                error = "Rnote returned a repeated pagination cursor"

            if next_cursor is not None:
                seen_cursors.add(next_cursor.fingerprint())
            comments_meta = {
                "status": (
                    "confirmed_empty"
                    if stop_reason == "confirmed_empty"
                    else "complete"
                    if stop_reason == "end_of_comments"
                    else "partial"
                    if records
                    else "failed"
                ),
                "collected_at": utc_now(),
                "sort_strategy": "latest_v2",
                "pages_fetched": pages,
                "raw_comment_count": len(records),
                "valid_unique_commenters": len(after_users),
                "platform_comment_count": platform_comment_count(content, page),
                "pagination_complete": stop_reason in {"end_of_comments", "confirmed_empty"},
                "stop_reason": stop_reason or None,
                "next_cursor": asdict(next_cursor) if next_cursor else None,
                "seen_cursors": sorted(seen_cursors),
                "consecutive_pages_without_new_users": no_new_pages,
                "author_filter_available": bool(author_hash),
                "embedded_reply_policy": (
                    "top-level comments plus replies embedded in those pages; "
                    "no separate reply-thread API calls"
                ),
                "error": error,
            }
            metadata["schema_version"] = CACHE_SCHEMA
            metadata["note_id"] = note_id
            metadata["sample_attempt_id"] = row["sample_attempt_id"]
            metadata["comments"] = comments_meta
            metadata["updated_at"] = utc_now()
            write_jsonl(comments_path, records)
            write_json(metadata_path, metadata)

            if stop_reason:
                break
            cursor = next_cursor
    except (RequestBudgetExceeded, FatalProviderError):
        raise
    except CollectorError as exc:
        error = str(exc)
        commenters = len(valid_user_hashes(records))
        comments_meta = {
            **comments_meta,
            "status": "partial" if records else "failed",
            "collected_at": utc_now(),
            "sort_strategy": "latest_v2",
            "pages_fetched": pages,
            "raw_comment_count": len(records),
            "valid_unique_commenters": commenters if records else None,
            "platform_comment_count": platform_comment_count(content, last_page),
            "pagination_complete": False,
            "stop_reason": "request_error",
            "next_cursor": asdict(cursor),
            "seen_cursors": sorted(seen_cursors),
            "consecutive_pages_without_new_users": no_new_pages,
            "author_filter_available": bool(author_hash),
            "error": error[:800],
        }
        metadata["comments"] = comments_meta
        metadata["updated_at"] = utc_now()
        write_jsonl(comments_path, records)
        write_json(metadata_path, metadata)

    metadata = read_json(metadata_path, metadata)
    metadata["comments"]["requests"] = client.request_log[request_log_start:]
    metadata["updated_at"] = utc_now()
    write_json(metadata_path, metadata)
    return records, metadata


def collection_status(
    content: Mapping[str, Any] | None,
    metadata: Mapping[str, Any],
) -> dict[str, Any]:
    comments = metadata.get("comments") if isinstance(metadata.get("comments"), dict) else {}
    content_state = metadata.get("content") if isinstance(metadata.get("content"), dict) else {}
    valid = safe_int(comments.get("valid_unique_commenters"))
    stop_reason = comments.get("stop_reason")
    status = comments.get("status")
    if content is None or content_state.get("status") != "complete":
        sample_status = "data_error"
        eligible = False
    elif status in {"failed", None} or stop_reason in {"request_error", "missing_next_cursor", "repeated_cursor"}:
        sample_status = "technical_missing"
        eligible = False
    elif valid is not None and valid >= MIN_VALID_COMMENTERS:
        sample_status = "scorable"
        eligible = True
    elif status == "confirmed_empty":
        sample_status = "confirmed_zero"
        eligible = False
    elif status == "complete" and valid is not None:
        sample_status = "below_minimum"
        eligible = False
    else:
        sample_status = "technical_missing"
        eligible = False
    return {
        "content_status": content_state.get("status") or ("complete" if content else "failed"),
        "comment_fetch_status": status or "not_retrieved",
        "comment_sample_status": sample_status,
        "valid_unique_commenters": valid if sample_status != "technical_missing" else None,
        "raw_comment_count": safe_int(comments.get("raw_comment_count")),
        "platform_comment_count": safe_int(comments.get("platform_comment_count")),
        "comment_pages_fetched": safe_int(comments.get("pages_fetched")),
        "comment_pagination_complete": comments.get("pagination_complete"),
        "stop_reason": stop_reason,
        "error": comments.get("error") or content_state.get("error"),
        "scorable": eligible,
    }


def read_csv(path: Path) -> list[dict[str, str]]:
    with path.open(encoding="utf-8-sig", newline="") as handle:
        return [dict(row) for row in csv.DictReader(handle)]


def build_base_rows(blind_path: Path, labels_path: Path) -> list[dict[str, str]]:
    blind = read_csv(blind_path)
    labels = {row["pilot_id"]: row for row in read_csv(labels_path)}
    counters = {"auto": 0, "non_auto": 0}
    result: list[dict[str, str]] = []
    for row in blind:
        pilot_id = row["pilot_id"]
        label = labels.get(pilot_id)
        if not label or label.get("note_id", "").lower() != row.get("note_id", "").lower():
            raise CollectorError(f"base label mismatch for {pilot_id}")
        stratum = label["source_label"]
        counters[stratum] += 1
        result.append(
            {
                "sample_attempt_id": pilot_id,
                "sample_role": "base_random_sample",
                "source_stratum": stratum,
                "source_sample_id": label.get("source_sample_id", ""),
                "target_slot": f"{stratum}_{counters[stratum]:02d}",
                "note_id": row["note_id"].lower(),
                "url": row["url"],
            }
        )
    if counters != {"auto": 5, "non_auto": 5}:
        raise CollectorError(f"base sample must be 5+5, got {counters}")
    return result


def build_replacement_rows(blind_path: Path, key_path: Path) -> list[dict[str, str]]:
    blind = read_csv(blind_path)
    keys = {row["candidate_id"]: row for row in read_csv(key_path)}
    result: list[dict[str, str]] = []
    for row in blind:
        candidate_id = row["candidate_id"]
        key = keys.get(candidate_id)
        if not key or key.get("note_id", "").lower() != row.get("note_id", "").lower():
            raise CollectorError(f"replacement label mismatch for {candidate_id}")
        result.append(
            {
                "sample_attempt_id": candidate_id,
                "sample_role": "replacement_candidate",
                "source_stratum": key["source_stratum"],
                "source_sample_id": key.get("source_sample_id", ""),
                "queue_position": key.get("queue_position", ""),
                "target_slot": "",
                "note_id": row["note_id"].lower(),
                "url": row["url"],
            }
        )
    return result


def process_one(
    row: dict[str, str],
    *,
    store: CacheStore,
    client: RnoteClient,
    public_types: dict[str, str],
    args: argparse.Namespace,
) -> dict[str, Any]:
    note_dir = store.note_dir(row["note_id"])
    content, _ = collect_content(
        row,
        note_dir=note_dir,
        store=store,
        client=client,
        public_types=public_types,
        public_timeout=args.public_timeout,
        refresh=args.refresh,
    )
    _, metadata = collect_comments(
        row,
        note_dir=note_dir,
        store=store,
        client=client,
        content=content,
        target_valid=args.target_valid,
        max_pages=args.max_pages,
        max_raw=args.max_raw,
        refresh=args.refresh,
    )
    status = collection_status(content, metadata)
    return {
        "schema_version": CACHE_SCHEMA,
        "collector_version": COLLECTOR_VERSION,
        "collected_at": utc_now(),
        **{key: value for key, value in row.items() if key != "url"},
        "url": clean_note_url(row["url"]),
        **status,
    }


def run_pilot(args: argparse.Namespace) -> tuple[list[dict[str, Any]], dict[str, Any]]:
    base_rows = build_base_rows(args.base_input, args.base_labels)
    replacement_rows = build_replacement_rows(args.replacements, args.replacement_labels)
    store = CacheStore(args.cache_dir)
    key = load_key(args.key_file)
    budget = RequestBudget(args.max_requests)
    client = RnoteClient(
        key,
        budget,
        timeout=args.timeout,
        delay=args.delay,
        retries=args.retries,
    )
    public_types = load_public_types(args.public_content)
    attempts: list[dict[str, Any]] = []
    slots: dict[str, dict[str, str] | None] = {
        row["target_slot"]: None for row in base_rows
    }
    stop_error: str | None = None

    def collect_and_record(row: dict[str, str]) -> dict[str, Any] | None:
        nonlocal stop_error
        try:
            result = process_one(
                row,
                store=store,
                client=client,
                public_types=public_types,
                args=args,
            )
        except RequestBudgetExceeded as exc:
            stop_error = str(exc)
            return None
        attempts.append(result)
        label = (
            f"{result['valid_unique_commenters']} valid users"
            if result.get("valid_unique_commenters") is not None
            else result.get("comment_sample_status")
        )
        print(
            json.dumps(
                {
                    "sample_attempt_id": result["sample_attempt_id"],
                    "source_stratum": result["source_stratum"],
                    "status": result["comment_sample_status"],
                    "evidence": label,
                    "api_requests_used": budget.used,
                },
                ensure_ascii=False,
            ),
            flush=True,
        )
        return result

    for row in base_rows:
        result = collect_and_record(row)
        if result is None:
            break
        if result["scorable"]:
            slots[row["target_slot"]] = {
                "sample_attempt_id": row["sample_attempt_id"],
                "note_id": row["note_id"],
            }

    if stop_error is None:
        for stratum in ("auto", "non_auto"):
            open_slots = [slot for slot, value in slots.items() if slot.startswith(stratum + "_") and value is None]
            candidates = [row for row in replacement_rows if row["source_stratum"] == stratum]
            used = 0
            for candidate in candidates:
                if not open_slots or used >= args.max_replacements_per_stratum:
                    break
                candidate = dict(candidate)
                candidate["target_slot"] = open_slots[0]
                result = collect_and_record(candidate)
                if result is None:
                    break
                used += 1
                if result["scorable"]:
                    slot = open_slots.pop(0)
                    slots[slot] = {
                        "sample_attempt_id": candidate["sample_attempt_id"],
                        "note_id": candidate["note_id"],
                    }
            if stop_error is not None:
                break

    # Rebuild final eligibility deterministically from filled slots.
    selected_ids = {
        value["sample_attempt_id"] for value in slots.values() if isinstance(value, dict)
    }
    for attempt in attempts:
        attempt["final_sample_eligible"] = attempt["sample_attempt_id"] in selected_ids
        if attempt["final_sample_eligible"]:
            attempt["sample_attempt_status"] = "final_scorable"
            attempt["replacement_reason"] = None
        else:
            attempt["sample_attempt_status"] = "comment_unqualified"
            attempt["replacement_reason"] = {
                "confirmed_zero": "confirmed_zero",
                "below_minimum": "below_20_valid_users",
                "technical_missing": "comment_not_retrieved",
                "data_error": "data_error",
            }.get(attempt["comment_sample_status"], "data_error")

    previous_summary = read_json(args.cache_dir / "pilot_collection_summary.json", {}) or {}
    summary = {
        "schema_version": CACHE_SCHEMA,
        "collector_version": COLLECTOR_VERSION,
        "generated_at": utc_now(),
        "sampling_seed": 20260719,
        "replacement_seed": 20260720,
        "requests_used_this_run": budget.used,
        "billed_requests_this_run": sum(bool(row.get("billed")) for row in client.request_log),
        "attempts": len(attempts),
        "final_slots_filled": sum(value is not None for value in slots.values()),
        "final_slots_target": len(slots),
        "final_by_stratum": {
            stratum: sum(
                value is not None and slot.startswith(stratum + "_")
                for slot, value in slots.items()
            )
            for stratum in ("auto", "non_auto")
        },
        "slots": slots,
        "stopped_error": stop_error,
        "cache_reuse_rule": "terminal content/comments cache entries do not call Rnote again",
        "privacy_rule": "no API key, nickname, raw user ID or raw comment ID is persisted",
    }
    for accounting_field in (
        "collection_billed_requests_across_runs",
        "video_enrichment_billed_requests",
        "total_billed_requests",
        "request_accounting_note",
    ):
        if accounting_field in previous_summary:
            summary[accounting_field] = previous_summary[accounting_field]
    write_jsonl(args.cache_dir / "pilot_collection_attempts.jsonl", attempts)
    write_json(args.cache_dir / "pilot_collection_summary.json", summary)
    return attempts, summary


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--base-input", type=Path, default=XHS_INPUT_DIR / "pilot_sample_10_blind.csv")
    parser.add_argument("--base-labels", type=Path, default=XHS_INPUT_DIR / "pilot_sample_10_labels.csv")
    parser.add_argument(
        "--replacements",
        type=Path,
        default=ARCHIVE_PROCESSED_DIR / "pilot_replacement_queue_v0.3_blind.csv",
    )
    parser.add_argument(
        "--replacement-labels",
        type=Path,
        default=ARCHIVE_PROCESSED_DIR / "pilot_replacement_queue_v0.3_key.csv",
    )
    parser.add_argument("--public-content", type=Path, default=ARCHIVE_PROCESSED_DIR / "pilot_public_content.jsonl")
    parser.add_argument("--key-file", type=Path, default=KEY_ROOT / "Rnote.env.local")
    parser.add_argument("--cache-dir", type=Path, default=RNOTE_CACHE_DIR)
    parser.add_argument("--target-valid", type=int, default=30)
    parser.add_argument("--max-pages", type=int, default=10)
    parser.add_argument("--max-raw", type=int, default=500)
    parser.add_argument("--max-requests", type=int, default=180)
    parser.add_argument("--max-replacements-per-stratum", type=int, default=20)
    parser.add_argument("--timeout", type=float, default=45.0)
    parser.add_argument("--public-timeout", type=int, default=30)
    parser.add_argument("--delay", type=float, default=0.25)
    parser.add_argument("--retries", type=int, default=1)
    parser.add_argument(
        "--refresh",
        action="store_true",
        help="Ignore terminal cache state and call the API again (normally do not use)",
    )
    args = parser.parse_args()
    for name in ("target_valid", "max_pages", "max_raw", "max_requests"):
        if getattr(args, name) <= 0:
            parser.error(f"--{name.replace('_', '-')} must be positive")
    if args.target_valid < MIN_VALID_COMMENTERS:
        parser.error(f"--target-valid must be at least {MIN_VALID_COMMENTERS}")
    if args.max_replacements_per_stratum < 0 or args.delay < 0 or args.retries < 0:
        parser.error("replacement limit, delay and retries must be non-negative")
    return args


def main() -> int:
    args = parse_args()
    _, summary = run_pilot(args)
    print(json.dumps(summary, ensure_ascii=False, indent=2))
    return 0 if summary["final_slots_filled"] == summary["final_slots_target"] else 2


if __name__ == "__main__":
    raise SystemExit(main())
