#!/usr/bin/env python3
"""Safely collect and diagnose OneBound Xiaohongshu comments.

Secrets are loaded from onebound.env, are never printed, and are not written to
result files. Raw commenter identifiers are never persisted: an HMAC digest is
used only to deduplicate commenters within the output.
"""

from __future__ import annotations

import argparse
import csv
import hashlib
import hmac
import json
import re
import socket
import time
import urllib.error
import urllib.parse
import urllib.request
from collections import Counter
from pathlib import Path
from typing import Any, Dict, Iterable, List, Mapping, Tuple

from project_paths import ARCHIVE_PROCESSED_DIR, XHS_INPUT_DIR


KEY_ROOT = Path("/Users/mark/Documents/key/DcarKey")
ENDPOINT = "https://api-gw.onebound.cn/smallredbook/item_review/"
SUCCESS_CODES = {"0000", "0"}
STOP_CODES = {"4003", "4004", "4005", "4006", "4007", "4009", "4010", "4012", "4013", "4014", "4016"}
NOTE_ID_RE = re.compile(r"^[0-9a-fA-F]{24}$")
SUB_COMMENT_KEYS = ("sub", "sub_comments", "subComments", "subCommentList", "replies")
COMMENT_LIST_KEYS = ("item", "items", "list", "comments", "comment_list", "commentList")
TEXT_KEYS = ("rate_content", "content", "comment_content", "commentContent", "text")
USER_ID_KEYS = ("user_id", "userId", "userid", "user_num_id", "userNumId")
COMMENT_ID_KEYS = ("comment_id", "commentId", "id")


class NoRedirect(urllib.request.HTTPRedirectHandler):
    def redirect_request(self, req, fp, code, msg, headers, newurl):  # noqa: ANN001
        return None


def load_env(path: Path) -> Dict[str, str]:
    if path.stat().st_mode & 0o077:
        raise ValueError(f"{path.name} must not be readable by group or other users")
    values: Dict[str, str] = {}
    for raw_line in path.read_text(encoding="utf-8-sig").splitlines():
        line = raw_line.strip()
        if not line or line.startswith("#"):
            continue
        if "=" not in line:
            raise ValueError(f"Invalid line in {path.name}; expected NAME=value")
        name, value = line.split("=", 1)
        values[name.strip()] = value.strip().strip('"').strip("'")
    key_value = values.get("ONEBOUND_KEY") or values.get("onebound_key")
    secret_value = values.get("ONEBOUND_SECRET") or values.get("onebound_secret")
    missing = []
    if not key_value:
        missing.append("ONEBOUND_KEY")
    if not secret_value:
        missing.append("ONEBOUND_SECRET")
    if missing:
        raise ValueError(f"Missing required fields in {path.name}: {', '.join(missing)}")
    values["ONEBOUND_KEY"] = key_value
    values["ONEBOUND_SECRET"] = secret_value
    return values


def sanitize(value: Any, secrets: Iterable[str], limit: int = 160) -> str:
    text = re.sub(r"[\r\n\t]+", " ", str(value or "")).strip()
    for secret in secrets:
        if secret:
            text = text.replace(secret, "[REDACTED]")
    text = re.sub(r"([?&](?:key|secret)=)[^&\s]+", r"\1[REDACTED]", text, flags=re.I)
    return text[:limit]


def normalize_code(value: Any) -> str:
    if value is None:
        return ""
    if isinstance(value, bool):
        return str(int(value))
    if isinstance(value, (int, float)) and not isinstance(value, bool):
        return str(int(value))
    return str(value).strip()


def _dict_list(value: Any) -> List[Dict[str, Any]]:
    if isinstance(value, list):
        return [item for item in value if isinstance(item, dict)]
    if isinstance(value, dict):
        if any(key in value for key in TEXT_KEYS + USER_ID_KEYS + COMMENT_ID_KEYS):
            return [value]
        return [item for item in value.values() if isinstance(item, dict)]
    return []


def comment_items(payload: Dict[str, Any]) -> List[Dict[str, Any]]:
    """Locate first-level comments across known OneBound response envelopes."""

    candidates: List[Dict[str, Any]] = []
    seen_objects = set()

    def append_many(value: Any) -> None:
        for item in _dict_list(value):
            marker = id(item)
            if marker not in seen_objects:
                candidates.append(item)
                seen_objects.add(marker)

    items = payload.get("items")
    if isinstance(items, list):
        append_many(items)
    elif isinstance(items, dict):
        for key in COMMENT_LIST_KEYS:
            if key in items:
                append_many(items[key])

    data = payload.get("data")
    if isinstance(data, list):
        append_many(data)
    elif isinstance(data, dict):
        for key in COMMENT_LIST_KEYS:
            if key not in data:
                continue
            nested = data[key]
            if isinstance(nested, dict) and not any(
                field in nested for field in TEXT_KEYS + USER_ID_KEYS + COMMENT_ID_KEYS
            ):
                for child_key in COMMENT_LIST_KEYS:
                    if child_key in nested:
                        append_many(nested[child_key])
            else:
                append_many(nested)

    for key in ("comments", "comment_list", "commentList", "list"):
        if key in payload:
            append_many(payload[key])
    return candidates


def _nested_value(node: Mapping[str, Any], keys: Iterable[str]) -> Any:
    for key in keys:
        value = node.get(key)
        if value not in (None, ""):
            return value
    for container_key in ("user", "user_info", "userInfo", "author"):
        container = node.get(container_key)
        if isinstance(container, Mapping):
            for key in keys:
                value = container.get(key)
                if value not in (None, ""):
                    return value
    return None


def _truthy(value: Any) -> bool:
    if isinstance(value, str):
        return value.strip().lower() in {"1", "true", "yes", "author", "owner"}
    return bool(value)


def _digest(value: Any, secret: str, kind: str) -> str | None:
    if value in (None, ""):
        return None
    message = f"{kind}:{value}".encode("utf-8")
    return hmac.new(secret.encode("utf-8"), message, hashlib.sha256).hexdigest()[:24]


def _comment_text(node: Mapping[str, Any]) -> str:
    values: List[str] = []
    for key in TEXT_KEYS:
        value = node.get(key)
        if isinstance(value, str) and value.strip():
            values.append(value.strip())
    if not values:
        feedback = node.get("add_feedback")
        if isinstance(feedback, str) and feedback.strip():
            values.append(feedback.strip())
    if not values:
        # OneBound's sample uses voice_count for a speech-to-text transcript.
        # Numeric values are counts and must not become fake comment text.
        voice = node.get("voice_count")
        if isinstance(voice, str) and voice.strip() and not re.fullmatch(r"[\d\s.,]+", voice):
            values.append(voice.strip())
    unique: List[str] = []
    for value in values:
        collapsed = re.sub(r"\s+", " ", value).strip()
        if collapsed and collapsed not in unique:
            unique.append(collapsed)
    return " / ".join(unique)


def _text_exclusion_reason(text: str) -> str | None:
    if not text:
        return "empty_text"
    without_mentions = re.sub(r"@[\w\-\u4e00-\u9fff.]+", "", text).strip()
    semantic = re.sub(r"[\W_]+", "", without_mentions, flags=re.UNICODE)
    if not semantic:
        return "no_semantic_text"
    spam_patterns = (
        r"(?:加|联系)(?:我)?(?:微信|vx|v信)",
        r"互(?:赞|关|粉)",
        r"点赞.*关注|关注.*点赞",
        r"抽奖口令|进群|代运营",
    )
    if any(re.search(pattern, text, flags=re.I) for pattern in spam_patterns):
        return "spam_or_engagement_bait"
    return None


def extract_comment_records(
    nodes: Iterable[Dict[str, Any]], secret: str
) -> Tuple[List[Dict[str, Any]], int]:
    """Flatten comments and sub-comments without retaining raw identities."""

    records: List[Dict[str, Any]] = []
    node_count = 0

    def visit(node: Dict[str, Any], parent_hash: str | None = None) -> None:
        nonlocal node_count
        node_count += 1
        text = _comment_text(node)
        user_hash = _digest(_nested_value(node, USER_ID_KEYS), secret, "user")
        comment_hash = _digest(_nested_value(node, COMMENT_ID_KEYS), secret, "comment")
        is_author = any(
            _truthy(node.get(key))
            for key in ("is_author", "isAuthor", "is_note_author", "isNoteAuthor", "author_reply")
        )
        exclusion = "author_reply" if is_author else _text_exclusion_reason(text)
        if exclusion is None and user_hash is None:
            exclusion = "missing_user_identity"
        records.append(
            {
                "comment_hash": comment_hash,
                "user_hash": user_hash,
                "parent_comment_hash": parent_hash,
                "text": text,
                "is_author_reply": is_author,
                "is_valid": exclusion is None,
                "exclusion_reason": exclusion,
            }
        )
        for key in SUB_COMMENT_KEYS:
            for sub in _dict_list(node.get(key)):
                visit(sub, comment_hash)

    for node in nodes:
        visit(node)
    return records, node_count


def aggregate_valid_users(records: Iterable[Dict[str, Any]]) -> List[Dict[str, Any]]:
    """Aggregate all usable texts per independent external commenter."""

    by_user: Dict[str, List[str]] = {}
    for record in records:
        if not record.get("is_valid") or not record.get("user_hash"):
            continue
        user_hash = str(record["user_hash"])
        text = str(record.get("text") or "").strip()
        bucket = by_user.setdefault(user_hash, [])
        if text and text not in bucket:
            bucket.append(text)
    return [{"user_hash": key, "texts": texts} for key, texts in by_user.items()]


def response_cursor(payload: Dict[str, Any]) -> Tuple[str, bool]:
    containers = [payload]
    for key in ("items", "data", "page", "pagination"):
        value = payload.get(key)
        if isinstance(value, dict):
            containers.insert(0, value)
    cursor: Any = ""
    has_more: Any = False
    for container in containers:
        for key in ("next_cursor", "nextCursor", "cursor", "last_id", "lastId"):
            if container.get(key) not in (None, ""):
                cursor = container[key]
                break
        for key in ("has_more", "hasMore"):
            if key in container:
                has_more = container[key]
                break
        if cursor or has_more:
            break
    if isinstance(has_more, str):
        has_more = has_more.lower() in {"1", "true", "yes"}
    else:
        has_more = bool(has_more)
    return str(cursor or ""), has_more


def response_note_ids(nodes: Iterable[Dict[str, Any]]) -> List[str]:
    result: List[str] = []

    def visit(node: Dict[str, Any]) -> None:
        value = node.get("num_iid")
        if value is not None and str(value).strip():
            result.append(str(value).strip())
        for key in SUB_COMMENT_KEYS:
            for sub in _dict_list(node.get(key)):
                visit(sub)

    for node in nodes:
        visit(node)
    return result


def request_page(key: str, secret: str, note_id: str, cursor: str, timeout: int) -> Tuple[int, Dict[str, Any], bool]:
    params = {"key": key, "secret": secret, "num_iid": note_id, "result_type": "json"}
    if cursor:
        params["cursor"] = cursor
    query = urllib.parse.urlencode(params)
    request = urllib.request.Request(
        f"{ENDPOINT}?{query}",
        headers={"Accept": "application/json", "Accept-Encoding": "identity", "Connection": "close"},
        method="GET",
    )
    opener = urllib.request.build_opener(NoRedirect())
    try:
        with opener.open(request, timeout=timeout) as response:
            status = int(response.status)
            body = response.read()
    except urllib.error.HTTPError as exc:
        status = int(exc.code)
        body = exc.read()
    try:
        payload = json.loads(body.decode("utf-8-sig"))
    except (json.JSONDecodeError, UnicodeDecodeError):
        return status, {}, False
    if not isinstance(payload, dict):
        return status, {}, False
    return status, payload, True


def choose_rows(path: Path, per_label: int) -> List[Dict[str, str]]:
    with path.open(encoding="utf-8-sig", newline="") as handle:
        rows = list(csv.DictReader(handle))
    selected: List[Dict[str, str]] = []
    counts = {"auto": 0, "non_auto": 0}
    seen_note_ids = set()
    for row in rows:
        label = row.get("gold_label", "")
        note_id = row.get("note_id", "").strip()
        if not NOTE_ID_RE.fullmatch(note_id) or note_id.lower() in seen_note_ids:
            continue
        if label in counts and counts[label] < per_label:
            selected.append(row)
            counts[label] += 1
            seen_note_ids.add(note_id.lower())
    missing = [label for label, count in counts.items() if count < per_label]
    if missing:
        raise ValueError(f"Not enough parsed notes for: {', '.join(missing)}")
    return selected


def main() -> int:
    parser = argparse.ArgumentParser()
    parser.add_argument("--input", type=Path, default=XHS_INPUT_DIR / "notes_unique.csv")
    parser.add_argument("--env", type=Path, default=KEY_ROOT / "onebound.env")
    parser.add_argument("--per-label", type=int, default=1)
    parser.add_argument("--max-pages", type=int, default=1)
    parser.add_argument("--max-requests", type=int, default=2)
    parser.add_argument("--max-texts", type=int, default=50)
    parser.add_argument("--min-valid-users", type=int, default=20)
    parser.add_argument("--target-valid-users", type=int, default=30)
    parser.add_argument("--timeout", type=int, default=45)
    parser.add_argument("--prefix", default="onebound_smoke")
    args = parser.parse_args()
    if min(
        args.per_label,
        args.max_pages,
        args.max_requests,
        args.max_texts,
        args.min_valid_users,
        args.target_valid_users,
    ) < 1:
        parser.error("numeric limits must be at least 1")
    if args.target_valid_users < args.min_valid_users:
        parser.error("target-valid-users must be at least min-valid-users")

    env = load_env(args.env)
    key = env["ONEBOUND_KEY"]
    secret = env["ONEBOUND_SECRET"]
    selected = choose_rows(args.input, args.per_label)
    result_path = ARCHIVE_PROCESSED_DIR / f"{args.prefix}_results.csv"
    comments_path = ARCHIVE_PROCESSED_DIR / f"{args.prefix}_comments.jsonl"
    result_rows: List[Dict[str, Any]] = []
    comment_records: List[Dict[str, Any]] = []
    global_stop = False
    total_attempt_count = 0
    print(json.dumps({"request_budget": args.max_requests, "selected_notes": len(selected)}, ensure_ascii=False))

    for row in selected:
        note_id = row["note_id"]
        label = row["gold_label"]
        started = time.monotonic()
        attempt_count = 0
        page_count = 0
        http_status = ""
        code = ""
        reason = ""
        request_id = ""
        all_records: List[Dict[str, Any]] = []
        comment_count = 0
        has_more = False
        cursor = ""
        seen_cursors = {""}
        status = "request_failed"

        for page_index in range(args.max_pages):
            if total_attempt_count >= args.max_requests:
                status = "request_budget_exhausted" if page_count == 0 else "success_partial"
                reason = "request budget reached"
                break
            total_attempt_count += 1
            attempt_count += 1
            try:
                http_status, payload, json_ok = request_page(key, secret, note_id, cursor, args.timeout)
            except (urllib.error.URLError, socket.timeout, TimeoutError, OSError):
                reason = "network_or_timeout_error"
                break
            if not json_ok:
                if http_status != 200:
                    status = "http_error"
                    reason = f"unexpected HTTP status {http_status}"
                    if 300 <= int(http_status) < 400 or int(http_status) in {401, 403}:
                        global_stop = True
                else:
                    status = "invalid_api_response"
                    reason = "invalid_api_response"
                break

            page_count += 1
            code = normalize_code(payload.get("error_code"))
            reason = sanitize(payload.get("reason") or payload.get("error"), (key, secret))
            request_id = sanitize(payload.get("request_id"), (key, secret), 80)
            if http_status != 200:
                status = "http_error"
                reason = f"unexpected HTTP status {http_status}"
                if 300 <= int(http_status) < 400 or int(http_status) in {401, 403}:
                    global_stop = True
                break
            api_type = str(payload.get("api_type") or "")
            if api_type and api_type != "smallredbook":
                status = "unexpected_api_type"
                reason = "API response api_type did not match smallredbook"
                break
            if code not in SUCCESS_CODES:
                if code == "4010":
                    status = "provider_api_unavailable"
                elif code == "4012":
                    status = "provider_api_not_enabled"
                else:
                    status = "no_result" if code == "2000" else "api_error"
                if code in STOP_CODES:
                    global_stop = True
                break

            page_nodes = comment_items(payload)
            returned_ids = response_note_ids(page_nodes)
            if any(value.lower() != note_id.lower() for value in returned_ids):
                status = "unexpected_note_id"
                reason = "response comment num_iid did not match requested note_id"
                break
            page_records, page_node_count = extract_comment_records(page_nodes, secret)
            comment_count += page_node_count
            all_records.extend(page_records)
            next_cursor, has_more = response_cursor(payload)
            if not has_more:
                status = "success_complete"
                break
            if not next_cursor or next_cursor in seen_cursors:
                status = "pagination_stalled"
                reason = "has_more was true but cursor was empty or repeated"
                break
            valid_users = aggregate_valid_users(all_records)
            valid_text_count = sum(len(user["texts"]) for user in valid_users)
            if len(valid_users) >= args.target_valid_users:
                status = "success_partial"
                reason = "valid independent commenter target reached"
                break
            if valid_text_count >= args.max_texts:
                status = "success_partial"
                reason = "effective text safety limit reached"
                break
            if page_index + 1 >= args.max_pages:
                status = "success_partial"
                reason = "max page limit reached"
                break
            if total_attempt_count >= args.max_requests:
                status = "success_partial"
                reason = "request budget reached"
                break
            cursor = next_cursor
            seen_cursors.add(cursor)

        valid_users = aggregate_valid_users(all_records)
        parsed_comment_count = sum(bool(str(record.get("text") or "").strip()) for record in all_records)
        unique_external_commenters = len(
            {
                str(record["user_hash"])
                for record in all_records
                if record.get("user_hash") and not record.get("is_author_reply")
            }
        )
        valid_text_count = sum(len(user["texts"]) for user in valid_users)
        exclusion_counts = Counter(
            str(record["exclusion_reason"])
            for record in all_records
            if record.get("exclusion_reason")
        )
        pagination_complete = status == "success_complete"
        if status in {"success_complete", "success_partial"}:
            if comment_count > 0 and parsed_comment_count == 0:
                status = "parse_failed"
                reason = "API returned comment nodes but no comment text could be parsed"
            elif comment_count == 0 and pagination_complete:
                status = "confirmed_zero"
                reason = "API completed and returned zero comment nodes"
            elif len(valid_users) >= args.min_valid_users:
                status = "qualified"
                if not reason:
                    reason = "minimum valid independent commenter threshold reached"
            elif pagination_complete:
                status = "insufficient_comments"
                reason = "pagination completed below the valid independent commenter threshold"
            else:
                status = "partial_insufficient_comments"
                reason = "collection stopped before reaching the valid independent commenter threshold"
        elapsed = round(time.monotonic() - started, 3)
        result_rows.append(
            {
                "sample_id": row["sample_id"],
                "gold_label": label,
                "note_id": note_id,
                "status": status,
                "http_status": http_status,
                "error_code": code,
                "reason": reason,
                "attempt_count": attempt_count,
                "page_count": page_count,
                "comment_node_count": comment_count,
                "parsed_comment_count": parsed_comment_count,
                "unique_external_commenters": (
                    unique_external_commenters if code in SUCCESS_CODES else None
                ),
                "valid_unique_commenters": len(valid_users) if code in SUCCESS_CODES else None,
                "effective_text_count": valid_text_count,
                "exclusion_counts": json.dumps(exclusion_counts, ensure_ascii=False, sort_keys=True),
                "has_more": int(has_more),
                "pagination_complete": int(pagination_complete),
                "request_id": request_id,
                "elapsed_seconds": elapsed,
            }
        )
        comment_records.append(
            {
                "sample_id": row["sample_id"],
                "gold_label": label,
                "note_id": note_id,
                "status": status,
                "valid_unique_commenters": len(valid_users) if code in SUCCESS_CODES else None,
                "users": valid_users,
                "exclusion_counts": dict(exclusion_counts),
            }
        )
        print(
            json.dumps(
                {
                    "sample_id": row["sample_id"],
                    "gold_label": label,
                    "status": status,
                    "error_code": code,
                    "attempts": attempt_count,
                    "pages": page_count,
                    "valid_unique_commenters": len(valid_users) if code in SUCCESS_CODES else None,
                    "effective_text_count": valid_text_count,
                },
                ensure_ascii=False,
            )
        )
        if global_stop:
            break

    fieldnames = [
        "sample_id",
        "gold_label",
        "note_id",
        "status",
        "http_status",
        "error_code",
        "reason",
        "attempt_count",
        "page_count",
        "comment_node_count",
        "parsed_comment_count",
        "unique_external_commenters",
        "valid_unique_commenters",
        "effective_text_count",
        "exclusion_counts",
        "has_more",
        "pagination_complete",
        "request_id",
        "elapsed_seconds",
    ]
    with result_path.open("w", encoding="utf-8-sig", newline="") as handle:
        writer = csv.DictWriter(handle, fieldnames=fieldnames)
        writer.writeheader()
        writer.writerows(result_rows)
    with comments_path.open("w", encoding="utf-8") as handle:
        for record in comment_records:
            handle.write(json.dumps(record, ensure_ascii=False) + "\n")
    print(json.dumps({"results": result_path.name, "comments": comments_path.name, "requests": total_attempt_count}, ensure_ascii=False))
    passing = {"qualified"}
    return 0 if len(result_rows) == len(selected) and all(r["status"] in passing for r in result_rows) else 2


if __name__ == "__main__":
    raise SystemExit(main())
