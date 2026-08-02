#!/usr/bin/env python3
"""Collect normalized public Xiaohongshu note data from blind pilot URLs.

The collector reads only the blind input, does not persist raw HTML, strips the
time-limited xsec query from output note URLs, and excludes author/commenter
identity fields.
"""

from __future__ import annotations

import argparse
import csv
import json
import re
import subprocess
import time
from html.parser import HTMLParser
from pathlib import Path
from typing import Any, Dict, Iterable, List, Optional, Tuple
from urllib.parse import urlsplit, urlunsplit

from project_paths import ARCHIVE_PROCESSED_DIR, XHS_INPUT_DIR


NOTE_ID_RE = re.compile(r"^[0-9a-fA-F]{24}$")
ALLOWED_HOSTS = {"www.xiaohongshu.com", "xiaohongshu.com"}
USER_AGENT = "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 Chrome/126 Safari/537.36"


class ScriptCollector(HTMLParser):
    def __init__(self) -> None:
        super().__init__()
        self._active = False
        self._buffer: List[str] = []
        self.scripts: List[str] = []

    def handle_starttag(self, tag: str, attrs: List[Tuple[str, str]]) -> None:
        if tag == "script":
            self._active = True
            self._buffer = []

    def handle_data(self, data: str) -> None:
        if self._active:
            self._buffer.append(data)

    def handle_endtag(self, tag: str) -> None:
        if tag == "script" and self._active:
            self.scripts.append("".join(self._buffer))
            self._active = False
            self._buffer = []


def clean_note_url(url: str) -> str:
    parts = urlsplit(url)
    return urlunsplit((parts.scheme, parts.netloc, parts.path, "", ""))


def validate_input_url(url: str, note_id: str) -> None:
    parts = urlsplit(url)
    if parts.scheme != "https" or parts.hostname not in ALLOWED_HOSTS:
        raise ValueError("input URL is not an allowed Xiaohongshu HTTPS URL")
    if not NOTE_ID_RE.fullmatch(note_id) or note_id.lower() not in parts.path.lower():
        raise ValueError("note_id is invalid or does not match URL path")


def fetch_html(url: str, timeout: int) -> Tuple[int, str, str]:
    marker = "__CODEX_HTTP_STATUS__"
    process = subprocess.run(
        [
            "curl",
            "-sS",
            "--max-time",
            str(timeout),
            "--connect-timeout",
            str(min(timeout, 10)),
            "--proto",
            "=https",
            "--max-redirs",
            "0",
            "-A",
            USER_AGENT,
            "-H",
            "Accept: text/html,application/xhtml+xml",
            "--write-out",
            f"\n{marker}%{{http_code}}",
            url,
        ],
        capture_output=True,
    )
    if process.returncode != 0:
        return 0, "", f"curl_error_{process.returncode}"
    output = process.stdout.decode("utf-8", "replace")
    suffix = f"\n{marker}"
    if suffix not in output:
        return 0, "", "missing_http_status"
    html, status_text = output.rsplit(suffix, 1)
    try:
        status = int(status_text.strip())
    except ValueError:
        return 0, "", "invalid_http_status"
    return status, html, ""


def parse_initial_state(html: str) -> Dict[str, Any]:
    collector = ScriptCollector()
    collector.feed(html)
    candidates = [script for script in collector.scripts if "__INITIAL_STATE__" in script and "noteDetailMap" in script]
    if not candidates:
        raise ValueError("initial_state_missing")
    script = max(candidates, key=len)
    start = script.find("{")
    if start < 0:
        raise ValueError("initial_state_object_missing")
    raw = script[start:].rstrip(" ;\n")
    raw = re.sub(r'(?<!["\\w])undefined(?!["\\w])', "null", raw)
    try:
        state = json.loads(raw)
    except json.JSONDecodeError as exc:
        raise ValueError("initial_state_invalid_json") from exc
    if not isinstance(state, dict):
        raise ValueError("initial_state_not_object")
    return state


def detail_object(state: Dict[str, Any], note_id: str) -> Dict[str, Any]:
    note_store = state.get("note")
    if not isinstance(note_store, dict):
        return {}
    detail_map = note_store.get("noteDetailMap")
    if not isinstance(detail_map, dict):
        return {}
    candidate = detail_map.get(note_id)
    if isinstance(candidate, dict):
        return candidate
    for key, value in detail_map.items():
        if str(key).lower() == note_id.lower() and isinstance(value, dict):
            return value
    return {}


def first_string(mapping: Dict[str, Any], names: Iterable[str]) -> str:
    for name in names:
        value = mapping.get(name)
        if isinstance(value, str) and value.strip():
            return value.strip()
    return ""


def safe_https_url(value: Any) -> str:
    if not isinstance(value, str):
        return ""
    text = value.strip()
    if text.startswith("http://"):
        return "https://" + text[len("http://"):]
    if text.startswith("https://"):
        return text
    return ""


def normalize_tags(raw: Any) -> List[str]:
    if isinstance(raw, dict):
        raw = list(raw.values())
    if not isinstance(raw, list):
        raw = [raw] if isinstance(raw, str) else []
    result: List[str] = []
    for entry in raw:
        if isinstance(entry, str):
            value = entry.strip()
        elif isinstance(entry, dict):
            value = first_string(entry, ("name", "tagName", "title"))
        else:
            value = ""
        if value and value not in result:
            result.append(value)
    return result


def normalize_images(raw: Any) -> List[Dict[str, Any]]:
    if not isinstance(raw, list):
        return []
    result: List[Dict[str, Any]] = []
    for index, entry in enumerate(raw):
        if not isinstance(entry, dict):
            continue
        url = ""
        for name in ("urlDefault", "url", "urlPre"):
            url = safe_https_url(entry.get(name))
            if url:
                break
        if not url:
            continue
        result.append(
            {
                "index": index,
                "url": url,
                "width": entry.get("width") if isinstance(entry.get("width"), int) else None,
                "height": entry.get("height") if isinstance(entry.get("height"), int) else None,
            }
        )
    return result


def collect_https_urls(value: Any, path: str = "", depth: int = 0) -> List[Dict[str, str]]:
    if depth > 10:
        return []
    result: List[Dict[str, str]] = []
    if isinstance(value, str) and safe_https_url(value):
        result.append({"path": path, "url": safe_https_url(value)})
    elif isinstance(value, dict):
        for key, child in value.items():
            child_path = f"{path}.{key}" if path else str(key)
            result.extend(collect_https_urls(child, child_path, depth + 1))
    elif isinstance(value, list):
        for index, child in enumerate(value):
            result.extend(collect_https_urls(child, f"{path}[{index}]", depth + 1))
    unique: List[Dict[str, str]] = []
    seen = set()
    for entry in result:
        if entry["url"] not in seen:
            seen.add(entry["url"])
            unique.append(entry)
    return unique


def normalize_comments(raw: Any, pilot_id: str, note_id: str) -> Tuple[List[Dict[str, Any]], Dict[str, Any]]:
    if not isinstance(raw, dict):
        return [], {"has_more": False, "cursor_present": False, "embedded_count": 0}
    top_level = raw.get("list")
    if not isinstance(top_level, list):
        top_level = []
    result: List[Dict[str, Any]] = []

    def add_comment(comment: Dict[str, Any], level: int, parent_index: Optional[int]) -> None:
        text = first_string(comment, ("content", "text", "comment"))
        if text:
            result.append(
                {
                    "pilot_id": pilot_id,
                    "note_id": note_id,
                    "level": level,
                    "parent_index": parent_index,
                    "comment_text": re.sub(r"\s+", " ", text).strip(),
                }
            )

    for top_index, comment in enumerate(top_level):
        if not isinstance(comment, dict):
            continue
        add_comment(comment, 1, None)
        children = comment.get("subComments") or comment.get("subCommentList") or comment.get("sub")
        if isinstance(children, dict):
            children = list(children.values())
        if isinstance(children, list):
            for child in children:
                if isinstance(child, dict):
                    add_comment(child, 2, top_index)

    metadata = {
        "has_more": bool(raw.get("hasMore")),
        "cursor_present": bool(raw.get("cursor")),
        "embedded_count": len(result),
    }
    return result, metadata


def normalize_interactions(raw: Any) -> Dict[str, Any]:
    if not isinstance(raw, dict):
        return {}
    allowed = ("likedCount", "collectedCount", "commentCount", "shareCount")
    return {key: raw.get(key) for key in allowed if raw.get(key) not in (None, "")}


def main() -> int:
    parser = argparse.ArgumentParser()
    parser.add_argument("--input", type=Path, default=XHS_INPUT_DIR / "pilot_sample_10_blind.csv")
    parser.add_argument("--limit", type=int, default=10)
    parser.add_argument("--timeout", type=int, default=35)
    parser.add_argument("--delay", type=float, default=0.6)
    parser.add_argument("--prefix", default="pilot_public")
    args = parser.parse_args()
    if args.limit < 1 or args.timeout < 1 or args.delay < 0:
        parser.error("invalid numeric limit")

    with args.input.open(encoding="utf-8-sig", newline="") as handle:
        rows = list(csv.DictReader(handle))[: args.limit]

    results: List[Dict[str, Any]] = []
    contents: List[Dict[str, Any]] = []
    comments: List[Dict[str, Any]] = []
    for index, row in enumerate(rows):
        started = time.monotonic()
        status = "fetch_error"
        reason = ""
        http_status = 0
        content: Dict[str, Any] = {}
        comment_meta = {"has_more": False, "cursor_present": False, "embedded_count": 0}
        try:
            validate_input_url(row["url"], row["note_id"])
            http_status, html, reason = fetch_html(row["url"], args.timeout)
            if http_status != 200:
                status = "http_error"
            elif not html:
                status = "empty_html"
            else:
                state = parse_initial_state(html)
                detail = detail_object(state, row["note_id"])
                note = detail.get("note") if isinstance(detail.get("note"), dict) else {}
                returned_note_id = first_string(note, ("noteId", "note_id", "id"))
                if not returned_note_id:
                    status = "detail_id_missing"
                elif returned_note_id.lower() != row["note_id"].lower():
                    status = "detail_id_mismatch"
                else:
                    normalized_comments, comment_meta = normalize_comments(detail.get("comments"), row["pilot_id"], row["note_id"])
                    comments.extend(normalized_comments)
                    content = {
                        "pilot_id": row["pilot_id"],
                        "note_id": row["note_id"],
                        "url": clean_note_url(row["url"]),
                        "title": first_string(note, ("title", "noteTitle")),
                        "desc": first_string(note, ("desc", "description")),
                        "note_type": first_string(note, ("type", "noteType")),
                        "tags": normalize_tags(note.get("tagList")),
                        "images": normalize_images(note.get("imageList")),
                        "video_urls": collect_https_urls(note.get("video")),
                        "published_at_ms": note.get("time") if isinstance(note.get("time"), int) else None,
                        "interactions": normalize_interactions(note.get("interactInfo")),
                        "comments_embedded": comment_meta,
                    }
                    contents.append(content)
                    status = "success"
        except (KeyError, ValueError) as exc:
            status = str(exc) if isinstance(exc, ValueError) else "input_schema_error"
            reason = status

        results.append(
            {
                "pilot_id": row.get("pilot_id", ""),
                "note_id": row.get("note_id", ""),
                "status": status,
                "http_status": http_status,
                "reason": reason,
                "title_present": int(bool(content.get("title"))),
                "desc_present": int(bool(content.get("desc"))),
                "image_count": len(content.get("images", [])),
                "video_url_count": len(content.get("video_urls", [])),
                "embedded_comment_count": comment_meta["embedded_count"],
                "comment_has_more": int(comment_meta["has_more"]),
                "elapsed_seconds": round(time.monotonic() - started, 3),
            }
        )
        print(json.dumps({"pilot_id": row.get("pilot_id", ""), "status": status, "embedded_comments": comment_meta["embedded_count"]}, ensure_ascii=False))
        if index + 1 < len(rows) and args.delay:
            time.sleep(args.delay)

    results_path = ARCHIVE_PROCESSED_DIR / f"{args.prefix}_fetch_results.csv"
    content_path = ARCHIVE_PROCESSED_DIR / f"{args.prefix}_content.jsonl"
    comments_path = ARCHIVE_PROCESSED_DIR / f"{args.prefix}_comments.jsonl"
    result_fields = [
        "pilot_id", "note_id", "status", "http_status", "reason", "title_present", "desc_present",
        "image_count", "video_url_count", "embedded_comment_count", "comment_has_more", "elapsed_seconds",
    ]
    with results_path.open("w", encoding="utf-8-sig", newline="") as handle:
        writer = csv.DictWriter(handle, fieldnames=result_fields)
        writer.writeheader()
        writer.writerows(results)
    with content_path.open("w", encoding="utf-8") as handle:
        for content in contents:
            handle.write(json.dumps(content, ensure_ascii=False) + "\n")
    with comments_path.open("w", encoding="utf-8") as handle:
        for comment in comments:
            handle.write(json.dumps(comment, ensure_ascii=False) + "\n")
    print(json.dumps({"results": results_path.name, "content": content_path.name, "comments": comments_path.name, "success": sum(r["status"] == "success" for r in results)}, ensure_ascii=False))
    return 0 if results and all(row["status"] == "success" for row in results) else 2


if __name__ == "__main__":
    raise SystemExit(main())
