#!/usr/bin/env python3
"""Fill missing Xiaohongshu content/comments via TikHub App V2."""

from __future__ import annotations

import argparse
import json
import re
import time
from dataclasses import asdict
from pathlib import Path
from typing import Any

from collect_rnote_full import read_input
from collect_rnote_pilot import (
    CacheStore,
    CollectorError,
    FatalProviderError,
    RnoteCursor,
    apply_duplicate_filter,
    comment_list,
    normalize_comment_tree,
    normalize_content,
    parse_cursor,
    read_json,
    read_jsonl,
    safe_int,
    utc_now,
    valid_user_hashes,
    write_json,
    write_jsonl,
)
from probe_tikhub_douyin import KEY_FILE, fetch, load_key
from project_paths import RNOTE_CACHE_DIR


IMAGE_ENDPOINT = "/api/v1/xiaohongshu/app_v2/get_image_note_detail"
VIDEO_ENDPOINT = "/api/v1/xiaohongshu/app_v2/get_video_note_detail"
COMMENTS_ENDPOINT = "/api/v1/xiaohongshu/app_v2/get_note_comments"
SUMMARY = RNOTE_CACHE_DIR / "tikhub_xhs_full_summary.json"
RESULTS = RNOTE_CACHE_DIR / "tikhub_xhs_full_results.jsonl"
TERMINAL_STOPS = {
    "target_valid_users", "end_of_comments", "confirmed_empty",
    "three_pages_without_new_users", "max_pages", "max_raw_comments",
}


def message(payload: Any) -> str:
    if not isinstance(payload, dict):
        return ""
    return str(payload.get("message_zh") or payload.get("message") or "")


def unwrap(payload: Any) -> Any:
    if not isinstance(payload, dict):
        raise CollectorError("TikHub returned non-object JSON")
    outer_message = message(payload)
    code = safe_int(payload.get("code"))
    if code != 200:
        if code in {401, 402, 403} or re.search(r"(?:余额|额度|鉴权|密钥|balance|credit|quota|auth)", outer_message, re.I):
            raise FatalProviderError(f"TikHub fatal provider error: {outer_message or code}")
        raise CollectorError(f"TikHub API error: {outer_message or code}")
    layer = payload.get("data")
    if isinstance(layer, dict) and "success" in layer:
        if layer.get("success") is False or layer.get("code") not in (None, 0, "0"):
            inner_message = str(layer.get("msg") or layer.get("message") or layer.get("code"))
            if re.search(r"(?:余额|额度|鉴权|密钥|balance|credit|quota|auth)", inner_message, re.I):
                raise FatalProviderError(f"TikHub fatal provider error: {inner_message}")
            raise CollectorError(f"TikHub upstream error: {inner_message}")
        layer = layer.get("data")
    if layer is None:
        raise CollectorError("TikHub response contains no data")
    return layer


class TikHubClient:
    def __init__(self, key: str, delay: float = 0.1) -> None:
        self.key = key
        self.delay = delay
        self.request_attempts = 0
        self.billed_requests = 0

    def get(self, endpoint: str, params: dict[str, Any]) -> Any:
        last: Exception | None = None
        for attempt in range(2):
            self.request_attempts += 1
            try:
                status, payload = fetch(endpoint, params, self.key)
                if status in {401, 402, 403}:
                    raise FatalProviderError(f"TikHub HTTP {status}: {message(payload)}")
                if status in {408, 429} or status >= 500:
                    raise TimeoutError(f"TikHub transient HTTP {status}")
                if status != 200:
                    raise CollectorError(f"TikHub HTTP {status}: {message(payload)}")
                value = unwrap(payload)
                self.billed_requests += 1
                if self.delay:
                    time.sleep(self.delay)
                return value
            except FatalProviderError:
                raise
            except (TimeoutError, ConnectionError, RuntimeError) as exc:
                last = exc
                if attempt == 0:
                    time.sleep(0.75)
                    continue
                raise CollectorError(f"TikHub transport error: {type(exc).__name__}") from exc
            except CollectorError:
                raise
        raise CollectorError(f"TikHub request failed: {last}")


def collect_content(
    row: dict[str, str],
    *,
    store: CacheStore,
    client: TikHubClient,
    refresh_missing_video: bool = False,
) -> dict[str, Any] | None:
    note_dir = store.note_dir(row["note_id"])
    content_path = note_dir / "content.json"
    metadata_path = note_dir / "collection.json"
    existing = read_json(content_path) if content_path.exists() else None
    needs_video_refresh = bool(
        refresh_missing_video
        and isinstance(existing, dict)
        and str(existing.get("note_type") or "").lower() == "video"
        and not existing.get("video_urls")
    )
    if existing is not None and not needs_video_refresh:
        return existing
    metadata = read_json(metadata_path, {}) or {}
    errors = []
    candidates: list[tuple[str, str]] = (
        [("video", VIDEO_ENDPOINT)]
        if needs_video_refresh
        else [("image", IMAGE_ENDPOINT), ("video", VIDEO_ENDPOINT)]
    )
    fallback_content: dict[str, Any] | None = existing
    for endpoint_type, endpoint in candidates:
        try:
            data = client.get(endpoint, {"note_id": row["note_id"]})
            content = normalize_content(data, row=row, endpoint_type=endpoint_type, store=store)
            content["provider"] = "tikhub"
            content["collector_version"] = "tikhub-xhs-full-v1.0"
            is_unresolved_video = (
                str(content.get("note_type") or "").lower() == "video"
                and not content.get("video_urls")
            )
            fallback_content = content
            if endpoint_type == "image" and is_unresolved_video:
                continue
            write_json(content_path, content)
            metadata.update({
                "schema_version": "xhs-cache-v2.0",
                "note_id": row["note_id"],
                "sample_attempt_id": row["sample_attempt_id"],
                "content": {
                    "status": "complete",
                    "endpoint_type": endpoint_type,
                    "provider": "tikhub",
                    "collected_at": utc_now(),
                    "media_resolution_status": (
                        "complete" if not is_unresolved_video else "unavailable"
                    ),
                },
                "updated_at": utc_now(),
            })
            write_json(metadata_path, metadata)
            return content
        except FatalProviderError:
            raise
        except CollectorError as exc:
            errors.append(f"{endpoint_type}:{exc}")
    if fallback_content is not None:
        write_json(content_path, fallback_content)
        unresolved = (
            str(fallback_content.get("note_type") or "").lower() == "video"
            and not fallback_content.get("video_urls")
        )
        metadata.update({
            "schema_version": "xhs-cache-v2.0",
            "note_id": row["note_id"],
            "sample_attempt_id": row["sample_attempt_id"],
            "content": {
                "status": "complete",
                "endpoint_type": str(fallback_content.get("endpoint_type") or ""),
                "provider": str(fallback_content.get("provider") or "tikhub"),
                "collected_at": utc_now(),
                "media_resolution_status": "unavailable" if unresolved else "complete",
                "media_resolution_error": " | ".join(errors)[:800],
            },
            "updated_at": utc_now(),
        })
        write_json(metadata_path, metadata)
        return fallback_content
    metadata.update({
        "schema_version": "xhs-cache-v2.0",
        "note_id": row["note_id"],
        "sample_attempt_id": row["sample_attempt_id"],
        "content": {"status": "failed", "provider": "tikhub", "error": " | ".join(errors)[:800]},
        "updated_at": utc_now(),
    })
    write_json(metadata_path, metadata)
    return None


def collect_comments(
    row: dict[str, str],
    *,
    content: dict[str, Any] | None,
    store: CacheStore,
    client: TikHubClient,
    max_pages: int,
) -> tuple[list[dict[str, Any]], dict[str, Any]]:
    note_dir = store.note_dir(row["note_id"])
    comments_path = note_dir / "comments.jsonl"
    metadata_path = note_dir / "collection.json"
    metadata = read_json(metadata_path, {}) or {}
    comments_meta = metadata.get("comments") if isinstance(metadata.get("comments"), dict) else {}
    records = read_jsonl(comments_path)
    apply_duplicate_filter(records)
    if comments_meta.get("stop_reason") in TERMINAL_STOPS:
        return records, metadata
    cursor_value = comments_meta.get("next_cursor")
    cursor = RnoteCursor(**cursor_value) if isinstance(cursor_value, dict) else RnoteCursor()
    seen = set(comments_meta.get("seen_cursors") or [])
    pages = safe_int(comments_meta.get("pages_fetched")) or 0
    no_new_pages = safe_int(comments_meta.get("consecutive_pages_without_new_users")) or 0
    stop_reason = ""
    author_hash = content.get("author_hash") if content else None
    while not stop_reason:
        before = valid_user_hashes(records)
        page = client.get(COMMENTS_ENDPOINT, {
            "note_id": row["note_id"],
            "cursor": cursor.cursor,
            "index": cursor.index,
            "pageArea": cursor.pageArea,
            "sort_strategy": "latest_v2",
        })
        if not isinstance(page, dict):
            raise CollectorError("TikHub comments payload is not an object")
        normalized, _ = normalize_comment_tree(
            comment_list(page), note_id=row["note_id"], store=store,
            author_hash=author_hash, start_order=len(records),
        )
        indexed = {str(item.get("comment_hash")): item for item in [*records, *normalized] if item.get("comment_hash")}
        records = sorted(indexed.values(), key=lambda item: int(item.get("retrieval_order") or 0))
        apply_duplicate_filter(records)
        pages += 1
        after = valid_user_hashes(records)
        no_new_pages = no_new_pages + 1 if len(after) == len(before) else 0
        has_more = bool(page.get("has_more") or page.get("hasMore"))
        next_cursor = parse_cursor(page.get("cursor"), cursor)
        if len(after) >= 20:
            stop_reason = "target_valid_users"
        elif not has_more:
            stop_reason = "confirmed_empty" if not records else "end_of_comments"
        elif no_new_pages >= 3:
            stop_reason = "three_pages_without_new_users"
        elif pages >= max_pages:
            stop_reason = "max_pages"
        elif len(records) >= 500:
            stop_reason = "max_raw_comments"
        elif next_cursor is None:
            raise CollectorError("TikHub has_more=true without a usable cursor")
        elif next_cursor.fingerprint() in seen:
            raise CollectorError("TikHub returned a repeated cursor")
        if next_cursor:
            seen.add(next_cursor.fingerprint())
        comments_meta = {
            "status": "confirmed_empty" if stop_reason == "confirmed_empty" else "complete" if stop_reason == "end_of_comments" else "partial",
            "provider": "tikhub",
            "collected_at": utc_now(),
            "pages_fetched": pages,
            "raw_comment_count": len(records),
            "valid_unique_commenters": len(after),
            "platform_comment_count": safe_int(page.get("comment_count") or page.get("comment_count_l1")),
            "pagination_complete": stop_reason in {"end_of_comments", "confirmed_empty"},
            "stop_reason": stop_reason or None,
            "next_cursor": asdict(next_cursor) if next_cursor else None,
            "seen_cursors": sorted(seen),
            "consecutive_pages_without_new_users": no_new_pages,
            "author_filter_available": bool(author_hash),
            "error": None,
        }
        metadata["comments"] = comments_meta
        metadata["updated_at"] = utc_now()
        write_jsonl(comments_path, records)
        write_json(metadata_path, metadata)
        if next_cursor:
            cursor = next_cursor
    return records, metadata


def collect_all(args: argparse.Namespace) -> dict[str, Any]:
    rows = read_input(args.input)
    store = CacheStore(args.cache_dir)
    client = TikHubClient(load_key(args.key_file), delay=args.delay)
    results = []
    stopped_error = ""
    for index, row in enumerate(rows, 1):
        try:
            content = collect_content(
                row,
                store=store,
                client=client,
                refresh_missing_video=args.refresh_missing_video,
            )
            comments, metadata = collect_comments(
                row, content=content, store=store, client=client, max_pages=args.max_pages
            ) if content else ([], read_json(store.note_dir(row["note_id"]) / "collection.json", {}) or {})
            comment_meta = metadata.get("comments") or {}
            valid = safe_int(comment_meta.get("valid_unique_commenters"))
            status = (
                "scorable" if valid is not None and valid >= 20
                else "confirmed_zero" if comment_meta.get("stop_reason") == "confirmed_empty"
                else "below_minimum" if comment_meta.get("status") == "complete"
                else "technical_missing"
            )
            results.append({
                "note_id": row["note_id"],
                "url": f"https://www.xiaohongshu.com/explore/{row['note_id']}",
                "content_status": (metadata.get("content") or {}).get("status") or ("complete" if content else "failed"),
                "comment_sample_status": status,
                "valid_unique_commenters": valid,
                "raw_comment_count": len(comments),
            })
        except FatalProviderError as exc:
            stopped_error = f"{type(exc).__name__}: {exc}"[:800]
            break
        except CollectorError as exc:
            results.append({
                "note_id": row["note_id"],
                "url": f"https://www.xiaohongshu.com/explore/{row['note_id']}",
                "content_status": "failed",
                "comment_sample_status": "technical_missing",
                "error": f"{type(exc).__name__}: {exc}"[:800],
            })
        write_jsonl(args.results, results)
        if index % 10 == 0 or index == len(rows):
            print(json.dumps({
                "progress": f"{index}/{len(rows)}",
                "request_attempts": client.request_attempts,
                "billed_requests": client.billed_requests,
            }, ensure_ascii=False), flush=True)
    summary = {
        "generated_at": utc_now(),
        "collector": "tikhub-xhs-full-v1.0",
        "total": len(rows),
        "results": len(results),
        "request_attempts_this_run": client.request_attempts,
        "billed_requests_this_run": client.billed_requests,
        "max_total_attempts_per_request": 2,
        "terminal_cache_reuse": True,
        "stopped_error": stopped_error,
        "privacy": "Only content-scoped HMAC user keys and semantic comment text are cached.",
    }
    write_json(args.summary, summary)
    return summary


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--input", type=Path, default=Path(__file__).resolve().parents[2] / "data/inputs/xiaohongshu/notes_unique.csv")
    parser.add_argument("--key-file", type=Path, default=KEY_FILE)
    parser.add_argument("--cache-dir", type=Path, default=RNOTE_CACHE_DIR)
    parser.add_argument("--results", type=Path, default=RESULTS)
    parser.add_argument("--summary", type=Path, default=SUMMARY)
    parser.add_argument("--delay", type=float, default=0.1)
    parser.add_argument("--max-pages", type=int, default=10)
    parser.add_argument(
        "--refresh-missing-video",
        action="store_true",
        help="Call the video-detail endpoint once for cached video notes without stream URLs.",
    )
    return parser.parse_args()


def main() -> int:
    summary = collect_all(parse_args())
    print(json.dumps(summary, ensure_ascii=False, indent=2))
    return 2 if summary.get("stopped_error") else 0


if __name__ == "__main__":
    raise SystemExit(main())
