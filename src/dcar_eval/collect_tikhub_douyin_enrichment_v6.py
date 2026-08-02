#!/usr/bin/env python3
"""Collect TikHub play counts and anonymized Douyin comments for the 438-post sample.

The collector is resumable and never writes the API key, raw user IDs, nicknames,
avatars, locations, or profile links. Statistics are fetched in batches of two,
which is the observed limit of the TikHub statistics route. Comment page one is
scanned for every post; posts whose API-reported total can reach the 20-valid-user
threshold are paged until the threshold, exhaustion, or ``--max-pages``.
"""

from __future__ import annotations

import argparse
from concurrent.futures import ThreadPoolExecutor, as_completed
import datetime as dt
import hashlib
import json
import re
from pathlib import Path
import threading
import time
from typing import Any, Iterable

from probe_tikhub_douyin import KEY_FILE, atomic_write_json, fetch, load_key
from project_paths import DOUYIN_PROCESSED_DIR, TIKHUB_CACHE_DIR


INPUT = DOUYIN_PROCESSED_DIR / "douyin_selling_point_labels_v4_full_publication_2026-08-02.jsonl"
DEFAULT_CACHE = TIKHUB_CACHE_DIR
STATS_ENDPOINT = "/api/v1/douyin/app/v3/fetch_video_statistics"
COMMENTS_ENDPOINT = "/api/v1/douyin/app/v3/fetch_video_comments"
SEMANTIC_RE = re.compile(r"[\u3400-\u9fffA-Za-z0-9]")
SPAM_RE = re.compile(
    r"(加(?:我|微|v|V)|私信|兼职|返现|刷单|引流|代理|进群|vx|v信|微信号|点击主页|直播间下单)",
    re.I,
)
PRINT_LOCK = threading.Lock()


def read_rows(path: Path = INPUT) -> list[dict[str, Any]]:
    return [json.loads(line) for line in path.read_text(encoding="utf-8").splitlines() if line.strip()]


def chunks(values: list[str], size: int) -> Iterable[list[str]]:
    for start in range(0, len(values), size):
        yield values[start : start + size]


def api_call(endpoint: str, params: dict[str, Any], key: str) -> tuple[int, dict[str, Any]]:
    last: Exception | None = None
    for attempt in range(3):
        try:
            status, payload = fetch(endpoint, params, key)
            if status == 200 and isinstance(payload, dict) and payload.get("code") == 200:
                return status, payload
            raise RuntimeError(f"HTTP {status}, API code {payload.get('code') if isinstance(payload, dict) else None}")
        except Exception as exc:  # bounded provider retry
            last = exc
            time.sleep(0.8 * (attempt + 1))
    assert last is not None
    raise last


def anon_user_key(aweme_id: str, user: Any) -> str:
    if not isinstance(user, dict):
        return ""
    raw = str(user.get("sec_uid") or user.get("uid") or user.get("unique_id") or "")
    if not raw:
        return ""
    return "U" + hashlib.sha256(f"{aweme_id}|{raw}".encode()).hexdigest()[:12]


def is_author(user: Any, author_uid: str) -> bool:
    if not isinstance(user, dict) or not author_uid:
        return False
    return str(user.get("uid") or "") == str(author_uid)


def clean_text(value: Any) -> str:
    return " ".join(str(value or "").replace("\x00", " ").split())[:1000]


def sanitize_comment(aweme_id: str, author_uid: str, item: Any, level: int = 1) -> dict[str, Any] | None:
    if not isinstance(item, dict):
        return None
    text = clean_text(item.get("text"))
    user = item.get("user")
    return {
        "user_key": anon_user_key(aweme_id, user),
        "is_author": is_author(user, author_uid),
        "text": text,
        "level": int(item.get("level") or level),
        "digg_count": int(item.get("digg_count") or 0),
    }


def sanitize_comment_page(
    *, aweme_id: str, author_uid: str, cursor_requested: int, payload: dict[str, Any]
) -> dict[str, Any]:
    data = payload.get("data") if isinstance(payload, dict) else {}
    data = data if isinstance(data, dict) else {}
    comments: list[dict[str, Any]] = []
    for item in data.get("comments") or []:
        normalized = sanitize_comment(aweme_id, author_uid, item, 1)
        if normalized:
            comments.append(normalized)
        for reply in item.get("reply_comment") or [] if isinstance(item, dict) else []:
            normalized_reply = sanitize_comment(aweme_id, author_uid, reply, 2)
            if normalized_reply:
                comments.append(normalized_reply)
    return {
        "schema_version": "1.0-anonymized",
        "fetched_at": dt.datetime.now(dt.timezone.utc).isoformat(),
        "aweme_id": aweme_id,
        "cursor_requested": cursor_requested,
        "cursor_next": int(data.get("cursor") or 0),
        "has_more": bool(data.get("has_more")),
        "reported_total": int(data.get("total") or 0),
        "comments": comments,
        "privacy_note": "TikHub user IDs were one-way hashed per post; nicknames and profile fields were not retained.",
    }


def valid_unique_comments(pages: Iterable[dict[str, Any]]) -> dict[str, str]:
    per_user: dict[str, list[str]] = {}
    for page in pages:
        for item in page.get("comments") or []:
            key = str(item.get("user_key") or "")
            text = clean_text(item.get("text"))
            if (
                not key
                or item.get("is_author")
                or not text
                or not SEMANTIC_RE.search(text)
                or SPAM_RE.search(text)
            ):
                continue
            texts = per_user.setdefault(key, [])
            if text not in texts and len(texts) < 3:
                texts.append(text)
    return {key: "；".join(texts) for key, texts in per_user.items() if texts}


def collect_stats(rows: list[dict[str, Any]], key: str, cache_dir: Path, workers: int, refresh: bool) -> None:
    stats_dir = cache_dir / "statistics"
    ids = [str(row["aweme_id"]) for row in rows]
    batches = list(chunks(ids, 2))

    def one(index: int, batch: list[str]) -> tuple[int, int]:
        path = stats_dir / f"batch_{index:03d}.json"
        if path.exists() and not refresh:
            record = json.loads(path.read_text(encoding="utf-8"))
            return index, len(record.get("statistics") or [])
        errors: list[dict[str, str]] = []
        try:
            _, payload = api_call(STATS_ENDPOINT, {"aweme_ids": ",".join(batch)}, key)
            data = payload.get("data") if isinstance(payload, dict) else {}
            items = data.get("statistics_list") if isinstance(data, dict) else []
        except Exception as batch_exc:
            # A deleted/private work can make a two-ID request fail. Split it so
            # the neighboring valid work is still retained and the bad ID is
            # recorded without stopping the 438-item run.
            items = []
            errors.append({"scope": "batch", "error": f"{type(batch_exc).__name__}: {batch_exc}"})
            for aweme_id in batch:
                try:
                    _, single_payload = api_call(STATS_ENDPOINT, {"aweme_ids": aweme_id}, key)
                    single_data = single_payload.get("data") if isinstance(single_payload, dict) else {}
                    items.extend(single_data.get("statistics_list") or [] if isinstance(single_data, dict) else [])
                except Exception as single_exc:
                    errors.append({
                        "scope": "single",
                        "aweme_id": aweme_id,
                        "error": f"{type(single_exc).__name__}: {single_exc}",
                    })
        keep = []
        for item in items or []:
            if not isinstance(item, dict):
                continue
            keep.append({
                "aweme_id": str(item.get("aweme_id") or ""),
                "play_count": int(item.get("play_count") or 0),
                "digg_count": int(item.get("digg_count") or 0),
                "share_count": int(item.get("share_count") or 0),
            })
        atomic_write_json(path, {
            "schema_version": "1.0",
            "fetched_at": dt.datetime.now(dt.timezone.utc).isoformat(),
            "requested_count": len(batch),
            "statistics": keep,
            "errors": errors,
        })
        return index, len(keep)

    completed = 0
    returned = 0
    with ThreadPoolExecutor(max_workers=workers) as pool:
        futures = [pool.submit(one, index, batch) for index, batch in enumerate(batches, 1)]
        for future in as_completed(futures):
            _, count = future.result()
            completed += 1
            returned += count
            if completed % 25 == 0 or completed == len(batches):
                with PRINT_LOCK:
                    print(f"STATS_PROGRESS {completed}/{len(batches)} returned={returned}", flush=True)


def page_path(cache_dir: Path, aweme_id: str, page_number: int) -> Path:
    return cache_dir / "comments" / aweme_id / f"page_{page_number:03d}.json"


def collect_comment_page(
    *, row: dict[str, Any], key: str, cache_dir: Path, page_number: int,
    cursor: int, refresh: bool,
) -> dict[str, Any]:
    path = page_path(cache_dir, str(row["aweme_id"]), page_number)
    if path.exists() and not refresh:
        return json.loads(path.read_text(encoding="utf-8"))
    _, payload = api_call(
        COMMENTS_ENDPOINT,
        {"aweme_id": str(row["aweme_id"]), "cursor": cursor, "count": 20},
        key,
    )
    page = sanitize_comment_page(
        aweme_id=str(row["aweme_id"]),
        author_uid=str(row.get("uid") or ""),
        cursor_requested=cursor,
        payload=payload,
    )
    atomic_write_json(path, page)
    return page


def collect_comment_first_pages(
    rows: list[dict[str, Any]], key: str, cache_dir: Path, workers: int, refresh: bool
) -> None:
    def one(row: dict[str, Any]) -> dict[str, Any]:
        return collect_comment_page(
            row=row, key=key, cache_dir=cache_dir, page_number=1, cursor=0, refresh=refresh
        )

    completed = 0
    with ThreadPoolExecutor(max_workers=workers) as pool:
        futures = [pool.submit(one, row) for row in rows]
        for future in as_completed(futures):
            future.result()
            completed += 1
            if completed % 25 == 0 or completed == len(rows):
                with PRINT_LOCK:
                    print(f"COMMENTS_PAGE1_PROGRESS {completed}/{len(rows)}", flush=True)


def collect_comment_followups(
    rows: list[dict[str, Any]], key: str, cache_dir: Path, refresh: bool,
    target_users: int, max_pages: int,
) -> None:
    eligible = 0
    reached = 0
    for index, row in enumerate(rows, 1):
        aweme_id = str(row["aweme_id"])
        first = json.loads(page_path(cache_dir, aweme_id, 1).read_text(encoding="utf-8"))
        if int(first.get("reported_total") or 0) < target_users:
            continue
        eligible += 1
        pages = [first]
        page_number = 1
        while (
            len(valid_unique_comments(pages)) < target_users
            and pages[-1].get("has_more")
            and page_number < max_pages
        ):
            cursor = int(pages[-1].get("cursor_next") or 0)
            if not cursor:
                break
            page_number += 1
            pages.append(collect_comment_page(
                row=row, key=key, cache_dir=cache_dir, page_number=page_number,
                cursor=cursor, refresh=refresh,
            ))
        if len(valid_unique_comments(pages)) >= target_users:
            reached += 1
        if eligible % 20 == 0:
            print(f"COMMENTS_FOLLOWUP eligible={eligible} reached={reached} scanned={index}/{len(rows)}", flush=True)
    print(f"COMMENTS_FOLLOWUP_DONE eligible={eligible} reached={reached}", flush=True)


def write_collection_summary(rows: list[dict[str, Any]], cache_dir: Path, target_users: int) -> None:
    stats: dict[str, dict[str, Any]] = {}
    for path in sorted((cache_dir / "statistics").glob("batch_*.json")):
        record = json.loads(path.read_text(encoding="utf-8"))
        for item in record.get("statistics") or []:
            stats[str(item.get("aweme_id") or "")] = item
    comment_status = []
    for row in rows:
        aweme_id = str(row["aweme_id"])
        pages = [json.loads(path.read_text(encoding="utf-8")) for path in sorted((cache_dir / "comments" / aweme_id).glob("page_*.json"))]
        users = valid_unique_comments(pages)
        first = pages[0] if pages else {}
        comment_status.append({
            "aweme_id": aweme_id,
            "reported_total": int(first.get("reported_total") or 0),
            "pages": len(pages),
            "valid_unique_commenters": len(users),
            "threshold_reached": len(users) >= target_users,
        })
    atomic_write_json(cache_dir / "collection_summary.json", {
        "schema_version": "1.0",
        "generated_at": dt.datetime.now(dt.timezone.utc).isoformat(),
        "total_publications": len(rows),
        "statistics_mapped": len(stats),
        "positive_play_counts": sum(int(item.get("play_count") or 0) > 0 for item in stats.values()),
        "comment_page_one_scanned": sum(item["pages"] >= 1 for item in comment_status),
        "comment_reported_total_at_least_20": sum(item["reported_total"] >= target_users for item in comment_status),
        "comment_threshold_reached": sum(item["threshold_reached"] for item in comment_status),
        "comment_threshold": target_users,
        "comment_status": comment_status,
        "privacy": "No API key, raw comment-user ID, nickname, avatar, location, or profile link is stored.",
    })


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--stage", choices=("stats", "comments", "all", "summary"), default="all")
    parser.add_argument("--key-file", type=Path, default=KEY_FILE)
    parser.add_argument("--cache-dir", type=Path, default=DEFAULT_CACHE)
    parser.add_argument("--workers", type=int, default=5)
    parser.add_argument("--target-users", type=int, default=20)
    parser.add_argument("--max-pages", type=int, default=10)
    parser.add_argument("--refresh", action="store_true")
    parser.add_argument("--limit", type=int, default=0)
    return parser.parse_args()


def main() -> int:
    args = parse_args()
    rows = read_rows()
    if args.limit:
        rows = rows[: args.limit]
    args.cache_dir.mkdir(parents=True, exist_ok=True)
    key = load_key(args.key_file) if args.stage != "summary" else ""
    if args.stage in {"stats", "all"}:
        collect_stats(rows, key, args.cache_dir, args.workers, args.refresh)
    if args.stage in {"comments", "all"}:
        collect_comment_first_pages(rows, key, args.cache_dir, args.workers, args.refresh)
        collect_comment_followups(
            rows, key, args.cache_dir, args.refresh, args.target_users, args.max_pages
        )
    write_collection_summary(rows, args.cache_dir, args.target_users)
    print(args.cache_dir / "collection_summary.json")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
