#!/usr/bin/env python3
"""Cache a bounded TikHub acceptance probe for paid Douyin/XHS routes."""

from __future__ import annotations

import argparse
import datetime as dt
import json
from pathlib import Path
from typing import Any

from probe_tikhub_douyin import (
    KEY_FILE,
    atomic_write_json,
    fetch,
    load_key,
    redact,
    safe_message,
)


ROOT = Path(__file__).resolve().parent


def cached_call(
    *,
    name: str,
    endpoint: str,
    params: dict[str, Any],
    cache_dir: Path,
    key: str,
    refresh: bool,
) -> tuple[dict[str, Any], dict[str, Any]]:
    cache_path = cache_dir / f"{name}.json"
    source = "cache"
    if cache_path.exists() and not refresh:
        record = json.loads(cache_path.read_text(encoding="utf-8"))
    else:
        source = "api"
        status, payload = fetch(endpoint, params, key)
        record = {
            "fetched_at": dt.datetime.now(dt.timezone.utc).isoformat(),
            "http_status": status,
            "request": {"endpoint": endpoint, "params": params},
            "response": redact(payload, key),
        }
        atomic_write_json(cache_path, record)
    payload = record.get("response") or {}
    summary = {
        "name": name,
        "source": source,
        "http_status": record.get("http_status"),
        "api_code": payload.get("code") if isinstance(payload, dict) else None,
        "message": safe_message(payload),
        "cache_path": str(cache_path),
    }
    return record, summary


def nested_data(payload: Any) -> dict[str, Any]:
    value = payload.get("data") if isinstance(payload, dict) else None
    for _ in range(3):
        if not isinstance(value, dict):
            return {}
        if any(key in value for key in ("comments", "comment_list", "cursor", "has_more")):
            return value
        nested = value.get("data")
        if not isinstance(nested, dict):
            return value
        value = nested
    return value if isinstance(value, dict) else {}


def run(args: argparse.Namespace) -> list[dict[str, Any]]:
    key = load_key(args.key_file)
    summaries: list[dict[str, Any]] = []
    douyin_sec_user_id = args.douyin_sec_user_id
    if args.douyin_uid:
        uid_record, uid_summary = cached_call(
            name="douyin_uid_resolve",
            endpoint="/api/v1/douyin/web/encrypt_uid_to_sec_user_id",
            params={"uid": args.douyin_uid},
            cache_dir=args.cache_dir,
            key=key,
            refresh=args.refresh,
        )
        summaries.append(uid_summary)
        uid_payload = uid_record.get("response") or {}
        uid_data = uid_payload.get("data") if isinstance(uid_payload, dict) else None
        if isinstance(uid_data, dict) and uid_data.get("sec_user_id"):
            douyin_sec_user_id = str(uid_data["sec_user_id"])
    if not douyin_sec_user_id:
        raise RuntimeError("Douyin sec_user_id could not be resolved")
    calls = [
        (
            "douyin_user_posts",
            "/api/v1/douyin/app/v3/fetch_user_post_videos",
            {"sec_user_id": douyin_sec_user_id},
        ),
        (
            "xhs_video_detail",
            "/api/v1/xiaohongshu/app_v2/get_video_note_detail",
            {"note_id": args.xhs_video_note_id},
        ),
        (
            "xhs_image_detail",
            "/api/v1/xiaohongshu/app_v2/get_image_note_detail",
            {"note_id": args.xhs_image_note_id},
        ),
        (
            "xhs_comments_page_1",
            "/api/v1/xiaohongshu/app_v2/get_note_comments",
            {
                "note_id": args.xhs_video_note_id,
                "cursor": "",
                "index": 0,
                "pageArea": "UNFOLDED",
                "sort_strategy": "latest_v2",
            },
        ),
    ]
    comment_record: dict[str, Any] | None = None
    for name, endpoint, params in calls:
        record, summary = cached_call(
            name=name,
            endpoint=endpoint,
            params=params,
            cache_dir=args.cache_dir,
            key=key,
            refresh=args.refresh,
        )
        summaries.append(summary)
        if name == "xhs_comments_page_1":
            comment_record = record

    if args.xhs_comment_pages >= 2 and comment_record:
        page = nested_data(comment_record.get("response"))
        cursor_value = page.get("cursor")
        page_area = page.get("pageArea") or page.get("page_area") or "UNFOLDED"
        index = page.get("index")
        if isinstance(cursor_value, str):
            try:
                decoded_cursor = json.loads(cursor_value)
            except json.JSONDecodeError:
                decoded_cursor = None
            if isinstance(decoded_cursor, dict):
                index = decoded_cursor.get("index", index)
                page_area = decoded_cursor.get("pageArea", page_area)
                cursor_value = decoded_cursor.get("cursor", "")
        if isinstance(cursor_value, dict):
            index = cursor_value.get("index", index)
            cursor_value = cursor_value.get("cursor", "")
        if index is None:
            index = 0
        if cursor_value:
            _, summary = cached_call(
                name="xhs_comments_page_2",
                endpoint="/api/v1/xiaohongshu/app_v2/get_note_comments",
                params={
                    "note_id": args.xhs_video_note_id,
                    "cursor": cursor_value,
                    "index": index,
                    "pageArea": page_area,
                    "sort_strategy": "latest_v2",
                },
                cache_dir=args.cache_dir,
                key=key,
                refresh=args.refresh,
            )
            summaries.append(summary)
        else:
            summaries.append(
                {
                    "name": "xhs_comments_page_2",
                    "source": "skipped",
                    "http_status": None,
                    "api_code": None,
                    "message": "page 1 did not return a next-page cursor",
                    "cache_path": "",
                }
            )
    return summaries


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--douyin-uid", default="")
    parser.add_argument("--douyin-sec-user-id", default="")
    parser.add_argument("--xhs-video-note-id", required=True)
    parser.add_argument("--xhs-image-note-id", required=True)
    parser.add_argument("--xhs-comment-pages", type=int, choices=(1, 2), default=2)
    parser.add_argument("--key-file", type=Path, default=KEY_FILE)
    parser.add_argument(
        "--cache-dir",
        type=Path,
        default=ROOT / "raw_responses" / "tikhub_paid_probe",
    )
    parser.add_argument("--refresh", action="store_true")
    return parser.parse_args()


def main() -> int:
    summaries = run(parse_args())
    print(json.dumps(summaries, ensure_ascii=False, indent=2))
    failed = [item for item in summaries if item["source"] != "skipped" and item["http_status"] != 200]
    return 1 if failed else 0


if __name__ == "__main__":
    raise SystemExit(main())
