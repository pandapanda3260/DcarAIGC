#!/usr/bin/env python3
"""Collect the frozen 338-note Xiaohongshu corpus with terminal-cache reuse."""

from __future__ import annotations

import argparse
import csv
import json
from pathlib import Path
from types import SimpleNamespace
from typing import Any

from collect_rnote_pilot import (
    CacheStore,
    CollectorError,
    FatalProviderError,
    RequestBudget,
    RnoteClient,
    load_key,
    process_one,
    utc_now,
    write_json,
    write_jsonl,
)
from project_paths import RNOTE_CACHE_DIR, XHS_INPUT_DIR


KEY_FILE = Path("/Users/mark/Documents/key/DcarKey/Rnote.env.local")
INPUT = XHS_INPUT_DIR / "notes_unique.csv"
SUMMARY = RNOTE_CACHE_DIR / "full_collection_summary.json"
RESULTS = RNOTE_CACHE_DIR / "full_collection_results.jsonl"


def read_input(path: Path = INPUT) -> list[dict[str, str]]:
    with path.open(encoding="utf-8-sig", newline="") as handle:
        source = list(csv.DictReader(handle))
    return [
        {
            "sample_attempt_id": str(row["sample_id"]),
            "sample_role": "full_corpus",
            "source_stratum": str(row["gold_label"]),
            "source_sample_id": str(row["sample_id"]),
            "target_slot": "",
            "note_id": str(row["note_id"]).lower(),
            # The token-bearing source URL is used only for transient public type
            # detection. process_one persists clean_note_url(row["url"]).
            "url": str(row.get("canonical_url") or f"https://www.xiaohongshu.com/explore/{str(row['note_id']).lower()}"),
        }
        for row in source
    ]


def preflight(cache_dir: Path = RNOTE_CACHE_DIR) -> dict[str, Any]:
    rows = read_input()
    content = 0
    comments = 0
    terminal_comments = 0
    for row in rows:
        note_dir = cache_dir / "notes" / row["note_id"]
        content += (note_dir / "content.json").exists()
        comments += (note_dir / "comments.jsonl").exists()
        collection = note_dir / "collection.json"
        if collection.exists():
            value = json.loads(collection.read_text(encoding="utf-8"))
            stop = (value.get("comments") or {}).get("stop_reason")
            terminal_comments += stop in {
                "target_valid_users", "end_of_comments", "confirmed_empty",
                "three_pages_without_new_users", "max_pages", "max_raw_comments",
            }
    return {
        "total": len(rows),
        "content_cached": content,
        "content_missing": len(rows) - content,
        "comments_file_cached": comments,
        "terminal_comments_cached": terminal_comments,
        "provider_calls": 0,
    }


def load_cached_public_types(cache_dir: Path) -> dict[str, str]:
    result: dict[str, str] = {}
    notes_root = cache_dir / "notes"
    if not notes_root.exists():
        return result
    for note_dir in notes_root.iterdir():
        if not note_dir.is_dir():
            continue
        for filename in ("public_content.json", "public_screen.json", "content.json"):
            path = note_dir / filename
            if not path.exists():
                continue
            try:
                value = json.loads(path.read_text(encoding="utf-8"))
            except (json.JSONDecodeError, OSError):
                continue
            note_type = str(value.get("note_type") or value.get("type") or "").lower()
            if note_type:
                result[note_dir.name.lower()] = note_type
                break
    return result


def collect_all(args: argparse.Namespace) -> tuple[list[dict[str, Any]], dict[str, Any]]:
    rows = read_input(args.input)
    store = CacheStore(args.cache_dir)
    client = RnoteClient(
        load_key(args.key_file),
        RequestBudget(None),
        timeout=args.timeout,
        delay=args.delay,
        retries=1,
    )
    options = SimpleNamespace(
        public_timeout=args.public_timeout,
        refresh=False,
        target_valid=20,
        max_pages=args.max_pages,
        max_raw=args.max_raw,
    )
    public_types = load_cached_public_types(args.cache_dir)
    results: list[dict[str, Any]] = []
    stopped_error = ""
    for index, row in enumerate(rows, 1):
        try:
            result = process_one(
                row,
                store=store,
                client=client,
                public_types=public_types,
                args=options,
            )
        except FatalProviderError as exc:
            stopped_error = f"{type(exc).__name__}: {exc}"[:800]
            print(json.dumps({"stopped": stopped_error, "progress": f"{index-1}/{len(rows)}"}, ensure_ascii=False), flush=True)
            break
        except CollectorError as exc:
            result = {
                **{key: value for key, value in row.items() if key != "url"},
                "url": f"https://www.xiaohongshu.com/explore/{row['note_id']}",
                "content_status": "failed",
                "comment_sample_status": "technical_missing",
                "error": f"{type(exc).__name__}: {exc}"[:800],
                "scorable": False,
            }
        results.append(result)
        if index % 10 == 0 or index == len(rows):
            print(json.dumps({
                "progress": f"{index}/{len(rows)}",
                "logical_attempts": client.budget.used,
                "billed_requests": sum(bool(item.get("billed")) for item in client.request_log),
            }, ensure_ascii=False), flush=True)
        write_jsonl(args.results, results)
    statuses: dict[str, int] = {}
    for row in results:
        key = str(row.get("comment_sample_status") or row.get("content_status") or "unknown")
        statuses[key] = statuses.get(key, 0) + 1
    summary = {
        "generated_at": utc_now(),
        "collector": "rnote-full-v1.0",
        "total": len(rows),
        "results": len(results),
        "status_counts": statuses,
        "request_attempts_this_run": client.budget.used,
        "billed_requests_this_run": sum(bool(item.get("billed")) for item in client.request_log),
        "max_total_attempts_per_request": 2,
        "terminal_cache_reuse": True,
        "privacy": "No API key, raw user ID, nickname, profile or raw comment ID is persisted.",
        "stopped_error": stopped_error,
    }
    write_json(args.summary, summary)
    return results, summary


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--input", type=Path, default=INPUT)
    parser.add_argument("--key-file", type=Path, default=KEY_FILE)
    parser.add_argument("--cache-dir", type=Path, default=RNOTE_CACHE_DIR)
    parser.add_argument("--results", type=Path, default=RESULTS)
    parser.add_argument("--summary", type=Path, default=SUMMARY)
    parser.add_argument("--timeout", type=float, default=25.0)
    parser.add_argument("--public-timeout", type=int, default=1)
    parser.add_argument("--delay", type=float, default=0.1)
    parser.add_argument("--max-pages", type=int, default=10)
    parser.add_argument("--max-raw", type=int, default=500)
    parser.add_argument("--preflight", action="store_true")
    return parser.parse_args()


def main() -> int:
    args = parse_args()
    if args.preflight:
        print(json.dumps(preflight(args.cache_dir), ensure_ascii=False, indent=2))
        return 0
    _, summary = collect_all(args)
    print(json.dumps(summary, ensure_ascii=False, indent=2))
    return 2 if summary.get("stopped_error") else 0


if __name__ == "__main__":
    raise SystemExit(main())
