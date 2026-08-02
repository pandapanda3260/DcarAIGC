#!/usr/bin/env python3
"""Cache public note metadata and screen impossible replacement candidates.

If a note's platform total comment count is below 20, it cannot contain 20
valid independent commenters.  Such a note can therefore be skipped without a
paid Rnote call while remaining recorded in the ordered screening ledger.
"""

from __future__ import annotations

import argparse
import csv
import json
import os
from concurrent.futures import ThreadPoolExecutor, as_completed
from pathlib import Path
from typing import Any, Mapping

from collect_rnote_pilot import (
    CACHE_SCHEMA,
    CacheStore,
    atomic_write_text,
    read_csv,
    read_json,
    safe_int,
    utc_now,
    write_json,
    write_jsonl,
)
from collect_xhs_public import (
    clean_note_url,
    collect_https_urls,
    detail_object,
    fetch_html,
    first_string,
    normalize_images,
    normalize_interactions,
    normalize_tags,
    parse_initial_state,
    validate_input_url,
)


ROOT = Path(__file__).resolve().parent
MIN_VALID_COMMENTERS = 20


def normalize_public_content(
    row: Mapping[str, str], note: Mapping[str, Any], store: CacheStore
) -> dict[str, Any]:
    user = note.get("user") if isinstance(note.get("user"), dict) else {}
    raw_user_id = user.get("userId") or user.get("user_id") or user.get("id")
    author_hash = store.digest("user", str(raw_user_id)) if raw_user_id else None
    interactions = normalize_interactions(note.get("interactInfo"))
    return {
        "schema_version": CACHE_SCHEMA,
        "provider": "xiaohongshu_public_page",
        "collected_at": utc_now(),
        "sample_attempt_id": row["candidate_id"],
        "note_id": row["note_id"].lower(),
        "url": clean_note_url(row["url"]),
        "note_type": first_string(note, ("type", "noteType")),
        "title": first_string(note, ("title", "noteTitle")),
        "desc": first_string(note, ("desc", "description")),
        "tags": normalize_tags(note.get("tagList")),
        "images": normalize_images(note.get("imageList")),
        "video_urls": collect_https_urls(note.get("video")),
        "published_at": note.get("time") if isinstance(note.get("time"), int) else None,
        "interactions": {
            "likes": safe_int(interactions.get("likedCount")),
            "collects": safe_int(interactions.get("collectedCount")),
            "comments": safe_int(interactions.get("commentCount")),
            "shares": safe_int(interactions.get("shareCount")),
        },
        "author_hash": author_hash,
    }


def screen_one(
    row: Mapping[str, str], *, store: CacheStore, timeout: int, refresh: bool
) -> dict[str, Any]:
    note_dir = store.note_dir(row["note_id"])
    screen_path = note_dir / "public_screen.json"
    content_path = note_dir / "public_content.json"
    if screen_path.exists() and not refresh:
        cached = read_json(screen_path)
        # Successful public metadata is immutable enough for this pilot.  HTTP
        # and parse failures are retried because Xiaohongshu may temporarily
        # redirect or throttle a burst of public-page requests.
        if isinstance(cached, dict) and cached.get("status") == "success":
            return cached

    result: dict[str, Any] = {
        "schema_version": CACHE_SCHEMA,
        "screened_at": utc_now(),
        "candidate_id": row["candidate_id"],
        "note_id": row["note_id"].lower(),
        "url": clean_note_url(row["url"]),
        "status": "parse_error",
        "http_status": 0,
        "platform_comment_count": None,
        "can_reach_20_valid_users": None,
        "note_type": None,
        "reason": None,
    }
    try:
        validate_input_url(row["url"], row["note_id"])
        http_status, html, reason = fetch_html(row["url"], timeout)
        result["http_status"] = http_status
        if http_status != 200 or not html:
            result["status"] = "http_error"
            result["reason"] = reason or f"http_{http_status}"
        else:
            state = parse_initial_state(html)
            detail = detail_object(state, row["note_id"])
            note = detail.get("note") if isinstance(detail.get("note"), dict) else {}
            returned_id = first_string(note, ("noteId", "note_id", "id"))
            if returned_id.lower() != row["note_id"].lower():
                raise ValueError("detail_id_missing_or_mismatch")
            content = normalize_public_content(row, note, store)
            count = content["interactions"].get("comments")
            result.update(
                {
                    "status": "success",
                    "platform_comment_count": count,
                    "can_reach_20_valid_users": (
                        None if count is None else count >= MIN_VALID_COMMENTERS
                    ),
                    "note_type": content["note_type"],
                    "reason": None,
                }
            )
            write_json(content_path, content)
    except (KeyError, ValueError) as exc:
        result["status"] = "parse_error"
        result["reason"] = str(exc)[:300]
    write_json(screen_path, result)
    return result


def write_csv(path: Path, rows: list[dict[str, str]], fieldnames: list[str]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    temporary = path.with_name(path.name + ".tmp")
    with temporary.open("w", encoding="utf-8-sig", newline="") as handle:
        writer = csv.DictWriter(handle, fieldnames=fieldnames)
        writer.writeheader()
        writer.writerows(rows)
    os.chmod(temporary, 0o600)
    os.replace(temporary, path)


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument(
        "--input",
        type=Path,
        default=ROOT / "pilot_replacement_queue_full_v1_blind.csv",
    )
    parser.add_argument(
        "--labels",
        type=Path,
        default=ROOT / "pilot_replacement_queue_full_v1_key.csv",
    )
    parser.add_argument("--cache-dir", type=Path, default=ROOT / "rnote_cache")
    parser.add_argument("--stratum", choices=("auto", "non_auto"), default="auto")
    parser.add_argument("--workers", type=int, default=6)
    parser.add_argument("--timeout", type=int, default=30)
    parser.add_argument("--refresh", action="store_true")
    parser.add_argument("--start-position", type=int, default=1)
    parser.add_argument("--end-position", type=int)
    parser.add_argument(
        "--include-unknown",
        action="store_true",
        help="Include public-page failures in the paid queue; default is cost-safe exclusion",
    )
    parser.add_argument(
        "--always-include-through",
        type=int,
        default=20,
        help="Preserve already-attempted queue positions even if below the public bound",
    )
    return parser.parse_args()


def main() -> int:
    args = parse_args()
    if args.workers < 1 or args.timeout < 1 or args.always_include_through < 0:
        raise SystemExit("workers/timeout must be positive and include limit non-negative")
    blind = read_csv(args.input)
    key_rows = read_csv(args.labels)
    key_by_id = {row["candidate_id"]: row for row in key_rows}
    all_selected = [
        row
        for row in blind
        if key_by_id.get(row["candidate_id"], {}).get("source_stratum")
        == args.stratum
    ]
    all_selected.sort(key=lambda row: int(key_by_id[row["candidate_id"]]["queue_position"]))
    selected = [
        row
        for row in all_selected
        if int(key_by_id[row["candidate_id"]]["queue_position"])
        >= args.start_position
        and (
            args.end_position is None
            or int(key_by_id[row["candidate_id"]]["queue_position"])
            <= args.end_position
        )
    ]
    store = CacheStore(args.cache_dir)
    results_by_id: dict[str, dict[str, Any]] = {}
    with ThreadPoolExecutor(max_workers=args.workers) as executor:
        futures = {
            executor.submit(
                screen_one,
                row,
                store=store,
                timeout=args.timeout,
                refresh=args.refresh,
            ): row
            for row in selected
        }
        completed = 0
        for future in as_completed(futures):
            row = futures[future]
            try:
                results_by_id[row["candidate_id"]] = future.result()
            except Exception as exc:  # keep the remaining screening batch alive
                results_by_id[row["candidate_id"]] = {
                    "schema_version": CACHE_SCHEMA,
                    "screened_at": utc_now(),
                    "candidate_id": row["candidate_id"],
                    "note_id": row["note_id"].lower(),
                    "url": clean_note_url(row["url"]),
                    "status": "parse_error",
                    "http_status": 0,
                    "platform_comment_count": None,
                    "can_reach_20_valid_users": None,
                    "note_type": None,
                    "reason": type(exc).__name__,
                }
            completed += 1
            if completed % 20 == 0 or completed == len(selected):
                possible = sum(
                    result.get("can_reach_20_valid_users") is not False
                    for result in results_by_id.values()
                )
                print(
                    json.dumps(
                        {
                            "screened": completed,
                            "total": len(selected),
                            "possible_or_unknown": possible,
                        },
                        ensure_ascii=False,
                    ),
                    flush=True,
                )

    # Aggregate every position from its per-note cache, not only the current
    # retry range, so multiple bounded runs build one durable screening ledger.
    ordered_results: list[dict[str, Any]] = []
    for row in all_selected:
        cached = read_json(store.note_dir(row["note_id"]) / "public_screen.json")
        if isinstance(cached, dict):
            ordered_results.append(cached)
        else:
            ordered_results.append(
                {
                    "schema_version": CACHE_SCHEMA,
                    "screened_at": None,
                    "candidate_id": row["candidate_id"],
                    "note_id": row["note_id"].lower(),
                    "url": clean_note_url(row["url"]),
                    "status": "not_screened",
                    "http_status": 0,
                    "platform_comment_count": None,
                    "can_reach_20_valid_users": None,
                    "note_type": None,
                    "reason": None,
                }
            )
    screening_path = args.cache_dir / f"public_screening_{args.stratum}.jsonl"
    write_jsonl(screening_path, ordered_results)

    possible_ids: set[str] = set()
    result_by_id = {row["candidate_id"]: row for row in ordered_results}
    for row in all_selected:
        result = result_by_id[row["candidate_id"]]
        position = int(key_by_id[row["candidate_id"]]["queue_position"])
        if (
            position <= args.always_include_through
            or result.get("can_reach_20_valid_users") is True
            or (args.include_unknown and result.get("can_reach_20_valid_users") is None)
        ):
            possible_ids.add(row["candidate_id"])
    filtered_blind = [row for row in blind if row["candidate_id"] in possible_ids]
    filtered_key = [row for row in key_rows if row["candidate_id"] in possible_ids]
    prefix = args.cache_dir / f"rnote_possible_{args.stratum}_queue"
    write_csv(prefix.with_name(prefix.name + "_blind.csv"), filtered_blind, list(blind[0]))
    write_csv(prefix.with_name(prefix.name + "_key.csv"), filtered_key, list(key_rows[0]))

    success = [row for row in ordered_results if row["status"] == "success"]
    summary = {
        "schema_version": CACHE_SCHEMA,
        "generated_at": utc_now(),
        "source_queue": args.input.name,
        "source_stratum": args.stratum,
        "queue_size": len(ordered_results),
        "screened_this_run": len(selected),
        "screened_success_total": len(success),
        "screen_errors_or_pending_total": len(ordered_results) - len(success),
        "platform_count_below_20": sum(
            row.get("can_reach_20_valid_users") is False for row in ordered_results
        ),
        "platform_count_at_least_20": sum(
            row.get("can_reach_20_valid_users") is True for row in ordered_results
        ),
        "platform_count_unknown": sum(
            row.get("can_reach_20_valid_users") is None for row in ordered_results
        ),
        "paid_queue_candidates_including_first_20_cached": len(filtered_blind),
        "unknowns_included_in_paid_queue": bool(args.include_unknown),
        "logical_rule": (
            "platform total comments <20 is a mathematical upper bound, so the note "
            "cannot meet the 20 valid independent commenter gate"
        ),
        "outputs": [
            screening_path.name,
            prefix.with_name(prefix.name + "_blind.csv").name,
            prefix.with_name(prefix.name + "_key.csv").name,
        ],
    }
    write_json(args.cache_dir / f"public_screening_{args.stratum}_summary.json", summary)
    print(json.dumps(summary, ensure_ascii=False, indent=2))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
