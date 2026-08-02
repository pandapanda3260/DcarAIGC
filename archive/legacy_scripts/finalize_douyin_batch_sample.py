#!/usr/bin/env python3
"""Finalize eligible accounts and reproducibly sample 10-20 recent posts each."""

from __future__ import annotations

import argparse
import csv
import hashlib
import json
import random
from collections import Counter
from pathlib import Path
from typing import Any


ROOT = Path(__file__).resolve().parent
QUALITY_ORDER = ["精品IP号", "原创号", "混剪号"]


def read_jsonl(path: Path) -> list[dict[str, Any]]:
    return [json.loads(line) for line in path.read_text(encoding="utf-8").splitlines() if line.strip()]


def seeded_rng(seed: int, uid: str) -> random.Random:
    digest = hashlib.sha256(f"{seed}:{uid}:posts".encode()).digest()
    return random.Random(int.from_bytes(digest[:8], "big"))


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument(
        "--draw-order",
        type=Path,
        default=ROOT / "douyin_30_account_draw_order_2026-08-01.csv",
    )
    parser.add_argument(
        "--cache-dir",
        type=Path,
        default=ROOT / "douyin_cache" / "sample30_2026-08-01",
    )
    parser.add_argument("--seed", type=int, default=20260801)
    parser.add_argument("--planned-accounts", type=int, default=30)
    parser.add_argument("--base-per-type", type=int, default=10)
    parser.add_argument(
        "--account-output",
        type=Path,
        default=ROOT / "douyin_30_account_final_sample_2026-08-01.csv",
    )
    parser.add_argument(
        "--post-output",
        type=Path,
        default=ROOT / "douyin_30_account_content_sample_2026-08-01.jsonl",
    )
    args = parser.parse_args()

    with args.draw_order.open(encoding="utf-8-sig", newline="") as handle:
        draw = list(csv.DictReader(handle))

    eligible: dict[str, dict[str, Any]] = {}
    observed: list[dict[str, Any]] = []
    for row in draw:
        account_path = args.cache_dir / "accounts" / row["uid"] / "account.json"
        posts_path = args.cache_dir / "accounts" / row["uid"] / "posts.jsonl"
        if not account_path.exists() or not posts_path.exists():
            continue
        account = json.loads(account_path.read_text(encoding="utf-8"))
        post_count = len(read_jsonl(posts_path))
        enriched = {
            **row,
            "returned_account_name": account.get("account_name", ""),
            "available_recent_posts": post_count,
            "eligible": post_count >= 10,
        }
        observed.append(enriched)
        if post_count >= 10:
            eligible[row["uid"]] = enriched

    selected: list[dict[str, Any]] = []
    selected_uids: set[str] = set()
    by_quality: dict[str, list[dict[str, Any]]] = {}
    for quality in QUALITY_ORDER:
        candidates = [
            eligible[row["uid"]]
            for row in draw
            if row["quality_label"] == quality and row["uid"] in eligible
        ]
        candidates.sort(key=lambda row: int(row["draw_position_within_type"]))
        by_quality[quality] = candidates
        for row in candidates[: args.base_per_type]:
            selected.append(row)
            selected_uids.add(row["uid"])

    # 精品IP号的公开作品不足时，按原创号、混剪号交替递补，保持两类数量接近。
    fill_order = ["原创号", "混剪号"]
    fill_index = 0
    while len(selected) < args.planned_accounts:
        quality = fill_order[fill_index % len(fill_order)]
        fill_index += 1
        candidate = next(
            (row for row in by_quality[quality] if row["uid"] not in selected_uids),
            None,
        )
        if candidate is None:
            other = fill_order[fill_index % len(fill_order)]
            candidate = next(
                (row for row in by_quality[other] if row["uid"] not in selected_uids),
                None,
            )
        if candidate is None:
            raise SystemExit("满足每账号至少10条作品的候选账号不足30个")
        selected.append(candidate)
        selected_uids.add(candidate["uid"])

    selected.sort(key=lambda row: (QUALITY_ORDER.index(row["quality_label"]), int(row["draw_position_within_type"])))
    sampled_posts: list[dict[str, Any]] = []
    account_rows: list[dict[str, Any]] = []
    for sample_index, row in enumerate(selected, 1):
        posts_path = args.cache_dir / "accounts" / row["uid"] / "posts.jsonl"
        posts = read_jsonl(posts_path)
        target = min(int(row["content_sample_target"]), len(posts), 20)
        if target < 10:
            raise SystemExit(f"{row['uid']} 最终样本不足10条")
        rng = seeded_rng(args.seed, row["uid"])
        chosen = rng.sample(posts, target)
        chosen.sort(key=lambda item: item.get("create_time", 0), reverse=True)
        for item in chosen:
            sampled_posts.append({
                **item,
                "quality_label": row["quality_label"],
                "account_sample_index": sample_index,
                "content_sample_seed": args.seed,
            })
        account_rows.append({
            "account_sample_index": sample_index,
            "quality_label": row["quality_label"],
            "draw_position_within_type": row["draw_position_within_type"],
            "input_nickname": row["nickname"],
            "returned_account_name": row["returned_account_name"],
            "uid": row["uid"],
            "available_recent_posts": len(posts),
            "sampled_posts": target,
            "seed": args.seed,
        })

    args.account_output.parent.mkdir(parents=True, exist_ok=True)
    with args.account_output.open("w", encoding="utf-8-sig", newline="") as handle:
        writer = csv.DictWriter(handle, fieldnames=list(account_rows[0]))
        writer.writeheader()
        writer.writerows(account_rows)
    with args.post_output.open("w", encoding="utf-8") as handle:
        for item in sampled_posts:
            handle.write(json.dumps(item, ensure_ascii=False, separators=(",", ":")) + "\n")

    metadata = {
        "seed": args.seed,
        "account_selection_method": "三类分层随机；每类先取10个满足至少10条作品的账号，精品IP不足部分按原创/混剪交替递补。",
        "content_selection_method": "每账号在最近接口首页中按固定种子随机抽取10-20条；若公开作品少于目标但不少于10，则取全部可用作品。",
        "selected_accounts": len(account_rows),
        "account_type_counts": dict(Counter(row["quality_label"] for row in account_rows)),
        "sampled_posts": len(sampled_posts),
        "sampled_posts_by_type": dict(Counter(item["quality_label"] for item in sampled_posts)),
        "observed_ineligible_accounts": [
            {
                "quality_label": row["quality_label"],
                "nickname": row["nickname"],
                "uid": row["uid"],
                "available_recent_posts": row["available_recent_posts"],
            }
            for row in observed
            if not row["eligible"]
        ],
    }
    args.account_output.with_suffix(".metadata.json").write_text(
        json.dumps(metadata, ensure_ascii=False, indent=2) + "\n",
        encoding="utf-8",
    )
    print(json.dumps(metadata, ensure_ascii=False, indent=2))


if __name__ == "__main__":
    main()
