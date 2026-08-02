#!/usr/bin/env python3
"""Create a reproducible 10+10+10 account draw order and content sample plan."""

from __future__ import annotations

import argparse
import csv
import hashlib
import json
import random
from collections import Counter
from pathlib import Path


ROOT = Path(__file__).resolve().parent
DEFAULT_POOL = ROOT / "douyin_account_uid_pool_2026-08-01.tsv"
QUALITY_ORDER = ["精品IP号", "原创号", "混剪号"]


def load_pool(path: Path) -> list[dict[str, str]]:
    with path.open(encoding="utf-8", newline="") as handle:
        rows = list(csv.DictReader(handle, delimiter="\t"))
    seen: set[str] = set()
    output: list[dict[str, str]] = []
    for row in rows:
        uid = row["uid"].strip()
        quality = row["quality_label"].strip()
        if not uid.isdigit() or quality not in QUALITY_ORDER or uid in seen:
            continue
        seen.add(uid)
        output.append({
            "nickname": row["nickname"].strip(),
            "uid": uid,
            "quality_label": quality,
        })
    return output


def deterministic_content_target(seed: int, uid: str) -> int:
    digest = hashlib.sha256(f"{seed}:{uid}:content-count".encode()).digest()
    return 10 + int.from_bytes(digest[:2], "big") % 11


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("--pool", type=Path, default=DEFAULT_POOL)
    parser.add_argument("--seed", type=int, default=20260801)
    parser.add_argument("--per-type", type=int, default=10)
    parser.add_argument(
        "--output",
        type=Path,
        default=ROOT / "douyin_30_account_draw_order_2026-08-01.csv",
    )
    args = parser.parse_args()

    pool = load_pool(args.pool)
    counts = Counter(row["quality_label"] for row in pool)
    if any(counts[quality] < args.per_type for quality in QUALITY_ORDER):
        raise SystemExit(f"候选池不足：{dict(counts)}")

    rng = random.Random(args.seed)
    draw: list[dict[str, object]] = []
    for quality in QUALITY_ORDER:
        candidates = [dict(row) for row in pool if row["quality_label"] == quality]
        candidates.sort(key=lambda row: row["uid"])
        rng.shuffle(candidates)
        for position, row in enumerate(candidates, 1):
            draw.append({
                **row,
                "draw_position_within_type": position,
                "initial_selection": position <= args.per_type,
                "content_sample_target": deterministic_content_target(args.seed, row["uid"]),
                "seed": args.seed,
            })

    args.output.parent.mkdir(parents=True, exist_ok=True)
    fields = [
        "quality_label", "draw_position_within_type", "initial_selection",
        "nickname", "uid", "content_sample_target", "seed",
    ]
    with args.output.open("w", encoding="utf-8-sig", newline="") as handle:
        writer = csv.DictWriter(handle, fieldnames=fields)
        writer.writeheader()
        writer.writerows(draw)

    metadata = {
        "seed": args.seed,
        "method": "三类账号分别排序后使用同一固定种子洗牌；每类取首10个，失败时按同类抽签顺序递补。",
        "eligible_pool_counts": dict(counts),
        "per_type": args.per_type,
        "planned_accounts": args.per_type * len(QUALITY_ORDER),
        "content_target_range": [10, 20],
        "missing_uid_or_unknown_quality_excluded": True,
    }
    args.output.with_suffix(".metadata.json").write_text(
        json.dumps(metadata, ensure_ascii=False, indent=2) + "\n",
        encoding="utf-8",
    )
    print(json.dumps(metadata, ensure_ascii=False))
    for row in draw:
        if row["initial_selection"]:
            print(row["quality_label"], row["draw_position_within_type"], row["nickname"], row["uid"], row["content_sample_target"])


if __name__ == "__main__":
    main()
