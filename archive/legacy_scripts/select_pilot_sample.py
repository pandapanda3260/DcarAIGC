#!/usr/bin/env python3
"""Create a reproducible balanced pilot sample from notes_unique.csv.

The evaluator-facing file intentionally excludes the source label and original
sample ID.  A separate answer-key file preserves those fields for later scoring.
"""

from __future__ import annotations

import argparse
import csv
import hashlib
import json
import random
from collections import Counter, defaultdict
from pathlib import Path


ROOT = Path(__file__).resolve().parent


def main() -> int:
    parser = argparse.ArgumentParser()
    parser.add_argument("--input", type=Path, default=ROOT / "notes_unique.csv")
    parser.add_argument("--per-label", type=int, default=5)
    parser.add_argument("--seed", type=int, default=20260719)
    parser.add_argument("--prefix", default="pilot_sample_10")
    args = parser.parse_args()
    if args.per_label < 1:
        parser.error("--per-label must be at least 1")

    with args.input.open(encoding="utf-8-sig", newline="") as handle:
        rows = list(csv.DictReader(handle))

    grouped = defaultdict(list)
    for row in rows:
        label = row.get("gold_label", "").strip()
        note_id = row.get("note_id", "").strip()
        url = row.get("canonical_url", "").strip()
        if label in {"auto", "non_auto"} and note_id and url:
            grouped[label].append(row)

    for label in ("auto", "non_auto"):
        if len(grouped[label]) < args.per_label:
            raise SystemExit(f"Not enough valid rows for {label}")

    rng = random.Random(args.seed)
    selected = []
    for label in ("auto", "non_auto"):
        selected.extend(rng.sample(grouped[label], args.per_label))
    rng.shuffle(selected)

    blind_path = ROOT / f"{args.prefix}_blind.csv"
    key_path = ROOT / f"{args.prefix}_labels.csv"
    metadata_path = ROOT / f"{args.prefix}_metadata.json"

    blind_rows = []
    key_rows = []
    for index, row in enumerate(selected, start=1):
        pilot_id = f"P{index:03d}"
        blind_rows.append(
            {
                "pilot_id": pilot_id,
                "note_id": row["note_id"],
                "url": row["canonical_url"],
            }
        )
        key_rows.append(
            {
                "pilot_id": pilot_id,
                "source_label": row["gold_label"],
                "source_sample_id": row["sample_id"],
                "note_id": row["note_id"],
                "source_file": row["source_file"],
                "source_line": row["source_line"],
            }
        )

    with blind_path.open("w", encoding="utf-8-sig", newline="") as handle:
        writer = csv.DictWriter(handle, fieldnames=list(blind_rows[0]))
        writer.writeheader()
        writer.writerows(blind_rows)

    with key_path.open("w", encoding="utf-8-sig", newline="") as handle:
        writer = csv.DictWriter(handle, fieldnames=list(key_rows[0]))
        writer.writeheader()
        writer.writerows(key_rows)

    source_hash = hashlib.sha256(args.input.read_bytes()).hexdigest()
    metadata = {
        "source_file": args.input.name,
        "source_sha256": source_hash,
        "seed": args.seed,
        "sampling_method": "random.sample within each label, then random.shuffle combined rows",
        "source_counts": dict(Counter(row["gold_label"] for row in rows)),
        "selected_counts": dict(Counter(row["gold_label"] for row in selected)),
        "outputs": [blind_path.name, key_path.name],
        "label_isolation": "blind file excludes source_label, source_sample_id, and source filename",
        "label_note": "source_label comes from the user-provided source group and must be manually confirmed before it is treated as a gold label",
    }
    metadata_path.write_text(json.dumps(metadata, ensure_ascii=False, indent=2) + "\n", encoding="utf-8")

    print(json.dumps(metadata, ensure_ascii=False, indent=2))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
