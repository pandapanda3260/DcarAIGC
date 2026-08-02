#!/usr/bin/env python3
"""Build a reproducible, label-isolated replacement queue for the pilot.

The blind queue contains no source label.  A separate key preserves the
provisional source stratum so replacements can fill the same pilot slot.
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
    parser.add_argument(
        "--current-key", type=Path, default=ROOT / "pilot_sample_10_labels.csv"
    )
    parser.add_argument("--seed", type=int, default=20260720)
    parser.add_argument("--per-label", type=int, default=20)
    parser.add_argument("--prefix", default="pilot_replacement_queue_v0.3")
    args = parser.parse_args()
    if args.per_label < 1:
        parser.error("--per-label must be at least 1")

    with args.input.open(encoding="utf-8-sig", newline="") as handle:
        source_rows = list(csv.DictReader(handle))
    with args.current_key.open(encoding="utf-8-sig", newline="") as handle:
        current_rows = list(csv.DictReader(handle))

    excluded_note_ids = {row["note_id"].strip() for row in current_rows}
    grouped = defaultdict(list)
    for row in source_rows:
        label = row.get("gold_label", "").strip()
        note_id = row.get("note_id", "").strip()
        url = row.get("canonical_url", "").strip()
        if label in {"auto", "non_auto"} and note_id and url and note_id not in excluded_note_ids:
            grouped[label].append(row)

    rng = random.Random(args.seed)
    blind_rows = []
    key_rows = []
    for label, prefix in (("auto", "RA"), ("non_auto", "RN")):
        rng.shuffle(grouped[label])
        selected = grouped[label][: args.per_label]
        for position, row in enumerate(selected, start=1):
            candidate_id = f"{prefix}{position:03d}"
            blind_rows.append(
                {
                    "candidate_id": candidate_id,
                    "note_id": row["note_id"],
                    "url": row["canonical_url"],
                }
            )
            key_rows.append(
                {
                    "candidate_id": candidate_id,
                    "source_stratum": label,
                    "queue_position": position,
                    "source_sample_id": row.get("sample_id", ""),
                    "note_id": row["note_id"],
                }
            )

    blind_path = ROOT / f"{args.prefix}_blind.csv"
    key_path = ROOT / f"{args.prefix}_key.csv"
    metadata_path = ROOT / f"{args.prefix}_metadata.json"
    with blind_path.open("w", encoding="utf-8-sig", newline="") as handle:
        writer = csv.DictWriter(handle, fieldnames=list(blind_rows[0]))
        writer.writeheader()
        writer.writerows(blind_rows)
    with key_path.open("w", encoding="utf-8-sig", newline="") as handle:
        writer = csv.DictWriter(handle, fieldnames=list(key_rows[0]))
        writer.writeheader()
        writer.writerows(key_rows)

    metadata = {
        "source_file": args.input.name,
        "source_sha256": hashlib.sha256(args.input.read_bytes()).hexdigest(),
        "excluded_current_notes": len(excluded_note_ids),
        "seed": args.seed,
        "sampling_method": "shuffle remaining notes within provisional source stratum",
        "requested_per_label": args.per_label,
        "available_after_exclusion": {key: len(value) for key, value in grouped.items()},
        "selected_counts": dict(Counter(row["source_stratum"] for row in key_rows)),
        "use_rule": (
            "consume one candidate at a time only after the comment channel works; "
            "keep every failed attempt in the sampling ledger"
        ),
        "label_isolation": "blind queue excludes source_stratum and source_sample_id",
        "source_label_note": "source_stratum is provisional and is never a scoring input",
        "outputs": [blind_path.name, key_path.name],
    }
    metadata_path.write_text(json.dumps(metadata, ensure_ascii=False, indent=2) + "\n", encoding="utf-8")
    print(json.dumps(metadata, ensure_ascii=False, indent=2))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
