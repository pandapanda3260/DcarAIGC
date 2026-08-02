#!/usr/bin/env python3
"""Run the final v3 taxonomy with caption/description evidence only.

This is a screening baseline, not a formal final judgment.  Scores remain
capped at V1=74 and a 60-point threshold is used only to create candidates for
the title-vs-video method comparison.
"""

from __future__ import annotations

import json
from pathlib import Path

from label_douyin_video_evidence_v3 import match_points


ROOT = Path(__file__).resolve().parent
SOURCE = ROOT / "douyin_30_account_content_sample_2026-08-01.jsonl"
OUTPUT = ROOT / "douyin_selling_point_labels_v3_caption_only_screening_2026-08-01.jsonl"


def main() -> None:
    rows = [json.loads(line) for line in SOURCE.read_text(encoding="utf-8-sig").splitlines() if line.strip()]
    results = []
    for row in rows:
        matches = match_points(row, {}, {}, "V1")
        primary = matches[0] if matches and int(matches[0]["score"]) >= 60 else None
        results.append(
            {
                "aweme_id": str(row["aweme_id"]),
                "account_name": row.get("account_name", ""),
                "desc": row.get("desc", ""),
                "screening_id": primary["id"] if primary else "",
                "screening_score": int(primary["score"]) if primary else 0,
                "screening_reason": primary["reason"] if primary else "仅标题/正文未形成卖点候选",
                "evidence_level": "V1",
                "formal_included": False,
            }
        )
    OUTPUT.write_text("".join(json.dumps(item, ensure_ascii=False) + "\n" for item in results), encoding="utf-8")
    print(json.dumps({"output": str(OUTPUT), "rows": len(results)}, ensure_ascii=False))


if __name__ == "__main__":
    main()
