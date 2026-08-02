#!/usr/bin/env python3
"""Rebuild the aggregate OCR manifest from the final per-work files."""

from __future__ import annotations

import json

from project_paths import DOUYIN_INPUT_DIR, DOUYIN_MEDIA_CACHE_DIR


SOURCE = DOUYIN_INPUT_DIR / "douyin_30_account_content_sample_2026-08-01.jsonl"
OCR_DIR = DOUYIN_MEDIA_CACHE_DIR / "ocr"
OUTPUT = DOUYIN_MEDIA_CACHE_DIR / "ocr_results.jsonl"


def main() -> None:
    rows = [json.loads(line) for line in SOURCE.read_text(encoding="utf-8-sig").splitlines() if line.strip()]
    merged = []
    for row in rows:
        aweme_id = str(row["aweme_id"])
        path = OCR_DIR / f"{aweme_id}.json"
        result = json.loads(path.read_text(encoding="utf-8")) if path.exists() else {
            "aweme_id": aweme_id,
            "status": "missing",
            "combined_text": "",
            "ocr_observation_count": 0,
        }
        result["aweme_id"] = aweme_id
        result["content_type"] = row.get("content_type", "")
        result["media_type"] = row.get("media_type", "")
        merged.append(result)
    OUTPUT.write_text("".join(json.dumps(item, ensure_ascii=False) + "\n" for item in merged), encoding="utf-8")
    print(json.dumps({"output": str(OUTPUT), "rows": len(merged)}, ensure_ascii=False))


if __name__ == "__main__":
    main()
