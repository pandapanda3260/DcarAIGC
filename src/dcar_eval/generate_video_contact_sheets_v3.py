#!/usr/bin/env python3
"""Generate six-frame contact sheets for low-text and audit videos."""

from __future__ import annotations

import argparse
import json
import subprocess
import tempfile
from pathlib import Path

from PIL import Image, ImageDraw, ImageFont

from project_paths import DOUYIN_MEDIA_CACHE_DIR, DOUYIN_PROCESSED_DIR


ANALYSIS = DOUYIN_MEDIA_CACHE_DIR
MEDIA = ANALYSIS / "media"
OUT = ANALYSIS / "contact_sheets"
LABELS = DOUYIN_PROCESSED_DIR / "douyin_selling_point_labels_v3_video_2026-08-01.jsonl"
FONT_PATH = "/System/Library/Fonts/Hiragino Sans GB.ttc"


def read_jsonl(path: Path) -> list[dict]:
    with path.open("r", encoding="utf-8-sig") as handle:
        return [json.loads(line) for line in handle if line.strip()]


def probe_duration(path: Path) -> float:
    result = subprocess.run(
        ["ffprobe", "-v", "error", "-show_entries", "format=duration", "-of", "default=nw=1:nk=1", str(path)],
        capture_output=True,
        text=True,
        timeout=30,
    )
    try:
        return max(0.0, float(result.stdout.strip()))
    except ValueError:
        return 0.0


def frame(media: Path, timestamp: float, target: Path) -> bool:
    result = subprocess.run(
        [
            "ffmpeg", "-y", "-nostdin", "-loglevel", "error", "-ss", str(timestamp), "-i", str(media),
            "-frames:v", "1", "-vf", "scale=360:-1", "-q:v", "3", str(target),
        ],
        capture_output=True,
        timeout=60,
    )
    return result.returncode == 0 and target.exists()


def truncate(value: str, n: int = 48) -> str:
    value = " ".join(value.split())
    return value if len(value) <= n else value[: n - 1] + "…"


def make_sheet(row: dict) -> Path | None:
    aweme_id = str(row["aweme_id"])
    target = OUT / f"{aweme_id}.jpg"
    if target.exists():
        return target
    media = MEDIA / f"{aweme_id}.mp4"
    if not media.exists():
        return None
    duration = probe_duration(media)
    times = [max(0.1, min(duration - 0.1, duration * ratio)) for ratio in (0.03, 0.2, 0.38, 0.58, 0.78, 0.96)]
    with tempfile.TemporaryDirectory(prefix=f"dcar-contact-{aweme_id}-") as temp:
        paths = []
        for index, timestamp in enumerate(times):
            path = Path(temp) / f"{index}.jpg"
            if frame(media, timestamp, path):
                paths.append((path, timestamp))
        if not paths:
            return None
        opened = [Image.open(path).convert("RGB") for path, _ in paths]
        cell_w = 360
        cell_h = max(image.height for image in opened)
        header_h = 118
        sheet = Image.new("RGB", (cell_w * 3, header_h + cell_h * 2), "#111827")
        draw = ImageDraw.Draw(sheet)
        font_main = ImageFont.truetype(FONT_PATH, 26)
        font_sub = ImageFont.truetype(FONT_PATH, 20)
        draw.text((20, 14), f"{aweme_id} · {row.get('account_name','')} · {row.get('evidence_level','')}", font=font_main, fill="white")
        draw.text((20, 56), truncate(str(row.get("desc") or "")), font=font_sub, fill="#D1D5DB")
        draw.text((20, 86), "关键帧用于判断：真实车型/功能、概念娱乐、故事，或其他用户任务", font=font_sub, fill="#93C5FD")
        for index, ((_, timestamp), image) in enumerate(zip(paths, opened)):
            x = (index % 3) * cell_w
            y = header_h + (index // 3) * cell_h
            sheet.paste(image, (x + (cell_w - image.width) // 2, y))
            draw.rectangle((x + 8, y + 8, x + 96, y + 39), fill="#111827")
            draw.text((x + 14, y + 10), f"{timestamp:.1f}s", font=font_sub, fill="white")
        sheet.save(target, quality=90)
    return target


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("--mode", choices=("incomplete", "all"), default="incomplete")
    args = parser.parse_args()
    OUT.mkdir(parents=True, exist_ok=True)
    rows = read_jsonl(LABELS)
    if args.mode == "incomplete":
        rows = [row for row in rows if row.get("evidence_level") in {"V0", "V1"}]
    made = 0
    for index, row in enumerate(rows, 1):
        if make_sheet(row):
            made += 1
        if index % 20 == 0 or index == len(rows):
            print(f"CONTACT_PROGRESS {index}/{len(rows)} made={made}", flush=True)
    print(json.dumps({"selected": len(rows), "made": made, "output": str(OUT)}, ensure_ascii=False))


if __name__ == "__main__":
    main()
