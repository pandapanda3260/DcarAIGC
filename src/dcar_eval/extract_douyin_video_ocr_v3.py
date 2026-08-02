#!/usr/bin/env python3
"""Extract key-frame and subtitle OCR with macOS Vision, with resume support."""

from __future__ import annotations

import argparse
import concurrent.futures
import json
import re
import subprocess
import tempfile
import time
from pathlib import Path
from typing import Any

from project_paths import DOUYIN_INPUT_DIR, DOUYIN_MEDIA_CACHE_DIR, RUNTIME_BIN_DIR


SOURCE = DOUYIN_INPUT_DIR / "douyin_30_account_content_sample_2026-08-01.jsonl"
ANALYSIS = DOUYIN_MEDIA_CACHE_DIR
MEDIA_DIR = ANALYSIS / "media"
TRANSCRIPT_DIR = ANALYSIS / "transcripts"
OCR_DIR = ANALYSIS / "ocr"
OCR_RESULTS = ANALYSIS / "ocr_results.jsonl"
OCR_BIN = RUNTIME_BIN_DIR / "vision_ocr"


def read_jsonl(path: Path) -> list[dict[str, Any]]:
    with path.open("r", encoding="utf-8-sig") as handle:
        return [json.loads(line) for line in handle if line.strip()]


def probe_duration(path: Path) -> float:
    try:
        result = subprocess.run(
            ["ffprobe", "-v", "error", "-show_entries", "format=duration", "-of", "default=nw=1:nk=1", str(path)],
            check=True,
            capture_output=True,
            text=True,
            timeout=30,
        )
        return max(0.0, float(result.stdout.strip()))
    except Exception:
        return 0.0


def normalize(text: str) -> str:
    return re.sub(r"\W+", "", text.lower())


def transcribed_text(aweme_id: str) -> str:
    path = TRANSCRIPT_DIR / f"{aweme_id}.json"
    try:
        data = json.loads(path.read_text(encoding="utf-8"))
        return str(data.get("text") or "") if data.get("status") == "success" else ""
    except Exception:
        return ""


def frame_times(duration: float, asr_text: str) -> list[float]:
    if duration <= 0:
        return [0.0]
    chinese_chars = len(re.findall(r"[\u4e00-\u9fff]", asr_text))
    if chinese_chars < 15:
        step = 1.0 if duration <= 60 else 2.0
        values = [min(duration - 0.1, 0.5 + step * i) for i in range(min(60, int(duration / step) + 1))]
    else:
        values = [min(duration - 0.1, max(0.2, duration * ratio)) for ratio in (0.08, 0.3, 0.55, 0.8, 0.95)]
    return sorted(set(round(value, 2) for value in values if value >= 0))


def extract_frame(media: Path, timestamp: float, target: Path) -> bool:
    result = subprocess.run(
        [
            "ffmpeg", "-y", "-nostdin", "-loglevel", "error", "-ss", str(timestamp), "-i", str(media),
            "-frames:v", "1", "-vf", "scale=960:-1", "-q:v", "3", str(target),
        ],
        capture_output=True,
        text=True,
        timeout=60,
    )
    return result.returncode == 0 and target.exists() and target.stat().st_size > 1024


def ocr_frames(paths: list[Path]) -> list[dict[str, Any]]:
    if not paths:
        return []
    result = subprocess.run(
        [str(OCR_BIN), *[str(path) for path in paths]],
        check=False,
        capture_output=True,
        text=True,
        timeout=max(60, 8 * len(paths)),
    )
    output = []
    for line in result.stdout.splitlines():
        try:
            output.append(json.loads(line))
        except json.JSONDecodeError:
            continue
    return output


def process_one(row: dict[str, Any]) -> dict[str, Any]:
    aweme_id = str(row["aweme_id"])
    target = OCR_DIR / f"{aweme_id}.json"
    if target.exists():
        try:
            cached = json.loads(target.read_text(encoding="utf-8"))
            if cached.get("status") == "success":
                return cached
        except Exception:
            pass
    media = MEDIA_DIR / f"{aweme_id}.mp4"
    if not media.exists():
        return {"aweme_id": aweme_id, "status": "media_missing", "texts": []}
    duration = probe_duration(media)
    asr_text = transcribed_text(aweme_id)
    times = frame_times(duration, asr_text)
    started = time.time()
    with tempfile.TemporaryDirectory(prefix=f"dcar-ocr-{aweme_id}-") as temp_dir:
        paths = []
        path_to_time: dict[str, float] = {}
        for index, timestamp in enumerate(times):
            path = Path(temp_dir) / f"frame_{index:03d}.jpg"
            if extract_frame(media, timestamp, path):
                paths.append(path)
                path_to_time[str(path)] = timestamp
        raw = ocr_frames(paths)
        observations = []
        seen: set[str] = set()
        for item in raw:
            text = str(item.get("text") or "").strip()
            key = normalize(text)
            if not key or key in seen:
                continue
            seen.add(key)
            observations.append(
                {
                    "time_sec": path_to_time.get(str(item.get("path") or "")),
                    "text": text,
                    "confidence": round(float(item.get("confidence") or 0), 3),
                }
            )
    result = {
        "aweme_id": aweme_id,
        "status": "success",
        "duration_sec": round(duration, 3),
        "frame_count": len(times),
        "ocr_observation_count": len(observations),
        "texts": observations,
        "combined_text": "\n".join(item["text"] for item in observations),
        "elapsed_sec": round(time.time() - started, 2),
    }
    target.write_text(json.dumps(result, ensure_ascii=False, indent=2) + "\n", encoding="utf-8")
    return result


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("--workers", type=int, default=4)
    parser.add_argument("--only-transcribed", action="store_true", help="Process only videos whose ASR result already exists")
    args = parser.parse_args()
    OCR_DIR.mkdir(parents=True, exist_ok=True)
    rows = read_jsonl(SOURCE)
    if args.only_transcribed:
        rows = [row for row in rows if (TRANSCRIPT_DIR / f"{row['aweme_id']}.json").exists()]
    results: list[dict[str, Any]] = []
    started = time.time()
    with concurrent.futures.ThreadPoolExecutor(max_workers=args.workers) as pool:
        futures = [pool.submit(process_one, row) for row in rows]
        for index, future in enumerate(concurrent.futures.as_completed(futures), 1):
            results.append(future.result())
            if index % 20 == 0 or index == len(rows):
                ok = sum(item.get("status") == "success" for item in results)
                print(f"OCR_PROGRESS {index}/{len(rows)} ok={ok} failed={index-ok} elapsed={time.time()-started:.1f}s", flush=True)
    results.sort(key=lambda item: item["aweme_id"])
    with OCR_RESULTS.open("w", encoding="utf-8") as handle:
        for result in results:
            handle.write(json.dumps(result, ensure_ascii=False) + "\n")
    print(json.dumps({"total": len(results), "success": sum(item.get("status") == "success" for item in results)}, ensure_ascii=False))


if __name__ == "__main__":
    main()
