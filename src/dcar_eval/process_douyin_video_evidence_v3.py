#!/usr/bin/env python3
"""Download and transcribe the 438 sampled Douyin videos with resume support."""

from __future__ import annotations

import argparse
import concurrent.futures
import json
import os
import statistics
import subprocess
import sys
import time
from pathlib import Path
from typing import Any

from project_paths import DOUYIN_INPUT_DIR, DOUYIN_MEDIA_CACHE_DIR, DOUYIN_PUBLIC_CACHE_DIR, VIDEO_DEPENDENCY_DIR


SOURCE = DOUYIN_INPUT_DIR / "douyin_30_account_content_sample_2026-08-01.jsonl"
RAW_ROOTS = [
    DOUYIN_PUBLIC_CACHE_DIR / "sample30_2026-08-01" / "accounts",
    DOUYIN_PUBLIC_CACHE_DIR / "video_refresh_v3" / "accounts",
]
OUT_ROOT = DOUYIN_MEDIA_CACHE_DIR
MEDIA_DIR = OUT_ROOT / "media"
TRANSCRIPT_DIR = OUT_ROOT / "transcripts"
DOWNLOAD_RESULTS = OUT_ROOT / "download_results.jsonl"
TRANSCRIBE_RESULTS = OUT_ROOT / "transcription_results.jsonl"
MODEL = "mlx-community/whisper-large-v3-turbo"
PROMPT = "以下是懂车帝中文汽车内容，可能包含懂车帝、AI小懂、车型、配置、车价、优惠、二手车、测评、车主口碑、保养、维修等词。"


def read_jsonl(path: Path) -> list[dict[str, Any]]:
    with path.open("r", encoding="utf-8-sig") as handle:
        return [json.loads(line) for line in handle if line.strip()]


def find_aweme_nodes(value: Any, output: dict[str, dict[str, Any]]) -> None:
    if isinstance(value, dict):
        aweme_id = str(value.get("aweme_id") or "")
        if aweme_id and "video" in value:
            output[aweme_id] = value
        for child in value.values():
            find_aweme_nodes(child, output)
    elif isinstance(value, list):
        for child in value:
            find_aweme_nodes(child, output)


def raw_aweme_map() -> dict[str, dict[str, Any]]:
    output: dict[str, dict[str, Any]] = {}
    for root in RAW_ROOTS:
        for path in root.glob("*/posts_page_001_raw.json"):
            try:
                find_aweme_nodes(json.loads(path.read_text(encoding="utf-8")), output)
            except (OSError, json.JSONDecodeError):
                continue
    return output


def url_candidates(row: dict[str, Any], raw_node: dict[str, Any] | None) -> list[str]:
    values: list[str] = []
    video = (raw_node or {}).get("video") or {}
    addresses = [video.get("play_addr") or {}]
    addresses.extend((item.get("play_addr") or {}) for item in (video.get("bit_rate") or []))
    for address in addresses:
        values.extend(str(url) for url in (address.get("url_list") or []) if url)
    if row.get("video_url"):
        values.append(str(row["video_url"]))
    unique = list(dict.fromkeys(values))
    return sorted(
        unique,
        key=lambda url: (
            0 if "www.douyin.com/aweme/v1/play" in url else 1 if "v3-dy-o.zjcdn.com" in url else 2,
            len(url),
        ),
    )


def probe(path: Path) -> dict[str, Any] | None:
    try:
        result = subprocess.run(
            [
                "ffprobe", "-v", "error", "-show_entries", "format=duration,size",
                "-of", "json", str(path),
            ],
            check=True,
            capture_output=True,
            text=True,
            timeout=30,
        )
        data = json.loads(result.stdout).get("format") or {}
        duration = float(data.get("duration") or 0)
        size = int(data.get("size") or path.stat().st_size)
        if size > 1024:
            return {"duration_sec": round(duration, 3), "size_bytes": size}
    except Exception:
        return None
    return None


def download_one(row: dict[str, Any], raw_node: dict[str, Any] | None) -> dict[str, Any]:
    aweme_id = str(row["aweme_id"])
    target = MEDIA_DIR / f"{aweme_id}.mp4"
    existing = probe(target) if target.exists() else None
    if existing:
        return {"aweme_id": aweme_id, "status": "cached", "media_path": str(target), **existing}

    errors: list[str] = []
    for index, url in enumerate(url_candidates(row, raw_node), 1):
        temp = MEDIA_DIR / f".{aweme_id}.part.mp4"
        temp.unlink(missing_ok=True)
        try:
            completed = subprocess.run(
                ["ffmpeg", "-y", "-nostdin", "-loglevel", "error", "-i", url, "-c", "copy", str(temp)],
                capture_output=True,
                text=True,
                timeout=180,
            )
            if completed.returncode == 0 and temp.exists():
                info = probe(temp)
                if info:
                    temp.replace(target)
                    return {
                        "aweme_id": aweme_id,
                        "status": "downloaded",
                        "media_path": str(target),
                        "url_candidate_index": index,
                        **info,
                    }
            errors.append((completed.stderr or "ffmpeg failed")[-300:])
        except subprocess.TimeoutExpired:
            errors.append("timeout")
        finally:
            temp.unlink(missing_ok=True)
    return {"aweme_id": aweme_id, "status": "failed", "errors": errors[-3:]}


def write_jsonl(path: Path, rows: list[dict[str, Any]]) -> None:
    with path.open("w", encoding="utf-8") as handle:
        for row in rows:
            handle.write(json.dumps(row, ensure_ascii=False) + "\n")


def download_all(workers: int) -> None:
    MEDIA_DIR.mkdir(parents=True, exist_ok=True)
    rows = read_jsonl(SOURCE)
    raw = raw_aweme_map()
    results: list[dict[str, Any]] = []
    started = time.time()
    with concurrent.futures.ThreadPoolExecutor(max_workers=workers) as pool:
        futures = {pool.submit(download_one, row, raw.get(str(row["aweme_id"]))): row for row in rows}
        for index, future in enumerate(concurrent.futures.as_completed(futures), 1):
            result = future.result()
            results.append(result)
            if index % 20 == 0 or index == len(rows):
                ok = sum(r["status"] != "failed" for r in results)
                print(f"DOWNLOAD_PROGRESS {index}/{len(rows)} ok={ok} failed={index-ok} elapsed={time.time()-started:.1f}s", flush=True)
    results.sort(key=lambda item: item["aweme_id"])
    write_jsonl(DOWNLOAD_RESULTS, results)
    print(json.dumps({"total": len(results), "success": sum(r["status"] != "failed" for r in results), "failed": sum(r["status"] == "failed" for r in results)}, ensure_ascii=False), flush=True)


def compact_segments(segments: list[dict[str, Any]]) -> list[dict[str, Any]]:
    output = []
    for segment in segments:
        output.append(
            {
                "start": round(float(segment.get("start") or 0), 2),
                "end": round(float(segment.get("end") or 0), 2),
                "text": str(segment.get("text") or "").strip(),
                "avg_logprob": round(float(segment.get("avg_logprob") or 0), 4),
                "no_speech_prob": round(float(segment.get("no_speech_prob") or 0), 4),
            }
        )
    return output


def transcribe_all(shard_index: int, shard_count: int) -> None:
    sys.path.insert(0, str(VIDEO_DEPENDENCY_DIR))
    import mlx_whisper  # type: ignore

    TRANSCRIPT_DIR.mkdir(parents=True, exist_ok=True)
    source = read_jsonl(SOURCE)
    selected = [row for index, row in enumerate(source) if index % shard_count == shard_index]
    results: list[dict[str, Any]] = []
    started = time.time()
    for index, row in enumerate(selected, 1):
        aweme_id = str(row["aweme_id"])
        media = MEDIA_DIR / f"{aweme_id}.mp4"
        target = TRANSCRIPT_DIR / f"{aweme_id}.json"
        if target.exists():
            try:
                cached = json.loads(target.read_text(encoding="utf-8"))
                if cached.get("status") == "success":
                    results.append(cached)
                    continue
            except (OSError, json.JSONDecodeError):
                pass
        if not media.exists():
            result = {"aweme_id": aweme_id, "status": "media_missing", "shard_index": shard_index}
            target.write_text(json.dumps(result, ensure_ascii=False, indent=2) + "\n", encoding="utf-8")
            results.append(result)
            continue
        item_started = time.time()
        try:
            raw = mlx_whisper.transcribe(
                str(media),
                path_or_hf_repo=MODEL,
                language="zh",
                verbose=None,
                word_timestamps=False,
                initial_prompt=PROMPT,
                condition_on_previous_text=True,
            )
            segments = compact_segments(raw.get("segments") or [])
            result = {
                "aweme_id": aweme_id,
                "status": "success",
                "model": MODEL,
                "language": raw.get("language") or "zh",
                "text": str(raw.get("text") or "").strip(),
                "segments": segments,
                "segment_count": len(segments),
                "avg_logprob": round(statistics.mean(s["avg_logprob"] for s in segments), 4) if segments else None,
                "max_no_speech_prob": max((s["no_speech_prob"] for s in segments), default=1.0),
                "elapsed_sec": round(time.time() - item_started, 2),
                "shard_index": shard_index,
            }
        except Exception as exc:
            result = {
                "aweme_id": aweme_id,
                "status": "failed",
                "error": f"{type(exc).__name__}: {exc}",
                "elapsed_sec": round(time.time() - item_started, 2),
                "shard_index": shard_index,
            }
        target.write_text(json.dumps(result, ensure_ascii=False, indent=2) + "\n", encoding="utf-8")
        results.append(result)
        if index % 10 == 0 or index == len(selected):
            ok = sum(r["status"] == "success" for r in results)
            print(
                f"TRANSCRIBE_PROGRESS shard={shard_index} {index}/{len(selected)} ok={ok} "
                f"failed={index-ok} elapsed={time.time()-started:.1f}s",
                flush=True,
            )
    write_jsonl(OUT_ROOT / f"transcription_results_shard_{shard_index:02d}.jsonl", results)


def merge_transcripts() -> None:
    rows = []
    for path in sorted(TRANSCRIPT_DIR.glob("*.json")):
        try:
            rows.append(json.loads(path.read_text(encoding="utf-8")))
        except (OSError, json.JSONDecodeError):
            continue
    rows.sort(key=lambda item: item["aweme_id"])
    write_jsonl(TRANSCRIBE_RESULTS, rows)
    print(json.dumps({"total": len(rows), "success": sum(r.get("status") == "success" for r in rows), "failed": sum(r.get("status") != "success" for r in rows)}, ensure_ascii=False))


def main() -> None:
    parser = argparse.ArgumentParser()
    sub = parser.add_subparsers(dest="command", required=True)
    download = sub.add_parser("download")
    download.add_argument("--workers", type=int, default=8)
    transcribe = sub.add_parser("transcribe")
    transcribe.add_argument("--shard-index", type=int, default=0)
    transcribe.add_argument("--shard-count", type=int, default=1)
    sub.add_parser("merge")
    args = parser.parse_args()
    if args.command == "download":
        download_all(args.workers)
    elif args.command == "transcribe":
        transcribe_all(args.shard_index, args.shard_count)
    else:
        merge_transcripts()


if __name__ == "__main__":
    main()
