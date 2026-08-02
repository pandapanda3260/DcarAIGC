#!/usr/bin/env python3
"""Download bounded pilot media and extract three frames per video."""

from __future__ import annotations

import argparse
import csv
import json
import subprocess
from pathlib import Path
from typing import Any, Dict, List
from urllib.parse import urlsplit


ROOT = Path(__file__).resolve().parent
ALLOWED_SUFFIXES = (".xhscdn.com", ".rednotecdn.com", ".xiaohongshu.com")


def allowed_url(url: str) -> bool:
    parts = urlsplit(url)
    host = (parts.hostname or "").lower()
    return parts.scheme == "https" and any(host.endswith(suffix) for suffix in ALLOWED_SUFFIXES)


def curl_download(url: str, output: Path, max_bytes: int, timeout: int) -> str:
    if not allowed_url(url):
        return "blocked_domain"
    if output.exists() and output.stat().st_size > 0:
        return "cached"
    output.parent.mkdir(parents=True, exist_ok=True)
    process = subprocess.run(
        [
            "curl", "-sS", "--fail", "--proto", "=https", "--max-redirs", "0",
            "--connect-timeout", "10", "--max-time", str(timeout),
            "--max-filesize", str(max_bytes), "-A", "Mozilla/5.0 Chrome/126 Safari/537.36",
            "--output", str(output), url,
        ],
        capture_output=True,
    )
    if process.returncode != 0:
        if output.exists():
            output.unlink()
        return f"curl_error_{process.returncode}"
    if not output.exists() or output.stat().st_size == 0:
        if output.exists():
            output.unlink()
        return "empty_file"
    return "success"


def ffprobe_duration(path: Path) -> float:
    process = subprocess.run(
        ["ffprobe", "-v", "error", "-show_entries", "format=duration", "-of", "default=nw=1:nk=1", str(path)],
        capture_output=True,
        text=True,
    )
    if process.returncode != 0:
        return 0.0
    try:
        return max(0.0, float(process.stdout.strip()))
    except ValueError:
        return 0.0


def extract_frames(video: Path, output_dir: Path, duration: float) -> List[str]:
    if duration <= 0:
        return []
    output_dir.mkdir(parents=True, exist_ok=True)
    timestamps = [max(0.1, duration * ratio) for ratio in (0.1, 0.5, 0.9)]
    result: List[str] = []
    for index, timestamp in enumerate(timestamps, start=1):
        frame = output_dir / f"frame_{index:02d}.jpg"
        if not frame.exists() or frame.stat().st_size == 0:
            process = subprocess.run(
                [
                    "ffmpeg", "-loglevel", "error", "-y", "-ss", f"{timestamp:.3f}",
                    "-i", str(video), "-frames:v", "1", "-q:v", "2", str(frame),
                ],
                capture_output=True,
            )
            if process.returncode != 0:
                if frame.exists():
                    frame.unlink()
                continue
        result.append(str(frame.relative_to(ROOT)))
    return result


def master_video_url(entries: Any) -> str:
    if not isinstance(entries, list):
        return ""
    for entry in entries:
        if isinstance(entry, dict) and "masterurl" in str(entry.get("path", "")).lower():
            return str(entry.get("url", ""))
    for entry in entries:
        if isinstance(entry, dict) and entry.get("url"):
            return str(entry["url"])
    return ""


def main() -> int:
    parser = argparse.ArgumentParser()
    parser.add_argument("--input", type=Path, default=ROOT / "pilot_public_content.jsonl")
    parser.add_argument("--media-dir", type=Path, default=ROOT / "pilot_media")
    parser.add_argument("--manifest", type=Path, default=ROOT / "pilot_media_manifest.json")
    parser.add_argument("--results", type=Path, default=ROOT / "pilot_media_download_results.csv")
    parser.add_argument("--max-image-bytes", type=int, default=15 * 1024 * 1024)
    parser.add_argument("--max-video-bytes", type=int, default=150 * 1024 * 1024)
    args = parser.parse_args()
    args.input = args.input.resolve()
    args.media_dir = args.media_dir.resolve()
    args.manifest = args.manifest.resolve()
    args.results = args.results.resolve()

    rows = [json.loads(line) for line in args.input.read_text(encoding="utf-8").splitlines() if line.strip()]
    results: List[Dict[str, Any]] = []
    manifest: List[Dict[str, Any]] = []
    for row in rows:
        pilot_id = row.get("pilot_id") or row.get("sample_attempt_id")
        if not isinstance(pilot_id, str) or not pilot_id:
            raise ValueError("each media row requires pilot_id or sample_attempt_id")
        note_dir = args.media_dir / pilot_id
        image_paths: List[str] = []
        image_statuses: List[str] = []
        for index, image in enumerate(row.get("images", [])):
            # Provider payloads sometimes repeat index=0 for every image.  The
            # stable list position avoids silently treating later images as a
            # cache hit for the first file.
            path = note_dir / "images" / f"image_{index:02d}.webp"
            status = curl_download(str(image.get("url", "")), path, args.max_image_bytes, 45)
            image_statuses.append(status)
            if status in {"success", "cached"}:
                image_paths.append(str(path.relative_to(ROOT)))

        video_url = master_video_url(row.get("video_urls"))
        video_path = note_dir / "video.mp4"
        video_status = "not_applicable"
        duration = 0.0
        frames: List[str] = []
        if video_url:
            video_status = curl_download(video_url, video_path, args.max_video_bytes, 120)
            if video_status in {"success", "cached"}:
                duration = ffprobe_duration(video_path)
                frames = extract_frames(video_path, note_dir / "frames", duration)

        manifest.append(
            {
                "pilot_id": pilot_id,
                "note_id": row["note_id"],
                "image_paths": image_paths,
                "video_path": str(video_path.relative_to(ROOT)) if video_status in {"success", "cached"} else "",
                "video_duration_seconds": round(duration, 3),
                "frame_paths": frames,
            }
        )
        results.append(
            {
                "pilot_id": pilot_id,
                "expected_images": len(row.get("images", [])),
                "downloaded_images": len(image_paths),
                "image_failures": sum(status not in {"success", "cached"} for status in image_statuses),
                "video_status": video_status,
                "video_duration_seconds": round(duration, 3),
                "extracted_frames": len(frames),
            }
        )
        print(json.dumps(results[-1], ensure_ascii=False))

    manifest_path = args.manifest
    results_path = args.results
    manifest_path.parent.mkdir(parents=True, exist_ok=True)
    results_path.parent.mkdir(parents=True, exist_ok=True)
    manifest_path.write_text(json.dumps(manifest, ensure_ascii=False, indent=2) + "\n", encoding="utf-8")
    fields = ["pilot_id", "expected_images", "downloaded_images", "image_failures", "video_status", "video_duration_seconds", "extracted_frames"]
    with results_path.open("w", encoding="utf-8-sig", newline="") as handle:
        writer = csv.DictWriter(handle, fieldnames=fields)
        writer.writeheader()
        writer.writerows(results)
    print(json.dumps({"manifest": manifest_path.name, "results": results_path.name}, ensure_ascii=False))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
