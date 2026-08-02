#!/usr/bin/env python3
"""Download and analyse all Rnote media with terminal local-cache reuse."""

from __future__ import annotations

import argparse
import json
import re
import statistics
import subprocess
import sys
import time
import urllib.error
import urllib.request
from concurrent.futures import ThreadPoolExecutor, as_completed
from pathlib import Path
from typing import Any

from project_paths import RNOTE_CACHE_DIR, VIDEO_DEPENDENCY_DIR


NOTES_ROOT = RNOTE_CACHE_DIR / "notes"
MEDIA_ROOT = RNOTE_CACHE_DIR / "media"
OCR_BIN = Path(__file__).resolve().parents[2] / "data/cache/douyin_media/bin/vision_ocr"
MODEL = "mlx-community/whisper-large-v3-turbo"
PROMPT = "汽车，懂车帝，AI小懂，二手车，新车，选车，买车，卖车，试驾，保养，维修，车型，价格，配置。"


def atomic_json(path: Path, value: Any) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    temporary = path.with_name(path.name + ".tmp")
    temporary.write_text(json.dumps(value, ensure_ascii=False, indent=2) + "\n", encoding="utf-8")
    temporary.replace(path)


def read_json(path: Path, default: Any = None) -> Any:
    if not path.exists():
        return default
    try:
        return json.loads(path.read_text(encoding="utf-8"))
    except (OSError, json.JSONDecodeError):
        return default


def valid_file(path: Path) -> bool:
    return path.exists() and path.stat().st_size > 1024


def valid_video(path: Path) -> bool:
    if not valid_file(path):
        return False
    result = subprocess.run(
        ["ffprobe", "-v", "error", "-select_streams", "v:0", "-show_entries", "stream=codec_type", "-of", "csv=p=0", str(path)],
        capture_output=True,
        text=True,
        timeout=20,
    )
    return result.returncode == 0 and "video" in result.stdout


def download(url: str, target: Path) -> bool:
    if valid_file(target):
        return True
    target.parent.mkdir(parents=True, exist_ok=True)
    temporary = target.with_name(target.name + ".part")
    temporary.unlink(missing_ok=True)
    request = urllib.request.Request(url, headers={"User-Agent": "Mozilla/5.0"})
    for attempt in range(2):
        try:
            with urllib.request.urlopen(request, timeout=60) as response, temporary.open("wb") as handle:
                while True:
                    chunk = response.read(1024 * 1024)
                    if not chunk:
                        break
                    handle.write(chunk)
            if valid_file(temporary):
                temporary.replace(target)
                return True
        except (urllib.error.URLError, TimeoutError, ConnectionError, OSError):
            temporary.unlink(missing_ok=True)
            if attempt == 0:
                time.sleep(0.5)
    return False


def download_video(urls: list[str], target: Path) -> bool:
    if valid_video(target):
        return True
    if target.exists():
        invalid = target.with_name(target.name + ".invalid")
        invalid.unlink(missing_ok=True)
        target.replace(invalid)
    for index, url in enumerate(urls):
        candidate = target.with_name(f".{target.name}.candidate-{index}")
        candidate.unlink(missing_ok=True)
        if download(url, candidate) and valid_video(candidate):
            candidate.replace(target)
            return True
        candidate.unlink(missing_ok=True)
    return False


def media_manifest(note_id: str) -> dict[str, Any]:
    content = read_json(NOTES_ROOT / note_id / "content.json", {}) or {}
    note_root = MEDIA_ROOT / note_id
    image_paths: list[str] = []
    failures: list[str] = []
    for index, item in enumerate(content.get("images") or []):
        url = str(item.get("url") or "") if isinstance(item, dict) else ""
        if not url:
            continue
        target = note_root / "images" / f"image_{index:03d}.webp"
        if download(url, target):
            image_paths.append(str(target))
        else:
            failures.append(f"image_{index:03d}")
    video_path = ""
    video_urls = [str(item.get("url") or "") for item in content.get("video_urls") or [] if isinstance(item, dict)]
    if video_urls:
        target = note_root / "video.mp4"
        if download_video([url for url in video_urls if url], target):
            video_path = str(target)
        if not video_path:
            failures.append("video")
    result = {
        "note_id": note_id,
        "note_type": str(content.get("note_type") or ""),
        "expected_images": len(content.get("images") or []),
        "image_paths": image_paths,
        "video_expected": bool(video_urls),
        "video_path": video_path,
        "status": "complete" if not failures and (image_paths or video_path) else "partial" if image_paths or video_path else "failed",
        "failures": failures,
    }
    atomic_json(note_root / "manifest.json", result)
    return result


def duration(path: Path) -> float:
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


def extract_video_frames(video: Path, target_dir: Path) -> list[Path]:
    seconds = duration(video)
    if seconds <= 0:
        return []
    count = min(24, max(4, round(seconds / 3)))
    timestamps = [(index + 0.5) * seconds / count for index in range(count)]
    target_dir.mkdir(parents=True, exist_ok=True)
    output: list[Path] = []
    for index, timestamp in enumerate(timestamps):
        target = target_dir / f"frame_{index:03d}.jpg"
        if not valid_file(target):
            subprocess.run(
                [
                    "ffmpeg", "-y", "-nostdin", "-loglevel", "error", "-ss", str(timestamp),
                    "-i", str(video), "-frames:v", "1", "-vf", "scale=960:-1", "-q:v", "3", str(target),
                ],
                capture_output=True,
                timeout=60,
            )
        if valid_file(target):
            output.append(target)
    return output


def run_ocr(paths: list[Path]) -> list[dict[str, Any]]:
    if not paths:
        return []
    result = subprocess.run(
        [str(OCR_BIN), *map(str, paths)],
        capture_output=True,
        text=True,
        timeout=max(90, len(paths) * 12),
    )
    output: list[dict[str, Any]] = []
    for line in result.stdout.splitlines():
        try:
            value = json.loads(line)
        except json.JSONDecodeError:
            continue
        if isinstance(value, dict):
            output.append(value)
    return output


def combined_ocr_text(observations: list[dict[str, Any]]) -> str:
    values: list[str] = []
    for observation in observations:
        for item in observation.get("observations") or observation.get("texts") or []:
            text = str(item.get("text") or "") if isinstance(item, dict) else str(item or "")
            text = " ".join(text.split())
            if text and text not in values:
                values.append(text)
    return "\n".join(values)


def reusable_ocr(cached: dict[str, Any], *, has_video: bool) -> bool:
    if cached.get("status") != "success":
        return False
    return not has_video or cached.get("source_kind") == "video_frames"


def analyse_ocr(note_id: str) -> dict[str, Any]:
    note_root = MEDIA_ROOT / note_id
    target = note_root / "ocr.json"
    cached = read_json(target, {}) or {}
    manifest = read_json(note_root / "manifest.json", {}) or {}
    images = [Path(path) for path in manifest.get("image_paths") or [] if valid_file(Path(path))]
    video = Path(str(manifest.get("video_path") or ""))
    has_video = valid_video(video)
    if reusable_ocr(cached, has_video=has_video):
        return cached
    frames = extract_video_frames(video, note_root / "frames") if has_video else []
    sources = frames if frames else images
    observations = run_ocr(sources)
    result = {
        "note_id": note_id,
        "status": "success" if sources and len(observations) == len(sources) else "partial" if observations else "failed",
        "source_kind": "video_frames" if frames else "all_original_images",
        "source_count": len(sources),
        "ocr_observation_count": len(observations),
        "combined_text": combined_ocr_text(observations),
        "observations": observations,
    }
    atomic_json(target, result)
    return result


def compact_segments(values: list[dict[str, Any]]) -> list[dict[str, Any]]:
    output = []
    for item in values:
        output.append({
            "start": round(float(item.get("start") or 0), 2),
            "end": round(float(item.get("end") or 0), 2),
            "text": str(item.get("text") or "").strip(),
            "avg_logprob": round(float(item.get("avg_logprob") or 0), 4),
            "no_speech_prob": round(float(item.get("no_speech_prob") or 0), 4),
        })
    return output


def transcribe(note_id: str) -> dict[str, Any]:
    target = MEDIA_ROOT / note_id / "transcript.json"
    cached = read_json(target, {}) or {}
    if cached.get("status") == "success":
        return cached
    manifest = read_json(MEDIA_ROOT / note_id / "manifest.json", {}) or {}
    video = Path(str(manifest.get("video_path") or ""))
    if not valid_video(video):
        result = {"note_id": note_id, "status": "not_video", "text": "", "segments": []}
        atomic_json(target, result)
        return result
    sys.path.insert(0, str(VIDEO_DEPENDENCY_DIR))
    import mlx_whisper  # type: ignore

    started = time.time()
    try:
        raw = mlx_whisper.transcribe(
            str(video), path_or_hf_repo=MODEL, language="zh", verbose=None,
            word_timestamps=False, initial_prompt=PROMPT, condition_on_previous_text=True,
        )
        segments = compact_segments(raw.get("segments") or [])
        result = {
            "note_id": note_id,
            "status": "success",
            "model": MODEL,
            "language": raw.get("language") or "zh",
            "text": str(raw.get("text") or "").strip(),
            "segments": segments,
            "segment_count": len(segments),
            "avg_logprob": round(statistics.mean(item["avg_logprob"] for item in segments), 4) if segments else None,
            "max_no_speech_prob": max((item["no_speech_prob"] for item in segments), default=1.0),
            "elapsed_sec": round(time.time() - started, 2),
        }
    except Exception as exc:
        result = {"note_id": note_id, "status": "failed", "error": f"{type(exc).__name__}: {exc}"[:500]}
    atomic_json(target, result)
    return result


def note_ids() -> list[str]:
    return sorted(path.name for path in NOTES_ROOT.iterdir() if (path / "content.json").exists())


def transcript_needed(note_id: str) -> bool:
    manifest = read_json(MEDIA_ROOT / note_id / "manifest.json", {}) or {}
    if not manifest.get("video_expected") or not valid_video(Path(str(manifest.get("video_path") or ""))):
        return False
    ocr = read_json(MEDIA_ROOT / note_id / "ocr.json", {}) or {}
    text = re.sub(r"\s+", "", str(ocr.get("combined_text") or ""))
    return not (ocr.get("status") == "success" and len(text) >= 15)


def run_parallel(function, values: list[str], workers: int, label: str) -> None:
    completed = 0
    ok = 0
    with ThreadPoolExecutor(max_workers=workers) as pool:
        futures = {pool.submit(function, value): value for value in values}
        for future in as_completed(futures):
            result = future.result()
            completed += 1
            ok += result.get("status") in {"complete", "success", "not_video"}
            if completed % 10 == 0 or completed == len(values):
                print(f"{label}_PROGRESS {completed}/{len(values)} ok={ok}", flush=True)


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("stage", choices=("download", "ocr", "transcribe", "all"))
    parser.add_argument("--workers", type=int, default=4)
    return parser.parse_args()


def main() -> int:
    args = parse_args()
    values = note_ids()
    if args.stage in {"download", "all"}:
        run_parallel(media_manifest, values, args.workers, "DOWNLOAD")
    if args.stage in {"ocr", "all"}:
        run_parallel(analyse_ocr, values, args.workers, "OCR")
    if args.stage in {"transcribe", "all"}:
        selected = [note_id for note_id in values if transcript_needed(note_id)]
        for index, note_id in enumerate(selected, 1):
            result = transcribe(note_id)
            if index % 5 == 0 or index == len(selected):
                print(f"TRANSCRIBE_PROGRESS {index}/{len(selected)} status={result.get('status')}", flush=True)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
