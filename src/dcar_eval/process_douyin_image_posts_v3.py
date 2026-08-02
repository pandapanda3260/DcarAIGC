#!/usr/bin/env python3
"""Download, OCR, and montage Douyin image posts mistaken for audio-only videos."""

from __future__ import annotations

import json
import subprocess
import concurrent.futures
from pathlib import Path
from typing import Any

from PIL import Image, ImageDraw, ImageFont, ImageOps

from process_douyin_video_evidence_v3 import raw_aweme_map, read_jsonl, SOURCE
from project_paths import DOUYIN_MEDIA_CACHE_DIR, RUNTIME_BIN_DIR


ANALYSIS = DOUYIN_MEDIA_CACHE_DIR
IMAGE_ROOT = ANALYSIS / "image_posts"
OCR_DIR = ANALYSIS / "ocr"
CONTACT_DIR = ANALYSIS / "contact_sheets"
OCR_BIN = RUNTIME_BIN_DIR / "vision_ocr"
FONT_PATH = "/System/Library/Fonts/Hiragino Sans GB.ttc"


def image_urls(node: dict[str, Any]) -> list[list[str]]:
    output = []
    for image in node.get("images") or node.get("image_list") or []:
        urls = list(image.get("url_list") or []) + list(image.get("download_url_list") or [])
        urls = sorted(dict.fromkeys(str(url) for url in urls if url), key=lambda url: (0 if ".jpeg?" in url else 1, len(url)))
        if urls:
            output.append(urls)
    return output


def download(urls: list[str], target: Path) -> bool:
    if target.exists() and target.stat().st_size > 1024:
        return True
    temp = target.with_suffix(".part.jpg")
    temp.unlink(missing_ok=True)
    for url in urls:
        result = subprocess.run(
            ["curl", "-L", "--fail", "--silent", "--show-error", "--max-time", "40", "-A", "Mozilla/5.0", "-o", str(temp), url],
            capture_output=True,
            text=True,
        )
        if result.returncode == 0 and temp.exists() and temp.stat().st_size > 1024:
            temp.replace(target)
            return True
        temp.unlink(missing_ok=True)
    return False


def ocr(paths: list[Path]) -> list[dict[str, Any]]:
    if not paths:
        return []
    result = subprocess.run([str(OCR_BIN), *map(str, paths)], capture_output=True, text=True, timeout=max(60, len(paths) * 10))
    rows = []
    for line in result.stdout.splitlines():
        try:
            rows.append(json.loads(line))
        except json.JSONDecodeError:
            pass
    return rows


def contact_sheet(aweme_id: str, account: str, desc: str, paths: list[Path]) -> Path | None:
    if not paths:
        return None
    images = [Image.open(path).convert("RGB") for path in paths[:9]]
    cell_w, cell_h = 320, 420
    cols = 3
    rows = (len(images) + cols - 1) // cols
    header = 120
    canvas = Image.new("RGB", (cell_w * cols, header + cell_h * rows), "#111827")
    draw = ImageDraw.Draw(canvas)
    title_font = ImageFont.truetype(FONT_PATH, 25)
    sub_font = ImageFont.truetype(FONT_PATH, 19)
    draw.text((18, 12), f"{aweme_id} · {account} · 图文作品", font=title_font, fill="white")
    clean = " ".join(desc.split())
    draw.text((18, 53), clean[:54] + ("…" if len(clean) > 54 else ""), font=sub_font, fill="#D1D5DB")
    draw.text((18, 84), "按原作品顺序展示全部关键图片（最多9张）", font=sub_font, fill="#93C5FD")
    for index, image in enumerate(images):
        fitted = ImageOps.contain(image, (cell_w - 8, cell_h - 8))
        x = (index % cols) * cell_w + (cell_w - fitted.width) // 2
        y = header + (index // cols) * cell_h + (cell_h - fitted.height) // 2
        canvas.paste(fitted, (x, y))
        draw.rounded_rectangle((index % cols * cell_w + 8, header + index // cols * cell_h + 8, index % cols * cell_w + 48, header + index // cols * cell_h + 40), radius=8, fill="#111827")
        draw.text((index % cols * cell_w + 20, header + index // cols * cell_h + 9), str(index + 1), font=sub_font, fill="white")
    target = CONTACT_DIR / f"{aweme_id}.jpg"
    canvas.save(target, quality=90)
    return target


def main() -> None:
    IMAGE_ROOT.mkdir(parents=True, exist_ok=True)
    CONTACT_DIR.mkdir(parents=True, exist_ok=True)
    OCR_DIR.mkdir(parents=True, exist_ok=True)
    source = read_jsonl(SOURCE)
    raw = raw_aweme_map()
    results = []
    for row in source:
        aweme_id = str(row["aweme_id"])
        node = raw.get(aweme_id) or {}
        candidates = image_urls(node)
        if not candidates:
            continue
        post_dir = IMAGE_ROOT / aweme_id
        post_dir.mkdir(parents=True, exist_ok=True)
        indexed = [(index, urls, post_dir / f"{index:02d}.jpg") for index, urls in enumerate(candidates, 1)]
        with concurrent.futures.ThreadPoolExecutor(max_workers=min(8, len(indexed))) as pool:
            downloaded = list(pool.map(lambda item: (item[0], item[2], download(item[1], item[2])), indexed))
        paths = [path for _, path, ok in sorted(downloaded) if ok]
        observations = ocr(paths)
        combined = []
        seen = set()
        for item in observations:
            text = str(item.get("text") or "").strip()
            key = "".join(text.lower().split())
            if text and key not in seen:
                seen.add(key)
                combined.append(text)
        ocr_result = {
            "aweme_id": aweme_id,
            "status": "success" if paths else "download_failed",
            "media_type": "image_post",
            "image_count": len(paths),
            "frame_count": len(paths),
            "ocr_observation_count": len(combined),
            "texts": observations,
            "combined_text": "\n".join(combined),
        }
        (OCR_DIR / f"{aweme_id}.json").write_text(json.dumps(ocr_result, ensure_ascii=False, indent=2) + "\n", encoding="utf-8")
        sheet = contact_sheet(aweme_id, str(row.get("account_name") or ""), str(row.get("desc") or ""), paths)
        result = {"aweme_id": aweme_id, "images": len(paths), "ocr_observations": len(combined), "contact_sheet": str(sheet or "")}
        results.append(result)
        print(json.dumps(result, ensure_ascii=False), flush=True)
    (ANALYSIS / "image_post_results.jsonl").write_text("".join(json.dumps(item, ensure_ascii=False) + "\n" for item in results), encoding="utf-8")
    print(json.dumps({"image_posts": len(results), "downloaded_images": sum(item["images"] for item in results)}, ensure_ascii=False))


if __name__ == "__main__":
    main()
