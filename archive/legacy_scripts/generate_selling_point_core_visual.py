#!/usr/bin/env python3
"""Generate a mobile-friendly share image for the 30-account DCar report."""

from __future__ import annotations

import json
from pathlib import Path

from PIL import Image, ImageDraw, ImageFont


ROOT = Path(__file__).resolve().parent
SUMMARY = ROOT / "douyin_selling_point_summary_v2_2026-08-01.json"
OUT = ROOT / "抖音30账号业务卖点核心结论_2026-08-01.png"
FONT_REGULAR = "/System/Library/Fonts/Hiragino Sans GB.ttc"
FONT_MEDIUM = "/System/Library/Fonts/STHeiti Medium.ttc"

W, H = 1440, 2560
BG = "#F4F6F9"
CARD = "#FFFFFF"
NAVY = "#18324A"
BLUE = "#2F6BFF"
TEAL = "#17A589"
ORANGE = "#F59E0B"
RED = "#E35D6A"
GRAY = "#AAB3C2"
LIGHT = "#E9EDF3"
MUTED = "#687588"
INK = "#17212B"


def font(size: int, medium: bool = False) -> ImageFont.FreeTypeFont:
    return ImageFont.truetype(FONT_MEDIUM if medium else FONT_REGULAR, size=size)


def rounded(draw: ImageDraw.ImageDraw, box: tuple[int, int, int, int], radius: int = 28, fill: str = CARD) -> None:
    draw.rounded_rectangle(box, radius=radius, fill=fill)


def text_width(draw: ImageDraw.ImageDraw, value: str, fnt: ImageFont.FreeTypeFont) -> int:
    return int(draw.textbbox((0, 0), value, font=fnt)[2])


def wrap(draw: ImageDraw.ImageDraw, value: str, fnt: ImageFont.FreeTypeFont, max_width: int) -> list[str]:
    lines: list[str] = []
    current = ""
    for char in value:
        trial = current + char
        if current and text_width(draw, trial, fnt) > max_width:
            lines.append(current)
            current = char
        else:
            current = trial
    if current:
        lines.append(current)
    return lines


def paragraph(
    draw: ImageDraw.ImageDraw,
    xy: tuple[int, int],
    value: str,
    fnt: ImageFont.FreeTypeFont,
    max_width: int,
    fill: str = INK,
    gap: int = 10,
) -> int:
    x, y = xy
    line_height = fnt.size + gap
    for line in wrap(draw, value, fnt, max_width):
        draw.text((x, y), line, font=fnt, fill=fill)
        y += line_height
    return y


def section_title(draw: ImageDraw.ImageDraw, x: int, y: int, number: str, title: str) -> None:
    draw.rounded_rectangle((x, y + 2, x + 44, y + 46), radius=12, fill=NAVY)
    draw.text((x + 13, y + 7), number, font=font(24, True), fill="#FFFFFF")
    draw.text((x + 62, y), title, font=font(34, True), fill=INK)


def metric_card(
    draw: ImageDraw.ImageDraw,
    x: int,
    y: int,
    width: int,
    value: str,
    count: str,
    label: str,
    accent: str,
) -> None:
    rounded(draw, (x, y, x + width, y + 160), 24, "#F8FAFC")
    draw.rounded_rectangle((x, y, x + 10, y + 160), radius=5, fill=accent)
    draw.text((x + 28, y + 18), value, font=font(52, True), fill=INK)
    draw.text((x + 30, y + 83), count, font=font(23), fill=MUTED)
    draw.text((x + 30, y + 118), label, font=font(24, True), fill=INK)


def stacked_bar(
    draw: ImageDraw.ImageDraw,
    x: int,
    y: int,
    width: int,
    height: int,
    values: list[tuple[int, str]],
    total: int,
) -> None:
    cursor = x
    for index, (value, color) in enumerate(values):
        segment = width - (cursor - x) if index == len(values) - 1 else round(width * value / total)
        draw.rectangle((cursor, y, cursor + segment, y + height), fill=color)
        cursor += segment


def main() -> None:
    summary = json.loads(SUMMARY.read_text(encoding="utf-8"))
    pub = summary["publication_metrics"]
    dedup = summary["deduplicated_creative_metrics"]
    by_quality = summary["by_quality_label"]

    image = Image.new("RGB", (W, H), BG)
    draw = ImageDraw.Draw(image)

    # Header
    draw.rectangle((0, 0, W, 300), fill=NAVY)
    draw.rounded_rectangle((72, 54, 222, 98), radius=22, fill="#2D4964")
    draw.text((96, 61), "核心结论", font=font(22, True), fill="#FFFFFF")
    draw.text((72, 122), "懂车帝抖音内容卖点抽样", font=font(58, True), fill="#FFFFFF")
    draw.text((74, 205), "30个账号｜438条作品｜固定种子随机抽样", font=font(27), fill="#D5E2EC")
    draw.text((1110, 205), "2026.08.01", font=font(25), fill="#D5E2EC")

    # Core rule
    rounded(draw, (48, 328, 1392, 450), 28, CARD)
    draw.text((82, 355), "本次修正的核心口径", font=font(25, True), fill=BLUE)
    draw.text((82, 396), "卖点不是“讲了什么汽车内容”，而是“用户通过懂车帝能完成什么”。", font=font(31, True), fill=INK)

    # Overall metrics
    rounded(draw, (48, 478, 1392, 890), 30, CARD)
    section_title(draw, 82, 510, "1", "总体结果：正式卖点依然极少")
    card_w = 292
    metric_card(draw, 82, 580, card_w, "4.6%", "20 / 438条", "正式卖点覆盖", BLUE)
    metric_card(draw, 398, 580, card_w, "2.7%", "12 / 438条", "核心卖点/全部", NAVY)
    metric_card(draw, 714, 580, card_w, "29.7%", "130 / 438条", "候选卖点可解释", TEAL)
    metric_card(draw, 1030, 580, card_w, "65.8%", "288 / 438条", "扩展后仍未命中", GRAY)
    stacked_bar(draw, 82, 778, 1240, 42, [(20, BLUE), (130, TEAL), (288, GRAY)], 438)
    legend = [("正式卖点 20", BLUE), ("候选卖点 130", TEAL), ("仍未命中 288", GRAY)]
    lx = 82
    for label, color in legend:
        draw.ellipse((lx, 844, lx + 18, 862), fill=color)
        draw.text((lx + 28, 836), label, font=font(23), fill=MUTED)
        lx += 310

    # Candidate selling points
    rounded(draw, (48, 918, 1392, 1320), 30, CARD)
    section_title(draw, 82, 950, "2", "从无标签内容中提炼出4类候选卖点")
    candidate_rows = [
        ("C1  实用汽车知识和用车解答", 102, "23.3%"),
        ("C2  车型细节、真实影像和场景体验", 21, "4.8%"),
        ("C3  车友社区交流和玩车内容", 6, "1.4%"),
        ("C4  新车发布和汽车行业动态", 1, "0.2%"),
    ]
    y = 1020
    max_count = 102
    for label, count, share in candidate_rows:
        draw.text((82, y), label, font=font(24, True), fill=INK)
        draw.rounded_rectangle((580, y + 3, 1220, y + 33), radius=15, fill=LIGHT)
        bar_w = max(8, round(640 * count / max_count))
        draw.rounded_rectangle((580, y + 3, 580 + bar_w, y + 33), radius=15, fill=TEAL)
        draw.text((1240, y - 2), f"{count}条｜{share}", font=font(23, True), fill=INK)
        y += 68
    draw.text((82, 1280), "候选标签不计入正式核心占比；转正前必须确认真实产品入口和用户任务。", font=font(22), fill=MUTED)

    # Account type comparison
    rounded(draw, (48, 1348, 1392, 1738), 30, CARD)
    section_title(draw, 82, 1380, "3", "三类账号：原创号最适合沉淀候选知识卖点")
    quality_rows = [("精品IP号", 8), ("原创号", 11), ("混剪号", 11)]
    y = 1460
    for quality, accounts in quality_rows:
        item = by_quality[quality]
        total = item["total"]
        official = item["official_included"]
        candidate = item["candidate_only"]
        unmatched = item["expanded_unmatched"]
        draw.text((82, y), f"{quality} · {accounts}个账号", font=font(25, True), fill=INK)
        stacked_bar(draw, 350, y + 3, 770, 34, [(official, BLUE), (candidate, TEAL), (unmatched, GRAY)], total)
        draw.text((1150, y - 2), f"正式 {official}｜候选 {candidate}", font=font(22), fill=MUTED)
        y += 76
    draw.text((82, 1690), "正式卖点占比：精品IP 8.8%｜原创 3.2%｜混剪 3.0%", font=font(23, True), fill=INK)

    # Unmatched reasons
    rounded(draw, (48, 1766, 1392, 2108), 30, CARD)
    section_title(draw, 82, 1798, "4", "为什么仍有65.8%无法打上卖点标签")
    reasons = [
        ("重复铺量或信息不足", 86),
        ("其他未形成平台任务", 77),
        ("只有懂车帝品牌露出", 60),
        ("汽车娱乐 / AI幻想", 34),
        ("情绪或生活故事", 31),
    ]
    y = 1870
    for label, count in reasons:
        draw.text((82, y), label, font=font(22), fill=INK)
        draw.rounded_rectangle((390, y + 2, 1130, y + 28), radius=13, fill=LIGHT)
        draw.rounded_rectangle((390, y + 2, 390 + round(740 * count / 86), y + 28), radius=13, fill=ORANGE)
        draw.text((1160, y - 4), f"{count}条", font=font(22, True), fill=INK)
        y += 48

    # Final implications
    rounded(draw, (48, 2136, 1392, 2472), 30, NAVY)
    draw.text((82, 2170), "结论与下一步", font=font(36, True), fill="#FFFFFF")
    takeaways = [
        "正式核心卖点仅占全部内容2.7%；“已命中内容内部核心占60%”不能替代这个指标。",
        f"去重后正式覆盖率仍只有{dedup['official_coverage_pct']:.1f}%，说明低覆盖不是单纯由混剪重复造成。",
        f"{pub['official_pending_review']}条弱匹配需要视频ASR/OCR复核，多数是只有“降价”但没有具体价格或来源。",
        "AI小懂三项核心卖点M1/M2/M3均为0；后续脚本必须前置“用户任务—平台能力—用户收益”。",
    ]
    y = 2230
    for index, takeaway in enumerate(takeaways, 1):
        draw.ellipse((84, y + 8, 108, y + 32), fill=TEAL if index < 4 else ORANGE)
        draw.text((90, y + 6), str(index), font=font(16, True), fill="#FFFFFF")
        paragraph(draw, (126, y), takeaway, font(24), 1190, fill="#EDF4F8", gap=8)
        y += 58

    draw.text((72, 2506), "证据范围：作品文案、话题与账号上下文；尚未对438条视频做全量语音转写和画面OCR。", font=font(20), fill=MUTED)
    draw.text((1192, 2506), "DCar", font=font(22, True), fill=NAVY)

    image.save(OUT, quality=95)
    print(OUT)


if __name__ == "__main__":
    main()
