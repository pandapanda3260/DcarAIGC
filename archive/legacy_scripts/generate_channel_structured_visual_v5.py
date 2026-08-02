#!/usr/bin/env python3
"""Generate one shareable structured conclusion image per channel."""

from __future__ import annotations

import json
from pathlib import Path

from PIL import Image, ImageDraw, ImageFont


ROOT = Path(__file__).resolve().parent
SOURCE = ROOT / "双渠道结构化结论_v5_2026-08-02.json"
OUTPUTS = {
    "douyin": ROOT / "抖音渠道结构化结论_v5_2026-08-02.png",
    "xiaohongshu": ROOT / "小红书渠道结构化结论_v5_2026-08-02.png",
}
FONT_CANDIDATES = (
    Path("/System/Library/Fonts/PingFang.ttc"),
    Path("/System/Library/Fonts/STHeiti Light.ttc"),
    Path("/Library/Fonts/Arial Unicode.ttf"),
)

W, H = 1800, 2050
BG = "#F4F7FB"
INK = "#172033"
MUTED = "#667085"
LINE = "#D9E0EA"
WHITE = "#FFFFFF"
BLUE = "#2F6BFF"
BLUE_LIGHT = "#EAF0FF"
GREEN = "#159A72"
GREEN_LIGHT = "#E7F6F1"
ORANGE = "#E99100"
ORANGE_LIGHT = "#FFF3D7"
RED = "#D94C4C"
RED_LIGHT = "#FDECEC"
PURPLE = "#7557E8"
PURPLE_LIGHT = "#F0EDFF"


def font(size: int, *, bold: bool = False) -> ImageFont.FreeTypeFont:
    path = next((candidate for candidate in FONT_CANDIDATES if candidate.exists()), None)
    if path is None:
        raise FileNotFoundError("No Chinese font found")
    try:
        return ImageFont.truetype(str(path), size=size, index=1 if bold else 0)
    except OSError:
        return ImageFont.truetype(str(path), size=size)


F_TITLE = font(52, bold=True)
F_SUB = font(23)
F_SECTION = font(31, bold=True)
F_KPI = font(42, bold=True)
F_LABEL = font(23, bold=True)
F_BODY = font(21)
F_SMALL = font(18)
F_SCENE = font(29, bold=True)


def rounded(draw: ImageDraw.ImageDraw, box: tuple[int, int, int, int], fill: str, radius: int = 22, outline: str | None = None) -> None:
    draw.rounded_rectangle(box, radius=radius, fill=fill, outline=outline, width=2 if outline else 1)


def txt(draw: ImageDraw.ImageDraw, xy: tuple[int, int], value: str, fnt: ImageFont.FreeTypeFont, fill: str = INK, anchor: str | None = None) -> None:
    draw.text(xy, value, font=fnt, fill=fill, anchor=anchor)


def wrap(draw: ImageDraw.ImageDraw, value: str, fnt: ImageFont.FreeTypeFont, max_width: int) -> list[str]:
    lines: list[str] = []
    current = ""
    for char in value:
        trial = current + char
        if current and draw.textlength(trial, font=fnt) > max_width:
            lines.append(current)
            current = char
        else:
            current = trial
    if current:
        lines.append(current)
    return lines


def paragraph(draw: ImageDraw.ImageDraw, x: int, y: int, value: str, width: int, fnt: ImageFont.FreeTypeFont = F_SMALL, fill: str = MUTED, max_lines: int = 3) -> None:
    lines = wrap(draw, value, fnt, width)[:max_lines]
    for line in lines:
        txt(draw, (x, y), line, fnt, fill)
        y += 29


def compact_result(value: str) -> str:
    result = value.split("；")[0]
    for old, new in (
        ("条有效播放", ""),
        ("条有效曝光", ""),
        ("条可评估", ""),
        ("条可计算", ""),
        ("数据覆盖", ""),
        ("样本覆盖", ""),
        ("样本达标", "达标"),
    ):
        result = result.replace(old, new)
    parts = [part.strip() for part in result.split("·")]
    if len(parts) >= 3 and "/100" in parts[0] and "/" in parts[1]:
        result = f"{parts[0]} · {parts[-1]}"
    return result


def top_card(draw: ImageDraw.ImageDraw, x: int, y: int, w: int, item: dict, label: str, accent: str) -> None:
    rounded(draw, (x, y, x + w, y + 154), WHITE, 22, LINE)
    draw.rounded_rectangle((x, y, x + 8, y + 154), radius=4, fill=accent)
    txt(draw, (x + 28, y + 20), item["display"].split("；")[0], F_KPI, accent)
    txt(draw, (x + 28, y + 82), label, F_LABEL)
    txt(draw, (x + 28, y + 119), item.get("qualitative") or item.get("scope") or "", F_SMALL, MUTED)


def summary_row(draw: ImageDraw.ImageDraw, x: int, y: int, w: int, label: str, item: dict, accent: str, light: str) -> None:
    rounded(draw, (x, y, x + w, y + 92), WHITE, 18, LINE)
    rounded(draw, (x + 16, y + 16, x + 252, y + 76), light, 14)
    txt(draw, (x + 32, y + 33), label, F_LABEL, accent)
    result = compact_result(item["display"])
    txt(draw, (x + 280, y + 17), result, F_BODY)
    note = item.get("qualitative") or item.get("reason") or ""
    note_lines = wrap(draw, note, F_SMALL, w - 310)
    txt(draw, (x + 280, y + 54), note_lines[0] if note_lines else "", F_SMALL, MUTED)


def scene_card(draw: ImageDraw.ImageDraw, x: int, y: int, w: int, h: int, scene: str, scene_data: dict, accent: str, light: str) -> None:
    rounded(draw, (x, y, x + w, y + h), WHITE, 24, LINE)
    rounded(draw, (x + 22, y + 22, x + w - 22, y + 78), light, 16)
    txt(draw, (x + 42, y + 34), scene, F_SCENE, accent)
    rows = (
        ("卖点条数", "selling_point_count_share"),
        ("核心条数", "core_selling_point_count_share"),
        ("卖点曝光", "selling_point_exposure_share"),
        ("核心曝光", "core_selling_point_exposure_share"),
        ("内容垂直度", "content_verticality"),
        ("互动垂直度", "audience_verticality"),
        ("拉新效果预估", "acquisition_effect_estimate"),
    )
    yy = y + 108
    for label, key in rows:
        item = scene_data[key]
        txt(draw, (x + 32, yy), label, F_SMALL, MUTED)
        result = compact_result(item["display"])
        txt(draw, (x + w - 32, yy), result, F_BODY, INK, anchor="ra")
        draw.line((x + 32, yy + 37, x + w - 32, yy + 37), fill=LINE, width=1)
        yy += 58
    boundary = scene_data.get("content_verticality", {}).get("scope") or ""
    paragraph(draw, x + 32, y + h - 88, boundary, w - 64, F_SMALL, MUTED, 2)


def render(channel_key: str, channel: dict) -> None:
    is_douyin = channel_key == "douyin"
    image = Image.new("RGB", (W, H), BG)
    draw = ImageDraw.Draw(image)
    title = "抖音渠道结构化结论" if is_douyin else "小红书渠道结构化结论"
    subtitle = channel["scope"]
    txt(draw, (76, 58), title, F_TITLE)
    txt(draw, (76, 130), subtitle, F_SUB, MUTED)
    badge = "全发布口径" if is_douyin else "全发布目标口径 · 当前含样本诊断"
    rounded(draw, (76, 178, 76 + int(draw.textlength(badge, font=F_SMALL)) + 42, 224), BLUE_LIGHT, 23)
    txt(draw, (97, 190), badge, F_SMALL, BLUE)

    txt(draw, (76, 276), "1、汇总", F_SECTION)
    s = channel["summary"]
    top_card(draw, 76, 332, 800, s["selling_point_count_share"], "卖点条数占比", BLUE)
    top_card(draw, 924, 332, 800, s["core_selling_point_count_share"], "核心卖点条数占比", ORANGE)
    summary_row(draw, 76, 520, 800, "卖点曝光占比", s["selling_point_exposure_share"], RED, RED_LIGHT)
    summary_row(draw, 924, 520, 800, "核心卖点曝光", s["core_selling_point_exposure_share"], RED, RED_LIGHT)
    summary_row(draw, 76, 632, 520, "内容垂直度", s["content_verticality"], GREEN, GREEN_LIGHT)
    summary_row(draw, 616, 632, 520, "互动用户垂直度", s["audience_verticality"], PURPLE, PURPLE_LIGHT)
    summary_row(draw, 1156, 632, 568, "内容拉新效果预估", s["acquisition_effect_estimate"], ORANGE, ORANGE_LIGHT)

    txt(draw, (76, 790), "2、三个业务场景", F_SECTION)
    colors = {
        "二手车": (ORANGE, ORANGE_LIGHT),
        "新车": (BLUE, BLUE_LIGHT),
        "媒体-AI小懂": (GREEN, GREEN_LIGHT),
    }
    card_w = 520
    for index, scene in enumerate(("二手车", "新车", "媒体-AI小懂")):
        accent, light = colors[scene]
        scene_card(draw, 76 + index * 564, 850, card_w, 590, scene, channel["scenes"][scene], accent, light)

    # Reading boundary and decision takeaway.
    rounded(draw, (76, 1488, 1724, 1658), ORANGE_LIGHT if not is_douyin else RED_LIGHT, 22)
    if is_douyin:
        paragraph(
            draw,
            106,
            1520,
            "曝光、互动用户垂直度和拉新效果预估没有被省略：当前分别缺少真实播放量、评论文本和统一模型所需的受众证据，因此明确显示为不可计算。",
            1584,
            F_BODY,
            RED,
            3,
        )
    else:
        paragraph(
            draw,
            106,
            1520,
            "三场景中的数值是10条全媒体诊断样本结果，不是338条渠道结论；二手车、新车没有场景样本，因此不以0%代替缺失结论。",
            1584,
            F_BODY,
            "#8A5B00",
            3,
        )

    rounded(draw, (76, 1710, 1724, 1950), INK, 26)
    txt(draw, (108, 1746), "核心判断", F_SECTION, WHITE)
    if is_douyin:
        paragraph(draw, 108, 1810, "卖点条数占比73.52%，核心卖点条数占比42.24%，低于60%-70%目标。核心内容几乎全部集中在新车场景：184条；二手车1条，媒体-AI小懂0条。", 1550, F_BODY, WHITE, 4)
    else:
        paragraph(draw, 108, 1810, "当前只够做样本诊断，不能做渠道卖点分布。媒体-AI小懂3条样本：内容垂直度93/100、互动用户垂直度50/100、拉新效果预估64/100。", 1550, F_BODY, WHITE, 4)

    image.save(OUTPUTS[channel_key], optimize=True)
    print(OUTPUTS[channel_key])


def main() -> None:
    data = json.loads(SOURCE.read_text(encoding="utf-8"))
    for channel_key in ("douyin", "xiaohongshu"):
        render(channel_key, data["channels"][channel_key])


if __name__ == "__main__":
    main()
