#!/usr/bin/env python3
"""Render the final ten-note three-proposition comparison as SVG/PNG/HTML."""

from __future__ import annotations

import argparse
import json
import subprocess
from pathlib import Path
from typing import Any
from xml.sax.saxutils import escape

from project_paths import RNOTE_CACHE_DIR, XHS_PROCESSED_DIR


WIDTH = 1800
HEIGHT = 1130


def read_jsonl(path: Path) -> list[dict[str, Any]]:
    return [
        json.loads(line)
        for line in path.read_text(encoding="utf-8").splitlines()
        if line.strip()
    ]


def _text(
    x: float,
    y: float,
    value: Any,
    *,
    size: int = 24,
    weight: int = 400,
    fill: str = "#111827",
    anchor: str = "start",
    opacity: float = 1.0,
) -> str:
    return (
        f'<text x="{x}" y="{y}" font-size="{size}" font-weight="{weight}" '
        f'fill="{fill}" text-anchor="{anchor}" opacity="{opacity}">'
        f"{escape(str(value))}</text>"
    )


def render_visual_svg(rows: list[dict[str, Any]], *, theme_aware: bool = False) -> str:
    if len(rows) != 10:
        raise ValueError(f"visual requires exactly 10 final rows, got {len(rows)}")

    background = "var(--background)" if theme_aware else "#FFFFFF"
    foreground = "var(--foreground)" if theme_aware else "#111827"
    muted = "var(--muted-foreground)" if theme_aware else "#667085"
    grid = "var(--border)" if theme_aware else "#E5E7EB"
    track = "var(--muted)" if theme_aware else "#EEF1F5"
    group_fill = "var(--muted)" if theme_aware else "#F7F8FA"
    colors = (
        ["var(--viz-series-1)", "var(--viz-series-2)", "var(--viz-series-3)"]
        if theme_aware
        else ["#3478F6", "#16A085", "#F59E0B"]
    )
    panels = [
        {
            "x": 400,
            "title": "命题1：是否为汽车内容",
            "key": "content_auto_score",
            "threshold": 70,
            "threshold_label": "≥70：汽车内容达标",
            "color": colors[0],
        },
        {
            "x": 850,
            "title": "命题2：互动用户是否偏汽车",
            "key": "audience_auto_score",
            "threshold": 60,
            "threshold_label": "≥60：多数用户有汽车兴趣",
            "color": colors[1],
        },
        {
            "x": 1300,
            "title": "命题3：懂车帝拉新潜力",
            "key": "dcd_acquisition_score",
            "threshold": 65,
            "threshold_label": "≥65：值得进入拉新实验",
            "color": colors[2],
        },
    ]
    plot_width = 370
    row_start = 320
    row_gap = 64
    row_end = row_start + (len(rows) - 1) * row_gap

    content_hits = sum(int(row["content_auto_score"]) >= 70 for row in rows)
    audience_hits = sum(int(row["audience_auto_score"]) >= 60 for row in rows)
    acquisition_hits = sum(int(row["dcd_acquisition_score"]) >= 65 for row in rows)
    automotive_ids = [
        row["sample_attempt_id"]
        for row in rows
        if int(row["content_auto_score"]) >= 70
    ]
    acquisition_ids = [
        row["sample_attempt_id"]
        for row in rows
        if int(row["dcd_acquisition_score"]) >= 65
    ]

    parts = [
        f'<svg xmlns="http://www.w3.org/2000/svg" width="{WIDTH}" height="{HEIGHT}" '
        f'viewBox="0 0 {WIDTH} {HEIGHT}" role="img" '
        'aria-labelledby="visual-title visual-desc">',
        '<title id="visual-title">最终10篇笔记三命题评分对比</title>',
        '<desc id="visual-desc">按笔记比较汽车内容、互动用户汽车倾向和懂车帝拉新潜力，满分100分。</desc>',
        f'<rect width="{WIDTH}" height="{HEIGHT}" fill="{background}"/>',
        '<g font-family="-apple-system, BlinkMacSystemFont, PingFang SC, Hiragino Sans GB, STHeiti, sans-serif">',
        _text(56, 72, "最终10篇笔记：三命题评分对比", size=38, weight=500, fill=foreground),
        _text(
            56,
            112,
            "统一采用0–100分横轴；虚线为各命题关键门槛；每篇均有不少于20名有效评论用户",
            size=22,
            fill=muted,
        ),
        _text(56, 170, f"汽车内容达标 {content_hits}/10", size=26, weight=500, fill=colors[0]),
        _text(400, 170, f"多数互动用户有汽车兴趣 {audience_hits}/10", size=26, weight=500, fill=colors[1]),
        _text(940, 170, f"值得进入拉新实验 {acquisition_hits}/10", size=26, weight=500, fill=colors[2]),
        f'<rect x="36" y="{row_start - 37}" width="1740" height="{row_gap * 5}" fill="{group_fill}" opacity="0.55"/>',
        _text(56, 246, "来源组", size=20, weight=500, fill=muted),
        _text(210, 246, "样本 · 有效评论用户", size=20, weight=500, fill=muted),
    ]

    for panel in panels:
        x = int(panel["x"])
        threshold_x = x + plot_width * int(panel["threshold"]) / 100
        parts.extend(
            [
                _text(x, 222, panel["title"], size=24, weight=500, fill=foreground),
                _text(x, 252, panel["threshold_label"], size=18, fill=muted),
                f'<line x1="{threshold_x:.1f}" y1="270" x2="{threshold_x:.1f}" y2="{row_end + 33}" '
                f'stroke="{panel["color"]}" stroke-width="2" stroke-dasharray="7 7" opacity="0.62"/>',
            ]
        )
        for tick in (0, 25, 50, 75, 100):
            tick_x = x + plot_width * tick / 100
            parts.append(
                f'<line x1="{tick_x:.1f}" y1="275" x2="{tick_x:.1f}" y2="{row_end + 22}" '
                f'stroke="{grid}" stroke-width="1"/>'
            )
            parts.append(
                _text(tick_x, row_end + 56, tick, size=17, fill=muted, anchor="middle")
            )

    for index, row in enumerate(rows):
        y = row_start + index * row_gap
        source = "汽车来源" if row["source_stratum"] == "auto" else "非汽车来源"
        parts.append(_text(56, y + 8, source, size=19, fill=muted))
        parts.append(
            _text(210, y + 2, row["sample_attempt_id"], size=24, weight=500, fill=foreground)
        )
        parts.append(
            _text(210, y + 29, f"{row['valid_unique_commenters']}人", size=17, fill=muted)
        )
        if index == 5:
            parts.append(
                f'<line x1="36" y1="{y - 34}" x2="1776" y2="{y - 34}" stroke="{grid}" stroke-width="2"/>'
            )
        for panel in panels:
            x = int(panel["x"])
            score = int(row[str(panel["key"])])
            bar_width = plot_width * score / 100
            endpoint = x + bar_width
            parts.append(
                f'<rect x="{x}" y="{y - 9}" width="{plot_width}" height="18" rx="9" fill="{track}"/>'
            )
            if score > 0:
                parts.append(
                    f'<rect x="{x}" y="{y - 9}" width="{bar_width:.1f}" height="18" rx="9" '
                    f'fill="{panel["color"]}" opacity="0.86"/>'
                )
            parts.append(
                f'<circle cx="{endpoint:.1f}" cy="{y}" r="7" fill="{panel["color"]}"/>'
            )
            if score >= 90:
                label_x = endpoint - 12
                anchor = "end"
            else:
                label_x = endpoint + 12
                anchor = "start"
            parts.append(
                _text(label_x, y - 15, score, size=19, weight=500, fill=foreground, anchor=anchor)
            )

    takeaway_y = 1022
    parts.extend(
        [
            f'<line x1="56" y1="968" x2="1744" y2="968" stroke="{grid}" stroke-width="2"/>',
            _text(56, takeaway_y, "图中结论", size=22, weight=500, fill=foreground),
            _text(
                190,
                takeaway_y,
                f"汽车内容达标：{'、'.join(automotive_ids)}；进入拉新实验：{'、'.join(acquisition_ids)}。",
                size=22,
                fill=foreground,
            ),
            _text(
                190,
                takeaway_y + 38,
                "RA049、RA054虽来自汽车链接文件，但内容主体和互动人群均未达到汽车门槛。",
                size=21,
                fill=muted,
            ),
            _text(
                56,
                1102,
                "注：来源组仅表示输入文件，不参与评分；拉新潜力是预测分，不是实际新增效果。",
                size=18,
                fill=muted,
            ),
            "</g>",
            "</svg>",
        ]
    )
    return "\n".join(parts)


def render_html_fragment(rows: list[dict[str, Any]]) -> str:
    svg = render_visual_svg(rows, theme_aware=True)
    return "\n".join(
        [
            '<div id="three-proposition-summary">',
            "<style>",
            "#three-proposition-summary { width: 100%; overflow: hidden; }",
            "#three-proposition-summary svg { display: block; width: 100%; height: auto; }",
            "</style>",
            svg,
            "</div>",
        ]
    )


def write_visual(
    rows: list[dict[str, Any]],
    *,
    svg_path: Path,
    png_path: Path | None = None,
    html_path: Path | None = None,
) -> None:
    svg_path.parent.mkdir(parents=True, exist_ok=True)
    svg_path.write_text(render_visual_svg(rows), encoding="utf-8")
    if png_path is not None:
        png_path.parent.mkdir(parents=True, exist_ok=True)
        subprocess.run(
            ["/usr/bin/sips", "-s", "format", "png", str(svg_path), "--out", str(png_path)],
            check=True,
            capture_output=True,
            text=True,
        )
    if html_path is not None:
        html_path.parent.mkdir(parents=True, exist_ok=True)
        html_path.write_text(render_html_fragment(rows), encoding="utf-8")


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument(
        "--results-jsonl",
        type=Path,
        default=XHS_PROCESSED_DIR / "rnote_three_proposition_results.jsonl",
    )
    parser.add_argument(
        "--svg",
        type=Path,
        default=RNOTE_CACHE_DIR / "three_proposition_visual_summary.svg",
    )
    parser.add_argument(
        "--png",
        type=Path,
        default=RNOTE_CACHE_DIR / "three_proposition_visual_summary.png",
    )
    parser.add_argument("--html", type=Path)
    return parser.parse_args()


def main() -> int:
    args = parse_args()
    rows = read_jsonl(args.results_jsonl)
    write_visual(rows, svg_path=args.svg, png_path=args.png, html_path=args.html)
    print(
        json.dumps(
            {
                "rows": len(rows),
                "svg": str(args.svg),
                "png": str(args.png),
                "html": str(args.html) if args.html else None,
            },
            ensure_ascii=False,
        )
    )
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
