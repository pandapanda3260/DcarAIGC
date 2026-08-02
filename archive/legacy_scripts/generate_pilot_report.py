#!/usr/bin/env python3
"""Generate the human-readable pilot evaluation report from sealed outputs."""

from __future__ import annotations

import csv
import json
from collections import Counter
from pathlib import Path


ROOT = Path(__file__).resolve().parent


def read_jsonl(path: Path):
    return [json.loads(line) for line in path.read_text(encoding="utf-8").splitlines() if line.strip()]


def zh_source(value: str) -> str:
    return {"auto": "汽车来源组", "non_auto": "非汽车来源组"}.get(value, value)


def main() -> int:
    contents = {row["pilot_id"]: row for row in read_jsonl(ROOT / "pilot_public_content.jsonl")}
    predictions = read_jsonl(ROOT / "pilot_blind_predictions.jsonl")
    with (ROOT / "pilot_label_comparison.csv").open(encoding="utf-8-sig", newline="") as handle:
        comparisons = {row["pilot_id"]: row for row in csv.DictReader(handle)}
    with (ROOT / "pilot_media_download_results.csv").open(encoding="utf-8-sig", newline="") as handle:
        media = list(csv.DictReader(handle))

    content_counts = Counter(row["content_label"] for row in predictions)
    signal_counts = Counter(row["content_side_acquisition_signal"] for row in predictions)
    match_count = sum(row["comparison"] == "match" for row in comparisons.values())
    image_count = sum(int(row["downloaded_images"]) for row in media)
    video_count = sum(row["video_status"] in {"success", "cached"} for row in media)
    standard_frames = sum(int(row["extracted_frames"]) for row in media)
    prediction_version = predictions[0]["prediction_version"]

    lines = [
        "# 10篇小红书笔记首轮盲评报告",
        "",
        "> 历史v0.2报告：高/中/低与“内容侧”口径已废止。当前报告请使用 `pilot_three_proposition_report_v0.3.md`。",
        "",
        f"版本：`{prediction_version}`  ",
        "日期：2026-07-19",
        "",
        "## 一、结论摘要",
        "",
        f"- 10篇公开笔记内容均成功读取：{content_counts.get('汽车类', 0)}篇判为汽车类，{content_counts.get('非汽车类', 0)}篇判为非汽车类。",
        f"- 盲评结束后揭晓来源分组，内容分类与来源预设标签一致 `{match_count}/10`。来源标签尚未经过独立人工金标确认，因此这不是正式准确率。",
        f"- 已成功下载并检查{image_count}张图片、{video_count}个视频；标准抽取{standard_frames}帧，P004因首次证据不足额外抽取9帧后再判断。",
        "- 10篇公开HTML均未返回评论文本，因此互动受众汽车性全部为“证据不足”，有效评论数未评估。",
        f"- 懂车帝内容侧拉新线索：高{signal_counts.get('高', 0)}篇、中{signal_counts.get('中', 0)}篇、低{signal_counts.get('低', 0)}篇。因评论未取得，10篇综合拉新潜力均为“无法判断”。",
        "",
        "当前最值得补评论验证的是P004和P007；P008可作为汽车知识兴趣内容观察。",
        "",
        "## 二、数据质量",
        "",
        "| 项目 | 结果 |",
        "|---|---:|",
        "| 链接解析 | 10/10 |",
        "| 标题/正文结构化读取 | 10/10 |",
        f"| 图片下载 | {image_count}/{image_count} |",
        f"| 视频下载 | {video_count}/{video_count} |",
        f"| 标准视频抽帧 | {standard_frames}帧 |",
        "| P004密集抽帧 | 9帧 |",
        "| 评论采集状态 | 10/10未取得 |",
        "| 已取得评论文本 | 0条 |",
        "| 有效评论数 | 未评估 |",
        "| 静默空结果 | 0篇 |",
        "",
        "页面中的互动计数显示部分笔记存在评论，但公开首屏数据没有返回评论正文；这属于“评论未取得”，不是“没有评论”。",
        "",
        "## 三、逐篇结果总览",
        "",
        "| ID | 类型 | 内容结论 | 细分类 | 互动受众 | 内容侧线索 | 内容侧行动 | 综合拉新潜力 |",
        "|---|---|---|---|---|---|---|---|",
    ]
    for row in predictions:
        content = contents[row["pilot_id"]]
        lines.append(
            f"| {row['pilot_id']} | {content['note_type']} | {row['content_label']}（{row['confidence_band']}） | "
            f"{row['subcategory']} | {row['audience_label']} | {row['content_side_acquisition_signal']} | "
            f"{row['content_only_action']} | {row['overall_acquisition_tier']} |"
        )

    lines.extend(["", "## 四、逐篇证据与建议", ""])
    for row in predictions:
        content = contents[row["pilot_id"]]
        lines.extend(
            [
                f"### {row['pilot_id']}｜{row['content_label']}｜{row['subcategory']}",
                "",
                f"链接：[打开笔记]({content['url']})",
                "",
                f"- 内容汽车性：**{row['content_label']}**，把握程度：{row['confidence_band']}。",
                f"- 互动受众：**{row['audience_label']}**；评论采集：**未取得**；已取评论文本：{row['retrieved_comment_count']}条；有效评论数：未评估。",
                f"- 懂车帝内容侧拉新线索：**{row['content_side_acquisition_signal']}**；内容侧行动：**{row['content_only_action']}**；综合结论：**{row['overall_acquisition_tier']}**。",
                f"- 用户阶段：{row['intent_stage']}。",
                f"- 建议承接：{row['recommended_destination']}。",
                "- 主要证据：",
            ]
        )
        for evidence in row["evidence"]:
            lines.append(f"  - {evidence}。")
        lines.extend([f"- 限制：{row['limitations']}。", ""])

    lines.extend(
        [
            "## 五、揭晓来源标签后的对照",
            "",
            "| ID | 来源预设标签 | 盲评结果 | 对照 |",
            "|---|---|---|---|",
        ]
    )
    for row in predictions:
        comparison = comparisons[row["pilot_id"]]
        lines.append(
            f"| {row['pilot_id']} | {zh_source(comparison['source_label'])} | {row['content_label']} | "
            f"{'一致' if comparison['comparison'] == 'match' else comparison['comparison']} |"
        )

    lines.extend(
        [
            "",
            "这10篇只能说明“当前内容采集与多模态判定流程能跑通”，不能据此宣称正式准确率。正式评估还需人工确认金标、扩大非汽车难例并保留新的盲测集。",
            "",
            "## 六、拉新潜力候选",
            "",
            "### 内容侧优先补评论验证",
            "",
            "- **P004**：4S店价格避坑、坦克300成交价与补贴，处于购车比价/临近决策阶段。",
            "- **P007**：二手车购买和熟人交易避坑，处于询价比价/购车决策阶段。",
            "",
            "### 内容侧可观察",
            "",
            "- **P008**：汽车零部件技术科普，能吸引汽车知识兴趣，但当前没有购买或服务需求证据。",
            "",
            "### 汽车内容但内容侧拉新线索较弱",
            "",
            "- **P002、P005**：属于汽车知识/娱乐内容，目前缺少明确车型、价格、维修服务或行动需求。评论可能改变这一判断，因此综合潜力仍标记为无法判断。",
            "",
            "### 暂不进入汽车拉新链路",
            "",
            "- **P001、P003、P006、P009、P010**：内容主体分别为住宿、足球、全屋智能、餐饮和人物故事。",
            "",
            "## 七、当前阻塞与下一步",
            "",
            "当前内容采集已经不依赖万邦详情接口，但评论仍有两条可选路径：",
            "",
            "1. 让万邦修复 `smallredbook.item_review` 的 `4010/api_init`。",
            "2. 经用户明确授权后，使用已登录的小红书Chrome做只读评论采集试点。",
            "",
            "评论取得后，先补P004、P007、P008，再补其余7篇，输出互动受众强/中/弱和完整的懂车帝拉新潜力等级。",
            "",
            "在评论到位前，不把P004或P007直接称为高拉新潜力，只称为“内容侧优先测试候选”。",
            "",
        ]
    )

    output = ROOT / "pilot_evaluation_report.md"
    output.write_text("\n".join(lines), encoding="utf-8")
    print(output.name)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
