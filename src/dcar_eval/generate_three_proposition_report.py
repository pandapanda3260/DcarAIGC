#!/usr/bin/env python3
"""Generate the user-facing three-proposition report from an attempt ledger.

The ledger remains the source of truth.  This module deliberately renders a
dash for propositions 2 and 3 when comment evidence is not scorable; it never
turns missing evidence into a synthetic 0 or 50.
"""

from __future__ import annotations

import argparse
import json
from collections import Counter
from datetime import date
from pathlib import Path
from statistics import mean
from typing import Any, Iterable

from project_paths import ARCHIVE_PROCESSED_DIR, ARCHIVE_REPORTS_DIR

from three_proposition_scoring import (
    MIN_VALID_COMMENTERS,
    acquisition_conclusion,
    audience_auto_score,
    audience_conclusion,
    content_conclusion,
    dcd_acquisition_score,
)


REPORT_VERSION = "three-proposition-report-v1.0"

COMMENT_SAMPLE_STATUSES = {
    "scorable",
    "below_minimum",
    "confirmed_zero",
    "technical_missing",
}
COMMENT_FETCH_STATUSES = {
    "complete",
    "partial",
    "confirmed_empty",
    "not_retrieved",
    "failed",
}
SOURCE_STRATA = ("auto", "non_auto")


def read_jsonl(path: Path) -> list[dict[str, Any]]:
    rows: list[dict[str, Any]] = []
    for line_number, line in enumerate(path.read_text(encoding="utf-8").splitlines(), 1):
        if not line.strip():
            continue
        try:
            value = json.loads(line)
        except json.JSONDecodeError as exc:
            raise ValueError(f"{path.name}:{line_number} is not valid JSON") from exc
        if not isinstance(value, dict):
            raise ValueError(f"{path.name}:{line_number} must contain a JSON object")
        rows.append(value)
    if not rows:
        raise ValueError(f"{path.name} contains no attempt rows")
    return rows


def _integer_score(value: Any, field: str, *, required: bool) -> int | None:
    if value is None:
        if required:
            raise ValueError(f"{field} is required")
        return None
    if isinstance(value, bool) or not isinstance(value, (int, float)):
        raise ValueError(f"{field} must be an integer score")
    if int(value) != value or not 0 <= int(value) <= 100:
        raise ValueError(f"{field} must be an integer between 0 and 100")
    return int(value)


def _nonnegative_integer_or_none(value: Any, field: str) -> int | None:
    if value is None:
        return None
    if isinstance(value, bool) or not isinstance(value, int) or value < 0:
        raise ValueError(f"{field} must be a non-negative integer or null")
    return value


def validate_attempt(row: dict[str, Any]) -> None:
    """Reject ledgers that disguise missing evidence as a numeric result."""

    attempt_id = row.get("sample_attempt_id")
    if not isinstance(attempt_id, str) or not attempt_id:
        raise ValueError("sample_attempt_id is required")

    prefix = f"{attempt_id}: "
    try:
        _integer_score(row.get("content_auto_score"), "content_auto_score", required=True)
        status = row.get("comment_sample_status")
        if status not in COMMENT_SAMPLE_STATUSES:
            raise ValueError(f"unknown comment_sample_status {status!r}")
        fetch_status = row.get("comment_fetch_status")
        if fetch_status not in COMMENT_FETCH_STATUSES:
            raise ValueError(f"unknown comment_fetch_status {fetch_status!r}")

        commenters = _nonnegative_integer_or_none(
            row.get("valid_unique_commenters"), "valid_unique_commenters"
        )
        raw_comment_count = _nonnegative_integer_or_none(
            row.get("raw_comment_count"), "raw_comment_count"
        )
        comment_pages_fetched = _nonnegative_integer_or_none(
            row.get("comment_pages_fetched"), "comment_pages_fetched"
        )
        pagination_complete = row.get("comment_pagination_complete")
        if pagination_complete is not None and not isinstance(pagination_complete, bool):
            raise ValueError("comment_pagination_complete must be boolean or null")
        final_eligible = row.get("final_sample_eligible")
        if not isinstance(final_eligible, bool):
            raise ValueError("final_sample_eligible must be boolean")

        gated_fields = (
            "audience_score_counts",
            "audience_auto_score",
            "audience_auto_conclusion",
            "audience_auto_evidence",
            "dcd_fit_score",
            "action_intent_score",
            "dcd_acquisition_score",
            "dcd_acquisition_conclusion",
            "dcd_acquisition_evidence",
        )

        if status == "scorable":
            if fetch_status not in {"complete", "partial"}:
                raise ValueError(
                    "scorable comments require comment_fetch_status=complete or partial"
                )
            if raw_comment_count is None or raw_comment_count <= 0:
                raise ValueError("scorable comments require raw_comment_count > 0")
            if comment_pages_fetched is None or comment_pages_fetched <= 0:
                raise ValueError("scorable comments require comment_pages_fetched > 0")
            if fetch_status == "complete" and pagination_complete is not True:
                raise ValueError(
                    "comment_fetch_status=complete requires comment_pagination_complete=true"
                )
            if fetch_status == "partial" and pagination_complete is not False:
                raise ValueError(
                    "comment_fetch_status=partial requires comment_pagination_complete=false"
                )
            if commenters is None or commenters < MIN_VALID_COMMENTERS:
                raise ValueError(
                    f"scorable comments require at least {MIN_VALID_COMMENTERS} valid users"
                )
            if raw_comment_count < commenters:
                raise ValueError(
                    "raw_comment_count cannot be smaller than valid_unique_commenters"
                )
            counts = row.get("audience_score_counts")
            if not isinstance(counts, dict):
                raise ValueError("audience_score_counts is required for a scorable sample")
            stored_audience = _integer_score(
                row.get("audience_auto_score"), "audience_auto_score", required=True
            )
            calculated_audience = audience_auto_score(
                counts,
                valid_unique_commenters=commenters,
                comment_sample_status="scorable",
            )
            if stored_audience != calculated_audience:
                raise ValueError(
                    "audience_auto_score does not match audience_score_counts "
                    f"({stored_audience} != {calculated_audience})"
                )

            fit = _integer_score(row.get("dcd_fit_score"), "dcd_fit_score", required=True)
            intent = _integer_score(
                row.get("action_intent_score"), "action_intent_score", required=True
            )
            stored_acquisition = _integer_score(
                row.get("dcd_acquisition_score"), "dcd_acquisition_score", required=True
            )
            calculated_acquisition = dcd_acquisition_score(
                content_score=row["content_auto_score"],
                audience_score=stored_audience,
                dcd_fit_score=fit,
                action_intent_score=intent,
            )
            if stored_acquisition != calculated_acquisition:
                raise ValueError(
                    "dcd_acquisition_score does not match the v1 formula "
                    f"({stored_acquisition} != {calculated_acquisition})"
                )
        else:
            if final_eligible:
                raise ValueError("a non-scorable sample cannot be final_sample_eligible")
            present = [field for field in gated_fields if row.get(field) is not None]
            if present:
                raise ValueError(
                    "non-scorable comments require null proposition 2/3 fields; "
                    f"found values in {', '.join(present)}"
                )
            if status == "technical_missing" and commenters is not None:
                raise ValueError("technical_missing requires valid_unique_commenters=null")
            if status == "confirmed_zero":
                if commenters != 0:
                    raise ValueError("confirmed_zero requires valid_unique_commenters=0")
                if fetch_status != "confirmed_empty":
                    raise ValueError("confirmed_zero requires comment_fetch_status=confirmed_empty")
                if raw_comment_count != 0:
                    raise ValueError("confirmed_zero requires raw_comment_count=0")
                if comment_pages_fetched is None or comment_pages_fetched <= 0:
                    raise ValueError("confirmed_zero requires comment_pages_fetched > 0")
                if pagination_complete is not True:
                    raise ValueError(
                        "confirmed_zero requires comment_pagination_complete=true"
                    )
            if status == "below_minimum" and not (
                commenters is not None and 0 < commenters < MIN_VALID_COMMENTERS
            ):
                raise ValueError(
                    f"below_minimum requires 1-{MIN_VALID_COMMENTERS - 1} valid users"
                )

        if final_eligible and status != "scorable":
            raise ValueError("final_sample_eligible requires comment_sample_status=scorable")
        if final_eligible and row.get("source_stratum") not in SOURCE_STRATA:
            raise ValueError(
                "final_sample_eligible requires source_stratum=auto or non_auto"
            )

        actual_status = row.get("actual_status")
        if actual_status == "not_tested":
            actual_fields = ("actual_clicks", "actual_installs", "actual_confirmed_new_users")
            present_actual = [field for field in actual_fields if row.get(field) is not None]
            if present_actual:
                raise ValueError(
                    "actual_status=not_tested requires null actual-effect fields; "
                    f"found values in {', '.join(present_actual)}"
                )
    except ValueError as exc:
        raise ValueError(prefix + str(exc)) from exc


def validate_attempts(rows: Iterable[dict[str, Any]]) -> list[dict[str, Any]]:
    validated = list(rows)
    seen: set[str] = set()
    for row in validated:
        validate_attempt(row)
        attempt_id = row["sample_attempt_id"]
        if attempt_id in seen:
            raise ValueError(f"duplicate sample_attempt_id: {attempt_id}")
        seen.add(attempt_id)
    return validated


def _rounded_mean(rows: list[dict[str, Any]], field: str) -> int | None:
    values = [row[field] for row in rows if row.get(field) is not None]
    return round(mean(values)) if values else None


def summarize_attempts(
    rows: list[dict[str, Any]], *, target_final_samples: int = 10
) -> dict[str, Any]:
    if target_final_samples <= 0:
        raise ValueError("target_final_samples must be positive")
    if target_final_samples % len(SOURCE_STRATA):
        raise ValueError(
            "target_final_samples must be evenly divisible across auto and non_auto"
        )
    validated = validate_attempts(rows)
    base_rows = [row for row in validated if row.get("sample_role") == "base_random_sample"]
    if not base_rows:
        base_rows = validated
    final_rows = [row for row in validated if row.get("final_sample_eligible")]
    target_per_stratum = target_final_samples // len(SOURCE_STRATA)
    target_by_stratum = {
        stratum: target_per_stratum for stratum in SOURCE_STRATA
    }
    final_by_stratum = Counter(row["source_stratum"] for row in final_rows)
    final_count_by_stratum = {
        stratum: final_by_stratum.get(stratum, 0) for stratum in SOURCE_STRATA
    }
    remaining_by_stratum = {
        stratum: max(0, target_by_stratum[stratum] - final_count_by_stratum[stratum])
        for stratum in SOURCE_STRATA
    }
    filled_balanced_slots = sum(
        min(final_count_by_stratum[stratum], target_by_stratum[stratum])
        for stratum in SOURCE_STRATA
    )
    semantic_fetch_rows = [
        row
        for row in validated
        if row.get("comment_fetch_status") in {"complete", "partial", "confirmed_empty"}
    ]
    comment_body_rows = [row for row in validated if (row.get("raw_comment_count") or 0) > 0]

    content_score = _rounded_mean(base_rows, "content_auto_score")
    audience_score = _rounded_mean(final_rows, "audience_auto_score")
    acquisition_score = _rounded_mean(final_rows, "dcd_acquisition_score")

    return {
        "report_version": REPORT_VERSION,
        "prediction_version": sorted(
            {row.get("prediction_version") for row in validated if row.get("prediction_version")}
        ),
        "counts": {
            "target_final_samples": target_final_samples,
            "target_final_by_stratum": target_by_stratum,
            "attempts": len(validated),
            "base_random_samples": len(base_rows),
            "replacement_attempts": len(validated) - len(base_rows),
            "comment_fetch_semantic_success": len(semantic_fetch_rows),
            "comment_body_retrieved": len(comment_body_rows),
            "comment_scorable_attempts": sum(
                row.get("comment_sample_status") == "scorable" for row in validated
            ),
            "final_sample_eligible": len(final_rows),
            "final_sample_eligible_by_stratum": final_count_by_stratum,
            "final_sample_slots_filled": filled_balanced_slots,
            "final_sample_remaining": sum(remaining_by_stratum.values()),
            "final_sample_remaining_by_stratum": remaining_by_stratum,
            "valid_unique_commenters_final": sum(
                row.get("valid_unique_commenters") or 0 for row in final_rows
            ),
        },
        "rates": {
            "comment_fetch_semantic_success_of_attempts": round(
                len(semantic_fetch_rows) / len(validated), 4
            ),
            "comment_scorable_of_attempts": round(len(final_rows) / len(validated), 4),
        },
        "failure_reasons": dict(
            Counter(
                row.get("replacement_reason") or "none"
                for row in validated
                if not row.get("final_sample_eligible")
            )
        ),
        "propositions": {
            "content_automotive": {
                "score": content_score,
                "sample_size": len(base_rows),
                "scope": "基础随机样本，按笔记等权",
                "conclusion": content_conclusion(content_score) if content_score is not None else None,
            },
            "audience_automotive": {
                "score": audience_score,
                "sample_size": len(final_rows),
                "scope": "评论达标且进入最终样本的笔记，按笔记等权",
                "conclusion": (
                    audience_conclusion(audience_score)
                    if audience_score is not None
                    else "未评分：当前没有评论证据达标的最终样本"
                ),
            },
            "dcd_acquisition_potential": {
                "score": acquisition_score,
                "sample_size": len(final_rows),
                "scope": "评论达标且进入最终样本的笔记，按笔记等权",
                "conclusion": (
                    acquisition_conclusion(acquisition_score)
                    if acquisition_score is not None
                    else "未评分：命题2不可计算，不能推断拉新潜力"
                ),
                "is_prediction_not_actual_effect": True,
            },
        },
        "report_state": (
            "complete"
            if all(value == 0 for value in remaining_by_stratum.values())
            else "partial"
            if final_rows
            else "awaiting_scorable_comments"
        ),
    }


def _status_label(row: dict[str, Any]) -> str:
    status = row["comment_sample_status"]
    commenters = row.get("valid_unique_commenters")
    if status == "scorable":
        return f"评论达标（{commenters}名有效独立用户）"
    if status == "technical_missing":
        return "评论正文技术缺失，不等于0评论"
    if status == "confirmed_zero":
        return "平台已确认0评论"
    return f"仅{commenters}名有效独立用户，低于{MIN_VALID_COMMENTERS}人门槛"


def _missing_audience_text(row: dict[str, Any]) -> str:
    if row["comment_sample_status"] == "technical_missing":
        return "—（评论正文技术缺失）"
    if row["comment_sample_status"] == "confirmed_zero":
        return "—（已确认0评论）"
    commenters = row.get("valid_unique_commenters")
    return f"—（{commenters}名有效用户，未达{MIN_VALID_COMMENTERS}人门槛）"


def _conclusion(row: dict[str, Any], field: str, fallback: str) -> str:
    value = row.get(field)
    return value.strip() if isinstance(value, str) and value.strip() else fallback


def _evidence(row: dict[str, Any], field: str) -> str:
    values = row.get(field)
    if not isinstance(values, list):
        return ""
    clean = [str(value).strip() for value in values if str(value).strip()]
    return "；".join(clean[:3])


def _score_cell(score: int, conclusion: str, *, predicted: bool = False) -> str:
    suffix = "（预测分）" if predicted else ""
    return f"{score}/100{suffix}<br>{conclusion}"


def _format_rate(value: float) -> str:
    return f"{value * 100:.1f}%"


def render_markdown(
    rows: list[dict[str, Any]],
    summary: dict[str, Any],
    *,
    title: str,
    report_date: str,
    input_label: str,
) -> str:
    validated = validate_attempts(rows)
    props = summary["propositions"]
    counts = summary["counts"]
    rates = summary["rates"]

    def aggregate_score(key: str, *, predicted: bool = False) -> str:
        proposition = props[key]
        score = proposition["score"]
        if score is None:
            return f"—<br>{proposition['conclusion']}"
        suffix = "（预测分）" if predicted else ""
        return f"{score}/100{suffix}<br>{proposition['conclusion']}"

    lines = [
        f"# {title}",
        "",
        f"报告版本：`{REPORT_VERSION}`  ",
        f"评分规则：`{', '.join(summary['prediction_version']) or '未标注'}`  ",
        f"数据源：`{input_label}`  ",
        f"生成日期：{report_date}",
        "",
        "## 一、三个命题的汇总结论",
        "",
        "| 最终业务命题 | 得分与定性说明 | 样本范围 |",
        "|---|---|---|",
        f"| 命题1：内容是否属于汽车类 | {aggregate_score('content_automotive')} | {props['content_automotive']['scope']}，n={props['content_automotive']['sample_size']} |",
        f"| 命题2：互动用户是否偏汽车 | {aggregate_score('audience_automotive')} | {props['audience_automotive']['scope']}，n={props['audience_automotive']['sample_size']} |",
        f"| 命题3：是否具备懂车帝用户拉新潜力 | {aggregate_score('dcd_acquisition_potential', predicted=True)} | {props['dcd_acquisition_potential']['scope']}，n={props['dcd_acquisition_potential']['sample_size']} |",
        "",
    ]

    if summary["report_state"] != "complete":
        lines.extend(
            [
                "> 当前是阶段性报告：命题2、3显示“—”代表评论证据未达到计算条件，不代表0分，也没有用50分代填。",
                "",
            ]
        )

    lines.extend(
        [
            "## 二、逐篇三个命题结果",
            "",
            "| 样本 | 数据状态 | 命题1：内容汽车属性 | 命题2：用户汽车倾向 | 命题3：懂车帝拉新潜力 |",
            "|---|---|---|---|---|",
        ]
    )
    for row in validated:
        content_score = row["content_auto_score"]
        content_text = _conclusion(
            row, "content_auto_conclusion", content_conclusion(content_score)
        )
        if row["comment_sample_status"] == "scorable":
            audience_score = row["audience_auto_score"]
            acquisition_score = row["dcd_acquisition_score"]
            audience_cell = _score_cell(
                audience_score,
                _conclusion(
                    row, "audience_auto_conclusion", audience_conclusion(audience_score)
                ),
            )
            acquisition_cell = _score_cell(
                acquisition_score,
                _conclusion(
                    row,
                    "dcd_acquisition_conclusion",
                    acquisition_conclusion(acquisition_score),
                ),
                predicted=True,
            )
        else:
            audience_cell = _missing_audience_text(row)
            acquisition_cell = "—（命题2不可计算，不对拉新潜力作假设）"
        lines.append(
            f"| {row['sample_attempt_id']} | {_status_label(row)} | "
            f"{_score_cell(content_score, content_text)} | {audience_cell} | {acquisition_cell} |"
        )

    lines.extend(["", "## 三、逐篇定性说明与依据", ""])
    for row in validated:
        content_score = row["content_auto_score"]
        content_text = _conclusion(
            row, "content_auto_conclusion", content_conclusion(content_score)
        )
        lines.extend(
            [
                f"### {row['sample_attempt_id']}",
                "",
                f"- 数据状态：{_status_label(row)}。",
                f"- 命题1：`{content_score}/100`。{content_text}",
            ]
        )
        content_evidence = _evidence(row, "content_auto_evidence")
        if content_evidence:
            lines.append(f"  - 关键依据：{content_evidence}。")
        if row["comment_sample_status"] == "scorable":
            audience_score = row["audience_auto_score"]
            acquisition_score = row["dcd_acquisition_score"]
            lines.append(
                f"- 命题2：`{audience_score}/100`。"
                + _conclusion(
                    row, "audience_auto_conclusion", audience_conclusion(audience_score)
                )
            )
            audience_evidence = _evidence(row, "audience_auto_evidence")
            if audience_evidence:
                lines.append(f"  - 关键依据：{audience_evidence}。")
            lines.append(
                f"- 命题3：`{acquisition_score}/100`（预测分）。"
                + _conclusion(
                    row,
                    "dcd_acquisition_conclusion",
                    acquisition_conclusion(acquisition_score),
                )
            )
            acquisition_evidence = _evidence(row, "dcd_acquisition_evidence")
            if acquisition_evidence:
                lines.append(f"  - 关键依据：{acquisition_evidence}。")
        else:
            lines.extend(
                [
                    f"- 命题2：{_missing_audience_text(row)}。",
                    "- 命题3：—（命题2不可计算，不对拉新潜力作假设）。",
                ]
            )
        lines.append("")

    failure_reasons = summary["failure_reasons"]
    target_by_stratum = counts["target_final_by_stratum"]
    final_by_stratum = counts["final_sample_eligible_by_stratum"]
    stratum_progress = "，".join(
        f"{stratum} {final_by_stratum[stratum]}/{target_by_stratum[stratum]}"
        for stratum in SOURCE_STRATA
    )
    failure_text = "、".join(
        f"{reason}={count}" for reason, count in sorted(failure_reasons.items())
    ) or "无"
    lines.extend(
        [
            "## 四、数据质量与抽检状态",
            "",
            f"- 基础随机样本：`{counts['base_random_samples']}`篇；全部尝试：`{counts['attempts']}`篇；替补尝试：`{counts['replacement_attempts']}`篇。",
            f"- 评论接口取得明确语义结果：`{counts['comment_fetch_semantic_success']}/{counts['attempts']}`（{_format_rate(rates['comment_fetch_semantic_success_of_attempts'])}）。",
            f"- 实际取得评论正文：`{counts['comment_body_retrieved']}/{counts['attempts']}`篇。",
            f"- 最终评论达标记录：`{counts['final_sample_eligible']}`篇；已填平衡目标槽位：`{counts['final_sample_slots_filled']}/{counts['target_final_samples']}`（{stratum_progress}）；占全部尝试`{_format_rate(rates['comment_scorable_of_attempts'])}`；还需`{counts['final_sample_remaining']}`篇。",
            f"- 最终样本累计有效独立评论用户：`{counts['valid_unique_commenters_final']}`人。",
            f"- 未入最终样本的原因：{failure_text}。",
            "",
            "## 五、结论边界",
            "",
            "- 命题1汇总基于基础随机样本；命题2、3只汇总评论达到20名有效独立用户门槛且进入最终样本的笔记。",
            "- 所有汇总均按笔记等权，不按评论数加权。来源层的5+5平衡样本不能外推为账号全部笔记的自然占比。",
            "- 命题3是用于候选排序的预测分，不是下载概率，也不是懂车帝侧实际新增效果；实际效果需等待点击、下载、激活、注册和新用户数据验证。",
            "- `not_retrieved`、`failed`和`technical_missing`属于数据状态；只有平台成功返回无评论时，才认定为真实0评论。",
            "",
        ]
    )
    return "\n".join(lines)


def write_outputs(
    rows: list[dict[str, Any]],
    *,
    report_path: Path,
    summary_path: Path,
    title: str,
    report_date: str,
    input_label: str,
    target_final_samples: int = 10,
) -> dict[str, Any]:
    summary = summarize_attempts(rows, target_final_samples=target_final_samples)
    report = render_markdown(
        rows,
        summary,
        title=title,
        report_date=report_date,
        input_label=input_label,
    )
    report_path.write_text(report, encoding="utf-8")
    summary_path.write_text(
        json.dumps(summary, ensure_ascii=False, indent=2) + "\n", encoding="utf-8"
    )
    return summary


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument(
        "--input",
        type=Path,
        default=ARCHIVE_PROCESSED_DIR / "pilot_three_proposition_attempts_v0.3.jsonl",
        help="Attempt-ledger JSONL path",
    )
    parser.add_argument(
        "--report",
        type=Path,
        default=ARCHIVE_REPORTS_DIR / "pilot_three_proposition_report_v1.md",
        help="Markdown report output path",
    )
    parser.add_argument(
        "--summary",
        type=Path,
        default=ARCHIVE_PROCESSED_DIR / "pilot_three_proposition_summary_v1.json",
        help="Machine-readable summary output path",
    )
    parser.add_argument(
        "--title",
        default="小红书笔记三命题100分评估报告",
        help="Report title",
    )
    parser.add_argument("--date", default=date.today().isoformat(), help="Report date")
    parser.add_argument(
        "--target-final",
        type=int,
        default=10,
        help="Target number of final scorable notes",
    )
    return parser.parse_args()


def main() -> int:
    args = parse_args()
    rows = read_jsonl(args.input)
    summary = write_outputs(
        rows,
        report_path=args.report,
        summary_path=args.summary,
        title=args.title,
        report_date=args.date,
        input_label=args.input.name,
        target_final_samples=args.target_final,
    )
    print(json.dumps(summary, ensure_ascii=False, indent=2))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
