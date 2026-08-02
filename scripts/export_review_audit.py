#!/usr/bin/env python3
"""Export append-only evaluation review lineage for human business audit."""

from __future__ import annotations

import argparse
import csv
import json
import sqlite3
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Dict, Iterable, List
from zoneinfo import ZoneInfo


PROJECT_ROOT = Path(__file__).resolve().parents[1]
DEFAULT_DB = PROJECT_ROOT / "app" / "data" / "dcar_insight.sqlite3"
DEFAULT_OUTPUT_DIR = PROJECT_ROOT / "reports" / "review_audits"
DEFAULT_REVIEWER = "Codex人工复核-2026-08-02"
DEFAULT_STEM = "灰区复核人工确认_2026-08-02"
SHANGHAI = ZoneInfo("Asia/Shanghai")


REVIEW_SQL = """
SELECT er.id review_id, er.queue_id, er.content_id, er.previous_evaluation_id,
       er.resulting_evaluation_id, er.decision, er.reason, er.reviewer,
       er.created_at reviewed_at_utc,
       rq.reason_code queue_reason_code, rq.status queue_status,
       c.link_id, c.platform, c.platform_content_id, c.canonical_url,
       c.published_at, c.title, c.body, c.content_type,
       c.raw_account_uid, c.raw_account_name,
       me.evidence_type manual_evidence_type,
       me.text_value manual_evidence_text,
       me.local_path manual_evidence_local_path,
       me.sha256 manual_evidence_sha256,
       prev.rule_version before_rule_version,
       prev.taxonomy_version before_taxonomy_version,
       prev.evaluation_source before_evaluation_source,
       prev.evaluated_at before_evaluated_at,
       prev.evidence_envelope_id before_evidence_envelope_id,
       prev.evidence_sha256 before_evidence_sha256,
       prev.evidence_level before_evidence_level,
       prev.evaluation_status before_evaluation_status,
       prev.primary_selling_point_code before_primary_selling_point_code,
       prev.selling_point_score before_selling_point_score,
       prev.selling_point_included before_selling_point_included,
       prev.content_direction before_content_direction,
       prev.content_automotive_score before_content_automotive_score,
       prev.audience_automotive_score before_audience_automotive_score,
       prev.acquisition_potential_score before_acquisition_potential_score,
       prev.pending_review before_pending_review,
       prev.invalidated_at before_invalidated_at,
       prev.payload_json before_payload_json,
       result.rule_version after_rule_version,
       result.taxonomy_version after_taxonomy_version,
       result.evaluation_source after_evaluation_source,
       result.evaluated_at after_evaluated_at,
       result.evidence_envelope_id after_evidence_envelope_id,
       result.evidence_sha256 after_evidence_sha256,
       result.evidence_level after_evidence_level,
       result.evaluation_status after_evaluation_status,
       result.primary_selling_point_code after_primary_selling_point_code,
       result.selling_point_score after_selling_point_score,
       result.selling_point_included after_selling_point_included,
       result.content_direction after_content_direction,
       result.content_automotive_score after_content_automotive_score,
       result.audience_automotive_score after_audience_automotive_score,
       result.acquisition_potential_score after_acquisition_potential_score,
       result.pending_review after_pending_review,
       result.invalidated_at after_invalidated_at,
       result.payload_json after_payload_json,
       (
           SELECT ev.id FROM evaluation_versions ev
           WHERE ev.content_id=er.content_id AND ev.invalidated_at IS NULL
           ORDER BY ev.evaluated_at DESC, ev.id DESC LIMIT 1
       ) current_latest_evaluation_id
FROM evaluation_reviews er
JOIN review_queue rq ON rq.id=er.queue_id
JOIN content_items c ON c.id=er.content_id
JOIN evaluation_versions prev ON prev.id=er.previous_evaluation_id
JOIN evaluation_versions result ON result.id=er.resulting_evaluation_id
LEFT JOIN manual_evidence me ON me.id=(
    SELECT me2.id FROM manual_evidence me2
    WHERE me2.review_id=er.id ORDER BY me2.id DESC LIMIT 1
)
WHERE er.reviewer=? AND rq.reason_code='evaluation_gray_zone'
ORDER BY er.id
"""


def _json_object(value: Any) -> Dict[str, Any]:
    if not value:
        return {}
    parsed = json.loads(str(value))
    return parsed if isinstance(parsed, dict) else {}


def _shanghai_text(value: str) -> str:
    parsed = datetime.fromisoformat(value.replace("Z", "+00:00"))
    return parsed.astimezone(SHANGHAI).isoformat(timespec="seconds")


def _allowed_scenes(
    connection: sqlite3.Connection, taxonomy_version: str, code: str | None
) -> List[str]:
    if not code:
        return []
    rows = connection.execute(
        """
        SELECT sps.scene
        FROM taxonomy_versions tv
        JOIN selling_points sp ON sp.taxonomy_id=tv.id
        JOIN selling_point_scenes sps ON sps.selling_point_id=sp.id
        WHERE tv.version=? AND sp.code=?
        ORDER BY sps.scene
        """,
        (taxonomy_version, code),
    ).fetchall()
    return [str(row["scene"]) for row in rows]


def load_review_audit(db_path: Path, reviewer: str) -> List[Dict[str, Any]]:
    checked_at = datetime.now(timezone.utc).isoformat(timespec="seconds").replace(
        "+00:00", "Z"
    )
    connection = sqlite3.connect(f"file:{db_path}?mode=ro", uri=True)
    connection.row_factory = sqlite3.Row
    try:
        output: List[Dict[str, Any]] = []
        for raw_row in connection.execute(REVIEW_SQL, (reviewer,)).fetchall():
            row = dict(raw_row)
            before_payload = _json_object(row.pop("before_payload_json"))
            after_payload = _json_object(row.pop("after_payload_json"))
            code = row["after_primary_selling_point_code"]
            direction = str(row["after_content_direction"])
            allowed_scenes = _allowed_scenes(
                connection, str(row["after_taxonomy_version"]), code
            )
            row.update(
                {
                    "reviewed_at_asia_shanghai": _shanghai_text(
                        str(row["reviewed_at_utc"])
                    ),
                    "before_payload": before_payload,
                    "after_payload": after_payload,
                    "before_valid_unique_commenters": before_payload.get(
                        "valid_unique_commenters"
                    ),
                    "before_action_intent_score": before_payload.get(
                        "action_intent_score"
                    ),
                    "before_dcar_task_fit_score": before_payload.get(
                        "dcar_task_fit_score"
                    ),
                    "after_valid_unique_commenters": after_payload.get(
                        "valid_unique_commenters"
                    ),
                    "after_action_intent_score": after_payload.get(
                        "action_intent_score"
                    ),
                    "after_dcar_task_fit_score": after_payload.get(
                        "dcar_task_fit_score"
                    ),
                    "primary_selling_point_allowed_scenes": allowed_scenes,
                    "scene_consistent": not code or direction in allowed_scenes,
                    "result_is_current_latest": (
                        row["resulting_evaluation_id"]
                        == row["current_latest_evaluation_id"]
                    ),
                    "current_checked_at": checked_at,
                    "evidence_api_url": (
                        "http://127.0.0.1:8765/api/v8/contents/"
                        f"{row['content_id']}/evidence"
                    ),
                }
            )
            output.append(row)
        return output
    finally:
        connection.close()


def _display(value: Any) -> str:
    if value is None:
        return "—"
    if isinstance(value, bool):
        return "是" if value else "否"
    return str(value)


def _evaluation_summary(row: Dict[str, Any], prefix: str) -> str:
    code = row[f"{prefix}_primary_selling_point_code"] or "无"
    included = "计入" if row[f"{prefix}_selling_point_included"] else "不计入"
    return (
        f"卖点 {code}/{_display(row[f'{prefix}_selling_point_score'])}（{included}）；"
        f"方向 {_display(row[f'{prefix}_content_direction'])}；"
        f"内容/受众/拉新 "
        f"{_display(row[f'{prefix}_content_automotive_score'])}/"
        f"{_display(row[f'{prefix}_audience_automotive_score'])}/"
        f"{_display(row[f'{prefix}_acquisition_potential_score'])}；"
        f"证据 {_display(row[f'{prefix}_evidence_level'])}"
    )


def _markdown_quote(value: str) -> str:
    lines = value.splitlines() or [""]
    return "\n".join(">" if not line.strip() else f"> {line.rstrip()}" for line in lines)


def render_markdown(rows: List[Dict[str, Any]], reviewer: str) -> str:
    generated_at = rows[0]["current_checked_at"] if rows else ""
    inconsistent = [row for row in rows if not row["scene_consistent"]]
    lines = [
        "# 9 条灰区复核人工确认清单",
        "",
        "> 状态说明：下列结论由实施代理 `Codex人工复核-2026-08-02` 提交，",
        "> 机制留痕有效，但尚未经过业务负责人或运营人员确认。本文档不代表业务审批完成。",
        "",
        f"- 导出 reviewer：`{reviewer}`",
        f"- 导出时间（UTC）：`{generated_at}`",
        f"- 记录数：`{len(rows)}`；decision 均为 `override`",
        "- 当前数据库状态未改动；认可时无需操作，不认可时在内容页搜索链接 ID，点击“再次复核”。",
        f"- 卖点—场景一致性异常：`{len(inconsistent)}` 条。",
        "",
        "## 汇总",
        "",
        "| 待确认 | 链接 ID | 内容 | 原评估 | Codex 改判 | 复核理由 | 场景校验 |",
        "| --- | --- | --- | --- | --- | --- | --- |",
    ]
    for row in rows:
        title = " ".join(str(row["title"] or row["body"] or "内容缺失").split())
        title = title[:48] + ("…" if len(title) > 48 else "")
        scene = "通过" if row["scene_consistent"] else "冲突，必须复审"
        lines.append(
            "| ☐认可 ☐不认可 | "
            f"[{row['link_id']}]({row['canonical_url']}) | {title} | "
            f"{_evaluation_summary(row, 'before')} | "
            f"{_evaluation_summary(row, 'after')} | {row['reason']} | {scene} |"
        )

    lines.extend(
        [
            "",
            "## 逐条确认",
            "",
            "每条必须由真实业务复核人员填写结论。受众/拉新为 `—` 表示 NULL，不能当作 0。",
            "",
        ]
    )
    for index, row in enumerate(rows, start=1):
        allowed = ", ".join(row["primary_selling_point_allowed_scenes"]) or "不适用"
        lines.extend(
            [
                f"### {index}. {row['link_id']}",
                "",
                f"- 原内容：[打开小红书链接]({row['canonical_url']})",
                f"- 本地证据 API：`{row['evidence_api_url']}`",
                f"- 内容 ID / 队列 ID / 复核 ID：`{row['content_id']} / {row['queue_id']} / {row['review_id']}`",
                f"- 发布账号：{_display(row['raw_account_name'])}；发布时间：{_display(row['published_at'])}",
                f"- 原评估 #{row['previous_evaluation_id']}：{_evaluation_summary(row, 'before')}",
                f"- Codex 改判 #{row['resulting_evaluation_id']}：{_evaluation_summary(row, 'after')}",
                f"- 当前仍为最新评估：{_display(row['result_is_current_latest'])}",
                f"- 复核理由：{row['reason']}",
                f"- 人工证据类型 / SHA-256：`{row['manual_evidence_type']}` / `{row['manual_evidence_sha256']}`",
                f"- 卖点允许场景：`{allowed}`；改判方向：`{row['after_content_direction']}`；一致性：`{'通过' if row['scene_consistent'] else '冲突'}`",
                "",
                "**内容原文**",
                "",
                _markdown_quote(str(row["title"] or row["body"] or "内容缺失")),
                "",
                "**Codex 提交的证据摘要（仅供核查，不等于人工认可）**",
                "",
                _markdown_quote(str(row["manual_evidence_text"] or "证据摘要缺失")),
                "",
                "**业务确认（请填写）**",
                "",
                "- [ ] 认可并保留当前版本",
                "- [ ] 不认可，已在证据工作台再次复核",
                "- 复核人：",
                "- 复核时间：",
                "- 备注：",
                "",
            ]
        )
    return "\n".join(lines).rstrip() + "\n"


CSV_FIELDS = [
    "link_id",
    "content_id",
    "platform",
    "platform_content_id",
    "canonical_url",
    "published_at",
    "title",
    "body",
    "raw_account_uid",
    "raw_account_name",
    "review_id",
    "queue_id",
    "queue_reason_code",
    "queue_status",
    "decision",
    "reason",
    "reviewer",
    "reviewed_at_utc",
    "reviewed_at_asia_shanghai",
    "manual_evidence_type",
    "manual_evidence_text",
    "manual_evidence_sha256",
    "previous_evaluation_id",
    "before_evaluation_source",
    "before_evidence_level",
    "before_primary_selling_point_code",
    "before_selling_point_score",
    "before_selling_point_included",
    "before_content_direction",
    "before_content_automotive_score",
    "before_audience_automotive_score",
    "before_acquisition_potential_score",
    "before_pending_review",
    "resulting_evaluation_id",
    "after_evaluation_source",
    "after_evidence_level",
    "after_primary_selling_point_code",
    "after_selling_point_score",
    "after_selling_point_included",
    "after_content_direction",
    "after_content_automotive_score",
    "after_audience_automotive_score",
    "after_acquisition_potential_score",
    "after_pending_review",
    "current_latest_evaluation_id",
    "result_is_current_latest",
    "primary_selling_point_allowed_scenes",
    "scene_consistent",
    "current_checked_at",
    "evidence_api_url",
    "business_confirmation",
    "business_reviewer",
    "business_reviewed_at",
    "business_note",
]


def write_csv(path: Path, rows: Iterable[Dict[str, Any]]) -> None:
    with path.open("w", encoding="utf-8-sig", newline="") as handle:
        writer = csv.DictWriter(handle, fieldnames=CSV_FIELDS, lineterminator="\n")
        writer.writeheader()
        for source in rows:
            row = {
                key: (
                    source.get(key).replace("\r\n", "\n")
                    .replace("\r", "\n")
                    .replace("\t", "\\t")
                    .replace("\n", "\\n")
                    if isinstance(source.get(key), str)
                    else source.get(key)
                )
                for key in CSV_FIELDS
            }
            row["primary_selling_point_allowed_scenes"] = ",".join(
                source["primary_selling_point_allowed_scenes"]
            )
            row.update(
                {
                    "business_confirmation": "",
                    "business_reviewer": "",
                    "business_reviewed_at": "",
                    "business_note": "",
                }
            )
            writer.writerow(row)


def export_review_audit(
    *, db_path: Path, output_dir: Path, reviewer: str, stem: str
) -> Dict[str, Any]:
    rows = load_review_audit(db_path, reviewer)
    if len(rows) != 9:
        raise RuntimeError(f"expected 9 gray-zone reviews, found {len(rows)}")
    if any(row["decision"] != "override" for row in rows):
        raise RuntimeError("all selected gray-zone reviews must be override decisions")
    output_dir.mkdir(parents=True, exist_ok=True)
    json_path = output_dir / f"{stem}.json"
    csv_path = output_dir / f"{stem}.csv"
    markdown_path = output_dir / f"{stem}.md"
    json_path.write_text(
        json.dumps(
            {
                "schema_version": "review-audit-v1",
                "reviewer": reviewer,
                "record_count": len(rows),
                "records": rows,
            },
            ensure_ascii=False,
            indent=2,
        )
        + "\n",
        encoding="utf-8",
    )
    write_csv(csv_path, rows)
    markdown_path.write_text(render_markdown(rows, reviewer), encoding="utf-8")
    return {
        "record_count": len(rows),
        "scene_conflict_count": sum(not row["scene_consistent"] for row in rows),
        "markdown": str(markdown_path),
        "csv": str(csv_path),
        "json": str(json_path),
    }


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--db", type=Path, default=DEFAULT_DB)
    parser.add_argument("--output-dir", type=Path, default=DEFAULT_OUTPUT_DIR)
    parser.add_argument("--reviewer", default=DEFAULT_REVIEWER)
    parser.add_argument("--stem", default=DEFAULT_STEM)
    return parser.parse_args()


def main() -> None:
    args = parse_args()
    print(
        json.dumps(
            export_review_audit(
                db_path=args.db,
                output_dir=args.output_dir,
                reviewer=args.reviewer,
                stem=args.stem,
            ),
            ensure_ascii=False,
            indent=2,
        )
    )


if __name__ == "__main__":
    main()
