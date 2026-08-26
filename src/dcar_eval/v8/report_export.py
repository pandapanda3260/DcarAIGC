"""Read-only report download bundles and dependency-free XLSX rendering.

The report revision CSV files are immutable snapshots.  This module turns those
snapshots into a presentation-friendly workbook at download time without
changing the task, revision, or registered report files.

账号页的「下载账号表格」导出（build_accounts_workbook）也复用这里的
XLSX 渲染，保持全站导出零第三方依赖。
"""

from __future__ import annotations

import csv
import io
import math
import re
import unicodedata
import zipfile
from dataclasses import dataclass
from datetime import datetime, timedelta, timezone
from typing import Any, Mapping, Sequence
from urllib.parse import unquote, urlsplit
from xml.sax.saxutils import escape, quoteattr


EXCEL_MAX_ROWS = 1_048_576
EXCEL_MAX_CELL_CHARS = 32_767
_INVALID_FILENAME_CHARS = re.compile(r'[\x00-\x1f\x7f<>:"/\\|?*]+')
_CSV_FORMULA_PREFIXES = ("=", "+", "-", "@")
_WINDOWS_RESERVED_FILENAMES = {
    "CON",
    "PRN",
    "AUX",
    "NUL",
    *(f"COM{index}" for index in range(1, 10)),
    *(f"LPT{index}" for index in range(1, 10)),
}

_CONTENT_EXPORT_COLUMNS = (
    ("report_period", "周期"),
    ("report_task", "报告任务"),
    ("platform_content_id", "平台作品编号"),
    ("link_id", "系统内容编号"),
    ("platform", "平台"),
    ("content_type", "内容类型"),
    ("published_at_beijing", "发布时间（北京时间）"),
    ("canonical_url", "内容链接"),
    ("title", "标题"),
    ("account_uid", "平台账号编号"),
    ("account_name", "账号名称"),
    ("account_type", "账号类型"),
    ("content_direction", "内容方向"),
    ("v3_status", "资料完整度"),
    ("primary_selling_point_code", "主要卖点编号"),
    ("primary_selling_point_label", "卖点信息"),
    ("selling_point_score", "卖点评分"),
    ("content_automotive_score", "内容垂直度"),
    ("view_count", "播放/阅读数"),
    ("comment_count", "评论数"),
)


def formula_safe_csv_value(value: Any) -> Any:
    """Prefix spreadsheet-interpreted CSV text without changing non-text values."""

    if not isinstance(value, str) or not value:
        return value
    stripped = value.lstrip()
    if value[0] in {"\t", "\r", "\n"} or stripped.startswith(
        _CSV_FORMULA_PREFIXES
    ):
        return f"'{value}"
    return value


_CONTENT_REQUIRED_HEADERS = {
    "content_id",
    "link_id",
    "platform",
    "published_at",
    "canonical_url",
    "title",
    "account_uid",
    "account_name",
    "account_type",
    "content_direction",
    "evidence_level",
    "primary_selling_point_code",
    "selling_point_score",
    "content_automotive_score",
    "view_count",
    "comment_count",
}
_MISSING_SELLING_POINT = "卖点资料不足"

_PLATFORM_LABELS = {
    "douyin": "抖音",
    "xiaohongshu": "小红书",
    "wechat_channels": "视频号",
    "kuaishou": "快手",
}
_CONTENT_TYPE_LABELS = {"video": "视频", "image": "图文", "unknown": "未知"}
_ACCOUNT_TYPE_LABELS = {
    "boutique_ip": "精品 IP",
    "original": "原创",
    "mixed_edit": "混剪",
    "unknown": "未知",
}
_CONTENT_DIRECTION_LABELS = {
    "new_car": "新车",
    "used_car": "二手车",
    "media": "媒体",
    "other": "其他",
    "unknown": "未知",
}
_ACCOUNT_REAL_NAME_LABELS = {"yes": "是", "no": "否", "unknown": "未知"}
# 账号表格导出的平台列顺序 = 账号页表格的平台列顺序（format.ts platformKeys）。
_ACCOUNT_EXPORT_PLATFORMS = ("douyin", "xiaohongshu", "wechat_channels", "kuaishou")
# 分组表头样式索引（_styles_xml cellXfs 13-18），配色取自账号页表格分组行。
_ACCOUNT_GROUP_BASE_STYLE = 13
_ACCOUNT_GROUP_PLATFORM_STYLES = {
    "douyin": 14,
    "xiaohongshu": 15,
    "wechat_channels": 16,
    "kuaishou": 17,
}
_ACCOUNT_GROUP_MANAGEMENT_STYLE = 18

_CHANNEL_HEADERS = {
    "platform": "平台内部标识",
    "platform_label": "平台",
    "scope": "统计范围内部标识",
    "scope_label": "统计范围",
    "publication_count": "发布内容数",
    "metric": "指标内部标识",
    "metric_label": "指标",
    "kind": "计算方式",
    "status": "结果状态",
    "value": "数值",
    "numerator": "符合条件数量",
    "denominator": "统计总数",
    "percentage": "比例",
    "eligible_count": "可计算内容数",
    "coverage_percentage": "覆盖率",
    "reason": "说明",
    "identity_coverage_percentage": "身份覆盖率",
    "candidate_user_count": "待判断用户数",
    "classified_user_count": "已分类用户数",
    "classification_coverage_percentage": "分类覆盖率",
    "comment_collection_coverage_percentage": "评论采集覆盖率",
    "captured_comment_count": "已采集评论数",
    "declared_comment_count": "平台显示的评论数",
    "capped_content_count": "评论只采集到上限的内容数",
    "audience_definition_version": "人群定义版本",
    "classifier_version": "用户分类规则版本",
    "user_key_version": "用户去重规则版本",
    "evidence_window_start": "评论统计开始时间（北京时间）",
    "evidence_window_end": "评论统计结束时间（北京时间）",
    "report_cutoff_at": "数据采集截止时间（北京时间）",
    "warm_up": "是否处于试运行期",
}

_CHANNEL_VALUE_LABELS = {
    "kind": {"quantity": "数量", "ratio": "占比", "score": "评分"},
    "status": {
        "available": "可用",
        "below_threshold": "暂不显示",
        "sample_only": "仅供参考",
        "not_applicable": "无相关内容",
        "not_calculable": "暂时无法计算",
        "missing": "暂无数据",
        "stale": "需要更新",
    },
    "scope": {"summary": "全部", "used_car": "二手车", "new_car": "新车", "media": "媒体"},
}

_CHANNEL_TECHNICAL_HEADERS = frozenset(
    {
        "platform",
        "scope",
        "metric",
        "audience_definition_version",
        "classifier_version",
        "user_key_version",
    }
)

_INTEGER_HEADERS = {
    "selling_point_score",
    "content_automotive_score",
    "view_count",
    "comment_count",
    "publication_count",
    "numerator",
    "denominator",
    "eligible_count",
    "candidate_user_count",
    "classified_user_count",
    "captured_comment_count",
    "declared_comment_count",
    "capped_content_count",
}
_DECIMAL_HEADERS = {"value", "duplicate_confidence"}
_PERCENT_HEADERS = {
    "percentage",
    "coverage_percentage",
    "identity_coverage_percentage",
    "classification_coverage_percentage",
    "comment_collection_coverage_percentage",
}
_BOOLEAN_HEADERS = {"evaluation_current", "warm_up"}
_DATETIME_HEADERS = {
    "published_at",
    "published_at_beijing",
    "evidence_window_start",
    "evidence_window_end",
    "report_cutoff_at",
}

_COLUMN_WIDTHS = {
    "report_period": 28,
    "report_task": 34,
    "platform_content_id": 24,
    "link_id": 14,
    "platform": 14,
    "content_type": 13,
    "published_at_beijing": 22,
    "canonical_url": 44,
    "title": 52,
    "account_uid": 24,
    "account_name": 22,
    "account_type": 16,
    "content_direction": 16,
    "v3_status": 18,
    "primary_selling_point_code": 16,
    "primary_selling_point_label": 54,
    "selling_point_score": 13,
    "content_automotive_score": 13,
    "view_count": 15,
    "comment_count": 12,
    "reason": 56,
    "metric": 32,
    "metric_label": 28,
    "audience_definition_version": 30,
    "classifier_version": 28,
    "user_key_version": 28,
    "evidence_window_start": 22,
    "evidence_window_end": 22,
    "report_cutoff_at": 22,
}


@dataclass(frozen=True, slots=True)
class _Worksheet:
    name: str
    rows: Sequence[Sequence[Any]]
    styles: Sequence[Sequence[int]]
    widths: Sequence[float]
    merge_cells: Sequence[str] = ()
    auto_filter: str | None = None
    frozen_rows: int = 0
    frozen_columns: int = 0
    row_heights: Mapping[int, float] | None = None
    hidden_columns: Sequence[int] = ()


def _valid_xml_char(character: str) -> bool:
    value = ord(character)
    return (
        value in {0x9, 0xA, 0xD}
        or 0x20 <= value <= 0xD7FF
        or 0xE000 <= value <= 0xFFFD
        or 0x10000 <= value <= 0x10FFFF
    )


def _clean_text(value: Any) -> str:
    text = "".join(character for character in str(value) if _valid_xml_char(character))
    if len(text) > EXCEL_MAX_CELL_CHARS:
        return text[: EXCEL_MAX_CELL_CHARS - 1] + "…"
    return text


def _csv_rows(payload: bytes) -> tuple[list[str], list[list[str]]]:
    text = payload.decode("utf-8-sig")
    reader = csv.reader(io.StringIO(text, newline=""))
    rows = list(reader)
    if not rows:
        raise ValueError("report detail CSV is empty")
    headers = [_clean_text(value).strip() for value in rows[0]]
    if not headers or any(not header for header in headers):
        raise ValueError("report detail CSV has an empty header")
    if len(set(headers)) != len(headers):
        raise ValueError("report detail CSV has duplicate headers")
    body: list[list[str]] = []
    for raw in rows[1:]:
        if len(raw) > len(headers):
            raise ValueError("report detail CSV row is wider than its header")
        body.append([*raw, *([""] * (len(headers) - len(raw)))])
    if len(body) + 1 > EXCEL_MAX_ROWS:
        raise ValueError("report detail exceeds the Excel row limit")
    return headers, body


def _finite_number(value: str) -> float | None:
    try:
        parsed = float(value)
    except ValueError:
        return None
    return parsed if math.isfinite(parsed) else None


def _parse_datetime(value: str) -> datetime | None:
    try:
        parsed = datetime.fromisoformat(value.replace("Z", "+00:00"))
    except ValueError:
        return None
    if parsed.tzinfo is not None:
        return parsed.astimezone(timezone.utc).replace(tzinfo=None)
    return parsed


def _parse_beijing_datetime(value: str) -> datetime | None:
    try:
        parsed = datetime.fromisoformat(value.replace("Z", "+00:00"))
    except ValueError:
        return None
    if parsed.tzinfo is None:
        return parsed
    beijing = timezone(timedelta(hours=8))
    return parsed.astimezone(beijing).replace(tzinfo=None)


def _plain_channel_reason(value: str) -> str:
    text = _clean_text(value).strip()
    rules = (
        (r"正式评估覆盖率为 ([\d.]+)%，低于 ([\d.]+)% 发布阈值", r"卖点评估完成率为 \1%，低于至少 \2% 的要求"),
        (r"曝光量快照覆盖率为 ([\d.]+)%，低于 ([\d.]+)% 发布阈值", r"有曝光量的数据占 \1%，低于至少 \2% 的要求"),
        (r"评论量快照覆盖率为 ([\d.]+)%，低于 ([\d.]+)% 发布阈值", r"有评论数的数据占 \1%，低于至少 \2% 的要求"),
        (r"固定采集截止点前 (\d+) 小时内的新鲜指标覆盖率为 ([\d.]+)%，低于 ([\d.]+)% 发布门槛", r"截止统计前 \1 小时内有更新的数据占 \2%，低于至少 \3% 的要求"),
        (r"报告窗口发现覆盖率为 ([\d.]+)%，低于 ([\d.]+)% 发布门槛", r"计划采集完成率为 \1%，低于至少 \2% 的要求"),
        (r"感知指纹覆盖率为 ([\d.]+)%，低于 ([\d.]+)% 发布阈值", r"完成重复内容识别的数据占 \1%，低于至少 \2% 的要求"),
        (r"用户身份覆盖率 ([\d.]+)%，低于 ([\d.]+)% 门槛", r"可识别评论用户的比例为 \1%，低于至少 \2% 的要求"),
        (r"用户分类覆盖率 ([\d.]+)%，低于 ([\d.]+)% 门槛", r"用户分类完成率为 \1%，低于至少 \2% 的要求"),
        (r"去重有效用户 (\d+) 人，低于 (\d+) 人门槛", r"去重后有 \1 位互动用户，少于至少 \2 位的要求"),
        (r"分类器未经金标核对，数值仅供参考", "用户分类规则还没完成人工检查，结果仅供参考"),
        (r"重复内容感知指纹尚未完成定标，重复率暂不可计算", "重复内容识别规则还没完成校验，暂时无法计算重复率"),
    )
    for pattern, replacement in rules:
        text = re.sub(pattern, replacement, text)
    replacements = (
        ("报告窗口", "报告期内"),
        ("发现覆盖", "计划采集完成率"),
        ("详情覆盖", "内容详情完整率"),
        ("指标新鲜度", "播放和互动数据更新率"),
        ("正式评估覆盖", "卖点评估完成率"),
        ("媒体处理终态覆盖", "视频和图片处理完成率"),
        ("重复指纹覆盖", "重复内容识别完成率"),
        ("周评论证据覆盖", "评论采集完成率"),
        ("统计口径", "计算方式"),
        ("分母", "统计总数"),
        ("发布阈值", "显示要求"),
        ("发布门槛", "显示要求"),
    )
    for old, new in replacements:
        text = text.replace(old, new)
    if re.search(r"[A-Za-z_]{3,}|定标|感知指纹|分类器|用户聚合|终态|快照", text):
        return "数据还不够完整，暂时无法显示结果。"
    return text


def _typed_value(header: str, value: str) -> Any:
    stripped = value.strip()
    if not stripped:
        return None
    if header == "reason":
        return _plain_channel_reason(stripped)
    labels = _CHANNEL_VALUE_LABELS.get(header)
    if labels is not None:
        return labels.get(stripped, _clean_text(value))
    if header == "warm_up":
        return "是" if stripped.lower() in {"true", "1", "yes"} else "否"
    if header in _BOOLEAN_HEADERS:
        lowered = stripped.lower()
        if lowered in {"true", "1", "yes"}:
            return True
        if lowered in {"false", "0", "no"}:
            return False
        return _clean_text(value)
    if header in _INTEGER_HEADERS:
        try:
            return int(stripped)
        except ValueError:
            return _clean_text(value)
    if header in _PERCENT_HEADERS:
        number = _finite_number(stripped)
        return number / 100 if number is not None else _clean_text(value)
    if header in _DECIMAL_HEADERS:
        number = _finite_number(stripped)
        return number if number is not None else _clean_text(value)
    if header in _DATETIME_HEADERS:
        parsed = (
            _parse_beijing_datetime(stripped)
            if header in {"published_at_beijing", "evidence_window_start", "evidence_window_end", "report_cutoff_at"}
            else _parse_datetime(stripped)
        )
        return parsed if parsed is not None else _clean_text(value)
    return _clean_text(value)


def _style_for(header: str, value: Any) -> int:
    if isinstance(value, bool):
        return 9
    if isinstance(value, datetime):
        return 8
    if header in _PERCENT_HEADERS and isinstance(value, (int, float)):
        return 7
    if isinstance(value, int):
        return 5
    if isinstance(value, float):
        return 6
    if isinstance(value, str):
        return 12
    return 4


def _display_width(value: str) -> int:
    return sum(
        2 if unicodedata.east_asian_width(character) in {"W", "F"} else 1
        for character in value
    )


def _widths(headers: Sequence[str], labels: Mapping[str, str]) -> list[float]:
    values: list[float] = []
    for header in headers:
        if header in _COLUMN_WIDTHS:
            values.append(float(_COLUMN_WIDTHS[header]))
            continue
        label_width = _display_width(labels.get(header, header)) + 3
        values.append(float(max(12, min(28, label_width))))
    return values


def _table_sheet(
    name: str,
    payload: bytes,
    labels: Mapping[str, str],
    *,
    hidden_headers: frozenset[str] = frozenset(),
) -> tuple[_Worksheet, int]:
    headers, raw_rows = _csv_rows(payload)
    translated = [labels.get(header, header) for header in headers]
    rows: list[list[Any]] = [translated]
    styles: list[list[int]] = [[1] * len(headers)]
    for raw_row in raw_rows:
        typed = [_typed_value(header, value) for header, value in zip(headers, raw_row)]
        rows.append(typed)
        styles.append([_style_for(header, value) for header, value in zip(headers, typed)])
    last_column = _column_name(len(headers))
    return (
        _Worksheet(
            name=name,
            rows=rows,
            styles=styles,
            widths=_widths(headers, labels),
            auto_filter=f"A1:{last_column}{len(rows)}",
            frozen_rows=1,
            row_heights={1: 28},
            hidden_columns=tuple(
                index
                for index, header in enumerate(headers, start=1)
                if header in hidden_headers
            ),
        ),
        len(raw_rows),
    )


def _source_value(
    row: Mapping[str, str], enrichment: Mapping[str, Any], key: str
) -> str:
    value = row.get(key)
    if value is None or not value.strip():
        value = enrichment.get(key)
    return "" if value is None else _clean_text(value)


def platform_content_id_from_url(platform: str, canonical_url: str) -> str:
    try:
        path = [unquote(value) for value in urlsplit(canonical_url).path.split("/") if value]
    except ValueError:
        return ""
    marker_paths = {
        "douyin": (("video",),),
        "xiaohongshu": (("explore",), ("discovery", "item")),
        "kuaishou": (("short-video",), ("photo",)),
        "wechat_channels": (("video",),),
    }
    for markers in marker_paths.get(platform, ()):
        marker_count = len(markers)
        for index in range(len(path) - marker_count):
            if tuple(path[index : index + marker_count]) == markers:
                return path[index + marker_count]
    return ""


def _v3_status(evidence_level: str, content_type: str) -> str:
    level = evidence_level.strip().upper()
    if level == "V3":
        return "资料完整（V3）"
    if level == "V2":
        return "资料较完整（V2，图文内容）" if content_type == "image" else "资料较完整（V2）"
    if level:
        return f"资料不足（{level}）"
    return "还没有评估"


def _content_sheet(
    *,
    task: Mapping[str, Any],
    payload: bytes,
    content_enrichment: Mapping[str, Mapping[str, Any]],
    selling_point_labels: Mapping[str, str],
) -> tuple[_Worksheet, int]:
    headers, raw_rows = _csv_rows(payload)
    missing_headers = sorted(_CONTENT_REQUIRED_HEADERS - set(headers))
    if missing_headers:
        raise ValueError(
            "content detail CSV is missing required headers: "
            + ", ".join(missing_headers)
        )
    period = (
        f"{_clean_text(task.get('period_start') or '')} 至 "
        f"{_clean_text(task.get('period_end') or '')}"
    )
    task_reference = _clean_text(task.get("id") or task.get("name") or "")
    output_headers = [label for _, label in _CONTENT_EXPORT_COLUMNS]
    rows: list[list[Any]] = [output_headers]
    styles: list[list[int]] = [[1] * len(output_headers)]
    for raw_row in raw_rows:
        source = dict(zip(headers, raw_row))
        internal_content_id = source.get("content_id", "").strip()
        enrichment = content_enrichment.get(internal_content_id, {})
        platform = _source_value(source, enrichment, "platform").strip()
        canonical_url = _source_value(source, enrichment, "canonical_url").strip()
        enriched_platform_id = _source_value(
            source, enrichment, "platform_content_id"
        ).strip()
        url_platform_id = platform_content_id_from_url(platform, canonical_url)
        if (
            enriched_platform_id
            and url_platform_id
            and enriched_platform_id != url_platform_id
        ):
            raise ValueError(
                f"platform content ID does not match canonical URL for content {internal_content_id}"
            )
        platform_content_id = url_platform_id or enriched_platform_id
        if not platform_content_id:
            raise ValueError(
                f"platform content ID is unavailable for content {internal_content_id}"
            )
        content_type = _source_value(source, enrichment, "content_type").strip()
        if not content_type:
            raise ValueError(
                f"content type is unavailable for content {internal_content_id}"
            )
        evidence_level = source.get("evidence_level", "")
        selling_point_code = source.get("primary_selling_point_code", "").strip()
        if selling_point_code:
            selling_point_label = _source_value(
                source, enrichment, "primary_selling_point_label"
            ).strip() or _clean_text(selling_point_labels.get(selling_point_code) or "")
            if not selling_point_label:
                raise ValueError(
                    f"selling point label is unavailable for code {selling_point_code}"
                )
            selling_point_code_value = selling_point_code
        else:
            selling_point_code_value = _MISSING_SELLING_POINT
            selling_point_label = _MISSING_SELLING_POINT
        values: list[Any] = [
            period,
            task_reference,
            platform_content_id,
            source.get("link_id", ""),
            _PLATFORM_LABELS.get(platform, platform),
            _CONTENT_TYPE_LABELS.get(content_type, content_type),
            _typed_value("published_at_beijing", source.get("published_at", "")),
            canonical_url,
            source.get("title", ""),
            source.get("account_uid", ""),
            source.get("account_name", ""),
            _ACCOUNT_TYPE_LABELS.get(
                source.get("account_type", ""), source.get("account_type", "")
            ),
            _CONTENT_DIRECTION_LABELS.get(
                source.get("content_direction", ""),
                source.get("content_direction", ""),
            ),
            _v3_status(evidence_level, content_type),
            selling_point_code_value,
            selling_point_label,
            _typed_value("selling_point_score", source.get("selling_point_score", "")),
            _typed_value(
                "content_automotive_score",
                source.get("content_automotive_score", ""),
            ),
            _typed_value("view_count", source.get("view_count", "")),
            _typed_value("comment_count", source.get("comment_count", "")),
        ]
        style_headers = [key for key, _ in _CONTENT_EXPORT_COLUMNS]
        rows.append(values)
        styles.append(
            [_style_for(header, value) for header, value in zip(style_headers, values)]
        )
    last_column = _column_name(len(output_headers))
    return (
        _Worksheet(
            name="内容明细",
            rows=rows,
            styles=styles,
            widths=[
                float(_COLUMN_WIDTHS.get(key, max(12, _display_width(label) + 3)))
                for key, label in _CONTENT_EXPORT_COLUMNS
            ],
            auto_filter=f"A1:{last_column}{len(rows)}",
            frozen_rows=1,
            row_heights={1: 36},
        ),
        len(raw_rows),
    )


def _summary_sheet(
    *, task: Mapping[str, Any], revision: int, content_count: int, channel_count: int
) -> _Worksheet:
    task_status_labels = {
        "succeeded": "已完成",
        "partial": "部分完成",
        "failed": "失败",
        "cancelled": "已取消",
    }
    raw_task_status = _clean_text(task.get("task_status") or "")
    rows: list[list[Any]] = [
        ["DCar 内容运营报告数据明细", None, None, None],
        [None, None, None, None],
        ["任务名称", _clean_text(task.get("name") or ""), None, None],
        ["任务编号", _clean_text(task.get("id") or ""), None, None],
        [
            "报告周期",
            f"{_clean_text(task.get('period_start') or '')} 至 {_clean_text(task.get('period_end') or '')}",
            None,
            None,
        ],
        ["报告版本", f"第 {revision} 版", None, None],
        ["任务状态", task_status_labels.get(raw_task_status, raw_task_status), None, None],
        ["导出内容", "图片报告 + Excel 数据明细", None, None],
        [
            "数据说明",
            "数据来自所选报告版本；旧报告缺少的作品编号和内容类型会从当前内容资料中补全",
            None,
            None,
        ],
        [None, None, None, None],
        ["工作表", "数据行数", None, None],
        ["内容明细", content_count, None, None],
        ["渠道结论", channel_count, None, None],
    ]
    styles: list[list[int]] = [
        [2, 0, 0, 0],
        [0, 0, 0, 0],
        [3, 10, 0, 0],
        [3, 10, 0, 0],
        [3, 10, 0, 0],
        [3, 10, 0, 0],
        [3, 10, 0, 0],
        [3, 10, 0, 0],
        [3, 11, 0, 0],
        [0, 0, 0, 0],
        [1, 1, 0, 0],
        [4, 5, 0, 0],
        [4, 5, 0, 0],
    ]
    return _Worksheet(
        name="报告说明",
        rows=rows,
        styles=styles,
        widths=[18, 34, 18, 18],
        merge_cells=(
            "A1:D1",
            "B3:D3",
            "B4:D4",
            "B5:D5",
            "B6:D6",
            "B7:D7",
            "B8:D8",
            "B9:D9",
        ),
        row_heights={1: 36, 9: 32},
    )


def build_report_detail_workbook(
    *,
    task: Mapping[str, Any],
    revision: int,
    content_csv: bytes,
    channel_csv: bytes | None,
    content_enrichment: Mapping[str, Mapping[str, Any]] | None = None,
    selling_point_labels: Mapping[str, str] | None = None,
) -> bytes:
    """Build a typed, styled XLSX from one immutable report revision."""

    content_sheet, content_count = _content_sheet(
        task=task,
        payload=content_csv,
        content_enrichment=content_enrichment or {},
        selling_point_labels=selling_point_labels or {},
    )
    channel_sheet: _Worksheet | None = None
    channel_count = 0
    if channel_csv is not None:
        channel_sheet, channel_count = _table_sheet(
            "渠道结论",
            channel_csv,
            _CHANNEL_HEADERS,
            hidden_headers=_CHANNEL_TECHNICAL_HEADERS,
        )
    sheets = [
        _summary_sheet(
            task=task,
            revision=revision,
            content_count=content_count,
            channel_count=channel_count,
        ),
        content_sheet,
    ]
    if channel_sheet is not None:
        sheets.append(channel_sheet)
    return _xlsx_bytes(
        sheets,
        title=f"{task.get('name') or task.get('id') or 'DCar 报告'} 数据明细",
        created_at=str(task.get("revision_created_at") or task.get("created_at") or ""),
    )


def _safe_filename_stem(value: Any, *, fallback: str) -> str:
    stem = unicodedata.normalize("NFKC", _clean_text(value or ""))
    stem = _INVALID_FILENAME_CHARS.sub("-", stem)
    stem = re.sub(r"\s+", " ", stem).strip(" .")
    if stem.casefold().endswith(".zip"):
        stem = stem[:-4].rstrip(" .")
    if not stem:
        stem = _INVALID_FILENAME_CHARS.sub("-", fallback).strip(" .-")
    if stem.partition(".")[0].upper() in _WINDOWS_RESERVED_FILENAMES:
        stem = f"DCar {stem}"
    # Common desktop filesystems cap a filename component at 255 UTF-8 bytes.
    stem = stem.encode("utf-8")[:240].decode("utf-8", errors="ignore").rstrip(" .")
    return stem or "DCar 报告"


def report_bundle_filename(*, task_name: str, task_id: str) -> str:
    """Use the visible task name for the browser download filename."""

    return f"{_safe_filename_stem(task_name, fallback=task_id)}.zip"


def accounts_workbook_filename(exported_at: datetime) -> str:
    """账号表格下载文件名：内容 + 北京时间导出时刻，多次导出可区分版本。"""

    return f"账号表格_{exported_at:%Y%m%d_%H%M}.xlsx"


def _enum_or_dash(labels: Mapping[str, str], value: Any) -> str:
    """与前端 label() 同口径：空值显示「—」，词典没有的枚举原样展示。"""

    text = _clean_text(value or "").strip()
    if not text:
        return "—"
    return labels.get(text, text)


def build_accounts_workbook(
    accounts: Sequence[Mapping[str, Any]],
    *,
    douyin_authorization_targets: Mapping[tuple[int, str], str] | None,
    exported_at: str,
) -> bytes:
    """账号表格（xlsx）：列结构、中文表头与取值口径都对齐账号页表格。

    - 表头两行：第一行是「账号基础信息 / 各平台 / 账号管理」分组（沿用页面
      分组配色），第二行是与页面一致的中文列名；冻结前 4 列与表头，列名行
      带筛选。
    - 手机号、平台账号编号写成文本单元格，避免 Excel 把长编号变成科学计数法
      （这是旧 CSV 导出的主要痛点）。
    - 「抖音开平授权」状态在抖音控制面，不在账号主数据库；由前端随下载请求
      带来精确 (account_id, platform_uid) 目标及状态。None 表示状态不可用，
      空集或未精确命中显示「未授权」；命中后显示「已授权」或「需重新授权」。
    - 粉丝量暂未采集（接口恒为 null），与页面一样显示「—」；关联内容量未绑定
      平台时与页面一样按 0 条展示。
    """

    group_row: list[Any] = ["账号基础信息", None, None, None]
    group_styles: list[int] = [_ACCOUNT_GROUP_BASE_STYLE] * 4
    headers: list[Any] = ["手机号", "运营人员", "账号类型", "内容方向"]
    widths: list[float] = [15, 12, 11, 11]
    merges: list[str] = ["A1:D1"]
    column = 5
    for platform in _ACCOUNT_EXPORT_PLATFORMS:
        span = 6 if platform == "douyin" else 5
        group_row.extend([_PLATFORM_LABELS[platform], *([None] * (span - 1))])
        group_styles.extend([_ACCOUNT_GROUP_PLATFORM_STYLES[platform]] * span)
        headers.append("平台账号编号")
        widths.append(26)
        headers.append("是否实名")
        widths.append(10)
        if platform == "douyin":
            headers.append("抖音开平授权")
            widths.append(14)
        headers.append("昵称")
        widths.append(20)
        headers.append("粉丝量")
        widths.append(11)
        headers.append("关联内容量")
        widths.append(12)
        merges.append(f"{_column_name(column)}1:{_column_name(column + span - 1)}1")
        column += span
    group_row.append("账号管理")
    group_styles.append(_ACCOUNT_GROUP_MANAGEMENT_STYLE)
    headers.append("状态")
    widths.append(9)

    rows: list[list[Any]] = [group_row, headers]
    styles: list[list[int]] = [group_styles, [1] * len(headers)]
    for account in accounts:
        identities = {
            str(identity.get("platform") or ""): identity
            for identity in account.get("platforms", ())
        }
        row: list[Any] = []
        row_styles: list[int] = []

        def put(value: Any, style: int) -> None:
            row.append(value)
            row_styles.append(style)

        put(_clean_text(account.get("phone") or ""), 12)
        put(_clean_text(account.get("operator_name") or "").strip() or "未填写", 10)
        put(_enum_or_dash(_ACCOUNT_TYPE_LABELS, account.get("account_type")), 10)
        put(
            _enum_or_dash(_CONTENT_DIRECTION_LABELS, account.get("content_direction")),
            10,
        )
        for platform in _ACCOUNT_EXPORT_PLATFORMS:
            identity = identities.get(platform)
            uid = _clean_text((identity or {}).get("uid") or "").strip()
            if uid:
                put(uid, 12)
            else:
                put("—", 10)
            if identity is None:
                put("未绑定", 10)
            else:
                put(
                    _enum_or_dash(
                        _ACCOUNT_REAL_NAME_LABELS, identity.get("real_name_status")
                    ),
                    10,
                )
            if platform == "douyin":
                if douyin_authorization_targets is None:
                    put("状态异常", 10)
                else:
                    authorization_state = douyin_authorization_targets.get(
                        (int(account.get("id") or 0), uid)
                    )
                    put(
                        "已授权"
                        if authorization_state == "authorized"
                        else "需重新授权"
                        if authorization_state == "needs_reauthorization"
                        else "未授权",
                        10,
                    )
            nickname = _clean_text((identity or {}).get("nickname") or "").strip()
            put(nickname or "—", 10)
            follower_count = (identity or {}).get("follower_count")
            if follower_count is None:
                put("—", 9)
            else:
                put(int(follower_count), 5)
            content_count = int((identity or {}).get("content_count") or 0)
            put(content_count, 19)
        put("运营中" if account.get("enabled") else "停用", 10)
        rows.append(row)
        styles.append(row_styles)

    last_column = _column_name(len(headers))
    sheet = _Worksheet(
        name="账号信息",
        rows=rows,
        styles=styles,
        widths=widths,
        merge_cells=tuple(merges),
        auto_filter=f"A2:{last_column}{len(rows)}",
        frozen_rows=2,
        frozen_columns=4,
        row_heights={1: 24, 2: 28},
    )
    return _xlsx_bytes([sheet], title="DCar 账号表格", created_at=exported_at)


def build_report_download_bundle(
    *, image_extension: str, image_bytes: bytes, workbook_bytes: bytes
) -> bytes:
    extension = image_extension.lower().lstrip(".")
    if extension not in {"png", "svg"}:
        raise ValueError("report image must be PNG or SVG")
    entries = {
        f"01_图片报告.{extension}": image_bytes,
        "02_数据明细.xlsx": workbook_bytes,
    }
    return _zip_entries(entries)


def _column_name(index: int) -> str:
    if index < 1:
        raise ValueError("Excel columns are one-based")
    name = ""
    value = index
    while value:
        value, remainder = divmod(value - 1, 26)
        name = chr(65 + remainder) + name
    return name


def _excel_serial(value: datetime) -> float:
    epoch = datetime(1899, 12, 30)
    return (value - epoch).total_seconds() / 86_400


def _number_text(value: int | float) -> str:
    if isinstance(value, int):
        return str(value)
    return format(value, ".15g")


def _cell_xml(reference: str, value: Any, style: int) -> str:
    style_attr = f' s="{style}"' if style else ""
    if value is None:
        return f'<c r="{reference}"{style_attr}/>'
    if isinstance(value, bool):
        return f'<c r="{reference}"{style_attr} t="b"><v>{1 if value else 0}</v></c>'
    if isinstance(value, datetime):
        return f'<c r="{reference}"{style_attr}><v>{_number_text(_excel_serial(value))}</v></c>'
    if isinstance(value, (int, float)) and not isinstance(value, bool):
        return f'<c r="{reference}"{style_attr}><v>{_number_text(value)}</v></c>'
    text = escape(_clean_text(value))
    return (
        f'<c r="{reference}"{style_attr} t="inlineStr">'
        f'<is><t xml:space="preserve">{text}</t></is></c>'
    )


def _sheet_xml(sheet: _Worksheet) -> str:
    column_count = max((len(row) for row in sheet.rows), default=1)
    row_count = max(len(sheet.rows), 1)
    dimension = f"A1:{_column_name(column_count)}{row_count}"
    pane = ""
    if sheet.frozen_rows or sheet.frozen_columns:
        top_left = f"{_column_name(sheet.frozen_columns + 1)}{sheet.frozen_rows + 1}"
        if sheet.frozen_rows and sheet.frozen_columns:
            active_pane = "bottomRight"
        elif sheet.frozen_rows:
            active_pane = "bottomLeft"
        else:
            active_pane = "topRight"
        splits = (
            f' xSplit="{sheet.frozen_columns}"' if sheet.frozen_columns else ""
        ) + (f' ySplit="{sheet.frozen_rows}"' if sheet.frozen_rows else "")
        pane = (
            f'<pane{splits} topLeftCell="{top_left}" '
            f'activePane="{active_pane}" state="frozen"/>'
            f'<selection pane="{active_pane}" activeCell="{top_left}" sqref="{top_left}"/>'
        )
    hidden_columns = set(sheet.hidden_columns)
    columns = "".join(
        f'<col min="{index}" max="{index}" width="{width}" customWidth="1"'
        f'{" hidden=\"1\"" if index in hidden_columns else ""}/>'
        for index, width in enumerate(sheet.widths, start=1)
    )
    row_xml: list[str] = []
    heights = sheet.row_heights or {}
    for row_index, row in enumerate(sheet.rows, start=1):
        height = heights.get(row_index)
        height_attr = f' ht="{height}" customHeight="1"' if height else ""
        style_row = sheet.styles[row_index - 1]
        cells = "".join(
            _cell_xml(
                f"{_column_name(column_index)}{row_index}",
                value,
                style_row[column_index - 1] if column_index - 1 < len(style_row) else 0,
            )
            for column_index, value in enumerate(row, start=1)
        )
        row_xml.append(f'<row r="{row_index}"{height_attr}>{cells}</row>')
    merges = ""
    if sheet.merge_cells:
        merge_values = "".join(
            f"<mergeCell ref={quoteattr(reference)}/>" for reference in sheet.merge_cells
        )
        merges = f'<mergeCells count="{len(sheet.merge_cells)}">{merge_values}</mergeCells>'
    auto_filter = (
        f"<autoFilter ref={quoteattr(sheet.auto_filter)}/>"
        if sheet.auto_filter
        else ""
    )
    return f'''<?xml version="1.0" encoding="UTF-8" standalone="yes"?>
<worksheet xmlns="http://schemas.openxmlformats.org/spreadsheetml/2006/main">
<dimension ref="{dimension}"/><sheetViews><sheetView workbookViewId="0" showGridLines="0">{pane}</sheetView></sheetViews>
<sheetFormatPr defaultRowHeight="18"/><cols>{columns}</cols><sheetData>{"".join(row_xml)}</sheetData>{merges}{auto_filter}
<pageMargins left="0.3" right="0.3" top="0.5" bottom="0.5" header="0.2" footer="0.2"/>
</worksheet>'''


def _styles_xml() -> str:
    return '''<?xml version="1.0" encoding="UTF-8" standalone="yes"?>
<styleSheet xmlns="http://schemas.openxmlformats.org/spreadsheetml/2006/main">
<numFmts count="2"><numFmt numFmtId="164" formatCode="yyyy-mm-dd hh:mm:ss"/><numFmt numFmtId="165" formatCode="#,##0&quot; 条&quot;"/></numFmts>
<fonts count="11">
<font><sz val="10"/><name val="Arial"/><family val="2"/></font>
<font><b/><color rgb="FFFFFFFF"/><sz val="10"/><name val="Arial"/><family val="2"/></font>
<font><b/><color rgb="FF1F2129"/><sz val="18"/><name val="Arial"/><family val="2"/></font>
<font><b/><color rgb="FF13262D"/><sz val="10"/><name val="Arial"/><family val="2"/></font>
<font><color rgb="FF0D6F65"/><u/><sz val="10"/><name val="Arial"/><family val="2"/></font>
<font><b/><color rgb="FF102C35"/><sz val="10"/><name val="Arial"/><family val="2"/></font>
<font><b/><color rgb="FFCF5336"/><sz val="10"/><name val="Arial"/><family val="2"/></font>
<font><b/><color rgb="FFC93453"/><sz val="10"/><name val="Arial"/><family val="2"/></font>
<font><b/><color rgb="FF247862"/><sz val="10"/><name val="Arial"/><family val="2"/></font>
<font><b/><color rgb="FFBD5630"/><sz val="10"/><name val="Arial"/><family val="2"/></font>
<font><b/><color rgb="FF536970"/><sz val="10"/><name val="Arial"/><family val="2"/></font>
</fonts>
<fills count="11">
<fill><patternFill patternType="none"/></fill><fill><patternFill patternType="gray125"/></fill>
<fill><patternFill patternType="solid"><fgColor rgb="FF102C35"/><bgColor indexed="64"/></patternFill></fill>
<fill><patternFill patternType="solid"><fgColor rgb="FFFFCD32"/><bgColor indexed="64"/></patternFill></fill>
<fill><patternFill patternType="solid"><fgColor rgb="FFF2F6F6"/><bgColor indexed="64"/></patternFill></fill>
<fill><patternFill patternType="solid"><fgColor rgb="FFEEF3F4"/><bgColor indexed="64"/></patternFill></fill>
<fill><patternFill patternType="solid"><fgColor rgb="FFFEF4EE"/><bgColor indexed="64"/></patternFill></fill>
<fill><patternFill patternType="solid"><fgColor rgb="FFFFF1F5"/><bgColor indexed="64"/></patternFill></fill>
<fill><patternFill patternType="solid"><fgColor rgb="FFEEF8F3"/><bgColor indexed="64"/></patternFill></fill>
<fill><patternFill patternType="solid"><fgColor rgb="FFFFF3ED"/><bgColor indexed="64"/></patternFill></fill>
<fill><patternFill patternType="solid"><fgColor rgb="FFF3F6F7"/><bgColor indexed="64"/></patternFill></fill>
</fills>
<borders count="2"><border><left/><right/><top/><bottom/><diagonal/></border><border><left/><right/><top/><bottom style="thin"><color rgb="FFE4E9EA"/></bottom><diagonal/></border></borders>
<cellStyleXfs count="1"><xf numFmtId="0" fontId="0" fillId="0" borderId="0"/></cellStyleXfs>
<cellXfs count="20">
<xf numFmtId="0" fontId="0" fillId="0" borderId="0" xfId="0"/>
<xf numFmtId="0" fontId="1" fillId="2" borderId="0" xfId="0" applyAlignment="1"><alignment horizontal="center" vertical="center" wrapText="1"/></xf>
<xf numFmtId="0" fontId="2" fillId="3" borderId="0" xfId="0" applyAlignment="1"><alignment horizontal="left" vertical="center"/></xf>
<xf numFmtId="0" fontId="3" fillId="4" borderId="1" xfId="0" applyAlignment="1"><alignment horizontal="left" vertical="center"/></xf>
<xf numFmtId="0" fontId="0" fillId="0" borderId="1" xfId="0" applyAlignment="1"><alignment horizontal="left" vertical="center"/></xf>
<xf numFmtId="3" fontId="0" fillId="0" borderId="1" xfId="0" applyNumberFormat="1" applyAlignment="1"><alignment horizontal="right" vertical="center"/></xf>
<xf numFmtId="4" fontId="0" fillId="0" borderId="1" xfId="0" applyNumberFormat="1" applyAlignment="1"><alignment horizontal="right" vertical="center"/></xf>
<xf numFmtId="10" fontId="0" fillId="0" borderId="1" xfId="0" applyNumberFormat="1" applyAlignment="1"><alignment horizontal="right" vertical="center"/></xf>
<xf numFmtId="164" fontId="0" fillId="0" borderId="1" xfId="0" applyNumberFormat="1" applyAlignment="1"><alignment horizontal="left" vertical="center"/></xf>
<xf numFmtId="0" fontId="0" fillId="0" borderId="1" xfId="0" applyAlignment="1"><alignment horizontal="center" vertical="center"/></xf>
<xf numFmtId="0" fontId="0" fillId="0" borderId="1" xfId="0" applyAlignment="1"><alignment horizontal="left" vertical="center"/></xf>
<xf numFmtId="0" fontId="0" fillId="0" borderId="1" xfId="0" applyAlignment="1"><alignment horizontal="left" vertical="center" wrapText="1"/></xf>
<xf numFmtId="49" fontId="0" fillId="0" borderId="1" xfId="0" quotePrefix="1" applyNumberFormat="1" applyAlignment="1"><alignment horizontal="left" vertical="center"/></xf>
<xf numFmtId="0" fontId="5" fillId="5" borderId="1" xfId="0" applyAlignment="1"><alignment horizontal="center" vertical="center"/></xf>
<xf numFmtId="0" fontId="6" fillId="6" borderId="1" xfId="0" applyAlignment="1"><alignment horizontal="center" vertical="center"/></xf>
<xf numFmtId="0" fontId="7" fillId="7" borderId="1" xfId="0" applyAlignment="1"><alignment horizontal="center" vertical="center"/></xf>
<xf numFmtId="0" fontId="8" fillId="8" borderId="1" xfId="0" applyAlignment="1"><alignment horizontal="center" vertical="center"/></xf>
<xf numFmtId="0" fontId="9" fillId="9" borderId="1" xfId="0" applyAlignment="1"><alignment horizontal="center" vertical="center"/></xf>
<xf numFmtId="0" fontId="10" fillId="10" borderId="1" xfId="0" applyAlignment="1"><alignment horizontal="center" vertical="center"/></xf>
<xf numFmtId="165" fontId="0" fillId="0" borderId="1" xfId="0" applyNumberFormat="1" applyAlignment="1"><alignment horizontal="right" vertical="center"/></xf>
</cellXfs>
<cellStyles count="1"><cellStyle name="Normal" xfId="0" builtinId="0"/></cellStyles>
<dxfs count="0"/><tableStyles count="0" defaultTableStyle="TableStyleMedium2" defaultPivotStyle="PivotStyleLight16"/>
</styleSheet>'''


def _core_timestamp(value: str) -> str:
    parsed = _parse_datetime(value) if value else None
    if parsed is None:
        parsed = datetime(2000, 1, 1)
    return parsed.replace(tzinfo=timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")


def _xlsx_bytes(
    sheets: Sequence[_Worksheet], *, title: str, created_at: str
) -> bytes:
    sheet_overrides = "".join(
        f'<Override PartName="/xl/worksheets/sheet{index}.xml" ContentType="application/vnd.openxmlformats-officedocument.spreadsheetml.worksheet+xml"/>'
        for index in range(1, len(sheets) + 1)
    )
    content_types = f'''<?xml version="1.0" encoding="UTF-8" standalone="yes"?>
<Types xmlns="http://schemas.openxmlformats.org/package/2006/content-types">
<Default Extension="rels" ContentType="application/vnd.openxmlformats-package.relationships+xml"/><Default Extension="xml" ContentType="application/xml"/>
<Override PartName="/xl/workbook.xml" ContentType="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet.main+xml"/>
<Override PartName="/xl/styles.xml" ContentType="application/vnd.openxmlformats-officedocument.spreadsheetml.styles+xml"/>{sheet_overrides}
<Override PartName="/docProps/core.xml" ContentType="application/vnd.openxmlformats-package.core-properties+xml"/><Override PartName="/docProps/app.xml" ContentType="application/vnd.openxmlformats-officedocument.extended-properties+xml"/>
</Types>'''
    root_rels = '''<?xml version="1.0" encoding="UTF-8" standalone="yes"?>
<Relationships xmlns="http://schemas.openxmlformats.org/package/2006/relationships">
<Relationship Id="rId1" Type="http://schemas.openxmlformats.org/officeDocument/2006/relationships/officeDocument" Target="xl/workbook.xml"/>
<Relationship Id="rId2" Type="http://schemas.openxmlformats.org/package/2006/relationships/metadata/core-properties" Target="docProps/core.xml"/>
<Relationship Id="rId3" Type="http://schemas.openxmlformats.org/officeDocument/2006/relationships/extended-properties" Target="docProps/app.xml"/>
</Relationships>'''
    workbook_sheets = "".join(
        f'<sheet name={quoteattr(sheet.name)} sheetId="{index}" r:id="rId{index}"/>'
        for index, sheet in enumerate(sheets, start=1)
    )
    workbook = f'''<?xml version="1.0" encoding="UTF-8" standalone="yes"?>
<workbook xmlns="http://schemas.openxmlformats.org/spreadsheetml/2006/main" xmlns:r="http://schemas.openxmlformats.org/officeDocument/2006/relationships">
<bookViews><workbookView xWindow="0" yWindow="0" windowWidth="24000" windowHeight="14000"/></bookViews><sheets>{workbook_sheets}</sheets><calcPr calcId="191029" fullCalcOnLoad="1"/>
</workbook>'''
    workbook_relationships = "".join(
        f'<Relationship Id="rId{index}" Type="http://schemas.openxmlformats.org/officeDocument/2006/relationships/worksheet" Target="worksheets/sheet{index}.xml"/>'
        for index in range(1, len(sheets) + 1)
    )
    workbook_rels = f'''<?xml version="1.0" encoding="UTF-8" standalone="yes"?>
<Relationships xmlns="http://schemas.openxmlformats.org/package/2006/relationships">{workbook_relationships}<Relationship Id="rId{len(sheets) + 1}" Type="http://schemas.openxmlformats.org/officeDocument/2006/relationships/styles" Target="styles.xml"/></Relationships>'''
    timestamp = _core_timestamp(created_at)
    core = f'''<?xml version="1.0" encoding="UTF-8" standalone="yes"?>
<cp:coreProperties xmlns:cp="http://schemas.openxmlformats.org/package/2006/metadata/core-properties" xmlns:dc="http://purl.org/dc/elements/1.1/" xmlns:dcterms="http://purl.org/dc/terms/" xmlns:dcmitype="http://purl.org/dc/dcmitype/" xmlns:xsi="http://www.w3.org/2001/XMLSchema-instance">
<dc:title>{escape(_clean_text(title))}</dc:title><dc:creator>DCar Sentinel</dc:creator><cp:lastModifiedBy>DCar Sentinel</cp:lastModifiedBy><dcterms:created xsi:type="dcterms:W3CDTF">{timestamp}</dcterms:created><dcterms:modified xsi:type="dcterms:W3CDTF">{timestamp}</dcterms:modified>
</cp:coreProperties>'''
    sheet_names = "".join(
        f'<vt:lpstr>{escape(sheet.name)}</vt:lpstr>' for sheet in sheets
    )
    app = f'''<?xml version="1.0" encoding="UTF-8" standalone="yes"?>
<Properties xmlns="http://schemas.openxmlformats.org/officeDocument/2006/extended-properties" xmlns:vt="http://schemas.openxmlformats.org/officeDocument/2006/docPropsVTypes"><Application>DCar Sentinel</Application><DocSecurity>0</DocSecurity><ScaleCrop>false</ScaleCrop><HeadingPairs><vt:vector size="2" baseType="variant"><vt:variant><vt:lpstr>工作表</vt:lpstr></vt:variant><vt:variant><vt:i4>{len(sheets)}</vt:i4></vt:variant></vt:vector></HeadingPairs><TitlesOfParts><vt:vector size="{len(sheets)}" baseType="lpstr">{sheet_names}</vt:vector></TitlesOfParts><Company>DCar</Company><LinksUpToDate>false</LinksUpToDate><SharedDoc>false</SharedDoc><HyperlinksChanged>false</HyperlinksChanged><AppVersion>1.0</AppVersion></Properties>'''
    entries: dict[str, str | bytes] = {
        "[Content_Types].xml": content_types,
        "_rels/.rels": root_rels,
        "docProps/core.xml": core,
        "docProps/app.xml": app,
        "xl/workbook.xml": workbook,
        "xl/_rels/workbook.xml.rels": workbook_rels,
        "xl/styles.xml": _styles_xml(),
    }
    for index, sheet in enumerate(sheets, start=1):
        entries[f"xl/worksheets/sheet{index}.xml"] = _sheet_xml(sheet)
    return _zip_entries(entries)


def _zip_entries(entries: Mapping[str, str | bytes]) -> bytes:
    output = io.BytesIO()
    with zipfile.ZipFile(output, "w", compression=zipfile.ZIP_DEFLATED) as archive:
        for name, payload in entries.items():
            info = zipfile.ZipInfo(name, date_time=(1980, 1, 1, 0, 0, 0))
            info.compress_type = zipfile.ZIP_DEFLATED
            info.external_attr = 0o600 << 16
            archive.writestr(
                info, payload.encode("utf-8") if isinstance(payload, str) else payload
            )
    return output.getvalue()
