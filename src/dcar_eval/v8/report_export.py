"""Read-only report download bundles and dependency-free XLSX rendering.

The report revision CSV files are immutable snapshots.  This module turns those
snapshots into a presentation-friendly workbook at download time without
changing the task, revision, or registered report files.
"""

from __future__ import annotations

import csv
import io
import math
import re
import unicodedata
import zipfile
from dataclasses import dataclass
from datetime import datetime, timezone
from typing import Any, Mapping, Sequence
from xml.sax.saxutils import escape, quoteattr


EXCEL_MAX_ROWS = 1_048_576
EXCEL_MAX_CELL_CHARS = 32_767
_INVALID_FILENAME_CHARS = re.compile(r'[\x00-\x1f\x7f<>:"/\\|?*]+')
_WINDOWS_RESERVED_FILENAMES = {
    "CON",
    "PRN",
    "AUX",
    "NUL",
    *(f"COM{index}" for index in range(1, 10)),
    *(f"LPT{index}" for index in range(1, 10)),
}

_CONTENT_HEADERS = {
    "content_id": "内容 ID",
    "link_id": "链接 ID",
    "platform": "平台代码",
    "published_at": "发布时间（UTC）",
    "canonical_url": "内容链接",
    "title": "标题",
    "account_uid": "账号 UID",
    "account_name": "账号名称",
    "account_type": "账号类型代码",
    "content_direction": "内容方向代码",
    "evidence_level": "证据等级",
    "primary_selling_point_code": "主卖点编码",
    "selling_point_score": "卖点评分",
    "content_automotive_score": "内容垂直度评分",
    "view_count": "播放量",
    "comment_count": "评论量",
    "duplicate_original_link_id": "重复原始链接 ID",
    "duplicate_method": "重复识别方法",
    "duplicate_confidence": "重复置信度",
    "evaluation_current": "是否当前正式评估",
}

_CHANNEL_HEADERS = {
    "platform": "平台代码",
    "platform_label": "平台",
    "scope": "范围代码",
    "scope_label": "范围",
    "publication_count": "发布内容数",
    "metric": "指标代码",
    "metric_label": "指标",
    "kind": "指标类型",
    "status": "发布状态",
    "value": "数值",
    "numerator": "分子",
    "denominator": "分母",
    "percentage": "比例",
    "eligible_count": "可计算内容数",
    "coverage_percentage": "覆盖率",
    "reason": "口径与说明",
    "identity_coverage_percentage": "身份覆盖率",
    "candidate_user_count": "候选用户数",
    "classified_user_count": "已分类用户数",
    "classification_coverage_percentage": "分类覆盖率",
    "comment_collection_coverage_percentage": "评论采集覆盖率",
    "captured_comment_count": "已采集评论数",
    "declared_comment_count": "声明评论数",
    "capped_content_count": "触达采集上限内容数",
    "audience_definition_version": "人群定义版本",
    "classifier_version": "分类器版本",
    "user_key_version": "用户键版本",
    "evidence_window_start": "证据窗口开始（UTC）",
    "evidence_window_end": "证据窗口结束（UTC）",
    "report_cutoff_at": "报告采集截止（UTC）",
    "warm_up": "是否预热期",
}

_INTEGER_HEADERS = {
    "content_id",
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
    "evidence_window_start",
    "evidence_window_end",
    "report_cutoff_at",
}

_COLUMN_WIDTHS = {
    "content_id": 12,
    "link_id": 16,
    "platform": 14,
    "published_at": 22,
    "canonical_url": 46,
    "title": 56,
    "account_uid": 22,
    "account_name": 24,
    "account_type": 18,
    "content_direction": 18,
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
    row_heights: Mapping[int, float] | None = None


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


def _typed_value(header: str, value: str) -> Any:
    stripped = value.strip()
    if not stripped:
        return None
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
        parsed = _parse_datetime(stripped)
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
        ),
        len(raw_rows),
    )


def _summary_sheet(
    *, task: Mapping[str, Any], revision: int, content_count: int, channel_count: int
) -> _Worksheet:
    rows: list[list[Any]] = [
        ["DCar 内容运营报告数据明细", None, None, None],
        [None, None, None, None],
        ["任务名称", _clean_text(task.get("name") or ""), None, None],
        ["任务 ID", _clean_text(task.get("id") or ""), None, None],
        [
            "报告周期",
            f"{_clean_text(task.get('period_start') or '')} 至 {_clean_text(task.get('period_end') or '')}",
            None,
            None,
        ],
        ["报告版本", f"第 {revision} 版", None, None],
        ["任务状态", _clean_text(task.get("task_status") or ""), None, None],
        ["导出内容", "图片报告 + Excel 数据明细", None, None],
        ["数据口径", "内容明细与渠道结论均来自该 revision 的不可变报告快照", None, None],
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
) -> bytes:
    """Build a typed, styled XLSX from one immutable report revision."""

    content_sheet, content_count = _table_sheet(
        "内容明细", content_csv, _CONTENT_HEADERS
    )
    channel_sheet: _Worksheet | None = None
    channel_count = 0
    if channel_csv is not None:
        channel_sheet, channel_count = _table_sheet(
            "渠道结论", channel_csv, _CHANNEL_HEADERS
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
    if sheet.frozen_rows:
        top_left = f"A{sheet.frozen_rows + 1}"
        pane = (
            f'<pane ySplit="{sheet.frozen_rows}" topLeftCell="{top_left}" '
            'activePane="bottomLeft" state="frozen"/>'
            f'<selection pane="bottomLeft" activeCell="{top_left}" sqref="{top_left}"/>'
        )
    columns = "".join(
        f'<col min="{index}" max="{index}" width="{width}" customWidth="1"/>'
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
<numFmts count="1"><numFmt numFmtId="164" formatCode="yyyy-mm-dd hh:mm:ss"/></numFmts>
<fonts count="5">
<font><sz val="10"/><name val="Arial"/><family val="2"/></font>
<font><b/><color rgb="FFFFFFFF"/><sz val="10"/><name val="Arial"/><family val="2"/></font>
<font><b/><color rgb="FF1F2129"/><sz val="18"/><name val="Arial"/><family val="2"/></font>
<font><b/><color rgb="FF13262D"/><sz val="10"/><name val="Arial"/><family val="2"/></font>
<font><color rgb="FF0D6F65"/><u/><sz val="10"/><name val="Arial"/><family val="2"/></font>
</fonts>
<fills count="5">
<fill><patternFill patternType="none"/></fill><fill><patternFill patternType="gray125"/></fill>
<fill><patternFill patternType="solid"><fgColor rgb="FF102C35"/><bgColor indexed="64"/></patternFill></fill>
<fill><patternFill patternType="solid"><fgColor rgb="FFFFCD32"/><bgColor indexed="64"/></patternFill></fill>
<fill><patternFill patternType="solid"><fgColor rgb="FFF2F6F6"/><bgColor indexed="64"/></patternFill></fill>
</fills>
<borders count="2"><border><left/><right/><top/><bottom/><diagonal/></border><border><left/><right/><top/><bottom style="thin"><color rgb="FFE4E9EA"/></bottom><diagonal/></border></borders>
<cellStyleXfs count="1"><xf numFmtId="0" fontId="0" fillId="0" borderId="0"/></cellStyleXfs>
<cellXfs count="13">
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
