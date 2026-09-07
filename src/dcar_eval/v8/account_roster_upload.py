"""Strict decoders for official roster attachments; acceptance lives in account_roster."""

from __future__ import annotations

import csv
import io
import json
import re
import zipfile
from datetime import datetime, timedelta, timezone
from pathlib import PurePosixPath
from typing import Any, Mapping
from xml.etree import ElementTree

from .account_roster import RosterError

MAX_SOURCE_BYTES = 20 * 1024 * 1024
MAX_UNCOMPRESSED_BYTES = 50 * 1024 * 1024
NS = {"s": "http://schemas.openxmlformats.org/spreadsheetml/2006/main"}
ALIASES = {
    "platform": ("platform", "平台", "平台类型", "platType", "所属平台"),
    "matrix_account_id": ("matrix_account_id", "matrixAccountId", "矩阵账号ID", "矩阵账号id", "矩阵账号编号", "账号唯一ID(矩阵通)"),
    "profile_ref": ("profile_ref", "profile_url", "主页链接", "账号链接", "主页地址", "账号主页", "账号主页链接"),
    "uid": ("uid", "UID", "平台UID", "平台 UID"),
    "nickname": ("nickname", "cNickname", "昵称", "账号名称", "账号"),
    "monitoring_status": ("monitoring_status", "监测状态", "矩阵监测", "是否监测"),
    "authorization_status": ("authorization_status", "授权状态", "矩阵授权", "是否授权"),
    "open_platform_authorization_status": ("开放平台授权状态",),
    "creator_publish_authorization_status": ("创作者&发布授权状态",),
    "leads_live_authorization_status": ("线索&直播授权状态",),
    "monitoring_started_at": ("monitoring_started_at", "监测开始时间", "开启监测时间"),
    "avatar_url": ("avatar_url", "cAvatar", "头像", "头像地址"),
    "unique_id": ("unique_id", "uniqueId", "短号", "平台短号", "账号唯一标识"),
}
FIELD_BY_HEADER = {alias: field for field, aliases in ALIASES.items() for alias in aliases}
PLATFORM_VALUES = {
    "douyin": "douyin", "抖音": "douyin", "2": "douyin",
    "xiaohongshu": "xiaohongshu", "小红书": "xiaohongshu", "6": "xiaohongshu",
    "kuaishou": "kuaishou", "快手": "kuaishou", "1": "kuaishou",
    "wechat_channels": "wechat_channels", "视频号": "wechat_channels", "4": "wechat_channels",
}
MONITORING_VALUES = {
    "monitored": "monitored", "已监测": "monitored", "监测中": "monitored", "是": "monitored",
    "not_monitored": "not_monitored", "未监测": "not_monitored", "否": "not_monitored",
    "unknown": "unknown", "未知": "unknown", "": "unknown",
}
AUTHORIZATION_VALUES = {
    "authorized": "authorized", "已授权": "authorized", "是": "authorized",
    "unauthorized": "unauthorized", "未授权": "unauthorized", "否": "unauthorized",
    "unknown": "unknown", "未知": "unknown", "": "unknown",
}
AUTHORIZATION_SCOPE_FIELDS = {
    "open_platform": "open_platform_authorization_status",
    "creator_publish": "creator_publish_authorization_status",
    "leads_live": "leads_live_authorization_status",
}


def _official_monitoring_time(value: Any) -> str | None:
    if value in (None, ""):
        return None
    if not isinstance(value, str) or not re.fullmatch(r"\d{4}-\d{2}-\d{2} \d{2}:\d{2}:\d{2}", value.strip()):
        raise RosterError("invalid_source_time", "Official monitoring time must be Beijing local YYYY-MM-DD HH:MM:SS")
    try:
        local_time = datetime.strptime(value.strip(), "%Y-%m-%d %H:%M:%S")
    except ValueError as exc:
        raise RosterError("invalid_source_time", "Official monitoring time is invalid") from exc
    return local_time.replace(tzinfo=timezone(timedelta(hours=8))).isoformat(timespec="seconds")


def _normalize_row(row: Mapping[str, Any]) -> dict[str, Any]:
    values: dict[str, Any] = {}
    for header, value in row.items():
        field = FIELD_BY_HEADER.get(str(header).strip())
        if field:
            if field in values:
                raise RosterError("duplicate_column", f"Duplicate roster column: {field}")
            values[field] = _official_monitoring_time(value) if str(header).strip() == "开启监测时间" else value
    for key in ("matrix_account_id", "profile_ref"):
        if not isinstance(values.get(key), str) or not values[key].strip():
            raise RosterError("missing_stable_key", f"Official export is missing {key}")
    uid = values.get("uid")
    if uid not in (None, "") and not isinstance(uid, str):
        raise RosterError("invalid_uid", "UID must be exported as text; numeric JSON IDs are not accepted")
    platform = PLATFORM_VALUES.get(str(values.get("platform") or "").strip())
    if not platform:
        raise RosterError("invalid_platform", "Official export contains an unsupported or missing platform")
    monitoring = MONITORING_VALUES.get(str(values.get("monitoring_status") or "").strip())
    authorization = AUTHORIZATION_VALUES.get(str(values.get("authorization_status") or "").strip())
    if monitoring is None or authorization is None:
        raise RosterError("invalid_account_state", "Unrecognized Matrix monitoring/authorization value")
    metadata = dict(row.get("metadata") or {}) if isinstance(row.get("metadata"), Mapping) else {}
    authorization_by_scope = {}
    for scope, field in AUTHORIZATION_SCOPE_FIELDS.items():
        if field in values:
            status = AUTHORIZATION_VALUES.get(str(values[field] or "").strip())
            if status is None:
                raise RosterError("invalid_account_state", "Unrecognized Matrix scoped authorization value")
            authorization_by_scope[scope] = status
    if authorization_by_scope:
        metadata["authorization_by_scope"] = authorization_by_scope
        # A grant in one scope says nothing about the other two scopes. Only
        # summarize a complete, unanimous set; missing or mixed scopes stay unknown.
        if "authorization_status" not in values and len(authorization_by_scope) == len(AUTHORIZATION_SCOPE_FIELDS):
            states = set(authorization_by_scope.values())
            if len(states) == 1:
                authorization = states.pop()
    for key in ("avatar_url", "unique_id"):
        if values.get(key) not in (None, ""):
            if not isinstance(values[key], str):
                raise RosterError("invalid_text_id", f"{key} must be text")
            metadata["display_account_id" if key == "unique_id" else key] = values[key].strip()
    return {
        "platform": platform, "matrix_account_id": values["matrix_account_id"].strip(),
        "profile_ref": values["profile_ref"].strip(), "uid": uid.strip() if uid else None,
        "nickname": str(values.get("nickname") or "").strip(),
        "monitoring_status": monitoring, "authorization_status": authorization,
        "monitoring_started_at": values.get("monitoring_started_at") or None,
        "metadata": metadata,
    }


def _rows_from_grid(grid: list[list[str]]) -> list[dict[str, Any]]:
    nonempty = [row for row in grid if any(value.strip() for value in row)]
    if not nonempty:
        return []
    headers = [value.strip() for value in nonempty[0]]
    if len(headers) != len(set(headers)):
        raise RosterError("duplicate_column", "Official export has duplicate column names")
    fields = {FIELD_BY_HEADER.get(header) for header in headers}
    if not {"platform", "matrix_account_id", "profile_ref"} <= fields:
        raise RosterError("unrecognized_export", "Official export must include platform, Matrix account ID and homepage columns")
    rows = []
    for values in nonempty[1:]:
        if len(values) > len(headers) and any(values[len(headers):]):
            raise RosterError("invalid_export_row", "Official export row exceeds its header")
        rows.append(_normalize_row(dict(zip(headers, values))))
    return rows


def _xlsx_rows(source: bytes, *, allow_empty: bool = False) -> list[dict[str, Any]]:
    try:
        with zipfile.ZipFile(io.BytesIO(source)) as archive:
            entries = archive.infolist()
            if len(entries) > 1000 or sum(item.file_size for item in entries) > MAX_UNCOMPRESSED_BYTES:
                raise RosterError("export_too_large", "Expanded workbook exceeds the safe upload limit")
            shared: list[str] = []
            if "xl/sharedStrings.xml" in archive.namelist():
                shared = [
                    "".join(item.itertext())
                    for item in ElementTree.fromstring(archive.read("xl/sharedStrings.xml")).findall("s:si", NS)
                ]
            sheets = sorted(
                name for name in archive.namelist()
                if re.fullmatch(r"xl/worksheets/sheet[0-9]+\.xml", name)
            )
            if not sheets:
                raise RosterError("invalid_export", "Workbook has no data worksheets")
            rows: list[dict[str, Any]] = []
            header_seen = False
            for name in sheets:
                grid: list[list[str]] = []
                for element in ElementTree.fromstring(archive.read(name)).findall("s:sheetData/s:row", NS):
                    row: list[str] = []
                    for cell in element.findall("s:c", NS):
                        address = cell.get("r", "")
                        match = re.fullmatch(r"([A-Z]+)[0-9]+", address)
                        if match is None:
                            raise RosterError("invalid_export", "Workbook cell address is invalid")
                        column = 0
                        for char in match[1]:
                            column = column * 26 + ord(char) - ord("A") + 1
                        if column > 512:
                            raise RosterError("invalid_export", "Workbook has too many columns")
                        while len(row) < column:
                            row.append("")
                        if cell.find("s:f", NS) is not None:
                            raise RosterError("formula_in_export", "Formula cells cannot prove roster identities")
                        raw = cell.findtext("s:v", "", NS)
                        cell_type = cell.get("t", "")
                        if cell_type == "s":
                            try:
                                value = shared[int(raw)]
                            except (ValueError, IndexError) as exc:
                                raise RosterError("invalid_export", "Workbook shared string is invalid") from exc
                        elif cell_type == "inlineStr":
                            inline = cell.find("s:is", NS)
                            value = "".join(inline.itertext()) if inline is not None else ""
                        elif cell_type in {"", "n"}:
                            if raw and (not re.fullmatch(r"[0-9]{1,15}", raw)):
                                raise RosterError("rounded_numeric_id", "Numeric long IDs/decimals cannot establish identity; export IDs as text")
                            value = raw
                        elif cell_type == "str":
                            value = raw
                        else:
                            raise RosterError("invalid_export", "Unsupported workbook cell type")
                        row[column - 1] = value
                    grid.append(row)
                rows.extend(_rows_from_grid(grid))
                header_seen = header_seen or any(any(value.strip() for value in row) for row in grid)
            if allow_empty and not header_seen:
                raise RosterError("empty_export", "A zero-member workbook must retain its complete official headers")
            return rows
    except (zipfile.BadZipFile, ElementTree.ParseError, UnicodeError, KeyError) as exc:
        raise RosterError("invalid_export", "Unable to read the official workbook") from exc


def decode_official_export(
    source: bytes, *, source_name: str, allow_empty: bool = False,
) -> list[dict[str, Any]]:
    if not source or len(source) > MAX_SOURCE_BYTES:
        raise RosterError("export_too_large", "Official attachment is empty or exceeds 20 MB")
    suffix = PurePosixPath(source_name).suffix.lower()
    if suffix == ".xlsx":
        rows = _xlsx_rows(source, allow_empty=allow_empty)
    elif suffix == ".csv":
        try:
            text = source.decode("utf-8-sig")
        except UnicodeDecodeError as exc:
            raise RosterError("invalid_encoding", "Export CSV must use UTF-8 encoding") from exc
        grid = list(csv.reader(io.StringIO(text, newline="")))
        if allow_empty and not any(any(value.strip() for value in row) for row in grid):
            raise RosterError("empty_export", "A zero-member CSV must retain its complete official headers")
        rows = _rows_from_grid(grid)
    elif suffix == ".json":
        try:
            parsed = json.loads(source)
        except (json.JSONDecodeError, UnicodeDecodeError) as exc:
            raise RosterError("invalid_export", "Export JSON is invalid") from exc
        values = parsed.get("members") if isinstance(parsed, dict) else parsed
        if not isinstance(values, list) or any(not isinstance(row, dict) for row in values):
            raise RosterError("invalid_export", "Export JSON must contain a member array")
        rows = [_normalize_row(row) for row in values]
    else:
        raise RosterError("invalid_export_type", "Only official XLSX, UTF-8 CSV and JSON exports are supported")
    if not rows and not allow_empty:
        raise RosterError("empty_export", "Official export contains no members")
    return rows
