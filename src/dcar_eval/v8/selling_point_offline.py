"""Offline manifest, retrieval, and prompt contracts for selling-points v5.3.

This module is deliberately read-only with respect to the DCar database.  It
freezes the 228-row development set, builds the same evidence-package-v2 used by
runtime code, and provides deterministic retrieval/prompt validation primitives
for the paid model bake-off.
"""

from __future__ import annotations

import hashlib
import json
import math
import posixpath
import re
import sqlite3
import unicodedata
import xml.etree.ElementTree as ET
import zipfile
from collections import Counter, defaultdict
from dataclasses import dataclass, replace
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Iterable, Mapping, Sequence

from .evaluation import (
    V9_RULE_VERSION,
    _current_evidence_state,
    _evidence_level,
)
from .selling_point_evidence import (
    CHANNELS,
    build_evidence_package,
    load_evidence_config,
)
from .selling_point_label_cards import cards_for_prompt, load_label_cards
from .storage import PROJECT_ROOT, connect


MANIFEST_VERSION = "selling-point-development-manifest-v2"
EXCLUSION_VERSION = "selling-point-development-exclusion-v2"
RETRIEVAL_VERSION = "selling-point-char-ngram-tfidf-v1"
PROMPT_CONTRACT_VERSION = "selling-point-flat-28-v4"
HARD_PRIORITY_VERSION = "selling-point-hard-priority-v2"
GOLD_OVERRIDES_VERSION = "selling-point-gold-overrides-v5.3"
DEFAULT_GOLD_OVERRIDES_PATH = (
    PROJECT_ROOT / "config" / "selling_point_gold_overrides_v5_3.json"
)
EXPECTED_GOLD_HEADERS = (
    "序号",
    "卖点编码",
    "卖点",
    "链接",
    "懂车帝植入位置",
    "视频主要内容",
    "状态",
    "修订说明",
)
EXPECTED_SOURCE_ROWS = 229
EXPECTED_DEVELOPMENT_ROWS = 228
EXPECTED_DEVELOPMENT_UNIQUE_CONTENT = 226
EXPECTED_EXCLUSION_UNIQUE_CONTENT = 227
EXPECTED_EXCLUDED_ROW = 129
EXPECTED_V0_DEVELOPMENT_ROWS = frozenset({98, 132, 193})
_ALLOWED_ANNOTATION_STATUSES = {"原始", "复核修正", "复核确认", "剔除"}
_PLATFORM_CONTENT_RE = re.compile(r"/(?:video|note)/([0-9A-Za-z_-]+)")
_CELL_REF_RE = re.compile(r"^([A-Z]+)([0-9]+)$")
_JSON_FENCE_RE = re.compile(r"^```(?:json)?\s*|\s*```$", re.MULTILINE)
_RETRIEVAL_CLEAN_RE = re.compile(r"[^0-9a-z\u4e00-\u9fff]+")

_MAIN_NS = "http://schemas.openxmlformats.org/spreadsheetml/2006/main"
_REL_NS = "http://schemas.openxmlformats.org/package/2006/relationships"
_DOC_REL_NS = (
    "http://schemas.openxmlformats.org/officeDocument/2006/relationships"
)


class SellingPointOfflineError(ValueError):
    """Raised when frozen inputs or model output violate the v5.3 contract."""


@dataclass(frozen=True)
class GoldRow:
    row_number: int
    gold_code: str | None
    gold_label: str | None
    source_url: str
    implant_position: str
    video_summary: str
    annotation_status: str
    revision_note: str

    @property
    def excluded(self) -> bool:
        return self.annotation_status == "剔除"


def canonical_json(value: Any) -> str:
    return json.dumps(
        value,
        ensure_ascii=False,
        separators=(",", ":"),
        sort_keys=True,
        allow_nan=False,
    )


def sha256_json(value: Any) -> str:
    return hashlib.sha256(canonical_json(value).encode("utf-8")).hexdigest()


def sha256_file(path: Path) -> str:
    digest = hashlib.sha256()
    with path.open("rb") as source:
        for chunk in iter(lambda: source.read(1024 * 1024), b""):
            digest.update(chunk)
    return digest.hexdigest()


def _column_index(reference: str) -> int:
    match = _CELL_REF_RE.fullmatch(reference)
    if match is None:
        raise SellingPointOfflineError(f"invalid XLSX cell reference: {reference}")
    value = 0
    for character in match.group(1):
        value = value * 26 + ord(character) - ord("A") + 1
    return value - 1


def _shared_strings(archive: zipfile.ZipFile) -> list[str]:
    try:
        payload = archive.read("xl/sharedStrings.xml")
    except KeyError:
        return []
    root = ET.fromstring(payload)
    return [
        "".join(node.text or "" for node in item.findall(f".//{{{_MAIN_NS}}}t"))
        for item in root.findall(f"{{{_MAIN_NS}}}si")
    ]


def _sheet_archive_path(archive: zipfile.ZipFile, sheet_name: str) -> str:
    workbook = ET.fromstring(archive.read("xl/workbook.xml"))
    relations = ET.fromstring(archive.read("xl/_rels/workbook.xml.rels"))
    targets = {
        str(item.attrib["Id"]): str(item.attrib["Target"])
        for item in relations.findall(f"{{{_REL_NS}}}Relationship")
    }
    for sheet in workbook.findall(f".//{{{_MAIN_NS}}}sheet"):
        if sheet.attrib.get("name") != sheet_name:
            continue
        relation_id = sheet.attrib.get(f"{{{_DOC_REL_NS}}}id")
        if relation_id is None or relation_id not in targets:
            raise SellingPointOfflineError(
                f"XLSX sheet has no relationship target: {sheet_name}"
            )
        target = targets[relation_id]
        if target.startswith("/"):
            return target.lstrip("/")
        return posixpath.normpath(posixpath.join("xl", target))
    raise SellingPointOfflineError(f"XLSX sheet does not exist: {sheet_name}")


def _cell_value(cell: ET.Element, shared: Sequence[str]) -> Any:
    cell_type = cell.attrib.get("t", "n")
    if cell_type == "inlineStr":
        return "".join(
            node.text or "" for node in cell.findall(f".//{{{_MAIN_NS}}}t")
        )
    value_node = cell.find(f"{{{_MAIN_NS}}}v")
    if value_node is None or value_node.text is None:
        return None
    raw = value_node.text
    if cell_type == "s":
        try:
            return shared[int(raw)]
        except (ValueError, IndexError) as error:
            raise SellingPointOfflineError("invalid XLSX shared string index") from error
    if cell_type in {"str", "e"}:
        return raw
    if cell_type == "b":
        return raw == "1"
    try:
        number = float(raw)
    except ValueError:
        return raw
    return int(number) if number.is_integer() else number


def read_xlsx_sheet(path: Path, sheet_name: str) -> list[list[Any]]:
    """Read values from one XLSX sheet with only the Python standard library."""

    try:
        with zipfile.ZipFile(path) as archive:
            shared = _shared_strings(archive)
            sheet_path = _sheet_archive_path(archive, sheet_name)
            root = ET.fromstring(archive.read(sheet_path))
    except (OSError, KeyError, zipfile.BadZipFile, ET.ParseError) as error:
        raise SellingPointOfflineError(f"cannot read XLSX workbook: {error}") from error

    rows: list[list[Any]] = []
    for row in root.findall(f".//{{{_MAIN_NS}}}sheetData/{{{_MAIN_NS}}}row"):
        values: list[Any] = []
        for cell in row.findall(f"{{{_MAIN_NS}}}c"):
            reference = str(cell.attrib.get("r") or "")
            column = _column_index(reference)
            while len(values) <= column:
                values.append(None)
            values[column] = _cell_value(cell, shared)
        rows.append(values)
    return rows


def _text(value: Any) -> str:
    return str(value).strip() if value is not None else ""


def load_gold_rows(
    path: Path,
    *,
    expected_sha256: str,
    valid_codes: Iterable[str],
) -> tuple[list[GoldRow], str]:
    resolved = path.resolve()
    workbook_sha256 = sha256_file(resolved)
    if workbook_sha256 != expected_sha256:
        raise SellingPointOfflineError(
            "gold workbook hash drifted: "
            f"expected {expected_sha256}, got {workbook_sha256}"
        )
    values = read_xlsx_sheet(resolved, "准确度训练样本v2")
    if not values or tuple(_text(item) for item in values[0][:8]) != EXPECTED_GOLD_HEADERS:
        raise SellingPointOfflineError("gold workbook headers do not match the contract")
    code_set = set(valid_codes)
    output: list[GoldRow] = []
    for raw in values[1:]:
        padded = [*raw, *([None] * max(0, 8 - len(raw)))]
        try:
            row_number = int(padded[0])
        except (TypeError, ValueError) as error:
            raise SellingPointOfflineError("gold row number must be an integer") from error
        status = _text(padded[6])
        if status not in _ALLOWED_ANNOTATION_STATUSES:
            raise SellingPointOfflineError(
                f"unsupported annotation status at row {row_number}: {status}"
            )
        code = _text(padded[1]) or None
        if status == "剔除":
            if code is not None:
                raise SellingPointOfflineError(
                    f"excluded row {row_number} must not have a gold code"
                )
        elif code not in code_set:
            raise SellingPointOfflineError(
                f"unknown gold code at row {row_number}: {code}"
            )
        url = _text(padded[3])
        if not url:
            raise SellingPointOfflineError(f"gold row {row_number} has no URL")
        output.append(
            GoldRow(
                row_number=row_number,
                gold_code=code,
                gold_label=_text(padded[2]) or None,
                source_url=url,
                implant_position=_text(padded[4]),
                video_summary=_text(padded[5]),
                annotation_status=status,
                revision_note=_text(padded[7]),
            )
        )
    if len(output) != EXPECTED_SOURCE_ROWS:
        raise SellingPointOfflineError(
            f"gold workbook must contain {EXPECTED_SOURCE_ROWS} rows, got {len(output)}"
        )
    if [row.row_number for row in output] != list(range(1, EXPECTED_SOURCE_ROWS + 1)):
        raise SellingPointOfflineError("gold row numbers must be contiguous 1..229")
    excluded = [row.row_number for row in output if row.excluded]
    if excluded != [EXPECTED_EXCLUDED_ROW]:
        raise SellingPointOfflineError("only gold row 129 may be excluded")
    return output, workbook_sha256


def load_gold_overrides(
    path: Path = DEFAULT_GOLD_OVERRIDES_PATH,
    *,
    expected_source_sha256: str,
    valid_codes: Iterable[str],
) -> dict[str, Any]:
    """Load and validate the immutable v5.3 gold-label decision overlay."""

    resolved = path.resolve()
    try:
        payload = resolved.read_bytes()
        source = json.loads(payload.decode("utf-8"))
    except (OSError, UnicodeDecodeError, json.JSONDecodeError) as error:
        raise SellingPointOfflineError(f"cannot load gold overrides: {error}") from error
    if not isinstance(source, dict):
        raise SellingPointOfflineError("gold overrides must be an object")
    if source.get("version") != GOLD_OVERRIDES_VERSION:
        raise SellingPointOfflineError("gold override version mismatch")
    if source.get("source_gold_sha256") != expected_source_sha256:
        raise SellingPointOfflineError("gold overrides do not bind the source workbook")
    decisions = source.get("decisions")
    expected_count = source.get("expected_change_count")
    if not isinstance(decisions, list) or expected_count != 18 or len(decisions) != 18:
        raise SellingPointOfflineError("gold overrides must contain exactly 18 decisions")
    codes = set(valid_codes)
    normalized: list[dict[str, Any]] = []
    seen_rows: set[int] = set()
    for raw in decisions:
        if not isinstance(raw, Mapping):
            raise SellingPointOfflineError("gold override decision must be an object")
        raw_excel_row = raw.get("excel_row")
        if isinstance(raw_excel_row, bool) or not isinstance(raw_excel_row, int):
            raise SellingPointOfflineError("gold override row must be an integer")
        excel_row = raw_excel_row
        from_code = str(raw.get("from_code") or "")
        to_code = str(raw.get("to_code") or "")
        decision_id = str(raw.get("decision_id") or "").strip()
        reason = str(raw.get("reason") or "").strip()
        if not 1 <= excel_row <= EXPECTED_SOURCE_ROWS or excel_row == EXPECTED_EXCLUDED_ROW:
            raise SellingPointOfflineError(f"invalid gold override row: {excel_row}")
        if excel_row in seen_rows:
            raise SellingPointOfflineError(f"duplicate gold override row: {excel_row}")
        if from_code not in codes or to_code not in codes or from_code == to_code:
            raise SellingPointOfflineError(f"invalid gold override codes at row {excel_row}")
        if not decision_id or not reason:
            raise SellingPointOfflineError(
                f"gold override decision and reason are required at row {excel_row}"
            )
        seen_rows.add(excel_row)
        normalized.append(
            {
                "excel_row": excel_row,
                "from_code": from_code,
                "to_code": to_code,
                "decision_id": decision_id,
                "reason": reason,
            }
        )
    expected_scene_counts = source.get("expected_scene_counts")
    if expected_scene_counts != {"X": 112, "E": 93, "M": 23}:
        raise SellingPointOfflineError("gold override scene counts must be X=112/E=93/M=23")
    return {
        "version": GOLD_OVERRIDES_VERSION,
        "source_path": str(resolved),
        "source_sha256": hashlib.sha256(payload).hexdigest(),
        "source_gold_sha256": expected_source_sha256,
        "expected_scene_counts": dict(expected_scene_counts),
        "decisions": sorted(normalized, key=lambda item: item["excel_row"]),
    }


def apply_gold_overrides(
    rows: Sequence[GoldRow],
    *,
    cards: Mapping[str, Mapping[str, Any]],
    source_gold_sha256: str,
    override_path: Path = DEFAULT_GOLD_OVERRIDES_PATH,
) -> tuple[list[GoldRow], dict[str, Any]]:
    """Apply the frozen decision overlay without changing the source workbook."""

    overrides = load_gold_overrides(
        override_path,
        expected_source_sha256=source_gold_sha256,
        valid_codes=cards,
    )
    decisions = {int(item["excel_row"]): item for item in overrides["decisions"]}
    output: list[GoldRow] = []
    changes: list[dict[str, Any]] = []
    for row in rows:
        decision = decisions.get(row.row_number)
        if decision is None:
            output.append(row)
            continue
        if row.gold_code != decision["from_code"]:
            raise SellingPointOfflineError(
                f"gold override source mismatch at row {row.row_number}: "
                f"expected {decision['from_code']}, got {row.gold_code}"
            )
        card = cards[decision["to_code"]]
        note = (
            f"2026-08-24 v5.3 {decision['decision_id']}: "
            f"{decision['from_code']} → {decision['to_code']}；{decision['reason']}"
        )
        revision_note = "；".join(value for value in (row.revision_note, note) if value)
        changed = replace(
            row,
            gold_code=str(decision["to_code"]),
            gold_label=str(card["label"]),
            annotation_status="复核修正",
            revision_note=revision_note,
        )
        output.append(changed)
        changes.append(
            {
                **decision,
                "from_label": row.gold_label,
                "to_label": changed.gold_label,
            }
        )
    if len(changes) != 18 or set(decisions) != {item["excel_row"] for item in changes}:
        raise SellingPointOfflineError("not all gold override decisions were applied")
    scored = [row for row in output if not row.excluded]
    scene_counts = Counter(str(row.gold_code)[0] for row in scored)
    if dict(scene_counts) != overrides["expected_scene_counts"]:
        raise SellingPointOfflineError(
            f"derived gold scene counts drifted: {dict(scene_counts)}"
        )
    label_counts = Counter(str(row.gold_code) for row in scored)
    return output, {
        **overrides,
        "change_count": len(changes),
        "changes": changes,
        "scene_counts": dict(sorted(scene_counts.items())),
        "label_counts": dict(sorted(label_counts.items())),
        "zero_sample_codes": sorted(code for code in cards if label_counts[code] == 0),
        "low_sample_codes": {
            code: label_counts[code]
            for code in sorted(cards)
            if 0 < label_counts[code] <= 2
        },
    }


def _platform_content_id(url: str) -> str | None:
    match = _PLATFORM_CONTENT_RE.search(url)
    return match.group(1) if match is not None else None


def _resolve_content(connection: sqlite3.Connection, gold: GoldRow) -> sqlite3.Row:
    platform_content_id = _platform_content_id(gold.source_url)
    rows = connection.execute(
        """
        SELECT * FROM content_items
        WHERE canonical_url=? OR (? IS NOT NULL AND platform_content_id=?)
        ORDER BY CASE WHEN canonical_url=? THEN 0 ELSE 1 END, id
        """,
        (
            gold.source_url,
            platform_content_id,
            platform_content_id,
            gold.source_url,
        ),
    ).fetchall()
    if not rows:
        raise SellingPointOfflineError(
            f"gold row {gold.row_number} URL is missing from content_items"
        )
    exact = [row for row in rows if str(row["canonical_url"]) == gold.source_url]
    candidates = exact or rows
    content_ids = {int(row["id"]) for row in candidates}
    if len(content_ids) != 1:
        raise SellingPointOfflineError(
            f"gold row {gold.row_number} URL maps to multiple contents"
        )
    return candidates[0]


def _read_json(path: Path | None) -> dict[str, Any]:
    if path is None or not path.is_file():
        return {}
    try:
        value = json.loads(path.read_text(encoding="utf-8"))
    except (OSError, UnicodeDecodeError, json.JSONDecodeError):
        return {}
    return value if isinstance(value, dict) else {}


def _ocr_observations(value: Mapping[str, Any]) -> list[str]:
    raw = value.get("observations")
    if not isinstance(raw, list):
        return []
    output: list[str] = []
    for item in raw:
        if isinstance(item, Mapping):
            text = _text(item.get("text"))
        else:
            text = _text(item)
        if text:
            output.append(text)
    return output


def _keyframes(connection: sqlite3.Connection, content_id: int) -> list[str]:
    row = connection.execute(
        """
        SELECT local_path FROM evidence_artifacts
        WHERE content_id=? AND artifact_type='frames_manifest'
          AND status='available' AND sha256 IS NOT NULL
        ORDER BY id DESC LIMIT 1
        """,
        (content_id,),
    ).fetchone()
    if row is None:
        return []
    manifest_path = Path(str(row["local_path"]))
    if not manifest_path.is_absolute():
        manifest_path = PROJECT_ROOT / manifest_path
    manifest = _read_json(manifest_path)
    raw_frames = manifest.get("frames")
    if not isinstance(raw_frames, list):
        return []
    output: list[str] = []
    for item in raw_frames:
        raw_path = item.get("path") if isinstance(item, Mapping) else item
        value = Path(_text(raw_path))
        if not str(value):
            continue
        resolved = value if value.is_absolute() else PROJECT_ROOT / value
        if resolved.is_file():
            try:
                output.append(str(resolved.relative_to(PROJECT_ROOT)))
            except ValueError:
                output.append(str(resolved))
    return output


def _active_release(connection: sqlite3.Connection) -> sqlite3.Row:
    rows = connection.execute(
        "SELECT * FROM evaluation_releases WHERE status='active' ORDER BY id"
    ).fetchall()
    if len(rows) != 1:
        raise SellingPointOfflineError("exactly one active evaluation release is required")
    release = rows[0]
    if str(release["rule_version"]) != V9_RULE_VERSION:
        raise SellingPointOfflineError("stage A manifest requires active evaluation-v9")
    if str(release["taxonomy_version"]) != "selling-points-v5.2":
        raise SellingPointOfflineError("stage A manifest requires selling-points-v5.2")
    return release


def _manifest_row(
    connection: sqlite3.Connection,
    gold: GoldRow,
    *,
    evidence_config: Mapping[str, Any],
    rule_version: str,
) -> dict[str, Any]:
    content = _resolve_content(connection, gold)
    content_id = int(content["id"])
    artifacts, components, evidence_sha256 = _current_evidence_state(
        connection,
        content_id,
        rule_version=rule_version,
    )
    asr = _read_json(artifacts["asr_path"])
    ocr = _read_json(artifacts["ocr_path"])
    title = str(content["title"] or "")
    body = str(content["body"] or "")
    asr_text = str(asr.get("text") or "")
    ocr_combined = str(ocr.get("combined_text") or "")
    observations = _ocr_observations(ocr)
    keyframes = _keyframes(connection, content_id)
    text = "\n".join(value for value in (title, body) if value)
    evidence_level, evidence_summary = _evidence_level(
        content_type=str(content["content_type"]),
        text=text,
        media_path=artifacts["media_path"],
        asr=asr,
        ocr=ocr,
    )
    package = build_evidence_package(
        title=title,
        body=body,
        asr=asr_text,
        ocr_observations=observations,
        ocr_combined=ocr_combined,
        keyframes=keyframes,
        evidence_level=evidence_level,
        evidence_sha256=evidence_sha256,
        config=evidence_config,
    )
    retrieval_code = gold.gold_code
    return {
        "excel_row": gold.row_number,
        "content_id": content_id,
        "canonical_url": str(content["canonical_url"]),
        "platform_content_id": str(content["platform_content_id"] or ""),
        "gold_code": gold.gold_code,
        "retrieval_code": retrieval_code,
        "gold_label": gold.gold_label,
        "annotation_status": gold.annotation_status,
        "implant_position": gold.implant_position,
        "video_summary": gold.video_summary,
        "revision_note": gold.revision_note,
        "evidence_level": evidence_level,
        "evidence_summary": evidence_summary,
        "evidence_sha256": evidence_sha256,
        "evidence_components": components,
        "original_channels": {
            "title": title,
            "body": body,
            "asr": asr_text,
            "ocr": "\n".join(observations) if observations else ocr_combined,
        },
        "evidence_package": package,
    }


def _duplicate_groups(rows: Sequence[Mapping[str, Any]]) -> list[dict[str, Any]]:
    groups: dict[int, list[int]] = defaultdict(list)
    for row in rows:
        groups[int(row["content_id"])].append(int(row["excel_row"]))
    return [
        {"content_id": content_id, "excel_rows": sorted(excel_rows)}
        for content_id, excel_rows in sorted(groups.items())
        if len(excel_rows) > 1
    ]


def build_development_artifacts(
    *,
    gold_path: Path,
    db_path: Path,
    generated_at: str | None = None,
) -> tuple[dict[str, Any], dict[str, Any]]:
    """Build the frozen 228-row manifest and 227-content blind-test exclusion."""

    labels = load_label_cards()
    evidence_config = load_evidence_config()
    expected_gold_sha256 = str(labels["source_gold_sha256"])
    source_gold_rows, workbook_sha256 = load_gold_rows(
        gold_path,
        expected_sha256=expected_gold_sha256,
        valid_codes=labels["cards"],
    )
    gold_rows, derived_gold = apply_gold_overrides(
        source_gold_rows,
        cards=labels["cards"],
        source_gold_sha256=workbook_sha256,
    )
    created_at = generated_at or datetime.now(timezone.utc).isoformat(
        timespec="seconds"
    ).replace("+00:00", "Z")
    all_rows: list[dict[str, Any]] = []
    with connect(db_path.resolve(), read_only=True) as connection:
        release = _active_release(connection)
        active_release_id = str(release["id"])
        for gold in gold_rows:
            all_rows.append(
                _manifest_row(
                    connection,
                    gold,
                    evidence_config=evidence_config,
                    rule_version=str(release["rule_version"]),
                )
            )

    development_rows = [
        row for row in all_rows if int(row["excel_row"]) != EXPECTED_EXCLUDED_ROW
    ]
    if len(development_rows) != EXPECTED_DEVELOPMENT_ROWS:
        raise SellingPointOfflineError("development manifest must contain 228 rows")
    unique_development = {int(row["content_id"]) for row in development_rows}
    if len(unique_development) != EXPECTED_DEVELOPMENT_UNIQUE_CONTENT:
        raise SellingPointOfflineError(
            "development rows must map to 226 unique current contents"
        )
    v0_rows = {
        int(row["excel_row"])
        for row in development_rows
        if row["evidence_level"] == "V0"
    }
    if v0_rows != EXPECTED_V0_DEVELOPMENT_ROWS:
        raise SellingPointOfflineError(
            f"development V0 rows drifted: expected {sorted(EXPECTED_V0_DEVELOPMENT_ROWS)}, "
            f"got {sorted(v0_rows)}"
        )
    row97 = next(row for row in development_rows if int(row["excel_row"]) == 97)
    if row97["evidence_level"] != "V1":
        raise SellingPointOfflineError("gold row 97 must remain V1")

    counts = Counter(str(row["evidence_level"]) for row in development_rows)
    manifest: dict[str, Any] = {
        "version": MANIFEST_VERSION,
        "generated_at": created_at,
        "source": {
            "gold_path": str(gold_path.resolve()),
            "gold_sha256": workbook_sha256,
            "gold_standard_version": derived_gold["version"],
            "gold_overrides_path": derived_gold["source_path"],
            "gold_overrides_sha256": derived_gold["source_sha256"],
            "label_cards_sha256": str(labels["source_sha256"]),
            "evidence_config_sha256": str(evidence_config["config_sha256"]),
            "matcher_bundle_sha256": str(
                evidence_config["source_matcher_bundle_sha256"]
            ),
            "matcher_replacements_sha256": str(
                evidence_config["source_ordered_replacements_sha256"]
            ),
            "database_path": str(db_path.resolve()),
            "active_release_id": active_release_id,
            "rule_version": V9_RULE_VERSION,
        },
        "counts": {
            "source_rows": len(all_rows),
            "development_rows": len(development_rows),
            "development_unique_content_ids": len(unique_development),
            "evidence_levels": dict(sorted(counts.items())),
            "excluded_rows": [EXPECTED_EXCLUDED_ROW],
        },
        "duplicate_groups": _duplicate_groups(development_rows),
        "derived_gold": {
            "version": derived_gold["version"],
            "change_count": derived_gold["change_count"],
            "changes": derived_gold["changes"],
            "scene_counts": derived_gold["scene_counts"],
            "label_counts": derived_gold["label_counts"],
            "zero_sample_codes": derived_gold["zero_sample_codes"],
            "low_sample_codes": derived_gold["low_sample_codes"],
        },
        "scoring_contract": {
            "denominator": 228,
            "overall_pass_count": 160,
            "scene_pass_rates": {"X": 0.75, "E": 0.70, "M": 0.60},
            "scene_pass_counts": {"X": 84, "E": 66, "M": 14},
            "row_45_gold_code": "X9",
            "v0_rows_count_as_incorrect": sorted(EXPECTED_V0_DEVELOPMENT_ROWS),
        },
        "rows": development_rows,
    }
    manifest["manifest_sha256"] = sha256_json(manifest)

    exclusion_groups: dict[int, dict[str, Any]] = {}
    for row in all_rows:
        content_id = int(row["content_id"])
        entry = exclusion_groups.setdefault(
            content_id,
            {
                "content_id": content_id,
                "canonical_url": str(row["canonical_url"]),
                "excel_rows": [],
            },
        )
        if entry["canonical_url"] != row["canonical_url"]:
            raise SellingPointOfflineError(
                f"content {content_id} has inconsistent canonical URLs"
            )
        entry["excel_rows"].append(int(row["excel_row"]))
    if len(exclusion_groups) != EXPECTED_EXCLUSION_UNIQUE_CONTENT:
        raise SellingPointOfflineError("blind-test exclusion must contain 227 contents")
    exclusion: dict[str, Any] = {
        "version": EXCLUSION_VERSION,
        "generated_at": created_at,
        "development_manifest_sha256": manifest["manifest_sha256"],
        "scope_note": (
            "排除原表全部229行对应的227个唯一内容；包含已从评分分母剔除的第129行，"
            "防止任何开发期已见内容进入G2。"
        ),
        "entries": [
            {
                **entry,
                "excel_rows": sorted(entry["excel_rows"]),
            }
            for _, entry in sorted(exclusion_groups.items())
        ],
    }
    exclusion["manifest_sha256"] = sha256_json(exclusion)
    return manifest, exclusion


def derive_gold_manifest(manifest: Mapping[str, Any]) -> dict[str, Any]:
    """Create the compact, signed v5.3 gold artifact consumed by G1 scoring."""

    rows = manifest.get("rows")
    derived = manifest.get("derived_gold")
    source = manifest.get("source")
    if not isinstance(rows, list) or not isinstance(derived, Mapping) or not isinstance(source, Mapping):
        raise SellingPointOfflineError("development manifest lacks derived gold metadata")
    compact_rows = [
        {
            "excel_row": int(row["excel_row"]),
            "content_id": int(row["content_id"]),
            "canonical_url": str(row["canonical_url"]),
            "gold_code": str(row["gold_code"]),
            "gold_label": str(row["gold_label"]),
            "annotation_status": str(row["annotation_status"]),
            "revision_note": str(row["revision_note"]),
            "evidence_level": str(row["evidence_level"]),
        }
        for row in rows
    ]
    value: dict[str, Any] = {
        "version": "selling-point-derived-gold-v5.3",
        "development_manifest_sha256": str(manifest.get("manifest_sha256") or ""),
        "source_gold_path": str(source.get("gold_path") or ""),
        "source_gold_sha256": str(source.get("gold_sha256") or ""),
        "gold_overrides_sha256": str(source.get("gold_overrides_sha256") or ""),
        "change_count": int(derived.get("change_count") or 0),
        "changes": list(derived.get("changes") or []),
        "scene_counts": dict(derived.get("scene_counts") or {}),
        "label_counts": dict(derived.get("label_counts") or {}),
        "zero_sample_codes": list(derived.get("zero_sample_codes") or []),
        "low_sample_codes": dict(derived.get("low_sample_codes") or {}),
        "rows": compact_rows,
    }
    value["manifest_sha256"] = sha256_json(value)
    return value


def build_g0_assertion_report(manifest: Mapping[str, Any]) -> dict[str, Any]:
    """Evaluate all blocking G0 invariants against one signed development set."""

    rows = manifest.get("rows")
    derived = manifest.get("derived_gold")
    source = manifest.get("source")
    if not isinstance(rows, list) or not isinstance(derived, Mapping) or not isinstance(source, Mapping):
        raise SellingPointOfflineError("development manifest lacks G0 inputs")
    cards = load_label_cards()["cards"]
    p0_hits: list[dict[str, Any]] = []
    p1_hits: list[dict[str, Any]] = []
    p2_hits: list[dict[str, Any]] = []
    conflicts: list[dict[str, Any]] = []
    for row in rows:
        package = row.get("evidence_package")
        if not isinstance(package, Mapping):
            raise SellingPointOfflineError("manifest row lacks an evidence package")
        priority = hard_priority(package, cards)
        item = {
            "excel_row": int(row["excel_row"]),
            "content_id": int(row["content_id"]),
            "gold_code": str(row["gold_code"]),
            "priority": str(priority["priority"]),
            "allowed_codes": list(priority["allowed_codes"]),
            "forced_code": priority["forced_code"],
            "evidence_level": str(row["evidence_level"]),
        }
        if priority["priority"] == "P0":
            p0_hits.append(item)
            if item["gold_code"] not in priority["allowed_codes"]:
                conflicts.append(item)
        elif priority["priority"] == "P1" and item["evidence_level"] != "V0":
            p1_hits.append(item)
            if item["gold_code"] != "X8":
                conflicts.append(item)
        elif priority["priority"] == "P2":
            p2_hits.append(item)
            if priority["forced_code"] is not None or set(priority["allowed_codes"]) != set(cards):
                conflicts.append(item)
    expected_p1_rows = [90, 91, 92, 93, 94, 95, 96, 97, 99]
    checks = {
        "development_rows": len(rows) == 228,
        "unique_content_ids": len({int(row["content_id"]) for row in rows}) == 226,
        "derived_change_count": int(derived.get("change_count") or 0) == 18,
        "scene_counts": dict(derived.get("scene_counts") or {})
        == {"E": 93, "M": 23, "X": 112},
        "zero_sample_codes": set(derived.get("zero_sample_codes") or [])
        == {"X11", "M1", "M3"},
        "low_sample_codes": dict(derived.get("low_sample_codes") or {})
        == {"X10": 1, "M5": 1, "M6": 1, "M8": 2},
        "p0_count": len(p0_hits) == 19,
        "p0_all_allowed": all(item["gold_code"] in item["allowed_codes"] for item in p0_hits),
        "p1_rows": [item["excel_row"] for item in p1_hits] == expected_p1_rows,
        "p1_all_x8": all(item["gold_code"] == "X8" for item in p1_hits),
        "p2_soft_only": all(
            item["forced_code"] is None and set(item["allowed_codes"]) == set(cards)
            for item in p2_hits
        ),
        "hard_conflicts_zero": not conflicts,
        "source_gold_unchanged": str(source.get("gold_sha256") or "")
        == "b34b5f7b550b948ec5f704620653e6e57662814d22d105f15631cc79588d8eec",
    }
    report: dict[str, Any] = {
        "version": "selling-point-g0-assertions-v1",
        "development_manifest_sha256": str(manifest.get("manifest_sha256") or ""),
        "checks": checks,
        "passed": all(checks.values()),
        "p0_hits": p0_hits,
        "p1_hits": p1_hits,
        "p2_hits": p2_hits,
        "hard_conflicts": conflicts,
        "blind_spots": {
            "zero_sample_codes": list(derived.get("zero_sample_codes") or []),
            "low_sample_codes": dict(derived.get("low_sample_codes") or {}),
        },
    }
    report["report_sha256"] = sha256_json(report)
    if not report["passed"]:
        failed = sorted(key for key, passed in checks.items() if not passed)
        raise SellingPointOfflineError(f"G0 assertions failed: {failed}")
    return report


def retrieval_text(row: Mapping[str, Any]) -> str:
    package = row.get("evidence_package")
    if not isinstance(package, Mapping):
        raise SellingPointOfflineError("manifest row has no evidence package")
    windows = package.get("anchor_windows")
    channels = package.get("channels")
    if not isinstance(windows, list) or not isinstance(channels, Mapping):
        raise SellingPointOfflineError("invalid evidence package in manifest row")
    parts = [
        str(window.get("text") or "")
        for window in windows
        if isinstance(window, Mapping)
    ]
    parts.extend(str(channels.get(channel) or "") for channel in CHANNELS)
    normalized = unicodedata.normalize("NFKC", "\n".join(parts)).casefold()
    return _RETRIEVAL_CLEAN_RE.sub("", normalized)


def char_ngrams(text: str, *, minimum: int = 2, maximum: int = 4) -> Counter[str]:
    cleaned = _RETRIEVAL_CLEAN_RE.sub(
        "", unicodedata.normalize("NFKC", str(text or "")).casefold()
    )
    output: Counter[str] = Counter()
    for size in range(minimum, maximum + 1):
        output.update(
            cleaned[index : index + size]
            for index in range(max(0, len(cleaned) - size + 1))
        )
    return output


def _retrieval_representatives(
    rows: Sequence[Mapping[str, Any]],
) -> list[Mapping[str, Any]]:
    groups: dict[int, list[Mapping[str, Any]]] = defaultdict(list)
    for row in rows:
        if str(row.get("evidence_level")) == "V0":
            continue
        groups[int(row["content_id"])].append(row)
    output: list[Mapping[str, Any]] = []
    for content_id, candidates in sorted(groups.items()):
        codes = {str(row.get("retrieval_code") or "") for row in candidates}
        if len(codes) != 1 or "" in codes:
            raise SellingPointOfflineError(
                f"content {content_id} has conflicting retrieval labels: {sorted(codes)}"
            )
        output.append(
            max(
                candidates,
                key=lambda row: (
                    len(str(row.get("implant_position") or ""))
                    + len(str(row.get("video_summary") or "")),
                    -int(row["excel_row"]),
                ),
            )
        )
    return output


class CharNgramTfidfIndex:
    """Small deterministic TF-IDF index for the frozen development corpus."""

    def __init__(self, rows: Sequence[Mapping[str, Any]]) -> None:
        self.rows = _retrieval_representatives(rows)
        self.counts = [char_ngrams(retrieval_text(row)) for row in self.rows]
        document_frequency: Counter[str] = Counter()
        for counts in self.counts:
            document_frequency.update(counts.keys())
        total = len(self.rows)
        self.idf = {
            token: math.log((1 + total) / (1 + frequency)) + 1.0
            for token, frequency in document_frequency.items()
        }
        self.vectors = [self._vector(counts) for counts in self.counts]
        self.index_sha256 = sha256_json(
            {
                "version": RETRIEVAL_VERSION,
                "ngram_min": 2,
                "ngram_max": 4,
                "documents": [
                    {
                        "content_id": int(row["content_id"]),
                        "canonical_url": str(row["canonical_url"]),
                        "excel_row": int(row["excel_row"]),
                        "code": str(row["retrieval_code"]),
                        "text_sha256": hashlib.sha256(
                            retrieval_text(row).encode("utf-8")
                        ).hexdigest(),
                    }
                    for row in self.rows
                ],
            }
        )

    def _vector(self, counts: Counter[str]) -> dict[str, float]:
        return {
            token: (1.0 + math.log(count)) * self.idf[token]
            for token, count in counts.items()
            if token in self.idf and count > 0
        }

    @staticmethod
    def _cosine(left: Mapping[str, float], right: Mapping[str, float]) -> float:
        if not left or not right:
            return 0.0
        shared = set(left) & set(right)
        numerator = sum(left[token] * right[token] for token in shared)
        left_norm = math.sqrt(sum(value * value for value in left.values()))
        right_norm = math.sqrt(sum(value * value for value in right.values()))
        return numerator / (left_norm * right_norm) if left_norm and right_norm else 0.0

    def select(
        self,
        target: Mapping[str, Any],
        *,
        limit: int = 4,
        max_per_label: int = 2,
    ) -> list[dict[str, Any]]:
        query = self._vector(char_ngrams(retrieval_text(target)))
        target_content_id = int(target["content_id"])
        target_url = str(target["canonical_url"])
        candidates: list[tuple[float, Mapping[str, Any]]] = []
        for row, vector in zip(self.rows, self.vectors, strict=True):
            if (
                int(row["content_id"]) == target_content_id
                or str(row["canonical_url"]) == target_url
            ):
                continue
            similarity = self._cosine(query, vector)
            if similarity > 0:
                candidates.append((similarity, row))
        candidates.sort(key=lambda item: (-item[0], int(item[1]["excel_row"])))
        selected: list[dict[str, Any]] = []
        label_counts: Counter[str] = Counter()
        for similarity, row in candidates:
            code = str(row["retrieval_code"])
            if label_counts[code] >= max_per_label:
                continue
            selected.append(
                {
                    "excel_row": int(row["excel_row"]),
                    "content_id": int(row["content_id"]),
                    "canonical_url": str(row["canonical_url"]),
                    "code": code,
                    "similarity": round(similarity, 8),
                    "implant_position": str(row.get("implant_position") or ""),
                    "video_summary": str(row.get("video_summary") or ""),
                    "evidence_excerpt": retrieval_text(row)[:900],
                }
            )
            label_counts[code] += 1
            if len(selected) >= limit:
                break
        return selected

    def select_for_codes(
        self,
        target: Mapping[str, Any],
        codes: Iterable[str],
    ) -> list[dict[str, Any]]:
        """Return the nearest leave-out precedent for every supported code."""

        requested = list(dict.fromkeys(str(code) for code in codes))
        query = self._vector(char_ngrams(retrieval_text(target)))
        target_content_id = int(target["content_id"])
        target_url = str(target["canonical_url"])
        best: dict[str, tuple[float, Mapping[str, Any]]] = {}
        for row, vector in zip(self.rows, self.vectors, strict=True):
            code = str(row["retrieval_code"])
            if code not in requested or (
                int(row["content_id"]) == target_content_id
                or str(row["canonical_url"]) == target_url
            ):
                continue
            similarity = self._cosine(query, vector)
            current = best.get(code)
            if current is None or (-similarity, int(row["excel_row"])) < (
                -current[0],
                int(current[1]["excel_row"]),
            ):
                best[code] = (similarity, row)
        output: list[dict[str, Any]] = []
        for code in requested:
            selected = best.get(code)
            if selected is None:
                continue
            similarity, row = selected
            output.append(
                {
                    "excel_row": int(row["excel_row"]),
                    "content_id": int(row["content_id"]),
                    "canonical_url": str(row["canonical_url"]),
                    "code": code,
                    "similarity": round(similarity, 8),
                    "implant_position": str(row.get("implant_position") or ""),
                    "video_summary": str(row.get("video_summary") or ""),
                    "evidence_excerpt": retrieval_text(row)[:900],
                }
            )
        return output

    def freeze_payload(self, *, development_manifest_sha256: str) -> dict[str, Any]:
        return {
            "version": RETRIEVAL_VERSION,
            "development_manifest_sha256": development_manifest_sha256,
            "index_sha256": self.index_sha256,
            "ngram_min": 2,
            "ngram_max": 4,
            "document_count": len(self.rows),
            "documents": [
                {
                    "excel_row": int(row["excel_row"]),
                    "content_id": int(row["content_id"]),
                    "canonical_url": str(row["canonical_url"]),
                    "code": str(row["retrieval_code"]),
                }
                for row in self.rows
            ],
        }


def _terms_within(
    text: str,
    left_terms: Sequence[str],
    right_terms: Sequence[str],
    *,
    width: int,
) -> bool:
    left_positions = [
        index
        for term in left_terms
        for index in range(len(text))
        if text.startswith(term, index)
    ]
    right_positions = [
        index
        for term in right_terms
        for index in range(len(text))
        if text.startswith(term, index)
    ]
    return any(abs(left - right) <= width for left in left_positions for right in right_positions)


def _p1_direct_trigger(channels: Mapping[str, Any]) -> bool:
    subsidy_terms = (
        "政府补贴",
        "购车补贴",
        "汽车补贴",
        "首购补贴",
        "置换补贴",
        "报废补贴",
        "国补",
        "省补",
        "地补",
        "厂补",
        "平台补",
        "以旧换新",
    )
    action_terms = (
        "领取",
        "申领",
        "办理",
        "上传",
        "填写",
        "提交",
        "入口",
        "端口",
        "打款",
        "资格",
        "流程",
        "教程",
        "教学",
    )
    entry_terms = ("打开", "进入", "登录", "下载")
    platform_terms = ("懂车帝", "承接平台", "指定平台")
    for channel in ("title", "body", "asr"):
        text = str(channels.get(channel) or "")
        if "搜索口令" in text or "补贴口令" in text:
            return True
        if _terms_within(text, subsidy_terms, action_terms, width=32):
            return True
        if (
            _terms_within(text, subsidy_terms, entry_terms, width=48)
            and _terms_within(text, subsidy_terms, platform_terms, width=48)
        ):
            return True
    return False


def hard_priority(package: Mapping[str, Any], all_codes: Iterable[str]) -> dict[str, Any]:
    channels = package.get("channels")
    windows = package.get("anchor_windows")
    if not isinstance(channels, Mapping) or not isinstance(windows, list):
        raise SellingPointOfflineError("invalid evidence package for hard priorities")
    direct_text = "\n".join(
        str(channels.get(channel) or "") for channel in ("title", "body", "asr")
    )
    text = "\n".join(
        [direct_text, str(channels.get("ocr") or "")]
        + [
            str(window.get("text") or "")
            for window in windows
            if isinstance(window, Mapping)
        ]
    )
    codes = sorted(set(all_codes))
    if "AI小懂" in text:
        return {
            "priority": "P0",
            "allowed_codes": [f"M{index}" for index in range(1, 7)],
            "forced_code": None,
            "default_code": "M2",
        }
    # OCR often captures unrelated tabs and recommendation cards while a creator
    # demonstrates another DCar task (the frozen 45/168 X9 boundary is a real
    # example).  OCR-only benefit text remains visible to the semantic model but
    # is not strong enough to collapse the allowed set to X8.
    if _p1_direct_trigger(channels):
        return {
            "priority": "P1",
            "allowed_codes": ["X8"],
            "forced_code": "X8",
            "default_code": None,
        }
    condition_terms = ("车况有保障", "透明车况", "车况透明", "车况保障")
    price_terms = ("价格透明", "价格有保障", "价格保障", "价格依据")
    if (
        "二手车" in text
        and any(term in text for term in condition_terms)
        and any(term in text for term in price_terms)
    ):
        return {
            "priority": "P2",
            "allowed_codes": codes,
            "forced_code": None,
            "default_code": None,
        }
    return {
        "priority": "P4",
        "allowed_codes": codes,
        "forced_code": None,
        "default_code": None,
    }


def prompt_quote_options(target: Mapping[str, Any], *, limit: int = 12) -> list[dict[str, str]]:
    """Build short, exact and source-mappable quote choices for the model."""

    package = target.get("evidence_package")
    if not isinstance(package, Mapping):
        raise SellingPointOfflineError("target has no evidence package")
    windows = package.get("anchor_windows")
    channels = package.get("channels")
    if not isinstance(windows, list) or not isinstance(channels, Mapping):
        raise SellingPointOfflineError("target has invalid quote-option evidence")
    candidates: list[tuple[str, str]] = []
    for window in windows:
        if not isinstance(window, Mapping):
            continue
        channel = str(window.get("channel") or "")
        text = str(window.get("text") or "")
        for anchor in window.get("anchors", []):
            if not isinstance(anchor, Mapping):
                continue
            term = str(anchor.get("term") or "")
            position = text.find(term)
            if position < 0:
                continue
            start = max(0, position - 4)
            end = min(len(text), position + len(term) + 24)
            candidates.append((channel, text[start:end].strip()))
    for channel in CHANNELS:
        text = str(channels.get(channel) or "").strip()
        if text:
            candidates.append((channel, text[:32].strip()))
    output: list[dict[str, str]] = []
    seen: set[tuple[str, str]] = set()
    for channel, quote in candidates:
        key = (channel, quote)
        if key in seen or not 6 <= len(quote) <= 48:
            continue
        seen.add(key)
        if map_quote_to_original(target, channel=channel, quote=quote)["status"] != (
            "accepted"
        ):
            continue
        output.append({"channel": channel, "quote": quote})
        if len(output) >= limit:
            break
    return output


def build_prompt(
    target: Mapping[str, Any],
    *,
    index: CharNgramTfidfIndex,
    label_cards: Mapping[str, Any] | None = None,
) -> dict[str, Any]:
    labels = dict(label_cards or load_label_cards())
    cards = labels.get("cards")
    if not isinstance(cards, Mapping):
        raise SellingPointOfflineError("loaded label cards are required")
    package = target.get("evidence_package")
    if not isinstance(package, Mapping):
        raise SellingPointOfflineError("target has no evidence package")
    if package.get("evidence_level") == "V0":
        raise SellingPointOfflineError("V0 rows must not generate a model prompt")
    priority = hard_priority(package, cards)
    # The first bake-off showed that two near-duplicate examples for a broad
    # label can hide the actual boundary label.  Keep the plan's four-example
    # ceiling while making the precedents label-diverse.
    examples = index.select(target, max_per_label=1)
    system_payload = {
        "role": "你是懂车帝内容唯一主卖点分类器。",
        "task": "在28个闭集标签中返回唯一Top1，并给出最多Top3和可回指的原文证据。",
        "rules": [
            "先执行P0-P4，植入任务高于视频泛主题。",
            "P1硬触发只读取title/body/ASR中的办理动作、搜索口令或平台承接证据；纯话题标签、OCR导航卡和顺带提及不覆盖实际主任务。",
            "P2只是语义倾向，不收窄候选：车况和价格词共现不直接判E2，必须判断平台购买或履约保障是否构成主要植入任务。",
            "similar_gold_examples是已裁决先例；产品入口、内容形态和任务链路相同的先例优先于宽泛定义，但不得照抄仅有泛词重合的样例。",
            "P3要求懂车帝锚点附近存在明确演示、CTA或反复任务；不能因OCR菜单里偶然出现功能词就覆盖主任务。",
            "primary_code及top3只能来自allowed_codes，禁止自造代码。",
            "anchor_quote必须逐字来自指定channel的证据文本，不得改写。",
            "anchor_quote优先从quote_options原样选择；必须是连续短句，不得替换标点、拼接或使用省略号。",
            "多任务按明确CTA或搜索口令、锚点任务重复次数、首次出现顺序裁决。",
            "命中boundary_checklist中的相邻标签且证据不能排除其一时，必须把两者放在top3前两位且置信度差不超过0.10，交给二判。",
            "只输出一个JSON对象。",
        ],
        "boundary_checklist": [
            "E2只在平台购车保障/透明车况/价格保障是主承诺时成立；专业检测步骤或报告主任务归E5，合同过户交付归E7，教程问答归E8。",
            "E3是按预算行情筛选高性价比二手车；E4必须有具体用车场景方案；E6是估价、差价、保值率或市场行情查询。",
            "X1是跨车型的全面横评或多维综合比较；X5是车型版本、配置项或配置价值比较；3D看车和外观内饰细节为首要演示时归X9。",
            "X2依赖权威榜单、第三方测评或真实车主口碑；X7依赖懂车帝原创实验/统一标准实测来验证宣称。",
            "为购车决策被动查看真实车主长期用车体验归X2；新车实用知识或用车问题解答归X11；车友社区发帖互动归M8。",
            "X3是车价、落地价、优惠和现车；X6是购车或长期用车成本计算；政府补贴只有作为内容主任务或实际植入权益任务时归X8。",
            "出现AI小懂时只在M1-M6中按具体任务判；无具体任务归M2。",
        ],
        "priority_rules": labels["priority_rules"],
        "cards": cards_for_prompt(labels),
        "output_schema": {
            "primary_code": "E1..E10/X1..X11/M1..M6/M8",
            "confidence": "0..1",
            "top3": [{"code": "closed-set code", "confidence": "0..1"}],
            "channel": "title|body|asr|ocr",
            "anchor_quote": "优先逐字复制quote_options中的6..48字连续短句",
            "reason": "简短边界理由",
        },
    }
    user_payload = {
        "allowed_codes": priority["allowed_codes"],
        "hard_priority": priority,
        "anchor_windows": [
            {
                "channel": window.get("channel"),
                "priority": window.get("priority"),
                "text": window.get("text"),
                "anchors": [
                    {
                        "group": anchor.get("group"),
                        "term": anchor.get("term"),
                    }
                    for anchor in window.get("anchors", [])
                    if isinstance(anchor, Mapping)
                ],
            }
            for window in package["anchor_windows"]
            if isinstance(window, Mapping)
        ],
        "channels": package["channels"],
        "quote_options": prompt_quote_options(target),
        "similar_gold_examples": examples,
    }
    prompt_identity = {
        "contract": PROMPT_CONTRACT_VERSION,
        "hard_priority_contract": HARD_PRIORITY_VERSION,
        "label_cards_sha256": labels["source_sha256"],
        "evidence_config_sha256": package["config_sha256"],
        "retrieval_index_sha256": index.index_sha256,
    }
    return {
        "prompt_version": f"{PROMPT_CONTRACT_VERSION}-{sha256_json(prompt_identity)[:16]}",
        "prompt_identity": prompt_identity,
        "priority": priority,
        "examples": examples,
        "system": canonical_json(system_payload),
        "user": canonical_json(user_payload),
    }


def parse_model_json(raw: str) -> dict[str, Any]:
    text = _JSON_FENCE_RE.sub("", str(raw or "").strip()).strip()
    try:
        value = json.loads(text)
    except json.JSONDecodeError as error:
        match = re.search(r"\{.*\}", text, re.DOTALL)
        if match is None:
            raise SellingPointOfflineError("model response is not JSON") from error
        try:
            value = json.loads(match.group(0))
        except json.JSONDecodeError as nested:
            raise SellingPointOfflineError("model response is not JSON") from nested
    if not isinstance(value, dict):
        raise SellingPointOfflineError("model response must be a JSON object")
    return value


def _confidence(value: Any, *, field: str) -> float:
    try:
        number = float(value)
    except (TypeError, ValueError) as error:
        raise SellingPointOfflineError(f"{field} must be a number") from error
    if not 0.0 <= number <= 1.0 or not math.isfinite(number):
        raise SellingPointOfflineError(f"{field} must be in 0..1")
    return number


def _mapped_original_span(
    span_map: Sequence[Any], start: int, end: int
) -> tuple[int, int] | None:
    selected = span_map[start:end]
    if len(selected) != end - start or not selected:
        return None
    spans: list[tuple[int, int]] = []
    for item in selected:
        if (
            not isinstance(item, Sequence)
            or isinstance(item, (str, bytes))
            or len(item) != 2
        ):
            return None
        try:
            spans.append((int(item[0]), int(item[1])))
        except (TypeError, ValueError):
            return None
    previous_start, previous_end = spans[0]
    for current_start, current_end in spans[1:]:
        if current_start < previous_start or current_start > previous_end:
            return None
        previous_start = current_start
        previous_end = max(previous_end, current_end)
    return min(item[0] for item in spans), max(item[1] for item in spans)


def _find_all(text: str, quote: str) -> list[tuple[int, int]]:
    output: list[tuple[int, int]] = []
    cursor = 0
    while quote and cursor <= len(text) - len(quote):
        start = text.find(quote, cursor)
        if start < 0:
            break
        output.append((start, start + len(quote)))
        cursor = start + 1
    return output


def map_quote_to_original(
    target: Mapping[str, Any], *, channel: str, quote: str
) -> dict[str, Any]:
    package = target.get("evidence_package")
    originals = target.get("original_channels")
    if not isinstance(package, Mapping) or not isinstance(originals, Mapping):
        raise SellingPointOfflineError("target has no quote mapping evidence")
    channels = package.get("channels")
    span_maps = package.get("span_maps")
    windows = package.get("anchor_windows")
    if (
        channel not in CHANNELS
        or not isinstance(channels, Mapping)
        or not isinstance(span_maps, Mapping)
        or not isinstance(windows, list)
    ):
        return {"status": "degraded_quote", "reason": "channel-invalid"}
    original = str(originals.get(channel) or "")
    mapped_spans: set[tuple[int, int]] = set()

    channel_text = str(channels.get(channel) or "")
    channel_map = span_maps.get(channel)
    if isinstance(channel_map, list):
        for start, end in _find_all(channel_text, quote):
            span = _mapped_original_span(channel_map, start, end)
            if span is not None:
                mapped_spans.add(span)
    for window in windows:
        if not isinstance(window, Mapping) or window.get("channel") != channel:
            continue
        window_text = str(window.get("text") or "")
        window_map = window.get("span_map")
        if not isinstance(window_map, list):
            continue
        for start, end in _find_all(window_text, quote):
            span = _mapped_original_span(window_map, start, end)
            if span is not None:
                mapped_spans.add(span)
    if len(mapped_spans) != 1:
        return {
            "status": "degraded_quote",
            "reason": "quote-not-found" if not mapped_spans else "quote-ambiguous",
        }
    start, end = next(iter(mapped_spans))
    if not 0 <= start < end <= len(original):
        return {"status": "degraded_quote", "reason": "original-span-invalid"}
    return {
        "status": "accepted",
        "reason": None,
        "original_start": start,
        "original_end": end,
        "original_quote": original[start:end],
    }


def validate_model_response(
    parsed: Mapping[str, Any],
    *,
    target: Mapping[str, Any],
    priority: Mapping[str, Any],
    valid_codes: Iterable[str],
) -> dict[str, Any]:
    all_codes = set(valid_codes)
    allowed_codes = set(str(item) for item in priority.get("allowed_codes", []))
    primary = str(parsed.get("primary_code") or "")
    if primary not in all_codes or primary not in allowed_codes:
        raise SellingPointOfflineError("primary_code violates the allowed closed set")
    forced_code = priority.get("forced_code")
    if forced_code is not None and primary != forced_code:
        raise SellingPointOfflineError("primary_code conflicts with the hard priority")
    confidence = _confidence(parsed.get("confidence"), field="confidence")
    raw_top3 = parsed.get("top3")
    if not isinstance(raw_top3, list) or not 1 <= len(raw_top3) <= 3:
        raise SellingPointOfflineError("top3 must contain 1..3 entries")
    top3: list[dict[str, Any]] = []
    seen: set[str] = set()
    for index, item in enumerate(raw_top3):
        if not isinstance(item, Mapping):
            raise SellingPointOfflineError("top3 entries must be objects")
        code = str(item.get("code") or "")
        if code not in all_codes or code not in allowed_codes or code in seen:
            raise SellingPointOfflineError("top3 violates the allowed unique closed set")
        seen.add(code)
        top3.append(
            {
                "code": code,
                "confidence": _confidence(
                    item.get("confidence"), field=f"top3[{index}].confidence"
                ),
            }
        )
    if top3[0]["code"] != primary:
        raise SellingPointOfflineError("top3[0] must equal primary_code")
    if abs(float(top3[0]["confidence"]) - confidence) > 1e-9:
        raise SellingPointOfflineError("top3[0] confidence must equal confidence")
    if any(
        float(top3[index]["confidence"])
        < float(top3[index + 1]["confidence"])
        for index in range(len(top3) - 1)
    ):
        raise SellingPointOfflineError("top3 confidence must be non-increasing")
    channel = str(parsed.get("channel") or "")
    quote = str(parsed.get("anchor_quote") or "").strip()
    if channel not in CHANNELS or not 2 <= len(quote) <= 120:
        raise SellingPointOfflineError("anchor quote channel/length is invalid")
    quote_mapping = map_quote_to_original(target, channel=channel, quote=quote)
    reason = str(parsed.get("reason") or "").strip()
    if not reason or len(reason) > 1000:
        raise SellingPointOfflineError("reason must contain 1..1000 characters")
    return {
        "status": quote_mapping["status"],
        "primary_code": primary,
        "confidence": confidence,
        "top3": top3,
        "channel": channel,
        "anchor_quote": quote,
        "original_quote": quote_mapping.get("original_quote"),
        "original_start": quote_mapping.get("original_start"),
        "original_end": quote_mapping.get("original_end"),
        "quote_reason": quote_mapping.get("reason"),
        "reason": reason,
        "hard_priority": str(priority.get("priority") or "P4"),
    }


def second_call_reason(decision: Mapping[str, Any]) -> str | None:
    if decision.get("status") != "accepted":
        return "structure_or_quote_repair"
    top3 = decision.get("top3")
    if isinstance(top3, list) and len(top3) >= 2:
        difference = float(top3[0]["confidence"]) - float(top3[1]["confidence"])
        if difference < 0.15:
            return "top2_boundary_second_pass"
    return None
