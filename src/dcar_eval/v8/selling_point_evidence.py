"""Versioned evidence package for selling-point semantic evaluation.

The module is intentionally pure and database-free so the offline bake-off and
the v10 runtime can share exactly the same normalization and truncation rules.
"""

from __future__ import annotations

import hashlib
import json
import re
import string
import unicodedata
from dataclasses import dataclass
from difflib import SequenceMatcher
from pathlib import Path
from typing import Any, Callable, Mapping, Sequence


PROJECT_ROOT = Path(__file__).resolve().parents[3]
DEFAULT_CONFIG_PATH = (
    PROJECT_ROOT / "config" / "selling_point_evidence_package_v2.json"
)
CHANNELS = ("title", "body", "asr", "ocr")
EVIDENCE_LEVELS = {"V0", "V1", "V2", "V3"}
_EDGE_PUNCTUATION = string.punctuation + "，。！？；：、‘’“”【】（）《》〈〉…—·"


class SellingPointEvidenceError(ValueError):
    """Raised when the evidence-package contract is invalid."""


@dataclass(frozen=True)
class MappedText:
    """Normalized text with a per-character mapping to its original source."""

    original: str
    text: str
    spans: tuple[tuple[int, int], ...]

    def original_span(self, start: int, end: int) -> tuple[int, int] | None:
        if start < 0 or end <= start or end > len(self.spans):
            return None
        selected = self.spans[start:end]
        if not selected:
            return None
        previous_start, previous_end = selected[0]
        for current_start, current_end in selected[1:]:
            if current_start < previous_start or current_start > previous_end:
                return None
            previous_start = current_start
            previous_end = max(previous_end, current_end)
        return min(item[0] for item in selected), max(item[1] for item in selected)

    def original_quote(self, start: int, end: int) -> str | None:
        span = self.original_span(start, end)
        if span is None:
            return None
        return self.original[span[0] : span[1]]


def _sha256(payload: bytes) -> str:
    return hashlib.sha256(payload).hexdigest()


def _canonical_json(value: Any) -> str:
    return json.dumps(
        value,
        ensure_ascii=False,
        separators=(",", ":"),
        sort_keys=True,
    )


def _ordered_replacements_sha(value: Any) -> str:
    payload = json.dumps(
        value,
        ensure_ascii=False,
        separators=(",", ":"),
    ) + "\n"
    return _sha256(payload.encode("utf-8"))


def load_evidence_config(
    path: Path = DEFAULT_CONFIG_PATH,
    *,
    verify_source: bool = True,
) -> dict[str, Any]:
    """Load and validate the frozen evidence-package-v2 configuration."""

    try:
        payload = path.resolve().read_bytes()
        value = json.loads(payload.decode("utf-8"))
    except (OSError, UnicodeDecodeError, json.JSONDecodeError) as error:
        raise SellingPointEvidenceError(f"cannot load evidence config: {error}") from error
    if not isinstance(value, dict) or value.get("version") != "evidence-package-v2":
        raise SellingPointEvidenceError("unsupported evidence config version")
    replacements = value.get("base_ordered_replacements")
    if not isinstance(replacements, list) or len(replacements) != 15:
        raise SellingPointEvidenceError("base replacements must contain 15 pairs")
    for item in replacements:
        if (
            not isinstance(item, list)
            or len(item) != 2
            or not all(isinstance(part, str) and part for part in item)
        ):
            raise SellingPointEvidenceError("base replacement entries must be pairs")
    expected_replacements_sha = str(
        value.get("source_ordered_replacements_sha256") or ""
    )
    if _ordered_replacements_sha(replacements) != expected_replacements_sha:
        raise SellingPointEvidenceError("frozen replacement hash does not match config")

    additional = value.get("additional_replacements")
    if not isinstance(additional, list) or [item.get("order") for item in additional] != [
        16,
        17,
        18,
    ]:
        raise SellingPointEvidenceError("additional replacements must be orders 16..18")
    if additional[0] != {
        "order": 16,
        "source": "AI小董",
        "target": "AI小懂",
        "mode": "unconditional",
    }:
        raise SellingPointEvidenceError("AI小董 must be the unconditional order 16")
    if replacements[3] != ["懂车地", "懂车帝"]:
        raise SellingPointEvidenceError("懂车地 must remain an unconditional base rule")

    limits = value.get("channel_limits")
    if limits != {"title": 200, "body": 1500, "asr": 1800, "ocr": 3000}:
        raise SellingPointEvidenceError("channel limits do not match v2 contract")
    if value.get("anchor_window_chars") != 120:
        raise SellingPointEvidenceError("anchor window must be 120 characters")
    if value.get("max_anchor_windows") != 4 or value.get("max_keyframes") != 6:
        raise SellingPointEvidenceError("anchor/keyframe limits do not match v2")

    if verify_source:
        source = PROJECT_ROOT / str(value.get("source_matcher_bundle") or "")
        try:
            source_payload = source.resolve().read_bytes()
            source_value = json.loads(source_payload.decode("utf-8"))
        except (OSError, UnicodeDecodeError, json.JSONDecodeError) as error:
            raise SellingPointEvidenceError(
                f"cannot verify matcher normalization source: {error}"
            ) from error
        if _sha256(source_payload) != value.get("source_matcher_bundle_sha256"):
            raise SellingPointEvidenceError("matcher bundle hash drifted")
        source_replacements = source_value.get("normalization", {}).get(
            "ordered_replacements"
        )
        if source_replacements != replacements:
            raise SellingPointEvidenceError("matcher replacements drifted")
    value["config_sha256"] = _sha256(payload)
    return value


def _unicode_whitespace_map(text: str) -> MappedText:
    output: list[str] = []
    spans: list[tuple[int, int]] = []
    for index, character in enumerate(text):
        normalized = unicodedata.normalize("NFKC", character)
        for item in normalized:
            if item.isspace():
                if output and output[-1] == " ":
                    spans[-1] = (spans[-1][0], index + 1)
                else:
                    output.append(" ")
                    spans.append((index, index + 1))
            else:
                output.append(item)
                spans.append((index, index + 1))
    while output and output[0] == " ":
        output.pop(0)
        spans.pop(0)
    while output and output[-1] == " ":
        output.pop()
        spans.pop()
    return MappedText(text, "".join(output), tuple(spans))


def _replace_mapped(
    mapped: MappedText,
    source: str,
    target: str,
    *,
    predicate: Callable[[str, int, int], bool] | None = None,
) -> MappedText:
    if not source or source not in mapped.text:
        return mapped
    output: list[str] = []
    spans: list[tuple[int, int]] = []
    cursor = 0
    while cursor < len(mapped.text):
        found = mapped.text.find(source, cursor)
        if found < 0:
            output.extend(mapped.text[cursor:])
            spans.extend(mapped.spans[cursor:])
            break
        end = found + len(source)
        output.extend(mapped.text[cursor:found])
        spans.extend(mapped.spans[cursor:found])
        if predicate is not None and not predicate(mapped.text, found, end):
            output.extend(mapped.text[found:end])
            spans.extend(mapped.spans[found:end])
            cursor = end
            continue
        selected = mapped.spans[found:end]
        original_span = (
            min(item[0] for item in selected),
            max(item[1] for item in selected),
        )
        output.extend(target)
        spans.extend([original_span] * len(target))
        cursor = end
    return MappedText(mapped.original, "".join(output), tuple(spans))


def _context_predicate(
    *,
    window: int,
    terms: Sequence[str],
) -> Callable[[str, int, int], bool]:
    folded_terms = tuple(term.casefold() for term in terms)

    def matches(text: str, start: int, end: int) -> bool:
        context = text[max(0, start - window) : min(len(text), end + window)]
        folded = context.casefold()
        return any(term in folded for term in folded_terms)

    return matches


def normalize_text(text: str, config: Mapping[str, Any]) -> MappedText:
    """Apply normalization-v2 while preserving a quote-to-original span map."""

    mapped = _unicode_whitespace_map(str(text or ""))
    for source, target in config["base_ordered_replacements"]:
        mapped = _replace_mapped(mapped, source, target)
    for rule in config["additional_replacements"]:
        predicate = None
        if rule["mode"] == "context":
            predicate = _context_predicate(
                window=int(rule["context_window_chars"]),
                terms=tuple(rule["context_terms"]),
            )
        elif rule["mode"] != "unconditional":
            raise SellingPointEvidenceError(
                f"unsupported replacement mode: {rule['mode']}"
            )
        mapped = _replace_mapped(
            mapped,
            str(rule["source"]),
            str(rule["target"]),
            predicate=predicate,
        )
    return mapped


def _comparison_text(text: str) -> str:
    value = unicodedata.normalize("NFKC", text)
    value = re.sub(r"\s+", " ", value).strip()
    return value.strip(_EDGE_PUNCTUATION)


def _offset_mapped(mapped: MappedText, *, offset: int, original: str) -> MappedText:
    return MappedText(
        original,
        mapped.text,
        tuple((start + offset, end + offset) for start, end in mapped.spans),
    )


def _join_mapped(items: Sequence[MappedText], *, original: str) -> MappedText:
    output: list[str] = []
    spans: list[tuple[int, int]] = []
    for index, item in enumerate(items):
        if index:
            left = spans[-1][1] if spans else 0
            right = item.spans[0][0] if item.spans else left
            output.append("\n")
            spans.append((left, max(left, right)))
        output.extend(item.text)
        spans.extend(item.spans)
    return MappedText(original, "".join(output), tuple(spans))


def dedupe_ocr_observations(
    observations: Sequence[str],
    config: Mapping[str, Any],
) -> MappedText:
    """Normalize OCR and remove only adjacent near-duplicate observations."""

    raw = [str(item or "") for item in observations]
    original = "\n".join(raw)
    mapped_items: list[MappedText] = []
    offset = 0
    for item in raw:
        mapped = _offset_mapped(normalize_text(item, config), offset=offset, original=original)
        offset += len(item) + 1
        if not mapped.text:
            continue
        if mapped_items:
            previous = mapped_items[-1]
            previous_compare = _comparison_text(previous.text)
            current_compare = _comparison_text(mapped.text)
            similar = bool(previous_compare and current_compare) and (
                previous_compare == current_compare
                or previous_compare in current_compare
                or current_compare in previous_compare
                or SequenceMatcher(None, previous_compare, current_compare).ratio()
                >= float(config["ocr_dedupe"]["sequence_matcher_threshold"])
            )
            if similar:
                if len(mapped.text) > len(previous.text):
                    mapped_items[-1] = mapped
                continue
        mapped_items.append(mapped)
    return _join_mapped(mapped_items, original=original)


def _find_occurrences(text: str, term: str) -> list[tuple[int, int]]:
    output: list[tuple[int, int]] = []
    cursor = 0
    while term and cursor < len(text):
        start = text.find(term, cursor)
        if start < 0:
            break
        output.append((start, start + len(term)))
        cursor = start + len(term)
    return output


def _anchor_matches(
    channels: Mapping[str, MappedText],
    config: Mapping[str, Any],
) -> list[dict[str, Any]]:
    matches: list[dict[str, Any]] = []
    channel_order = {name: index for index, name in enumerate(config["channel_order"])}
    radius = int(config["anchor_window_chars"])
    for group in config["anchor_groups"]:
        for channel, mapped in channels.items():
            for term in group["terms"]:
                for start, end in _find_occurrences(mapped.text, str(term)):
                    original_span = mapped.original_span(start, end)
                    original_quote = mapped.original_quote(start, end)
                    matches.append(
                        {
                            "priority": int(group["priority"]),
                            "group": str(group["name"]),
                            "channel": channel,
                            "channel_order": channel_order[channel],
                            "term": str(term),
                            "anchor_start": start,
                            "anchor_end": end,
                            "original_start": (
                                original_span[0] if original_span is not None else None
                            ),
                            "original_end": (
                                original_span[1] if original_span is not None else None
                            ),
                            "start": max(0, start - radius),
                            "end": min(len(mapped.text), end + radius),
                            "anchor_quote": original_quote,
                        }
                    )
    return matches


def _merge_anchor_windows(
    matches: Sequence[Mapping[str, Any]],
    channels: Mapping[str, MappedText],
    config: Mapping[str, Any],
) -> list[dict[str, Any]]:
    by_channel: dict[str, list[Mapping[str, Any]]] = {name: [] for name in CHANNELS}
    for match in matches:
        by_channel[str(match["channel"])].append(match)
    merged: list[dict[str, Any]] = []
    for channel in config["channel_order"]:
        ordered = sorted(
            by_channel[channel],
            key=lambda item: (int(item["start"]), int(item["end"]), int(item["priority"])),
        )
        for item in ordered:
            if merged and merged[-1]["channel"] == channel and int(item["start"]) <= int(
                merged[-1]["end"]
            ):
                merged[-1]["end"] = max(int(merged[-1]["end"]), int(item["end"]))
                merged[-1]["priority"] = min(
                    int(merged[-1]["priority"]), int(item["priority"])
                )
                merged[-1]["anchors"].append(dict(item))
            else:
                merged.append(
                    {
                        "channel": channel,
                        "channel_order": int(item["channel_order"]),
                        "priority": int(item["priority"]),
                        "start": int(item["start"]),
                        "end": int(item["end"]),
                        "anchors": [dict(item)],
                    }
                )
    merged.sort(
        key=lambda item: (
            int(item["priority"]),
            int(item["channel_order"]),
            int(item["start"]),
        )
    )
    output: list[dict[str, Any]] = []
    for item in merged[: int(config["max_anchor_windows"])]:
        mapped = channels[str(item["channel"])]
        start = int(item["start"])
        end = int(item["end"])
        original_span = mapped.original_span(start, end)
        output.append(
            {
                "channel": item["channel"],
                "priority": item["priority"],
                "start": start,
                "end": end,
                "text": mapped.text[start:end],
                "original_quote": mapped.original_quote(start, end),
                "original_start": (
                    original_span[0] if original_span is not None else None
                ),
                "original_end": (
                    original_span[1] if original_span is not None else None
                ),
                "span_map": [list(span) for span in mapped.spans[start:end]],
                "anchors": [
                    {
                        key: anchor[key]
                        for key in (
                            "group",
                            "term",
                            "anchor_quote",
                            "anchor_start",
                            "anchor_end",
                            "original_start",
                            "original_end",
                        )
                    }
                    for anchor in item["anchors"]
                ],
            }
        )
    return output


def build_evidence_package(
    *,
    title: str = "",
    body: str = "",
    asr: str = "",
    ocr_observations: Sequence[str] = (),
    ocr_combined: str = "",
    keyframes: Sequence[str] = (),
    evidence_level: str,
    evidence_sha256: str,
    config: Mapping[str, Any] | None = None,
) -> dict[str, Any]:
    """Build one deterministic evidence-package-v2 payload."""

    if evidence_level not in EVIDENCE_LEVELS:
        raise SellingPointEvidenceError(f"invalid evidence level: {evidence_level}")
    resolved = dict(config or load_evidence_config())
    base = {
        "version": str(resolved["version"]),
        "normalization_version": str(resolved["normalization_version"]),
        "config_sha256": str(resolved["config_sha256"]),
        "evidence_level": evidence_level,
        "evidence_sha256": str(evidence_sha256),
    }
    package: dict[str, Any]
    if evidence_level == "V0":
        package = {
            **base,
            "channels": {},
            "span_maps": {},
            "anchor_windows": [],
            "keyframes": [],
        }
        package["package_sha256"] = _sha256(_canonical_json(package).encode("utf-8"))
        return package

    channels = {
        "title": normalize_text(title, resolved),
        "body": normalize_text(body, resolved),
        "asr": normalize_text(asr, resolved),
    }
    ocr_source = list(ocr_observations) or ([ocr_combined] if ocr_combined else [])
    channels["ocr"] = dedupe_ocr_observations(ocr_source, resolved)
    matches = _anchor_matches(channels, resolved)
    windows = _merge_anchor_windows(matches, channels, resolved)
    limits = resolved["channel_limits"]
    package = {
        **base,
        "channels": {
            channel: channels[channel].text[: int(limits[channel])]
            for channel in CHANNELS
        },
        "span_maps": {
            channel: [
                list(span)
                for span in channels[channel].spans[: int(limits[channel])]
            ]
            for channel in CHANNELS
        },
        "anchor_windows": windows,
        "keyframes": [str(item) for item in keyframes[: int(resolved["max_keyframes"])]],
    }
    package["package_sha256"] = _sha256(_canonical_json(package).encode("utf-8"))
    return package
