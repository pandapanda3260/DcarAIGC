"""Validated v5.3 selling-point label cards for offline and runtime prompts."""

from __future__ import annotations

import hashlib
import json
from pathlib import Path
from typing import Any, Mapping

from .matcher_dsl import V5_2_POINT_SPEC


PROJECT_ROOT = Path(__file__).resolve().parents[3]
DEFAULT_LABEL_CARD_PATH = (
    PROJECT_ROOT / "config" / "business_selling_points_v5_3.json"
)
_SCENE_NAMES = {"二手车": "used_car", "新车": "new_car", "媒体-AI小懂": "media"}
_CARD_KEYS = {
    "id",
    "tier",
    "label",
    "definition",
    "business_scene",
    "positive_evidence",
    "negative_evidence",
    "boundary_rules",
}


class SellingPointLabelCardError(ValueError):
    """Raised when the v5.3 label-card source violates its contract."""


def _validate_examples(value: Any, *, label: str) -> list[str]:
    if not isinstance(value, list) or not 1 <= len(value) <= 100:
        raise SellingPointLabelCardError(f"{label} must contain 1..100 items")
    output: list[str] = []
    for item in value:
        if not isinstance(item, str) or item != item.strip() or not item:
            raise SellingPointLabelCardError(f"{label} items must be trimmed strings")
        if len(item) > 500:
            raise SellingPointLabelCardError(f"{label} items must not exceed 500 chars")
        output.append(item)
    if len(output) != len(set(output)):
        raise SellingPointLabelCardError(f"{label} items must be unique")
    return output

def load_label_cards(
    path: Path = DEFAULT_LABEL_CARD_PATH,
) -> dict[str, Any]:
    """Load the complete v5.3 standard and return cards keyed by code."""

    try:
        payload = path.resolve().read_bytes()
        source = json.loads(payload.decode("utf-8"))
    except (OSError, UnicodeDecodeError, json.JSONDecodeError) as error:
        raise SellingPointLabelCardError(f"cannot load label cards: {error}") from error
    if not isinstance(source, dict):
        raise SellingPointLabelCardError("label-card source must be an object")
    if source.get("database_taxonomy_version") != "selling-points-v5.3":
        raise SellingPointLabelCardError("label cards must target selling-points-v5.3")
    if source.get("base_database_taxonomy_version") != "selling-points-v5.2":
        raise SellingPointLabelCardError("label cards must extend selling-points-v5.2")
    if not str(source.get("definition") or "").strip():
        raise SellingPointLabelCardError("taxonomy definition is required")

    priorities = source.get("priority_rules")
    if not isinstance(priorities, list) or [item.get("id") for item in priorities] != [
        "P0",
        "P1",
        "P2",
        "P3",
        "P4",
    ]:
        raise SellingPointLabelCardError("priority rules must be ordered P0..P4")
    if any(not str(item.get("rule") or "").strip() for item in priorities):
        raise SellingPointLabelCardError("priority rules must be non-empty")

    values = source.get("labels")
    if not isinstance(values, list):
        raise SellingPointLabelCardError("labels must be a list")
    cards: dict[str, dict[str, Any]] = {}
    for index, raw in enumerate(values):
        if not isinstance(raw, dict) or set(raw) != _CARD_KEYS:
            raise SellingPointLabelCardError(
                f"label card {index} must contain exactly {sorted(_CARD_KEYS)}"
            )
        code = str(raw["id"])
        if code in cards:
            raise SellingPointLabelCardError(f"duplicate label card: {code}")
        scene = _SCENE_NAMES.get(str(raw["business_scene"]))
        if scene is None or {scene} != set(V5_2_POINT_SPEC.get(code, set())):
            raise SellingPointLabelCardError(f"scene does not match point spec: {code}")
        if raw["tier"] not in {"core", "other"}:
            raise SellingPointLabelCardError(f"invalid tier for {code}")
        if not str(raw["label"]).strip() or not str(raw["definition"]).strip():
            raise SellingPointLabelCardError(f"label and definition are required: {code}")
        card = dict(raw)
        for key in ("positive_evidence", "negative_evidence", "boundary_rules"):
            card[key] = _validate_examples(card[key], label=f"{code}.{key}")
        card["scene"] = scene
        cards[code] = card
    if set(cards) != set(V5_2_POINT_SPEC):
        raise SellingPointLabelCardError("label cards do not match the 28 point codes")

    return {
        "taxonomy_version": "selling-points-v5.3",
        "definition": str(source["definition"]),
        "priority_rules": [dict(item) for item in priorities],
        "cards": cards,
        "source_sha256": hashlib.sha256(payload).hexdigest(),
        "source_gold_sha256": str(source.get("source_gold_sha256") or ""),
    }


def cards_for_prompt(value: Mapping[str, Any]) -> list[dict[str, Any]]:
    """Return a deterministic, prompt-safe projection of all 28 cards."""

    cards = value.get("cards")
    if not isinstance(cards, Mapping):
        raise SellingPointLabelCardError("loaded cards mapping is required")
    output: list[dict[str, Any]] = []
    for code in sorted(cards):
        raw = cards[code]
        if not isinstance(raw, Mapping):
            raise SellingPointLabelCardError(f"invalid loaded card: {code}")
        output.append(
            {
                "code": code,
                "label": raw["label"],
                "definition": raw["definition"],
                "positive_evidence": list(raw["positive_evidence"]),
                "negative_evidence": list(raw["negative_evidence"]),
                "boundary_rules": list(raw["boundary_rules"]),
            }
        )
    return output
