"""Import a frozen Dongchedi trim catalog into the existing SPU domain.

The importer deliberately owns only concrete trim rows and trim aliases.  It
does not create series nodes, delete rows, or change SPU-to-audience mappings.
The normalized input is therefore replayable against any schema-v14 database
that contains the same series nodes.

Safety properties:

* dry-run is the default and uses a WAL-aware read-only connection;
* every identity or natural-key collision fails closed;
* apply requires the exact plan SHA-256 returned by dry-run;
* all database changes are made in one ``BEGIN IMMEDIATE`` transaction;
* a validated SQLite online backup is taken before writes, unless an explicit
  candidate-database ``skip_backup`` is requested;
* ``skip_backup`` is never accepted for the canonical ``DEFAULT_DB``;
* aliases are inserted or explicitly updated, but never deleted merely because
  they are absent from a later input file.
"""

from __future__ import annotations

import hashlib
import json
import math
import os
import re
import sqlite3
import unicodedata
from contextlib import contextmanager
from dataclasses import dataclass
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Dict, Iterator, List, Mapping, Optional, Sequence, Tuple

from .storage import DEFAULT_DB, PROJECT_ROOT, SCHEMA_VERSION, connect, now_utc


INPUT_SCHEMA_VERSION = "dcar-dongchedi-spu-trims-normalized-v1"
SOURCE_ARTIFACT_SCHEMA = "dcar-dongchedi-trim-catalog-v1"
SOURCE_PROVIDER = "dongchedi"
SOURCE_ENDPOINT = "https://www.dongchedi.com/motor/car_page/m/v1/series_all_json/"
DEFAULT_MAPPING_PATH = PROJECT_ROOT / "config" / "dongchedi_spu_series_map_v1.json"
PLAN_SCHEMA_VERSION = "dcar-dongchedi-spu-trim-import-plan-v1"
RECEIPT_SCHEMA_VERSION = "dcar-dongchedi-spu-trim-import-receipt-v1"

_ALIAS_TYPES = frozenset({"official", "nickname", "slang", "model_code"})
_POWERTRAINS = frozenset({"ice", "hev", "phev", "erev", "ev"})
_SHA256_RE = re.compile(r"^[0-9a-f]{64}$")
_NUMERIC_ID_RE = re.compile(r"^[0-9]+$")
_SERIES_SLUG_RE = re.compile(r"^[a-z0-9][a-z0-9_-]*$")
_REQUIRED_DOMAIN_TABLES = frozenset(
    {
        "spu_catalog",
        "spu_alias",
        "spu_association_runs",
    }
)
_CATALOG_MUTABLE_FIELDS = (
    "trim_label",
    "model_year",
    "powertrain",
    "body_style",
    "price_low",
    "price_high",
    "enabled",
)
_ALIAS_MUTABLE_FIELDS = ("alias_type", "ambiguous", "enabled")


class SpuCatalogImportError(RuntimeError):
    """Raised when a normalized catalog cannot be planned or applied safely."""


@dataclass(frozen=True)
class AliasSpec:
    alias: str
    alias_type: str = "official"
    ambiguous: bool = False
    enabled: bool = True


@dataclass(frozen=True)
class TrimSpec:
    series_slug: str
    dcd_series_id: str
    car_id: str
    trim_label: str
    model_year: int
    powertrain: str
    body_style: str
    price_low: Optional[float]
    price_high: Optional[float]
    aliases: Tuple[AliasSpec, ...]
    source_bucket: str
    expected_brand: Optional[str]
    expected_series: Optional[str]

    @property
    def spu_id(self) -> str:
        return f"{self.series_slug}__dcd-{self.car_id}"

    @property
    def external_ref(self) -> str:
        return f"dongchedi:car:{self.car_id}"


@dataclass(frozen=True)
class FrozenCatalog:
    input_path: Path
    input_sha256: str
    catalog_sha256: str
    source_attestation: Mapping[str, Any]
    rows: Tuple[TrimSpec, ...]


@dataclass(frozen=True)
class ImportPlan:
    db_path: Path
    input_sha256: str
    database_state_sha256: str
    series_state_sha256: str
    health: Mapping[str, Any]
    source_buckets: Mapping[str, int]
    catalog_inserts: Tuple[Mapping[str, Any], ...]
    catalog_updates: Tuple[Mapping[str, Any], ...]
    alias_inserts: Tuple[Mapping[str, Any], ...]
    alias_updates: Tuple[Mapping[str, Any], ...]
    unchanged_catalog_rows: int
    unchanged_aliases: int
    warnings: Tuple[str, ...]
    plan_sha256: str

    @property
    def change_count(self) -> int:
        return (
            len(self.catalog_inserts)
            + len(self.catalog_updates)
            + len(self.alias_inserts)
            + len(self.alias_updates)
        )

    def operation_counts(self) -> Dict[str, int]:
        return {
            "catalog_insert": len(self.catalog_inserts),
            "catalog_update": len(self.catalog_updates),
            "catalog_unchanged": self.unchanged_catalog_rows,
            "alias_insert": len(self.alias_inserts),
            "alias_update": len(self.alias_updates),
            "alias_unchanged": self.unchanged_aliases,
            "total_changes": self.change_count,
        }

    def changed_ids(self) -> Dict[str, List[str]]:
        return {
            "catalog_insert": [str(row["spu_id"]) for row in self.catalog_inserts],
            "catalog_update": [str(row["spu_id"]) for row in self.catalog_updates],
            "alias_insert": [
                f"{row['spu_id']}::{row['alias']}" for row in self.alias_inserts
            ],
            "alias_update": [
                f"{row['spu_id']}::{row['alias']}" for row in self.alias_updates
            ],
        }


def _sha256_bytes(payload: bytes) -> str:
    return hashlib.sha256(payload).hexdigest()


def _sha256_file(path: Path) -> str:
    digest = hashlib.sha256()
    with path.open("rb") as handle:
        for chunk in iter(lambda: handle.read(1024 * 1024), b""):
            digest.update(chunk)
    return digest.hexdigest()


def _canonical_sha256(value: Any) -> str:
    encoded = json.dumps(
        value,
        ensure_ascii=False,
        sort_keys=True,
        separators=(",", ":"),
    ).encode("utf-8")
    return _sha256_bytes(encoded)


def _normalized_text_key(value: str) -> str:
    return unicodedata.normalize("NFKC", value).casefold().strip()


def _required_text(
    row: Mapping[str, Any], key: str, *, label: str, max_length: int
) -> str:
    value = row.get(key)
    if not isinstance(value, str):
        raise SpuCatalogImportError(f"{label}.{key} must be a string")
    normalized = value.strip()
    if not normalized:
        raise SpuCatalogImportError(f"{label}.{key} must not be blank")
    if len(normalized) > max_length:
        raise SpuCatalogImportError(f"{label}.{key} exceeds {max_length} characters")
    return normalized


def _numeric_identifier(value: Any, *, label: str) -> str:
    if isinstance(value, bool):
        raise SpuCatalogImportError(f"{label} must be a numeric identifier")
    if isinstance(value, int):
        normalized = str(value)
    elif isinstance(value, str):
        normalized = value.strip()
    else:
        raise SpuCatalogImportError(f"{label} must be a numeric identifier")
    if _NUMERIC_ID_RE.fullmatch(normalized) is None:
        raise SpuCatalogImportError(f"{label} must contain digits only")
    return normalized


def _model_year(value: Any, *, label: str) -> int:
    if isinstance(value, bool):
        raise SpuCatalogImportError(f"{label} must be an integer year")
    try:
        parsed = int(value)
    except (TypeError, ValueError) as exc:
        raise SpuCatalogImportError(f"{label} must be an integer year") from exc
    if isinstance(value, float) and not value.is_integer():
        raise SpuCatalogImportError(f"{label} must be an integer year")
    if not 1990 <= parsed <= 2100:
        raise SpuCatalogImportError(f"{label} must be between 1990 and 2100")
    return parsed


def _optional_price(value: Any, *, label: str) -> Optional[float]:
    if value is None:
        return None
    if isinstance(value, bool):
        raise SpuCatalogImportError(f"{label} must be a non-negative number or null")
    try:
        parsed = float(value)
    except (TypeError, ValueError) as exc:
        raise SpuCatalogImportError(
            f"{label} must be a non-negative number or null"
        ) from exc
    if not math.isfinite(parsed) or parsed < 0:
        raise SpuCatalogImportError(f"{label} must be finite and non-negative")
    return parsed


def _strict_bool(value: Any, *, label: str, default: bool) -> bool:
    if value is None:
        return default
    if not isinstance(value, bool):
        raise SpuCatalogImportError(f"{label} must be a boolean")
    return value


def _parse_aliases(
    value: Any, *, trim_label: str, row_label: str
) -> Tuple[AliasSpec, ...]:
    if not isinstance(value, list):
        raise SpuCatalogImportError(f"{row_label}.aliases must be a list")
    parsed: List[AliasSpec] = [AliasSpec(alias=trim_label)]
    for index, raw_alias in enumerate(value):
        alias_label = f"{row_label}.aliases[{index}]"
        if isinstance(raw_alias, str):
            alias_text = raw_alias.strip()
            alias_type = "official"
            ambiguous = False
            enabled = True
        elif isinstance(raw_alias, dict):
            alias_text = _required_text(
                raw_alias, "alias", label=alias_label, max_length=60
            )
            alias_type = str(raw_alias.get("alias_type") or "official").strip()
            ambiguous = _strict_bool(
                raw_alias.get("ambiguous"),
                label=f"{alias_label}.ambiguous",
                default=False,
            )
            enabled = _strict_bool(
                raw_alias.get("enabled"),
                label=f"{alias_label}.enabled",
                default=True,
            )
        else:
            raise SpuCatalogImportError(
                f"{alias_label} must be a string or an alias object"
            )
        if not alias_text:
            raise SpuCatalogImportError(f"{alias_label} must not be blank")
        if len(alias_text) > 60:
            raise SpuCatalogImportError(f"{alias_label} exceeds 60 characters")
        if alias_type not in _ALIAS_TYPES:
            raise SpuCatalogImportError(
                f"{alias_label}.alias_type is unsupported: {alias_type!r}"
            )
        parsed.append(
            AliasSpec(
                alias=alias_text,
                alias_type=alias_type,
                ambiguous=ambiguous,
                enabled=enabled,
            )
        )

    by_key: Dict[str, AliasSpec] = {}
    for alias_spec in parsed:
        key = _normalized_text_key(alias_spec.alias)
        previous = by_key.get(key)
        if previous is None:
            by_key[key] = alias_spec
            continue
        if (
            previous.alias_type != alias_spec.alias_type
            or previous.ambiguous != alias_spec.ambiguous
            or previous.enabled != alias_spec.enabled
        ):
            raise SpuCatalogImportError(
                f"{row_label} contains conflicting case-insensitive alias definitions: "
                f"{previous.alias!r} and {alias_spec.alias!r}"
            )
    return tuple(
        sorted(by_key.values(), key=lambda item: _normalized_text_key(item.alias))
    )


def _parse_row(raw: Any, *, index: int) -> TrimSpec:
    label = f"rows[{index}]"
    if not isinstance(raw, dict):
        raise SpuCatalogImportError(f"{label} must be an object")
    series_slug = _required_text(raw, "series_slug", label=label, max_length=120)
    if _SERIES_SLUG_RE.fullmatch(series_slug) is None:
        raise SpuCatalogImportError(f"{label}.series_slug has an invalid format")
    dcd_series_id = _numeric_identifier(
        raw.get("dcd_series_id"), label=f"{label}.dcd_series_id"
    )
    car_id = _numeric_identifier(raw.get("car_id"), label=f"{label}.car_id")
    trim_label = _required_text(raw, "trim_label", label=label, max_length=60)
    year = _model_year(raw.get("model_year"), label=f"{label}.model_year")
    powertrain = _required_text(raw, "powertrain", label=label, max_length=16)
    if powertrain not in _POWERTRAINS:
        raise SpuCatalogImportError(
            f"{label}.powertrain is unsupported: {powertrain!r}"
        )
    body_style = _required_text(raw, "body_style", label=label, max_length=16)
    price_low = _optional_price(raw.get("price_low"), label=f"{label}.price_low")
    price_high = _optional_price(raw.get("price_high"), label=f"{label}.price_high")
    if (price_low is None) != (price_high is None):
        raise SpuCatalogImportError(
            f"{label}.price_low and price_high must both be null or both be numbers"
        )
    if price_low is not None and price_high is not None and price_low > price_high:
        raise SpuCatalogImportError(f"{label}.price_low must not exceed price_high")
    source_bucket = _required_text(raw, "source_bucket", label=label, max_length=120)
    expected_brand = None
    if raw.get("brand") is not None:
        expected_brand = _required_text(raw, "brand", label=label, max_length=60)
    expected_series = None
    if raw.get("series") is not None:
        expected_series = _required_text(raw, "series", label=label, max_length=80)
    aliases = _parse_aliases(raw.get("aliases"), trim_label=trim_label, row_label=label)
    spec = TrimSpec(
        series_slug=series_slug,
        dcd_series_id=dcd_series_id,
        car_id=car_id,
        trim_label=trim_label,
        model_year=year,
        powertrain=powertrain,
        body_style=body_style,
        price_low=price_low,
        price_high=price_high,
        aliases=aliases,
        source_bucket=source_bucket,
        expected_brand=expected_brand,
        expected_series=expected_series,
    )
    if len(spec.spu_id) > 160:
        raise SpuCatalogImportError(f"{label} generates an spu_id longer than 160")
    return spec


def _artifact_integer(value: Any, *, label: str) -> int:
    if isinstance(value, bool) or not isinstance(value, int):
        raise SpuCatalogImportError(f"{label} must be an integer")
    return value


def _approved_mapping(path: Path) -> Dict[str, Any]:
    candidate = path.expanduser().resolve()
    if not candidate.is_file() or candidate.is_symlink():
        raise SpuCatalogImportError(
            f"approved mapping must be a regular non-symlink file: {candidate}"
        )
    raw = candidate.read_bytes()
    try:
        payload = json.loads(raw)
    except (UnicodeDecodeError, json.JSONDecodeError) as exc:
        raise SpuCatalogImportError(
            f"approved mapping is not valid UTF-8 JSON: {candidate}"
        ) from exc
    if not isinstance(payload, dict) or not isinstance(payload.get("entries"), dict):
        raise SpuCatalogImportError("approved mapping must contain an entries object")
    unresolved_raw = payload.get("unresolved", [])
    if not isinstance(unresolved_raw, list):
        raise SpuCatalogImportError("approved mapping unresolved must be a list")
    unresolved = {
        str(item.get("series_slug") or "")
        for item in unresolved_raw
        if isinstance(item, dict)
    }
    if "" in unresolved or len(unresolved) != len(unresolved_raw):
        raise SpuCatalogImportError("approved mapping has invalid unresolved entries")
    allowed_pairs: set[Tuple[str, str]] = set()
    for slug, entry in payload["entries"].items():
        if not isinstance(slug, str) or not isinstance(entry, dict):
            raise SpuCatalogImportError("approved mapping has an invalid entry")
        official_series = entry.get("official_series")
        if not isinstance(official_series, list):
            raise SpuCatalogImportError(
                f"approved mapping {slug!r} has no official_series list"
            )
        for source in official_series:
            if not isinstance(source, dict):
                raise SpuCatalogImportError(
                    f"approved mapping {slug!r} has an invalid official series"
                )
            if source.get("include_recent") is True:
                allowed_pairs.add(
                    (
                        slug,
                        _numeric_identifier(source.get("series_id"), label="series_id"),
                    )
                )
    configured = {str(slug) for slug in payload["entries"]}
    if not unresolved.issubset(configured):
        raise SpuCatalogImportError(
            "approved mapping unresolved references unknown slugs"
        )
    return {
        "path": str(candidate),
        "sha256": _sha256_bytes(raw),
        "configured_slugs": configured,
        "unresolved_slugs": unresolved,
        "allowed_pairs": allowed_pairs,
    }


def _validate_source_artifact(
    payload: Any,
    *,
    mapping_path: Optional[Path],
) -> Tuple[List[Any], str, Mapping[str, Any]]:
    if not isinstance(payload, dict):
        raise SpuCatalogImportError("normalized input must be an attested JSON object")
    if payload.get("schema") != SOURCE_ARTIFACT_SCHEMA:
        raise SpuCatalogImportError(
            f"unsupported source artifact schema: {payload.get('schema')!r}"
        )
    if payload.get("schema_version") != INPUT_SCHEMA_VERSION:
        raise SpuCatalogImportError(
            f"unsupported normalized input schema_version: {payload.get('schema_version')!r}"
        )
    declared_catalog_sha = str(payload.get("catalog_sha256") or "").lower()
    if _SHA256_RE.fullmatch(declared_catalog_sha) is None:
        raise SpuCatalogImportError("source artifact catalog_sha256 is invalid")
    hash_material = dict(payload)
    hash_material.pop("catalog_sha256", None)
    if _canonical_sha256(hash_material) != declared_catalog_sha:
        raise SpuCatalogImportError(
            "source artifact catalog_sha256 does not match content"
        )

    source = payload.get("source")
    summary = payload.get("summary")
    raw_rows = payload.get("rows")
    unresolved_raw = payload.get("unresolved")
    series_summaries = payload.get("series_summaries")
    if not isinstance(source, dict) or not isinstance(summary, dict):
        raise SpuCatalogImportError("source artifact lacks source/summary objects")
    if not isinstance(raw_rows, list) or not raw_rows:
        raise SpuCatalogImportError("normalized input rows must be a non-empty list")
    if not isinstance(unresolved_raw, list) or not isinstance(series_summaries, list):
        raise SpuCatalogImportError(
            "source artifact unresolved/series_summaries must be lists"
        )
    if (
        source.get("provider") != SOURCE_PROVIDER
        or source.get("endpoint") != SOURCE_ENDPOINT
    ):
        raise SpuCatalogImportError("source artifact provider/endpoint is not approved")
    if source.get("buckets_included") != ["online", "offline"]:
        raise SpuCatalogImportError("source artifact included buckets are not approved")
    if set(source.get("buckets_excluded") or []) != {"presale_car", "urban"}:
        raise SpuCatalogImportError("source artifact excluded buckets are not approved")
    if source.get("online_policy") != "all_through_max_model_year":
        raise SpuCatalogImportError("source artifact online policy is not approved")
    offline_min = _artifact_integer(
        source.get("offline_min_model_year"), label="source.offline_min_model_year"
    )
    max_year = _artifact_integer(
        source.get("max_model_year"), label="source.max_model_year"
    )
    current_year = datetime.now(timezone.utc).year
    if offline_min != 2024 or max_year != current_year:
        raise SpuCatalogImportError("source artifact model-year window is not approved")
    if source.get("city_name") != "北京":
        raise SpuCatalogImportError("source artifact city_name is not approved")
    mapping_sha = str(source.get("mapping_sha256") or "").lower()
    if _SHA256_RE.fullmatch(mapping_sha) is None:
        raise SpuCatalogImportError("source artifact mapping_sha256 is invalid")

    buckets: Dict[str, int] = {"online": 0, "offline": 0}
    row_slugs: set[str] = set()
    row_pairs: set[Tuple[str, str]] = set()
    for index, row in enumerate(raw_rows):
        if not isinstance(row, dict):
            raise SpuCatalogImportError(f"rows[{index}] must be an object")
        bucket = str(row.get("source_bucket") or "")
        if bucket not in buckets:
            raise SpuCatalogImportError(
                f"rows[{index}] has an unapproved source_bucket"
            )
        year = _model_year(row.get("model_year"), label=f"rows[{index}].model_year")
        if year > max_year or (bucket == "offline" and year < offline_min):
            raise SpuCatalogImportError(
                f"rows[{index}] falls outside the approved window"
            )
        slug = str(row.get("series_slug") or "")
        series_id = _numeric_identifier(
            row.get("dcd_series_id"), label=f"rows[{index}].dcd_series_id"
        )
        buckets[bucket] += 1
        row_slugs.add(slug)
        row_pairs.add((slug, series_id))

    unresolved_slugs: set[str] = set()
    for index, item in enumerate(unresolved_raw):
        if not isinstance(item, dict) or not str(item.get("series_slug") or ""):
            raise SpuCatalogImportError(f"unresolved[{index}] is invalid")
        unresolved_slugs.add(str(item["series_slug"]))
    if len(unresolved_slugs) != len(unresolved_raw) or row_slugs & unresolved_slugs:
        raise SpuCatalogImportError("resolved and unresolved source slugs overlap")

    summary_expectations = {
        "rows": len(raw_rows),
        "online_rows": buckets["online"],
        "offline_rows": buckets["offline"],
        "resolved_series_slugs": len(row_slugs),
        "unresolved_series_slugs": len(unresolved_slugs),
        "official_series_requests": len(series_summaries),
        "configured_series_slugs": len(row_slugs | unresolved_slugs),
    }
    for field, expected in summary_expectations.items():
        if _artifact_integer(summary.get(field), label=f"summary.{field}") != expected:
            raise SpuCatalogImportError(
                f"source artifact summary.{field} does not match content"
            )

    summary_pairs: set[Tuple[str, str]] = set()
    for index, item in enumerate(series_summaries):
        if not isinstance(item, dict):
            raise SpuCatalogImportError(f"series_summaries[{index}] is invalid")
        pair = (
            str(item.get("series_slug") or ""),
            _numeric_identifier(
                item.get("dcd_series_id"),
                label=f"series_summaries[{index}].dcd_series_id",
            ),
        )
        selected = _artifact_integer(
            item.get("online_selected"),
            label=f"series_summaries[{index}].online_selected",
        ) + _artifact_integer(
            item.get("offline_selected"),
            label=f"series_summaries[{index}].offline_selected",
        )
        response_sha = str(item.get("response_sha256") or "").lower()
        if not pair[0] or selected <= 0 or _SHA256_RE.fullmatch(response_sha) is None:
            raise SpuCatalogImportError(f"series_summaries[{index}] is incomplete")
        if pair in summary_pairs:
            raise SpuCatalogImportError(
                "source artifact repeats an official series summary"
            )
        summary_pairs.add(pair)
    if row_pairs != summary_pairs:
        raise SpuCatalogImportError(
            "source rows and official series summaries do not align"
        )

    approval = _approved_mapping(mapping_path) if mapping_path is not None else None
    if approval is not None:
        if mapping_sha != approval["sha256"]:
            raise SpuCatalogImportError(
                "source artifact mapping_sha256 does not match the approved mapping"
            )
        if unresolved_slugs != approval["unresolved_slugs"]:
            raise SpuCatalogImportError(
                "source artifact unresolved slugs do not match mapping"
            )
        if row_slugs | unresolved_slugs != approval["configured_slugs"]:
            raise SpuCatalogImportError(
                "source artifact does not cover the approved mapping"
            )
        if summary_pairs != approval["allowed_pairs"]:
            raise SpuCatalogImportError(
                "source artifact official-series requests do not match approved mapping"
            )

    attestation = {
        "schema": SOURCE_ARTIFACT_SCHEMA,
        "catalog_sha256": declared_catalog_sha,
        "source": dict(source),
        "summary": dict(summary),
        "unresolved": list(unresolved_raw),
        "series_summaries": list(series_summaries),
    }
    return raw_rows, declared_catalog_sha, attestation


def load_frozen_catalog(
    input_path: Path, *, mapping_path: Optional[Path] = None
) -> FrozenCatalog:
    """Load and fully validate one attested normalized source artifact."""

    candidate = input_path.expanduser().absolute()
    if candidate.is_symlink():
        raise SpuCatalogImportError(
            f"normalized input must be a regular non-symlink file: {candidate}"
        )
    path = candidate.resolve()
    if not path.is_file():
        raise SpuCatalogImportError(f"normalized input file is missing: {path}")
    raw_bytes = path.read_bytes()
    try:
        payload = json.loads(raw_bytes)
    except (UnicodeDecodeError, json.JSONDecodeError) as exc:
        raise SpuCatalogImportError(
            f"normalized input is not valid UTF-8 JSON: {path}"
        ) from exc
    raw_rows, catalog_sha256, source_attestation = _validate_source_artifact(
        payload, mapping_path=mapping_path
    )
    rows = tuple(_parse_row(raw, index=index) for index, raw in enumerate(raw_rows))

    seen_car_ids: Dict[str, TrimSpec] = {}
    seen_natural: Dict[Tuple[str, str, int], TrimSpec] = {}
    dcd_to_series: Dict[str, str] = {}
    for row in rows:
        if row.car_id in seen_car_ids:
            raise SpuCatalogImportError(
                f"duplicate car_id in normalized input: {row.car_id}"
            )
        seen_car_ids[row.car_id] = row
        natural = (
            row.series_slug,
            _normalized_text_key(row.trim_label),
            row.model_year,
        )
        if natural in seen_natural:
            previous = seen_natural[natural]
            raise SpuCatalogImportError(
                "duplicate series/year/trim identity in normalized input: "
                f"{previous.car_id} and {row.car_id}"
            )
        seen_natural[natural] = row
        prior_slug = dcd_to_series.setdefault(row.dcd_series_id, row.series_slug)
        if prior_slug != row.series_slug:
            raise SpuCatalogImportError(
                f"dcd_series_id {row.dcd_series_id!r} maps to multiple series_slug values"
            )
    return FrozenCatalog(
        input_path=path,
        input_sha256=_sha256_bytes(raw_bytes),
        catalog_sha256=catalog_sha256,
        source_attestation=source_attestation,
        rows=tuple(sorted(rows, key=lambda item: (item.series_slug, item.car_id))),
    )


def _deny_formal_database_in_tests(db_path: Path) -> None:
    if (
        os.environ.get("DCAR_TEST_DENY_FORMAL_DB") == "1"
        and db_path.resolve() == DEFAULT_DB.resolve()
    ):
        raise SpuCatalogImportError(
            "test process attempted to open the formal DCar database"
        )


@contextmanager
def _connect_read_only(db_path: Path) -> Iterator[sqlite3.Connection]:
    """Open a WAL-aware read-only connection (do not use immutable=1 here)."""

    candidate = db_path.expanduser().absolute()
    if candidate.is_symlink():
        raise SpuCatalogImportError(
            f"database must be a regular non-symlink file: {candidate}"
        )
    path = candidate.resolve()
    _deny_formal_database_in_tests(path)
    if not path.is_file():
        raise SpuCatalogImportError(f"database file is missing: {path}")
    connection = sqlite3.connect(
        f"{path.as_uri()}?mode=ro",
        uri=True,
        timeout=10,
    )
    connection.row_factory = sqlite3.Row
    try:
        connection.execute("PRAGMA foreign_keys=ON")
        connection.execute("PRAGMA query_only=ON")
        connection.execute("PRAGMA busy_timeout=10000")
        yield connection
    finally:
        connection.close()


def _database_health(connection: sqlite3.Connection) -> Dict[str, Any]:
    version = int(connection.execute("PRAGMA user_version").fetchone()[0])
    if version != SCHEMA_VERSION:
        raise SpuCatalogImportError(
            f"database schema must be v{SCHEMA_VERSION}, got v{version}"
        )
    tables = {
        str(row[0])
        for row in connection.execute(
            "SELECT name FROM sqlite_master WHERE type='table'"
        ).fetchall()
    }
    missing = sorted(_REQUIRED_DOMAIN_TABLES - tables)
    if missing:
        raise SpuCatalogImportError(f"database is missing SPU domain tables: {missing}")
    quick_rows = [str(row[0]) for row in connection.execute("PRAGMA quick_check")]
    if quick_rows != ["ok"]:
        raise SpuCatalogImportError(f"database quick_check failed: {quick_rows[:5]}")
    foreign_keys = connection.execute("PRAGMA foreign_key_check").fetchall()
    if foreign_keys:
        raise SpuCatalogImportError(
            f"database has {len(foreign_keys)} foreign-key violation(s)"
        )
    running = int(
        connection.execute(
            "SELECT COUNT(*) FROM spu_association_runs WHERE status='running'"
        ).fetchone()[0]
    )
    if running:
        raise SpuCatalogImportError(
            f"refusing import while {running} SPU association run(s) are running"
        )
    return {
        "user_version": version,
        "quick_check": "ok",
        "foreign_key_violations": 0,
        "running_association_runs": 0,
    }


def _catalog_state(connection: sqlite3.Connection) -> List[Dict[str, Any]]:
    return [
        dict(row)
        for row in connection.execute(
            """
            SELECT spu_id,brand,series,series_slug,trim_label,is_series_node,
                   model_year,powertrain,body_style,price_low,price_high,
                   external_ref,enabled,created_at,updated_at
            FROM spu_catalog ORDER BY spu_id
            """
        )
    ]


def _alias_state(connection: sqlite3.Connection) -> List[Dict[str, Any]]:
    return [
        dict(row)
        for row in connection.execute(
            """
            SELECT id,alias,alias_type,spu_scope,spu_id,ambiguous,enabled
            FROM spu_alias ORDER BY spu_id,alias,id
            """
        )
    ]


def _series_state(connection: sqlite3.Connection) -> List[Dict[str, Any]]:
    return [
        dict(row)
        for row in connection.execute(
            """
            SELECT spu_id,brand,series,series_slug,trim_label,is_series_node,
                   model_year,powertrain,body_style,price_low,price_high,
                   external_ref,enabled,created_at,updated_at
            FROM spu_catalog WHERE is_series_node=1 ORDER BY series_slug
            """
        )
    ]


def _difference(
    existing: Mapping[str, Any], desired: Mapping[str, Any], fields: Sequence[str]
) -> Dict[str, Dict[str, Any]]:
    changes: Dict[str, Dict[str, Any]] = {}
    for field in fields:
        before = existing.get(field)
        after = desired.get(field)
        if before != after:
            changes[field] = {"from": before, "to": after}
    return changes


def _sort_operations(rows: List[Mapping[str, Any]]) -> Tuple[Mapping[str, Any], ...]:
    return tuple(
        sorted(
            rows,
            key=lambda row: (
                str(row.get("spu_id") or ""),
                str(row.get("alias") or ""),
            ),
        )
    )


def _build_plan_from_connection(
    connection: sqlite3.Connection,
    catalog: FrozenCatalog,
    *,
    db_path: Path,
) -> ImportPlan:
    health = _database_health(connection)
    catalog_state = _catalog_state(connection)
    alias_state = _alias_state(connection)
    series_state = _series_state(connection)
    database_state_sha256 = _canonical_sha256(
        {"spu_catalog": catalog_state, "spu_alias": alias_state}
    )
    series_state_sha256 = _canonical_sha256(series_state)

    series_nodes: Dict[str, Dict[str, Any]] = {
        str(row["series_slug"]): row for row in series_state
    }
    existing_by_id: Dict[str, Dict[str, Any]] = {
        str(row["spu_id"]): row for row in catalog_state
    }
    external_by_ref: Dict[str, List[Dict[str, Any]]] = {}
    natural_by_key: Dict[Tuple[str, str, int], List[Dict[str, Any]]] = {}
    for existing_row in catalog_state:
        external_ref = str(existing_row.get("external_ref") or "")
        if external_ref:
            external_by_ref.setdefault(external_ref, []).append(existing_row)
        if (
            not int(existing_row["is_series_node"])
            and existing_row["model_year"] is not None
        ):
            natural = (
                str(existing_row["series_slug"]),
                _normalized_text_key(str(existing_row["trim_label"] or "")),
                int(existing_row["model_year"]),
            )
            natural_by_key.setdefault(natural, []).append(existing_row)
    duplicate_refs = {
        ref: rows for ref, rows in external_by_ref.items() if len(rows) > 1
    }
    if duplicate_refs:
        sample = sorted(duplicate_refs)[0]
        raise SpuCatalogImportError(
            f"existing database has duplicate external_ref {sample!r}"
        )

    alias_by_target_key: Dict[Tuple[str, str], List[Dict[str, Any]]] = {}
    series_alias_keys: Dict[Tuple[str, str], List[str]] = {}
    catalog_by_id = existing_by_id
    for alias_row in alias_state:
        normalized = _normalized_text_key(str(alias_row["alias"]))
        alias_by_target_key.setdefault(
            (str(alias_row["spu_id"]), normalized), []
        ).append(alias_row)
        target = catalog_by_id.get(str(alias_row["spu_id"]))
        if target is not None and int(target["is_series_node"]):
            series_alias_keys.setdefault(
                (str(target["series_slug"]), normalized), []
            ).append(str(alias_row["alias"]))

    catalog_inserts: List[Mapping[str, Any]] = []
    catalog_updates: List[Mapping[str, Any]] = []
    alias_inserts: List[Mapping[str, Any]] = []
    alias_updates: List[Mapping[str, Any]] = []
    warnings: List[str] = []
    unchanged_catalog = 0
    unchanged_aliases = 0
    input_alias_targets: Dict[Tuple[str, str], List[str]] = {}

    for row in catalog.rows:
        series_node = series_nodes.get(row.series_slug)
        if series_node is None:
            raise SpuCatalogImportError(
                f"input references unknown existing series_slug: {row.series_slug!r}"
            )
        if not int(series_node["enabled"]):
            raise SpuCatalogImportError(
                f"input references disabled series_slug: {row.series_slug!r}"
            )
        if (
            row.expected_brand is not None
            and str(series_node["brand"]) != row.expected_brand
        ):
            raise SpuCatalogImportError(
                f"input brand for {row.series_slug!r} does not match the existing series node: "
                f"{row.expected_brand!r} != {series_node['brand']!r}"
            )
        if (
            row.expected_series is not None
            and str(series_node["series"]) != row.expected_series
        ):
            raise SpuCatalogImportError(
                f"input series for {row.series_slug!r} does not match the existing series node: "
                f"{row.expected_series!r} != {series_node['series']!r}"
            )
        natural = (
            row.series_slug,
            _normalized_text_key(row.trim_label),
            row.model_year,
        )
        natural_matches = natural_by_key.get(natural, [])
        if any(str(item["spu_id"]) != row.spu_id for item in natural_matches):
            other = next(
                str(item["spu_id"])
                for item in natural_matches
                if str(item["spu_id"]) != row.spu_id
            )
            raise SpuCatalogImportError(
                f"natural trim identity for {row.spu_id!r} already belongs to {other!r}"
            )
        referenced = external_by_ref.get(row.external_ref, [])
        if referenced and str(referenced[0]["spu_id"]) != row.spu_id:
            raise SpuCatalogImportError(
                f"external_ref {row.external_ref!r} already belongs to "
                f"{referenced[0]['spu_id']!r}"
            )
        desired = {
            "spu_id": row.spu_id,
            "brand": str(series_node["brand"]),
            "series": str(series_node["series"]),
            "series_slug": row.series_slug,
            "trim_label": row.trim_label,
            "is_series_node": 0,
            "model_year": row.model_year,
            "powertrain": row.powertrain,
            "body_style": row.body_style,
            "price_low": row.price_low,
            "price_high": row.price_high,
            "external_ref": row.external_ref,
            "enabled": 1,
            "dcd_series_id": row.dcd_series_id,
            "source_bucket": row.source_bucket,
        }
        existing_target = existing_by_id.get(row.spu_id)
        if existing_target is None:
            catalog_inserts.append(desired)
        else:
            if int(existing_target["is_series_node"]):
                raise SpuCatalogImportError(
                    f"generated trim spu_id collides with a series node: {row.spu_id!r}"
                )
            immutable_pairs = {
                "brand": desired["brand"],
                "series": desired["series"],
                "series_slug": desired["series_slug"],
                "external_ref": desired["external_ref"],
            }
            for field, expected in immutable_pairs.items():
                if existing_target[field] != expected:
                    raise SpuCatalogImportError(
                        f"existing {row.spu_id!r} conflicts on immutable {field}: "
                        f"{existing_target[field]!r} != {expected!r}"
                    )
            changes = _difference(existing_target, desired, _CATALOG_MUTABLE_FIELDS)
            if changes:
                catalog_updates.append(
                    {"spu_id": row.spu_id, "desired": desired, "changes": changes}
                )
            else:
                unchanged_catalog += 1

        for alias_spec in row.aliases:
            normalized = _normalized_text_key(alias_spec.alias)
            input_alias_targets.setdefault((row.series_slug, normalized), []).append(
                row.spu_id
            )
            if (row.series_slug, normalized) in series_alias_keys:
                warnings.append(
                    f"trim alias {alias_spec.alias!r} for {row.spu_id} is also a series alias"
                )
            target_matches = alias_by_target_key.get((row.spu_id, normalized), [])
            if len(target_matches) > 1:
                raise SpuCatalogImportError(
                    f"existing database has case-insensitive duplicate alias "
                    f"{alias_spec.alias!r} for {row.spu_id!r}"
                )
            desired_alias = {
                "spu_id": row.spu_id,
                "alias": alias_spec.alias,
                "alias_type": alias_spec.alias_type,
                "spu_scope": "trim",
                "ambiguous": 1 if alias_spec.ambiguous else 0,
                "enabled": 1 if alias_spec.enabled else 0,
            }
            if not target_matches:
                alias_inserts.append(desired_alias)
                continue
            existing_alias = target_matches[0]
            if str(existing_alias["spu_scope"]) != "trim":
                raise SpuCatalogImportError(
                    f"existing alias {alias_spec.alias!r} for {row.spu_id!r} has non-trim scope"
                )
            changes = _difference(existing_alias, desired_alias, _ALIAS_MUTABLE_FIELDS)
            if changes:
                alias_updates.append(
                    {
                        "id": int(existing_alias["id"]),
                        "spu_id": row.spu_id,
                        "alias": str(existing_alias["alias"]),
                        "desired": desired_alias,
                        "changes": changes,
                    }
                )
            else:
                unchanged_aliases += 1

    for (series_slug, normalized), targets in sorted(input_alias_targets.items()):
        unique_targets = sorted(set(targets))
        if len(unique_targets) > 1:
            warnings.append(
                f"case-insensitive alias {normalized!r} is shared by {len(unique_targets)} "
                f"trims in {series_slug}; trim resolution may safely fall back to series"
            )

    bucket_counts: Dict[str, int] = {}
    for row in catalog.rows:
        bucket_counts[row.source_bucket] = bucket_counts.get(row.source_bucket, 0) + 1

    inserts = _sort_operations(catalog_inserts)
    updates = _sort_operations(catalog_updates)
    alias_adds = _sort_operations(alias_inserts)
    alias_changes = _sort_operations(alias_updates)
    plan_material = {
        "schema_version": PLAN_SCHEMA_VERSION,
        "db_path": str(db_path.resolve()),
        "input_sha256": catalog.input_sha256,
        "database_state_sha256": database_state_sha256,
        "series_state_sha256": series_state_sha256,
        "catalog_inserts": inserts,
        "catalog_updates": updates,
        "alias_inserts": alias_adds,
        "alias_updates": alias_changes,
    }
    return ImportPlan(
        db_path=db_path.resolve(),
        input_sha256=catalog.input_sha256,
        database_state_sha256=database_state_sha256,
        series_state_sha256=series_state_sha256,
        health=health,
        source_buckets=dict(sorted(bucket_counts.items())),
        catalog_inserts=inserts,
        catalog_updates=updates,
        alias_inserts=alias_adds,
        alias_updates=alias_changes,
        unchanged_catalog_rows=unchanged_catalog,
        unchanged_aliases=unchanged_aliases,
        warnings=tuple(sorted(set(warnings))),
        plan_sha256=_canonical_sha256(plan_material),
    )


def build_import_plan(
    catalog: FrozenCatalog, *, db_path: Path = DEFAULT_DB
) -> ImportPlan:
    """Build a deterministic, read-only import plan for one target database."""

    candidate = db_path.expanduser().absolute()
    if candidate.is_symlink():
        raise SpuCatalogImportError(
            f"database must be a regular non-symlink file: {candidate}"
        )
    resolved = candidate.resolve()
    with _connect_read_only(resolved) as connection:
        return _build_plan_from_connection(connection, catalog, db_path=resolved)


def _backup_validation(path: Path) -> Dict[str, Any]:
    with _connect_read_only(path) as connection:
        health = _database_health(connection)
        page_count = int(connection.execute("PRAGMA page_count").fetchone()[0])
        page_size = int(connection.execute("PRAGMA page_size").fetchone()[0])
    return {**health, "page_count": page_count, "page_size": page_size}


def _online_backup(db_path: Path, backup_dir: Path) -> Dict[str, Any]:
    backup_candidate = backup_dir.expanduser().absolute()
    if backup_candidate.exists() and backup_candidate.is_symlink():
        raise SpuCatalogImportError(
            f"backup directory must not be a symlink: {backup_candidate}"
        )
    backup_root = backup_candidate.resolve()
    backup_root.mkdir(parents=True, exist_ok=True)
    stamp = datetime.now(timezone.utc).strftime("%Y%m%dT%H%M%S.%fZ")
    final_path = backup_root / (
        f"{db_path.stem}.before_dongchedi_spu_trim_import.{stamp}{db_path.suffix}"
    )
    partial_path = final_path.with_name(final_path.name + ".partial")
    if final_path.exists() or partial_path.exists():
        raise SpuCatalogImportError(f"backup target already exists: {final_path}")
    try:
        with _connect_read_only(db_path) as source:
            target = sqlite3.connect(partial_path)
            try:
                source.backup(target, pages=4096, sleep=0.01)
                target.execute("PRAGMA journal_mode=DELETE")
                target.commit()
            finally:
                target.close()
        validation = _backup_validation(partial_path)
        os.chmod(partial_path, 0o600)
        with partial_path.open("rb") as handle:
            os.fsync(handle.fileno())
        partial_path.replace(final_path)
    except Exception:
        if partial_path.exists():
            partial_path.unlink()
        raise
    return {
        "path": str(final_path),
        "byte_size": final_path.stat().st_size,
        "sha256": _sha256_file(final_path),
        "validation": validation,
    }


def _apply_plan(
    connection: sqlite3.Connection, plan: ImportPlan, *, captured_at: str
) -> None:
    for row in plan.catalog_inserts:
        connection.execute(
            """
            INSERT INTO spu_catalog(
                spu_id,brand,series,series_slug,trim_label,is_series_node,
                model_year,powertrain,body_style,price_low,price_high,
                external_ref,enabled,created_at,updated_at
            ) VALUES (?,?,?,?,?,0,?,?,?,?,?,?,1,?,?)
            """,
            (
                row["spu_id"],
                row["brand"],
                row["series"],
                row["series_slug"],
                row["trim_label"],
                row["model_year"],
                row["powertrain"],
                row["body_style"],
                row["price_low"],
                row["price_high"],
                row["external_ref"],
                captured_at,
                captured_at,
            ),
        )
    for operation in plan.catalog_updates:
        desired = operation["desired"]
        changed_fields = sorted(operation["changes"])
        if not set(changed_fields).issubset(_CATALOG_MUTABLE_FIELDS):
            raise SpuCatalogImportError("import plan contains an unsafe catalog update")
        assignments = ",".join(f"{field}=?" for field in changed_fields)
        parameters = [desired[field] for field in changed_fields]
        cursor = connection.execute(
            f"UPDATE spu_catalog SET {assignments},updated_at=? WHERE spu_id=?",  # noqa: S608
            (*parameters, captured_at, operation["spu_id"]),
        )
        if cursor.rowcount != 1:
            raise SpuCatalogImportError(
                f"catalog update lost target {operation['spu_id']!r}"
            )
    for row in plan.alias_inserts:
        connection.execute(
            """
            INSERT INTO spu_alias(
                alias,alias_type,spu_scope,spu_id,ambiguous,enabled
            ) VALUES (?,?,?,?,?,?)
            """,
            (
                row["alias"],
                row["alias_type"],
                "trim",
                row["spu_id"],
                row["ambiguous"],
                row["enabled"],
            ),
        )
    for operation in plan.alias_updates:
        desired = operation["desired"]
        changed_fields = sorted(operation["changes"])
        if not set(changed_fields).issubset(_ALIAS_MUTABLE_FIELDS):
            raise SpuCatalogImportError("import plan contains an unsafe alias update")
        assignments = ",".join(f"{field}=?" for field in changed_fields)
        parameters = [desired[field] for field in changed_fields]
        cursor = connection.execute(
            f"UPDATE spu_alias SET {assignments} WHERE id=?",  # noqa: S608
            (*parameters, operation["id"]),
        )
        if cursor.rowcount != 1:
            raise SpuCatalogImportError(
                f"alias update lost target id={operation['id']}"
            )


def _verify_catalog_rows(
    connection: sqlite3.Connection,
    catalog: FrozenCatalog,
    *,
    expected_series_state_sha256: str,
) -> None:
    if _canonical_sha256(_series_state(connection)) != expected_series_state_sha256:
        raise SpuCatalogImportError("series nodes changed during trim import")
    for spec in catalog.rows:
        row = connection.execute(
            """
            SELECT spu_id,series_slug,trim_label,is_series_node,model_year,
                   powertrain,body_style,price_low,price_high,external_ref,enabled
            FROM spu_catalog WHERE spu_id=?
            """,
            (spec.spu_id,),
        ).fetchone()
        if row is None:
            raise SpuCatalogImportError(f"post-import row is missing: {spec.spu_id}")
        expected = {
            "spu_id": spec.spu_id,
            "series_slug": spec.series_slug,
            "trim_label": spec.trim_label,
            "is_series_node": 0,
            "model_year": spec.model_year,
            "powertrain": spec.powertrain,
            "body_style": spec.body_style,
            "price_low": spec.price_low,
            "price_high": spec.price_high,
            "external_ref": spec.external_ref,
            "enabled": 1,
        }
        for field, value in expected.items():
            if row[field] != value:
                raise SpuCatalogImportError(
                    f"post-import verification failed for {spec.spu_id}.{field}"
                )
        for alias in spec.aliases:
            matches = connection.execute(
                "SELECT alias,alias_type,ambiguous,enabled FROM spu_alias WHERE spu_id=?",
                (spec.spu_id,),
            ).fetchall()
            normalized = [
                item
                for item in matches
                if _normalized_text_key(str(item["alias"]))
                == _normalized_text_key(alias.alias)
            ]
            if len(normalized) != 1:
                raise SpuCatalogImportError(
                    f"post-import alias verification failed for {spec.spu_id}: {alias.alias!r}"
                )
            stored = normalized[0]
            if (
                str(stored["alias_type"]) != alias.alias_type
                or int(stored["ambiguous"]) != int(alias.ambiguous)
                or int(stored["enabled"]) != int(alias.enabled)
            ):
                raise SpuCatalogImportError(
                    f"post-import alias metadata mismatch for {spec.spu_id}: {alias.alias!r}"
                )
    violations = connection.execute("PRAGMA foreign_key_check").fetchall()
    if violations:
        raise SpuCatalogImportError(
            f"post-import foreign_key_check found {len(violations)} violation(s)"
        )


def _receipt(
    catalog: FrozenCatalog,
    plan: ImportPlan,
    *,
    mode: str,
    applied: bool,
    backup: Optional[Mapping[str, Any]],
    after_health: Optional[Mapping[str, Any]] = None,
    after_database_state_sha256: Optional[str] = None,
) -> Dict[str, Any]:
    return {
        "schema_version": RECEIPT_SCHEMA_VERSION,
        "ok": True,
        "mode": mode,
        "applied": applied,
        "created_at": now_utc(),
        "input": {
            "path": str(catalog.input_path),
            "sha256": catalog.input_sha256,
            "catalog_sha256": catalog.catalog_sha256,
            "rows": len(catalog.rows),
            "source_buckets": dict(plan.source_buckets),
            "attestation": dict(catalog.source_attestation),
        },
        "database": {
            "path": str(plan.db_path),
            "before_state_sha256": plan.database_state_sha256,
            "after_state_sha256": after_database_state_sha256,
            "before_health": dict(plan.health),
            "after_health": dict(after_health) if after_health is not None else None,
        },
        "plan_sha256": plan.plan_sha256,
        "operations": plan.operation_counts(),
        "changed_ids": plan.changed_ids(),
        "warnings": list(plan.warnings),
        "backup": dict(backup) if backup is not None else None,
        "full_association_recompute_required": bool(applied and plan.change_count),
    }


def write_receipt(receipt: Mapping[str, Any], path: Path) -> None:
    """Atomically persist a JSON receipt when the operator requests one."""

    destination = path.expanduser().resolve()
    destination.parent.mkdir(parents=True, exist_ok=True)
    temporary = destination.with_name(destination.name + ".partial")
    if temporary.exists():
        raise SpuCatalogImportError(
            f"receipt temporary file already exists: {temporary}"
        )
    payload = json.dumps(receipt, ensure_ascii=False, sort_keys=True, indent=2) + "\n"
    try:
        temporary.write_text(payload, encoding="utf-8")
        os.chmod(temporary, 0o600)
        temporary.replace(destination)
    except Exception:
        if temporary.exists():
            temporary.unlink()
        raise


def _protected_sqlite_paths(db_path: Path) -> frozenset[Path]:
    resolved = db_path.expanduser().resolve()
    return frozenset(
        {resolved}
        | {Path(f"{resolved}{suffix}") for suffix in ("-wal", "-shm", "-journal")}
    )


def execute_import(
    input_path: Path,
    *,
    db_path: Path = DEFAULT_DB,
    apply: bool = False,
    expected_plan_sha256: Optional[str] = None,
    backup_dir: Optional[Path] = None,
    skip_backup: bool = False,
    receipt_path: Optional[Path] = None,
    mapping_path: Optional[Path] = DEFAULT_MAPPING_PATH,
) -> Dict[str, Any]:
    """Dry-run or atomically apply one frozen Dongchedi trim catalog."""

    if mapping_path is None:
        raise SpuCatalogImportError("an approved mapping_path is required")
    resolved_input = input_path.expanduser().resolve()
    resolved_db_argument = db_path.expanduser().resolve()
    if receipt_path is not None:
        resolved_receipt = receipt_path.expanduser().resolve()
        protected = _protected_sqlite_paths(resolved_db_argument) | {resolved_input}
        if mapping_path is not None:
            protected |= {mapping_path.expanduser().resolve()}
        if resolved_receipt in protected:
            raise SpuCatalogImportError(
                "receipt path must not overwrite the input catalog or SQLite database/sidecars"
            )
    catalog = load_frozen_catalog(input_path, mapping_path=mapping_path)
    initial_plan = build_import_plan(catalog, db_path=db_path)
    resolved_db = initial_plan.db_path
    if not apply:
        receipt = _receipt(
            catalog,
            initial_plan,
            mode="dry_run",
            applied=False,
            backup=None,
        )
        if receipt_path is not None:
            write_receipt(receipt, receipt_path)
        return receipt

    expected = str(expected_plan_sha256 or "").strip().lower()
    if _SHA256_RE.fullmatch(expected) is None:
        raise SpuCatalogImportError(
            "--apply requires the 64-character --expect-plan-sha256 from dry-run"
        )
    if initial_plan.plan_sha256 != expected:
        raise SpuCatalogImportError(
            "dry-run plan hash does not match current database/input state: "
            f"expected {expected}, current {initial_plan.plan_sha256}"
        )
    if skip_backup and resolved_db == DEFAULT_DB.resolve():
        raise SpuCatalogImportError(
            "--skip-backup is forbidden for the formal DEFAULT_DB"
        )

    backup: Optional[Mapping[str, Any]] = None
    with connect(resolved_db, read_only=False) as connection:
        connection.execute("BEGIN IMMEDIATE")
        try:
            locked_plan = _build_plan_from_connection(
                connection, catalog, db_path=resolved_db
            )
            if locked_plan.plan_sha256 != expected:
                raise SpuCatalogImportError(
                    "database changed after dry-run; rerun dry-run and approve the new plan hash"
                )
            if locked_plan.change_count:
                if not skip_backup:
                    backup = _online_backup(
                        resolved_db,
                        backup_dir or (resolved_db.parent / "backups"),
                    )
                _apply_plan(connection, locked_plan, captured_at=now_utc())
                _verify_catalog_rows(
                    connection,
                    catalog,
                    expected_series_state_sha256=locked_plan.series_state_sha256,
                )
            connection.commit()
        except Exception:
            connection.rollback()
            raise

    after_plan = build_import_plan(catalog, db_path=resolved_db)
    if after_plan.change_count:
        raise SpuCatalogImportError(
            "post-import idempotence verification failed: changes still remain"
        )
    receipt = _receipt(
        catalog,
        initial_plan,
        mode="apply",
        applied=bool(initial_plan.change_count),
        backup=backup,
        after_health=after_plan.health,
        after_database_state_sha256=after_plan.database_state_sha256,
    )
    if receipt_path is not None:
        write_receipt(receipt, receipt_path)
    return receipt


__all__ = [
    "AliasSpec",
    "FrozenCatalog",
    "ImportPlan",
    "INPUT_SCHEMA_VERSION",
    "RECEIPT_SCHEMA_VERSION",
    "SpuCatalogImportError",
    "TrimSpec",
    "build_import_plan",
    "execute_import",
    "load_frozen_catalog",
    "write_receipt",
]
