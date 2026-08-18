"""Safely import a frozen official Dongchedi series catalog.

This importer owns only series nodes in ``spu_catalog`` and series-scoped
aliases in ``spu_alias``.  It never deletes catalog data, mutates concrete trim
rows/aliases, or creates audience mappings.

Safety properties:

* dry-run is the default and uses a WAL-aware read-only connection;
* the normalized source artifact is content-addressed and validated offline;
* identity, external-reference, natural-key, and alias conflicts fail closed;
* apply requires the exact plan SHA-256 produced by dry-run;
* writes run in one ``BEGIN IMMEDIATE`` transaction;
* a validated SQLite online backup is mandatory for ``DEFAULT_DB``;
* active SPU association runs block planning and apply;
* post-write verification checks quick_check, foreign keys, idempotence, and
  proves that trim rows/aliases and audience mappings did not change.
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

from .storage import (
    DEFAULT_DB,
    SCHEMA_VERSION,
    connect,
    is_formal_database_path,
    now_utc,
)


SOURCE_ARTIFACT_SCHEMA = "dcar-dongchedi-series-catalog-v1"
INPUT_SCHEMA_VERSION = "dcar-dongchedi-spu-series-normalized-v1"
PLAN_SCHEMA_VERSION = "dcar-dongchedi-spu-series-import-plan-v1"
RECEIPT_SCHEMA_VERSION = "dcar-dongchedi-spu-series-import-receipt-v1"
SOURCE_PROVIDER = "dongchedi"
BRAND_CATALOG_ENDPOINT = "https://www.dongchedi.com/motor/pc/car/brand/all_brand"
BRAND_SERIES_ENDPOINT = (
    "https://www.dongchedi.com/motor/pc/car/brand/get_brand_series_list"
)
APPROVED_CITY_NAME = "北京"
APPROVED_BUSINESS_STATUS_CODES = [0, 2]
APPROVED_BRANDS_REQUESTED = 645

_ALIAS_TYPES = frozenset({"official", "nickname", "slang", "model_code"})
_SHA256_RE = re.compile(r"^[0-9a-f]{64}$")
_NUMERIC_ID_RE = re.compile(r"^[0-9]+$")
_SERIES_SLUG_RE = re.compile(r"^[a-z0-9][a-z0-9_-]*$")
_REQUIRED_DOMAIN_TABLES = frozenset(
    {
        "spu_catalog",
        "spu_alias",
        "spu_audience_map",
        "spu_association_runs",
    }
)
_CATALOG_MUTABLE_FIELDS = (
    "body_style",
    "price_low",
    "price_high",
    "external_ref",
    "enabled",
)
_ALIAS_MUTABLE_FIELDS = ("alias_type", "ambiguous", "enabled")


class SpuSeriesCatalogImportError(RuntimeError):
    """Raised when a series artifact cannot be planned or applied safely."""


@dataclass(frozen=True)
class AliasSpec:
    alias: str
    alias_type: str
    ambiguous: bool
    enabled: bool


@dataclass(frozen=True)
class SeriesSpec:
    dcd_brand_id: str
    brand: str
    dcd_series_id: str
    represented_official_series_ids: Tuple[str, ...]
    series: str
    body_style: str
    business_status: str
    price_low: Optional[float]
    price_high: Optional[float]
    external_ref: str
    aliases: Tuple[AliasSpec, ...]
    existing_series_slug: Optional[str]

    @property
    def series_slug(self) -> str:
        if self.existing_series_slug is not None:
            return self.existing_series_slug
        return f"dcd__series-{self.dcd_series_id}"

    @property
    def spu_id(self) -> str:
        return self.series_slug


@dataclass(frozen=True)
class FrozenSeriesCatalog:
    input_path: Path
    input_sha256: str
    catalog_sha256: str
    source_attestation: Mapping[str, Any]
    artifact_summary: Mapping[str, int]
    rows: Tuple[SeriesSpec, ...]


@dataclass(frozen=True)
class ImportPlan:
    db_path: Path
    input_sha256: str
    database_state_sha256: str
    trim_state_sha256: str
    audience_state_sha256: str
    health: Mapping[str, Any]
    business_statuses: Mapping[str, int]
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
            "series_insert": len(self.catalog_inserts),
            "series_update": len(self.catalog_updates),
            "series_unchanged": self.unchanged_catalog_rows,
            "alias_insert": len(self.alias_inserts),
            "alias_update": len(self.alias_updates),
            "alias_unchanged": self.unchanged_aliases,
            "total_changes": self.change_count,
        }

    def changed_ids(self) -> Dict[str, List[str]]:
        return {
            "series_insert": [str(row["spu_id"]) for row in self.catalog_inserts],
            "series_update": [str(row["spu_id"]) for row in self.catalog_updates],
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
    if key not in row:
        raise SpuSeriesCatalogImportError(f"{label}.{key} is required")
    value = row.get(key)
    if not isinstance(value, str):
        raise SpuSeriesCatalogImportError(f"{label}.{key} must be a string")
    normalized = value.strip()
    if not normalized:
        raise SpuSeriesCatalogImportError(f"{label}.{key} must not be blank")
    if len(normalized) > max_length:
        raise SpuSeriesCatalogImportError(
            f"{label}.{key} exceeds {max_length} characters"
        )
    return normalized


def _numeric_identifier(value: Any, *, label: str) -> str:
    if isinstance(value, bool):
        raise SpuSeriesCatalogImportError(f"{label} must be a numeric identifier")
    if isinstance(value, int):
        normalized = str(value)
    elif isinstance(value, str):
        normalized = value.strip()
    else:
        raise SpuSeriesCatalogImportError(f"{label} must be a numeric identifier")
    if _NUMERIC_ID_RE.fullmatch(normalized) is None:
        raise SpuSeriesCatalogImportError(f"{label} must contain digits only")
    return normalized


def _optional_price(value: Any, *, label: str) -> Optional[float]:
    if value is None:
        return None
    if isinstance(value, bool):
        raise SpuSeriesCatalogImportError(
            f"{label} must be a non-negative number or null"
        )
    try:
        parsed = float(value)
    except (TypeError, ValueError) as exc:
        raise SpuSeriesCatalogImportError(
            f"{label} must be a non-negative number or null"
        ) from exc
    if not math.isfinite(parsed) or parsed < 0:
        raise SpuSeriesCatalogImportError(f"{label} must be finite and non-negative")
    return parsed


def _strict_bool(value: Any, *, label: str, default: bool) -> bool:
    if value is None:
        return default
    if not isinstance(value, bool):
        raise SpuSeriesCatalogImportError(f"{label} must be a boolean")
    return value


def _business_status(value: Any, *, label: str) -> str:
    mapping = {
        0: "on_sale",
        2: "upcoming",
        "0": "on_sale",
        "2": "upcoming",
        "on_sale": "on_sale",
        "upcoming": "upcoming",
    }
    if isinstance(value, bool) or value not in mapping:
        raise SpuSeriesCatalogImportError(
            f"{label} must be one of on_sale, upcoming, 0, or 2"
        )
    return mapping[value]


def _parse_price(
    value: Any, *, row_label: str
) -> Tuple[Optional[float], Optional[float]]:
    if not isinstance(value, dict):
        raise SpuSeriesCatalogImportError(f"{row_label}.price must be an object")
    if "low" not in value or "high" not in value:
        raise SpuSeriesCatalogImportError(
            f"{row_label}.price must contain low and high"
        )
    unit = value.get("unit", "万元")
    if unit != "万元":
        raise SpuSeriesCatalogImportError(f"{row_label}.price.unit must be '万元'")
    low = _optional_price(value.get("low"), label=f"{row_label}.price.low")
    high = _optional_price(value.get("high"), label=f"{row_label}.price.high")
    if (low is None) != (high is None):
        raise SpuSeriesCatalogImportError(
            f"{row_label}.price low/high must both be null or both be numbers"
        )
    if low is not None and high is not None and low > high:
        raise SpuSeriesCatalogImportError(
            f"{row_label}.price.low must not exceed price.high"
        )
    return low, high


def _parse_aliases(value: Any, *, series: str, row_label: str) -> Tuple[AliasSpec, ...]:
    if not isinstance(value, list) or not value:
        raise SpuSeriesCatalogImportError(
            f"{row_label}.aliases must be a non-empty list"
        )
    parsed: List[AliasSpec] = []
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
            raise SpuSeriesCatalogImportError(
                f"{alias_label} must be a string or an alias object"
            )
        if not alias_text:
            raise SpuSeriesCatalogImportError(f"{alias_label} must not be blank")
        if len(alias_text) > 60:
            raise SpuSeriesCatalogImportError(f"{alias_label} exceeds 60 characters")
        if alias_type not in _ALIAS_TYPES:
            raise SpuSeriesCatalogImportError(
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
        if previous != alias_spec:
            raise SpuSeriesCatalogImportError(
                f"{row_label} contains conflicting case-insensitive aliases: "
                f"{previous.alias!r} and {alias_spec.alias!r}"
            )
    official = by_key.get(_normalized_text_key(series))
    if official is None or official.alias_type != "official" or not official.enabled:
        raise SpuSeriesCatalogImportError(
            f"{row_label}.aliases must include the enabled official series name {series!r}"
        )
    return tuple(
        sorted(by_key.values(), key=lambda item: _normalized_text_key(item.alias))
    )


def _parse_row(raw: Any, *, index: int) -> SeriesSpec:
    label = f"rows[{index}]"
    if not isinstance(raw, dict):
        raise SpuSeriesCatalogImportError(f"{label} must be an object")
    dcd_brand_id = _numeric_identifier(
        raw.get("dcd_brand_id"), label=f"{label}.dcd_brand_id"
    )
    brand = _required_text(raw, "brand", label=label, max_length=60)
    dcd_series_id = _numeric_identifier(
        raw.get("dcd_series_id"), label=f"{label}.dcd_series_id"
    )
    represented_raw = raw.get("represented_official_series_ids")
    represented_ids: Tuple[str, ...]
    if represented_raw is None:
        represented_ids = (dcd_series_id,)
    elif isinstance(represented_raw, list) and represented_raw:
        represented_ids = tuple(
            _numeric_identifier(value, label=f"{label}.represented_official_series_ids")
            for value in represented_raw
        )
        if len(set(represented_ids)) != len(represented_ids):
            raise SpuSeriesCatalogImportError(
                f"{label}.represented_official_series_ids contains duplicates"
            )
        if represented_ids[0] != dcd_series_id:
            raise SpuSeriesCatalogImportError(
                f"{label}.dcd_series_id must be the first represented official series ID"
            )
    else:
        raise SpuSeriesCatalogImportError(
            f"{label}.represented_official_series_ids must be a non-empty list"
        )
    series = _required_text(raw, "series", label=label, max_length=80)
    body_style = _required_text(raw, "body_style", label=label, max_length=24)
    if "business_status" not in raw:
        raise SpuSeriesCatalogImportError(f"{label}.business_status is required")
    business_status = _business_status(
        raw.get("business_status"), label=f"{label}.business_status"
    )
    if "price" not in raw:
        raise SpuSeriesCatalogImportError(f"{label}.price is required")
    price_low, price_high = _parse_price(raw.get("price"), row_label=label)
    external_ref = _required_text(raw, "external_ref", label=label, max_length=160)
    canonical_ref = f"dongchedi:series:{dcd_series_id}"
    if external_ref != canonical_ref:
        raise SpuSeriesCatalogImportError(
            f"{label}.external_ref must be {canonical_ref!r}"
        )
    existing_slug_raw = raw.get("existing_series_slug")
    existing_slug: Optional[str]
    if existing_slug_raw is None:
        existing_slug = None
    elif isinstance(existing_slug_raw, str) and existing_slug_raw.strip():
        existing_slug = existing_slug_raw.strip()
        if _SERIES_SLUG_RE.fullmatch(existing_slug) is None:
            raise SpuSeriesCatalogImportError(
                f"{label}.existing_series_slug has an invalid format"
            )
    else:
        raise SpuSeriesCatalogImportError(
            f"{label}.existing_series_slug must be a non-empty string or null"
        )
    if existing_slug is None and len(represented_ids) != 1:
        raise SpuSeriesCatalogImportError(
            f"{label}: a new official series must map one ID to one node"
        )
    aliases = _parse_aliases(raw.get("aliases"), series=series, row_label=label)
    spec = SeriesSpec(
        dcd_brand_id=dcd_brand_id,
        brand=brand,
        dcd_series_id=dcd_series_id,
        represented_official_series_ids=represented_ids,
        series=series,
        body_style=body_style,
        business_status=business_status,
        price_low=price_low,
        price_high=price_high,
        external_ref=external_ref,
        aliases=aliases,
        existing_series_slug=existing_slug,
    )
    for identity_field in ("series_slug", "spu_id"):
        declared_identity = raw.get(identity_field)
        if declared_identity is not None and declared_identity != spec.series_slug:
            raise SpuSeriesCatalogImportError(
                f"{label}.{identity_field} must match derived identity "
                f"{spec.series_slug!r}"
            )
    if len(spec.spu_id) > 160:
        raise SpuSeriesCatalogImportError(
            f"{label} generates an spu_id longer than 160 characters"
        )
    return spec


def _validate_official_source(source: Any) -> Mapping[str, Any]:
    if not isinstance(source, dict):
        raise SpuSeriesCatalogImportError("source artifact lacks a source object")
    if source.get("provider") != SOURCE_PROVIDER:
        raise SpuSeriesCatalogImportError("source artifact provider is not approved")
    if source.get("brand_endpoint") != BRAND_CATALOG_ENDPOINT:
        raise SpuSeriesCatalogImportError(
            "source.brand_endpoint is not the approved Dongchedi all_brand endpoint"
        )
    if source.get("series_endpoint") != BRAND_SERIES_ENDPOINT:
        raise SpuSeriesCatalogImportError(
            "source.series_endpoint is not the approved Dongchedi "
            "get_brand_series_list endpoint"
        )
    if source.get("city_name") != APPROVED_CITY_NAME:
        raise SpuSeriesCatalogImportError("source.city_name must be 北京")
    if source.get("included_business_statuses") != APPROVED_BUSINESS_STATUS_CODES:
        raise SpuSeriesCatalogImportError(
            "source.included_business_statuses must be exactly [0, 2]"
        )
    if source.get("brands_requested") != APPROVED_BRANDS_REQUESTED:
        raise SpuSeriesCatalogImportError(
            f"source.brands_requested must be {APPROVED_BRANDS_REQUESTED}"
        )
    brand_response_sha = str(source.get("brand_response_sha256") or "").lower()
    if _SHA256_RE.fullmatch(brand_response_sha) is None:
        raise SpuSeriesCatalogImportError(
            "source.brand_response_sha256 must be a SHA-256"
        )
    requests = source.get("series_requests")
    if not isinstance(requests, list) or len(requests) != APPROVED_BRANDS_REQUESTED:
        raise SpuSeriesCatalogImportError(
            f"source.series_requests must contain {APPROVED_BRANDS_REQUESTED} requests"
        )
    request_brand_ids: set[str] = set()
    for index, request in enumerate(requests):
        if not isinstance(request, dict):
            raise SpuSeriesCatalogImportError(
                f"source.series_requests[{index}] must be an object"
            )
        brand_id = _numeric_identifier(
            request.get("dcd_brand_id"),
            label=f"source.series_requests[{index}].dcd_brand_id",
        )
        if brand_id in request_brand_ids:
            raise SpuSeriesCatalogImportError(
                f"source.series_requests contains duplicate brand ID {brand_id}"
            )
        request_brand_ids.add(brand_id)
        response_sha = str(request.get("response_sha256") or "").lower()
        if _SHA256_RE.fullmatch(response_sha) is None:
            raise SpuSeriesCatalogImportError(
                f"source.series_requests[{index}].response_sha256 must be a SHA-256"
            )
    validated = dict(source)
    validated["request_brand_ids"] = sorted(request_brand_ids, key=int)
    return validated


def load_frozen_series_catalog(input_path: Path) -> FrozenSeriesCatalog:
    """Load and validate one content-addressed official series artifact."""

    candidate = input_path.expanduser().absolute()
    if not candidate.is_file() or candidate.is_symlink():
        raise SpuSeriesCatalogImportError(
            f"input must be a regular non-symlink file: {candidate}"
        )
    resolved = candidate.resolve()
    raw = resolved.read_bytes()
    try:
        payload = json.loads(raw)
    except (UnicodeDecodeError, json.JSONDecodeError) as exc:
        raise SpuSeriesCatalogImportError(
            f"input is not valid UTF-8 JSON: {resolved}"
        ) from exc
    if not isinstance(payload, dict):
        raise SpuSeriesCatalogImportError("series artifact must be a JSON object")
    if payload.get("schema") != SOURCE_ARTIFACT_SCHEMA:
        raise SpuSeriesCatalogImportError(
            f"unsupported source artifact schema: {payload.get('schema')!r}"
        )
    if payload.get("schema_version") != INPUT_SCHEMA_VERSION:
        raise SpuSeriesCatalogImportError(
            f"unsupported input schema_version: {payload.get('schema_version')!r}"
        )
    generated_at = payload.get("generated_at")
    if not isinstance(generated_at, str) or not generated_at.strip():
        raise SpuSeriesCatalogImportError("source artifact generated_at is required")
    declared_sha = str(payload.get("catalog_sha256") or "").lower()
    if _SHA256_RE.fullmatch(declared_sha) is None:
        raise SpuSeriesCatalogImportError("source artifact catalog_sha256 is invalid")
    hash_material = dict(payload)
    hash_material.pop("catalog_sha256", None)
    if _canonical_sha256(hash_material) != declared_sha:
        raise SpuSeriesCatalogImportError(
            "source artifact catalog_sha256 does not match content"
        )
    source = _validate_official_source(payload.get("source"))
    raw_rows = payload.get("rows")
    raw_summary = payload.get("summary")
    if not isinstance(raw_rows, list) or not raw_rows:
        raise SpuSeriesCatalogImportError("source artifact rows must be non-empty")
    if not isinstance(raw_summary, dict):
        raise SpuSeriesCatalogImportError("source artifact summary must be an object")
    rows = tuple(_parse_row(row, index=index) for index, row in enumerate(raw_rows))

    by_series_id: Dict[str, SeriesSpec] = {}
    by_external_ref: Dict[str, SeriesSpec] = {}
    by_slug: Dict[str, SeriesSpec] = {}
    natural_rows: Dict[Tuple[str, str], SeriesSpec] = {}
    alias_targets: Dict[str, List[Tuple[SeriesSpec, AliasSpec]]] = {}
    represented_ids: Dict[str, SeriesSpec] = {}
    for row in rows:
        if row.dcd_series_id in by_series_id:
            raise SpuSeriesCatalogImportError(
                f"duplicate dcd_series_id in input: {row.dcd_series_id}"
            )
        by_series_id[row.dcd_series_id] = row
        for represented_id in row.represented_official_series_ids:
            previous = represented_ids.get(represented_id)
            if previous is not None:
                raise SpuSeriesCatalogImportError(
                    f"represented official series ID {represented_id} is shared by "
                    f"{previous.series_slug!r} and {row.series_slug!r}"
                )
            represented_ids[represented_id] = row
        if row.external_ref in by_external_ref:
            raise SpuSeriesCatalogImportError(
                f"duplicate external_ref in input: {row.external_ref!r}"
            )
        by_external_ref[row.external_ref] = row
        if row.series_slug in by_slug:
            raise SpuSeriesCatalogImportError(
                f"multiple rows target series_slug {row.series_slug!r}"
            )
        by_slug[row.series_slug] = row
        natural = (_normalized_text_key(row.brand), _normalized_text_key(row.series))
        if natural in natural_rows:
            raise SpuSeriesCatalogImportError(
                f"duplicate brand/series identity in input: {row.brand} {row.series}"
            )
        natural_rows[natural] = row
        for alias in row.aliases:
            alias_targets.setdefault(_normalized_text_key(alias.alias), []).append(
                (row, alias)
            )
    for normalized, targets in alias_targets.items():
        target_ids = {row.dcd_series_id for row, _ in targets}
        if len(target_ids) > 1 and any(not alias.ambiguous for _, alias in targets):
            raise SpuSeriesCatalogImportError(
                f"shared series alias {normalized!r} must be marked ambiguous for every target"
            )

    expected_summary = {
        "logical_import_rows": len(rows),
        "represented_official_series_id_count": len(represented_ids),
        "logical_existing_series_rows": sum(
            row.existing_series_slug is not None for row in rows
        ),
        "logical_new_series_rows": sum(
            row.existing_series_slug is None for row in rows
        ),
        "selected_count": len(represented_ids),
    }
    for key, expected in expected_summary.items():
        value = raw_summary.get(key)
        if isinstance(value, bool) or not isinstance(value, int) or value != expected:
            raise SpuSeriesCatalogImportError(
                f"source artifact summary.{key} must be {expected}"
            )
    requested_brand_ids = set(source["request_brand_ids"])
    missing_brand_ids = sorted(
        {row.dcd_brand_id for row in rows} - requested_brand_ids,
        key=int,
    )
    if missing_brand_ids:
        raise SpuSeriesCatalogImportError(
            "source.series_requests does not attest every row brand ID: "
            f"{missing_brand_ids[:5]}"
        )

    return FrozenSeriesCatalog(
        input_path=resolved,
        input_sha256=_sha256_bytes(raw),
        catalog_sha256=declared_sha,
        source_attestation={
            **source,
            "generated_at": generated_at.strip(),
        },
        artifact_summary=expected_summary,
        rows=rows,
    )


def _deny_formal_database_in_tests(db_path: Path) -> None:
    if (
        os.environ.get("DCAR_TEST_DENY_FORMAL_DB") == "1"
        and is_formal_database_path(db_path, formal_database=DEFAULT_DB)
    ):
        raise SpuSeriesCatalogImportError(
            "test process attempted to open the formal DCar database"
        )


@contextmanager
def _connect_read_only(db_path: Path) -> Iterator[sqlite3.Connection]:
    candidate = db_path.expanduser().absolute()
    if candidate.is_symlink():
        raise SpuSeriesCatalogImportError(
            f"database must be a regular non-symlink file: {candidate}"
        )
    path = candidate.resolve()
    _deny_formal_database_in_tests(path)
    if not path.is_file():
        raise SpuSeriesCatalogImportError(f"database file is missing: {path}")
    connection = sqlite3.connect(f"{path.as_uri()}?mode=ro", uri=True, timeout=10)
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
        raise SpuSeriesCatalogImportError(
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
        raise SpuSeriesCatalogImportError(
            f"database is missing SPU domain tables: {missing}"
        )
    quick_rows = [str(row[0]) for row in connection.execute("PRAGMA quick_check")]
    if quick_rows != ["ok"]:
        raise SpuSeriesCatalogImportError(
            f"database quick_check failed: {quick_rows[:5]}"
        )
    foreign_keys = connection.execute("PRAGMA foreign_key_check").fetchall()
    if foreign_keys:
        raise SpuSeriesCatalogImportError(
            f"database has {len(foreign_keys)} foreign-key violation(s)"
        )
    running = int(
        connection.execute(
            "SELECT COUNT(*) FROM spu_association_runs WHERE status='running'"
        ).fetchone()[0]
    )
    if running:
        raise SpuSeriesCatalogImportError(
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


def _audience_state(connection: sqlite3.Connection) -> List[Dict[str, Any]]:
    return [
        dict(row)
        for row in connection.execute(
            """
            SELECT scope,scope_key,audience_code,role,weight,basis
            FROM spu_audience_map
            ORDER BY scope,scope_key,audience_code,role
            """
        )
    ]


def _trim_state(
    catalog_state: Sequence[Mapping[str, Any]],
    alias_state: Sequence[Mapping[str, Any]],
) -> Dict[str, Any]:
    trim_ids = {
        str(row["spu_id"]) for row in catalog_state if not int(row["is_series_node"])
    }
    return {
        "catalog": [row for row in catalog_state if str(row["spu_id"]) in trim_ids],
        "aliases": [row for row in alias_state if str(row["spu_id"]) in trim_ids],
    }


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
    catalog: FrozenSeriesCatalog,
    *,
    db_path: Path,
) -> ImportPlan:
    health = _database_health(connection)
    catalog_state = _catalog_state(connection)
    alias_state = _alias_state(connection)
    audience_state = _audience_state(connection)
    database_state_sha256 = _canonical_sha256(
        {"spu_catalog": catalog_state, "spu_alias": alias_state}
    )
    trim_state_sha256 = _canonical_sha256(_trim_state(catalog_state, alias_state))
    audience_state_sha256 = _canonical_sha256(audience_state)

    existing_by_id = {str(row["spu_id"]): row for row in catalog_state}
    series_by_slug: Dict[str, List[Dict[str, Any]]] = {}
    natural_by_key: Dict[Tuple[str, str], List[Dict[str, Any]]] = {}
    external_by_ref: Dict[str, List[Dict[str, Any]]] = {}
    for row in catalog_state:
        if int(row["is_series_node"]):
            series_by_slug.setdefault(str(row["series_slug"]), []).append(row)
            natural = (
                _normalized_text_key(str(row["brand"])),
                _normalized_text_key(str(row["series"])),
            )
            natural_by_key.setdefault(natural, []).append(row)
        external_ref = str(row.get("external_ref") or "")
        if external_ref:
            external_by_ref.setdefault(external_ref, []).append(row)
    duplicate_refs = {
        ref: rows for ref, rows in external_by_ref.items() if len(rows) > 1
    }
    if duplicate_refs:
        sample = sorted(duplicate_refs)[0]
        raise SpuSeriesCatalogImportError(
            f"existing database has duplicate external_ref {sample!r}"
        )
    duplicate_slugs = {
        slug: rows for slug, rows in series_by_slug.items() if len(rows) > 1
    }
    if duplicate_slugs:
        sample = sorted(duplicate_slugs)[0]
        raise SpuSeriesCatalogImportError(
            f"existing database has duplicate series_slug {sample!r}"
        )

    alias_by_target_key: Dict[Tuple[str, str], List[Dict[str, Any]]] = {}
    series_alias_by_key: Dict[str, List[Dict[str, Any]]] = {}
    for alias in alias_state:
        normalized = _normalized_text_key(str(alias["alias"]))
        alias_by_target_key.setdefault((str(alias["spu_id"]), normalized), []).append(
            alias
        )
        target = existing_by_id.get(str(alias["spu_id"]))
        if target is not None and int(target["is_series_node"]):
            series_alias_by_key.setdefault(normalized, []).append(alias)

    catalog_inserts: List[Mapping[str, Any]] = []
    catalog_updates: List[Mapping[str, Any]] = []
    alias_inserts: List[Mapping[str, Any]] = []
    alias_updates: List[Mapping[str, Any]] = []
    warnings: List[str] = []
    unchanged_catalog = 0
    unchanged_aliases = 0

    for spec in catalog.rows:
        target = existing_by_id.get(spec.spu_id)
        natural = (
            _normalized_text_key(spec.brand),
            _normalized_text_key(spec.series),
        )
        natural_matches = natural_by_key.get(natural, [])

        if spec.existing_series_slug is not None:
            if target is None or not int(target["is_series_node"]):
                raise SpuSeriesCatalogImportError(
                    f"existing_series_slug {spec.existing_series_slug!r} does not name "
                    "an existing series node"
                )
        elif target is None and natural_matches:
            matched = str(natural_matches[0]["series_slug"])
            raise SpuSeriesCatalogImportError(
                f"official series {spec.brand} {spec.series} already exists as {matched!r}; "
                "set existing_series_slug explicitly"
            )
        if target is not None and not int(target["is_series_node"]):
            raise SpuSeriesCatalogImportError(
                f"series spu_id {spec.spu_id!r} collides with a concrete trim"
            )
        if target is not None:
            if str(target["series_slug"]) != spec.series_slug:
                raise SpuSeriesCatalogImportError(
                    f"existing target {spec.spu_id!r} conflicts on series_slug"
                )
            for field, expected in {"brand": spec.brand, "series": spec.series}.items():
                if _normalized_text_key(str(target[field])) != _normalized_text_key(
                    expected
                ):
                    raise SpuSeriesCatalogImportError(
                        f"existing {spec.spu_id!r} conflicts on immutable {field}: "
                        f"{target[field]!r} != {expected!r}"
                    )
        other_natural = [
            row for row in natural_matches if str(row["spu_id"]) != spec.spu_id
        ]
        if other_natural:
            raise SpuSeriesCatalogImportError(
                f"natural series identity for {spec.spu_id!r} already belongs to "
                f"{other_natural[0]['spu_id']!r}"
            )
        referenced = external_by_ref.get(spec.external_ref, [])
        if referenced and str(referenced[0]["spu_id"]) != spec.spu_id:
            raise SpuSeriesCatalogImportError(
                f"external_ref {spec.external_ref!r} already belongs to "
                f"{referenced[0]['spu_id']!r}"
            )
        if target is not None:
            old_ref = str(target.get("external_ref") or "")
            if old_ref and old_ref != spec.external_ref:
                raise SpuSeriesCatalogImportError(
                    f"existing {spec.spu_id!r} has conflicting external_ref {old_ref!r}"
                )

        desired = {
            "spu_id": spec.spu_id,
            "brand": spec.brand,
            "series": spec.series,
            "series_slug": spec.series_slug,
            "trim_label": None,
            "is_series_node": 1,
            "model_year": None,
            "powertrain": "",
            "body_style": spec.body_style,
            "price_low": spec.price_low,
            "price_high": spec.price_high,
            "external_ref": spec.external_ref,
            "enabled": 1,
            "dcd_brand_id": spec.dcd_brand_id,
            "dcd_series_id": spec.dcd_series_id,
            "business_status": spec.business_status,
        }
        if target is None:
            catalog_inserts.append(desired)
        else:
            changes = _difference(target, desired, _CATALOG_MUTABLE_FIELDS)
            if changes:
                catalog_updates.append(
                    {"spu_id": spec.spu_id, "desired": desired, "changes": changes}
                )
            else:
                unchanged_catalog += 1

        for alias_spec in spec.aliases:
            normalized = _normalized_text_key(alias_spec.alias)
            target_matches = alias_by_target_key.get((spec.spu_id, normalized), [])
            other_aliases = [
                row
                for row in series_alias_by_key.get(normalized, [])
                if str(row["spu_id"]) != spec.spu_id
            ]
            if other_aliases and (
                not alias_spec.ambiguous
                or any(not int(row["ambiguous"]) for row in other_aliases)
            ):
                raise SpuSeriesCatalogImportError(
                    f"series alias {alias_spec.alias!r} is already used by another "
                    "series and must be ambiguous on every target"
                )
            desired_alias = {
                "spu_id": spec.spu_id,
                "alias": alias_spec.alias,
                "alias_type": alias_spec.alias_type,
                "spu_scope": "series",
                "ambiguous": 1 if alias_spec.ambiguous else 0,
                "enabled": 1 if alias_spec.enabled else 0,
            }
            if not target_matches:
                alias_inserts.append(desired_alias)
                continue
            # The legacy seed contains harmless case-only duplicates on the
            # same target (for example 秦PLUS/秦plus). Preserve every row, while
            # enforcing one safe metadata definition for the normalized group.
            for existing_alias in target_matches:
                if str(existing_alias["spu_scope"]) != "series":
                    raise SpuSeriesCatalogImportError(
                        f"existing alias {alias_spec.alias!r} for {spec.spu_id!r} "
                        "has non-series scope"
                    )
                changes = _difference(
                    existing_alias, desired_alias, _ALIAS_MUTABLE_FIELDS
                )
                if changes:
                    alias_updates.append(
                        {
                            "id": int(existing_alias["id"]),
                            "spu_id": spec.spu_id,
                            "alias": str(existing_alias["alias"]),
                            "desired": desired_alias,
                            "changes": changes,
                        }
                    )
                else:
                    unchanged_aliases += 1

    statuses: Dict[str, int] = {}
    for series_spec in catalog.rows:
        statuses[series_spec.business_status] = (
            statuses.get(series_spec.business_status, 0) + 1
        )

    inserts = _sort_operations(catalog_inserts)
    updates = _sort_operations(catalog_updates)
    alias_adds = _sort_operations(alias_inserts)
    alias_changes = _sort_operations(alias_updates)
    plan_material = {
        "schema_version": PLAN_SCHEMA_VERSION,
        "db_path": str(db_path.resolve()),
        "input_sha256": catalog.input_sha256,
        "database_state_sha256": database_state_sha256,
        "trim_state_sha256": trim_state_sha256,
        "audience_state_sha256": audience_state_sha256,
        "catalog_inserts": inserts,
        "catalog_updates": updates,
        "alias_inserts": alias_adds,
        "alias_updates": alias_changes,
    }
    return ImportPlan(
        db_path=db_path.resolve(),
        input_sha256=catalog.input_sha256,
        database_state_sha256=database_state_sha256,
        trim_state_sha256=trim_state_sha256,
        audience_state_sha256=audience_state_sha256,
        health=health,
        business_statuses=dict(sorted(statuses.items())),
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
    catalog: FrozenSeriesCatalog, *, db_path: Path = DEFAULT_DB
) -> ImportPlan:
    """Build a deterministic read-only plan for one schema-v15 database."""

    candidate = db_path.expanduser().absolute()
    if candidate.is_symlink():
        raise SpuSeriesCatalogImportError(
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
        raise SpuSeriesCatalogImportError(
            f"backup directory must not be a symlink: {backup_candidate}"
        )
    backup_root = backup_candidate.resolve()
    backup_root.mkdir(parents=True, exist_ok=True)
    stamp = datetime.now(timezone.utc).strftime("%Y%m%dT%H%M%S.%fZ")
    final_path = backup_root / (
        f"{db_path.stem}.before_dongchedi_spu_series_import.{stamp}{db_path.suffix}"
    )
    partial_path = final_path.with_name(final_path.name + ".partial")
    if final_path.exists() or partial_path.exists():
        raise SpuSeriesCatalogImportError(f"backup target already exists: {final_path}")
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
            ) VALUES (?,?,?,?,NULL,1,NULL,'',?,?,?,?,1,?,?)
            """,
            (
                row["spu_id"],
                row["brand"],
                row["series"],
                row["series_slug"],
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
            raise SpuSeriesCatalogImportError(
                "import plan contains an unsafe series update"
            )
        assignments = ",".join(f"{field}=?" for field in changed_fields)
        parameters = [desired[field] for field in changed_fields]
        cursor = connection.execute(
            f"UPDATE spu_catalog SET {assignments},updated_at=? WHERE spu_id=?",  # noqa: S608
            (*parameters, captured_at, operation["spu_id"]),
        )
        if cursor.rowcount != 1:
            raise SpuSeriesCatalogImportError(
                f"series update lost target {operation['spu_id']!r}"
            )
    for row in plan.alias_inserts:
        connection.execute(
            """
            INSERT INTO spu_alias(
                alias,alias_type,spu_scope,spu_id,ambiguous,enabled
            ) VALUES (?,?,'series',?,?,?)
            """,
            (
                row["alias"],
                row["alias_type"],
                row["spu_id"],
                row["ambiguous"],
                row["enabled"],
            ),
        )
    for operation in plan.alias_updates:
        desired = operation["desired"]
        changed_fields = sorted(operation["changes"])
        if not set(changed_fields).issubset(_ALIAS_MUTABLE_FIELDS):
            raise SpuSeriesCatalogImportError(
                "import plan contains an unsafe alias update"
            )
        assignments = ",".join(f"{field}=?" for field in changed_fields)
        parameters = [desired[field] for field in changed_fields]
        cursor = connection.execute(
            f"UPDATE spu_alias SET {assignments} WHERE id=?",  # noqa: S608
            (*parameters, operation["id"]),
        )
        if cursor.rowcount != 1:
            raise SpuSeriesCatalogImportError(
                f"alias update lost target id={operation['id']}"
            )


def _verify_post_write(
    connection: sqlite3.Connection,
    catalog: FrozenSeriesCatalog,
    *,
    expected_trim_state_sha256: str,
    expected_audience_state_sha256: str,
) -> None:
    catalog_state = _catalog_state(connection)
    alias_state = _alias_state(connection)
    if _canonical_sha256(_trim_state(catalog_state, alias_state)) != (
        expected_trim_state_sha256
    ):
        raise SpuSeriesCatalogImportError(
            "concrete trim rows or trim aliases changed during series import"
        )
    if _canonical_sha256(_audience_state(connection)) != expected_audience_state_sha256:
        raise SpuSeriesCatalogImportError(
            "audience mappings changed during series import"
        )
    for spec in catalog.rows:
        row = connection.execute(
            """
            SELECT spu_id,brand,series,series_slug,trim_label,is_series_node,
                   model_year,powertrain,body_style,price_low,price_high,
                   external_ref,enabled
            FROM spu_catalog WHERE spu_id=?
            """,
            (spec.spu_id,),
        ).fetchone()
        if row is None:
            raise SpuSeriesCatalogImportError(
                f"post-import series is missing: {spec.spu_id}"
            )
        expected = {
            "spu_id": spec.spu_id,
            "brand": spec.brand,
            "series": spec.series,
            "series_slug": spec.series_slug,
            "trim_label": None,
            "is_series_node": 1,
            "model_year": None,
            "powertrain": ""
            if spec.existing_series_slug is None
            else row["powertrain"],
            "body_style": spec.body_style,
            "price_low": spec.price_low,
            "price_high": spec.price_high,
            "external_ref": spec.external_ref,
            "enabled": 1,
        }
        for field, value in expected.items():
            if row[field] != value:
                raise SpuSeriesCatalogImportError(
                    f"post-import verification failed for {spec.spu_id}.{field}"
                )
        stored_aliases = connection.execute(
            """
            SELECT alias,alias_type,spu_scope,ambiguous,enabled
            FROM spu_alias WHERE spu_id=?
            """,
            (spec.spu_id,),
        ).fetchall()
        for alias in spec.aliases:
            normalized = [
                item
                for item in stored_aliases
                if _normalized_text_key(str(item["alias"]))
                == _normalized_text_key(alias.alias)
            ]
            if not normalized:
                raise SpuSeriesCatalogImportError(
                    f"post-import alias verification failed for {spec.spu_id}: "
                    f"{alias.alias!r}"
                )
            for stored in normalized:
                if (
                    str(stored["spu_scope"]) != "series"
                    or str(stored["alias_type"]) != alias.alias_type
                    or int(stored["ambiguous"]) != int(alias.ambiguous)
                    or int(stored["enabled"]) != int(alias.enabled)
                ):
                    raise SpuSeriesCatalogImportError(
                        f"post-import alias metadata mismatch for {spec.spu_id}: "
                        f"{alias.alias!r}"
                    )
    quick_rows = [str(row[0]) for row in connection.execute("PRAGMA quick_check")]
    if quick_rows != ["ok"]:
        raise SpuSeriesCatalogImportError(
            f"post-import quick_check failed: {quick_rows[:5]}"
        )
    violations = connection.execute("PRAGMA foreign_key_check").fetchall()
    if violations:
        raise SpuSeriesCatalogImportError(
            f"post-import foreign_key_check found {len(violations)} violation(s)"
        )


def _receipt(
    catalog: FrozenSeriesCatalog,
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
            "business_statuses": dict(plan.business_statuses),
            "attestation": dict(catalog.source_attestation),
            "artifact_summary": dict(catalog.artifact_summary),
            "series_id_audit": [
                {
                    "series_slug": row.series_slug,
                    "dcd_brand_id": row.dcd_brand_id,
                    "primary_dcd_series_id": row.dcd_series_id,
                    "represented_official_series_ids": list(
                        row.represented_official_series_ids
                    ),
                    "external_ref": row.external_ref,
                }
                for row in catalog.rows
            ],
        },
        "database": {
            "path": str(plan.db_path),
            "before_state_sha256": plan.database_state_sha256,
            "after_state_sha256": after_database_state_sha256,
            "before_health": dict(plan.health),
            "after_health": dict(after_health) if after_health is not None else None,
            "protected_trim_state_sha256": plan.trim_state_sha256,
            "protected_audience_state_sha256": plan.audience_state_sha256,
        },
        "plan_sha256": plan.plan_sha256,
        "operations": plan.operation_counts(),
        "changed_ids": plan.changed_ids(),
        "warnings": list(plan.warnings),
        "backup": dict(backup) if backup is not None else None,
        "full_association_recompute_required": bool(applied and plan.change_count),
    }


def write_receipt(receipt: Mapping[str, Any], path: Path) -> None:
    destination = path.expanduser().resolve()
    destination.parent.mkdir(parents=True, exist_ok=True)
    temporary = destination.with_name(destination.name + ".partial")
    if temporary.exists():
        raise SpuSeriesCatalogImportError(
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
) -> Dict[str, Any]:
    """Dry-run or atomically apply one frozen official series catalog."""

    resolved_input = input_path.expanduser().resolve()
    resolved_db_argument = db_path.expanduser().resolve()
    if resolved_input in _protected_sqlite_paths(resolved_db_argument):
        raise SpuSeriesCatalogImportError(
            "input artifact must not be the SQLite database or one of its sidecars"
        )
    if receipt_path is not None:
        resolved_receipt = receipt_path.expanduser().resolve()
        protected = _protected_sqlite_paths(resolved_db_argument) | {resolved_input}
        if resolved_receipt in protected:
            raise SpuSeriesCatalogImportError(
                "receipt path must not overwrite the input artifact or SQLite "
                "database/sidecars"
            )
    catalog = load_frozen_series_catalog(input_path)
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
        raise SpuSeriesCatalogImportError(
            "--apply requires the 64-character --expect-plan-sha256 from dry-run"
        )
    if initial_plan.plan_sha256 != expected:
        raise SpuSeriesCatalogImportError(
            "dry-run plan hash does not match current database/input state: "
            f"expected {expected}, current {initial_plan.plan_sha256}"
        )
    if skip_backup and is_formal_database_path(
        resolved_db, formal_database=DEFAULT_DB
    ):
        raise SpuSeriesCatalogImportError(
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
                raise SpuSeriesCatalogImportError(
                    "database changed after dry-run; rerun dry-run and approve the "
                    "new plan hash"
                )
            if locked_plan.change_count:
                if not skip_backup:
                    backup = _online_backup(
                        resolved_db,
                        backup_dir or (resolved_db.parent / "backups"),
                    )
                _apply_plan(connection, locked_plan, captured_at=now_utc())
                _verify_post_write(
                    connection,
                    catalog,
                    expected_trim_state_sha256=locked_plan.trim_state_sha256,
                    expected_audience_state_sha256=locked_plan.audience_state_sha256,
                )
            connection.commit()
        except Exception:
            connection.rollback()
            raise

    after_plan = build_import_plan(catalog, db_path=resolved_db)
    if after_plan.change_count:
        raise SpuSeriesCatalogImportError(
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
    "APPROVED_BRANDS_REQUESTED",
    "APPROVED_BUSINESS_STATUS_CODES",
    "APPROVED_CITY_NAME",
    "AliasSpec",
    "BRAND_CATALOG_ENDPOINT",
    "BRAND_SERIES_ENDPOINT",
    "FrozenSeriesCatalog",
    "ImportPlan",
    "INPUT_SCHEMA_VERSION",
    "PLAN_SCHEMA_VERSION",
    "RECEIPT_SCHEMA_VERSION",
    "SOURCE_ARTIFACT_SCHEMA",
    "SeriesSpec",
    "SpuSeriesCatalogImportError",
    "build_import_plan",
    "execute_import",
    "load_frozen_series_catalog",
    "write_receipt",
]
