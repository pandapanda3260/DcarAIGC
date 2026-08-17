"""Fetch and normalize recent Dongchedi trims without writing SQLite.

The endpoint used here is Dongchedi's website backend rather than a documented
public API.  Treat every response as untrusted input: the normalizer validates
the mapped series, keeps the fetch bounded, and emits a deterministic JSON
artifact that can be reviewed before a separate importer touches the database.
"""

from __future__ import annotations

import concurrent.futures
import hashlib
import json
import os
import re
import sqlite3
import time
import urllib.parse
import urllib.request
import uuid
from collections import Counter
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Callable, Dict, Iterable, List, Mapping, Sequence, Tuple


SOURCE_SCHEMA = "dcar-dongchedi-trim-catalog-v1"
NORMALIZED_SCHEMA_VERSION = "dcar-dongchedi-spu-trims-normalized-v1"
SOURCE_PROVIDER = "dongchedi"
SERIES_ALL_ENDPOINT = "https://www.dongchedi.com/motor/car_page/m/v1/series_all_json/"
DEFAULT_CITY_NAME = "北京"
DEFAULT_OFFLINE_MIN_MODEL_YEAR = 2024
MAX_WORKERS = 8
MAX_RESPONSE_BYTES = 50 * 1024 * 1024

_ROW_POWERTRAINS = frozenset({"ice", "hev", "phev", "erev", "ev"})
_SOURCE_POWERTRAINS = _ROW_POWERTRAINS | {"mixed"}

FetchJson = Callable[[str, float], Mapping[str, Any]]
Sleep = Callable[[float], None]

_YEAR_PREFIX_RE = re.compile(r"^\s*(20\d{2})\s*款\s*[-—–:]?\s*")
_YEAR_ANYWHERE_RE = re.compile(r"(20\d{2})\s*款")
_PRICE_RE = re.compile(r"(-?\d+(?:\.\d+)?)")
_SPACE_RE = re.compile(r"\s+")

_GENERIC_TRIM_ALIASES = {
    "pro",
    "max",
    "ultra",
    "plus",
    "air",
    "旗舰版",
    "豪华版",
    "尊贵版",
    "精英版",
    "标准版",
    "入门版",
    "舒适版",
    "运动版",
    "领先版",
    "智享版",
    "悦享版",
    "卓越版",
}


class DongchediSourceError(RuntimeError):
    """Raised when mapping, local metadata, or a source response is unsafe."""


def _utc_now() -> str:
    return (
        datetime.now(timezone.utc).isoformat(timespec="seconds").replace("+00:00", "Z")
    )


def _canonical_json(value: Any) -> bytes:
    return json.dumps(
        value,
        ensure_ascii=False,
        sort_keys=True,
        separators=(",", ":"),
    ).encode("utf-8")


def load_mapping(path: Path) -> Tuple[Dict[str, Any], str]:
    """Load the exact mapping file and return it with its byte-level SHA-256."""

    try:
        raw = path.read_bytes()
    except OSError as exc:
        raise DongchediSourceError(f"无法读取车系映射：{path}: {exc}") from exc
    try:
        decoded = json.loads(raw.decode("utf-8"))
    except (UnicodeDecodeError, json.JSONDecodeError) as exc:
        raise DongchediSourceError(
            f"车系映射不是有效 UTF-8 JSON：{path}: {exc}"
        ) from exc
    if not isinstance(decoded, dict):
        raise DongchediSourceError("车系映射顶层必须是 JSON object")
    _validate_mapping(decoded)
    return decoded, hashlib.sha256(raw).hexdigest()


def _validate_mapping(mapping: Mapping[str, Any]) -> None:
    version = mapping.get("mapping_version")
    entries = mapping.get("entries")
    unresolved = mapping.get("unresolved", [])
    if not isinstance(version, str) or not version.strip():
        raise DongchediSourceError("车系映射缺少 mapping_version")
    if not isinstance(entries, dict) or not entries:
        raise DongchediSourceError("车系映射 entries 必须是非空 object")
    if not isinstance(unresolved, list):
        raise DongchediSourceError("车系映射 unresolved 必须是 array")

    unresolved_slugs: set[str] = set()
    for item in unresolved:
        if not isinstance(item, dict):
            raise DongchediSourceError("unresolved 每项必须是 object")
        slug = item.get("series_slug")
        reason = item.get("reason")
        if not isinstance(slug, str) or not slug:
            raise DongchediSourceError("unresolved 项缺少 series_slug")
        if not isinstance(reason, str) or not reason.strip():
            raise DongchediSourceError(f"unresolved {slug} 缺少 reason")
        if slug in unresolved_slugs:
            raise DongchediSourceError(f"unresolved 重复 series_slug：{slug}")
        unresolved_slugs.add(slug)

    seen_series_ids: Dict[int, str] = {}
    for slug, entry in entries.items():
        if not isinstance(slug, str) or not slug:
            raise DongchediSourceError("entries 包含空 series_slug")
        if not isinstance(entry, dict):
            raise DongchediSourceError(f"{slug} 的映射必须是 object")
        default_powertrain = entry.get("default_powertrain")
        if default_powertrain not in _ROW_POWERTRAINS:
            raise DongchediSourceError(
                f"{slug} 的 default_powertrain 非法：{default_powertrain!r}"
            )
        resolution = entry.get("powertrain_resolution")
        if resolution not in {"fixed", "infer_from_official_series_or_trim_spec"}:
            raise DongchediSourceError(
                f"{slug} 的 powertrain_resolution 非法：{resolution!r}"
            )
        official_series = entry.get("official_series")
        if not isinstance(official_series, list) or not official_series:
            raise DongchediSourceError(f"{slug} 缺少 official_series")
        for source in official_series:
            if not isinstance(source, dict):
                raise DongchediSourceError(f"{slug} 的 official_series 项必须是 object")
            try:
                series_id = int(source["series_id"])
            except (KeyError, TypeError, ValueError) as exc:
                raise DongchediSourceError(f"{slug} 存在非法 series_id") from exc
            if series_id <= 0:
                raise DongchediSourceError(f"{slug} 存在非法 series_id={series_id}")
            official_name = source.get("official_name")
            if not isinstance(official_name, str) or not official_name.strip():
                raise DongchediSourceError(f"{slug}/{series_id} 缺少 official_name")
            if not isinstance(source.get("include_recent"), bool):
                raise DongchediSourceError(
                    f"{slug}/{series_id} 的 include_recent 必须是 boolean"
                )
            if source.get("powertrain") not in _SOURCE_POWERTRAINS:
                raise DongchediSourceError(
                    f"{slug}/{series_id} 的 powertrain 非法：{source.get('powertrain')!r}"
                )
            previous = seen_series_ids.get(series_id)
            if previous is not None and previous != slug:
                raise DongchediSourceError(
                    f"官方 series_id={series_id} 同时映射到 {previous} 与 {slug}"
                )
            seen_series_ids[series_id] = slug

        has_recent = any(bool(item.get("include_recent")) for item in official_series)
        if (
            any(item.get("powertrain") == "mixed" for item in official_series)
            and resolution != "infer_from_official_series_or_trim_spec"
        ):
            raise DongchediSourceError(
                f"{slug} 含 mixed 官方车系，必须启用具体款型能源推断"
            )
        if slug in unresolved_slugs and has_recent:
            raise DongchediSourceError(
                f"{slug} 已标记 unresolved，不能同时 include_recent=true"
            )
        if not has_recent and slug not in unresolved_slugs:
            raise DongchediSourceError(
                f"{slug} 没有 include_recent=true，且未显式记入 unresolved"
            )

    unknown_unresolved = unresolved_slugs.difference(entries)
    if unknown_unresolved:
        raise DongchediSourceError(
            "unresolved 引用了未配置车系：" + ", ".join(sorted(unknown_unresolved))
        )


def load_local_series_nodes(
    db_path: Path, expected_slugs: Iterable[str]
) -> Dict[str, Dict[str, Any]]:
    """Read enabled series-node metadata through a read-only SQLite handle."""

    if not db_path.is_file():
        raise DongchediSourceError(f"正式数据库不存在：{db_path}")
    uri = f"{db_path.resolve().as_uri()}?mode=ro"
    try:
        connection = sqlite3.connect(uri, uri=True, timeout=10)
        connection.row_factory = sqlite3.Row
        connection.execute("PRAGMA query_only = ON")
        rows = connection.execute(
            """
            SELECT spu_id, brand, series, series_slug, powertrain, body_style
            FROM spu_catalog
            WHERE is_series_node=1 AND enabled=1
            """
        ).fetchall()
    except sqlite3.Error as exc:
        raise DongchediSourceError(f"读取正式车型库失败：{db_path}: {exc}") from exc
    finally:
        if "connection" in locals():
            connection.close()

    local = {
        str(row["series_slug"]): {
            "spu_id": str(row["spu_id"]),
            "brand": str(row["brand"]),
            "series": str(row["series"]),
            "series_slug": str(row["series_slug"]),
            "powertrain": str(row["powertrain"] or ""),
            "body_style": str(row["body_style"] or ""),
        }
        for row in rows
    }
    missing = sorted(set(expected_slugs).difference(local))
    if missing:
        raise DongchediSourceError(
            "正式车型库缺少或禁用了映射中的车系节点：" + ", ".join(missing)
        )
    return local


def _default_fetch_json(url: str, timeout: float) -> Mapping[str, Any]:
    parsed = urllib.parse.urlsplit(url)
    series_id = urllib.parse.parse_qs(parsed.query).get("series_id", [""])[0]
    request = urllib.request.Request(
        url,
        headers={
            "Accept": "application/json,text/plain,*/*",
            "Referer": f"https://www.dongchedi.com/auto/series/{series_id}",
            "User-Agent": (
                "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) "
                "AppleWebKit/537.36 (KHTML, like Gecko) "
                "Chrome/136.0 Safari/537.36"
            ),
        },
    )
    with urllib.request.urlopen(request, timeout=timeout) as response:
        status = getattr(response, "status", 200)
        if int(status) != 200:
            raise DongchediSourceError(f"HTTP {status}: {url}")
        raw = response.read(MAX_RESPONSE_BYTES + 1)
        if len(raw) > MAX_RESPONSE_BYTES:
            raise DongchediSourceError(
                f"官方接口响应超过 {MAX_RESPONSE_BYTES} bytes 上限：{url}"
            )
    try:
        decoded = json.loads(raw.decode("utf-8"))
    except (UnicodeDecodeError, json.JSONDecodeError) as exc:
        raise DongchediSourceError(f"官方接口返回非 UTF-8 JSON：{url}") from exc
    if not isinstance(decoded, dict):
        raise DongchediSourceError(f"官方接口 JSON 顶层不是 object：{url}")
    return decoded


def _series_url(series_id: int, city_name: str) -> str:
    query = urllib.parse.urlencode(
        {
            "series_id": series_id,
            "city_name": city_name,
            "show_city_price": 1,
            "m_station_dealer_price_v": 1,
        }
    )
    return f"{SERIES_ALL_ENDPOINT}?{query}"


def _normalized_name(value: Any) -> str:
    return _SPACE_RE.sub("", str(value or "")).casefold()


def _fetch_validated_response(
    *,
    source: Mapping[str, Any],
    city_name: str,
    timeout: float,
    retries: int,
    fetch_json: FetchJson,
    sleep_fn: Sleep,
) -> Tuple[Mapping[str, Any], str, str]:
    series_id = int(source["series_id"])
    official_name = str(source["official_name"])
    url = _series_url(series_id, city_name)
    last_error: Exception | None = None
    for attempt in range(1, retries + 1):
        try:
            payload = fetch_json(url, timeout)
            if not isinstance(payload, Mapping):
                raise DongchediSourceError("响应顶层不是 object")
            if payload.get("status") != "success":
                raise DongchediSourceError(
                    f"status={payload.get('status')!r}, message={payload.get('message')!r}"
                )
            data = payload.get("data")
            if not isinstance(data, Mapping):
                raise DongchediSourceError("响应缺少 data object")
            try:
                actual_series_id = int(data.get("series_id"))
            except (TypeError, ValueError) as exc:
                raise DongchediSourceError("data.series_id 非法") from exc
            if actual_series_id != series_id:
                raise DongchediSourceError(
                    f"series_id 错位：期望 {series_id}，实际 {actual_series_id}"
                )
            actual_name = str(data.get("series_name") or "").strip()
            if not actual_name:
                raise DongchediSourceError("data.series_name 为空")
            if _normalized_name(actual_name) != _normalized_name(official_name):
                raise DongchediSourceError(
                    f"车系名错位：期望 {official_name!r}，实际 {actual_name!r}"
                )
            for bucket in ("online", "offline"):
                bucket_value = data.get(bucket, [])
                if bucket_value in (None, 0):
                    continue
                if not isinstance(bucket_value, list):
                    raise DongchediSourceError(
                        f"data.{bucket} 应为 array 或 0，实际 {type(bucket_value).__name__}"
                    )
            response_sha256 = hashlib.sha256(_canonical_json(payload)).hexdigest()
            return data, url, response_sha256
        except Exception as exc:  # retry transport failures and schema drift alike
            last_error = exc
            if attempt < retries:
                sleep_fn(0.5 * (2 ** (attempt - 1)))
    raise DongchediSourceError(
        f"官方车系 {series_id}（{official_name}）抓取/校验失败，已尝试 {retries} 次：{last_error}"
    ) from last_error


def _iter_car_info(data: Mapping[str, Any], bucket: str) -> Iterable[Mapping[str, Any]]:
    value = data.get(bucket, [])
    if value in (None, 0):
        return
    assert isinstance(value, list)  # checked by _fetch_validated_response
    for item in value:
        if not isinstance(item, Mapping):
            raise DongchediSourceError(f"data.{bucket} 包含非 object 项")
        if str(item.get("type")) != "1037":
            continue
        info = item.get("info")
        if not isinstance(info, Mapping):
            raise DongchediSourceError(f"data.{bucket} 的 1037 项缺少 info object")
        yield info


def _positive_int(value: Any, field: str) -> int:
    try:
        parsed = int(value)
    except (TypeError, ValueError) as exc:
        raise DongchediSourceError(f"{field} 非法：{value!r}") from exc
    if parsed <= 0:
        raise DongchediSourceError(f"{field} 必须大于 0：{value!r}")
    return parsed


def _model_year(info: Mapping[str, Any], trim_label: str) -> int:
    value = info.get("year")
    if value not in (None, ""):
        return _positive_int(value, "year")
    match = _YEAR_ANYWHERE_RE.search(trim_label)
    if match is None:
        raise DongchediSourceError(f"款型缺少可解析年款：{trim_label!r}")
    return int(match.group(1))


def _parse_price_value(value: Any) -> float | None:
    if value in (None, "", "暂无报价", "暂无"):
        return None
    if isinstance(value, bool):
        return None
    if isinstance(value, (int, float)):
        parsed = float(value)
    else:
        match = _PRICE_RE.search(str(value).replace(",", ""))
        if match is None:
            return None
        parsed = float(match.group(1))
    if parsed <= 0 or parsed >= 1000:
        return None
    return round(parsed, 4)


def _official_price(info: Mapping[str, Any]) -> float | None:
    """Return the official price in 万元, accommodating offline row shape."""

    direct = _parse_price_value(info.get("official_price"))
    if direct is not None:
        return direct
    price_info = info.get("price_info")
    if isinstance(price_info, Mapping):
        nested = _parse_price_value(price_info.get("official_price"))
        if nested is not None:
            return nested
    # Current offline 1037 rows expose the same guide price as "12.34万" only.
    return _parse_price_value(info.get("price"))


def _compact(value: str) -> str:
    return _SPACE_RE.sub("", value)


def _specific_configuration_alias(value: str) -> bool:
    compact = _compact(value).strip("-—–:：/()（）")
    if len(compact) < 4:
        return False
    if compact.casefold() in _GENERIC_TRIM_ALIASES:
        return False
    return True


def build_trim_aliases(trim_label: str, car_name: str = "") -> List[Dict[str, Any]]:
    """Build space-tolerant aliases while rejecting short generic variants."""

    candidates: List[str] = []

    def append(value: str) -> None:
        normalized = _SPACE_RE.sub(" ", value).strip()
        if normalized and normalized not in candidates:
            candidates.append(normalized)
        compact = _compact(normalized)
        if compact and compact not in candidates:
            candidates.append(compact)

    append(trim_label)
    without_year = _YEAR_PREFIX_RE.sub("", trim_label).strip()
    if without_year != trim_label and _specific_configuration_alias(without_year):
        append(without_year)
    cleaned_car_name = _SPACE_RE.sub(" ", str(car_name or "")).strip()
    if cleaned_car_name and _specific_configuration_alias(cleaned_car_name):
        append(cleaned_car_name)
    return [
        {"alias": alias, "alias_type": "official", "ambiguous": False}
        for alias in candidates
    ]


def _infer_mixed_powertrain(text: str, *, fallback: str) -> str:
    lowered = _compact(text).casefold()
    if any(token in lowered for token in ("增程", "erev", "rangeextender")):
        return "erev"
    if any(
        token in lowered
        for token in (
            "phev",
            "插电",
            "插混",
            "dm-i",
            "dmi",
            "dm-p",
            "dmp",
            "hi4-t",
        )
    ):
        return "phev"
    if any(
        token in lowered
        for token in (
            "双擎",
            "hev",
            "油混",
            "混动",
            "油电混动",
            "油电混合",
            "e-power",
            "超混电驱",
            "智擎",
            "雷神混动",
            "混动版",
        )
    ):
        return "hev"
    if (
        "纯电" in lowered
        or "bev" in lowered
        or re.search(r"(?:^|[^a-z])ev(?:$|[^a-z])", lowered)
    ):
        return "ev"
    if any(token in lowered for token in ("燃油", "汽油", "柴油")):
        return "ice"
    if re.search(
        r"(?:^|[^0-9])(?:1\.[0-9]|2\.[0-9]|3\.[0-9]|4\.[0-9])(?:t|l)",
        lowered,
    ):
        return "ice"
    return fallback


def _row_from_info(
    *,
    info: Mapping[str, Any],
    source: Mapping[str, Any],
    local: Mapping[str, Any],
    series_slug: str,
    actual_series_name: str,
    bucket: str,
    offline_min_model_year: int,
    max_model_year: int,
    default_powertrain: str,
) -> Dict[str, Any] | None:
    trim_label = _SPACE_RE.sub(" ", str(info.get("name") or "")).strip()
    if not trim_label:
        raise DongchediSourceError(
            f"series_id={source['series_id']} 的 {bucket} 1037 款型缺少 name"
        )
    year = _model_year(info, trim_label)
    if year > max_model_year:
        return None
    if bucket == "offline" and year < offline_min_model_year:
        return None
    car_id = _positive_int(info.get("car_id", info.get("id")), "car_id")
    row_series_id = info.get("series_id")
    if row_series_id not in (None, "") and int(row_series_id) != int(
        source["series_id"]
    ):
        raise DongchediSourceError(
            f"car_id={car_id} 的 series_id={row_series_id} 与响应车系 {source['series_id']} 不一致"
        )
    configured_powertrain = str(source.get("powertrain") or "").strip()
    if configured_powertrain and configured_powertrain != "mixed":
        powertrain = configured_powertrain
    else:
        inference_text = " ".join(
            [
                actual_series_name,
                trim_label,
                str(info.get("car_name") or ""),
                " ".join(str(item) for item in info.get("tags", []) if item),
            ]
        )
        powertrain = _infer_mixed_powertrain(
            inference_text,
            fallback=str(default_powertrain or local.get("powertrain") or ""),
        )
    if powertrain not in _ROW_POWERTRAINS:
        raise DongchediSourceError(
            f"car_id={car_id} 归一化后的 powertrain 非法：{powertrain!r}"
        )
    price = _official_price(info)
    return {
        "series_slug": series_slug,
        "brand": str(local["brand"]),
        "series": str(local["series"]),
        "dcd_series_id": int(source["series_id"]),
        "dcd_series_name": actual_series_name,
        "car_id": car_id,
        "trim_label": trim_label,
        "model_year": year,
        "powertrain": powertrain,
        "body_style": str(local.get("body_style") or ""),
        "price_low": price,
        "price_high": price,
        "aliases": build_trim_aliases(trim_label, str(info.get("car_name") or "")),
        "source_bucket": bucket,
    }


def _deduplicate_rows(rows: Sequence[Dict[str, Any]]) -> List[Dict[str, Any]]:
    """Deduplicate globally by official car_id, preferring an online occurrence."""

    by_car_id: Dict[int, Dict[str, Any]] = {}
    for row in rows:
        car_id = int(row["car_id"])
        existing = by_car_id.get(car_id)
        if existing is None:
            by_car_id[car_id] = row
            continue
        if existing["series_slug"] != row["series_slug"]:
            raise DongchediSourceError(
                f"car_id={car_id} 同时归到 {existing['series_slug']} 与 {row['series_slug']}"
            )
        identity_fields = ("dcd_series_id", "trim_label", "model_year")
        if any(existing[field] != row[field] for field in identity_fields):
            raise DongchediSourceError(f"car_id={car_id} 在官方响应中存在冲突元数据")
        if existing["source_bucket"] != "online" and row["source_bucket"] == "online":
            by_car_id[car_id] = row

    result = list(by_car_id.values())
    alias_counts: Counter[Tuple[str, str]] = Counter()
    for row in result:
        for alias in row["aliases"]:
            alias_counts[(str(row["series_slug"]), str(alias["alias"]).casefold())] += 1
    for row in result:
        for alias in row["aliases"]:
            alias["ambiguous"] = (
                alias_counts[(str(row["series_slug"]), str(alias["alias"]).casefold())]
                > 1
            )
    result.sort(
        key=lambda row: (
            str(row["series_slug"]),
            -int(row["model_year"]),
            int(row["dcd_series_id"]),
            str(row["trim_label"]),
            int(row["car_id"]),
        )
    )
    return result


def build_normalized_catalog(
    *,
    mapping: Mapping[str, Any],
    local_series: Mapping[str, Mapping[str, Any]],
    mapping_sha256: str = "",
    offline_min_model_year: int = DEFAULT_OFFLINE_MIN_MODEL_YEAR,
    max_model_year: int,
    city_name: str = DEFAULT_CITY_NAME,
    workers: int = 4,
    timeout: float = 20.0,
    retries: int = 3,
    fetch_json: FetchJson = _default_fetch_json,
    sleep_fn: Sleep = time.sleep,
) -> Dict[str, Any]:
    """Fetch all include_recent sources and return one normalized artifact."""

    _validate_mapping(mapping)
    if offline_min_model_year < 1900 or max_model_year < offline_min_model_year:
        raise DongchediSourceError(
            f"非法停售年款范围：{offline_min_model_year}..{max_model_year}"
        )
    if not city_name.strip():
        raise DongchediSourceError("city_name 不能为空")
    if workers < 1 or workers > MAX_WORKERS:
        raise DongchediSourceError(f"workers 必须在 1..{MAX_WORKERS} 之间")
    if timeout <= 0:
        raise DongchediSourceError("timeout 必须大于 0")
    if retries < 1 or retries > 6:
        raise DongchediSourceError("retries 必须在 1..6 之间")

    entries = mapping["entries"]
    assert isinstance(entries, Mapping)
    missing_local = sorted(set(entries).difference(local_series))
    if missing_local:
        raise DongchediSourceError(
            "缺少正式车系节点元数据：" + ", ".join(missing_local)
        )

    jobs: List[Tuple[str, Mapping[str, Any], Mapping[str, Any]]] = []
    for series_slug, entry_value in entries.items():
        entry = entry_value
        assert isinstance(entry, Mapping)
        for source_value in entry["official_series"]:
            source = source_value
            assert isinstance(source, Mapping)
            if bool(source["include_recent"]):
                jobs.append((str(series_slug), entry, source))

    fetched: List[
        Tuple[str, Mapping[str, Any], Mapping[str, Any], Mapping[str, Any], str, str]
    ] = []

    def fetch_job(
        job: Tuple[str, Mapping[str, Any], Mapping[str, Any]],
    ) -> Tuple[str, Mapping[str, Any], Mapping[str, Any], Mapping[str, Any], str, str]:
        slug, entry, source = job
        data, url, response_sha256 = _fetch_validated_response(
            source=source,
            city_name=city_name,
            timeout=timeout,
            retries=retries,
            fetch_json=fetch_json,
            sleep_fn=sleep_fn,
        )
        return slug, entry, source, data, url, response_sha256

    with concurrent.futures.ThreadPoolExecutor(max_workers=workers) as pool:
        futures = [pool.submit(fetch_job, job) for job in jobs]
        try:
            for future in concurrent.futures.as_completed(futures):
                fetched.append(future.result())
        except Exception:
            for future in futures:
                future.cancel()
            raise

    raw_rows: List[Dict[str, Any]] = []
    series_summaries: List[Dict[str, Any]] = []
    for slug, entry, source, data, url, response_sha256 in sorted(
        fetched, key=lambda item: (item[0], int(item[2]["series_id"]))
    ):
        actual_series_name = str(data["series_name"]).strip()
        local = local_series[slug]
        default_powertrain = str(
            entry.get("default_powertrain") or local.get("powertrain") or ""
        )
        source_rows: List[Dict[str, Any]] = []
        bucket_seen: Dict[str, int] = {"online": 0, "offline": 0}
        bucket_selected: Dict[str, int] = {"online": 0, "offline": 0}
        for bucket in ("online", "offline"):
            for info in _iter_car_info(data, bucket):
                bucket_seen[bucket] += 1
                row = _row_from_info(
                    info=info,
                    source=source,
                    local=local,
                    series_slug=slug,
                    actual_series_name=actual_series_name,
                    bucket=bucket,
                    offline_min_model_year=offline_min_model_year,
                    max_model_year=max_model_year,
                    default_powertrain=default_powertrain,
                )
                if row is not None:
                    source_rows.append(row)
                    bucket_selected[bucket] += 1
        if not source_rows:
            raise DongchediSourceError(
                "include_recent 官方子车系在当前窗口内没有任何款型，拒绝静默少导："
                f"{slug}/{source['series_id']}（{actual_series_name}）"
            )
        raw_rows.extend(source_rows)
        series_summaries.append(
            {
                "series_slug": slug,
                "dcd_series_id": int(source["series_id"]),
                "dcd_series_name": actual_series_name,
                "url": url,
                "response_sha256": response_sha256,
                "online_seen": bucket_seen["online"],
                "online_selected": bucket_selected["online"],
                "offline_seen": bucket_seen["offline"],
                "offline_selected": bucket_selected["offline"],
            }
        )

    rows = _deduplicate_rows(raw_rows)
    unresolved_records: List[Dict[str, Any]] = []
    for item in mapping.get("unresolved", []):
        assert isinstance(item, Mapping)
        slug = str(item["series_slug"])
        local = local_series[slug]
        unresolved_records.append(
            {
                "series_slug": slug,
                "brand": str(local["brand"]),
                "series": str(local["series"]),
                "reason": str(item["reason"]),
            }
        )

    row_slugs = {str(row["series_slug"]) for row in rows}
    unresolved_slugs = {str(row["series_slug"]) for row in unresolved_records}
    if row_slugs & unresolved_slugs:
        raise DongchediSourceError("unresolved 车系不能同时产出近期款型")
    configured_slugs = {str(slug) for slug in entries}
    if row_slugs | unresolved_slugs != configured_slugs:
        missing = sorted(configured_slugs - row_slugs - unresolved_slugs)
        raise DongchediSourceError(
            "近期款型与 unresolved 未完整覆盖映射车系：" + ", ".join(missing)
        )

    source_counts = Counter(str(row["source_bucket"]) for row in rows)
    slug_counts = Counter(str(row["series_slug"]) for row in rows)
    empty_recent_slugs = sorted(
        slug
        for slug in entries
        if slug not in {str(item["series_slug"]) for item in unresolved_records}
        and slug_counts[slug] == 0
    )
    if empty_recent_slugs:
        raise DongchediSourceError(
            "以下非 unresolved 车系在年款窗口内没有任何款型，拒绝生成："
            + ", ".join(empty_recent_slugs)
        )

    effective_mapping_sha = (
        mapping_sha256 or hashlib.sha256(_canonical_json(mapping)).hexdigest()
    )
    result: Dict[str, Any] = {
        "schema": SOURCE_SCHEMA,
        "schema_version": NORMALIZED_SCHEMA_VERSION,
        "generated_at": _utc_now(),
        "source": {
            "provider": SOURCE_PROVIDER,
            "endpoint": SERIES_ALL_ENDPOINT,
            "mapping_version": str(mapping["mapping_version"]),
            "mapping_sha256": effective_mapping_sha,
            "city_name": city_name,
            "buckets_included": ["online", "offline"],
            "buckets_excluded": ["presale_car", "urban"],
            "online_policy": "all_through_max_model_year",
            "offline_min_model_year": offline_min_model_year,
            "max_model_year": max_model_year,
        },
        "summary": {
            "configured_series_slugs": len(entries),
            "resolved_series_slugs": len(slug_counts),
            "unresolved_series_slugs": len(unresolved_records),
            "official_series_requests": len(jobs),
            "rows": len(rows),
            "online_rows": int(source_counts["online"]),
            "offline_rows": int(source_counts["offline"]),
        },
        "unresolved": unresolved_records,
        "series_summaries": series_summaries,
        "rows": rows,
    }
    result["catalog_sha256"] = hashlib.sha256(_canonical_json(result)).hexdigest()
    return result


def fetch_normalized_catalog(
    *,
    mapping_path: Path,
    db_path: Path,
    offline_min_model_year: int,
    max_model_year: int,
    city_name: str = DEFAULT_CITY_NAME,
    workers: int = 4,
    timeout: float = 20.0,
    retries: int = 3,
    fetch_json: FetchJson = _default_fetch_json,
    sleep_fn: Sleep = time.sleep,
) -> Dict[str, Any]:
    mapping, mapping_sha256 = load_mapping(mapping_path)
    entries = mapping["entries"]
    assert isinstance(entries, Mapping)
    local = load_local_series_nodes(db_path, entries.keys())
    return build_normalized_catalog(
        mapping=mapping,
        local_series=local,
        mapping_sha256=mapping_sha256,
        offline_min_model_year=offline_min_model_year,
        max_model_year=max_model_year,
        city_name=city_name,
        workers=workers,
        timeout=timeout,
        retries=retries,
        fetch_json=fetch_json,
        sleep_fn=sleep_fn,
    )


def write_json_atomic(path: Path, value: Mapping[str, Any]) -> None:
    """Write one UTF-8 JSON file by fsyncing then replacing in the same folder."""

    path.parent.mkdir(parents=True, exist_ok=True)
    temporary = path.with_name(f".{path.name}.{os.getpid()}.{uuid.uuid4().hex}.tmp")
    try:
        with temporary.open("x", encoding="utf-8") as handle:
            json.dump(value, handle, ensure_ascii=False, indent=2, sort_keys=True)
            handle.write("\n")
            handle.flush()
            os.fsync(handle.fileno())
        os.replace(temporary, path)
    except Exception:
        try:
            temporary.unlink()
        except FileNotFoundError:
            pass
        raise


__all__ = [
    "DEFAULT_CITY_NAME",
    "DEFAULT_OFFLINE_MIN_MODEL_YEAR",
    "DongchediSourceError",
    "SERIES_ALL_ENDPOINT",
    "NORMALIZED_SCHEMA_VERSION",
    "SOURCE_SCHEMA",
    "build_normalized_catalog",
    "build_trim_aliases",
    "fetch_normalized_catalog",
    "load_local_series_nodes",
    "load_mapping",
    "write_json_atomic",
]
