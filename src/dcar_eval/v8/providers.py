"""Verified supplier adapters for one-row and scheduled v8 capture updates."""

from __future__ import annotations

import hashlib
import json
import math
import os
import sqlite3
import time
import urllib.parse
import urllib.request
from dataclasses import replace
from datetime import date, datetime
from functools import partial
from pathlib import Path
from typing import Any, Callable, Dict, List, Mapping, NoReturn, Optional, Sequence
from zoneinfo import ZoneInfo

from tikhub_config import (
    DEFAULT_TIKHUB_API_BASE,
    DEFAULT_TIKHUB_CONFIG_FILE,
    TikHubConfigurationError,
    load_tikhub_api_base,
    load_tikhub_api_key,
    resolve_tikhub_transport_manifest,
)
from workflow.privacy import CommentHasher  # type: ignore[import-not-found,import-untyped]

from .capture import (
    CaptureError,
    CaptureOutcome,
    ProviderResult,
    RawResponseIntegrityError,
    SlotUnavailable,
    execute_account_fetch,
    execute_content_fetch,
    execute_derived_content_fetch,
    load_succeeded_raw_response,
    mark_fetch_slot_terminal_failure,
)
from .duplicates import refresh_content_duplicates
from .evaluation import evaluate_content, upsert_comment_user_scores
from .comment_paging import (
    COMMENT_CAP,
    COVERAGE_TARGET,
    PageFetch,
    PageFetchDeferred,
    capture_content_comments,
    page_window_key,
)
from .identity import (
    PlatformUserHasher,
    comment_identity_key,
    insert_comment_rows,
    legacy_user_score_rows,
    normalized_parent_comment_id,
)
from .media import (
    get_media_source_state,
    is_supported_media_url,
    processor_versions,
    process_content_media,
    recover_stale_media_processing_slots,
    store_media_source_manifest,
)
from .metric_observations import persist_metric_observation
from .source_routing import select_content_metrics
from .operations import (
    IdentityConflictError,
    reconcile_content_account_identity,
    upsert_content,
)
from .provider_budget import (
    DEFAULT_TASK_MAX_AMOUNT_USD,
    task_budget_id,
    task_budget_purpose,
)
from .paid_identity import PaidRequestIdentity, build_paid_request_identity
from .provider_transport import (
    JsonTransportResult,
    ProviderTransportError,
    request_json as request_json_transport,
)
from .storage import DEFAULT_DB, connect, now_utc, transaction


SHANGHAI = ZoneInfo("Asia/Shanghai")
TIKHUB_KEY_FILE = DEFAULT_TIKHUB_CONFIG_FILE
RNOTE_KEY_FILE = Path("/Users/mark/Documents/key/DcarKey/Rnote.env.local")
RNOTE_BASE = "https://rnote.dev/api/v2/crawler/note"
TIKHUB_PRICE = 0.001
TIKHUB_DOUYIN_HIGH_QUALITY_PRICE = 0.005
TIKHUB_XHS_PRICE = 0.01
RNOTE_PRICE = 0.008
RNOTE_RETIRED_MESSAGE = "Rnote retired; use TikHub"
PRICE_VERIFIED_AT = "2026-08-02T13:55:00Z"
DISCOVERY_PRICE_VERIFIED_AT = "2026-08-02T14:49:00Z"
TIKHUB_XHS_PRICE_VERIFIED_AT = "2026-08-03T06:28:14Z"
XHS_TYPE_PROBE_WINDOW = "xhs-type-probe-v1"
XHS_TYPE_PROBE_ADAPTER = "tikhub-xhs-type-probe-v1"


STAGE_CONFIG = {
    ("douyin", "detail"): (
        "TikHub",
        "tikhub-detail-v8.0",
        "douyin_video_detail",
        TIKHUB_PRICE,
    ),
    ("douyin", "metrics"): (
        "TikHub",
        "tikhub-statistics-v8.0",
        "douyin_video_statistics",
        TIKHUB_PRICE,
    ),
    ("douyin", "comments"): (
        "TikHub",
        "tikhub-comments-v8.0",
        "douyin_video_comments",
        TIKHUB_PRICE,
    ),
    ("xiaohongshu", "detail"): (
        "TikHub",
        "tikhub-xhs-app-v2-detail-v8.1",
        "xiaohongshu_note_detail",
        TIKHUB_XHS_PRICE,
    ),
    ("xiaohongshu", "metrics"): (
        "TikHub",
        "tikhub-xhs-app-v2-statistics-v8.1",
        "xiaohongshu_note_statistics",
        TIKHUB_XHS_PRICE,
    ),
    ("xiaohongshu", "comments"): (
        "TikHub",
        "tikhub-xhs-app-v2-comments-v8.1",
        "xiaohongshu_note_comments",
        TIKHUB_XHS_PRICE,
    ),
}


class ProviderConfigurationError(RuntimeError):
    pass


def _load_key(path: Path, variable: str) -> str:
    if variable == "TIKHUB_API_KEY":
        try:
            load_tikhub_api_base(path)
            return load_tikhub_api_key(path)
        except TikHubConfigurationError as error:
            raise ProviderConfigurationError(str(error)) from error
    direct_value = os.environ.get(variable, "").strip()
    if direct_value:
        return direct_value
    configured_path = os.environ.get(f"{variable}_FILE", "").strip()
    if configured_path:
        path = Path(configured_path).expanduser()
    if not path.is_file():
        raise ProviderConfigurationError(f"供应商凭据文件不存在：{path}")
    text = path.read_text(encoding="utf-8-sig").strip()
    for line in text.splitlines():
        line = line.strip()
        if not line or line.startswith("#"):
            continue
        if "=" in line:
            name, value = line.split("=", 1)
            if name.strip() == variable:
                secret = value.strip().strip("\"'")
                if secret:
                    return secret
        elif len(text.splitlines()) == 1:
            return line.strip("\"'")
    raise ProviderConfigurationError(f"{variable} 未配置")


def _load_tikhub_base(path: Path | None = None) -> str:
    try:
        return load_tikhub_api_base(path or TIKHUB_KEY_FILE)
    except TikHubConfigurationError as error:
        raise ProviderConfigurationError(str(error)) from error


# Keep API imports independent of credential availability.  _load_key validates
# the configured base at the provider access boundary before returning a key.
TIKHUB_BASE = DEFAULT_TIKHUB_API_BASE


def _tikhub_url(path: str) -> str:
    """Resolve the sealed provider route at the request-construction boundary."""

    if not path.startswith("/"):
        raise ProviderConfigurationError("TikHub endpoint path must be absolute")
    from .provider_transport import current_request_transport
    frozen = current_request_transport()
    base = frozen["manifest"]["api_base"] if frozen is not None else _load_tikhub_base()
    return f"{base}{path}"


def _freeze_tikhub_transport(call_override: Any = None) -> dict[str, Any] | None:
    """Injected fixture responses are not qualified live transports."""
    if call_override is not None:
        return None
    return {
        "manifest": resolve_tikhub_transport_manifest(TIKHUB_KEY_FILE),
        "config_path": str(TIKHUB_KEY_FILE), "honor_environment": True,
    }


def _paid_request_identity(
    *,
    operation: str,
    platform: str,
    subject: str,
    params: Mapping[str, Any],
    cursor: Any,
    due_bucket: str,
    request_window: Mapping[str, str] | None = None,
) -> PaidRequestIdentity:
    return build_paid_request_identity(
        provider="TikHub",
        operation=operation,
        platform=platform,
        subject=subject,
        request_parameters=params,
        cursor=cursor,
        due_bucket=due_bucket,
        request_window=request_window,
    )


def ensure_operational_budget(
    *, provider: str, operation: str, price: float, db_path: Path
) -> str:
    budget_id = f"operational-{provider.lower()}-{operation}-v1"
    captured_at = now_utc()
    max_requests = 1000 if provider == "TikHub" else 100
    daily_quota = 100 if provider == "TikHub" else 20
    with connect(db_path) as connection, transaction(connection):
        row = connection.execute(
            "SELECT * FROM provider_budget_batches WHERE id=?", (budget_id,)
        ).fetchone()
        if row is None:
            connection.execute(
                """
                INSERT INTO provider_budget_batches(
                    id, purpose, provider, operation, currency, verified_unit_price,
                    max_billable_requests, max_amount, pilot_size, daily_quota,
                    price_verified_at, status, created_at, updated_at
                ) VALUES (?, ?, ?, ?, 'USD', ?, ?, ?, 0, ?, ?, 'approved', ?, ?)
                """,
                (
                    budget_id,
                    f"v8_{operation}",
                    provider,
                    operation,
                    price,
                    max_requests,
                    round(max_requests * price, 6),
                    daily_quota,
                    PRICE_VERIFIED_AT,
                    captured_at,
                    captured_at,
                ),
            )
        else:
            if (
                row["provider"] != provider
                or row["operation"] != operation
                or abs(float(row["verified_unit_price"]) - price) > 1e-9
            ):
                raise ProviderConfigurationError("现有供应商预算与已核验价格不一致")
            if row["status"] not in {"approved", "pilot"}:
                raise ProviderConfigurationError(
                    f"供应商预算状态为 {row['status']}，已阻断调用"
                )
    return budget_id


def ensure_task_budget(
    *,
    provider: str,
    operation: str,
    price: float,
    task_id: str,
    task_max_amount: float,
    db_path: Path,
) -> str:
    if not task_id.strip() or task_max_amount <= 0:
        raise ProviderConfigurationError("任务预算必须包含有效 task_id 和正数上限")
    budget_id = task_budget_id(task_id, provider, operation)
    max_requests = max(1, math.floor((task_max_amount + 1e-9) / price))
    captured_at = now_utc()
    verified_at = (
        TIKHUB_XHS_PRICE_VERIFIED_AT
        if provider == "TikHub" and abs(price - TIKHUB_XHS_PRICE) < 1e-9
        else PRICE_VERIFIED_AT
    )
    with connect(db_path) as connection, transaction(connection):
        row = connection.execute(
            "SELECT * FROM provider_budget_batches WHERE id=?", (budget_id,)
        ).fetchone()
        if row is None:
            connection.execute(
                """
                INSERT INTO provider_budget_batches(
                    id, purpose, provider, operation, currency, verified_unit_price,
                    max_billable_requests, max_amount, pilot_size, daily_quota,
                    price_verified_at, status, created_at, updated_at
                ) VALUES (?, ?, ?, ?, 'USD', ?, ?, ?, 0, ?, ?, 'approved', ?, ?)
                """,
                (
                    budget_id,
                    task_budget_purpose(task_id, operation),
                    provider,
                    operation,
                    price,
                    max_requests,
                    task_max_amount,
                    max_requests,
                    verified_at,
                    captured_at,
                    captured_at,
                ),
            )
        else:
            if (
                row["provider"] != provider
                or row["operation"] != operation
                or abs(float(row["verified_unit_price"]) - price) > 1e-9
                or abs(float(row["max_amount"]) - task_max_amount) > 1e-9
            ):
                raise ProviderConfigurationError("现有任务预算与本次任务合同不一致")
            if row["status"] not in {"approved", "pilot"}:
                raise ProviderConfigurationError(
                    f"任务预算状态为 {row['status']}，已阻断调用"
                )
    return budget_id


def _budget_for_call(
    *,
    provider: str,
    operation: str,
    price: float,
    task_id: Optional[str],
    task_max_amount: Optional[float],
    db_path: Path,
) -> str:
    if task_id is None and task_max_amount is None:
        return ensure_operational_budget(
            provider=provider, operation=operation, price=price, db_path=db_path
        )
    if task_id is None or task_max_amount is None:
        raise ProviderConfigurationError("task_id 与 task_max_amount 必须同时提供")
    return ensure_task_budget(
        provider=provider,
        operation=operation,
        price=price,
        task_id=task_id,
        task_max_amount=task_max_amount,
        db_path=db_path,
    )


def _request_json(
    url: str,
    *,
    headers: Mapping[str, str],
    params: Mapping[str, Any],
    provider: str,
) -> JsonTransportResult:
    from .provider_transport import current_request_transport
    binding = current_request_transport()
    manifest = binding["manifest"] if binding is not None else None
    if manifest is not None:
        selected = urllib.parse.urlsplit(url)
        if provider.lower() != "tikhub" or f"{selected.scheme}://{selected.netloc}" != manifest["api_base"]:
            raise ProviderConfigurationError("Request URL differs from the frozen paid transport")
    query = urllib.parse.urlencode(
        {key: value for key, value in params.items() if value is not None}
    )
    request = urllib.request.Request(
        f"{url}?{query}",
        headers={
            **headers,
            "Accept": "application/json",
            "User-Agent": "DCar-Insight-v8/1.0",
        },
    )
    try:
        parsed_url = urllib.parse.urlsplit(url)
        host = parsed_url.hostname or "unknown"
        route_base = f"{parsed_url.scheme.lower()}://{parsed_url.netloc.lower()}"
        route_generation = (
            "route-config-sha256:"
            + hashlib.sha256(route_base.encode("utf-8")).hexdigest()
        )
        response = request_json_transport(
            request,
            route_id=manifest["transport_route_id"] if manifest else f"{provider.lower()}-{host}-stream-v1",
            route_generation=manifest["route_generation"] if manifest else route_generation,
            http_stack=manifest["http_stack"] if manifest else "urllib-stream-v1",
            timeout=45,
        )
    except ProviderTransportError as exc:
        raise CaptureError(
            f"{provider} transport error: {exc.error_code}",
            retryable=True,
            error_code="transport_error",
            transport_receipt=exc.receipt,
            transport_partial=exc.partial_bytes,
        ) from exc
    if manifest is not None:
        for field in ("request_host", "transport_route_id", "route_generation", "http_stack"):
            if response.receipt.get(field) != manifest[field]:
                raise CaptureError(
                    "Response transport differs from the frozen paid request",
                    retryable=False, error_code="transport_error",
                    transport_receipt={**response.receipt, "error_code": "transport_binding_mismatch"},
                    transport_partial=response.encoded_body,
                )
        response.receipt["request_transport_config_sha256"] = manifest["config_sha256"]
    status = response.status
    payload = response.payload
    if status >= 300:
        payload_text = json.dumps(payload, ensure_ascii=False).lower()
        provider_retry_requested = (
            provider == "TikHub"
            and status == 400
            and ("please retry" in payload_text or "请重试" in payload_text)
        )
        explicitly_unbilled = provider == "TikHub" and (
            "won't be charged" in payload_text
            or "will not be charged" in payload_text
            or "不会被扣费" in payload_text
        )
        provider_blocked = status in {401, 402, 403}
        terminal = status in {400, 404, 410, 422} and not provider_retry_requested
        retry_after_seconds = None
        retry_header = response.receipt.get("retry_after")
        if retry_header:
            try:
                retry_after_seconds = max(0.0, float(retry_header))
            except ValueError:
                from email.utils import parsedate_to_datetime

                try:
                    retry_after_seconds = max(0.0, (parsedate_to_datetime(retry_header) - datetime.now(SHANGHAI)).total_seconds())
                except (TypeError, ValueError):
                    pass
        raise CaptureError(
            f"{provider} HTTP {status}",
            retryable=status == 402
            or provider_retry_requested
            or (
                status not in {401, 403}
                and not terminal
                and (status in {408, 429} or status >= 500)
            ),
            error_code=(
                "provider_balance_blocked"
                if status == 402
                else "provider_auth_blocked"
                if provider_blocked
                else "provider_retry_requested"
                if provider_retry_requested
                else f"http_{status}"
            ),
            http_status=status,
            billed=(
                False
                if status in {401, 402, 403} or explicitly_unbilled
                else None
            ),
            raw_response=payload,
            entity_bytes=response.entity_body,
            transport_receipt=response.receipt,
            retry_after_seconds=retry_after_seconds,
        )
    return response


def _with_transport(
    response: JsonTransportResult | tuple[int, Any],
    result: ProviderResult,
) -> ProviderResult:
    """Attach true pre-parse bytes when the live transport produced them.

    Tuple results remain accepted for fixture-only parser tests.  They cannot
    qualify a live transport campaign because they carry no transport receipt.
    """

    if not isinstance(response, JsonTransportResult):
        return result
    return replace(
        result,
        entity_bytes=response.entity_body,
        transport_receipt=response.receipt,
    )


def _raise_with_transport(
    error: CaptureError,
    response: JsonTransportResult | tuple[int, Any],
) -> NoReturn:
    if isinstance(response, JsonTransportResult):
        error.entity_bytes = response.entity_body
        error.transport_receipt = response.receipt
    raise error


def _raise_parse_failure_with_transport(
    error: Exception,
    response: JsonTransportResult | tuple[int, Any],
    *,
    payload: Any,
    status: int,
) -> NoReturn:
    """Turn unexpected parser failures into evidenced provider-contract errors."""

    if isinstance(error, CaptureError):
        _raise_with_transport(error, response)
    wrapped = CaptureError(
        f"TikHub response parser failed: {type(error).__name__}",
        retryable=False,
        error_code="invalid_response",
        http_status=status,
        billed=True,
        raw_response=payload,
    )
    try:
        _raise_with_transport(wrapped, response)
    except CaptureError as failure:
        raise failure from error


def _tikhub_data(payload: Any) -> Any:
    if not isinstance(payload, dict) or payload.get("code") != 200:
        message = (
            payload.get("message_zh")
            if isinstance(payload, dict)
            else "invalid response"
        )
        raise CaptureError(
            f"TikHub semantic error: {message}",
            retryable=True,
            error_code="semantic_error",
            http_status=200,
            billed=False,
            raw_response=payload,
        )
    return payload.get("data")


def _tikhub_douyin_data(payload: Any) -> Any:
    """Unwrap TikHub and reject Douyin's HTTP-200 business errors."""

    value = _tikhub_data(payload)
    if isinstance(value, dict):
        upstream_status = value.get("status_code")
        if upstream_status not in (None, 0, "0"):
            message = value.get("status_msg") or value.get("message") or upstream_status
            raise CaptureError(
                f"TikHub Douyin upstream error: {message}",
                retryable=False if str(upstream_status) == "5" else True,
                error_code=(
                    "upstream_invalid_request"
                    if str(upstream_status) == "5"
                    else "upstream_error"
                ),
                http_status=200,
                billed=True,
                raw_response=payload,
            )
    return value


def _tikhub_xhs_data(payload: Any) -> Any:
    """Unwrap TikHub's envelope and the Xiaohongshu App V2 upstream envelope."""

    value = _tikhub_data(payload)
    if not isinstance(value, dict):
        raise CaptureError(
            "TikHub Xiaohongshu response omitted upstream envelope",
            retryable=True,
            error_code="invalid_response",
            http_status=200,
            billed=True,
            raw_response=payload,
        )
    upstream_code = value.get("code")
    if value.get("success") is False or upstream_code not in (None, 0, "0", 200, "200"):
        raise CaptureError(
            f"TikHub Xiaohongshu upstream error: {value.get('msg') or value.get('message')}",
            retryable=True,
            error_code="upstream_error",
            http_status=200,
            billed=True,
            raw_response=payload,
        )
    if "data" not in value:
        raise CaptureError(
            "TikHub Xiaohongshu response omitted data",
            retryable=True,
            error_code="invalid_response",
            http_status=200,
            billed=True,
            raw_response=payload,
        )
    return value["data"]


def _find_aweme(value: Any, aweme_id: str) -> Optional[Dict[str, Any]]:
    if isinstance(value, dict):
        if str(value.get("aweme_id") or "") == aweme_id and (
            "desc" in value or "statistics" in value or "author" in value
        ):
            return value
        for child in value.values():
            found = _find_aweme(child, aweme_id)
            if found:
                return found
    elif isinstance(value, list):
        for child in value:
            found = _find_aweme(child, aweme_id)
            if found:
                return found
    return None


def _first_int(value: Mapping[str, Any], *names: str) -> Optional[int]:
    for name in names:
        raw = value.get(name)
        if isinstance(raw, bool) or raw is None:
            continue
        try:
            return int(str(raw).replace(",", ""))
        except ValueError:
            continue
    return None


def _mapping(value: Any) -> Mapping[str, Any]:
    return value if isinstance(value, dict) else {}


def _bool(value: Any) -> bool:
    if isinstance(value, str):
        return value.strip().lower() in {"1", "true", "yes"}
    return bool(value)


def _discovery_has_more(
    page: Mapping[str, Any], *, provider: str, raw_response: Any
) -> bool:
    """Parse pagination without treating a missing field as exhaustion."""

    if "has_more" not in page:
        raise CaptureError(
            f"{provider} discovery response omitted has_more",
            retryable=True,
            error_code="invalid_response",
            http_status=200,
            billed=True,
            raw_response=raw_response,
        )
    value = page["has_more"]
    if isinstance(value, bool):
        return value
    if isinstance(value, (int, float)) and not isinstance(value, bool):
        if value in {0, 1}:
            return bool(value)
    if isinstance(value, str):
        normalized = value.strip().lower()
        if normalized in {"0", "false", "no"}:
            return False
        if normalized in {"1", "true", "yes"}:
            return True
    raise CaptureError(
        f"{provider} discovery response has invalid has_more",
        retryable=True,
        error_code="invalid_response",
        http_status=200,
        billed=True,
        raw_response=raw_response,
    )


def _timestamp_iso(value: Any) -> Optional[str]:
    if value in (None, "", 0, "0"):
        return None
    try:
        timestamp = float(value)
    except (TypeError, ValueError):
        text = str(value).strip()
        if not text:
            return None
        try:
            parsed = datetime.fromisoformat(text.replace("Z", "+00:00"))
        except ValueError:
            return None
        if parsed.tzinfo is None:
            parsed = parsed.replace(tzinfo=SHANGHAI)
        return parsed.astimezone(ZoneInfo("UTC")).isoformat().replace("+00:00", "Z")
    if timestamp > 100_000_000_000:
        timestamp /= 1000
    try:
        return (
            datetime.fromtimestamp(timestamp, tz=ZoneInfo("UTC"))
            .isoformat()
            .replace("+00:00", "Z")
        )
    except (OverflowError, OSError, ValueError):
        return None


def _find_string(value: Any, name: str) -> Optional[str]:
    if isinstance(value, dict):
        candidate = value.get(name)
        if candidate not in (None, ""):
            return str(candidate)
        for child in value.values():
            found = _find_string(child, name)
            if found:
                return found
    elif isinstance(value, list):
        for child in value:
            found = _find_string(child, name)
            if found:
                return found
    return None


def _collect_dicts(value: Any, identity_name: str) -> List[Mapping[str, Any]]:
    found: List[Mapping[str, Any]] = []
    seen: set[str] = set()

    def visit(child: Any) -> None:
        if isinstance(child, dict):
            identity = child.get(identity_name)
            if identity not in (None, ""):
                key = str(identity)
                if key not in seen:
                    seen.add(key)
                    found.append(child)
                return
            for nested in child.values():
                visit(nested)
        elif isinstance(child, list):
            for nested in child:
                visit(nested)

    visit(value)
    return found


def _collect_media_urls(value: Any, media_kind: str) -> List[str]:
    """Extract provider media URLs while excluding avatars and unrelated navigation assets."""

    positive: tuple[str, ...]
    if media_kind == "video":
        positive = ("video", "play", "stream", "h264", "h265", "master", "bit_rate")
    elif media_kind == "image":
        positive = (
            "image",
            "images",
            "image_list",
            "imageinfo",
            "url_default",
            "url_pre",
        )
    else:
        raise ValueError(f"unsupported media kind: {media_kind}")
    excluded = (
        "avatar",
        "author",
        "user",
        "music",
        "share",
        "icon",
        "cover",
        "subtitle",
    )
    output: List[str] = []

    def visit(child: Any, path: str) -> None:
        if isinstance(child, str):
            lowered = path.lower()
            if (
                child.startswith("https://")
                and any(token in lowered for token in positive)
                and not any(token in lowered for token in excluded)
                and child not in output
            ):
                output.append(child)
        elif isinstance(child, dict):
            for key, nested in child.items():
                visit(nested, f"{path}.{key}" if path else str(key))
        elif isinstance(child, list):
            for index, nested in enumerate(child):
                visit(nested, f"{path}[{index}]")

    visit(value, "")
    return output


def _douyin_image_url_groups(item: Mapping[str, Any]) -> List[List[str]]:
    """Preserve TikHub's one-``images[]``-entry-per-logical-image grouping.

    Douyin exposes several format and CDN candidates for each logical image.
    The frozen media source remains the same flat URL list; callers that have
    the discovery payload can additionally retain these candidate boundaries.
    """

    raw_images = item.get("images")
    images = raw_images if isinstance(raw_images, list) else []
    groups: List[List[str]] = []
    for raw_image in images:
        image = _mapping(raw_image)
        candidates = _collect_media_urls(
            {
                "images": [
                    {
                        "download_url_list": image.get("download_url_list"),
                        "url_list": image.get("url_list"),
                    }
                ]
            },
            "image",
        )
        if candidates:
            groups.append(candidates)
    return groups


def _douyin_media_urls(item: Mapping[str, Any], content_type: str) -> List[str]:
    if content_type == "image":
        return list(
            dict.fromkeys(
                url
                for group in _douyin_image_url_groups(item)
                for url in group
            )
        )
    video = _mapping(item.get("video"))
    return _collect_media_urls(
        {
            "play_addr": video.get("play_addr"),
            "download_addr": video.get("download_addr"),
            "bit_rate": video.get("bit_rate"),
        },
        "video",
    )


def _xhs_known_type(value: Any) -> str:
    return {"normal": "image", "image": "image", "video": "video"}.get(
        str(value or "").strip().lower(), "unknown"
    )


def _xhs_media_urls(item: Mapping[str, Any], content_type: str) -> List[str]:
    if content_type not in {"image", "video"}:
        return []
    if content_type == "image":
        images = item.get("images_list") or item.get("image_list") or item.get("images")
        return _collect_media_urls({"images_list": images}, "image")

    values: List[str] = []
    video_info = _mapping(item.get("video_info_v2") or item.get("video_info"))
    media = _mapping(video_info.get("media"))
    stream = _mapping(media.get("stream"))
    for codec in ("h264", "h265", "h266", "av1"):
        raw_entries = stream.get(codec)
        entries = raw_entries if isinstance(raw_entries, list) else []
        ordered = sorted(
            (_mapping(entry) for entry in entries),
            key=lambda entry: int(_bool(entry.get("default_stream"))),
            reverse=True,
        )
        for entry in ordered:
            candidates = [entry.get("master_url")]
            backups = entry.get("backup_urls")
            if isinstance(backups, list):
                candidates.extend(backups)
            for candidate in candidates:
                if (
                    isinstance(candidate, str)
                    and is_supported_media_url(candidate)
                    and candidate not in values
                ):
                    values.append(candidate)
    return values


def _douyin_reference_call(uid: str, key: str) -> ProviderResult:
    response = _request_json(
        _tikhub_url("/api/v1/douyin/web/fetch_user_profile_by_uid"),
        headers={"Authorization": f"Bearer {key}"},
        params={"uid": uid},
        provider="TikHub",
    )
    status, payload = response
    try:
        result = _parse_douyin_reference_payload(payload, status=status)
    except Exception as error:
        _raise_parse_failure_with_transport(
            error, response, payload=payload, status=status
        )
    return _with_transport(response, result)


def _valid_douyin_sec_user_id(value: str) -> bool:
    return value.startswith("MS4wLjAB") and 40 <= len(value) <= 128


def _parse_douyin_reference_payload(
    payload: Any, *, status: int = 200
) -> ProviderResult:
    data = (
        payload.get("data")
        if isinstance(payload, dict) and "code" not in payload
        else _tikhub_douyin_data(payload)
    )
    reference = _find_string(data, "sec_user_id") or _find_string(data, "sec_uid")
    if not reference or not _valid_douyin_sec_user_id(reference):
        raise CaptureError(
            "TikHub UID profile omitted a valid App V3 sec_user_id",
            retryable=True,
            error_code="invalid_response",
            http_status=status,
            billed=True,
            raw_response=payload,
        )
    return ProviderResult({"reference": reference}, payload, status, True)


def _douyin_discovery_call(
    sec_user_id: str, key: str, max_cursor: Any = 0
) -> ProviderResult:
    response = _request_json(
        _tikhub_url("/api/v1/douyin/app/v3/fetch_user_post_videos"),
        headers={"Authorization": f"Bearer {key}"},
        params={
            "sec_user_id": sec_user_id,
            "max_cursor": max_cursor or 0,
            "count": 20,
            "sort_type": 0,
        },
        provider="TikHub",
    )
    status, payload = response
    try:
        result = _parse_douyin_discovery_payload(payload, status=status)
    except Exception as error:
        _raise_parse_failure_with_transport(
            error, response, payload=payload, status=status
        )
    return _with_transport(response, result)


def _parse_douyin_discovery_payload(
    payload: Any, *, status: int = 200
) -> ProviderResult:
    data = (
        payload.get("data")
        if isinstance(payload, dict) and "code" not in payload
        else _tikhub_douyin_data(payload)
    )
    page = data if isinstance(data, dict) else {}
    items = []
    for item in _collect_dicts(data, "aweme_id"):
        aweme_id = str(item["aweme_id"])
        author = _mapping(item.get("author"))
        content_type = "image" if item.get("images") else "video"
        statistics = _mapping(item.get("statistics"))
        items.append(
            {
                "platform": "douyin",
                "platform_content_id": aweme_id,
                "canonical_url": f"https://www.douyin.com/video/{aweme_id}",
                "title": str(item.get("desc") or ""),
                "body": str(item.get("desc") or ""),
                "published_at": item.get("create_time"),
                "content_type": content_type,
                "account_uid": str(author.get("uid") or ""),
                "account_name": str(author.get("nickname") or ""),
                "media_urls": _douyin_media_urls(item, content_type),
                "metrics": _count_fields(statistics, _DOUYIN_METRIC_ALIASES, platform="douyin"),
            }
        )
    return ProviderResult(
        {
            "items": items,
            "next_cursor": page.get("max_cursor", page.get("cursor")),
            "has_more": _discovery_has_more(
                page, provider="TikHub Douyin", raw_response=payload
            ),
        },
        payload,
        status,
        True,
    )


def _find_list_page(value: Any, list_name: str) -> Optional[Mapping[str, Any]]:
    if isinstance(value, dict):
        if isinstance(value.get(list_name), list):
            return value
        for child in value.values():
            found = _find_list_page(child, list_name)
            if found is not None:
                return found
    elif isinstance(value, list):
        for child in value:
            found = _find_list_page(child, list_name)
            if found is not None:
                return found
    return None


def _parse_xhs_discovery_payload(payload: Any) -> Dict[str, Any]:
    """Normalize one App V2 posted-notes page without performing any I/O."""

    data = (
        _tikhub_xhs_data(payload)
        if (isinstance(payload, dict) and "code" in payload and "data" in payload)
        else payload
    )
    page = _find_list_page(data, "notes")
    if page is None:
        raise CaptureError(
            "TikHub Xiaohongshu discovery response omitted notes",
            retryable=True,
            error_code="invalid_response",
            http_status=200,
            billed=True,
            raw_response=payload,
        )
    raw_items = page.get("notes") or []
    items: List[Dict[str, Any]] = []
    for raw_item in raw_items:
        item = _mapping(raw_item)
        card = _mapping(item.get("note_card") or item.get("note")) or item
        note_id = str(
            item.get("note_id") or card.get("note_id") or card.get("id") or ""
        ).strip()
        if not note_id:
            continue
        user = _mapping(card.get("user") or item.get("user"))
        item_type = str(card.get("type") or item.get("type") or "").lower()
        published = (
            card.get("time")
            or card.get("publish_time")
            or card.get("create_time")
            or item.get("time")
            or item.get("publish_time")
            or item.get("create_time")
        )
        items.append(
            {
                "platform": "xiaohongshu",
                "platform_content_id": note_id,
                "canonical_url": f"https://www.xiaohongshu.com/explore/{note_id}",
                "title": str(
                    card.get("title")
                    or card.get("display_title")
                    or item.get("display_title")
                    or ""
                ),
                "body": str(
                    card.get("desc")
                    or card.get("description")
                    or item.get("desc")
                    or item.get("description")
                    or ""
                ),
                "published_at": published,
                "content_type": _xhs_known_type(item_type),
                "account_uid": str(
                    user.get("userid")
                    or user.get("user_id")
                    or user.get("id")
                    or user.get("uid")
                    or ""
                ),
                "account_name": str(user.get("nickname") or user.get("name") or ""),
                "media_urls": _xhs_media_urls(
                    card, _xhs_known_type(item_type)
                ),
                "metrics": _xhs_metrics(card),
            }
        )
    next_cursor = page.get("cursor") or page.get("next_cursor")
    if next_cursor in (None, "") and raw_items:
        last = _mapping(raw_items[-1])
        next_cursor = last.get("cursor") or last.get("next_cursor")
    return {
        "items": items,
        "next_cursor": next_cursor,
        "has_more": _discovery_has_more(
            page, provider="TikHub Xiaohongshu", raw_response=payload
        ),
    }


def _xhs_discovery_call(uid: str, key: str, cursor: Any = "") -> ProviderResult:
    response = _request_json(
        _tikhub_url("/api/v1/xiaohongshu/app_v2/get_user_posted_notes"),
        headers={"Authorization": f"Bearer {key}"},
        params={"user_id": uid, "cursor": cursor or ""},
        provider="TikHub",
    )
    status, payload = response
    try:
        normalized = _parse_xhs_discovery_payload(payload)
    except Exception as error:
        _raise_parse_failure_with_transport(
            error, response, payload=payload, status=status
        )
    return _with_transport(response, ProviderResult(normalized, payload, status, True))


def _rnote_discovery_call(uid: str, key: str) -> ProviderResult:
    raise ProviderConfigurationError(RNOTE_RETIRED_MESSAGE)

    # Historical parser/call shape remains below for audit compatibility. The
    # fail-closed guard ensures no Rnote request can be issued.
    status, payload = _request_json(
        "https://rnote.dev/api/v2/crawler/user/posted",
        headers={"X-API-Key": key},
        params={"user_id": uid, "cursor": "", "num": 20},
        provider="Rnote",
    )
    if not isinstance(payload, dict) or status != 200:
        raise CaptureError(
            "Rnote user posts returned invalid response",
            retryable=True,
            error_code="invalid_response",
            http_status=status,
            billed=False,
            raw_response=payload,
        )
    data = payload.get("data", payload)
    items = []
    for item in _collect_dicts(data, "note_id"):
        note_id = str(item["note_id"])
        items.append(
            {
                "platform": "xiaohongshu",
                "platform_content_id": note_id,
                "canonical_url": f"https://www.xiaohongshu.com/explore/{note_id}",
                "title": str(item.get("title") or ""),
                "body": str(item.get("desc") or item.get("description") or ""),
                "published_at": item.get("time")
                or item.get("publish_time")
                or item.get("published_at"),
                "content_type": "video"
                if item.get("type") in {"video", 1, "1"}
                else "image",
                "account_name": str(item.get("nickname") or ""),
            }
        )
    return ProviderResult({"items": items}, payload, status, True)


def _douyin_request(
    stage: str,
    content_id: str,
    cursor: Optional[Mapping[str, Any]] = None,
) -> tuple[str, Dict[str, Any]]:
    comment_cursor = int((cursor or {}).get("cursor") or 0)
    endpoints: Dict[str, tuple[str, Dict[str, Any]]] = {
        "detail": ("/api/v1/douyin/app/v3/fetch_one_video", {"aweme_id": content_id}),
        "metrics": (
            "/api/v1/douyin/app/v3/fetch_video_statistics",
            {"aweme_ids": content_id},
        ),
        "comments": (
            "/api/v1/douyin/app/v3/fetch_video_comments",
            {"aweme_id": content_id, "cursor": comment_cursor, "count": 20},
        ),
    }
    try:
        return endpoints[stage]
    except KeyError as error:
        raise ProviderConfigurationError(f"未知抖音抓取阶段：{stage}") from error


def _douyin_call(
    stage: str,
    content_id: str,
    key: str,
    *,
    cursor: Optional[Mapping[str, Any]] = None,
) -> ProviderResult:
    endpoint, params = _douyin_request(stage, content_id, cursor)
    response = _request_json(
        _tikhub_url(endpoint),
        headers={"Authorization": f"Bearer {key}"},
        params=params,
        provider="TikHub",
    )
    status, payload = response
    try:
        result = _parse_douyin_stage_payload(stage, content_id, payload, status=status)
    except Exception as error:
        _raise_parse_failure_with_transport(
            error, response, payload=payload, status=status
        )
    return _with_transport(response, result)


def _douyin_web_detail_call(content_id: str, key: str) -> ProviderResult:
    """Fetch one Douyin detail through TikHub's Web adapter."""

    response = _request_json(
        _tikhub_url("/api/v1/douyin/web/fetch_one_video"),
        headers={"Authorization": f"Bearer {key}"},
        params={"aweme_id": content_id},
        provider="TikHub",
    )
    status, payload = response
    try:
        result = _parse_douyin_stage_payload(
            "detail", content_id, payload, status=status
        )
    except Exception as error:
        _raise_parse_failure_with_transport(
            error, response, payload=payload, status=status
        )
    return _with_transport(response, result)


def _parse_douyin_high_quality_video_payload(
    content_id: str, payload: Any, *, status: int
) -> ProviderResult:
    value = _tikhub_douyin_data(payload)
    if not isinstance(value, dict):
        raise CaptureError(
            "TikHub high-quality video response has no data object",
            retryable=True,
            error_code="invalid_response",
            http_status=status,
            billed=True,
            raw_response=payload,
        )
    video_id = str(value.get("video_id") or value.get("aweme_id") or "")
    raw_url = value.get("original_video_url")
    urls = (
        [raw_url]
        if isinstance(raw_url, str)
        else [item for item in raw_url if isinstance(item, str)]
        if isinstance(raw_url, list)
        else []
    )
    urls = list(dict.fromkeys(url for url in urls if url.startswith("https://")))
    if video_id != content_id or not urls:
        raise CaptureError(
            "TikHub high-quality video response did not match the requested work",
            retryable=True,
            error_code="invalid_response",
            http_status=status,
            billed=True,
            raw_response=payload,
        )
    return ProviderResult(
        {
            "title": "",
            "body": "",
            "published_at": None,
            "account_uid": "",
            "account_name": "",
            "content_type": "video",
            "media_urls": urls,
        },
        payload,
        status,
        True,
    )


def _douyin_high_quality_video_call(content_id: str, key: str) -> ProviderResult:
    """Fetch the original-quality Douyin video URL through TikHub App V3."""

    response = _request_json(
        _tikhub_url("/api/v1/douyin/app/v3/fetch_video_high_quality_play_url"),
        headers={"Authorization": f"Bearer {key}"},
        params={
            "aweme_id": content_id,
            "share_url": f"https://www.douyin.com/video/{content_id}",
            "region": "CN",
        },
        provider="TikHub",
    )
    status, payload = response
    try:
        result = _parse_douyin_high_quality_video_payload(
            content_id, payload, status=status
        )
    except Exception as error:
        _raise_parse_failure_with_transport(
            error, response, payload=payload, status=status
        )
    return _with_transport(response, result)


def _count_fields(
    value: Mapping[str, Any], aliases: Mapping[str, Sequence[str]], *, platform: str,
) -> Dict[str, Any]:
    """Keep absent, invalid and unrequested counters distinct; never round counts."""
    output: Dict[str, Any] = {}
    statuses: Dict[str, Any] = {}
    for field in ("view_count", "comment_count", "like_count", "share_count", "collect_count"):
        names = aliases.get(field, ())
        source_field = next((name for name in names if name in value), None)
        raw = value.get(source_field) if source_field is not None else None
        number = raw
        if isinstance(number, str) and number.strip().isdigit():
            number = int(number.strip())
        valid = isinstance(number, int) and not isinstance(number, bool) and number >= 0
        status = (
            "not_applicable" if platform == "xiaohongshu" and field == "view_count"
            else "not_requested" if not names
            else "missing" if raw is None
            else "provided" if valid else "invalid"
        )
        output[field] = number if status == "provided" else None
        statuses[field] = {"status": status, "reason": status, "source_field": source_field}
    output["_field_status"] = statuses
    return output


_DOUYIN_METRIC_ALIASES = {
    "view_count": ("play_count",), "comment_count": ("comment_count",),
    "like_count": ("digg_count",), "share_count": ("share_count",),
    "collect_count": ("collect_count", "favorite_count"),
}


def _parse_douyin_stage_payload(
    stage: str, content_id: str, payload: Any, *, status: int = 200
) -> ProviderResult:
    if (
        isinstance(payload, dict)
        and payload.get("stage") == stage
        and isinstance(payload.get("data"), dict)
    ):
        return ProviderResult(dict(payload["data"]), payload, status, True)
    data = _tikhub_douyin_data(payload)
    if stage == "metrics":
        items = data.get("statistics_list") if isinstance(data, dict) else None
        item = items[0] if isinstance(items, list) and items else None
        if not isinstance(item, dict) or str(item.get("aweme_id")) != content_id:
            raise CaptureError(
                "TikHub statistics did not contain requested content",
                retryable=True,
                error_code="invalid_response",
                http_status=status,
                billed=True,
                raw_response=payload,
            )
        normalized = _count_fields(item, {
            key: names for key, names in _DOUYIN_METRIC_ALIASES.items()
            if key in {"view_count", "like_count", "share_count"}
        }, platform="douyin")
        if normalized["view_count"] is None:
            raise CaptureError(
                "TikHub statistics omitted play_count for requested content",
                retryable=True,
                error_code="invalid_response",
                http_status=status,
                billed=True,
                raw_response=payload,
            )
        return ProviderResult(normalized, payload, status, True)
    if stage == "detail":
        item = _find_aweme(data, content_id)
        if item is None:
            raise CaptureError(
                "TikHub detail did not contain requested content",
                retryable=False,
                error_code="content_unavailable",
                http_status=status,
                billed=True,
                raw_response=payload,
            )
        author = _mapping(item.get("author"))
        published = _first_int(item, "create_time")
        content_type = "image" if item.get("images") else "video"
        normalized = {
            "title": str(item.get("desc") or ""),
            "body": str(item.get("desc") or ""),
            "published_at": datetime.fromtimestamp(published, tz=ZoneInfo("UTC"))
            .isoformat()
            .replace("+00:00", "Z")
            if published
            else None,
            "account_uid": str(author.get("uid") or ""),
            "account_name": str(author.get("nickname") or ""),
            "content_type": content_type,
            "media_urls": _douyin_media_urls(item, content_type),
            "metrics": _count_fields(_mapping(item.get("statistics")), _DOUYIN_METRIC_ALIASES, platform="douyin"),
        }
        return ProviderResult(normalized, payload, status, True)
    page = data if isinstance(data, dict) else {}
    hasher = CommentHasher()
    platform_hasher = PlatformUserHasher()
    sanitized: List[Dict[str, Any]] = []
    for item in page.get("comments") or []:
        if not isinstance(item, dict):
            continue
        if item.get("anonymous_user_key") and item.get("body"):
            replayed = dict(item)
            replayed["parent_comment_id"] = normalized_parent_comment_id(
                replayed.get("parent_comment_id")
            )
            sanitized.append(replayed)
            continue
        comment_user = _mapping(item.get("user"))
        raw_user = str(
            comment_user.get("sec_uid")
            or comment_user.get("uid")
            or comment_user.get("unique_id")
            or ""
        )
        text = " ".join(str(item.get("text") or "").split())[:2000]
        if not text:
            continue
        comment: Dict[str, Any] = {
            "platform_comment_id": str(item.get("cid") or ""),
            "anonymous_user_key": hasher.user_key("douyin", content_id, raw_user),
            "pseudonymous_user_key": platform_hasher.user_key("douyin", raw_user),
            "body": text,
            "published_at": datetime.fromtimestamp(
                int(item["create_time"]), tz=ZoneInfo("UTC")
            )
            .isoformat()
            .replace("+00:00", "Z")
            if item.get("create_time")
            else None,
            "like_count": _first_int(item, "digg_count"),
            # TikHub douyin marks first-level comments with reply_id "0";
            # only a real reply id may survive as parent_comment_id.
            "parent_comment_id": normalized_parent_comment_id(
                item.get("reply_id")
            ),
        }
        comment["comment_identity_key"] = comment_identity_key(
            platform_comment_id=comment["platform_comment_id"],
            pseudonymous_user_key=comment["pseudonymous_user_key"],
            body=comment["body"],
            published_at=comment["published_at"],
        )
        sanitized.append(comment)
    declared_total = _first_int(page, "total")
    has_more = bool(page.get("has_more"))
    raw_cursor = page.get("cursor")
    parsed_cursor = _first_int(page, "cursor")
    if has_more and parsed_cursor is None:
        raise CaptureError(
            "TikHub comments returned an invalid cursor",
            retryable=True,
            error_code="invalid_response",
            http_status=status,
            billed=True,
            raw_response=payload,
        )
    next_cursor_params = (
        {"cursor": parsed_cursor}
        if has_more and parsed_cursor is not None
        else None
    )
    safe_payload = {
        "code": 200,
        "data": {
            "total": declared_total,
            "has_more": has_more,
            "cursor": raw_cursor,
            "comments": sanitized,
            "privacy_note": (
                "业务评论投影已按内容级与平台级 HMAC-SHA256 双匿名化；"
                "完整供应商响应仅保存在受限原始证据层。"
            ),
        },
    }
    return ProviderResult(
        {
            "comment_count": declared_total,
            "comments": sanitized,
            "declared_total": declared_total,
            "has_more": has_more,
            "next_cursor": raw_cursor,
            "next_cursor_params": next_cursor_params,
        },
        safe_payload,
        status,
        True,
    )


def _xhs_metrics(note: Mapping[str, Any]) -> Dict[str, Any]:
    return _count_fields(note, {
        "comment_count": ("comments_count", "comment_count"),
        "like_count": ("liked_count", "like_count", "likes"),
        "share_count": ("shared_count", "share_count"),
        "collect_count": ("collected_count", "collect_count", "favorite_count"),
    }, platform="xiaohongshu")


def _derived_xhs_metrics_result(
    metrics: Mapping[str, Any],
    source_raw_response_id: Optional[int],
    source_captured_at: str,
) -> ProviderResult:
    normalized = {
        **dict(metrics),
        "_evidence_captured_at": source_captured_at,
    }
    return ProviderResult(
        normalized,
        {
            "derived_from_operation": "xiaohongshu_note_detail",
            "source_raw_response_id": source_raw_response_id,
            "source_captured_at": source_captured_at,
            "metrics": normalized,
        },
        200,
        False,
    )


def _raw_response_captured_at(raw_response_id: int, *, db_path: Path) -> str:
    with connect(db_path) as connection:
        row = connection.execute(
            "SELECT captured_at FROM provider_raw_responses WHERE id=?",
            (raw_response_id,),
        ).fetchone()
    if row is None:
        raise ProviderConfigurationError(
            f"原始供应商响应不存在：{raw_response_id}"
        )
    return str(row["captured_at"])


def _sanitize_xhs_comments(
    raw_comments: Any, *, content_id: str
) -> List[Dict[str, Any]]:
    hasher = CommentHasher()
    platform_hasher = PlatformUserHasher()
    comments: List[Dict[str, Any]] = []

    def visit(values: Any, parent_id: Optional[str] = None) -> None:
        if not isinstance(values, list):
            return
        for raw_item in values:
            item = _mapping(raw_item)
            if item.get("anonymous_user_key") and item.get("body"):
                replayed = dict(item)
                replayed["parent_comment_id"] = normalized_parent_comment_id(
                    replayed.get("parent_comment_id")
                )
                comments.append(replayed)
                continue
            comment_id = str(item.get("id") or item.get("comment_id") or "")
            user = _mapping(item.get("user"))
            raw_user = str(
                user.get("userid")
                or user.get("user_id")
                or user.get("id")
                or item.get("user_id")
                or ""
            )
            body = " ".join(str(item.get("content") or item.get("text") or "").split())[
                :2000
            ]
            if body:
                comment: Dict[str, Any] = {
                    "platform_comment_id": comment_id,
                    "anonymous_user_key": hasher.user_key(
                        "xiaohongshu", content_id, raw_user
                    ),
                    "pseudonymous_user_key": platform_hasher.user_key(
                        "xiaohongshu", raw_user
                    ),
                    "body": body,
                    "published_at": _timestamp_iso(
                        item.get("time") or item.get("create_time")
                    ),
                    "like_count": _first_int(item, "like_count", "liked_count"),
                    "parent_comment_id": parent_id,
                }
                comment["comment_identity_key"] = comment_identity_key(
                    platform_comment_id=comment["platform_comment_id"],
                    pseudonymous_user_key=comment["pseudonymous_user_key"],
                    body=comment["body"],
                    published_at=comment["published_at"],
                )
                comments.append(comment)
            visit(item.get("sub_comments"), comment_id or parent_id)

    visit(raw_comments)
    return comments


def _xhs_comment_cursor_params(
    page: Mapping[str, Any],
) -> Optional[Dict[str, Any]]:
    """Normalize opaque or JSON-encoded XHS pagination cursors."""

    explicit = page.get("next_cursor_params")
    raw_cursor = page.get("cursor")
    cursor_data: Mapping[str, Any] = {}
    if isinstance(explicit, Mapping):
        cursor_data = explicit
    elif isinstance(raw_cursor, Mapping):
        cursor_data = raw_cursor
    elif isinstance(raw_cursor, str) and raw_cursor.strip():
        try:
            decoded = json.loads(raw_cursor)
        except json.JSONDecodeError:
            decoded = None
        cursor_data = decoded if isinstance(decoded, Mapping) else {
            "cursor": raw_cursor
        }
    cursor_value = cursor_data.get("cursor")
    if cursor_value in (None, ""):
        return None
    if "index" not in cursor_data and "index" not in page:
        return None
    if "pageArea" not in cursor_data and "pageArea" not in page:
        return None
    raw_index = cursor_data.get("index", page.get("index", 0))
    try:
        index = int(raw_index or 0)
    except (TypeError, ValueError):
        return None
    return {
        "cursor": str(cursor_value),
        "index": index,
        "pageArea": str(
            cursor_data.get("pageArea")
            or page.get("pageArea")
            or "UNFOLDED"
        ),
    }


def _xhs_request(
    stage: str,
    content_id: str,
    content_type: str,
    *,
    cursor: Optional[Mapping[str, Any]] = None,
) -> tuple[str, Dict[str, Any]]:
    if stage not in {"detail", "metrics", "comments"}:
        raise ProviderConfigurationError(f"未知小红书抓取阶段：{stage}")
    if stage == "comments":
        page_cursor = cursor or {}
        endpoint = "/api/v1/xiaohongshu/app_v2/get_note_comments"
        params: Dict[str, Any] = {
            "note_id": content_id,
            "cursor": str(page_cursor.get("cursor") or ""),
            "index": int(page_cursor.get("index") or 0),
            "pageArea": str(page_cursor.get("pageArea") or "UNFOLDED"),
            "sort_strategy": "latest_v2",
        }
    else:
        endpoint = (
            "/api/v1/xiaohongshu/app_v2/get_video_note_detail"
            if content_type == "video"
            else "/api/v1/xiaohongshu/app_v2/get_image_note_detail"
        )
        params = {"note_id": content_id}
    return endpoint, params


def _xhs_call(
    stage: str,
    content_id: str,
    key: str,
    content_type: str,
    *,
    cursor: Optional[Mapping[str, Any]] = None,
) -> ProviderResult:
    endpoint, params = _xhs_request(stage, content_id, content_type, cursor=cursor)
    response = _request_json(
        _tikhub_url(endpoint),
        headers={"Authorization": f"Bearer {key}"},
        params=params,
        provider="TikHub",
    )
    status, payload = response
    try:
        result = _parse_xhs_stage_payload(
            stage, content_id, content_type, payload, status=status
        )
    except Exception as error:
        _raise_parse_failure_with_transport(
            error, response, payload=payload, status=status
        )
    return _with_transport(response, result)


def _parse_xhs_stage_payload(
    stage: str,
    content_id: str,
    content_type: str,
    payload: Any,
    *,
    status: int = 200,
) -> ProviderResult:
    if (
        isinstance(payload, dict)
        and payload.get("stage") == stage
        and isinstance(payload.get("data"), dict)
    ):
        return ProviderResult(dict(payload["data"]), payload, status, True)
    if (
        stage == "metrics"
        and isinstance(payload, dict)
        and isinstance(payload.get("metrics"), dict)
    ):
        return ProviderResult(dict(payload["metrics"]), payload, status, False)
    data = _tikhub_xhs_data(payload)
    if stage == "comments":
        page = data if isinstance(data, dict) else {}
        comments = _sanitize_xhs_comments(
            page.get("comments") or page.get("comment_list"), content_id=content_id
        )
        total = _first_int(page, "comment_count_l1", "comment_count", "total")
        has_more = _bool(page.get("has_more"))
        raw_cursor = page.get("cursor")
        next_cursor_params = _xhs_comment_cursor_params(page) if has_more else None
        if has_more and next_cursor_params is None:
            raise CaptureError(
                "TikHub Xiaohongshu comments omitted a valid next cursor",
                retryable=True,
                error_code="invalid_response",
                http_status=status,
                billed=True,
                raw_response=payload,
            )
        safe_payload = {
            "code": 200,
            "data": {
                "success": True,
                "code": 0,
                "data": {
                    "comment_count": total,
                    "comments": comments,
                    "cursor": raw_cursor,
                    "next_cursor_params": next_cursor_params,
                    "index": (
                        next_cursor_params.get("index")
                        if next_cursor_params is not None
                        else page.get("index")
                    ),
                    "pageArea": (
                        next_cursor_params.get("pageArea")
                        if next_cursor_params is not None
                        else page.get("pageArea")
                    ),
                    "has_more": has_more,
                    "privacy_note": (
                        "业务评论投影已按内容 HMAC-SHA256 匿名化；"
                        "完整供应商响应仅保存在受限原始证据层。"
                    ),
                },
            },
        }
        return ProviderResult(
            {
                "comment_count": total,
                "comments": comments,
                "declared_total": total,
                "next_cursor": raw_cursor,
                "next_cursor_params": next_cursor_params,
                "has_more": has_more,
            },
            safe_payload,
            status,
            True,
        )

    note = _find_note(data, content_id)
    if note is None:
        raise CaptureError(
            "TikHub Xiaohongshu detail did not contain requested note",
            retryable=False,
            error_code="content_unavailable",
            http_status=status,
            billed=True,
            raw_response=payload,
        )
    if (
        str(note.get("model_type") or "").strip().lower() == "error"
        and str(note.get("text") or "").strip().lower()
        == "note is not available"
    ):
        raise CaptureError(
            "TikHub Xiaohongshu note is not available",
            retryable=False,
            error_code="content_unavailable",
            http_status=status,
            billed=True,
            raw_response=payload,
        )
    metrics = _xhs_metrics(note)
    if stage == "metrics":
        return ProviderResult(metrics, payload, status, True)
    note_user = _mapping(note.get("user") or note.get("author"))
    if content_type not in {"image", "video"}:
        content_type = _xhs_known_type(note.get("type"))
    normalized: Dict[str, Any] = {
        "title": str(note.get("title") or ""),
        "body": str(
            note.get("desc") or note.get("description") or note.get("content") or ""
        ),
        "published_at": _timestamp_iso(
            note.get("time") or note.get("publish_time") or note.get("create_time")
        ),
        "account_uid": str(
            note_user.get("userid")
            or note_user.get("user_id")
            or note_user.get("id")
            or note_user.get("uid")
            or ""
        ),
        "account_name": str(note_user.get("nickname") or note_user.get("name") or ""),
        "content_type": content_type,
        "media_urls": _xhs_media_urls(note, content_type),
        # App V2 has no separate metrics route. Keeping these normalized values
        # lets the orchestrator open today's zero-cost metrics slot from this call.
        "metrics": metrics,
    }
    return ProviderResult(normalized, payload, status, True)


def _rnote_unwrap(payload: Any) -> Any:
    if not isinstance(payload, dict):
        raise CaptureError(
            "Rnote invalid response", retryable=True, error_code="invalid_response"
        )
    if payload.get("success") is False:
        raise CaptureError(
            f"Rnote semantic error: {payload.get('error') or payload.get('detail')}",
            retryable=True,
            error_code="semantic_error",
            http_status=200,
            billed=bool(payload.get("billed")),
            raw_response=payload,
        )
    value = payload.get("data")
    if isinstance(value, dict) and "success" in value:
        if value.get("success") is False:
            raise CaptureError(
                f"Rnote upstream error: {value.get('msg') or value.get('message')}",
                retryable=True,
                error_code="upstream_error",
                http_status=200,
                billed=bool(payload.get("billed")),
                raw_response=payload,
            )
        value = value.get("data")
    return value


def _find_note(value: Any, note_id: str) -> Optional[Dict[str, Any]]:
    if isinstance(value, dict):
        if (
            str(
                value.get("id") or value.get("note_id") or value.get("noteId") or ""
            ).lower()
            == note_id.lower()
        ):
            return value
        for child in value.values():
            found = _find_note(child, note_id)
            if found:
                return found
    elif isinstance(value, list):
        for child in value:
            found = _find_note(child, note_id)
            if found:
                return found
    return None


def _rnote_call(
    stage: str, content_id: str, key: str, content_type: str
) -> ProviderResult:
    raise ProviderConfigurationError(RNOTE_RETIRED_MESSAGE)

    # Historical parser/call shape remains below for audit compatibility. The
    # fail-closed guard ensures no Rnote request can be issued.
    endpoint = (
        "comments"
        if stage == "comments"
        else "video"
        if content_type == "video"
        else "image"
    )
    params: Dict[str, Any] = {"note_id": content_id}
    if stage == "comments":
        params.update({"sort_strategy": "latest_v2"})
    status, payload = _request_json(
        f"{RNOTE_BASE}/{endpoint}",
        headers={"X-API-Key": key},
        params=params,
        provider="Rnote",
    )
    payload_mapping = payload if isinstance(payload, dict) else {}
    data = _rnote_unwrap(payload)
    if stage == "comments":
        page = data if isinstance(data, dict) else {}
        raw_comments = page.get("comments") or page.get("comment_list") or []
        hasher = CommentHasher()
        comments: List[Dict[str, Any]] = []
        for item in raw_comments if isinstance(raw_comments, list) else []:
            if not isinstance(item, dict):
                continue
            comment_user = _mapping(item.get("user"))
            raw_user = str(
                comment_user.get("user_id")
                or comment_user.get("id")
                or item.get("user_id")
                or ""
            )
            text = " ".join(str(item.get("content") or item.get("text") or "").split())[
                :2000
            ]
            if text:
                comments.append(
                    {
                        "platform_comment_id": str(
                            item.get("id") or item.get("comment_id") or ""
                        ),
                        "anonymous_user_key": hasher.user_key(
                            "xiaohongshu", content_id, raw_user
                        ),
                        "body": text,
                        "published_at": None,
                        "like_count": _first_int(item, "like_count", "liked_count"),
                        "parent_comment_id": normalized_parent_comment_id(
                            item.get("parent_comment_id")
                        ),
                    }
                )
        total = _first_int(page, "total", "comment_count", "comments_count")
        safe = {
            "success": True,
            "data": {
                "total": total,
                "comments": comments,
                "privacy_note": "用户身份已匿名化",
            },
        }
        return ProviderResult(
            {"comment_count": total, "comments": comments},
            safe,
            status,
            bool(payload_mapping.get("billed", True)),
        )
    note = _find_note(data, content_id)
    if note is None:
        raise CaptureError(
            "Rnote detail did not contain requested note",
            retryable=False,
            error_code="content_unavailable",
            http_status=status,
            billed=bool(payload_mapping.get("billed", True)),
            raw_response=payload,
        )
    note_user = _mapping(note.get("user") or note.get("author"))
    if stage == "detail":
        timestamp = _first_int(note, "time", "publish_time", "create_time")
        normalized: Dict[str, Any] = {
            "title": str(note.get("title") or ""),
            "body": str(
                note.get("desc") or note.get("description") or note.get("content") or ""
            ),
            "published_at": datetime.fromtimestamp(timestamp, tz=ZoneInfo("UTC"))
            .isoformat()
            .replace("+00:00", "Z")
            if timestamp
            else None,
            "account_uid": str(
                note_user.get("user_id")
                or note_user.get("id")
                or note_user.get("uid")
                or ""
            ),
            "account_name": str(
                note_user.get("nickname") or note_user.get("name") or ""
            ),
            "content_type": content_type,
            "media_urls": _collect_media_urls(
                note, "video" if content_type == "video" else "image"
            ),
        }
    else:
        normalized = {
            "view_count": _first_int(note, "view_count", "read_count", "views"),
            "comment_count": _first_int(note, "comment_count", "comments_count"),
            "like_count": _first_int(note, "liked_count", "like_count", "likes"),
            "share_count": _first_int(note, "share_count", "shared_count"),
            "collect_count": _first_int(
                note, "collected_count", "collect_count", "favorite_count"
            ),
        }
    return ProviderResult(
        normalized, payload, status, bool(payload_mapping.get("billed", True))
    )


def _store_stage_result(
    content: Mapping[str, Any],
    stage: str,
    window_key: str,
    outcome,
    *,
    db_path: Path,
    applied_source: str = "live_applied",
    mark_raw_applied: bool = True,
    media_root: Optional[Path] = None,
    preserve_existing_content_fields: bool = False,
) -> None:
    data = outcome.data
    if stage == "detail" and data.get("content_type") not in {"video", "image"}:
        raise ProviderConfigurationError("详情未确认视频或图文类型，不能把 unknown 当主体图片")
    mutation_at = now_utc()
    detail_media_kind: Optional[str] = None
    with connect(db_path) as connection, transaction(connection):
        from .provider_budget import assert_paid_scope_owner
        assert_paid_scope_owner(connection)
        source_raw = connection.execute(
            """
            SELECT captured_at,local_path,sha256,provider,operation
            FROM provider_raw_responses WHERE id=?
            """,
            (outcome.raw_response_id,),
        ).fetchone()
        if source_raw is None:
            raise RuntimeError("stage raw response is missing")
        evidence_captured_at = str(
            data.get("_evidence_captured_at") or source_raw["captured_at"]
        )
        if stage == "detail":
            existing_content = connection.execute(
                "SELECT id FROM content_items WHERE id=?",
                (content["id"],),
            ).fetchone()
            if existing_content is None:
                raise RuntimeError("detail content does not exist")
            if preserve_existing_content_fields:
                connection.execute(
                    """
                    UPDATE content_items SET
                        title=COALESCE(NULLIF(title,''),NULLIF(?,''),title),
                        body=COALESCE(NULLIF(body,''),NULLIF(?,''),body),
                        published_at=COALESCE(published_at,?),
                        raw_account_uid=COALESCE(
                            NULLIF(raw_account_uid,''),NULLIF(?,''),raw_account_uid
                        ),
                        raw_account_name=COALESCE(
                            NULLIF(raw_account_name,''),NULLIF(?,''),raw_account_name
                        ),
                        content_type=CASE
                            WHEN NULLIF(content_type,'') IS NOT NULL
                                 AND content_type!='unknown' THEN content_type
                            ELSE COALESCE(NULLIF(?,''),content_type)
                        END,
                        updated_at=? WHERE id=?
                    """,
                    (
                        data.get("title"),
                        data.get("body"),
                        data.get("published_at"),
                        data.get("account_uid"),
                        data.get("account_name"),
                        data.get("content_type"),
                        mutation_at,
                        content["id"],
                    ),
                )
            else:
                connection.execute(
                    """
                    UPDATE content_items SET title=COALESCE(NULLIF(?,''),title),
                        body=COALESCE(NULLIF(?,''),body),
                        published_at=COALESCE(?,published_at),
                        raw_account_uid=COALESCE(NULLIF(?,''),raw_account_uid),
                        raw_account_name=COALESCE(NULLIF(?,''),raw_account_name),
                        content_type=COALESCE(NULLIF(?,''),content_type), updated_at=? WHERE id=?
                    """,
                    (
                        data.get("title"),
                        data.get("body"),
                        data.get("published_at"),
                        data.get("account_uid"),
                        data.get("account_name"),
                        data.get("content_type"),
                        mutation_at,
                        content["id"],
                    ),
                )
            reconcile_content_account_identity(connection, int(content["id"]))
            stored_detail = connection.execute(
                "SELECT content_type FROM content_items WHERE id=?", (content["id"],),
            ).fetchone()
            if stored_detail is None or stored_detail["content_type"] not in {"video", "image"}:
                raise ProviderConfigurationError("详情落库后仍未确认视频或图文类型")
            detail_media_kind = str(stored_detail["content_type"])
        if stage == "metrics" or (stage == "detail" and isinstance(data.get("metrics"), Mapping)):
            metrics = data if stage == "metrics" else dict(data["metrics"])
            metrics_status = (
                "available" if any(metrics.get(key) is not None for key in
                    ("view_count", "comment_count", "like_count", "share_count", "collect_count")) else "missing"
            )
            metrics_metadata = json.dumps(
                {
                    "exposure_observation": (
                        "observed"
                        if metrics.get("view_count") is not None
                        else "missing_from_statistics_response"
                    ),
                    "fields": metrics.get("_field_status", {}),
                    "operation": str(source_raw["operation"]),
                },
                ensure_ascii=False,
                sort_keys=True,
                separators=(",", ":"),
            )
            persist_metric_observation(
                connection,
                content_id=int(content["id"]),
                captured_at=evidence_captured_at,
                window_key=(window_key if stage == "metrics" else
                    datetime.fromisoformat(evidence_captured_at.replace("Z", "+00:00")).astimezone(SHANGHAI).date().isoformat()),
                view_count=metrics.get("view_count"),
                comment_count=metrics.get("comment_count"),
                like_count=metrics.get("like_count"),
                share_count=metrics.get("share_count"),
                collect_count=metrics.get("collect_count"),
                status=metrics_status,
                provider=("tikhub" if source_raw["provider"] == "TikHub" else
                          "douyin_openapi" if source_raw["provider"] == "DouyinOpenAPI" else str(source_raw["provider"])),
                platform=str(content["platform"]),
                raw_response_id=int(outcome.raw_response_id),
                metadata_json=metrics_metadata,
            )
        if stage == "comments":
            cursor = connection.execute(
                """
                INSERT INTO comment_evidence_versions(
                    content_id, captured_at, iso_week, source, local_path,
                    sha256, comment_count, status, created_at
                ) VALUES (?, ?, ?, ?, ?, ?, ?, 'available', ?)
                ON CONFLICT(content_id, iso_week, sha256) DO NOTHING
                """,
                (
                    content["id"],
                    evidence_captured_at,
                    window_key,
                    content["platform"],
                    source_raw["local_path"],
                    source_raw["sha256"],
                    data.get("comment_count"),
                    mutation_at,
                ),
            )
            evidence_id = cursor.lastrowid if cursor.rowcount > 0 else None
            if evidence_id is None:
                row = connection.execute(
                    "SELECT id FROM comment_evidence_versions WHERE content_id=? AND iso_week=? AND sha256=?",
                    (content["id"], window_key, source_raw["sha256"]),
                ).fetchone()
                if row is None:
                    raise RuntimeError("comment evidence was not materialized")
                evidence_id = row["id"]
            insert_comment_rows(
                connection,
                platform=str(content["platform"]),
                evidence_version_id=int(evidence_id),
                comments=data.get("comments") or [],
                captured_at=evidence_captured_at,
            )
    if stage == "detail":
        media_kwargs: Dict[str, Any] = {}
        if media_root is not None:
            media_kwargs["media_root"] = media_root
        store_media_source_manifest(
            int(content["id"]),
            media_kind=str(detail_media_kind),
            urls=[
                str(value)
                for value in data.get("media_urls", [])
                if isinstance(value, str)
            ],
            raw_response_id=int(outcome.raw_response_id),
            db_path=db_path,
            **media_kwargs,
        )
    if stage == "comments" and evidence_id is not None:
        upsert_comment_user_scores(
            int(content["id"]),
            int(evidence_id),
            legacy_user_score_rows(data.get("comments") or []),
            db_path=db_path,
        )
    if mark_raw_applied:
        _mark_raw_response_applied(
            int(outcome.raw_response_id),
            applied_source=applied_source,
            db_path=db_path,
        )


def _mark_raw_response_applied(
    raw_response_id: int,
    *,
    applied_source: str = "live_applied",
    db_path: Path,
) -> None:
    if applied_source not in {"live_applied", "derived_applied"}:
        raise ValueError(f"unsupported applied source: {applied_source}")
    with connect(db_path) as connection, transaction(connection):
        connection.execute(
            """
            UPDATE provider_raw_responses SET source=?
            WHERE id=? AND source='live'
            """,
            (applied_source, raw_response_id),
        )


def _slot_raw_is_applied(
    *, content_id: int, stage: str, window_key: str, db_path: Path
) -> bool:
    from .capture_singletons import attempt_slot_sql

    with connect(db_path) as connection:
        row = connection.execute(
            f"""
            SELECT pr.source
            FROM fetch_slots fs
            JOIN fetch_attempts fa ON {attempt_slot_sql(connection)}=fs.id
            JOIN provider_raw_responses pr ON pr.fetch_attempt_id=fa.id
            WHERE fs.content_id=? AND fs.stage=? AND fs.window_key=?
              AND fs.status='succeeded'
            ORDER BY fa.attempt_number DESC, pr.id DESC LIMIT 1
            """,
            (content_id, stage, window_key),
        ).fetchone()
    return row is not None and str(row["source"]) in {
        "live_applied",
        "derived_applied",
    }


def _derived_discovery_result(
    *,
    stage: str,
    data: Mapping[str, Any],
    discovery_operation: str,
    source_raw_response_id: int,
    source_sha256: str,
    source_captured_at: str,
) -> ProviderResult:
    normalized = {**dict(data), "_evidence_captured_at": source_captured_at}
    return ProviderResult(
        normalized,
        {
            "stage": stage,
            "data": normalized,
            "derived_from_operation": discovery_operation,
            "source_raw_response_id": source_raw_response_id,
            "source_sha256": source_sha256,
            "source_captured_at": source_captured_at,
        },
        200,
        False,
    )


def _materialize_discovery_stages(
    *,
    content_id: int,
    item: Mapping[str, Any],
    account_uid: str,
    metrics_window_key: str,
    discovery_operation: str,
    source_raw_response_id: int,
    provider: str,
    derived_adapter_version: str,
    derived_operations: Mapping[str, str],
    zero_view_is_authoritative: bool,
    db_path: Path,
    materialize_detail: bool = True,
    materialize_metrics: bool = True,
    derived_raw_root: Optional[Path] = None,
    media_root: Optional[Path] = None,
    preserve_existing_content_fields: bool = False,
) -> Dict[str, Any]:
    """Persist fields already present in a paid discovery response at zero extra cost."""

    with connect(db_path) as connection:
        row = connection.execute(
            "SELECT * FROM content_items WHERE id=?", (content_id,)
        ).fetchone()
        source_raw = connection.execute(
            "SELECT sha256,captured_at FROM provider_raw_responses WHERE id=?",
            (source_raw_response_id,),
        ).fetchone()
    if row is None:
        raise ProviderConfigurationError("作品列表落库后内容记录不存在")
    if source_raw is None:
        raise ProviderConfigurationError("作品列表原始响应不存在")
    content = dict(row)
    platform = str(content["platform"])
    media_urls = [
        str(value)
        for value in item.get("media_urls") or []
        if isinstance(value, str) and is_supported_media_url(value)
    ]
    metrics_value = item.get("metrics")
    metrics = dict(metrics_value) if isinstance(metrics_value, Mapping) else {}
    stage_values: List[tuple[str, str, Dict[str, Any]]] = []
    if media_urls and materialize_detail:
        stage_values.append(
            (
                "detail",
                "lifetime",
                {
                    "title": str(item.get("title") or ""),
                    "body": str(item.get("body") or ""),
                    "published_at": item.get("published_at"),
                    "account_uid": str(item.get("account_uid") or account_uid),
                    "account_name": str(item.get("account_name") or ""),
                    "content_type": str(item.get("content_type") or "unknown"),
                    "media_urls": media_urls,
                },
            )
        )
    # Some account-post discovery endpoints return a placeholder zero for
    # views, while Douyin OpenAPI returns an authoritative zero.  The caller
    # owns that provider-specific contract so a missing exposure value never
    # accidentally closes the daily statistics slot.
    if materialize_metrics:
        discovery_view_count = metrics.get("view_count")
        has_authoritative_discovery_exposure = (
            isinstance(discovery_view_count, (int, float))
            and not isinstance(discovery_view_count, bool)
            and (
                discovery_view_count > 0
                or (zero_view_is_authoritative and discovery_view_count == 0)
            )
        )
        if has_authoritative_discovery_exposure:
            stage_values.append(("metrics", metrics_window_key, metrics))
        elif any(value is not None for value in metrics.values()):
            metadata = json.dumps(
                {
                    "derived_from_operation": discovery_operation,
                    "exposure_observation": "missing_or_placeholder",
                    "reported_view_count": discovery_view_count,
                    "fields": {
                        **metrics.get("_field_status", {}),
                        "view_count": {
                            "status": "not_applicable" if platform == "xiaohongshu" else
                                      "invalid" if discovery_view_count == 0 else "missing",
                            "reason": "missing_or_placeholder",
                        },
                    },
                },
                ensure_ascii=False,
                sort_keys=True,
                separators=(",", ":"),
            )
            with connect(db_path) as connection, transaction(connection):
                persist_metric_observation(
                    connection,
                    content_id=content_id,
                    captured_at=str(source_raw["captured_at"]),
                    window_key=metrics_window_key,
                    view_count=None,
                    comment_count=metrics.get("comment_count"),
                    like_count=metrics.get("like_count"),
                    share_count=metrics.get("share_count"),
                    collect_count=metrics.get("collect_count"),
                    status="missing",
                    provider="tikhub" if provider == "TikHub" else provider,
                    platform=platform,
                    raw_response_id=source_raw_response_id,
                    metadata_json=metadata,
                    snapshot_mode="preserve_existing_exposure",
                )

    output: Dict[str, Any] = {
        "created": [],
        "replayed": [],
        "already_succeeded": [],
        "failed": [],
        "skipped": [],
    }
    expected = {stage for stage, _, _ in stage_values}
    output["skipped"] = [
        stage for stage in ("detail", "metrics") if stage not in expected
    ]
    for stage, window_key, data in stage_values:
        operation = str(derived_operations.get(stage) or "").strip()
        if not operation:
            raise ProviderConfigurationError(
                f"missing derived discovery operation for {stage}"
            )
        try:
            fetch_kwargs: Dict[str, Any] = {}
            if derived_raw_root is not None:
                fetch_kwargs["raw_root"] = derived_raw_root
            outcome = execute_derived_content_fetch(
                content_id=content_id,
                stage=stage,
                window_key=window_key,
                provider=provider,
                adapter_version=derived_adapter_version,
                operation=operation,
                result=_derived_discovery_result(
                    stage=stage,
                    data=data,
                    discovery_operation=discovery_operation,
                    source_raw_response_id=source_raw_response_id,
                    source_sha256=str(source_raw["sha256"]),
                    source_captured_at=str(source_raw["captured_at"]),
                ),
                source_raw_response_id=source_raw_response_id,
                db_path=db_path,
                allow_terminal_retry=True,
                **fetch_kwargs,
            )
            _store_stage_result(
                content,
                stage,
                window_key,
                outcome,
                db_path=db_path,
                applied_source="derived_applied",
                media_root=media_root,
                preserve_existing_content_fields=preserve_existing_content_fields,
            )
            output["created"].append(stage)
        except SlotUnavailable:
            with connect(db_path) as connection:
                existing_slot = connection.execute(
                    """
                    SELECT id,provider FROM fetch_slots
                    WHERE content_id=? AND stage=? AND window_key=?
                    """,
                    (content_id, stage, window_key),
                ).fetchone()
            storage_ready = _stage_storage_exists(
                content_id=content_id,
                platform=platform,
                stage=stage,
                window_key=window_key,
                db_path=db_path,
            )
            raw_applied = _slot_raw_is_applied(
                content_id=content_id,
                stage=stage,
                window_key=window_key,
                db_path=db_path,
            )
            legacy_slot = (
                existing_slot is not None
                and str(existing_slot["provider"]) == "legacy-cache"
            )
            if storage_ready and (raw_applied or legacy_slot):
                output["already_succeeded"].append(stage)
                continue
            if legacy_slot:
                synthetic = CaptureOutcome(
                    slot_id=int(existing_slot["id"]),
                    attempt_id=0,
                    raw_response_id=source_raw_response_id,
                    data=data,
                    billed=False,
                    amount=0.0,
                    currency="USD",
                )
                try:
                    _store_stage_result(
                        content,
                        stage,
                        window_key,
                        synthetic,
                        db_path=db_path,
                        mark_raw_applied=False,
                        media_root=media_root,
                        preserve_existing_content_fields=preserve_existing_content_fields,
                    )
                    output["replayed"].append(stage)
                except Exception as exc:
                    output["failed"].append(
                        {
                            "stage": stage,
                            "error_code": getattr(
                                exc, "error_code", type(exc).__name__
                            ),
                            "message": str(exc)[:500],
                        }
                    )
                continue
            try:
                replayed = _replay_content_stage(
                    content,
                    stage=stage,
                    window_key=window_key,
                    operation=operation,
                    db_path=db_path,
                )
                _store_stage_result(
                    content,
                    stage,
                    window_key,
                    replayed,
                    db_path=db_path,
                    applied_source="derived_applied",
                    media_root=media_root,
                    preserve_existing_content_fields=preserve_existing_content_fields,
                )
                output["replayed"].append(stage)
            except Exception as exc:
                output["failed"].append(
                    {
                        "stage": stage,
                        "error_code": getattr(exc, "error_code", type(exc).__name__),
                        "message": str(exc)[:500],
                    }
                )
        except Exception as exc:
            output["failed"].append(
                {
                    "stage": stage,
                    "error_code": getattr(exc, "error_code", type(exc).__name__),
                    "message": str(exc)[:500],
                }
            )
    return output


def materialize_account_discovery_page(
    *,
    account_id: int,
    platform: str,
    account_uid: str,
    page: Mapping[str, Any],
    source_raw_response_id: int,
    metrics_window_key: str,
    discovery_operation: str,
    provider: str,
    derived_adapter_version: str,
    derived_operations: Mapping[str, str],
    zero_view_is_authoritative: bool,
    db_path: Path = DEFAULT_DB,
    published_start: Optional[datetime] = None,
    published_end: Optional[datetime] = None,
    materialize_detail: bool = True,
    materialize_existing_stages: bool = True,
    new_content_source_group: Optional[Callable[[str], str]] = None,
    derived_raw_root: Optional[Path] = None,
    media_root: Optional[Path] = None,
    preserve_existing_content_fields: bool = False,
) -> Dict[str, Any]:
    """Apply one already-captured account discovery page to shared storage.

    The caller must first persist the complete provider page through
    ``execute_account_fetch`` and pass that raw-response id here.  This keeps
    identity, evidence, detail and metric writes on the same TikHub path.
    OpenAPI archives remain readable, but cannot re-enter the discovery chain.
    """

    if provider == "DouyinOpenAPI":
        raise ProviderConfigurationError("DouyinOpenAPI discovery is retired; raw evidence is audit-only")
    if platform not in {"douyin", "xiaohongshu"}:
        raise ProviderConfigurationError(f"unsupported platform: {platform}")
    if not provider.strip() or not derived_adapter_version.strip():
        raise ProviderConfigurationError(
            "provider and derived_adapter_version are required"
        )
    with connect(db_path) as connection:
        identity = connection.execute(
            """
            SELECT nickname FROM account_platform_identities
            WHERE account_id=? AND platform=? AND uid=?
            """,
            (account_id, platform, account_uid),
        ).fetchone()
        from .capture_singletons import attempt_slot_sql

        source_raw = connection.execute(
            f"""
            SELECT pr.id
            FROM provider_raw_responses pr
            JOIN fetch_attempts fa ON fa.id=pr.fetch_attempt_id
            JOIN fetch_slots fs ON fs.id={attempt_slot_sql(connection)}
            WHERE pr.id=? AND fs.account_id=? AND fs.stage='discovery'
              AND pr.provider=? AND pr.operation=?
            """,
            (
                source_raw_response_id,
                account_id,
                provider,
                discovery_operation,
            ),
        ).fetchone()
    if identity is None:
        raise ProviderConfigurationError("账号平台身份不存在或已变化")
    if source_raw is None:
        raise ProviderConfigurationError("作品列表原始响应与账号不匹配")

    inserted = 0
    updated = 0
    derived_created = 0
    derived_replayed = 0
    derived_already_succeeded = 0
    derived_skipped = 0
    derived_failures: List[Dict[str, Any]] = []
    content_changes: List[Dict[str, Any]] = []
    page_items = [
        dict(item) for item in page.get("items") or [] if isinstance(item, Mapping)
    ]
    persisted_items: List[Dict[str, Any]] = []
    missing_published_at_count = 0
    for item in page_items:
        published_iso = _timestamp_iso(item.get("published_at"))
        if published_start is not None or published_end is not None:
            if published_iso is None:
                missing_published_at_count += 1
                continue
            published = datetime.fromisoformat(published_iso.replace("Z", "+00:00"))
            if published_start is not None and published < published_start:
                continue
            if published_end is not None and published > published_end:
                continue
        if published_iso is not None:
            item["published_at"] = published_iso
        persisted_items.append(item)
        source_group_on_insert = (
            new_content_source_group(published_iso)
            if new_content_source_group is not None and published_iso is not None
            else ""
        )
        value = {
            **dict(item),
            "account_uid": account_uid,
            "account_name": item.get("account_name") or identity["nickname"],
            # Discovery text is often blank or truncated.  It can seed a new
            # content row but must not overwrite richer detail/manual text.
            "_preserve_existing_content_fields": True,
        }
        result = upsert_content(
            value,
            db_path=db_path,
            source_group_on_insert=source_group_on_insert,
        )
        content_id = int(result["id"])
        content_changes.append(
            {"content_id": content_id, "action": str(result["action"])}
        )
        inserted += int(result["action"] == "inserted")
        updated += int(result["action"] == "updated")
        if result["action"] == "inserted" or materialize_existing_stages:
            derived = _materialize_discovery_stages(
                content_id=content_id,
                item=item,
                account_uid=account_uid,
                metrics_window_key=metrics_window_key,
                discovery_operation=discovery_operation,
                source_raw_response_id=source_raw_response_id,
                provider=provider,
                derived_adapter_version=derived_adapter_version,
                derived_operations=derived_operations,
                zero_view_is_authoritative=zero_view_is_authoritative,
                db_path=db_path,
                materialize_detail=materialize_detail,
                derived_raw_root=derived_raw_root,
                media_root=media_root,
                preserve_existing_content_fields=preserve_existing_content_fields,
            )
        else:
            derived = {
                "created": [],
                "replayed": [],
                "already_succeeded": [],
                "skipped": ["detail", "metrics"],
                "failed": [],
            }
        derived_created += len(derived["created"])
        derived_replayed += len(derived["replayed"])
        derived_already_succeeded += len(derived["already_succeeded"])
        derived_skipped += len(derived["skipped"])
        derived_failures.extend(derived["failed"])

    if missing_published_at_count:
        derived_failures.append(
            {
                "stage": "discovery",
                "error_code": "missing_published_at",
                "message": (
                    f"{missing_published_at_count} items have no usable published_at; "
                    "kept in raw response for quarantine/manual resolution"
                ),
            }
        )
    if not derived_failures:
        _mark_raw_response_applied(source_raw_response_id, db_path=db_path)
    return {
        "inserted": inserted,
        "updated": updated,
        "page_item_count": len(page_items),
        "persisted_item_count": len(persisted_items),
        "missing_published_at_count": missing_published_at_count,
        "content_changes": content_changes,
        "derived_stages": {
            "created": derived_created,
            "replayed": derived_replayed,
            "already_succeeded": derived_already_succeeded,
            "skipped": derived_skipped,
            "failures": derived_failures,
        },
        "page_published_at": [
            value
            for value in (
                _timestamp_iso(item.get("published_at")) for item in page_items
            )
            if value is not None
        ],
    }


def discover_account_content(
    account_id: int,
    platform: str,
    uid: str,
    *,
    as_of: Optional[date] = None,
    cursor: Any = None,
    window_key: Optional[str] = None,
    published_start: Optional[datetime] = None,
    published_end: Optional[datetime] = None,
    task_id: Optional[str] = None,
    task_max_amount: Optional[float] = None,
    db_path: Path = DEFAULT_DB,
    call_override: Optional[Callable[[str, Mapping[str, Any]], ProviderResult]] = None,
    materialize_discovery_detail: bool = True,
    materialize_existing_discovery_stages: bool = True,
    new_content_source_group: Optional[Callable[[str], str]] = None,
) -> Dict[str, Any]:
    if platform not in {"douyin", "xiaohongshu"}:
        return {
            "account_id": account_id,
            "platform": platform,
            "status": "unsupported",
            "inserted": 0,
            "updated": 0,
            "provider_cost": 0.0,
        }
    target_day = as_of or datetime.now(SHANGHAI).date()
    with connect(db_path) as connection:
        identity = connection.execute(
            """
            SELECT * FROM account_platform_identities
            WHERE account_id=? AND platform=? AND uid=?
            """,
            (account_id, platform, uid),
        ).fetchone()
        if identity is None:
            raise ProviderConfigurationError("账号平台身份不存在或已变化")

    costs = 0.0
    reference = uid
    reference_status = "not_required"
    if platform == "douyin":
        with connect(db_path) as connection:
            stored = connection.execute(
                """
                SELECT reference_value FROM account_provider_references
                WHERE account_identity_id=? AND provider='TikHub' COLLATE NOCASE
                  AND reference_kind='sec_user_id'
                """,
                (identity["id"],),
            ).fetchone()
        if stored is not None and _valid_douyin_sec_user_id(
            str(stored["reference_value"])
        ):
            reference = str(stored["reference_value"])
            reference_status = "cached"
        else:
            operation = "douyin_uid_profile"
            budget_id = _budget_for_call(
                provider="TikHub",
                operation=operation,
                price=TIKHUB_PRICE,
                task_id=task_id,
                task_max_amount=task_max_amount,
                db_path=db_path,
            )
            call = (
                partial(call_override, "resolve_account", dict(identity))
                if call_override is not None
                else partial(
                    _douyin_reference_call,
                    uid,
                    _load_key(TIKHUB_KEY_FILE, "TIKHUB_API_KEY"),
                )
            )
            try:
                outcome = execute_account_fetch(
                    request_transport=_freeze_tikhub_transport(call_override),
                    account_id=account_id,
                    stage="discovery",
                    window_key="reference-profile-lifetime",
                    provider="TikHub",
                    adapter_version="tikhub-uid-profile-v8.1",
                    operation=operation,
                    call=call,
                    db_path=db_path,
                    budget_id=budget_id,
                    task_id=task_id,
                    task_max_amount=task_max_amount,
                    paid_request_identity=_paid_request_identity(
                        operation=operation,
                        platform="douyin",
                        subject=uid,
                        params={"uid": uid},
                        cursor=None,
                        due_bucket="reference-profile-lifetime",
                    ),
                )
                reference_data = outcome.data
                reference_raw_response_id = outcome.raw_response_id
                costs += outcome.amount
                reference_status = "resolved"
            except SlotUnavailable:
                stored_raw = load_succeeded_raw_response(
                    account_id=account_id,
                    stage="discovery",
                    window_key="reference-profile-lifetime",
                    operation=operation,
                    db_path=db_path,
                )
                parsed = _parse_douyin_reference_payload(
                    stored_raw.value, status=stored_raw.http_status or 200
                )
                reference_data = parsed.data
                reference_raw_response_id = stored_raw.raw_response_id
                reference_status = "replayed"
            reference = str(reference_data.get("reference") or "")
            if not _valid_douyin_sec_user_id(reference):
                raise ProviderConfigurationError(
                    "账号 UID 未解析出有效 App V3 sec_user_id"
                )
            with connect(db_path) as connection, transaction(connection):
                connection.execute(
                    """
                    INSERT INTO account_provider_references(
                        account_identity_id, provider, reference_kind, reference_value,
                        source_raw_response_id, created_at, updated_at
                    ) VALUES (?, 'TikHub', 'sec_user_id', ?, ?, ?, ?)
                    ON CONFLICT(account_identity_id,provider,reference_kind) DO UPDATE SET
                        reference_value=excluded.reference_value,
                        source_raw_response_id=excluded.source_raw_response_id,
                        updated_at=excluded.updated_at
                    """,
                    (
                        identity["id"],
                        reference,
                        reference_raw_response_id,
                        now_utc(),
                        now_utc(),
                    ),
                )
            _mark_raw_response_applied(int(reference_raw_response_id), db_path=db_path)

    provider = "TikHub"
    price = TIKHUB_PRICE if platform == "douyin" else TIKHUB_XHS_PRICE
    operation = (
        "douyin_user_posts" if platform == "douyin" else "xiaohongshu_user_posts"
    )
    adapter = (
        "tikhub-user-posts-v8.1"
        if platform == "douyin"
        else "tikhub-xhs-app-v2-user-posts-v8.1"
    )
    budget_id = _budget_for_call(
        provider=provider,
        operation=operation,
        price=price,
        task_id=task_id,
        task_max_amount=task_max_amount,
        db_path=db_path,
    )
    discovery_identity = {**dict(identity), "cursor": cursor}
    call = (
        partial(call_override, "discover_content", discovery_identity)
        if call_override is not None
        else partial(
            _douyin_discovery_call,
            reference,
            _load_key(TIKHUB_KEY_FILE, "TIKHUB_API_KEY"),
            cursor or 0,
        )
        if platform == "douyin"
        else partial(
            _xhs_discovery_call,
            uid,
            _load_key(TIKHUB_KEY_FILE, "TIKHUB_API_KEY"),
            cursor or "",
        )
    )
    effective_window_key = window_key or target_day.isoformat()
    discovery_params: dict[str, Any] = (
        {
            "sec_user_id": reference,
            "max_cursor": cursor or 0,
            "count": 20,
            "sort_type": 0,
        }
        if platform == "douyin"
        else {"user_id": uid, "cursor": cursor or ""}
    )
    try:
        outcome = execute_account_fetch(
            request_transport=_freeze_tikhub_transport(call_override),
            account_id=account_id,
            stage="discovery",
            window_key=effective_window_key,
            provider=provider,
            adapter_version=adapter,
            operation=operation,
            call=call,
            db_path=db_path,
            budget_id=budget_id,
            task_id=task_id,
            task_max_amount=task_max_amount,
            paid_request_identity=_paid_request_identity(
                operation=operation,
                platform=platform,
                subject=reference if platform == "douyin" else uid,
                params=discovery_params,
                cursor=cursor or (0 if platform == "douyin" else ""),
                due_bucket=effective_window_key,
            ),
        )
        page_data = outcome.data
        page_amount = outcome.amount
        page_raw_response_id = outcome.raw_response_id
        replayed = False
    except SlotUnavailable as unavailable:
        if unavailable.error_code == IdentityConflictError.error_code:
            raise IdentityConflictError(
                "identity_conflict: 内容身份冲突，已记录重复内容关系待处理",
                provider_cost=round(costs, 6),
            ) from unavailable
        stored_raw = load_succeeded_raw_response(
            account_id=account_id,
            stage="discovery",
            window_key=effective_window_key,
            operation=operation,
            db_path=db_path,
        )
        if platform == "douyin":
            parsed_page = _parse_douyin_discovery_payload(
                stored_raw.value, status=stored_raw.http_status or 200
            )
            page_data = parsed_page.data
        else:
            page_data = _parse_xhs_discovery_payload(stored_raw.value)
        page_amount = 0.0
        page_raw_response_id = stored_raw.raw_response_id
        page_slot_id = stored_raw.slot_id
        replayed = True
    else:
        page_slot_id = outcome.slot_id
    costs += page_amount
    try:
        for lock_attempt in range(3):
            try:
                materialized = materialize_account_discovery_page(
                    account_id=account_id,
                    platform=platform,
                    account_uid=uid,
                    page=page_data,
                    source_raw_response_id=int(page_raw_response_id),
                    metrics_window_key=target_day.isoformat(),
                    discovery_operation=operation,
                    provider=provider,
                    derived_adapter_version="tikhub-discovery-derived-v8.1",
                    derived_operations={
                        "detail": STAGE_CONFIG[(platform, "detail")][2],
                        "metrics": STAGE_CONFIG[(platform, "metrics")][2],
                    },
                    zero_view_is_authoritative=False,
                    db_path=db_path,
                    published_start=published_start,
                    published_end=published_end,
                    materialize_detail=materialize_discovery_detail,
                    materialize_existing_stages=materialize_existing_discovery_stages,
                    new_content_source_group=new_content_source_group,
                )
                break
            except sqlite3.OperationalError as error:
                if "database is locked" not in str(error).lower() or lock_attempt == 2:
                    raise
                # The paid page raw is already committed. Retry only its local,
                # idempotent materialization; never issue another provider call.
                time.sleep(0.05 * (2**lock_attempt))
    except IdentityConflictError as error:
        mark_fetch_slot_terminal_failure(
            db_path=db_path,
            slot_id=page_slot_id,
            error_code=IdentityConflictError.error_code,
            error_message=str(error),
        )
        error.provider_cost = round(costs, 6)
        raise
    derived = materialized["derived_stages"]
    derived_failures = list(derived["failures"])
    return {
        "account_id": account_id,
        "platform": platform,
        "status": (
            "partial"
            if derived_failures
            else "already_succeeded"
            if replayed
            else "succeeded"
        ),
        "replayed": replayed,
        "reference_status": reference_status,
        "inserted": materialized["inserted"],
        "updated": materialized["updated"],
        "provider_cost": round(costs, 6),
        "next_cursor": page_data.get("next_cursor"),
        "has_more": _bool(page_data.get("has_more")),
        "page_item_count": materialized["page_item_count"],
        "persisted_item_count": materialized["persisted_item_count"],
        "missing_published_at_count": materialized[
            "missing_published_at_count"
        ],
        "content_changes": materialized["content_changes"],
        "derived_stages": derived,
        "page_published_at": materialized["page_published_at"],
    }


def _stage_storage_exists(
    *, content_id: int, platform: str, stage: str, window_key: str, db_path: Path
) -> bool:
    with connect(db_path) as connection:
        if stage == "detail":
            row = connection.execute(
                """
                SELECT 1 FROM evidence_artifacts
                WHERE content_id=? AND artifact_type='media_source'
                  AND status='available' LIMIT 1
                """,
                (content_id,),
            ).fetchone()
        elif stage == "metrics":
            row = connection.execute(
                """
                SELECT 1 FROM content_metric_snapshots
                WHERE content_id=? AND window_key=? AND source=? LIMIT 1
                """,
                (content_id, window_key, platform),
            ).fetchone()
        else:
            row = connection.execute(
                """
                SELECT 1 FROM comment_evidence_versions
                WHERE content_id=? AND iso_week=? LIMIT 1
                """,
                (content_id, window_key),
            ).fetchone()
    return row is not None


def _replay_content_stage(
    content: Mapping[str, Any],
    *,
    stage: str,
    window_key: str,
    operation: str,
    db_path: Path,
) -> CaptureOutcome:
    stored = load_succeeded_raw_response(
        content_id=int(content["id"]),
        stage=stage,
        window_key=window_key,
        operation=operation,
        db_path=db_path,
    )
    platform = str(content["platform"])
    if platform == "douyin":
        parsed = _parse_douyin_stage_payload(
            stage,
            str(content["platform_content_id"]),
            stored.value,
            status=stored.http_status or 200,
        )
    else:
        parsed = _parse_xhs_stage_payload(
            stage,
            str(content["platform_content_id"]),
            str(content["content_type"]),
            stored.value,
            status=stored.http_status or 200,
        )
    replayed_data = dict(parsed.data)
    stored_payload = _mapping(stored.value)
    stored_data = _mapping(stored_payload.get("data"))
    stored_metrics = _mapping(stored_payload.get("metrics"))
    source_captured_at = next(
        (
            str(value)
            for value in (
                replayed_data.get("_evidence_captured_at"),
                stored_data.get("_evidence_captured_at"),
                stored_metrics.get("_evidence_captured_at"),
                stored_payload.get("source_captured_at"),
                stored.captured_at,
            )
            if value not in (None, "")
        ),
        stored.captured_at,
    )
    replayed_data["_evidence_captured_at"] = source_captured_at
    return CaptureOutcome(
        slot_id=stored.slot_id,
        attempt_id=0,
        raw_response_id=stored.raw_response_id,
        data=replayed_data,
        billed=False,
        amount=0.0,
        currency="USD",
    )


def materialize_zero_comment_evidence(
    content_id: int,
    *,
    as_of: date,
    db_path: Path = DEFAULT_DB,
) -> Dict[str, Any]:
    """Materialize a confirmed zero through the canonical paged run."""

    with connect(db_path) as connection:
        content_exists = connection.execute(
            "SELECT 1 FROM content_items WHERE id=?", (content_id,)
        ).fetchone()
    if content_exists is None:
        raise ProviderConfigurationError("内容不存在")
    if _zero_comment_metric_result(
        content_id,
        metric_window_key=as_of.isoformat(),
        db_path=db_path,
    ) is None:
        return {
            "content_id": content_id,
            "status": "not_applicable",
            "reason": "当日指标没有确认评论数为 0",
        }
    return capture_content_comments_live(content_id, as_of=as_of, db_path=db_path)


def _comment_page_call(
    content: Mapping[str, Any],
    cursor: Optional[Mapping[str, Any]],
    *,
    call_override: Optional[Callable[[str, Mapping[str, Any]], ProviderResult]],
) -> ProviderResult:
    platform = str(content["platform"])
    content_key = str(content["platform_content_id"])
    if call_override is not None:
        override_content = dict(content)
        override_content["_comment_cursor"] = cursor
        return call_override("comments", override_content)
    key = _load_key(TIKHUB_KEY_FILE, "TIKHUB_API_KEY")
    if platform == "douyin":
        return _douyin_call("comments", content_key, key, cursor=cursor)
    return _xhs_call(
        "comments", content_key, key, str(content["content_type"]), cursor=cursor
    )


def _stored_comment_page(
    content: Mapping[str, Any],
    *,
    window_key: str,
    operation: str,
    db_path: Path,
) -> PageFetch:
    stored = load_succeeded_raw_response(
        stage="comments",
        window_key=window_key,
        content_id=int(content["id"]),
        operation=operation,
        db_path=db_path,
    )
    if str(content["platform"]) == "douyin":
        result = _parse_douyin_stage_payload(
            "comments",
            str(content["platform_content_id"]),
            stored.value,
            status=stored.http_status or 200,
        )
    else:
        result = _parse_xhs_stage_payload(
            "comments",
            str(content["platform_content_id"]),
            str(content["content_type"]),
            stored.value,
            status=stored.http_status or 200,
        )
    return PageFetch(
        raw_response_id=int(stored.raw_response_id),
        fetch_slot_id=int(stored.slot_id),
        result=result,
        already_stored=True,
    )


def _zero_comment_metric_result(
    content_id: int,
    *,
    metric_window_key: str,
    db_path: Path,
) -> Optional[ProviderResult]:
    with connect(db_path) as connection:
        # Route-cycle and Matrix-date observations share the same field
        # selector; an exact legacy daily slot would hide fresh valid zeros.
        selected = select_content_metrics(connection, [content_id]).get(content_id, {})
        field = selected.get("fields", {}).get("comment_count", {})
        if (field.get("value") != 0 or field.get("status") != "provided"
                or field.get("freshness") != "fresh" or not field.get("raw_response_id")
                or field.get("effective_provider") not in {"newrank_matrix", "tikhub"}):
            return None
        raw = connection.execute("SELECT sha256 FROM provider_raw_responses WHERE id=?",
                                 (field["raw_response_id"],)).fetchone()
        if raw is None:
            return None
    return _derived_discovery_result(
        stage="comments",
        data={
            "comment_count": 0,
            "comments": [],
            "declared_total": 0,
            "has_more": False,
            "next_cursor": None,
            "next_cursor_params": None,
        },
        discovery_operation=f"zero_comments_from_metric_observation:{field['observation_id']}",
        source_raw_response_id=int(field["raw_response_id"]),
        source_sha256=str(raw["sha256"]),
        source_captured_at=str(field["captured_at"]),
    )


def _live_comment_page_fetcher(
    content: Mapping[str, Any],
    *,
    base_window_key: str,
    metric_window_key: str,
    provider: str,
    adapter_version: str,
    operation: str,
    price: float,
    db_path: Path,
    task_id: Optional[str],
    task_max_amount: Optional[float],
    call_override: Optional[Callable[[str, Mapping[str, Any]], ProviderResult]],
    cache_only: bool,
) -> Callable[[int, Optional[Mapping[str, Any]]], PageFetch]:
    content_id = int(content["id"])
    budget_id: Optional[str] = None

    def fetch(page_number: int, cursor: Optional[Mapping[str, Any]]) -> PageFetch:
        nonlocal budget_id
        window = page_window_key(base_window_key, cursor)
        try:
            return _stored_comment_page(
                content,
                window_key=window,
                operation=operation,
                db_path=db_path,
            )
        except SlotUnavailable:
            pass
        if page_number == 1 and cursor is None:
            try:
                return _stored_comment_page(
                    content,
                    window_key=base_window_key,
                    operation=operation,
                    db_path=db_path,
                )
            except SlotUnavailable:
                pass
            derived_zero = _zero_comment_metric_result(
                content_id,
                metric_window_key=metric_window_key,
                db_path=db_path,
            )
            if derived_zero is not None:
                try:
                    outcome = execute_derived_content_fetch(
                        content_id=content_id,
                        stage="comments",
                        window_key=window,
                        provider=provider,
                        adapter_version=adapter_version,
                        operation=operation,
                        result=derived_zero,
                        source_raw_response_id=int(derived_zero.raw_response["source_raw_response_id"]),
                        db_path=db_path,
                    )
                    _mark_raw_response_applied(
                        int(outcome.raw_response_id),
                        applied_source="derived_applied",
                        db_path=db_path,
                    )
                    return PageFetch(
                        raw_response_id=int(outcome.raw_response_id),
                        fetch_slot_id=int(outcome.slot_id),
                        result=outcome,
                    )
                except SlotUnavailable:
                    return _stored_comment_page(
                        content,
                        window_key=window,
                        operation=operation,
                        db_path=db_path,
                    )
        if cache_only:
            raise PageFetchDeferred("cache_only_page_unavailable")
        if budget_id is None:
            budget_id = _budget_for_call(
                provider=provider,
                operation=operation,
                price=price,
                task_id=task_id,
                task_max_amount=task_max_amount,
                db_path=db_path,
            )
        try:
            platform = str(content["platform"])
            platform_content_id = str(content["platform_content_id"])
            if platform == "douyin":
                _, request_params = _douyin_request(
                    "comments", platform_content_id, cursor
                )
            else:
                _, request_params = _xhs_request(
                    "comments",
                    platform_content_id,
                    str(content["content_type"]),
                    cursor=cursor,
                )
            outcome = execute_content_fetch(
                request_transport=_freeze_tikhub_transport(call_override),
                content_id=content_id,
                stage="comments",
                window_key=window,
                provider=provider,
                adapter_version=adapter_version,
                operation=operation,
                call=partial(
                    _comment_page_call,
                    content,
                    cursor,
                    call_override=call_override,
                ),
                db_path=db_path,
                budget_id=budget_id,
                task_id=task_id if budget_id is not None else None,
                task_max_amount=(task_max_amount if budget_id is not None else None),
                paid_request_identity=_paid_request_identity(
                    operation=operation,
                    platform=platform,
                    subject=platform_content_id,
                    params=request_params,
                    cursor=cursor,
                    due_bucket=window,
                ),
            )
            return PageFetch(
                raw_response_id=int(outcome.raw_response_id),
                fetch_slot_id=int(outcome.slot_id),
                result=outcome,
            )
        except SlotUnavailable:
            return _stored_comment_page(
                content,
                window_key=window,
                operation=operation,
                db_path=db_path,
            )

    return fetch


def capture_content_comments_live(
    content_id: int,
    *,
    as_of: Optional[date] = None,
    db_path: Path = DEFAULT_DB,
    call_override: Optional[Callable[[str, Mapping[str, Any]], ProviderResult]] = None,
    comment_cap: int = COMMENT_CAP,
    coverage_target: float = COVERAGE_TARGET,
    task_id: Optional[str] = None,
    task_max_amount: Optional[float] = None,
    cache_only: bool = False,
) -> Dict[str, Any]:
    """Run a cursor-paged comment capture for one content (paged-comments-v2)."""

    with connect(db_path) as connection:
        row = connection.execute(
            "SELECT * FROM content_items WHERE id=?", (content_id,)
        ).fetchone()
    if row is None:
        raise ProviderConfigurationError("内容不存在")
    content = dict(row)
    platform = str(content["platform"])
    if platform not in {"douyin", "xiaohongshu"}:
        raise ProviderConfigurationError(
            "视频号和快手首版只支持人工导入，未配置自动数据源"
        )
    local_date = as_of or datetime.now(SHANGHAI).date()
    iso = local_date.isocalendar()
    window_key = f"{iso.year}-W{iso.week:02d}"
    provider, adapter_version, operation, price = STAGE_CONFIG[(platform, "comments")]
    adapter_version = f"{adapter_version}+paged-comments-v2"
    fetcher = _live_comment_page_fetcher(
        content,
        base_window_key=window_key,
        metric_window_key=local_date.isoformat(),
        provider=provider,
        adapter_version=adapter_version,
        operation=operation,
        price=price,
        db_path=db_path,
        task_id=task_id,
        task_max_amount=task_max_amount,
        call_override=call_override,
        cache_only=cache_only,
    )
    return capture_content_comments(
        content,
        window_key=window_key,
        page_fetcher=fetcher,
        provider=provider,
        adapter_version=adapter_version,
        db_path=db_path,
        comment_cap=comment_cap,
        coverage_target=coverage_target,
    )


def _xhs_type_probe(
    content: Mapping[str, Any],
    *,
    db_path: Path,
    call_override: Optional[Callable[[str, Mapping[str, Any]], ProviderResult]],
    task_id: Optional[str],
    task_max_amount: Optional[float],
) -> CaptureOutcome:
    """The priced image endpoint has its own lifetime type-probe slot."""
    content_id = int(content["id"])
    provider, _, operation, price = STAGE_CONFIG[("xiaohongshu", "detail")]
    with connect(db_path) as connection:
        slot = connection.execute(
            "SELECT * FROM fetch_slots WHERE content_id=? AND stage='detail' AND window_key=?",
            (content_id, XHS_TYPE_PROBE_WINDOW),
        ).fetchone()
        identity = connection.execute(
            "SELECT uid FROM account_platform_identities WHERE account_id=? AND platform='xiaohongshu'",
            (content.get("account_id"),),
        ).fetchone()
    expected_uid = str(content.get("raw_account_uid") or (identity["uid"] if identity else "") or "")

    def validate(result: ProviderResult) -> ProviderResult:
        data = _mapping(result.data)
        if data.get("content_type") not in {"image", "video"}:
            raise CaptureError(
                "Xiaohongshu image endpoint did not establish the real content type",
                retryable=False, error_code="content_type_unresolved",
                http_status=result.http_status, billed=result.billed,
                raw_response=result.raw_response,
                entity_bytes=result.entity_bytes,
                transport_receipt=result.transport_receipt,
            )
        actual_uid = str(data.get("account_uid") or "")
        if not expected_uid or actual_uid != expected_uid:
            raise CaptureError(
                "Xiaohongshu type probe author does not match the managed identity",
                retryable=False, error_code="identity_conflict",
                http_status=result.http_status, billed=result.billed,
                raw_response=result.raw_response,
                entity_bytes=result.entity_bytes,
                transport_receipt=result.transport_receipt,
            )
        return result

    if slot is not None and slot["status"] == "succeeded":
        stored = load_succeeded_raw_response(
            content_id=content_id, stage="detail", window_key=XHS_TYPE_PROBE_WINDOW,
            operation=operation, db_path=db_path,
        )
        parsed = _parse_xhs_stage_payload(
            "detail", str(content["platform_content_id"]), "unknown",
            stored.value, status=stored.http_status or 200,
        )
        parsed = validate(ProviderResult(parsed.data, parsed.raw_response, parsed.http_status, False))
        return CaptureOutcome(
            slot_id=stored.slot_id, attempt_id=0, raw_response_id=stored.raw_response_id,
            data={**dict(parsed.data), "_evidence_captured_at": stored.captured_at},
            billed=False, amount=0.0, currency="USD",
        )
    if slot is not None and slot["status"] == "terminal_failed":
        raise CaptureError(
            str(slot["last_error_message"] or "Xiaohongshu type probe is terminal"),
            retryable=False,
            error_code=str(slot["last_error_code"] or "content_type_unresolved"),
            billed=False,
        )

    def call() -> ProviderResult:
        if call_override is not None:
            # Preserve the historical override stage name; its scope is explicit.
            result = call_override("detail", {
                **dict(content), "content_type": "unknown",
                "_detail_window_key": XHS_TYPE_PROBE_WINDOW,
                "_xhs_type_probe": True,
                "_xhs_endpoint": "get_image_note_detail",
            })
        else:
            result = _xhs_call(
                "detail", str(content["platform_content_id"]),
                _load_key(TIKHUB_KEY_FILE, "TIKHUB_API_KEY"), "unknown",
            )
        return validate(result)

    budget_id = _budget_for_call(
        provider=provider, operation=operation, price=price,
        task_id=task_id, task_max_amount=task_max_amount, db_path=db_path,
    )
    # The same priced endpoint operation has a distinct raw directory. Identical
    # probe/video bodies must never collide with the later lifetime raw.
    from .capture import RAW_ROOT
    return execute_content_fetch(
        request_transport=_freeze_tikhub_transport(call_override),
        content_id=content_id,
        stage="detail",
        window_key=XHS_TYPE_PROBE_WINDOW,
        provider=provider,
        adapter_version=XHS_TYPE_PROBE_ADAPTER,
        operation=operation,
        call=call,
        budget_id=budget_id,
        task_id=task_id,
        task_max_amount=task_max_amount,
        db_path=db_path,
        raw_root=RAW_ROOT / XHS_TYPE_PROBE_WINDOW,
        paid_request_identity=_paid_request_identity(
            operation=operation,
            platform="xiaohongshu",
            subject=str(content["platform_content_id"]),
            params={"note_id": str(content["platform_content_id"])},
            cursor=None,
            due_bucket=XHS_TYPE_PROBE_WINDOW,
        ),
    )


def _apply_xhs_type_probe(
    content: Mapping[str, Any], outcome: CaptureOutcome, *, db_path: Path,
) -> dict[str, Any]:
    """Apply the confirmed type, never a media manifest or a lifetime slot."""
    from .provider_budget import assert_paid_scope_owner
    with connect(db_path) as connection, transaction(connection):
        assert_paid_scope_owner(connection)
        current = connection.execute(
            "SELECT * FROM content_items WHERE id=?", (content["id"],),
        ).fetchone()
        raw = connection.execute(
            "SELECT content_id,provider FROM provider_raw_responses WHERE id=?",
            (outcome.raw_response_id,),
        ).fetchone()
        if (current is None or raw is None or raw["content_id"] != content["id"]
                or raw["provider"] != "TikHub"):
            raise ProviderConfigurationError("类型探测原始证据与作品不一致")
        confirmed = outcome.data["content_type"]
        if current["content_type"] not in {"unknown", confirmed}:
            raise ProviderConfigurationError("类型探测与已确认作品类型冲突")
        if current["content_type"] == "unknown":
            connection.execute(
                "UPDATE content_items SET content_type=?,updated_at=? WHERE id=?",
                (confirmed, now_utc(), content["id"]),
            )
        updated = dict(current)
        updated["content_type"] = confirmed
    metrics = _mapping(outcome.data.get("metrics"))
    if metrics:
        captured_at = _raw_response_captured_at(outcome.raw_response_id, db_path=db_path)
        # The already-paid probe's counters are observations, not a metrics slot.
        metric_outcome = CaptureOutcome(
            slot_id=outcome.slot_id, attempt_id=outcome.attempt_id,
            raw_response_id=outcome.raw_response_id,
            data={**dict(metrics), "_evidence_captured_at": captured_at},
            billed=False, amount=0.0, currency="USD",
        )
        _store_stage_result(
            updated, "metrics",
            datetime.fromisoformat(captured_at.replace("Z", "+00:00")).astimezone(SHANGHAI).date().isoformat(),
            metric_outcome, db_path=db_path,
        )
    return updated



def update_content_data(
    content_id: int,
    *,
    as_of: Optional[date] = None,
    db_path: Path = DEFAULT_DB,
    call_override: Optional[Callable[[str, Mapping[str, Any]], ProviderResult]] = None,
    stages: Optional[Sequence[str]] = None,
    process_media: bool = True,
    task_id: Optional[str] = None,
    task_max_amount: Optional[float] = None,
    cache_only: bool = False,
) -> Dict[str, Any]:
    with connect(db_path) as connection:
        row = connection.execute(
            "SELECT * FROM content_items WHERE id=?", (content_id,)
        ).fetchone()
        if row is None:
            raise ProviderConfigurationError("内容不存在")
        content = dict(row)
        succeeded_slots = {
            (str(slot["stage"]), str(slot["window_key"])): str(slot["provider"])
            for slot in connection.execute(
                """
                SELECT stage, window_key, provider FROM fetch_slots
                WHERE content_id=? AND status='succeeded'
                  AND stage IN ('detail','metrics','comments')
                """,
                (content_id,),
            ).fetchall()
        }
    platform = str(content["platform"])
    if platform not in {"douyin", "xiaohongshu"}:
        raise ProviderConfigurationError(
            "视频号和快手首版只支持人工导入，未配置自动数据源"
        )
    local_date = as_of or datetime.now(SHANGHAI).date()
    iso = local_date.isocalendar()
    detail_slot = succeeded_slots.get(("detail", "lifetime"))
    detail_ready = detail_slot is not None and (
        detail_slot == "legacy-cache"
        or _slot_raw_is_applied(
            content_id=content_id,
            stage="detail",
            window_key="lifetime",
            db_path=db_path,
        )
    )
    requested_stages = [] if detail_ready else [("detail", "lifetime")]
    requested_stages.extend(
        [
            ("metrics", local_date.isoformat()),
            ("comments", f"{iso.year}-W{iso.week:02d}"),
        ]
    )
    if stages is not None:
        requested = set(stages)
        invalid = requested - {"detail", "metrics", "comments"}
        if invalid:
            raise ProviderConfigurationError(
                f"未知抓取阶段：{','.join(sorted(invalid))}"
            )
        requested_stages = [item for item in requested_stages if item[0] in requested]
        if (
            platform == "xiaohongshu"
            and detail_ready
            and {"detail", "metrics"}.issubset(requested)
        ):
            requested_stages.insert(0, ("detail", "lifetime"))
    paired_xhs_detail_metrics = (
        platform == "xiaohongshu"
        and stages is not None
        and {"detail", "metrics"}.issubset(set(stages))
    )
    outcomes: List[Dict[str, Any]] = []
    xhs_detail_metrics: Optional[Mapping[str, Any]] = None
    xhs_detail_raw_response_id: Optional[int] = None
    for stage, window_key in requested_stages:
        if stage == "comments":
            try:
                capture_result = capture_content_comments_live(
                    content_id,
                    as_of=local_date,
                    db_path=db_path,
                    call_override=call_override,
                    task_id=task_id,
                    task_max_amount=task_max_amount,
                    cache_only=cache_only,
                )
                capture_status = str(capture_result["status"])
                amount = float(capture_result.get("provider_cost") or 0.0)
                stop_reason = str(capture_result.get("stop_reason") or "")
                cache_only_unavailable = (
                    capture_status == "incomplete"
                    and stop_reason == "cache_only_page_unavailable"
                )
                outcomes.append(
                    {
                        "stage": "comments",
                        "status": (
                            "failed"
                            if capture_status == "incomplete"
                            else capture_status
                        ),
                        "retryable": capture_status == "incomplete",
                        "error_code": (
                            "cache_only_stage_unavailable"
                            if cache_only_unavailable
                            else "comment_capture_incomplete"
                            if capture_status == "incomplete"
                            else None
                        ),
                        "message": stop_reason,
                        "completion_kind": capture_result.get("completion_kind"),
                        "stop_reason": capture_result.get("stop_reason"),
                        "pages_fetched": int(
                            capture_result.get("pages_fetched") or 0
                        ),
                        "billed": amount > 0,
                        "amount": amount,
                        "currency": "USD",
                    }
                )
            except Exception as exc:
                error_code = getattr(exc, "error_code", type(exc).__name__)
                outcomes.append(
                    {
                        "stage": "comments",
                        "status": "failed",
                        "error_code": error_code,
                        "retryable": bool(getattr(exc, "retryable", False)),
                        "message": str(exc),
                    }
                )
                if error_code in {
                    "provider_balance_blocked",
                    "provider_auth_blocked",
                    "budget_blocked",
                    "task_budget_exhausted",
                    "budget_daily_quota_exhausted",
                }:
                    break
            continue
        succeeded_provider = succeeded_slots.get((stage, window_key))
        raw_is_applied = succeeded_provider is not None and _slot_raw_is_applied(
            content_id=content_id,
            stage=stage,
            window_key=window_key,
            db_path=db_path,
        )
        storage_is_ready = stage == "detail" or _stage_storage_exists(
            content_id=content_id,
            platform=platform,
            stage=stage,
            window_key=window_key,
            db_path=db_path,
        )
        if succeeded_provider is not None and (
            succeeded_provider == "legacy-cache"
            or (raw_is_applied and storage_is_ready)
        ):
            if paired_xhs_detail_metrics and stage == "detail":
                try:
                    outcome = _replay_content_stage(
                        content,
                        stage=stage,
                        window_key=window_key,
                        operation=STAGE_CONFIG[(platform, stage)][2],
                        db_path=db_path,
                    )
                    _store_stage_result(
                        content, stage, window_key, outcome, db_path=db_path
                    )
                    detail_metrics = outcome.data.get("metrics")
                    if isinstance(detail_metrics, dict):
                        xhs_detail_metrics = detail_metrics
                        xhs_detail_raw_response_id = int(outcome.raw_response_id)
                    outcomes.append(
                        {
                            "stage": stage,
                            "status": "replayed",
                            "billed": False,
                            "amount": 0.0,
                            "currency": "USD",
                        }
                    )
                except Exception as exc:
                    outcomes.append(
                        {
                            "stage": stage,
                            "status": "failed",
                            "error_code": getattr(
                                exc, "error_code", type(exc).__name__
                            ),
                            "message": str(exc),
                        }
                    )
                continue
            outcomes.append(
                {
                    "stage": stage,
                    "status": "already_succeeded",
                    "message": "该内容在同一时间窗已有成功槽，未重复调用供应商",
                }
            )
            continue
        provider, adapter_version, operation, price = STAGE_CONFIG[(platform, stage)]
        if succeeded_provider is not None:
            try:
                outcome = _replay_content_stage(
                    content,
                    stage=stage,
                    window_key=window_key,
                    operation=operation,
                    db_path=db_path,
                )
                _store_stage_result(
                    content, stage, window_key, outcome, db_path=db_path
                )
                if platform == "xiaohongshu" and stage == "detail":
                    detail_metrics = outcome.data.get("metrics")
                    if isinstance(detail_metrics, dict):
                        xhs_detail_metrics = detail_metrics
                        xhs_detail_raw_response_id = int(outcome.raw_response_id)
                outcomes.append(
                    {
                        "stage": stage,
                        "status": "replayed",
                        "billed": False,
                        "amount": 0.0,
                        "currency": "USD",
                    }
                )
            except Exception as exc:
                outcomes.append(
                    {
                        "stage": stage,
                        "status": "failed",
                        "error_code": getattr(exc, "error_code", type(exc).__name__),
                        "message": str(exc),
                    }
                )
            continue
        type_probe: Optional[CaptureOutcome] = None
        if (platform == "xiaohongshu" and stage == "detail"
                and (content["content_type"] not in {"image", "video"}
                     or ("detail", XHS_TYPE_PROBE_WINDOW) in succeeded_slots)):
            if (
                cache_only
                and ("detail", XHS_TYPE_PROBE_WINDOW) not in succeeded_slots
            ):
                outcomes.append(
                    {
                        "stage": "detail",
                        "window_key": XHS_TYPE_PROBE_WINDOW,
                        "status": "failed",
                        "error_code": "cache_only_stage_unavailable",
                        "retryable": True,
                        "message": (
                            "cache-only mode has no succeeded local type-probe "
                            "evidence"
                        ),
                        "billed": False,
                        "amount": 0.0,
                        "currency": "USD",
                    }
                )
                continue
            try:
                type_probe = _xhs_type_probe(
                    content, db_path=db_path, call_override=call_override,
                    task_id=task_id, task_max_amount=task_max_amount,
                )
                content = _apply_xhs_type_probe(content, type_probe, db_path=db_path)
                outcomes.append({
                    "stage": "detail_type_probe", "window_key": XHS_TYPE_PROBE_WINDOW,
                    "status": "replayed" if type_probe.attempt_id == 0 else "succeeded",
                    "billed": type_probe.billed, "amount": type_probe.amount,
                    "currency": type_probe.currency or "USD",
                })
                if content["content_type"] == "image" and not type_probe.data.get("media_urls"):
                    raise CaptureError(
                        "Xiaohongshu image probe omitted main image sources",
                        retryable=False, error_code="xhs_probe_media_missing", billed=False,
                    )
            except Exception as exc:
                local_probe_unavailable = cache_only and isinstance(
                    exc, (SlotUnavailable, RawResponseIntegrityError)
                )
                outcomes.append({
                    "stage": "detail", "window_key": XHS_TYPE_PROBE_WINDOW,
                    "status": "failed", "error_code": (
                        "cache_only_stage_unavailable"
                        if local_probe_unavailable
                        else getattr(exc, "error_code", type(exc).__name__)
                    ),
                    "retryable": (
                        True
                        if local_probe_unavailable
                        else bool(getattr(exc, "retryable", False))
                    ),
                    "message": str(exc), "billed": getattr(exc, "billed", False),
                    "amount": price if getattr(exc, "billed", False) is True else 0.0,
                    "currency": "USD",
                })
                break
        derived_xhs_detail = type_probe is not None and content["content_type"] == "image"

        derived_xhs_metrics = (
            platform == "xiaohongshu"
            and stage == "metrics"
            and xhs_detail_metrics is not None
        )
        if paired_xhs_detail_metrics and stage == "metrics" and not derived_xhs_metrics:
            outcomes.append(
                {
                    "stage": stage,
                    "status": "failed",
                    "error_code": "xhs_detail_metrics_missing",
                    "retryable": False,
                    "message": "小红书详情未返回可派生指标；未发起未报价的第二次调用",
                }
            )
            continue
        if cache_only and not (derived_xhs_metrics or derived_xhs_detail):
            outcomes.append(
                {
                    "stage": stage,
                    "window_key": window_key,
                    "status": "failed",
                    "error_code": "cache_only_stage_unavailable",
                    "retryable": True,
                    "message": (
                        "cache-only mode has no succeeded local evidence for "
                        "this stage"
                    ),
                    "billed": False,
                    "amount": 0.0,
                    "currency": "USD",
                }
            )
            continue
        budget_id = (
            None
            if derived_xhs_metrics or derived_xhs_detail
            else _budget_for_call(
                provider=provider,
                operation=operation,
                price=price,
                task_id=task_id,
                task_max_amount=task_max_amount,
                db_path=db_path,
            )
        )
        try:
            if derived_xhs_detail:
                assert type_probe is not None
                with connect(db_path) as connection:
                    probe_raw = connection.execute(
                        "SELECT sha256,captured_at FROM provider_raw_responses WHERE id=?",
                        (type_probe.raw_response_id,),
                    ).fetchone()
                if probe_raw is None:
                    raise ProviderConfigurationError("小红书类型探测缺少原始响应")
                adapter_version = "tikhub-xhs-probe-derived-detail-v1"
                call = partial(
                    _derived_discovery_result, stage="detail", data=type_probe.data,
                    discovery_operation=operation,
                    source_raw_response_id=type_probe.raw_response_id,
                    source_sha256=str(probe_raw["sha256"]),
                    source_captured_at=str(probe_raw["captured_at"]),
                )
            elif derived_xhs_metrics:
                adapter_version = "tikhub-xhs-app-v2-statistics-derived-v8.1"
                metrics = dict(xhs_detail_metrics or {})
                if xhs_detail_raw_response_id is None:
                    raise ProviderConfigurationError("小红书详情指标缺少原始响应")
                source_captured_at = _raw_response_captured_at(
                    xhs_detail_raw_response_id, db_path=db_path
                )
                call = partial(
                    _derived_xhs_metrics_result,
                    metrics,
                    xhs_detail_raw_response_id,
                    source_captured_at,
                )
            elif call_override is not None:
                override_content = dict(content)
                if platform == "xiaohongshu" and stage == "detail":
                    override_content.update(
                        _detail_window_key=window_key, _xhs_type_probe=False,
                        _xhs_endpoint="get_video_note_detail" if content["content_type"] == "video" else "get_image_note_detail",
                    )
                call = partial(call_override, stage, override_content)
            elif platform == "douyin":
                key = _load_key(TIKHUB_KEY_FILE, "TIKHUB_API_KEY")
                call = partial(
                    _douyin_call, stage, str(content["platform_content_id"]), key
                )
            else:
                key = _load_key(TIKHUB_KEY_FILE, "TIKHUB_API_KEY")
                call = partial(
                    _xhs_call,
                    stage,
                    str(content["platform_content_id"]),
                    key,
                    str(content["content_type"]),
                )
            if derived_xhs_metrics or derived_xhs_detail:
                source_raw_id = type_probe.raw_response_id if derived_xhs_detail and type_probe is not None else xhs_detail_raw_response_id
                assert source_raw_id is not None
                outcome = execute_derived_content_fetch(
                    content_id=content_id, stage=stage, window_key=window_key,
                    provider=provider, adapter_version=adapter_version,
                    operation=operation, result=call(),
                    source_raw_response_id=source_raw_id, db_path=db_path,
                )
            else:
                platform_content_id = str(content["platform_content_id"])
                if platform == "douyin":
                    _, request_params = _douyin_request(stage, platform_content_id)
                else:
                    _, request_params = _xhs_request(
                        stage,
                        platform_content_id,
                        str(content["content_type"]),
                    )
                outcome = execute_content_fetch(
                    request_transport=_freeze_tikhub_transport(call_override),
                    content_id=content_id,
                    stage=stage,
                    window_key=window_key,
                    provider=provider,
                    adapter_version=adapter_version,
                    operation=operation,
                    call=call,
                    db_path=db_path,
                    budget_id=budget_id,
                    task_id=task_id if budget_id is not None else None,
                    task_max_amount=(
                        task_max_amount if budget_id is not None else None
                    ),
                    paid_request_identity=_paid_request_identity(
                        operation=operation,
                        platform=platform,
                        subject=platform_content_id,
                        params=request_params,
                        cursor=None,
                        due_bucket=window_key,
                    ),
                )
            _store_stage_result(content, stage, window_key, outcome, db_path=db_path)
            if platform == "xiaohongshu" and stage == "detail":
                detail_metrics = outcome.data.get("metrics")
                if isinstance(detail_metrics, dict):
                    xhs_detail_metrics = detail_metrics
                    xhs_detail_raw_response_id = int(outcome.raw_response_id)
            outcomes.append(
                {
                    "stage": stage,
                    "status": "succeeded",
                    "billed": outcome.billed,
                    "amount": outcome.amount,
                    "currency": outcome.currency or "USD",
                }
            )
        except SlotUnavailable as exc:
            with connect(db_path) as connection:
                slot = connection.execute(
                    """
                    SELECT status,provider,last_error_code,last_error_message
                    FROM fetch_slots
                    WHERE content_id=? AND stage=? AND window_key=?
                    """,
                    (content_id, stage, window_key),
                ).fetchone()
            slot_status = str(slot["status"]) if slot is not None else "missing"
            legacy_slot = (
                slot is not None and str(slot["provider"]) == "legacy-cache"
            )
            raw_is_applied = slot_status == "succeeded" and _slot_raw_is_applied(
                content_id=content_id,
                stage=stage,
                window_key=window_key,
                db_path=db_path,
            )
            storage_is_ready = stage == "detail" or _stage_storage_exists(
                content_id=content_id,
                platform=platform,
                stage=stage,
                window_key=window_key,
                db_path=db_path,
            )
            if slot_status == "succeeded" and (
                legacy_slot or (raw_is_applied and storage_is_ready)
            ):
                outcomes.append(
                    {
                        "stage": stage,
                        "status": "already_succeeded",
                        "message": str(exc),
                    }
                )
            else:
                outcomes.append(
                    {
                        "stage": stage,
                        "status": "failed",
                        "error_code": (
                            str(slot["last_error_code"] or exc.error_code)
                            if slot is not None
                            else exc.error_code
                        ),
                        "retryable": slot_status in {
                            "pending",
                            "running",
                            "retryable_failed",
                        },
                        "message": (
                            str(slot["last_error_message"] or exc)
                            if slot is not None
                            else str(exc)
                        ),
                        "slot_status": slot_status,
                    }
                )
        except Exception as exc:
            error_code = getattr(exc, "error_code", type(exc).__name__)
            outcomes.append(
                {
                    "stage": stage,
                    "status": "failed",
                    "error_code": error_code,
                    "retryable": bool(getattr(exc, "retryable", False)),
                    "message": str(exc),
                    "billed": getattr(exc, "billed", False),
                    "amount": price if getattr(exc, "billed", False) is True else 0.0,
                    "currency": "USD",
                }
            )
            if error_code in {
                "provider_balance_blocked",
                "provider_auth_blocked",
                "budget_blocked",
                "task_budget_exhausted",
                "budget_daily_quota_exhausted",
            }:
                break
    media_result: Optional[Dict[str, Any]] = None
    if process_media:
        try:
            media_result = process_content_media(content_id, db_path=db_path)
        except Exception as exc:
            media_result = {
                "content_id": content_id,
                "status": "retryable_failed",
                "error": f"{type(exc).__name__}: {exc}"[:500],
            }
    evaluation = (
        evaluate_content(content_id, db_path=db_path) if process_media else None
    )
    duplicate_result: Optional[Dict[str, Any]] = None
    if process_media:
        try:
            duplicate_result = refresh_content_duplicates(content_id, db_path=db_path)
        except Exception as exc:
            duplicate_result = {
                "content_id": content_id,
                "status": "retryable_failed",
                "error": f"{type(exc).__name__}: {exc}"[:500],
            }
    failed = any(item["status"] == "failed" for item in outcomes)
    if media_result is not None and media_result.get("status") == "retryable_failed":
        failed = True
    if (
        duplicate_result is not None
        and duplicate_result.get("status") == "retryable_failed"
    ):
        failed = True
    return {
        "content_id": content_id,
        "status": "partial" if failed else "succeeded",
        "stages": outcomes,
        "media": media_result,
        "evaluation_id": evaluation.evaluation_id if evaluation is not None else None,
        "evaluation_created": evaluation.created if evaluation is not None else False,
        "duplicates": duplicate_result,
        "provider_cost": round(
            sum(float(item.get("amount") or 0) for item in outcomes), 6
        ),
        "currency": "USD",
    }


def update_content_data_manual(
    content_id: int, *, db_path: Path = DEFAULT_DB,
    call_override: Optional[Callable[[str, Mapping[str, Any]], ProviderResult]] = None,
) -> Dict[str, Any]:
    """An explicit UI update uses the same field routes, cycles and budgets."""
    with connect(db_path) as connection:
        command_runtime = int(connection.execute("PRAGMA user_version").fetchone()[0]) >= 20
    if command_runtime:
        # Schema20 has a durable Writer command executor. Do not also execute
        # the legacy synchronous pipeline with a second scope or due bucket.
        from .capture_commands import submit_command

        return submit_command(db_path=db_path, content_id=content_id, kind="manual_update")
    from .account_roster import require_active_member
    from .media import MediaProcessingError, _has_managed_history, _managed_bundle
    from .media_lifecycle import LifecycleError
    from .media_retention import original_availability
    from .provider_budget import freeze_scope, paid_scope
    from .provider_updates import refresh_content_metrics

    with connect(db_path) as connection:
        frozen = freeze_scope(connection, content_id=content_id, account_id=None, stage="detail")
        assert frozen.identity_id is not None
        require_active_member(connection, frozen.identity_id, frozen.roster_snapshot_id,
                              frozen.roster_snapshot_hash, require_uid=True)
        bundle = _managed_bundle(connection, content_id)
        if bundle is None and _has_managed_history(connection, content_id):
            raise LifecycleError("managed_source_pending")
        media_view = original_availability(connection, content_id) if bundle is not None else None
    timestamp = now_utc()
    local_date = datetime.fromisoformat(timestamp.replace("Z", "+00:00")).astimezone(SHANGHAI).date()
    task_id = f"manual-content:{local_date.isoformat()}:{content_id}"
    results: List[Dict[str, Any]] = []
    stages = ["detail"] if bundle is None else []
    for stage in stages:
        with paid_scope(stage, roster_snapshot_id=frozen.roster_snapshot_id, roster_snapshot_hash=frozen.roster_snapshot_hash):
            results.append(update_content_data(
                content_id, as_of=local_date, db_path=db_path, call_override=call_override,
                stages=(stage,), process_media=False, task_id=task_id,
                task_max_amount=DEFAULT_TASK_MAX_AMOUNT_USD,
            ))
    with paid_scope("metrics", roster_snapshot_id=frozen.roster_snapshot_id, roster_snapshot_hash=frozen.roster_snapshot_hash):
        metrics = refresh_content_metrics(
            content_id, db_path=db_path, at=timestamp, task_id=task_id,
            task_max_amount=DEFAULT_TASK_MAX_AMOUNT_USD,
            call_override=call_override,
        )
    with paid_scope("comments", roster_snapshot_id=frozen.roster_snapshot_id, roster_snapshot_hash=frozen.roster_snapshot_hash):
        results.append(update_content_data(
            content_id, as_of=local_date, db_path=db_path, call_override=call_override,
            stages=("comments",), process_media=False, task_id=task_id,
            task_max_amount=DEFAULT_TASK_MAX_AMOUNT_USD,
        ))
    media_result: Optional[Dict[str, Any]] = None
    evaluation_result = None
    duplicate_result = None
    if media_view is not None and media_view["http_status"] != 200:
        media_result = {"status": "expired_non_replayable" if media_view["http_status"] == 410 else "restore_required",
                        "reason": media_view["reason"], "can_restore": media_view["can_restore"]}
    else:
        try:
            media_result = process_content_media(content_id, db_path=db_path)
            if media_result.get("status") == "evidence_ready":
                evaluation_result = evaluate_content(content_id, db_path=db_path)
                duplicate_result = refresh_content_duplicates(content_id, db_path=db_path)
        except LifecycleError as error:
            media_result = {"status": "expired_non_replayable" if error.error_code in {"original_expired", "expired_non_replayable"} else "restore_required",
                            "reason": error.error_code}
        except (MediaProcessingError, CaptureError) as error:
            media_result = {"status": "retryable_failed", "reason": str(getattr(error, "error_code", type(error).__name__))}
    return {
        "content_id": content_id,
        "status": "partial" if any(item["status"] != "succeeded" for item in results) or metrics["status"] != "succeeded" else "succeeded",
        "stages": [stage for item in results for stage in item.get("stages", [])],
        "metrics": metrics, "media": media_result, "duplicates": duplicate_result,
        "evaluation_id": evaluation_result.evaluation_id if evaluation_result else None,
        "evaluation_created": evaluation_result.created if evaluation_result else False,
        "provider_cost": round(sum(float(item.get("provider_cost") or 0) for item in results) + float(metrics["provider_cost"]), 6),
        "currency": "USD", "provider_policy": "matrix-first-v1",
    }



def retry_content_media(
    content_id: int,
    *,
    allow_paid_refresh: bool = False,
    db_path: Path = DEFAULT_DB,
    call_override: Optional[Callable[[str, Mapping[str, Any]], ProviderResult]] = None,
    task_id: Optional[str] = None,
    task_max_amount: Optional[float] = None,
) -> Dict[str, Any]:
    """Retry locally, legacy URL refresh only; managed reacquisition needs a task."""

    from .media import _has_managed_history, _managed_bundle
    from .media_lifecycle import LifecycleError
    from .media_retention import original_availability

    with connect(db_path) as connection:
        bundle = _managed_bundle(connection, content_id)
        if bundle is None and _has_managed_history(connection, content_id):
            raise LifecycleError(
                "explicit_reacquire_contract_not_bound"
                if allow_paid_refresh else "managed_source_pending"
            )
        if bundle is not None:
            view = original_availability(connection, content_id)
            if view["http_status"] != 200:
                raise LifecycleError(str(view["reason"]))
            if allow_paid_refresh:
                raise LifecycleError("explicit_reacquire_contract_not_bound")

    stale_recovery = recover_stale_media_processing_slots(
        db_path=db_path,
        processor_version_by_type=processor_versions(),
        content_ids=(content_id,),
    )
    source_before = get_media_source_state(content_id, db_path=db_path)
    download_before = (
        source_before.get("download_slot") if source_before is not None else None
    )
    terminal_source = bool(
        isinstance(download_before, dict)
        and download_before.get("status") == "terminal_failed"
    )
    local: Optional[Dict[str, Any]] = None
    if terminal_source:
        if not allow_paid_refresh:
            raise ProviderConfigurationError(
                "当前媒体源下载已终态失败；重新获取必须另建任务并授权，不能通过重试付费刷新"
            )
    else:
        try:
            local = process_content_media(content_id, db_path=db_path)
        except Exception as exc:
            source_after_local = get_media_source_state(content_id, db_path=db_path)
            download_after_local = (
                source_after_local.get("download_slot")
                if source_after_local is not None
                else None
            )
            download_failed = bool(
                isinstance(download_after_local, dict)
                and download_after_local.get("status")
                in {"retryable_failed", "terminal_failed"}
            )
            if source_after_local is not None:
                source_before = source_after_local
            terminal_source = bool(
                isinstance(download_after_local, dict)
                and download_after_local.get("status") == "terminal_failed"
            )
            if not download_failed:
                raise
            if not allow_paid_refresh:
                raise ProviderConfigurationError(
                    "当前媒体源下载失败；本地重试没有获取新来源的授权，请联系管理员核查"
                ) from exc

    if local is not None and local.get("status") in {"restore_required", "expired_non_replayable"}:
        raise LifecycleError(str(local.get("reason") or local["status"]))
    if local is not None and local.get("status") != "no_source":
        evaluation = evaluate_content(content_id, db_path=db_path)
        duplicates = refresh_content_duplicates(content_id, db_path=db_path)
        return {
            "content_id": content_id,
            "status": str(local.get("status")),
            "media": local,
            "evaluation_id": evaluation.evaluation_id,
            "evaluation_created": evaluation.created,
            "duplicates": duplicates,
            "provider_cost": 0.0,
            "currency": "USD",
            "media_source_refresh": {
                "status": "not_needed",
                "billed": False,
                "amount": 0.0,
                "raw_response_id": None,
                "previous_source_sha256": (
                    source_before.get("source_sha256")
                    if source_before is not None
                    else None
                ),
                "source_sha256": (
                    source_before.get("source_sha256")
                    if source_before is not None
                    else None
                ),
            },
            "stale_recovery": stale_recovery,
        }
    if not allow_paid_refresh:
        raise ProviderConfigurationError(
            "本地没有可用媒体源；重新获取必须另建任务并授权，不能通过重试付费刷新"
        )
    with connect(db_path) as connection:
        # Local processing may have registered an instance since entry.  Never
        # reinterpret that instance or its history as a failed legacy URL.
        if _managed_bundle(connection, content_id) is not None or _has_managed_history(connection, content_id):
            raise LifecycleError("explicit_reacquire_contract_not_bound")
        row = connection.execute(
            "SELECT * FROM content_items WHERE id=?", (content_id,)
        ).fetchone()
        if row is None:
            raise ProviderConfigurationError("内容不存在")
        content = dict(row)
    platform = str(content["platform"])
    if platform not in {"douyin", "xiaohongshu"}:
        raise ProviderConfigurationError("视频号和快手未配置自动媒体源刷新")
    provider, _, operation, price = STAGE_CONFIG[(platform, "detail")]
    adapter_version = (
        "tikhub-media-source-refresh-v8.1"
        if platform == "douyin"
        else "tikhub-xhs-app-v2-media-source-refresh-v8.1"
    )
    budget_id = _budget_for_call(
        provider=provider,
        operation=operation,
        price=price,
        task_id=task_id,
        task_max_amount=task_max_amount,
        db_path=db_path,
    )
    if call_override is not None:
        call = partial(call_override, "detail", content)
    elif platform == "douyin":
        call = partial(
            _douyin_call,
            "detail",
            str(content["platform_content_id"]),
            _load_key(TIKHUB_KEY_FILE, "TIKHUB_API_KEY"),
        )
    else:
        call = partial(
            _xhs_call,
            "detail",
            str(content["platform_content_id"]),
            _load_key(TIKHUB_KEY_FILE, "TIKHUB_API_KEY"),
            str(content["content_type"]),
        )
    platform_content_id = str(content["platform_content_id"])
    if platform == "douyin":
        _, request_params = _douyin_request("detail", platform_content_id)
    else:
        _, request_params = _xhs_request(
            "detail", platform_content_id, str(content["content_type"])
        )
    outcome = execute_content_fetch(
        request_transport=_freeze_tikhub_transport(call_override),
        content_id=content_id,
        stage="media_source_refresh",
        window_key="lifetime",
        provider=provider,
        adapter_version=adapter_version,
        operation=operation,
        call=call,
        db_path=db_path,
        budget_id=budget_id,
        task_id=task_id,
        task_max_amount=task_max_amount,
        paid_request_identity=_paid_request_identity(
            operation=operation,
            platform=platform,
            subject=platform_content_id,
            params=request_params,
            cursor=None,
            due_bucket="lifetime",
        ),
    )
    _store_stage_result(content, "detail", "lifetime", outcome, db_path=db_path)
    refreshed_source = get_media_source_state(content_id, db_path=db_path)
    if refreshed_source is None:
        raise ProviderConfigurationError("付费详情刷新未返回可用媒体源")
    refreshed_urls = refreshed_source.get("urls")
    refreshed_sha256 = str(refreshed_source.get("source_sha256") or "")
    if (
        refreshed_source.get("raw_response_id") != outcome.raw_response_id
        or not isinstance(refreshed_urls, list)
        or not refreshed_urls
        or not all(
            isinstance(value, str) and is_supported_media_url(value)
            for value in refreshed_urls
        )
        or len(refreshed_sha256) != 64
        or any(value not in "0123456789abcdef" for value in refreshed_sha256.lower())
    ):
        raise ProviderConfigurationError(
            "付费详情刷新的媒体源缺失、无效或与本次原始响应不匹配"
        )
    previous_sha256 = (
        str(source_before.get("source_sha256") or "")
        if source_before is not None
        else None
    )
    if terminal_source and previous_sha256 == refreshed_sha256:
        raise ProviderConfigurationError(
            "付费详情刷新未提供新媒体源，已拒绝复用终态失败的旧源"
        )
    media = process_content_media(content_id, db_path=db_path)
    evaluation = evaluate_content(content_id, db_path=db_path)
    duplicates = refresh_content_duplicates(content_id, db_path=db_path)
    return {
        "content_id": content_id,
        "status": str(media.get("status")),
        "media": media,
        "evaluation_id": evaluation.evaluation_id,
        "evaluation_created": evaluation.created,
        "duplicates": duplicates,
        "provider_cost": outcome.amount,
        "currency": outcome.currency or "USD",
        "media_source_refresh": {
            "status": "succeeded",
            "billed": outcome.billed,
            "amount": outcome.amount,
            "raw_response_id": outcome.raw_response_id,
            "previous_source_sha256": previous_sha256,
            "source_sha256": refreshed_sha256,
        },
        "stale_recovery": stale_recovery,
    }
