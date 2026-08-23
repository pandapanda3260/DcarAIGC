"""Independent Douyin OpenAPI reconciliation for the Mac writer."""

from __future__ import annotations

from datetime import datetime, time, timedelta, timezone
from pathlib import Path
from typing import Any, Callable, Mapping
from zoneinfo import ZoneInfo

from .capture import (
    CaptureError,
    ProviderResult,
    SlotUnavailable,
    execute_account_fetch,
    load_succeeded_raw_response,
    mark_succeeded_fetch_slot_retryable_failure,
)
from .douyin_openapi_client import DouyinMachineAPIError, DouyinMachineClient
from .providers import materialize_account_discovery_page
from .storage import DEFAULT_DB, connect


PROVIDER = "DouyinOpenAPI"
DISCOVERY_ADAPTER_VERSION = "douyin-openapi-video-list-v1"
DERIVED_ADAPTER_VERSION = "douyin-openapi-video-list-derived-v1"
DISCOVERY_OPERATION = "douyin_openapi_video_list"
DERIVED_OPERATIONS = {
    "detail": "douyin_openapi_video_list_derived_detail",
    "metrics": "douyin_openapi_video_list_derived_metrics",
}
PAGE_SIZE = 20
MAX_PAGES_PER_ACCOUNT = 1000
SHANGHAI = ZoneInfo("Asia/Shanghai")


def _utc_iso(value: datetime) -> str:
    return (
        value.astimezone(timezone.utc)
        .isoformat(timespec="seconds")
        .replace("+00:00", "Z")
    )


def _window(scheduled_for: datetime) -> tuple[datetime, datetime]:
    if scheduled_for.tzinfo is None:
        raise ValueError("scheduled_for must be timezone-aware")
    coverage_end = scheduled_for.astimezone(timezone.utc)
    local = scheduled_for.astimezone(SHANGHAI)
    current_monday = local.date() - timedelta(days=local.weekday())
    previous_monday = current_monday - timedelta(days=7)
    coverage_start = datetime.combine(previous_monday, time.min, SHANGHAI).astimezone(
        timezone.utc
    )
    if coverage_end < coverage_start:
        raise ValueError("scheduled_for precedes the reconciliation window")
    return coverage_start, coverage_end


def _active_identity_matches(
    *, account_id: int, platform_uid: str, db_path: Path
) -> bool:
    with connect(db_path) as connection:
        row = connection.execute(
            """
            SELECT 1
            FROM accounts a
            JOIN account_platform_identities api ON api.account_id=a.id
            WHERE a.id=? AND a.enabled=1
              AND api.platform='douyin' AND api.uid=?
            """,
            (account_id, platform_uid),
        ).fetchone()
    return row is not None


def _content_type(media_type: int | None) -> str:
    if media_type == 2:
        return "image"
    if media_type == 4:
        return "video"
    return "unknown"


def _discovery_item(item: Mapping[str, Any]) -> dict[str, Any]:
    video_id = str(item["video_id"])
    statistics = dict(item["statistics"])
    return {
        "platform": "douyin",
        "platform_content_id": video_id,
        "canonical_url": f"https://www.douyin.com/video/{video_id}",
        "published_at": int(item["create_time"]),
        "content_type": _content_type(item.get("media_type")),
        "title": str(item.get("title") or ""),
        "body": str(item.get("title") or ""),
        "media_urls": [],
        "metrics": {
            "view_count": statistics.get("play_count"),
            "comment_count": statistics.get("comment_count"),
            "like_count": statistics.get("digg_count"),
            "share_count": statistics.get("share_count"),
            "collect_count": None,
        },
    }


def _receipt(
    *,
    account_id: int,
    platform_uid: str,
    coverage_start: datetime,
    coverage_end: datetime,
) -> dict[str, Any]:
    return {
        "account_id": account_id,
        "platform_uid": platform_uid,
        "status": "failed",
        "coverage_start": _utc_iso(coverage_start),
        "coverage_end": _utc_iso(coverage_end),
        "coverage_complete": False,
        "pagination_complete": False,
        "materialization_complete": False,
        "pages_fetched": 0,
        "items_discovered": 0,
    }


def _error_code(exc: BaseException) -> str:
    if isinstance(exc, DouyinMachineAPIError):
        return exc.error_code
    if isinstance(exc, CaptureError):
        return exc.error_code
    if isinstance(exc, SlotUnavailable):
        return exc.error_code
    return "douyin_openapi_sync_failed"


def _capture_page(
    *,
    client: DouyinMachineClient,
    authorization_id: str,
    account_id: int,
    cursor: int,
    window_key: str,
    db_path: Path,
    raw_root: Path | None,
    disallowed_next_cursors: set[int],
    coverage_start_epoch: int,
    final_allowed_page: bool,
) -> tuple[dict[str, Any], int, int]:
    def call() -> ProviderResult:
        try:
            page = client.video_list_page(
                authorization_id=authorization_id,
                cursor=cursor,
                count=PAGE_SIZE,
            )
            has_more = bool(page["has_more"])
            boundary_reached = any(
                int(item["create_time"]) < coverage_start_epoch
                and item.get("is_top") is not True
                for item in page["items"]
            )
            if has_more and int(page["cursor"]) in disallowed_next_cursors:
                raise CaptureError(
                    "Douyin OpenAPI cursor repeated",
                    retryable=True,
                    error_code="pagination_cursor_loop",
                    http_status=200,
                    billed=False,
                    raw_response=page,
                )
            if final_allowed_page and has_more and not boundary_reached:
                raise CaptureError(
                    "Douyin OpenAPI page cap reached",
                    retryable=True,
                    error_code="pagination_page_limit",
                    http_status=200,
                    billed=False,
                    raw_response=page,
                )
        except DouyinMachineAPIError as exc:
            raise CaptureError(
                exc.error_code,
                retryable=exc.retryable,
                error_code=exc.error_code,
                http_status=exc.http_status,
                billed=False,
                raw_response=exc.raw_response,
            ) from exc
        return ProviderResult(
            data=page,
            raw_response=page,
            http_status=200,
            billed=False,
        )

    kwargs: dict[str, Any] = {}
    if raw_root is not None:
        kwargs["raw_root"] = raw_root
    try:
        outcome = execute_account_fetch(
            account_id=account_id,
            stage="discovery",
            window_key=window_key,
            provider=PROVIDER,
            adapter_version=DISCOVERY_ADAPTER_VERSION,
            operation=DISCOVERY_OPERATION,
            call=call,
            db_path=db_path,
            allow_terminal_retry=True,
            **kwargs,
        )
        return dict(outcome.data), outcome.raw_response_id, outcome.slot_id
    except SlotUnavailable:
        stored = load_succeeded_raw_response(
            account_id=account_id,
            stage="discovery",
            window_key=window_key,
            operation=DISCOVERY_OPERATION,
            db_path=db_path,
        )
        page = DouyinMachineClient._video_page(stored.value, count=PAGE_SIZE)
        return page, stored.raw_response_id, stored.slot_id


def _materialization_failed(result: Mapping[str, Any]) -> bool:
    derived = result.get("derived_stages")
    return not isinstance(derived, Mapping) or bool(derived.get("failures"))


def _reconcile_authorization(
    authorization: Mapping[str, Any],
    *,
    scheduled_for: datetime,
    coverage_start: datetime,
    coverage_end: datetime,
    client: DouyinMachineClient,
    db_path: Path,
    raw_root: Path | None,
    materialize_page: Callable[..., Mapping[str, Any]],
) -> dict[str, Any]:
    account_id = int(authorization["account_id"])
    platform_uid = str(authorization["platform_uid"])
    receipt = _receipt(
        account_id=account_id,
        platform_uid=platform_uid,
        coverage_start=coverage_start,
        coverage_end=coverage_end,
    )
    if not _active_identity_matches(
        account_id=account_id, platform_uid=platform_uid, db_path=db_path
    ):
        receipt["error_code"] = "authorization_identity_mismatch"
        return receipt
    if authorization.get("needs_reauthorization") is True:
        receipt["error_code"] = "authorization_needs_reauthorization"
        return receipt
    scopes = authorization.get("scopes")
    if not isinstance(scopes, list) or "video.list" not in scopes:
        receipt["error_code"] = "authorization_scope_missing"
        return receipt

    authorization_id = str(authorization["authorization_id"])
    occurrence = scheduled_for.astimezone(timezone.utc).strftime("%Y%m%dT%H%M%SZ")
    metrics_window_key = scheduled_for.astimezone(SHANGHAI).date().isoformat()
    cursor = 0
    requested_cursors: set[int] = set()
    discovered_ids: set[str] = set()
    materialized_any = False
    materialization_complete = True
    pagination_complete = False
    try:
        while receipt["pages_fetched"] < MAX_PAGES_PER_ACCOUNT:
            if cursor in requested_cursors:
                raise CaptureError(
                    "Douyin OpenAPI cursor repeated",
                    retryable=True,
                    error_code="pagination_cursor_loop",
                )
            requested_cursors.add(cursor)
            page_window_key = f"douyin-openapi:{occurrence}:cursor:{cursor}"
            page, raw_response_id, slot_id = _capture_page(
                client=client,
                authorization_id=authorization_id,
                account_id=account_id,
                cursor=cursor,
                window_key=page_window_key,
                db_path=db_path,
                raw_root=raw_root,
                disallowed_next_cursors=requested_cursors,
                coverage_start_epoch=int(coverage_start.timestamp()),
                final_allowed_page=(
                    receipt["pages_fetched"] + 1 == MAX_PAGES_PER_ACCOUNT
                ),
            )
            receipt["pages_fetched"] += 1
            raw_items = list(page["items"])
            boundary_reached = any(
                int(item["create_time"]) < int(coverage_start.timestamp())
                and item.get("is_top") is not True
                for item in raw_items
            )
            page_items: list[dict[str, Any]] = []
            for item in raw_items:
                created_at = int(item["create_time"])
                video_id = str(item["video_id"])
                if not int(coverage_start.timestamp()) <= created_at <= int(
                    coverage_end.timestamp()
                ):
                    continue
                if video_id in discovered_ids:
                    continue
                discovered_ids.add(video_id)
                page_items.append(_discovery_item(item))
            receipt["items_discovered"] = len(discovered_ids)
            normalized_page = {
                "captured_at": page["captured_at"],
                "cursor": page["cursor"],
                "has_more": page["has_more"],
                "items": page_items,
            }
            try:
                materialized = materialize_page(
                    account_id=account_id,
                    platform="douyin",
                    account_uid=platform_uid,
                    page=normalized_page,
                    source_raw_response_id=raw_response_id,
                    metrics_window_key=metrics_window_key,
                    discovery_operation=DISCOVERY_OPERATION,
                    provider=PROVIDER,
                    derived_adapter_version=DERIVED_ADAPTER_VERSION,
                    derived_operations=DERIVED_OPERATIONS,
                    zero_view_is_authoritative=True,
                    db_path=db_path,
                    published_start=coverage_start,
                    published_end=coverage_end,
                    materialize_detail=False,
                    materialize_existing_stages=True,
                )
                if _materialization_failed(materialized):
                    raise RuntimeError("derived materialization reported failures")
                materialized_any = materialized_any or bool(page_items)
            except Exception as exc:
                mark_succeeded_fetch_slot_retryable_failure(
                    db_path=db_path,
                    slot_id=slot_id,
                    error_code="derived_materialization_failed",
                    error_message=f"{type(exc).__name__}: derived page materialization failed",
                )
                materialization_complete = False
                raise CaptureError(
                    "Douyin OpenAPI materialization failed",
                    retryable=True,
                    error_code="derived_materialization_failed",
                ) from exc

            if boundary_reached or not bool(page["has_more"]):
                pagination_complete = True
                break
            next_cursor = int(page["cursor"])
            if next_cursor in requested_cursors:
                raise CaptureError(
                    "Douyin OpenAPI cursor repeated",
                    retryable=True,
                    error_code="pagination_cursor_loop",
                )
            cursor = next_cursor
        else:
            raise CaptureError(
                "Douyin OpenAPI page cap reached",
                retryable=True,
                error_code="pagination_page_limit",
            )
    except Exception as exc:
        receipt["status"] = (
            "partial"
            if receipt["pages_fetched"] > 1 or materialized_any
            else "failed"
        )
        receipt["coverage_complete"] = False
        receipt["pagination_complete"] = False
        receipt["materialization_complete"] = materialization_complete
        receipt["error_code"] = _error_code(exc)
        return receipt

    receipt["coverage_complete"] = pagination_complete and materialization_complete
    receipt["pagination_complete"] = pagination_complete
    receipt["materialization_complete"] = materialization_complete
    receipt["status"] = (
        "succeeded"
        if receipt["coverage_complete"]
        else "partial"
    )
    return receipt


def reconcile_with_client(
    *,
    scheduled_for: datetime,
    db_path: Path,
    client: DouyinMachineClient,
    raw_root: Path | None = None,
    materialize_page: Callable[..., Mapping[str, Any]] = materialize_account_discovery_page,
) -> Mapping[str, Any]:
    """Reconcile all active authorizations using an injected machine client."""

    coverage_start, coverage_end = _window(scheduled_for)
    authorizations = client.list_authorizations()
    accounts = [
        _reconcile_authorization(
            authorization,
            scheduled_for=scheduled_for,
            coverage_start=coverage_start,
            coverage_end=coverage_end,
            client=client,
            db_path=db_path,
            raw_root=raw_root,
            materialize_page=materialize_page,
        )
        for authorization in authorizations
    ]
    return {
        "window_start": _utc_iso(coverage_start),
        "coverage_end": _utc_iso(coverage_end),
        "accounts": accounts,
    }


def run_douyin_openapi_reconcile(
    *, scheduled_for: datetime, db_path: Path = DEFAULT_DB
) -> Mapping[str, Any]:
    """Public scheduler entry point; it intentionally remains outside JOBS."""

    with DouyinMachineClient.from_env() as client:
        return reconcile_with_client(
            scheduled_for=scheduled_for,
            db_path=db_path,
            client=client,
        )
