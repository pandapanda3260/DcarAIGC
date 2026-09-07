"""Read-only snapshot installation freshness, separate from capture completeness."""
from __future__ import annotations

import re
from collections.abc import Mapping
from datetime import datetime, time, timezone
from typing import Any
from zoneinfo import ZoneInfo

from . import artifact_paths


SHANGHAI = ZoneInfo("Asia/Shanghai")
PUBLISH_START_HOUR = 9
DELAY_AFTER_ACTIVE_SECONDS = 2 * 60 * 60
_TIMESTAMP = re.compile(r"\d{4}-\d{2}-\d{2}T\d{2}:\d{2}:\d{2}(?:\.\d+)?(?:Z|[+-](?:[01]\d|2[0-3]):[0-5]\d)")


def _timestamp(value: object) -> datetime | None:
    if not isinstance(value, str) or _TIMESTAMP.fullmatch(value) is None:
        return None
    try:
        parsed = datetime.fromisoformat(value.replace("Z", "+00:00"))
        return parsed.astimezone(timezone.utc) if parsed.utcoffset() is not None else None
    except (ValueError, OverflowError):
        return None


def active_publish_seconds(start: datetime, end: datetime) -> float:
    """Count only 09:00–24:00 Beijing, retaining any previous day's delay."""
    first, last = start.astimezone(SHANGHAI), end.astimezone(SHANGHAI)

    def progress(value: datetime) -> float:
        seconds = value.hour * 3600 + value.minute * 60 + value.second + value.microsecond / 1_000_000
        return max(0, seconds - PUBLISH_START_HOUR * 3600)

    return ((last.date() - first.date()).days * (24 - PUBLISH_START_HOUR) * 3600
            + progress(last) - progress(first))


def snapshot_sync_from_receipt(receipt: Mapping[str, Any] | None, *, now: datetime) -> dict[str, Any]:
    """Project an already verified installation receipt; never infer capture coverage."""
    if now.utcoffset() is None:
        raise ValueError("snapshot sync clock requires a timezone")
    current = now.astimezone(SHANGHAI)
    inactive = current.hour < PUBLISH_START_HOUR
    result: dict[str, Any] = {
        "status": "unknown",
        "last_verified_install_at": None,
        "window_state": "inactive" if inactive else "active",
        # The daily 09:00 calendar trigger is fixed. Active-hour runs use a
        # launchd interval, whose exact next execution cannot be inferred here.
        "next_scheduled_at": datetime.combine(current.date(), time(PUBLISH_START_HOUR), SHANGHAI).isoformat() if inactive else None,
    }
    if not isinstance(receipt, Mapping) or receipt.get("activation_status") != "succeeded":
        return result
    installed = _timestamp(receipt.get("installed_at"))
    if installed is None or installed > now:
        return result
    result["last_verified_install_at"] = installed.isoformat().replace("+00:00", "Z")
    result["status"] = "delayed" if active_publish_seconds(installed, now) > DELAY_AFTER_ACTIVE_SECONDS else "current"
    return result


def snapshot_sync_status(*, read_only: bool, now: datetime | None = None) -> dict[str, Any] | None:
    """Reuse the API's identity-keyed verified snapshot cache, with no data writes."""
    if not read_only:
        return None
    current = now or datetime.now(timezone.utc)
    try:
        context = artifact_paths.installed_snapshot()
        receipt = context.get("receipt") if context is not None else None
    except (OSError, ValueError, TypeError, KeyError):
        receipt = None
    return snapshot_sync_from_receipt(receipt, now=current)
