"""The persisted reconcile date is the first business day owned by automation."""

from __future__ import annotations

import os
from contextlib import contextmanager
from contextvars import ContextVar
from datetime import date, datetime, time
from typing import Iterator
from zoneinfo import ZoneInfo

BEIJING = ZoneInfo("Asia/Shanghai")
_BUSINESS_DAY: ContextVar[date | None] = ContextVar("automatic_business_day", default=None)


def automatic_from_date() -> date | None:
    scoped = _BUSINESS_DAY.get()
    if scoped is not None:
        return scoped
    value = os.environ.get("DCAR_DAILY_CAPTURE_RECONCILE_FROM", "").strip()
    if not value:
        return None
    parsed = date.fromisoformat(value)
    if parsed.isoformat() != value:
        raise ValueError("DCAR_DAILY_CAPTURE_RECONCILE_FROM must be YYYY-MM-DD")
    return parsed


@contextmanager
def automatic_scope(first_day: date | None) -> Iterator[None]:
    token = _BUSINESS_DAY.set(first_day or automatic_from_date())
    try:
        yield
    finally:
        _BUSINESS_DAY.reset(token)


def automatic_start_at() -> datetime | None:
    first_day = automatic_from_date()
    return datetime.combine(first_day, time.min, BEIJING) if first_day else None


def within_automatic_scope(value: object) -> bool:
    """Unknown publication dates cannot establish eligibility for future-only work."""
    start = automatic_start_at()
    if start is None:
        return True
    try:
        timestamp = datetime.fromisoformat(str(value).replace("Z", "+00:00"))
        if timestamp.tzinfo is None:
            if len(str(value)) != 10:
                return False
            timestamp = timestamp.replace(tzinfo=BEIJING)
        return timestamp >= start
    except (TypeError, ValueError):
        return False
