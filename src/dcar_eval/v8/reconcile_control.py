"""Process-local work budget for one bounded reconcile control invocation.

The budget deliberately owns no database state.  It limits how many new units
of control work may be claimed and when another unit may start; it does not try
to preempt a filesystem sync or database transaction that is already running.
"""

from __future__ import annotations

import math
from contextlib import contextmanager
from contextvars import ContextVar
from threading import Lock
from time import monotonic
from typing import Callable, Iterator


MAX_RECONCILE_ITEMS = 50
MAX_RECONCILE_SECONDS = 60.0


def _item_limit(value: object) -> int:
    if type(value) is not int:
        raise TypeError("max_items must be an integer")
    if not 1 <= value <= MAX_RECONCILE_ITEMS:
        raise ValueError(f"max_items must be 1..{MAX_RECONCILE_ITEMS}")
    return value


def _seconds_limit(value: object) -> float:
    if isinstance(value, bool) or not isinstance(value, (int, float)):
        raise TypeError("max_seconds must be a number")
    seconds = float(value)
    if not math.isfinite(seconds) or not 0 < seconds <= MAX_RECONCILE_SECONDS:
        raise ValueError(f"max_seconds must be >0 and <= {MAX_RECONCILE_SECONDS:g}")
    return seconds


class ReconcileBudget:
    """Thread-safe item/deadline gate shared by copied execution contexts."""

    def __init__(
        self,
        *,
        max_items: int,
        max_seconds: float,
        monotonic_fn: Callable[[], float],
    ) -> None:
        self.max_items = _item_limit(max_items)
        self.max_seconds = _seconds_limit(max_seconds)
        self._clock = monotonic_fn
        started_at = self._read_clock()
        self.started_at = started_at
        self.deadline = started_at + self.max_seconds
        if not math.isfinite(self.deadline):
            raise ValueError("reconcile deadline must be finite")
        self._remaining = self.max_items
        self._lock = Lock()

    def _read_clock(self) -> float:
        value = self._clock()
        if isinstance(value, bool) or not isinstance(value, (int, float)):
            raise TypeError("monotonic clock must return a number")
        current = float(value)
        if not math.isfinite(current):
            raise ValueError("monotonic clock must return a finite number")
        return current

    def _expired_locked(self) -> bool:
        return self._read_clock() >= self.deadline

    @property
    def expired(self) -> bool:
        with self._lock:
            return self._expired_locked()

    @property
    def remaining(self) -> int:
        """Return units that may still be claimed at the instant of the read."""

        with self._lock:
            if self._expired_locked():
                return 0
            return self._remaining

    @property
    def used(self) -> int:
        """Return the exact number of units claimed or completed so far."""

        with self._lock:
            return self.max_items - self._remaining

    def take(self, count: int = 1) -> bool:
        """Atomically claim ``count`` new units if item and time budgets allow."""

        if type(count) is not int:
            raise TypeError("count must be an integer")
        if count < 1:
            raise ValueError("count must be positive")
        with self._lock:
            if self._expired_locked() or count > self._remaining:
                return False
            self._remaining -= count
            return True

    def account_completed(self, count: int) -> None:
        """Record units completed by already-started work, even after deadline.

        This is retrospective accounting only: callers must bound the work from
        a prior ``remaining`` read, and this method never authorizes another
        unit to start.
        """

        if type(count) is not int:
            raise TypeError("count must be an integer")
        if count < 1:
            raise ValueError("count must be positive")
        with self._lock:
            if count > self._remaining:
                raise ValueError("completed count exceeds remaining item budget")
            self._remaining -= count


_CURRENT_RECONCILE_BUDGET: ContextVar[ReconcileBudget | None] = ContextVar(
    "current_reconcile_budget", default=None
)


def current_reconcile_budget() -> ReconcileBudget | None:
    """Return the budget bound to this execution context, if any."""

    return _CURRENT_RECONCILE_BUDGET.get()


@contextmanager
def reconcile_budget_scope(
    *,
    max_items: int = MAX_RECONCILE_ITEMS,
    max_seconds: float = MAX_RECONCILE_SECONDS,
    monotonic_fn: Callable[[], float] = monotonic,
) -> Iterator[ReconcileBudget]:
    """Bind one bounded reconcile budget and restore any outer context on exit."""

    budget = ReconcileBudget(
        max_items=max_items,
        max_seconds=max_seconds,
        monotonic_fn=monotonic_fn,
    )
    token = _CURRENT_RECONCILE_BUDGET.set(budget)
    try:
        yield budget
    finally:
        _CURRENT_RECONCILE_BUDGET.reset(token)
