"""Thread-local, in-memory diagnostics; never evidence or authorization."""
from __future__ import annotations

from contextlib import contextmanager
from contextvars import ContextVar
from functools import wraps
import threading
import time


_CURRENT = ContextVar("runtime_phase_timing", default=None)


@contextmanager
def recording(*, enabled=True):
    state = {"owner": threading.get_ident(), "events": [], "stack": [], "dropped": 0}
    token = _CURRENT.set(state if enabled else None)
    try:
        yield state
    finally:
        _CURRENT.reset(token)


@contextmanager
def phase(name: str, **counts):
    state = _CURRENT.get()
    if state is None or state["owner"] != threading.get_ident():
        yield counts
        return
    started, cpu = time.perf_counter_ns(), time.thread_time_ns()
    parent = state["stack"][-1] if state["stack"] else None
    event = {"name": name, "parent": parent["id"] if parent else None,
             "id": len(state["events"]), "children_ns": 0, "children_cpu_ns": 0,
             "counts": counts, "outcome": "completed"}
    # Bound diagnostics independently of business work. Dropping is explicit.
    retained = len(state["events"]) < 4096
    if retained:
        state["events"].append(event)
    else:
        state["dropped"] += 1
    state["stack"].append(event)
    try:
        yield counts
    except BaseException:
        event["outcome"] = "error"
        raise
    finally:
        elapsed, cpu_elapsed = time.perf_counter_ns() - started, time.thread_time_ns() - cpu
        state["stack"].pop()
        event.update(wall_ns=elapsed, thread_cpu_ns=cpu_elapsed,
                     exclusive_ns=max(0, elapsed - event.pop("children_ns")),
                     exclusive_cpu_ns=max(0, cpu_elapsed - event.pop("children_cpu_ns")))
        if parent:
            parent["children_ns"] += elapsed
            parent["children_cpu_ns"] += cpu_elapsed


def measured(name, function, /, *args, **kwargs):
    with phase(name):
        return function(*args, **kwargs)


def timed(name):
    def decorate(function):
        @wraps(function)
        def wrapped(*args, **kwargs):
            with phase(name):
                return function(*args, **kwargs)
        return wrapped
    return decorate


def snapshot(state):
    """Materialize only after the owner has released the write lock."""
    return {"events": state["events"], "dropped": state["dropped"]}
