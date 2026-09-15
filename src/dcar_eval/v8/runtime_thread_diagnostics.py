"""Read-only thread locations; no source text, arguments, or local values."""
from __future__ import annotations

import sys
import threading
from collections import deque


def snapshot() -> dict:
    """Keep at most 20 frames: ten inner and ten outer when a stack is long."""
    threads = {thread.ident: thread for thread in threading.enumerate()}
    current = sys._current_frames()
    result = []
    frame = None
    try:
        for ident, frame in sorted(current.items()):
            thread = threads.get(ident)
            inner, outer = [], deque(maxlen=10)
            frame_count = 0
            while frame is not None:
                module = frame.f_globals.get("__name__")
                location = {
                    "module": module if isinstance(module, str) else None,
                    "function": frame.f_code.co_name,
                    "line": frame.f_lineno,
                }
                if frame_count < 10:
                    inner.append(location)
                else:
                    outer.append(location)
                frame_count += 1
                frame = frame.f_back
            omitted = max(0, frame_count - 20)
            result.append({
                "ident": ident,
                "native_id": thread.native_id if thread is not None else None,
                "name": thread.name if thread is not None else None,
                "frames": inner + list(outer),
                "truncated": omitted > 0,
                "omitted_frame_count": omitted,
            })
    finally:
        # Do not retain live frame objects after the snapshot returns.
        frame = None
        current.clear()
    return {"contract": "runtime-thread-locations-v1", "threads": result}
