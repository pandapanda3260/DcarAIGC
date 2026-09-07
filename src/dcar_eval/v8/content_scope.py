"""Read-only logical content scope; retained merge evidence is not a second work."""

from __future__ import annotations

import re
import sqlite3
from datetime import datetime

from .metric_field_facts import utc


def canonical_content_predicate(
    connection: sqlite3.Connection, alias: str = "c", *, knowledge_at: str | None = None,
) -> str:
    """Exclude merge losers known at the read cutoff, preserving schema19 scope.

    Only a validated SQL identifier and a parsed, canonical timestamp are emitted;
    neither caller-supplied SQL nor arbitrary text can enter this predicate.
    Existing frozen report scopes are not rewritten when an alias is learned.
    """
    if re.fullmatch(r"[A-Za-z_][A-Za-z_0-9]*", alias) is None:
        raise ValueError("Invalid content table alias")
    if int(connection.execute("PRAGMA user_version").fetchone()[0]) != 20:
        return "1=1"
    cutoff = ""
    if knowledge_at is not None:
        value = datetime.fromisoformat(knowledge_at.replace("Z", "+00:00"))
        if value.tzinfo is None:
            raise ValueError("Content scope knowledge time must be timezone aware")
        timestamp = utc(knowledge_at)
        cutoff = f" AND content_merge.recorded_at<='{timestamp}'"
    return (
        "NOT EXISTS (SELECT 1 FROM content_identity_merge_events content_merge "
        f"WHERE content_merge.loser_content_id={alias}.id{cutoff})"
    )
