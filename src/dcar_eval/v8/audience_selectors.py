"""Canonical as-of selectors for user-level audience evidence.

Audience metrics and the deterministic classifier must read the same comment
snapshot.  These helpers select one latest evidence version per content and
one latest classification per user at a report cutoff; append-only history is
kept for audit but never accumulated into a current denominator.
"""

from __future__ import annotations

import sqlite3
from datetime import datetime
from typing import Any, Dict, List, Optional, Sequence


def _as_utc_datetime(value: str) -> datetime:
    return datetime.fromisoformat(str(value).replace("Z", "+00:00"))


def timestamp_at_or_after(candidate: str, required: str) -> bool:
    """Return whether ``candidate`` is at/after ``required``; fail closed."""

    try:
        return _as_utc_datetime(candidate) >= _as_utc_datetime(required)
    except (TypeError, ValueError):
        return False


def timestamp_before(candidate: str, upper_bound: str) -> bool:
    """Return whether ``candidate`` is strictly before ``upper_bound``."""

    try:
        return _as_utc_datetime(candidate) < _as_utc_datetime(upper_bound)
    except (TypeError, ValueError):
        return False


def latest_comment_rows(
    connection: sqlite3.Connection,
    content_ids: Sequence[int],
    *,
    report_cutoff_at: Optional[str] = None,
    evidence_window_start: Optional[str] = None,
    evidence_window_end: Optional[str] = None,
) -> List[Dict[str, Any]]:
    """Return L1 comments from one latest evidence version per content.

    ``captured_at`` is bounded by the report cutoff.  Comment behavior time is
    bounded only when both evidence-window endpoints are supplied. Missing or
    unparsable behavior timestamps fail closed and do not enter a timed ratio.
    """

    ids = list(dict.fromkeys(int(value) for value in content_ids))
    if not ids:
        return []
    placeholders = ",".join("?" for _ in ids)
    parameters: List[Any] = [*ids]
    cutoff_clause = ""
    if report_cutoff_at is not None:
        cutoff_clause = """
              AND julianday(cev.captured_at) <= julianday(?)
              AND julianday(cev.created_at) <= julianday(?)
        """
        parameters.extend((report_cutoff_at, report_cutoff_at))
    window_clause = ""
    if evidence_window_start is not None and evidence_window_end is not None:
        window_clause = """
          AND julianday(c.published_at) >= julianday(?)
          AND julianday(c.published_at) < julianday(?)
        """
        parameters.extend((evidence_window_start, evidence_window_end))
    rows = connection.execute(
        f"""
        WITH ranked_comment_evidence AS (
            SELECT cev.*,
                   ROW_NUMBER() OVER (
                       PARTITION BY cev.content_id
                       ORDER BY julianday(cev.captured_at) DESC, cev.id DESC
                   ) AS selector_rank
            FROM comment_evidence_versions cev
            WHERE cev.content_id IN ({placeholders})
              {cutoff_clause}
        ),
        selected_comment_evidence AS (
            SELECT * FROM ranked_comment_evidence WHERE selector_rank=1
        )
        SELECT c.*, cev.content_id,
               cev.id AS selected_evidence_version_id,
               cev.sha256 AS selected_evidence_sha256,
               cev.captured_at AS evidence_captured_at,
               cev.status AS evidence_status,
               iu.platform AS interaction_user_platform,
               iu.key_version AS interaction_user_key_version
        FROM selected_comment_evidence cev
        JOIN comments c ON c.evidence_version_id=cev.id
        LEFT JOIN interaction_users iu ON iu.id=c.interaction_user_id
        WHERE c.parent_comment_id IS NULL
          {window_clause}
        ORDER BY cev.content_id, c.id
        """,
        parameters,
    ).fetchall()
    return [dict(row) for row in rows]


def latest_user_classifications(
    connection: sqlite3.Connection,
    interaction_user_ids: Sequence[int],
    *,
    audience_definition_version: str,
    classifier_version: str,
    report_cutoff_at: Optional[str] = None,
    evidence_window_end: Optional[str] = None,
) -> Dict[int, Dict[str, Any]]:
    """Return the latest applicable classification per user at a cutoff."""

    ids = list(dict.fromkeys(int(value) for value in interaction_user_ids))
    if not ids:
        return {}
    placeholders = ",".join("?" for _ in ids)
    parameters: List[Any] = [
        audience_definition_version,
        classifier_version,
        *ids,
    ]
    cutoff_clause = ""
    if report_cutoff_at is not None:
        cutoff_clause = """
              AND julianday(cls.created_at) <= julianday(?)
              AND julianday(cls.evidence_window_end) <= julianday(?)
        """
        parameters.extend((report_cutoff_at, report_cutoff_at))
    evidence_clause = ""
    if evidence_window_end is not None:
        evidence_clause = """
              AND julianday(cls.evidence_window_end) <= julianday(?)
              AND ABS(
                    julianday(cls.evidence_window_end)
                    - julianday(cls.evidence_window_start)
                    - 90.0
              ) < 0.000001
        """
        parameters.append(evidence_window_end)
    rows = connection.execute(
        f"""
        SELECT * FROM (
            SELECT cls.*,
                   ROW_NUMBER() OVER (
                       PARTITION BY cls.interaction_user_id
                       ORDER BY julianday(cls.evidence_window_end) DESC,
                                julianday(cls.created_at) DESC,
                                cls.id DESC
                   ) AS selector_rank
            FROM interaction_user_classification_versions cls
            WHERE cls.audience_definition_version=?
              AND cls.classifier_version=?
              AND cls.interaction_user_id IN ({placeholders})
              {cutoff_clause}
              {evidence_clause}
        )
        WHERE selector_rank=1
        ORDER BY interaction_user_id
        """,
        parameters,
    ).fetchall()
    return {int(row["interaction_user_id"]): dict(row) for row in rows}
