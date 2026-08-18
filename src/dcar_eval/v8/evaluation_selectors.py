"""Central evaluation read models for release-aware consumers.

The selectors are intentionally separate from evaluation writers.  Product
surfaces prefer the active release and may fall back to an older valid result
with an explicit stale marker.  Formal consumers never fall back.
"""

from __future__ import annotations

import re
import sqlite3
from collections.abc import Mapping, Sequence
from typing import Any


class EvaluationSelectorError(RuntimeError):
    """Raised when release state cannot support a strict selector."""


DISPLAY_EFFECTIVE_EVALUATIONS_CTE = """
active_evaluation_release AS (
    SELECT id FROM evaluation_releases WHERE status='active'
),
ranked_display_evaluations AS (
    SELECT ev.*,
           ar.id active_release_id,
           CASE
             WHEN ev.release_id=ar.id THEN 'current'
             ELSE 'stale'
           END evaluation_freshness,
           ROW_NUMBER() OVER (
             PARTITION BY ev.content_id
             ORDER BY
               CASE
                 WHEN ar.id IS NOT NULL AND ev.release_id=ar.id THEN 0
                 ELSE 1
               END,
               ev.evaluated_at DESC,
               ev.id DESC
           ) selector_rank
    FROM evaluation_versions ev
    JOIN evaluation_releases er ON er.id=ev.release_id
    JOIN active_evaluation_release ar ON 1=1
    WHERE ev.invalidated_at IS NULL
      AND (ev.release_id=ar.id OR er.status='retired')
),
display_effective_evaluations AS (
    SELECT * FROM ranked_display_evaluations WHERE selector_rank=1
)
""".strip()


FORMAL_CURRENT_EVALUATIONS_CTE = """
active_evaluation_release AS (
    SELECT id FROM evaluation_releases WHERE status='active'
),
ranked_formal_evaluations AS (
    SELECT ev.*,
           ROW_NUMBER() OVER (
             PARTITION BY ev.content_id
             ORDER BY ev.evaluated_at DESC, ev.id DESC
           ) selector_rank
    FROM evaluation_versions ev
    JOIN active_evaluation_release ar ON ar.id=ev.release_id
    WHERE ev.invalidated_at IS NULL
),
formal_current_evaluations AS (
    SELECT * FROM ranked_formal_evaluations WHERE selector_rank=1
)
""".strip()


def active_release(
    connection: sqlite3.Connection, *, required: bool = True
) -> sqlite3.Row | None:
    """Return the unique active release, failing closed on ambiguous state."""

    rows = connection.execute(
        "SELECT * FROM evaluation_releases WHERE status='active' ORDER BY id"
    ).fetchall()
    if len(rows) > 1:
        raise EvaluationSelectorError("multiple active evaluation releases exist")
    if not rows:
        if required:
            raise EvaluationSelectorError("no active evaluation release exists")
        return None
    return rows[0]


def _ids_clause(content_ids: Sequence[int]) -> tuple[str, list[int]]:
    ids = [int(value) for value in content_ids]
    if not ids:
        return "", []
    return f"WHERE content_id IN ({','.join('?' for _ in ids)})", ids


def formal_current_evaluations(
    connection: sqlite3.Connection, content_ids: Sequence[int]
) -> dict[int, dict[str, Any]]:
    """Return latest valid rows from the active release only."""

    active_release(connection)
    where, parameters = _ids_clause(content_ids)
    if not parameters:
        return {}
    rows = connection.execute(
        f"""
        WITH {FORMAL_CURRENT_EVALUATIONS_CTE}
        SELECT * FROM formal_current_evaluations
        {where}
        ORDER BY content_id
        """,
        parameters,
    ).fetchall()
    return {int(row["content_id"]): dict(row) for row in rows}


def formal_as_of_evaluations(
    connection: sqlite3.Connection,
    content_ids: Sequence[int],
    *,
    report_cutoff_at: str,
) -> dict[int, dict[str, Any]]:
    """Return the formal evaluation snapshot that was active at ``cutoff``.

    Release activation/retirement and evaluation invalidation are both
    interpreted temporally.  A missing historical release fails closed to an
    empty context; overlapping releases are corrupt lineage and raise.
    """

    releases = connection.execute(
        """
        SELECT * FROM evaluation_releases
        WHERE activated_at IS NOT NULL
          AND julianday(activated_at) <= julianday(?)
          AND (retired_at IS NULL OR julianday(retired_at) > julianday(?))
        ORDER BY julianday(activated_at) DESC, id DESC
        """,
        (report_cutoff_at, report_cutoff_at),
    ).fetchall()
    if len(releases) > 1:
        raise EvaluationSelectorError(
            "multiple evaluation releases were active at report cutoff"
        )
    if not releases:
        return {}
    ids = [int(value) for value in content_ids]
    if not ids:
        return {}
    placeholders = ",".join("?" for _ in ids)
    rows = connection.execute(
        f"""
        SELECT * FROM (
            SELECT ev.*,
                   ROW_NUMBER() OVER (
                       PARTITION BY ev.content_id
                       ORDER BY julianday(ev.evaluated_at) DESC, ev.id DESC
                   ) selector_rank
            FROM evaluation_versions ev
            WHERE ev.release_id=?
              AND ev.content_id IN ({placeholders})
              AND julianday(ev.evaluated_at) <= julianday(?)
              AND (
                    ev.invalidated_at IS NULL
                    OR julianday(ev.invalidated_at) > julianday(?)
              )
        ) WHERE selector_rank=1
        ORDER BY content_id
        """,
        [str(releases[0]["id"]), *ids, report_cutoff_at, report_cutoff_at],
    ).fetchall()
    return {int(row["content_id"]): dict(row) for row in rows}


def release_current_evaluations(
    connection: sqlite3.Connection,
    release_id: str,
    content_ids: Sequence[int],
) -> dict[int, dict[str, Any]]:
    """Return latest valid rows from one explicitly pinned release."""

    release = connection.execute(
        "SELECT id FROM evaluation_releases WHERE id=?", (release_id,)
    ).fetchone()
    if release is None:
        raise EvaluationSelectorError(
            f"evaluation release does not exist: {release_id}"
        )
    ids = [int(value) for value in content_ids]
    if not ids:
        return {}
    placeholders = ",".join("?" for _ in ids)
    rows = connection.execute(
        f"""
        SELECT * FROM (
            SELECT ev.*,
                   ROW_NUMBER() OVER (
                       PARTITION BY ev.content_id
                       ORDER BY ev.evaluated_at DESC, ev.id DESC
                   ) selector_rank
            FROM evaluation_versions ev
            WHERE ev.release_id=? AND ev.invalidated_at IS NULL
              AND ev.content_id IN ({placeholders})
        ) WHERE selector_rank=1
        ORDER BY content_id
        """,
        [release_id, *ids],
    ).fetchall()
    return {int(row["content_id"]): dict(row) for row in rows}


def formal_eligible_release_evaluations(
    connection: sqlite3.Connection,
    release_id: str,
    content_ids: Sequence[int],
) -> dict[int, dict[str, Any]]:
    """Return report-safe evaluations for one pinned release.

    Every rule excludes weak V0/V1 evidence.  Evaluation-v9 V2/V3 rows are
    always formal.  For historical (pre-v9) releases the retired
    ``pending_review`` gray-zone marker is derived from stored fields instead:
    an automatic 60-74 weak match never entered formal metrics, while human
    (``manual_review``) conclusions were final regardless of score.  Schema
    v16 removed the manual-review domain, so no queue state is consulted.
    """

    release = connection.execute(
        "SELECT rule_version FROM evaluation_releases WHERE id=?", (release_id,)
    ).fetchone()
    if release is None:
        raise EvaluationSelectorError(
            f"evaluation release does not exist: {release_id}"
        )
    evaluations = release_current_evaluations(connection, release_id, content_ids)
    if not evaluations:
        return {}
    base_eligible = {
        content_id: value
        for content_id, value in evaluations.items()
        if value["evidence_level"] in {"V2", "V3"}
    }
    if str(release["rule_version"]) == "evaluation-v9":
        return base_eligible
    return {
        content_id: value
        for content_id, value in base_eligible.items()
        if not (
            str(value["evaluation_source"]) != "manual_review"
            and value["selling_point_score"] is not None
            and 60 <= int(value["selling_point_score"]) < 75
        )
    }


def display_effective_evaluations(
    connection: sqlite3.Connection, content_ids: Sequence[int]
) -> dict[int, dict[str, Any]]:
    """Prefer active-release rows and explicitly mark historical fallback rows."""

    active_release(connection)
    where, parameters = _ids_clause(content_ids)
    if not parameters:
        return {}
    rows = connection.execute(
        f"""
        WITH {DISPLAY_EFFECTIVE_EVALUATIONS_CTE}
        SELECT * FROM display_effective_evaluations
        {where}
        ORDER BY content_id
        """,
        parameters,
    ).fetchall()
    return {int(row["content_id"]): dict(row) for row in rows}


def display_effective_evaluation(
    connection: sqlite3.Connection, content_id: int
) -> dict[str, Any] | None:
    return display_effective_evaluations(connection, [content_id]).get(content_id)


def audit_evaluations(
    connection: sqlite3.Connection, content_id: int
) -> list[sqlite3.Row]:
    """Return the full append-only history, including invalidated rows."""

    return connection.execute(
        """
        SELECT * FROM evaluation_versions
        WHERE content_id=? ORDER BY evaluated_at DESC, id DESC
        """,
        (content_id,),
    ).fetchall()


def effective_direction(
    content: Mapping[str, Any], evaluation: Mapping[str, Any] | None
) -> str:
    """Resolve direction while treating the legacy ``unknown`` token as absent."""

    evaluation = evaluation or {}
    for value in (
        content.get("manual_content_direction"),
        evaluation.get("content_direction"),
        content.get("evaluation_content_direction"),
        content.get("account_content_direction"),
    ):
        direction = str(value or "")
        if direction in {"new_car", "used_car", "media", "other"}:
            return direction
    return "unknown"


def effective_direction_sql(
    *, content_alias: str = "c", evaluation_alias: str = "ev", account_alias: str = "a"
) -> str:
    """Return the SQL equivalent of :func:`effective_direction`."""

    aliases = (content_alias, evaluation_alias, account_alias)
    if any(re.fullmatch(r"[A-Za-z_][A-Za-z0-9_]*", alias) is None for alias in aliases):
        raise ValueError("SQL aliases must be simple identifiers")
    return (
        f"COALESCE(NULLIF({content_alias}.manual_content_direction,'unknown'),"
        f"NULLIF({evaluation_alias}.content_direction,'unknown'),"
        f"NULLIF({content_alias}.evaluation_content_direction,'unknown'),"
        f"NULLIF({account_alias}.content_direction,'unknown'),'unknown')"
    )
