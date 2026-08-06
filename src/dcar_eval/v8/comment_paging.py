"""Cursor-paged comment capture with page-level idempotency (paged-comments-v2).

A capture *run* owns one ``(content_id, window_key)`` slot and walks the
provider cursor page by page. Every page is fetched through an idempotent
per-page fetch slot (window key ``<window>:page:<cursor_sha>``) so a resumed or
replayed run never pays twice for a page it already stored. When the run
terminates the ordered pages are folded into a single aggregate evidence
manifest, one ``comment_evidence_versions`` row, the deduplicated first-level
comments, and one deterministic legacy score pass.

Only first-level comments (``parent_comment_id IS NULL``) enter the audience
universe, coverage and the 1,000-comment cap. Second-level replies that ride
along in a payload are still stored as evidence but never counted.
"""

from __future__ import annotations

import hashlib
import json
import sqlite3
from dataclasses import dataclass, field
from pathlib import Path
from typing import Any, Callable, Dict, List, Mapping, Optional, Sequence

from .capture import canonical_json_bytes
from .evaluation import upsert_comment_user_scores
from .identity import comment_identity_key, insert_comment_rows, legacy_user_score_rows
from .storage import (
    COMMENT_COLLECTION_VERSION,
    DEFAULT_DB,
    PROJECT_ROOT,
    connect,
    now_utc,
    transaction,
)


COMMENT_CAP = 1000
COVERAGE_TARGET = 0.9
MANIFEST_ROOT = PROJECT_ROOT / "data" / "cache" / "v8" / "comment_manifests"

_COMPLETION_KINDS = {
    "provider_exhausted",
    "coverage_target_reached",
    "cap_reached",
    "zero_comments",
}


class CommentPagingError(RuntimeError):
    pass


@dataclass
class PageFetch:
    """One page delivered by the injected fetcher."""

    raw_response_id: int
    fetch_slot_id: int
    result: Any  # ProviderResult-like: .data mapping
    already_stored: bool = False


PageFetcher = Callable[[int, Optional[Mapping[str, Any]]], PageFetch]


@dataclass
class _Accumulator:
    comments_by_identity: Dict[str, Dict[str, Any]] = field(default_factory=dict)
    page_manifest: List[Dict[str, Any]] = field(default_factory=list)
    declared_total: Optional[int] = None

    def distinct_l1(self) -> int:
        return sum(
            1
            for comment in self.comments_by_identity.values()
            if comment.get("parent_comment_id") in (None, "")
        )

    def stable_identity_l1(self) -> int:
        return sum(
            1
            for comment in self.comments_by_identity.values()
            if comment.get("parent_comment_id") in (None, "")
            and comment.get("pseudonymous_user_key")
        )


def cursor_sha256(cursor: Optional[Mapping[str, Any]]) -> str:
    return hashlib.sha256(canonical_json_bytes(cursor)).hexdigest()


def page_window_key(window_key: str, cursor: Optional[Mapping[str, Any]]) -> str:
    return f"{window_key}:page:{cursor_sha256(cursor)}"


def _get_or_create_run(
    connection: sqlite3.Connection,
    *,
    content_id: int,
    window_key: str,
    provider: str,
    adapter_version: str,
    comment_cap: int,
) -> sqlite3.Row:
    now = now_utc()
    connection.execute(
        """
        INSERT INTO comment_capture_runs(
            content_id, window_key, collection_version, provider, adapter_version,
            status, comment_cap, created_at, updated_at, started_at
        ) VALUES (?, ?, ?, ?, ?, 'running', ?, ?, ?, ?)
        ON CONFLICT(content_id, window_key) DO UPDATE SET
            status=CASE
                WHEN comment_capture_runs.status IN ('succeeded') THEN
                    comment_capture_runs.status
                ELSE 'running' END,
            updated_at=excluded.updated_at
        """,
        (
            content_id,
            window_key,
            COMMENT_COLLECTION_VERSION,
            provider,
            adapter_version,
            comment_cap,
            now,
            now,
            now,
        ),
    )
    row = connection.execute(
        "SELECT * FROM comment_capture_runs WHERE content_id=? AND window_key=?",
        (content_id, window_key),
    ).fetchone()
    if row is None:
        raise CommentPagingError("failed to create comment capture run")
    return row


def _load_existing_pages(
    connection: sqlite3.Connection, run_id: int
) -> List[sqlite3.Row]:
    return connection.execute(
        "SELECT * FROM comment_capture_pages WHERE capture_run_id=? ORDER BY page_number",
        (run_id,),
    ).fetchall()


def _accumulate_page(accumulator: _Accumulator, result_data: Mapping[str, Any]) -> None:
    declared = result_data.get("declared_total")
    if declared is not None:
        accumulator.declared_total = int(declared)
    for raw in result_data.get("comments") or []:
        body = str(raw.get("body") or "")
        identity_key = str(raw.get("comment_identity_key") or "") or comment_identity_key(
            platform_comment_id=raw.get("platform_comment_id"),
            pseudonymous_user_key=(
                raw.get("pseudonymous_user_key")
                or raw.get("anonymous_user_key")
            ),
            body=body,
            published_at=raw.get("published_at"),
        )
        if not identity_key:
            continue
        accumulator.comments_by_identity.setdefault(identity_key, dict(raw))


def _page_counts(result_data: Mapping[str, Any]) -> Dict[str, int]:
    comments = result_data.get("comments") or []
    l1 = [c for c in comments if c.get("parent_comment_id") in (None, "")]
    stable = [c for c in l1 if c.get("pseudonymous_user_key")]
    distinct = {
        str(c.get("comment_identity_key") or "")
        for c in l1
        if c.get("comment_identity_key")
    }
    return {
        "received": len(comments),
        "valid": len(l1),
        "distinct": len(distinct),
        "stable_identity": len(stable),
    }


def _decide_stop(
    *,
    accumulator: _Accumulator,
    has_more: bool,
    next_cursor: Optional[Mapping[str, Any]],
    comment_cap: int,
    coverage_target: float,
    pages_fetched: int,
) -> Optional[str]:
    distinct = accumulator.distinct_l1()
    # Hard cap first.
    if distinct >= comment_cap:
        return "cap_reached"
    # Genuine exhaustion (no further page) outranks an early-stop target: we
    # consumed everything rather than stopping short.
    if not has_more or next_cursor is None:
        if distinct == 0 and pages_fetched >= 1:
            return "zero_comments"
        return "provider_exhausted"
    # Early stop while more pages still exist and the coverage target is met.
    declared = accumulator.declared_total
    if (
        declared is not None
        and declared > 0
        and distinct >= coverage_target * declared
    ):
        return "coverage_target_reached"
    return None


def _write_manifest(
    *, content_id: int, window_key: str, manifest: Mapping[str, Any]
) -> tuple[str, str]:
    body = canonical_json_bytes(manifest)
    sha = hashlib.sha256(body).hexdigest()
    MANIFEST_ROOT.mkdir(parents=True, exist_ok=True)
    path = MANIFEST_ROOT / f"{content_id}_{window_key.replace(':', '_')}_{sha[:16]}.json"
    path.write_bytes(body)
    try:
        local_path = str(path.relative_to(PROJECT_ROOT))
    except ValueError:
        local_path = str(path)
    return local_path, sha


def capture_content_comments(
    content: Mapping[str, Any],
    *,
    window_key: str,
    page_fetcher: PageFetcher,
    provider: str,
    adapter_version: str,
    db_path: Path = DEFAULT_DB,
    comment_cap: int = COMMENT_CAP,
    coverage_target: float = COVERAGE_TARGET,
    max_pages: int = 200,
) -> Dict[str, Any]:
    """Drive a cursor-paged comment run to a terminal state and fold it down.

    ``page_fetcher(page_number, cursor)`` must return a :class:`PageFetch` whose
    ``result.data`` carries ``comments``, ``declared_total``, ``has_more`` and a
    structured ``next_cursor`` (or ``None``). It owns the paid call and the
    per-page idempotent slot; this orchestrator owns ordering, dedup, the stop
    policy, the aggregate manifest, evidence, comments and legacy scoring.
    """

    content_id = int(content["id"])
    platform = str(content["platform"])
    with connect(db_path) as connection:
        with transaction(connection):
            run = _get_or_create_run(
                connection,
                content_id=content_id,
                window_key=window_key,
                provider=provider,
                adapter_version=adapter_version,
                comment_cap=comment_cap,
            )
        if str(run["status"]) == "succeeded":
            return {
                "content_id": content_id,
                "status": "already_succeeded",
                "capture_run_id": int(run["id"]),
                "completion_kind": run["completion_kind"],
            }
        run_id = int(run["id"])
        existing_pages = _load_existing_pages(connection, run_id)

    accumulator = _Accumulator()
    cursor: Optional[Mapping[str, Any]] = None
    page_number = 0
    seen_cursor_shas: set[str] = set()

    # Resume: replay already-stored pages to rebuild the cursor + accumulator.
    for stored in existing_pages:
        page_number = int(stored["page_number"])
        seen_cursor_shas.add(str(stored["request_cursor_sha256"]))
        fetched = page_fetcher(page_number, _load_cursor(stored["request_cursor_json"]))
        _accumulate_page(accumulator, fetched.result.data)
        cursor = _load_cursor(stored["next_cursor_json"])
        if cursor is None:
            break

    completion_kind: Optional[str] = None
    stop_reason: Optional[str] = None
    pages_fetched = len(existing_pages)

    while pages_fetched < max_pages:
        sha = cursor_sha256(cursor)
        if sha in seen_cursor_shas and pages_fetched > 0:
            completion_kind = "provider_exhausted"
            stop_reason = "cursor_cycle_detected"
            break
        page_number += 1
        try:
            fetched = page_fetcher(page_number, cursor)
        except _BudgetExhausted as exc:
            stop_reason = f"budget_exhausted:{exc}"
            break
        result_data = fetched.result.data
        _accumulate_page(accumulator, result_data)
        counts = _page_counts(result_data)
        has_more = bool(result_data.get("has_more"))
        next_cursor = result_data.get("next_cursor_params") or result_data.get("next_cursor")
        with connect(db_path) as connection, transaction(connection):
            connection.execute(
                """
                INSERT INTO comment_capture_pages(
                    capture_run_id, page_number, request_cursor_json,
                    request_cursor_sha256, next_cursor_json, next_cursor_sha256,
                    fetch_slot_id, raw_response_id, has_more, provider_declared_total,
                    received_count, captured_distinct_count, valid_count,
                    stable_identity_count, captured_at
                ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                ON CONFLICT(capture_run_id, page_number) DO NOTHING
                """,
                (
                    run_id,
                    page_number,
                    _dump_cursor(cursor),
                    sha,
                    _dump_cursor(next_cursor),
                    cursor_sha256(next_cursor) if next_cursor is not None else None,
                    fetched.fetch_slot_id,
                    fetched.raw_response_id,
                    int(has_more),
                    accumulator.declared_total,
                    counts["received"],
                    counts["distinct"],
                    counts["valid"],
                    counts["stable_identity"],
                    now_utc(),
                ),
            )
        seen_cursor_shas.add(sha)
        pages_fetched += 1
        completion_kind = _decide_stop(
            accumulator=accumulator,
            has_more=has_more,
            next_cursor=next_cursor,
            comment_cap=comment_cap,
            coverage_target=coverage_target,
            pages_fetched=pages_fetched,
        )
        if completion_kind is not None:
            break
        cursor = next_cursor

    return _finalize_run(
        content=content,
        window_key=window_key,
        run_id=run_id,
        platform=platform,
        accumulator=accumulator,
        pages_fetched=pages_fetched,
        completion_kind=completion_kind,
        stop_reason=stop_reason,
        comment_cap=comment_cap,
        db_path=db_path,
    )


def _finalize_run(
    *,
    content: Mapping[str, Any],
    window_key: str,
    run_id: int,
    platform: str,
    accumulator: _Accumulator,
    pages_fetched: int,
    completion_kind: Optional[str],
    stop_reason: Optional[str],
    comment_cap: int,
    db_path: Path,
) -> Dict[str, Any]:
    content_id = int(content["id"])
    terminated = completion_kind in _COMPLETION_KINDS
    comments = list(accumulator.comments_by_identity.values())
    distinct_l1 = accumulator.distinct_l1()
    stable_l1 = accumulator.stable_identity_l1()

    with connect(db_path) as connection, transaction(connection):
        pages = connection.execute(
            """
            SELECT page_number, request_cursor_sha256, next_cursor_sha256,
                   raw_response_id, has_more, provider_declared_total,
                   received_count, captured_distinct_count, valid_count,
                   stable_identity_count
            FROM comment_capture_pages WHERE capture_run_id=? ORDER BY page_number
            """,
            (run_id,),
        ).fetchall()
        raw_shas = {
            int(row["id"]): str(row["sha256"])
            for row in connection.execute(
                "SELECT id, sha256 FROM provider_raw_responses WHERE id IN (%s)"
                % (",".join(str(int(r["raw_response_id"])) for r in pages) or "NULL")
            ).fetchall()
        } if pages else {}
        manifest = {
            "collection_version": COMMENT_COLLECTION_VERSION,
            "content_id": content_id,
            "window_key": window_key,
            "completion_kind": completion_kind,
            "declared_total": accumulator.declared_total,
            "distinct_l1": distinct_l1,
            "stable_identity_l1": stable_l1,
            "comment_cap": comment_cap,
            "pages": [
                {
                    "page_number": int(row["page_number"]),
                    "request_cursor_sha256": str(row["request_cursor_sha256"]),
                    "next_cursor_sha256": row["next_cursor_sha256"],
                    "raw_sha256": raw_shas.get(int(row["raw_response_id"])),
                    "received": int(row["received_count"]),
                    "valid": int(row["valid_count"]),
                    "distinct": int(row["captured_distinct_count"]),
                    "stable_identity": int(row["stable_identity_count"]),
                }
                for row in pages
            ],
        }
        local_path, aggregate_sha = _write_manifest(
            content_id=content_id, window_key=window_key, manifest=manifest
        )
        evidence_status = "available" if terminated else "partial"
        cursor = connection.execute(
            """
            INSERT INTO comment_evidence_versions(
                content_id, captured_at, iso_week, source, local_path,
                sha256, comment_count, capture_run_id, status, created_at
            ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            ON CONFLICT(content_id, iso_week, sha256) DO NOTHING
            """,
            (
                content_id,
                now_utc(),
                window_key,
                platform,
                local_path,
                aggregate_sha,
                distinct_l1,
                run_id,
                evidence_status,
                now_utc(),
            ),
        )
        evidence_id = cursor.lastrowid
        if evidence_id is None:
            row = connection.execute(
                """
                SELECT id FROM comment_evidence_versions
                WHERE content_id=? AND iso_week=? AND sha256=?
                """,
                (content_id, window_key, aggregate_sha),
            ).fetchone()
            evidence_id = int(row["id"])
        insert_comment_rows(
            connection,
            platform=platform,
            evidence_version_id=int(evidence_id),
            comments=comments,
            captured_at=now_utc(),
        )
        connection.execute(
            """
            UPDATE comment_capture_runs SET
                status=?, completion_kind=?, stop_reason=?,
                declared_total_count=?, captured_distinct_count=?,
                valid_comment_count=?, stable_identity_comment_count=?,
                page_count=?, completed_at=?, updated_at=?
            WHERE id=?
            """,
            (
                "succeeded" if terminated else "retryable_failed",
                completion_kind,
                stop_reason,
                accumulator.declared_total,
                distinct_l1,
                distinct_l1,
                stable_l1,
                pages_fetched,
                now_utc() if terminated else None,
                now_utc(),
                run_id,
            ),
        )

    upsert_comment_user_scores(
        content_id,
        int(evidence_id),
        legacy_user_score_rows(comments),
        db_path=db_path,
    )

    coverage = None
    if accumulator.declared_total:
        denom = max(int(accumulator.declared_total), distinct_l1)
        coverage = round(distinct_l1 * 100 / denom, 2) if denom else None
    elif not terminated:
        coverage = None
    elif completion_kind == "provider_exhausted":
        coverage = 100.0 if distinct_l1 else None

    return {
        "content_id": content_id,
        "status": "succeeded" if terminated else "incomplete",
        "capture_run_id": run_id,
        "evidence_version_id": int(evidence_id),
        "completion_kind": completion_kind,
        "stop_reason": stop_reason,
        "pages_fetched": pages_fetched,
        "distinct_l1_comments": distinct_l1,
        "stable_identity_l1_comments": stable_l1,
        "declared_total": accumulator.declared_total,
        "comment_collection_coverage_percentage": coverage,
    }


class _BudgetExhausted(RuntimeError):
    pass


def _dump_cursor(cursor: Optional[Mapping[str, Any]]) -> str:
    return canonical_json_bytes(cursor).decode("utf-8")


def _load_cursor(value: Any) -> Optional[Mapping[str, Any]]:
    if value in (None, "", b""):
        return None
    parsed = json.loads(value) if isinstance(value, (str, bytes)) else value
    return parsed if isinstance(parsed, Mapping) else None


def collect_first_level(comments: Sequence[Mapping[str, Any]]) -> List[Mapping[str, Any]]:
    return [c for c in comments if c.get("parent_comment_id") in (None, "")]
