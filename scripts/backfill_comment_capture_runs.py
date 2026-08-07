#!/usr/bin/env python3
"""Adopt legacy weekly comment raw responses into paged capture runs.

Dry-run by default. ``--apply`` is deliberately cache-only: it never creates a
provider budget, reads credentials, or calls a provider. Missing/corrupt/incomplete
cache stays retryable for the normal scheduler instead of being called complete.
"""

from __future__ import annotations

import argparse
import json
import re
import sys
from collections import Counter
from datetime import date
from pathlib import Path
from typing import Any, Sequence

PROJECT_ROOT = Path(__file__).resolve().parents[1]
PACKAGE_ROOT = PROJECT_ROOT / "src" / "dcar_eval"
if str(PACKAGE_ROOT) not in sys.path:
    sys.path.insert(0, str(PACKAGE_ROOT))

from v8.providers import capture_content_comments_live  # noqa: E402
from v8.storage import connect  # noqa: E402


_WEEK_KEY = re.compile(r"^(\d{4})-W(\d{2})$")


def _candidate_rows(db_path: Path, *, limit: int | None) -> list[dict[str, Any]]:
    limit_clause = " LIMIT ?" if limit is not None else ""
    parameters: list[Any] = []
    if limit is not None:
        if limit <= 0:
            raise ValueError("limit must be positive")
        parameters.append(limit)
    with connect(db_path) as connection:
        rows = connection.execute(
            """
            SELECT fs.content_id,fs.window_key,fs.status legacy_slot_status,
                   fs.provider legacy_provider,
                   EXISTS (
                       SELECT 1 FROM fetch_attempts fa
                       JOIN provider_raw_responses pr ON pr.fetch_attempt_id=fa.id
                       WHERE fa.slot_id=fs.id
                   ) has_raw,
                   r.status run_status
            FROM fetch_slots fs
            LEFT JOIN comment_capture_runs r
              ON r.content_id=fs.content_id AND r.window_key=fs.window_key
            WHERE fs.content_id IS NOT NULL AND fs.stage='comments'
              AND fs.window_key GLOB '[0-9][0-9][0-9][0-9]-W[0-9][0-9]'
              AND (r.id IS NULL OR r.status!='succeeded')
            ORDER BY fs.window_key,fs.content_id
            """
            + limit_clause,
            parameters,
        ).fetchall()
    return [dict(row) for row in rows]


def _ledger_snapshot(db_path: Path) -> dict[str, Any]:
    with connect(db_path) as connection:
        return {
            "provider_usage_rows": int(
                connection.execute("SELECT COUNT(*) FROM provider_usage").fetchone()[0]
            ),
            "provider_usage_amount": float(
                connection.execute(
                    "SELECT COALESCE(SUM(amount),0) FROM provider_usage"
                ).fetchone()[0]
            ),
            "fetch_attempt_rows": int(
                connection.execute("SELECT COUNT(*) FROM fetch_attempts").fetchone()[0]
            ),
            "raw_response_rows": int(
                connection.execute(
                    "SELECT COUNT(*) FROM provider_raw_responses"
                ).fetchone()[0]
            ),
            "budget_consumed_requests": int(
                connection.execute(
                    "SELECT COALESCE(SUM(consumed_requests),0) FROM provider_budget_batches"
                ).fetchone()[0]
            ),
            "budget_consumed_amount": float(
                connection.execute(
                    "SELECT COALESCE(SUM(consumed_amount),0) FROM provider_budget_batches"
                ).fetchone()[0]
            ),
        }


def _week_date(window_key: str) -> date:
    match = _WEEK_KEY.fullmatch(window_key)
    if match is None:
        raise ValueError(f"invalid weekly window key: {window_key}")
    return date.fromisocalendar(int(match.group(1)), int(match.group(2)), 1)


def main(argv: Sequence[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description=__doc__.split("\n", 1)[0])
    parser.add_argument(
        "--db",
        type=Path,
        default=PROJECT_ROOT / "app" / "data" / "dcar_insight.sqlite3",
    )
    parser.add_argument("--limit", type=int)
    parser.add_argument("--apply", action="store_true")
    args = parser.parse_args(argv)
    db_path = args.db.resolve()
    if not db_path.exists():
        raise SystemExit(f"database not found: {db_path}")

    candidates = _candidate_rows(db_path, limit=args.limit)
    summary: dict[str, Any] = {
        "command": "backfill-comment-capture-runs",
        "mode": "apply" if args.apply else "dry_run",
        "db": str(db_path),
        "candidates": len(candidates),
        "legacy": {
            "with_raw": sum(int(row["has_raw"]) for row in candidates),
            "without_raw": sum(not bool(row["has_raw"]) for row in candidates),
        },
    }
    if not args.apply:
        print(json.dumps(summary, ensure_ascii=False, indent=2, sort_keys=True))
        return 0

    before = _ledger_snapshot(db_path)
    statuses: Counter[str] = Counter()
    completions: Counter[str] = Counter()
    errors: list[dict[str, Any]] = []
    for row in candidates:
        try:
            result = capture_content_comments_live(
                int(row["content_id"]),
                as_of=_week_date(str(row["window_key"])),
                db_path=db_path,
                cache_only=True,
            )
            statuses[str(result["status"])] += 1
            completions[str(result.get("completion_kind") or "none")] += 1
        except Exception as exc:
            statuses["failed"] += 1
            if len(errors) < 20:
                errors.append(
                    {
                        "content_id": int(row["content_id"]),
                        "window_key": str(row["window_key"]),
                        "error_code": str(
                            getattr(exc, "error_code", type(exc).__name__)
                        ),
                        "message": str(exc)[:300],
                    }
                )
    after = _ledger_snapshot(db_path)
    if after != before:
        raise RuntimeError(
            "cache-only backfill changed provider ledger/raw state: "
            f"before={before}, after={after}"
        )
    summary.update(
        {
            "statuses": dict(sorted(statuses.items())),
            "completion_kinds": dict(sorted(completions.items())),
            "sample_errors": errors,
            "provider_state_unchanged": True,
            "provider_cost": 0.0,
        }
    )
    print(json.dumps(summary, ensure_ascii=False, indent=2, sort_keys=True))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
