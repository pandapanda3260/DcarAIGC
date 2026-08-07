#!/usr/bin/env python3
"""Run audience-classifier-v1 over interaction users in a fixed snapshot.

``automotive_user_rate`` 的分子 A 来自
``interaction_user_classification_versions``；这张表只有在分类器实际跑过之后
才有数据。本脚本把最近 ``--days`` 天内发布内容，或
``--all-contents`` 选中的全部内容，其在固定 90 天证据窗内的评论用户送入确定性规则分类器
（无付费调用、无网络请求），按证据指纹幂等写入版本行——重复执行安全。

Dry-run by default (no writes). ``--apply`` persists classification rows.

Operator flow:
    python3 scripts/run_audience_classifier.py --db app/data/dcar_insight.sqlite3
    python3 scripts/run_audience_classifier.py --db app/data/dcar_insight.sqlite3 --apply
然后重启 API 服务（概览是读取时计算的，无需重建前端）。
"""

from __future__ import annotations

import argparse
import json
import sys
from datetime import datetime, time, timedelta, timezone
from pathlib import Path
from typing import Sequence
from zoneinfo import ZoneInfo

PROJECT_ROOT = Path(__file__).resolve().parents[1]
PACKAGE_ROOT = PROJECT_ROOT / "src" / "dcar_eval"
if str(PACKAGE_ROOT) not in sys.path:
    sys.path.insert(0, str(PACKAGE_ROOT))

from v8.audience_classifier import (  # type: ignore[import-not-found,import-untyped]  # noqa: E402
    AUDIENCE_DEFINITION_VERSION,
    CLASSIFIER_VERSION,
    EVIDENCE_WINDOW_DAYS,
    classify_window,
)
from v8.storage import connect  # type: ignore[import-not-found,import-untyped]  # noqa: E402

SHANGHAI = ZoneInfo("Asia/Shanghai")


def _utc_text(value: datetime) -> str:
    return (
        value.astimezone(timezone.utc)
        .isoformat(timespec="seconds")
        .replace("+00:00", "Z")
    )


def _classified_per_platform(connection) -> list:
    return [
        dict(row)
        for row in connection.execute(
            """
            SELECT iu.platform, cls.label, COUNT(DISTINCT cls.interaction_user_id) users
            FROM interaction_user_classification_versions cls
            JOIN interaction_users iu ON iu.id=cls.interaction_user_id
            WHERE cls.audience_definition_version=? AND cls.classifier_version=?
            GROUP BY iu.platform, cls.label ORDER BY iu.platform, cls.label
            """,
            (AUDIENCE_DEFINITION_VERSION, CLASSIFIER_VERSION),
        )
    ]


def _overview_window_ends(now_utc_dt: datetime) -> list:
    """As-of snapshot ends the overview/report windows read against.

    audience_selectors 按"报告截止时的最新分类快照"取数：一个已封闭窗口
    （昨天/上周）只认 ``evidence_window_end <= 该窗口结束时间`` 的分类行，
    所以必须按每个窗口自己的结束时间各拍一次快照，按时间升序执行——
    证据未变化的用户在更早窗口已有快照（按证据指纹幂等），有新证据的
    用户会自然产生新的快照行。
    """

    now_local = now_utc_dt.astimezone(SHANGHAI)
    today = datetime.combine(now_local.date(), time.min, tzinfo=SHANGHAI)
    this_week = today - timedelta(days=today.weekday())
    ends = sorted(
        {
            (this_week - timedelta(days=7)).astimezone(timezone.utc),  # 上周起点（上上周末）
            this_week.astimezone(timezone.utc),  # 上周末 = 本周一 00:00
            (today - timedelta(days=1)).astimezone(timezone.utc),  # 前天末
            today.astimezone(timezone.utc),  # 昨天末 = 今天 00:00
            now_utc_dt,  # 实时（本周窗口）
        }
    )
    return [end for end in ends if end <= now_utc_dt]


def main(argv: Sequence[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description=__doc__.split("\n", 1)[0])
    parser.add_argument(
        "--db",
        type=Path,
        default=PROJECT_ROOT / "app" / "data" / "dcar_insight.sqlite3",
        help="sqlite database (default: app/data/dcar_insight.sqlite3)",
    )
    scope = parser.add_mutually_exclusive_group()
    scope.add_argument(
        "--days",
        type=int,
        default=30,
        help="classify users of contents published in the last N days (default 30)",
    )
    scope.add_argument(
        "--all-contents",
        action="store_true",
        help="classify users across every content published before the snapshot",
    )
    parser.add_argument(
        "--snapshot-end",
        help="fixed timezone-aware snapshot end; defaults to the current UTC time",
    )
    parser.add_argument(
        "--apply",
        action="store_true",
        help="persist classification rows (default: dry run, no writes)",
    )
    args = parser.parse_args(argv)
    if not args.db.exists():
        raise SystemExit(f"database not found: {args.db}")
    if int(args.days) <= 0:
        parser.error("--days must be positive")

    actual_now = datetime.now(timezone.utc)
    if args.snapshot_end:
        try:
            now = datetime.fromisoformat(args.snapshot_end.replace("Z", "+00:00"))
        except ValueError as exc:
            parser.error(f"invalid --snapshot-end: {exc}")
        if now.tzinfo is None:
            parser.error("--snapshot-end must include a timezone")
        now = now.astimezone(timezone.utc)
        if now > actual_now + timedelta(minutes=1):
            parser.error("--snapshot-end cannot be in the future")
    else:
        now = actual_now
    window_start = (
        None if args.all_contents
        else _utc_text(now - timedelta(days=int(args.days)))
    )

    connection = connect(args.db.resolve())
    try:
        if args.all_contents:
            content_rows = connection.execute(
                "SELECT id FROM content_items"
                " WHERE published_at IS NULL OR published_at < ?"
                " ORDER BY id",
                (_utc_text(now),),
            )
        else:
            content_rows = connection.execute(
                "SELECT id FROM content_items"
                " WHERE published_at >= ? AND published_at < ?"
                " ORDER BY id",
                (window_start, _utc_text(now)),
            )
        content_ids = [int(row["id"]) for row in content_rows]
        runs = []
        # A full-corpus backfill is a current retrospective snapshot, not a
        # reconstruction of yesterday/last week.  Newly captured evidence did
        # not exist at those historical report cutoffs, so emitting backdated
        # classification rows would be unusable and misleading.
        snapshot_ends = [now] if args.all_contents else _overview_window_ends(now)
        for end_dt in snapshot_ends:
            end_text = _utc_text(end_dt)
            summary = classify_window(
                connection,
                content_ids=content_ids,
                evidence_window_start=_utc_text(
                    end_dt - timedelta(days=EVIDENCE_WINDOW_DAYS)
                ),
                evidence_window_end=end_text,
                report_cutoff_at=end_text,
                persist=bool(args.apply),
            )
            runs.append(
                {
                    "evidence_window_end": end_text,
                    "total_users": summary["total_users"],
                    "label_counts": summary["label_counts"],
                }
            )
        result = {
            "command": "run-audience-classifier",
            "ok": True,
            "mode": "apply" if args.apply else "dry_run",
            "db": str(args.db),
            "content_scope": (
                "all_contents_as_of_snapshot" if args.all_contents else "recent_contents"
            ),
            "content_window_start": window_start,
            "snapshot_end": _utc_text(now),
            "content_count": len(content_ids),
            "snapshot_runs": runs,
        }
        if args.apply:
            result["stored_per_platform"] = _classified_per_platform(connection)
        print(json.dumps(result, ensure_ascii=False, sort_keys=True, indent=2))
        return 0
    finally:
        connection.close()


if __name__ == "__main__":
    raise SystemExit(main())
