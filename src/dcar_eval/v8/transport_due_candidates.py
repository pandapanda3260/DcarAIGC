"""Read-only enumeration of existing natural-due TikHub requests.

The primary campaign keeps its Douyin list-only contract; the legacy selector
also supports Xiaohongshu lists and frozen content/metrics/comments queues.
Terminal or unclaimed runs, history, old/future work, local replay, and missing
list references are ineligible. Broken durable ownership, malformed eligible
evidence, and unprovable natural parents fail closed with ``NaturalDueError``;
they are never silently removed from the result.

This module does not claim work, create new due work, reserve budget, or decide
whether an existing purchase can be repeated. Campaign membership and prior
request-use checks belong to the caller. Every result needs validation again
at the final transport boundary.
"""

from __future__ import annotations

import json
import sqlite3
from dataclasses import replace
from typing import Any

from . import providers, tikhub_scan, transport_natural_due as natural_due
from .account_roster import RosterError, require_active_member
from .durable_runs import DurableClaim
from .paid_identity import PaidIdentityError
from .provider_budget import PaidScope, freeze_scope


_INELIGIBLE = frozenset(
    {
        "natural_due_future",
        "natural_due_day_expired",
        "natural_due_not_pending",
        "natural_due_history_excluded",
    }
)


def _positive_int(value: Any, *, label: str) -> int:
    if isinstance(value, bool) or not isinstance(value, int) or value < 1:
        raise natural_due.NaturalDueError(
            "natural_due_source_invalid", f"{label} must be a positive integer"
        )
    return value


def _candidate(
    connection: sqlite3.Connection,
    row: sqlite3.Row,
    *,
    at: str,
    platform: str = "douyin",
) -> dict[str, Any] | None:
    try:
        details = natural_due._object(
            json.loads(row["details_json"]), label="run details"
        )
    except (TypeError, ValueError) as error:
        raise natural_due.NaturalDueError(
            "natural_due_source_invalid", "Natural-due source run is malformed"
        ) from error
    identity = natural_due._object(details.get("identity"), label="run identity")
    owner = natural_due._object(details.get("owner"), label="run owner")
    base = PaidScope(
        purpose=str(identity.get("purpose") or ""),
        scheduler_run_id=int(row["id"]),
        scheduler_attempt_id=_positive_int(owner.get("attempt_id"), label="attempt_id"),
    )
    checked, _row, details, identity, checkpoint = natural_due._load_source_run(
        connection, base
    )
    if (
        identity.get("purpose") == "history"
        or identity.get("platform") != platform
        or identity.get("contract_version") == tikhub_scan.LEGACY_CONTRACT_VERSION
    ):
        return None
    now = natural_due._timestamp(at, label="natural-due at")
    window_end = natural_due._same_current_day(
        identity.get("window_end"), now=now, label="scan window_end"
    )
    scope = replace(
        checked,
        activation_id=_positive_int(
            identity.get("activation_id"), label="activation_id"
        ),
        roster_snapshot_id=_positive_int(
            identity.get("roster_snapshot_id"), label="roster_snapshot_id"
        ),
        roster_snapshot_hash=str(identity.get("roster_snapshot_hash") or ""),
        business_day=natural_due._day(window_end),
    )
    active = natural_due._current_activation(
        connection, identity=identity, scope=scope, at=at
    )
    identity_id = _positive_int(identity.get("identity_id"), label="identity_id")
    try:
        member = require_active_member(
            connection,
            identity_id,
            int(active["roster_snapshot_id"]),
            str(active["roster_members_sha256"]),
            activation=active,
        )
    except RosterError as error:
        raise natural_due.NaturalDueError(
            "natural_due_nonmember", "Paid target is not a current active-roster member"
        ) from error
    scope = replace(
        scope,
        identity_id=identity_id,
        account_id=int(member["account_id"]),
        uid=str(member["uid"]),
        platform=str(member["platform"]),
        category="reconcile",
    )
    reference = (
        checkpoint.get("reference") if platform == "douyin" else str(member["uid"])
    )
    if reference in (None, ""):
        return None
    if not isinstance(reference, str) or (
        platform == "douyin" and not providers._valid_douyin_sec_user_id(reference)
    ):
        raise natural_due.NaturalDueError(
            "natural_due_reference_invalid", "Douyin list reference is malformed"
        )
    for field in ("generation", "page_number"):
        value = checkpoint.get(field)
        if isinstance(value, bool) or not isinstance(value, int) or value < 0:
            raise natural_due.NaturalDueError(
                "natural_due_cursor_invalid", "TikHub scan cursor generation is invalid"
            )
    try:
        cursor = tikhub_scan._cursor(
            platform, checkpoint.get("cursor"), initial=checkpoint["page_number"] == 0
        )
    except tikhub_scan.TikHubScanError as error:
        raise natural_due.NaturalDueError(
            "natural_due_cursor_invalid", "TikHub scan cursor is invalid"
        ) from error
    claim = DurableClaim(
        scheduler_run_id=int(row["id"]),
        attempt_id=_positive_int(owner.get("attempt_id"), label="attempt_id"),
        attempt_number=_positive_int(
            owner.get("attempt_number"), label="attempt_number"
        ),
        owner_token=str(scope.scheduler_owner_token),
        scan_id=str(scope.scheduler_scan_id),
    )
    window = tikhub_scan._page_key(claim, {**checkpoint, "cursor": cursor})
    try:
        request = providers._paid_request_identity(
            operation=f"{platform}_user_posts",
            platform=platform,
            subject=reference,
            params={
                "sec_user_id": reference,
                "max_cursor": cursor or 0,
                "count": 20,
                "sort_type": 0,
            }
            if platform == "douyin"
            else {"user_id": reference, "cursor": cursor or ""},
            cursor=cursor,
            due_bucket=window,
            request_window={
                "start": identity["window_start"],
                "end": identity["window_end"],
            },
        )
    except (KeyError, PaidIdentityError) as error:
        raise natural_due.NaturalDueError(
            "natural_due_request_mismatch", "Natural-due request cannot be derived"
        ) from error
    proof = natural_due.validate_natural_due_request(
        connection, scope=scope, request_identity=request, stage="discovery", at=at
    )
    return {
        "scope": scope,
        "request_identity": request,
        "stage": "discovery",
        "proof": proof,
    }


def list_primary_due_candidates(
    connection: sqlite3.Connection, *, at: str
) -> list[dict[str, Any]]:
    """Return deterministic full proofs for existing, currently due Douyin pages.

    Ordering uses real UTC schedule time, account UID, canonical cursor JSON,
    and paid request identity. Prior reservations/starts do not remove records;
    this preserves the caller's campaign member and failure denominators.
    """
    if not isinstance(connection, sqlite3.Connection):
        raise natural_due.NaturalDueError(
            "natural_due_connection_invalid", "A live SQLite connection is required"
        )
    natural_due._timestamp(at, label="natural-due at")
    candidates = []
    for row in connection.execute(
        "SELECT id,details_json FROM scheduler_runs "
        "WHERE job_id='tikhub_reconcile' AND status='running' ORDER BY id"
    ):
        try:
            candidate = _candidate(connection, row, at=at)
        except natural_due.NaturalDueError as error:
            if error.code in _INELIGIBLE:
                continue
            raise
        if candidate is not None:
            candidates.append(candidate)
    candidates.sort(
        key=lambda item: (
            natural_due._timestamp(
                item["proof"]["scheduled_for"], label="scheduled_for"
            ),
            item["proof"]["account_uid"],
            natural_due._canonical(item["proof"]["request_document"]["cursor"]),
            item["proof"]["paid_scope_identity"],
        )
    )
    return candidates


def _queue_candidates(
    connection: sqlite3.Connection, row: sqlite3.Row, *, operation: str, at: str
) -> list[dict[str, Any]]:
    """Derive requests from existing frozen queue items, never enqueue or claim."""
    from .pipeline import _candidate_work_targets
    from .source_routing import load_policy
    from .comment_paging import page_window_key

    try:
        details = natural_due._object(
            json.loads(row["details_json"]), label="run details"
        )
    except (TypeError, ValueError) as error:
        raise natural_due.NaturalDueError(
            "natural_due_source_invalid", "Queue details are malformed"
        ) from error
    owner = natural_due._object(details.get("owner"), label="run owner")
    stage = {
        "content_pipeline": "detail",
        "metrics_backfill": "metrics",
        "comments_refresh": "comments",
    }[row["job_id"]]
    base = PaidScope(
        purpose=stage,
        scheduler_run_id=int(row["id"]),
        scheduler_attempt_id=_positive_int(owner.get("attempt_id"), label="attempt_id"),
    )
    checked, _, _, identity, checkpoint = natural_due._load_source_run(connection, base)
    created = natural_due._same_current_day(
        identity.get("created_for"),
        now=natural_due._timestamp(at, label="at"),
        label="queue created_for",
    )
    scope = replace(
        checked,
        activation_id=_positive_int(
            identity.get("activation_id"), label="activation_id"
        ),
        roster_snapshot_id=_positive_int(
            identity.get("roster_snapshot_id"), label="roster_snapshot_id"
        ),
        roster_snapshot_hash=str(identity.get("roster_snapshot_hash") or ""),
        business_day=natural_due._day(created),
    )
    natural_due._current_activation(connection, identity=identity, scope=scope, at=at)
    pending, frozen_ids, items = (
        checkpoint.get("pending_ids"),
        identity.get("candidate_ids"),
        checkpoint.get("items"),
    )
    if (
        not isinstance(pending, list)
        or not isinstance(frozen_ids, list)
        or not isinstance(items, list)
        or any(type(cid) is not int or cid <= 0 for cid in pending)
        or len(pending) != len(set(pending))
    ):
        raise natural_due.NaturalDueError(
            "natural_due_source_invalid", "Frozen queue membership is malformed"
        )
    result = []
    for cid in pending:
        matches = [
            item for item in items if isinstance(item, dict) and item.get("id") == cid
        ]
        if cid not in frozen_ids or len(matches) != 1:
            raise natural_due.NaturalDueError(
                "natural_due_source_invalid",
                "Frozen queue item is missing or ambiguous",
            )
        frozen = matches[0]
        if frozen.get("historical") is True:
            continue
        content = connection.execute(
            "SELECT * FROM content_items WHERE id=?", (cid,)
        ).fetchone()
        if content is None:
            raise natural_due.NaturalDueError(
                "natural_due_target_missing", "Frozen content target no longer exists"
            )
        # Freeze only the already-held owner and member; this helper performs reads.
        target_scope = freeze_scope(
            connection, content_id=cid, account_id=None, stage=stage, scope=scope
        )
        try:
            targets = _candidate_work_targets(
                connection, str(row["job_id"]), {**dict(content), **frozen}, at=at
            )
        except (KeyError, TypeError, ValueError) as error:
            raise natural_due.NaturalDueError(
                "natural_due_source_invalid",
                "Frozen queue target or persisted continuation is malformed",
            ) from error
        for target in targets:
            if (
                target["operation"] != operation
                or target["stage"] != stage
                or target.get("local_replay")
            ):
                continue
            cursor = None
            window = str(target["window_key"])
            source_stage = stage
            if stage == "comments":
                try:
                    week, cursor, _ = natural_due._comment_state(
                        connection,
                        content_id=cid,
                        comment_as_of=str(frozen.get("comment_as_of") or ""),
                    )
                except natural_due.NaturalDueError as error:
                    if error.code in _INELIGIBLE:
                        continue
                    raise
                if page_window_key(week, cursor) != window:
                    continue
            elif stage == "metrics":
                rules = load_policy()["metric_supplement_groups"][content["platform"]]
                rule = next(
                    (rule for rule in rules if rule["name"] == target.get("group")),
                    None,
                )
                if rule is None:
                    raise natural_due.NaturalDueError(
                        "natural_due_source_invalid",
                        "Metric group is not in the current policy",
                    )
                source_stage = rule["stage"]
            content_key = str(content["platform_content_id"])
            if content["platform"] == "douyin":
                _, params = providers._douyin_request(
                    source_stage, content_key, cursor=cursor
                )
            else:
                _, params = providers._xhs_request(
                    source_stage,
                    content_key,
                    str(content["content_type"]),
                    cursor=cursor,
                )
            request = providers._paid_request_identity(
                operation=operation,
                platform=content["platform"],
                subject=content_key,
                params=params,
                cursor=cursor,
                due_bucket=window,
            )
            try:
                proof = natural_due.validate_natural_due_request(
                    connection,
                    scope=target_scope,
                    request_identity=request,
                    stage=stage,
                    at=at,
                )
            except natural_due.NaturalDueError as error:
                if error.code in _INELIGIBLE:
                    continue
                raise
            result.append(
                {
                    "scope": target_scope,
                    "request_identity": request,
                    "stage": stage,
                    "proof": proof,
                }
            )
    return result


def list_legacy_due_candidates(
    connection: sqlite3.Connection, *, operation: str, at: str
) -> list[dict[str, Any]]:
    """Enumerate one operation's pending legacy requests with full natural proofs.

    Membership does not filter prior billing: the issuer must reject a used
    fixed member, never silently replace it. Enumeration confers no permission.
    """
    if not isinstance(connection, sqlite3.Connection):
        raise natural_due.NaturalDueError(
            "natural_due_connection_invalid", "A live SQLite connection is required"
        )
    natural_due._timestamp(at, label="natural-due at")
    posts = {"douyin_user_posts": "douyin", "xiaohongshu_user_posts": "xiaohongshu"}
    queues = {
        "douyin_video_detail",
        "douyin_video_statistics",
        "douyin_video_comments",
        "xiaohongshu_note_detail",
        "xiaohongshu_note_statistics",
        "xiaohongshu_note_comments",
    }
    if operation not in posts and operation not in queues:
        raise natural_due.NaturalDueError(
            "natural_due_operation_unsupported",
            "Operation has no legacy enumeration contract",
        )
    jobs = (
        ("tikhub_reconcile",)
        if operation in posts
        else tuple(sorted(natural_due.QUEUE_JOBS))
    )
    result = []
    for row in connection.execute(
        f"SELECT id,job_id,details_json FROM scheduler_runs WHERE status='running' AND job_id IN ({','.join('?' for _ in jobs)}) ORDER BY id",
        jobs,
    ):
        try:
            if operation in posts:
                value = _candidate(connection, row, at=at, platform=posts[operation])
                found = [value] if value is not None else []
            else:
                found = _queue_candidates(connection, row, operation=operation, at=at)
        except natural_due.NaturalDueError as error:
            if error.code in _INELIGIBLE:
                continue
            raise
        result.extend(found)
    result.sort(
        key=lambda item: (
            natural_due._timestamp(
                item["proof"]["scheduled_for"], label="scheduled_for"
            ),
            item["proof"]["account_uid"],
            natural_due._canonical(item["proof"]["request_document"]["cursor"]),
            item["proof"]["paid_scope_identity"],
        )
    )
    return result
