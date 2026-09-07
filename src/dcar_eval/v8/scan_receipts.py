"""Read-only profile-day scan coverage, shared by reports and runtime health.

The denominator is the exact roster/eligible set frozen by that day's round,
never today's enabled accounts. Missing historical scopes remain unknown.
"""
from __future__ import annotations

import hashlib
import json
import sqlite3
from datetime import date, datetime, time, timedelta
from pathlib import Path
from typing import Any, Mapping
from zoneinfo import ZoneInfo

from . import durable_runs, raw_archive
from .artifact_paths import resolve
from .matrix_scan import _read_blob
from .raw_evidence import RawEvidenceError, read_raw_json
from .scan_terminals import (
    blocker_priority,
    coverage_decision,
    validate_terminal_summary,
)
from .source_routing import parse_time

BEIJING = ZoneInfo("Asia/Shanghai")
LEGACY_CONTRACT_VERSION = "matrix-first-coverage-v1"
MATRIX_FIRST_CONTRACT_VERSION = "matrix-first-coverage-v2"
CONTRACT_VERSION = "profile-day-coverage-v1"


def _discovery_window_start(end: datetime, days: int, scope: Mapping[str, Any]) -> datetime:
    """Interpret the scope frozen with this round, never today's environment."""
    start = end - timedelta(days=days)
    value = scope.get("automatic_from_date")
    if value is not None:
        first_day = date.fromisoformat(str(value))
        if first_day.isoformat() != value:
            raise ValueError("automatic_business_day_invalid")
        floor = datetime.combine(first_day, time.min, BEIJING)
        if floor >= end:
            raise ValueError("automatic_business_day_outside_report_period")
        start = max(start, floor)
    return start


TIKHUB_SCAN_V1 = "tikhub-account-scan-v1"
TIKHUB_SCAN_V2 = "tikhub-account-scan-v2"
TIKHUB_V2_MANIFEST_FIELDS = {
    "provider_has_more", "provider_next_cursor", "execution_next_cursor",
    "completion_reason", "range_start_proof",
}
TIKHUB_V2_ITEM_FIELDS = {
    "platform_content_id", "published_at", "is_pinned", "event_tuple",
}
TIKHUB_V2_ONLY_ITEM_FIELDS = {"published_at", "is_pinned", "event_tuple"}
TIKHUB_V2_CHECKPOINT_FIELDS = {
    "provider_next_cursor", "completion_reason", "qualifying_old_page_count",
}
BENIGN_SCAN_ERRORS = frozenset({"scan_not_complete_at_cutoff"})


def _same_json_value(left: Any, right: Any) -> bool:
    """Compare receipt values without Python's bool/int/float coercions."""
    try:
        return json.dumps(
            left, sort_keys=True, separators=(",", ":"), allow_nan=False
        ) == json.dumps(
            right, sort_keys=True, separators=(",", ":"), allow_nan=False
        )
    except (TypeError, ValueError):
        return False


def _supports_profiles(connection: sqlite3.Connection) -> bool:
    return connection.execute(
        "SELECT 1 FROM sqlite_master WHERE type='table' "
        "AND name='acquisition_profile_activations'"
    ).fetchone() is not None


def _roster_scope(
    connection: sqlite3.Connection, scope: Mapping[str, Any]
) -> tuple[dict[str, Any], list[int], Path]:
    roster_row = connection.execute("SELECT * FROM account_roster_snapshots WHERE id=?", (scope.get("roster_snapshot_id"),)).fetchone()
    if roster_row is None:
        raise ValueError("roster_scope_mismatch")
    roster = dict(roster_row)
    if roster["members_sha256"] != scope.get("roster_snapshot_hash") or parse_time(roster["accepted_at"]) > parse_time(scope["scheduled_at"]):
        raise ValueError("roster_scope_mismatch")
    source_family = str(roster.get("source_family") or "matrix")
    if source_family == "matrix":
        rows = connection.execute(
            "SELECT account_identity_id,platform,matrix_account_id "
            "FROM account_roster_members WHERE snapshot_id=? "
            "ORDER BY platform,matrix_account_id",
            (roster["id"],),
        ).fetchall()
        keys: list[Any] = [(row["platform"], row["matrix_account_id"]) for row in rows]
    elif source_family == "system":
        rows = connection.execute(
            "SELECT account_identity_id,platform,member_key "
            "FROM account_roster_members WHERE snapshot_id=? ORDER BY member_key",
            (roster["id"],),
        ).fetchall()
        keys = [row["member_key"] for row in rows]
        if any(not isinstance(key, str) or not key.strip() for key in keys):
            raise ValueError("roster_members_changed")
    else:
        raise ValueError("roster_scope_mismatch")
    from .account_roster import _json, _sha
    if len(rows) != roster["member_count"] or len(rows) != roster["declared_count"] or _sha(_json(keys)) != roster["members_sha256"]:
        raise ValueError("roster_members_changed")
    eligible = scope["eligible_identity_ids"]
    if not isinstance(eligible, list) or any(type(value) is not int for value in eligible) or sorted(set(eligible)) != eligible or not set(eligible) <= {row["account_identity_id"] for row in rows}:
        raise ValueError("roster_eligible_scope_mismatch")
    raw = resolve(roster["source_path"])
    if any(item.is_symlink() for item in (raw, *raw.parents)) or not raw.is_file() or hashlib.sha256(raw.read_bytes()).hexdigest() != roster["source_sha256"]:
        raise ValueError("roster_source_missing_or_changed")
    return roster, eligible, raw


def _profile_scope(
    connection: sqlite3.Connection, scope: Mapping[str, Any]
) -> dict[str, Any] | None:
    """Validate the exact acquisition epoch frozen by a profile-day anchor."""

    if not _supports_profiles(connection):
        return None
    from .profile_activations import activation_at, activation_by_id

    activation_id = scope.get("activation_id")
    profile_id = scope.get("profile_id")
    activation_sha256 = scope.get("activation_sha256")
    scheduled_at = scope.get("scheduled_at")
    if (
        type(activation_id) is not int
        or not isinstance(profile_id, str)
        or not isinstance(activation_sha256, str)
        or not isinstance(scheduled_at, str)
    ):
        raise ValueError("profile_day_activation_scope_unknown")
    frozen = activation_by_id(connection, activation_id)
    planned = activation_at(connection, scheduled_at)
    if (
        frozen.get("cancellation") is not None
        or planned is None
        or int(planned["activation_id"]) != activation_id
        or frozen["profile_id"] != profile_id
        or planned["profile_id"] != profile_id
        or frozen["activation_sha256"] != activation_sha256
        or frozen["roster_snapshot_id"] != scope.get("roster_snapshot_id")
        or frozen["roster_members_sha256"] != scope.get("roster_snapshot_hash")
    ):
        raise ValueError("profile_day_activation_scope_mismatch")
    return frozen


def _same_epoch(
    identity: Mapping[str, Any], scope: Mapping[str, Any], *, tikhub: bool
) -> bool:
    if "activation_id" not in scope:
        return True
    if (
        identity.get("activation_id") != scope.get("activation_id")
        or identity.get("profile_id") != scope.get("profile_id")
    ):
        return False
    return not tikhub or identity.get("activation_sha256") == scope.get(
        "activation_sha256"
    )


def verify_scan(connection: sqlite3.Connection, row: Mapping[str, Any], *, cutoff_at: str) -> dict[str, Any]:
    """Verify closed pagination and every disposition/raw hash in its chain."""
    details = json.loads(row["details_json"])
    if row["status"] != "succeeded" or not row["completed_at"] or parse_time(row["completed_at"]) > parse_time(cutoff_at):
        raise ValueError("scan_not_complete_at_cutoff")
    if details.get("contract_version") != durable_runs.CONTRACT_VERSION or details.get("complete") is not True:
        raise ValueError("scan_contract_mismatch")
    scope, cp = details["identity"], details["checkpoint"]
    if details["scan_id"] != durable_runs.scan_identity(str(row["job_id"]), scope):
        raise ValueError("scan_identity_changed")
    if (not cp.get("complete") or cp.get("pending_page") or cp.get("pending_raw")
            or cp.get("pending_materialization")):
        raise ValueError("scan_has_pending_page")
    provider = str(scope.get("provider", "")).lower()
    matrix = provider == "newrank_matrix"
    if provider not in {"newrank_matrix", "tikhub"}:
        raise ValueError("scan_provider_mismatch")
    tikhub_contract = None if matrix else scope.get("contract_version")
    if not matrix and tikhub_contract not in {TIKHUB_SCAN_V1, TIKHUB_SCAN_V2}:
        raise ValueError("scan_manifest_scope_mismatch")
    tikhub_v2 = tikhub_contract == TIKHUB_SCAN_V2
    if not matrix and not tikhub_v2 and TIKHUB_V2_CHECKPOINT_FIELDS & cp.keys():
        raise ValueError("scan_contract_mismatch")
    head = cp.get("last_manifest")
    if not isinstance(head, dict):
        raise ValueError("scan_manifest_missing")
    seen: set[str] = set()
    counts: dict[str, int] = {}
    references = []
    pages: list[dict[str, Any]] = []
    while head is not None:
        if head["sha256"] in seen:
            raise ValueError("scan_manifest_cycle")
        seen.add(head["sha256"])
        value = _read_blob(head)
        expected_contract = "matrix-scan-v1" if matrix else tikhub_contract
        # Matrix's public version is read from its authoritative module.
        if matrix:
            from .matrix_scan import CONTRACT_VERSION as matrix_contract_version

            expected_contract = matrix_contract_version
        if value.get("contract_version") != expected_contract or value.get("scan_id") != details["scan_id"]:
            raise ValueError("scan_manifest_scope_mismatch")
        if not matrix and not _same_json_value(value.get("scope"), scope):
            raise ValueError("scan_manifest_scope_mismatch")
        if not matrix and (
            (tikhub_v2 and not TIKHUB_V2_MANIFEST_FIELDS <= value.keys())
            or (not tikhub_v2 and TIKHUB_V2_MANIFEST_FIELDS & value.keys())
        ):
            raise ValueError("scan_manifest_scope_mismatch")
        dispositions = value["rows"] if matrix else value["items"]
        if not isinstance(dispositions, list) or any(not isinstance(item, dict) for item in dispositions):
            raise ValueError("scan_disposition_conservation_failed")
        if (not matrix and not tikhub_v2
                and any(TIKHUB_V2_ONLY_ITEM_FIELDS & item.keys() for item in dispositions)):
            raise ValueError("scan_manifest_scope_mismatch")
        count = value["row_count"] if matrix else value["raw_items"]
        local_counts = value["counts"]
        if (not isinstance(local_counts, dict)
                or any(type(number) is not int or number < 0 for number in local_counts.values())
                or type(count) is not int or count < 0 or count != len(dispositions)
                or count != sum(local_counts.values())):
            raise ValueError("scan_disposition_conservation_failed")
        for name, number in local_counts.items():
            if number != sum(item.get("disposition") == name for item in dispositions):
                raise ValueError("scan_disposition_conservation_failed")
            counts[name] = counts.get(name, 0) + number
        raw_id = value["raw_id"] if matrix else value["raw"]["raw_response_id"]
        raw_sha = value["raw_sha256"] if matrix else value["raw"]["sha256"]
        if type(raw_id) is not int:
            raise ValueError("scan_raw_identity_mismatch")
        raw = connection.execute("SELECT * FROM provider_raw_responses WHERE id=?", (raw_id,)).fetchone()
        if raw is None or raw["sha256"] != raw_sha or str(raw["provider"]).lower() != provider or parse_time(raw["captured_at"]) > parse_time(cutoff_at):
            raise ValueError("scan_raw_identity_mismatch")
        path = resolve(raw["local_path"])
        managed = "raw_blob_id" in raw.keys() and raw["raw_blob_id"] is not None
        if not managed and (any(item.is_symlink() for item in (path, *path.parents)) or not path.is_file()):
            raise ValueError("scan_raw_missing")
        try:
            if managed:
                raw_value = json.loads(raw_archive.read_response_entity(connection, raw_id))
            else:
                raw_value = read_raw_json(
                    path,
                    expected_stored_sha256=str(raw_sha),
                    expected_stored_size=int(raw["byte_size"]),
                )
        except FileNotFoundError as error:
            raise ValueError("scan_raw_missing") from error
        except (RawEvidenceError, TypeError, ValueError) as error:
            if isinstance(error, raw_archive.RawArchiveError) and str(error).startswith("raw_expired:"):
                raise ValueError("raw_expired: scan raw was intentionally retired; deep verification is unavailable") from error
            raise ValueError("scan_raw_hash_mismatch") from error
        if matrix:
            from .matrix_scan import _expected_query
            if (raw_value.get("scan_id") != details["scan_id"] or raw_value.get("error_code")
                    or raw_value.get("page_index") != value.get("page_index")
                    or raw_value.get("request_cursor") != value.get("request_cursor")
                    or raw_value.get("query") != _expected_query(scope, value["request_cursor"])):
                raise ValueError("scan_raw_scope_mismatch")
            payload = raw_value["response"]
            actual_rows = payload.get("data")
            actual_rows = json.loads(actual_rows) if isinstance(actual_rows, str) else actual_rows
            if not isinstance(actual_rows, list) or payload.get("code") != 0:
                raise ValueError("scan_raw_page_invalid")
            actual_next = actual_rows[-1].get("scrollId") if actual_rows and isinstance(actual_rows[-1], dict) else None
            terminal = not actual_rows
            if (value.get("complete") is not terminal or value.get("reason") is not None
                    or value.get("next_cursor") != actual_next
                    or (not terminal and (not isinstance(actual_next, list) or not actual_next))):
                raise ValueError("scan_terminal_page_mismatch")
        else:
            from .capture import StoredRawResponse
            from .tikhub_scan import _cursor, _digest, _page
            receipt = value["raw"]
            if type(receipt.get("slot_id")) is not int:
                raise ValueError("scan_raw_scope_mismatch")
            slot = connection.execute("SELECT * FROM fetch_slots WHERE id=?", (receipt["slot_id"],)).fetchone()
            attempt = connection.execute(
                "SELECT * FROM fetch_attempts WHERE id=?", (raw["fetch_attempt_id"],),
            ).fetchone()
            expected_key = f"scan:{details['scan_id']}:g{value['generation']}:p{value['page_number']}:{_digest(value['request_cursor'])}"
            if (slot is None or slot["status"] != "succeeded"
                    or not _same_json_value(slot["account_id"], scope["account_id"])
                    or slot["stage"] != "discovery" or slot["window_key"] != expected_key
                    or str(slot["provider"]).lower() != provider
                    or slot["adapter_version"] != tikhub_contract
                    or attempt is None or attempt["slot_id"] != slot["id"]
                    or receipt["window_key"] != expected_key
                    or not _same_json_value(raw["account_id"], scope["account_id"])
                    or raw["operation"] != ("douyin_user_posts" if scope["platform"] == "douyin" else "xiaohongshu_user_posts")):
                raise ValueError("scan_raw_scope_mismatch")
            stored = StoredRawResponse(slot_id=slot["id"], raw_response_id=raw["id"], provider=raw["provider"],
                operation=raw["operation"], value=raw_value, http_status=raw["http_status"],
                captured_at=raw["captured_at"], sha256=raw["sha256"], local_path=path)
            actual_rows, more, actual_next, _total = _page(stored, scope["platform"])
            if more:
                actual_next = _cursor(scope["platform"], actual_next)
            elif not tikhub_v2:
                actual_next = None
            terminal = not more
            if not tikhub_v2 and not _same_json_value(value["next_cursor"], actual_next):
                raise ValueError("scan_terminal_page_mismatch")
        if (len(actual_rows) != count
                or any(type(item.get("index")) is not int or item["index"] != index
                       for index, item in enumerate(dispositions))):
            raise ValueError("scan_raw_disposition_mismatch")
        pages.append(
            {
                "manifest": value,
                "terminal": terminal,
                "raw_rows": actual_rows,
                "provider_next_cursor": actual_next,
            }
        )
        references.append(
            {
                "manifest": head,
                "raw_id": raw_id,
                "raw_sha256": raw_sha,
                "raw_path": str(path),
                "raw_byte_size": int(raw["byte_size"]),
            }
        )
        head = value.get("previous")
    if (not _same_json_value(counts, cp.get("counts"))
            or not _same_json_value(
                sum(counts.values()), cp.get("raw_row_count" if matrix else "raw_items"),
            )):
        raise ValueError("scan_checkpoint_conservation_failed")
    if not pages:
        raise ValueError("scan_terminal_page_mismatch")
    if (matrix or not tikhub_v2) and (
        not pages[0]["terminal"] or any(page["terminal"] for page in pages[1:])
    ):
        raise ValueError("scan_terminal_page_mismatch")
    cursor = None if matrix else 0 if scope["platform"] == "douyin" else ""
    generation = 0
    qualifying_old_page_count = 0
    completion_reason = None
    provider_next_cursor = None
    for index, page in enumerate(reversed(pages)):
        value = page["manifest"]
        manifest_page_number = value["page_index" if matrix else "page_number"]
        if type(manifest_page_number) is not int or manifest_page_number != index:
            raise ValueError("scan_page_sequence_mismatch")
        if not matrix:
            next_generation = value["generation"]
            if type(next_generation) is not int or next_generation < generation:
                raise ValueError("scan_generation_mismatch")
            if next_generation > generation:
                cursor = 0 if scope["platform"] == "douyin" else ""
                qualifying_old_page_count = 0
            generation = next_generation
        if not _same_json_value(value["request_cursor"], cursor):
            raise ValueError("scan_cursor_sequence_mismatch")
        if not tikhub_v2:
            cursor = value["next_cursor"]
            continue
        from .tikhub_scan import _range_start_page_proof

        evidence, proof = _range_start_page_proof(
            str(scope["platform"]), page["raw_rows"],
            window_start=str(scope["window_start"]),
            prior_qualifying_old_page_count=qualifying_old_page_count,
        )
        for disposition, expected in zip(value["items"], evidence, strict=True):
            if (not TIKHUB_V2_ITEM_FIELDS <= disposition.keys()
                    or any(not _same_json_value(disposition[name], expected[name])
                           for name in expected)):
                raise ValueError("scan_raw_disposition_mismatch")
        if not _same_json_value(value.get("range_start_proof"), proof):
            raise ValueError("scan_range_start_proof_mismatch")
        qualifying_old_page_count = int(proof["qualifying_old_page_count"])
        provider_next_cursor = page["provider_next_cursor"]
        if value.get("provider_has_more") is not (not page["terminal"]):
            raise ValueError("scan_terminal_page_mismatch")
        if not _same_json_value(value.get("provider_next_cursor"), provider_next_cursor):
            raise ValueError("scan_terminal_page_mismatch")
        expected_reason = (
            "provider_exhausted"
            if page["terminal"]
            else "range_start_reached"
            if qualifying_old_page_count >= 2
            else None
        )
        expected_cursor = None if expected_reason else provider_next_cursor
        if (not _same_json_value(value.get("completion_reason"), expected_reason)
                or not _same_json_value(value.get("execution_next_cursor"), expected_cursor)
                or not _same_json_value(value.get("next_cursor"), expected_cursor)):
            raise ValueError("scan_terminal_page_mismatch")
        if expected_reason is not None and index != len(pages) - 1:
            raise ValueError("scan_terminal_page_mismatch")
        completion_reason = expected_reason
        cursor = expected_cursor
    checkpoint_page_number = cp.get("page_index" if matrix else "page_number")
    if (type(checkpoint_page_number) is not int or checkpoint_page_number != len(pages)
            or cp.get("next_cursor" if matrix else "cursor") is not None):
        raise ValueError("scan_checkpoint_page_mismatch")
    if not matrix and (type(cp.get("generation")) is not int or cp["generation"] != generation):
        raise ValueError("scan_generation_mismatch")
    if tikhub_v2 and (
        not TIKHUB_V2_CHECKPOINT_FIELDS <= cp.keys()
        or completion_reason not in {"provider_exhausted", "range_start_reached"}
        or not _same_json_value(cp["completion_reason"], completion_reason)
        or type(cp["qualifying_old_page_count"]) is not int
        or cp["qualifying_old_page_count"] != qualifying_old_page_count
        or not _same_json_value(cp["provider_next_cursor"], provider_next_cursor)
    ):
        raise ValueError("scan_checkpoint_page_mismatch")
    return {"run_id": row["id"], "scope": scope, "completed_at": row["completed_at"], "counts": counts, "references": references}


def verify_terminal_scan(
    connection: sqlite3.Connection,
    row: Mapping[str, Any],
    *,
    cutoff_at: str,
) -> dict[str, Any]:
    """Verify one immutable, accounted TikHub terminal-blocked receipt."""

    details = json.loads(row["details_json"])
    if (
        row["status"] != "failed"
        or not row["completed_at"]
        or parse_time(row["completed_at"]) > parse_time(cutoff_at)
        or details.get("contract_version") != durable_runs.CONTRACT_VERSION
        or details.get("complete") is not False
        or details.get("checkpoint", {}).get("complete") is not False
    ):
        raise ValueError("scan_terminal_receipt_invalid")
    scope = details.get("identity")
    if (
        not isinstance(scope, dict)
        or str(scope.get("provider", "")).lower() != "tikhub"
        or scope.get("contract_version") not in {TIKHUB_SCAN_V1, TIKHUB_SCAN_V2}
        or row["job_id"] != "tikhub_reconcile"
        or details.get("scan_id")
        != durable_runs.scan_identity(str(row["job_id"]), scope)
    ):
        raise ValueError("scan_terminal_scope_mismatch")
    summary = validate_terminal_summary(details.get("summary", {}))
    if summary["terminal_class"] == "success":
        raise ValueError("scan_terminal_receipt_invalid")
    attempts = connection.execute(
        "SELECT * FROM scheduler_run_attempts WHERE scheduler_run_id=? "
        "ORDER BY attempt_number",
        (row["id"],),
    ).fetchall()
    if (
        not attempts
        or attempts[-1]["status"] != "failed"
        or attempts[-1]["completed_at"] != row["completed_at"]
        or attempts[-1]["details_json"] != row["details_json"]
    ):
        raise ValueError("scan_terminal_attempt_mismatch")
    return {
        "run_id": row["id"],
        "scope": scope,
        "completed_at": row["completed_at"],
        "terminal_class": summary["terminal_class"],
        "accounted": summary["accounted"],
        "required": summary["required"],
        "publication_blocker": summary["publication_blocker"],
        "reason": summary["reason"],
        "references": [],
    }


def coverage(connection: sqlite3.Connection, *, period_start: str, period_end: str,
             cutoff_at: str) -> dict[str, Any]:
    start, end = date.fromisoformat(period_start), date.fromisoformat(period_end)
    if end < start:
        raise ValueError("invalid discovery period")
    runs = [dict(row) for row in connection.execute(
        "SELECT * FROM scheduler_runs WHERE job_id IN ('matrix_works_scan','tikhub_reconcile') "
        "AND julianday(started_at)<=julianday(?) ORDER BY id", (cutoff_at,))]
    round_rows = connection.execute(
        "SELECT * FROM scheduler_runs WHERE job_id='pipeline_round:tikhub_reconcile' "
        "AND julianday(started_at)<=julianday(?) ORDER BY id", (cutoff_at,),
    ).fetchall()
    rounds: dict[str, list[dict[str, Any]]] = {}
    for row in round_rows:
        value = json.loads(row["details_json"])
        if value.get("contract_version") == durable_runs.CONTRACT_VERSION:
            identity = value.get("identity", {})
            if (
                identity.get("registration_id") == "tikhub_reconcile"
                and identity.get("job_id") == "tikhub_reconcile"
                and identity.get("round_id") == "tikhub_reconcile:03:00"
            ):
                rounds.setdefault(str(identity.get("beijing_day")), []).append(
                    {"id": row["id"], **identity}
                )
    has_profiles = _supports_profiles(connection)
    contract_version = CONTRACT_VERSION if has_profiles else MATRIX_FIRST_CONTRACT_VERSION
    cache: dict[int, dict[str, Any] | None] = {}
    errors: dict[str, str] = {}
    days = []
    current = start
    while current <= end:
        following = current + timedelta(days=1)
        anchor_at = datetime.combine(following, time(3), BEIJING)
        candidates = []
        for value in rounds.get(following.isoformat(), []):
            try:
                scheduled_at = value.get("scheduled_at")
                if (
                    isinstance(scheduled_at, str)
                    and parse_time(scheduled_at) == anchor_at
                ):
                    candidates.append(value)
            except (TypeError, ValueError):
                continue
        scope = candidates[0] if len(candidates) == 1 else None
        day: dict[str, Any] = {
            "date": current.isoformat(),
            "known": False,
            "complete": False,
            "partial_publishable": False,
            "eligible_identity_ids": [],
            "covered_identity_ids": [],
            "succeeded_identity_ids": [],
            "blocked_identity_ids": [],
            "not_applicable_identity_ids": [],
            "accounted_identity_ids": [],
            "required_identity_ids": [],
            "matrix_run_ids": [],
            "tikhub_run_ids": [],
            "terminal_blockers": {},
        }
        if scope is None or not isinstance(scope.get("eligible_identity_ids"), list):
            day["reason"] = (
                "profile_day_anchor_ambiguous"
                if len(candidates) > 1
                else "historical_roster_scope_unknown"
            )
            days.append(day)
            current = following
            continue
        try:
            activation = _profile_scope(connection, scope)
            roster, eligible, raw = _roster_scope(connection, scope)
            upper = datetime.combine(following, time.min, BEIJING)
            matrix_start = _discovery_window_start(upper, 30, scope)
            tikhub_start = _discovery_window_start(upper, 7, scope)
            if activation is not None:
                from .profile_activations import PROFILE_FAMILIES

                if roster.get("source_family") != PROFILE_FAMILIES[activation["profile_id"]]:
                    raise ValueError("profile_day_roster_family_mismatch")
        except (ValueError, OSError, TypeError, KeyError) as error:
            day["reason"] = str(error)
            days.append(day)
            current = following
            continue
        profile_id = str(scope.get("profile_id") or "matrix_hybrid_v1")
        source_family = str(roster.get("source_family") or "matrix")
        matrix_required = not has_profiles or profile_id == "matrix_hybrid_v1"
        matrix_days = (upper - matrix_start).days
        matrix_expected = matrix_days * 2 if has_profiles and matrix_required else 2 if matrix_required else 0
        day.update(
            known=True,
            activation_id=scope.get("activation_id"),
            activation_sha256=scope.get("activation_sha256"),
            profile_id=profile_id,
            source_family=source_family,
            eligible_identity_ids=eligible,
            roster_snapshot_id=roster["id"],
            roster_snapshot_hash=roster["members_sha256"],
            round_run_id=scope["id"],
            anchor_scheduled_at=str(scope["scheduled_at"]),
            roster_source={"path": str(raw), "sha256": roster["source_sha256"]},
            matrix_expected_windows=matrix_expected,
            matrix_complete_windows=0,
            matrix_forbidden_run_ids=[],
        )
        if "automatic_from_date" in scope:
            day["automatic_from_date"] = scope["automatic_from_date"]
        lower = datetime.combine(current, time.min, BEIJING)
        upper = datetime.combine(following, time.min, BEIJING)
        platforms: set[str] = set()
        matrix_finished: set[tuple[str, datetime, datetime]] = set()
        matrix_expected_keys = {
            (platform, upper - timedelta(days=index + 1), upper - timedelta(days=index))
            for index in range(matrix_days)
            for platform in ("douyin", "xiaohongshu")
        }
        succeeded: set[int] = set()
        blocked: set[int] = set()
        not_applicable: set[int] = set()
        terminal_blockers: dict[int, dict[str, Any]] = {}
        for row in runs:
            details = json.loads(row["details_json"])
            identity = details.get("identity", {})
            if identity.get("roster_snapshot_id") != roster["id"] or identity.get("roster_snapshot_hash") != roster["members_sha256"]:
                continue
            matrix = identity.get("provider") == "newrank_matrix"
            if not _same_epoch(identity, scope, tikhub=not matrix):
                continue
            try:
                begin, finish = (identity["start_at"], identity["end_at"]) if matrix else (identity["window_start"], identity["window_end"])
                if matrix:
                    key = (identity.get("platform"), parse_time(begin), parse_time(finish))
                    if has_profiles:
                        if (
                            key not in matrix_expected_keys
                            or parse_time(identity["overall_start_at"]) != matrix_start
                            or parse_time(identity["overall_end_at"]) != upper
                        ):
                            continue
                    elif parse_time(begin) != lower or parse_time(finish) != upper:
                        continue
                elif (
                    parse_time(begin) != tikhub_start
                    or parse_time(finish) != upper
                ):
                    continue
                if row["id"] not in cache:
                    cache[row["id"]] = (
                        verify_terminal_scan(connection, row, cutoff_at=cutoff_at)
                        if row["status"] == "failed"
                        else verify_scan(connection, row, cutoff_at=cutoff_at)
                    )
                proof = cache[row["id"]]
                if proof is None:
                    continue
            except (ValueError, RuntimeError, OSError, KeyError, TypeError) as error:
                cache[row["id"]] = None
                errors[str(row["id"])] = str(error)
                continue
            if matrix and identity.get("kind") == "works":
                if matrix_required:
                    platforms.add(identity["platform"])
                    matrix_finished.add(key)
                    day["matrix_run_ids"].append(row["id"])
                else:
                    day["matrix_forbidden_run_ids"].append(row["id"])
            elif not matrix and identity.get("identity_id") in eligible:
                identity_id = int(identity["identity_id"])
                terminal_class = proof.get("terminal_class")
                if terminal_class is None:
                    succeeded.add(identity_id)
                    blocked.discard(identity_id)
                    not_applicable.discard(identity_id)
                    terminal_blockers.pop(identity_id, None)
                elif identity_id not in succeeded and terminal_class == "not_applicable":
                    not_applicable.add(identity_id)
                    blocked.discard(identity_id)
                    terminal_blockers.pop(identity_id, None)
                elif identity_id not in succeeded and identity_id not in not_applicable:
                    blocked.add(identity_id)
                    candidate = {
                        "terminal_class": terminal_class,
                        "reason": proof["reason"],
                        "publication_blocker": proof["publication_blocker"],
                        "scheduler_run_id": row["id"],
                    }
                    previous = terminal_blockers.get(identity_id)
                    proofs = (
                        list(previous.get("proofs", []))
                        if isinstance(previous, dict)
                        else []
                    )
                    proofs.append(candidate)
                    selected = max(
                        proofs,
                        key=lambda value: (
                            blocker_priority(str(value["terminal_class"])),
                            int(value["scheduler_run_id"]),
                        ),
                    )
                    terminal_blockers[identity_id] = {
                        **selected,
                        "terminal_classes": sorted(
                            {str(value["terminal_class"]) for value in proofs}
                        ),
                        "scheduler_run_ids": sorted(
                            {int(value["scheduler_run_id"]) for value in proofs}
                        ),
                        "publication_blocker": any(
                            bool(value["publication_blocker"]) for value in proofs
                        ),
                        "proofs": proofs,
                    }
                day["tikhub_run_ids"].append(row["id"])
        required = set(eligible) - not_applicable
        accounted = succeeded | blocked | not_applicable
        matrix_complete = (
            matrix_finished == matrix_expected_keys
            if has_profiles and matrix_required
            else platforms == {"douyin", "xiaohongshu"}
            if matrix_required
            else not day["matrix_forbidden_run_ids"]
        )
        day["matrix_complete_windows"] = (
            len(matrix_finished) if has_profiles and matrix_required else len(platforms)
        )
        day["prerequisite"] = {
            "profile_id": profile_id,
            "source_family": source_family,
            "matrix_required": matrix_required,
            "matrix_forbidden": not matrix_required,
            "matrix_expected_windows": matrix_expected,
            "matrix_complete_windows": day["matrix_complete_windows"],
            "matrix_forbidden_run_ids": list(day["matrix_forbidden_run_ids"]),
            "complete": matrix_complete,
        }
        formula = coverage_decision(
            scope_total=len(eligible),
            succeeded=len(succeeded & required),
            blocked=len(blocked),
            not_applicable=len(not_applicable),
            blocker_classes=frozenset(
                terminal_class
                for value in terminal_blockers.values()
                for terminal_class in value["terminal_classes"]
            ),
            prerequisite_complete=matrix_complete,
        )
        day.update(
            covered_identity_ids=sorted(succeeded),
            succeeded_identity_ids=sorted(succeeded),
            blocked_identity_ids=sorted(blocked),
            not_applicable_identity_ids=sorted(not_applicable),
            accounted_identity_ids=sorted(accounted),
            required_identity_ids=sorted(required),
            terminal_blockers={str(key): value for key, value in terminal_blockers.items()},
            success_percentage=formula["success_percentage"],
            accounted_percentage=formula["accounted_percentage"],
        )
        day["complete"] = formula["complete"]
        day["partial_publishable"] = (
            formula["partial_publishable"] and not day["complete"]
        )
        day["reason"] = (
            ""
            if day["complete"]
            else "matrix_evidence_forbidden"
            if day["matrix_forbidden_run_ids"]
            else "terminal_blocked_partial_publishable"
            if day["partial_publishable"]
            else "scan_pagination_or_provider_gap"
        )
        days.append(day)
        current = following
    unknown = [item["date"] for item in days if not item["known"]]
    gaps = [item["date"] for item in days if item["known"] and not item["complete"]]
    scope_total = sum(len(item["eligible_identity_ids"]) for item in days)
    succeeded_count = sum(len(item["succeeded_identity_ids"]) for item in days)
    blocked_count = sum(len(item["blocked_identity_ids"]) for item in days)
    not_applicable_count = sum(
        len(item["not_applicable_identity_ids"]) for item in days
    )
    accounted_count = sum(len(item["accounted_identity_ids"]) for item in days)
    required_count = sum(len(item["required_identity_ids"]) for item in days)
    complete = not unknown and not gaps
    partial_publishable = not complete and not unknown and all(
        item["complete"] or item["partial_publishable"] for item in days
    )
    percentage = (
        round(100 * succeeded_count / required_count, 2)
        if required_count and not unknown
        else 100.0
        if scope_total and not unknown
        else None
    )
    detail = {"status": "unknown" if unknown else "not_applicable" if not scope_total else "available" if percentage is not None and percentage >= 90 else "below_threshold",
              "covered_identity_occurrence_count": succeeded_count, "eligible_identity_occurrence_count": scope_total,
              "succeeded_identity_occurrence_count": succeeded_count,
              "blocked_identity_occurrence_count": blocked_count,
              "not_applicable_identity_occurrence_count": not_applicable_count,
              "accounted_identity_occurrence_count": accounted_count,
              "required_identity_occurrence_count": required_count,
              "accounted_percentage": round(100 * accounted_count / scope_total, 2) if scope_total and not unknown else 100.0 if not unknown else None,
              "observed_occurrence_count": sum(item["known"] for item in days), "expected_occurrence_count": len(days),
              "percentage": percentage,
              "eligible_basis": (
                  "frozen_profile_roster_identity_occurrences"
                  if has_profiles
                  else "frozen_matrix_roster_identity_occurrences"
              ), "complete": complete,
              "partial_publishable": partial_publishable,
              "missing_occurrence_dates": unknown + gaps, "roster_validation_failures": len(unknown),
              "success_rule": contract_version, "reason": "已验证完整空名册，无适用采集账号" if complete and not scope_total else "" if complete else "全部义务已终态且满足部分发布门槛" if partial_publishable else "冻结名册或来源分页证据不完整"}
    observation = {"status": "complete" if complete else "partial_publishable" if partial_publishable else "incomplete", "capture_observation_start_date": None,
                   "expected_dates": [item["date"] for item in days], "legacy_unobserved_dates": unknown,
                   "pipeline_gap_dates": gaps, "zero_content_dates": []}
    return {"contract_version": contract_version, "cutoff_at": cutoff_at, "days": days,
            "discovery_coverage": detail, "pipeline_observation": observation,
            "complete": complete, "partial_publishable": partial_publishable,
            "roster_evidence_valid": not unknown,
            "scan_traceable": (
                (complete or partial_publishable)
                and all(error in BENIGN_SCAN_ERRORS for error in errors.values())
            ),
            "scan_errors": errors,
            "scan_references": [value for value in cache.values() if value is not None]}


def runtime_coverage(connection: sqlite3.Connection, *, at: str) -> dict[str, Any]:
    """Current profile-day: exact prerequisites plus the seven-day roster scan.

    No completed child flag, empty processing queue, or today's enabled set can
    substitute for the accepted scope and verified raw pagination receipts.
    """
    end = datetime.combine(parse_time(at).astimezone(BEIJING).date(), time.min, BEIJING)
    yesterday = (end.date() - timedelta(days=1)).isoformat()
    evidence = coverage(connection, period_start=yesterday, period_end=yesterday, cutoff_at=at)
    day = evidence["days"][0]
    has_profiles = _supports_profiles(connection)
    result: dict[str, Any] = {
        "contract_version": evidence["contract_version"],
        "business_day": yesterday,
        "activation_id": day.get("activation_id"),
        "activation_sha256": day.get("activation_sha256"),
        "profile_id": day.get("profile_id"),
        "source_family": day.get("source_family"),
        "matrix_expected_windows": day.get("matrix_expected_windows", 60),
        "matrix_complete_windows": day.get("matrix_complete_windows", 0),
        "matrix_forbidden_run_ids": list(day.get("matrix_forbidden_run_ids", [])),
        "tikhub_expected_members": len(day["eligible_identity_ids"]) if day["known"] else None,
        "tikhub_complete_members": len(day["covered_identity_ids"]) if day["known"] else 0,
        "tikhub_succeeded_members": len(day["succeeded_identity_ids"]) if day["known"] else 0,
        "tikhub_blocked_members": len(day["blocked_identity_ids"]) if day["known"] else 0,
        "tikhub_not_applicable_members": len(day["not_applicable_identity_ids"]) if day["known"] else 0,
        "tikhub_accounted_members": len(day["accounted_identity_ids"]) if day["known"] else 0,
        "tikhub_required_members": len(day["required_identity_ids"]) if day["known"] else None,
        "status": "incomplete" if day["known"] else "unknown", "complete": False,
        "partial_publishable": False,
        "reason": day.get("reason", ""), "roster_snapshot_id": day.get("roster_snapshot_id"),
        "roster_snapshot_hash": day.get("roster_snapshot_hash"),
        "round_run_id": day.get("round_run_id"),
        "anchor_scheduled_at": day.get("anchor_scheduled_at"),
        "matrix_run_ids": list(day.get("matrix_run_ids", [])),
        "tikhub_run_ids": list(day.get("tikhub_run_ids", [])),
        "required_scan_run_ids": sorted(
            set(day.get("matrix_run_ids", [])) | set(day.get("tikhub_run_ids", []))
        ),
        "days": [day],
        "scan_errors": dict(evidence["scan_errors"]),
    }
    if not has_profiles:
        matrix_start = _discovery_window_start(end, 30, day)
        matrix_days = (end - matrix_start).days
        result["matrix_expected_windows"] = matrix_days * 2
        result["matrix_complete_windows"] = 0
        if not day["known"]:
            return result
        cache = {
            proof["run_id"]: proof for proof in evidence["scan_references"]
        }
        expected = {
            (
                platform,
                end - timedelta(days=index + 1),
                end - timedelta(days=index),
            )
            for index in range(matrix_days)
            for platform in ("douyin", "xiaohongshu")
        }
        finished: set[tuple[str, datetime, datetime]] = set()
        matrix_run_ids: list[int] = []
        for record in connection.execute(
            "SELECT * FROM scheduler_runs WHERE job_id='matrix_works_scan' "
            "AND julianday(started_at)<=julianday(?) ORDER BY id",
            (at,),
        ):
            row = dict(record)
            identity = json.loads(row["details_json"]).get("identity", {})
            if (
                identity.get("roster_snapshot_id") != day["roster_snapshot_id"]
                or identity.get("roster_snapshot_hash")
                != day["roster_snapshot_hash"]
            ):
                continue
            try:
                if (
                    identity.get("provider") != "newrank_matrix"
                    or identity.get("kind") != "works"
                    or parse_time(identity["overall_start_at"])
                    != matrix_start
                    or parse_time(identity["overall_end_at"]) != end
                ):
                    continue
                key = (
                    identity["platform"],
                    parse_time(identity["start_at"]),
                    parse_time(identity["end_at"]),
                )
                if key not in expected:
                    continue
                if row["id"] not in cache:
                    cache[row["id"]] = verify_scan(
                        connection, row, cutoff_at=at
                    )
                finished.add(key)
                matrix_run_ids.append(int(row["id"]))
            except (ValueError, RuntimeError, OSError, KeyError, TypeError) as error:
                result["scan_errors"][str(row["id"])] = str(error)
        result["matrix_complete_windows"] = len(finished)
        result["matrix_run_ids"] = matrix_run_ids
        result["required_scan_run_ids"] = sorted(
            set(matrix_run_ids) | set(result["tikhub_run_ids"])
        )
        result["complete"] = expected == finished and set(
            day["required_identity_ids"]
        ) <= set(day["succeeded_identity_ids"])
        result["partial_publishable"] = (
            expected == finished and day["partial_publishable"]
        )
        result["status"] = (
            "complete"
            if result["complete"]
            else "partial_publishable"
            if result["partial_publishable"]
            else "incomplete"
        )
        result["reason"] = (
            ""
            if result["complete"]
            else "terminal_blocked_partial_publishable"
            if result["partial_publishable"]
            else "scan_pagination_or_provider_gap"
        )
        return result
    if not day["known"]:
        return result
    result["complete"] = bool(day["complete"])
    result["partial_publishable"] = bool(day["partial_publishable"])
    result["status"] = (
        "complete"
        if result["complete"]
        else "partial_publishable"
        if result["partial_publishable"]
        else "incomplete"
    )
    return result
