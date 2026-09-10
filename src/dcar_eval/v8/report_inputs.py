"""Versioned, immutable report input snapshots in the existing task event log.

First-time snapshots exclude paused accounts without imposing today's roster
on enabled historical contents. Existing snapshots remain immutable. Unknown
historical dimensions stay unknown; retries never widen an empty frozen scope.
"""
from __future__ import annotations

import copy
import hashlib
import json
import sqlite3
from datetime import date, timedelta
from typing import Any, Mapping

from .account_classification import classification_sql, classification_updated_at_sql
from .content_scope import canonical_content_predicate
from .source_routing import parse_time
from .statistics_scope import content_statistics_scope_sql
from .storage import now_utc

SCOPE_EVENT = "report_scope_v1"
INPUT_EVENT = "report_inputs_v1"
CONTRACT_VERSION = "report-inputs-v1"
ACCOUNT_CLASSIFICATION_VERSION = "account-classification-v2"
PROFILE_DAY_SCAN_INPUT_CONTRACT = "report-profile-day-scan-inputs-v1"
PROFILE_DAY_PERIOD_COVERAGE_CONTRACT = "profile-day-coverage-period-v1"


class FrozenInputError(RuntimeError):
    pass


def canonical(value: Any) -> str:
    # SQLite IDs are often integer mapping keys in Python, but JSON object keys
    # are strings. Normalize before sorting so 2/10 survive a disk round trip.
    normalized = json.loads(json.dumps(value, ensure_ascii=False, allow_nan=False))
    return json.dumps(normalized, ensure_ascii=False, sort_keys=True, separators=(",", ":"), allow_nan=False)


def digest(value: Any) -> str:
    return hashlib.sha256(canonical(value).encode()).hexdigest()


def _sha256(value: Any) -> bool:
    return (
        isinstance(value, str)
        and len(value) == 64
        and all(character in "0123456789abcdef" for character in value)
    )


def compact_profile_day_scan_inputs(
    scans: Mapping[str, Any],
    *,
    period_start: str,
    period_end: str,
) -> dict[str, Any]:
    """Freeze the exact native day-receipt bindings without raw scan proofs."""

    if scans.get("contract_version") != PROFILE_DAY_PERIOD_COVERAGE_CONTRACT:
        raise FrozenInputError("profile-day period receipt contract is invalid")
    try:
        current = date.fromisoformat(period_start)
        end = date.fromisoformat(period_end)
    except (TypeError, ValueError) as error:
        raise FrozenInputError("profile-day receipt period is invalid") from error
    if end < current:
        raise FrozenInputError("profile-day receipt period is invalid")
    expected_dates: list[str] = []
    while current <= end:
        expected_dates.append(current.isoformat())
        current += timedelta(days=1)

    days = scans.get("days")
    references = scans.get("scan_references")
    if not isinstance(days, list) or not isinstance(references, list):
        raise FrozenInputError("profile-day period receipt references are invalid")
    days_by_date: dict[str, Mapping[str, Any]] = {}
    for day in days:
        if not isinstance(day, Mapping) or not isinstance(day.get("date"), str):
            raise FrozenInputError("profile-day period receipt day is invalid")
        business_day = str(day["date"])
        if business_day in days_by_date:
            raise FrozenInputError("profile-day period receipt day is duplicated")
        days_by_date[business_day] = day
    if list(days_by_date) != expected_dates:
        raise FrozenInputError("profile-day period receipt dates do not match the report")

    compact_references: list[dict[str, Any]] = []
    referenced_days: set[str] = set()
    for reference in references:
        if not isinstance(reference, Mapping):
            raise FrozenInputError("profile-day receipt reference is invalid")
        business_day = str(reference.get("business_day") or "")
        if business_day not in days_by_date or business_day in referenced_days:
            raise FrozenInputError("profile-day receipt reference scope is invalid")
        referenced_days.add(business_day)
        day = days_by_date[business_day]
        if day.get("known") is not True:
            raise FrozenInputError(
                "profile-day receipt reference points to an unknown day"
            )
        for field in ("run_id", "attempt_id", "sequence"):
            value = reference.get(field)
            if type(value) is not int or value < 1:
                raise FrozenInputError(
                    "profile-day receipt reference identity is invalid"
                )
        for field in ("activation_id", "roster_snapshot_id"):
            value = day.get(field)
            if type(value) is not int or value < 1:
                raise FrozenInputError(
                    "profile-day receipt activation or roster is invalid"
                )
        if not all(
            isinstance(day.get(field), str) and str(day[field]).strip()
            for field in ("profile_id", "source_family")
        ) or not all(
            _sha256(day.get(field))
            for field in ("activation_sha256", "roster_snapshot_hash")
        ):
            raise FrozenInputError(
                "profile-day receipt activation or roster is invalid"
            )
        receipt_sha256 = reference.get("self_sha256")
        if not _sha256(receipt_sha256):
            raise FrozenInputError("profile-day receipt reference hash is invalid")
        compact_references.append(
            {
                "business_day": business_day,
                "run_id": reference["run_id"],
                "attempt_id": reference["attempt_id"],
                "sequence": reference["sequence"],
                "self_sha256": receipt_sha256,
                "activation_id": day["activation_id"],
                "profile_id": day["profile_id"],
                "activation_sha256": day["activation_sha256"],
                "source_family": day["source_family"],
                "roster_snapshot_id": day["roster_snapshot_id"],
                "roster_snapshot_hash": day["roster_snapshot_hash"],
            }
        )
    known_days = {
        business_day
        for business_day, day in days_by_date.items()
        if day.get("known") is True
    }
    if referenced_days != known_days:
        raise FrozenInputError(
            "profile-day receipt references do not cover every known day"
        )
    compact_references.sort(key=lambda value: str(value["business_day"]))
    binding: dict[str, Any] = {
        "contract_version": PROFILE_DAY_SCAN_INPUT_CONTRACT,
        "coverage_contract_version": scans["contract_version"],
        "period_start": period_start,
        "period_end": period_end,
        "cutoff_at": scans.get("cutoff_at"),
        "receipt_references": compact_references,
        "receipt_references_sha256": digest(compact_references),
        "period_coverage_sha256": digest(scans),
    }
    binding["self_sha256"] = digest(binding)
    return binding


def load_event(connection: sqlite3.Connection, task_id: str, kind: str) -> dict[str, Any] | None:
    rows = connection.execute("SELECT id,payload_json FROM task_events WHERE task_id=? AND event_type=? ORDER BY id",
                              (task_id, kind)).fetchall()
    if not rows:
        return None
    if len(rows) != 1:
        raise FrozenInputError("multiple report input freezes")
    event = json.loads(rows[0]["payload_json"])
    if event.get("contract_version") != CONTRACT_VERSION or event.get("sha256") != digest(event.get("payload")):
        raise FrozenInputError("report input freeze digest mismatch")
    return {**event, "event_id": rows[0]["id"]}


def _store(connection: sqlite3.Connection, task_id: str, kind: str, payload: Mapping[str, Any]) -> dict[str, Any]:
    existing = load_event(connection, task_id, kind)
    if existing is not None:
        return existing
    # Return exactly the representation we persist, including string JSON keys.
    # First generation and retry must expose identical frozen input references.
    # Preserve deliberate presentation order (for example seven channel
    # metrics); canonical sorting is for the digest, not the stored payload.
    normalized = json.loads(json.dumps(payload, ensure_ascii=False, allow_nan=False))
    value = {"contract_version": CONTRACT_VERSION, "payload": normalized, "sha256": digest(normalized)}
    inserted = connection.execute(
        "INSERT INTO task_events(task_id,event_type,message,payload_json,created_at) VALUES (?,?,?,?,?)",
        (task_id, kind, "冻结报告范围" if kind == SCOPE_EVENT else "冻结报告全部输入",
         json.dumps(value, ensure_ascii=False, separators=(",", ":"), allow_nan=False), now_utc()),
    )
    return {**value, "event_id": inserted.lastrowid}


def freeze_scope(connection: sqlite3.Connection, task: Mapping[str, Any], *, start_at: str,
                 end_at: str, cutoff_at: str) -> dict[str, Any]:
    existing = load_event(connection, str(task["id"]), SCOPE_EVENT)
    if existing is not None:
        if existing["payload"]["cutoff_at"] != cutoff_at:
            raise FrozenInputError("report scope cutoff changed")
        return existing
    rows = connection.execute(
        f"SELECT c.*,{classification_sql(connection, 'account_group')} account_group,"
        f"{classification_sql(connection, 'business_direction')} business_direction,"
        f"{classification_updated_at_sql(connection)} classification_updated_at,"
        "a.created_at account_created_at,a.updated_at account_updated_at "
        "FROM content_items c LEFT JOIN accounts a ON a.id=c.account_id "
        "WHERE julianday(c.published_at)>=julianday(?) AND julianday(c.published_at)<julianday(?) "
        "AND julianday(c.imported_at)<=julianday(?) AND julianday(c.created_at)<=julianday(?) "
        f"AND {canonical_content_predicate(connection, knowledge_at=cutoff_at)} "
        f"AND {content_statistics_scope_sql(connection=connection)} "
        "ORDER BY c.published_at,c.id", (start_at, end_at, cutoff_at, cutoff_at),
    ).fetchall()
    contents = []
    unknown: dict[str, list[str]] = {}
    for row in rows:
        value = dict(row)
        reasons = []
        if parse_time(value["updated_at"]) > parse_time(cutoff_at):
            # No temporal title/body/direction table exists. Do not label a
            # current edit as the old cutoff's exact text or dimensions.
            reasons.append("content_changed_after_cutoff")
            value.update(title="", body="", manual_content_direction=None, evaluation_content_direction=None)
        if value["account_id"] is not None and (not value["account_created_at"]
                or parse_time(value["account_created_at"]) > parse_time(cutoff_at)
                or parse_time(value["account_updated_at"]) > parse_time(cutoff_at)
                or (value["classification_updated_at"] and
                    parse_time(value["classification_updated_at"]) > parse_time(cutoff_at))):
            reasons.append("account_dimension_unreconstructable")
            value.update(account_id=None, account_group="unknown", business_direction="unknown")
        # Content imports may still carry legacy source metadata; new snapshots
        # never turn that historical label into the current account taxonomy.
        value.pop("legacy_account_type", None)
        if reasons:
            unknown[str(value["id"])] = reasons
            value.update(account_group="unknown", business_direction="unknown")
        contents.append(value)
    for value in contents:
        connection.execute("INSERT INTO task_contents(task_id,content_id,inclusion_status,reason) "
                           "VALUES (?,?,'included','冻结截止前已入库且发布时间位于区间') "
                           "ON CONFLICT(task_id,content_id) DO NOTHING", (task["id"], value["id"]))
    # Keep the historical missing-boundary audit, bounded by the same cutoff.
    connection.execute("INSERT INTO task_contents(task_id,content_id,inclusion_status,reason) "
                       "SELECT ?,c.id,'excluded_missing_boundary','发布日期缺失，不能归入报告区间' FROM content_items c "
                       "WHERE c.published_at IS NULL AND julianday(c.imported_at)<=julianday(?) "
                       "AND julianday(c.created_at)<=julianday(?) "
                       f"AND {canonical_content_predicate(connection, knowledge_at=cutoff_at)} "
                       f"AND {content_statistics_scope_sql(connection=connection)} "
                       "ON CONFLICT(task_id,content_id) DO NOTHING", (task["id"], cutoff_at, cutoff_at))
    return _store(connection, str(task["id"]), SCOPE_EVENT, {
        "task_id": task["id"], "cutoff_at": cutoff_at, "period_start_at": start_at,
        "period_end_at": end_at, "content_ids": [row["id"] for row in contents],
        "contents": contents, "unknown_dimensions": unknown,
        "account_classification_version": ACCOUNT_CLASSIFICATION_VERSION,
    })


def frozen_report(connection: sqlite3.Connection, task_id: str) -> dict[str, Any] | None:
    return load_event(connection, task_id, INPUT_EVENT)


def store_report(connection: sqlite3.Connection, task_id: str, report: Mapping[str, Any]) -> dict[str, Any]:
    body = copy.deepcopy(dict(report))
    body.pop("files", None)
    body.pop("frozen_inputs", None)
    body["metadata"].pop("revision", None)
    body["metadata"].pop("generated_at", None)
    return _store(connection, task_id, INPUT_EVENT, body)


def render_frozen(event: Mapping[str, Any], *, revision: int, generated_at: str,
                  files: list[dict[str, Any]]) -> dict[str, Any]:
    report = copy.deepcopy(event["payload"])
    report["metadata"].update(revision=revision, generated_at=generated_at)
    report["files"] = copy.deepcopy(files)
    report["frozen_inputs"] = {"contract_version": CONTRACT_VERSION, "event_id": event["event_id"], "sha256": event["sha256"]}
    return report
