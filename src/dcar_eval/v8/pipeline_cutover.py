"""Hash-bound publication dependencies and a one-day pipeline handover receipt.

Read paths never initialize a database or repair a receipt.  Recording a cutover
requires an already recorded activation and real scans/reports; it does not
enable a scheduler, fabricate a cron occurrence, or invoke any provider.
"""
from __future__ import annotations

import argparse
import fcntl
import hashlib
import json
import os
import pwd
import re
import sqlite3
from datetime import date, datetime, time, timedelta
from pathlib import Path
from typing import Any, Mapping, Sequence
from zoneinfo import ZoneInfo

from . import (
    durable_runs,
    report_inputs,
    runtime_receipts,
    scan_receipts,
    scan_terminals,
)
from .contracts import CURRENT_REPORT_VERSION, validate_report
from .snapshot_contract import descriptor, validate_descriptor
from .source_routing import load_policy, parse_time
from .runtime_database import (
    DatabaseAccessMode,
    ResolvedDatabaseAccess,
    RuntimeDatabaseError,
    acquire_writer_lock,
    is_installed_formal_database,
    load_installed_writer_contract,
    observe_writer_lock,
    resolve_installed_database_access,
    resolve_isolated_candidate,
)
from .storage import PROJECT_ROOT, configure_connection_safety, connect, is_formal_database_path, now_utc, require_schema_compatibility, transaction

CONTRACT_VERSION = "pipeline-cutover-v1"
LEGACY_REPORT_VERSION = "dcar-content-operations-report-v8.8"
PROFILE_DAY_DISCOVERY_CONTRACT = "profile-day-publication-discovery-v1"
PROFILE_DAY_REPORT_SCAN_CONTRACT = report_inputs.PROFILE_DAY_SCAN_INPUT_CONTRACT
CUTOVER_JOB = "pipeline_cutover"
ACTIVATION_JOB = "matrix_pipeline_activation"
BEIJING = ZoneInfo("Asia/Shanghai")
SHA256 = re.compile(r"[0-9a-f]{64}")


class PublicationEvidenceError(ValueError):
    """Publication evidence is missing, altered, or outside its frozen scope."""


def canonical(value: Any) -> str:
    return report_inputs.canonical(value)


def digest(value: Any) -> str:
    return report_inputs.digest(value)


def verify_frozen_source_policy(value: Any, expected_sha256: Any) -> None:
    """Validate the report's versioned policy, independently of today's routing."""
    supported = {
        "source-routing-matrix-first-v1": "source_routing_matrix_first_v1.json",
        "source-routing-matrix-first-v2": "source_routing_matrix_first_v2.json",
    }
    version = value.get("policy_version") if isinstance(value, dict) else None
    filename = supported.get(version) if isinstance(version, str) else None
    if filename is None or digest(value) != expected_sha256:
        raise PublicationEvidenceError("publication_report_source_policy_invalid")
    path = Path(__file__).resolve().parents[3] / "config" / filename
    try:
        approved = json.loads(path.read_text(encoding="utf-8"))
    except (OSError, ValueError) as error:
        raise PublicationEvidenceError("publication_report_source_policy_unavailable") from error
    if digest(approved) != expected_sha256:
        raise PublicationEvidenceError("publication_report_source_policy_invalid")


def verify_file(value: Mapping[str, Any], *, project_root: Path = PROJECT_ROOT) -> dict[str, Any]:
    """Verify a registered file without following links or rewriting its path."""
    name = value.get("path")
    expected = value.get("sha256")
    if not isinstance(name, str) or not name or not isinstance(expected, str) or SHA256.fullmatch(expected) is None:
        raise PublicationEvidenceError("publication_file_identity_invalid")
    path = Path(name)
    path = path if path.is_absolute() else project_root / path
    if any(part == ".." for part in path.parts) or any(part.is_symlink() for part in (path, *path.parents)) or not path.is_file():
        raise PublicationEvidenceError("publication_file_missing_or_unsafe")
    before = path.stat()
    hasher = hashlib.sha256()
    with path.open("rb") as stream:
        for chunk in iter(lambda: stream.read(1024 * 1024), b""):
            hasher.update(chunk)
    after = path.stat()
    if ((before.st_dev, before.st_ino, before.st_size, before.st_mtime_ns, before.st_ctime_ns)
            != (after.st_dev, after.st_ino, after.st_size, after.st_mtime_ns, after.st_ctime_ns)
            or hasher.hexdigest() != expected
            or (value.get("byte_size") is not None and value["byte_size"] != after.st_size)):
        raise PublicationEvidenceError("publication_file_hash_mismatch")
    return {"path": str(path), "sha256": expected, "byte_size": after.st_size}


def terminal_run(connection: sqlite3.Connection, row: Mapping[str, Any], *, at: str,
                 statuses: frozenset[str] = frozenset({"succeeded", "partial"})) -> dict[str, Any]:
    """A mutable run flag cannot substitute for its final immutable attempt."""
    if row["status"] not in statuses or not row["completed_at"]:
        raise PublicationEvidenceError("publication_run_not_terminal")
    started, completed = parse_time(row["started_at"]), parse_time(row["completed_at"])
    if completed < started or completed > parse_time(at):
        raise PublicationEvidenceError("publication_run_time_invalid")
    attempt = connection.execute(
        "SELECT * FROM scheduler_run_attempts WHERE scheduler_run_id=? ORDER BY attempt_number DESC LIMIT 1",
        (row["id"],),
    ).fetchone()
    details = json.loads(row["details_json"])
    if (attempt is None or attempt["status"] != row["status"]
            or attempt["started_at"] != row["started_at"] or attempt["completed_at"] != row["completed_at"]
            or canonical(json.loads(attempt["details_json"])) != canonical(details)):
        raise PublicationEvidenceError("publication_attempt_mismatch")
    if details.get("contract_version") == durable_runs.CONTRACT_VERSION:
        owner = details.get("owner", {})
        if (owner.get("attempt_id") != attempt["id"] or owner.get("attempt_number") != attempt["attempt_number"]
                or details.get("scan_id") != durable_runs.scan_identity(row["job_id"], details["identity"])
                or details.get("complete") is not (row["status"] == "succeeded")):
            raise PublicationEvidenceError("publication_durable_identity_mismatch")
    return {"run_id": row["id"], "attempt_id": attempt["id"], "job_id": row["job_id"],
            "status": row["status"], "started_at": row["started_at"], "completed_at": row["completed_at"],
            "details_sha256": digest(details)}


def _scan_errors(
    connection: sqlite3.Connection,
    value: Mapping[str, Any],
    *,
    at: str,
) -> bool:
    errors = value.get("scan_errors", {})
    if not isinstance(errors, Mapping):
        raise PublicationEvidenceError("publication_scan_lineage_invalid")
    for key, expected in errors.items():
        if (
            not isinstance(key, str)
            or not key.isdigit()
            or expected not in scan_receipts.BENIGN_SCAN_ERRORS
        ):
            raise PublicationEvidenceError("publication_scan_lineage_invalid")
        row = connection.execute(
            "SELECT * FROM scheduler_runs WHERE id=?",
            (int(key),),
        ).fetchone()
        if (
            row is None
            or row["job_id"] not in {"matrix_works_scan", "tikhub_reconcile"}
            or parse_time(row["started_at"]) > parse_time(at)
        ):
            raise PublicationEvidenceError("publication_scan_lineage_invalid")
        try:
            if row["status"] == "failed":
                scan_receipts.verify_terminal_scan(connection, dict(row), cutoff_at=at)
            else:
                scan_receipts.verify_scan(connection, dict(row), cutoff_at=at)
        except (ValueError, RuntimeError, KeyError, TypeError, OSError) as error:
            if str(error) != expected:
                raise PublicationEvidenceError("publication_scan_lineage_invalid") from error
        else:
            raise PublicationEvidenceError("publication_scan_lineage_invalid")
    for day in value.get("days", []):
        if not day.get("known") and day.get("reason") != "historical_roster_scope_unknown":
            raise PublicationEvidenceError("publication_roster_lineage_invalid")
    return all(
        error in scan_receipts.BENIGN_SCAN_ERRORS for error in errors.values()
    )


def _record_tikhub_proof(
    *,
    identity_id: int,
    run_id: int,
    proof: Mapping[str, Any],
    succeeded: set[int],
    blocked: set[int],
    not_applicable: set[int],
    terminal_blockers: dict[int, dict[str, Any]],
) -> None:
    """Mirror the producer's success/not-applicable/worst-blocker precedence."""

    terminal_class = proof.get("terminal_class")
    if terminal_class is None:
        succeeded.add(identity_id)
        blocked.discard(identity_id)
        not_applicable.discard(identity_id)
        terminal_blockers.pop(identity_id, None)
        return
    if identity_id in succeeded:
        return
    if terminal_class == "not_applicable":
        not_applicable.add(identity_id)
        blocked.discard(identity_id)
        terminal_blockers.pop(identity_id, None)
        return
    if identity_id in not_applicable:
        return
    blocked.add(identity_id)
    candidate = {
        "terminal_class": terminal_class,
        "reason": proof["reason"],
        "publication_blocker": proof["publication_blocker"],
        "scheduler_run_id": run_id,
    }
    previous = terminal_blockers.get(identity_id)
    proofs = list(previous.get("proofs", [])) if isinstance(previous, dict) else []
    proofs.append(candidate)
    selected = max(
        proofs,
        key=lambda value: (
            scan_terminals.blocker_priority(str(value["terminal_class"])),
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


def verify_scan_reference(connection: sqlite3.Connection, run_id: int, *, at: str) -> dict[str, Any]:
    row = connection.execute("SELECT * FROM scheduler_runs WHERE id=?", (run_id,)).fetchone()
    if row is None or row["job_id"] not in {"matrix_works_scan", "tikhub_reconcile"}:
        raise PublicationEvidenceError("publication_scan_missing")
    try:
        if row["status"] == "succeeded":
            terminal = terminal_run(
                connection,
                dict(row),
                at=at,
                statuses=frozenset({"succeeded"}),
            )
            proof = scan_receipts.verify_scan(connection, dict(row), cutoff_at=at)
        elif row["status"] == "failed":
            terminal = terminal_run(
                connection,
                dict(row),
                at=at,
                statuses=frozenset({"failed"}),
            )
            proof = scan_receipts.verify_terminal_scan(
                connection,
                dict(row),
                cutoff_at=at,
            )
        else:
            raise ValueError("scan_not_terminal_at_cutoff")
    except (ValueError, RuntimeError, KeyError, TypeError, OSError) as error:
        raise PublicationEvidenceError("publication_scan_lineage_invalid") from error
    return {"terminal": terminal, "proof": proof}


def _round_scope(connection: sqlite3.Connection, row: Mapping[str, Any], *, at: str) -> dict[str, Any]:
    first = connection.execute("SELECT * FROM scheduler_run_attempts WHERE scheduler_run_id=? ORDER BY attempt_number LIMIT 1", (row["id"],)).fetchone()
    details = json.loads(row["details_json"])
    seed = json.loads(first["details_json"]) if first is not None else {}
    if (first is None or parse_time(first["started_at"]) > parse_time(at)
            or details.get("contract_version") != durable_runs.CONTRACT_VERSION
            or seed.get("identity") != details.get("identity")
            or details.get("scan_id") != durable_runs.scan_identity(row["job_id"], details["identity"])):
        raise PublicationEvidenceError("publication_round_scope_unproven")
    return {"run_id": row["id"], "first_attempt_id": first["id"], "scoped_at": first["started_at"],
            "identity_sha256": digest(details["identity"])}


def _verify_frozen_scans_v1(
    connection: sqlite3.Connection,
    value: Mapping[str, Any],
    *,
    period_start: str,
    period_end: str,
    cutoff_at: str,
) -> None:
    """Verify the immutable pre-terminal coverage shape without normalizing it."""

    _scan_errors(connection, value, at=cutoff_at)
    dates = []
    day = date.fromisoformat(period_start)
    while day <= date.fromisoformat(period_end):
        dates.append(day.isoformat())
        day += timedelta(days=1)
    if (
        value.get("contract_version") != scan_receipts.LEGACY_CONTRACT_VERSION
        or value.get("cutoff_at") != cutoff_at
        or not isinstance(value.get("days"), list)
        or [item.get("date") for item in value["days"]] != dates
    ):
        raise PublicationEvidenceError("publication_frozen_scan_scope_invalid")
    proofs = {}
    for proof in value["scan_references"]:
        actual = verify_scan_reference(connection, proof["run_id"], at=cutoff_at)["proof"]
        if (
            actual != proof
            or proof.get("terminal_class") is not None
            or proof["run_id"] in proofs
        ):
            raise PublicationEvidenceError("publication_frozen_scan_reference_changed")
        proofs[proof["run_id"]] = proof
    used = set()
    expected_count = covered_count = known_count = 0
    unknown, gaps = [], []
    for day in value["days"]:
        if not day["known"]:
            if (
                day["reason"] != "historical_roster_scope_unknown"
                or day["complete"]
                or day["eligible_identity_ids"]
                or day["covered_identity_ids"]
                or day["matrix_run_ids"]
                or day["tikhub_run_ids"]
            ):
                raise PublicationEvidenceError("publication_frozen_unknown_scope_invalid")
            unknown.append(day["date"])
            continue
        known_count += 1
        row = connection.execute(
            "SELECT * FROM scheduler_runs "
            "WHERE id=? AND job_id='pipeline_round:tikhub_reconcile'",
            (day["round_run_id"],),
        ).fetchone()
        if row is None:
            raise PublicationEvidenceError("publication_frozen_round_missing")
        _round_scope(connection, dict(row), at=cutoff_at)
        scope = json.loads(row["details_json"])["identity"]
        roster, eligible, raw = scan_receipts._roster_scope(connection, scope)
        if (
            scope.get("beijing_day")
            != (date.fromisoformat(day["date"]) + timedelta(days=1)).isoformat()
            or day["roster_snapshot_id"] != roster["id"]
            or day["roster_snapshot_hash"] != roster["members_sha256"]
            or day["eligible_identity_ids"] != eligible
            or day["roster_source"]
            != {"path": str(raw), "sha256": roster["source_sha256"]}
        ):
            raise PublicationEvidenceError("publication_frozen_roster_changed")
        lower = datetime.combine(date.fromisoformat(day["date"]), time.min, BEIJING)
        upper = lower + timedelta(days=1)
        covered, platforms = set(), set()
        for matrix, ids in (
            (True, day["matrix_run_ids"]),
            (False, day["tikhub_run_ids"]),
        ):
            if len(ids) != len(set(ids)):
                raise PublicationEvidenceError("publication_duplicate_scan_reference")
            for run_id in ids:
                proof = proofs.get(run_id)
                if proof is None:
                    raise PublicationEvidenceError("publication_scan_reference_missing")
                used.add(run_id)
                identity = proof["scope"]
                if (
                    identity.get("roster_snapshot_id") != roster["id"]
                    or identity.get("roster_snapshot_hash") != roster["members_sha256"]
                ):
                    raise PublicationEvidenceError("publication_scan_roster_mismatch")
                if matrix:
                    if (
                        identity.get("provider") != "newrank_matrix"
                        or identity.get("kind") != "works"
                        or parse_time(identity["start_at"]) != lower
                        or parse_time(identity["end_at"]) != upper
                    ):
                        raise PublicationEvidenceError("publication_matrix_window_invalid")
                    platforms.add(identity["platform"])
                else:
                    if (
                        str(identity.get("provider", "")).lower() != "tikhub"
                        or identity.get("identity_id") not in eligible
                        or parse_time(identity["window_start"]) != upper - timedelta(days=7)
                        or parse_time(identity["window_end"]) != upper
                    ):
                        raise PublicationEvidenceError("publication_tikhub_window_invalid")
                    covered.add(identity["identity_id"])
        complete = covered == set(eligible) and platforms == {"douyin", "xiaohongshu"}
        if day["complete"] is not complete or day["covered_identity_ids"] != sorted(covered):
            raise PublicationEvidenceError("publication_frozen_scan_conservation_failed")
        expected_count += len(eligible)
        covered_count += len(covered)
        if not complete:
            gaps.append(day["date"])
    detail = value["discovery_coverage"]
    complete = not unknown and not gaps
    expected_percentage = (
        round(100 * covered_count / expected_count, 2)
        if expected_count and not unknown
        else None
    )
    if (
        set(proofs) != used
        or value["complete"] is not complete
        or value["scan_traceable"] is not complete
        or value["roster_evidence_valid"] is not (not unknown)
        or detail["eligible_identity_occurrence_count"] != expected_count
        or detail["covered_identity_occurrence_count"] != covered_count
        or detail["expected_occurrence_count"] != len(dates)
        or detail["observed_occurrence_count"] != known_count
        or detail["percentage"] != expected_percentage
        or detail["complete"] is not complete
        or detail["missing_occurrence_dates"] != unknown + gaps
    ):
        raise PublicationEvidenceError("publication_frozen_coverage_invalid")


def _verify_frozen_scans_v2(
    connection: sqlite3.Connection,
    value: Mapping[str, Any],
    *,
    period_start: str,
    period_end: str,
    cutoff_at: str,
) -> None:
    """Verify terminal-aware frozen coverage without changing its cutoff."""
    scan_errors_are_benign = _scan_errors(connection, value, at=cutoff_at)
    dates = []
    day = date.fromisoformat(period_start)
    while day <= date.fromisoformat(period_end):
        dates.append(day.isoformat())
        day += timedelta(days=1)
    if (value.get("contract_version") != scan_receipts.MATRIX_FIRST_CONTRACT_VERSION or value.get("cutoff_at") != cutoff_at
            or not isinstance(value.get("days"), list) or [item.get("date") for item in value["days"]] != dates):
        raise PublicationEvidenceError("publication_frozen_scan_scope_invalid")
    proofs = {}
    for proof in value["scan_references"]:
        actual = verify_scan_reference(connection, proof["run_id"], at=cutoff_at)["proof"]
        if actual != proof or proof["run_id"] in proofs:
            raise PublicationEvidenceError("publication_frozen_scan_reference_changed")
        proofs[proof["run_id"]] = proof
    used = set()
    scope_total = succeeded_count = blocked_count = 0
    not_applicable_count = accounted_count = required_count = known_count = 0
    unknown, gaps = [], []
    for day in value["days"]:
        if not day["known"]:
            if (day["reason"] != "historical_roster_scope_unknown" or day["complete"]
                    or day.get("partial_publishable")
                    or day["eligible_identity_ids"] or day["covered_identity_ids"]
                    or day.get("succeeded_identity_ids")
                    or day.get("blocked_identity_ids")
                    or day.get("not_applicable_identity_ids")
                    or day.get("accounted_identity_ids")
                    or day.get("required_identity_ids")
                    or day["matrix_run_ids"] or day["tikhub_run_ids"]
                    or day.get("terminal_blockers")):
                raise PublicationEvidenceError("publication_frozen_unknown_scope_invalid")
            unknown.append(day["date"])
            continue
        known_count += 1
        row = connection.execute("SELECT * FROM scheduler_runs WHERE id=? AND job_id='pipeline_round:tikhub_reconcile'", (day["round_run_id"],)).fetchone()
        if row is None:
            raise PublicationEvidenceError("publication_frozen_round_missing")
        _round_scope(connection, dict(row), at=cutoff_at)
        scope = json.loads(row["details_json"])["identity"]
        roster, eligible, raw = scan_receipts._roster_scope(connection, scope)
        if (scope.get("beijing_day") != (date.fromisoformat(day["date"]) + timedelta(days=1)).isoformat()
                or day["roster_snapshot_id"] != roster["id"] or day["roster_snapshot_hash"] != roster["members_sha256"]
                or day["eligible_identity_ids"] != eligible
                or day["roster_source"] != {"path": str(raw), "sha256": roster["source_sha256"]}):
            raise PublicationEvidenceError("publication_frozen_roster_changed")
        lower = datetime.combine(date.fromisoformat(day["date"]), time.min, BEIJING)
        upper = lower + timedelta(days=1)
        succeeded: set[int] = set()
        blocked: set[int] = set()
        not_applicable: set[int] = set()
        terminal_blockers: dict[int, dict[str, Any]] = {}
        platforms = set()
        for matrix, ids in ((True, day["matrix_run_ids"]), (False, day["tikhub_run_ids"])):
            if len(ids) != len(set(ids)):
                raise PublicationEvidenceError("publication_duplicate_scan_reference")
            for run_id in ids:
                proof = proofs.get(run_id)
                if proof is None:
                    raise PublicationEvidenceError("publication_scan_reference_missing")
                used.add(run_id)
                identity = proof["scope"]
                if identity.get("roster_snapshot_id") != roster["id"] or identity.get("roster_snapshot_hash") != roster["members_sha256"]:
                    raise PublicationEvidenceError("publication_scan_roster_mismatch")
                if matrix:
                    if (identity.get("provider") != "newrank_matrix" or identity.get("kind") != "works"
                            or proof.get("terminal_class") is not None
                            or parse_time(identity["start_at"]) != lower or parse_time(identity["end_at"]) != upper):
                        raise PublicationEvidenceError("publication_matrix_window_invalid")
                    platforms.add(identity["platform"])
                else:
                    if (str(identity.get("provider", "")).lower() != "tikhub" or identity.get("identity_id") not in eligible
                            or parse_time(identity["window_start"]) != upper - timedelta(days=7)
                            or parse_time(identity["window_end"]) != upper):
                        raise PublicationEvidenceError("publication_tikhub_window_invalid")
                    _record_tikhub_proof(
                        identity_id=int(identity["identity_id"]),
                        run_id=run_id,
                        proof=proof,
                        succeeded=succeeded,
                        blocked=blocked,
                        not_applicable=not_applicable,
                        terminal_blockers=terminal_blockers,
                    )
        required = set(eligible) - not_applicable
        accounted = succeeded | blocked | not_applicable
        formula = scan_terminals.coverage_decision(
            scope_total=len(eligible),
            succeeded=len(succeeded & required),
            blocked=len(blocked),
            not_applicable=len(not_applicable),
            blocker_classes=frozenset(
                terminal_class
                for item in terminal_blockers.values()
                for terminal_class in item["terminal_classes"]
            ),
            prerequisite_complete=platforms == {"douyin", "xiaohongshu"},
        )
        expected_partial = (
            formula["partial_publishable"] and not formula["complete"]
        )
        expected_reason = (
            ""
            if formula["complete"]
            else "terminal_blocked_partial_publishable"
            if expected_partial
            else "scan_pagination_or_provider_gap"
        )
        if (
            day["complete"] is not formula["complete"]
            or day.get("partial_publishable") is not expected_partial
            or day["covered_identity_ids"] != sorted(succeeded)
            or day.get("succeeded_identity_ids") != sorted(succeeded)
            or day.get("blocked_identity_ids") != sorted(blocked)
            or day.get("not_applicable_identity_ids") != sorted(not_applicable)
            or day.get("accounted_identity_ids") != sorted(accounted)
            or day.get("required_identity_ids") != sorted(required)
            or day.get("terminal_blockers")
            != {str(key): item for key, item in terminal_blockers.items()}
            or day.get("success_percentage") != formula["success_percentage"]
            or day.get("accounted_percentage") != formula["accounted_percentage"]
            or day.get("reason") != expected_reason
        ):
            raise PublicationEvidenceError("publication_frozen_scan_conservation_failed")
        scope_total += len(eligible)
        succeeded_count += len(succeeded)
        blocked_count += len(blocked)
        not_applicable_count += len(not_applicable)
        accounted_count += len(accounted)
        required_count += len(required)
        if not formula["complete"]:
            gaps.append(day["date"])
    detail = value["discovery_coverage"]
    complete = not unknown and not gaps
    partial_publishable = not complete and not unknown and all(
        item.get("complete") is True or item.get("partial_publishable") is True
        for item in value["days"]
    )
    expected_percentage = (
        round(100 * succeeded_count / required_count, 2)
        if required_count and not unknown
        else 100.0
        if scope_total and not unknown
        else None
    )
    expected_accounted_percentage = (
        round(100 * accounted_count / scope_total, 2)
        if scope_total and not unknown
        else 100.0
        if not unknown
        else None
    )
    expected_status = (
        "unknown"
        if unknown
        else "not_applicable"
        if not scope_total
        else "available"
        if expected_percentage is not None and expected_percentage >= 90
        else "below_threshold"
    )
    expected_reason = (
        "已验证完整空名册，无适用采集账号"
        if complete and not scope_total
        else ""
        if complete
        else "全部义务已终态且满足部分发布门槛"
        if partial_publishable
        else "冻结名册或来源分页证据不完整"
    )
    expected_observation = {
        "status": (
            "complete"
            if complete
            else "partial_publishable"
            if partial_publishable
            else "incomplete"
        ),
        "capture_observation_start_date": None,
        "expected_dates": dates,
        "legacy_unobserved_dates": unknown,
        "pipeline_gap_dates": gaps,
        "zero_content_dates": [],
    }
    if (set(proofs) != used or value["complete"] is not complete
            or value.get("partial_publishable") is not partial_publishable
            or value["scan_traceable"]
            is not (
                (complete or partial_publishable) and scan_errors_are_benign
            )
            or value["roster_evidence_valid"] is not (not unknown)
            or detail["eligible_identity_occurrence_count"] != scope_total
            or detail["covered_identity_occurrence_count"] != succeeded_count
            or detail.get("succeeded_identity_occurrence_count") != succeeded_count
            or detail.get("blocked_identity_occurrence_count") != blocked_count
            or detail.get("not_applicable_identity_occurrence_count") != not_applicable_count
            or detail.get("accounted_identity_occurrence_count") != accounted_count
            or detail.get("required_identity_occurrence_count") != required_count
            or detail.get("accounted_percentage") != expected_accounted_percentage
            or detail["expected_occurrence_count"] != len(dates) or detail["observed_occurrence_count"] != known_count
            or detail["percentage"] != expected_percentage or detail["complete"] is not complete
            or detail.get("partial_publishable") is not partial_publishable
            or detail.get("status") != expected_status
            or detail.get("eligible_basis") != "frozen_matrix_roster_identity_occurrences"
            or detail.get("success_rule") != scan_receipts.MATRIX_FIRST_CONTRACT_VERSION
            or detail.get("reason") != expected_reason
            or detail["missing_occurrence_dates"] != unknown + gaps
            or detail.get("roster_validation_failures") != len(unknown)
            or value.get("pipeline_observation") != expected_observation):
        raise PublicationEvidenceError("publication_frozen_coverage_invalid")


def verify_frozen_scans(
    connection: sqlite3.Connection,
    value: Mapping[str, Any],
    *,
    period_start: str,
    period_end: str,
    cutoff_at: str,
) -> None:
    """Dispatch only by the frozen coverage version; never infer its shape."""

    version = value.get("contract_version")
    if version == PROFILE_DAY_REPORT_SCAN_CONTRACT:
        try:
            period = runtime_receipts.period_coverage_from_receipts(
                connection,
                period_start=period_start,
                period_end=period_end,
                cutoff_at=cutoff_at,
            )
            expected = report_inputs.compact_profile_day_scan_inputs(
                period,
                period_start=period_start,
                period_end=period_end,
            )
        except (
            report_inputs.FrozenInputError,
            runtime_receipts.RuntimeReceiptError,
        ) as error:
            raise PublicationEvidenceError(
                "publication_frozen_receipt_lineage_invalid"
            ) from error
        if canonical(expected) != canonical(value):
            raise PublicationEvidenceError(
                "publication_frozen_receipt_reference_changed"
            )
        return
    if version == runtime_receipts.PERIOD_RECEIPT_CONTRACT:
        try:
            actual = runtime_receipts.period_coverage_from_receipts(
                connection,
                period_start=period_start,
                period_end=period_end,
                cutoff_at=cutoff_at,
            )
        except runtime_receipts.RuntimeReceiptError as error:
            raise PublicationEvidenceError(
                "publication_frozen_receipt_lineage_invalid"
            ) from error
        if canonical(actual) != canonical(value):
            raise PublicationEvidenceError(
                "publication_frozen_receipt_reference_changed"
            )
        return
    if version == scan_receipts.LEGACY_CONTRACT_VERSION:
        _verify_frozen_scans_v1(
            connection,
            value,
            period_start=period_start,
            period_end=period_end,
            cutoff_at=cutoff_at,
        )
        return
    if version == scan_receipts.MATRIX_FIRST_CONTRACT_VERSION:
        _verify_frozen_scans_v2(
            connection,
            value,
            period_start=period_start,
            period_end=period_end,
            cutoff_at=cutoff_at,
        )
        return
    raise PublicationEvidenceError("publication_frozen_scan_scope_invalid")


def runtime_evidence(connection: sqlite3.Connection, *, at: str) -> dict[str, Any]:
    """Only complete or fully-accounted publishable coverage may be exposed."""
    if runtime_receipts._has_native_receipts(connection):
        try:
            receipt = runtime_receipts.read_profile_day_coverage_receipt(
                connection,
                at=at,
            )
        except runtime_receipts.RuntimeReceiptError as error:
            raise PublicationEvidenceError(
                "publication_runtime_receipt_lineage_invalid"
            ) from error
        if receipt is None:
            raise PublicationEvidenceError(
                "publication_current_profile_day_receipt_missing"
            )
        summary = receipt.get("summary")
        scope = receipt.get("scope")
        if not isinstance(summary, dict) or not isinstance(scope, dict):
            raise PublicationEvidenceError(
                "publication_runtime_receipt_scope_invalid"
            )
        coverage = summary.get("coverage")
        business_day = runtime_receipts._coverage_business_day(at)
        if (
            not isinstance(coverage, dict)
            or coverage.get("contract_version")
            != runtime_receipts.PROFILE_DAY_CONTRACT
            or scope.get("business_day") != business_day
            or coverage.get("business_day") != business_day
            or any(
                coverage.get(key) != scope.get(key)
                for key in (
                    "activation_id",
                    "profile_id",
                    "activation_sha256",
                    "source_family",
                    "roster_snapshot_id",
                    "roster_snapshot_hash",
                )
            )
        ):
            raise PublicationEvidenceError(
                "publication_runtime_receipt_scope_invalid"
            )
        if not (
            coverage.get("complete") is True
            or coverage.get("partial_publishable") is True
        ):
            raise PublicationEvidenceError(
                "publication_runtime_coverage_not_publishable"
            )
        return {
            "contract_version": PROFILE_DAY_DISCOVERY_CONTRACT,
            "verified_at": at,
            "coverage": {
                **coverage,
                "receipt": {
                    "run_id": receipt["run_id"],
                    "attempt_id": receipt["attempt_id"],
                    "sequence": summary["sequence"],
                    "sealed_at": summary["sealed_at"],
                    "scope": scope,
                    "self_sha256": receipt["self_sha256"],
                },
            },
        }
    result = scan_receipts.runtime_coverage(connection, at=at)
    scan_errors_are_benign = _scan_errors(connection, result, at=at)
    if result["status"] == "unknown" or result.get("roster_snapshot_id") is None:
        raise PublicationEvidenceError("publication_current_roster_scope_unknown")
    round_row = connection.execute("SELECT * FROM scheduler_runs WHERE id=?", (result["round_run_id"],)).fetchone()
    if round_row is None:
        raise PublicationEvidenceError("publication_current_round_missing")
    round_proof = _round_scope(connection, dict(round_row), at=at)
    identity = json.loads(round_row["details_json"])["identity"]
    _roster, _eligible, raw = scan_receipts._roster_scope(connection, identity)
    end = parse_time(identity["scheduled_at"]).astimezone(BEIJING).replace(hour=0, minute=0, second=0, microsecond=0)
    scans = []
    expected_matrix = {
        (platform, end - timedelta(days=index + 1), end - timedelta(days=index))
        for index in range(30)
        for platform in ("douyin", "xiaohongshu")
    }
    finished_matrix = set()
    succeeded: set[int] = set()
    blocked: set[int] = set()
    not_applicable: set[int] = set()
    terminal_blockers: dict[int, dict[str, Any]] = {}
    for record in connection.execute(
        "SELECT * FROM scheduler_runs WHERE job_id IN ('matrix_works_scan','tikhub_reconcile') ORDER BY id"
    ):
        row = dict(record)
        detail = json.loads(row["details_json"])
        scope = detail.get("identity", {})
        if (scope.get("roster_snapshot_id") != identity["roster_snapshot_id"]
                or scope.get("roster_snapshot_hash") != identity["roster_snapshot_hash"]):
            continue
        if scope.get("provider") == "newrank_matrix":
            if (scope.get("kind") != "works" or scope.get("overall_start_at") is None
                    or parse_time(scope["overall_start_at"]) != end - timedelta(days=30)
                    or parse_time(scope["overall_end_at"]) != end):
                continue
        elif (scope.get("identity_id") not in identity["eligible_identity_ids"]
              or parse_time(scope["window_start"]) != end - timedelta(days=7)
              or parse_time(scope["window_end"]) != end):
            continue
        if (row["status"] not in {"succeeded", "failed"} or not row["completed_at"]
                or parse_time(row["completed_at"]) > parse_time(at)):
            continue
        if row["status"] == "failed" and scope.get("provider") == "newrank_matrix":
            continue
        verified = verify_scan_reference(connection, row["id"], at=at)
        proof = verified["proof"]
        scans.append(verified)
        if scope.get("provider") == "newrank_matrix":
            key = (
                scope["platform"],
                parse_time(scope["start_at"]),
                parse_time(scope["end_at"]),
            )
            if key in expected_matrix:
                finished_matrix.add(key)
            continue
        _record_tikhub_proof(
            identity_id=int(scope["identity_id"]),
            run_id=row["id"],
            proof=proof,
            succeeded=succeeded,
            blocked=blocked,
            not_applicable=not_applicable,
            terminal_blockers=terminal_blockers,
        )
    required = set(identity["eligible_identity_ids"]) - not_applicable
    accounted = succeeded | blocked | not_applicable
    formula = scan_terminals.coverage_decision(
        scope_total=len(identity["eligible_identity_ids"]),
        succeeded=len(succeeded & required),
        blocked=len(blocked),
        not_applicable=len(not_applicable),
        blocker_classes=frozenset(
            terminal_class
            for item in terminal_blockers.values()
            for terminal_class in item["terminal_classes"]
        ),
        prerequisite_complete=finished_matrix == expected_matrix,
    )
    expected_complete = formula["complete"]
    expected_partial = formula["partial_publishable"]
    expected_status = (
        "complete"
        if expected_complete
        else "partial_publishable"
        if expected_partial
        else "incomplete"
    )
    expected_reason = (
        ""
        if expected_complete
        else "terminal_blocked_partial_publishable"
        if expected_partial
        else "scan_pagination_or_provider_gap"
    )
    if (
        result.get("matrix_expected_windows") != len(expected_matrix)
        or result.get("matrix_complete_windows") != len(finished_matrix)
        or result.get("roster_snapshot_id") != identity["roster_snapshot_id"]
        or result.get("round_run_id") != round_proof["run_id"]
        or result.get("tikhub_expected_members") != len(identity["eligible_identity_ids"])
        or result.get("tikhub_complete_members") != len(succeeded)
        or result.get("tikhub_succeeded_members") != len(succeeded)
        or result.get("tikhub_blocked_members") != len(blocked)
        or result.get("tikhub_not_applicable_members") != len(not_applicable)
        or result.get("tikhub_accounted_members") != len(accounted)
        or result.get("tikhub_required_members") != len(required)
        or result.get("complete") is not expected_complete
        or result.get("partial_publishable") is not expected_partial
        or result.get("status") != expected_status
        or result.get("reason") != expected_reason
        or (
            "scan_traceable" in result
            and result["scan_traceable"]
            is not (
                (expected_complete or expected_partial)
                and scan_errors_are_benign
            )
        )
    ):
        raise PublicationEvidenceError("publication_runtime_coverage_invalid")
    if not (expected_complete or expected_partial):
        raise PublicationEvidenceError("publication_runtime_coverage_not_publishable")
    return {"contract_version": "matrix-publication-discovery-v1", "verified_at": at,
            "coverage": result, "round": round_proof, "scope": identity,
            "roster_source": verify_file({"path": str(raw), "sha256": _roster["source_sha256"]}),
            "scans": scans}


def _raw_ids(value: Any) -> set[int]:
    found: set[int] = set()
    if isinstance(value, dict):
        for key, child in value.items():
            if key in {"raw_response_id", "raw_id"} and type(child) is int:
                found.add(child)
            else:
                found.update(_raw_ids(child))
    elif isinstance(value, list):
        for child in value:
            found.update(_raw_ids(child))
    return found


def report_dependency(connection: sqlite3.Connection, task_id: str, *, at: str,
                      project_root: Path = PROJECT_ROOT, revision: int | None = None) -> dict[str, Any]:
    """Re-read hashes, scope/input freezes, and exact supplier references."""
    task_row = connection.execute("SELECT * FROM report_tasks WHERE id=?", (task_id,)).fetchone()
    if task_row is None or task_row["task_status"] not in {"succeeded", "partial"}:
        raise PublicationEvidenceError("publication_report_not_terminal")
    task = dict(task_row)
    if not task["completed_at"] or parse_time(task["completed_at"]) > parse_time(at):
        raise PublicationEvidenceError("publication_report_time_invalid")
    query = "SELECT * FROM report_revisions WHERE task_id=? AND invalidated_at IS NULL"
    args: tuple[Any, ...] = (task_id,)
    if revision is not None:
        query += " AND revision=?"
        args += (revision,)
    record = connection.execute(query + " ORDER BY revision DESC LIMIT 1", args).fetchone()
    schema_version = int(connection.execute("PRAGMA user_version").fetchone()[0])
    expected_report_version = (
        LEGACY_REPORT_VERSION if schema_version == 18 else CURRENT_REPORT_VERSION
    )
    if record is None or record["contract_version"] != expected_report_version:
        raise PublicationEvidenceError("publication_report_contract_mismatch")
    rev = dict(record)
    report_file = verify_file({"path": rev["report_json_path"], "sha256": rev["report_sha256"]}, project_root=project_root)
    report = json.loads(Path(report_file["path"]).read_bytes())
    validate_report(report)
    inputs = report_inputs.load_event(connection, task_id, report_inputs.INPUT_EVENT)
    scope = report_inputs.load_event(connection, task_id, report_inputs.SCOPE_EVENT)
    if inputs is None or scope is None:
        raise PublicationEvidenceError("publication_report_freeze_missing")
    frozen = report.get("frozen_inputs")
    expected_frozen = {"contract_version": report_inputs.CONTRACT_VERSION, "event_id": inputs["event_id"], "sha256": inputs["sha256"]}
    refs = inputs["payload"]["input_references"]
    verify_frozen_source_policy(refs.get("source_policy"), refs.get("source_policy_sha256"))
    cutoff = inputs["payload"]["metadata"]["collection_cutoff_at"]
    if (frozen != expected_frozen or report["metadata"]["task_id"] != task_id
            or report["metadata"]["revision"] != rev["revision"]
            or inputs["payload"]["task"]["task_status"] != task["task_status"]
            or scope["payload"]["task_id"] != task_id or scope["payload"]["cutoff_at"] != cutoff
            or refs["scope_event_id"] != scope["event_id"] or refs["scope_sha256"] != scope["sha256"]
            or refs["content_ids"] != scope["payload"]["content_ids"]
            or refs["release_id"] != rev["release_id"]
            or refs["source_policy_sha256"] != digest(refs["source_policy"])
            or parse_time(cutoff) > parse_time(at)):
        raise PublicationEvidenceError("publication_report_freeze_binding_mismatch")
    if digest({key: value for key, value in report.items() if key not in {"files", "frozen_inputs", "metadata"}}) != digest(
            {key: value for key, value in inputs["payload"].items() if key != "metadata"}):
        raise PublicationEvidenceError("publication_report_input_payload_mismatch")
    release = connection.execute("SELECT * FROM evaluation_releases WHERE id=? AND status='active'", (refs["release_id"],)).fetchone()
    if release is None or release["matcher_rule_sha256"] != refs["matcher_rule_sha256"]:
        raise PublicationEvidenceError("publication_report_release_mismatch")
    scans = refs["scans"]
    verify_frozen_scans(connection, scans, period_start=task["period_start"], period_end=task["period_end"], cutoff_at=cutoff)
    files = []
    for item in connection.execute("SELECT * FROM report_files WHERE task_id=? AND revision=? ORDER BY file_kind", (task_id, rev["revision"])):
        if item["status"] != "available":
            raise PublicationEvidenceError("publication_report_file_unavailable")
        files.append({"file_kind": item["file_kind"], **verify_file(
            {"path": item["local_path"], "sha256": item["sha256"], "byte_size": item["byte_size"]}, project_root=project_root)})
    if not any(item["file_kind"] == "report-json" and item["sha256"] == rev["report_sha256"] for item in files):
        raise PublicationEvidenceError("publication_report_json_unregistered")
    raw_files = []
    for raw_id in sorted(_raw_ids(refs)):
        raw = connection.execute("SELECT * FROM provider_raw_responses WHERE id=?", (raw_id,)).fetchone()
        if raw is None or parse_time(raw["captured_at"]) > parse_time(cutoff):
            raise PublicationEvidenceError("publication_report_raw_reference_invalid")
        raw_files.append({"raw_id": raw_id, **verify_file(
            {"path": raw["local_path"], "sha256": raw["sha256"], "byte_size": raw["byte_size"]}, project_root=project_root)})
    return {"task_id": task_id, "task_type": task["task_type"], "creation_source": task["creation_source"],
            "period_start": task["period_start"], "period_end": task["period_end"], "status": task["task_status"],
            "completed_at": task["completed_at"], "cutoff_at": cutoff, "revision": rev["revision"],
            "release_id": refs["release_id"], "matcher_rule_sha256": refs["matcher_rule_sha256"],
            "scope_event_id": scope["event_id"], "scope_sha256": scope["sha256"],
            "input_event_id": inputs["event_id"], "input_sha256": inputs["sha256"],
            "report_file": report_file, "files": files, "raw_files": raw_files,
            "source_policy_sha256": refs["source_policy_sha256"], "scan_sha256": digest(scans),
            "partial_reasons": inputs["payload"]["data_quality_details"] if task["task_status"] == "partial" else {}}


def _accepted_roster(connection: sqlite3.Connection, roster_id: int, members_sha256: str, *, at: str) -> tuple[dict[str, Any], Path]:
    """An accepted row is not sufficient without its full-source candidate."""
    from . import account_roster

    members = [record[0] for record in connection.execute(
        "SELECT account_identity_id FROM account_roster_members WHERE snapshot_id=? ORDER BY account_identity_id", (roster_id,))]
    roster, _eligible, raw = scan_receipts._roster_scope(connection, {
        "roster_snapshot_id": roster_id, "roster_snapshot_hash": members_sha256,
        "scheduled_at": at, "eligible_identity_ids": members})
    scope, metadata = json.loads(roster["scope_json"]), json.loads(roster["metadata_json"])
    if (roster["contract_version"] != account_roster.CONTRACT_VERSION
            or not scope.get("organization") or scope.get("coverage") != "full"
            or scope.get("account_scope") != "all_added_accounts" or set(scope.get("platforms", [])) != account_roster.PLATFORMS):
        raise PublicationEvidenceError("activation_roster_scope_incomplete")
    candidate_row = connection.execute("SELECT * FROM scheduler_runs WHERE id=? AND job_id=?", (
        metadata.get("candidate_id"), account_roster.CANDIDATE_JOB)).fetchone()
    if candidate_row is None or candidate_row["status"] != "succeeded" or candidate_row["completed_at"] != roster["accepted_at"]:
        raise PublicationEvidenceError("activation_roster_acceptance_unproven")
    candidate = json.loads(candidate_row["details_json"])
    payload = candidate.get("payload", {})
    if (candidate.get("status") != "accepted" or candidate.get("snapshot_id") != roster_id
            or candidate.get("raw_response_id") != metadata.get("raw_response_id")
            or candidate.get("payload_sha256") != metadata.get("manifest_sha256")
            or payload.get("scope") != scope
            or any(payload.get(key) != roster[key] for key in (
                "source_type", "scope_key", "source_instance_id", "source_captured_at",
                "declared_count", "members_sha256", "source_sha256"))):
        raise PublicationEvidenceError("activation_roster_acceptance_unproven")
    account_roster._verify_raw(connection, candidate)
    return roster, raw


def _activation(connection: sqlite3.Connection, run_id: int, *, at: str) -> dict[str, Any]:
    rows = connection.execute("SELECT * FROM scheduler_runs WHERE job_id=? ORDER BY id", (ACTIVATION_JOB,)).fetchall()
    if len(rows) != 1 or rows[0]["id"] != run_id:
        raise PublicationEvidenceError("cutover_requires_single_activation")
    row = dict(rows[0])
    proof = terminal_run(connection, row, at=at, statuses=frozenset({"succeeded"}))
    value = json.loads(row["details_json"])
    if (value.get("contract_version") != "matrix-first-pipeline-v1" or value.get("mode") != "active"
            or value.get("schema_version") != 18 or value.get("schema_migration") != "matrix-roster-source-routing"
            or value.get("report_version") != LEGACY_REPORT_VERSION or value.get("source_policy_sha256") != digest(load_policy())):
        raise PublicationEvidenceError("cutover_activation_contract_mismatch")
    validate_descriptor(value.get("snapshot_contract"))
    release = connection.execute("SELECT er.*,tv.status taxonomy_status FROM evaluation_releases er JOIN taxonomy_versions tv ON tv.version=er.taxonomy_version WHERE er.status='active'").fetchall()
    if (len(release) != 1 or release[0]["id"] != value.get("active_release_id")
            or release[0]["matcher_rule_sha256"] != value.get("matcher_rule_sha256")
            or release[0]["id"] != "evaluation-v9__selling-points-v5.2" or release[0]["rule_version"] != "evaluation-v9"
            or release[0]["taxonomy_version"] != "selling-points-v5.2" or release[0]["taxonomy_status"] != "published"):
        raise PublicationEvidenceError("cutover_activation_release_mismatch")
    cutover = parse_time(value["cutover_at"])
    if cutover != parse_time(row["started_at"]) or cutover.astimezone(BEIJING).date() != parse_time(at).astimezone(BEIJING).date():
        raise PublicationEvidenceError("cutover_outside_first_beijing_day")
    roster, raw = _accepted_roster(connection, value["roster_snapshot_id"], value["roster_snapshot_hash"], at=value["cutover_at"])
    return {"activation": proof, "cutover_at": value["cutover_at"], "beijing_date": cutover.astimezone(BEIJING).date().isoformat(),
            "roster_snapshot_id": roster["id"], "roster_snapshot_hash": roster["members_sha256"],
            "roster_source": verify_file({"path": str(raw), "sha256": roster["source_sha256"]})}


def record_activation(*, db_path: Path, at: str | None = None) -> dict[str, Any]:
    """Record the actual one-time activation action, never a capture result.

    The caller owns the single-writer lock. Formal clocks cannot be overridden;
    deterministic timestamps are supported only for isolated/offline fixtures.
    Repeated calls return the original event and never change its time/scope.
    """
    if is_formal_database_path(db_path) and at is not None:
        raise PublicationEvidenceError("formal_activation_clock_override_forbidden")
    if db_path.is_symlink() or not db_path.is_file():
        raise PublicationEvidenceError("activation_database_missing")
    timestamp = at or now_utc()
    parse_time(timestamp)
    from .account_roster import current_snapshot
    with connect(db_path) as connection, transaction(connection):
        require_schema_compatibility(connection, supported_versions=frozenset({18}))
        roster = current_snapshot(connection)
        if roster is None:
            raise PublicationEvidenceError("activation_accepted_roster_missing")
        _accepted_roster(connection, roster["id"], roster["members_sha256"], at=timestamp)
        releases = connection.execute("SELECT er.*,tv.status taxonomy_status FROM evaluation_releases er JOIN taxonomy_versions tv ON tv.version=er.taxonomy_version WHERE er.status='active'").fetchall()
        if (len(releases) != 1 or releases[0]["id"] != "evaluation-v9__selling-points-v5.2"
                or releases[0]["rule_version"] != "evaluation-v9" or releases[0]["taxonomy_version"] != "selling-points-v5.2"
                or releases[0]["taxonomy_status"] != "published" or SHA256.fullmatch(releases[0]["matcher_rule_sha256"]) is None):
            raise PublicationEvidenceError("activation_release_identity_invalid")
        details = {"contract_version": "matrix-first-pipeline-v1", "mode": "active", "cutover_at": timestamp,
                   "roster_snapshot_id": roster["id"], "roster_snapshot_hash": roster["members_sha256"],
                   "source_policy_sha256": digest(load_policy()), "snapshot_contract": descriptor(),
                   "schema_version": 18, "schema_migration": "matrix-roster-source-routing", "report_version": LEGACY_REPORT_VERSION,
                   "active_release_id": releases[0]["id"], "matcher_rule_sha256": releases[0]["matcher_rule_sha256"]}
        existing = connection.execute("SELECT * FROM scheduler_runs WHERE job_id=? ORDER BY id", (ACTIVATION_JOB,)).fetchall()
        if existing:
            if len(existing) != 1:
                raise PublicationEvidenceError("activation_event_ambiguous")
            old = json.loads(existing[0]["details_json"])
            if {key: value for key, value in old.items() if key != "cutover_at"} != {key: value for key, value in details.items() if key != "cutover_at"}:
                raise PublicationEvidenceError("activation_cannot_reset_or_rebind")
            return _activation(connection, existing[0]["id"], at=old["cutover_at"])
        encoded = canonical(details)
        run_id = connection.execute("INSERT INTO scheduler_runs(job_id,scheduled_for,status,started_at,details_json) VALUES (?,?,'running',?,?)",
            (ACTIVATION_JOB, timestamp, timestamp, encoded)).lastrowid
        attempt_id = connection.execute("INSERT INTO scheduler_run_attempts(scheduler_run_id,attempt_number,invocation_source,status,started_at,details_json) "
            "VALUES (?,1,'operator_retry','running',?,?)", (run_id, timestamp, encoded)).lastrowid
        connection.execute("UPDATE scheduler_run_attempts SET status='succeeded',completed_at=? WHERE id=?", (timestamp, attempt_id))
        connection.execute("UPDATE scheduler_runs SET status='succeeded',completed_at=? WHERE id=?", (timestamp, run_id))
        return _activation(connection, int(run_id or 0), at=timestamp)


def _cutover_payload(connection: sqlite3.Connection, *, activation_run_id: int, report_task_ids: Sequence[str],
                     scan_run_ids: Sequence[int], cutoff_at: str, at: str, project_root: Path,
                     report_revisions: Mapping[str, int] | None = None) -> dict[str, Any]:
    configure_connection_safety(connection)
    require_schema_compatibility(connection, supported_versions=frozenset({18}))
    active = _activation(connection, activation_run_id, at=at)
    if not report_task_ids or len(set(report_task_ids)) != len(report_task_ids) or not scan_run_ids or len(set(scan_run_ids)) != len(scan_run_ids):
        raise PublicationEvidenceError("cutover_requires_real_reports_and_scans")
    if parse_time(cutoff_at) < parse_time(active["cutover_at"]) or parse_time(cutoff_at) > parse_time(at):
        raise PublicationEvidenceError("cutover_cutoff_invalid")
    reports = [report_dependency(connection, task_id, at=at, project_root=project_root,
        revision=report_revisions[task_id] if report_revisions is not None else None) for task_id in sorted(report_task_ids)]
    if any(report["creation_source"] != "manual" or report["cutoff_at"] != cutoff_at for report in reports):
        raise PublicationEvidenceError("cutover_requires_explicit_manual_cutoff")
    scans = [verify_scan_reference(connection, run_id, at=cutoff_at) for run_id in sorted(scan_run_ids)]
    for scan in scans:
        scope = scan["proof"]["scope"]
        if (scope.get("roster_snapshot_id") != active["roster_snapshot_id"]
                or scope.get("roster_snapshot_hash") != active["roster_snapshot_hash"]
                or parse_time(scan["terminal"]["started_at"]) < parse_time(active["cutover_at"])):
            raise PublicationEvidenceError("cutover_scan_scope_mismatch")
    return {"contract_version": CONTRACT_VERSION, **active, "cutoff_at": cutoff_at,
            "schema_version": 18, "schema_migration": "matrix-roster-source-routing", "report_version": LEGACY_REPORT_VERSION,
            "snapshot_contract": descriptor(), "source_policy_sha256": digest(load_policy()),
            "report_task_ids": sorted(report_task_ids), "scan_run_ids": sorted(scan_run_ids),
            "reports": reports, "scans": scans,
            "status": "partial" if any(report["status"] == "partial" for report in reports) else "succeeded"}


def record_cutover(*, db_path: Path, activation_run_id: int, report_task_ids: Sequence[str],
                   scan_run_ids: Sequence[int], cutoff_at: str, at: str | None = None,
                   project_root: Path = PROJECT_ROOT) -> dict[str, Any]:
    """Seal a real first-day handover; caller must already own the writer.

    This explicit mutator has no default database and does not run on startup.
    It reuses the durable ledger, writes one O_EXCL file, then finalizes one
    immutable attempt. A failed/incomplete write is never a publishable receipt.
    """
    if is_formal_database_path(db_path) and at is not None:
        raise PublicationEvidenceError("formal_cutover_clock_override_forbidden")
    at = at or now_utc()
    if db_path.is_symlink() or not db_path.is_file() or not project_root.is_dir():
        raise PublicationEvidenceError("cutover_database_or_project_missing")
    with connect(db_path) as connection:
        existing = connection.execute("SELECT id FROM scheduler_runs WHERE job_id=? AND json_extract(details_json,'$.identity.activation_run_id')=?",
                                      (CUTOVER_JOB, activation_run_id)).fetchall()
        if existing:
            if len(existing) != 1:
                raise PublicationEvidenceError("cutover_receipt_ambiguous")
            receipt = verify_cutover(connection, run_id=existing[0]["id"], at=at, project_root=project_root)
            if (receipt["payload"]["report_task_ids"] != sorted(report_task_ids)
                    or receipt["payload"]["scan_run_ids"] != sorted(scan_run_ids)
                    or receipt["payload"]["cutoff_at"] != cutoff_at):
                raise PublicationEvidenceError("cutover_cannot_rebind_reports_or_scans")
            return receipt
        payload = _cutover_payload(connection, activation_run_id=activation_run_id, report_task_ids=report_task_ids,
            scan_run_ids=scan_run_ids, cutoff_at=cutoff_at, at=at, project_root=project_root)
    identity = {"contract_version": CONTRACT_VERSION, "activation_run_id": activation_run_id,
                "beijing_date": payload["beijing_date"], "cutoff_at": cutoff_at, "payload_sha256": digest(payload)}
    claim = durable_runs.claim_run(CUTOVER_JOB, identity, db_path=db_path, now=at,
        invocation_source="operator_retry", scope_key={"activation_run_id": activation_run_id})
    if claim is None:
        raise PublicationEvidenceError("cutover_already_recorded_or_incomplete")
    root = project_root / "data/cache/pipeline-cutovers"
    root.mkdir(parents=True, exist_ok=True, mode=0o700)
    if any(item.is_symlink() for item in (root, *root.parents)):
        raise PublicationEvidenceError("cutover_receipt_directory_unsafe")
    body = (canonical(payload) + "\n").encode()
    sha = hashlib.sha256(body).hexdigest()
    path = root / f"cutover-{activation_run_id}-{sha}.json"
    descriptor_number = os.open(path, os.O_WRONLY | os.O_CREAT | os.O_EXCL | os.O_NOFOLLOW, 0o600)
    with os.fdopen(descriptor_number, "wb") as stream:
        stream.write(body)
        stream.flush()
        os.fsync(stream.fileno())
    directory = os.open(root, os.O_RDONLY)
    try:
        os.fsync(directory)
    finally:
        os.close(directory)
    reference = {"path": str(path), "sha256": sha, "byte_size": len(body)}
    with connect(db_path) as connection, transaction(connection):
        current = _cutover_payload(connection, activation_run_id=activation_run_id, report_task_ids=report_task_ids,
            scan_run_ids=scan_run_ids, cutoff_at=cutoff_at, at=at, project_root=project_root,
            report_revisions={report["task_id"]: report["revision"] for report in payload["reports"]})
        if current != payload:
            raise PublicationEvidenceError("cutover_inputs_changed_during_seal")
        durable_runs.checkpoint(connection, claim, {"complete": True, "receipt": reference}, now=at)
    durable_runs.finish_run(claim, status="succeeded", summary={"publication_status": payload["status"]}, db_path=db_path, now=at)
    with connect(db_path) as connection:
        return verify_cutover(connection, run_id=claim.scheduler_run_id, at=at, project_root=project_root)


def verify_cutover(connection: sqlite3.Connection, *, run_id: int, at: str,
                   project_root: Path = PROJECT_ROOT) -> dict[str, Any]:
    row = connection.execute("SELECT * FROM scheduler_runs WHERE id=? AND job_id=?", (run_id, CUTOVER_JOB)).fetchone()
    if row is None:
        raise PublicationEvidenceError("cutover_receipt_missing")
    terminal = terminal_run(connection, dict(row), at=at, statuses=frozenset({"succeeded"}))
    details = json.loads(row["details_json"])
    if details.get("contract_version") != durable_runs.CONTRACT_VERSION or details["identity"].get("contract_version") != CONTRACT_VERSION:
        raise PublicationEvidenceError("cutover_contract_mismatch")
    reference = verify_file(details["checkpoint"]["receipt"], project_root=project_root)
    payload = json.loads(Path(reference["path"]).read_bytes())
    if digest(payload) != details["identity"]["payload_sha256"]:
        raise PublicationEvidenceError("cutover_payload_hash_mismatch")
    validate_descriptor(payload.get("snapshot_contract"))
    expected = _cutover_payload(connection, activation_run_id=payload["activation"]["run_id"],
        report_task_ids=payload["report_task_ids"], scan_run_ids=payload["scan_run_ids"],
        cutoff_at=payload["cutoff_at"], at=at, project_root=project_root,
        report_revisions={report["task_id"]: report["revision"] for report in payload["reports"]})
    if expected != payload:
        raise PublicationEvidenceError("cutover_bound_evidence_changed")
    return {"contract_version": CONTRACT_VERSION, "terminal": terminal, "receipt": reference, "payload": payload}


def _installed_writer(*, required: bool) -> tuple[Path, dict[str, Any]] | None:
    """Compatibility view over the shared installed-runtime resolver."""
    home = Path(pwd.getpwuid(os.getuid()).pw_dir).resolve(strict=True)
    try:
        installed = load_installed_writer_contract(required=required, home=home)
    except (OSError, RuntimeDatabaseError) as error:
        raise PublicationEvidenceError(
            "cutover_installed_writer_config_missing_or_unsafe"
        ) from error
    if installed is None:
        return None
    return home, dict(installed.payload)


def _installed_writer_access(root: Path, db_path: Path) -> ResolvedDatabaseAccess:
    home = Path(pwd.getpwuid(os.getuid()).pw_dir).resolve(strict=True)
    try:
        installed = load_installed_writer_contract(required=True, home=home)
        assert installed is not None
        if not os.environ.get("DCAR_V8_DB"):
            raise RuntimeDatabaseError("DCAR_V8_DB is required")
        if not os.environ.get("DCAR_WRITER_LOCK"):
            raise RuntimeDatabaseError("DCAR_WRITER_LOCK is required")
        if not os.environ.get("DCAR_PROJECT_ROOT"):
            raise RuntimeDatabaseError("DCAR_PROJECT_ROOT is required")
        return resolve_installed_database_access(
            DatabaseAccessMode.FORMAL_MUTATION,
            database=db_path,
            project_root=root,
            installed=installed,
        )
    except (OSError, RuntimeDatabaseError, ValueError) as error:
        raise PublicationEvidenceError(
            "cutover_formal_writer_lock_config_mismatch"
        ) from error


def _isolated_database_access(db_path: Path) -> ResolvedDatabaseAccess:
    home = Path(pwd.getpwuid(os.getuid()).pw_dir).resolve(strict=True)
    try:
        installed = load_installed_writer_contract(required=False, home=home)
        return resolve_isolated_candidate(db_path, installed=installed)
    except (OSError, RuntimeDatabaseError) as error:
        raise PublicationEvidenceError("cutover_isolated_database_invalid") from error


def _cli_database_is_formal(db_path: Path) -> bool:
    """A different imported checkout must not hide the installed writer DB."""
    try:
        if is_formal_database_path(db_path):
            return True
    except RuntimeDatabaseError:
        # An invalid installed contract must not downgrade a possible formal
        # target to the isolated path.  The formal resolver below reports the
        # specific fail-closed contract error before DB or lock mutation.
        return True
    contract = _installed_writer(required=False)
    if contract is None:
        return False
    home, _payload = contract
    try:
        installed = load_installed_writer_contract(required=True, home=home)
        assert installed is not None
        return is_installed_formal_database(db_path, installed=installed)
    except (OSError, RuntimeDatabaseError) as error:
        raise PublicationEvidenceError(
            "cutover_installed_writer_database_invalid"
        ) from error


def _cli_writer_lock(root: Path, db_path: Path, *, formal: bool) -> Path:
    """Resolve the same lock as the installed Mac writer, never an override.

    ApiConfig.from_env consumes DCAR_WRITER_LOCK. The Mac renderer pins that
    value in the installed LaunchAgent, outside every checkout. Reading the
    plist here avoids importing api (and its global application startup) or
    sourcing writer.env, which is neither necessary nor a lock authority.
    """
    if not formal:
        return root / "runtime/writer-worker.lock"
    access = _installed_writer_access(root, db_path)
    try:
        observed = observe_writer_lock(access)
    except (OSError, RuntimeDatabaseError) as error:
        raise PublicationEvidenceError(
            "cutover_formal_writer_lock_missing_or_unsafe"
        ) from error
    if observed["held"]:
        raise PublicationEvidenceError("cutover_writer_lock_held")
    assert access.writer_lock is not None
    return access.writer_lock


def main(argv: Sequence[str] | None = None) -> int:
    """Explicit maintenance CLI; no provider, scheduler startup or migration.

    Example: python -m v8.pipeline_cutover --db <existing18-db>
      --project-root <sealed-release> --activation-run-id <actual-id>
      --report-task-id <manual-frozen-task> --scan-run-id <verified-id>
      --cutoff-at <explicit-cutoff>

    The owning writer can call record_cutover in-process. This separate CLI
    requires the installed LaunchAgent's DCAR_WRITER_LOCK and the same explicit
    environment value for a formal DB. A live writer makes it fail. Formal
    invocations use the actual clock and must not provide --at.
    """
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--db", type=Path, required=True)
    parser.add_argument("--project-root", type=Path, required=True)
    parser.add_argument(
        "--isolated-candidate",
        action="store_true",
        help="Explicitly authorize an existing non-installed offline database.",
    )
    parser.add_argument("--activate-only", action="store_true", help="Record the actual one-time activation without creating a report cutover.")
    parser.add_argument("--activation-run-id", type=int)
    parser.add_argument("--report-task-id", action="append")
    parser.add_argument("--scan-run-id", type=int, action="append")
    parser.add_argument("--cutoff-at")
    parser.add_argument("--at", help="Deterministic offline clock; forbidden for a formal database.")
    args = parser.parse_args(argv)
    root = args.project_root
    if not root.is_absolute() or root != root.resolve(strict=True):
        raise PublicationEvidenceError("cutover_project_root_not_canonical")
    formal = _cli_database_is_formal(args.db)
    if formal and args.isolated_candidate:
        raise PublicationEvidenceError("cutover_formal_database_cannot_be_isolated")
    if not formal and not args.isolated_candidate:
        raise PublicationEvidenceError("cutover_database_authority_unresolved")
    if formal and root != PROJECT_ROOT:
        raise PublicationEvidenceError("cutover_formal_writer_root_mismatch")
    if formal and args.at is not None:
        raise PublicationEvidenceError("formal_cutover_clock_override_forbidden")
    lock_path = _cli_writer_lock(root, args.db, formal=formal)
    if not lock_path.parent.is_dir() or any(path.is_symlink() for path in (lock_path, *lock_path.parents)):
        raise PublicationEvidenceError("cutover_writer_lock_unsafe")
    # Formal operations must not create a fresh inode in place of a missing
    # live-writer lock. Only isolated fixture roots may create their own lock.
    if formal:
        access = _installed_writer_access(root, args.db)
        try:
            with acquire_writer_lock(access):
                if args.activate_only:
                    if any((args.activation_run_id, args.report_task_id, args.scan_run_id, args.cutoff_at)):
                        parser.error("--activate-only cannot include a report cutover scope")
                    result = record_activation(db_path=access.database, at=args.at)
                else:
                    if not all((args.activation_run_id, args.report_task_id, args.scan_run_id, args.cutoff_at)):
                        parser.error("recording cutover requires --activation-run-id, --report-task-id, --scan-run-id and --cutoff-at")
                    result = record_cutover(db_path=access.database, activation_run_id=args.activation_run_id,
                        report_task_ids=args.report_task_id, scan_run_ids=args.scan_run_id,
                        cutoff_at=args.cutoff_at, at=args.at, project_root=root)
        except RuntimeDatabaseError as error:
            if "already held" in str(error):
                raise PublicationEvidenceError("cutover_writer_lock_held") from error
            raise PublicationEvidenceError(
                "cutover_formal_writer_lock_missing_or_unsafe"
            ) from error
        print(json.dumps(result, ensure_ascii=False, sort_keys=True))
        return 0

    _isolated_database_access(args.db)
    flags = os.O_RDWR | os.O_CLOEXEC | os.O_NOFOLLOW
    handle = os.open(lock_path, flags | os.O_CREAT, 0o600)
    acquired = False
    try:
        try:
            fcntl.flock(handle, fcntl.LOCK_EX | fcntl.LOCK_NB)
            acquired = True
        except BlockingIOError as error:
            raise PublicationEvidenceError("cutover_writer_lock_held") from error
        if (os.fstat(handle).st_dev, os.fstat(handle).st_ino) != (lock_path.stat().st_dev, lock_path.stat().st_ino):
            raise PublicationEvidenceError("cutover_writer_lock_replaced")
        if args.activate_only:
            if any((args.activation_run_id, args.report_task_id, args.scan_run_id, args.cutoff_at)):
                parser.error("--activate-only cannot include a report cutover scope")
            result = record_activation(db_path=args.db, at=args.at)
        else:
            if not all((args.activation_run_id, args.report_task_id, args.scan_run_id, args.cutoff_at)):
                parser.error("recording cutover requires --activation-run-id, --report-task-id, --scan-run-id and --cutoff-at")
            result = record_cutover(db_path=args.db, activation_run_id=args.activation_run_id,
                report_task_ids=args.report_task_id, scan_run_ids=args.scan_run_id,
                cutoff_at=args.cutoff_at, at=args.at, project_root=root)
    finally:
        if acquired:
            fcntl.flock(handle, fcntl.LOCK_UN)
        os.close(handle)
    print(json.dumps(result, ensure_ascii=False, sort_keys=True))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
