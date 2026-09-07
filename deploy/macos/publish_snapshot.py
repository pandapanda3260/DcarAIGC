#!/usr/bin/env python3
"""Publish a verified macOS-writer snapshot to the Ubuntu read replica."""

from __future__ import annotations

import argparse
import fcntl
import hashlib
import importlib.util
import json
import os
import re
import shlex
import shutil
import sqlite3
import stat
import subprocess
import sys
import tempfile
import time as clock_time
import urllib.error
import urllib.request
from contextlib import closing, contextmanager, nullcontext
from dataclasses import dataclass, replace
from datetime import date, datetime, time, timedelta, timezone
from pathlib import Path
from types import ModuleType
from typing import Any, Callable, Iterator, Mapping, Optional, Sequence
from zoneinfo import ZoneInfo


PACKAGE_ROOT = Path(__file__).resolve().parents[2] / "src/dcar_eval"
if str(PACKAGE_ROOT) not in sys.path:
    sys.path.insert(0, str(PACKAGE_ROOT))

from v8 import pipeline_cutover, runtime_database, runtime_receipts  # noqa: E402
from v8.runtime_paths import source_root  # noqa: E402
from v8.snapshot_contract import (  # noqa: E402
    ARTIFACT_POLICY, MANAGED_ORIGINALS_CONTRACT, descriptor, validate_descriptor,
)
from v8.storage import configure_connection_safety, require_schema_compatibility  # noqa: E402

SHANGHAI = ZoneInfo("Asia/Shanghai")
TERMINAL_REPORT_STATUSES = frozenset({"succeeded", "partial"})
SAFE_ALIAS_RE = re.compile(r"[A-Za-z0-9_.-]{1,64}")
SAFE_REMOTE_PATH_RE = re.compile(r"/[A-Za-z0-9_./-]+")
SNAPSHOT_ID_RE = re.compile(r"[0-9]{8}T[0-9]{6}Z-[0-9a-f]{12}")
SHA256_RE = re.compile(r"[0-9a-f]{64}")
RUNTIME_IDENTITY_SCHEMA = "dcar-runtime-identity-v1"
EXPECTED_REPORT_VERSION = "dcar-content-operations-report-v8.9"
EXPECTED_DATABASE_SCHEMA_VERSION = 19
EXPECTED_DATABASE_SCHEMA_MIGRATION = "dual-acquisition-profile-roster-v1"
SUPPORTED_SCHEMA_MIGRATIONS = {
    19: EXPECTED_DATABASE_SCHEMA_MIGRATION,
    20: "integrated-video-capture-v25",
}
EXPECTED_ACTIVE_RELEASE_ID = "evaluation-v9__selling-points-v5.2"
EXPECTED_ACTIVE_RELEASE_STATUS = "active"
EXPECTED_RULE_VERSION = "evaluation-v9"
EXPECTED_TAXONOMY_VERSION = "selling-points-v5.2"
EXPECTED_TAXONOMY_STATUS = "published"
RUNTIME_IDENTITY_KEYS = frozenset(
    {
        "schema",
        "report_version",
        "database_schema_version",
        "database_schema_migration",
        "active_release_id",
        "active_release_status",
        "rule_version",
        "taxonomy_version",
        "taxonomy_status",
        "matcher_rule_sha256",
    }
)
FRESHNESS_SCHEMA = "profile-day-publication-freshness-v1"
PUBLISHER_RECEIPT_SCHEMA = "dcar-snapshot-publisher-receipt-v2"
AUTOMATIC_STATE_SCHEMA = "dcar-automatic-snapshot-publisher-state-v2"
AUTOMATIC_STATE_FILENAME = "automatic-publisher-state-v2.json"
PENDING_STATE_SCHEMA = "dcar-snapshot-publisher-pending-v2"
PENDING_STATE_FILENAME = "snapshot-publisher-pending.json"
SOURCE_RECEIPT_CONTRACT = "snapshot-source-receipt-v1"
SOURCE_RECEIPT_FILENAME = "snapshot-source-receipt.json"
REMOTE_PROBE_SCHEMA = "dcar-remote-publisher-probe-v1"
LEGACY_TRANSITION_SCHEMA = "dcar-schema17-to18-server-transition-v1"
TRANSITION_SCHEMA = "dcar-schema18-to19-server-transition-v1"
INTEGRATED_TRANSITION_SCHEMA = "dcar-schema19-to20-server-transition-v1"
AUTOMATIC_START_HOUR = 9
WRITER_ENDPOINT_TIMEOUT_SECONDS = 120
REMOTE_ENDPOINT_TIMEOUT_SECONDS = 120
REMOTE_PROBE_COMMAND_TIMEOUT_SECONDS = 420
# Pre-publish pruning keeps two existing points; the new successful install
# becomes the third retained point.
REMOTE_SNAPSHOT_RETAIN_COUNT = 2
LOCAL_SNAPSHOT_RETAIN_COUNT = 3
LOCAL_SNAPSHOT_DIR_RE = re.compile(r"snapshot-[0-9]{8}T[0-9]{6}Z")
AUTOMATIC_STATE_KEYS = frozenset(
    {
        "schema",
        "beijing_date",
        "snapshot_id",
        "published_at",
        "database_sha256",
        "runtime_identity",
        "snapshot_contract",
        "publication_evidence_sha256",
        "publisher_receipt_sha256",
        "output_name",
    }
)
PENDING_STATE_KEYS = frozenset(
    {
        "schema",
        "beijing_date",
        "snapshot_id",
        "database_sha256",
        "runtime_identity",
        "previous_snapshot_id",
        "previous_database_sha256",
        "previous_manifest_sha256",
        "previous_runtime_identity",
        "snapshot_contract",
        "previous_snapshot_contract",
        "output_name",
        "receipt",
        "receipt_sha256",
    }
)
ALLOWED_ENV_KEYS = frozenset(
    {
        "DCAR_PUBLISH_SSH_ALIAS",
        "DCAR_PUBLISH_REMOTE_PROJECT_ROOT",
        "DCAR_PUBLISH_REMOTE_STATE_ROOT",
        "DCAR_PUBLISH_REMOTE_PYTHON",
        "DCAR_PUBLISH_SNAPSHOT_ROOT",
        "DCAR_PUBLISH_MIN_REMOTE_FREE_BYTES",
        "DCAR_PUBLISH_EXPECTED_USER_VERSION",
        "DCAR_PUBLISH_MAX_CONTENT_LAG_DAYS",
    }
)


class SnapshotPublishError(RuntimeError):
    """A publish was refused before changing the active read replica."""


@dataclass(frozen=True)
class PublishConfig:
    ssh_alias: str
    remote_project_root: str
    remote_state_root: str
    remote_python: str
    snapshot_root: Path
    minimum_remote_free_bytes: int
    expected_user_version: int
    maximum_content_lag_days: int

    @property
    def remote_active_cache_root(self) -> str:
        return self.remote_state_root + "/cache"

    @property
    def remote_active_reports_root(self) -> str:
        return self.remote_state_root + "/reports"

    @property
    def remote_incoming_root(self) -> str:
        return self.remote_state_root + "/incoming"

    @property
    def remote_installer(self) -> str:
        return self.remote_project_root + "/deploy/server/install_snapshot.py"


@dataclass(frozen=True)
class WriterFreshness:
    evidence: dict[str, Any]
    latest_published_at: Optional[str]
    content_count: int
    runtime_identity: dict[str, Any]
    snapshot_contract: dict[str, str]

    @property
    def daily_report_status(self) -> Optional[str]:
        return next((item["status"] for item in self.evidence["reports"] if item["task_type"] == "daily"), None)

    @property
    def weekly_report_status(self) -> Optional[str]:
        return next((item["status"] for item in self.evidence["reports"] if item["task_type"] == "weekly"), None)


@dataclass(frozen=True)
class FormalReadObservation:
    database: Path
    database_identity: dict[str, Any]
    writer_lock: dict[str, Any]
    freshness: WriterFreshness


CommandRunner = Callable[..., subprocess.CompletedProcess[str]]
JsonFetcher = Callable[[str], dict[str, Any]]
BuildSnapshot = Callable[..., dict[str, Any]]


@dataclass(frozen=True)
class RsyncTransferPlan:
    byte_size: int
    changed_paths: tuple[str, ...]


def _utc_now() -> str:
    return (
        datetime.now(timezone.utc).isoformat(timespec="seconds").replace("+00:00", "Z")
    )


def _validate_runtime_identity(value: object, *, label: str,
                               expected_schema: Optional[int] = None) -> dict[str, Any]:
    if not isinstance(value, dict) or set(value) != RUNTIME_IDENTITY_KEYS:
        raise SnapshotPublishError(f"{label} runtime identity has an invalid shape")
    version = value.get("database_schema_version") if expected_schema is None else expected_schema
    if type(version) is not int or version not in SUPPORTED_SCHEMA_MIGRATIONS:
        raise SnapshotPublishError(f"{label} requires explicit schema 19 or 20")
    expected = {
        "schema": RUNTIME_IDENTITY_SCHEMA,
        "report_version": EXPECTED_REPORT_VERSION,
        "database_schema_version": version,
        "database_schema_migration": SUPPORTED_SCHEMA_MIGRATIONS[version],
        "active_release_id": EXPECTED_ACTIVE_RELEASE_ID,
        "active_release_status": EXPECTED_ACTIVE_RELEASE_STATUS,
        "rule_version": EXPECTED_RULE_VERSION,
        "taxonomy_version": EXPECTED_TAXONOMY_VERSION,
        "taxonomy_status": EXPECTED_TAXONOMY_STATUS,
    }
    for key, expected_value in expected.items():
        if value.get(key) != expected_value:
            raise SnapshotPublishError(
                f"{label} runtime identity mismatch for {key}: "
                f"{value.get(key)!r}, expected {expected_value!r}"
            )
    matcher_sha = value.get("matcher_rule_sha256")
    if not isinstance(matcher_sha, str) or SHA256_RE.fullmatch(matcher_sha) is None:
        raise SnapshotPublishError(
            f"{label} runtime identity has an invalid matcher_rule_sha256"
        )
    return dict(value)


def _database_runtime_identity(connection: sqlite3.Connection, *,
                               expected_schema: int = EXPECTED_DATABASE_SCHEMA_VERSION) -> dict[str, Any]:
    user_version = int(connection.execute("PRAGMA user_version").fetchone()[0])
    try:
        migration_rows = connection.execute(
            "SELECT name FROM schema_migrations WHERE version=?", (user_version,)
        ).fetchall()
        max_migration = connection.execute(
            "SELECT MAX(version) FROM schema_migrations"
        ).fetchone()[0]
        release_rows = connection.execute(
            """
            SELECT er.id,er.rule_version,er.taxonomy_version,
                   er.matcher_rule_sha256,er.status release_status,
                   tv.status taxonomy_status
            FROM evaluation_releases er
            JOIN taxonomy_versions tv ON tv.version=er.taxonomy_version
            WHERE er.status='active'
            ORDER BY er.id
            """
        ).fetchall()
    except sqlite3.Error as exc:
        raise SnapshotPublishError(
            "writer database lacks the required runtime identity tables"
        ) from exc
    if len(migration_rows) != 1 or max_migration != user_version:
        raise SnapshotPublishError(
            "writer database has an ambiguous schema migration identity"
        )
    if len(release_rows) != 1:
        raise SnapshotPublishError(
            "writer database must have exactly one active evaluation release"
        )
    release = release_rows[0]
    return _validate_runtime_identity(
        {
            "schema": RUNTIME_IDENTITY_SCHEMA,
            "report_version": EXPECTED_REPORT_VERSION,
            "database_schema_version": user_version,
            "database_schema_migration": str(migration_rows[0]["name"]),
            "active_release_id": str(release["id"]),
            "active_release_status": str(release["release_status"]),
            "rule_version": str(release["rule_version"]),
            "taxonomy_version": str(release["taxonomy_version"]),
            "taxonomy_status": str(release["taxonomy_status"]),
            "matcher_rule_sha256": str(release["matcher_rule_sha256"]),
        },
        label="writer database", expected_schema=expected_schema,
    )


def _parse_iso(value: Any, *, label: str) -> datetime:
    if not isinstance(value, str) or not value:
        raise SnapshotPublishError(f"{label} is missing")
    try:
        parsed = datetime.fromisoformat(value.replace("Z", "+00:00"))
    except ValueError as exc:
        raise SnapshotPublishError(f"{label} is invalid") from exc
    if parsed.tzinfo is None:
        raise SnapshotPublishError(f"{label} must include a timezone")
    return parsed


def _validate_snapshot_contract(value: object, *, label: str) -> dict[str, str]:
    try:
        return validate_descriptor(value)
    except ValueError as error:
        raise SnapshotPublishError(f"{label} snapshot consumer contract mismatch") from error


def _require_current_config(config: PublishConfig) -> None:
    if config.expected_user_version not in SUPPORTED_SCHEMA_MIGRATIONS:
        raise SnapshotPublishError(
            "normal publisher requires explicit schema 19 or 20; "
            "the first version transition must use schema-upgrade"
        )


def _preparation_dependency(connection: sqlite3.Connection, *, current: datetime, at: str) -> dict[str, Any]:
    expected = datetime.combine(current.date(), time(7, 30), SHANGHAI)
    candidates = []
    for record in connection.execute("SELECT * FROM scheduler_runs WHERE job_id='daily_pipeline_summary' ORDER BY id"):
        details = json.loads(record["details_json"])
        identity = details.get("identity", {})
        if identity.get("at") and _parse_iso(identity["at"], label="pipeline summary scope") == expected:
            candidates.append((dict(record), details))
    if len(candidates) != 1:
        raise SnapshotPublishError("today's real 07:30 pipeline preparation receipt is missing or ambiguous")
    row, details = candidates[0]
    terminal = pipeline_cutover.terminal_run(connection, row, at=at, statuses=frozenset({"succeeded"}))
    summary = details.get("summary", {})
    if (details.get("contract_version") != "durable-run-v1"
            or details["identity"].get("version") != "matrix-first-pipeline-v1"
            or summary.get("contract_version") != "matrix-first-pipeline-v1"
            or summary.get("captured_at") != details["identity"]["at"]
            or not isinstance(summary.get("discovery_coverage"), dict)):
        raise SnapshotPublishError("pipeline preparation has no versioned discovery observation")
    # The 07:30 summary is an observation, not an upstream success flag. Its
    # original state is kept; independently verified scan/report facts decide
    # publication, including explicitly partial late-arrival reports.
    return {**terminal, "scheduled_at": details["identity"]["at"], "observation": summary}


def _report_run_dependency(connection: sqlite3.Connection, row: Mapping[str, Any], *, at: str,
                           project_root: Path) -> dict[str, Any]:
    terminal = pipeline_cutover.terminal_run(connection, row, at=at)
    details = json.loads(row["details_json"])
    task_id = details.get("task_id")
    if not isinstance(task_id, str) or not task_id:
        raise SnapshotPublishError("report scheduler receipt lacks a bound task_id")
    dependency = pipeline_cutover.report_dependency(connection, task_id, at=at, project_root=project_root)
    if dependency["status"] != row["status"] or details.get("task_status") != row["status"]:
        raise SnapshotPublishError("report terminal status does not match its task and frozen inputs")
    return {**dependency, "scheduler": terminal, "scheduled_for": row["scheduled_for"]}


def _reconcile_from(scheduler: Mapping[str, Any]) -> date | None:
    value = scheduler.get("reconcile_from")
    if value is None:
        return None
    try:
        boundary = date.fromisoformat(value)
    except (TypeError, ValueError) as error:
        raise SnapshotPublishError("writer reconcile_from is invalid") from error
    if boundary.isoformat() != value:
        raise SnapshotPublishError("writer reconcile_from is not canonical")
    return boundary


def _normal_report_dependencies(connection: sqlite3.Connection, *, current: datetime, at: str,
                                project_root: Path, reconcile_from: date | None = None) -> list[dict[str, Any]]:
    result = []
    jobs = [("daily_report", 0, 1)]
    if current.weekday() == 0:
        jobs.append(("weekly_report", 30, 7))
    for job_id, minute, days in jobs:
        if reconcile_from is not None and current.date() - timedelta(days=days) < reconcile_from:
            continue
        scheduled = datetime.combine(current.date(), time(8, minute), SHANGHAI)
        row = connection.execute("SELECT * FROM scheduler_runs WHERE job_id=? AND julianday(scheduled_for)=julianday(?)",
                                 (job_id, scheduled.isoformat())).fetchone()
        _validate_today_run(row, job_id=job_id, hour=8, minute=minute, allowed_statuses=TERMINAL_REPORT_STATUSES, current=current)
        assert row is not None
        dependency = _report_run_dependency(connection, dict(row), at=at, project_root=project_root)
        if (dependency["creation_source"] != "automatic" or dependency["task_type"] != ("daily" if days == 1 else "weekly")
                or dependency["period_start"] != (current.date() - timedelta(days=days)).isoformat()
                or dependency["period_end"] != (current.date() - timedelta(days=1)).isoformat()
                or _parse_iso(dependency["cutoff_at"], label="report cutoff") != scheduled):
            raise SnapshotPublishError("report frozen period/cutoff does not match the scheduler occurrence")
        result.append(dependency)
    return result


def _run_observation(connection: sqlite3.Connection, row: sqlite3.Row, *, at: str) -> dict[str, Any]:
    """A failed business run is publishable; a forged terminal flag is not."""
    if row["status"] in {"succeeded", "partial", "failed", "interrupted", "cancelled", "skipped"}:
        pipeline_cutover.terminal_run(connection, dict(row), at=at, statuses=frozenset({row["status"]}))
    return {**{key: row[key] for key in (
        "id", "job_id", "scheduled_for", "status", "started_at", "completed_at"
    )}, "details_sha256": pipeline_cutover.digest(json.loads(row["details_json"]))}


def _observed_publication_evidence(
    connection: sqlite3.Connection, *, current: datetime, at: str,
    project_root: Path, boundary: date,
) -> dict[str, Any]:
    """Publish actual forward progress without claiming unavailable reports are complete."""
    reports: list[dict[str, Any]] = []
    observations: list[dict[str, Any]] = []
    reasons: list[str] = []
    jobs = [("daily_report", 0, 1)] + ([("weekly_report", 30, 7)] if current.weekday() == 0 else [])
    for job_id, minute, days in jobs:
        period_start = current.date() - timedelta(days=days)
        if period_start < boundary:
            continue
        scheduled = datetime.combine(current.date(), time(8, minute), SHANGHAI)
        expected = {"job_id": job_id, "scheduled_for": scheduled.isoformat(),
                    "period_start": period_start.isoformat(),
                    "period_end": (current.date() - timedelta(days=1)).isoformat()}
        if current < scheduled:
            observations.append({**expected, "status": "not_due"})
            reasons.append(job_id + "_not_due")
            continue
        row = connection.execute(
            "SELECT * FROM scheduler_runs WHERE job_id=? AND julianday(scheduled_for)=julianday(?)",
            (job_id, scheduled.isoformat()),
        ).fetchone()
        if row is None:
            observations.append({**expected, "status": "missing"})
            reasons.append(job_id + "_missing")
            continue
        observed = _run_observation(connection, row, at=at)
        observations.append({**expected, "status": row["status"], "run": observed})
        if row["status"] in TERMINAL_REPORT_STATUSES:
            dependency = _report_run_dependency(connection, dict(row), at=at, project_root=project_root)
            if (dependency["creation_source"] != "automatic"
                    or dependency["task_type"] != ("daily" if days == 1 else "weekly")
                    or dependency["period_start"] != expected["period_start"]
                    or dependency["period_end"] != expected["period_end"]
                    or _parse_iso(dependency["cutoff_at"], label="report cutoff") != scheduled):
                raise SnapshotPublishError("report frozen period/cutoff does not match the scheduler occurrence")
            reports.append(dependency)
        if row["status"] != "succeeded":
            reasons.append(job_id + "_" + str(row["status"]))
    discovery: dict[str, Any]
    if current.date() - timedelta(days=1) < boundary:
        discovery = {"contract_version": pipeline_cutover.PROFILE_DAY_DISCOVERY_CONTRACT,
                     "coverage": {"complete": False, "partial_publishable": False,
                                  "reason": "first_report_not_due"}}
        reasons.append("first_report_not_due")
    else:
        try:
            discovery = pipeline_cutover.runtime_evidence(connection, at=at)
        except pipeline_cutover.PublicationEvidenceError as error:
            if str(error) not in {
                "publication_current_profile_day_receipt_missing", "publication_runtime_coverage_not_publishable",
            }:
                raise
            # This still validates the native receipt lineage. Missing coverage
            # remains unknown; no complete/partial-publishable flag is invented.
            discovery = {"contract_version": pipeline_cutover.PROFILE_DAY_DISCOVERY_CONTRACT,
                         "coverage": runtime_receipts.latest_runtime_coverage(connection, at=at)}
            reasons.append(str(error))
        if discovery["coverage"].get("complete") is not True:
            reasons.append("discovery_incomplete")
    since = datetime.combine(boundary, time.min, SHANGHAI).isoformat()
    content = connection.execute(
        "SELECT COUNT(*) row_count,MAX(id) max_id,MAX(updated_at) updated_at "
        "FROM content_items WHERE julianday(published_at)>=julianday(?)", (since,),
    ).fetchone()
    metrics = {}
    for table in ("content_metric_observations", "account_metric_observations"):
        row = connection.execute(
            f"SELECT COUNT(*) row_count,MAX(id) max_id FROM {table} WHERE julianday(captured_at)>=julianday(?)", (since,),
        ).fetchone()
        metrics[table] = dict(row)
    runs = [{**{key: row[key] for key in ("id", "job_id", "scheduled_for", "status", "started_at", "completed_at")},
             "details_sha256": hashlib.sha256(str(row["details_json"]).encode()).hexdigest()}
            for row in connection.execute(
        "SELECT id,job_id,scheduled_for,status,started_at,completed_at,details_json FROM scheduler_runs "
        "WHERE julianday(started_at)>=julianday(?) ORDER BY id", (since,),
    )]
    tasks = [dict(row) for row in connection.execute(
        "SELECT id,task_status,progress,message,updated_at FROM report_tasks WHERE period_start>=? ORDER BY id",
        (boundary.isoformat(),),
    )]
    return {
        "schema": FRESHNESS_SCHEMA, "beijing_date": current.date().isoformat(), "verified_at": at,
        "mode": "observed", "reconcile_from": boundary.isoformat(), "cutover": None,
        "discovery": discovery, "preparation": None, "reports": reports,
        "report_observations": observations,
        "first_daily_report_due_at": datetime.combine(boundary + timedelta(days=1), time(8), SHANGHAI).isoformat(),
        "current_observation": {"content": dict(content), "metrics": metrics,
                                "scheduler_sha256": pipeline_cutover.digest(runs),
                                "tasks_sha256": pipeline_cutover.digest(tasks)},
        "status": "partial" if reasons else "succeeded", "reasons": sorted(set(reasons)),
    }


def _validate_publication_evidence(value: object) -> dict[str, Any]:
    if not isinstance(value, dict) or value.get("schema") != FRESHNESS_SCHEMA:
        raise SnapshotPublishError("publication freshness receipt contract is invalid")
    verified = _parse_iso(value.get("verified_at"), label="publication verified_at")
    if (verified.astimezone(SHANGHAI).date().isoformat() != value.get("beijing_date")
            or value.get("status") not in TERMINAL_REPORT_STATUSES
            or (not value.get("reports") and value.get("mode") != "observed")):
        raise SnapshotPublishError("publication freshness day/status/reports are invalid")
    if value.get("mode") == "scheduled":
        if (value.get("cutover") is not None or not isinstance(value.get("discovery"), dict)
                or value["discovery"].get("contract_version") not in {
                    "matrix-publication-discovery-v1",
                    pipeline_cutover.PROFILE_DAY_DISCOVERY_CONTRACT,
                }
                or not isinstance(value.get("preparation"), dict)
                or value["preparation"].get("job_id") != "daily_pipeline_summary"):
            raise SnapshotPublishError("publication freshness has no real scan/preparation dependencies")
    elif value.get("mode") == "observed":
        if (_reconcile_from(value) is None or value.get("cutover") is not None
                or not isinstance(value.get("current_observation"), dict)
                or not isinstance(value.get("report_observations"), list)
                or not isinstance(value.get("reports"), list)
                or not isinstance(value.get("discovery"), dict)
                or not isinstance(value.get("reasons"), list)
                or (value["status"] == "partial" and not value["reasons"])):
            raise SnapshotPublishError("observed publication evidence is invalid")
    elif value.get("mode") == "cutover":
        cutover = value.get("cutover")
        if (not isinstance(cutover, dict) or cutover.get("contract_version") != pipeline_cutover.CONTRACT_VERSION
                or cutover.get("payload", {}).get("beijing_date") != value["beijing_date"]
                or value.get("reports") != cutover["payload"].get("reports")):
            raise SnapshotPublishError("publication cutover is not bound to the first-day report")
    else:
        raise SnapshotPublishError("publication freshness mode is unsupported")
    boundary = _reconcile_from(value)
    for report in value["reports"]:
        if (report.get("status") not in TERMINAL_REPORT_STATUSES
                or not isinstance(report.get("input_sha256"), str) or SHA256_RE.fullmatch(report["input_sha256"]) is None
                or not isinstance(report.get("scope_sha256"), str) or SHA256_RE.fullmatch(report["scope_sha256"]) is None
                or not report.get("report_file") or not report.get("files")
                or (report["status"] == "partial" and not report.get("partial_reasons"))):
            raise SnapshotPublishError("publication report dependency is invalid")
        if boundary is not None:
            try:
                period_start = date.fromisoformat(report["period_start"])
            except (KeyError, TypeError, ValueError) as error:
                raise SnapshotPublishError("publication report period is invalid") from error
            if period_start < boundary:
                raise SnapshotPublishError("publication report predates reconcile_from")
    return value


def _schema20_deployment(connection: sqlite3.Connection, *, project_root: Path) -> dict[str, Any]:
    from v8.capture_release import _release_tools

    return dict(_release_tools().validate_deployment_receipt(connection, project_root=project_root))


def _schema20_installed_bindings(deployment: Mapping[str, Any], *, project_root: Path) -> tuple[dict[str, Any], dict[str, Any]]:
    """Read the actual installed generation without taking the Writer lease."""
    from v8 import capture_release, forward_recovery

    installed = runtime_database.load_installed_writer_contract(required=True)
    if installed is None or installed.project_root.resolve() != project_root.resolve():
        raise SnapshotPublishError("schema20 successor installed project differs")
    environment = installed.payload.get("EnvironmentVariables")
    if not isinstance(environment, dict):
        raise SnapshotPublishError("schema20 installed build environment is missing")
    # The installed wrapper forbids a preset ID and derives it from this file.
    # Publisher is a separate process, so derive the same identity from the
    # installed path, then retain the private-envelope and lineage checks.
    receipt_name = environment.get("DCAR_LOADED_BUILD_RECEIPT")
    if (environment.get("DCAR_LOADED_BUILD_ID") or not isinstance(receipt_name, str)
            or not Path(receipt_name).is_absolute()):
        raise SnapshotPublishError("schema20 installed sealed build receipt is missing or overridden")
    receipt_path = Path(receipt_name)
    identity = _file_identity(receipt_path, label="installed sealed build receipt")
    loaded_sha = _sha256_regular_file(receipt_path, expected_identity=identity)
    build = forward_recovery._private_receipt(receipt_path, loaded_sha, "sealed-build-receipt-v1")
    runtime = forward_recovery._private_receipt(Path(build["runtime_root_receipt"]["path"]),
        build["runtime_root_receipt"]["sha256"], "runtime-root-binding-v1")
    bindings = {"build_sha256": loaded_sha, "runtime_sha256": build["runtime_root_receipt"]["sha256"],
        "config_sha256": deployment["bindings"]["config_sha256"]}
    decision = deployment.get("release_decision")
    if decision is not None and (decision.get("runtime_bindings") != bindings
            or any(decision.get("runtime_evidence", {}).get("build", {}).get(key) != value
                   for key, value in (("path", str(receipt_path)), ("sha256", loaded_sha)))
            or any(decision.get("runtime_evidence", {}).get("runtime", {}).get(key) != build["runtime_root_receipt"][key]
                   for key in ("path", "sha256"))):
        raise SnapshotPublishError("schema20 installed generation differs from the release decision")
    forward_recovery._successor_archive(build, live=True)
    if (build["schema_contract"]["formal_schema"] != 20 or build["schema_contract"]["code_schema"] != 20
            or build["source_archive"]["sha256"] != deployment["evidence"]["source_archive"]["sha256"]
            or any(build["postmigration_lineage"][key]["sha256"] != deployment["evidence"][evidence_key]["sha256"]
                   for key, evidence_key in (("install_receipt", "install"), ("migration_receipt", "migration")))):
        raise SnapshotPublishError("schema20 successor sealed migration/source pair differs")
    install = capture_release._private_json(deployment["evidence"]["install"])
    actual = installed.database.stat()
    if (install["formal_database"] != str(installed.database.resolve())
            or (install["installed"]["file"]["device"], install["installed"]["file"]["inode"]) != (actual.st_dev, actual.st_ino)
            or any(runtime["formal_database"][key] != value for key, value in (
                ("device", actual.st_dev), ("inode", actual.st_ino)))
            or runtime["installed_runtime"]["database"]["path"] != str(installed.database.resolve())):
        raise SnapshotPublishError("schema20 successor installed database generation differs")
    return bindings, dict(forward_recovery._route())


def _schema20_publication_evidence(connection: sqlite3.Connection, *, current: datetime,
                                   at: str, project_root: Path) -> dict[str, Any]:
    """A real bounded deployment does not imply a complete profile day."""
    from v8.capture_code_successor import current_proof
    from v8.profile_activations import activation_at

    code_successor = current_proof(connection, project_root=project_root, at=at)
    deployment = _schema20_deployment(connection, project_root=project_root)
    active = activation_at(connection, at)
    if active is None:
        raise SnapshotPublishError("schema20 current activation is missing")
    successor = None
    if any(deployment["bindings"].get(key) != active[key] for key in (
        "activation_id", "profile_id", "roster_snapshot_id", "roster_members_sha256", "activation_sha256",
    )):
        if active["profile_id"] != "integrated_route_v1":
            raise SnapshotPublishError("schema20 deployment does not bind the current activation")
        from v8.capture_activation_release import validate_installed_activation_successor
        if code_successor is None:
            bindings, manifest = _schema20_installed_bindings(deployment, project_root=project_root)
        else:
            # The historical RELEASE remains bound to its original generation.
            # current_proof separately verifies the new live generation.
            bindings, manifest = code_successor["origin_runtime_bindings"], code_successor["manifest"]
        successor = validate_installed_activation_successor(connection, source_deployment=deployment,
            current_active=active, runtime_bindings=bindings, manifest=manifest, at=at)
    boundary = _parse_iso(active["effective_at"], label="active profile effective_at").astimezone(SHANGHAI).date()
    evidence = _observed_publication_evidence(connection, current=current, at=at,
        project_root=project_root, boundary=boundary)
    # Only immutable DB identity is added: project-external release evidence is
    # verified by the deployment validator, not misclassified as report files.
    evidence["deployment_readiness"] = {key: deployment[key] for key in (
        "contract_version", "deployment_id", "status", "receipt_sha256", "coverage_complete",
    )}
    if successor is not None:
        evidence["activation_successor"] = successor
    if code_successor is not None:
        evidence["code_successor"] = code_successor
    return evidence


def _publication_fingerprint(evidence: Mapping[str, Any]) -> str:
    """Ignore only observation times; every bound report/coverage revision counts."""
    stable = dict(evidence)
    stable.pop("verified_at", None)
    discovery = stable.get("discovery")
    if isinstance(discovery, dict):
        stable["discovery"] = {key: value for key, value in discovery.items() if key != "verified_at"}
    return pipeline_cutover.digest(stable)


def _snapshot_database_manifest_item(manifest: Mapping[str, Any]) -> dict[str, Any]:
    databases = manifest.get("databases")
    if not isinstance(databases, list) or not all(
        isinstance(item, dict) for item in databases
    ):
        raise SnapshotPublishError("snapshot database manifest is invalid")
    matches = [item for item in databases if item.get("name") == "dcar_insight.sqlite3"]
    if (
        len(matches) != 1
        or matches[0].get("bundle_path") != "databases/dcar_insight.sqlite3"
    ):
        raise SnapshotPublishError("snapshot database manifest is invalid")
    return matches[0]


def _verify_snapshot_dependencies(output: Path, manifest: Mapping[str, Any], freshness: WriterFreshness,
                                  *, project_root: Path) -> None:
    """Bind preflight evidence to the exact detached database being sent."""
    item = _snapshot_database_manifest_item(manifest)
    path = output / item["bundle_path"]
    try:
        pipeline_cutover.verify_file({"path": str(path), "sha256": item["sha256"], "byte_size": item["byte_size"]})
        connection = sqlite3.connect(f"{path.as_uri()}?mode=ro", uri=True)
        connection.row_factory = sqlite3.Row
        try:
            configure_connection_safety(connection)
            connection.execute("PRAGMA query_only=ON")
            require_schema_compatibility(
                connection,
                supported_versions=frozenset({freshness.runtime_identity["database_schema_version"]}),
            )
            version = freshness.runtime_identity["database_schema_version"]
            if _database_runtime_identity(connection, expected_schema=version) != freshness.runtime_identity:
                raise SnapshotPublishError("snapshot database runtime identity drifted")
            evidence = freshness.evidence
            at = evidence["verified_at"]
            if version == 20:
                if _schema20_deployment(connection, project_root=project_root) != manifest.get("deployment_readiness"):
                    raise SnapshotPublishError("snapshot schema20 deployment/migration evidence drifted")
                if _schema20_publication_evidence(connection,
                    current=_parse_iso(at, label="snapshot verification time").astimezone(SHANGHAI),
                    at=at, project_root=project_root) != evidence:
                    raise SnapshotPublishError("snapshot schema20 current observations drifted")
                if evidence.get("code_successor") != manifest.get("code_successor"):
                    raise SnapshotPublishError("snapshot schema20 code successor drifted")
            elif evidence.get("mode") == "observed":
                boundary = _reconcile_from(evidence)
                if boundary is None or _observed_publication_evidence(
                    connection, current=_parse_iso(at, label="snapshot verification time").astimezone(SHANGHAI),
                    at=at, project_root=project_root, boundary=boundary,
                ) != evidence:
                    raise SnapshotPublishError("snapshot current observations drifted")
            elif evidence["cutover"] is not None:
                actual = pipeline_cutover.verify_cutover(connection, run_id=evidence["cutover"]["terminal"]["run_id"], at=at, project_root=project_root)
                if actual != evidence["cutover"]:
                    raise SnapshotPublishError("snapshot cutover evidence drifted")
            else:
                if pipeline_cutover.runtime_evidence(connection, at=at) != evidence["discovery"]:
                    raise SnapshotPublishError("snapshot discovery evidence drifted")
                current = _parse_iso(at, label="snapshot verification time").astimezone(SHANGHAI)
                if _preparation_dependency(connection, current=current, at=at) != evidence["preparation"]:
                    raise SnapshotPublishError("snapshot preparation evidence drifted")
                boundary = _reconcile_from(evidence)
                if _normal_report_dependencies(connection, current=current, at=at, project_root=project_root,
                                               reconcile_from=boundary) != evidence["reports"]:
                    raise SnapshotPublishError("snapshot report dependencies drifted")
            included = {item["project_path"]: item for item in manifest["files"]}

            def require_references(value: Any) -> None:
                if isinstance(value, dict):
                    name, digest = value.get("path"), value.get("sha256")
                    if value.get("raw_path") is not None:
                        name, digest = value["raw_path"], value.get("raw_sha256")
                    if isinstance(name, str) and isinstance(digest, str):
                        reference = Path(name)
                        if reference.is_absolute():
                            reference = reference.relative_to(project_root)
                        item = included.get(reference.as_posix())
                        if item is None or item["sha256"] != digest:
                            raise SnapshotPublishError("snapshot omits a hash-bound publication evidence file")
                        size = value.get("raw_byte_size") if value.get("raw_path") is not None else value.get("byte_size")
                        if size is not None and item["byte_size"] != size:
                            raise SnapshotPublishError("snapshot publication evidence byte size changed")
                    for child in value.values():
                        require_references(child)
                elif isinstance(value, list):
                    for child in value:
                        require_references(child)

            # This exact proof was revalidated above and is bound independently
            # in the manifest. Its private files are never business artifacts.
            business_evidence = dict(freshness.evidence)
            if version == 20:
                business_evidence.pop("code_successor", None)
            require_references(business_evidence)
        finally:
            connection.close()
    except (ValueError, RuntimeError, OSError, sqlite3.Error, KeyError, TypeError) as error:
        if isinstance(error, SnapshotPublishError):
            raise
        raise SnapshotPublishError(f"snapshot publication dependency verification failed: {error}") from error


def _validate_manifest_contract(manifest: Mapping[str, Any], *, project_root: Path) -> None:
    if manifest.get("artifact_policy") != ARTIFACT_POLICY:
        raise SnapshotPublishError("snapshot builder did not return the required thin-server-v2 policy")
    _validate_snapshot_contract(manifest.get("snapshot_contract"), label="snapshot manifest")
    if manifest.get("writer_project_root") != str(project_root.resolve()):
        raise SnapshotPublishError("snapshot writer project root does not match the publication source")
    managed = manifest.get("managed_originals")
    if (not isinstance(managed, dict) or managed.get("contract_version") != MANAGED_ORIGINALS_CONTRACT
            or not isinstance(managed.get("bundles"), list)):
        raise SnapshotPublishError("snapshot managed-originals disposition contract is invalid")
    transferred = set()
    for key in ("files", "optional_reuse_files"):
        if not isinstance(manifest.get(key), list):
            raise SnapshotPublishError("snapshot artifact collection is invalid")
        for row in manifest[key]:
            name = row.get("project_path")
            if not isinstance(name, str) or not name or name in transferred:
                raise SnapshotPublishError("snapshot artifact identity is duplicated or invalid")
            transferred.add(name)
    originals = set()
    for bundle in managed["bundles"]:
        if not isinstance(bundle, dict) or not isinstance(bundle.get("members"), list):
            raise SnapshotPublishError("snapshot managed-originals bundle is invalid")
        for member in bundle["members"]:
            name = member.get("project_path")
            if not isinstance(name, str) or not name or name in transferred or name in originals:
                raise SnapshotPublishError("managed original was transferred, reused, or duplicated")
            originals.add(name)
    _snapshot_database_manifest_item(manifest)


def _require_regular_local_file(path: Path, *, label: str) -> Path:
    candidate = path.expanduser()
    if candidate.is_symlink() or not candidate.is_file():
        raise SnapshotPublishError(f"{label} must be a regular non-symlink file")
    if candidate.stat().st_size <= 0:
        raise SnapshotPublishError(f"{label} is empty")
    return candidate.resolve()


def _file_identity(path: Path, *, label: str) -> dict[str, Any]:
    """Read one regular-file identity without following or locking it."""
    flags = os.O_RDONLY | getattr(os, "O_CLOEXEC", 0) | getattr(os, "O_NOFOLLOW", 0)
    try:
        descriptor_number = os.open(path, flags)
    except OSError as error:
        raise SnapshotPublishError(f"{label} is missing or unsafe") from error
    try:
        opened = os.fstat(descriptor_number)
        current = path.stat()
    finally:
        os.close(descriptor_number)
    if (
        not stat.S_ISREG(opened.st_mode)
        or (opened.st_dev, opened.st_ino) != (current.st_dev, current.st_ino)
        or current.st_uid != os.getuid()
        or stat.S_IMODE(current.st_mode) & 0o022
    ):
        raise SnapshotPublishError(f"{label} identity or permissions are unsafe")
    return {
        "canonical_path": str(path.resolve(strict=True)),
        "device": int(current.st_dev),
        "inode": int(current.st_ino),
        "nlink": int(current.st_nlink),
    }


def _sha256_regular_file(path: Path, *, expected_identity: Mapping[str, Any]) -> str:
    """Hash one already-authorized inode and reject replacement or in-read drift."""
    flags = os.O_RDONLY | getattr(os, "O_CLOEXEC", 0) | getattr(os, "O_NOFOLLOW", 0)
    try:
        descriptor_number = os.open(path, flags)
    except OSError as error:
        raise SnapshotPublishError("formal writer database changed before source sealing") from error
    try:
        before = os.fstat(descriptor_number)
        if (before.st_dev, before.st_ino, before.st_nlink) != (
            expected_identity["device"],
            expected_identity["inode"],
            expected_identity["nlink"],
        ):
            raise SnapshotPublishError(
                "formal writer database identity changed before source sealing"
            )
        digest = hashlib.sha256()
        while True:
            block = os.read(descriptor_number, 1024 * 1024)
            if not block:
                break
            digest.update(block)
        after = os.fstat(descriptor_number)
    finally:
        os.close(descriptor_number)
    current = path.stat()
    stable_fields = ("st_dev", "st_ino", "st_nlink", "st_size", "st_mtime_ns")
    if any(getattr(before, name) != getattr(after, name) for name in stable_fields) or (
        after.st_dev,
        after.st_ino,
        after.st_nlink,
    ) != (current.st_dev, current.st_ino, current.st_nlink):
        raise SnapshotPublishError(
            "formal writer database changed while the source receipt was sealed"
        )
    return digest.hexdigest()


def _validate_runtime_database_identity(
    value: object, *, label: str
) -> dict[str, Any]:
    keys = {"canonical_path", "device", "inode", "nlink", "access_mode"}
    if not isinstance(value, dict) or set(value) != keys:
        raise SnapshotPublishError(f"{label} runtime database identity is invalid")
    canonical_path = value.get("canonical_path")
    if (
        not isinstance(canonical_path, str)
        or not Path(canonical_path).is_absolute()
        or ".." in Path(canonical_path).parts
        or value.get("access_mode") != "writer"
    ):
        raise SnapshotPublishError(f"{label} runtime database identity is invalid")
    for key in ("device", "inode", "nlink"):
        if not isinstance(value.get(key), int) or int(value[key]) <= 0:
            raise SnapshotPublishError(f"{label} runtime database identity is invalid")
    return dict(value)


def _validate_writer_lock_observation(
    value: object, *, label: str
) -> dict[str, Any]:
    keys = {"path", "device", "inode", "held"}
    if not isinstance(value, dict) or set(value) != keys:
        raise SnapshotPublishError(f"{label} writer-lock observation is invalid")
    path = value.get("path")
    if (
        not isinstance(path, str)
        or not Path(path).is_absolute()
        or ".." in Path(path).parts
        or value.get("held") is not True
    ):
        raise SnapshotPublishError(f"{label} writer-lock observation is invalid")
    for key in ("device", "inode"):
        if not isinstance(value.get(key), int) or int(value[key]) <= 0:
            raise SnapshotPublishError(f"{label} writer-lock observation is invalid")
    return dict(value)


def _installed_formal_read_contract(
    *, project_root: Path, database: Path
) -> tuple[Path, dict[str, Any], dict[str, Any]]:
    """Resolve the installed writer authority without opening or taking its lock."""
    try:
        access = runtime_database.resolve_installed_database_access(
            runtime_database.DatabaseAccessMode.FORMAL_READ,
            database=database,
            project_root=project_root,
        )
    except runtime_database.RuntimeDatabaseError as error:
        raise SnapshotPublishError(
            f"formal-read database authority refused: {error}"
        ) from error
    database = access.database
    database_identity = {
        "canonical_path": str(access.database),
        "device": access.database_identity.device,
        "inode": access.database_identity.inode,
        "nlink": access.database_identity.nlink,
    }
    if access.writer_lock is None:
        raise SnapshotPublishError("installed writer lock is missing")
    lock_path = access.writer_lock
    lock_file_identity = _file_identity(lock_path, label="installed writer lock")
    if lock_file_identity["nlink"] != 1 or stat.S_IMODE(lock_path.stat().st_mode) != 0o600:
        raise SnapshotPublishError(
            "installed writer lock must be a single-link 0600 regular file"
        )
    lock_identity = {
        "path": lock_file_identity["canonical_path"],
        "device": lock_file_identity["device"],
        "inode": lock_file_identity["inode"],
        "held": True,
    }
    return database, database_identity, lock_identity


def _artifact_selection(manifest: Mapping[str, Any]) -> dict[str, Any]:
    rows: list[dict[str, str]] = []
    for collection in ("files", "optional_reuse_files"):
        values = manifest.get(collection)
        if not isinstance(values, list):
            raise SnapshotPublishError("snapshot artifact collection is invalid")
        for value in values:
            if not isinstance(value, dict):
                raise SnapshotPublishError("snapshot artifact collection is invalid")
            root = value.get("root")
            path = value.get("path")
            project_path = value.get("project_path")
            if not all(isinstance(item, str) and item for item in (root, path, project_path)):
                raise SnapshotPublishError("snapshot artifact collection is invalid")
            rows.append(
                {
                    "collection": collection,
                    "root": str(root),
                    "path": str(path),
                    "project_path": str(project_path),
                }
            )
    rows.sort(key=lambda item: (item["collection"], item["root"], item["path"], item["project_path"]))
    return {
        "roots": sorted({row["root"] for row in rows}),
        "path_count": len(rows),
        "paths_sha256": pipeline_cutover.digest(rows),
    }


def _source_receipt_payload(
    *,
    manifest: Mapping[str, Any],
    manifest_sha256: str,
    freshness: WriterFreshness,
    database: Path,
    database_identity: Mapping[str, Any],
    writer_lock: Mapping[str, Any],
) -> dict[str, Any]:
    formal_database = {
        **dict(database_identity),
        "access_mode": "formal_read",
        "sha256": _sha256_regular_file(
            database, expected_identity=database_identity
        ),
    }
    payload: dict[str, Any] = {
        "contract_version": SOURCE_RECEIPT_CONTRACT,
        "snapshot_id": manifest.get("snapshot_id"),
        "manifest_sha256": manifest_sha256,
        "publication_evidence": freshness.evidence,
        "publication_evidence_sha256": pipeline_cutover.digest(freshness.evidence),
        "formal_database": formal_database,
        "observed_writer_lock": dict(writer_lock),
        "runtime_identity": freshness.runtime_identity,
        "snapshot_contract": freshness.snapshot_contract,
        "artifact_selection": _artifact_selection(manifest),
        "content_count": freshness.content_count,
        "latest_published_at": freshness.latest_published_at,
        "created_at": _utc_now(),
    }
    payload["payload_sha256"] = pipeline_cutover.digest(payload)
    return payload


def _validate_source_receipt(
    value: object,
    *,
    manifest: Mapping[str, Any],
    manifest_sha256: str,
) -> tuple[dict[str, Any], WriterFreshness]:
    required = {
        "contract_version",
        "snapshot_id",
        "manifest_sha256",
        "publication_evidence",
        "publication_evidence_sha256",
        "formal_database",
        "observed_writer_lock",
        "runtime_identity",
        "snapshot_contract",
        "artifact_selection",
        "content_count",
        "latest_published_at",
        "created_at",
        "payload_sha256",
    }
    if not isinstance(value, dict) or not required.issubset(value):
        raise SnapshotPublishError("snapshot source receipt has an invalid shape")
    if value.get("contract_version") != SOURCE_RECEIPT_CONTRACT:
        raise SnapshotPublishError("snapshot source receipt contract is invalid")
    payload = dict(value)
    claimed_payload_sha256 = payload.pop("payload_sha256", None)
    if (
        not isinstance(claimed_payload_sha256, str)
        or SHA256_RE.fullmatch(claimed_payload_sha256) is None
        or pipeline_cutover.digest(payload) != claimed_payload_sha256
    ):
        raise SnapshotPublishError("snapshot source receipt payload SHA-256 mismatch")
    if (
        value.get("snapshot_id") != manifest.get("snapshot_id")
        or value.get("manifest_sha256") != manifest_sha256
        or value.get("artifact_selection") != _artifact_selection(manifest)
    ):
        raise SnapshotPublishError("snapshot source receipt does not bind the manifest")
    evidence = _validate_publication_evidence(value.get("publication_evidence"))
    if (
        value.get("publication_evidence_sha256")
        != pipeline_cutover.digest(evidence)
    ):
        raise SnapshotPublishError("snapshot source receipt evidence SHA-256 mismatch")
    runtime_identity = _validate_runtime_identity(
        value.get("runtime_identity"), label="snapshot source receipt"
    )
    snapshot_contract = _validate_snapshot_contract(
        value.get("snapshot_contract"), label="snapshot source receipt"
    )
    formal_database = value.get("formal_database")
    if not isinstance(formal_database, dict):
        raise SnapshotPublishError("snapshot source receipt formal database is invalid")
    if set(formal_database) != {
        "canonical_path",
        "device",
        "inode",
        "nlink",
        "access_mode",
        "sha256",
    }:
        raise SnapshotPublishError("snapshot source receipt formal database is invalid")
    if (
        not isinstance(formal_database.get("canonical_path"), str)
        or not Path(formal_database["canonical_path"]).is_absolute()
        or ".." in Path(formal_database["canonical_path"]).parts
        or formal_database.get("access_mode") != "formal_read"
        or not isinstance(formal_database.get("sha256"), str)
        or SHA256_RE.fullmatch(formal_database["sha256"]) is None
    ):
        raise SnapshotPublishError("snapshot source receipt formal database is invalid")
    for key in ("device", "inode", "nlink"):
        if not isinstance(formal_database.get(key), int) or formal_database[key] <= 0:
            raise SnapshotPublishError("snapshot source receipt formal database is invalid")
    _validate_writer_lock_observation(
        value.get("observed_writer_lock"), label="snapshot source receipt"
    )
    _parse_iso(value.get("created_at"), label="snapshot source receipt created_at")
    content_count = value.get("content_count")
    latest_published_at = value.get("latest_published_at")
    if not isinstance(content_count, int) or content_count < 0:
        raise SnapshotPublishError("snapshot source receipt content count is invalid")
    if latest_published_at is not None and not isinstance(latest_published_at, str):
        raise SnapshotPublishError("snapshot source receipt publication time is invalid")
    freshness = WriterFreshness(
        evidence=evidence,
        latest_published_at=latest_published_at,
        content_count=content_count,
        runtime_identity=runtime_identity,
        snapshot_contract=snapshot_contract,
    )
    return dict(value), freshness


def _load_source_receipt(
    output: Path,
    *,
    manifest: Mapping[str, Any],
    manifest_sha256: str,
) -> tuple[dict[str, Any], WriterFreshness]:
    path = _require_regular_local_file(
        output / SOURCE_RECEIPT_FILENAME, label="snapshot source receipt"
    )
    metadata = path.stat()
    if metadata.st_uid != os.getuid() or stat.S_IMODE(metadata.st_mode) != 0o600:
        raise SnapshotPublishError("snapshot source receipt permissions are unsafe")
    try:
        value = json.loads(path.read_bytes())
    except (OSError, UnicodeDecodeError, json.JSONDecodeError) as error:
        raise SnapshotPublishError("snapshot source receipt is unreadable") from error
    return _validate_source_receipt(
        value, manifest=manifest, manifest_sha256=manifest_sha256
    )


def _load_resumable_snapshot(
    config: PublishConfig,
    snapshot_id: str,
    *,
    project_root: Path,
) -> tuple[Path, dict[str, Any], datetime, bytes, str]:
    """Load one explicitly selected, locally sealed automatic snapshot."""
    if SNAPSHOT_ID_RE.fullmatch(snapshot_id) is None:
        raise SnapshotPublishError("staged resume snapshot_id is invalid")
    root = config.snapshot_root
    if root.is_symlink() or not root.is_dir():
        raise SnapshotPublishError("snapshot root is unsafe")
    matches: list[tuple[Path, dict[str, Any], bytes]] = []
    for output in root.iterdir():
        if (
            LOCAL_SNAPSHOT_DIR_RE.fullmatch(output.name) is None
            or output.is_symlink()
            or not output.is_dir()
        ):
            continue
        manifest_path = output / "manifest.json"
        if not manifest_path.exists():
            continue
        manifest_path = _require_regular_local_file(
            manifest_path, label="resumable snapshot manifest"
        )
        try:
            manifest_bytes = manifest_path.read_bytes()
            manifest = json.loads(manifest_bytes)
        except (OSError, UnicodeDecodeError, json.JSONDecodeError) as error:
            raise SnapshotPublishError(
                f"resumable snapshot manifest is unreadable: {output.name}"
            ) from error
        if not isinstance(manifest, dict):
            raise SnapshotPublishError(
                f"resumable snapshot manifest is invalid: {output.name}"
            )
        if manifest.get("snapshot_id") == snapshot_id:
            matches.append((output, manifest, manifest_bytes))
    if len(matches) != 1:
        raise SnapshotPublishError(
            "staged resume must match exactly one local frozen snapshot"
        )
    output, manifest, manifest_bytes = matches[0]
    metadata = output.stat()
    if metadata.st_uid != os.getuid() or stat.S_IMODE(metadata.st_mode) & 0o022:
        raise SnapshotPublishError("resumable snapshot output permissions are unsafe")
    if manifest.get("schema") != "dcar-read-replica-snapshot-v2":
        raise SnapshotPublishError("resumable snapshot manifest schema is invalid")
    _validate_manifest_contract(manifest, project_root=project_root)
    manifest_sha256 = hashlib.sha256(manifest_bytes).hexdigest()
    checksum_path = _require_regular_local_file(
        output / "manifest.sha256", label="resumable snapshot manifest checksum"
    )
    for path in (output / "manifest.json", checksum_path):
        file_metadata = path.stat()
        if (
            file_metadata.st_uid != os.getuid()
            or stat.S_IMODE(file_metadata.st_mode) & 0o022
        ):
            raise SnapshotPublishError(
                "resumable snapshot manifest permissions are unsafe"
            )
    try:
        checksum = checksum_path.read_text(encoding="ascii").strip()
    except (OSError, UnicodeDecodeError) as error:
        raise SnapshotPublishError(
            "resumable snapshot manifest checksum is unreadable"
        ) from error
    if checksum != manifest_sha256 + "  manifest.json":
        raise SnapshotPublishError("resumable snapshot manifest checksum mismatch")
    try:
        attempt_started_at = datetime.strptime(
            output.name, "snapshot-%Y%m%dT%H%M%SZ"
        ).replace(tzinfo=timezone.utc).astimezone(SHANGHAI)
    except ValueError as error:
        raise SnapshotPublishError(
            "resumable snapshot output time is invalid"
        ) from error
    if attempt_started_at.hour < AUTOMATIC_START_HOUR:
        raise SnapshotPublishError(
            "staged resume is restricted to an automatic-window snapshot"
        )
    return output, manifest, attempt_started_at, manifest_bytes, manifest_sha256


def _verify_local_snapshot(
    output: Path,
    manifest: Mapping[str, Any],
    *,
    current: datetime,
    config: PublishConfig,
    project_root: Path,
    fetch_json: JsonFetcher,
    sealed_freshness: WriterFreshness,
    expected_snapshot_id: Optional[str] = None,
    expected_runtime_identity: Optional[Mapping[str, Any]] = None,
    manifest_bytes: Optional[bytes] = None,
    freeze_observed: bool = False,
) -> tuple[dict[str, Any], WriterFreshness, str, int]:
    manifest_runtime_identity = _validate_runtime_identity(
        manifest.get("runtime_identity"), label="snapshot manifest", expected_schema=config.expected_user_version,
    )
    if (
        expected_runtime_identity is not None
        and manifest_runtime_identity != expected_runtime_identity
    ):
        raise SnapshotPublishError(
            "snapshot runtime identity drifted from the verified writer"
        )
    snapshot_id = str(manifest.get("snapshot_id") or "")
    if SNAPSHOT_ID_RE.fullmatch(snapshot_id) is None:
        raise SnapshotPublishError("snapshot manifest has an invalid snapshot_id")
    if expected_snapshot_id is not None and snapshot_id != expected_snapshot_id:
        raise SnapshotPublishError(
            "resumable snapshot manifest does not match the requested snapshot_id"
        )
    _validate_manifest_contract(manifest, project_root=project_root)
    snapshot_database_item = _snapshot_database_manifest_item(manifest)
    snapshot_database = output / snapshot_database_item["bundle_path"]
    freshness = sealed_freshness
    if freshness.runtime_identity != manifest_runtime_identity:
        raise SnapshotPublishError(
            "snapshot runtime identity drifted from the verified writer"
        )
    if freeze_observed and freshness.evidence.get("mode") == "observed":
        # A live writer can progress while SQLite backup runs. Seal the actual
        # detached snapshot, then validate it exactly; never copy a stale claim
        # from the preflight onto different database bytes.
        pipeline_cutover.verify_file({"path": str(snapshot_database),
            "sha256": snapshot_database_item["sha256"], "byte_size": snapshot_database_item["byte_size"]})
        captured = _parse_iso(manifest.get("created_at", freshness.evidence["verified_at"]), label="snapshot created_at").astimezone(SHANGHAI)
        observed_at = max(captured, current)
        if observed_at.date() != current.date():
            raise SnapshotPublishError("snapshot crossed the publication business day")
        boundary = _reconcile_from(freshness.evidence)
        assert boundary is not None
        with closing(sqlite3.connect(f"{snapshot_database.as_uri()}?mode=ro", uri=True)) as connection:
            connection.row_factory = sqlite3.Row
            configure_connection_safety(connection)
            connection.execute("PRAGMA query_only=ON")
            at = observed_at.astimezone(timezone.utc).isoformat(timespec="seconds").replace("+00:00", "Z")
            if config.expected_user_version == 20:
                evidence = _schema20_publication_evidence(connection, current=observed_at,
                    at=at, project_root=project_root)
            else:
                evidence = _observed_publication_evidence(connection, current=observed_at,
                    at=at, project_root=project_root, boundary=boundary)
        freshness = replace(freshness, evidence=evidence)
    _verify_snapshot_dependencies(
        output, manifest, freshness, project_root=project_root
    )
    try:
        connection = sqlite3.connect(
            f"{snapshot_database.as_uri()}?mode=ro", uri=True
        )
        connection.row_factory = sqlite3.Row
        try:
            configure_connection_safety(connection)
            connection.execute("PRAGMA query_only=ON")
            content = connection.execute(
                "SELECT COUNT(*) content_count,MAX(published_at) latest_published_at "
                "FROM content_items"
            ).fetchone()
        finally:
            connection.close()
    except sqlite3.Error as error:
        raise SnapshotPublishError(
            f"snapshot publication dependency verification failed: {error}"
        ) from error
    freshness = WriterFreshness(
        evidence=freshness.evidence,
        content_count=int(content["content_count"]),
        latest_published_at=(
            str(content["latest_published_at"])
            if content["latest_published_at"]
            else None
        ),
        runtime_identity=freshness.runtime_identity,
        snapshot_contract=freshness.snapshot_contract,
    )
    manifest_path = _require_regular_local_file(
        output / "manifest.json", label="snapshot manifest"
    )
    sealed_manifest_bytes = manifest_path.read_bytes()
    if manifest_bytes is not None and sealed_manifest_bytes != manifest_bytes:
        raise SnapshotPublishError("resumable snapshot manifest changed while loading")
    if json.loads(sealed_manifest_bytes) != manifest:
        raise SnapshotPublishError(
            "snapshot manifest bytes differ from the verified manifest"
        )
    manifest_sha256 = hashlib.sha256(sealed_manifest_bytes).hexdigest()
    checksum_path = _require_regular_local_file(
        output / "manifest.sha256", label="snapshot manifest checksum"
    )
    if (
        checksum_path.read_text(encoding="ascii").strip()
        != manifest_sha256 + "  manifest.json"
    ):
        raise SnapshotPublishError("snapshot manifest checksum mismatch")
    return (
        manifest_runtime_identity,
        freshness,
        manifest_sha256,
        _bundle_byte_size(output),
    )


def _validate_today_run(
    row: Optional[sqlite3.Row],
    *,
    job_id: str,
    hour: int,
    minute: int,
    allowed_statuses: frozenset[str],
    current: datetime,
) -> tuple[str, datetime, datetime]:
    if row is None:
        raise SnapshotPublishError(f"today's {job_id} has not reached the database")
    scheduled = _parse_iso(row["scheduled_for"], label=f"{job_id} scheduled_for")
    scheduled_local = scheduled.astimezone(SHANGHAI)
    if (
        scheduled_local.date() != current.date()
        or scheduled_local.hour != hour
        or scheduled_local.minute != minute
    ):
        raise SnapshotPublishError(
            f"today's {hour:02d}:{minute:02d} {job_id} is missing"
        )
    status = str(row["status"] or "")
    if status not in allowed_statuses:
        raise SnapshotPublishError(
            f"today's {job_id} status is not publishable: {status or 'missing'}"
        )
    completed = _parse_iso(row["completed_at"], label=f"{job_id} completed_at")
    if completed < scheduled or completed > current.astimezone(timezone.utc):
        raise SnapshotPublishError(f"{job_id} completion time is not current")
    return status, scheduled, completed


def _read_external_env(path: Path, *, project_root: Path) -> PublishConfig:
    path = path.expanduser()
    project_root = project_root.resolve()
    if path.is_symlink() or not path.is_file():
        raise SnapshotPublishError(
            "publisher environment must be a regular non-symlink file"
        )
    path = path.resolve()
    mode = stat.S_IMODE(path.stat().st_mode)
    if mode not in {0o400, 0o600}:
        raise SnapshotPublishError("publisher environment must have mode 0400 or 0600")
    if path.stat().st_uid != os.getuid():
        raise SnapshotPublishError(
            "publisher environment must be owned by the current user"
        )
    if path == project_root or project_root in path.parents:
        raise SnapshotPublishError(
            "publisher environment must stay outside the repository"
        )
    values: dict[str, str] = {}
    for raw_line in path.read_text(encoding="utf-8").splitlines():
        line = raw_line.strip()
        if not line or line.startswith("#"):
            continue
        key, separator, value = line.partition("=")
        if not separator or key not in ALLOWED_ENV_KEYS:
            raise SnapshotPublishError(
                f"unsupported publisher environment entry: {key}"
            )
        if key in values:
            raise SnapshotPublishError(f"duplicate publisher environment entry: {key}")
        values[key] = value.strip()
    missing = sorted(ALLOWED_ENV_KEYS - values.keys())
    if missing:
        raise SnapshotPublishError(
            "publisher environment is missing: " + ", ".join(missing)
        )
    alias = values["DCAR_PUBLISH_SSH_ALIAS"]
    if not SAFE_ALIAS_RE.fullmatch(alias) or alias in {"localhost", "127.0.0.1"}:
        raise SnapshotPublishError("publisher requires a dedicated safe SSH alias")
    remote_values = {
        key: values[key]
        for key in (
            "DCAR_PUBLISH_REMOTE_PROJECT_ROOT",
            "DCAR_PUBLISH_REMOTE_STATE_ROOT",
            "DCAR_PUBLISH_REMOTE_PYTHON",
        )
    }
    if any(
        not SAFE_REMOTE_PATH_RE.fullmatch(value)
        or "//" in value
        or "/../" in value
        or value.endswith("/")
        for value in remote_values.values()
    ):
        raise SnapshotPublishError(
            "publisher remote paths must be simple absolute paths"
        )
    snapshot_root_value = Path(values["DCAR_PUBLISH_SNAPSHOT_ROOT"]).expanduser()
    if not snapshot_root_value.is_absolute() or snapshot_root_value.is_symlink():
        raise SnapshotPublishError("snapshot root must be absolute and not a symlink")
    snapshot_root = snapshot_root_value.resolve()
    if snapshot_root == project_root or project_root in snapshot_root.parents:
        raise SnapshotPublishError(
            "snapshot root must be an absolute path outside the repository"
        )
    try:
        minimum_remote_free_bytes = int(values["DCAR_PUBLISH_MIN_REMOTE_FREE_BYTES"])
        expected_user_version = int(values["DCAR_PUBLISH_EXPECTED_USER_VERSION"])
        maximum_content_lag_days = int(values["DCAR_PUBLISH_MAX_CONTENT_LAG_DAYS"])
    except ValueError as exc:
        raise SnapshotPublishError("publisher numeric settings are invalid") from exc
    if minimum_remote_free_bytes < 1024 * 1024 * 1024:
        raise SnapshotPublishError("remote free-space reserve must be at least 1 GiB")
    if expected_user_version not in SUPPORTED_SCHEMA_MIGRATIONS:
        raise SnapshotPublishError(
            "publisher environment must pin schema 19 or 20 explicitly; "
            "the first version transition uses schema-upgrade"
        )
    if not 0 <= maximum_content_lag_days <= 7:
        raise SnapshotPublishError("maximum content lag must be between 0 and 7 days")
    return PublishConfig(
        ssh_alias=alias,
        remote_project_root=remote_values["DCAR_PUBLISH_REMOTE_PROJECT_ROOT"],
        remote_state_root=remote_values["DCAR_PUBLISH_REMOTE_STATE_ROOT"],
        remote_python=remote_values["DCAR_PUBLISH_REMOTE_PYTHON"],
        snapshot_root=snapshot_root,
        minimum_remote_free_bytes=minimum_remote_free_bytes,
        expected_user_version=expected_user_version,
        maximum_content_lag_days=maximum_content_lag_days,
    )


def _default_fetch_json(url: str) -> dict[str, Any]:
    request = urllib.request.Request(
        url,
        headers={"Accept": "application/json", "User-Agent": "DcarPublisher/1"},
    )
    try:
        with urllib.request.urlopen(
            request, timeout=WRITER_ENDPOINT_TIMEOUT_SECONDS
        ) as response:
            if int(response.status) != 200:
                raise SnapshotPublishError(
                    f"writer endpoint returned {response.status}"
                )
            value = json.loads(response.read().decode("utf-8"))
    except (
        OSError,
        UnicodeDecodeError,
        json.JSONDecodeError,
        urllib.error.URLError,
    ) as exc:
        raise SnapshotPublishError(f"writer endpoint is unavailable: {url}") from exc
    if not isinstance(value, dict):
        raise SnapshotPublishError(f"writer endpoint returned non-object JSON: {url}")
    return value


def check_writer_freshness(
    database: Path,
    *,
    now: Optional[datetime] = None,
    maximum_content_lag_days: int,
    fetch_json: JsonFetcher = _default_fetch_json,
    project_root: Path = PACKAGE_ROOT.parents[1],
    expected_runtime_database_identity: Optional[Mapping[str, Any]] = None,
    expected_writer_lock: Optional[Mapping[str, Any]] = None,
    expected_user_version: int = EXPECTED_DATABASE_SCHEMA_VERSION,
) -> WriterFreshness:
    database = _require_regular_local_file(database, label="formal writer database")
    local_database_identity = _file_identity(database, label="formal writer database")
    expected_local_runtime_identity = {
        **local_database_identity,
        "access_mode": "writer",
    }
    current = (now or datetime.now(SHANGHAI)).astimezone(SHANGHAI)
    health = fetch_json("http://127.0.0.1:8766/api/v8/health")
    if health.get("status") != "ok" or health.get("database") != database.name:
        raise SnapshotPublishError("writer health does not match the formal database")
    runtime_database_identity = _validate_runtime_database_identity(
        health.get("runtime_database_identity"), label="writer health"
    )
    if runtime_database_identity != expected_local_runtime_identity:
        raise SnapshotPublishError(
            "writer health runtime database identity does not match the formal database"
        )
    if (
        expected_runtime_database_identity is not None
        and runtime_database_identity != dict(expected_runtime_database_identity)
    ):
        raise SnapshotPublishError(
            "writer health runtime database identity does not match the installed writer"
        )
    health_database_state = health.get("database_state")
    if not isinstance(health_database_state, dict):
        raise SnapshotPublishError("writer health omitted database identity")
    health_runtime_identity = _validate_runtime_identity(
        health_database_state.get("runtime_identity"), label="writer health", expected_schema=expected_user_version,
    )
    snapshot_contract = _validate_snapshot_contract(health.get("snapshot_contract"), label="writer health")
    scheduler = fetch_json("http://127.0.0.1:8766/api/v8/scheduler")
    boundary = _reconcile_from(scheduler)
    health_writer_lock = _validate_writer_lock_observation(
        health.get("writer_lock"), label="writer health"
    )
    writer_lock = _validate_writer_lock_observation(
        scheduler.get("writer_lock"), label="writer scheduler"
    )
    if health_writer_lock != writer_lock:
        raise SnapshotPublishError(
            "writer health and scheduler observed different writer locks"
        )
    if expected_writer_lock is not None and writer_lock != dict(expected_writer_lock):
        raise SnapshotPublishError(
            "observed writer lock does not match the installed writer"
        )
    if scheduler.get("requested") is not True or scheduler.get("enabled") is not True:
        raise SnapshotPublishError(
            "designated writer scheduler is not current and enabled"
        )
    # Startup catch-up is an execution aid, not report truth. The durable
    # occurrence, frozen inputs and terminal attempt below are authoritative.
    catchup = scheduler.get("startup_catchup")
    if isinstance(catchup, dict) and catchup.get("requested") is True:
        results = catchup.get("results", [])
        if catchup.get("mode") != "report_only" or not isinstance(results, list) or any(
            not isinstance(item, dict) or item.get("job_id") not in {"daily_report", "weekly_report"}
            for item in results
        ):
            raise SnapshotPublishError("writer startup catch-up contains non-report work")
    connection = sqlite3.connect(f"{database.as_uri()}?mode=ro", uri=True)
    connection.row_factory = sqlite3.Row
    timestamp = current.astimezone(timezone.utc).isoformat(timespec="seconds").replace("+00:00", "Z")
    try:
        configure_connection_safety(connection)
        connection.execute("PRAGMA query_only=ON")
        connection.execute("BEGIN")
        require_schema_compatibility(
            connection,
            supported_versions=frozenset({expected_user_version}),
        )
        runtime_identity = _database_runtime_identity(connection, expected_schema=expected_user_version)
        if runtime_identity != health_runtime_identity:
            raise SnapshotPublishError("writer health runtime identity does not match the formal database")
        if expected_user_version == 20:
            evidence = _schema20_publication_evidence(connection, current=current, at=timestamp, project_root=project_root)
            content = connection.execute("SELECT COUNT(*) content_count,MAX(published_at) latest_published_at FROM content_items").fetchone()
            return WriterFreshness(evidence=evidence, content_count=int(content["content_count"]),
                latest_published_at=content["latest_published_at"], runtime_identity=runtime_identity, snapshot_contract=snapshot_contract)
        if boundary is not None:
            evidence = _observed_publication_evidence(
                connection, current=current, at=timestamp, project_root=project_root, boundary=boundary,
            )
            content = connection.execute("SELECT COUNT(*) content_count,MAX(published_at) latest_published_at FROM content_items").fetchone()
            return WriterFreshness(evidence=evidence, content_count=int(content["content_count"]),
                latest_published_at=content["latest_published_at"], runtime_identity=runtime_identity, snapshot_contract=snapshot_contract)
        cutover_rows = connection.execute(
            "SELECT id FROM scheduler_runs WHERE job_id=? AND "
            "json_extract(details_json,'$.identity.beijing_date')=? ORDER BY id",
            (pipeline_cutover.CUTOVER_JOB, current.date().isoformat()),
        ).fetchall()
        cutover = None
        if cutover_rows:
            if len(cutover_rows) != 1:
                raise SnapshotPublishError("writer has ambiguous first-day cutover receipts")
            cutover = pipeline_cutover.verify_cutover(connection, run_id=cutover_rows[0]["id"], at=timestamp, project_root=project_root)
            reports = cutover["payload"]["reports"]
            discovery = {"mode": "cutover", "scans": cutover["payload"]["scans"]}
            preparation = None
        else:
            discovery = pipeline_cutover.runtime_evidence(connection, at=timestamp)
            preparation = _preparation_dependency(connection, current=current, at=timestamp)
            reports = _normal_report_dependencies(connection, current=current, at=timestamp, project_root=project_root,
                                                   reconcile_from=boundary)
        content = connection.execute("SELECT COUNT(*) content_count,MAX(published_at) latest_published_at FROM content_items").fetchone()
        # The latest publication date is display-only. Complete empty scans and
        # quiet accounts must never be misreported as a stale writer.
        if not 0 <= maximum_content_lag_days <= 7:
            raise SnapshotPublishError("maximum content lag setting is invalid")
        latest_published_at = str(content["latest_published_at"]) if content["latest_published_at"] else None
        evidence = {
            "schema": FRESHNESS_SCHEMA, "beijing_date": current.date().isoformat(),
            "verified_at": timestamp, "mode": "cutover" if cutover else "scheduled",
            "discovery": discovery, "preparation": preparation, "reports": reports,
            "cutover": cutover,
            **({"reconcile_from": boundary.isoformat()} if boundary is not None else {}),
            "status": "partial" if any(item["status"] == "partial" for item in reports)
                      or (not cutover and not discovery["coverage"]["complete"]) else "succeeded",
        }
    except (sqlite3.Error, ValueError, RuntimeError, KeyError, TypeError, OSError) as error:
        if isinstance(error, SnapshotPublishError):
            raise
        raise SnapshotPublishError(f"writer publication evidence is invalid: {error}") from error
    finally:
        connection.close()
    return WriterFreshness(evidence=evidence, content_count=int(content["content_count"]),
        latest_published_at=latest_published_at, runtime_identity=runtime_identity, snapshot_contract=snapshot_contract)


def _observe_formal_read_source(
    database: Path,
    *,
    project_root: Path,
    now: Optional[datetime],
    config: PublishConfig,
    fetch_json: JsonFetcher,
) -> FormalReadObservation:
    database, database_identity, writer_lock = _installed_formal_read_contract(
        project_root=project_root, database=database
    )
    runtime_database_identity = {**database_identity, "access_mode": "writer"}
    freshness = check_writer_freshness(
        database,
        now=now,
        maximum_content_lag_days=config.maximum_content_lag_days,
        fetch_json=fetch_json,
        project_root=project_root,
        expected_runtime_database_identity=runtime_database_identity,
        expected_writer_lock=writer_lock,
        expected_user_version=config.expected_user_version,
    )
    return FormalReadObservation(
        database=database,
        database_identity=database_identity,
        writer_lock=writer_lock,
        freshness=freshness,
    )


def _load_builder(project_root: Path) -> ModuleType:
    path = source_root(project_root) / "scripts/build_server_snapshot.py"
    specification = importlib.util.spec_from_file_location(
        "dcar_snapshot_builder_for_publisher", path
    )
    if specification is None or specification.loader is None:
        raise SnapshotPublishError("snapshot builder cannot be loaded")
    module = importlib.util.module_from_spec(specification)
    sys.modules[specification.name] = module
    specification.loader.exec_module(module)
    return module


def _run_checked(
    runner: CommandRunner,
    arguments: Sequence[str],
    *,
    timeout: int,
    environment: Optional[Mapping[str, str]] = None,
    input_text: Optional[str] = None,
) -> str:
    kwargs: dict[str, Any] = {
        "check": False,
        "capture_output": True,
        "text": True,
        "timeout": timeout,
    }
    if environment is not None:
        kwargs["env"] = dict(environment)
    if input_text is not None:
        kwargs["input"] = input_text
    try:
        completed = runner(list(arguments), **kwargs)
    except (OSError, subprocess.SubprocessError) as exc:
        raise SnapshotPublishError(
            f"command could not complete ({Path(arguments[0]).name}): {exc}"
        ) from exc
    if completed.returncode != 0:
        detail = (completed.stderr or completed.stdout or "").strip()[-1000:]
        raise SnapshotPublishError(
            f"command failed ({Path(arguments[0]).name}): {detail}"
        )
    return completed.stdout.strip()


RSYNC_RETRY_DELAYS = (2, 10)
RSYNC_TRANSIENT_ERRORS = (
    "can't assign requested address", "broken pipe", "connection reset by peer",
    "connection timed out", "connection closed by remote host", "no route to host",
    "network is unreachable",
)


def _run_rsync_checked(
    runner: CommandRunner, arguments: Sequence[str], *, timeout: int,
    environment: Optional[Mapping[str, str]] = None,
) -> str:
    """Retry only idempotent rsync operations, within their original deadline."""
    deadline = clock_time.monotonic() + timeout
    for attempt in range(len(RSYNC_RETRY_DELAYS) + 1):
        remaining = max(1, int(deadline - clock_time.monotonic()))
        try:
            return _run_checked(runner, arguments, timeout=remaining, environment=environment)
        except SnapshotPublishError as error:
            if (attempt == len(RSYNC_RETRY_DELAYS)
                    or not any(reason in str(error).lower() for reason in RSYNC_TRANSIENT_ERRORS)
                    or deadline - clock_time.monotonic() <= RSYNC_RETRY_DELAYS[attempt]):
                raise
            delay = RSYNC_RETRY_DELAYS[attempt]
            print(f"snapshot rsync transient disconnect; retry {attempt + 1}/"
                  f"{len(RSYNC_RETRY_DELAYS)} in {delay}s", file=sys.stderr, flush=True)
            clock_time.sleep(delay)
    raise AssertionError("unreachable rsync retry state")


def _prepare_local_resume_incoming(
    config: PublishConfig, ssh: Sequence[str], *, runner: CommandRunner,
    snapshot_id: str, manifest_sha256: str,
) -> None:
    """Create or continue only this sealed snapshot's private staging paths."""
    code = """
import hashlib, os, stat, sys
from pathlib import Path
root, snapshot_id, expected = Path(sys.argv[1]), sys.argv[2], sys.argv[3]
paths = [root, root / snapshot_id, root / snapshot_id / "artifacts",
         root / snapshot_id / "artifacts/cache", root / snapshot_id / "artifacts/reports",
         root / snapshot_id / "bundle"]
for path in paths:
    if path.exists() or path.is_symlink():
        metadata = path.lstat()
        if not stat.S_ISDIR(metadata.st_mode) or metadata.st_uid != os.getuid() or metadata.st_mode & 0o022:
            raise SystemExit("local resume staging directory is unsafe")
    else:
        path.mkdir(mode=0o750)
manifest = paths[-1] / "manifest.json"
if manifest.exists() or manifest.is_symlink():
    metadata = manifest.lstat()
    if not stat.S_ISREG(metadata.st_mode) or metadata.st_nlink != 1:
        raise SystemExit("local resume remote manifest is unsafe")
    if hashlib.sha256(manifest.read_bytes()).hexdigest() != expected:
        raise SystemExit("local resume remote manifest identity differs")
print("local-resume-staging-ready")
"""
    command = shlex.join([config.remote_python, "-c", code, config.remote_incoming_root,
                          snapshot_id, manifest_sha256])
    _run_checked(runner, _remote_command(ssh, command), timeout=30)


def _verified_bundle_basis(
    config: PublishConfig, ssh: Sequence[str], *, runner: CommandRunner,
    previous_remote: Mapping[str, Any], snapshot_id: str,
) -> str:
    """Use only the independent bundle bound to the verified active receipt."""
    previous_id = str(previous_remote.get("snapshot_id") or "")
    if (not SNAPSHOT_ID_RE.fullmatch(previous_id)
            or not SNAPSHOT_ID_RE.fullmatch(snapshot_id)
            or previous_id == snapshot_id
            or any(not SHA256_RE.fullmatch(str(previous_remote.get(key) or ""))
                   for key in ("manifest_sha256", "database_sha256"))):
        raise SnapshotPublishError("bundle delta basis identity is invalid or equals target")
    code = """
import hashlib, json, os, stat, sys
from pathlib import Path
root, expected = Path(sys.argv[1]), json.loads(sys.argv[2])
basis = root / expected["snapshot_id"] / "bundle"
def safe(path, directory=False):
    m = path.lstat()
    valid = stat.S_ISDIR(m.st_mode) if directory else stat.S_ISREG(m.st_mode) and m.st_nlink == 1
    if not valid or m.st_mode & 0o022:
        raise SystemExit("bundle delta basis has unsafe path")
    return m
def digest(path):
    before = safe(path)
    with open(os.open(path, os.O_RDONLY | os.O_NOFOLLOW), "rb") as stream:
        opened = os.fstat(stream.fileno())
        if (opened.st_dev, opened.st_ino) != (before.st_dev, before.st_ino):
            raise SystemExit("bundle delta basis changed during verification")
        h = hashlib.sha256()
        for chunk in iter(lambda: stream.read(1024 * 1024), b""):
            h.update(chunk)
        after = os.fstat(stream.fileno())
    if (before.st_size, before.st_mtime_ns, before.st_ctime_ns) != (after.st_size, after.st_mtime_ns, after.st_ctime_ns):
        raise SystemExit("bundle delta basis changed during verification")
    return h.hexdigest(), before.st_size
for path in [root, basis.parent, basis]:
    safe(path, directory=True)
for path in basis.rglob("*"):
    safe(path, directory=path.is_dir())
manifest_path = basis / "manifest.json"
if digest(manifest_path)[0] != expected["manifest_sha256"]:
    raise SystemExit("bundle delta basis manifest hash differs")
manifest = json.loads(manifest_path.read_text())
if (basis / "manifest.sha256").read_text().split() != [expected["manifest_sha256"], "manifest.json"]:
    raise SystemExit("bundle delta basis manifest seal differs")
for key in ["snapshot_id", "runtime_identity", "snapshot_contract"]:
    if manifest.get(key) != expected[key]:
        raise SystemExit("bundle delta basis active identity differs")
databases = manifest.get("databases")
if not isinstance(databases, list) or not databases:
    raise SystemExit("bundle delta basis databases missing")
seen = set()
for row in databases:
    name = row["name"]
    if name not in ["dcar_insight.sqlite3", "web_mvp.sqlite3"] or name in seen or row["bundle_path"] != "databases/" + name:
        raise SystemExit("bundle delta basis database path differs")
    seen.add(name)
    if digest(basis / row["bundle_path"]) != (row["sha256"], row["byte_size"]):
        raise SystemExit("bundle delta basis database hash differs")
    if name == "dcar_insight.sqlite3" and row["sha256"] != expected["database_sha256"]:
        raise SystemExit("bundle delta basis active database differs")
if "dcar_insight.sqlite3" not in seen:
    raise SystemExit("bundle delta basis active database missing")
print("verified-active-bundle-delta-basis")
"""
    expected = {key: previous_remote[key] for key in (
        "snapshot_id", "manifest_sha256", "database_sha256", "runtime_identity", "snapshot_contract"
    )}
    command = shlex.join([config.remote_python, "-c", code, config.remote_incoming_root,
                          json.dumps(expected, sort_keys=True)])
    _run_checked(runner, _remote_command(ssh, command), timeout=REMOTE_PROBE_COMMAND_TIMEOUT_SECONDS)
    return config.remote_incoming_root + "/" + previous_id + "/bundle"


def _ssh_arguments(config: PublishConfig) -> list[str]:
    known_hosts = Path.home() / ".ssh/known_hosts"
    if known_hosts.is_symlink() or not known_hosts.is_file():
        raise SnapshotPublishError("standard SSH known_hosts file is missing or unsafe")
    known_hosts_metadata = known_hosts.stat()
    if (
        known_hosts_metadata.st_uid != os.getuid()
        or stat.S_IMODE(known_hosts_metadata.st_mode) & 0o022
    ):
        raise SnapshotPublishError(
            "standard SSH known_hosts ownership or mode is unsafe"
        )
    return [
        "ssh",
        "-o",
        "BatchMode=yes",
        "-o",
        "PasswordAuthentication=no",
        "-o",
        "KbdInteractiveAuthentication=no",
        "-o",
        "IdentitiesOnly=yes",
        "-o",
        "IdentityAgent=none",
        "-o",
        "StrictHostKeyChecking=yes",
        "-o",
        f"UserKnownHostsFile={known_hosts}",
        "-o",
        "ServerAliveInterval=30",
        "-o",
        "ServerAliveCountMax=3",
        "-o",
        "ConnectTimeout=15",
        config.ssh_alias,
    ]


def _check_ssh_alias(
    config: PublishConfig, *, runner: CommandRunner = subprocess.run
) -> list[str]:
    ssh = _ssh_arguments(config)
    output = _run_checked(runner, [*ssh[:-1], "-G", config.ssh_alias], timeout=15)
    values: dict[str, list[str]] = {}
    for line in output.splitlines():
        key, separator, value = line.partition(" ")
        if separator:
            values.setdefault(key.lower(), []).append(value.strip())
    if not values.get("hostname") or not values.get("user"):
        raise SnapshotPublishError("SSH alias does not resolve to a host and user")
    identities = values.get("identityfile", [])
    existing_identities = []
    for value in identities:
        identity = Path(value).expanduser()
        if value.lower() == "none" or identity.is_symlink() or not identity.is_file():
            continue
        metadata = identity.stat()
        if metadata.st_uid == os.getuid() and stat.S_IMODE(metadata.st_mode) in {
            0o400,
            0o600,
        }:
            existing_identities.append(identity)
    if not existing_identities:
        raise SnapshotPublishError("SSH alias has no readable dedicated IdentityFile")
    return ssh


def _json_command_output(output: str, *, label: str) -> dict[str, Any]:
    try:
        value = json.loads(output)
    except (TypeError, json.JSONDecodeError) as exc:
        raise SnapshotPublishError(f"{label} returned invalid JSON") from exc
    if not isinstance(value, dict):
        raise SnapshotPublishError(f"{label} returned non-object JSON")
    return dict(value)


def _remote_probe(
    config: PublishConfig,
    ssh: Sequence[str],
    *,
    runner: CommandRunner,
) -> dict[str, Any]:
    code = """
import json
import os
import subprocess
import sys
import urllib.request
from pathlib import Path

project_root = Path(sys.argv[1])
state_root = Path(sys.argv[2])
python_path = Path(sys.argv[3])
installer = Path(sys.argv[4])
expected_schema = int(sys.argv[5])
installer_schema_support = None
if expected_schema == 20:
    import importlib.util
    spec = importlib.util.spec_from_file_location("dcar_remote_installer_probe", installer)
    if spec is None or spec.loader is None:
        raise RuntimeError("installed schema20 receiver cannot be loaded")
    module = importlib.util.module_from_spec(spec)
    sys.modules[spec.name] = module
    spec.loader.exec_module(module)
    installer_schema_support = {
        "versions": sorted(module.SUPPORTED_SCHEMA_VERSIONS),
        "transitions": [list(pair) for pair in sorted(module.SUPPORTED_SCHEMA_TRANSITIONS)],
    }

def fetch(url):
    request = urllib.request.Request(
        url,
        headers={"Accept": "application/json", "User-Agent": "DcarRemoteProbe/1"},
    )
    with urllib.request.urlopen(request, timeout=__REMOTE_ENDPOINT_TIMEOUT__) as response:
        if response.status != 200:
            raise RuntimeError(f"unexpected HTTP status {response.status}: {url}")
        value = json.loads(response.read().decode("utf-8"))
        if not isinstance(value, dict):
            raise RuntimeError(f"non-object JSON: {url}")
        return value

services = {
    service: subprocess.run(
        ["systemctl", "is-active", service],
        check=False,
        capture_output=True,
        text=True,
        timeout=15,
    ).stdout.strip()
    for service in ("dcar-api.service", "dcar-web.service", "dcar-auth.service", "dcar-douyin-control.service")
}
directories = {
    name: (state_root / name).is_dir()
    for name in ("db", "cache", "reports", "runtime", "incoming")
}
active_path = state_root / "runtime" / "active-snapshot.json"
active = json.loads(active_path.read_text(encoding="utf-8"))
transition_path = state_root / "runtime" / "schema-upgrade-transition.json"
transition = None
if transition_path.is_symlink():
    raise RuntimeError("schema transition marker is unsafe")
if transition_path.exists():
    transition = json.loads(transition_path.read_text(encoding="utf-8"))
stat = os.statvfs(state_root)
print(json.dumps({
    "schema": "dcar-remote-publisher-probe-v1",
    "current_release": str(project_root.resolve(strict=True)),
    "python_ready": python_path.is_file() and os.access(python_path, os.X_OK),
    "installer_ready": installer.is_file(),
    "installer_schema_support": installer_schema_support,
    "services": services,
    "directories": directories,
    "free_bytes": stat.f_bavail * stat.f_frsize,
    "active_receipt": active,
    "schema_transition": transition,
    "health": fetch("http://127.0.0.1:8765/api/v8/health"),
    "overview": fetch("http://127.0.0.1:8765/api/v8/overview"),
    "scheduler": fetch("http://127.0.0.1:8765/api/v8/scheduler"),
}, sort_keys=True))
""".replace(
        "__REMOTE_ENDPOINT_TIMEOUT__", str(REMOTE_ENDPOINT_TIMEOUT_SECONDS)
    ).strip()
    command = shlex.join(
        [
            config.remote_python,
            "-c",
            code,
            config.remote_project_root,
            config.remote_state_root,
            config.remote_python,
            config.remote_installer,
            str(config.expected_user_version),
        ]
    )
    output = _run_checked(
        runner,
        _remote_command(ssh, command),
        timeout=REMOTE_PROBE_COMMAND_TIMEOUT_SECONDS,
    )
    return _json_command_output(output, label="remote publisher probe")


def _active_snapshot_id(active_receipt: Mapping[str, Any]) -> Optional[str]:
    schema = active_receipt.get("schema")
    if schema != "dcar-read-replica-install-receipt-v1":
        return None
    value = active_receipt.get("snapshot_id")
    return (
        str(value)
        if isinstance(value, str) and SNAPSHOT_ID_RE.fullmatch(value)
        else None
    )


def _validate_remote_probe(
    value: object,
    *,
    config: PublishConfig,
    expected_snapshot_id: Optional[str] = None,
    expected_database_sha256: Optional[str] = None,
    expected_runtime_identity: Optional[Mapping[str, Any]] = None,
    expected_manifest_sha256: Optional[str] = None,
) -> dict[str, Any]:
    _require_current_config(config)
    if not isinstance(value, dict) or value.get("schema") != REMOTE_PROBE_SCHEMA:
        raise SnapshotPublishError("remote publisher probe has an invalid shape")
    if "schema_transition" not in value:
        raise SnapshotPublishError("remote probe omitted the schema transition barrier")
    transition = value["schema_transition"]
    if config.expected_user_version == 20:
        support = value.get("installer_schema_support")
        if (not isinstance(support, dict) or not isinstance(support.get("versions"), list)
                or 20 not in support["versions"] or not isinstance(support.get("transitions"), list)
                or [19, 20] not in support["transitions"]):
            raise SnapshotPublishError("remote installed receiver has no verified schema20 support")
        if (not isinstance(transition, dict) or transition.get("schema") != INTEGRATED_TRANSITION_SCHEMA
                or transition.get("from_schema") != 19 or transition.get("to_schema") != 20
                or transition.get("status") != "succeeded"):
            raise SnapshotPublishError("remote 19-to-20 pairing is unsettled; use explicit schema-upgrade first")
    if transition is not None:
        legacy_settled = (isinstance(transition, dict) and config.expected_user_version == 19
            and transition.get("schema") == TRANSITION_SCHEMA and transition.get("status") in {"succeeded", "rolled_back"})
        integrated_settled = (isinstance(transition, dict) and transition.get("schema") == INTEGRATED_TRANSITION_SCHEMA
            and transition.get("from_schema") == 19 and transition.get("to_schema") == 20
            and transition.get("status") == ("succeeded" if config.expected_user_version == 20 else "rolled_back"))
        if not (legacy_settled or integrated_settled):
            raise SnapshotPublishError("remote schema-upgrade transition is unsettled; normal publishing is blocked")
        _parse_iso(transition.get("completed_at"), label="schema transition completed_at")
    current_release = value.get("current_release")
    if (
        not isinstance(current_release, str)
        or not current_release.startswith("/")
        or value.get("python_ready") is not True
        or value.get("installer_ready") is not True
    ):
        raise SnapshotPublishError("remote release or publisher installer is not ready")
    services = value.get("services")
    if not isinstance(services, dict) or services != {
        "dcar-api.service": "active",
        "dcar-web.service": "active",
        "dcar-auth.service": "active",
        "dcar-douyin-control.service": "active",
    }:
        raise SnapshotPublishError("remote Dcar services are not all active")
    directories = value.get("directories")
    if not isinstance(directories, dict) or not directories or not all(
        item is True for item in directories.values()
    ):
        raise SnapshotPublishError("remote snapshot directories are incomplete")
    free_bytes = value.get("free_bytes")
    if not isinstance(free_bytes, int) or free_bytes <= 0:
        raise SnapshotPublishError("remote publisher probe returned invalid free space")
    health = value.get("health")
    overview = value.get("overview")
    scheduler = value.get("scheduler")
    if not isinstance(health, dict) or health.get("status") != "ok":
        raise SnapshotPublishError("remote API health is not ok")
    if health.get("read_only") is not True:
        raise SnapshotPublishError("remote API is not read-only")
    snapshot_contract = _validate_snapshot_contract(health.get("snapshot_contract"), label="remote health")
    if not isinstance(overview, dict) or overview.get("status") != "ready":
        raise SnapshotPublishError("remote overview is not ready")
    if (
        not isinstance(scheduler, dict)
        or scheduler.get("read_only") is not True
        or scheduler.get("requested")
        or scheduler.get("enabled")
    ):
        raise SnapshotPublishError("remote scheduler safety state is invalid")
    database_state = health.get("database_state")
    if not isinstance(database_state, dict):
        raise SnapshotPublishError("remote health omitted database identity")
    database_sha256 = database_state.get("sha256")
    if not isinstance(database_sha256, str) or SHA256_RE.fullmatch(
        database_sha256
    ) is None:
        raise SnapshotPublishError("remote health database SHA-256 is invalid")
    if database_state.get("user_version") != config.expected_user_version:
        raise SnapshotPublishError("remote health schema version is invalid")
    runtime_identity = _validate_runtime_identity(
        database_state.get("runtime_identity"), label="remote health", expected_schema=config.expected_user_version,
    )
    active_receipt = value.get("active_receipt")
    if not isinstance(active_receipt, dict) or _active_snapshot_id(active_receipt) is None:
        raise SnapshotPublishError("remote active snapshot receipt is invalid")
    if active_receipt.get("schema") == "dcar-read-replica-install-receipt-v1":
        receipt_shas = active_receipt.get("database_sha256")
        if (
            not isinstance(receipt_shas, dict)
            or receipt_shas.get("dcar_insight.sqlite3") != database_sha256
            or active_receipt.get("runtime_identity") != runtime_identity
            or active_receipt.get("snapshot_contract") != snapshot_contract
            or active_receipt.get("artifact_policy") != ARTIFACT_POLICY
            or not isinstance(active_receipt.get("manifest_sha256"), str)
            or SHA256_RE.fullmatch(active_receipt["manifest_sha256"]) is None
        ):
            raise SnapshotPublishError(
                "remote active receipt does not match the active database"
            )
    if expected_snapshot_id is not None and _active_snapshot_id(
        active_receipt
    ) != expected_snapshot_id:
        raise SnapshotPublishError("remote active snapshot_id does not match the publish")
    if (
        expected_database_sha256 is not None
        and database_sha256 != expected_database_sha256
    ):
        raise SnapshotPublishError("remote active database SHA-256 does not match")
    if (
        expected_runtime_identity is not None
        and runtime_identity != dict(expected_runtime_identity)
    ):
        raise SnapshotPublishError("remote active runtime identity does not match")
    if expected_manifest_sha256 is not None and active_receipt["manifest_sha256"] != expected_manifest_sha256:
        raise SnapshotPublishError("remote active manifest SHA-256 does not match")
    return {
        "current_release": current_release,
        "free_bytes": free_bytes,
        "snapshot_id": _active_snapshot_id(active_receipt),
        "database_sha256": database_sha256,
        "runtime_identity": runtime_identity,
        "snapshot_contract": snapshot_contract,
        "manifest_sha256": active_receipt["manifest_sha256"],
        "active_receipt": dict(active_receipt),
        "schema_transition": transition,
    }


def _check_remote_sudo(
    config: PublishConfig,
    ssh: Sequence[str],
    *,
    runner: CommandRunner,
) -> None:
    command = shlex.join(
        ["sudo", "-n", config.remote_python, config.remote_installer, "--help"]
    )
    _run_checked(runner, _remote_command(ssh, command), timeout=30)


def remote_check(
    config: PublishConfig, *, runner: CommandRunner = subprocess.run
) -> dict[str, Any]:
    _require_current_config(config)
    ssh = _check_ssh_alias(config, runner=runner)
    _check_remote_sudo(config, ssh, runner=runner)
    result = _validate_remote_probe(
        _remote_probe(config, ssh, runner=runner), config=config
    )
    return {
        "status": "remote-check-ok",
        "publisher_intent": "remote_only",
        "current_release": result["current_release"],
        "snapshot_id": result["snapshot_id"],
        "database_sha256": result["database_sha256"],
        "database_schema_version": config.expected_user_version,
        "report_version": EXPECTED_REPORT_VERSION,
        "remote_free_bytes": result["free_bytes"],
        "services": ["dcar-api.service", "dcar-web.service", "dcar-auth.service", "dcar-douyin-control.service"],
        "snapshot_contract": descriptor(),
        "no_snapshot_built": True,
        "no_remote_write": True,
    }


def _remote_command(ssh: Sequence[str], command: str) -> list[str]:
    return [*ssh, command]


def _bundle_byte_size(path: Path) -> int:
    return sum(
        candidate.stat().st_size
        for candidate in path.rglob("*")
        if candidate.is_file() and not candidate.is_symlink()
    )


def _rsync_transfer_bytes(
    runner: CommandRunner,
    arguments: Sequence[str],
    *,
    timeout: int,
) -> int:
    environment = os.environ.copy()
    environment.update({"LANG": "C", "LC_ALL": "C"})
    output = _run_rsync_checked(
        runner,
        [*arguments[:-2], "--dry-run", "--stats", *arguments[-2:]],
        timeout=timeout,
        environment=environment,
    )
    match = re.search(
        r"^Total transferred file size: ([0-9,]+) (?:bytes|B)$", output, re.M
    )
    if match is None:
        raise SnapshotPublishError("rsync dry-run did not report a transfer size")
    return int(match.group(1).replace(",", ""))


# GNU rsync escapes control characters in --out-format (for example, ``\x1f``
# becomes the literal text ``\#037``).  A printable separator is stable across
# the macOS and server rsync implementations; split only once because paths may
# contain the same character.
RSYNC_ITEM_SEPARATOR = "|"


def _parse_rsync_transfer_plan(output: str) -> RsyncTransferPlan:
    match = re.search(
        r"^Total transferred file size: ([0-9,]+) (?:bytes|B)$", output, re.M
    )
    if match is None:
        raise SnapshotPublishError("rsync dry-run did not report a transfer size")
    changed_paths: list[str] = []
    seen_paths: set[str] = set()
    for line in output.splitlines():
        if RSYNC_ITEM_SEPARATOR not in line:
            continue
        itemized, relative_path = line.split(RSYNC_ITEM_SEPARATOR, 1)
        if itemized.startswith((">f", "<f")):
            if not relative_path or relative_path in seen_paths:
                raise SnapshotPublishError(
                    "rsync dry-run reported an invalid changed artifact path"
                )
            changed_paths.append(relative_path)
            seen_paths.add(relative_path)
    byte_size = int(match.group(1).replace(",", ""))
    if byte_size > 0 and not changed_paths:
        raise SnapshotPublishError(
            "rsync dry-run omitted changed artifact identities"
        )
    return RsyncTransferPlan(byte_size=byte_size, changed_paths=tuple(changed_paths))


def _rsync_transfer_plan(
    runner: CommandRunner,
    arguments: Sequence[str],
    *,
    timeout: int,
) -> RsyncTransferPlan:
    environment = os.environ.copy()
    environment.update({"LANG": "C", "LC_ALL": "C"})
    output = _run_rsync_checked(
        runner,
        [
            *arguments[:-2],
            "--dry-run",
            "--stats",
            "--8-bit-output",
            f"--out-format=%i{RSYNC_ITEM_SEPARATOR}%n",
            *arguments[-2:],
        ],
        timeout=timeout,
        environment=environment,
    )
    return _parse_rsync_transfer_plan(output)


def _remote_rsync_transfer_plan(
    config: PublishConfig,
    ssh: Sequence[str],
    arguments: Sequence[str],
    *,
    runner: CommandRunner,
    timeout: int,
) -> RsyncTransferPlan:
    remote_arguments = [
        *arguments[:-2],
        "--dry-run",
        "--stats",
        "--8-bit-output",
        f"--out-format=%i{RSYNC_ITEM_SEPARATOR}%n",
        *arguments[-2:],
    ]
    output = _run_rsync_checked(
        runner,
        _remote_command(
            ssh, "env LANG=C LC_ALL=C " + shlex.join(remote_arguments)
        ),
        timeout=timeout,
    )
    return _parse_rsync_transfer_plan(output)


def _manifest_artifact_bytes(
    manifest: Mapping[str, Any], *, root: str, paths: Sequence[str]
) -> int:
    index: dict[str, int] = {}
    for item in manifest["files"]:
        if item.get("root") != root:
            continue
        relative_path = item.get("path")
        byte_size = item.get("byte_size")
        if (
            not isinstance(relative_path, str)
            or not relative_path
            or any(ord(character) < 32 for character in relative_path)
            or not isinstance(byte_size, int)
            or byte_size < 0
            or relative_path in index
        ):
            raise SnapshotPublishError(
                "snapshot artifact manifest cannot support a safe install headroom check"
            )
        index[relative_path] = byte_size
    unknown = set(paths).difference(index)
    if unknown:
        raise SnapshotPublishError(
            "rsync dry-run changed paths are not bound to the snapshot manifest"
        )
    return sum(index[path] for path in paths)


def _remote_install_backup_bytes(
    config: PublishConfig,
    ssh: Sequence[str],
    *,
    cache_paths: Sequence[str],
    report_paths: Sequence[str],
    runner: CommandRunner,
) -> dict[str, int]:
    payload = json.dumps(
        {
            "schema": "dcar-install-headroom-v1",
            "artifacts": {
                "cache": list(cache_paths),
                "reports": list(report_paths),
            },
        },
        separators=(",", ":"),
        sort_keys=True,
    )
    code = """
import json
import stat
import sys
from pathlib import Path, PurePosixPath

state_root = Path(sys.argv[1]).resolve(strict=True)
value = json.load(sys.stdin)
if value.get("schema") != "dcar-install-headroom-v1":
    raise RuntimeError("install headroom request schema is invalid")
artifacts = value.get("artifacts")
if not isinstance(artifacts, dict) or set(artifacts) != {"cache", "reports"}:
    raise RuntimeError("install headroom artifact request is invalid")
artifact_bytes = 0
for root_name in ("cache", "reports"):
    root = (state_root / root_name).resolve(strict=True)
    paths = artifacts[root_name]
    if not isinstance(paths, list) or not all(isinstance(item, str) for item in paths):
        raise RuntimeError("install headroom artifact paths are invalid")
    for raw_path in paths:
        relative = PurePosixPath(raw_path)
        if relative.is_absolute() or any(part in {"", ".", ".."} for part in relative.parts):
            raise RuntimeError("install headroom artifact path is unsafe")
        target = root.joinpath(*relative.parts).resolve()
        if root not in target.parents:
            raise RuntimeError("install headroom artifact path escapes its root")
        try:
            metadata = target.lstat()
        except FileNotFoundError:
            continue
        if stat.S_ISLNK(metadata.st_mode) or not stat.S_ISREG(metadata.st_mode):
            raise RuntimeError("install headroom active artifact is unsafe")
        artifact_bytes += metadata.st_size
database_bytes = 0
database_root = (state_root / "db").resolve(strict=True)
for name in ("dcar_insight.sqlite3", "web_mvp.sqlite3"):
    target = database_root / name
    try:
        metadata = target.lstat()
    except FileNotFoundError:
        continue
    if stat.S_ISLNK(metadata.st_mode) or not stat.S_ISREG(metadata.st_mode):
        raise RuntimeError("install headroom active database is unsafe")
    database_bytes += metadata.st_size
print(json.dumps({
    "schema": "dcar-install-headroom-v1",
    "artifact_backup_bytes": artifact_bytes,
    "database_backup_bytes": database_bytes,
}, sort_keys=True))
""".strip()
    command = shlex.join(
        [config.remote_python, "-c", code, config.remote_state_root]
    )
    output = _run_checked(
        runner,
        _remote_command(ssh, command),
        timeout=60 * 60,
        input_text=payload,
    )
    value = _json_command_output(output, label="remote install headroom probe")
    if value.get("schema") != "dcar-install-headroom-v1":
        raise SnapshotPublishError("remote install headroom probe has an invalid shape")
    result: dict[str, int] = {}
    for key in ("artifact_backup_bytes", "database_backup_bytes"):
        byte_size = value.get(key)
        if not isinstance(byte_size, int) or byte_size < 0:
            raise SnapshotPublishError(
                "remote install headroom probe returned invalid byte counts"
            )
        result[key] = byte_size
    return result


def _remote_free_bytes(
    config: PublishConfig,
    ssh: Sequence[str],
    *,
    runner: CommandRunner,
) -> int:
    code = "import os,sys; s=os.statvfs(sys.argv[1]); print(s.f_bavail*s.f_frsize)"
    command = shlex.join([config.remote_python, "-c", code, config.remote_state_root])
    output = _run_checked(runner, _remote_command(ssh, command), timeout=30)
    try:
        value = int(output)
    except ValueError as exc:
        raise SnapshotPublishError(
            "remote free-space probe returned invalid output"
        ) from exc
    if value <= 0:
        raise SnapshotPublishError("remote free-space probe returned no usable space")
    return value


def _prune_remote_snapshots(
    config: PublishConfig,
    ssh: Sequence[str],
    *,
    runner: CommandRunner,
) -> dict[str, Any]:
    remote = shlex.join(
        [
            "sudo",
            "-n",
            config.remote_python,
            config.remote_installer,
            "prune",
            "--incoming-root",
            config.remote_incoming_root,
            "--retain-count",
            str(REMOTE_SNAPSHOT_RETAIN_COUNT),
        ]
    )
    output = _run_checked(runner, _remote_command(ssh, remote), timeout=10 * 60)
    try:
        value = json.loads(output)
    except (TypeError, json.JSONDecodeError) as exc:
        raise SnapshotPublishError(
            "remote snapshot retention returned invalid JSON"
        ) from exc
    if (
        not isinstance(value, dict)
        or value.get("schema") != "dcar-read-replica-prune-receipt-v1"
        or value.get("retain_count") != REMOTE_SNAPSHOT_RETAIN_COUNT
    ):
        raise SnapshotPublishError("remote snapshot retention receipt is invalid")
    return dict(value)


def _remote_installer_operation(
    config: PublishConfig,
    ssh: Sequence[str],
    operation: str,
    *,
    runner: CommandRunner,
    bundle: Optional[str] = None,
    snapshot_id: Optional[str] = None,
) -> dict[str, Any]:
    arguments = [
        "sudo",
        "-n",
        config.remote_python,
        config.remote_installer,
        operation,
    ]
    if bundle is not None:
        arguments.extend(["--bundle", bundle])
    if snapshot_id is not None:
        arguments.extend(["--snapshot-id", snapshot_id])
    if config.expected_user_version == 20:
        arguments.extend(["--expected-schema", "20"])
    output = _run_checked(
        runner,
        _remote_command(ssh, shlex.join(arguments)),
        timeout=60 * 60,
    )
    value = _json_command_output(output, label=f"remote {operation}")
    if operation == "verify":
        if value.get("status") != "verified" or value.get("snapshot_id") is None:
            raise SnapshotPublishError("remote verify receipt is invalid")
    elif operation == "install":
        if value.get("schema") != "dcar-read-replica-install-receipt-v1":
            raise SnapshotPublishError("remote install receipt is invalid")
    elif operation == "rollback":
        if value.get("schema") != "dcar-read-replica-rollback-receipt-v1":
            raise SnapshotPublishError("remote rollback receipt is invalid")
    return value


def _rsync_rsh(ssh: Sequence[str]) -> str:
    return shlex.join(ssh[:-1])


def _write_json_atomic(path: Path, value: Mapping[str, Any]) -> None:
    descriptor, temporary_name = tempfile.mkstemp(
        prefix=f".{path.name}.", dir=path.parent
    )
    temporary = Path(temporary_name)
    try:
        payload = (
            json.dumps(value, ensure_ascii=False, indent=2, sort_keys=True) + "\n"
        ).encode("utf-8")
        with os.fdopen(descriptor, "wb") as handle:
            handle.write(payload)
            handle.flush()
            os.fsync(handle.fileno())
        os.replace(temporary, path)
        directory_descriptor = os.open(path.parent, os.O_RDONLY)
        try:
            os.fsync(directory_descriptor)
        finally:
            os.close(directory_descriptor)
    finally:
        if temporary.exists():
            temporary.unlink()


def _record_publisher_status(config: PublishConfig, result: Mapping[str, Any]) -> None:
    """Observable attempt outcome; never used as authorization or publish proof."""
    root = config.snapshot_root
    root.mkdir(parents=True, exist_ok=True, mode=0o700)
    if root.is_symlink() or not root.is_dir():
        raise SnapshotPublishError("publisher status directory is unsafe")
    path = root / "publisher-status.json"
    if path.is_symlink():
        raise SnapshotPublishError("publisher status file is unsafe")
    payload = {
        "schema": "dcar-publisher-status-v1",
        "observed_at": datetime.now(timezone.utc).isoformat(timespec="seconds"),
        "status": result.get("status", "published" if result.get("snapshot_id") and result.get("published_at") else "unknown"),
        **{key: result[key] for key in (
            "status", "reason", "snapshot_id", "published_at", "publication_status", "beijing_date",
            "no_snapshot_built", "no_ssh_attempted",
        ) if key in result},
    }
    evidence = result.get("publication_evidence")
    if isinstance(evidence, dict):
        payload["publication_reasons"] = evidence.get("reasons", [])
        payload["reconcile_from"] = evidence.get("reconcile_from")
    _write_json_atomic(path, payload)


def _write_json_exclusive(path: Path, value: Mapping[str, Any]) -> None:
    """Durably create one immutable receipt; an existing name is never reused."""
    payload = (
        json.dumps(value, ensure_ascii=False, indent=2, sort_keys=True) + "\n"
    ).encode("utf-8")
    flags = (
        os.O_WRONLY
        | os.O_CREAT
        | os.O_EXCL
        | getattr(os, "O_CLOEXEC", 0)
        | getattr(os, "O_NOFOLLOW", 0)
    )
    try:
        descriptor_number = os.open(path, flags, 0o600)
    except FileExistsError as error:
        raise SnapshotPublishError(
            f"refusing to overwrite existing exclusive receipt: {path.name}"
        ) from error
    created = True
    try:
        with os.fdopen(descriptor_number, "wb") as handle:
            handle.write(payload)
            handle.flush()
            os.fsync(handle.fileno())
        os.chmod(path, 0o600)
        directory_descriptor = os.open(path.parent, os.O_RDONLY)
        try:
            os.fsync(directory_descriptor)
        finally:
            os.close(directory_descriptor)
    except BaseException:
        if created:
            try:
                path.unlink()
            except FileNotFoundError:
                pass
        raise


@contextmanager
def _publisher_lock(snapshot_root: Path) -> Iterator[None]:
    snapshot_root.mkdir(parents=True, exist_ok=True, mode=0o700)
    if snapshot_root.is_symlink() or not snapshot_root.is_dir():
        raise SnapshotPublishError("snapshot root is unsafe")
    os.chmod(snapshot_root, 0o700)
    lock_path = snapshot_root / "publisher.lock"
    descriptor = os.open(lock_path, os.O_CREAT | os.O_RDWR, 0o600)
    try:
        try:
            fcntl.flock(descriptor, fcntl.LOCK_EX | fcntl.LOCK_NB)
        except BlockingIOError as exc:
            raise SnapshotPublishError(
                "another snapshot publisher is already running"
            ) from exc
        yield
    finally:
        fcntl.flock(descriptor, fcntl.LOCK_UN)
        os.close(descriptor)


def _automatic_state_path(snapshot_root: Path) -> Path:
    return snapshot_root / AUTOMATIC_STATE_FILENAME


def _pending_state_path(snapshot_root: Path) -> Path:
    return snapshot_root / PENDING_STATE_FILENAME


def _validate_pending_state(value: object) -> dict[str, Any]:
    if isinstance(value, dict) and value.get("schema") == "dcar-snapshot-publisher-pending-v1":
        raise SnapshotPublishError("legacy pending publish requires explicit settlement before the schema-upgrade; retained unchanged")
    if not isinstance(value, dict) or set(value) != PENDING_STATE_KEYS:
        raise SnapshotPublishError("publisher pending state has an invalid shape")
    if value.get("schema") != PENDING_STATE_SCHEMA:
        raise SnapshotPublishError("publisher pending state schema is invalid")
    snapshot_id = value.get("snapshot_id")
    database_sha256 = value.get("database_sha256")
    previous_snapshot_id = value.get("previous_snapshot_id")
    previous_database_sha256 = value.get("previous_database_sha256")
    output_name = value.get("output_name")
    if not isinstance(snapshot_id, str) or SNAPSHOT_ID_RE.fullmatch(snapshot_id) is None:
        raise SnapshotPublishError("publisher pending snapshot_id is invalid")
    if not isinstance(database_sha256, str) or SHA256_RE.fullmatch(
        database_sha256
    ) is None:
        raise SnapshotPublishError("publisher pending database SHA-256 is invalid")
    if previous_snapshot_id is not None and (
        not isinstance(previous_snapshot_id, str)
        or SNAPSHOT_ID_RE.fullmatch(previous_snapshot_id) is None
    ):
        raise SnapshotPublishError("publisher previous snapshot_id is invalid")
    if not isinstance(previous_database_sha256, str) or SHA256_RE.fullmatch(
        previous_database_sha256
    ) is None:
        raise SnapshotPublishError("publisher previous database SHA-256 is invalid")
    if not isinstance(value.get("previous_manifest_sha256"), str) or SHA256_RE.fullmatch(value["previous_manifest_sha256"]) is None:
        raise SnapshotPublishError("publisher previous manifest SHA-256 is invalid")
    if not isinstance(output_name, str) or LOCAL_SNAPSHOT_DIR_RE.fullmatch(
        output_name
    ) is None:
        raise SnapshotPublishError("publisher pending output directory is invalid")
    _validate_runtime_identity(value.get("runtime_identity"), label="publisher pending")
    _validate_runtime_identity(
        value.get("previous_runtime_identity"), label="publisher previous"
    )
    _validate_snapshot_contract(value.get("snapshot_contract"), label="publisher pending")
    _validate_snapshot_contract(value.get("previous_snapshot_contract"), label="publisher previous")
    beijing_date_value = value.get("beijing_date")
    if beijing_date_value is not None:
        try:
            date.fromisoformat(str(beijing_date_value))
        except ValueError as exc:
            raise SnapshotPublishError(
                "publisher pending Beijing date is invalid"
            ) from exc
    receipt = value.get("receipt")
    if (
        not isinstance(receipt, dict)
        or receipt.get("snapshot_id") != snapshot_id
        or receipt.get("database_sha256") != database_sha256
        or value.get("receipt_sha256") != pipeline_cutover.digest(receipt)
        or receipt.get("runtime_identity") != value.get("runtime_identity")
        or receipt.get("snapshot_contract") != value.get("snapshot_contract")
    ):
        raise SnapshotPublishError("publisher pending receipt is invalid")
    day = date.fromisoformat(str(beijing_date_value)) if beijing_date_value else date.fromisoformat(receipt["publication_evidence"]["beijing_date"])
    _state_from_receipt(receipt, beijing_date=day, output_name=output_name)
    return dict(value)


def _read_pending_state(snapshot_root: Path) -> Optional[dict[str, Any]]:
    path = _pending_state_path(snapshot_root)
    if path.is_symlink():
        raise SnapshotPublishError("publisher pending state must not be a symlink")
    if not path.exists():
        return None
    if not path.is_file():
        raise SnapshotPublishError("publisher pending state must be a regular file")
    metadata = path.stat()
    if metadata.st_uid != os.getuid() or stat.S_IMODE(metadata.st_mode) not in {
        0o400,
        0o600,
    }:
        raise SnapshotPublishError("publisher pending state permissions are unsafe")
    try:
        value = json.loads(path.read_text(encoding="utf-8"))
    except (OSError, UnicodeDecodeError, json.JSONDecodeError) as exc:
        raise SnapshotPublishError("publisher pending state is unreadable") from exc
    return _validate_pending_state(value)


def _clear_pending_state(snapshot_root: Path) -> None:
    path = _pending_state_path(snapshot_root)
    if path.is_symlink():
        raise SnapshotPublishError("publisher pending state must not be a symlink")
    if not path.exists():
        return
    path.unlink()
    descriptor = os.open(snapshot_root, os.O_RDONLY)
    try:
        os.fsync(descriptor)
    finally:
        os.close(descriptor)


def _state_from_receipt(
    value: Mapping[str, Any], *, beijing_date: date, output_name: str
) -> dict[str, Any]:
    if value.get("schema") != PUBLISHER_RECEIPT_SCHEMA:
        raise SnapshotPublishError("automatic publisher receipt schema is invalid")
    snapshot_id, database_sha256 = value.get("snapshot_id"), value.get("database_sha256")
    if not isinstance(snapshot_id, str) or SNAPSHOT_ID_RE.fullmatch(snapshot_id) is None:
        raise SnapshotPublishError("automatic publisher receipt snapshot_id is invalid")
    if not isinstance(database_sha256, str) or SHA256_RE.fullmatch(database_sha256) is None:
        raise SnapshotPublishError("automatic publisher receipt database SHA-256 is invalid")
    if LOCAL_SNAPSHOT_DIR_RE.fullmatch(output_name) is None:
        raise SnapshotPublishError("automatic publisher output directory is invalid")
    published_at = _parse_iso(value.get("published_at"), label="automatic publisher receipt published_at")
    identity = _validate_runtime_identity(value.get("runtime_identity"), label="automatic publisher receipt")
    snapshot_contract = _validate_snapshot_contract(value.get("snapshot_contract"), label="automatic publisher receipt")
    evidence = _validate_publication_evidence(value.get("publication_evidence"))
    sha = pipeline_cutover.digest(evidence)
    if (value.get("publication_evidence_sha256") != sha or value.get("publication_status") != evidence["status"]
            or evidence["beijing_date"] != beijing_date.isoformat()
            or not isinstance(value.get("manifest_sha256"), str)
            or SHA256_RE.fullmatch(value["manifest_sha256"]) is None):
        raise SnapshotPublishError("automatic publisher receipt is not bound to verified current-day evidence")
    return {
        "schema": AUTOMATIC_STATE_SCHEMA, "beijing_date": beijing_date.isoformat(),
        "snapshot_id": snapshot_id, "published_at": published_at.isoformat(),
        "database_sha256": database_sha256, "runtime_identity": identity,
        "snapshot_contract": snapshot_contract, "publication_evidence_sha256": sha,
        "publisher_receipt_sha256": pipeline_cutover.digest(value), "output_name": output_name,
    }


def _validate_automatic_state(value: object) -> dict[str, Any]:
    if not isinstance(value, dict) or set(value) != AUTOMATIC_STATE_KEYS or value.get("schema") != AUTOMATIC_STATE_SCHEMA:
        raise SnapshotPublishError("automatic publisher state has an invalid shape or version")
    try:
        day = date.fromisoformat(value["beijing_date"])
    except (ValueError, TypeError) as error:
        raise SnapshotPublishError("automatic publisher state Beijing date is invalid") from error
    if day.isoformat() != value["beijing_date"]:
        raise SnapshotPublishError("automatic publisher state Beijing date is not canonical")
    if not isinstance(value["snapshot_id"], str) or SNAPSHOT_ID_RE.fullmatch(value["snapshot_id"]) is None:
        raise SnapshotPublishError("automatic publisher state snapshot_id is invalid")
    for key in ("database_sha256", "publication_evidence_sha256", "publisher_receipt_sha256"):
        if not isinstance(value[key], str) or SHA256_RE.fullmatch(value[key]) is None:
            raise SnapshotPublishError("automatic publisher state hash binding is invalid")
    if not isinstance(value["output_name"], str) or LOCAL_SNAPSHOT_DIR_RE.fullmatch(value["output_name"]) is None:
        raise SnapshotPublishError("automatic publisher state output directory is invalid")
    _validate_runtime_identity(value["runtime_identity"], label="automatic publisher state")
    _validate_snapshot_contract(value["snapshot_contract"], label="automatic publisher state")
    _parse_iso(value["published_at"], label="automatic publisher state published_at")
    return dict(value)


def _read_automatic_state(snapshot_root: Path, *, beijing_date: date | None = None) -> Optional[dict[str, Any]]:
    path = _automatic_state_path(snapshot_root)
    if path.is_symlink():
        raise SnapshotPublishError("automatic publisher state must not be a symlink")
    if not path.exists():
        return None
    if not path.is_file():
        raise SnapshotPublishError("automatic publisher state must be a regular file")
    metadata = path.stat()
    if metadata.st_uid != os.getuid() or stat.S_IMODE(metadata.st_mode) not in {
        0o400,
        0o600,
    }:
        raise SnapshotPublishError(
            "automatic publisher state ownership or mode is unsafe"
        )
    try:
        value = json.loads(path.read_text(encoding="utf-8"))
    except (OSError, UnicodeDecodeError, json.JSONDecodeError) as exc:
        raise SnapshotPublishError("automatic publisher state is unreadable") from exc
    # A prior day's verified runtime may have a previous schema/report version.
    # It cannot deduplicate today's publication, nor may it block a new release.
    if (beijing_date is not None and isinstance(value, dict)
            and set(value) == AUTOMATIC_STATE_KEYS and value.get("schema") == AUTOMATIC_STATE_SCHEMA):
        try:
            state_day = date.fromisoformat(value["beijing_date"])
        except (ValueError, TypeError) as error:
            raise SnapshotPublishError("automatic publisher state Beijing date is invalid") from error
        if state_day.isoformat() == value["beijing_date"] and state_day < beijing_date:
            return None
    return _validate_automatic_state(value)


def _recover_automatic_state(
    snapshot_root: Path, *, beijing_date: date
) -> Optional[dict[str, Any]]:
    if not snapshot_root.exists():
        return None
    if snapshot_root.is_symlink() or not snapshot_root.is_dir():
        raise SnapshotPublishError("snapshot root is unsafe")
    for receipt_path in sorted(
        snapshot_root.glob("snapshot-*/publisher-receipt.json"), reverse=True
    ):
        if receipt_path.is_symlink() or not receipt_path.is_file():
            continue
        try:
            value = json.loads(receipt_path.read_text(encoding="utf-8"))
            state = _state_from_receipt(value, beijing_date=beijing_date, output_name=receipt_path.parent.name)
        except (
            OSError,
            UnicodeDecodeError,
            json.JSONDecodeError,
            SnapshotPublishError,
        ):
            continue
        _write_json_atomic(_automatic_state_path(snapshot_root), state)
        return state
    return None


def _daily_automatic_success(
    snapshot_root: Path, *, beijing_date: date
) -> Optional[dict[str, Any]]:
    state = _read_automatic_state(snapshot_root, beijing_date=beijing_date)
    if state is not None and state["beijing_date"] == beijing_date.isoformat():
        receipt_path = snapshot_root / state["output_name"] / "publisher-receipt.json"
        if receipt_path.parent.is_symlink() or receipt_path.is_symlink() or not receipt_path.is_file():
            raise SnapshotPublishError("automatic publisher receipt is missing or unsafe")
        try:
            value = json.loads(receipt_path.read_text(encoding="utf-8"))
            actual = _state_from_receipt(value, beijing_date=beijing_date, output_name=receipt_path.parent.name)
        except (ValueError, OSError, TypeError) as error:
            raise SnapshotPublishError("automatic publisher receipt is unreadable") from error
        if actual != state:
            raise SnapshotPublishError("automatic publisher state no longer matches its sealed receipt")
        return state
    return _recover_automatic_state(snapshot_root, beijing_date=beijing_date)


def _prune_local_snapshots(
    snapshot_root: Path, *, retain_count: int = LOCAL_SNAPSHOT_RETAIN_COUNT
) -> list[str]:
    if retain_count < 1:
        raise SnapshotPublishError("local snapshot retain count must be positive")
    if snapshot_root.is_symlink() or not snapshot_root.is_dir():
        raise SnapshotPublishError("snapshot root is unsafe")
    candidates = sorted(
        (
            path
            for path in snapshot_root.iterdir()
            if LOCAL_SNAPSHOT_DIR_RE.fullmatch(path.name)
            and not path.is_symlink()
            and path.is_dir()
        ),
        key=lambda path: path.name,
        reverse=True,
    )
    deleted: list[str] = []
    for path in candidates[retain_count:]:
        if path.is_symlink() or not path.is_dir():
            raise SnapshotPublishError(
                f"local snapshot changed while pruning: {path}"
            )
        try:
            shutil.rmtree(path)
        except OSError as exc:
            raise SnapshotPublishError(
                f"cannot prune local snapshot: {path}"
            ) from exc
        deleted.append(path.name)
    if deleted:
        descriptor = os.open(snapshot_root, os.O_RDONLY)
        try:
            os.fsync(descriptor)
        finally:
            os.close(descriptor)
    return sorted(deleted)


def _finish_pending_publish(
    config: PublishConfig, pending: Mapping[str, Any]
) -> dict[str, Any]:
    output = config.snapshot_root / str(pending["output_name"])
    if output.parent != config.snapshot_root or output.is_symlink() or not output.is_dir():
        raise SnapshotPublishError("pending snapshot output is missing or unsafe")
    receipt = dict(pending["receipt"])
    _write_json_atomic(output / "publisher-receipt.json", receipt)
    beijing_date_value = pending.get("beijing_date")
    if beijing_date_value is not None:
        beijing_date = date.fromisoformat(str(beijing_date_value))
        state = _state_from_receipt(receipt, beijing_date=beijing_date, output_name=output.name)
        _write_json_atomic(_automatic_state_path(config.snapshot_root), state)
    _clear_pending_state(config.snapshot_root)
    _prune_local_snapshots(config.snapshot_root)
    return receipt


def _validate_pending_source_receipt(
    config: PublishConfig, pending: Mapping[str, Any]
) -> dict[str, Any]:
    output = config.snapshot_root / str(pending["output_name"])
    if output.parent != config.snapshot_root or output.is_symlink() or not output.is_dir():
        raise SnapshotPublishError("pending snapshot output is missing or unsafe")
    manifest_path = _require_regular_local_file(
        output / "manifest.json", label="pending snapshot manifest"
    )
    manifest_bytes = manifest_path.read_bytes()
    try:
        manifest = json.loads(manifest_bytes)
    except (UnicodeDecodeError, json.JSONDecodeError) as error:
        raise SnapshotPublishError("pending snapshot manifest is unreadable") from error
    if not isinstance(manifest, dict):
        raise SnapshotPublishError("pending snapshot manifest is invalid")
    manifest_sha256 = hashlib.sha256(manifest_bytes).hexdigest()
    checksum_path = _require_regular_local_file(
        output / "manifest.sha256", label="pending snapshot manifest checksum"
    )
    if checksum_path.read_text(encoding="ascii").strip() != (
        manifest_sha256 + "  manifest.json"
    ):
        raise SnapshotPublishError("pending snapshot manifest checksum mismatch")
    source_receipt, _freshness = _load_source_receipt(
        output, manifest=manifest, manifest_sha256=manifest_sha256
    )
    receipt = pending["receipt"]
    if (
        source_receipt["snapshot_id"] != pending["snapshot_id"]
        or source_receipt["manifest_sha256"] != receipt.get("manifest_sha256")
        or source_receipt["publication_evidence_sha256"]
        != receipt.get("publication_evidence_sha256")
        or source_receipt["runtime_identity"] != pending["runtime_identity"]
        or source_receipt["snapshot_contract"] != pending["snapshot_contract"]
        or source_receipt["payload_sha256"]
        != receipt.get("source_receipt_payload_sha256")
    ):
        raise SnapshotPublishError(
            "pending publish does not match its sealed source receipt"
        )
    return source_receipt


def _reconcile_pending_publish(
    config: PublishConfig,
    ssh: Sequence[str],
    *,
    runner: CommandRunner,
) -> Optional[dict[str, Any]]:
    pending = _read_pending_state(config.snapshot_root)
    if pending is None:
        return None
    _validate_pending_source_receipt(config, pending)
    remote = _validate_remote_probe(
        _remote_probe(config, ssh, runner=runner), config=config
    )
    if (
        remote["snapshot_id"] == pending["snapshot_id"]
        and remote["database_sha256"] == pending["database_sha256"]
        and remote["runtime_identity"] == pending["runtime_identity"]
        and remote["snapshot_contract"] == pending["snapshot_contract"]
        and remote["manifest_sha256"] == pending["receipt"]["manifest_sha256"]
    ):
        return _finish_pending_publish(config, pending)
    if (
        remote["database_sha256"] == pending["previous_database_sha256"]
        and remote["runtime_identity"] == pending["previous_runtime_identity"]
        and remote["snapshot_contract"] == pending["previous_snapshot_contract"]
        and remote["snapshot_id"] == pending["previous_snapshot_id"]
        and remote["manifest_sha256"] == pending["previous_manifest_sha256"]
    ):
        _clear_pending_state(config.snapshot_root)
        return None
    raise SnapshotPublishError(
        "pending publish matches neither the new nor previous healthy remote snapshot"
    )


def publish_snapshot(
    *,
    project_root: Path,
    database: Optional[Path],
    legacy_database: Optional[Path],
    config: PublishConfig,
    now: Optional[datetime] = None,
    runner: CommandRunner = subprocess.run,
    fetch_json: JsonFetcher = _default_fetch_json,
    build_snapshot: Optional[BuildSnapshot] = None,
    automatic_beijing_date: Optional[date] = None,
    resume_staged_snapshot_id: Optional[str] = None,
    resume_local_snapshot_id: Optional[str] = None,
    _lock_held: bool = False,
) -> dict[str, Any]:
    _require_current_config(config)
    project_root = project_root.resolve()
    current = (now or datetime.now(SHANGHAI)).astimezone(SHANGHAI)
    lock = nullcontext() if _lock_held else _publisher_lock(config.snapshot_root)
    with lock:
        if resume_staged_snapshot_id is not None and resume_local_snapshot_id is not None:
            raise SnapshotPublishError("choose exactly one snapshot resume mode")
        resume_snapshot_id = resume_staged_snapshot_id or resume_local_snapshot_id
        resuming = resume_snapshot_id is not None
        staged_only = resume_staged_snapshot_id is not None
        requested_day = current.date()
        if not resuming:
            if database is None:
                raise SnapshotPublishError(
                    "formal-read publish requires an explicit --db"
                )
            database = _require_regular_local_file(
                database, label="formal writer database"
            )
            legacy_database = (
                _require_regular_local_file(legacy_database, label="legacy database")
                if legacy_database
                else None
            )
        resume_manifest_bytes: Optional[bytes] = None
        if resuming:
            assert resume_snapshot_id is not None
            (
                output,
                manifest,
                current,
                resume_manifest_bytes,
                _resume_manifest_sha256,
            ) = _load_resumable_snapshot(
                config,
                resume_snapshot_id,
                project_root=project_root,
            )
            if resume_local_snapshot_id is not None and current.date() != requested_day:
                raise SnapshotPublishError("local resume must remain in the original Beijing business day")
            source_receipt, sealed_freshness = _load_source_receipt(
                output,
                manifest=manifest,
                manifest_sha256=_resume_manifest_sha256,
            )
            if (
                automatic_beijing_date is not None
                and automatic_beijing_date != current.date()
            ):
                raise SnapshotPublishError(
                    "staged resume Beijing date does not match the frozen attempt"
                )
            automatic_beijing_date = current.date()
            pending_before_resume = _read_pending_state(config.snapshot_root)
            if (
                pending_before_resume is not None
                and pending_before_resume["snapshot_id"]
                != resume_snapshot_id
            ):
                raise SnapshotPublishError(
                    "another snapshot publish is already pending"
                )
            (
                manifest_runtime_identity,
                freshness,
                manifest_sha256,
                bundle_byte_size,
            ) = _verify_local_snapshot(
                output,
                manifest,
                current=current,
                config=config,
                project_root=project_root,
                fetch_json=fetch_json,
                sealed_freshness=sealed_freshness,
                expected_snapshot_id=resume_snapshot_id,
                manifest_bytes=resume_manifest_bytes,
            )
        else:
            assert database is not None
            source_observation = _observe_formal_read_source(
                database,
                project_root=project_root,
                now=current,
                config=config,
                fetch_json=fetch_json,
            )
            output = config.snapshot_root / (
                "snapshot-"
                + current.astimezone(timezone.utc).strftime("%Y%m%dT%H%M%SZ")
            )
            builder = build_snapshot
            if builder is None:
                builder = _load_builder(project_root).build_snapshot
            manifest = builder(
                project_root=project_root,
                database=database,
                legacy_database=legacy_database,
                output=output,
                expected_user_version=config.expected_user_version,
            )
            (
                manifest_runtime_identity,
                freshness,
                manifest_sha256,
                _bundle_byte_size_before_source_receipt,
            ) = _verify_local_snapshot(
                output,
                manifest,
                current=current,
                config=config,
                project_root=project_root,
                fetch_json=fetch_json,
                sealed_freshness=source_observation.freshness,
                expected_runtime_identity=source_observation.freshness.runtime_identity,
                freeze_observed=True,
            )
            source_receipt = _source_receipt_payload(
                manifest=manifest,
                manifest_sha256=manifest_sha256,
                freshness=freshness,
                database=source_observation.database,
                database_identity=source_observation.database_identity,
                writer_lock=source_observation.writer_lock,
            )
            _validate_source_receipt(
                source_receipt,
                manifest=manifest,
                manifest_sha256=manifest_sha256,
            )
            _write_json_exclusive(
                output / SOURCE_RECEIPT_FILENAME, source_receipt
            )
            _load_source_receipt(
                output,
                manifest=manifest,
                manifest_sha256=manifest_sha256,
            )
            bundle_byte_size = _bundle_byte_size(output)
        pending_before_ssh = _read_pending_state(config.snapshot_root)
        if pending_before_ssh is not None:
            _validate_pending_source_receipt(config, pending_before_ssh)
        ssh = _check_ssh_alias(config, runner=runner)
        reconciled = _reconcile_pending_publish(config, ssh, runner=runner)
        if reconciled is not None:
            if (
                resume_snapshot_id is not None
                and reconciled.get("snapshot_id") != resume_snapshot_id
            ):
                raise SnapshotPublishError(
                    "reconciled pending publish differs from the requested snapshot"
                )
            return reconciled
        previous_remote = _validate_remote_probe(
            _remote_probe(config, ssh, runner=runner), config=config
        )
        remote_prune: Optional[dict[str, Any]] = None
        if not resuming:
            remote_prune = _prune_remote_snapshots(config, ssh, runner=runner)
        snapshot_id = str(manifest.get("snapshot_id") or "")
        free_bytes_before = _remote_free_bytes(config, ssh, runner=runner)
        incoming = config.remote_incoming_root + "/" + snapshot_id
        incoming_artifacts = incoming + "/artifacts"
        incoming_cache = incoming_artifacts + "/cache"
        incoming_reports = incoming_artifacts + "/reports"
        incoming_bundle = incoming + "/bundle"
        cache_transfer_bytes: Optional[int] = None
        reports_transfer_bytes: Optional[int] = None
        bundle_transfer_bytes: Optional[int] = None
        transfer_bytes: Optional[int] = None
        free_bytes_after_dry_run: Optional[int] = None
        if free_bytes_before < config.minimum_remote_free_bytes:
            context = " for staged resume" if resuming else ""
            raise SnapshotPublishError(
                f"remote free space is below the reserve{context}; "
                "active data was not changed"
            )
        if resume_local_snapshot_id is not None:
            _prepare_local_resume_incoming(config, ssh, runner=runner,
                snapshot_id=snapshot_id, manifest_sha256=manifest_sha256)
        elif not resuming:
            create_incoming = " && ".join(
                [
                    f"test ! -e {shlex.quote(incoming)}",
                    "install -d -m 0750 "
                    + " ".join(
                        shlex.quote(path)
                        for path in (
                            incoming,
                            incoming_artifacts,
                            incoming_cache,
                            incoming_reports,
                            incoming_bundle,
                        )
                    ),
                ]
            )
            _run_checked(
                runner,
                _remote_command(ssh, create_incoming),
                timeout=30,
            )
        rsync_base = [
            "rsync",
            "-a",
            "--checksum",
            "--no-owner",
            "--no-group",
            "--no-perms",
            "--delay-updates",
            "--from0",
            "-e",
            _rsync_rsh(ssh),
        ]
        cache_rsync = [
            *rsync_base,
            f"--compare-dest={config.remote_active_cache_root}",
            f"--files-from={output / 'cache-files-from0'}",
            str(project_root / "data/cache") + "/",
            f"{config.ssh_alias}:{incoming_cache}/",
        ]
        reports_rsync = [
            *rsync_base,
            f"--compare-dest={config.remote_active_reports_root}",
            f"--files-from={output / 'reports-files-from0'}",
            str(project_root / "reports") + "/",
            f"{config.ssh_alias}:{incoming_reports}/",
        ]
        bundle_rsync = [
            "rsync",
            "-a",
            "--delay-updates",
            "-e",
            _rsync_rsh(ssh),
            str(output) + "/",
            f"{config.ssh_alias}:{incoming_bundle}/",
        ]
        bundle_basis = None
        if not staged_only:
            bundle_basis = _verified_bundle_basis(config, ssh, runner=runner,
                previous_remote=previous_remote, snapshot_id=snapshot_id)
            # copy-dest keeps a complete independent target; compare-dest omits
            # unchanged files and link-dest would alias the previous snapshot.
            bundle_rsync[2:2] = ["--checksum", "--no-whole-file", f"--copy-dest={bundle_basis}"]
        if staged_only:
            cache_plan = _remote_rsync_transfer_plan(
                config,
                ssh,
                [
                    "rsync",
                    "-a",
                    "--checksum",
                    f"{incoming_cache}/",
                    f"{config.remote_active_cache_root}/",
                ],
                runner=runner,
                timeout=6 * 60 * 60,
            )
            reports_plan = _remote_rsync_transfer_plan(
                config,
                ssh,
                [
                    "rsync",
                    "-a",
                    "--checksum",
                    f"{incoming_reports}/",
                    f"{config.remote_active_reports_root}/",
                ],
                runner=runner,
                timeout=60 * 60,
            )
        else:
            cache_plan = _rsync_transfer_plan(
                runner,
                cache_rsync,
                timeout=6 * 60 * 60,
            )
            reports_plan = _rsync_transfer_plan(
                runner,
                reports_rsync,
                timeout=60 * 60,
            )
        cache_transfer_bytes = cache_plan.byte_size
        reports_transfer_bytes = reports_plan.byte_size
        bundle_transfer_bytes = _rsync_transfer_bytes(
            runner, bundle_rsync, timeout=60 * 60
        )
        if staged_only and bundle_transfer_bytes:
            raise SnapshotPublishError(
                "staged resume bundle is incomplete; active data was not changed"
            )
        cache_activation_paths = cache_plan.changed_paths
        reports_activation_paths = reports_plan.changed_paths
        if resume_local_snapshot_id is not None:
            # A previous transfer can already have files in staging. They no
            # longer count as transport bytes, but still need activation/backup.
            staged_cache_plan = _remote_rsync_transfer_plan(config, ssh,
                ["rsync", "-a", "--checksum", incoming_cache + "/",
                 config.remote_active_cache_root + "/"], runner=runner, timeout=6 * 60 * 60)
            staged_reports_plan = _remote_rsync_transfer_plan(config, ssh,
                ["rsync", "-a", "--checksum", incoming_reports + "/",
                 config.remote_active_reports_root + "/"], runner=runner, timeout=60 * 60)
            cache_activation_paths = tuple(sorted(set(cache_activation_paths) |
                                                   set(staged_cache_plan.changed_paths)))
            reports_activation_paths = tuple(sorted(set(reports_activation_paths) |
                                                     set(staged_reports_plan.changed_paths)))
        artifact_activation_bytes = _manifest_artifact_bytes(
            manifest, root="cache", paths=cache_activation_paths
        ) + _manifest_artifact_bytes(
            manifest, root="reports", paths=reports_activation_paths
        )
        backup_bytes = _remote_install_backup_bytes(
            config,
            ssh,
            cache_paths=cache_activation_paths,
            report_paths=reports_activation_paths,
            runner=runner,
        )
        install_headroom_total = (
            artifact_activation_bytes
            + backup_bytes["artifact_backup_bytes"]
            + bundle_byte_size
            + backup_bytes["database_backup_bytes"]
        )
        install_headroom = {
            "schema": "dcar-install-headroom-v1",
            "artifact_activation_bytes": artifact_activation_bytes,
            "artifact_backup_bytes": backup_bytes["artifact_backup_bytes"],
            "database_and_metadata_activation_bytes": bundle_byte_size,
            "database_backup_bytes": backup_bytes["database_backup_bytes"],
            "total_bytes": install_headroom_total,
        }
        transfer_bytes = (
            0
            if staged_only
            else cache_transfer_bytes
            + reports_transfer_bytes
            + bundle_transfer_bytes
        )
        # copy-dest may copy unchanged files locally without reporting transfer
        # bytes. Reserve the entire independent bundle, including on resume.
        staging_bytes = (0 if staged_only else cache_transfer_bytes + reports_transfer_bytes
                         + max(bundle_byte_size, bundle_transfer_bytes))
        required_bytes = (
            staging_bytes
            + install_headroom_total
            + config.minimum_remote_free_bytes
        )
        free_bytes_after_dry_run = _remote_free_bytes(config, ssh, runner=runner)
        if free_bytes_after_dry_run < required_bytes:
            raise SnapshotPublishError(
                "remote free space is insufficient for staging and install headroom "
                "after rsync dry-run: "
                f"{free_bytes_after_dry_run} < {required_bytes}; "
                f"staging={staging_bytes}, "
                f"install_headroom={install_headroom['total_bytes']}"
            )
        if not staged_only:
            _run_rsync_checked(
                runner,
                cache_rsync,
                timeout=6 * 60 * 60,
            )
            _run_rsync_checked(
                runner,
                reports_rsync,
                timeout=60 * 60,
            )
            _run_rsync_checked(
                runner,
                bundle_rsync,
                timeout=60 * 60,
            )
        free_bytes_before_install = _remote_free_bytes(config, ssh, runner=runner)
        install_required_bytes = (
            install_headroom_total + config.minimum_remote_free_bytes
        )
        if free_bytes_before_install < install_required_bytes:
            failure_point = (
                "before install" if staged_only else "after staging"
            )
            raise SnapshotPublishError(
                f"remote free space cannot preserve install headroom {failure_point}; "
                f"{free_bytes_before_install} < {install_required_bytes}; "
                "active data was not changed"
            )
        verify_receipt = _remote_installer_operation(
            config,
            ssh,
            "verify",
            runner=runner,
            bundle=incoming_bundle,
        )
        if (verify_receipt.get("status") != "verified" or verify_receipt.get("snapshot_id") != snapshot_id
                or verify_receipt.get("snapshot_contract") != freshness.snapshot_contract
                or verify_receipt.get("manifest_sha256") != manifest_sha256):
            raise SnapshotPublishError("remote verify manifest/consumer contract does not match")
        receipt = {
            "schema": PUBLISHER_RECEIPT_SCHEMA,
            "snapshot_id": snapshot_id,
            "published_at": _utc_now(),
            "publication_status": freshness.evidence["status"],
            "publication_evidence": freshness.evidence,
            "publication_evidence_sha256": pipeline_cutover.digest(freshness.evidence),
            "daily_report_status": freshness.daily_report_status,
            "weekly_report_status": freshness.weekly_report_status,
            "latest_published_at": freshness.latest_published_at,
            "content_count": freshness.content_count,
            "database_sha256": str(manifest["databases"][0]["sha256"]),
            "runtime_identity": manifest_runtime_identity,
            "snapshot_contract": freshness.snapshot_contract,
            "manifest_sha256": manifest_sha256,
            "source_receipt_payload_sha256": source_receipt["payload_sha256"],
            "remote_free_bytes_before": free_bytes_before,
            "remote_free_bytes_after_dry_run": free_bytes_after_dry_run,
            "remote_free_bytes_before_install": free_bytes_before_install,
            "required_remote_bytes": required_bytes,
            "install_headroom": install_headroom,
            "artifact_manifest_bytes": int(manifest["file_byte_size"]),
            "artifact_policy": manifest["artifact_policy"],
            "optional_reuse_manifest_bytes": int(
                manifest.get("optional_reuse_byte_size", 0)
            ),
            "bundle_bytes": bundle_byte_size,
            "bundle_delta_basis": bundle_basis,
            "staging_bytes": staging_bytes,
            "rsync_dry_run_cache_bytes": cache_transfer_bytes,
            "rsync_dry_run_reports_bytes": reports_transfer_bytes,
            "rsync_dry_run_bundle_bytes": bundle_transfer_bytes,
            "rsync_dry_run_transfer_bytes": transfer_bytes,
            "remote_staging_root": incoming,
            "remote_retention": remote_prune,
        }
        if resuming:
            receipt["transport_mode"] = ("reused-complete-staging" if staged_only
                                         else "resumed-verified-local-snapshot")
        pending = {
            "schema": PENDING_STATE_SCHEMA,
            "beijing_date": (
                automatic_beijing_date.isoformat()
                if automatic_beijing_date is not None
                else None
            ),
            "snapshot_id": snapshot_id,
            "database_sha256": receipt["database_sha256"],
            "runtime_identity": manifest_runtime_identity,
            "previous_snapshot_id": previous_remote["snapshot_id"],
            "previous_database_sha256": previous_remote["database_sha256"],
            "previous_manifest_sha256": previous_remote["manifest_sha256"],
            "previous_runtime_identity": previous_remote["runtime_identity"],
            "snapshot_contract": freshness.snapshot_contract,
            "previous_snapshot_contract": previous_remote["snapshot_contract"],
            "output_name": output.name,
            "receipt": receipt,
            "receipt_sha256": pipeline_cutover.digest(receipt),
        }
        _validate_pending_state(pending)
        _write_json_atomic(_pending_state_path(config.snapshot_root), pending)
        try:
            install_receipt = _remote_installer_operation(
                config,
                ssh,
                "install",
                runner=runner,
                bundle=incoming_bundle,
            )
        except SnapshotPublishError as install_error:
            try:
                remote_after_error = _validate_remote_probe(
                    _remote_probe(config, ssh, runner=runner), config=config
                )
            except SnapshotPublishError as probe_error:
                raise SnapshotPublishError(
                    "remote install outcome is unknown; pending state retained"
                ) from probe_error
            if (
                remote_after_error["snapshot_id"] == snapshot_id
                and remote_after_error["database_sha256"]
                == receipt["database_sha256"]
                and remote_after_error["runtime_identity"]
                == manifest_runtime_identity
                and remote_after_error["snapshot_contract"] == freshness.snapshot_contract
                and remote_after_error["manifest_sha256"] == manifest_sha256
            ):
                return _finish_pending_publish(config, pending)
            if (
                remote_after_error["database_sha256"]
                == previous_remote["database_sha256"]
                and remote_after_error["runtime_identity"]
                == previous_remote["runtime_identity"]
                and remote_after_error["snapshot_id"] == previous_remote["snapshot_id"]
                and remote_after_error["snapshot_contract"] == previous_remote["snapshot_contract"]
                and remote_after_error["manifest_sha256"] == previous_remote["manifest_sha256"]
            ):
                _clear_pending_state(config.snapshot_root)
                raise install_error
            raise SnapshotPublishError(
                "failed install did not restore the previous remote snapshot; "
                "pending state retained"
            ) from install_error
        if (
            install_receipt.get("snapshot_id") != snapshot_id
            or install_receipt.get("database_sha256", {}).get(
                "dcar_insight.sqlite3"
            )
            != receipt["database_sha256"]
            or install_receipt.get("runtime_identity") != manifest_runtime_identity
            or install_receipt.get("snapshot_contract") != freshness.snapshot_contract
            or install_receipt.get("artifact_policy") != ARTIFACT_POLICY
            or install_receipt.get("schema") != "dcar-read-replica-install-receipt-v1"
            or install_receipt.get("manifest_sha256") != manifest_sha256
        ):
            install_mismatch: Optional[SnapshotPublishError] = SnapshotPublishError(
                "remote install receipt does not match the verified snapshot"
            )
        else:
            try:
                remote_probe_value = _remote_probe(config, ssh, runner=runner)
            except SnapshotPublishError as probe_error:
                raise SnapshotPublishError(
                    "post-install remote verification is temporarily unavailable; "
                    "healthy remote state was not rolled back and pending state was retained"
                ) from probe_error
            try:
                _validate_remote_probe(
                    remote_probe_value,
                    config=config,
                    expected_snapshot_id=snapshot_id,
                    expected_database_sha256=str(receipt["database_sha256"]),
                    expected_runtime_identity=manifest_runtime_identity,
                    expected_manifest_sha256=manifest_sha256,
                )
            except SnapshotPublishError as exc:
                install_mismatch = exc
            else:
                install_mismatch = None
        if install_mismatch is not None:
            try:
                _remote_installer_operation(
                    config,
                    ssh,
                    "rollback",
                    runner=runner,
                    snapshot_id=snapshot_id,
                )
                _validate_remote_probe(
                    _remote_probe(config, ssh, runner=runner),
                    config=config,
                    expected_snapshot_id=previous_remote["snapshot_id"],
                    expected_database_sha256=str(
                        previous_remote["database_sha256"]
                    ),
                    expected_runtime_identity=previous_remote["runtime_identity"],
                    expected_manifest_sha256=previous_remote["manifest_sha256"],
                )
            except SnapshotPublishError as rollback_error:
                raise SnapshotPublishError(
                    "post-install validation failed and rollback could not be verified; "
                    "pending state retained"
                ) from rollback_error
            _clear_pending_state(config.snapshot_root)
            raise SnapshotPublishError(
                f"post-install validation failed; previous snapshot restored: "
                f"{install_mismatch}"
            ) from install_mismatch
        return _finish_pending_publish(config, pending)


def publish_snapshot_automatically(
    *,
    project_root: Path,
    database: Path,
    legacy_database: Optional[Path],
    config: PublishConfig,
    now: Optional[datetime] = None,
    runner: CommandRunner = subprocess.run,
    fetch_json: JsonFetcher = _default_fetch_json,
    build_snapshot: Optional[BuildSnapshot] = None,
) -> dict[str, Any]:
    _require_current_config(config)
    current = (now or datetime.now(SHANGHAI)).astimezone(SHANGHAI)
    current_day = current.date()
    if current.hour < AUTOMATIC_START_HOUR:
        return {
            "status": "before-automatic-window",
            "beijing_date": current_day.isoformat(),
            "no_snapshot_built": True,
            "no_ssh_attempted": True,
        }
    with _publisher_lock(config.snapshot_root):
        # An unresolved attempt outranks a cached earlier success. In particular
        # an old v1 pending file is preserved and requires operator settlement.
        pending = _read_pending_state(config.snapshot_root)
        if pending is not None:
            _validate_pending_source_receipt(config, pending)
            ssh = _check_ssh_alias(config, runner=runner)
            reconciled = _reconcile_pending_publish(config, ssh, runner=runner)
            if reconciled is not None:
                return reconciled
        prior_success = _daily_automatic_success(
            config.snapshot_root, beijing_date=current_day
        )
        observation = _observe_formal_read_source(
            database, project_root=project_root, now=current,
            config=config, fetch_json=fetch_json,
        )
        if prior_success is not None:
            prior_receipt = json.loads((config.snapshot_root / prior_success["output_name"] / "publisher-receipt.json").read_text())
            if _publication_fingerprint(observation.freshness.evidence) == _publication_fingerprint(prior_receipt["publication_evidence"]):
                return {
                    "status": "already-published-current-evidence",
                    "beijing_date": current_day.isoformat(),
                    "snapshot_id": prior_success["snapshot_id"],
                    "published_at": prior_success["published_at"],
                    "publication_status": observation.freshness.evidence["status"],
                    "publication_evidence": observation.freshness.evidence,
                    "no_snapshot_built": True,
                    "no_ssh_attempted": True,
                }
        # Bound failed build/transfer attempts too: two existing directories
        # plus this attempt can never grow beyond the normal retain count.
        _prune_local_snapshots(config.snapshot_root, retain_count=2)
        return publish_snapshot(
            project_root=project_root,
            database=database,
            legacy_database=legacy_database,
            config=config,
            now=current,
            runner=runner,
            fetch_json=fetch_json,
            build_snapshot=build_snapshot,
            automatic_beijing_date=current_day,
            _lock_held=True,
        )


def _parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--project-root", type=Path, required=True)
    parser.add_argument("--env-file", type=Path, required=True)
    parser.add_argument(
        "--db",
        type=Path,
        help="Required only for formal_read check, automatic, and new-publish modes.",
    )
    parser.add_argument("--legacy-db", type=Path)
    mode = parser.add_mutually_exclusive_group()
    mode.add_argument(
        "--check",
        action="store_true",
        help="Validate local configuration and writer freshness without building or connecting.",
    )
    mode.add_argument(
        "--remote-check",
        action="store_true",
        help="Run read-only SSH, service, receipt, schema, runtime, and API checks.",
    )
    mode.add_argument(
        "--automatic",
        action="store_true",
        help="Publish changed verified current-day report evidence after 09:00; truthful partial coverage is retained.",
    )
    mode.add_argument(
        "--resume-staged-snapshot",
        metavar="SNAPSHOT_ID",
        help="Resume one explicitly selected, completely staged automatic snapshot.",
    )
    mode.add_argument(
        "--resume-local-snapshot", metavar="SNAPSHOT_ID",
        help="Resume transfer of one verified local snapshot within its original Beijing day.",
    )
    return parser


def main() -> int:
    arguments = _parser().parse_args()
    project_root = arguments.project_root.expanduser().resolve()
    config: PublishConfig | None = None
    try:
        config = _read_external_env(arguments.env_file, project_root=project_root)
        if arguments.check:
            if arguments.db is None:
                raise SnapshotPublishError("--check formal_read mode requires --db")
            observation = _observe_formal_read_source(
                arguments.db,
                project_root=project_root,
                now=None,
                config=config,
                fetch_json=_default_fetch_json,
            )
            freshness = observation.freshness
            result: Mapping[str, Any] = {
                "status": "local-check-ok",
                "publisher_intent": "formal_read",
                "publication_status": freshness.evidence["status"],
                "publication_reasons": freshness.evidence.get("reasons", []),
                "reconcile_from": freshness.evidence.get("reconcile_from"),
                "publication_evidence_sha256": pipeline_cutover.digest(freshness.evidence),
                "snapshot_contract": freshness.snapshot_contract,
                "daily_report_status": freshness.daily_report_status,
                "weekly_report_status": freshness.weekly_report_status,
                "latest_published_at": freshness.latest_published_at,
                "no_snapshot_built": True,
                "no_ssh_attempted": True,
            }
        elif arguments.remote_check:
            result = remote_check(config)
        elif arguments.automatic:
            if arguments.db is None:
                raise SnapshotPublishError("--automatic formal_read mode requires --db")
            result = publish_snapshot_automatically(
                project_root=project_root,
                database=arguments.db,
                legacy_database=arguments.legacy_db,
                config=config,
            )
        elif arguments.resume_local_snapshot:
            result = publish_snapshot(
                project_root=project_root, database=None, legacy_database=None, config=config,
                resume_local_snapshot_id=arguments.resume_local_snapshot,
            )
        elif arguments.resume_staged_snapshot:
            result = publish_snapshot(
                project_root=project_root,
                database=None,
                legacy_database=None,
                config=config,
                resume_staged_snapshot_id=arguments.resume_staged_snapshot,
            )
        else:
            if arguments.db is None:
                raise SnapshotPublishError("new formal_read publish requires --db")
            result = publish_snapshot(
                project_root=project_root,
                database=arguments.db,
                legacy_database=arguments.legacy_db,
                config=config,
            )
    except SnapshotPublishError as exc:
        if config is not None and (arguments.automatic or arguments.resume_local_snapshot):
            _record_publisher_status(config, {"status": "blocked", "reason": str(exc)})
        raise SystemExit(f"snapshot publish refused: {exc}") from exc
    if arguments.automatic or arguments.resume_local_snapshot:
        assert config is not None
        _record_publisher_status(config, result)
    print(json.dumps(result, ensure_ascii=False, sort_keys=True))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
