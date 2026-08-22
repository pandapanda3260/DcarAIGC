#!/usr/bin/env python3
"""Publish a verified macOS-writer snapshot to the Ubuntu read replica."""

from __future__ import annotations

import argparse
import fcntl
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
import urllib.error
import urllib.request
from contextlib import contextmanager, nullcontext
from dataclasses import dataclass
from datetime import date, datetime, timedelta, timezone
from pathlib import Path
from types import ModuleType
from typing import Any, Callable, Iterator, Mapping, Optional, Sequence
from zoneinfo import ZoneInfo


SHANGHAI = ZoneInfo("Asia/Shanghai")
TERMINAL_CAPTURE_STATUSES = frozenset({"succeeded", "partial"})
TERMINAL_PIPELINE_STATUSES = frozenset({"succeeded"})
TERMINAL_REPORT_STATUSES = frozenset({"succeeded", "partial"})
REPORT_CATCHUP_JOB_IDS = frozenset({"daily_report", "weekly_report"})
REPORT_CATCHUP_TERMINAL_STATUSES = frozenset({"succeeded", "partial", "skipped"})
SAFE_ALIAS_RE = re.compile(r"[A-Za-z0-9_.-]{1,64}")
SAFE_REMOTE_PATH_RE = re.compile(r"/[A-Za-z0-9_./-]+")
SNAPSHOT_ID_RE = re.compile(r"[0-9]{8}T[0-9]{6}Z-[0-9a-f]{12}")
SHA256_RE = re.compile(r"[0-9a-f]{64}")
RUNTIME_IDENTITY_SCHEMA = "dcar-runtime-identity-v1"
EXPECTED_REPORT_VERSION = "dcar-content-operations-report-v8.7"
EXPECTED_DATABASE_SCHEMA_VERSION = 16
EXPECTED_DATABASE_SCHEMA_MIGRATION = "remove-manual-review"
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
ARTIFACT_POLICY = {
    "name": "thin-server-v1",
    "included": "reports-and-small-text-evidence",
    "optional_reuse": "active-same-path-size-sha256-only",
    "on_optional_missing_or_mismatch": "omitted",
    "delete_unlisted": False,
}
AUTOMATIC_STATE_SCHEMA = "dcar-automatic-snapshot-publisher-state-v1"
AUTOMATIC_STATE_FILENAME = "automatic-publisher-state.json"
AUTOMATIC_START_HOUR = 9
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
        "capture_scheduled_for",
        "database_sha256",
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
    capture_status: str
    capture_scheduled_for: str
    capture_completed_at: str
    media_download_status: str
    media_download_scheduled_for: str
    media_download_completed_at: str
    media_processing_status: str
    media_processing_scheduled_for: str
    media_processing_completed_at: str
    media_cutoff_status: str
    media_cutoff_scheduled_for: str
    media_cutoff_completed_at: str
    daily_report_status: str
    daily_report_scheduled_for: str
    daily_report_completed_at: str
    weekly_report_status: Optional[str]
    weekly_report_scheduled_for: Optional[str]
    weekly_report_completed_at: Optional[str]
    latest_published_at: str
    content_count: int
    runtime_identity: dict[str, Any]


CommandRunner = Callable[..., subprocess.CompletedProcess[str]]
JsonFetcher = Callable[[str], dict[str, Any]]
BuildSnapshot = Callable[..., dict[str, Any]]


def _utc_now() -> str:
    return (
        datetime.now(timezone.utc).isoformat(timespec="seconds").replace("+00:00", "Z")
    )


def _validate_runtime_identity(value: object, *, label: str) -> dict[str, Any]:
    if not isinstance(value, dict) or set(value) != RUNTIME_IDENTITY_KEYS:
        raise SnapshotPublishError(f"{label} runtime identity has an invalid shape")
    expected = {
        "schema": RUNTIME_IDENTITY_SCHEMA,
        "report_version": EXPECTED_REPORT_VERSION,
        "database_schema_version": EXPECTED_DATABASE_SCHEMA_VERSION,
        "database_schema_migration": EXPECTED_DATABASE_SCHEMA_MIGRATION,
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


def _database_runtime_identity(connection: sqlite3.Connection) -> dict[str, Any]:
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
        label="writer database",
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


def _require_regular_local_file(path: Path, *, label: str) -> Path:
    candidate = path.expanduser()
    if candidate.is_symlink() or not candidate.is_file():
        raise SnapshotPublishError(f"{label} must be a regular non-symlink file")
    if candidate.stat().st_size <= 0:
        raise SnapshotPublishError(f"{label} is empty")
    return candidate.resolve()


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
    if expected_user_version <= 0:
        raise SnapshotPublishError("expected SQLite user_version must be positive")
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
        with urllib.request.urlopen(request, timeout=10) as response:
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
) -> WriterFreshness:
    database = _require_regular_local_file(database, label="formal writer database")
    current = (now or datetime.now(SHANGHAI)).astimezone(SHANGHAI)
    health = fetch_json("http://127.0.0.1:8766/api/v8/health")
    if health.get("status") != "ok" or health.get("database") != database.name:
        raise SnapshotPublishError("writer health does not match the formal database")
    health_database_state = health.get("database_state")
    if not isinstance(health_database_state, dict):
        raise SnapshotPublishError("writer health omitted database identity")
    health_runtime_identity = _validate_runtime_identity(
        health_database_state.get("runtime_identity"), label="writer health"
    )
    scheduler = fetch_json("http://127.0.0.1:8766/api/v8/scheduler")
    catchup = scheduler.get("startup_catchup")
    writer_lock = scheduler.get("writer_lock")
    if scheduler.get("requested") is not True or scheduler.get("enabled") is not True:
        raise SnapshotPublishError(
            "designated writer scheduler is not current and enabled"
        )
    if not isinstance(writer_lock, dict) or writer_lock.get("held") is not True:
        raise SnapshotPublishError(
            "designated writer does not hold the single-writer lock"
        )
    if not isinstance(catchup, dict) or catchup.get("mode") != "report_only":
        raise SnapshotPublishError("writer startup catch-up is not report-only")
    if catchup.get("requested") is not True or catchup.get("enabled") is not True:
        raise SnapshotPublishError("writer report-only startup catch-up is not enabled")
    catchup_status = catchup.get("status")
    if catchup_status == "running":
        raise SnapshotPublishError("writer report-only startup catch-up is still running")
    if catchup_status not in {"succeeded", "failed", "deferred"}:
        raise SnapshotPublishError("writer report-only startup catch-up status is invalid")
    catchup_results = catchup.get("results")
    if not isinstance(catchup_results, list):
        raise SnapshotPublishError("writer startup catch-up results are invalid")
    report_occurrences: list[tuple[str, str, str]] = []
    seen_report_occurrences: set[tuple[str, str]] = set()
    for result in catchup_results:
        if not isinstance(result, dict):
            raise SnapshotPublishError("writer startup catch-up result is invalid")
        job_id = result.get("job_id")
        if job_id not in REPORT_CATCHUP_JOB_IDS:
            raise SnapshotPublishError(
                "writer startup catch-up contains a non-report job"
            )
        status = result.get("status")
        if status not in REPORT_CATCHUP_TERMINAL_STATUSES | {
            "failed",
            "deferred",
            "skipped_duplicate",
        }:
            raise SnapshotPublishError(
                "writer startup catch-up contains an invalid report result"
            )
        if (
            catchup_status == "succeeded"
            and status not in REPORT_CATCHUP_TERMINAL_STATUSES
        ):
            raise SnapshotPublishError(
                "successful startup catch-up contains a non-terminal report result"
            )
        scheduled_for = result.get("scheduled_for")
        if not isinstance(scheduled_for, str) or not scheduled_for:
            raise SnapshotPublishError(
                "writer startup catch-up report result lacks scheduled_for"
            )
        occurrence_key = (str(job_id), scheduled_for)
        if occurrence_key in seen_report_occurrences:
            raise SnapshotPublishError(
                "writer startup catch-up contains a duplicate report occurrence"
            )
        seen_report_occurrences.add(occurrence_key)
        if catchup_status == "succeeded":
            report_occurrences.append((str(job_id), scheduled_for, str(status)))
    connection = sqlite3.connect(f"{database.as_uri()}?mode=ro", uri=True)
    connection.row_factory = sqlite3.Row
    try:
        runtime_identity = _database_runtime_identity(connection)
        if runtime_identity != health_runtime_identity:
            raise SnapshotPublishError(
                "writer health runtime identity does not match the formal database"
            )
        for job_id, scheduled_for, reported_status in report_occurrences:
            occurrence = connection.execute(
                """
                SELECT r.status,r.completed_at,a.status attempt_status,
                       a.completed_at attempt_completed_at
                FROM scheduler_runs r
                JOIN scheduler_run_attempts a ON a.scheduler_run_id=r.id
                WHERE r.job_id=? AND r.scheduled_for=?
                ORDER BY a.attempt_number DESC LIMIT 1
                """,
                (job_id, scheduled_for),
            ).fetchone()
            if occurrence is None:
                raise SnapshotPublishError(
                    "writer startup catch-up report occurrence is absent from the database"
                )
            if (
                occurrence["status"] != reported_status
                or occurrence["attempt_status"] != reported_status
                or occurrence["completed_at"] is None
                or occurrence["attempt_completed_at"] is None
            ):
                raise SnapshotPublishError(
                    "writer startup catch-up report result does not match the database"
                )
        capture = connection.execute(
            """
            SELECT scheduled_for,status,started_at,completed_at FROM scheduler_runs
            WHERE job_id='daily_capture'
            ORDER BY scheduled_for DESC LIMIT 1
            """
        ).fetchone()
        media_download = connection.execute(
            """
            SELECT scheduled_for,status,started_at,completed_at FROM scheduler_runs
            WHERE job_id='daily_media_download'
            ORDER BY scheduled_for DESC LIMIT 1
            """
        ).fetchone()
        media_processing = connection.execute(
            """
            SELECT scheduled_for,status,started_at,completed_at FROM scheduler_runs
            WHERE job_id='daily_media_processing'
            ORDER BY scheduled_for DESC LIMIT 1
            """
        ).fetchone()
        media_cutoff = connection.execute(
            """
            SELECT scheduled_for,status,started_at,completed_at FROM scheduler_runs
            WHERE job_id='daily_media_cutoff'
            ORDER BY scheduled_for DESC LIMIT 1
            """
        ).fetchone()
        daily_report = connection.execute(
            """
            SELECT scheduled_for,status,started_at,completed_at FROM scheduler_runs
            WHERE job_id='daily_report'
            ORDER BY scheduled_for DESC LIMIT 1
            """
        ).fetchone()
        weekly_report = (
            connection.execute(
                """
                SELECT scheduled_for,status,started_at,completed_at FROM scheduler_runs
                WHERE job_id='weekly_report'
                ORDER BY scheduled_for DESC LIMIT 1
                """
            ).fetchone()
            if current.weekday() == 0
            else None
        )
        content = connection.execute(
            "SELECT COUNT(*) content_count,MAX(published_at) latest_published_at "
            "FROM content_items"
        ).fetchone()
    except sqlite3.Error as exc:
        raise SnapshotPublishError(
            "writer database lacks scheduler freshness data"
        ) from exc
    finally:
        connection.close()
    if content is None:
        raise SnapshotPublishError("writer database has no capture freshness data")
    capture_status, capture_scheduled, capture_completed = _validate_today_run(
        capture,
        job_id="daily_capture",
        hour=2,
        minute=0,
        allowed_statuses=TERMINAL_CAPTURE_STATUSES,
        current=current,
    )
    (
        media_download_status,
        media_download_scheduled,
        media_download_completed,
    ) = _validate_today_run(
        media_download,
        job_id="daily_media_download",
        hour=2,
        minute=20,
        allowed_statuses=TERMINAL_PIPELINE_STATUSES,
        current=current,
    )
    (
        media_processing_status,
        media_processing_scheduled,
        media_processing_completed,
    ) = _validate_today_run(
        media_processing,
        job_id="daily_media_processing",
        hour=3,
        minute=0,
        allowed_statuses=TERMINAL_PIPELINE_STATUSES,
        current=current,
    )
    cutoff_status, cutoff_scheduled, cutoff_completed = _validate_today_run(
        media_cutoff,
        job_id="daily_media_cutoff",
        hour=7,
        minute=30,
        allowed_statuses=frozenset({"succeeded"}),
        current=current,
    )
    (
        daily_report_status,
        daily_report_scheduled,
        daily_report_completed,
    ) = _validate_today_run(
        daily_report,
        job_id="daily_report",
        hour=8,
        minute=0,
        allowed_statuses=TERMINAL_REPORT_STATUSES,
        current=current,
    )
    weekly_report_status: Optional[str] = None
    weekly_report_scheduled: Optional[datetime] = None
    weekly_report_completed: Optional[datetime] = None
    if current.weekday() == 0:
        (
            weekly_report_status,
            weekly_report_scheduled,
            weekly_report_completed,
        ) = _validate_today_run(
            weekly_report,
            job_id="weekly_report",
            hour=8,
            minute=30,
            allowed_statuses=TERMINAL_REPORT_STATUSES,
            current=current,
        )
    completion_chain = [
        ("daily_capture", capture, capture_completed),
        ("daily_media_download", media_download, media_download_completed),
        ("daily_media_processing", media_processing, media_processing_completed),
        ("daily_media_cutoff", media_cutoff, cutoff_completed),
        ("daily_report", daily_report, daily_report_completed),
    ]
    if weekly_report_completed is not None:
        completion_chain.append(("weekly_report", weekly_report, weekly_report_completed))
    for (upstream_job, _, upstream_completed), (
        downstream_job,
        downstream_row,
        downstream_completed,
    ) in zip(completion_chain, completion_chain[1:]):
        assert downstream_row is not None
        downstream_started = _parse_iso(
            downstream_row["started_at"], label=f"{downstream_job} started_at"
        )
        if (
            downstream_started < upstream_completed
            or downstream_completed < upstream_completed
        ):
            raise SnapshotPublishError(
                f"{downstream_job} started before its dependency {upstream_job} completed"
            )
    content_count = int(content["content_count"])
    if content_count <= 0:
        raise SnapshotPublishError("writer database contains no content")
    latest_published = _parse_iso(
        content["latest_published_at"], label="latest content published_at"
    )
    oldest_allowed_date = current.date() - timedelta(days=maximum_content_lag_days)
    if latest_published.astimezone(SHANGHAI).date() < oldest_allowed_date:
        raise SnapshotPublishError(
            "writer content is stale; refusing to publish another stale online snapshot"
        )
    latest_storage_mtime = max(
        path.stat().st_mtime
        for path in (
            database,
            database.with_name(database.name + "-wal"),
            database.with_name(database.name + "-shm"),
        )
        if path.exists()
    )
    required_completions = [
        capture_completed,
        media_download_completed,
        media_processing_completed,
        cutoff_completed,
        daily_report_completed,
    ]
    if weekly_report_completed is not None:
        required_completions.append(weekly_report_completed)
    newest_required_completion = max(required_completions)
    if datetime.fromtimestamp(
        latest_storage_mtime, timezone.utc
    ) < newest_required_completion - timedelta(minutes=5):
        raise SnapshotPublishError(
            "writer DB/WAL files are older than the completed analysis cutoff"
        )
    return WriterFreshness(
        capture_status=capture_status,
        capture_scheduled_for=str(capture["scheduled_for"]),
        capture_completed_at=str(capture["completed_at"]),
        media_download_status=media_download_status,
        media_download_scheduled_for=str(media_download["scheduled_for"]),
        media_download_completed_at=str(media_download["completed_at"]),
        media_processing_status=media_processing_status,
        media_processing_scheduled_for=str(media_processing["scheduled_for"]),
        media_processing_completed_at=str(media_processing["completed_at"]),
        media_cutoff_status=cutoff_status,
        media_cutoff_scheduled_for=str(media_cutoff["scheduled_for"]),
        media_cutoff_completed_at=str(media_cutoff["completed_at"]),
        daily_report_status=daily_report_status,
        daily_report_scheduled_for=str(daily_report["scheduled_for"]),
        daily_report_completed_at=str(daily_report["completed_at"]),
        weekly_report_status=weekly_report_status,
        weekly_report_scheduled_for=(
            str(weekly_report["scheduled_for"]) if weekly_report is not None else None
        ),
        weekly_report_completed_at=(
            str(weekly_report["completed_at"]) if weekly_report is not None else None
        ),
        latest_published_at=str(content["latest_published_at"]),
        content_count=content_count,
        runtime_identity=runtime_identity,
    )


def _load_builder(project_root: Path) -> ModuleType:
    path = project_root / "scripts/build_server_snapshot.py"
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
) -> str:
    kwargs: dict[str, Any] = {
        "check": False,
        "capture_output": True,
        "text": True,
        "timeout": timeout,
    }
    if environment is not None:
        kwargs["env"] = dict(environment)
    completed = runner(list(arguments), **kwargs)
    if completed.returncode != 0:
        detail = (completed.stderr or completed.stdout or "").strip()[-1000:]
        raise SnapshotPublishError(
            f"command failed ({Path(arguments[0]).name}): {detail}"
        )
    return completed.stdout.strip()


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
        "StrictHostKeyChecking=yes",
        "-o",
        f"UserKnownHostsFile={known_hosts}",
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


def _remote_command(ssh: Sequence[str], command: str) -> list[str]:
    return [*ssh, command]


def _required_remote_bytes(
    manifest: Mapping[str, Any], bundle_byte_size: int, config: PublishConfig
) -> int:
    return (
        int(manifest["file_byte_size"])
        + bundle_byte_size
        + config.minimum_remote_free_bytes
    )


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
    output = _run_checked(
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


def _state_from_receipt(
    value: Mapping[str, Any], *, beijing_date: date
) -> dict[str, Any]:
    if value.get("schema") != "dcar-snapshot-publisher-receipt-v1":
        raise SnapshotPublishError("automatic publisher receipt schema is invalid")
    snapshot_id = value.get("snapshot_id")
    database_sha256 = value.get("database_sha256")
    if (
        not isinstance(snapshot_id, str)
        or SNAPSHOT_ID_RE.fullmatch(snapshot_id) is None
    ):
        raise SnapshotPublishError("automatic publisher receipt snapshot_id is invalid")
    if (
        not isinstance(database_sha256, str)
        or SHA256_RE.fullmatch(database_sha256) is None
    ):
        raise SnapshotPublishError(
            "automatic publisher receipt database SHA-256 is invalid"
        )
    published_at = _parse_iso(
        value.get("published_at"), label="automatic publisher receipt published_at"
    )
    capture_scheduled = _parse_iso(
        value.get("capture_scheduled_for"),
        label="automatic publisher receipt capture_scheduled_for",
    )
    if capture_scheduled.astimezone(SHANGHAI).date() != beijing_date:
        raise SnapshotPublishError(
            "automatic publisher receipt does not cover the requested Beijing day"
        )
    expected_statuses = {
        "capture_status": TERMINAL_CAPTURE_STATUSES,
        "media_download_status": TERMINAL_PIPELINE_STATUSES,
        "media_processing_status": TERMINAL_PIPELINE_STATUSES,
        "media_cutoff_status": TERMINAL_PIPELINE_STATUSES,
        "daily_report_status": TERMINAL_REPORT_STATUSES,
    }
    if beijing_date.weekday() == 0:
        expected_statuses["weekly_report_status"] = TERMINAL_REPORT_STATUSES
    for key, allowed_statuses in expected_statuses.items():
        if value.get(key) not in allowed_statuses:
            raise SnapshotPublishError(
                f"automatic publisher receipt {key} is not successful"
            )
    return {
        "schema": AUTOMATIC_STATE_SCHEMA,
        "beijing_date": beijing_date.isoformat(),
        "snapshot_id": snapshot_id,
        "published_at": published_at.isoformat(),
        "capture_scheduled_for": capture_scheduled.isoformat(),
        "database_sha256": database_sha256,
    }


def _validate_automatic_state(value: object) -> dict[str, Any]:
    if not isinstance(value, dict) or set(value) != AUTOMATIC_STATE_KEYS:
        raise SnapshotPublishError("automatic publisher state has an invalid shape")
    if value.get("schema") != AUTOMATIC_STATE_SCHEMA:
        raise SnapshotPublishError("automatic publisher state schema is invalid")
    beijing_date_value = value.get("beijing_date")
    try:
        parsed_day = date.fromisoformat(str(beijing_date_value))
    except ValueError as exc:
        raise SnapshotPublishError(
            "automatic publisher state Beijing date is invalid"
        ) from exc
    snapshot_id = value.get("snapshot_id")
    database_sha256 = value.get("database_sha256")
    if (
        not isinstance(snapshot_id, str)
        or SNAPSHOT_ID_RE.fullmatch(snapshot_id) is None
    ):
        raise SnapshotPublishError("automatic publisher state snapshot_id is invalid")
    if (
        not isinstance(database_sha256, str)
        or SHA256_RE.fullmatch(database_sha256) is None
    ):
        raise SnapshotPublishError(
            "automatic publisher state database SHA-256 is invalid"
        )
    _parse_iso(
        value.get("published_at"), label="automatic publisher state published_at"
    )
    capture_scheduled = _parse_iso(
        value.get("capture_scheduled_for"),
        label="automatic publisher state capture_scheduled_for",
    )
    if capture_scheduled.astimezone(SHANGHAI).date() != parsed_day:
        raise SnapshotPublishError(
            "automatic publisher state capture day does not match its Beijing date"
        )
    return dict(value)


def _read_automatic_state(snapshot_root: Path) -> Optional[dict[str, Any]]:
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
            state = _state_from_receipt(value, beijing_date=beijing_date)
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
    state = _read_automatic_state(snapshot_root)
    if state is not None and state["beijing_date"] == beijing_date.isoformat():
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


def publish_snapshot(
    *,
    project_root: Path,
    database: Path,
    legacy_database: Optional[Path],
    config: PublishConfig,
    now: Optional[datetime] = None,
    runner: CommandRunner = subprocess.run,
    fetch_json: JsonFetcher = _default_fetch_json,
    build_snapshot: Optional[BuildSnapshot] = None,
    automatic_beijing_date: Optional[date] = None,
    _lock_held: bool = False,
) -> dict[str, Any]:
    project_root = project_root.resolve()
    database = _require_regular_local_file(database, label="formal writer database")
    legacy_database = (
        _require_regular_local_file(legacy_database, label="legacy database")
        if legacy_database
        else None
    )
    current = (now or datetime.now(SHANGHAI)).astimezone(SHANGHAI)
    lock = nullcontext() if _lock_held else _publisher_lock(config.snapshot_root)
    with lock:
        freshness = check_writer_freshness(
            database,
            now=current,
            maximum_content_lag_days=config.maximum_content_lag_days,
            fetch_json=fetch_json,
        )
        ssh = _check_ssh_alias(config, runner=runner)
        remote_prune = _prune_remote_snapshots(config, ssh, runner=runner)
        output = config.snapshot_root / (
            "snapshot-" + current.astimezone(timezone.utc).strftime("%Y%m%dT%H%M%SZ")
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
        manifest_runtime_identity = _validate_runtime_identity(
            manifest.get("runtime_identity"), label="snapshot manifest"
        )
        if manifest_runtime_identity != freshness.runtime_identity:
            raise SnapshotPublishError(
                "snapshot runtime identity drifted from the verified writer"
            )
        snapshot_id = str(manifest.get("snapshot_id") or "")
        if not SNAPSHOT_ID_RE.fullmatch(snapshot_id):
            raise SnapshotPublishError(
                "snapshot builder returned an invalid snapshot_id"
            )
        if manifest.get("artifact_policy") != ARTIFACT_POLICY:
            raise SnapshotPublishError(
                "snapshot builder did not return the required thin-server policy"
            )
        bundle_byte_size = _bundle_byte_size(output)
        free_bytes_before = _remote_free_bytes(config, ssh, runner=runner)
        included_snapshot_bytes = _required_remote_bytes(
            manifest, bundle_byte_size, config
        )
        if free_bytes_before < included_snapshot_bytes:
            raise SnapshotPublishError(
                "remote free space cannot hold the included thin snapshot: "
                f"{free_bytes_before} < {included_snapshot_bytes}"
            )
        incoming = config.remote_incoming_root + "/" + snapshot_id
        incoming_artifacts = incoming + "/artifacts"
        incoming_cache = incoming_artifacts + "/cache"
        incoming_reports = incoming_artifacts + "/reports"
        incoming_bundle = incoming + "/bundle"
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
            "--delay-updates",
            "--from0",
            "-e",
            _rsync_rsh(ssh),
        ]
        cache_rsync = [
            *rsync_base,
            f"--link-dest={config.remote_active_cache_root}",
            f"--files-from={output / 'cache-files-from0'}",
            str(project_root / "data/cache") + "/",
            f"{config.ssh_alias}:{incoming_cache}/",
        ]
        reports_rsync = [
            *rsync_base,
            f"--link-dest={config.remote_active_reports_root}",
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
        cache_transfer_bytes = _rsync_transfer_bytes(
            runner, cache_rsync, timeout=6 * 60 * 60
        )
        reports_transfer_bytes = _rsync_transfer_bytes(
            runner, reports_rsync, timeout=60 * 60
        )
        bundle_transfer_bytes = _rsync_transfer_bytes(
            runner, bundle_rsync, timeout=60 * 60
        )
        transfer_bytes = (
            cache_transfer_bytes + reports_transfer_bytes + bundle_transfer_bytes
        )
        # Optional large evidence is never staged. Require space only for the
        # explicitly included thin set, the bundle, and the configured reserve.
        required_bytes = max(
            included_snapshot_bytes,
            transfer_bytes + config.minimum_remote_free_bytes,
        )
        free_bytes_after_dry_run = _remote_free_bytes(config, ssh, runner=runner)
        if free_bytes_after_dry_run < required_bytes:
            raise SnapshotPublishError(
                "remote free space is insufficient after rsync dry-run: "
                f"{free_bytes_after_dry_run} < {required_bytes}; "
                f"rsync dry-run transfer={transfer_bytes}"
            )
        _run_checked(
            runner,
            cache_rsync,
            timeout=6 * 60 * 60,
        )
        _run_checked(
            runner,
            reports_rsync,
            timeout=60 * 60,
        )
        _run_checked(
            runner,
            bundle_rsync,
            timeout=60 * 60,
        )
        free_bytes_before_install = _remote_free_bytes(config, ssh, runner=runner)
        if free_bytes_before_install < config.minimum_remote_free_bytes:
            raise SnapshotPublishError(
                "remote free space fell below the reserve after staging; "
                "active data was not changed"
            )
        for operation in ("verify", "install"):
            remote = shlex.join(
                [
                    "sudo",
                    "-n",
                    config.remote_python,
                    config.remote_installer,
                    operation,
                    "--bundle",
                    incoming_bundle,
                ]
            )
            _run_checked(runner, _remote_command(ssh, remote), timeout=60 * 60)
        receipt = {
            "schema": "dcar-snapshot-publisher-receipt-v1",
            "snapshot_id": snapshot_id,
            "published_at": _utc_now(),
            "capture_status": freshness.capture_status,
            "capture_scheduled_for": freshness.capture_scheduled_for,
            "capture_completed_at": freshness.capture_completed_at,
            "media_download_status": freshness.media_download_status,
            "media_download_scheduled_for": freshness.media_download_scheduled_for,
            "media_download_completed_at": freshness.media_download_completed_at,
            "media_processing_status": freshness.media_processing_status,
            "media_processing_scheduled_for": (
                freshness.media_processing_scheduled_for
            ),
            "media_processing_completed_at": freshness.media_processing_completed_at,
            "media_cutoff_status": freshness.media_cutoff_status,
            "media_cutoff_scheduled_for": freshness.media_cutoff_scheduled_for,
            "media_cutoff_completed_at": freshness.media_cutoff_completed_at,
            "daily_report_status": freshness.daily_report_status,
            "daily_report_scheduled_for": freshness.daily_report_scheduled_for,
            "daily_report_completed_at": freshness.daily_report_completed_at,
            "weekly_report_status": freshness.weekly_report_status,
            "weekly_report_scheduled_for": freshness.weekly_report_scheduled_for,
            "weekly_report_completed_at": freshness.weekly_report_completed_at,
            "latest_published_at": freshness.latest_published_at,
            "content_count": freshness.content_count,
            "database_sha256": str(manifest["databases"][0]["sha256"]),
            "runtime_identity": manifest_runtime_identity,
            "remote_free_bytes_before": free_bytes_before,
            "remote_free_bytes_after_dry_run": free_bytes_after_dry_run,
            "remote_free_bytes_before_install": free_bytes_before_install,
            "required_remote_bytes": required_bytes,
            "artifact_manifest_bytes": int(manifest["file_byte_size"]),
            "artifact_policy": manifest["artifact_policy"],
            "optional_reuse_manifest_bytes": int(
                manifest.get("optional_reuse_byte_size", 0)
            ),
            "bundle_bytes": bundle_byte_size,
            "rsync_dry_run_cache_bytes": cache_transfer_bytes,
            "rsync_dry_run_reports_bytes": reports_transfer_bytes,
            "rsync_dry_run_bundle_bytes": bundle_transfer_bytes,
            "rsync_dry_run_transfer_bytes": transfer_bytes,
            "remote_staging_root": incoming,
            "remote_retention": remote_prune,
        }
        _write_json_atomic(output / "publisher-receipt.json", receipt)
        if automatic_beijing_date is not None:
            state = _state_from_receipt(receipt, beijing_date=automatic_beijing_date)
            _write_json_atomic(_automatic_state_path(config.snapshot_root), state)
            _prune_local_snapshots(config.snapshot_root)
        return receipt


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
        prior_success = _daily_automatic_success(
            config.snapshot_root, beijing_date=current_day
        )
        if prior_success is not None:
            return {
                "status": "already-published-today",
                "beijing_date": current_day.isoformat(),
                "snapshot_id": prior_success["snapshot_id"],
                "published_at": prior_success["published_at"],
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
    parser.add_argument("--db", type=Path, required=True)
    parser.add_argument("--legacy-db", type=Path)
    mode = parser.add_mutually_exclusive_group()
    mode.add_argument(
        "--check",
        action="store_true",
        help="Validate local configuration and writer freshness without building or connecting.",
    )
    mode.add_argument(
        "--automatic",
        action="store_true",
        help="Publish once after 09:00 when today's complete local pipeline is ready.",
    )
    return parser


def main() -> int:
    arguments = _parser().parse_args()
    project_root = arguments.project_root.expanduser().resolve()
    try:
        config = _read_external_env(arguments.env_file, project_root=project_root)
        if arguments.check:
            freshness = check_writer_freshness(
                arguments.db,
                maximum_content_lag_days=config.maximum_content_lag_days,
            )
            result: Mapping[str, Any] = {
                "status": "local-check-ok",
                "capture_status": freshness.capture_status,
                "media_download_status": freshness.media_download_status,
                "media_processing_status": freshness.media_processing_status,
                "media_cutoff_status": freshness.media_cutoff_status,
                "daily_report_status": freshness.daily_report_status,
                "weekly_report_status": freshness.weekly_report_status,
                "latest_published_at": freshness.latest_published_at,
                "no_snapshot_built": True,
                "no_ssh_attempted": True,
            }
        elif arguments.automatic:
            result = publish_snapshot_automatically(
                project_root=project_root,
                database=arguments.db,
                legacy_database=arguments.legacy_db,
                config=config,
            )
        else:
            result = publish_snapshot(
                project_root=project_root,
                database=arguments.db,
                legacy_database=arguments.legacy_db,
                config=config,
            )
    except SnapshotPublishError as exc:
        raise SystemExit(f"snapshot publish refused: {exc}") from exc
    print(json.dumps(result, ensure_ascii=False, sort_keys=True))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
