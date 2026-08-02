"""SQLite storage and forward-only migrations for the local workflow."""

from __future__ import annotations

import sqlite3
from contextlib import contextmanager
from datetime import datetime, timezone
from pathlib import Path
from typing import Iterator


SCHEMA_VERSION = 4


def now_iso() -> str:
    return datetime.now(timezone.utc).isoformat()


def connect(db_path: Path) -> sqlite3.Connection:
    db_path.parent.mkdir(parents=True, exist_ok=True)
    connection = sqlite3.connect(db_path, timeout=10)
    connection.row_factory = sqlite3.Row
    connection.execute("PRAGMA foreign_keys = ON")
    connection.execute("PRAGMA journal_mode = WAL")
    connection.execute("PRAGMA busy_timeout = 10000")
    return connection


@contextmanager
def transaction(connection: sqlite3.Connection) -> Iterator[sqlite3.Connection]:
    try:
        connection.execute("BEGIN IMMEDIATE")
        yield connection
        connection.commit()
    except Exception:
        connection.rollback()
        raise


MIGRATION_1 = """
CREATE TABLE IF NOT EXISTS schema_migrations (
    version INTEGER PRIMARY KEY,
    applied_at TEXT NOT NULL
);

CREATE TABLE IF NOT EXISTS runs (
    id TEXT PRIMARY KEY,
    created_at TEXT NOT NULL,
    updated_at TEXT NOT NULL,
    mode TEXT NOT NULL,
    channel TEXT NOT NULL,
    status TEXT NOT NULL,
    progress INTEGER NOT NULL DEFAULT 0,
    input_count INTEGER NOT NULL DEFAULT 0,
    message TEXT NOT NULL DEFAULT '',
    output_path TEXT,
    output_sha256 TEXT
);

CREATE TABLE IF NOT EXISTS content_items (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    platform TEXT NOT NULL CHECK (platform IN ('douyin', 'xiaohongshu')),
    platform_content_id TEXT NOT NULL,
    canonical_url TEXT NOT NULL,
    source_group TEXT NOT NULL DEFAULT '',
    source_label TEXT NOT NULL DEFAULT '',
    account_uid TEXT NOT NULL DEFAULT '',
    account_name TEXT NOT NULL DEFAULT '',
    account_quality TEXT NOT NULL DEFAULT '',
    caption TEXT NOT NULL DEFAULT '',
    content_type TEXT NOT NULL DEFAULT '',
    published_at TEXT NOT NULL DEFAULT '',
    exposure_value INTEGER,
    exposure_status TEXT NOT NULL DEFAULT 'missing',
    source_path TEXT NOT NULL,
    source_line INTEGER,
    imported_at TEXT NOT NULL,
    UNIQUE(platform, platform_content_id)
);

CREATE INDEX IF NOT EXISTS idx_content_items_platform
ON content_items(platform, platform_content_id);

CREATE TABLE IF NOT EXISTS content_import_audit (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    platform TEXT NOT NULL,
    source_path TEXT NOT NULL,
    source_line INTEGER NOT NULL,
    platform_content_id TEXT NOT NULL DEFAULT '',
    status TEXT NOT NULL,
    duplicate_of TEXT NOT NULL DEFAULT '',
    reason TEXT NOT NULL DEFAULT '',
    imported_at TEXT NOT NULL,
    UNIQUE(platform, source_path, source_line)
);

CREATE TABLE IF NOT EXISTS evidence_assets (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    content_item_id INTEGER NOT NULL REFERENCES content_items(id) ON DELETE CASCADE,
    evidence_type TEXT NOT NULL,
    local_path TEXT NOT NULL,
    status TEXT NOT NULL CHECK (status IN ('available', 'missing', 'failed', 'pending')),
    byte_size INTEGER,
    fingerprint TEXT NOT NULL DEFAULT '',
    indexed_at TEXT NOT NULL,
    UNIQUE(content_item_id, evidence_type)
);

CREATE INDEX IF NOT EXISTS idx_evidence_assets_content
ON evidence_assets(content_item_id, evidence_type);

CREATE TABLE IF NOT EXISTS corpus_snapshots (
    id TEXT PRIMARY KEY,
    created_at TEXT NOT NULL,
    douyin_count INTEGER NOT NULL,
    xiaohongshu_count INTEGER NOT NULL,
    content_sha256 TEXT NOT NULL,
    evidence_sha256 TEXT NOT NULL,
    summary_json TEXT NOT NULL
);
"""


MIGRATION_2_TABLES = """
CREATE TABLE IF NOT EXISTS run_events (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    run_id TEXT NOT NULL REFERENCES runs(id) ON DELETE CASCADE,
    created_at TEXT NOT NULL,
    event_type TEXT NOT NULL,
    message TEXT NOT NULL DEFAULT '',
    payload_json TEXT NOT NULL DEFAULT '{}'
);

CREATE INDEX IF NOT EXISTS idx_run_events_run
ON run_events(run_id, id);

CREATE TABLE IF NOT EXISTS formal_baseline (
    singleton_id INTEGER PRIMARY KEY CHECK (singleton_id = 1),
    run_id TEXT NOT NULL REFERENCES runs(id),
    selected_at TEXT NOT NULL
);
"""


RUN_V2_COLUMNS = {
    "run_kind": "TEXT NOT NULL DEFAULT 'temporary'",
    "scope": "TEXT NOT NULL DEFAULT 'single_channel'",
    "rule_version": "TEXT NOT NULL DEFAULT ''",
    "report_version": "TEXT NOT NULL DEFAULT ''",
    "is_formal_baseline": "INTEGER NOT NULL DEFAULT 0",
    "cancel_requested": "INTEGER NOT NULL DEFAULT 0",
    "attempt_count": "INTEGER NOT NULL DEFAULT 0",
    "max_attempts": "INTEGER NOT NULL DEFAULT 2",
    "last_error_code": "TEXT NOT NULL DEFAULT ''",
    "report_revision": "INTEGER NOT NULL DEFAULT 0",
    "report_stale": "INTEGER NOT NULL DEFAULT 0",
    "corpus_snapshot_id": "TEXT NOT NULL DEFAULT ''",
    "provider_calls": "INTEGER NOT NULL DEFAULT 0",
}


MIGRATION_3 = """
CREATE TABLE IF NOT EXISTS evaluations (
    content_item_id INTEGER PRIMARY KEY REFERENCES content_items(id) ON DELETE CASCADE,
    rule_version TEXT NOT NULL,
    taxonomy_version TEXT NOT NULL,
    evaluation_status TEXT NOT NULL,
    evidence_level TEXT NOT NULL,
    evidence_summary TEXT NOT NULL DEFAULT '',
    primary_selling_point_id TEXT NOT NULL DEFAULT '',
    primary_selling_point_label TEXT NOT NULL DEFAULT '',
    primary_tier TEXT NOT NULL DEFAULT '',
    business_scene TEXT NOT NULL DEFAULT '',
    selling_point_score INTEGER,
    selling_point_qualitative TEXT NOT NULL DEFAULT '',
    selling_point_included INTEGER NOT NULL DEFAULT 0,
    pending_review INTEGER NOT NULL DEFAULT 0,
    secondary_selling_point_ids_json TEXT NOT NULL DEFAULT '[]',
    no_match_reason TEXT NOT NULL DEFAULT '',
    content_automotive_score INTEGER,
    content_automotive_qualitative TEXT NOT NULL DEFAULT '',
    valid_unique_commenters INTEGER,
    comment_sample_status TEXT NOT NULL DEFAULT 'technical_missing',
    audience_automotive_score INTEGER,
    audience_automotive_qualitative TEXT NOT NULL DEFAULT '',
    dcar_task_fit_score INTEGER,
    action_intent_score INTEGER,
    acquisition_potential INTEGER,
    acquisition_potential_qualitative TEXT NOT NULL DEFAULT '',
    evaluated_at TEXT NOT NULL
);

CREATE INDEX IF NOT EXISTS idx_evaluations_scene
ON evaluations(business_scene, primary_tier);

CREATE TABLE IF NOT EXISTS comment_user_scores (
    content_item_id INTEGER NOT NULL REFERENCES content_items(id) ON DELETE CASCADE,
    anonymous_user_key TEXT NOT NULL,
    audience_automotive_score INTEGER NOT NULL,
    action_intent_score INTEGER NOT NULL,
    evaluated_at TEXT NOT NULL,
    PRIMARY KEY(content_item_id, anonymous_user_key)
);

CREATE TABLE IF NOT EXISTS provider_usage (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    run_id TEXT REFERENCES runs(id),
    provider TEXT NOT NULL,
    operation TEXT NOT NULL,
    request_attempts INTEGER NOT NULL DEFAULT 0,
    billed_requests INTEGER NOT NULL DEFAULT 0,
    currency TEXT NOT NULL DEFAULT '',
    amount REAL,
    recorded_at TEXT NOT NULL,
    details_json TEXT NOT NULL DEFAULT '{}'
);
"""


MIGRATION_4 = """
CREATE TABLE IF NOT EXISTS run_evaluations (
    run_id TEXT NOT NULL REFERENCES runs(id) ON DELETE CASCADE,
    content_item_id INTEGER NOT NULL REFERENCES content_items(id) ON DELETE CASCADE,
    evaluation_json TEXT NOT NULL,
    updated_at TEXT NOT NULL,
    PRIMARY KEY(run_id, content_item_id)
);

CREATE TABLE IF NOT EXISTS manual_reviews (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    run_id TEXT NOT NULL REFERENCES runs(id) ON DELETE CASCADE,
    content_item_id INTEGER NOT NULL REFERENCES content_items(id) ON DELETE CASCADE,
    previous_evaluation_json TEXT NOT NULL,
    patch_json TEXT NOT NULL,
    reason TEXT NOT NULL,
    reviewer TEXT NOT NULL DEFAULT 'local-user',
    applied_revision INTEGER NOT NULL,
    created_at TEXT NOT NULL
);

CREATE INDEX IF NOT EXISTS idx_manual_reviews_run
ON manual_reviews(run_id, id);

CREATE TABLE IF NOT EXISTS report_revisions (
    run_id TEXT NOT NULL REFERENCES runs(id) ON DELETE CASCADE,
    revision INTEGER NOT NULL,
    created_at TEXT NOT NULL,
    report_json_path TEXT NOT NULL,
    report_markdown_path TEXT NOT NULL,
    summary_image_path TEXT NOT NULL,
    output_sha256 TEXT NOT NULL,
    source_evaluation_sha256 TEXT NOT NULL,
    is_current INTEGER NOT NULL DEFAULT 1,
    PRIMARY KEY(run_id, revision)
);
"""


def _column_names(connection: sqlite3.Connection, table: str) -> set[str]:
    return {str(row["name"]) for row in connection.execute(f"PRAGMA table_info({table})")}


def migrate(connection: sqlite3.Connection) -> int:
    connection.execute(
        "CREATE TABLE IF NOT EXISTS schema_migrations (version INTEGER PRIMARY KEY, applied_at TEXT NOT NULL)"
    )
    current_row = connection.execute("SELECT MAX(version) AS version FROM schema_migrations").fetchone()
    current = int(current_row["version"] or 0)
    if current < 1:
        with transaction(connection):
            connection.executescript(MIGRATION_1)
            connection.execute(
                "INSERT OR REPLACE INTO schema_migrations(version, applied_at) VALUES (?, ?)",
                (1, now_iso()),
            )
        current = 1
    if current < 2:
        with transaction(connection):
            existing = _column_names(connection, "runs")
            for name, definition in RUN_V2_COLUMNS.items():
                if name not in existing:
                    connection.execute(f"ALTER TABLE runs ADD COLUMN {name} {definition}")
            connection.executescript(MIGRATION_2_TABLES)
            connection.execute(
                "INSERT OR REPLACE INTO schema_migrations(version, applied_at) VALUES (?, ?)",
                (2, now_iso()),
            )
        current = 2
    if current < 3:
        with transaction(connection):
            connection.executescript(MIGRATION_3)
            connection.execute(
                "INSERT OR REPLACE INTO schema_migrations(version, applied_at) VALUES (?, ?)",
                (3, now_iso()),
            )
        current = 3
    if current < 4:
        with transaction(connection):
            connection.executescript(MIGRATION_4)
            connection.execute(
                "INSERT OR REPLACE INTO schema_migrations(version, applied_at) VALUES (?, ?)",
                (4, now_iso()),
            )
        current = 4
    if current != SCHEMA_VERSION:
        raise RuntimeError(f"unsupported schema version {current}; expected {SCHEMA_VERSION}")
    return current
