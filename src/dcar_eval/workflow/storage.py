"""SQLite storage and forward-only migrations for the local workflow."""

from __future__ import annotations

import sqlite3
from contextlib import contextmanager
from datetime import datetime, timezone
from pathlib import Path
from typing import Iterator


SCHEMA_VERSION = 1


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
    if current != SCHEMA_VERSION:
        raise RuntimeError(f"unsupported schema version {current}; expected {SCHEMA_VERSION}")
    return current

