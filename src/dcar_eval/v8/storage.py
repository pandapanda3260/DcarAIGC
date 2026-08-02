"""Independent SQLite storage for DCar Insight v8."""

from __future__ import annotations

import sqlite3
from contextlib import contextmanager
from datetime import datetime, timezone
from pathlib import Path
from typing import Iterator


PROJECT_ROOT = Path(__file__).resolve().parents[3]
DEFAULT_DB = PROJECT_ROOT / "app" / "data" / "dcar_insight.sqlite3"
SCHEMA_VERSION = 3


def now_utc() -> str:
    return datetime.now(timezone.utc).isoformat(timespec="seconds").replace("+00:00", "Z")


def connect(path: Path = DEFAULT_DB) -> sqlite3.Connection:
    path.parent.mkdir(parents=True, exist_ok=True)
    connection = sqlite3.connect(path, timeout=10)
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


SCHEMA_SQL = r"""
CREATE TABLE IF NOT EXISTS schema_migrations (
    version INTEGER PRIMARY KEY,
    name TEXT NOT NULL UNIQUE,
    applied_at TEXT NOT NULL
);

CREATE TABLE IF NOT EXISTS accounts (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    phone TEXT NOT NULL,
    phone_normalized TEXT NOT NULL UNIQUE,
    operator_name TEXT NOT NULL DEFAULT '',
    account_type TEXT NOT NULL DEFAULT 'unknown'
        CHECK(account_type IN ('boutique_ip','original','mixed_edit','unknown')),
    content_direction TEXT NOT NULL DEFAULT 'unknown'
        CHECK(content_direction IN ('new_car','used_car','media','other','unknown')),
    enabled INTEGER NOT NULL DEFAULT 1 CHECK(enabled IN (0,1)),
    created_at TEXT NOT NULL,
    updated_at TEXT NOT NULL
);

CREATE TABLE IF NOT EXISTS account_platform_identities (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    account_id INTEGER NOT NULL REFERENCES accounts(id) ON DELETE CASCADE,
    platform TEXT NOT NULL CHECK(platform IN ('douyin','xiaohongshu','wechat_channels','kuaishou')),
    uid TEXT NOT NULL,
    nickname TEXT NOT NULL DEFAULT '',
    real_name_status TEXT NOT NULL DEFAULT 'unknown'
        CHECK(real_name_status IN ('yes','no','unknown')),
    source TEXT NOT NULL DEFAULT 'manual',
    created_at TEXT NOT NULL,
    updated_at TEXT NOT NULL,
    UNIQUE(platform, uid),
    UNIQUE(account_id, platform)
);

CREATE TABLE IF NOT EXISTS account_provider_references (
    account_identity_id INTEGER NOT NULL REFERENCES account_platform_identities(id) ON DELETE CASCADE,
    provider TEXT NOT NULL,
    reference_kind TEXT NOT NULL,
    reference_value TEXT NOT NULL,
    source_raw_response_id INTEGER,
    created_at TEXT NOT NULL,
    updated_at TEXT NOT NULL,
    PRIMARY KEY(account_identity_id, provider, reference_kind)
);

CREATE TABLE IF NOT EXISTS content_items (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    link_id TEXT NOT NULL UNIQUE CHECK(length(link_id)=6),
    platform TEXT NOT NULL CHECK(platform IN ('douyin','xiaohongshu','wechat_channels','kuaishou')),
    platform_content_id TEXT,
    canonical_url TEXT NOT NULL,
    normalized_url_hash TEXT,
    account_id INTEGER REFERENCES accounts(id) ON DELETE SET NULL,
    raw_account_uid TEXT,
    raw_account_name TEXT,
    legacy_account_type TEXT,
    title TEXT NOT NULL DEFAULT '',
    body TEXT NOT NULL DEFAULT '',
    content_type TEXT NOT NULL DEFAULT 'unknown',
    published_at TEXT,
    published_at_raw TEXT,
    manual_content_direction TEXT,
    evaluation_content_direction TEXT,
    source_group TEXT NOT NULL DEFAULT '',
    source_label TEXT NOT NULL DEFAULT '',
    source_path TEXT NOT NULL DEFAULT '',
    source_line INTEGER,
    imported_at TEXT NOT NULL,
    created_at TEXT NOT NULL,
    updated_at TEXT NOT NULL,
    CHECK(manual_content_direction IS NULL OR manual_content_direction IN ('new_car','used_car','media','other','unknown')),
    CHECK(evaluation_content_direction IS NULL OR evaluation_content_direction IN ('new_car','used_car','media','other','unknown'))
);

CREATE UNIQUE INDEX IF NOT EXISTS uq_content_platform_id
ON content_items(platform, platform_content_id)
WHERE platform_content_id IS NOT NULL;

CREATE UNIQUE INDEX IF NOT EXISTS uq_content_fallback_url
ON content_items(platform, normalized_url_hash)
WHERE platform IN ('wechat_channels','kuaishou')
  AND platform_content_id IS NULL
  AND normalized_url_hash IS NOT NULL;

CREATE INDEX IF NOT EXISTS idx_content_published_at ON content_items(published_at);
CREATE INDEX IF NOT EXISTS idx_content_account ON content_items(account_id);

CREATE TABLE IF NOT EXISTS content_identities (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    content_id INTEGER NOT NULL REFERENCES content_items(id) ON DELETE CASCADE,
    identity_kind TEXT NOT NULL CHECK(identity_kind IN ('platform_content_id','canonical_url')),
    identity_value TEXT NOT NULL,
    platform_identity_key TEXT NOT NULL,
    is_primary INTEGER NOT NULL DEFAULT 0 CHECK(is_primary IN (0,1)),
    created_at TEXT NOT NULL,
    UNIQUE(platform_identity_key)
);

CREATE TABLE IF NOT EXISTS content_aliases (
    alias_link_id TEXT PRIMARY KEY CHECK(length(alias_link_id)=6),
    content_id INTEGER NOT NULL REFERENCES content_items(id) ON DELETE CASCADE,
    reason TEXT NOT NULL,
    created_at TEXT NOT NULL
);

CREATE TABLE IF NOT EXISTS import_batches (
    id TEXT PRIMARY KEY,
    entity_type TEXT NOT NULL CHECK(entity_type IN ('account','content','legacy_migration')),
    source_name TEXT NOT NULL,
    status TEXT NOT NULL CHECK(status IN ('previewed','committed','failed')),
    total_rows INTEGER NOT NULL DEFAULT 0,
    inserted_rows INTEGER NOT NULL DEFAULT 0,
    updated_rows INTEGER NOT NULL DEFAULT 0,
    rejected_rows INTEGER NOT NULL DEFAULT 0,
    created_at TEXT NOT NULL,
    committed_at TEXT
);

CREATE TABLE IF NOT EXISTS import_rows (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    batch_id TEXT NOT NULL REFERENCES import_batches(id) ON DELETE CASCADE,
    source_row INTEGER NOT NULL,
    status TEXT NOT NULL CHECK(status IN ('inserted','updated','rejected','duplicate_in_file')),
    entity_id INTEGER,
    identity_key TEXT NOT NULL DEFAULT '',
    raw_json TEXT NOT NULL,
    reason TEXT NOT NULL DEFAULT '',
    UNIQUE(batch_id, source_row)
);

CREATE TABLE IF NOT EXISTS fetch_slots (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    account_id INTEGER REFERENCES accounts(id) ON DELETE CASCADE,
    content_id INTEGER REFERENCES content_items(id) ON DELETE CASCADE,
    stage TEXT NOT NULL CHECK(stage IN ('discovery','detail','metrics','comments','media_source_refresh')),
    window_key TEXT NOT NULL,
    provider TEXT NOT NULL,
    adapter_version TEXT NOT NULL,
    status TEXT NOT NULL CHECK(status IN ('pending','running','succeeded','retryable_failed','terminal_failed')),
    attempt_count INTEGER NOT NULL DEFAULT 0,
    last_error_code TEXT,
    last_error_message TEXT,
    started_at TEXT,
    finished_at TEXT,
    created_at TEXT NOT NULL,
    updated_at TEXT NOT NULL,
    CHECK((account_id IS NOT NULL) <> (content_id IS NOT NULL)),
    UNIQUE(account_id, content_id, stage, window_key, provider, adapter_version)
);

CREATE INDEX IF NOT EXISTS idx_fetch_slots_due ON fetch_slots(stage, status, window_key);
CREATE UNIQUE INDEX IF NOT EXISTS uq_fetch_content_slot
ON fetch_slots(content_id, stage, window_key, provider, adapter_version)
WHERE content_id IS NOT NULL;
CREATE UNIQUE INDEX IF NOT EXISTS uq_fetch_account_slot
ON fetch_slots(account_id, stage, window_key, provider, adapter_version)
WHERE account_id IS NOT NULL;

CREATE TABLE IF NOT EXISTS fetch_attempts (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    slot_id INTEGER NOT NULL REFERENCES fetch_slots(id) ON DELETE CASCADE,
    attempt_number INTEGER NOT NULL,
    request_started_at TEXT NOT NULL,
    response_finished_at TEXT,
    http_status INTEGER,
    billed INTEGER NOT NULL DEFAULT 0 CHECK(billed IN (0,1)),
    amount REAL,
    currency TEXT,
    error_code TEXT,
    error_message TEXT,
    UNIQUE(slot_id, attempt_number)
);

CREATE TABLE IF NOT EXISTS provider_raw_responses (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    fetch_attempt_id INTEGER REFERENCES fetch_attempts(id) ON DELETE SET NULL,
    account_id INTEGER REFERENCES accounts(id) ON DELETE CASCADE,
    content_id INTEGER REFERENCES content_items(id) ON DELETE CASCADE,
    provider TEXT NOT NULL,
    operation TEXT NOT NULL,
    local_path TEXT NOT NULL,
    sha256 TEXT NOT NULL,
    byte_size INTEGER NOT NULL,
    http_status INTEGER,
    captured_at TEXT NOT NULL,
    source TEXT NOT NULL DEFAULT 'live',
    UNIQUE(content_id, provider, operation, local_path, sha256)
);

CREATE TABLE IF NOT EXISTS provider_usage (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    task_id TEXT,
    budget_batch_id TEXT,
    provider TEXT NOT NULL,
    operation TEXT NOT NULL,
    request_attempts INTEGER NOT NULL DEFAULT 0,
    billed_requests INTEGER NOT NULL DEFAULT 0,
    currency TEXT,
    amount REAL,
    recorded_at TEXT NOT NULL,
    details_json TEXT NOT NULL DEFAULT '{}'
);

CREATE TABLE IF NOT EXISTS provider_budget_batches (
    id TEXT PRIMARY KEY,
    purpose TEXT NOT NULL UNIQUE,
    provider TEXT NOT NULL,
    operation TEXT NOT NULL,
    currency TEXT NOT NULL,
    verified_unit_price REAL NOT NULL,
    max_billable_requests INTEGER NOT NULL,
    max_amount REAL NOT NULL,
    pilot_size INTEGER NOT NULL,
    daily_quota INTEGER NOT NULL,
    consumed_requests INTEGER NOT NULL DEFAULT 0,
    consumed_amount REAL NOT NULL DEFAULT 0,
    price_verified_at TEXT NOT NULL,
    status TEXT NOT NULL CHECK(status IN ('draft','pilot','approved','suspended','exhausted','completed')),
    created_at TEXT NOT NULL,
    updated_at TEXT NOT NULL
);

CREATE TABLE IF NOT EXISTS content_metric_snapshots (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    content_id INTEGER NOT NULL REFERENCES content_items(id) ON DELETE CASCADE,
    captured_at TEXT NOT NULL,
    window_key TEXT NOT NULL,
    view_count INTEGER,
    comment_count INTEGER,
    like_count INTEGER,
    share_count INTEGER,
    collect_count INTEGER,
    status TEXT NOT NULL CHECK(status IN ('available','missing','stale')),
    source TEXT NOT NULL,
    raw_response_id INTEGER REFERENCES provider_raw_responses(id),
    metadata_json TEXT NOT NULL DEFAULT '{}',
    UNIQUE(content_id, window_key, source)
);

CREATE TABLE IF NOT EXISTS comment_evidence_versions (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    content_id INTEGER NOT NULL REFERENCES content_items(id) ON DELETE CASCADE,
    captured_at TEXT NOT NULL,
    iso_week TEXT NOT NULL,
    source TEXT NOT NULL,
    local_path TEXT NOT NULL,
    sha256 TEXT NOT NULL,
    comment_count INTEGER,
    status TEXT NOT NULL CHECK(status IN ('available','missing','failed')),
    created_at TEXT NOT NULL,
    UNIQUE(content_id, iso_week, sha256)
);

CREATE TABLE IF NOT EXISTS comments (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    evidence_version_id INTEGER NOT NULL REFERENCES comment_evidence_versions(id) ON DELETE CASCADE,
    platform_comment_id TEXT,
    anonymous_user_key TEXT,
    body TEXT NOT NULL,
    published_at TEXT,
    like_count INTEGER,
    parent_comment_id TEXT,
    raw_json TEXT NOT NULL DEFAULT '{}',
    UNIQUE(evidence_version_id, platform_comment_id)
);

CREATE TABLE IF NOT EXISTS comment_user_scores (
    content_id INTEGER NOT NULL REFERENCES content_items(id) ON DELETE CASCADE,
    evidence_version_id INTEGER REFERENCES comment_evidence_versions(id) ON DELETE SET NULL,
    anonymous_user_key TEXT NOT NULL,
    audience_automotive_score INTEGER NOT NULL,
    action_intent_score INTEGER NOT NULL,
    evaluated_at TEXT NOT NULL,
    PRIMARY KEY(content_id, anonymous_user_key)
);

CREATE TABLE IF NOT EXISTS evidence_artifacts (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    content_id INTEGER NOT NULL REFERENCES content_items(id) ON DELETE CASCADE,
    artifact_type TEXT NOT NULL,
    local_path TEXT NOT NULL,
    status TEXT NOT NULL CHECK(status IN ('available','missing','failed','pending')),
    byte_size INTEGER,
    sha256 TEXT,
    legacy_fingerprint TEXT,
    captured_at TEXT,
    processor_version TEXT,
    metadata_json TEXT NOT NULL DEFAULT '{}',
    created_at TEXT NOT NULL,
    UNIQUE(content_id, artifact_type, local_path)
);

CREATE TABLE IF NOT EXISTS evidence_envelopes (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    content_id INTEGER NOT NULL REFERENCES content_items(id) ON DELETE CASCADE,
    schema_version TEXT NOT NULL,
    detail_raw_sha256 TEXT,
    text_sha256 TEXT NOT NULL,
    media_sha256 TEXT,
    asr_sha256 TEXT,
    ocr_sha256 TEXT,
    comments_version_sha256 TEXT,
    manual_evidence_sha256 TEXT,
    evidence_sha256 TEXT NOT NULL,
    components_json TEXT NOT NULL,
    created_at TEXT NOT NULL,
    UNIQUE(content_id, evidence_sha256)
);

CREATE TABLE IF NOT EXISTS media_processing_slots (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    content_id INTEGER NOT NULL REFERENCES content_items(id) ON DELETE CASCADE,
    source_sha256 TEXT NOT NULL,
    processor_type TEXT NOT NULL CHECK(processor_type IN ('download','frames','asr','ocr','ocr_merge','duplicate_fingerprint')),
    processor_version TEXT NOT NULL,
    status TEXT NOT NULL CHECK(status IN ('pending','running','succeeded','retryable_failed','terminal_failed')),
    output_artifact_id INTEGER REFERENCES evidence_artifacts(id),
    attempt_count INTEGER NOT NULL DEFAULT 0,
    error_message TEXT,
    created_at TEXT NOT NULL,
    updated_at TEXT NOT NULL,
    UNIQUE(content_id, source_sha256, processor_type, processor_version)
);

CREATE TABLE IF NOT EXISTS taxonomy_versions (
    id TEXT PRIMARY KEY,
    version TEXT NOT NULL UNIQUE,
    status TEXT NOT NULL CHECK(status IN ('draft','published','retired')),
    definition TEXT NOT NULL,
    source_path TEXT,
    source_sha256 TEXT,
    created_at TEXT NOT NULL,
    published_at TEXT
);

CREATE TABLE IF NOT EXISTS selling_points (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    taxonomy_id TEXT NOT NULL REFERENCES taxonomy_versions(id) ON DELETE CASCADE,
    code TEXT NOT NULL,
    tier TEXT NOT NULL CHECK(tier IN ('core','other')),
    label TEXT NOT NULL,
    definition TEXT NOT NULL DEFAULT '',
    positive_evidence_json TEXT NOT NULL DEFAULT '[]',
    negative_evidence_json TEXT NOT NULL DEFAULT '[]',
    boundary_rules_json TEXT NOT NULL DEFAULT '[]',
    enabled INTEGER NOT NULL DEFAULT 1 CHECK(enabled IN (0,1)),
    UNIQUE(taxonomy_id, code)
);

CREATE TABLE IF NOT EXISTS selling_point_scenes (
    selling_point_id INTEGER NOT NULL REFERENCES selling_points(id) ON DELETE CASCADE,
    scene TEXT NOT NULL CHECK(scene IN ('new_car','used_car','media')),
    PRIMARY KEY(selling_point_id, scene)
);

CREATE TABLE IF NOT EXISTS evaluation_versions (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    content_id INTEGER NOT NULL REFERENCES content_items(id) ON DELETE CASCADE,
    evidence_envelope_id INTEGER REFERENCES evidence_envelopes(id) ON DELETE RESTRICT,
    rule_version TEXT NOT NULL,
    taxonomy_version TEXT NOT NULL,
    evidence_sha256 TEXT NOT NULL,
    evaluation_source TEXT NOT NULL CHECK(evaluation_source IN ('automatic','manual_review','migrated_from_v5')),
    evaluation_status TEXT NOT NULL,
    evidence_level TEXT NOT NULL CHECK(evidence_level IN ('V0','V1','V2','V3')),
    primary_selling_point_code TEXT,
    selling_point_score INTEGER,
    selling_point_included INTEGER NOT NULL DEFAULT 0 CHECK(selling_point_included IN (0,1)),
    content_direction TEXT NOT NULL DEFAULT 'unknown'
        CHECK(content_direction IN ('new_car','used_car','media','other','unknown')),
    content_automotive_score INTEGER,
    audience_automotive_score INTEGER,
    acquisition_potential_score INTEGER,
    pending_review INTEGER NOT NULL DEFAULT 0 CHECK(pending_review IN (0,1)),
    payload_json TEXT NOT NULL,
    evaluated_at TEXT NOT NULL,
    UNIQUE(content_id, rule_version, taxonomy_version, evidence_sha256)
);

CREATE INDEX IF NOT EXISTS idx_evaluation_current ON evaluation_versions(content_id, evaluated_at DESC);

CREATE TABLE IF NOT EXISTS evaluation_matches (
    evaluation_id INTEGER NOT NULL REFERENCES evaluation_versions(id) ON DELETE CASCADE,
    selling_point_code TEXT NOT NULL,
    match_role TEXT NOT NULL CHECK(match_role IN ('primary','secondary')),
    score INTEGER,
    evidence_json TEXT NOT NULL DEFAULT '{}',
    PRIMARY KEY(evaluation_id, selling_point_code)
);

CREATE TABLE IF NOT EXISTS review_queue (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    content_id INTEGER NOT NULL REFERENCES content_items(id) ON DELETE CASCADE,
    evaluation_id INTEGER REFERENCES evaluation_versions(id) ON DELETE SET NULL,
    reason_code TEXT NOT NULL,
    priority INTEGER NOT NULL DEFAULT 50,
    status TEXT NOT NULL CHECK(status IN ('pending','in_review','resolved','manual_required','terminal_failed')),
    assigned_to TEXT,
    created_at TEXT NOT NULL,
    updated_at TEXT NOT NULL,
    resolved_at TEXT,
    UNIQUE(content_id, reason_code)
);

CREATE TABLE IF NOT EXISTS evaluation_reviews (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    queue_id INTEGER REFERENCES review_queue(id) ON DELETE SET NULL,
    content_id INTEGER NOT NULL REFERENCES content_items(id) ON DELETE CASCADE,
    previous_evaluation_id INTEGER REFERENCES evaluation_versions(id),
    resulting_evaluation_id INTEGER REFERENCES evaluation_versions(id),
    decision TEXT NOT NULL,
    reason TEXT NOT NULL,
    reviewer TEXT NOT NULL,
    created_at TEXT NOT NULL
);

CREATE TABLE IF NOT EXISTS manual_evidence (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    review_id INTEGER NOT NULL REFERENCES evaluation_reviews(id) ON DELETE CASCADE,
    content_id INTEGER NOT NULL REFERENCES content_items(id) ON DELETE CASCADE,
    evidence_type TEXT NOT NULL,
    text_value TEXT,
    local_path TEXT,
    sha256 TEXT NOT NULL,
    created_at TEXT NOT NULL
);

CREATE TABLE IF NOT EXISTS duplicate_relations (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    duplicate_content_id INTEGER NOT NULL REFERENCES content_items(id) ON DELETE CASCADE,
    original_content_id INTEGER NOT NULL REFERENCES content_items(id) ON DELETE CASCADE,
    method TEXT NOT NULL,
    confidence REAL NOT NULL,
    evidence_json TEXT NOT NULL,
    status TEXT NOT NULL CHECK(status IN ('confirmed','pending_review','dismissed')),
    created_at TEXT NOT NULL,
    UNIQUE(duplicate_content_id, original_content_id, method),
    CHECK(duplicate_content_id <> original_content_id)
);

CREATE TABLE IF NOT EXISTS report_tasks (
    id TEXT PRIMARY KEY,
    task_type TEXT NOT NULL CHECK(task_type IN ('daily','weekly','custom')),
    name TEXT NOT NULL,
    period_start TEXT NOT NULL,
    period_end TEXT NOT NULL,
    creation_source TEXT NOT NULL CHECK(creation_source IN ('automatic','manual')),
    task_status TEXT NOT NULL CHECK(task_status IN ('queued','running','succeeded','partial','failed','cancel_requested','cancelled','interrupted')),
    progress INTEGER NOT NULL DEFAULT 0,
    message TEXT NOT NULL DEFAULT '',
    created_at TEXT NOT NULL,
    started_at TEXT,
    completed_at TEXT,
    updated_at TEXT NOT NULL
);

CREATE UNIQUE INDEX IF NOT EXISTS uq_automatic_report_period
ON report_tasks(task_type, period_start, period_end, creation_source)
WHERE creation_source='automatic';

CREATE TABLE IF NOT EXISTS task_events (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    task_id TEXT NOT NULL REFERENCES report_tasks(id) ON DELETE CASCADE,
    event_type TEXT NOT NULL,
    message TEXT NOT NULL DEFAULT '',
    payload_json TEXT NOT NULL DEFAULT '{}',
    created_at TEXT NOT NULL
);

CREATE TABLE IF NOT EXISTS task_contents (
    task_id TEXT NOT NULL REFERENCES report_tasks(id) ON DELETE CASCADE,
    content_id INTEGER NOT NULL REFERENCES content_items(id) ON DELETE CASCADE,
    inclusion_status TEXT NOT NULL CHECK(inclusion_status IN ('included','excluded_missing_boundary','excluded_other')),
    reason TEXT NOT NULL DEFAULT '',
    PRIMARY KEY(task_id, content_id)
);

CREATE TABLE IF NOT EXISTS report_revisions (
    task_id TEXT NOT NULL REFERENCES report_tasks(id) ON DELETE CASCADE,
    revision INTEGER NOT NULL,
    contract_version TEXT NOT NULL,
    rule_version TEXT NOT NULL,
    taxonomy_version TEXT NOT NULL,
    report_json_path TEXT NOT NULL,
    report_sha256 TEXT NOT NULL,
    created_at TEXT NOT NULL,
    PRIMARY KEY(task_id, revision)
);

CREATE TABLE IF NOT EXISTS report_files (
    id TEXT PRIMARY KEY,
    task_id TEXT NOT NULL,
    revision INTEGER NOT NULL,
    file_kind TEXT NOT NULL,
    local_path TEXT NOT NULL,
    sha256 TEXT NOT NULL,
    byte_size INTEGER NOT NULL,
    status TEXT NOT NULL CHECK(status IN ('available','failed')),
    error_message TEXT,
    created_at TEXT NOT NULL,
    FOREIGN KEY(task_id, revision) REFERENCES report_revisions(task_id, revision) ON DELETE CASCADE,
    UNIQUE(task_id, revision, file_kind)
);

CREATE TABLE IF NOT EXISTS scheduler_runs (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    job_id TEXT NOT NULL,
    scheduled_for TEXT NOT NULL,
    status TEXT NOT NULL CHECK(status IN ('running','succeeded','failed','skipped')),
    started_at TEXT NOT NULL,
    completed_at TEXT,
    details_json TEXT NOT NULL DEFAULT '{}',
    UNIQUE(job_id, scheduled_for)
);

CREATE TABLE IF NOT EXISTS migration_audit (
    id TEXT PRIMARY KEY,
    baseline_id TEXT NOT NULL,
    source_database TEXT NOT NULL,
    source_sha256 TEXT NOT NULL,
    status TEXT NOT NULL CHECK(status IN ('running','succeeded','failed')),
    summary_json TEXT NOT NULL DEFAULT '{}',
    started_at TEXT NOT NULL,
    completed_at TEXT
);

CREATE TABLE IF NOT EXISTS migration_row_audit (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    migration_id TEXT NOT NULL REFERENCES migration_audit(id) ON DELETE CASCADE,
    source_table TEXT NOT NULL,
    source_pk TEXT NOT NULL,
    field_name TEXT NOT NULL,
    raw_value TEXT,
    normalized_value TEXT,
    status TEXT NOT NULL CHECK(status IN ('normalized','missing','copied','rejected')),
    reason TEXT NOT NULL DEFAULT '',
    UNIQUE(migration_id, source_table, source_pk, field_name)
);
"""


def initialize_database(connection: sqlite3.Connection) -> None:
    with transaction(connection):
        connection.executescript(SCHEMA_SQL)
        raw_columns = {
            str(row["name"])
            for row in connection.execute("PRAGMA table_info(provider_raw_responses)").fetchall()
        }
        if "account_id" not in raw_columns:
            connection.execute(
                "ALTER TABLE provider_raw_responses ADD COLUMN account_id INTEGER REFERENCES accounts(id) ON DELETE CASCADE"
            )
        connection.execute(
            """
            INSERT INTO schema_migrations(version, name, applied_at)
            VALUES (?, ?, ?)
            ON CONFLICT(version) DO NOTHING
            """,
            (2, "dcar-insight-v8-initial-schema", now_utc()),
        )
        connection.execute(
            """
            INSERT INTO schema_migrations(version, name, applied_at)
            VALUES (?, ?, ?)
            ON CONFLICT(version) DO NOTHING
            """,
            (SCHEMA_VERSION, "account-discovery-provider-references", now_utc()),
        )
