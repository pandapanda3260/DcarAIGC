"""Independent SQLite storage for DCar Insight v8."""

from __future__ import annotations

import hashlib
import json
import os
import re
import sqlite3
from contextlib import contextmanager
from datetime import datetime, timezone
from pathlib import Path
from typing import Iterator


PROJECT_ROOT = Path(__file__).resolve().parents[3]
DEFAULT_DB = PROJECT_ROOT / "app" / "data" / "dcar_insight.sqlite3"
SCHEMA_VERSION = 11
CURRENT_SCHEMA_MIGRATION_NAME = "interaction-user-v1-fallback-keys"
LEGACY_TAXONOMY_VERSION = "selling-points-v5.0"
LEGACY_V6_RELEASE_ID = "evaluation-v6__selling-points-v5.0"
LEGACY_V7_RELEASE_ID = "evaluation-v7__selling-points-v5.0"
LEGACY_COMMENT_USER_KEY_VERSION = "content-user-hmac-v1"
LEGACY_COMMENT_SCORE_RULE_VERSION = "legacy-audience-action-v1"
PLATFORM_USER_KEY_VERSION = "platform-user-hmac-v2"
COMMENT_COLLECTION_VERSION = "paged-comments-v2"
#: 全量历史回溯的内容分组标记（content_items.source_group）。
#: history-archive：证据窗之外的历史内容，仅入库+指标，不参与自动评估与媒体截止闸门；
#: history-backfill：证据窗内、由回溯批量入库的内容，待 local-evidence 阶段完成媒体+评估后
#: 清除标记并回归常规增量链路。两类标记均不影响每日 30 天监控窗内的指标/评论刷新。
HISTORY_ARCHIVE_SOURCE_GROUP = "history-archive"
HISTORY_BACKFILL_SOURCE_GROUP = "history-backfill"
BACKFILL_SOURCE_GROUPS = (
    HISTORY_ARCHIVE_SOURCE_GROUP,
    HISTORY_BACKFILL_SOURCE_GROUP,
)
LEGACY_MATCHER_RULE_SHA256 = (
    "38f647e9b05e38777bbe4727b5c563b67c61e28854d8b37027af8119023eefdc"
)


def now_utc() -> str:
    return (
        datetime.now(timezone.utc).isoformat(timespec="seconds").replace("+00:00", "Z")
    )


def connect(path: Path = DEFAULT_DB) -> sqlite3.Connection:
    if (
        os.environ.get("DCAR_TEST_DENY_FORMAL_DB") == "1"
        and path.resolve() == DEFAULT_DB.resolve()
    ):
        raise RuntimeError(
            "test process attempted to open the formal DCar database"
        )
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

CREATE TABLE IF NOT EXISTS pending_platform_identities (
    platform TEXT NOT NULL CHECK(platform IN ('douyin','xiaohongshu','wechat_channels','kuaishou')),
    uid TEXT NOT NULL,
    nickname TEXT NOT NULL DEFAULT '',
    content_count INTEGER NOT NULL DEFAULT 0,
    first_published_at TEXT,
    last_published_at TEXT,
    created_at TEXT NOT NULL,
    updated_at TEXT NOT NULL,
    PRIMARY KEY(platform, uid)
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
ON fetch_slots(content_id, stage, window_key)
WHERE content_id IS NOT NULL;
CREATE UNIQUE INDEX IF NOT EXISTS uq_fetch_account_slot
ON fetch_slots(account_id, stage, window_key)
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

CREATE TABLE IF NOT EXISTS interaction_users (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    platform TEXT NOT NULL,
    pseudonymous_user_key TEXT NOT NULL,
    -- v11: 'content-user-hmac-v1' is the explicit degraded fallback for
    -- pre-v8.4 comments whose raw platform uid was never stored (privacy by
    -- design), so a platform-level v2 key can never be derived for them.
    -- v1 keys are content-scoped: the same person on two contents counts as
    -- two users. New captures keep writing v2 keys only.
    key_version TEXT NOT NULL DEFAULT 'platform-user-hmac-v2'
        CHECK(key_version IN ('platform-user-hmac-v2','content-user-hmac-v1')),
    first_seen_at TEXT NOT NULL,
    last_seen_at TEXT NOT NULL,
    UNIQUE(platform, key_version, pseudonymous_user_key)
);

CREATE TABLE IF NOT EXISTS interaction_user_classification_versions (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    interaction_user_id INTEGER NOT NULL REFERENCES interaction_users(id) ON DELETE CASCADE,
    audience_definition_version TEXT NOT NULL,
    classifier_version TEXT NOT NULL,
    evidence_window_start TEXT NOT NULL,
    evidence_window_end TEXT NOT NULL,
    evidence_sha256 TEXT NOT NULL CHECK(length(evidence_sha256)=64),
    label TEXT NOT NULL CHECK(label IN ('automotive','not_identified','excluded')),
    confidence REAL CHECK(confidence IS NULL OR (confidence >= 0 AND confidence <= 1)),
    reason_json TEXT NOT NULL DEFAULT '{}',
    created_at TEXT NOT NULL,
    UNIQUE(interaction_user_id, audience_definition_version, classifier_version, evidence_sha256)
);

CREATE TABLE IF NOT EXISTS comment_capture_runs (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    content_id INTEGER NOT NULL REFERENCES content_items(id) ON DELETE CASCADE,
    window_key TEXT NOT NULL,
    collection_version TEXT NOT NULL DEFAULT 'paged-comments-v2',
    provider TEXT NOT NULL,
    adapter_version TEXT NOT NULL,
    status TEXT NOT NULL CHECK(status IN ('pending','running','succeeded','retryable_failed','terminal_failed')),
    completion_kind TEXT CHECK(completion_kind IS NULL OR completion_kind IN ('provider_exhausted','coverage_target_reached','cap_reached','zero_comments')),
    stop_reason TEXT,
    declared_total_count INTEGER,
    captured_distinct_count INTEGER NOT NULL DEFAULT 0,
    valid_comment_count INTEGER NOT NULL DEFAULT 0,
    stable_identity_comment_count INTEGER NOT NULL DEFAULT 0,
    page_count INTEGER NOT NULL DEFAULT 0,
    comment_cap INTEGER NOT NULL DEFAULT 1000,
    started_at TEXT,
    completed_at TEXT,
    created_at TEXT NOT NULL,
    updated_at TEXT NOT NULL,
    UNIQUE(content_id, window_key)
);

CREATE INDEX IF NOT EXISTS idx_comment_capture_runs_window
ON comment_capture_runs(window_key, status);

CREATE TABLE IF NOT EXISTS comment_capture_pages (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    capture_run_id INTEGER NOT NULL REFERENCES comment_capture_runs(id) ON DELETE CASCADE,
    page_number INTEGER NOT NULL CHECK(page_number >= 1),
    request_cursor_json TEXT NOT NULL,
    request_cursor_sha256 TEXT NOT NULL CHECK(length(request_cursor_sha256)=64),
    next_cursor_json TEXT,
    next_cursor_sha256 TEXT CHECK(next_cursor_sha256 IS NULL OR length(next_cursor_sha256)=64),
    fetch_slot_id INTEGER NOT NULL REFERENCES fetch_slots(id) ON DELETE RESTRICT,
    raw_response_id INTEGER NOT NULL REFERENCES provider_raw_responses(id) ON DELETE RESTRICT,
    has_more INTEGER NOT NULL CHECK(has_more IN (0,1)),
    provider_declared_total INTEGER,
    received_count INTEGER NOT NULL DEFAULT 0,
    captured_distinct_count INTEGER NOT NULL DEFAULT 0,
    valid_count INTEGER NOT NULL DEFAULT 0,
    stable_identity_count INTEGER NOT NULL DEFAULT 0,
    captured_at TEXT NOT NULL,
    UNIQUE(capture_run_id, page_number),
    UNIQUE(capture_run_id, request_cursor_sha256),
    UNIQUE(fetch_slot_id),
    UNIQUE(raw_response_id)
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
    capture_run_id INTEGER REFERENCES comment_capture_runs(id),
    status TEXT NOT NULL CHECK(status IN ('available','partial','missing','failed')),
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
    capture_page_id INTEGER REFERENCES comment_capture_pages(id),
    interaction_user_id INTEGER REFERENCES interaction_users(id),
    comment_identity_key TEXT,
    raw_json TEXT NOT NULL DEFAULT '{}',
    UNIQUE(evidence_version_id, platform_comment_id)
);

CREATE UNIQUE INDEX IF NOT EXISTS uq_comments_identity_per_evidence
ON comments(evidence_version_id, comment_identity_key)
WHERE comment_identity_key IS NOT NULL;

CREATE INDEX IF NOT EXISTS idx_comments_interaction_user
ON comments(interaction_user_id);

CREATE TABLE IF NOT EXISTS comment_user_scores (
    content_id INTEGER NOT NULL REFERENCES content_items(id) ON DELETE CASCADE,
    evidence_version_id INTEGER REFERENCES comment_evidence_versions(id) ON DELETE SET NULL,
    anonymous_user_key TEXT NOT NULL,
    audience_automotive_score INTEGER NOT NULL,
    action_intent_score INTEGER NOT NULL,
    key_version TEXT NOT NULL DEFAULT 'content-user-hmac-v1'
        CHECK(key_version IN ('content-user-hmac-v1')),
    score_rule_version TEXT NOT NULL DEFAULT 'legacy-audience-action-v1'
        CHECK(score_rule_version IN ('legacy-audience-action-v1')),
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
    matcher_rule_json TEXT NOT NULL DEFAULT '{}',
    enabled INTEGER NOT NULL DEFAULT 1 CHECK(enabled IN (0,1)),
    UNIQUE(taxonomy_id, code)
);

CREATE TABLE IF NOT EXISTS selling_point_scenes (
    selling_point_id INTEGER NOT NULL REFERENCES selling_points(id) ON DELETE CASCADE,
    scene TEXT NOT NULL CHECK(scene IN ('new_car','used_car','media')),
    PRIMARY KEY(selling_point_id, scene)
);

CREATE TABLE IF NOT EXISTS evaluation_releases (
    id TEXT PRIMARY KEY,
    rule_version TEXT NOT NULL,
    taxonomy_version TEXT NOT NULL
        REFERENCES taxonomy_versions(version) ON DELETE RESTRICT,
    matcher_rule_sha256 TEXT NOT NULL CHECK(length(matcher_rule_sha256)=64),
    status TEXT NOT NULL
        CHECK(status IN ('draft','backfilling','ready','active','retired','failed')),
    created_at TEXT NOT NULL,
    updated_at TEXT NOT NULL,
    activated_at TEXT,
    retired_at TEXT,
    failure_reason TEXT,
    UNIQUE(rule_version, taxonomy_version),
    UNIQUE(id, rule_version, taxonomy_version),
    UNIQUE(id, rule_version, taxonomy_version, matcher_rule_sha256)
);

CREATE UNIQUE INDEX IF NOT EXISTS uq_evaluation_releases_one_active
ON evaluation_releases(status) WHERE status='active';
CREATE INDEX IF NOT EXISTS idx_evaluation_releases_status
ON evaluation_releases(status, created_at, id);

CREATE TABLE IF NOT EXISTS evaluation_versions (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    content_id INTEGER NOT NULL REFERENCES content_items(id) ON DELETE RESTRICT,
    evidence_envelope_id INTEGER REFERENCES evidence_envelopes(id) ON DELETE RESTRICT,
    release_id TEXT NOT NULL,
    parent_evaluation_id INTEGER REFERENCES evaluation_versions(id)
        ON DELETE RESTRICT DEFERRABLE INITIALLY DEFERRED,
    review_id INTEGER REFERENCES evaluation_reviews(id)
        ON DELETE RESTRICT DEFERRABLE INITIALLY DEFERRED,
    rule_version TEXT NOT NULL,
    taxonomy_version TEXT NOT NULL,
    matcher_rule_sha256 TEXT NOT NULL CHECK(length(matcher_rule_sha256)=64),
    evidence_sha256 TEXT NOT NULL,
    evaluation_source TEXT NOT NULL CHECK(evaluation_source IN ('automatic','manual_review','migrated_from_v5')),
    evaluation_status TEXT NOT NULL
        CHECK(evaluation_status IN ('evaluated','insufficient_evidence')),
    evidence_level TEXT NOT NULL CHECK(evidence_level IN ('V0','V1','V2','V3')),
    primary_selling_point_code TEXT,
    selling_point_score INTEGER CHECK(selling_point_score IS NULL OR selling_point_score BETWEEN 0 AND 100),
    selling_point_included INTEGER NOT NULL DEFAULT 0 CHECK(selling_point_included IN (0,1)),
    content_direction TEXT NOT NULL DEFAULT 'unknown'
        CHECK(content_direction IN ('new_car','used_car','media','other','unknown')),
    content_automotive_score INTEGER
        CHECK(content_automotive_score IS NULL OR content_automotive_score BETWEEN 0 AND 100),
    audience_automotive_score INTEGER
        CHECK(audience_automotive_score IS NULL OR audience_automotive_score BETWEEN 0 AND 100),
    acquisition_potential_score INTEGER
        CHECK(acquisition_potential_score IS NULL OR acquisition_potential_score BETWEEN 0 AND 100),
    pending_review INTEGER NOT NULL DEFAULT 0 CHECK(pending_review IN (0,1)),
    payload_json TEXT NOT NULL,
    evaluated_at TEXT NOT NULL,
    invalidated_at TEXT,
    invalidation_reason TEXT,
    FOREIGN KEY(release_id, rule_version, taxonomy_version, matcher_rule_sha256)
        REFERENCES evaluation_releases(
            id, rule_version, taxonomy_version, matcher_rule_sha256
        ) ON DELETE RESTRICT,
    CHECK(
        (evaluation_source='manual_review' AND review_id IS NOT NULL)
        OR (evaluation_source<>'manual_review' AND review_id IS NULL)
    ),
    CHECK(parent_evaluation_id IS NULL OR parent_evaluation_id<>id),
    CHECK(
        (invalidated_at IS NULL AND invalidation_reason IS NULL)
        OR (invalidated_at IS NOT NULL AND invalidation_reason IS NOT NULL)
    )
);

CREATE UNIQUE INDEX IF NOT EXISTS uq_evaluation_automatic_idempotency
ON evaluation_versions(content_id, release_id, evidence_sha256)
WHERE evaluation_source='automatic';
CREATE UNIQUE INDEX IF NOT EXISTS uq_evaluation_manual_idempotency
ON evaluation_versions(release_id, review_id)
WHERE evaluation_source='manual_review' AND review_id IS NOT NULL;
CREATE UNIQUE INDEX IF NOT EXISTS uq_evaluation_migrated_parent_idempotency
ON evaluation_versions(release_id, parent_evaluation_id)
WHERE evaluation_source='migrated_from_v5' AND parent_evaluation_id IS NOT NULL;
CREATE INDEX IF NOT EXISTS idx_evaluation_content_audit
ON evaluation_versions(content_id, evaluated_at DESC, id DESC);
CREATE INDEX IF NOT EXISTS idx_evaluation_release_current
ON evaluation_versions(release_id, content_id, evaluated_at DESC, id DESC)
WHERE invalidated_at IS NULL;
CREATE INDEX IF NOT EXISTS idx_evaluation_parent
ON evaluation_versions(parent_evaluation_id)
WHERE parent_evaluation_id IS NOT NULL;
CREATE INDEX IF NOT EXISTS idx_evaluation_review
ON evaluation_versions(review_id) WHERE review_id IS NOT NULL;

CREATE TABLE IF NOT EXISTS evaluation_matches (
    evaluation_id INTEGER NOT NULL REFERENCES evaluation_versions(id) ON DELETE RESTRICT,
    selling_point_code TEXT NOT NULL,
    scene TEXT NOT NULL CHECK(scene IN ('new_car','used_car','media')),
    match_role TEXT NOT NULL CHECK(match_role IN ('primary','secondary')),
    score INTEGER CHECK(score IS NULL OR score BETWEEN 0 AND 100),
    evidence_json TEXT NOT NULL DEFAULT '{}',
    PRIMARY KEY(evaluation_id, selling_point_code)
);

CREATE UNIQUE INDEX IF NOT EXISTS uq_evaluation_primary_match
ON evaluation_matches(evaluation_id) WHERE match_role='primary';
CREATE INDEX IF NOT EXISTS idx_evaluation_matches_scene_code
ON evaluation_matches(scene, selling_point_code, evaluation_id);

CREATE TABLE IF NOT EXISTS review_queue (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    content_id INTEGER NOT NULL REFERENCES content_items(id) ON DELETE RESTRICT,
    evaluation_id INTEGER REFERENCES evaluation_versions(id) ON DELETE RESTRICT,
    reason_code TEXT NOT NULL,
    priority INTEGER NOT NULL DEFAULT 50,
    status TEXT NOT NULL CHECK(status IN ('pending','in_review','resolved','manual_required','terminal_failed')),
    assigned_to TEXT,
    created_at TEXT NOT NULL,
    updated_at TEXT NOT NULL,
    resolved_at TEXT,
    UNIQUE(content_id, reason_code)
);

CREATE INDEX IF NOT EXISTS idx_review_queue_status_priority
ON review_queue(status, priority DESC, updated_at, id);
CREATE INDEX IF NOT EXISTS idx_review_queue_evaluation
ON review_queue(evaluation_id) WHERE evaluation_id IS NOT NULL;

CREATE TABLE IF NOT EXISTS evaluation_reviews (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    queue_id INTEGER REFERENCES review_queue(id) ON DELETE RESTRICT,
    content_id INTEGER NOT NULL REFERENCES content_items(id) ON DELETE RESTRICT,
    previous_evaluation_id INTEGER REFERENCES evaluation_versions(id) ON DELETE RESTRICT,
    resulting_evaluation_id INTEGER REFERENCES evaluation_versions(id) ON DELETE RESTRICT,
    decision TEXT NOT NULL,
    reason TEXT NOT NULL,
    reviewer TEXT NOT NULL,
    created_at TEXT NOT NULL
);

CREATE INDEX IF NOT EXISTS idx_evaluation_reviews_queue
ON evaluation_reviews(queue_id, created_at, id);
CREATE UNIQUE INDEX IF NOT EXISTS uq_evaluation_review_result
ON evaluation_reviews(resulting_evaluation_id)
WHERE resulting_evaluation_id IS NOT NULL;

CREATE TABLE IF NOT EXISTS review_reopen_events (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    queue_id INTEGER NOT NULL REFERENCES review_queue(id) ON DELETE RESTRICT,
    content_id INTEGER NOT NULL REFERENCES content_items(id) ON DELETE RESTRICT,
    previous_review_id INTEGER REFERENCES evaluation_reviews(id) ON DELETE RESTRICT,
    base_evaluation_id INTEGER REFERENCES evaluation_versions(id) ON DELETE RESTRICT,
    reopened_by TEXT NOT NULL,
    reason TEXT NOT NULL,
    created_at TEXT NOT NULL
);

CREATE INDEX IF NOT EXISTS idx_review_reopen_queue
ON review_reopen_events(queue_id, created_at, id);

CREATE TABLE IF NOT EXISTS manual_evidence (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    review_id INTEGER NOT NULL REFERENCES evaluation_reviews(id) ON DELETE RESTRICT,
    content_id INTEGER NOT NULL REFERENCES content_items(id) ON DELETE RESTRICT,
    evidence_type TEXT NOT NULL,
    text_value TEXT,
    local_path TEXT,
    sha256 TEXT NOT NULL,
    created_at TEXT NOT NULL
);

CREATE INDEX IF NOT EXISTS idx_manual_evidence_review
ON manual_evidence(review_id, id);

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

CREATE TABLE IF NOT EXISTS duplicate_fingerprints (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    content_id INTEGER NOT NULL REFERENCES content_items(id) ON DELETE CASCADE,
    fingerprint_version TEXT NOT NULL,
    source_sha256 TEXT NOT NULL,
    text_sha256 TEXT,
    media_sha256_json TEXT NOT NULL DEFAULT '[]',
    frame_phashes_json TEXT NOT NULL DEFAULT '[]',
    text_simhash TEXT,
    asr_simhash TEXT,
    ocr_simhash TEXT,
    text_char_count INTEGER NOT NULL DEFAULT 0,
    asr_char_count INTEGER NOT NULL DEFAULT 0,
    ocr_char_count INTEGER NOT NULL DEFAULT 0,
    artifact_id INTEGER REFERENCES evidence_artifacts(id) ON DELETE SET NULL,
    payload_json TEXT NOT NULL,
    created_at TEXT NOT NULL,
    UNIQUE(content_id, fingerprint_version, source_sha256)
);

CREATE INDEX IF NOT EXISTS idx_duplicate_fingerprint_current
ON duplicate_fingerprints(content_id, fingerprint_version, created_at DESC);

CREATE TABLE IF NOT EXISTS duplicate_calibration_runs (
    id TEXT PRIMARY KEY,
    calibration_version TEXT NOT NULL,
    fingerprint_version TEXT NOT NULL,
    dataset_sha256 TEXT NOT NULL,
    pair_count INTEGER NOT NULL,
    positive_count INTEGER NOT NULL,
    negative_count INTEGER NOT NULL,
    predicted_positive_count INTEGER NOT NULL,
    true_positive_count INTEGER NOT NULL,
    false_positive_count INTEGER NOT NULL,
    precision REAL NOT NULL,
    recall REAL NOT NULL,
    thresholds_json TEXT NOT NULL,
    status TEXT NOT NULL CHECK(status IN ('passed','failed')),
    created_at TEXT NOT NULL,
    UNIQUE(calibration_version, fingerprint_version, dataset_sha256)
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
    task_id TEXT NOT NULL REFERENCES report_tasks(id) ON DELETE RESTRICT,
    revision INTEGER NOT NULL,
    release_id TEXT NOT NULL,
    contract_version TEXT NOT NULL,
    rule_version TEXT NOT NULL,
    taxonomy_version TEXT NOT NULL,
    report_json_path TEXT NOT NULL,
    report_sha256 TEXT NOT NULL,
    created_at TEXT NOT NULL,
    invalidated_at TEXT,
    invalidation_reason TEXT,
    PRIMARY KEY(task_id, revision),
    FOREIGN KEY(release_id, rule_version, taxonomy_version)
        REFERENCES evaluation_releases(id, rule_version, taxonomy_version)
        ON DELETE RESTRICT,
    CHECK(
        (invalidated_at IS NULL AND invalidation_reason IS NULL)
        OR (invalidated_at IS NOT NULL AND invalidation_reason IS NOT NULL)
    )
);

CREATE INDEX IF NOT EXISTS idx_report_revision_audit
ON report_revisions(task_id, revision DESC);
CREATE INDEX IF NOT EXISTS idx_report_revision_current
ON report_revisions(task_id, release_id, revision DESC)
WHERE invalidated_at IS NULL;

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
    FOREIGN KEY(task_id, revision) REFERENCES report_revisions(task_id, revision) ON DELETE RESTRICT,
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


class SchemaMigrationError(RuntimeError):
    pass


_REBUILT_V9_TABLES = (
    "evaluation_versions",
    "evaluation_matches",
    "review_queue",
    "evaluation_reviews",
    "review_reopen_events",
    "manual_evidence",
    "report_revisions",
    "report_files",
)


def _schema_statements() -> list[str]:
    statements: list[str] = []
    buffer = ""
    for line in SCHEMA_SQL.splitlines(keepends=True):
        buffer += line
        if sqlite3.complete_statement(buffer):
            statement = buffer.strip().removesuffix(";").strip()
            if statement:
                statements.append(statement)
            buffer = ""
    if buffer.strip():
        raise SchemaMigrationError("SCHEMA_SQL ends with an incomplete statement")
    return statements


def _table_names(connection: sqlite3.Connection) -> set[str]:
    return {
        str(row[0])
        for row in connection.execute(
            """
            SELECT name FROM sqlite_master
            WHERE type='table' AND name NOT LIKE 'sqlite_%'
            """
        )
    }


def _table_columns(connection: sqlite3.Connection, table: str) -> list[str]:
    return [
        str(row["name"])
        for row in connection.execute(f'PRAGMA table_info("{table}")').fetchall()
    ]


def _table_projection_sha256(
    connection: sqlite3.Connection,
    table: str,
    columns: list[str],
) -> str:
    quoted = ",".join('"' + column.replace('"', '""') + '"' for column in columns)
    primary = [
        (int(row["pk"]), str(row["name"]))
        for row in connection.execute(f'PRAGMA table_info("{table}")').fetchall()
        if int(row["pk"])
    ]
    order_columns = [column for _, column in sorted(primary)] or columns
    order = ",".join('"' + column.replace('"', '""') + '"' for column in order_columns)
    digest = hashlib.sha256()
    for row in connection.execute(f'SELECT {quoted} FROM "{table}" ORDER BY {order}'):
        digest.update(
            json.dumps(
                list(row),
                ensure_ascii=False,
                separators=(",", ":"),
                default=lambda value: {"bytes_hex": bytes(value).hex()},
            ).encode("utf-8")
        )
        digest.update(b"\n")
    return digest.hexdigest()


def _schema_table_statement(table: str, replacements: dict[str, str]) -> str:
    prefix = f"CREATE TABLE IF NOT EXISTS {table} "
    try:
        statement = next(
            value for value in _schema_statements() if value.startswith(prefix)
        )
    except StopIteration as exc:
        raise SchemaMigrationError(f"missing schema template for {table}") from exc
    statement = statement.replace("CREATE TABLE IF NOT EXISTS", "CREATE TABLE", 1)
    for source in sorted(replacements, key=len, reverse=True):
        statement = re.sub(rf"\b{re.escape(source)}\b", replacements[source], statement)
    return statement


def _legacy_release_id(rule_version: str, taxonomy_version: str) -> str:
    pair = (rule_version, taxonomy_version)
    if pair == ("evaluation-v6", LEGACY_TAXONOMY_VERSION):
        return LEGACY_V6_RELEASE_ID
    if pair == ("evaluation-v7", LEGACY_TAXONOMY_VERSION):
        return LEGACY_V7_RELEASE_ID
    raise SchemaMigrationError(
        f"unsupported legacy evaluation release: {rule_version}/{taxonomy_version}"
    )


def ensure_legacy_evaluation_release(
    connection: sqlite3.Connection,
    *,
    rule_version: str,
    taxonomy_version: str,
) -> sqlite3.Row:
    release_id = _legacy_release_id(rule_version, taxonomy_version)
    taxonomy = connection.execute(
        "SELECT 1 FROM taxonomy_versions WHERE version=?", (taxonomy_version,)
    ).fetchone()
    if taxonomy is None:
        raise SchemaMigrationError(f"taxonomy does not exist: {taxonomy_version}")
    captured_at = now_utc()
    status = "active" if rule_version == "evaluation-v7" else "retired"
    connection.execute(
        """
        INSERT INTO evaluation_releases(
            id,rule_version,taxonomy_version,matcher_rule_sha256,status,
            created_at,updated_at,activated_at,retired_at
        ) VALUES (?,?,?,?,?,?,?,?,?)
        ON CONFLICT(id) DO NOTHING
        """,
        (
            release_id,
            rule_version,
            taxonomy_version,
            LEGACY_MATCHER_RULE_SHA256,
            status,
            captured_at,
            captured_at,
            captured_at,
            captured_at if status == "retired" else None,
        ),
    )
    row = connection.execute(
        "SELECT * FROM evaluation_releases WHERE id=?", (release_id,)
    ).fetchone()
    if row is None:
        raise SchemaMigrationError(f"failed to register legacy release {release_id}")
    return row


def active_evaluation_release(connection: sqlite3.Connection) -> sqlite3.Row:
    row = connection.execute(
        "SELECT * FROM evaluation_releases WHERE status='active'"
    ).fetchone()
    if row is None:
        raise SchemaMigrationError("no active evaluation release")
    return row


def _migration_checkpoint(_name: str) -> None:
    """Patchable failure-injection point used by atomic migration tests."""


def _preflight_v8(
    connection: sqlite3.Connection,
) -> tuple[dict[int, int], dict[int, int]]:
    tables = _table_names(connection)
    missing = set(_REBUILT_V9_TABLES) - tables
    if missing:
        raise SchemaMigrationError(
            f"v8 schema is missing required tables: {sorted(missing)}"
        )
    residual = sorted(table for table in tables if table.endswith("_v9_new"))
    if residual:
        raise SchemaMigrationError(f"partial v9 tables already exist: {residual}")
    violations = connection.execute("PRAGMA foreign_key_check").fetchall()
    if violations:
        raise SchemaMigrationError(f"v8 foreign-key violations: {len(violations)}")

    combinations = {
        (str(row[0]), str(row[1]))
        for row in connection.execute(
            """
            SELECT rule_version,taxonomy_version FROM evaluation_versions
            UNION
            SELECT rule_version,taxonomy_version FROM report_revisions
            """
        )
    }
    allowed = {
        ("evaluation-v6", LEGACY_TAXONOMY_VERSION),
        ("evaluation-v7", LEGACY_TAXONOMY_VERSION),
    }
    if combinations - allowed:
        raise SchemaMigrationError(
            f"unknown legacy release combinations: {sorted(combinations - allowed)}"
        )
    taxonomy = connection.execute(
        "SELECT 1 FROM taxonomy_versions WHERE version=?", (LEGACY_TAXONOMY_VERSION,)
    ).fetchone()
    if taxonomy is None:
        raise SchemaMigrationError(f"missing legacy taxonomy {LEGACY_TAXONOMY_VERSION}")

    parent_by_evaluation: dict[int, int] = {}
    rows = connection.execute(
        """
        SELECT id,content_id,rule_version,taxonomy_version,evidence_sha256,
               evaluation_source,payload_json
        FROM evaluation_versions ORDER BY id
        """
    ).fetchall()
    by_id = {int(row["id"]): row for row in rows}
    for row in rows:
        try:
            payload = json.loads(str(row["payload_json"] or "{}"))
        except json.JSONDecodeError as exc:
            raise SchemaMigrationError(
                f"evaluation {row['id']} has invalid payload JSON"
            ) from exc
        if (
            not isinstance(payload, dict)
            or payload.get("upgraded_from_rule_version") != "evaluation-v6"
        ):
            continue
        if str(row["rule_version"]) != "evaluation-v7":
            raise SchemaMigrationError(
                f"evaluation {row['id']} has invalid upgrade lineage marker"
            )
        parents = connection.execute(
            """
            SELECT id FROM evaluation_versions
            WHERE content_id=? AND rule_version='evaluation-v6'
              AND taxonomy_version=? AND evidence_sha256=?
              AND evaluation_source=?
            ORDER BY id
            """,
            (
                row["content_id"],
                row["taxonomy_version"],
                row["evidence_sha256"],
                row["evaluation_source"],
            ),
        ).fetchall()
        if len(parents) != 1:
            raise SchemaMigrationError(
                f"evaluation {row['id']} has {len(parents)} candidate parents"
            )
        parent_by_evaluation[int(row["id"])] = int(parents[0]["id"])

    direct_reviews = {
        int(row["resulting_evaluation_id"]): int(row["id"])
        for row in connection.execute(
            """
            SELECT id,resulting_evaluation_id FROM evaluation_reviews
            WHERE resulting_evaluation_id IS NOT NULL
            """
        )
    }
    review_by_evaluation: dict[int, int] = {}
    for evaluation_id, row in by_id.items():
        if str(row["evaluation_source"]) != "manual_review":
            continue
        review_id = direct_reviews.get(evaluation_id)
        if review_id is None and evaluation_id in parent_by_evaluation:
            review_id = direct_reviews.get(parent_by_evaluation[evaluation_id])
        if review_id is None:
            raise SchemaMigrationError(
                f"manual evaluation {evaluation_id} has no review lineage"
            )
        review_by_evaluation[evaluation_id] = review_id

    invalid_match_count = int(
        connection.execute(
            """
            SELECT COUNT(*) FROM evaluation_matches em
            JOIN evaluation_versions ev ON ev.id=em.evaluation_id
            WHERE ev.content_direction NOT IN ('new_car','used_car','media')
            """
        ).fetchone()[0]
    )
    if invalid_match_count:
        raise SchemaMigrationError(
            f"{invalid_match_count} evaluation matches have no E/X/M scene"
        )
    return parent_by_evaluation, review_by_evaluation


def _restore_sequences(
    connection: sqlite3.Connection,
    sequences: dict[str, int],
    *,
    tables: tuple[str, ...] = _REBUILT_V9_TABLES,
) -> None:
    for table in tables:
        if table not in sequences:
            continue
        connection.execute(
            "UPDATE sqlite_sequence SET seq=? WHERE name=?",
            (sequences[table], table),
        )


def _migrate_v8_to_v9(connection: sqlite3.Connection) -> None:
    if connection.in_transaction:
        raise SchemaMigrationError("v9 migration requires no active transaction")
    foreign_keys = int(connection.execute("PRAGMA foreign_keys").fetchone()[0])
    if foreign_keys != 1:
        raise SchemaMigrationError("v9 migration requires PRAGMA foreign_keys=ON")

    parent_by_evaluation, review_by_evaluation = _preflight_v8(connection)
    old_columns = {
        table: _table_columns(connection, table) for table in _REBUILT_V9_TABLES
    }
    old_counts = {
        table: int(connection.execute(f'SELECT COUNT(*) FROM "{table}"').fetchone()[0])
        for table in _REBUILT_V9_TABLES
    }
    old_hashes = {
        table: _table_projection_sha256(connection, table, columns)
        for table, columns in old_columns.items()
    }
    sequences = {
        str(row["name"]): int(row["seq"])
        for row in connection.execute("SELECT name,seq FROM sqlite_sequence")
    }
    replacements = {table: f"{table}_v9_new" for table in _REBUILT_V9_TABLES}
    captured_at = now_utc()

    connection.execute("PRAGMA foreign_keys=OFF")
    try:
        connection.execute("BEGIN IMMEDIATE")
        connection.execute(_schema_table_statement("evaluation_releases", {}))
        connection.execute(
            "ALTER TABLE selling_points ADD COLUMN matcher_rule_json TEXT NOT NULL DEFAULT '{}'"
        )
        for rule_version in ("evaluation-v6", "evaluation-v7"):
            release_id = _legacy_release_id(rule_version, LEGACY_TAXONOMY_VERSION)
            first_evaluation = connection.execute(
                "SELECT MIN(evaluated_at) FROM evaluation_versions WHERE rule_version=?",
                (rule_version,),
            ).fetchone()[0]
            release_time = str(first_evaluation or captured_at)
            status = "active" if rule_version == "evaluation-v7" else "retired"
            connection.execute(
                """
                INSERT INTO evaluation_releases(
                    id,rule_version,taxonomy_version,matcher_rule_sha256,status,
                    created_at,updated_at,activated_at,retired_at
                ) VALUES (?,?,?,?,?,?,?,?,?)
                """,
                (
                    release_id,
                    rule_version,
                    LEGACY_TAXONOMY_VERSION,
                    LEGACY_MATCHER_RULE_SHA256,
                    status,
                    release_time,
                    captured_at,
                    release_time,
                    captured_at if status == "retired" else None,
                ),
            )
        _migration_checkpoint("releases_registered")

        for table in _REBUILT_V9_TABLES:
            connection.execute(_schema_table_statement(table, replacements))

        evaluation_rows = connection.execute(
            "SELECT * FROM evaluation_versions ORDER BY id"
        ).fetchall()
        connection.executemany(
            """
            INSERT INTO evaluation_versions_v9_new(
                id,content_id,evidence_envelope_id,release_id,parent_evaluation_id,
                review_id,rule_version,taxonomy_version,matcher_rule_sha256,
                evidence_sha256,evaluation_source,evaluation_status,evidence_level,
                primary_selling_point_code,selling_point_score,selling_point_included,
                content_direction,content_automotive_score,audience_automotive_score,
                acquisition_potential_score,pending_review,payload_json,evaluated_at,
                invalidated_at,invalidation_reason
            ) VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)
            """,
            (
                (
                    row["id"],
                    row["content_id"],
                    row["evidence_envelope_id"],
                    _legacy_release_id(
                        str(row["rule_version"]), str(row["taxonomy_version"])
                    ),
                    parent_by_evaluation.get(int(row["id"])),
                    review_by_evaluation.get(int(row["id"])),
                    row["rule_version"],
                    row["taxonomy_version"],
                    LEGACY_MATCHER_RULE_SHA256,
                    row["evidence_sha256"],
                    row["evaluation_source"],
                    row["evaluation_status"],
                    row["evidence_level"],
                    row["primary_selling_point_code"],
                    row["selling_point_score"],
                    row["selling_point_included"],
                    row["content_direction"],
                    row["content_automotive_score"],
                    row["audience_automotive_score"],
                    row["acquisition_potential_score"],
                    row["pending_review"],
                    row["payload_json"],
                    row["evaluated_at"],
                    row["invalidated_at"],
                    row["invalidation_reason"],
                )
                for row in evaluation_rows
            ),
        )
        connection.execute(
            """
            INSERT INTO evaluation_matches_v9_new(
                evaluation_id,selling_point_code,scene,match_role,score,evidence_json
            )
            SELECT em.evaluation_id,em.selling_point_code,ev.content_direction,
                   em.match_role,em.score,em.evidence_json
            FROM evaluation_matches em
            JOIN evaluation_versions ev ON ev.id=em.evaluation_id
            """
        )
        for table in (
            "review_queue",
            "evaluation_reviews",
            "review_reopen_events",
            "manual_evidence",
            "report_files",
        ):
            columns = old_columns[table]
            names = ",".join(f'"{column}"' for column in columns)
            connection.execute(
                f'INSERT INTO "{table}_v9_new"({names}) SELECT {names} FROM "{table}"'
            )
        connection.execute(
            """
            INSERT INTO report_revisions_v9_new(
                task_id,revision,release_id,contract_version,rule_version,
                taxonomy_version,report_json_path,report_sha256,created_at,
                invalidated_at,invalidation_reason
            )
            SELECT task_id,revision,
                   CASE rule_version
                       WHEN 'evaluation-v6' THEN ?
                       WHEN 'evaluation-v7' THEN ?
                   END,
                   contract_version,rule_version,taxonomy_version,
                   report_json_path,report_sha256,created_at,
                   invalidated_at,invalidation_reason
            FROM report_revisions
            """,
            (LEGACY_V6_RELEASE_ID, LEGACY_V7_RELEASE_ID),
        )
        _migration_checkpoint("rows_copied")

        for table in (
            "manual_evidence",
            "review_reopen_events",
            "evaluation_reviews",
            "review_queue",
            "evaluation_matches",
            "evaluation_versions",
            "report_files",
            "report_revisions",
        ):
            connection.execute(f'DROP TABLE "{table}"')
        _migration_checkpoint("old_tables_dropped")
        for table in _REBUILT_V9_TABLES:
            connection.execute(f'ALTER TABLE "{table}_v9_new" RENAME TO "{table}"')
        _migration_checkpoint("new_tables_renamed")

        for statement in _schema_statements():
            is_index = statement.startswith(
                "CREATE INDEX IF NOT EXISTS"
            ) or statement.startswith("CREATE UNIQUE INDEX IF NOT EXISTS")
            target = re.search(r"\bON\s+([a-z_]+)\s*\(", statement, re.IGNORECASE)
            if (
                is_index
                and target is not None
                and target.group(1) in {*_REBUILT_V9_TABLES, "evaluation_releases"}
            ):
                connection.execute(statement)
        _restore_sequences(connection, sequences)
        connection.execute(
            """
            INSERT INTO schema_migrations(version,name,applied_at)
            VALUES (9,'release-bound-evaluation-schema',?)
            """,
            (captured_at,),
        )
        connection.execute("PRAGMA user_version=9")
        _migration_checkpoint("indexes_created")

        for table in _REBUILT_V9_TABLES:
            current_count = int(
                connection.execute(f'SELECT COUNT(*) FROM "{table}"').fetchone()[0]
            )
            if current_count != old_counts[table]:
                raise SchemaMigrationError(
                    f"row count changed for {table}: {old_counts[table]}->{current_count}"
                )
            current_hash = _table_projection_sha256(
                connection, table, old_columns[table]
            )
            if current_hash != old_hashes[table]:
                raise SchemaMigrationError(f"legacy projection changed for {table}")
        violations = connection.execute("PRAGMA foreign_key_check").fetchall()
        if violations:
            raise SchemaMigrationError(
                f"v9 foreign-key violations before commit: {len(violations)}"
            )
        connection.commit()
    except Exception:
        connection.rollback()
        raise
    finally:
        connection.execute("PRAGMA foreign_keys=ON")

    if connection.execute("PRAGMA foreign_key_check").fetchall():
        raise SchemaMigrationError("v9 foreign-key check failed after commit")
    integrity = str(connection.execute("PRAGMA integrity_check").fetchone()[0])
    if integrity != "ok":
        raise SchemaMigrationError(f"v9 integrity check failed: {integrity}")


def _create_fresh_schema(connection: sqlite3.Connection) -> None:
    if connection.in_transaction:
        raise SchemaMigrationError(
            "fresh schema creation requires no active transaction"
        )
    try:
        connection.execute("BEGIN IMMEDIATE")
        for statement in _schema_statements():
            connection.execute(statement)
        captured_at = now_utc()
        connection.execute(
            """
            INSERT INTO schema_migrations(version,name,applied_at)
            VALUES (9,'release-bound-evaluation-schema',?)
            """,
            (captured_at,),
        )
        connection.execute(
            """
            INSERT INTO schema_migrations(version,name,applied_at)
            VALUES (10,'audience-interaction-user-domain',?)
            """,
            (captured_at,),
        )
        connection.execute(
            """
            INSERT INTO schema_migrations(version,name,applied_at)
            VALUES (11,'interaction-user-v1-fallback-keys',?)
            """,
            (captured_at,),
        )
        connection.execute(f"PRAGMA user_version={SCHEMA_VERSION}")
        connection.commit()
    except Exception:
        connection.rollback()
        raise


def _validate_v9(connection: sqlite3.Connection) -> None:
    columns = set(_table_columns(connection, "evaluation_versions"))
    required = {
        "release_id",
        "parent_evaluation_id",
        "review_id",
        "matcher_rule_sha256",
    }
    if required - columns:
        raise SchemaMigrationError(
            f"schema v9 is missing evaluation columns: {sorted(required - columns)}"
        )
    indexes = {
        str(row["name"])
        for row in connection.execute("PRAGMA index_list(evaluation_versions)")
    }
    required_indexes = {
        "uq_evaluation_automatic_idempotency",
        "uq_evaluation_manual_idempotency",
        "uq_evaluation_migrated_parent_idempotency",
    }
    if required_indexes - indexes or "uq_evaluation_idempotency" in indexes:
        raise SchemaMigrationError("schema v9 evaluation indexes are inconsistent")
    if connection.execute("PRAGMA foreign_key_check").fetchall():
        raise SchemaMigrationError("schema v9 has foreign-key violations")


_REBUILT_V10_TABLES = (
    "comment_evidence_versions",
    "comments",
    "comment_user_scores",
)
_NEW_V10_TABLES = (
    "interaction_users",
    "interaction_user_classification_versions",
    "comment_capture_runs",
    "comment_capture_pages",
)


def _preflight_v9_for_v10(connection: sqlite3.Connection) -> None:
    tables = _table_names(connection)
    missing = set(_REBUILT_V10_TABLES) - tables
    if missing:
        raise SchemaMigrationError(
            f"v9 schema is missing required tables: {sorted(missing)}"
        )
    residual = sorted(table for table in tables if table.endswith("_v10_new"))
    if residual:
        raise SchemaMigrationError(f"partial v10 tables already exist: {residual}")
    already = sorted(set(_NEW_V10_TABLES) & tables)
    if already:
        raise SchemaMigrationError(
            f"v10 tables already exist before migration: {already}"
        )
    if "key_version" in _table_columns(connection, "comment_user_scores"):
        raise SchemaMigrationError(
            "comment_user_scores already carries key_version before v10 migration"
        )
    violations = connection.execute("PRAGMA foreign_key_check").fetchall()
    if violations:
        raise SchemaMigrationError(f"v9 foreign-key violations: {len(violations)}")


def _migrate_v9_to_v10(connection: sqlite3.Connection) -> None:
    if connection.in_transaction:
        raise SchemaMigrationError("v10 migration requires no active transaction")
    foreign_keys = int(connection.execute("PRAGMA foreign_keys").fetchone()[0])
    if foreign_keys != 1:
        raise SchemaMigrationError("v10 migration requires PRAGMA foreign_keys=ON")

    _preflight_v9_for_v10(connection)
    old_columns = {
        table: _table_columns(connection, table) for table in _REBUILT_V10_TABLES
    }
    old_counts = {
        table: int(connection.execute(f'SELECT COUNT(*) FROM "{table}"').fetchone()[0])
        for table in _REBUILT_V10_TABLES
    }
    old_hashes = {
        table: _table_projection_sha256(connection, table, columns)
        for table, columns in old_columns.items()
    }
    try:
        sequences = {
            str(row["name"]): int(row["seq"])
            for row in connection.execute(
                "SELECT name, seq FROM sqlite_sequence"
            ).fetchall()
            if str(row["name"]) in _REBUILT_V10_TABLES
        }
    except sqlite3.OperationalError:
        sequences = {}

    captured_at = now_utc()
    statements = _schema_statements()
    tables_by_name: dict[str, str] = {}
    for statement in statements:
        match = re.match(r"CREATE TABLE IF NOT EXISTS ([a-z_]+) ", statement)
        if match:
            tables_by_name[match.group(1)] = statement

    connection.execute("PRAGMA foreign_keys=OFF")
    try:
        connection.execute("BEGIN IMMEDIATE")
        for table in _NEW_V10_TABLES:
            table_statement = tables_by_name.get(table)
            if table_statement is None:
                raise SchemaMigrationError(f"missing schema template for {table}")
            connection.execute(table_statement)
        _migration_checkpoint("v10_new_tables_created")

        replacements = {table: f"{table}_v10_new" for table in _REBUILT_V10_TABLES}
        for table in _REBUILT_V10_TABLES:
            connection.execute(_schema_table_statement(table, replacements))
        for table in _REBUILT_V10_TABLES:
            names = ",".join(f'"{column}"' for column in old_columns[table])
            connection.execute(
                f'INSERT INTO "{table}_v10_new"({names}) SELECT {names} FROM "{table}"'
            )
        _migration_checkpoint("v10_rows_copied")

        for table in ("comments", "comment_user_scores", "comment_evidence_versions"):
            connection.execute(f'DROP TABLE "{table}"')
        for table in _REBUILT_V10_TABLES:
            connection.execute(f'ALTER TABLE "{table}_v10_new" RENAME TO "{table}"')
        _migration_checkpoint("v10_tables_renamed")

        index_targets = {*_REBUILT_V10_TABLES, *_NEW_V10_TABLES}
        for statement in statements:
            is_index = statement.startswith(
                "CREATE INDEX IF NOT EXISTS"
            ) or statement.startswith("CREATE UNIQUE INDEX IF NOT EXISTS")
            target = re.search(r"\bON\s+([a-z_]+)\s*\(", statement, re.IGNORECASE)
            if is_index and target is not None and target.group(1) in index_targets:
                connection.execute(statement)
        _restore_sequences(connection, sequences, tables=_REBUILT_V10_TABLES)
        connection.execute(
            """
            INSERT INTO schema_migrations(version,name,applied_at)
            VALUES (10,'audience-interaction-user-domain',?)
            """,
            (captured_at,),
        )
        connection.execute("PRAGMA user_version=10")
        _migration_checkpoint("v10_indexes_created")

        for table in _REBUILT_V10_TABLES:
            current_count = int(
                connection.execute(f'SELECT COUNT(*) FROM "{table}"').fetchone()[0]
            )
            if current_count != old_counts[table]:
                raise SchemaMigrationError(
                    f"row count changed for {table}: {old_counts[table]}->{current_count}"
                )
            current_hash = _table_projection_sha256(
                connection, table, old_columns[table]
            )
            if current_hash != old_hashes[table]:
                raise SchemaMigrationError(f"legacy projection changed for {table}")
        drifted = int(
            connection.execute(
                """
                SELECT COUNT(*) FROM comment_user_scores
                WHERE key_version<>? OR score_rule_version<>?
                """,
                (
                    LEGACY_COMMENT_USER_KEY_VERSION,
                    LEGACY_COMMENT_SCORE_RULE_VERSION,
                ),
            ).fetchone()[0]
        )
        if drifted:
            raise SchemaMigrationError(
                f"comment_user_scores version backfill failed for {drifted} rows"
            )
        violations = connection.execute("PRAGMA foreign_key_check").fetchall()
        if violations:
            raise SchemaMigrationError(
                f"v10 foreign-key violations before commit: {len(violations)}"
            )
        connection.commit()
    except Exception:
        connection.rollback()
        raise
    finally:
        connection.execute("PRAGMA foreign_keys=ON")

    if connection.execute("PRAGMA foreign_key_check").fetchall():
        raise SchemaMigrationError("v10 foreign-key check failed after commit")
    integrity = str(connection.execute("PRAGMA integrity_check").fetchone()[0])
    if integrity != "ok":
        raise SchemaMigrationError(f"v10 integrity check failed: {integrity}")


def _validate_v10(connection: sqlite3.Connection) -> None:
    tables = _table_names(connection)
    missing = set(_NEW_V10_TABLES) - tables
    if missing:
        raise SchemaMigrationError(
            f"schema v10 is missing tables: {sorted(missing)}"
        )
    comment_columns = set(_table_columns(connection, "comments"))
    required_comment_columns = {
        "capture_page_id",
        "interaction_user_id",
        "comment_identity_key",
    }
    if required_comment_columns - comment_columns:
        raise SchemaMigrationError(
            "schema v10 is missing comment columns: "
            f"{sorted(required_comment_columns - comment_columns)}"
        )
    score_columns = set(_table_columns(connection, "comment_user_scores"))
    if {"key_version", "score_rule_version"} - score_columns:
        raise SchemaMigrationError(
            "schema v10 is missing comment_user_scores version columns"
        )
    if "capture_run_id" not in _table_columns(connection, "comment_evidence_versions"):
        raise SchemaMigrationError(
            "schema v10 is missing comment_evidence_versions.capture_run_id"
        )
    indexes = {
        str(row["name"])
        for row in connection.execute("PRAGMA index_list(comments)")
    }
    if "uq_comments_identity_per_evidence" not in indexes:
        raise SchemaMigrationError(
            "schema v10 is missing the comment identity uniqueness index"
        )
    if connection.execute("PRAGMA foreign_key_check").fetchall():
        raise SchemaMigrationError("schema v10 has foreign-key violations")


_REBUILT_V11_TABLES = ("interaction_users",)


def _migrate_v10_to_v11(connection: sqlite3.Connection) -> None:
    """Widen interaction_users.key_version to accept the v1 fallback domain.

    2026-08-07 owner decision (Mark): pre-v8.4 comments carry only the
    content-scoped ``content-user-hmac-v1`` pseudonym — the raw uid was never
    stored, so the platform-level v2 key is underivable. To let historical
    windows publish a user-level automotive rate at all, v1 keys become an
    explicit, queryable fallback key_version instead of dead data. Pure
    schema rebuild: row contents are copied verbatim and hash-verified.
    """

    if connection.in_transaction:
        raise SchemaMigrationError("v11 migration requires no active transaction")
    foreign_keys = int(connection.execute("PRAGMA foreign_keys").fetchone()[0])
    if foreign_keys != 1:
        raise SchemaMigrationError("v11 migration requires PRAGMA foreign_keys=ON")
    existing_sql = connection.execute(
        "SELECT sql FROM sqlite_master WHERE type='table' AND name='interaction_users'"
    ).fetchone()
    if existing_sql is None:
        raise SchemaMigrationError("v11 migration requires interaction_users")
    if "content-user-hmac-v1" in str(existing_sql[0]):
        # The table was already created from the current template (e.g. the
        # v9->v10 step in this same ladder run built it with the widened
        # CHECK). Nothing to rebuild — just stamp the migration row.
        captured_at = now_utc()
        connection.execute("BEGIN IMMEDIATE")
        try:
            connection.execute(
                """
                INSERT INTO schema_migrations(version,name,applied_at)
                VALUES (11,'interaction-user-v1-fallback-keys',?)
                """,
                (captured_at,),
            )
            connection.execute("PRAGMA user_version=11")
            connection.commit()
        except Exception:
            connection.rollback()
            raise
        return

    old_columns = {
        table: _table_columns(connection, table) for table in _REBUILT_V11_TABLES
    }
    old_counts = {
        table: int(connection.execute(f'SELECT COUNT(*) FROM "{table}"').fetchone()[0])
        for table in _REBUILT_V11_TABLES
    }
    old_hashes = {
        table: _table_projection_sha256(connection, table, columns)
        for table, columns in old_columns.items()
    }
    try:
        sequences = {
            str(row["name"]): int(row["seq"])
            for row in connection.execute(
                "SELECT name, seq FROM sqlite_sequence"
            ).fetchall()
            if str(row["name"]) in _REBUILT_V11_TABLES
        }
    except sqlite3.OperationalError:
        sequences = {}

    captured_at = now_utc()
    connection.execute("PRAGMA foreign_keys=OFF")
    try:
        connection.execute("BEGIN IMMEDIATE")
        replacements = {table: f"{table}_v11_new" for table in _REBUILT_V11_TABLES}
        for table in _REBUILT_V11_TABLES:
            connection.execute(_schema_table_statement(table, replacements))
        for table in _REBUILT_V11_TABLES:
            names = ",".join(f'"{column}"' for column in old_columns[table])
            connection.execute(
                f'INSERT INTO "{table}_v11_new"({names}) SELECT {names} FROM "{table}"'
            )
        for table in _REBUILT_V11_TABLES:
            connection.execute(f'DROP TABLE "{table}"')
            connection.execute(f'ALTER TABLE "{table}_v11_new" RENAME TO "{table}"')
        _restore_sequences(connection, sequences, tables=_REBUILT_V11_TABLES)
        connection.execute(
            """
            INSERT INTO schema_migrations(version,name,applied_at)
            VALUES (11,'interaction-user-v1-fallback-keys',?)
            """,
            (captured_at,),
        )
        connection.execute("PRAGMA user_version=11")

        for table in _REBUILT_V11_TABLES:
            current_count = int(
                connection.execute(f'SELECT COUNT(*) FROM "{table}"').fetchone()[0]
            )
            if current_count != old_counts[table]:
                raise SchemaMigrationError(
                    f"row count changed for {table}: {old_counts[table]}->{current_count}"
                )
            current_hash = _table_projection_sha256(
                connection, table, old_columns[table]
            )
            if current_hash != old_hashes[table]:
                raise SchemaMigrationError(f"projection changed for {table}")
        violations = connection.execute("PRAGMA foreign_key_check").fetchall()
        if violations:
            raise SchemaMigrationError(
                f"v11 foreign-key violations before commit: {len(violations)}"
            )
        connection.commit()
    except Exception:
        connection.rollback()
        raise
    finally:
        connection.execute("PRAGMA foreign_keys=ON")

    if connection.execute("PRAGMA foreign_key_check").fetchall():
        raise SchemaMigrationError("v11 foreign-key check failed after commit")
    integrity = str(connection.execute("PRAGMA integrity_check").fetchone()[0])
    if integrity != "ok":
        raise SchemaMigrationError(f"v11 integrity check failed: {integrity}")


def _validate_v11(connection: sqlite3.Connection) -> None:
    row = connection.execute(
        "SELECT sql FROM sqlite_master WHERE type='table' AND name='interaction_users'"
    ).fetchone()
    if row is None:
        raise SchemaMigrationError("schema v11 is missing interaction_users")
    sql = str(row[0])
    if "content-user-hmac-v1" not in sql or "platform-user-hmac-v2" not in sql:
        raise SchemaMigrationError(
            "schema v11 interaction_users must accept both key versions"
        )


def initialize_database(connection: sqlite3.Connection) -> None:
    tables = _table_names(connection)
    if not tables:
        _create_fresh_schema(connection)
        _validate_v9(connection)
        _validate_v10(connection)
        _validate_v11(connection)
        return
    if "schema_migrations" not in tables:
        raise SchemaMigrationError("database has tables but no schema_migrations")
    row = connection.execute("SELECT MAX(version) FROM schema_migrations").fetchone()
    version = int(row[0]) if row is not None and row[0] is not None else 0
    if version == 8:
        _migrate_v8_to_v9(connection)
        version = 9
    if version == 9:
        _migrate_v9_to_v10(connection)
        version = 10
    if version == 10:
        _migrate_v10_to_v11(connection)
        version = 11
    if version != SCHEMA_VERSION:
        raise SchemaMigrationError(f"unsupported schema version: {version}")
    _validate_v9(connection)
    _validate_v10(connection)
    _validate_v11(connection)
